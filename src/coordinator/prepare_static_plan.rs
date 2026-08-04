use crate::common::now_ns;
use crate::coordinator::MetricsStore;
use crate::coordinator::distributed::{PreparedPlan, SetPlanProtoStats};
use crate::coordinator::task_spawner::{
    CoordinatorToWorkerMetrics, CoordinatorToWorkerTaskSpawner, SerializedTaskPlan,
    serialize_task_plan,
};
use crate::stage::{LocalStage, RemoteStage};
use crate::{
    DistributedConfig, NetworkBoundaryExt, Stage, TaskEstimator, TaskRoutingContext,
    get_distributed_worker_resolver,
};
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err, internal_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use rand::Rng;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct PreparedStaticPlan {
    pub(super) head_stage: Arc<dyn ExecutionPlan>,
    pub(super) query_start_time_ns: u64,
    stages: Vec<PreparedStage>,
}

#[derive(Debug)]
struct PreparedStage {
    stage: LocalStage,
    tasks: Vec<PreparedTask>,
}

#[derive(Debug)]
struct PreparedTask {
    task_index: usize,
    worker_url: url::Url,
    serialized_plan: SerializedTaskPlan,
}

impl PreparedStaticPlan {
    pub(super) fn set_plan_proto_stats(&self) -> Result<SetPlanProtoStats> {
        let mut stats = SetPlanProtoStats {
            stage_count: self.stages.len(),
            ..Default::default()
        };
        for task in self.stages.iter().flat_map(|stage| &stage.tasks) {
            let task_bytes = task.serialized_plan.plan_proto.len();
            stats.task_count = stats
                .task_count
                .checked_add(1)
                .ok_or_else(|| internal_datafusion_err!("Distributed task count overflow"))?;
            stats.max_task_plan_proto_bytes = stats.max_task_plan_proto_bytes.max(task_bytes);
            stats.aggregate_plan_proto_bytes = stats
                .aggregate_plan_proto_bytes
                .checked_add(task_bytes)
                .ok_or_else(|| {
                    internal_datafusion_err!("Distributed plan_proto payload size overflow")
                })?;
        }
        Ok(stats)
    }
}

/// Prepares the distributed plan for execution without starting worker requests, which implies:
/// 1. Perform some worker URL assignation, choosing either:
///    - The URLs set by the user with [crate::TaskEstimator::route_tasks].
///    - Randomly otherwise
/// 2. Specialize and serialize each task plan after its child stages have been routed.
/// 3. In each network boundary, set the input plan to `None`. That way, network boundaries
///    become nodes without children and traversing them will not go further down in.
pub(super) fn prepare_static_plan(
    base_plan: &Arc<dyn ExecutionPlan>,
    ctx: &Arc<TaskContext>,
) -> Result<PreparedStaticPlan> {
    let query_start_time_ns = now_ns();
    let worker_resolver = get_distributed_worker_resolver(ctx.session_config())?;

    let available_urls = worker_resolver.get_urls()?;

    let mut stages = vec![];
    let prepared = Arc::clone(base_plan).transform_up(|plan| {
        // The following logic is just applied on network boundaries.
        let Some(plan) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };

        let Stage::Local(stage) = plan.input_stage() else {
            return exec_err!("Input stage from network boundary was not in Local state");
        };

        let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;
        let task_estimator = &d_cfg.__private_task_estimator;

        let routed_urls = match task_estimator.route_tasks(&TaskRoutingContext {
            task_ctx: Arc::clone(ctx),
            plan: &stage.plan,
            task_count: stage.tasks,
            available_urls: &available_urls,
        }) {
            Ok(Some(routed_urls)) => routed_urls,
            // If the user has not defined custom routing with a `route_tasks` implementation, we
            // default to round-robin task assignation from a randomized starting point.
            Ok(None) => {
                let start_idx = rand::rng().random_range(0..available_urls.len());
                (0..stage.tasks)
                    .map(|i| available_urls[(start_idx + i) % available_urls.len()].clone())
                    .collect()
            }
            Err(e) => return exec_err!("error routing tasks to workers: {e}"),
        };

        if routed_urls.len() != stage.tasks {
            return exec_err!(
                "number of tasks ({}) was not equal to number of urls ({}) at execution time",
                stage.tasks,
                routed_urls.len()
            );
        }

        let mut workers = Vec::with_capacity(stage.tasks);
        let mut tasks = Vec::with_capacity(stage.tasks);
        for (i, routed_url) in routed_urls.into_iter().enumerate() {
            workers.push(routed_url.clone());
            tasks.push(PreparedTask {
                task_index: i,
                worker_url: routed_url,
                serialized_plan: serialize_task_plan(&stage.plan, stage.tasks, i, ctx)?,
            });
        }
        stages.push(PreparedStage {
            stage: stage.clone(),
            tasks,
        });

        Ok(Transformed::yes(plan.with_input_stage(Stage::Remote(
            RemoteStage {
                query_id: stage.query_id,
                num: stage.num,
                workers,
            },
        ))?))
    })?;
    Ok(PreparedStaticPlan {
        head_stage: prepared.data,
        query_start_time_ns,
        stages,
    })
}

/// Publishes a prepared distributed plan and starts its worker communication tasks.
pub(super) fn publish_prepared_plan(
    prepared: PreparedStaticPlan,
    metrics: &ExecutionPlanMetricsSet,
    task_metrics: &Option<Arc<MetricsStore>>,
    ctx: &Arc<TaskContext>,
) -> Result<PreparedPlan> {
    let metrics = CoordinatorToWorkerMetrics::new(metrics, prepared.query_start_time_ns);
    let mut join_set = JoinSet::new();

    for prepared_stage in prepared.stages {
        let mut spawner = CoordinatorToWorkerTaskSpawner::new(
            &prepared_stage.stage,
            &metrics,
            task_metrics,
            &mut join_set,
        )?;
        for task in prepared_stage.tasks {
            let (tx, worker_rx) = spawner.send_plan_task(
                Arc::clone(ctx),
                task.task_index,
                task.worker_url,
                task.serialized_plan,
            )?;
            spawner.metrics_collection_task(task.task_index, worker_rx);
            spawner.work_unit_feed_task(Arc::clone(ctx), task.task_index, tx)?;
        }
    }

    Ok(PreparedPlan {
        head_stage: prepared.head_stage,
        join_set,
    })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::memory::DataSourceExec;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionConfig;
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use prost::Message;

    use crate::TaskKey;
    use crate::common::serialize_uuid;
    use crate::execution_plans::DistributedLeafExec;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::worker::generated::worker as pb;
    use crate::{
        DistributedCodec, DistributedConfig, DistributedExt, NetworkCoalesceExec,
        SessionStateBuilderExt,
    };

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
    }

    fn empty_plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    fn scan_plan(path: &str) -> Arc<dyn ExecutionPlan> {
        let builder = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("s3://test-bucket").unwrap(),
            Arc::new(ParquetSource::new(schema())),
        )
        .with_file(PartitionedFile::new(path, 1024));
        Arc::new(DataSourceExec::new(Arc::new(builder.build())))
    }

    fn task_context() -> Arc<TaskContext> {
        let mut config = SessionConfig::new();
        config.set_distributed_option_extension(DistributedConfig::default());
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_distributed_planner()
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(8))
            .build();
        SessionContext::new_with_state(state).task_ctx()
    }

    fn into_set_plan_requests(prepared: PreparedStaticPlan) -> Vec<pb::SetPlanRequest> {
        let query_start_time_ns = prepared.query_start_time_ns;
        prepared
            .stages
            .into_iter()
            .flat_map(|stage| {
                stage.tasks.into_iter().map(move |task| {
                    let task_key = TaskKey {
                        query_id: serialize_uuid(&stage.stage.query_id),
                        stage_id: stage.stage.num as u64,
                        task_number: task.task_index as u64,
                    };
                    task.serialized_plan.into_set_plan_request(
                        stage.stage.tasks,
                        task_key,
                        &task.worker_url,
                        query_start_time_ns,
                    )
                })
            })
            .collect()
    }

    #[test]
    fn multi_stage_stats_match_published_plan_proto_payloads() {
        let context = task_context();
        let child_boundary = Arc::new(NetworkCoalesceExec::try_new(empty_plan(), 2, 3).unwrap());
        let root_boundary = Arc::new(NetworkCoalesceExec::try_new(child_boundary, 3, 1).unwrap());
        let prepared =
            prepare_static_plan(&(root_boundary as Arc<dyn ExecutionPlan>), &context).unwrap();
        let stats = prepared.set_plan_proto_stats().unwrap();

        assert_eq!(stats.stage_count, 2);
        assert_eq!(stats.task_count, 5);

        let parent_payloads = prepared.stages[1]
            .tasks
            .iter()
            .map(|task| &task.serialized_plan.plan_proto)
            .collect::<Vec<_>>();
        let codec = DistributedCodec::new_combined_with_user(context.session_config());
        for payload in parent_payloads {
            let proto = PhysicalPlanNode::decode(payload.as_slice()).unwrap();
            let plan = proto.try_into_physical_plan(&context, &codec).unwrap();
            let boundary = plan.as_network_boundary().unwrap();
            assert!(matches!(boundary.input_stage(), Stage::Remote(_)));
        }

        let requests = into_set_plan_requests(prepared);
        assert!(
            requests
                .iter()
                .all(|request| request.query_start_time_ns > 0)
        );
        assert!(requests.windows(2).all(|requests| {
            requests[0].query_start_time_ns == requests[1].query_start_time_ns
        }));
        let request_sizes = requests
            .iter()
            .map(|request| request.plan_proto.len())
            .collect::<Vec<_>>();
        assert_eq!(
            stats.max_task_plan_proto_bytes,
            *request_sizes.iter().max().unwrap()
        );
        assert_eq!(
            stats.aggregate_plan_proto_bytes,
            request_sizes.iter().sum::<usize>()
        );
    }

    #[test]
    fn published_plan_proto_uses_each_task_specialization() {
        let context = task_context();
        let first_path = "data/task-zero.parquet";
        let second_path = "data/task-one-with-a-longer-name.parquet";
        let specialized = Arc::new(DistributedLeafExec::new(
            scan_plan("data/original.parquet"),
            [scan_plan(first_path), scan_plan(second_path)],
        ));
        let boundary = Arc::new(NetworkCoalesceExec::try_new(specialized, 2, 1).unwrap());
        let prepared =
            prepare_static_plan(&(boundary as Arc<dyn ExecutionPlan>), &context).unwrap();
        let requests = into_set_plan_requests(prepared);

        assert_eq!(requests.len(), 2);
        let first = String::from_utf8_lossy(&requests[0].plan_proto);
        let second = String::from_utf8_lossy(&requests[1].plan_proto);
        assert!(first.contains(first_path));
        assert!(!first.contains(second_path));
        assert!(second.contains(second_path));
        assert!(!second.contains(first_path));
        assert_ne!(requests[0].plan_proto, requests[1].plan_proto);
    }
}
