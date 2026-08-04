use crate::common::{require_one_child, serialize_uuid};
use crate::coordinator::metrics_store::MetricsStore;
use crate::coordinator::prepare_static_plan::{
    PreparedStaticPlan, prepare_static_plan, publish_prepared_plan,
};
use crate::coordinator::task_spawner::{PlanCancellation, PlanPublication};
use crate::distributed_planner::NetworkBoundaryExt;
use crate::worker::generated::worker::TaskKey;
use datafusion::common::internal_datafusion_err;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;

/// [ExecutionPlan] that executes the inner plan in distributed mode.
/// Before worker requests are started, two modifications are performed on the plan:
/// 1. Assigns worker URLs to all the stages. Unless explicitly set in
///    [crate::TaskEstimator::route_tasks], a random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    plan: Arc<dyn ExecutionPlan>,
    prepared_plan: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
    lifecycle: Arc<Mutex<ExecutionLifecycle>>,
    metrics: ExecutionPlanMetricsSet,
    pub(crate) metrics_store: Option<Arc<MetricsStore>>,
}

#[derive(Debug)]
enum ExecutionLifecycle {
    Ready,
    Prepared {
        plan: PreparedStaticPlan,
        context: Arc<TaskContext>,
    },
    ExecutionStarted,
}

/// Statistics for the bytes stored in `SetPlanRequest.plan_proto` across a distributed plan.
///
/// This does not include the rest of the `SetPlanRequest` protobuf envelope.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SetPlanProtoStats {
    /// Largest task-specialized `SetPlanRequest.plan_proto` payload in bytes.
    pub max_task_plan_proto_bytes: usize,
    /// Sum of `SetPlanRequest.plan_proto` bytes across every stage and task recipient.
    pub aggregate_plan_proto_bytes: usize,
    /// Number of distributed stages measured.
    pub stage_count: usize,
    /// Number of task recipients measured across all stages.
    pub task_count: usize,
}

pub(super) struct PreparedPlan {
    pub(super) head_stage: Arc<dyn ExecutionPlan>,
    pub(super) join_set: JoinSet<Result<()>>,
    pub(super) plan_publications: Vec<PlanPublication>,
    pub(super) plan_cancellations: Vec<PlanCancellation>,
    pub(super) plans_published_tx: tokio::sync::watch::Sender<bool>,
}

async fn await_plan_publications(plan_publications: Vec<PlanPublication>) -> Result<()> {
    let mut pending = futures::stream::FuturesUnordered::from_iter(plan_publications);
    while let Some(publication) = pending.next().await {
        publication?;
    }
    Ok(())
}

fn cancel_published_plans(plan_cancellations: Vec<PlanCancellation>) {
    for cancellation in plan_cancellations {
        cancellation.cancel_in_background();
    }
}

impl DistributedExec {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan,
            prepared_plan: Arc::new(Mutex::new(None)),
            lifecycle: Arc::new(Mutex::new(ExecutionLifecycle::Ready)),
            metrics: ExecutionPlanMetricsSet::new(),
            metrics_store: None,
        }
    }

    /// Enables task metrics collection from remote workers.
    pub fn with_metrics_collection(mut self, enabled: bool) -> Self {
        self.metrics_store = match enabled {
            true => Some(Arc::new(MetricsStore::new())),
            false => None,
        };
        self
    }

    /// Prepares and measures the exact `SetPlanRequest.plan_proto` payloads execution will dispatch.
    ///
    /// Preparation routes stages bottom-up, specializes each task plan, and caches the resulting
    /// bytes. A subsequent [`ExecutionPlan::execute`] call publishes those same bytes without
    /// serializing accepted payloads again. Routing therefore occurs during this preflight and an
    /// accepted plan should be executed immediately afterward.
    ///
    /// Execution must receive the same [`Arc<TaskContext>`] instance passed here, as validated with
    /// [`Arc::ptr_eq`]. Callers must retain and reuse that `Arc`; an equivalent context allocated in
    /// a different `Arc` is rejected. No worker request is started by this method. Statistics may
    /// not be requested after execution has started.
    pub fn set_plan_proto_stats(&self, context: &Arc<TaskContext>) -> Result<SetPlanProtoStats> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock execution lifecycle: {e}"))?;
        match &*lifecycle {
            ExecutionLifecycle::Ready => {
                let plan = prepare_static_plan(&self.plan, context)?;
                let stats = plan.set_plan_proto_stats()?;
                *lifecycle = ExecutionLifecycle::Prepared {
                    plan,
                    context: Arc::clone(context),
                };
                Ok(stats)
            }
            ExecutionLifecycle::Prepared {
                plan,
                context: prepared_context,
            } => {
                if !Arc::ptr_eq(prepared_context, context) {
                    return exec_err!(
                        "SetPlanRequest.plan_proto payloads were prepared with a different TaskContext Arc"
                    );
                }
                plan.set_plan_proto_stats()
            }
            ExecutionLifecycle::ExecutionStarted => exec_err!(
                "Cannot measure SetPlanRequest.plan_proto payloads after DistributedExec execution has started"
            ),
        }
    }

    fn take_static_plan_for_execution(
        &self,
        context: &Arc<TaskContext>,
    ) -> Result<PreparedStaticPlan> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock execution lifecycle: {e}"))?;
        match std::mem::replace(&mut *lifecycle, ExecutionLifecycle::ExecutionStarted) {
            ExecutionLifecycle::Ready => match prepare_static_plan(&self.plan, context) {
                Ok(plan) => Ok(plan),
                Err(error) => {
                    *lifecycle = ExecutionLifecycle::Ready;
                    Err(error)
                }
            },
            ExecutionLifecycle::Prepared {
                plan,
                context: prepared_context,
            } => {
                if Arc::ptr_eq(&prepared_context, context) {
                    Ok(plan)
                } else {
                    *lifecycle = ExecutionLifecycle::Prepared {
                        plan,
                        context: prepared_context,
                    };
                    exec_err!(
                        "DistributedExec must use the same TaskContext Arc passed to set_plan_proto_stats"
                    )
                }
            }
            ExecutionLifecycle::ExecutionStarted => {
                *lifecycle = ExecutionLifecycle::ExecutionStarted;
                exec_err!("DistributedExec execution has already started")
            }
        }
    }

    /// Waits until all worker tasks have reported their metrics back via the coordinator channel.
    ///
    /// Metrics are delivered asynchronously after query execution completes, so callers that need
    /// complete metrics (e.g. for observability or display) should await this before inspecting
    /// [`Self::task_metrics`] or calling [`rewrite_distributed_plan_with_metrics`].
    ///
    /// [`rewrite_distributed_plan_with_metrics`]: crate::rewrite_distributed_plan_with_metrics
    pub async fn wait_for_metrics(&self) {
        let mut expected_keys: Vec<TaskKey> = Vec::new();
        let Some(task_metrics) = &self.metrics_store else {
            return;
        };
        let _ = self.plan.apply(|plan| {
            if let Some(boundary) = plan.as_network_boundary() {
                let stage = boundary.input_stage();
                for i in 0..stage.task_count() {
                    expected_keys.push(TaskKey {
                        query_id: serialize_uuid(&stage.query_id()),
                        stage_id: stage.num() as u64,
                        task_number: i as u64,
                    });
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if expected_keys.is_empty() {
            return;
        }
        let mut rx = task_metrics.rx.clone();
        let _ = rx
            .wait_for(|map| expected_keys.iter().all(|key| map.contains_key(key)))
            .await;
    }

    /// Returns the plan which is prepared by the first `execute()` call and actually gets executed.
    /// Returns an error if `.execute()` has not been called.
    pub(crate) fn prepared_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        self.prepared_plan
            .lock()
            .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {}", e))?
            .clone()
            .ok_or_else(|| {
                internal_datafusion_err!("No prepared plan found. Was execute() called?")
            })
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedExec")
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedExec {
            plan: require_one_child(&children)?,
            prepared_plan: self.prepared_plan.clone(),
            lifecycle: Arc::new(Mutex::new(ExecutionLifecycle::Ready)),
            metrics: self.metrics.clone(),
            metrics_store: self.metrics_store.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition > 0 {
            // The DistributedExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "DistributedExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

        let prepared_static_plan = self.take_static_plan_for_execution(&context)?;
        let PreparedPlan {
            head_stage,
            join_set,
            plan_publications,
            plan_cancellations,
            plans_published_tx,
        } = publish_prepared_plan(
            prepared_static_plan,
            &self.metrics,
            &self.metrics_store,
            &context,
        )?;
        {
            let mut guard = self
                .prepared_plan
                .lock()
                .map_err(|e| internal_datafusion_err!("Failed to lock prepared plan: {e}"))?;
            *guard = Some(head_stage.clone());
        }
        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema(), 1);
        let tx = builder.tx();
        // Wait for every worker to publish its plan before execution can issue
        // ExecuteTask RPCs through the network boundaries.
        builder.spawn(async move {
            if let Err(error) = await_plan_publications(plan_publications).await {
                cancel_published_plans(plan_cancellations);
                return Err(error);
            }
            // Cancellation handles retain coordinator senders, so release them once publication
            // succeeds to let completed work unit feeds close their worker streams.
            drop(plan_cancellations);
            let _ = plans_published_tx.send(true);
            let mut stream = head_stage.execute(partition, context)?;
            while let Some(msg) = stream.next().await {
                if tx.send(msg).await.is_err() {
                    break; // channel closed
                }
            }
            Ok(())
        });
        // Coordinator channels remain open for work unit feeds and task metrics.
        builder.spawn(async move {
            let mut join_set = join_set;
            while let Some(result) = join_set.join_next().await {
                result.map_err(|error| {
                    internal_datafusion_err!("Coordinator task failed: {error}")
                })??;
            }
            Ok(())
        });
        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::memory::DataSourceExec;
    use datafusion::common::exec_datafusion_err;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::prelude::SessionConfig;

    use crate::common::now_ns;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::{DistributedConfig, DistributedExt, NetworkCoalesceExec, SessionStateBuilderExt};

    use super::*;

    fn empty_plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            true,
        )]))))
    }

    fn scan_plan(file_count: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let mut builder = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("s3://test-bucket").unwrap(),
            Arc::new(ParquetSource::new(schema)),
        );
        for file_i in 0..file_count {
            builder = builder.with_file(PartitionedFile::new(
                format!("data/partition-{file_i:04}/file-{file_i:04}.parquet"),
                1024,
            ));
        }
        Arc::new(DataSourceExec::new(Arc::new(builder.build())))
    }

    fn distributed_plan(input: Arc<dyn ExecutionPlan>, task_count: usize) -> DistributedExec {
        let boundary = NetworkCoalesceExec::try_new(input, task_count, 1).unwrap();
        DistributedExec::new(Arc::new(boundary))
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

    #[test]
    fn set_plan_proto_stats_include_every_task_recipient() {
        let context = task_context();
        let one_task = distributed_plan(empty_plan(), 1)
            .set_plan_proto_stats(&context)
            .unwrap();
        let three_tasks = distributed_plan(empty_plan(), 3)
            .set_plan_proto_stats(&context)
            .unwrap();

        assert_eq!(three_tasks.stage_count, 1);
        assert_eq!(three_tasks.task_count, 3);
        assert_eq!(
            three_tasks.max_task_plan_proto_bytes,
            one_task.max_task_plan_proto_bytes
        );
        assert_eq!(
            three_tasks.aggregate_plan_proto_bytes,
            one_task.aggregate_plan_proto_bytes * 3
        );
    }

    #[test]
    fn repeated_scans_increase_set_plan_proto_bytes() {
        let context = task_context();
        let scan = scan_plan(64);
        let single = distributed_plan(Arc::clone(&scan), 1)
            .set_plan_proto_stats(&context)
            .unwrap();
        let repeated = UnionExec::try_new(vec![
            Arc::clone(&scan),
            Arc::clone(&scan),
            Arc::clone(&scan),
            scan,
        ])
        .unwrap();
        let repeated = distributed_plan(repeated, 1)
            .set_plan_proto_stats(&context)
            .unwrap();

        assert!(repeated.max_task_plan_proto_bytes > single.max_task_plan_proto_bytes);
        assert!(repeated.aggregate_plan_proto_bytes > single.aggregate_plan_proto_bytes);
    }

    #[test]
    fn prepared_payloads_require_the_same_task_context_arc() {
        let context = task_context();
        let different_context = task_context();
        let distributed = distributed_plan(empty_plan(), 1);

        distributed.set_plan_proto_stats(&context).unwrap();
        let stats_error = distributed
            .set_plan_proto_stats(&different_context)
            .unwrap_err();
        assert!(
            stats_error
                .to_string()
                .contains("different TaskContext Arc")
        );

        let execute_error = distributed
            .take_static_plan_for_execution(&different_context)
            .unwrap_err();
        assert!(
            execute_error
                .to_string()
                .contains("same TaskContext Arc passed to set_plan_proto_stats")
        );

        distributed
            .take_static_plan_for_execution(&context)
            .unwrap();
    }

    #[test]
    fn execution_lifecycle_rejects_repeated_execution_and_late_stats() {
        let context = task_context();
        let distributed = distributed_plan(empty_plan(), 1);

        distributed.set_plan_proto_stats(&context).unwrap();
        distributed
            .take_static_plan_for_execution(&context)
            .unwrap();

        let execute_error = distributed
            .take_static_plan_for_execution(&context)
            .unwrap_err();
        assert!(
            execute_error
                .to_string()
                .contains("execution has already started")
        );
        let stats_error = distributed.set_plan_proto_stats(&context).unwrap_err();
        assert!(
            stats_error
                .to_string()
                .contains("after DistributedExec execution has started")
        );
    }

    #[test]
    fn execution_without_preflight_keeps_static_preparation_start_time() {
        let context = task_context();
        let distributed = distributed_plan(empty_plan(), 1);
        let before = now_ns();

        let prepared = distributed
            .take_static_plan_for_execution(&context)
            .unwrap();
        let after = now_ns();

        assert!(prepared.query_start_time_ns >= before);
        assert!(prepared.query_start_time_ns <= after);
    }

    #[tokio::test]
    async fn waits_for_every_plan_publication() {
        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        let (last_tx, last_rx) = tokio::sync::oneshot::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = Arc::clone(&completed);

        #[allow(clippy::disallowed_methods)]
        let waiter = tokio::spawn(async move {
            await_plan_publications(vec![
                Box::pin(async move { first_rx.await.unwrap() }),
                Box::pin(async move { last_rx.await.unwrap() }),
            ])
            .await
            .unwrap();
            completed_clone.store(true, Ordering::SeqCst);
        });

        first_tx.send(Ok(())).unwrap();
        tokio::task::yield_now().await;
        assert!(!completed.load(Ordering::SeqCst));

        last_tx.send(Ok(())).unwrap();
        waiter.await.unwrap();
        assert!(completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn propagates_plan_publication_failure() {
        let publication =
            Box::pin(async { Err(exec_datafusion_err!("injected publication failure")) });

        let error = await_plan_publications(vec![publication])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("injected publication failure"));
    }

    #[tokio::test]
    async fn publication_failure_is_not_blocked_by_pending_sibling() {
        let pending = Box::pin(std::future::pending());
        let failed = Box::pin(async { Err(exec_datafusion_err!("later publication failed")) });

        let error = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            await_plan_publications(vec![pending, failed]),
        )
        .await
        .expect("Publication failure should be fail-fast")
        .unwrap_err();
        assert!(error.to_string().contains("later publication failed"));
    }

    #[tokio::test]
    async fn reports_plan_publication_task_cancellation() {
        let publication = Box::pin(async {
            Err(internal_datafusion_err!(
                "Plan publication task ended without reporting acknowledgement"
            ))
        });

        let error = await_plan_publications(vec![publication])
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ended without reporting acknowledgement")
        );
    }
}
