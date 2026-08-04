use crate::common::{TreeNodeExt, now_ns, serialize_uuid, task_ctx_with_extension};
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::coordinator::MetricsStore;
use crate::execution_plans::{ChildrenIsolatorUnionExec, DistributedLeafExec};
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::tonic_status_to_datafusion_error;
use crate::stage::LocalStage;
use crate::work_unit_feed::{build_work_unit_batch_msg, set_work_unit_send_time};
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::{
    ChannelResolver, DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, DistributedCodec, DistributedConfig,
    DistributedTaskContext, DistributedWorkUnitFeedContext, PLAN_PUBLICATION_ERROR_PREFIX, TaskKey,
    get_distributed_channel_resolver,
};
use datafusion::common::Result;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{DataFusionError, exec_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::{
    Count, ExecutionPlanMetricsSet, Label, MetricBuilder, MetricValue, Time,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use http::Extensions;
use prost::Message;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataMap;
use url::Url;
use uuid::Uuid;

/// How many [crate::WorkUnit] messages are allowed to be chunked synchronously together in order to
/// send fewer bigger [crate::WorkUnit] batches over the wire, reducing the overhead of sending many
/// small batches. See [StreamExt::ready_chunks] docs for more details about how chunking works.
const WORK_UNIT_FEED_CHUNK_SIZE: usize = 256;
const PLAN_PUBLICATION_TIMEOUT_SECS: u64 = 30;

fn plan_publication_status_error(
    error: tonic::Status,
    task_key: &TaskKey,
    plan_fingerprint: u64,
    worker: &Url,
) -> DataFusionError {
    let source = tonic_status_to_datafusion_error(&error)
        .map_or_else(|| error.to_string(), |decoded| decoded.to_string());
    exec_datafusion_err!(
        "{PLAN_PUBLICATION_ERROR_PREFIX} Plan publication failed: task_key={task_key:?}, plan_fingerprint={plan_fingerprint:016x}, worker={worker}, phase=waiting for CoordinatorChannel acknowledgement: {source}"
    )
}

type PlanTaskChannels = (
    UnboundedSender<pb::CoordinatorToWorkerMsg>,
    UnboundedReceiver<pb::WorkerToCoordinatorMsg>,
    PlanPublication,
    PlanCancellation,
);

pub(super) type PlanPublication = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

#[derive(Clone)]
pub(super) struct PlanCancellation {
    channel_resolver: Arc<dyn ChannelResolver + Send + Sync>,
    coordinator_tx: UnboundedSender<pb::CoordinatorToWorkerMsg>,
    task_key: TaskKey,
    url: Url,
}

impl PlanCancellation {
    pub(super) fn cancel_stream(&self) {
        let _ = self.coordinator_tx.send(pb::CoordinatorToWorkerMsg {
            inner: Some(Inner::CancelPlanRequest(pb::CancelPlanRequest {
                task_key: Some(self.task_key.clone()),
            })),
        });
    }

    pub(super) async fn cancel(self) {
        self.cancel_stream();
        let Ok(Ok(mut client)) = tokio::time::timeout(
            Duration::from_secs(5),
            self.channel_resolver.get_worker_client_for_url(&self.url),
        )
        .await
        else {
            return;
        };
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            client.cancel_plan(pb::CancelPlanRequest {
                task_key: Some(self.task_key),
            }),
        )
        .await;
    }

    pub(super) fn cancel_in_background(self) {
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(self.cancel());
    }
}

/// Metrics that measure network details about communications between [DistributedExec] and a
/// worker.
#[derive(Clone)]
pub(super) struct CoordinatorToWorkerMetrics {
    pub(super) plan_bytes_sent: Count,
    pub(super) plan_send_latency: Arc<LatencyMetric>,
    pub(super) instantiation_time: u64,
}

impl CoordinatorToWorkerMetrics {
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            // Metric that measures to total sum of bytes worth of subplans sent.
            plan_bytes_sent: MetricBuilder::new(metrics)
                .with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0"))
                .global_counter("plan_bytes_sent"),
            // Latency statistics about the network calls issued to the workers for feeding subplans.
            plan_send_latency: Arc::new(LatencyMetric::new(
                "plan_send_latency",
                |b| b.with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0")),
                metrics,
            )),
            instantiation_time: now_ns(),
        }
    }
}

/// Builder for the different kind of tasks that handle the communications between the
/// [DistributedExec] node to the workers. This struct is responsible for instantiating the tasks
/// as boxed futures so that [DistributedExec] can tokio-spawn them at will.
///
/// This struct is responsible for:
/// - Building tasks that communicate a serialized plan to multiple workers for further execution.
/// - Building tasks that stream partition feeds from local [WorkUnitFeedExec] nodes to their
///   remote counterparts.
pub(super) struct CoordinatorToWorkerTaskSpawner<'a> {
    plan: &'a Arc<dyn ExecutionPlan>,
    query_id: Uuid,
    stage_id: usize,
    task_count: usize,
    task_ctx: &'a TaskContext,
    metrics: &'a CoordinatorToWorkerMetrics,
    task_metrics: &'a Option<Arc<MetricsStore>>,
    join_set: &'a mut JoinSet<Result<()>>,
}

impl<'a> CoordinatorToWorkerTaskSpawner<'a> {
    /// Builds a new [CoordinatorToWorkerTaskSpawner] based on the [Stage] that needs to be
    /// fanned out to multiple workers.
    pub(super) fn new(
        stage: &'a LocalStage,
        metrics: &'a CoordinatorToWorkerMetrics,
        task_metrics: &'a Option<Arc<MetricsStore>>,
        task_ctx: &'a TaskContext,
        join_set: &'a mut JoinSet<Result<()>>,
    ) -> Result<Self> {
        Ok(Self {
            plan: &stage.plan,
            query_id: stage.query_id,
            stage_id: stage.num,
            task_count: stage.tasks,
            task_ctx,
            metrics,
            task_metrics,
            join_set,
        })
    }

    /// Sends a serialized plan to a specific worker and sets up the bidirectional gRPC stream.
    /// Returns the sender for outbound coordinator-to-worker messages and the receiver for
    /// inbound worker-to-coordinator messages.
    pub(super) fn send_plan_task(
        &mut self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        url: Url,
    ) -> Result<PlanTaskChannels> {
        let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;
        let wuf_registry = &d_cfg.__private_work_unit_feed_registry;
        let plan_publication_timeout = Duration::from_secs(PLAN_PUBLICATION_TIMEOUT_SECS);

        let mut work_unit_feed_declarations = vec![];
        let d_ctx = DistributedTaskContext {
            task_index: task_i,
            task_count: self.task_count,
        };

        let plan = Arc::clone(self.plan);
        let specialized = plan.transform_down_with_dt_ctx(d_ctx, |plan, d_ctx| {
            if let Some(wuf) = wuf_registry.get_work_unit_feed(&plan) {
                work_unit_feed_declarations.push(WorkUnitFeedDeclaration {
                    id: serialize_uuid(&wuf.id()),
                    partitions: plan.properties().partitioning.partition_count() as u64,
                });
            };

            if let Some(ciu) = plan.downcast_ref::<ChildrenIsolatorUnionExec>() {
                let ciu = ciu.to_task_specialized(d_ctx.task_index);
                return Ok(Transformed::yes(Arc::new(ciu)));
            };

            if let Some(dle) = plan.downcast_ref::<DistributedLeafExec>() {
                let specialized = dle.to_task_specialized(d_ctx.task_index);
                return Ok(Transformed::yes(specialized));
            }

            Ok(Transformed::no(plan))
        })?;

        let codec = DistributedCodec::new_combined_with_user(self.task_ctx.session_config());

        let plan_proto =
            PhysicalPlanNode::try_from_physical_plan(specialized.data, &codec)?.encode_to_vec();
        let plan_size = plan_proto.len();
        let mut plan_hasher = DefaultHasher::new();
        plan_proto.hash(&mut plan_hasher);
        let plan_fingerprint = plan_hasher.finish();

        let task_key = TaskKey {
            query_id: serialize_uuid(&self.query_id),
            stage_id: self.stage_id as u64,
            task_number: task_i as u64,
        };
        let msg = pb::CoordinatorToWorkerMsg {
            inner: Some(Inner::SetPlanRequest(pb::SetPlanRequest {
                plan_proto,
                task_count: self.task_count as u64,
                task_key: Some(task_key.clone()),
                work_unit_feed_declarations,
                target_worker_url: url.to_string(),
                query_start_time_ns: self.metrics.instantiation_time,
            })),
        };

        let (coordinator_to_worker_tx, coordinator_to_worker_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (worker_to_coordinator_tx, worker_to_coordinator_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (plan_publication_tx, plan_publication_rx) = oneshot::channel();

        let channel_resolver = get_distributed_channel_resolver(ctx.as_ref());
        let plan_cancellation = PlanCancellation {
            channel_resolver: Arc::clone(&channel_resolver),
            coordinator_tx: coordinator_to_worker_tx.clone(),
            task_key: task_key.clone(),
            url: url.clone(),
        };

        let mut headers = get_config_extension_propagation_headers(ctx.session_config())?;
        headers.extend(get_passthrough_headers(ctx.session_config()));

        let request = Request::from_parts(
            MetadataMap::from_headers(headers),
            Extensions::default(),
            futures::stream::once(async { msg }).chain(
                UnboundedReceiverStream::new(coordinator_to_worker_rx).map(set_work_unit_send_time),
            ),
        );

        let metrics = self.metrics.clone();
        let publication_task_key = task_key.clone();
        let publication_url = url.clone();

        self.join_set.spawn(async move {
            let start = Instant::now();
            let response = tokio::time::timeout(plan_publication_timeout, async {
                let mut client = channel_resolver
                    .get_worker_client_for_url(&url)
                    .await
                    .map_err(|error| {
                        exec_datafusion_err!(
                            "{PLAN_PUBLICATION_ERROR_PREFIX} Plan publication failed: task_key={publication_task_key:?}, plan_fingerprint={plan_fingerprint:016x}, worker={url}, phase=resolving worker channel: {error}"
                        )
                    })?;
                client
                    .coordinator_channel(request)
                    .await
                    .map_err(|error| {
                        plan_publication_status_error(
                            error,
                            &publication_task_key,
                            plan_fingerprint,
                            &url,
                        )
                    })
            })
            .await;
            let response = match response {
                Ok(response) => response,
                Err(_) => Err(exec_datafusion_err!(
                    "{PLAN_PUBLICATION_ERROR_PREFIX} Plan publication timeout after {}s: task_key={publication_task_key:?}, plan_fingerprint={plan_fingerprint:016x}, worker={url}, phase=resolving worker and waiting for CoordinatorChannel acknowledgement, last_completed_phase=plan serialized",
                    plan_publication_timeout.as_secs()
                )),
            };
            let response = match response {
                Ok(response) => {
                    let _ = plan_publication_tx.send(Ok(()));
                    response
                }
                Err(error) => {
                    let _ = plan_publication_tx.send(Err(error));
                    return Ok(());
                }
            };
            metrics.plan_send_latency.record(&start);
            metrics.plan_bytes_sent.add(plan_size);
            let mut worker_to_coordinator_stream = response.into_inner();
            while let Some(msg_or_err) = worker_to_coordinator_stream.next().await {
                let msg = match msg_or_err {
                    Ok(msg) => msg,
                    Err(err) => {
                        return Err(tonic_status_to_datafusion_error(err).unwrap_or_else(|| {
                            exec_datafusion_err!("Unknown error on worker to coordinator stream")
                        }));
                    }
                };
                if worker_to_coordinator_tx.send(msg).is_err() {
                    break; // receiver dropped
                }
            }
            Ok::<_, DataFusionError>(())
        });

        let plan_publication = Box::pin(async move {
            plan_publication_rx
                .await
                .map_err(|_| {
                    exec_datafusion_err!(
                        "{PLAN_PUBLICATION_ERROR_PREFIX} Plan publication task ended without acknowledgement: task_key={task_key:?}, plan_fingerprint={plan_fingerprint:016x}, worker={publication_url}, phase=waiting for CoordinatorChannel acknowledgement, last_completed_phase=plan serialized"
                    )
                })?
        });

        Ok((
            coordinator_to_worker_tx,
            worker_to_coordinator_rx,
            plan_publication,
            plan_cancellation,
        ))
    }

    pub(super) fn metrics_collection_task(
        &mut self,
        task_i: usize,
        mut worker_to_coordinator_rx: UnboundedReceiver<pb::WorkerToCoordinatorMsg>,
    ) {
        let task_key = TaskKey {
            query_id: serialize_uuid(&self.query_id),
            stage_id: self.stage_id as u64,
            task_number: task_i as u64,
        };
        let task_metrics = self.task_metrics.clone();
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            while let Some(msg) = worker_to_coordinator_rx.recv().await {
                let Some(inner) = msg.inner else { continue };

                match inner {
                    pb::worker_to_coordinator_msg::Inner::TaskMetrics(pre_order_metrics) => {
                        if let Some(task_metrics) = &task_metrics {
                            task_metrics.insert(task_key.clone(), pre_order_metrics);
                        }
                    }
                }
            }
        });
    }

    /// Launches the task that based on the different local [WorkUnitFeedExec] nodes, sends their
    /// inner [WorkUnitFeeds] over the network to their remote counterparts.
    ///
    /// The feeds are constructed when this function is called, but they are not consumed until all
    /// worker plans report successful publication through `plans_published`.
    pub(super) fn work_unit_feed_task(
        &mut self,
        ctx: Arc<TaskContext>,
        task_i: usize,
        tx: UnboundedSender<pb::CoordinatorToWorkerMsg>,
        mut plans_published: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let d_ctx = DistributedTaskContext {
            task_index: task_i,
            task_count: self.task_count,
        };
        let d_cfg = DistributedConfig::from_config_options(ctx.session_config().options())?;
        let wuf_registry = &d_cfg.__private_work_unit_feed_registry;
        let mut futures = vec![];
        self.plan.apply_with_dt_ctx(d_ctx, |plan, d_ctx| {
            let Some(wuf) = wuf_registry.get_work_unit_feed(plan) else {
                return Ok(TreeNodeRecursion::Continue);
            };

            let partitions = plan.properties().partitioning.partition_count();
            let start_partition = partitions * d_ctx.task_index;
            let end_partition = start_partition + partitions;

            let dist_feed_ctx = DistributedWorkUnitFeedContext {
                fan_out_tasks: d_ctx.task_count,
            };
            let t_ctx = Arc::new(task_ctx_with_extension(&ctx, dist_feed_ctx));

            let mut feeds = Vec::with_capacity(end_partition - start_partition);
            for (partition, feed_idx) in (start_partition..end_partition).enumerate() {
                let feed = wuf
                    .feed(feed_idx, Arc::clone(&t_ctx))?
                    .map(move |el| (partition, el));
                feeds.push(feed);
            }
            let interleaved_feed = futures::stream::select_all(feeds);
            let mut chunked_interleaved_feed =
                interleaved_feed.ready_chunks(WORK_UNIT_FEED_CHUNK_SIZE);

            let id = wuf.id();
            let tx = tx.clone();
            futures.push(Box::pin(async move {
                // At this point, the partition feed contains a stream of decoded messages,
                // so they must be encoded in order to send them over the wire.
                while let Some(chunk) = chunked_interleaved_feed.next().await {
                    if tx.send(build_work_unit_batch_msg(&id, chunk)?).is_err() {
                        break; // channel closed.
                    };
                }
                Ok::<_, DataFusionError>(())
            }));
            Ok(TreeNodeRecursion::Continue)
        })?;
        self.join_set.spawn(async move {
            while !*plans_published.borrow_and_update() {
                if plans_published.changed().await.is_err() {
                    return Ok(());
                }
            }
            futures::future::try_join_all(futures).await?;
            Ok(())
        });
        Ok(())
    }
}

/// DataFusion metrics system is pretty limited from an API standpoint. This intermediate struct
/// bridges the gaps that are not satisfied by upstream API for measuring latency.
pub(super) struct LatencyMetric {
    max: Time,
    avg: Time,
    max_latency_micros: AtomicU64,
    sum_latency_micros: AtomicU64,
    count_latency_micros: AtomicU64,
}

impl Drop for LatencyMetric {
    fn drop(&mut self) {
        self.max.add_duration(Duration::from_micros(
            self.max_latency_micros.load(Ordering::Relaxed),
        ));
        self.avg.add_duration(Duration::from_micros(
            self.sum_latency_micros.load(Ordering::Relaxed)
                / self.count_latency_micros.load(Ordering::Relaxed).max(1),
        ));
    }
}

impl LatencyMetric {
    pub(super) fn new(
        name: impl Display,
        builder: impl Fn(MetricBuilder) -> MetricBuilder,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let max = Time::new();
        builder(MetricBuilder::new(metrics)).build(MetricValue::Time {
            name: format!("{name}_max").into(),
            time: max.clone(),
        });
        let avg = Time::new();
        builder(MetricBuilder::new(metrics)).build(MetricValue::Time {
            name: format!("{name}_avg").into(),
            time: avg.clone(),
        });
        Self {
            max,
            avg,
            max_latency_micros: AtomicU64::new(0),
            sum_latency_micros: AtomicU64::new(0),
            count_latency_micros: AtomicU64::new(0),
        }
    }

    fn record(&self, start: &Instant) {
        let micros = start.elapsed().as_micros() as u64;
        self.max_latency_micros.fetch_max(micros, Ordering::Relaxed);
        self.sum_latency_micros.fetch_add(micros, Ordering::Relaxed);
        self.count_latency_micros.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::datafusion_error_to_tonic_status;

    #[test]
    fn worker_publication_error_is_tagged_with_coordinator_context() {
        let task_key = TaskKey {
            query_id: vec![1, 2, 3],
            stage_id: 4,
            task_number: 5,
        };
        let worker_error = exec_datafusion_err!("worker plan decode failed");
        let status = datafusion_error_to_tonic_status(&worker_error);
        let worker = Url::parse("http://worker.example:8080").unwrap();

        let error = plan_publication_status_error(status, &task_key, 0x1234, &worker);
        let message = error.to_string();

        assert!(message.contains(PLAN_PUBLICATION_ERROR_PREFIX));
        assert!(message.contains("worker plan decode failed"));
        assert!(message.contains("plan_fingerprint=0000000000001234"));
        assert!(message.contains("worker=http://worker.example:8080/"));
    }
}
