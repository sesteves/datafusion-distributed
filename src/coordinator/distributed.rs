use crate::common::{require_one_child, serialize_uuid};
use crate::coordinator::metrics_store::MetricsStore;
use crate::coordinator::prepare_static_plan::prepare_static_plan;
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
/// Before executing it, two modifications are lazily performed on the plan:
/// 1. Assigns worker URLs to all the stages. Unless explicitly set in
///    [crate::TaskEstimator::route_tasks], a random set of URLs are sampled from the
///    channel resolver and assigned to each task in each stage.
/// 2. Encodes all the plans in protobuf format so that network boundary nodes can send them
///    over the wire.
#[derive(Debug)]
pub struct DistributedExec {
    plan: Arc<dyn ExecutionPlan>,
    prepared_plan: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
    metrics: ExecutionPlanMetricsSet,
    pub(crate) metrics_store: Option<Arc<MetricsStore>>,
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

    /// Returns the plan which is lazily prepared on `execute()` and actually gets executed.
    /// It is updated on every call to `execute()`. Returns an error if `.execute()` has not been
    /// called.
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

        let PreparedPlan {
            head_stage,
            join_set,
            plan_publications,
            plan_cancellations,
            plans_published_tx,
        } = prepare_static_plan(&self.plan, &self.metrics, &self.metrics_store, &context)?;
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
    use super::*;
    use datafusion::common::exec_datafusion_err;
    use std::sync::atomic::{AtomicBool, Ordering};

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
