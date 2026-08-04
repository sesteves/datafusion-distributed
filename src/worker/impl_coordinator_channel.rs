use crate::common::deserialize_uuid;
use crate::plan_telemetry::{WorkerTaskPlanTelemetry, notify_observer};
use crate::protobuf::datafusion_error_to_tonic_status;
use crate::work_unit_feed::{RemoteWorkUnitFeedRegistry, set_work_unit_received_time};
use crate::worker::LocalWorkerContext;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::worker::generated::worker::worker_service_server::WorkerService;
use crate::worker::generated::worker::{
    CoordinatorToWorkerMsg, WorkerToCoordinatorMsg, worker_to_coordinator_msg,
};
use crate::worker::single_write_multi_read::SingleWriteMultiRead;
use crate::worker::task_data::TaskDataMetrics;
use crate::worker::worker_service::{CancelledTaskKeys, ResultTaskData};
use crate::{
    DistributedCodec, DistributedConfig, DistributedExt, DistributedTaskContext, TaskData, Worker,
    WorkerQueryContext,
};
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::{FutureExt, StreamExt, TryStreamExt};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, Streaming};
use url::Url;

const WORKER_PUBLICATION_NANOS_HEADER: &str = "x-datafusion-worker-plan-publication-nanos";

impl Worker {
    pub(super) async fn impl_coordinator_channel(
        &self,
        request: Request<Streaming<CoordinatorToWorkerMsg>>,
    ) -> Result<Response<<Worker as WorkerService>::CoordinatorChannelStream>, Status> {
        let (grpc_headers, _ext, mut body) = request.into_parts();

        // The first message must be a SetPlanRequest.
        let Some(msg) = body.next().await else {
            return Err(Status::internal("Empty Coordinator stream"));
        };
        let Some(Inner::SetPlanRequest(request)) = msg?.inner else {
            return Err(Status::internal(
                "First Coordinator message must be SetPlanRequest",
            ));
        };
        let key = request.task_key.ok_or_else(missing("task_key"))?;
        let publication_start = Instant::now();
        let serialized_bytes = request.plan_proto.len();
        if let Err(error) =
            ensure_plan_not_cancelled(&self.task_data_entries, &self.cancelled_task_keys, &key)
                .await
        {
            self.observe_worker_plan(
                key,
                serialized_bytes,
                publication_start.elapsed(),
                crate::PlanPublicationOutcome::Cancellation,
            );
            return Err(error);
        }

        let entry = self
            .task_data_entries
            .get_with(key.clone(), async { Default::default() })
            .await;

        let mut remote_work_unit_feed_registry = RemoteWorkUnitFeedRegistry::default();
        for WorkUnitFeedDeclaration { id, partitions } in &request.work_unit_feed_declarations {
            if let Ok(id) = deserialize_uuid(id) {
                remote_work_unit_feed_registry.add(id, *partitions as usize);
            }
        }

        let (metrics_tx, metrics_rx) = oneshot::channel();

        let task_data = || async {
            let headers = grpc_headers.into_headers();

            let mut cfg = SessionConfig::default()
                .with_extension(Arc::new(remote_work_unit_feed_registry.receivers))
                .with_extension(Arc::new(DistributedTaskContext {
                    task_index: key.task_number as usize,
                    task_count: request.task_count as usize,
                }))
                .with_extension(Arc::new(LocalWorkerContext {
                    task_data_entries: Arc::clone(&self.task_data_entries),
                    self_url: Url::parse(&request.target_worker_url)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                }))
                .with_distributed_option_extension_from_headers::<DistributedConfig>(&headers)?;

            let d_cfg = DistributedConfig::from_config_options(cfg.options())?;
            let shuffle_batch_size = d_cfg.shuffle_batch_size;
            let collect_metrics = d_cfg.collect_metrics;
            if shuffle_batch_size != 0 {
                cfg = cfg.with_batch_size(shuffle_batch_size);
            }

            let session_state = self
                .session_builder
                .build_session_state(WorkerQueryContext {
                    builder: SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(cfg)
                        .with_runtime_env(Arc::clone(&self.runtime)),
                    headers,
                })
                .await?;

            let codec = DistributedCodec::new_combined_with_user(session_state.config());
            let task_ctx = session_state.task_ctx();
            let proto_node = PhysicalPlanNode::try_decode(request.plan_proto.as_ref())?;
            let mut plan = proto_node.try_into_physical_plan(&task_ctx, &codec)?;

            for hook in self.hooks.on_plan.iter() {
                plan = hook(plan)
            }

            // Initialize partition count to the number of partitions in the stage
            let total_partitions = plan.properties().partitioning.partition_count();
            Ok::<_, DataFusionError>(TaskData {
                base_plan: plan,
                final_plan: Arc::new(OnceLock::new()),
                task_ctx,
                num_partitions_remaining: Arc::new(AtomicUsize::new(total_partitions)),
                metrics_tx: match collect_metrics {
                    true => Arc::new(std::sync::Mutex::new(Some(metrics_tx))),
                    false => Arc::new(std::sync::Mutex::new(None)),
                },
                task_data_metrics: Arc::new(TaskDataMetrics::new(request.query_start_time_ns)),
            })
        };

        if let Err(error) = publish_task_data(&entry, &key, task_data().await.map_err(Arc::new)) {
            self.observe_worker_plan(
                key,
                serialized_bytes,
                publication_start.elapsed(),
                crate::PlanPublicationOutcome::Other,
            );
            return Err(error);
        }
        if let Err(error) =
            ensure_plan_not_cancelled(&self.task_data_entries, &self.cancelled_task_keys, &key)
                .await
        {
            self.observe_worker_plan(
                key,
                serialized_bytes,
                publication_start.elapsed(),
                crate::PlanPublicationOutcome::Cancellation,
            );
            return Err(error);
        }
        let publication_duration = publication_start.elapsed();
        self.observe_worker_plan(
            key.clone(),
            serialized_bytes,
            publication_duration,
            crate::PlanPublicationOutcome::Success,
        );

        // Continue reading remaining messages (work unit feed data) in the background.
        let mut work_unit_senders = remote_work_unit_feed_registry.senders;
        let task_data_entries = Arc::clone(&self.task_data_entries);
        let cancelled_task_keys = Arc::clone(&self.cancelled_task_keys);
        let cancellation_key = key.clone();
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            let mut body = body.map_ok(set_work_unit_received_time);
            while let Some(Ok(msg)) = body.next().await {
                match msg.inner {
                    Some(Inner::WorkUnitBatch(msg)) => {
                        for msg in msg.batch {
                            let Ok(id) = deserialize_uuid(&msg.id) else {
                                continue;
                            };
                            let partition = msg.partition as usize;
                            let Some(tx) = work_unit_senders.get(&(id, partition)) else {
                                continue;
                            };
                            if tx.send(Ok(msg)).is_err() {
                                work_unit_senders.remove(&(id, partition));
                            }
                        }
                    }
                    Some(Inner::CancelPlanRequest(request)) => {
                        let key = request.task_key.as_ref().unwrap_or(&cancellation_key);
                        cancel_published_plan(&task_data_entries, &cancelled_task_keys, key).await;
                        break;
                    }
                    Some(Inner::SetPlanRequest(_)) | None => {}
                }
            }
        });

        // Stream back the metrics once the task finishes executing.
        // The oneshot receiver resolves when impl_execute_task sends the collected
        // metrics after all partitions have finished or been dropped.
        let metrics_stream = metrics_rx.into_stream();
        let metrics_stream = metrics_stream.filter_map(|task_metrics| async move {
            match task_metrics {
                Ok(task_metrics) => Some(WorkerToCoordinatorMsg {
                    inner: Some(worker_to_coordinator_msg::Inner::TaskMetrics(task_metrics)),
                }),
                Err(_) => None, // channel dropped without sending any message
            }
        });
        let mut response = Response::new(metrics_stream.map(Ok).boxed());
        if let Ok(value) = tonic::metadata::MetadataValue::try_from(
            publication_duration
                .as_nanos()
                .min(u64::MAX as u128)
                .to_string(),
        ) {
            response
                .metadata_mut()
                .insert(WORKER_PUBLICATION_NANOS_HEADER, value);
        }
        Ok(response)
    }

    fn observe_worker_plan(
        &self,
        task_key: crate::TaskKey,
        serialized_bytes: usize,
        decode_publication_duration: std::time::Duration,
        outcome: crate::PlanPublicationOutcome,
    ) {
        let Some(observer) = &self.plan_telemetry_observer else {
            return;
        };
        let telemetry = WorkerTaskPlanTelemetry {
            task_key,
            serialized_bytes,
            decode_publication_duration,
            outcome,
        };
        notify_observer(|| observer.worker_task_plan_published(&telemetry));
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}

fn cancelled(key: &crate::TaskKey) -> Status {
    Status::cancelled(format!("Plan publication cancelled for TaskKey {key:?}"))
}

async fn ensure_plan_not_cancelled(
    task_data_entries: &crate::worker::worker_service::TaskDataEntries,
    cancelled_task_keys: &CancelledTaskKeys,
    key: &crate::TaskKey,
) -> Result<(), Status> {
    if cancelled_task_keys.contains_key(key) {
        task_data_entries.invalidate(key).await;
        return Err(cancelled(key));
    }
    Ok(())
}

fn publish_task_data(
    entry: &SingleWriteMultiRead<ResultTaskData>,
    key: &crate::TaskKey,
    task_data: ResultTaskData,
) -> Result<(), Status> {
    let publication_error = task_data.as_ref().err().cloned();
    entry.write(task_data).map_err(|_| {
        Status::internal(format!(
            "Logic error while setting plan for TaskKey {key:?}: the plan was set twice. This is a bug in datafusion-distributed, please report it."
        ))
    })?;
    if let Some(error) = publication_error {
        return Err(datafusion_error_to_tonic_status(error.as_ref()));
    }
    Ok(())
}

pub(crate) async fn cancel_published_plan(
    task_data_entries: &crate::worker::worker_service::TaskDataEntries,
    cancelled_task_keys: &CancelledTaskKeys,
    key: &crate::TaskKey,
) {
    cancelled_task_keys.insert(key.clone(), ()).await;
    task_data_entries.invalidate(key).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::tonic_status_to_datafusion_error;
    use datafusion::common::exec_datafusion_err;

    #[test]
    fn producer_failure_is_published_and_returned_to_coordinator() {
        let entry = SingleWriteMultiRead::default();
        let key = crate::TaskKey {
            query_id: vec![1, 2, 3],
            stage_id: 7,
            task_number: 11,
        };

        let status = publish_task_data(
            &entry,
            &key,
            Err(Arc::new(exec_datafusion_err!(
                "injected plan decode failure"
            ))),
        )
        .unwrap_err();

        let coordinator_error = tonic_status_to_datafusion_error(status).unwrap();
        assert!(
            coordinator_error
                .to_string()
                .contains("injected plan decode failure")
        );
        let reader_error = entry.read_now().unwrap().unwrap_err();
        assert!(
            reader_error
                .to_string()
                .contains("injected plan decode failure")
        );
    }

    #[tokio::test]
    async fn cancellation_invalidates_published_plan() {
        let worker = Worker::default();
        let key = crate::TaskKey {
            query_id: vec![1, 2, 3],
            stage_id: 7,
            task_number: 11,
        };
        worker
            .task_data_entries
            .insert(key.clone(), Arc::new(SingleWriteMultiRead::default()))
            .await;

        cancel_published_plan(&worker.task_data_entries, &worker.cancelled_task_keys, &key).await;

        assert!(worker.task_data_entries.get(&key).await.is_none());
        assert!(worker.cancelled_task_keys.contains_key(&key));
    }

    #[tokio::test]
    async fn cancellation_before_publication_rejects_the_plan() {
        let worker = Worker::default();
        let key = crate::TaskKey {
            query_id: vec![4, 5, 6],
            stage_id: 8,
            task_number: 12,
        };

        cancel_published_plan(&worker.task_data_entries, &worker.cancelled_task_keys, &key).await;
        worker
            .task_data_entries
            .insert(key.clone(), Arc::new(SingleWriteMultiRead::default()))
            .await;

        let status =
            ensure_plan_not_cancelled(&worker.task_data_entries, &worker.cancelled_task_keys, &key)
                .await
                .unwrap_err();

        assert_eq!(status.code(), tonic::Code::Cancelled);
        assert!(worker.task_data_entries.get(&key).await.is_none());
    }
}
