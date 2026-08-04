use crate::TaskKey;
use datafusion::config::{ConfigField, Visit};
use datafusion::prelude::SessionConfig;
use std::fmt::{Debug, Formatter};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// Terminal outcome of distributed plan publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanPublicationOutcome {
    /// Every task plan was published successfully.
    Success,
    /// At least one task plan exceeded the publication timeout.
    PublicationTimeout,
    /// Publication ended because the query or publication task was cancelled.
    Cancellation,
    /// Publication ended for another reason.
    Other,
}

/// Per-task coordinator measurement emitted after serialization.
#[derive(Clone, Debug)]
pub struct SerializedTaskPlanTelemetry {
    /// Identity of the serialized task plan.
    pub task_key: TaskKey,
    /// Encoded protobuf size sent to one worker.
    pub serialized_bytes: usize,
    /// Wall-clock time spent specializing and serializing this task plan.
    pub serialization_duration: Duration,
}

/// Per-task coordinator measurement emitted after worker acknowledgement.
#[derive(Clone, Debug)]
pub struct PublishedTaskPlanTelemetry {
    /// Identity of the published task plan.
    pub task_key: TaskKey,
    /// Coordinator-side transport and acknowledgement latency with worker work removed.
    pub coordinator_transfer_duration: Duration,
    /// Worker-side session build, decode, hooks, and publication latency.
    pub worker_decode_publication_duration: Duration,
    /// Publication outcome for this task.
    pub outcome: PlanPublicationOutcome,
}

/// Per-task worker measurement emitted after publication finishes.
#[derive(Clone, Debug)]
pub struct WorkerTaskPlanTelemetry {
    /// Identity of the task plan.
    pub task_key: TaskKey,
    /// Encoded protobuf size received by the worker.
    pub serialized_bytes: usize,
    /// Worker-side session build, decode, hooks, and publication latency.
    pub decode_publication_duration: Duration,
    /// Publication outcome for this task.
    pub outcome: PlanPublicationOutcome,
}

/// Aggregate coordinator measurement emitted once per distributed query.
#[derive(Clone, Debug)]
pub struct DistributedPlanTelemetry {
    /// Number of distributed stages.
    pub stage_count: usize,
    /// Number of serialized task plans across all stages.
    pub task_count: usize,
    /// Number of workers available from the worker resolver at execution time.
    pub worker_pool_size: usize,
    /// Largest encoded task-plan protobuf.
    pub max_serialized_task_plan_bytes: usize,
    /// Sum of encoded task-plan bytes across every stage and recipient.
    pub aggregate_fanout_bytes: u64,
    /// Sum of per-task specialization and serialization durations.
    pub serialization_duration: Duration,
    /// Sum of per-task coordinator transfer durations.
    pub coordinator_transfer_duration: Duration,
    /// Sum of worker-reported decode and publication durations.
    pub worker_decode_publication_duration: Duration,
    /// Terminal publication outcome.
    pub outcome: PlanPublicationOutcome,
}

/// Receives observe-only distributed plan measurements.
pub trait DistributedPlanTelemetryObserver: Send + Sync {
    /// Observes one serialized task plan on the coordinator.
    fn task_plan_serialized(&self, _telemetry: &SerializedTaskPlanTelemetry) {}

    /// Observes one acknowledged task plan on the coordinator.
    fn task_plan_published(&self, _telemetry: &PublishedTaskPlanTelemetry) {}

    /// Observes one worker-side plan publication attempt.
    fn worker_task_plan_published(&self, _telemetry: &WorkerTaskPlanTelemetry) {}

    /// Observes the terminal aggregate for one distributed query.
    fn distributed_plan_finished(&self, _telemetry: &DistributedPlanTelemetry) {}
}

#[derive(Clone, Default)]
pub(crate) struct PlanTelemetryObserverExtension(
    pub(crate) Option<Arc<dyn DistributedPlanTelemetryObserver>>,
);

pub(crate) fn notify_observer(notify: impl FnOnce()) {
    let _ = catch_unwind(AssertUnwindSafe(notify));
}

pub(crate) struct DistributedPlanTelemetryState {
    observer: Option<Arc<dyn DistributedPlanTelemetryObserver>>,
    worker_pool_size: AtomicUsize,
    stage_count: AtomicUsize,
    task_count: AtomicUsize,
    max_serialized_task_plan_bytes: AtomicUsize,
    aggregate_fanout_bytes: AtomicU64,
    serialization_nanos: AtomicU64,
    coordinator_transfer_nanos: AtomicU64,
    worker_decode_publication_nanos: AtomicU64,
    reported: AtomicBool,
}

impl DistributedPlanTelemetryState {
    pub(crate) fn new(
        observer: Option<Arc<dyn DistributedPlanTelemetryObserver>>,
        worker_pool_size: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            observer,
            worker_pool_size: AtomicUsize::new(worker_pool_size),
            stage_count: AtomicUsize::new(0),
            task_count: AtomicUsize::new(0),
            max_serialized_task_plan_bytes: AtomicUsize::new(0),
            aggregate_fanout_bytes: AtomicU64::new(0),
            serialization_nanos: AtomicU64::new(0),
            coordinator_transfer_nanos: AtomicU64::new(0),
            worker_decode_publication_nanos: AtomicU64::new(0),
            reported: AtomicBool::new(false),
        })
    }

    pub(crate) fn stage_started(&self) {
        self.stage_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_worker_pool_size(&self, worker_pool_size: usize) {
        self.worker_pool_size
            .store(worker_pool_size, Ordering::Relaxed);
    }

    pub(crate) fn task_serialized(&self, telemetry: SerializedTaskPlanTelemetry) {
        self.task_count.fetch_add(1, Ordering::Relaxed);
        self.max_serialized_task_plan_bytes
            .fetch_max(telemetry.serialized_bytes, Ordering::Relaxed);
        self.aggregate_fanout_bytes
            .fetch_add(telemetry.serialized_bytes as u64, Ordering::Relaxed);
        self.serialization_nanos.fetch_add(
            duration_nanos(telemetry.serialization_duration),
            Ordering::Relaxed,
        );
        if let Some(observer) = &self.observer {
            notify_observer(|| observer.task_plan_serialized(&telemetry));
        }
    }

    pub(crate) fn task_published(&self, telemetry: PublishedTaskPlanTelemetry) {
        self.coordinator_transfer_nanos.fetch_add(
            duration_nanos(telemetry.coordinator_transfer_duration),
            Ordering::Relaxed,
        );
        self.worker_decode_publication_nanos.fetch_add(
            duration_nanos(telemetry.worker_decode_publication_duration),
            Ordering::Relaxed,
        );
        if let Some(observer) = &self.observer {
            notify_observer(|| observer.task_plan_published(&telemetry));
        }
    }

    pub(crate) fn finish(&self, outcome: PlanPublicationOutcome) {
        if self.reported.swap(true, Ordering::AcqRel) {
            return;
        }
        let telemetry = DistributedPlanTelemetry {
            stage_count: self.stage_count.load(Ordering::Relaxed),
            task_count: self.task_count.load(Ordering::Relaxed),
            worker_pool_size: self.worker_pool_size.load(Ordering::Relaxed),
            max_serialized_task_plan_bytes: self
                .max_serialized_task_plan_bytes
                .load(Ordering::Relaxed),
            aggregate_fanout_bytes: self.aggregate_fanout_bytes.load(Ordering::Relaxed),
            serialization_duration: Duration::from_nanos(
                self.serialization_nanos.load(Ordering::Relaxed),
            ),
            coordinator_transfer_duration: Duration::from_nanos(
                self.coordinator_transfer_nanos.load(Ordering::Relaxed),
            ),
            worker_decode_publication_duration: Duration::from_nanos(
                self.worker_decode_publication_nanos.load(Ordering::Relaxed),
            ),
            outcome,
        };
        if let Some(observer) = &self.observer {
            notify_observer(|| observer.distributed_plan_finished(&telemetry));
        }
    }
}

pub(crate) struct DistributedPlanTelemetryGuard {
    state: Arc<DistributedPlanTelemetryState>,
}

impl DistributedPlanTelemetryGuard {
    pub(crate) fn new(state: Arc<DistributedPlanTelemetryState>) -> Self {
        Self { state }
    }

    pub(crate) fn finish(self, outcome: PlanPublicationOutcome) {
        self.state.finish(outcome);
    }
}

impl Drop for DistributedPlanTelemetryGuard {
    fn drop(&mut self) {
        self.state.finish(PlanPublicationOutcome::Cancellation);
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingObserver {
        finished: Mutex<Vec<DistributedPlanTelemetry>>,
    }

    impl DistributedPlanTelemetryObserver for RecordingObserver {
        fn distributed_plan_finished(&self, telemetry: &DistributedPlanTelemetry) {
            self.finished.lock().unwrap().push(telemetry.clone());
        }
    }

    fn task_key(task_number: u64) -> TaskKey {
        TaskKey {
            query_id: vec![1, 2, 3],
            stage_id: 4,
            task_number,
        }
    }

    #[test]
    fn aggregates_query_measurements_once() {
        let observer = Arc::new(RecordingObserver::default());
        let state = DistributedPlanTelemetryState::new(
            Some(observer.clone() as Arc<dyn DistributedPlanTelemetryObserver>),
            7,
        );
        state.stage_started();
        state.stage_started();
        state.task_serialized(SerializedTaskPlanTelemetry {
            task_key: task_key(0),
            serialized_bytes: 100,
            serialization_duration: Duration::from_micros(10),
        });
        state.task_serialized(SerializedTaskPlanTelemetry {
            task_key: task_key(1),
            serialized_bytes: 250,
            serialization_duration: Duration::from_micros(20),
        });
        state.task_published(PublishedTaskPlanTelemetry {
            task_key: task_key(0),
            coordinator_transfer_duration: Duration::from_micros(30),
            worker_decode_publication_duration: Duration::from_micros(40),
            outcome: PlanPublicationOutcome::Success,
        });
        state.finish(PlanPublicationOutcome::Success);
        state.finish(PlanPublicationOutcome::Other);

        let finished = observer.finished.lock().unwrap();
        assert_eq!(finished.len(), 1);
        let telemetry = &finished[0];
        assert_eq!(telemetry.stage_count, 2);
        assert_eq!(telemetry.task_count, 2);
        assert_eq!(telemetry.worker_pool_size, 7);
        assert_eq!(telemetry.max_serialized_task_plan_bytes, 250);
        assert_eq!(telemetry.aggregate_fanout_bytes, 350);
        assert_eq!(telemetry.serialization_duration, Duration::from_micros(30));
        assert_eq!(
            telemetry.coordinator_transfer_duration,
            Duration::from_micros(30)
        );
        assert_eq!(
            telemetry.worker_decode_publication_duration,
            Duration::from_micros(40)
        );
        assert_eq!(telemetry.outcome, PlanPublicationOutcome::Success);
    }

    #[test]
    fn dropped_guard_reports_cancellation() {
        let observer = Arc::new(RecordingObserver::default());
        let state = DistributedPlanTelemetryState::new(
            Some(observer.clone() as Arc<dyn DistributedPlanTelemetryObserver>),
            3,
        );
        drop(DistributedPlanTelemetryGuard::new(state));

        let finished = observer.finished.lock().unwrap();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].outcome, PlanPublicationOutcome::Cancellation);
    }

    struct PanickingObserver;

    impl DistributedPlanTelemetryObserver for PanickingObserver {
        fn distributed_plan_finished(&self, _: &DistributedPlanTelemetry) {
            panic!("injected observer panic");
        }
    }

    #[test]
    fn observer_panics_do_not_change_publication_behavior() {
        let observer: Arc<dyn DistributedPlanTelemetryObserver> = Arc::new(PanickingObserver);
        let state = DistributedPlanTelemetryState::new(Some(observer), 1);
        state.finish(PlanPublicationOutcome::Success);
    }
}

impl Debug for PlanTelemetryObserverExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("PlanTelemetryObserverExtension")
    }
}

impl ConfigField for PlanTelemetryObserverExtension {
    fn visit<V: Visit>(&self, _: &mut V, _: &str, _: &'static str) {}

    fn set(&mut self, _: &str, _: &str) -> datafusion::common::Result<()> {
        Ok(())
    }
}

/// Installs a coordinator-side observer in a distributed session configuration.
pub fn set_distributed_plan_telemetry_observer(
    config: &mut SessionConfig,
    observer: Arc<dyn DistributedPlanTelemetryObserver>,
) {
    let options = config.options_mut();
    if let Some(distributed) = options.extensions.get_mut::<crate::DistributedConfig>() {
        distributed.__private_plan_telemetry_observer =
            PlanTelemetryObserverExtension(Some(observer));
    } else {
        crate::config_extension_ext::set_distributed_option_extension(
            config,
            crate::DistributedConfig {
                __private_plan_telemetry_observer: PlanTelemetryObserverExtension(Some(observer)),
                ..Default::default()
            },
        );
    }
}
