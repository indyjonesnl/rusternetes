//! `LatencyTrackers` (`staging/src/k8s.io/apiserver/pkg/endpoints/request/webhook_duration.go`):
//! per-request accumulators of the time spent in components whose latency the
//! `request_sli_duration_seconds` / `request_slo_duration_seconds` metrics
//! subtract (`MonitorRequest`, `endpoints/metrics/metrics.go:619-624`).
//!
//! Only the trackers `MonitorRequest` reads are ported: the mutating and
//! validating webhook trackers and the APF queue-wait tracker.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Aggregate {
    Sum,
    Max,
}

/// `DurationTracker` (webhook_duration.go:40-60), safe for concurrent use.
#[derive(Debug)]
pub struct DurationTracker {
    aggregate: Aggregate,
    latency: Mutex<Duration>,
}

impl DurationTracker {
    /// `newSumLatencyTracker` (:97-102).
    fn sum() -> Self {
        Self {
            aggregate: Aggregate::Sum,
            latency: Mutex::new(Duration::ZERO),
        }
    }

    /// `newMaxLatencyTracker` (:104-109).
    fn max() -> Self {
        Self {
            aggregate: Aggregate::Max,
            latency: Mutex::new(Duration::ZERO),
        }
    }

    /// `TrackDuration` (:79-84).
    pub fn track_duration(&self, d: Duration) {
        let mut l = self.latency.lock().unwrap_or_else(|e| e.into_inner());
        *l = match self.aggregate {
            Aggregate::Sum => *l + d,
            Aggregate::Max => (*l).max(d),
        };
    }

    /// `Track` (:62-77): time `f` and aggregate its duration.
    pub async fn track<F: Future>(&self, f: F) -> F::Output {
        let started = Instant::now();
        let out = f.await;
        self.track_duration(started.elapsed());
        out
    }

    /// `GetLatency` (:86-91).
    pub fn get_latency(&self) -> Duration {
        *self.latency.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The trackers `MonitorRequest` subtracts (webhook_duration.go:111-195).
#[derive(Debug)]
pub struct LatencyTrackers {
    /// Mutating webhooks run sequentially: summed.
    pub mutating_webhook: DurationTracker,
    /// Validating webhooks run in parallel: max.
    pub validating_webhook: DurationTracker,
    /// Priority & fairness queue wait: max.
    pub apf_queue_wait: DurationTracker,
}

impl LatencyTrackers {
    /// `WithLatencyTrackersAndCustomClock`'s initial value (:184-196).
    pub fn new() -> Self {
        Self {
            mutating_webhook: DurationTracker::sum(),
            validating_webhook: DurationTracker::max(),
            apf_queue_wait: DurationTracker::max(),
        }
    }

    /// The time `MonitorRequest` excludes from the SLI latency
    /// (metrics.go:621): webhooks and APF queue wait.
    pub fn excluded_latency(&self) -> Duration {
        self.mutating_webhook.get_latency()
            + self.validating_webhook.get_latency()
            + self.apf_queue_wait.get_latency()
    }
}

impl Default for LatencyTrackers {
    fn default() -> Self {
        Self::new()
    }
}

tokio::task_local! {
    static TRACKERS: Arc<LatencyTrackers>;
}

/// `WithLatencyTrackers` (:197-205): serve `fut` with `trackers` installed.
pub async fn with_latency_trackers<F: Future>(trackers: Arc<LatencyTrackers>, fut: F) -> F::Output {
    TRACKERS.scope(trackers, fut).await
}

/// `LatencyTrackersFrom` (:207-212). Clone the `Arc` before `tokio::spawn`:
/// task-locals do not cross a spawn.
pub fn latency_trackers_from_context() -> Option<Arc<LatencyTrackers>> {
    TRACKERS.try_with(Arc::clone).ok()
}

/// `TrackAPFQueueWaitLatency` (:270-276).
pub fn track_apf_queue_wait_latency(d: Duration) {
    if let Some(t) = latency_trackers_from_context() {
        t.apf_queue_wait.track_duration(d);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `TestLatencyTrackersFrom` (webhook_duration_test.go:27-77).
    #[test]
    fn latency_trackers_sum_and_max() {
        let durations = [100u64, 200, 300, 200, 400, 300, 100];
        let wd = LatencyTrackers::new();
        assert_eq!(wd.excluded_latency(), Duration::ZERO);
        for d in durations {
            let d = Duration::from_nanos(d);
            wd.mutating_webhook.track_duration(d);
            wd.validating_webhook.track_duration(d);
            wd.apf_queue_wait.track_duration(d);
        }
        assert_eq!(
            wd.mutating_webhook.get_latency(),
            Duration::from_nanos(1600)
        );
        assert_eq!(
            wd.validating_webhook.get_latency(),
            Duration::from_nanos(400)
        );
        assert_eq!(wd.apf_queue_wait.get_latency(), Duration::from_nanos(400));
    }

    #[tokio::test]
    async fn context_lookup_and_apf_helper() {
        assert!(latency_trackers_from_context().is_none());
        // Without trackers the helper is a no-op (upstream `ok` check).
        track_apf_queue_wait_latency(Duration::from_millis(5));
        let t = Arc::new(LatencyTrackers::new());
        with_latency_trackers(t.clone(), async {
            assert!(latency_trackers_from_context().is_some());
            track_apf_queue_wait_latency(Duration::from_millis(5));
            track_apf_queue_wait_latency(Duration::from_millis(3));
        })
        .await;
        assert_eq!(t.apf_queue_wait.get_latency(), Duration::from_millis(5));
    }

    #[tokio::test]
    async fn track_future_measures_elapsed() {
        let t = LatencyTrackers::new();
        t.mutating_webhook
            .track(tokio::time::sleep(Duration::from_millis(20)))
            .await;
        t.mutating_webhook
            .track(tokio::time::sleep(Duration::from_millis(30)))
            .await;
        assert!(t.mutating_webhook.get_latency() >= Duration::from_millis(50));
    }
}
