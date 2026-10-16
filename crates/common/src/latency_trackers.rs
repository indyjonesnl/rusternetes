//! `LatencyTrackers` (`staging/src/k8s.io/apiserver/pkg/endpoints/request/webhook_duration.go`):
//! per-request accumulators of the time spent in components whose latency the
//! `request_sli_duration_seconds` / `request_slo_duration_seconds` metrics
//! subtract (`MonitorRequest`, `endpoints/metrics/metrics.go:619-624`).
//!
//! All ten `LatencyTrackers` fields exist. The mutating/validating webhook and
//! APF queue-wait trackers feed `MonitorRequest`; authentication feeds the
//! `WithAuthentication` filter; every non-zero tracker feeds
//! `AuditAnnotationsFromLatencyTrackers`. Storage, transform, serialization,
//! response-write and decode are declared with their `Track*` helpers but their
//! call sites are not wired yet (tracked as follow-ups). Authorization is
//! retained per request (max) and is not subtracted by `MonitorRequest` from
//! the SLI latency.

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

/// Per-request latency trackers (webhook_duration.go:111-195).
#[derive(Debug)]
pub struct LatencyTrackers {
    /// Mutating webhooks run sequentially: summed.
    pub mutating_webhook: DurationTracker,
    /// Validating webhooks run in parallel: max.
    pub validating_webhook: DurationTracker,
    /// Priority & fairness queue wait: max.
    pub apf_queue_wait: DurationTracker,
    /// Authentication: summed (:99).
    pub authentication: DurationTracker,
    /// Authorization: max (:100).
    pub authorization: DurationTracker,
    /// Storage round trips: summed (:102).
    pub storage: DurationTracker,
    /// Response transformation: summed (:103).
    pub transform: DurationTracker,
    /// Response serialization: summed (:104).
    pub serialization: DurationTracker,
    /// Writes to the response writer: summed (:105).
    pub response_write: DurationTracker,
    /// Decode of storage objects: summed (:106).
    pub decode: DurationTracker,
}

impl LatencyTrackers {
    /// `WithLatencyTrackersAndCustomClock`'s initial value (:184-196).
    pub fn new() -> Self {
        Self {
            mutating_webhook: DurationTracker::sum(),
            validating_webhook: DurationTracker::max(),
            apf_queue_wait: DurationTracker::max(),
            authentication: DurationTracker::sum(),
            authorization: DurationTracker::max(),
            storage: DurationTracker::sum(),
            transform: DurationTracker::sum(),
            serialization: DurationTracker::sum(),
            response_write: DurationTracker::sum(),
            decode: DurationTracker::sum(),
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

fn track(f: impl FnOnce(&LatencyTrackers)) {
    if let Some(t) = latency_trackers_from_context() {
        f(&t);
    }
}

/// `TrackStorageLatency` (webhook_duration.go:239-243).
pub fn track_storage_latency(d: Duration) {
    track(|t| t.storage.track_duration(d));
}

/// `TrackResponseWriteLatency`.
pub fn track_response_write_latency(d: Duration) {
    track(|t| t.response_write.track_duration(d));
}

/// `TrackAuthenticationLatency`.
pub fn track_authentication_latency(d: Duration) {
    track(|t| t.authentication.track_duration(d));
}

/// `TrackAuthorizationLatency`.
pub fn track_authorization_latency(d: Duration) {
    track(|t| t.authorization.track_duration(d));
}

/// `TrackDecodeLatency`.
pub fn track_decode_latency(d: Duration) {
    track(|t| t.decode.track_duration(d));
}

/// `TrackTransformResponseObjectLatency`: time `f` when trackers are
/// installed, else just run it.
pub async fn track_transform_response_object_latency<F: Future>(f: F) -> F::Output {
    match latency_trackers_from_context() {
        Some(t) => t.transform.track(f).await,
        None => f.await,
    }
}

/// `TrackSerializeResponseObjectLatency`.
pub async fn track_serialize_response_object_latency<F: Future>(f: F) -> F::Output {
    match latency_trackers_from_context() {
        Some(t) => t.serialization.track(f).await,
        None => f.await,
    }
}

/// `AuditAnnotationsFromLatencyTrackers`: one annotation per non-zero
/// tracker, valued with Go's `Duration.String`. `None` when the request
/// carries no trackers (`!ok` -> nil).
pub fn audit_annotations_from_latency_trackers() -> Option<Vec<(String, String)>> {
    let t = latency_trackers_from_context()?;
    Some(annotations_from(&t))
}

/// The map-building half of `AuditAnnotationsFromLatencyTrackers`.
pub fn annotations_from(t: &LatencyTrackers) -> Vec<(String, String)> {
    let table: [(&str, &DurationTracker); 10] = [
        (
            "apiserver.latency.k8s.io/transform-response-object",
            &t.transform,
        ),
        ("apiserver.latency.k8s.io/etcd", &t.storage),
        (
            "apiserver.latency.k8s.io/serialize-response-object",
            &t.serialization,
        ),
        ("apiserver.latency.k8s.io/response-write", &t.response_write),
        (
            "apiserver.latency.k8s.io/mutating-webhook",
            &t.mutating_webhook,
        ),
        (
            "apiserver.latency.k8s.io/validating-webhook",
            &t.validating_webhook,
        ),
        ("apiserver.latency.k8s.io/decode-response-object", &t.decode),
        ("apiserver.latency.k8s.io/apf-queue-wait", &t.apf_queue_wait),
        ("apiserver.latency.k8s.io/authentication", &t.authentication),
        ("apiserver.latency.k8s.io/authorization", &t.authorization),
    ];
    table
        .into_iter()
        .filter_map(|(k, tr)| {
            let l = tr.get_latency();
            (!l.is_zero()).then(|| {
                (
                    k.to_string(),
                    crate::go_duration::format_go_duration(l.as_nanos() as i64),
                )
            })
        })
        .collect()
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

    /// `TestLatencyTrackersFrom` extended to the remaining trackers plus the
    /// annotation map (webhook_duration.go:289-345): zero trackers are
    /// omitted, sum/max aggregation per tracker, Go duration formatting.
    #[test]
    fn audit_annotations_cover_every_tracker() {
        let t = LatencyTrackers::new();
        assert!(annotations_from(&t).is_empty());
        for d in [Duration::from_millis(100), Duration::from_millis(250)] {
            t.authentication.track_duration(d);
            t.authorization.track_duration(d);
            t.storage.track_duration(d);
            t.transform.track_duration(d);
            t.serialization.track_duration(d);
            t.response_write.track_duration(d);
            t.decode.track_duration(d);
        }
        let m: std::collections::BTreeMap<_, _> = annotations_from(&t).into_iter().collect();
        assert_eq!(m.len(), 7);
        assert_eq!(m["apiserver.latency.k8s.io/authentication"], "350ms");
        assert_eq!(m["apiserver.latency.k8s.io/authorization"], "250ms");
        assert_eq!(m["apiserver.latency.k8s.io/etcd"], "350ms");
        assert_eq!(
            m["apiserver.latency.k8s.io/transform-response-object"],
            "350ms"
        );
        assert_eq!(
            m["apiserver.latency.k8s.io/serialize-response-object"],
            "350ms"
        );
        assert_eq!(m["apiserver.latency.k8s.io/response-write"], "350ms");
        assert_eq!(
            m["apiserver.latency.k8s.io/decode-response-object"],
            "350ms"
        );
    }

    #[tokio::test]
    async fn annotations_none_without_trackers_and_helpers_route() {
        assert!(audit_annotations_from_latency_trackers().is_none());
        let t = Arc::new(LatencyTrackers::new());
        with_latency_trackers(t.clone(), async {
            track_authentication_latency(Duration::from_millis(1));
            track_storage_latency(Duration::from_millis(2));
            track_decode_latency(Duration::from_millis(3));
            track_response_write_latency(Duration::from_millis(4));
            track_authorization_latency(Duration::from_millis(5));
            track_serialize_response_object_latency(tokio::time::sleep(Duration::from_millis(5)))
                .await;
            track_transform_response_object_latency(async {}).await;
            assert!(audit_annotations_from_latency_trackers().unwrap().len() >= 6);
        })
        .await;
        assert_eq!(t.storage.get_latency(), Duration::from_millis(2));
        assert!(t.serialization.get_latency() >= Duration::from_millis(5));
    }
}
