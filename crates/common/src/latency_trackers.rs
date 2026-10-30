//! `LatencyTrackers` (`staging/src/k8s.io/apiserver/pkg/endpoints/request/webhook_duration.go`):
//! per-request accumulators of the time spent in components whose latency the
//! `request_sli_duration_seconds` / `request_slo_duration_seconds` metrics
//! subtract (`MonitorRequest`, `endpoints/metrics/metrics.go:619-624`).
//!
//! All ten `LatencyTrackers` fields exist. The mutating/validating webhook and
//! APF queue-wait trackers feed `MonitorRequest`; authentication feeds the
//! `WithAuthentication` filter; every non-zero tracker feeds
//! `AuditAnnotationsFromLatencyTrackers`. Storage (every etcd/rhino round
//! trip), list-item decode, response transform and response serialization are
//! wired at their call sites. Response-write is not (axum writes the body
//! after the handler chain returns; tracked as a follow-up). Authorization is
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

/// Synchronous [`track_transform_response_object_latency`]: Go's
/// `TrackTransformResponseObjectLatency(ctx, func())` takes a plain func, and
/// most of our transform code is synchronous.
pub fn track_transform_response_object_latency_sync<R>(f: impl FnOnce() -> R) -> R {
    match latency_trackers_from_context() {
        Some(t) => {
            let started = Instant::now();
            let out = f();
            t.transform.track_duration(started.elapsed());
            out
        }
        None => f(),
    }
}

/// Synchronous [`track_serialize_response_object_latency`]
/// (`TrackSerializeResponseObjectLatency(ctx, func())`, webhook_duration.go:232).
pub fn track_serialize_response_object_latency_sync<R>(f: impl FnOnce() -> R) -> R {
    match latency_trackers_from_context() {
        Some(t) => {
            let started = Instant::now();
            let out = f();
            t.serialization.track_duration(started.elapsed());
            out
        }
        None => f(),
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
