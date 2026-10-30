//! Prometheus series for the pod garbage collector.
//!
//! Port of `pkg/controller/podgc/metrics/metrics.go` (release-1.35): subsystem
//! `pod_gc_collector`, counters `force_delete_pods_total` and
//! `force_delete_pod_errors_total`, both labelled `namespace` and `reason`.
//! Upstream registers them in `legacyregistry` once (`registerMetrics
//! sync.Once`); a process-global [`Registry`] plays that role here.

use prometheus::{Encoder, IntCounterVec, Opts, Registry, TextEncoder};
use std::sync::LazyLock;

const SUBSYSTEM: &str = "pod_gc_collector";

/// `PodGCReasonTerminated` (metrics.go).
pub const REASON_TERMINATED: &str = "terminated";
/// `PodGCReasonTerminatingOutOfService`: terminating pod on a not-ready node
/// carrying the `node.kubernetes.io/out-of-service` taint.
pub const REASON_TERMINATING_OUT_OF_SERVICE: &str = "out-of-service";
/// `PodGCReasonOrphaned`: the pod's node has been deleted.
pub const REASON_ORPHANED: &str = "orphaned";
/// `PodGCReasonTerminatingUnscheduled`: terminating and never scheduled.
pub const REASON_TERMINATING_UNSCHEDULED: &str = "unscheduled";

struct Series {
    registry: Registry,
    deleting: IntCounterVec,
    deleting_errors: IntCounterVec,
}

static SERIES: LazyLock<Series> = LazyLock::new(|| {
    let registry = Registry::new();
    let deleting = IntCounterVec::new(
        Opts::new(
            "force_delete_pods_total",
            "Number of pods that are being forcefully deleted since the Pod GC Controller started.",
        )
        .subsystem(SUBSYSTEM),
        &["namespace", "reason"],
    )
    .expect("valid opts");
    let deleting_errors = IntCounterVec::new(
        Opts::new(
            "force_delete_pod_errors_total",
            "Number of errors encountered when forcefully deleting the pods since the Pod GC Controller started.",
        )
        .subsystem(SUBSYSTEM),
        &["namespace", "reason"],
    )
    .expect("valid opts");
    registry
        .register(Box::new(deleting.clone()))
        .expect("register once");
    registry
        .register(Box::new(deleting_errors.clone()))
        .expect("register once");
    Series {
        registry,
        deleting,
        deleting_errors,
    }
});

/// `DeletingPodsTotal.WithLabelValues(ns, reason).Inc()`.
pub fn inc_deleting_pods(namespace: &str, reason: &str) {
    SERIES
        .deleting
        .with_label_values(&[namespace, reason])
        .inc();
}

/// `DeletingPodsErrorTotal.WithLabelValues(ns, reason).Inc()`.
pub fn inc_deleting_pod_errors(namespace: &str, reason: &str) {
    SERIES
        .deleting_errors
        .with_label_values(&[namespace, reason])
        .inc();
}

/// `testutil.GetCounterMetricValue(DeletingPodsTotal.WithLabelValues(..))`.
pub fn deleting_pods(namespace: &str, reason: &str) -> u64 {
    SERIES
        .deleting
        .with_label_values(&[namespace, reason])
        .get()
}

/// `testutil.GetCounterMetricValue(DeletingPodsErrorTotal.WithLabelValues(..))`.
pub fn deleting_pod_errors(namespace: &str, reason: &str) -> u64 {
    SERIES
        .deleting_errors
        .with_label_values(&[namespace, reason])
        .get()
}

/// The registry in Prometheus text exposition format.
pub fn gather() -> String {
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&SERIES.registry.gather(), &mut buf)
        .expect("text encode");
    String::from_utf8(buf).expect("utf8")
}
