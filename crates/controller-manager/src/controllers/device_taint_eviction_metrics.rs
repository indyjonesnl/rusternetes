//! Prometheus series for the device taint eviction controller.
//!
//! Port of `pkg/controller/devicetainteviction/metrics/metrics.go`
//! (release-1.35): subsystem `device_taint_eviction_controller`, counter
//! `pod_deletions_total` and histogram `pod_deletion_duration_seconds` with
//! buckets 5ms..4m. Upstream's `New(buckets...)` builds an independent
//! registry per call (the unit tests use a one-bucket variant) and `Global`
//! plus `Register()` publish one instance in `legacyregistry`; here
//! [`Metrics::new`] is `New` and [`global`] is `Global`.

use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, Opts, Registry, TextEncoder};
use std::sync::{Arc, LazyLock};

/// `controllerSubsystem`.
const SUBSYSTEM: &str = "device_taint_eviction_controller";

/// Default histogram buckets (`metrics.go:51`, "5ms to 4m").
pub const DEFAULT_BUCKETS: [f64; 12] = [
    0.005, 0.025, 0.1, 0.5, 1.0, 2.5, 10.0, 30.0, 60.0, 120.0, 180.0, 240.0,
];

/// `Metrics`: every metric of the controller plus the registry holding them.
pub struct Metrics {
    registry: Registry,
    /// `PodDeletionsTotal`.
    pub pod_deletions_total: IntCounter,
    /// `PodDeletionsLatency`.
    pub pod_deletions_latency: Histogram,
}

impl Metrics {
    /// `New(buckets...)`: empty `buckets` selects [`DEFAULT_BUCKETS`].
    pub fn new(buckets: &[f64]) -> Self {
        let buckets = if buckets.is_empty() {
            DEFAULT_BUCKETS.to_vec()
        } else {
            buckets.to_vec()
        };
        let registry = Registry::new();
        let pod_deletions_total = IntCounter::with_opts(
            Opts::new(
                "pod_deletions_total",
                "Total number of Pods deleted by DeviceTaintEvictionController since its start.",
            )
            .subsystem(SUBSYSTEM),
        )
        .expect("valid opts");
        let pod_deletions_latency = Histogram::with_opts(
            HistogramOpts::new(
                "pod_deletion_duration_seconds",
                "Latency, in seconds, between the time when a device taint effect has been activated and a Pod's deletion via DeviceTaintEvictionController.",
            )
            .subsystem(SUBSYSTEM)
            .buckets(buckets),
        )
        .expect("valid opts");
        registry
            .register(Box::new(pod_deletions_total.clone()))
            .expect("register once");
        registry
            .register(Box::new(pod_deletions_latency.clone()))
            .expect("register once");
        Self {
            registry,
            pod_deletions_total,
            pod_deletions_latency,
        }
    }

    /// The registry in Prometheus text exposition format
    /// (`Metrics.Gather`).
    pub fn gather(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("text encode");
        String::from_utf8(buf).expect("utf8")
    }
}

static GLOBAL: LazyLock<Arc<Metrics>> = LazyLock::new(|| Arc::new(Metrics::new(&[])));

/// `metrics.Global`.
pub fn global() -> Arc<Metrics> {
    GLOBAL.clone()
}

/// The global instance's series, for the controller-manager `/metrics`.
pub fn gather() -> String {
    GLOBAL.gather()
}
