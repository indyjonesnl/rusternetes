//! Port of `pkg/kubelet/server/metrics/metrics.go`: `HTTPRequests` (`:35-46`),
//! `HTTPRequestsDuration` (`:48-58`), `HTTPInflightRequests` (`:60-69`),
//! `VolumeStatCalDuration` (`:70-80`), `Register` (`:84-91`) and
//! `CollectVolumeStatCalDuration` (`:98-101`).
//!
//! `legacyregistry` is `prometheus::default_registry()`, which the kubelet's
//! `/metrics` also gathers. Upstream registers in `Register()` (`:84-91`,
//! `sync.Once`); here registration happens once on first collection. A
//! `HistogramVec` with no children exposes no series, so the served body is
//! identical to registering eagerly.

use prometheus::{CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts};
use std::sync::{LazyLock, Once};
use std::time::Instant;

/// `kubeletSubsystem` (`metrics.go`).
const KUBELET_SUBSYSTEM: &str = "kubelet";

/// `VolumeStatCalDuration` (`metrics.go:69-79`): `Buckets: metrics.DefBuckets`
/// is `prometheus.DefBuckets`, the crate's default buckets. The metric is
/// `StabilityLevel: metrics.ALPHA` (`metrics.go:75`), so component-base
/// prefixes the served help with `[ALPHA] ` (`component-base/metrics/opts.go:193-198`).
static VOLUME_STAT_CAL_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            format!("{KUBELET_SUBSYSTEM}_volume_metric_collection_duration_seconds"),
            "[ALPHA] Duration in seconds to calculate volume stats",
        ),
        &["metric_source"],
    )
    .expect("valid volume stat histogram definition")
});

/// The label names shared by the three HTTP metrics (`metrics.go:45,57,68`).
/// `server_type` differentiates the readonly and readwrite server;
/// `long_running` marks exec/attach/portforward/debug.
const HTTP_LABELS: [&str; 4] = ["method", "path", "server_type", "long_running"];

/// `HTTPRequests` (`metrics.go:35-46`), `StabilityLevel: ALPHA`.
pub static HTTP_REQUESTS: LazyLock<CounterVec> = LazyLock::new(|| {
    CounterVec::new(
        Opts::new(
            format!("{KUBELET_SUBSYSTEM}_http_requests_total"),
            "[ALPHA] Number of the http requests received since the server started",
        ),
        &HTTP_LABELS,
    )
    .expect("valid http requests counter definition")
});

/// `HTTPRequestsDuration` (`metrics.go:48-58`): `Buckets: metrics.DefBuckets`.
pub static HTTP_REQUESTS_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            format!("{KUBELET_SUBSYSTEM}_http_requests_duration_seconds"),
            "[ALPHA] Duration in seconds to serve http requests",
        ),
        &HTTP_LABELS,
    )
    .expect("valid http requests duration definition")
});

/// `HTTPInflightRequests` (`metrics.go:60-69`).
pub static HTTP_INFLIGHT_REQUESTS: LazyLock<GaugeVec> = LazyLock::new(|| {
    GaugeVec::new(
        Opts::new(
            format!("{KUBELET_SUBSYSTEM}_http_inflight_requests"),
            "[ALPHA] Number of the inflight http requests",
        ),
        &HTTP_LABELS,
    )
    .expect("valid http inflight gauge definition")
});

static REGISTER_METRICS: Once = Once::new();

/// `Register` (`metrics.go:84-91`): `registerMetrics.Do(...)` over the same
/// four collectors, in the same order. Upstream calls it from
/// `NewMainKubelet` (`pkg/kubelet/kubelet.go:1686`).
pub fn register() {
    REGISTER_METRICS.call_once(|| {
        let r = prometheus::default_registry();
        r.register(Box::new(HTTP_REQUESTS.clone()))
            .expect("http requests counter registered once");
        r.register(Box::new(HTTP_REQUESTS_DURATION.clone()))
            .expect("http requests duration registered once");
        r.register(Box::new(HTTP_INFLIGHT_REQUESTS.clone()))
            .expect("http inflight gauge registered once");
        prometheus::default_registry()
            .register(Box::new(VOLUME_STAT_CAL_DURATION.clone()))
            .expect("volume stat histogram registered once");
    });
}

/// `CollectVolumeStatCalDuration` (`metrics.go:98-101`): observes the seconds
/// since `start` under `metric_source`.
pub fn collect_volume_stat_cal_duration(metric_source: &str, start: Instant) {
    register();
    VOLUME_STAT_CAL_DURATION
        .with_label_values(&[metric_source])
        .observe(start.elapsed().as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The histogram is served by the kubelet's `/metrics` body with
    /// upstream's name, help and `metric_source` label.
    #[test]
    fn collected_duration_is_served_by_kubelet_metrics() {
        collect_volume_stat_cal_duration("unit_test_src", Instant::now());
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains("# HELP kubelet_volume_metric_collection_duration_seconds [ALPHA] Duration in seconds to calculate volume stats"));
        assert!(body.contains(
            "kubelet_volume_metric_collection_duration_seconds_count{metric_source=\"unit_test_src\"} 1"
        ));
    }
}
