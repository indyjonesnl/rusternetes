//! Port of the volume-stat part of `pkg/kubelet/server/metrics/metrics.go`:
//! `VolumeStatCalDuration` (`:67-80`) and `CollectVolumeStatCalDuration`
//! (`:98-101`).
//!
//! `legacyregistry` is `prometheus::default_registry()`, which the kubelet's
//! `/metrics` also gathers. Upstream registers in `Register()` (`:84-91`,
//! `sync.Once`); here registration happens once on first collection. A
//! `HistogramVec` with no children exposes no series, so the served body is
//! identical to registering eagerly.

use prometheus::{HistogramOpts, HistogramVec};
use std::sync::{LazyLock, Once};
use std::time::Instant;

/// `kubeletSubsystem` (`metrics.go`).
const KUBELET_SUBSYSTEM: &str = "kubelet";

/// `VolumeStatCalDuration` (`metrics.go:69-79`): `Buckets: metrics.DefBuckets`
/// is `prometheus.DefBuckets`, the crate's default buckets.
static VOLUME_STAT_CAL_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            format!("{KUBELET_SUBSYSTEM}_volume_metric_collection_duration_seconds"),
            "Duration in seconds to calculate volume stats",
        ),
        &["metric_source"],
    )
    .expect("valid volume stat histogram definition")
});

static REGISTER_METRICS: Once = Once::new();

/// `Register` (`metrics.go:84-91`): `registerMetrics.Do(...)`.
fn register() {
    REGISTER_METRICS.call_once(|| {
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
        assert!(body.contains("# HELP kubelet_volume_metric_collection_duration_seconds Duration in seconds to calculate volume stats"));
        assert!(body.contains(
            "kubelet_volume_metric_collection_duration_seconds_count{metric_source=\"unit_test_src\"} 1"
        ));
    }
}
