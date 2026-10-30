//! Unit tests moved from `src/server_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts};
    pub use std::sync::{LazyLock, Once};
    pub use std::time::Instant;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::server_metrics::*;

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
