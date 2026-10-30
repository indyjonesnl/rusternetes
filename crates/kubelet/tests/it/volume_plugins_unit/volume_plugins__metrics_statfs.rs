//! Unit tests moved from `src/volume_plugins/metrics_statfs.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_kubelet::server_metrics::collect_volume_stat_cal_duration;
    pub use rusternetes_kubelet::volume_plugins::plugin::{Metrics, MetricsProvider};
    pub use std::time::Instant;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::metrics_statfs::*;

    fn code(e: &anyhow::Error) -> Option<MetricsErrorCode> {
        e.downcast_ref::<MetricsError>().map(|m| m.code)
    }

    // TestGetMetricsStatFS (metrics_statfs_test.go:29)
    #[test]
    fn get_metrics_statfs() {
        let err = MetricsStatFs::new("").get_metrics().unwrap_err();
        assert_eq!(code(&err), Some(MetricsErrorCode::NoPathDefined));
        assert_eq!(err.to_string(), "no path defined for disk usage metrics.");

        let err = MetricsStatFs::new("/not/a/real/directory")
            .get_metrics()
            .unwrap_err();
        assert_eq!(code(&err), Some(MetricsErrorCode::FsInfoFailed));
        assert!(err
            .to_string()
            .starts_with("failed to get FsInfo due to error "));

        let tmp = tempfile::tempdir().unwrap();
        let m = MetricsStatFs::new(tmp.path().to_str().unwrap())
            .get_metrics()
            .unwrap();
        assert!(m.time.is_some());
        assert!(m.capacity.as_ref().unwrap().value() > 0);
        assert!(m.available.as_ref().unwrap().value() > 0);
        assert!(m.used.as_ref().unwrap().value() >= 0);
        assert_eq!(
            m.inodes_used.as_ref().unwrap().value(),
            m.inodes.as_ref().unwrap().value() - m.inodes_free.as_ref().unwrap().value()
        );
    }

    // metrics_statfs.go:47: GetMetrics defers CollectVolumeStatCalDuration
    // ("statfs", ..), so every call (even the no-path error) is observed.
    #[test]
    fn get_metrics_statfs_observes_volume_stat_duration() {
        let _ = MetricsStatFs::new("").get_metrics();
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains(
            "kubelet_volume_metric_collection_duration_seconds_count{metric_source=\"statfs\"}"
        ));
    }
}
