//! Unit tests moved from `src/volume_plugins/metrics_du.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_kubelet::server_metrics::collect_volume_stat_cal_duration;
    pub use rusternetes_kubelet::volume_plugins::metrics_statfs::{
        fs_info, fs_info_failed_error, no_path_defined_error,
    };
    pub use rusternetes_kubelet::volume_plugins::plugin::{Metrics, MetricsProvider};
    pub use std::collections::HashSet;
    pub use std::os::unix::fs::MetadataExt;
    pub use std::path::Path;
    pub use std::time::Instant;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::metrics_du::*;
    use rusternetes_kubelet::volume_plugins::metrics_statfs::MetricsErrorCode;

    fn blocks_bytes(p: &Path) -> i64 {
        std::fs::symlink_metadata(p).unwrap().blocks() as i64 * 512
    }

    fn du_count() -> f64 {
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        body.lines()
            .find(|l| {
                l.starts_with(
                    "kubelet_volume_metric_collection_duration_seconds_count{metric_source=\"du\"}",
                )
            })
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0)
    }

    // TestMetricsDuGetCapacity (metrics_du_test.go:31)
    #[test]
    fn metrics_du_get_capacity() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let metrics = MetricsDu::new(dir.to_str().unwrap());
        let empty = blocks_bytes(dir);

        let actual = metrics.get_metrics().unwrap();
        assert_eq!(empty as i128, actual.used.as_ref().unwrap().value());
        assert!(actual.capacity.as_ref().unwrap().value() > 0);
        assert!(actual.available.as_ref().unwrap().value() > 0);

        let f1 = dir.join("f1");
        std::fs::write(&f1, "Hello World").unwrap();
        let actual = metrics.get_metrics().unwrap();
        assert_eq!(
            (empty + blocks_bytes(&f1)) as i128,
            actual.used.as_ref().unwrap().value()
        );

        // a hardlink leaves the inode count unchanged
        let previous = actual.inodes_used.as_ref().unwrap().value();
        std::fs::hard_link(&f1, dir.join("f2")).unwrap();
        let actual = metrics.get_metrics().unwrap();
        assert_eq!(previous, actual.inodes_used.as_ref().unwrap().value());
    }

    // TestMetricsDuRequirePath (metrics_du_test.go:84)
    #[test]
    fn metrics_du_require_path() {
        let err = MetricsDu::new("").get_metrics().unwrap_err();
        let me =
            err.downcast_ref::<rusternetes_kubelet::volume_plugins::metrics_statfs::MetricsError>();
        assert_eq!(me.map(|m| m.code), Some(MetricsErrorCode::NoPathDefined));
    }

    // TestMetricsDuRequireRealDirectory (metrics_du_test.go:98)
    #[test]
    fn metrics_du_require_real_directory() {
        assert!(MetricsDu::new("/not/a/real/directory")
            .get_metrics()
            .is_err());
    }

    // fs.go:84-85: DiskUsage defers CollectVolumeStatCalDuration("du", ..).
    #[test]
    fn disk_usage_observes_du_duration() {
        let tmp = tempfile::tempdir().unwrap();
        let before = du_count();
        disk_usage(tmp.path().to_str().unwrap()).unwrap();
        assert!(du_count() >= before + 1.0);
    }

    // fs.go:69-71
    #[test]
    fn disk_usage_rejects_empty_path() {
        assert_eq!(disk_usage("").unwrap_err().to_string(), "invalid directory");
    }
}
