//! Unit tests moved from `src/volume_plugins/util/metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use prometheus::{HistogramOpts, HistogramVec};
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_kubelet::server_metrics::collect_volume_stat_cal_duration;
    pub use rusternetes_kubelet::volume_plugins::plugin::{Metrics, MetricsProvider};
    pub use std::io::{Seek, SeekFrom};
    pub use std::sync::{LazyLock, Once};
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::metrics::*;

    /// `metrics_block.go:62-64`: an empty device is `NoPathDefined`.
    #[test]
    fn block_metrics_without_a_device_is_no_path_defined() {
        let e = new_metrics_block("").get_metrics().unwrap_err();
        assert_eq!(e.to_string(), NO_PATH_DEFINED);
    }

    /// `metrics_block.go:55`: GetMetrics defers
    /// `CollectVolumeStatCalDuration("block", ..)`.
    #[test]
    fn block_metrics_observes_volume_stat_duration() {
        let _ = new_metrics_block("").get_metrics();
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains(
            "kubelet_volume_metric_collection_duration_seconds_count{metric_source=\"block\"}"
        ));
    }

    /// `getBlockInfo` seeks to the end: the capacity is the file size.
    #[test]
    fn block_metrics_capacity_is_the_size_found_by_seeking_to_the_end() {
        let f = tempfile::NamedTempFile::new().unwrap();
        f.as_file().set_len(4096).unwrap();
        let m = new_metrics_block(f.path().to_str().unwrap())
            .get_metrics()
            .unwrap();
        assert_eq!(m.capacity.unwrap().value(), 4096);
    }

    #[test]
    fn block_metrics_of_a_missing_device_fail_to_open() {
        let e = new_metrics_block("/nonexistent/dev")
            .get_metrics()
            .unwrap_err();
        assert!(e.to_string().starts_with("unable to open device"), "{e}");
    }

    #[test]
    fn nil_metrics_are_not_supported() {
        assert_eq!(
            MetricsNil.get_metrics().unwrap_err().to_string(),
            NOT_SUPPORTED
        );
    }

    /// `getErrorCode` (`metrics.go:148-161`).
    #[test]
    fn error_code_names_follow_grpc_codes_string() {
        assert_eq!(get_error_code(None), "OK");
        assert_eq!(
            get_error_code(Some(&tonic::Status::unavailable("x"))),
            "Unavailable"
        );
        assert_eq!(
            get_error_code(Some(&tonic::Status::cancelled("x"))),
            "Canceled"
        );
    }

    /// `RecordCSIOperationLatencyMetrics` is served by the kubelet's
    /// `/metrics` with upstream's name, help and labels.
    #[test]
    fn csi_operation_latency_is_served_with_migrated_label() {
        record_csi_operation_latency_metrics(
            "util.metrics.test.driver",
            "/csi.v1.Node/NodeStageVolume",
            None,
            Duration::from_millis(5),
            "true",
        );
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains("# HELP csi_operations_seconds [ALPHA] Container Storage Interface operation duration with gRPC error code status total"));
        assert!(body.contains("csi_operations_seconds_count{driver_name=\"util.metrics.test.driver\",grpc_status_code=\"OK\",method_name=\"/csi.v1.Node/NodeStageVolume\",migrated=\"true\"} 1"), "{body}");
    }

    /// `OperationCompleteHook` (`metrics.go:90-108`): `status` is `success` or
    /// `fail-unknown`, `migrated` defaults to false.
    #[test]
    fn operation_complete_hook_observes_status_and_migrated() {
        let h = operation_complete_hook("util.metrics.test/plugin", "unmount_device");
        h.complete(None, None);
        h.complete(Some(&anyhow::anyhow!("boom")), Some(true));
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains(
            "# HELP storage_operation_duration_seconds [ALPHA] Storage operation duration"
        ));
        assert!(body.contains("storage_operation_duration_seconds_count{migrated=\"false\",operation_name=\"unmount_device\",status=\"success\",volume_plugin=\"util.metrics.test/plugin\"} 1"), "{body}");
        assert!(body.contains("storage_operation_duration_seconds_count{migrated=\"true\",operation_name=\"unmount_device\",status=\"fail-unknown\",volume_plugin=\"util.metrics.test/plugin\"} 1"), "{body}");
    }

    /// `GetFullQualifiedPluginNameForVolume` (`metrics.go:127-139`).
    #[test]
    fn csi_plugin_name_carries_the_driver() {
        use rusternetes_kubelet::volume_plugins::plugin::OwnedSpec;
        let pv: rusternetes_common::resources::volume::PersistentVolume =
            serde_json::from_value(serde_json::json!({
                "metadata": {"name": "pv"},
                "spec": {"csi": {"driver": "d.example.com", "volumeHandle": "h"}}
            }))
            .unwrap();
        let spec = OwnedSpec {
            volume: serde_json::from_value(serde_json::json!({"name": "v"})).unwrap(),
            persistent_volume: Some(pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert_eq!(
            get_full_qualified_plugin_name_for_volume("kubernetes.io/csi", Some(&spec.as_spec())),
            "kubernetes.io/csi:d.example.com"
        );
        assert_eq!(
            get_full_qualified_plugin_name_for_volume("kubernetes.io/csi", None),
            "kubernetes.io/csi"
        );
        let plain = OwnedSpec {
            persistent_volume: None,
            ..spec
        };
        assert_eq!(
            get_full_qualified_plugin_name_for_volume(
                "kubernetes.io/host-path",
                Some(&plain.as_spec())
            ),
            "kubernetes.io/host-path"
        );
    }
}
