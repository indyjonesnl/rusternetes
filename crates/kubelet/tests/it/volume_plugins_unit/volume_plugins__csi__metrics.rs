//! Unit tests moved from `src/volume_plugins/csi/metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Context, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::csi::FSGroupPolicy;
    pub use rusternetes_common::resources::{
        CSIDriver, PersistentVolume, PersistentVolumeAccessMode, Pod, Secret, VolumeAttachment,
    };
    pub use rusternetes_kubelet::server_metrics::collect_volume_stat_cal_duration;
    pub use rusternetes_kubelet::volume_plugins::csi::*;
    pub use rusternetes_kubelet::volume_plugins::csi_client::CSI_TIMEOUT;
    pub use rusternetes_kubelet::volume_plugins::csi_client::{CsiDriverClient, CsiError};
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        DeviceMountableVolumePlugin, DeviceMounter, DeviceMounterArgs, DeviceUnmounter,
    };
    pub use rusternetes_kubelet::volume_plugins::plugin::{Metrics, MetricsProvider};
    pub use rusternetes_kubelet::volume_plugins::util::metrics::new_not_supported_error_with_driver_name;
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
    };
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use sha2::{Digest, Sha256};
    pub use std::collections::HashMap;
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;
    pub use std::sync::Mutex;
    pub use std::time::Instant;
    pub use tracing::{debug, error, info};
}

mod tests {
    //! Port of `pkg/volume/csi/csi_metrics_test.go` (`TestGetMetrics`,
    //! `TestGetMetricsDriverNotSupportStats`, `TestGetMetricsDriverNotFound`) plus
    //! the `NodeGetVolumeStats` branches of `csi_client.go:588-664` the upstream
    //! test file does not reach (volume condition, empty usage).
    //!
    //! Upstream injects a fake `NodeClient` through `nodeV1ClientCreator`; here
    //! the driver is a real gRPC `csi.v1.Node` server on a unix socket
    //! (`csi_client::fake`), registered in the driver store under a unique name.

    use super::__parent::*;
    use crate::csi_fake::{serve, FakeDriver};
    use rusternetes_common::feature_gates::{reset_to_defaults, set_from_str};
    use rusternetes_kubelet::volume_plugins::csi::metrics::*;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::node_service_capability::rpc::Type as Cap;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::{
        volume_usage::Unit, NodeGetVolumeStatsResponse, VolumeCondition, VolumeUsage,
    };
    use rusternetes_kubelet::volume_plugins::csi_drivers_store::{csi_drivers, Driver};
    use rusternetes_kubelet::volume_plugins::util::metrics::is_not_supported;
    use serial_test::serial;

    struct Fx {
        _dir: tempfile::TempDir,
        driver: String,
        fake: FakeDriver,
        _srv: tokio::task::JoinHandle<()>,
    }

    async fn fx(name: &str, caps: &[Cap], resp: Option<NodeGetVolumeStatsResponse>) -> Fx {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let driver = format!("{name}.metrics.csi.example.com");
        let fake = FakeDriver::with_capabilities(caps);
        *fake.volume_stats_resp.lock().unwrap() = resp;
        let srv = serve(fake.clone(), &sock);
        csi_drivers().set(
            &driver,
            Driver {
                endpoint: sock.to_string_lossy().to_string(),
                highest_supported_version: "1.0.0".into(),
            },
        );
        Fx {
            _dir: dir,
            driver,
            fake,
            _srv: srv,
        }
    }

    /// `getRawVolumeInfo` (`csi_metrics_test.go`).
    fn raw_volume_info() -> NodeGetVolumeStatsResponse {
        NodeGetVolumeStatsResponse {
            usage: vec![
                VolumeUsage {
                    available: 10,
                    total: 10,
                    used: 2,
                    unit: Unit::Bytes as i32,
                },
                VolumeUsage {
                    available: 100,
                    total: 100,
                    used: 20,
                    unit: Unit::Inodes as i32,
                },
            ],
            volume_condition: None,
        }
    }

    fn val(q: &Option<rusternetes_common::quantity::Quantity>) -> i128 {
        q.as_ref().expect("quantity reported").value()
    }

    /// `TestGetMetrics`: a driver with `GET_VOLUME_STATS` answers, and the BYTES
    /// and INODES usages land in the matching `volume.Metrics` fields.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn get_metrics_maps_bytes_and_inodes_usage() {
        let f = fx(
            "getmetrics",
            &[Cap::GetVolumeStats],
            Some(raw_volume_info()),
        )
        .await;
        let before = chrono::Utc::now();
        let m = new_metrics_csi("foobar", "/mnt/foo", &f.driver)
            .get_metrics()
            .unwrap();
        assert_eq!(val(&m.available), 10);
        assert_eq!(val(&m.capacity), 10);
        assert_eq!(val(&m.used), 2);
        assert_eq!(val(&m.inodes_free), 100);
        assert_eq!(val(&m.inodes), 100);
        assert_eq!(val(&m.inodes_used), 20);
        // `metrics.Time = currentTime` (`csi_metrics.go:88`).
        assert!(m.time.unwrap() >= before);
        // No VolumeCondition without the CSIVolumeHealth gate.
        assert_eq!(m.abnormal, None);
        assert_eq!(m.message, None);

        // `NodeGetVolumeStats(ctx, volumeID, targetPath)` request fields.
        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.volume_stats.len(), 1);
        assert_eq!(calls.volume_stats[0].volume_id, "foobar");
        assert_eq!(calls.volume_stats[0].volume_path, "/mnt/foo");
    }

    /// `NodeGetVolumeStats` only reports the units it is given
    /// (`csi_client.go:616-623`): the rest stay a zero quantity, not nil.
    #[tokio::test(flavor = "multi_thread")]
    async fn unreported_units_are_zero_quantities() {
        let mut resp = raw_volume_info();
        resp.usage.truncate(1); // BYTES only
        let f = fx("bytesonly", &[Cap::GetVolumeStats], Some(resp)).await;
        let m = new_metrics_csi("foobar", "/mnt/foo", &f.driver)
            .get_metrics()
            .unwrap();
        assert_eq!(val(&m.capacity), 10);
        assert_eq!(val(&m.inodes), 0);
        assert_eq!(val(&m.inodes_free), 0);
        assert_eq!(val(&m.inodes_used), 0);
    }

    /// `TestGetMetricsDriverNotSupportStats`: without `GET_VOLUME_STATS` the
    /// error is `NotSupported` (naming the driver) and the driver is never asked
    /// for stats.
    #[tokio::test(flavor = "multi_thread")]
    async fn driver_without_get_volume_stats_is_not_supported() {
        let f = fx("nostats", &[], Some(raw_volume_info())).await;
        let err = new_metrics_csi("foobar", "/mnt/foo", &f.driver)
            .get_metrics()
            .unwrap_err();
        assert!(is_not_supported(&err), "{err}");
        assert_eq!(
            err.to_string(),
            format!("metrics are not supported for {} volumes", f.driver)
        );
        assert!(f.fake.calls.lock().unwrap().volume_stats.is_empty());
    }

    /// `TestGetMetricsDriverNotFound`: an unregistered driver is a transient
    /// failure (`csi_metrics.go:65-66`), not a permanent one.
    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_driver_is_a_transient_failure() {
        let err = new_metrics_csi("foobar", "/mnt/foo", "unknown-driver.csi.example.com")
            .get_metrics()
            .unwrap_err();
        assert!(
            matches!(err.downcast_ref::<CsiError>(), Some(CsiError::Transient(_))),
            "{err:?}"
        );
    }

    /// `csi_client.go:640-642`: no usage and no volume condition is an error.
    #[tokio::test(flavor = "multi_thread")]
    async fn empty_usage_without_volume_condition_is_an_error() {
        let f = fx("nousage", &[Cap::GetVolumeStats], None).await;
        let err = new_metrics_csi("foobar", "/mnt/foo", &f.driver)
            .get_metrics()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed to get usage from response. usage is nil"
        );
    }

    /// `csi_client.go:590-595`: both ids are required.
    #[tokio::test(flavor = "multi_thread")]
    async fn missing_volume_id_or_target_path_is_an_error() {
        let f = fx("noids", &[Cap::GetVolumeStats], Some(raw_volume_info())).await;
        let err = new_metrics_csi("", "/mnt/foo", &f.driver)
            .get_metrics()
            .unwrap_err();
        assert_eq!(err.to_string(), "missing volume id");
        let err = new_metrics_csi("foobar", "", &f.driver)
            .get_metrics()
            .unwrap_err();
        assert_eq!(err.to_string(), "missing target path");
    }

    /// `csi_client.go:625-636`: with `CSIVolumeHealth` on and the driver
    /// advertising `VOLUME_CONDITION`, the condition is copied to
    /// `Abnormal`/`Message`; a nil condition reads as `false`/`""`; a driver
    /// without the capability reports none.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn volume_condition_is_reported_when_gate_and_capability_are_on() {
        set_from_str("CSIVolumeHealth=true").unwrap();
        let mut resp = raw_volume_info();
        resp.volume_condition = Some(VolumeCondition {
            abnormal: true,
            message: "disk is failing".into(),
        });
        let with_cap = fx(
            "condition",
            &[Cap::GetVolumeStats, Cap::VolumeCondition],
            Some(resp.clone()),
        )
        .await;
        let m = new_metrics_csi("foobar", "/mnt/foo", &with_cap.driver)
            .get_metrics()
            .unwrap();
        assert_eq!(m.abnormal, Some(true));
        assert_eq!(m.message.as_deref(), Some("disk is failing"));

        let nil_condition = fx(
            "nilcondition",
            &[Cap::GetVolumeStats, Cap::VolumeCondition],
            Some(raw_volume_info()),
        )
        .await;
        let m = new_metrics_csi("foobar", "/mnt/foo", &nil_condition.driver)
            .get_metrics()
            .unwrap();
        assert_eq!(m.abnormal, Some(false));
        assert_eq!(m.message.as_deref(), Some(""));

        let without_cap = fx("nocondcap", &[Cap::GetVolumeStats], Some(resp)).await;
        let m = new_metrics_csi("foobar", "/mnt/foo", &without_cap.driver)
            .get_metrics()
            .unwrap();
        assert_eq!(m.abnormal, None);
        assert_eq!(m.message, None);
        reset_to_defaults();
    }

    /// With the gate on and `VOLUME_CONDITION` advertised, a response with a
    /// condition but no usage is not an error (`csi_client.go:640`).
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn condition_only_response_is_not_an_error() {
        set_from_str("CSIVolumeHealth=true").unwrap();
        let resp = NodeGetVolumeStatsResponse {
            usage: vec![],
            volume_condition: Some(VolumeCondition {
                abnormal: false,
                message: "healthy".into(),
            }),
        };
        let f = fx(
            "condonly",
            &[Cap::GetVolumeStats, Cap::VolumeCondition],
            Some(resp),
        )
        .await;
        let m = new_metrics_csi("foobar", "/mnt/foo", &f.driver)
            .get_metrics()
            .unwrap();
        reset_to_defaults();
        assert_eq!(m.abnormal, Some(false));
        assert_eq!(m.message.as_deref(), Some("healthy"));
        assert_eq!(val(&m.capacity), 0);
    }

    /// `defer servermetrics.CollectVolumeStatCalDuration(driverName, startTime)`
    /// (`csi_metrics.go:57`): observed under the driver name, also on failure.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_metrics_observes_volume_stat_duration_by_driver() {
        let driver = "durationmetrics.csi.example.com";
        let _ = new_metrics_csi("foobar", "/mnt/foo", driver).get_metrics();
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(
            body.contains(&format!(
                "kubelet_volume_metric_collection_duration_seconds_count{{metric_source=\"{driver}\"}}"
            )),
            "{body}"
        );
    }

    /// Outside a tokio runtime the sync trait method still works.
    #[test]
    fn get_metrics_works_without_a_runtime() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let f = rt.block_on(fx(
            "noruntime",
            &[Cap::GetVolumeStats],
            Some(raw_volume_info()),
        ));
        let driver = f.driver.clone();
        // The server task lives on `rt`; drive the provider from a plain thread.
        let m = std::thread::spawn(move || {
            new_metrics_csi("foobar", "/mnt/foo", &driver)
                .get_metrics()
                .unwrap()
        })
        .join()
        .unwrap();
        assert_eq!(val(&m.capacity), 10);
    }
}
