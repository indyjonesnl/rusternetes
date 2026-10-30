//! Unit tests moved from `src/volume_plugins/csi/expander.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Context, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::csi::FSGroupPolicy;
    pub use rusternetes_common::resources::volume::{CSIVolumeSource, PersistentVolumeMode};
    pub use rusternetes_common::resources::{
        CSIDriver, PersistentVolume, PersistentVolumeAccessMode, Pod, Secret, VolumeAttachment,
    };
    pub use rusternetes_kubelet::volume_plugins::csi::*;
    pub use rusternetes_kubelet::volume_plugins::csi_client::{CsiDriverClient, CsiError};
    pub use rusternetes_kubelet::volume_plugins::csi_client::{
        CsiResizeOptions, NodeExpandError, FS_TYPE_BLOCK_NAME,
    };
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        DeviceMountableVolumePlugin, DeviceMounter, DeviceMounterArgs, DeviceUnmounter,
    };
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        NodeExpandableVolumePlugin, NodeResizeOptions,
    };
    pub use rusternetes_kubelet::volume_plugins::util::types::VolumeOperationError;
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
    };
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use sha2::{Digest, Sha256};
    pub use std::collections::HashMap;
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info};
}

/// Ported in shape from `TestNodeExpand` and `TestNodeExpandNoClientError`
/// (`pkg/volume/csi/expander_test.go:34-230`). Upstream's fake `csiClient` is
/// a real gRPC Node server on a unix socket here.
mod tests {
    use super::__parent::*;
    use crate::csi_fake::{serve, FakeDriver};
    use rusternetes_common::quantity::Quantity;
    use rusternetes_common::resources::{Secret, Volume};
    use rusternetes_kubelet::volume_plugins::csi::expander::*;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::volume_capability::AccessType;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::{
        node_service_capability::rpc::Type as Cap, NodeExpandVolumeRequest,
    };
    use rusternetes_storage::{MemoryStorage, StorageBackend};
    use serde_json::json;

    struct Fx {
        _dir: tempfile::TempDir,
        fake: FakeDriver,
        plugin: CsiPlugin,
        client: CsiDriverClient,
        _srv: tokio::task::JoinHandle<()>,
    }

    async fn fx(caps: &[Cap], secret: Option<(&str, &str, &str)>) -> Fx {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let fake = FakeDriver::with_capabilities(caps);
        let srv = serve(fake.clone(), &sock);
        let storage = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
        if let Some((ns, name, v)) = secret {
            let s = Secret::new(name, ns).with_data(HashMap::from([(
                "apiUsername".to_string(),
                v.as_bytes().to_vec(),
            )]));
            storage
                .create(&build_key("secrets", Some(ns), name), &s)
                .await
                .unwrap();
        }
        let host = Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                dir.path().to_string_lossy().to_string(),
                Some(storage),
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            ),
        );
        Fx {
            plugin: CsiPlugin::new(host),
            client: CsiDriverClient::with_endpoint("expandable", &sock),
            fake,
            _dir: dir,
            _srv: srv,
        }
    }

    /// `makeTestPV("test-pv", 10, "expandable", "test-vol")`.
    fn test_pv(expand_secret: bool, block: bool) -> PersistentVolume {
        let mut csi = json!({"driver": "expandable", "volumeHandle": "test-vol", "fsType": "ext4"});
        if expand_secret {
            csi["nodeExpandSecretRef"] = json!({"name": "expand-secret", "namespace": "default"});
        }
        serde_json::from_value(json!({
            "metadata": {"name": "test-pv"},
            "spec": {
                "capacity": {"storage": "10Gi"},
                "accessModes": ["ReadWriteMany"],
                "mountOptions": ["noatime"],
                "volumeMode": if block { "Block" } else { "Filesystem" },
                "csi": csi
            }
        }))
        .unwrap()
    }

    fn volume() -> Volume {
        serde_json::from_value(json!({"name": "v", "persistentVolumeClaim": {"claimName": "c"}}))
            .unwrap()
    }

    fn resize<'a>(spec: &'a Spec<'a>, stage: &str) -> NodeResizeOptions<'a> {
        NodeResizeOptions {
            volume_spec: spec,
            device_path: "/mnt/foobar".into(),
            device_mount_path: "/foo/bar".into(),
            device_stage_path: stage.into(),
            new_size: Quantity::parse("20Gi").unwrap(),
            old_size: Quantity::parse("10Gi").unwrap(),
        }
    }

    async fn run(f: &Fx, pv: &PersistentVolume, stage: &str, fs_volume: bool) -> Result<bool> {
        let vol = volume();
        let spec = Spec {
            volume: &vol,
            persistent_volume: Some(pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        f.plugin
            .node_expand_with_client(
                resize(&spec, stage),
                pv.spec.csi.as_ref().unwrap(),
                &f.client,
                fs_volume,
            )
            .await
    }

    fn only_expand(f: &Fx) -> NodeExpandVolumeRequest {
        f.fake.calls.lock().unwrap().expand[0].clone()
    }

    /// "when node expansion is not set": the driver lacks `EXPAND_VOLUME`, so
    /// the result is `OperationNotSupported` and nothing is sent.
    #[tokio::test]
    async fn expansion_not_set_is_operation_not_supported() {
        let f = fx(&[], None).await;
        let err = run(&f, &test_pv(false, false), "", true).await.unwrap_err();
        assert!(matches!(
            err.downcast_ref::<VolumeOperationError>(),
            Some(VolumeOperationError::OperationNotSupported(m))
                if m == "NodeExpand is not supported by the CSI driver expandable"
        ));
        assert!(f.fake.calls.lock().unwrap().expand.is_empty());
    }

    /// "nodeExpansion=on, nodeStage=on, volumePhase=staged": the staging path
    /// reaches the driver; a filesystem volume expands with a mount capability
    /// at the device mount path.
    #[tokio::test]
    async fn staged_filesystem_volume_expands() {
        let f = fx(&[Cap::ExpandVolume, Cap::StageUnstageVolume], None).await;
        assert!(run(&f, &test_pv(false, false), "/foo/bar", true)
            .await
            .unwrap());
        let r = only_expand(&f);
        assert_eq!(r.staging_target_path, "/foo/bar");
        assert_eq!(r.volume_path, "/foo/bar");
        assert_eq!(r.volume_id, "test-vol");
        assert_eq!(r.capacity_range.unwrap().required_bytes, 20 << 30);
        match r.volume_capability.unwrap().access_type.unwrap() {
            AccessType::Mount(m) => {
                assert_eq!(m.fs_type, "ext4");
                assert_eq!(m.mount_flags, vec!["noatime".to_string()]);
            }
            other => panic!("expected mount, got {other:?}"),
        }
    }

    /// "fsVolume=false": a block volume is expanded by DEVICE path with the
    /// block access type (`expander.go:104-110`).
    #[tokio::test]
    async fn block_volume_expands_by_device_path() {
        let f = fx(&[Cap::ExpandVolume], None).await;
        assert!(run(&f, &test_pv(false, true), "", false).await.unwrap());
        let r = only_expand(&f);
        assert_eq!(r.volume_path, "/mnt/foobar");
        assert!(matches!(
            r.volume_capability.unwrap().access_type,
            Some(AccessType::Block(_))
        ));
    }

    /// The access mode is the PV's first (`expander.go:112-114`).
    #[tokio::test]
    async fn access_mode_is_the_pvs_first() {
        use rusternetes_kubelet::volume_plugins::csi_client::proto::volume_capability::access_mode::Mode;
        let f = fx(&[Cap::ExpandVolume], None).await;
        run(&f, &test_pv(false, false), "", true).await.unwrap();
        let mode = only_expand(&f)
            .volume_capability
            .unwrap()
            .access_mode
            .unwrap()
            .mode;
        assert_eq!(mode, Mode::MultiNodeMultiWriter as i32);
    }

    /// "has grpc volume-in-use error": FailedPrecondition is surfaced as a
    /// `FailedPrecondition` operation error.
    #[tokio::test]
    async fn failed_precondition_is_volume_in_use() {
        let f = fx(&[Cap::ExpandVolume], None).await;
        *f.fake.expand_error.lock().unwrap() = Some(tonic::Code::FailedPrecondition);
        let err = run(&f, &test_pv(false, false), "", true).await.unwrap_err();
        assert!(matches!(
            err.downcast_ref::<VolumeOperationError>(),
            Some(VolumeOperationError::FailedPrecondition(_))
        ));
    }

    /// `isInfeasibleError` (`expander.go:147-164`): InvalidArgument, OutOfRange
    /// and NotFound are infeasible; anything else is a plain error.
    #[tokio::test]
    async fn infeasible_codes_are_infeasible() {
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::OutOfRange,
            tonic::Code::NotFound,
        ] {
            let f = fx(&[Cap::ExpandVolume], None).await;
            *f.fake.expand_error.lock().unwrap() = Some(code);
            let err = run(&f, &test_pv(false, false), "", true).await.unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<VolumeOperationError>(),
                    Some(VolumeOperationError::Infeasible(_))
                ),
                "{code:?}"
            );
        }
        let f = fx(&[Cap::ExpandVolume], None).await;
        *f.fake.expand_error.lock().unwrap() = Some(tonic::Code::Internal);
        let err = run(&f, &test_pv(false, false), "", true).await.unwrap_err();
        assert!(err.downcast_ref::<VolumeOperationError>().is_none());
        assert!(err
            .to_string()
            .starts_with("Expander.NodeExpand failed to expand the volume : "));
    }

    /// `NodeExpandSecretRef` (`expander.go:85-91`): the Secret's data is sent.
    #[tokio::test]
    async fn node_expand_secret_is_sent() {
        let f = fx(
            &[Cap::ExpandVolume],
            Some(("default", "expand-secret", "csiusername")),
        )
        .await;
        run(&f, &test_pv(true, false), "/foo/bar", true)
            .await
            .unwrap();
        assert_eq!(only_expand(&f).secrets["apiUsername"], "csiusername");
    }

    /// `TestNodeExpandNoClientError`: an unregistered driver is a transient
    /// failure (`expander.go:45-49`).
    #[tokio::test]
    async fn unregistered_driver_is_transient() {
        let f = fx(&[Cap::ExpandVolume], None).await;
        let pv = test_pv(false, false);
        let vol = volume();
        let spec = Spec {
            volume: &vol,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let err = f
            .plugin
            .node_expand(resize(&spec, "/foo/bar"))
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<VolumeOperationError>(),
            Some(VolumeOperationError::TransientOperationFailure(_))
        ));
    }

    /// The plugin is discoverable as node-expandable and requires an FS resize
    /// (`expander.go:32-36`).
    #[tokio::test]
    async fn csi_is_a_node_expandable_plugin() {
        let f = fx(&[], None).await;
        let p = f.plugin.as_node_expandable_plugin().unwrap();
        assert!(p.requires_fs_resize());
    }
}
