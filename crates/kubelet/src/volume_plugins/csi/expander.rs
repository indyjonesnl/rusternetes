//! Port of `pkg/volume/csi/expander.go` — `csiPlugin` as a
//! `volume.NodeExpandableVolumePlugin`.

use super::*;
use crate::volume_plugins::csi_client::{CsiResizeOptions, NodeExpandError, FS_TYPE_BLOCK_NAME};
use crate::volume_plugins::plugin::{NodeExpandableVolumePlugin, NodeResizeOptions};
use crate::volume_plugins::util::types::VolumeOperationError;
use rusternetes_common::resources::volume::{CSIVolumeSource, PersistentVolumeMode};

/// Port of `inUseError` (`expander.go:132-142`): a FailedPrecondition means the
/// driver does not support expansion of in-use volumes. A non-gRPC error (here
/// an uncertain-progress one, which upstream flattens to a message) is not one.
fn in_use_error(err: &NodeExpandError) -> bool {
    matches!(err, NodeExpandError::Grpc(s) if s.code() == tonic::Code::FailedPrecondition)
}

/// Port of `isInfeasibleError` (`expander.go:144-164`): the terminal gRPC codes
/// that say the operation is not possible in the current state.
fn is_infeasible_error(err: &NodeExpandError) -> bool {
    matches!(
        err,
        NodeExpandError::Grpc(s) if matches!(
            s.code(),
            tonic::Code::InvalidArgument | tonic::Code::OutOfRange | tonic::Code::NotFound
        )
    )
}

/// Port of `util.CheckVolumeModeFilesystem` + `GetVolumeMode`
/// (`pkg/volume/util/util.go:339-365`): a spec without a PersistentVolume is a
/// filesystem volume; a PV with no `volumeMode` is an error.
fn check_volume_mode_filesystem(spec: &Spec<'_>) -> Result<bool> {
    let Some(pv) = spec.persistent_volume else {
        return Ok(true);
    };
    match pv.spec.volume_mode {
        Some(PersistentVolumeMode::Block) => Ok(false),
        Some(PersistentVolumeMode::Filesystem) => Ok(true),
        None => Err(anyhow!("cannot get volumeMode for volume: {}", spec.name())),
    }
}

#[async_trait]
impl NodeExpandableVolumePlugin for CsiPlugin {
    /// `RequiresFSResize` (`expander.go:34-36`).
    fn requires_fs_resize(&self) -> bool {
        true
    }

    /// `NodeExpand` (`expander.go:38-57`).
    async fn node_expand(&self, resize_options: NodeResizeOptions<'_>) -> Result<bool> {
        debug!("Expander.NodeExpand({})", resize_options.device_mount_path);
        // `getCSISourceFromSpec` -> `getPVSourceFromSpec` (`csi_util.go:93`,
        // `165-174`): an inline CSIVolumeSource is rejected.
        let spec = resize_options.volume_spec;
        if spec.volume.csi.is_some() && spec.persistent_volume.is_some() {
            return Err(anyhow!(
                "Expander.NodeExpand failed to get CSI persistent source: volume.Spec has both volume and persistent volume sources"
            ));
        }
        let csi_source = match spec.persistent_volume.and_then(|pv| pv.spec.csi.as_ref()) {
            Some(src) => src,
            None if spec.volume.csi.is_some() => {
                return Err(anyhow!(
                    "Expander.NodeExpand failed to get CSI persistent source: unexpected api.CSIVolumeSource found in volume.Spec"
                ))
            }
            None => {
                return Err(anyhow!(
                    "Expander.NodeExpand failed to get CSI persistent source: volume source not found in volume.Spec"
                ))
            }
        };

        let client = match CsiDriverClient::new(&csi_source.driver) {
            Ok(c) => c,
            // Treat the absence of the CSI driver as a transient error
            // See https://github.com/kubernetes/kubernetes/issues/120268
            Err(e) => {
                return Err(VolumeOperationError::TransientOperationFailure(e.to_string()).into())
            }
        };
        let fs_volume = check_volume_mode_filesystem(spec).map_err(|e| {
            anyhow!("Expander.NodeExpand failed to check VolumeMode of source: {e}")
        })?;

        self.node_expand_with_client(resize_options, csi_source, &client, fs_volume)
            .await
    }
}

impl CsiPlugin {
    /// `nodeExpandWithClient` (`expander.go:59-130`).
    async fn node_expand_with_client(
        &self,
        resize_options: NodeResizeOptions<'_>,
        csi_source: &CSIVolumeSource,
        client: &CsiDriverClient,
        fs_volume: bool,
    ) -> Result<bool> {
        let driver_name = &csi_source.driver;

        let node_expand_set = client.node_supports_node_expand().await.map_err(|e| {
            anyhow!("Expander.NodeExpand failed to check if node supports expansion : {e}")
        })?;
        if !node_expand_set {
            return Err(VolumeOperationError::OperationNotSupported(format!(
                "NodeExpand is not supported by the CSI driver {driver_name}"
            ))
            .into());
        }

        let Some(pv) = resize_options.volume_spec.persistent_volume else {
            return Err(anyhow!(
                "Expander.NodeExpand failed to find associated PersistentVolume for plugin {}",
                self.name()
            ));
        };

        let mut node_expand_secrets = HashMap::new();
        if let Some(r) = csi_source.node_expand_secret_ref.as_ref() {
            let ns = r.namespace.clone().unwrap_or_default();
            let name = r.name.clone().unwrap_or_default();
            node_expand_secrets =
                get_credentials_from_secret(self.host.get_kube_client(), &ns, &name)
                    .await
                    .map_err(|e| {
                        anyhow!(
                        "expander.NodeExpand failed to get NodeExpandSecretRef {ns}/{name}: {e}"
                    )
                    })?;
        }

        let mut opts = CsiResizeOptions {
            volume_path: resize_options.device_mount_path.clone(),
            staging_target_path: resize_options.device_stage_path.clone(),
            volume_id: csi_source.volume_handle.clone().unwrap_or_default(),
            new_size: resize_options.new_size,
            fs_type: csi_source.fs_type.clone().unwrap_or_default(),
            access_mode: PersistentVolumeAccessMode::ReadWriteOnce,
            mount_options: pv.spec.mount_options.clone().unwrap_or_default(),
            secrets: node_expand_secrets,
        };

        if !fs_volume {
            // for block volumes the volumePath in CSI NodeExpandvolumeRequest is
            // basically same as DevicePath because block devices are not mounted and hence
            // DeviceMountPath does not get populated in resizeOptions.DeviceMountPath
            opts.volume_path = resize_options.device_path.clone();
            opts.fs_type = FS_TYPE_BLOCK_NAME.to_string();
        }

        if let Some(mode) = pv.spec.access_modes.first() {
            opts.access_mode = mode.clone();
        }

        match client.node_expand_volume(&opts).await {
            Ok(_) => Ok(true),
            Err(e) if in_use_error(&e) => Err(VolumeOperationError::FailedPrecondition(format!(
                "Expander.NodeExpand failed to expand the volume : {e}"
            ))
            .into()),
            Err(e) if is_infeasible_error(&e) => Err(VolumeOperationError::Infeasible(format!(
                "Expander.NodeExpand failed to expand the volume {e}"
            ))
            .into()),
            Err(e) => Err(anyhow!(
                "Expander.NodeExpand failed to expand the volume : {e}"
            )),
        }
    }
}

/// Ported in shape from `TestNodeExpand` and `TestNodeExpandNoClientError`
/// (`pkg/volume/csi/expander_test.go:34-230`). Upstream's fake `csiClient` is
/// a real gRPC Node server on a unix socket here.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_plugins::csi_client::fake::{serve, FakeDriver};
    use crate::volume_plugins::csi_client::proto::volume_capability::AccessType;
    use crate::volume_plugins::csi_client::proto::{
        node_service_capability::rpc::Type as Cap, NodeExpandVolumeRequest,
    };
    use rusternetes_common::quantity::Quantity;
    use rusternetes_common::resources::{Secret, Volume};
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
        let host = Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
            dir.path().to_string_lossy().to_string(),
            Some(storage),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        ));
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
        use crate::volume_plugins::csi_client::proto::volume_capability::access_mode::Mode;
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
