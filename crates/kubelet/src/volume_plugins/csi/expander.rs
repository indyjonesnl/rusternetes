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
    match &pv.spec.volume_mode {
        Some(PersistentVolumeMode::Block) => Ok(false),
        // An unrecognised mode is rejected at admission; like upstream's
        // `CheckVolumeModeFilesystem` (only Block is non-filesystem) treat it
        // as a filesystem volume.
        Some(PersistentVolumeMode::Filesystem | PersistentVolumeMode::Unknown(_)) => Ok(true),
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

        // `createCSIOperationContext(resizeOptions.VolumeSpec, csiTimeout)`
        // (`expander.go:66`).
        let client = match CsiDriverClient::new(&csi_source.driver) {
            Ok(c) => c.with_operation_spec(Some(spec)),
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
    pub async fn node_expand_with_client(
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
