//! Block-volume support for the CSI plugin.
//!
//! Port of `pkg/volume/csi/csi_block.go` (`csiBlockMapper`) and the
//! `BlockVolumePlugin` half of `pkg/volume/csi/csi_plugin.go:726-856`
//! (`NewBlockVolumeMapper`, `NewBlockVolumeUnmapper`, `ConstructBlockVolumeSpec`),
//! with the path helpers of `csi_util.go:111-129`.
//!
//! The CSI driver stages/publishes the device; mapping it into the global map
//! path and the pod device map path is the operation executor's job
//! (`MapBlockVolume`/`UnmapBlockVolume`), not this file's. Upstream's summary of
//! the sequence (`csi_block.go:17-70`):
//!
//! - `SetUpDevice` calls `NodeStageVolume` (staging path),
//! - `MapPodDevice` calls `NodePublishVolume` (publish path),
//! - `UnmapPodDevice` calls `NodeUnpublishVolume`,
//! - `TearDownDevice` calls `NodeUnstageVolume`.

use super::metrics::new_metrics_csi;
use super::{
    get_attachment_name, get_credentials_from_secret, load_volume_data, make_dir_0750,
    pv_source_from_spec, remove_mount_dir, save_volume_data, transient, vol_data_key, CsiPlugin,
    VolumeLifecycleMode,
};
use crate::volume_plugins::csi_client::{CsiDriverClient, CsiError};
use crate::volume_plugins::plugin::{
    BlockVolumeMapper, BlockVolumePlugin, BlockVolumeUnmapper, CustomBlockVolumeMapper,
    CustomBlockVolumeUnmapper, Metrics, MetricsProvider, OwnedSpec, Spec,
};
use crate::volume_plugins::util::types::is_operation_finished_error;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{PersistentVolumeAccessMode, Pod, VolumeAttachment};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, error, info};

/// `fsTypeBlockName` (`csi_plugin.go:59`).
use crate::volume_plugins::csi_client::FS_TYPE_BLOCK_NAME;

/// `volumetypes.IsOperationFinishedError` for the errors this file returns:
/// the CSI client's [`CsiError`] (`csi_client.go`'s uncertain-progress and
/// transient errors) and the plugin's [`transient`] ones are typed, the rest
/// are plain `errors.New` and so finished.
fn is_finished(err: &anyhow::Error) -> bool {
    match err.downcast_ref::<CsiError>() {
        Some(e) => e.is_operation_finished(),
        None => is_operation_finished_error(err),
    }
}

/// `getVolumePluginDir` (`csi_util.go:111-115`): where the CSI plugin keeps
/// metadata for a given volume, `plugins/kubernetes.io/csi/volumeDevices/{specVolID}`.
fn get_volume_plugin_dir(spec_vol_id: &str, plugin: &CsiPlugin) -> PathBuf {
    Path::new(
        &plugin
            .host
            .get_volume_device_plugin_dir(crate::pod_dirs::plugin::CSI),
    )
    .join(crate::pod_dirs::escape_qualified_name(spec_vol_id))
}

/// `getVolumeDevicePluginDir` (`csi_util.go:117-122`):
/// `plugins/kubernetes.io/csi/volumeDevices/{specVolumeID}/dev`.
fn get_volume_device_plugin_dir(spec_vol_id: &str, plugin: &CsiPlugin) -> PathBuf {
    get_volume_plugin_dir(spec_vol_id, plugin).join("dev")
}

/// `getVolumeDeviceDataDir` (`csi_util.go:124-129`):
/// `plugins/kubernetes.io/csi/volumeDevices/{specVolumeID}/data`.
fn get_volume_device_data_dir(spec_vol_id: &str, plugin: &CsiPlugin) -> PathBuf {
    get_volume_plugin_dir(spec_vol_id, plugin).join("data")
}

/// Port of `csiBlockMapper` (`csi_block.go:84-95`): both the
/// `BlockVolumeMapper` and the `BlockVolumeUnmapper` (the unmapper is the same
/// struct with only `plugin`, `podUID`, `specName`, `driverName` and
/// `volumeID` filled in).
///
/// `csiClientGetter` is not a field: `csiClientGetter.Get()` is
/// `CsiDriverClient::new(driver_name)`, called where it is used.
struct CsiBlockMapper {
    /// `k8s`; `None` for an unmapper, as upstream's nil.
    storage: Option<Arc<StorageBackend>>,
    plugin: CsiPlugin,
    driver_name: String,
    spec_name: String,
    volume_id: String,
    read_only: bool,
    spec: Option<OwnedSpec>,
    pod: Option<Pod>,
    pod_uid: String,
    /// `m.plugin.host.GetNodeName()` (`csi_block.go:272`, `:337`). `VolumeHost`
    /// here has no node name (see `verify_exhausted_resource`), so it is read
    /// off the pod the mapper was built for, as `CsiMounter` does.
    node_name: String,
}

impl BlockVolumePlugin for CsiPlugin {
    /// `NewBlockVolumeMapper` (`csi_plugin.go:729-803`).
    fn new_block_volume_mapper(
        &self,
        spec: &Spec<'_>,
        pod: &Pod,
    ) -> Result<Box<dyn BlockVolumeMapper>> {
        let pv_source = pv_source_from_spec(spec)?;
        // `getReadOnlyFromSpec` (`csi_util.go:97-104`).
        if spec
            .persistent_volume
            .and_then(|pv| pv.spec.csi.as_ref())
            .is_none()
        {
            return Err(anyhow!("CSIPersistentVolumeSource not defined in spec"));
        }
        let read_only = spec.read_only;
        let volume_handle = pv_source.volume_handle.clone().unwrap_or_default();

        debug!(
            "kubernetes.io/csi: setting up block mapper for [volume={},driver={}]",
            volume_handle, pv_source.driver
        );

        let Some(storage) = self.host.get_kube_client().cloned() else {
            return Err(anyhow!(
                "kubernetes.io/csi: failed to get a kubernetes client"
            ));
        };

        let spec_name = CsiPlugin::spec_name(spec);
        let node_name = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .unwrap_or_default();
        let mapper = CsiBlockMapper {
            storage: Some(storage),
            plugin: CsiPlugin::new(self.host.clone()),
            driver_name: pv_source.driver.clone(),
            spec_name: spec_name.clone(),
            volume_id: volume_handle.clone(),
            read_only,
            spec: Some(spec.to_owned_spec()),
            pod: Some(pod.clone()),
            pod_uid: pod.metadata.uid.clone(),
            node_name: node_name.clone(),
        };

        // Save volume info in pod dir.
        let data_dir = get_volume_device_data_dir(&spec_name, self);
        make_dir_0750(&data_dir).map_err(|e| {
            anyhow!(
                "kubernetes.io/csi: failed to create data dir {}:  {e}",
                data_dir.display()
            )
        })?;
        debug!(
            "kubernetes.io/csi: created path successfully [{}]",
            data_dir.display()
        );

        // Persist volume info data for teardown.
        let attach_id = get_attachment_name(&volume_handle, &pv_source.driver, &node_name);
        let vol_data = HashMap::from([
            (vol_data_key::SPEC_VOL_ID.to_string(), spec_name),
            (vol_data_key::VOL_HANDLE.to_string(), volume_handle),
            (
                vol_data_key::DRIVER_NAME.to_string(),
                pv_source.driver.clone(),
            ),
            (vol_data_key::NODE_NAME.to_string(), node_name),
            (vol_data_key::ATTACHMENT_ID.to_string(), attach_id),
        ]);
        if let Err(e) = save_volume_data(&data_dir, &vol_data) {
            let msg = format!(
                "kubernetes.io/csi: csi.NewBlockVolumeMapper failed to save volume info data: {e}"
            );
            error!("{msg}");
            // Only if there was an error and the volume operation was
            // considered finished, we should remove the directory.
            if is_finished(&e) {
                if let Err(re) = remove_mount_dir(&data_dir) {
                    error!(
                        "kubernetes.io/csi: attacher.MountDevice failed to remove mount dir after error [{}]: {re}",
                        data_dir.display()
                    );
                }
            }
            return Err(anyhow!(msg));
        }

        Ok(Box::new(mapper))
    }

    /// `NewBlockVolumeUnmapper` (`csi_plugin.go:805-824`).
    fn new_block_volume_unmapper(
        &self,
        name: &str,
        pod_uid: &str,
    ) -> Result<Box<dyn BlockVolumeUnmapper>> {
        debug!("kubernetes.io/csi: setting up block unmapper for [Spec={name}, podUID={pod_uid}]");

        // Load volume info from file.
        let data_dir = get_volume_device_data_dir(name, self);
        let data = load_volume_data(&data_dir).map_err(|e| {
            anyhow!(
                "kubernetes.io/csi: unmapper failed to load volume data file [{}]: {e}",
                data_dir.display()
            )
        })?;
        Ok(Box::new(CsiBlockMapper {
            storage: None,
            plugin: CsiPlugin::new(self.host.clone()),
            driver_name: data
                .get(vol_data_key::DRIVER_NAME)
                .cloned()
                .unwrap_or_default(),
            spec_name: name.to_string(),
            volume_id: data
                .get(vol_data_key::VOL_HANDLE)
                .cloned()
                .unwrap_or_default(),
            read_only: false,
            spec: None,
            pod: None,
            pod_uid: pod_uid.to_string(),
            node_name: String::new(),
        }))
    }

    /// `ConstructBlockVolumeSpec` (`csi_plugin.go:826-856`): a PV with a CSI
    /// source (driver and handle only) and `VolumeMode: Block`.
    fn construct_block_volume_spec(
        &self,
        pod_uid: &str,
        volume_name: &str,
        volume_path: &str,
    ) -> Result<OwnedSpec> {
        debug!(
            "kubernetes.io/csi: plugin.ConstructBlockVolumeSpec [podUID={pod_uid}, specVolName={volume_name}, path={volume_path}]"
        );

        let data_dir = get_volume_device_data_dir(volume_name, self);
        let vol_data = load_volume_data(&data_dir).map_err(|e| {
            anyhow!(
                "kubernetes.io/csi: plugin.ConstructBlockVolumeSpec failed loading volume data using [{volume_path}]: {e}"
            )
        })?;
        debug!("kubernetes.io/csi: plugin.ConstructBlockVolumeSpec extracted [{vol_data:?}]");

        let get = |k: &str| vol_data.get(k).cloned().unwrap_or_default();
        let spec_vol_id = get(vol_data_key::SPEC_VOL_ID);
        let pv: rusternetes_common::resources::PersistentVolume =
            serde_json::from_value(serde_json::json!({
                "metadata": {"name": spec_vol_id},
                "spec": {
                    "csi": {
                        "driver": get(vol_data_key::DRIVER_NAME),
                        "volumeHandle": get(vol_data_key::VOL_HANDLE),
                    },
                    "volumeMode": "Block",
                }
            }))?;
        // `volume.NewSpecFromPersistentVolume(pv, false)`: our `Spec` needs a
        // `Volume`, named after the PV (see `Spec::name`).
        let volume = serde_json::from_value(serde_json::json!({"name": spec_vol_id}))?;
        Ok(OwnedSpec {
            volume,
            persistent_volume: Some(pv),
            read_only: false,
        })
    }
}

impl CsiPlugin {
    /// `podInfoEnabled` (`csi_plugin.go:944-960`): the driver's
    /// `CSIDriver.Spec.PodInfoOnMount`. A missing CSIDriver is `false`.
    fn pod_info_enabled(&self, driver_name: &str) -> Result<bool> {
        match self.get_csi_driver(driver_name)? {
            None => {
                debug!(
                    "kubernetes.io/csi: CSIDriver {driver_name:?} not found, not adding pod information"
                );
                Ok(false)
            }
            Some(d) if d.spec.pod_info_on_mount == Some(true) => Ok(true),
            Some(_) => {
                debug!(
                    "kubernetes.io/csi: CSIDriver {driver_name:?} does not require pod information"
                );
                Ok(false)
            }
        }
    }
}

/// `getPodInfoAttrs` (`csi_util.go:207-217`).
fn get_pod_info_attrs(pod: &Pod, mode: VolumeLifecycleMode) -> HashMap<String, String> {
    HashMap::from([
        (
            "csi.storage.k8s.io/pod.name".into(),
            pod.metadata.name.clone(),
        ),
        (
            "csi.storage.k8s.io/pod.namespace".into(),
            pod.metadata.namespace.clone().unwrap_or_default(),
        ),
        (
            "csi.storage.k8s.io/pod.uid".into(),
            pod.metadata.uid.clone(),
        ),
        (
            "csi.storage.k8s.io/serviceAccount.name".into(),
            pod.spec
                .as_ref()
                .and_then(|s| s.service_account_name.clone())
                .unwrap_or_default(),
        ),
        (
            "csi.storage.k8s.io/ephemeral".into(),
            (mode == VolumeLifecycleMode::Ephemeral).to_string(),
        ),
    ])
}

impl CsiBlockMapper {
    /// `GetGlobalMapPath` (`csi_block.go:101-105`).
    fn global_map_path(&self) -> String {
        get_volume_device_plugin_dir(&self.spec_name, &self.plugin)
            .to_string_lossy()
            .into_owned()
    }

    /// `GetStagingPath` (`csi_block.go:109-111`):
    /// `plugins/kubernetes.io/csi/volumeDevices/staging/{specName}`.
    fn staging_path(&self) -> String {
        Path::new(
            &self
                .plugin
                .host
                .get_volume_device_plugin_dir(crate::pod_dirs::plugin::CSI),
        )
        .join("staging")
        .join(&self.spec_name)
        .to_string_lossy()
        .into_owned()
    }

    /// `getPublishDir` (`csi_block.go:121-123`):
    /// `plugins/kubernetes.io/csi/volumeDevices/publish/{specName}`.
    fn publish_dir(&self) -> PathBuf {
        Path::new(
            &self
                .plugin
                .host
                .get_volume_device_plugin_dir(crate::pod_dirs::plugin::CSI),
        )
        .join("publish")
        .join(&self.spec_name)
    }

    /// `getPublishPath` (`csi_block.go:127-129`):
    /// `plugins/kubernetes.io/csi/volumeDevices/publish/{specName}/{podUID}`.
    fn publish_path(&self) -> PathBuf {
        self.publish_dir().join(&self.pod_uid)
    }

    /// `GetPodDeviceMapPath` (`csi_block.go:133-137`):
    /// `pods/{podUID}/volumeDevices/kubernetes.io~csi`, `{specName}`.
    fn pod_device_map_path(&self) -> (String, String) {
        let path = self.plugin.host.get_pod_volume_device_dir(
            &self.pod_uid,
            &crate::pod_dirs::escape_qualified_name(crate::pod_dirs::plugin::CSI),
        );
        debug!(
            "kubernetes.io/csi: blockMapper.GetPodDeviceMapPath [path={path}; name={}]",
            self.spec_name
        );
        (path, self.spec_name.clone())
    }

    /// The VolumeAttachment lookup `SetUpDevice` and `MapPodDevice` share
    /// (`csi_block.go:268-281`, `:333-346`); `what` is the method name used in
    /// the transient error.
    async fn get_attachment(
        &self,
        what: &str,
        driver: &str,
        volume_handle: &str,
    ) -> Result<Option<VolumeAttachment>> {
        let skip = self.plugin.skip_attach(driver).map_err(|e| {
            anyhow!(
                "kubernetes.io/csi: blockMapper.{what} failed to check CSIDriver for {driver}: {e}"
            )
        })?;
        if skip {
            return Ok(None);
        }
        // Search for attachment by VolumeAttachment.Spec.Source.PersistentVolumeName.
        let attach_id = get_attachment_name(volume_handle, driver, &self.node_name);
        let storage = self.storage.as_ref().ok_or_else(|| {
            anyhow!("kubernetes.io/csi: blockMapper.{what} failed to get a kubernetes client")
        })?;
        storage
            .get::<VolumeAttachment>(&build_key("volumeattachments", None, &attach_id))
            .await
            .map(Some)
            .map_err(|e| {
                transient(format!(
                    "kubernetes.io/csi: blockMapper.{what} failed to get volume attachment [id={attach_id}]: {e}"
                ))
            })
    }

    /// `stageVolumeForBlock` (`csi_block.go:140-197`). Returns the staging
    /// path, or `""` when the driver has no `STAGE_UNSTAGE_VOLUME`.
    async fn stage_volume_for_block(
        &self,
        client: &CsiDriverClient,
        access_mode: &PersistentVolumeAccessMode,
        csi_source: &rusternetes_common::resources::volume::CSIVolumeSource,
        attachment: Option<&VolumeAttachment>,
    ) -> Result<String> {
        debug!("kubernetes.io/csi: blockMapper.stageVolumeForBlock called");

        let staging_path = self.staging_path();
        debug!(
            "kubernetes.io/csi: blockMapper.stageVolumeForBlock stagingPath set [{staging_path}]"
        );

        // Check whether "STAGE_UNSTAGE_VOLUME" is set.
        let stage_unstage_set = client.node_supports_stage_unstage().await.map_err(|e| {
            anyhow!("kubernetes.io/csi: blockMapper.stageVolumeForBlock failed to check STAGE_UNSTAGE_VOLUME capability: {e}")
        })?;
        if !stage_unstage_set {
            info!("kubernetes.io/csi: blockMapper.stageVolumeForBlock STAGE_UNSTAGE_VOLUME capability not set. Skipping MountDevice...");
            return Ok(String::new());
        }
        let publish_volume_info = attachment
            .and_then(|a| a.status.as_ref())
            .and_then(|s| s.attachment_metadata.clone())
            .unwrap_or_default();
        let mut node_stage_secrets = HashMap::new();
        if let Some(r) = csi_source.node_stage_secret_ref.as_ref() {
            let (ns, name) = (
                r.namespace.as_deref().unwrap_or_default(),
                r.name.as_deref().unwrap_or_default(),
            );
            node_stage_secrets = get_credentials_from_secret(self.storage.as_ref(), ns, name)
                .await
                .map_err(|e| {
                    transient(format!(
                        "kubernetes.io/csi: failed to get NodeStageSecretRef {ns}/{name}: {e}"
                    ))
                })?;
        }

        // Creating a stagingPath directory before call to NodeStageVolume.
        make_dir_0750(Path::new(&staging_path)).map_err(|e| {
            anyhow!("kubernetes.io/csi: blockMapper.stageVolumeForBlock failed to create dir {staging_path}: {e}")
        })?;
        debug!("kubernetes.io/csi: blockMapper.stageVolumeForBlock created stagingPath directory successfully [{staging_path}]");

        // Request to stage a block volume to stagingPath. Expected
        // implementation for the driver is creating a driver specific resource
        // on stagingPath and attaching the block volume to the node.
        client
            .node_stage_volume(
                csi_source.volume_handle.as_deref().unwrap_or_default(),
                publish_volume_info,
                &staging_path,
                FS_TYPE_BLOCK_NAME,
                access_mode,
                node_stage_secrets,
                csi_source.volume_attributes.clone().unwrap_or_default(),
                &[],  /* MountOptions */
                None, /* fsGroup */
            )
            .await?;

        debug!("kubernetes.io/csi: blockMapper.stageVolumeForBlock successfully requested NodeStageVolume [{staging_path}]");
        Ok(staging_path)
    }

    /// `publishVolumeForBlock` (`csi_block.go:200-269`).
    async fn publish_volume_for_block(
        &self,
        client: &CsiDriverClient,
        access_mode: &PersistentVolumeAccessMode,
        csi_source: &rusternetes_common::resources::volume::CSIVolumeSource,
        attachment: Option<&VolumeAttachment>,
    ) -> Result<String> {
        debug!("kubernetes.io/csi: blockMapper.publishVolumeForBlock called");

        let publish_volume_info = attachment
            .and_then(|a| a.status.as_ref())
            .and_then(|s| s.attachment_metadata.clone())
            .unwrap_or_default();

        // Inject pod information into volume_attributes.
        let mut vol_attribs = csi_source.volume_attributes.clone().unwrap_or_default();
        let pod_info_enabled = self.plugin.pod_info_enabled(&self.driver_name).map_err(|e| {
            transient(format!("kubernetes.io/csi: blockMapper.publishVolumeForBlock failed to assemble volume attributes: {e}"))
        })?;
        let spec = self.spec.as_ref().map(|s| s.as_spec());
        let mode = match spec.as_ref() {
            Some(spec) => CsiPlugin::get_volume_lifecycle_mode(spec),
            None => Err(anyhow!("volume.Spec nil")),
        }
        .map_err(|e| {
            transient(format!("kubernetes.io/csi: blockMapper.publishVolumeForBlock failed to get VolumeLifecycleMode: {e}"))
        })?;
        if pod_info_enabled {
            if let Some(pod) = self.pod.as_ref() {
                vol_attribs.extend(get_pod_info_attrs(pod, mode));
            }
        }

        let mut node_publish_secrets = HashMap::new();
        if let Some(r) = csi_source.node_publish_secret_ref.as_ref() {
            let (ns, name) = (
                r.namespace.as_deref().unwrap_or_default(),
                r.name.as_deref().unwrap_or_default(),
            );
            node_publish_secrets = get_credentials_from_secret(self.storage.as_ref(), ns, name)
                .await
                .map_err(|e| {
                    transient(format!(
                        "kubernetes.io/csi: blockMapper.publishVolumeForBlock failed to get NodePublishSecretRef {ns}/{name}: {e}"
                    ))
                })?;
        }

        let publish_path = self.publish_path();
        // Setup a parent directory for publishPath before call to NodePublishVolume.
        let publish_dir = publish_path
            .parent()
            .expect("publish path has a parent")
            .to_path_buf();
        make_dir_0750(&publish_dir).map_err(|e| {
            anyhow!(
                "kubernetes.io/csi: blockMapper.publishVolumeForBlock failed to create dir {}:  {e}",
                publish_dir.display()
            )
        })?;
        debug!(
            "kubernetes.io/csi: blockMapper.publishVolumeForBlock created directory for publishPath successfully [{}]",
            publish_dir.display()
        );

        // Request to publish a block volume to publishPath. Expectation for
        // the driver is to place a block volume on the publishPath, by
        // bind-mounting the device file on the publishPath or creating a
        // device file on the publishPath. The parent directory for
        // publishPath is created by k8s, but the driver is responsible for
        // creating publishPath itself. If the driver doesn't implement
        // NodeStageVolume, attaching the block volume to the node may be
        // done here.
        let publish_path_str = publish_path.to_string_lossy().into_owned();
        client
            .node_publish_volume(
                &self.volume_id,
                self.read_only,
                &self.staging_path(),
                &publish_path_str,
                access_mode,
                publish_volume_info,
                vol_attribs,
                node_publish_secrets,
                FS_TYPE_BLOCK_NAME,
                &[],  /* mountOptions */
                None, /* fsGroup */
            )
            .await?;

        Ok(publish_path_str)
    }

    /// `unpublishVolumeForBlock` (`csi_block.go:365-378`).
    async fn unpublish_volume_for_block(
        &self,
        client: &CsiDriverClient,
        publish_path: &str,
    ) -> Result<()> {
        // Request to unpublish a block volume from publishPath. Expectation
        // for the driver is to remove the block volume from the publishPath,
        // by unmounting the bind-mounted device file or deleting the device
        // file. The driver is responsible for deleting publishPath itself.
        client
            .node_unpublish_volume(&self.volume_id, publish_path)
            .await
            .map_err(|e| {
                anyhow!("kubernetes.io/csi: blockMapper.unpublishVolumeForBlock failed: {e}")
            })?;
        debug!("kubernetes.io/csi: blockMapper.unpublishVolumeForBlock NodeUnpublished successfully [{publish_path}]");
        Ok(())
    }

    /// `unstageVolumeForBlock` (`csi_block.go:380-407`).
    async fn unstage_volume_for_block(
        &self,
        client: &CsiDriverClient,
        staging_path: &str,
    ) -> Result<()> {
        // Check whether "STAGE_UNSTAGE_VOLUME" is set.
        let stage_unstage_set = client.node_supports_stage_unstage().await.map_err(|e| {
            anyhow!("kubernetes.io/csi: blockMapper.unstageVolumeForBlock failed to check STAGE_UNSTAGE_VOLUME capability: {e}")
        })?;
        if !stage_unstage_set {
            info!("kubernetes.io/csi: blockMapper.unstageVolumeForBlock STAGE_UNSTAGE_VOLUME capability not set. Skipping unstageVolumeForBlock ...");
            return Ok(());
        }

        // Request to unstage a block volume from stagingPath. Expected
        // implementation for the driver is removing the driver specific
        // resource in stagingPath and detaching the block volume from the node.
        client
            .node_unstage_volume(&self.volume_id, staging_path)
            .await
            .map_err(|e| {
                anyhow!("kubernetes.io/csi: blockMapper.unstageVolumeForBlock failed: {e}")
            })?;
        debug!("kubernetes.io/csi: blockMapper.unstageVolumeForBlock NodeUnstageVolume successfully [{staging_path}]");

        // Remove stagingPath directory and its contents.
        match std::fs::remove_dir_all(staging_path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(anyhow!(
                "kubernetes.io/csi: blockMapper.unstageVolumeForBlock failed to remove staging path after NodeUnstageVolume() error [{staging_path}]: {e}"
            )),
            _ => Ok(()),
        }
    }

    /// `cleanupOrphanDeviceFiles` (`csi_block.go:446-472`): clean up any
    /// orphan files / directories when a block volume is being unstaged. At
    /// this point there is no pod using the volume and all files are indeed
    /// orphaned.
    fn cleanup_orphan_device_files(&self) -> Result<()> {
        let not_exist = |e: &std::io::Error| e.kind() == std::io::ErrorKind::NotFound;

        // Remove artifacts of NodePublish. Each PublishVolume() created a
        // subdirectory there. Since everything should be already unpublished
        // at this point, the directory should be empty by now.
        let publish_dir = self.publish_dir();
        if let Err(e) = std::fs::remove_dir(&publish_dir) {
            if !not_exist(&e) {
                return Err(anyhow!(
                    "kubernetes.io/csi: failed to remove publish directory [{}]: {e}",
                    publish_dir.display()
                ));
            }
        }

        // Remove artifacts of NodeStage.
        let staging_path = self.staging_path();
        if let Err(e) = std::fs::remove_dir(&staging_path) {
            if !not_exist(&e) {
                return Err(anyhow!(
                    "kubernetes.io/csi: failed to delete volume staging path [{staging_path}]: {e}"
                ));
            }
        }

        // Remove everything under .../volumeDevices/<volume name>. At this
        // point it contains only "data/vol_data.json" and empty "dev/".
        let volume_dir = get_volume_plugin_dir(&self.spec_name, &self.plugin);
        crate::removeall::remove_all_one_filesystem(&volume_dir)?;
        Ok(())
    }

    /// The access mode `SetUpDevice` and `MapPodDevice` use.
    ///
    /// TODO (vladimirvivien) implement better AccessModes mapping between k8s
    /// and CSI — upstream's own TODO (`csi_block.go:288-292`).
    fn access_mode(spec: &Spec<'_>) -> PersistentVolumeAccessMode {
        spec.persistent_volume
            .and_then(|pv| pv.spec.access_modes.first().cloned())
            .unwrap_or(PersistentVolumeAccessMode::ReadWriteOnce)
    }
}

impl MetricsProvider for CsiBlockMapper {
    /// Upstream sets `MetricsProvider = NewMetricsCsi(volumeHandle,
    /// blockPath+"/"+podUID, driverName)` (`csi_plugin.go:772`), which asks
    /// the driver for `NodeGetVolumeStats`.
    fn get_metrics(&self) -> Result<Metrics> {
        let path = format!("{}/{}", self.global_map_path(), self.pod_uid);
        new_metrics_csi(&self.volume_id, &path, &self.driver_name).get_metrics()
    }
}

impl BlockVolumeMapper for CsiBlockMapper {
    fn get_global_map_path(&self, _spec: &Spec<'_>) -> Result<String> {
        let dir = self.global_map_path();
        debug!("kubernetes.io/csi: blockMapper.GetGlobalMapPath = {dir}");
        Ok(dir)
    }

    fn get_pod_device_map_path(&self) -> (String, String) {
        self.pod_device_map_path()
    }

    /// `SupportsMetrics` (`csi_block.go:114-117`).
    fn supports_metrics(&self) -> bool {
        true
    }

    /// `var _ volume.CustomBlockVolumeMapper = &csiBlockMapper{}`
    /// (`csi_block.go:98`).
    fn as_custom_block_volume_mapper(&self) -> Option<&dyn CustomBlockVolumeMapper> {
        Some(self)
    }
}

impl BlockVolumeUnmapper for CsiBlockMapper {
    fn get_global_map_path(&self, _spec: &Spec<'_>) -> Result<String> {
        let dir = self.global_map_path();
        debug!("kubernetes.io/csi: blockMapper.GetGlobalMapPath = {dir}");
        Ok(dir)
    }

    fn get_pod_device_map_path(&self) -> (String, String) {
        self.pod_device_map_path()
    }

    fn supports_metrics(&self) -> bool {
        true
    }

    /// `var _ volume.CustomBlockVolumeUnmapper = &csiBlockMapper{}`
    /// (`csi_block.go:363`).
    fn as_custom_block_volume_unmapper(&self) -> Option<&dyn CustomBlockVolumeUnmapper> {
        Some(self)
    }
}

#[async_trait]
impl CustomBlockVolumeMapper for CsiBlockMapper {
    /// `SetUpDevice` (`csi_block.go:245-319`): ensures the device is attached
    /// and returns the path where the device is located.
    async fn set_up_device(&self) -> Result<String> {
        debug!("kubernetes.io/csi: blockMapper.SetUpDevice called");

        // Get csiSource from spec.
        let Some(owned) = self.spec.as_ref() else {
            return Err(anyhow!(
                "kubernetes.io/csi: blockMapper.SetUpDevice spec is nil"
            ));
        };
        let spec = owned.as_spec();
        let csi_source = pv_source_from_spec(&spec).map_err(|e| {
            anyhow!("kubernetes.io/csi: blockMapper.SetUpDevice failed to get CSI persistent source: {e}")
        })?;

        let attachment = self
            .get_attachment(
                "SetupDevice",
                &csi_source.driver,
                csi_source.volume_handle.as_deref().unwrap_or_default(),
            )
            .await?;
        let access_mode = Self::access_mode(&spec);

        // Treat the absence of the CSI driver as a transient error.
        // See https://github.com/kubernetes/kubernetes/issues/120268
        let client = CsiDriverClient::new(&self.driver_name).map_err(|e| {
            transient(format!(
                "kubernetes.io/csi: blockMapper.SetUpDevice failed to get CSI client: {e}"
            ))
        })?;

        // Call NodeStageVolume.
        match self
            .stage_volume_for_block(&client, &access_mode, csi_source, attachment.as_ref())
            .await
        {
            Ok(staging_path) => Ok(staging_path),
            Err(e) => {
                if is_finished(&e) {
                    if let Err(ce) = self.cleanup_orphan_device_files() {
                        // V(4) for not so serious error.
                        debug!("Failed to clean up block volume directory {ce}");
                    }
                }
                Err(e)
            }
        }
    }

    /// `MapPodDevice` (`csi_block.go:321-363`).
    async fn map_pod_device(&self) -> Result<String> {
        debug!("kubernetes.io/csi: blockMapper.MapPodDevice called");

        // Get csiSource from spec.
        let Some(owned) = self.spec.as_ref() else {
            return Err(anyhow!(
                "kubernetes.io/csi: blockMapper.MapPodDevice spec is nil"
            ));
        };
        let spec = owned.as_spec();
        let csi_source = pv_source_from_spec(&spec).map_err(|e| {
            anyhow!("kubernetes.io/csi: blockMapper.MapPodDevice failed to get CSI persistent source: {e}")
        })?;

        let attachment = self
            .get_attachment(
                "MapPodDevice",
                &csi_source.driver,
                csi_source.volume_handle.as_deref().unwrap_or_default(),
            )
            .await?;
        let access_mode = Self::access_mode(&spec);

        let client = CsiDriverClient::new(&self.driver_name).map_err(|e| {
            transient(format!(
                "kubernetes.io/csi: blockMapper.MapPodDevice failed to get CSI client: {e}"
            ))
        })?;

        // Call NodePublishVolume.
        self.publish_volume_for_block(&client, &access_mode, csi_source, attachment.as_ref())
            .await
    }

    /// `GetStagingPath` (`csi_block.go:109-111`).
    fn get_staging_path(&self) -> String {
        self.staging_path()
    }
}

#[async_trait]
impl CustomBlockVolumeUnmapper for CsiBlockMapper {
    /// `TearDownDevice` (`csi_block.go:410-444`): removes traces of the
    /// `SetUpDevice`.
    async fn tear_down_device(&self, _global_map_path: &str, _device_path: &str) -> Result<()> {
        // Treat the absence of the CSI driver as a transient error.
        // See https://github.com/kubernetes/kubernetes/issues/120268
        let client = CsiDriverClient::new(&self.driver_name).map_err(|e| {
            transient(format!(
                "kubernetes.io/csi: blockMapper.TearDownDevice failed to get CSI client: {e}"
            ))
        })?;

        // Call NodeUnstageVolume.
        let staging_path = self.staging_path();
        match std::fs::metadata(&staging_path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("kubernetes.io/csi: blockMapper.TearDownDevice stagingPath({staging_path}) has already been deleted, skip calling NodeUnstageVolume");
            }
            Err(e) => return Err(e.into()),
            Ok(_) => {
                self.unstage_volume_for_block(&client, &staging_path)
                    .await?
            }
        }
        if let Err(e) = self.cleanup_orphan_device_files() {
            // V(4) for not so serious error.
            debug!("Failed to clean up block volume directory {e}");
        }
        Ok(())
    }

    /// `UnmapPodDevice` (`csi_block.go:474-496`): unmaps the block device path.
    async fn unmap_pod_device(&self) -> Result<()> {
        let publish_path = self.publish_path();

        // Treat the absence of the CSI driver as a transient error.
        // See https://github.com/kubernetes/kubernetes/issues/120268
        let client = CsiDriverClient::new(&self.driver_name).map_err(|e| {
            transient(format!(
                "kubernetes.io/csi: blockMapper.UnmapPodDevice failed to get CSI client: {e}"
            ))
        })?;

        // Call NodeUnpublishVolume. Even if publishPath does not exist -
        // previous NodePublish may have timed out and Kubernetes makes sure
        // that the operation is finished.
        self.unpublish_volume_for_block(&client, &publish_path.to_string_lossy())
            .await
    }
}

#[cfg(test)]
mod tests;
