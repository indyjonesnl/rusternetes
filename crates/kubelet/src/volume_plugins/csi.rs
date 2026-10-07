use super::csi_client::{CsiDriverClient, CsiError};
use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{
    CSIDriver, PersistentVolume, PersistentVolumeAccessMode, Pod, Secret, VolumeAttachment,
};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, error, info};

/// Port of `volDataFileName` (`pkg/volume/csi/csi_plugin.go:58`): the per-volume
/// info file the mounter persists so teardown can find the driver and handle.
const VOL_DATA_FILE_NAME: &str = "vol_data.json";

/// Port of `volDataKey` (`csi_mounter.go:42-53`).
mod vol_data_key {
    pub const SPEC_VOL_ID: &str = "specVolID";
    pub const VOL_HANDLE: &str = "volumeHandle";
    pub const DRIVER_NAME: &str = "driverName";
    pub const NODE_NAME: &str = "nodeName";
    pub const ATTACHMENT_ID: &str = "attachmentID";
    pub const VOLUME_LIFECYCLE_MODE: &str = "volumeLifecycleMode";
}

/// `globalMountInGlobalPath` (`csi_attacher.go:46`).
const GLOBAL_MOUNT_IN_GLOBAL_PATH: &str = "globalmount";

/// Port of `storage.VolumeLifecycleMode` (`CSIDriverSpec.VolumeLifecycleModes`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecycleMode {
    Persistent,
    Ephemeral,
}

impl LifecycleMode {
    fn as_str(self) -> &'static str {
        match self {
            LifecycleMode::Persistent => "Persistent",
            LifecycleMode::Ephemeral => "Ephemeral",
        }
    }
}

/// Port of the shape in `pkg/volume/csi/csi_plugin.go`.
///
/// Both volume arms are served, as upstream's `CanSupport`
/// (`csi_plugin.go:447-455`) does: a PersistentVolume with a CSI source and an
/// inline `csi` volume. Mounting goes through the CSI Node service
/// (`NodeStageVolume` when the driver advertises `STAGE_UNSTAGE_VOLUME`, then
/// `NodePublishVolume`) against the driver registered in
/// [`super::csi_drivers_store`].
///
/// **Deliberate deviation (inline arm only):** an inline volume whose driver is
/// not registered keeps the pre-plugin behaviour — a placeholder directory —
/// rather than failing as upstream does ("no CSIDriver object",
/// `csi_mounter.go:550`). A PersistentVolume never falls back: an unregistered
/// driver is a transient failure, so a pod cannot start with an empty volume.
pub struct CsiPlugin {
    host: Arc<dyn VolumeHost>,
}

impl CsiPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }

    /// `Spec.Name` for the CSI plugin (`plugins.go:444-453`): the PV's name for
    /// a PV-resolved spec, the volume's name for an inline one.
    ///
    /// Upstream's `Spec.Volume` is nil for a PV-resolved spec, so the PV arm is
    /// what runs; our `Spec.volume` is always set (see `plugin.rs`), so the
    /// PV-first order here reproduces upstream's result for every reachable
    /// state. The CSI directory is keyed on this name, which is why it matters.
    fn spec_name(spec: &Spec<'_>) -> String {
        match spec.persistent_volume {
            Some(pv) => pv.metadata.name.clone(),
            None => spec.volume.name.clone(),
        }
    }
}

#[async_trait]
impl VolumePlugin for CsiPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::CSI
    }

    /// `GetVolumeName` (`csi_plugin.go:437-445`): `"<driver>^<volumeHandle>"`,
    /// where `^` is upstream's `volNameSep` (`csi_plugin.go:57`).
    ///
    /// Upstream resolves the source with `getPVSourceFromSpec`
    /// (`csi_util.go:165-174`), which rejects an inline `CSIVolumeSource`
    /// outright — an inline CSI volume is ephemeral and pod-scoped, so it never
    /// reaches the unique-name-from-spec branch.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        if spec.volume.csi.is_some() {
            return Err(anyhow!(
                "plugin.GetVolumeName failed to extract volume source from spec: unexpected api.CSIVolumeSource found in volume.Spec"
            ));
        }
        let Some(source) = spec.persistent_volume.and_then(|pv| pv.spec.csi.as_ref()) else {
            return Err(anyhow!(
                "plugin.GetVolumeName failed to extract volume source from spec: volume source not found in volume.Spec"
            ));
        };
        Ok(format!(
            "{}^{}",
            source.driver,
            source.volume_handle.as_deref().unwrap_or_default()
        ))
    }

    /// `RequiresRemount` (`csi_plugin.go:457-460`): upstream consults the
    /// CSIDriver lister and returns `false` when it is nil. We have no lister,
    /// which is exactly that nil case.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`csi_plugin.go:637-...`): upstream looks
    /// the driver up in the CSIDriver lister to read its `SELinuxMount` field.
    /// With no lister we cannot answer `true` for any driver, so this reports
    /// `false` — the same answer upstream gives for a driver that does not
    /// declare `seLinuxMount: true`, which is the default.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `CanSupport` (`csi_plugin.go:447-455`):
    ///
    /// ```go
    /// return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.CSI != nil) ||
    ///     (spec.Volume != nil && spec.Volume.CSI != nil)
    /// ```
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.persistent_volume
            .is_some_and(|pv| pv.spec.csi.is_some())
            || spec.volume.csi.is_some()
    }

    /// `NewMounter` (`csi_plugin.go:474-538`).
    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        // `getSourceFromSpec` (`csi_util.go:146-163`).
        if spec.volume.csi.is_some() && spec.persistent_volume.is_some() {
            return Err(anyhow!(
                "volume.Spec has both volume and persistent volume sources"
            ));
        }
        let spec_name = Self::spec_name(spec);
        let (source, driver_name, volume_id, read_only, mode) =
            if let Some(vol_src) = spec.volume.csi.as_ref() {
                (
                    Source::Inline(Box::new(vol_src.clone())),
                    vol_src.driver.clone(),
                    make_volume_handle(&pod.metadata.uid, &spec_name),
                    vol_src.read_only.unwrap_or(false),
                    LifecycleMode::Ephemeral,
                )
            } else if let Some(pv) = spec.persistent_volume.filter(|pv| pv.spec.csi.is_some()) {
                let pv_src = pv.spec.csi.as_ref().expect("filtered above");
                (
                    Source::Pv(Box::new(pv.clone())),
                    pv_src.driver.clone(),
                    pv_src.volume_handle.clone().unwrap_or_default(),
                    // `Spec.ReadOnly` (`volume.NewSpecFromPersistentVolume(pv,
                    // readOnly)`): the claim volume's readOnly flag.
                    spec.volume
                        .persistent_volume_claim
                        .as_ref()
                        .and_then(|c| c.read_only)
                        .unwrap_or(false),
                    LifecycleMode::Persistent,
                )
            } else {
                return Err(anyhow!("volume source not found in volume.Spec"));
            };

        // `getTargetPath` + `GetCSIMounterPath` (`csi_mounter.go:84-96`,
        // `csi_util.go:177-179`): <pod volume dir>/<escaped spec name>/mount.
        let volume_dir = self.host.get_pod_volume_dir(
            &pod.metadata.uid,
            self.name(),
            &crate::pod_dirs::escape_qualified_name(&spec_name),
        );
        let path = Path::new(&volume_dir).join("mount");

        Ok(Box::new(CsiMounter {
            path: path.to_string_lossy().to_string(),
            spec_name,
            driver_name,
            volume_id,
            read_only,
            mode,
            source,
            pod_name: pod.metadata.name.clone(),
            pod_namespace: pod
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            pod_uid: pod.metadata.uid.clone(),
            service_account_name: pod
                .spec
                .as_ref()
                .and_then(|s| s.service_account_name.clone())
                .unwrap_or_default(),
            node_name: pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.clone())
                .unwrap_or_default(),
            storage: self.host.get_kube_client().cloned(),
            plugin_dir: plugin_dir(self.host.get_volumes_base_path()),
            token_manager: self.host.get_service_account_token_func().clone(),
        }))
    }
}

impl CsiPlugin {
    /// `NewUnmounter` (`csi_plugin.go:540-567`): reload the volume info file the
    /// mounter persisted and rebuild enough state to unpublish.
    pub fn new_unmounter(&self, spec_name: &str, pod_uid: &str) -> Result<CsiUnmounter> {
        let volume_dir = self.host.get_pod_volume_dir(
            pod_uid,
            self.name(),
            &crate::pod_dirs::escape_qualified_name(spec_name),
        );
        let data_dir = PathBuf::from(volume_dir);
        let path = data_dir.join("mount");
        let data = load_volume_data(&data_dir).map_err(|e| {
            anyhow!(
                "kubernetes.io/csi: unmounter failed to load volume data file [{}]: {e}",
                path.display()
            )
        })?;
        Ok(CsiUnmounter {
            path,
            driver_name: data
                .get(vol_data_key::DRIVER_NAME)
                .cloned()
                .unwrap_or_default(),
            volume_id: data
                .get(vol_data_key::VOL_HANDLE)
                .cloned()
                .unwrap_or_default(),
        })
    }
}

/// `GetPluginDir(CSIPluginName)` (`pkg/kubelet/volume_host.go`):
/// `<root>/plugins/kubernetes.io/csi`. The plugin name's `/` stays a path
/// separator here, unlike the pod-volume directory where it is escaped to `~`.
fn plugin_dir(volumes_base_path: &str) -> PathBuf {
    Path::new(volumes_base_path)
        .join("plugins")
        .join(crate::pod_dirs::plugin::CSI)
}

enum Source {
    Inline(Box<rusternetes_common::resources::csi::CSIVolumeSource>),
    Pv(Box<PersistentVolume>),
}

/// Port of `csiMountMgr` (`csi_mounter.go:61-79`), the `volume.Mounter` side.
struct CsiMounter {
    path: String,
    spec_name: String,
    driver_name: String,
    volume_id: String,
    read_only: bool,
    mode: LifecycleMode,
    source: Source,
    pod_name: String,
    pod_namespace: String,
    pod_uid: String,
    service_account_name: String,
    node_name: String,
    storage: Option<Arc<StorageBackend>>,
    plugin_dir: PathBuf,
    /// `GetServiceAccountTokenFunc` (`plugins.go:405`): signs the fallback
    /// token when the backend has no api-server to ask (see
    /// [`CsiMounter::pod_service_account_token_attrs`]).
    token_manager: rusternetes_common::auth::TokenManager,
}

fn transient(msg: String) -> anyhow::Error {
    CsiError::Transient(msg).into()
}

impl CsiMounter {
    /// Read a CSIDriver. `Ok(None)` is upstream's `apierrors.IsNotFound` (the
    /// CSIDriver object is optional); other errors are real.
    async fn get_csi_driver(&self) -> Result<Option<CSIDriver>> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(None);
        };
        match storage
            .get::<CSIDriver>(&build_key("csidrivers", None, &self.driver_name))
            .await
        {
            Ok(d) => Ok(Some(d)),
            Err(rusternetes_common::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(anyhow!("failed to get CSIDriver {}: {e}", self.driver_name)),
        }
    }

    /// Port of `supportsVolumeLifecycleMode` (`csi_mounter.go:531-560`).
    ///
    /// An unset `volumeLifecycleModes` is read as `[Persistent]`, the value the
    /// API server defaults it to; reading storage directly we may see it unset.
    fn supports_volume_lifecycle_mode(&self, driver: Option<&CSIDriver>) -> Result<()> {
        use rusternetes_common::resources::csi::VolumeLifecycleMode as M;
        let mode = self.mode;
        match driver {
            None if mode == LifecycleMode::Persistent => Ok(()),
            None => Err(anyhow!(
                "volume mode {:?} not supported by driver {} (no CSIDriver object)",
                mode.as_str(),
                self.driver_name
            )),
            Some(d) => {
                let modes = d
                    .spec
                    .volume_lifecycle_modes
                    .clone()
                    .unwrap_or_else(|| vec![M::Persistent]);
                let want = match mode {
                    LifecycleMode::Persistent => M::Persistent,
                    LifecycleMode::Ephemeral => M::Ephemeral,
                };
                if modes.contains(&want) {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "volume mode {:?} not supported by driver {} (only supports {:?})",
                        mode.as_str(),
                        self.driver_name,
                        modes
                            .iter()
                            .map(|m| match m {
                                M::Persistent => "Persistent",
                                M::Ephemeral => "Ephemeral",
                                M::Unknown(v) => v.as_str(),
                            })
                            .collect::<Vec<_>>()
                    ))
                }
            }
        }
    }

    /// Port of `getCredentialsFromSecret` (`csi_util.go:45-57`).
    async fn get_credentials_from_secret(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<HashMap<String, String>> {
        let storage = self
            .storage
            .as_ref()
            .context("failed to get a kubernetes client")?;
        let secret: Secret = storage
            .get(&build_key("secrets", Some(namespace), name))
            .await
            .map_err(|e| {
                anyhow!(
                    "kubernetes.io/csi: failed to find the secret {name} in the namespace {namespace} with error: {e}"
                )
            })?;
        Ok(secret
            .data
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, String::from_utf8_lossy(&v).to_string()))
            .collect())
    }

    /// Port of `getPublishContext` + `skipAttach` (`csi_plugin.go:858-928`).
    async fn get_publish_context(
        &self,
        driver: Option<&CSIDriver>,
        handle: &str,
    ) -> Result<HashMap<String, String>> {
        // `skipAttach`: a missing CSIDriver does NOT skip attach.
        if driver.is_some_and(|d| d.spec.attach_required == Some(false)) {
            return Ok(HashMap::new());
        }
        let storage = self
            .storage
            .as_ref()
            .context("failed to get a kubernetes client")?;
        let attach_id = get_attachment_name(handle, &self.driver_name, &self.node_name);
        let attachment: VolumeAttachment = storage
            .get(&build_key("volumeattachments", None, &attach_id))
            .await
            .map_err(|e| anyhow!("VolumeAttachment {attach_id}: {e}"))?;
        Ok(attachment
            .status
            .and_then(|s| s.attachment_metadata)
            .unwrap_or_default())
    }

    /// Port of `makeDeviceMountPath` (`csi_attacher.go:598-622`):
    /// `<pluginDir>/<driver>/<sha256(volumeHandle)>/globalmount`.
    fn make_device_mount_path(&self, pv_handle: &str) -> Result<PathBuf> {
        if self.driver_name.is_empty() {
            return Err(anyhow!(
                "makeDeviceMountPath failed, csi source driver name is empty"
            ));
        }
        if pv_handle.is_empty() {
            return Err(anyhow!(
                "makeDeviceMountPath failed, CSIPersistentVolumeSource volume handle is empty"
            ));
        }
        Ok(self
            .plugin_dir
            .join(&self.driver_name)
            .join(format!("{:x}", Sha256::digest(pv_handle.as_bytes())))
            .join(GLOBAL_MOUNT_IN_GLOBAL_PATH))
    }

    /// Port of `csiAttacher.MountDevice` (`csi_attacher.go:264-411`):
    /// `NodeStageVolume` when the driver advertises `STAGE_UNSTAGE_VOLUME`.
    ///
    /// **Deviation:** upstream runs this from the volume manager's reconciler
    /// (the `MountVolume` operation calls `MountDevice` for a device-mountable
    /// plugin, then `SetUp`). The reconciler here does not drive device mounts
    /// yet (`can_device_mount` is false for every plugin), so the mounter calls
    /// it directly, immediately before `NodePublishVolume`, in the same order.
    /// SELinux mount options and `VOLUME_MOUNT_GROUP` are not ported (no
    /// `DeviceMounterArgs`).
    async fn mount_device(
        &self,
        client: &CsiDriverClient,
        pv: &PersistentVolume,
        publish_context: HashMap<String, String>,
        device_mount_path: &Path,
    ) -> Result<()> {
        let pv_src = pv.spec.csi.as_ref().expect("checked by new_mounter");
        let stage_unstage_set = client.node_supports_stage_unstage().await?;

        let mut stage_secrets = HashMap::new();
        if let (Some(r), true) = (pv_src.node_stage_secret_ref.as_ref(), stage_unstage_set) {
            let (ns, name) = (
                r.namespace.as_deref().unwrap_or_default(),
                r.name.as_deref().unwrap_or_default(),
            );
            stage_secrets = self
                .get_credentials_from_secret(ns, name)
                .await
                .map_err(|e| {
                    transient(format!(
                        "fetching NodeStageSecretRef {ns}/{name} failed: {e}"
                    ))
                })?;
        }

        // Store volume metadata for UnmountDevice. Keep it around even if the
        // driver does not support NodeStage.
        make_dir_0750(device_mount_path).map_err(|e| {
            anyhow!(
                "kubernetes.io/csi: attacher.MountDevice failed to create dir {:?}:  {e}",
                device_mount_path
            )
        })?;
        let data_dir = device_mount_path
            .parent()
            .expect("device mount path has a parent");
        if !stage_unstage_set {
            info!("kubernetes.io/csi: attacher.MountDevice STAGE_UNSTAGE_VOLUME capability not set. Skipping MountDevice...");
            return Ok(());
        }

        let data = HashMap::from([
            (
                vol_data_key::VOL_HANDLE.to_string(),
                pv_src.volume_handle.clone().unwrap_or_default(),
            ),
            (vol_data_key::DRIVER_NAME.to_string(), pv_src.driver.clone()),
        ]);
        if let Err(e) = save_volume_data(data_dir, &data) {
            error!("kubernetes.io/csi: failed to save volume info data: {e}");
            if let Err(re) = remove_mount_dir(device_mount_path) {
                error!(
                    "kubernetes.io/csi: attacher.MountDevice failed to remove mount dir after error [{}]: {re}",
                    device_mount_path.display()
                );
            }
            return Err(e);
        }

        // TODO (vladimirvivien) implement better AccessModes mapping between
        // k8s and CSI — upstream's own TODO; first access mode wins.
        let access_mode = pv
            .spec
            .access_modes
            .first()
            .cloned()
            .unwrap_or(PersistentVolumeAccessMode::ReadWriteOnce);
        let staging = device_mount_path.to_string_lossy();
        let result = client
            .node_stage_volume(
                pv_src.volume_handle.as_deref().unwrap_or_default(),
                publish_context,
                &staging,
                pv_src.fs_type.as_deref().unwrap_or_default(),
                &access_mode,
                stage_secrets,
                pv_src.volume_attributes.clone().unwrap_or_default(),
                pv.spec.mount_options.as_deref().unwrap_or_default(),
                None,
            )
            .await;
        if let Err(e) = result {
            if e.is_operation_finished() {
                error!("kubernetes.io/csi: attacher.MountDevice failed: {e}");
                if let Err(re) = remove_mount_dir(device_mount_path) {
                    error!(
                        "kubernetes.io/csi: attacher.MountDevice failed to remove mount dir after error [{}]: {re}",
                        device_mount_path.display()
                    );
                }
            }
            return Err(e.into());
        }
        debug!(
            "kubernetes.io/csi: attacher.MountDevice successfully requested NodeStageVolume [{}]",
            device_mount_path.display()
        );
        Ok(())
    }
}

#[async_trait]
impl Mounter for CsiMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// Port of `csiMountMgr.SetUpAt` (`csi_mounter.go:102-356`).
    ///
    /// Not ported: `FSGroup` handling (`VOLUME_MOUNT_GROUP` delegation and the
    /// kubelet-side ownership change — `set_up` carries no `MounterArgs` yet),
    /// SELinux mount context, and the post-publish SELinux-support probe. See
    /// #2312.
    async fn set_up(&self) -> Result<()> {
        let dir = Path::new(&self.path);

        let client = match CsiDriverClient::new(&self.driver_name) {
            Ok(c) => c,
            // DEVIATION (inline arm only, see `CsiPlugin`): keep the
            // placeholder directory the pre-plugin `create_volume` produced.
            Err(e) if matches!(self.source, Source::Inline(_)) => {
                debug!("kubernetes.io/csi: {e}; creating placeholder for inline volume");
                std::fs::create_dir_all(dir).context("Failed to create CSI volume directory")?;
                info!(
                    "Created CSI ephemeral volume {} at {} (managed by CSI driver)",
                    self.spec_name, self.path
                );
                return Ok(());
            }
            // Treat the absence of the CSI driver as a transient error
            // (https://github.com/kubernetes/kubernetes/issues/120268).
            Err(e) => {
                return Err(transient(format!(
                    "kubernetes.io/csi: mounter.SetUpAt failed to get CSI client: {e}"
                )))
            }
        };

        let csi_driver = self.get_csi_driver().await.map_err(|e| {
            transient(format!(
                "kubernetes.io/csi: mounter.SetupAt failed to check volume lifecycle mode: {e}"
            ))
        })?;
        self.supports_volume_lifecycle_mode(csi_driver.as_ref())
            .map_err(|e| {
                transient(format!(
                    "kubernetes.io/csi: mounter.SetupAt failed to check volume lifecycle mode: {e}"
                ))
            })?;

        let mut access_mode = PersistentVolumeAccessMode::ReadWriteOnce;
        let fs_type: String;
        let mut vol_attribs: HashMap<String, String>;
        let mut mount_options: Vec<String> = Vec::new();
        let mut device_mount_path = String::new();
        let mut publish_context = HashMap::new();
        let secret_ref: Option<(String, String)>;

        match &self.source {
            Source::Inline(v) => {
                fs_type = v.fs_type.clone().unwrap_or_default();
                vol_attribs = v.volume_attributes.clone().unwrap_or_default();
                // Inline secret refs name a Secret in the pod's namespace.
                secret_ref = v.node_publish_secret_ref.as_ref().map(|r| {
                    (
                        self.pod_namespace.clone(),
                        r.name.clone().unwrap_or_default(),
                    )
                });
            }
            Source::Pv(pv) => {
                let pv_src = pv.spec.csi.as_ref().expect("checked by new_mounter");
                fs_type = pv_src.fs_type.clone().unwrap_or_default();
                vol_attribs = pv_src.volume_attributes.clone().unwrap_or_default();
                secret_ref = pv_src.node_publish_secret_ref.as_ref().map(|r| {
                    (
                        r.namespace.clone().unwrap_or_default(),
                        r.name.clone().unwrap_or_default(),
                    )
                });
                // TODO (vladimirvivien) implement better AccessModes mapping
                // between k8s and CSI — upstream's own TODO.
                if let Some(first) = pv.spec.access_modes.first() {
                    access_mode = first.clone();
                }
                mount_options = pv.spec.mount_options.clone().unwrap_or_default();

                // Check for STAGE_UNSTAGE_VOLUME and populate deviceMountPath.
                let stage_unstage_set = client.node_supports_stage_unstage().await.map_err(|e| {
                    anyhow!("kubernetes.io/csi: mounter.SetUpAt failed to check for STAGE_UNSTAGE_VOLUME capability: {e}")
                })?;
                let staging = if stage_unstage_set {
                    let p = self.make_device_mount_path(&self.volume_id).map_err(|e| {
                        anyhow!("kubernetes.io/csi: mounter.SetUpAt failed to make device mount path: {e}")
                    })?;
                    device_mount_path = p.to_string_lossy().to_string();
                    Some(p)
                } else {
                    None
                };

                // Search for the attachment (VolumeAttachment).
                publish_context = self
                    .get_publish_context(csi_driver.as_ref(), &self.volume_id)
                    .await
                    .map_err(|e| {
                        transient(format!(
                            "kubernetes.io/csi: mounter.SetUpAt failed to fetch publishContext: {e}"
                        ))
                    })?;

                // MountDevice, then SetUp, as the reconciler would sequence them.
                if let Some(staging) = staging {
                    self.mount_device(&client, pv, publish_context.clone(), &staging)
                        .await?;
                }
            }
        }

        // create target_dir before call to NodePublish
        let parent_dir = dir.parent().expect("mount path has a parent");
        make_dir_0750(parent_dir).map_err(|e| {
            anyhow!(
                "kubernetes.io/csi: mounter.SetUpAt failed to create dir {:?}:  {e}",
                parent_dir
            )
        })?;
        debug!(
            "kubernetes.io/csi: created target path successfully [{}]",
            parent_dir.display()
        );

        let mut node_publish_secrets = HashMap::new();
        if let Some((ns, name)) = &secret_ref {
            node_publish_secrets =
                self.get_credentials_from_secret(ns, name)
                    .await
                    .map_err(|e| {
                        transient(format!(
                            "fetching NodePublishSecretRef {ns}/{name} failed: {e}"
                        ))
                    })?;
        }

        // Inject pod information into volume_attributes
        // (`podInfoEnabled`, `getPodInfoAttrs`).
        if csi_driver
            .as_ref()
            .is_some_and(|d| d.spec.pod_info_on_mount == Some(true))
        {
            vol_attribs.extend(self.pod_info_attrs());
        }

        // Inject service account token information into
        // 1. volume_attributes (default behavior)
        // 2. node_publish_secrets (if ServiceAccountTokenInSecrets is true in
        //    the CSIDriver spec)
        // (`csi_mounter.go:236-250`).
        let (token_attrs, tokens_in_secrets) = self
            .pod_service_account_token_attrs(csi_driver.as_ref())
            .await
            .map_err(|e| {
                transient(format!(
                    "kubernetes.io/csi: mounter.SetUpAt failed to get service accoount token attributes: {e}"
                ))
            })?;
        if tokens_in_secrets {
            node_publish_secrets.extend(token_attrs);
        } else {
            vol_attribs.extend(token_attrs);
        }

        // Save volume info in pod dir; persisted for teardown.
        let volume_data = HashMap::from([
            (
                vol_data_key::SPEC_VOL_ID.to_string(),
                self.spec_name.clone(),
            ),
            (vol_data_key::VOL_HANDLE.to_string(), self.volume_id.clone()),
            (
                vol_data_key::DRIVER_NAME.to_string(),
                self.driver_name.clone(),
            ),
            (vol_data_key::NODE_NAME.to_string(), self.node_name.clone()),
            (
                vol_data_key::VOLUME_LIFECYCLE_MODE.to_string(),
                self.mode.as_str().to_string(),
            ),
            (
                vol_data_key::ATTACHMENT_ID.to_string(),
                get_attachment_name(&self.volume_id, &self.driver_name, &self.node_name),
            ),
        ]);
        if let Err(e) = save_volume_data(parent_dir, &volume_data) {
            error!("kubernetes.io/csi: mounter.SetUpAt failed to save volume info data: {e}");
            if let Err(re) = remove_mount_dir(dir) {
                error!(
                    "kubernetes.io/csi: mounter.SetUpAt failed to remove mount dir after error [{}]: {re}",
                    dir.display()
                );
            }
            return Err(e);
        }

        let result = client
            .node_publish_volume(
                &self.volume_id,
                self.read_only,
                &device_mount_path,
                &self.path,
                &access_mode,
                publish_context,
                vol_attribs,
                node_publish_secrets,
                &fs_type,
                &mount_options,
                None,
            )
            .await;
        if let Err(e) = result {
            // If the operation finished with an error we can remove the mount
            // directory.
            if e.is_operation_finished() {
                if let Err(re) = remove_mount_dir(dir) {
                    error!(
                        "kubernetes.io/csi: mounter.SetupAt failed to remove mount dir after a NodePublish() error [{}]: {re}",
                        dir.display()
                    );
                }
            }
            return Err(e.into());
        }

        debug!(
            "kubernetes.io/csi: mounter.SetUp successfully requested NodePublish [{}]",
            dir.display()
        );
        Ok(())
    }
}

impl CsiMounter {
    /// Port of `podServiceAccountTokenAttrs` (`csi_mounter.go:358-416`).
    ///
    /// Returns the `csi.storage.k8s.io/serviceAccount.tokens` attribute (a JSON
    /// map of requested audience -> `TokenRequestStatus`) and whether the
    /// driver wants it delivered in `node_publish_secrets`
    /// (`CSIDriver.spec.serviceAccountTokenInSecrets`). `driver` is the
    /// CSIDriver `set_up` already fetched; upstream re-reads it from the
    /// lister, `None` being its `IsNotFound` arm.
    ///
    /// DEVIATION: upstream always asks the api-server (`serviceAccountTokenGetter`
    /// = TokenRequest). Storage-direct backends have no api-server client, so
    /// there the token is self-minted with the host's `TokenManager`, exactly as
    /// the projected volume plugin does (`projected.rs`).
    async fn pod_service_account_token_attrs(
        &self,
        driver: Option<&CSIDriver>,
    ) -> Result<(HashMap<String, String>, bool)> {
        let Some(driver) = driver else {
            return Ok((HashMap::new(), false));
        };
        let requests = driver.spec.token_requests.as_deref().unwrap_or_default();
        if requests.is_empty() {
            return Ok((HashMap::new(), false));
        }

        // `map[string]authenticationv1.TokenRequestStatus`; json.Marshal sorts
        // map keys, as BTreeMap does.
        let mut outputs: std::collections::BTreeMap<
            String,
            rusternetes_common::resources::TokenRequestStatus,
        > = Default::default();
        for request in requests {
            let audience = request.audience.clone();
            // `audience == ""` requests no audience (the api-server defaults it).
            let audiences: Vec<String> = if audience.is_empty() {
                vec![]
            } else {
                vec![audience.clone()]
            };
            // `ExpirationSeconds` defaults to one hour
            // (`pkg/apis/authentication/v1/defaults.go:29-31`).
            let expiration_seconds = request.expiration_seconds.unwrap_or(3600);
            let status = self
                .get_service_account_token(&audiences, expiration_seconds)
                .await?;
            outputs.insert(audience, status);
        }
        debug!(
            "kubernetes.io/csi: fetched service account token attrs for CSIDriver {}",
            self.driver_name
        );
        let tokens = serde_json::to_string(&outputs)?;
        Ok((
            HashMap::from([(
                "csi.storage.k8s.io/serviceAccount.tokens".to_string(),
                tokens,
            )]),
            driver.spec.service_account_token_in_secrets == Some(true),
        ))
    }

    /// `serviceAccountTokenGetter(namespace, serviceAccountName, tokenRequest)`
    /// with `BoundObjectRef` = this pod (`csi_mounter.go:382-394`).
    async fn get_service_account_token(
        &self,
        audiences: &[String],
        expiration_seconds: i64,
    ) -> Result<rusternetes_common::resources::TokenRequestStatus> {
        if let Some(st) = self.storage.as_ref() {
            if let Some((token, expiration_timestamp)) = st
                .create_sa_token_status(
                    &self.pod_namespace,
                    &self.service_account_name,
                    audiences,
                    expiration_seconds,
                    Some((self.pod_name.as_str(), self.pod_uid.as_str())),
                )
                .await?
            {
                return Ok(rusternetes_common::resources::TokenRequestStatus {
                    expiration_timestamp,
                    token,
                });
            }
        }
        // Self-mint (storage-direct backends), claims as in `projected.rs`.
        let sa_uid = match self.storage.as_ref() {
            Some(st) => st
                .get::<rusternetes_common::resources::ServiceAccount>(&build_key(
                    "serviceaccounts",
                    Some(&self.pod_namespace),
                    &self.service_account_name,
                ))
                .await
                .map(|sa| sa.metadata.uid)
                .unwrap_or_default(),
            None => String::new(),
        };
        let mut claims = rusternetes_common::auth::ServiceAccountClaims::new(
            self.service_account_name.clone(),
            self.pod_namespace.clone(),
            sa_uid,
            0,
        );
        let exp = chrono::Utc::now() + chrono::Duration::seconds(expiration_seconds);
        claims.exp = exp.timestamp();
        claims.aud = if audiences.is_empty() {
            vec!["rusternetes".to_string()]
        } else {
            audiences.to_vec()
        };
        claims.pod_name = Some(self.pod_name.clone());
        claims.pod_uid = Some(self.pod_uid.clone());
        claims.node_name = Some(self.node_name.clone()).filter(|n| !n.is_empty());
        let token = self.token_manager.generate_token(claims)?;
        Ok(rusternetes_common::resources::TokenRequestStatus {
            expiration_timestamp: exp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            token,
        })
    }

    /// Port of `getPodInfoAttrs` (`csi_util.go:208-217`).
    fn pod_info_attrs(&self) -> HashMap<String, String> {
        HashMap::from([
            ("csi.storage.k8s.io/pod.name".into(), self.pod_name.clone()),
            (
                "csi.storage.k8s.io/pod.namespace".into(),
                self.pod_namespace.clone(),
            ),
            ("csi.storage.k8s.io/pod.uid".into(), self.pod_uid.clone()),
            (
                "csi.storage.k8s.io/serviceAccount.name".into(),
                self.service_account_name.clone(),
            ),
            (
                "csi.storage.k8s.io/ephemeral".into(),
                (self.mode == LifecycleMode::Ephemeral).to_string(),
            ),
        ])
    }
}

/// The `volume.Unmounter` half of `csiMountMgr` (`csi_mounter.go:429-466`).
pub struct CsiUnmounter {
    path: PathBuf,
    driver_name: String,
    volume_id: String,
}

impl CsiUnmounter {
    pub fn get_path(&self) -> String {
        self.path.to_string_lossy().to_string()
    }

    /// Port of `TearDownAt` (`csi_mounter.go:432-466`): `NodeUnpublishVolume`,
    /// then remove the (now unmounted) mount dir and volume info file.
    pub async fn tear_down(&self) -> Result<()> {
        let client = CsiDriverClient::new(&self.driver_name).map_err(|e| {
            // Treat the absence of the CSI driver as a transient error.
            transient(format!(
                "kubernetes.io/csi: Unmounter.TearDownAt failed to get CSI client: {e}"
            ))
        })?;
        client
            .node_unpublish_volume(&self.volume_id, &self.get_path())
            .await
            .map_err(|e| anyhow!("kubernetes.io/csi: Unmounter.TearDownAt failed: {e}"))?;

        // The driver MUST remove the target path (CSI spec), but some are
        // buggy, so remove it here if it is unmounted and empty.
        remove_mount_dir(&self.path).map_err(|e| {
            anyhow!(
                "kubernetes.io/csi: Unmounter.TearDownAt failed to clean mount dir [{}]: {e}",
                self.path.display()
            )
        })?;
        debug!(
            "kubernetes.io/csi: Unmounter.TearDownAt successfully unmounted dir [{}]",
            self.path.display()
        );
        Ok(())
    }
}

/// `getAttachmentName` (`csi_attacher.go:593-596`).
pub(crate) fn get_attachment_name(vol_name: &str, driver: &str, node: &str) -> String {
    format!(
        "csi-{:x}",
        Sha256::digest(format!("{vol_name}{driver}{node}"))
    )
}

/// `makeVolumeHandle` (`csi_mounter.go:612-615`): `csi-<sha256(podUID,volName)>`.
fn make_volume_handle(pod_uid: &str, vol_source_spec_name: &str) -> String {
    format!(
        "csi-{:x}",
        Sha256::digest(format!("{pod_uid}{vol_source_spec_name}"))
    )
}

/// `os.MkdirAll(dir, 0750)`.
fn make_dir_0750(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o750)
        .create(dir)
}

/// `saveVolumeData` (`csi_util.go:59-73`).
fn save_volume_data(dir: &Path, data: &HashMap<String, String>) -> Result<()> {
    let file = dir.join(VOL_DATA_FILE_NAME);
    let json = serde_json::to_string(data).map_err(|e| {
        anyhow!(
            "kubernetes.io/csi: failed to save volume data file {}: {e}",
            file.display()
        )
    })?;
    std::fs::write(&file, format!("{json}\n")).map_err(|e| {
        anyhow!(
            "kubernetes.io/csi: failed to save volume data file {}: {e}",
            file.display()
        )
    })
}

/// `loadVolumeData` (`csi_util.go:75-91`).
fn load_volume_data(dir: &Path) -> Result<HashMap<String, String>> {
    let file = dir.join(VOL_DATA_FILE_NAME);
    let text = std::fs::read_to_string(&file).map_err(|e| {
        anyhow!(
            "kubernetes.io/csi: failed to open volume data file [{}]: {e}",
            file.display()
        )
    })?;
    serde_json::from_str(&text).map_err(|e| {
        anyhow!(
            "kubernetes.io/csi: failed to parse volume data file [{}]: {e}",
            file.display()
        )
    })
}

/// Port of `removeMountDir` + `isDirMounted` (`csi_mounter.go:572-610`): when
/// `mount_path` is not a mount point, remove it, the volume info file and the
/// volume dir. A still-mounted path is left alone — never delete through a mount.
fn remove_mount_dir(mount_path: &Path) -> Result<()> {
    debug!(
        "kubernetes.io/csi: removing mount path [{}]",
        mount_path.display()
    );
    let mounted = match crate::pod_dirs::is_likely_not_mount_point(mount_path) {
        Ok(not_mnt) => !not_mnt,
        // A missing path is not mounted (`!os.IsNotExist(err)` in Go).
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(e.into()),
    };
    if mounted {
        return Ok(());
    }
    let ignore_missing = |r: std::io::Result<()>, what: &str, p: &Path| -> Result<()> {
        match r {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(anyhow!(
                "kubernetes.io/csi: failed to {what} [{}]: {e}",
                p.display()
            )),
            _ => Ok(()),
        }
    };
    ignore_missing(std::fs::remove_dir(mount_path), "remove dir", mount_path)?;
    let vol_path = mount_path.parent().expect("mount path has a parent");
    let data_file = vol_path.join(VOL_DATA_FILE_NAME);
    ignore_missing(
        std::fs::remove_file(&data_file),
        "delete volume data file",
        &data_file,
    )?;
    ignore_missing(
        std::fs::remove_dir(vol_path),
        "delete volume path",
        vol_path,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
