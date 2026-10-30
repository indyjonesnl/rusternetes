use crate::volume_plugins::metrics_statfs::MetricsStatFs;
use crate::volume_plugins::plugin::{
    BlockVolumeMapper, BlockVolumePlugin, BlockVolumeUnmapper, CustomBlockVolumeMapper,
    DeviceMountableVolumePlugin, DeviceUnmounter, Metrics, MetricsProvider, OwnedSpec,
};
use crate::volume_plugins::util::exec::{Exec, RealExec};
use crate::volume_plugins::util::hostutil::FileType;
use crate::volume_plugins::util::key_mutex::KeyMutex;
use crate::volume_plugins::util::metrics::{new_metrics_block, MetricsBlock, MetricsNil};
use crate::volume_plugins::util::mount_interface::{MountInterface, SafeFormatAndMount};
use crate::volume_plugins::util::mount_ops::cleanup_mount_point;
use crate::volume_plugins::util::mount_utils::{is_not_mount_point_with, path_within_base};
use crate::volume_plugins::util::{join_mount_options, mount_option_from_spec};
use crate::volume_plugins::{
    Attributes, DeviceMounter, DeviceMounterArgs, Mounter, MounterArgs, ReconstructedVolume, Spec,
    Unmounter, VolumeHost, VolumePlugin,
};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{EventType, Pod};
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// `localVolumePluginName` (`local.go:62`).
pub const LOCAL_VOLUME_PLUGIN_NAME: &str = crate::pod_dirs::plugin::LOCAL;

/// `defaultFSType` (`local.go:44`).
const DEFAULT_FS_TYPE: &str = "ext4";

/// `util.MountsInGlobalPDPath` (`pkg/volume/util/util.go:58`): the directory
/// appended to a volume plugin name for global PD mounts.
const MOUNTS_IN_GLOBAL_PD_PATH: &str = "mounts";

/// `events.WarnAlreadyMountedVolume` (`pkg/kubelet/events/event.go:71`).
pub const WARN_ALREADY_MOUNTED_VOLUME: &str = "AlreadyMountedVolume";

/// `hostutil.HostUtils.GetFileType` (`hostutil.go:55`), the seam upstream's
/// tests fake with `FakeHostUtil`'s `pathToFSType`.
type FileTypeFn = Arc<dyn Fn(&str) -> io::Result<FileType> + Send + Sync>;

/// Port of `pkg/volume/local/local.go` (the `localVolumePlugin`): the
/// filesystem mounter (a bind mount of the global path onto the pod volume
/// dir, with fsGroup ownership applied by the first pod only), the block
/// mapper/unmapper, and the device mounter that formats and mounts a local
/// block device at the global path.
///
/// Not ported: `NodeExpand` (`local.go:341-`; no non-CSI consumer of
/// `NodeExpandableVolumePlugin` is wired) and
/// `ownershipChanger.AddProgressNotifier` (`local.go:622`; the progress
/// monitor is not ported, see `volume_ownership`).
#[derive(Clone)]
pub struct LocalPlugin {
    host: Arc<dyn VolumeHost>,
    /// `plugin.volumeLocks` (`local.go:68`): `keymutex.NewHashed(0)`.
    volume_locks: Arc<KeyMutex>,
    /// `plugin.host.GetMounter()` (`local.go:151`, `:162`).
    mounter: Arc<dyn MountInterface>,
    /// `kvh.GetHostUtil().GetFileType`.
    file_type: FileTypeFn,
}

impl LocalPlugin {
    /// `Init` (`local.go:64-69`). The recorder upstream keeps on the plugin
    /// (`host.GetEventRecorder()`) arrives per call in
    /// [`MounterArgs::recorder`] here.
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        let mounter: Arc<dyn MountInterface> = Arc::new(host.get_mounter().clone());
        // `kvh.GetHostUtil().GetFileType` (`local.go:137`, `:280`).
        let file_type_host = host.clone();
        Self {
            host,
            volume_locks: Arc::new(KeyMutex::new_hashed(0)),
            mounter,
            file_type: Arc::new(move |p| file_type_host.get_host_util().get_file_type(p)),
        }
    }

    /// Replace the `mount.Interface` (upstream's tests inject
    /// `mount.NewFakeMounter` through the `FakeKubeletVolumeHost`).
    pub fn with_mounter(mut self, mounter: Arc<dyn MountInterface>) -> Self {
        self.mounter = mounter;
        self
    }

    /// Replace `GetFileType` (upstream's `pathToFSType` fake).
    pub fn with_file_type(mut self, f: FileTypeFn) -> Self {
        self.file_type = f;
        self
    }

    /// `generateBlockDeviceBaseGlobalPath` (`local.go:266-268`).
    fn generate_block_device_base_global_path(&self) -> String {
        Path::new(&self.host.get_plugin_dir(LOCAL_VOLUME_PLUGIN_NAME))
            .join(MOUNTS_IN_GLOBAL_PD_PATH)
            .to_string_lossy()
            .into_owned()
    }

    /// `getGlobalLocalPath` (`local.go:270-292`): the source directory of a
    /// directory PV, or the device's global mount path for a block device.
    pub fn get_global_local_path(&self, spec: &Spec<'_>) -> Result<String> {
        let path = match spec.persistent_volume.and_then(|pv| pv.spec.local.as_ref()) {
            Some(l) if !l.path.is_empty() => l.path.as_str(),
            _ => bail!("local volume source is nil or local path is not set"),
        };
        match (self.file_type)(path)? {
            FileType::Directory => Ok(path.to_string()),
            FileType::BlockDev => Ok(Path::new(&self.generate_block_device_base_global_path())
                .join(spec.name())
                .to_string_lossy()
                .into_owned()),
            _ => bail!("only directory and block device are supported"),
        }
    }

    /// `NewMounter` (`local.go:115-146`), returning the concrete mounter so
    /// its embedded `MetricsProvider` stays reachable. The `MetricsProvider`
    /// of `localVolume` (`local.go:500`) is `volume.NewMetricsStatFS` of the
    /// pod volume dir on the mounter (`local.go:140`) and nil on the
    /// unmounter (`local.go:148-157`).
    pub fn new_local_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<LocalMounter> {
        let (_, read_only) = get_volume_source(spec)?;
        let global_local_path = self.get_global_local_path(spec)?;
        let mut volume = self.new_local_volume(&pod.metadata.uid, spec.name(), &global_local_path);
        volume.pod = Some(pod.clone());
        // The pod volume dir `SetUpAt` bind-mounts `globalPath` onto, i.e.
        // `GetPath()` (`local.go:503-505`).
        let metrics = MetricsStatFs::new(&volume.get_path());
        Ok(LocalMounter {
            volume,
            mount_options: mount_option_from_spec(spec, &[]),
            read_only,
            metrics,
        })
    }

    pub fn new_local_volume(
        &self,
        pod_uid: &str,
        vol_name: &str,
        global_path: &str,
    ) -> LocalVolume {
        LocalVolume {
            pod: None,
            pod_uid: pod_uid.to_string(),
            vol_name: vol_name.to_string(),
            global_path: global_path.to_string(),
            plugin: self.clone(),
        }
    }

    /// `newDeviceMounterInternal` / `newDeviceUnmounterInternal`: the same
    /// `deviceMounter` struct serves both roles.
    pub fn new_device_mounter_internal(&self, exec: Arc<dyn Exec>) -> LocalDeviceMounter {
        LocalDeviceMounter {
            plugin: self.clone(),
            mounter: SafeFormatAndMount {
                mounter: self.mounter.clone(),
                exec,
                format_limit: Default::default(),
            },
        }
    }
}

/// `getVolumeSource` (`local.go:107-113`): the local source and the spec's
/// read-only flag. Only the PV arm exists.
fn get_volume_source<'a>(
    spec: &'a Spec<'_>,
) -> Result<(
    &'a rusternetes_common::resources::volume::LocalVolumeSource,
    bool,
)> {
    match spec.persistent_volume.and_then(|pv| pv.spec.local.as_ref()) {
        Some(l) => Ok((l, spec.read_only)),
        None => bail!("Spec does not reference a Local volume type"),
    }
}

#[async_trait]
impl VolumePlugin for LocalPlugin {
    fn name(&self) -> &'static str {
        LOCAL_VOLUME_PLUGIN_NAME
    }

    /// `GetVolumeName` (`local.go:82-85`): the PV name.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        spec.persistent_volume
            .and_then(|pv| pv.spec.local.as_ref().map(|_| pv.metadata.name.clone()))
            .ok_or_else(|| anyhow!("spec does not reference a Local volume type"))
    }

    /// `CanSupport` (`local.go:87-90`):
    /// `spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Local != nil`.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        get_volume_source(spec).is_ok()
    }

    /// `RequiresRemount` (`local.go:92-94`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`local.go:100-102`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `CanDeviceMount` (`local.go:304-306`): `true`.
    fn can_device_mount(&self, _spec: &Spec<'_>) -> bool {
        true
    }

    /// `var _ volume.DeviceMountableVolumePlugin = &localVolumePlugin{}`
    /// (`local.go:302`).
    fn as_device_mountable_plugin(&self) -> Option<&dyn DeviceMountableVolumePlugin> {
        Some(self)
    }

    /// `var _ volume.BlockVolumePlugin = &localVolumePlugin{}` (`local.go:60`).
    fn as_block_volume_plugin(&self) -> Option<&dyn BlockVolumePlugin> {
        Some(self)
    }

    /// `NewMounter` (`local.go:115-146`).
    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(self.new_local_mounter(spec, pod)?))
    }

    /// `NewUnmounter` (`local.go:148-157`).
    fn new_unmounter(&self, vol_name: &str, pod_uid: &str) -> Result<Box<dyn Unmounter>> {
        Ok(Box::new(LocalUnmounter {
            volume: self.new_local_volume(pod_uid, vol_name, ""),
        }))
    }

    /// `ConstructVolumeSpec` (`local.go:196-242`): a PV named after the volume
    /// with a Filesystem local source. "For filesystem volume with directory
    /// source, no global mount path is needed to clean. Empty path is ok. For
    /// filesystem volume with block source, we should resolve to its device
    /// path if global mount path exists."
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        mount_path: &str,
    ) -> Result<ReconstructedVolume> {
        let mut path = String::new();
        let refs = self.mounter.get_mount_refs(mount_path)?;
        let base_mount_path = self.generate_block_device_base_global_path();
        for r in &refs {
            if path_within_base(r, &base_mount_path) {
                // If the global mount for block device exists, the source is
                // block device. The resolved device path may not be the exact
                // same as path in local PV object if symbolic link is used.
                // However, it's the true source and can be used in
                // reconstructed volume.
                path = crate::volume_plugins::util::mount_list::get_device_name_from_mount(
                    &*self.mounter,
                    r,
                )?
                .0;
                debug!(
                    "local: reconstructing volume {vol_name:?} (pod volume mount: {mount_path:?}) with device {path:?}"
                );
                break;
            }
        }
        let pv = serde_json::from_value(serde_json::json!({
            "metadata": {"name": vol_name},
            "spec": {"local": {"path": path}, "volumeMode": "Filesystem"}
        }))?;
        let mut rv =
            crate::volume_plugins::util::reconstructed_volume(vol_name, serde_json::json!({}))?;
        rv.persistent_volume = Some(pv);
        Ok(rv)
    }
}

impl DeviceMountableVolumePlugin for LocalPlugin {
    /// `NewDeviceMounter` (`local.go:308-322`).
    fn new_device_mounter(&self) -> Result<Box<dyn DeviceMounter>> {
        Ok(Box::new(
            self.new_device_mounter_internal(Arc::new(RealExec)),
        ))
    }

    /// `NewDeviceUnmounter` (`local.go:456-465`).
    fn new_device_unmounter(&self) -> Result<Box<dyn DeviceUnmounter>> {
        Ok(Box::new(
            self.new_device_mounter_internal(Arc::new(RealExec)),
        ))
    }

    /// `GetDeviceMountRefs` (`local.go:467-470`).
    fn get_device_mount_refs(&self, device_mount_path: &str) -> Result<Vec<String>> {
        Ok(self.mounter.get_mount_refs(device_mount_path)?)
    }
}

impl BlockVolumePlugin for LocalPlugin {
    /// `NewBlockVolumeMapper` (`local.go:159-182`).
    fn new_block_volume_mapper(
        &self,
        spec: &Spec<'_>,
        pod: &Pod,
    ) -> Result<Box<dyn BlockVolumeMapper>> {
        let (source, read_only) = get_volume_source(spec)?;
        let volume = self.new_local_volume(&pod.metadata.uid, spec.name(), &source.path);
        let block_path = volume
            .get_global_map_path(spec)
            .map_err(|e| anyhow!("failed to get device path: {e}"))?;
        let metrics = new_metrics_block(
            &Path::new(&block_path)
                .join(&pod.metadata.uid)
                .to_string_lossy(),
        );
        Ok(Box::new(LocalMapper {
            volume,
            read_only,
            metrics,
        }))
    }

    /// `NewBlockVolumeUnmapper` (`local.go:184-193`).
    fn new_block_volume_unmapper(
        &self,
        name: &str,
        pod_uid: &str,
    ) -> Result<Box<dyn BlockVolumeUnmapper>> {
        Ok(Box::new(LocalUnmapper {
            volume: self.new_local_volume(pod_uid, name, ""),
        }))
    }

    /// `ConstructBlockVolumeSpec` (`local.go:244-264`): "Not needed because we
    /// don't need to detach local device from the host", so the path is empty.
    fn construct_block_volume_spec(
        &self,
        _pod_uid: &str,
        volume_name: &str,
        _map_path: &str,
    ) -> Result<OwnedSpec> {
        let pv = serde_json::from_value(serde_json::json!({
            "metadata": {"name": volume_name},
            "spec": {"local": {"path": ""}, "volumeMode": "Block"}
        }))?;
        let volume = serde_json::from_value(serde_json::json!({"name": volume_name}))?;
        Ok(OwnedSpec {
            volume,
            persistent_volume: Some(pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        })
    }
}

/// `localVolume` (`local.go:490-503`): "Local volumes represent a local
/// directory on a node. The directory at the globalPath will be bind-mounted
/// to the pod's directory".
#[derive(Clone)]
pub struct LocalVolume {
    vol_name: String,
    pod: Option<Pod>,
    pod_uid: String,
    /// Global path to the volume.
    global_path: String,
    plugin: LocalPlugin,
}

impl LocalVolume {
    /// `localVolume.GetPath` (`local.go:505-507`).
    fn get_path(&self) -> String {
        self.plugin
            .host
            .get_pod_volume_dir(&self.pod_uid, LOCAL_VOLUME_PLUGIN_NAME, &self.vol_name)
    }

    /// `GetGlobalMapPath` (`local.go:696-700`):
    /// `plugins/kubernetes.io/local-volume/volumeDevices/{volumeName}`.
    fn get_global_map_path(&self, _spec: &Spec<'_>) -> Result<String> {
        Ok(Path::new(&self.plugin.host.get_volume_device_plugin_dir(
            &crate::pod_dirs::escape_qualified_name(LOCAL_VOLUME_PLUGIN_NAME),
        ))
        .join(&self.vol_name)
        .to_string_lossy()
        .into_owned())
    }

    /// `GetPodDeviceMapPath` (`local.go:702-708`): path
    /// `pods/{podUid}/volumeDevices/kubernetes.io~local-volume`, volName
    /// `local-pv-ff0d6d4`.
    fn get_pod_device_map_path(&self) -> (String, String) {
        (
            self.plugin.host.get_pod_volume_device_dir(
                &self.pod_uid,
                &crate::pod_dirs::escape_qualified_name(LOCAL_VOLUME_PLUGIN_NAME),
            ),
            self.vol_name.clone(),
        )
    }
}

/// `localVolumeMounter` (`local.go:509-513`).
pub struct LocalMounter {
    pub volume: LocalVolume,
    pub read_only: bool,
    pub mount_options: Vec<String>,
    /// Embedded `volume.MetricsProvider` (`local.go:500`, set at `:140`).
    pub metrics: MetricsStatFs,
}

impl MetricsProvider for LocalMounter {
    fn get_metrics(&self) -> Result<Metrics> {
        self.metrics.get_metrics()
    }
}

/// What `SetUpAt` decided to tell the pod, handed back out of the blocking
/// section so the event is recorded on the async side.
struct SetUpOutcome {
    /// `recorder.Eventf(m.pod, Warning, WarnAlreadyMountedVolume, ...)`.
    warning: Option<String>,
}

impl LocalMounter {
    fn clone_mounter(&self) -> LocalMounter {
        LocalMounter {
            volume: self.volume.clone(),
            read_only: self.read_only,
            mount_options: self.mount_options.clone(),
            metrics: self.metrics.clone(),
        }
    }

    /// `filterPodMounts` (`local.go:631-639`): only mount paths inside the
    /// kubelet pod directory.
    pub fn filter_pod_mounts(&self, refs: Vec<String>) -> Vec<String> {
        let prefix = format!(
            "{}{}",
            self.volume.plugin.host.get_pods_dir(),
            std::path::MAIN_SEPARATOR
        );
        refs.into_iter()
            .filter(|r| r.starts_with(&prefix))
            .collect()
    }

    /// The synchronous body of `SetUpAt` (`local.go:529-626`), run on the
    /// blocking pool: `volumeLocks`, `mount(8)` and the ownership walk all
    /// block.
    fn set_up_at_blocking(
        &self,
        dir: &str,
        fs_group: Option<i64>,
        fs_group_change_policy: Option<&str>,
        outcome: &mut SetUpOutcome,
    ) -> Result<()> {
        let v = &self.volume;
        let mounter = &*v.plugin.mounter;
        let _guard = v.plugin.volume_locks.lock_key(&v.global_path);

        if v.global_path.is_empty() {
            bail!("LocalVolume volume {:?} path is empty", v.vol_name);
        }

        // `validation.ValidatePathNoBacksteps`.
        if v.global_path.split('/').any(|i| i == "..") {
            bail!("invalid path: {} must not contain '..'", v.global_path);
        }

        let not_mnt = match is_not_mount_point_with(mounter, dir) {
            Ok(n) => n,
            // `err != nil && !os.IsNotExist(err)`: a missing dir is simply not
            // mounted (`IsLikelyNotMountPoint` returns `true` with the error).
            Err(e) if e.kind() == io::ErrorKind::NotFound => true,
            Err(e) => {
                error!("cannot validate mount point: {dir} {e}");
                return Err(e.into());
            }
        };
        debug!(
            "LocalVolume mount setup: PodDir({dir}) VolDir({}) Mounted({}), ReadOnly({})",
            v.global_path, !not_mnt, self.read_only
        );
        if !not_mnt {
            return Ok(());
        }

        let mut refs = mounter.get_mount_refs(&v.global_path);
        if let Some(fs_group_new) = fs_group {
            let collected = match refs {
                Ok(r) => r,
                Err(e) => {
                    error!("cannot collect mounting information: {} {e}", v.global_path);
                    return Err(e.into());
                }
            };
            // Only count mounts from other pods
            let collected = self.filter_pod_mounts(collected);
            if !collected.is_empty() {
                let (_, fs_group_old) = crate::volume_plugins::util::hostutil::get_owner(
                    &v.global_path,
                )
                .map_err(|e| anyhow!("failed to check fsGroup for {} ({e})", v.global_path))?;
                if fs_group_new != fs_group_old {
                    outcome.warning = Some(format!(
                        "The requested fsGroup is {fs_group_new}, but the volume {} has GID {fs_group_old}. The volume may not be shareable.",
                        v.vol_name
                    ));
                }
            }
            refs = Ok(collected);
        }
        // Go's `refs` is a nil slice when `GetMountRefs` failed and no
        // fsGroup was requested, so `len(refs) == 0` below.
        let refs = refs.unwrap_or_default();

        if let Err(e) = std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o750)
            .create(dir)
        {
            error!("mkdir failed on disk {dir} ({e})");
            return Err(e.into());
        }
        // Perform a bind mount to the full path to allow duplicate mounts of
        // the same volume.
        let mut options = vec!["bind".to_string()];
        if self.read_only {
            options.push("ro".to_string());
        }
        let mount_options = join_mount_options(&self.mount_options, &options);

        debug!("attempting to mount {dir}");
        if let Err(err) =
            mounter.mount_sensitive_without_systemd(&v.global_path, dir, "", &mount_options, &[])
        {
            error!("Mount of volume {dir} failed: {err}");
            let err = anyhow::Error::from(err);
            let not_mnt = match is_not_mount_point_with(mounter, dir) {
                Ok(n) => n,
                Err(m) => {
                    error!("IsNotMountPoint check failed: {m}");
                    return Err(err);
                }
            };
            if !not_mnt {
                if let Err(m) = mounter.unmount(dir) {
                    error!("Failed to unmount: {m}");
                    return Err(err);
                }
                match is_not_mount_point_with(mounter, dir) {
                    Err(m) => {
                        error!("IsNotMountPoint check failed: {m}");
                        return Err(err);
                    }
                    Ok(false) => {
                        // This is very odd, we don't expect it. We'll try
                        // again next sync loop.
                        error!(
                            "{dir} is still mounted, despite call to unmount().  Will try again next sync loop."
                        );
                        return Err(err);
                    }
                    Ok(true) => {}
                }
            }
            if let Err(rm) = std::fs::remove_dir(dir) {
                warn!("failed to remove {dir}: {rm}");
            }
            return Err(err);
        }
        if !self.read_only {
            // Volume owner will be written only once on the first volume mount
            if refs.is_empty() {
                return crate::volume_ownership::set_volume_ownership_with_policy(
                    Path::new(dir),
                    fs_group,
                    fs_group_change_policy,
                    false,
                )
                .map_err(Into::into);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Mounter for LocalMounter {
    fn get_path(&self) -> String {
        self.volume.get_path()
    }

    /// `localVolumeMounter.GetAttributes` (`local.go:515-521`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: self.read_only,
            managed: !self.read_only,
            selinux_relabel: true,
        }
    }

    /// `SetUpAt` (`local.go:529-626`): bind mounts the directory to the volume
    /// path and sets up volume ownership.
    async fn set_up_at(&self, dir: &str, args: &MounterArgs) -> Result<()> {
        let this = self.clone_mounter();
        let (dir_owned, fs_group, policy) = (
            dir.to_string(),
            args.fs_group,
            args.fs_group_change_policy.clone(),
        );
        let (result, outcome) = tokio::task::spawn_blocking(move || {
            let mut outcome = SetUpOutcome { warning: None };
            let r = this.set_up_at_blocking(&dir_owned, fs_group, policy.as_deref(), &mut outcome);
            (r, outcome)
        })
        .await
        .map_err(|e| anyhow!("local volume SetUpAt task panicked: {e}"))?;

        if let (Some(message), Some(recorder), Some(pod)) = (
            outcome.warning,
            args.recorder.as_ref(),
            self.volume.pod.as_ref(),
        ) {
            crate::volume_plugins::util::node_expander::record_event(
                &recorder.0,
                &crate::events::pod_object_reference(pod),
                pod.spec.as_ref().and_then(|s| s.node_name.as_deref()),
                EventType::Warning,
                WARN_ALREADY_MOUNTED_VOLUME,
                &message,
            )
            .await;
        }
        result
    }
}

/// `localVolumeUnmounter` (`local.go:639-641`).
struct LocalUnmounter {
    volume: LocalVolume,
}

impl MetricsProvider for LocalUnmounter {
    /// `NewUnmounter` (`local.go:148-157`) leaves `localVolume`'s embedded
    /// `MetricsProvider` nil, so nothing can call it; `MetricsNil` is the safe
    /// equivalent.
    fn get_metrics(&self) -> Result<Metrics> {
        MetricsNil.get_metrics()
    }
}

#[async_trait]
impl Unmounter for LocalUnmounter {
    fn get_path(&self) -> String {
        self.volume.get_path()
    }

    /// `TearDownAt` (`local.go:653-657`): unmounts the bind mount,
    /// `mount.CleanupMountPoint(dir, u.mounter, true)`
    /// (`extensiveMountPointCheck = true`).
    async fn tear_down_at(&self, dir: &str) -> Result<()> {
        info!(
            "Unmounting volume {:?} at path {dir:?}",
            self.volume.vol_name
        );
        let mounter = self.volume.plugin.mounter.clone();
        let dir = dir.to_string();
        tokio::task::spawn_blocking(move || cleanup_mount_point(&dir, &*mounter, true))
            .await
            .map_err(|e| anyhow!("local volume TearDownAt task panicked: {e}"))?
            .map_err(Into::into)
    }
}

/// `localVolumeMapper` (`local.go:655-659`).
struct LocalMapper {
    volume: LocalVolume,
    #[allow(dead_code)] // mirrors upstream's field; read by no ported caller
    read_only: bool,
    metrics: MetricsBlock,
}

impl MetricsProvider for LocalMapper {
    fn get_metrics(&self) -> Result<Metrics> {
        self.metrics.get_metrics()
    }
}

impl BlockVolumeMapper for LocalMapper {
    fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        self.volume.get_global_map_path(spec)
    }

    fn get_pod_device_map_path(&self) -> (String, String) {
        self.volume.get_pod_device_map_path()
    }

    /// `SupportsMetrics` (`local.go:686-689`): "returns true for
    /// SupportsMetrics as it initializes the MetricsProvider".
    fn supports_metrics(&self) -> bool {
        true
    }

    /// `var _ volume.CustomBlockVolumeMapper = &localVolumeMapper{}`
    /// (`local.go:662`).
    fn as_custom_block_volume_mapper(&self) -> Option<&dyn CustomBlockVolumeMapper> {
        Some(self)
    }
}

#[async_trait]
impl CustomBlockVolumeMapper for LocalMapper {
    /// `SetUpDevice` (`local.go:668-670`).
    async fn set_up_device(&self) -> Result<String> {
        Ok(String::new())
    }

    /// `MapPodDevice` (`local.go:673-677`): the physical device path for the
    /// local PV (`util.MakeAbsolutePath` is the identity on Linux).
    async fn map_pod_device(&self) -> Result<String> {
        debug!("MapPodDevice returning path {}", self.volume.global_path);
        Ok(self.volume.global_path.clone())
    }

    /// `GetStagingPath` (`local.go:680-682`).
    fn get_staging_path(&self) -> String {
        String::new()
    }
}

/// `localVolumeUnmapper` (`local.go:693-696`); `volume.MetricsNil` is its
/// `MetricsProvider`.
struct LocalUnmapper {
    volume: LocalVolume,
}

impl MetricsProvider for LocalUnmapper {
    fn get_metrics(&self) -> Result<Metrics> {
        MetricsNil.get_metrics()
    }
}

impl BlockVolumeUnmapper for LocalUnmapper {
    fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        self.volume.get_global_map_path(spec)
    }

    fn get_pod_device_map_path(&self) -> (String, String) {
        self.volume.get_pod_device_map_path()
    }

    /// `MetricsNil.SupportsMetrics` (`metrics_nil.go:27-29`).
    fn supports_metrics(&self) -> bool {
        false
    }
}

/// `deviceMounter` (`local.go:307-311`): the `DeviceMounter` and
/// `DeviceUnmounter` of a local block device.
pub struct LocalDeviceMounter {
    plugin: LocalPlugin,
    mounter: SafeFormatAndMount,
}

impl LocalDeviceMounter {
    /// `mountLocalBlockDevice` (`local.go:324-363`).
    fn mount_local_block_device(
        &self,
        fs_type: &str,
        read_only: bool,
        mount_options: &[String],
        device_path: &str,
        device_mount_path: &str,
    ) -> Result<()> {
        debug!("local: mounting device {device_path} to {device_mount_path}");
        let not_mnt = match self
            .mounter
            .mounter
            .is_likely_not_mount_point(device_mount_path)
        {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o750)
                    .create(device_mount_path)?;
                true
            }
            Err(e) => return Err(e.into()),
        };
        if !not_mnt {
            return Ok(());
        }
        let mut options = Vec::new();
        if read_only {
            options.push("ro".to_string());
        }
        // `util.MountOptionFromSpec(spec, options...)`.
        let mount_options = join_mount_options(mount_options, &options);
        if let Err(e) =
            self.mounter
                .format_and_mount(device_path, device_mount_path, fs_type, &mount_options)
        {
            if let Err(rm) = std::fs::remove_dir(device_mount_path) {
                warn!("local: failed to remove {device_mount_path}: {rm}");
            }
            bail!(
                "local: failed to mount device {device_path} at {device_mount_path} (fstype: {fs_type}), error {e}"
            );
        }
        info!(
            "local: successfully mount device {device_path} at {device_mount_path} (fstype: {fs_type})"
        );
        Ok(())
    }
}

#[async_trait]
impl DeviceMounter for LocalDeviceMounter {
    /// `GetDeviceMountPath` (`local.go:452-454`).
    fn get_device_mount_path(&self, spec: &Spec<'_>) -> Result<String> {
        self.plugin.get_global_local_path(spec)
    }

    /// `MountDevice` (`local.go:365-384`): a block device is formatted and
    /// mounted at the device mount path; a directory is left alone.
    async fn mount_device(
        &self,
        spec: &Spec<'_>,
        _device_path: &str,
        device_mount_path: &str,
        _args: &DeviceMounterArgs,
    ) -> Result<()> {
        let local = match spec.persistent_volume.and_then(|pv| pv.spec.local.as_ref()) {
            Some(l) if !l.path.is_empty() => l,
            _ => bail!("local volume source is nil or local path is not set"),
        };
        match (self.plugin.file_type)(&local.path)? {
            // local volume plugin does not implement AttachableVolumePlugin
            // interface, so set devicePath to Path in PV spec directly
            FileType::BlockDev => {
                // `getVolumeSourceFSType`: default "ext4" when unset.
                let fs_type = local
                    .fs_type
                    .clone()
                    .unwrap_or_else(|| DEFAULT_FS_TYPE.to_string());
                let this = self
                    .plugin
                    .new_device_mounter_internal(self.mounter.exec.clone());
                let (device, target) = (local.path.clone(), device_mount_path.to_string());
                let (read_only, opts) = (spec.read_only, mount_option_from_spec(spec, &[]));
                tokio::task::spawn_blocking(move || {
                    this.mount_local_block_device(&fs_type, read_only, &opts, &device, &target)
                })
                .await
                .map_err(|e| anyhow!("local MountDevice task panicked: {e}"))?
            }
            // if the given local volume path is of already filesystem
            // directory, return directly
            FileType::Directory => Ok(()),
            _ => bail!("only directory and block device are supported"),
        }
    }
}

#[async_trait]
impl DeviceUnmounter for LocalDeviceMounter {
    /// `UnmountDevice` (`local.go:474-483`): "If the local PV is a block
    /// device, the deviceMountPath is generated to the format like
    /// `/var/lib/kubelet/plugins/kubernetes.io/local-volume/mounts/<pv>`; if
    /// it is a filesystem directory, then the deviceMountPath is set directly
    /// to pvSpec.Local.Path. We only need to unmount block device here."
    async fn unmount_device(&self, device_mount_path: &str) -> Result<()> {
        let base = self.plugin.generate_block_device_base_global_path();
        if path_within_base(device_mount_path, &base) {
            let (mounter, path) = (self.mounter.mounter.clone(), device_mount_path.to_string());
            return tokio::task::spawn_blocking(move || {
                cleanup_mount_point(&path, &*mounter, false)
            })
            .await
            .map_err(|e| anyhow!("local UnmountDevice task panicked: {e}"))?
            .map_err(Into::into);
        }
        Ok(())
    }
}
