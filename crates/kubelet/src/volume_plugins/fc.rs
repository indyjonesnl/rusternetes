//! Port of `pkg/volume/fc/fc.go` (`fcPlugin`), registered by
//! `fc.ProbeVolumePlugins` (`cmd/kubelet/app/plugins.go`). The sibling files
//! are `fc_util.go` ([`util`]), `disk_manager.go` ([`disk_manager`]) and
//! `attacher.go` ([`attacher`]).
//!
//! Not ported (each noted at its call site):
//!
//! - `volume.NewMetricsStatFS` is not used by the filesystem mounter or
//!   unmounter upstream either (`fcDisk` embeds `volume.MetricsNil`,
//!   `fc.go:353`), and the block mapper's `NewMetricsBlock` is ported
//!   ([`super::util::metrics`]).
//! - `ConstructVolumeSpec`'s `SELinuxMountContext` (`fc.go:283-294`):
//!   [`ReconstructedVolume`] cannot carry one (#2312, #3493).
//! - `VerifyExhaustedResource`, `NewDetacher`/`Detach` and the attach side of
//!   `Attacher` (`Attach`, `VolumesAreAttached`): attach/detach-controller
//!   interfaces with no kubelet caller (#3498).
//! - `mountArgs.SELinuxLabel` / `AddSELinuxMountOption`
//!   (`attacher.go:125-127`): [`DeviceMounterArgs`] cannot carry the label
//!   (#2312, #3493). No kubelet caller invokes `UnmountDevice` yet (#3492).
//! - A real FibreChannel run (`/dev/disk/by-path`, `multipath`, sysfs) is
//!   unproven in this repository's CI (no FC HBA).

mod attacher;
mod disk_manager;
#[cfg(test)]
mod tests;
mod util;

pub use attacher::{FcAttacher, FcDetacher};
pub(crate) use disk_manager::DiskManager;
pub use util::FcUtil;
pub(crate) use util::{IoHandler, OsIoHandler};

use super::plugin::{
    Attributes, BlockVolumeMapper, BlockVolumePlugin, BlockVolumeUnmapper,
    CustomBlockVolumeUnmapper, Metrics, MetricsProvider, OwnedSpec,
};
use super::util::device_util::{new_os_device_handler, DeviceUtil};
use super::util::exec::{Exec, RealExec};
use super::util::metrics::{new_metrics_block, MetricsBlock, MetricsNil};
use super::util::mount_interface::{MountInterface, SafeFormatAndMount};
use super::util::mount_ops::cleanup_mount_point;
use super::util::mount_option_from_spec;
use super::{
    DeviceMounter, MounterArgs, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use rusternetes_common::resources::volume::{
    FCVolumeSource, PersistentVolumeAccessMode, PersistentVolumeMode,
};
use rusternetes_common::resources::Pod;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{error, warn};

/// `fcPluginName` (`fc.go:56`).
pub const FC_PLUGIN_NAME: &str = "kubernetes.io/fc";

/// `fcPlugin` (`fc.go:47-49`), plus its `Init` (`fc.go:59-62`).
///
/// Cheap to clone: `volumeSpecToMounter` builds a fresh
/// `&fcPlugin{host: host}` for every call (`attacher.go:233`), and every
/// [`FcDisk`] holds its plugin.
#[derive(Clone)]
pub struct FcPlugin {
    pub(crate) host: Arc<dyn VolumeHost>,
}

impl FcPlugin {
    /// `Init` (`fc.go:59-62`).
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }

    /// `GetAccessModes` (`fc.go:104-109`): `PersistentVolumePlugin`.
    pub fn get_access_modes(&self) -> Vec<PersistentVolumeAccessMode> {
        vec![
            PersistentVolumeAccessMode::ReadWriteOnce,
            PersistentVolumeAccessMode::ReadOnlyMany,
        ]
    }

    /// `SupportsMountOption` (`fc.go:96-98`): `true`.
    pub fn supports_mount_option(&self) -> bool {
        true
    }

    /// `newMounterInternal` (`fc.go:116-154`).
    pub(crate) fn new_mounter_internal(
        &self,
        spec: &Spec<'_>,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
        mounter: Arc<dyn MountInterface>,
        exec: Arc<dyn Exec>,
    ) -> Result<FcDiskMounter> {
        // fc volumes used directly in a pod have a ReadOnly flag set by the pod author.
        // fc volumes used as a PersistentVolume gets the ReadOnly flag indirectly through the persistent-claim volume used to mount the PV
        let (fc, read_only) = get_volume_source(spec)?;

        let (wwns, lun, wwids) = get_wwns_lun_wwids(&fc).map_err(|_| {
            anyhow!("fc: no fc disk information found. failed to make a new mounter")
        })?;
        let fc_disk = FcDisk {
            pod_uid: pod_uid.to_string(),
            vol_name: spec.name().to_string(),
            wwns,
            lun,
            wwids,
            manager,
            io: Arc::new(OsIoHandler),
            plugin: self.clone(),
        };

        let volume_mode = super::iscsi::get_volume_mode(spec)?;

        tracing::trace!("fc: newMounterInternal volumeMode {volume_mode:?}");
        Ok(FcDiskMounter {
            disk: fc_disk,
            fs_type: fc.fs_type.clone().unwrap_or_default(),
            volume_mode,
            read_only,
            mounter: SafeFormatAndMount {
                mounter,
                exec: exec.clone(),
                format_limit: Default::default(),
            },
            device_util: new_os_device_handler(),
            mount_options: mount_option_from_spec(spec, &[]),
            mounted_with_selinux_context: Arc::new(AtomicBool::new(false)),
        })
    }

    /// `newBlockVolumeMapperInternal` (`fc.go:166-199`).
    pub(crate) fn new_block_volume_mapper_internal(
        &self,
        spec: &Spec<'_>,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
    ) -> Result<FcDiskMapper> {
        let (fc, read_only) = get_volume_source(spec)?;

        let (wwns, lun, wwids) = get_wwns_lun_wwids(&fc).map_err(|_| {
            anyhow!("fc: no fc disk information found. failed to make a new mapper")
        })?;

        let mut mapper = FcDiskMapper {
            disk: FcDisk {
                pod_uid: pod_uid.to_string(),
                vol_name: spec.name().to_string(),
                wwns,
                lun,
                wwids,
                manager,
                io: Arc::new(OsIoHandler),
                plugin: self.clone(),
            },
            read_only,
            device_util: new_os_device_handler(),
            metrics: new_metrics_block(""),
        };

        let block_path = mapper
            .disk
            .get_global_map_path(spec)
            .map_err(|e| anyhow!("failed to get device path: {e}"))?;
        mapper.metrics = new_metrics_block(
            &std::path::Path::new(&block_path)
                .join(pod_uid)
                .to_string_lossy(),
        );

        Ok(mapper)
    }

    /// `newUnmounterInternal` (`fc.go:206-219`).
    pub(crate) fn new_unmounter_internal(
        &self,
        vol_name: &str,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
        mounter: Arc<dyn MountInterface>,
        exec: Arc<dyn Exec>,
    ) -> FcDiskUnmounter {
        FcDiskUnmounter {
            disk: FcDisk::blank(vol_name, pod_uid, manager, self.clone()),
            mounter,
            device_util: new_os_device_handler(),
            exec,
        }
    }

    /// `newUnmapperInternal` (`fc.go:225-237`).
    pub(crate) fn new_unmapper_internal(
        &self,
        vol_name: &str,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
        exec: Arc<dyn Exec>,
    ) -> FcDiskUnmapper {
        FcDiskUnmapper {
            disk: FcDisk::blank(vol_name, pod_uid, manager, self.clone()),
            exec,
            device_util: new_os_device_handler(),
        }
    }

    fn real_manager(&self) -> Arc<dyn DiskManager> {
        Arc::new(FcUtil)
    }

    /// `plugin.host.GetMounter()` as the `mount.Interface` handed to the
    /// mounters. The kubelet's mounter is cheap to clone.
    fn host_mounter(&self) -> Arc<dyn MountInterface> {
        Arc::new(self.host.get_mounter().clone())
    }
}

#[async_trait]
impl VolumePlugin for FcPlugin {
    /// `GetPluginName` (`fc.go:64-66`).
    fn name(&self) -> &'static str {
        FC_PLUGIN_NAME
    }

    /// `GetVolumeName` (`fc.go:68-86`). API server validates these parameters
    /// beforehand but attach/detach controller creates volumespec without
    /// validation. They may be nil or zero length. We should check again to
    /// avoid unexpected conditions.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        let (volume_source, _) = get_volume_source(spec)?;

        let target_wwns = volume_source.target_wwns.as_deref().unwrap_or_default();
        let wwids = volume_source.wwids.as_deref().unwrap_or_default();
        if let (false, Some(lun)) = (target_wwns.is_empty(), volume_source.lun) {
            // TargetWWNs are the FibreChannel target worldwide names; Go's
            // `%v` of a `[]string` is `[a b]`.
            return Ok(format!("[{}]:{lun}", target_wwns.join(" ")));
        } else if !wwids.is_empty() {
            // WWIDs are the FibreChannel World Wide Identifiers
            return Ok(format!("[{}]", wwids.join(" ")));
        }

        // `return "", err` with `err == nil` at this point.
        Ok(String::new())
    }

    /// `CanSupport` (`fc.go:88-90`).
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.legacy_sources.fc.is_some()
            || spec
                .persistent_volume
                .is_some_and(|pv| pv.spec.fc.is_some())
    }

    /// `RequiresRemount` (`fc.go:92-94`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`fc.go:100-102`): `true, nil`,
    /// unconditionally.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(true)
    }

    /// `CanAttach` (`attacher.go:208-210`): `true, nil`.
    fn can_attach(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(true)
    }

    /// `CanDeviceMount` (`attacher.go:212-214`): `true, nil`.
    fn can_device_mount(&self, _spec: &Spec<'_>) -> bool {
        true
    }

    fn as_device_mountable_plugin(
        &self,
    ) -> Option<&dyn super::plugin::DeviceMountableVolumePlugin> {
        Some(self)
    }

    fn as_attachable_plugin(&self) -> Option<&dyn super::plugin::AttachableVolumePlugin> {
        Some(self)
    }

    fn as_block_volume_plugin(&self) -> Option<&dyn BlockVolumePlugin> {
        Some(self)
    }

    /// `NewMounter` (`fc.go:111-114`).
    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn super::Mounter>> {
        // Inject real implementations here, test through the internal function.
        Ok(Box::new(self.new_mounter_internal(
            spec,
            &pod.metadata.uid,
            self.real_manager(),
            self.host_mounter(),
            Arc::new(RealExec),
        )?))
    }

    /// `NewUnmounter` (`fc.go:201-204`).
    fn new_unmounter(&self, vol_name: &str, pod_uid: &str) -> Result<Box<dyn Unmounter>> {
        // Inject real implementations here, test through the internal function.
        Ok(Box::new(self.new_unmounter_internal(
            vol_name,
            pod_uid,
            self.real_manager(),
            self.host_mounter(),
            Arc::new(RealExec),
        )))
    }

    /// `ConstructVolumeSpec` (`fc.go:239-302`).
    fn construct_volume_spec(
        &self,
        volume_name: &str,
        mount_path: &str,
    ) -> Result<ReconstructedVolume> {
        // Find globalPDPath from pod volume directory(mountPath)
        // examples:
        //   mountPath:     pods/{podUid}/volumes/kubernetes.io~fc/{volumeName}
        //   globalPDPath : plugins/kubernetes.io/fc/50060e801049cfd1-lun-0
        let mut global_pd_path = String::new();

        let mounter = self.host.get_mounter();
        // Try really hard to get the global mount of the volume, an error returned from here would
        // leave the global mount still mounted, while marking the volume as unused.
        // The volume can then be mounted on several nodes, resulting in volume
        // corruption.
        let paths = match super::util::mount_utils::get_reliable_mount_refs(mounter, mount_path) {
            Err(e) if super::util::mount_utils::is_inconsistent_read_error(&e) => {
                tracing::error!(
                    "Failed to read mount refs from /proc/mounts for {mount_path}: {e}"
                );
                tracing::error!(
                    "Kubelet cannot unmount volume at {mount_path}, please unmount it manually"
                );
                return Err(e.into());
            }
            r => r?,
        };
        let plugin_dir = self.host.get_plugin_dir(FC_PLUGIN_NAME);
        for path in &paths {
            if path.contains(&plugin_dir) {
                global_pd_path = path.clone();
                break;
            }
        }
        // Couldn't fetch globalPDPath
        if global_pd_path.is_empty() {
            bail!("couldn't fetch globalPDPath. failed to obtain volume spec");
        }

        let (wwns, lun, wwids) = util::parse_pd_name(&global_pd_path).map_err(|e| {
            anyhow!("failed to retrieve volume plugin information from globalPDPath: {e}")
        })?;
        // Create volume from wwn+lun or wwid
        let fc_source = fc_source_json(&wwns, lun, &wwids);
        tracing::trace!("ConstructVolumeSpec: TargetWWNs: {wwns:?}, Lun: {lun}, WWIDs: {wwids:?}");
        super::util::reconstructed_volume(volume_name, serde_json::json!({ "fc": fc_source }))
    }
}

impl super::plugin::DeviceMountableVolumePlugin for FcPlugin {
    /// `NewDeviceMounter` (`attacher.go:56-58`): `plugin.NewAttacher()`.
    fn new_device_mounter(&self) -> Result<Box<dyn DeviceMounter>> {
        Ok(Box::new(self.new_attacher_internal()))
    }

    /// `NewDeviceUnmounter` (`attacher.go:158-160`): `plugin.NewDetacher()`.
    fn new_device_unmounter(&self) -> Result<Box<dyn super::plugin::DeviceUnmounter>> {
        Ok(Box::new(self.new_detacher_internal()))
    }

    /// `GetDeviceMountRefs` (`attacher.go:60-63`).
    ///
    /// ```go
    /// mounter := plugin.host.GetMounter()
    /// return mounter.GetMountRefs(deviceMountPath)
    /// ```
    fn get_device_mount_refs(&self, device_mount_path: &str) -> Result<Vec<String>> {
        Ok(self.host.get_mounter().get_mount_refs(device_mount_path)?)
    }
}

impl BlockVolumePlugin for FcPlugin {
    /// `NewBlockVolumeMapper` (`fc.go:156-164`).
    fn new_block_volume_mapper(
        &self,
        spec: &Spec<'_>,
        pod: &Pod,
    ) -> Result<Box<dyn BlockVolumeMapper>> {
        // `pod` is never nil here; upstream passes an empty UID only when
        // called from `GenerateUnmapDeviceFunc` with a nil pod.
        Ok(Box::new(self.new_block_volume_mapper_internal(
            spec,
            &pod.metadata.uid,
            self.real_manager(),
        )?))
    }

    /// `NewBlockVolumeUnmapper` (`fc.go:221-223`).
    fn new_block_volume_unmapper(
        &self,
        name: &str,
        pod_uid: &str,
    ) -> Result<Box<dyn BlockVolumeUnmapper>> {
        Ok(Box::new(self.new_unmapper_internal(
            name,
            pod_uid,
            self.real_manager(),
            Arc::new(RealExec),
        )))
    }

    /// `ConstructBlockVolumeSpec` (`fc.go:304-340`): searches a file whose name
    /// is `{pod uuid}` under the volume plugin directory, retrieves
    /// `volumePluginDependentPath` from `globalMapPathUUID`, and stores the
    /// volume information to the `VolumeSource`.
    ///
    /// ```text
    /// mapPath: pods/{podUid}/{DefaultKubeletVolumeDevicesDirName}/{escapeQualifiedPluginName}/{volumeName}
    /// globalMapPathUUID : plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}/{pod uuid}
    /// ```
    fn construct_block_volume_spec(
        &self,
        pod_uid: &str,
        volume_name: &str,
        map_path: &str,
    ) -> Result<OwnedSpec> {
        let plugin_dir = self.host.get_volume_device_plugin_dir(FC_PLUGIN_NAME);
        let global_map_path_uuid = super::util::volumepathhandler::VolumePathHandler
            .find_global_map_path_uuid_from_pod(&plugin_dir, map_path, pod_uid)?;
        tracing::trace!("globalMapPathUUID: {global_map_path_uuid:?}");

        // Retrieve globalPDPath from globalMapPathUUID
        // globalMapPathUUID examples:
        //   wwn+lun: plugins/kubernetes.io/fc/volumeDevices/50060e801049cfd1-lun-0/{pod uuid}
        //   wwid: plugins/kubernetes.io/fc/volumeDevices/3600508b400105e210000900000490000/{pod uuid}
        let global_pd_path = std::path::Path::new(&global_map_path_uuid)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| ".".to_string());
        // Create volume from wwn+lun or wwid
        let (wwns, lun, wwids) = util::parse_pd_name(&global_pd_path).map_err(|e| {
            anyhow!("failed to retrieve volume plugin information from globalPDPath: {e}")
        })?;
        tracing::trace!(
            "ConstructBlockVolumeSpec: TargetWWNs: {wwns:?}, Lun: {lun}, WWIDs: {wwids:?}"
        );
        create_persistent_volume_from_fc_volume_source(
            volume_name,
            FCVolumeSource {
                target_wwns: Some(wwns),
                lun: Some(lun),
                wwids: Some(wwids),
                ..Default::default()
            },
        )
    }
}

/// `v1.FCVolumeSource{WWIDs: wwids, Lun: &lun, TargetWWNs: wwns}` as the JSON
/// `VolumeSource.fc` value; Go's nil slices are omitted by `omitempty`.
fn fc_source_json(wwns: &[String], lun: i32, wwids: &[String]) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    if !wwns.is_empty() {
        m.insert("targetWWNs".into(), serde_json::json!(wwns));
    }
    m.insert("lun".into(), serde_json::json!(lun));
    if !wwids.is_empty() {
        m.insert("wwids".into(), serde_json::json!(wwids));
    }
    serde_json::Value::Object(m)
}

/// `fcDisk` (`fc.go:342-354`); `volume.MetricsNil` is its `MetricsProvider`.
#[derive(Clone)]
pub(crate) struct FcDisk {
    pub vol_name: String,
    pub pod_uid: String,
    pub wwns: Vec<String>,
    pub lun: String,
    pub wwids: Vec<String>,
    pub plugin: FcPlugin,
    /// Utility interface that provides API calls to the provider to attach/detach disks.
    pub manager: Arc<dyn DiskManager>,
    /// io handler interface
    pub io: Arc<dyn IoHandler>,
}

impl FcDisk {
    /// The `&fcDisk{podUID, volName, manager, plugin, io}` literal of
    /// `newUnmounterInternal` / `newUnmapperInternal`.
    pub(crate) fn blank(
        vol_name: &str,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
        plugin: FcPlugin,
    ) -> Self {
        Self {
            vol_name: vol_name.to_string(),
            pod_uid: pod_uid.to_string(),
            wwns: Vec::new(),
            lun: String::new(),
            wwids: Vec::new(),
            plugin,
            manager,
            io: Arc::new(OsIoHandler),
        }
    }

    /// `fcDisk.GetPath` (`fc.go:356-359`): safe to use PodVolumeDir now:
    /// volume teardown occurs before pod is cleaned up.
    pub fn get_path(&self) -> String {
        self.plugin
            .host
            .get_pod_volume_dir(&self.pod_uid, FC_PLUGIN_NAME, &self.vol_name)
    }

    /// `fcGlobalMapPath` (`fc.go:361-368`).
    pub fn fc_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        let mounter = volume_spec_to_mounter(spec, &self.plugin.host)
            .inspect_err(|e| warn!("failed to get fc mounter: {e}"))?;
        Ok(self.manager.make_global_vdpd_name(&mounter.disk))
    }

    /// `fcPodDeviceMapPath` (`fc.go:370-372`).
    pub fn fc_pod_device_map_path(&self) -> (String, String) {
        (
            self.plugin.host.get_pod_volume_device_dir(
                &self.pod_uid,
                &crate::pod_dirs::escape_qualified_name(FC_PLUGIN_NAME),
            ),
            self.vol_name.clone(),
        )
    }

    /// `GetGlobalMapPath` (`fc.go:469-473`): path
    /// `plugins/kubernetes.io/{PluginName}/volumeDevices/{WWID}/{podUid}`.
    pub fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        self.fc_global_map_path(spec)
    }

    /// `GetPodDeviceMapPath` (`fc.go:475-480`): path
    /// `pods/{podUid}/volumeDevices/kubernetes.io~fc`, volumeName `pv0001`.
    pub fn get_pod_device_map_path(&self) -> (String, String) {
        self.fc_pod_device_map_path()
    }
}

/// `fcDiskMounter` (`fc.go:374-383`).
#[derive(Clone)]
pub(crate) struct FcDiskMounter {
    pub disk: FcDisk,
    pub read_only: bool,
    #[allow(dead_code)] // read by upstream's `fsType` consumers we do not port
    pub fs_type: String,
    #[allow(dead_code)] // set by both constructors, read by no ported caller
    pub volume_mode: PersistentVolumeMode,
    pub mounter: SafeFormatAndMount,
    pub device_util: Arc<dyn DeviceUtil>,
    pub mount_options: Vec<String>,
    pub mounted_with_selinux_context: Arc<AtomicBool>,
}

#[async_trait]
impl super::Mounter for FcDiskMounter {
    fn get_path(&self) -> String {
        self.disk.get_path()
    }

    /// `GetAttributes` (`fc.go:387-393`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: self.read_only,
            managed: !self.read_only,
            selinux_relabel: !self.mounted_with_selinux_context.load(Ordering::SeqCst),
        }
    }

    /// `SetUpAt` (`fc.go:399-411`). `diskSetUp` blocks (mount(8), stat), so it
    /// runs off the async runtime.
    async fn set_up_at(&self, dir: &str, args: &MounterArgs) -> Result<()> {
        // diskSetUp checks mountpoints and prevent repeated calls
        let this = self.clone();
        let (dir_owned, args_owned) = (dir.to_string(), args.clone());
        let res = tokio::task::spawn_blocking(move || {
            disk_manager::disk_set_up(
                this.disk.manager.as_ref(),
                &this,
                &dir_owned,
                this.mounter.mounter.as_ref(),
                &args_owned,
            )
        })
        .await
        .map_err(|e| anyhow!("fc SetUpAt task failed: {e}"))?;
        if res.is_err() {
            error!("fc: failed to setup");
        }

        if rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
        ) {
            // The volume must have been mounted in MountDevice with -o context.
            self.mounted_with_selinux_context
                .store(!args.selinux_label.is_empty(), Ordering::SeqCst);
        }
        res
    }
}

/// `fcDiskUnmounter` (`fc.go:413-418`).
#[derive(Clone)]
pub(crate) struct FcDiskUnmounter {
    pub disk: FcDisk,
    pub mounter: Arc<dyn MountInterface>,
    pub device_util: Arc<dyn DeviceUtil>,
    pub exec: Arc<dyn Exec>,
}

#[async_trait]
impl Unmounter for FcDiskUnmounter {
    fn get_path(&self) -> String {
        self.disk.get_path()
    }

    /// `TearDownAt` (`fc.go:428-430`): unmounts the bind mount; the disk is
    /// detached only by `UnmountDevice` (`attacher.go:166-206`).
    async fn tear_down_at(&self, dir: &str) -> Result<()> {
        let mounter = self.mounter.clone();
        let dir = dir.to_string();
        tokio::task::spawn_blocking(move || cleanup_mount_point(&dir, mounter.as_ref(), false))
            .await
            .map_err(|e| anyhow!("fc TearDownAt task failed: {e}"))??;
        Ok(())
    }
}

/// `fcDiskMapper` (`fc.go:433-439`), the `BlockVolumeMapper`.
#[allow(dead_code)] // mirrors upstream's struct; fields are read by its (unported) `MapPodDevice`
pub(crate) struct FcDiskMapper {
    pub disk: FcDisk,
    pub read_only: bool,
    pub device_util: Arc<dyn DeviceUtil>,
    metrics: MetricsBlock,
}

impl MetricsProvider for FcDiskMapper {
    fn get_metrics(&self) -> Result<Metrics> {
        self.metrics.get_metrics()
    }
}

impl BlockVolumeMapper for FcDiskMapper {
    fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        self.disk.get_global_map_path(spec)
    }

    fn get_pod_device_map_path(&self) -> (String, String) {
        self.disk.get_pod_device_map_path()
    }

    /// `volume.MetricsProvider` is initialised in `newBlockVolumeMapperInternal`.
    fn supports_metrics(&self) -> bool {
        true
    }
}

/// `fcDiskUnmapper` (`fc.go:443-447`), the `BlockVolumeUnmapper`;
/// `volume.MetricsNil` is its `MetricsProvider`.
pub(crate) struct FcDiskUnmapper {
    pub disk: FcDisk,
    pub exec: Arc<dyn Exec>,
    pub device_util: Arc<dyn DeviceUtil>,
}

impl MetricsProvider for FcDiskUnmapper {
    fn get_metrics(&self) -> Result<Metrics> {
        MetricsNil.get_metrics()
    }
}

impl BlockVolumeUnmapper for FcDiskUnmapper {
    fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        self.disk.get_global_map_path(spec)
    }

    fn get_pod_device_map_path(&self) -> (String, String) {
        self.disk.get_pod_device_map_path()
    }

    /// `MetricsNil.SupportsMetrics` (`metrics_nil.go:27-29`).
    fn supports_metrics(&self) -> bool {
        false
    }

    /// `var _ volume.CustomBlockVolumeUnmapper = &fcDiskUnmapper{}`
    /// (`fc.go:450`).
    fn as_custom_block_volume_unmapper(&self) -> Option<&dyn CustomBlockVolumeUnmapper> {
        Some(self)
    }
}

impl CustomBlockVolumeUnmapper for FcDiskUnmapper {
    /// `TearDownDevice` (`fc.go:452-463`).
    fn tear_down_device(&self, map_path: &str, device_path: &str) -> Result<()> {
        self.disk
            .manager
            .detach_block_fc_disk(self, map_path, device_path)
            .map_err(|e| anyhow!("fc: failed to detach disk: {map_path}\nError: {e}"))?;
        tracing::debug!("fc: {map_path:?} is unmounted, deleting the directory");
        std::fs::remove_dir_all(map_path)
            .or_else(|e| {
                // `os.RemoveAll` of a missing path is nil.
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e)
                }
            })
            .map_err(|e| anyhow!("fc: failed to delete the directory: {map_path}\nError: {e}"))?;
        tracing::debug!("fc: successfully detached disk: {map_path}");
        Ok(())
    }

    /// `UnmapPodDevice` (`fc.go:465-467`).
    fn unmap_pod_device(&self) -> Result<()> {
        Ok(())
    }
}

/// `getVolumeSource` (`fc.go:482-493`): the FC source and its read-only flag.
/// FC volumes used directly in a pod have a ReadOnly flag set by the pod
/// author; used as a PersistentVolume they get the ReadOnly flag indirectly
/// through the persistent-claim volume used to mount the PV.
pub(crate) fn get_volume_source(spec: &Spec<'_>) -> Result<(FCVolumeSource, bool)> {
    if let Some(v) = spec.volume.legacy_sources.fc.as_ref() {
        // `Volume.fc` is carried opaquely by the API types.
        let fc: FCVolumeSource = serde_json::from_value(v.clone())
            .map_err(|e| anyhow!("Spec does not reference a FibreChannel volume type: {e}"))?;
        let read_only = fc.read_only.unwrap_or(false);
        return Ok((fc, read_only));
    } else if let Some(fc) = spec.persistent_volume.and_then(|pv| pv.spec.fc.as_ref()) {
        return Ok((fc.clone(), spec.read_only));
    }

    bail!("Spec does not reference a FibreChannel volume type")
}

/// `createPersistentVolumeFromFCVolumeSource` (`fc.go:495-508`), wrapped as
/// `volume.NewSpecFromPersistentVolume(fcPV, false)` (`fc.go:339`): our `Spec`
/// needs a `Volume`, named after the PV (see `Spec::name`).
fn create_persistent_volume_from_fc_volume_source(
    volume_name: &str,
    fc: FCVolumeSource,
) -> Result<OwnedSpec> {
    let pv: rusternetes_common::resources::PersistentVolume =
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": volume_name},
            "spec": {"fc": serde_json::to_value(&fc)?, "volumeMode": "Block"}
        }))?;
    let volume: rusternetes_common::resources::Volume =
        serde_json::from_value(serde_json::json!({"name": volume_name}))?;
    Ok(OwnedSpec {
        volume,
        persistent_volume: Some(pv),
        read_only: false,
    })
}

/// `getWwnsLunWwids` (`fc.go:510-524`): `(wwns, lun, wwids)`.
pub(crate) fn get_wwns_lun_wwids(
    fc: &FCVolumeSource,
) -> Result<(Vec<String>, String, Vec<String>)> {
    let target_wwns = fc.target_wwns.clone().unwrap_or_default();
    if let (Some(lun), false) = (fc.lun, target_wwns.is_empty()) {
        return Ok((target_wwns, lun.to_string(), Vec::new()));
    }
    let fc_wwids = fc.wwids.as_deref().unwrap_or_default();
    if !fc_wwids.is_empty() {
        let wwids = fc_wwids.iter().map(|w| w.replace(' ', "_")).collect();
        return Ok((target_wwns, String::new(), wwids));
    }
    bail!("fc: no fc disk information found")
}

/// `volumeSpecToMounter` (`attacher.go:216-257`).
pub(crate) fn volume_spec_to_mounter(
    spec: &Spec<'_>,
    host: &Arc<dyn VolumeHost>,
) -> Result<FcDiskMounter> {
    let (fc, read_only) = get_volume_source(spec)?;
    let mut lun = String::new();
    let mut wwids: Vec<String> = Vec::new();
    let target_wwns = fc.target_wwns.clone().unwrap_or_default();
    let fc_wwids = fc.wwids.as_deref().unwrap_or_default();
    if let (Some(l), false) = (fc.lun, target_wwns.is_empty()) {
        lun = l.to_string();
    } else if !fc_wwids.is_empty() {
        for wwid in fc_wwids {
            wwids.push(wwid.replace(' ', "_"));
        }
    } else {
        bail!("fc: no fc disk information found. failed to make a new mounter");
    }
    let fc_disk = FcDisk {
        vol_name: String::new(),
        pod_uid: String::new(),
        plugin: FcPlugin { host: host.clone() },
        wwns: target_wwns,
        lun,
        wwids,
        // Go's nil `manager`; nothing reads it on this path.
        manager: Arc::new(FcUtil),
        io: Arc::new(OsIoHandler),
    };

    let volume_mode = super::iscsi::get_volume_mode(spec)?;

    tracing::trace!("fc: volumeSpecToMounter volumeMode {volume_mode:?}");
    let exec: Arc<dyn Exec> = Arc::new(RealExec);
    Ok(FcDiskMounter {
        disk: fc_disk,
        fs_type: fc.fs_type.clone().unwrap_or_default(),
        volume_mode,
        read_only,
        mounter: SafeFormatAndMount {
            mounter: Arc::new(host.get_mounter().clone()),
            exec,
            format_limit: Default::default(),
        },
        device_util: new_os_device_handler(),
        mount_options: mount_option_from_spec(spec, &[]),
        mounted_with_selinux_context: Arc::new(AtomicBool::new(false)),
    })
}
