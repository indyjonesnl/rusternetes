//! Port of `pkg/volume/iscsi/iscsi.go` (`iscsiPlugin`), registered by
//! `iscsi.ProbeVolumePlugins` (`cmd/kubelet/app/plugins.go`). The sibling
//! files are `iscsi_util.go` ([`util`]), `disk_manager.go`
//! ([`disk_manager`]) and `attacher.go` ([`attacher`]).
//!
//! Not ported (each noted at its call site):
//!
//! - `Mounter`/`Unmounter` do not embed `MetricsProvider` as a supertrait
//!   (#3483); the filesystem mounter/unmounter implement it directly with
//!   `volume.NewMetricsStatFS` (`iscsi.go:128`, `:194`), and the block
//!   mapper's `NewMetricsBlock` is ported ([`super::util::metrics`]).
//! - `ConstructVolumeSpec`'s `SELinuxMountContext` (`iscsi.go:286`, `:294`):
//!   [`ReconstructedVolume`] cannot carry one (#2312, #3493).
//! - `VerifyExhaustedResource`, `NewDetacher`/`Detach` and the attach side of
//!   `Attacher` (`Attach`, `VolumesAreAttached`): attach/detach-controller
//!   interfaces with no kubelet caller (#3498).
//! - No kubelet caller invokes `UnmountDevice` yet (#3492), and a real
//!   `iscsiadm` run is unproven (#3496).

mod attacher;
mod disk_manager;
#[cfg(test)]
mod tests;
mod util;

pub use attacher::IscsiAttacher;
pub(crate) use disk_manager::DiskManager;
pub use util::IscsiUtil;

use super::metrics_statfs::MetricsStatFs;
use super::plugin::{
    Attributes, BlockVolumeMapper, BlockVolumePlugin, BlockVolumeUnmapper,
    CustomBlockVolumeUnmapper, Metrics, MetricsProvider, OwnedSpec,
};
use super::util::device_util::{new_os_device_handler, DeviceUtil};
use super::util::exec::{Exec, RealExec};
use super::util::key_mutex::KeyMutex;
use super::util::metrics::{new_metrics_block, MetricsBlock, MetricsNil};
use super::util::mount_interface::{MountInterface, SafeFormatAndMount};
use super::util::mount_ops::cleanup_mount_point;
use super::util::{mount_option_from_spec, types::VolumeOperationError};
use super::{
    DeviceMounter, MounterArgs, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use rusternetes_common::resources::volume::{
    ISCSIVolumeSource, PersistentVolumeAccessMode, PersistentVolumeMode,
};
use rusternetes_common::resources::{Pod, Secret};
use rusternetes_storage::{build_key, Storage};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{error, warn};

/// `iscsiPluginName` (`iscsi.go:59`).
pub const ISCSI_PLUGIN_NAME: &str = "kubernetes.io/iscsi";

/// `iscsiPlugin` (`iscsi.go:49-52`), plus its `Init` (`iscsi.go:62-66`).
///
/// Cheap to clone: `volumeSpecToMounter` builds a fresh
/// `&iscsiPlugin{host: host, targetLocks: targetLocks}` for every call
/// (`attacher.go:202`, `:210`), and every [`IscsiDisk`] holds its plugin.
#[derive(Clone)]
pub struct IscsiPlugin {
    pub(crate) host: Arc<dyn VolumeHost>,
    pub(crate) target_locks: Arc<KeyMutex>,
}

impl IscsiPlugin {
    /// `Init` (`iscsi.go:62-66`): `keymutex.NewHashed(0)`.
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self {
            host,
            target_locks: Arc::new(KeyMutex::new_hashed(0)),
        }
    }

    /// `GetAccessModes` (`iscsi.go:97-102`): `PersistentVolumePlugin`.
    pub fn get_access_modes(&self) -> Vec<PersistentVolumeAccessMode> {
        vec![
            PersistentVolumeAccessMode::ReadWriteOnce,
            PersistentVolumeAccessMode::ReadOnlyMany,
        ]
    }

    /// `SupportsMountOption` (`iscsi.go:89-91`): `true`.
    pub fn supports_mount_option(&self) -> bool {
        true
    }

    /// `newMounterInternal` (`iscsi.go:115-139`).
    pub(crate) fn new_mounter_internal(
        &self,
        spec: &Spec<'_>,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
        mounter: Arc<dyn MountInterface>,
        exec: Arc<dyn Exec>,
        secret: HashMap<String, String>,
    ) -> Result<IscsiDiskMounter> {
        let (read_only, fs_type) = get_iscsi_volume_info(spec)?;
        let mut iscsi_disk = create_iscsi_disk(spec, pod_uid, self, manager, secret)?;
        // Add volume metrics (`iscsi.go:126-129`).
        iscsi_disk.metrics = MetricsStatFs::new(&iscsi_disk.get_path());
        Ok(IscsiDiskMounter {
            disk: iscsi_disk,
            fs_type,
            read_only,
            volume_mode: None,
            mounter: SafeFormatAndMount {
                mounter,
                exec: exec.clone(),
                format_limit: Default::default(),
            },
            exec,
            device_util: new_os_device_handler(),
            mount_options: mount_option_from_spec(spec, &[]),
            mounted_with_selinux_context: Arc::new(AtomicBool::new(false)),
        })
    }

    /// `newUnmounterInternal` (`iscsi.go:187-200`).
    pub(crate) fn new_unmounter_internal(
        &self,
        vol_name: &str,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
        mounter: Arc<dyn MountInterface>,
        exec: Arc<dyn Exec>,
    ) -> IscsiDiskUnmounter {
        let mut disk = IscsiDisk::blank(vol_name, pod_uid, manager, self.clone());
        // `MetricsProvider: volume.NewMetricsStatFS(GetPodVolumeDir(..))`
        // (`iscsi.go:194`).
        disk.metrics = MetricsStatFs::new(&disk.get_path());
        IscsiDiskUnmounter {
            disk,
            mounter,
            exec,
            device_util: new_os_device_handler(),
        }
    }

    /// `newBlockVolumeMapperInternal` (`iscsi.go:158-181`).
    pub(crate) fn new_block_volume_mapper_internal(
        &self,
        spec: &Spec<'_>,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
        exec: Arc<dyn Exec>,
        secret: HashMap<String, String>,
    ) -> Result<IscsiDiskMapper> {
        let (read_only, _) = get_iscsi_volume_info(spec)?;
        let iscsi_disk = create_iscsi_disk(spec, pod_uid, self, manager, secret)?;
        let mut mapper = IscsiDiskMapper {
            disk: iscsi_disk,
            read_only,
            exec,
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

    /// `newUnmapperInternal` (`iscsi.go:207-218`).
    pub(crate) fn new_unmapper_internal(
        &self,
        vol_name: &str,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
        exec: Arc<dyn Exec>,
    ) -> IscsiDiskUnmapper {
        IscsiDiskUnmapper {
            disk: IscsiDisk::blank(vol_name, pod_uid, manager, self.clone()),
            exec,
            device_util: new_os_device_handler(),
        }
    }

    fn real_manager(&self) -> Arc<dyn DiskManager> {
        Arc::new(IscsiUtil)
    }

    /// `plugin.host.GetMounter()` as the `mount.Interface` handed to the
    /// mounters. The kubelet's mounter is cheap to clone.
    fn host_mounter(&self) -> Arc<dyn MountInterface> {
        Arc::new(self.host.get_mounter().clone())
    }
}

#[async_trait]
impl VolumePlugin for IscsiPlugin {
    /// `GetPluginName` (`iscsi.go:68-70`).
    fn name(&self) -> &'static str {
        ISCSI_PLUGIN_NAME
    }

    /// `GetVolumeName` (`iscsi.go:72-79`): `"%v:%v:%v"` of the *unmodified*
    /// target portal, iqn and lun.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        let (tp, _, iqn, lun) = get_iscsi_target_info(spec)?;
        Ok(format!("{tp}:{iqn}:{lun}"))
    }

    /// `CanSupport` (`iscsi.go:81-83`).
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.iscsi.is_some()
            || spec
                .persistent_volume
                .is_some_and(|pv| pv.spec.iscsi.is_some())
    }

    /// `RequiresRemount` (`iscsi.go:85-87`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`iscsi.go:93-95`): `true, nil`,
    /// unconditionally.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(true)
    }

    /// `CanAttach` (`attacher.go:186-188`): `true, nil`.
    fn can_attach(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(true)
    }

    /// `CanDeviceMount` (`attacher.go:190-192`): `true, nil`.
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

    /// `NewMounter` (`iscsi.go:104-113`).
    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn super::Mounter>> {
        // `if pod == nil { return nil, fmt.Errorf("nil pod") }` is a type
        // guarantee here: `pod` is a reference.
        let secret = create_secret_map(
            spec,
            self,
            pod.metadata.namespace.as_deref().unwrap_or_default(),
        )
        .await?;
        Ok(Box::new(self.new_mounter_internal(
            spec,
            &pod.metadata.uid,
            self.real_manager(),
            self.host_mounter(),
            Arc::new(RealExec),
            secret,
        )?))
    }

    /// `NewUnmounter` (`iscsi.go:183-185`).
    fn new_unmounter(&self, vol_name: &str, pod_uid: &str) -> Result<Box<dyn Unmounter>> {
        Ok(Box::new(self.new_unmounter_internal(
            vol_name,
            pod_uid,
            self.real_manager(),
            self.host_mounter(),
            Arc::new(RealExec),
        )))
    }

    /// `ConstructVolumeSpec` (`iscsi.go:220-296`).
    fn construct_volume_spec(
        &self,
        volume_name: &str,
        mount_path: &str,
    ) -> Result<ReconstructedVolume> {
        // Find globalPDPath from pod volume directory(mountPath)
        let mut global_pd_path = String::new();
        let mounter = self.host.get_mounter();
        // Try really hard to get the global mount of the volume, an error returned from here would
        // leave the global mount still mounted, while marking the volume as unused.
        // The volume can then be mounted on several nodes, resulting in volume
        // corruption.
        let paths = get_reliable_mount_refs(mounter, mount_path)?;

        for path in &paths {
            if path.contains(&self.host.get_plugin_dir(ISCSI_PLUGIN_NAME)) {
                global_pd_path = path.clone();
                break;
            }
        }
        // Couldn't fetch globalPDPath
        if global_pd_path.is_empty() {
            bail!("couldn't fetch globalPDPath. failed to obtain volume spec");
        }

        // Obtain iscsi disk configurations from globalPDPath
        let (device, _) = util::extract_device_and_prefix(&global_pd_path)?;
        let (bkp_portal, iqn) = util::extract_portal_and_iqn(&device)?;
        let arr: Vec<&str> = device.split("-lun-").collect();
        if arr.len() < 2 {
            bail!("failed to retrieve lun from globalPDPath: {global_pd_path}");
        }
        let lun: i32 = arr[1].parse().map_err(|e| anyhow!("{e}"))?;
        let (iface, _) = util::extract_iface(&global_pd_path);
        super::util::reconstructed_volume(
            volume_name,
            serde_json::json!({"iscsi": {
                "targetPortal": bkp_portal,
                "iqn": iqn,
                "lun": lun,
                "iscsiInterface": iface,
            }}),
        )
    }
}

impl super::plugin::DeviceMountableVolumePlugin for IscsiPlugin {
    /// `NewDeviceMounter` (`attacher.go:61-63`): `plugin.NewAttacher()`.
    fn new_device_mounter(&self) -> Result<Box<dyn DeviceMounter>> {
        Ok(Box::new(self.new_attacher_internal()))
    }

    /// `NewDeviceUnmounter` (`attacher.go:163-165`): `plugin.NewDetacher()`.
    fn new_device_unmounter(&self) -> Result<Box<dyn super::plugin::DeviceUnmounter>> {
        Ok(Box::new(self.new_attacher_internal()))
    }

    /// `GetDeviceMountRefs` (`attacher.go:65-68`).
    ///
    /// ```go
    /// mounter := plugin.host.GetMounter()
    /// return mounter.GetMountRefs(deviceMountPath)
    /// ```
    fn get_device_mount_refs(&self, device_mount_path: &str) -> Result<Vec<String>> {
        Ok(self.host.get_mounter().get_mount_refs(device_mount_path)?)
    }
}

impl BlockVolumePlugin for IscsiPlugin {
    /// `NewBlockVolumeMapper` (`iscsi.go:142-156`).
    fn new_block_volume_mapper(
        &self,
        spec: &Spec<'_>,
        pod: &Pod,
    ) -> Result<Box<dyn BlockVolumeMapper>> {
        // `createSecretMap` reads the Secret through the API client, which is
        // asynchronous here; the sync trait method drives it on the current
        // runtime.
        let ns = pod.metadata.namespace.clone().unwrap_or_default();
        let secret = block_on_secret_map(spec, self, &ns)?;
        Ok(Box::new(self.new_block_volume_mapper_internal(
            spec,
            &pod.metadata.uid,
            self.real_manager(),
            Arc::new(RealExec),
            secret,
        )?))
    }

    /// `NewBlockVolumeUnmapper` (`iscsi.go:203-205`).
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

    /// `ConstructBlockVolumeSpec` (`iscsi.go:298-312`).
    fn construct_block_volume_spec(
        &self,
        pod_uid: &str,
        volume_name: &str,
        map_path: &str,
    ) -> Result<OwnedSpec> {
        let plugin_dir = self.host.get_volume_device_plugin_dir(ISCSI_PLUGIN_NAME);
        let global_map_path_uuid = super::util::volumepathhandler::VolumePathHandler
            .find_global_map_path_uuid_from_pod(&plugin_dir, map_path, pod_uid)?;
        tracing::trace!("globalMapPathUUID: {global_map_path_uuid:?}");
        // Retrieve volume information from globalMapPathUUID
        // globalMapPathUUID example:
        // plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}/{pod uuid}
        // plugins/kubernetes.io/iscsi/volumeDevices/iface-default/192.168.0.10:3260-iqn.2017-05.com.example:test-lun-0/{pod uuid}
        let global_map_path = std::path::Path::new(&global_map_path_uuid)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| ".".to_string());
        get_volume_spec_from_global_map_path(volume_name, &global_map_path)
    }
}

/// `iscsiDisk` (`iscsi.go:314-330`). The exported fields are the ones
/// `persistISCSIFile` writes to `iscsi.json`.
#[derive(Clone)]
pub(crate) struct IscsiDisk {
    pub vol_name: String,
    pub pod_uid: String,
    pub portals: Vec<String>,
    pub iqn: String,
    pub lun: String,
    pub init_iface: String,
    pub iface: String,
    pub chap_discovery: bool,
    pub chap_session: bool,
    pub secret: HashMap<String, String>,
    pub initiator_name: String,
    pub plugin: IscsiPlugin,
    /// Utility interface that provides API calls to the provider to attach/detach disks.
    pub manager: Arc<dyn DiskManager>,
    /// Embedded `volume.MetricsProvider` (`iscsi.go:315`). Set to
    /// `volume.NewMetricsStatFS(GetPath())` by `newMounterInternal`
    /// (`:128`) and `newUnmounterInternal` (`:194`); the other builders leave
    /// Go's nil, here a path-less [`MetricsStatFs`] that reports
    /// `NoPathDefined` instead of a nil dereference.
    pub metrics: MetricsStatFs,
}

impl IscsiDisk {
    /// The `&iscsiDisk{podUID, VolName, manager, plugin}` literal of
    /// `newUnmounterInternal` / `newUnmapperInternal`.
    pub(crate) fn blank(
        vol_name: &str,
        pod_uid: &str,
        manager: Arc<dyn DiskManager>,
        plugin: IscsiPlugin,
    ) -> Self {
        Self {
            vol_name: vol_name.to_string(),
            pod_uid: pod_uid.to_string(),
            portals: Vec::new(),
            iqn: String::new(),
            lun: String::new(),
            init_iface: String::new(),
            iface: String::new(),
            chap_discovery: false,
            chap_session: false,
            secret: HashMap::new(),
            initiator_name: String::new(),
            plugin,
            manager,
            metrics: MetricsStatFs::new(""),
        }
    }

    /// `&iscsiDisk{plugin: plugin}` (`volumeSpecToUnmounter`, `attacher.go:238-240`).
    pub(crate) fn blank_for_plugin(plugin: IscsiPlugin) -> Self {
        Self::blank("", "", Arc::new(IscsiUtil), plugin)
    }

    /// `iscsiDisk.GetPath` (`iscsi.go:332-336`): safe to use PodVolumeDir now:
    /// volume teardown occurs before pod is cleaned up.
    pub fn get_path(&self) -> String {
        self.plugin
            .host
            .get_pod_volume_dir(&self.pod_uid, ISCSI_PLUGIN_NAME, &self.vol_name)
    }

    /// `iscsiGlobalMapPath` (`iscsi.go:338-345`).
    pub fn iscsi_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        let mounter =
            volume_spec_to_mounter_no_pod(spec, &self.plugin.host, &self.plugin.target_locks)
                .inspect_err(|e| warn!("failed to get iscsi mounter: {e}"))?;
        Ok(self.manager.make_global_vdpd_name(&mounter.disk))
    }

    /// `iscsiPodDeviceMapPath` (`iscsi.go:347-350`).
    pub fn iscsi_pod_device_map_path(&self) -> (String, String) {
        (
            self.plugin.host.get_pod_volume_device_dir(
                &self.pod_uid,
                &crate::pod_dirs::escape_qualified_name(ISCSI_PLUGIN_NAME),
            ),
            self.vol_name.clone(),
        )
    }

    /// `GetGlobalMapPath` (`iscsi.go:461-463`): path
    /// `plugins/kubernetes.io/{PluginName}/volumeDevices/{ifaceName}/{portal-some_iqn-lun-lun_id}`.
    pub fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        self.iscsi_global_map_path(spec)
    }
}

/// `iscsiDiskMounter` (`iscsi.go:352-362`).
#[derive(Clone)]
pub(crate) struct IscsiDiskMounter {
    pub disk: IscsiDisk,
    pub read_only: bool,
    #[allow(dead_code)]
    // `fsType` is only read by upstream's `iscsiDiskMounter` consumers we do not port
    pub fs_type: String,
    /// `volumeMode`: only `volumeSpecToMounter` sets it (`attacher.go:194-234`);
    /// `newMounterInternal` leaves Go's zero value, here `None`.
    pub volume_mode: Option<PersistentVolumeMode>,
    pub mounter: SafeFormatAndMount,
    pub exec: Arc<dyn Exec>,
    pub device_util: Arc<dyn DeviceUtil>,
    pub mount_options: Vec<String>,
    pub mounted_with_selinux_context: Arc<AtomicBool>,
}

#[async_trait]
impl super::Mounter for IscsiDiskMounter {
    fn get_path(&self) -> String {
        self.disk.get_path()
    }

    /// `GetAttributes` (`iscsi.go:366-372`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: self.read_only,
            managed: !self.read_only,
            selinux_relabel: !self.mounted_with_selinux_context.load(Ordering::SeqCst),
        }
    }

    /// `SetUpAt` (`iscsi.go:378-391`). `diskSetUp` blocks (mount(8), stat), so
    /// it runs off the async runtime.
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
        .map_err(|e| anyhow!("iscsi SetUpAt task failed: {e}"))?;
        if res.is_err() {
            error!("iscsi: failed to setup");
        }

        if rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
        ) {
            // The volume must have been mounted in MountDevice with -o context.
            // TODO: extract from mount table in GetAttributes() to be sure?
            self.mounted_with_selinux_context
                .store(!args.selinux_label.is_empty(), Ordering::SeqCst);
        }
        res
    }
}

impl MetricsProvider for IscsiDiskMounter {
    fn get_metrics(&self) -> Result<Metrics> {
        self.disk.metrics.get_metrics()
    }
}

impl MetricsProvider for IscsiDiskUnmounter {
    fn get_metrics(&self) -> Result<Metrics> {
        self.disk.metrics.get_metrics()
    }
}

/// `iscsiDiskUnmounter` (`iscsi.go:393-398`).
#[derive(Clone)]
pub(crate) struct IscsiDiskUnmounter {
    pub disk: IscsiDisk,
    pub mounter: Arc<dyn MountInterface>,
    pub exec: Arc<dyn Exec>,
    pub device_util: Arc<dyn DeviceUtil>,
}

#[async_trait]
impl Unmounter for IscsiDiskUnmounter {
    fn get_path(&self) -> String {
        self.disk.get_path()
    }

    /// `TearDownAt` (`iscsi.go:408-410`): unmounts the bind mount; the disk is
    /// detached only by `UnmountDevice` (`attacher.go:171-184`).
    async fn tear_down_at(&self, dir: &str) -> Result<()> {
        let mounter = self.mounter.clone();
        let dir = dir.to_string();
        tokio::task::spawn_blocking(move || cleanup_mount_point(&dir, mounter.as_ref(), false))
            .await
            .map_err(|e| anyhow!("iscsi TearDownAt task failed: {e}"))??;
        Ok(())
    }
}

/// `iscsiDiskMapper` (`iscsi.go:413-418`), the `BlockVolumeMapper`.
#[allow(dead_code)] // mirrors upstream's struct; fields are read by its (unported) `MapPodDevice`
pub(crate) struct IscsiDiskMapper {
    pub disk: IscsiDisk,
    pub read_only: bool,
    pub exec: Arc<dyn Exec>,
    pub device_util: Arc<dyn DeviceUtil>,
    metrics: MetricsBlock,
}

impl MetricsProvider for IscsiDiskMapper {
    fn get_metrics(&self) -> Result<Metrics> {
        self.metrics.get_metrics()
    }
}

impl BlockVolumeMapper for IscsiDiskMapper {
    fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        self.disk.get_global_map_path(spec)
    }

    fn get_pod_device_map_path(&self) -> (String, String) {
        self.disk.iscsi_pod_device_map_path()
    }

    /// `SupportsMetrics` (`iscsi.go:431-433`): true, as it initializes the
    /// MetricsProvider.
    fn supports_metrics(&self) -> bool {
        true
    }
}

/// `iscsiDiskUnmapper` (`iscsi.go:422-427`), the `BlockVolumeUnmapper`;
/// `volume.MetricsNil` is its `MetricsProvider`.
pub(crate) struct IscsiDiskUnmapper {
    pub disk: IscsiDisk,
    pub exec: Arc<dyn Exec>,
    pub device_util: Arc<dyn DeviceUtil>,
}

impl MetricsProvider for IscsiDiskUnmapper {
    fn get_metrics(&self) -> Result<Metrics> {
        MetricsNil.get_metrics()
    }
}

impl BlockVolumeUnmapper for IscsiDiskUnmapper {
    fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String> {
        self.disk.get_global_map_path(spec)
    }

    fn get_pod_device_map_path(&self) -> (String, String) {
        self.disk.iscsi_pod_device_map_path()
    }

    /// `MetricsNil.SupportsMetrics` (`metrics_nil.go:27-29`).
    fn supports_metrics(&self) -> bool {
        false
    }

    /// `var _ volume.CustomBlockVolumeUnmapper = &iscsiDiskUnmapper{}`
    /// (`iscsi.go:436`).
    fn as_custom_block_volume_unmapper(&self) -> Option<&dyn CustomBlockVolumeUnmapper> {
        Some(self)
    }
}

#[async_trait]
impl CustomBlockVolumeUnmapper for IscsiDiskUnmapper {
    /// `TearDownDevice` (`iscsi.go:441-453`). Even though iSCSI plugin has
    /// attacher/detacher implementation, iSCSI plugin needs volume detach
    /// operation during TearDownDevice(). This method is only chance that
    /// operations are done on kubelet node during volume teardown sequences.
    async fn tear_down_device(&self, map_path: &str, _device_path: &str) -> Result<()> {
        // `DetachBlockISCSIDisk(*c, mapPath)` copies the unmapper but shares
        // the `*iscsiDisk` it loads `iscsi.json` into.
        let mut c = IscsiDiskUnmapper {
            disk: self.disk.clone(),
            exec: self.exec.clone(),
            device_util: self.device_util.clone(),
        };
        self.disk
            .manager
            .detach_block_iscsi_disk(&mut c, map_path)
            .map_err(|e| anyhow!("iscsi: failed to detach disk: {map_path}\nError: {e}"))?;
        tracing::debug!("iscsi: {map_path:?} is unmounted, deleting the directory");
        std::fs::remove_dir_all(map_path)
            .or_else(|e| {
                // `os.RemoveAll` of a missing path is nil.
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e)
                }
            })
            .map_err(|e| {
                anyhow!("iscsi: failed to delete the directory: {map_path}\nError: {e}")
            })?;
        tracing::debug!("iscsi: successfully detached disk: {map_path}");
        Ok(())
    }

    /// `UnmapPodDevice` (`iscsi.go:455-457`).
    async fn unmap_pod_device(&self) -> Result<()> {
        Ok(())
    }
}

/// `GetReliableMountRefs` (`util.go:661-683`) plus the caller's
/// `IsInconsistentReadError` logging (`iscsi.go:229-233`).
fn get_reliable_mount_refs(
    mounter: &super::util::mount_ops::Mounter,
    mount_path: &str,
) -> Result<Vec<String>> {
    match super::util::mount_utils::get_reliable_mount_refs(mounter, mount_path) {
        Err(e) if super::util::mount_utils::is_inconsistent_read_error(&e) => {
            tracing::error!("Failed to read mount refs from /proc/mounts for {mount_path}: {e}");
            tracing::error!(
                "Kubelet cannot unmount volume at {mount_path}, please unmount it and all mounts of the same device manually."
            );
            Err(e.into())
        }
        r => Ok(r?),
    }
}

/// `portalMounter` (`iscsi.go:472-477`).
pub(crate) fn portal_mounter(portal: &str) -> String {
    if !portal.contains(':') {
        format!("{portal}:3260")
    } else {
        portal.to_string()
    }
}

const NOT_ISCSI: &str = "Spec does not reference an ISCSI volume type";

/// The one inline-or-PV source arm every `getISCSI*` helper switches on
/// (`spec.Volume.ISCSI` first, else `spec.PersistentVolume.Spec.ISCSI`).
/// `bool` is whether it is the inline arm.
fn iscsi_source<'a>(spec: &'a Spec<'_>) -> Option<(&'a ISCSIVolumeSource, bool)> {
    if let Some(s) = spec.volume.iscsi.as_ref() {
        return Some((s, true));
    }
    spec.persistent_volume
        .and_then(|pv| pv.spec.iscsi.as_ref())
        .map(|s| (s, false))
}

/// `getISCSIVolumeInfo` (`iscsi.go:480-491`): readOnly and fstype. For a
/// volume source, readonly is in volume spec; for a PV it is `spec.ReadOnly`,
/// which the PV gets indirectly through the PVC source.
pub(crate) fn get_iscsi_volume_info(spec: &Spec<'_>) -> Result<(bool, String)> {
    match iscsi_source(spec) {
        Some((s, true)) => Ok((
            s.read_only.unwrap_or(false),
            s.fs_type.clone().unwrap_or_default(),
        )),
        Some((s, false)) => Ok((spec.read_only, s.fs_type.clone().unwrap_or_default())),
        None => bail!(NOT_ISCSI),
    }
}

/// `getISCSITargetInfo` (`iscsi.go:494-503`): target portal, portals, iqn,
/// and lun.
pub(crate) fn get_iscsi_target_info(spec: &Spec<'_>) -> Result<(String, Vec<String>, String, i32)> {
    let (s, _) = iscsi_source(spec).ok_or_else(|| anyhow!(NOT_ISCSI))?;
    Ok((
        s.target_portal.clone(),
        s.portals.clone().unwrap_or_default(),
        s.iqn.clone(),
        s.lun,
    ))
}

/// `getISCSIInitiatorInfo` (`iscsi.go:506-515`): iface and initiator name.
///
/// `iscsiInterface` is `+default="default"` in `core/v1`
/// (`staging/src/k8s.io/api/core/v1/types.go:1510`, `:1558`); the API server
/// applies it (`SetObjectDefaults_Pod`/`_PersistentVolume`), so like upstream
/// the kubelet reads the field as stored (#3491).
pub(crate) fn get_iscsi_initiator_info(spec: &Spec<'_>) -> Result<(String, Option<String>)> {
    let (s, _) = iscsi_source(spec).ok_or_else(|| anyhow!(NOT_ISCSI))?;
    Ok((
        s.iscsi_interface.clone().unwrap_or_default(),
        s.initiator_name.clone(),
    ))
}

/// `getISCSIDiscoveryCHAPInfo` (`iscsi.go:518-527`).
pub(crate) fn get_iscsi_discovery_chap_info(spec: &Spec<'_>) -> Result<bool> {
    let (s, _) = iscsi_source(spec).ok_or_else(|| anyhow!(NOT_ISCSI))?;
    Ok(s.chap_auth_discovery.unwrap_or(false))
}

/// `getISCSISessionCHAPInfo` (`iscsi.go:530-539`).
pub(crate) fn get_iscsi_session_chap_info(spec: &Spec<'_>) -> Result<bool> {
    let (s, _) = iscsi_source(spec).ok_or_else(|| anyhow!(NOT_ISCSI))?;
    Ok(s.chap_auth_session.unwrap_or(false))
}

/// `getISCSISecretNameAndNamespace` (`iscsi.go:542-562`): secret name and
/// namespace. An inline volume's `LocalObjectReference` has no namespace; a
/// PV's `SecretReference` may name one, else the default applies.
pub(crate) fn get_iscsi_secret_name_and_namespace(
    spec: &Spec<'_>,
    default_secret_namespace: &str,
) -> Result<(String, String)> {
    match iscsi_source(spec) {
        Some((s, true)) => match s.secret_ref.as_ref() {
            Some(r) => Ok((
                r.name.clone().unwrap_or_default(),
                default_secret_namespace.to_string(),
            )),
            None => Ok((String::new(), String::new())),
        },
        Some((s, false)) => match s.secret_ref.as_ref() {
            Some(r) => {
                let ns = r
                    .namespace
                    .clone()
                    .filter(|n| !n.is_empty())
                    .unwrap_or_else(|| default_secret_namespace.to_string());
                Ok((r.name.clone().unwrap_or_default(), ns))
            }
            None => Ok((String::new(), String::new())),
        },
        None => bail!(NOT_ISCSI),
    }
}

/// `createISCSIDisk` (`iscsi.go:564-615`).
pub(crate) fn create_iscsi_disk(
    spec: &Spec<'_>,
    pod_uid: &str,
    plugin: &IscsiPlugin,
    manager: Arc<dyn DiskManager>,
    secret: HashMap<String, String>,
) -> Result<IscsiDisk> {
    let (tp, portals, iqn, lun_int) = get_iscsi_target_info(spec)?;

    let lun = lun_int.to_string();
    let portal = portal_mounter(&tp);
    let mut bkportal = vec![portal];
    for p in &portals {
        bkportal.push(portal_mounter(p));
    }

    let (iface, initiator_name_ptr) = get_iscsi_initiator_info(spec)?;
    let initiator_name = initiator_name_ptr.unwrap_or_default();
    let chap_discovery = get_iscsi_discovery_chap_info(spec)?;
    let chap_session = get_iscsi_session_chap_info(spec)?;

    let init_iface = iface.clone();
    let iface = if !initiator_name.is_empty() {
        format!("{}:{}", bkportal[0], spec.name())
    } else {
        iface
    };

    Ok(IscsiDisk {
        vol_name: spec.name().to_string(),
        pod_uid: pod_uid.to_string(),
        portals: bkportal,
        iqn,
        lun,
        init_iface,
        iface,
        chap_discovery,
        chap_session,
        secret,
        initiator_name,
        plugin: plugin.clone(),
        manager,
        metrics: MetricsStatFs::new(""),
    })
}

/// `createSecretMap` (`iscsi.go:617-652`).
pub(crate) async fn create_secret_map(
    spec: &Spec<'_>,
    plugin: &IscsiPlugin,
    namespace: &str,
) -> Result<HashMap<String, String>> {
    let mut secret = HashMap::new();
    let chap_discover = get_iscsi_discovery_chap_info(spec)?;
    let chap_session = get_iscsi_session_chap_info(spec)?;
    if chap_discover || chap_session {
        let (secret_name, secret_namespace) = get_iscsi_secret_name_and_namespace(spec, namespace)?;

        if !secret_name.is_empty() && !secret_namespace.is_empty() {
            // if secret is provideded, retrieve it
            let Some(kube_client) = plugin.host.get_kube_client() else {
                bail!("cannot get kube client");
            };
            let key = build_key("secrets", Some(&secret_namespace), &secret_name);
            let secret_obj: Secret = kube_client.get(&key).await.map_err(|e| {
                anyhow!("couldn't get secret {secret_namespace}/{secret_name} error: {e}")
            })?;
            for (name, data) in secret_obj.data.unwrap_or_default() {
                tracing::debug!("retrieving CHAP secret name: {name}");
                secret.insert(name, String::from_utf8_lossy(&data).into_owned());
            }
        }
    }
    Ok(secret)
}

/// The sync trait method [`BlockVolumePlugin::new_block_volume_mapper`] reads
/// the CHAP Secret only when CHAP is on; that read is the one async step.
fn block_on_secret_map(
    spec: &Spec<'_>,
    plugin: &IscsiPlugin,
    namespace: &str,
) -> Result<HashMap<String, String>> {
    match tokio::runtime::Handle::try_current() {
        Ok(h) => {
            tokio::task::block_in_place(|| h.block_on(create_secret_map(spec, plugin, namespace)))
        }
        Err(_) => bail!("cannot read the CHAP secret outside a runtime"),
    }
}

/// `createPersistentVolumeFromISCSIPVSource` (`iscsi.go:654-667`).
fn create_persistent_volume_from_iscsi_pv_source(
    volume_name: &str,
    iscsi: ISCSIVolumeSource,
) -> Result<OwnedSpec> {
    let pv: rusternetes_common::resources::PersistentVolume =
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": volume_name},
            "spec": {"iscsi": serde_json::to_value(&iscsi)?, "volumeMode": "Block"}
        }))?;
    // `volume.NewSpecFromPersistentVolume(iscsiPV, false)` (`iscsi.go:710`):
    // our `Spec` needs a `Volume`, named after the PV (see `Spec::name`).
    let volume: rusternetes_common::resources::Volume =
        serde_json::from_value(serde_json::json!({"name": volume_name}))?;
    Ok(OwnedSpec {
        volume,
        persistent_volume: Some(pv),
        read_only: false,
    })
}

/// `getVolumeSpecFromGlobalMapPath` (`iscsi.go:669-711`).
pub(crate) fn get_volume_spec_from_global_map_path(
    volume_name: &str,
    global_map_path: &str,
) -> Result<OwnedSpec> {
    // Retrieve volume spec information from globalMapPath
    // globalMapPath example:
    // plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}
    // plugins/kubernetes.io/iscsi/volumeDevices/iface-default/192.168.0.10:3260-iqn.2017-05.com.example:test-lun-0

    // device: 192.168.0.10:3260-iqn.2017-05.com.example:test-lun-0
    let (device, _) = util::extract_device_and_prefix(global_map_path)?;
    let (bkp_portal, iqn) = util::extract_portal_and_iqn(&device)?;
    let arr: Vec<&str> = device.split("-lun-").collect();
    if arr.len() < 2 {
        bail!("failed to retrieve lun from globalMapPath: {global_map_path}");
    }
    let lun: i32 = arr[1].parse().map_err(|e| anyhow!("{e}"))?;
    let (iface, found) = util::extract_iface(global_map_path);
    if !found {
        bail!("failed to retrieve iface from globalMapPath: {global_map_path}");
    }
    tracing::trace!(
        "ConstructBlockVolumeSpec: TargetPortal: {bkp_portal}, IQN: {iqn}, Lun: {lun}, ISCSIInterface: {iface}"
    );
    create_persistent_volume_from_iscsi_pv_source(
        volume_name,
        ISCSIVolumeSource {
            target_portal: bkp_portal,
            iqn,
            lun,
            iscsi_interface: Some(iface),
            ..Default::default()
        },
    )
}

/// `volumeSpecToMounter` (`attacher.go:194-234`) for `pod == nil`.
pub(crate) fn volume_spec_to_mounter_no_pod(
    spec: &Spec<'_>,
    host: &Arc<dyn VolumeHost>,
    target_locks: &Arc<KeyMutex>,
) -> Result<IscsiDiskMounter> {
    volume_spec_to_mounter_with(spec, host, target_locks, "", HashMap::new())
}

/// The part of `volumeSpecToMounter` after the secret has been resolved.
pub(crate) fn volume_spec_to_mounter_with(
    spec: &Spec<'_>,
    host: &Arc<dyn VolumeHost>,
    target_locks: &Arc<KeyMutex>,
    pod_uid: &str,
    secret: HashMap<String, String>,
) -> Result<IscsiDiskMounter> {
    let (read_only, fs_type) = get_iscsi_volume_info(spec)?;
    let plugin = IscsiPlugin {
        host: host.clone(),
        target_locks: target_locks.clone(),
    };
    let iscsi_disk = create_iscsi_disk(spec, pod_uid, &plugin, Arc::new(IscsiUtil), secret)?;
    let exec: Arc<dyn Exec> = Arc::new(RealExec);

    let volume_mode = get_volume_mode(spec)?;

    tracing::trace!("iscsi: VolumeSpecToMounter volumeMode {volume_mode:?}");
    Ok(IscsiDiskMounter {
        disk: iscsi_disk,
        fs_type,
        volume_mode: Some(volume_mode),
        read_only,
        mounter: SafeFormatAndMount {
            mounter: Arc::new(host.get_mounter().clone()),
            exec: exec.clone(),
            format_limit: Default::default(),
        },
        exec,
        device_util: new_os_device_handler(),
        mount_options: Vec::new(),
        mounted_with_selinux_context: Arc::new(AtomicBool::new(false)),
    })
}

/// `GetVolumeMode` (`pkg/volume/util/util.go:339-347`): a spec without a PV
/// is Filesystem; a PV must carry a `volumeMode`.
pub(crate) fn get_volume_mode(spec: &Spec<'_>) -> Result<PersistentVolumeMode> {
    let Some(pv) = spec.persistent_volume else {
        return Ok(PersistentVolumeMode::Filesystem);
    };
    match pv.spec.volume_mode.clone() {
        Some(m) => Ok(m),
        None => bail!("cannot get volumeMode for volume: {}", spec.name()),
    }
}

/// The "uncertain" error `AttachDisk` returns when it cannot persist
/// `iscsi.json` (`iscsi_util.go:477-481`).
pub(crate) fn uncertain_progress(msg: String) -> anyhow::Error {
    VolumeOperationError::UncertainProgress(msg).into()
}
