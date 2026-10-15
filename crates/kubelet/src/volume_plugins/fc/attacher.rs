//! Port of `pkg/volume/fc/attacher.go`: `fcAttacher` (the `volume.Attacher` /
//! `volume.DeviceMounter`) and `fcDetacher` (its `volume.DeviceUnmounter`
//! half).
//!
//! Not ported: `Attach`, `VolumesAreAttached`, `Detach`, `NewDetacher` and
//! `VerifyExhaustedResource` (`attacher.go:65-80`, `:162-164`) belong to the
//! attach/detach controller, which this project does not run in the kubelet
//! (#3498). `mountArgs.SELinuxLabel` / `AddSELinuxMountOption`
//! (`attacher.go:125-127`) waits on [`DeviceMounterArgs`] carrying the label
//! (#2312, #3493). Nothing calls `UnmountDevice` yet (#3492).

use super::{
    get_volume_source, volume_spec_to_mounter, DiskManager, FcDisk, FcDiskUnmounter, FcPlugin,
    FcUtil, OsIoHandler,
};
use crate::volume_plugins::plugin::{
    AttachableVolumePlugin, Attacher, DeviceMounter, DeviceMounterArgs, DeviceUnmounter,
};
use crate::volume_plugins::util::exec::{Exec, RealExec};
use crate::volume_plugins::util::mount_interface::{MountInterface, SafeFormatAndMount};
use crate::volume_plugins::util::mount_list::get_device_name_from_mount;
use crate::volume_plugins::util::mount_ops::cleanup_mount_point;
use crate::volume_plugins::util::{device_util::new_os_device_handler, mount_option_from_spec};
use crate::volume_plugins::{Spec, VolumeHost};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::Pod;
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// `fcAttacher` (`attacher.go:36-39`).
pub struct FcAttacher {
    pub(crate) host: Arc<dyn VolumeHost>,
    pub(crate) manager: Arc<dyn DiskManager>,
}

impl FcPlugin {
    /// `NewAttacher` (`attacher.go:49-54`).
    pub(crate) fn new_attacher_internal(&self) -> FcAttacher {
        FcAttacher {
            host: self.host.clone(),
            manager: Arc::new(FcUtil),
        }
    }

    /// `NewDetacher` (`attacher.go:150-156`).
    pub(crate) fn new_detacher_internal(&self) -> FcDetacher {
        FcDetacher {
            mounter: Arc::new(self.host.get_mounter().clone()),
            manager: Arc::new(FcUtil),
            host: self.host.clone(),
        }
    }
}

impl AttachableVolumePlugin for FcPlugin {
    /// `NewAttacher` (`attacher.go:49-54`).
    fn new_attacher(&self) -> Result<Box<dyn Attacher>> {
        Ok(Box::new(self.new_attacher_internal()))
    }

    /// `NewDeviceUnmounter` (`attacher.go:158-160`): `plugin.NewDetacher()`.
    fn new_device_unmounter(&self) -> Result<Box<dyn DeviceUnmounter>> {
        Ok(Box::new(self.new_detacher_internal()))
    }

    /// `GetDeviceMountRefs` (`attacher.go:60-63`).
    fn get_device_mount_refs(&self, device_mount_path: &str) -> Result<Vec<String>> {
        let mounter = self.host.get_mounter();
        Ok(MountInterface::get_mount_refs(mounter, device_mount_path)?)
    }
}

#[async_trait]
impl Attacher for FcAttacher {
    /// `WaitForAttach` (`attacher.go:82-89`): `volumeSpecToMounter`, then
    /// `AttachDisk` (the `/dev/disk` search, SCSI rescan and multipath lookup).
    async fn wait_for_attach(
        &self,
        spec: &Spec<'_>,
        _device_path: &str,
        _pod: &Pod,
        _timeout: Duration,
    ) -> Result<String> {
        let mounter = volume_spec_to_mounter(spec, &self.host)
            .inspect_err(|e| warn!("failed to get fc mounter: {e}"))?;
        let manager = self.manager.clone();
        // `AttachDisk` blocks (sysfs writes, symlink walks).
        tokio::task::spawn_blocking(move || manager.attach_disk(&mounter))
            .await
            .map_err(|e| anyhow!("fc AttachDisk task failed: {e}"))?
    }
}

#[async_trait]
impl DeviceMounter for FcAttacher {
    /// `GetDeviceMountPath` (`attacher.go:91-100`).
    fn get_device_mount_path(&self, spec: &Spec<'_>) -> Result<String> {
        let mounter = volume_spec_to_mounter(spec, &self.host)
            .inspect_err(|e| warn!("failed to get fc mounter: {e}"))?;
        Ok(self.manager.make_global_pd_name(&mounter.disk))
    }

    /// `MountDevice` (`attacher.go:102-138`).
    async fn mount_device(
        &self,
        spec: &Spec<'_>,
        device_path: &str,
        device_mount_path: &str,
        _args: &DeviceMounterArgs,
    ) -> Result<()> {
        let mounter: Arc<dyn MountInterface> = Arc::new(self.host.get_mounter().clone());
        let not_mnt = match mounter.is_likely_not_mount_point(device_mount_path) {
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

        let (volume_source, read_only) = get_volume_source(spec)?;

        let mut options: Vec<String> = Vec::new();
        if read_only {
            options.push("ro".to_string());
        }
        // `mountArgs.SELinuxLabel` -> `AddSELinuxMountOption`: see the module docs.
        if not_mnt {
            let exec: Arc<dyn Exec> = Arc::new(RealExec);
            let disk_mounter = SafeFormatAndMount::new(mounter, exec);
            let mount_options = mount_option_from_spec(spec, &options);
            let fs_type = volume_source.fs_type.clone().unwrap_or_default();
            let (device_path, target) = (device_path.to_string(), device_mount_path.to_string());
            let res = tokio::task::spawn_blocking(move || {
                disk_mounter.format_and_mount(&device_path, &target, &fs_type, &mount_options)
            })
            .await
            .map_err(|e| anyhow!("fc MountDevice task failed: {e}"))?;
            if let Err(e) = res {
                let _ = std::fs::remove_file(device_mount_path)
                    .or_else(|_| std::fs::remove_dir(device_mount_path));
                return Err(e.into());
            }
        }
        Ok(())
    }
}

/// `fcDetacher` (`attacher.go:140-144`).
pub struct FcDetacher {
    pub(crate) mounter: Arc<dyn MountInterface>,
    pub(crate) manager: Arc<dyn DiskManager>,
    pub(crate) host: Arc<dyn VolumeHost>,
}

/// `wait.PollImmediate` interval and timeout of `UnmountDevice`
/// (`attacher.go:192`).
const DETACH_POLL_INTERVAL: Duration = Duration::from_secs(10);
const DETACH_POLL_TIMEOUT: Duration = Duration::from_secs(2 * 60);

impl FcDetacher {
    /// `UnmountDevice` (`attacher.go:166-206`) with the poll cadence as
    /// parameters, so tests need not wait two minutes.
    pub(crate) async fn unmount_device_with(
        &self,
        device_mount_path: &str,
        poll_interval: Duration,
        poll_timeout: Duration,
    ) -> Result<()> {
        // Specify device name for DetachDisk later
        let mounter = self.mounter.clone();
        let path = device_mount_path.to_string();
        let dev_name = tokio::task::spawn_blocking({
            let mounter = mounter.clone();
            move || get_device_name_from_mount(mounter.as_ref(), &path)
        })
        .await
        .map_err(|e| anyhow!("fc GetDeviceNameFromMount task failed: {e}"))?;
        let (dev_name, _) = match dev_name {
            Ok(d) => d,
            Err(e) => {
                error!("fc: failed to get device from mnt: {device_mount_path}\nError: {e}");
                return Err(e.into());
            }
        };
        // Unmount for deviceMountPath(=globalPDPath)
        let path = device_mount_path.to_string();
        tokio::task::spawn_blocking({
            let mounter = mounter.clone();
            move || cleanup_mount_point(&path, mounter.as_ref(), false)
        })
        .await
        .map_err(|e| anyhow!("fc CleanupMountPoint task failed: {e}"))?
        .map_err(|e| anyhow!("fc: failed to unmount: {device_mount_path}\nError: {e}"))?;
        // GetDeviceNameFromMount from above returns an empty string if deviceMountPath is not a mount point
        // There is no need to DetachDisk if this is the case (and DetachDisk will throw an error if we attempt)
        if dev_name.is_empty() {
            return Ok(());
        }

        let un_mounter = Arc::new(volume_spec_to_unmounter(self.host.clone(), mounter));
        // The device is unmounted now. If UnmountDevice was retried, GetDeviceNameFromMount
        // won't find any mount and won't return DetachDisk below.
        // Therefore implement our own retry mechanism here.
        // E.g. DetachDisk sometimes fails to flush a multipath device with "device is busy" when it was
        // just unmounted.
        // 2 minutes should be enough within 6 minute force detach timeout.
        // `wait.PollImmediate(10*time.Second, 2*time.Minute, ...)`: one attempt
        // now, then one per interval until the timeout elapses.
        let start = tokio::time::Instant::now();
        let mut detach_error: Option<anyhow::Error> = None;
        let mut detached = false;
        loop {
            let (manager, un_mounter, dev) =
                (self.manager.clone(), un_mounter.clone(), dev_name.clone());
            let res = tokio::task::spawn_blocking(move || manager.detach_disk(&un_mounter, &dev))
                .await
                .map_err(|e| anyhow!("fc DetachDisk task failed: {e}"))?;
            match res {
                Ok(()) => {
                    detached = true;
                    break;
                }
                Err(e) => {
                    debug!("fc: failed to detach disk {dev_name} ({device_mount_path}): {e}");
                    detach_error = Some(e);
                }
            }
            if start.elapsed() + poll_interval > poll_timeout {
                break;
            }
            tokio::time::sleep(poll_interval).await;
        }
        if !detached {
            return Err(anyhow!(
                "fc: failed to detach disk: {dev_name}: {}",
                detach_error.map(|e| e.to_string()).unwrap_or_default()
            ));
        }

        info!("fc: successfully detached disk: {dev_name}");
        Ok(())
    }
}

#[async_trait]
impl DeviceUnmounter for FcDetacher {
    async fn unmount_device(&self, device_mount_path: &str) -> Result<()> {
        self.unmount_device_with(device_mount_path, DETACH_POLL_INTERVAL, DETACH_POLL_TIMEOUT)
            .await
    }
}

/// `volumeSpecToUnmounter` (`attacher.go:259-268`).
fn volume_spec_to_unmounter(
    host: Arc<dyn VolumeHost>,
    mounter: Arc<dyn MountInterface>,
) -> FcDiskUnmounter {
    FcDiskUnmounter {
        disk: FcDisk {
            vol_name: String::new(),
            pod_uid: String::new(),
            wwns: Vec::new(),
            lun: String::new(),
            wwids: Vec::new(),
            // Go's nil `plugin`/`manager`: `DetachDisk` reads neither, but ours
            // are non-optional.
            plugin: FcPlugin { host },
            manager: Arc::new(FcUtil),
            io: Arc::new(OsIoHandler),
        },
        mounter,
        device_util: new_os_device_handler(),
        exec: Arc::new(RealExec),
    }
}
