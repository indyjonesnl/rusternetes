//! Port of `pkg/volume/iscsi/attacher.go`: `iscsiAttacher` (the
//! `volume.Attacher` / `volume.DeviceMounter`) and `iscsiDetacher` (its
//! `volume.DeviceUnmounter` half).
//!
//! Not ported: `Attach`, `VolumesAreAttached`, `Detach`, `NewDetacher` and
//! `VerifyExhaustedResource` (`attacher.go:57-59`, `:122-129`) belong to the
//! attach/detach controller, which this project does not run in the kubelet.
//! `mountArgs.SELinuxLabel` / `AddSELinuxMountOption` (`attacher.go:128-130`)
//! waits on [`DeviceMounterArgs`] carrying the label (#2312, #3493). Nothing
//! calls `UnmountDevice` yet (#3492).

use super::{
    create_secret_map, get_iscsi_volume_info, volume_spec_to_mounter_with, DiskManager, IscsiDisk,
    IscsiDiskUnmounter, IscsiPlugin, IscsiUtil,
};
use crate::volume_plugins::plugin::{
    AttachableVolumePlugin, Attacher, DeviceMounter, DeviceMounterArgs, DeviceUnmounter,
};
use crate::volume_plugins::util::exec::{Exec, RealExec};
use crate::volume_plugins::util::mount_interface::{MountInterface, SafeFormatAndMount};
use crate::volume_plugins::util::{device_util::new_os_device_handler, mount_option_from_spec};
use crate::volume_plugins::Spec;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::Pod;
use std::collections::HashMap;
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::sync::Arc;
use tracing::warn;

/// `iscsiAttacher` (`attacher.go:35-39`), which also serves as the
/// `iscsiDetacher` (`attacher.go:143-148`): both hold the host, the plugin's
/// target locks and a `diskManager`.
pub struct IscsiAttacher {
    pub(crate) plugin: IscsiPlugin,
    pub(crate) manager: Arc<dyn DiskManager>,
}

impl IscsiPlugin {
    /// `NewAttacher` (`attacher.go:49-55`).
    pub(crate) fn new_attacher_internal(&self) -> IscsiAttacher {
        IscsiAttacher {
            plugin: self.clone(),
            manager: Arc::new(IscsiUtil),
        }
    }
}

impl AttachableVolumePlugin for IscsiPlugin {
    /// `NewAttacher` (`attacher.go:49-55`).
    fn new_attacher(&self) -> Result<Box<dyn Attacher>> {
        Ok(Box::new(self.new_attacher_internal()))
    }

    /// `NewDeviceUnmounter` (`attacher.go:163-165`): `plugin.NewDetacher()`.
    fn new_device_unmounter(&self) -> Result<Box<dyn DeviceUnmounter>> {
        Ok(Box::new(self.new_attacher_internal()))
    }

    /// `GetDeviceMountRefs` (`attacher.go:65-68`).
    fn get_device_mount_refs(&self, device_mount_path: &str) -> Result<Vec<String>> {
        let mounter = self.host.get_mounter();
        Ok(MountInterface::get_mount_refs(mounter, device_mount_path)?)
    }
}

#[async_trait]
impl Attacher for IscsiAttacher {
    /// `WaitForAttach` (`attacher.go:83-90`): `volumeSpecToMounter` with the
    /// pod, then `AttachDisk` (the `iscsiadm` login, scan and device wait).
    async fn wait_for_attach(
        &self,
        spec: &Spec<'_>,
        _device_path: &str,
        pod: &Pod,
        _timeout: std::time::Duration,
    ) -> Result<String> {
        // `volumeSpecToMounter` (`attacher.go:194-234`) with a pod: the CHAP
        // secret is read in the pod's namespace.
        let secret = create_secret_map(
            spec,
            &self.plugin,
            pod.metadata.namespace.as_deref().unwrap_or_default(),
        )
        .await
        .inspect_err(|e| warn!("failed to get iscsi mounter: {e}"))?;
        let mounter = volume_spec_to_mounter_with(
            spec,
            &self.plugin.host,
            &self.plugin.target_locks,
            &pod.metadata.uid,
            secret,
        )
        .inspect_err(|e| warn!("failed to get iscsi mounter: {e}"))?;
        let manager = self.manager.clone();
        // `AttachDisk` blocks for minutes (`iscsiadm`, sleeps, sysfs).
        tokio::task::spawn_blocking(move || manager.attach_disk(&mounter))
            .await
            .map_err(|e| anyhow!("iscsi AttachDisk task failed: {e}"))?
    }
}

#[async_trait]
impl DeviceMounter for IscsiAttacher {
    /// `GetDeviceMountPath` (`attacher.go:92-104`).
    fn get_device_mount_path(&self, spec: &Spec<'_>) -> Result<String> {
        let mut mounter = volume_spec_to_mounter_with(
            spec,
            &self.plugin.host,
            &self.plugin.target_locks,
            "",
            HashMap::new(),
        )
        .inspect_err(|e| warn!("failed to get iscsi mounter: {e}"))?;
        if !mounter.disk.initiator_name.is_empty() {
            // new iface name is <target portal>:<volume name>
            mounter.disk.iface = format!("{}:{}", mounter.disk.portals[0], mounter.disk.vol_name);
        }
        Ok(self.manager.make_global_pd_name(&mounter.disk))
    }

    /// `MountDevice` (`attacher.go:106-141`).
    async fn mount_device(
        &self,
        spec: &Spec<'_>,
        device_path: &str,
        device_mount_path: &str,
        _args: &DeviceMounterArgs,
    ) -> Result<()> {
        let mounter: Arc<dyn MountInterface> = Arc::new(self.plugin.host.get_mounter().clone());
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
        let (read_only, fs_type) = get_iscsi_volume_info(spec)?;

        let mut options: Vec<String> = Vec::new();
        if read_only {
            options.push("ro".to_string());
        }
        // `mountArgs.SELinuxLabel` -> `AddSELinuxMountOption`: see the module docs.
        if not_mnt {
            let exec: Arc<dyn Exec> = Arc::new(RealExec);
            let disk_mounter = SafeFormatAndMount::new(mounter, exec);
            let mount_options = mount_option_from_spec(spec, &options);
            let (device_path, target) = (device_path.to_string(), device_mount_path.to_string());
            let res = tokio::task::spawn_blocking(move || {
                disk_mounter.format_and_mount(&device_path, &target, &fs_type, &mount_options)
            })
            .await
            .map_err(|e| anyhow!("iscsi MountDevice task failed: {e}"))?;
            if let Err(e) = res {
                let _ = std::fs::remove_dir(device_mount_path);
                return Err(e.into());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl DeviceUnmounter for IscsiAttacher {
    /// `UnmountDevice` (`attacher.go:171-184`).
    async fn unmount_device(&self, device_mount_path: &str) -> Result<()> {
        let un_mounter = volume_spec_to_unmounter(&self.plugin);
        let manager = self.manager.clone();
        let path = device_mount_path.to_string();
        tokio::task::spawn_blocking(move || {
            let mut un_mounter = un_mounter;
            manager.detach_disk(&mut un_mounter, &path)
        })
        .await
        .map_err(|e| anyhow!("iscsi DetachDisk task failed: {e}"))?
        .map_err(|e| anyhow!("iscsi: failed to detach disk: {device_mount_path}\nError: {e}"))?;
        tracing::debug!("iscsi: {device_mount_path:?} is unmounted, deleting the directory");
        match std::fs::remove_dir_all(device_mount_path) {
            // `os.RemoveAll` of a missing path is nil.
            Err(e) if e.kind() != io::ErrorKind::NotFound => {
                return Err(anyhow!(
                    "iscsi: failed to delete the directory: {device_mount_path}\nError: {e}"
                ))
            }
            _ => {}
        }
        tracing::debug!("iscsi: successfully detached disk: {device_mount_path}");
        Ok(())
    }
}

/// `volumeSpecToUnmounter` (`attacher.go:236-245`).
fn volume_spec_to_unmounter(plugin: &IscsiPlugin) -> IscsiDiskUnmounter {
    IscsiDiskUnmounter {
        disk: IscsiDisk::blank_for_plugin(plugin.clone()),
        mounter: Arc::new(plugin.host.get_mounter().clone()),
        exec: Arc::new(RealExec),
        device_util: new_os_device_handler(),
    }
}
