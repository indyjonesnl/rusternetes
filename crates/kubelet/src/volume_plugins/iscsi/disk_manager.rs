//! Port of `pkg/volume/iscsi/disk_manager.go`: the `diskManager` interface and
//! `diskSetUp`.

use super::{IscsiDisk, IscsiDiskMounter, IscsiDiskUnmapper, IscsiDiskUnmounter};
use crate::volume_plugins::util::join_mount_options;
use crate::volume_plugins::util::mount_interface::MountInterface;
use crate::volume_plugins::MounterArgs;
use anyhow::Result;
use std::io;
use std::os::unix::fs::DirBuilderExt;
use tracing::error;

/// Abstract interface to disk operations (`disk_manager.go:29-39`).
pub(crate) trait DiskManager: Send + Sync {
    fn make_global_pd_name(&self, disk: &IscsiDisk) -> String;
    fn make_global_vdpd_name(&self, disk: &IscsiDisk) -> String;
    /// Attaches the disk to the kubelet's host machine.
    fn attach_disk(&self, b: &IscsiDiskMounter) -> Result<String>;
    /// Detaches the disk from the kubelet's host machine. `c.disk` is shared
    /// with the caller in Go (`*iscsiDisk`), which `DetachDisk` loads
    /// `iscsi.json` into; here the load is visible through `&mut`.
    fn detach_disk(&self, c: &mut IscsiDiskUnmounter, mnt_path: &str) -> Result<()>;
    /// Detaches the block disk from the kubelet's host machine.
    fn detach_block_iscsi_disk(&self, c: &mut IscsiDiskUnmapper, mnt_path: &str) -> Result<()>;
}

/// `diskSetUp` (`disk_manager.go:44-106`): utility to mount a disk based
/// filesystem.
///
/// - `globalPDPath`: global mount path like,
///   `/var/lib/kubelet/plugins/kubernetes.io/iscsi/{ifaceName}/{portal-some_iqn-lun-lun_id}`
/// - `volPath`: pod volume dir path like,
///   `/var/lib/kubelet/pods/{podUID}/volumes/kubernetes.io~iscsi/{volumeName}`
pub(crate) fn disk_set_up(
    manager: &dyn DiskManager,
    b: &IscsiDiskMounter,
    vol_path: &str,
    mounter: &dyn MountInterface,
    mounter_args: &MounterArgs,
) -> Result<()> {
    let fs_group = mounter_args.fs_group;
    let fs_group_change_policy = mounter_args.fs_group_change_policy.as_deref();
    let not_mnt = match mounter.is_likely_not_mount_point(vol_path) {
        Ok(n) => n,
        // `if err != nil && !os.IsNotExist(err)`: notMnt is `true` with the
        // error from the stat.
        Err(e) if e.kind() == io::ErrorKind::NotFound => true,
        Err(e) => {
            error!("cannot validate mountpoint: {vol_path}");
            return Err(e.into());
        }
    };
    if !not_mnt {
        return Ok(());
    }

    if let Err(e) = std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o750)
        .create(vol_path)
    {
        error!("failed to mkdir:{vol_path}");
        return Err(e.into());
    }
    // Perform a bind mount to the full path to allow duplicate mounts of the same disk.
    let mut options = vec!["bind".to_string()];
    if b.read_only {
        options.push("ro".to_string());
    }
    let mut disk = b.disk.clone();
    if !disk.initiator_name.is_empty() {
        // new iface name is <target portal>:<volume name>
        disk.iface = format!("{}:{}", disk.portals[0], disk.vol_name);
    }
    let global_pd_path = manager.make_global_pd_name(&disk);
    let mount_options = join_mount_options(&b.mount_options, &options);
    if let Err(err) =
        mounter.mount_sensitive_without_systemd(&global_pd_path, vol_path, "", &mount_options, &[])
    {
        error!("Failed to bind mount: source:{global_pd_path}, target:{vol_path}, err:{err}");
        let no_mnt = match b.mounter.mounter.is_likely_not_mount_point(vol_path) {
            Ok(n) => n,
            Err(mnt_err) => {
                error!("IsLikelyNotMountPoint check failed: {mnt_err}");
                return Err(err.into());
            }
        };
        if !no_mnt {
            if let Err(mnt_err) = b.mounter.mounter.unmount(vol_path) {
                error!("Failed to unmount: {mnt_err}");
                return Err(err.into());
            }
            match b.mounter.mounter.is_likely_not_mount_point(vol_path) {
                Err(mnt_err) => {
                    error!("IsLikelyNotMountPoint check failed: {mnt_err}");
                    return Err(err.into());
                }
                Ok(false) => {
                    //  will most likely retry on next sync loop.
                    error!(
                        "{vol_path} is still mounted, despite call to unmount().  Will try again next sync loop."
                    );
                    return Err(err.into());
                }
                Ok(true) => {}
            }
        }
        let _ = std::fs::remove_dir(vol_path);
        return Err(err.into());
    }

    if !b.read_only {
        // This code requires larger refactor to monitor progress of ownership change
        // `volume.NewVolumeOwnership(&b, volPath, fsGroup, fsGroupChangePolicy,
        // util.FSGroupCompleteHook(b.plugin, nil)).ChangePermissions()`: the
        // mounter is not read-only here, so the walk applies the read-write mask.
        let _ = crate::volume_ownership::set_volume_ownership_with_policy(
            std::path::Path::new(vol_path),
            fs_group,
            fs_group_change_policy,
            false,
        );
    }

    Ok(())
}
