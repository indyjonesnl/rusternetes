//! Port of `pkg/volume/util/volumepathhandler/volume_path_handler.go`.
//!
//! Upstream maps a block device into a pod by creating, under the pod device
//! map path (`BlockVolume::GetPodDeviceMapPath`), a symlink named after the
//! volume whose target is the host device. The kubelet then hands that device
//! to the runtime as a CRI `Device` (`ContainerConfig.devices`), never as a
//! mount — so this module only prepares the host-side link; the CRI wiring is
//! the consumer's job.
//!
//! The bind-mount mapping (`mapBindMountDevice` / `unmapBindMountDevice`) goes
//! through [`MountInterface`] (upstream's inline `mount.New("")`), and the
//! losetup file-device functions (`volume_path_handler_linux.go`) through
//! [`Exec`] plus a sysfs root, so tests can substitute both.

use anyhow::{bail, Context, Result};
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::{symlink, FileTypeExt};
use std::path::Path;

use super::exec::{Exec, RealExec};
use super::mount_interface::MountInterface;
use super::mount_ops::{detect_safe_not_mounted_behavior, Mounter};

/// `losetupPath` (`volume_path_handler.go:33`).
const LOSETUP_PATH: &str = "losetup";
/// `ErrDeviceNotFound` (`volume_path_handler.go:34`); upstream compares
/// `err.Error()` against it, so the error text is the sentinel.
pub const ERR_DEVICE_NOT_FOUND: &str = "device not found";
/// The glob root of `getLoopDeviceFromSysfs` (`/sys/block/loop*`).
const SYS_BLOCK: &str = "/sys/block";

/// `mount.New("")` (`mount_linux.go:79-85`), as `host.rs` builds it.
fn default_mounter() -> Mounter {
    Mounter::new("", true, detect_safe_not_mounted_behavior())
}

/// Port of `VolumePathHandler` (`volume_path_handler.go:55`), the only
/// implementation of upstream's `BlockVolumePathHandler` interface.
#[derive(Debug, Default, Clone, Copy)]
pub struct VolumePathHandler;

impl VolumePathHandler {
    /// Port of `MapDevice` (`volume_path_handler.go:67-95`).
    pub fn map_device(
        &self,
        device_path: &str,
        map_path: &str,
        link_name: &str,
        bind_mount: bool,
    ) -> Result<()> {
        self.map_device_with(
            &default_mounter(),
            device_path,
            map_path,
            link_name,
            bind_mount,
        )
    }

    /// [`Self::map_device`] with the mounter `mapBindMountDevice` builds
    /// inline (`mount.New("")`) made explicit.
    pub fn map_device_with(
        &self,
        mounter: &dyn MountInterface,
        device_path: &str,
        map_path: &str,
        link_name: &str,
        bind_mount: bool,
    ) -> Result<()> {
        if device_path.is_empty() {
            bail!("failed to map device to map path. devicePath is empty");
        }
        if map_path.is_empty() {
            bail!("failed to map device to map path. mapPath is empty");
        }
        if !Path::new(map_path).is_absolute() {
            bail!("the map path should be absolute: map path: {map_path}");
        }
        match fs::metadata(map_path) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => bail!("cannot validate map path: {map_path}: {e}"),
        }
        if let Err(e) = fs::create_dir_all(map_path) {
            bail!("failed to mkdir {map_path}: {e}");
        }
        if bind_mount {
            return map_bind_mount_device(mounter, device_path, map_path, link_name);
        }
        map_symlink_device(device_path, map_path, link_name)
    }

    /// Port of `UnmapDevice` (`volume_path_handler.go:171-182`).
    pub fn unmap_device(&self, map_path: &str, link_name: &str, bind_mount: bool) -> Result<()> {
        self.unmap_device_with(&default_mounter(), map_path, link_name, bind_mount)
    }

    /// [`Self::unmap_device`] with the mounter made explicit.
    pub fn unmap_device_with(
        &self,
        mounter: &dyn MountInterface,
        map_path: &str,
        link_name: &str,
        bind_mount: bool,
    ) -> Result<()> {
        if map_path.is_empty() {
            bail!("failed to unmap device from map path. mapPath is empty");
        }
        if bind_mount {
            return self.unmap_bind_mount_device(mounter, map_path, link_name);
        }
        self.unmap_symlink_device(map_path, link_name)
    }

    /// Port of `unmapBindMountDevice` (`volume_path_handler.go:174-214`).
    fn unmap_bind_mount_device(
        &self,
        mounter: &dyn MountInterface,
        map_path: &str,
        link_name: &str,
    ) -> Result<()> {
        // Check bind mount exists
        let link_path = Path::new(map_path).join(link_name);
        let link_path_str = link_path.to_string_lossy();
        if !self.is_device_bind_mount_exist(&link_path_str)? {
            match fs::metadata(&link_path) {
                Ok(_) => {}
                // linkPath has already been removed
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
                Err(e) => bail!("failed to check if path {link_path_str} exists: {e}"),
            }
            // Remove file
            return remove_ignoring_not_found(&link_path);
        }

        // Unmount file
        mounter
            .unmount(&link_path_str)
            .map_err(|e| anyhow::anyhow!("failed to unmount linkPath {link_path_str}: {e}"))?;

        // Remove file
        remove_ignoring_not_found(&link_path)
    }

    /// Port of `unmapSymlinkDevice` (`volume_path_handler.go:216-227`).
    fn unmap_symlink_device(&self, map_path: &str, link_name: &str) -> Result<()> {
        let link_path = Path::new(map_path).join(link_name);
        if !self.is_symlink_exist(&link_path.to_string_lossy())? {
            return Ok(());
        }
        fs::remove_file(&link_path).map_err(Into::into)
    }

    /// Port of `RemoveMapPath` (`volume_path_handler.go:229-240`).
    pub fn remove_map_path(&self, map_path: &str) -> Result<()> {
        if map_path.is_empty() {
            bail!("failed to remove map path. mapPath is empty");
        }
        match fs::remove_dir_all(map_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => bail!("failed to remove directory {map_path}: {e}"),
        }
    }

    /// Port of `IsSymlinkExist` (`volume_path_handler.go:242-257`).
    pub fn is_symlink_exist(&self, map_path: &str) -> Result<bool> {
        match fs::symlink_metadata(map_path) {
            Ok(md) => Ok(md.file_type().is_symlink()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => bail!("failed to Lstat file {map_path}: {e}"),
        }
    }

    /// Port of `IsDeviceBindMountExist` (`volume_path_handler.go:259-276`).
    /// Go's `ModeDevice` covers block and character devices.
    pub fn is_device_bind_mount_exist(&self, map_path: &str) -> Result<bool> {
        match fs::symlink_metadata(map_path) {
            Ok(md) => {
                let ft = md.file_type();
                Ok(ft.is_block_device() || ft.is_char_device())
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => bail!("failed to Lstat file {map_path}: {e}"),
        }
    }

    /// Port of `GetDeviceBindMountRefs` (`volume_path_handler.go:284-300`):
    /// the device nodes (bind mounts) directly under `map_path`.
    pub fn get_device_bind_mount_refs(
        &self,
        _dev_path: &str,
        map_path: &str,
    ) -> Result<Vec<String>> {
        let mut refs = Vec::new();
        // `os.ReadDir` returns entries sorted by filename.
        let mut entries: Vec<_> = fs::read_dir(map_path)?.collect::<std::io::Result<_>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let ft = entry.file_type()?;
            if !(ft.is_block_device() || ft.is_char_device()) {
                continue;
            }
            // TODO upstream: might need to check the file is linked to devPath
            refs.push(
                Path::new(map_path)
                    .join(entry.file_name())
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        tracing::trace!("GetDeviceBindMountRefs: refs {refs:?}");
        Ok(refs)
    }

    /// Port of `AttachFileDevice` (`volume_path_handler_linux.go:38-54`).
    pub fn attach_file_device(&self, path: &str) -> Result<String> {
        attach_file_device_with(&RealExec, Path::new(SYS_BLOCK), path)
    }

    /// Port of `DetachFileDevice` (`volume_path_handler_linux.go:57-75`).
    pub fn detach_file_device(&self, path: &str) -> Result<()> {
        detach_file_device_with(&RealExec, Path::new(SYS_BLOCK), path)
    }

    /// Port of `GetLoopDevice` (`volume_path_handler_linux.go:77-87`).
    pub fn get_loop_device(&self, path: &str) -> Result<String> {
        get_loop_device_with(Path::new(SYS_BLOCK), path)
    }

    /// Port of `FindGlobalMapPathUUIDFromPod`
    /// (`volume_path_handler_linux.go:164-185`): find the `{pod uuid}` device
    /// node (bind mount) under `plugin_dir` whose device is the one the pod's
    /// `map_path` symlink points at. Returns the empty string when none
    /// matches.
    pub fn find_global_map_path_uuid_from_pod(
        &self,
        plugin_dir: &str,
        map_path: &str,
        pod_uid: &str,
    ) -> Result<String> {
        let mut global_map_path_uuid = String::new();
        // `filepath.Walk`: lexical order, symlinks not followed, a failed
        // `Lstat` of the root (or a failed directory read) aborts the walk.
        fn walk(path: &Path, f: &mut dyn FnMut(&Path, &fs::Metadata)) -> std::io::Result<()> {
            let md = fs::symlink_metadata(path)?;
            f(path, &md);
            if md.is_dir() {
                let mut names: Vec<_> = fs::read_dir(path)?
                    .map(|e| e.map(|e| e.file_name()))
                    .collect::<std::io::Result<_>>()?;
                names.sort();
                for n in names {
                    walk(&path.join(n), f)?;
                }
            }
            Ok(())
        }
        walk(Path::new(plugin_dir), &mut |path, md| {
            let ft = md.file_type();
            let is_device = ft.is_block_device() || ft.is_char_device();
            if is_device && path.file_name().is_some_and(|n| n == pod_uid) {
                tracing::debug!(
                    "FindGlobalMapPathFromPod: path {}, mapPath {map_path}",
                    path.display()
                );
                if let Ok(true) = compare_bind_mount_and_symlinks(&path.to_string_lossy(), map_path)
                {
                    global_map_path_uuid = path.to_string_lossy().into_owned();
                }
            }
        })
        .map_err(|e| anyhow::anyhow!("FindGlobalMapPathUUIDFromPod failed: {e}"))?;
        // Return path contains global map path + {pod uuid}
        Ok(global_map_path_uuid)
    }
}

/// Port of `compareBindMountAndSymlinks` (`volume_path_handler_linux.go:190-217`):
/// whether the global path (bind mount) and the pod path (symlink) point at the
/// same device, i.e. the devices have the same major/minor number.
fn compare_bind_mount_and_symlinks(global: &str, pod: &str) -> Result<bool> {
    let dev_num_global = get_device_major_minor(global)
        .map_err(|e| anyhow::anyhow!("getDeviceMajorMinor failed for path {global}: {e}"))?;
    let dev_pod =
        fs::read_link(pod).map_err(|e| anyhow::anyhow!("failed to readlink path {pod}: {e}"))?;
    let dev_pod = dev_pod.to_string_lossy();
    let dev_num_pod = get_device_major_minor(&dev_pod)
        .map_err(|e| anyhow::anyhow!("getDeviceMajorMinor failed for path {dev_pod}: {e}"))?;
    Ok(dev_num_global == dev_num_pod)
}

/// Port of `getDeviceMajorMinor` (`volume_path_handler_linux.go:224-236`):
/// `major:minor` in hex, from the path's `st_rdev`.
fn get_device_major_minor(path: &str) -> Result<String> {
    use std::os::unix::fs::MetadataExt;
    let md = fs::metadata(path).map_err(|e| anyhow::anyhow!("failed to stat path {path}: {e}"))?;
    let rdev = md.rdev();
    Ok(format!("{:x}:{:x}", libc::major(rdev), libc::minor(rdev)))
}

/// Port of `mapBindMountDevice` (`volume_path_handler.go:111-150`).
fn map_bind_mount_device(
    mounter: &dyn MountInterface,
    device_path: &str,
    map_path: &str,
    link_name: &str,
) -> Result<()> {
    // Check bind mount exists
    let link_path = Path::new(map_path).join(link_name);
    let link_path_str = link_path.to_string_lossy();
    match fs::metadata(&link_path) {
        Err(e) => {
            if e.kind() != ErrorKind::NotFound {
                bail!("failed to stat file {link_path_str}: {e}");
            }
            // Create file (O_CREATE|O_RDWR, 0750)
            use std::os::unix::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o750)
                .open(&link_path)
                .map_err(|e| anyhow::anyhow!("failed to open file {link_path_str}: {e}"))?;
        }
        Ok(md) => {
            // Check if device file
            // TODO upstream: check this is actually the expected bind mount
            let ft = md.file_type();
            if ft.is_block_device() || ft.is_char_device() {
                tracing::warn!(
                    "Warning: Map skipped because bind mount already exists on the path: {link_path_str}"
                );
                return Ok(());
            }
            tracing::warn!(
                "Warning: file {link_path_str} already exists but is not mounted, skip creating file"
            );
        }
    }

    // Bind mount file
    mounter
        .mount_sensitive_without_systemd(
            device_path,
            &link_path_str,
            "",
            &["bind".to_string()],
            &[],
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to bind mount devicePath: {device_path} to linkPath {link_path_str}: {e}"
            )
        })
}

/// Port of `AttachFileDevice` over an injectable `losetup` runner / sysfs.
fn attach_file_device_with(exec: &dyn Exec, sys_block: &Path, path: &str) -> Result<String> {
    let mut block_device_path = match get_loop_device_with(sys_block, path) {
        Ok(p) => p,
        Err(e) if e.to_string() == ERR_DEVICE_NOT_FOUND => String::new(),
        Err(e) => bail!("GetLoopDevice failed for path {path}: {e}"),
    };
    // If no existing loop device for the path, create one
    if block_device_path.is_empty() {
        tracing::debug!("Creating device for path: {path}");
        block_device_path = make_loop_device(exec, sys_block, path)
            .map_err(|e| anyhow::anyhow!("makeLoopDevice failed for path {path}: {e}"))?;
    }
    Ok(block_device_path)
}

/// Port of `DetachFileDevice` over an injectable `losetup` runner / sysfs.
fn detach_file_device_with(exec: &dyn Exec, sys_block: &Path, path: &str) -> Result<()> {
    match get_loop_device_with(sys_block, path) {
        Err(e) if e.to_string() == ERR_DEVICE_NOT_FOUND => {
            tracing::warn!(
                "couldn't find loopback device which takes file descriptor lock. Skip detaching device. device path: {path:?}"
            );
            Ok(())
        }
        Err(e) => bail!("GetLoopDevice failed for path {path}: {e}"),
        Ok(loop_path) => {
            if !loop_path.is_empty() {
                remove_loop_device(exec, &loop_path)
                    .map_err(|e| anyhow::anyhow!("removeLoopDevice failed for path {path}: {e}"))?;
            }
            Ok(())
        }
    }
}

/// Port of `GetLoopDevice` (`volume_path_handler_linux.go:77-87`).
fn get_loop_device_with(sys_block: &Path, path: &str) -> Result<String> {
    match fs::metadata(path) {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => bail!("{ERR_DEVICE_NOT_FOUND}"),
        Err(e) => bail!("not attachable: {e}"),
    }
    get_loop_device_from_sysfs(sys_block, path)
}

/// Port of `makeLoopDevice` (`volume_path_handler_linux.go:89-101`).
fn make_loop_device(exec: &dyn Exec, sys_block: &Path, path: &str) -> Result<String> {
    let args = vec!["-f".to_string(), path.to_string()];
    let (out, res) = exec.combined_output(LOSETUP_PATH, &args);
    if let Err(e) = res {
        tracing::debug!("Failed device create command for path: {path} {e} {out}");
        bail!("losetup {} failed: {e}", args.join(" "));
    }
    get_loop_device_from_sysfs(sys_block, path)
}

/// Port of `removeLoopDevice` (`volume_path_handler_linux.go:103-114`).
fn remove_loop_device(exec: &dyn Exec, device: &str) -> Result<()> {
    let args = vec!["-d".to_string(), device.to_string()];
    let (out, res) = exec.combined_output(LOSETUP_PATH, &args);
    if let Err(e) = res {
        if matches!(fs::metadata(device), Err(me) if me.kind() == ErrorKind::NotFound) {
            return Ok(());
        }
        tracing::debug!("Failed to remove loopback device: {device}: {e} {out}");
        bail!("losetup -d {device} failed: {e}");
    }
    Ok(())
}

/// Port of `getLoopDeviceFromSysfs` (`volume_path_handler_linux.go:119-148`):
/// the backing file of each `<sys_block>/loop*/loop/backing_file`.
fn get_loop_device_from_sysfs(sys_block: &Path, path: &str) -> Result<String> {
    // If the file is a symlink.
    let real_path = fs::canonicalize(path)
        .map_err(|e| anyhow::anyhow!("failed to evaluate path {path}: {e}"))?;
    let real_path = real_path.to_string_lossy();

    // `filepath.Glob("/sys/block/loop*")` is lexical; an unreadable sysfs
    // directory yields no matches rather than an error.
    let mut devices: Vec<_> = match fs::read_dir(sys_block) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("loop"))
            .map(|e| e.path())
            .collect(),
        Err(_) => Vec::new(),
    };
    devices.sort();

    for device in devices {
        // The contents of this file is the absolute path of "path".
        let Ok(data) = fs::read_to_string(device.join("loop/backing_file")) else {
            continue;
        };
        // Return the first match.
        let backing = clean_backing_file_path(&data);
        if backing == path || backing == real_path {
            let base = device.file_name().unwrap_or_default().to_string_lossy();
            return Ok(format!("/dev/{base}"));
        }
    }
    bail!("{ERR_DEVICE_NOT_FOUND}")
}

/// Port of `cleanBackingFilePath` (`volume_path_handler_linux.go:151-156`).
fn clean_backing_file_path(path: &str) -> String {
    // If the block device was deleted, the path will contain a "(deleted)" suffix
    let path = path.trim();
    let path = path.strip_suffix("(deleted)").unwrap_or(path);
    path.trim().to_string()
}

/// Port of `mapSymlinkDevice` (`volume_path_handler.go:160-169`): replace
/// whatever is at `<mapPath>/<linkName>` with a symlink to the device.
fn map_symlink_device(device_path: &str, map_path: &str, link_name: &str) -> Result<()> {
    let link_path = Path::new(map_path).join(link_name);
    remove_ignoring_not_found(&link_path)?;
    symlink(device_path, &link_path)
        .with_context(|| format!("failed to symlink {device_path} to {}", link_path.display()))
}

fn remove_ignoring_not_found(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => bail!("failed to remove file {}: {e}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    //! Ported from `volume_path_handler_test.go` (`TestMapDevice`,
    //! `TestUnmapDevice`, `TestRemoveMapPath`, `TestIsSymlinkExist`).
    use super::*;
    use tempfile::tempdir;

    fn err_of(r: Result<()>) -> String {
        r.unwrap_err().to_string()
    }

    #[test]
    fn map_device_valid_symlink() {
        let tmp = tempdir().unwrap();
        let map = tmp.path().join("test-map-valid");
        fs::create_dir_all(&map).unwrap();
        VolumePathHandler
            .map_device("/dev/fakeDevice", map.to_str().unwrap(), "validLink", false)
            .unwrap();
        let link = map.join("validLink");
        assert_eq!(fs::read_link(&link).unwrap(), Path::new("/dev/fakeDevice"));
    }

    #[test]
    fn map_device_replaces_existing_link() {
        let tmp = tempdir().unwrap();
        let map = tmp.path().to_str().unwrap();
        VolumePathHandler
            .map_device("/dev/a", map, "l", false)
            .unwrap();
        VolumePathHandler
            .map_device("/dev/b", map, "l", false)
            .unwrap();
        assert_eq!(
            fs::read_link(tmp.path().join("l")).unwrap(),
            Path::new("/dev/b")
        );
    }

    #[test]
    fn map_device_empty_device_path() {
        let tmp = tempdir().unwrap();
        let e = err_of(VolumePathHandler.map_device("", tmp.path().to_str().unwrap(), "l", false));
        assert_eq!(e, "failed to map device to map path. devicePath is empty");
    }

    #[test]
    fn map_device_empty_map_path() {
        let e = err_of(VolumePathHandler.map_device("/dev/fakeDevice", "", "l", false));
        assert_eq!(e, "failed to map device to map path. mapPath is empty");
    }

    #[test]
    fn map_device_relative_map_path() {
        let e = err_of(VolumePathHandler.map_device("/dev/x", "rel/path", "l", false));
        assert_eq!(e, "the map path should be absolute: map path: rel/path");
    }

    #[test]
    fn unmap_device_valid_symlink() {
        let tmp = tempdir().unwrap();
        symlink("/dev/fakeDevice", tmp.path().join("validLink")).unwrap();
        VolumePathHandler
            .unmap_device(tmp.path().to_str().unwrap(), "validLink", false)
            .unwrap();
        assert!(fs::symlink_metadata(tmp.path().join("validLink")).is_err());
    }

    #[test]
    fn unmap_device_symlink_does_not_exist() {
        let tmp = tempdir().unwrap();
        VolumePathHandler
            .unmap_device(tmp.path().to_str().unwrap(), "nonexistentLink", false)
            .unwrap();
    }

    #[test]
    fn unmap_device_bind_mount_file_exists_but_not_mounted() {
        let tmp = tempdir().unwrap();
        fs::write(tmp.path().join("bindFile"), b"").unwrap();
        VolumePathHandler
            .unmap_device(tmp.path().to_str().unwrap(), "bindFile", true)
            .unwrap();
        assert!(fs::symlink_metadata(tmp.path().join("bindFile")).is_err());
    }

    #[test]
    fn unmap_device_bind_mount_file_does_not_exist() {
        let tmp = tempdir().unwrap();
        VolumePathHandler
            .unmap_device(tmp.path().to_str().unwrap(), "bindFileNotExist", true)
            .unwrap();
    }

    #[test]
    fn unmap_device_empty_map_path() {
        for bind in [false, true] {
            let e = err_of(VolumePathHandler.unmap_device("", "someLink", bind));
            assert_eq!(e, "failed to unmap device from map path. mapPath is empty");
        }
    }

    #[test]
    fn remove_map_path_existing_and_missing() {
        let tmp = tempdir().unwrap();
        let p = tmp.path().join("test-remove-map-path");
        fs::create_dir_all(&p).unwrap();
        VolumePathHandler
            .remove_map_path(p.to_str().unwrap())
            .unwrap();
        assert!(!p.exists());
        VolumePathHandler
            .remove_map_path(tmp.path().join("non-existing-path").to_str().unwrap())
            .unwrap();
        assert_eq!(
            err_of(VolumePathHandler.remove_map_path("")),
            "failed to remove map path. mapPath is empty"
        );
    }

    #[test]
    fn is_symlink_exist_cases() {
        let tmp = tempdir().unwrap();
        let link = tmp.path().join("test-symlink-link");
        symlink("/dev/fakeDevice", &link).unwrap();
        assert!(VolumePathHandler
            .is_symlink_exist(link.to_str().unwrap())
            .unwrap());
        assert!(!VolumePathHandler
            .is_symlink_exist(tmp.path().join("non-existing-symlink").to_str().unwrap())
            .unwrap());
        // A regular file is not a symlink.
        let f = tmp.path().join("file");
        fs::write(&f, b"").unwrap();
        assert!(!VolumePathHandler
            .is_symlink_exist(f.to_str().unwrap())
            .unwrap());
    }
}

#[cfg(test)]
#[path = "volumepathhandler_bind_loop_tests.rs"]
mod bind_mount_and_loop_tests;
