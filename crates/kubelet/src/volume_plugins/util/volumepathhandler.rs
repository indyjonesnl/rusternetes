//! Port of `pkg/volume/util/volumepathhandler/volume_path_handler.go`.
//!
//! Upstream maps a block device into a pod by creating, under the pod device
//! map path (`BlockVolume::GetPodDeviceMapPath`), a symlink named after the
//! volume whose target is the host device. The kubelet then hands that device
//! to the runtime as a CRI `Device` (`ContainerConfig.devices`), never as a
//! mount — so this module only prepares the host-side link; the CRI wiring is
//! the consumer's job.
//!
//! **Not ported (tracked in #2293):** the `bindMount == true` mapping and the
//! unmount of a live bind mount (both need `mount(2)`/`umount(2)`; this crate
//! has no mount helper yet), `FindGlobalMapPathUUIDFromPod` /
//! `GetDeviceBindMountRefs` (consumers of bind mounts), and the `losetup`
//! file-device functions (`AttachFileDevice`, `DetachFileDevice`,
//! `GetLoopDevice`) which back `volumeMode: Block` on a file-backed PV.

use anyhow::{bail, Context, Result};
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::{symlink, FileTypeExt};
use std::path::Path;

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
            bail!(
                "bind-mount block device mapping is not supported: no mount helper in this \
                 crate (devicePath {device_path}, linkName {link_name})"
            );
        }
        map_symlink_device(device_path, map_path, link_name)
    }

    /// Port of `UnmapDevice` (`volume_path_handler.go:171-182`).
    pub fn unmap_device(&self, map_path: &str, link_name: &str, bind_mount: bool) -> Result<()> {
        if map_path.is_empty() {
            bail!("failed to unmap device from map path. mapPath is empty");
        }
        if bind_mount {
            return self.unmap_bind_mount_device(map_path, link_name);
        }
        self.unmap_symlink_device(map_path, link_name)
    }

    /// Port of `unmapBindMountDevice` (`volume_path_handler.go:184-214`).
    /// Only the "not mounted" arm is reachable; a live mount is an error
    /// rather than a silent leak (see the module comment).
    fn unmap_bind_mount_device(&self, map_path: &str, link_name: &str) -> Result<()> {
        let link_path = Path::new(map_path).join(link_name);
        let link_path_str = link_path.to_string_lossy();
        if self.is_device_bind_mount_exist(&link_path_str)? {
            bail!(
                "failed to unmount linkPath {link_path_str}: unmounting is not supported: \
                 no mount helper in this crate"
            );
        }
        match fs::metadata(&link_path) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => bail!("failed to check if path {link_path_str} exists: {e}"),
        }
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

    #[test]
    fn bind_mount_mapping_is_an_explicit_error() {
        let tmp = tempdir().unwrap();
        let e =
            err_of(VolumePathHandler.map_device("/dev/x", tmp.path().to_str().unwrap(), "l", true));
        assert!(e.contains("not supported"), "{e}");
    }
}
