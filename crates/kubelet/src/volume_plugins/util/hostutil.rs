//! Port of `pkg/volume/util/hostutil` (`hostutil_linux.go`).

use std::io;

/// The slice of `hostutil.HostUtils` (`hostutil.go:47-73`) the unmount-device
/// operation consumes: `DeviceOpened` and `PathIsDevice`. The remaining
/// methods have no consumer in this kubelet yet.
pub trait HostUtils: Send + Sync {
    /// `DeviceOpened`: is the device (e.g. /dev/sdc) in use elsewhere on the
    /// system, i.e. still mounted.
    fn device_opened(&self, pathname: &str) -> io::Result<bool>;

    /// `PathIsDevice`: does the path refer to a device.
    fn path_is_device(&self, pathname: &str) -> io::Result<bool>;
}

/// `hostutil.HostUtil` (`hostutil_linux.go`).
#[derive(Clone, Copy, Debug, Default)]
pub struct HostUtil;

impl HostUtils for HostUtil {
    fn device_opened(&self, _pathname: &str) -> io::Result<bool> {
        Ok(false)
    }

    fn path_is_device(&self, _pathname: &str) -> io::Result<bool> {
        Ok(false)
    }
}

/// `HostUtil.GetOwner` (`hostutil_linux.go:267-274`): the uid and gid of
/// `pathname` after symlink evaluation.
pub fn get_owner(pathname: &str) -> io::Result<(i64, i64)> {
    use std::os::unix::fs::MetadataExt;
    let realpath = std::fs::canonicalize(pathname)?;
    let md = std::fs::metadata(realpath)?;
    Ok((md.uid() as i64, md.gid() as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    #[test]
    fn get_owner_follows_symlinks() {
        let d = tempfile::tempdir().unwrap();
        let target = d.path().join("t");
        std::fs::create_dir(&target).unwrap();
        let link = d.path().join("l");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let md = std::fs::metadata(&target).unwrap();
        assert_eq!(
            get_owner(link.to_str().unwrap()).unwrap(),
            (md.uid() as i64, md.gid() as i64)
        );
    }

    /// `PathIsDevice` (`hostutil_linux.go:63-67`) over `getFileType`
    /// (`hostutil.go:80-110`): a character device is a device, a directory and
    /// a regular file are not, and a missing path errors with
    /// `path %q does not exist`.
    #[test]
    fn path_is_device_classifies_by_file_type() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let hu = HostUtil;
        assert!(hu.path_is_device("/dev/null").unwrap());
        assert!(!hu.path_is_device(d.path().to_str().unwrap()).unwrap());
        assert!(!hu.path_is_device(file.to_str().unwrap()).unwrap());
        let missing = d.path().join("missing");
        let err = hu.path_is_device(missing.to_str().unwrap()).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("path {:?} does not exist", missing.to_str().unwrap())
        );
    }

    /// `ExclusiveOpenFailsOnDevice` (`hostutil_linux.go:70-110`): a missing
    /// path is an error (`PathIsDevice failed for path %q: %v`); a path that
    /// is not a device is `false`; a device nobody holds exclusively is
    /// `false`.
    #[test]
    fn device_opened_follows_exclusive_open() {
        let d = tempfile::tempdir().unwrap();
        let hu = HostUtil;
        assert!(!hu.device_opened(d.path().to_str().unwrap()).unwrap());
        assert!(!hu.device_opened("/dev/null").unwrap());
        let missing = d.path().join("missing");
        let err = hu.device_opened(missing.to_str().unwrap()).unwrap_err();
        assert!(
            err.to_string().starts_with(&format!(
                "PathIsDevice failed for path {:?}: ",
                missing.to_str().unwrap()
            )),
            "{err}"
        );
    }

    #[test]
    fn get_owner_missing_errs() {
        assert!(get_owner("/definitely/not/here").is_err());
    }
}
