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

/// `FileType` (`hostutil.go:24-39`), reduced to the kinds `PathIsDevice`
/// distinguishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileType {
    BlockDev,
    CharDev,
    Directory,
    File,
    Socket,
}

/// `getFileType` (`hostutil.go:80-110`): `os.Stat`, then classify the mode.
fn get_file_type(pathname: &str) -> io::Result<FileType> {
    use std::os::unix::fs::FileTypeExt;
    let info = match std::fs::metadata(pathname) {
        Ok(info) => info,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(io::Error::other(format!(
                "path {pathname:?} does not exist"
            )));
        }
        Err(e) => return Err(e),
    };
    let ft = info.file_type();
    if ft.is_dir() {
        Ok(FileType::Directory)
    } else if ft.is_file() {
        Ok(FileType::File)
    } else if ft.is_socket() {
        Ok(FileType::Socket)
    } else if ft.is_char_device() {
        Ok(FileType::CharDev)
    } else if ft.is_block_device() {
        Ok(FileType::BlockDev)
    } else {
        // `errUnknownFileType` (`hostutil.go:43`).
        Err(io::Error::other(
            "only recognise file, directory, socket, block device and character device",
        ))
    }
}

/// `ExclusiveOpenFailsOnDevice` (`hostutil_linux.go:70-110`): is the block
/// device in use, by opening it with `O_EXCL`.
fn exclusive_open_fails_on_device(pathname: &str) -> io::Result<bool> {
    use std::os::unix::fs::FileTypeExt;
    // err in call to os.Stat
    let finfo = std::fs::metadata(pathname)
        .map_err(|e| io::Error::other(format!("PathIsDevice failed for path {pathname:?}: {e}")))?;
    // path refers to a device
    let ft = finfo.file_type();
    if !(ft.is_block_device() || ft.is_char_device()) {
        tracing::error!("Path {pathname:?} is not referring to a device.");
        return Ok(false);
    }
    let c = std::ffi::CString::new(pathname)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    // SAFETY: `c` is a valid NUL-terminated string.
    let fd = unsafe { libc::open(c.as_ptr(), libc::O_RDONLY | libc::O_EXCL | libc::O_CLOEXEC) };
    if fd >= 0 {
        // device not in use
        // SAFETY: `fd` is the descriptor just opened.
        unsafe { libc::close(fd) };
        return Ok(false);
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EBUSY) {
        // device is in use
        return Ok(true);
    }
    // error during call to Open
    Err(err)
}

impl HostUtils for HostUtil {
    /// `HostUtil.DeviceOpened` (`hostutil_linux.go:57-59`).
    fn device_opened(&self, pathname: &str) -> io::Result<bool> {
        exclusive_open_fails_on_device(pathname)
    }

    /// `HostUtil.PathIsDevice` (`hostutil_linux.go:63-67`).
    fn path_is_device(&self, pathname: &str) -> io::Result<bool> {
        let t = get_file_type(pathname)?;
        Ok(matches!(t, FileType::CharDev | FileType::BlockDev))
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

/// `findMountInfo` (`hostutil_linux.go:183-202`): walk `mountinfo_path`
/// backwards and take the first mount point that is a prefix of `path`.
fn find_mount_info(path: &str, mountinfo_path: &str) -> io::Result<super::mount_utils::MountInfo> {
    let infos = super::mount_utils::parse_mount_info(mountinfo_path)?;
    infos
        .into_iter()
        .rev()
        .find(|i| super::mount_utils::path_within_base(path, &i.mount_point))
        .ok_or_else(|| io::Error::other(format!("cannot find mount point for {path:?}")))
}

/// `GetSELinux` (`hostutil_linux.go:236-259`): does the mount `path` lives on
/// carry `seclabel` (in the mount or the super options)? `selinux_enabled`
/// is `selinux.GetEnabled`; the mountinfo parse is skipped when it is false.
pub fn get_selinux(
    path: &str,
    mountinfo_path: &str,
    selinux_enabled: impl Fn() -> bool,
) -> io::Result<bool> {
    if !selinux_enabled() {
        return Ok(false);
    }
    let info = find_mount_info(path, mountinfo_path)?;
    Ok(info
        .super_options
        .iter()
        .chain(info.mount_options.iter())
        .any(|o| o == "seclabel"))
}

/// `HostUtil.GetSELinuxSupport` (`hostutil_linux.go:263-265`).
pub fn get_selinux_support(pathname: &str) -> io::Result<bool> {
    get_selinux(
        pathname,
        super::mount_utils::PROC_MOUNT_INFO_PATH,
        crate::go_selinux::get_enabled,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    /// `TestGetSELinuxSupport` (`hostutil_linux_test.go:137`).
    #[test]
    fn get_selinux_support_cases() {
        let info = "62 0 253:0 / / rw,relatime shared:1 - ext4 /dev/mapper/ssd-root rw,seclabel,data=ordered
78 62 0:41 / /tmp rw,nosuid,nodev shared:30 - tmpfs tmpfs rw,seclabel
83 63 0:44 / /var/lib/bar rw,relatime - tmpfs tmpfs rw
227 62 253:0 /var/lib/docker/devicemapper /var/lib/docker/devicemapper rw,relatime - ext4 /dev/mapper/ssd-root rw,seclabel,data=ordered
150 23 1:58 / /media/nfs_vol rw,relatime shared:89 - nfs4 172.18.4.223:/srv/nfs rw,vers=4.0,rsize=524288,wsize=524288,namlen=255,hard,proto=tcp,port=0,timeo=600,retrans=2,sec=sys,clientaddr=172.18.4.223,local_lock=none,addr=172.18.4.223
";
        let d = tempfile::tempdir().unwrap();
        let f = d.path().join("mountinfo");
        std::fs::write(&f, info).unwrap();
        let f = f.to_str().unwrap();
        for (name, mp, enabled, want) in [
            ("ext4 on / with disabled SELinux", "/", false, false),
            ("ext4 on /", "/", true, true),
            ("tmpfs on /var/lib/bar", "/var/lib/bar", true, false),
            ("nfsv4", "/media/nfs_vol", true, false),
        ] {
            assert_eq!(get_selinux(mp, f, || enabled).unwrap(), want, "{name}");
        }
    }

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
