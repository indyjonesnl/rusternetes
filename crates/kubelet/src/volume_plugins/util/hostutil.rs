//! Port of `pkg/volume/util/hostutil` (`hostutil_linux.go`).

use std::io;

/// The slice of `hostutil.HostUtils` (`hostutil.go:47-73`) the unmount-device
/// operation consumes: `DeviceOpened` and `PathIsDevice`. `GetFileType` is the
/// local plugin's. The remaining methods have no consumer in this kubelet yet.
pub trait HostUtils: Send + Sync {
    /// `DeviceOpened`: is the device (e.g. /dev/sdc) in use elsewhere on the
    /// system, i.e. still mounted.
    fn device_opened(&self, pathname: &str) -> io::Result<bool>;

    /// `PathIsDevice`: does the path refer to a device.
    fn path_is_device(&self, pathname: &str) -> io::Result<bool>;

    /// `GetFileType` (`hostutil.go:55`, `hostutil_linux.go:`): the kind of
    /// file at `pathname`; the local plugin reads it through
    /// `VolumeHost.GetHostUtil()` (`local.go:280`, `:406`).
    fn get_file_type(&self, pathname: &str) -> io::Result<FileType>;

    /// `GetSELinuxMountContext` (`hostutil.go:72`, `hostutil_linux.go:301-305`):
    /// the value of the `-o context=` mount option of the mount at `pathname`
    /// (empty when none, or when SELinux is disabled).
    fn get_selinux_mount_context(&self, pathname: &str) -> io::Result<String>;
}

/// `hostutil.HostUtil` (`hostutil_linux.go`).
#[derive(Clone, Copy, Debug, Default)]
pub struct HostUtil;

/// `FileType` (`hostutil.go:24-39`), reduced to the kinds `PathIsDevice`
/// distinguishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileType {
    BlockDev,
    CharDev,
    Directory,
    File,
    Socket,
}

/// `getFileType` (`hostutil.go:80-110`): `os.Stat`, then classify the mode.
pub fn get_file_type(pathname: &str) -> io::Result<FileType> {
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
    fn get_file_type(&self, pathname: &str) -> io::Result<FileType> {
        get_file_type(pathname)
    }

    fn get_selinux_mount_context(&self, pathname: &str) -> io::Result<String> {
        get_selinux_mount_context(
            pathname,
            super::mount_utils::PROC_MOUNT_INFO_PATH,
            crate::go_selinux::get_enabled,
        )
    }

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

/// `getSELinuxMountContext` (`hostutil_linux.go:309-331`): the `context=`
/// super option of the mount `path` lives on, unquoted. The mountinfo parse is
/// skipped (and `""` returned) when SELinux is disabled.
pub fn get_selinux_mount_context(
    path: &str,
    mountinfo_path: &str,
    selinux_enabled: impl Fn() -> bool,
) -> io::Result<String> {
    // Skip /proc/mounts parsing if SELinux is disabled.
    if !selinux_enabled() {
        return Ok(String::new());
    }
    let info = find_mount_info(path, mountinfo_path)?;
    for opt in &info.super_options {
        let Some(context) = opt.strip_prefix("context=") else {
            continue;
        };
        // Remove double quotes
        return Ok(context.trim_matches('"').to_string());
    }
    Ok(String::new())
}

/// `HostUtil.GetSELinuxSupport` (`hostutil_linux.go:263-265`).
pub fn get_selinux_support(pathname: &str) -> io::Result<bool> {
    get_selinux(
        pathname,
        super::mount_utils::PROC_MOUNT_INFO_PATH,
        crate::go_selinux::get_enabled,
    )
}
