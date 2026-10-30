//! Port of the parts of `staging/src/k8s.io/mount-utils` that a plugin needs
//! to be handed a *swappable* mounter and to format a block device:
//!
//! - `mount.Interface` (`mount.go:39-89`), restricted to the methods the
//!   iscsi plugin calls, and `mount.FakeMounter` (`fake_mounter.go`) for tests;
//! - `MountError` / `MountErrorType` (`mount.go:112-142`);
//! - `SafeFormatAndMount.FormatAndMount*` (`mount.go:189-208`) and its Linux
//!   body `formatAndMountSensitive` (`mount_linux.go:554-642`), `format`
//!   (`:645-666`), `getDiskFormat` (`:668-`), `checkAndRepairFilesystem`
//!   (`:531-552`).
//!
//! The concrete [`Mounter`] already ports `mount_linux.go`'s mount path; the
//! trait lets a plugin test substitute `FakeMounter` exactly as upstream's
//! `doTestPlugin` does.
//!
//! `SafeFormatAndMount.formatSem` / `formatTimeout` (`mount.go:151-152`) are
//! [`FormatLimit`], enabled by `WithMaxConcurrentFormat` (`mount.go:174-181`);
//! the kubelet never passes it to the iscsi plugin (`&mount.SafeFormatAndMount{
//! Interface: mounter, Exec: exec}`, `iscsi.go:134`, `attacher.go:132`), so
//! by default it is unset, as upstream.

use super::exec::{Exec, ExecError};
use super::mount_ops::{sanitized_options_for_logging, Mounter};
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tracing::{error, info, warn};

/// `mount.Interface` (`mount.go:39-89`) restricted to what the volume plugins
/// in this crate call.
pub trait MountInterface: Send + Sync {
    /// `MountSensitive` (`mount.go:48`).
    fn mount_sensitive(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
    ) -> io::Result<()>;
    /// `MountSensitiveWithoutSystemd` (`mount.go:50`).
    fn mount_sensitive_without_systemd(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
    ) -> io::Result<()>;
    /// `Unmount` (`mount.go:54`).
    fn unmount(&self, target: &str) -> io::Result<()>;
    /// `IsLikelyNotMountPoint` (`mount.go:67`).
    fn is_likely_not_mount_point(&self, file: &str) -> io::Result<bool>;
    /// `CanSafelySkipMountPointCheck` (`mount.go:71`).
    fn can_safely_skip_mount_point_check(&self) -> bool;
    /// `GetMountRefs` (`mount.go:88`).
    fn get_mount_refs(&self, pathname: &str) -> io::Result<Vec<String>>;
    /// `List` (`mount.go:59`): all mounted filesystems.
    fn list(&self) -> io::Result<Vec<super::mount_list::MountPoint>>;
}

impl MountInterface for Mounter {
    fn mount_sensitive(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
    ) -> io::Result<()> {
        Mounter::mount_sensitive(self, source, target, fstype, options, sensitive_options)
    }

    fn mount_sensitive_without_systemd(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
    ) -> io::Result<()> {
        Mounter::mount_sensitive_without_systemd(
            self,
            source,
            target,
            fstype,
            options,
            sensitive_options,
            &[],
        )
    }

    fn unmount(&self, target: &str) -> io::Result<()> {
        Mounter::unmount(self, target)
    }

    fn is_likely_not_mount_point(&self, file: &str) -> io::Result<bool> {
        Mounter::is_likely_not_mount_point(self, file)
    }

    fn can_safely_skip_mount_point_check(&self) -> bool {
        Mounter::can_safely_skip_mount_point_check(self)
    }

    fn get_mount_refs(&self, pathname: &str) -> io::Result<Vec<String>> {
        super::mount_utils::get_mount_refs(pathname)
    }

    /// `Mounter.List` (`mount_linux.go:426-428`).
    fn list(&self) -> io::Result<Vec<super::mount_list::MountPoint>> {
        super::mount_list::list_proc_mounts(super::mount_list::PROC_MOUNTS_PATH)
    }
}

/// `MountErrorType` (`mount.go:112-121`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MountErrorType {
    FilesystemMismatch,
    HasFilesystemErrors,
    UnformattedReadOnly,
    FormatFailed,
    GetDiskFormatFailed,
    UnknownMountError,
}

/// `MountError` (`mount.go:123-134`): `Error()` is the bare message.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct MountError {
    pub kind: MountErrorType,
    pub message: String,
}

impl MountError {
    /// `NewMountError` (`mount.go:136-142`).
    pub fn new(kind: MountErrorType, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

/// `fsckErrorsCorrected` (`mount_linux.go:50`).
const FSCK_ERRORS_CORRECTED: i32 = 1;
/// `fsckErrorsUncorrected` (`mount_linux.go:52`).
const FSCK_ERRORS_UNCORRECTED: i32 = 4;

/// `formatSem` + `formatTimeout` (`mount.go:151-152`): a counting semaphore
/// (`chan any` of capacity `n`) and the per-format token hold time. `None` is
/// a nil `formatSem` (unlimited).
#[derive(Clone, Default)]
pub struct FormatLimit(Option<FormatLimitInner>);

#[derive(Clone)]
struct FormatLimitInner {
    /// `cap(formatSem)`.
    capacity: usize,
    /// Tokens currently held (`len(formatSem)`).
    held: Arc<(Mutex<usize>, Condvar)>,
    /// `formatTimeout`.
    timeout: Duration,
}

impl FormatLimit {
    /// `WithMaxConcurrentFormat` (`mount.go:174-181`): `n < 1` is unlimited.
    pub fn with_max_concurrent_format(n: i64, timeout: Duration) -> Self {
        if n > 0 {
            Self(Some(FormatLimitInner {
                capacity: n as usize,
                held: Arc::new((Mutex::new(0), Condvar::new())),
                timeout,
            }))
        } else {
            Self(None)
        }
    }

    /// `formatSem != nil`.
    pub fn has_sem(&self) -> bool {
        self.0.is_some()
    }

    /// `cap(formatSem)`.
    pub fn capacity(&self) -> Option<usize> {
        self.0.as_ref().map(|i| i.capacity)
    }

    /// `formatTimeout`.
    pub fn timeout(&self) -> Option<Duration> {
        self.0.as_ref().map(|i| i.timeout)
    }
}

/// `mount.SafeFormatAndMount` (`mount.go:147-153`): `Interface` plus `Exec`,
/// and the optional `formatSem`/`formatTimeout`.
#[derive(Clone)]
pub struct SafeFormatAndMount {
    pub mounter: Arc<dyn MountInterface>,
    pub exec: Arc<dyn Exec>,
    pub format_limit: FormatLimit,
}

impl SafeFormatAndMount {
    /// `NewSafeFormatAndMount` (`mount.go:155-165`) without options.
    pub fn new(mounter: Arc<dyn MountInterface>, exec: Arc<dyn Exec>) -> Self {
        Self {
            mounter,
            exec,
            format_limit: FormatLimit::default(),
        }
    }

    /// `NewSafeFormatAndMount(.., WithMaxConcurrentFormat(n, timeout))`.
    pub fn with_max_concurrent_format(mut self, n: i64, timeout: Duration) -> Self {
        self.format_limit = FormatLimit::with_max_concurrent_format(n, timeout);
        self
    }

    /// `FormatAndMount` (`mount.go:189-191`).
    pub fn format_and_mount(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
    ) -> Result<(), MountError> {
        self.format_and_mount_sensitive(source, target, fstype, options, &[])
    }

    /// `FormatAndMountSensitive` (`mount.go:198-200`).
    pub fn format_and_mount_sensitive(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
    ) -> Result<(), MountError> {
        self.format_and_mount_sensitive_with_format_options(
            source,
            target,
            fstype,
            options,
            sensitive_options,
            &[],
        )
    }

    /// `FormatAndMountSensitiveWithFormatOptions` (`mount.go:206-208`).
    pub fn format_and_mount_sensitive_with_format_options(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
        format_options: &[String],
    ) -> Result<(), MountError> {
        self.format_and_mount_sensitive_impl(
            source,
            target,
            fstype,
            options,
            sensitive_options,
            format_options,
        )
    }

    /// `formatAndMountSensitive` (`mount_linux.go:554-642`).
    fn format_and_mount_sensitive_impl(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
        format_options: &[String],
    ) -> Result<(), MountError> {
        let mut read_only = options.iter().any(|o| o == "ro");
        if !read_only {
            // Check sensitiveOptions for ro
            read_only = sensitive_options.iter().any(|o| o == "ro");
        }

        let mut options = options.to_vec();
        options.push("defaults".to_string());
        let mut mount_error_value = MountErrorType::UnknownMountError;

        // Check if the disk is already formatted
        let existing_format = self.get_disk_format(source).map_err(|e| {
            MountError::new(
                MountErrorType::GetDiskFormatFailed,
                format!("failed to get disk format of disk {source}: {e}"),
            )
        })?;

        // Use 'ext4' as the default
        let fstype = if fstype.is_empty() { "ext4" } else { fstype };

        if existing_format.is_empty() {
            // Do not attempt to format the disk if mounting as readonly, return an error to reflect this.
            if read_only {
                return Err(MountError::new(
                    MountErrorType::UnformattedReadOnly,
                    format!(
                        "cannot mount unformatted disk {source} as we are manipulating it in read-only mode"
                    ),
                ));
            }

            // Disk is unformatted so format it.
            let mut args: Vec<String> = vec![source.to_string()];
            if fstype == "ext4" || fstype == "ext3" {
                args = vec![
                    "-F".to_string(),  // Force flag
                    "-m0".to_string(), // Zero blocks reserved for super-user
                    source.to_string(),
                ];
            } else if fstype == "xfs" {
                args = vec![
                    "-f".to_string(), // force flag
                    source.to_string(),
                ];
            }
            let mut all = format_options.to_vec();
            all.extend(args);
            let args = all;

            info!(
                "Disk {source:?} appears to be unformatted, attempting to format as type: {fstype:?} with options: {args:?}"
            );

            let (output, res) = self.format(fstype, &args);
            if let Err(err) = res {
                // Do not log sensitiveOptions only options
                let sensitive_options_log =
                    sanitized_options_for_logging(&options, sensitive_options);
                let detailed = format!(
                    "format of disk {source:?} failed: type:({fstype:?}) target:({target:?}) options:({sensitive_options_log:?}) errcode:({err}) output:({output}) "
                );
                error!("{detailed}");
                return Err(MountError::new(MountErrorType::FormatFailed, detailed));
            }

            info!("Disk successfully formatted (mkfs): {fstype} - {source} {target}");
        } else {
            if fstype != existing_format {
                // Verify that the disk is formatted with filesystem type we are expecting
                mount_error_value = MountErrorType::FilesystemMismatch;
                warn!(
                    "Configured to mount disk {source} as {fstype} but current format is {existing_format}, things might break"
                );
            }

            if !read_only {
                // Run check tools on the disk to fix repairable issues, only do this for formatted volumes requested as rw.
                self.check_and_repair_filesystem(source)?;
            }
        }

        // Mount the disk
        tracing::debug!("Attempting to mount disk {source} in {fstype} format at {target}");
        self.mounter
            .mount_sensitive(source, target, fstype, &options, sensitive_options)
            .map_err(|e| MountError::new(mount_error_value, e.to_string()))
    }

    /// `format` (`mount_linux.go:645-666`). With a `formatSem` it acquires a
    /// token (`mounter.formatSem <- struct{}{}`), and a goroutine releases it
    /// when the command returns (`<-done`) or `formatTimeout` elapses,
    /// whichever is first; the command itself is allowed to complete.
    pub fn format(&self, fstype: &str, args: &[String]) -> (String, Result<(), ExecError>) {
        // `done` is closed on return (`defer close(done)`): dropping the
        // sender wakes the releaser thread.
        let _done = self.format_limit.0.as_ref().map(|limit| {
            let (lock, cv) = &*limit.held;
            let mut held = lock.lock().unwrap();
            while *held >= limit.capacity {
                held = cv.wait(held).unwrap();
            }
            *held += 1;
            drop(held);

            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            let sem = limit.held.clone();
            let timeout = limit.timeout;
            std::thread::spawn(move || {
                // select { case <-done: case <-timeout.C: }
                let _ = done_rx.recv_timeout(timeout);
                let (lock, cv) = &*sem;
                *lock.lock().unwrap() -= 1;
                cv.notify_one();
            });
            done_tx
        });
        self.exec.combined_output(&format!("mkfs.{fstype}"), args)
    }

    /// `GetDiskFormat` (`mount_linux.go:721-723`).
    pub fn get_disk_format(&self, disk: &str) -> Result<String, String> {
        get_disk_format(self.exec.as_ref(), disk)
    }

    /// `checkAndRepairFilesystem` (`mount_linux.go:531-552`).
    fn check_and_repair_filesystem(&self, source: &str) -> Result<(), MountError> {
        tracing::debug!("Checking for issues with fsck on disk: {source}");
        let args = vec!["-a".to_string(), source.to_string()];
        let (out, res) = self.exec.combined_output("fsck", &args);
        if let Err(err) = res {
            match &err {
                ExecError::NotFound => {
                    warn!("'fsck' not found on system; continuing mount without running 'fsck'.")
                }
                e if e.exit_status() == Some(FSCK_ERRORS_CORRECTED) => {
                    info!("Device {source} has errors which were corrected by fsck.")
                }
                e if e.exit_status() == Some(FSCK_ERRORS_UNCORRECTED) => {
                    return Err(MountError::new(
                        MountErrorType::HasFilesystemErrors,
                        format!(
                            "'fsck' found errors on device {source} but could not correct them: {out}"
                        ),
                    ));
                }
                e if e.exit_status().is_some_and(|c| c > FSCK_ERRORS_UNCORRECTED) => {
                    info!("`fsck` error {out}")
                }
                _ => warn!("fsck on device {source} failed with error {err}, output: {out}"),
            }
        }
        Ok(())
    }
}

/// `getDiskFormat` (`mount_linux.go:668-719`).
pub fn get_disk_format(exec: &dyn Exec, disk: &str) -> Result<String, String> {
    let args: Vec<String> = ["-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", disk]
        .iter()
        .map(|s| s.to_string())
        .collect();
    tracing::debug!(
        "Attempting to determine if disk {disk:?} is formatted using blkid with args: ({args:?})"
    );
    let (output, res) = exec.combined_output("blkid", &args);
    tracing::debug!("Output: {output:?}");

    if let Err(err) = res {
        if err.exit_status() == Some(2) {
            // Disk device is unformatted.
            // For `blkid`, if the specified token (TYPE/PTTYPE, etc) was
            // not found, or no (specified) devices could be identified, an
            // exit code of 2 is returned.
            return Ok(String::new());
        }
        error!("Could not determine if disk {disk:?} is formatted ({err})");
        return Err(err.to_string());
    }

    let mut fstype = String::new();
    let mut pttype = String::new();
    for l in output.split('\n') {
        if l.is_empty() {
            // Ignore empty line.
            continue;
        }
        let cs: Vec<&str> = l.split('=').collect();
        if cs.len() != 2 {
            return Err(format!("blkid returns invalid output: {output}"));
        }
        // TYPE is filesystem type, and PTTYPE is partition table type, according
        // to https://www.kernel.org/pub/linux/utils/util-linux/v2.21/libblkid-docs/.
        if cs[0] == "TYPE" {
            fstype = cs[1].to_string();
        } else if cs[0] == "PTTYPE" {
            pttype = cs[1].to_string();
        }
    }

    if !pttype.is_empty() {
        tracing::debug!("Disk {disk} detected partition table type: {pttype}");
        // Returns a special non-empty string as filesystem type, then kubelet
        // will not format it.
        return Ok("unknown data, probably partitions".to_string());
    }

    Ok(fstype)
}

pub mod testing {
    //! `mount.FakeMounter` (`fake_mounter.go`).
    use super::*;
    use std::sync::Mutex;

    /// `mount.MountPoint`.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct MountPoint {
        pub device: String,
        pub path: String,
        pub fstype: String,
        pub opts: Vec<String>,
    }

    #[derive(Default)]
    pub struct FakeMounter {
        pub mount_points: Mutex<Vec<MountPoint>>,
        /// `ErrorMounter` (`safe_format_and_mount_test.go:30-47`): the error
        /// to return from the n-th `MountSensitive` call, if any.
        pub mount_errs: Mutex<Vec<Option<String>>>,
        pub mount_calls: Mutex<usize>,
    }

    impl FakeMounter {
        pub fn new() -> Self {
            Self::default()
        }
    }

    fn eval(path: &str) -> String {
        std::fs::canonicalize(path)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| path.to_string())
    }

    impl MountInterface for FakeMounter {
        /// `FakeMounter.MountSensitive` (`fake_mounter.go:99-139`).
        fn mount_sensitive(
            &self,
            source: &str,
            target: &str,
            fstype: &str,
            options: &[String],
            sensitive_options: &[String],
        ) -> io::Result<()> {
            {
                let mut n = self.mount_calls.lock().unwrap();
                let i = *n;
                *n += 1;
                if let Some(Some(e)) = self.mount_errs.lock().unwrap().get(i) {
                    return Err(io::Error::other(e.clone()));
                }
            }
            let mut mps = self.mount_points.lock().unwrap();
            let mut source = source.to_string();
            let mut opts = Vec::new();
            for option in options {
                if option == "bind" {
                    // use the original device of the bind-mount as the real source
                    if let Some(m) = mps.iter().find(|m| m.path == source) {
                        source = m.device.clone();
                    }
                }
                opts.push(option.clone());
            }
            opts.extend(sensitive_options.iter().cloned());
            let abs_target = eval(target);
            mps.push(MountPoint {
                device: source,
                path: abs_target,
                fstype: fstype.to_string(),
                opts,
            });
            Ok(())
        }

        /// `fake_mounter.go:141-143`: sensitive options dropped.
        fn mount_sensitive_without_systemd(
            &self,
            source: &str,
            target: &str,
            fstype: &str,
            options: &[String],
            _sensitive_options: &[String],
        ) -> io::Result<()> {
            self.mount_sensitive(source, target, fstype, options, &[])
        }

        /// `FakeMounter.Unmount` (`fake_mounter.go:150-179`).
        fn unmount(&self, target: &str) -> io::Result<()> {
            let abs_target = eval(target);
            self.mount_points
                .lock()
                .unwrap()
                .retain(|mp| mp.path != abs_target);
            Ok(())
        }

        /// `FakeMounter.IsLikelyNotMountPoint` (`fake_mounter.go:191-219`).
        fn is_likely_not_mount_point(&self, file: &str) -> io::Result<bool> {
            std::fs::metadata(file)?;
            let abs = eval(file);
            Ok(!self
                .mount_points
                .lock()
                .unwrap()
                .iter()
                .any(|m| m.path == abs))
        }

        fn can_safely_skip_mount_point_check(&self) -> bool {
            false
        }

        /// `FakeMounter.GetMountRefs` -> `getMountRefsByDev`
        /// (`fake_mounter.go:235-242`, `mount.go:210-238`): other mount points of the same device.
        fn get_mount_refs(&self, pathname: &str) -> io::Result<Vec<String>> {
            let real = eval(pathname);
            let mps = self.mount_points.lock().unwrap();
            let Some(dev) = mps
                .iter()
                .find(|m| m.path == real)
                .map(|m| m.device.clone())
            else {
                return Ok(vec![]);
            };
            Ok(mps
                .iter()
                .filter(|m| m.device == dev && m.path != real)
                .map(|m| m.path.clone())
                .collect())
        }

        /// `FakeMounter.List` (`fake_mounter.go:181-187`): the in-memory mount points.
        fn list(&self) -> io::Result<Vec<crate::volume_plugins::util::mount_list::MountPoint>> {
            Ok(self
                .mount_points
                .lock()
                .unwrap()
                .iter()
                .map(|m| crate::volume_plugins::util::mount_list::MountPoint {
                    device: m.device.clone(),
                    path: m.path.clone(),
                    fstype: m.fstype.clone(),
                    opts: m.opts.clone(),
                    freq: 0,
                    pass: 0,
                })
                .collect())
        }
    }
}
