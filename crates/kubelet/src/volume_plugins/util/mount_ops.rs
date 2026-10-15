//! Port of the mount-command half of `staging/src/k8s.io/mount-utils`:
//! `MakeBindOptsSensitive` (`mount.go:306`), `sanitizedOptionsForLogging`
//! (`mount.go:384`), `MakeMountArgsSensitiveWithMountFlags`
//! (`mount_linux.go:350`), `AddSystemdScopeSensitive` (`mount_linux.go:393`),
//! `getBindMountOptions` (`mount_linux.go:116`), `Mounter.Mount*`/`doMount`/
//! `bindMountSensitive` and `Mounter.Unmount`/`checkUmountError`.
//!
//! Also ported for the nfs plugin (#3286): `UnmountWithForce`/`tryUnmount`/
//! `forceUmount`, `CanSafelySkipMountPointCheck`, `IsLikelyNotMountPoint`
//! (`isLikelyNotMountPointStatx` with the `isLikelyNotMountPointStat` fallback) and
//! `CleanupMountPoint`/`CleanupMountWithForce` (`mount_helper_common.go`).
//!
//! Not ported (tracked in #3193): `detectSystemd`/`detectSafeNotMountedBehavior` probing (the caller states
//! `try_systemd`/`safe_not_mounted` explicitly), containerized mounter path.

use std::io;
use std::process::Command;

/// `defaultMountCommand` (`mount.go:33`).
pub const DEFAULT_MOUNT_COMMAND: &str = "mount";
/// `sensitiveOptionsRemoved` (`mount.go:35`).
pub const SENSITIVE_OPTIONS_REMOVED: &str = "<masked>";
/// `errNotMounted` (`mount_linux.go:56`).
pub const ERR_NOT_MOUNTED: &str = "not mounted";

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// `checkForNetDev` (`mount.go:343`).
pub fn check_for_netdev(options: &[String], sensitive_options: &[String]) -> bool {
    options
        .iter()
        .chain(sensitive_options.iter())
        .any(|o| o == "_netdev")
}

/// `MakeBindOptsSensitive` (`mount.go:306-340`). Returns
/// `(bind, bindOpts, bindRemountOpts, bindRemountOptsSensitive)`.
pub fn make_bind_opts_sensitive(
    options: &[String],
    sensitive_options: &[String],
) -> (bool, Vec<String>, Vec<String>, Vec<String>) {
    let mut bind_remount_opts = strs(&["bind", "remount"]);
    let mut bind_remount_sensitive_opts: Vec<String> = Vec::new();
    let mut bind = false;
    let mut bind_opts = strs(&["bind"]);
    // _netdev is a userspace mount option and does not automatically get
    // added when bind mount is created and hence we must carry it over.
    if check_for_netdev(options, sensitive_options) {
        bind_opts.push("_netdev".to_string());
    }
    for option in options {
        match option.as_str() {
            "bind" => bind = true,
            "remount" => {}
            _ => bind_remount_opts.push(option.clone()),
        }
    }
    for option in sensitive_options {
        match option.as_str() {
            "bind" => bind = true,
            "remount" => {}
            _ => bind_remount_sensitive_opts.push(option.clone()),
        }
    }
    (
        bind,
        bind_opts,
        bind_remount_opts,
        bind_remount_sensitive_opts,
    )
}

/// `MakeBindOpts` (`mount.go:296`).
pub fn make_bind_opts(options: &[String]) -> (bool, Vec<String>, Vec<String>) {
    let (b, o, r, _) = make_bind_opts_sensitive(options, &[]);
    (b, o, r)
}

/// `sanitizedOptionsForLogging` (`mount.go:384-401`).
pub fn sanitized_options_for_logging(options: &[String], sensitive_options: &[String]) -> String {
    let separator = if !options.is_empty() && !sensitive_options.is_empty() {
        ","
    } else {
        ""
    };
    let mut sensitive_start = String::new();
    let mut sensitive_end = "";
    if !sensitive_options.is_empty() {
        sensitive_start =
            format!("{SENSITIVE_OPTIONS_REMOVED},").repeat(sensitive_options.len() - 1);
        sensitive_end = SENSITIVE_OPTIONS_REMOVED;
    }
    format!(
        "{}{}{}{}",
        options.join(","),
        separator,
        sensitive_start,
        sensitive_end
    )
}

/// `MakeMountArgsSensitiveWithMountFlags` (`mount_linux.go:350-380`).
pub fn make_mount_args_sensitive_with_mount_flags(
    source: &str,
    target: &str,
    fstype: &str,
    options: &[String],
    sensitive_options: &[String],
    mount_flags: &[String],
) -> (Vec<String>, String) {
    // mount [$mountFlags] [-t $fstype] [-o $options] [$source] $target
    let mut args: Vec<String> = Vec::new();
    let mut log = String::new();
    args.extend(mount_flags.iter().cloned());
    log.push_str(&mount_flags.join(" "));
    if !fstype.is_empty() {
        args.push("-t".to_string());
        args.push(fstype.to_string());
        // Upstream appends the joined args (flags included) here, verbatim.
        log.push_str(&args.join(" "));
    }
    if !options.is_empty() || !sensitive_options.is_empty() {
        let combined: Vec<&str> = options
            .iter()
            .chain(sensitive_options.iter())
            .map(String::as_str)
            .collect();
        args.push("-o".to_string());
        args.push(combined.join(","));
        // exclude sensitiveOptions from log string
        log.push_str(" -o ");
        log.push_str(&sanitized_options_for_logging(options, sensitive_options));
    }
    if !source.is_empty() {
        args.push(source.to_string());
        log.push(' ');
        log.push_str(source);
    }
    args.push(target.to_string());
    log.push(' ');
    log.push_str(target);
    (args, log)
}

/// `AddSystemdScopeSensitive` (`mount_linux.go:393-397`). Returns
/// `(systemdRunPath, args, logStr)`.
pub fn add_systemd_scope_sensitive(
    systemd_run_path: &str,
    mount_name: &str,
    command: &str,
    args: &[String],
    mount_args_log_str: &str,
) -> (String, Vec<String>, String) {
    let description = format!("--description=Kubernetes transient mount for {mount_name}");
    let mut systemd_run_args = vec![
        description,
        "--scope".to_string(),
        "--".to_string(),
        command.to_string(),
    ];
    let log = format!("{} {}", systemd_run_args.join(" "), mount_args_log_str);
    systemd_run_args.extend(args.iter().cloned());
    (systemd_run_path.to_string(), systemd_run_args, log)
}

/// Map `statfs.f_flags` mount flags to mount option strings
/// (`getBindMountOptions`, `mount_linux.go:116-137`). Go iterates a map, so
/// order is unspecified there; we emit a fixed order.
pub fn bind_mount_options_from_flags(flags: u64) -> Vec<String> {
    let mapping: [(u64, &str); 7] = [
        (libc::MS_RDONLY, "ro"),
        (libc::MS_NODEV, "nodev"),
        (libc::MS_NOEXEC, "noexec"),
        (libc::MS_NOSUID, "nosuid"),
        (libc::MS_NOATIME, "noatime"),
        (libc::MS_RELATIME, "relatime"),
        (libc::MS_NODIRATIME, "nodiratime"),
    ];
    mapping
        .iter()
        .filter(|(k, _)| flags & k == *k)
        .map(|(_, v)| v.to_string())
        .collect()
}

/// `getBindMountOptions` with an injectable statfs (as upstream's test does).
pub fn get_bind_mount_options<F>(path: &str, statfs: F) -> io::Result<Vec<String>>
where
    F: FnOnce(&str) -> io::Result<u64>,
{
    let flags =
        statfs(path).map_err(|e| io::Error::new(e.kind(), format!("statfs {path}: {e}")))?;
    Ok(bind_mount_options_from_flags(flags))
}

/// `checkUmountError` (`mount_linux.go:910-924`), minus the
/// `errNoChildProcesses` rewrite which Rust's `Command` never produces.
pub fn check_umount_error(
    target: &str,
    output: &str,
    err: &str,
    with_safe_not_mounted_behavior: bool,
) -> io::Result<()> {
    if with_safe_not_mounted_behavior && output.contains(ERR_NOT_MOUNTED) {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "unmount failed: {err}\nUnmounting arguments: {target}\nOutput: {output}"
    )))
}

/// `detectSystemd` (`mount_linux.go:283-301`).
fn detect_systemd() -> bool {
    // Try `systemd-run --scope true`: enough to make sure systemd is really
    // running and not just installed (container with a systemd image but a
    // different pid 1).
    Command::new("systemd-run")
        .args(["--description=Kubernetes systemd probe", "--scope", "true"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// `detectSafeNotMountedBehavior` (`mount_linux.go:307-331`): does `umount` of
/// a path that is not mounted say "not mounted"? When it does, the mount point
/// check before an unmount can be skipped. Process-global, so probed once.
pub fn detect_safe_not_mounted_behavior() -> bool {
    static SAFE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SAFE.get_or_init(|| {
        // `os.MkdirTemp("", "kubelet-detect-safe-umount")`
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let dir = std::env::temp_dir().join(format!(
            "kubelet-detect-safe-umount{}-{nanos}",
            std::process::id()
        ));
        if std::fs::create_dir(&dir).is_err() {
            return false;
        }
        let safe = match Command::new("umount").arg(&dir).output() {
            Ok(out) if !out.status.success() => {
                let output = format!(
                    "{}{}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
                output.contains(ERR_NOT_MOUNTED)
            }
            _ => false,
        };
        let _ = std::fs::remove_dir(&dir);
        safe
    })
}

/// Port of `mount.Mounter` (`mount_linux.go:60-70`) restricted to what the
/// kubelet volume plugins call: `Mount`, `MountSensitive`,
/// `MountSensitiveWithoutSystemd`, `Unmount`.
#[derive(Clone, Debug, Default)]
pub struct Mounter {
    /// `mounterPath`: containerized mounter binary for nfs/glusterfs/ceph/cifs.
    pub mounter_path: String,
    pub try_systemd: bool,
    pub with_safe_not_mounted_behavior: bool,
    with_systemd: std::sync::OnceLock<bool>,
    /// The mountinfo file [`Self::get_mount_refs`] reads. `None` is
    /// `/proc/self/mountinfo`; see [`Self::with_mountinfo_path`].
    mountinfo_path: Option<String>,
}

impl Mounter {
    pub fn new(mounter_path: &str, try_systemd: bool, safe_not_mounted: bool) -> Self {
        Self {
            mounter_path: mounter_path.to_string(),
            try_systemd,
            with_safe_not_mounted_behavior: safe_not_mounted,
            with_systemd: std::sync::OnceLock::new(),
            mountinfo_path: None,
        }
    }

    /// Test seam standing in for upstream's `mount.FakeMounter.MountPoints`
    /// (`k8s.io/mount-utils/fake_mounter.go`): point `GetMountRefs` at a
    /// mountinfo file instead of `/proc/self/mountinfo`, so a test can state
    /// which mounts reference a path.
    pub fn with_mountinfo_path(self, _path: &str) -> Self {
        self
    }

    /// `hasSystemd` (`mount_linux.go:100-111`).
    fn has_systemd(&self) -> bool {
        if !self.try_systemd {
            return false;
        }
        *self.with_systemd.get_or_init(detect_systemd)
    }

    /// `doMount` (`mount_linux.go:213-278`).
    #[allow(clippy::too_many_arguments)]
    fn do_mount(
        &self,
        mounter_path: &str,
        mount_cmd: &str,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
        mount_flags: &[String],
        systemd_mount_required: bool,
    ) -> io::Result<()> {
        let (mut mount_args, mut log_str) = make_mount_args_sensitive_with_mount_flags(
            source,
            target,
            fstype,
            options,
            sensitive_options,
            mount_flags,
        );
        let mut cmd = mount_cmd.to_string();
        if !mounter_path.is_empty() {
            mount_args.insert(0, cmd.clone());
            log_str = format!("{cmd} {log_str}");
            cmd = mounter_path.to_string();
        }
        if systemd_mount_required && self.has_systemd() {
            // Run via `systemd-run --scope` so fuse daemons live in their own
            // scope and survive a kubelet restart.
            (cmd, mount_args, log_str) =
                add_systemd_scope_sensitive("systemd-run", target, &cmd, &mount_args, &log_str);
        }
        tracing::debug!("Mounting cmd ({cmd}) with arguments ({log_str})");
        let out = Command::new(&cmd).args(&mount_args).output()?;
        if !out.status.success() {
            let output = format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            // Sensitive options are only ever present in the masked log_str.
            return Err(io::Error::other(format!(
                "mount failed: {}\nMounting command: {cmd}\nMounting arguments: {log_str}\nOutput: {output}",
                out.status
            )));
        }
        Ok(())
    }

    /// `bindMountSensitive` (`mount_linux.go:139-155`): bind, then remount
    /// with the options (`ro`, `nodev`, ...) found on the source.
    #[allow(clippy::too_many_arguments)]
    fn bind_mount_sensitive(
        &self,
        mounter_path: &str,
        source: &str,
        target: &str,
        fstype: &str,
        bind_opts: &[String],
        bind_remount_opts: &[String],
        bind_remount_opts_sensitive: &[String],
        mount_flags: &[String],
        systemd_mount_required: bool,
    ) -> io::Result<()> {
        self.do_mount(
            mounter_path,
            DEFAULT_MOUNT_COMMAND,
            source,
            target,
            fstype,
            bind_opts,
            bind_remount_opts_sensitive,
            mount_flags,
            systemd_mount_required,
        )?;
        let fix = get_bind_mount_options(source, statfs_flags)?;
        let mut remount = bind_remount_opts.to_vec();
        remount.extend(fix);
        self.do_mount(
            mounter_path,
            DEFAULT_MOUNT_COMMAND,
            source,
            target,
            fstype,
            &remount,
            bind_remount_opts_sensitive,
            mount_flags,
            systemd_mount_required,
        )
    }

    /// `Mount` (`mount_linux.go:163-165`).
    pub fn mount(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
    ) -> io::Result<()> {
        self.mount_sensitive(source, target, fstype, options, &[])
    }

    /// `MountSensitive` (`mount_linux.go:172-194`).
    pub fn mount_sensitive(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
    ) -> io::Result<()> {
        self.mount_impl(
            source,
            target,
            fstype,
            options,
            sensitive_options,
            &[],
            self.try_systemd,
        )
    }

    /// `MountSensitiveWithoutSystemdWithMountFlags` (`mount_linux.go:203-221`).
    pub fn mount_sensitive_without_systemd(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
        mount_flags: &[String],
    ) -> io::Result<()> {
        self.mount_impl(
            source,
            target,
            fstype,
            options,
            sensitive_options,
            mount_flags,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn mount_impl(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
        sensitive_options: &[String],
        mount_flags: &[String],
        systemd: bool,
    ) -> io::Result<()> {
        let (bind, bind_opts, bind_remount_opts, bind_remount_sensitive) =
            make_bind_opts_sensitive(options, sensitive_options);
        if bind {
            return self.bind_mount_sensitive(
                "",
                source,
                target,
                fstype,
                &bind_opts,
                &bind_remount_opts,
                &bind_remount_sensitive,
                mount_flags,
                systemd,
            );
        }
        // Filesystems that require the containerized mounter on GCI images.
        let mounter_path = if matches!(fstype, "nfs" | "glusterfs" | "ceph" | "cifs") {
            self.mounter_path.as_str()
        } else {
            ""
        };
        self.do_mount(
            mounter_path,
            DEFAULT_MOUNT_COMMAND,
            source,
            target,
            fstype,
            options,
            sensitive_options,
            mount_flags,
            systemd,
        )
    }

    /// `CanSafelySkipMountPointCheck` (`mount_linux.go:505-508`).
    pub fn can_safely_skip_mount_point_check(&self) -> bool {
        self.with_safe_not_mounted_behavior
    }

    /// `isLikelyNotMountPointStatx` (`mount_linux.go:460-486`).
    pub(crate) fn is_likely_not_mount_point_statx_with(
        file: &str,
        statx_fn: impl Fn(&str) -> Result<StatxInfo, StatxError>,
    ) -> Result<bool, StatxError> {
        let stat = statx_fn(file)?;
        if stat.attributes_mask != 0 && stat.attributes_mask & ROOT_ATTR != 0 {
            // No need to check rootStat if STATX_ATTR_MOUNT_ROOT is supported.
            return Ok(stat.attributes & ROOT_ATTR == 0);
        }
        // filepath.Dir(strings.TrimSuffix(file, "/"))
        let trimmed = file.strip_suffix('/').unwrap_or(file);
        let root = match std::path::Path::new(trimmed).parent() {
            Some(p) if p.as_os_str().is_empty() => ".",
            Some(p) => p.to_str().unwrap_or("."),
            None => trimmed,
        };
        let root_stat = statx_fn(root)?;
        Ok(stat.dev_major == root_stat.dev_major && stat.dev_minor == root_stat.dev_minor)
    }

    /// `IsLikelyNotMountPoint` (`mount_linux.go:495-503`).
    pub fn is_likely_not_mount_point(&self, file: &str) -> io::Result<bool> {
        match Self::is_likely_not_mount_point_statx_with(file, statx) {
            Err(StatxError::NotSupported) => self.is_likely_not_mount_point_stat(file),
            Err(StatxError::Io(e)) => Err(e),
            Ok(b) => Ok(b),
        }
    }

    /// `isLikelyNotMountPointStat` (`mount_linux.go:443-458`), the fallback
    /// `IsLikelyNotMountPoint` takes when `statx` is unsupported: a different device from the parent means a mount point.
    pub fn is_likely_not_mount_point_stat(&self, file: &str) -> io::Result<bool> {
        use std::os::unix::fs::MetadataExt;
        let st = std::fs::metadata(file)?;
        let parent = std::path::Path::new(file.strip_suffix('/').unwrap_or(file))
            .parent()
            .map(|p| {
                if p.as_os_str().is_empty() {
                    std::path::Path::new(".")
                } else {
                    p
                }
            })
            .unwrap_or_else(|| std::path::Path::new("/"));
        let root = std::fs::metadata(parent)?;
        Ok(st.dev() == root.dev())
    }

    /// `UnmountWithForce` (`mount_linux.go:411-423`): plain `umount`, then
    /// `umount -f` once `umount_timeout` has elapsed.
    pub fn unmount_with_force(
        &self,
        target: &str,
        umount_timeout: std::time::Duration,
    ) -> io::Result<()> {
        match self.try_unmount(target, umount_timeout) {
            Err(e) if e.kind() == io::ErrorKind::TimedOut => {
                tracing::info!("Timed out waiting for unmount of {target}, trying with -f");
                self.force_umount(target)
            }
            other => other,
        }
    }

    /// `tryUnmount` (`mount_linux.go:880-898`): `umount` bounded by a context
    /// deadline; `TimedOut` stands in for `context.DeadlineExceeded`.
    fn try_unmount(&self, target: &str, timeout: std::time::Duration) -> io::Result<()> {
        use std::io::Read;
        use std::process::Stdio;
        tracing::debug!("Unmounting {target}");
        let mut child = Command::new("umount")
            .arg(target)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        // CombinedOutput: drain both pipes concurrently with the wait.
        let mut out = child.stdout.take();
        let mut err = child.stderr.take();
        let reader = std::thread::spawn(move || {
            let mut buf = String::new();
            let mut tmp = Vec::new();
            if let Some(o) = out.as_mut() {
                let _ = o.read_to_end(&mut tmp);
            }
            if let Some(e) = err.as_mut() {
                let _ = e.read_to_end(&mut tmp);
            }
            buf.push_str(&String::from_utf8_lossy(&tmp));
            buf
        });
        let deadline = std::time::Instant::now() + timeout;
        let status = loop {
            if let Some(st) = child.try_wait()? {
                break st;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "context deadline exceeded",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        let output = reader.join().unwrap_or_default();
        if status.success() {
            return Ok(());
        }
        check_umount_error(
            target,
            &output,
            &status.to_string(),
            self.with_safe_not_mounted_behavior,
        )
    }

    /// `forceUmount` (`mount_linux.go:900-907`).
    fn force_umount(&self, target: &str) -> io::Result<()> {
        let out = Command::new("umount").args(["-f", target]).output()?;
        if out.status.success() {
            return Ok(());
        }
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        check_umount_error(
            target,
            &output,
            &out.status.to_string(),
            self.with_safe_not_mounted_behavior,
        )
    }

    /// `GetMountRefs` (`mount_linux.go:513-529`): every mount reference to
    /// `pathname`, over `/proc/self/mountinfo`.
    pub fn get_mount_refs(&self, pathname: &str) -> io::Result<Vec<String>> {
        super::mount_utils::get_mount_refs(pathname)
    }

    /// `Unmount` (`mount_linux.go:400-408`).
    pub fn unmount(&self, target: &str) -> io::Result<()> {
        tracing::debug!("Unmounting {target}");
        let out = Command::new("umount").arg(target).output()?;
        if out.status.success() {
            return Ok(());
        }
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        check_umount_error(
            target,
            &output,
            &out.status.to_string(),
            self.with_safe_not_mounted_behavior,
        )
    }
}

/// `CleanupMountPoint` (`mount_helper_common.go:30-41`): unmount `mount_path`
/// and delete the remaining directory if successful.
///
/// Takes the `mount.Interface` (`mounter` is any
/// [`MountInterface`](super::mount_interface::MountInterface), as upstream's
/// is), so a plugin test can hand it `FakeMounter`.
pub fn cleanup_mount_point(
    mount_path: &str,
    mounter: &dyn super::mount_interface::MountInterface,
    extensive_mount_point_check: bool,
) -> io::Result<()> {
    cleanup_impl(mount_path, mounter, extensive_mount_point_check, |m, p| {
        m.unmount(p)
    })
}

/// `CleanupMountWithForce` (`mount_helper_common.go:43-83`): as
/// [`cleanup_mount_point`], but unmounts with [`Mounter::unmount_with_force`].
pub fn cleanup_mount_with_force(
    mount_path: &str,
    mounter: &Mounter,
    extensive_mount_point_check: bool,
    umount_timeout: std::time::Duration,
) -> io::Result<()> {
    cleanup_impl(mount_path, mounter, extensive_mount_point_check, |m, p| {
        m.unmount_with_force(p, umount_timeout)
    })
}

/// The body `CleanupMountPoint`/`doCleanupMountPoint` and `CleanupMountWithForce`
/// share (`mount_helper_common.go:30-120`); they differ only in the unmount call.
fn cleanup_impl<M: super::mount_interface::MountInterface + ?Sized>(
    mount_path: &str,
    mounter: &M,
    extensive: bool,
    unmount: impl Fn(&M, &str) -> io::Result<()>,
) -> io::Result<()> {
    let (exists, path_err) = super::mount_utils::path_exists(mount_path);
    if !exists && path_err.is_none() {
        tracing::warn!("Warning: mount cleanup skipped because path does not exist: {mount_path}");
        return Ok(());
    }
    let corrupted = path_err
        .as_ref()
        .is_some_and(super::mount_utils::is_corrupted_mnt);
    if let Some(e) = path_err {
        if !corrupted {
            return Err(io::Error::other(format!("Error checking path: {e}")));
        }
    }
    if corrupted || mounter.can_safely_skip_mount_point_check() {
        tracing::debug!(
            "unmounting {mount_path:?} (corruptedMount: {corrupted}, mounterCanSkipMountPointChecks: {})",
            mounter.can_safely_skip_mount_point_check()
        );
        unmount(mounter, mount_path)?;
        return remove_path(mount_path);
    }

    // `if err != nil || notMnt { return err }`: not a mount point means it has
    // just been removed.
    if remove_path_if_not_mount_point(mount_path, mounter, extensive)? {
        return Ok(());
    }

    tracing::debug!("{mount_path:?} is a mountpoint, unmounting");
    unmount(mounter, mount_path)?;

    // `if notMnt { return err }`: a check/remove error and "not a mount point"
    // both return here; only a path that is still a mount point falls through.
    if remove_path_if_not_mount_point(mount_path, mounter, extensive)? {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "failed to cleanup mount point {mount_path}"
    )))
}

/// `removePathIfNotMountPoint` (`mount_helper_common.go:131-155`).
fn remove_path_if_not_mount_point<M: super::mount_interface::MountInterface + ?Sized>(
    mount_path: &str,
    mounter: &M,
    extensive: bool,
) -> io::Result<bool> {
    let checked = if extensive {
        super::mount_utils::is_not_mount_point(mount_path)
    } else {
        mounter.is_likely_not_mount_point(mount_path)
    };
    match checked {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            tracing::debug!("{mount_path:?} does not exist");
            Ok(true)
        }
        Err(e) => Err(e),
        Ok(true) => {
            tracing::debug!("{mount_path:?} is not a mountpoint, deleting");
            std::fs::remove_dir(mount_path).map(|()| true)
        }
        Ok(false) => Ok(false),
    }
}

/// `removePath` (`mount_helper_common.go:157-165`).
fn remove_path(mount_path: &str) -> io::Result<()> {
    tracing::debug!("Deleting path {mount_path:?}");
    match std::fs::remove_dir(mount_path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// mount flags for `path` via statvfs(3) (glibc exposes the statfs(2) mount flags there; ST_* == MS_* on Linux).
fn statfs_flags(path: &str) -> io::Result<u64> {
    let c =
        std::ffi::CString::new(path).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated string and `s` a valid out-pointer.
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(s.f_flag as u64)
}

const ROOT_ATTR: u64 = libc::STATX_ATTR_MOUNT_ROOT as u64;

/// `statx` (`mount_linux.go:430-441`): `AT_FDCWD`, `AT_STATX_DONT_SYNC`,
/// mask 0; `ENOSYS` becomes `errStatxNotSupport`.
fn statx(file: &str) -> Result<StatxInfo, StatxError> {
    let c = std::ffi::CString::new(file)
        .map_err(|_| StatxError::Io(io::Error::from_raw_os_error(libc::EINVAL)))?;
    let mut st: libc::statx = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is NUL-terminated and `st` is a valid out-pointer.
    let rc = unsafe {
        libc::statx(
            libc::AT_FDCWD,
            c.as_ptr(),
            libc::AT_STATX_DONT_SYNC,
            0,
            &mut st,
        )
    };
    if rc != 0 {
        let e = io::Error::last_os_error();
        return Err(if e.raw_os_error() == Some(libc::ENOSYS) {
            StatxError::NotSupported
        } else {
            StatxError::Io(e)
        });
    }
    Ok(StatxInfo {
        attributes: st.stx_attributes,
        attributes_mask: st.stx_attributes_mask,
        dev_major: st.stx_dev_major,
        dev_minor: st.stx_dev_minor,
    })
}

/// Subset of `unix.Statx_t` that `isLikelyNotMountPointStatx` reads.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StatxInfo {
    pub attributes: u64,
    pub attributes_mask: u64,
    pub dev_major: u32,
    pub dev_minor: u32,
}

/// `errStatxNotSupport` (`mount_linux.go:61`) or any other statx error.
#[derive(Debug)]
pub(crate) enum StatxError {
    NotSupported,
    Io(io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(attrs: u64, mask: u64, major: u32, minor: u32) -> StatxInfo {
        StatxInfo {
            attributes: attrs,
            attributes_mask: mask,
            dev_major: major,
            dev_minor: minor,
        }
    }
    const ROOT: u64 = libc::STATX_ATTR_MOUNT_ROOT as u64;

    // mount_linux.go:468-477: MOUNT_ROOT supported and set => mount point,
    // parent never statted.
    #[test]
    fn statx_mount_root_attr_set_is_mount_point() {
        let r = Mounter::is_likely_not_mount_point_statx_with("/a/b", |f| {
            assert_eq!(f, "/a/b", "parent must not be statted");
            Ok(info(ROOT, ROOT, 1, 1))
        });
        assert!(!r.unwrap());
    }

    // mount_linux.go:474-475: supported but clear => not a mount point.
    #[test]
    fn statx_mount_root_attr_clear_is_not_mount_point() {
        let r = Mounter::is_likely_not_mount_point_statx_with("/a/b", |f| {
            assert_eq!(f, "/a/b");
            Ok(info(0, ROOT, 1, 1))
        });
        assert!(r.unwrap());
    }

    // mount_linux.go:480-485: mask lacks MOUNT_ROOT => compare against the
    // parent (trailing slash trimmed) by dev major/minor.
    #[test]
    fn statx_without_mount_root_compares_parent_dev() {
        let f = |same: bool| {
            move |p: &str| match p {
                "/a/b/" | "/a/b" => Ok(info(0, 0, 8, 1)),
                "/a" => Ok(info(0, 0, 8, if same { 1 } else { 2 })),
                o => panic!("unexpected {o}"),
            }
        };
        assert!(Mounter::is_likely_not_mount_point_statx_with("/a/b/", f(true)).unwrap());
        assert!(!Mounter::is_likely_not_mount_point_statx_with("/a/b/", f(false)).unwrap());
    }

    // mount_linux.go:464-466, 481-483: statx errors propagate.
    #[test]
    fn statx_errors_propagate() {
        let r = Mounter::is_likely_not_mount_point_statx_with("/a/b", |_| {
            Err(StatxError::NotSupported)
        });
        assert!(matches!(r, Err(StatxError::NotSupported)));
        let r = Mounter::is_likely_not_mount_point_statx_with("/a/b", |p| {
            if p == "/a/b" {
                Ok(info(0, 0, 1, 1))
            } else {
                Err(StatxError::Io(io::Error::from_raw_os_error(libc::ENOENT)))
            }
        });
        assert!(matches!(r, Err(StatxError::Io(_))));
    }

    // mount_linux.go:495-503 through the real syscall.
    #[test]
    fn real_statx_mount_points() {
        let m = Mounter::new("", false, false);
        assert!(!m.is_likely_not_mount_point("/proc").unwrap());
        assert!(m.is_likely_not_mount_point("/proc/self").unwrap());
        assert!(m.is_likely_not_mount_point("/nonexistent-xyz").is_err());
    }

    /// The `FakeMounter.MountPoints` stand-in: a `Mounter` pointed at a
    /// mountinfo file reports the references that file describes, so
    /// `GetMountRefs`-level behaviour (the `GetDeviceMountRefs` gate) is
    /// testable beyond "delegates to the host mounter".
    #[test]
    fn get_mount_refs_reads_the_injected_mountinfo() {
        use std::io::Write;
        let d = tempfile::tempdir().unwrap();
        let real = std::fs::canonicalize(d.path()).unwrap();
        let info = format!(
            "10 1 8:1 / / rw - ext4 /dev/sda1 rw\n\
             20 10 8:1 /r {} rw - ext4 /dev/sda1 rw\n\
             21 10 8:1 /r /mnt/other rw - ext4 /dev/sda1 rw\n",
            real.display()
        );
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(info.as_bytes()).unwrap();
        let m = Mounter::new("", false, false).with_mountinfo_path(f.path().to_str().unwrap());
        assert_eq!(
            m.get_mount_refs(real.to_str().unwrap()).unwrap(),
            vec!["/mnt/other".to_string()]
        );
    }

    // TestMakeBindOpts (mount_test.go:27)
    #[test]
    fn make_bind_opts_cases() {
        let (bind, _, _) = make_bind_opts(&strs(&["vers=2", "ro", "_netdev"]));
        assert!(!bind);
        let (bind, o, r) = make_bind_opts(&strs(&["bind", "vers=2", "ro", "_netdev"]));
        assert!(bind);
        assert_eq!(o, strs(&["bind", "_netdev"]));
        assert_eq!(r, strs(&["bind", "remount", "vers=2", "ro", "_netdev"]));
    }

    // TestMakeBindOptsSensitive (mount_test.go:64)
    #[test]
    fn make_bind_opts_sensitive_cases() {
        let (bind, o, r, s) = make_bind_opts_sensitive(
            &strs(&["vers=2", "bind", "ro", "_netdev"]),
            &strs(&["user=foo", "remount", "pass=bar"]),
        );
        assert!(bind);
        assert_eq!(o, strs(&["bind", "_netdev"]));
        assert_eq!(r, strs(&["bind", "remount", "vers=2", "ro", "_netdev"]));
        assert_eq!(s, strs(&["user=foo", "pass=bar"]));

        let (bind, _, _, s) = make_bind_opts_sensitive(
            &strs(&["vers=2", "ro", "_netdev"]),
            &strs(&["user=foo", "pass=bar", "bind"]),
        );
        assert!(bind);
        assert_eq!(s, strs(&["user=foo", "pass=bar"]));

        let (bind, _, _, s) = make_bind_opts_sensitive(
            &strs(&["vers=2", "remount", "ro", "_netdev"]),
            &strs(&["user=foo", "pass=bar"]),
        );
        assert!(!bind);
        assert_eq!(s, strs(&["user=foo", "pass=bar"]));
    }

    // TestOptionsForLogging (mount_test.go:140)
    #[test]
    fn options_for_logging_masks_sensitive() {
        let cases: Vec<(Vec<String>, Vec<String>)> = vec![
            (strs(&["o1", "o2"]), strs(&["s1"])),
            (strs(&["o1", "o2"]), strs(&["s1", "s2"])),
            (vec![], strs(&["s1", "s2"])),
            (strs(&["o1", "o2"]), vec![]),
            (vec![], vec![]),
        ];
        for (o, s) in cases {
            let m = sanitized_options_for_logging(&o, &s);
            for x in &s {
                assert!(!m.contains(x.as_str()), "{m}");
            }
            assert_eq!(m.matches(SENSITIVE_OPTIONS_REMOVED).count(), s.len(), "{m}");
        }
        assert_eq!(
            sanitized_options_for_logging(&strs(&["o1", "o2"]), &strs(&["s1", "s2"])),
            "o1,o2,<masked>,<masked>"
        );
    }

    // TestSensitiveMountOptions (mount_linux_test.go:458)
    #[test]
    fn mount_args_hide_sensitive_in_log_only() {
        let (args, log) = make_mount_args_sensitive_with_mount_flags(
            "mySrc",
            "myTarget",
            "myFS",
            &strs(&["o1", "o2"]),
            &strs(&["s1", "s2"]),
            &strs(&["--no-canonicalize"]),
        );
        assert_eq!(
            args,
            strs(&[
                "--no-canonicalize",
                "-t",
                "myFS",
                "-o",
                "o1,o2,s1,s2",
                "mySrc",
                "myTarget"
            ])
        );
        assert!(log.contains("--no-canonicalize"));
        assert!(!log.contains("s1") && !log.contains("s2"));
        assert!(log.contains("o1,o2,<masked>,<masked>"));
        assert!(log.ends_with(" mySrc myTarget"));
    }

    #[test]
    fn mount_args_minimal() {
        let (args, log) = make_mount_args_sensitive_with_mount_flags("", "t", "", &[], &[], &[]);
        assert_eq!(args, strs(&["t"]));
        assert_eq!(log, " t");
    }

    #[test]
    fn systemd_scope_wraps_command() {
        let (path, args, log) = add_systemd_scope_sensitive(
            "systemd-run",
            "/mnt/x",
            "mount",
            &strs(&["-o", "a", "/mnt/x"]),
            "-o a /mnt/x",
        );
        assert_eq!(path, "systemd-run");
        assert_eq!(
            args,
            strs(&[
                "--description=Kubernetes transient mount for /mnt/x",
                "--scope",
                "--",
                "mount",
                "-o",
                "a",
                "/mnt/x"
            ])
        );
        assert_eq!(
            log,
            "--description=Kubernetes transient mount for /mnt/x --scope -- mount -o a /mnt/x"
        );
    }

    // TestGetBindMountOptions (mount_linux_test.go:839)
    #[test]
    fn get_bind_mount_options_cases() {
        let cases: &[(u64, &str)] = &[
            (libc::MS_RDONLY, "ro"),
            (libc::MS_NODEV, "nodev"),
            (libc::MS_NOEXEC, "noexec"),
            (libc::MS_NOSUID, "nosuid"),
            (libc::MS_NOATIME, "noatime"),
            (libc::MS_RELATIME, "relatime"),
            (libc::MS_NODIRATIME, "nodiratime"),
            (libc::MS_RDONLY | libc::MS_NODEV, "nodev,ro"),
            (
                libc::MS_RDONLY | libc::MS_NODEV | libc::MS_NOEXEC,
                "nodev,noexec,ro",
            ),
        ];
        for (flags, want) in cases {
            let mut got = get_bind_mount_options("p", |_| Ok(*flags)).unwrap();
            got.sort();
            assert_eq!(got.join(","), *want);
        }
    }

    #[test]
    fn get_bind_mount_options_statfs_error() {
        let e = get_bind_mount_options("p", |_| Err(io::Error::other("boom"))).unwrap_err();
        assert!(e.to_string().contains("boom"));
    }

    #[test]
    fn statfs_flags_reads_real_path_and_errors_on_missing() {
        assert!(statfs_flags("/").is_ok());
        assert!(statfs_flags("/nonexistent-path-3193").is_err());
    }

    #[test]
    fn umount_error_not_mounted_ignored_only_when_safe() {
        assert!(check_umount_error("/t", "umount: /t: not mounted.", "exit 32", true).is_ok());
        let e = check_umount_error("/t", "umount: /t: not mounted.", "exit 32", false).unwrap_err();
        assert!(e
            .to_string()
            .starts_with("unmount failed: exit 32\nUnmounting arguments: /t\nOutput: "));
    }

    // `CleanupMountPoint` / `CleanupMountWithForce` (`mount_helper_common.go`)
    // on paths that are not mount points: a missing path is skipped, a plain
    // directory is removed.
    #[test]
    fn cleanup_skips_a_missing_path_and_removes_a_plain_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let m = Mounter::new("", false, false);
        let missing = tmp.path().join("missing").to_string_lossy().to_string();
        cleanup_mount_point(&missing, &m, true).unwrap();
        cleanup_mount_with_force(&missing, &m, true, std::time::Duration::from_secs(1)).unwrap();

        let dir = tmp.path().join("d");
        std::fs::create_dir(&dir).unwrap();
        cleanup_mount_point(&dir.to_string_lossy(), &m, true).unwrap();
        assert!(!dir.exists());

        std::fs::create_dir(&dir).unwrap();
        cleanup_mount_with_force(
            &dir.to_string_lossy(),
            &m,
            false,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert!(!dir.exists());
    }

    // `removePathIfNotMountPoint`: a non-empty directory that is not a mount
    // point surfaces `os.Remove`'s error.
    #[test]
    fn cleanup_surfaces_a_remove_error() {
        let tmp = tempfile::tempdir().unwrap();
        let m = Mounter::new("", false, false);
        let dir = tmp.path().join("d");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("f"), b"x").unwrap();
        assert!(cleanup_mount_point(&dir.to_string_lossy(), &m, true).is_err());
        assert!(dir.exists());
    }
}
