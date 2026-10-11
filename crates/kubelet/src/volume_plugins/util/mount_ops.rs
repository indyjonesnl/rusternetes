//! Port of the mount-command half of `staging/src/k8s.io/mount-utils`:
//! `MakeBindOptsSensitive` (`mount.go:306`), `sanitizedOptionsForLogging`
//! (`mount.go:384`), `MakeMountArgsSensitiveWithMountFlags`
//! (`mount_linux.go:350`), `AddSystemdScopeSensitive` (`mount_linux.go:393`),
//! `getBindMountOptions` (`mount_linux.go:116`), `Mounter.Mount*`/`doMount`/
//! `bindMountSensitive` and `Mounter.Unmount`/`checkUmountError`.
//!
//! Not ported (tracked in #3193): `UnmountWithForce`, `CleanupMountPoint`,
//! `detectSystemd`/`detectSafeNotMountedBehavior` probing (the caller states
//! `try_systemd`/`safe_not_mounted` explicitly), containerized mounter path.

#![allow(dead_code)]

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
}

impl Mounter {
    pub fn new(mounter_path: &str, try_systemd: bool, safe_not_mounted: bool) -> Self {
        Self {
            mounter_path: mounter_path.to_string(),
            try_systemd,
            with_safe_not_mounted_behavior: safe_not_mounted,
            with_systemd: std::sync::OnceLock::new(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
