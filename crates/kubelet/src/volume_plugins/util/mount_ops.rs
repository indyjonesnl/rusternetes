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
    todo!("red")
}

/// `MakeBindOpts` (`mount.go:296`).
pub fn make_bind_opts(options: &[String]) -> (bool, Vec<String>, Vec<String>) {
    let (b, o, r, _) = make_bind_opts_sensitive(options, &[]);
    (b, o, r)
}

/// `sanitizedOptionsForLogging` (`mount.go:384-401`).
pub fn sanitized_options_for_logging(options: &[String], sensitive_options: &[String]) -> String {
    todo!("red")
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
    todo!("red")
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
    todo!("red")
}

/// Map `statfs.f_flags` mount flags to mount option strings
/// (`getBindMountOptions`, `mount_linux.go:116-137`). Go iterates a map, so
/// order is unspecified there; we emit a fixed order.
pub fn bind_mount_options_from_flags(flags: u64) -> Vec<String> {
    todo!("red")
}

/// `getBindMountOptions` with an injectable statfs (as upstream's test does).
pub fn get_bind_mount_options<F>(path: &str, statfs: F) -> io::Result<Vec<String>>
where
    F: FnOnce(&str) -> io::Result<u64>,
{
    todo!("red")
}

/// `checkUmountError` (`mount_linux.go:910-924`), minus the
/// `errNoChildProcesses` rewrite which Rust's `Command` never produces.
pub fn check_umount_error(
    target: &str,
    output: &str,
    err: &str,
    with_safe_not_mounted_behavior: bool,
) -> io::Result<()> {
    todo!("red")
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
}

impl Mounter {
    pub fn mount(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
    ) -> io::Result<()> {
        self.mount_sensitive(source, target, fstype, options, &[])
    }

    pub fn mount_sensitive(
        &self,
        _source: &str,
        _target: &str,
        _fstype: &str,
        _options: &[String],
        _sensitive_options: &[String],
    ) -> io::Result<()> {
        todo!("red")
    }

    pub fn mount_sensitive_without_systemd(
        &self,
        _source: &str,
        _target: &str,
        _fstype: &str,
        _options: &[String],
        _sensitive_options: &[String],
        _mount_flags: &[String],
    ) -> io::Result<()> {
        todo!("red")
    }

    pub fn unmount(&self, _target: &str) -> io::Result<()> {
        todo!("red")
    }
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
    fn umount_error_not_mounted_ignored_only_when_safe() {
        assert!(check_umount_error("/t", "umount: /t: not mounted.", "exit 32", true).is_ok());
        let e = check_umount_error("/t", "umount: /t: not mounted.", "exit 32", false).unwrap_err();
        assert!(e
            .to_string()
            .starts_with("unmount failed: exit 32\nUnmounting arguments: /t\nOutput: "));
    }
}
