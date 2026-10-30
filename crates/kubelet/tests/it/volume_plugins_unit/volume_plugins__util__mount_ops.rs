//! Unit tests moved from `src/volume_plugins/util/mount_ops.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::io;
    pub use std::process::Command;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::mount_ops::*;

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
