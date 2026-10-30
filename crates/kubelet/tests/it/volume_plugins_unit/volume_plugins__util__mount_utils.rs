//! Unit tests moved from `src/volume_plugins/util/mount_utils.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::io;
    pub use std::path::{Path, PathBuf};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::mount_utils::*;
    use std::io::Write;

    // Fixture modelled on mount_helper_unix_test.go TestParseMountInfo.
    const INFO: &str = "\
10 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
20 10 8:1 /var/lib/kubelet/pods/a/vol /var/lib/kubelet/pods/a/volumes/x rw,relatime shared:1 - ext4 /dev/sda1 rw
21 10 8:1 /var/lib/kubelet/pods/b/vol /var/lib/kubelet/pods/b/volumes/x rw,relatime shared:1 - ext4 /dev/sda1 rw
22 10 8:1 /var/lib/kubelet/pods/a/vol /mnt/other rw - ext4 /dev/sda1 rw
30 10 0:25 / /tmp rw,nosuid - tmpfs tmpfs rw,context=\"a,b\"
";

    fn write_tmp(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn parse_mount_info_fields() {
        let mis = parse_mount_info_str(INFO).unwrap();
        assert_eq!(mis.len(), 5);
        assert_eq!(mis[1].id, 20);
        assert_eq!(mis[1].parent_id, 10);
        assert_eq!((mis[1].major, mis[1].minor), (8, 1));
        assert_eq!(mis[1].root, "/var/lib/kubelet/pods/a/vol");
        assert_eq!(mis[1].mount_point, "/var/lib/kubelet/pods/a/volumes/x");
        assert_eq!(mis[1].optional_fields, vec!["shared:1"]);
        assert_eq!(mis[1].fs_type, "ext4");
        assert_eq!(mis[1].source, "/dev/sda1");
        assert!(mis[3].optional_fields.is_empty());
        // double-quoted commas are not separators
        assert_eq!(mis[4].super_options, vec!["rw", "context=\"a,b\""]);
    }

    #[test]
    fn parse_mount_info_rejects_short_line() {
        let e = parse_mount_info_str("1 2 3\n").unwrap_err();
        assert!(e.contains("wrong number of fields"), "{e}");
    }

    #[test]
    fn path_within_base_cases() {
        assert!(path_within_base("/a/b", "/a"));
        assert!(path_within_base("/a", "/a"));
        assert!(path_within_base("/a/b", "/"));
        assert!(!path_within_base("/ab", "/a"));
        assert!(!path_within_base("/a", "/a/b"));
    }

    #[test]
    fn search_mount_points_finds_bind_refs() {
        let f = write_tmp(INFO);
        let p = f.path().to_str().unwrap();
        // refs of pod a's mount: the other bind of the same root+dev, NOT itself.
        let mut refs = search_mount_points("/var/lib/kubelet/pods/a/volumes/x", p).unwrap();
        refs.sort();
        assert_eq!(refs, vec!["/mnt/other".to_string()]);
    }

    #[test]
    fn search_mount_points_errors_when_no_mount_covers() {
        let f = write_tmp("10 1 8:1 /x /only rw - ext4 /dev/sda1 rw\n");
        let e = search_mount_points("/elsewhere", f.path().to_str().unwrap()).unwrap_err();
        assert!(e.to_string().contains("failed to get root path"), "{e}");
    }

    #[test]
    fn get_mount_refs_missing_path_is_empty() {
        let f = write_tmp(INFO);
        let r = get_mount_refs_at("/definitely/not/here", f.path().to_str().unwrap()).unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn is_mount_point_missing_is_not_found() {
        let f = write_tmp(INFO);
        let e = is_mount_point_at("/definitely/not/here", f.path().to_str().unwrap()).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn is_mount_point_matches_mountinfo_bind() {
        // A plain dir on the same device as its parent is only a mount point
        // if mountinfo says so (the bind-mount case IsLikelyNotMountPoint misses).
        let d = tempfile::tempdir().unwrap();
        let real = std::fs::canonicalize(d.path()).unwrap();
        let line = format!("40 1 8:1 /r {} rw - ext4 /dev/sda1 rw\n", real.display());
        let f = write_tmp(&line);
        assert!(is_mount_point_at(real.to_str().unwrap(), f.path().to_str().unwrap()).unwrap());
        let g = write_tmp(INFO);
        assert!(!is_mount_point_at(real.to_str().unwrap(), g.path().to_str().unwrap()).unwrap());
    }

    #[test]
    fn corrupted_mnt_errnos() {
        assert!(is_corrupted_mnt(&io::Error::from_raw_os_error(107))); // ENOTCONN
        assert!(is_corrupted_mnt(&io::Error::from_raw_os_error(116))); // ESTALE
        assert!(!is_corrupted_mnt(&io::Error::from_raw_os_error(2))); // ENOENT
    }
    #[test]
    fn consistent_read_returns_stable_content() {
        let f = write_tmp("abc");
        let got = consistent_read(f.path().to_str().unwrap(), 3).unwrap();
        assert_eq!(got, b"abc");
    }

    #[test]
    fn consistent_read_reports_inconsistent_after_attempts() {
        // The file changes between every pair of reads (upstream's
        // consistentReadSync `sync` hook), so it never stabilises.
        let f = write_tmp("0");
        let path = f.path().to_str().unwrap().to_string();
        let mut sync = |i: usize| std::fs::write(&path, format!("{}", i + 1)).unwrap();
        let err = consistent_read_sync(&path, 5, Some(&mut sync)).unwrap_err();
        assert!(is_inconsistent_read_error(&err));
        assert_eq!(
            err.to_string(),
            format!("could not get consistent content of {path} after 5 attempts")
        );
    }

    #[test]
    fn consistent_read_succeeds_once_content_settles() {
        let f = write_tmp("0");
        let path = f.path().to_str().unwrap().to_string();
        let mut sync = |i: usize| {
            if i < 2 {
                std::fs::write(&path, format!("{}", i + 1)).unwrap();
            }
        };
        let got = consistent_read_sync(&path, 5, Some(&mut sync)).unwrap();
        assert_eq!(got, b"2");
    }

    #[test]
    fn plain_io_errors_are_not_inconsistent_reads() {
        assert!(!is_inconsistent_read_error(&io::Error::other("x")));
        assert!(!is_inconsistent_read_error(&io::Error::from(
            io::ErrorKind::NotFound
        )));
    }

    #[test]
    fn kernel_release_bug_gate() {
        let bug = kernel_release_has_mountinfo_bug;
        assert!(!bug(b"5.8.0"));
        assert!(!bug(b"6.1.0-generic"));
        assert!(bug(b"5.7.19"));
        assert!(bug(b"4.19.0"));
        assert!(bug(b"5.15")); // fewer than 3 parts: assume old kernel
    }

    fn inconsistent() -> io::Error {
        io::Error::other(InconsistentReadError {
            filename: "/proc/self/mountinfo".into(),
            attempts: 10,
        })
    }

    #[test]
    fn reliable_mount_refs_retries_inconsistent_reads() {
        let mut calls = 0;
        let got = get_reliable_mount_refs_with(
            std::time::Duration::from_millis(1),
            std::time::Duration::from_secs(5),
            || {
                calls += 1;
                if calls < 3 {
                    Err(inconsistent())
                } else {
                    Ok(vec!["/a".to_string()])
                }
            },
        )
        .unwrap();
        assert_eq!(got, vec!["/a".to_string()]);
        assert_eq!(calls, 3);
    }

    #[test]
    fn reliable_mount_refs_returns_other_errors_immediately() {
        let mut calls = 0;
        let err = get_reliable_mount_refs_with(
            std::time::Duration::from_millis(1),
            std::time::Duration::from_secs(5),
            || {
                calls += 1;
                Err(io::Error::other("boom"))
            },
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "boom");
        assert_eq!(calls, 1);
    }

    #[test]
    fn reliable_mount_refs_times_out_with_last_inconsistent_error() {
        let err = get_reliable_mount_refs_with(
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(20),
            || Err(inconsistent()),
        )
        .unwrap_err();
        assert!(is_inconsistent_read_error(&err));
    }
}
