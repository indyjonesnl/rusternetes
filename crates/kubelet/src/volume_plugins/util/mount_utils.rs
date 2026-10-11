//! Port of `staging/src/k8s.io/mount-utils` (Linux): `ParseMountInfo`,
//! `SearchMountPoints`, `Mounter.GetMountRefs`, `Mounter.IsMountPoint`,
//! `IsNotMountPoint`, `PathWithinBase`, `IsCorruptedMnt`.
//!
//! Not ported (tracked separately): `Mount`/`MountSensitiveWithoutSystemd`
//! (`mount_linux.go:163`), `Unmount`, `CleanupMountPoint`.

use std::io;

/// Port of `mount.MountInfo` (`mount_helper_unix.go:66-90`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MountInfo {
    pub id: i32,
    pub parent_id: i32,
    pub major: i32,
    pub minor: i32,
    pub root: String,
    pub source: String,
    pub mount_point: String,
    pub optional_fields: Vec<String>,
    pub fs_type: String,
    pub mount_options: Vec<String>,
    pub super_options: Vec<String>,
}

pub const PROC_MOUNT_INFO_PATH: &str = "/proc/self/mountinfo";

/// `splitMountOptions` (`mount_helper_unix.go:150-160`).
pub fn split_mount_options(_s: &str) -> Vec<String> {
    todo!()
}

/// `ParseMountInfo`'s body (`mount_helper_unix.go:102-148`) over file content.
pub fn parse_mount_info_str(_content: &str) -> Result<Vec<MountInfo>, String> {
    todo!()
}

/// `ParseMountInfo` (`mount_helper_unix.go:102`).
pub fn parse_mount_info(_filename: &str) -> io::Result<Vec<MountInfo>> {
    todo!()
}

/// `PathWithinBase` (`mount.go:362`).
pub fn path_within_base(_full_path: &str, _base_path: &str) -> bool {
    todo!()
}

/// `SearchMountPoints` (`mount_linux.go:779-820`).
pub fn search_mount_points(_host_source: &str, _mountinfo_path: &str) -> io::Result<Vec<String>> {
    todo!()
}

/// `IsCorruptedMnt` (`mount_helper_unix.go:41-65`).
pub fn is_corrupted_mnt(_err: &io::Error) -> bool {
    todo!()
}

/// `Mounter.GetMountRefs` (`mount_linux.go:513-529`) over a mountinfo file.
pub fn get_mount_refs_at(_pathname: &str, _mountinfo_path: &str) -> io::Result<Vec<String>> {
    todo!()
}

/// `Mounter.GetMountRefs` against `/proc/self/mountinfo`.
pub fn get_mount_refs(pathname: &str) -> io::Result<Vec<String>> {
    get_mount_refs_at(pathname, PROC_MOUNT_INFO_PATH)
}

/// `Mounter.IsMountPoint` (`mount_linux.go:840-880`) over a mountinfo file.
pub fn is_mount_point_at(_file: &str, _mountinfo_path: &str) -> io::Result<bool> {
    todo!()
}

/// `mount.IsNotMountPoint` (`mount.go:240-247`).
pub fn is_not_mount_point(file: &str) -> io::Result<bool> {
    is_mount_point_at(file, PROC_MOUNT_INFO_PATH).map(|m| !m)
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
