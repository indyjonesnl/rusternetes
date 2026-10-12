//! Port of `staging/src/k8s.io/mount-utils` (Linux): `ParseMountInfo`,
//! `SearchMountPoints`, `Mounter.GetMountRefs`, `Mounter.IsMountPoint`,
//! `IsNotMountPoint`, `PathWithinBase`, `IsCorruptedMnt`.
//!
//! Not ported (tracked separately): `Mount`/`MountSensitiveWithoutSystemd`
//! (`mount_linux.go:163`), `Unmount`, `CleanupMountPoint`.

use std::io;
use std::path::{Path, PathBuf};

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

/// `expectedAtLeastNumFieldsPerMountInfo` (`mount_helper_unix.go:35`).
const EXPECTED_AT_LEAST_NUM_FIELDS_PER_MOUNT_INFO: usize = 10;

/// `splitMountOptions` (`mount_helper_unix.go:150-160`): commas inside double
/// quotes are not separators.
pub fn split_mount_options(s: &str) -> Vec<String> {
    let mut in_quotes = false;
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in s.chars() {
        if c == '"' {
            in_quotes = !in_quotes;
        }
        if c == ',' && !in_quotes {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push(c);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// `ParseMountInfo`'s body (`mount_helper_unix.go:102-148`) over file content.
pub fn parse_mount_info_str(content: &str) -> Result<Vec<MountInfo>, String> {
    let mut infos = Vec::new();
    for line in content.split('\n') {
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < EXPECTED_AT_LEAST_NUM_FIELDS_PER_MOUNT_INFO {
            return Err(format!(
                "wrong number of fields in (expected at least {}, got {}): {}",
                EXPECTED_AT_LEAST_NUM_FIELDS_PER_MOUNT_INFO,
                fields.len(),
                line
            ));
        }
        let id: i32 = fields[0].parse().map_err(|e| format!("{e}"))?;
        let parent_id: i32 = fields[1].parse().map_err(|e| format!("{e}"))?;
        let mm: Vec<&str> = fields[2].split(':').collect();
        if mm.len() != 2 {
            return Err(format!(
                "parsing '{line}' failed: unexpected minor:major pair {mm:?}"
            ));
        }
        let major: i32 = mm[0].parse().map_err(|e| {
            format!(
                "parsing '{}' failed: unable to parse major device id, err:{e}",
                mm[0]
            )
        })?;
        let minor: i32 = mm[1].parse().map_err(|e| {
            format!(
                "parsing '{}' failed: unable to parse minor device id, err:{e}",
                mm[1]
            )
        })?;
        let mut info = MountInfo {
            id,
            parent_id,
            major,
            minor,
            root: fields[3].to_string(),
            mount_point: fields[4].to_string(),
            mount_options: split_mount_options(fields[5]),
            ..Default::default()
        };
        // All fields until "-" are "optional fields".
        let mut i = 6;
        while i < fields.len() && fields[i] != "-" {
            info.optional_fields.push(fields[i].to_string());
            i += 1;
        }
        i += 1;
        if fields.len() < i + 3 {
            return Err(format!(
                "expect 3 fields in {}, got {}",
                line,
                fields.len().saturating_sub(i)
            ));
        }
        info.fs_type = fields[i].to_string();
        info.source = fields[i + 1].to_string();
        info.super_options = split_mount_options(fields[i + 2]);
        infos.push(info);
    }
    Ok(infos)
}

/// `ParseMountInfo` (`mount_helper_unix.go:102`).
pub fn parse_mount_info(filename: &str) -> io::Result<Vec<MountInfo>> {
    let content = std::fs::read_to_string(filename)?;
    parse_mount_info_str(&content).map_err(io::Error::other)
}

/// Go `path.Clean` for the subset we need (lexical).
fn clean(p: &str) -> String {
    let abs = p.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for c in p.split('/') {
        match c {
            "" | "." => {}
            ".." => {
                if matches!(parts.last(), Some(l) if *l != "..") {
                    parts.pop();
                } else if !abs {
                    parts.push("..");
                }
            }
            c => parts.push(c),
        }
    }
    let j = parts.join("/");
    match (abs, j.is_empty()) {
        (true, _) => format!("/{j}"),
        (false, true) => ".".into(),
        _ => j,
    }
}

/// `PathWithinBase` (`mount.go:362`): `filepath.Rel` then `StartsWithBackstep`.
pub fn path_within_base(full_path: &str, base_path: &str) -> bool {
    if full_path.starts_with('/') != base_path.starts_with('/') {
        return false; // filepath.Rel errors
    }
    let (f, b) = (clean(full_path), clean(base_path));
    if b == "." {
        return f != ".." && !f.starts_with("../");
    }
    if f == b {
        return true;
    }
    let prefix = if b == "/" {
        "/".to_string()
    } else {
        format!("{b}/")
    };
    f.starts_with(&prefix)
}

/// `SearchMountPoints` (`mount_linux.go:779-820`).
pub fn search_mount_points(host_source: &str, mountinfo_path: &str) -> io::Result<Vec<String>> {
    let mis = parse_mount_info(mountinfo_path)?;
    let mut mount_id = 0;
    let mut root_path = String::new();
    let mut major = -1;
    let mut minor = -1;
    // Backward: later mounts may overlap earlier ones.
    for mi in mis.iter().rev() {
        if host_source == mi.mount_point || path_within_base(host_source, &mi.mount_point) {
            mount_id = mi.id;
            let suffix = host_source
                .strip_prefix(mi.mount_point.as_str())
                .unwrap_or(host_source);
            root_path = clean(&format!("{}/{}", mi.root, suffix));
            major = mi.major;
            minor = mi.minor;
            break;
        }
    }
    if root_path.is_empty() || major == -1 || minor == -1 {
        return Err(io::Error::other(format!(
            "failed to get root path and major:minor for {host_source}"
        )));
    }
    Ok(mis
        .iter()
        .filter(|mi| {
            mi.id != mount_id && mi.root == root_path && mi.major == major && mi.minor == minor
        })
        .map(|mi| mi.mount_point.clone())
        .collect())
}

/// `IsCorruptedMnt` (`mount_helper_unix.go:41-65`).
pub fn is_corrupted_mnt(err: &io::Error) -> bool {
    // ENOTCONN, ESTALE, EIO, EACCES, EHOSTDOWN, EWOULDBLOCK, ENODEV
    matches!(err.raw_os_error(), Some(107 | 116 | 5 | 13 | 112 | 11 | 19))
}

/// `PathExists` (`mount_helper_unix.go`): `(exists, err)`.
pub(crate) fn path_exists(path: &str) -> (bool, Option<io::Error>) {
    match std::fs::metadata(path) {
        Ok(_) => (true, None),
        Err(e) if e.kind() == io::ErrorKind::NotFound => (false, None),
        Err(e) if is_corrupted_mnt(&e) => (true, Some(e)),
        Err(e) => (false, Some(e)),
    }
}

/// `Mounter.GetMountRefs` (`mount_linux.go:513-529`) over a mountinfo file.
pub fn get_mount_refs_at(pathname: &str, mountinfo_path: &str) -> io::Result<Vec<String>> {
    let (exists, err) = path_exists(pathname);
    match err {
        None if !exists => return Ok(vec![]),
        Some(e) if is_corrupted_mnt(&e) => return Ok(vec![]),
        Some(e) => {
            return Err(io::Error::other(format!(
                "error checking path {pathname}: {e}"
            )))
        }
        None => {}
    }
    let real: PathBuf = std::fs::canonicalize(Path::new(pathname))?;
    search_mount_points(&real.to_string_lossy(), mountinfo_path)
}

/// `Mounter.GetMountRefs` against `/proc/self/mountinfo`.
pub fn get_mount_refs(pathname: &str) -> io::Result<Vec<String>> {
    get_mount_refs_at(pathname, PROC_MOUNT_INFO_PATH)
}

/// `Mounter.IsMountPoint` (`mount_linux.go:840-880`) over a mountinfo file.
///
/// Deviation: upstream's fast path is `mountinfo.MountedFast`
/// (moby/sys/mountinfo); here the fast path is the device compare against the
/// parent and, as upstream does whenever the fast path is not sure, the list
/// (`List()` == `ParseMountInfo`) is always consulted for the same-device
/// (bind mount) case.
pub fn is_mount_point_at(file: &str, mountinfo_path: &str) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let st = std::fs::metadata(file)?; // NotFound propagates as ErrNotExist
    let resolved = std::fs::canonicalize(file)?;
    if let Some(parent) = resolved.parent() {
        if let Ok(pst) = std::fs::metadata(parent) {
            if pst.dev() != st.dev() {
                return Ok(true);
            }
        }
    }
    let resolved = resolved.to_string_lossy();
    // isMountPointMatch (`mount_helper_unix.go:162`).
    Ok(parse_mount_info(mountinfo_path)?.iter().any(|mi| {
        mi.mount_point
            .strip_suffix("\\040(deleted)")
            .unwrap_or(&mi.mount_point)
            == resolved
    }))
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
