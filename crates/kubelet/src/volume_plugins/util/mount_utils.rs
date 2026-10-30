//! Port of `staging/src/k8s.io/mount-utils` (Linux): `ParseMountInfo`,
//! `SearchMountPoints`, `Mounter.GetMountRefs`, `Mounter.IsMountPoint`,
//! `IsNotMountPoint`, `PathWithinBase`, `IsCorruptedMnt`, `ConsistentRead`
//! and `GetReliableMountRefs`.
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

/// `maxListTries` (`mount_helper_unix.go:42`).
const MAX_LIST_TRIES: usize = 10;

/// `InconsistentReadError` (`k8s.io/utils/io/read.go`, vendored at
/// `vendor/k8s.io/utils/io/read.go:62-72`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InconsistentReadError {
    pub filename: String,
    pub attempts: usize,
}

impl std::fmt::Display for InconsistentReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "could not get consistent content of {} after {} attempts",
            self.filename, self.attempts
        )
    }
}

impl std::error::Error for InconsistentReadError {}

/// `IsInconsistentReadError` (`vendor/k8s.io/utils/io/read.go:74-79`): Go's
/// type assertion on the error value, which does not unwrap, so only an error
/// carrying exactly an [`InconsistentReadError`] matches.
pub fn is_inconsistent_read_error(err: &io::Error) -> bool {
    err.get_ref()
        .is_some_and(|e| e.downcast_ref::<InconsistentReadError>().is_some())
}

/// `consistentReadSync` (`vendor/k8s.io/utils/io/read.go:41-60`); `sync` is the
/// hook the upstream tests use to mutate the file between reads.
pub fn consistent_read_sync(
    filename: &str,
    attempts: usize,
    mut sync: Option<&mut dyn FnMut(usize)>,
) -> io::Result<Vec<u8>> {
    let mut old_content = std::fs::read(filename)?;
    for i in 0..attempts {
        if let Some(f) = sync.as_mut() {
            f(i);
        }
        let new_content = std::fs::read(filename)?;
        if old_content == new_content {
            return Ok(new_content);
        }
        // Files are different, continue reading
        old_content = new_content;
    }
    Err(io::Error::other(InconsistentReadError {
        filename: filename.to_string(),
        attempts,
    }))
}

/// `ConsistentRead` (`vendor/k8s.io/utils/io/read.go:30-32`): reads a file
/// until it gets the same content twice.
pub fn consistent_read(filename: &str, attempts: usize) -> io::Result<Vec<u8>> {
    consistent_read_sync(filename, attempts, None)
}

/// `kernelHasMountinfoBug` (`mount_helper_unix.go:211-247`): the bug was fixed
/// by kernel commit 9f6c61f96f2d97 (since Linux 5.8).
fn kernel_has_mountinfo_bug() -> bool {
    static HAS_BUG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *HAS_BUG.get_or_init(|| {
        // Assume old kernel when uname fails.
        // SAFETY: a zeroed utsname is a valid out-parameter for uname(2).
        let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
        if unsafe { libc::uname(&mut uts) } != 0 {
            return true;
        }
        let release: Vec<u8> = uts
            .release
            .iter()
            .take_while(|c| **c != 0)
            .map(|c| *c as u8)
            .collect();
        kernel_release_has_mountinfo_bug(&release)
    })
}

/// The version test inside `kernelHasMountinfoBug`: `SplitN(release, ".", 3)`
/// must give three parts, then `major > 5 || (major == 5 && minor >= 8)` is
/// fixed.
pub fn kernel_release_has_mountinfo_bug(release: &[u8]) -> bool {
    let v: Vec<&[u8]> = release.splitn(3, |b| *b == b'.').collect();
    if v.len() != 3 {
        return true;
    }
    // strconv.Atoi errors are ignored upstream (value 0).
    let atoi = |b: &[u8]| {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0)
    };
    let (major, minor) = (atoi(v[0]), atoi(v[1]));
    !(major > 5 || (major == 5 && minor >= 8))
}

/// `readMountInfo` (`mount_helper_unix.go:250-256`).
fn read_mount_info(path: &str) -> io::Result<Vec<u8>> {
    if kernel_has_mountinfo_bug() {
        return consistent_read(path, MAX_LIST_TRIES);
    }
    std::fs::read(path)
}

/// `ParseMountInfo` (`mount_helper_unix.go:102`).
pub fn parse_mount_info(filename: &str) -> io::Result<Vec<MountInfo>> {
    let content = read_mount_info(filename)?;
    let content = String::from_utf8_lossy(&content);
    parse_mount_info_str(&content).map_err(io::Error::other)
}

/// `GetReliableMountRefs` (`pkg/volume/util/util.go:661-683`): calls
/// `GetMountRefs` and retries on [`is_inconsistent_read_error`], polling
/// immediately then every 10ms for up to a minute.
pub fn get_reliable_mount_refs<M: super::mount_interface::MountInterface + ?Sized>(
    mounter: &M,
    mount_path: &str,
) -> io::Result<Vec<String>> {
    get_reliable_mount_refs_with(
        std::time::Duration::from_millis(10),
        std::time::Duration::from_secs(60),
        || mounter.get_mount_refs(mount_path),
    )
}

/// `wait.PollImmediate(interval, timeout, ..)` around `GetMountRefs`; on
/// timeout (`wait.Interrupted`) the last inconsistent-read error is returned.
pub fn get_reliable_mount_refs_with(
    interval: std::time::Duration,
    timeout: std::time::Duration,
    mut get: impl FnMut() -> io::Result<Vec<String>>,
) -> io::Result<Vec<String>> {
    let start = std::time::Instant::now();
    loop {
        match get() {
            Err(e) if is_inconsistent_read_error(&e) => {
                if start.elapsed() >= timeout {
                    return Err(e);
                }
                std::thread::sleep(interval);
            }
            r => return r,
        }
    }
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
pub fn path_exists(path: &str) -> (bool, Option<io::Error>) {
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

/// `mount.IsNotMountPoint(mounter, file)` (`mount.go:240-290`), over any
/// [`MountInterface`](super::mount_interface::MountInterface): the cheap
/// `IsLikelyNotMountPoint` check, then, when that says "not a mount point",
/// the authoritative `List()` scan (a bind mount of the same device is
/// invisible to the cheap check). A permission error on the cheap check falls
/// back to the scan; any other error is returned.
pub fn is_not_mount_point_with(
    mounter: &dyn super::mount_interface::MountInterface,
    file: &str,
) -> io::Result<bool> {
    let not_mnt = match mounter.is_likely_not_mount_point(file) {
        Ok(v) => v,
        // We were not allowed to do the simple stat() check, e.g. on NFS with
        // root_squash. Fall back to the mount list check below.
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => true,
        Err(e) => return Err(e),
    };
    // identified as mountpoint, so return this fact.
    if !not_mnt {
        return Ok(false);
    }
    // Resolve any symlinks in file, kernel would do the same and use the
    // resolved path in /proc/mounts.
    let resolved = std::fs::canonicalize(file)?;
    let resolved = resolved.to_string_lossy();
    // check all mountpoints since IsLikelyNotMountPoint did not help
    Ok(!mounter.list()?.iter().any(|mp| mp.path == resolved))
}
