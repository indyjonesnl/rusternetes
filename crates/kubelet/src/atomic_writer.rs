//! Faithful port of the upstream kubelet AtomicWriter
//! (`k8s.io/kubernetes/pkg/volume/util/atomic_writer.go`).
//!
//! Projects a set of files into a target directory so that:
//!   * re-projecting an UNCHANGED payload makes ZERO filesystem changes, and
//!   * user-visible files are symlinks through a `..data` symlink that is
//!     swapped atomically, and only when the content actually changes.
//!
//! This is *why* re-running volume SetUp is inert upstream: a watcher such as
//! kube-proxy — which exits on ANY change to its mounted config file
//! ("content of the proxy server's configuration file was updated") — never
//! sees an event unless the projected content genuinely changed. Writing plain
//! files and re-writing/re-chmod'ing them (even with identical bytes) is what
//! crash-loops such watchers; this layout avoids it on every distro.
//!
//! Unix-only (the kubelet runs on Linux nodes).

use std::collections::BTreeMap;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

const DATA_DIR: &str = "..data";
const NEW_DATA_DIR: &str = "..data_tmp";

/// One projected file: its bytes and **its own** mode.
///
/// Port of upstream `FileProjection` (`pkg/volume/util/atomic_writer.go:64-68`).
/// The mode is per file because a
/// ConfigMap/Secret volume may set `items[].mode` on individual keys while the
/// rest of the volume keeps `defaultMode` — collapsing it to one mode for the
/// whole volume makes `items[].mode` unrepresentable.
///
/// `fs_user` is upstream's `FsUser *int64` (`:67`): when set, the file is
/// chowned to it after the chmod (`:444-450`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileProjection {
    pub data: Vec<u8>,
    pub mode: u32,
    pub fs_user: Option<i64>,
}

/// Upstream's `setPerms func(subPath string) error` argument to
/// `AtomicWriter.Write` (`atomic_writer.go:155`). It receives the volume's
/// target dir (the projected plugin's callback re-owns the whole volume, not
/// only the new timestamp dir — `projected.go:200-206`).
pub type SetPerms<'a> = &'a dyn Fn(&Path) -> io::Result<()>;

/// Project `payload` (relative user-visible path -> [`FileProjection`]) into
/// `target_dir`, atomically and idempotently, applying each entry's own mode.
///
/// This is the shape upstream's `AtomicWriter.Write` takes
/// (`map[string]FileProjection`), so a volume with mixed per-item modes
/// projects in one pass.
///
/// No-op (no writes, no chmod, no symlink swap) when the on-disk `..data`
/// payload already equals `payload` — the property that keeps kube-proxy and
/// other config-file watchers stable across the kubelet's periodic re-SetUp.
pub fn write_projected_payload(
    target_dir: &Path,
    payload: &BTreeMap<String, FileProjection>,
) -> io::Result<()> {
    write_projected_payload_with(target_dir, payload, None)
}

/// [`write_projected_payload`] with upstream's `setPerms` hook
/// (`atomic_writer.go:100-125` step 7, `:196-201`): called after the payload is
/// written to the new timestamp dir and before `..data` is swapped to it, only
/// when a write happens; its error aborts the write.
pub fn write_projected_payload_with(
    target_dir: &Path,
    payload: &BTreeMap<String, FileProjection>,
    set_perms: Option<SetPerms<'_>>,
) -> io::Result<()> {
    std::fs::create_dir_all(target_dir)?;
    let data_link = target_dir.join(DATA_DIR);

    // Timestamped dir currently referenced by `..data`, if any.
    let old_ts: Option<PathBuf> = std::fs::read_link(&data_link).ok();

    // shouldWrite: if `..data` exists, compare the payload against it; write
    // only when something differs (content changed, a key added, or removed).
    let mut should_write = true;
    if let Some(ref old) = old_ts {
        let old_path = target_dir.join(old);
        should_write = payload_differs(payload, &old_path);
    }

    if should_write {
        let ts_name = new_timestamp_dirname();
        let ts_dir = target_dir.join(&ts_name);
        std::fs::create_dir(&ts_dir)?;
        // 0755 so group/other can traverse into the data dir (upstream parity).
        std::fs::set_permissions(&ts_dir, std::fs::Permissions::from_mode(0o755))?;

        for (rel, projection) in payload {
            let dest = ts_dir.join(rel);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, &projection.data)?;
            // Per-file mode: `items[].mode` when the volume set one for this
            // key, else the volume's defaultMode (upstream FileProjection.Mode).
            std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(projection.mode))?;
            // `FsUser` (`atomic_writer.go:444-450`): chown(fsUser, -1).
            if let Some(uid) = projection.fs_user {
                rustix::fs::chownat(
                    rustix::fs::CWD,
                    &dest,
                    Some(rustix::fs::Uid::from_raw(uid as u32)),
                    None,
                    rustix::fs::AtFlags::empty(),
                )
                .map_err(io::Error::from)?;
            }
        }

        // (7) `setPerms` (`atomic_writer.go:196-201`).
        if let Some(set_perms) = set_perms {
            set_perms(target_dir)?;
        }

        // Atomically point `..data` at the new ts dir: create `..data_tmp`
        // symlink then rename over `..data` (rename is atomic on the same fs).
        let new_link = target_dir.join(NEW_DATA_DIR);
        let _ = std::fs::remove_file(&new_link);
        std::os::unix::fs::symlink(&ts_name, &new_link)?;
        std::fs::rename(&new_link, &data_link)?;

        // Remove the previous ts dir now that nothing points to it.
        if let Some(ref old) = old_ts {
            let _ = std::fs::remove_dir_all(target_dir.join(old));
        }
    }

    // Ensure the user-visible symlink for each payload entry exists
    // (`<first-path-segment>` -> `..data/<first-path-segment>`). Relative so it
    // resolves inside a bind mount. Runs even when should_write is false, per
    // upstream (kubernetes #121472).
    let mut wanted: std::collections::HashSet<String> = std::collections::HashSet::new();
    for rel in payload.keys() {
        let seg = rel.split('/').next().unwrap_or(rel.as_str());
        wanted.insert(seg.to_string());
        let link = target_dir.join(seg);
        match std::fs::symlink_metadata(&link) {
            Ok(md) if md.file_type().is_symlink() => {}
            Ok(md) => {
                // Rusternetes-only deviation (#2527): upstream's
                // `createUserVisibleFiles` (`atomic_writer.go`) only creates
                // the link when `os.Readlink` reports ENOENT, so a plain file
                // left by an older kubelet (EINVAL) is skipped. Upstream never
                // meets one (its secret/configMap/downwardAPI/projected volumes
                // are tmpfs, wiped on restart); ours persist on disk, so a
                // stale plain file would shadow `..data` and never update.
                if md.is_dir() {
                    std::fs::remove_dir_all(&link)?;
                } else {
                    std::fs::remove_file(&link)?;
                }
                std::os::unix::fs::symlink(PathBuf::from(DATA_DIR).join(seg), &link)?;
            }
            Err(_) => {
                std::os::unix::fs::symlink(PathBuf::from(DATA_DIR).join(seg), &link)?;
            }
        }
    }

    // Prune stale user-visible entries no longer in the payload (a key removed
    // from the ConfigMap/Secret), mirroring upstream `removeUserVisiblePaths`.
    // The internal dotfiles (`..data`, `..data_tmp`, timestamped `..` dirs) are
    // never touched.
    if let Ok(entries) = std::fs::read_dir(target_dir) {
        for e in entries.flatten() {
            if let Some(name) = e.file_name().to_str() {
                if !name.starts_with("..") && !wanted.contains(name) {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
    }

    Ok(())
}

/// True when `payload` differs from what is stored under `old_ts_path` — any
/// file whose bytes differ or is missing, or an extra file present on disk that
/// is no longer in the payload.
fn payload_differs(payload: &BTreeMap<String, FileProjection>, old_ts_path: &Path) -> bool {
    for (rel, projection) in payload {
        // Content only, not mode — upstream's `shouldWriteFile` compares bytes
        // (`atomic_writer.go`), so a mode-only change does not trigger a
        // rewrite there either. Comparing mode here would also break the
        // unchanged-re-projection no-op that config-file watchers depend on
        // (#1652).
        match std::fs::read(old_ts_path.join(rel)) {
            Ok(existing) if existing == projection.data => {}
            _ => return true,
        }
    }
    // Detect removed keys: any regular file under old_ts not in the payload.
    if let Ok(entries) = std::fs::read_dir(old_ts_path) {
        for e in entries.flatten() {
            if let Some(name) = e.file_name().to_str() {
                if !payload.keys().any(|k| k.split('/').next() == Some(name)) {
                    return true;
                }
            }
        }
    }
    false
}

/// `..YYYY_MM_DD_HH_MM_SS.<nanos>` — mirrors upstream's `MkdirTemp` timestamp
/// prefix. A new ts dir is only created on an actual content change, so the
/// nanosecond suffix is ample to avoid collisions.
fn new_timestamp_dirname() -> String {
    format!("..{}", chrono::Utc::now().format("%Y_%m_%d_%H_%M_%S.%9f"))
}
