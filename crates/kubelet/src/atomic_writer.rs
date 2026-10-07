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
#[cfg_attr(not(test), allow(dead_code))]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Uniform-mode payload, the common shape in these tests.
    fn payload(pairs: &[(&str, &[u8])]) -> BTreeMap<String, FileProjection> {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    FileProjection {
                        fs_user: None,
                        data: v.to_vec(),
                        mode: 0o644,
                    },
                )
            })
            .collect()
    }

    /// A `ConfigMap`/`Secret` volume may set a mode **per item**
    /// (`items[].mode`), which upstream carries in `FileProjection.Mode`
    /// (`pkg/volume/util/atomic_writer.go:64-68`) rather than as one mode for
    /// the whole volume.
    ///
    /// Pins the upstream Conformance spec "ConfigMap should be consumable from
    /// pods in volume with mappings and Item mode set [LinuxOnly]", which sets
    /// `Items[0].Mode = 0400` on a nested path and asserts the projected file's
    /// mode (`test/e2e/common/storage/configmap_volume.go:99-102`, via
    /// `doConfigMapE2EWithMappings`).
    #[tokio::test]
    async fn per_item_mode_is_applied_to_each_projected_file() {
        use std::os::unix::fs::PermissionsExt;

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("aw-mode-{nanos}"));

        // The spec's shape: a nested mapped path carrying its own mode, next to
        // a file that keeps the volume default.
        let mut projections: BTreeMap<String, FileProjection> = BTreeMap::new();
        projections.insert(
            "path/to/data-2".to_string(),
            FileProjection {
                fs_user: None,
                data: b"value-2\n".to_vec(),
                mode: 0o400,
            },
        );
        projections.insert(
            "plain".to_string(),
            FileProjection {
                fs_user: None,
                data: b"value-1\n".to_vec(),
                mode: 0o644,
            },
        );

        write_projected_payload(&dir, &projections).unwrap();

        let nested = dir.join("path/to/data-2");
        assert_eq!(
            std::fs::read(&nested).unwrap(),
            b"value-2\n",
            "nested mapped path must be readable through the ..data symlink"
        );

        let mode = std::fs::metadata(&nested).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o400,
            "items[].mode must reach the projected file (got {mode:o})"
        );

        let plain_mode = std::fs::metadata(dir.join("plain"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            plain_mode, 0o644,
            "a sibling without items[].mode keeps the volume default"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Upstream `newTimestampDir` (`pkg/volume/util/atomic_writer.go:399-413`)
    /// chmods the timestamp dir to 0755 "regardless of the process' umask" so
    /// group/other can recurse the tree; per-file modes are chmod'd likewise
    /// (`writePayloadToDir`, `:435-439`).
    #[tokio::test]
    async fn timestamp_dir_is_0755_regardless_of_umask() {
        use std::os::unix::fs::PermissionsExt;
        extern "C" {
            fn umask(mask: u32) -> u32;
        }
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("aw-umask-{nanos}"));
        // SAFETY: umask(2) is process-global; restored immediately below.
        let old = unsafe { umask(0o077) };
        let res = write_projected_payload(&dir, &payload(&[("a", b"x")]));
        unsafe { umask(old) };
        res.unwrap();
        let ts = std::fs::read_link(dir.join("..data")).unwrap();
        let ts_mode = std::fs::metadata(dir.join(&ts))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(ts_mode & 0o777, 0o755);
        let f_mode = std::fs::metadata(dir.join("a"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(f_mode & 0o777, 0o644);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #2527: plain files/dirs left by an older kubelet are converted to the
    /// `..data` symlink layout on re-SetUp, and then track content changes.
    #[test]
    fn plain_files_from_older_kubelet_are_converted_to_symlinks() {
        let dir = std::env::temp_dir().join(format!("aw-legacy-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("nested/inner"), b"old").unwrap();
        std::fs::write(dir.join("a"), b"old").unwrap();

        let p = payload(&[("a", b"new"), ("nested/inner", b"new")]);
        write_projected_payload(&dir, &p).unwrap();
        for v in ["a", "nested"] {
            assert!(
                std::fs::symlink_metadata(dir.join(v))
                    .unwrap()
                    .file_type()
                    .is_symlink(),
                "{v} must be a symlink"
            );
        }
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), b"new");
        assert_eq!(std::fs::read(dir.join("nested/inner")).unwrap(), b"new");

        let p2 = payload(&[("a", b"newer"), ("nested/inner", b"new")]);
        write_projected_payload(&dir, &p2).unwrap();
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), b"newer");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Core regression: re-projecting an UNCHANGED payload must not touch the
    // user-visible file's mtime/ctime (so a kube-proxy-style config watcher
    // never fires). Also verifies the file is a symlink through `..data` and
    // reads back the projected content.
    #[tokio::test]
    async fn reprojection_is_a_noop_when_unchanged() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("aw-test-{nanos}"));
        let p = payload(&[("config.conf", b"hello=world\n")]);

        write_projected_payload(&dir, &p).unwrap();
        let visible = dir.join("config.conf");
        assert!(std::fs::symlink_metadata(&visible)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read(&visible).unwrap(), b"hello=world\n");

        // ctime of the real file after first projection.
        use std::os::unix::fs::MetadataExt;
        let real = std::fs::canonicalize(&visible).unwrap();
        let ctime1 = std::fs::metadata(&real).unwrap().ctime();
        let data_link1 = std::fs::read_link(dir.join("..data")).unwrap();

        // Re-project identical payload several times.
        for _ in 0..3 {
            write_projected_payload(&dir, &p).unwrap();
        }
        // `..data` must NOT have swapped, and the real file must be untouched.
        let data_link2 = std::fs::read_link(dir.join("..data")).unwrap();
        assert_eq!(
            data_link1, data_link2,
            "..data must not swap when unchanged"
        );
        let ctime2 = std::fs::metadata(&real).unwrap().ctime();
        assert_eq!(
            ctime1, ctime2,
            "unchanged re-projection must not touch the file"
        );

        // A real change swaps ..data and updates content.
        let p2 = payload(&[("config.conf", b"hello=changed\n")]);
        write_projected_payload(&dir, &p2).unwrap();
        assert_eq!(std::fs::read(&visible).unwrap(), b"hello=changed\n");
        assert_ne!(std::fs::read_link(dir.join("..data")).unwrap(), data_link2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `AtomicWriter.Write(payload, setPerms)` step (7)
    /// (`pkg/volume/util/atomic_writer.go:100-125`, `:196-201`): `setPerms` runs
    /// after the payload is written and BEFORE `..data` is swapped to it, and
    /// only when a write actually happens — an unchanged re-projection does not
    /// call it (so the ownership walk never touches an inert volume, #2390).
    #[test]
    fn set_perms_runs_before_data_swap_and_only_on_write() {
        let dir = std::env::temp_dir().join(format!("aw-setperms-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let p = payload(&[("a", b"1")]);
        let calls = std::cell::Cell::new(0);
        let saw_data = std::cell::Cell::new(true);
        let hook = |target: &Path| -> io::Result<()> {
            calls.set(calls.get() + 1);
            saw_data.set(target.join("..data").exists());
            Ok(())
        };
        write_projected_payload_with(&dir, &p, Some(&hook)).unwrap();
        assert_eq!(calls.get(), 1);
        assert!(!saw_data.get(), "setPerms runs before ..data exists");
        write_projected_payload_with(&dir, &p, Some(&hook)).unwrap();
        assert_eq!(
            calls.get(),
            1,
            "an unchanged payload must not call setPerms"
        );
        let p2 = payload(&[("a", b"2")]);
        write_projected_payload_with(&dir, &p2, Some(&hook)).unwrap();
        assert_eq!(calls.get(), 2);
        // A setPerms error aborts the write (`atomic_writer.go:197-200`).
        let p3 = payload(&[("a", b"3")]);
        let failing = |_: &Path| -> io::Result<()> { Err(io::Error::other("boom")) };
        assert!(write_projected_payload_with(&dir, &p3, Some(&failing)).is_err());
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), b"2");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `FileProjection.FsUser` (`atomic_writer.go:67`, applied `:444-450`): the
    /// file is chowned to FsUser after the chmod. Chowning to the process's own
    /// uid is permitted without root, so the syscall path is exercised
    /// hermetically.
    #[test]
    fn fs_user_chowns_the_projected_file() {
        use std::os::unix::fs::MetadataExt;
        let dir = std::env::temp_dir().join(format!("aw-fsuser-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let uid = std::fs::metadata(&dir).unwrap().uid();
        let mut p = payload(&[("a", b"1")]);
        p.get_mut("a").unwrap().fs_user = Some(uid as i64);
        write_projected_payload(&dir, &p).unwrap();
        assert_eq!(std::fs::metadata(dir.join("a")).unwrap().uid(), uid);
        // A chown that cannot succeed is an error, not swallowed
        // (`atomic_writer.go:448-451`); only assertable when not root.
        if uid != 0 {
            let mut bad = payload(&[("b", b"1")]);
            bad.get_mut("b").unwrap().fs_user = Some(0);
            let dir2 = std::env::temp_dir().join(format!("aw-fsuser2-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir2);
            assert!(write_projected_payload(&dir2, &bad).is_err());
            let _ = std::fs::remove_dir_all(&dir2);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
