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

/// Project `payload` (relative user-visible path -> bytes) into `target_dir`,
/// atomically and idempotently, applying `file_mode` to each projected file.
///
/// No-op (no writes, no chmod, no symlink swap) when the on-disk `..data`
/// payload already equals `payload` — the property that keeps kube-proxy and
/// other config-file watchers stable across the kubelet's periodic re-SetUp.
pub fn write_payload(
    target_dir: &Path,
    payload: &BTreeMap<String, Vec<u8>>,
    file_mode: u32,
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

        for (rel, content) in payload {
            let dest = ts_dir.join(rel);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, content)?;
            std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(file_mode))?;
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
    for rel in payload.keys() {
        let seg = rel.split('/').next().unwrap_or(rel.as_str());
        let link = target_dir.join(seg);
        if std::fs::symlink_metadata(&link).is_err() {
            std::os::unix::fs::symlink(PathBuf::from(DATA_DIR).join(seg), &link)?;
        }
    }

    Ok(())
}

/// True when `payload` differs from what is stored under `old_ts_path` — any
/// file whose bytes differ or is missing, or an extra file present on disk that
/// is no longer in the payload.
fn payload_differs(payload: &BTreeMap<String, Vec<u8>>, old_ts_path: &Path) -> bool {
    for (rel, content) in payload {
        match std::fs::read(old_ts_path.join(rel)) {
            Ok(existing) if existing == *content => {}
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

    fn payload(pairs: &[(&str, &[u8])]) -> BTreeMap<String, Vec<u8>> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_vec()))
            .collect()
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

        write_payload(&dir, &p, 0o644).unwrap();
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
            write_payload(&dir, &p, 0o644).unwrap();
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
        write_payload(&dir, &p2, 0o644).unwrap();
        assert_eq!(std::fs::read(&visible).unwrap(), b"hello=changed\n");
        assert_ne!(std::fs::read_link(dir.join("..data")).unwrap(), data_link2);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
