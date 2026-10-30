//! Unit tests moved from `src/atomic_writer.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::BTreeMap;
    pub use std::io;
    pub use std::os::unix::fs::PermissionsExt;
    pub use std::path::{Path, PathBuf};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::atomic_writer::*;

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
