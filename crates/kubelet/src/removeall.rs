//! Mount-boundary-respecting recursive removal.
//!
//! Port of `pkg/util/removeall/removeall.go`. Behaves like
//! `rm -rf --one-file-system`: it refuses to descend into a mount point, so a
//! stale directory tree can be reaped without ever deleting the *contents* of
//! something still mounted underneath it.
//!
//! The two entry points differ only in the syscall used to remove a leaf, and
//! that difference is the whole safety story:
//!
//! - [`remove_all_one_filesystem`] uses `unlink`/`rmdir` (Go's `os.Remove`), so
//!   it deletes files too.
//! - [`remove_dirs_one_filesystem`] uses `rmdir` only, so it removes empty
//!   directories and **fails on any regular file**.
//!
//! Upstream reaps a pod's `volumes` dir with the second one deliberately:
//! "There should be no files left under normal conditions when this is called,
//! so it effectively does a recursive rmdir instead of RemoveAll to ensure it
//! only removes empty directories and files that were used as mount points, but
//! not content of the mount points" (`pkg/kubelet/kubelet_volumes.go:114-118`).

use std::io;
use std::path::Path;

use crate::pod_dirs::is_likely_not_mount_point;

/// How a leaf is removed. Mirrors the `remove func(string) error` parameter of
/// upstream's `RemoveAllOneFilesystemCommon`.
#[derive(Clone, Copy)]
enum Remover {
    /// `os.Remove` — files and empty directories.
    Any,
    /// `syscall.Rmdir` — empty directories only.
    DirsOnly,
}

impl Remover {
    fn remove(self, path: &Path) -> io::Result<()> {
        match self {
            // std::fs::remove_file maps to unlink(2); fall back to rmdir for a
            // directory, which is what Go's os.Remove does in one call.
            Remover::Any => std::fs::remove_file(path).or_else(|_| std::fs::remove_dir(path)),
            Remover::DirsOnly => std::fs::remove_dir(path),
        }
    }
}

/// Remove `path` and everything under it without crossing a mount boundary,
/// deleting files as well as directories.
///
/// Port of `RemoveAllOneFilesystem` (`removeall.go:113`).
pub fn remove_all_one_filesystem(path: &Path) -> io::Result<()> {
    remove_all_one_filesystem_common(path, Remover::Any)
}

/// Remove `path` and any *empty* subdirectories without crossing a mount
/// boundary. Returns an error if any regular file is encountered.
///
/// Port of `RemoveDirsOneFilesystem` (`removeall.go:126`).
pub fn remove_dirs_one_filesystem(path: &Path) -> io::Result<()> {
    remove_all_one_filesystem_common(path, Remover::DirsOnly)
}

/// Port of `RemoveAllOneFilesystemCommon` (`removeall.go:35`).
///
/// Removes everything it can and returns the first error it encountered. A
/// missing path is not an error.
fn remove_all_one_filesystem_common(path: &Path, remover: Remover) -> io::Result<()> {
    // Simple case: if the remove works, we're done.
    let first_err = match remover.remove(path) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => e,
    };

    // Otherwise, is this a directory we need to recurse into?
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        // ENOTDIR: a parent component is not a directory, so there is nothing
        // here to remove. Upstream treats this the same as "not exist".
        Err(e) if e.kind() == io::ErrorKind::NotADirectory => return Ok(()),
        Err(e) => return Err(e),
    };
    if !meta.is_dir() {
        // Not a directory; return the error from the remove above.
        return Err(first_err);
    }

    // Directory. Never descend through a mount point — that is the
    // "--one-file-system" guarantee.
    if !is_likely_not_mount_point(path)? {
        return Err(io::Error::other(format!(
            "cannot delete directory {}: it is a mount point",
            path.display()
        )));
    }

    let entries = match std::fs::read_dir(path) {
        Ok(e) => e,
        // Race: deleted between the stat and the open.
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    // Remove contents, keeping the first error.
    let mut err: Option<io::Error> = None;
    for entry in entries {
        let res = match entry {
            Ok(entry) => remove_all_one_filesystem_common(&entry.path(), remover),
            Err(e) => Err(e),
        };
        if let (None, Err(e)) = (&err, res) {
            err = Some(e);
        }
    }

    // Remove the now-empty directory itself.
    match remover.remove(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(err.unwrap_or(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("rn-removeall-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn a_missing_path_is_not_an_error() {
        let root = tmp("missing");
        let absent = root.join("nope");
        assert!(remove_all_one_filesystem(&absent).is_ok());
        assert!(remove_dirs_one_filesystem(&absent).is_ok());
    }

    #[test]
    fn an_empty_tree_of_dirs_is_removed_by_both() {
        let root = tmp("emptytree");
        let deep = root.join("a").join("b").join("c");
        std::fs::create_dir_all(&deep).unwrap();
        remove_dirs_one_filesystem(&root.join("a")).unwrap();
        assert!(!root.join("a").exists());
    }

    /// The property the pod-volumes reaper depends on: a regular file must stop
    /// the rmdir-only walk, so mount-point *content* is never deleted.
    #[test]
    fn a_regular_file_blocks_the_dirs_only_walk() {
        let root = tmp("dirsonly");
        let dir = root.join("vol");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("payload");
        std::fs::write(&file, b"real data").unwrap();

        let err = remove_dirs_one_filesystem(&dir).expect_err("must refuse to remove a file");
        assert!(file.exists(), "the file must survive: {err}");
        assert!(dir.exists(), "its directory must survive too");
    }

    /// The same tree is removable when files are allowed.
    #[test]
    fn the_same_tree_is_removed_when_files_are_allowed() {
        let root = tmp("any");
        let dir = root.join("vol");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("payload"), b"scratch").unwrap();

        remove_all_one_filesystem(&dir).unwrap();
        assert!(!dir.exists());
    }
}
