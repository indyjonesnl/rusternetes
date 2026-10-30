//! Unit tests moved from `src/removeall.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_kubelet::pod_dirs::is_likely_not_mount_point;
    pub use std::io;
    pub use std::path::Path;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::removeall::*;

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
