//! Port of `pkg/volume/util/hostutil` (`hostutil_linux.go`).

use std::io;

/// `HostUtil.GetOwner` (`hostutil_linux.go:267-274`): the uid and gid of
/// `pathname` after symlink evaluation.
pub fn get_owner(pathname: &str) -> io::Result<(i64, i64)> {
    use std::os::unix::fs::MetadataExt;
    let realpath = std::fs::canonicalize(pathname)?;
    let md = std::fs::metadata(realpath)?;
    Ok((md.uid() as i64, md.gid() as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    #[test]
    fn get_owner_follows_symlinks() {
        let d = tempfile::tempdir().unwrap();
        let target = d.path().join("t");
        std::fs::create_dir(&target).unwrap();
        let link = d.path().join("l");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let md = std::fs::metadata(&target).unwrap();
        assert_eq!(
            get_owner(link.to_str().unwrap()).unwrap(),
            (md.uid() as i64, md.gid() as i64)
        );
    }

    #[test]
    fn get_owner_missing_errs() {
        assert!(get_owner("/definitely/not/here").is_err());
    }
}
