//! Port of `pkg/volume/volume_linux.go` `VolumeOwnership.ChangePermissions`
//! (`:61-79`, `:100-120`, `:147-181`), the `SetVolumeOwnership` that a volume
//! plugin runs when the pod sets `fsGroup`.
//!
//! Not ported: `fsGroupChangePolicy` / `skipPermissionChange` (`:183-229`) and
//! the progress monitor (`:81-98`, `:123-145`) — the projected plugin passes
//! `nil` for the policy (`projected.go:208`).
//!
//! Deliberate deviation: upstream's `changeFilePermission` logs an `Lchown` /
//! `Chmod` failure and returns nil (`:151-175`); here a failure is returned, as
//! the existing in-process fsGroup path in `volumes.rs` already does, so a pod
//! never starts against a volume it cannot read.

use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// `rwMask` (`volume_linux.go:44`).
const RW_MASK: u32 = 0o660;
/// `roMask` (`volume_linux.go:45`).
const RO_MASK: u32 = 0o440;
/// `execMask` (`volume_linux.go:46`).
const EXEC_MASK: u32 = 0o110;
/// `os.ModeSetgid` expressed as the `S_ISGID` mode bit.
const SETGID: u32 = 0o2000;

/// `NewVolumeOwnership(..).ChangePermissions()` (`volume_linux.go:61-79`): a
/// no-op with no fsGroup, otherwise `changePermissionsRecursively`.
pub fn set_volume_ownership(dir: &Path, fs_group: Option<i64>, read_only: bool) -> io::Result<()> {
    let Some(fs_group) = fs_group else {
        return Ok(());
    };
    walk_deep(dir, &mut |path, meta| {
        change_file_permission(path, fs_group, read_only, meta)
    })
}

/// `changeFilePermission` (`volume_linux.go:147-181`): `Lchown(-1, fsGroup)`,
/// skip `chmod` for a symlink, else `chmod(mode | mask)` where the mask is
/// `roMask` for a read-only volume else `rwMask`, plus `setgid|execMask` on a
/// directory.
fn change_file_permission(
    path: &Path,
    fs_group: i64,
    read_only: bool,
    meta: &std::fs::Metadata,
) -> io::Result<()> {
    rustix::fs::chownat(
        rustix::fs::CWD,
        path,
        None,
        Some(rustix::fs::Gid::from_raw(fs_group as u32)),
        rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
    )
    .map_err(io::Error::from)?;

    if meta.file_type().is_symlink() {
        return Ok(());
    }

    let mut mask = if read_only { RO_MASK } else { RW_MASK };
    if meta.is_dir() {
        mask |= SETGID | EXEC_MASK;
    }
    std::fs::set_permissions(
        path,
        std::fs::Permissions::from_mode(meta.mode_bits() | mask),
    )
}

trait ModeBits {
    fn mode_bits(&self) -> u32;
}

impl ModeBits for std::fs::Metadata {
    fn mode_bits(&self) -> u32 {
        // Permission bits plus setuid/setgid/sticky, as Go's `FileMode` carries
        // them across `os.Chmod` (`info.Mode()|mask`).
        self.permissions().mode() & 0o7777
    }
}

/// `walkDeep` (`volume_linux.go:259-296`): `Lstat` based (symlinks are never
/// followed), and the callback runs on a directory AFTER its children.
fn walk_deep(
    root: &Path,
    f: &mut dyn FnMut(&Path, &std::fs::Metadata) -> io::Result<()>,
) -> io::Result<()> {
    let meta = std::fs::symlink_metadata(root)?;
    walk(root, &meta, f)
}

fn walk(
    path: &Path,
    meta: &std::fs::Metadata,
    f: &mut dyn FnMut(&Path, &std::fs::Metadata) -> io::Result<()>,
) -> io::Result<()> {
    if meta.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let child = entry?.path();
            let child_meta = std::fs::symlink_metadata(&child)?;
            walk(&child, &child_meta, f)?;
        }
    }
    f(path, meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("volown-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn mode(p: &Path) -> u32 {
        std::fs::symlink_metadata(p).unwrap().permissions().mode() & 0o7777
    }

    /// `ChangePermissions` (`volume_linux.go:62-64`): no fsGroup, no change.
    #[test]
    fn no_fs_group_changes_nothing() {
        let d = tmp("nofsg");
        let f = d.join("token");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        set_volume_ownership(&d, None, true).unwrap();
        assert_eq!(mode(&f), 0o600);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// The conformance case `service_accounts.go:368` "fsGroup": the token is
    /// written 0600 (`projected.go:279-282`) and `changeFilePermission` ORs
    /// `roMask` (0440) for the projected volume (`GetAttributes().ReadOnly`,
    /// `projected.go:175-181`) => `-rw-r-----` (0640), NOT 0660.
    #[test]
    fn read_only_volume_ors_ro_mask_so_0600_becomes_0640() {
        let d = tmp("ro");
        let f = d.join("token");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        let gid = std::fs::metadata(&f).unwrap().gid() as i64;
        set_volume_ownership(&d, Some(gid), true).unwrap();
        assert_eq!(mode(&f), 0o640);
        // The root dir gets setgid and execMask: `mask |= ModeSetgid; mask |= execMask`.
        let dm = mode(&d);
        assert_eq!(dm & SETGID, SETGID, "root dir must be setgid");
        assert_eq!(dm & 0o110, 0o110, "dir gets execMask (0110)");
        assert_eq!(dm & 0o440, 0o440, "dir gets roMask");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// A read-write volume ORs `rwMask` (0660).
    #[test]
    fn read_write_volume_ors_rw_mask() {
        let d = tmp("rw");
        let f = d.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        let gid = std::fs::metadata(&f).unwrap().gid() as i64;
        set_volume_ownership(&d, Some(gid), false).unwrap();
        assert_eq!(mode(&f), 0o660);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// `changeFilePermission` (`volume_linux.go:157-164`): a symlink is
    /// lchown'd but never chmod'd — chmod would pass through to its target and
    /// override the plugin's `defaultMode`. The AtomicWriter's `..data` and
    /// user-visible entries are symlinks, so the target keeps its own mode.
    #[test]
    fn symlinks_are_not_chmodded_and_not_followed() {
        let d = tmp("symlink");
        let ts = d.join("..ts");
        std::fs::create_dir_all(&ts).unwrap();
        let real = ts.join("token");
        std::fs::write(&real, b"x").unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o400)).unwrap();
        std::os::unix::fs::symlink("..ts", d.join("..data")).unwrap();
        std::os::unix::fs::symlink("..data/token", d.join("token")).unwrap();
        let gid = std::fs::metadata(&real).unwrap().gid() as i64;
        // rw volume: if the symlink were chmod'd through, 0400 would become 0660.
        set_volume_ownership(&d, Some(gid), false).unwrap();
        // The real file is reached once, as a regular file inside `..ts`
        // (0400|0660 = 0660), never twice via the links; the links stay links.
        assert!(std::fs::symlink_metadata(d.join("token"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(mode(&real), 0o660);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// `walkDeep` (`volume_linux.go:252-255`): a failure is surfaced.
    #[test]
    fn missing_dir_is_an_error() {
        let d = std::env::temp_dir().join(format!("volown-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        assert!(set_volume_ownership(&d, Some(0), true).is_err());
    }
}
