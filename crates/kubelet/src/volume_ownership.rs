//! Port of `pkg/volume/volume_linux.go` `VolumeOwnership.ChangePermissions`
//! (`:61-79`, `:100-120`, `:147-181`), the `SetVolumeOwnership` that a volume
//! plugin runs when the pod sets `fsGroup`.
//!
//! `fsGroupChangePolicy` / `skipPermissionChange` / `requiresPermissionChange`
//! (`:183-229`) are ported in [`set_volume_ownership_with_policy`]; every
//! in-tree plugin ported so far passes `nil` for the policy (`projected.go:208`,
//! `empty_dir.go:277`). The progress monitor (`:81-98`, `:123-145`) is not
//! ported.
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
    set_volume_ownership_with_policy(dir, fs_group, None, read_only)
}

/// `ChangePermissions` with a `fsGroupChangePolicy` (`volume_linux.go:61-79`):
/// `skipPermissionChange` short-circuits the walk when the policy is
/// `OnRootMismatch` and the root already matches (`:183-229`). Any other policy
/// (`Always`, unset) walks recursively.
pub fn set_volume_ownership_with_policy(
    dir: &Path,
    fs_group: Option<i64>,
    fs_group_change_policy: Option<&str>,
    read_only: bool,
) -> io::Result<()> {
    let Some(fs_group) = fs_group else {
        return Ok(());
    };
    if skip_permission_change(dir, fs_group, fs_group_change_policy, read_only) {
        return Ok(());
    }
    walk_deep(dir, &mut |path, meta| {
        change_file_permission(path, fs_group, read_only, meta)
    })
}

/// The `setPerms` closure the configMap, secret, downwardAPI and projected
/// plugins each build around `writer.Write(payload, setPerms)`
/// (`configmap.go:246-252`, `secret.go:242-248`, `downwardapi.go:217-223`,
/// `projected.go:200-214`): "change the permissions on the whole volume and not
/// only in the timestamp directory", then write through the AtomicWriter.
/// All four volumes report `ReadOnly: true` (`GetAttributes`), so `read_only`
/// is `true` for each caller.
pub fn write_payload_with_ownership(
    dir: &Path,
    payload: &std::collections::BTreeMap<String, crate::atomic_writer::FileProjection>,
    fs_group: Option<i64>,
    read_only: bool,
) -> io::Result<()> {
    let set_perms =
        move |dir: &Path| -> io::Result<()> { set_volume_ownership(dir, fs_group, read_only) };
    crate::atomic_writer::write_projected_payload_with(dir, payload, Some(&set_perms))
}

/// `skipPermissionChange` (`volume_linux.go:183-189`).
fn skip_permission_change(
    dir: &Path,
    fs_group: i64,
    policy: Option<&str>,
    read_only: bool,
) -> bool {
    if policy != Some("OnRootMismatch") {
        return false;
    }
    !requires_permission_change(dir, fs_group, read_only)
}

/// `requiresPermissionChange` (`volume_linux.go:191-229`): the root must be a
/// directory owned by `fsGroup`, setgid, with a permission superset of
/// `rwMask|execMask` (`roMask|execMask` when read-only).
fn requires_permission_change(root: &Path, fs_group: i64, read_only: bool) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Ok(meta) = std::fs::metadata(root) else {
        return true;
    };
    if i64::from(meta.gid()) != fs_group {
        return true;
    }
    if !meta.is_dir() {
        return true;
    }
    let want = (if read_only { RO_MASK } else { RW_MASK }) | EXEC_MASK;
    let have = meta.permissions().mode() & 0o777;
    (want & have != want) || (meta.permissions().mode() & SETGID == 0)
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

    fn own_gid(p: &Path) -> i64 {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(p).unwrap().gid() as i64
    }

    #[test]
    fn on_root_mismatch_skips_when_root_already_matches() {
        let d = tmp("orm-skip");
        let gid = own_gid(&d);
        // A conforming root, with a child that a recursive walk would change.
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o2770)).unwrap();
        let f = d.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        set_volume_ownership_with_policy(&d, Some(gid), Some("OnRootMismatch"), false).unwrap();
        assert_eq!(
            mode(&f),
            0o600,
            "OnRootMismatch with matching root must not walk"
        );
    }

    #[test]
    fn on_root_mismatch_walks_when_root_lacks_setgid() {
        let d = tmp("orm-walk");
        let gid = own_gid(&d);
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o770)).unwrap();
        let f = d.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        set_volume_ownership_with_policy(&d, Some(gid), Some("OnRootMismatch"), false).unwrap();
        assert_eq!(mode(&f), 0o660);
        assert_eq!(mode(&d) & 0o2000, 0o2000);
    }

    #[test]
    fn always_policy_walks_even_when_root_matches() {
        let d = tmp("always");
        let gid = own_gid(&d);
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o2770)).unwrap();
        let f = d.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        set_volume_ownership_with_policy(&d, Some(gid), Some("Always"), false).unwrap();
        assert_eq!(mode(&f), 0o660);
    }

    #[test]
    fn requires_change_for_root_readonly_superset_check() {
        let d = tmp("ro-superset");
        let gid = own_gid(&d);
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o2550)).unwrap();
        // roMask|execMask = 0550 is satisfied; rwMask|execMask = 0770 is not.
        assert!(!requires_permission_change(&d, gid, true));
        assert!(requires_permission_change(&d, gid, false));
        assert!(requires_permission_change(&d, gid + 1, true));
    }

    /// `verifyDirectoryPermission` (`volume_linux_test.go:474-494`).
    fn verify_directory_permission(p: &Path, read_only: bool) -> bool {
        let Ok(m) = std::fs::symlink_metadata(p) else {
            return false;
        };
        let want = (if read_only { RO_MASK } else { RW_MASK }) | EXEC_MASK;
        let have = m.permissions().mode();
        (want & (have & 0o777) == want) && (have & SETGID != 0)
    }

    /// `TestSetVolumeOwnershipMode` "fsgroupchangepolicy=always"
    /// (`volume_linux_test.go:197-229`): a subdirectory without setgid is fixed.
    #[test]
    fn upstream_mode_always_fixes_rogue_subdir() {
        let d = tmp("up-always");
        let gid = own_gid(&d);
        let m = mode(&d);
        std::fs::set_permissions(
            &d,
            std::fs::Permissions::from_mode(m | RW_MASK | SETGID | EXEC_MASK),
        )
        .unwrap();
        let rogue = d.join("roguedir");
        std::fs::create_dir(&rogue).unwrap();
        std::fs::set_permissions(&rogue, std::fs::Permissions::from_mode(m & !SETGID)).unwrap();
        set_volume_ownership_with_policy(&d, Some(gid), Some("Always"), false).unwrap();
        assert!(verify_directory_permission(&rogue, false));
        let _ = std::fs::remove_dir_all(&d);
    }

    /// "onrootmismatch,rootdir=validperm" (`:230-262`): a valid root means the
    /// rogue subdirectory is left alone.
    #[test]
    fn upstream_mode_on_root_mismatch_valid_root_skips_subdir() {
        let d = tmp("up-orm-valid");
        let gid = own_gid(&d);
        let m = mode(&d);
        std::fs::set_permissions(
            &d,
            std::fs::Permissions::from_mode(m | RW_MASK | SETGID | EXEC_MASK),
        )
        .unwrap();
        let rogue = d.join("roguedir");
        std::fs::create_dir(&rogue).unwrap();
        std::fs::set_permissions(&rogue, std::fs::Permissions::from_mode(RW_MASK)).unwrap();
        set_volume_ownership_with_policy(&d, Some(gid), Some("OnRootMismatch"), false).unwrap();
        assert!(!verify_directory_permission(&rogue, false));
        let _ = std::fs::remove_dir_all(&d);
    }

    /// "onrootmismatch,rootdir=invalidperm" (`:263-293`): root 0770 lacks
    /// setgid, so the walk runs and the subdirectory is fixed.
    #[test]
    fn upstream_mode_on_root_mismatch_invalid_root_walks() {
        let d = tmp("up-orm-invalid");
        let gid = own_gid(&d);
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o770)).unwrap();
        let rogue = d.join("roguedir");
        std::fs::create_dir(&rogue).unwrap();
        std::fs::set_permissions(&rogue, std::fs::Permissions::from_mode(RW_MASK)).unwrap();
        set_volume_ownership_with_policy(&d, Some(gid), Some("OnRootMismatch"), false).unwrap();
        assert!(verify_directory_permission(&rogue, false));
        let _ = std::fs::remove_dir_all(&d);
    }

    /// `TestSetVolumeOwnershipOwner` "symlink" (`:555-583`), runnable as a
    /// non-root user by chowning to the caller's own gid: the link itself is
    /// lchown'd (`Lstat` owner), and the walk succeeds on it.
    #[test]
    fn upstream_owner_symlink_is_lchowned_not_followed() {
        let d = tmp("up-owner-symlink");
        let gid = own_gid(&d);
        let f = d.join("file.txt");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o755)).unwrap();
        let link = d.join("file_link.txt");
        std::os::unix::fs::symlink(&f, &link).unwrap();
        set_volume_ownership_with_policy(&d, Some(gid), Some("Always"), false).unwrap();
        let lm = std::fs::symlink_metadata(&link).unwrap();
        assert!(lm.file_type().is_symlink());
        assert_eq!(i64::from(lm.gid()), gid);
        assert_eq!(
            i64::from(lm.uid()),
            i64::from(std::fs::metadata(&f).unwrap().uid())
        );
        // The target was reached once as a regular file: 0755 | rwMask.
        assert_eq!(mode(&f), 0o775);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// `TestSetVolumeOwnershipOwner` "fsGroup=nil" (`:520-541`): owner and
    /// group are untouched.
    #[test]
    fn upstream_owner_nil_fs_group_keeps_owner() {
        let d = tmp("up-owner-nil");
        let f = d.join("file.txt");
        std::fs::write(&f, b"x").unwrap();
        let before = std::fs::metadata(&f).unwrap();
        set_volume_ownership_with_policy(&d, None, Some("Always"), false).unwrap();
        let after = std::fs::metadata(&f).unwrap();
        assert_eq!((before.uid(), before.gid()), (after.uid(), after.gid()));
        let _ = std::fs::remove_dir_all(&d);
    }
}
