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
pub const RW_MASK: u32 = 0o660;
/// `roMask` (`volume_linux.go:45`).
pub const RO_MASK: u32 = 0o440;
/// `execMask` (`volume_linux.go:46`).
pub const EXEC_MASK: u32 = 0o110;
/// `os.ModeSetgid` expressed as the `S_ISGID` mode bit.
pub const SETGID: u32 = 0o2000;

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
pub fn requires_permission_change(root: &Path, fs_group: i64, read_only: bool) -> bool {
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
