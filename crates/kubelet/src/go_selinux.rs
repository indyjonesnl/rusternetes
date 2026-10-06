//! Port of the `GetEnabled` slice of `github.com/opencontainers/selinux/go-selinux`
//! (`selinux_linux.go`), the library `pkg/volume/util/selinux.go` and
//! `pkg/kubelet/kuberuntime/kuberuntime_container.go` call as `selinux.GetEnabled()`.
//!
//! Upstream: `selinuxState.getEnabled` (`selinux_linux.go:104-120`) is true when
//! an selinuxfs is mounted AND the process label (`/proc/thread-self/attr/current`)
//! is not `"kernel"`. The mount point is found by `findSELinuxfs`
//! (`:151-185`): the default `/sys/fs/selinux` if it is a real, writable
//! selinuxfs, otherwise the `selinuxfs` entry of `/proc/self/mountinfo`
//! (`findSELinuxfsMount`, `:187-204`), but only if `/proc/filesystems` lists
//! `selinuxfs`. The result is computed once per process (`enabledSet`).

use std::sync::OnceLock;

/// `selinuxfsMount` (`selinux_linux.go:34`).
const SELINUXFS_MOUNT: &str = "/sys/fs/selinux";
/// `unix.SELINUX_MAGIC`.
const SELINUX_MAGIC: u64 = 0xf97c_ff8c;
/// `unix.ST_RDONLY`.
const ST_RDONLY: u64 = 1;

/// `findSELinuxfsMount` (`selinux_linux.go:187-204`): the mount point (5th
/// mountinfo field) of the first line whose fs type is `selinuxfs`.
pub fn find_selinuxfs_mount(mountinfo: &str) -> Option<String> {
    for line in mountinfo.lines() {
        if !line.contains(" - selinuxfs ") {
            continue;
        }
        // `bytes.SplitN(txt, " ", mPos+1)` with mPos = 5; needs 6 fields.
        let fields: Vec<&str> = line.splitn(6, ' ').collect();
        if fields.len() < 6 {
            continue;
        }
        return Some(fields[4].to_string());
    }
    None
}

/// The `/proc/filesystems` gate of `findSELinuxfs` (`:162`):
/// `bytes.Contains(fs, []byte("\tselinuxfs\n"))`.
pub fn proc_filesystems_has_selinuxfs(filesystems: &str) -> bool {
    filesystems.contains("\tselinuxfs\n")
}

/// The decision of `getEnabled` (`selinux_linux.go:113-118`), given the
/// already-discovered inputs: an selinuxfs mount point exists and the current
/// label is not `"kernel"`.
pub fn enabled_from(selinuxfs_mount: Option<&str>, current_label: &str) -> bool {
    matches!(selinuxfs_mount, Some(fs) if !fs.is_empty()) && current_label != "kernel"
}

/// `verifySELinuxfsMount` (`selinux_linux.go:127-149`).
fn verify_selinuxfs_mount(mnt: &str) -> bool {
    match rustix::fs::statfs(mnt) {
        Ok(buf) => buf.f_type as u64 == SELINUX_MAGIC && (buf.f_flags as u64 & ST_RDONLY) == 0,
        Err(_) => false,
    }
}

/// `findSELinuxfs` (`selinux_linux.go:151-185`).
fn find_selinuxfs() -> Option<String> {
    // fast path: check the default mount first
    if verify_selinuxfs_mount(SELINUXFS_MOUNT) {
        return Some(SELINUXFS_MOUNT.to_string());
    }
    // check if selinuxfs is available before going the slow path
    let fs = std::fs::read_to_string("/proc/filesystems").ok()?;
    if !proc_filesystems_has_selinuxfs(&fs) {
        return None;
    }
    // slow path: try to find among the mounts
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    find_selinuxfs_mount(&mountinfo)
}

/// `readConFd` (`selinux_linux.go:258-264`): the contents minus one trailing NUL.
fn current_label() -> String {
    let data = std::fs::read("/proc/thread-self/attr/current")
        .or_else(|_| std::fs::read("/proc/self/attr/current"))
        .unwrap_or_default();
    let data = data.strip_suffix(&[0]).unwrap_or(&data);
    String::from_utf8_lossy(data).into_owned()
}

/// `selinux.GetEnabled()` (`selinux_linux.go:224-226`), cached for the life of
/// the process like upstream's `enabledSet`.
pub fn get_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let mnt = find_selinuxfs();
        enabled_from(mnt.as_deref(), &current_label())
    })
}

/// CRI `Mount.selinux_relabel` for a mount the kubelet requests relabelled.
/// `enabled` is `selinux.GetEnabled()`; upstream computes
/// `v.SELinuxRelabel && selinux.GetEnabled()`
/// (`pkg/kubelet/kuberuntime/kuberuntime_container.go:484`).
pub fn relabel_if_enabled(requested: bool, enabled: bool) -> bool {
    requested && enabled
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mountinfo_selinuxfs_mount_point_is_fifth_field() {
        let mi = "22 21 0:19 / /sys/kernel/security rw - securityfs securityfs rw\n\
                  30 22 0:20 / /sys/fs/selinux rw,relatime shared:12 - selinuxfs selinuxfs rw\n";
        assert_eq!(find_selinuxfs_mount(mi).as_deref(), Some("/sys/fs/selinux"));
        assert_eq!(
            find_selinuxfs_mount("1 2 0:1 / / rw - ext4 /dev/sda rw\n"),
            None
        );
    }

    #[test]
    fn proc_filesystems_gate_matches_upstream_bytes() {
        assert!(proc_filesystems_has_selinuxfs(
            "nodev\tsysfs\nnodev\tselinuxfs\n\text4\n"
        ));
        assert!(!proc_filesystems_has_selinuxfs("nodev\tsysfs\n\text4\n"));
    }

    #[test]
    fn enabled_needs_mount_and_non_kernel_label() {
        assert!(enabled_from(
            Some("/sys/fs/selinux"),
            "system_u:system_r:kubelet_t:s0"
        ));
        assert!(!enabled_from(Some("/sys/fs/selinux"), "kernel"));
        assert!(!enabled_from(None, "system_u:system_r:kubelet_t:s0"));
        assert!(!enabled_from(Some(""), "x"));
    }

    #[test]
    fn relabel_requires_request_and_enabled() {
        assert!(relabel_if_enabled(true, true));
        assert!(!relabel_if_enabled(true, false));
        assert!(!relabel_if_enabled(false, true));
    }
}
