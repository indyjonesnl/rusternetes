//! Unit tests moved from `src/go_selinux.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::sync::OnceLock;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::go_selinux::*;

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
