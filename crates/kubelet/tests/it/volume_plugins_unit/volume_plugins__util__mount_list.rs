//! Unit tests moved from `src/volume_plugins/util/mount_list.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_kubelet::volume_plugins::util::mount_interface::MountInterface;
    pub use std::io;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::mount_list::*;

    /// `TestListProcMounts` (`mount_linux_test.go`) shape: six fields per line,
    /// comma-split options.
    #[test]
    fn parse_proc_mounts_fields() {
        let mps = parse_proc_mounts(
            "/dev/sdb /var/lib/kubelet/plugins/kubernetes.io/fc/5006-lun-0 ext4 rw,relatime 0 0\n\
             tmpfs /run tmpfs rw,nosuid 0 0\n",
        )
        .unwrap();
        assert_eq!(mps.len(), 2);
        assert_eq!(mps[0].device, "/dev/sdb");
        assert_eq!(mps[0].fstype, "ext4");
        assert_eq!(mps[0].opts, vec!["rw".to_string(), "relatime".to_string()]);
    }

    #[test]
    fn parse_proc_mounts_rejects_short_line() {
        assert_eq!(
            parse_proc_mounts("/dev/sdb /x ext4 rw 0\n").unwrap_err(),
            "wrong number of fields (expected 6, got 5)"
        );
    }
}
