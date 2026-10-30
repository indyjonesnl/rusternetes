//! Unit tests moved from `src/volume_plugins/util/resizefs.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_kubelet::volume_plugins::util::exec::{Exec, RealExec};
    pub use rusternetes_kubelet::volume_plugins::util::mount_interface::get_disk_format;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::exec::testing::{
        script, CommandScript, FakeExec,
    };
    use rusternetes_kubelet::volume_plugins::util::resizefs::*;

    fn out(mut s: CommandScript, o: &str) -> CommandScript {
        s.output = o.to_string();
        s
    }

    // Trimmed `cmdOutputSuccessBtrfs` / `cmdOutputNoDataBtrfs`
    // (`resizefs_linux_test.go:30`): the latter lacks `total_bytes` and `sectorsize`.
    fn btrfs_dump(full: bool) -> String {
        let mut s = String::from(
            "superblock: bytenr=65536, device=/dev/loop0\n\tmagic                   _BHRfS_M [match]\n\tlabel\n\tgeneration              7\n",
        );
        if full {
            s.push_str("\ttotal_bytes             1048576000\n");
        }
        s.push_str("\tbytes_used              147456\n");
        if full {
            s.push_str("\tsectorsize              4096\n");
        }
        s.push_str("\tnodesize                16384\n\tleafsize (deprecated)   16384\n\tdev_item.total_bytes    1048576000\n\tdev_item.sector_size    4096\n");
        s
    }

    const DP: &str = "/dev/test1";

    fn blkid(t: &str) -> CommandScript {
        out(
            script(
                "blkid",
                &["-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", DP],
            ),
            t,
        )
    }

    #[test]
    fn get_file_system_size_btrfs_success() {
        let fx = FakeExec::new(vec![out(
            script("btrfs", &["inspect-internal", "dump-super", "-f", DP]),
            &btrfs_dump(true),
        )]);
        let (bs, fs) = new_resize_fs(&fx).get_btrfs_size(DP).unwrap();
        assert_eq!(bs, 4096);
        assert_eq!(fs, 4096 * 256000);
    }

    #[test]
    fn get_file_system_size_btrfs_block_size_missing() {
        let fx = FakeExec::new(vec![out(
            script("btrfs", &["inspect-internal", "dump-super", "-f", DP]),
            &btrfs_dump(false),
        )]);
        assert!(new_resize_fs(&fx).get_btrfs_size(DP).is_err());
    }

    /// `TestNeedResize` table (`resizefs_linux_test.go:307`).
    #[test]
    fn need_resize_table() {
        // (name, readonly, device_size, ext_size, fstype_out, expect_err, expect)
        let cases = [
            ("True", "0", "2048", "20", "TYPE=ext3", false, true),
            (
                "False - readonly",
                "1",
                "2048",
                "20",
                "TYPE=ext3",
                false,
                false,
            ),
            (
                "False - btrfs not needed",
                "0",
                "20",
                "2048",
                "TYPE=btrfs",
                false,
                false,
            ),
            (
                "True - btrfs needed",
                "0",
                "2048",
                "20",
                "TYPE=btrfs",
                false,
                true,
            ),
            (
                "False - unsupported",
                "0",
                "2048",
                "1",
                "TYPE=ntfs",
                true,
                false,
            ),
        ];
        for (name, ro, dev, ext, ty, expect_err, expect) in cases {
            let mut scripts = vec![out(script("blockdev", &["--getro", DP]), ro)];
            // A readonly device returns before getDiskFormat, so script only what runs.
            if ro != "1" {
                scripts.push(blkid(ty));
                if ty == "TYPE=btrfs" {
                    scripts.push(out(script("blockdev", &["--getsize64", DP]), dev));
                    scripts.push(out(
                        script("btrfs", &["inspect-internal", "dump-super", "-f", DP]),
                        &format!("sectorsize {ext}\ntotal_bytes 1\n"),
                    ));
                }
            }
            let fx = FakeExec::new(scripts);
            let r = new_resize_fs(&fx).need_resize(DP, "/mnt/test1");
            assert_eq!(r.is_err(), expect_err, "{name}: {r:?}");
            if !expect_err {
                assert_eq!(r.unwrap(), expect, "{name}");
            }
        }
    }

    #[test]
    fn resize_dispatches_per_format() {
        let fx = FakeExec::new(vec![blkid("TYPE=ext4"), script("resize2fs", &[DP])]);
        assert!(new_resize_fs(&fx).resize(DP, "/mnt").unwrap());
        let fx = FakeExec::new(vec![
            blkid("TYPE=xfs"),
            script("xfs_growfs", &["-d", "/mnt"]),
        ]);
        assert!(new_resize_fs(&fx).resize(DP, "/mnt").unwrap());
        let fx = FakeExec::new(vec![
            blkid("TYPE=btrfs"),
            script("btrfs", &["filesystem", "resize", "max", "/mnt"]),
        ]);
        assert!(new_resize_fs(&fx).resize(DP, "/mnt").unwrap());
        // unformatted (blkid exit 2): no resize
        let mut s = blkid("");
        s.return_code = 2;
        let fx = FakeExec::new(vec![s]);
        assert!(!new_resize_fs(&fx).resize(DP, "/mnt").unwrap());
        // unsupported
        let fx = FakeExec::new(vec![blkid("TYPE=ntfs")]);
        let e = new_resize_fs(&fx).resize(DP, "/mnt").unwrap_err();
        assert_eq!(
            e,
            "ResizeFS.Resize - resize of format ntfs is not supported for device /dev/test1 mounted at /mnt"
        );
        // failure carries the tool output
        let mut f = out(script("resize2fs", &[DP]), "boom");
        f.return_code = 1;
        let fx = FakeExec::new(vec![blkid("TYPE=ext3"), f]);
        let e = new_resize_fs(&fx).resize(DP, "/mnt").unwrap_err();
        assert_eq!(
            e,
            "resize of device /dev/test1 failed: exit status 1. resize2fs output: boom"
        );
    }
}
