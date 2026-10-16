//! Port of `k8s.io/mount-utils` `resizefs_linux.go` (`ResizeFs`) and of
//! `GenericResizeFS` (`pkg/volume/util/resize_util.go:460`), the filesystem
//! resizer node expansion falls back to for plugins with no special
//! filesystem-resize requirements.

use super::exec::{Exec, RealExec};
use super::mount_interface::get_disk_format;

const BLOCK_DEV: &str = "blockdev";

/// `mount.ResizeFs` (`resizefs_linux.go:36`).
pub struct ResizeFs<'a> {
    exec: &'a dyn Exec,
}

/// `mount.NewResizeFs` (`resizefs_linux.go:41`).
pub fn new_resize_fs(exec: &dyn Exec) -> ResizeFs<'_> {
    ResizeFs { exec }
}

/// `GenericResizeFS` (`resize_util.go:460`): `mount.NewResizeFs(exec.New())`.
pub fn generic_resize_fs(device_path: &str, device_mount_path: &str) -> Result<bool, String> {
    new_resize_fs(&RealExec).resize(device_path, device_mount_path)
}

impl<'a> ResizeFs<'a> {
    fn run(&self, cmd: &str, args: &[&str]) -> (String, Result<(), super::exec::ExecError>) {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        self.exec.combined_output(cmd, &args)
    }

    /// `ResizeFs.Resize` (`resizefs_linux.go:46`).
    pub fn resize(&self, device_path: &str, device_mount_path: &str) -> Result<bool, String> {
        let format = get_disk_format(self.exec, device_path).map_err(|e| {
            format!("ResizeFS.Resize - error checking format for device {device_path}: {e}")
        })?;

        // If disk has no format, there is no need to resize the disk because mkfs.*
        // by default will use whole disk anyways.
        if format.is_empty() {
            return Ok(false);
        }

        match format.as_str() {
            "ext3" | "ext4" => self.ext_resize(device_path),
            "xfs" => self.xfs_resize(device_mount_path),
            "btrfs" => self.btrfs_resize(device_mount_path),
            _ => Err(format!(
                "ResizeFS.Resize - resize of format {format} is not supported for device {device_path} mounted at {device_mount_path}"
            )),
        }
    }

    /// `ResizeFs.extResize` (`resizefs_linux.go:72`).
    fn ext_resize(&self, device_path: &str) -> Result<bool, String> {
        let (output, err) = self.run("resize2fs", &[device_path]);
        match err {
            Ok(()) => Ok(true),
            Err(e) => Err(format!(
                "resize of device {device_path} failed: {e}. resize2fs output: {output}"
            )),
        }
    }

    /// `ResizeFs.xfsResize` (`resizefs_linux.go:83`).
    fn xfs_resize(&self, device_mount_path: &str) -> Result<bool, String> {
        let (output, err) = self.run("xfs_growfs", &["-d", device_mount_path]);
        match err {
            Ok(()) => Ok(true),
            Err(e) => Err(format!(
                "resize of device {device_mount_path} failed: {e}. xfs_growfs output: {output}"
            )),
        }
    }

    /// `ResizeFs.btrfsResize` (`resizefs_linux.go:96`).
    fn btrfs_resize(&self, device_mount_path: &str) -> Result<bool, String> {
        let (output, err) = self.run("btrfs", &["filesystem", "resize", "max", device_mount_path]);
        match err {
            Ok(()) => Ok(true),
            Err(e) => Err(format!(
                "resize of device {device_mount_path} failed: {e}. btrfs output: {output}"
            )),
        }
    }

    /// `ResizeFs.NeedResize` (`resizefs_linux.go:109`).
    pub fn need_resize(&self, device_path: &str, _device_mount_path: &str) -> Result<bool, String> {
        // Do nothing if device is mounted as readonly
        if self.get_device_ro(device_path)? {
            return Ok(false);
        }

        let format = get_disk_format(self.exec, device_path).map_err(|e| {
            format!("ResizeFS.Resize - error checking format for device {device_path}: {e}")
        })?;

        if format.is_empty() {
            return Ok(false);
        }

        match format.as_str() {
            // resize2fs/xfs_growfs do the check themselves.
            "ext3" | "ext4" | "xfs" => Ok(true),
            "btrfs" => {
                let device_size = self.get_device_size(device_path)?;
                let (block_size, fs_size) = self.get_btrfs_size(device_path)?;
                Ok(device_size > fs_size.saturating_add(block_size))
            }
            _ => Err(format!(
                "could not parse fs info of given filesystem format: {format}. Supported fs types are: xfs, ext3, ext4"
            )),
        }
    }

    /// `ResizeFs.getDeviceSize` (`resizefs_linux.go:156`).
    fn get_device_size(&self, device_path: &str) -> Result<u64, String> {
        let (output, err) = self.run(BLOCK_DEV, &["--getsize64", device_path]);
        let out_str = output.trim();
        if let Err(e) = err {
            return Err(format!(
                "failed to read size of device {device_path}: {e}: {out_str}"
            ));
        }
        out_str
            .parse::<u64>()
            .map_err(|e| format!("failed to parse size of device {device_path} {out_str}: {e}"))
    }

    /// `ResizeFs.getBtrfsSize` (`resizefs_linux.go:169`): `(blockSize, totalBytes)`.
    fn get_btrfs_size(&self, device_path: &str) -> Result<(u64, u64), String> {
        let (output, err) = self.run(
            "btrfs",
            &["inspect-internal", "dump-super", "-f", device_path],
        );
        if let Err(e) = err {
            return Err(format!(
                "failed to read size of filesystem on {device_path}: {e}: {output}"
            ));
        }
        // Upstream discards parseBtrfsInfoOutput's error (`blockSize, totalBytes, _ :=`);
        // on a parse error it returns zeros, which the checks below report.
        let (block_size, total_bytes) =
            parse_btrfs_info_output(&output, "sectorsize", "total_bytes").unwrap_or((0, 0));
        if block_size == 0 {
            return Err(format!("could not find block size of device {device_path}"));
        }
        if total_bytes == 0 {
            return Err(format!("could not find total size of device {device_path}"));
        }
        Ok((block_size, total_bytes))
    }

    /// `ResizeFs.getDeviceRO` (`resizefs_linux.go:214`).
    fn get_device_ro(&self, device_path: &str) -> Result<bool, String> {
        let (output, err) = self.run(BLOCK_DEV, &["--getro", device_path]);
        let out_str = output.trim();
        if let Err(e) = err {
            return Err(format!(
                "failed to get readonly bit from device {device_path}: {e}: {out_str}"
            ));
        }
        match out_str {
            "0" => Ok(false),
            "1" => Ok(true),
            _ => Err(format!(
                "failed readonly device check. Expected 1 or 0, got '{out_str}'"
            )),
        }
    }
}

/// `ResizeFs.parseBtrfsInfoOutput` (`resizefs_linux.go:186`): `(blockSize, blockCount)`.
fn parse_btrfs_info_output(
    cmd_output: &str,
    block_size_key: &str,
    total_bytes_key: &str,
) -> Result<(u64, u64), String> {
    let (mut block_size, mut block_count) = (0u64, 0u64);
    for line in cmd_output.split('\n') {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        if tokens.len() != 2 {
            continue;
        }
        let (key, value) = (tokens[0].to_lowercase(), tokens[1].to_lowercase());
        if key == block_size_key {
            block_size = value
                .parse()
                .map_err(|e| format!("failed to parse block size {value}: {e}"))?;
        }
        if key == total_bytes_key {
            block_count = value
                .parse()
                .map_err(|e| format!("failed to parse total size {value}: {e}"))?;
        }
    }
    Ok((block_size, block_count))
}

#[cfg(test)]
mod tests {
    use super::super::exec::testing::{script, CommandScript, FakeExec};
    use super::*;

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
