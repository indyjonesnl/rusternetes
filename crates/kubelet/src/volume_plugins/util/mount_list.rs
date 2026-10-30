//! Port of `Mounter.List` / `ListProcMounts` / `parseProcMounts`
//! (`staging/src/k8s.io/mount-utils/mount_linux.go:426-428`, `:726-770`) and
//! `GetDeviceNameFromMount` (`mount-utils/mount.go:260-289`), which the fc
//! plugin's `UnmountDevice` (`pkg/volume/fc/attacher.go:168`) calls.

use super::mount_interface::MountInterface;
use std::io;

/// `procMountsPath` (`mount_linux.go:46`).
pub const PROC_MOUNTS_PATH: &str = "/proc/mounts";

/// `expectedNumFieldsPerLine` (`mount_linux.go:44`).
const EXPECTED_NUM_FIELDS_PER_LINE: usize = 6;

/// `mount.MountPoint` (`mount.go:103-110`): a single line in /proc/mounts.
/// `opts` may contain sensitive mount options and must not be logged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountPoint {
    pub device: String,
    pub path: String,
    pub fstype: String,
    pub opts: Vec<String>,
    pub freq: i32,
    pub pass: i32,
}

/// `ListProcMounts` (`mount_linux.go:726-732`): all mounted filesystems.
pub fn list_proc_mounts(mount_file_path: &str) -> io::Result<Vec<MountPoint>> {
    let content = std::fs::read(mount_file_path)?;
    parse_proc_mounts(&String::from_utf8_lossy(&content)).map_err(io::Error::other)
}

/// `parseProcMounts` (`mount_linux.go:734-770`).
pub fn parse_proc_mounts(content: &str) -> Result<Vec<MountPoint>, String> {
    let mut out = Vec::new();
    for line in content.split('\n') {
        if line.is_empty() {
            // the last split() item is empty string following the last \n
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() != EXPECTED_NUM_FIELDS_PER_LINE {
            // Do not log line in case it contains sensitive Mount options
            return Err(format!(
                "wrong number of fields (expected {EXPECTED_NUM_FIELDS_PER_LINE}, got {})",
                fields.len()
            ));
        }
        let freq = fields[4].parse::<i32>().map_err(|e| e.to_string())?;
        let pass = fields[5].parse::<i32>().map_err(|e| e.to_string())?;
        out.push(MountPoint {
            device: fields[0].to_string(),
            path: fields[1].to_string(),
            fstype: fields[2].to_string(),
            opts: fields[3].split(',').map(str::to_string).collect(),
            freq,
            pass,
        });
    }
    Ok(out)
}

/// `GetDeviceNameFromMount` (`mount.go:260-289`): the device mounted at
/// `mount_path` and how many mount points reference that device.
pub fn get_device_name_from_mount(
    mounter: &dyn MountInterface,
    mount_path: &str,
) -> io::Result<(String, usize)> {
    let mps = mounter.list()?;

    // Find the device name.
    // FIXME if multiple devices mounted on the same mount path, only the first one is returned.
    let mut device = String::new();
    // If mountPath is symlink, need get its target path.
    let sl_target = std::fs::canonicalize(mount_path)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| mount_path.to_string());
    for mp in &mps {
        if mp.path == sl_target {
            device = mp.device.clone();
            break;
        }
    }

    // Find all references to the device.
    let ref_count = mps.iter().filter(|mp| mp.device == device).count();
    Ok((device, ref_count))
}
