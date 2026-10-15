//! Port of `pkg/volume/fc/fc_util.go`: the sysfs / `/dev/disk` walkers that
//! resolve a WWN+LUN (`/dev/disk/by-path`) or a WWID (`/dev/disk/by-id`) to a
//! block device and its multipath parent, `AttachDisk`, `DetachDisk`,
//! `DetachBlockFCDisk`, and the global (`plugins/kubernetes.io/fc/...`) path
//! names.

use super::{DiskManager, FcDisk, FcDiskMounter, FcDiskUnmapper, FcDiskUnmounter, FC_PLUGIN_NAME};
use crate::volume_plugins::util::device_util::{DeviceUtil, IoUtil, OsIoHandler as OsIoUtil};
use crate::volume_plugins::util::exec::Exec;
use crate::volume_plugins::util::mount_utils::path_exists;
use crate::volume_plugins::VolumeHost;
use anyhow::{anyhow, bail, Result};
use regex::Regex;
use std::io;
use std::path::Path;
use tracing::{debug, error, info, warn};

/// `ioHandler` (`fc_util.go:36-41`), a mockable seam over `/dev/disk` and
/// sysfs. `ReadDir` returns only the entry names (every caller uses `Name()`
/// alone), sorted by filename as `ioutil.ReadDir` does. Upstream's `Lstat` is
/// not declared: only `util.DeviceHandler` calls it, through
/// [`IoUtil`], which carries its own `lstat`.
pub(crate) trait IoHandler: Send + Sync {
    fn read_dir(&self, dirname: &str) -> io::Result<Vec<String>>;
    fn eval_symlinks(&self, path: &str) -> io::Result<String>;
    fn write_file(&self, filename: &str, data: &[u8], perm: u32) -> io::Result<()>;
}

/// `osIOHandler` (`fc_util.go:43`, `:50-61`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OsIoHandler;

impl IoHandler for OsIoHandler {
    fn read_dir(&self, dirname: &str) -> io::Result<Vec<String>> {
        OsIoUtil.read_dir(dirname)
    }

    fn eval_symlinks(&self, path: &str) -> io::Result<String> {
        OsIoUtil.eval_symlinks(path)
    }

    /// `ioutil.WriteFile(filename, data, perm)`: create-or-truncate with `perm`.
    fn write_file(&self, filename: &str, data: &[u8], perm: u32) -> io::Result<()> {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(perm)
            .open(filename)?;
        f.write_all(data)
    }
}

/// `byPath` (`fc_util.go:46`).
pub(crate) const BY_PATH: &str = "/dev/disk/by-path/";
/// `byID` (`fc_util.go:47`).
pub(crate) const BY_ID: &str = "/dev/disk/by-id/";

/// `findDisk` (`fc_util.go:63-81`): given a wwn and lun, find the device and
/// associated devicemapper parent.
fn find_disk(
    wwn: &str,
    lun: &str,
    io: &dyn IoHandler,
    device_util: &dyn DeviceUtil,
) -> (String, String) {
    let fc_path_exp = format!("^(pci-.*-fc|fc)-0x{wwn}-lun-{lun}$");
    // `regexp.MustCompile` panics on an invalid expression.
    let r = Regex::new(&fc_path_exp)
        .unwrap_or_else(|e| panic!("regexp: Compile({fc_path_exp:?}): {e}"));
    let dev_path = BY_PATH;
    if let Ok(dirs) = io.read_dir(dev_path) {
        for name in dirs {
            if r.is_match(&name) {
                if let Ok(disk) = io.eval_symlinks(&format!("{dev_path}{name}")) {
                    let dm = device_util.find_multipath_device_for_device(&disk);
                    info!("fc: find disk: {disk}, dm: {dm}, fc path: {name}");
                    return (disk, dm);
                }
            }
        }
    }
    (String::new(), String::new())
}

/// `findDiskWWIDs` (`fc_util.go:84-113`): given a wwid, find the device and
/// associated devicemapper parent.
fn find_disk_wwids(
    wwid: &str,
    io: &dyn IoHandler,
    device_util: &dyn DeviceUtil,
) -> (String, String) {
    // Example wwid format:
    //   3600508b400105e210000900000490000
    //   <VENDOR NAME> <IDENTIFIER NUMBER>
    // Example of symlink under by-id:
    //   /dev/by-id/scsi-3600508b400105e210000900000490000
    //   /dev/by-id/scsi-<VENDOR NAME>_<IDENTIFIER NUMBER>
    // The wwid could contain white space and it will be replaced
    // underscore when wwid is exposed under /dev/by-id.

    let fc_path = format!("scsi-{wwid}");
    let dev_id = BY_ID;
    if let Ok(dirs) = io.read_dir(dev_id) {
        for name in dirs {
            if name == fc_path {
                let disk = match io.eval_symlinks(&format!("{dev_id}{name}")) {
                    Ok(d) => d,
                    Err(e) => {
                        debug!(
                            "fc: failed to find a corresponding disk from symlink[{dev_id}{name}], error {e}"
                        );
                        return (String::new(), String::new());
                    }
                };
                let dm = device_util.find_multipath_device_for_device(&disk);
                info!("fc: find disk: {disk}, dm: {dm}");
                return (disk, dm);
            }
        }
    }
    debug!("fc: failed to find a disk [{dev_id}{fc_path}]");
    (String::new(), String::new())
}

/// `flushDevice` (`fc_util.go:115-123`): flushes any outstanding I/O to the
/// device.
fn flush_device(device_name: &str, exec: &dyn Exec) {
    let (out, err) = exec.combined_output(
        "blockdev",
        &["--flushbufs".to_string(), device_name.to_string()],
    );
    if let Err(e) = err {
        // Ignore the error and continue deleting the device. There is will be no retry on error.
        warn!("Failed to flush device {device_name}: {e}\n{out}");
    }
    debug!("Flushed device {device_name}");
}

/// `removeFromScsiSubsystem` (`fc_util.go:125-131`): removes a scsi device
/// based upon /dev/sdX name.
fn remove_from_scsi_subsystem(device_name: &str, io: &dyn IoHandler) {
    let file_name = format!("/sys/block/{device_name}/device/delete");
    debug!("fc: remove device from scsi-subsystem: path: {file_name}");
    let _ = io.write_file(&file_name, b"1", 0o666);
}

/// `scsiHostRescan` (`fc_util.go:133-143`): rescan scsi bus.
fn scsi_host_rescan(io: &dyn IoHandler) {
    let scsi_path = "/sys/class/scsi_host/";
    if let Ok(dirs) = io.read_dir(scsi_path) {
        for f in dirs {
            let name = format!("{scsi_path}{f}/scan");
            let _ = io.write_file(&name, b"- - -", 0o666);
        }
    }
}

/// `makePDNameInternal` (`fc_util.go:145-152`): make a directory like
/// /var/lib/kubelet/plugins/kubernetes.io/fc/target1-target2-lun-0.
pub(crate) fn make_pd_name_internal(
    host: &dyn VolumeHost,
    wwns: &[String],
    lun: &str,
    wwids: &[String],
) -> String {
    let plugin_dir = host.get_plugin_dir(FC_PLUGIN_NAME);
    if !wwns.is_empty() {
        let w = wwns.join("-");
        return Path::new(&plugin_dir)
            .join(format!("{w}-lun-{lun}"))
            .to_string_lossy()
            .into_owned();
    }
    Path::new(&plugin_dir)
        .join(wwids.join("-"))
        .to_string_lossy()
        .into_owned()
}

/// `makeVDPDNameInternal` (`fc_util.go:154-161`): make a directory like
/// /var/lib/kubelet/plugins/kubernetes.io/fc/volumeDevices/target-lun-0.
pub(crate) fn make_vdpd_name_internal(
    host: &dyn VolumeHost,
    wwns: &[String],
    lun: &str,
    wwids: &[String],
) -> String {
    let plugin_dir = host.get_volume_device_plugin_dir(FC_PLUGIN_NAME);
    if !wwns.is_empty() {
        let w = wwns.join("-");
        return Path::new(&plugin_dir)
            .join(format!("{w}-lun-{lun}"))
            .to_string_lossy()
            .into_owned();
    }
    Path::new(&plugin_dir)
        .join(wwids.join("-"))
        .to_string_lossy()
        .into_owned()
}

/// `strconv.Atoi` for the one caller that needs its error text.
fn atoi(s: &str) -> std::result::Result<i64, String> {
    s.parse::<i64>().map_err(|e| {
        let why = match e.kind() {
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                "value out of range"
            }
            _ => "invalid syntax",
        };
        format!("strconv.Atoi: parsing {s:?}: {why}")
    })
}

/// `parsePDName` (`fc_util.go:163-186`): parse a directory name created by
/// `makePDNameInternal` or `makeVDPDNameInternal` into `(wwns, lun, wwids)`.
pub(crate) fn parse_pd_name(path: &str) -> Result<(Vec<String>, i32, Vec<String>)> {
    let dirname = Path::new(path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".".to_string());
    let components: Vec<String> = dirname.split('-').map(str::to_string).collect();
    let l = components.len();
    if l == 1 {
        // No '-', it must be single WWID
        return Ok((Vec::new(), 0, components));
    }
    if components[l - 2] == "lun" {
        // it has -lun-, it's list of WWNs + lun number as the last component
        if l == 2 {
            bail!("no wwn in: {dirname}");
        }
        let lun = atoi(&components[l - 1]).map_err(|e| anyhow!(e))?;
        return Ok((components[..l - 2].to_vec(), lun as i32, Vec::new()));
    }
    // no -lun-, it's just list of WWIDs
    Ok((Vec::new(), 0, components))
}

/// `fcUtil` (`fc_util.go:188`): the real [`DiskManager`].
#[derive(Clone, Copy, Debug, Default)]
pub struct FcUtil;

/// `searchDisk` (`fc_util.go:199-249`).
pub(crate) fn search_disk(b: &FcDiskMounter) -> Result<String> {
    let io = b.disk.io.as_ref();
    let wwids = &b.disk.wwids;
    let wwns = &b.disk.wwns;
    let lun = &b.disk.lun;

    let disk_ids = if !wwns.is_empty() { wwns } else { wwids };

    let mut disk = String::new();
    let mut dm = String::new();
    let mut rescanned = false;
    // two-phase search:
    // first phase, search existing device path, if a multipath dm is found, exit loop
    // otherwise, in second phase, rescan scsi bus and search again, return with any findings
    loop {
        for disk_id in disk_ids {
            (disk, dm) = if !wwns.is_empty() {
                find_disk(disk_id, lun, io, b.device_util.as_ref())
            } else {
                find_disk_wwids(disk_id, io, b.device_util.as_ref())
            };
            // if multipath device is found, break
            if !dm.is_empty() {
                break;
            }
        }
        // if a dm is found, exit loop
        if rescanned || !dm.is_empty() {
            break;
        }
        // rescan and search again
        // rescan scsi bus
        scsi_host_rescan(io);
        rescanned = true;
    }
    // if no disk matches input wwn and lun, exit
    if disk.is_empty() && dm.is_empty() {
        bail!("no fc disk found");
    }

    // if multipath devicemapper device is found, use it; otherwise use raw disk
    if !dm.is_empty() {
        return Ok(dm);
    }
    Ok(disk)
}

impl FcUtil {
    /// `detachFCDisk` (`fc_util.go:302-313`): removes scsi device file such as
    /// /dev/sdX from the node.
    fn detach_fc_disk(&self, io: &dyn IoHandler, exec: &dyn Exec, device_path: &str) -> Result<()> {
        // Remove scsi device from the node.
        if !device_path.starts_with("/dev/") {
            bail!("fc detach disk: invalid device name: {device_path}");
        }
        flush_device(device_path, exec);
        let dev = device_path.rsplit('/').next().unwrap_or(device_path);
        remove_from_scsi_subsystem(dev, io);
        Ok(())
    }

    /// `deleteMultipathDevice` (`fc_util.go:402-409`).
    fn delete_multipath_device(&self, exec: &dyn Exec, dm_device: &str) -> Result<()> {
        let (out, err) =
            exec.combined_output("multipath", &["-f".to_string(), dm_device.to_string()]);
        if let Err(e) = err {
            bail!("failed to flush multipath device {dm_device}: {e}\n{out}");
        }
        debug!("Flushed multipath device: {dm_device}");
        Ok(())
    }
}

/// `checkPathExists` (`fc_util.go:411-419`): `(exists, err)`.
fn check_path_exists(path: &str) -> (bool, Option<anyhow::Error>) {
    let (exists, err) = path_exists(path);
    if let Some(e) = err {
        return (exists, Some(anyhow!("error checking if path exists: {e}")));
    } else if !exists {
        warn!("Warning: Unmap skipped because path does not exist: {path}");
        return (exists, None);
    }
    (true, None)
}

impl DiskManager for FcUtil {
    /// `MakeGlobalPDName` (`fc_util.go:190-192`).
    fn make_global_pd_name(&self, fc: &FcDisk) -> String {
        make_pd_name_internal(fc.plugin.host.as_ref(), &fc.wwns, &fc.lun, &fc.wwids)
    }

    /// `MakeGlobalVDPDName` (`fc_util.go:194-197`): global volume device plugin dir.
    fn make_global_vdpd_name(&self, fc: &FcDisk) -> String {
        make_vdpd_name_internal(fc.plugin.host.as_ref(), &fc.wwns, &fc.lun, &fc.wwids)
    }

    /// `AttachDisk` (`fc_util.go:251-266`).
    fn attach_disk(&self, b: &FcDiskMounter) -> Result<String> {
        let device_path = search_disk(b)?;

        let (exists, err) = path_exists(&device_path);
        match (exists, err) {
            (true, None) => Ok(device_path),
            (false, _) => bail!("device {device_path} does not exist"),
            (true, Some(e)) => Err(e.into()),
        }
    }

    /// `DetachDisk` (`fc_util.go:268-300`): removes scsi device file such as
    /// /dev/sdX from the node.
    fn detach_disk(&self, c: &FcDiskUnmounter, device_path: &str) -> Result<()> {
        let mut devices: Vec<String> = Vec::new();
        // devicePath might be like /dev/mapper/mpathX. Find destination.
        let dst_path = c.disk.io.eval_symlinks(device_path)?;
        // Find slave
        if dst_path.starts_with("/dev/dm-") {
            devices = c.device_util.find_slave_devices_on_multipath(&dst_path);
            self.delete_multipath_device(c.exec.as_ref(), &dst_path)?;
        } else {
            // Add single devicepath to devices
            devices.push(dst_path.clone());
        }
        debug!(
            "fc: DetachDisk devicePath: {device_path}, dstPath: {dst_path}, devices: {devices:?}"
        );
        let mut last_err: Option<anyhow::Error> = None;
        for device in &devices {
            if let Err(e) = self.detach_fc_disk(c.disk.io.as_ref(), c.exec.as_ref(), device) {
                error!("fc: detachFCDisk failed. device: {device} err: {e}");
                last_err = Some(anyhow!(
                    "fc: detachFCDisk failed. device: {device} err: {e}"
                ));
            }
        }
        if let Some(e) = last_err {
            error!("fc: last error occurred during detach disk:\n{e}");
            return Err(e);
        }
        Ok(())
    }

    /// `DetachBlockFCDisk` (`fc_util.go:315-400`): detaches a volume from
    /// kubelet node, removes scsi device file such as /dev/sdX from the node,
    /// and then removes loopback for the scsi device.
    fn detach_block_fc_disk(
        &self,
        c: &FcDiskUnmapper,
        map_path: &str,
        device_path: &str,
    ) -> Result<()> {
        let mut device_path = device_path.to_string();
        // Check if devicePath is valid
        if !device_path.is_empty() {
            let (path_exists, path_err) = check_path_exists(&device_path);
            if !path_exists || path_err.is_some() {
                return path_err.map_or(Ok(()), Err);
            }
        } else {
            // TODO: FC plugin can't obtain the devicePath from kubelet because devicePath
            // in volume object isn't updated when volume is attached to kubelet node.
            info!("fc: devicePath is empty. Try to retrieve FC configuration from global map path: {map_path}");
        }

        // Check if global map path is valid
        // global map path examples:
        //   wwn+lun: plugins/kubernetes.io/fc/volumeDevices/50060e801049cfd1-lun-0/
        //   wwid: plugins/kubernetes.io/fc/volumeDevices/3600508b400105e210000900000490000/
        let (path_exists, path_err) = check_path_exists(map_path);
        if !path_exists || path_err.is_some() {
            return path_err.map_or(Ok(()), Err);
        }

        // Retrieve volume plugin dependent path like '50060e801049cfd1-lun-0' from global map path
        let volume_info = map_path.rsplit('/').next().unwrap_or(map_path);

        // Search symbolic link which matches volumeInfo under /dev/disk/by-path or /dev/disk/by-id
        // then find destination device path from the link
        let search_path = if volume_info.contains("-lun-") {
            BY_PATH
        } else {
            BY_ID
        };
        // `ioutil.ReadDir(searchPath)`: straight to the OS, not through `c.io`.
        let fis = OsIoUtil.read_dir(search_path)?;
        for name in &fis {
            if name.contains(volume_info) {
                device_path = format!("{search_path}{name}");
                debug!("fc: updated devicePath: {device_path}");
                break;
            }
        }
        if device_path.is_empty() {
            bail!("fc: failed to find corresponding device from searchPath: {search_path}");
        }
        let mut dst_path = c.disk.io.eval_symlinks(&device_path)?;
        debug!("fc: find destination device path from symlink: {dst_path}");

        let mut devices: Vec<String> = Vec::new();
        let dm = c.device_util.find_multipath_device_for_device(&dst_path);
        if !dm.is_empty() {
            dst_path = dm.clone();
        }

        // Detach volume from kubelet node
        if !dm.is_empty() {
            // Find all devices which are managed by multipath
            devices = c.device_util.find_slave_devices_on_multipath(&dm);
            self.delete_multipath_device(c.exec.as_ref(), &dm)?;
        } else {
            // Add single device path to devices
            devices.push(dst_path);
        }
        let mut last_err: Option<anyhow::Error> = None;
        for device in &devices {
            if let Err(e) = self.detach_fc_disk(c.disk.io.as_ref(), c.exec.as_ref(), device) {
                error!("fc: detachFCDisk failed. device: {device} err: {e}");
                last_err = Some(anyhow!(
                    "fc: detachFCDisk failed. device: {device} err: {e}"
                ));
            }
        }
        if let Some(e) = last_err {
            error!("fc: last error occurred during detach disk:\n{e}");
            return Err(e);
        }
        Ok(())
    }
}
