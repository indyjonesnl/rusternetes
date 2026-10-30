//! Port of `pkg/volume/util/device_util.go` (`DeviceUtil`, `deviceHandler`),
//! `device_util_linux.go` and `io_util.go` (`IoUtil`, `osIOHandler`): the sysfs
//! walkers the iscsi plugin uses to find a LUN's block devices, its multipath
//! map and the SCSI host of a portal.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tracing::{error, info};

/// `IoUtil` (`io_util.go:25-30`), a mockable seam over sysfs reads.
/// `ReadDir` returns only the entry names: every caller uses `Name()` alone.
pub trait IoUtil: Send + Sync {
    fn read_file(&self, filename: &str) -> io::Result<Vec<u8>>;
    /// `ioutil.ReadDir`: entries sorted by filename.
    fn read_dir(&self, dirname: &str) -> io::Result<Vec<String>>;
    /// `os.Lstat`; only whether it succeeds is used.
    fn lstat(&self, name: &str) -> io::Result<()>;
    fn eval_symlinks(&self, path: &str) -> io::Result<String>;
}

/// `osIOHandler` (`io_util.go:32-50`).
#[derive(Clone, Copy, Debug, Default)]
pub struct OsIoHandler;

impl IoUtil for OsIoHandler {
    fn read_file(&self, filename: &str) -> io::Result<Vec<u8>> {
        std::fs::read(filename)
    }

    fn read_dir(&self, dirname: &str) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for e in std::fs::read_dir(dirname)? {
            names.push(e?.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        Ok(names)
    }

    fn lstat(&self, name: &str) -> io::Result<()> {
        std::fs::symlink_metadata(name).map(|_| ())
    }

    fn eval_symlinks(&self, path: &str) -> io::Result<String> {
        std::fs::canonicalize(path).map(|p| p.to_string_lossy().into_owned())
    }
}

/// `DeviceUtil` (`device_util.go:20-26`).
pub trait DeviceUtil: Send + Sync {
    fn find_multipath_device_for_device(&self, disk: &str) -> String;
    fn find_slave_devices_on_multipath(&self, disk: &str) -> Vec<String>;
    fn get_iscsi_portal_host_map_for_target(
        &self,
        target_iqn: &str,
    ) -> io::Result<HashMap<String, i32>>;
    fn find_devices_for_iscsi_lun(&self, target_iqn: &str, lun: i32) -> io::Result<Vec<String>>;
}

/// `deviceHandler` (`device_util.go:28-30`).
pub struct DeviceHandler {
    get_io: Arc<dyn IoUtil>,
}

/// `NewDeviceHandler` (`device_util.go:33-35`).
pub fn new_device_handler(io: Arc<dyn IoUtil>) -> DeviceHandler {
    DeviceHandler { get_io: io }
}

/// `NewDeviceHandler(NewIOHandler())`, the form every iscsi call site builds.
pub fn new_os_device_handler() -> Arc<dyn DeviceUtil> {
    Arc::new(new_device_handler(Arc::new(OsIoHandler)))
}

/// `findDeviceForPath` (`device_util_linux.go:62-74`): the underlying disk for
/// a linked path such as /dev/disk/by-path/XXXX or /dev/mapper/XXXX; `sdX` or
/// `hdX` etc. If /dev/sdX is passed in then sdX is returned.
pub fn find_device_for_path(path: &str, io: &dyn IoUtil) -> Result<String, String> {
    let device_path = io.eval_symlinks(path).map_err(|e| e.to_string())?;
    // if path /dev/hdX split into "", "dev", "hdX" then we will
    // return just the last part
    let parts: Vec<&str> = device_path.split('/').collect();
    if parts.len() == 3 && parts[1].starts_with("dev") {
        return Ok(parts[2].to_string());
    }
    Err(format!("Illegal path for device {device_path}"))
}

/// `net.JoinHostPort`: IPv6 hosts (containing ':') are bracketed.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

impl DeviceUtil for DeviceHandler {
    /// `FindMultipathDeviceForDevice` (`device_util_linux.go:36-58`): given a
    /// device name like /dev/sdx, find the devicemapper parent. If called with
    /// a device already resolved to devicemapper, do nothing.
    fn find_multipath_device_for_device(&self, device: &str) -> String {
        if device.starts_with("/dev/dm-") {
            return device.to_string();
        }

        let io = self.get_io.as_ref();
        let Ok(disk) = find_device_for_path(device, io) else {
            return String::new();
        };
        let sys_path = "/sys/block/";
        if let Ok(dirs) = io.read_dir(sys_path) {
            for name in dirs {
                if name.starts_with("dm-")
                    && io.lstat(&format!("{sys_path}{name}/slaves/{disk}")).is_ok()
                {
                    return format!("/dev/{name}");
                }
            }
        }
        String::new()
    }

    /// `FindSlaveDevicesOnMultipath` (`device_util_linux.go:78-94`): given a dm
    /// name like /dev/dm-1, find all devices which are managed by the
    /// devicemapper dm-1.
    fn find_slave_devices_on_multipath(&self, dm: &str) -> Vec<String> {
        let mut devices = Vec::new();
        // Split path /dev/dm-1 into "", "dev", "dm-1"
        let parts: Vec<&str> = dm.split('/').collect();
        if parts.len() != 3 || !parts[1].starts_with("dev") {
            return devices;
        }
        let disk = parts[2];
        let slaves_path = format!("/sys/block/{disk}/slaves");
        if let Ok(files) = self.get_io.read_dir(&slaves_path) {
            for f in files {
                devices.push(format!("/dev/{f}"));
            }
        }
        devices
    }

    /// `GetISCSIPortalHostMapForTarget` (`device_util_linux.go:103-220`): given
    /// a target iqn, find all the scsi hosts logged into that target. Returns
    /// a map of iSCSI portals to SCSI host numbers, e.g.
    /// `{"192.168.30.7:3260": 2, "192.168.30.8:3260": 3}`.
    fn get_iscsi_portal_host_map_for_target(
        &self,
        target_iqn: &str,
    ) -> io::Result<HashMap<String, i32>> {
        let mut portal_host_map = HashMap::new();
        let io = self.get_io.as_ref();

        // Iterate over all the iSCSI hosts in sysfs
        let sys_path = "/sys/class/iscsi_host";
        let host_dirs = match io.read_dir(sys_path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(portal_host_map),
            Err(e) => return Err(e),
        };
        for host_name in host_dirs {
            // iSCSI hosts are always of the format "host%d"
            // See drivers/scsi/hosts.c in Linux
            let Some(rest) = host_name.strip_prefix("host") else {
                continue;
            };
            let Ok(host_number) = rest.parse::<i32>() else {
                error!("Could not get number from iSCSI host: {host_name}");
                continue;
            };

            // Iterate over the children of the iscsi_host device
            // We are looking for the associated session
            let device_path = format!("{sys_path}/{host_name}/device");
            let device_dirs = io.read_dir(&device_path)?;
            for session_name in device_dirs {
                // Skip over files that aren't the session
                // Sessions are of the format "session%u"
                // See drivers/scsi/scsi_transport_iscsi.c in Linux
                if !session_name.starts_with("session") {
                    continue;
                }

                let session_path = format!("{device_path}/{session_name}");

                // Read the target name for the iSCSI session
                let target_name_path =
                    format!("{session_path}/iscsi_session/{session_name}/targetname");
                let target_name = match io.read_file(&target_name_path) {
                    Ok(t) => t,
                    Err(e) => {
                        info!("Failed to process session {session_name}, assuming this session is unavailable: {e}");
                        continue;
                    }
                };

                // Ignore hosts that don't matchthe target we were looking for.
                if String::from_utf8_lossy(&target_name).trim() != target_iqn {
                    continue;
                }

                // Iterate over the children of the iSCSI session looking
                // for the iSCSI connection.
                let dirs2 = match io.read_dir(&session_path) {
                    Ok(d) => d,
                    Err(e) => {
                        info!("Failed to process session {session_name}, assuming this session is unavailable: {e}");
                        continue;
                    }
                };
                for dir_name in dirs2 {
                    // Skip over files that aren't the connection
                    // Connections are of the format "connection%d:%u"
                    // See drivers/scsi/scsi_transport_iscsi.c in Linux
                    if !dir_name.starts_with("connection") {
                        continue;
                    }

                    let connection_path =
                        format!("{session_path}/{dir_name}/iscsi_connection/{dir_name}");

                    // Read the current and persistent portal information for the connection.
                    let mut vals = Vec::new();
                    let mut failed = false;
                    for leaf in ["address", "port", "persistent_address", "persistent_port"] {
                        match io.read_file(&format!("{connection_path}/{leaf}")) {
                            Ok(v) => vals.push(String::from_utf8_lossy(&v).trim().to_string()),
                            Err(e) => {
                                info!("Failed to process connection {dir_name}, assuming this connection is unavailable: {e}");
                                failed = true;
                                break;
                            }
                        }
                    }
                    if failed {
                        continue;
                    }

                    // Add entries to the map for both the current and persistent portals
                    // pointing to the SCSI host for those connections
                    // JoinHostPort will add `[]` around IPv6 addresses.
                    portal_host_map.insert(join_host_port(&vals[0], &vals[1]), host_number);
                    portal_host_map.insert(join_host_port(&vals[2], &vals[3]), host_number);
                }
            }
        }

        Ok(portal_host_map)
    }

    /// `FindDevicesForISCSILun` (`device_util_linux.go:224-306`): given an
    /// iqn, and lun number, find all the devices corresponding to that LUN.
    fn find_devices_for_iscsi_lun(&self, target_iqn: &str, lun: i32) -> io::Result<Vec<String>> {
        let mut devices = Vec::new();
        let io = self.get_io.as_ref();

        // Iterate over all the iSCSI hosts in sysfs
        let sys_path = "/sys/class/iscsi_host";
        let host_dirs = io.read_dir(sys_path)?;
        for host_name in host_dirs {
            // iSCSI hosts are always of the format "host%d"
            // See drivers/scsi/hosts.c in Linux
            let Some(rest) = host_name.strip_prefix("host") else {
                continue;
            };
            let Ok(host_number) = rest.parse::<i32>() else {
                error!("Could not get number from iSCSI host: {host_name}");
                continue;
            };

            // Iterate over the children of the iscsi_host device
            // We are looking for the associated session
            let device_path = format!("{sys_path}/{host_name}/device");
            let device_dirs = io.read_dir(&device_path)?;
            for session_name in device_dirs {
                // Skip over files that aren't the session
                // Sessions are of the format "session%u"
                // See drivers/scsi/scsi_transport_iscsi.c in Linux
                if !session_name.starts_with("session") {
                    continue;
                }

                // Read the target name for the iSCSI session
                let target_name_path =
                    format!("{device_path}/{session_name}/iscsi_session/{session_name}/targetname");
                let target_name = io.read_file(&target_name_path)?;

                // Only if the session matches the target we were looking for,
                // add it to the map
                if String::from_utf8_lossy(&target_name).trim() != target_iqn {
                    continue;
                }

                // The list of block devices on the scsi bus will be in a
                // directory called "target%d:%d:%d".
                // See drivers/scsi/scsi_scan.c in Linux
                // We assume the channel/bus and device/controller are always zero for iSCSI
                let target_path = format!("{device_path}/{session_name}/target{host_number}:0:0");

                // The block device for a given lun will be "%d:%d:%d:%d" --
                // host:channel:bus:LUN
                let block_device_path = format!("{target_path}/{host_number}:0:0:{lun}");

                // If the LUN doesn't exist on this bus, continue on
                if io.lstat(&block_device_path).is_err() {
                    continue;
                }

                // Read the block directory, there should only be one child --
                // the block device "sd*"
                let path = format!("{block_device_path}/block");
                let dirs = io.read_dir(&path)?;
                if let Some(first) = dirs.into_iter().next() {
                    devices.push(first);
                }
            }
        }

        Ok(devices)
    }
}
