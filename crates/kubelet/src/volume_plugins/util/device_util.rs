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
pub(crate) fn find_device_for_path(path: &str, io: &dyn IoUtil) -> Result<String, String> {
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
fn join_host_port(host: &str, port: &str) -> String {
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

#[cfg(test)]
mod tests {
    //! Ports of `device_util_linux_test.go` (`TestFindMultipathDeviceForDevice`,
    //! `TestFindDeviceForPath`, `TestFindSlaveDevicesOnMultipath`,
    //! `TestGetISCSIPortalHostMapForTarget`, `TestFindDevicesForISCSILun`) over
    //! the same `mockOsIOHandler` tables (`:30-196`).
    use super::*;

    struct Mock;

    fn nf() -> io::Error {
        io::Error::other("not Implemented for Mock")
    }

    fn host_of(filename: &str, suffixes: &[&str]) -> Option<char> {
        // ^/sys/class/iscsi_host/(host\d)/device/session\d/<suffix>$
        let rest = filename.strip_prefix("/sys/class/iscsi_host/host")?;
        let digit = rest.chars().next().filter(char::is_ascii_digit)?;
        let rest = rest[1..].strip_prefix("/device/session")?;
        let rest = rest.strip_prefix(|c: char| c.is_ascii_digit())?;
        for s in suffixes {
            // `connection\d:0` / `session\d` back-references are digits.
            let mut pat = s.chars();
            let mut it = rest.chars();
            let mut ok = true;
            loop {
                match (pat.next(), it.next()) {
                    (None, None) => break,
                    (Some('#'), Some(c)) if c.is_ascii_digit() => {}
                    (Some(p), Some(c)) if p == c => {}
                    _ => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                return Some(digit);
            }
        }
        None
    }

    impl IoUtil for Mock {
        fn read_file(&self, filename: &str) -> io::Result<Vec<u8>> {
            let conn = "/connection#:0/iscsi_connection/connection#:0/";
            if host_of(
                filename,
                &[&format!("{conn}port"), &format!("{conn}persistent_port")],
            )
            .is_some()
            {
                return Ok(b"3260".to_vec());
            }
            if let Some(h) = host_of(
                filename,
                &[
                    &format!("{conn}address"),
                    &format!("{conn}persistent_address"),
                ],
            ) {
                match h {
                    '2' => return Ok(b"10.0.0.1".to_vec()),
                    '3' => return Ok(b"10.0.0.2".to_vec()),
                    _ => {}
                }
            }
            if let Some(h) = host_of(filename, &["/iscsi_session/session#/targetname"]) {
                match h {
                    '2' => return Ok(b"target1".to_vec()),
                    '3' => return Ok(b"target2".to_vec()),
                    _ => {}
                }
            }
            Err(nf())
        }

        fn read_dir(&self, dirname: &str) -> io::Result<Vec<String>> {
            let v = |n: &[&str]| Ok(n.iter().map(|s| s.to_string()).collect());
            match dirname {
                "/sys/block/dm-1/slaves" => v(&["sda", "sdb"]),
                "/sys/block/" => v(&["sda", "dm-1"]),
                "/sys/class/iscsi_host" => v(&["host2", "host3", "ignore"]),
                "/sys/class/iscsi_host/host2/device" => v(&["session1", "ignore"]),
                "/sys/class/iscsi_host/host3/device" => v(&["session2", "ignore"]),
                "/sys/class/iscsi_host/host2/device/session1" => v(&["connection1:0", "ignore"]),
                "/sys/class/iscsi_host/host3/device/session2" => v(&["connection2:0", "ignore"]),
                "/sys/class/iscsi_host/host2/device/session1/target2:0:0/2:0:0:1/block" => {
                    v(&["sda"])
                }
                "/sys/class/iscsi_host/host2/device/session1/target2:0:0/2:0:0:2/block" => {
                    v(&["sdc"])
                }
                "/sys/class/iscsi_host/host3/device/session2/target3:0:0/3:0:0:1/block" => {
                    v(&["sdb"])
                }
                "/sys/class/iscsi_host/host3/device/session2/target3:0:0/3:0:0:2/block" => {
                    v(&["sdd"])
                }
                _ => Err(nf()),
            }
        }

        fn lstat(&self, name: &str) -> io::Result<()> {
            const LINKS: [&str; 6] = [
                "/sys/block/dm-1/slaves/sda",
                "/dev/sda",
                "/sys/class/iscsi_host/host2/device/session1/target2:0:0/2:0:0:1",
                "/sys/class/iscsi_host/host2/device/session1/target2:0:0/2:0:0:2",
                "/sys/class/iscsi_host/host3/device/session2/target3:0:0/3:0:0:1",
                "/sys/class/iscsi_host/host3/device/session2/target3:0:0/3:0:0:2",
            ];
            if LINKS.contains(&name) {
                Ok(())
            } else {
                Err(nf())
            }
        }

        fn eval_symlinks(&self, path: &str) -> io::Result<String> {
            // `links[path]`: a missing key is "", with a nil error.
            Ok(match path {
                "/returns/a/dev" => "/dev/sde",
                "/returns/non/dev" => "/sys/block",
                "/dev/disk/by-path/127.0.0.1:3260-eui.02004567A425678D-lun-0" => "/dev/sda",
                "/dev/disk/by-path/127.0.0.3:3260-eui.03004567A425678D-lun-0" => "/dev/sdb",
                "/dev/dm-2" => "/dev/dm-2",
                "/dev/dm-3" => "/dev/dm-3",
                "/dev/sdc" => "/dev/sdc",
                "/dev/sde" => "/dev/sde",
                _ => "",
            }
            .to_string())
        }
    }

    fn mock_util() -> DeviceHandler {
        new_device_handler(Arc::new(Mock))
    }

    #[test]
    fn find_multipath_device_for_device() {
        let util = mock_util();
        for (name, device, want) in [
            ("Device is already a dm device", "/dev/dm-1", "/dev/dm-1"),
            ("Device has no multipath", "/dev/sdc", ""),
            (
                "Device has multipath",
                "/dev/disk/by-path/127.0.0.1:3260-eui.02004567A425678D-lun-0",
                "/dev/dm-1",
            ),
            ("Invalid device path", "/dev/nonexistent", ""),
        ] {
            assert_eq!(
                util.find_multipath_device_for_device(device),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn find_device_for_path_cases() {
        let io = Mock;
        assert_eq!(find_device_for_path("/dev/sde", &io).unwrap(), "sde");
        assert_eq!(find_device_for_path("/returns/a/dev", &io).unwrap(), "sde");
        assert!(
            find_device_for_path("/returns/non/dev", &io).is_err(),
            "link is to incorrect dev"
        );
        assert!(
            find_device_for_path("/path/doesnt/exist", &OsIoHandler).is_err(),
            "path shouldn't exist but still doesn't give an error"
        );
    }

    #[test]
    fn find_slave_devices_on_multipath() {
        let util = mock_util();
        assert_eq!(
            util.find_slave_devices_on_multipath("/dev/dm-1"),
            vec!["/dev/sda".to_string(), "/dev/sdb".to_string()]
        );
        assert!(util.find_slave_devices_on_multipath("/dev/sdc").is_empty());
    }

    #[test]
    fn get_iscsi_portal_host_map_for_target() {
        let m = mock_util()
            .get_iscsi_portal_host_map_for_target("target1")
            .unwrap();
        assert_eq!(m.len(), 1, "wrong number of map entries: {m:?}");
        assert_eq!(m["10.0.0.1:3260"], 2, "{m:?}");
    }

    #[test]
    fn find_devices_for_iscsi_lun() {
        let d = mock_util()
            .find_devices_for_iscsi_lun("target1", 1)
            .unwrap();
        assert_eq!(d, vec!["sda".to_string()]);
    }

    /// `net.JoinHostPort` brackets an IPv6 literal.
    #[test]
    fn join_host_port_brackets_ipv6() {
        assert_eq!(join_host_port("10.0.0.1", "3260"), "10.0.0.1:3260");
        assert_eq!(join_host_port("2001:db8::1", "3260"), "[2001:db8::1]:3260");
    }
}
