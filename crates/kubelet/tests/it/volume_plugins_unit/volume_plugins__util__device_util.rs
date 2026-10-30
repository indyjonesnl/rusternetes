//! Unit tests moved from `src/volume_plugins/util/device_util.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::HashMap;
    pub use std::io;
    pub use std::sync::Arc;
    pub use tracing::{error, info};
}

mod tests {
    //! Ports of `device_util_linux_test.go` (`TestFindMultipathDeviceForDevice`,
    //! `TestFindDeviceForPath`, `TestFindSlaveDevicesOnMultipath`,
    //! `TestGetISCSIPortalHostMapForTarget`, `TestFindDevicesForISCSILun`) over
    //! the same `mockOsIOHandler` tables (`:30-196`).
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::device_util::*;

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
