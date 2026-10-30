//! Unit tests moved from `src/volume_plugins/iscsi/util.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, bail, Result};
    pub use rusternetes_common::resources::volume::PersistentVolumeMode;
    pub use rusternetes_kubelet::volume_plugins::iscsi::{
        uncertain_progress, DiskManager, IscsiDisk, IscsiDiskMounter, IscsiDiskUnmapper,
        IscsiDiskUnmounter, ISCSI_PLUGIN_NAME,
    };
    pub use rusternetes_kubelet::volume_plugins::util::device_util::DeviceUtil;
    pub use rusternetes_kubelet::volume_plugins::util::exec::ExecError;
    pub use rusternetes_kubelet::volume_plugins::util::mount_utils::path_exists;
    pub use rusternetes_kubelet::volume_plugins::VolumeHost;
    pub use std::collections::BTreeMap;
    pub use std::io;
    pub use std::io::Write;
    pub use std::os::unix::fs::DirBuilderExt;
    pub use std::path::{Path, PathBuf};
    pub use std::time::Duration;
    pub use tracing::{error, info, warn};
}

mod tests {
    //! Ports of `pkg/volume/iscsi/iscsi_util_test.go`, plus an `AttachDisk`
    //! exec-sequence test the Go suite lacks (it cannot run `AttachDisk`
    //! without a SCSI host).
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::iscsi::disk_manager::DiskManager;
    use rusternetes_kubelet::volume_plugins::iscsi::util::*;
    use rusternetes_kubelet::volume_plugins::iscsi::IscsiPlugin;
    use rusternetes_kubelet::volume_plugins::util::device_util::DeviceUtil;
    use rusternetes_kubelet::volume_plugins::util::exec::testing::{
        script, CommandScript, FakeExec,
    };
    use rusternetes_kubelet::volume_plugins::util::exec::Exec;
    use rusternetes_kubelet::volume_plugins::util::mount_interface::testing::FakeMounter;
    use rusternetes_kubelet::volume_plugins::util::mount_interface::SafeFormatAndMount;
    use rusternetes_kubelet::volume_plugins::KubeletVolumeHost;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    const TEST_IFACE: &str = "192.168.1.10:pv0001";

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn extract_device_and_prefix_test() {
        let device_path = "127.0.0.1:3260-iqn.2014-12.com.example:test.tgt00";
        let mount_prefix =
            format!("/var/lib/kubelet/plugins/kubernetes.io/iscsi/iface-default/{device_path}");
        let lun = "-lun-0";
        let (device, prefix) = extract_device_and_prefix(&format!("{mount_prefix}{lun}")).unwrap();
        assert_eq!(device, format!("{device_path}{lun}"));
        assert_eq!(prefix, mount_prefix);
    }

    #[test]
    fn extract_iface_test() {
        let device_path = "127.0.0.1:3260-iqn.2014-12.com.example:test.tgt00-lun-0";
        let (iface, found) = extract_iface(&format!(
            "/var/lib/kubelet/plugins/kubernetes.io/iscsi/iface-default/{device_path}"
        ));
        assert!(found);
        assert_eq!(iface, "default");
        let (iface, found) = extract_iface(&format!(
            "/var/lib/kubelet/plugins/kubernetes.io/iscsi/{device_path}"
        ));
        assert!(!found);
        assert_eq!(iface, "");
    }

    #[test]
    fn extract_portal_and_iqn_test() {
        for (device, portal, iqn) in [
            (
                "127.0.0.1:3260-iqn.2014-12.com.example:test.tgt00-lun-0",
                "127.0.0.1:3260",
                "iqn.2014-12.com.example:test.tgt00",
            ),
            (
                "127.0.0.1:3260-eui.02004567A425678D-lun-0",
                "127.0.0.1:3260",
                "eui.02004567A425678D",
            ),
            (
                "[2001:db8:0:f101::1]:3260-iqn.2014-12.com.example:test.tgt00-lun-0",
                "[2001:db8:0:f101::1]:3260",
                "iqn.2014-12.com.example:test.tgt00",
            ),
        ] {
            let (p, q) = extract_portal_and_iqn(device).unwrap();
            assert_eq!((p.as_str(), q.as_str()), (portal, iqn));
        }
    }

    #[test]
    fn remove_duplicate_test() {
        let dup = s(&["127.0.0.1:3260", "127.0.0.1:3260", "127.0.0.100:3260"]);
        assert_eq!(
            remove_duplicate(&dup),
            s(&["127.0.0.1:3260", "127.0.0.100:3260"])
        );
    }

    #[test]
    fn extract_transportname_test() {
        let out = |t: Option<&str>| {
            let mut o = String::from("# BEGIN RECORD 2.0-873\niface.iscsi_ifacename = default\n");
            if let Some(t) = t {
                o += &format!("iface.transport_name = {t}\n");
            }
            o + "iface.initiatorname = <empty>\n# END RECORD"
        };
        assert_eq!(extract_transportname(&out(Some("tcp"))), "tcp");
        assert_eq!(extract_transportname(&out(Some("cxgb4i"))), "cxgb4i");
        assert_eq!(extract_transportname(&out(Some("<empty>"))), "tcp");
        assert_eq!(extract_transportname(&out(None)), "");
    }

    #[test]
    fn wait_for_path_to_exist_test() {
        let p0 = "/dev/disk/by-path/ip-127.0.0.1:3260-iqn.2014-12.com.example:test.tgt00-lun-0";
        let p1 =
            "/dev/disk/by-path/pci-*-ip-127.0.0.1:3260-iqn.2014-12.com.example:test.tgt00-lun-0";
        let fpath = "/dev/disk/by-path/pci-0000:00:00.0-ip-127.0.0.1:3260-iqn.2014-12.com.example:test.tgt00-lun-0";
        let fake_stat = |_: &str| Ok(());
        let real_stat = |p: &str| std::fs::metadata(p).map(|_| ());
        let fake_glob = |p: &str| vec![p.to_string()];
        let fake_glob2 = |_: &str| vec![fpath.to_string()];

        let mut d = p0.to_string();
        assert!(wait_for_path_to_exist_internal(
            &mut d,
            1,
            "tcp",
            &fake_stat,
            &filepath_glob
        ));
        let mut d = p0.to_string();
        assert!(!wait_for_path_to_exist_internal(
            &mut d,
            1,
            "fake_iface",
            &fake_stat,
            &filepath_glob
        ));
        let mut d = p1.to_string();
        assert!(wait_for_path_to_exist_internal(
            &mut d,
            1,
            "fake_iface",
            &real_stat,
            &fake_glob
        ));
        let mut d = p1.to_string();
        assert!(!wait_for_path_to_exist_internal(
            &mut d, 1, "tcp", &real_stat, &fake_glob
        ));
        let mut d = p1.to_string();
        let _ = wait_for_path_to_exist_internal(&mut d, 1, "fake_iface", &real_stat, &fake_glob2);
        assert_eq!(d, fpath);
    }

    /// The glob seam the Go test takes from `filepath.Glob`.
    #[test]
    fn filepath_glob_matches_last_component_wildcard() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("pci-0000:00:00.0-ip-1"), "").unwrap();
        std::fs::write(tmp.path().join("pci-0000:00:01.0-ip-1"), "").unwrap();
        std::fs::write(tmp.path().join("other"), "").unwrap();
        let pat = tmp.path().join("pci-*-ip-1").to_string_lossy().into_owned();
        let got = filepath_glob(&pat);
        assert_eq!(got.len(), 2);
        assert!(got[0].ends_with("pci-0000:00:00.0-ip-1"));
        assert!(filepath_glob(&tmp.path().join("nomatch*").to_string_lossy()).is_empty());
    }

    #[test]
    fn parse_iscsiadm_show_test() {
        let rec = |name: &str, transport: &str| {
            format!(
                "# BEGIN RECORD 2.0-873\niface.iscsi_ifacename = {name}\niface.transport_name = {transport}\niface.initiatorname = <empty>\niface.mtu = 0\n# END RECORD"
            )
        };
        let m = |kv: &[(&str, &str)]| {
            kv.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>()
        };
        assert_eq!(
            parse_iscsiadm_show(&rec("default", "tcp")).unwrap(),
            m(&[("iface.transport_name", "tcp"), ("iface.mtu", "0")])
        );
        assert_eq!(
            parse_iscsiadm_show(&rec("default", "cxgb4i")).unwrap(),
            m(&[("iface.transport_name", "cxgb4i"), ("iface.mtu", "0")])
        );
        assert_eq!(
            parse_iscsiadm_show(&rec("custom", "<empty>")).unwrap(),
            m(&[("iface.mtu", "0")])
        );
        assert!(parse_iscsiadm_show("iface.iscsi_ifacename=error").is_err());
        assert!(parse_iscsiadm_show("iface.iscsi_ifacename + error").is_err());
    }

    fn host(tmp: &Path) -> Arc<dyn VolumeHost> {
        Arc::new(KubeletVolumeHost::new_for_tests(
            tmp.to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        ))
    }

    /// `iscsiDiskMounter{iscsiDisk: &iscsiDisk{Iface: TestIface, plugin}, exec}`.
    fn mounter_with(
        tmp: &Path,
        exec: Arc<FakeExec>,
        device_util: Arc<dyn DeviceUtil>,
    ) -> IscsiDiskMounter {
        let plugin = IscsiPlugin::new(host(tmp));
        let mut disk = IscsiDisk::blank("vol", "poduid", Arc::new(IscsiUtil), plugin);
        disk.iface = TEST_IFACE.to_string();
        disk.portals = vec!["127.0.0.1:3260".to_string()];
        disk.iqn = "iqn.2014-12.server:storage.target01".to_string();
        disk.lun = "0".to_string();
        disk.init_iface = "default".to_string();
        let exec_dyn: Arc<dyn Exec> = exec;
        IscsiDiskMounter {
            disk,
            read_only: false,
            fs_type: String::new(),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            mounter: SafeFormatAndMount {
                mounter: Arc::new(FakeMounter::new()),
                exec: exec_dyn.clone(),
                format_limit: Default::default(),
            },
            exec: exec_dyn,
            device_util,
            mount_options: Vec::new(),
            mounted_with_selinux_context: Arc::new(AtomicBool::new(false)),
        }
    }

    fn cmd(args: &[&str]) -> CommandScript {
        script("iscsiadm", args)
    }

    fn iface_show(init_iface: &str, output: &str, rc: i32) -> CommandScript {
        CommandScript {
            output: output.to_string(),
            return_code: rc,
            ..cmd(&["-m", "iface", "-I", init_iface, "-o", "show"])
        }
    }

    struct NoDevices;
    impl DeviceUtil for NoDevices {
        fn find_multipath_device_for_device(&self, _: &str) -> String {
            String::new()
        }
        fn find_slave_devices_on_multipath(&self, _: &str) -> Vec<String> {
            Vec::new()
        }
        fn get_iscsi_portal_host_map_for_target(
            &self,
            _: &str,
        ) -> io::Result<HashMap<String, i32>> {
            Ok(HashMap::new())
        }
        fn find_devices_for_iscsi_lun(&self, _: &str, _: i32) -> io::Result<Vec<String>> {
            Ok(Vec::new())
        }
    }

    const IFACE_OUT: &str =
        "iface.ipaddress = <empty>\niface.transport_name = tcp\niface.initiatorname = <empty>\n";

    fn update_iface(key: &str, value: &str) -> CommandScript {
        cmd(&[
            "-m", "iface", "-I", TEST_IFACE, "-o", "update", "-n", key, "-v", value,
        ])
    }

    #[test]
    fn cloned_iface() {
        let tmp = tempfile::tempdir().unwrap();
        let scripts = vec![
            iface_show("", IFACE_OUT, 0),
            cmd(&["-m", "iface", "-I", TEST_IFACE, "-o", "new"]),
            update_iface("iface.initiatorname", ""),
            update_iface("iface.transport_name", "tcp"),
        ];
        let n = scripts.len();
        let exec = Arc::new(FakeExec::new(scripts));
        let mut m = mounter_with(tmp.path(), exec.clone(), Arc::new(NoDevices));
        m.disk.init_iface = String::new();
        clone_iface(&m).unwrap();
        assert_eq!(exec.command_calls(), n);
    }

    #[test]
    fn cloned_iface_show_error() {
        let tmp = tempfile::tempdir().unwrap();
        let exec = Arc::new(FakeExec::new(vec![iface_show("", "test error", 1)]));
        let mut m = mounter_with(tmp.path(), exec.clone(), Arc::new(NoDevices));
        m.disk.init_iface = String::new();
        assert!(clone_iface(&m).is_err());
        assert_eq!(exec.command_calls(), 1);
    }

    #[test]
    fn cloned_iface_update_error() {
        let tmp = tempfile::tempdir().unwrap();
        let scripts = vec![
            iface_show("", IFACE_OUT, 0),
            cmd(&["-m", "iface", "-I", TEST_IFACE, "-o", "new"]),
            update_iface("iface.initiatorname", ""),
            CommandScript {
                return_code: 1,
                ..update_iface("iface.transport_name", "tcp")
            },
            cmd(&["-m", "iface", "-I", TEST_IFACE, "-o", "delete"]),
        ];
        let n = scripts.len();
        let exec = Arc::new(FakeExec::new(scripts));
        let mut m = mounter_with(tmp.path(), exec.clone(), Arc::new(NoDevices));
        m.disk.init_iface = String::new();
        assert!(clone_iface(&m).is_err());
        assert_eq!(exec.command_calls(), n);
    }

    fn create_fake_plugin_dirs() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for d in [
            "iface-127.0.0.1:3260:pv1/127.0.0.1:3260-iqn.2003-01.io.k8s:e2e.volume-1-lun-3",
            "iface-127.0.0.1:3260:pv2/127.0.0.1:3260-iqn.2003-01.io.k8s:e2e.volume-1-lun-2",
            "iface-127.0.0.1:3260:pv2/192.168.0.1:3260-iqn.2003-01.io.k8s:e2e.volume-1-lun-1",
            "volumeDevices/iface-127.0.0.1:3260/192.168.0.2:3260-iqn.2003-01.io.k8s:e2e.volume-1-lun-4",
            "volumeDevices/iface-127.0.0.1:3260/192.168.0.3:3260-iqn.2003-01.io.k8s:e2e.volume-1-lun-5",
        ] {
            std::fs::create_dir_all(dir.path().join(d)).unwrap();
        }
        dir
    }

    #[test]
    fn get_vol_count_test() {
        let dir = create_fake_plugin_dirs();
        let base = dir.path();
        let vd = base.join(DEFAULT_KUBELET_VOLUME_DEVICES_DIR_NAME);
        let v1 = "iqn.2003-01.io.k8s:e2e.volume-1";
        for (name, base_dir, portal, iqn, count) in [
            (
                "wrong portal, no volumes",
                base.to_path_buf(),
                "192.168.0.2:3260",
                v1,
                0,
            ),
            (
                "wrong iqn, no volumes",
                base.to_path_buf(),
                "127.0.0.1:3260",
                "iqn.2003-01.io.k8s:e2e.volume-3",
                0,
            ),
            (
                "single volume",
                base.to_path_buf(),
                "192.168.0.1:3260",
                v1,
                1,
            ),
            ("two volumes", base.to_path_buf(), "127.0.0.1:3260", v1, 2),
            (
                "volumeDevices (block) volume",
                vd,
                "192.168.0.2:3260",
                "iqn.2003-01.io.k8s:e2e.volume-1-lun-4",
                1,
            ),
            (
                "nonexistent path",
                base.join("this_path_should_not_exist"),
                "127.0.0.1:3260",
                "iqn.2003-01.io.k8s:e2e.unknown",
                0,
            ),
        ] {
            let got = get_vol_count(&base_dir.to_string_lossy(), portal, iqn).unwrap();
            assert_eq!(got, count, "{name}");
        }
    }

    /// `AttachDisk` against a portal with no SCSI host: the exact `iscsiadm`
    /// login sequence of `iscsi_util.go:289-483`, then, the refreshed host map
    /// still lacking the portal, the first round ends with no path attached:
    /// the (cloned) iface is deleted and the attach fails without retrying
    /// (`iscsi_util.go:432-438`).
    #[test]
    fn attach_disk_logs_in_every_round_then_deletes_the_iface() {
        let tmp = tempfile::tempdir().unwrap();
        let tp = "127.0.0.1:3260";
        let iqn = "iqn.2014-12.server:storage.target01";
        let mut scripts = vec![iface_show("default", IFACE_OUT, 0)];
        for _ in 0..1 {
            scripts.extend([
                cmd(&[
                    "-m",
                    "discoverydb",
                    "-t",
                    "sendtargets",
                    "-p",
                    tp,
                    "-I",
                    TEST_IFACE,
                    "-o",
                    "new",
                ]),
                cmd(&[
                    "-m",
                    "discoverydb",
                    "-t",
                    "sendtargets",
                    "-p",
                    tp,
                    "-I",
                    TEST_IFACE,
                    "--discover",
                ]),
                cmd(&[
                    "-m",
                    "node",
                    "-p",
                    tp,
                    "-T",
                    iqn,
                    "-I",
                    TEST_IFACE,
                    "-o",
                    "update",
                    "-n",
                    "node.session.scan",
                    "-v",
                    "manual",
                ]),
                cmd(&[
                    "-m", "node", "-p", tp, "-T", iqn, "-I", TEST_IFACE, "--login",
                ]),
                cmd(&[
                    "-m",
                    "node",
                    "-p",
                    tp,
                    "-T",
                    iqn,
                    "-o",
                    "update",
                    "-n",
                    "node.startup",
                    "-v",
                    "manual",
                ]),
            ]);
        }
        scripts.push(cmd(&["-m", "iface", "-I", TEST_IFACE, "-o", "delete"]));
        let exec = Arc::new(FakeExec::new(scripts));
        let m = mounter_with(tmp.path(), exec.clone(), Arc::new(NoDevices));
        let err = IscsiUtil.attach_disk(&m).unwrap_err().to_string();
        assert!(
            err.contains("failed to get any path for iscsi disk"),
            "{err}"
        );
        assert_eq!(exec.command_calls(), 1 + 5 + 1);
    }

    /// `persistISCSIFile` / `loadISCSI` round trip through the exact
    /// `encoding/json` field names (`iscsi_util.go:189-217`).
    #[test]
    fn iscsi_json_round_trip_uses_upstream_field_names() {
        let tmp = tempfile::tempdir().unwrap();
        let exec = Arc::new(FakeExec::new(vec![]));
        let mut m = mounter_with(tmp.path(), exec, Arc::new(NoDevices));
        m.disk.initiator_name = "iqn.init".to_string();
        IscsiUtil
            .persist_iscsi_file(&m.disk, &tmp.path().to_string_lossy())
            .unwrap();
        let raw = std::fs::read_to_string(tmp.path().join("iscsi.json")).unwrap();
        for k in [
            "\"VolName\"",
            "\"Portals\"",
            "\"Iqn\"",
            "\"Lun\"",
            "\"InitIface\"",
            "\"Iface\"",
            "\"InitiatorName\"",
            "\"MetricsProvider\":null",
        ] {
            assert!(raw.contains(k), "{k} missing from {raw}");
        }
        let mut loaded = IscsiDisk::blank("", "", Arc::new(IscsiUtil), m.disk.plugin.clone());
        IscsiUtil
            .load_iscsi(&mut loaded, &tmp.path().to_string_lossy())
            .unwrap();
        assert_eq!(loaded.portals, m.disk.portals);
        assert_eq!(loaded.iqn, m.disk.iqn);
        assert_eq!(loaded.initiator_name, "iqn.init");
        assert_eq!(loaded.iface, TEST_IFACE);
    }

    #[test]
    fn ignore_exit_codes_swallows_only_listed_exit_errors() {
        let exit = |code| {
            Err(ExecError::Exit {
                code,
                message: String::new(),
            })
        };
        assert!(ignore_exit_codes(exit(21), &[21, 2]).is_ok());
        assert!(ignore_exit_codes(exit(2), &[21, 2]).is_ok());
        assert!(ignore_exit_codes(exit(1), &[21, 2]).is_err());
        assert!(ignore_exit_codes(Err(ExecError::NotFound), &[21, 2]).is_err());
        assert!(ignore_exit_codes(Ok(()), &[21, 2]).is_ok());
    }
}
