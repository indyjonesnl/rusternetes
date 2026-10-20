//! Ports of `pkg/volume/fc/fc_test.go` (`TestCanSupport`, `TestGetAccessModes`,
//! `doTestPlugin`, `doTestPluginNilMounter`, `TestPluginVolume`,
//! `TestPluginPersistentVolume`, `TestPluginVolumeWWIDs`,
//! `TestPluginPersistentVolumeWWIDs`, `TestPluginVolumeNoDiskInfo`,
//! `TestPluginPersistentVolumeNoDiskInfo`, `TestPersistentClaimReadOnlyFlag`,
//! `Test_getWwnsLun`, `Test_getWwids`, `Test_getWwnsLunWwidsError`,
//! `Test_ConstructVolumeSpec`, `Test_ConstructVolumeSpecNoRefs`),
//! `fc_util_test.go` (`TestSearchDisk`, `TestSearchDiskWWID`,
//! `TestParsePDName`) and `fc_util_linux_test.go`
//! (`TestSearchDiskMultipathDevice`), plus tests for the parts upstream
//! leaves untested (`DetachDisk`, `DetachBlockFCDisk`'s guards,
//! `UnmountDevice`, the global paths).
use super::util::search_disk;
use super::*;
use crate::volume_plugins::plugin::{DeviceMountableVolumePlugin, DeviceMounterArgs};
use crate::volume_plugins::util::device_util::{new_device_handler, IoUtil};
use crate::volume_plugins::util::exec::testing::{no_commands, script, FakeExec};
use crate::volume_plugins::util::mount_interface::testing::FakeMounter;
use crate::volume_plugins::Mounter as _;
use crate::volume_plugins::{KubeletVolumeHost, VolumePluginMgr};
use rusternetes_common::resources::{PersistentVolume, Volume};
use serde_json::json;
use std::collections::HashMap;
use std::io;
use std::sync::Mutex;

fn host(tmp: &std::path::Path) -> Arc<dyn VolumeHost> {
    Arc::new(KubeletVolumeHost::new(
        tmp.to_string_lossy().to_string(),
        None,
        rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        HashMap::new(),
    ))
}

fn vol(name: &str, source: serde_json::Value) -> Volume {
    serde_json::from_value(json!({"name": name, "fc": source})).unwrap()
}

fn bare(name: &str) -> Volume {
    serde_json::from_value(json!({"name": name})).unwrap()
}

fn pv(v: serde_json::Value) -> PersistentVolume {
    serde_json::from_value(v).unwrap()
}

fn pod() -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": "p", "uid": "poduid"},
        "spec": {"containers": []}
    }))
    .unwrap()
}

fn spec<'a>(volume: &'a Volume, pv: Option<&'a PersistentVolume>, ro: bool) -> Spec<'a> {
    Spec {
        volume,
        persistent_volume: pv,
        read_only: ro,
        migrated: false,
        inline_volume_spec_for_csi_migration: false,
    }
}

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

/// `TestCanSupport`.
#[test]
fn can_support() {
    let tmp = tempfile::tempdir().unwrap();
    let mgr = VolumePluginMgr::new(vec![Box::new(FcPlugin::new(host(tmp.path())))]);
    let plug = mgr.find_plugin_by_name("kubernetes.io/fc").unwrap();
    assert_eq!(plug.name(), "kubernetes.io/fc");

    let none = bare("x");
    assert!(!plug.can_support(&spec(&none, None, false)));
    let v = vol("x", json!({}));
    assert!(plug.can_support(&spec(&v, None, false)));
    let empty_pv = pv(json!({"metadata": {"name": "pv"}, "spec": {}}));
    assert!(!plug.can_support(&spec(&none, Some(&empty_pv), false)));
    let fc_pv = pv(json!({"metadata": {"name": "pv"}, "spec": {"fc": {}}}));
    assert!(plug.can_support(&spec(&none, Some(&fc_pv), false)));
}

/// `TestGetAccessModes`.
#[test]
fn get_access_modes() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let modes = plug.get_access_modes();
    assert!(modes.contains(&PersistentVolumeAccessMode::ReadWriteOnce));
    assert!(modes.contains(&PersistentVolumeAccessMode::ReadOnlyMany));
}

/// `fakeDiskManager` (`fc_test.go:95-146`).
struct FakeDiskManager {
    tmp_dir: tempfile::TempDir,
}

impl FakeDiskManager {
    fn new() -> Self {
        Self {
            tmp_dir: tempfile::tempdir().unwrap(),
        }
    }
}

impl DiskManager for FakeDiskManager {
    fn make_global_pd_name(&self, _: &FcDisk) -> String {
        self.tmp_dir.path().to_string_lossy().into_owned()
    }

    fn make_global_vdpd_name(&self, _: &FcDisk) -> String {
        self.tmp_dir.path().to_string_lossy().into_owned()
    }

    fn attach_disk(&self, b: &FcDiskMounter) -> Result<String> {
        let global_path = b.disk.manager.make_global_pd_name(&b.disk);
        std::fs::create_dir_all(&global_path)?;
        Ok(String::new())
    }

    fn detach_disk(&self, c: &FcDiskUnmounter, _: &str) -> Result<()> {
        let global_path = c.disk.manager.make_global_pd_name(&c.disk);
        std::fs::remove_dir_all(global_path)?;
        Ok(())
    }

    fn detach_block_fc_disk(&self, _c: &FcDiskUnmapper, map_path: &str, _: &str) -> Result<()> {
        std::fs::remove_dir_all(map_path)?;
        Ok(())
    }
}

/// `doTestPlugin` (`fc_test.go:148-209`).
async fn do_test_plugin(spec: &Spec<'_>) {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let fake_manager = Arc::new(FakeDiskManager::new());
    let fake_mounter = Arc::new(FakeMounter::new());
    let fake_exec = Arc::new(no_commands());
    let mounter = plug
        .new_mounter_internal(
            spec,
            "poduid",
            fake_manager,
            fake_mounter.clone(),
            fake_exec.clone(),
        )
        .unwrap();

    let path = mounter.get_path();
    let expected_path = tmp
        .path()
        .join("pods/poduid/volumes/kubernetes.io~fc/vol1")
        .to_string_lossy()
        .into_owned();
    assert_eq!(path, expected_path);

    mounter.set_up().await.unwrap();
    assert!(
        std::path::Path::new(&path).exists(),
        "SetUp() failed, volume path not created: {path}"
    );

    let fake_manager2 = Arc::new(FakeDiskManager::new());
    let unmounter =
        plug.new_unmounter_internal("vol1", "poduid", fake_manager2, fake_mounter, fake_exec);

    unmounter.tear_down().await.unwrap();
    assert!(
        !std::path::Path::new(&path).exists(),
        "TearDown() failed, volume path still exists: {path}"
    );
}

/// `doTestPluginNilMounter` (`fc_test.go:211-236`).
fn do_test_plugin_nil_mounter(spec: &Spec<'_>) {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let err = plug
        .new_mounter_internal(
            spec,
            "poduid",
            Arc::new(FakeDiskManager::new()),
            Arc::new(FakeMounter::new()),
            Arc::new(no_commands()),
        )
        .err()
        .expect("Error failed to make a new Mounter is expected");
    assert_eq!(
        err.to_string(),
        "fc: no fc disk information found. failed to make a new mounter"
    );
}

fn fc_pv(name: &str, source: serde_json::Value) -> PersistentVolume {
    pv(json!({"metadata": {"name": name},
        "spec": {"fc": source, "volumeMode": "Filesystem"}}))
}

/// `TestPluginVolume`.
#[tokio::test]
async fn plugin_volume() {
    let v = vol(
        "vol1",
        json!({"targetWWNs": ["500a0981891b8dc5"], "fsType": "ext4", "lun": 0}),
    );
    do_test_plugin(&spec(&v, None, false)).await;
}

/// `TestPluginPersistentVolume`.
#[tokio::test]
async fn plugin_persistent_volume() {
    let p = fc_pv(
        "vol1",
        json!({"targetWWNs": ["500a0981891b8dc5"], "fsType": "ext4", "lun": 0}),
    );
    let placeholder = bare("vol1");
    do_test_plugin(&spec(&placeholder, Some(&p), false)).await;
}

/// `TestPluginVolumeWWIDs`.
#[tokio::test]
async fn plugin_volume_wwids() {
    let v = vol(
        "vol1",
        json!({"wwids": ["3600508b400105e210000900000490000"], "fsType": "ext4"}),
    );
    do_test_plugin(&spec(&v, None, false)).await;
}

/// `TestPluginPersistentVolumeWWIDs`.
#[tokio::test]
async fn plugin_persistent_volume_wwids() {
    let p = fc_pv(
        "vol1",
        json!({"wwids": ["3600508b400105e21 000900000490000"], "fsType": "ext4"}),
    );
    let placeholder = bare("vol1");
    do_test_plugin(&spec(&placeholder, Some(&p), false)).await;
}

/// `TestPluginVolumeNoDiskInfo`.
#[test]
fn plugin_volume_no_disk_info() {
    let v = vol("vol1", json!({"fsType": "ext4"}));
    do_test_plugin_nil_mounter(&spec(&v, None, false));
}

/// `TestPluginPersistentVolumeNoDiskInfo`.
#[test]
fn plugin_persistent_volume_no_disk_info() {
    let p = fc_pv("vol1", json!({"fsType": "ext4"}));
    let placeholder = bare("vol1");
    do_test_plugin_nil_mounter(&spec(&placeholder, Some(&p), false));
}

/// `TestPersistentClaimReadOnlyFlag`.
#[tokio::test]
async fn persistent_claim_read_only_flag() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let p = pv(json!({"metadata": {"name": "pvA"},
        "spec": {"fc": {"targetWWNs": ["some_wwn"], "fsType": "ext4", "lun": 0},
                 "claimRef": {"name": "claimA"}, "volumeMode": "Filesystem"}}));
    // readOnly bool is supplied by persistent-claim volume source when its mounter creates other volumes
    let placeholder = bare("pvA");
    let m = plug
        .new_mounter(&spec(&placeholder, Some(&p), true), &pod())
        .await
        .unwrap();
    assert!(m.get_attributes().read_only);
}

/// A PV without a `volumeMode` makes `util.GetVolumeMode` fail
/// (`pkg/volume/util/util.go:346`), and `newMounterInternal` returns that error.
#[tokio::test]
async fn pv_without_volume_mode_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let p = pv(json!({"metadata": {"name": "pvA"},
        "spec": {"fc": {"targetWWNs": ["w"], "lun": 0}}}));
    let placeholder = bare("pvA");
    let err = plug
        .new_mounter(&spec(&placeholder, Some(&p), false), &pod())
        .await
        .err()
        .unwrap();
    assert_eq!(err.to_string(), "cannot get volumeMode for volume: pvA");
}

/// `Test_getWwnsLun`.
#[test]
fn get_wwns_lun() {
    let fc = FCVolumeSource {
        target_wwns: Some(s(&["500a0981891b8dc5"])),
        fs_type: Some("ext4".into()),
        lun: Some(0),
        ..Default::default()
    };
    let (wwn, lun, _) = get_wwns_lun_wwids(&fc).unwrap();
    assert!(!(wwn.is_empty() && lun != "0"));
    assert_eq!(wwn, s(&["500a0981891b8dc5"]));
    assert_eq!(lun, "0");
}

/// `Test_getWwids`: a space in a WWID becomes `_` (`fc.go:519`).
#[test]
fn get_wwids() {
    let fc = FCVolumeSource {
        fs_type: Some("ext4".into()),
        wwids: Some(s(&["3600508b400105e210000900000490000", "36005 08b"])),
        ..Default::default()
    };
    let (_, _, wwid) = get_wwns_lun_wwids(&fc).unwrap();
    assert_eq!(wwid, s(&["3600508b400105e210000900000490000", "36005_08b"]));
}

/// `Test_getWwnsLunWwidsError`.
#[test]
fn get_wwns_lun_wwids_error() {
    let fc = FCVolumeSource {
        fs_type: Some("ext4".into()),
        ..Default::default()
    };
    assert_eq!(
        get_wwns_lun_wwids(&fc).unwrap_err().to_string(),
        "fc: no fc disk information found"
    );
}

/// `getWwnsLunWwids` returns `fc.TargetWWNs` unchanged on the wwids branch,
/// when a LUN is missing (`fc.go:517-521`).
#[test]
fn get_wwns_lun_wwids_wwid_branch_keeps_target_wwns() {
    let fc = FCVolumeSource {
        target_wwns: Some(s(&["w1"])),
        lun: None,
        wwids: Some(s(&["id"])),
        ..Default::default()
    };
    let (wwns, lun, wwids) = get_wwns_lun_wwids(&fc).unwrap();
    assert_eq!((wwns, lun, wwids), (s(&["w1"]), String::new(), s(&["id"])));
}

/// `Test_ConstructVolumeSpec` (`fc_test.go:434-485`): the mount-refs lookup the
/// plugin's `ConstructVolumeSpec` performs, over a `FakeMounter`.
#[test]
fn construct_volume_spec_refs() {
    let fm = FakeMounter::new();
    for (device, path) in [
        (
            "/dev/sdb",
            "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod1",
        ),
        (
            "/dev/sdb",
            "/var/lib/kubelet/plugins/kubernetes.io/fc/50060e801049cfd1-lun-0",
        ),
        (
            "/dev/sdc",
            "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod2",
        ),
        (
            "/dev/sdc",
            "/var/lib/kubelet/plugins/kubernetes.io/fc/volumeDevices/3600508b400105e210000900000490000",
        ),
    ] {
        fm.mount_points
            .lock()
            .unwrap()
            .push(crate::volume_plugins::util::mount_interface::testing::MountPoint {
                device: device.into(),
                path: path.into(),
                fstype: String::new(),
                opts: vec![],
            });
    }
    for path in [
        "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod1",
        "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod2",
    ] {
        let refs = MountInterface::get_mount_refs(&fm, path).unwrap();
        let global_pd_path = refs
            .iter()
            .find(|r| r.contains("kubernetes.io/fc"))
            .expect("couldn't fetch mountrefs");
        let volume_info = global_pd_path.rsplit('/').next().unwrap();
        let (wwns, lun, wwids) = util::parse_pd_name(global_pd_path).unwrap();
        if volume_info.contains("-lun-") {
            assert_eq!((wwns, lun, wwids), (s(&["50060e801049cfd1"]), 0, vec![]));
        } else {
            assert_eq!(volume_info, "3600508b400105e210000900000490000");
            assert_eq!(
                (wwns, lun, wwids),
                (vec![], 0, s(&["3600508b400105e210000900000490000"]))
            );
        }
    }
}

/// `Test_ConstructVolumeSpecNoRefs` (`fc_test.go:487-508`).
#[test]
fn construct_volume_spec_no_refs() {
    let fm = FakeMounter::new();
    fm.mount_points.lock().unwrap().push(
        crate::volume_plugins::util::mount_interface::testing::MountPoint {
            device: "/dev/sdd".into(),
            path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod1".into(),
            fstype: String::new(),
            opts: vec![],
        },
    );
    let refs = MountInterface::get_mount_refs(
        &fm,
        "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~fc/fc-in-pod1",
    )
    .unwrap();
    assert!(refs.iter().all(|r| !r.contains("kubernetes.io/fc")));
}

/// `fakeIOHandler` (`fc_util_test.go:56-144`), which upstream also hands to
/// `util.NewDeviceHandler` as its `IoUtil`. Writes are recorded.
#[derive(Default)]
struct FakeIoHandler {
    writes: Mutex<Vec<(String, Vec<u8>)>>,
}

impl IoHandler for FakeIoHandler {
    fn read_dir(&self, dirname: &str) -> io::Result<Vec<String>> {
        Ok(match dirname {
            "/dev/disk/by-path/" => vec![
                "pci-0000:41:00.0-fc-0x500a0981891b8dc5-lun-12",
                "pci-0000:41:00.0-fc-0x500a0981891b8dc5-lun-1",
                "fc-0x5005076810213b32-lun-25",
                "pci-0000:41:00.0-fc-0x500a0981891b8dc5-lun-0",
                "fc-0x5005076810213b32-lun-2",
                "abc-0000:41:00.0-fc-0x5005076810213404-lun-0",
                "fc-0x500507681021a537-lun-0",
                "fc-0x500507681022a554-lun-2",
            ],
            "/sys/block/" => vec!["dm-1"],
            "/dev/disk/by-id/" => vec!["scsi-3600508b400105e210000900000490000"],
            _ => vec![],
        }
        .into_iter()
        .map(str::to_string)
        .collect())
    }

    fn eval_symlinks(&self, path: &str) -> io::Result<String> {
        // `return "", nil` for anything unlisted.
        Ok(match path {
            "/dev/disk/by-path/pci-0000:41:00.0-fc-0x500a0981891b8dc5-lun-0" => "/dev/sda",
            "/dev/disk/by-path/pci-0000:41:00.0-fc-0x500a0981891b8dc5-lun-1" => "/dev/sdb",
            "/dev/disk/by-path/fc-0x5005076810213b32-lun-2" => "/dev/sdc",
            "/dev/disk/by-path/pci-0000:41:00.0-fc-0x500a0981891b8dc5-lun-12" => "/dev/sdl",
            "/dev/disk/by-path/fc-0x5005076810213b32-lun-25" => "/dev/sdx",
            "/dev/disk/by-id/scsi-3600508b400105e210000900000490000" => "/dev/sdd",
            "/dev/disk/by-path/fc-0x500507681021a537-lun-0" => "/dev/sde",
            "/dev/disk/by-path/fc-0x500507681022a554-lun-2" => "/dev/sdf",
            "/dev/sde" => "/dev/sde",
            "/dev/sdf" => "/dev/sdf",
            _ => "",
        }
        .to_string())
    }

    fn write_file(&self, filename: &str, data: &[u8], _perm: u32) -> io::Result<()> {
        self.writes
            .lock()
            .unwrap()
            .push((filename.to_string(), data.to_vec()));
        Ok(())
    }
}

impl IoUtil for FakeIoHandler {
    fn read_file(&self, _filename: &str) -> io::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn read_dir(&self, dirname: &str) -> io::Result<Vec<String>> {
        IoHandler::read_dir(self, dirname)
    }

    fn lstat(&self, name: &str) -> io::Result<()> {
        if [
            "/sys/block/dm-1/slaves/sde",
            "/sys/block/dm-1/slaves/sdf",
            "/sys/block/dm-1/slaves/sdg",
        ]
        .contains(&name)
        {
            Ok(())
        } else {
            Err(io::Error::other("device not found for mock"))
        }
    }

    fn eval_symlinks(&self, path: &str) -> io::Result<String> {
        IoHandler::eval_symlinks(self, path)
    }
}

/// The `fcDiskMounter{fcDisk: &fcDisk{wwns, lun, wwids, io}, deviceUtil}` literal
/// of the `fc_util*_test.go` tests.
fn search_mounter(
    wwns: &[&str],
    lun: &str,
    wwids: &[&str],
    io: Arc<dyn IoHandler>,
    device_util: Arc<dyn DeviceUtil>,
) -> FcDiskMounter {
    let tmp = tempfile::tempdir().unwrap();
    let plugin = FcPlugin::new(host(tmp.path()));
    FcDiskMounter {
        disk: FcDisk {
            vol_name: String::new(),
            pod_uid: String::new(),
            wwns: s(wwns),
            lun: lun.to_string(),
            wwids: s(wwids),
            plugin,
            manager: Arc::new(FcUtil),
            io,
        },
        read_only: false,
        fs_type: String::new(),
        volume_mode: PersistentVolumeMode::Filesystem,
        mounter: SafeFormatAndMount {
            mounter: Arc::new(FakeMounter::new()),
            exec: Arc::new(no_commands()),
            format_limit: Default::default(),
        },
        device_util,
        mount_options: vec![],
        mounted_with_selinux_context: Arc::new(AtomicBool::new(false)),
    }
}

fn fake_device_util() -> Arc<dyn DeviceUtil> {
    Arc::new(new_device_handler(Arc::new(FakeIoHandler::default())))
}

/// `TestSearchDisk`.
#[test]
fn search_disk_cases() {
    for (name, wwns, lun, disk, expect_error) in [
        ("PCI disk 0", "500a0981891b8dc5", "0", "/dev/sda", false),
        ("PCI disk 1", "500a0981891b8dc5", "1", "/dev/sdb", false),
        ("Non PCI disk", "5005076810213b32", "2", "/dev/sdc", false),
        (
            "Invalid Storage Controller",
            "5005076810213404",
            "0",
            "",
            true,
        ),
        ("Non existing disk", "500507681fffffff", "0", "", true),
    ] {
        let m = search_mounter(
            &[wwns],
            lun,
            &[],
            Arc::new(FakeIoHandler::default()),
            fake_device_util(),
        );
        let result = search_disk(&m);
        assert_eq!(result.is_err(), expect_error, "{name}: {result:?}");
        assert_eq!(result.unwrap_or_default(), disk, "{name}");
    }
}

/// `TestSearchDiskWWID`: upstream hands the OS `IoUtil` to the device handler.
#[test]
fn search_disk_wwid() {
    let m = search_mounter(
        &[],
        "",
        &["3600508b400105e210000900000490000"],
        Arc::new(FakeIoHandler::default()),
        new_os_device_handler(),
    );
    let device_path = search_disk(&m).expect("no fc disk found");
    assert!(!device_path.is_empty());
}

/// `TestSearchDiskMultipathDevice`.
#[test]
fn search_disk_multipath_device() {
    for (name, wwns, lun) in [
        ("Non PCI disk 0", "500507681021a537", "0"),
        ("Non PCI disk 1", "500507681022a554", "2"),
    ] {
        let m = search_mounter(
            &[wwns],
            lun,
            &[],
            Arc::new(FakeIoHandler::default()),
            fake_device_util(),
        );
        assert_eq!(search_disk(&m).unwrap(), "/dev/dm-1", "{name}");
    }
}

/// A miss rescans the SCSI bus once (`fc_util.go:214-238`): `- - -` is written
/// to each host's `scan`.
#[test]
fn search_disk_rescans_the_scsi_bus_once_on_a_miss() {
    struct Hosts(FakeIoHandler);
    impl IoHandler for Hosts {
        fn read_dir(&self, dirname: &str) -> io::Result<Vec<String>> {
            if dirname == "/sys/class/scsi_host/" {
                return Ok(s(&["host0", "host1"]));
            }
            IoHandler::read_dir(&self.0, dirname)
        }
        fn eval_symlinks(&self, path: &str) -> io::Result<String> {
            IoHandler::eval_symlinks(&self.0, path)
        }
        fn write_file(&self, f: &str, d: &[u8], p: u32) -> io::Result<()> {
            self.0.write_file(f, d, p)
        }
    }
    let io = Arc::new(Hosts(FakeIoHandler::default()));
    let m = search_mounter(
        &["500507681fffffff"],
        "0",
        &[],
        io.clone(),
        fake_device_util(),
    );
    assert_eq!(search_disk(&m).unwrap_err().to_string(), "no fc disk found");
    assert_eq!(
        *io.0.writes.lock().unwrap(),
        vec![
            (
                "/sys/class/scsi_host/host0/scan".to_string(),
                b"- - -".to_vec()
            ),
            (
                "/sys/class/scsi_host/host1/scan".to_string(),
                b"- - -".to_vec()
            ),
        ]
    );
}

/// `TestParsePDName`.
#[test]
fn parse_pd_name_cases() {
    type Case = (
        &'static str,
        &'static str,
        Vec<String>,
        i32,
        Vec<String>,
        bool,
    );
    let cases: Vec<Case> = vec![
        (
            "single WWID",
            "/var/lib/kubelet/plugins/kubernetes.io/fc/60050763008084e6e0000000000001ae",
            vec![],
            0,
            s(&["60050763008084e6e0000000000001ae"]),
            false,
        ),
        (
            "multiple WWID",
            "/var/lib/kubelet/plugins/kubernetes.io/fc/60050763008084e6e0000000000001ae-60050763008084e6e0000000000001af",
            vec![],
            0,
            s(&[
                "60050763008084e6e0000000000001ae",
                "60050763008084e6e0000000000001af",
            ]),
            false,
        ),
        (
            "single WWN",
            "/var/lib/kubelet/plugins/kubernetes.io/fc/50050768030539b6-lun-0",
            s(&["50050768030539b6"]),
            0,
            vec![],
            false,
        ),
        (
            "multiple WWNs",
            "/var/lib/kubelet/plugins/kubernetes.io/fc/50050768030539b6-50050768030539b7-lun-0",
            s(&["50050768030539b6", "50050768030539b7"]),
            0,
            vec![],
            false,
        ),
        (
            "no WWNs",
            "/var/lib/kubelet/plugins/kubernetes.io/fc/lun-0",
            vec![],
            0,
            vec![],
            true,
        ),
        (
            "invalid lun",
            "/var/lib/kubelet/plugins/kubernetes.io/fc/50050768030539b6-lun-x",
            vec![],
            0,
            vec![],
            true,
        ),
    ];
    for (name, path, wwns, lun, wwids, expect_error) in cases {
        match util::parse_pd_name(path) {
            Ok(got) => {
                assert!(!expect_error, "{name}: expected error but got none");
                assert_eq!(got, (wwns, lun, wwids), "{name}");
            }
            Err(e) => assert!(expect_error, "{name}: got unexpected error: {e}"),
        }
    }
    assert_eq!(
        util::parse_pd_name("/p/50050768030539b6-lun-x")
            .unwrap_err()
            .to_string(),
        "strconv.Atoi: parsing \"x\": invalid syntax"
    );
}

/// `GetVolumeName` (`fc.go:68-86`): Go's `%v` of the WWN list and the LUN, or
/// of the WWID list; `("", nil)` when neither is set.
#[test]
fn get_volume_name_formats_like_go() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let v = vol("v", json!({"targetWWNs": ["w1", "w2"], "lun": 3}));
    assert_eq!(
        plug.get_volume_name(&spec(&v, None, false)).unwrap(),
        "[w1 w2]:3"
    );
    let v = vol("v", json!({"wwids": ["a", "b"]}));
    assert_eq!(
        plug.get_volume_name(&spec(&v, None, false)).unwrap(),
        "[a b]"
    );
    let v = vol("v", json!({"fsType": "ext4"}));
    assert_eq!(plug.get_volume_name(&spec(&v, None, false)).unwrap(), "");
    let none = bare("x");
    assert_eq!(
        plug.get_volume_name(&spec(&none, None, false))
            .unwrap_err()
            .to_string(),
        "Spec does not reference a FibreChannel volume type"
    );
}

/// `makePDNameInternal` / `makeVDPDNameInternal` (`fc_util.go:145-161`) and the
/// global paths that use them (`attacher.go:99`, `fc.go:367`).
#[test]
fn global_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let attacher = plug.new_attacher_internal();
    let base = tmp.path().join("plugins/kubernetes.io/fc");

    let v = vol(
        "vol1",
        json!({"targetWWNs": ["50060e801049cfd1", "50060e801049cfd2"], "lun": 7}),
    );
    assert_eq!(
        attacher
            .get_device_mount_path(&spec(&v, None, false))
            .unwrap(),
        base.join("50060e801049cfd1-50060e801049cfd2-lun-7")
            .to_string_lossy()
    );
    // block: plugins/kubernetes.io/fc/volumeDevices/{wwn-lun}
    assert_eq!(
        FcDisk::blank("vol1", "poduid", Arc::new(FcUtil), plug.clone())
            .get_global_map_path(&spec(&v, None, false))
            .unwrap(),
        base.join("volumeDevices/50060e801049cfd1-50060e801049cfd2-lun-7")
            .to_string_lossy()
    );

    // WWIDs: a space becomes `_`, several are joined by `-`.
    let v = vol("vol1", json!({"wwids": ["3600508b4001 05e2", "36005"]}));
    assert_eq!(
        attacher
            .get_device_mount_path(&spec(&v, None, false))
            .unwrap(),
        base.join("3600508b4001_05e2-36005").to_string_lossy()
    );

    // `FindAttachablePluginBySpec` / `FindDeviceMountablePluginBySpec`.
    let mgr = VolumePluginMgr::new(vec![Box::new(plug.clone())]);
    let found = mgr
        .find_attachable_plugin_by_spec(&spec(&v, None, false))
        .and_then(|p| p.as_attachable_plugin())
        .unwrap();
    assert!(found.new_attacher().is_ok());
    assert!(found.new_device_unmounter().is_ok());
    assert!(DeviceMountableVolumePlugin::new_device_mounter(&plug).is_ok());
    assert_eq!(
        DeviceMountableVolumePlugin::get_device_mount_refs(&plug, "/nonexistent").unwrap(),
        Vec::<String>::new()
    );
}

/// `getVolumeSource` (`fc.go:482-493`): inline `readOnly` vs the PV's
/// `spec.ReadOnly`.
#[test]
fn read_only_comes_from_the_right_place() {
    let v = vol(
        "v",
        json!({"targetWWNs": ["w"], "lun": 0, "readOnly": true}),
    );
    assert!(get_volume_source(&spec(&v, None, false)).unwrap().1);
    let p = fc_pv(
        "p",
        json!({"targetWWNs": ["w"], "lun": 0, "readOnly": true}),
    );
    let placeholder = bare("p");
    // A PV's own `readOnly` is not consulted; `spec.ReadOnly` is.
    assert!(
        !get_volume_source(&spec(&placeholder, Some(&p), false))
            .unwrap()
            .1
    );
    assert!(
        get_volume_source(&spec(&placeholder, Some(&p), true))
            .unwrap()
            .1
    );
}

/// `ConstructBlockVolumeSpec` tail (`fc.go:326-339`), `createPersistentVolumeFromFCVolumeSource`
/// (`fc.go:495-508`): a Block-mode PV carrying the parsed source.
#[test]
fn block_volume_spec_from_global_pd_path() {
    let (wwns, lun, wwids) =
        util::parse_pd_name("plugins/kubernetes.io/fc/volumeDevices/50060e801049cfd1-lun-0")
            .unwrap();
    let spec = create_persistent_volume_from_fc_volume_source(
        "pv0001",
        FCVolumeSource {
            target_wwns: Some(wwns),
            lun: Some(lun),
            wwids: Some(wwids),
            ..Default::default()
        },
    )
    .unwrap();
    let pv = spec.persistent_volume.as_ref().unwrap();
    assert_eq!(pv.metadata.name, "pv0001");
    assert_eq!(pv.spec.volume_mode, Some(PersistentVolumeMode::Block));
    let fc = pv.spec.fc.as_ref().unwrap();
    assert_eq!(fc.target_wwns, Some(s(&["50060e801049cfd1"])));
    assert_eq!(fc.lun, Some(0));
    assert!(!spec.read_only);
}

/// The block mapper's paths (`fc.go:156-199`, `:469-480`).
#[test]
fn block_mapper_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let v = vol("vol1", json!({"targetWWNs": ["w1"], "lun": 2}));
    let p = pod();
    let mapper = plug
        .new_block_volume_mapper(&spec(&v, None, false), &p)
        .unwrap();
    assert_eq!(
        mapper.get_global_map_path(&spec(&v, None, false)).unwrap(),
        tmp.path()
            .join("plugins/kubernetes.io/fc/volumeDevices/w1-lun-2")
            .to_string_lossy()
    );
    assert_eq!(
        mapper.get_pod_device_map_path(),
        (
            tmp.path()
                .join("pods/poduid/volumeDevices/kubernetes.io~fc")
                .to_string_lossy()
                .into_owned(),
            "vol1".to_string()
        )
    );
    assert!(mapper.supports_metrics());

    let none = bare("x");
    assert_eq!(
        plug.new_block_volume_mapper(&spec(&none, None, false), &p)
            .err()
            .unwrap()
            .to_string(),
        "Spec does not reference a FibreChannel volume type"
    );
    let no_info = vol("vol1", json!({}));
    assert_eq!(
        plug.new_block_volume_mapper(&spec(&no_info, None, false), &p)
            .err()
            .unwrap()
            .to_string(),
        "fc: no fc disk information found. failed to make a new mapper"
    );

    let unmapper = plug.new_block_volume_unmapper("vol1", "poduid").unwrap();
    assert!(!unmapper.supports_metrics());
    assert!(unmapper.as_custom_block_volume_unmapper().is_some());
}

fn unmounter_with(
    io: Arc<dyn IoHandler>,
    exec: Arc<dyn Exec>,
    device_util: Arc<dyn DeviceUtil>,
) -> FcDiskUnmounter {
    let tmp = tempfile::tempdir().unwrap();
    FcDiskUnmounter {
        disk: FcDisk {
            io,
            ..FcDisk::blank("v", "u", Arc::new(FcUtil), FcPlugin::new(host(tmp.path())))
        },
        mounter: Arc::new(FakeMounter::new()),
        device_util,
        exec,
    }
}

/// `DetachDisk` on a single device (`fc_util.go:268-300`, `:302-313`): flush
/// I/O, then delete it from the SCSI subsystem.
#[test]
fn detach_disk_single_device_flushes_and_deletes() {
    let io = Arc::new(FakeIoHandler::default());
    let exec = Arc::new(FakeExec::new(vec![script(
        "blockdev",
        &["--flushbufs", "/dev/sde"],
    )]));
    let c = unmounter_with(io.clone(), exec.clone(), fake_device_util());
    FcUtil
        .detach_disk(&c, "/dev/disk/by-path/fc-0x500507681021a537-lun-0")
        .unwrap();
    assert_eq!(exec.command_calls(), 1);
    assert_eq!(
        *io.writes.lock().unwrap(),
        vec![("/sys/block/sde/device/delete".to_string(), b"1".to_vec())]
    );
}

/// `DetachDisk` on a multipath map: slaves are found, the map is flushed with
/// `multipath -f`, then every slave is flushed and deleted.
#[test]
fn detach_disk_multipath_flushes_map_then_slaves() {
    struct Dm(FakeIoHandler);
    impl IoHandler for Dm {
        fn read_dir(&self, d: &str) -> io::Result<Vec<String>> {
            IoHandler::read_dir(&self.0, d)
        }
        fn eval_symlinks(&self, p: &str) -> io::Result<String> {
            if p == "/dev/mapper/mpatha" {
                return Ok("/dev/dm-1".into());
            }
            IoHandler::eval_symlinks(&self.0, p)
        }
        fn write_file(&self, f: &str, d: &[u8], p: u32) -> io::Result<()> {
            self.0.write_file(f, d, p)
        }
    }
    struct Slaves;
    impl DeviceUtil for Slaves {
        fn find_multipath_device_for_device(&self, _: &str) -> String {
            String::new()
        }
        fn find_slave_devices_on_multipath(&self, dm: &str) -> Vec<String> {
            assert_eq!(dm, "/dev/dm-1");
            s(&["/dev/sde", "/dev/sdf"])
        }
        fn get_iscsi_portal_host_map_for_target(
            &self,
            _: &str,
        ) -> io::Result<HashMap<String, i32>> {
            unreachable!()
        }
        fn find_devices_for_iscsi_lun(&self, _: &str, _: i32) -> io::Result<Vec<String>> {
            unreachable!()
        }
    }
    let io = Arc::new(Dm(FakeIoHandler::default()));
    let exec = Arc::new(FakeExec::new(vec![
        script("multipath", &["-f", "/dev/dm-1"]),
        script("blockdev", &["--flushbufs", "/dev/sde"]),
        script("blockdev", &["--flushbufs", "/dev/sdf"]),
    ]));
    let c = unmounter_with(io.clone(), exec.clone(), Arc::new(Slaves));
    FcUtil.detach_disk(&c, "/dev/mapper/mpatha").unwrap();
    assert_eq!(exec.command_calls(), 3);
    assert_eq!(
        *io.0.writes.lock().unwrap(),
        vec![
            ("/sys/block/sde/device/delete".to_string(), b"1".to_vec()),
            ("/sys/block/sdf/device/delete".to_string(), b"1".to_vec()),
        ]
    );
}

/// A failing `multipath -f` aborts the detach before any slave is touched, with
/// the exact `deleteMultipathDevice` message (`fc_util.go:402-409`).
#[test]
fn detach_disk_multipath_flush_failure_aborts() {
    struct Dm(FakeIoHandler);
    impl IoHandler for Dm {
        fn read_dir(&self, d: &str) -> io::Result<Vec<String>> {
            IoHandler::read_dir(&self.0, d)
        }
        fn eval_symlinks(&self, _: &str) -> io::Result<String> {
            Ok("/dev/dm-1".into())
        }
        fn write_file(&self, f: &str, d: &[u8], p: u32) -> io::Result<()> {
            self.0.write_file(f, d, p)
        }
    }
    let io = Arc::new(Dm(FakeIoHandler::default()));
    let mut failing = script("multipath", &["-f", "/dev/dm-1"]);
    failing.return_code = 1;
    failing.output = "map in use".into();
    let exec = Arc::new(FakeExec::new(vec![failing]));
    let c = unmounter_with(io.clone(), exec, fake_device_util());
    let err = FcUtil.detach_disk(&c, "/dev/mapper/mpatha").unwrap_err();
    assert_eq!(
        err.to_string(),
        "failed to flush multipath device /dev/dm-1: exit status 1\nmap in use"
    );
    assert!(io.0.writes.lock().unwrap().is_empty());
}

/// `detachFCDisk` refuses a path outside `/dev/` (`fc_util.go:305-307`); the
/// last such error is returned from `DetachDisk` (`:287-298`).
#[test]
fn detach_disk_rejects_invalid_device_name() {
    struct Odd(FakeIoHandler);
    impl IoHandler for Odd {
        fn read_dir(&self, d: &str) -> io::Result<Vec<String>> {
            IoHandler::read_dir(&self.0, d)
        }
        fn eval_symlinks(&self, _: &str) -> io::Result<String> {
            Ok("/tmp/notadev".into())
        }
        fn write_file(&self, f: &str, d: &[u8], p: u32) -> io::Result<()> {
            self.0.write_file(f, d, p)
        }
    }
    let c = unmounter_with(
        Arc::new(Odd(FakeIoHandler::default())),
        Arc::new(no_commands()),
        fake_device_util(),
    );
    assert_eq!(
        FcUtil.detach_disk(&c, "/x").unwrap_err().to_string(),
        "fc: detachFCDisk failed. device: /tmp/notadev err: fc detach disk: invalid device name: /tmp/notadev"
    );
}

/// `DetachBlockFCDisk` with a missing device or map path is a no-op
/// (`fc_util.go:319-335`): `checkPathExists` returns `(false, nil)` and the
/// function returns that nil error.
#[test]
fn detach_block_missing_paths_is_a_no_op() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let c = plug.new_unmapper_internal("v", "u", Arc::new(FcUtil), Arc::new(no_commands()));
    // devicePath given but absent
    FcUtil
        .detach_block_fc_disk(&c, "/nonexistent/map", "/nonexistent/dev")
        .unwrap();
    // devicePath empty, mapPath absent
    FcUtil
        .detach_block_fc_disk(&c, "/nonexistent/map", "")
        .unwrap();
}

/// `TearDownDevice` (`fc.go:452-463`): detach, then delete `mapPath`; a manager
/// failure is wrapped as `fc: failed to detach disk: <path>\nError: <err>`.
#[tokio::test]
async fn tear_down_device_detaches_then_removes_the_map_path() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let map_path = tmp.path().join("volumeDevices/w1-lun-0");
    std::fs::create_dir_all(&map_path).unwrap();
    let c = plug.new_unmapper_internal(
        "v",
        "u",
        Arc::new(FakeDiskManager::new()),
        Arc::new(no_commands()),
    );
    c.tear_down_device(&map_path.to_string_lossy(), "")
        .await
        .unwrap();
    assert!(!map_path.exists());

    // The fake manager's `DetachBlockFCDisk` removes `mapPath`, which no longer exists.
    let err = c
        .tear_down_device(&map_path.to_string_lossy(), "")
        .await
        .unwrap_err();
    assert!(
        err.to_string().starts_with(&format!(
            "fc: failed to detach disk: {}\nError: ",
            map_path.display()
        )),
        "{err}"
    );
}

/// `GetDeviceNameFromMount` (`mount.go:260-289`).
#[test]
fn get_device_name_from_mount_finds_device_and_ref_count() {
    use crate::volume_plugins::util::mount_interface::testing::MountPoint;
    let fm = FakeMounter::new();
    for (device, path) in [
        ("/dev/sdb", "/g/fc/w-lun-0"),
        ("/dev/sdb", "/pods/p/volumes/kubernetes.io~fc/v"),
        ("/dev/sdc", "/g/fc/other-lun-0"),
    ] {
        fm.mount_points.lock().unwrap().push(MountPoint {
            device: device.into(),
            path: path.into(),
            fstype: String::new(),
            opts: vec![],
        });
    }
    assert_eq!(
        crate::volume_plugins::util::mount_list::get_device_name_from_mount(&fm, "/g/fc/w-lun-0")
            .unwrap(),
        ("/dev/sdb".to_string(), 2)
    );
    // Not a mount point: the empty device, whose "references" are the mounts
    // with an empty device (none).
    assert_eq!(
        crate::volume_plugins::util::mount_list::get_device_name_from_mount(&fm, "/g/fc/none")
            .unwrap(),
        (String::new(), 0)
    );
}

/// `UnmountDevice` (`attacher.go:166-206`) on a path that is not a mount point:
/// `GetDeviceNameFromMount` returns an empty name, so nothing is detached.
#[tokio::test]
async fn unmount_device_not_a_mount_point_skips_detach() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let detacher = FcDetacher {
        mounter: Arc::new(FakeMounter::new()),
        manager: Arc::new(FakeDiskManager::new()),
        host: plug.host.clone(),
    };
    let dir = tmp.path().join("global");
    std::fs::create_dir_all(&dir).unwrap();
    detacher
        .unmount_device_with(
            &dir.to_string_lossy(),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(5),
        )
        .await
        .unwrap();
    // `CleanupMountPoint` removed the empty directory.
    assert!(!dir.exists());
}

/// `UnmountDevice` on a mounted device unmounts and then `DetachDisk`s with the
/// device name the mount table gave.
#[tokio::test]
async fn unmount_device_detaches_the_mounted_device() {
    use crate::volume_plugins::util::mount_interface::testing::MountPoint;
    struct Recording {
        seen: Mutex<Vec<String>>,
    }
    impl DiskManager for Recording {
        fn make_global_pd_name(&self, _: &FcDisk) -> String {
            String::new()
        }
        fn make_global_vdpd_name(&self, _: &FcDisk) -> String {
            String::new()
        }
        fn attach_disk(&self, _: &FcDiskMounter) -> Result<String> {
            unreachable!()
        }
        fn detach_disk(&self, _: &FcDiskUnmounter, device_path: &str) -> Result<()> {
            self.seen.lock().unwrap().push(device_path.to_string());
            Ok(())
        }
        fn detach_block_fc_disk(&self, _: &FcDiskUnmapper, _: &str, _: &str) -> Result<()> {
            unreachable!()
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let dir = tmp.path().join("global");
    std::fs::create_dir_all(&dir).unwrap();
    let fm = Arc::new(FakeMounter::new());
    fm.mount_points.lock().unwrap().push(MountPoint {
        device: "/dev/sdb".into(),
        path: std::fs::canonicalize(&dir)
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        fstype: "ext4".into(),
        opts: vec![],
    });
    let manager = Arc::new(Recording {
        seen: Mutex::new(vec![]),
    });
    let detacher = FcDetacher {
        mounter: fm.clone(),
        manager: manager.clone(),
        host: plug.host.clone(),
    };
    detacher
        .unmount_device_with(
            &dir.to_string_lossy(),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(5),
        )
        .await
        .unwrap();
    assert_eq!(*manager.seen.lock().unwrap(), vec!["/dev/sdb".to_string()]);
    assert!(fm.mount_points.lock().unwrap().is_empty());
}

/// `UnmountDevice` retries `DetachDisk` (`attacher.go:186-199`) and, when it
/// never succeeds, returns `fc: failed to detach disk: <dev>: <last error>`.
#[tokio::test]
async fn unmount_device_retries_then_reports_the_last_detach_error() {
    use crate::volume_plugins::util::mount_interface::testing::MountPoint;
    struct Failing {
        calls: Mutex<usize>,
    }
    impl DiskManager for Failing {
        fn make_global_pd_name(&self, _: &FcDisk) -> String {
            String::new()
        }
        fn make_global_vdpd_name(&self, _: &FcDisk) -> String {
            String::new()
        }
        fn attach_disk(&self, _: &FcDiskMounter) -> Result<String> {
            unreachable!()
        }
        fn detach_disk(&self, _: &FcDiskUnmounter, _: &str) -> Result<()> {
            let mut n = self.calls.lock().unwrap();
            *n += 1;
            Err(anyhow!("device is busy ({n})"))
        }
        fn detach_block_fc_disk(&self, _: &FcDiskUnmapper, _: &str, _: &str) -> Result<()> {
            unreachable!()
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let dir = tmp.path().join("global");
    std::fs::create_dir_all(&dir).unwrap();
    let fm = Arc::new(FakeMounter::new());
    fm.mount_points.lock().unwrap().push(MountPoint {
        device: "/dev/sdb".into(),
        path: std::fs::canonicalize(&dir)
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        fstype: "ext4".into(),
        opts: vec![],
    });
    let manager = Arc::new(Failing {
        calls: Mutex::new(0),
    });
    let detacher = FcDetacher {
        mounter: fm,
        manager: manager.clone(),
        host: plug.host.clone(),
    };
    // No tick can fire (1h interval) and the timer fires 1ms after the
    // immediate attempt fails, so `wait.PollImmediate` makes exactly two
    // attempts: the immediate one and the one its closed channel runs
    // (`waitForWithContext`, wait.go: `if !open { return ErrWaitTimeout }`).
    // Neither count depends on how fast this machine is.
    let err = detacher
        .unmount_device_with(
            &dir.to_string_lossy(),
            std::time::Duration::from_secs(3600),
            std::time::Duration::from_millis(1),
        )
        .await
        .unwrap_err();
    let calls = *manager.calls.lock().unwrap();
    assert_eq!(calls, 2, "DetachDisk must be retried once on timeout");
    assert_eq!(
        err.to_string(),
        "fc: failed to detach disk: /dev/sdb: device is busy (2)"
    );
}

/// `UnmountDevice`'s `wait.PollImmediate` retries on every tick until
/// `DetachDisk` succeeds; the timeout (1h here) is never reached, so the count
/// is exact however slowly the ticks arrive.
#[tokio::test]
async fn unmount_device_retries_on_each_tick_until_detach_succeeds() {
    use crate::volume_plugins::util::mount_interface::testing::MountPoint;
    struct BusyTwice {
        calls: Mutex<usize>,
    }
    impl DiskManager for BusyTwice {
        fn make_global_pd_name(&self, _: &FcDisk) -> String {
            String::new()
        }
        fn make_global_vdpd_name(&self, _: &FcDisk) -> String {
            String::new()
        }
        fn attach_disk(&self, _: &FcDiskMounter) -> Result<String> {
            unreachable!()
        }
        fn detach_disk(&self, _: &FcDiskUnmounter, _: &str) -> Result<()> {
            let mut n = self.calls.lock().unwrap();
            *n += 1;
            if *n < 3 {
                return Err(anyhow!("device is busy ({n})"));
            }
            Ok(())
        }
        fn detach_block_fc_disk(&self, _: &FcDiskUnmapper, _: &str, _: &str) -> Result<()> {
            unreachable!()
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let dir = tmp.path().join("global");
    std::fs::create_dir_all(&dir).unwrap();
    let fm = Arc::new(FakeMounter::new());
    fm.mount_points.lock().unwrap().push(MountPoint {
        device: "/dev/sdb".into(),
        path: std::fs::canonicalize(&dir)
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        fstype: "ext4".into(),
        opts: vec![],
    });
    let manager = Arc::new(BusyTwice {
        calls: Mutex::new(0),
    });
    let detacher = FcDetacher {
        mounter: fm,
        manager: manager.clone(),
        host: plug.host.clone(),
    };
    detacher
        .unmount_device_with(
            &dir.to_string_lossy(),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_secs(3600),
        )
        .await
        .unwrap();
    assert_eq!(*manager.calls.lock().unwrap(), 3);
}

/// `MountDevice` (`attacher.go:102-138`): the global path is created
/// (`attacher.go:104-114`), `FormatAndMount` runs, and when it fails the path
/// is removed again (`attacher.go:133`). A non-device source fails without
/// root; the successful mount arm needs `mount(8)` and is not run here.
#[tokio::test]
async fn mount_device_failure_removes_the_global_path() {
    let tmp = tempfile::tempdir().unwrap();
    let plug = FcPlugin::new(host(tmp.path()));
    let attacher = plug.new_attacher_internal();
    let global = tmp.path().join("plugins/kubernetes.io/fc/w-lun-0");
    let v = vol(
        "v",
        json!({"targetWWNs": ["w"], "lun": 0, "fsType": "ext4"}),
    );
    let res = attacher
        .mount_device(
            &spec(&v, None, false),
            "/definitely/not/a/device",
            &global.to_string_lossy(),
            &DeviceMounterArgs::default(),
        )
        .await;
    assert!(res.is_err());
    assert!(!global.exists(), "the failed mount's directory is removed");
}
