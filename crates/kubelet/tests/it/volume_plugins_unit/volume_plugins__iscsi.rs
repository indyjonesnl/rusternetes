//! Unit tests moved from `src/volume_plugins/iscsi.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, bail, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::volume::{
        ISCSIVolumeSource, PersistentVolumeAccessMode, PersistentVolumeMode,
    };
    pub use rusternetes_common::resources::{Pod, Secret};
    pub use rusternetes_kubelet::volume_plugins::iscsi::attacher::IscsiAttacher;
    pub use rusternetes_kubelet::volume_plugins::iscsi::disk_manager::DiskManager;
    pub use rusternetes_kubelet::volume_plugins::iscsi::util::IscsiUtil;
    pub use rusternetes_kubelet::volume_plugins::metrics_statfs::MetricsStatFs;
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        Attributes, BlockVolumeMapper, BlockVolumePlugin, BlockVolumeUnmapper,
        CustomBlockVolumeUnmapper, Metrics, MetricsProvider, OwnedSpec,
    };
    pub use rusternetes_kubelet::volume_plugins::util::device_util::{
        new_os_device_handler, DeviceUtil,
    };
    pub use rusternetes_kubelet::volume_plugins::util::exec::{Exec, RealExec};
    pub use rusternetes_kubelet::volume_plugins::util::key_mutex::KeyMutex;
    pub use rusternetes_kubelet::volume_plugins::util::metrics::{
        new_metrics_block, MetricsBlock, MetricsNil,
    };
    pub use rusternetes_kubelet::volume_plugins::util::mount_interface::{
        MountInterface, SafeFormatAndMount,
    };
    pub use rusternetes_kubelet::volume_plugins::util::mount_ops::cleanup_mount_point;
    pub use rusternetes_kubelet::volume_plugins::util::{
        mount_option_from_spec, types::VolumeOperationError,
    };
    pub use rusternetes_kubelet::volume_plugins::{
        DeviceMounter, MounterArgs, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
    };
    pub use rusternetes_storage::{build_key, Storage};
    pub use std::collections::HashMap;
    pub use std::sync::atomic::{AtomicBool, Ordering};
    pub use std::sync::Arc;
    pub use tracing::{error, warn};
}

mod tests {
    //! Ports of `pkg/volume/iscsi/iscsi_test.go` (`TestCanSupport`,
    //! `TestGetAccessModes`, `doTestPlugin`, `TestPluginVolume`,
    //! `TestPluginPersistentVolume`, `TestPersistentClaimReadOnlyFlag`,
    //! `TestPortalMounter`, `TestGetSecretNameAndNamespaceForPV`,
    //! `TestGetISCSIInitiatorInfo`, `TestGetISCSICHAP`, `TestGetVolumeSpec*`).
    use super::__parent::*;
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use rusternetes_kubelet::volume_plugins::iscsi::*;
    use rusternetes_kubelet::volume_plugins::util::exec::testing::no_commands;
    use rusternetes_kubelet::volume_plugins::util::mount_interface::testing::FakeMounter;
    use rusternetes_kubelet::volume_plugins::Mounter as _;
    use rusternetes_kubelet::volume_plugins::{KubeletVolumeHost, VolumePluginMgr};
    use serde_json::json;

    fn host(tmp: &std::path::Path) -> Arc<dyn VolumeHost> {
        Arc::new(KubeletVolumeHost::new_for_tests(
            tmp.to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        ))
    }

    fn vol(name: &str, source: serde_json::Value) -> Volume {
        serde_json::from_value(json!({"name": name, "iscsi": source})).unwrap()
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

    fn source() -> serde_json::Value {
        json!({
            "targetPortal": "127.0.0.1:3260",
            "iqn": "iqn.2014-12.server:storage.target01",
            "fsType": "ext4",
            "lun": 0,
            // As stored: the api-server applies `+default="default"` (#3491).
            "iscsiInterface": "default"
        })
    }

    /// `TestCanSupport`.
    #[test]
    fn can_support() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumePluginMgr::new(vec![Box::new(IscsiPlugin::new(host(tmp.path())))]);
        let plug = mgr.find_plugin_by_name("kubernetes.io/iscsi").unwrap();
        assert_eq!(plug.name(), "kubernetes.io/iscsi");

        let none = bare("x");
        assert!(!plug.can_support(&spec(&none, None, false)));
        let v = vol("x", json!({"targetPortal": "", "iqn": "", "lun": 0}));
        assert!(plug.can_support(&spec(&v, None, false)));
        let empty_pv = pv(json!({"metadata": {"name": "pv"}, "spec": {}}));
        assert!(!plug.can_support(&spec(&none, Some(&empty_pv), false)));
        let iscsi_pv = pv(json!({"metadata": {"name": "pv"},
            "spec": {"iscsi": {"targetPortal": "", "iqn": "", "lun": 0}}}));
        assert!(plug.can_support(&spec(&none, Some(&iscsi_pv), false)));
    }

    /// `TestGetAccessModes`.
    #[test]
    fn get_access_modes() {
        let tmp = tempfile::tempdir().unwrap();
        let plug = IscsiPlugin::new(host(tmp.path()));
        let modes = plug.get_access_modes();
        assert!(modes.contains(&PersistentVolumeAccessMode::ReadWriteOnce));
        assert!(modes.contains(&PersistentVolumeAccessMode::ReadOnlyMany));
    }

    /// `fakeDiskManager` (`iscsi_test.go:93-140`).
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
        fn make_global_pd_name(&self, _: &IscsiDisk) -> String {
            self.tmp_dir.path().to_string_lossy().into_owned()
        }

        fn make_global_vdpd_name(&self, _: &IscsiDisk) -> String {
            self.tmp_dir.path().to_string_lossy().into_owned()
        }

        fn attach_disk(&self, b: &IscsiDiskMounter) -> Result<String> {
            let global_path = b.disk.manager.make_global_pd_name(&b.disk);
            std::fs::create_dir_all(&global_path)?;
            // Simulate the global mount so that the fakeMounter returns the
            // expected number of mounts for the attached disk.
            let _ = b.mounter.mounter.mount_sensitive_without_systemd(
                &global_path,
                &global_path,
                &b.fs_type,
                &[],
                &[],
            );
            Ok("/dev/sdb".to_string())
        }

        fn detach_disk(&self, c: &mut IscsiDiskUnmounter, _: &str) -> Result<()> {
            let global_path = c.disk.manager.make_global_pd_name(&c.disk);
            std::fs::remove_dir_all(global_path)?;
            Ok(())
        }

        fn detach_block_iscsi_disk(&self, c: &mut IscsiDiskUnmapper, _: &str) -> Result<()> {
            let global_path = c.disk.manager.make_global_vdpd_name(&c.disk);
            std::fs::remove_dir_all(global_path)?;
            Ok(())
        }
    }

    /// `doTestPlugin` (`iscsi_test.go:142-215`).
    async fn do_test_plugin(spec: &Spec<'_>) {
        let tmp = tempfile::tempdir().unwrap();
        let plug = IscsiPlugin::new(host(tmp.path()));
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
                HashMap::new(),
            )
            .unwrap();

        let path = mounter.get_path();
        let expected_path = tmp
            .path()
            .join("pods/poduid/volumes/kubernetes.io~iscsi/vol1")
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

    /// `TestPluginVolume`.
    #[tokio::test]
    async fn plugin_volume() {
        let v = vol("vol1", source());
        do_test_plugin(&spec(&v, None, false)).await;
    }

    /// `TestPluginPersistentVolume`.
    #[tokio::test]
    async fn plugin_persistent_volume() {
        let p = pv(json!({"metadata": {"name": "vol1"}, "spec": {"iscsi": source()}}));
        let placeholder = bare("vol1");
        do_test_plugin(&spec(&placeholder, Some(&p), false)).await;
    }

    /// `TestPersistentClaimReadOnlyFlag`.
    #[tokio::test]
    async fn persistent_claim_read_only_flag() {
        let tmp = tempfile::tempdir().unwrap();
        let plug = IscsiPlugin::new(host(tmp.path()));
        let p = pv(json!({"metadata": {"name": "pvA"},
            "spec": {"iscsi": source(), "claimRef": {"name": "claimA"}}}));
        // readOnly bool is supplied by persistent-claim volume source when its mounter creates other volumes
        let placeholder = bare("pvA");
        let m = plug
            .new_mounter(&spec(&placeholder, Some(&p), true), &pod())
            .await
            .unwrap();
        assert!(m.get_attributes().read_only);
    }

    /// `TestPortalMounter`.
    #[test]
    fn portal_mounter_test() {
        assert_eq!(portal_mounter("127.0.0.1"), "127.0.0.1:3260");
        assert_eq!(portal_mounter("127.0.0.1:3260"), "127.0.0.1:3260");
    }

    /// `TestGetSecretNameAndNamespaceForPV`.
    #[test]
    fn get_secret_name_and_namespace_for_pv() {
        let none = bare("x");
        let with_ns = pv(json!({"metadata": {"name": "p"}, "spec": {"iscsi": {
            "targetPortal": "127.0.0.1:3260", "iqn": "iqn.2014-12.server:storage.target01",
            "fsType": "ext4", "lun": 0, "secretRef": {"name": "name", "namespace": "ns"}}}}));
        assert_eq!(
            get_iscsi_secret_name_and_namespace(&spec(&none, Some(&with_ns), false), "default")
                .unwrap(),
            ("name".to_string(), "ns".to_string())
        );
        let no_ns = pv(json!({"metadata": {"name": "p"}, "spec": {"iscsi": {
            "targetPortal": "127.0.0.1:3260", "iqn": "iqn.2014-12.server:storage.target01",
            "fsType": "ext4", "lun": 0, "secretRef": {"name": "name"}}}}));
        assert_eq!(
            get_iscsi_secret_name_and_namespace(&spec(&none, Some(&no_ns), false), "default")
                .unwrap(),
            ("name".to_string(), "default".to_string())
        );
        let pod_vol = vol("v", source());
        assert_eq!(
            get_iscsi_secret_name_and_namespace(&spec(&pod_vol, None, false), "default").unwrap(),
            (String::new(), String::new())
        );
    }

    /// `TestGetISCSIInitiatorInfo`.
    #[test]
    fn get_iscsi_initiator_info_test() {
        let none = bare("x");
        let p = pv(json!({"metadata": {"name": "p"}, "spec": {"iscsi": {
            "targetPortal": "127.0.0.1:3260", "iqn": "iqn.2014-12.server:storage.target01",
            "fsType": "ext4", "lun": 0, "iscsiInterface": "tcp"}}}));
        assert_eq!(
            get_iscsi_initiator_info(&spec(&none, Some(&p), false))
                .unwrap()
                .0,
            "tcp"
        );
        let v = vol(
            "v",
            json!({"targetPortal": "127.0.0.1:3260", "iqn": "iqn.2014-12.server:storage.target01",
                "fsType": "ext4", "lun": 0, "iscsiInterface": "tcp"}),
        );
        assert_eq!(
            get_iscsi_initiator_info(&spec(&v, None, false)).unwrap().0,
            "tcp"
        );
    }

    /// `TestGetISCSICHAP`.
    #[test]
    fn get_iscsi_chap() {
        let none = bare("x");
        let p = pv(json!({"metadata": {"name": "p"}, "spec": {"iscsi": {
            "targetPortal": "", "iqn": "", "lun": 0,
            "chapAuthDiscovery": true, "chapAuthSession": true}}}));
        let s = spec(&none, Some(&p), false);
        assert!(get_iscsi_discovery_chap_info(&s).unwrap());
        assert!(get_iscsi_session_chap_info(&s).unwrap());

        let v = vol(
            "v",
            json!({"targetPortal": "", "iqn": "", "lun": 0,
                "chapAuthDiscovery": true, "chapAuthSession": true}),
        );
        let s = spec(&v, None, false);
        assert!(get_iscsi_discovery_chap_info(&s).unwrap());
        assert!(get_iscsi_session_chap_info(&s).unwrap());

        // "no volume"
        let s = spec(&none, None, false);
        assert!(get_iscsi_discovery_chap_info(&s).is_err());
        assert_eq!(
            get_iscsi_session_chap_info(&s).unwrap_err().to_string(),
            "Spec does not reference an ISCSI volume type"
        );
    }

    const MAP_PATH: &str = "plugins/kubernetes.io/iscsi/volumeDevices/iface-default/127.0.0.1:3260-iqn.2014-12.server:storage.target01-lun-0";

    /// `TestGetVolumeSpec`.
    #[test]
    fn get_volume_spec() {
        let spec = get_volume_spec_from_global_map_path("test", MAP_PATH).unwrap();
        let src = spec
            .persistent_volume
            .as_ref()
            .unwrap()
            .spec
            .iscsi
            .as_ref()
            .unwrap();
        assert_eq!(src.target_portal, "127.0.0.1:3260");
        assert_eq!(src.iqn, "iqn.2014-12.server:storage.target01");
        assert_eq!(src.lun, 0);
        assert_eq!(src.iscsi_interface.as_deref(), Some("default"));
    }

    /// `TestGetVolumeSpec_no_lun`.
    #[test]
    fn get_volume_spec_no_lun() {
        let path = "plugins/kubernetes.io/iscsi/volumeDevices/iface-default/127.0.0.1:3260-iqn.2014-12.server:storage.target01";
        let err = get_volume_spec_from_global_map_path("test", path)
            .err()
            .unwrap();
        assert!(err.to_string().contains("malformatted mnt path"), "{err}");
    }

    /// `TestGetVolumeSpec_no_iface`.
    #[test]
    fn get_volume_spec_no_iface() {
        let path = "plugins/kubernetes.io/iscsi/volumeDevices/default/127.0.0.1:3260-iqn.2014-12.server:storage.target01-lun-0";
        let err = get_volume_spec_from_global_map_path("test", path)
            .err()
            .unwrap();
        assert!(
            err.to_string().contains("failed to retrieve iface"),
            "{err}"
        );
    }

    /// `GetDeviceMountPath` (`attacher.go:92-104`) is the global `MakeGlobalPDName`
    /// path, `iface-<ifaceName>/<portal>-<iqn>-lun-<lun>` under the plugin dir; with
    /// an `initiatorName` the iface becomes `<portal>:<volume name>`.
    #[test]
    fn device_mount_path_is_the_global_pd_name() {
        use rusternetes_kubelet::volume_plugins::plugin::DeviceMounter;
        let tmp = tempfile::tempdir().unwrap();
        let plug = IscsiPlugin::new(host(tmp.path()));
        let attacher = plug.new_attacher_internal();
        let base = tmp.path().join("plugins/kubernetes.io/iscsi");

        let v = vol("vol1", source());
        assert_eq!(
            attacher
                .get_device_mount_path(&spec(&v, None, false))
                .unwrap(),
            base.join("iface-default/127.0.0.1:3260-iqn.2014-12.server:storage.target01-lun-0")
                .to_string_lossy()
        );

        let mut with_initiator = source();
        with_initiator["initiatorName"] = json!("iqn.2014-12.client:init");
        let v = vol("vol1", with_initiator);
        assert_eq!(
            attacher
                .get_device_mount_path(&spec(&v, None, false))
                .unwrap(),
            base.join(
                "iface-127.0.0.1:3260:vol1/127.0.0.1:3260-iqn.2014-12.server:storage.target01-lun-0"
            )
            .to_string_lossy()
        );

        // `FindAttachablePluginBySpec` type-asserts to `AttachableVolumePlugin`.
        let mgr = VolumePluginMgr::new(vec![Box::new(plug.clone())]);
        let found = mgr
            .find_attachable_plugin_by_spec(&spec(&v, None, false))
            .and_then(|p| p.as_attachable_plugin())
            .unwrap();
        assert!(found.new_attacher().is_ok());
    }

    /// `iscsiDisk` embeds `volume.NewMetricsStatFS(GetPath())` on both the
    /// mounter (`iscsi.go:128`) and the unmounter (`iscsi.go:194`): metrics are
    /// the statfs of the pod volume dir (#3494).
    #[tokio::test]
    async fn mounter_and_unmounter_provide_statfs_metrics() {
        use rusternetes_kubelet::volume_plugins::plugin::MetricsProvider;
        let tmp = tempfile::tempdir().unwrap();
        let plug = IscsiPlugin::new(host(tmp.path()));
        let v = vol("vol1", source());
        let m = plug
            .new_mounter_internal(
                &spec(&v, None, false),
                "poduid",
                Arc::new(FakeDiskManager::new()),
                Arc::new(FakeMounter::new()),
                Arc::new(no_commands()),
                HashMap::new(),
            )
            .unwrap();
        let u = plug.new_unmounter_internal(
            "vol1",
            "poduid",
            Arc::new(FakeDiskManager::new()),
            Arc::new(FakeMounter::new()),
            Arc::new(no_commands()),
        );

        // Directory absent: FsInfoFailed, like upstream's statfs error.
        let err = m.get_metrics().unwrap_err();
        assert!(err.to_string().starts_with("failed to get FsInfo"), "{err}");

        std::fs::create_dir_all(m.get_path()).unwrap();
        for got in [m.get_metrics().unwrap(), u.get_metrics().unwrap()] {
            assert!(got.capacity.unwrap().value() > 0);
            assert!(got.available.unwrap().value() > 0);
        }
    }

    /// `MountDevice`'s option list (`attacher.go:124-130`).
    #[test]
    fn device_mount_options_add_selinux_context() {
        use rusternetes_kubelet::volume_plugins::iscsi::attacher::device_mount_options;
        assert!(device_mount_options(false, "").is_empty());
        assert_eq!(device_mount_options(true, ""), ["ro"]);
        assert_eq!(
            device_mount_options(true, "system_u:object_r:container_file_t:s0:c1,c2"),
            [
                "ro",
                "context=\"system_u:object_r:container_file_t:s0:c1,c2\""
            ]
        );
        let _off = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
            false,
        );
        assert_eq!(device_mount_options(true, "system_u:object_r:x:s0"), ["ro"]);
    }

    struct ContextHostUtil(String);

    impl rusternetes_kubelet::volume_plugins::util::hostutil::HostUtils for ContextHostUtil {
        fn device_opened(&self, _: &str) -> std::io::Result<bool> {
            Ok(false)
        }
        fn path_is_device(&self, _: &str) -> std::io::Result<bool> {
            Ok(false)
        }
        fn get_file_type(
            &self,
            _: &str,
        ) -> std::io::Result<rusternetes_kubelet::volume_plugins::util::hostutil::FileType>
        {
            Err(std::io::Error::other("not faked"))
        }
        fn get_selinux_mount_context(&self, _: &str) -> std::io::Result<String> {
            Ok(self.0.clone())
        }
    }

    /// `ConstructVolumeSpec` (`iscsi.go:220-296`) returns the pod mount's
    /// `-o context=` as `SELinuxMountContext` under `SELinuxMountReadWriteOncePod`.
    #[test]
    #[serial_test::serial]
    fn construct_volume_spec_reports_selinux_mount_context() {
        let tmp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(tmp.path()).unwrap();
        let pod_path = root.join("pods/uid/volumes/kubernetes.io~iscsi/mypv");
        let global = root.join(
            "plugins/kubernetes.io/iscsi/iface-default/127.0.0.1:3260-iqn.2014-12.server:storage.target01-lun-0",
        );
        std::fs::create_dir_all(&pod_path).unwrap();
        std::fs::create_dir_all(&global).unwrap();
        let info = format!(
            "10 1 8:1 / / rw - ext4 /dev/sda1 rw\n20 10 8:1 /r {} rw - ext4 /dev/sda1 rw\n21 10 8:1 /r {} rw - ext4 /dev/sda1 rw\n",
            pod_path.display(),
            global.display()
        );
        let mountinfo = root.join("mountinfo");
        std::fs::write(&mountinfo, info).unwrap();
        let host: Arc<dyn VolumeHost> = Arc::new(
            KubeletVolumeHost::new_for_tests(
                root.to_string_lossy().to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            )
            .with_mounter(
                rusternetes_kubelet::volume_plugins::util::mount_ops::Mounter::new(
                    "", false, false,
                )
                .with_mountinfo_path(mountinfo.to_str().unwrap()),
            )
            .with_host_util(Arc::new(ContextHostUtil(
                "system_u:object_r:c:s0:c1".into(),
            ))),
        );
        let plug = IscsiPlugin::new(host);
        let got = plug
            .construct_volume_spec("mypv", pod_path.to_str().unwrap())
            .unwrap();
        assert_eq!(got.selinux_mount_context, "system_u:object_r:c:s0:c1");
        let iscsi = got.volume.unwrap().iscsi.unwrap();
        assert_eq!(iscsi.iqn, "iqn.2014-12.server:storage.target01");

        // Gate off: upstream never asks the host util.
        let _off = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
            false,
        );
        let got = plug
            .construct_volume_spec("mypv", pod_path.to_str().unwrap())
            .unwrap();
        assert_eq!(got.selinux_mount_context, "");
    }
}
