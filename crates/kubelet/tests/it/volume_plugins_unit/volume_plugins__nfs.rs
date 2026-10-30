//! Unit tests moved from `src/volume_plugins/nfs.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::volume::{NFSVolumeSource, PersistentVolumeAccessMode};
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_kubelet::volume_plugins::metrics_statfs::MetricsStatFs;
    pub use rusternetes_kubelet::volume_plugins::plugin::{Metrics, MetricsProvider};
    pub use rusternetes_kubelet::volume_plugins::util::mount_ops::{
        cleanup_mount_with_force, Mounter as MountUtil,
    };
    pub use rusternetes_kubelet::volume_plugins::util::mount_utils::is_not_mount_point;
    pub use rusternetes_kubelet::volume_plugins::util::{
        join_mount_options, mount_option_from_spec,
    };
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
    };
    pub use std::io;
    pub use std::os::unix::fs::DirBuilderExt;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{debug, error};
}

mod tests {
    //! Ports of `pkg/volume/nfs/nfs_test.go` (`TestCanSupport`,
    //! `TestGetAccessModes`, `TestPluginVolume`, `TestIPV6VolumeSource`,
    //! `TestIPV4VolumeSource`, `TestPluginPersistentVolume`,
    //! `TestPersistentClaimReadOnlyFlag`). `doTestPlugin` drives the mounter
    //! through a `FakeMounter`; our `mount.Interface` is the concrete
    //! `Mounter` shelling out to mount(8), so the mount call itself is not
    //! faked here: the source string it receives is asserted from the
    //! mounter's inputs, and the cleanup path is exercised on a real directory.
    use super::__parent::*;
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use rusternetes_kubelet::volume_plugins::nfs::*;
    use rusternetes_kubelet::volume_plugins::{KubeletVolumeHost, VolumePluginMgr};
    use serde_json::json;

    fn host(tmp: &std::path::Path) -> Arc<dyn VolumeHost> {
        Arc::new(KubeletVolumeHost::new_for_tests(
            tmp.to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        ))
    }

    fn plugin(tmp: &std::path::Path) -> NfsPlugin {
        NfsPlugin::new(host(tmp))
    }

    fn vol(source: serde_json::Value) -> Volume {
        serde_json::from_value(json!({"name": "vol1", "nfs": source})).unwrap()
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

    /// `TestCanSupport`.
    #[test]
    fn can_support() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumePluginMgr::new(vec![Box::new(plugin(tmp.path()))]);
        let plug = mgr.find_plugin_by_name("kubernetes.io/nfs").unwrap();
        assert_eq!(plug.name(), "kubernetes.io/nfs");

        let v = vol(json!({"server": "", "path": ""}));
        assert!(plug.can_support(&spec(&v, None, false)));
        let p =
            pv(json!({"metadata": {"name": "pv"}, "spec": {"nfs": {"server": "", "path": ""}}}));
        let empty = bare("x");
        assert!(plug.can_support(&spec(&empty, Some(&p), false)));
        assert!(!plug.can_support(&spec(&empty, None, false)));
    }

    /// `TestGetAccessModes`.
    #[test]
    fn get_access_modes() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(
            plugin(tmp.path()).get_access_modes(),
            vec![
                PersistentVolumeAccessMode::ReadWriteOnce,
                PersistentVolumeAccessMode::ReadOnlyMany,
                PersistentVolumeAccessMode::ReadWriteMany
            ]
        );
    }

    /// `doTestPlugin` (`nfs_test.go:99-190`) as far as it does not touch the
    /// fake mounter: the volume path under `kubernetes.io~nfs`, the
    /// `<server>:<path>` source, and `GetAttributes`.
    /// `nfs` embeds `volume.NewMetricsStatFS(getPath(..))` in both the mounter
    /// and the unmounter (`nfs.go:131`, `:150`): metrics are the statfs of the
    /// pod volume dir.
    #[tokio::test]
    async fn mounter_and_unmounter_provide_statfs_metrics() {
        let tmp = tempfile::tempdir().unwrap();
        let plug = plugin(tmp.path());
        let v = vol(json!({"server": "127.0.0.1", "path": "/somepath"}));
        let m = plug
            .new_nfs_mounter(&spec(&v, None, false), &pod())
            .unwrap();
        let u = plug.new_nfs_unmounter("vol1", "poduid");

        // Directory absent: FsInfoFailed, like upstream's statfs error.
        let err = m.get_metrics().unwrap_err();
        assert!(err.to_string().starts_with("failed to get FsInfo"), "{err}");

        std::fs::create_dir_all(m.get_path()).unwrap();
        for got in [m.get_metrics().unwrap(), u.get_metrics().unwrap()] {
            assert!(got.capacity.unwrap().value() > 0);
            assert!(got.available.unwrap().value() > 0);
        }
    }

    async fn do_test_plugin(spec: &Spec<'_>, want_source: &str) {
        let tmp = tempfile::tempdir().unwrap();
        let plug = plugin(tmp.path());
        let m = plug.new_mounter(spec, &pod()).await.unwrap();
        let want = tmp
            .path()
            .join("pods/poduid/volumes/kubernetes.io~nfs/vol1")
            .to_string_lossy()
            .to_string();
        assert_eq!(m.get_path(), want);
        let (source, _) = get_volume_source(spec).unwrap();
        assert_eq!(
            format!("{}:{}", get_server_from_source(source), source.path),
            want_source
        );
        let a = m.get_attributes();
        assert!(!a.managed && !a.selinux_relabel);
        assert_eq!(
            plug.new_unmounter("vol1", "poduid").unwrap().get_path(),
            want
        );
    }

    #[tokio::test]
    async fn plugin_volume() {
        let v = vol(json!({"server": "localhost", "path": "/somepath", "readOnly": false}));
        do_test_plugin(&spec(&v, None, false), "localhost:/somepath").await;
    }

    #[tokio::test]
    async fn ipv6_volume_source() {
        let v = vol(json!({"server": "0:0:0:0:0:0:0:1", "path": "/somepath"}));
        do_test_plugin(&spec(&v, None, false), "[0:0:0:0:0:0:0:1]:/somepath").await;
    }

    #[tokio::test]
    async fn ipv4_volume_source() {
        let v = vol(json!({"server": "127.0.0.1", "path": "/somepath"}));
        do_test_plugin(&spec(&v, None, false), "127.0.0.1:/somepath").await;
    }

    /// `TestPluginPersistentVolume`.
    #[tokio::test]
    async fn plugin_persistent_volume() {
        let p = pv(json!({"metadata": {"name": "vol1"},
            "spec": {"nfs": {"server": "localhost", "path": "/somepath"}}}));
        let v = bare("vol1");
        do_test_plugin(&spec(&v, Some(&p), false), "localhost:/somepath").await;
    }

    /// `TestPersistentClaimReadOnlyFlag`: the claim's readOnly reaches the
    /// mounter through `Spec.ReadOnly`.
    #[tokio::test]
    async fn persistent_claim_read_only_flag() {
        let tmp = tempfile::tempdir().unwrap();
        let p =
            pv(json!({"metadata": {"name": "pvA"}, "spec": {"nfs": {"server": "", "path": ""}}}));
        let v = bare("pvA");
        let m = plugin(tmp.path())
            .new_mounter(&spec(&v, Some(&p), true), &pod())
            .await
            .unwrap();
        assert!(m.get_attributes().read_only);
    }

    /// `GetVolumeName` (`nfs.go:79-89`) and `getVolumeSource`'s error.
    #[test]
    fn volume_name_and_missing_source() {
        let tmp = tempfile::tempdir().unwrap();
        let plug = plugin(tmp.path());
        let v = vol(json!({"server": "s", "path": "/p"}));
        assert_eq!(
            plug.get_volume_name(&spec(&v, None, false)).unwrap(),
            "s//p"
        );
        let empty = bare("x");
        assert_eq!(
            plug.get_volume_name(&spec(&empty, None, false))
                .unwrap_err()
                .to_string(),
            "Spec does not reference a NFS volume type"
        );
    }

    /// `MountOptionFromSpec` reads `spec.mountOptions`; `SetUpAt` joins `ro`
    /// (`nfs.go:238-243`) through `JoinMountOptions` (sorted, de-duplicated).
    #[test]
    fn mount_options_come_from_the_pv() {
        let p = pv(json!({"metadata": {"name": "pv"},
            "spec": {"nfs": {"server": "s", "path": "/p"}, "mountOptions": ["vers=4.1", "hard"]}}));
        let v = bare("pv");
        assert_eq!(
            mount_option_from_spec(&spec(&v, Some(&p), false), &[]),
            vec!["hard", "vers=4.1"]
        );
        assert_eq!(
            join_mount_options(
                &["vers=4.1".into(), "hard".into(), "".into()],
                &["ro".into()]
            ),
            vec!["hard", "ro", "vers=4.1"]
        );
        assert!(mount_option_from_spec(&spec(&v, None, false), &[]).is_empty());
    }

    /// `ConstructVolumeSpec` (`nfs.go:175-187`).
    #[test]
    fn construct_volume_spec_uses_the_volume_name_as_path() {
        let tmp = tempfile::tempdir().unwrap();
        let rv = plugin(tmp.path())
            .construct_volume_spec("vol1", "/m")
            .unwrap();
        assert_eq!(rv.volume.as_ref().unwrap().name, "vol1");
        assert_eq!(rv.volume.unwrap().nfs.unwrap().path, "vol1");
    }

    /// `TearDownAt` on a directory that is not a mount point removes it
    /// (`CleanupMountWithForce` -> `removePathIfNotMountPoint`), and skips a
    /// path that does not exist.
    #[tokio::test]
    async fn tear_down_removes_an_unmounted_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let u = plugin(tmp.path()).new_unmounter("vol1", "poduid").unwrap();
        let dir = u.get_path();
        std::fs::create_dir_all(&dir).unwrap();
        u.tear_down().await.unwrap();
        assert!(!std::path::Path::new(&dir).exists());
        u.tear_down().await.unwrap();
    }
}
