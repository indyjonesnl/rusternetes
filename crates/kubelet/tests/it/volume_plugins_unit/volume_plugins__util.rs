//! Unit tests moved from `src/volume_plugins/util/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::volume::PersistentVolumeAccessMode;
    pub use rusternetes_common::resources::{Pod, Volume};
    pub use rusternetes_kubelet::volume_plugins::plugin::{Spec, VolumePlugin};
    pub use rusternetes_kubelet::volume_plugins::registry::VolumePluginMgr;
    pub use rusternetes_kubelet::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
}

mod fs_user_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::*;

    fn pod(v: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "u"},
            "spec": v
        }))
        .unwrap()
    }

    fn c(uid: Option<i64>) -> serde_json::Value {
        match uid {
            Some(u) => {
                serde_json::json!({"name": "c", "image": "i", "securityContext": {"runAsUser": u}})
            }
            None => serde_json::json!({"name": "c", "image": "i", "securityContext": {}}),
        }
    }

    /// `TestFsUserFrom` (`pkg/volume/util/util_test.go:149-292`), all five cases.
    #[test]
    fn fs_user_from_matches_upstream_cases() {
        // "no runAsUser specified"
        assert_eq!(
            fs_user_from(&pod(serde_json::json!({"containers": []}))),
            None
        );
        // "some have runAsUser specified"
        let p = pod(serde_json::json!({
            "initContainers": [c(Some(1000))],
            "containers": [c(Some(1000)), c(None)]
        }));
        assert_eq!(fs_user_from(&p), None);
        // "all have runAsUser specified but not the same"
        let p = pod(serde_json::json!({
            "initContainers": [c(Some(999))],
            "containers": [c(Some(1000)), c(Some(1000))],
            "ephemeralContainers": [c(Some(1001))]
        }));
        assert_eq!(fs_user_from(&p), None);
        // "init and regular containers have runAsUser specified and the same"
        let p = pod(serde_json::json!({
            "initContainers": [c(Some(1000))],
            "containers": [c(Some(1000)), c(Some(1000))]
        }));
        assert_eq!(fs_user_from(&p), Some(1000));
        // "all have runAsUser specified and the same"
        let p = pod(serde_json::json!({
            "initContainers": [c(Some(1000))],
            "containers": [c(Some(1000)), c(Some(1000))],
            "ephemeralContainers": [c(Some(1000))]
        }));
        assert_eq!(fs_user_from(&p), Some(1000));
    }

    /// `DetermineEffectiveRunAsUser` (`securitycontext/util.go:127-141`): the
    /// pod-level `runAsUser` applies to containers that set none.
    #[test]
    fn pod_level_run_as_user_is_the_default() {
        let p = pod(serde_json::json!({
            "securityContext": {"runAsUser": 7, "fsGroup": 9},
            "containers": [c(None), {"name": "d", "image": "i"}]
        }));
        assert_eq!(fs_user_from(&p), Some(7));
        assert_eq!(fs_group_from(&p), Some(9));
    }
}

/// `NewUnmounter` / `ConstructVolumeSpec` of the plugins that wrap emptyDir
/// (`configmap_test.go`, `secret_test.go`, `downwardapi_test.go`,
/// `projected_test.go` each end their plugin test with `TearDown()` and assert
/// the volume path is gone) and of hostPath (`TearDown` is a no-op,
/// `host_path.go:272-274`).
mod unmounter_tests {
    use rusternetes_kubelet::volume_plugins::{
        config_map::ConfigMapPlugin, downward_api::DownwardApiPlugin, host_path::HostPathPlugin,
        projected::ProjectedPlugin, secret::SecretPlugin, KubeletVolumeHost, VolumeHost,
        VolumePlugin,
    };
    use std::sync::Arc;

    fn host(root: &str) -> Arc<dyn VolumeHost> {
        Arc::new(KubeletVolumeHost::new_for_tests(
            root.to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        ))
    }

    fn root(tag: &str) -> String {
        let p = std::env::temp_dir().join(format!("unmounter-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p.to_string_lossy().into_owned()
    }

    async fn tear_down_removes(plugin: &dyn VolumePlugin, root: &str) {
        // The atomic writer's layout: <vol>/..data -> ..ts/, <vol>/key -> ..data/key.
        let dir =
            rusternetes_kubelet::pod_dirs::get_pod_volume_dir(root, "uid-1", plugin.name(), "vol");
        std::fs::create_dir_all(dir.join("..ts")).unwrap();
        std::fs::write(dir.join("..ts/key"), "v").unwrap();
        std::os::unix::fs::symlink("..ts", dir.join("..data")).unwrap();
        std::os::unix::fs::symlink("..data/key", dir.join("key")).unwrap();

        let u = plugin.new_unmounter("vol", "uid-1").unwrap();
        assert_eq!(u.get_path(), dir.to_string_lossy());
        u.tear_down().await.unwrap();

        assert!(!dir.exists(), "TearDown() failed, volume path still exists");
    }

    #[tokio::test]
    async fn config_map_tear_down_removes_the_volume() {
        let r = root("cm");
        tear_down_removes(&ConfigMapPlugin::new(host(&r)), &r).await;
    }

    #[tokio::test]
    async fn secret_tear_down_removes_the_volume() {
        let r = root("secret");
        tear_down_removes(&SecretPlugin::new(host(&r)), &r).await;
    }

    #[tokio::test]
    async fn downward_api_tear_down_removes_the_volume() {
        let r = root("dapi");
        tear_down_removes(&DownwardApiPlugin::new(host(&r)), &r).await;
    }

    #[tokio::test]
    async fn projected_tear_down_removes_the_volume() {
        let r = root("proj");
        tear_down_removes(&ProjectedPlugin::new(host(&r)), &r).await;
    }

    /// `TearDownAt(dir)` tears down the `dir` it is given, not its own path
    /// (`UnmountViaEmptyDir(dir, ...)`), so the wrapper-name rule
    /// (`wrapped_<name>`) never changes what is removed.
    #[tokio::test]
    async fn tear_down_at_removes_the_given_dir() {
        let r = root("at");
        let other = std::path::Path::new(&r).join("elsewhere");
        std::fs::create_dir_all(&other).unwrap();
        let u = SecretPlugin::new(host(&r))
            .new_unmounter("vol", "uid-1")
            .unwrap();
        u.tear_down_at(other.to_str().unwrap()).await.unwrap();
        assert!(!other.exists());
    }

    /// `host_path.go:272-280`: `TearDown` does nothing, `TearDownAt` is an
    /// error, and an unmounter's path is `""`.
    #[tokio::test]
    async fn host_path_tear_down_is_a_no_op_and_tear_down_at_is_an_error() {
        let r = root("hp");
        let u = HostPathPlugin::new(host(&r))
            .new_unmounter("vol", "uid-1")
            .unwrap();
        assert_eq!(u.get_path(), "");
        u.tear_down().await.unwrap();
        assert_eq!(
            u.tear_down_at("/x").await.unwrap_err().to_string(),
            "TearDownAt() does not make sense for host paths"
        );
    }

    /// Each `ConstructVolumeSpec` returns a volume of its own kind named after
    /// the volume (`secret.go:124-136` also sets `SecretName: volName`;
    /// `host_path.go:184-196` sets `Path: volumeName`).
    #[test]
    fn construct_volume_spec_rebuilds_a_volume_of_the_plugins_kind() {
        let r = root("construct");
        let h = host(&r);
        let cm = ConfigMapPlugin::new(h.clone())
            .construct_volume_spec("v", "/p")
            .unwrap();
        assert_eq!(cm.volume.as_ref().unwrap().name, "v");
        assert!(cm.volume.as_ref().unwrap().config_map.is_some());
        let sec = SecretPlugin::new(h.clone())
            .construct_volume_spec("v", "/p")
            .unwrap();
        assert_eq!(
            sec.volume.unwrap().secret.unwrap().secret_name.as_deref(),
            Some("v")
        );
        let d = DownwardApiPlugin::new(h.clone())
            .construct_volume_spec("v", "/p")
            .unwrap();
        assert!(d.volume.unwrap().downward_api.is_some());
        let p = ProjectedPlugin::new(h.clone())
            .construct_volume_spec("v", "/p")
            .unwrap();
        assert!(p.volume.unwrap().projected.is_some());
        let hp = HostPathPlugin::new(h)
            .construct_volume_spec("v", "/p")
            .unwrap();
        assert_eq!(hp.volume.unwrap().host_path.unwrap().path, "v");
    }
}

/// `IsLocalEphemeralVolume` (`pkg/volume/util/util.go:498-502`) — one case per
/// arm of the upstream disjunction.
mod local_ephemeral_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::*;

    fn vol(v: serde_json::Value) -> Volume {
        let mut m = serde_json::json!({"name": "v"});
        m.as_object_mut()
            .unwrap()
            .extend(v.as_object().unwrap().clone());
        serde_json::from_value(m).unwrap()
    }

    #[test]
    fn git_repo_is_local_ephemeral() {
        let v = vol(serde_json::json!({"gitRepo": {"repository": "https://x/y.git"}}));
        assert!(is_local_ephemeral_volume(&v));
    }

    #[test]
    fn default_medium_empty_dir_is_local_ephemeral() {
        assert!(is_local_ephemeral_volume(&vol(
            serde_json::json!({"emptyDir": {}})
        )));
        assert!(!is_local_ephemeral_volume(&vol(
            serde_json::json!({"emptyDir": {"medium": "Memory"}})
        )));
    }

    #[test]
    fn config_map_is_local_ephemeral() {
        assert!(is_local_ephemeral_volume(&vol(
            serde_json::json!({"configMap": {"name": "c"}})
        )));
    }

    #[test]
    fn other_sources_are_not_local_ephemeral() {
        assert!(!is_local_ephemeral_volume(&vol(
            serde_json::json!({"secret": {"secretName": "s"}})
        )));
    }
}

/// `TestHasMountRefs` (`pkg/volume/util/util_test.go:308-340`) and the
/// `GetDeviceMountRefs check failed` arms of `GenerateUnmountDeviceFunc`.
mod mount_refs_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::*;

    fn v(s: &[&str]) -> Vec<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn has_mount_refs_matches_upstream_cases() {
        let path = "/var/lib/kubelet/plugins/kubernetes.io/some-plugin/mounts/volume-XXXX";
        // "plugin mounts only"
        assert!(!has_mount_refs(
            path,
            &v(&[
                "/home/somewhere/var/lib/kubelet/plugins/kubernetes.io/some-plugin/mounts/volume-XXXX",
                "/var/lib/kubelet/plugins/kubernetes.io/some-plugin/mounts/volume-XXXX",
                "/mnt/kubelet/plugins/kubernetes.io/some-plugin/mounts/volume-XXXX",
                "/mnt/plugins/kubernetes.io/some-plugin/mounts/volume-XXXX",
            ])
        ));
        // "extra local mount"
        assert!(has_mount_refs(
            path,
            &v(&[
                "/home/somewhere/var/lib/kubelet/plugins/kubernetes.io/some-plugin/mounts/volume-XXXX",
                "/local/data/kubernetes.io/some-plugin/mounts/volume-XXXX",
                "/mnt/kubelet/plugins/kubernetes.io/some-plugin/mounts/volume-XXXX",
                "/mnt/plugins/kubernetes.io/some-plugin/mounts/volume-XXXX",
            ])
        ));
    }

    /// No `/plugins/kubernetes.io/` in the path: the whole path is searched for.
    #[test]
    fn has_mount_refs_without_plugin_prefix_uses_the_whole_path() {
        assert!(!has_mount_refs("/a/b", &v(&["/x/a/b/y"])));
        assert!(has_mount_refs("/a/b", &v(&["/data/other"])));
        assert!(!has_mount_refs("/a/b", &[]));
    }

    #[test]
    fn check_device_mount_refs_errors_like_upstream() {
        let path = "/var/lib/kubelet/plugins/kubernetes.io/csi/pv/x/globalmount";
        assert!(check_device_mount_refs(path, Ok(vec![])).is_ok());
        let own = vec![format!("/mnt{path}")];
        assert!(check_device_mount_refs(path, Ok(own)).is_ok());
        let err = check_device_mount_refs(path, Ok(vec!["/local/manual".to_string()]))
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            format!(
                "the device mount path \"{path}\" is still mounted by other references [/local/manual]"
            )
        );
        let err = check_device_mount_refs(path, Err(anyhow::anyhow!("boom")))
            .unwrap_err()
            .to_string();
        assert_eq!(err, "boom");
    }
}

mod wrapper_mounter_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::empty_dir::fake_mount_deps;
    use rusternetes_kubelet::volume_plugins::util::*;

    fn pod() -> rusternetes_common::resources::Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    fn tmp(tag: &str) -> String {
        let d = std::env::temp_dir().join(format!("wrapper-mounter-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().into_owned()
    }

    fn host_with(
        root: &str,
        deps: rusternetes_kubelet::volume_plugins::empty_dir::EmptyDirMountDeps,
    ) -> std::sync::Arc<dyn rusternetes_kubelet::volume_plugins::VolumeHost> {
        std::sync::Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                root.to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            )
            .with_empty_dir_mount_deps(deps),
        )
    }

    /// `wrappedVolumeSpec()` with `Medium: Memory` (`secret.go:52-56`,
    /// `downwardapi.go:54-58`, `projected.go:59-67`): `SetUpAt` mounts a
    /// tmpfs on the dir (`setupTmpfs`, `empty_dir.go:320-340`) and writes the
    /// ready file (`:284`).
    #[tokio::test]
    async fn memory_wrapper_mounts_a_tmpfs_and_sets_ready() {
        let root = tmp("mem");
        let (deps, rec) = fake_mount_deps();
        let host = host_with(&root, deps);
        let dir = format!("{root}/vol");
        let m = new_wrapper_mounter(&host, "sec", Some("Memory"), &pod())
            .await
            .unwrap();
        m.set_up_at(&dir, &Default::default()).await.unwrap();
        let calls = rec.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "{calls:?}");
        assert_eq!(calls[0].0, "tmpfs");
        assert_eq!(calls[0].1, dir);
        assert_eq!(calls[0].2, "tmpfs");
        assert!(std::path::Path::new(&dir).is_dir());
        assert!(
            is_ready(&wrapped_empty_dir_meta_dir(&host, "sec", "uid-1")),
            "wrapped_sec ready dir"
        );
    }

    /// configMap's `wrappedVolumeSpec()` has no medium (`configmap.go:165-172`):
    /// a disk emptyDir, so no mount.
    #[tokio::test]
    async fn default_medium_wrapper_does_not_mount() {
        let root = tmp("def");
        let (deps, rec) = fake_mount_deps();
        let host = host_with(&root, deps);
        let dir = format!("{root}/vol");
        let m = new_wrapper_mounter(&host, "cfg", None, &pod())
            .await
            .unwrap();
        m.set_up_at(&dir, &Default::default()).await.unwrap();
        assert!(rec.calls.lock().unwrap().is_empty());
        assert!(is_ready(&wrapped_empty_dir_meta_dir(&host, "cfg", "uid-1")));
    }

    /// `setupTmpfs` is idempotent (`empty_dir.go:326-334`): a dir that already
    /// is a memory mount point is not mounted again.
    #[tokio::test]
    async fn memory_wrapper_does_not_remount_an_existing_tmpfs() {
        use rusternetes_kubelet::volume_plugins::empty_dir::{
            EmptyDirMountDeps, Medium, MountDetector,
        };
        struct AlreadyMounted;
        impl MountDetector for AlreadyMounted {
            fn get_mount_medium(
                &self,
                _p: &str,
                _m: &str,
            ) -> anyhow::Result<(Medium, bool, Option<u64>)> {
                Ok((Medium::Memory, true, None))
            }
        }
        let root = tmp("idem");
        let (deps, rec) = fake_mount_deps();
        let host = host_with(
            &root,
            EmptyDirMountDeps {
                detector: std::sync::Arc::new(AlreadyMounted),
                ..deps
            },
        );
        let m = new_wrapper_mounter(&host, "sec", Some("Memory"), &pod())
            .await
            .unwrap();
        m.set_up_at(&format!("{root}/vol"), &Default::default())
            .await
            .unwrap();
        assert!(rec.calls.lock().unwrap().is_empty());
    }
}

mod unmount_device_tests {
    //! Tests for `operationGenerator.GenerateUnmountDeviceFunc`
    //! (`pkg/volume/util/operationexecutor/operation_generator.go:807-905`) and
    //! `isDeviceOpened` (`:2179-2198`).
    //!
    //! Upstream has no direct unit test of the function: `operation_executor_test.go`
    //! drives it only through `fakegenerator.go`. The cases here are therefore one
    //! per branch of the closure, with `volumetesting.FakeVolumePlugin` replaced by
    //! a fake [`DeviceMountableVolumePlugin`] and `hostutil.FakeHostUtil`
    //! (`fake_hostutil.go:51-66`) by [`FakeHostUtil`].

    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use rusternetes_common::resources::Pod;
    use rusternetes_kubelet::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
    use rusternetes_kubelet::volume_plugins::plugin::{
        DeviceMountableVolumePlugin, DeviceMounter, DeviceMounterArgs, DeviceUnmounter, Mounter,
        OwnedSpec, ReconstructedVolume, Spec, Unmounter, VolumePlugin,
    };
    use rusternetes_kubelet::volume_plugins::registry::VolumePluginMgr;
    use rusternetes_kubelet::volume_plugins::util::hostutil::HostUtils;
    use rusternetes_kubelet::volume_plugins::util::operation_executor::AttachedVolume;
    use rusternetes_kubelet::volume_plugins::util::operation_executor::DeviceMountState;
    use rusternetes_kubelet::volume_plugins::util::operation_generator::generate_unmount_device_func;
    use rusternetes_kubelet::volume_plugins::util::types::{OperationContext, UniqueVolumeName};
    use std::io;
    use std::sync::{Arc, Mutex};

    const PLUGIN: &str = "fake-plugin";
    const VOLUME: &str = "vol-1";
    const DEVICE_PATH: &str = "/dev/fake";
    const MOUNT_PATH: &str = "/var/lib/kubelet/plugins/kubernetes.io/fake/globalmount";

    /// What the fake plugin was asked to do, and what it should answer.
    #[derive(Default)]
    struct Behaviour {
        /// `GetDeviceMountPath`: `Ok(path)` or `Err(message)`.
        mount_path: Option<Result<String, String>>,
        /// `GetDeviceMountRefs`.
        refs: Option<Result<Vec<String>, String>>,
        /// `UnmountDevice` fails with this message.
        unmount_error: Option<String>,
        fail_new_unmounter: bool,
        fail_new_mounter: bool,
        /// Every `UnmountDevice(deviceMountPath)` call, in order.
        unmounted: Vec<String>,
        /// Every `GetDeviceMountRefs(deviceMountPath)` call.
        refs_asked: Vec<String>,
    }

    #[derive(Clone, Default)]
    struct Fake(Arc<Mutex<Behaviour>>);

    struct FakePlugin {
        fake: Fake,
        /// `false` registers a plugin that is not a `DeviceMountableVolumePlugin`.
        device_mountable: bool,
    }

    #[async_trait]
    impl VolumePlugin for FakePlugin {
        fn name(&self) -> &'static str {
            PLUGIN
        }
        fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
            Ok(spec.name().to_string())
        }
        fn can_support(&self, _spec: &Spec<'_>) -> bool {
            true
        }
        fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
            false
        }
        fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
            Ok(false)
        }
        fn as_device_mountable_plugin(&self) -> Option<&dyn DeviceMountableVolumePlugin> {
            self.device_mountable.then_some(self as _)
        }
        async fn new_mounter(&self, _s: &Spec<'_>, _p: &Pod) -> Result<Box<dyn Mounter>> {
            Err(anyhow!("not used"))
        }
        fn new_unmounter(&self, _n: &str, _u: &str) -> Result<Box<dyn Unmounter>> {
            Err(anyhow!("not used"))
        }
        fn construct_volume_spec(&self, _n: &str, _p: &str) -> Result<ReconstructedVolume> {
            Err(anyhow!("not used"))
        }
    }

    impl DeviceMountableVolumePlugin for FakePlugin {
        fn new_device_mounter(&self) -> Result<Box<dyn DeviceMounter>> {
            if self.fake.0.lock().unwrap().fail_new_mounter {
                return Err(anyhow!("new mounter boom"));
            }
            Ok(Box::new(self.fake.clone()))
        }
        fn new_device_unmounter(&self) -> Result<Box<dyn DeviceUnmounter>> {
            if self.fake.0.lock().unwrap().fail_new_unmounter {
                return Err(anyhow!("new unmounter boom"));
            }
            Ok(Box::new(self.fake.clone()))
        }
        fn get_device_mount_refs(&self, device_mount_path: &str) -> Result<Vec<String>> {
            let mut b = self.fake.0.lock().unwrap();
            b.refs_asked.push(device_mount_path.to_string());
            match b.refs.clone().unwrap_or(Ok(vec![])) {
                Ok(r) => Ok(r),
                Err(e) => Err(anyhow!(e)),
            }
        }
    }

    #[async_trait]
    impl DeviceMounter for Fake {
        fn get_device_mount_path(&self, _spec: &Spec<'_>) -> Result<String> {
            match self
                .0
                .lock()
                .unwrap()
                .mount_path
                .clone()
                .unwrap_or_else(|| Ok(MOUNT_PATH.to_string()))
            {
                Ok(p) => Ok(p),
                Err(e) => Err(anyhow!(e)),
            }
        }
        async fn mount_device(
            &self,
            _spec: &Spec<'_>,
            _device_path: &str,
            _device_mount_path: &str,
            _args: &DeviceMounterArgs,
        ) -> Result<()> {
            Err(anyhow!("not used"))
        }
    }

    #[async_trait]
    impl DeviceUnmounter for Fake {
        async fn unmount_device(&self, device_mount_path: &str) -> Result<()> {
            let mut b = self.0.lock().unwrap();
            b.unmounted.push(device_mount_path.to_string());
            match b.unmount_error.clone() {
                Some(e) => Err(anyhow!(e)),
                None => Ok(()),
            }
        }
    }

    /// `hostutil.FakeHostUtil` with the answers made settable.
    struct FakeHostUtil {
        path_is_device: io::Result<bool>,
        device_opened: io::Result<bool>,
    }

    impl FakeHostUtil {
        /// `FakeHostUtil.PathIsDevice` always returns true; no mount holds the
        /// device (`fake_hostutil.go:51-66`).
        fn idle() -> Self {
            Self {
                path_is_device: Ok(true),
                device_opened: Ok(false),
            }
        }
    }

    fn copy(r: &io::Result<bool>) -> io::Result<bool> {
        match r {
            Ok(b) => Ok(*b),
            Err(e) => Err(io::Error::other(e.to_string())),
        }
    }

    impl HostUtils for FakeHostUtil {
        fn device_opened(&self, _pathname: &str) -> io::Result<bool> {
            copy(&self.device_opened)
        }
        fn path_is_device(&self, _pathname: &str) -> io::Result<bool> {
            copy(&self.path_is_device)
        }
        fn get_file_type(
            &self,
            _pathname: &str,
        ) -> io::Result<rusternetes_kubelet::volume_plugins::util::hostutil::FileType> {
            Err(io::Error::other("FakeHostUtil: GetFileType is not faked"))
        }
    }

    struct Fixture {
        fake: Fake,
        mgr: Arc<VolumePluginMgr>,
        asw: ActualStateOfWorld,
        volume: AttachedVolume,
    }

    fn spec() -> Arc<OwnedSpec> {
        Arc::new(OwnedSpec {
            volume: serde_json::from_value(serde_json::json!({"name": VOLUME, "emptyDir": {}}))
                .unwrap(),
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        })
    }

    /// A volume the ASW knows as globally mounted at [`MOUNT_PATH`].
    fn fixture_with(device_mountable: bool, plugin_name: &str) -> Fixture {
        let fake = Fake::default();
        let mgr = Arc::new(VolumePluginMgr::new(vec![Box::new(FakePlugin {
            fake: fake.clone(),
            device_mountable,
        })]));
        let asw = ActualStateOfWorld::new("node-1", Arc::clone(&mgr));
        let name = UniqueVolumeName(format!("{PLUGIN}/{VOLUME}"));
        asw.mark_volume_as_attached(Some(&name), spec(), "node-1", DEVICE_PATH)
            .unwrap();
        asw.mark_device_as_mounted(&name, DEVICE_PATH, MOUNT_PATH, "")
            .unwrap();
        let volume = AttachedVolume {
            volume_name: name,
            volume_spec: spec(),
            node_name: "node-1".to_string(),
            plugin_is_attachable: false,
            device_path: DEVICE_PATH.to_string(),
            device_mount_path: "/recorded/in/asw".to_string(),
            plugin_name: plugin_name.to_string(),
            selinux_mount_context: String::new(),
        };
        Fixture {
            fake,
            mgr,
            asw,
            volume,
        }
    }

    fn fixture() -> Fixture {
        fixture_with(true, PLUGIN)
    }

    impl Fixture {
        async fn run(&self, host: &FakeHostUtil) -> OperationContext {
            generate_unmount_device_func(&self.mgr, &self.volume, &self.asw, host)
                .expect("generate")
                .run()
                .await
        }

        fn state(&self) -> DeviceMountState {
            self.asw.get_device_mount_state(&self.volume.volume_name)
        }

        fn unmounted(&self) -> Vec<String> {
            self.fake.0.lock().unwrap().unmounted.clone()
        }
    }

    /// The detailed message `AttachedVolume.GenerateMsgDetailed` builds
    /// (`operation_executor.go:576-583`, `generateVolumeMsgDetailed` `:315-317`).
    fn detailed(prefix: &str, suffix: &str) -> String {
        format!(
            "{prefix} for volume {VOLUME:?} (UniqueName: \"{PLUGIN}/{VOLUME}\") on node \"node-1\" {suffix}"
        )
    }

    /// The simple message `AttachedVolume.GenerateMsg` builds for events
    /// (`generateVolumeMsg`, `operation_executor.go:320-323`).
    fn simple(prefix: &str, suffix: &str) -> String {
        format!("{prefix} for volume {VOLUME:?} {suffix}")
    }

    fn event(ctx: &OperationContext) -> Option<String> {
        ctx.event_err.as_ref().map(|e| e.to_string())
    }

    fn logged(ctx: &OperationContext) -> Option<String> {
        ctx.detailed_err.as_ref().map(|e| e.to_string())
    }

    // ---- generation-time failures (`:812-828`) ---------------------------------

    #[test]
    fn unknown_plugin_name_fails_generation() {
        let f = fixture_with(true, "no-such-plugin");
        let err = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &FakeHostUtil::idle())
            .err()
            .expect("error");
        assert_eq!(
            err.to_string(),
            detailed(
                "UnmountDevice.FindDeviceMountablePluginByName failed",
                ": no volume plugin matched"
            )
        );
    }

    /// `FindDeviceMountablePluginByName` returns `(nil, nil)` for a plugin that is
    /// not a `DeviceMountableVolumePlugin` (`plugins.go:857-860`), which the
    /// generator reports through the same `GenerateErrorDetailed` with a nil error.
    #[test]
    fn plugin_that_is_not_device_mountable_fails_generation() {
        let f = fixture_with(false, PLUGIN);
        let err = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &FakeHostUtil::idle())
            .err()
            .expect("error");
        assert_eq!(
            err.to_string(),
            detailed("UnmountDevice.FindDeviceMountablePluginByName failed", "")
        );
    }

    #[test]
    fn new_device_unmounter_failure_fails_generation() {
        let f = fixture();
        f.fake.0.lock().unwrap().fail_new_unmounter = true;
        let err = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &FakeHostUtil::idle())
            .err()
            .expect("error");
        assert_eq!(
            err.to_string(),
            detailed(
                "UnmountDevice.NewDeviceUnmounter failed",
                ": new unmounter boom"
            )
        );
    }

    #[test]
    fn new_device_mounter_failure_fails_generation() {
        let f = fixture();
        f.fake.0.lock().unwrap().fail_new_mounter = true;
        let err = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &FakeHostUtil::idle())
            .err()
            .expect("error");
        assert_eq!(
            err.to_string(),
            detailed(
                "UnmountDevice.NewDeviceMounter failed",
                ": new mounter boom"
            )
        );
    }

    #[test]
    fn operation_is_named_unmount_device() {
        let f = fixture();
        let host = FakeHostUtil::idle();
        let op = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &host).expect("generate");
        assert_eq!(op.operation_name, "unmount_device");
    }

    // ---- the closure (`:830-896`) ----------------------------------------------

    /// The whole happy path: refs checked on the path the mounter computed,
    /// `UnmountDevice` on it, `MarkDeviceAsUnmounted` (`:895`).
    #[tokio::test]
    async fn unmounts_the_device_and_marks_it_unmounted() {
        let f = fixture();
        let ctx = f.run(&FakeHostUtil::idle()).await;
        assert_eq!((event(&ctx), logged(&ctx)), (None, None));
        assert_eq!(f.unmounted(), vec![MOUNT_PATH.to_string()]);
        assert_eq!(
            f.fake.0.lock().unwrap().refs_asked,
            vec![MOUNT_PATH.to_string()]
        );
        assert_eq!(f.state(), DeviceMountState::DeviceNotMounted);
        assert!(!ctx.migrated);
    }

    /// `GetDeviceMountPath` failing with "does not exist" is not fatal: the path
    /// recorded on the volume is used instead (`:824-836`).
    #[tokio::test]
    async fn missing_device_mount_path_falls_back_to_the_recorded_one() {
        let f = fixture();
        f.fake.0.lock().unwrap().mount_path = Some(Err("the volume does not exist".into()));
        let ctx = f.run(&FakeHostUtil::idle()).await;
        assert_eq!((event(&ctx), logged(&ctx)), (None, None));
        assert_eq!(f.unmounted(), vec!["/recorded/in/asw".to_string()]);
        assert_eq!(f.state(), DeviceMountState::DeviceNotMounted);
    }

    /// Any other `GetDeviceMountPath` failure is returned (`:826-830`).
    #[tokio::test]
    async fn other_device_mount_path_failure_is_returned() {
        let f = fixture();
        f.fake.0.lock().unwrap().mount_path = Some(Err("boom".into()));
        let ctx = f.run(&FakeHostUtil::idle()).await;
        assert_eq!(
            event(&ctx),
            Some(simple("GetDeviceMountPath failed", ": boom"))
        );
        assert_eq!(
            logged(&ctx),
            Some(detailed("GetDeviceMountPath failed", ": boom"))
        );
        assert!(f.unmounted().is_empty());
        assert_eq!(f.state(), DeviceMountState::DeviceGloballyMounted);
    }

    /// A failing `GetDeviceMountRefs` blocks the unmount (`:846-856`).
    #[tokio::test]
    async fn refs_lookup_failure_blocks_the_unmount() {
        let f = fixture();
        f.fake.0.lock().unwrap().refs = Some(Err("mountinfo unreadable".into()));
        let ctx = f.run(&FakeHostUtil::idle()).await;
        assert_eq!(
            event(&ctx),
            Some(simple(
                "GetDeviceMountRefs check failed",
                ": mountinfo unreadable"
            ))
        );
        assert!(f.unmounted().is_empty());
        assert_eq!(f.state(), DeviceMountState::DeviceGloballyMounted);
    }

    /// An outside reference to the device mount path blocks the unmount
    /// (`HasMountRefs`, `:846-856`); the kubelet's own plugin-dir binds do not.
    #[tokio::test]
    async fn outside_reference_blocks_the_unmount() {
        let f = fixture();
        f.fake.0.lock().unwrap().refs = Some(Ok(vec!["/mnt/other".into()]));
        let ctx = f.run(&FakeHostUtil::idle()).await;
        assert_eq!(
            event(&ctx),
            Some(simple(
                "GetDeviceMountRefs check failed",
                &format!(
                    ": the device mount path {MOUNT_PATH:?} is still mounted by other references [/mnt/other]"
                )
            ))
        );
        assert!(f.unmounted().is_empty());

        let f = fixture();
        f.fake.0.lock().unwrap().refs = Some(Ok(vec![format!("/some/root{MOUNT_PATH}")]));
        let ctx = f.run(&FakeHostUtil::idle()).await;
        assert_eq!((event(&ctx), logged(&ctx)), (None, None));
        assert_eq!(f.unmounted().len(), 1);
    }

    /// `UnmountDevice` failing marks the device uncertain so `MountDevice` is
    /// called for new pods, and returns the error (`:859-870`).
    #[tokio::test]
    async fn unmount_failure_marks_the_device_uncertain() {
        let f = fixture();
        f.fake.0.lock().unwrap().unmount_error = Some("unstage boom".into());
        let ctx = f.run(&FakeHostUtil::idle()).await;
        assert_eq!(
            event(&ctx),
            Some(simple("UnmountDevice failed", ": unstage boom"))
        );
        assert_eq!(
            logged(&ctx),
            Some(detailed("UnmountDevice failed", ": unstage boom"))
        );
        assert_eq!(f.state(), DeviceMountState::DeviceMountUncertain);
    }

    /// A failing `MarkDeviceAsUncertain` is only logged ("There is nothing else
    /// we can do", `:862-865`); the `UnmountDevice` error is still returned.
    #[tokio::test]
    async fn failing_mark_uncertain_does_not_replace_the_unmount_error() {
        let mut f = fixture();
        f.fake.0.lock().unwrap().unmount_error = Some("unstage boom".into());
        // The ASW has never heard of this volume.
        f.asw = ActualStateOfWorld::new("node-1", Arc::clone(&f.mgr));
        let ctx = f.run(&FakeHostUtil::idle()).await;
        assert_eq!(
            event(&ctx),
            Some(simple("UnmountDevice failed", ": unstage boom"))
        );
    }

    /// The device is still open elsewhere after a successful `UnmountDevice`:
    /// uncertain, retry (`:874-893`).
    #[tokio::test]
    async fn device_still_open_marks_uncertain_and_retries() {
        let f = fixture();
        let host = FakeHostUtil {
            path_is_device: Ok(true),
            device_opened: Ok(true),
        };
        let ctx = f.run(&host).await;
        assert_eq!(
            event(&ctx),
            Some(simple(
                "UnmountDevice failed",
                ": the device is in use when it was no longer expected to be in use"
            ))
        );
        assert_eq!(f.unmounted().len(), 1);
        assert_eq!(f.state(), DeviceMountState::DeviceMountUncertain);
    }

    /// `isDeviceOpened`: `PathIsDevice` false, or an error mentioning "does not
    /// exist", skips the check (`:2183-2188`).
    #[tokio::test]
    async fn not_a_device_or_missing_path_skips_the_opened_check() {
        for path_is_device in [
            Ok(false),
            Err(io::Error::other("path \"/x\" does not exist")),
        ] {
            let f = fixture();
            let host = FakeHostUtil {
                path_is_device,
                // Would fail the operation if it were consulted.
                device_opened: Ok(true),
            };
            let ctx = f.run(&host).await;
            assert_eq!((event(&ctx), logged(&ctx)), (None, None));
            assert_eq!(f.state(), DeviceMountState::DeviceNotMounted);
        }
    }

    /// Any other `PathIsDevice` error is returned as a detailed-only error, with
    /// no event error and no state change (`:2189-2191`, `:877-880`).
    #[tokio::test]
    async fn path_is_device_error_is_a_detailed_only_failure() {
        let f = fixture();
        let host = FakeHostUtil {
            path_is_device: Err(io::Error::other("permission denied")),
            device_opened: Ok(false),
        };
        let ctx = f.run(&host).await;
        assert_eq!(event(&ctx), None);
        assert_eq!(
            logged(&ctx),
            Some(detailed("PathIsDevice failed", ": permission denied"))
        );
        assert_eq!(f.state(), DeviceMountState::DeviceGloballyMounted);
    }

    #[tokio::test]
    async fn device_opened_error_is_a_detailed_only_failure() {
        let f = fixture();
        let host = FakeHostUtil {
            path_is_device: Ok(true),
            device_opened: Err(io::Error::other("ebadf")),
        };
        let ctx = f.run(&host).await;
        assert_eq!(event(&ctx), None);
        assert_eq!(
            logged(&ctx),
            Some(detailed("DeviceOpened failed", ": ebadf"))
        );
        assert_eq!(f.state(), DeviceMountState::DeviceGloballyMounted);
    }

    /// `MarkDeviceAsUnmounted` failing (volume absent from the ASW) is an error
    /// the caller retries (`:895-901`).
    #[tokio::test]
    async fn failing_mark_unmounted_is_returned() {
        let mut f = fixture();
        f.asw = ActualStateOfWorld::new("node-1", Arc::clone(&f.mgr));
        let ctx = f.run(&FakeHostUtil::idle()).await;
        let msg = event(&ctx).expect("event error");
        assert!(
            msg.starts_with(&simple("MarkDeviceAsUnmounted failed", ": ")),
            "{msg}"
        );
        assert_eq!(f.unmounted().len(), 1);
    }
}
