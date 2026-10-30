//! Unit tests moved from `src/volume_plugins/local.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, bail, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::{EventType, Pod};
    pub use rusternetes_kubelet::volume_plugins::metrics_statfs::MetricsStatFs;
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        BlockVolumeMapper, BlockVolumePlugin, BlockVolumeUnmapper, CustomBlockVolumeMapper,
        DeviceMountableVolumePlugin, DeviceUnmounter, Metrics, MetricsProvider, OwnedSpec,
    };
    pub use rusternetes_kubelet::volume_plugins::util::exec::{Exec, RealExec};
    pub use rusternetes_kubelet::volume_plugins::util::hostutil::FileType;
    pub use rusternetes_kubelet::volume_plugins::util::key_mutex::KeyMutex;
    pub use rusternetes_kubelet::volume_plugins::util::metrics::{
        new_metrics_block, MetricsBlock, MetricsNil,
    };
    pub use rusternetes_kubelet::volume_plugins::util::mount_interface::{
        MountInterface, SafeFormatAndMount,
    };
    pub use rusternetes_kubelet::volume_plugins::util::mount_ops::cleanup_mount_point;
    pub use rusternetes_kubelet::volume_plugins::util::mount_utils::{
        is_not_mount_point_with, path_within_base,
    };
    pub use rusternetes_kubelet::volume_plugins::util::{
        join_mount_options, mount_option_from_spec,
    };
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, DeviceMounter, DeviceMounterArgs, Mounter, MounterArgs, ReconstructedVolume,
        Spec, Unmounter, VolumeHost, VolumePlugin,
    };
    pub use std::io;
    pub use std::os::unix::fs::DirBuilderExt;
    pub use std::path::Path;
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    //! Port of `pkg/volume/local/local_test.go`, with `volumetest.FakeKubeletVolumeHost`
    //! replaced by a [`KubeletVolumeHost`] rooted in a temp dir and
    //! `mount.FakeMounter` by the crate's `FakeMounter`.

    use super::__parent::*;
    use rusternetes_common::resources::{Event, PersistentVolume, Volume};
    use rusternetes_kubelet::volume_plugins::local::*;
    use rusternetes_kubelet::volume_plugins::util::exec::ExecError;
    use rusternetes_kubelet::volume_plugins::util::mount_interface::testing::{
        FakeMounter, MountPoint,
    };
    use rusternetes_kubelet::volume_plugins::{KubeletVolumeHost, MounterArgs};
    use rusternetes_storage::{EventRecorder, Storage, StorageBackend};
    use serde_json::json;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    const TEST_PV_NAME: &str = "pvA";
    const TEST_POD_UID: &str = "poduid";

    /// `testingexec.FakeExec{DisableScripts: true}` (`local_test.go:269`): every
    /// command succeeds with empty output.
    struct DisabledScripts;

    impl Exec for DisabledScripts {
        fn combined_output(&self, _cmd: &str, _args: &[String]) -> (String, Result<(), ExecError>) {
            (String::new(), Ok(()))
        }
    }

    /// A canonicalised temp dir (the fake mounter canonicalises mount targets).
    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("local-plugin-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::canonicalize(d).unwrap()
    }

    fn host(base: &Path) -> Arc<dyn VolumeHost> {
        Arc::new(KubeletVolumeHost::new_for_tests(
            base.to_string_lossy().into_owned(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        ))
    }

    /// `getPlugin` (`local_test.go:62-`): the plugin over a fake mounter and a
    /// host util where every path is a directory.
    fn plugin(base: &Path) -> (LocalPlugin, Arc<FakeMounter>) {
        let fake = Arc::new(FakeMounter::new());
        let plug = LocalPlugin::new(host(base))
            .with_mounter(fake.clone())
            .with_file_type(Arc::new(|_| Ok(FileType::Directory)));
        (plug, fake)
    }

    fn pv(path: &str, block: bool, mount_options: &[&str]) -> PersistentVolume {
        serde_json::from_value(json!({
            "metadata": {"name": TEST_PV_NAME},
            "spec": {
                "local": {"path": path},
                "volumeMode": if block { "Block" } else { "Filesystem" },
                "mountOptions": mount_options,
            }
        }))
        .unwrap()
    }

    fn claimed() -> Volume {
        serde_json::from_value(json!({"name": "pvA", "persistentVolumeClaim": {"claimName": "c"}}))
            .unwrap()
    }

    fn spec<'a>(v: &'a Volume, pv: &'a PersistentVolume, read_only: bool) -> Spec<'a> {
        Spec {
            volume: v,
            persistent_volume: Some(pv),
            read_only,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        }
    }

    fn pod(uid: &str) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": format!("p-{uid}"), "namespace": "default", "uid": uid},
            "spec": {"containers": [], "nodeName": "node-1"}
        }))
        .unwrap()
    }

    fn mode(p: &Path) -> u32 {
        std::fs::metadata(p).unwrap().permissions().mode() & 0o7777
    }

    fn pod_volume_path(base: &Path, uid: &str) -> std::path::PathBuf {
        base.join("pods")
            .join(uid)
            .join("volumes")
            .join("kubernetes.io~local-volume")
            .join(TEST_PV_NAME)
    }

    /// `TestCanSupport` (`local_test.go:201`): PV arm only.
    #[test]
    fn can_support_only_a_local_pv() {
        let base = tmp("can-support");
        let (plug, _) = plugin(&base);
        let (v, p) = (claimed(), pv("/mnt/disks/a", false, &[]));
        assert!(plug.can_support(&spec(&v, &p, false)));
        let s = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(!plug.can_support(&s));
    }

    /// `TestGetVolumeName` (`local_test.go:227`): the PV name.
    #[test]
    fn volume_name_is_the_pv_name() {
        let base = tmp("vol-name");
        let (plug, _) = plugin(&base);
        let (v, p) = (claimed(), pv("/mnt/disks/a", false, &[]));
        assert_eq!(
            plug.get_volume_name(&spec(&v, &p, false)).unwrap(),
            TEST_PV_NAME
        );
    }

    /// `TestMountUnmount` (`local_test.go:360`): `GetPath` is the pod volume dir,
    /// `SetUp` bind mounts the global path onto it, `TearDown` removes it.
    #[tokio::test]
    async fn mount_unmount_binds_global_path_onto_pod_dir() {
        let base = tmp("mount-unmount");
        let src = tmp("mount-unmount-src");
        let (plug, fake) = plugin(&base);
        let (v, p) = (claimed(), pv(src.to_str().unwrap(), false, &[]));
        let m = plug
            .new_mounter(&spec(&v, &p, false), &pod(TEST_POD_UID))
            .await
            .unwrap();

        let path = pod_volume_path(&base, TEST_POD_UID);
        assert_eq!(m.get_path(), path.to_string_lossy());
        m.set_up_with(&MounterArgs::default()).await.unwrap();
        assert!(path.is_dir(), "SetUp() failed, volume path not created");
        {
            let mps = fake.mount_points.lock().unwrap();
            assert_eq!(mps.len(), 1);
            assert_eq!(mps[0].path, path.to_string_lossy());
            assert_eq!(mps[0].device, src.to_string_lossy());
            assert_eq!(mps[0].opts, vec!["bind".to_string()]);
        }
        assert_eq!(mode(&path), 0o750);

        let u = plug.new_unmounter(TEST_PV_NAME, TEST_POD_UID).unwrap();
        assert_eq!(u.get_path(), path.to_string_lossy());
        u.tear_down_at(&u.get_path()).await.unwrap();
        assert!(
            !path.exists(),
            "TearDown() failed, volume path still exists"
        );
    }

    /// `SetUpAt` is idempotent: an already-mounted dir returns before mounting
    /// again (`local.go:562-564`).
    #[tokio::test]
    async fn set_up_twice_mounts_once() {
        let base = tmp("twice");
        let src = tmp("twice-src");
        let (plug, fake) = plugin(&base);
        let (v, p) = (claimed(), pv(src.to_str().unwrap(), false, &[]));
        let m = plug
            .new_mounter(&spec(&v, &p, false), &pod(TEST_POD_UID))
            .await
            .unwrap();
        m.set_up().await.unwrap();
        m.set_up().await.unwrap();
        assert_eq!(fake.mount_points.lock().unwrap().len(), 1);
    }

    /// `TestInvalidLocalPath` (`local_test.go:240`).
    #[tokio::test]
    async fn invalid_local_path_is_rejected() {
        let base = tmp("invalid");
        let (plug, fake) = plugin(&base);
        let (v, p) = (claimed(), pv("/no/backsteps/allowed/..", false, &[]));
        let m = plug
            .new_mounter(&spec(&v, &p, false), &pod(TEST_POD_UID))
            .await
            .unwrap();
        let err = m.set_up().await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid path: /no/backsteps/allowed/.. must not contain '..'"
        );
        assert!(fake.mount_points.lock().unwrap().is_empty());
    }

    /// `TestMountOptions` (`local_test.go:641`): `bind` plus the PV's options.
    #[tokio::test]
    async fn mount_options_are_joined_with_bind() {
        let base = tmp("mount-opts");
        let src = tmp("mount-opts-src");
        let (plug, fake) = plugin(&base);
        let (v, p) = (
            claimed(),
            pv(src.to_str().unwrap(), false, &["test-option"]),
        );
        let m = plug
            .new_mounter(&spec(&v, &p, false), &pod(TEST_POD_UID))
            .await
            .unwrap();
        m.set_up().await.unwrap();
        let mps = fake.mount_points.lock().unwrap();
        assert_eq!(
            mps[0].opts,
            vec!["bind".to_string(), "test-option".to_string()]
        );
    }

    /// `TestPersistentClaimReadOnlyFlag` (`local_test.go:668`): the claim's
    /// read-only flag becomes `ro`, `GetAttributes().ReadOnly`, and skips the
    /// ownership change (`local.go:616`).
    #[tokio::test]
    async fn read_only_claim_mounts_ro_and_is_not_chowned() {
        let base = tmp("ro");
        let src = tmp("ro-src");
        let (plug, fake) = plugin(&base);
        let dir = pod_volume_path(&base, TEST_POD_UID);
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o400)).unwrap();
        let gid = std::fs::metadata(&dir).unwrap().gid() as i64;
        let (v, p) = (claimed(), pv(src.to_str().unwrap(), false, &[]));
        let m = plug
            .new_mounter(&spec(&v, &p, true), &pod(TEST_POD_UID))
            .await
            .unwrap();
        assert!(m.get_attributes().read_only);
        m.set_up_with(&MounterArgs {
            fs_group: Some(gid),
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(
            fake.mount_points.lock().unwrap()[0].opts,
            vec!["bind".to_string(), "ro".to_string()]
        );
        assert_eq!(mode(&f), 0o400);
    }

    /// `local.go:616-624`: the first pod's mount applies the fsGroup, reading it
    /// from `MounterArgs` rather than the pod (#2900).
    #[tokio::test]
    async fn first_mount_applies_fs_group_from_mounter_args() {
        let base = tmp("fsgroup");
        let src = tmp("fsgroup-src");
        let (plug, _) = plugin(&base);
        let dir = pod_volume_path(&base, TEST_POD_UID);
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o400)).unwrap();
        let gid = std::fs::metadata(&dir).unwrap().gid() as i64;
        let (v, p) = (claimed(), pv(src.to_str().unwrap(), false, &[]));
        let m = plug
            .new_mounter(&spec(&v, &p, false), &pod(TEST_POD_UID))
            .await
            .unwrap();
        m.set_up_with(&MounterArgs {
            fs_group: Some(gid),
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(mode(&f), 0o660);
        assert_eq!(mode(&dir) & 0o2000, 0o2000);
    }

    /// `mounterArgs.FSGroupChangePolicy` is threaded through (`local.go:621`):
    /// OnRootMismatch with a matching root must not walk, while Always must.
    #[tokio::test]
    async fn fs_group_change_policy_is_honoured() {
        let base = tmp("policy");
        let src = tmp("policy-src");
        let dir = pod_volume_path(&base, TEST_POD_UID);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o2770)).unwrap();
        let f = dir.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        let gid = std::fs::metadata(&dir).unwrap().gid() as i64;
        let (v, p) = (claimed(), pv(src.to_str().unwrap(), false, &[]));
        for (policy, want, why) in [
            (
                "OnRootMismatch",
                0o600,
                "OnRootMismatch + matching root: no walk",
            ),
            ("Always", 0o660, "Always walks"),
        ] {
            // A fresh fake each round: the previous round's mount is not shared.
            let (plug, _) = plugin(&base);
            let m = plug
                .new_mounter(&spec(&v, &p, false), &pod(TEST_POD_UID))
                .await
                .unwrap();
            m.set_up_with(&MounterArgs {
                fs_group: Some(gid),
                fs_group_change_policy: Some(policy.into()),
                ..Default::default()
            })
            .await
            .unwrap();
            assert_eq!(mode(&f), want, "{why}");
        }
    }

    /// A second pod bind mounting a path already mounted by another pod must not
    /// re-own the volume (`local.go:619`: `len(refs) == 0`), and warns when the
    /// requested fsGroup differs from the volume's GID (`local.go:565-579`).
    #[tokio::test]
    async fn second_pod_does_not_chown_and_warns_on_fs_group_mismatch() {
        let base = tmp("shared");
        let src = tmp("shared-src");
        let (plug, fake) = plugin(&base);
        // The global path is itself a mount point of a device, as a real local
        // PV is; the pod mounts are bind mounts of it.
        fake.mount_points.lock().unwrap().push(MountPoint {
            device: "/dev/sdx".into(),
            path: src.to_string_lossy().into_owned(),
            fstype: "ext4".into(),
            opts: vec![],
        });
        let owner_gid = std::fs::metadata(&src).unwrap().gid() as i64;
        let (v, p) = (claimed(), pv(src.to_str().unwrap(), false, &[]));

        let storage = Arc::new(StorageBackend::new_memory());
        let recorder = rusternetes_kubelet::volume_plugins::plugin::MounterRecorder(
            EventRecorder::new(Arc::clone(&storage)),
        );

        // Pod A: first reference, takes ownership.
        let a = pod("pod-a");
        let ma = plug.new_mounter(&spec(&v, &p, false), &a).await.unwrap();
        let dir_a = pod_volume_path(&base, "pod-a");
        std::fs::create_dir_all(&dir_a).unwrap();
        ma.set_up_with(&MounterArgs {
            fs_group: Some(owner_gid),
            recorder: Some(recorder.clone()),
            ..Default::default()
        })
        .await
        .unwrap();

        // Pod B, same fsGroup: no warning.
        let b = pod("pod-b");
        let mb = plug.new_mounter(&spec(&v, &p, false), &b).await.unwrap();
        let dir_b = pod_volume_path(&base, "pod-b");
        std::fs::create_dir_all(&dir_b).unwrap();
        let f = dir_b.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o400)).unwrap();
        mb.set_up_with(&MounterArgs {
            fs_group: Some(owner_gid),
            recorder: Some(recorder.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(mode(&f), 0o400, "pod B is not the first reference");
        let key_b = format!(
            "/registry/events/default/{}",
            Event::generate_name(
                &rusternetes_kubelet::events::pod_object_reference(&b),
                WARN_ALREADY_MOUNTED_VOLUME
            )
        );
        assert!(
            storage.get::<Event>(&key_b).await.is_err(),
            "same GID: no warning"
        );

        // Pod C, different fsGroup: warning on the pod.
        let c = pod("pod-c");
        let mc = plug.new_mounter(&spec(&v, &p, false), &c).await.unwrap();
        mc.set_up_with(&MounterArgs {
            fs_group: Some(owner_gid + 1),
            recorder: Some(recorder),
            ..Default::default()
        })
        .await
        .unwrap();
        let key_c = format!(
            "/registry/events/default/{}",
            Event::generate_name(
                &rusternetes_kubelet::events::pod_object_reference(&c),
                WARN_ALREADY_MOUNTED_VOLUME
            )
        );
        let ev: Event = storage.get(&key_c).await.expect("warning event recorded");
        assert_eq!(ev.reason, WARN_ALREADY_MOUNTED_VOLUME);
        assert_eq!(ev.event_type, EventType::Warning);
        assert_eq!(
            ev.message,
            format!(
                "The requested fsGroup is {}, but the volume {TEST_PV_NAME} has GID {owner_gid}. The volume may not be shareable.",
                owner_gid + 1
            )
        );
    }

    /// A mount failure cleans up the pod dir and returns the mount error
    /// (`local.go:589-613`).
    #[tokio::test]
    async fn failed_mount_removes_the_pod_dir() {
        let base = tmp("fail");
        let src = tmp("fail-src");
        let (plug, fake) = plugin(&base);
        fake.mount_errs.lock().unwrap().push(Some("boom".into()));
        let (v, p) = (claimed(), pv(src.to_str().unwrap(), false, &[]));
        let m = plug
            .new_mounter(&spec(&v, &p, false), &pod(TEST_POD_UID))
            .await
            .unwrap();
        let err = m.set_up().await.unwrap_err();
        assert!(err.to_string().contains("boom"), "{err}");
        assert!(!pod_volume_path(&base, TEST_POD_UID).exists());
    }

    /// `TestFilterPodMounts` (`local_test.go:730`).
    #[tokio::test]
    async fn filter_pod_mounts_keeps_only_pods_dir_refs() {
        let base = tmp("filter");
        let (plug, _) = plugin(&base);
        let (v, p) = (claimed(), pv("/mnt/x", false, &[]));
        let _ = plug
            .new_mounter(&spec(&v, &p, false), &pod(TEST_POD_UID))
            .await
            .unwrap();
        let m = LocalMounter {
            volume: plug.new_local_volume(TEST_POD_UID, TEST_PV_NAME, "/mnt/x"),
            read_only: false,
            mount_options: vec![],
            metrics: MetricsStatFs::new("/mnt/x"),
        };
        let pods_dir = base.join("pods").to_string_lossy().into_owned();
        let s = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let join = |n: &str| format!("{pods_dir}/{n}");
        assert!(m.filter_pod_mounts(vec![]).is_empty());
        assert!(m.filter_pod_mounts(s(&["/mnt/outside"])).is_empty());
        assert_eq!(
            m.filter_pod_mounts(vec![join("pod-mount")]),
            vec![join("pod-mount")]
        );
        assert!(m
            .filter_pod_mounts(vec![format!("{pods_dir}pod-mount")])
            .is_empty());
        assert_eq!(
            m.filter_pod_mounts(vec![
                "/mnt/outside".into(),
                join("pod-mount"),
                "/another/outside".into(),
                join("pod-mount2"),
            ]),
            vec![join("pod-mount"), join("pod-mount2")]
        );
    }

    /// `TestFSGlobalPathAndMountDevice` (`local_test.go:303`): a directory PV's
    /// device mount path is the directory and `MountDevice` does nothing.
    #[tokio::test]
    async fn fs_global_path_and_mount_device() {
        let base = tmp("fs-global");
        let src = tmp("fs-global-src");
        let (plug, fake) = plugin(&base);
        assert!(plug.can_device_mount(&spec(&claimed(), &pv("/x", false, &[]), false)));
        let dm = plug.new_device_mounter().unwrap();
        let (v, p) = (claimed(), pv(src.to_str().unwrap(), false, &[]));
        let s = spec(&v, &p, false);
        let got = dm.get_device_mount_path(&s).unwrap();
        assert_eq!(got, src.to_string_lossy());
        dm.mount_device(&s, &got, &got, &DeviceMounterArgs::default())
            .await
            .unwrap();
        assert!(fake.mount_points.lock().unwrap().is_empty());
    }

    /// `TestBlockDeviceGlobalPathAndMountDevice` (`local_test.go:257`): a block
    /// device is mounted at `<plugin dir>/mounts/<pv>` and unmounted from it.
    #[tokio::test]
    async fn block_device_global_path_and_mount_device() {
        let base = tmp("blk-global");
        let dev = tmp("blk-global-dev");
        let dev_s = dev.to_string_lossy().into_owned();
        let fake = Arc::new(FakeMounter::new());
        let plug = LocalPlugin::new(host(&base))
            .with_mounter(fake.clone())
            .with_file_type(Arc::new(|_| Ok(FileType::BlockDev)));
        let dm = plug.new_device_mounter_internal(Arc::new(DisabledScripts));
        let (v, p) = (claimed(), pv(&dev_s, false, &[]));
        let s = spec(&v, &p, false);

        let expected = base
            .join("plugins/kubernetes.io/local-volume/mounts")
            .join(TEST_PV_NAME);
        let actual = dm.get_device_mount_path(&s).unwrap();
        assert_eq!(actual, expected.to_string_lossy());

        dm.mount_device(&s, &dev_s, &actual, &DeviceMounterArgs::default())
            .await
            .unwrap();
        assert!(expected.is_dir(), "device mount path not created");
        {
            let mps = fake.mount_points.lock().unwrap();
            assert_eq!(mps.len(), 1);
            assert_eq!(mps[0].device, dev_s);
            assert_eq!(mps[0].fstype, "ext4", "defaultFSType");
        }

        // `new_mounter` of the same spec hands the pod mount the global path.
        let m = plug.new_mounter(&s, &pod(TEST_POD_UID)).await.unwrap();
        m.set_up().await.unwrap();
        let last_device = fake
            .mount_points
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .device
            .clone();
        assert_eq!(last_device, dev_s, "bind mount resolves to the device");

        dm.unmount_device(&actual).await.unwrap();
        assert!(!expected.exists(), "UnmountDevice removes the mount dir");
    }

    /// `UnmountDevice` only touches paths under `<plugin dir>/mounts`
    /// (`local.go:474-483`): a directory PV's path is left alone.
    #[tokio::test]
    async fn unmount_device_ignores_directory_pvs() {
        let base = tmp("unmount-dev");
        let src = tmp("unmount-dev-src");
        let (plug, _) = plugin(&base);
        let du = plug.new_device_unmounter().unwrap();
        du.unmount_device(src.to_str().unwrap()).await.unwrap();
        assert!(src.exists());
    }

    /// `getGlobalLocalPath` (`local.go:270-292`): neither a directory nor a
    /// block device is an error.
    #[test]
    fn global_local_path_rejects_other_file_types() {
        let base = tmp("ftype");
        let plug = LocalPlugin::new(host(&base)).with_file_type(Arc::new(|_| Ok(FileType::File)));
        let (v, p) = (claimed(), pv("/some/file", false, &[]));
        let err = plug
            .get_global_local_path(&spec(&v, &p, false))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "only directory and block device are supported"
        );
        let (v, p) = (claimed(), pv("", false, &[]));
        assert_eq!(
            plug.get_global_local_path(&spec(&v, &p, false))
                .unwrap_err()
                .to_string(),
            "local volume source is nil or local path is not set"
        );
    }

    /// `TestConstructVolumeSpec` (`local_test.go:506`).
    #[test]
    fn construct_volume_spec() {
        for (name, mps, expected_path) in [
            (
                "filesystem volume with directory source",
                vec![(
                    "/mnt/disk/ssd0",
                    "pods/poduid/volumes/kubernetes.io~local-volume/pvA",
                )],
                "",
            ),
            (
                "filesystem volume with block source",
                vec![
                    (
                        "/dev/loop0",
                        "pods/poduid/volumes/kubernetes.io~local-volume/pvA",
                    ),
                    (
                        "/dev/loop0",
                        "plugins/kubernetes.io/local-volume/mounts/pvA",
                    ),
                ],
                "/dev/loop0",
            ),
        ] {
            let base = tmp("construct");
            let (plug, fake) = plugin(&base);
            *fake.mount_points.lock().unwrap() = mps
                .into_iter()
                .map(|(device, path)| MountPoint {
                    device: device.into(),
                    path: base.join(path).to_string_lossy().into_owned(),
                    fstype: String::new(),
                    opts: vec![],
                })
                .collect();
            let vol_path = pod_volume_path(&base, TEST_POD_UID);
            std::fs::create_dir_all(&vol_path).unwrap();
            let rec = plug
                .construct_volume_spec(TEST_PV_NAME, vol_path.to_str().unwrap())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(rec.volume.unwrap().name, TEST_PV_NAME, "{name}");
            let pv = rec.persistent_volume.expect(name);
            assert_eq!(pv.metadata.name, TEST_PV_NAME);
            assert_eq!(
                serde_json::to_value(&pv.spec.volume_mode).unwrap(),
                json!("Filesystem"),
                "{name}"
            );
            assert_eq!(pv.spec.local.expect(name).path, expected_path, "{name}");
        }
    }

    /// `TestConstructBlockVolumeSpec` (`local_test.go:600`).
    #[test]
    fn construct_block_volume_spec() {
        let base = tmp("construct-block");
        let (plug, _) = plugin(&base);
        let spec = plug
            .as_block_volume_plugin()
            .unwrap()
            .construct_block_volume_spec(
                TEST_POD_UID,
                TEST_PV_NAME,
                base.join("pods/poduid/volumeDevices/kubernetes.io~local-volume")
                    .to_str()
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(spec.name(), TEST_PV_NAME);
        let pv = spec.persistent_volume.expect("PersistentVolume");
        assert_eq!(
            serde_json::to_value(&pv.spec.volume_mode).unwrap(),
            json!("Block")
        );
        assert!(pv.spec.local.is_some());
    }

    /// `TestMapUnmap` (`local_test.go:413`): block map and unmap.
    #[tokio::test]
    async fn map_unmap() {
        let base = tmp("map-unmap");
        let dev = tmp("map-unmap-dev");
        let (plug, _) = plugin(&base);
        let (v, p) = (claimed(), pv(dev.to_str().unwrap(), true, &[]));
        let s = spec(&v, &p, false);
        let bp = plug.as_block_volume_plugin().expect("BlockVolumePlugin");
        let mapper = bp.new_block_volume_mapper(&s, &pod(TEST_POD_UID)).unwrap();

        let expected_global = base
            .join("plugins/kubernetes.io~local-volume/volumeDevices")
            .join(TEST_PV_NAME);
        assert_eq!(
            mapper.get_global_map_path(&s).unwrap(),
            expected_global.to_string_lossy()
        );
        let (pod_path, vol_name) = mapper.get_pod_device_map_path();
        assert_eq!(
            pod_path,
            base.join("pods/poduid/volumeDevices/kubernetes.io~local-volume")
                .to_string_lossy()
        );
        assert_eq!(vol_name, TEST_PV_NAME);
        assert!(mapper.supports_metrics());

        let custom = mapper
            .as_custom_block_volume_mapper()
            .expect("custom mapper");
        assert_eq!(custom.set_up_device().await.unwrap(), "");
        let dev_path = custom.map_pod_device().await.unwrap();
        assert_eq!(dev_path, dev.to_string_lossy());
        assert!(Path::new(&dev_path).exists());
        assert_eq!(custom.get_staging_path(), "");

        let unmapper = bp
            .new_block_volume_unmapper(TEST_PV_NAME, TEST_POD_UID)
            .unwrap();
        assert_eq!(
            unmapper.get_global_map_path(&s).unwrap(),
            expected_global.to_string_lossy()
        );
        assert!(!unmapper.supports_metrics());
        assert!(unmapper.as_custom_block_volume_unmapper().is_none());
    }

    /// `NewMounter` (`local.go:140`) embeds `volume.NewMetricsStatFS` of the pod
    /// volume dir: the mounter's metrics are the statfs of that directory. An
    /// absent directory is `FsInfoFailed`, like upstream's statfs error.
    #[tokio::test]
    async fn mounter_provides_statfs_metrics() {
        let base = tmp("metrics");
        let src = tmp("metrics-src");
        let (plug, _) = plugin(&base);
        let (v, p) = (claimed(), pv(src.to_str().unwrap(), false, &[]));
        let m = plug
            .new_local_mounter(&spec(&v, &p, false), &pod(TEST_POD_UID))
            .unwrap();

        let err = m.get_metrics().unwrap_err();
        assert!(err.to_string().starts_with("failed to get FsInfo"), "{err}");

        std::fs::create_dir_all(pod_volume_path(&base, TEST_POD_UID)).unwrap();
        let got = m.get_metrics().unwrap();
        assert!(got.capacity.unwrap().value() > 0);
        assert!(got.available.unwrap().value() > 0);
    }
}
