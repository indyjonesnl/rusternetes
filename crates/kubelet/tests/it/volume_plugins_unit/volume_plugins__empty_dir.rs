//! Unit tests moved from `src/volume_plugins/empty_dir.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::{EmptyDirVolumeSource, Pod};
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
    };
    pub use std::path::Path;
    pub use std::sync::Arc;
    pub use tracing::{info, warn};
}

mod tests {
    /// `assignQuota` (`empty_dir.go:292-316`): no size is a no-op, and with
    /// `LocalStorageCapacityIsolationFSQuotaMonitoring` off (or no user
    /// namespace) `SupportsQuotas` is false, so nothing is assigned.
    #[cfg(target_os = "linux")]
    #[test]
    fn assign_quota_is_noop_without_size_or_support() {
        use rusternetes_kubelet::fsquota::{FsQuota, RealMounter};
        let mk = |on: bool| {
            FsQuota::new(
                vec![],
                "/nonexistent/p".into(),
                "/nonexistent/q".into(),
                Box::new(move || on),
            )
        };
        let q = mk(true);
        assert!(assign_quota_with(&q, &RealMounter, "/tmp", "uid", None, true).is_ok());
        // Gate on, pod not in a user namespace: SupportsQuotas is false.
        assert!(assign_quota_with(&q, &RealMounter, "/tmp", "uid", Some(1024), false).is_ok());
        let off = mk(false);
        assert!(assign_quota_with(&off, &RealMounter, "/tmp", "uid", Some(1024), true).is_ok());
    }

    use super::__parent::*;
    use rusternetes_common::resources::Volume;
    use rusternetes_kubelet::volume_plugins::empty_dir::*;
    use serde_json::json;

    fn plugin() -> EmptyDirPlugin {
        EmptyDirPlugin::new(std::sync::Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    #[tokio::test]
    async fn set_up_applies_fs_group_with_rw_mask_not_owner_mirror() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let d = tmp("fsgroup-setup");
        std::fs::create_dir_all(&d).unwrap();
        let f = d.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o400)).unwrap();
        let gid = std::fs::metadata(&d).unwrap().gid() as i64;
        let m = EmptyDirMounter {
            pod: Pod::new("p", Default::default()),
            mounter: Arc::new(RealMounter),
            mount_detector: Arc::new(RealMountDetector),
            meta_dir: d.join("meta").to_string_lossy().into_owned(),
            path: d.to_string_lossy().into_owned(),
            volume_name: "v".into(),
            empty_dir: EmptyDirVolumeSource {
                medium: None,
                size_limit: None,
            },
        };
        m.set_up_with(&rusternetes_kubelet::volume_plugins::MounterArgs {
            fs_group: Some(gid),
            ..Default::default()
        })
        .await
        .unwrap();
        // volume_linux.go:147-181: mode | rwMask. 0400|0660 = 0660 (the old
        // owner->group mirror would have left 0440), and a dir
        // gets setgid|execMask.
        assert_eq!(
            std::fs::metadata(&f).unwrap().permissions().mode() & 0o7777,
            0o660
        );
        assert_eq!(
            std::fs::metadata(&d).unwrap().permissions().mode() & 0o7777,
            0o2777
        );
    }

    #[test]
    fn supports_an_inline_empty_dir() {
        let v: Volume = serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v: Volume =
            serde_json::from_value(json!({"name": "cfg", "configMap": {"name": "x"}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(
            plugin().name(),
            rusternetes_kubelet::pod_dirs::plugin::EMPTY_DIR
        );
    }

    fn tmp(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("emptydir-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn unknown_medium_is_an_error() {
        let e = classify_medium(Some("Bogus")).unwrap_err().to_string();
        assert_eq!(e, "unknown storage medium \"Bogus\"");
        assert_eq!(classify_medium(None).unwrap(), Medium::Default);
        assert_eq!(classify_medium(Some("")).unwrap(), Medium::Default);
        assert_eq!(classify_medium(Some("Memory")).unwrap(), Medium::Memory);
        assert_eq!(
            classify_medium(Some("HugePages-2Mi")).unwrap(),
            Medium::HugePages
        );
    }

    #[test]
    fn setup_dir_returns_mkdir_errors() {
        let root = tmp("mkdirerr");
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("file");
        std::fs::write(&file, "x").unwrap();
        let target = file.join("vol");
        assert!(setup_dir(target.to_str().unwrap()).is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn setup_dir_fixes_mode_to_0777() {
        use std::os::unix::fs::PermissionsExt;
        let d = tmp("mode");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o700)).unwrap();
        setup_dir(d.to_str().unwrap()).unwrap();
        let m = std::fs::metadata(&d).unwrap().permissions().mode() & 0o777;
        assert_eq!(m, 0o777);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// `setupTmpfs` (`empty_dir.go:324-362`) returns the error from
    /// `MountSensitiveWithoutSystemd`; a failed tmpfs mount fails SetUp
    /// instead of silently degrading to a persistent directory. The mount
    /// point does not exist, so `mount(8)` fails whether or not we are root.
    #[test]
    fn tmpfs_mount_failure_is_returned() {
        let r = RealMounter.mount_sensitive_without_systemd(
            "tmpfs",
            "/nonexistent/emptydir/mountpoint",
            "tmpfs",
            &["mode=0777".to_string()],
        );
        assert!(r.is_err());
    }

    // ---- TearDownAt (`doTestPlugin`/`testTearDown`, empty_dir_test.go:146-300) ----

    /// `fakeMountDetector` (`empty_dir_test.go:76-83`).
    struct FakeMountDetector {
        medium: Medium,
        is_mount: bool,
    }

    impl MountDetector for FakeMountDetector {
        fn get_mount_medium(&self, _path: &str, _m: &str) -> Result<(Medium, bool, Option<u64>)> {
            Ok((self.medium, self.is_mount, None))
        }
    }

    struct FailingDetector;

    impl MountDetector for FailingDetector {
        fn get_mount_medium(&self, path: &str, _m: &str) -> Result<(Medium, bool, Option<u64>)> {
            Err(anyhow!("statfs({path:?}): boom"))
        }
    }

    /// `mount.NewFakeMounter`'s log, reduced to the unmounts.
    #[derive(Default)]
    struct FakeMounter {
        unmounted: std::sync::Mutex<Vec<String>>,
        fail: bool,
    }

    impl Unmount for FakeMounter {
        fn unmount(&self, target: &str) -> Result<()> {
            if self.fail {
                return Err(anyhow!("unmount failed"));
            }
            self.unmounted.lock().unwrap().push(target.to_string());
            Ok(())
        }
    }

    /// A pod volume dir holding a file, under a fresh root.
    fn filled(tag: &str) -> (String, std::path::PathBuf) {
        let root = tmp(tag);
        std::fs::create_dir_all(&root).unwrap();
        let root = root.to_string_lossy().into_owned();
        let dir = rusternetes_kubelet::pod_dirs::get_pod_volume_dir(
            &root,
            "poduid",
            plugin().name(),
            "scratch",
        );
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("nested/file"), "data").unwrap();
        (root, dir)
    }

    fn unmounter_for(
        root: &str,
        detector: Arc<dyn MountDetector>,
        mounter: Arc<dyn Unmount>,
    ) -> EmptyDirUnmounter {
        EmptyDirPlugin::new(Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                root.to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
        .new_unmounter_internal("scratch", "poduid", mounter, detector)
    }

    /// `testTearDown`: `TearDown()` removes the volume path, whatever it held
    /// (`teardownDefault`, `os.RemoveAll(dir)`), and `GetPath` is
    /// `<pod>/volumes/kubernetes.io~empty-dir/<name>`.
    #[tokio::test]
    async fn tear_down_removes_a_default_medium_volume_and_its_contents() {
        let (root, dir) = filled("td-default");
        let mounter = Arc::new(FakeMounter::default());
        let u = unmounter_for(
            &root,
            Arc::new(FakeMountDetector {
                medium: Medium::Default,
                is_mount: false,
            }),
            mounter.clone(),
        );
        assert_eq!(u.get_path(), dir.to_string_lossy());

        u.tear_down().await.unwrap();

        assert!(!dir.exists(), "TearDown() failed, volume path still exists");
        assert!(mounter.unmounted.lock().unwrap().is_empty());
    }

    /// `teardownTmpfsOrHugetlbfs` (`empty_dir.go:544-555`): a tmpfs that is a
    /// mount point is unmounted exactly once, then removed
    /// (`expectedTeardownMounts: 1`, `FakeActionUnmount`).
    #[tokio::test]
    async fn tear_down_unmounts_a_tmpfs_mount_point_then_removes_it() {
        let (root, dir) = filled("td-tmpfs");
        let mounter = Arc::new(FakeMounter::default());
        let u = unmounter_for(
            &root,
            Arc::new(FakeMountDetector {
                medium: Medium::Memory,
                is_mount: true,
            }),
            mounter.clone(),
        );

        u.tear_down().await.unwrap();

        assert_eq!(
            *mounter.unmounted.lock().unwrap(),
            vec![dir.to_string_lossy().into_owned()]
        );
        assert!(!dir.exists());
    }

    /// `TestPluginHugetlbfs` (`empty_dir_test.go:112-133`): hugetlbfs is torn
    /// down the same way.
    #[tokio::test]
    async fn tear_down_unmounts_a_hugetlbfs_mount_point() {
        let (root, dir) = filled("td-huge");
        let mounter = Arc::new(FakeMounter::default());
        let u = unmounter_for(
            &root,
            Arc::new(FakeMountDetector {
                medium: Medium::HugePages,
                is_mount: true,
            }),
            mounter.clone(),
        );

        u.tear_down().await.unwrap();

        assert_eq!(mounter.unmounted.lock().unwrap().len(), 1);
        assert!(!dir.exists());
    }

    /// `if isMnt { ... }` (`empty_dir.go:514`): a tmpfs *path* that is not the
    /// mount root (a directory inside someone else's tmpfs) is only removed.
    #[tokio::test]
    async fn tear_down_does_not_unmount_a_tmpfs_that_is_not_a_mount_point() {
        let (root, dir) = filled("td-tmpfs-nomnt");
        let mounter = Arc::new(FakeMounter::default());
        let u = unmounter_for(
            &root,
            Arc::new(FakeMountDetector {
                medium: Medium::Memory,
                is_mount: false,
            }),
            mounter.clone(),
        );

        u.tear_down().await.unwrap();

        assert!(mounter.unmounted.lock().unwrap().is_empty());
        assert!(!dir.exists());
    }

    /// `empty_dir.go:502-507`: nothing to do, and no error, for a path that is
    /// already gone (`volumeDirExists: false`).
    #[tokio::test]
    async fn tear_down_of_a_missing_path_is_ok_and_does_not_unmount() {
        let root = tmp("td-missing").to_string_lossy().into_owned();
        let mounter = Arc::new(FakeMounter::default());
        let u = unmounter_for(
            &root,
            Arc::new(FakeMountDetector {
                medium: Medium::Memory,
                is_mount: true,
            }),
            mounter.clone(),
        );

        u.tear_down().await.unwrap();

        assert!(mounter.unmounted.lock().unwrap().is_empty());
    }

    /// `empty_dir.go:548-550`: a failed unmount is returned and the directory
    /// is NOT removed — deleting the contents of a still-mounted tmpfs is
    /// exactly what the unmount-first order exists to avoid.
    #[tokio::test]
    async fn a_failed_unmount_is_returned_and_keeps_the_directory() {
        let (root, dir) = filled("td-unmount-fail");
        let mounter = Arc::new(FakeMounter {
            fail: true,
            ..Default::default()
        });
        let u = unmounter_for(
            &root,
            Arc::new(FakeMountDetector {
                medium: Medium::Memory,
                is_mount: true,
            }),
            mounter,
        );

        assert!(u.tear_down().await.is_err());
        assert!(dir.join("nested/file").exists());
    }

    /// `empty_dir.go:510-513`: a medium that cannot be determined is an error,
    /// not "assume default" — the default arm would `RemoveAll` a tmpfs.
    #[tokio::test]
    async fn a_mount_detector_error_is_returned_and_keeps_the_directory() {
        let (root, dir) = filled("td-detect-fail");
        let u = unmounter_for(
            &root,
            Arc::new(FailingDetector),
            Arc::new(FakeMounter::default()),
        );

        let err = u.tear_down().await.unwrap_err().to_string();

        assert!(err.contains("boom"), "{err}");
        assert!(dir.join("nested/file").exists());
    }

    /// The real detector on a plain directory: default medium, not a mount.
    #[test]
    fn the_real_detector_reports_a_plain_directory_as_default() {
        let d = tmp("real-detector");
        std::fs::create_dir_all(&d).unwrap();
        let (medium, is_mnt, _) = RealMountDetector
            .get_mount_medium(d.to_str().unwrap(), "")
            .unwrap();
        // /tmp may itself be a tmpfs; either way a subdirectory is no mount point.
        assert!(matches!(medium, Medium::Default | Medium::Memory));
        assert!(!is_mnt);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// `/dev/shm` is a tmpfs mount point: `statfs` magic `0x01021994` and a
    /// different device from `/dev`. Skipped where it is not mounted.
    #[test]
    fn the_real_detector_reports_dev_shm_as_a_tmpfs_mount_point() {
        let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
        if !mounts.lines().any(|l| {
            let f: Vec<_> = l.split_whitespace().collect();
            f.get(1) == Some(&"/dev/shm") && f.get(2) == Some(&"tmpfs")
        }) {
            return;
        }
        let (medium, is_mnt, _) = RealMountDetector.get_mount_medium("/dev/shm", "").unwrap();
        assert_eq!(medium, Medium::Memory);
        assert!(is_mnt);
    }

    /// `ConstructVolumeSpec` (`empty_dir.go:185-195`).
    #[test]
    fn construct_volume_spec_is_a_bare_empty_dir_named_after_the_volume() {
        let r = plugin()
            .construct_volume_spec("scratch", "/ignored")
            .unwrap();
        assert_eq!(r.volume.as_ref().unwrap().name, "scratch");
        assert!(r.volume.as_ref().unwrap().empty_dir.is_some());
        assert!(r.persistent_volume.is_none());
    }

    /// The trait entry point builds a real unmounter over the host's path.
    #[test]
    fn new_unmounter_points_at_the_pod_volume_dir() {
        let u = plugin().new_unmounter("scratch", "uid-1").unwrap();
        assert_eq!(
            u.get_path(),
            "/var/lib/rusternetes/pods/uid-1/volumes/kubernetes.io~empty-dir/scratch"
        );
    }

    /// `emptyDir.GetAttributes` (`pkg/volume/emptydir/empty_dir.go:220-226`):
    /// writable, but managed and relabelled.
    #[test]
    fn attributes_are_writable_managed_relabel() {
        let m = EmptyDirMounter {
            pod: Pod::new("p", Default::default()),
            mounter: Arc::new(RealMounter),
            mount_detector: Arc::new(RealMountDetector),
            meta_dir: "/tmp/x-meta".into(),
            path: "/tmp/x".into(),
            volume_name: "v".into(),
            empty_dir: EmptyDirVolumeSource {
                medium: None,
                size_limit: None,
            },
        };
        assert_eq!(
            m.get_attributes(),
            rusternetes_kubelet::volume_plugins::plugin::Attributes {
                read_only: false,
                managed: true,
                selinux_relabel: true
            }
        );
    }

    // ---- ready file (`SetUpAt` / `TearDownAt`, empty_dir.go:234-286, 495-500) ----

    fn mounter_in(root: &str) -> EmptyDirMounter {
        let p = plugin_in(root);
        EmptyDirMounter {
            pod: Pod::new("p", Default::default()),
            mounter: Arc::new(RealMounter),
            mount_detector: Arc::new(RealMountDetector),
            path: p.host.get_pod_volume_dir("poduid", p.name(), "scratch"),
            meta_dir: p.get_meta_dir("poduid", "scratch"),
            volume_name: "scratch".into(),
            empty_dir: EmptyDirVolumeSource {
                medium: None,
                size_limit: None,
            },
        }
    }

    fn plugin_in(root: &str) -> EmptyDirPlugin {
        EmptyDirPlugin::new(Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                root.to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    /// `volumeutil.SetReady(ed.getMetaDir())` after a successful setup.
    #[tokio::test]
    async fn set_up_writes_the_ready_file_under_the_plugin_dir() {
        let root = tmp("ready-set");
        std::fs::create_dir_all(&root).unwrap();
        let root = root.to_string_lossy().into_owned();
        let m = mounter_in(&root);
        m.set_up_with(&Default::default()).await.unwrap();
        assert!(
            m.meta_dir
                .ends_with("pods/poduid/plugins/kubernetes.io~empty-dir/scratch"),
            "{}",
            m.meta_dir
        );
        assert!(std::path::Path::new(&m.meta_dir).join("ready").is_file());
    }

    /// Ready file present + default medium + dir present: setup is a no-op
    /// (`empty_dir.go:244-250`), so existing content and mode are untouched.
    #[tokio::test]
    async fn set_up_with_ready_file_and_existing_dir_returns_early() {
        let root = tmp("ready-early");
        std::fs::create_dir_all(&root).unwrap();
        let root = root.to_string_lossy().into_owned();
        let m = mounter_in(&root);
        std::fs::create_dir_all(&m.path).unwrap();
        rusternetes_kubelet::volume_plugins::util::set_ready(&m.meta_dir);
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&m.path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        m.set_up_with(&Default::default()).await.unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&m.path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "setupDir must not have run");
    }

    /// Ready file present but the volume dir deleted by hand: the ready dir is
    /// removed and setup runs again (`empty_dir.go:251-259`).
    #[tokio::test]
    async fn set_up_with_stale_ready_file_recreates_the_dir() {
        let root = tmp("ready-stale");
        std::fs::create_dir_all(&root).unwrap();
        let root = root.to_string_lossy().into_owned();
        let m = mounter_in(&root);
        rusternetes_kubelet::volume_plugins::util::set_ready(&m.meta_dir);
        assert!(!std::path::Path::new(&m.path).exists());
        m.set_up_with(&Default::default()).await.unwrap();
        assert!(std::path::Path::new(&m.path).is_dir());
        assert!(std::path::Path::new(&m.meta_dir).join("ready").is_file());
    }

    /// `TearDownAt` first removes the ready dir (`empty_dir.go:497-500`),
    /// even when the volume dir is already gone (`readyDirExists` cases).
    #[tokio::test]
    async fn tear_down_removes_the_ready_dir() {
        let (root, dir) = filled("td-ready");
        let m = mounter_in(&root);
        rusternetes_kubelet::volume_plugins::util::set_ready(&m.meta_dir);
        let u = unmounter_for(
            &root,
            Arc::new(FakeMountDetector {
                medium: Medium::Default,
                is_mount: false,
            }),
            Arc::new(FakeMounter::default()),
        );
        u.tear_down().await.unwrap();
        assert!(!dir.exists());
        assert!(!std::path::Path::new(&m.meta_dir).exists());

        // Volume dir already gone, ready dir still there.
        rusternetes_kubelet::volume_plugins::util::set_ready(&m.meta_dir);
        u.tear_down().await.unwrap();
        assert!(!std::path::Path::new(&m.meta_dir).exists());
    }

    // ---- hugepages (`TestGetHugePagesMountOptions`, `TestGetPageSize`,
    // `TestSetupHugepages`, empty_dir_test.go:377-976) ----

    use rusternetes_common::resources::{Container, PodSpec};
    use rusternetes_common::types::ResourceRequirements;

    fn reqs(r: &[(&str, &str)]) -> ResourceRequirements {
        ResourceRequirements {
            requests: Some(
                r.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            limits: None,
            claims: None,
        }
    }

    fn container(r: &[(&str, &str)]) -> Container {
        serde_json::from_value::<Container>(json!({"name": "c", "image": "i"}))
            .map(|mut c| {
                c.resources = Some(reqs(r));
                c
            })
            .unwrap()
    }

    fn pod_of(
        containers: &[&[(&str, &str)]],
        init: &[&[(&str, &str)]],
        pod_level: Option<&[(&str, &str)]>,
    ) -> Pod {
        let spec = PodSpec {
            containers: containers.iter().map(|r| container(r)).collect(),
            init_containers: if init.is_empty() {
                None
            } else {
                Some(init.iter().map(|r| container(r)).collect())
            },
            resources: pod_level.map(reqs),
            ..Default::default()
        };
        Pod::new("p", spec)
    }

    #[test]
    fn page_size_mount_option_matches_upstream_cases() {
        let h2 = ("hugepages-2Mi", "100Mi");
        let h1 = ("hugepages-1Gi", "2Gi");
        let ok =
            |m, p: Pod, want: &str| assert_eq!(get_page_size_mount_option(m, &p).unwrap(), want);
        let bad = |m, p: Pod| assert!(get_page_size_mount_option(m, &p).is_err(), "{m}");

        ok("HugePages", pod_of(&[&[h2]], &[], None), "pagesize=2Mi");
        // ProperValuesAndDifferentPageSize: same size in two containers.
        ok(
            "HugePages",
            pod_of(&[&[h1], &[("hugepages-1Gi", "4Gi")]], &[], None),
            "pagesize=1Gi",
        );
        ok(
            "HugePages",
            pod_of(&[], &[&[h1], &[("hugepages-1Gi", "4Gi")]], None),
            "pagesize=1Gi",
        );
        bad(
            "HugePages",
            pod_of(&[], &[&[h2], &[("hugepages-1Gi", "4Gi")]], None),
        );
        bad("HugePages", pod_of(&[&[h1], &[h2]], &[], None));
        bad("HugePages", pod_of(&[], &[], None));
        ok(
            "HugePages-1Gi",
            pod_of(&[&[h2, h1]], &[], None),
            "pagesize=1Gi",
        );
        ok(
            "HugePages-2Mi",
            pod_of(&[], &[&[h1, h2], &[h1, h2]], None),
            "pagesize=2Mi",
        );
        bad("HugePages", pod_of(&[&[h2, h1]], &[], None));
        bad("HugePages-", pod_of(&[&[h2, h1]], &[], None));
        bad("foo", pod_of(&[&[h2, h1]], &[], None));
        bad("HugePages-1Mi", pod_of(&[&[h2, h1]], &[], None));
        // PodLevelResources.
        ok("HugePages", pod_of(&[], &[], Some(&[h2])), "pagesize=2Mi");
        ok(
            "HugePages-2Mi",
            pod_of(&[], &[], Some(&[h2])),
            "pagesize=2Mi",
        );
        bad("HugePages", pod_of(&[], &[], Some(&[h1, h2])));
        ok(
            "HugePages-2Mi",
            pod_of(&[], &[], Some(&[h1, h2])),
            "pagesize=2Mi",
        );
        bad("HugePages", pod_of(&[&[h2]], &[], Some(&[h1])));
        ok(
            "HugePages-2Mi",
            pod_of(&[&[h2]], &[], Some(&[h1])),
            "pagesize=2Mi",
        );
    }

    #[test]
    fn get_page_size_reads_pagesize_option_of_the_mount() {
        // `mount.FakeMounter` mount points, as mountinfo lines (options are
        // the per-mount options then the super options).
        let info = "\
25 1 8:2 / / rw,relatime - ext4 /dev/sda2 rw,errors=remount-ro
40 25 0:30 / /mnt/hugepages-2Mi rw,relatime - hugetlbfs /dev/hugepages rw,pagesize=2M
42 25 0:31 / /sys rw,nosuid - sysfs sysfs rw
41 25 0:32 / /mnt/hugepages-1Gi rw,relatime - hugetlbfs /dev/hugepages rw,pagesize=1024M
43 25 0:33 / /mnt/noopt rw,relatime - hugetlbfs /dev/hugepages rw
44 25 0:34 / /mnt/badopt rw,relatime - hugetlbfs /dev/hugepages rw,pagesize=NN
45 25 0:35 / /mnt/mi rw shared:1 - hugetlbfs /dev/hugepages rw,pagesize=2Mi
";
        assert_eq!(get_page_size(info, "/mnt/hugepages-2Mi").unwrap(), 2 << 20);
        assert_eq!(get_page_size(info, "/mnt/hugepages-1Gi").unwrap(), 1 << 30);
        assert_eq!(get_page_size(info, "/mnt/mi").unwrap(), 2 << 20);
        assert!(get_page_size(info, "/mnt/nomp").is_err());
        assert!(get_page_size(info, "/mnt/noopt").is_err());
        assert!(get_page_size(info, "/mnt/badopt").is_err());
    }

    struct HugeDetector {
        page_size: Option<u64>,
        is_mnt: bool,
        err: bool,
    }

    impl MountDetector for HugeDetector {
        fn get_mount_medium(&self, _path: &str, _m: &str) -> Result<(Medium, bool, Option<u64>)> {
            if self.err {
                return Err(anyhow!("GetMountMedium error"));
            }
            Ok((Medium::HugePages, self.is_mnt, self.page_size))
        }
    }

    fn huge_mounter(
        medium: &str,
        pod: Pod,
        detector: HugeDetector,
    ) -> (EmptyDirMounter, Arc<RecordingMount>) {
        let rec = Arc::new(RecordingMount::default());
        let m = EmptyDirMounter {
            pod,
            mounter: rec.clone(),
            mount_detector: Arc::new(detector),
            path: String::new(),
            meta_dir: String::new(),
            volume_name: "v".into(),
            empty_dir: EmptyDirVolumeSource {
                medium: Some(medium.into()),
                size_limit: None,
            },
        };
        (m, rec)
    }

    #[test]
    fn setup_hugepages_matches_upstream_cases() {
        let dir = tmp("setup-hugepages");
        let d = dir.to_str().unwrap();
        let h2 = ("hugepages-2Mi", "100Mi");
        let det = |page_size, is_mnt, err| HugeDetector {
            page_size,
            is_mnt,
            err,
        };

        // Valid: mount expected, with `nodev`/`hugetlbfs`/`pagesize=2Mi`.
        let (m, rec) = huge_mounter(
            "HugePages",
            pod_of(&[&[h2]], &[], None),
            det(Some(0), false, false),
        );
        m.setup_hugepages(d).unwrap();
        assert_eq!(
            *rec.calls.lock().unwrap(),
            vec![(
                "nodev".into(),
                d.into(),
                "hugetlbfs".into(),
                vec!["pagesize=2Mi".into()]
            )]
        );

        // Valid: already mounted with correct pagesize / already mounted.
        let (m, rec) = huge_mounter(
            "HugePages-2Mi",
            pod_of(&[&[h2]], &[], None),
            det(Some(2 << 20), true, false),
        );
        m.setup_hugepages(d).unwrap();
        let (m2, _) = huge_mounter(
            "HugePages",
            pod_of(&[&[h2]], &[], None),
            det(None, true, false),
        );
        m2.setup_hugepages(d).unwrap();
        assert!(rec.calls.lock().unwrap().is_empty());

        // Invalid: GetMountMedium error; medium and page size differ; bad
        // medium; pagesize undetermined for a sized medium.
        let (m, _) = huge_mounter(
            "HugePages-2Mi",
            pod_of(&[&[h2]], &[], None),
            det(Some(2 << 20), true, true),
        );
        assert!(m.setup_hugepages(d).is_err());
        let (m, _) = huge_mounter(
            "HugePages-1Gi",
            pod_of(&[&[("hugepages-1Gi", "2Gi")]], &[], None),
            det(Some(2 << 20), true, false),
        );
        assert!(m
            .setup_hugepages(d)
            .unwrap_err()
            .to_string()
            .contains("differ"));
        let (m, _) = huge_mounter(
            "HugePages-NN",
            pod_of(&[&[h2]], &[], None),
            det(Some(0), false, false),
        );
        assert!(m.setup_hugepages(d).is_err());
        let (m, _) = huge_mounter(
            "HugePages-2Mi",
            pod_of(&[&[h2]], &[], None),
            det(None, true, false),
        );
        assert!(m.setup_hugepages(d).is_err());

        // Invalid: setupDir fails.
        let (m, _) = huge_mounter(
            "HugePages",
            pod_of(&[&[h2]], &[], None),
            det(None, false, false),
        );
        assert!(m.setup_hugepages("").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn quantity_string_is_canonical_binary_si() {
        assert_eq!(quantity_string(2 << 20), "2Mi");
        assert_eq!(quantity_string(1 << 30), "1Gi");
        assert_eq!(quantity_string(2048 << 10), "2Mi");
        assert_eq!(quantity_string(1000), "1000");
    }
}
