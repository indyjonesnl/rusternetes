//! Unit tests moved from `src/fsquota/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use rusternetes_kubelet::fsquota::common::{
        mount_parse, LinuxVolumeQuotaApplier, LinuxVolumeQuotaProvider, QuotaID, VolumeProvider,
        BAD_QUOTA_ID, MOUNTS_FILE,
    };
    pub use std::collections::HashMap;
    pub use std::path::PathBuf;
    pub use std::sync::{Arc, Mutex, OnceLock};
    pub use tracing::{debug, info, warn};
}

mod tests {
    //! Port of `pkg/volume/util/fsquota/quota_linux_test.go`.

    use super::__parent::*;
    use rusternetes_kubelet::fsquota::common::{
        LinuxVolumeQuotaApplier, LinuxVolumeQuotaProvider, QuotaID, BAD_QUOTA_ID,
    };
    use rusternetes_kubelet::fsquota::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    const DUMMY_MOUNT_DATA: &str = "sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0
proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0
devtmpfs /dev devtmpfs rw,nosuid,size=6133536k,nr_inodes=1533384,mode=755 0 0
tmpfs /tmp tmpfs rw,nosuid,nodev 0 0
/dev/sda1 /boot ext4 rw,relatime 0 0
/dev/mapper/fedora-root / ext4 rw,noatime 0 0
/dev/mapper/fedora-home /home ext4 rw,noatime 0 0
/dev/sdb1 /virt xfs rw,noatime,attr2,inode64,usrquota,prjquota 0 0
";

    /// `mount.NewFakeMounter(...)`'s `IsLikelyNotMountPoint`: stat the path, then
    /// check the mount list.
    struct FakeMounter(Vec<(&'static str, &'static str)>); // (device, path)

    impl MountInterface for FakeMounter {
        fn is_likely_not_mount_point(&self, path: &str) -> std::io::Result<bool> {
            std::fs::metadata(path)?;
            let resolved = std::fs::canonicalize(path)?;
            let resolved = resolved.to_string_lossy();
            Ok(!self.0.iter().any(|(_, p)| *p == resolved))
        }
    }

    fn dummy_fake_mount1() -> FakeMounter {
        FakeMounter(vec![
            ("tmpfs", "/tmp"),
            ("/dev/sda1", "/boot"),
            ("/dev/mapper/fedora-root", "/"),
            ("/dev/mapper/fedora-home", "/home"),
            ("/dev/sdb1", "/mnt/virt"),
        ])
    }

    fn backing_dev1(path: &str, mountdata: &str) -> Result<String> {
        let dir = tempfile::tempdir()?;
        let f = dir.path().join("backingdev");
        std::fs::write(&f, mountdata)?;
        detect_backing_dev_internal(path, f.to_str().unwrap())
    }

    #[test]
    fn test_backing_dev() {
        // (name, path, expected, expect_failure)
        let cases = [
            ("Root", "/", "/dev/mapper/fedora-root", false),
            ("tmpfs", "/tmp", "tmpfs", false),
            ("user filesystem", "/virt", "/dev/sdb1", false),
            ("empty mountpoint", "", "", true),
            ("bad mountpoint", "/kiusf", "", true),
        ];
        for (name, path, expected, expect_failure) in cases {
            match backing_dev1(path, DUMMY_MOUNT_DATA) {
                Err(_) if expect_failure => {}
                Err(e) => panic!("{name} failed: {e}"),
                Ok(d) if expect_failure => {
                    panic!("{name}: path {path} expected to fail; succeeded and got {d}")
                }
                Ok(d) => assert_eq!(d, expected, "{name}"),
            }
        }
    }

    #[test]
    fn test_detect_mount_point() {
        let cases = [
            ("Root", "/", "/"),
            ("(empty)", "", "/"),
            ("(invalid)", "", "/"),
            ("/usr", "/usr", "/"),
            ("/var/tmp", "/var/tmp", "/"),
        ];
        for (name, path, expected) in cases {
            let mp = detect_mountpoint_internal(&dummy_fake_mount1(), path)
                .unwrap_or_else(|e| panic!("Case {name} failed: {e}"));
            assert_eq!(mp, expected, "Case {name}");
        }
    }

    // (device, path)
    const DUMMY_MOUNT_POINTS: [(&str, &str); 4] = [
        ("/dev/sda2", "/quota1"),
        ("/dev/sda3", "/quota2"),
        ("/dev/sda3", "/noquota"),
        ("/dev/sda1", "/"),
    ];

    fn dummy_quota_test() -> FakeMounter {
        FakeMounter(DUMMY_MOUNT_POINTS.to_vec())
    }

    /// The mock quota applier's bookkeeping (`testIDQuotaMap`/`testQuotaIDMap`).
    #[derive(Default)]
    struct TestQuotaState {
        id_quota: HashMap<QuotaID, String>,
        quota_id: HashMap<String, QuotaID>,
    }

    #[derive(Clone, Default)]
    struct TestVolumeQuota(Arc<Mutex<TestQuotaState>>);

    impl LinuxVolumeQuotaApplier for TestVolumeQuota {
        fn set_quota_on_dir(&self, dir: &str, id: QuotaID, _bytes: i64) -> Result<()> {
            let mut s = self.0.lock().unwrap();
            if let Some(odir) = s.id_quota.get(&id) {
                if dir != odir {
                    return Err(anyhow!("ID {id} is already in use"));
                }
            }
            if let Some(oid) = s.quota_id.get(dir) {
                if id != *oid {
                    return Err(anyhow!("directory {dir} already has a quota applied"));
                }
            }
            s.quota_id.insert(dir.to_string(), id);
            s.id_quota.insert(id, dir.to_string());
            Ok(())
        }

        fn get_quota_on_dir(&self, path: &str) -> Result<QuotaID> {
            match self.0.lock().unwrap().quota_id.get(path) {
                Some(id) => Ok(*id),
                None => Err(anyhow!("no quota available for {path}")),
            }
        }

        fn quota_id_is_in_use(&self, id: QuotaID) -> Result<bool> {
            Ok(self.0.lock().unwrap().id_quota.contains_key(&id))
        }

        fn get_consumption(&self, _path: &str, _id: QuotaID) -> Result<i64> {
            Ok(4096)
        }

        fn get_inodes(&self, _path: &str, _id: QuotaID) -> Result<i64> {
            Ok(1)
        }
    }

    /// `VolumeProvider1`/`VolumeProvider2`.
    struct PrefixProvider(&'static str, TestVolumeQuota);

    impl LinuxVolumeQuotaProvider for PrefixProvider {
        fn get_quota_applier(
            &self,
            mountpoint: &str,
            _backing_dev: &str,
        ) -> Option<Box<dyn LinuxVolumeQuotaApplier>> {
            mountpoint
                .starts_with(self.0)
                .then(|| Box::new(self.1.clone()) as Box<dyn LinuxVolumeQuotaApplier>)
        }
    }

    struct Harness {
        q: FsQuota,
        enabled: Arc<AtomicBool>,
        _dir: tempfile::TempDir,
    }

    impl Harness {
        fn new(enabled: bool) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let projects = dir.path().join("projects");
            let projid = dir.path().join("projid");
            std::fs::write(&projects, PROJECTS_HEADER).unwrap();
            std::fs::write(&projid, PROJID_HEADER).unwrap();
            let state = TestVolumeQuota::default();
            let flag = Arc::new(AtomicBool::new(enabled));
            let f2 = flag.clone();
            let q = FsQuota::new(
                vec![
                    Box::new(PrefixProvider("/quota1", state.clone())),
                    Box::new(PrefixProvider("/quota2", state)),
                ],
                projects,
                projid,
                Box::new(move || f2.load(Ordering::SeqCst)),
            );
            Harness {
                q,
                enabled: flag,
                _dir: dir,
            }
        }

        /// `dummySetFSInfo`.
        fn set_fs_info(&self, path: &str) {
            if self.enabled.load(Ordering::SeqCst) {
                for (dev, mp) in DUMMY_MOUNT_POINTS {
                    if path.starts_with(mp) {
                        let mut fs = self.q.fs.lock().unwrap();
                        fs.mountpoint_map.insert(path.to_string(), mp.to_string());
                        fs.backing_dev_map.insert(path.to_string(), dev.to_string());
                        return;
                    }
                }
            }
        }

        fn fake_supports_quotas(&self, path: &str, userns: bool) -> Result<bool> {
            self.set_fs_info(path);
            self.q.supports_quotas(&dummy_quota_test(), path, userns)
        }

        fn fake_assign_quota(
            &self,
            path: &str,
            poduid: &str,
            bytes: i64,
            userns: bool,
        ) -> Result<()> {
            self.set_fs_info(path);
            self.q
                .assign_quota(&dummy_quota_test(), path, poduid, Some(bytes), userns)
        }

        fn fake_clear_quota(&self, path: &str, userns: bool) -> Result<()> {
            self.set_fs_info(path);
            self.q.clear_quota(&dummy_quota_test(), path, userns)
        }

        fn projects_file(&self) -> std::path::PathBuf {
            self.q.projects_file.clone()
        }

        fn projid_file(&self) -> std::path::PathBuf {
            self.q.projid_file.clone()
        }
    }

    struct QuotaTestCase {
        name: &'static str,
        path: &'static str,
        poduid: &'static str,
        bytes: i64,
        op: &'static str,
        expected_projects: &'static str,
        expected_projid: &'static str,
        supports_quota: bool,
        expects_set_quota: bool,
        user_namespaces_enabled: bool,
        /// pod_quota, dir_quota, quota_pod, dir_pod, dev_applier, dir_applier,
        /// pod_dir_count, quota_size, supports_quotas, backing_dev, mountpoint.
        delta: [i32; 11],
    }

    const PROJECTS_HEADER: &str = "# This is a /etc/projects header\n1048578:/quota/d\n";
    const PROJECTS1: &str = "1048577:/quota1/a\n";
    const PROJECTS2: &str = "1048577:/quota1/a\n1048580:/quota1/b\n";
    const PROJECTS3: &str = "1048577:/quota1/a\n1048580:/quota1/b\n1048581:/quota2/b\n";
    const PROJECTS4: &str = "1048577:/quota1/a\n1048581:/quota2/b\n";
    const PROJECTS5: &str = "1048581:/quota2/b\n";

    const PROJID_HEADER: &str = "# This is a /etc/projid header\nxxxxxx:1048579\n";
    const PROJID1: &str = "volume1048577:1048577\n";
    const PROJID2: &str = "volume1048577:1048577\nvolume1048580:1048580\n";
    const PROJID3: &str = "volume1048577:1048577\nvolume1048580:1048580\nvolume1048581:1048581\n";
    const PROJID4: &str = "volume1048577:1048577\nvolume1048581:1048581\n";
    const PROJID5: &str = "volume1048581:1048581\n";

    #[allow(clippy::too_many_arguments)]
    const fn tc(
        name: &'static str,
        path: &'static str,
        poduid: &'static str,
        bytes: i64,
        op: &'static str,
        expected_projects: &'static str,
        expected_projid: &'static str,
        supports_quota: bool,
        expects_set_quota: bool,
        user_namespaces_enabled: bool,
        delta: [i32; 11],
    ) -> QuotaTestCase {
        QuotaTestCase {
            name,
            path,
            poduid,
            bytes,
            op,
            expected_projects,
            expected_projid,
            supports_quota,
            expects_set_quota,
            user_namespaces_enabled,
            delta,
        }
    }

    fn quota_test_cases() -> Vec<QuotaTestCase> {
        vec![
            tc(
                "SupportsQuotaOnQuotaVolumeWithUserNamespace",
                "/quota1/a",
                "",
                1024,
                "Supports",
                "",
                "",
                true,
                true,
                true,
                [0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1],
            ),
            tc(
                "SupportsQuotaOnQuotaVolumeWithoutUserNamespace",
                "/quota1/a",
                "",
                1024,
                "Supports",
                "",
                "",
                true,
                true,
                false,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            tc(
                "AssignQuotaFirstTime",
                "/quota1/a",
                "",
                1024,
                "Set",
                PROJECTS1,
                PROJID1,
                true,
                true,
                true,
                [1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0],
            ),
            tc(
                "AssignQuotaFirstTime",
                "/quota1/b",
                "x",
                1024,
                "Set",
                PROJECTS2,
                PROJID2,
                true,
                true,
                true,
                [1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1],
            ),
            tc(
                "AssignQuotaFirstTime",
                "/quota2/b",
                "x",
                1024,
                "Set",
                PROJECTS3,
                PROJID3,
                true,
                true,
                true,
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ),
            tc(
                "AssignQuotaSecondTimeWithSameSize",
                "/quota1/b",
                "x",
                1024,
                "Set",
                PROJECTS3,
                PROJID3,
                true,
                true,
                true,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            tc(
                "AssignQuotaSecondTimeWithDifferentSize",
                "/quota2/b",
                "x",
                2048,
                "Set",
                PROJECTS3,
                PROJID3,
                true,
                false,
                true,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            tc(
                "ClearQuotaFirstTime",
                "/quota1/b",
                "",
                1024,
                "Clear",
                PROJECTS4,
                PROJID4,
                true,
                true,
                true,
                [-1, -1, -1, -1, 0, -1, -1, -1, -1, -1, -1],
            ),
            tc(
                "SupportsQuotaOnNonQuotaVolume",
                "/noquota/a",
                "",
                1024,
                "Supports",
                PROJECTS4,
                PROJID4,
                false,
                false,
                true,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            tc(
                "ClearQuotaFirstTime",
                "/quota1/a",
                "",
                1024,
                "Clear",
                PROJECTS5,
                PROJID5,
                true,
                true,
                true,
                [-1, -1, -1, -1, 0, -1, -1, -1, -1, -1, -1],
            ),
            tc(
                "ClearQuotaSecondTime",
                "/quota1/a",
                "",
                1024,
                "Clear",
                PROJECTS5,
                PROJID5,
                true,
                false,
                true,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            tc(
                "ClearQuotaFirstTime",
                "/quota2/b",
                "",
                1024,
                "Clear",
                "",
                "",
                true,
                true,
                true,
                [-1, -1, -1, -1, 0, -1, -1, -1, -1, -1, -1],
            ),
        ]
    }

    fn compare_projects_files(h: &Harness, tcase: &QuotaTestCase, enabled: bool) {
        let mut p = PROJECTS_HEADER.to_string();
        if enabled {
            p += tcase.expected_projects;
        }
        let s = std::fs::read_to_string(h.projects_file()).unwrap();
        assert_eq!(s, p, "Case {} /etc/projects miscompare", tcase.path);
        let mut p = PROJID_HEADER.to_string();
        if enabled {
            p += tcase.expected_projid;
        }
        let s = std::fs::read_to_string(h.projid_file()).unwrap();
        assert_eq!(s, p, "Case {} /etc/projid miscompare", tcase.path);
    }

    fn run_case_enabled(h: &Harness, tcase: &QuotaTestCase, seq: usize) {
        let err = match tcase.op {
            "Supports" => {
                let supports = h
                    .fake_supports_quotas(tcase.path, tcase.user_namespaces_enabled)
                    .unwrap_or_else(|e| {
                        panic!(
                            "Case {seq} ({}, {}) Got error in fakeSupportsQuotas: {e}",
                            tcase.name, tcase.path
                        )
                    });
                let expected = tcase.supports_quota && tcase.user_namespaces_enabled;
                assert_eq!(
                    supports, expected,
                    "Case {seq} ({}, {}, userNamespacesEnabled: {}) fakeSupportsQuotas",
                    tcase.name, tcase.path, tcase.user_namespaces_enabled
                );
                return;
            }
            "Set" => h
                .fake_assign_quota(
                    tcase.path,
                    tcase.poduid,
                    tcase.bytes,
                    tcase.user_namespaces_enabled,
                )
                .err(),
            "Clear" => h
                .fake_clear_quota(tcase.path, tcase.user_namespaces_enabled)
                .err(),
            "GetConsumption" => h.q.get_consumption(tcase.path).err(),
            "GetInodes" => h.q.get_inodes(tcase.path).err(),
            op => panic!(
                "Case {seq} ({}, {}) unknown operation {op}",
                tcase.name, tcase.path
            ),
        };
        match (err, tcase.expects_set_quota) {
            (Some(e), true) => panic!(
                "Case {seq} ({}, {}) {} expected to clear quota but failed {e}",
                tcase.name, tcase.path, tcase.op
            ),
            (None, false) => panic!(
                "Case {seq} ({}, {}) {} expected not to clear quota but succeeded",
                tcase.name, tcase.path, tcase.op
            ),
            _ => {}
        }
    }

    fn run_case_disabled(h: &Harness, tcase: &QuotaTestCase, seq: usize) {
        let err = match tcase.op {
            "Supports" => {
                let supports = h
                    .fake_supports_quotas(tcase.path, tcase.user_namespaces_enabled)
                    .unwrap_or(false);
                assert!(
                    !supports,
                    "Case {seq} ({}, {}) supports quotas but shouldn't",
                    tcase.name, tcase.path
                );
                return;
            }
            "Set" => h
                .fake_assign_quota(
                    tcase.path,
                    tcase.poduid,
                    tcase.bytes,
                    tcase.user_namespaces_enabled,
                )
                .err(),
            "Clear" => h
                .fake_clear_quota(tcase.path, tcase.user_namespaces_enabled)
                .err(),
            "GetConsumption" => h.q.get_consumption(tcase.path).err(),
            "GetInodes" => h.q.get_inodes(tcase.path).err(),
            op => panic!(
                "Case {seq} ({}, {}) unknown operation {op}",
                tcase.name, tcase.path
            ),
        };
        assert!(
            err.is_some(),
            "Case {seq} ({}, {}) {}: supports quotas but shouldn't",
            tcase.name,
            tcase.path,
            tcase.op
        );
    }

    fn test_add_remove_quotas(enabled: bool) {
        let h = Harness::new(enabled);
        let mut expected = [0i32; 11];
        for (seq, tcase) in quota_test_cases().iter().enumerate() {
            if enabled {
                for (e, d) in expected.iter_mut().zip(tcase.delta) {
                    *e += d;
                }
            }
            if enabled {
                run_case_enabled(&h, tcase, seq);
            } else {
                run_case_disabled(&h, tcase, seq);
            }
            compare_projects_files(&h, tcase, enabled);
            let q = h.q.quota.lock().unwrap();
            let fs = h.q.fs.lock().unwrap();
            let got = [
                q.pod_quota_map.len(),
                q.dir_quota_map.len(),
                q.quota_pod_map.len(),
                q.dir_pod_map.len(),
                fs.dev_applier_map.len(),
                fs.dir_applier_map.len(),
                q.pod_dir_count_map.len(),
                q.quota_size_map.len(),
                fs.supports_quotas_map.len(),
                fs.backing_dev_map.len(),
                fs.mountpoint_map.len(),
            ];
            let names = [
                "podQuotaCount",
                "dirQuotaCount",
                "quotaPodCount",
                "dirPodCount",
                "devApplierCount",
                "dirApplierCount",
                "podDirCountCount",
                "quotaSizeCount",
                "supportsQuotasCount",
                "BackingDevCount",
                "MountpointCount",
            ];
            for i in 0..11 {
                assert_eq!(
                    got[i] as i32, expected[i],
                    "Case {seq} ({}, {}, {enabled}) {} mismatch",
                    tcase.name, tcase.path, names[i]
                );
            }
        }
    }

    #[test]
    fn test_add_remove_quotas_enabled() {
        test_add_remove_quotas(true);
    }

    #[test]
    fn test_add_remove_quotas_disabled() {
        test_add_remove_quotas(false);
    }

    /// `DiskUsage` (`fs.go:77,85`) reads `GetConsumption`/`GetInodes`: `None`
    /// until a quota applier is bound to the path, then the applier's numbers.
    #[test]
    fn get_consumption_and_inodes_use_the_bound_applier() {
        let h = Harness::new(true);
        assert_eq!(h.q.get_consumption("/quota1/a").unwrap(), None);
        assert_eq!(h.q.get_inodes("/quota1/a").unwrap(), None);
        assert!(h.fake_supports_quotas("/quota1/a", true).unwrap());
        assert_eq!(h.q.get_consumption("/quota1/a").unwrap(), Some(4096));
        assert_eq!(h.q.get_inodes("/quota1/a").unwrap(), Some(1));
        h.fake_assign_quota("/quota1/a", "uid", 1024, true).unwrap();
        assert_eq!(h.q.get_consumption("/quota1/a").unwrap(), Some(4096));
        assert_eq!(BAD_QUOTA_ID, 0);
    }

    #[test]
    fn assign_null_quota_is_refused() {
        let h = Harness::new(true);
        let err =
            h.q.assign_quota(&dummy_quota_test(), "/quota1/a", "u", None, true)
                .unwrap_err();
        assert_eq!(
            err.to_string(),
            "attempting to assign null quota to /quota1/a"
        );
    }
}
