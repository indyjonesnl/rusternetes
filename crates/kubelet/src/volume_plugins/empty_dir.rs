use crate::volume_plugins::{
    Mounter, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{EmptyDirVolumeSource, Pod};
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

/// Port of `pkg/volume/emptydir/empty_dir.go`.
pub struct EmptyDirPlugin {
    host: Arc<dyn VolumeHost>,
}

impl EmptyDirPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }

    /// `newUnmounterInternal` (`empty_dir.go:172-183`): the unmounter with its
    /// mount detector and mounter injected, so a test can drive every branch
    /// of `TearDownAt` without a real tmpfs.
    pub(crate) fn new_unmounter_internal(
        &self,
        vol_name: &str,
        pod_uid: &str,
        mounter: Arc<dyn Unmount>,
        mount_detector: Arc<dyn MountDetector>,
    ) -> EmptyDirUnmounter {
        EmptyDirUnmounter {
            path: self.host.get_pod_volume_dir(pod_uid, self.name(), vol_name),
            mounter,
            mount_detector,
        }
    }
}

#[async_trait]
impl VolumePlugin for EmptyDirPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::EMPTY_DIR
    }

    /// `GetVolumeName` (`empty_dir.go:84-92`): the user-defined volume name,
    /// because this is an ephemeral volume type.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        if spec.volume.empty_dir.is_none() {
            return Err(anyhow!("spec does not reference an emptyDir volume type"));
        }
        Ok(spec.name().to_string())
    }

    /// `RequiresRemount` (`empty_dir.go:98-100`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`empty_dir.go:106-108`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `CanSupport` (`empty_dir.go:94`). emptyDir has no PV form, so only the
    /// inline arm is checked.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.empty_dir.is_some()
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(EmptyDirMounter {
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            volume_name: spec.volume.name.clone(),
            empty_dir: spec
                .volume
                .empty_dir
                .clone()
                .expect("checked by can_support"),
            fs_group: crate::volume_plugins::util::fs_group_from(pod),
        }))
    }

    /// `NewUnmounter` (`empty_dir.go:167-170`): the real detector and mounter
    /// are injected here, the fakes in `newUnmounterInternal`'s callers.
    fn new_unmounter(&self, vol_name: &str, pod_uid: &str) -> Result<Box<dyn Unmounter>> {
        Ok(Box::new(self.new_unmounter_internal(
            vol_name,
            pod_uid,
            Arc::new(RealMounter),
            Arc::new(RealMountDetector),
        )))
    }

    /// `ConstructVolumeSpec` (`empty_dir.go:185-195`): a bare emptyDir named
    /// after the volume; the medium is rediscovered at teardown from the mount.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<ReconstructedVolume> {
        crate::volume_plugins::util::reconstructed_volume(
            vol_name,
            serde_json::json!({"emptyDir": {}}),
        )
    }
}

/// `perm` (`empty_dir.go:50`): `const perm os.FileMode = 0777`.
#[cfg(unix)]
const PERM: u32 = 0o777;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Medium {
    Default,
    Memory,
    /// `v1helper.IsHugePageMedium`. Upstream mounts hugetlbfs (`setupHugepages`);
    /// we do not yet, so it gets a plain directory as before (tracked separately).
    HugePages,
}

/// The medium switch of `SetUpAt` (`empty_dir.go:268-277`):
/// `default: err = fmt.Errorf("unknown storage medium %q", ed.medium)`.
/// `IsHugePageMedium` is `HugePages` or the `HugePages-` prefix
/// (`pkg/apis/core/v1/helper/helpers.go`).
fn classify_medium(medium: Option<&str>) -> Result<Medium> {
    match medium.unwrap_or("") {
        "" => Ok(Medium::Default),
        "Memory" => Ok(Medium::Memory),
        m if m == "HugePages" || m.starts_with("HugePages-") => Ok(Medium::HugePages),
        m => Err(anyhow!("unknown storage medium {:?}", m)),
    }
}

/// `setupDir` (`empty_dir.go:447-486`): MkdirAll, Lstat, chmod to `perm` when
/// the mode differs (umask), and return every error. The old helper swallowed
/// them.
pub(crate) fn setup_dir(dir: &str) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::symlink_metadata(dir)?.permissions().mode() & 0o777;
        if mode != PERM {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(PERM))?;
            let after = std::fs::symlink_metadata(dir)?.permissions().mode() & 0o777;
            if after != PERM {
                tracing::error!(
                    "Expected directory {:?} permissions to be: {:o}; got: {:o}",
                    dir,
                    PERM,
                    after
                );
            }
        }
    }
    Ok(())
}

/// Port of `mountDetector` (`empty_dir.go:198-205`): how to find what kind of
/// mount a path is backed by.
///
/// `GetMountMedium` also returns the huge-page size as a `*resource.Quantity`;
/// only `setupHugepages`/`getPageSize` read it, and neither is ported (see
/// [`Medium::HugePages`]), so it is not returned.
pub(crate) trait MountDetector: Send + Sync {
    /// `GetMountMedium`: the medium `path` is backed by and whether `path` is
    /// itself the mount point. `(Memory, false)` means the path is on a tmpfs
    /// but is not its root.
    fn get_mount_medium(&self, path: &str) -> Result<(Medium, bool)>;
}

/// The one method of `mount.Interface` (`staging/src/k8s.io/mount-utils/`) emptyDir's
/// teardown calls: `Unmount`.
pub(crate) trait Unmount: Send + Sync {
    fn unmount(&self, target: &str) -> Result<()>;
}

/// `realMountDetector` (`empty_dir_linux.go:41-116`), minus the huge-page size.
struct RealMountDetector;

impl MountDetector for RealMountDetector {
    /// `empty_dir_linux.go:87-116`: `IsLikelyNotMountPoint`, then `statfs` and
    /// the filesystem magic.
    fn get_mount_medium(&self, path: &str) -> Result<(Medium, bool)> {
        let not_mnt = crate::pod_dirs::is_likely_not_mount_point(std::path::Path::new(path))
            .map_err(|e| anyhow!("IsLikelyNotMountPoint({path:?}): {e}"))?;
        let buf = rustix::fs::statfs(path).map_err(|e| anyhow!("statfs({path:?}): {e}"))?;
        let medium = match buf.f_type as u64 {
            LINUX_TMPFS_MAGIC => Medium::Memory,
            LINUX_HUGETLBFS_MAGIC => Medium::HugePages,
            _ => Medium::Default,
        };
        Ok((medium, !not_mnt))
    }
}

/// `linuxTmpfsMagic` / `linuxHugetlbfsMagic` (`empty_dir_linux.go:36-37`).
const LINUX_TMPFS_MAGIC: u64 = 0x0102_1994;
const LINUX_HUGETLBFS_MAGIC: u64 = 0x9584_58f6;

/// `mount.New("")`'s `Unmount` (`staging/src/k8s.io/mount-utils/mount_linux.go:401`): `umount <target>`.
struct RealMounter;

impl Unmount for RealMounter {
    fn unmount(&self, target: &str) -> Result<()> {
        crate::runtime::unmount_path(target)
    }
}

/// Port of the `emptyDir` unmounter (`empty_dir.go:209-218`). Setup stays on
/// [`EmptyDirMounter`]; upstream's one `emptyDir` struct serves both.
pub(crate) struct EmptyDirUnmounter {
    path: String,
    mounter: Arc<dyn Unmount>,
    mount_detector: Arc<dyn MountDetector>,
}

#[async_trait]
impl Unmounter for EmptyDirUnmounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `TearDownAt` (`empty_dir.go:495-525`).
    ///
    /// Not ported: the first step, removing the ready dir
    /// (`getMetaDir`, `empty_dir.go:497-500`). Our `SetUp` never calls
    /// `volumeutil.SetReady` (`empty_dir.go:284`), so there is none to remove;
    /// tracked with the rest of `SetUpAt`'s readiness handling (#1972).
    async fn tear_down_at(&self, dir: &str) -> Result<()> {
        // `mount.PathExists` (`staging/src/k8s.io/mount-utils/mount_helper_unix.go:190`).
        match std::fs::metadata(dir) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                warn!("Warning: Unmount skipped because path does not exist: {dir}");
                return Ok(());
            }
            Err(e) => return Err(anyhow!("error checking if path exists: {e}")),
        }

        // Figure out the medium.
        let (medium, is_mnt) = self.mount_detector.get_mount_medium(dir)?;
        if is_mnt && matches!(medium, Medium::Memory | Medium::HugePages) {
            return self.teardown_tmpfs_or_hugetlbfs(dir);
        }
        // assume StorageMediumDefault
        teardown_default(dir)
    }
}

impl EmptyDirUnmounter {
    /// `teardownTmpfsOrHugetlbfs` (`empty_dir.go:544-555`).
    fn teardown_tmpfs_or_hugetlbfs(&self, dir: &str) -> Result<()> {
        self.mounter.unmount(dir)?;
        remove_all(dir)
    }
}

/// `teardownDefault` (`empty_dir.go:527-542`). The fsquota branch
/// (`LocalStorageCapacityIsolationFSQuotaMonitoring`) is not ported: the
/// feature is off by default and `SetUp` never assigns a quota
/// (`assignQuota`).
fn teardown_default(dir: &str) -> Result<()> {
    // Renaming the directory is not required anymore because the operation
    // executor now handles duplicate operations on the same volume.
    remove_all(dir)
}

/// `os.RemoveAll`: a path that does not exist is not an error.
fn remove_all(dir: &str) -> Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

struct EmptyDirMounter {
    path: String,
    volume_name: String,
    empty_dir: EmptyDirVolumeSource,
    /// `MounterArgs.FsGroup` (`operation_generator.go:501-509`).
    fs_group: Option<i64>,
}

#[async_trait]
impl Mounter for EmptyDirMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
        let volume_dir = &self.path;
        let empty_dir = &self.empty_dir;
        // SetUpAt (empty_dir.go:268-277): the medium switch. Anything that is
        // not Default, Memory or a huge-page medium is an error.
        let medium = classify_medium(empty_dir.medium.as_deref())?;
        // setupDir's errors are returned by upstream, not swallowed.
        setup_dir(volume_dir)?;

        // Memory-medium emptyDir is a tmpfs. Mount it on the host volume dir
        // (propagated to the host daemon via the kubelet's rshared bind) so
        // it persists across container restarts AND reports fs_type=tmpfs.
        // K8s ref: pkg/volume/emptydir/empty_dir.go setupTmpfs.
        if medium == Medium::Memory {
            let size_bytes = empty_dir
                .size_limit
                .as_deref()
                .and_then(crate::runtime::parse_quantity_bytes);
            crate::runtime::mount_tmpfs_for_emptydir(volume_dir, size_bytes)?;
        }
        // `volume.NewVolumeOwnership(ed, dir, mounterArgs.FsGroup, nil
        // /*fsGroupChangePolicy*/, ..).ChangePermissions()` (empty_dir.go:277-278)
        // after the medium is set up. emptyDir's `GetAttributes` is
        // `ReadOnly: false` (:222), so rwMask 0660 (+ setgid|0110 on dirs).
        // Deviation: upstream discards the error (`_ =`); we return it, as the
        // other plugins here do, so a pod never starts against an unreadable
        // volume.
        crate::volume_ownership::set_volume_ownership(Path::new(volume_dir), self.fs_group, false)?;
        info!(
            "Created emptyDir volume {} at {}",
            self.volume_name, volume_dir
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::Volume;
    use serde_json::json;

    fn plugin() -> EmptyDirPlugin {
        EmptyDirPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
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
            path: d.to_string_lossy().into_owned(),
            volume_name: "v".into(),
            empty_dir: EmptyDirVolumeSource {
                medium: None,
                size_limit: None,
            },
            fs_group: Some(gid),
        };
        m.set_up().await.unwrap();
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
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), crate::pod_dirs::plugin::EMPTY_DIR);
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
        let r = crate::runtime::mount_tmpfs_for_emptydir("/nonexistent/emptydir/mountpoint", None);
        assert!(r.is_err());
    }

    // ---- TearDownAt (`doTestPlugin`/`testTearDown`, empty_dir_test.go:146-300) ----

    /// `fakeMountDetector` (`empty_dir_test.go:76-83`).
    struct FakeMountDetector {
        medium: Medium,
        is_mount: bool,
    }

    impl MountDetector for FakeMountDetector {
        fn get_mount_medium(&self, _path: &str) -> Result<(Medium, bool)> {
            Ok((self.medium, self.is_mount))
        }
    }

    struct FailingDetector;

    impl MountDetector for FailingDetector {
        fn get_mount_medium(&self, path: &str) -> Result<(Medium, bool)> {
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
        let dir = crate::pod_dirs::get_pod_volume_dir(&root, "poduid", plugin().name(), "scratch");
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("nested/file"), "data").unwrap();
        (root, dir)
    }

    fn unmounter_for(
        root: &str,
        detector: Arc<dyn MountDetector>,
        mounter: Arc<dyn Unmount>,
    ) -> EmptyDirUnmounter {
        EmptyDirPlugin::new(Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
            root.to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        )))
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
        let (medium, is_mnt) = RealMountDetector
            .get_mount_medium(d.to_str().unwrap())
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
        let (medium, is_mnt) = RealMountDetector.get_mount_medium("/dev/shm").unwrap();
        assert_eq!(medium, Medium::Memory);
        assert!(is_mnt);
    }

    /// `ConstructVolumeSpec` (`empty_dir.go:185-195`).
    #[test]
    fn construct_volume_spec_is_a_bare_empty_dir_named_after_the_volume() {
        let r = plugin()
            .construct_volume_spec("scratch", "/ignored")
            .unwrap();
        assert_eq!(r.volume.name, "scratch");
        assert!(r.volume.empty_dir.is_some());
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
}
