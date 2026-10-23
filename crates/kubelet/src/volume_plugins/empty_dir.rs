use crate::volume_plugins::{
    Attributes, Mounter, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
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

    /// `getMetaDir` (`empty_dir.go:557-559`):
    /// `filepath.Join(host.GetPodPluginDir(pod.UID, EscapeQualifiedName(emptyDirPluginName)), volName)`.
    fn get_meta_dir(&self, pod_uid: &str, vol_name: &str) -> String {
        let escaped = crate::pod_dirs::escape_qualified_name(self.name());
        Path::new(&self.host.get_pod_plugin_dir(pod_uid, &escaped))
            .join(vol_name)
            .to_string_lossy()
            .into_owned()
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
            meta_dir: self.get_meta_dir(pod_uid, vol_name),
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
            pod: pod.clone(),
            mounter: Arc::new(RealMounter),
            mount_detector: Arc::new(RealMountDetector),
            meta_dir: self.get_meta_dir(&pod.metadata.uid, &spec.volume.name),
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            volume_name: spec.volume.name.clone(),
            empty_dir: spec
                .volume
                .empty_dir
                .clone()
                .expect("checked by can_support"),
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
    /// `v1helper.IsHugePageMedium`: a hugetlbfs mount (`setupHugepages`).
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
pub(crate) trait MountDetector: Send + Sync {
    /// `GetMountMedium`: the medium `path` is backed by, whether `path` is
    /// itself the mount point, and the huge-page size in bytes (the
    /// `*resource.Quantity`; `None` unless `path` is a hugetlbfs and
    /// `requested_medium` names a size). `(Memory, false, None)` means the
    /// path is on a tmpfs but is not its root.
    fn get_mount_medium(
        &self,
        path: &str,
        requested_medium: &str,
    ) -> Result<(Medium, bool, Option<u64>)>;
}

/// The `mount.Interface.MountSensitiveWithoutSystemd` call emptyDir's setup
/// makes (`staging/src/k8s.io/mount-utils/mount.go`).
pub(crate) trait Mount: Send + Sync {
    fn mount_sensitive_without_systemd(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
    ) -> Result<()>;
}

/// The one method of `mount.Interface` (`staging/src/k8s.io/mount-utils/`) emptyDir's
/// teardown calls: `Unmount`.
pub(crate) trait Unmount: Send + Sync {
    fn unmount(&self, target: &str) -> Result<()>;
}

/// `realMountDetector` (`empty_dir_linux.go:41-116`).
struct RealMountDetector;

/// `getPageSize` (`empty_dir_linux.go:47-85`): the `pagesize=` option of the
/// mount whose path is `path`, in bytes. `mounter.List()` is the parsed
/// mountinfo (`mount-utils` `ParseMountInfo`), whose `Opts` are the per-mount
/// options followed by the super-block options.
fn get_page_size(mountinfo: &str, path: &str) -> Result<u64> {
    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split(' ').collect();
        // `ParseMountInfo` requires a " - " separator after the optional fields.
        let Some(sep) = fields.iter().position(|f| *f == "-") else {
            continue;
        };
        if sep < 6 || fields.len() < sep + 4 {
            continue;
        }
        if fields[4] != path {
            continue;
        }
        let opts = fields[5].split(',').chain(fields[sep + 3].split(','));
        for opt in opts {
            let opt = opt.trim();
            if let Some(v) = opt.strip_prefix("pagesize=") {
                // NOTE: Adding suffix 'i' as result should be comparable with
                // a medium size (`empty_dir_linux.go:70-76`).
                let q = if v.ends_with('i') {
                    v.to_string()
                } else {
                    format!("{v}i")
                };
                return crate::runtime::parse_quantity_bytes(&q).ok_or_else(|| {
                    anyhow!("error getting page size from '{opt}' mount option: invalid quantity")
                });
            }
        }
        return Err(anyhow!("no pagesize option specified for {path} mount"));
    }
    Err(anyhow!("mount point for {path} not found"))
}

impl MountDetector for RealMountDetector {
    /// `empty_dir_linux.go:87-116`: `IsLikelyNotMountPoint`, then `statfs` and
    /// the filesystem magic.
    fn get_mount_medium(
        &self,
        path: &str,
        requested_medium: &str,
    ) -> Result<(Medium, bool, Option<u64>)> {
        let not_mnt = crate::pod_dirs::is_likely_not_mount_point(std::path::Path::new(path))
            .map_err(|e| anyhow!("IsLikelyNotMountPoint({path:?}): {e}"))?;
        let buf = rustix::fs::statfs(path).map_err(|e| anyhow!("statfs({path:?}): {e}"))?;
        match buf.f_type as u64 {
            LINUX_TMPFS_MAGIC => Ok((Medium::Memory, !not_mnt, None)),
            LINUX_HUGETLBFS_MAGIC => {
                // Skip page size detection if requested medium doesn't have
                // size specified.
                if requested_medium == "HugePages" {
                    return Ok((Medium::HugePages, !not_mnt, None));
                }
                let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")
                    .map_err(|e| anyhow!("error listing mount points: {e}"))?;
                let size = get_page_size(&mountinfo, path)?;
                Ok((Medium::HugePages, !not_mnt, Some(size)))
            }
            _ => Ok((Medium::Default, !not_mnt, None)),
        }
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

impl Mount for RealMounter {
    /// `mount -t <fstype> -o <options> <source> <target>`
    /// (`mount_linux.go` `doMount`, without the systemd-run wrapper).
    fn mount_sensitive_without_systemd(
        &self,
        source: &str,
        target: &str,
        fstype: &str,
        options: &[String],
    ) -> Result<()> {
        crate::runtime::mount_fs(source, target, fstype, options)
    }
}

/// Port of the `emptyDir` unmounter (`empty_dir.go:209-218`). Setup stays on
/// [`EmptyDirMounter`]; upstream's one `emptyDir` struct serves both.
pub(crate) struct EmptyDirUnmounter {
    path: String,
    meta_dir: String,
    mounter: Arc<dyn Unmount>,
    mount_detector: Arc<dyn MountDetector>,
}

#[async_trait]
impl Unmounter for EmptyDirUnmounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `TearDownAt` (`empty_dir.go:495-525`).
    async fn tear_down_at(&self, dir: &str) -> Result<()> {
        // First remove ready dir which created in SetUp func
        // (`empty_dir.go:497-500`).
        if let Err(e) = std::fs::remove_dir_all(&self.meta_dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(anyhow!(
                    "failed to remove ready dir [{}]: {e}",
                    self.meta_dir
                ));
            }
        }

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
        let (medium, is_mnt, _) = self.mount_detector.get_mount_medium(dir, "")?;
        if is_mnt && matches!(medium, Medium::Memory | Medium::HugePages) {
            return self.teardown_tmpfs_or_hugetlbfs(dir);
        }
        // assume StorageMediumDefault
        teardown_default(dir)
    }
}

impl EmptyDirMounter {
    /// `setupHugepages` (`empty_dir.go:343-385`).
    fn setup_hugepages(&self, dir: &str) -> Result<()> {
        setup_dir(dir)?;
        let requested = self.empty_dir.medium.as_deref().unwrap_or("");
        // Make SetUp idempotent.
        let (medium, is_mnt, mount_page_size) =
            self.mount_detector.get_mount_medium(dir, requested)?;
        // If the directory is a mountpoint with medium hugepages of the same
        // page size, there is no work to do since we are already in the
        // desired state.
        if is_mnt && medium == Medium::HugePages {
            // Medium is: Hugepages
            if requested == "HugePages" {
                return Ok(());
            }
            let mount_page_size = mount_page_size.ok_or_else(|| {
                anyhow!(
                    "pod {}: mounted dir {} pagesize is not determined",
                    self.pod.metadata.uid,
                    dir
                )
            })?;
            // Medium is: Hugepages-<size>. Mounted page size and medium size
            // must be equal.
            let medium_size = huge_page_size_from_medium(requested)?;
            if medium_size != mount_page_size {
                return Err(anyhow!(
                    "pod {}: mounted dir {} pagesize '{}' and requested medium size '{}' differ",
                    self.pod.metadata.uid,
                    dir,
                    quantity_string(mount_page_size),
                    quantity_string(medium_size)
                ));
            }
            return Ok(());
        }

        let page_size_mount_option = get_page_size_mount_option(requested, &self.pod)?;

        info!(
            "pod {}: mounting hugepages for volume {}",
            self.pod.metadata.uid, self.volume_name
        );
        self.mounter.mount_sensitive_without_systemd(
            "nodev",
            dir,
            "hugetlbfs",
            &[page_size_mount_option],
        )
    }
}

/// `v1helper.HugePageSizeFromMedium` (`pkg/apis/core/v1/helper/helpers.go:115-126`),
/// in bytes.
fn huge_page_size_from_medium(medium: &str) -> Result<u64> {
    if !(medium == "HugePages" || medium.starts_with("HugePages-")) {
        return Err(anyhow!("medium: {medium} is not a hugepage medium"));
    }
    if medium == "HugePages" {
        return Err(anyhow!("medium: {medium} doesn't have size information"));
    }
    let size = &medium["HugePages-".len()..];
    crate::runtime::parse_quantity_bytes(size)
        .ok_or_else(|| anyhow!("quantities must match the regular expression: {size:?}"))
}

/// `v1helper.HugePageSizeFromResourceName` (`helpers.go:81-87`), in bytes.
fn huge_page_size_from_resource_name(name: &str) -> Result<u64> {
    let Some(size) = name.strip_prefix("hugepages-") else {
        return Err(anyhow!("resource name: {name} is an invalid hugepage name"));
    };
    crate::runtime::parse_quantity_bytes(size)
        .ok_or_else(|| anyhow!("quantities must match the regular expression: {size:?}"))
}

/// `resource.Quantity.String()` for a binary-SI size: the largest `Ki`..`Ei`
/// suffix that divides evenly (huge-page sizes are always powers of two).
fn quantity_string(bytes: u64) -> String {
    const SUFFIXES: [&str; 7] = ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"];
    let (mut v, mut i) = (bytes, 0);
    while v != 0 && v % 1024 == 0 && i < SUFFIXES.len() - 1 {
        v /= 1024;
        i += 1;
    }
    format!("{v}{}", SUFFIXES[i])
}

/// `getPageSizeMountOption` (`empty_dir.go:389-445`): the `pagesize=<size>`
/// mount option from the pod's huge-page requests and the medium.
///
/// `PodLevelResources` is on by default in 1.35 (beta since 1.34,
/// `kube_features.go:1612`), so pod-level requests count whenever
/// `IsPodLevelResourcesSet` (`component-helpers/resource/helpers.go:80`).
fn get_page_size_mount_option(medium: &str, pod: &Pod) -> Result<String> {
    let mut page_size_found = false;
    let mut page_size = 0u64;

    let mut medium_page_size = 0u64;
    if medium != "HugePages" {
        // medium is: Hugepages-<size>
        medium_page_size = huge_page_size_from_medium(medium)?;
    }

    let mut requests: Vec<&std::collections::HashMap<String, String>> = Vec::new();
    if let Some(spec) = pod.spec.as_ref() {
        if let Some(r) = spec.resources.as_ref() {
            let set = |m: &Option<std::collections::HashMap<String, String>>| {
                m.as_ref().is_some_and(|m| {
                    m.keys()
                        .any(|k| k == "cpu" || k == "memory" || k.starts_with("hugepages-"))
                })
            };
            if set(&r.requests) || set(&r.limits) {
                if let Some(m) = r.requests.as_ref() {
                    requests.push(m);
                }
            }
        }
        // In some rare cases init containers can also consume Huge pages.
        let init = spec.init_containers.iter().flatten();
        for c in spec.containers.iter().chain(init) {
            if let Some(m) = c.resources.as_ref().and_then(|r| r.requests.as_ref()) {
                requests.push(m);
            }
        }
    }

    // We can take request because limit and requests must match.
    for list in requests {
        for name in list.keys() {
            if !name.starts_with("hugepages-") {
                continue;
            }
            let current = huge_page_size_from_resource_name(name)?;
            if medium == "HugePages" {
                // medium is: Hugepages, size is not specified. PageSize for
                // all volumes in a POD must be equal if medium is "Hugepages".
                if page_size_found && page_size != current {
                    return Err(anyhow!(
                        "medium: {medium} can't be used if container requests multiple huge page sizes"
                    ));
                }
                page_size_found = true;
                page_size = current;
            } else if current == medium_page_size {
                // medium is: Hugepages-<size>
                page_size_found = true;
                page_size = current;
            }
        }
    }

    if !page_size_found {
        return Err(anyhow!(
            "medium {medium}: hugePages storage requested, but there is no resource request for huge pages"
        ));
    }

    Ok(format!("pagesize={}", quantity_string(page_size)))
}

impl EmptyDirUnmounter {
    /// `teardownTmpfsOrHugetlbfs` (`empty_dir.go:544-555`).
    fn teardown_tmpfs_or_hugetlbfs(&self, dir: &str) -> Result<()> {
        self.mounter.unmount(dir)?;
        remove_all(dir)
    }
}

/// `teardownDefault` (`empty_dir.go:527-542`).
fn teardown_default(dir: &str) -> Result<()> {
    // The unmounter's `emptyDir` carries only the pod UID (`NewUnmounter`),
    // so `HostUsers` is nil and `userNamespacesEnabled` is false.
    #[cfg(target_os = "linux")]
    if rusternetes_common::feature_gates::gate_enabled(
        "LocalStorageCapacityIsolationFSQuotaMonitoring",
    )
    .unwrap_or(false)
    {
        // Remove any quota.
        if let Err(e) = crate::fsquota::clear_quota(dir, false) {
            warn!("Warning: Failed to clear quota on {dir}: {e}");
        }
    }
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

/// `userNamespacesEnabled` of `assignQuota`/`teardownDefault`
/// (`empty_dir.go:297-300`): `HostUsers` is true by default, so a pod is by
/// default not confined by a user namespace.
fn user_namespaces_enabled(pod: &Pod) -> bool {
    rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::UserNamespacesSupport,
    ) && pod.spec.as_ref().and_then(|s| s.host_users) == Some(false)
}

/// `assignQuota` (`empty_dir.go:292-316`): checks if the underlying medium
/// supports quotas and if so, sets one. A `None` size is a no-op.
#[cfg(target_os = "linux")]
fn assign_quota_with(
    quota: &crate::fsquota::FsQuota,
    mounter: &dyn crate::fsquota::MountInterface,
    dir: &str,
    pod_uid: &str,
    mounter_size: Option<i64>,
    user_namespaces_enabled: bool,
) -> Result<()> {
    if mounter_size.is_none() {
        return Ok(());
    }
    match quota.supports_quotas(mounter, dir, user_namespaces_enabled) {
        Err(e) => {
            tracing::debug!("Unable to check for quota support on {dir}: {e}");
        }
        Ok(true) => {
            tracing::debug!("emptydir trying to assign quota {mounter_size:?} on {dir}");
            if let Err(e) =
                quota.assign_quota(mounter, dir, pod_uid, mounter_size, user_namespaces_enabled)
            {
                tracing::debug!("Set quota on {dir} failed {e}");
                return Err(e);
            }
        }
        Ok(false) => {}
    }
    Ok(())
}

struct EmptyDirMounter {
    /// `ed.pod`: `getPageSizeMountOption` reads its resource requests.
    pod: Pod,
    mounter: Arc<dyn Mount>,
    mount_detector: Arc<dyn MountDetector>,
    path: String,
    /// `getMetaDir` (`empty_dir.go:557-559`).
    meta_dir: String,
    volume_name: String,
    empty_dir: EmptyDirVolumeSource,
}

impl EmptyDirMounter {
    /// `emptyDir.assignQuota` (`empty_dir.go:292-316`).
    fn assign_quota(&self, dir: &str, mounter_size: Option<i64>) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            assign_quota_with(
                crate::fsquota::global(),
                &crate::fsquota::RealMounter,
                dir,
                &self.pod.metadata.uid,
                mounter_size,
                user_namespaces_enabled(&self.pod),
            )
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (dir, mounter_size, user_namespaces_enabled(&self.pod));
            Ok(())
        }
    }
}

#[async_trait]
impl Mounter for EmptyDirMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `emptyDir.GetAttributes` (`pkg/volume/emptydir/empty_dir.go:220-226`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: false,
            managed: true,
            selinux_relabel: true,
        }
    }

    async fn set_up_at(&self, dir: &str, args: &crate::volume_plugins::MounterArgs) -> Result<()> {
        let volume_dir = dir;
        let empty_dir = &self.empty_dir;
        // SetUpAt (empty_dir.go:234-261): IsLikelyNotMountPoint, tolerating a
        // missing dir, then the ready-file short circuit.
        let not_mnt = match crate::pod_dirs::is_likely_not_mount_point(Path::new(volume_dir)) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
            Err(e) => return Err(anyhow!("IsLikelyNotMountPoint({volume_dir:?}): {e}")),
        };
        let kind = classify_medium(empty_dir.medium.as_deref());
        if crate::volume_plugins::util::is_ready(&self.meta_dir) {
            match kind {
                Ok(Medium::Memory) if !not_mnt => return Ok(()),
                Ok(Medium::Default) => {
                    // Further check dir exists
                    if std::fs::metadata(volume_dir).is_ok() {
                        // Dir exists, so check and assign quota if the
                        // underlying medium supports quotas.
                        return self.assign_quota(volume_dir, args.desired_size);
                    }
                    // This situation should not happen unless user manually
                    // delete volume dir. In this case, delete ready file.
                    warn!(
                        "volume ready file dir {} exist, but volume dir {} does not. Remove ready dir",
                        self.meta_dir, volume_dir
                    );
                    if let Err(e) = std::fs::remove_dir_all(&self.meta_dir) {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            warn!("failed to remove ready dir [{}]: {e}", self.meta_dir);
                        }
                    }
                }
                _ => {}
            }
        }
        // SetUpAt (empty_dir.go:268-277): the medium switch. Anything that is
        // not Default, Memory or a huge-page medium is an error.
        let medium = classify_medium(empty_dir.medium.as_deref())?;
        match medium {
            // setupDir's errors are returned by upstream, not swallowed.
            Medium::Default => setup_dir(volume_dir)?,
            Medium::HugePages => self.setup_hugepages(volume_dir)?,
            // Memory: `setupTmpfs` calls `setupDir` first.
            Medium::Memory => setup_dir(volume_dir)?,
        }

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
        crate::volume_ownership::set_volume_ownership(Path::new(volume_dir), args.fs_group, false)?;
        // `volumeutil.SetReady(ed.getMetaDir())` (`empty_dir.go:284`).
        crate::volume_plugins::util::set_ready(&self.meta_dir);
        info!(
            "Created emptyDir volume {} at {}",
            self.volume_name, volume_dir
        );
        // `err = ed.assignQuota(dir, mounterArgs.DesiredSize)` after SetReady
        // (empty_dir.go:283-286): the error is returned.
        self.assign_quota(volume_dir, args.desired_size)
    }
}

#[cfg(test)]
mod tests {
    /// `assignQuota` (`empty_dir.go:292-316`): no size is a no-op, and with
    /// `LocalStorageCapacityIsolationFSQuotaMonitoring` off (or no user
    /// namespace) `SupportsQuotas` is false, so nothing is assigned.
    #[cfg(target_os = "linux")]
    #[test]
    fn assign_quota_is_noop_without_size_or_support() {
        use crate::fsquota::{FsQuota, RealMounter};
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
        m.set_up_with(&crate::volume_plugins::MounterArgs {
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
            crate::volume_plugins::plugin::Attributes {
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
        EmptyDirPlugin::new(Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
            root.to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        )))
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
        crate::volume_plugins::util::set_ready(&m.meta_dir);
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
        crate::volume_plugins::util::set_ready(&m.meta_dir);
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
        crate::volume_plugins::util::set_ready(&m.meta_dir);
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
        crate::volume_plugins::util::set_ready(&m.meta_dir);
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

    type MountCall = (String, String, String, Vec<String>);

    #[derive(Default)]
    struct RecordingMount {
        calls: std::sync::Mutex<Vec<MountCall>>,
    }

    impl Mount for RecordingMount {
        fn mount_sensitive_without_systemd(
            &self,
            source: &str,
            target: &str,
            fstype: &str,
            options: &[String],
        ) -> Result<()> {
            self.calls.lock().unwrap().push((
                source.into(),
                target.into(),
                fstype.into(),
                options.to_vec(),
            ));
            Ok(())
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
