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
    pub host: Arc<dyn VolumeHost>,
}

impl EmptyDirPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }

    /// `getMetaDir` (`empty_dir.go:557-559`):
    /// `filepath.Join(host.GetPodPluginDir(pod.UID, EscapeQualifiedName(emptyDirPluginName)), volName)`.
    pub fn get_meta_dir(&self, pod_uid: &str, vol_name: &str) -> String {
        let escaped = crate::pod_dirs::escape_qualified_name(self.name());
        Path::new(&self.host.get_pod_plugin_dir(pod_uid, &escaped))
            .join(vol_name)
            .to_string_lossy()
            .into_owned()
    }

    /// `newUnmounterInternal` (`empty_dir.go:172-183`): the unmounter with its
    /// mount detector and mounter injected, so a test can drive every branch
    /// of `TearDownAt` without a real tmpfs.
    pub fn new_unmounter_internal(
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

    /// `NewMounter` (`empty_dir.go:141-165`): `plugin.host.GetMounter()` and
    /// the real mount detector, unless the host injects others.
    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        let deps = self
            .host
            .empty_dir_mount_deps()
            .unwrap_or_else(EmptyDirMountDeps::real);
        let empty_dir = spec
            .volume
            .empty_dir
            .clone()
            .expect("checked by can_support");
        // `newMounterInternal` (`empty_dir.go:143-154`): only a memory-backed
        // emptyDir has a computed size limit; the default is a zero Quantity.
        let size_limit = if empty_dir.medium.as_deref() == Some("Memory") {
            let node_allocatable_memory = self
                .host
                .get_node_allocatable()
                .get("memory")
                .and_then(|m| crate::runtime::parse_quantity_bytes(m))
                .map(|b| i64::try_from(b).unwrap_or(i64::MAX))
                .unwrap_or(0);
            calculate_empty_dir_memory_size(
                node_allocatable_memory,
                empty_dir.size_limit.as_deref(),
                pod,
            )
        } else {
            0
        };
        Ok(Box::new(EmptyDirMounter {
            size_limit,
            plugin_dir: self.host.get_plugin_dir(self.name()),
            pod: pod.clone(),
            mounter: deps.mounter,
            mount_detector: deps.detector,
            meta_dir: self.get_meta_dir(&pod.metadata.uid, &spec.volume.name),
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            volume_name: spec.volume.name.clone(),
            empty_dir,
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
pub enum Medium {
    Default,
    Memory,
    /// `v1helper.IsHugePageMedium`: a hugetlbfs mount (`setupHugepages`).
    HugePages,
}

/// The medium switch of `SetUpAt` (`empty_dir.go:268-277`):
/// `default: err = fmt.Errorf("unknown storage medium %q", ed.medium)`.
/// `IsHugePageMedium` is `HugePages` or the `HugePages-` prefix
/// (`pkg/apis/core/v1/helper/helpers.go`).
pub fn classify_medium(medium: Option<&str>) -> Result<Medium> {
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
pub fn setup_dir(dir: &str) -> Result<()> {
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
pub trait MountDetector: Send + Sync {
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
pub trait Mount: Unmount + Send + Sync {
    /// `IsLikelyNotMountPoint` (`mount-utils/mount.go`), which `SetUpAt`
    /// asks its mounter (`empty_dir.go:236`).
    fn is_likely_not_mount_point(&self, dir: &str) -> std::io::Result<bool>;

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
pub trait Unmount: Send + Sync {
    fn unmount(&self, target: &str) -> Result<()>;
}

/// `calculateEmptyDirMemorySize` (`empty_dir.go:114-140`): the size limit
/// defaults to node allocatable memory, is lowered to the pod's memory limit
/// (`cm.ResourceConfigForPod(..).Memory`) when that is in `(0, size]`, and
/// finally to the volume's own `sizeLimit` when that is in `(0, size]`.
pub fn calculate_empty_dir_memory_size(
    node_allocatable_memory: i64,
    volume_size_limit: Option<&str>,
    pod: &Pod,
) -> i64 {
    let mut size_limit = node_allocatable_memory;
    if let Some(pod_memory_limit) = crate::cm_helpers::resource_config_for_pod_memory(pod) {
        // ensure 0 < value < size
        if pod_memory_limit > 0 && pod_memory_limit <= size_limit {
            size_limit = pod_memory_limit;
        }
    }
    // volume local size is used if and only if less than what pod could consume
    if let Some(volume_size_limit) =
        volume_size_limit.and_then(crate::runtime::parse_quantity_bytes)
    {
        let volume_size_limit = i64::try_from(volume_size_limit).unwrap_or(i64::MAX);
        if volume_size_limit > 0 && volume_size_limit <= size_limit {
            size_limit = volume_size_limit;
        }
    }
    size_limit
}

/// `generateTmpfsMountOptions` (`empty_dir.go:573-584`).
pub fn generate_tmpfs_mount_options(size_limit: i64, noswap_supported: bool) -> Vec<String> {
    let mut options = Vec::new();
    // Linux system default is 50% of capacity.
    if size_limit > 0 {
        options.push(format!("size={size_limit}"));
    }
    if noswap_supported {
        options.push(crate::swap_util::TMPFS_NOSWAP_OPTION.to_string());
    }
    options
}

/// `realMountDetector` (`empty_dir_linux.go:41-116`).
pub struct RealMountDetector;

/// `getPageSize` (`empty_dir_linux.go:47-85`): the `pagesize=` option of the
/// mount whose path is `path`, in bytes. `mounter.List()` is the parsed
/// mountinfo (`mount-utils` `ParseMountInfo`), whose `Opts` are the per-mount
/// options followed by the super-block options.
pub fn get_page_size(mountinfo: &str, path: &str) -> Result<u64> {
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
pub struct RealMounter;

impl Unmount for RealMounter {
    fn unmount(&self, target: &str) -> Result<()> {
        crate::runtime::unmount_path(target)
    }
}

impl Mount for RealMounter {
    fn is_likely_not_mount_point(&self, dir: &str) -> std::io::Result<bool> {
        crate::pod_dirs::is_likely_not_mount_point(Path::new(dir))
    }

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

/// The `mount.Interface` and `mountDetector` an emptyDir mounter is built with
/// (`newMounterInternal`, `empty_dir.go:128-139`). A [`VolumeHost`] may inject
/// others (upstream's `FakeVolumeHost` injects `mount.NewFakeMounter`), so the
/// plugins that wrap an emptyDir can be tested without mounting a tmpfs.
#[derive(Clone)]
pub struct EmptyDirMountDeps {
    pub mounter: Arc<dyn Mount>,
    pub detector: Arc<dyn MountDetector>,
}

impl EmptyDirMountDeps {
    /// `mount.New("")` and `&realMountDetector{mounter}`.
    pub fn real() -> Self {
        Self {
            mounter: Arc::new(RealMounter),
            detector: Arc::new(RealMountDetector),
        }
    }
}

/// Port of the `emptyDir` unmounter (`empty_dir.go:209-218`). Setup stays on
/// [`EmptyDirMounter`]; upstream's one `emptyDir` struct serves both.
pub struct EmptyDirUnmounter {
    path: String,
    meta_dir: String,
    mounter: Arc<dyn Unmount>,
    mount_detector: Arc<dyn MountDetector>,
}

impl crate::volume_plugins::plugin::MetricsProvider for EmptyDirUnmounter {
    /// `emptyDir.MetricsProvider = volume.NewMetricsDu(getPath(..))`
    /// (`empty_dir.go:163`, `:180`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        crate::volume_plugins::plugin::MetricsProvider::get_metrics(
            &crate::volume_plugins::metrics_du::MetricsDu::new(&self.path),
        )
    }
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
    /// `setupTmpfs` (`empty_dir.go:320-340`): `setupDir`, then mount a tmpfs
    /// unless `dir` already is one (SetUp is idempotent). The mount goes
    /// through the injected mounter, as upstream's
    /// `ed.mounter.MountSensitiveWithoutSystemd("tmpfs", dir, "tmpfs", options, nil)`.
    ///
    /// The options are `generateTmpfsMountOptions` (`empty_dir.go:573-584`)
    /// with `swap.IsTmpfsNoswapOptionSupported(ed.mounter, GetPluginDir(..))`
    /// (`empty_dir.go:336`).
    fn setup_tmpfs(&self, dir: &str) -> Result<()> {
        setup_dir(dir)?;
        // Make SetUp idempotent.
        let (medium, is_mnt, _) = self.mount_detector.get_mount_medium(dir, "Memory")?;
        // If the directory is a mountpoint with medium memory, there is no
        // work to do since we are already in the desired state.
        if is_mnt && medium == Medium::Memory {
            return Ok(());
        }
        let options = generate_tmpfs_mount_options(
            self.size_limit,
            crate::swap_util::is_tmpfs_noswap_option_supported(
                self.mounter.as_ref(),
                &self.plugin_dir,
            ),
        );
        info!(
            "pod {}: mounting tmpfs for volume {}",
            self.pod.metadata.uid, self.volume_name
        );
        self.mounter
            .mount_sensitive_without_systemd("tmpfs", dir, "tmpfs", &options)
    }

    /// `setupHugepages` (`empty_dir.go:343-385`).
    pub fn setup_hugepages(&self, dir: &str) -> Result<()> {
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
pub fn quantity_string(bytes: u64) -> String {
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
pub fn get_page_size_mount_option(medium: &str, pod: &Pod) -> Result<String> {
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
pub fn assign_quota_with(
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

pub struct EmptyDirMounter {
    /// `ed.sizeLimit.Value()` (`newMounterInternal`): the computed tmpfs size
    /// for a memory-backed emptyDir, 0 otherwise.
    pub size_limit: i64,
    /// `ed.plugin.host.GetPluginDir(emptyDirPluginName)`: scratch space for
    /// the `noswap` probe.
    pub plugin_dir: String,
    /// `ed.pod`: `getPageSizeMountOption` reads its resource requests.
    pub pod: Pod,
    pub mounter: Arc<dyn Mount>,
    pub mount_detector: Arc<dyn MountDetector>,
    pub path: String,
    /// `getMetaDir` (`empty_dir.go:557-559`).
    pub meta_dir: String,
    pub volume_name: String,
    pub empty_dir: EmptyDirVolumeSource,
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

impl crate::volume_plugins::plugin::MetricsProvider for EmptyDirMounter {
    /// `emptyDir.MetricsProvider = volume.NewMetricsDu(getPath(..))`
    /// (`empty_dir.go:163`, `:180`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        crate::volume_plugins::plugin::MetricsProvider::get_metrics(
            &crate::volume_plugins::metrics_du::MetricsDu::new(&self.path),
        )
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
        let not_mnt = match self.mounter.is_likely_not_mount_point(volume_dir) {
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
            Medium::Memory => self.setup_tmpfs(volume_dir)?,
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

/// `(source, target, fstype, options)` of one recorded mount.
pub type MountCall = (String, String, String, Vec<String>);

/// Test double for `mount.NewFakeMounter`: records every
/// `MountSensitiveWithoutSystemd` call instead of mounting, and treats a
/// target as a mount point once it was "mounted" (as `FakeMounter`'s
/// `MountPoints` do), so idempotent `SetUpAt` paths can be driven. Also used by
/// the integration tests, which run unprivileged and so cannot mount a tmpfs.
#[derive(Default)]
pub struct RecordingMount {
    /// `(source, target, fstype, options)` per call.
    pub calls: std::sync::Mutex<Vec<MountCall>>,
}

impl RecordingMount {
    fn is_mounted(&self, dir: &str) -> bool {
        self.calls.lock().unwrap().iter().any(|c| c.1 == dir)
    }
}

impl Unmount for RecordingMount {
    fn unmount(&self, _target: &str) -> Result<()> {
        Ok(())
    }
}

impl Mount for RecordingMount {
    fn is_likely_not_mount_point(&self, dir: &str) -> std::io::Result<bool> {
        Ok(!self.is_mounted(dir))
    }

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

/// Detector over a [`RecordingMount`]: a mounted target is a memory mount.
struct RecordedDetector(Arc<RecordingMount>);

impl MountDetector for RecordedDetector {
    fn get_mount_medium(&self, path: &str, _m: &str) -> Result<(Medium, bool, Option<u64>)> {
        if self.0.is_mounted(path) {
            Ok((Medium::Memory, true, None))
        } else {
            Ok((Medium::Default, false, None))
        }
    }
}

/// Deps whose mounter records, plus a handle on the recorder.
#[doc(hidden)]
pub fn fake_mount_deps() -> (EmptyDirMountDeps, Arc<RecordingMount>) {
    let rec = Arc::new(RecordingMount::default());
    (
        EmptyDirMountDeps {
            mounter: rec.clone(),
            detector: Arc::new(RecordedDetector(rec.clone())),
        },
        rec,
    )
}
