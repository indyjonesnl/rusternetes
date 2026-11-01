//! On-disk pod directory layout.
//!
//! A direct port of upstream's pod path family so that our on-disk layout is
//! the one Kubernetes defines, rather than one of our own:
//!
//! - directory names: `pkg/kubelet/kubeletconfig/defaults.go:20-28`
//! - path getters: `pkg/kubelet/kubelet_getters.go:60-216`
//! - plugin-name escaping: `k8s.io/utils/strings/escape.go::EscapeQualifiedName`
//!
//! The layout is:
//!
//! ```text
//! <root>/pods/<podUID>/volumes/<escaped-plugin-name>/<volumeName>
//! ```
//!
//! Every path is keyed on the pod **UID**, never the pod name. Upstream's
//! `getPodDir(podUID types.UID)` takes a UID and has no name fallback: a
//! recreated pod reusing a name gets a new UID and therefore a guaranteed-fresh
//! directory, so the kubelet can never serve the previous incarnation's files.
//! The case that makes this concrete is hydrophone, which always names its
//! driver pod `e2e-conformance-test`: keyed on name, a second run reads the
//! first run's `/tmp/results`.
//!
//! There is deliberately no name fallback. Pods admitted through the api-server
//! get their UID at `BeforeCreate`, and static pods get one from the config hash
//! (`pkg/kubelet/config/common.go:61-73`, mirrored in [`crate::static_pods`]),
//! so a real pod always has one.

use std::path::{Path, PathBuf};

/// `pkg/kubelet/kubeletconfig/defaults.go:21`
const PODS_DIR_NAME: &str = "pods";
/// `pkg/kubelet/kubeletconfig/defaults.go:22`
pub const VOLUMES_DIR_NAME: &str = "volumes";

/// `DefaultKubeletVolumeDevicesDirName` (`pkg/kubelet/config/defaults.go`):
/// where a pod's block volumes are mapped.
pub const VOLUME_DEVICES_DIR_NAME: &str = "volumeDevices";
/// `pkg/kubelet/kubeletconfig/defaults.go:23`
const VOLUME_SUBPATHS_DIR_NAME: &str = "volume-subpaths";

/// Upstream volume plugin names. Each is the `<plugin>PluginName` constant from
/// the corresponding `pkg/volume/<plugin>` package; they appear on disk escaped
/// by [`escape_qualified_name`].
pub mod plugin {
    /// `pkg/volume/emptydir/empty_dir.go:66`
    pub const EMPTY_DIR: &str = "kubernetes.io/empty-dir";
    /// `pkg/volume/configmap/configmap.go:40`
    pub const CONFIG_MAP: &str = "kubernetes.io/configmap";
    /// `pkg/volume/secret/secret.go:41`
    pub const SECRET: &str = "kubernetes.io/secret";
    /// `pkg/volume/downwardapi/downwardapi.go:40`
    pub const DOWNWARD_API: &str = "kubernetes.io/downward-api";
    /// `pkg/volume/projected/projected.go:45`
    pub const PROJECTED: &str = "kubernetes.io/projected";
    /// `pkg/volume/csi/csi_plugin.go:54`
    pub const CSI: &str = "kubernetes.io/csi";
    /// `pkg/volume/nfs/nfs.go:66`
    pub const NFS: &str = "kubernetes.io/nfs";
    /// `pkg/volume/git_repo/git_repo.go:53`
    pub const GIT_REPO: &str = "kubernetes.io/git-repo";
    /// `pkg/volume/image/image.go:41`
    pub const IMAGE: &str = "kubernetes.io/image";
    /// `pkg/volume/hostpath/host_path.go:77`
    pub const HOST_PATH: &str = "kubernetes.io/host-path";
    /// `pkg/volume/local/local.go:64`
    pub const LOCAL: &str = "kubernetes.io/local-volume";
}

/// Convert a plugin name, which contains a `/`, into a form safe to use on
/// disk. Upstream uses `~` rather than `:` "in case we ever use a filesystem
/// that doesn't allow `:`".
///
/// Port of `k8s.io/utils/strings/escape.go::EscapeQualifiedName`.
pub fn escape_qualified_name(name: &str) -> String {
    name.replace('/', "~")
}

/// Rusternetes-specific: the plugin segment used only to *locate* (cleanup,
/// resync) a volume no plugin claims (nfs, iscsi, …). Since #3262 such a volume
/// is never provisioned: `create_volume` fails like upstream's
/// `FindPluginBySpec` (`pkg/volume/plugins.go:659`) instead of creating an
/// empty directory under this segment. The `rusternetes.io/` prefix keeps it
/// from colliding with a real `kubernetes.io/` plugin.
pub const UNSUPPORTED_PLUGIN: &str = "rusternetes.io/unsupported";

/// `<root>/pods` — `getPodsDir` (`kubelet_getters.go:60-62`).
pub fn get_pods_dir(root: &str) -> PathBuf {
    Path::new(root).join(PODS_DIR_NAME)
}

/// `<root>/pods/<podUID>` — `getPodDir` (`kubelet_getters.go:160-162`).
pub fn get_pod_dir(root: &str, pod_uid: &str) -> PathBuf {
    get_pods_dir(root).join(pod_uid)
}

/// `<root>/pods/<podUID>/volumes` — `getPodVolumesDir`
/// (`kubelet_getters.go:174-176`).
pub fn get_pod_volumes_dir(root: &str, pod_uid: &str) -> PathBuf {
    get_pod_dir(root, pod_uid).join(VOLUMES_DIR_NAME)
}

/// `<root>/pods/<podUID>/volumeDevices` — `getPodVolumeDevicesDir`
/// (`kubelet_getters.go:186-188`).
pub fn get_pod_volume_devices_dir(root: &str, pod_uid: &str) -> PathBuf {
    get_pod_dir(root, pod_uid).join(VOLUME_DEVICES_DIR_NAME)
}

/// `<root>/pods/<podUID>/plugins/<escaped-plugin>` — `getPodPluginDir`
/// (`kubelet_getters.go:208-210`, over `getPodPluginsDir` `:201-203` and
/// `DefaultKubeletPluginsDirName`, `defaults.go:24`). The caller passes the
/// already-escaped plugin name, as `emptyDir.getMetaDir` does.
pub fn get_pod_plugin_dir(root: &str, pod_uid: &str, escaped_plugin_name: &str) -> PathBuf {
    get_pod_dir(root, pod_uid)
        .join("plugins")
        .join(escaped_plugin_name)
}

/// `<root>/pods/<podUID>/volume-subpaths` — `getPodVolumeSubpathsDir`
/// (`kubelet_getters.go:167-169`).
pub fn get_pod_volume_subpaths_dir(root: &str, pod_uid: &str) -> PathBuf {
    get_pod_dir(root, pod_uid).join(VOLUME_SUBPATHS_DIR_NAME)
}

/// `<root>/pods/<podUID>/volumes/<escaped-plugin>/<volumeName>` —
/// `getPodVolumeDir` (`kubelet_getters.go:181-183`).
pub fn get_pod_volume_dir(
    root: &str,
    pod_uid: &str,
    plugin_name: &str,
    volume_name: &str,
) -> PathBuf {
    get_pod_volumes_dir(root, pod_uid)
        .join(escape_qualified_name(plugin_name))
        .join(volume_name)
}

/// Report whether `path` is *likely not* a mount point, by comparing its device
/// with its parent's.
///
/// Port of `isLikelyNotMountPointStat`
/// (`staging/src/k8s.io/mount-utils/mount_linux.go:443-458`), which is the
/// fallback `IsLikelyNotMountPoint` uses when `statx` is unavailable.
///
/// The error case carries the safety property: upstream returns `(true, err)`
/// on a failed stat, callers propagate the error, and
/// [`crate::volumes::VolumeManager::pod_volumes_exist`] turns any error into
/// "volumes might still exist" so cleanup is **skipped**. Never collapse this
/// into a bare bool — swallowing the error flips the default from "leave it
/// alone" to "delete it".
pub fn is_likely_not_mount_point(path: &Path) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let stat = std::fs::metadata(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other(format!("{} has no parent", path.display())))?;
    let root_stat = std::fs::metadata(parent)?;
    // A different device from the parent means this is a mount point.
    Ok(stat.dev() == root_stat.dev())
}

/// The pod UIDs that have a directory on disk.
///
/// Port of `listPodsFromDisk` (`pkg/kubelet/kubelet_pods.go:185-197`): every
/// entry under `<root>/pods` is named by a pod UID, which is what makes the
/// orphan sweep exact rather than heuristic. A missing pods dir yields an empty
/// list, not an error — nothing has run yet.
pub fn list_pods_from_disk(root: &str) -> std::io::Result<Vec<String>> {
    let pods_dir = get_pods_dir(root);
    let entries = match std::fs::read_dir(&pods_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut uids = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            uids.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    Ok(uids)
}

/// Every per-volume directory a pod has on disk.
///
/// Port of `getPodVolumePathListFromDisk`
/// (`pkg/kubelet/kubelet_getters.go:337-379`). Walks
/// `<pod>/volumes/<plugin>/<volume>`.
///
/// CSI is special-cased exactly as upstream does: a CSI volume's mount lives one
/// level deeper, at `<volume>/mount` (`GetCSIMounterPath`,
/// `pkg/volume/csi/csi_util.go:177-179`), so that path is listed instead — and
/// only when it exists.
pub fn get_pod_volume_path_list_from_disk(
    root: &str,
    pod_uid: &str,
) -> std::io::Result<Vec<PathBuf>> {
    let pod_vol_dir = get_pod_volumes_dir(root, pod_uid);
    let plugin_dirs = match std::fs::read_dir(&pod_vol_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut volumes = Vec::new();
    for plugin_dir in plugin_dirs {
        let plugin_path = plugin_dir?.path();
        let unescaped =
            unescape_qualified_name(&plugin_path.file_name().unwrap().to_string_lossy());
        for volume_dir in std::fs::read_dir(&plugin_path)? {
            let volume_path = volume_dir?.path();
            if unescaped == plugin::CSI {
                let csi_mount_path = volume_path.join("mount");
                if csi_mount_path.exists() {
                    volumes.push(csi_mount_path);
                }
            } else {
                volumes.push(volume_path);
            }
        }
    }
    Ok(volumes)
}

/// Every subpath bind-mount target a pod has on disk.
///
/// Port of `getPodVolumeSubpathListFromDisk`
/// (`pkg/kubelet/kubelet_getters.go:406-447`). Walks the fixed
/// `<volume>/<container name>/<subPathIndex>` shape.
pub fn get_pod_volume_subpath_list_from_disk(
    root: &str,
    pod_uid: &str,
) -> std::io::Result<Vec<PathBuf>> {
    let subpaths_dir = get_pod_volume_subpaths_dir(root, pod_uid);
    let volume_dirs = match std::fs::read_dir(&subpaths_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut subpaths = Vec::new();
    for volume_dir in volume_dirs {
        for container_dir in std::fs::read_dir(volume_dir?.path())? {
            for sub_path in std::fs::read_dir(container_dir?.path())? {
                subpaths.push(sub_path?.path());
            }
        }
    }
    Ok(subpaths)
}

/// `v1.PersistentVolumeMode` of a volume found on disk: which of the pod's
/// directories (`volumes` or `volumeDevices`) it was found under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodVolumeMode {
    Filesystem,
    Block,
}

/// A volume found under a pod directory: upstream's `podVolume`
/// (`reconstruct_common.go:44-50`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodVolume {
    pub pod_uid: String,
    /// The directory name under the plugin directory: the volume's name as per
    /// the `v1.Volume` spec (`volumeSpecName`).
    pub volume_spec_name: String,
    /// `<pod>/volumes/<escaped plugin>/<volume>` or, for a block volume,
    /// `<pod>/volumeDevices/<escaped plugin>/<volume>` (`volumePath`).
    pub volume_path: PathBuf,
    /// The unescaped plugin name, e.g. `kubernetes.io/empty-dir`.
    pub plugin_name: String,
    pub volume_mode: PodVolumeMode,
}

/// The volumes a pod directory holds on disk.
///
/// Port of `getVolumesFromPodDir` (`pkg/kubelet/volumemanager/reconciler/
/// reconstruct_common.go:191-240`) for one pod: its
/// `<pod>/volumes/{escapeQualifiedPluginName}/{volumeName}` walk and its
/// `<pod>/volumeDevices/...` walk (block volumes). Upstream scans every pod
/// under the pods dir in one call; the per-pod split is because the callers
/// ask about one pod at a time. A pod without such a dir has no volumes of
/// that mode, as upstream's `continue` on a failed `ReadDir` says.
pub fn get_volumes_from_pod_dir(root: &str, pod_uid: &str) -> std::io::Result<Vec<PodVolume>> {
    let mut volumes = Vec::new();
    for (volume_mode, volumes_dir) in [
        (
            PodVolumeMode::Filesystem,
            get_pod_volumes_dir(root, pod_uid),
        ),
        (
            PodVolumeMode::Block,
            get_pod_volume_devices_dir(root, pod_uid),
        ),
    ] {
        let plugin_dirs = match std::fs::read_dir(&volumes_dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for plugin_dir in plugin_dirs.flatten() {
            let escaped = plugin_dir.file_name().to_string_lossy().into_owned();
            let plugin_path = volumes_dir.join(&escaped);
            let volume_dirs = match std::fs::read_dir(&plugin_path) {
                Ok(e) => e,
                Err(e) => {
                    tracing::error!(
                        "Could not read volume plugin directory {}: {}",
                        plugin_path.display(),
                        e
                    );
                    continue;
                }
            };
            let plugin_name = unescape_qualified_name(&escaped);
            for volume_dir in volume_dirs.flatten() {
                let volume_spec_name = volume_dir.file_name().to_string_lossy().into_owned();
                volumes.push(PodVolume {
                    pod_uid: pod_uid.to_string(),
                    volume_path: plugin_path.join(&volume_spec_name),
                    volume_spec_name,
                    plugin_name: plugin_name.clone(),
                    volume_mode,
                });
            }
        }
    }
    Ok(volumes)
}

/// Inverse of [`escape_qualified_name`].
///
/// Port of `k8s.io/utils/strings/escape.go::UnescapeQualifiedName`.
fn unescape_qualified_name(name: &str) -> String {
    name.replace('~', "/")
}
