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
const VOLUMES_DIR_NAME: &str = "volumes";

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
}

/// Convert a plugin name, which contains a `/`, into a form safe to use on
/// disk. Upstream uses `~` rather than `:` "in case we ever use a filesystem
/// that doesn't allow `:`".
///
/// Port of `k8s.io/utils/strings/escape.go::EscapeQualifiedName`.
pub fn escape_qualified_name(name: &str) -> String {
    name.replace('/', "~")
}

/// Rusternetes-specific: the plugin segment used for volume types we do not
/// implement natively (nfs, iscsi, image, …).
///
/// **This is a deliberate deviation from upstream** (CLAUDE.md rule 8).
/// Upstream's `FindPluginBySpec` returns an error for an unsupported volume and
/// the pod fails to start; `create_volume` instead provisions an empty
/// directory so the pod can run. That fallback predates this module — it is
/// preserved here rather than introduced. The `rusternetes.io/` prefix
/// guarantees the segment can never collide with a real `kubernetes.io/` plugin
/// if one of these types is implemented properly later.
pub const UNSUPPORTED_PLUGIN: &str = "rusternetes.io/unsupported";

/// Resolve the volume plugin that owns a volume, and hence the plugin segment
/// of its on-disk path.
///
/// Upstream resolves this by asking every registered plugin `CanSupport(spec)`
/// (`pkg/volume/plugins.go::FindPluginBySpec:634`). We have no plugin registry,
/// so the equivalent is a match over the volume source. The arm order mirrors
/// the branch order in [`crate::volumes::VolumeManager::create_volume`] so the
/// directory always agrees with the branch that actually populates it.
///
/// Returns [`UNSUPPORTED_PLUGIN`] for volume sources that reach
/// `create_volume`'s fallback. `hostPath`, `persistentVolumeClaim` and
/// `ephemeral` volumes never get a directory under the pod dir — they resolve
/// to a path elsewhere on the host — so they are not represented here.
pub fn plugin_for_volume(volume: &rusternetes_common::resources::Volume) -> &'static str {
    if volume.empty_dir.is_some() {
        plugin::EMPTY_DIR
    } else if volume.config_map.is_some() {
        plugin::CONFIG_MAP
    } else if volume.secret.is_some() {
        plugin::SECRET
    } else if volume.downward_api.is_some() {
        plugin::DOWNWARD_API
    } else if volume.csi.is_some() {
        plugin::CSI
    } else if volume.projected.is_some() {
        plugin::PROJECTED
    } else {
        UNSUPPORTED_PLUGIN
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_volume_dir_follows_the_upstream_layout() {
        assert_eq!(
            get_pod_volume_dir("/var/lib/kubelet", "uid-1", plugin::CONFIG_MAP, "cfg"),
            Path::new("/var/lib/kubelet/pods/uid-1/volumes/kubernetes.io~configmap/cfg"),
        );
    }

    #[test]
    fn a_plugin_name_is_escaped_on_disk() {
        // EscapeQualifiedName replaces every "/" with "~".
        assert_eq!(
            escape_qualified_name("kubernetes.io/empty-dir"),
            "kubernetes.io~empty-dir"
        );
        assert_eq!(escape_qualified_name("a/b/c"), "a~b~c");
        assert_eq!(escape_qualified_name("no-slash"), "no-slash");
    }

    /// The reason upstream keys on UID rather than name: a pod recreated under
    /// the same name is a different pod, and must not inherit the previous
    /// incarnation's files.
    #[test]
    fn a_recreated_pod_with_the_same_name_gets_a_distinct_dir() {
        let first = get_pod_volume_dir("/root", "uid-aaa", plugin::EMPTY_DIR, "scratch");
        let second = get_pod_volume_dir("/root", "uid-bbb", plugin::EMPTY_DIR, "scratch");
        assert_ne!(first, second);
    }

    /// Two volumes of different kinds may share a name; the plugin segment is
    /// what keeps them apart on disk.
    #[test]
    fn same_volume_name_under_different_plugins_does_not_collide() {
        let cm = get_pod_volume_dir("/root", "uid-1", plugin::CONFIG_MAP, "shared");
        let secret = get_pod_volume_dir("/root", "uid-1", plugin::SECRET, "shared");
        assert_ne!(cm, secret);
    }

    #[test]
    fn the_per_pod_dirs_sit_under_the_pods_dir() {
        assert_eq!(get_pods_dir("/root"), Path::new("/root/pods"));
        assert_eq!(get_pod_dir("/root", "u"), Path::new("/root/pods/u"));
        assert_eq!(
            get_pod_volumes_dir("/root", "u"),
            Path::new("/root/pods/u/volumes")
        );
    }
}
