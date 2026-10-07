//! Port of `pkg/volume/util` — helpers shared by the volume plugins and the
//! volume manager's caches.
//!
//! Only the helpers with a consumer in the volume-manager caches are ported;
//! `pkg/volume/util/util.go` is ~900 lines of which this is a small slice.

pub mod operation_executor;
pub mod operation_generator;
pub mod selinux;
pub mod types;
pub mod volumepathhandler;

use crate::volume_plugins::plugin::{Spec, VolumePlugin};
use crate::volume_plugins::registry::VolumePluginMgr;
use rusternetes_common::resources::volume::PersistentVolumeAccessMode;
use rusternetes_common::resources::{Pod, Volume};
use types::{UniquePodName, UniqueVolumeName};

/// Port of `GetUniquePodName` (`pkg/volume/util/util.go:250-253`):
///
/// ```go
/// func GetUniquePodName(pod *v1.Pod) types.UniquePodName {
///     return types.UniquePodName(pod.UID)
/// }
/// ```
pub fn get_unique_pod_name(pod: &Pod) -> UniquePodName {
    UniquePodName(pod.metadata.uid.clone())
}

/// Port of `GetUniqueVolumeName` (`pkg/volume/util/util.go:255-263`):
///
/// ```go
/// return v1.UniqueVolumeName(fmt.Sprintf("%s/%s", pluginName, volumeName))
/// ```
///
/// `volumeName` must uniquely identify the actual backing device, directory or
/// path — the plugin's `GetVolumeName`, not `spec.Name()`.
pub fn get_unique_volume_name(plugin_name: &str, volume_name: &str) -> UniqueVolumeName {
    UniqueVolumeName(format!("{plugin_name}/{volume_name}"))
}

/// Port of `GetUniqueVolumeNameFromSpecWithPod`
/// (`pkg/volume/util/util.go:265-272`):
///
/// ```go
/// return v1.UniqueVolumeName(
///     fmt.Sprintf("%s/%v-%s", volumePlugin.GetPluginName(), podName, volumeSpec.Name()))
/// ```
///
/// **Pod-scoped.** Two pods referencing "the same" non-attachable,
/// non-device-mountable volume (emptyDir, configMap, secret, downwardAPI,
/// projected) get two distinct cache entries — which is correct, because each
/// pod gets its own copy of the content. Using this for an attachable volume
/// would mean attaching the same disk once per pod.
pub fn get_unique_volume_name_from_spec_with_pod(
    pod_name: &UniquePodName,
    volume_plugin: &dyn VolumePlugin,
    volume_spec: &Spec<'_>,
) -> UniqueVolumeName {
    UniqueVolumeName(format!(
        "{}/{}-{}",
        volume_plugin.name(),
        pod_name,
        volume_spec.name()
    ))
}

/// Port of `GetUniqueVolumeNameFromSpec` (`pkg/volume/util/util.go:274-299`).
///
/// **Pod-independent.** The same backing device gets the same name across
/// every pod that references it, which is what lets one attached disk be
/// shared by several pods in the cache. Using this for a non-attachable volume
/// would make two pods' emptyDirs collide.
///
/// Upstream's nil-plugin arm is unreachable here — a `&dyn VolumePlugin`
/// cannot be nil — so only the empty-name/error arm is ported. Errors are
/// returned as the formatted string because the sole caller embeds them in a
/// larger message.
pub fn get_unique_volume_name_from_spec(
    volume_plugin: &dyn VolumePlugin,
    volume_spec: &Spec<'_>,
) -> Result<UniqueVolumeName, String> {
    let volume_name = match volume_plugin.get_volume_name(volume_spec) {
        Ok(name) if !name.is_empty() => name,
        Ok(_) => {
            return Err(format!(
                "failed to GetVolumeName from volumePlugin for volumeSpec {:?} err=<nil>",
                volume_spec.name()
            ))
        }
        Err(err) => {
            return Err(format!(
                "failed to GetVolumeName from volumePlugin for volumeSpec {:?} err={err}",
                volume_spec.name()
            ))
        }
    };
    Ok(get_unique_volume_name(volume_plugin.name(), &volume_name))
}

/// Port of `IsAttachableVolume` (`pkg/volume/util/util.go:635-645`).
///
/// Upstream also requires `NewAttacher()` to succeed; see
/// [`VolumePlugin::can_attach`](crate::volume_plugins::VolumePlugin::can_attach)
/// for why that check lives inside the predicate here.
pub fn is_attachable_volume(volume_spec: &Spec<'_>, volume_plugin_mgr: &VolumePluginMgr) -> bool {
    volume_plugin_mgr
        .find_attachable_plugin_by_spec(volume_spec)
        .is_some()
}

/// Port of `IsDeviceMountableVolume` (`pkg/volume/util/util.go:648-658`).
pub fn is_device_mountable_volume(
    volume_spec: &Spec<'_>,
    volume_plugin_mgr: &VolumePluginMgr,
) -> bool {
    volume_plugin_mgr
        .find_device_mountable_plugin_by_spec(volume_spec)
        .is_some()
}

/// Port of `IsLocalEphemeralVolume` (`pkg/volume/util/util.go:498-502`):
///
/// ```go
/// return volume.GitRepo != nil ||
///     (volume.EmptyDir != nil && volume.EmptyDir.Medium == v1.StorageMediumDefault) ||
///     volume.ConfigMap != nil
/// ```
///
/// **Deviation:** the `GitRepo` arm is dropped because
/// `rusternetes_common::resources::Volume` has no `gitRepo` field — the source
/// was removed from the API and this project never modelled it. Every other
/// arm is verbatim; `StorageMediumDefault` is the empty string
/// (`staging/src/k8s.io/api/core/v1/types.go`), which is what an absent
/// `medium` decodes to.
pub fn is_local_ephemeral_volume(volume: &Volume) -> bool {
    volume
        .empty_dir
        .as_ref()
        .is_some_and(|ed| ed.medium.as_deref().unwrap_or("").is_empty())
        || volume.config_map.is_some()
}

/// Port of `ContainsAccessMode` (`pkg/volume/util/util.go:222-229`).
pub fn contains_access_mode(
    modes: &[PersistentVolumeAccessMode],
    mode: &PersistentVolumeAccessMode,
) -> bool {
    modes.iter().any(|m| m == mode)
}

/// Port of `FsUserFrom` (`pkg/volume/util/util.go:541-557`): the pod's single
/// effective `runAsUser`, or `None` when any container leaves it unset or two
/// containers disagree.
///
/// Visits `podutil.AllFeatureEnabledContainers()` — init, regular and
/// ephemeral containers. `DetermineEffectiveRunAsUser`
/// (`pkg/securitycontext/util.go:127-141`): the container's `runAsUser` wins
/// over the pod's.
pub fn fs_user_from(pod: &Pod) -> Option<i64> {
    let spec = pod.spec.as_ref()?;
    let pod_run_as_user = spec.security_context.as_ref().and_then(|sc| sc.run_as_user);
    let container_users = spec
        .init_containers
        .iter()
        .flatten()
        .map(|c| c.security_context.as_ref().and_then(|sc| sc.run_as_user))
        .chain(
            spec.containers
                .iter()
                .map(|c| c.security_context.as_ref().and_then(|sc| sc.run_as_user)),
        )
        .chain(
            spec.ephemeral_containers
                .iter()
                .flatten()
                .map(|c| c.security_context.as_ref().and_then(|sc| sc.run_as_user)),
        );
    let mut fs_user: Option<i64> = None;
    for container_user in container_users {
        // `!ok || (fsUser != nil && *fsUser != *runAsUser)` => nil, stop.
        let effective = container_user.or(pod_run_as_user)?;
        match fs_user {
            Some(u) if u != effective => return None,
            _ => fs_user = Some(effective),
        }
    }
    fs_user
}

/// The `fsGroup` the operation generator hands a mounter
/// (`pkg/volume/util/operationexecutor/operation_generator.go:501-509`):
/// `pod.Spec.SecurityContext.FSGroup`.
pub fn fs_group_from(pod: &Pod) -> Option<i64> {
    pod.spec
        .as_ref()
        .and_then(|s| s.security_context.as_ref())
        .and_then(|sc| sc.fs_group)
}

#[cfg(test)]
mod fs_user_tests {
    use super::*;

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
