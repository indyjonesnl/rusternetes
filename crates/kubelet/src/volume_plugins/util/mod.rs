//! Port of `pkg/volume/util` — helpers shared by the volume plugins and the
//! volume manager's caches.
//!
//! Only the helpers with a consumer in the volume-manager caches are ported;
//! `pkg/volume/util/util.go` is ~900 lines of which this is a small slice.

pub mod nested_volumes;
pub mod node_expander;
pub mod operation_executor;
pub mod operation_generator;
pub mod resize_util;
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

/// The `volume.ReconstructedVolume{Spec: volume.NewSpecFromVolume(&v1.Volume{
/// Name: name, VolumeSource: ...})}` every non-CSI `ConstructVolumeSpec`
/// returns. `source` is the one-key JSON object of the `VolumeSource` field.
pub(crate) fn reconstructed_volume(
    name: &str,
    source: serde_json::Value,
) -> anyhow::Result<crate::volume_plugins::ReconstructedVolume> {
    let mut volume = source;
    volume["name"] = serde_json::Value::String(name.to_string());
    Ok(crate::volume_plugins::ReconstructedVolume {
        volume: serde_json::from_value(volume)?,
        persistent_volume: None,
    })
}

/// The unmounter of the four wrapper plugins (configMap, secret, downwardAPI,
/// projected).
///
/// Upstream has four identical types — `configMapVolumeUnmounter`,
/// `secretVolumeUnmounter`, `downwardAPIVolumeUnmounter` and
/// `projectedVolumeUnmounter` — each of which is `{volName, podUID, plugin}`
/// and whose `TearDownAt(dir)` is
/// `volumeutil.UnmountViaEmptyDir(dir, host, volName, wrappedVolumeSpec(), podUID)`
/// (`configmap.go:330-332`, `secret.go:317-319`, `downwardapi.go:295-297`,
/// `projected.go:443-457`). One type holds the shared body, as a sibling would.
pub(crate) struct WrappedEmptyDirUnmounter {
    pub(crate) host: std::sync::Arc<dyn crate::volume_plugins::VolumeHost>,
    pub(crate) plugin_name: &'static str,
    pub(crate) vol_name: String,
    pub(crate) pod_uid: String,
}

#[async_trait::async_trait]
impl crate::volume_plugins::Unmounter for WrappedEmptyDirUnmounter {
    /// `getPath(podUID, volName, host)` of the plugin (`configmap.go:70-72`).
    fn get_path(&self) -> String {
        self.host
            .get_pod_volume_dir(&self.pod_uid, self.plugin_name, &self.vol_name)
    }

    async fn tear_down_at(&self, dir: &str) -> anyhow::Result<()> {
        unmount_via_empty_dir(&self.host, dir, &self.vol_name, &self.pod_uid).await
    }
}

/// `emptyDir.getMetaDir` (`empty_dir.go:557-559`) of the emptyDir wrapped for
/// `vol_name`: `<pod>/plugins/kubernetes.io~empty-dir/wrapped_<vol_name>`
/// (`NewWrapperMounter`, `volume_host.go:194-198`).
pub fn wrapped_empty_dir_meta_dir(
    host: &std::sync::Arc<dyn crate::volume_plugins::VolumeHost>,
    vol_name: &str,
    pod_uid: &str,
) -> String {
    let escaped = crate::pod_dirs::escape_qualified_name(crate::pod_dirs::plugin::EMPTY_DIR);
    std::path::Path::new(&host.get_pod_plugin_dir(pod_uid, &escaped))
        .join(format!("wrapped_{vol_name}"))
        .to_string_lossy()
        .into_owned()
}

/// `volumeutil.SetReady` (`pkg/volume/util/util.go:93-106`): MkdirAll the dir
/// at 0750 and touch the `ready` file. Errors are logged, not returned.
pub fn set_ready(dir: &str) {
    use std::os::unix::fs::DirBuilderExt;
    if let Err(e) = std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o750)
        .create(dir)
    {
        tracing::error!("Can't mkdir {dir}: {e}");
        return;
    }
    let ready_file = std::path::Path::new(dir).join("ready");
    if let Err(e) = std::fs::File::create(&ready_file) {
        tracing::error!("Can't touch {}: {e}", ready_file.display());
    }
}

/// Port of `UnmountViaEmptyDir` (`pkg/volume/util/util.go:173-184`): the
/// tear-down of secret, configMap, downwardAPI and projected is delegated to
/// emptyDir, which finds out whether `dir` is a tmpfs mount, unmounts it if so
/// and removes it.
///
/// **Deviation.** Upstream's `host.NewWrapperUnmounter`
/// (`pkg/kubelet/volume_host.go:203-216`) resolves the plugin with
/// `volumePluginMgr.FindPluginBySpec(&spec)` on the wrapped spec, which is
/// always `wrappedVolumeSpec()` — an `EmptyDir{Medium: Memory}` volume — so the
/// answer is always the emptyDir plugin. Our [`VolumeHost`] has no registry
/// back-reference (the registry is built from the host), so the emptyDir plugin
/// is constructed directly. The wrapper name rule (`"wrapped_" + volName`,
/// `volume_host.go:205`) is kept. #2537 adds `NewWrapperMounter`, which needs
/// the same lookup and is the place to introduce the back-reference.
pub async fn unmount_via_empty_dir(
    host: &std::sync::Arc<dyn crate::volume_plugins::VolumeHost>,
    dir: &str,
    vol_name: &str,
    pod_uid: &str,
) -> anyhow::Result<()> {
    tracing::info!("Tearing down volume {vol_name} for pod {pod_uid} at {dir}");
    // Wrap EmptyDir, let it do the teardown.
    let wrapped = crate::volume_plugins::empty_dir::EmptyDirPlugin::new(host.clone())
        .new_unmounter(&format!("wrapped_{vol_name}"), pod_uid)?;
    wrapped.tear_down_at(dir).await
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

/// `NewUnmounter` / `ConstructVolumeSpec` of the plugins that wrap emptyDir
/// (`configmap_test.go`, `secret_test.go`, `downwardapi_test.go`,
/// `projected_test.go` each end their plugin test with `TearDown()` and assert
/// the volume path is gone) and of hostPath (`TearDown` is a no-op,
/// `host_path.go:272-274`).
#[cfg(test)]
mod unmounter_tests {
    use crate::volume_plugins::{
        config_map::ConfigMapPlugin, downward_api::DownwardApiPlugin, host_path::HostPathPlugin,
        projected::ProjectedPlugin, secret::SecretPlugin, KubeletVolumeHost, VolumeHost,
        VolumePlugin,
    };
    use std::sync::Arc;

    fn host(root: &str) -> Arc<dyn VolumeHost> {
        Arc::new(KubeletVolumeHost::new(
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
        let dir = crate::pod_dirs::get_pod_volume_dir(root, "uid-1", plugin.name(), "vol");
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
        assert_eq!(cm.volume.name, "v");
        assert!(cm.volume.config_map.is_some());
        let sec = SecretPlugin::new(h.clone())
            .construct_volume_spec("v", "/p")
            .unwrap();
        assert_eq!(sec.volume.secret.unwrap().secret_name.as_deref(), Some("v"));
        let d = DownwardApiPlugin::new(h.clone())
            .construct_volume_spec("v", "/p")
            .unwrap();
        assert!(d.volume.downward_api.is_some());
        let p = ProjectedPlugin::new(h.clone())
            .construct_volume_spec("v", "/p")
            .unwrap();
        assert!(p.volume.projected.is_some());
        let hp = HostPathPlugin::new(h)
            .construct_volume_spec("v", "/p")
            .unwrap();
        assert_eq!(hp.volume.host_path.unwrap().path, "v");
    }
}
