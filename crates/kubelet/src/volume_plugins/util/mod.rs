//! Port of `pkg/volume/util` — helpers shared by the volume plugins and the
//! volume manager's caches.
//!
//! Only the helpers with a consumer in the volume-manager caches are ported;
//! `pkg/volume/util/util.go` is ~900 lines of which this is a small slice.

pub mod device_util;
pub mod exec;
pub mod exponential_backoff;
pub mod hostutil;
pub mod key_mutex;
pub mod metrics;
pub mod mount_interface;
pub mod mount_list;
pub mod mount_ops;
pub mod mount_utils;
pub mod nested_pending_operations;
pub mod nested_volumes;
pub mod node_expander;
pub mod operation_executor;
pub mod operation_generator;
pub mod resize_util;
pub mod resizefs;
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
/// `volume.GitRepo` is `Volume.legacy_sources.git_repo` here (the deprecated
/// source is carried losslessly in `LegacyVolumeSources`). `StorageMediumDefault`
/// is the empty string (`staging/src/k8s.io/api/core/v1/types.go`), which is
/// what an absent `medium` decodes to.
pub fn is_local_ephemeral_volume(volume: &Volume) -> bool {
    volume.legacy_sources.git_repo.is_some()
        || volume
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

/// The effective SELinux options of every container that mounts
/// `volume_name`: the SELinux half of `GetPodVolumeNames(pod, true)`
/// (`pkg/volume/util/util.go:507-537`).
///
/// Visits init, regular and ephemeral containers. The effective options are
/// the container's own, else the pod's (`DetermineEffectiveSecurityContext`,
/// `pkg/securitycontext/util.go:44-62`); a container with neither adds
/// nothing (`seLinuxOptions != nil` at `util.go:524`).
pub fn selinux_container_contexts(
    pod: &Pod,
    volume_name: &str,
) -> Vec<Option<rusternetes_common::resources::pod::SELinuxOptions>> {
    let Some(spec) = pod.spec.as_ref() else {
        return Vec::new();
    };
    let pod_opts = spec
        .security_context
        .as_ref()
        .and_then(|sc| sc.se_linux_options.clone());
    let mut out = Vec::new();
    let mut visit =
        |mounts: &Option<Vec<rusternetes_common::resources::pod::VolumeMount>>,
         opts: Option<rusternetes_common::resources::pod::SELinuxOptions>| {
            let Some(opts) = opts.or_else(|| pod_opts.clone()) else {
                return;
            };
            for m in mounts.iter().flatten() {
                if m.name == volume_name {
                    out.push(Some(opts.clone()));
                }
            }
        };
    for c in spec.init_containers.iter().flatten() {
        visit(
            &c.volume_mounts,
            c.security_context
                .as_ref()
                .and_then(|sc| sc.se_linux_options.clone()),
        );
    }
    for c in &spec.containers {
        visit(
            &c.volume_mounts,
            c.security_context
                .as_ref()
                .and_then(|sc| sc.se_linux_options.clone()),
        );
    }
    for c in spec.ephemeral_containers.iter().flatten() {
        visit(
            &c.volume_mounts,
            c.security_context
                .as_ref()
                .and_then(|sc| sc.se_linux_options.clone()),
        );
    }
    out
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

/// `v1.MountOptionAnnotation` (`staging/src/k8s.io/api/core/v1/annotation_key_constants.go`).
pub const MOUNT_OPTION_ANNOTATION: &str = "volume.beta.kubernetes.io/mount-options";

/// Port of `MountOptionFromSpec` (`pkg/volume/util/util.go:187-203`): the
/// PersistentVolume's mount options (beta annotation first, then
/// `spec.mountOptions`) joined with `options`; `options` alone for a spec
/// without a PV or without mount options.
pub fn mount_option_from_spec(spec: &Spec<'_>, options: &[String]) -> Vec<String> {
    if let Some(pv) = spec.persistent_volume {
        // Use beta annotation first
        if let Some(mo) = pv
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(MOUNT_OPTION_ANNOTATION))
        {
            let mo_list: Vec<String> = mo.split(',').map(str::to_string).collect();
            return join_mount_options(&mo_list, options);
        }
        if let Some(mo) = pv.spec.mount_options.as_ref().filter(|m| !m.is_empty()) {
            return join_mount_options(mo, options);
        }
    }
    options.to_vec()
}

/// Port of `JoinMountOptions` (`pkg/volume/util/util.go:205-219`): the union of
/// both lists, sorted and de-duplicated (`sets.List`); empty user options are
/// dropped, empty system options are kept.
pub fn join_mount_options(user_options: &[String], system_options: &[String]) -> Vec<String> {
    let mut all: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for o in user_options {
        if !o.is_empty() {
            all.insert(o.clone());
        }
    }
    for o in system_options {
        all.insert(o.clone());
    }
    all.into_iter().collect()
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
        selinux_mount_context: String::new(),
        volume: Some(serde_json::from_value(volume)?),
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
    /// Embedded `volume.MetricsProvider`: `NewCachedMetrics(NewMetricsDu(..))`
    /// for the four plugins above (`configmap.go:115`, `secret.go:119`,
    /// `downwardapi.go:116`, `projected.go:132`); `MetricsNil` for gitRepo
    /// (`git_repo.go:141`).
    pub(crate) metrics: Box<dyn crate::volume_plugins::plugin::MetricsProvider + Send + Sync>,
}

impl WrappedEmptyDirUnmounter {
    /// The four DU-backed plugins' unmounter.
    pub(crate) fn new(
        host: std::sync::Arc<dyn crate::volume_plugins::VolumeHost>,
        plugin_name: &'static str,
        vol_name: &str,
        pod_uid: &str,
    ) -> Self {
        let path = host.get_pod_volume_dir(pod_uid, plugin_name, vol_name);
        Self {
            host,
            plugin_name,
            vol_name: vol_name.to_string(),
            pod_uid: pod_uid.to_string(),
            metrics: Box::new(crate::volume_plugins::metrics_cached::cached_du(&path)),
        }
    }
}

impl crate::volume_plugins::plugin::MetricsProvider for WrappedEmptyDirUnmounter {
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        self.metrics.get_metrics()
    }
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

/// `VolumeHost.NewWrapperMounter` (`pkg/kubelet/volume_host.go:190-199`): the
/// mounter of the emptyDir named `wrapped_<vol_name>` built from the caller's
/// `wrappedVolumeSpec()`. `medium` is that spec's `EmptyDir.Medium`:
/// `Some("Memory")` for secret (`secret.go:52-56`), downwardAPI
/// (`downwardapi.go:54-58`) and projected (`projected.go:59-67`); `None` for
/// configMap (`configmap.go:165-172`: "This should be on a tmpfs instead of the
/// local disk") and git_repo. Its `SetUpAt` creates the volume dir, mounts the
/// tmpfs for `Memory`, and ends with `volumeutil.SetReady(ed.getMetaDir())`
/// (`empty_dir.go:284`).
///
/// Deviation (see [`unmount_via_empty_dir`]): upstream resolves the plugin by
/// `FindPluginBySpec`; the wrapped spec always selects emptyDir, so it is
/// constructed directly.
pub async fn new_wrapper_mounter(
    host: &std::sync::Arc<dyn crate::volume_plugins::VolumeHost>,
    vol_name: &str,
    medium: Option<&str>,
    pod: &rusternetes_common::resources::Pod,
) -> anyhow::Result<Box<dyn crate::volume_plugins::Mounter>> {
    use crate::volume_plugins::VolumePlugin;
    let mut empty_dir = serde_json::json!({});
    if let Some(m) = medium {
        empty_dir["medium"] = m.into();
    }
    let volume: rusternetes_common::resources::Volume = serde_json::from_value(
        serde_json::json!({"name": format!("wrapped_{vol_name}"), "emptyDir": empty_dir}),
    )
    .expect("static emptyDir volume");
    crate::volume_plugins::empty_dir::EmptyDirPlugin::new(host.clone())
        .new_mounter(
            &crate::volume_plugins::Spec {
                volume: &volume,
                persistent_volume: None,
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            },
            pod,
        )
        .await
}

/// `volumeutil.IsReady` (`pkg/volume/util/util.go:75-90`): true when `dir`
/// holds a regular file called `ready`.
pub fn is_ready(dir: &str) -> bool {
    let ready_file = std::path::Path::new(dir).join("ready");
    match std::fs::metadata(&ready_file) {
        Err(_) => false,
        Ok(m) if !m.is_file() => {
            tracing::error!("ready-file is not a file: {}", ready_file.display());
            false
        }
        Ok(_) => true,
    }
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

/// `kubernetesPluginPathPrefix` (`pkg/volume/util/util.go:68-69`).
const KUBERNETES_PLUGIN_PATH_PREFIX: &str = "/plugins/kubernetes.io/";

/// Port of `HasMountRefs` (`pkg/volume/util/util.go:559-593`).
///
/// A ref is a real external reference when it does not contain the
/// `/plugins/kubernetes.io/...` suffix of `mount_path` (the kubelet's own
/// bind mounts of the same device, possibly under a different root). With no
/// such segment the whole `mount_path` is searched for. (Upstream's
/// "gci mounter" workaround, kept verbatim.)
pub fn has_mount_refs(mount_path: &str, mount_refs: &[String]) -> bool {
    let path_to_find = match mount_path.find(KUBERNETES_PLUGIN_PATH_PREFIX) {
        Some(i) => &mount_path[i..],
        None => mount_path,
    };
    mount_refs.iter().any(|r| !r.contains(path_to_find))
}

/// The refs gate of `GenerateUnmountDeviceFunc`
/// (`operation_generator.go:846-856`): `Err` when the refs lookup failed or
/// the device mount path is still referenced elsewhere.
///
/// ```go
/// if err != nil || util.HasMountRefs(deviceMountPath, refs) {
///     if err == nil {
///         err = fmt.Errorf("the device mount path %q is still mounted by other references %v", deviceMountPath, refs)
///     }
///     ... GenerateError("GetDeviceMountRefs check failed", err)
/// ```
pub fn check_device_mount_refs(
    device_mount_path: &str,
    refs: anyhow::Result<Vec<String>>,
) -> anyhow::Result<()> {
    let refs = refs?;
    if has_mount_refs(device_mount_path, &refs) {
        // `%q` / `%v` of a `[]string`: Go-quoted path, `[a b]`.
        anyhow::bail!(
            "the device mount path {device_mount_path:?} is still mounted by other references [{}]",
            refs.join(" ")
        );
    }
    Ok(())
}
