//! Runtime-agnostic volume provisioning.
//!
//! [`VolumeManager`] owns the logic that materialises pod volumes on the host
//! filesystem (configMap / secret / projected / downwardAPI / emptyDir /
//! hostPath) plus the periodic resync/refresh of mutable sources. It is
//! deliberately free of any container-runtime coupling so that the CRI runtime
//! ([`crate::cri_runtime::CriContainerRuntime`]) can reuse it; behavior is
//! identical to when this code lived inline in `runtime.rs`.

use anyhow::{Context, Result};
use rusternetes_common::resources::{
    ConfigMap, KeyToPath, PersistentVolume, PersistentVolumeClaim, Pod,
};
use rusternetes_storage::{build_key, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

// Free helpers that stay in `runtime.rs` (they are not runtime-specific but are
// shared with non-volume code paths there). Imported so the moved bodies keep
// calling them by their bare names, verbatim.
use crate::atomic_writer::FileProjection;
use crate::volume_plugins::VolumePlugin;

/// Build the projection payload (relative user-visible path -> bytes) for a
/// ConfigMap volume, honoring `items` (specific keys → mapped paths) or, when
/// absent, every key from `data` + `binaryData`. This is the SINGLE source of
/// truth for what a ConfigMap volume should contain, shared by the initial
/// mount and every re-projection so they all feed the same bytes to the
/// AtomicWriter (and therefore no-op identically when unchanged).
pub(crate) fn build_configmap_payload(
    configmap: &ConfigMap,
    items: Option<&Vec<KeyToPath>>,
    _configmap_name: &str,
    is_optional: bool,
    default_mode: u32,
) -> Result<std::collections::BTreeMap<String, FileProjection>> {
    let mut payload: std::collections::BTreeMap<String, FileProjection> =
        std::collections::BTreeMap::new();
    // `if len(mappings) == 0` (`pkg/volume/configmap/configmap.go:207`): an
    // empty `items` list projects every key, same as an absent one.
    if let Some(items) = items.filter(|i| !i.is_empty()) {
        for item in items {
            // `items[].mode` wins over the volume `defaultMode` for this key
            // alone — upstream carries it per file in `FileProjection.Mode`
            // (`pkg/volume/util/atomic_writer.go:64-68`), which is what the
            // "with mappings and Item mode set" Conformance spec asserts.
            let mode = item.mode.map(|m| m as u32).unwrap_or(default_mode);
            if let Some(v) = configmap.data.as_ref().and_then(|d| d.get(&item.key)) {
                payload.insert(
                    item.path.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone().into_bytes(),
                        mode,
                    },
                );
            } else if let Some(v) = configmap
                .binary_data
                .as_ref()
                .and_then(|d| d.get(&item.key))
            {
                payload.insert(
                    item.path.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone(),
                        mode,
                    },
                );
            } else if !is_optional {
                // `MakePayload` (`configmap.go:222-229`): a mapped key absent
                // from data and binaryData fails the mount unless optional.
                anyhow::bail!("configmap references non-existent config key: {}", item.key);
            }
        }
    } else {
        if let Some(data) = &configmap.data {
            for (k, v) in data {
                payload.insert(
                    k.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone().into_bytes(),
                        mode: default_mode,
                    },
                );
            }
        }
        if let Some(bin) = &configmap.binary_data {
            for (k, v) in bin {
                payload.insert(
                    k.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone(),
                        mode: default_mode,
                    },
                );
            }
        }
    }
    Ok(payload)
}

/// PV annotation carrying a supplemental GID granted to pods that mount the
/// volume. Upstream `pkg/volume/util/util.go` `VolumeGidAnnotationKey`.
const VOLUME_GID_ANNOTATION_KEY: &str = "pv.beta.kubernetes.io/gid";

/// Provisions and maintains pod volumes on the host filesystem, independent of
/// any container runtime. See the module docs.
#[derive(Clone)]
pub struct VolumeManager {
    pub volumes_base_path: String,
    pub storage: Option<Arc<rusternetes_storage::StorageBackend>>,
    pub token_manager: rusternetes_common::auth::TokenManager,
    /// The node's `status.allocatable`, used to default an unset downwardAPI
    /// `limits.*` the way upstream `defaultPodLimitsForDownwardAPI`
    /// (`pkg/kubelet/kubelet_resources.go:43-47`) does. Seeded from the same
    /// [`crate::kubelet::node_allocatable_map`] the kubelet posts in NodeStatus,
    /// so a volume file and the NodeStatus can never disagree.
    pub node_allocatable: std::collections::HashMap<String, String>,
    /// The plugin registry. Built in `new` from the same host every plugin
    /// receives, so a plugin's directory can never disagree with the
    /// manager's. `Arc`-wrapped (rather than the bare `VolumePluginMgr` Task 3's
    /// brief showed) because `VolumeManager` is `#[derive(Clone)]` —
    /// `CriContainerRuntime` clones it per attach — and `VolumePluginMgr`'s
    /// `Vec<Box<dyn VolumePlugin>>` cannot itself derive `Clone`.
    pub plugin_mgr: Arc<crate::volume_plugins::VolumePluginMgr>,
    /// A CSI plugin over the same host, for the CSI-specific device methods
    /// (`unmount_device`, `list_staged_devices`) that are not part of
    /// `VolumePlugin`. `plugin_mgr` holds the plugin as a `dyn VolumePlugin`.
    pub(crate) csi_plugin: Arc<crate::volume_plugins::csi::CsiPlugin>,
    /// Each mounted pod volume's `Mounter.GetAttributes()` result plus the
    /// `SELinuxLabeled` bit, keyed by (pod UID, volume name). Stands in for the
    /// `VolumeInfo{Mounter, SELinuxLabeled}` map upstream's `makeMounts` reads
    /// (`pkg/kubelet/container/runtime.go` `VolumeInfo`;
    /// `kubelet_pods.go:296`, `:392`). `Arc`-shared because the manager is
    /// `Clone`d per runtime attach and every clone must see one record.
    mounted: Arc<std::sync::Mutex<HashMap<(String, String), MountedVolume>>>,
    /// The kubelet's pod certificate manager, the same one the volume host
    /// hands to the projected plugin (`kubeletVolumeHost.podCertificateManager`,
    /// `pkg/kubelet/volume_host.go:86`). Resync re-asks it for the bundle of
    /// each `podCertificate` source so a rotated credential reaches the mount.
    pod_certificate_manager: Option<Arc<dyn crate::podcertificate::Manager>>,
}

/// `kubecontainer.VolumeInfo` as far as `makeMounts` reads it.
#[derive(Clone, Copy, Debug, Default)]
struct MountedVolume {
    attributes: crate::volume_plugins::plugin::Attributes,
    selinux_labeled: bool,
}

/// `pvcSource.ReadOnly` as `createVolumeSpec` passes it to
/// `NewSpecFromPersistentVolume(pv, pvcReadOnly)`
/// (`pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go:461`,
/// `:588`). An `ephemeral` volume carries no such flag (`:432-441` builds the
/// claim source from the generated name alone), so it is false.
fn pvc_read_only(volume: &rusternetes_common::resources::Volume) -> bool {
    volume
        .persistent_volume_claim
        .as_ref()
        .and_then(|c| c.read_only)
        .unwrap_or(false)
}

impl VolumeManager {
    /// Construct a `VolumeManager` from the same three values the bollard
    /// `ContainerRuntime` already carries.
    pub fn new(
        volumes_base_path: String,
        storage: Option<Arc<rusternetes_storage::StorageBackend>>,
        token_manager: rusternetes_common::auth::TokenManager,
    ) -> Self {
        Self::new_with_pod_certificate_manager(volumes_base_path, storage, token_manager, None)
    }

    /// [`VolumeManager::new`] plus the kubelet's pod certificate manager, which
    /// the volume host hands to the projected `podCertificate` source
    /// (`kubeletVolumeHost.podCertificateManager`, `pkg/kubelet/volume_host.go:86`).
    pub fn new_with_pod_certificate_manager(
        volumes_base_path: String,
        storage: Option<Arc<rusternetes_storage::StorageBackend>>,
        token_manager: rusternetes_common::auth::TokenManager,
        pod_certificate_manager: Option<Arc<dyn crate::podcertificate::Manager>>,
    ) -> Self {
        let node_allocatable = crate::kubelet::node_allocatable_map();
        let mut kubelet_host = crate::volume_plugins::KubeletVolumeHost::new(
            volumes_base_path.clone(),
            storage.clone(),
            token_manager.clone(),
            node_allocatable.clone(),
        );
        if let Some(m) = pod_certificate_manager.clone() {
            kubelet_host = kubelet_host.with_pod_certificate_manager(m);
        }
        let host: Arc<dyn crate::volume_plugins::VolumeHost> = Arc::new(kubelet_host);
        let plugin_mgr = Arc::new(crate::volume_plugins::VolumePluginMgr::new(vec![
            Box::new(crate::volume_plugins::empty_dir::EmptyDirPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::host_path::HostPathPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::local::LocalPlugin::new(host.clone())),
            Box::new(crate::volume_plugins::config_map::ConfigMapPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::csi::CsiPlugin::new(host.clone())),
            Box::new(crate::volume_plugins::secret::SecretPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::downward_api::DownwardApiPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::projected::ProjectedPlugin::new(
                host.clone(),
            )),
        ]));
        Self {
            volumes_base_path,
            storage,
            token_manager,
            node_allocatable,
            plugin_mgr,
            csi_plugin: Arc::new(crate::volume_plugins::csi::CsiPlugin::new(host)),
            mounted: Arc::default(),
            pod_certificate_manager,
        }
    }

    /// The `makeMounts` read of each mounted volume's attributes for one
    /// container, keyed by volume name (`pkg/kubelet/kubelet_pods.go:296`,
    /// `:392`): `read_only` is `Attributes.ReadOnly` (`mustMountRO`);
    /// `selinux_relabel` is `Managed && SELinuxRelabel && !SELinuxLabeled`,
    /// and the volume is then marked labeled so a later container's mount of
    /// it is not relabelled again. A volume with no record (not mounted by this
    /// manager) is absent, as upstream errors for a missing `vol.Mounter`; the
    /// mount is then left exactly as the container spec declared it.
    pub fn mount_attributes(
        &self,
        pod: &Pod,
        container: &rusternetes_common::resources::Container,
    ) -> HashMap<String, crate::cri_runtime::translate::MountAttrs> {
        let mut out = HashMap::new();
        let mut mounted = self.mounted.lock().unwrap();
        for vm in container.volume_mounts.iter().flatten() {
            let Some(vol) = mounted.get_mut(&(pod.metadata.uid.clone(), vm.name.clone())) else {
                continue;
            };
            let a = vol.attributes;
            let mut relabel = false;
            if a.managed && a.selinux_relabel && !vol.selinux_labeled {
                vol.selinux_labeled = true;
                relabel = true;
            }
            let entry =
                out.entry(vm.name.clone())
                    .or_insert(crate::cri_runtime::translate::MountAttrs {
                        read_only: a.read_only,
                        selinux_relabel: false,
                    });
            entry.selinux_relabel |= relabel;
        }
        out
    }

    /// Rebuild the mount-attribute record of every pod volume that has none
    /// (after a kubelet restart, or when `start_container` runs without
    /// `create_pod_volumes` in this process).
    ///
    /// Port of the Mounter half of `reconstructVolume`
    /// (`pkg/kubelet/volumemanager/reconciler/reconstruct_common.go:352`,
    /// `volumeMounter, err = plugin.NewMounter(volumeSpec, pod)`): the mounter
    /// is built from the spec WITHOUT `SetUp`, and `makeMounts` then reads its
    /// `GetAttributes()` (`kubelet_pods.go:296`). An existing record is kept
    /// (upstream's `MarkVolumeAsMounted` refreshes the mounter, but never
    /// resets `SELinuxLabeled`). As upstream's `reconstructVolume` returns the
    /// error and the volume is simply not reconstructed, a volume whose
    /// mounter cannot be built is left without a record (logged).
    pub async fn reconstruct_mount_attributes(&self, pod: &Pod) {
        let Some(spec) = pod.spec.as_ref() else {
            return;
        };
        for volume in spec.volumes.iter().flatten() {
            let key = (pod.metadata.uid.clone(), volume.name.clone());
            if self.mounted.lock().unwrap().contains_key(&key) {
                continue;
            }
            let pv = match self.resolve_persistent_volume(pod, volume).await {
                Ok(pv) => pv,
                Err(e) => {
                    warn!("reconstruct mount attributes: volume {}: {e}", volume.name);
                    continue;
                }
            };
            let vspec = crate::volume_plugins::Spec {
                volume,
                persistent_volume: pv.as_ref(),
                read_only: pv.is_some() && pvc_read_only(volume),
            };
            let Ok(plugin) = self.plugin_mgr.find_plugin_by_spec(&vspec) else {
                continue;
            };
            match plugin.new_mounter(&vspec, pod).await {
                Ok(mounter) => {
                    self.mounted
                        .lock()
                        .unwrap()
                        .entry(key)
                        .or_insert_with(|| MountedVolume {
                            attributes: mounter.get_attributes(),
                            selinux_labeled: false,
                        });
                }
                Err(e) => warn!(
                    "reconstructVolume.NewMounter failed for volume {} pod {}: {e}",
                    volume.name, pod.metadata.name
                ),
            }
        }
    }

    /// Whether a pod still has volumes mounted, in which case its directory
    /// must not be touched.
    ///
    /// Port of `podVolumesExist` (`pkg/kubelet/kubelet_volumes.go:78-96`) built
    /// on `getMountedVolumePathListFromDisk` (`kubelet_getters.go:381-402`).
    ///
    /// Any error answers **true**. That is upstream's rule, not caution added
    /// here: "If checking returns error, podVolumesExist will return true which
    /// means we consider volumes might exist and requires further checking."
    /// Deleting a directory whose volumes are still mounted can corrupt data,
    /// so an unreadable path must never be read as "safe to remove".
    ///
    /// Upstream also consults the volume manager's in-memory record
    /// (`HasPossiblyMountedVolumesForPod`) before going to disk. We have no such
    /// record, so this is the disk check alone — noted as a gap rather than
    /// papered over: it makes the guard weaker than upstream's, never stronger.
    fn pod_volumes_exist(&self, pod_uid: &str) -> bool {
        let volume_paths = match crate::pod_dirs::get_pod_volume_path_list_from_disk(
            &self.volumes_base_path,
            pod_uid,
        ) {
            Ok(paths) => paths,
            Err(e) => {
                warn!(
                    "Pod {} found, but error occurred during checking mounted volumes: {}",
                    pod_uid, e
                );
                return true;
            }
        };
        for path in &volume_paths {
            match crate::pod_dirs::is_likely_not_mount_point(path) {
                Ok(true) => {}
                Ok(false) => {
                    debug!(
                        "Pod {} found, but volumes are still mounted on disk: {}",
                        pod_uid,
                        path.display()
                    );
                    return true;
                }
                Err(e) => {
                    warn!("Fail to check mount point {}: {}", path.display(), e);
                    return true;
                }
            }
        }
        false
    }

    /// Remove a pod's volumes directory and its subdirectories.
    ///
    /// Port of `removeOrphanedPodVolumeDirs`
    /// (`pkg/kubelet/kubelet_volumes.go:119-165`). Returns every error it hit;
    /// an empty vec means the volumes dir is gone.
    ///
    /// Each individual volume path is removed with `rmdir`, never a recursive
    /// delete: under normal conditions these are empty mount points, and `rmdir`
    /// failing on a non-empty directory is what stops us deleting the contents
    /// of something still mounted.
    fn remove_orphaned_pod_volume_dirs(&self, pod_uid: &str) -> Vec<String> {
        let root = &self.volumes_base_path;
        let mut errors = Vec::new();

        // If there are still volume directories, attempt to rmdir them.
        match crate::pod_dirs::get_pod_volume_path_list_from_disk(root, pod_uid) {
            Ok(volume_paths) => {
                for volume_path in volume_paths {
                    if let Err(e) = std::fs::remove_dir(&volume_path) {
                        errors.push(format!(
                            "orphaned pod {} found, but failed to rmdir() volume at path {}: {}",
                            pod_uid,
                            volume_path.display(),
                            e
                        ));
                    } else {
                        info!(
                            "Cleaned up orphaned volume from pod {}: {}",
                            pod_uid,
                            volume_path.display()
                        );
                    }
                }
            }
            Err(e) => {
                errors.push(format!(
                    "orphaned pod {pod_uid} found, but error occurred during reading volume dir from disk: {e}"
                ));
                return errors;
            }
        }

        // If there are any volume-subpaths, attempt to remove them. These may be
        // bind mounts of a file OR a directory, so plain remove, not rmdir.
        match crate::pod_dirs::get_pod_volume_subpath_list_from_disk(root, pod_uid) {
            Ok(subpaths) => {
                for subpath in subpaths {
                    let removed =
                        std::fs::remove_file(&subpath).or_else(|_| std::fs::remove_dir(&subpath));
                    if let Err(e) = removed {
                        errors.push(format!(
                            "orphaned pod {} found, but failed to rmdir() subpath at path {}: {}",
                            pod_uid,
                            subpath.display(),
                            e
                        ));
                    } else {
                        info!(
                            "Cleaned up orphaned volume subpath from pod {}: {}",
                            pod_uid,
                            subpath.display()
                        );
                    }
                }
            }
            Err(e) => {
                errors.push(format!(
                    "orphaned pod {pod_uid} found, but error occurred during reading of volume-subpaths dir from disk: {e}"
                ));
                return errors;
            }
        }

        // Remove any remaining subdirectories along with the volumes directory
        // itself. Fails if any regular file is encountered.
        let pod_vol_dir = crate::pod_dirs::get_pod_volumes_dir(root, pod_uid);
        match crate::removeall::remove_dirs_one_filesystem(&pod_vol_dir) {
            Ok(()) => info!(
                "Cleaned up orphaned pod volumes dir for {}: {}",
                pod_uid,
                pod_vol_dir.display()
            ),
            Err(e) => errors.push(format!(
                "orphaned pod {pod_uid} found, but error occurred when trying to remove the volumes dir: {e}"
            )),
        }

        errors
    }

    /// Tear down every volume of the pods that are gone.
    ///
    /// Drives `Unmounter.TearDown` the way upstream's volume manager does: the
    /// reconciler's `unmountVolumes`
    /// (`pkg/kubelet/volumemanager/reconciler/reconciler_common.go:148`) runs
    /// `UnmountVolume` -> `plugin.NewUnmounter(volName, podUID)` -> `TearDown`
    /// (`pkg/volume/util/operationexecutor/operation_generator.go:729-757`) for every
    /// volume of a pod that no longer wants it. This is what releases a
    /// `medium: Memory` emptyDir's tmpfs, removes a configMap/secret/
    /// downwardAPI/projected volume's files, and NodeUnpublishes a CSI volume
    /// (`csiMountMgr.TearDownAt`, `csi_mounter.go:432-466`).
    ///
    /// Without a desired/actual state of world, the volumes are found the way
    /// upstream's post-restart reconstruction finds them: by walking
    /// `<pod>/volumes/<plugin>/<volume>` on disk (`getVolumesFromPodDir`,
    /// `reconstruct_common.go:191-240`) and looking the plugin up by the
    /// directory's name (`FindPluginByName`, `reconstruct_common.go:261`
    /// `reconstructVolume`).
    ///
    /// Runs before [`Self::cleanup_orphaned_pod_dirs`], which refuses to touch a
    /// still-mounted volume. A failure (including the transient "driver not
    /// registered" error) is logged and left for the next sync, as the
    /// reconciler retries a failed operation. A directory no registered plugin
    /// owns (the `rusternetes.io/unsupported` placeholder) is left to the
    /// orphan sweep's `rmdir`.
    ///
    /// `terminated_pod_uids` are pods still present in the API whose runtime
    /// is gone (Succeeded/Failed): upstream's populator drops their volumes
    /// from the desired state once `ShouldPodRuntimeBeRemoved`
    /// (`pod_workers.go:698`) holds (`findAndRemoveDeletedPods`,
    /// `desired_state_of_world_populator.go:200-247`), so the reconciler
    /// unmounts them as for a deleted pod. The staged CSI device is released by
    /// [`Self::unmount_unused_csi_devices`], after this.
    pub async fn unmount_orphaned_volumes(
        &self,
        live_pod_uids: &std::collections::HashSet<String>,
        terminated_pod_uids: &std::collections::HashSet<String>,
    ) {
        let root = &self.volumes_base_path;
        let pods = match crate::pod_dirs::list_pods_from_disk(root) {
            Ok(uids) => uids,
            Err(e) => {
                warn!("Could not list pods from disk: {}", e);
                return;
            }
        };
        for uid in pods {
            if live_pod_uids.contains(&uid) && !terminated_pod_uids.contains(&uid) {
                continue;
            }
            let volumes = match crate::pod_dirs::get_volumes_from_pod_dir(root, &uid) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Orphaned pod {uid}: could not list its volumes: {e}");
                    continue;
                }
            };
            for volume in volumes {
                let plugin = match self.plugin_mgr.find_plugin_by_name(&volume.plugin_name) {
                    Ok(p) => p,
                    Err(e) => {
                        debug!(
                            "Orphaned pod {uid}: no plugin to unmount {}: {e}",
                            volume.volume_path.display()
                        );
                        continue;
                    }
                };
                let name = &volume.volume_spec_name;
                let unmounter = match plugin.new_unmounter(name, &uid) {
                    Ok(u) => u,
                    Err(e) => {
                        warn!("Orphaned pod {uid}: cannot unmount volume {name}: {e:#}");
                        continue;
                    }
                };
                if let Err(e) = unmounter.tear_down().await {
                    warn!("Orphaned pod {uid}: TearDown of volume {name} failed: {e:#}");
                }
            }
        }
    }

    /// `(driver, volumeHandle)` of every persistent CSI volume a pod directory
    /// on disk still holds (published and not yet torn down). The actual state
    /// of world's "pods mounted to this volume", rebuilt the way upstream's
    /// reconstruction does it: `ConstructVolumeSpec` on each volume dir.
    fn csi_volumes_held_by_pods(&self) -> std::collections::HashSet<(String, String)> {
        let root = &self.volumes_base_path;
        let mut held = std::collections::HashSet::new();
        let Ok(pods) = crate::pod_dirs::list_pods_from_disk(root) else {
            return held;
        };
        for uid in pods {
            let csi_dir = crate::pod_dirs::get_pod_volumes_dir(root, &uid).join(
                crate::pod_dirs::escape_qualified_name(crate::pod_dirs::plugin::CSI),
            );
            let Ok(entries) = std::fs::read_dir(&csi_dir) else {
                continue;
            };
            for entry in entries.flatten() {
                if let Ok(rec) = self.csi_plugin.construct_volume_spec(
                    &entry.file_name().to_string_lossy(),
                    &entry.path().to_string_lossy(),
                ) {
                    if let Some(csi) = rec.persistent_volume.and_then(|pv| pv.spec.csi) {
                        held.insert((csi.driver, csi.volume_handle.unwrap_or_default()));
                    }
                }
            }
        }
        held
    }

    /// `(driver, volumeHandle)` of every CSI PV the given pods want mounted:
    /// the desired state of world's `VolumeExists`. `None` when a claim could
    /// not be resolved, in which case no device may be unmounted this round
    /// (an unknown desire is not "no desire").
    async fn csi_volumes_desired_by(
        &self,
        pods: &[Pod],
    ) -> Option<std::collections::HashSet<(String, String)>> {
        let mut desired = std::collections::HashSet::new();
        for pod in pods {
            let Some(volumes) = pod.spec.as_ref().and_then(|s| s.volumes.as_ref()) else {
                continue;
            };
            for volume in volumes {
                if volume.persistent_volume_claim.is_none() {
                    continue;
                }
                match self.resolve_persistent_volume(pod, volume).await {
                    Ok(Some(pv)) => {
                        if let Some(csi) = pv.spec.csi {
                            desired.insert((csi.driver, csi.volume_handle.unwrap_or_default()));
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!("Cannot resolve a claim of pod {}: {e:#}", pod.metadata.name);
                        return None;
                    }
                }
            }
        }
        Some(desired)
    }

    /// NodeUnstage the staged CSI devices nothing wants any more.
    ///
    /// Port of the reconciler's `unmountDetachDevices`
    /// (`pkg/kubelet/volumemanager/reconciler/reconciler_common.go:273-315`):
    /// a device is unmounted when it is not mounted for any pod
    /// (`GetUnmountedVolumes`: no pod dir still holds it, so this must run
    /// after [`Self::unmount_orphaned_volumes`], which gives upstream's
    /// unpublish-before-unstage order) and the desired state does not want it
    /// (`!DesiredStateOfWorld.VolumeExists`: `desired_pods`' PVCs). Several pods
    /// sharing one staged volume therefore refcount it by presence. Failures are
    /// logged and retried next sync, as the reconciler does.
    pub async fn unmount_unused_csi_devices(&self, desired_pods: &[Pod]) {
        let Some(desired) = self.csi_volumes_desired_by(desired_pods).await else {
            return;
        };
        self.unmount_csi_devices_not_in(&desired).await;
    }

    /// [`Self::unmount_unused_csi_devices`] with the desired set already known.
    pub(crate) async fn unmount_csi_devices_not_in(
        &self,
        desired: &std::collections::HashSet<(String, String)>,
    ) {
        let held = self.csi_volumes_held_by_pods();
        for (driver, handle, device_path) in self.csi_plugin.list_staged_devices() {
            let key = (driver, handle);
            if held.contains(&key) || desired.contains(&key) {
                continue;
            }
            if let Err(e) = self.csi_plugin.unmount_device(&device_path).await {
                warn!(
                    "CSI UnmountDevice of {} ({}) failed: {e:#}",
                    key.1,
                    device_path.display()
                );
            }
        }
    }

    /// Remove the directories of pods that should not be running and have no
    /// containers running.
    ///
    /// Port of `cleanupOrphanedPodDirs`
    /// (`pkg/kubelet/kubelet_volumes.go:169-260`). Upstream calls this from
    /// `HandlePodCleanups` (`kubelet_pods.go:1304`) on the sync loop, next to
    /// its container cleanup — which is why this sits beside
    /// `cleanup_orphaned_containers` rather than on a shutdown path. A periodic
    /// sweep is also the only form that survives a crash or a killed teardown.
    ///
    /// `live_pod_uids` must contain every pod the kubelet knows about *and*
    /// every pod with a running container, matching upstream's union of `pods`
    /// and `runningPods`.
    pub fn cleanup_orphaned_pod_dirs(&self, live_pod_uids: &std::collections::HashSet<String>) {
        let root = &self.volumes_base_path;
        let found = match crate::pod_dirs::list_pods_from_disk(root) {
            Ok(uids) => uids,
            Err(e) => {
                warn!("Could not list pods from disk: {}", e);
                return;
            }
        };

        for uid in found {
            if live_pod_uids.contains(&uid) {
                continue;
            }

            // If volumes have not been unmounted, do not delete the directory.
            // Doing so may result in corruption of data.
            if self.pod_volumes_exist(&uid) {
                debug!("Orphaned pod {} found, but volumes are not cleaned up", uid);
                continue;
            }

            let volume_errors = self.remove_orphaned_pod_volume_dirs(&uid);
            if !volume_errors.is_empty() {
                // Not all volumes were removed, so don't clean up the pod
                // directory yet — mountpoints or files left behind would make
                // the removal below fail anyway.
                for e in &volume_errors {
                    warn!("{}", e);
                }
                continue;
            }

            let pod_dir = crate::pod_dirs::get_pod_dir(root, &uid);
            let subdirs = match std::fs::read_dir(&pod_dir) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!(
                        "Orphaned pod {} found, but error occurred during reading the pod dir from disk: {}",
                        uid, e
                    );
                    continue;
                }
            };

            let mut cleanup_failed = false;
            for subdir in subdirs {
                let subdir_path = match subdir {
                    Ok(entry) => entry.path(),
                    Err(e) => {
                        cleanup_failed = true;
                        warn!("Failed to read a subdir of pod dir {}: {}", uid, e);
                        continue;
                    }
                };
                // Never recursively delete the volumes directory: that could
                // lose data. It should already be gone, so finding it here is an
                // error to report, not something to force.
                if subdir_path
                    .file_name()
                    .map(|n| n == crate::pod_dirs::VOLUMES_DIR_NAME)
                    == Some(true)
                {
                    cleanup_failed = true;
                    warn!(
                        "Orphaned pod {} found, but failed to remove volumes subdir: volumes subdir was found after it was removed ({})",
                        uid,
                        subdir_path.display()
                    );
                    continue;
                }
                if let Err(e) = crate::removeall::remove_all_one_filesystem(&subdir_path) {
                    cleanup_failed = true;
                    warn!(
                        "Failed to remove orphaned pod subdir {}: {}",
                        subdir_path.display(),
                        e
                    );
                }
            }

            // Rmdir the pod dir, which should be empty if everything above
            // succeeded.
            if !cleanup_failed {
                debug!("Orphaned pod {} found, removing", uid);
                if let Err(e) = std::fs::remove_dir(&pod_dir) {
                    warn!(
                        "Failed to remove orphaned pod dir {}: {}",
                        pod_dir.display(),
                        e
                    );
                }
            }
        }
    }

    /// The plugin that owns a volume, and hence the plugin segment of its
    /// on-disk path. Replaces the ordered if-else chain this file used to
    /// dispatch `create_volume` through: upstream resolves this by asking
    /// every plugin `CanSupport` (`pkg/volume/plugins.go:634-666`), which is
    /// what the registry does.
    ///
    /// Both registry failures collapse to [`crate::pod_dirs::UNSUPPORTED_PLUGIN`]:
    ///
    /// - `NoPluginMatched` is exactly what the old if-else chain's
    ///   else-arm used to return, so `resync_volumes`, `refresh_volumes` and
    ///   the `kubelet.rs` init-container-restart path map keep looking in the
    ///   identical directory. This is the whole reason the mapping exists.
    /// - `MultipleMatched` would, under the old first-match-wins chain, have
    ///   picked a real plugin name here — but `create_volume` now **errors**
    ///   on `MultipleMatched` before any directory is created, so the pod
    ///   fails and nothing ever reads a path for that volume. The divergence
    ///   between this helper and `create_volume` is therefore unreachable.
    ///
    /// A `PersistentVolume` is never passed in: this helper answers "what
    /// plugin does the pod's directory belong to", which for a
    /// `persistentVolumeClaim`/`ephemeral` volume is moot — those never get a
    /// directory under the pod dir (they resolve to a path elsewhere on the
    /// host), so an unmatched PVC/ephemeral volume correctly falls back to
    /// `UNSUPPORTED_PLUGIN` here too.
    pub(crate) fn plugin_name_for_volume(
        &self,
        volume: &rusternetes_common::resources::Volume,
    ) -> &'static str {
        let spec = crate::volume_plugins::Spec {
            volume,
            persistent_volume: None,
            read_only: false,
        };
        match self.plugin_mgr.find_plugin_by_spec(&spec) {
            Ok(plugin) => plugin.name(),
            Err(_) => crate::pod_dirs::UNSUPPORTED_PLUGIN,
        }
    }

    /// The on-disk directory for one of a pod's volumes:
    /// `<root>/pods/<podUID>/volumes/<escaped-plugin>/<volumeName>`.
    ///
    /// The single place a pod volume path is built, mirroring upstream's one
    /// `getPodVolumeDir` (`pkg/kubelet/kubelet_getters.go:181-183`). Every
    /// volume kind routes through here so the layout cannot drift between kinds
    /// — it previously did, with emptyDir keyed on the pod UID and every other
    /// kind keyed on the pod name.
    pub(crate) fn pod_volume_dir(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) -> String {
        crate::pod_dirs::get_pod_volume_dir(
            &self.volumes_base_path,
            &pod.metadata.uid,
            self.plugin_name_for_volume(volume),
            &volume.name,
        )
        .to_string_lossy()
        .into_owned()
    }

    /// Create volumes for a pod and return volume bindings for containers
    pub async fn create_pod_volumes(&self, pod: &Pod) -> Result<HashMap<String, String>> {
        let mut volume_paths = HashMap::new();

        if let Some(volumes) = &pod.spec.as_ref().unwrap().volumes {
            for volume in volumes {
                let volume_path = self.create_volume(pod, volume).await?;
                volume_paths.insert(volume.name.clone(), volume_path);
            }
        }

        // fsGroup is applied by each volume plugin's own SetUp
        // (`volume.NewVolumeOwnership(..).ChangePermissions()`: empty_dir.go:277,
        // configmap.go:246-252, secret.go:242-248, downwardapi.go:217-223,
        // projected.go:200-214). Upstream never chowns hostPath, and there is no
        // cross-volume owner->group mirror pass.
        Ok(volume_paths)
    }

    /// Collect the supplemental GIDs contributed by the pod's mounted volumes,
    /// read from the `pv.beta.kubernetes.io/gid` annotation on each PVC-bound PV
    /// (`VOLUME_GID_ANNOTATION_KEY`). This is the volume-GID half of upstream
    /// `GetExtraSupplementalGroupsForPod`; the CRI translation layer then applies
    /// `translate::extra_supplemental_gids` to drop GIDs already in the pod's
    /// `supplementalGroups` and de-duplicate, before appending them to the
    /// container/sandbox security contexts. Volumes with no PVC, unbound PVCs,
    /// missing PVs, a missing annotation, or a non-numeric annotation value are
    /// skipped.
    pub async fn volume_gids(&self, pod: &Pod) -> Vec<i64> {
        let Some(storage) = self.storage.as_ref() else {
            return Vec::new();
        };
        let Some(volumes) = pod.spec.as_ref().and_then(|s| s.volumes.as_ref()) else {
            return Vec::new();
        };
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let mut gids = Vec::new();
        for volume in volumes {
            let Some(pvc_source) = volume.persistent_volume_claim.as_ref() else {
                continue;
            };
            let pvc_key = build_key(
                "persistentvolumeclaims",
                Some(namespace),
                &pvc_source.claim_name,
            );
            let Ok(pvc) = storage.get::<PersistentVolumeClaim>(&pvc_key).await else {
                continue;
            };
            let Some(pv_name) = pvc.spec.volume_name.as_ref() else {
                continue;
            };
            let pv_key = build_key("persistentvolumes", None, pv_name);
            let Ok(pv) = storage.get::<PersistentVolume>(&pv_key).await else {
                continue;
            };
            if let Some(val) = pv
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(VOLUME_GID_ANNOTATION_KEY))
            {
                if let Ok(gid) = val.parse::<i64>() {
                    gids.push(gid);
                }
            }
        }
        gids
    }

    /// Resync projected/secret/configmap volumes for a running pod.
    /// Re-reads source data from storage and updates volume files if changed.
    ///
    /// Two phases per volume, mirroring upstream's split between the
    /// desired-state populator (API reads) and the operation executor, which
    /// runs each mount/unmount in its own goroutine
    /// (`pkg/kubelet/volumemanager/reconciler/reconciler_common.go` ->
    /// `operationexecutor`, `util/nestedpendingoperations`): the storage reads
    /// stay on the async runtime and all `std::fs` work moves to
    /// `spawn_blocking`, so a slow disk cannot stall async workers (#1620).
    pub async fn resync_volumes<S: rusternetes_storage::Storage>(
        &self,
        pod: &Pod,
        storage: &S,
    ) -> Result<()> {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        if let Some(volumes) = &pod.spec.as_ref().unwrap().volumes {
            for volume in volumes {
                let mut fetched = FetchedSources::fetch(storage, namespace, volume).await;
                fetched
                    .fetch_pod_certificates(self.pod_certificate_manager.as_deref(), pod, volume)
                    .await;
                let this = self.clone();
                let pod = pod.clone();
                let volume = volume.clone();
                tokio::task::spawn_blocking(move || this.resync_volume_fs(&pod, &volume, &fetched))
                    .await
                    .context("resync_volumes blocking task failed")??;
            }
        }
        Ok(())
    }

    /// The blocking half of [`Self::resync_volumes`]: every `std::fs` read,
    /// write and directory walk for ONE volume, against objects the async half
    /// already fetched into `fetched`. Runs on the blocking pool, never on an
    /// async worker.
    fn resync_volume_fs(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
        fetched: &FetchedSources,
    ) -> Result<()> {
        // Resync secret volumes (AtomicWriter re-projection, #1656).
        if let Some(secret_source) = &volume.secret {
            let secret_name = match &secret_source.secret_name {
                Some(n) => n,
                None => return Ok(()),
            };
            let volume_dir = self.pod_volume_dir(pod, volume);
            Self::reproject_secret(
                &volume_dir,
                secret_source,
                fetched.secret(secret_name),
                crate::volume_plugins::util::fs_group_from(pod),
            );
        }
        // Resync configmap volumes. Re-project through the AtomicWriter
        // (same as the initial mount) so an unchanged ConfigMap is a true
        // no-op — never an in-place rewrite through the `..data` symlinks,
        // which would fire an fsnotify Write on the user-visible file and
        // crash a config watcher such as kube-proxy (#1652).
        if let Some(cm_source) = &volume.config_map {
            if let Some(cm_name) = &cm_source.name {
                if let Some(cm) = fetched.config_map(cm_name) {
                    let volume_dir = self.pod_volume_dir(pod, volume);
                    let is_optional = cm_source.optional.unwrap_or(false);
                    let mode = cm_source.default_mode.unwrap_or(0o644) as u32;
                    match build_configmap_payload(
                        cm,
                        cm_source.items.as_ref(),
                        cm_name,
                        is_optional,
                        mode,
                    ) {
                        Ok(payload) => {
                            let _ = crate::volume_ownership::write_payload_with_ownership(
                                std::path::Path::new(&volume_dir),
                                &payload,
                                crate::volume_plugins::util::fs_group_from(pod),
                                true,
                            );
                        }
                        Err(e) => warn!("ConfigMap {} resync: {}", cm_name, e),
                    }
                }
            }
        }
        // Resync projected volumes: re-run the projected plugin's SetUp
        // (`projected.go:136-224` `SetUpAt`: `collectData` then
        // `AtomicWriter.Write`), never an in-place rewrite through the
        // `..data` symlinks (#1656, #2390). An unchanged payload is a no-op, a
        // changed one swaps `..data`, and a removed key is pruned by the
        // writer. A source that cannot be collected leaves the volume
        // untouched, as upstream's failed `SetUpAt` writes nothing.
        if let Some(projected) = &volume.projected {
            let volume_dir = self.pod_volume_dir(pod, volume);
            let dir = std::path::Path::new(&volume_dir);
            match crate::volume_plugins::projected::resync_payload(
                projected,
                pod,
                &self.node_allocatable,
                dir,
                |n| fetched.secret(n),
                |n| fetched.config_map(n),
                |i| fetched.trust_anchors(i),
                |i| fetched.pod_certificate(i),
            ) {
                Ok(payload) => {
                    if let Err(e) = crate::volume_plugins::projected::write_payload(
                        dir,
                        &payload,
                        crate::volume_plugins::util::fs_group_from(pod),
                    ) {
                        warn!("projected volume {} resync: {}", volume.name, e);
                    }
                }
                Err(e) => warn!("projected volume {} resync: {}", volume.name, e),
            }
        }
        // Resync standalone downwardAPI volumes: re-project through the
        // AtomicWriter like the initial mount (downwardapi.go SetUpAt),
        // so unchanged content is a no-op (#1656).
        if let Some(downward_api) = &volume.downward_api {
            let volume_dir = self.pod_volume_dir(pod, volume);
            let items = downward_api.items.as_deref().unwrap_or(&[]);
            let mode = downward_api.default_mode.unwrap_or(0o644) as u32;
            match crate::volume_plugins::downward_api::collect_data(
                items,
                pod,
                &self.node_allocatable,
                mode,
            ) {
                Ok(payload) => {
                    let _ = crate::volume_ownership::write_payload_with_ownership(
                        std::path::Path::new(&volume_dir),
                        &payload,
                        crate::volume_plugins::util::fs_group_from(pod),
                        true,
                    );
                }
                Err(e) => warn!("downwardAPI volume {} resync: {}", volume.name, e),
            }
        }
        Ok(())
    }

    /// Re-project a Secret volume through the AtomicWriter (#1656).
    ///
    /// Upstream re-runs `secretVolumeMounter.SetUpAt` for a `RequiresRemount`
    /// volume (`pkg/volume/secret/secret.go:85-87`, `:162-208`):
    /// `MakePayload` then `AtomicWriter.Write`, which is a no-op for unchanged
    /// content. A required Secret that cannot be read leaves the volume
    /// untouched (SetUp fails, kubelet retries); an optional one projects an
    /// empty Secret (`secret.go:167-172`).
    fn reproject_secret(
        volume_dir: &str,
        source: &rusternetes_common::resources::SecretVolumeSource,
        fetched: Option<&rusternetes_common::resources::Secret>,
        fs_group: Option<i64>,
    ) {
        let Some(secret_name) = source.secret_name.as_ref() else {
            return;
        };
        let optional = source.optional.unwrap_or(false);
        let namespace = fetched
            .and_then(|s| s.metadata.namespace.clone())
            .unwrap_or_default();
        let empty = rusternetes_common::resources::Secret::new(secret_name.clone(), namespace);
        let secret = match fetched {
            Some(s) => s,
            None if optional => &empty,
            None => return,
        };
        let mode = source.default_mode.unwrap_or(0o644) as u32;
        let payload = match crate::volume_plugins::secret::make_payload(
            source.items.as_deref(),
            secret,
            mode,
            optional,
        ) {
            Ok(p) => p,
            Err(e) => {
                warn!("Secret {} resync: {}", secret_name, e);
                return;
            }
        };

        if let Err(e) = crate::volume_ownership::write_payload_with_ownership(
            std::path::Path::new(volume_dir),
            &payload,
            fs_group,
            true,
        ) {
            warn!("Secret {} resync: {:#}", secret_name, e);
        }
    }

    /// Resolve a claim-backed volume to the PersistentVolume that backs it.
    ///
    /// There is no `pvc` or `ephemeral` package under `pkg/volume`: upstream
    /// resolves the claim in the volume manager and then dispatches on the
    /// PV's SOURCE. `desiredStateOfWorldPopulator.createVolumeSpec`
    /// (`pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go:426-471`)
    /// fetches the PVC, reads `pvc.Spec.VolumeName`, then `getPVSpec`
    /// (`:562-589`) fetches the PV and builds a `volume.Spec` from it via
    /// `NewSpecFromPersistentVolume` — the plugin lookup then runs against
    /// that `Spec`, exactly as `find_plugin_by_spec` does below. This is that
    /// resolution step. Returns `Ok(None)` for a volume that is neither a
    /// claim nor an ephemeral claim template.
    pub(crate) async fn resolve_persistent_volume(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) -> Result<Option<PersistentVolume>> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        // PersistentVolumeClaim: find bound PV and use its path
        if let Some(pvc_source) = &volume.persistent_volume_claim {
            // ---- moved verbatim from create_volume's PersistentVolumeClaim branch
            //      (991a503d:crates/kubelet/src/volumes.rs:1328-1373) ----
            let storage = self
                .storage
                .as_ref()
                .context("Storage not available for PersistentVolumeClaim volumes")?;

            let pvc_key = build_key(
                "persistentvolumeclaims",
                Some(namespace),
                &pvc_source.claim_name,
            );
            let pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.with_context(|| {
                format!(
                    "PersistentVolumeClaim {} not found in namespace {}",
                    pvc_source.claim_name, namespace
                )
            })?;

            // Get the bound PV name
            let pv_name = pvc
                .spec
                .volume_name
                .as_ref()
                .context("PersistentVolumeClaim is not bound to a volume")?;

            // Get the PV
            let pv_key = build_key("persistentvolumes", None, pv_name);
            let pv: PersistentVolume = storage
                .get(&pv_key)
                .await
                .with_context(|| format!("PersistentVolume {} not found", pv_name))?;
            // ---- end moved body ----
            // The final mount path is not known here — the matched plugin
            // resolves and logs it separately — so log what this step has:
            // the pod's volume name and the PV it resolved to. Restores the
            // claim/PV pairing that used to be logged from this branch's
            // tail before that tail moved into the hostPath plugin
            // (`991a503d:volumes.rs:1364`).
            info!(
                "Using PersistentVolumeClaim volume {} backed by PV {}",
                volume.name, pv_name
            );
            return Ok(Some(pv));
        }

        // Ephemeral: generic ephemeral volume with PVC template
        if let Some(ephemeral) = &volume.ephemeral {
            if let Some(pvc_template) = &ephemeral.volume_claim_template {
                // ---- moved verbatim from create_volume's Ephemeral branch
                //      (991a503d:crates/kubelet/src/volumes.rs:1464-1551) ----
                let storage = self
                    .storage
                    .as_ref()
                    .context("Storage not available for ephemeral volumes")?;

                // Create a PVC name based on pod name and volume name
                let pvc_name = format!("{}-{}", pod_name, volume.name);

                // Check if PVC already exists
                let pvc_key = build_key("persistentvolumeclaims", Some(namespace), &pvc_name);
                let pvc_exists = storage.get::<PersistentVolumeClaim>(&pvc_key).await.is_ok();

                if !pvc_exists {
                    // Create the PVC from the template
                    let mut pvc = PersistentVolumeClaim {
                        type_meta: rusternetes_common::types::TypeMeta {
                            kind: "PersistentVolumeClaim".to_string(),
                            api_version: "v1".to_string(),
                        },
                        metadata: rusternetes_common::types::ObjectMeta::new(&pvc_name)
                            .with_namespace(namespace),
                        spec: pvc_template.spec.clone(),
                        status: None,
                    };

                    // Copy labels and annotations from template if provided
                    if let Some(template_meta) = &pvc_template.metadata {
                        if let Some(labels) = &template_meta.labels {
                            pvc.metadata.labels = Some(labels.clone());
                        }
                        if let Some(annotations) = &template_meta.annotations {
                            pvc.metadata.annotations = Some(annotations.clone());
                        }
                    }

                    // Store the PVC
                    storage
                        .create(&pvc_key, &pvc)
                        .await
                        .context("Failed to create ephemeral PVC")?;

                    info!(
                        "Created ephemeral PVC {} for volume {}",
                        pvc_name, volume.name
                    );

                    // Wait for PVC to be bound (simplified - in production would poll/watch)
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }

                // Now use the PVC like a regular PersistentVolumeClaim
                let pvc: PersistentVolumeClaim =
                    storage.get(&pvc_key).await.with_context(|| {
                        format!(
                            "PersistentVolumeClaim {} not found in namespace {}",
                            pvc_name, namespace
                        )
                    })?;

                if let Some(pv_name) = &pvc.spec.volume_name {
                    let pv_key = build_key("persistentvolumes", None, pv_name);
                    let pv: PersistentVolume = storage.get(&pv_key).await.with_context(|| {
                        format!(
                            "PersistentVolume {} not found for ephemeral volume",
                            pv_name
                        )
                    })?;
                    // ---- end moved body ----
                    // Same rationale as the PersistentVolumeClaim branch
                    // above: no mount path here, so log the claim/PV pairing
                    // only. Restores the tail dropped from this branch
                    // (`991a503d:volumes.rs:1538`).
                    info!(
                        "Using ephemeral volume {} backed by PVC {} and PV {}",
                        volume.name, pvc_name, pv_name
                    );
                    return Ok(Some(pv));
                } else {
                    // Same wording as the PersistentVolumeClaim branch so
                    // `is_volume_wait_error` treats it as a retriable wait
                    // (upstream: an ephemeral volume is handled "the same way
                    // as a PVC reference", `desired_state_of_world_populator.go:432-441`).
                    return Err(anyhow::anyhow!(
                        "PersistentVolumeClaim {} is not bound to a volume",
                        pvc_name
                    ));
                }
            }
        }

        Ok(None)
    }

    /// Create a single volume and return its host path.
    ///
    /// Port of `pkg/volume/plugins.go:634-666`'s `FindPluginBySpec` used the
    /// way `desiredStateOfWorldPopulator.createVolumeSpec` uses it: resolve a
    /// `persistentVolumeClaim`/`ephemeral` volume to its bound PV first (see
    /// `resolve_persistent_volume` — upstream has no separate pvc plugin), then
    /// look the resulting `Spec` up in the registry exactly once. What used to
    /// be eight `if volume.<kind>.is_some()` blocks, each constructing its own
    /// plugin, is now the one dispatch the registry exists for.
    pub(crate) async fn create_volume(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) -> Result<String> {
        // persistentVolumeClaim / ephemeral resolve to a PersistentVolume
        // first; the PV's source then selects the plugin. Returns `None` for
        // every other volume kind, and for an ephemeral volume with no claim
        // template (which is not actionable and falls through like upstream's
        // empty spec would).
        let pv = self.resolve_persistent_volume(pod, volume).await?;
        let spec = crate::volume_plugins::Spec {
            volume,
            persistent_volume: pv.as_ref(),
            // `NewSpecFromPersistentVolume(pv, pvcSource.ReadOnly)`; a spec
            // without a PV is `NewSpecFromVolume`, ReadOnly false.
            read_only: pv.is_some() && pvc_read_only(volume),
        };

        match self.plugin_mgr.find_plugin_by_spec(&spec) {
            Ok(_) => {
                let (path, attributes) =
                    crate::volume_plugins::util::operation_generator::mount_volume_with_attributes(
                        &self.plugin_mgr,
                        &spec,
                        pod,
                        "",
                    )
                    .await?;
                // Keep an existing `SELinuxLabeled` across a re-mount of the
                // same pod volume (upstream keeps it on the pod's VolumeInfo).
                let mut mounted = self.mounted.lock().unwrap();
                let rec = mounted
                    .entry((pod.metadata.uid.clone(), volume.name.clone()))
                    .or_default();
                rec.attributes = attributes;
                Ok(path)
            }
            Err(crate::volume_plugins::PluginLookupError::NoPluginMatched) if pv.is_some() => {
                // Preserve the pre-registry message: today only a
                // hostPath-sourced PV resolves, so "no plugin matched" for a
                // resolved PV means it has no hostPath source. `lifecycle.rs`
                // documents this exact string as a non-wait error.
                Err(anyhow::anyhow!(
                    "PersistentVolume does not have a hostPath volume source"
                ))
            }
            // DEVIATION from upstream, preserved from the pre-registry
            // `create_volume` and documented at
            // `pod_dirs::UNSUPPORTED_PLUGIN`: upstream fails the pod for an
            // unimplemented inline kind (nfs, iscsi, image, ...); we
            // provision an empty directory so it can run instead. Kept at
            // this call site rather than inside the registry — a catch-all
            // plugin cannot know whether another plugin already matched, so
            // it would trip `MultipleMatched` on every volume — so the
            // registry stays a faithful port and this is one match arm to
            // delete once every kind is implemented.
            Err(crate::volume_plugins::PluginLookupError::NoPluginMatched) => {
                warn!(
                    "Unknown volume type for volume {}, creating empty directory as fallback (volume debug: downward_api={}, empty_dir={}, host_path={}, config_map={}, secret={}, projected={}, pvc={}, csi={}, ephemeral={})",
                    volume.name,
                    volume.downward_api.is_some(),
                    volume.empty_dir.is_some(),
                    volume.host_path.is_some(),
                    volume.config_map.is_some(),
                    volume.secret.is_some(),
                    volume.projected.is_some(),
                    volume.persistent_volume_claim.is_some(),
                    volume.csi.is_some(),
                    volume.ephemeral.is_some(),
                );
                let volume_dir = self.pod_volume_dir(pod, volume);
                std::fs::create_dir_all(&volume_dir)
                    .context("Failed to create fallback volume directory")?;
                Ok(volume_dir)
            }
            // A volume declaring two sources (e.g. both emptyDir and
            // configMap) previously resolved silently, first-arm-wins, under
            // the ordered if-chain. The registry rejects it outright
            // (`pkg/volume/plugins.go:661-663`).
            Err(e) => Err(e.into()),
        }
    }

    /// Refresh Secret and ConfigMap volumes for a running pod.
    /// Re-reads the data from storage and overwrites files on disk.
    ///
    /// Storage reads stay on the async runtime; every `std::fs` call runs in
    /// `spawn_blocking` per volume (see [`Self::resync_volumes`] for the
    /// upstream split this follows, #1620).
    pub async fn refresh_volumes(&self, pod: &Pod) -> Result<()> {
        let storage = match &self.storage {
            Some(s) => s,
            None => return Ok(()),
        };
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let spec = match &pod.spec {
            Some(s) => s,
            None => return Ok(()),
        };
        let volumes = match &spec.volumes {
            Some(v) => v,
            None => return Ok(()),
        };

        for volume in volumes {
            let fetched = FetchedSources::fetch(storage.as_ref(), namespace, volume).await;
            let this = self.clone();
            let pod = pod.clone();
            let volume = volume.clone();
            tokio::task::spawn_blocking(move || this.refresh_volume_fs(&pod, &volume, &fetched))
                .await
                .context("refresh_volumes blocking task failed")??;
        }
        Ok(())
    }

    /// The blocking half of [`Self::refresh_volumes`] for ONE volume.
    fn refresh_volume_fs(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
        fetched: &FetchedSources,
    ) -> Result<()> {
        {
            let volume_dir = self.pod_volume_dir(pod, volume);

            // Refresh Secret volumes: re-run the projection through the
            // AtomicWriter (upstream `SetUpAt` re-runs on every remount of a
            // RequiresRemount volume), never an in-place rewrite (#1656).
            if let Some(secret_source) = &volume.secret {
                let _ = std::fs::create_dir_all(&volume_dir);
                let secret_name = match &secret_source.secret_name {
                    Some(n) => n,
                    None => return Ok(()),
                };
                Self::reproject_secret(
                    &volume_dir,
                    secret_source,
                    fetched.secret(secret_name),
                    crate::volume_plugins::util::fs_group_from(pod),
                );
            }

            // Refresh ConfigMap volumes
            if let Some(cm_source) = &volume.config_map {
                let _ = std::fs::create_dir_all(&volume_dir);
                let cm_name = match &cm_source.name {
                    Some(n) => n,
                    None => return Ok(()),
                };
                match fetched.config_map(cm_name) {
                    Some(cm) => {
                        // Re-project through the AtomicWriter (same path as the
                        // initial mount): an unchanged ConfigMap is a true no-op,
                        // and key removals are handled by the writer's stale
                        // user-visible-symlink pruning. This must NOT rewrite the
                        // user-visible files in place — that follows the `..data`
                        // symlinks and fires an fsnotify Write, crash-looping a
                        // config watcher like kube-proxy (#1652).
                        let is_optional = cm_source.optional.unwrap_or(false);
                        let mode = cm_source.default_mode.unwrap_or(0o644) as u32;
                        match build_configmap_payload(
                            cm,
                            cm_source.items.as_ref(),
                            cm_name,
                            is_optional,
                            mode,
                        ) {
                            Ok(payload) => {
                                let _ = crate::volume_ownership::write_payload_with_ownership(
                                    std::path::Path::new(&volume_dir),
                                    &payload,
                                    crate::volume_plugins::util::fs_group_from(pod),
                                    true,
                                );
                            }
                            Err(e) => warn!("ConfigMap {} refresh: {}", cm_name, e),
                        }
                    }
                    None => {
                        // ConfigMap deleted — clean up files if optional
                        let is_optional = cm_source.optional.unwrap_or(false);
                        if is_optional {
                            if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                for entry in entries.flatten() {
                                    let _ = std::fs::remove_file(entry.path());
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Secrets and ConfigMaps a volume references, read from storage on the async
/// runtime so the blocking filesystem half (`resync_volume_fs` /
/// `refresh_volume_fs`) never has to `.await`. A source that is absent from
/// storage is simply absent from the map — the same signal the old inline
/// `storage.get(..).await` `Err` arm gave (#1620).
#[derive(Default)]
struct FetchedSources {
    secrets: HashMap<String, rusternetes_common::resources::Secret>,
    config_maps: HashMap<String, ConfigMap>,
    /// Trust anchors of each projected `clusterTrustBundle` source, keyed by
    /// the source's index in `projected.sources` (`projected.go:320-355`).
    trust_anchors: HashMap<usize, std::result::Result<Vec<u8>, String>>,
    /// `(key, certificate chain)` of each projected `podCertificate` source,
    /// keyed by source index (`projected.go:392`); filled only on resync.
    pod_certificates: HashMap<usize, crate::volume_plugins::projected::BundleResult>,
}

impl FetchedSources {
    async fn fetch<S: Storage + ?Sized>(
        storage: &S,
        namespace: &str,
        volume: &rusternetes_common::resources::Volume,
    ) -> Self {
        let mut out = Self::default();
        let mut secrets: Vec<&String> = Vec::new();
        let mut cms: Vec<&String> = Vec::new();
        if let Some(n) = volume.secret.as_ref().and_then(|s| s.secret_name.as_ref()) {
            secrets.push(n);
        }
        if let Some(n) = volume.config_map.as_ref().and_then(|c| c.name.as_ref()) {
            cms.push(n);
        }
        if let Some(sources) = volume.projected.as_ref().and_then(|p| p.sources.as_ref()) {
            for (index, source) in sources.iter().enumerate() {
                if let Some(ctb) = &source.cluster_trust_bundle {
                    let anchors = crate::volume_plugins::projected::trust_anchors_for(storage, ctb)
                        .await
                        .map_err(|e| e.to_string());
                    out.trust_anchors.insert(index, anchors);
                }
                if let Some(n) = source.secret.as_ref().and_then(|s| s.name.as_ref()) {
                    secrets.push(n);
                }
                if let Some(n) = source.config_map.as_ref().and_then(|c| c.name.as_ref()) {
                    cms.push(n);
                }
            }
        }
        for name in secrets {
            let key = build_key("secrets", Some(namespace), name);
            if let Ok(secret) = storage
                .get::<rusternetes_common::resources::Secret>(&key)
                .await
            {
                out.secrets.insert(name.clone(), secret);
            }
        }
        for name in cms {
            let key = build_key("configmaps", Some(namespace), name);
            if let Ok(cm) = storage.get::<ConfigMap>(&key).await {
                out.config_maps.insert(name.clone(), cm);
            }
        }
        out
    }

    /// Ask the pod certificate manager for each `podCertificate` source's
    /// bundle (`GetPodCertificateCredentialBundle`, `projected.go:392`). With
    /// no manager the source errors `unimplemented`, as `NoOpManager` does
    /// (`podcertificatemanager.go:994`).
    async fn fetch_pod_certificates(
        &mut self,
        manager: Option<&dyn crate::podcertificate::Manager>,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) {
        let Some(sources) = volume.projected.as_ref().and_then(|p| p.sources.as_ref()) else {
            return;
        };
        for (index, source) in sources.iter().enumerate() {
            if source.pod_certificate.is_none() {
                continue;
            }
            let bundle = match manager {
                Some(m) => {
                    m.get_pod_certificate_credential_bundle(
                        pod.metadata.namespace.as_deref().unwrap_or("default"),
                        &pod.metadata.name,
                        &pod.metadata.uid,
                        &volume.name,
                        index,
                    )
                    .await
                }
                None => Err(anyhow::anyhow!("unimplemented")),
            };
            self.pod_certificates
                .insert(index, bundle.map_err(|e| e.to_string()));
        }
    }

    fn pod_certificate(
        &self,
        index: usize,
    ) -> Option<&crate::volume_plugins::projected::BundleResult> {
        self.pod_certificates.get(&index)
    }

    fn secret(&self, name: &str) -> Option<&rusternetes_common::resources::Secret> {
        self.secrets.get(name)
    }

    fn config_map(&self, name: &str) -> Option<&ConfigMap> {
        self.config_maps.get(name)
    }

    fn trust_anchors(&self, index: usize) -> Option<&std::result::Result<Vec<u8>, String>> {
        self.trust_anchors.get(&index)
    }
}

#[cfg(test)]
mod configmap_payload_tests {
    use super::*;

    fn cm(pairs: &[(&str, &str)]) -> ConfigMap {
        let mut c = ConfigMap::new("cm", "default");
        c.data = Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        c
    }

    fn item(key: &str, path: &str, mode: Option<i32>) -> KeyToPath {
        KeyToPath {
            key: key.to_string(),
            path: path.to_string(),
            mode,
        }
    }

    /// `items[].mode` must reach the projected file; siblings without one keep
    /// the volume `defaultMode`.
    ///
    /// Pins the upstream Conformance spec "ConfigMap should be consumable from
    /// pods in volume with mappings and Item mode set [LinuxOnly]"
    /// (`test/e2e/common/storage/configmap_volume.go:99-102`), which maps a key
    /// to the nested path `path/to/data-2` with `Mode: 0400` and asserts the
    /// file mode. Upstream carries the mode per file in `FileProjection.Mode`
    /// (`pkg/volume/util/atomic_writer.go:64-68`); this projection used to
    /// collapse it to one mode for the whole volume, so the spec could not pass.
    #[test]
    fn item_mode_overrides_default_mode_per_file() {
        let configmap = cm(&[("data-1", "value-1"), ("data-2", "value-2")]);
        let items = vec![
            item("data-2", "path/to/data-2", Some(0o400)),
            item("data-1", "plain", None),
        ];

        let payload =
            build_configmap_payload(&configmap, Some(&items), "cm", false, 0o644).unwrap();

        assert_eq!(
            payload.get("path/to/data-2").map(|p| p.mode),
            Some(0o400),
            "items[].mode must be carried per file"
        );
        assert_eq!(
            payload.get("path/to/data-2").map(|p| p.data.as_slice()),
            Some(b"value-2".as_slice())
        );
        assert_eq!(
            payload.get("plain").map(|p| p.mode),
            Some(0o644),
            "an item without a mode falls back to the volume defaultMode"
        );
    }

    /// With no `items`, every key takes the volume `defaultMode`.
    #[test]
    fn without_items_every_key_takes_default_mode() {
        let configmap = cm(&[("a", "1"), ("b", "2")]);
        let payload = build_configmap_payload(&configmap, None, "cm", false, 0o400).unwrap();

        assert_eq!(payload.len(), 2);
        assert!(payload.values().all(|p| p.mode == 0o400));
    }
}

#[cfg(all(test, unix))]
mod projected_mode_tests {
    use super::*;
    use rusternetes_common::resources::Secret;
    use rusternetes_storage::{build_key, Storage, StorageBackend};
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;

    /// A projected configMap item with an explicit `mode` must have that mode
    /// applied to the file the resync path (re)writes — matching the
    /// initial-mount path and upstream's atomic writer. Regression guard for the
    /// resync gap where new keys landed at the umask default (#1050).
    #[tokio::test]
    async fn resync_projected_configmap_applies_item_mode() {
        let tmp = std::env::temp_dir().join(format!("rn-projmode-{}-{}", std::process::id(), "cm"));
        let _ = std::fs::remove_dir_all(&tmp);

        let storage = Arc::new(StorageBackend::new_memory());
        let cm: rusternetes_common::resources::ConfigMap = serde_json::from_value(json!({
            "metadata": {"name": "cfg", "namespace": "default"},
            "data": {"app.conf": "hello"}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm,
        )
        .await
        .unwrap();

        // defaultMode 0644 (420), item mode 0400 (256).
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-p"},
            "spec": {"containers": [], "volumes": [{
                "name": "proj",
                "projected": {
                    "defaultMode": 420,
                    "sources": [{"configMap": {
                        "name": "cfg",
                        "items": [{"key": "app.conf", "path": "app.conf", "mode": 256}]
                    }}]
                }
            }]}
        }))
        .unwrap();

        let vm = VolumeManager::new(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();

        let file = crate::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-p",
            crate::pod_dirs::plugin::PROJECTED,
            "proj",
        )
        .join("app.conf");
        let perms = std::fs::metadata(&file)
            .expect("projected file must exist after resync")
            .permissions();
        assert_eq!(
            perms.mode() & 0o777,
            0o400,
            "projected configMap item mode must be applied on resync"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// #1652 regression: re-projecting an UNCHANGED ConfigMap volume (via
    /// `refresh_volumes` on every sync AND `resync_volumes`) must be a true
    /// no-op — it must NOT rewrite the user-visible file in place through the
    /// AtomicWriter `..data` symlink. An in-place rewrite bumps the real file's
    /// ctime and fires an fsnotify Write, which crash-loops a config watcher
    /// such as kube-proxy ("content of the proxy server's configuration file
    /// was updated"). The user-visible file must stay a symlink and its target
    /// inode's ctime must be unchanged across re-projection.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_unchanged_configmap_is_a_noop() {
        use std::os::unix::fs::MetadataExt;

        let tmp = std::env::temp_dir().join(format!("rn-cm-noop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let storage = Arc::new(StorageBackend::new_memory());
        let cm: ConfigMap = serde_json::from_value(json!({
            "metadata": {"name": "kube-proxy", "namespace": "kube-system"},
            "data": {"config.conf": "apiVersion: v1\nkind: Config\n", "kubeconfig.conf": "x"}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("kube-system"), "kube-proxy"),
            &cm,
        )
        .await
        .unwrap();

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "kube-proxy-xyz", "namespace": "kube-system", "uid": "uid-kp"},
            "spec": {"containers": [], "volumes": [{
                "name": "kube-proxy",
                "configMap": {"name": "kube-proxy"}
            }]}
        }))
        .unwrap();

        let vm = VolumeManager::new(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );

        // Initial projection (AtomicWriter layout: config.conf -> ..data/config.conf).
        vm.create_pod_volumes(&pod).await.unwrap();
        let visible = crate::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-kp",
            crate::pod_dirs::plugin::CONFIG_MAP,
            "kube-proxy",
        )
        .join("config.conf");
        assert!(
            std::fs::symlink_metadata(&visible)
                .unwrap()
                .file_type()
                .is_symlink(),
            "config.conf must be a symlink (AtomicWriter layout)"
        );
        let real = std::fs::canonicalize(&visible).unwrap();
        let ctime_before = std::fs::metadata(&real).unwrap().ctime();
        let data_link_before = std::fs::read_link(
            crate::pod_dirs::get_pod_volume_dir(
                &tmp.to_string_lossy(),
                "uid-kp",
                crate::pod_dirs::plugin::CONFIG_MAP,
                "kube-proxy",
            )
            .join("..data"),
        )
        .unwrap();

        // Re-project the UNCHANGED ConfigMap through BOTH sync-loop paths repeatedly.
        for _ in 0..3 {
            vm.refresh_volumes(&pod).await.unwrap();
            vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        }

        // The user-visible file must still be a symlink, the real inode's ctime
        // must be untouched (no in-place write), and ..data must not have swapped.
        assert!(
            std::fs::symlink_metadata(&visible)
                .unwrap()
                .file_type()
                .is_symlink(),
            "config.conf must remain a symlink after re-projection"
        );
        let ctime_after = std::fs::metadata(&real).unwrap().ctime();
        assert_eq!(
            ctime_before, ctime_after,
            "unchanged re-projection must not rewrite the config file in place (would crash kube-proxy)"
        );
        let data_link_after = std::fs::read_link(
            crate::pod_dirs::get_pod_volume_dir(
                &tmp.to_string_lossy(),
                "uid-kp",
                crate::pod_dirs::plugin::CONFIG_MAP,
                "kube-proxy",
            )
            .join("..data"),
        )
        .unwrap();
        assert_eq!(
            data_link_before, data_link_after,
            "..data must not swap when the ConfigMap is unchanged"
        );
        assert_eq!(
            std::fs::read_to_string(&visible).unwrap(),
            "apiVersion: v1\nkind: Config\n"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// Drives a volume through the initial mount then repeated
    /// `refresh_volumes` + `resync_volumes`, asserting the visible file stays
    /// an AtomicWriter symlink and its real inode is never rewritten (#1656).
    #[cfg(unix)]
    async fn assert_reproject_is_noop(
        tag: &str,
        pod: Pod,
        secret: Option<Secret>,
        plugin_dir: &str,
        volume_name: &str,
        file: &str,
        expected: &str,
    ) {
        use std::os::unix::fs::MetadataExt;
        let tmp = std::env::temp_dir().join(format!("rn-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Arc::new(StorageBackend::new_memory());
        if let Some(s) = secret {
            Storage::create(
                storage.as_ref(),
                &build_key("secrets", Some("default"), &s.metadata.name),
                &s,
            )
            .await
            .unwrap();
        }
        let vm = VolumeManager::new(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.create_pod_volumes(&pod).await.unwrap();
        let dir = crate::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            &pod.metadata.uid,
            plugin_dir,
            volume_name,
        );
        let visible = dir.join(file);
        assert!(
            std::fs::symlink_metadata(&visible)
                .unwrap()
                .file_type()
                .is_symlink(),
            "{file} must be an AtomicWriter symlink after the initial mount"
        );
        let real = std::fs::canonicalize(&visible).unwrap();
        let ctime_before = std::fs::metadata(&real).unwrap().ctime();
        let link_before = std::fs::read_link(dir.join("..data")).unwrap();
        for _ in 0..3 {
            vm.refresh_volumes(&pod).await.unwrap();
            vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        }
        assert!(std::fs::symlink_metadata(&visible)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            ctime_before,
            std::fs::metadata(&real).unwrap().ctime(),
            "unchanged re-projection must not rewrite {file} in place"
        );
        assert_eq!(link_before, std::fs::read_link(dir.join("..data")).unwrap());
        assert_eq!(std::fs::read_to_string(&visible).unwrap(), expected);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// #1656: Secret re-projection (`refresh_volumes` + `resync_volumes`) must
    /// be a true no-op, like ConfigMap.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_unchanged_secret_is_a_noop() {
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-sec"},
            "spec": {"containers": [], "volumes": [{
                "name": "creds", "secret": {"secretName": "creds"}
            }]}
        }))
        .unwrap();
        let secret =
            Secret::new("creds", "default").with_data(std::collections::HashMap::from([(
                "password".to_string(),
                b"hunter2".to_vec(),
            )]));
        assert_reproject_is_noop(
            "sec-noop",
            pod,
            Some(secret),
            crate::pod_dirs::plugin::SECRET,
            "creds",
            "password",
            "hunter2",
        )
        .await;
    }

    /// #2390: the projected branch of `resync_volumes` must re-project through
    /// the AtomicWriter like the initial mount (`projected.go:208-221`) --
    /// never write through the `..data` symlinks. Unchanged => zero fs changes
    /// (ctime of every real file and the `..data` target untouched, bound SA
    /// token kept); a changed source swaps `..data`; a removed key is pruned.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_unchanged_projected_is_a_noop() {
        use std::os::unix::fs::MetadataExt;
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-proj"},
            "spec": {"containers": [], "volumes": [{
                "name": "proj",
                "projected": {"defaultMode": 420, "sources": [
                    {"configMap": {"name": "cfg"}},
                    {"secret": {"name": "sec", "items": [{"key": "password", "path": "pw", "mode": 256}]}},
                    {"downwardAPI": {"items": [{"path": "podname", "fieldRef": {"fieldPath": "metadata.name"}}]}},
                    {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}}
                ]}
            }]}
        }))
        .unwrap();
        let tmp = std::env::temp_dir().join(format!("rn-proj-noop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Arc::new(StorageBackend::new_memory());
        let cm = ConfigMap::new("cfg", "default").with_data(HashMap::from([
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]));
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm,
        )
        .await
        .unwrap();
        let secret = Secret::new("sec", "default").with_data(HashMap::from([(
            "password".to_string(),
            b"hunter2".to_vec(),
        )]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();
        let vm = VolumeManager::new(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.create_pod_volumes(&pod).await.unwrap();
        let dir = crate::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-proj",
            crate::pod_dirs::plugin::PROJECTED,
            "proj",
        );
        let files = ["a", "b", "pw", "podname", "token"];
        let snapshot = |dir: &std::path::Path| -> Vec<(i64, i64, u32)> {
            files
                .iter()
                .map(|f| {
                    let real = std::fs::canonicalize(dir.join(f)).unwrap();
                    let m = std::fs::metadata(&real).unwrap();
                    (m.ctime(), m.ctime_nsec(), m.permissions().mode() & 0o777)
                })
                .collect()
        };
        for f in files {
            assert!(
                std::fs::symlink_metadata(dir.join(f))
                    .unwrap()
                    .file_type()
                    .is_symlink(),
                "{f} must be an AtomicWriter symlink after the initial mount"
            );
        }
        let before = snapshot(&dir);
        let link_before = std::fs::read_link(dir.join("..data")).unwrap();
        let token_before = std::fs::read(dir.join("token")).unwrap();
        for _ in 0..3 {
            vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        }
        assert_eq!(
            before,
            snapshot(&dir),
            "unchanged resync must not touch any file"
        );
        assert_eq!(link_before, std::fs::read_link(dir.join("..data")).unwrap());
        assert_eq!(std::fs::read(dir.join("token")).unwrap(), token_before);
        assert_eq!(
            std::fs::metadata(dir.join("pw"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400,
            "items[].mode is kept"
        );

        // A changed ConfigMap key swaps `..data`; the removed key `b` is
        // pruned; the bound token and the other sources survive.
        let cm2 = ConfigMap::new("cfg", "default")
            .with_data(HashMap::from([("a".to_string(), "changed".to_string())]));
        Storage::update(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm2,
        )
        .await
        .unwrap();
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        assert_ne!(link_before, std::fs::read_link(dir.join("..data")).unwrap());
        assert_eq!(std::fs::read_to_string(dir.join("a")).unwrap(), "changed");
        assert!(std::fs::symlink_metadata(dir.join("b")).is_err());
        assert_eq!(std::fs::read_to_string(dir.join("pw")).unwrap(), "hunter2");
        assert_eq!(std::fs::read_to_string(dir.join("podname")).unwrap(), "p");
        assert_eq!(std::fs::read(dir.join("token")).unwrap(), token_before);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// #2638: a `kube-api-access*` / `*-token` Secret volume is an ordinary
    /// Secret volume (upstream `secret.go` has no token special case), so the
    /// stored token is projected at mount and re-projected unchanged.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_sa_named_secret_projects_stored_token() {
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-sat"},
            "spec": {"containers": [], "volumes": [{
                "name": "kube-api-access-x", "secret": {"secretName": "default-token"}
            }]}
        }))
        .unwrap();
        let secret = Secret::new("default-token", "default").with_data(
            std::collections::HashMap::from([("token".to_string(), b"static-unbound".to_vec())]),
        );
        let tmp = std::env::temp_dir().join(format!("rn-sat-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Arc::new(StorageBackend::new_memory());
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "default-token"),
            &secret,
        )
        .await
        .unwrap();
        let vm = VolumeManager::new(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.create_pod_volumes(&pod).await.unwrap();
        let token = crate::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-sat",
            crate::pod_dirs::plugin::SECRET,
            "kube-api-access-x",
        )
        .join("token");
        let before = std::fs::read(&token).unwrap();
        assert_eq!(before, b"static-unbound", "stored token projected verbatim");
        vm.refresh_volumes(&pod).await.unwrap();
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        assert_eq!(std::fs::read(&token).unwrap(), before);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// #1656: standalone downwardAPI resync must be a true no-op.
    #[cfg(unix)]
    #[tokio::test]
    async fn reproject_unchanged_downward_api_is_a_noop() {
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "dapi-pod", "namespace": "default", "uid": "uid-da"},
            "spec": {"containers": [], "volumes": [{
                "name": "podinfo", "downwardAPI": {"items": [
                    {"path": "name", "fieldRef": {"fieldPath": "metadata.name"}}
                ]}
            }]}
        }))
        .unwrap();
        assert_reproject_is_noop(
            "da-noop",
            pod,
            None,
            crate::pod_dirs::plugin::DOWNWARD_API,
            "podinfo",
            "name",
            "dapi-pod",
        )
        .await;
    }

    /// Projected secret resync must honor `items` mappings. If it writes every
    /// secret key at the volume root, stale cleanup can delete the mapped file
    /// that the container is waiting for.
    #[tokio::test]
    async fn resync_projected_secret_honors_items_mapping() {
        let tmp =
            std::env::temp_dir().join(format!("rn-projmode-{}-{}", std::process::id(), "secret"));
        let _ = std::fs::remove_dir_all(&tmp);

        let storage = Arc::new(StorageBackend::new_memory());
        let secret: rusternetes_common::resources::Secret = serde_json::from_value(json!({
            "metadata": {"name": "sec", "namespace": "default"},
            "data": {"data-1": "dmFsdWUtMQ=="}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-p"},
            "spec": {"containers": [], "volumes": [{
                "name": "proj",
                "projected": {
                    "defaultMode": 420,
                    "sources": [{"secret": {
                        "name": "sec",
                        "items": [{"key": "data-1", "path": "new-path-data-1", "mode": 256}]
                    }}]
                }
            }]}
        }))
        .unwrap();

        let volume_dir = crate::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-p",
            crate::pod_dirs::plugin::PROJECTED,
            "proj",
        );
        std::fs::create_dir_all(&volume_dir).unwrap();
        std::fs::write(volume_dir.join("data-1"), b"stale-root").unwrap();

        let vm = VolumeManager::new(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();

        let mapped = volume_dir.join("new-path-data-1");
        assert_eq!(std::fs::read(&mapped).unwrap(), b"value-1");
        assert!(
            !volume_dir.join("data-1").exists(),
            "unmapped root secret key must be removed as stale"
        );
        let perms = std::fs::metadata(&mapped).unwrap().permissions();
        assert_eq!(
            perms.mode() & 0o777,
            0o400,
            "projected secret item mode must be applied on resync"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// A pod mounting a PVC bound to a PV carrying the
    /// `pv.beta.kubernetes.io/gid` annotation must surface that GID via
    /// `volume_gids` (upstream `VolumeGIDValue`). A PVC-backed volume whose PV
    /// lacks the annotation contributes nothing.
    #[tokio::test]
    async fn volume_gids_reads_pv_gid_annotation_for_pvc() {
        use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim};

        let storage = Arc::new(StorageBackend::new_memory());

        // PV `pv-gid` carries the GID annotation; PV `pv-nogid` does not.
        for (name, ann) in [
            ("pv-gid", json!({"pv.beta.kubernetes.io/gid": "7777"})),
            ("pv-nogid", json!({})),
        ] {
            let pv: PersistentVolume = serde_json::from_value(json!({
                "metadata": {"name": name, "annotations": ann},
                "spec": {
                    "capacity": {"storage": "1Gi"},
                    "accessModes": ["ReadWriteOnce"],
                    "hostPath": {"path": format!("/tmp/{name}")}
                }
            }))
            .unwrap();
            Storage::create(
                storage.as_ref(),
                &build_key("persistentvolumes", None, name),
                &pv,
            )
            .await
            .unwrap();
        }

        for (claim, pv) in [("claim-gid", "pv-gid"), ("claim-nogid", "pv-nogid")] {
            let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
                "metadata": {"name": claim, "namespace": "default"},
                "spec": {"accessModes": ["ReadWriteOnce"], "volumeName": pv,
                         "resources": {"requests": {"storage": "1Gi"}}}
            }))
            .unwrap();
            Storage::create(
                storage.as_ref(),
                &build_key("persistentvolumeclaims", Some("default"), claim),
                &pvc,
            )
            .await
            .unwrap();
        }

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-p"},
            "spec": {"containers": [], "volumes": [
                {"name": "with-gid", "persistentVolumeClaim": {"claimName": "claim-gid"}},
                {"name": "no-gid", "persistentVolumeClaim": {"claimName": "claim-nogid"}}
            ]}
        }))
        .unwrap();

        let vm = VolumeManager::new(
            std::env::temp_dir().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        assert_eq!(vm.volume_gids(&pod).await, vec![7777]);
    }

    /// Characterization test for the secret volume plugin's `set_up` body
    /// (moved verbatim from `create_volume` in #1970's Task 6). Goes through
    /// the public entry point, `VolumeManager::create_volume`, not the plugin
    /// directly, and asserts on the real on-disk result: the returned path,
    /// the written file contents, and the mode the body applies from
    /// `defaultMode`. This is the test the Task 6 review found missing — the
    /// safety argument for the highest-risk moved body in the refactor had
    /// rested entirely on the text diff. It was re-run unmodified against
    /// `991a503d` (the pre-move commit) to confirm it characterizes the OLD
    /// behaviour too, not just whatever the new code happens to do — see the
    /// task-6 report for both runs.
    /// #1983: the map `create_pod_volumes` returns — which both container
    /// start paths now consume, init-container restart included — holds a
    /// hostPath volume's host path itself, never a dir under the pod dir.
    /// Upstream records the mounted path per volume and the runtime reads
    /// that (`GetMountedVolumesForPod`, `volume_manager.go:320`).
    #[tokio::test]
    async fn create_pod_volumes_maps_a_host_path_volume_to_the_host_path() {
        let tmp = tempfile::tempdir().unwrap();
        let host = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": [], "volumes": [
                {"name": "hp", "hostPath": {"path": host.path(), "type": "Directory"}}
            ]}
        }))
        .unwrap();
        let paths = vm.create_pod_volumes(&pod).await.unwrap();
        assert_eq!(paths["hp"], host.path().to_string_lossy());
    }

    #[tokio::test]
    async fn create_volume_writes_secret_data_to_disk() {
        let storage = Arc::new(StorageBackend::new_memory());

        let secret = Secret::new("mysecret", "default").with_data(HashMap::from([
            ("username".to_string(), b"admin".to_vec()),
            ("password".to_string(), b"hunter2".to_vec()),
        ]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "mysecret"),
            &secret,
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap();
        let volume: rusternetes_common::resources::Volume = serde_json::from_value(json!({
            "name": "sec",
            "secret": {"secretName": "mysecret", "defaultMode": 0o400}
        }))
        .unwrap();

        let path = vm.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(
            path,
            format!(
                "{}/pods/uid-1/volumes/kubernetes.io~secret/sec",
                tmp.path().to_string_lossy()
            )
        );

        let username = std::fs::read(format!("{path}/username")).unwrap();
        assert_eq!(username, b"admin");
        let password = std::fs::read(format!("{path}/password")).unwrap();
        assert_eq!(password, b"hunter2");

        let mode = std::fs::metadata(format!("{path}/username"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o400,
            "file mode must come from the volume's defaultMode"
        );

        // The volume dir's mode no longer derives from defaultMode (#1975):
        // upstream's AtomicWriter creates the timestamped data dir 0755
        // (atomic_writer.go) and the volume dir comes from the emptyDir
        // wrapper, never from the Secret's defaultMode.
        let data_dir = std::fs::canonicalize(format!("{path}/..data")).unwrap();
        let dir_mode = std::fs::metadata(&data_dir).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o777, 0o755);
    }

    /// Characterization test for the downwardAPI volume plugin's `set_up`
    /// body, written and run BEFORE that body moves out of `create_volume`
    /// (#1970 Task 7) — see the task-7 report for the equivalence run against
    /// the pre-move commit. Goes through the public entry point,
    /// `VolumeManager::create_volume`, not the plugin directly, and asserts
    /// on the real on-disk result: the returned path, a `fieldRef` item's
    /// contents, a `resourceFieldRef` item's contents, the mode a per-item
    /// `mode` applies, the mode `defaultMode` applies to an item with no
    /// override, and the directory mode (`defaultMode | 0o111`).
    #[tokio::test]
    async fn create_volume_writes_downward_api_data_to_disk() {
        let storage = Arc::new(StorageBackend::new_memory());

        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": [{
                "name": "app",
                "resources": {"limits": {"cpu": "2"}}
            }]}
        }))
        .unwrap();
        let volume: rusternetes_common::resources::Volume = serde_json::from_value(json!({
            "name": "podinfo",
            "downwardAPI": {
                "defaultMode": 416,
                "items": [
                    {"path": "podname", "fieldRef": {"fieldPath": "metadata.name"}},
                    {
                        "path": "cpu_limit",
                        "resourceFieldRef": {"containerName": "app", "resource": "limits.cpu"},
                        "mode": 256
                    }
                ]
            }
        }))
        .unwrap();

        let path = vm.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(
            path,
            format!(
                "{}/pods/uid-1/volumes/kubernetes.io~downward-api/podinfo",
                tmp.path().to_string_lossy()
            )
        );

        let podname = std::fs::read_to_string(format!("{path}/podname")).unwrap();
        assert_eq!(podname, "p", "fieldRef item must contain metadata.name");
        let cpu_limit = std::fs::read_to_string(format!("{path}/cpu_limit")).unwrap();
        assert_eq!(
            cpu_limit, "2",
            "resourceFieldRef item must contain the container's limits.cpu, in whole cores"
        );

        let podname_mode = std::fs::metadata(format!("{path}/podname"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            podname_mode & 0o777,
            0o640,
            "file mode must come from the volume's defaultMode when the item has no mode"
        );

        let cpu_limit_mode = std::fs::metadata(format!("{path}/cpu_limit"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            cpu_limit_mode & 0o777,
            0o400,
            "file mode must come from the item's own mode when set"
        );

        // The wrapped emptyDir's setupDir leaves the root at 0777
        // (empty_dir.go:447-486); downwardapi.go never chmods it (#2323).
        let dir_mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o777, 0o777);
    }

    /// Characterization test for the projected volume plugin's `set_up` body,
    /// written and run BEFORE that body moves out of `create_volume` (#1970
    /// Task 8) — see the task-8 report for the equivalence run against the
    /// pre-move commit. Goes through the public entry point,
    /// `VolumeManager::create_volume`, not the plugin directly, and combines
    /// the three source kinds a projected volume fans out to in production
    /// (configMap, secret, downwardAPI) in one volume, matching the whole
    /// point of the projected plugin: asserts on the real on-disk result —
    /// the returned path, each source's file contents, a per-item `mode`
    /// override (the configMap item), the `defaultMode` fallback (the secret
    /// and downwardAPI items, which set no `mode`), and the directory mode
    /// (`defaultMode | 0o111`).
    ///
    /// The serviceAccountToken source is NOT covered here — it mints a real
    /// token via `TokenManager` and reads ServiceAccount/Node uids from
    /// storage, which is a materially different code path from the other
    /// three. Left as a follow-up, same as the gap flagged for the secret
    /// plugin in Task 6.
    #[tokio::test]
    async fn create_volume_writes_projected_sources_to_disk() {
        let storage = Arc::new(StorageBackend::new_memory());

        let cm = ConfigMap::new("cfg", "default").with_data(HashMap::from([(
            "app.conf".to_string(),
            "hello".to_string(),
        )]));
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm,
        )
        .await
        .unwrap();

        let secret = Secret::new("sec", "default").with_data(HashMap::from([(
            "password".to_string(),
            b"hunter2".to_vec(),
        )]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap();
        let volume: rusternetes_common::resources::Volume = serde_json::from_value(json!({
            "name": "proj",
            "projected": {
                "defaultMode": 416,
                "sources": [
                    {"configMap": {
                        "name": "cfg",
                        "items": [{"key": "app.conf", "path": "app.conf", "mode": 256}]
                    }},
                    {"secret": {
                        "name": "sec",
                        "items": [{"key": "password", "path": "password"}]
                    }},
                    {"downwardAPI": {
                        "items": [{"path": "podname", "fieldRef": {"fieldPath": "metadata.name"}}]
                    }}
                ]
            }
        }))
        .unwrap();

        let path = vm.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(
            path,
            format!(
                "{}/pods/uid-1/volumes/kubernetes.io~projected/proj",
                tmp.path().to_string_lossy()
            )
        );

        let app_conf = std::fs::read_to_string(format!("{path}/app.conf")).unwrap();
        assert_eq!(
            app_conf, "hello",
            "configMap item must contain its key's value"
        );
        let password = std::fs::read(format!("{path}/password")).unwrap();
        assert_eq!(
            password, b"hunter2",
            "secret item must contain its key's value"
        );
        let podname = std::fs::read_to_string(format!("{path}/podname")).unwrap();
        assert_eq!(
            podname, "p",
            "downwardAPI fieldRef item must contain metadata.name"
        );

        let app_conf_mode = std::fs::metadata(format!("{path}/app.conf"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            app_conf_mode & 0o777,
            0o400,
            "configMap item's own mode must override the volume's defaultMode"
        );

        let password_mode = std::fs::metadata(format!("{path}/password"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            password_mode & 0o777,
            0o640,
            "secret item with no mode must fall back to the volume's defaultMode"
        );

        let podname_mode = std::fs::metadata(format!("{path}/podname"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            podname_mode & 0o777,
            0o640,
            "downwardAPI item with no mode must fall back to the volume's defaultMode"
        );

        // The volume directory itself is no longer chmod'd to
        // `defaultMode | 0o111`: upstream's projected SetUpAt hands the
        // payload to the AtomicWriter (`projected.go` -> `writer.Write`), which
        // never applies defaultMode to the directory, so it must simply stay
        // traversable.
        let dir_mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o100, 0o100, "volume dir must stay traversable");
        let ts = std::fs::read_link(format!("{path}/..data")).unwrap();
        let ts_mode = std::fs::metadata(format!("{path}/{}", ts.display()))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            ts_mode & 0o777,
            0o755,
            "timestamp dir is 0755 (atomic_writer.go:399-408)"
        );
    }

    /// With a pod `fsGroup`, a projected file written 0600 ends up 0640
    /// (`volume_linux.go:171-175`: `mode | roMask` for the projected volume's
    /// `GetAttributes().ReadOnly`) -- NOT 0660, which the generic owner->group
    /// mirror in `create_pod_volumes` would produce. The projected plugin owns
    /// its ownership step (`projected.go:200-214`), so the post-pass must skip
    /// it (#2333).
    #[tokio::test]
    async fn create_pod_volumes_projected_fs_group_is_ro_mask_not_mirror() {
        use std::os::unix::fs::MetadataExt;
        let storage = Arc::new(StorageBackend::new_memory());
        let secret = Secret::new("sec", "default").with_data(HashMap::from([(
            "password".to_string(),
            b"hunter2".to_vec(),
        )]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let probe = tmp.path().join("probe");
        std::fs::write(&probe, b"").unwrap();
        let gid = std::fs::metadata(&probe).unwrap().gid();
        let vm = VolumeManager::new(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {
                "securityContext": {"fsGroup": gid},
                "containers": [],
                "volumes": [{
                    "name": "proj",
                    "projected": {"defaultMode": 384, "sources": [
                        {"secret": {"name": "sec"}}
                    ]}
                }]
            }
        }))
        .unwrap();
        let paths = vm.create_pod_volumes(&pod).await.unwrap();
        let file = std::path::Path::new(&paths["proj"]).join("password");
        let meta = std::fs::metadata(&file).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o640);
        assert_eq!(meta.gid(), gid);
    }

    /// configMap, secret and downwardAPI volumes with a pod `fsGroup` get
    /// `chmod(mode | roMask)` (`volume_linux.go:171-175`; each reports
    /// `ReadOnly: true`, `configmap.go:160`, `secret.go:166`,
    /// `downwardapi.go:156`), run from their own AtomicWriter `setPerms`
    /// (`configmap.go:246-252`, `secret.go:242-248`, `downwardapi.go:217-223`).
    /// A 0644 file stays 0644 (the owner->group mirror made it 0664) and a 0600
    /// file becomes 0640 (#2540).
    #[tokio::test]
    async fn create_pod_volumes_fs_group_is_ro_mask_for_cm_secret_downward_api() {
        use rusternetes_common::resources::ConfigMap;
        use std::os::unix::fs::MetadataExt;
        let storage = Arc::new(StorageBackend::new_memory());
        let secret = Secret::new("sec", "default")
            .with_data(HashMap::from([("k".to_string(), b"v".to_vec())]));
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "sec"),
            &secret,
        )
        .await
        .unwrap();
        let cm: ConfigMap = serde_json::from_value(json!({
            "metadata": {"name": "cm", "namespace": "default"},
            "data": {"k": "v"}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("configmaps", Some("default"), "cm"),
            &cm,
        )
        .await
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let probe = tmp.path().join("probe");
        std::fs::write(&probe, b"").unwrap();
        let gid = std::fs::metadata(&probe).unwrap().gid();
        let vm = VolumeManager::new(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1",
                         "labels": {"a": "b"}},
            "spec": {
                "securityContext": {"fsGroup": gid},
                "containers": [],
                "volumes": [
                    {"name": "cm", "configMap": {"name": "cm"}},
                    {"name": "sec", "secret": {"secretName": "sec", "defaultMode": 384}},
                    {"name": "da", "downwardAPI": {"items": [
                        {"path": "labels", "fieldRef": {"fieldPath": "metadata.labels"}}
                    ]}}
                ]
            }
        }))
        .unwrap();
        let paths = vm.create_pod_volumes(&pod).await.unwrap();
        let check = |vol: &str, file: &str, want: u32| {
            let meta = std::fs::metadata(std::path::Path::new(&paths[vol]).join(file)).unwrap();
            assert_eq!(meta.permissions().mode() & 0o777, want, "{vol}/{file}");
            assert_eq!(meta.gid(), gid, "{vol}/{file} gid");
        };
        check("cm", "k", 0o644);
        check("da", "labels", 0o644);
        check("sec", "k", 0o640);
    }
}

/// Orphaned pod directory sweep — port of `cleanupOrphanedPodDirs`
/// (`pkg/kubelet/kubelet_volumes.go:169-260`).
#[cfg(test)]
mod orphan_sweep_tests {
    use super::*;
    use std::collections::HashSet;

    fn tmp(tag: &str) -> String {
        let p = std::env::temp_dir().join(format!("rn-sweep-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p.to_string_lossy().into_owned()
    }

    fn vm(root: &str) -> VolumeManager {
        VolumeManager::new(
            root.to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
    }

    /// Lay down `<root>/pods/<uid>/volumes/<plugin>/<volume>` as create_volume
    /// would, so the sweep sees a realistic tree.
    fn seed_pod(root: &str, uid: &str, volume: &str) -> std::path::PathBuf {
        let dir = crate::pod_dirs::get_pod_volume_dir(
            root,
            uid,
            crate::pod_dirs::plugin::EMPTY_DIR,
            volume,
        );
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Lay down a non-empty volume of `plugin`, as a running pod leaves it.
    fn seed_filled_volume(root: &str, uid: &str, plugin: &str, volume: &str) -> std::path::PathBuf {
        let dir = crate::pod_dirs::get_pod_volume_dir(root, uid, plugin, volume);
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/data"), "x").unwrap();
        dir
    }

    /// The gap this slice closes: `removeOrphanedPodVolumeDirs` `rmdir`s each
    /// volume (`kubelet_volumes.go:119-165`), which fails on a volume that
    /// still holds files, so a pod whose emptyDir was written to leaked its
    /// directory forever — upstream relies on the reconciler's `TearDown`
    /// having emptied it first.
    #[test]
    fn the_sweep_alone_cannot_remove_a_volume_that_holds_files() {
        let root = tmp("filled-sweep-only");
        seed_filled_volume(
            &root,
            "uid-gone",
            crate::pod_dirs::plugin::EMPTY_DIR,
            "scratch",
        );

        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());

        assert!(crate::pod_dirs::get_pod_dir(&root, "uid-gone").exists());
    }

    /// `unmountVolumes` -> `NewUnmounter` -> `TearDown`, then the sweep.
    #[tokio::test]
    async fn teardown_then_sweep_reaps_every_non_csi_volume_kind() {
        let root = tmp("filled-teardown");
        use crate::pod_dirs::plugin;
        for (p, v) in [
            (plugin::EMPTY_DIR, "scratch"),
            (plugin::CONFIG_MAP, "cfg"),
            (plugin::SECRET, "sec"),
            (plugin::DOWNWARD_API, "dapi"),
            (plugin::PROJECTED, "proj"),
        ] {
            seed_filled_volume(&root, "uid-gone", p, v);
        }
        // A live pod's identical volumes must not be touched.
        let live_dir = seed_filled_volume(&root, "uid-live", plugin::EMPTY_DIR, "scratch");
        let live: HashSet<String> = ["uid-live".to_string()].into_iter().collect();

        let vm = vm(&root);
        vm.unmount_orphaned_volumes(&live, &HashSet::new()).await;
        vm.cleanup_orphaned_pod_dirs(&live);

        assert!(
            !crate::pod_dirs::get_pod_dir(&root, "uid-gone").exists(),
            "the orphan's directory must be reaped once its volumes are torn down"
        );
        assert!(live_dir.join("sub/data").exists(), "live pod untouched");
    }

    /// A pod still in the API but terminated no longer wants its volumes
    /// (`ShouldPodRuntimeBeRemoved`), so they are torn down as for a deleted
    /// pod.
    #[tokio::test]
    async fn a_terminated_pods_volumes_are_torn_down() {
        let root = tmp("filled-terminated");
        let dir = seed_filled_volume(
            &root,
            "uid-done",
            crate::pod_dirs::plugin::EMPTY_DIR,
            "scratch",
        );
        let uids: HashSet<String> = ["uid-done".to_string()].into_iter().collect();

        vm(&root).unmount_orphaned_volumes(&uids, &uids).await;

        assert!(!dir.exists());
    }

    /// A hostPath volume's `TearDown` does nothing (`host_path.go:272-274`),
    /// and a directory no plugin owns is left for the sweep.
    #[tokio::test]
    async fn host_path_and_unowned_dirs_are_left_alone_by_teardown() {
        let root = tmp("hostpath-unowned");
        let hp = seed_filled_volume(&root, "uid-gone", crate::pod_dirs::plugin::HOST_PATH, "hp");
        let other = seed_filled_volume(&root, "uid-gone", crate::pod_dirs::UNSUPPORTED_PLUGIN, "x");

        vm(&root)
            .unmount_orphaned_volumes(&HashSet::new(), &HashSet::new())
            .await;

        assert!(hp.join("sub/data").exists());
        assert!(other.join("sub/data").exists());
    }

    #[test]
    fn a_pod_dir_with_no_live_pod_is_removed() {
        let root = tmp("orphan");
        seed_pod(&root, "uid-gone", "scratch");

        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());

        assert!(
            !crate::pod_dirs::get_pod_dir(&root, "uid-gone").exists(),
            "an orphaned pod dir must be reaped"
        );
    }

    #[test]
    fn a_live_pods_dir_is_left_alone() {
        let root = tmp("live");
        seed_pod(&root, "uid-live", "scratch");

        let live: HashSet<String> = ["uid-live".to_string()].into_iter().collect();
        vm(&root).cleanup_orphaned_pod_dirs(&live);

        assert!(
            crate::pod_dirs::get_pod_volume_dir(
                &root,
                "uid-live",
                crate::pod_dirs::plugin::EMPTY_DIR,
                "scratch"
            )
            .exists(),
            "a live pod's volumes must survive the sweep"
        );
    }

    /// Only the orphan goes; a live pod sharing the sweep is untouched.
    #[test]
    fn the_sweep_removes_only_the_orphans() {
        let root = tmp("mixed");
        seed_pod(&root, "uid-live", "scratch");
        seed_pod(&root, "uid-gone", "scratch");

        let live: HashSet<String> = ["uid-live".to_string()].into_iter().collect();
        vm(&root).cleanup_orphaned_pod_dirs(&live);

        assert!(crate::pod_dirs::get_pod_dir(&root, "uid-live").exists());
        assert!(!crate::pod_dirs::get_pod_dir(&root, "uid-gone").exists());
    }

    /// The property that makes the sweep safe to run unattended: a volume dir
    /// holding real content is removed with `rmdir`, which fails, so the data
    /// stays. Upstream relies on exactly this rather than a recursive delete
    /// (`kubelet_volumes.go:114-118`).
    #[test]
    fn content_left_in_a_volume_dir_is_never_deleted() {
        let root = tmp("content");
        let vol = seed_pod(&root, "uid-data", "scratch");
        let payload = vol.join("important.txt");
        std::fs::write(&payload, b"do not delete me").unwrap();

        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());

        assert!(payload.exists(), "file content must survive the sweep");
        assert_eq!(
            std::fs::read(&payload).unwrap(),
            b"do not delete me",
            "and must be unmodified"
        );
        assert!(
            crate::pod_dirs::get_pod_dir(&root, "uid-data").exists(),
            "the pod dir must be kept while content remains under it"
        );
    }

    /// Non-`volumes` subdirs of the pod dir are reaped with the recursive
    /// remover, matching upstream's RemoveAllOneFilesystem pass.
    #[test]
    fn other_pod_subdirs_are_reaped() {
        let root = tmp("subdirs");
        seed_pod(&root, "uid-sub", "scratch");
        let containers = crate::pod_dirs::get_pod_dir(&root, "uid-sub").join("containers");
        std::fs::create_dir_all(&containers).unwrap();
        std::fs::write(containers.join("log"), b"x").unwrap();

        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());

        assert!(!crate::pod_dirs::get_pod_dir(&root, "uid-sub").exists());
    }

    #[test]
    fn a_missing_pods_dir_is_not_an_error() {
        let root = tmp("empty");
        // No pods/ dir at all — nothing has run yet.
        vm(&root).cleanup_orphaned_pod_dirs(&HashSet::new());
        assert!(crate::pod_dirs::list_pods_from_disk(&root)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn list_pods_from_disk_returns_the_uids_on_disk() {
        let root = tmp("list");
        seed_pod(&root, "uid-a", "v");
        seed_pod(&root, "uid-b", "v");

        let mut found = crate::pod_dirs::list_pods_from_disk(&root).unwrap();
        found.sort();
        assert_eq!(found, vec!["uid-a".to_string(), "uid-b".to_string()]);
    }
}

/// Task 10 (#1970): `persistentVolumeClaim` and `ephemeral` have no plugin of
/// their own — they resolve to a `PersistentVolume` and the PV's SOURCE picks
/// the plugin (`host_path.go:98-101`). These tests cover
/// `VolumeManager::resolve_persistent_volume` and the dispatch that follows
/// it in `create_volume`, with particular attention to the error strings
/// `lifecycle.rs::is_volume_wait_error` matches on byte-for-byte.
#[cfg(test)]
mod pvc_resolution_tests {
    use super::*;
    use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim, Pod, Volume};
    use rusternetes_storage::StorageBackend;
    use serde_json::json;

    fn vm(storage: Option<Arc<StorageBackend>>) -> VolumeManager {
        VolumeManager::new(
            std::env::temp_dir().to_string_lossy().to_string(),
            storage,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
    }

    /// A PVC-backed volume must reach the plugin that owns the PV's SOURCE.
    /// There is no pvc plugin upstream: hostPath claims it via the
    /// PersistentVolume arm of `CanSupport` (`host_path.go:98-101`).
    #[tokio::test]
    async fn a_bound_pvc_resolves_to_its_pv_and_dispatches_on_the_pv_source() {
        let storage = Arc::new(StorageBackend::new_memory());

        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "hostPath": {"path": "/mnt/data"}
            }
        }))
        .unwrap();
        storage
            .create(&build_key("persistentvolumes", None, "pv-1"), &pv)
            .await
            .unwrap();

        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "default"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeName": "pv-1",
                "resources": {"requests": {"storage": "1Gi"}}
            }
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("default"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();

        let volume: Volume = serde_json::from_value(json!({
            "name": "claimed",
            "persistentVolumeClaim": {"claimName": "claim-1"}
        }))
        .unwrap();

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let manager = vm(Some(storage));
        let resolved = manager
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap();
        let resolved = resolved.expect("a bound PVC resolves to a PV");
        let spec = crate::volume_plugins::Spec {
            volume: &volume,
            persistent_volume: Some(&resolved),
            read_only: false,
        };
        let plugin = manager.plugin_mgr.find_plugin_by_spec(&spec).unwrap();
        assert_eq!(plugin.name(), crate::pod_dirs::plugin::HOST_PATH);

        // And create_volume must dispatch to the same plugin end-to-end.
        let path = manager.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(path, "/mnt/data");
    }

    /// #2782: a `readOnly: true` PVC over a hostPath PV must reach the CRI
    /// mount as read-only (`makeMounts`: `mustMountRO`, kubelet_pods.go:392),
    /// while a read-write claim and an inline hostPath stay read-write.
    #[tokio::test]
    async fn read_only_pvc_over_host_path_pv_yields_read_only_mount_attrs() {
        let storage = Arc::new(StorageBackend::new_memory());
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "hostPath": {"path": "/mnt/data"}
            }
        }))
        .unwrap();
        storage
            .create(&build_key("persistentvolumes", None, "pv-1"), &pv)
            .await
            .unwrap();
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "default"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeName": "pv-1",
                "resources": {"requests": {"storage": "1Gi"}}
            }
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("default"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {
                "volumes": [
                    {"name": "ro", "persistentVolumeClaim": {"claimName": "claim-1", "readOnly": true}},
                    {"name": "rw", "persistentVolumeClaim": {"claimName": "claim-1"}},
                    {"name": "inline", "hostPath": {"path": "/mnt/inline"}}
                ],
                "containers": [{
                    "name": "c", "image": "i",
                    "volumeMounts": [
                        {"name": "ro", "mountPath": "/ro"},
                        {"name": "rw", "mountPath": "/rw"},
                        {"name": "inline", "mountPath": "/inline"}
                    ]
                }]
            }
        }))
        .unwrap();
        let manager = vm(Some(storage));
        manager.create_pod_volumes(&pod).await.unwrap();
        let attrs = manager.mount_attributes(&pod, &pod.spec.as_ref().unwrap().containers[0]);
        assert!(attrs["ro"].read_only);
        assert!(!attrs["rw"].read_only);
        assert!(!attrs["inline"].read_only);
        // hostPath is not Managed: never relabelled (host_path.go:180).
        assert!(attrs.values().all(|a| !a.selinux_relabel));
    }

    /// #2801: after a kubelet restart nothing ran `create_volume`, so the
    /// attribute record is empty. Upstream rebuilds a Mounter per volume
    /// (`reconstructVolume`, `reconciler/reconstruct_common.go:352`
    /// `plugin.NewMounter(volumeSpec, pod)`) and `makeMounts` reads
    /// `GetAttributes()` from it.
    #[tokio::test]
    async fn mount_attributes_are_reconstructed_without_create_pod_volumes() {
        let storage = Arc::new(StorageBackend::new_memory());
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                     "hostPath": {"path": "/mnt/data"}}
        }))
        .unwrap();
        storage
            .create(&build_key("persistentvolumes", None, "pv-1"), &pv)
            .await
            .unwrap();
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "default"},
            "spec": {"accessModes": ["ReadWriteOnce"], "volumeName": "pv-1",
                     "resources": {"requests": {"storage": "1Gi"}}}
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("default"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {
                "volumes": [
                    {"name": "ro", "persistentVolumeClaim": {"claimName": "claim-1", "readOnly": true}},
                    {"name": "rw", "persistentVolumeClaim": {"claimName": "claim-1"}}
                ],
                "containers": [{"name": "c", "image": "i", "volumeMounts": [
                    {"name": "ro", "mountPath": "/ro"}, {"name": "rw", "mountPath": "/rw"}]}]
            }
        }))
        .unwrap();
        // A fresh manager: the restarted kubelet. No create_pod_volumes.
        let manager = vm(Some(storage));
        manager.reconstruct_mount_attributes(&pod).await;
        let attrs = manager.mount_attributes(&pod, &pod.spec.as_ref().unwrap().containers[0]);
        assert!(attrs["ro"].read_only, "ReadOnly forcing survives a restart");
        assert!(!attrs["rw"].read_only);
    }

    /// A re-reconstruction must not clobber a live record (and its
    /// `SELinuxLabeled` bit) written by `create_volume`.
    #[tokio::test]
    async fn reconstruct_keeps_an_existing_record() {
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-2"},
            "spec": {"volumes": [{"name": "e", "emptyDir": {}}],
                "containers": [{"name": "c", "image": "i",
                    "volumeMounts": [{"name": "e", "mountPath": "/e"}]}]}
        }))
        .unwrap();
        let manager = vm(None);
        manager
            .mounted
            .lock()
            .unwrap()
            .entry(("uid-2".into(), "e".into()))
            .or_default()
            .selinux_labeled = true;
        manager.reconstruct_mount_attributes(&pod).await;
        assert!(
            manager.mounted.lock().unwrap()[&("uid-2".to_string(), "e".to_string())]
                .selinux_labeled
        );
    }

    /// `createVolumeSpec` hands `pvcSource.ReadOnly` to
    /// `NewSpecFromPersistentVolume(pv, pvcReadOnly)`
    /// (`desired_state_of_world_populator.go:461`, `:588`); an inline volume's
    /// `NewSpecFromVolume` leaves it false (`plugins.go:549-553`).
    #[test]
    fn spec_read_only_comes_from_the_claim_source() {
        let ro: Volume = serde_json::from_value(json!({
            "name": "c", "persistentVolumeClaim": {"claimName": "c", "readOnly": true}
        }))
        .unwrap();
        let rw: Volume = serde_json::from_value(json!({
            "name": "c", "persistentVolumeClaim": {"claimName": "c"}
        }))
        .unwrap();
        let inline: Volume =
            serde_json::from_value(json!({"name": "h", "hostPath": {"path": "/x"}})).unwrap();
        assert!(pvc_read_only(&ro));
        assert!(!pvc_read_only(&rw));
        assert!(!pvc_read_only(&inline));
    }

    #[tokio::test]
    async fn a_non_claim_volume_resolves_to_no_pv() {
        let volume: Volume = serde_json::from_value(json!({
            "name": "scratch",
            "emptyDir": {}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p"},
            "spec": {"containers": []}
        }))
        .unwrap();

        assert!(vm(None)
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap()
            .is_none());
    }

    /// Preserved byte-for-byte: `lifecycle.rs::is_volume_wait_error` matches
    /// "not found in namespace" together with "PersistentVolumeClaim" to keep
    /// a pod in `ContainerCreating` rather than failing it outright.
    #[tokio::test]
    async fn a_missing_pvc_yields_the_exact_not_found_message() {
        let storage = Arc::new(StorageBackend::new_memory());
        let volume: Volume = serde_json::from_value(json!({
            "name": "claimed",
            "persistentVolumeClaim": {"claimName": "missing"}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm(Some(storage))
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "PersistentVolumeClaim missing not found in namespace ns"
        );
    }

    /// Preserved byte-for-byte: `lifecycle.rs::is_volume_wait_error` matches
    /// this exact string to keep the pod waiting instead of failing it.
    #[tokio::test]
    async fn an_unbound_pvc_yields_the_exact_not_bound_message() {
        let storage = Arc::new(StorageBackend::new_memory());
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "ns"},
            "spec": {"accessModes": ["ReadWriteOnce"],
                     "resources": {"requests": {"storage": "1Gi"}}}
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("ns"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();

        let volume: Volume = serde_json::from_value(json!({
            "name": "claimed",
            "persistentVolumeClaim": {"claimName": "claim-1"}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm(Some(storage))
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "PersistentVolumeClaim is not bound to a volume"
        );
    }

    /// #1981: upstream handles a generic ephemeral volume "the same way as a
    /// PVC reference" (`desired_state_of_world_populator.go:432-441`), so an
    /// unbound ephemeral claim must be a retriable volume wait exactly like an
    /// unbound PVC — not a start failure.
    #[tokio::test]
    async fn an_unbound_ephemeral_pvc_is_a_volume_wait_error() {
        let storage = Arc::new(StorageBackend::new_memory());
        let volume: Volume = serde_json::from_value(json!({
            "name": "scratch",
            "ephemeral": {"volumeClaimTemplate": {"spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1Gi"}}}}}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm(Some(storage))
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "PersistentVolumeClaim p-scratch is not bound to a volume"
        );
        assert!(crate::lifecycle::is_volume_wait_error(&err.to_string()));
    }

    /// Ruling 3: `ephemeral` set but `volumeClaimTemplate` absent falls
    /// through both arms silently — upstream's structure (nested `if let`)
    /// is reproduced verbatim, not converted into an error. `create_volume`
    /// then hits the unsupported-kind fallback and gets an empty directory.
    #[tokio::test]
    async fn an_ephemeral_volume_without_a_template_resolves_to_no_pv() {
        let volume: Volume = serde_json::from_value(json!({
            "name": "eph",
            "ephemeral": {}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p"},
            "spec": {"containers": []}
        }))
        .unwrap();

        assert!(vm(None)
            .resolve_persistent_volume(&pod, &volume)
            .await
            .unwrap()
            .is_none());
    }

    /// Ruling 2: a PV with no claimable source (today, anything but hostPath)
    /// must surface the pre-registry message verbatim, not the registry's own
    /// "no volume plugin matched" — `lifecycle.rs:486` documents this exact
    /// string as a worked example of a non-wait error.
    #[tokio::test]
    async fn a_pv_without_a_hostpath_source_yields_the_preserved_message() {
        let storage = Arc::new(StorageBackend::new_memory());
        // No source field on the PV at all — deliberately not a CSI source
        // (that specific case is not this test's concern); any PV with no
        // claimable source must produce this message.
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"]
            }
        }))
        .unwrap();
        storage
            .create(&build_key("persistentvolumes", None, "pv-1"), &pv)
            .await
            .unwrap();

        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "claim-1", "namespace": "default"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeName": "pv-1",
                "resources": {"requests": {"storage": "1Gi"}}
            }
        }))
        .unwrap();
        storage
            .create(
                &build_key("persistentvolumeclaims", Some("default"), "claim-1"),
                &pvc,
            )
            .await
            .unwrap();

        let volume: Volume = serde_json::from_value(json!({
            "name": "claimed",
            "persistentVolumeClaim": {"claimName": "claim-1"}
        }))
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm(Some(storage))
            .create_volume(&pod, &volume)
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "PersistentVolume does not have a hostPath volume source"
        );
    }
}

/// Task 11 (#1970): `create_volume`'s eight-branch `if let` chain becomes one
/// `find_plugin_by_spec` lookup. These pin the #1967 invariant directly: the
/// directory a volume is created in (`create_volume`) must be the directory
/// every other code path looks for it in (`plugin_name_for_volume`, hence
/// `pod_volume_dir` and the `kubelet.rs` init-container-restart path map).
#[cfg(test)]
mod dispatch_tests {
    use super::*;
    use rusternetes_common::resources::Volume;
    use serde_json::json;

    fn vm() -> VolumeManager {
        VolumeManager::new(
            std::env::temp_dir().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
    }

    fn volume(name: &str, source: serde_json::Value) -> Volume {
        let mut fields = source;
        fields
            .as_object_mut()
            .unwrap()
            .insert("name".to_string(), json!(name));
        serde_json::from_value(fields).unwrap()
    }

    /// Every implemented kind reaches its own plugin through the registry.
    /// `plugin_name_for_volume` and `create_volume` build the identical
    /// `Spec` and call the identical `find_plugin_by_spec`, so this is also
    /// the "same name the registry dispatches to" half of the #1967
    /// invariant — the other half is `an_unimplemented_kind_falls_back_to_unsupported`.
    #[test]
    fn each_kind_resolves_to_its_own_plugin() {
        let vm = vm();
        let cases: Vec<(Volume, &str)> = vec![
            (
                volume("a", json!({"emptyDir": {}})),
                crate::pod_dirs::plugin::EMPTY_DIR,
            ),
            (
                volume("b", json!({"configMap": {"name": "cm"}})),
                crate::pod_dirs::plugin::CONFIG_MAP,
            ),
            (
                volume("c", json!({"secret": {"secretName": "s"}})),
                crate::pod_dirs::plugin::SECRET,
            ),
            (
                volume("d", json!({"downwardAPI": {}})),
                crate::pod_dirs::plugin::DOWNWARD_API,
            ),
            (
                volume("e", json!({"projected": {}})),
                crate::pod_dirs::plugin::PROJECTED,
            ),
            (
                volume("f", json!({"csi": {"driver": "d"}})),
                crate::pod_dirs::plugin::CSI,
            ),
            (
                volume("g", json!({"hostPath": {"path": "/tmp"}})),
                crate::pod_dirs::plugin::HOST_PATH,
            ),
        ];
        for (v, want) in cases {
            assert_eq!(vm.plugin_name_for_volume(&v), want, "volume {}", v.name);
        }
    }

    /// The storage-free kinds (emptyDir, hostPath, downwardAPI, csi, and
    /// projected with no sources) prove `create_volume` itself — not just
    /// `plugin_name_for_volume` — routes through the same single
    /// `find_plugin_by_spec` call and lands the mount under the matched
    /// plugin's directory segment. configMap/secret/PVC dispatch through the
    /// identical call and are exercised with storage by
    /// `create_volume_writes_secret_data_to_disk`,
    /// `create_volume_writes_downward_api_data_to_disk`,
    /// `create_volume_writes_projected_sources_to_disk` and
    /// `pvc_resolution_tests`.
    #[tokio::test]
    async fn create_volume_dispatches_storage_free_kinds_to_their_plugin_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let cases: Vec<(Volume, &str)> = vec![
            (
                volume("a", json!({"emptyDir": {}})),
                crate::pod_dirs::plugin::EMPTY_DIR,
            ),
            (
                volume("d", json!({"downwardAPI": {}})),
                crate::pod_dirs::plugin::DOWNWARD_API,
            ),
            // `projected` is not storage-free: upstream's SetUpAt fails with
            // "kube client is not configured" without a client, so its
            // plugin-dir dispatch is asserted by the projected tests.
            (
                volume("f", json!({"csi": {"driver": "d"}})),
                crate::pod_dirs::plugin::CSI,
            ),
            (
                volume("g", json!({"hostPath": {"path": "/tmp"}})),
                crate::pod_dirs::plugin::HOST_PATH,
            ),
        ];
        for (v, want) in cases {
            let pod: Pod = serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "default", "uid": format!("uid-{}", v.name)},
                "spec": {"containers": []}
            }))
            .unwrap();
            let path = vm.create_volume(&pod, &v).await.unwrap();
            // hostPath's mounter returns the host path itself rather than a
            // pod-dir-derived path, so this row asserts the literal instead
            // of the plugin-dir shape asserted below. The fixture is an
            // inline hostPath with no `$`, so its env expansion is a no-op.
            if want == crate::pod_dirs::plugin::HOST_PATH {
                assert_eq!(path, "/tmp");
            } else {
                assert!(
                    path.contains(&crate::pod_dirs::escape_qualified_name(want)),
                    "volume {} took path {path}, want it to contain the {want} plugin segment",
                    v.name
                );
            }
        }
    }

    /// An unimplemented kind (nfs, iscsi, image, ...) matches no plugin.
    /// `create_volume` applies the documented empty-directory fallback rather
    /// than erroring, and `plugin_name_for_volume` reports the same
    /// `UNSUPPORTED_PLUGIN` segment that fallback directory sits under — the
    /// other half of the #1967 invariant.
    #[tokio::test]
    async fn an_unimplemented_kind_falls_back_to_unsupported() {
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new(
            tmp.path().to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        let v = volume(
            "nfs-vol",
            json!({"nfs": {"server": "10.0.0.1", "path": "/export"}}),
        );

        assert_eq!(
            vm.plugin_name_for_volume(&v),
            crate::pod_dirs::UNSUPPORTED_PLUGIN
        );

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-nfs"},
            "spec": {"containers": []}
        }))
        .unwrap();
        let path = vm.create_volume(&pod, &v).await.unwrap();
        assert!(
            path.contains(&crate::pod_dirs::escape_qualified_name(
                crate::pod_dirs::UNSUPPORTED_PLUGIN
            )),
            "path {path} should sit under the unsupported-plugin segment"
        );
        assert!(std::path::Path::new(&path).is_dir());
    }

    /// The case the ordered if-else this replaced silently resolved: two
    /// sources set, first arm wins. The registry rejects it
    /// (`pkg/volume/plugins.go:661-663`), and `create_volume` now propagates
    /// that error instead of mounting either source.
    #[tokio::test]
    async fn a_volume_with_two_sources_is_rejected() {
        let mut v = volume("malformed", json!({"emptyDir": {}}));
        v.config_map = Some(serde_json::from_value(json!({"name": "cm"})).unwrap());

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-malformed"},
            "spec": {"containers": []}
        }))
        .unwrap();

        let err = vm().create_volume(&pod, &v).await.unwrap_err();
        assert!(
            err.to_string().contains("multiple volume plugins matched"),
            "{err}"
        );
    }
}

/// #1620: `resync_volumes` / `refresh_volumes` must not run `std::fs` on an
/// async worker. Upstream runs each volume operation in its own goroutine
/// (`pkg/kubelet/volumemanager/reconciler/reconciler_common.go` ->
/// `operationexecutor`), never inline in the loop that serves everything else.
///
/// The probe is a FIFO standing in for a secret key's file: `std::fs::read` /
/// `write` on it blocks until the other end opens, exactly like IO stuck on a
/// wedged disk. On a single-threaded runtime an inline blocking call starves
/// every other task; offloaded, a heartbeat task keeps ticking.
#[cfg(all(test, unix))]
mod blocking_fs_offload_tests {
    use super::*;
    use rusternetes_storage::{build_key, Storage, StorageBackend};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    async fn setup(tag: &str) -> (VolumeManager, Pod, Arc<StorageBackend>, std::path::PathBuf) {
        let tmp = std::env::temp_dir().join(format!("rn-offload-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Arc::new(StorageBackend::new_memory());
        let secret: rusternetes_common::resources::Secret = serde_json::from_value(json!({
            "metadata": {"name": "s", "namespace": "default"},
            "data": {"k": "dg=="}
        }))
        .unwrap();
        Storage::create(
            storage.as_ref(),
            &build_key("secrets", Some("default"), "s"),
            &secret,
        )
        .await
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-p"},
            "spec": {"containers": [], "volumes": [{"name": "sv", "secret": {"secretName": "s"}}]}
        }))
        .unwrap();
        let dir = crate::pod_dirs::get_pod_volume_dir(
            &tmp.to_string_lossy(),
            "uid-p",
            crate::pod_dirs::plugin::SECRET,
            "sv",
        );
        std::fs::create_dir_all(&dir).unwrap();
        let fifo = dir.join("k");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let vm = VolumeManager::new(
            tmp.to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        (vm, pod, storage, fifo)
    }

    /// Run `op` while a heartbeat ticks; the FIFO IO inside `op` blocks until
    /// we open the other end. Returns the ticks seen while it was blocked.
    async fn ticks_while_blocked<Fut>(fifo: std::path::PathBuf, op: Fut) -> usize
    where
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let beats = Arc::new(AtomicUsize::new(0));
        let b = beats.clone();
        let hb = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                b.fetch_add(1, Ordering::SeqCst);
            }
        });
        let task = tokio::spawn(op);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let ticks = beats.load(Ordering::SeqCst);
        // Release the blocked FIFO IO (O_RDWR satisfies either end).
        let release = std::thread::spawn(move || {
            // The code under test may block on several FIFO opens/reads in a
            // row, so keep re-opening: each hold lets the peer's open() complete
            // and each drop gives its read an EOF.
            for _ in 0..30 {
                let _held = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&fifo);
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
        hb.abort();
        let _ = release.join();
        ticks
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resync_volumes_does_not_block_the_runtime() {
        let (vm, pod, storage, fifo) = setup("resync").await;
        let ticks = ticks_while_blocked(fifo, async move {
            let _ = vm.resync_volumes(&pod, storage.as_ref()).await;
        })
        .await;
        assert!(
            ticks >= 5,
            "runtime starved by blocking fs in resync_volumes: {ticks} ticks in 300ms"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_volumes_does_not_block_the_runtime() {
        let (vm, pod, _storage, fifo) = setup("refresh").await;
        let ticks = ticks_while_blocked(fifo, async move {
            let _ = vm.refresh_volumes(&pod).await;
        })
        .await;
        assert!(
            ticks >= 5,
            "runtime starved by blocking fs in refresh_volumes: {ticks} ticks in 300ms"
        );
    }
}

/// #2913: a mounted projected `podCertificate` volume must pick up a rotated
/// credential bundle on resync. Upstream: the projected plugin
/// `RequiresRemount` (`pkg/volume/projected/projected.go:100-102`) makes the
/// reconciler re-run `SetUpAt` (`reconciler_common.go:185,210`), whose
/// `collectData` re-asks the pod certificate manager
/// (`projected.go:391-428`) and rewrites through the AtomicWriter.
#[cfg(all(test, unix))]
mod pod_certificate_resync_tests {
    use super::*;
    use rusternetes_storage::StorageBackend;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    /// A manager whose bundle the test rotates.
    struct RotatingManager(Mutex<(Vec<u8>, Vec<u8>)>);

    #[async_trait::async_trait]
    impl crate::podcertificate::Manager for RotatingManager {
        fn track_pod(&self, _pod: &Pod) {}
        fn forget_pod(&self, _pod: &Pod) {}
        async fn get_pod_certificate_credential_bundle(
            &self,
            _namespace: &str,
            _pod_name: &str,
            _pod_uid: &str,
            _volume_name: &str,
            _source_index: usize,
        ) -> Result<(Vec<u8>, Vec<u8>)> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn metric_report(&self) -> crate::podcertificate::MetricReport {
            Default::default()
        }
    }

    #[tokio::test]
    async fn resync_projects_rotated_pod_certificate() {
        let mgr = Arc::new(RotatingManager(Mutex::new((
            b"key1\n".to_vec(),
            b"cert1\n".to_vec(),
        ))));
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(StorageBackend::new_memory());
        let vm = VolumeManager::new_with_pod_certificate_manager(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            Some(mgr.clone()),
        );
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-pc"},
            "spec": {"containers": [], "volumes": [{
                "name": "creds",
                "projected": {"defaultMode": 420, "sources": [
                    {"podCertificate": {
                        "signerName": "example.com/signer",
                        "keyType": "ED25519",
                        "keyPath": "key.pem",
                        "certificateChainPath": "chain.pem",
                        "credentialBundlePath": "bundle.pem"
                    }}
                ]}
            }]}
        }))
        .unwrap();
        let volume = pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        let path = vm.create_volume(&pod, &volume).await.unwrap();
        assert_eq!(
            std::fs::read(format!("{path}/chain.pem")).unwrap(),
            b"cert1\n"
        );

        // An unchanged bundle must keep every file.
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        assert_eq!(std::fs::read(format!("{path}/key.pem")).unwrap(), b"key1\n");
        assert_eq!(
            std::fs::read(format!("{path}/chain.pem")).unwrap(),
            b"cert1\n"
        );

        // Rotate: the manager now hands out a new bundle.
        *mgr.0.lock().unwrap() = (b"key2\n".to_vec(), b"cert2\n".to_vec());
        vm.resync_volumes(&pod, storage.as_ref()).await.unwrap();
        assert_eq!(std::fs::read(format!("{path}/key.pem")).unwrap(), b"key2\n");
        assert_eq!(
            std::fs::read(format!("{path}/chain.pem")).unwrap(),
            b"cert2\n"
        );
        assert_eq!(
            std::fs::read(format!("{path}/bundle.pem")).unwrap(),
            b"key2\ncert2\n"
        );
    }
    /// A manager error leaves the volume untouched, as a failed `SetUpAt`
    /// writes nothing (`projected.go:393-396` appends to `errlist`;
    /// `:337` returns the aggregate; `SetUpAt` bails before the writer).
    #[tokio::test]
    async fn resync_keeps_files_when_manager_errors() {
        struct Failing;
        #[async_trait::async_trait]
        impl crate::podcertificate::Manager for Failing {
            fn track_pod(&self, _pod: &Pod) {}
            fn forget_pod(&self, _pod: &Pod) {}
            async fn get_pod_certificate_credential_bundle(
                &self,
                _: &str,
                _: &str,
                _: &str,
                _: &str,
                _: usize,
            ) -> Result<(Vec<u8>, Vec<u8>)> {
                Err(anyhow::anyhow!("no credential bundle yet"))
            }
            fn metric_report(&self) -> crate::podcertificate::MetricReport {
                Default::default()
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(StorageBackend::new_memory());
        let dir = tmp
            .path()
            .join("pods/uid-pc/volumes/kubernetes.io~projected/creds");
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-pc"},
            "spec": {"containers": [], "volumes": [{
                "name": "creds",
                "projected": {"sources": [
                    {"podCertificate": {
                        "signerName": "example.com/signer", "keyType": "ED25519",
                        "keyPath": "key.pem"
                    }}
                ]}
            }]}
        }))
        .unwrap();
        let ok = Arc::new(RotatingManager(Mutex::new((
            b"k\n".to_vec(),
            b"c\n".to_vec(),
        ))));
        let mk = |m: Arc<dyn crate::podcertificate::Manager>| {
            VolumeManager::new_with_pod_certificate_manager(
                tmp.path().to_string_lossy().to_string(),
                Some(storage.clone()),
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                Some(m),
            )
        };
        let volume = pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        mk(ok).create_volume(&pod, &volume).await.unwrap();
        mk(Arc::new(Failing))
            .resync_volumes(&pod, storage.as_ref())
            .await
            .unwrap();
        assert_eq!(std::fs::read(dir.join("key.pem")).unwrap(), b"k\n");
    }
}
