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
    ConfigMap, KeyToPath, PersistentVolume, PersistentVolumeClaim, Pod, Secret,
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
    configmap_name: &str,
    is_optional: bool,
    default_mode: u32,
) -> std::collections::BTreeMap<String, FileProjection> {
    let mut payload: std::collections::BTreeMap<String, FileProjection> =
        std::collections::BTreeMap::new();
    if let Some(items) = items {
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
                        data: v.clone(),
                        mode,
                    },
                );
            } else if !is_optional {
                warn!("ConfigMap {} missing key {}", configmap_name, item.key);
            }
        }
    } else {
        if let Some(data) = &configmap.data {
            for (k, v) in data {
                payload.insert(
                    k.clone(),
                    FileProjection {
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
                        data: v.clone(),
                        mode: default_mode,
                    },
                );
            }
        }
    }
    payload
}

/// Apply fsGroup group-ownership to volume trees in-process (no fork/exec).
///
/// Mirrors upstream `SetVolumeOwnership` (`pkg/volume/volume_linux.go`): change
/// the GROUP owner via `lchown` (owner left unchanged, symlinks not followed),
/// mirror the owner permission bits into the group bits (so a 0440 file becomes
/// group-readable but not group-writable), and set setgid on each root volume dir
/// so newly created files inherit the group. Every failure is returned, never
/// swallowed — the caller fails `start_pod` and the pod is retried rather than a
/// non-root container starting against a root-owned, unreadable file.
#[cfg(unix)]
fn apply_volume_ownership(paths: &[std::path::PathBuf], fs_group: i64) -> std::io::Result<()> {
    use rustix::fs::{chownat, AtFlags, Gid, CWD};
    use std::os::unix::fs::PermissionsExt;

    // SAFETY: `from_raw` requires the value to be a valid Unix group ID; fsGroup
    // comes straight from the pod's PodSecurityContext (an i64 GID, validated at
    // the API layer), so the cast/wrap is a plain reinterpretation, not UB.
    let gid = unsafe { Gid::from_raw(fs_group as u32) };

    fn chown_group(path: &std::path::Path, gid: Gid) -> std::io::Result<()> {
        // lchown: group only, do not follow symlinks (upstream os.Lchown).
        chownat(CWD, path, None, Some(gid), AtFlags::SYMLINK_NOFOLLOW).map_err(std::io::Error::from)
    }

    fn mirror_group_bits(path: &std::path::Path) -> std::io::Result<()> {
        let mode = std::fs::symlink_metadata(path)?.permissions().mode();
        let owner_bits = (mode >> 6) & 0o7;
        let new_mode = (mode & !0o070) | (owner_bits << 3);
        if new_mode != mode {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(new_mode))?;
        }
        Ok(())
    }

    fn walk(dir: &std::path::Path, gid: Gid) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            chown_group(&path, gid)?;
            let meta = std::fs::symlink_metadata(&path)?;
            if meta.file_type().is_symlink() {
                continue;
            }
            mirror_group_bits(&path)?;
            if meta.file_type().is_dir() {
                walk(&path, gid)?;
            }
        }
        Ok(())
    }

    for path in paths {
        chown_group(path, gid)?;
        walk(path, gid)?;
        // setgid + owner->group bit mirror on the root dir.
        let mode = std::fs::metadata(path)?.permissions().mode();
        let owner_bits = (mode >> 6) & 0o7;
        let new_mode = (mode & !0o070) | (owner_bits << 3) | 0o2000;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(new_mode))?;
    }
    Ok(())
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
    pub host: Arc<dyn crate::volume_plugins::VolumeHost>,
}

impl VolumeManager {
    /// Construct a `VolumeManager` from the same three values the bollard
    /// `ContainerRuntime` already carries.
    pub fn new(
        volumes_base_path: String,
        storage: Option<Arc<rusternetes_storage::StorageBackend>>,
        token_manager: rusternetes_common::auth::TokenManager,
    ) -> Self {
        let node_allocatable = crate::kubelet::node_allocatable_map();
        let host: Arc<dyn crate::volume_plugins::VolumeHost> =
            Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
                volumes_base_path.clone(),
                storage.clone(),
                token_manager.clone(),
                node_allocatable.clone(),
            ));
        let plugin_mgr = Arc::new(crate::volume_plugins::VolumePluginMgr::new(vec![
            Box::new(crate::volume_plugins::empty_dir::EmptyDirPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::host_path::HostPathPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::config_map::ConfigMapPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::secret::SecretPlugin::new(
                host.clone(),
            )),
        ]));
        Self {
            volumes_base_path,
            storage,
            token_manager,
            node_allocatable,
            plugin_mgr,
            host,
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
            crate::pod_dirs::plugin_for_volume(volume),
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

        // Apply fsGroup: change group ownership on all volume files.
        // Real Kubernetes behavior: fsGroup changes the group owner of volume files
        // to the specified GID, and sets group permission bits to match the owner bits
        // (i.e., if owner has read, group gets read; if owner has write, group gets write).
        // This preserves the defaultMode permissions — a file with mode 0440 stays 0440,
        // not 0460 (which would happen with unconditional g+rwX).
        #[cfg(unix)]
        if let Some(fs_group) = pod
            .spec
            .as_ref()
            .and_then(|s| s.security_context.as_ref())
            .and_then(|sc| sc.fs_group)
        {
            let paths: Vec<std::path::PathBuf> = volume_paths
                .values()
                .map(std::path::PathBuf::from)
                .collect();
            let n = paths.len();
            // Blocking recursive lchown syscalls run off the async worker so
            // concurrent pod setup does not starve the tokio runtime. Errors
            // propagate: a failed ownership application fails start_pod (retried)
            // rather than starting a non-root container against a root-owned file.
            tokio::task::spawn_blocking(move || apply_volume_ownership(&paths, fs_group))
                .await
                .context("fsGroup ownership task panicked")?
                .with_context(|| format!("failed to apply fsGroup {fs_group} to volumes"))?;
            info!("Applied fsGroup {} to {} volumes", fs_group, n);
        }

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
    pub async fn resync_volumes<S: rusternetes_storage::Storage>(
        &self,
        pod: &Pod,
        storage: &S,
    ) -> Result<()> {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        if let Some(volumes) = &pod.spec.as_ref().unwrap().volumes {
            for volume in volumes {
                // Resync secret volumes
                if let Some(secret_source) = &volume.secret {
                    let secret_name = match &secret_source.secret_name {
                        Some(n) => n,
                        None => continue,
                    };
                    let key =
                        rusternetes_storage::build_key("secrets", Some(namespace), secret_name);
                    let volume_dir = self.pod_volume_dir(pod, volume);
                    if let Ok(secret) = storage
                        .get::<rusternetes_common::resources::Secret>(&key)
                        .await
                    {
                        let mut expected_files: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        if let Some(data) = &secret.data {
                            if let Some(ref items) = secret_source.items {
                                // Only mount the specified keys at their mapped paths
                                for item in items {
                                    if let Some(v) = data.get(&item.key) {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        expected_files.insert(item.path.clone());
                                        if let Ok(existing) = std::fs::read(&file_path) {
                                            if existing == *v {
                                                continue;
                                            }
                                        }
                                        if let Some(parent) =
                                            std::path::Path::new(&file_path).parent()
                                        {
                                            let _ = std::fs::create_dir_all(parent);
                                        }
                                        let _ = std::fs::write(&file_path, v);
                                    }
                                }
                            } else {
                                // Mount all keys
                                for (k, v) in data {
                                    let file_path = format!("{}/{}", volume_dir, k);
                                    expected_files.insert(k.clone());
                                    // Only write if content changed
                                    if let Ok(existing) = std::fs::read(&file_path) {
                                        if existing == *v {
                                            continue;
                                        }
                                    }
                                    let _ = std::fs::write(&file_path, v);
                                }
                            }
                        }
                        // Remove files that are no longer expected
                        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                            for entry in entries.flatten() {
                                if let Some(name) = entry.file_name().to_str() {
                                    if !expected_files.contains(name) {
                                        let _ = std::fs::remove_file(entry.path());
                                    }
                                }
                            }
                        }
                    } else if secret_source.optional != Some(true) {
                        // Secret was deleted entirely — remove all files if not optional
                        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                            for entry in entries.flatten() {
                                let _ = std::fs::remove_file(entry.path());
                            }
                        }
                    }
                }
                // Resync configmap volumes. Re-project through the AtomicWriter
                // (same as the initial mount) so an unchanged ConfigMap is a true
                // no-op — never an in-place rewrite through the `..data` symlinks,
                // which would fire an fsnotify Write on the user-visible file and
                // crash a config watcher such as kube-proxy (#1652).
                if let Some(cm_source) = &volume.config_map {
                    if let Some(cm_name) = &cm_source.name {
                        let key =
                            rusternetes_storage::build_key("configmaps", Some(namespace), cm_name);
                        if let Ok(cm) = storage
                            .get::<rusternetes_common::resources::ConfigMap>(&key)
                            .await
                        {
                            let volume_dir = self.pod_volume_dir(pod, volume);
                            let is_optional = cm_source.optional.unwrap_or(false);
                            let mode = cm_source.default_mode.unwrap_or(0o644) as u32;
                            let payload = build_configmap_payload(
                                &cm,
                                cm_source.items.as_ref(),
                                cm_name,
                                is_optional,
                                mode,
                            );
                            let _ = crate::atomic_writer::write_projected_payload(
                                std::path::Path::new(&volume_dir),
                                &payload,
                            );
                        }
                    }
                }
                // Resync projected volumes (may contain configmap/secret projections)
                if let Some(projected) = &volume.projected {
                    if let Some(sources) = &projected.sources {
                        let volume_dir = self.pod_volume_dir(pod, volume);
                        // Track expected files so we can delete stale ones
                        let mut expected_files: std::collections::HashSet<String> =
                            std::collections::HashSet::new();

                        // Per-file permissions must survive a resync rewrite, matching
                        // the initial-mount path and upstream's atomic writer (which
                        // re-applies each file's mode on every update). A plain
                        // `fs::write` of a *new* key (added after pod start) would
                        // otherwise leave it at the umask default instead of the
                        // item's `mode` / the projection `defaultMode` (#1050).
                        let proj_default_mode = projected.default_mode.unwrap_or(0o644);
                        let apply_mode = |path: &str, mode: i32| {
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                let _ = std::fs::set_permissions(
                                    path,
                                    std::fs::Permissions::from_mode(mode as u32),
                                );
                            }
                            #[cfg(not(unix))]
                            {
                                let _ = (path, mode);
                            }
                        };

                        for source in sources {
                            if let Some(cm_proj) = &source.config_map {
                                if let Some(cm_name) = &cm_proj.name {
                                    let key = rusternetes_storage::build_key(
                                        "configmaps",
                                        Some(namespace),
                                        cm_name,
                                    );
                                    if let Ok(cm) = storage
                                        .get::<rusternetes_common::resources::ConfigMap>(&key)
                                        .await
                                    {
                                        if let Some(items) = &cm_proj.items {
                                            // Selective projection — only mount specified keys
                                            for item in items {
                                                let file_path =
                                                    format!("{}/{}", volume_dir, item.path);
                                                expected_files.insert(file_path.clone());
                                                if let Some(value) =
                                                    cm.data.as_ref().and_then(|d| d.get(&item.key))
                                                {
                                                    if let Ok(existing) =
                                                        std::fs::read_to_string(&file_path)
                                                    {
                                                        if existing == *value {
                                                            continue;
                                                        }
                                                    }
                                                    if let Some(parent) =
                                                        std::path::Path::new(&file_path).parent()
                                                    {
                                                        let _ = std::fs::create_dir_all(parent);
                                                    }
                                                    if std::fs::write(&file_path, value).is_ok() {
                                                        apply_mode(
                                                            &file_path,
                                                            item.mode.unwrap_or(proj_default_mode),
                                                        );
                                                    }
                                                }
                                            }
                                        } else if let Some(data) = &cm.data {
                                            // Mount all keys
                                            for (k, v) in data {
                                                let file_path = format!("{}/{}", volume_dir, k);
                                                expected_files.insert(file_path.clone());
                                                if let Ok(existing) =
                                                    std::fs::read_to_string(&file_path)
                                                {
                                                    if existing == *v {
                                                        continue;
                                                    }
                                                }
                                                if std::fs::write(&file_path, v).is_ok() {
                                                    apply_mode(&file_path, proj_default_mode);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if let Some(sec_proj) = &source.secret {
                                if let Some(sec_name) = &sec_proj.name {
                                    let key = rusternetes_storage::build_key(
                                        "secrets",
                                        Some(namespace),
                                        sec_name,
                                    );
                                    if let Ok(secret) = storage
                                        .get::<rusternetes_common::resources::Secret>(&key)
                                        .await
                                    {
                                        if let Some(data) = &secret.data {
                                            if let Some(items) = &sec_proj.items {
                                                for item in items {
                                                    let file_path =
                                                        format!("{}/{}", volume_dir, item.path);
                                                    expected_files.insert(file_path.clone());
                                                    if let Some(v) = data.get(&item.key) {
                                                        if let Ok(existing) =
                                                            std::fs::read(&file_path)
                                                        {
                                                            if existing == *v {
                                                                apply_mode(
                                                                    &file_path,
                                                                    item.mode.unwrap_or(
                                                                        proj_default_mode,
                                                                    ),
                                                                );
                                                                continue;
                                                            }
                                                        }
                                                        if let Some(parent) =
                                                            std::path::Path::new(&file_path)
                                                                .parent()
                                                        {
                                                            let _ = std::fs::create_dir_all(parent);
                                                        }
                                                        if std::fs::write(&file_path, v).is_ok() {
                                                            apply_mode(
                                                                &file_path,
                                                                item.mode
                                                                    .unwrap_or(proj_default_mode),
                                                            );
                                                        }
                                                    }
                                                }
                                            } else {
                                                for (k, v) in data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    expected_files.insert(file_path.clone());
                                                    if let Ok(existing) = std::fs::read(&file_path)
                                                    {
                                                        if existing == *v {
                                                            apply_mode(
                                                                &file_path,
                                                                proj_default_mode,
                                                            );
                                                            continue;
                                                        }
                                                    }
                                                    if std::fs::write(&file_path, v).is_ok() {
                                                        apply_mode(&file_path, proj_default_mode);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // ServiceAccountToken projection resync — preserve the token file
                            if let Some(sa_token) = &source.service_account_token {
                                let file_path = format!("{}/{}", volume_dir, sa_token.path);
                                expected_files.insert(file_path);
                            }
                            // DownwardAPI projection resync
                            if let Some(downward_api) = &source.downward_api {
                                if let Some(items) = &downward_api.items {
                                    for item in items {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        expected_files.insert(file_path.clone());
                                        let value = if let Some(ref field_ref) = item.field_ref {
                                            self.get_pod_field_value(pod, &field_ref.field_path)
                                                .unwrap_or_default()
                                        } else if let Some(ref resource_ref) =
                                            item.resource_field_ref
                                        {
                                            self.get_container_resource_value(pod, resource_ref)
                                                .unwrap_or_default()
                                        } else {
                                            String::new()
                                        };
                                        if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                            if existing == value {
                                                continue;
                                            }
                                        }
                                        let _ = std::fs::write(&file_path, &value);
                                    }
                                }
                            }
                        }

                        // Delete stale files that are no longer in any projection source
                        if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                            for entry in entries.flatten() {
                                let path = entry.path();
                                if path.is_file() {
                                    let path_str = path.to_string_lossy().to_string();
                                    if !expected_files.contains(&path_str) {
                                        let _ = std::fs::remove_file(&path);
                                    }
                                }
                            }
                        }
                    }
                }
                // Resync standalone downwardAPI volumes
                if let Some(downward_api) = &volume.downward_api {
                    if let Some(items) = &downward_api.items {
                        let volume_dir = self.pod_volume_dir(pod, volume);
                        for item in items {
                            let file_path = format!("{}/{}", volume_dir, item.path);
                            let value = if let Some(ref field_ref) = item.field_ref {
                                self.get_pod_field_value(pod, &field_ref.field_path)
                                    .unwrap_or_default()
                            } else if let Some(ref resource_ref) = item.resource_field_ref {
                                self.get_container_resource_value(pod, resource_ref)
                                    .unwrap_or_default()
                            } else {
                                String::new()
                            };
                            if let Ok(existing) = std::fs::read_to_string(&file_path) {
                                if existing == value {
                                    continue;
                                }
                            }
                            let _ = std::fs::write(&file_path, &value);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Create a single volume and return its host path
    pub(crate) async fn create_volume(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) -> Result<String> {
        // Used for content that legitimately carries the pod's NAME — the
        // service-account token audience, the generated ephemeral-PVC name, and
        // projected sources. On-disk paths never use it; those go through
        // `pod_volume_dir`, which keys on the pod UID.
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        // EmptyDir: create a directory on the shared volumes path.
        // K8s ref: pkg/volume/emptydir/empty_dir.go — setupDir() sets mode 0777.
        // Note: host bind mounts through virtiofs (Podman Machine / Docker Desktop)
        // may not enforce chmod correctly. The emptyDir 0777/0666 permission tests
        // are pre-existing failures on macOS VM-based runtimes. On Linux (where
        // conformance actually runs), bind mounts preserve mode bits, so setup_emptydir_dir
        // ensures the directory exists with mode 0o777 and idempotently re-chmods even
        // when the directory pre-exists from a prior run.
        if volume.empty_dir.is_some() {
            let spec = crate::volume_plugins::Spec {
                volume,
                persistent_volume: None,
            };
            let plugin = crate::volume_plugins::empty_dir::EmptyDirPlugin::new(self.host.clone());
            let mounter = plugin.new_mounter(&spec, pod).await?;
            mounter.set_up().await?;
            return Ok(mounter.get_path());
        }

        // HostPath: use the specified host path. The `type` field is validated
        // (and "OrCreate" variants are materialised) via `check_host_path_type`,
        // mirroring upstream `pkg/volume/host_path/host_path.go::checkType` —
        // see also tests/conformance_storage_emptydir_hostpath.rs.
        if volume.host_path.is_some() {
            let spec = crate::volume_plugins::Spec {
                volume,
                persistent_volume: None,
            };
            let plugin = crate::volume_plugins::host_path::HostPathPlugin::new(self.host.clone());
            let mounter = plugin.new_mounter(&spec, pod).await?;
            mounter.set_up().await?;
            return Ok(mounter.get_path());
        }

        // ConfigMap: mount configmap data as files
        if volume.config_map.is_some() {
            let spec = crate::volume_plugins::Spec {
                volume,
                persistent_volume: None,
            };
            let plugin = crate::volume_plugins::config_map::ConfigMapPlugin::new(self.host.clone());
            let mounter = plugin.new_mounter(&spec, pod).await?;
            mounter.set_up().await?;
            return Ok(mounter.get_path());
        }

        // Secret: mount secret data as files
        if volume.secret.is_some() {
            let spec = crate::volume_plugins::Spec {
                volume,
                persistent_volume: None,
            };
            let plugin = crate::volume_plugins::secret::SecretPlugin::new(self.host.clone());
            let mounter = plugin.new_mounter(&spec, pod).await?;
            mounter.set_up().await?;
            return Ok(mounter.get_path());
        }

        // PersistentVolumeClaim: find bound PV and use its path
        if let Some(pvc_source) = &volume.persistent_volume_claim {
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

            // Get the host path from the PV
            let path = if let Some(hp) = &pv.spec.host_path {
                hp.path.clone()
            } else {
                return Err(anyhow::anyhow!(
                    "PersistentVolume does not have a hostPath volume source"
                ));
            };
            info!(
                "Using PersistentVolumeClaim volume {} backed by PV {} at {}",
                volume.name, pv_name, path
            );
            return Ok(path);
        }

        // DownwardAPI: expose pod/container metadata as files
        if let Some(downward_api) = &volume.downward_api {
            let volume_dir = self.pod_volume_dir(pod, volume);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create DownwardAPI volume directory")?;

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let da_default_mode = downward_api.default_mode.unwrap_or(0o644);

            // Compute final directory permissions (applied after files are written)
            #[cfg(unix)]
            let da_dir_mode = da_default_mode as u32 | 0o111;

            if let Some(items) = &downward_api.items {
                for item in items {
                    let file_path = format!("{}/{}", volume_dir, item.path);

                    // Create parent directories if needed
                    if let Some(parent) = std::path::Path::new(&file_path).parent() {
                        std::fs::create_dir_all(parent)?;
                    }

                    // Get the value from field_ref or resource_field_ref
                    let value = if let Some(field_ref) = &item.field_ref {
                        self.get_pod_field_value(pod, &field_ref.field_path)?
                    } else if let Some(resource_ref) = &item.resource_field_ref {
                        self.get_container_resource_value(pod, resource_ref)?
                    } else {
                        return Err(anyhow::anyhow!(
                            "DownwardAPI item must have either fieldRef or resourceFieldRef"
                        ));
                    };

                    std::fs::write(&file_path, value).with_context(|| {
                        format!("Failed to write DownwardAPI file {}", file_path)
                    })?;

                    // Set file permissions: per-item mode overrides defaultMode
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let mode = item.mode.unwrap_or(da_default_mode) as u32;
                        std::fs::set_permissions(
                            &file_path,
                            std::fs::Permissions::from_mode(mode),
                        )?;
                    }

                    info!(
                        "Wrote DownwardAPI file {} with value from {}",
                        file_path, item.path
                    );
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(da_dir_mode),
                )?;
            }

            info!(
                "Created DownwardAPI volume {} at {}",
                volume.name, volume_dir
            );
            return Ok(volume_dir);
        }

        // CSI: ephemeral inline volume (handled by external CSI driver)
        if let Some(_csi) = &volume.csi {
            // CSI ephemeral inline volumes are managed by the CSI driver via the kubelet CSI plugin
            // For conformance, we create a placeholder directory and rely on the CSI driver to populate it
            let volume_dir = self.pod_volume_dir(pod, volume);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create CSI volume directory")?;

            info!(
                "Created CSI ephemeral volume {} at {} (managed by CSI driver)",
                volume.name, volume_dir
            );
            return Ok(volume_dir);
        }

        // Ephemeral: generic ephemeral volume with PVC template
        if let Some(ephemeral) = &volume.ephemeral {
            if let Some(pvc_template) = &ephemeral.volume_claim_template {
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
                let pvc: PersistentVolumeClaim = storage
                    .get(&pvc_key)
                    .await
                    .with_context(|| format!("Ephemeral PVC {} not found", pvc_name))?;

                if let Some(pv_name) = &pvc.spec.volume_name {
                    let pv_key = build_key("persistentvolumes", None, pv_name);
                    let pv: PersistentVolume = storage.get(&pv_key).await.with_context(|| {
                        format!(
                            "PersistentVolume {} not found for ephemeral volume",
                            pv_name
                        )
                    })?;

                    let path = if let Some(hp) = &pv.spec.host_path {
                        hp.path.clone()
                    } else {
                        return Err(anyhow::anyhow!(
                            "PersistentVolume does not have a hostPath volume source"
                        ));
                    };

                    info!(
                        "Using ephemeral volume {} backed by PVC {} and PV {} at {}",
                        volume.name, pvc_name, pv_name, path
                    );
                    return Ok(path);
                } else {
                    return Err(anyhow::anyhow!(
                        "Ephemeral PVC {} is not bound yet",
                        pvc_name
                    ));
                }
            }
        }

        // Projected: combine multiple volume sources (configMap, secret, downwardAPI, serviceAccountToken) into one directory
        if let Some(projected) = &volume.projected {
            let volume_dir = self.pod_volume_dir(pod, volume);
            std::fs::create_dir_all(&volume_dir)
                .context("Failed to create projected volume directory")?;

            // Determine the default file mode: spec defaultMode, or 0644 (Kubernetes default)
            let proj_default_mode = projected.default_mode.unwrap_or(0o644);

            // Compute final directory permissions (will be applied after files are written)
            #[cfg(unix)]
            let proj_dir_mode = proj_default_mode as u32 | 0o111;

            if let Some(sources) = &projected.sources {
                let storage = self.storage.as_ref();

                for source in sources {
                    // ConfigMap projection
                    if let Some(cm_proj) = &source.config_map {
                        if let Some(cm_name) = &cm_proj.name {
                            let key = build_key("configmaps", Some(namespace), cm_name);
                            if let Some(storage) = storage {
                                match storage.get::<ConfigMap>(&key).await {
                                    Ok(cm) => {
                                        // Helper to write a projected file with permissions
                                        let write_proj_file =
                                            |path: &str, content: &[u8], mode: i32| -> Result<()> {
                                                if let Some(parent) =
                                                    std::path::Path::new(path).parent()
                                                {
                                                    std::fs::create_dir_all(parent)?;
                                                }
                                                std::fs::write(path, content)?;
                                                #[cfg(unix)]
                                                {
                                                    use std::os::unix::fs::PermissionsExt;
                                                    std::fs::set_permissions(
                                                        path,
                                                        std::fs::Permissions::from_mode(
                                                            mode as u32,
                                                        ),
                                                    )?;
                                                }
                                                Ok(())
                                            };

                                        if let Some(items) = &cm_proj.items {
                                            for item in items {
                                                let mode = item.mode.unwrap_or(proj_default_mode);
                                                let file_path =
                                                    format!("{}/{}", volume_dir, item.path);
                                                // Try data first, then binaryData
                                                if let Some(value) =
                                                    cm.data.as_ref().and_then(|d| d.get(&item.key))
                                                {
                                                    write_proj_file(
                                                        &file_path,
                                                        value.as_bytes(),
                                                        mode,
                                                    )?;
                                                } else if let Some(value) = cm
                                                    .binary_data
                                                    .as_ref()
                                                    .and_then(|d| d.get(&item.key))
                                                {
                                                    write_proj_file(&file_path, value, mode)?;
                                                }
                                            }
                                        } else {
                                            // Mount all keys from data
                                            if let Some(data) = &cm.data {
                                                for (k, v) in data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    write_proj_file(
                                                        &file_path,
                                                        v.as_bytes(),
                                                        proj_default_mode,
                                                    )?;
                                                }
                                            }
                                            // Mount all keys from binaryData
                                            if let Some(binary_data) = &cm.binary_data {
                                                for (k, v) in binary_data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    write_proj_file(
                                                        &file_path,
                                                        v,
                                                        proj_default_mode,
                                                    )?;
                                                }
                                            }
                                        }
                                    }
                                    Err(_) if cm_proj.optional.unwrap_or(false) => {
                                        // Optional configmap not found, skip
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to get ConfigMap {} for projected volume: {}. Skipping.",
                                            cm_name, e
                                        );
                                    }
                                }
                            }
                        }
                    }

                    // Secret projection
                    if let Some(secret_proj) = &source.secret {
                        if let Some(secret_name) = &secret_proj.name {
                            let key = build_key("secrets", Some(namespace), secret_name);
                            if let Some(storage) = storage {
                                match storage.get::<Secret>(&key).await {
                                    Ok(secret) => {
                                        if let Some(data) = &secret.data {
                                            if let Some(items) = &secret_proj.items {
                                                for item in items {
                                                    if let Some(value) = data.get(&item.key) {
                                                        let file_path =
                                                            format!("{}/{}", volume_dir, item.path);
                                                        if let Some(parent) =
                                                            std::path::Path::new(&file_path)
                                                                .parent()
                                                        {
                                                            std::fs::create_dir_all(parent)?;
                                                        }
                                                        std::fs::write(&file_path, value)?;
                                                        #[cfg(unix)]
                                                        {
                                                            use std::os::unix::fs::PermissionsExt;
                                                            let mode = item
                                                                .mode
                                                                .unwrap_or(proj_default_mode)
                                                                as u32;
                                                            std::fs::set_permissions(
                                                                &file_path,
                                                                std::fs::Permissions::from_mode(
                                                                    mode,
                                                                ),
                                                            )?;
                                                        }
                                                    }
                                                }
                                            } else {
                                                for (k, v) in data {
                                                    let file_path = format!("{}/{}", volume_dir, k);
                                                    std::fs::write(&file_path, v)?;
                                                    #[cfg(unix)]
                                                    {
                                                        use std::os::unix::fs::PermissionsExt;
                                                        std::fs::set_permissions(
                                                            &file_path,
                                                            std::fs::Permissions::from_mode(
                                                                proj_default_mode as u32,
                                                            ),
                                                        )?;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(_) if secret_proj.optional.unwrap_or(false) => {
                                        // Optional secret not found, skip
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to get Secret {} for projected volume: {}. Skipping.",
                                            secret_name, e
                                        );
                                    }
                                }
                            }
                        }
                    }

                    // DownwardAPI projection
                    if let Some(downward_api) = &source.downward_api {
                        if let Some(items) = &downward_api.items {
                            for item in items {
                                let file_path = format!("{}/{}", volume_dir, item.path);
                                if let Some(parent) = std::path::Path::new(&file_path).parent() {
                                    std::fs::create_dir_all(parent)?;
                                }
                                let value = if let Some(ref field_ref) = item.field_ref {
                                    self.get_pod_field_value(pod, &field_ref.field_path)
                                        .unwrap_or_default()
                                } else if let Some(ref resource_ref) = item.resource_field_ref {
                                    self.get_container_resource_value(pod, resource_ref)
                                        .unwrap_or_default()
                                } else {
                                    String::new()
                                };
                                std::fs::write(&file_path, &value)?;
                                #[cfg(unix)]
                                {
                                    use std::os::unix::fs::PermissionsExt;
                                    let mode = item.mode.unwrap_or(proj_default_mode) as u32;
                                    std::fs::set_permissions(
                                        &file_path,
                                        std::fs::Permissions::from_mode(mode),
                                    )?;
                                }
                            }
                        }
                    }

                    // ServiceAccountToken projection
                    if let Some(sa_token) = &source.service_account_token {
                        let token_path = format!("{}/{}", volume_dir, sa_token.path);
                        if let Some(parent) = std::path::Path::new(&token_path).parent() {
                            std::fs::create_dir_all(parent)?;
                        }
                        // Generate a real JWT token bound to this pod
                        let sa_name = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.service_account_name.as_deref())
                            .unwrap_or("default");
                        let sa_uid = if let Some(storage) = storage {
                            let sa_key = build_key("serviceaccounts", Some(namespace), sa_name);
                            match storage
                                .get::<rusternetes_common::resources::ServiceAccount>(&sa_key)
                                .await
                            {
                                Ok(sa) => sa.metadata.uid.clone(),
                                Err(_) => String::new(),
                            }
                        } else {
                            String::new()
                        };
                        // TokenRequest requires expirationSeconds >= 600 (10m).
                        let expiration_seconds =
                            sa_token.expiration_seconds.unwrap_or(3600).max(600);
                        let now = chrono::Utc::now();
                        let exp = now.timestamp() + expiration_seconds;
                        // Audience to REQUEST from the api-server: exactly what the
                        // projection asked for (empty => the api-server's own
                        // default api-audience, which it will then accept — do NOT
                        // force "rusternetes", or a vanilla api-server issues a
                        // token whose audience it rejects on use).
                        let requested_audiences: Vec<String> =
                            sa_token.audience.iter().cloned().collect();
                        // Audience baked into the self-mint FALLBACK claims (native
                        // storage-mode only): default to "rusternetes".
                        let mut audiences = vec!["rusternetes".to_string()];
                        if let Some(ref aud) = sa_token.audience {
                            audiences = vec![aud.clone()];
                        }
                        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.clone());
                        let node_uid = if let (Some(ref nn), Some(st)) = (&node_name, storage) {
                            let node_key = build_key("nodes", None::<&str>, nn);
                            st.get::<serde_json::Value>(&node_key)
                                .await
                                .ok()
                                .and_then(|v| {
                                    v.pointer("/metadata/uid")
                                        .and_then(|u| u.as_str())
                                        .map(|s| s.to_string())
                                })
                        } else {
                            None
                        };
                        let claims = rusternetes_common::auth::ServiceAccountClaims {
                            sub: format!("system:serviceaccount:{}:{}", namespace, sa_name),
                            namespace: namespace.to_string(),
                            uid: sa_uid.clone(),
                            iat: now.timestamp(),
                            exp,
                            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
                            aud: audiences.clone(),
                            kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
                                namespace: namespace.to_string(),
                                svcacct: rusternetes_common::auth::KubeRef {
                                    name: sa_name.to_string(),
                                    uid: sa_uid,
                                },
                                pod: Some(rusternetes_common::auth::KubeRef {
                                    name: pod_name.clone(),
                                    uid: pod.metadata.uid.clone(),
                                }),
                                node: node_name.as_ref().map(|nn| {
                                    rusternetes_common::auth::KubeRef {
                                        name: nn.clone(),
                                        uid: node_uid.clone().unwrap_or_default(),
                                    }
                                }),
                            }),
                            pod_name: Some(pod_name.clone()),
                            pod_uid: Some(pod.metadata.uid.clone()),
                            node_name,
                            node_uid,
                        };
                        // Reuse a still-fresh token: re-mint only when the file is
                        // missing or past ~80% of its lifetime. The per-sync volume
                        // re-creation would otherwise hit the api-server TokenRequest
                        // endpoint every few seconds per pod and churn the token file.
                        let refresh_after = (expiration_seconds * 8 / 10).max(60);
                        let token_fresh = std::fs::metadata(&token_path)
                            .and_then(|m| m.modified())
                            .ok()
                            .and_then(|mt| mt.elapsed().ok())
                            .map(|age| (age.as_secs() as i64) < refresh_after)
                            .unwrap_or(false);
                        if !token_fresh {
                            // Prefer an api-server-issued bound token (TokenRequest),
                            // matching the upstream kubelet (pkg/kubelet/token) — it
                            // never self-signs. A vanilla api-server only trusts
                            // tokens IT signed, so a self-minted token is 401-rejected
                            // for in-cluster clients (kindnet et al.). Self-mint only
                            // as a fallback for the storage-direct backends
                            // (all-in-one), whose co-located api-server trusts our key.
                            let mut issued: Option<String> = None;
                            if let Some(st) = storage {
                                // Bind the token to this pod, as upstream's
                                // projected volume plugin does
                                // (pkg/volume/projected/projected.go). The
                                // api-server derives the pod/node claims from
                                // the ref, and a TokenReview on the mounted
                                // token then reports the
                                // authentication.kubernetes.io/pod-name,
                                // pod-uid and node-name extras (#1684).
                                match st
                                    .create_sa_token(
                                        namespace,
                                        sa_name,
                                        &requested_audiences,
                                        expiration_seconds,
                                        Some((pod_name.as_str(), pod.metadata.uid.as_str())),
                                    )
                                    .await
                                {
                                    Ok(Some(t)) => issued = Some(t),
                                    Ok(None) => {}
                                    Err(e) => warn!(
                                        "TokenRequest for {}/{} failed: {}; self-minting",
                                        namespace, sa_name, e
                                    ),
                                }
                            }
                            let token = match issued {
                                Some(t) => t,
                                None => match self.token_manager.generate_token(claims) {
                                    Ok(t) => t,
                                    Err(e) => {
                                        warn!(
                                    "Failed to generate SA token for pod {}: {}, using placeholder",
                                    pod_name, e
                                );
                                        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.placeholder"
                                            .to_string()
                                    }
                                },
                            };
                            std::fs::write(&token_path, &token)?;
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                std::fs::set_permissions(
                                    &token_path,
                                    std::fs::Permissions::from_mode(proj_default_mode as u32),
                                )?;
                            }
                        } // end if !token_fresh
                    }
                }
            }

            // Set directory permissions after files are written so that restrictive
            // defaultMode values don't prevent file creation.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &volume_dir,
                    std::fs::Permissions::from_mode(proj_dir_mode),
                )?;
            }

            info!("Created projected volume {} at {}", volume.name, volume_dir);
            return Ok(volume_dir);
        }

        // Fallback: create an empty directory for unrecognized volume types
        // (e.g. nfs, iscsi, image, or any future types)
        // This prevents pod startup failures for volumes we don't natively handle.
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

    /// Refresh Secret and ConfigMap volumes for a running pod.
    /// Re-reads the data from storage and overwrites files on disk.
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
            let volume_dir = self.pod_volume_dir(pod, volume);

            // Refresh Secret volumes
            if let Some(secret_source) = &volume.secret {
                // Create volume dir if it doesn't exist (optional Secret that was created later)
                let _ = std::fs::create_dir_all(&volume_dir);
                let secret_name = match &secret_source.secret_name {
                    Some(n) => n,
                    None => continue,
                };
                let secret_key =
                    rusternetes_storage::build_key("secrets", Some(namespace), secret_name);
                match storage
                    .get::<rusternetes_common::resources::Secret>(&secret_key)
                    .await
                {
                    Ok(secret) => {
                        if let Some(data) = &secret.data {
                            let items = secret_source.items.as_ref();
                            if let Some(items) = items {
                                for item in items {
                                    if let Some(value) = data.get(&item.key) {
                                        let file_path = format!("{}/{}", volume_dir, item.path);
                                        let _ = std::fs::write(&file_path, value);
                                    }
                                }
                            } else {
                                // Write all current keys
                                for (key, value) in data {
                                    let file_path = format!("{}/{}", volume_dir, key);
                                    let _ = std::fs::write(&file_path, value);
                                }
                                // Delete files for keys that no longer exist
                                if let Ok(entries) = std::fs::read_dir(&volume_dir) {
                                    for entry in entries.flatten() {
                                        if let Some(fname) = entry.file_name().to_str() {
                                            if !data.contains_key(fname)
                                                && fname != "..data"
                                                && fname != "ca.crt"
                                            {
                                                let _ = std::fs::remove_file(entry.path());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Secret was deleted — if optional, remove all volume files
                        let is_optional = secret_source.optional.unwrap_or(false);
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

            // Refresh ConfigMap volumes
            if let Some(cm_source) = &volume.config_map {
                let _ = std::fs::create_dir_all(&volume_dir);
                let cm_name = match &cm_source.name {
                    Some(n) => n,
                    None => continue,
                };
                let cm_key = rusternetes_storage::build_key("configmaps", Some(namespace), cm_name);
                match storage
                    .get::<rusternetes_common::resources::ConfigMap>(&cm_key)
                    .await
                {
                    Ok(cm) => {
                        // Re-project through the AtomicWriter (same path as the
                        // initial mount): an unchanged ConfigMap is a true no-op,
                        // and key removals are handled by the writer's stale
                        // user-visible-symlink pruning. This must NOT rewrite the
                        // user-visible files in place — that follows the `..data`
                        // symlinks and fires an fsnotify Write, crash-looping a
                        // config watcher like kube-proxy (#1652).
                        let is_optional = cm_source.optional.unwrap_or(false);
                        let mode = cm_source.default_mode.unwrap_or(0o644) as u32;
                        let payload = build_configmap_payload(
                            &cm,
                            cm_source.items.as_ref(),
                            cm_name,
                            is_optional,
                            mode,
                        );
                        let _ = crate::atomic_writer::write_projected_payload(
                            std::path::Path::new(&volume_dir),
                            &payload,
                        );
                    }
                    Err(_) => {
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

    /// Get a pod field value for DownwardAPI
    /// Resolve a downwardAPI/projected volume item's `fieldRef` to the string
    /// written into the volume file.
    ///
    /// Delegates to [`crate::downward_api::resolve_pod_field`] so the volume and
    /// env-var paths render `metadata.labels['x']`, `status.podIP` and friends
    /// identically. The hand-rolled copy this replaced was a near-duplicate that
    /// had drifted: it hardcoded the bracket-key offsets (`&path[17..]`, so only
    /// single quotes parsed) where the shared helper accepts the double-quoted
    /// and unquoted forms the API allows too.
    pub(crate) fn get_pod_field_value(&self, pod: &Pod, field_path: &str) -> Result<String> {
        crate::downward_api::resolve_pod_field(pod, field_path).map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Resolve a downwardAPI/projected volume item's `resourceFieldRef` to the
    /// string written into the volume file.
    ///
    /// Delegates to [`crate::downward_api::resolve_container_resource`], the port
    /// of upstream `ExtractResourceValueByContainerNameAndNodeAllocatable`, which
    /// is exactly what `pkg/volume/downwardapi/downwardapi.go:266` calls. The env
    /// var path ([`crate::cri_runtime::translate`]) resolves the same selectors
    /// through the same function, so a `resourceFieldRef` cannot render one value
    /// as a file and a different one as an environment variable.
    ///
    /// This used to be a second, hand-rolled copy that (a) fell back to a
    /// hardcoded 4 cores / 8 GiB instead of consulting the node's allocatable —
    /// which reported `limits.ephemeral-storage` as 8 GiB on a node advertising
    /// 100 GiB — (b) never searched init containers, and (c) had no
    /// requests-default-to-limits fallback.
    pub(crate) fn get_container_resource_value(
        &self,
        pod: &Pod,
        resource_ref: &rusternetes_common::resources::ResourceFieldSelector,
    ) -> Result<String> {
        crate::downward_api::resolve_container_resource(
            pod,
            resource_ref,
            Some(&self.node_allocatable),
        )
        .map_err(|e| anyhow::anyhow!("{e}"))
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

        let payload = build_configmap_payload(&configmap, Some(&items), "cm", false, 0o644);

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
        let payload = build_configmap_payload(&configmap, None, "cm", false, 0o400);

        assert_eq!(payload.len(), 2);
        assert!(payload.values().all(|p| p.mode == 0o400));
    }
}

#[cfg(all(test, unix))]
mod projected_mode_tests {
    use super::*;
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

    /// fsGroup ownership must be applied IN-PROCESS, return Ok, preserve a 0440
    /// file's mode (group gets owner's read, not write), and set setgid on the root
    /// dir. Chowns to the file's OWN current gid so the lchown is permitted without
    /// root and the assertion is environment-independent (a real cross-group change
    /// is exercised by the root-privileged integration run, Task 3). Regression
    /// guard for the flaky fork/exec `chown -R` whose failures were swallowed.
    #[cfg(unix)]
    #[test]
    fn apply_volume_ownership_preserves_mode_and_sets_setgid() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let tmp = std::env::temp_dir().join(format!("rn-fsg-ok-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let file = tmp.join("secret-key");
        std::fs::write(&file, b"value").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o440)).unwrap();

        // The file's current gid is the process egid — chowning to it is permitted
        // without CAP_CHOWN, so this exercises the full syscall path hermetically.
        let gid = std::fs::metadata(&file).unwrap().gid() as i64;
        apply_volume_ownership(std::slice::from_ref(&tmp), gid).expect("ownership must succeed");

        let meta = std::fs::metadata(&file).unwrap();
        assert_eq!(meta.gid() as i64, gid, "file group must be the applied gid");
        assert_eq!(
            meta.permissions().mode() & 0o777,
            0o440,
            "0440 must be preserved (group gets owner's read, not write)"
        );
        let dir_mode = std::fs::metadata(&tmp).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o2000, 0o2000, "root dir must be setgid");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// An ownership failure must PROPAGATE (return Err), not be silently swallowed
    /// like the old `let _ = ...chown...output()`. A non-existent path yields ENOENT
    /// (returned before the gid is even used).
    #[cfg(unix)]
    #[test]
    fn apply_volume_ownership_propagates_errors() {
        let missing = std::env::temp_dir().join(format!("rn-fsg-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&missing);
        let err = apply_volume_ownership(&[missing], 0);
        assert!(
            err.is_err(),
            "missing path must return Err, not be swallowed"
        );
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
    /// `395a9c08` (the pre-move commit) to confirm it characterizes the OLD
    /// behaviour too, not just whatever the new code happens to do — see the
    /// task-6 report for both runs.
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

        // secret_dir_mode = defaultMode | 0o111 (secret.rs's moved body)
        let dir_mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o777, 0o400 | 0o111);
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
