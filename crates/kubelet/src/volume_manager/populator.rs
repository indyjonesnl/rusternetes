//! Port of `pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go`:
//! the loop that keeps the desired state of the world in sync with the pods the
//! kubelet should be running.
//!
//! It periodically walks the active pods and makes sure each one that has
//! volumes exists in the desired state of the world, and drops the pods whose
//! runtime has been removed.
//!
//! Deviations (each is the minimum the kubelet's shape forces):
//!
//! - `kubeClient` is the kubelet's storage backend: `getPVCExtractPV` and
//!   `getPVSpec` read the PVC and PV from it.
//! - `Run` takes `sources_ready` as a closure. Upstream's `config.SourcesReady`
//!   is the pod-config merge's readiness; here the node's pods are listed from
//!   storage on every sync pass, so the kubelet passes "the first sync pass has
//!   completed".
//! - `createVolumeSpec` stays on [`crate::volumes::VolumeManager`], where it
//!   already ports the CSI-migration half (`IsMigratable` /
//!   `TranslateInTreeSpecToCSI`); the populator calls it with the PV it
//!   resolved, as `createVolumeSpec` does.
//! - [`PodStateProvider`] is implemented in `kubelet.rs`: the kubelet has no
//!   per-pod worker status for a pod that is simply running, so it derives the
//!   answer from `pod_states` and the pod object.

use crate::volume_manager::cache::{ActualStateOfWorld, DesiredStateOfWorld};
use crate::volume_plugins::plugin::OwnedSpec;
use crate::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
use crate::volume_plugins::util::{get_pod_volume_names, get_unique_pod_name};
use crate::volume_plugins::VolumePluginMgr;
use anyhow::{anyhow, bail, Result};
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::volume::{
    PersistentVolume, PersistentVolumeClaim, PersistentVolumeClaimPhase, PersistentVolumeMode,
    PVC_PROTECTION_FINALIZER,
};
use rusternetes_common::resources::{Pod, Volume};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{debug, error, info};

/// `PV annotation carrying a supplemental GID` (`util.VolumeGidAnnotationKey`,
/// `pkg/volume/util/util.go`).
const VOLUME_GID_ANNOTATION_KEY: &str = "pv.beta.kubernetes.io/gid";

/// Port of `PodStateProvider` (`desired_state_of_world_populator.go:77-80`):
/// can determine if a pod is going to be terminated.
pub trait PodStateProvider: Send + Sync {
    fn should_pod_containers_be_terminating(&self, uid: &str) -> bool;
    fn should_pod_runtime_be_removed(&self, uid: &str) -> bool;
}

/// Port of `PodManager` (`desired_state_of_world_populator.go:82-87`): the
/// subset of methods the populator needs to observe the kubelet's pods.
pub use crate::podcertificate::PodManager;

/// Port of `desiredStateOfWorldPopulator` (`:107-120`).
pub struct DesiredStateOfWorldPopulator {
    storage: Arc<StorageBackend>,
    loop_sleep_duration: Duration,
    pod_manager: Arc<dyn PodManager>,
    pod_state_provider: Arc<dyn PodStateProvider>,
    desired_state_of_world: Arc<DesiredStateOfWorld>,
    actual_state_of_world: Arc<ActualStateOfWorld>,
    /// Home of `createVolumeSpec`'s CSI-migration translation.
    spec_factory: Arc<crate::volumes::VolumeManager>,
    volume_plugin_mgr: Arc<VolumePluginMgr>,
    /// `processedPods`.
    pods: RwLock<HashMap<UniquePodName, bool>>,
    has_added_pods: AtomicBool,
}

impl DesiredStateOfWorldPopulator {
    /// Port of `NewDesiredStateOfWorldPopulator` (`:99-127`).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<StorageBackend>,
        loop_sleep_duration: Duration,
        pod_manager: Arc<dyn PodManager>,
        pod_state_provider: Arc<dyn PodStateProvider>,
        desired_state_of_world: Arc<DesiredStateOfWorld>,
        actual_state_of_world: Arc<ActualStateOfWorld>,
        spec_factory: Arc<crate::volumes::VolumeManager>,
        volume_plugin_mgr: Arc<VolumePluginMgr>,
    ) -> Self {
        Self {
            storage,
            loop_sleep_duration,
            pod_manager,
            pod_state_provider,
            desired_state_of_world,
            actual_state_of_world,
            spec_factory,
            volume_plugin_mgr,
            pods: RwLock::new(HashMap::new()),
            has_added_pods: AtomicBool::new(false),
        }
    }

    /// Port of `Run` (`:130-146`): loops until sources are ready, marks
    /// `hasAddedPods`, then loops forever.
    pub async fn run(&self, sources_ready: impl Fn() -> bool) {
        info!("Desired state populator starts to run");
        // `wait.PollUntilContextCancel(ctx, loopSleepDuration, false, ...)`:
        // the condition (`AllReady` sampled *before* the loop body) is checked
        // after each run, so the loop that finishes the initial populate began
        // after the sources were ready.
        loop {
            let done = sources_ready();
            self.populator_loop().await;
            if done {
                break;
            }
            tokio::time::sleep(self.loop_sleep_duration).await;
        }
        if !self.has_added_pods.swap(true, Ordering::SeqCst) {
            info!("Finished populating initial desired state of world");
        }
        loop {
            tokio::time::sleep(self.loop_sleep_duration).await;
            self.populator_loop().await;
        }
    }

    /// Port of `ReprocessPod` (`:148-151`): forces the pod to be processed
    /// again, so a pod update remounts volumes that depend on it (downward API).
    pub fn reprocess_pod(&self, pod_name: &UniquePodName) {
        self.mark_pod_processing_failed(pod_name);
    }

    /// Test seam for `dswp.hasAddedPods = ...` (`desired_state_of_world_populator_test.go:157`).
    #[doc(hidden)]
    pub fn set_has_added_pods(&self, value: bool) {
        self.has_added_pods.store(value, Ordering::SeqCst);
    }

    /// Port of `HasAddedPods` (`:153-157`).
    pub fn has_added_pods(&self) -> bool {
        self.has_added_pods.load(Ordering::SeqCst)
    }

    /// Port of `populatorLoop` (`:159-163`).
    pub async fn populator_loop(&self) {
        self.find_and_add_new_pods().await;
        self.find_and_remove_deleted_pods();
    }

    /// Port of `findAndAddNewPods` (`:165-182`): iterate through all pods and
    /// add to the desired state of the world those that are not there but
    /// should be.
    pub async fn find_and_add_new_pods(&self) {
        let has_added_pods = self.has_added_pods();
        for pod in self.pod_manager.get_pods() {
            // Keep consistency of adding pod during reconstruction
            if has_added_pods
                && self
                    .pod_state_provider
                    .should_pod_containers_be_terminating(&pod.metadata.uid)
            {
                // Do not (re)add volumes for pods that can't also be starting containers
                continue;
            }

            if !has_added_pods
                && self
                    .pod_state_provider
                    .should_pod_runtime_be_removed(&pod.metadata.uid)
            {
                // When kubelet restarts, we need to add pods to dsw if there is a possibility
                // that the container may still be running
                continue;
            }

            self.process_pod_volumes(&pod).await;
        }
    }

    /// Port of `findAndRemoveDeletedPods` (`:184-253`): iterate through all
    /// pods in the desired state of the world and remove those that no longer
    /// exist.
    pub fn find_and_remove_deleted_pods(&self) {
        let mut pods_from_cache: HashSet<UniquePodName> = HashSet::new();
        for volume_to_mount in self.desired_state_of_world.get_volumes_to_mount() {
            pods_from_cache.insert(UniquePodName(volume_to_mount.pod.metadata.uid.clone()));
            let pod = self
                .pod_manager
                .get_pod_by_uid(&volume_to_mount.pod.metadata.uid);
            let pod_exists = pod.is_some();
            if let Some(pod) = &pod {
                // check if the attachability has changed for this volume
                if volume_to_mount.plugin_is_attachable {
                    let spec = volume_to_mount.volume_spec.as_spec();
                    // `err == nil && attachableVolumePlugin == nil`: only this
                    // means the plugin is truly non-attachable. A spec no
                    // plugin matches is an error upstream, and falls through.
                    let plugin_mgr = &self.volume_plugin_mgr;
                    if plugin_mgr.find_plugin_by_spec(&spec).is_ok()
                        && plugin_mgr.find_attachable_plugin_by_spec(&spec).is_none()
                    {
                        // It is not possible right now for a CSI plugin to be both attachable and non-deviceMountable
                        // So the uniqueVolumeName should remain the same after the attachability change
                        self.desired_state_of_world
                            .mark_volume_attachability(&volume_to_mount.volume_name, false);
                        info!(
                            "Volume changes from attachable to non-attachable: {}",
                            volume_to_mount.volume_name
                        );
                        continue;
                    }
                }

                // Exclude known pods that we expect to be running
                if !self
                    .pod_state_provider
                    .should_pod_runtime_be_removed(&pod.metadata.uid)
                {
                    continue;
                }
            }

            // Once a pod has been deleted from kubelet pod manager, do not delete
            // it immediately from volume manager. Instead, check the kubelet
            // pod state provider to verify that all containers in the pod have
            // been terminated.
            if !self
                .pod_state_provider
                .should_pod_runtime_be_removed(&volume_to_mount.pod.metadata.uid)
            {
                debug!(
                    "Pod {}/{} still has one or more containers in the non-exited state and will not be removed from desired state",
                    volume_to_mount.pod.metadata.namespace.as_deref().unwrap_or(""),
                    volume_to_mount.pod.metadata.name
                );
                continue;
            }
            let volume_to_mount_spec_name = volume_to_mount.volume_spec.name().to_string();
            let removed = self
                .actual_state_of_world
                .pod_removed_from_volume(&volume_to_mount.pod_name, &volume_to_mount.volume_name);
            if removed && pod_exists {
                debug!(
                    "Actual state does not yet have volume mount information and pod still exists in pod manager, skip removing volume from desired state: pod {} volume {}",
                    volume_to_mount.pod.metadata.uid, volume_to_mount_spec_name
                );
                continue;
            }
            debug!(
                "Removing volume from desired state: pod {} volume {}",
                volume_to_mount.pod.metadata.uid, volume_to_mount_spec_name
            );
            self.desired_state_of_world
                .delete_pod_from_volume(&volume_to_mount.pod_name, &volume_to_mount.volume_name);
            self.delete_processed_pod(&volume_to_mount.pod_name);
        }

        // Cleanup orphaned entries from processedPods
        let orphaned_pods: Vec<UniquePodName> = self
            .pods
            .read()
            .expect("processed pods lock")
            .keys()
            .filter(|k| !pods_from_cache.contains(*k))
            .cloned()
            .collect();
        for orphaned_pod in orphaned_pods {
            let uid = orphaned_pod.0.as_str();
            let pod_exists = self.pod_manager.get_pod_by_uid(uid).is_some();
            if !pod_exists && self.pod_state_provider.should_pod_runtime_be_removed(uid) {
                self.delete_processed_pod(&orphaned_pod);
            }
        }

        let pods_with_error = self.desired_state_of_world.get_pods_with_errors();
        for pod_name in pods_with_error {
            if self.pod_manager.get_pod_by_uid(&pod_name.0).is_none() {
                self.desired_state_of_world.pop_pod_errors(&pod_name);
            }
        }
    }

    /// Port of `processPodVolumes` (`:255-327`): processes the volumes in the
    /// given pod and adds them to the desired state of the world.
    async fn process_pod_volumes(&self, pod: &Pod) {
        let unique_pod_name = get_unique_pod_name(pod);
        if self.pod_previously_processed(&unique_pod_name) {
            return;
        }

        let mut all_volumes_added = true;
        let collect_selinux_options = feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod);
        let (mounts, devices, selinux_container_contexts) =
            get_pod_volume_names(pod, collect_selinux_options);
        let pod_arc = Arc::new(pod.clone());
        let pod_label = format!(
            "{}/{}",
            pod.metadata.namespace.as_deref().unwrap_or(""),
            pod.metadata.name
        );

        // Process volume spec for each volume defined in pod
        for pod_volume in pod.spec.iter().flat_map(|s| s.volumes.iter().flatten()) {
            if !mounts.contains(&pod_volume.name) && !devices.contains(&pod_volume.name) {
                // Volume is not used in the pod, ignore it.
                debug!(
                    "Skipping unused volume {} of pod {pod_label}",
                    pod_volume.name
                );
                continue;
            }

            let (pvc, volume_spec, volume_gid_value) = match self
                .create_volume_spec(pod_volume, pod, &mounts, &devices)
                .await
            {
                Ok(r) => r,
                Err(err) => {
                    error!(
                        "Error processing volume {} of pod {pod_label}: {err}",
                        pod_volume.name
                    );
                    self.desired_state_of_world
                        .add_error_to_pod(&unique_pod_name, &err.to_string());
                    all_volumes_added = false;
                    continue;
                }
            };
            let volume_spec = Arc::new(volume_spec);

            // Add volume to desired state of world
            let contexts = selinux_container_contexts
                .get(&pod_volume.name)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let unique_volume_name = match self.desired_state_of_world.add_pod_to_volume(
                &unique_pod_name,
                pod_arc.clone(),
                volume_spec.clone(),
                &pod_volume.name,
                &volume_gid_value,
                contexts,
            ) {
                Ok(n) => n,
                Err(err) => {
                    error!(
                        "Failed to add volume {} (spec {}) of pod {pod_label} to desiredStateOfWorld: {err}",
                        pod_volume.name,
                        volume_spec.name()
                    );
                    self.desired_state_of_world
                        .add_error_to_pod(&unique_pod_name, &err.to_string());
                    all_volumes_added = false;
                    continue;
                }
            };

            debug!(
                "Added volume {} (spec {}) of pod {pod_label} to desired state",
                pod_volume.name,
                volume_spec.name()
            );
            self.check_volume_fs_resize(
                &pod_label,
                pod_volume,
                pvc.as_ref(),
                &volume_spec,
                &unique_volume_name,
            );
        }

        // some of the volume additions may have failed, should not mark this pod as fully processed
        if all_volumes_added {
            self.mark_pod_processed(&unique_pod_name);
            // New pod has been synced. Re-mount all volumes that need it
            // (e.g. DownwardAPI)
            self.actual_state_of_world
                .mark_remount_required(&unique_pod_name);
            // Remove any stored errors for the pod, everything went well in this processPodVolumes
            self.desired_state_of_world.pop_pod_errors(&unique_pod_name);
        } else if self.pod_has_been_seen_once(&unique_pod_name) {
            // For the Pod which has been processed at least once, even though some volumes
            // may not have been reprocessed successfully this round, we still mark it as processed to avoid
            // processing it at a very high frequency. The pod will be reprocessed when volume manager calls
            // ReprocessPod() which is triggered by SyncPod.
            self.mark_pod_processed(&unique_pod_name);
        }
    }

    /// Port of `checkVolumeFSResize` (`:329-362`): records the desired PVC
    /// size for a volume mounted by the pod. It is compared with the actual
    /// size (from `pvc.Status.Capacity`) to call volume expansion on the node
    /// if needed.
    fn check_volume_fs_resize(
        &self,
        pod_label: &str,
        pod_volume: &Volume,
        pvc: Option<&PersistentVolumeClaim>,
        volume_spec: &OwnedSpec,
        unique_volume_name: &UniqueVolumeName,
    ) {
        // if a volumeSpec does not have PV or has InlineVolumeSpecForCSIMigration set or pvc is nil
        // we can't resize the volume and hence resizing should be skipped.
        let (Some(pv), Some(pvc)) = (volume_spec.persistent_volume.as_ref(), pvc) else {
            return;
        };
        if volume_spec.inline_volume_spec_for_csi_migration {
            // Only PVC supports resize operation.
            return;
        }

        // volumeSpec.ReadOnly is the value that determines if volume could be formatted when being mounted.
        // This is the same flag that determines filesystem resizing behaviour for offline resizing and hence
        // we should use it here. This value comes from Pod.spec.volumes.persistentVolumeClaim.readOnly.
        if volume_spec.read_only {
            // This volume is used as read only by this pod, we don't perform resize for read only volumes.
            debug!(
                "Skip file system resize check for the volume {}, as the volume is mounted as readonly (pod {pod_label})",
                pod_volume.name
            );
            return;
        }

        // `Capacity.Storage()` is the zero quantity when the key is absent.
        let storage_of = |capacity: Option<&HashMap<String, String>>| {
            capacity
                .and_then(|c| c.get("storage"))
                .and_then(|s| Quantity::parse(s).ok())
                .unwrap_or_else(|| Quantity::from_value(0, Format::BinarySI))
        };
        let pv_cap = storage_of(Some(&pv.spec.capacity));
        let pvc_status_cap = storage_of(pvc.status.as_ref().and_then(|s| s.capacity.as_ref()));
        self.desired_state_of_world
            .update_persistent_volume_size(unique_volume_name, pv_cap);
        debug!(
            "NodeExpandVolume updating size: actualSize {}, desiredSize {}, volumeName {unique_volume_name}",
            pvc_status_cap.canonical_string(),
            pv_cap.canonical_string()
        );
        // in case the actualStateOfWorld was rebuild after kubelet restart ensure that claimSize is set to accurate value
        self.actual_state_of_world
            .initialize_claim_size(unique_volume_name, pvc_status_cap);
    }

    /// Port of `podPreviouslyProcessed` (`:364-373`): true if the volumes for
    /// this pod have already been processed/reprocessed by the populator.
    pub fn pod_previously_processed(&self, pod_name: &UniquePodName) -> bool {
        self.pods
            .read()
            .expect("processed pods lock")
            .get(pod_name)
            .copied()
            .unwrap_or(false)
    }

    /// Port of `markPodProcessingFailed` (`:375-381`).
    fn mark_pod_processing_failed(&self, pod_name: &UniquePodName) {
        self.pods
            .write()
            .expect("processed pods lock")
            .insert(pod_name.clone(), false);
    }

    /// Port of `podHasBeenSeenOnce` (`:383-390`).
    pub fn pod_has_been_seen_once(&self, pod_name: &UniquePodName) -> bool {
        self.pods
            .read()
            .expect("processed pods lock")
            .contains_key(pod_name)
    }

    /// Port of `markPodProcessed` (`:392-399`).
    fn mark_pod_processed(&self, pod_name: &UniquePodName) {
        self.pods
            .write()
            .expect("processed pods lock")
            .insert(pod_name.clone(), true);
    }

    /// Port of `deleteProcessedPod` (`:401-407`).
    fn delete_processed_pod(&self, pod_name: &UniquePodName) {
        self.pods
            .write()
            .expect("processed pods lock")
            .remove(pod_name);
    }

    /// Port of `createVolumeSpec` (`:409-503`): the spec for a pod volume,
    /// dereferencing a PVC (or generic ephemeral volume) to its bound PV.
    /// Returns the PVC (when there is one), the spec and the PV's GID
    /// annotation value. Errors when the volume cannot be obtained at this
    /// time.
    async fn create_volume_spec(
        &self,
        pod_volume: &Volume,
        pod: &Pod,
        mounts: &HashSet<String>,
        devices: &HashSet<String>,
    ) -> Result<(Option<PersistentVolumeClaim>, OwnedSpec, String)> {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("");
        let mut pvc_source = pod_volume
            .persistent_volume_claim
            .as_ref()
            .map(|s| (s.claim_name.clone(), s.read_only.unwrap_or(false)));
        let is_ephemeral = pvc_source.is_none() && pod_volume.ephemeral.is_some();
        if is_ephemeral {
            // Generic ephemeral inline volumes are handled the
            // same way as a PVC reference. The only additional
            // constraint (checked below) is that the PVC must be
            // owned by the pod.
            pvc_source = Some((
                rusternetes_common::ephemeral::volume_claim_name(pod, pod_volume),
                false,
            ));
        }
        if let Some((claim_name, read_only)) = pvc_source {
            debug!("Found PVC {namespace}/{claim_name}");
            // If podVolume is a PVC, fetch the real PV behind the claim
            let pvc = self
                .get_pvc_extract_pv(namespace, &claim_name)
                .await
                .map_err(|e| anyhow!("error processing PVC {namespace}/{claim_name}: {e}"))?;
            if is_ephemeral {
                rusternetes_common::ephemeral::volume_is_for_pod(pod, &pvc)?;
            }
            let pv_name = pvc.spec.volume_name.clone().unwrap_or_default();
            let pvc_uid = pvc.metadata.uid.clone();
            debug!("Found bound PV {pv_name} for PVC {namespace}/{claim_name} (uid {pvc_uid})");
            // Fetch actual PV object
            let (pv, volume_gid_value) = self
                .get_pv_spec(&pv_name, &pvc_uid)
                .await
                .map_err(|e| anyhow!("error processing PVC {namespace}/{claim_name}: {e}"))?;
            let volume_spec = self.spec_factory.create_volume_spec(
                namespace,
                pod_volume,
                Some(&pv),
                read_only,
            )?;
            debug!(
                "Extracted volumeSpec from bound PV and PVC {namespace}/{claim_name}: PV {pv_name}, spec {}",
                volume_spec.name()
            );

            let volume_mode =
                crate::volume_plugins::iscsi::get_volume_mode(&volume_spec.as_spec())?;
            // Error if a container has volumeMounts but the volumeMode of PVC isn't Filesystem.
            if mounts.contains(&pod_volume.name) && volume_mode != PersistentVolumeMode::Filesystem
            {
                bail!(
                    "volume {} has volumeMode {}, but is specified in volumeMounts",
                    pod_volume.name,
                    volume_mode_name(&volume_mode)
                );
            }
            // Error if a container has volumeDevices but the volumeMode of PVC isn't Block
            if devices.contains(&pod_volume.name) && volume_mode != PersistentVolumeMode::Block {
                bail!(
                    "volume {} has volumeMode {}, but is specified in volumeDevices",
                    pod_volume.name,
                    volume_mode_name(&volume_mode)
                );
            }
            return Ok((Some(pvc), volume_spec, volume_gid_value));
        }

        // Do not return the original volume object, since the source could mutate it
        let spec = self
            .spec_factory
            .create_volume_spec(namespace, pod_volume, None, false)?;
        Ok((None, spec, String::new()))
    }

    /// Port of `getPVCExtractPV` (`:505-541`): fetches the PVC, checks whether
    /// it is being deleted, and returns it. An error is returned if the PVC's
    /// phase is not "Bound".
    async fn get_pvc_extract_pv(
        &self,
        namespace: &str,
        claim_name: &str,
    ) -> Result<PersistentVolumeClaim> {
        let key = build_key("persistentvolumeclaims", Some(namespace), claim_name);
        let pvc: PersistentVolumeClaim = self
            .storage
            .get(&key)
            .await
            .map_err(|e| anyhow!("failed to fetch PVC from API server: {e}"))?;

        // Pods that uses a PVC that is being deleted and not protected by
        // kubernetes.io/pvc-protection must not be started.
        //
        // 1) In case an old kubelet is running without this check, the worst
        // that can happen is that such pod is scheduled. This was the default
        // behavior in 1.8 and earlier and users should not be that surprised.
        // It should happen only in very rare case when scheduler schedules
        // a pod and user deletes a PVC that's used by it at the same time.
        //
        // 2) Adding a check for kubernetes.io/pvc-protection here to prevent
        // the existing running pods from being affected during the rebuild of
        // the desired state of the world cache when the kubelet is restarted.
        // It is safe for kubelet to add this check here because the PVC will
        // be stuck in Terminating state until the pod is deleted.
        if pvc.metadata.deletion_timestamp.is_some()
            && !pvc
                .metadata
                .finalizers
                .iter()
                .flatten()
                .any(|f| f == PVC_PROTECTION_FINALIZER)
        {
            bail!("PVC is being deleted");
        }

        if pvc.status.as_ref().map(|s| &s.phase) != Some(&PersistentVolumeClaimPhase::Bound) {
            bail!("PVC is not bound");
        }
        if pvc.spec.volume_name.as_deref().unwrap_or("").is_empty() {
            bail!("PVC has empty pvc.Spec.VolumeName");
        }

        Ok(pvc)
    }

    /// Port of `getPVSpec` (`:543-574`): fetches the PV with the given name
    /// and returns it with the value of its GID annotation. The caller wraps
    /// it in a spec (`NewSpecFromPersistentVolume`).
    async fn get_pv_spec(
        &self,
        name: &str,
        expected_claim_uid: &str,
    ) -> Result<(PersistentVolume, String)> {
        let key = build_key("persistentvolumes", None, name);
        let pv: PersistentVolume = self
            .storage
            .get(&key)
            .await
            .map_err(|e| anyhow!("failed to fetch PV {name} from API server: {e}"))?;

        let Some(claim_ref) = pv.spec.claim_ref.as_ref() else {
            bail!(
                "found PV object {name} but it has a nil pv.Spec.ClaimRef indicating it is not yet bound to the claim"
            );
        };

        let claim_uid = claim_ref.uid.as_deref().unwrap_or("");
        if claim_uid != expected_claim_uid {
            bail!(
                "found PV object {name} but its pv.Spec.ClaimRef.UID {claim_uid} does not point to claim.UID {expected_claim_uid}"
            );
        }

        let volume_gid_value = get_pv_volume_gid_annotation_value(&pv);
        Ok((pv, volume_gid_value))
    }
}

/// Port of `getPVVolumeGidAnnotationValue` (`:576-582`).
fn get_pv_volume_gid_annotation_value(pv: &PersistentVolume) -> String {
    pv.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(VOLUME_GID_ANNOTATION_KEY))
        .cloned()
        .unwrap_or_default()
}

/// The `%s` of a `v1.PersistentVolumeMode`.
fn volume_mode_name(mode: &PersistentVolumeMode) -> String {
    match mode {
        PersistentVolumeMode::Filesystem => "Filesystem".to_string(),
        PersistentVolumeMode::Block => "Block".to_string(),
        other => format!("{other:?}"),
    }
}
