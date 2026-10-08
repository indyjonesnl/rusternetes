use anyhow::Result;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::{
    get_default_class, NodeSelectorTerm, PersistentVolumeClaimPhase, PersistentVolumeMode,
    PersistentVolumePhase, PersistentVolumeReclaimPolicy, StorageClass, VolumeBindingMode,
    VolumeNodeAffinity,
};
use rusternetes_common::resources::{
    EventSource, EventType, Node, PersistentVolume, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PersistentVolumeSpec, PersistentVolumeStatus, Pod,
};
use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

pub const ANN_BOUND_BY_CONTROLLER: &str = "pv.kubernetes.io/bound-by-controller";
/// `storagehelpers.AnnBindCompleted` (component-helpers pv_helpers.go:39).
pub const ANN_BIND_COMPLETED: &str = "pv.kubernetes.io/bind-completed";
pub const ANN_DYNAMICALLY_PROVISIONED: &str = "pv.kubernetes.io/provisioned-by";
/// `storagehelpers.AnnMigratedTo` (component-helpers pv_helpers.go).
const ANN_MIGRATED_TO: &str = "pv.kubernetes.io/migrated-to";
/// `util.AnnPreResizeCapacity` (`pkg/volume/util/resize_util.go:52`).
const ANN_PRE_RESIZE_CAPACITY: &str = "volume.alpha.kubernetes.io/pre-resize-capacity";
/// `storagehelpers.AnnStorageProvisioner` (pv_helpers.go:75).
const ANN_STORAGE_PROVISIONER: &str = "volume.kubernetes.io/storage-provisioner";
/// `storagehelpers.AnnBetaStorageProvisioner` (pv_helpers.go:76).
const ANN_BETA_STORAGE_PROVISIONER: &str = "volume.beta.kubernetes.io/storage-provisioner";

/// A Kubernetes `resource.Quantity` as an exact integer count of nano-units,
/// or `None` when it does not parse (`resource.ParseQuantity`). Enough to
/// implement `Quantity.Cmp` equality, so `1Gi` equals `1024Mi`.
fn parse_quantity_nano(q: &str) -> Option<i128> {
    let q = q.trim();
    let (sign, q) = match q.strip_prefix('-') {
        Some(r) => (-1i128, r),
        None => (1, q.strip_prefix('+').unwrap_or(q)),
    };
    let split = q
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(q.len());
    let (num, suffix) = q.split_at(split);
    let (int_s, frac_s) = num.split_once('.').unwrap_or((num, ""));
    if int_s.is_empty() && frac_s.is_empty() {
        return None;
    }
    let digits: i128 = format!("{int_s}{frac_s}").parse().ok()?;
    let scale = 10i128.checked_pow(u32::try_from(frac_s.len()).ok()?)?;
    let nano: i128 = match suffix {
        "n" => 1,
        "u" => 1_000,
        "m" => 1_000_000,
        "" => 1_000_000_000,
        "k" => 10i128.pow(12),
        "M" => 10i128.pow(15),
        "G" => 10i128.pow(18),
        "T" => 10i128.pow(21),
        "P" => 10i128.pow(24),
        "E" => 10i128.pow(27),
        "Ki" => 1024 * 10i128.pow(9),
        "Mi" => 1024i128.pow(2) * 10i128.pow(9),
        "Gi" => 1024i128.pow(3) * 10i128.pow(9),
        "Ti" => 1024i128.pow(4) * 10i128.pow(9),
        "Pi" => 1024i128.pow(5) * 10i128.pow(9),
        "Ei" => 1024i128.pow(6) * 10i128.pow(9),
        exp => {
            let e: i32 = exp.strip_prefix(['e', 'E'])?.parse().ok()?;
            if !(-9..=18).contains(&e) {
                return None;
            }
            10i128.pow((9 + e) as u32)
        }
    };
    Some(sign * digits.checked_mul(nano)? / scale)
}

/// `volumeCap.Cmp(claimCap) == 0` (`pv_controller.go:835`); unparsable
/// quantities compare by their text.
fn quantity_eq(a: &str, b: &str) -> bool {
    match (parse_quantity_nano(a), parse_quantity_nano(b)) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

/// `AnnSelectedNode` (component-helpers `pv_helpers.go:51`).
const ANN_SELECTED_NODE: &str = "volume.kubernetes.io/selected-node";
/// `v1.BetaStorageClassAnnotation`.
const ANN_BETA_STORAGE_CLASS: &str = "volume.beta.kubernetes.io/storage-class";

/// `util.IsPodTerminated` (`pkg/volume/util/util.go:303-310`).
fn is_pod_terminated(pod: &Pod) -> bool {
    use rusternetes_common::resources::pod::{ContainerState, ContainerStatus};
    use rusternetes_common::types::Phase;
    let Some(status) = pod.status.as_ref() else {
        return false;
    };
    if matches!(status.phase, Some(Phase::Failed) | Some(Phase::Succeeded)) {
        return true;
    }
    // `notRunning` (util.go:312-319): every status is Terminated or Waiting.
    let not_running = |s: &Option<Vec<ContainerStatus>>| {
        s.iter().flatten().all(|c| {
            matches!(
                c.state,
                Some(ContainerState::Terminated { .. }) | Some(ContainerState::Waiting { .. })
            )
        })
    };
    pod.metadata.deletion_timestamp.is_some()
        && not_running(&status.init_container_statuses)
        && not_running(&status.container_statuses)
        && not_running(&status.ephemeral_container_statuses)
}

/// `FindRecyclablePluginBySpec` (`pkg/volume/plugins.go:751`): the in-tree
/// plugins kube-controller-manager registers a recycler for are hostPath and
/// NFS (`cmd/kube-controller-manager/app/plugins.go:67-120`). Their
/// recycler-pod execution is not ported, so those volumes are left Released;
/// every other source has no recycler.
fn has_recyclable_plugin(spec: &PersistentVolumeSpec) -> bool {
    spec.host_path.is_some() || spec.nfs.is_some()
}

pub struct PVBinderController<S: Storage> {
    storage: Arc<S>,
    recorder: EventRecorder<S>,
}

impl<S: Storage + 'static> PVBinderController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting PV/PVC Binder Controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("persistentvolumeclaims", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Upstream's volume informer drives syncVolume on every PV event.
            let pv_prefix = rusternetes_storage::build_prefix("persistentvolumes", None);
            let mut pv_watch = match self.storage.watch(&pv_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish PV watch: {}, retrying", e);
                    time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = pv_watch.next() => {
                        match event {
                            Some(Ok(_)) => {
                                if let Err(e) = self.sync_volumes(&queue).await {
                                    error!("PV availability pass failed: {}", e);
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("PV watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("PV watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("persistentvolumeclaims", Some(ns), name);
            match self
                .storage
                .get::<PersistentVolumeClaim>(&storage_key)
                .await
            {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.bind_pvc(&mut resource).await {
                        Ok(()) => queue.forget(&key).await,
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
                Err(_) => {
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        // Volume side of the sync (upstream syncVolume).
        if let Err(e) = self.sync_volumes(queue).await {
            error!("PV availability pass failed: {}", e);
        }
        match self
            .storage
            .list::<PersistentVolumeClaim>("/registry/persistentvolumeclaims/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("persistentvolumeclaims/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list persistentvolumeclaims for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        // Get all PVCs
        let pvcs: Vec<PersistentVolumeClaim> = self
            .storage
            .list("/registry/persistentvolumeclaims/")
            .await?;

        for mut pvc in pvcs {
            if let Err(e) = self.bind_pvc(&mut pvc).await {
                error!("Failed to bind PVC {}: {}", pvc.metadata.name, e);
            }
        }

        // Volume side of the sync (upstream syncVolume): release/reclaim,
        // Bound, unbind. Claims it enqueues go to a throwaway queue here.
        if let Err(e) = self.sync_volumes(&WorkQueue::new()).await {
            error!("PV sync pass failed: {}", e);
        }

        Ok(())
    }

    /// Volume side of the controller: run [`Self::sync_volume`] over every PV.
    /// Upstream's volume informer invokes `syncVolume` per PV event/resync
    /// (`pv_controller_base.go` `volumeWorker`); this controller has no
    /// per-key PV queue, so a PV event (or the resync) sweeps all PVs. A
    /// failure on one PV is logged and does not stop the others — the next
    /// event/resync retries it ("Nothing was saved; we will fall back into the
    /// same condition in the next call").
    ///
    /// `claim_queue` is upstream's `ctrl.claimQueue`: `syncVolume` enqueues a
    /// claim there to speed up its `syncClaim` (`pv_controller.go:701-706`).
    pub async fn sync_volumes(&self, claim_queue: &WorkQueue) -> Result<()> {
        let pvs: Vec<PersistentVolume> = self.storage.list("/registry/persistentvolumes/").await?;
        for pv in pvs {
            // Being deleted: nothing to advance.
            if pv.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let name = pv.metadata.name.clone();
            if let Err(e) = self.sync_volume(pv, claim_queue).await {
                error!("Failed to sync PV {}: {}", name, e);
            }
        }
        Ok(())
    }

    /// Port of upstream `PersistentVolumeController.syncVolume`
    /// (`pkg/controller/volume/persistentvolume/pv_controller.go:556-770`),
    /// the decision table of binder_test.go "4-1".."4-12" and
    /// "14-13".."14-14":
    ///
    /// * no `claimRef`, or `claimRef.uid` empty (pre-bound, not yet bound):
    ///   `Available` (`:576-596`).
    /// * claim not found, or found with a different UID (the old one was
    ///   deleted and recreated): `Released` unless already `Released`/`Failed`
    ///   ("do not overwrite previous Failed state", `:668-681`), then
    ///   `reclaimVolume` (`:682-690`).
    /// * claim found but `spec.volumeName` empty: on a volumeMode mismatch emit
    ///   `VolumeMismatch` on the PV and the claim and skip; otherwise enqueue
    ///   the claim so `syncClaim` finishes the bind (`:694-728`).
    /// * claim bound to this volume: `Bound` (`:729-736`).
    /// * claim bound elsewhere: a dynamically provisioned `Delete` volume is
    ///   released and reclaimed; otherwise `unbindVolume` (`:737-770`).
    ///
    async fn sync_volume(&self, pv: PersistentVolume, claim_queue: &WorkQueue) -> Result<()> {
        // "Set correct "migrated-to" annotations and modify finalizers on PV and
        // update in API server if necessary" (`pv_controller.go:565-573`).
        let pv = self
            .update_volume_migration_annotations_and_finalizers(pv)
            .await?;
        // `volume.Spec.ClaimRef == nil` and `claimRef.UID == ""`: unused, or
        // reserved for a claim that has not yet bound (`:576-596`).
        let claim_ref = match pv.spec.claim_ref.clone() {
            Some(cr) if !cr.uid.as_deref().unwrap_or("").is_empty() => cr,
            _ => {
                self.update_volume_phase(pv, PersistentVolumePhase::Available)
                    .await?;
                return Ok(());
            }
        };

        // Get the PVC by name; a claim whose UID differs from the claimRef's
        // is a recreation of the one the PV was bound to, i.e. "treat the
        // volume as bound to a missing claim" (`:633-657`). The storage read is
        // authoritative, so upstream's informer-cache-then-apiserver double
        // check collapses to this one read.
        let claim = match (claim_ref.namespace.as_deref(), claim_ref.name.as_deref()) {
            (Some(ns), Some(name)) => {
                let key = build_key("persistentvolumeclaims", Some(ns), name);
                match self.storage.get::<PersistentVolumeClaim>(&key).await {
                    Ok(c) => Some(c),
                    Err(rusternetes_common::Error::NotFound(_)) => None,
                    Err(e) => return Err(e.into()),
                }
            }
            _ => None,
        };
        let claim = claim.filter(|c| Some(c.metadata.uid.as_str()) == claim_ref.uid.as_deref());

        let Some(claim) = claim else {
            // The claim must have been deleted; reclaimVolume may release the
            // PV back into the pool, recycle it or do nothing (retain).
            let pv = self.release_if_needed(pv).await?;
            return self.reclaim_volume(&pv).await;
        };

        let claim_volume_name = claim.spec.volume_name.as_deref().unwrap_or("");
        if claim_volume_name.is_empty() {
            if volume_mode_mismatches(&claim.spec, &pv.spec) {
                // Binding won't be called in syncUnboundClaim, because
                // findBestMatchForClaim won't return the volume due to
                // volumeMode mismatch (`:684-692`).
                let volume_msg = format!(
                    "Cannot bind PersistentVolume to requested PersistentVolumeClaim \"{}\" due to incompatible volumeMode.",
                    claim.metadata.name
                );
                self.warn(object_ref_for_pv(&pv), "VolumeMismatch", &volume_msg)
                    .await;
                let claim_msg = format!(
                    "Cannot bind PersistentVolume \"{}\" to requested PersistentVolumeClaim due to incompatible volumeMode.",
                    pv.metadata.name
                );
                self.warn(object_ref_for_pvc(&claim), "VolumeMismatch", &claim_msg)
                    .await;
                // Skipping syncClaim.
                return Ok(());
            }
            // The volume is Bound and the claim is Pending (either the binding
            // is not completed, or it was unbound by the user). Enqueue the
            // claim so syncClaim fixes it shortly (`:694-706`).
            claim_queue
                .add(format!(
                    "persistentvolumeclaims/{}/{}",
                    claim.metadata.namespace.as_deref().unwrap_or(""),
                    claim.metadata.name
                ))
                .await;
            return Ok(());
        }

        if claim_volume_name == pv.metadata.name {
            // Volume is bound to a claim properly, update status if necessary.
            self.update_volume_phase(pv, PersistentVolumePhase::Bound)
                .await?;
            return Ok(());
        }

        // Volume is bound to a claim, but the claim is bound elsewhere.
        let dynamically_provisioned = pv
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_DYNAMICALLY_PROVISIONED));
        if dynamically_provisioned
            && pv.spec.persistent_volume_reclaim_policy
                == Some(PersistentVolumeReclaimPolicy::Delete)
        {
            // Dynamically provisioned for this claim, which got bound
            // elsewhere: this volume is not needed. Mark it Released for
            // external deleters (don't overwrite Failed) and delete it.
            let pv = self.release_if_needed(pv).await?;
            return self.reclaim_volume(&pv).await;
        }
        // Bound by the controller: it tried to use this volume for a claim
        // that was fulfilled by another volume, so roll back. Bound by the
        // user: only clear the binding UID and leave the pre-bind.
        self.unbind_volume(pv).await
    }

    /// `updateVolumePhase` (`pv_controller.go:912-937`): a no-op when the phase
    /// is already set, otherwise `UpdateStatus` with the phase set and
    /// `status.message` cleared. Plain `updateVolumePhase` records no event.
    async fn update_volume_phase(
        &self,
        pv: PersistentVolume,
        phase: PersistentVolumePhase,
    ) -> Result<PersistentVolume> {
        self.update_volume_phase_with_message(pv, phase, None).await
    }

    /// `updateVolumePhaseWithEvent` (`pv_controller.go:942-962`): set the
    /// phase and message, and emit a Warning event only when the phase
    /// actually changes ("not every time syncClaim is called").
    async fn update_volume_phase_with_event(
        &self,
        pv: PersistentVolume,
        phase: PersistentVolumePhase,
        reason: &str,
        message: &str,
    ) -> Result<PersistentVolume> {
        if pv.status.as_ref().map(|s| &s.phase) == Some(&phase) {
            return Ok(pv);
        }
        let new_pv = self
            .update_volume_phase_with_message(pv, phase, Some(message.to_string()))
            .await?;
        self.warn(object_ref_for_pv(&new_pv), reason, message).await;
        Ok(new_pv)
    }

    /// `updateVolumePhase` with an explicit `message` (`:912-937`).
    async fn update_volume_phase_with_message(
        &self,
        mut pv: PersistentVolume,
        phase: PersistentVolumePhase,
        message: Option<String>,
    ) -> Result<PersistentVolume> {
        if pv.status.as_ref().map(|s| &s.phase) == Some(&phase) {
            return Ok(pv);
        }
        let mut status = pv.status.take().unwrap_or_default();
        status.phase = phase.clone();
        status.message = message;
        pv.status = Some(status);
        let pv_key = build_key("persistentvolumes", None, &pv.metadata.name);
        // Phase-only write -> status subresource (pv_controller.go:925).
        let new_pv = self.storage.update_status(&pv_key, &pv).await?;
        info!("Volume {} entered phase {:?}", new_pv.metadata.name, phase);
        Ok(new_pv)
    }

    /// `Released` unless already `Released` or `Failed`: "Do not overwrite
    /// previous Failed state - let the user see that something went wrong,
    /// while we still re-try to reclaim the volume" (`pv_controller.go:672-681`
    /// and the same guard at `:741-749`).
    async fn release_if_needed(&self, pv: PersistentVolume) -> Result<PersistentVolume> {
        match pv.status.as_ref().map(|s| &s.phase) {
            Some(PersistentVolumePhase::Released) | Some(PersistentVolumePhase::Failed) => Ok(pv),
            _ => {
                info!(
                    "Volume {} is released and reclaim policy {:?} will be executed",
                    pv.metadata.name, pv.spec.persistent_volume_reclaim_policy
                );
                self.update_volume_phase(pv, PersistentVolumePhase::Released)
                    .await
            }
        }
    }

    /// `updateVolumeMigrationAnnotationsAndFinalizers`
    /// (`pv_controller_base.go:361-381`): one `Update` when either the
    /// `migrated-to` annotation or the deletion finalizers need fixing.
    async fn update_volume_migration_annotations_and_finalizers(
        &self,
        mut pv: PersistentVolume,
    ) -> Result<PersistentVolume> {
        let ann_modified = update_migration_annotations(pv.metadata.annotations.as_mut(), false);
        let (finalizers, finalizers_modified) = modify_deletion_finalizers(&pv);
        if !ann_modified && !finalizers_modified {
            return Ok(pv);
        }
        if finalizers_modified {
            pv.metadata.finalizers = finalizers;
        }
        let pv_key = build_key("persistentvolumes", None, &pv.metadata.name);
        Ok(self.storage.update(&pv_key, &pv).await?)
    }

    /// `reclaimVolume` (`pv_controller.go:1180-1230`). `Retain` does nothing;
    /// `Delete` removes the PV. `Recycle` on a volume with no recycler
    /// plugin fails it (`recycleVolumeOperation`, `:1279-1286`); the hostPath/NFS
    /// recycler pods are not ported, so those stay `Released` like `Retain`. A PV carrying the
    /// `pv.kubernetes.io/migrated-to` annotation is left to the external
    /// provisioner (`:1183-1187`).
    async fn reclaim_volume(&self, pv: &PersistentVolume) -> Result<()> {
        if pv
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(ANN_MIGRATED_TO))
            .is_some_and(|v| !v.is_empty())
        {
            return Ok(());
        }
        if pv.spec.persistent_volume_reclaim_policy == Some(PersistentVolumeReclaimPolicy::Recycle)
            && !has_recyclable_plugin(&pv.spec)
        {
            // recycleVolumeOperation, "No recycler found" branch
            // (`pv_controller.go:1279-1286`): Failed phase + Warning event.
            // Upstream: "the controller will retry
            // recycling the volume in every syncVolume() call".
            self.update_volume_phase_with_event(
                pv.clone(),
                PersistentVolumePhase::Failed,
                "VolumeFailedRecycle",
                "No recycler plugin found for the volume!",
            )
            .await?;
            return Ok(());
        }
        if matches!(
            pv.spec.persistent_volume_reclaim_policy,
            Some(PersistentVolumeReclaimPolicy::Unknown(_))
        ) {
            // `default:` branch of reclaimVolume (`pv_controller.go:1217-1222`).
            self.update_volume_phase_with_event(
                pv.clone(),
                PersistentVolumePhase::Failed,
                "VolumeUnknownReclaimPolicy",
                "Volume has unrecognized PersistentVolumeReclaimPolicy",
            )
            .await?;
            return Ok(());
        }
        if pv.spec.persistent_volume_reclaim_policy == Some(PersistentVolumeReclaimPolicy::Delete) {
            let pv_key = build_key("persistentvolumes", None, &pv.metadata.name);
            match self.storage.delete(&pv_key).await {
                Ok(()) | Err(rusternetes_common::Error::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
            info!("Reclaim(Delete): deleted released PV {}", pv.metadata.name);
        }
        Ok(())
    }

    /// `unbindVolume` (`pv_controller.go:1140-1178`): roll back a binding. A
    /// volume bound by the controller loses its `claimRef` and the
    /// bound-by-controller annotation; one pre-bound by the user only loses the
    /// claimRef UID. The spec goes through `Update`, then the phase returns to
    /// `Available` via `updateVolumePhase`.
    async fn unbind_volume(&self, mut pv: PersistentVolume) -> Result<()> {
        let bound_by_controller = pv
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_BOUND_BY_CONTROLLER));
        if bound_by_controller {
            pv.spec.claim_ref = None;
            if let Some(a) = pv.metadata.annotations.as_mut() {
                a.remove(ANN_BOUND_BY_CONTROLLER);
                if a.is_empty() {
                    // "No annotations look better than empty annotation map".
                    pv.metadata.annotations = None;
                }
            }
        } else if let Some(cr) = pv.spec.claim_ref.as_mut() {
            cr.uid = None;
        }
        let pv_key = build_key("persistentvolumes", None, &pv.metadata.name);
        let new_pv = self.storage.update(&pv_key, &pv).await?;
        self.update_volume_phase(new_pv, PersistentVolumePhase::Available)
            .await?;
        Ok(())
    }

    /// `ctrl.eventRecorder.Event(obj, v1.EventTypeWarning, reason, msg)`.
    /// Recording is fire-and-forget upstream; a failure is only logged.
    async fn warn(&self, involved: ObjectReference, reason: &str, message: &str) {
        let source = EventSource {
            component: "persistentvolume-controller".to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(&involved, &source, EventType::Warning, reason, message)
            .await
        {
            tracing::warn!("Failed to record {} event: {}", reason, e);
        }
    }

    /// `eventRecorder.Event(obj, v1.EventTypeNormal, reason, message)`.
    async fn event(&self, involved: ObjectReference, reason: &str, message: &str) {
        let source = EventSource {
            component: "persistentvolume-controller".to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(&involved, &source, EventType::Normal, reason, message)
            .await
        {
            tracing::warn!("Failed to record {} event: {}", reason, e);
        }
    }

    /// Port of upstream `syncClaim` dispatch (`pv_controller.go:251-255`): a
    /// claim without `pv.kubernetes.io/bind-completed` goes to
    /// `syncUnboundClaim`, one with it to `syncBoundClaim`.
    ///
    /// Not ported here (tracked separately): the `provisionClaim` call in the
    /// `volumeName == ""` branch (done by the dynamic-provisioner controller)
    /// and `unbindClaim`.
    async fn bind_pvc(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        // "Set correct "migrated-to" annotations on PVC and update in API
        // server if necessary" (`syncClaim`, pv_controller.go:240-249).
        self.update_claim_migration_annotations(pvc).await?;
        let bind_completed = pvc
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_BIND_COMPLETED));
        if bind_completed {
            return self.sync_bound_claim(pvc).await;
        }
        // `claim.Spec.VolumeName != ""` (`:411`): the user asked for a
        // specific PV.
        if pvc
            .spec
            .volume_name
            .as_deref()
            .is_some_and(|v| !v.is_empty())
        {
            return self.sync_prebound_claim(pvc).await;
        }
        self.sync_unbound_claim(pvc).await
    }

    /// `updateClaimMigrationAnnotations` (`pv_controller_base.go:336-359`):
    /// when the claim's provisioner annotation says it is (no longer)
    /// CSI-migrated, anneal `pv.kubernetes.io/migrated-to` and `Update` the
    /// claim.
    async fn update_claim_migration_annotations(
        &self,
        pvc: &mut PersistentVolumeClaim,
    ) -> Result<()> {
        let mut annotated = pvc.clone();
        if !update_migration_annotations(annotated.metadata.annotations.as_mut(), true) {
            return Ok(());
        }
        let key = build_key(
            "persistentvolumeclaims",
            pvc.metadata.namespace.as_deref(),
            &pvc.metadata.name,
        );
        let updated = self.storage.update(&key, &annotated).await.map_err(|e| {
            anyhow::anyhow!("persistent Volume Controller can't anneal migration annotations: {e}")
        })?;
        *pvc = updated;
        Ok(())
    }

    /// `PersistentVolumeController.syncBoundClaim`
    /// (`pv_controller.go:492-560`; binder_test.go "3-1".."3-7").
    async fn sync_bound_claim(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        let volume_name = pvc.spec.volume_name.clone().unwrap_or_default();
        if volume_name.is_empty() {
            // Claim was bound before but not any more.
            return self
                .update_claim_status_with_event(
                    pvc,
                    PersistentVolumeClaimPhase::Lost,
                    "ClaimLost",
                    "Bound claim has lost reference to PersistentVolume. Data on the volume is lost!",
                )
                .await;
        }
        let Some(pv) = self.get_volume(&volume_name).await? else {
            // Claim is bound to a non-existing volume.
            return self
                .update_claim_status_with_event(
                    pvc,
                    PersistentVolumeClaimPhase::Lost,
                    "ClaimLost",
                    "Bound claim has lost its PersistentVolume. Data on the volume is lost!",
                )
                .await;
        };
        match pv.spec.claim_ref.as_ref() {
            // Claim is bound but volume has come unbound (or the controller
            // has not seen the updated volume yet; the two cannot be told
            // apart): bind the volume again and set all states to Bound.
            None => self.bind(pv, pvc).await,
            // All is well; `bind` does nothing when everything is set.
            Some(cr) if cr.uid.as_deref() == Some(pvc.metadata.uid.as_str()) => {
                self.bind(pv, pvc).await
            }
            // Claim is bound but volume has a different claimant: `Lost` is a
            // terminal phase.
            Some(_) => {
                self.update_claim_status_with_event(
                    pvc,
                    PersistentVolumeClaimPhase::Lost,
                    "ClaimMisbound",
                    "Two claims are bound to the same volume, this one is bound incorrectly",
                )
                .await
            }
        }
    }

    /// The `claim.Spec.VolumeName != ""` branch of `syncUnboundClaim`
    /// (`pv_controller.go:411-490`; binder_test.go "2-1".."2-10"): the user
    /// asked for a specific PV.
    async fn sync_prebound_claim(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        let volume_name = pvc.spec.volume_name.clone().unwrap_or_default();
        let Some(pv) = self.get_volume(&volume_name).await? else {
            // User asked for a PV that does not exist; retry later.
            return self
                .update_claim_status(pvc, PersistentVolumeClaimPhase::Pending, None)
                .await;
        };
        if pv.spec.claim_ref.is_none() {
            // User asked for a PV that is not claimed.
            if let Err(e) = self.check_volume_satisfy_claim(&pv, pvc) {
                let msg = format!(
                    "Cannot bind to requested volume {:?}: {}",
                    pv.metadata.name, e
                );
                self.warn(object_ref_for_pvc(pvc), "VolumeMismatch", &msg)
                    .await;
                return self
                    .update_claim_status(pvc, PersistentVolumeClaimPhase::Pending, None)
                    .await;
            }
            return self.bind(pv, pvc).await;
        }
        if is_volume_bound_to_claim(&pv, pvc) {
            // User asked for a PV that is claimed by this PVC: finish the
            // binding by adding the claim UID.
            return self.bind(pv, pvc).await;
        }
        // User asked for a PV that is claimed by someone else.
        let claim_msg = format!(
            "volume {:?} already bound to a different claim.",
            pv.metadata.name
        );
        self.warn(object_ref_for_pvc(pvc), "FailedBinding", &claim_msg)
            .await;
        let bound_by_controller = pvc
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_BOUND_BY_CONTROLLER));
        if !bound_by_controller {
            // User asked for a specific PV, retry later.
            return self
                .update_claim_status(pvc, PersistentVolumeClaimPhase::Pending, None)
                .await;
        }
        // "This should never happen because someone had to remove
        // AnnBindCompleted annotation on the claim."
        let other = pv.spec.claim_ref.as_ref();
        anyhow::bail!(
            "invalid binding of claim \"{}/{}\" to volume \"{}\": volume already claimed by \"{}/{}\"",
            pvc.metadata.namespace.as_deref().unwrap_or(""),
            pvc.metadata.name,
            volume_name,
            other.and_then(|c| c.namespace.as_deref()).unwrap_or(""),
            other.and_then(|c| c.name.as_deref()).unwrap_or(""),
        )
    }

    /// `PersistentVolumeController.bind` (`pv_controller.go:1000-1035`) as far
    /// as this controller models it: a no-op when volume and claim already
    /// agree (upstream's `bindVolumeToClaim`/`bindClaimToVolume` each write
    /// only when dirty), else [`Self::complete_binding`].
    async fn bind(&self, pv: PersistentVolume, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        let bound = pv.status.as_ref().map(|s| &s.phase) == Some(&PersistentVolumePhase::Bound)
            && pv
                .spec
                .claim_ref
                .as_ref()
                .is_some_and(|cr| cr.uid.as_deref() == Some(pvc.metadata.uid.as_str()))
            && is_volume_bound_to_claim(&pv, pvc)
            && pvc.spec.volume_name.as_deref() == Some(pv.metadata.name.as_str())
            && pvc
                .metadata
                .annotations
                .as_ref()
                .is_some_and(|a| a.contains_key(ANN_BIND_COMPLETED))
            && pvc.status.as_ref().is_some_and(|s| {
                s.phase == PersistentVolumeClaimPhase::Bound && s.capacity.is_some()
            });
        if bound {
            return Ok(());
        }
        self.complete_binding(pv, pvc).await
    }

    async fn get_volume(&self, name: &str) -> Result<Option<PersistentVolume>> {
        match self
            .storage
            .get::<PersistentVolume>(&build_key("persistentvolumes", None, name))
            .await
        {
            Ok(pv) => Ok(Some(pv)),
            Err(rusternetes_common::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// `checkVolumeSatisfyClaim` (`pv_controller.go:262-301`) for a volume the
    /// user named explicitly. Error strings are upstream's verbatim.
    fn check_volume_satisfy_claim(
        &self,
        pv: &PersistentVolume,
        pvc: &PersistentVolumeClaim,
    ) -> std::result::Result<(), String> {
        if pv.metadata.deletion_timestamp.is_some() {
            return Err(format!(
                "the volume is marked for deletion {:?}",
                pv.metadata.name
            ));
        }
        if let Some(requested) = pvc
            .spec
            .resources
            .requests
            .as_ref()
            .and_then(|r| r.get("storage"))
        {
            let sufficient = pv
                .spec
                .capacity
                .get("storage")
                .is_some_and(|have| self.storage_sufficient(have, requested));
            if !sufficient {
                return Err("requested PV is too small".to_string());
            }
        }
        if class_of(
            pvc.spec.storage_class_name.as_deref(),
            pvc.metadata.annotations.as_ref(),
        ) != class_of(
            pv.spec.storage_class_name.as_deref(),
            pv.metadata.annotations.as_ref(),
        ) {
            return Err("storageClassName does not match".to_string());
        }
        if pvc
            .spec
            .volume_attributes_class_name
            .as_deref()
            .unwrap_or("")
            != pv
                .spec
                .volume_attributes_class_name
                .as_deref()
                .unwrap_or("")
        {
            return Err("volumeAttributesClassName does not match".to_string());
        }
        if volume_mode_mismatches(&pvc.spec, &pv.spec) {
            return Err("incompatible volumeMode".to_string());
        }
        if !pvc
            .spec
            .access_modes
            .iter()
            .all(|m| pv.spec.access_modes.contains(m))
        {
            return Err("incompatible accessMode".to_string());
        }
        Ok(())
    }

    /// `updateClaimStatus` (`pv_controller.go:784-858`). With `volume ==
    /// None` (`:796-806`) the phase is set and accessModes / capacity /
    /// currentVolumeAttributesClassName are reset; with a volume (`:823-858`)
    /// accessModes follow the volume, capacity is copied only on a phase
    /// transition (honouring the pre-resize-capacity annotation) and
    /// currentVolumeAttributesClassName only on Pending -> Bound. A no-op when
    /// nothing changes; otherwise written through the status subresource.
    async fn update_claim_status(
        &self,
        pvc: &mut PersistentVolumeClaim,
        phase: PersistentVolumeClaimPhase,
        volume: Option<&PersistentVolume>,
    ) -> Result<()> {
        let old_phase = pvc.status.as_ref().map(|s| s.phase.clone());
        let mut status = pvc.status.clone().unwrap_or_default();
        let mut dirty = pvc.status.is_none() || status.phase != phase;
        status.phase = phase.clone();
        match volume {
            None => {
                dirty |= status.access_modes.take().is_some();
                dirty |= status.capacity.take().is_some();
                dirty |= status.current_volume_attributes_class_name.take().is_some();
            }
            Some(volume) => {
                if status.access_modes.as_ref() != Some(&volume.spec.access_modes) {
                    status.access_modes = Some(volume.spec.access_modes.clone());
                    dirty = true;
                }
                // "Update Capacity if the claim is becoming Bound, not if it
                // was already. A discrepancy can be intentional to mean that
                // the PVC filesystem size doesn't match the PV block device
                // size, so don't clobber it" (`:817-819`).
                if old_phase.as_ref() != Some(&phase) {
                    let Some(volume_cap) = volume.spec.capacity.get("storage") else {
                        anyhow::bail!(
                            "PersistentVolume {:?} is without a storage capacity",
                            volume.metadata.name
                        );
                    };
                    let pre_resize = volume
                        .metadata
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get(ANN_PRE_RESIZE_CAPACITY));
                    if let Some(pre) = pre_resize {
                        // `resource.ParseQuantity` failing falls back to the
                        // volume's capacity (`:830-833`).
                        let qty = if parse_quantity_nano(pre).is_some() {
                            pre.clone()
                        } else {
                            volume_cap.clone()
                        };
                        status
                            .capacity
                            .get_or_insert_with(Default::default)
                            .insert("storage".to_string(), qty);
                        dirty = true;
                    } else if status
                        .capacity
                        .as_ref()
                        .and_then(|c| c.get("storage"))
                        .is_none_or(|c| !quantity_eq(volume_cap, c))
                    {
                        status.capacity = Some(volume.spec.capacity.clone());
                        dirty = true;
                    }
                }
                // `:841-857`: only during binding, never afterwards.
                if old_phase == Some(PersistentVolumeClaimPhase::Pending)
                    && phase == PersistentVolumeClaimPhase::Bound
                    && status.current_volume_attributes_class_name
                        != volume.spec.volume_attributes_class_name
                {
                    status.current_volume_attributes_class_name =
                        volume.spec.volume_attributes_class_name.clone();
                    dirty = true;
                }
            }
        }
        if !dirty {
            return Ok(());
        }
        pvc.status = Some(status);
        let key = build_key(
            "persistentvolumeclaims",
            pvc.metadata.namespace.as_deref(),
            &pvc.metadata.name,
        );
        self.storage.update_status(&key, &*pvc).await?;
        Ok(())
    }

    /// `updateClaimStatusWithEvent` (`pv_controller.go:889-909`): nothing when
    /// the phase is already set; otherwise the status write, then the event, so
    /// it is emitted on a change and not on every sync.
    async fn update_claim_status_with_event(
        &self,
        pvc: &mut PersistentVolumeClaim,
        phase: PersistentVolumeClaimPhase,
        reason: &str,
        message: &str,
    ) -> Result<()> {
        if pvc.status.as_ref().map(|s| &s.phase) == Some(&phase) {
            return Ok(());
        }
        self.update_claim_status(pvc, phase, None).await?;
        self.warn(object_ref_for_pvc(pvc), reason, message).await;
        Ok(())
    }

    /// The `claim.Spec.VolumeName == ""` branch of `syncUnboundClaim`
    /// (`pv_controller.go:331-409`), first-match binding only.
    async fn sync_unbound_claim(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
        let pvc_name = &pvc.metadata.name;
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("default");

        let pvc_spec = &pvc.spec;

        debug!("Looking for PV to bind to PVC {}/{}", namespace, pvc_name);
        debug!(
            "PVC requirements: storage_class={:?}, capacity={:?}, access_modes={:?}",
            pvc_spec.storage_class_name,
            pvc_spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage")),
            pvc_spec.access_modes
        );

        // Get all available PVs
        let pvs: Vec<PersistentVolume> = self.storage.list("/registry/persistentvolumes/").await?;

        debug!("Found {} PVs to check for binding", pvs.len());

        // Lazily-loaded node inventory, only fetched when a candidate PV
        // actually declares node affinity (upstream `volume_scheduling`).
        let mut nodes_cache: Option<Vec<Node>> = None;

        // Phase 1 (#1095): honor a pre-bound PV. A dynamically provisioned PV
        // carries a `claimRef` naming the exact PVC it was provisioned for, so
        // it must bind to THAT PVC and no other — otherwise two PVCs provisioned
        // at the same time cross-bind to each other's PV. A PV whose claimRef
        // points to this PVC was provisioned for us; one pointing elsewhere is
        // unavailable. The pre-bound PV already satisfies the request (it was
        // sized for it), so it wins over any unclaimed first-match candidate.
        if let Some(pv) = pvs.iter().find(|pv| {
            pv.spec
                .claim_ref
                .as_ref()
                .is_some_and(|cr| claim_ref_points_to(cr, namespace, pvc_name, &pvc.metadata.uid))
        }) {
            info!(
                "Completing pre-bound PVC {}/{} ↔ PV {} (claimRef)",
                namespace, pvc_name, pv.metadata.name
            );
            return self.complete_binding(pv.clone(), pvc).await;
        }

        // Phase 2: first-match among genuinely unclaimed PVs (static volumes /
        // legacy PVs with no claimRef).
        for pv in pvs {
            debug!("Checking PV {} (storage_class={:?}, capacity={:?}, access_modes={:?}, claim_ref={:?})",
                pv.metadata.name,
                pv.spec.storage_class_name,
                pv.spec.capacity,
                pv.spec.access_modes,
                pv.spec.claim_ref.is_some());

            // Skip if PV is already bound/pre-bound (a pre-bind to this PVC was
            // handled in phase 1; anything else belongs to another claim).
            if pv.spec.claim_ref.is_some() {
                continue;
            }

            // Check if PV matches PVC requirements
            let matches = self.pv_matches_pvc(&pv.spec, pvc_spec);
            debug!(
                "PV {} matches PVC requirements: {}",
                pv.metadata.name, matches
            );
            if !matches {
                continue;
            }

            // Honor the PV's required node affinity (upstream parity with
            // `pkg/controller/volume/scheduling`): a PV constrained to a node
            // topology must not bind unless at least one node in the cluster
            // satisfies that constraint. Otherwise the bound volume could never
            // be mounted by any pod.
            if let Some(node_affinity) = pv.spec.node_affinity.as_ref() {
                if node_affinity.required.is_some() {
                    let nodes = match nodes_cache.as_ref() {
                        Some(n) => n,
                        None => {
                            let listed: Vec<Node> = self.storage.list("/registry/nodes/").await?;
                            nodes_cache = Some(listed);
                            nodes_cache.as_ref().unwrap()
                        }
                    };
                    if !node_affinity_satisfied(node_affinity, nodes) {
                        debug!(
                            "Skipping PV {}: required nodeAffinity matches no node",
                            pv.metadata.name
                        );
                        continue;
                    }
                }
            }

            info!(
                "Binding PVC {}/{} to PV {}",
                namespace, pvc_name, pv.metadata.name
            );
            return self.complete_binding(pv, pvc).await;
        }

        debug!("No matching PV found for PVC {}/{}", namespace, pvc_name);
        self.sync_unbound_claim_without_volume(pvc).await
    }

    /// The `volume == nil` arm of `syncUnboundClaim`
    /// (`pv_controller.go:345-390`): try `assignDefaultStorageClass`, then
    /// either explain why a delay-binding claim waits, hand a classed claim to
    /// provisioning, or report that nothing can serve a classless claim.
    ///
    /// Deviation: `provisionClaim` (`:372`) is not called here; provisioning is
    /// done by the separate dynamic-provisioner controller, so a claim with a
    /// class simply returns (as upstream does after `provisionClaim`).
    async fn sync_unbound_claim_without_volume(
        &self,
        pvc: &mut PersistentVolumeClaim,
    ) -> Result<()> {
        if self.assign_default_storage_class(pvc).await? {
            // "PersistentVolumeClaim update successful, restarting claim sync"
            return Ok(());
        }
        let class = class_of(
            pvc.spec.storage_class_name.as_deref(),
            pvc.metadata.annotations.as_ref(),
        )
        .to_string();
        let selected_node = pvc
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_SELECTED_NODE));
        if self.is_delay_binding_mode(&class).await? && !selected_node {
            // Scheduler does not observe any pod using this claim.
            self.emit_event_for_unbound_delay_binding_claim(pvc).await?;
        } else if !class.is_empty() {
            return Ok(());
        } else {
            self.event(
                object_ref_for_pvc(pvc),
                "FailedBinding",
                "no persistent volumes available for this claim and no storage class is set",
            )
            .await;
        }
        // Mark the claim as Pending and try to find a match in the next
        // periodic syncClaim.
        self.update_claim_status(pvc, PersistentVolumeClaimPhase::Pending, None)
            .await
    }

    /// `storagehelpers.IsDelayBindingMode` (component-helpers
    /// `pv_helpers.go:96-115`): no class or a missing class is not delay
    /// binding; a class without `volumeBindingMode` is an error.
    async fn is_delay_binding_mode(&self, class_name: &str) -> Result<bool> {
        if class_name.is_empty() {
            return Ok(false);
        }
        let class: StorageClass = match self
            .storage
            .get(&build_key("storageclasses", None, class_name))
            .await
        {
            Ok(c) => c,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        match class.volume_binding_mode {
            None => anyhow::bail!(
                "VolumeBindingMode not set for StorageClass {:?}",
                class_name
            ),
            Some(m) => Ok(m == VolumeBindingMode::WaitForFirstConsumer),
        }
    }

    /// `assignDefaultStorageClass` (`pv_controller.go:967-994`): a claim that
    /// asks for no class gets the default one, written to the API server.
    /// Returns whether the claim was updated.
    async fn assign_default_storage_class(&self, pvc: &mut PersistentVolumeClaim) -> Result<bool> {
        // `PersistentVolumeClaimHasClass` (component-helpers helpers.go:28-39).
        let has_class = pvc.spec.storage_class_name.is_some()
            || pvc
                .metadata
                .annotations
                .as_ref()
                .is_some_and(|a| a.contains_key(ANN_BETA_STORAGE_CLASS));
        if has_class {
            return Ok(false);
        }
        let classes: Vec<StorageClass> = self.storage.list("/registry/storageclasses/").await?;
        let Some(class) = get_default_class(classes) else {
            return Ok(false);
        };
        pvc.spec.storage_class_name = Some(class.metadata.name);
        let key = build_key(
            "persistentvolumeclaims",
            pvc.metadata.namespace.as_deref(),
            &pvc.metadata.name,
        );
        self.storage.update(&key, &*pvc).await?;
        Ok(true)
    }

    /// `emitEventForUnboundDelayBindingClaim` (`pv_controller.go:306-326`).
    async fn emit_event_for_unbound_delay_binding_claim(
        &self,
        pvc: &PersistentVolumeClaim,
    ) -> Result<()> {
        let mut reason = "WaitForFirstConsumer";
        let mut message = "waiting for first consumer to be created before binding".to_string();
        let pod_names = self.find_non_scheduled_pods_by_pvc(pvc).await?;
        if !pod_names.is_empty() {
            reason = "WaitForPodScheduled";
            message = if pod_names.len() > 1 {
                format!("waiting for pods {} to be scheduled", pod_names.join(","))
            } else {
                format!("waiting for pod {} to be scheduled", pod_names[0])
            };
        }
        self.event(object_ref_for_pvc(pvc), reason, &message).await;
        Ok(())
    }

    /// `findNonScheduledPodsByPVC` (`pv_controller.go:1477-1493`) over
    /// `PodPVCIndexFunc` (`pkg/controller/volume/common/common.go:35-55`).
    async fn find_non_scheduled_pods_by_pvc(
        &self,
        pvc: &PersistentVolumeClaim,
    ) -> Result<Vec<String>> {
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("");
        let pods: Vec<Pod> = self
            .storage
            .list(&format!("/registry/pods/{}/", namespace))
            .await?;
        let mut names = Vec::new();
        for pod in pods {
            let Some(spec) = pod.spec.as_ref() else {
                continue;
            };
            let uses_claim = spec.volumes.iter().flatten().any(|v| {
                if let Some(src) = v.persistent_volume_claim.as_ref() {
                    src.claim_name == pvc.metadata.name
                } else if v.ephemeral.is_some() {
                    // `ephemeral.VolumeClaimName`: `<pod>-<volume>`.
                    format!("{}-{}", pod.metadata.name, v.name) == pvc.metadata.name
                } else {
                    false
                }
            });
            if !uses_claim || is_pod_terminated(&pod) {
                continue;
            }
            if spec.node_name.as_deref().unwrap_or("").is_empty() {
                names.push(pod.metadata.name.clone());
            }
        }
        Ok(names)
    }

    /// Complete a PVC↔PV binding: pin the PV's `claimRef` to this PVC, mark both
    /// Bound, and persist them. Shared by the pre-bound (claimRef) and the
    /// first-match (unclaimed) paths in [`Self::bind_pvc`].
    async fn complete_binding(
        &self,
        mut pv: PersistentVolume,
        pvc: &mut PersistentVolumeClaim,
    ) -> Result<()> {
        let namespace = pvc.metadata.namespace.clone().unwrap_or("default".into());
        let pvc_name = pvc.metadata.name.clone();

        let pv_name = pv.metadata.name.clone();

        // GetBindVolumeToClaim (component-helpers pv_helpers.go:120-154): a PV
        // not already bound/pre-bound to this claim is bound BY the controller,
        // recorded in `pv.kubernetes.io/bound-by-controller` so that
        // `unbindVolume` knows it may clear the whole claimRef.
        if !is_volume_bound_to_claim(&pv, pvc) {
            pv.metadata
                .annotations
                .get_or_insert_with(Default::default)
                .entry(ANN_BOUND_BY_CONTROLLER.to_string())
                .or_insert_with(|| "yes".to_string());
        }
        // Pin the PV to this PVC (idempotent for an already pre-bound PV).
        pv.spec.claim_ref = Some(
            rusternetes_common::resources::service_account::ObjectReference {
                kind: Some("PersistentVolumeClaim".to_string()),
                namespace: Some(namespace.clone()),
                name: Some(pvc_name.clone()),
                uid: Some(pvc.metadata.uid.clone()),
                api_version: Some("v1".to_string()),
                resource_version: None,
                field_path: None,
            },
        );
        pv.status = Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Bound,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        });
        let pv_key = build_key("persistentvolumes", None, &pv_name);
        // Upstream binds in two writes, and so must we: the spec half
        // (`claimRef`) goes through the main resource
        // (`PersistentVolumes().Update`, pv_controller.go:1019) and the phase
        // through the status subresource (`UpdateStatus`, pv_controller.go:925).
        // A single full-object PUT would have its `.status` stripped by a
        // conformant api-server, leaving the PV bound but never Bound (#1723).
        self.storage.update(&pv_key, &pv).await?;
        self.storage.update_status(&pv_key, &pv).await?;

        bind_claim_to_volume(pvc, &pv_name);
        let pvc_key = build_key("persistentvolumeclaims", Some(&namespace), &pvc_name);
        // Same two-write split for the claim: `spec.volumeName` via the main
        // resource (`bindClaimToVolume`), then phase/capacity/VAC via the
        // status subresource (`updateClaimStatus(claim, ClaimBound, volume)`,
        // `bind` at pv_controller.go:1122).
        self.storage.update(&pvc_key, pvc).await?;
        self.update_claim_status(pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await?;

        info!(
            "Successfully bound PVC {}/{} to PV {}",
            namespace, pvc_name, pv_name
        );
        Ok(())
    }

    /// Check if a PV matches the requirements of a PVC
    fn pv_matches_pvc(
        &self,
        pv_spec: &rusternetes_common::resources::PersistentVolumeSpec,
        pvc_spec: &rusternetes_common::resources::PersistentVolumeClaimSpec,
    ) -> bool {
        // Check storage class match
        if let (Some(pv_class), Some(pvc_class)) =
            (&pv_spec.storage_class_name, &pvc_spec.storage_class_name)
        {
            if pv_class != pvc_class {
                return false;
            }
        }

        // Check capacity
        if let (Some(pv_storage), Some(pvc_storage)) = (
            pv_spec.capacity.get("storage"),
            pvc_spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage")),
        ) {
            // Simple string comparison - in real Kubernetes, this would parse quantities
            // For now, we'll just check if PV storage >= PVC storage
            if !self.storage_sufficient(pv_storage, pvc_storage) {
                return false;
            }
        }

        // Check access modes - PV must support all modes requested by PVC
        for pvc_mode in &pvc_spec.access_modes {
            if !pv_spec.access_modes.contains(pvc_mode) {
                return false;
            }
        }

        true
    }

    /// Check if PV storage is sufficient for PVC
    /// This is a simple string comparison for now
    fn storage_sufficient(&self, pv_storage: &str, pvc_storage: &str) -> bool {
        // Parse the numeric part and unit from storage strings like "10Gi", "5Gi"
        let parse_storage = |s: &str| -> Option<(f64, String)> {
            let numeric_end = s.chars().position(|c| !c.is_numeric() && c != '.')?;
            let (num_str, unit) = s.split_at(numeric_end);
            let num = num_str.parse::<f64>().ok()?;
            Some((num, unit.to_string()))
        };

        match (parse_storage(pv_storage), parse_storage(pvc_storage)) {
            (Some((pv_num, pv_unit)), Some((pvc_num, pvc_unit))) => {
                // Units must match
                if pv_unit != pvc_unit {
                    debug!(
                        "Storage units don't match: PV has {}, PVC needs {}",
                        pv_unit, pvc_unit
                    );
                    return false;
                }
                // PV must have at least as much storage as PVC
                let sufficient = pv_num >= pvc_num;
                debug!(
                    "Storage comparison: PV has {}{}, PVC needs {}{} -> sufficient: {}",
                    pv_num, pv_unit, pvc_num, pvc_unit, sufficient
                );
                sufficient
            }
            _ => {
                debug!(
                    "Failed to parse storage values: PV='{}', PVC='{}'",
                    pv_storage, pvc_storage
                );
                // Fall back to string comparison if parsing fails
                pv_storage >= pvc_storage
            }
        }
    }
}

/// True if at least one node satisfies the PV's required node affinity.
///
/// `required.nodeSelectorTerms` are ORed; within a term every match expression
/// and match field must hold (AND). Mirrors upstream
/// `pkg/apis/core/v1/helper.MatchNodeSelectorTerms` semantics for the operators
/// rusternetes models.
/// True when a PV's `claimRef` names this exact PVC (namespace + name), with a
/// matching UID when the ref carries one. A pre-bound PV from the dynamic
/// provisioner sets all three; the UID guard prevents binding to a PV that was
/// pinned to an earlier, since-deleted PVC of the same name (#1095).
/// `storagehelpers.CheckVolumeModeMismatches`
/// (`staging/src/k8s.io/component-helpers/storage/volume/pv_helpers.go:331-343`):
/// a nil volumeMode defaults to Filesystem on both sides.
/// Claim-side metadata of `bindClaimToVolume`
/// (`pv_controller.go:1037-1092`): when the claim is not yet bound to this
/// volume (`:1044-1047`) set `spec.volumeName` and, unless already present,
/// `pv.kubernetes.io/bound-by-controller: "yes"` (`:1058-1066`); then set
/// `pv.kubernetes.io/bind-completed: "yes"` unless already present
/// (`:1064-1068`), which is what `syncClaim` (`:251`) and the scheduler's
/// `isPVCBound` (`volumebinding/binder.go:776`) key on.
/// `storagehelpers.PVDeletionInTreeProtectionFinalizer`
/// (component-helpers pv_helpers.go:82).
const IN_TREE_PV_DELETION_PROTECTION_FINALIZER: &str = "kubernetes.io/pv-controller";
/// `storagehelpers.PVDeletionProtectionFinalizer` (pv_helpers.go:79).
const EXTERNAL_PV_DELETION_PROTECTION_FINALIZER: &str =
    "external-provisioner.volume.kubernetes.io/finalizer";

/// `csilibplugins.*InTreePluginName` -> `*DriverName`, the entries of
/// `inTreePlugins` in `csi-translation-lib/translate.go:30-38` (names from
/// `plugins/{aws_ebs,gce_pd,azure_file,azure_disk,openstack_cinder,
/// vsphere_volume,portworx}.go`). Only the name mapping is ported; Rusternetes
/// has no volume-source translation.
const IN_TREE_TO_CSI_DRIVER: &[(&str, &str)] = &[
    ("kubernetes.io/gce-pd", "pd.csi.storage.gke.io"),
    ("kubernetes.io/aws-ebs", "ebs.csi.aws.com"),
    ("kubernetes.io/cinder", "cinder.csi.openstack.org"),
    ("kubernetes.io/azure-disk", "disk.csi.azure.com"),
    ("kubernetes.io/azure-file", "file.csi.azure.com"),
    ("kubernetes.io/vsphere-volume", "csi.vsphere.vmware.com"),
    ("kubernetes.io/portworx-volume", "pxd.portworx.com"),
];

/// `CSITranslator.GetCSINameFromInTreeName` (`translate.go:168-175`).
fn csi_name_from_in_tree_name(plugin: &str) -> Option<&'static str> {
    IN_TREE_TO_CSI_DRIVER
        .iter()
        .find(|(p, _)| *p == plugin)
        .map(|(_, d)| *d)
}

/// `PluginManager.IsMigrationEnabledForPlugin`
/// (`pkg/volume/csimigration/plugin_manager.go:85-107`). In 1.35 every
/// per-plugin gate is GA and locked on (`CSIMigrationPortworx`:
/// `kube_features.go:1178`), so each translator plugin is migrated.
fn is_migration_enabled_for_plugin(plugin: &str) -> bool {
    csi_name_from_in_tree_name(plugin).is_some()
}

/// `updateMigrationAnnotations` (`pv_controller_base.go:445-496`): the one
/// helper for volumes (`claim == false`, provisioner key
/// `AnnDynamicallyProvisioned`) and claims (`claim == true`, key
/// `AnnStorageProvisioner`, else the beta key; `:455-470`). Adds
/// `migrated-to` for a migrated plugin (`:477-485`) and removes it when
/// migration is off (`:486-490`). Returns whether the map changed.
fn update_migration_annotations(
    ann: Option<&mut std::collections::HashMap<String, String>>,
    claim: bool,
) -> bool {
    let Some(ann) = ann else { return false };
    let provisioner = if claim {
        ann.get(ANN_STORAGE_PROVISIONER)
            .or_else(|| ann.get(ANN_BETA_STORAGE_PROVISIONER))
    } else {
        ann.get(ANN_DYNAMICALLY_PROVISIONED)
    };
    let Some(provisioner) = provisioner.cloned() else {
        return false;
    };
    let migrated_to = ann.get(ANN_MIGRATED_TO).cloned().unwrap_or_default();
    if is_migration_enabled_for_plugin(&provisioner) {
        let Some(driver) = csi_name_from_in_tree_name(&provisioner) else {
            return false;
        };
        if migrated_to != driver {
            ann.insert(ANN_MIGRATED_TO.to_string(), driver.to_string());
            return true;
        }
    } else if !migrated_to.is_empty() {
        ann.remove(ANN_MIGRATED_TO);
        return true;
    }
    false
}

/// `modifyDeletionFinalizers` (`pv_controller_base.go:398-443`) with CSI
/// migration per `is_migration_enabled_for_plugin`;
/// `HonorPVReclaimPolicy` is GA and locked on in 1.35. Returns the new
/// finalizers and whether they changed.
fn modify_deletion_finalizers(pv: &PersistentVolume) -> (Option<Vec<String>>, bool) {
    let unchanged = || (pv.metadata.finalizers.clone(), false);
    let Some(provisioner) = pv
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANN_DYNAMICALLY_PROVISIONED))
    else {
        // Supported only for dynamically provisioned volumes.
        return unchanged();
    };
    let mut out = pv.metadata.finalizers.clone().unwrap_or_default();
    let mut modified = false;
    let has = |out: &[String], f: &str| out.iter().any(|x| x == f);
    if is_migration_enabled_for_plugin(provisioner) {
        // `:411-420`: remove the in-tree delete finalizer, migration is on.
        if has(&out, IN_TREE_PV_DELETION_PROTECTION_FINALIZER) {
            out.retain(|f| f != IN_TREE_PV_DELETION_PROTECTION_FINALIZER);
            modified = true;
        }
        return (
            if modified {
                Some(out).filter(|o| !o.is_empty())
            } else {
                pv.metadata.finalizers.clone()
            },
            modified,
        );
    }
    if !provisioner.starts_with("kubernetes.io/") {
        return unchanged();
    }
    let policy = pv.spec.persistent_volume_reclaim_policy.as_ref();
    if policy == Some(&PersistentVolumeReclaimPolicy::Delete)
        && !has(&out, IN_TREE_PV_DELETION_PROTECTION_FINALIZER)
    {
        out.push(IN_TREE_PV_DELETION_PROTECTION_FINALIZER.to_string());
        modified = true;
    } else if matches!(
        policy,
        Some(PersistentVolumeReclaimPolicy::Retain | PersistentVolumeReclaimPolicy::Recycle)
    ) && has(&out, IN_TREE_PV_DELETION_PROTECTION_FINALIZER)
    {
        out.retain(|f| f != IN_TREE_PV_DELETION_PROTECTION_FINALIZER);
        modified = true;
    }
    if has(&out, EXTERNAL_PV_DELETION_PROTECTION_FINALIZER) {
        out.retain(|f| f != EXTERNAL_PV_DELETION_PROTECTION_FINALIZER);
        modified = true;
    }
    if !modified {
        return unchanged();
    }
    (if out.is_empty() { None } else { Some(out) }, true)
}

fn bind_claim_to_volume(pvc: &mut PersistentVolumeClaim, volume_name: &str) {
    if pvc.spec.volume_name.as_deref() != Some(volume_name) {
        pvc.spec.volume_name = Some(volume_name.to_string());
        pvc.metadata
            .annotations
            .get_or_insert_with(Default::default)
            .entry(ANN_BOUND_BY_CONTROLLER.to_string())
            .or_insert_with(|| "yes".to_string());
    }
    pvc.metadata
        .annotations
        .get_or_insert_with(Default::default)
        .entry(ANN_BIND_COMPLETED.to_string())
        .or_insert_with(|| "yes".to_string());
}

/// `storagehelpers.GetPersistentVolumeClaimClass` / `GetPersistentVolumeClass`
/// (pv_helpers.go): `spec.storageClassName`, else the legacy beta annotation,
/// else "".
fn class_of<'a>(
    spec_class: Option<&'a str>,
    annotations: Option<&'a std::collections::HashMap<String, String>>,
) -> &'a str {
    spec_class
        .or_else(|| {
            annotations
                .and_then(|a| a.get("volume.beta.kubernetes.io/storage-class"))
                .map(String::as_str)
        })
        .unwrap_or("")
}

fn volume_mode_mismatches(claim: &PersistentVolumeClaimSpec, pv: &PersistentVolumeSpec) -> bool {
    let requested = claim
        .volume_mode
        .clone()
        .unwrap_or(PersistentVolumeMode::Filesystem);
    let have = pv
        .volume_mode
        .clone()
        .unwrap_or(PersistentVolumeMode::Filesystem);
    requested != have
}

/// `storagehelpers.IsVolumeBoundToClaim` (pv_helpers.go:159-169): name and
/// namespace must match, and so must the UID when the claimRef carries one.
fn is_volume_bound_to_claim(pv: &PersistentVolume, pvc: &PersistentVolumeClaim) -> bool {
    let Some(cr) = pv.spec.claim_ref.as_ref() else {
        return false;
    };
    if cr.name.as_deref() != Some(pvc.metadata.name.as_str())
        || cr.namespace.as_deref() != pvc.metadata.namespace.as_deref()
    {
        return false;
    }
    match cr.uid.as_deref() {
        Some(uid) if !uid.is_empty() => uid == pvc.metadata.uid,
        _ => true,
    }
}

fn object_ref_for_pv(pv: &PersistentVolume) -> ObjectReference {
    ObjectReference {
        kind: Some("PersistentVolume".to_string()),
        namespace: None,
        name: Some(pv.metadata.name.clone()),
        uid: Some(pv.metadata.uid.clone()),
        api_version: Some("v1".to_string()),
        resource_version: pv.metadata.resource_version.clone(),
        field_path: None,
    }
}

fn object_ref_for_pvc(pvc: &PersistentVolumeClaim) -> ObjectReference {
    ObjectReference {
        kind: Some("PersistentVolumeClaim".to_string()),
        namespace: pvc.metadata.namespace.clone(),
        name: Some(pvc.metadata.name.clone()),
        uid: Some(pvc.metadata.uid.clone()),
        api_version: Some("v1".to_string()),
        resource_version: pvc.metadata.resource_version.clone(),
        field_path: None,
    }
}

fn claim_ref_points_to(
    claim_ref: &rusternetes_common::resources::service_account::ObjectReference,
    namespace: &str,
    pvc_name: &str,
    pvc_uid: &str,
) -> bool {
    claim_ref.name.as_deref() == Some(pvc_name)
        && claim_ref.namespace.as_deref() == Some(namespace)
        && claim_ref.uid.as_ref().is_none_or(|u| u == pvc_uid)
}

fn node_affinity_satisfied(node_affinity: &VolumeNodeAffinity, nodes: &[Node]) -> bool {
    let required = match &node_affinity.required {
        Some(r) => r,
        None => return true,
    };
    nodes.iter().any(|node| {
        required
            .node_selector_terms
            .iter()
            .any(|term| volume_term_matches(node, term))
    })
}

/// A single node-selector term matches when all of its match expressions (over
/// node labels) and match fields (over `metadata.name`) hold.
fn volume_term_matches(node: &Node, term: &NodeSelectorTerm) -> bool {
    let labels = node.metadata.labels.as_ref();
    if let Some(exprs) = term.match_expressions.as_ref() {
        for req in exprs {
            let value = labels.and_then(|l| l.get(&req.key)).map(|s| s.as_str());
            if !requirement_matches(value, &req.operator, req.values.as_deref()) {
                return false;
            }
        }
    }
    if let Some(fields) = term.match_fields.as_ref() {
        for req in fields {
            let value = match req.key.as_str() {
                "metadata.name" => Some(node.metadata.name.as_str()),
                _ => None,
            };
            if !requirement_matches(value, &req.operator, req.values.as_deref()) {
                return false;
            }
        }
    }
    true
}

/// Evaluate one selector requirement against a (possibly absent) node value.
fn requirement_matches(value: Option<&str>, operator: &str, values: Option<&[String]>) -> bool {
    let values = values.unwrap_or(&[]);
    match operator {
        "In" => value
            .map(|v| values.iter().any(|x| x == v))
            .unwrap_or(false),
        "NotIn" => value
            .map(|v| !values.iter().any(|x| x == v))
            .unwrap_or(true),
        "Exists" => value.is_some(),
        "DoesNotExist" => value.is_none(),
        "Gt" | "Lt" => {
            let (node_val, req_val) = match (value, values.first()) {
                (Some(v), Some(r)) => match (v.parse::<i64>(), r.parse::<i64>()) {
                    (Ok(a), Ok(b)) => (a, b),
                    _ => return false,
                },
                _ => return false,
            };
            if operator == "Gt" {
                node_val > req_val
            } else {
                node_val < req_val
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::service_account::ObjectReference;
    use rusternetes_common::resources::volume::{
        PersistentVolumeAccessMode, PersistentVolumeClaimPhase, PersistentVolumeClaimStatus,
        PersistentVolumeMode, ResourceRequirements,
    };
    use rusternetes_common::resources::{
        PersistentVolumeClaimSpec, PersistentVolumeSpec, PersistentVolumeStatus,
    };
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    fn ann_map(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// pv_controller_test.go TestAnnealMigrationAnnotations: every case, run
    /// through the one `updateMigrationAnnotations(..., claim)` helper
    /// (`pv_controller_base.go:445-496`). Migrated plugin = gce-pd
    /// (`IsMigrationEnabledForPlugin`, plugin_manager.go:85-107); non-migrated
    /// = rbd; `non-migrated-plugin` = unknown.
    #[test]
    fn anneal_migration_annotations_table() {
        const GCE: &str = "kubernetes.io/gce-pd";
        const GCE_DRIVER: &str = "pd.csi.storage.gke.io";
        const RBD: &str = "kubernetes.io/rbd";
        const RBD_DRIVER: &str = "rbd.csi.ceph.com";
        let vol = ANN_DYNAMICALLY_PROVISIONED;
        let claim = ANN_STORAGE_PROVISIONER;
        let beta = ANN_BETA_STORAGE_PROVISIONER;
        // (key, in, expected, is_claim)
        type Kv<'a> = Vec<(&'a str, &'a str)>;
        let cases: Vec<(&str, Kv, Kv, bool)> = vec![
            (
                "vol on",
                vec![(vol, GCE)],
                vec![(vol, GCE), (ANN_MIGRATED_TO, GCE_DRIVER)],
                false,
            ),
            (
                "claim on",
                vec![(claim, GCE)],
                vec![(claim, GCE), (ANN_MIGRATED_TO, GCE_DRIVER)],
                true,
            ),
            (
                "claim on beta",
                vec![(beta, GCE)],
                vec![(beta, GCE), (ANN_MIGRATED_TO, GCE_DRIVER)],
                true,
            ),
            ("vol off", vec![(vol, RBD)], vec![(vol, RBD)], false),
            ("claim off", vec![(claim, RBD)], vec![(claim, RBD)], true),
            (
                "vol rollback",
                vec![(vol, RBD), (ANN_MIGRATED_TO, RBD_DRIVER)],
                vec![(vol, RBD)],
                false,
            ),
            (
                "claim rollback",
                vec![(claim, RBD), (ANN_MIGRATED_TO, RBD_DRIVER)],
                vec![(claim, RBD)],
                true,
            ),
            (
                "claim rollback beta",
                vec![(beta, RBD), (ANN_MIGRATED_TO, RBD_DRIVER)],
                vec![(beta, RBD)],
                true,
            ),
            (
                "vol other",
                vec![(vol, "non-migrated-plugin")],
                vec![(vol, "non-migrated-plugin")],
                false,
            ),
            ("not provisioned", vec![], vec![], false),
            (
                "wrong key for kind",
                vec![(claim, GCE)],
                vec![(claim, GCE)],
                false,
            ),
            (
                "vol stale driver",
                vec![(vol, GCE), (ANN_MIGRATED_TO, "old")],
                vec![(vol, GCE), (ANN_MIGRATED_TO, GCE_DRIVER)],
                false,
            ),
        ];
        for (name, input, want, is_claim) in cases {
            let mut ann = ann_map(&input);
            let changed = update_migration_annotations(Some(&mut ann), is_claim);
            assert_eq!(ann, ann_map(&want), "{name}");
            assert_eq!(
                changed,
                input.iter().collect::<std::collections::BTreeSet<_>>() != want.iter().collect(),
                "{name}: changed flag"
            );
        }
        assert!(!update_migration_annotations(None, true));
    }

    /// `IsMigrationEnabledForPlugin` (plugin_manager.go:85-107) with the
    /// 1.35 GA-locked gates: all seven translator plugins are migrated.
    #[test]
    fn migration_enabled_plugins_match_upstream() {
        for (plugin, driver) in [
            ("kubernetes.io/aws-ebs", "ebs.csi.aws.com"),
            ("kubernetes.io/gce-pd", "pd.csi.storage.gke.io"),
            ("kubernetes.io/azure-file", "file.csi.azure.com"),
            ("kubernetes.io/azure-disk", "disk.csi.azure.com"),
            ("kubernetes.io/cinder", "cinder.csi.openstack.org"),
            ("kubernetes.io/vsphere-volume", "csi.vsphere.vmware.com"),
            ("kubernetes.io/portworx-volume", "pxd.portworx.com"),
        ] {
            assert!(is_migration_enabled_for_plugin(plugin), "{plugin}");
            assert_eq!(csi_name_from_in_tree_name(plugin), Some(driver));
        }
        assert!(!is_migration_enabled_for_plugin("kubernetes.io/rbd"));
        assert_eq!(csi_name_from_in_tree_name("kubernetes.io/rbd"), None);
    }

    /// TestModifyDeletionFinalizers migration-on branch
    /// (`pv_controller_base.go:411-420`): the in-tree delete finalizer is
    /// removed for a migrated plugin and nothing else is touched.
    #[test]
    fn modify_deletion_finalizers_migration_on_removes_in_tree_finalizer() {
        let mut pv = pv_with("m", None, Some(PersistentVolumePhase::Available));
        pv.metadata.annotations = Some(ann_map(&[(
            ANN_DYNAMICALLY_PROVISIONED,
            "kubernetes.io/gce-pd",
        )]));
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        pv.metadata.finalizers = Some(vec![
            IN_TREE_PV_DELETION_PROTECTION_FINALIZER.into(),
            EXTERNAL_PV_DELETION_PROTECTION_FINALIZER.into(),
        ]);
        let (out, modified) = modify_deletion_finalizers(&pv);
        assert!(modified);
        assert_eq!(
            out,
            Some(vec![EXTERNAL_PV_DELETION_PROTECTION_FINALIZER.to_string()])
        );
        // migrated plugin without the in-tree finalizer: unchanged (no add).
        pv.metadata.finalizers = None;
        assert!(!modify_deletion_finalizers(&pv).1);
    }

    #[test]
    fn test_storage_comparison() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PVBinderController::new(storage);

        assert!(controller.storage_sufficient("10Gi", "5Gi"));
        assert!(controller.storage_sufficient("10Gi", "10Gi"));
        assert!(!controller.storage_sufficient("5Gi", "10Gi"));
        assert!(controller.storage_sufficient("100Mi", "50Mi"));
        assert!(!controller.storage_sufficient("50Mi", "100Mi"));
    }

    #[test]
    fn claim_ref_points_to_matches_name_namespace_uid() {
        let cr = ObjectReference {
            kind: Some("PersistentVolumeClaim".into()),
            namespace: Some("ns".into()),
            name: Some("pvc".into()),
            uid: Some("uid-1".into()),
            api_version: Some("v1".into()),
            resource_version: None,
            field_path: None,
        };
        assert!(claim_ref_points_to(&cr, "ns", "pvc", "uid-1"));
        assert!(!claim_ref_points_to(&cr, "ns", "other", "uid-1")); // wrong name
        assert!(!claim_ref_points_to(&cr, "other", "pvc", "uid-1")); // wrong ns
        assert!(!claim_ref_points_to(&cr, "ns", "pvc", "uid-2")); // stale uid
                                                                  // A pre-bind without a uid still counts.
        let cr_no_uid = ObjectReference {
            uid: None,
            ..cr.clone()
        };
        assert!(claim_ref_points_to(&cr_no_uid, "ns", "pvc", "uid-1"));
    }

    fn make_pvc(name: &str, uid: &str) -> PersistentVolumeClaim {
        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "1Gi".to_string());
        PersistentVolumeClaim {
            type_meta: TypeMeta {
                kind: "PersistentVolumeClaim".into(),
                api_version: "v1".into(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some("sstest".into());
                m.uid = uid.into();
                m
            },
            spec: PersistentVolumeClaimSpec {
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                resources: ResourceRequirements {
                    limits: None,
                    requests: Some(requests),
                },
                volume_name: None,
                storage_class_name: Some("standard".into()),
                volume_mode: None,
                selector: None,
                data_source: None,
                data_source_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Pending,
                access_modes: None,
                capacity: None,
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            }),
        }
    }

    fn make_prebound_pv(pv_name: &str, pvc_name: &str, pvc_uid: &str) -> PersistentVolume {
        let mut capacity = HashMap::new();
        capacity.insert("storage".to_string(), "1Gi".to_string());
        PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".into(),
                api_version: "v1".into(),
            },
            metadata: ObjectMeta::new(pv_name),
            spec: PersistentVolumeSpec {
                capacity,
                host_path: None,
                nfs: None,
                iscsi: None,
                local: None,
                csi: None,
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                persistent_volume_reclaim_policy: None,
                storage_class_name: Some("standard".into()),
                mount_options: None,
                volume_mode: None,
                node_affinity: None,
                claim_ref: Some(ObjectReference {
                    kind: Some("PersistentVolumeClaim".into()),
                    namespace: Some("sstest".into()),
                    name: Some(pvc_name.into()),
                    uid: Some(pvc_uid.into()),
                    api_version: Some("v1".into()),
                    resource_version: None,
                    field_path: None,
                }),
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Available,
                message: None,
                reason: None,
                last_phase_transition_time: None,
            }),
        }
    }

    fn pv_with(
        name: &str,
        claim_uid: Option<Option<&str>>,
        phase: Option<PersistentVolumePhase>,
    ) -> PersistentVolume {
        let mut pv = make_prebound_pv(name, "c", "u");
        pv.spec.claim_ref = claim_uid.map(|uid| ObjectReference {
            uid: uid.map(String::from),
            ..pv.spec.claim_ref.clone().unwrap()
        });
        pv.status = phase.map(|phase| PersistentVolumeStatus {
            phase,
            message: Some("stale".into()),
            reason: None,
            last_phase_transition_time: None,
        });
        pv
    }

    /// #2066: upstream syncVolume (pv_controller.go:576-596) moves a PV with no
    /// claimRef, or a claimRef with an empty UID, to Available; a claimRef with
    /// a UID is the bound branch and is untouched.
    #[tokio::test]
    async fn sync_unbound_volumes_moves_pending_to_available() {
        use PersistentVolumePhase::*;
        let storage = Arc::new(MemoryStorage::new());
        let controller = PVBinderController::new(storage.clone());
        let cases = [
            (
                "nil-claim",
                pv_with("nil-claim", None, Some(Pending)),
                Available,
            ),
            ("no-status", pv_with("no-status", None, None), Available),
            (
                "empty-uid",
                pv_with("empty-uid", Some(Some("")), Some(Pending)),
                Available,
            ),
            (
                "absent-uid",
                pv_with("absent-uid", Some(None), Some(Pending)),
                Available,
            ),
            (
                // claimRef with a UID and no such claim: the claim-not-found
                // branch (pv_controller.go:598+), covered in depth below.
                "bound-uid",
                pv_with("bound-uid", Some(Some("u")), Some(Pending)),
                Released,
            ),
        ];
        for (name, pv, _) in &cases {
            storage
                .create(&build_key("persistentvolumes", None, name), pv)
                .await
                .unwrap();
        }
        controller.sync_volumes(&WorkQueue::new()).await.unwrap();
        for (name, _, want) in &cases {
            let got: PersistentVolume = storage
                .get(&build_key("persistentvolumes", None, name))
                .await
                .unwrap();
            assert_eq!(got.status.as_ref().unwrap().phase, *want, "{name}");
        }
        let got: PersistentVolume = storage
            .get(&build_key("persistentvolumes", None, "nil-claim"))
            .await
            .unwrap();
        assert_eq!(got.status.unwrap().message, None, "message cleared");
    }

    /// #1095: two PVCs whose PVs were pre-bound (claimRef) by the dynamic
    /// provisioner must each bind to THEIR OWN PV, never cross. PVs are stored
    /// in the reverse of the bind order so a first-match binder would mis-pair.
    #[tokio::test]
    async fn prebound_pvs_do_not_cross_bind() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PVBinderController::new(storage.clone());

        let pv_a = make_prebound_pv("pvc-sstest-explicit-pvc", "explicit-pvc", "uid-a");
        let pv_b = make_prebound_pv("pvc-sstest-default-pvc", "default-pvc", "uid-b");
        // Insert b first, a second — list order must not decide the pairing.
        storage
            .create(
                &build_key("persistentvolumes", None, &pv_b.metadata.name),
                &pv_b,
            )
            .await
            .unwrap();
        storage
            .create(
                &build_key("persistentvolumes", None, &pv_a.metadata.name),
                &pv_a,
            )
            .await
            .unwrap();

        let mut explicit = make_pvc("explicit-pvc", "uid-a");
        let mut default = make_pvc("default-pvc", "uid-b");
        // PVCs exist in storage (created by the user) before binding; the binder
        // persists the bound result via update().
        for pvc in [&explicit, &default] {
            storage
                .create(
                    &build_key(
                        "persistentvolumeclaims",
                        pvc.metadata.namespace.as_deref(),
                        &pvc.metadata.name,
                    ),
                    pvc,
                )
                .await
                .unwrap();
        }

        controller.bind_pvc(&mut explicit).await.unwrap();
        assert_eq!(
            explicit.spec.volume_name.as_deref(),
            Some("pvc-sstest-explicit-pvc"),
            "explicit-pvc must bind to its own pre-bound PV"
        );

        controller.bind_pvc(&mut default).await.unwrap();
        assert_eq!(
            default.spec.volume_name.as_deref(),
            Some("pvc-sstest-default-pvc"),
            "default-pvc must bind to its own pre-bound PV, not explicit-pvc's"
        );

        // Each PV's claimRef ends up pinned to the matching PVC.
        let bound_a: PersistentVolume = storage
            .get(&build_key(
                "persistentvolumes",
                None,
                "pvc-sstest-explicit-pvc",
            ))
            .await
            .unwrap();
        assert_eq!(
            bound_a.spec.claim_ref.unwrap().name.as_deref(),
            Some("explicit-pvc")
        );
    }

    // ---- syncVolume bound branches (pv_controller.go:598-770; binder_test.go 4-3..4-12) ----

    async fn put_pv(storage: &Arc<MemoryStorage>, pv: &PersistentVolume) {
        storage
            .create(&build_key("persistentvolumes", None, &pv.metadata.name), pv)
            .await
            .unwrap();
    }

    async fn put_pvc(storage: &Arc<MemoryStorage>, pvc: &PersistentVolumeClaim) {
        storage
            .create(
                &build_key(
                    "persistentvolumeclaims",
                    pvc.metadata.namespace.as_deref(),
                    &pvc.metadata.name,
                ),
                pvc,
            )
            .await
            .unwrap();
    }

    async fn get_pv(storage: &Arc<MemoryStorage>, name: &str) -> Option<PersistentVolume> {
        storage
            .get(&build_key("persistentvolumes", None, name))
            .await
            .ok()
    }

    async fn volume_mismatch_events(
        storage: &Arc<MemoryStorage>,
    ) -> Vec<rusternetes_common::resources::Event> {
        let all: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        all.into_iter()
            .filter(|e| e.reason == "VolumeMismatch")
            .collect()
    }

    fn bound_pv(name: &str) -> PersistentVolume {
        pv_with(name, Some(Some("u")), Some(PersistentVolumePhase::Bound))
    }

    /// binder_test.go "4-3": bound volume with missing claim -> Released, and
    /// Retain leaves it (and its claimRef) in place.
    #[tokio::test]
    async fn bound_pv_with_missing_claim_is_released_and_retained() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Retain);
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.expect("retained");
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Released);
        assert!(got.spec.claim_ref.is_some());
    }

    /// binder_test.go "4-4": claim recreated with a different UID counts as
    /// missing.
    #[tokio::test]
    async fn bound_pv_with_claim_of_different_uid_is_released() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &bound_pv("pv")).await;
        put_pvc(&storage, &make_pvc("c", "other-uid")).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Released);
    }

    /// reclaimVolume Delete (pv_controller.go:1180+): released PV is deleted.
    #[tokio::test]
    async fn released_pv_with_delete_policy_is_deleted() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        assert!(get_pv(&storage, "pv").await.is_none());
    }

    /// A released `Delete` PV backed by a hostPath. All paths live under a
    /// unique `/tmp/hostpath.<uuid>` so the deleter's `/tmp/.+` guard passes
    /// and no test can touch anything outside its own scratch directory.
    fn host_path_delete_pv(name: &str, path: &str) -> PersistentVolume {
        use rusternetes_common::resources::volume::HostPathVolumeSource;
        let mut pv = bound_pv(name);
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        pv.spec.host_path = Some(HostPathVolumeSource {
            path: path.to_string(),
            r#type: None,
        });
        pv
    }

    fn scratch_dir() -> std::path::PathBuf {
        std::path::PathBuf::from(format!("/tmp/hostpath.{}", uuid::Uuid::new_v4()))
    }

    /// host_path_test.go `TestDeleter`: the deleter removes the directory,
    /// then the PV object is deleted (`deleteVolumeOperation`,
    /// pv_controller.go:1323-1393).
    #[tokio::test]
    async fn host_path_deleter_removes_directory_then_pv() {
        let dir = scratch_dir();
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/data"), b"x").unwrap();
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &host_path_delete_pv("pv", dir.to_str().unwrap())).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let existed = dir.exists();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!existed, "hostPath directory must be wiped by the deleter");
        assert!(get_pv(&storage, "pv").await.is_none());
    }

    /// host_path_test.go `TestDeleterTempDir` "just-tmp" / "not-tmp": the
    /// deleter refuses, so the PV goes Failed with a `VolumeFailedDelete`
    /// warning and is NOT deleted (`deleteVolumeOperation`, :1360-1366).
    #[tokio::test]
    async fn host_path_deleter_refuses_paths_outside_tmp() {
        for path in ["/tmp", "/tmp/", "/nottmp", "/nottmp/tmp/x"] {
            let storage = Arc::new(MemoryStorage::new());
            let c = PVBinderController::new(storage.clone());
            put_pv(&storage, &host_path_delete_pv("pv", path)).await;
            c.sync_volumes(&WorkQueue::new()).await.unwrap();
            let got = get_pv(&storage, "pv")
                .await
                .unwrap_or_else(|| panic!("{path}: PV must survive a refused delete"));
            let status = got.status.unwrap();
            assert_eq!(status.phase, PersistentVolumePhase::Failed, "{path}");
            let want =
                format!("host_path deleter only supports /tmp/.+ but received provided {path}");
            assert_eq!(status.message.as_deref(), Some(want.as_str()), "{path}");
            let events: Vec<rusternetes_common::resources::Event> =
                storage.list("/registry/events/").await.unwrap();
            assert!(
                events.iter().any(|e| e.reason == "VolumeFailedDelete"),
                "{path}: expected a VolumeFailedDelete event"
            );
        }
    }

    /// Hardening beyond upstream's unanchored `/tmp/.+` regex: a `..`
    /// component must not let a path escape /tmp/<x>/ and wipe a sibling.
    #[tokio::test]
    async fn host_path_deleter_refuses_parent_dir_traversal() {
        let victim = scratch_dir();
        std::fs::create_dir_all(&victim).unwrap();
        std::fs::write(victim.join("keep"), b"x").unwrap();
        let decoy = scratch_dir();
        std::fs::create_dir_all(&decoy).unwrap();
        let sneaky = format!(
            "{}/../{}",
            decoy.display(),
            victim.file_name().unwrap().to_str().unwrap()
        );
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &host_path_delete_pv("pv", &sneaky)).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let survived = victim.join("keep").exists();
        let _ = std::fs::remove_dir_all(&victim);
        let _ = std::fs::remove_dir_all(&decoy);
        assert!(survived, "traversal path must not delete the target");
        let got = get_pv(&storage, "pv").await.expect("PV must survive");
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Failed);
    }

    /// `os.RemoveAll` returns nil for a missing path, so an already-gone
    /// directory still lets the PV be deleted.
    #[tokio::test]
    async fn host_path_deleter_missing_directory_is_ok() {
        let dir = scratch_dir();
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &host_path_delete_pv("pv", dir.to_str().unwrap())).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        assert!(get_pv(&storage, "pv").await.is_none());
    }

    /// recycle_test.go "6-3": a Recycle volume with no recycler plugin goes
    /// Failed with message "No recycler plugin found for the volume!" and a
    /// `Warning VolumeFailedRecycle` event (pv_controller.go:1279-1286).
    #[tokio::test]
    async fn recycle_policy_without_recycler_plugin_fails_volume() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Recycle);
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let status = get_pv(&storage, "pv").await.unwrap().status.unwrap();
        assert_eq!(status.phase, PersistentVolumePhase::Failed);
        assert_eq!(
            status.message.as_deref(),
            Some("No recycler plugin found for the volume!")
        );
        let events: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        let ev: Vec<_> = events
            .iter()
            .filter(|e| e.reason == "VolumeFailedRecycle")
            .collect();
        assert_eq!(ev.len(), 1);
        // Already Failed: a further sync must not emit another event.
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let events: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.reason == "VolumeFailedRecycle")
                .count(),
            1
        );
    }

    /// "Do not overwrite previous Failed state" (pv_controller.go:672-681).
    #[tokio::test]
    async fn failed_pv_with_missing_claim_stays_failed() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let pv = pv_with("pv", Some(Some("u")), Some(PersistentVolumePhase::Failed));
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Failed);
    }

    fn dynamic_pv(
        name: &str,
        policy: PersistentVolumeReclaimPolicy,
        finalizers: &[&str],
    ) -> PersistentVolume {
        let mut pv = pv_with(name, None, Some(PersistentVolumePhase::Available));
        pv.spec.persistent_volume_reclaim_policy = Some(policy);
        pv.metadata.annotations = Some(
            [(
                ANN_DYNAMICALLY_PROVISIONED.to_string(),
                "kubernetes.io/rbd".to_string(),
            )]
            .into(),
        );
        pv.metadata.finalizers = if finalizers.is_empty() {
            None
        } else {
            Some(finalizers.iter().map(|s| s.to_string()).collect())
        };
        pv
    }

    /// pv_controller_test.go "5-9" + TestModifyDeletionFinalizers 13-3..13-6,
    /// 13-12..13-14 (CSI migration disabled): `syncVolume` first runs
    /// `updateVolumeMigrationAnnotationsAndFinalizers`
    /// (pv_controller.go:565, pv_controller_base.go:361-381).
    #[tokio::test]
    async fn sync_volume_adds_in_tree_finalizer_and_drops_external_one() {
        use PersistentVolumeReclaimPolicy::*;
        let cases: Vec<(&str, PersistentVolume, Option<Vec<&str>>)> = vec![
            // 13-4: Delete, no finalizers -> in-tree finalizer added.
            (
                "a",
                dynamic_pv("a", Delete, &[]),
                Some(vec![IN_TREE_PV_DELETION_PROTECTION_FINALIZER]),
            ),
            // 13-6: external finalizer removed, custom kept, in-tree added.
            (
                "b",
                dynamic_pv(
                    "b",
                    Delete,
                    &[EXTERNAL_PV_DELETION_PROTECTION_FINALIZER, "custom"],
                ),
                Some(vec!["custom", IN_TREE_PV_DELETION_PROTECTION_FINALIZER]),
            ),
            // 13-12: Retain, nothing to add.
            ("c", dynamic_pv("c", Retain, &[]), None),
            // 13-14: Retain removes the in-tree finalizer.
            (
                "d",
                dynamic_pv("d", Retain, &[IN_TREE_PV_DELETION_PROTECTION_FINALIZER]),
                None,
            ),
        ];
        for (name, pv, want) in cases {
            let storage = Arc::new(MemoryStorage::new());
            let c = PVBinderController::new(storage.clone());
            put_pv(&storage, &pv).await;
            c.sync_volumes(&WorkQueue::new()).await.unwrap();
            let got = get_pv(&storage, name).await.unwrap();
            let want = want.map(|v| v.into_iter().map(String::from).collect::<Vec<_>>());
            assert_eq!(got.metadata.finalizers, want, "case {name}");
        }
    }

    /// 13-11 / 13-15 / 13-10: statically provisioned or unannotated volumes
    /// are never touched, even when they carry the external finalizer.
    #[tokio::test]
    async fn sync_volume_leaves_static_volume_finalizers_alone() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = pv_with("s", None, Some(PersistentVolumePhase::Available));
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        pv.metadata.finalizers = Some(vec![EXTERNAL_PV_DELETION_PROTECTION_FINALIZER.into()]);
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "s").await.unwrap();
        assert_eq!(
            got.metadata.finalizers,
            Some(vec![EXTERNAL_PV_DELETION_PROTECTION_FINALIZER.to_string()])
        );
    }

    /// TestControllerSync 5-9 / updateMigrationAnnotations: the migrated-to
    /// annotation is removed when migration is off for the plugin.
    #[tokio::test]
    async fn sync_volume_removes_stale_migrated_to_annotation() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = dynamic_pv("m", PersistentVolumeReclaimPolicy::Retain, &[]);
        pv.metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(ANN_MIGRATED_TO.into(), "pd.csi.storage.gke.io".into());
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "m").await.unwrap();
        assert!(!got
            .metadata
            .annotations
            .unwrap_or_default()
            .contains_key(ANN_MIGRATED_TO));
    }

    /// pv_controller.go:1217-1222: an unrecognised reclaim policy fails the
    /// volume with a `VolumeUnknownReclaimPolicy` Warning event.
    #[tokio::test]
    async fn unknown_reclaim_policy_fails_volume() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut raw = serde_json::to_value(bound_pv("u")).unwrap();
        raw["spec"]["persistentVolumeReclaimPolicy"] = serde_json::json!("Bogus");
        let pv: PersistentVolume = serde_json::from_value(raw).expect("lenient decode");
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let status = get_pv(&storage, "u").await.unwrap().status.unwrap();
        assert_eq!(status.phase, PersistentVolumePhase::Failed);
        assert_eq!(
            status.message.as_deref(),
            Some("Volume has unrecognized PersistentVolumeReclaimPolicy")
        );
        let events: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.reason == "VolumeUnknownReclaimPolicy")
                .count(),
            1
        );
    }

    /// binder_test.go "4-6": volume and claim bound to each other -> Bound.
    #[tokio::test]
    async fn pv_bound_to_claim_that_points_back_is_bound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(
            &storage,
            &pv_with(
                "pv",
                Some(Some("u")),
                Some(PersistentVolumePhase::Available),
            ),
        )
        .await;
        let mut pvc = make_pvc("c", "u");
        pvc.spec.volume_name = Some("pv".into());
        put_pvc(&storage, &pvc).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Bound);
    }

    /// binder_test.go "14-13": a PV pre-bound to a claim with an incompatible
    /// volumeMode emits VolumeMismatch on BOTH objects (pv_controller.go:684-692).
    #[tokio::test]
    async fn volume_mode_mismatch_emits_events_on_pv_and_claim() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &bound_pv("pv")).await;
        let mut pvc = make_pvc("c", "u");
        pvc.spec.volume_mode = Some(PersistentVolumeMode::Block);
        put_pvc(&storage, &pvc).await;
        let queue = WorkQueue::new();
        c.sync_volumes(&queue).await.unwrap();
        let events = volume_mismatch_events(&storage).await;
        let pv_ev = events
            .iter()
            .find(|e| e.involved_object.kind.as_deref() == Some("PersistentVolume"))
            .expect("event on the PV");
        assert_eq!(
            pv_ev.message,
            "Cannot bind PersistentVolume to requested PersistentVolumeClaim \"c\" due to incompatible volumeMode."
        );
        let claim_ev = events
            .iter()
            .find(|e| e.involved_object.kind.as_deref() == Some("PersistentVolumeClaim"))
            .expect("event on the claim");
        assert_eq!(
            claim_ev.message,
            "Cannot bind PersistentVolume \"pv\" to requested PersistentVolumeClaim due to incompatible volumeMode."
        );
        // "Skipping syncClaim": the claim is NOT enqueued.
        let got = tokio::time::timeout(Duration::from_millis(100), queue.get()).await;
        assert!(got.is_err(), "claim must not be enqueued on mismatch");
    }

    /// pv_controller.go:694-707: a bound PV whose claim has no volumeName
    /// enqueues the claim so syncClaim fixes it promptly.
    #[tokio::test]
    async fn pv_with_unbound_claim_enqueues_the_claim() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &bound_pv("pv")).await;
        put_pvc(&storage, &make_pvc("c", "u")).await;
        let queue = WorkQueue::new();
        c.sync_volumes(&queue).await.unwrap();
        let key = tokio::time::timeout(Duration::from_millis(500), queue.get())
            .await
            .expect("claim enqueued");
        assert_eq!(key.as_deref(), Some("persistentvolumeclaims/sstest/c"));
        assert!(volume_mismatch_events(&storage).await.is_empty());
    }

    fn claim_bound_elsewhere() -> PersistentVolumeClaim {
        let mut pvc = make_pvc("c", "u");
        pvc.spec.volume_name = Some("other-pv".into());
        pvc
    }

    /// binder_test.go "4-7": bound by the controller to a claim bound
    /// elsewhere -> unbind fully (claimRef + annotation cleared), Available.
    #[tokio::test]
    async fn controller_bound_pv_of_claim_bound_elsewhere_is_unbound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.metadata.annotations = Some(HashMap::from([(
            ANN_BOUND_BY_CONTROLLER.to_string(),
            "yes".to_string(),
        )]));
        put_pv(&storage, &pv).await;
        put_pvc(&storage, &claim_bound_elsewhere()).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert!(got.spec.claim_ref.is_none());
        assert!(got.metadata.annotations.is_none());
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
    }

    /// binder_test.go "4-8": bound by the user -> only the UID is cleared.
    #[tokio::test]
    async fn user_bound_pv_of_claim_bound_elsewhere_keeps_prebind() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &bound_pv("pv")).await;
        put_pvc(&storage, &claim_bound_elsewhere()).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        let cr = got.spec.claim_ref.expect("still pre-bound");
        assert_eq!(cr.name.as_deref(), Some("c"));
        assert!(cr.uid.as_deref().unwrap_or("").is_empty());
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
    }

    /// binder_test.go "4-9": dynamically provisioned + Delete, claim bound
    /// elsewhere -> deleted.
    #[tokio::test]
    async fn dynamically_provisioned_delete_pv_of_claim_bound_elsewhere_is_deleted() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        pv.metadata.annotations = Some(HashMap::from([(
            ANN_DYNAMICALLY_PROVISIONED.to_string(),
            "x".to_string(),
        )]));
        put_pv(&storage, &pv).await;
        put_pvc(&storage, &claim_bound_elsewhere()).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        assert!(get_pv(&storage, "pv").await.is_none());
    }

    /// GetBindVolumeToClaim (component-helpers pv_helpers.go:120-154): a PV the
    /// controller binds (no prior claimRef) gets bound-by-controller; one
    /// pre-bound by the user does not.
    #[tokio::test]
    async fn binding_sets_bound_by_controller_only_when_not_prebound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut free = make_prebound_pv("free", "x", "x");
        free.spec.claim_ref = None;
        put_pv(&storage, &free).await;
        let mut pvc = make_pvc("c", "u");
        put_pvc(&storage, &pvc).await;
        c.bind_pvc(&mut pvc).await.unwrap();
        let got = get_pv(&storage, "free").await.unwrap();
        assert_eq!(
            got.metadata
                .annotations
                .unwrap()
                .get(ANN_BOUND_BY_CONTROLLER)
                .map(String::as_str),
            Some("yes")
        );

        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &make_prebound_pv("pre", "c", "u")).await;
        let mut pvc = make_pvc("c", "u");
        put_pvc(&storage, &pvc).await;
        c.bind_pvc(&mut pvc).await.unwrap();
        let got = get_pv(&storage, "pre").await.unwrap();
        assert!(got.metadata.annotations.is_none());
    }

    /// binder_test.go claim expectations (newClaimArray with
    /// annBoundByController + annBindCompleted): a claim the controller binds
    /// carries both annotations, persisted via the main resource.
    #[tokio::test]
    async fn binding_sets_claim_bound_by_controller_and_bind_completed() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut free = make_prebound_pv("free", "x", "x");
        free.spec.claim_ref = None;
        put_pv(&storage, &free).await;
        let mut pvc = make_pvc("c", "u");
        put_pvc(&storage, &pvc).await;
        c.bind_pvc(&mut pvc).await.unwrap();
        let key = build_key("persistentvolumeclaims", Some("sstest"), "c");
        let got: PersistentVolumeClaim = storage.get(&key).await.unwrap();
        let ann = got.metadata.annotations.unwrap();
        assert_eq!(
            ann.get(ANN_BOUND_BY_CONTROLLER).map(String::as_str),
            Some("yes")
        );
        assert_eq!(ann.get(ANN_BIND_COMPLETED).map(String::as_str), Some("yes"));
    }

    /// bindClaimToVolume: a claim already pointing at the volume (user
    /// pre-bound) is not marked bound-by-controller, only bind-completed.
    #[test]
    fn bind_claim_to_volume_skips_bound_by_controller_when_prebound() {
        let mut pvc = make_pvc("c", "u");
        pvc.spec.volume_name = Some("pv".into());
        bind_claim_to_volume(&mut pvc, "pv");
        let ann = pvc.metadata.annotations.unwrap();
        assert!(!ann.contains_key(ANN_BOUND_BY_CONTROLLER));
        assert_eq!(ann.get(ANN_BIND_COMPLETED).map(String::as_str), Some("yes"));
    }

    // ---- syncClaim bound/pre-bound branches (pv_controller.go:251, :331-490,
    // :492-560; binder_test.go sets 2 and 3) ----

    fn claim_with(
        name: &str,
        uid: &str,
        volume: Option<&str>,
        phase: PersistentVolumeClaimPhase,
        annotations: &[&str],
    ) -> PersistentVolumeClaim {
        let mut pvc = make_pvc(name, uid);
        pvc.spec.volume_name = volume.map(String::from);
        pvc.status.as_mut().unwrap().phase = phase;
        if !annotations.is_empty() {
            pvc.metadata.annotations = Some(
                annotations
                    .iter()
                    .map(|a| (a.to_string(), "yes".to_string()))
                    .collect(),
            );
        }
        pvc
    }

    fn pv_for(name: &str, claim: Option<(&str, &str)>) -> PersistentVolume {
        let mut pv = make_prebound_pv(name, "x", "x");
        pv.spec.claim_ref = claim.map(|(n, uid)| ObjectReference {
            name: Some(n.into()),
            uid: if uid.is_empty() {
                None
            } else {
                Some(uid.into())
            },
            ..pv.spec.claim_ref.clone().unwrap()
        });
        pv.status.as_mut().unwrap().phase = PersistentVolumePhase::Available;
        pv
    }

    async fn sync_claim(
        storage: &Arc<MemoryStorage>,
        pvc: PersistentVolumeClaim,
    ) -> (Result<()>, PersistentVolumeClaim) {
        put_pvc(storage, &pvc).await;
        let c = PVBinderController::new(storage.clone());
        let mut pvc = pvc;
        let r = c.bind_pvc(&mut pvc).await;
        let key = build_key(
            "persistentvolumeclaims",
            pvc.metadata.namespace.as_deref(),
            &pvc.metadata.name,
        );
        (r, storage.get(&key).await.unwrap())
    }

    async fn events_with_reason(
        storage: &Arc<MemoryStorage>,
        reason: &str,
    ) -> Vec<rusternetes_common::resources::Event> {
        let all: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        all.into_iter().filter(|e| e.reason == reason).collect()
    }

    fn phase_of(pvc: &PersistentVolumeClaim) -> PersistentVolumeClaimPhase {
        pvc.status.as_ref().unwrap().phase.clone()
    }

    /// binder_test.go "3-1".
    #[tokio::test]
    async fn bound_claim_with_missing_volume_name_is_lost() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with(
            "c",
            "u",
            None,
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BOUND_BY_CONTROLLER, ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Lost);
        let ev = events_with_reason(&storage, "ClaimLost").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "Bound claim has lost reference to PersistentVolume. Data on the volume is lost!"
        );
    }

    /// binder_test.go "3-2".
    #[tokio::test]
    async fn bound_claim_with_missing_volume_is_lost() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with(
            "c",
            "u",
            Some("gone"),
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BOUND_BY_CONTROLLER, ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Lost);
        let ev = events_with_reason(&storage, "ClaimLost").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "Bound claim has lost its PersistentVolume. Data on the volume is lost!"
        );
    }

    /// binder_test.go "3-3": volume has come unbound -> bound again.
    #[tokio::test]
    async fn bound_claim_with_unbound_volume_rebinds() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", None)).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[ANN_BOUND_BY_CONTROLLER, ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("u"));
    }

    /// binder_test.go "3-4": claimRef with a different UID -> Lost/ClaimMisbound.
    #[tokio::test]
    async fn bound_claim_with_prebound_volume_of_other_uid_is_misbound() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("c", "c-x")))).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[ANN_BOUND_BY_CONTROLLER, ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Lost);
        let ev = events_with_reason(&storage, "ClaimMisbound").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "Two claims are bound to the same volume, this one is bound incorrectly"
        );
        // The volume is left alone.
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("c-x"));
    }

    /// binder_test.go "3-5": claim and volume agree -> claim Bound.
    #[tokio::test]
    async fn bound_claim_with_bound_volume_becomes_bound() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("c", "u")))).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
        assert!(events_with_reason(&storage, "ClaimMisbound")
            .await
            .is_empty());
    }

    /// updateClaimStatusWithEvent (pv_controller.go:889-909): the event is
    /// emitted only when the phase actually changes.
    #[tokio::test]
    async fn lost_claim_event_is_emitted_once() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with(
            "c",
            "u",
            None,
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BIND_COMPLETED],
        );
        let (_, got) = sync_claim(&storage, pvc).await;
        let c = PVBinderController::new(storage.clone());
        let mut got = got;
        c.bind_pvc(&mut got).await.unwrap();
        assert_eq!(events_with_reason(&storage, "ClaimLost").await.len(), 1);
    }

    /// binder_test.go "2-1"/"2-2": claim pre-bound to a missing volume stays
    /// Pending, and a stale Bound phase is reset to Pending.
    #[tokio::test]
    async fn prebound_claim_to_missing_volume_is_reset_to_pending() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with(
            "c",
            "u",
            Some("gone"),
            PersistentVolumeClaimPhase::Bound,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        assert!(got.status.unwrap().capacity.is_none());
    }

    /// binder_test.go "2-3": claim pre-bound to an unbound volume binds it,
    /// the PV is marked bound-by-controller, the claim only bind-completed.
    #[tokio::test]
    async fn prebound_claim_to_unbound_volume_binds() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", None)).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
        let ann = got.metadata.annotations.unwrap();
        assert_eq!(ann.get(ANN_BIND_COMPLETED).map(String::as_str), Some("yes"));
        assert!(!ann.contains_key(ANN_BOUND_BY_CONTROLLER));
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("u"));
        assert!(pv
            .metadata
            .annotations
            .unwrap()
            .contains_key(ANN_BOUND_BY_CONTROLLER));
    }

    /// binder_test.go "2-4": a PV pre-bound to the claim by name (no UID) is
    /// finished: UID set, no bound-by-controller on the PV.
    #[tokio::test]
    async fn prebound_claim_to_prebound_volume_by_name_binds() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("c", "")))).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("u"));
        assert!(pv.metadata.annotations.is_none());
    }

    /// binder_test.go "2-6": volume bound to another claim, claim not bound by
    /// the controller -> reset to Pending, volume untouched.
    #[tokio::test]
    async fn prebound_claim_to_volume_of_other_claim_is_pending() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("other", "other-uid")))).await;
        let pvc = claim_with("c", "u", Some("pv"), PersistentVolumeClaimPhase::Bound, &[]);
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("other-uid"));
        assert_eq!(events_with_reason(&storage, "FailedBinding").await.len(), 1);
    }

    /// binder_test.go "2-7": same but the claim is bound-by-controller -> error.
    #[tokio::test]
    async fn controller_bound_claim_to_volume_of_other_claim_errors() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("other", "other-uid")))).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BOUND_BY_CONTROLLER],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        assert!(r.is_err());
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
    }

    /// binder_test.go "2-9": volume smaller than requested -> VolumeMismatch,
    /// claim reset to Pending, volume not bound.
    #[tokio::test]
    async fn prebound_claim_to_too_small_volume_is_mismatch() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", None)).await;
        let mut pvc = claim_with("c", "u", Some("pv"), PersistentVolumeClaimPhase::Bound, &[]);
        pvc.spec
            .resources
            .requests
            .as_mut()
            .unwrap()
            .insert("storage".into(), "2Gi".into());
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let ev = events_with_reason(&storage, "VolumeMismatch").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "Cannot bind to requested volume \"pv\": requested PV is too small"
        );
        assert!(get_pv(&storage, "pv")
            .await
            .unwrap()
            .spec
            .claim_ref
            .is_none());
    }

    /// binder_test.go "2-10": storageClassName differs.
    #[tokio::test]
    async fn prebound_claim_to_volume_of_other_class_is_mismatch() {
        let storage = Arc::new(MemoryStorage::new());
        let mut pv = pv_for("pv", None);
        pv.spec.storage_class_name = Some("gold".into());
        put_pv(&storage, &pv).await;
        let pvc = claim_with("c", "u", Some("pv"), PersistentVolumeClaimPhase::Bound, &[]);
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let ev = events_with_reason(&storage, "VolumeMismatch").await;
        assert_eq!(ev.len(), 1);
        assert!(ev[0].message.ends_with("storageClassName does not match"));
    }

    #[test]
    fn quantity_eq_compares_by_value() {
        assert!(quantity_eq("1Gi", "1024Mi"));
        assert!(quantity_eq("1.5Gi", "1536Mi"));
        assert!(quantity_eq("1G", "1000M"));
        assert!(quantity_eq("1e3", "1k"));
        assert!(quantity_eq("500m", "0.5"));
        assert!(!quantity_eq("1Gi", "1G"));
        assert_eq!(parse_quantity_nano("bogus!"), None);
        assert_eq!(parse_quantity_nano(""), None);
    }

    fn storage_cap(v: &str) -> HashMap<String, String> {
        HashMap::from([("storage".to_string(), v.to_string())])
    }

    /// binder_test.go "2-12" (VolumeAttributesClass, GA in 1.35): binding a
    /// claim to a volume with the same VAC sets
    /// `status.currentVolumeAttributesClassName` from the volume
    /// (`updateClaimStatus`, pv_controller.go:841-857).
    #[tokio::test]
    async fn binding_sets_current_vac_name_and_capacity_from_volume() {
        let storage = Arc::new(MemoryStorage::new());
        let mut pv = pv_for("pv", None);
        pv.spec.volume_attributes_class_name = Some("gold".into());
        put_pv(&storage, &pv).await;
        let mut pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        pvc.spec.volume_attributes_class_name = Some("gold".into());
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        let st = got.status.unwrap();
        assert_eq!(st.phase, PersistentVolumeClaimPhase::Bound);
        assert_eq!(
            st.current_volume_attributes_class_name.as_deref(),
            Some("gold")
        );
        assert_eq!(st.capacity, Some(storage_cap("1Gi")));
        assert_eq!(st.access_modes, Some(pv.spec.access_modes.clone()));
    }

    /// pv_controller_test.go "5-2-3": a PV with the pre-resize-capacity
    /// annotation makes the claim's status capacity that value (:826-837).
    #[tokio::test]
    async fn binding_uses_pre_resize_capacity_annotation() {
        let storage = Arc::new(MemoryStorage::new());
        let mut pv = pv_for("pv", None);
        pv.spec.capacity = storage_cap("2Gi");
        pv.metadata.annotations = Some(HashMap::from([(
            "volume.alpha.kubernetes.io/pre-resize-capacity".to_string(),
            "1Gi".to_string(),
        )]));
        put_pv(&storage, &pv).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(got.status.unwrap().capacity, Some(storage_cap("1Gi")));
    }

    /// An unparsable pre-resize annotation falls back to the volume capacity
    /// (:830-833).
    #[tokio::test]
    async fn unparsable_pre_resize_capacity_falls_back_to_volume_capacity() {
        let storage = Arc::new(MemoryStorage::new());
        let mut pv = pv_for("pv", None);
        pv.spec.capacity = storage_cap("2Gi");
        pv.metadata.annotations = Some(HashMap::from([(
            "volume.alpha.kubernetes.io/pre-resize-capacity".to_string(),
            "bogus!".to_string(),
        )]));
        put_pv(&storage, &pv).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(got.status.unwrap().capacity, Some(storage_cap("2Gi")));
    }

    /// :817-819: capacity is only copied when the phase changes; an already
    /// Bound claim keeps a status capacity that differs from the volume's (a
    /// filesystem not yet resized), and nothing is written.
    #[tokio::test]
    async fn update_claim_status_does_not_clobber_capacity_when_already_bound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = pv_for("pv", None);
        pv.spec.capacity = storage_cap("2Gi");
        let mut pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BIND_COMPLETED],
        );
        {
            let st = pvc.status.as_mut().unwrap();
            st.capacity = Some(storage_cap("1Gi"));
            st.access_modes = Some(pv.spec.access_modes.clone());
        }
        put_pvc(&storage, &pvc).await;
        c.update_claim_status(&mut pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await
            .unwrap();
        assert_eq!(
            pvc.status.as_ref().unwrap().capacity,
            Some(storage_cap("1Gi"))
        );
        assert!(pvc.status.as_ref().unwrap().access_modes.is_some());
    }

    /// `volumeCap.Cmp(claimCap) != 0` (:835): quantities are compared by
    /// value, so an equal-valued status capacity is left as it was.
    #[tokio::test]
    async fn update_claim_status_compares_capacity_by_quantity_value() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let pv = pv_for("pv", None);
        let mut pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        {
            let st = pvc.status.as_mut().unwrap();
            st.capacity = Some(storage_cap("1024Mi"));
            st.access_modes = Some(pv.spec.access_modes.clone());
        }
        put_pvc(&storage, &pvc).await;
        c.update_claim_status(&mut pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await
            .unwrap();
        let st = pvc.status.unwrap();
        assert_eq!(st.phase, PersistentVolumeClaimPhase::Bound);
        assert_eq!(st.capacity, Some(storage_cap("1024Mi")));
    }

    /// :821-824: a volume without storage capacity is an error on the phase
    /// transition.
    #[tokio::test]
    async fn update_claim_status_errors_on_volume_without_capacity() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = pv_for("pv", None);
        pv.spec.capacity = HashMap::new();
        let mut pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        put_pvc(&storage, &pvc).await;
        let e = c
            .update_claim_status(&mut pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "PersistentVolume \"pv\" is without a storage capacity"
        );
    }

    /// :841-857: currentVolumeAttributesClassName is set only on the
    /// Pending -> Bound transition, never afterwards.
    #[tokio::test]
    async fn current_vac_name_is_only_set_on_pending_to_bound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = pv_for("pv", None);
        pv.spec.volume_attributes_class_name = Some("gold".into());
        let mut pvc = claim_with("c", "u", Some("pv"), PersistentVolumeClaimPhase::Bound, &[]);
        {
            let st = pvc.status.as_mut().unwrap();
            st.capacity = Some(storage_cap("1Gi"));
            st.access_modes = Some(pv.spec.access_modes.clone());
        }
        put_pvc(&storage, &pvc).await;
        c.update_claim_status(&mut pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await
            .unwrap();
        assert_eq!(
            pvc.status.unwrap().current_volume_attributes_class_name,
            None
        );
    }

    fn claim_with_annotations(pairs: &[(&str, &str)]) -> PersistentVolumeClaim {
        let mut pvc = claim_with("c", "u", None, PersistentVolumeClaimPhase::Pending, &[]);
        pvc.metadata.annotations = Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        pvc
    }

    /// pv_controller_test.go TestAnnealMigrationAnnotations "migration off
    /// removes migrated to (rollback)" (and the Beta provisioner variant) for
    /// a claim, via `syncClaim` (`updateClaimMigrationAnnotations`,
    /// pv_controller.go:240) with the claim written back.
    #[tokio::test]
    async fn sync_claim_removes_stale_migrated_to_annotation() {
        for provisioner_key in [
            "volume.kubernetes.io/storage-provisioner",
            "volume.beta.kubernetes.io/storage-provisioner",
        ] {
            let storage = Arc::new(MemoryStorage::new());
            let pvc = claim_with_annotations(&[
                (provisioner_key, "kubernetes.io/rbd"),
                (ANN_MIGRATED_TO, "rbd.csi.ceph.com"),
            ]);
            let (r, got) = sync_claim(&storage, pvc).await;
            r.unwrap();
            let ann = got.metadata.annotations.unwrap();
            assert!(!ann.contains_key(ANN_MIGRATED_TO), "{provisioner_key}");
            assert!(ann.contains_key(provisioner_key));
        }
    }

    /// TestAnnealMigrationAnnotations "not dynamically provisioned": a claim
    /// without a provisioner annotation keeps its annotations untouched.
    #[tokio::test]
    async fn sync_claim_leaves_unprovisioned_claim_annotations_alone() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with_annotations(&[(ANN_MIGRATED_TO, "x.csi")]);
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(
            got.metadata
                .annotations
                .unwrap()
                .get(ANN_MIGRATED_TO)
                .map(String::as_str),
            Some("x.csi")
        );
    }

    // ---- syncUnboundClaim volumeName == "" branch with no match
    // (pv_controller.go:331-409) ----

    async fn put_class(storage: &Arc<MemoryStorage>, name: &str, default: bool, mode: &str) {
        let mut sc: serde_json::Value = serde_json::json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {"name": name},
            "provisioner": "example.com/none",
            "volumeBindingMode": mode,
        });
        if default {
            sc["metadata"]["annotations"] =
                serde_json::json!({"storageclass.kubernetes.io/is-default-class": "true"});
        }
        let sc: rusternetes_common::resources::StorageClass = serde_json::from_value(sc).unwrap();
        storage
            .create(&build_key("storageclasses", None, name), &sc)
            .await
            .unwrap();
    }

    async fn put_pod(storage: &Arc<MemoryStorage>, name: &str, node: Option<&str>, claim: &str) {
        let pod: rusternetes_common::resources::Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": "sstest"},
            "spec": {
                "nodeName": node,
                "containers": [{"name": "c", "image": "i"}],
                "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": claim}}]
            }
        }))
        .unwrap();
        storage
            .create(&build_key("pods", Some("sstest"), name), &pod)
            .await
            .unwrap();
    }

    fn classless_claim() -> PersistentVolumeClaim {
        let mut pvc = make_pvc("c", "u");
        pvc.spec.storage_class_name = None;
        pvc
    }

    /// `assignDefaultStorageClass` (pv_controller.go:967-994): the claim gets
    /// the default class written back and the sync stops there.
    #[tokio::test]
    async fn unbound_claim_without_class_gets_the_default_class() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "old", true, "Immediate").await;
        let (r, got) = sync_claim(&storage, classless_claim()).await;
        r.unwrap();
        assert_eq!(got.spec.storage_class_name.as_deref(), Some("old"));
        assert!(events_with_reason(&storage, "FailedBinding")
            .await
            .is_empty());
    }

    /// A claim that asks for a class (even via the beta annotation) is left alone.
    #[tokio::test]
    async fn unbound_claim_with_beta_class_annotation_is_not_defaulted() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "dflt", true, "Immediate").await;
        let mut pvc = classless_claim();
        pvc.metadata.annotations = Some(
            [(
                "volume.beta.kubernetes.io/storage-class".to_string(),
                "gold".to_string(),
            )]
            .into(),
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(got.spec.storage_class_name, None);
    }

    /// pv_controller.go:371-373: no class, no default, no volume.
    #[tokio::test]
    async fn unbound_claim_without_any_class_emits_failed_binding_and_is_pending() {
        let storage = Arc::new(MemoryStorage::new());
        let (r, got) = sync_claim(&storage, classless_claim()).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let ev = events_with_reason(&storage, "FailedBinding").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "no persistent volumes available for this claim and no storage class is set"
        );
        assert_eq!(
            ev[0].event_type,
            rusternetes_common::resources::EventType::Normal
        );
    }

    /// emitEventForUnboundDelayBindingClaim (:306-326), no pod yet.
    #[tokio::test]
    async fn delay_binding_claim_without_pod_waits_for_first_consumer() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let ev = events_with_reason(&storage, "WaitForFirstConsumer").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "waiting for first consumer to be created before binding"
        );
    }

    /// One unscheduled pod uses the claim; scheduled and terminated pods are
    /// not counted (findNonScheduledPodsByPVC, :1477-1493).
    #[tokio::test]
    async fn delay_binding_claim_names_the_unscheduled_pods() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        put_pod(&storage, "p1", None, "c").await;
        put_pod(&storage, "p-sched", Some("node-1"), "c").await;
        put_pod(&storage, "p-other", None, "other-claim").await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        let (r, _) = sync_claim(&storage, pvc.clone()).await;
        r.unwrap();
        let ev = events_with_reason(&storage, "WaitForPodScheduled").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].message, "waiting for pod p1 to be scheduled");
    }

    #[tokio::test]
    async fn delay_binding_claim_names_all_unscheduled_pods() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        put_pod(&storage, "p1", None, "c").await;
        put_pod(&storage, "p2", None, "c").await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        let (r, _) = sync_claim(&storage, pvc).await;
        r.unwrap();
        let ev = events_with_reason(&storage, "WaitForPodScheduled").await;
        assert_eq!(ev.len(), 1);
        assert!(
            ev[0].message == "waiting for pods p1,p2 to be scheduled"
                || ev[0].message == "waiting for pods p2,p1 to be scheduled"
        );
    }

    /// `IsDelayBindingProvisioning`: the scheduler has chosen a node, so the
    /// claim is provisioned instead of waiting (:361-364 falls through).
    #[tokio::test]
    async fn delay_binding_claim_with_selected_node_does_not_wait() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        pvc.metadata.annotations = Some(
            [(
                "volume.kubernetes.io/selected-node".to_string(),
                "node-1".to_string(),
            )]
            .into(),
        );
        let (r, _) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert!(events_with_reason(&storage, "WaitForFirstConsumer")
            .await
            .is_empty());
        assert!(events_with_reason(&storage, "FailedBinding")
            .await
            .is_empty());
    }

    /// An Immediate class with no volume is provisioned by the dynamic
    /// provisioner (provisionClaim, :366-374): no event from this branch.
    #[tokio::test]
    async fn immediate_class_claim_without_volume_emits_nothing() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "standard", false, "Immediate").await;
        let (r, _) = sync_claim(&storage, classless_claim().with_class("standard")).await;
        r.unwrap();
        assert!(events_with_reason(&storage, "FailedBinding")
            .await
            .is_empty());
        assert!(events_with_reason(&storage, "WaitForFirstConsumer")
            .await
            .is_empty());
    }

    trait WithClass {
        fn with_class(self, c: &str) -> Self;
    }
    impl WithClass for PersistentVolumeClaim {
        fn with_class(mut self, c: &str) -> Self {
            self.spec.storage_class_name = Some(c.into());
            self
        }
    }
}
