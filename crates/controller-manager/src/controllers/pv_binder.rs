use anyhow::Result;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::{
    NodeSelectorTerm, PersistentVolumeClaimPhase, PersistentVolumeClaimStatus,
    PersistentVolumeMode, PersistentVolumePhase, PersistentVolumeReclaimPolicy, VolumeNodeAffinity,
};
use rusternetes_common::resources::{
    EventSource, EventType, Node, PersistentVolume, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PersistentVolumeSpec, PersistentVolumeStatus,
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
    /// Not ported (see issue #2185 follow-up):
    /// `updateVolumeMigrationAnnotationsAndFinalizers` (`:565`; in-tree to CSI
    /// migration, which Rusternetes has no plugins for).
    async fn sync_volume(&self, pv: PersistentVolume, claim_queue: &WorkQueue) -> Result<()> {
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
        mut pv: PersistentVolume,
        phase: PersistentVolumePhase,
    ) -> Result<PersistentVolume> {
        if pv.status.as_ref().map(|s| &s.phase) == Some(&phase) {
            return Ok(pv);
        }
        let mut status = pv.status.take().unwrap_or_default();
        status.phase = phase.clone();
        status.message = None;
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

    /// `reclaimVolume` (`pv_controller.go:1180-1230`). `Retain` does nothing;
    /// `Delete` removes the PV. `Recycle` is deprecated and its recycler
    /// plugins (`recycleVolumeOperation`) are not ported, so it leaves the
    /// volume `Released` like `Retain`. A PV carrying the
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

    /// Port of upstream `syncClaim` dispatch (`pv_controller.go:251-255`): a
    /// claim without `pv.kubernetes.io/bind-completed` goes to
    /// `syncUnboundClaim`, one with it to `syncBoundClaim`.
    ///
    /// Not ported here (tracked separately): `updateClaimMigrationAnnotations`
    /// (`:240`, CSI migration), `assignDefaultStorageClass`,
    /// `provisionClaim`/delay-binding handling in the `volumeName == ""`
    /// branch, and `unbindClaim`.
    async fn bind_pvc(&self, pvc: &mut PersistentVolumeClaim) -> Result<()> {
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

    /// `updateClaimStatus` (`pv_controller.go:784-882`), the `volume == nil`
    /// reset form: set the phase and clear accessModes/capacity/
    /// currentVolumeAttributesClassName; a no-op when nothing changes.
    /// Written through the status subresource.
    async fn update_claim_status(
        &self,
        pvc: &mut PersistentVolumeClaim,
        phase: PersistentVolumeClaimPhase,
        _volume: Option<&PersistentVolume>,
    ) -> Result<()> {
        let mut status = pvc.status.clone().unwrap_or_default();
        let mut dirty = pvc.status.is_none() || status.phase != phase;
        status.phase = phase;
        dirty |= status.access_modes.take().is_some();
        dirty |= status.capacity.take().is_some();
        dirty |= status.current_volume_attributes_class_name.take().is_some();
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
        Ok(())
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

        let pv_access_modes = pv.spec.access_modes.clone();
        let pv_capacity = pv.spec.capacity.clone();
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
        pvc.status = Some(PersistentVolumeClaimStatus {
            phase: PersistentVolumeClaimPhase::Bound,
            access_modes: Some(pv_access_modes),
            capacity: Some(pv_capacity),
            conditions: None,
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        });
        let pvc_key = build_key("persistentvolumeclaims", Some(&namespace), &pvc_name);
        // Same two-write split for the claim: `spec.volumeName` via the main
        // resource, phase/capacity via the status subresource
        // (pv_controller.go:866).
        self.storage.update(&pvc_key, pvc).await?;
        self.storage.update_status(&pvc_key, pvc).await?;

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
}
