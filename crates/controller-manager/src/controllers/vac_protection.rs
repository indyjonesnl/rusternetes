//! VolumeAttributesClass protection controller.
//!
//! Port of `pkg/controller/volume/vacprotection/vac_protection_controller.go`
//! (`processVAC`, `isBeingUsed`, `vacAddedUpdated`, `getPVCReferencedVACNames`,
//! `getPVReferencedVACNames`) and `protectionutil.IsDeletionCandidate` /
//! `NeedToAddFinalizer`.
//!
//! Removes `kubernetes.io/vac-protection` from a terminating class that no PV
//! (`spec.volumeAttributesClassName`) and no PVC (`spec.volumeAttributesClassName`,
//! `status.currentVolumeAttributesClassName`,
//! `status.modifyVolumeStatus.targetVolumeAttributesClassName`) references, and
//! adds it to a live class that lacks it.
//!
//! Deviations: there are no informer indexers here, so the "who references this
//! class" lookup (`getPVsAssignedToVAC` / `getPVCsAssignedToVAC`) lists PVs and
//! PVCs from storage; any VAC/PV/PVC event re-queues every class that needs
//! work (a superset of upstream's per-name `queue.Add`).

use super::pvc_protection::{is_deletion_candidate, need_to_add_finalizer, update_or_reap};
use anyhow::Result;
use rusternetes_common::resources::csi::VolumeAttributesClass;
use rusternetes_common::resources::volume::VAC_PROTECTION_FINALIZER;
use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim};
use rusternetes_storage::{build_key, build_prefix, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

/// `getPVCReferencedVACNames`.
pub(crate) fn pvc_referenced_vac_names(pvc: &PersistentVolumeClaim) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let status = pvc.status.as_ref();
    for n in [
        pvc.spec.volume_attributes_class_name.as_deref(),
        status.and_then(|s| s.current_volume_attributes_class_name.as_deref()),
        status
            .and_then(|s| s.modify_volume_status.as_ref())
            .and_then(|m| m.target_volume_attributes_class_name.as_deref()),
    ]
    .into_iter()
    .flatten()
    {
        if !n.is_empty() && !names.iter().any(|x| x == n) {
            names.push(n.to_string());
        }
    }
    names
}

/// `getPVReferencedVACNames`.
pub(crate) fn pv_referenced_vac_names(pv: &PersistentVolume) -> Vec<String> {
    match pv.spec.volume_attributes_class_name.as_deref() {
        Some(n) if !n.is_empty() => vec![n.to_string()],
        _ => vec![],
    }
}

pub struct VacProtectionController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> VacProtectionController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// `isBeingUsed`: any PV or PVC references the class.
    async fn is_being_used(&self, vac_name: &str) -> Result<bool> {
        let pvs: Vec<PersistentVolume> = self
            .storage
            .list(&build_prefix("persistentvolumes", None))
            .await?;
        if pvs
            .iter()
            .any(|pv| pv_referenced_vac_names(pv).iter().any(|n| n == vac_name))
        {
            return Ok(true);
        }
        let pvcs: Vec<PersistentVolumeClaim> = self
            .storage
            .list(&build_prefix("persistentvolumeclaims", None))
            .await?;
        Ok(pvcs
            .iter()
            .any(|c| pvc_referenced_vac_names(c).iter().any(|n| n == vac_name)))
    }

    /// `processVAC` for the class `name`.
    pub async fn process_vac(&self, name: &str) -> Result<()> {
        let key = build_key("volumeattributesclasses", None, name);
        let mut vac: VolumeAttributesClass = match self.storage.get(&key).await {
            Ok(v) => v,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        // A used class is kept: falls through, where NeedToAddFinalizer is
        // false for a terminating object.
        if is_deletion_candidate(&vac.metadata, VAC_PROTECTION_FINALIZER)
            && !self.is_being_used(name).await?
        {
            if let Some(f) = vac.metadata.finalizers.as_mut() {
                f.retain(|x| x != VAC_PROTECTION_FINALIZER);
            }
            update_or_reap(&*self.storage, &key, &vac).await?;
            info!("Removed protection finalizer from VAC {name}");
            return Ok(());
        }
        if need_to_add_finalizer(&vac.metadata, VAC_PROTECTION_FINALIZER) {
            vac.metadata
                .finalizers
                .get_or_insert_with(Vec::new)
                .push(VAC_PROTECTION_FINALIZER.to_string());
            self.storage.update(&key, &vac).await?;
            info!("Added protection finalizer to VAC {name}");
        }
        Ok(())
    }

    /// `vacAddedUpdated` for every class.
    async fn enqueue_all(&self, queue: &WorkQueue) {
        let prefix = build_prefix("volumeattributesclasses", None);
        match self.storage.list::<VolumeAttributesClass>(&prefix).await {
            Ok(vacs) => {
                for v in vacs {
                    if need_to_add_finalizer(&v.metadata, VAC_PROTECTION_FINALIZER)
                        || is_deletion_candidate(&v.metadata, VAC_PROTECTION_FINALIZER)
                    {
                        queue.add(v.metadata.name.clone()).await;
                    }
                }
            }
            Err(e) => error!("VAC protection: failed to list VACs: {e}"),
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;
        info!("Starting VAC protection controller");

        let queue = WorkQueue::new();
        let worker = self.clone();
        let wq = queue.clone();
        tokio::spawn(async move {
            while let Some(name) = wq.get().await {
                match worker.process_vac(&name).await {
                    Ok(()) => wq.forget(&name).await,
                    Err(e) => {
                        error!("VAC protection: {name} failed: {e}");
                        wq.requeue_rate_limited(name.clone()).await;
                    }
                }
                wq.done(&name).await;
            }
        });

        loop {
            self.enqueue_all(&queue).await;
            let mut watches = Vec::new();
            for rt in [
                "volumeattributesclasses",
                "persistentvolumes",
                "persistentvolumeclaims",
            ] {
                match self.storage.watch(&build_prefix(rt, None)).await {
                    Ok(w) => watches.push(w),
                    Err(e) => {
                        warn!("VAC protection: watch on {rt} failed: {e}, retrying");
                        break;
                    }
                }
            }
            if watches.len() != 3 {
                time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            let mut merged = futures::stream::select_all(watches);
            let mut resync = time::interval(Duration::from_secs(30));
            resync.tick().await;
            loop {
                tokio::select! {
                    ev = merged.next() => match ev {
                        Some(Ok(_)) => self.enqueue_all(&queue).await,
                        _ => break,
                    },
                    _ = resync.tick() => self.enqueue_all(&queue).await,
                }
            }
        }
    }
}
