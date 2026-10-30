//! PV protection controller.
//!
//! Port of `pkg/controller/volume/pvprotection/pv_protection_controller.go`
//! (`processPV`, `isBeingUsed`, `pvAddedUpdated`). Removes
//! `kubernetes.io/pv-protection` from a terminating volume that is not
//! `Bound`, and adds it to a live volume that lacks it (volumes created before
//! the `StorageObjectInUseProtection` admission plugin).

use super::pvc_protection::{is_deletion_candidate, need_to_add_finalizer, update_or_reap};
use anyhow::Result;
use rusternetes_common::resources::volume::{PersistentVolumePhase, PV_PROTECTION_FINALIZER};
use rusternetes_common::resources::PersistentVolume;
use rusternetes_storage::{build_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

/// `isBeingUsed`: the volume is bound to a claim, per the status the PV
/// controller maintains.
pub(crate) fn is_being_used(pv: &PersistentVolume) -> bool {
    pv.status
        .as_ref()
        .is_some_and(|s| s.phase == PersistentVolumePhase::Bound)
}

pub struct PvProtectionController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> PvProtectionController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// `processPV` for the volume `name`.
    pub async fn process_pv(&self, name: &str) -> Result<()> {
        let key = build_key("persistentvolumes", None, name);
        let mut pv: PersistentVolume = match self.storage.get(&key).await {
            Ok(p) => p,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if is_deletion_candidate(&pv.metadata, PV_PROTECTION_FINALIZER) && !is_being_used(&pv) {
            if let Some(f) = pv.metadata.finalizers.as_mut() {
                f.retain(|x| x != PV_PROTECTION_FINALIZER);
            }
            update_or_reap(&*self.storage, &key, &pv).await?;
            info!("Removed protection finalizer from PV {name}");
            return Ok(());
        }
        if need_to_add_finalizer(&pv.metadata, PV_PROTECTION_FINALIZER) {
            pv.metadata
                .finalizers
                .get_or_insert_with(Vec::new)
                .push(PV_PROTECTION_FINALIZER.to_string());
            self.storage.update(&key, &pv).await?;
        }
        Ok(())
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        let prefix = rusternetes_storage::build_prefix("persistentvolumes", None);
        match self.storage.list::<PersistentVolume>(&prefix).await {
            Ok(pvs) => {
                for v in pvs {
                    if need_to_add_finalizer(&v.metadata, PV_PROTECTION_FINALIZER)
                        || is_deletion_candidate(&v.metadata, PV_PROTECTION_FINALIZER)
                    {
                        queue.add(v.metadata.name.clone()).await;
                    }
                }
            }
            Err(e) => error!("PV protection: failed to list PVs: {e}"),
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;
        info!("Starting PV protection controller");

        let queue = WorkQueue::new();
        let worker = self.clone();
        let wq = queue.clone();
        tokio::spawn(async move {
            while let Some(name) = wq.get().await {
                match worker.process_pv(&name).await {
                    Ok(()) => wq.forget(&name).await,
                    Err(e) => {
                        error!("PV protection: {name} failed: {e}");
                        wq.requeue_rate_limited(name.clone()).await;
                    }
                }
                wq.done(&name).await;
            }
        });

        loop {
            self.enqueue_all(&queue).await;
            let prefix = rusternetes_storage::build_prefix("persistentvolumes", None);
            let mut watch = match self.storage.watch(&prefix).await {
                Ok(w) => w,
                Err(e) => {
                    warn!("PV protection: watch failed: {e}, retrying");
                    time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = time::interval(Duration::from_secs(30));
            resync.tick().await;
            loop {
                tokio::select! {
                    ev = watch.next() => match ev {
                        Some(Ok(_)) => self.enqueue_all(&queue).await,
                        _ => break,
                    },
                    _ = resync.tick() => self.enqueue_all(&queue).await,
                }
            }
        }
    }
}
