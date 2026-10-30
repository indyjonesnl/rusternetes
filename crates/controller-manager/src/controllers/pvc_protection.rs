//! PVC protection controller.
//!
//! Port of `pkg/controller/volume/pvcprotection/pvc_protection_controller.go`
//! (`processPVC`, `isBeingUsed`, `askAPIServer`, `podUsesPVC`, `podIsShutDown`,
//! `pvcAddedUpdated`) and `pkg/controller/volume/protectionutil/utils.go`
//! (`IsDeletionCandidate`, `NeedToAddFinalizer`).
//!
//! Removes `kubernetes.io/pvc-protection` from a terminating claim that no
//! scheduled pod uses, and adds it to a live claim that lacks it (claims
//! created before the `StorageObjectInUseProtection` admission plugin).
//!
//! Deviations: there is no informer cache here, so only upstream's "live list"
//! path (`askAPIServer`) is used, per claim (upstream's `LazyLivePodList` batch
//! cache is an API-call optimisation). Pod events carry only a key, so a pod
//! event re-queues the terminating claims of that pod's namespace rather than
//! the claims the pod mounts (`enqueuePVCs`).

use anyhow::Result;
use rusternetes_common::resources::volume::PVC_PROTECTION_FINALIZER;
use rusternetes_common::resources::{PersistentVolumeClaim, Pod};
use rusternetes_common::types::ObjectMeta;
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// `protectionutil.IsDeletionCandidate`.
pub fn is_deletion_candidate(meta: &ObjectMeta, finalizer: &str) -> bool {
    meta.deletion_timestamp.is_some() && has_finalizer(meta, finalizer)
}

/// `protectionutil.NeedToAddFinalizer`.
pub fn need_to_add_finalizer(meta: &ObjectMeta, finalizer: &str) -> bool {
    meta.deletion_timestamp.is_none() && !has_finalizer(meta, finalizer)
}

/// Persist `obj` after a finalizer change. A terminating object whose last
/// finalizer is gone is deleted here: upstream PATCHes/Updates and the
/// apiserver reaps it, but this controller writes to storage directly (the
/// same shortcut `servicecidr::remove_finalizer_if_needed` takes).
pub async fn update_or_reap<S: Storage, T>(storage: &S, key: &str, obj: &T) -> Result<()>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    let meta = serde_json::to_value(obj)?;
    let finalizers_left = meta["metadata"]["finalizers"]
        .as_array()
        .is_some_and(|f| !f.is_empty());
    if !finalizers_left && !meta["metadata"]["deletionTimestamp"].is_null() {
        match storage.delete(key).await {
            Ok(()) | Err(rusternetes_common::Error::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
    } else {
        storage.update(key, obj).await?;
    }
    Ok(())
}

fn has_finalizer(meta: &ObjectMeta, finalizer: &str) -> bool {
    meta.finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|x| x == finalizer))
}

/// `podIsShutDown`: kubelet is done with the pod or it was force-deleted.
pub fn pod_is_shut_down(pod: &Pod) -> bool {
    pod.metadata.deletion_timestamp.is_some()
        && pod.metadata.deletion_grace_period_seconds == Some(0)
}

/// `podUsesPVC`.
pub fn pod_uses_pvc(pod: &Pod, pvc: &PersistentVolumeClaim) -> bool {
    let Some(spec) = pod.spec.as_ref() else {
        return false;
    };
    // Only a scheduled pod: kubelet won't start a pod whose claim is
    // terminating, so an unscheduled pod cannot be using it.
    if spec.node_name.as_deref().unwrap_or("").is_empty() {
        return false;
    }
    spec.volumes.iter().flatten().any(|v| {
        if v.persistent_volume_claim
            .as_ref()
            .is_some_and(|p| p.claim_name == pvc.metadata.name)
        {
            return true;
        }
        // ephemeral.VolumeClaimName == pod.Name + "-" + volume.Name, and
        // ephemeral.VolumeIsForPod == the claim is controlled by the pod
        // (component-helpers/storage/ephemeral/ephemeral.go:41-57).
        !pod_is_shut_down(pod)
            && v.ephemeral.is_some()
            && format!("{}-{}", pod.metadata.name, v.name) == pvc.metadata.name
            && pvc.metadata.namespace == pod.metadata.namespace
            && pvc
                .metadata
                .owner_references
                .iter()
                .flatten()
                .any(|o| o.controller == Some(true) && o.uid == pod.metadata.uid)
    })
}

pub struct PvcProtectionController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> PvcProtectionController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// `processPVC` for the claim `namespace/name`.
    pub async fn process_pvc(&self, namespace: &str, name: &str) -> Result<()> {
        let key = build_key("persistentvolumeclaims", Some(namespace), name);
        let mut pvc: PersistentVolumeClaim = match self.storage.get(&key).await {
            Ok(p) => p,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if is_deletion_candidate(&pvc.metadata, PVC_PROTECTION_FINALIZER) {
            // PVC should be deleted. Check if it's used and remove the
            // finalizer if it's not (`isBeingUsed` -> `askAPIServer`).
            if !self.is_being_used(&pvc).await? {
                if let Some(f) = pvc.metadata.finalizers.as_mut() {
                    f.retain(|x| x != PVC_PROTECTION_FINALIZER);
                }
                update_or_reap(&*self.storage, &key, &pvc).await?;
                info!("Removed protection finalizer from PVC {namespace}/{name}");
                return Ok(());
            }
            debug!("Keeping PVC {namespace}/{name} because it is being used");
        }
        if need_to_add_finalizer(&pvc.metadata, PVC_PROTECTION_FINALIZER) {
            // Finalizer is normally added by admission; this covers PVCs that
            // predate the plugin.
            pvc.metadata
                .finalizers
                .get_or_insert_with(Vec::new)
                .push(PVC_PROTECTION_FINALIZER.to_string());
            self.storage.update(&key, &pvc).await?;
            debug!("Added protection finalizer to PVC {namespace}/{name}");
        }
        Ok(())
    }

    /// `askAPIServer`: a live list of the namespace's pods.
    async fn is_being_used(&self, pvc: &PersistentVolumeClaim) -> Result<bool> {
        let ns = pvc.metadata.namespace.as_deref().unwrap_or("");
        let prefix = rusternetes_storage::build_prefix("pods", Some(ns));
        let pods: Vec<Pod> = self.storage.list(&prefix).await?;
        Ok(pods.iter().any(|p| pod_uses_pvc(p, pvc)))
    }

    /// Queue every claim that needs work (`pvcAddedUpdated`'s filter),
    /// optionally limited to one namespace.
    async fn enqueue(&self, queue: &WorkQueue, namespace: Option<&str>) {
        let prefix = rusternetes_storage::build_prefix("persistentvolumeclaims", namespace);
        match self.storage.list::<PersistentVolumeClaim>(&prefix).await {
            Ok(pvcs) => {
                for c in pvcs {
                    if need_to_add_finalizer(&c.metadata, PVC_PROTECTION_FINALIZER)
                        || is_deletion_candidate(&c.metadata, PVC_PROTECTION_FINALIZER)
                    {
                        let ns = c.metadata.namespace.as_deref().unwrap_or("");
                        queue
                            .add(format!("persistentvolumeclaims/{ns}/{}", c.metadata.name))
                            .await;
                    }
                }
            }
            Err(e) => error!("PVC protection: failed to list PVCs: {e}"),
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;
        info!("Starting PVC protection controller");

        let queue = WorkQueue::new();
        let worker = self.clone();
        let wq = queue.clone();
        tokio::spawn(async move {
            while let Some(key) = wq.get().await {
                let parts: Vec<&str> = key.splitn(3, '/').collect();
                if parts.len() == 3 {
                    match worker.process_pvc(parts[1], parts[2]).await {
                        Ok(()) => wq.forget(&key).await,
                        Err(e) => {
                            error!("PVC protection: {key} failed: {e}");
                            wq.requeue_rate_limited(key.clone()).await;
                        }
                    }
                } else {
                    wq.forget(&key).await;
                }
                wq.done(&key).await;
            }
        });

        loop {
            self.enqueue(&queue, None).await;
            let pvc_watch = self
                .storage
                .watch(&rusternetes_storage::build_prefix(
                    "persistentvolumeclaims",
                    None,
                ))
                .await;
            let pod_watch = self
                .storage
                .watch(&rusternetes_storage::build_prefix("pods", None))
                .await;
            let (mut pvc_watch, mut pod_watch) = match (pvc_watch, pod_watch) {
                (Ok(a), Ok(b)) => (a, b),
                _ => {
                    warn!("PVC protection: watch failed, retrying");
                    time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = time::interval(Duration::from_secs(30));
            resync.tick().await;
            loop {
                // A PVC or pod event can only change the claims of its own
                // namespace (`pvcAddedUpdated`, `podAddedDeletedUpdated`).
                let ev = tokio::select! {
                    ev = pvc_watch.next() => Some(ev),
                    ev = pod_watch.next() => Some(ev),
                    _ = resync.tick() => None,
                };
                match ev {
                    None => self.enqueue(&queue, None).await,
                    Some(Some(Ok(ev))) => {
                        let k = extract_key(&ev);
                        let mut p = k.splitn(3, '/');
                        if let (Some(_), Some(ns)) = (p.next(), p.next()) {
                            self.enqueue(&queue, Some(ns)).await;
                        }
                    }
                    Some(_) => break,
                }
            }
        }
    }
}
