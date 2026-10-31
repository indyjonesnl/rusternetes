//! Generic ephemeral volume controller.
//!
//! Port of `pkg/controller/volume/ephemeral/controller.go`
//! (`syncHandler`, `handleVolume`, `enqueuePod`, `onPVCDelete`) and
//! `staging/src/k8s.io/component-helpers/storage/ephemeral/ephemeral.go`
//! (`VolumeClaimName`, `VolumeIsForPod`).
//!
//! For every pod that has a `volumes[].ephemeral` source, creates the
//! stand-alone PVC `<pod>-<volume>` from the `volumeClaimTemplate`, owned
//! (controller + blockOwnerDeletion) by the pod so the garbage collector
//! removes it with the pod. A PVC of that name that is not controlled by the
//! pod is an error (and a `FailedBinding` event on the pod).
//!
//! Deviations: no informer cache, so the pod/PVC "lister" is a storage read
//! per volume. Upstream ignores pod updates (the pod spec is immutable) and
//! only re-enqueues on pod add and PVC delete; here every pod/PVC event
//! re-enqueues the pods of that namespace, which is safe because
//! `handleVolume` is idempotent. The metrics
//! (`ephemeral_volume_controller_create_total` / `_create_failures_total`)
//! live in [`super::ephemeral_volume_metrics`].

use super::ephemeral_volume_metrics;
use anyhow::{anyhow, Result};
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::PersistentVolumeClaim;
use rusternetes_common::resources::{EventSource, EventType, Pod, Volume};
use rusternetes_common::types::{ObjectMeta, OwnerReference, TypeMeta};
use rusternetes_common::Error;
use rusternetes_storage::{
    build_key, build_prefix, extract_key, EventRecorder, Storage, WorkQueue,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// `events.FailedBinding` (`pkg/controller/volume/events/event.go:21`).
pub const FAILED_BINDING: &str = "FailedBinding";

pub use rusternetes_common::ephemeral::{volume_claim_name, volume_is_for_pod};

fn has_ephemeral_volume(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .is_some_and(|s| s.volumes.iter().flatten().any(|v| v.ephemeral.is_some()))
}

pub struct EphemeralVolumeController<S: Storage> {
    storage: Arc<S>,
    recorder: EventRecorder<S>,
}

impl<S: Storage + 'static> EphemeralVolumeController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
        }
    }

    /// `syncHandler` for the pod `namespace/name`.
    pub async fn sync_pod(&self, namespace: &str, name: &str) -> Result<()> {
        let key = build_key("pods", Some(namespace), name);
        let pod: Pod = match self.storage.get(&key).await {
            Ok(p) => p,
            // "nothing to do for pod, it is gone"
            Err(Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        // "nothing to do for pod, it is marked for deletion"
        if pod.metadata.deletion_timestamp.is_some() {
            return Ok(());
        }
        let Some(spec) = pod.spec.as_ref() else {
            return Ok(());
        };
        for vol in spec.volumes.iter().flatten() {
            if let Err(e) = self.handle_volume(&pod, vol).await {
                self.record_pod_event(
                    &pod,
                    FAILED_BINDING,
                    &format!("ephemeral volume {}: {e}", vol.name),
                )
                .await;
                return Err(anyhow!(
                    "pod {namespace}/{name}, ephemeral volume {}: {e}",
                    vol.name
                ));
            }
        }
        Ok(())
    }

    /// `handleVolume`.
    async fn handle_volume(&self, pod: &Pod, vol: &Volume) -> Result<()> {
        if vol.ephemeral.is_none() {
            return Ok(());
        }
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("");
        let pvc_name = volume_claim_name(pod, vol);
        let key = build_key("persistentvolumeclaims", Some(namespace), &pvc_name);
        match self.storage.get::<PersistentVolumeClaim>(&key).await {
            Ok(pvc) => {
                volume_is_for_pod(pod, &pvc)?;
                debug!("Ephemeral: PVC {namespace}/{pvc_name} already created");
                return Ok(());
            }
            Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }

        self.create_claim(pod, vol).await
    }

    /// The create half of `handleVolume` (`controller.go:271-301`).
    pub async fn create_claim(&self, pod: &Pod, vol: &Volume) -> Result<()> {
        let Some(eph) = vol.ephemeral.as_ref() else {
            return Ok(());
        };
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("");
        let pvc_name = volume_claim_name(pod, vol);
        let key = build_key("persistentvolumeclaims", Some(namespace), &pvc_name);
        // Create the PVC with pod as owner. A pod without a template cannot
        // pass validation; there is nothing to copy.
        let template = eph
            .volume_claim_template
            .as_ref()
            .ok_or_else(|| anyhow!("ephemeral volume {} has no volumeClaimTemplate", vol.name))?;
        let mut metadata = ObjectMeta::new(&pvc_name).with_namespace(namespace);
        metadata.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            name: pod.metadata.name.clone(),
            uid: pod.metadata.uid.clone(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        if let Some(tm) = template.metadata.as_ref() {
            metadata.annotations = tm.annotations.clone();
            metadata.labels = tm.labels.clone();
        }
        let pvc = PersistentVolumeClaim {
            type_meta: TypeMeta {
                kind: "PersistentVolumeClaim".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            spec: template.spec.clone(),
            status: None,
        };
        // controller.go:295-300: count the attempt before the call, the
        // failure after it.
        ephemeral_volume_metrics::inc_create_attempts();
        if let Err(e) = self.storage.create(&key, &pvc).await {
            ephemeral_volume_metrics::inc_create_failures();
            return Err(anyhow!("create PVC {pvc_name}: {e}"));
        }
        info!(
            "Created ephemeral PVC {namespace}/{pvc_name} for pod {}",
            pod.metadata.name
        );
        Ok(())
    }

    async fn record_pod_event(&self, pod: &Pod, reason: &str, message: &str) {
        let involved = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: pod.metadata.namespace.clone(),
            name: Some(pod.metadata.name.clone()),
            uid: Some(pod.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            ..Default::default()
        };
        let source = EventSource {
            component: "ephemeral_volume".to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(&involved, &source, EventType::Warning, reason, message)
            .await
        {
            warn!("failed to record {reason} event: {e}");
        }
    }

    /// `enqueuePod` over the pods that have an ephemeral volume, optionally
    /// limited to one namespace. Pods being deleted are skipped as upstream
    /// does.
    async fn enqueue(&self, queue: &WorkQueue, namespace: Option<&str>) {
        let prefix = build_prefix("pods", namespace);
        match self.storage.list::<Pod>(&prefix).await {
            Ok(pods) => {
                for p in pods {
                    if p.metadata.deletion_timestamp.is_none() && has_ephemeral_volume(&p) {
                        let ns = p.metadata.namespace.as_deref().unwrap_or("");
                        queue.add(format!("pods/{ns}/{}", p.metadata.name)).await;
                    }
                }
            }
            Err(e) => error!("Ephemeral volume: failed to list pods: {e}"),
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;
        info!("Starting ephemeral volume controller");

        let queue = WorkQueue::new();
        let worker = self.clone();
        let wq = queue.clone();
        tokio::spawn(async move {
            while let Some(key) = wq.get().await {
                let parts: Vec<&str> = key.splitn(3, '/').collect();
                if parts.len() == 3 {
                    match worker.sync_pod(parts[1], parts[2]).await {
                        Ok(()) => wq.forget(&key).await,
                        Err(e) => {
                            error!("Ephemeral volume: {key} failed: {e}");
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
            let pod_watch = self.storage.watch(&build_prefix("pods", None)).await;
            let pvc_watch = self
                .storage
                .watch(&build_prefix("persistentvolumeclaims", None))
                .await;
            let (mut pod_watch, mut pvc_watch) = match (pod_watch, pvc_watch) {
                (Ok(a), Ok(b)) => (a, b),
                _ => {
                    warn!("Ephemeral volume: watch failed, retrying");
                    time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = time::interval(Duration::from_secs(30));
            resync.tick().await;
            loop {
                let ev = tokio::select! {
                    ev = pod_watch.next() => Some(ev),
                    ev = pvc_watch.next() => Some(ev),
                    _ = resync.tick() => None,
                };
                match ev {
                    None => self.enqueue(&queue, None).await,
                    Some(Some(Ok(ev))) => {
                        // A pod add or a PVC delete can only affect the pods of
                        // its own namespace (`enqueuePod`, `onPVCDelete`).
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
