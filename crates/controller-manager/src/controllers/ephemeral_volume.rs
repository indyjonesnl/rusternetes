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
const FAILED_BINDING: &str = "FailedBinding";

/// `ephemeral.VolumeClaimName`.
pub(crate) fn volume_claim_name(pod: &Pod, volume: &Volume) -> String {
    format!("{}-{}", pod.metadata.name, volume.name)
}

/// `ephemeral.VolumeIsForPod`: the claim must be in the pod's namespace and
/// controlled by the pod (`metav1.IsControlledBy`: controller ref with the
/// pod's UID).
pub(crate) fn volume_is_for_pod(pod: &Pod, pvc: &PersistentVolumeClaim) -> Result<()> {
    let controlled = pvc
        .metadata
        .owner_references
        .iter()
        .flatten()
        .any(|o| o.controller == Some(true) && o.uid == pod.metadata.uid);
    if pvc.metadata.namespace != pod.metadata.namespace || !controlled {
        return Err(anyhow!(
            "PVC {}/{} was not created for pod {}/{} (pod is not owner)",
            pvc.metadata.namespace.as_deref().unwrap_or(""),
            pvc.metadata.name,
            pod.metadata.namespace.as_deref().unwrap_or(""),
            pod.metadata.name
        ));
    }
    Ok(())
}

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
    async fn create_claim(&self, pod: &Pod, vol: &Volume) -> Result<()> {
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

#[cfg(test)]
mod tests {
    //! Mirrors `pkg/controller/volume/ephemeral/controller_test.go`
    //! `TestSyncHandler`.
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::json;

    fn pod(name: &str, ns: &str, uid: &str, deleting: bool, with_vol: bool) -> Pod {
        let mut meta = json!({"name": name, "namespace": ns, "uid": uid});
        if deleting {
            meta["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        }
        let volumes = if with_vol {
            json!([{"name": "myvolume", "ephemeral": {"volumeClaimTemplate": {
                "metadata": {"labels": {"l": "v"}, "annotations": {"a": "b"}},
                "spec": {"storageClassName": "sc"}}}}])
        } else {
            json!([])
        };
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": meta,
            "spec": {"containers": [], "volumes": volumes}
        }))
        .unwrap()
    }

    fn claim(name: &str, ns: &str, owner_uid: Option<&str>) -> PersistentVolumeClaim {
        let mut meta = json!({"name": name, "namespace": ns, "uid": format!("c-{name}")});
        if let Some(u) = owner_uid {
            meta["ownerReferences"] = json!([{"apiVersion": "v1", "kind": "Pod",
                "name": "test-pod", "uid": u, "controller": true}]);
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": meta, "spec": {}
        }))
        .unwrap()
    }

    async fn setup(
        pods: &[Pod],
        pvcs: &[PersistentVolumeClaim],
    ) -> (Arc<MemoryStorage>, EphemeralVolumeController<MemoryStorage>) {
        let s = Arc::new(MemoryStorage::new());
        for p in pods {
            let k = build_key("pods", p.metadata.namespace.as_deref(), &p.metadata.name);
            s.create(&k, p).await.unwrap();
        }
        for c in pvcs {
            let k = build_key(
                "persistentvolumeclaims",
                c.metadata.namespace.as_deref(),
                &c.metadata.name,
            );
            s.create(&k, c).await.unwrap();
        }
        let c = EphemeralVolumeController::new(s.clone());
        (s, c)
    }

    async fn pvcs(s: &MemoryStorage) -> Vec<PersistentVolumeClaim> {
        s.list(&build_prefix("persistentvolumeclaims", None))
            .await
            .unwrap()
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn creates_the_claim_owned_by_the_pod() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        let got = pvcs(&s).await;
        assert_eq!(got.len(), 1);
        let p = &got[0];
        assert_eq!(p.metadata.name, "test-pod-myvolume");
        assert_eq!(p.metadata.namespace.as_deref(), Some("ns"));
        let o = &p.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(
            (o.kind.as_str(), o.name.as_str(), o.uid.as_str()),
            ("Pod", "test-pod", "u1")
        );
        assert_eq!(o.controller, Some(true));
        assert_eq!(o.block_owner_deletion, Some(true));
        assert_eq!(p.metadata.labels.as_ref().unwrap()["l"], "v");
        assert_eq!(p.metadata.annotations.as_ref().unwrap()["a"], "b");
        assert_eq!(p.spec.storage_class_name.as_deref(), Some("sc"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn no_such_pod_is_a_noop() {
        let (s, c) = setup(&[], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert!(pvcs(&s).await.is_empty());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn deleted_pod_is_a_noop() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", true, true)], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert!(pvcs(&s).await.is_empty());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn pod_without_volumes_is_a_noop() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, false)], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert!(pvcs(&s).await.is_empty());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_with_other_namespace_claim() {
        let other = claim("test-pod-myvolume", "other", None);
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[other]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert_eq!(pvcs(&s).await.len(), 2);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn existing_owned_claim_is_left_alone() {
        let own = claim("test-pod-myvolume", "ns", Some("u1"));
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[own]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        let got = pvcs(&s).await;
        assert_eq!(got.len(), 1);
        assert!(got[0].metadata.labels.is_none());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn claim_owned_by_someone_else_is_an_error() {
        let foreign = claim("test-pod-myvolume", "ns", None);
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[foreign]).await;
        let err = c.sync_pod("ns", "test-pod").await.unwrap_err().to_string();
        assert!(
            err.contains("was not created for pod ns/test-pod (pod is not owner)"),
            "{err}"
        );
        assert_eq!(pvcs(&s).await.len(), 1);
        // upstream records a FailedBinding warning on the pod
        let events: Vec<serde_json::Value> =
            s.list(&build_prefix("events", Some("ns"))).await.unwrap();
        assert!(events.iter().any(|e| e["reason"] == FAILED_BINDING));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn a_claim_deleted_out_from_under_the_pod_is_recreated() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        s.delete(&build_key(
            "persistentvolumeclaims",
            Some("ns"),
            "test-pod-myvolume",
        ))
        .await
        .unwrap();
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert_eq!(pvcs(&s).await.len(), 1);
    }

    // controller_test.go TestSyncHandler "create": one create attempt, no failure.
    #[tokio::test]
    #[serial_test::serial]
    async fn metrics_count_a_successful_create() {
        let (_s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[]).await;
        let (a, f) = (
            ephemeral_volume_metrics::create_attempts(),
            ephemeral_volume_metrics::create_failures(),
        );
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert_eq!(ephemeral_volume_metrics::create_attempts(), a + 1);
        assert_eq!(ephemeral_volume_metrics::create_failures(), f);
        assert!(
            ephemeral_volume_metrics::gather().contains("ephemeral_volume_controller_create_total")
        );
    }

    // Existing claim: no create call, so no metric.
    #[tokio::test]
    #[serial_test::serial]
    async fn metrics_ignore_an_existing_claim() {
        let own = claim("test-pod-myvolume", "ns", Some("u1"));
        let (_s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[own]).await;
        let (a, f) = (
            ephemeral_volume_metrics::create_attempts(),
            ephemeral_volume_metrics::create_failures(),
        );
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert_eq!(ephemeral_volume_metrics::create_attempts(), a);
        assert_eq!(ephemeral_volume_metrics::create_failures(), f);
    }

    // "create with failure": a Create that errors counts an attempt AND a
    // failure. The claim appears between the read and the create (the
    // lister-lag race), so the create is refused with AlreadyExists.
    #[tokio::test]
    #[serial_test::serial]
    async fn metrics_count_a_failed_create() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[]).await;
        let p = pod("test-pod", "ns", "u1", false, true);
        let vol = p.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        let key = build_key("persistentvolumeclaims", Some("ns"), "test-pod-myvolume");
        s.create(&key, &claim("test-pod-myvolume", "ns", None))
            .await
            .unwrap();
        let (a, f) = (
            ephemeral_volume_metrics::create_attempts(),
            ephemeral_volume_metrics::create_failures(),
        );
        let err = c.create_claim(&p, &vol).await.unwrap_err().to_string();
        assert!(err.contains("create PVC test-pod-myvolume"), "{err}");
        assert_eq!(ephemeral_volume_metrics::create_attempts(), a + 1);
        assert_eq!(ephemeral_volume_metrics::create_failures(), f + 1);
    }
}
