use crate::controllers::worker_pool::spawn_workers;
use anyhow::{Context, Result};
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::{
    PersistentVolumeClaimPhase, PersistentVolumeClaimResizeStatus,
};
use rusternetes_common::resources::{EventSource, EventType};
use rusternetes_common::resources::{
    PersistentVolume, PersistentVolumeClaim, PersistentVolumeClaimStatus, StorageClass,
};
use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

/// Workers draining this controller's queue: `defaultWorkerCount = 10`
/// (pkg/controller/volume/expand/expand_controller.go:58), launched in `Run` (:341-345).
const CONCURRENT_VOLUME_EXPAND_SYNCS: usize = 10;

/// `util.AnnPreResizeCapacity` (`pkg/volume/util/resize_util.go:52`).
const ANN_PRE_RESIZE_CAPACITY: &str = "volume.alpha.kubernetes.io/pre-resize-capacity";

pub struct VolumeExpansionController<S: Storage> {
    storage: Arc<S>,
    /// `expc.recorder` (expand_controller.go).
    recorder: EventRecorder<S>,
}

impl<S: Storage + 'static> VolumeExpansionController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        let recorder = EventRecorder::new(Arc::clone(&storage));
        Self { storage, recorder }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting Volume Expansion Controller");

        let queue = WorkQueue::new();

        // N workers drain ONE shared queue; a key in flight is never handed to
        // a second worker (`wait.UntilWithContext(ctx, worker, time.Second)` x N).
        spawn_workers(CONCURRENT_VOLUME_EXPAND_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
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
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
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
                Ok(resource) => match self.reconcile_pvc(&resource).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<PersistentVolumeClaim>("/registry/persistentvolumeclaims/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("persistentvolumeclaims/{}/{}", ns, item.metadata.name)
                    };
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

        for pvc in pvcs {
            if let Err(e) = self.reconcile_pvc(&pvc).await {
                error!(
                    "Failed to reconcile PVC {}/{}: {}",
                    pvc.metadata.namespace.as_deref().unwrap_or("default"),
                    pvc.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    /// `syncHandler` (pkg/controller/volume/expand/expand_controller.go:202-287).
    async fn reconcile_pvc(&self, pvc: &PersistentVolumeClaim) -> Result<()> {
        let pvc_name = &pvc.metadata.name;
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("default");
        let key = format!("{namespace}/{pvc_name}");

        // Only process bound PVCs (the informer's update filter, :135-160).
        if pvc.status.as_ref().map(|s| &s.phase) != Some(&PersistentVolumeClaimPhase::Bound) {
            return Ok(());
        }

        // `getPersistentVolume` (:354-363): Get(volumeName) fails for "".
        let volume_name = pvc.spec.volume_name.as_deref().unwrap_or("");
        let pv: PersistentVolume = self
            .storage
            .get(&build_key("persistentvolumes", None, volume_name))
            .await
            .with_context(|| format!("failed to get PV {volume_name:?}"))?;

        // :222-226
        let claim_ref = pv.spec.claim_ref.as_ref();
        if claim_ref.is_none_or(|r| {
            r.namespace.as_deref().unwrap_or("") != namespace
                || r.uid.as_deref().unwrap_or("") != pvc.metadata.uid
        }) {
            anyhow::bail!("persistent Volume is not bound to PVC being updated : {key}");
        }

        if !self.needs_expansion(pvc, &pv)? {
            return Ok(());
        }

        // A CSI volume is expanded by the external-resizer sidecar calling
        // ControllerExpandVolume (and the kubelet NodeExpandVolume), never by this
        // controller: `FindExpandablePluginBySpec` finds no plugin for it and the
        // controller only records ExternalExpanding and returns nil (:262-273; the
        // "for csi plugin without migration path" case of TestSyncHandler,
        // expand_controller_test.go:84-91). Writing PV/PVC capacity here would
        // bypass the CSI contract.
        if pv.spec.csi.is_some() {
            self.record_external_expanding(pvc).await;
            return Ok(());
        }

        info!("PVC {}/{} needs expansion", namespace, pvc_name);

        // Get the storage class
        let storage_class_name = pvc
            .spec
            .storage_class_name
            .as_ref()
            .context("PVC has no storage class name")?;

        let sc_key = build_key("storageclasses", None, storage_class_name);
        let storage_class: StorageClass = self
            .storage
            .get(&sc_key)
            .await
            .with_context(|| format!("StorageClass {} not found", storage_class_name))?;

        // Check if volume expansion is allowed
        if !storage_class.allow_volume_expansion.unwrap_or(false) {
            warn!(
                "Volume expansion not allowed for StorageClass {}. PVC {}/{} cannot be expanded.",
                storage_class_name, namespace, pvc_name
            );
            return Ok(());
        }

        // Perform the expansion
        self.expand_volume(pvc, &storage_class).await?;

        Ok(())
    }

    /// `expc.recorder.Event(pvc, EventTypeNormal, events.ExternalExpanding, msg)`
    /// (expand_controller.go:262-271).
    async fn record_external_expanding(&self, pvc: &PersistentVolumeClaim) {
        let involved = ObjectReference {
            kind: Some("PersistentVolumeClaim".to_string()),
            namespace: pvc.metadata.namespace.clone(),
            name: Some(pvc.metadata.name.clone()),
            uid: Some(pvc.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            ..Default::default()
        };
        let source = EventSource {
            component: "volume_expand".to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(
                &involved,
                &source,
                EventType::Normal,
                "ExternalExpanding",
                "waiting for an external controller to expand this PVC",
            )
            .await
        {
            warn!("failed to record ExternalExpanding event: {e}");
        }
    }

    /// The gate at expand_controller.go:228-236: expand only when the request
    /// exceeds `status.capacity` (a `resource.Quantity.Cmp`, so 1Gi == 1024Mi) or the PV
    /// carries `util.AnnPreResizeCapacity`. An absent quantity is the zero Quantity.
    fn needs_expansion(&self, pvc: &PersistentVolumeClaim, pv: &PersistentVolume) -> Result<bool> {
        let zero = Quantity::parse("0").expect("0 is a quantity");
        let parse = |s: Option<&String>| -> Result<Quantity> {
            match s {
                Some(s) => Quantity::parse(s).map_err(|e| anyhow::anyhow!("{s:?}: {e}")),
                None => Ok(zero),
            }
        };
        let request = parse(
            pvc.spec
                .resources
                .requests
                .as_ref()
                .and_then(|r| r.get("storage")),
        )?;
        let current = parse(
            pvc.status
                .as_ref()
                .and_then(|s| s.capacity.as_ref())
                .and_then(|c| c.get("storage")),
        )?;
        let has_pre_resize = pv
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_PRE_RESIZE_CAPACITY));
        Ok(request.cmp_value(&current) == std::cmp::Ordering::Greater || has_pre_resize)
    }

    /// Expand a PVC to the requested size
    async fn expand_volume(
        &self,
        pvc: &PersistentVolumeClaim,
        storage_class: &StorageClass,
    ) -> Result<()> {
        let pvc_name = &pvc.metadata.name;
        let namespace = pvc.metadata.namespace.as_deref().unwrap_or("default");

        let requested_storage = pvc
            .spec
            .resources
            .requests
            .as_ref()
            .and_then(|r| r.get("storage"))
            .context("PVC has no storage request")?;

        info!(
            "Expanding PVC {}/{} to {}",
            namespace, pvc_name, requested_storage
        );

        // Get the bound PV
        let pv_name = pvc
            .spec
            .volume_name
            .as_ref()
            .context("PVC has no volume name")?;

        let pv_key = build_key("persistentvolumes", None, pv_name);
        let mut pv: PersistentVolume = self
            .storage
            .get(&pv_key)
            .await
            .with_context(|| format!("PV {} not found", pv_name))?;

        // Update PVC status to indicate resize is in progress
        let mut updated_pvc = pvc.clone();
        let mut status = updated_pvc
            .status
            .clone()
            .unwrap_or(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Bound,
                access_modes: None,
                capacity: None,
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            });

        // Set allocated resources to the new requested size
        let mut allocated = HashMap::new();
        allocated.insert("storage".to_string(), requested_storage.clone());
        status.allocated_resources = Some(allocated);
        status.resize_status = Some(PersistentVolumeClaimResizeStatus::ControllerResizeInProgress);

        updated_pvc.status = Some(status.clone());

        let pvc_key = build_key("persistentvolumeclaims", Some(namespace), pvc_name);
        // Status subresource write: a full-object PUT strips `.status` (#1723).
        self.storage.update_status(&pvc_key, &updated_pvc).await?;

        info!(
            "Updated PVC {}/{} status to ControllerResizeInProgress",
            namespace, pvc_name
        );

        // Perform the actual expansion on the PV
        // For hostpath volumes, this is immediate
        // For CSI volumes, this would call the CSI driver
        match self
            .resize_pv(&mut pv, requested_storage, storage_class)
            .await
        {
            Ok(_) => {
                info!(
                    "Successfully resized PV {} to {}",
                    pv_name, requested_storage
                );

                // Update PVC status to indicate resize is complete
                status.capacity = Some({
                    let mut capacity = HashMap::new();
                    capacity.insert("storage".to_string(), requested_storage.clone());
                    capacity
                });
                status.resize_status = None; // Clear resize status when complete
                updated_pvc.status = Some(status);

                // Status subresource write: a full-object PUT strips `.status` (#1723).
                self.storage.update_status(&pvc_key, &updated_pvc).await?;

                info!("Expansion completed for PVC {}/{}", namespace, pvc_name);
                Ok(())
            }
            Err(e) => {
                error!("Failed to resize PV {}: {}", pv_name, e);

                // Update PVC status to indicate resize failed
                status.resize_status =
                    Some(PersistentVolumeClaimResizeStatus::ControllerResizeFailed);
                updated_pvc.status = Some(status);

                // Status subresource write: a full-object PUT strips `.status` (#1723).
                self.storage.update_status(&pvc_key, &updated_pvc).await?;

                Err(e)
            }
        }
    }

    /// Resize the underlying PersistentVolume
    async fn resize_pv(
        &self,
        pv: &mut PersistentVolume,
        new_size: &str,
        _storage_class: &StorageClass,
    ) -> Result<()> {
        let pv_name = &pv.metadata.name;

        info!("Resizing PV {} to {}", pv_name, new_size);

        // Update PV capacity
        pv.spec
            .capacity
            .insert("storage".to_string(), new_size.to_string());

        // In a real implementation, this would:
        // 1. For CSI volumes: Call CSI ControllerExpandVolume
        // 2. For hostpath: Adjust quota or filesystem size
        // 3. For cloud volumes: Resize the underlying disk

        // For now, we'll just update the capacity in etcd
        let pv_key = build_key("persistentvolumes", None, pv_name);
        self.storage.update(&pv_key, pv).await?;

        info!("Updated PV {} capacity to {}", pv_name, new_size);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::Event;
    use rusternetes_storage::memory::MemoryStorage;

    // Port of `TestSyncHandler` (pkg/controller/volume/expand/expand_controller_test.go:48-157)
    // and its fixtures `getFakePersistentVolume` / `getFakePersistentVolumeClaim` (:175-).

    fn pv(name: &str, driver: Option<&str>, size: &str, claim_uid: &str) -> PersistentVolume {
        let mut spec = serde_json::json!({
            "capacity": {"storage": size},
            "claimRef": {"namespace": "default", "uid": claim_uid},
        });
        match driver {
            Some(d) => spec["csi"] = serde_json::json!({"driver": d, "volumeHandle": "h"}),
            None => spec["hostPath"] = serde_json::json!({"path": "/tmp/x"}),
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name}, "spec": spec,
        }))
        .unwrap()
    }

    fn pvc(
        name: &str,
        volume: &str,
        status: &str,
        request: &str,
        uid: &str,
    ) -> PersistentVolumeClaim {
        let mut v = serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "default", "uid": uid},
            "spec": {"storageClassName": "sc", "resources": {"requests": {"storage": request}}},
            "status": {"phase": "Bound", "capacity": {"storage": status}},
        });
        if !volume.is_empty() {
            v["spec"]["volumeName"] = volume.into();
        }
        serde_json::from_value(v).unwrap()
    }

    async fn setup(
        pv_: Option<PersistentVolume>,
        pvc_: &PersistentVolumeClaim,
    ) -> (Arc<MemoryStorage>, VolumeExpansionController<MemoryStorage>) {
        let storage = Arc::new(MemoryStorage::new());
        if let Some(pv_) = pv_ {
            storage
                .create(
                    &build_key("persistentvolumes", None, &pv_.metadata.name),
                    &pv_,
                )
                .await
                .unwrap();
        }
        storage
            .create(
                &build_key(
                    "persistentvolumeclaims",
                    Some("default"),
                    &pvc_.metadata.name,
                ),
                pvc_,
            )
            .await
            .unwrap();
        let c = VolumeExpansionController::new(Arc::clone(&storage));
        (storage, c)
    }

    async fn events(storage: &Arc<MemoryStorage>) -> Vec<Event> {
        storage
            .list::<Event>("/registry/events/default/")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn pvc_with_no_pv_binding_is_an_error() {
        // "when pvc has no PV binding": hasError (expand_controller_test.go:58-63);
        // upstream looks the PV up BEFORE comparing sizes (expand_controller.go:215-220).
        let c1 = pvc("no-pv-pvc", "", "1Gi", "1Gi", "u1");
        let (_s, c) = setup(None, &c1).await;
        assert!(c.reconcile_pvc(&c1).await.is_err());
    }

    #[tokio::test]
    async fn pv_not_bound_to_this_pvc_is_an_error() {
        // `pv.Spec.ClaimRef == nil || pvc.Namespace != ... || pvc.UID != ...`
        // -> "persistent Volume is not bound to PVC being updated" (:222-226).
        let c1 = pvc("p", "vol", "1Gi", "2Gi", "uid-mine");
        let (_s, c) = setup(
            Some(pv("vol", Some("com.csi.ceph"), "1Gi", "uid-other")),
            &c1,
        )
        .await;
        assert!(c.reconcile_pvc(&c1).await.is_err());

        let mut unbound = pv("vol", Some("com.csi.ceph"), "1Gi", "uid-mine");
        unbound.spec.claim_ref = None;
        let (_s, c) = setup(Some(unbound), &c1).await;
        assert!(c.reconcile_pvc(&c1).await.is_err());
    }

    #[tokio::test]
    async fn csi_pv_is_not_resized_in_controller() {
        // "for csi plugin without migration path": expansionCalled false, no error
        // (:84-91). No expandable in-tree plugin -> ExternalExpanding event and the
        // external resizer does the work (expand_controller.go:262-273).
        let c1 = pvc("ceph-csi-pvc", "vol-5", "1Gi", "2Gi", "u5");
        let (s, c) = setup(Some(pv("vol-5", Some("com.csi.ceph"), "1Gi", "u5")), &c1).await;
        c.reconcile_pvc(&c1).await.unwrap();

        let stored_pv: PersistentVolume = s
            .get(&build_key("persistentvolumes", None, "vol-5"))
            .await
            .unwrap();
        assert_eq!(
            stored_pv.spec.capacity["storage"], "1Gi",
            "CSI PV must not be resized here"
        );
        let stored: PersistentVolumeClaim = s
            .get(&build_key(
                "persistentvolumeclaims",
                Some("default"),
                "ceph-csi-pvc",
            ))
            .await
            .unwrap();
        let st = stored.status.unwrap();
        assert_eq!(st.capacity.unwrap()["storage"], "1Gi");
        assert!(st.resize_status.is_none() && st.allocated_resources.is_none());
        let evs = events(&s).await;
        assert_eq!(evs.len(), 1, "{evs:?}");
        assert_eq!(evs[0].reason, "ExternalExpanding");
        assert_eq!(
            evs[0].message,
            "waiting for an external controller to expand this PVC"
        );
    }

    #[tokio::test]
    async fn pre_resize_annotation_triggers_sync_even_when_sizes_match() {
        // "if pv has pre-resize capacity annotation" (:72-83): the early return at
        // :228-236 is skipped, so the CSI PV still reaches ExternalExpanding.
        let c1 = pvc("p", "vol-4", "2Gi", "2Gi", "u4");
        let mut p = pv("vol-4", Some("com.csi.ceph"), "2Gi", "u4");
        p.metadata.annotations = Some(HashMap::from([(
            "volume.alpha.kubernetes.io/pre-resize-capacity".to_string(),
            "1Gi".to_string(),
        )]));
        let (s, c) = setup(Some(p), &c1).await;
        c.reconcile_pvc(&c1).await.unwrap();
        assert_eq!(events(&s).await.len(), 1);
    }

    #[tokio::test]
    async fn equal_sizes_in_different_units_do_not_expand() {
        // `pvcRequestSize.Cmp(pvcStatusSize) <= 0` is resource.Quantity (:235): 1Gi == 1024Mi.
        let c1 = pvc("p", "vol", "1024Mi", "1Gi", "u");
        let (s, c) = setup(Some(pv("vol", Some("com.csi.ceph"), "1Gi", "u")), &c1).await;
        c.reconcile_pvc(&c1).await.unwrap();
        assert!(events(&s).await.is_empty());
    }

    #[tokio::test]
    async fn larger_request_in_different_units_expands() {
        // 2Gi > 1500Mi, 1Gi > 1G: the old string compare returned false on unit mismatch.
        for (status, request) in [("1500Mi", "2Gi"), ("1G", "1Gi")] {
            let c1 = pvc("p", "vol", status, request, "u");
            let (s, c) = setup(Some(pv("vol", Some("com.csi.ceph"), "1Gi", "u")), &c1).await;
            c.reconcile_pvc(&c1).await.unwrap();
            assert_eq!(events(&s).await.len(), 1, "{status} -> {request}");
        }
    }

    #[tokio::test]
    async fn hostpath_pv_is_not_resized_in_controller() {
        // No expandable in-tree plugin exists for hostPath, so
        // `FindExpandablePluginBySpec` returns nil and the controller only emits
        // ExternalExpanding and returns nil (expand_controller.go:271-283). It must
        // not write PV capacity or PVC status.
        let c1 = pvc("p", "vol-hp", "5Gi", "10Gi", "u-hp");
        let (s, c) = setup(Some(pv("vol-hp", None, "5Gi", "u-hp")), &c1).await;
        c.reconcile_pvc(&c1).await.unwrap();

        let stored_pv: PersistentVolume = s
            .get(&build_key("persistentvolumes", None, "vol-hp"))
            .await
            .unwrap();
        assert_eq!(stored_pv.spec.capacity["storage"], "5Gi");
        let stored: PersistentVolumeClaim = s
            .get(&build_key("persistentvolumeclaims", Some("default"), "p"))
            .await
            .unwrap();
        let st = stored.status.unwrap();
        assert_eq!(st.capacity.unwrap()["storage"], "5Gi");
        assert!(st.resize_status.is_none() && st.allocated_resources.is_none());
        let evs = events(&s).await;
        assert_eq!(evs.len(), 1, "{evs:?}");
        assert_eq!(evs[0].reason, "ExternalExpanding");
        assert_eq!(
            evs[0].message,
            "waiting for an external controller to expand this PVC"
        );
    }

    #[tokio::test]
    async fn storage_class_is_not_consulted() {
        // upstream never reads the StorageClass in syncHandler; allowVolumeExpansion
        // is enforced by the PVC admission plugin. A missing class is not an error.
        let c1 = pvc("p", "vol", "1Gi", "2Gi", "u");
        let (s, c) = setup(Some(pv("vol", None, "1Gi", "u")), &c1).await;
        c.reconcile_all().await.unwrap();
        assert_eq!(events(&s).await.len(), 1);
    }
}
