use crate::controllers::worker_pool::spawn_workers;
use anyhow::{Context, Result};
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::PersistentVolumeClaimPhase;
use rusternetes_common::resources::{EventSource, EventType};
use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim};
use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

/// Workers draining this controller's queue: `defaultWorkerCount = 10`
/// (pkg/controller/volume/expand/expand_controller.go:58), launched in `Run` (:341-345).
const CONCURRENT_VOLUME_EXPAND_SYNCS: usize = 10;

/// `util.AnnPreResizeCapacity` (`pkg/volume/util/resize_util.go:52`).
const ANN_PRE_RESIZE_CAPACITY: &str = "volume.alpha.kubernetes.io/pre-resize-capacity";

/// `volumetypes.VolumeResizerKey` (`pkg/volume/util/types/types.go:209`).
const VOLUME_RESIZER_KEY: &str = "volume.kubernetes.io/storage-resizer";

/// `CSITranslator.GetInTreePluginNameFromSpec` for a PV
/// (`staging/src/k8s.io/csi-translation-lib/translate.go:146-153`): the first
/// `inTreePlugins` entry whose `CanSupport(pv)` holds, i.e. the plugin whose
/// source the PV carries (`gce_pd.go` `CanSupport`: `GCEPersistentDisk != nil`;
/// `aws_ebs.go`: `AWSElasticBlockStore != nil`; `openstack_cinder.go:162`:
/// `Cinder`; `azure_disk.go:235`: `AzureDisk`; `azure_file.go:235`: `AzureFile`;
/// `vsphere_volume.go:204`: `VsphereVolume`; `portworx.go:189`:
/// `PortworxVolume`). A PV carries one source, so the map-iteration order of
/// `inTreePlugins` (`translate.go:30-42`) is immaterial.
fn in_tree_plugin_name_from_pv(pv: &PersistentVolume) -> Option<&'static str> {
    let s = &pv.spec;
    let m = &s.more_sources;
    if s.gce_persistent_disk.is_some() {
        Some("kubernetes.io/gce-pd")
    } else if s.aws_elastic_block_store.is_some() {
        Some("kubernetes.io/aws-ebs")
    } else if m.cinder.is_some() {
        Some("kubernetes.io/cinder")
    } else if m.azure_disk.is_some() {
        Some("kubernetes.io/azure-disk")
    } else if m.azure_file.is_some() {
        Some("kubernetes.io/azure-file")
    } else if m.vsphere_volume.is_some() {
        Some("kubernetes.io/vsphere-volume")
    } else if m.portworx_volume.is_some() {
        Some("kubernetes.io/portworx-volume")
    } else {
        None
    }
}
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
    pub async fn reconcile_pvc(&self, pvc: &PersistentVolumeClaim) -> Result<()> {
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

        // `csiMigratedPluginManager.IsMigratable(volumeSpec)` (:238-243;
        // `plugin_manager.go:111-123`): a PV whose in-tree plugin has CSI
        // migration enabled is handed to the external resizer (:245-269).
        if let Some(plugin) = in_tree_plugin_name_from_pv(&pv)
            .filter(|p| crate::controllers::pv_binder::is_migration_enabled_for_plugin(p))
        {
            self.record_event(
                pvc,
                EventType::Normal,
                &format!(
                    "CSI migration enabled for {plugin}; waiting for external resizer to expand the pvc"
                ),
            )
            .await;
            // `GetCSINameFromInTreeName` (:252-257).
            let Some(resizer) = crate::controllers::pv_binder::csi_name_from_in_tree_name(plugin)
            else {
                let msg = format!("error getting CSI driver name for pvc {key}, with error could not find CSI Driver name for plugin {plugin}");
                self.record_event(pvc, EventType::Warning, &msg).await;
                anyhow::bail!(msg);
            };
            // `util.SetClaimResizer` (:259-264).
            if let Err(e) = self.set_claim_resizer(pvc, resizer).await {
                let msg = format!("error setting resizer annotation to pvc {key}, with error {e}");
                self.record_event(pvc, EventType::Warning, &msg).await;
                anyhow::bail!(msg);
            }
            return Ok(());
        }

        // `FindExpandablePluginBySpec` (:271-283) finds an expandable plugin only for
        // in-tree volume types that implement `ExpandableVolumePlugin`; rusternetes
        // has none (hostPath/nfs/iscsi/local never did upstream either), and a CSI
        // volume is expanded by the external-resizer sidecar calling
        // ControllerExpandVolume (and the kubelet NodeExpandVolume). So every other PV
        // lands on the `volumePlugin == nil` branch: record ExternalExpanding and
        // return nil without requeueing (the "for csi plugin without migration
        // path" case of TestSyncHandler, expand_controller_test.go:84-91). Writing
        // PV/PVC capacity here would bypass the CSI contract. The StorageClass is
        // never read: allowVolumeExpansion is enforced by PVC admission.
        self.record_event(
            pvc,
            EventType::Normal,
            "waiting for an external controller to expand this PVC",
        )
        .await;
        Ok(())
    }

    /// `util.SetClaimResizer` (`pkg/volume/util/resize_util.go:167-185`):
    /// `PatchPVCStatus` (:325-340) of `volume.kubernetes.io/storage-resizer`
    /// guarded by the resourceVersion (`createPVCPatch` addResourceVersionCheck,
    /// :360-365); a no-op when the annotation already holds `resizer` (:178-180).
    async fn set_claim_resizer(&self, pvc: &PersistentVolumeClaim, resizer: &str) -> Result<()> {
        if pvc
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(VOLUME_RESIZER_KEY))
            .is_some_and(|v| v == resizer)
        {
            return Ok(());
        }
        let patch = serde_json::json!({"metadata": {
            "annotations": {VOLUME_RESIZER_KEY: resizer},
            "resourceVersion": pvc.metadata.resource_version,
        }});
        let mut full = pvc.clone();
        // The REST scope's kind picks the patch schema (strategicPatchObject);
        // a PVC read back from storage may carry no TypeMeta.
        full.type_meta = rusternetes_common::types::TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        };
        full.metadata
            .annotations
            .get_or_insert_with(Default::default)
            .insert(VOLUME_RESIZER_KEY.to_string(), resizer.to_string());
        let key = build_key(
            "persistentvolumeclaims",
            pvc.metadata.namespace.as_deref(),
            &pvc.metadata.name,
        );
        self.storage
            .patch_status_strategic_merge::<PersistentVolumeClaim>(&key, &patch, &full)
            .await?;
        Ok(())
    }

    /// `expc.recorder.Event(pvc, eventType, events.ExternalExpanding, msg)`
    /// (expand_controller.go:247, :255, :262, :271).
    async fn record_event(&self, pvc: &PersistentVolumeClaim, type_: EventType, msg: &str) {
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
            .event(&involved, &source, type_, "ExternalExpanding", msg)
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
}
