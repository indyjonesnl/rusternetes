use anyhow::{Context, Result};
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::PersistentVolumeClaimPhase;
use rusternetes_common::resources::volume::VolumeSnapshotContentSource;
use rusternetes_common::resources::{
    DeletionPolicy, EventSource, EventType, PersistentVolume, PersistentVolumeClaim,
    VolumeSnapshot, VolumeSnapshotClass, VolumeSnapshotContent, VolumeSnapshotContentSpec,
    VolumeSnapshotContentStatus, VolumeSnapshotStatus,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{
    build_key, build_prefix, extract_key, EventRecorder, Storage, WorkQueue,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// `IsDefaultSnapshotClassAnnotation` (external-snapshotter
/// `pkg/utils/util.go:94`).
const IS_DEFAULT_SNAPSHOT_CLASS_ANNOTATION: &str =
    "snapshot.storage.kubernetes.io/is-default-class";

pub struct VolumeSnapshotController<S: Storage> {
    storage: Arc<S>,
    recorder: EventRecorder<S>,
}

impl<S: Storage + 'static> VolumeSnapshotController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting Volume Snapshot Controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("volumesnapshots", None);
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
            let storage_key = build_key("volumesnapshots", Some(ns), name);
            match self.storage.get::<VolumeSnapshot>(&storage_key).await {
                Ok(snapshot) => {
                    // Only process snapshots that don't have a bound content yet
                    if snapshot
                        .status
                        .as_ref()
                        .and_then(|s| s.bound_volume_snapshot_content_name.as_ref())
                        .is_none()
                    {
                        if let Err(e) = self.create_snapshot(&snapshot).await {
                            error!("Failed to create snapshot for {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        } else {
                            queue.forget(&key).await;
                        }
                    } else {
                        queue.forget(&key).await;
                    }
                    // Also reconcile snapshot deletions
                    let _ = self.reconcile_deletions().await;
                }
                Err(_) => {
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<VolumeSnapshot>("/registry/volumesnapshots/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("volumesnapshots/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list volumesnapshots for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        // Get all VolumeSnapshots
        let snapshots: Vec<VolumeSnapshot> =
            self.storage.list("/registry/volumesnapshots/").await?;

        for snapshot in snapshots {
            // Only process snapshots that don't have a bound content yet
            if snapshot
                .status
                .as_ref()
                .and_then(|s| s.bound_volume_snapshot_content_name.as_ref())
                .is_none()
            {
                if let Err(e) = self.create_snapshot(&snapshot).await {
                    error!(
                        "Failed to create snapshot for VolumeSnapshot {}/{}: {}",
                        snapshot.metadata.namespace.as_deref().unwrap_or("default"),
                        snapshot.metadata.name,
                        e
                    );
                }
            }
        }

        // Also reconcile snapshot deletions
        self.reconcile_deletions().await?;

        Ok(())
    }

    async fn create_snapshot(&self, vs: &VolumeSnapshot) -> Result<()> {
        let vs_name = &vs.metadata.name;
        let namespace = vs.metadata.namespace.as_deref().unwrap_or("default");

        debug!("Processing VolumeSnapshot {}/{}", namespace, vs_name);

        // Get the VolumeSnapshotClass. `volumeSnapshotClassName` is optional in
        // the external-snapshotter CRD: nil means "use the default
        // VolumeSnapshotClass for this driver", which `SetDefaultSnapshotClass`
        // resolves from the source PVC's PV
        // (`pkg/common-controller/snapshot_controller.go:1466-1526`).
        // `checkAndUpdateSnapshotClass`
        // (`pkg/common-controller/snapshot_controller_base.go:503-521`) is the
        // caller: it emits a `SetDefaultSnapshotClassFailed` warning and gives
        // up on this pass when the lookup fails (#2027).
        let vsc_name = match vs.spec.volume_snapshot_class_name.as_deref() {
            Some(name) => name.to_string(),
            None => match self.set_default_snapshot_class(vs).await {
                Ok(Some(name)) => name,
                // Pre-provisioned: the content names its own driver, so there
                // is no class to find and nothing for this path to do.
                Ok(None) => return Ok(()),
                Err(e) => {
                    let message = format!("Failed to set default snapshot class with error {e}");
                    warn!("VolumeSnapshot {}/{}: {}", namespace, vs_name, message);
                    self.emit(vs, "SetDefaultSnapshotClassFailed", &message)
                        .await;
                    return Err(e);
                }
            },
        };
        let vsc_name = vsc_name.as_str();
        let vsc_key = build_key("volumesnapshotclasses", None, vsc_name);
        let vsc: VolumeSnapshotClass = self
            .storage
            .get(&vsc_key)
            .await
            .with_context(|| format!("VolumeSnapshotClass {} not found", vsc_name))?;

        debug!(
            "Found VolumeSnapshotClass {} with driver {}",
            vsc_name, vsc.driver
        );

        // Check if driver is supported
        if !self.is_driver_supported(&vsc.driver) {
            warn!(
                "Driver {} is not supported. Skipping VolumeSnapshot {}/{}",
                vsc.driver, namespace, vs_name
            );
            return Ok(());
        }

        // Get the source PVC
        let pvc_name = vs
            .spec
            .source
            .persistent_volume_claim_name
            .as_ref()
            .context("VolumeSnapshot source must specify persistentVolumeClaimName")?;

        let pvc_key = build_key("persistentvolumeclaims", Some(namespace), pvc_name);
        let pvc: PersistentVolumeClaim = self
            .storage
            .get(&pvc_key)
            .await
            .with_context(|| format!("PVC {}/{} not found", namespace, pvc_name))?;

        // Ensure PVC is bound
        let pv_name = pvc
            .spec
            .volume_name
            .as_ref()
            .context("PVC must be bound to a PV before taking a snapshot")?;

        info!(
            "Creating snapshot of PVC {}/{} (bound to PV {})",
            namespace, pvc_name, pv_name
        );

        // Create VolumeSnapshotContent name
        let content_name = format!("snapcontent-{}-{}", namespace, vs_name);

        // Check if content already exists
        let content_key = build_key("volumesnapshotcontents", None, &content_name);
        if let Ok(_existing_content) = self
            .storage
            .get::<VolumeSnapshotContent>(&content_key)
            .await
        {
            debug!("VolumeSnapshotContent {} already exists", content_name);
            // Update VolumeSnapshot status if needed
            self.update_snapshot_status(vs, &content_name, true).await?;
            return Ok(());
        }

        // Create the snapshot content
        let content = self.create_snapshot_content(&vsc, vs, &content_name, pv_name)?;

        // Store the content
        self.storage
            .create(&content_key, &content)
            .await
            .with_context(|| format!("Failed to create VolumeSnapshotContent {}", content_name))?;

        info!(
            "Successfully created VolumeSnapshotContent {} for VolumeSnapshot {}/{}",
            content_name, namespace, vs_name
        );

        // Update VolumeSnapshot status
        self.update_snapshot_status(vs, &content_name, true).await?;

        Ok(())
    }

    /// `SetDefaultSnapshotClass` (external-snapshotter
    /// `pkg/common-controller/snapshot_controller.go:1466-1526`).
    ///
    /// ```go
    /// for _, class := range list {
    ///     if utils.IsVolumeSnapshotClassDefaultAnnotation(class.ObjectMeta) && pvDriver == class.Driver {
    ///         defaultClasses = append(defaultClasses, class)
    ///     }
    /// }
    /// if len(defaultClasses) == 0 {
    ///     return nil, snapshot, fmt.Errorf("cannot find default snapshot class")
    /// }
    /// if len(defaultClasses) > 1 {
    ///     return nil, snapshot, fmt.Errorf("%d default snapshot classes were found", len(defaultClasses))
    /// }
    /// ```
    ///
    /// "Default" is per **CSI driver**, not per cluster, which is why the
    /// driver has to be resolved from the source PVC's PV first: a cluster may
    /// carry one default class per driver and they do not conflict.
    ///
    /// Upstream then patches `/spec/volumeSnapshotClassName` onto the snapshot,
    /// so the choice is recorded on the object rather than recomputed every
    /// pass; this does the same. `Ok(None)` is upstream's pre-provisioned
    /// no-op.
    ///
    /// Not ported: the `IsVolumeGroupSnapshotMember` short-circuit (`:1475`) —
    /// Rusternetes has no VolumeGroupSnapshot.
    async fn set_default_snapshot_class(&self, vs: &VolumeSnapshot) -> Result<Option<String>> {
        if vs.spec.source.volume_snapshot_content_name.is_some() {
            return Ok(None);
        }

        let pv_driver = self.pv_driver_from_snapshot(vs).await?;

        let classes: Vec<VolumeSnapshotClass> = self
            .storage
            .list(&build_prefix("volumesnapshotclasses", None))
            .await
            .context("failed to list VolumeSnapshotClasses")?;

        let defaults: Vec<&VolumeSnapshotClass> = classes
            .iter()
            .filter(|class| {
                class
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(IS_DEFAULT_SNAPSHOT_CLASS_ANNOTATION))
                    .map(String::as_str)
                    == Some("true")
                    && class.driver == pv_driver
            })
            .collect();

        match defaults.len() {
            0 => anyhow::bail!("cannot find default snapshot class"),
            1 => {}
            n => anyhow::bail!("{n} default snapshot classes were found"),
        }
        let chosen = defaults[0].metadata.name.clone();

        // Upstream's `PatchVolumeSnapshot` with a single `replace` op on
        // `/spec/volumeSnapshotClassName` (`:1517-1524`).
        let namespace = vs.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key("volumesnapshots", Some(namespace), &vs.metadata.name);
        let mut patched = vs.clone();
        patched.spec.volume_snapshot_class_name = Some(chosen.clone());
        self.storage
            .update(&key, &patched)
            .await
            .with_context(|| format!("failed to record default class on {key}"))?;

        info!(
            "VolumeSnapshot {}/{}: default VolumeSnapshotClass {} selected for driver {}",
            namespace, vs.metadata.name, chosen, pv_driver
        );
        Ok(Some(chosen))
    }

    /// `pvDriverFromSnapshot` (`snapshot_controller.go:1394-1408`) together with
    /// `getVolumeFromVolumeSnapshot` (`:1350-1376`) and
    /// `getClaimFromVolumeSnapshot` (`:1528-1545`), which it is only ever called
    /// through. Every error string is upstream's: they reach the user in the
    /// `SetDefaultSnapshotClassFailed` event.
    async fn pv_driver_from_snapshot(&self, vs: &VolumeSnapshot) -> Result<String> {
        let namespace = vs.metadata.namespace.as_deref().unwrap_or("default");
        let vs_name = &vs.metadata.name;

        let Some(pvc_name) = vs.spec.source.persistent_volume_claim_name.as_deref() else {
            anyhow::bail!("the snapshot source PVC name is not specified");
        };
        if pvc_name.is_empty() {
            anyhow::bail!("the PVC name is not specified in snapshot {namespace}/{vs_name}");
        }

        let pvc: PersistentVolumeClaim = self
            .storage
            .get(&build_key(
                "persistentvolumeclaims",
                Some(namespace),
                pvc_name,
            ))
            .await
            .with_context(|| format!("failed to retrieve PVC {namespace}/{pvc_name}"))?;

        // `getVolumeFromVolumeSnapshot`: the PVC must be Bound before its PV
        // can be read (`:1356-1358`).
        if pvc.status.as_ref().map(|s| s.phase.clone()) != Some(PersistentVolumeClaimPhase::Bound) {
            anyhow::bail!(
                "the PVC {pvc_name} is not yet bound to a PV, will not attempt to take a snapshot"
            );
        }
        let pv_name = pvc.spec.volume_name.as_deref().unwrap_or("");
        let pv: PersistentVolume = self
            .storage
            .get(&build_key("persistentvolumes", None, pv_name))
            .await
            .with_context(|| format!("failed to retrieve PV {pv_name} from the API server"))?;

        // `isVolumeBoundToClaim` (`:1381-1392`): the binding must hold in both
        // directions, or the snapshot would be taken of someone else's volume.
        let claim_ref = pv.spec.claim_ref.as_ref();
        let bound = claim_ref.is_some_and(|r| {
            r.name.as_deref() == Some(pvc.metadata.name.as_str())
                && r.namespace.as_deref() == Some(namespace)
                && r.uid
                    .as_deref()
                    .is_none_or(|uid| uid.is_empty() || uid == pvc.metadata.uid)
        });
        if !bound {
            warn!(
                "binding between PV {} and PVC {} is broken",
                pv_name, pvc.metadata.name
            );
            anyhow::bail!("claim in dataSource not bound or invalid");
        }

        // "supports ONLY CSI volumes" (`:1403-1406`).
        let Some(csi) = pv.spec.csi.as_ref() else {
            anyhow::bail!(
                "snapshotting non-CSI volumes is not supported, snapshot:{namespace}/{vs_name}"
            );
        };
        Ok(csi.driver.clone())
    }

    /// `updateSnapshotErrorStatusWithEvent`'s event half
    /// (`snapshot_controller_base.go:518`). A failure to record the event is
    /// logged and dropped: it must not mask the error it describes.
    async fn emit(&self, vs: &VolumeSnapshot, reason: &str, message: &str) {
        let involved = ObjectReference {
            kind: Some("VolumeSnapshot".to_string()),
            namespace: vs.metadata.namespace.clone(),
            name: Some(vs.metadata.name.clone()),
            uid: Some(vs.metadata.uid.clone()),
            api_version: Some("snapshot.storage.k8s.io/v1".to_string()),
            ..Default::default()
        };
        let source = EventSource {
            component: "snapshot-controller".to_string(),
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

    fn is_driver_supported(&self, driver: &str) -> bool {
        // For now, we only support our own hostpath snapshotter
        matches!(
            driver,
            "rusternetes.io/hostpath-snapshotter" | "hostpath-snapshotter"
        )
    }

    fn create_snapshot_content(
        &self,
        vsc: &VolumeSnapshotClass,
        vs: &VolumeSnapshot,
        content_name: &str,
        volume_handle: &str,
    ) -> Result<VolumeSnapshotContent> {
        let namespace = vs.metadata.namespace.as_deref().unwrap_or("default");
        let snapshot_handle = format!(
            "snapshot-{}-{}-{}",
            namespace,
            vs.metadata.name,
            uuid::Uuid::new_v4()
        );

        let content = VolumeSnapshotContent {
            type_meta: TypeMeta {
                kind: "VolumeSnapshotContent".to_string(),
                api_version: "snapshot.storage.k8s.io/v1".to_string(),
            },
            metadata: {
                let mut meta = ObjectMeta::new(content_name);
                meta.uid = uuid::Uuid::new_v4().to_string();
                meta.resource_version = Some("1".to_string());
                meta
            },
            spec: VolumeSnapshotContentSpec {
                source: VolumeSnapshotContentSource {
                    volume_handle: Some(volume_handle.to_string()),
                    snapshot_handle: None, // Will be set by the actual snapshotter
                },
                volume_snapshot_ref: ObjectReference {
                    kind: Some("VolumeSnapshot".to_string()),
                    namespace: Some(namespace.to_string()),
                    name: Some(vs.metadata.name.clone()),
                    uid: Some(vs.metadata.uid.clone()),
                    api_version: Some("snapshot.storage.k8s.io/v1".to_string()),
                    resource_version: vs.metadata.resource_version.clone(),
                    field_path: None,
                },
                volume_snapshot_class_name: Some(vsc.metadata.name.clone()),
                deletion_policy: vsc.deletion_policy.clone(),
                driver: vsc.driver.clone(),
            },
            status: Some(VolumeSnapshotContentStatus {
                snapshot_handle: Some(snapshot_handle.clone()),
                creation_time: Some(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)),
                ready_to_use: Some(true), // Simplified - in real impl, this would be async
                restore_size: None,       // Would be populated by actual snapshotter
                error: None,
            }),
        };

        Ok(content)
    }

    async fn update_snapshot_status(
        &self,
        vs: &VolumeSnapshot,
        content_name: &str,
        ready: bool,
    ) -> Result<()> {
        let namespace = vs.metadata.namespace.as_deref().unwrap_or("default");
        let vs_key = build_key("volumesnapshots", Some(namespace), &vs.metadata.name);

        // creation_time records when the snapshot was first taken and must
        // NEVER advance once set. Preserve the prior value on every reconcile;
        // otherwise we'd stamp Utc::now() at every interval and emit a
        // MODIFIED watch event per cycle — a controller hot-loop.
        let preserved_creation_time = vs
            .status
            .as_ref()
            .and_then(|s| s.creation_time.clone())
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        let new_status = VolumeSnapshotStatus {
            bound_volume_snapshot_content_name: Some(content_name.to_string()),
            creation_time: Some(preserved_creation_time),
            ready_to_use: Some(ready),
            restore_size: None,
            error: None,
        };

        // Skip the write when nothing actually changed. Compare via canonical
        // JSON since VolumeSnapshotStatus doesn't derive PartialEq.
        let old_v = serde_json::to_value(vs.status.as_ref()).ok();
        let new_v = serde_json::to_value(Some(&new_status)).ok();
        if old_v == new_v {
            return Ok(());
        }

        let mut updated_vs = vs.clone();
        updated_vs.status = Some(new_status);
        // Status subresource write: a full-object PUT strips `.status` (#1723).
        self.storage.update_status(&vs_key, &updated_vs).await?;

        info!(
            "Updated VolumeSnapshot {}/{} status (ready: {})",
            namespace, vs.metadata.name, ready
        );

        Ok(())
    }

    async fn reconcile_deletions(&self) -> Result<()> {
        // Get all VolumeSnapshotContents
        let contents: Vec<VolumeSnapshotContent> = self
            .storage
            .list("/registry/volumesnapshotcontents/")
            .await?;

        for content in contents {
            // Check if the referenced VolumeSnapshot still exists
            let vs_ref = &content.spec.volume_snapshot_ref;

            if let (Some(namespace), Some(name)) = (&vs_ref.namespace, &vs_ref.name) {
                let vs_key = build_key("volumesnapshots", Some(namespace), name);

                // If the VolumeSnapshot is deleted and deletion policy is Delete
                if self.storage.get::<VolumeSnapshot>(&vs_key).await.is_err() {
                    if content.spec.deletion_policy == DeletionPolicy::Delete {
                        info!(
                            "VolumeSnapshot {}/{} was deleted. Deleting VolumeSnapshotContent {} (deletion policy: Delete)",
                            namespace, name, content.metadata.name
                        );

                        let content_key =
                            build_key("volumesnapshotcontents", None, &content.metadata.name);
                        self.storage.delete(&content_key).await?;

                        info!("Deleted VolumeSnapshotContent {}", content.metadata.name);
                    } else {
                        info!(
                            "VolumeSnapshot {}/{} was deleted, but VolumeSnapshotContent {} will be retained (deletion policy: Retain)",
                            namespace, name, content.metadata.name
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "volume_snapshot_tests.rs"]
mod tests;
