//! `volume-data-source-validator`: warns on a PVC whose `dataSourceRef` kind
//! no registered `VolumePopulator` handles.
//!
//! Ported from kubernetes-csi/volume-data-source-validator
//! `pkg/data-source-validator/data_source_validator.go`
//! (`syncPvcByKey`, `validateGroupKind`, `enqueueWork`, `worker`). The
//! in-tree half of volume populators (`dataSourceRef` defaulting, backwards
//! compatibility and the CrossNamespaceVolumeDataSource gate) is the
//! `persistentvolumeclaim` strategy
//! (`pkg/api/persistentvolumeclaim/util.go`); the provisioner leaves unknown
//! kinds to the populator (`csi_provisioner.rs`, `prepare_provision`). This
//! controller is the third piece: it tells the user nobody is going to
//! populate their claim.
//!
//! `VolumePopulator` (`populator.storage.k8s.io/v1beta1`, cluster scoped,
//! `sourceKind: {group, kind}`) is a CRD
//! (`test/e2e/testing-manifests/storage-csi/any-volume-datasource/crd/
//! populator.storage.k8s.io_volumepopulators.yaml`), so it is read as a
//! custom resource: storage type `populator_storage_k8s_io_volumepopulators`.

use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::{EventSource, EventType, PersistentVolumeClaim};
use rusternetes_storage::{
    build_key, build_prefix, extract_key, EventRecorder, Storage, WorkQueue,
};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// `eventRecorder = broadcaster.NewRecorder(scheme.Scheme,
/// v1.EventSource{Component: "volume-data-source-validator"})`.
const COMPONENT: &str = "volume-data-source-validator";

/// `PopulatorResource = popv1beta1.SchemeGroupVersion.WithResource(
/// "volumepopulators")`, stored the way the custom-resource handler keys a
/// CR (`{group with '_'}_{plural}`).
const POPULATOR_RESOURCE: &str = "populator_storage_k8s_io_volumepopulators";

/// `cmd/volume-data-source-validator/main.go` `--worker-threads` default.
const WORKER_THREADS: usize = 10;

/// `ctrl.eventRecorder.Event(pvc, v1.EventTypeWarning,
/// "UnrecognizedDataSourceKind", ...)`.
const UNRECOGNIZED_REASON: &str = "UnrecognizedDataSourceKind";
const UNRECOGNIZED_MESSAGE: &str =
    "The datasource for this PVC does not match any registered VolumePopulator";

/// `validateGroupKind`'s verdict, i.e. which `metrics.*ResultName` bucket the
/// claim lands in.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Validation {
    /// `pvcGK`: cloning is a special case allowed by the core.
    Pvc,
    /// `volumeSnapshotGK`: likewise.
    VolumeSnapshot,
    /// A `VolumePopulator` registers this kind.
    Populator,
    /// No populator matches.
    Invalid,
}

impl Validation {
    pub fn is_valid(self) -> bool {
        self != Validation::Invalid
    }
}

/// `validateGroupKind` (data_source_validator.go): PVC and VolumeSnapshot are
/// special cases; otherwise some `VolumePopulator.sourceKind` (a
/// `metav1.GroupKind`) must equal the claim's `{apiGroup, kind}`.
pub fn validate_group_kind(group: &str, kind: &str, populators: &[Value]) -> Validation {
    match (group, kind) {
        ("", "PersistentVolumeClaim") => return Validation::Pvc,
        ("snapshot.storage.k8s.io", "VolumeSnapshot") => return Validation::VolumeSnapshot,
        _ => {}
    }
    for p in populators {
        let sk = &p["sourceKind"];
        // `populator.SourceKind == gk`: a missing field decodes to "".
        if sk["group"].as_str().unwrap_or("") == group && sk["kind"].as_str().unwrap_or("") == kind
        {
            return Validation::Populator;
        }
    }
    Validation::Invalid
}

pub struct VolumeDataSourceValidator<S: Storage> {
    storage: Arc<S>,
    recorder: EventRecorder<S>,
}

impl<S: Storage + 'static> VolumeDataSourceValidator<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
        }
    }

    /// `Run` + the PVC informer handlers: every PVC add/update/delete is
    /// enqueued; N workers drain one queue.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting volume-data-source-validator controller");
        let queue = WorkQueue::new();
        spawn_workers(WORKER_THREADS, &queue, |worker_queue| {
            let this = Arc::clone(&self);
            async move { this.worker(worker_queue).await }
        });

        loop {
            self.enqueue_all(&queue).await;
            let prefix = build_prefix("persistentvolumeclaims", None);
            let mut watch = match self.storage.watch(&prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("volume-data-source-validator: watch failed: {e}, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = tokio::time::interval(Duration::from_secs(30));
            resync.tick().await;
            loop {
                tokio::select! {
                    ev = watch.next() => match ev {
                        Some(Ok(ev)) => queue.add(extract_key(&ev)).await,
                        _ => break,
                    },
                    // A VolumePopulator registered after the claim must be
                    // noticed too; the Go controller relies on the PVC
                    // informer's resync for that.
                    _ = resync.tick() => self.enqueue_all(&queue).await,
                }
            }
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<PersistentVolumeClaim>(&build_prefix("persistentvolumeclaims", None))
            .await
        {
            Ok(pvcs) => {
                for p in &pvcs {
                    let ns = p.metadata.namespace.as_deref().unwrap_or("");
                    queue
                        .add(format!("persistentvolumeclaims/{ns}/{}", p.metadata.name))
                        .await;
                }
            }
            Err(e) => error!("volume-data-source-validator: list PVCs: {e}"),
        }
    }

    /// `worker`: a failed sync is re-queued rate limited, a successful one
    /// forgotten.
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            match self.sync_pvc_by_key(&key).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    debug!("Failed to sync pvc {key:?}, will retry again: {e}");
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// `syncPvcByKey`. A claim that no longer exists is not an error.
    pub async fn sync_pvc_by_key(&self, key: &str) -> Result<()> {
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        let [_, ns, name] = parts[..] else {
            return Ok(());
        };
        let pvc: PersistentVolumeClaim = match self
            .storage
            .get(&build_key("persistentvolumeclaims", Some(ns), name))
            .await
        {
            Ok(p) => p,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        self.sync_pvc(&pvc).await
    }

    pub async fn sync_pvc(&self, pvc: &PersistentVolumeClaim) -> Result<()> {
        // `dataSourceRef == nil`: "No data source".
        let Some(r) = pvc.spec.data_source_ref.as_ref() else {
            return Ok(());
        };
        let group = r.api_group.as_deref().unwrap_or("");
        debug!(
            "PVC {:?} datasource is {group}/{}",
            pvc.metadata.name, r.kind
        );

        // `validateGroupKind` only lists populators for the non-special kinds;
        // an error listing them is returned and retried.
        let populators: Vec<Value> = match validate_group_kind(group, &r.kind, &[]) {
            Validation::Pvc | Validation::VolumeSnapshot => Vec::new(),
            _ => {
                self.storage
                    .list::<Value>(&build_prefix(POPULATOR_RESOURCE, None))
                    .await?
            }
        };
        if !validate_group_kind(group, &r.kind, &populators).is_valid() {
            warn!("No populator matches {group}/{}", r.kind);
            let involved = ObjectReference {
                kind: Some("PersistentVolumeClaim".to_string()),
                namespace: pvc.metadata.namespace.clone(),
                name: Some(pvc.metadata.name.clone()),
                uid: Some(pvc.metadata.uid.clone()),
                api_version: Some("v1".to_string()),
                resource_version: pvc.metadata.resource_version.clone(),
                field_path: None,
            };
            let source = EventSource {
                component: COMPONENT.to_string(),
                host: None,
            };
            if let Err(e) = self
                .recorder
                .event(
                    &involved,
                    &source,
                    EventType::Warning,
                    UNRECOGNIZED_REASON,
                    UNRECOGNIZED_MESSAGE,
                )
                .await
            {
                warn!("failed to record {UNRECOGNIZED_REASON} event: {e}");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn populator(group: &str, kind: &str) -> Value {
        json!({"sourceKind": {"group": group, "kind": kind}})
    }

    #[test]
    fn pvc_and_snapshot_are_special_cases() {
        assert_eq!(
            validate_group_kind("", "PersistentVolumeClaim", &[]),
            Validation::Pvc
        );
        assert_eq!(
            validate_group_kind("snapshot.storage.k8s.io", "VolumeSnapshot", &[]),
            Validation::VolumeSnapshot
        );
    }

    #[test]
    fn registered_populator_matches_group_and_kind() {
        let pops = [populator("hello.example.com", "Hello")];
        assert_eq!(
            validate_group_kind("hello.example.com", "Hello", &pops),
            Validation::Populator
        );
        assert!(!validate_group_kind("other.example.com", "Hello", &pops).is_valid());
        assert!(!validate_group_kind("hello.example.com", "Other", &pops).is_valid());
        // A core-group kind is not a PVC just because a populator exists.
        assert!(!validate_group_kind("", "Hello", &pops).is_valid());
    }
}
