//! Storage version garbage collector.
//!
//! Port of upstream `pkg/controller/storageversiongc/gc_controller.go`
//! (`Controller`, `NewStorageVersionGC`, `Run`) and its registration in
//! `cmd/kube-controller-manager/app/core.go`
//! (`newStorageVersionGarbageCollectorController`; the descriptor requires
//! both `APIServerIdentity` and `StorageVersionAPI`).
//!
//! It watches kube-apiserver identity leases (`kube-system`, label
//! `apiserver.kubernetes.io/identity=kube-apiserver`) and StorageVersions and
//! removes stale `status.storageVersions` entries / whole StorageVersion
//! objects whose apiserver ID no longer has a lease. Two queues, one worker
//! each, exactly as upstream:
//!
//! - `leaseQueue` (`processDeletedLease`): fed by the lease informer's
//!   `DeleteFunc` (`onDeleteLease`).
//! - `storageVersionQueue` (`syncStorageVersion`): fed by the StorageVersion
//!   informer's Add/Update handlers (`enqueueStorageVersion`), which only
//!   enqueue an object that has an entry with no live identity lease.
//!
//! Divergences, with reasons: this workspace has no informers/listers, so the
//! "lister" lookups in `enqueueStorageVersion` read storage directly, and the
//! informer resync (`use the default resync period from the informer`) is a
//! periodic re-enqueue of every StorageVersion. Writes go straight to storage
//! (no apiserver in the loop, same as the sibling controllers).
//! The upstream controller has no metrics.

use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::feature_gates::{enabled, Feature};
use rusternetes_common::resources::storageversion::{
    set_common_encoding_version, ServerStorageVersion, StorageVersion,
};
use rusternetes_common::resources::Lease;
use rusternetes_common::Error;
use rusternetes_storage::{build_key, build_prefix, Storage, WatchEvent, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// `metav1.NamespaceSystem`.
const NAMESPACE_SYSTEM: &str = "kube-system";

/// `controlplane.IdentityLeaseComponentLabelKey`
/// (`pkg/controlplane/instance.go`).
const IDENTITY_LEASE_COMPONENT_LABEL_KEY: &str = "apiserver.kubernetes.io/identity";

/// `apiserver.KubeAPIServer` (`pkg/controlplane/apiserver/...`).
const KUBE_API_SERVER: &str = "kube-apiserver";

/// Resync period for the StorageVersion handlers (informer resync stand-in).
const RESYNC_INTERVAL: Duration = Duration::from_secs(30);

/// The controller descriptor's `requiredFeatureGates`
/// (`core.go` `newStorageVersionGarbageCollectorControllerDescriptor`).
pub fn gates_enabled() -> bool {
    enabled(Feature::APIServerIdentity) && enabled(Feature::StorageVersionAPI)
}

fn lease_key(name: &str) -> String {
    build_key("leases", Some(NAMESPACE_SYSTEM), name)
}

fn sv_key(name: &str) -> String {
    build_key("storageversions", None, name)
}

fn is_identity_lease(lease: &Lease) -> bool {
    lease
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(IDENTITY_LEASE_COMPONENT_LABEL_KEY))
        .is_some_and(|v| v == KUBE_API_SERVER)
}

fn name_from_event(ev: &WatchEvent) -> String {
    let key = match ev {
        WatchEvent::Added(k, _) | WatchEvent::Modified(k, _) | WatchEvent::Deleted(k, _) => k,
    };
    key.rsplit('/').next().unwrap_or(key).to_string()
}

pub struct StorageVersionGCController<S: Storage> {
    storage: Arc<S>,
    resync: Duration,
}

impl<S: Storage + 'static> StorageVersionGCController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            resync: RESYNC_INTERVAL,
        }
    }

    /// Does `apiserver_id` have a live kube-apiserver identity lease?
    /// (The predicate shared by `syncStorageVersion` and
    /// `enqueueStorageVersion`: lease exists, has the identity label.)
    async fn has_valid_lease(&self, apiserver_id: &str) -> bool {
        match self.storage.get::<Lease>(&lease_key(apiserver_id)).await {
            Ok(lease) => is_identity_lease(&lease),
            Err(_) => false,
        }
    }

    /// Upstream `Run`: one worker per queue.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting storage version garbage collector");
        let lease_queue = WorkQueue::new();
        let sv_queue = WorkQueue::new();

        let me = Arc::clone(&self);
        let q = lease_queue.clone();
        tokio::spawn(async move { me.run_lease_worker(q).await });
        let me = Arc::clone(&self);
        let q = sv_queue.clone();
        tokio::spawn(async move { me.run_storage_version_worker(q).await });

        loop {
            self.enqueue_all_storage_versions(&sv_queue).await;

            let lease_prefix = build_key("leases", Some(NAMESPACE_SYSTEM), "");
            let sv_prefix = build_prefix("storageversions", None);
            let (mut lease_watch, mut sv_watch) = match tokio::try_join!(
                self.storage.watch(&lease_prefix),
                self.storage.watch(&sv_prefix)
            ) {
                Ok(w) => w,
                Err(e) => {
                    warn!("storage version GC watch failed: {e}; retrying");
                    tokio::time::sleep(self.resync).await;
                    continue;
                }
            };
            let mut tick = tokio::time::interval(self.resync);
            tick.tick().await;
            loop {
                tokio::select! {
                    ev = lease_watch.next() => match ev {
                        Some(Ok(ev)) => self.on_lease_event(&lease_queue, &ev).await,
                        _ => break,
                    },
                    ev = sv_watch.next() => match ev {
                        Some(Ok(ev)) => self.on_storage_version_event(&sv_queue, &ev).await,
                        _ => break,
                    },
                    _ = tick.tick() => self.enqueue_all_storage_versions(&sv_queue).await,
                }
            }
        }
    }

    /// Upstream `onDeleteLease`: only deletions of kube-system identity
    /// leases labelled `kube-apiserver` are enqueued (`enqueueLease`).
    pub async fn on_lease_event(&self, queue: &WorkQueue, ev: &WatchEvent) {
        let WatchEvent::Deleted(_, value) = ev else {
            return;
        };
        let Ok(lease) = serde_json::from_str::<Lease>(value) else {
            return;
        };
        if lease
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(NAMESPACE_SYSTEM)
            == NAMESPACE_SYSTEM
            && is_identity_lease(&lease)
        {
            debug!("Observed lease deleted: {}", lease.metadata.name);
            queue.add(lease.metadata.name.clone()).await;
        }
    }

    /// Upstream `onAddStorageVersion` / `onUpdateStorageVersion` (delete has
    /// no handler).
    pub async fn on_storage_version_event(&self, queue: &WorkQueue, ev: &WatchEvent) {
        let value = match ev {
            WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => v,
            WatchEvent::Deleted(..) => return,
        };
        if let Ok(sv) = serde_json::from_str::<StorageVersion>(value) {
            self.enqueue_storage_version(queue, &sv).await;
        } else {
            debug!(
                "ignoring undecodable StorageVersion {}",
                name_from_event(ev)
            );
        }
    }

    /// Upstream `enqueueStorageVersion`: enqueue if any entry has no valid
    /// identity lease.
    pub async fn enqueue_storage_version(&self, queue: &WorkQueue, sv: &StorageVersion) {
        for ssv in &sv.status.storage_versions {
            if !self.has_valid_lease(&ssv.api_server_id).await {
                debug!(
                    "Observed storage version with invalid apiserver entry: {}",
                    sv.metadata.name
                );
                queue.add(sv.metadata.name.clone()).await;
                return;
            }
        }
    }

    async fn enqueue_all_storage_versions(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<StorageVersion>(&build_prefix("storageversions", None))
            .await
        {
            Ok(items) => {
                for sv in &items {
                    self.enqueue_storage_version(queue, sv).await;
                }
            }
            Err(e) => error!("Failed to list storageversions: {e}"),
        }
    }

    /// Upstream `runLeaseWorker` / `processNextLease`: on error
    /// `AddRateLimited` (unbounded), on success `Forget`.
    async fn run_lease_worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            match self.process_deleted_lease(&key).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    error!("lease {key} failed with: {e}");
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Upstream `runStorageVersionWorker` / `processNextStorageVersion`.
    async fn run_storage_version_worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            match self.sync_storage_version(&key).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    error!("storage version {key} failed with: {e}");
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Upstream `processDeletedLease`.
    pub async fn process_deleted_lease(&self, name: &str) -> Result<()> {
        match self.storage.get::<Lease>(&lease_key(name)).await {
            // the lease isn't deleted, nothing we need to do here
            Ok(_) => return Ok(()),
            Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        let list: Vec<StorageVersion> = self
            .storage
            .list(&build_prefix("storageversions", None))
            .await?;

        let mut errors: Vec<String> = Vec::new();
        for sv in list {
            let mut has_stale_record = false;
            let mut remaining: Vec<ServerStorageVersion> = Vec::new();
            for ssv in &sv.status.storage_versions {
                if ssv.api_server_id == name {
                    has_stale_record = true;
                    continue;
                }
                remaining.push(ssv.clone());
            }
            if !has_stale_record {
                continue;
            }
            if let Err(e) = self.update_or_delete_storage_version(sv, remaining).await {
                errors.push(e.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            // utilerrors.NewAggregate
            Err(anyhow::anyhow!("[{}]", errors.join(", ")))
        }
    }

    /// Upstream `syncStorageVersion`.
    pub async fn sync_storage_version(&self, name: &str) -> Result<()> {
        let sv = match self.storage.get::<StorageVersion>(&sv_key(name)).await {
            Ok(sv) => sv,
            // The problematic storage version that was added/updated recently
            // is gone. Nothing we need to do here.
            Err(Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        let mut has_invalid_id = false;
        let mut remaining: Vec<ServerStorageVersion> = Vec::new();
        for v in &sv.status.storage_versions {
            if !self.has_valid_lease(&v.api_server_id).await {
                // We cannot find a corresponding identity lease from
                // apiserver as well. We need to clean up this storage version.
                has_invalid_id = true;
                continue;
            }
            remaining.push(v.clone());
        }
        if !has_invalid_id {
            return Ok(());
        }
        self.update_or_delete_storage_version(sv, remaining).await
    }

    /// Upstream `updateOrDeleteStorageVersion`.
    async fn update_or_delete_storage_version(
        &self,
        mut sv: StorageVersion,
        remaining: Vec<ServerStorageVersion>,
    ) -> Result<()> {
        let key = sv_key(&sv.metadata.name);
        if remaining.is_empty() {
            return Ok(self.storage.delete(&key).await?);
        }
        sv.status.storage_versions = remaining;
        set_common_encoding_version(&mut sv);
        self.storage.update(&key, &sv).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Ports of `pkg/controller/storageversiongc/gc_controller_test.go`.
    use super::*;
    use rusternetes_common::resources::storageversion::StorageVersionStatus;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    fn lease(name: &str) -> Lease {
        let mut l = Lease::new(name, NAMESPACE_SYSTEM);
        l.metadata.labels = Some(HashMap::from([(
            IDENTITY_LEASE_COMPONENT_LABEL_KEY.to_string(),
            KUBE_API_SERVER.to_string(),
        )]));
        l
    }

    fn ssv(id: &str, enc: &str) -> ServerStorageVersion {
        ServerStorageVersion {
            api_server_id: id.into(),
            encoding_version: enc.into(),
            decodable_versions: vec![enc.into()],
            served_versions: vec![],
        }
    }

    async fn setup(
        leases: &[&str],
        entries: Vec<ServerStorageVersion>,
        common: Option<&str>,
    ) -> Arc<MemoryStorage> {
        let storage = Arc::new(MemoryStorage::new());
        for l in leases {
            storage.create(&lease_key(l), &lease(l)).await.unwrap();
        }
        let mut sv = StorageVersion::default();
        sv.metadata.name = "k8s.test.resources".into();
        sv.status = StorageVersionStatus {
            storage_versions: entries,
            common_encoding_version: common.map(String::from),
            conditions: vec![],
        };
        storage
            .create(&sv_key("k8s.test.resources"), &sv)
            .await
            .unwrap();
        storage
    }

    async fn get_sv(s: &MemoryStorage) -> Result<StorageVersion, Error> {
        s.get(&sv_key("k8s.test.resources")).await
    }

    /// Run `run()` and delete `lease_name`; poll for `done`.
    async fn run_and_delete(
        storage: &Arc<MemoryStorage>,
        lease_name: &str,
        done: impl Fn(Option<StorageVersion>) -> bool,
    ) {
        let c = Arc::new(StorageVersionGCController::new(storage.clone()));
        let h = tokio::spawn(async move { c.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        storage.delete(&lease_key(lease_name)).await.unwrap();
        for _ in 0..100 {
            if done(get_sv(storage).await.ok()) {
                h.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        h.abort();
        panic!("controller did not reconcile storage version in time");
    }

    /// Test_StorageVersionUpdatedWithAllEncodingVersionsEqualOnLeaseDeletion
    #[tokio::test]
    async fn updated_with_all_encoding_versions_equal_on_lease_deletion() {
        let s = setup(
            &["kube-apiserver-1", "kube-apiserver-2", "kube-apiserver-3"],
            vec![
                ssv("kube-apiserver-1", "v1"),
                ssv("kube-apiserver-2", "v2"),
                ssv("kube-apiserver-3", "v2"),
            ],
            Some("v1"),
        )
        .await;
        run_and_delete(&s, "kube-apiserver-1", |sv| {
            sv.is_some_and(|sv| {
                sv.status.storage_versions
                    == vec![ssv("kube-apiserver-2", "v2"), ssv("kube-apiserver-3", "v2")]
                    && sv.status.common_encoding_version.as_deref() == Some("v2")
            })
        })
        .await;
    }

    /// Test_StorageVersionUpdatedWithDifferentEncodingVersionsOnLeaseDeletion
    #[tokio::test]
    async fn updated_with_different_encoding_versions_on_lease_deletion() {
        let s = setup(
            &["kube-apiserver-1", "kube-apiserver-2", "kube-apiserver-3"],
            vec![ssv("kube-apiserver-1", "v1"), ssv("kube-apiserver-3", "v2")],
            Some("v1"),
        )
        .await;
        // kube-apiserver-2 has no entry: its deletion must change nothing.
        let c = StorageVersionGCController::new(s.clone());
        s.delete(&lease_key("kube-apiserver-2")).await.unwrap();
        c.process_deleted_lease("kube-apiserver-2").await.unwrap();
        let sv = get_sv(&s).await.unwrap();
        assert_eq!(
            sv.status.storage_versions,
            vec![ssv("kube-apiserver-1", "v1"), ssv("kube-apiserver-3", "v2")]
        );
        assert_eq!(sv.status.common_encoding_version.as_deref(), Some("v1"));
    }

    /// Test_StorageVersionContainsInvalidLeaseID
    #[tokio::test]
    async fn contains_invalid_lease_id() {
        let s = setup(
            &["kube-apiserver-1", "kube-apiserver-2", "kube-apiserver-3"],
            vec![
                ssv("kube-apiserver-1", "v1"),
                ssv("kube-apiserver-2", "v2"),
                ssv("kube-apiserver-3", "v2"),
                ssv("kube-apiserver-4", "v2"), // no lease
            ],
            Some("v1"),
        )
        .await;
        let c = Arc::new(StorageVersionGCController::new(s.clone()));
        let h = tokio::spawn(async move { c.run().await });
        for _ in 0..100 {
            if get_sv(&s)
                .await
                .is_ok_and(|sv| sv.status.storage_versions.len() == 3)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        h.abort();
        let sv = get_sv(&s).await.unwrap();
        assert_eq!(
            sv.status.storage_versions,
            vec![
                ssv("kube-apiserver-1", "v1"),
                ssv("kube-apiserver-2", "v2"),
                ssv("kube-apiserver-3", "v2")
            ]
        );
        assert_eq!(sv.status.conditions.len(), 1);
        assert_eq!(
            sv.status.conditions[0].condition_type,
            "AllEncodingVersionsEqual"
        );
        assert_eq!(sv.status.conditions[0].status, "False");
        assert_eq!(sv.status.common_encoding_version, None);
    }

    /// Test_StorageVersionDeletedOnLeaseDeletion
    #[tokio::test]
    async fn deleted_on_lease_deletion() {
        let s = setup(
            &["kube-apiserver-1"],
            vec![ssv("kube-apiserver-1", "v1")],
            None,
        )
        .await;
        run_and_delete(&s, "kube-apiserver-1", |sv| sv.is_none()).await;
    }

    /// A lease without the identity label is not a valid owner
    /// (`syncStorageVersion`'s label check).
    #[tokio::test]
    async fn unlabelled_lease_is_invalid() {
        let s = setup(&[], vec![ssv("x", "v1")], None).await;
        let mut l = Lease::new("x", NAMESPACE_SYSTEM);
        l.metadata.labels = None;
        s.create(&lease_key("x"), &l).await.unwrap();
        let c = StorageVersionGCController::new(s.clone());
        c.sync_storage_version("k8s.test.resources").await.unwrap();
        assert!(get_sv(&s).await.is_err());
    }

    #[tokio::test]
    async fn on_delete_lease_ignores_non_identity_leases() {
        let s = Arc::new(MemoryStorage::new());
        let c = StorageVersionGCController::new(s);
        let q = WorkQueue::new();
        let plain = Lease::new("other", NAMESPACE_SYSTEM);
        c.on_lease_event(
            &q,
            &WatchEvent::Deleted("k".into(), serde_json::to_string(&plain).unwrap()),
        )
        .await;
        assert!(q.is_empty().await);
        c.on_lease_event(
            &q,
            &WatchEvent::Deleted("k".into(), serde_json::to_string(&lease("a")).unwrap()),
        )
        .await;
        assert_eq!(q.len().await, 1);
    }
}
