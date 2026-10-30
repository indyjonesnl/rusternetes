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
pub const NAMESPACE_SYSTEM: &str = "kube-system";

/// `controlplane.IdentityLeaseComponentLabelKey`
/// (`pkg/controlplane/instance.go`).
pub const IDENTITY_LEASE_COMPONENT_LABEL_KEY: &str = "apiserver.kubernetes.io/identity";

/// `apiserver.KubeAPIServer` (`pkg/controlplane/apiserver/...`).
pub const KUBE_API_SERVER: &str = "kube-apiserver";

/// Resync period for the StorageVersion handlers (informer resync stand-in).
const RESYNC_INTERVAL: Duration = Duration::from_secs(30);

/// The controller descriptor's `requiredFeatureGates`
/// (`core.go` `newStorageVersionGarbageCollectorControllerDescriptor`).
pub fn gates_enabled() -> bool {
    enabled(Feature::APIServerIdentity) && enabled(Feature::StorageVersionAPI)
}

pub fn lease_key(name: &str) -> String {
    build_key("leases", Some(NAMESPACE_SYSTEM), name)
}

pub fn sv_key(name: &str) -> String {
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
