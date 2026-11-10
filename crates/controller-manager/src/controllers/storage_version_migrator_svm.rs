//! Storage version migrator: the migration controller (slices 3 and 4 of
//! #3825).
//!
//! Port of `pkg/controller/storageversionmigrator/storageversionmigrator.go`
//! (release-1.35): `SVMController` (`NewSVMController`, `Run`, `processNext`,
//! `sync`, `runMigration`, `isRetriableError`, `failMigration`). For each
//! `StorageVersionMigration` whose `status.resourceVersion` the
//! resource-version controller has stamped, it rewrites every object of the
//! resource that existed at that checkpoint with a no-op server-side apply
//! carrying only `apiVersion`, `kind`, `metadata.uid` and
//! `metadata.resourceVersion` (field manager = the controller name), so the
//! api-server re-encodes it at the current storage version.
//!
//! Seams (the upstream fakes in `storageversionmigrator_test.go` inject the
//! same two things): [`GraphMonitor`] is the garbage collector's
//! `GraphBuilder.GetMonitor` (the store plus `LastSyncResourceVersion`), and
//! [`MigrationPatcher`] is the dynamic client's `Patch(ApplyPatchType)`.
//!
//! Deviations:
//! - The garbage collector here has no per-GVR monitor with a synced store, so
//!   [`StorageGraphMonitor`] reads the objects from storage itself: the
//!   revision read BEFORE the list stands for `LastSyncResourceVersion` (the
//!   list can only be at or after it). The comparison against
//!   `status.resourceVersion` (`storageversionmigrator.go:253-262`) is kept,
//!   and a monitor is never "not synced".
//! - [`StoragePatcher`] is [`Storage::apply`]: a real `PATCH
//!   application/apply-patch+json` through the api-server in API mode (which
//!   converts to the storage version), the shared apply engine on a direct
//!   backend.
//! - `SuggestsClientDelay` (`processNext`) has no counterpart on our errors, so
//!   a failed sync is always `AddRateLimited`.
//! - The SVM informer is a storage watch plus a 30s resync, as the
//!   resource-version controller's.

use super::storage_version_migrator::{
    set_status_conditions, MIGRATION_FAILED_STATUS_REASON, MIGRATION_RUNNING_STATUS_REASON,
    MIGRATION_SUCCESS_STATUS_REASON,
};
use super::storage_version_migrator_rv::{condition_true, resource_for, SvmDiscovery};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use rusternetes_common::resources::storageversionmigration::{
    StorageVersionMigration, MIGRATION_FAILED, MIGRATION_RUNNING, MIGRATION_SUCCEEDED,
};
use rusternetes_common::validation::storageversionmigration::compare_resource_version;
use rusternetes_common::Error;
use rusternetes_discovery::restmapper::{Gvk, Gvr};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WatchEvent, WorkQueue};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// `workers` (storageversionmigrator.go:55).
pub const WORKERS: usize = 5;

const SVM_PLURAL: &str = "storageversionmigrations";

/// One object of the monitor's store (`meta.Accessor`).
#[derive(Debug, Clone, PartialEq)]
pub struct MonitoredObject {
    pub namespace: Option<String>,
    pub name: String,
    pub uid: String,
    pub resource_version: String,
}

/// `garbagecollector.Monitor`: the synced store and the informer's
/// `LastSyncResourceVersion()`.
#[derive(Debug, Clone, Default)]
pub struct Monitor {
    pub last_sync_resource_version: String,
    pub objects: Vec<MonitoredObject>,
}

/// `graphBuilder` (storageversionmigrator.go:58-60). `None` is
/// `hasSynced == false` ("resource does not exist in GC").
#[async_trait]
pub trait GraphMonitor: Send + Sync {
    async fn get_monitor(&self, gvr: &Gvr) -> Result<Option<Monitor>>;
}

/// The error classes `runMigration` tells apart.
#[derive(Debug)]
pub enum PatchError {
    /// `apierrors.IsConflict`.
    Conflict(String),
    /// `isRetriableError` (storageversionmigrator.go:333-341).
    Retriable(String),
    /// Anything else fails the migration.
    Fatal(String),
}

impl PatchError {
    /// `isRetriableError`: connection reset / lost / EOF (our `Network`),
    /// `ServerTimeout`, `TooManyRequests`, `ServiceUnavailable`,
    /// `InternalError`, `Timeout`.
    pub fn classify(e: &Error) -> Self {
        let msg = e.to_string();
        match e {
            Error::Conflict(_) => Self::Conflict(msg),
            Error::Network(_)
            | Error::TooManyRequests(_)
            | Error::Internal(_)
            | Error::Storage(_) => Self::Retriable(msg),
            Error::Status(s) if matches!(s.code, Some(500 | 503 | 504 | 429)) => {
                Self::Retriable(msg)
            }
            Error::Status(s) if s.code == Some(409) => Self::Conflict(msg),
            _ => Self::Fatal(msg),
        }
    }
}

/// The dynamic client's server-side-apply `Patch`
/// (`svmc.dynamicClient.Resource(gvr).Namespace(ns).Patch(ctx, name,
/// types.ApplyPatchType, data, PatchOptions{FieldManager: controllerName})`).
#[async_trait]
pub trait MigrationPatcher: Send + Sync {
    async fn apply(
        &self,
        gvr: &Gvr,
        namespace: Option<&str>,
        name: &str,
        body: &Value,
        field_manager: &str,
    ) -> std::result::Result<(), PatchError>;
}

/// [`GraphMonitor`] over storage; see the module deviations.
pub struct StorageGraphMonitor<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> StorageGraphMonitor<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: Storage + 'static> GraphMonitor for StorageGraphMonitor<S> {
    async fn get_monitor(&self, gvr: &Gvr) -> Result<Option<Monitor>> {
        // Read the revision first: the list below can only be at or after it.
        let revision = self.storage.current_revision().await?;
        let items: Vec<Value> = self
            .storage
            .list(&build_prefix(&gvr.resource, None))
            .await?;
        let objects = items
            .iter()
            .filter_map(|o| {
                let m = o.get("metadata")?;
                let text = |k: &str| {
                    m.get(k)
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string()
                };
                Some(MonitoredObject {
                    namespace: Some(text("namespace")).filter(|n| !n.is_empty()),
                    name: text("name"),
                    uid: text("uid"),
                    resource_version: text("resourceVersion"),
                })
            })
            .collect();
        Ok(Some(Monitor {
            last_sync_resource_version: revision.to_string(),
            objects,
        }))
    }
}

/// [`MigrationPatcher`] over [`Storage::apply`].
pub struct StoragePatcher<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> StoragePatcher<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: Storage + 'static> MigrationPatcher for StoragePatcher<S> {
    async fn apply(
        &self,
        gvr: &Gvr,
        namespace: Option<&str>,
        name: &str,
        body: &Value,
        field_manager: &str,
    ) -> std::result::Result<(), PatchError> {
        let key = build_key(&gvr.resource, namespace, name);
        self.storage
            .apply::<Value>(&key, body, field_manager, false, None)
            .await
            .map(|_| ())
            .map_err(|e| PatchError::classify(&e))
    }
}

/// `gvk.ToAPIVersionAndKind()`.
fn api_version_and_kind(gvk: &Gvk) -> (String, String) {
    let api_version = if gvk.group.is_empty() {
        gvk.version.clone()
    } else {
        format!("{}/{}", gvk.group, gvk.version)
    };
    (api_version, gvk.kind.clone())
}

/// `SVMController` (storageversionmigrator.go:62-72).
pub struct SvmController<S: Storage> {
    controller_name: String,
    storage: Arc<S>,
    discovery: Arc<dyn SvmDiscovery>,
    monitor: Arc<dyn GraphMonitor>,
    patcher: Arc<dyn MigrationPatcher>,
}

impl<S: Storage + 'static> SvmController<S> {
    /// The production wiring over `storage`.
    pub fn new(storage: Arc<S>, controller_name: impl Into<String>) -> Self {
        Self::with_seams(
            storage.clone(),
            controller_name,
            Arc::new(super::storage_version_migrator_rv::StorageDiscovery::new(
                storage.clone(),
            )),
            Arc::new(StorageGraphMonitor::new(storage.clone())),
            Arc::new(StoragePatcher::new(storage)),
        )
    }

    pub fn with_seams(
        storage: Arc<S>,
        controller_name: impl Into<String>,
        discovery: Arc<dyn SvmDiscovery>,
        monitor: Arc<dyn GraphMonitor>,
        patcher: Arc<dyn MigrationPatcher>,
    ) -> Self {
        Self {
            controller_name: controller_name.into(),
            storage,
            discovery,
            monitor,
            patcher,
        }
    }

    pub fn name(&self) -> &str {
        &self.controller_name
    }

    /// `Run` (storageversionmigrator.go:134-155): [`WORKERS`] workers over one
    /// queue named after the controller. `AddFunc` / `UpdateFunc` enqueue; deletes
    /// do not.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting {}", self.controller_name);
        let queue = WorkQueue::named(&self.controller_name);
        for _ in 0..WORKERS {
            let me = Arc::clone(&self);
            let q = queue.clone();
            tokio::spawn(async move { while me.process_next(&q).await {} });
        }
        loop {
            self.enqueue_all(&queue).await;
            let mut watch = match self.storage.watch(&build_prefix(SVM_PLURAL, None)).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch on {SVM_PLURAL}: {e}, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = tokio::time::interval(Duration::from_secs(30));
            resync.tick().await;
            loop {
                tokio::select! {
                    event = watch.next() => match event {
                        Some(Ok(ev)) => {
                            if !matches!(ev, WatchEvent::Deleted(..)) {
                                queue.add(extract_key(&ev)).await;
                            }
                        }
                        Some(Err(e)) => {
                            warn!("{} watch error: {e}, reconnecting", self.controller_name);
                            break;
                        }
                        None => break,
                    },
                    _ = resync.tick() => self.enqueue_all(&queue).await,
                }
            }
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<StorageVersionMigration>(&build_prefix(SVM_PLURAL, None))
            .await
        {
            Ok(svms) => {
                for svm in &svms {
                    queue
                        .add(format!("{SVM_PLURAL}/{}", svm.metadata.name))
                        .await;
                }
            }
            Err(e) => error!("Failed to list {SVM_PLURAL}: {e}"),
        }
    }

    /// `processNext` (storageversionmigrator.go:161-181).
    pub async fn process_next(&self, queue: &WorkQueue) -> bool {
        let Some(key) = queue.get().await else {
            return false;
        };
        match self.sync(&key).await {
            Ok(()) => queue.forget(&key).await,
            Err(e) => {
                debug!("Error syncing SVM resource {key}, retrying: {e}");
                queue.requeue_rate_limited(key.clone()).await;
            }
        }
        queue.done(&key).await;
        true
    }

    /// `sync` (storageversionmigrator.go:183-296).
    pub async fn sync(&self, key: &str) -> Result<()> {
        // SVM is a cluster scoped resource so we don't care about the namespace.
        let name = key.rsplit('/').next().unwrap_or(key);
        let skey = build_key(SVM_PLURAL, None, name);
        let svm: StorageVersionMigration = match self.storage.get(&skey).await {
            Ok(s) => s,
            // no work to do, don't fail and requeue
            Err(Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        if condition_true(&svm, MIGRATION_SUCCEEDED) || condition_true(&svm, MIGRATION_FAILED) {
            debug!("Migration has already succeeded or failed previously, skipping {name}");
            return Ok(());
        }
        if svm.status.resource_version.is_empty() {
            debug!("The latest resource version is empty. We will attempt to migrate once the resource version is available.");
            return Ok(());
        }

        let recent = svm
            .metadata
            .creation_timestamp
            .is_some_and(|t| t + chrono::Duration::minutes(1) > Utc::now());
        let Some(gvr) = resource_for(
            self.discovery.as_ref(),
            &svm.spec.resource.group,
            &svm.spec.resource.resource,
        )
        .await?
        else {
            if recent {
                return Err(anyhow!(
                    "resource does not exist in rest mapper, requeuing to attempt again"
                ));
            }
            return self
                .fail_migration(&skey, svm, "unable to obtain GVR from discovery")
                .await;
        };

        // prevent unsynced monitor from blocking forever: a short timeout so
        // that we fail quickly and can handle other migrations meanwhile.
        let monitor = tokio::time::timeout(Duration::from_secs(10), self.monitor.get_monitor(&gvr))
            .await
            .map_err(|_| anyhow!("timed out getting the monitor for {gvr}"))??;
        let Some(monitor) = monitor else {
            // our mapper could be missing a recently created custom resource,
            // so give it some time to catch up
            if recent {
                return Err(anyhow!(
                    "resource does not exist in GC, requeuing to attempt again"
                ));
            }
            return self
                .fail_migration(&skey, svm, "resource does not exist in GC")
                .await;
        };

        let list_rv = svm.status.resource_version.clone();
        match compare_resource_version(&monitor.last_sync_resource_version, &list_rv) {
            Err(e) => {
                return self
                    .fail_migration(
                        &skey,
                        svm,
                        &format!(
                            "error comparing resource versions between GC and SVM resource: {e}"
                        ),
                    )
                    .await;
            }
            Ok(-1) => {
                return Err(anyhow!(
                    "GC cache is not up to date, requeuing to attempt again. gcListResourceVersion: {}, listResourceVersion: {list_rv}",
                    monitor.last_sync_resource_version
                ));
            }
            Ok(_) => {}
        }

        let mut running = svm;
        set_status_conditions(
            &mut running,
            MIGRATION_RUNNING,
            MIGRATION_RUNNING_STATUS_REASON,
            "",
        );
        let running = self.storage.update_status_cas(&skey, &running).await?;

        let (failed, retry) = self
            .run_migration(&skey, &gvr, &monitor, &running, &list_rv)
            .await;
        if let Some(e) = retry {
            return Err(e);
        }
        if failed {
            return Ok(());
        }

        let mut done = running;
        set_status_conditions(
            &mut done,
            MIGRATION_SUCCEEDED,
            MIGRATION_SUCCESS_STATUS_REASON,
            "",
        );
        self.storage.update_status_cas(&skey, &done).await?;
        debug!("Finished syncing svm resource {key}");
        Ok(())
    }

    /// `runMigration` (storageversionmigrator.go:298-331). Returns
    /// `(failed, error)`: an error with `failed == false` is a retriable one
    /// that requeues; `failed == true` means the migration was failed.
    async fn run_migration(
        &self,
        skey: &str,
        gvr: &Gvr,
        monitor: &Monitor,
        svm: &StorageVersionMigration,
        list_rv: &str,
    ) -> (bool, Option<anyhow::Error>) {
        let gvk = match self.discovery.rest_mapper().await {
            Ok(m) => match m.kind_for(&Gvr::new(&gvr.group, &gvr.version, &gvr.resource)) {
                Ok(k) => k,
                Err(e) => return self.failed(skey, svm, &e.to_string()).await,
            },
            Err(e) => return self.failed(skey, svm, &e.to_string()).await,
        };
        let (api_version, kind) = api_version_and_kind(&gvk);

        for obj in &monitor.objects {
            match compare_resource_version(&obj.resource_version, list_rv) {
                Err(e) => {
                    debug!(
                        "Unable to compare the resource version of {}/{}: {e}",
                        obj.namespace.as_deref().unwrap_or(""),
                        obj.name
                    );
                    return self.failed(skey, svm, &e).await;
                }
                // Resource ignored due to resource version being greater than
                // the SVM checkpoint.
                Ok(1) => continue,
                Ok(_) => {}
            }

            // UID: when a resource gets deleted we get an "uid mismatch"
            // conflict instead of trying to create it. RV: when it is updated
            // or deleted+recreated we get an "object has been modified"
            // conflict. Both must be set (storageversionmigrator.go:316-322).
            let body = json!({
                "apiVersion": api_version,
                "kind": kind,
                "metadata": {"uid": obj.uid, "resourceVersion": obj.resource_version},
            });
            match self
                .patcher
                .apply(
                    gvr,
                    obj.namespace.as_deref(),
                    &obj.name,
                    &body,
                    &self.controller_name,
                )
                .await
            {
                Ok(()) => debug!("Successfully migrated {}/{}", gvr, obj.name),
                // updated, deleted or recreated: migration already done or
                // not needed.
                Err(PatchError::Conflict(_)) => continue,
                Err(PatchError::Retriable(e)) => return (false, Some(anyhow!(e))),
                Err(PatchError::Fatal(e)) => return self.failed(skey, svm, &e).await,
            }
        }
        (false, None)
    }

    async fn failed(
        &self,
        skey: &str,
        svm: &StorageVersionMigration,
        msg: &str,
    ) -> (bool, Option<anyhow::Error>) {
        match self.fail_migration(skey, svm.clone(), msg).await {
            Ok(()) => (true, None),
            // `return svmc.failMigration(...), true`: the status error is
            // returned, so the key is retried.
            Err(e) => (true, Some(e)),
        }
    }

    /// `failMigration` (storageversionmigrator.go:343-355).
    async fn fail_migration(
        &self,
        key: &str,
        mut svm: StorageVersionMigration,
        message: &str,
    ) -> Result<()> {
        let msg = format!("migration encountered unhandled error: {message}");
        set_status_conditions(
            &mut svm,
            MIGRATION_FAILED,
            MIGRATION_FAILED_STATUS_REASON,
            &msg,
        );
        self.storage.update_status_cas(key, &svm).await?;
        Ok(())
    }
}
