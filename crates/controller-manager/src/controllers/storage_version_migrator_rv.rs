//! Storage version migrator: the resource-version controller (slice 2 of
//! #3825).
//!
//! Port of `pkg/controller/storageversionmigrator/resourceversion.go`
//! (release-1.35): `ResourceVersionController`. Before a migration starts it
//! stamps `status.resourceVersion` with the latest resource version of the
//! migrated resource; the migration controller (a later slice) compares the
//! garbage collector's cache against that value. Upstream registers it
//! together with `SVMController` behind the `StorageVersionMigrator` gate
//! (`cmd/kube-controller-manager/app/storageversionmigrator.go:41-`), so it is
//! NOT wired into the controller manager yet: on its own it would fail
//! migrations that nothing would otherwise run.
//!
//! Seams (the upstream fakes in `resourceversion_test.go` inject the same
//! things): [`SvmDiscovery`] is the `DiscoveryInterface` + `RESTMapper`,
//! [`LatestResourceVersion`] is the metadata client's
//! `List(limit=1)` in `getLatestResourceVersion`. Their storage-backed
//! implementations answer from this server's own tables:
//! `served_resources_for_group_version` / `new_discovery_rest_mapper` over the
//! stored CRDs, and `Storage::current_revision` (the list resourceVersion of
//! any resource is the store revision, which is what the fake-namespace list
//! upstream observes too).
//!
//! Deviations: the informer is a storage watch plus a 30s resync with a
//! single worker (as `Run`); `UpdateStatus` is `update_status_cas`, which
//! keeps upstream's conflict-on-stale-resourceVersion behaviour.

use super::storage_version_migrator::{
    has_required_verbs, set_status_conditions, MIGRATION_FAILED_STATUS_REASON,
    VERBS_REQUIRED_FOR_MIGRATION,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use k3rs_common::resources::storageversionmigration::{
    StorageVersionMigration, MIGRATION_FAILED, MIGRATION_SUCCEEDED,
};
use k3rs_discovery::restmapper::{
    new_discovery_rest_mapper, served_group_resources, served_resources_for_group_version, Gvr,
    PriorityRestMapper, ServedResource,
};
use k3rs_storage::{build_key, build_prefix, extract_key, Storage, WatchEvent, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// `ResourceVersionControllerName`.
pub const RESOURCE_VERSION_CONTROLLER_NAME: &str = "resource-version-controller";
/// `fakeSVMNamespaceName`: not a valid namespace name, so it is guaranteed to
/// be absent from the cluster.
pub const FAKE_SVM_NAMESPACE_NAME: &str = "@fake:svm_ns!";

const PLURAL: &str = "storageversionmigrations";

/// `discovery.DiscoveryInterface` (the group-version listing) plus the
/// `meta.RESTMapper` the controller was built with.
#[async_trait]
pub trait SvmDiscovery: Send + Sync {
    async fn rest_mapper(&self) -> Result<PriorityRestMapper>;
    /// `ServerResourcesForGroupVersion`; `None` is a 404.
    async fn server_resources_for_group_version(
        &self,
        group: &str,
        version: &str,
    ) -> Result<Option<Vec<ServedResource>>>;
}

/// The metadata client's `List(Limit: 1)` as `getLatestResourceVersion` uses
/// it: the list resourceVersion of `gvr`, from the fake namespace when the
/// resource is namespaced.
#[async_trait]
pub trait LatestResourceVersion: Send + Sync {
    async fn list_resource_version(&self, gvr: &Gvr, namespace: Option<&str>) -> Result<String>;
}

/// [`SvmDiscovery`] over the CRDs in storage.
pub struct StorageDiscovery<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> StorageDiscovery<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    async fn crds(&self) -> Result<Vec<serde_json::Value>> {
        Ok(self
            .storage
            .list(&build_prefix("customresourcedefinitions", None))
            .await?)
    }
}

#[async_trait]
impl<S: Storage + 'static> SvmDiscovery for StorageDiscovery<S> {
    async fn rest_mapper(&self) -> Result<PriorityRestMapper> {
        Ok(new_discovery_rest_mapper(&served_group_resources(
            &self.crds().await?,
        )))
    }

    async fn server_resources_for_group_version(
        &self,
        group: &str,
        version: &str,
    ) -> Result<Option<Vec<ServedResource>>> {
        Ok(served_resources_for_group_version(
            group,
            version,
            &self.crds().await?,
        ))
    }
}

/// [`LatestResourceVersion`] from the store revision.
pub struct StorageResourceVersion<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> StorageResourceVersion<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: Storage + 'static> LatestResourceVersion for StorageResourceVersion<S> {
    async fn list_resource_version(&self, _gvr: &Gvr, _namespace: Option<&str>) -> Result<String> {
        Ok(self.storage.current_revision().await?.to_string())
    }
}

/// `resourceversion.CompareResourceVersion(rv, rv)` fails exactly when `rv` is
/// not well formed (`isWellFormed`, apimachinery `util/resourceversion`): a
/// non-empty run of digits without a leading zero.
pub fn is_well_formed_resource_version(rv: &str) -> bool {
    !rv.is_empty() && !rv.starts_with('0') && rv.bytes().all(|b| b.is_ascii_digit())
}

/// `ResourceVersionController` (resourceversion.go:57-65).
pub struct ResourceVersionController<S: Storage> {
    storage: Arc<S>,
    discovery: Arc<dyn SvmDiscovery>,
    metadata: Arc<dyn LatestResourceVersion>,
}

impl<S: Storage + 'static> ResourceVersionController<S> {
    /// The production wiring over `storage`.
    pub fn new(storage: Arc<S>) -> Self {
        Self::with_seams(
            storage.clone(),
            Arc::new(StorageDiscovery::new(storage.clone())),
            Arc::new(StorageResourceVersion::new(storage)),
        )
    }

    pub fn with_seams(
        storage: Arc<S>,
        discovery: Arc<dyn SvmDiscovery>,
        metadata: Arc<dyn LatestResourceVersion>,
    ) -> Self {
        Self {
            storage,
            discovery,
            metadata,
        }
    }

    /// `Run` (resourceversion.go:121-142): ONE worker. The informer's
    /// `AddFunc` / `UpdateFunc` enqueue the SVM name; deletes do not.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting {RESOURCE_VERSION_CONTROLLER_NAME}");
        let queue = WorkQueue::new();
        let me = Arc::clone(&self);
        let worker_queue = queue.clone();
        tokio::spawn(async move { while me.process_next(&worker_queue).await {} });

        loop {
            self.enqueue_all(&queue).await;
            let mut watch = match self.storage.watch(&build_prefix(PLURAL, None)).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch on {PLURAL}: {e}, retrying");
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
                            warn!("{RESOURCE_VERSION_CONTROLLER_NAME} watch error: {e}, reconnecting");
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
            .list::<StorageVersionMigration>(&build_prefix(PLURAL, None))
            .await
        {
            Ok(svms) => {
                for svm in &svms {
                    queue.add(format!("{PLURAL}/{}", svm.metadata.name)).await;
                }
            }
            Err(e) => error!("Failed to list {PLURAL}: {e}"),
        }
    }

    /// `processNext` (resourceversion.go:149-165): success forgets the key, an
    /// error requeues it rate-limited.
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

    /// `sync` (resourceversion.go:167-253).
    pub async fn sync(&self, key: &str) -> Result<()> {
        // SVM is a cluster scoped resource so we don't care about the namespace.
        let name = key.rsplit('/').next().unwrap_or(key);
        let skey = build_key(PLURAL, None, name);
        let mut svm: StorageVersionMigration = match self.storage.get(&skey).await {
            Ok(s) => s,
            // no work to do, don't fail and requeue
            Err(k3rs_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let gr = svm.spec.resource.clone();

        if condition_true(&svm, MIGRATION_SUCCEEDED) || condition_true(&svm, MIGRATION_FAILED) {
            debug!("Migration has already succeeded or failed previously, skipping {name}");
            return Ok(());
        }
        if !svm.status.resource_version.is_empty() {
            debug!("Resource version is already set for {name}");
            return Ok(());
        }

        let Some(gvr) = self.resource_for(&gr.group, &gr.resource).await? else {
            // our GC cache could be missing a recently created custom
            // resource, so give it some time to catch up
            let recent = svm
                .metadata
                .creation_timestamp
                .is_some_and(|t| t + chrono::Duration::minutes(1) > Utc::now());
            if recent {
                return Err(anyhow!(
                    "resource does not exist in our rest mapper, requeuing to attempt again"
                ));
            }
            return self
                .fail_migration(&skey, svm, "resource does not exist in discovery")
                .await;
        };

        if !self.is_resource_migratable(&gvr).await? {
            let msg = format!(
                "resource {:?} does not support discovery operations: {:?}",
                gvr.to_string(),
                VERBS_REQUIRED_FOR_MIGRATION
            );
            error!("resource is not able to be migrated, not retrying: {msg}");
            return self.fail_migration(&skey, svm, &msg).await;
        }

        let latest_rv = self.get_latest_resource_version(&gvr).await?;

        // Compare the resource version against itself, if it fails then it is
        // not a well formed RV and we should fail migration.
        if !is_well_formed_resource_version(&latest_rv) {
            let msg = format!(
                "latest resourceVersion for {gvr} is not valid: resource version {latest_rv:?} is not valid"
            );
            return self.fail_migration(&skey, svm, &msg).await;
        }
        svm.status.resource_version = latest_rv;
        self.storage
            .update_status_cas(&skey, &svm)
            .await
            .map_err(|e| anyhow!("error updating status for {}: {e}", svm.metadata.name))?;
        debug!("Resource version has been successfully added to {key}");
        Ok(())
    }

    /// `getLatestResourceVersion` (resourceversion.go:255-281).
    async fn get_latest_resource_version(&self, gvr: &Gvr) -> Result<String> {
        let namespaced = self.is_resource_namespace_scoped(gvr).await?;
        let ns = namespaced.then_some(FAKE_SVM_NAMESPACE_NAME);
        self.metadata
            .list_resource_version(gvr, ns)
            .await
            .map_err(|e| anyhow!("error getting latest resourceVersion for {gvr}: {e}"))
    }

    /// `resourceFor` (resourceversion.go:283-304).
    async fn resource_for(&self, group: &str, resource: &str) -> Result<Option<Gvr>> {
        resource_for(self.discovery.as_ref(), group, resource).await
    }

    async fn resources_of(&self, gvr: &Gvr) -> Result<Option<Vec<ServedResource>>> {
        self.discovery
            .server_resources_for_group_version(&gvr.group, &gvr.version)
            .await
    }

    /// `isResourceNamespaceScoped` (resourceversion.go:306-321).
    async fn is_resource_namespace_scoped(&self, gvr: &Gvr) -> Result<bool> {
        let list = self
            .resources_of(gvr)
            .await?
            .ok_or_else(|| anyhow!("the server could not find the requested resource"))?;
        list.iter()
            .find(|r| r.name == gvr.resource)
            .map(|r| r.namespaced)
            .ok_or_else(|| anyhow!("resource {:?} not found", gvr.to_string()))
    }

    /// `isResourceMigratable` (resourceversion.go:323-352): all of
    /// [`VERBS_REQUIRED_FOR_MIGRATION`] in the discovery document; a 404 for
    /// the group-version is "not migratable", a resource missing from an
    /// existing document is an error.
    async fn is_resource_migratable(&self, gvr: &Gvr) -> Result<bool> {
        let Some(list) = self.resources_of(gvr).await? else {
            return Ok(false);
        };
        match list.iter().find(|r| r.name == gvr.resource) {
            Some(r) => Ok(has_required_verbs(&r.verbs)),
            None => Err(anyhow!(
                "resource {:?} not found in discovery",
                gvr.to_string()
            )),
        }
    }

    /// `failMigration` (resourceversion.go:354-366).
    async fn fail_migration(
        &self,
        key: &str,
        mut svm: StorageVersionMigration,
        message: &str,
    ) -> Result<()> {
        set_status_conditions(
            &mut svm,
            MIGRATION_FAILED,
            MIGRATION_FAILED_STATUS_REASON,
            message,
        );
        self.storage.update_status_cas(key, &svm).await?;
        Ok(())
    }
}

/// `resourceFor` (resourceversion.go:283-304): `ResourcesFor` with only group
/// and resource; a no-match is "does not exist". Shared with the migration
/// controller, which calls the same function (`storageversionmigrator.go:177`).
pub async fn resource_for(
    discovery: &dyn SvmDiscovery,
    group: &str,
    resource: &str,
) -> Result<Option<Gvr>> {
    let mapper = discovery.rest_mapper().await?;
    match mapper.resources_for(&Gvr::new(group, "", resource)) {
        Err(e) if e.is_no_match() => Ok(None),
        Err(e) => Err(anyhow!("{e}")),
        Ok(gvrs) => Ok(gvrs
            .into_iter()
            .find(|g| g.group == group && g.resource == resource)),
    }
}

/// `meta.IsStatusConditionTrue`.
pub fn condition_true(svm: &StorageVersionMigration, ty: &str) -> bool {
    svm.status
        .conditions
        .iter()
        .any(|c| c.condition_type == ty && c.status == "True")
}
