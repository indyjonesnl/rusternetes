// Garbage Collector - Manages cascade deletion and orphaning of dependent resources
//
// Implements:
// - Owner reference tracking
// - Cascade deletion (foreground and background)
// - Orphan deletion
// - Finalizer handling for deletion protection

use rusternetes_common::types::{DeletionPropagation, ObjectMeta};
use rusternetes_storage::Storage;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

/// Prefix covering every stored object; the GC watches all of it because any
/// resource type can own a dependent, mirroring upstream GC monitoring every
/// deletable resource (`pkg/controller/garbagecollector/garbagecollector.go`
/// `resyncMonitors` over `GetDeletableResources`).
const REGISTRY_PREFIX: &str = "/registry/";

/// How long a watch-kicked scan waits before running, so a cascade that
/// deletes many objects costs one scan rather than one per object. Also the
/// floor on how fast a broken watch can drive rescans.
const KICK_DEBOUNCE: Duration = Duration::from_millis(250);

/// Garbage collector controller
#[allow(dead_code)]
pub struct GarbageCollector<S: Storage> {
    storage: Arc<S>,
    /// Base GC scan cadence. Used when the previous scan did work (found
    /// orphans or processed deletions) — see [`next_scan_interval`].
    scan_interval: Duration,
    /// Upper bound the scan interval backs off to while the cluster is idle
    /// (no orphans, nothing being deleted). Keeps an idle controller-manager
    /// quiet (#1040) without a hard-coded fast poll.
    max_scan_interval: Duration,
    /// Maximum number of concurrent delete operations
    max_concurrent_deletes: usize,
    /// Batch size for deletion operations
    delete_batch_size: usize,
    /// Maximum retry attempts for failed deletions
    max_retries: u32,
    /// Orphans detected in the previous scan. Only delete orphans that appear
    /// in TWO consecutive scans. This prevents race conditions where a resource
    /// is created between the GC listing owners and listing dependents.
    /// K8s avoids this via informer caches; we use a grace period.
    pending_orphans: std::sync::Mutex<HashSet<String>>,
    /// Number of full-cluster list passes performed. Each pass LISTs every
    /// resource type (28 of them), so this is the GC's dominant cost against
    /// the api-server and the thing that starves a cascade of its own budget.
    /// Counted so a regression is a test failure rather than a slow nightly.
    list_passes: std::sync::atomic::AtomicUsize,
}

impl<S: Storage + 'static> GarbageCollector<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            scan_interval: Duration::from_secs(5),
            max_scan_interval: Duration::from_secs(60),
            max_concurrent_deletes: 50,
            delete_batch_size: 100,
            max_retries: 3,
            pending_orphans: std::sync::Mutex::new(HashSet::new()),
            list_passes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Create a new garbage collector with custom settings
    #[allow(dead_code)]
    pub fn with_config(
        storage: Arc<S>,
        scan_interval_secs: u64,
        max_concurrent_deletes: usize,
        delete_batch_size: usize,
    ) -> Self {
        Self {
            storage,
            scan_interval: Duration::from_secs(scan_interval_secs),
            max_scan_interval: Duration::from_secs(60),
            max_concurrent_deletes,
            delete_batch_size,
            max_retries: 3,
            pending_orphans: std::sync::Mutex::new(HashSet::new()),
            list_passes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Create a garbage collector with explicit scan cadence bounds.
    ///
    /// `scan_interval` is the base cadence used after a scan that did work;
    /// `max_scan_interval` is the ceiling the idle backoff walks up to. See
    /// [`next_scan_interval`].
    #[allow(dead_code)]
    pub fn with_scan_intervals(
        storage: Arc<S>,
        scan_interval: Duration,
        max_scan_interval: Duration,
    ) -> Self {
        Self {
            scan_interval,
            max_scan_interval,
            ..Self::new(storage)
        }
    }

    /// Start the garbage collector.
    ///
    /// The scan cadence adapts to activity: a scan that did work runs again at
    /// the base [`scan_interval`](Self::scan_interval); a run of idle scans
    /// backs off exponentially up to [`max_scan_interval`](Self::max_scan_interval)
    /// so an idle controller-manager stays quiet (#1040). Unlike the other
    /// controllers this cannot be driven by a single-resource watch — GC scans
    /// the whole owner/dependent graph — so idle backoff is the scan-based
    /// analogue of upstream GC's workqueue rate-limiter.
    pub async fn run(&self) {
        info!("Starting Garbage Collector");
        let base = self.scan_interval;
        let max = self.max_scan_interval;
        let mut interval = base;
        let mut watch: Option<rusternetes_storage::WatchStream> = None;
        loop {
            match self.scan_and_collect_inner().await {
                Ok(did_work) => interval = next_scan_interval(interval, did_work, base, max),
                Err(e) => {
                    error!("Garbage collection scan failed: {}", e);
                    // Retry promptly at the base cadence after an error.
                    interval = base;
                }
            }
            if watch.is_none() {
                match self.storage.watch(REGISTRY_PREFIX).await {
                    Ok(w) => watch = Some(w),
                    Err(e) => warn!(
                        "GC registry watch unavailable ({}); polling only this cycle",
                        e
                    ),
                }
            }
            self.wait_for_next_scan(interval, &mut watch).await;
        }
    }

    /// Wait out `interval`, but return early when a deletion event says there
    /// is work now.
    ///
    /// This is the piece the idle backoff cannot supply on its own: with the
    /// cadence stretched to `max_scan_interval`, an owner deleted right after
    /// an idle scan is not *noticed* for up to that long, which loses the 30s
    /// `wait.ForeverTestTimeout` budget deletion specs run on (#1839) and the
    /// 90s `[sig-api-machinery] Namespaces [Serial]` budget. Upstream has no
    /// such window because the delete event itself enqueues the dependents
    /// (pkg/controller/garbagecollector/graph_builder.go `processGraphChanges`
    /// -> `attemptToDelete`); a registry-wide watch is the closest thing we
    /// have to that until the graph builder lands (#1039).
    async fn wait_for_next_scan(
        &self,
        interval: Duration,
        watch: &mut Option<rusternetes_storage::WatchStream>,
    ) {
        use futures::StreamExt;
        let deadline = tokio::time::Instant::now() + interval;
        while let Some(stream) = watch.as_mut() {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return,
                event = stream.next() => match event {
                    Some(Ok(ev)) if event_warrants_scan(&ev) => {
                        debug!("GC scan kicked by a deletion event ahead of its poll");
                        // Collapse a burst (a cascade deletes many objects at
                        // once) into one scan instead of one scan per object.
                        sleep(KICK_DEBOUNCE).await;
                        return;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        // Lagged or broken: events were missed, so the safe
                        // move is to relist now and reconnect next cycle.
                        warn!("GC registry watch error ({}); rescanning and reconnecting", e);
                        *watch = None;
                        sleep(KICK_DEBOUNCE).await;
                        return;
                    }
                    None => {
                        warn!("GC registry watch ended; reconnecting on the next cycle");
                        *watch = None;
                    }
                }
            }
        }
        tokio::time::sleep_until(deadline).await;
    }

    /// Scan all resources and collect orphans.
    ///
    /// Thin wrapper preserving the historical `Result<()>` API used by tests;
    /// [`scan_and_collect_inner`](Self::scan_and_collect_inner) carries the
    /// did-work signal the adaptive [`run`](Self::run) loop needs. Only the
    /// tests call this now (production `run` uses the inner method directly).
    #[allow(dead_code)]
    pub async fn scan_and_collect(&self) -> rusternetes_common::Result<()> {
        self.scan_and_collect_inner().await.map(|_| ())
    }

    /// Scan all resources and collect orphans, returning whether the scan did
    /// any work (found orphans to delete, or resources pending deletion). The
    /// adaptive `run` loop uses this to decide whether to back off.
    async fn scan_and_collect_inner(&self) -> rusternetes_common::Result<bool> {
        debug!("Running garbage collection scan");

        // Get all resources from storage
        let all_resources = self.get_all_resources().await?;

        // Build owner-dependent relationship map
        let (owner_map, dependent_map) = self.build_relationship_maps(&all_resources);

        // Detect cycles in dependency graph (non-blocking, just warn)
        if let Err(cycle_info) = self.detect_cycles(&dependent_map, &all_resources) {
            warn!(
                "Detected dependency cycle in resource graph: {}",
                cycle_info
            );
        }

        // Find orphaned resources (resources whose owners no longer exist)
        let orphans = self.find_orphans(&all_resources, &owner_map);

        // K8s GC does not require multiple scans to confirm an orphan — it
        // re-reads each owner from the apiserver before deleting the dependent
        // (attemptToDeleteItem → getObject in garbagecollector.go:521). We
        // do the same in `delete_orphan`, which re-reads both the dependent
        // and every owner from storage and only deletes when all owners are
        // confirmed gone. The previous 2-scan grace introduced a full-cycle
        // delay between owner deletion and dependent removal, which
        // conformance tests for GC orphan-pod cleanup observe as a failure.
        //
        // We still record `pending_orphans` as a no-op so existing fields
        // compile, but it is no longer consulted to gate deletion.
        {
            let mut pending = self.pending_orphans.lock().unwrap();
            *pending = orphans.iter().map(|o| o.key.clone()).collect();
        }

        let mut found_orphans = !orphans.is_empty();
        if found_orphans {
            info!(
                "Found {} orphaned resources to verify and delete",
                orphans.len()
            );

            let mut deleted_count = 0;
            let mut failed_count = 0;

            // Collect TRANSITIVELY, not one level per scan. Deleting an orphan
            // orphans whatever pointed at it, and re-listing here is what makes
            // a chain (pod2 -> pod1, pod3 -> pod2) collapse in a single sweep.
            //
            // Upstream never waits for its next pass either: the deletion feeds
            // the graph builder, which enqueues every dependent of the removed
            // node (pkg/controller/garbagecollector/graph_builder.go
            // `processGraphChanges` → `attemptToDelete` over `n.dependents`).
            // Peeling one level per scan cost the conformance spec
            // "Garbage collector should not be blocked by dependency circle"
            // its 150s budget once scans got slow under load: the chain needed
            // as many full-cluster scans as it was long.
            //
            // Bounded so a pathological graph (or a resource that keeps
            // reappearing) cannot spin a scan forever — the next scan picks up
            // whatever is left.
            const MAX_CASCADE_ROUNDS: usize = 10;
            let mut round_orphans = orphans.clone();
            for round in 0..MAX_CASCADE_ROUNDS {
                for orphan in &round_orphans {
                    match self.delete_orphan(orphan).await {
                        Ok(_) => deleted_count += 1,
                        Err(e) => {
                            failed_count += 1;
                            error!("Failed to delete orphan {}: {}", orphan.key, e);
                        }
                    }
                }

                // Re-list and re-derive: anything that just lost its last owner
                // is an orphan now. Stop as soon as a round finds nothing new.
                let refreshed = self.get_all_resources().await?;
                let (refreshed_owner_map, _) = self.build_relationship_maps(&refreshed);
                let next = self.find_orphans(&refreshed, &refreshed_owner_map);
                let previous: HashSet<String> =
                    round_orphans.iter().map(|o| o.key.clone()).collect();
                round_orphans = next
                    .into_iter()
                    .filter(|o| !previous.contains(&o.key))
                    .collect();
                if round_orphans.is_empty() {
                    break;
                }
                debug!(
                    "GC cascade round {}: {} newly orphaned resource(s)",
                    round + 1,
                    round_orphans.len()
                );
            }

            found_orphans = true;
            info!(
                "GC orphan deletion complete: {} deleted, {} failed",
                deleted_count, failed_count
            );
        }

        // NOTE: Namespace deletion is handled by the NamespaceController, NOT the GC.
        // K8s GC handles ownerReference cascading (e.g. Deployment → ReplicaSet → Pod).
        // Namespace cleanup (deleting all resources in a namespace) is done by the
        // NamespacedResourcesDeleter (our NamespaceController).
        // Previously, the GC also did cascade_delete_namespace which force-deleted
        // all resources ignoring finalizers, racing with the namespace controller
        // and breaking conformance tests that rely on finalizer-blocked deletion ordering.
        // K8s ref: pkg/controller/namespace/deletion/namespaced_resources_deleter.go
        //
        // Skipping namespace cascade in GC. The deleted_namespaces detection below
        // is kept but the cascade is removed.
        let deleted_namespaces: Vec<_> = all_resources
            .iter()
            .filter(|r| r.resource_type == "namespaces" && r.metadata.is_being_deleted())
            .collect();

        for _namespace in deleted_namespaces {
            // Namespace cleanup handled by NamespaceController — do nothing here.
            // The namespace controller respects finalizers and deletion ordering.
        }

        // Process resources with deletion timestamp
        let being_deleted: Vec<_> = all_resources
            .iter()
            .filter(|r| r.metadata.is_being_deleted())
            .collect();
        let had_being_deleted = !being_deleted.is_empty();

        for resource in being_deleted {
            if let Err(e) = self.process_deletion(resource, &dependent_map).await {
                error!("Failed to process deletion for {:?}: {}", resource.key, e);
            }
        }

        debug!("Garbage collection scan complete");
        // "Did work" = something was actionable this scan. Drives idle backoff
        // in `run`; when both are empty the cluster is quiet and the next scan
        // waits longer.
        Ok(found_orphans || had_being_deleted)
    }

    /// Full-cluster list passes performed so far. See
    /// [`list_passes`](Self::list_passes).
    #[cfg(test)]
    fn list_pass_count(&self) -> usize {
        self.list_passes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get all resources from storage
    async fn get_all_resources(&self) -> rusternetes_common::Result<Vec<ResourceInfo>> {
        self.list_passes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut resources = Vec::new();

        // List all resources across all namespaces and resource types
        // In a real implementation, this would be more sophisticated
        // For now, we'll scan known resource types
        let resource_types = vec![
            ("namespaces", false), // cluster-scoped
            ("pods", true),
            ("services", true),
            ("endpoints", true),
            ("endpointslices", true),
            ("ingresses", true),
            ("networkpolicies", true),
            ("verticalpodautoscalers", true),
            ("volumesnapshots", true),
            ("volumesnapshotcontents", false), // cluster-scoped
            ("resourceclaims", true),
            ("certificatesigningrequests", false), // cluster-scoped
            ("customresourcedefinitions", false),  // cluster-scoped
            ("deployments", true),
            ("replicationcontrollers", true),
            ("replicasets", true),
            ("statefulsets", true),
            ("daemonsets", true),
            ("controllerrevisions", true),
            ("jobs", true),
            ("cronjobs", true),
            ("configmaps", true),
            ("secrets", true),
            ("serviceaccounts", true),
            ("persistentvolumeclaims", true),
            ("persistentvolumes", false),   // cluster-scoped
            ("clusterroles", false),        // cluster-scoped
            ("clusterrolebindings", false), // cluster-scoped
        ];

        for (resource_type, namespaced) in resource_types {
            if namespaced {
                // For namespaced resources, we need to list across all namespaces
                // This is simplified - in reality we'd list all namespaces first
                let prefix = format!("/registry/{}/", resource_type);
                if let Ok(items) = self
                    .list_resources_with_metadata(&prefix, resource_type)
                    .await
                {
                    resources.extend(items);
                }
            } else {
                // For cluster-scoped resources
                let prefix = format!("/registry/{}/", resource_type);
                if let Ok(items) = self
                    .list_resources_with_metadata(&prefix, resource_type)
                    .await
                {
                    resources.extend(items);
                }
            }
        }

        Ok(resources)
    }

    /// List resources with metadata
    async fn list_resources_with_metadata(
        &self,
        prefix: &str,
        resource_type: &str,
    ) -> rusternetes_common::Result<Vec<ResourceInfo>> {
        let values: Vec<Value> = self.storage.list(prefix).await?;
        let mut resources = Vec::new();

        for value in values {
            if let Err(e) = self.extract_metadata(&value) {
                debug!(
                    "GC: Failed to extract metadata from {} resource: {} (key hint: {:?})",
                    resource_type,
                    e,
                    value.pointer("/metadata/name").and_then(|v| v.as_str())
                );
            }
            if let Ok(metadata) = self.extract_metadata(&value) {
                // Reconstruct the storage key using the same format as build_key
                let key = match &metadata.namespace {
                    Some(ns) => format!("/registry/{}/{}/{}", resource_type, ns, metadata.name),
                    None => format!("/registry/{}/{}", resource_type, metadata.name),
                };

                resources.push(ResourceInfo {
                    key,
                    metadata,
                    resource_type: resource_type.to_string(),
                    value,
                });
            }
        }

        Ok(resources)
    }

    /// Extract metadata from a resource
    fn extract_metadata(&self, value: &Value) -> rusternetes_common::Result<ObjectMeta> {
        let metadata = value.get("metadata").ok_or_else(|| {
            rusternetes_common::Error::InvalidResource("Missing metadata".to_string())
        })?;

        serde_json::from_value(metadata.clone())
            .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))
    }

    /// Build owner-dependent relationship maps
    fn build_relationship_maps(
        &self,
        resources: &[ResourceInfo],
    ) -> (HashMap<String, Vec<String>>, HashMap<String, Vec<String>>) {
        let mut owner_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut dependent_map: HashMap<String, Vec<String>> = HashMap::new();

        for resource in resources {
            let resource_uid = &resource.metadata.uid;

            // Track what this resource owns
            if let Some(owner_refs) = &resource.metadata.owner_references {
                for owner_ref in owner_refs {
                    // Map: owner UID -> list of dependent UIDs
                    owner_map
                        .entry(owner_ref.uid.clone())
                        .or_default()
                        .push(resource_uid.clone());

                    // Map: dependent UID -> list of owner UIDs
                    dependent_map
                        .entry(resource_uid.clone())
                        .or_default()
                        .push(owner_ref.uid.clone());
                }
            }
        }

        (owner_map, dependent_map)
    }

    /// Find orphaned resources — resources where ALL owner references point to
    /// non-existent owners. A resource with at least one valid owner is NOT an orphan.
    ///
    /// Owner resolution is **namespace-scoped**, matching upstream
    /// `pkg/controller/garbagecollector/garbagecollector.go` (`classifyReferences`):
    ///
    ///   * A namespaced dependent's owner ref resolves only against resources
    ///     in the same namespace *or* cluster-scoped resources. A UID that
    ///     happens to exist in a different namespace is treated as
    ///     unresolvable — cross-namespace owner refs are invalid by K8s API
    ///     contract, so they MUST orphan the dependent regardless of UID
    ///     collisions elsewhere in the cluster.
    ///   * A cluster-scoped dependent's owner ref must resolve to another
    ///     cluster-scoped resource (cluster → namespaced refs are illegal).
    ///
    /// Resources with `deletionTimestamp` are still treated as "existing"
    /// here — they're only truly gone once removed from storage, and their
    /// dependents are handled by the foreground/orphan finalizer paths.
    fn find_orphans(
        &self,
        resources: &[ResourceInfo],
        _owner_map: &HashMap<String, Vec<String>>,
    ) -> Vec<ResourceInfo> {
        // Bucket existing UIDs by namespace (`None` = cluster-scoped).
        let mut uids_by_namespace: HashMap<Option<&str>, HashSet<&str>> = HashMap::new();
        for r in resources {
            let ns = r.metadata.namespace.as_deref();
            uids_by_namespace
                .entry(ns)
                .or_default()
                .insert(r.metadata.uid.as_str());
        }
        let cluster_scoped_uids: &HashSet<&str> = uids_by_namespace
            .get(&None)
            .map(|s| s as &HashSet<&str>)
            .unwrap_or({
                static EMPTY: std::sync::OnceLock<HashSet<&str>> = std::sync::OnceLock::new();
                EMPTY.get_or_init(HashSet::new)
            });

        let mut orphans = Vec::new();
        for resource in resources {
            let owner_refs = match &resource.metadata.owner_references {
                Some(refs) if !refs.is_empty() => refs,
                _ => continue,
            };
            let dep_ns = resource.metadata.namespace.as_deref();
            let same_ns_uids = uids_by_namespace.get(&dep_ns);

            let all_owners_missing = owner_refs.iter().all(|owner_ref| {
                let uid = owner_ref.uid.as_str();
                let in_same_ns = same_ns_uids.map(|s| s.contains(uid)).unwrap_or(false);
                let in_cluster_scope = cluster_scoped_uids.contains(uid);
                // A namespaced dependent accepts same-namespace OR cluster-scoped
                // owners; a cluster-scoped dependent accepts only cluster-scoped
                // owners. Either way, an owner sitting in some *other* namespace
                // is unresolvable.
                match dep_ns {
                    Some(_) => !(in_same_ns || in_cluster_scope),
                    None => !in_cluster_scope,
                }
            });

            if all_owners_missing {
                debug!(
                    "GC: orphan {} — ownerRef UIDs {:?} unresolvable in namespace {:?}",
                    resource.key,
                    owner_refs
                        .iter()
                        .map(|r| r.uid.as_str())
                        .collect::<Vec<_>>(),
                    dep_ns,
                );
                orphans.push(resource.clone());
            }
        }

        orphans
    }

    /// Delete an orphaned resource, but only after re-verifying the owner is gone.
    ///
    /// The initial orphan detection uses a snapshot which can be stale — resources
    /// created between the scan start and the orphan check won't be in the snapshot.
    /// K8s GC re-reads the owner from the API server before deleting dependents:
    /// see attemptToDeleteItem → getObject in garbagecollector.go:521.
    ///
    /// We re-read both the dependent (to get fresh ownerRefs) and then look up
    /// each owner by constructing the storage key from the ownerReference fields.
    async fn delete_orphan(&self, orphan: &ResourceInfo) -> rusternetes_common::Result<()> {
        // Re-read the resource from storage to get fresh ownerReferences.
        // It may have been updated since the scan snapshot.
        let fresh: Value = match self.storage.get(&orphan.key).await {
            Ok(v) => v,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()), // already gone
            Err(e) => return Err(e),
        };
        let fresh_meta = match self.extract_metadata(&fresh) {
            Ok(m) => m,
            Err(_) => return Ok(()), // can't parse metadata, skip
        };

        // If ownerReferences were removed (orphan policy processed), skip deletion
        let owner_refs = match &fresh_meta.owner_references {
            Some(refs) if !refs.is_empty() => refs,
            _ => return Ok(()), // no owners = not an orphan (or already orphaned)
        };

        // For each ownerReference, construct the storage key and check if the owner exists.
        // K8s uses the owner's GVR + namespace + name to look it up.
        // We use kind → plural resource name mapping + namespace from the dependent.
        let namespace = fresh_meta.namespace.as_deref();

        for owner_ref in owner_refs {
            let plural = kind_to_plural(&owner_ref.kind);
            if plural.is_empty() {
                // Unknown kind — be conservative, don't delete
                debug!(
                    "GC: {} has owner of unknown kind '{}', skipping",
                    orphan.key, owner_ref.kind
                );
                return Ok(());
            }
            let owner_key = if let Some(ns) = namespace {
                format!("/registry/{}/{}/{}", plural, ns, owner_ref.name)
            } else {
                format!("/registry/{}/{}", plural, owner_ref.name)
            };

            // Try to read the owner from storage
            match self.storage.get::<Value>(&owner_key).await {
                Ok(owner_value) => {
                    // Owner exists — verify UID matches
                    if let Some(uid) = owner_value
                        .pointer("/metadata/uid")
                        .and_then(|u| u.as_str())
                    {
                        if uid == owner_ref.uid {
                            // Owner with matching UID exists — NOT an orphan
                            debug!(
                                "GC: {} is NOT orphan — owner {}/{} (uid={}) still exists",
                                orphan.key, owner_ref.kind, owner_ref.name, uid
                            );
                            return Ok(());
                        }
                        // UID mismatch — the resource was recreated with a different UID.
                        // The old owner is gone, this ownerRef is dangling.
                    }
                }
                Err(rusternetes_common::Error::NotFound(_)) => {
                    // Owner not found — this ownerRef is dangling
                }
                Err(_) => {
                    // Storage error — be conservative, don't delete
                    return Ok(());
                }
            }
        }

        // All owners verified as gone — this is truly an orphan
        info!(
            "Deleting orphaned resource: {} ({}) — all owners verified gone",
            orphan.key, orphan.resource_type
        );
        self.storage.delete(&orphan.key).await
    }

    /// Process deletion for a resource with deletion timestamp
    async fn process_deletion(
        &self,
        resource: &ResourceInfo,
        dependent_map: &HashMap<String, Vec<String>>,
    ) -> rusternetes_common::Result<()> {
        // Check deletion propagation policy from finalizers
        let propagation_policy = self.determine_propagation_policy(&resource.metadata);

        match propagation_policy {
            DeletionPropagation::Foreground => {
                // In foreground deletion, we must delete all dependents first,
                // then remove the foregroundDeletion finalizer.
                self.delete_dependents_foreground(resource, dependent_map)
                    .await?;

                // GATE: only remove the foregroundDeletion finalizer once every
                // blocking dependent is actually gone from storage.
                // `delete_dependents_foreground` operates on a snapshot, so pods
                // that were still draining (or created after the snapshot) can
                // outlive a single pass. Removing the finalizer now would delete
                // the owner while dependents remain — the exact failure the
                // "keep the rc around until all its pods are deleted" conformance
                // test catches. Upstream keeps the finalizer until all blocking
                // dependents are removed (garbagecollector.go attemptToDeleteItem);
                // we re-check and wait for the next scan instead.
                if self.still_has_dependents(&resource.metadata.uid).await? {
                    debug!(
                        "Foreground deletion of {} waiting: dependents still present in storage",
                        resource.key
                    );
                    return Ok(());
                }

                // Remove the foregroundDeletion finalizer from the resource
                self.remove_finalizer(resource, "foregroundDeletion")
                    .await?;
            }
            DeletionPropagation::Orphan => {
                // In orphan mode, we remove owner references from dependents,
                // then remove the orphan finalizer
                self.orphan_dependents(resource, dependent_map).await?;

                // Remove the orphan finalizer from the resource
                self.remove_finalizer(resource, "orphan").await?;
            }
            DeletionPropagation::Background => {
                // In background deletion, we delete the owner and let GC clean up dependents
                // via the orphan detection in the next scan
            }
        }

        // Re-read the resource to see if it still has finalizers
        let current: rusternetes_common::Result<Value> = self.storage.get(&resource.key).await;
        match current {
            Ok(value) => {
                if let Ok(meta) = self.extract_metadata(&value) {
                    if !meta.has_finalizers()
                        && !spec_finalizers_remain(&resource.resource_type, &value)
                    {
                        // An object still inside its deletion grace period belongs
                        // to whoever is draining it — for a pod, the kubelet, which
                        // removes it once the container actually stops. Upstream's
                        // GC never competes for that: `attemptToDeleteItem` returns
                        // at once for an item that is being deleted and is not
                        // deleting dependents
                        // (pkg/controller/garbagecollector/garbagecollector.go:511),
                        // and the object's removal is the api-server's
                        // `deleteAfterGracePeriod` / `ShouldDeleteDuringUpdate` job.
                        //
                        // We cannot drop this sweep outright yet:
                        // `ShouldDeleteDuringUpdate` is implemented only on the
                        // PATCH path (crates/api-server/src/handlers/generic_patch.rs),
                        // so an object whose last finalizer is removed by a PUT —
                        // or by a controller writing straight to storage — has
                        // nothing else to finish it. Waiting out the grace period
                        // fixes the harm that matters (a terminating pod dropping
                        // out of the API before its grace period had run) while
                        // keeping the sweep as the backstop it currently is. #1828
                        // tracks removing it once every write path honours
                        // ShouldDeleteDuringUpdate.
                        if !Self::deletion_grace_period_elapsed(&meta) {
                            debug!(
                                "Resource {} is still inside its deletion grace \
                                 period, leaving it to its owner",
                                resource.key
                            );
                            return Ok(());
                        }
                        info!(
                            "Deleting resource (no finalizers remaining): {}",
                            resource.key
                        );
                        self.storage.delete(&resource.key).await?;
                    } else {
                        debug!(
                            "Resource {} still has finalizers {:?}, waiting",
                            resource.key, meta.finalizers
                        );
                    }
                }
            }
            Err(_) => {
                // Resource already deleted
                debug!("Resource {} already deleted", resource.key);
            }
        }

        Ok(())
    }

    /// Whether an object's deletion grace period has run out.
    ///
    /// `true` when there is no grace period to wait on (nothing set, or it is
    /// zero), so callers keep their existing behaviour for every resource that
    /// is not gracefully deleted.
    fn deletion_grace_period_elapsed(meta: &ObjectMeta) -> bool {
        let Some(deleted_at) = meta.deletion_timestamp else {
            return true;
        };
        match meta.deletion_grace_period_seconds {
            Some(secs) if secs > 0 => {
                chrono::Utc::now() >= deleted_at + chrono::Duration::seconds(secs)
            }
            _ => true,
        }
    }

    /// Remove a specific finalizer from a resource in storage
    async fn remove_finalizer(
        &self,
        resource: &ResourceInfo,
        finalizer: &str,
    ) -> rusternetes_common::Result<()> {
        // Re-read the resource to get the latest version
        let current: Value = self.storage.get(&resource.key).await?;
        let mut updated = current;

        if let Some(metadata) = updated.get_mut("metadata") {
            if let Some(finalizers) = metadata.get_mut("finalizers") {
                if let Some(arr) = finalizers.as_array_mut() {
                    arr.retain(|f| f.as_str() != Some(finalizer));
                    if arr.is_empty() {
                        if let Some(obj) = metadata.as_object_mut() {
                            obj.remove("finalizers");
                        }
                    }
                }
            }
        }

        info!("Removing {} finalizer from {}", finalizer, resource.key);
        self.storage.update_raw(&resource.key, &updated).await?;
        Ok(())
    }

    /// Determine deletion propagation policy from metadata
    fn determine_propagation_policy(&self, metadata: &ObjectMeta) -> DeletionPropagation {
        if let Some(finalizers) = &metadata.finalizers {
            if finalizers.contains(&"foregroundDeletion".to_string()) {
                return DeletionPropagation::Foreground;
            }
            if finalizers.contains(&"orphan".to_string()) {
                return DeletionPropagation::Orphan;
            }
        }
        // Default to background deletion
        DeletionPropagation::Background
    }

    /// Delete dependents in foreground mode.
    /// Deletes dependents whose ONLY owner is the resource being deleted.
    /// Dependents with other valid owners are not deleted — instead, the
    /// owner reference to the deleted resource is removed from them.
    async fn delete_dependents_foreground(
        &self,
        resource: &ResourceInfo,
        dependent_map: &HashMap<String, Vec<String>>,
    ) -> rusternetes_common::Result<()> {
        // ONE snapshot for the whole cascade. Re-listing per level made the
        // cost of deleting an owner scale with its dependent count — see
        // `delete_dependents_foreground_visiting`.
        let all_resources = self.get_all_resources().await?;
        let mut visited = HashSet::new();
        self.delete_dependents_foreground_visiting(
            resource,
            dependent_map,
            &all_resources,
            &mut visited,
        )
        .await
    }

    /// Cycle-safe body of [`Self::delete_dependents_foreground`].
    ///
    /// `visited` holds the UIDs already descended into during THIS foreground
    /// pass. Ownership graphs may legitimately contain cycles — the upstream
    /// Conformance spec "Garbage collector should not be blocked by dependency
    /// circle" builds one deliberately — and without this the recursion never
    /// terminates: pod1 -> pod2 -> pod3 -> pod1 overflowed the stack and took
    /// the whole controller-manager process down with it.
    async fn delete_dependents_foreground_visiting(
        &self,
        resource: &ResourceInfo,
        _dependent_map: &HashMap<String, Vec<String>>,
        all_resources: &[ResourceInfo],
        visited: &mut HashSet<String>,
    ) -> rusternetes_common::Result<()> {
        let resource_uid = &resource.metadata.uid;
        if !visited.insert(resource_uid.clone()) {
            debug!(
                "Foreground deletion: already descended into {} this pass (ownership cycle)",
                resource.key
            );
            return Ok(());
        }

        // Find all resources that have this resource as an owner, from the
        // snapshot the cascade opened with.
        let dependents: Vec<_> = all_resources
            .iter()
            .filter(|r| {
                r.metadata
                    .owner_references
                    .as_ref()
                    .is_some_and(|refs| refs.iter().any(|oref| oref.uid == *resource_uid))
            })
            .collect();

        if dependents.is_empty() {
            debug!("No dependents to delete for resource {}", resource.key);
            return Ok(());
        }

        info!(
            "Foreground deletion: processing {} dependents of {}",
            dependents.len(),
            resource.key
        );

        let existing_uids: HashSet<_> = all_resources
            .iter()
            .map(|r| r.metadata.uid.as_str())
            .collect();

        for dependent in dependents {
            if let Some(owner_refs) = &dependent.metadata.owner_references {
                // Check if this dependent has other VALID owners (besides the one being deleted)
                let has_other_valid_owner = owner_refs.iter().any(|oref| {
                    oref.uid != *resource_uid && existing_uids.contains(oref.uid.as_str())
                });

                if has_other_valid_owner {
                    // Dependent has another valid owner — just remove the reference
                    // to the owner being deleted
                    info!(
                        "Dependent {} has other valid owners, removing reference to {}",
                        dependent.key, resource.key
                    );
                    let mut dependent_value = dependent.value.clone();
                    if let Some(metadata) = dependent_value.get_mut("metadata") {
                        if let Some(owner_refs_val) = metadata.get_mut("ownerReferences") {
                            if let Some(arr) = owner_refs_val.as_array_mut() {
                                arr.retain(|oref| {
                                    oref.get("uid")
                                        .and_then(|u| u.as_str())
                                        .map(|u| u != resource_uid)
                                        .unwrap_or(true)
                                });
                            }
                        }
                    }
                    if let Err(e) = self
                        .storage
                        .update_raw(&dependent.key, &dependent_value)
                        .await
                    {
                        error!("Failed to update dependent {}: {}", dependent.key, e);
                    }
                } else if visited.contains(&dependent.metadata.uid) {
                    // Ownership cycle: this dependent is already on the walk, so
                    // it is the object whose own foreground deletion started it.
                    // Leave it entirely alone. Deleting it here would take it out
                    // of storage behind its own finalizers — including the
                    // `foregroundDeletion` one that must not be dropped until
                    // every blocking dependent is gone
                    // (upstream `processDeletingDependentsItem`,
                    // pkg/controller/garbagecollector/garbagecollector.go). The
                    // nodes between here and it are deleted as this recursion
                    // unwinds, which clears its gate; `process_deletion` then
                    // removes the finalizer and deletes it properly.
                    debug!(
                        "Foreground deletion: {} closes an ownership cycle with {}, \
                         leaving it to its own deletion pass",
                        dependent.key, resource.key
                    );
                } else {
                    // Dependent's only owner is the one being deleted — delete it
                    info!(
                        "Deleting dependent {} (sole owner being deleted)",
                        dependent.key
                    );

                    // Recursively handle foreground deletion for this dependent's dependents
                    let (_, sub_dependent_map) = self.build_relationship_maps(all_resources);
                    Box::pin(self.delete_dependents_foreground_visiting(
                        dependent,
                        &sub_dependent_map,
                        all_resources,
                        visited,
                    ))
                    .await?;

                    match self.storage.delete(&dependent.key).await {
                        Ok(_) => {}
                        // The snapshot is from the start of the cascade, so a
                        // dependent something else already removed is expected,
                        // not an error.
                        Err(rusternetes_common::Error::NotFound(_)) => {
                            debug!("Dependent {} already gone", dependent.key);
                        }
                        Err(e) => error!("Failed to delete dependent {}: {}", dependent.key, e),
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns true if `owner_uid` still has a **blocking** dependent in
    /// storage — one whose ownerReference to it sets
    /// `blockOwnerDeletion: true`.
    ///
    /// Used to gate foreground-deletion finalizer removal: the owner's
    /// `foregroundDeletion` finalizer must stay (and the owner must not be
    /// deleted) until every blocking dependent has actually been removed from
    /// storage, not merely had a delete issued against it.
    ///
    /// `blockOwnerDeletion` is the whole point of the field and upstream gates
    /// on precisely that subset — `node.blockingDependents()`
    /// (pkg/controller/garbagecollector/graph.go:178):
    ///
    /// ```text
    /// for _, owner := range dep.owners {
    ///     if owner.UID == n.identity.UID && owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion {
    ///         ret = append(ret, dep)
    ///     }
    /// }
    /// ```
    ///
    /// with `processDeletingDependentsItem` (garbagecollector.go:654) dropping
    /// the finalizer as soon as `len(blockingDependents) == 0`. A nil
    /// `BlockOwnerDeletion` does not block, so `None` here does not either.
    /// Counting every dependent instead held an owner open on dependents that
    /// never asked to block it (#1829).
    async fn still_has_dependents(&self, owner_uid: &str) -> rusternetes_common::Result<bool> {
        let all_resources = self.get_all_resources().await?;
        Ok(all_resources.iter().any(|r| {
            r.metadata.owner_references.as_ref().is_some_and(|refs| {
                refs.iter()
                    .any(|oref| oref.uid == owner_uid && oref.block_owner_deletion == Some(true))
            })
        }))
    }

    /// Orphan dependents by removing owner references.
    /// Finds all resources that have the deleted resource as an owner,
    /// then removes the owner reference to the deleted resource from each.
    async fn orphan_dependents(
        &self,
        resource: &ResourceInfo,
        _dependent_map: &HashMap<String, Vec<String>>,
    ) -> rusternetes_common::Result<()> {
        let resource_uid = &resource.metadata.uid;

        // Find all resources that reference this resource as an owner
        let all_resources = self.get_all_resources().await?;
        let dependents: Vec<_> = all_resources
            .iter()
            .filter(|r| {
                r.metadata
                    .owner_references
                    .as_ref()
                    .is_some_and(|refs| refs.iter().any(|oref| oref.uid == *resource_uid))
            })
            .collect();

        if dependents.is_empty() {
            debug!("No dependents to orphan for resource {}", resource.key);
            return Ok(());
        }

        info!(
            "Orphan deletion: removing owner references from {} dependents of {}",
            dependents.len(),
            resource.key
        );

        // Remove owner references from each dependent
        for dependent in dependents {
            info!("Orphaning dependent {}", dependent.key);

            // Parse the dependent's full object
            let mut dependent_value = dependent.value.clone();

            // Remove the owner reference to this resource
            if let Some(metadata) = dependent_value.get_mut("metadata") {
                if let Some(owner_refs) = metadata.get_mut("ownerReferences") {
                    if let Some(owner_refs_array) = owner_refs.as_array_mut() {
                        // Filter out the owner reference matching this resource
                        owner_refs_array.retain(|owner_ref| {
                            owner_ref
                                .get("uid")
                                .and_then(|uid| uid.as_str())
                                .map(|uid| uid != resource_uid)
                                .unwrap_or(true)
                        });

                        // If no more owner references, remove the field entirely
                        if owner_refs_array.is_empty() {
                            if let Some(metadata_obj) = metadata.as_object_mut() {
                                metadata_obj.remove("ownerReferences");
                            }
                        }
                    }
                }
            }

            // Update the dependent in storage
            if let Err(e) = self
                .storage
                .update_raw(&dependent.key, &dependent_value)
                .await
            {
                error!("Failed to orphan dependent {}: {}", dependent.key, e);
                // Return error so the orphan finalizer is NOT removed.
                // This prevents the race where the owner is deleted while
                // dependents still have ownerReferences pointing to it.
                return Err(rusternetes_common::Error::Internal(format!(
                    "Failed to orphan dependent {}: {}",
                    dependent.key, e
                )));
            }
        }

        Ok(())
    }

    /// Cascade delete all resources in a namespace
    #[allow(dead_code)]
    async fn cascade_delete_namespace(
        &self,
        namespace: &ResourceInfo,
        all_resources: &[ResourceInfo],
    ) -> rusternetes_common::Result<()> {
        let namespace_name = &namespace.metadata.name;
        info!("Cascading delete for namespace: {}", namespace_name);

        // Find all resources in this namespace
        let resources_in_namespace: Vec<_> = all_resources
            .iter()
            .filter(|r| {
                r.metadata.namespace.as_deref() == Some(namespace_name)
                    && r.resource_type != "namespaces"
            })
            .collect();

        // Delete all resources in the namespace
        for resource in resources_in_namespace {
            info!(
                "Deleting {} {} in namespace {}",
                resource.resource_type, resource.metadata.name, namespace_name
            );
            if let Err(e) = self.storage.delete(&resource.key).await {
                error!(
                    "Failed to delete {} {}: {}",
                    resource.resource_type, resource.metadata.name, e
                );
            }
        }

        // If no resources left in namespace and no finalizers, delete the namespace
        if !namespace.metadata.has_finalizers() {
            info!("Deleting namespace: {}", namespace_name);
            self.storage.delete(&namespace.key).await?;
        }

        Ok(())
    }

    /// Detect cycles in the dependency graph
    /// Returns Ok(()) if no cycles, Err with cycle info if found
    fn detect_cycles(
        &self,
        dependent_map: &HashMap<String, Vec<String>>,
        resources: &[ResourceInfo],
    ) -> Result<(), String> {
        // Build UID to resource name map for better error messages
        let uid_to_name: HashMap<_, _> = resources
            .iter()
            .map(|r| (r.metadata.uid.clone(), r.metadata.name.clone()))
            .collect();

        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        // DFS cycle detection
        for resource in resources {
            if !visited.contains(&resource.metadata.uid) {
                if let Some(cycle_path) = self.detect_cycle_dfs(
                    &resource.metadata.uid,
                    dependent_map,
                    &mut visited,
                    &mut rec_stack,
                    &mut Vec::new(),
                    &uid_to_name,
                ) {
                    return Err(format!("Cycle detected in ownership chain: {}", cycle_path));
                }
            }
        }

        Ok(())
    }

    /// DFS helper for cycle detection
    #[allow(clippy::only_used_in_recursion)]
    fn detect_cycle_dfs(
        &self,
        uid: &str,
        dependent_map: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
        path: &mut Vec<String>,
        uid_to_name: &HashMap<String, String>,
    ) -> Option<String> {
        visited.insert(uid.to_string());
        rec_stack.insert(uid.to_string());
        path.push(uid.to_string());

        // Check all owners of this resource (dependents -> owners)
        if let Some(owners) = dependent_map.get(uid) {
            for owner_uid in owners {
                // Skip self-references — a resource owning itself is not a cycle
                if owner_uid == uid {
                    continue;
                }
                if !visited.contains(owner_uid) {
                    if let Some(cycle) = self.detect_cycle_dfs(
                        owner_uid,
                        dependent_map,
                        visited,
                        rec_stack,
                        path,
                        uid_to_name,
                    ) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(owner_uid) {
                    // Cycle detected! Build human-readable path
                    let cycle_start_idx = path.iter().position(|u| u == owner_uid).unwrap();
                    let cycle_path: Vec<_> = path[cycle_start_idx..]
                        .iter()
                        .map(|uid| {
                            uid_to_name
                                .get(uid)
                                .map(|name| name.as_str())
                                .unwrap_or("unknown")
                        })
                        .collect();
                    return Some(format!("{} -> {}", cycle_path.join(" -> "), cycle_path[0]));
                }
            }
        }

        path.pop();
        rec_stack.remove(uid);
        None
    }

    /// Delete a batch of orphans with retry logic (no re-verification).
    /// NOTE: For orphan deletion, use `delete_orphan()` which re-verifies
    /// owner existence before deleting. This method is kept for non-orphan
    /// batch deletions where re-verification is not needed.
    #[allow(dead_code)]
    async fn delete_batch_with_retry(&self, orphans: &[ResourceInfo]) -> Vec<Result<(), String>> {
        use futures::future::join_all;

        // Limit concurrency
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent_deletes));
        let mut tasks = Vec::new();

        for orphan in orphans {
            let sem = Arc::clone(&semaphore);
            let storage = Arc::clone(&self.storage);
            let orphan_clone = orphan.clone();
            let max_retries = self.max_retries;

            let task = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                // Retry with exponential backoff
                let mut attempt = 0;
                let mut last_error = None;

                while attempt < max_retries {
                    match storage.delete(&orphan_clone.key).await {
                        Ok(_) => {
                            info!(
                                "Successfully deleted orphan {} (attempt {})",
                                orphan_clone.key,
                                attempt + 1
                            );
                            return Ok(());
                        }
                        Err(e) => {
                            attempt += 1;
                            last_error = Some(e.to_string());

                            if attempt < max_retries {
                                // Exponential backoff: 100ms, 200ms, 400ms, ...
                                let backoff_ms = 100 * (1 << attempt);
                                debug!(
                                    "Failed to delete {} (attempt {}), retrying in {}ms: {}",
                                    orphan_clone.key, attempt, backoff_ms, e
                                );
                                sleep(Duration::from_millis(backoff_ms)).await;
                            }
                        }
                    }
                }

                Err(format!(
                    "Failed to delete {} after {} attempts: {}",
                    orphan_clone.key,
                    max_retries,
                    last_error.unwrap_or_else(|| "unknown error".to_string())
                ))
            });

            tasks.push(task);
        }

        // Wait for all tasks and collect results
        let results = join_all(tasks).await;
        results
            .into_iter()
            .map(|r| r.unwrap_or_else(|e| Err(format!("Task panicked: {}", e))))
            .collect()
    }
}

/// Information about a resource for GC purposes
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct ResourceInfo {
    /// Storage key for the resource
    key: String,
    /// Resource metadata
    metadata: ObjectMeta,
    /// Resource type (e.g., "pods", "deployments")
    resource_type: String,
    /// Full resource value
    value: Value,
}

/// Cascade deletion options
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteOptions {
    /// Deletion propagation policy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub propagation_policy: Option<DeletionPropagation>,

    /// Grace period seconds before deletion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grace_period_seconds: Option<i64>,

    /// Preconditions for deletion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preconditions: Option<Preconditions>,

    /// Whether to orphan dependents (deprecated, use propagation_policy)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orphan_dependents: Option<bool>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Preconditions {
    /// UID must match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,

    /// ResourceVersion must match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self {
            propagation_policy: Some(DeletionPropagation::Background),
            grace_period_seconds: Some(30),
            preconditions: None,
            orphan_dependents: None,
        }
    }
}

/// Map K8s Kind names to their plural storage resource names.
/// K8s uses discovery API for this; we use a static mapping.
fn kind_to_plural(kind: &str) -> &str {
    match kind {
        "Pod" => "pods",
        "Service" => "services",
        "Endpoints" => "endpoints",
        "EndpointSlice" => "endpointslices",
        "Namespace" => "namespaces",
        "Node" => "nodes",
        "ConfigMap" => "configmaps",
        "Secret" => "secrets",
        "ServiceAccount" => "serviceaccounts",
        "Deployment" => "deployments",
        "ReplicaSet" => "replicasets",
        "StatefulSet" => "statefulsets",
        "DaemonSet" => "daemonsets",
        "ReplicationController" => "replicationcontrollers",
        "Job" => "jobs",
        "CronJob" => "cronjobs",
        "Ingress" => "ingresses",
        "NetworkPolicy" => "networkpolicies",
        "PersistentVolumeClaim" => "persistentvolumeclaims",
        "PersistentVolume" => "persistentvolumes",
        "StorageClass" => "storageclasses",
        "ClusterRole" => "clusterroles",
        "ClusterRoleBinding" => "clusterrolebindings",
        "Role" => "roles",
        "RoleBinding" => "rolebindings",
        "CustomResourceDefinition" => "customresourcedefinitions",
        "ControllerRevision" => "controllerrevisions",
        "HorizontalPodAutoscaler" => "horizontalpodautoscalers",
        "PodDisruptionBudget" => "poddisruptionbudgets",
        "ResourceQuota" => "resourcequotas",
        "LimitRange" => "limitranges",
        _ => {
            // Fallback: lowercase + "s" (works for many K8s kinds)
            // This is imperfect but better than failing
            tracing::warn!("GC: unknown kind '{}', using lowercase+s fallback", kind);
            // Return a static str — caller should handle the fallback case
            // We can't return a dynamically constructed string as &str,
            // so return an empty string to signal "unknown"
            ""
        }
    }
}

/// Compute the next GC scan interval with idle exponential backoff.
///
/// Mirrors upstream garbage collector's workqueue rate-limiter
/// (`pkg/controller/garbagecollector/garbagecollector.go`: `Forget` on an
/// actionable item, `AddRateLimited` otherwise): a scan that did work resets
/// to `base`; an idle scan doubles the interval, clamped to `max`. Rusternetes
/// has no dependency-graph informer to make GC fully event-driven (tracked in
/// #1039), so a scan-based GC approximates "silent when idle" (#1040) by
/// stretching the poll interval when there is nothing to collect.
/// Whether an object keeps unfinished finalizers in `spec`, where
/// [`ObjectMeta::has_finalizers`] cannot see them.
///
/// Only namespaces do this. Upstream gates their final removal on it in two
/// places: `Delete` returns the object untouched while they remain —
///
/// ```text
/// // prior to final deletion, we must ensure that finalizers is empty
/// if len(namespace.Spec.Finalizers) != 0 {
///     return namespace, false, nil
/// }
/// ```
///
/// (`pkg/registry/core/namespace/storage/storage.go:250-253`) — and the update
/// path overrides the generic hook this sweep stands in for (#1828):
///
/// ```text
/// func ShouldDeleteNamespaceDuringUpdate(ctx, key, obj, existing) bool {
///     ...
///     return len(ns.Spec.Finalizers) == 0 &&
///         genericregistry.ShouldDeleteDuringUpdate(ctx, key, obj, existing)
/// }
/// ```
///
/// (same file, `:257-265`). Without the override the sweep read only
/// `metadata.finalizers` — empty on every namespace — decided a Terminating
/// namespace was finished, and issued a DELETE per scan that the api-server
/// correctly refused, logging "no finalizers remaining" on a loop (#1846).
fn spec_finalizers_remain(resource_type: &str, value: &Value) -> bool {
    if resource_type != "namespaces" {
        return false;
    }
    value
        .pointer("/spec/finalizers")
        .and_then(|f| f.as_array())
        .is_some_and(|f| !f.is_empty())
}

/// Whether a storage watch event should kick a GC scan ahead of its poll.
///
/// Upstream GC reacts to every event because its graph builder holds a cached
/// dependency graph, so a status write costs a map update
/// (`pkg/controller/garbagecollector/graph_builder.go` `processGraphChanges`).
/// Ours has no graph (#1039) — a scan re-LISTs 28 resource types — so only the
/// events that can actually create GC work are allowed to trigger one:
///
/// * a **delete**, which is what orphans a dependent and what
///   `processGraphChanges` turns into `attemptToDelete` over `n.dependents`;
/// * a **modify that stamps `deletionTimestamp`**, i.e. the start of a graceful
///   or foreground deletion, which is the scan's `process_deletion` arm.
///
/// Everything else (creates, status writes) waits for the poll, preserving the
/// idle-quiet property the backoff buys (#1040).
fn event_warrants_scan(event: &rusternetes_storage::WatchEvent) -> bool {
    use rusternetes_storage::WatchEvent;
    match event {
        WatchEvent::Deleted(..) => true,
        WatchEvent::Modified(_, value) => value_has_deletion_timestamp(value),
        WatchEvent::Added(..) => false,
    }
}

/// Whether a serialised object carries a `metadata.deletionTimestamp`.
///
/// The substring test is a cheap gate so the common case (a status write) never
/// pays for a parse; the parse is what actually decides, so a payload that only
/// happens to contain the word does not kick a scan.
fn value_has_deletion_timestamp(value: &str) -> bool {
    if !value.contains("deletionTimestamp") {
        return false;
    }
    serde_json::from_str::<Value>(value)
        .ok()
        .and_then(|v| {
            v.pointer("/metadata/deletionTimestamp")
                .map(|t| !t.is_null())
        })
        .unwrap_or(false)
}

fn next_scan_interval(
    current: Duration,
    did_work: bool,
    base: Duration,
    max: Duration,
) -> Duration {
    if did_work {
        base
    } else {
        // Double, then clamp to [base, max]. saturating_mul avoids overflow.
        current.saturating_mul(2).min(max).max(base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::OwnerReference;
    use rusternetes_storage::memory::MemoryStorage;

    #[test]
    fn gc_scan_interval_backs_off_when_idle_and_resets_on_work() {
        let base = Duration::from_secs(5);
        let max = Duration::from_secs(60);

        // Idle scans back off exponentially, clamped at max.
        let mut i = base;
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, Duration::from_secs(10));
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, Duration::from_secs(20));
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, Duration::from_secs(40));
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, max, "must clamp at max (80s -> 60s)");
        i = next_scan_interval(i, false, base, max);
        assert_eq!(i, max, "stays at max while idle");

        // A scan that did work resets to the base cadence immediately.
        i = next_scan_interval(i, true, base, max);
        assert_eq!(i, base, "work resets to base");

        // Never drops below base; a single idle scan from base doubles it.
        assert_eq!(
            next_scan_interval(base, false, base, max),
            Duration::from_secs(10)
        );
        assert_eq!(next_scan_interval(base, true, base, max), base);
    }

    #[tokio::test]
    async fn test_deletion_propagation_policy() {
        let gc = GarbageCollector::new(Arc::new(MemoryStorage::new()));

        // Test foreground deletion
        let mut metadata = ObjectMeta::new("test");
        metadata.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        assert_eq!(
            gc.determine_propagation_policy(&metadata),
            DeletionPropagation::Foreground
        );

        // Test orphan deletion
        metadata.finalizers = Some(vec!["orphan".to_string()]);
        assert_eq!(
            gc.determine_propagation_policy(&metadata),
            DeletionPropagation::Orphan
        );

        // Test default (background)
        metadata.finalizers = None;
        assert_eq!(
            gc.determine_propagation_policy(&metadata),
            DeletionPropagation::Background
        );
    }

    #[test]
    fn test_owner_reference_creation() {
        let owner_ref = OwnerReference::new("v1", "Pod", "my-pod", "abc-123")
            .with_controller(true)
            .with_block_owner_deletion(true);

        assert_eq!(owner_ref.kind, "Pod");
        assert_eq!(owner_ref.controller, Some(true));
        assert_eq!(owner_ref.block_owner_deletion, Some(true));
    }

    #[test]
    fn test_metadata_finalizer_helpers() {
        let mut metadata = ObjectMeta::new("test");

        // Test add_finalizer
        metadata.add_finalizer("my-finalizer".to_string());
        assert!(metadata.has_finalizers());
        assert_eq!(metadata.finalizers.as_ref().unwrap().len(), 1);

        // Test idempotent add
        metadata.add_finalizer("my-finalizer".to_string());
        assert_eq!(metadata.finalizers.as_ref().unwrap().len(), 1);

        // Test remove_finalizer
        metadata.remove_finalizer("my-finalizer");
        assert!(!metadata.has_finalizers());
    }

    /// A foreground deletion inside an ownership CYCLE must still complete.
    ///
    /// Three pods each own the next and each carries `blockOwnerDeletion: true`,
    /// so every member blocks its owner. Deleting one with foreground
    /// propagation deadlocks a naive implementation: pod1 waits for pod2, pod2
    /// waits for pod3, pod3 waits for pod1.
    ///
    /// Upstream breaks the ring in `attemptToDeleteItem`
    /// (`pkg/controller/garbagecollector/garbagecollector.go:595-620`): when an
    /// item has a dependent that is ITSELF deleting dependents, it patches its
    /// own ownerReferences to be non-blocking and proceeds with the foreground
    /// delete. The comment there calls the check a deliberate false-positive-
    /// prone circle detection.
    ///
    /// Pins `[sig-api-machinery] Garbage collector should not be blocked by
    /// dependency circle [Conformance]`
    /// (`test/e2e/apimachinery/garbage_collector.go:826-880`), which builds this
    /// exact three-pod ring and requires ALL of them to disappear.
    #[tokio::test]
    async fn foreground_deletion_breaks_an_ownership_cycle() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        // pod1 <- pod2 <- pod3 <- pod1: each owned by the previous one.
        let uids = ["pod1-uid", "pod2-uid", "pod3-uid"];
        let names = ["pod1", "pod2", "pod3"];

        for i in 0..3 {
            // owner is the PREVIOUS pod in the ring
            let owner_idx = (i + 2) % 3;
            let mut meta = ObjectMeta::new(names[i]);
            meta.namespace = Some("default".to_string());
            meta.uid = uids[i].to_string();
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                name: names[owner_idx].to_string(),
                uid: uids[owner_idx].to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            // The one being deleted enters foreground deletion.
            if i == 0 {
                meta.deletion_timestamp = Some(chrono::Utc::now());
                meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
            }
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            let key = format!("/registry/pods/default/{}", names[i]);
            storage.create(&key, &pod).await.unwrap();
        }

        // A bounded number of scans must drain the whole ring. Upstream needs
        // several passes too (each pass unblocks/deletes one more member), but
        // it must CONVERGE rather than spin forever.
        for _ in 0..10 {
            gc.scan_and_collect().await.unwrap();
            let remaining: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
            if remaining.is_empty() {
                return;
            }
        }

        let remaining: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let names: Vec<&str> = remaining.iter().map(|p| p.metadata.name.as_str()).collect();
        panic!("ownership cycle deadlocked foreground deletion; still present: {names:?}");
    }

    /// Foreground deletion must NOT remove the `foregroundDeletion` finalizer
    /// (and thus must not delete the owner) while any blocking dependent still
    /// exists in storage.
    ///
    /// Reproduces `[sig-api-machinery] Garbage collector should keep the rc
    /// around until all its pods are deleted if the deleteOptions says so`
    /// (garbage_collector.go:711), which deletes the rc with
    /// PropagationPolicy=Foreground and then asserts there are zero pods once
    /// the rc is gone. The previous code removed the finalizer in the same pass
    /// it issued the dependent deletes — gated only on the delete calls
    /// returning, not on the dependents actually being gone — so a stale
    /// snapshot (pods still draining or created after the snapshot) let the rc
    /// be deleted while pods remained. `still_has_dependents` is the gate.
    #[tokio::test]
    async fn still_has_dependents_blocks_until_dependent_gone() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let owner_uid = "rc-foreground-uid";

        let mut pod_meta = ObjectMeta::new("dependent-pod");
        pod_meta.namespace = Some("default".to_string());
        pod_meta.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
            name: "simpletest.rc".to_string(),
            uid: owner_uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: None,
            status: None,
        };
        let pod_key = "/registry/pods/default/dependent-pod";
        storage.create(pod_key, &pod).await.unwrap();

        assert!(
            gc.still_has_dependents(owner_uid).await.unwrap(),
            "owner with an owned pod still in storage must report remaining dependents"
        );

        // Pod drained — gate must clear so the owner can be finalized/deleted.
        storage.delete(pod_key).await.unwrap();
        assert!(
            !gc.still_has_dependents(owner_uid).await.unwrap(),
            "once the owned pod is gone, the owner must report no remaining dependents"
        );
    }

    /// Once all dependents drain, the foreground gate must clear: the owner's
    /// `foregroundDeletion` finalizer is removed and the owner deleted. Guards
    /// the gate against deadlocking the happy path.
    #[tokio::test]
    async fn foreground_owner_deleted_after_dependents_drain() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let owner_uid = "rc-fg-complete-uid";

        // Owner rc: being foreground-deleted (deletionTimestamp + finalizer).
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        rc_meta.deletion_timestamp = Some(chrono::Utc::now());
        rc_meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        let rc = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 1},
        });
        let rc_key = "/registry/replicationcontrollers/default/simpletest.rc";
        storage.create(rc_key, &rc).await.unwrap();

        // One pod owned solely by the rc.
        let mut pod_meta = ObjectMeta::new("rc-pod");
        pod_meta.namespace = Some("default".to_string());
        pod_meta.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
            name: "simpletest.rc".to_string(),
            uid: owner_uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: None,
            status: None,
        };
        storage
            .create("/registry/pods/default/rc-pod", &pod)
            .await
            .unwrap();

        // First scan: deletes the pod, gate clears (0 dependents), finalizer
        // removed, rc deleted — all in one scan since the snapshot is complete.
        gc.scan_and_collect().await.unwrap();

        let pods: Vec<Pod> = storage
            .list("/registry/pods/default/")
            .await
            .unwrap_or_default();
        assert!(
            pods.is_empty(),
            "dependent pod must be deleted, {} left",
            pods.len()
        );

        let rc_after: rusternetes_common::Result<Value> = storage.get(rc_key).await;
        assert!(
            rc_after.is_err(),
            "rc must be deleted once its dependents are gone (foreground complete)"
        );
    }

    /// The exact Bug-B race: an in-flight pod the RC controller created just
    /// before observing the owner's deletion lands AFTER the foreground delete
    /// pass took its snapshot. The owner's `foregroundDeletion` finalizer must
    /// NOT be removed (owner must NOT be deleted) while that pod still exists —
    /// only once it drains. This is what
    /// `[sig-api-machinery] Garbage collector should keep the rc around until
    /// all its pods are deleted` (garbage_collector.go:711) asserts; the old
    /// code removed the finalizer in the same pass and deleted the rc with pods
    /// still present.
    #[tokio::test]
    async fn foreground_gate_holds_for_inflight_pod_after_delete_pass() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());
        let owner_uid = "rc-fg-inflight-uid";

        // rc being foreground-deleted.
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        rc_meta.deletion_timestamp = Some(chrono::Utc::now());
        rc_meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        let rc_key = "/registry/replicationcontrollers/default/simpletest.rc";
        let rc_val = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 1},
        });
        storage.create(rc_key, &rc_val).await.unwrap();

        // Helper: a pod owned solely by the rc.
        let mk_pod = |name: &str| Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some("default".to_string());
                m.owner_references = Some(vec![OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "ReplicationController".to_string(),
                    name: "simpletest.rc".to_string(),
                    uid: owner_uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]);
                m
            },
            spec: None,
            status: None,
        };

        // pod_a is present when the foreground delete pass takes its snapshot.
        let pod_a_key = "/registry/pods/default/rc-pod-a";
        storage
            .create(pod_a_key, &mk_pod("rc-pod-a"))
            .await
            .unwrap();

        let rc_info = ResourceInfo {
            key: rc_key.to_string(),
            metadata: rc_meta.clone(),
            resource_type: "replicationcontrollers".to_string(),
            value: rc_val.clone(),
        };
        let empty_dep_map: HashMap<String, Vec<String>> = HashMap::new();

        // 1) Foreground delete pass: deletes the dependents it saw (pod_a).
        gc.delete_dependents_foreground(&rc_info, &empty_dep_map)
            .await
            .unwrap();
        assert!(
            storage.get::<Value>(pod_a_key).await.is_err(),
            "pod_a should have been deleted by the foreground pass"
        );

        // 2) An in-flight pod the RC controller created before noticing the
        //    deletion lands AFTER the delete pass's snapshot.
        let pod_b_key = "/registry/pods/default/rc-pod-b";
        storage
            .create(pod_b_key, &mk_pod("rc-pod-b"))
            .await
            .unwrap();

        // 3) GATE (the protection this pass provides): a dependent still exists,
        //    so the foreground finalizer must NOT be removed and the rc must be
        //    retained. The old code removed it here and deleted the rc.
        assert!(
            gc.still_has_dependents(owner_uid).await.unwrap(),
            "gate must report remaining dependents while the in-flight pod exists"
        );
        assert!(
            storage.get::<Value>(rc_key).await.is_ok(),
            "rc must be retained while the in-flight pod_b still exists"
        );

        // 4) pod_b drains; a subsequent scan deletes any remaining dependent and,
        //    finding the gate clear, finally collects the rc.
        storage.delete(pod_b_key).await.unwrap();
        gc.scan_and_collect().await.unwrap();
        assert!(
            storage.get::<Value>(rc_key).await.is_err(),
            "rc must be collected once every dependent has drained"
        );
    }

    /// A single GC scan must remove an orphan whose owner is already gone.
    /// Conformance tests for orphan pod cleanup observe per-cycle latency,
    /// so the previous "2-scan grace" gating added a full reconcile cycle
    /// of wait before any orphan was reaped. `delete_orphan` already re-reads
    /// the owner from storage as a race guard, so the second scan was
    /// unnecessary and observably slow.
    #[tokio::test]
    async fn test_orphan_deleted_on_first_scan() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let mut m = ObjectMeta::new("orphan-pod");
        m.namespace = Some("default".to_string());
        m.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: "missing-rs".to_string(),
            uid: "missing-rs-uid-never-existed".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: m,
            spec: None,
            status: None,
        };
        storage
            .create("/registry/pods/default/orphan-pod", &pod)
            .await
            .unwrap();

        gc.scan_and_collect().await.unwrap();

        let pods: Vec<Pod> = storage
            .list("/registry/pods/default/")
            .await
            .unwrap_or_default();
        assert!(
            pods.is_empty(),
            "orphan must be deleted on the first GC scan, got {} remaining",
            pods.len()
        );
    }

    /// One GC scan must drain a whole ownership CHAIN, not one level of it.
    ///
    /// `[sig-api-machinery] Garbage collector should not be blocked by
    /// dependency circle` (test/e2e/apimachinery/garbage_collector.go:826)
    /// builds pod1 -> pod3 -> pod2 -> pod1, deletes pod1, and gives the cluster
    /// 150s for ALL THREE to disappear. The pods carry
    /// terminationGracePeriodSeconds=0, so the api-server removes pod1 at once
    /// and the GC inherits a 2-long dangling chain: pod2 (owner pod1, gone) and
    /// pod3 (owner pod2, still present).
    ///
    /// Deleting only the orphans present in this scan's snapshot peels one
    /// level per scan, so the chain needs as many scans as it is long. Each
    /// scan lists every resource type across the cluster and the interval backs
    /// off to 60s while idle — under conformance load that ran past the spec's
    /// 150s budget and left pod3 behind (run 33155140545, 2026-08-28).
    ///
    /// Upstream does not wait for its next sweep: deleting a dependent feeds
    /// the graph builder, which enqueues everything that pointed at it, so a
    /// chain collapses transitively in one go
    /// (pkg/controller/garbagecollector/graph_builder.go `processGraphChanges`
    /// → `attemptToDelete` enqueue of `n.dependents`). A scan-based GC gets the
    /// same property by re-running the orphan pass until it reaches a fixpoint.
    #[tokio::test]
    async fn one_scan_drains_a_whole_orphan_chain() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        // pod2 -> pod1 (already deleted by the api-server), pod3 -> pod2.
        let chain = [("pod2", "pod1", "pod1-uid"), ("pod3", "pod2", "pod2-uid")];
        for (name, owner_name, owner_uid) in chain {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some("gc-ns".to_string());
            meta.uid = format!("{name}-uid");
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                name: owner_name.to_string(),
                uid: owner_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/pods/gc-ns/{name}"), &pod)
                .await
                .unwrap();
        }

        gc.scan_and_collect().await.unwrap();

        let remaining: Vec<Pod> = storage.list("/registry/pods/gc-ns/").await.unwrap();
        let names: Vec<&str> = remaining.iter().map(|p| p.metadata.name.as_str()).collect();
        assert!(
            remaining.is_empty(),
            "a single scan must collect the whole chain, not one level per scan; still present: {names:?}"
        );
    }

    /// The object whose foreground deletion started the walk must never be
    /// removed from storage BY the walk. In an ownership cycle the walk comes
    /// back around to it (pod3's dependent is pod1, the object being deleted);
    /// deleting it there takes it out from behind its own finalizers.
    ///
    /// Upstream never does this: `attemptToDeleteItem` returns immediately for
    /// an item that is already being deleted, and an item's own
    /// `foregroundDeletion` finalizer is only dropped by
    /// `processDeletingDependentsItem` once its blocking dependents are gone
    /// (pkg/controller/garbagecollector/garbagecollector.go). A third-party
    /// finalizer makes the difference observable: the pod must survive the scan.
    #[tokio::test]
    async fn foreground_cycle_does_not_delete_the_walk_root_behind_its_finalizers() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let uids = ["root-uid", "mid-uid", "tail-uid"];
        let names = ["root", "mid", "tail"];

        for i in 0..3 {
            let owner_idx = (i + 2) % 3;
            let mut meta = ObjectMeta::new(names[i]);
            meta.namespace = Some("default".to_string());
            meta.uid = uids[i].to_string();
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                name: names[owner_idx].to_string(),
                uid: uids[owner_idx].to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            if i == 0 {
                meta.deletion_timestamp = Some(chrono::Utc::now());
                // A third-party finalizer alongside the GC one: nothing in the
                // GC is allowed to remove the object while this is present.
                meta.finalizers = Some(vec![
                    "foregroundDeletion".to_string(),
                    "example.com/blocker".to_string(),
                ]);
            }
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/pods/default/{}", names[i]), &pod)
                .await
                .unwrap();
        }

        gc.scan_and_collect().await.unwrap();

        let root: rusternetes_common::Result<Value> =
            storage.get("/registry/pods/default/root").await;
        let root = root.expect("root must survive: its third-party finalizer is still set");
        let finalizers: Vec<String> = root
            .pointer("/metadata/finalizers")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        assert!(
            finalizers.contains(&"example.com/blocker".to_string()),
            "the third-party finalizer must remain, got {finalizers:?}"
        );
    }

    /// A pod in graceful termination — `deletionTimestamp` and a grace period
    /// set, no finalizers — belongs to the kubelet, which removes it once the
    /// container actually stops. The GC must leave it alone until then.
    ///
    /// Upstream returns immediately for exactly this shape, in
    /// `attemptToDeleteItem`
    /// (pkg/controller/garbagecollector/garbagecollector.go:511):
    ///
    /// ```text
    /// // "being deleted" is an one-way trip to the final deletion. We'll just
    /// // wait for the final deletion, and then process the object's dependents.
    /// if item.isBeingDeleted() && !item.isDeletingDependents() {
    ///     return nil
    /// }
    /// ```
    ///
    /// `isDeletingDependents` is the `foregroundDeletion` finalizer, so an
    /// object carrying no GC finalizer is never the GC's to remove. Ours
    /// deleted it on the next scan, racing the kubelet's shutdown and taking
    /// the pod out of the API before its grace period had run.
    ///
    /// The sweep is deferred, not dropped: it is still the only thing that
    /// finishes an object whose last finalizer is removed by a PUT, since
    /// `ShouldDeleteDuringUpdate` is implemented on the PATCH path alone.
    /// #1828 tracks retiring it.
    #[tokio::test]
    async fn gc_leaves_a_gracefully_terminating_pod_alone() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let mut meta = ObjectMeta::new("terminating-pod");
        meta.namespace = Some("default".to_string());
        meta.uid = "terminating-pod-uid".to_string();
        meta.deletion_timestamp = Some(chrono::Utc::now());
        meta.deletion_grace_period_seconds = Some(30);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: meta,
            spec: None,
            status: None,
        };
        let key = "/registry/pods/default/terminating-pod";
        storage.create(key, &pod).await.unwrap();

        gc.scan_and_collect().await.unwrap();

        let after: rusternetes_common::Result<Value> = storage.get(key).await;
        assert!(
            after.is_ok(),
            "a terminating pod inside its grace period must survive the GC scan — \
             the kubelet removes it when the container stops"
        );
    }

    /// The same sweep must still fire once the grace period has run out: it is
    /// the only thing that finishes an object whose last finalizer was removed
    /// by a PUT (see `deletion_grace_period_elapsed`).
    #[tokio::test]
    async fn gc_still_sweeps_a_pod_past_its_grace_period() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let mut meta = ObjectMeta::new("expired-pod");
        meta.namespace = Some("default".to_string());
        meta.uid = "expired-pod-uid".to_string();
        meta.deletion_timestamp = Some(chrono::Utc::now() - chrono::Duration::seconds(120));
        meta.deletion_grace_period_seconds = Some(30);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: meta,
            spec: None,
            status: None,
        };
        let key = "/registry/pods/default/expired-pod";
        storage.create(key, &pod).await.unwrap();

        gc.scan_and_collect().await.unwrap();

        let after: rusternetes_common::Result<Value> = storage.get(key).await;
        assert!(
            after.is_err(),
            "a pod whose grace period expired long ago must still be swept"
        );
    }

    /// The foreground gate counts *blocking* dependents only — those whose
    /// ownerReference to the owner sets `blockOwnerDeletion: true`.
    ///
    /// Upstream gates on exactly that set. `node.blockingDependents()`
    /// (pkg/controller/garbagecollector/graph.go:178):
    ///
    /// ```text
    /// for _, owner := range dep.owners {
    ///     if owner.UID == n.identity.UID && owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion {
    ///         ret = append(ret, dep)
    ///     }
    /// }
    /// ```
    ///
    /// and `processDeletingDependentsItem` (garbagecollector.go:654) drops the
    /// `foregroundDeletion` finalizer as soon as that set is empty. A dependent
    /// that does not ask to block its owner must not hold the owner open.
    #[tokio::test]
    async fn foreground_gate_counts_blocking_dependents_only() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());
        let owner_uid = "gate-owner-uid";

        let make_pod = |name: &str, block: Option<bool>| {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some("default".to_string());
            meta.uid = format!("{name}-uid");
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "ReplicationController".to_string(),
                name: "gate-owner".to_string(),
                uid: owner_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: block,
            }]);
            Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            }
        };

        // blockOwnerDeletion explicitly false: not a blocking dependent.
        storage
            .create(
                "/registry/pods/default/non-blocking",
                &make_pod("non-blocking", Some(false)),
            )
            .await
            .unwrap();
        // Field absent: upstream treats a nil BlockOwnerDeletion as not blocking.
        storage
            .create("/registry/pods/default/unset", &make_pod("unset", None))
            .await
            .unwrap();

        assert!(
            !gc.still_has_dependents(owner_uid).await.unwrap(),
            "dependents that do not set blockOwnerDeletion: true must not hold \
             the owner's foregroundDeletion finalizer open"
        );

        // One blocking dependent is enough to hold the gate.
        storage
            .create(
                "/registry/pods/default/blocking",
                &make_pod("blocking", Some(true)),
            )
            .await
            .unwrap();
        assert!(
            gc.still_has_dependents(owner_uid).await.unwrap(),
            "a dependent with blockOwnerDeletion: true must hold the gate"
        );
    }

    /// The foreground cascade must not re-LIST the whole cluster once per
    /// dependent.
    ///
    /// `delete_dependents_foreground` recursed into every sole-owned dependent
    /// and each level called `get_all_resources()`, which LISTs all 28 resource
    /// types. Deleting an rc with N pods therefore cost N + 3 full passes —
    /// ~1484 LIST operations for the 50 sole-owned pods of
    /// `[sig-api-machinery] Garbage collector should not delete dependents that
    /// have both valid owner and owner that's waiting for dependents to be
    /// deleted [Serial]`, per scan, while that same spec's 90s budget was
    /// waiting on the cascade to finish (#1836).
    ///
    /// Upstream never pays this: its GC walks an in-memory dependency graph
    /// maintained by informers (pkg/controller/garbagecollector/graph_builder.go)
    /// and does no listing during a cascade at all. One snapshot per cascade is
    /// the closest equivalent that keeps our scan-based design.
    #[tokio::test]
    async fn foreground_cascade_takes_one_list_pass_not_one_per_dependent() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let owner_uid = "rc-many-pods-uid";
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        rc_meta.deletion_timestamp = Some(chrono::Utc::now());
        rc_meta.finalizers = Some(vec!["foregroundDeletion".to_string()]);
        let rc = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 20},
        });
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest.rc",
                &rc,
            )
            .await
            .unwrap();

        const PODS: usize = 20;
        for i in 0..PODS {
            let name = format!("rc-pod-{i}");
            let mut meta = ObjectMeta::new(name.clone());
            meta.namespace = Some("default".to_string());
            meta.uid = format!("{name}-uid");
            meta.owner_references = Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "ReplicationController".to_string(),
                name: "simpletest.rc".to_string(),
                uid: owner_uid.to_string(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/pods/default/{name}"), &pod)
                .await
                .unwrap();
        }

        gc.scan_and_collect().await.unwrap();

        // The cascade still has to work.
        let pods: Vec<Pod> = storage
            .list("/registry/pods/default/")
            .await
            .unwrap_or_default();
        assert!(
            pods.is_empty(),
            "all {PODS} pods must be collected, {} left",
            pods.len()
        );

        // One pass to open the scan, one for the cascade, one for the
        // foreground gate's fresh re-read. Emphatically not one per pod.
        let passes = gc.list_pass_count();
        assert!(
            passes <= 4,
            "foreground cascade of {PODS} dependents took {passes} full-cluster \
             list passes; it must not scale with the number of dependents"
        );
    }

    /// Only deletion signals may kick a scan.
    ///
    /// Upstream's graph builder reacts to every event, but it keeps a cached
    /// graph so a status write is cheap
    /// (`pkg/controller/garbagecollector/graph_builder.go`
    /// `processGraphChanges`). Our scan re-LISTs 28 resource types, so kicking
    /// on an Added or a plain Modified would turn every pod status write into
    /// a full-cluster pass and undo the idle-quiet property (#1040).
    ///
    /// A delete orphans dependents; a Modified that stamps `deletionTimestamp`
    /// starts a graceful/foreground deletion the scan's `process_deletion` arm
    /// owns. Everything else waits for the poll.
    #[test]
    fn watch_event_warrants_scan_only_for_deletion_signals() {
        use rusternetes_storage::WatchEvent;

        let live = r#"{"metadata":{"name":"p","uid":"u"}}"#.to_string();
        let terminating =
            r#"{"metadata":{"name":"p","uid":"u","deletionTimestamp":"2026-09-02T23:23:14Z"}}"#
                .to_string();

        assert!(event_warrants_scan(&WatchEvent::Deleted(
            "/registry/services/default/svc".to_string(),
            live.clone()
        )));
        assert!(event_warrants_scan(&WatchEvent::Modified(
            "/registry/namespaces/ns-1842".to_string(),
            terminating.clone()
        )));

        assert!(!event_warrants_scan(&WatchEvent::Added(
            "/registry/pods/default/p".to_string(),
            live.clone()
        )));
        assert!(!event_warrants_scan(&WatchEvent::Modified(
            "/registry/pods/default/p".to_string(),
            live
        )));
        // An Added of an already-terminating object is still not a new signal:
        // the object was created being-deleted only in a relist, and the poll
        // arm picks it up.
        assert!(!event_warrants_scan(&WatchEvent::Added(
            "/registry/pods/default/p".to_string(),
            terminating
        )));
        // Unparseable payloads must not kick — a malformed value is not a
        // deletion signal.
        assert!(!event_warrants_scan(&WatchEvent::Modified(
            "/registry/pods/default/p".to_string(),
            "not json, has deletionTimestamp in it though".to_string()
        )));
    }

    /// The deletion-latency regression.
    ///
    /// The scan interval backs off while idle (5 -> 10 -> 20 -> 40 -> 60s), so
    /// an owner deleted just after an idle scan is not even *noticed* for up to
    /// `max_scan_interval`. Measured 2026-09-02 on the kine leg: the
    /// `[sig-api-machinery] Namespaces [Serial]` spec gave up at 23:23:14 and
    /// the GC's first delete attempt landed at 23:23:42 — 28s late. Budgets
    /// that lose: 30s (`wait.ForeverTestTimeout`, issue #1839) and 90s
    /// (the Namespaces [Serial] specs).
    ///
    /// Upstream never has this window: it is informer-driven, and a delete
    /// event enqueues the removed node's dependents directly
    /// (pkg/controller/garbagecollector/graph_builder.go `processGraphChanges`
    /// -> `attemptToDelete`). Port the reaction: a registry watch kicks the
    /// scan instead of waiting out the backoff.
    #[tokio::test]
    async fn watch_kick_collects_orphan_without_waiting_out_backoff() {
        use rusternetes_common::resources::Pod;
        use rusternetes_common::types::TypeMeta;

        let storage = Arc::new(MemoryStorage::new());
        // Base 30s / max 60s: any purely poll-driven GC needs >= 30s to look
        // again, so collecting inside the 5s assertion below can only happen
        // because the delete event woke it.
        let gc = Arc::new(GarbageCollector::with_scan_intervals(
            storage.clone(),
            Duration::from_secs(30),
            Duration::from_secs(60),
        ));

        let owner_uid = "rc-watch-kick-uid";
        let rc_key = "/registry/replicationcontrollers/default/simpletest.rc";
        let mut rc_meta = ObjectMeta::new("simpletest.rc");
        rc_meta.namespace = Some("default".to_string());
        rc_meta.uid = owner_uid.to_string();
        let rc = serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": serde_json::to_value(&rc_meta).unwrap(),
            "spec": {"replicas": 1},
        });
        storage.create(rc_key, &rc).await.unwrap();

        let pod_key = "/registry/pods/default/rc-pod";
        let mut pod_meta = ObjectMeta::new("rc-pod");
        pod_meta.namespace = Some("default".to_string());
        pod_meta.uid = "rc-watch-kick-pod-uid".to_string();
        pod_meta.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
            name: "simpletest.rc".to_string(),
            uid: owner_uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: None,
            status: None,
        };
        storage.create(pod_key, &pod).await.unwrap();

        let runner = Arc::clone(&gc);
        let handle = tokio::spawn(async move { runner.run().await });

        // Let the opening scan finish and settle into its (backed-off) sleep.
        // Nothing is collectable yet: the owner still exists.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            storage.get::<Value>(pod_key).await.is_ok(),
            "pod must survive while its owner lives"
        );

        storage.delete(rc_key).await.unwrap();

        let collected = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if storage.get::<Value>(pod_key).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        handle.abort();

        assert!(
            collected.is_ok(),
            "orphan must be collected off the owner's delete event, not after \
             the {:?} idle backoff",
            Duration::from_secs(60)
        );
    }

    /// A Terminating namespace still holding `spec.finalizers` must NOT be
    /// swept by the GC's no-finalizers backstop.
    ///
    /// `ObjectMeta::has_finalizers()` reads `metadata.finalizers`. A namespace
    /// keeps its finalizer in **`spec.finalizers`**, so the backstop concluded
    /// "no finalizers remaining" for every Terminating namespace and fired a
    /// DELETE on each scan. Against an api-server those are correctly refused
    /// (upstream `Delete` returns the object untouched while finalizers remain,
    /// pkg/registry/core/namespace/storage/storage.go:250-252), so the sweep
    /// just re-logged
    /// "Deleting resource (no finalizers remaining): /registry/namespaces/..."
    /// on a loop — observed 8 times in 93s against kine on a namespace that
    /// took 118s to finalize (#1846).
    ///
    /// Upstream states the rule as a namespace-specific override of exactly the
    /// hook this sweep stands in for (#1828):
    ///
    ///     func ShouldDeleteNamespaceDuringUpdate(...) bool {
    ///         return len(ns.Spec.Finalizers) == 0 &&
    ///             genericregistry.ShouldDeleteDuringUpdate(ctx, key, obj, existing)
    ///     }
    ///
    /// (same file, :257-265)
    #[tokio::test]
    async fn terminating_namespace_with_spec_finalizers_is_not_swept() {
        let storage = Arc::new(MemoryStorage::new());
        let gc = GarbageCollector::new(storage.clone());

        let key = "/registry/namespaces/ns-1846";
        let mut meta = ObjectMeta::new("ns-1846");
        meta.uid = "ns-1846-uid".to_string();
        meta.deletion_timestamp = Some(chrono::Utc::now());
        // Empty, exactly as a real namespace has it: the finalizer lives in
        // spec, not metadata.
        meta.finalizers = None;

        let namespace = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": serde_json::to_value(&meta).unwrap(),
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Terminating"},
        });
        storage.create(key, &namespace).await.unwrap();

        gc.scan_and_collect().await.unwrap();

        assert!(
            storage.get::<Value>(key).await.is_ok(),
            "namespace must survive the sweep while spec.finalizers is non-empty"
        );

        // Once the namespace controller has cleared spec.finalizers the
        // backstop must still finish the job, or clearing them would leak the
        // object. This half guards against "fixing" the bug by disabling the
        // sweep for namespaces outright.
        let mut finalized: Value = storage.get(key).await.unwrap();
        finalized["spec"]["finalizers"] = serde_json::json!([]);
        storage.update(key, &finalized).await.unwrap();

        gc.scan_and_collect().await.unwrap();

        assert!(
            storage.get::<Value>(key).await.is_err(),
            "namespace must be removed once spec.finalizers is empty"
        );
    }
}
