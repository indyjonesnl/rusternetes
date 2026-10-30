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

/// How many dependent operations the GC keeps in flight at once.
///
/// Upstream's `ConcurrentGCSyncs` default
/// (pkg/controller/garbagecollector/config/v1alpha1/defaults.go:38), which is
/// the number of `attemptToDelete` workers draining the dependent queue.
///
/// `orphanDependents` (garbagecollector.go:673-696) is in fact *unbounded* —
/// one goroutine per dependent, then `wg.Wait()`. We use upstream's own worker
/// count as the bound instead of copying the unbounded fan-out, because in
/// direct-storage mode there is no client rate limiter underneath us to meter
/// the result; 20 is the conservative choice that is still an upstream number.
/// Either way the point stands: these operations must overlap. Serially, the
/// 100 PATCHes the orphan conformance spec expects cost 100 round trips.
const GC_CONCURRENT_SYNCS: usize = 20;

/// Attempts allowed for a dependent write that loses a write race.
///
/// Upstream's `retry.DefaultRetry`
/// (staging/src/k8s.io/client-go/util/retry/util.go:28-33), used with
/// `RetryOnConflict` (`:103-105`) wherever a controller does
/// read-modify-write against an object something else may be writing.
const GC_CONFLICT_RETRY_ATTEMPTS: usize = 5;

/// Delay between those attempts. Upstream's `DefaultRetry.Duration`, with
/// `Factor: 1.0` — a flat 10ms, not a backoff.
const GC_CONFLICT_RETRY_DELAY: Duration = Duration::from_millis(10);

/// One unit of dependent work discovered by the foreground walk.
///
/// The walk is sequential (it threads a `visited` set through an ownership
/// graph that may contain cycles); the *I/O* is what has to overlap, so the
/// walk records what to do and a second pass runs it concurrently. Upstream
/// gets the same split for free: its graph builder enqueues nodes and 20
/// `attemptToDelete` workers drain the queue in arbitrary order.
pub enum DependentOp {
    /// Delete this dependent — its only owner is going away.
    Delete(String),
    /// Keep the dependent, drop its reference to the owner being deleted.
    ///
    /// Carries the owner's uid rather than a precomputed object: the new value
    /// is derived from a *fresh* read at execution time. Building it from the
    /// scan snapshot meant writing a stale object over whatever the kubelet had
    /// written in the meantime — which the api-server rejects outright, see
    /// [`GarbageCollector::run_dependent_ops`].
    StripOwnerRef { key: String, owner_uid: String },
    /// Re-verify that every owner of this dependent is gone, then delete it —
    /// one background orphan-collection item (see [`delete_orphan_verified`]).
    DeleteIfOrphan(Box<ResourceInfo>),
}

/// Delete an orphaned resource, but only after re-verifying the owner is gone.
///
/// The initial orphan detection uses a snapshot which can be stale — resources
/// created between the scan start and the orphan check won't be in the snapshot.
/// K8s GC re-reads the owner from the API server before deleting dependents:
/// see attemptToDeleteItem → getObject in garbagecollector.go:521.
///
/// Free function over `&S` (not a `&self` method) so the executor can run it
/// from a spawned task; see [`GarbageCollector::run_dependent_ops`].
///
/// We re-read both the dependent (to get fresh ownerRefs) and then look up
/// each owner by constructing the storage key from the ownerReference fields.
async fn delete_orphan_verified<S: Storage>(
    storage: &S,
    orphan: &ResourceInfo,
) -> rusternetes_common::Result<()> {
    // Re-read the resource from storage to get fresh ownerReferences.
    // It may have been updated since the scan snapshot.
    let fresh: Value = match storage.get(&orphan.key).await {
        Ok(v) => v,
        Err(rusternetes_common::Error::NotFound(_)) => return Ok(()), // already gone
        Err(e) => return Err(e),
    };
    let fresh_meta: ObjectMeta = match fresh
        .get("metadata")
        .and_then(|m| serde_json::from_value(m.clone()).ok())
    {
        Some(m) => m,
        None => return Ok(()), // can't parse metadata, skip
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
        match storage.get::<Value>(&owner_key).await {
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
    storage.delete(&orphan.key).await
}

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
    /// Dependent operations currently in flight, and the peak ever reached.
    ///
    /// The GC's deadline is set by how fast it can work through an owner's
    /// dependents, so whether those operations overlap is a correctness
    /// property, not a tuning detail — see [`GC_CONCURRENT_SYNCS`]. Counted
    /// so a regression back to a serial loop is a test failure.
    dependent_ops_inflight: Arc<std::sync::atomic::AtomicUsize>,
    dependent_ops_peak: Arc<std::sync::atomic::AtomicUsize>,
}

impl<S: Storage + 'static> GarbageCollector<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            scan_interval: Duration::from_secs(5),
            max_scan_interval: Duration::from_secs(60),
            pending_orphans: std::sync::Mutex::new(HashSet::new()),
            list_passes: std::sync::atomic::AtomicUsize::new(0),
            dependent_ops_inflight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            dependent_ops_peak: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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
        // do the same in `delete_orphan_verified`, which re-reads both the dependent
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
                // Each orphan is verified and deleted independently, so the
                // round fans out over the same bounded executor as the policy
                // paths. Upstream drains these through `ConcurrentGCSyncs` = 20
                // `attemptToDeleteItem` workers, one queue item each
                // (pkg/controller/garbagecollector/garbagecollector.go:172,
                // :521). The re-read of every owner inside each item is kept
                // as-is; it is overlapped across items, not skipped. The
                // re-list below still runs strictly after the round joins.
                let ops: Vec<DependentOp> = round_orphans
                    .iter()
                    .cloned()
                    .map(|o| DependentOp::DeleteIfOrphan(Box::new(o)))
                    .collect();
                let total = ops.len();
                let failures = self.run_dependent_ops(ops).await;
                failed_count += failures.len();
                deleted_count += total - failures.len();

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
    pub fn list_pass_count(&self) -> usize {
        self.list_passes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Highest number of dependent operations that were in flight at once.
    pub fn peak_concurrent_dependent_ops(&self) -> usize {
        self.dependent_ops_peak
            .load(std::sync::atomic::Ordering::SeqCst)
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
                        // PATCH path (crates/api-server/src/endpoints/handlers/),
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
    pub fn determine_propagation_policy(&self, metadata: &ObjectMeta) -> DeletionPropagation {
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
    pub async fn delete_dependents_foreground(
        &self,
        resource: &ResourceInfo,
        dependent_map: &HashMap<String, Vec<String>>,
    ) -> rusternetes_common::Result<()> {
        // ONE snapshot for the whole cascade. Re-listing per level made the
        // cost of deleting an owner scale with its dependent count — see
        // `delete_dependents_foreground_visiting`.
        let all_resources = self.get_all_resources().await?;
        let mut visited = HashSet::new();
        let mut ops = Vec::new();
        self.collect_dependents_foreground_visiting(
            resource,
            dependent_map,
            &all_resources,
            &mut visited,
            &mut ops,
        );

        // Failures are logged, not propagated: the foreground finalizer is
        // gated on `still_has_dependents` re-reading storage, so a lost write
        // costs a scan, never correctness.
        let _ = self.run_dependent_ops(ops).await;
        Ok(())
    }

    /// Run collected dependent operations concurrently, returning one message
    /// per operation that genuinely failed.
    ///
    /// Bounded at [`GC_CONCURRENT_SYNCS`]. `NotFound` is not a failure: every
    /// caller works from a snapshot, so a dependent something else already
    /// removed is expected — and either way it cannot still reference the
    /// owner. Upstream skips it for the same reason (garbagecollector.go:691)
    /// and collects the rest rather than returning on the first
    /// (garbagecollector.go:697-706).
    pub async fn run_dependent_ops(&self, ops: Vec<DependentOp>) -> Vec<String> {
        use futures::future::join_all;
        use std::sync::atomic::Ordering;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(GC_CONCURRENT_SYNCS));
        let mut tasks = Vec::with_capacity(ops.len());

        for op in ops {
            let sem = Arc::clone(&semaphore);
            let storage = Arc::clone(&self.storage);
            let inflight = Arc::clone(&self.dependent_ops_inflight);
            let peak = Arc::clone(&self.dependent_ops_peak);

            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.ok()?;

                // Record the overlap. Whether these operations run at once is
                // what decides if the GC meets its deadline, so a regression
                // back to a serial walk should be a test failure rather than a
                // slow nightly.
                let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);

                let outcome = match &op {
                    DependentOp::Delete(key) => match storage.delete(key).await {
                        Ok(_) => None,
                        Err(rusternetes_common::Error::NotFound(_)) => {
                            debug!("Dependent {} already gone", key);
                            None
                        }
                        Err(e) => {
                            error!("Failed to delete dependent {}: {}", key, e);
                            Some(format!("{key}: {e}"))
                        }
                    },
                    DependentOp::DeleteIfOrphan(orphan) => {
                        match delete_orphan_verified(storage.as_ref(), orphan).await {
                            Ok(()) => None,
                            Err(e) => {
                                error!("Failed to delete orphan {}: {}", orphan.key, e);
                                Some(format!("{}: {e}", orphan.key))
                            }
                        }
                    }
                    DependentOp::StripOwnerRef { key, owner_uid } => {
                        // Read-modify-write against a FRESH read, retrying if we
                        // lose the race.
                        //
                        // Upstream does not need this: it PATCHes away the one
                        // ownerReference
                        // (`GenerateDeleteOwnerRefStrategicMergeBytes`,
                        // pkg/controller/controller_ref_manager.go:572-589), so a
                        // concurrent write to an unrelated field is not a conflict
                        // at all. Our `Storage` trait has no patch verb, so this
                        // ports upstream's other sanctioned answer for
                        // read-modify-write — `retry.RetryOnConflict` with
                        // `DefaultRetry` (client-go/util/retry/util.go:28-33,
                        // 103-105).
                        //
                        // Building the object from the scan snapshot instead cost
                        // a conformance spec: the kubelet writes pod status
                        // continuously, so the snapshot was stale by the time the
                        // write landed and the api-server refused it. A failed
                        // strip leaves the dependent pointing at the owner, which
                        // keeps `still_has_dependents` true and pins the owner's
                        // `foregroundDeletion` finalizer until a later scan.
                        let mut outcome = None;
                        for attempt in 0..GC_CONFLICT_RETRY_ATTEMPTS {
                            let current: Value = match storage.get(key).await {
                                Ok(v) => v,
                                Err(rusternetes_common::Error::NotFound(_)) => {
                                    debug!("Dependent {} already gone, nothing to strip", key);
                                    break;
                                }
                                Err(e) => {
                                    error!("Failed to read dependent {}: {}", key, e);
                                    outcome = Some(format!("{key}: {e}"));
                                    break;
                                }
                            };

                            let stripped = without_owner_ref(current, owner_uid);
                            match storage.update_raw(key, &stripped).await {
                                Ok(()) => {
                                    outcome = None;
                                    break;
                                }
                                Err(rusternetes_common::Error::NotFound(_)) => {
                                    debug!("Dependent {} already gone, nothing to strip", key);
                                    outcome = None;
                                    break;
                                }
                                Err(rusternetes_common::Error::Conflict(msg))
                                    if attempt + 1 < GC_CONFLICT_RETRY_ATTEMPTS =>
                                {
                                    debug!(
                                        "Lost a write race stripping {} from {} \
                                         (attempt {}): {}",
                                        owner_uid,
                                        key,
                                        attempt + 1,
                                        msg
                                    );
                                    outcome = Some(format!("{key}: {msg}"));
                                    tokio::time::sleep(GC_CONFLICT_RETRY_DELAY).await;
                                }
                                Err(e) => {
                                    error!("Failed to update dependent {}: {}", key, e);
                                    outcome = Some(format!("{key}: {e}"));
                                    break;
                                }
                            }
                        }
                        outcome
                    }
                };

                inflight.fetch_sub(1, Ordering::SeqCst);
                outcome
            }));
        }

        join_all(tasks)
            .await
            .into_iter()
            .filter_map(|joined| match joined {
                Ok(outcome) => outcome,
                // A JoinError carries no per-operation outcome, so it cannot be
                // read as success. Swallowing it would drop the owner's
                // finalizer while a dependent still references it — the exact
                // race the aggregate below exists to prevent.
                Err(e) => Some(format!("dependent task did not complete: {e}")),
            })
            .collect()
    }

    /// Cycle-safe body of [`Self::delete_dependents_foreground`].
    ///
    /// `visited` holds the UIDs already descended into during THIS foreground
    /// pass. Ownership graphs may legitimately contain cycles — the upstream
    /// Conformance spec "Garbage collector should not be blocked by dependency
    /// circle" builds one deliberately — and without this the recursion never
    /// terminates: pod1 -> pod2 -> pod3 -> pod1 overflowed the stack and took
    /// the whole controller-manager process down with it.
    fn collect_dependents_foreground_visiting(
        &self,
        resource: &ResourceInfo,
        _dependent_map: &HashMap<String, Vec<String>>,
        all_resources: &[ResourceInfo],
        visited: &mut HashSet<String>,
        ops: &mut Vec<DependentOp>,
    ) {
        let resource_uid = &resource.metadata.uid;
        if !visited.insert(resource_uid.clone()) {
            debug!(
                "Foreground deletion: already descended into {} this pass (ownership cycle)",
                resource.key
            );
            return;
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
            return;
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
                    ops.push(DependentOp::StripOwnerRef {
                        key: dependent.key.clone(),
                        owner_uid: resource_uid.clone(),
                    });
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

                    // Descend first so a dependent's own dependents are
                    // recorded before it is. The ops themselves then run in
                    // arbitrary order, exactly as upstream's 20 workers drain
                    // their queue — deletion order is not what makes the
                    // cascade correct; the finalizer gate in `process_deletion`
                    // is.
                    let (_, sub_dependent_map) = self.build_relationship_maps(all_resources);
                    self.collect_dependents_foreground_visiting(
                        dependent,
                        &sub_dependent_map,
                        all_resources,
                        visited,
                        ops,
                    );

                    ops.push(DependentOp::Delete(dependent.key.clone()));
                }
            }
        }
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
    pub async fn still_has_dependents(&self, owner_uid: &str) -> rusternetes_common::Result<bool> {
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

        // Build the patched objects first (pure, no I/O), then fan the writes
        // out through the shared executor.
        //
        // Upstream starts one goroutine per dependent and waits for the lot
        // (pkg/controller/garbagecollector/garbagecollector.go:673-696):
        //
        // ```text
        // errCh := make(chan error, len(dependents))
        // wg := sync.WaitGroup{}
        // wg.Add(len(dependents))
        // for i := range dependents {
        //     go func(dependent *node) {
        //         defer wg.Done()
        //         ...
        // ```
        //
        // That concurrency is load-bearing for the deadline, not an
        // optimisation. The e2e spec budgets 120s to orphan 100 pods and its
        // own comment (test/e2e/apimachinery/garbage_collector.go:410-416)
        // explains why that is generous: 100 concurrent PATCHes metered by a
        // 5 QPS client limiter is ~20s. One hundred *sequential* PATCHes is
        // 100 round-trip latencies, which is what timed out `should orphan
        // pods created by rc if delete options say so [Serial] [Conformance]`.
        let ops: Vec<DependentOp> = dependents
            .iter()
            .map(|dependent| DependentOp::StripOwnerRef {
                key: dependent.key.clone(),
                owner_uid: resource_uid.clone(),
            })
            .collect();

        let failures = self.run_dependent_ops(ops).await;

        // Every dependent is attempted before anything is reported, as upstream
        // does with its buffered errCh, `wg.Wait()` and `NewAggregate`
        // (garbagecollector.go:697-706). Returning on the first failure left
        // the remaining dependents still pointing at an owner that was about to
        // be deleted.
        //
        // A non-empty result means the orphan finalizer stays on, so the owner
        // is not deleted while a dependent still references it.
        if !failures.is_empty() {
            return Err(rusternetes_common::Error::Internal(format!(
                "failed to orphan {} dependent(s) of {}: {}",
                failures.len(),
                resource.key,
                failures.join("; ")
            )));
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
}

/// Information about a resource for GC purposes
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ResourceInfo {
    /// Storage key for the resource
    pub key: String,
    /// Resource metadata
    pub metadata: ObjectMeta,
    /// Resource type (e.g., "pods", "deployments")
    pub resource_type: String,
    /// Full resource value
    pub value: Value,
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
/// The object with any ownerReference whose uid is `owner_uid` removed.
///
/// Drops `metadata.ownerReferences` entirely once it is empty, so a dependent
/// that has lost its last owner reads as "no owner" rather than "owner list of
/// length zero" — `find_orphans` treats those differently, and an object with
/// no ownerReferences is not garbage.
fn without_owner_ref(mut object: Value, owner_uid: &str) -> Value {
    if let Some(metadata) = object.get_mut("metadata") {
        if let Some(owner_refs) = metadata.get_mut("ownerReferences") {
            if let Some(arr) = owner_refs.as_array_mut() {
                arr.retain(|owner_ref| {
                    owner_ref
                        .get("uid")
                        .and_then(|uid| uid.as_str())
                        .map(|uid| uid != owner_uid)
                        .unwrap_or(true)
                });
                if arr.is_empty() {
                    if let Some(metadata_obj) = metadata.as_object_mut() {
                        metadata_obj.remove("ownerReferences");
                    }
                }
            }
        }
    }
    object
}

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
pub fn event_warrants_scan(event: &rusternetes_storage::WatchEvent) -> bool {
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

pub fn next_scan_interval(
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
