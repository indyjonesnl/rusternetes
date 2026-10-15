use super::expectations::ControllerExpectations;
use super::replicationcontroller::{
    generate_pod_name, is_namespace_terminating_rejection, slow_start_batches, BURST_REPLICAS,
    MAX_NAME_GENERATION_CREATE_ATTEMPTS, SLOW_START_INITIAL_BATCH_SIZE,
};
use crate::controllers::worker_pool::spawn_workers;
use futures::StreamExt;
use rusternetes_common::{
    resources::{Pod, PodStatus, ReplicaSet, ReplicaSetStatus},
    types::{ObjectMeta, Phase},
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WatchEvent, WorkQueue};
use std::{sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

/// Workers draining this controller's queue: upstream `ConcurrentRSSyncs` default,
/// pkg/controller/replicaset/config/v1alpha1/defaults.go:33-34, launched at cmd/kube-controller-manager/app/apps.go:116.
const CONCURRENT_RS_SYNCS: usize = 5;

/// ReplicaSetController reconciles ReplicaSet resources
/// A ReplicaSet ensures that a specified number of pod replicas are running at any given time
pub struct ReplicaSetController<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
    /// Per-ReplicaSet (keyed by "ns/name") expectations of in-flight pod
    /// creates/deletes. Prevents a burst of duplicate pods when the controller
    /// re-reconciles (on rapid pod watch events or resync) before a prior
    /// create/delete is reflected in a `list` — the read-after-write window of
    /// the storage backend. Without this, preemption churn made a replicas=1
    /// ReplicaSet spawn ~8 pods in 400ms (#542).
    ///
    /// Shared implementation in [`super::expectations`], ported from upstream
    /// `ControllerExpectations` (pkg/controller/controller_utils.go:150-300).
    /// This controller previously carried a private copy with the same
    /// semantics; it was extracted so the ReplicationController could use the
    /// same brake rather than the codebase growing a third copy.
    expectations: Arc<ControllerExpectations>,
}

impl<S: Storage + 'static> ReplicaSetController<S> {
    pub fn new(storage: Arc<S>, interval_secs: u64) -> Self {
        Self {
            storage,
            interval: Duration::from_secs(interval_secs),
            expectations: Arc::new(ControllerExpectations::new()),
        }
    }

    /// Key used to track expectations for a ReplicaSet ("ns/name").
    fn expectation_key(replicaset: &ReplicaSet) -> String {
        format!(
            "{}/{}",
            replicaset.metadata.namespace.as_deref().unwrap_or(""),
            replicaset.metadata.name
        )
    }

    /// True if the ReplicaSet has no outstanding (unobserved) create/delete
    /// operations, or its expectation has expired. When false, the controller
    /// must NOT issue further creates/deletes — it is still waiting to observe
    /// the pods from its previous action.
    fn expectations_satisfied(&self, key: &str) -> bool {
        self.expectations.satisfied(key)
    }

    /// Record that the controller is about to create `adds` and delete `dels`
    /// pods; reconciliation is then gated until they are observed.
    fn set_expectations(&self, key: &str, adds: i64, dels: i64) {
        self.expectations.set_expectations(key, adds, dels);
    }

    /// Lower the outstanding-create count after observing a pod creation (or
    /// after a create call failed, so we don't wait on a pod that never lands).
    fn observe_creation(&self, key: &str) {
        self.expectations.creation_observed(key);
    }

    /// Lower the outstanding-delete count after observing `pod_key` being
    /// deleted — either its deletionTimestamp being set or its final removal.
    /// Counted once per expected pod (`UIDTrackingControllerExpectations.
    /// DeletionObserved`, `pkg/controller/controller_utils.go:380-390`).
    fn observe_deletion(&self, key: &str, pod_key: &str) {
        self.expectations.deletion_observed_of(key, pod_key);
    }

    /// Drop all expectations for a ReplicaSet (e.g. when it is deleted).
    fn clear_expectations(&self, key: &str) {
        self.expectations.delete_expectations(key);
    }

    pub async fn run(self: Arc<Self>) -> rusternetes_common::Result<()> {
        info!("ReplicaSet controller started (watch-based)");

        let queue = WorkQueue::new();

        // Upstream launches `ConcurrentRSSyncs` workers over one shared queue
        // (`for i := 0; i < workers; i++ { go wait.UntilWithContext(ctx, worker, time.Second) }`);
        // a key in flight is never handed to a second worker.
        spawn_workers(CONCURRENT_RS_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch for changes to ReplicaSets AND Pods
            let prefix = build_prefix("replicasets", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish watch: {}, retrying in {:?}",
                        e, self.interval
                    );
                    tokio::time::sleep(self.interval).await;
                    continue;
                }
            };

            let pod_prefix = build_prefix("pods", None);
            let mut pod_watch = match self.storage.watch(&pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish pod watch: {}, retrying in {:?}",
                        e, self.interval
                    );
                    tokio::time::sleep(self.interval).await;
                    continue;
                }
            };

            // Periodic full resync as safety net
            let mut resync = tokio::time::interval(std::time::Duration::from_secs(5));
            resync.tick().await; // consume first immediate tick

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
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                self.enqueue_owner_replicaset(&queue, &ev).await;
                            }
                            Some(Err(e)) => {
                                warn!("Pod watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Pod watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
            // Watch broke — loop back to re-establish
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
            let storage_key = build_key("replicasets", Some(ns), name);
            match self.storage.get::<ReplicaSet>(&storage_key).await {
                Ok(resource) => match self.reconcile_replicaset(&resource).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // Resource was deleted — nothing to reconcile; drop any
                    // tracked expectations so the map doesn't leak entries.
                    self.clear_expectations(&format!("{}/{}", ns, name));
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<ReplicaSet>("/registry/replicasets/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("replicasets/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list replicasets for enqueue: {}", e);
            }
        }
    }

    /// When a pod changes, check its ownerReferences for a ReplicaSet owner,
    /// update that ReplicaSet's create/delete expectations, and enqueue it for
    /// reconciliation.
    ///
    /// The owner is parsed from the event's embedded value (the pod JSON, or
    /// the *previous* pod JSON for a Deleted event) rather than re-fetched from
    /// storage — a re-fetch races the delete and previously forced a fallback
    /// that enqueued every ReplicaSet in the namespace. Parsing the event value
    /// also lets us observe creations/deletions precisely:
    ///   * Added   → a create we were waiting for landed (`observe_creation`)
    ///   * Deleted → a delete we issued (or a preemption/GC delete) completed
    ///     (`observe_deletion`)
    async fn enqueue_owner_replicaset(&self, queue: &WorkQueue, event: &WatchEvent) {
        let pod_key = extract_key(event);
        let parts: Vec<&str> = pod_key.splitn(3, '/').collect();
        let ns = match parts.get(1) {
            Some(ns) => *ns,
            None => return,
        };

        let (value, is_add, is_del) = match event {
            WatchEvent::Added(_, v) => (v, true, false),
            WatchEvent::Modified(_, v) => (v, false, false),
            WatchEvent::Deleted(_, v) => (v, false, true),
        };

        // Extract ReplicaSet owner names from the pod JSON in the event.
        let owners: Vec<String> = serde_json::from_str::<Pod>(value)
            .ok()
            .and_then(|pod| pod.metadata.owner_references)
            .map(|refs| {
                refs.into_iter()
                    .filter(|r| r.kind == "ReplicaSet")
                    .map(|r| r.name)
                    .collect()
            })
            .unwrap_or_default();

        if owners.is_empty() {
            // Could not determine owner from the event value (e.g. malformed
            // or owner-less pod). Fall back to re-enqueuing every ReplicaSet in
            // the namespace so a relevant one is not missed; do not touch
            // expectations since we can't attribute the event.
            if let Ok(items) = self
                .storage
                .list::<ReplicaSet>(&build_prefix("replicasets", Some(ns)))
                .await
            {
                for rs in &items {
                    queue
                        .add(format!("replicasets/{}/{}", ns, rs.metadata.name))
                        .await;
                }
            }
            return;
        }

        // `updatePod` treats a pod whose deletionTimestamp is set like a
        // deletion (replica_set.go:468-480): a graceful delete MODIFIES the
        // pod first and the kubelet removes it only after the grace period,
        // yet the RS must replace it now.
        let terminating = !is_del
            && serde_json::from_str::<serde_json::Value>(value)
                .ok()
                .is_some_and(|v| {
                    v.pointer("/metadata/deletionTimestamp")
                        .is_some_and(|t| !t.is_null())
                });
        let pod_name = parts.get(2).copied().unwrap_or("");
        let observed_key = format!("{}/{}", ns, pod_name);

        for owner in owners {
            let exp_key = format!("{}/{}", ns, owner);
            if is_add {
                self.observe_creation(&exp_key);
            } else if is_del || terminating {
                self.observe_deletion(&exp_key, &observed_key);
            }
            queue.add(format!("replicasets/{}/{}", ns, owner)).await;
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> rusternetes_common::Result<()> {
        debug!("Reconciling all replicasets");

        // Get all replicasets
        let prefix = build_prefix("replicasets", None);
        let replicasets: Vec<ReplicaSet> = self.storage.list(&prefix).await?;

        for replicaset in replicasets {
            if let Err(e) = self.reconcile_replicaset(&replicaset).await {
                error!(
                    "Error reconciling replicaset {}: {}",
                    replicaset.metadata.name, e
                );
            }
        }

        Ok(())
    }

    async fn reconcile_replicaset(
        &self,
        replicaset: &ReplicaSet,
    ) -> rusternetes_common::Result<()> {
        let namespace = replicaset
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");

        debug!(
            "Reconciling replicaset: {}/{}",
            namespace, replicaset.metadata.name
        );

        // Read expectations BEFORE listing pods. Upstream does the same
        // (`rsNeedsSync := rsc.expectations.SatisfiedExpectations(logger, key)`,
        // pkg/controller/replicaset/replica_set.go:728, precedes the pod list)
        // and `TestRSSyncExpectations` pins why: checked after the list, a pod
        // arriving in between makes the record look fulfilled while the list
        // still lacks that pod, and the controller creates a duplicate.
        let exp_key = Self::expectation_key(replicaset);
        let needs_sync = self.expectations_satisfied(&exp_key);

        // Get all pods for this replicaset
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods: Vec<Pod> = self.storage.list(&pods_prefix).await?;

        // Adopt orphan pods: pods that match the selector labels but have no controller ownerReference
        // Release owned pods: pods owned by this RS but whose labels no longer match the selector
        let all_pods = self
            .adopt_and_release(replicaset, all_pods, namespace)
            .await?;

        // Filter pods that match this replicaset's selector (owned + matching labels)
        let replicaset_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|p| {
                // `FilterActivePods` (controller_utils.go:1004): a pod being
                // gracefully deleted is no longer a replica.
                if p.metadata.deletion_timestamp.is_some() {
                    return false;
                }
                let matches = self.matches_selector(p, replicaset);
                debug!(
                    "Pod {} matches selector: {} (labels: {:?})",
                    p.metadata.name, matches, p.metadata.labels
                );
                matches
            })
            .collect();

        // Count ready and available pods
        let ready_count = replicaset_pods
            .iter()
            .filter(|p| rusternetes_common::podutil::is_pod_ready(p))
            .count() as i32;

        let available_count = replicaset_pods
            .iter()
            .filter(|p| self.is_pod_available(p, replicaset))
            .count() as i32;

        let current_replicas = replicaset_pods.len() as i32;
        let desired_replicas = replicaset.spec.replicas;

        debug!(
            "ReplicaSet {}/{}: current={}, ready={}, available={}, desired={}",
            namespace,
            replicaset.metadata.name,
            current_replicas,
            ready_count,
            available_count,
            desired_replicas
        );

        // `manageReplicas` (replica_set.go:601-694), gated as `syncReplicaSet`
        // gates it: `if rsNeedsSync && rs.DeletionTimestamp == nil` (:756).
        // Acting while expectations are unmet is exactly what caused the #542
        // burst: a re-reconcile (rapid pod watch event or 5s resync) would
        // `list` before the prior create was visible, see current<desired
        // again, and create a duplicate.
        //
        // `manage_error` is upstream's `manageReplicasErr`: recorded as the
        // ReplicaFailure condition by `calculateStatus` and returned from the
        // sync AFTER the status update, so the workqueue retries with backoff.
        let mut manage_error: Option<String> = None;
        // Creates that actually succeeded; only these can ever be observed.
        let mut successful_creations = 0usize;
        if !needs_sync {
            debug!(
                "ReplicaSet {}/{}: expectations unmet, deferring create/delete this sync",
                namespace, replicaset.metadata.name
            );
        } else if replicaset.metadata.deletion_timestamp.is_none() {
            if current_replicas < desired_replicas {
                // `diff > rsc.burstReplicas` => `diff = rsc.burstReplicas`
                // (replica_set.go:610-612).
                let diff = ((desired_replicas - current_replicas) as usize).min(BURST_REPLICAS);
                info!(
                    "Creating {} pods for replicaset {}/{}",
                    diff, namespace, replicaset.metadata.name
                );
                // `ExpectCreations` BEFORE issuing the creates (:619), so a
                // reconcile triggered by our own pod-create watch events sees
                // an unmet expectation instead of re-issuing the batch.
                self.set_expectations(&exp_key, diff as i64, 0);
                // `slowStartBatch` (:820-844): batches of 1, 2, 4, ... each run
                // concurrently, stopping at the first batch with a failure and
                // reporting what succeeded.
                for batch in slow_start_batches(diff, SLOW_START_INITIAL_BATCH_SIZE) {
                    let results = futures::future::join_all(
                        (0..batch).map(|_| self.create_pod_for_batch(replicaset)),
                    )
                    .await;
                    let mut batch_error: Option<String> = None;
                    for result in results {
                        match result {
                            Ok(()) => successful_creations += 1,
                            Err(e) => {
                                error!(
                                    "Failed to create pod for replicaset {}/{}: {}",
                                    namespace, replicaset.metadata.name, e
                                );
                                batch_error.get_or_insert_with(|| e.to_string());
                            }
                        }
                    }
                    if batch_error.is_some() {
                        manage_error = batch_error;
                        break;
                    }
                }
                // Any skipped (or failed) pods that never landed shouldn't be
                // expected: "Decrement the expected number of creates because
                // the informer won't observe this pod" (:640-650).
                for _ in 0..diff.saturating_sub(successful_creations) {
                    self.observe_creation(&exp_key);
                }
            } else if current_replicas > desired_replicas {
                // Need to delete excess pods; capped at burstReplicas (:653-655).
                let diff = ((current_replicas - desired_replicas) as usize).min(BURST_REPLICAS);
                info!(
                    "Deleting {} excess pods for replicaset {}/{}",
                    diff, namespace, replicaset.metadata.name
                );
                // Snapshot the keys of the pods we expect to see deleted, so each
                // is recorded exactly once whether it is observed as a
                // deletionTimestamp update or as the delete (`ExpectDeletions`,
                // replica_set.go:670).
                let doomed: Vec<&Pod> = replicaset_pods.iter().take(diff).collect();
                let doomed_keys: Vec<String> = doomed
                    .iter()
                    .map(|p| format!("{}/{}", namespace, p.metadata.name))
                    .collect();
                self.expectations
                    .expect_deletions_of(&exp_key, &doomed_keys);
                // The deletes run concurrently and every one is attempted; only
                // the first error is reported (:672-694).
                let results = futures::future::join_all(
                    doomed
                        .iter()
                        .map(|pod| self.delete_pod(&pod.metadata.name, namespace)),
                )
                .await;
                for (result, pod_key) in results.into_iter().zip(&doomed_keys) {
                    if let Err(e) = result {
                        // The informer will not observe this deletion
                        // (replica_set.go:683-689).
                        self.observe_deletion(&exp_key, pod_key);
                        if !matches!(e, rusternetes_common::Error::NotFound(_)) {
                            manage_error.get_or_insert_with(|| e.to_string());
                        }
                    }
                }
            }
        }

        // Re-fetch and recount pods after create/delete operations to get accurate status
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods_after: Vec<Pod> = self.storage.list(&pods_prefix).await?;

        let replicaset_pods_after: Vec<Pod> = all_pods_after
            .into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .filter(|p| self.matches_selector(p, replicaset))
            .collect();

        let final_ready_count = replicaset_pods_after
            .iter()
            .filter(|p| rusternetes_common::podutil::is_pod_ready(p))
            .count() as i32;

        let final_available_count = replicaset_pods_after
            .iter()
            .filter(|p| self.is_pod_available(p, replicaset))
            .count() as i32;

        let final_current_replicas = replicaset_pods_after.len() as i32;

        // Lower expectations for the creates this re-list already shows.
        //
        // Upstream lowers them from informer events (replica_set.go:436
        // `CreationObserved`) because its sync reads a cache; ours reads
        // storage directly, so this re-list is an observation in its own right.
        // The pod watch lowers the count too, which covers the window before
        // this runs. Only creates that SUCCEEDED can ever be observed — the
        // failed/skipped ones were settled above — so re-arming the count for a
        // failed create would wedge the next sync until the expectations TTL.
        // Mirrors the sibling ReplicationController controller.
        if needs_sync && successful_creations > 0 {
            let newly_visible = (final_current_replicas as i64 - current_replicas as i64)
                .clamp(0, successful_creations as i64);
            for _ in 0..newly_visible {
                self.observe_creation(&exp_key);
            }
        }

        // Update status with accurate counts
        self.update_status(
            replicaset,
            final_current_replicas,
            final_ready_count,
            final_available_count,
            manage_error.as_deref(),
            replica_failure_reason(current_replicas, desired_replicas),
        )
        .await?;

        // `if manageReplicasErr != nil { return manageReplicasErr }` after the
        // status update (replica_set.go:765-767): the error requeues the key
        // with rate-limited backoff.
        if let Some(e) = manage_error {
            return Err(rusternetes_common::Error::Internal(e));
        }

        Ok(())
    }

    /// Check if a pod's labels match the ReplicaSet's selector (ignoring ownerReference)
    fn labels_match_selector(&self, pod: &Pod, replicaset: &ReplicaSet) -> bool {
        if let Some(match_labels) = &replicaset.spec.selector.match_labels {
            if let Some(pod_labels) = &pod.metadata.labels {
                for (key, value) in match_labels {
                    if pod_labels.get(key) != Some(value) {
                        return false;
                    }
                }
                return true;
            }
        }
        false
    }

    /// Check if a pod is owned by this ReplicaSet (has a controller ownerReference pointing to it).
    ///
    /// Mirrors `controller.IsControlledBy` from upstream
    /// `pkg/controller/replicaset/replica_set.go`: a pod is owned iff one of its
    /// `metadata.ownerReferences` has `controller=true` AND `uid == rs.UID`.
    /// UID matching is the upstream contract (the conformance test
    /// `ReplicaSet should adopt matching pods on creation and release no
    /// longer matching pods` polls for `owner.UID == rs.UID`). When the RS UID
    /// is empty (in-process tests that never go through the api-server) we
    /// fall back to name+kind matching so existing reconciler unit-tests still
    /// pass.
    fn is_owned_by(&self, pod: &Pod, replicaset: &ReplicaSet) -> bool {
        pod.metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter().any(|r| {
                    if r.controller != Some(true) {
                        return false;
                    }
                    if !replicaset.metadata.uid.is_empty() {
                        // UID match is the upstream-strict path
                        r.uid == replicaset.metadata.uid
                    } else {
                        // Fallback for unit-tests where the RS lacks a UID
                        r.kind == "ReplicaSet" && r.name == replicaset.metadata.name
                    }
                })
            })
            .unwrap_or(false)
    }

    /// Check if a pod has any controller ownerReference at all
    fn has_controller_owner(&self, pod: &Pod) -> bool {
        pod.metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.controller == Some(true)))
            .unwrap_or(false)
    }

    /// Adopt orphan pods that match the selector and release owned pods that no longer match.
    /// Returns the updated list of all pods (with ownerReferences modified as needed).
    async fn adopt_and_release(
        &self,
        replicaset: &ReplicaSet,
        mut all_pods: Vec<Pod>,
        namespace: &str,
    ) -> rusternetes_common::Result<Vec<Pod>> {
        #[allow(clippy::needless_range_loop)]
        for i in 0..all_pods.len() {
            let pod = &all_pods[i];

            // Skip terminated or deleting pods
            if let Some(ref status) = pod.status {
                if let Some(ref phase) = status.phase {
                    if matches!(phase, Phase::Failed | Phase::Succeeded) {
                        continue;
                    }
                }
            }
            if pod.metadata.deletion_timestamp.is_some() {
                continue;
            }

            let labels_match = self.labels_match_selector(pod, replicaset);
            let owned = self.is_owned_by(pod, replicaset);

            if labels_match && !owned && !self.has_controller_owner(pod) {
                // Adopt orphan pod: labels match, no controller owner
                let mut adopted_pod = pod.clone();
                let owner_ref = rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "ReplicaSet".to_string(),
                    name: replicaset.metadata.name.clone(),
                    uid: replicaset.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                };
                adopted_pod
                    .metadata
                    .owner_references
                    .get_or_insert_with(Vec::new)
                    .push(owner_ref);

                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                match self.storage.update(&pod_key, &adopted_pod).await {
                    Ok(_) => {
                        info!(
                            "Adopted orphan pod {} for replicaset {}/{}",
                            pod.metadata.name, namespace, replicaset.metadata.name
                        );
                        all_pods[i] = adopted_pod;
                    }
                    Err(e) => {
                        debug!("Failed to adopt pod {}: {}", pod.metadata.name, e);
                    }
                }
            } else if !labels_match && owned {
                // Release owned pod: labels no longer match.
                // Mirror `is_owned_by` and remove ALL controller refs whose
                // UID matches this RS (with name fallback for tests without
                // a UID). Upstream `release()` in `controllerRefManager.go`
                // uses UID matching exclusively.
                let mut released_pod = pod.clone();
                if let Some(refs) = &mut released_pod.metadata.owner_references {
                    let rs_uid = replicaset.metadata.uid.clone();
                    let rs_name = replicaset.metadata.name.clone();
                    refs.retain(|r| {
                        if r.controller != Some(true) {
                            return true;
                        }
                        let points_at_us = if !rs_uid.is_empty() {
                            r.uid == rs_uid
                        } else {
                            r.kind == "ReplicaSet" && r.name == rs_name
                        };
                        !points_at_us
                    });
                    if refs.is_empty() {
                        released_pod.metadata.owner_references = None;
                    }
                }

                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                match self.storage.update(&pod_key, &released_pod).await {
                    Ok(_) => {
                        info!(
                            "Released pod {} from replicaset {}/{}",
                            pod.metadata.name, namespace, replicaset.metadata.name
                        );
                        all_pods[i] = released_pod;
                    }
                    Err(e) => {
                        debug!("Failed to release pod {}: {}", pod.metadata.name, e);
                    }
                }
            }
        }

        Ok(all_pods)
    }

    fn matches_selector(&self, pod: &Pod, replicaset: &ReplicaSet) -> bool {
        // Skip pods that are terminated (Failed or Succeeded) — they don't count toward replicas
        if let Some(ref status) = pod.status {
            if let Some(ref phase) = status.phase {
                if matches!(phase, Phase::Failed | Phase::Succeeded) {
                    return false;
                }
            }
        }

        // Skip pods being deleted (have a deletionTimestamp)
        if pod.metadata.deletion_timestamp.is_some() {
            return false;
        }

        // Check owner reference — only count pods owned by this ReplicaSet.
        // Delegates to `is_owned_by` so name vs UID matching stays in one
        // place (upstream conformance polls for `owner.UID == rs.UID`).
        if !self.is_owned_by(pod, replicaset) {
            return false;
        }

        self.labels_match_selector(pod, replicaset)
    }

    fn is_pod_available(&self, pod: &Pod, replicaset: &ReplicaSet) -> bool {
        // K8s IsPodAvailable: Ready condition True + minReadySeconds + not terminating
        // Does NOT require phase == Running (a pod can be Ready before/during phase transitions)
        if !rusternetes_common::podutil::is_pod_ready(pod) {
            return false;
        }

        // Pod must not be terminating
        if pod.metadata.deletion_timestamp.is_some() {
            return false;
        }

        // Check if pod has been ready for minReadySeconds
        let min_ready_seconds = replicaset.spec.min_ready_seconds.unwrap_or(0);
        if min_ready_seconds > 0 {
            // Get pod creation time as a proxy for when it became ready
            // In a full implementation, we'd check the Ready condition's lastTransitionTime
            if let Some(creation_time) = pod.metadata.creation_timestamp {
                let now = chrono::Utc::now();
                let elapsed = now.signed_duration_since(creation_time);

                // Pod is available if it's been ready for at least minReadySeconds
                return elapsed.num_seconds() >= min_ready_seconds as i64;
            }
            // If no timestamp, can't determine availability
            false
        } else {
            // If minReadySeconds is 0, pod is available as soon as it's ready
            true
        }
    }

    async fn update_status(
        &self,
        replicaset: &ReplicaSet,
        replicas: i32,
        ready_replicas: i32,
        available_replicas: i32,
        failure_message: Option<&str>,
        failure_reason: Option<&str>,
    ) -> rusternetes_common::Result<()> {
        let namespace = replicaset
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");

        let key = build_key("replicasets", Some(namespace), &replicaset.metadata.name);

        // Base the write on the object this sync read, not a fresh re-read:
        // upstream `updateReplicaSetStatus`
        // (pkg/controller/replicaset/replica_set_utils.go) copies the rs it was
        // handed and calls `UpdateStatus` on it, so the resourceVersion
        // precondition covers the pod list the counts came from.
        let mut updated_rs: ReplicaSet = replicaset.clone();

        // `calculateStatus` (replica_set_utils.go:124-136): ReplicaFailure is
        // only SET when none exists, and only REMOVED when there is no error.
        // While a failure persists the existing condition is left alone, so its
        // lastTransitionTime and message are not rewritten every sync (the
        // `SetCondition` no-op on matching status+reason, :169-176, is what
        // keeps it stable). Conditions of other types are preserved.
        let mut conditions: Vec<rusternetes_common::resources::ReplicaSetCondition> = updated_rs
            .status
            .as_ref()
            .and_then(|s| s.conditions.clone())
            .unwrap_or_default();
        let has_failure_condition = conditions
            .iter()
            .any(|c| c.condition_type == "ReplicaFailure");
        match failure_message {
            Some(msg) if !has_failure_condition => {
                conditions.push(rusternetes_common::resources::ReplicaSetCondition {
                    condition_type: "ReplicaFailure".to_string(),
                    status: "True".to_string(),
                    last_transition_time: Some(chrono::Utc::now()),
                    reason: failure_reason.map(str::to_string),
                    message: Some(msg.to_string()),
                });
            }
            None if has_failure_condition => {
                conditions.retain(|c| c.condition_type != "ReplicaFailure");
            }
            _ => {}
        }
        let conditions = if conditions.is_empty() {
            None
        } else {
            Some(conditions)
        };

        let new_status = Some(ReplicaSetStatus {
            replicas,
            ready_replicas,
            available_replicas,
            fully_labeled_replicas: Some(replicas),
            observed_generation: updated_rs.metadata.generation,
            conditions,
            terminating_replicas: None,
        });

        // Only write status if it actually changed to avoid unnecessary storage writes
        // that trigger watch events and cause feedback loops
        if updated_rs.status != new_status {
            updated_rs.status = new_status;

            // Status subresource write, conditional on `replicaset`'s
            // resourceVersion (`update_status_cas`, #2153). A Conflict is
            // returned, not dropped: the worker requeues the key rate-limited
            // and the next sync recomputes from fresh state (upstream
            // `syncReplicaSet` returns the `updateReplicaSetStatus` error, and
            // the workqueue retries). NotFound means deleted mid-sync: done.
            match self.storage.update_status_cas(&key, &updated_rs).await {
                Ok(_) => {}
                Err(rusternetes_common::Error::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }

        debug!(
            "Updated status for replicaset {}/{}: replicas={}, ready={}, available={}",
            namespace, replicaset.metadata.name, replicas, ready_replicas, available_replicas
        );

        Ok(())
    }

    async fn create_pod(&self, replicaset: &ReplicaSet) -> rusternetes_common::Result<()> {
        let namespace = replicaset
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");

        // Name is generated per attempt below, so a collision can be retried
        // under a new one (upstream `createWithGenerateNameRetry`). Shared with
        // the ReplicationController: batches of hundreds of creates make a
        // collision of a 5-character suffix likely, not hypothetical.
        let pod_name = generate_pod_name(&replicaset.metadata.name);

        let mut metadata = ObjectMeta::new(&pod_name);
        metadata.namespace = Some(namespace.to_string());
        // Use template labels when present, otherwise fall back to selector
        // matchLabels so created pods can be matched by the controller on the
        // next reconcile. Per K8s validation, template labels must be a
        // superset of the selector — but a defensive fallback prevents
        // runaway pod creation if a malformed RS arrives with no template
        // labels.
        metadata.labels = replicaset
            .spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .or_else(|| replicaset.spec.selector.match_labels.clone());

        // Set owner reference so pods are garbage collected when ReplicaSet is deleted
        metadata.owner_references = Some(vec![rusternetes_common::types::OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: replicaset.metadata.name.clone(),
            uid: replicaset.metadata.uid.clone(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);

        let mut pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            spec: Some(replicaset.spec.template.spec.clone()),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                host_ip: None,
                pod_ip: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                host_i_ps: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                ..Default::default()
            }),
        };

        // Inject the ServiceAccount token volume exactly as the api-server's
        // admission does for HTTP pod creates. Controllers write pods straight
        // to storage and bypass that admission path, so without this a
        // controller-created pod (and anything running in it, e.g. an
        // aggregated apiserver — #225) has no
        // /var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt,namespace}.
        if let Some(spec) = pod.spec.as_mut() {
            let sa_name = rusternetes_common::serviceaccount::ensure_service_account_name(spec);
            let service_account = self
                .storage
                .get::<rusternetes_common::resources::ServiceAccount>(&build_key(
                    "serviceaccounts",
                    Some(namespace),
                    &sa_name,
                ))
                .await
                .ok();

            // Propagate the SA's imagePullSecrets (#1084) — controllers bypass
            // the api-server admission path that normally does this. Applies
            // regardless of automount.
            rusternetes_common::serviceaccount::propagate_image_pull_secrets(
                spec,
                service_account
                    .as_ref()
                    .and_then(|sa| sa.image_pull_secrets.as_deref()),
            );

            let sa_automount = service_account.and_then(|sa| sa.automount_service_account_token);
            let should_mount = match spec.automount_service_account_token {
                Some(v) => v,
                None => sa_automount.unwrap_or(true),
            };
            if should_mount {
                rusternetes_common::serviceaccount::add_kube_api_access_volume(spec);
            }

            // DefaultTolerationSeconds admission (#442): controllers bypass the
            // api-server admission path that adds these NoExecute tolerations.
            rusternetes_common::tolerations::add_default_tolerations(spec);
        }

        // Retry a name collision under a freshly generated name, as upstream's
        // api-server does for `generateName` objects
        // (registry/generic/registry/store.go:463-470). Any other error is
        // returned on the first attempt.
        let mut attempt_name = pod_name;
        for attempt in 0..MAX_NAME_GENERATION_CREATE_ATTEMPTS {
            pod.metadata.name = attempt_name.clone();
            let key = build_key("pods", Some(namespace), &attempt_name);
            match self.storage.create(&key, &pod).await {
                Ok(_) => {
                    info!(
                        "Created pod {}/{} for replicaset {}",
                        namespace, attempt_name, replicaset.metadata.name
                    );
                    return Ok(());
                }
                Err(rusternetes_common::Error::AlreadyExists(_))
                    if attempt + 1 < MAX_NAME_GENERATION_CREATE_ATTEMPTS =>
                {
                    attempt_name = generate_pod_name(&replicaset.metadata.name);
                }
                Err(e) => return Err(e),
            }
        }

        Err(rusternetes_common::Error::AlreadyExists(format!(
            "could not generate a free pod name for {}/{} after {} attempts",
            namespace, replicaset.metadata.name, MAX_NAME_GENERATION_CREATE_ATTEMPTS
        )))
    }

    /// The per-pod create `manageReplicas` hands to `slowStartBatch`
    /// (replica_set.go:624-632): a rejection because the namespace is
    /// terminating returns nil, so it is neither a failure nor a
    /// `ReplicaFailure` — "any creation will fail". Upstream matches the
    /// structured `NamespaceTerminatingCause`; see
    /// [`is_namespace_terminating_rejection`] for how it is recognised here.
    async fn create_pod_for_batch(
        &self,
        replicaset: &ReplicaSet,
    ) -> rusternetes_common::Result<()> {
        match self.create_pod(replicaset).await {
            Err(e) if is_namespace_terminating_rejection(&e) => Ok(()),
            other => other,
        }
    }

    async fn delete_pod(&self, name: &str, namespace: &str) -> rusternetes_common::Result<()> {
        let key = build_key("pods", Some(namespace), name);
        // Upstream `RealPodControl.DeletePod` is a graceful delete,
        // `Pods(ns).Delete(ctx, podID, metav1.DeleteOptions{})`
        // (pkg/controller/controller_utils.go:618): the pod gets a
        // deletionTimestamp and its grace period instead of vanishing.
        self.storage.delete_gracefully(&key).await?;

        info!("Deleted pod {}/{}", namespace, name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::PodSpec;
    use rusternetes_common::types::{LabelSelector, OwnerReference, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;
    use rusternetes_storage::Storage;
    use std::collections::HashMap;

    fn make_labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn make_replicaset(name: &str, labels: HashMap<String, String>, replicas: i32) -> ReplicaSet {
        ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace("default"),
            spec: rusternetes_common::resources::ReplicaSetSpec {
                replicas,
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels)),
                    spec: PodSpec {
                        containers: vec![],
                        ..Default::default()
                    },
                },
                min_ready_seconds: None,
            },
            status: None,
        }
    }

    fn make_pod(name: &str, labels: HashMap<String, String>) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name)
                .with_namespace("default")
                .with_labels(labels),
            spec: Some(PodSpec {
                containers: vec![],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_matches_selector() {
        let labels = make_labels(&[("app", "test")]);

        let pod_with_labels = make_pod("test-pod", labels);
        assert_eq!(pod_with_labels.metadata.name, "test-pod");
    }

    /// Regression for `[sig-apps] ReplicaSet should surface a failure condition
    /// on a common issue like exceeded quota`: a pod-creation failure must be
    /// surfaced as a `ReplicaFailure`/`FailedCreate` status condition, and
    /// cleared once creates succeed. (MemoryStorage can't deny on quota, so we
    /// exercise the status path directly.)
    #[tokio::test]
    async fn test_update_status_sets_and_clears_replica_failure() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);
        let labels = make_labels(&[("app", "rf")]);
        let rs = make_replicaset("rf-rs", labels, 1);
        let rs_key = build_key("replicasets", Some("default"), "rf-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        // Failure present → ReplicaFailure=True / FailedCreate.
        controller
            .update_status(
                &rs,
                0,
                0,
                0,
                Some("pods \"x\" is forbidden: exceeded quota"),
                Some("FailedCreate"),
            )
            .await
            .unwrap();
        let after: ReplicaSet = storage.get(&rs_key).await.unwrap();
        let cond = after
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.condition_type == "ReplicaFailure"))
            .expect("ReplicaFailure condition must be set on a pod-create failure");
        assert_eq!(cond.status, "True");
        assert_eq!(cond.reason.as_deref(), Some("FailedCreate"));
        assert!(cond.message.as_deref().unwrap_or("").contains("quota"));

        // Failure resolved → ReplicaFailure cleared.
        // A status write is conditional on the resourceVersion of the object it
        // was handed (#2153), so the second sync re-reads like a worker would.
        let rs: ReplicaSet = storage.get(&rs_key).await.unwrap();
        controller
            .update_status(&rs, 1, 1, 1, None, None)
            .await
            .unwrap();
        let after2: ReplicaSet = storage.get(&rs_key).await.unwrap();
        let has_rf = after2
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure"))
            .unwrap_or(false);
        assert!(!has_rf, "ReplicaFailure must clear once creates succeed");
    }

    /// `[sig-apps] ReplicaSet should surface a failure condition on a common
    /// issue like exceeded quota`: the rejection is the api-server's quota
    /// admission 403, not a client-side pre-check (#3618; upstream
    /// `manageReplicas` -> `calculateStatus` records the create error as
    /// `ReplicaFailure`). The controller must issue the create and surface
    /// the Forbidden it gets back, then clear the condition once it clears.
    #[tokio::test]
    async fn test_rs_replica_failure_from_apiserver_quota_forbidden() {
        let storage =
            Arc::new(crate::controllers::quota_admission_double::QuotaAdmissionStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);
        let labels = make_labels(&[("app", "q")]);
        let rs = make_replicaset("q-rs", labels, 2);
        let rs_key = build_key("replicasets", Some("default"), "q-rs");
        storage.create(&rs_key, &rs).await.unwrap();
        let quota = serde_json::json!({
            "apiVersion": "v1", "kind": "ResourceQuota",
            "metadata": {"name": "pq", "namespace": "default", "uid": "q-uid"},
            "spec": {"hard": {"pods": "0"}}
        });
        let quota_key = "/registry/resourcequotas/default/pq";
        storage.create(quota_key, &quota).await.unwrap();

        controller.reconcile_all().await.unwrap();

        assert!(
            storage
                .denied_creates
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0,
            "the controller must reach the api-server and be denied there"
        );
        let after: ReplicaSet = storage.get(&rs_key).await.unwrap();
        let cond = after
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.condition_type == "ReplicaFailure"))
            .expect("ReplicaFailure must be set from the api-server's Forbidden");
        assert_eq!(cond.reason.as_deref(), Some("FailedCreate"));
        assert!(cond
            .message
            .as_deref()
            .unwrap_or("")
            .contains("exceeded quota"));

        storage.delete(quota_key).await.unwrap();
        controller.reconcile_all().await.unwrap();
        let after: ReplicaSet = storage.get(&rs_key).await.unwrap();
        assert!(
            !after
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure")),
            "ReplicaFailure must clear once creates succeed"
        );
    }

    #[tokio::test]
    async fn test_adopt_orphan_pods_and_release_non_matching() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);

        let labels = make_labels(&[("app", "myapp")]);

        // Create an orphan pod with matching labels but no ownerReference
        let orphan_pod = make_pod("orphan-pod", labels.clone());
        let pod_key = build_key("pods", Some("default"), "orphan-pod");
        storage.create(&pod_key, &orphan_pod).await.unwrap();

        // Create a ReplicaSet with matching selector and replicas=1
        let rs = make_replicaset("my-rs", labels.clone(), 1);
        let rs_key = build_key("replicasets", Some("default"), "my-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        // Run reconciliation
        controller.reconcile_all().await.unwrap();

        // Verify the orphan pod was adopted (now has ownerReference pointing to the RS)
        let adopted_pod: Pod = storage.get(&pod_key).await.unwrap();
        let owner_refs = adopted_pod.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "ReplicaSet");
        assert_eq!(owner_refs[0].name, "my-rs");
        assert_eq!(owner_refs[0].controller, Some(true));
        assert_eq!(owner_refs[0].block_owner_deletion, Some(true));

        // Since the RS wants 1 replica and has adopted 1, no extra pods should be created
        let pods_prefix = build_prefix("pods", Some("default"));
        let all_pods: Vec<Pod> = storage.list(&pods_prefix).await.unwrap();
        assert_eq!(
            all_pods.len(),
            1,
            "Should have exactly 1 pod (the adopted one), got {}",
            all_pods.len()
        );

        // Now change the pod's labels so they no longer match the RS selector
        let mut modified_pod: Pod = storage.get(&pod_key).await.unwrap();
        modified_pod.metadata.labels = Some(make_labels(&[("app", "different")]));
        storage.update(&pod_key, &modified_pod).await.unwrap();

        // Run reconciliation again
        controller.reconcile_all().await.unwrap();

        // Verify the pod's ownerReference was removed (released)
        let released_pod: Pod = storage.get(&pod_key).await.unwrap();
        let has_rs_owner = released_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.kind == "ReplicaSet" && r.name == "my-rs")
            })
            .unwrap_or(false);
        assert!(
            !has_rs_owner,
            "Pod should no longer have ownerReference to RS after label change"
        );
    }

    #[tokio::test]
    async fn test_adopt_does_not_steal_owned_pods() {
        // Pods that already have a controller owner should NOT be adopted
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);

        let labels = make_labels(&[("app", "myapp")]);

        // Create a pod owned by a different controller
        let mut owned_pod = make_pod("owned-pod", labels.clone());
        owned_pod.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: "other-rs".to_string(),
            uid: "other-uid".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod_key = build_key("pods", Some("default"), "owned-pod");
        storage.create(&pod_key, &owned_pod).await.unwrap();

        // Create a RS with matching selector
        let rs = make_replicaset("my-rs", labels.clone(), 1);
        let rs_key = build_key("replicasets", Some("default"), "my-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        // Run reconciliation
        controller.reconcile_all().await.unwrap();

        // Verify the owned pod was NOT adopted (still owned by other-rs)
        let pod: Pod = storage.get(&pod_key).await.unwrap();
        let refs = pod.metadata.owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "other-rs");
    }

    #[tokio::test]
    async fn test_rs_creates_pods_with_selector_labels_when_template_has_none() {
        // Regression: a ReplicaSet whose template has no labels would create
        // pods with no labels, so the controller's matches_selector check
        // failed for its own pods, causing infinite pod creation. Fall back
        // to selector matchLabels when template labels are absent.
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 5);

        let labels = make_labels(&[("app", "myapp")]);

        // Build an RS where the pod template has no labels at all.
        let mut rs = make_replicaset("no-template-labels-rs", labels.clone(), 2);
        rs.spec.template.metadata = None;
        let rs_key = build_key("replicasets", Some("default"), "no-template-labels-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        controller.reconcile_all().await.unwrap();

        // Two pods should be created with selector labels applied so the
        // controller matches them on the next reconcile.
        let pods_prefix = build_prefix("pods", Some("default"));
        let pods: Vec<Pod> = storage.list(&pods_prefix).await.unwrap();
        assert_eq!(pods.len(), 2, "RS should create exactly 2 pods");
        for pod in &pods {
            let pod_labels = pod
                .metadata
                .labels
                .as_ref()
                .expect("Pod must inherit selector labels");
            assert_eq!(pod_labels.get("app"), Some(&"myapp".to_string()));
        }

        // Second reconcile must not create more pods (matching works).
        controller.reconcile_all().await.unwrap();
        let pods: Vec<Pod> = storage.list(&pods_prefix).await.unwrap();
        assert_eq!(
            pods.len(),
            2,
            "second reconcile must match existing pods, not create more"
        );
    }

    #[tokio::test]
    async fn test_rs_pods_inherit_sa_image_pull_secrets() {
        // #1084: SA imagePullSecrets must reach controller-created pods, which
        // bypass the api-server admission path.
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 5);

        let mut sa = rusternetes_common::resources::ServiceAccount::new("default", "default");
        sa.image_pull_secrets = Some(vec![
            rusternetes_common::resources::service_account::LocalObjectReference {
                name: "regcred".to_string(),
            },
        ]);
        storage
            .create("/registry/serviceaccounts/default/default", &sa)
            .await
            .unwrap();

        let labels = make_labels(&[("app", "pullsecrets")]);
        let rs = make_replicaset("pullsecrets-rs", labels, 1);
        let rs_key = build_key("replicasets", Some("default"), "pullsecrets-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 1);
        let secrets = pods[0]
            .spec
            .as_ref()
            .unwrap()
            .image_pull_secrets
            .as_ref()
            .expect("pod must inherit the SA's imagePullSecrets");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "regcred");
    }

    #[test]
    fn test_expectations_primitives() {
        let c = ReplicaSetController::new(Arc::new(MemoryStorage::new()), 30);
        // No entry → satisfied.
        assert!(c.expectations_satisfied("default/rs"));
        // Outstanding create → not satisfied until observed.
        c.set_expectations("default/rs", 2, 0);
        assert!(!c.expectations_satisfied("default/rs"));
        c.observe_creation("default/rs");
        assert!(!c.expectations_satisfied("default/rs"));
        c.observe_creation("default/rs");
        assert!(c.expectations_satisfied("default/rs"));
        // Outstanding delete → not satisfied until observed.
        c.expectations
            .expect_deletions_of("default/rs", &["default/p".to_string()]);
        assert!(!c.expectations_satisfied("default/rs"));
        c.observe_deletion("default/rs", "default/p");
        assert!(c.expectations_satisfied("default/rs"));
        // Clearing removes the entry (treated as satisfied).
        c.set_expectations("default/rs", 5, 0);
        c.clear_expectations("default/rs");
        assert!(c.expectations_satisfied("default/rs"));
    }

    /// The core #542 guard: while a ReplicaSet has unmet create expectations
    /// (pods created but not yet observed via the watch), a re-reconcile that
    /// momentarily sees too few pods MUST NOT create duplicates.
    #[tokio::test]
    async fn test_reconcile_defers_create_while_expectations_unmet() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let labels = make_labels(&[("name", "pod1")]);
        let rs = make_replicaset("rs1", labels.clone(), 2);
        let rs_key = build_key("replicasets", Some("default"), "rs1");
        storage.create(&rs_key, &rs).await.unwrap();

        // Simulate "we already issued 2 creates that haven't been observed yet"
        // — i.e. the storage list does not yet reflect them.
        controller.set_expectations("default/rs1", 2, 0);

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(
            pods.len(),
            0,
            "must not create pods while create expectations are unmet (got {})",
            pods.len()
        );

        // Once both creations are observed, the next reconcile may act.
        controller.observe_creation("default/rs1");
        controller.observe_creation("default/rs1");
        controller.reconcile_replicaset(&rs).await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(
            pods.len(),
            2,
            "after expectations satisfied, reconcile creates the desired pods"
        );
    }

    fn owned_pod(name: &str, labels: HashMap<String, String>, rs: &str, uid: &str) -> Pod {
        let mut pod = make_pod(name, labels);
        pod.metadata.owner_references = Some(vec![rusternetes_common::types::OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: rs.to_string(),
            uid: uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        pod
    }

    /// #2458: upstream `DeletePod` issues `Delete(ctx, podID, DeleteOptions{})`
    /// (`pkg/controller/controller_utils.go:618`), a graceful delete: the pod
    /// is stamped with a deletionTimestamp and survives for its grace period.
    /// The terminating pod must not count as a replica
    /// (`FilterActivePods`, `controller_utils.go:1004`), or the next sync would
    /// delete another pod.
    #[tokio::test]
    async fn test_scale_down_deletes_pods_gracefully() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let labels = make_labels(&[("app", "g")]);
        let rs = make_replicaset("g", labels.clone(), 1);
        storage
            .create(&build_key("replicasets", Some("default"), "g"), &rs)
            .await
            .unwrap();
        for n in ["p1", "p2", "p3"] {
            storage
                .create(
                    &build_key("pods", Some("default"), n),
                    &owned_pod(n, labels.clone(), "g", &rs.metadata.uid),
                )
                .await
                .unwrap();
        }

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 3, "graceful delete keeps the pods in storage");
        let terminating = pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
            .count();
        assert_eq!(terminating, 2);

        // A second sync sees 1 active pod == desired: nothing more to delete.
        controller.reconcile_replicaset(&rs).await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(
            pods.iter()
                .filter(|p| p.metadata.deletion_timestamp.is_some())
                .count(),
            2
        );
        let got: ReplicaSet = storage
            .get(&build_key("replicasets", Some("default"), "g"))
            .await
            .unwrap();
        assert_eq!(
            got.status.unwrap().replicas,
            1,
            "terminating pods are not replicas"
        );
    }

    /// #2458: a graceful delete first MODIFIES the pod (deletionTimestamp),
    /// and upstream `updatePod` observes the deletion there
    /// (`replica_set.go:468-480`) so the RS can create replacements without
    /// waiting for the kubelet's final DELETE. The later DELETED event for the
    /// same pod must not be counted twice (`UIDTrackingControllerExpectations`,
    /// `controller_utils.go:380-390`).
    #[tokio::test]
    async fn test_modified_with_deletion_timestamp_observes_deletion_once() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let labels = make_labels(&[("app", "g")]);
        controller.expectations.expect_deletions_of(
            "default/g",
            &["default/p1".to_string(), "default/p2".to_string()],
        );

        let mut pod = owned_pod("p1", labels, "g", "u");
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let json = serde_json::to_string(&pod).unwrap();
        let key = "/registry/pods/default/p1".to_string();

        controller
            .enqueue_owner_replicaset(&queue, &WatchEvent::Modified(key.clone(), json.clone()))
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((0, 1))
        );

        controller
            .enqueue_owner_replicaset(&queue, &WatchEvent::Deleted(key, json))
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((0, 1)),
            "the DELETED event of an already-observed pod must not count again"
        );
    }

    // ---------------------------------------------------------------------
    // manageReplicas / syncReplicaSet parity (#3619)
    //
    // Ported from upstream pkg/controller/replicaset/replica_set_test.go:
    // TestSlowStartBatch, TestSyncReplicaSetCreateFailures,
    // TestControllerBurstReplicas, TestRSSyncExpectations,
    // TestControllerUpdateStatusWithFailure and, from
    // replica_set_utils_test.go, TestCalculateStatusConditions.
    // ---------------------------------------------------------------------

    use crate::controllers::quota_admission_double::QuotaAdmissionStorage;
    use crate::controllers::replicationcontroller::tests::{
        ConcurrencyProbeStorage, TerminatingNamespaceStorage,
    };

    fn rs_condition(
        rs: &ReplicaSet,
        ty: &str,
    ) -> Option<rusternetes_common::resources::ReplicaSetCondition> {
        rs.status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.condition_type == ty))
            .cloned()
    }

    /// `slowStartBatch` (replica_set.go:820-844) issues each batch
    /// concurrently, and one sync must reach the full replica count. Serial
    /// creates could not create 100 pods inside the conformance spec's 120s
    /// budget (#1847).
    #[tokio::test]
    async fn rs_creates_pods_in_concurrent_slow_start_batches() {
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(ConcurrencyProbeStorage::new(inner));
        let controller = ReplicaSetController::new(storage.clone(), 5);
        let labels = make_labels(&[("app", "ss")]);
        let rs = make_replicaset("ss", labels, 100);
        storage
            .create(&build_key("replicasets", Some("default"), "ss"), &rs)
            .await
            .unwrap();

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 100);
        assert!(
            storage.peak() > 1,
            "creates within a slow-start batch must overlap (peak {})",
            storage.peak()
        );
    }

    /// `TestControllerBurstReplicas`: `diff > rsc.burstReplicas` is capped
    /// (replica_set.go:611-613); the next sync creates the remainder.
    #[tokio::test]
    async fn rs_caps_creates_at_burst_replicas_and_finishes_next_sync() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 5);
        let desired = crate::controllers::replicationcontroller::BURST_REPLICAS as i32 + 100;
        let rs = make_replicaset("burst", make_labels(&[("app", "burst")]), desired);
        storage
            .create(&build_key("replicasets", Some("default"), "burst"), &rs)
            .await
            .unwrap();

        controller.reconcile_replicaset(&rs).await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(
            pods.len(),
            crate::controllers::replicationcontroller::BURST_REPLICAS,
            "one sync must not exceed burstReplicas"
        );

        // The creates were observed (pod watch Added events); simulate them.
        for _ in 0..crate::controllers::replicationcontroller::BURST_REPLICAS {
            controller.observe_creation("default/burst");
        }
        let rs: ReplicaSet = storage
            .get(&build_key("replicasets", Some("default"), "burst"))
            .await
            .unwrap();
        controller.reconcile_replicaset(&rs).await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), desired as usize);
    }

    /// `TestSyncReplicaSetCreateFailures`: 100 replicas, a limit of 10 (quota);
    /// the controller must not make 100 create calls in one pass. Slow start
    /// stops after the first failing batch (replica_set.go:838-840).
    #[tokio::test]
    async fn rs_stops_creating_after_the_first_failing_batch() {
        let storage = Arc::new(QuotaAdmissionStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 5);
        let rs = make_replicaset("cf", make_labels(&[("app", "cf")]), 100);
        storage
            .create(&build_key("replicasets", Some("default"), "cf"), &rs)
            .await
            .unwrap();
        storage
            .create(
                "/registry/resourcequotas/default/pq",
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "ResourceQuota",
                    "metadata": {"name": "pq", "namespace": "default", "uid": "q"},
                    "spec": {"hard": {"pods": "10"}}
                }),
            )
            .await
            .unwrap();

        // syncReplicaSet returns manageReplicasErr after updating status, so
        // the workqueue requeues with backoff (replica_set.go:765-767).
        let result = controller.reconcile_replicaset(&rs).await;
        assert!(result.is_err(), "a create failure must requeue the key");

        let denied = storage
            .denied_creates
            .load(std::sync::atomic::Ordering::SeqCst);
        // Batches 1,2,4,8: the 8-batch is the first to fail, so <= 8 denials.
        assert!(
            denied > 0 && denied <= 8,
            "expected the sync to stop after the first failing batch, saw {denied} denials"
        );
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert!(pods.len() >= 10 && pods.len() < 100, "got {}", pods.len());
    }

    /// `HasStatusCause(err, v1.NamespaceTerminatingCause)` -> `return nil`
    /// (replica_set.go:632-637): no ReplicaFailure, no requeue.
    #[tokio::test]
    async fn rs_ignores_namespace_terminating_create_rejection() {
        let storage = Arc::new(TerminatingNamespaceStorage::new(Arc::new(
            MemoryStorage::new(),
        )));
        let controller = ReplicaSetController::new(storage.clone(), 5);
        let rs = make_replicaset("doomed", make_labels(&[("app", "doomed")]), 5);
        let key = build_key("replicasets", Some("default"), "doomed");
        storage.create(&key, &rs).await.unwrap();

        controller
            .reconcile_replicaset(&rs)
            .await
            .expect("a terminating namespace must not fail the sync");

        let after: ReplicaSet = storage.get(&key).await.unwrap();
        assert!(
            rs_condition(&after, "ReplicaFailure").is_none(),
            "a terminating namespace must not raise ReplicaFailure"
        );
    }

    /// Storage whose first pod `list` lets one "late" pod arrive and lowers
    /// the RS's outstanding create expectation AFTER the snapshot was taken —
    /// upstream's `FakeRSExpectations` hook in `TestRSSyncExpectations`.
    struct LatePodStorage {
        inner: Arc<MemoryStorage>,
        late_pod: std::sync::Mutex<Option<Pod>>,
        expectations: std::sync::OnceLock<Arc<ControllerExpectations>>,
    }

    #[async_trait::async_trait]
    impl Storage for LatePodStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }
        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.get(key).await
        }
        async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.update(key, value).await
        }
        async fn update_raw(
            &self,
            key: &str,
            value: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.inner.update_raw(key, value).await
        }
        async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
            self.inner.delete(key).await
        }
        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            let snapshot = self.inner.list(prefix).await;
            if prefix.starts_with("/registry/pods/") {
                let late = self.late_pod.lock().unwrap().take();
                if let Some(pod) = late {
                    // The pod arrives, and its Added event lowers the
                    // expectation, strictly after the snapshot above.
                    let key = build_key("pods", Some("default"), &pod.metadata.name);
                    self.inner.create(&key, &pod).await.unwrap();
                    self.expectations
                        .get()
                        .unwrap()
                        .creation_observed("default/race");
                }
            }
            snapshot
        }
        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }
        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }
        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }
        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    /// `TestRSSyncExpectations`: a pod cannot sneak in between listing pods and
    /// checking expectations. Expectations must be read BEFORE the list
    /// (replica_set.go:728 precedes :734): checked after, the late pod makes the
    /// record look fulfilled while the list still lacks it, and the controller
    /// creates a duplicate.
    #[tokio::test]
    async fn rs_sync_reads_expectations_before_listing_pods() {
        let labels = make_labels(&[("app", "race")]);
        let rs = make_replicaset("race", labels.clone(), 2);
        let storage = Arc::new(LatePodStorage {
            inner: Arc::new(MemoryStorage::new()),
            late_pod: std::sync::Mutex::new(Some(owned_pod(
                "late",
                labels.clone(),
                "race",
                &rs.metadata.uid,
            ))),
            expectations: std::sync::OnceLock::new(),
        });
        let controller = ReplicaSetController::new(storage.clone(), 5);
        storage
            .expectations
            .set(controller.expectations.clone())
            .ok()
            .unwrap();
        storage
            .create(&build_key("replicasets", Some("default"), "race"), &rs)
            .await
            .unwrap();
        storage
            .create(
                &build_key("pods", Some("default"), "first"),
                &owned_pod("first", labels, "race", &rs.metadata.uid),
            )
            .await
            .unwrap();
        // One create is in flight: the "late" pod.
        controller.expectations.expect_creations("default/race", 1);

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 2, "no duplicate create: got {}", pods.len());
    }

    /// `SetCondition` (replica_set_utils.go:169-176) is a no-op when the
    /// condition already exists with the same status and reason, and
    /// `calculateStatus` only sets ReplicaFailure when none exists
    /// (:124-136): a failure that persists keeps its lastTransitionTime and
    /// message instead of being rewritten every sync.
    #[tokio::test]
    async fn rs_replica_failure_is_not_rewritten_while_it_persists() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);
        let rs = make_replicaset("stable", make_labels(&[("app", "s")]), 1);
        let key = build_key("replicasets", Some("default"), "stable");
        storage.create(&key, &rs).await.unwrap();

        controller
            .update_status(&rs, 0, 0, 0, Some("first failure"), Some("FailedCreate"))
            .await
            .unwrap();
        let first = rs_condition(&storage.get(&key).await.unwrap(), "ReplicaFailure").unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        let rs: ReplicaSet = storage.get(&key).await.unwrap();
        controller
            .update_status(&rs, 0, 0, 0, Some("second failure"), Some("FailedCreate"))
            .await
            .unwrap();
        let second = rs_condition(&storage.get(&key).await.unwrap(), "ReplicaFailure").unwrap();

        assert_eq!(second.message, first.message);
        assert_eq!(second.last_transition_time, first.last_transition_time);
    }

    /// `TestCalculateStatusConditions`: reason is FailedCreate for diff < 0,
    /// FailedDelete for diff > 0 (replica_set_utils.go:127-132).
    #[test]
    fn rs_replica_failure_reason_follows_the_diff() {
        assert_eq!(replica_failure_reason(1, 2), Some("FailedCreate"));
        assert_eq!(replica_failure_reason(3, 2), Some("FailedDelete"));
        // diff == 0 leaves `reason` unset (a zero-value string upstream).
        assert_eq!(replica_failure_reason(2, 2), None);
    }
}

/// Reason of the ReplicaFailure condition, from the pod count BEFORE
/// `manageReplicas` ran against `.spec.replicas`: `FailedCreate` if short,
/// `FailedDelete` if over, unset when equal (a zero-value string upstream).
/// Upstream `calculateStatus`, replica_set_utils.go:127-132.
pub(crate) fn replica_failure_reason(active: i32, desired: i32) -> Option<&'static str> {
    match active.cmp(&desired) {
        std::cmp::Ordering::Less => Some("FailedCreate"),
        std::cmp::Ordering::Greater => Some("FailedDelete"),
        std::cmp::Ordering::Equal => None,
    }
}
