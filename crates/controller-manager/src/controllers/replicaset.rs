use super::controller_utils::{
    find_min_next_pod_availability_check, is_pod_available, is_pod_ready, is_pod_terminating,
    node_of, pod_key, pods_on_node_counts, sort_active_pods_with_ranks, RankGates,
};
use super::expectations::ControllerExpectations;
use super::pod_informer::{
    apply_pod_observation, ControllerRef, PodEventTarget, PodObservation, ResolvedController,
    SharedPodInformer,
};
use super::replicationcontroller::{
    generate_pod_name, is_namespace_terminating_rejection, slow_start_batches, BURST_REPLICAS,
    MAX_NAME_GENERATION_CREATE_ATTEMPTS, SLOW_START_INITIAL_BATCH_SIZE,
};
use crate::controllers::worker_pool::spawn_workers;
use futures::StreamExt;
use rusternetes_common::{
    resources::{Pod, PodStatus, ReplicaSet, ReplicaSetStatus},
    types::{label_selector_as_selector, ObjectMeta, Phase, Selector},
};
#[cfg(test)]
use rusternetes_storage::WatchEvent;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
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
    /// What the shared pod informer's indexer gives upstream's event handlers:
    /// add-versus-update, and the adds/deletes a watch gap hid
    /// ([`super::pod_informer`]). The handlers, not the sync, lower
    /// `expectations` (replica_set.go:436, :568).
    pod_informer: Arc<SharedPodInformer<S>>,
}

impl<S: Storage + 'static> ReplicaSetController<S> {
    pub fn new(storage: Arc<S>, interval_secs: u64) -> Self {
        let interval = Duration::from_secs(interval_secs);
        let pod_informer = Arc::new(SharedPodInformer::new(storage.clone(), interval));
        Self::with_pod_informer(storage, interval_secs, pod_informer)
    }

    /// A controller whose pod event handlers hang off an informer shared with
    /// other controllers (upstream builds one `PodInformer` from the shared
    /// informer factory and hands it to every controller).
    pub fn with_pod_informer(
        storage: Arc<S>,
        interval_secs: u64,
        pod_informer: Arc<SharedPodInformer<S>>,
    ) -> Self {
        Self {
            storage,
            interval: Duration::from_secs(interval_secs),
            expectations: Arc::new(ControllerExpectations::new()),
            pod_informer,
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

        // Upstream's controller registers its handlers on the shared pod
        // informer (`podInformer.Informer().AddEventHandler`, replica_set.go:207,
        // NewBaseController) and the informer factory runs the one watch; the
        // first controller to get here starts it.
        self.pod_informer
            .add_event_handler(Arc::clone(&self), queue.clone())
            .await;
        self.pod_informer.start();

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
                    Ok(next_sync) => {
                        queue.forget(&key).await;
                        // `rsc.queue.AddAfter(key, *nextSyncDuration)`
                        // (replica_set.go:786-788).
                        if let Some(delay) = next_sync {
                            queue.add_after(key.clone(), delay).await;
                        }
                    }
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

    /// `DeltaFIFO.Replace` on (re)connect: list every pod and hand the
    /// informer handlers the adds and deletes the cache has not seen.
    async fn relist_pods(&self, queue: &WorkQueue) {
        match self.storage.list::<Pod>(&build_prefix("pods", None)).await {
            Ok(pods) => {
                for observation in self.pod_informer.cache().replace(&pods) {
                    self.apply_observation(queue, &observation).await;
                }
            }
            Err(e) => error!("Failed to list pods for the informer re-list: {}", e),
        }
    }

    /// The informer's `addPod` / `updatePod` / `deletePod` handlers
    /// (replica_set.go:415-570), for one classified pod event
    /// ([`PodInformerCache::on_event`]): resolve the controller reference,
    /// lower the expectations and enqueue the ReplicaSets it names.
    async fn apply_observation(&self, queue: &WorkQueue, observation: &PodObservation) {
        apply_pod_observation(self, queue, observation).await;
    }

    /// One pod watch event through the informer handlers: `addPod`,
    /// `updatePod` or `deletePod`, whichever the pod cache says it is.
    #[cfg(test)]
    async fn enqueue_owner_replicaset(&self, queue: &WorkQueue, event: &WatchEvent) {
        if let Some(observation) = self.pod_informer.cache().on_event(event) {
            self.apply_observation(queue, &observation).await;
        }
    }

    /// One synchronous pass, for callers that do not run the watch loop: let the
    /// pod informer catch up (the reflector's list, [`Self::relist_pods`]) and
    /// then sync every object once. The informer step is what lowers the
    /// expectations for pods created by an earlier pass, as the pod watch's
    /// Added events do in [`Self::run`] (replica_set.go:436).
    pub async fn reconcile_all(&self) -> rusternetes_common::Result<()> {
        debug!("Reconciling all replicasets");
        self.relist_pods(&WorkQueue::new()).await;

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

    /// `syncReplicaSet` (replica_set.go:713-790), after the key has been read.
    ///
    /// Returns upstream's `nextSyncDuration` (:775-788): the delay after which
    /// the caller must re-enqueue the key to recompute availability.
    async fn reconcile_replicaset(
        &self,
        replicaset: &ReplicaSet,
    ) -> rusternetes_common::Result<Option<Duration>> {
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

        // `metav1.LabelSelectorAsSelector(rs.Spec.Selector)`: an invalid
        // selector is logged and the sync returns nil (:729-733) — nothing is
        // claimed, created or deleted, and no status is written.
        let selector = match label_selector_as_selector(Some(&replicaset.spec.selector)) {
            Ok(selector) => selector,
            Err(e) => {
                error!(
                    "error converting pod selector to selector for rs {}/{}: {}",
                    namespace, replicaset.metadata.name, e
                );
                return Ok(None);
            }
        };

        // Get all pods for this replicaset
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods: Vec<Pod> = self.storage.list(&pods_prefix).await?;

        // Adopt orphan pods: pods that match the selector labels but have no controller ownerReference
        // Release owned pods: pods owned by this RS but whose labels no longer match the selector
        // (`claimPods`, replica_set.go:792-806).
        let all_pods = self
            .adopt_and_release(replicaset, &selector, all_pods, namespace)
            .await?;

        // `activePods`: claimed and active (`FilterActivePods`,
        // controller_utils.go:1001 — a pod being gracefully deleted is no
        // longer a replica). They are what `manageReplicas` and
        // `calculateStatus` both read, and `manageReplicas` does NOT modify
        // them (:597), so pods created or deleted this sync show up in the
        // status of the sync their watch events trigger (:743).
        let active_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| self.matches_selector(p, replicaset, &selector))
            .collect();

        // `terminatingPods` are only listed when the gate is on
        // (:748-753): `FilterTerminatingPods` then `FilterClaimedPods`.
        let terminating_pods: Vec<&Pod> = if terminating_replicas_enabled() {
            all_pods
                .iter()
                .filter(|p| {
                    is_pod_terminating(p)
                        && self.is_owned_by(p, replicaset)
                        && selector.matches(p.metadata.labels.as_ref())
                })
                .collect()
        } else {
            Vec::new()
        };

        debug!(
            "ReplicaSet {}/{}: active={}, desired={}",
            namespace,
            replicaset.metadata.name,
            active_pods.len(),
            replicaset.spec.replicas
        );

        // `manageReplicas`, gated as `syncReplicaSet` gates it: `if rsNeedsSync
        // && rs.DeletionTimestamp == nil` (:757). Acting while expectations
        // are unmet is exactly what caused the #542 burst: a re-reconcile
        // (rapid pod watch event or 5s resync) would `list` before the prior
        // create was visible, see current<desired again, and create a
        // duplicate.
        //
        // `manage_error` is upstream's `manageReplicasErr`: recorded as the
        // ReplicaFailure condition by `calculateStatus` and returned from the
        // sync AFTER the status update, so the workqueue retries with backoff.
        let mut manage_error: Option<String> = None;
        if !needs_sync {
            debug!(
                "ReplicaSet {}/{}: expectations unmet, deferring create/delete this sync",
                namespace, replicaset.metadata.name
            );
        } else if replicaset.metadata.deletion_timestamp.is_none() {
            manage_error = self
                .manage_replicas(&active_pods, &all_pods, replicaset)
                .await;
        }

        // "Use the same time for calculating status and nextSyncDuration"
        // (:745-746).
        let now = chrono::Utc::now();
        let new_status = calculate_status(
            replicaset,
            &active_pods,
            &terminating_pods,
            manage_error.as_deref(),
            now,
        );

        // Always updates status as pods come up or die (:748).
        self.update_status(replicaset, new_status.clone()).await?;

        // `if manageReplicasErr != nil { return manageReplicasErr }` after the
        // status update (:772-774): the error requeues the key with
        // rate-limited backoff.
        if let Some(e) = manage_error {
            return Err(rusternetes_common::Error::Internal(e));
        }

        // Plan the next availability check as a last line of defense against
        // queue preemption or early sync (:775-788).
        let min_ready_seconds = replicaset.spec.min_ready_seconds.unwrap_or(0);
        if min_ready_seconds > 0 && new_status.ready_replicas != new_status.available_replicas {
            // Safeguard fallback to `.spec.minReadySeconds` so
            // `.status.availableReplicas` is always eventually updated.
            return Ok(Some(
                find_min_next_pod_availability_check(
                    &active_pods,
                    min_ready_seconds,
                    now,
                    chrono::Utc::now(),
                )
                .unwrap_or(Duration::from_secs(min_ready_seconds as u64)),
            ));
        }
        Ok(None)
    }

    /// `manageReplicas` (replica_set.go:601-702): create or delete pods to
    /// close the gap between `activePods` and `.spec.replicas`. Returns the
    /// error upstream returns, if any.
    ///
    /// `all_pods` is what the pod lister would hold, for
    /// `getIndirectlyRelatedPods`.
    async fn manage_replicas(
        &self,
        active_pods: &[&Pod],
        all_pods: &[Pod],
        replicaset: &ReplicaSet,
    ) -> Option<String> {
        let namespace = replicaset
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");
        let exp_key = Self::expectation_key(replicaset);
        let current_replicas = active_pods.len() as i32;
        let desired_replicas = replicaset.spec.replicas;
        let mut manage_error: Option<String> = None;

        if current_replicas < desired_replicas {
            // `diff > rsc.burstReplicas` => `diff = rsc.burstReplicas`
            // (replica_set.go:611-613).
            let diff = ((desired_replicas - current_replicas) as usize).min(BURST_REPLICAS);
            info!(
                "Creating {} pods for replicaset {}/{}",
                diff, namespace, replicaset.metadata.name
            );
            // `ExpectCreations` BEFORE issuing the creates (:619), so a
            // reconcile triggered by our own pod-create watch events sees an
            // unmet expectation instead of re-issuing the batch.
            self.set_expectations(&exp_key, diff as i64, 0);
            // Creates that actually succeeded; only these can ever be observed.
            let mut successful_creations = 0usize;
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
            // expected: "Decrement the expected number of creates because the
            // informer won't observe this pod" (:641-650). The creates that
            // did land are observed by the pod watch, as upstream's informer
            // observes them (`addPod`, :436) — never by this sync.
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
            // `getIndirectlyRelatedPods` then `getPodsToDelete` (:658-662):
            // "Choose which Pods to delete, preferring those in earlier phases
            // of startup".
            let related = self.get_indirectly_related_pods(replicaset, all_pods).await;
            let pods_to_delete = get_pods_to_delete(active_pods, &related, diff);
            // Snapshot the keys of the pods we expect to see deleted, so each
            // is recorded exactly once whether it is observed as a
            // deletionTimestamp update or as the delete (`ExpectDeletions`,
            // replica_set.go:670).
            let doomed_keys: Vec<String> = pods_to_delete.iter().map(|p| pod_key(p)).collect();
            self.expectations
                .expect_deletions_of(&exp_key, &doomed_keys);
            // The deletes run concurrently and every one is attempted; only
            // the first error is reported (:672-697).
            let results = futures::future::join_all(
                pods_to_delete
                    .iter()
                    .map(|pod| self.delete_pod(&pod.metadata.name, namespace)),
            )
            .await;
            for (result, pod_key) in results.into_iter().zip(&doomed_keys) {
                if let Err(e) = result {
                    // The informer will not observe this deletion
                    // (replica_set.go:679-686).
                    self.observe_deletion(&exp_key, pod_key);
                    if !matches!(e, rusternetes_common::Error::NotFound(_)) {
                        manage_error.get_or_insert_with(|| e.to_string());
                    }
                }
            }
        }
        manage_error
    }

    /// `getIndirectlyRelatedPods` (replica_set.go:848-872): every pod selected
    /// by any ReplicaSet sharing `rs`'s controller. `pods` stands in for the
    /// pod lister (`podLister.Pods(ns).List(selector)`).
    async fn get_indirectly_related_pods<'a>(
        &self,
        replicaset: &ReplicaSet,
        pods: &'a [Pod],
    ) -> Vec<&'a Pod> {
        let namespace = replicaset
            .metadata
            .namespace
            .as_deref()
            .unwrap_or("default");
        let related_sets: Vec<ReplicaSet> = match self
            .storage
            .list(&build_prefix("replicasets", Some(namespace)))
            .await
        {
            Ok(sets) => sets,
            Err(e) => {
                // `utilruntime.HandleError(err)` then proceeds with whatever
                // was found (:660-661): no related pods, so every rank is 0.
                error!("listing ReplicaSets for related pods: {}", e);
                Vec::new()
            }
        };
        let sets: Vec<RelatedSet<'_>> = related_sets
            .iter()
            .map(|rs| RelatedSet {
                metadata: &rs.metadata,
                selector: rs.spec.selector.clone(),
            })
            .collect();
        indirectly_related_pods(&replicaset.metadata, &sets, pods)
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
        selector: &Selector,
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

            let labels_match = selector.matches(pod.metadata.labels.as_ref());
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

    fn matches_selector(&self, pod: &Pod, replicaset: &ReplicaSet, selector: &Selector) -> bool {
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

        selector.matches(pod.metadata.labels.as_ref())
    }

    /// `updateReplicaSetStatus` (replica_set_utils.go:41-86): write `new_status`
    /// when it differs from what the object already carries.
    async fn update_status(
        &self,
        replicaset: &ReplicaSet,
        new_status: ReplicaSetStatus,
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
        let new_status = Some(new_status);

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
            "Updated status for replicaset {}/{}: {:?}",
            namespace, replicaset.metadata.name, updated_rs.status
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
        // (registry/generic/registry/store.go:464-475). Any other error is
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
    /// (replica_set.go:630-639): a rejection because the namespace is
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

/// The ReplicaSet half of the pod event handlers (replica_set.go:289-321).
impl<S: Storage + 'static> PodEventTarget for ReplicaSetController<S> {
    fn queue_prefix(&self) -> &'static str {
        "replicasets"
    }

    /// `resolveControllerRef`: "We can't look up by UID, so look up by Name
    /// and then verify UID. Don't even try to look up by Name if it's the
    /// wrong Kind."
    async fn resolve_controller_ref(
        &self,
        namespace: &str,
        controller_ref: &ControllerRef,
    ) -> Option<ResolvedController> {
        if controller_ref.kind != "ReplicaSet" {
            return None;
        }
        let rs: ReplicaSet = self
            .storage
            .get(&build_key(
                "replicasets",
                Some(namespace),
                &controller_ref.name,
            ))
            .await
            .ok()?;
        // "The controller we found with this Name is not the same one that
        // the ControllerRef points to."
        (rs.metadata.uid == controller_ref.uid).then(|| ResolvedController {
            name: rs.metadata.name.clone(),
            min_ready_seconds: rs.spec.min_ready_seconds.unwrap_or(0),
        })
    }

    /// `ReplicaSetLister.GetPodReplicaSets` (client-go
    /// `listers/apps/v1/replicaset_expansion.go:38-74`): a pod without labels
    /// matches none; an invalid selector "does not match the pod"; "a
    /// ReplicaSet with a nil or empty selector creeps in, it should match
    /// nothing, not everything".
    async fn pod_controllers(
        &self,
        namespace: &str,
        pod_labels: &HashMap<String, String>,
    ) -> Vec<String> {
        if pod_labels.is_empty() {
            return Vec::new();
        }
        let Ok(replicasets) = self
            .storage
            .list::<ReplicaSet>(&build_prefix("replicasets", Some(namespace)))
            .await
        else {
            return Vec::new();
        };
        replicasets
            .into_iter()
            .filter(|rs| {
                matches!(
                    label_selector_as_selector(Some(&rs.spec.selector)),
                    Ok(selector @ Selector::Requirements(_)) if selector.matches(Some(pod_labels))
                )
            })
            .map(|rs| rs.metadata.name)
            .collect()
    }

    fn creation_observed(&self, key: &str) {
        self.observe_creation(key);
    }

    fn deletion_observed(&self, key: &str, pod_key: &str) {
        self.observe_deletion(key, pod_key);
    }
}

/// Whether `status.terminatingReplicas` is maintained:
/// `utilfeature.DefaultFeatureGate.Enabled(features.DeploymentReplicaSetTerminatingReplicas)
/// && controllerFeatures.EnableStatusTerminatingReplicas` (replica_set.go:750,
/// replica_set_utils.go:120). The ReplicaSet controller's
/// `DefaultReplicaSetControllerFeatures` sets the second half true
/// (replica_set.go:135); the ReplicationController, which rides the same
/// code, sets it false (replication_controller.go:69) and has no such status
/// field anyway.
fn terminating_replicas_enabled() -> bool {
    rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::DeploymentReplicaSetTerminatingReplicas,
    )
}

/// The status counts `calculateStatus` derives from `activePods`
/// (replica_set_utils.go:96-118), shared with the ReplicationController, which
/// upstream converts onto this very code (pkg/controller/replication/conversion.go).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PodCounts {
    pub replicas: i32,
    pub fully_labeled: i32,
    pub ready: i32,
    pub available: i32,
}

/// `calculateStatus`'s loop (replica_set_utils.go:100-118): a pod is fully
/// labeled when the TEMPLATE labels are a subset of its labels ("the matching
/// pods may have more labels than are in the template"), ready per
/// `IsPodReady`, available per `IsPodAvailable` at `now`.
pub(crate) fn count_pods(
    template_labels: Option<&HashMap<String, String>>,
    min_ready_seconds: i32,
    active_pods: &[&Pod],
    now: chrono::DateTime<chrono::Utc>,
) -> PodCounts {
    let mut counts = PodCounts {
        replicas: active_pods.len() as i32,
        fully_labeled: 0,
        ready: 0,
        available: 0,
    };
    for pod in active_pods {
        // `labels.Set(template).AsSelectorPreValidated().Matches(pod.Labels)`:
        // every template label present with the same value; no template
        // labels matches everything.
        let fully_labeled = template_labels.is_none_or(|template| {
            template
                .iter()
                .all(|(k, v)| pod.metadata.labels.as_ref().and_then(|l| l.get(k)) == Some(v))
        });
        if fully_labeled {
            counts.fully_labeled += 1;
        }
        if is_pod_ready(pod) {
            counts.ready += 1;
            if is_pod_available(pod, min_ready_seconds, now) {
                counts.available += 1;
            }
        }
    }
    counts
}

/// `calculateStatus` (replica_set_utils.go:96-141). `terminating_pods` are
/// only consulted when `terminatingReplicas` is maintained; pass the
/// already-gated list (empty when the gate is off).
pub(crate) fn calculate_status(
    rs: &ReplicaSet,
    active_pods: &[&Pod],
    terminating_pods: &[&Pod],
    manage_error: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> ReplicaSetStatus {
    let mut new_status = rs.status.clone().unwrap_or_default();
    let counts = count_pods(
        rs.spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.as_ref()),
        rs.spec.min_ready_seconds.unwrap_or(0),
        active_pods,
        now,
    );

    // ReplicaFailure is only SET when none exists, and only REMOVED when there
    // is no error (:124-136). While a failure persists the existing condition
    // is left alone, so its lastTransitionTime and message are not rewritten
    // every sync (the `SetCondition` no-op on matching status+reason,
    // :169-176). Conditions of other types are preserved.
    let mut conditions = new_status.conditions.take().unwrap_or_default();
    let has_failure = conditions
        .iter()
        .any(|c| c.condition_type == "ReplicaFailure");
    match manage_error {
        Some(msg) if !has_failure => {
            conditions.push(rusternetes_common::resources::ReplicaSetCondition {
                condition_type: "ReplicaFailure".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(chrono::Utc::now()),
                reason: replica_failure_reason(counts.replicas, rs.spec.replicas)
                    .map(str::to_string),
                message: Some(msg.to_string()),
            });
        }
        None if has_failure => conditions.retain(|c| c.condition_type != "ReplicaFailure"),
        _ => {}
    }
    new_status.conditions = if conditions.is_empty() {
        None
    } else {
        Some(conditions)
    };

    new_status.replicas = counts.replicas;
    new_status.fully_labeled_replicas = Some(counts.fully_labeled);
    new_status.ready_replicas = counts.ready;
    new_status.available_replicas = counts.available;
    new_status.terminating_replicas =
        terminating_replicas_enabled().then_some(terminating_pods.len() as i32);
    // `updateReplicaSetStatus`: "Save the generation number we acted on"
    // (replica_set_utils.go:57-62).
    new_status.observed_generation = rs.metadata.generation;
    new_status
}

/// `getPodsToDelete` (replica_set.go:874-883): the `diff` pods to delete,
/// "preferring those in earlier phases of startup". No ranking is done when
/// every pod is going anyway.
pub(crate) fn get_pods_to_delete<'a>(
    filtered_pods: &[&'a Pod],
    related_pods: &[&Pod],
    diff: usize,
) -> Vec<&'a Pod> {
    if diff < filtered_pods.len() {
        // `getPodsRankedByRelatedPodsOnSameNode` (:909-921): rank = the
        // number of active related pods on the pod's node.
        let on_node = pods_on_node_counts(related_pods);
        let ranks: Vec<usize> = filtered_pods
            .iter()
            .map(|p| on_node.get(node_of(p)).copied().unwrap_or(0))
            .collect();
        let sorted = sort_active_pods_with_ranks(
            filtered_pods,
            &ranks,
            Some(chrono::Utc::now()),
            RankGates::current(),
        );
        return sorted[..diff].to_vec();
    }
    filtered_pods.to_vec()
}

/// A controller object as `getIndirectlyRelatedPods` sees it: its metadata
/// (for the controller reference) and its pod selector. The
/// ReplicationController supplies these too, upstream converting it onto a
/// ReplicaSet (pkg/controller/replication/conversion.go `convertRCtoRS`).
pub(crate) struct RelatedSet<'a> {
    pub metadata: &'a ObjectMeta,
    pub selector: rusternetes_common::types::LabelSelector,
}

fn controller_uid(metadata: &ObjectMeta) -> Option<&str> {
    metadata
        .owner_references
        .iter()
        .flatten()
        .find(|r| r.controller == Some(true))
        .map(|r| r.uid.as_str())
}

/// `getIndirectlyRelatedPods` (replica_set.go:848-872) over the given
/// controllers (`getReplicaSetsWithSameController`, :264-286, reads
/// `rsIndexer.ByIndex(controllerUIDIndex, controllerRef.UID)`) and pods (the
/// pod lister).
pub(crate) fn indirectly_related_pods<'a>(
    this: &ObjectMeta,
    all_sets: &[RelatedSet<'_>],
    pods: &'a [Pod],
) -> Vec<&'a Pod> {
    let Some(controller_uid) = controller_uid(this) else {
        // "ReplicaSet has no controller" (:267): no related ReplicaSets.
        return Vec::new();
    };
    let mut related: Vec<&Pod> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for related_set in all_sets
        .iter()
        .filter(|candidate| self::controller_uid(candidate.metadata) == Some(controller_uid))
    {
        // "This object has an invalid selector, it does not match any pods"
        let Ok(selector) =
            rusternetes_common::types::label_selector_as_selector(Some(&related_set.selector))
        else {
            continue;
        };
        for pod in pods
            .iter()
            .filter(|p| p.metadata.namespace == related_set.metadata.namespace)
            .filter(|p| selector.matches(p.metadata.labels.as_ref()))
        {
            let id = if pod.metadata.uid.is_empty() {
                pod_key(pod)
            } else {
                pod.metadata.uid.clone()
            };
            if seen.insert(id) {
                related.push(pod);
            }
        }
    }
    related
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
                calculate_status(
                    &rs,
                    &[],
                    &[],
                    Some("pods \"x\" is forbidden: exceeded quota"),
                    chrono::Utc::now(),
                ),
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
            .update_status(
                &rs,
                calculate_status(&rs, &[], &[], None, chrono::Utc::now()),
            )
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
        // `resolveControllerRef` needs the RS the pod's reference names.
        let mut rs = make_replicaset("g", labels.clone(), 1);
        rs.metadata.uid = "u".to_string();
        put_rs(&storage, &rs).await;
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
        // the workqueue requeues with backoff (replica_set.go:772-774).
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
    /// (replica_set.go:728 precedes :736): checked after, the late pod makes the
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
            .update_status(
                &rs,
                calculate_status(&rs, &[], &[], Some("first failure"), chrono::Utc::now()),
            )
            .await
            .unwrap();
        let first = rs_condition(&storage.get(&key).await.unwrap(), "ReplicaFailure").unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        let rs: ReplicaSet = storage.get(&key).await.unwrap();
        controller
            .update_status(
                &rs,
                calculate_status(&rs, &[], &[], Some("second failure"), chrono::Utc::now()),
            )
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

    // ---------------------------------------------------------------------
    // syncReplicaSet / calculateStatus / getPodsToDelete parity (#3624)
    //
    // Ported from upstream pkg/controller/replicaset/replica_set.go
    // (`syncReplicaSet` :713-790, `getPodsToDelete` :874) and
    // replica_set_utils.go (`calculateStatus` :96-141), with
    // pkg/controller/controller_utils.go `ActivePodsWithRanks` (:778-870).
    // ---------------------------------------------------------------------

    use rusternetes_common::resources::pod::PodCondition;

    fn ready_condition(since: chrono::DateTime<chrono::Utc>) -> PodCondition {
        PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(since),
            observed_generation: None,
        }
    }

    /// A pod owned by `rs`, on `node` ("" = unscheduled), in `phase`, Ready
    /// since `ready_since` (None = not ready).
    fn ranked_pod(
        name: &str,
        rs: &ReplicaSet,
        node: &str,
        phase: Phase,
        ready_since: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Pod {
        let labels = rs.spec.selector.match_labels.clone().unwrap_or_default();
        let mut pod = owned_pod(name, labels, &rs.metadata.name, &rs.metadata.uid);
        if !node.is_empty() {
            pod.spec.as_mut().unwrap().node_name = Some(node.to_string());
        }
        let status = pod.status.as_mut().unwrap();
        status.phase = Some(phase);
        status.conditions = ready_since.map(|t| vec![ready_condition(t)]);
        pod
    }

    async fn seed_rs(storage: &Arc<MemoryStorage>, rs: &ReplicaSet, pods: &[Pod]) {
        storage
            .create(
                &build_key("replicasets", Some("default"), &rs.metadata.name),
                rs,
            )
            .await
            .unwrap();
        for pod in pods {
            storage
                .create(&build_key("pods", Some("default"), &pod.metadata.name), pod)
                .await
                .unwrap();
        }
    }

    async fn terminating_pod_names(storage: &Arc<MemoryStorage>) -> Vec<String> {
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        let mut names: Vec<String> = pods
            .into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
            .map(|p| p.metadata.name)
            .collect();
        names.sort();
        names
    }

    async fn status_of(storage: &Arc<MemoryStorage>, name: &str) -> ReplicaSetStatus {
        let rs: ReplicaSet = storage
            .get(&build_key("replicasets", Some("default"), name))
            .await
            .unwrap();
        rs.status.expect("status written")
    }

    /// `syncReplicaSet` computes status from the `activePods` it listed BEFORE
    /// `manageReplicas` (replica_set.go:743 `calculateStatus(rs, activePods,
    /// ...)`; `manageReplicas` is documented "Does NOT modify <activePods>",
    /// :597). Pods created this sync are counted by the sync their Added
    /// events trigger, not by a second list.
    #[tokio::test]
    async fn rs_status_counts_the_pods_listed_before_manage_replicas() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("pre", make_labels(&[("app", "pre")]), 3);
        seed_rs(&storage, &rs, &[]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 3);
        assert_eq!(
            status_of(&storage, "pre").await.replicas,
            0,
            "status is the pre-manage count, not a re-list"
        );
    }

    /// Creations are observed by the informer's `addPod` handler
    /// (replica_set.go:436 `CreationObserved`), never by the sync itself: a
    /// sync that lowers its own expectations from a re-list defeats the brake.
    #[tokio::test]
    async fn rs_sync_does_not_lower_its_own_create_expectations() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("exp", make_labels(&[("app", "exp")]), 3);
        seed_rs(&storage, &rs, &[]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(
            controller.expectations.get_expectations("default/exp"),
            Some((3, 0)),
            "only pod-watch Added events lower the creation expectation"
        );
    }

    /// `getPodsToDelete` (replica_set.go:874) deletes the ranked victims,
    /// not the first `diff` pods: not-ready before ready
    /// (`ActivePodsWithRanks.Less` rule 3, controller_utils.go:849).
    #[tokio::test]
    async fn rs_scale_down_deletes_the_not_ready_pod_first() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("rank", make_labels(&[("app", "rank")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let pods = [
            ranked_pod("a-ready", &rs, "n1", Phase::Running, Some(ago)),
            ranked_pod("z-notready", &rs, "n1", Phase::Running, None),
        ];
        seed_rs(&storage, &rs, &pods).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(terminating_pod_names(&storage).await, vec!["z-notready"]);
    }

    /// Rule 5 of `ActivePodsWithRanks.Less` (controller_utils.go:861): a pod
    /// colocated with more related pods is deleted first
    /// (`getPodsRankedByRelatedPodsOnSameNode`, replica_set.go:942-957).
    #[tokio::test]
    async fn rs_scale_down_prefers_the_pod_sharing_a_node() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("colo", make_labels(&[("app", "colo")]), 2);
        // Related pods come from the ReplicaSets sharing this one's controller
        // (`getReplicaSetsWithSameController`, replica_set.go:264-286); a bare
        // ReplicaSet has none, and every rank is 0.
        rs.metadata.owner_references = Some(vec![OwnerReference::new(
            "apps/v1",
            "Deployment",
            "colo-deploy",
            "deploy-uid",
        )
        .with_controller(true)]);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let pods = [
            ranked_pod("a-alone", &rs, "n2", Phase::Running, Some(ago)),
            ranked_pod("b-shared", &rs, "n1", Phase::Running, Some(ago)),
            ranked_pod("c-shared", &rs, "n1", Phase::Running, Some(ago)),
        ];
        seed_rs(&storage, &rs, &pods).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let victims = terminating_pod_names(&storage).await;
        assert_eq!(victims.len(), 1);
        assert_ne!(victims[0], "a-alone", "the pod alone on its node survives");
    }

    /// Unscheduled before scheduled, pending before running
    /// (controller_utils.go:835-846; TestGetPodsToDelete "various pod phases").
    #[tokio::test]
    async fn rs_scale_down_prefers_unscheduled_then_pending_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("phase", make_labels(&[("app", "phase")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let pods = [
            ranked_pod("a-running", &rs, "n1", Phase::Running, Some(ago)),
            ranked_pod("b-pending", &rs, "n1", Phase::Pending, None),
            ranked_pod("c-unscheduled", &rs, "", Phase::Pending, None),
        ];
        seed_rs(&storage, &rs, &pods).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(
            terminating_pod_names(&storage).await,
            vec!["b-pending", "c-unscheduled"]
        );
    }

    /// `calculateStatus` counts a pod fully labeled when the TEMPLATE labels
    /// match it (replica_set_utils.go:103-110), not every active pod.
    #[tokio::test]
    async fn rs_fully_labeled_replicas_matches_template_labels() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("fl", make_labels(&[("app", "fl")]), 2);
        rs.spec.template.metadata =
            Some(ObjectMeta::new("").with_labels(make_labels(&[("app", "fl"), ("tier", "web")])));
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let mut full = ranked_pod("full", &rs, "n1", Phase::Running, Some(ago));
        full.metadata.labels = Some(make_labels(&[("app", "fl"), ("tier", "web")]));
        let partial = ranked_pod("partial", &rs, "n1", Phase::Running, Some(ago));
        seed_rs(&storage, &rs, &[full, partial]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let status = status_of(&storage, "fl").await;
        assert_eq!(status.replicas, 2);
        assert_eq!(status.fully_labeled_replicas, Some(1));
    }

    /// `IsPodAvailable` (pkg/api/v1/pod/util.go:283-294) measures
    /// minReadySeconds from the Ready condition's lastTransitionTime, not the
    /// pod's creation time.
    #[tokio::test]
    async fn rs_availability_uses_the_ready_transition_time() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("avail", make_labels(&[("app", "avail")]), 2);
        rs.spec.min_ready_seconds = Some(60);
        let now = chrono::Utc::now();
        // Created an hour ago but only just became Ready: not available.
        let mut just_ready = ranked_pod(
            "just-ready",
            &rs,
            "n1",
            Phase::Running,
            Some(now - chrono::Duration::seconds(10)),
        );
        just_ready.metadata.creation_timestamp = Some(now - chrono::Duration::hours(1));
        // Also created an hour ago, Ready for two minutes: available.
        let mut long_ready = ranked_pod(
            "long-ready",
            &rs,
            "n1",
            Phase::Running,
            Some(now - chrono::Duration::seconds(120)),
        );
        long_ready.metadata.creation_timestamp = Some(now - chrono::Duration::hours(1));
        seed_rs(&storage, &rs, &[just_ready, long_ready]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let status = status_of(&storage, "avail").await;
        assert_eq!(status.ready_replicas, 2);
        assert_eq!(status.available_replicas, 1);
    }

    /// `syncReplicaSet` plans the next availability check
    /// (replica_set.go:775-788): `FindMinNextPodAvailabilityCheck` gives the
    /// time until the earliest ready pod turns available.
    #[tokio::test]
    async fn rs_requeues_when_a_ready_pod_is_not_yet_available() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("next", make_labels(&[("app", "next")]), 1);
        rs.spec.min_ready_seconds = Some(60);
        let pod = ranked_pod(
            "p",
            &rs,
            "n1",
            Phase::Running,
            Some(chrono::Utc::now() - chrono::Duration::seconds(10)),
        );
        seed_rs(&storage, &rs, &[pod]).await;

        let next = controller
            .reconcile_replicaset(&rs)
            .await
            .unwrap()
            .expect("an AddAfter must be planned");

        assert!(
            next > Duration::from_secs(48) && next <= Duration::from_secs(50),
            "~50s left until the pod is available, got {next:?}"
        );
    }

    /// With `DeploymentReplicaSetTerminatingReplicas` on, `calculateStatus`
    /// reports the claimed terminating pods (replica_set_utils.go:119-122,
    /// replica_set.go:748-753).
    #[tokio::test]
    #[serial_test::serial]
    async fn rs_reports_terminating_replicas_when_the_gate_is_on() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let _gate = with_feature(Feature::DeploymentReplicaSetTerminatingReplicas, true);
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("term", make_labels(&[("app", "term")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let live = ranked_pod("live", &rs, "n1", Phase::Running, Some(ago));
        let mut dying = ranked_pod("dying", &rs, "n1", Phase::Running, Some(ago));
        dying.metadata.deletion_timestamp = Some(chrono::Utc::now());
        seed_rs(&storage, &rs, &[live, dying]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let status = status_of(&storage, "term").await;
        assert_eq!(status.replicas, 1);
        assert_eq!(status.terminating_replicas, Some(1));
    }

    /// Gate off: the field stays unset (replica_set_utils.go:119).
    #[tokio::test]
    #[serial_test::serial]
    async fn rs_leaves_terminating_replicas_unset_when_the_gate_is_off() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let _gate = with_feature(Feature::DeploymentReplicaSetTerminatingReplicas, false);
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("termoff", make_labels(&[("app", "termoff")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let live = ranked_pod("live", &rs, "n1", Phase::Running, Some(ago));
        let mut dying = ranked_pod("dying", &rs, "n1", Phase::Running, Some(ago));
        dying.metadata.deletion_timestamp = Some(chrono::Utc::now());
        seed_rs(&storage, &rs, &[live, dying]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(
            status_of(&storage, "termoff").await.terminating_replicas,
            None
        );
    }

    // ---- ports of the pure helpers' upstream tests (#3624) ----

    fn names(pods: &[&Pod]) -> Vec<String> {
        pods.iter().map(|p| p.metadata.name.clone()).collect()
    }

    /// `TestGetPodsToDelete` (replica_set_test.go:1942-2140).
    #[test]
    fn get_pods_to_delete_follows_upstream_table() {
        let rs = make_replicaset("foo", make_labels(&[("name", "foo")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let unscheduled_pending =
            ranked_pod("unscheduled-pending-pod", &rs, "", Phase::Pending, None);
        let scheduled_pending = ranked_pod(
            "scheduled-pending-pod",
            &rs,
            "fake-node",
            Phase::Pending,
            None,
        );
        let running_not_ready = ranked_pod(
            "scheduled-running-not-ready-pod",
            &rs,
            "fake-node",
            Phase::Running,
            None,
        );
        let ready_node1 = ranked_pod(
            "scheduled-running-ready-pod-on-node-1",
            &rs,
            "fake-node-1",
            Phase::Running,
            Some(ago),
        );
        let ready_node2 = ranked_pod(
            "scheduled-running-ready-pod-on-node-2",
            &rs,
            "fake-node-2",
            Phase::Running,
            Some(ago),
        );

        struct Case<'a> {
            name: &'a str,
            pods: Vec<&'a Pod>,
            // defaults to `pods` when None
            related: Option<Vec<&'a Pod>>,
            diff: usize,
            expected: Vec<&'a Pod>,
        }
        let cases = vec![
            Case {
                name: "len(pods) = 0 (i.e., diff = 0 too)",
                pods: vec![],
                related: None,
                diff: 0,
                expected: vec![],
            },
            Case {
                name: "diff = len(pods)",
                pods: vec![&running_not_ready, &ready_node1],
                related: None,
                diff: 2,
                expected: vec![&running_not_ready, &ready_node1],
            },
            Case {
                name: "diff < len(pods)",
                pods: vec![&ready_node1, &running_not_ready],
                related: None,
                diff: 1,
                expected: vec![&running_not_ready],
            },
            Case {
                name: "various pod phases and conditions, diff = len(pods)",
                pods: vec![
                    &ready_node1,
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
                related: None,
                diff: 6,
                expected: vec![
                    &ready_node1,
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
            },
            Case {
                name: "various pod phases and conditions, diff = len(pods), relatedPods empty",
                pods: vec![
                    &ready_node1,
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
                related: Some(vec![]),
                diff: 6,
                expected: vec![
                    &ready_node1,
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
            },
            Case {
                name: "scheduled vs unscheduled, diff < len(pods)",
                pods: vec![&scheduled_pending, &unscheduled_pending],
                related: None,
                diff: 1,
                expected: vec![&unscheduled_pending],
            },
            Case {
                name: "ready vs not-ready, diff < len(pods)",
                pods: vec![&ready_node1, &running_not_ready, &running_not_ready],
                related: None,
                diff: 2,
                expected: vec![&running_not_ready, &running_not_ready],
            },
            Case {
                name:
                    "ready and colocated with another ready pod vs not colocated, diff < len(pods)",
                pods: vec![&ready_node1, &ready_node2],
                related: Some(vec![&ready_node1, &ready_node2, &ready_node2]),
                diff: 1,
                expected: vec![&ready_node2],
            },
            Case {
                name: "pending vs running, diff < len(pods)",
                pods: vec![&scheduled_pending, &running_not_ready],
                related: None,
                diff: 1,
                expected: vec![&scheduled_pending],
            },
            Case {
                name: "various pod phases and conditions, diff < len(pods)",
                pods: vec![
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
                related: None,
                diff: 3,
                expected: vec![&unscheduled_pending, &scheduled_pending, &running_not_ready],
            },
        ];
        for case in cases {
            let related = case.related.clone().unwrap_or_else(|| case.pods.clone());
            let got = get_pods_to_delete(&case.pods, &related, case.diff);
            assert_eq!(
                names(&got),
                names(&case.expected),
                "{}: unexpected pods to delete",
                case.name
            );
        }
    }

    /// `TestRelatedPodsLookup` (replica_set_test.go:552-625).
    #[test]
    fn related_pods_lookup_follows_upstream_table() {
        let owned_by = |mut rs: ReplicaSet, uid: &str| {
            rs.metadata.owner_references = Some(vec![OwnerReference::new(
                "apps/v1",
                "Deployment",
                "owner",
                uid,
            )
            .with_controller(true)]);
            rs
        };
        let some_rs = owned_by(
            make_replicaset("foo1", make_labels(&[("foo", "bar")]), 1),
            "123",
        );
        let related_rs = owned_by(
            make_replicaset("foo2", make_labels(&[("foo", "baz")]), 1),
            "123",
        );
        let unrelated_rs = owned_by(
            make_replicaset("bar1", make_labels(&[("foo", "quux")]), 1),
            "456",
        );
        let mut pending_deletion_rs = owned_by(
            make_replicaset("foo3", make_labels(&[("foo", "xyzzy")]), 1),
            "789",
        );
        pending_deletion_rs.metadata.deletion_timestamp = Some(chrono::Utc::now());

        let pod1 = ranked_pod("pod1", &some_rs, "", Phase::Running, None);
        let pod2 = ranked_pod("pod2", &some_rs, "", Phase::Running, None);
        let pod3 = ranked_pod("pod3", &related_rs, "", Phase::Running, None);
        let pod4 = ranked_pod("pod4", &unrelated_rs, "", Phase::Running, None);
        let pending_pod = ranked_pod("pod", &pending_deletion_rs, "", Phase::Running, None);

        let related_names = |rs: &ReplicaSet, all: &[&ReplicaSet], pods: &[Pod]| {
            let sets: Vec<RelatedSet<'_>> = all
                .iter()
                .map(|r| RelatedSet {
                    metadata: &r.metadata,
                    selector: r.spec.selector.clone(),
                })
                .collect();
            let mut got: Vec<String> = indirectly_related_pods(&rs.metadata, &sets, pods)
                .iter()
                .map(|p| p.metadata.name.clone())
                .collect();
            got.sort();
            got
        };

        assert_eq!(
            related_names(
                &pending_deletion_rs,
                &[&pending_deletion_rs, &unrelated_rs],
                std::slice::from_ref(&pending_pod)
            ),
            vec!["pod"],
            "a pod is related even if its owning ReplicaSet is pending deletion"
        );
        assert_eq!(
            related_names(
                &some_rs,
                &[&some_rs, &unrelated_rs],
                &[pod1.clone(), pod2.clone(), pod4.clone()]
            ),
            vec!["pod1", "pod2"],
            "only the ReplicaSet's own pods when there is no related ReplicaSet"
        );
        assert_eq!(
            related_names(
                &some_rs,
                &[&some_rs, &related_rs, &unrelated_rs],
                &[pod1, pod2, pod3, pod4]
            ),
            vec!["pod1", "pod2", "pod3"],
            "own pods plus any related ReplicaSet's, never an unrelated one's"
        );

        // `getReplicaSetsWithSameController`: no controller, no related sets.
        let orphan = make_replicaset("lonely", make_labels(&[("foo", "bar")]), 1);
        assert!(related_names(
            &orphan,
            &[&orphan],
            &[ranked_pod("p", &orphan, "", Phase::Running, None)]
        )
        .is_empty());
    }

    /// `TestCalculateStatus` (replica_set_utils_test.go:38-330).
    #[test]
    #[serial_test::serial]
    fn calculate_status_follows_upstream_table() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        // Template labels are always the full map (`newReplicaSet`); the
        // pods carry the selector's labels (`newPod`).
        let rs_with = |replicas: i32, selector: &[(&str, &str)], min_ready: i32| {
            let mut rs = make_replicaset("foobar", make_labels(selector), replicas);
            rs.spec.template.metadata = Some(
                ObjectMeta::new("")
                    .with_labels(make_labels(&[("name", "foo"), ("type", "production")])),
            );
            rs.spec.min_ready_seconds = Some(min_ready);
            rs
        };
        let not_fully = rs_with(1, &[("name", "foo")], 0);
        let fully = rs_with(2, &[("name", "foo"), ("type", "production")], 0);
        let long_min_ready = rs_with(1, &[("name", "foo"), ("type", "production")], 3600);
        let now = chrono::Utc::now();
        let ready = |name: &str, rs: &ReplicaSet, since: Option<chrono::DateTime<chrono::Utc>>| {
            ranked_pod(name, rs, "", Phase::Running, Some(since.unwrap_or(now)))
        };
        let terminating = |mut p: Pod| {
            p.metadata.deletion_timestamp = Some(now);
            p
        };

        struct Case {
            name: &'static str,
            gate: bool,
            rs: ReplicaSet,
            active: Vec<Pod>,
            terminating: Vec<Pod>,
            // replicas, fully labeled, ready, available, terminating
            expected: (i32, i32, i32, i32, Option<i32>),
        }
        let cases = vec![
            Case {
                name: "1 fully labelled pod",
                gate: false,
                rs: fully.clone(),
                active: vec![ready("pod1", &fully, None)],
                terminating: vec![],
                expected: (1, 1, 1, 1, None),
            },
            Case {
                name: "1 not fully labelled pod",
                gate: false,
                rs: not_fully.clone(),
                active: vec![ready("pod1", &not_fully, None)],
                terminating: vec![],
                expected: (1, 0, 1, 1, None),
            },
            Case {
                name: "2 fully labelled pods with DeploymentReplicaSetTerminatingReplicas",
                gate: true,
                rs: fully.clone(),
                active: vec![ready("pod1", &fully, None), ready("pod2", &fully, None)],
                terminating: vec![],
                expected: (2, 2, 2, 2, Some(0)),
            },
            Case {
                name: "1 fully labelled pod, 1 not fully labelled pod",
                gate: false,
                rs: not_fully.clone(),
                active: vec![ready("pod1", &not_fully, None), ready("pod2", &fully, None)],
                terminating: vec![],
                expected: (2, 1, 2, 2, None),
            },
            Case {
                name: "1 non-ready pod",
                gate: false,
                rs: fully.clone(),
                active: vec![ranked_pod("pod1", &fully, "", Phase::Pending, None)],
                terminating: vec![],
                expected: (1, 1, 0, 0, None),
            },
            Case {
                name: "1 ready but non-available pod",
                gate: false,
                rs: long_min_ready.clone(),
                active: vec![ready("pod1", &long_min_ready, Some(now))],
                terminating: vec![],
                expected: (1, 1, 1, 0, None),
            },
            Case {
                name: "1 available pod with minReadySeconds",
                gate: false,
                rs: long_min_ready.clone(),
                active: vec![ready(
                    "pod1",
                    &long_min_ready,
                    Some(now - chrono::Duration::seconds(3600)),
                )],
                terminating: vec![],
                expected: (1, 1, 1, 1, None),
            },
            Case {
                name: "1 fully labelled pod and 1 terminating without the gate",
                gate: false,
                rs: fully.clone(),
                active: vec![ready("pod1", &fully, None)],
                terminating: vec![terminating(ready("pod2", &fully, None))],
                expected: (1, 1, 1, 1, None),
            },
            Case {
                name: "1 fully labelled pod and 2 terminating with the gate",
                gate: true,
                rs: fully.clone(),
                active: vec![ready("pod1", &fully, None)],
                terminating: vec![
                    terminating(ready("pod2", &fully, None)),
                    terminating(ready("pod3", &fully, None)),
                ],
                expected: (1, 1, 1, 1, Some(2)),
            },
        ];
        for case in cases {
            let _gate = with_feature(Feature::DeploymentReplicaSetTerminatingReplicas, case.gate);
            let active: Vec<&Pod> = case.active.iter().collect();
            let terminating: Vec<&Pod> = case.terminating.iter().collect();
            let status = calculate_status(&case.rs, &active, &terminating, None, now);
            assert_eq!(
                (
                    status.replicas,
                    status.fully_labeled_replicas,
                    status.ready_replicas,
                    status.available_replicas,
                    status.terminating_replicas
                ),
                (
                    case.expected.0,
                    Some(case.expected.1),
                    case.expected.2,
                    case.expected.3,
                    case.expected.4
                ),
                "{}",
                case.name
            );
        }
    }

    /// `TestCalculateStatusConditions` (replica_set_utils_test.go:332-440):
    /// set once with the diff's reason, kept while it persists, removed when
    /// the error clears.
    #[test]
    fn calculate_status_conditions_follow_upstream_table() {
        let now = chrono::Utc::now();
        let rs = make_replicaset("foobar", make_labels(&[("name", "foo")]), 2);
        let mut failing_rs = make_replicaset("foobar", make_labels(&[("name", "foo")]), 10);
        failing_rs.status = Some(ReplicaSetStatus {
            conditions: Some(vec![rusternetes_common::resources::ReplicaSetCondition {
                condition_type: "ReplicaFailure".to_string(),
                status: "True".to_string(),
                last_transition_time: None,
                reason: None,
                message: None,
            }]),
            ..Default::default()
        });
        let pods = |rs: &ReplicaSet, n: usize| -> Vec<Pod> {
            (0..n)
                .map(|i| ranked_pod(&format!("pod{i}"), rs, "", Phase::Running, Some(now)))
                .collect()
        };
        let conds = |rs: &ReplicaSet, n: usize, err: Option<&str>| {
            let owned = pods(rs, n);
            let refs: Vec<&Pod> = owned.iter().collect();
            calculate_status(rs, &refs, &[], err, now)
                .conditions
                .unwrap_or_default()
        };

        let c = conds(&rs, 1, Some("fake manageReplicasErr"));
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].reason.as_deref(), Some("FailedCreate"), "diff < 0");
        assert_eq!(c[0].message.as_deref(), Some("fake manageReplicasErr"));

        let c = conds(&rs, 3, Some("fake manageReplicasErr"));
        assert_eq!(c[0].reason.as_deref(), Some("FailedDelete"), "diff > 0");

        assert!(
            conds(&failing_rs, 1, None).is_empty(),
            "manageReplicasErr == nil && failureCond != nil removes it"
        );

        let c = conds(&failing_rs, 1, Some("fake manageReplicasErr"));
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].reason, None, "the existing condition is left alone");
        assert_eq!(c[0].message, None);

        assert!(conds(&rs, 1, None).is_empty());
    }

    /// The informer's re-list on (re)connect (`DeltaFIFO.Replace`): a pod
    /// created while the watch was down is observed as an add, exactly once,
    /// so a creation whose Added event was lost does not wedge the sync until
    /// the expectations TTL.
    #[tokio::test]
    async fn rs_relist_observes_creations_the_watch_missed() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs = make_replicaset("gap", make_labels(&[("app", "gap")]), 2);
        let pods = [
            ranked_pod("p1", &rs, "n1", Phase::Pending, None),
            ranked_pod("p2", &rs, "n1", Phase::Pending, None),
        ];
        seed_rs(&storage, &rs, &pods).await;
        // The sync issued two creates; their Added events never arrived.
        controller.set_expectations("default/gap", 2, 0);

        controller.relist_pods(&queue).await;
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((0, 0))
        );

        // A second re-list, and a late Added event for a pod already seen,
        // are updates: they must not lower the count again.
        controller.set_expectations("default/gap", 1, 0);
        controller.relist_pods(&queue).await;
        controller
            .enqueue_owner_replicaset(
                &queue,
                &WatchEvent::Added(
                    "/registry/pods/default/p1".to_string(),
                    serde_json::to_string(&pods[0]).unwrap(),
                ),
            )
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((1, 0)),
            "a pod the informer already holds is not a creation observation"
        );
    }

    /// Pods that vanished while the watch was down are observed as deletes
    /// (`DeletedFinalStateUnknown`), so the delete expectation clears.
    #[tokio::test]
    async fn rs_relist_observes_deletions_the_watch_missed() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs = make_replicaset("gone", make_labels(&[("app", "gone")]), 1);
        let pods = [
            ranked_pod("keep", &rs, "n1", Phase::Pending, None),
            ranked_pod("doomed", &rs, "n1", Phase::Pending, None),
        ];
        seed_rs(&storage, &rs, &pods).await;
        controller.relist_pods(&queue).await;

        controller
            .expectations
            .expect_deletions_of("default/gone", &["default/doomed".to_string()]);
        storage
            .delete(&build_key("pods", Some("default"), "doomed"))
            .await
            .unwrap();
        controller.relist_pods(&queue).await;

        assert_eq!(
            controller.expectations.get_expectations("default/gone"),
            Some((0, 0))
        );
    }

    // ---------------------------------------------------------------------
    // #3633 / #3634: selector semantics and informer event handlers.
    //
    // Ported from upstream pkg/controller/replicaset/replica_set_test.go
    // (TestPodControllerLookup :486, TestUpdatePods :731) and
    // replica_set.go addPod/updatePod/deletePod :415-570 +
    // `metav1.LabelSelectorAsSelector`.
    // ---------------------------------------------------------------------

    pub(crate) fn expr(
        key: &str,
        op: &str,
        values: &[&str],
    ) -> rusternetes_common::types::LabelSelector {
        rusternetes_common::types::LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![rusternetes_common::types::LabelSelectorRequirement {
                key: key.to_string(),
                operator: op.to_string(),
                values: if values.is_empty() {
                    None
                } else {
                    Some(values.iter().map(|v| v.to_string()).collect())
                },
            }]),
        }
    }

    /// Everything currently queued, sorted.
    pub(crate) async fn queued(queue: &WorkQueue) -> Vec<String> {
        let mut keys = Vec::new();
        while queue.len().await > 0 {
            let key = queue.get().await.unwrap();
            queue.done(&key).await;
            keys.push(key);
        }
        keys.sort();
        keys
    }

    pub(crate) fn controlled(
        mut pod: Pod,
        kind: &str,
        name: &str,
        uid: &str,
        controller: bool,
    ) -> Pod {
        pod.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
            controller: Some(controller),
            block_owner_deletion: None,
        }]);
        pod
    }

    pub(crate) fn pod_event(
        make: fn(String, String) -> WatchEvent,
        pod: &Pod,
        rv: &str,
    ) -> WatchEvent {
        let mut pod = pod.clone();
        pod.metadata.resource_version = Some(rv.to_string());
        make(
            format!("/registry/pods/default/{}", pod.metadata.name),
            serde_json::to_string(&pod).unwrap(),
        )
    }

    async fn put_rs(storage: &Arc<MemoryStorage>, rs: &ReplicaSet) {
        storage
            .create(
                &build_key("replicasets", Some("default"), &rs.metadata.name),
                rs,
            )
            .await
            .unwrap();
    }

    /// `claimPods` reads the selector through `LabelSelectorAsSelector`, so a
    /// matchExpressions selector adopts a matching orphan.
    #[tokio::test]
    async fn rs_match_expressions_selector_adopts_orphan() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("expr", make_labels(&[("app", "a")]), 1);
        rs.spec.selector = expr("app", "In", &["a", "b"]);
        let orphan = make_pod("orphan", make_labels(&[("app", "a")]));
        seed_rs(&storage, &rs, &[orphan]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 1, "the orphan fills the one replica");
        let adopted = &pods[0].metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(adopted.uid, rs.metadata.uid);
    }

    /// A pod this RS controls whose labels satisfy a matchExpressions selector
    /// stays claimed, and one that does not is released
    /// (`ControllerRefManager.ClaimObject`).
    #[tokio::test]
    async fn rs_match_expressions_selector_keeps_matching_and_releases_unmatched() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("expr", make_labels(&[("app", "a")]), 1);
        rs.spec.selector = expr("tier", "In", &["db"]);
        let uid = rs.metadata.uid.clone();
        let kept = owned_pod("kept", make_labels(&[("tier", "db")]), "expr", &uid);
        let dropped = owned_pod("dropped", make_labels(&[("tier", "web")]), "expr", &uid);
        seed_rs(&storage, &rs, &[kept, dropped]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let owned_by_rs = |p: &Pod| {
            p.metadata
                .owner_references
                .as_ref()
                .is_some_and(|refs| refs.iter().any(|r| r.uid == uid))
        };
        let kept: Pod = storage
            .get(&build_key("pods", Some("default"), "kept"))
            .await
            .unwrap();
        let dropped: Pod = storage
            .get(&build_key("pods", Some("default"), "dropped"))
            .await
            .unwrap();
        assert!(owned_by_rs(&kept), "tier In [db] still matches");
        assert!(!owned_by_rs(&dropped), "tier=web no longer matches");
    }

    /// `syncReplicaSet`: "error converting pod selector to selector" is
    /// logged and the sync returns nil (replica_set.go:729-733). No pod is
    /// created or claimed.
    #[tokio::test]
    async fn rs_invalid_selector_skips_the_sync() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("bad", make_labels(&[("app", "a")]), 2);
        rs.spec.selector = expr("app", "Bogus", &["a"]);
        seed_rs(&storage, &rs, &[]).await;

        let next = controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(next, None);
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert!(pods.is_empty(), "an invalid selector syncs nothing");
    }

    /// `addPod`: "If it has a ControllerRef, that's all that matters"
    /// (replica_set.go:426-440). `GetControllerOf` returns only the
    /// `controller: true` ref, so a plain owner reference is an orphan.
    #[tokio::test]
    async fn rs_pod_event_ignores_a_non_controller_owner_reference() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs = make_replicaset("g", make_labels(&[("app", "g")]), 1);
        put_rs(&storage, &rs).await;
        controller.set_expectations("default/g", 1, 0);

        // A pod with labels the RS does not select and a non-controller ref.
        let pod = controlled(
            make_pod("p", make_labels(&[("other", "x")])),
            "ReplicaSet",
            "g",
            &rs.metadata.uid,
            false,
        );
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &pod, "1"))
            .await;

        assert!(queued(&queue).await.is_empty());
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((1, 0)),
            "a creation is observed only for the controller reference"
        );
    }

    /// `resolveControllerRef` (replica_set.go:302-321): wrong kind, a missing
    /// RS and a UID mismatch all resolve to nil, and nothing is enqueued or
    /// observed.
    #[tokio::test]
    async fn rs_pod_event_unresolvable_controller_ref_does_nothing() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs = make_replicaset("g", make_labels(&[("app", "g")]), 1);
        put_rs(&storage, &rs).await;
        controller.set_expectations("default/g", 1, 0);
        let labels = make_labels(&[("app", "g")]);

        for (i, (kind, name, uid)) in [
            ("Deployment", "g", rs.metadata.uid.as_str()),
            ("ReplicaSet", "missing", rs.metadata.uid.as_str()),
            ("ReplicaSet", "g", "recreated-uid"),
        ]
        .into_iter()
        .enumerate()
        {
            let pod = controlled(
                make_pod(&format!("p{i}"), labels.clone()),
                kind,
                name,
                uid,
                true,
            );
            controller
                .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &pod, "1"))
                .await;
        }

        assert!(queued(&queue).await.is_empty());
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((1, 0))
        );
    }

    /// `addPod` of an orphan syncs every RS whose selector matches it
    /// (`getPodReplicaSets`, replica_set.go:289-300; the lister skips a pod
    /// without labels and an empty selector) — not every RS in the namespace —
    /// and observes no creation (:441-453). `TestPodControllerLookup`.
    #[tokio::test]
    async fn rs_orphan_pod_added_enqueues_only_matching_replicasets() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let by_labels = make_replicaset("by-labels", make_labels(&[("app", "a")]), 1);
        let mut by_expr = make_replicaset("by-expr", make_labels(&[("app", "a")]), 1);
        by_expr.spec.selector = expr("app", "In", &["a"]);
        let other = make_replicaset("other", make_labels(&[("app", "z")]), 1);
        let mut invalid = make_replicaset("invalid", make_labels(&[("app", "a")]), 1);
        invalid.spec.selector = expr("app", "Bogus", &["a"]);
        let mut empty = make_replicaset("empty", make_labels(&[("app", "a")]), 1);
        empty.spec.selector = rusternetes_common::types::LabelSelector {
            match_labels: None,
            match_expressions: None,
        };
        for rs in [&by_labels, &by_expr, &other, &invalid, &empty] {
            put_rs(&storage, rs).await;
        }
        controller.set_expectations("default/by-labels", 1, 0);

        let orphan = make_pod("orphan", make_labels(&[("app", "a")]));
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &orphan, "1"))
            .await;

        assert_eq!(
            queued(&queue).await,
            vec![
                "replicasets/default/by-expr",
                "replicasets/default/by-labels"
            ]
        );
        assert_eq!(
            controller
                .expectations
                .get_expectations("default/by-labels"),
            Some((1, 0)),
            "no controller waits for an orphan"
        );

        // "pods without labels don't match any ReplicaSets".
        let unlabeled = make_pod("unlabeled", HashMap::new());
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &unlabeled, "1"))
            .await;
        assert!(queued(&queue).await.is_empty());
    }

    /// `deletePod`: "No controller should care about orphans being deleted"
    /// (replica_set.go:553-557).
    #[tokio::test]
    async fn rs_orphan_pod_deleted_enqueues_nothing() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        put_rs(
            &storage,
            &make_replicaset("a", make_labels(&[("app", "a")]), 1),
        )
        .await;
        let orphan = make_pod("orphan", make_labels(&[("app", "a")]));
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &orphan, "1"))
            .await;
        queued(&queue).await;

        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Deleted, &orphan, "2"))
            .await;

        assert!(queued(&queue).await.is_empty());
    }

    /// `TestUpdatePods` (replica_set_test.go:731): case 1, a label change on a
    /// controlled pod syncs the owner only; case 3, dropping the ControllerRef
    /// syncs the former owner and any label-matching RS; an update that changes
    /// nothing about an orphan syncs nobody (:517-528).
    #[tokio::test]
    async fn rs_update_pod_follows_the_controller_ref() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs1 = make_replicaset("foo", make_labels(&[("foo", "bar")]), 1);
        let rs2 = make_replicaset("barfoo", make_labels(&[("bar", "foo")]), 1);
        put_rs(&storage, &rs1).await;
        put_rs(&storage, &rs2).await;

        // Case 1: owned by rs1, labels change to rs2's: only the owner wakes.
        let owned = controlled(
            make_pod("p", make_labels(&[("foo", "bar")])),
            "ReplicaSet",
            "foo",
            &rs1.metadata.uid,
            true,
        );
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &owned, "1"))
            .await;
        queued(&queue).await;
        let mut relabeled = owned.clone();
        relabeled.metadata.labels = Some(make_labels(&[("bar", "foo")]));
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Modified, &relabeled, "2"))
            .await;
        assert_eq!(queued(&queue).await, vec!["replicasets/default/foo"]);

        // Case 3: the ControllerRef is removed: former owner + matching RS.
        let mut orphaned = relabeled.clone();
        orphaned.metadata.owner_references = None;
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Modified, &orphaned, "3"))
            .await;
        assert_eq!(
            queued(&queue).await,
            vec!["replicasets/default/barfoo", "replicasets/default/foo"]
        );

        // An orphan whose labels and ref did not change wakes nobody.
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Modified, &orphaned, "4"))
            .await;
        assert!(queued(&queue).await.is_empty());

        // An orphan that changes labels wakes the RSs matching the new ones.
        let mut moved = orphaned.clone();
        moved.metadata.labels = Some(make_labels(&[("foo", "bar")]));
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Modified, &moved, "5"))
            .await;
        assert_eq!(queued(&queue).await, vec!["replicasets/default/foo"]);
    }

    /// The worker wires `nextSyncDuration` to `queue.AddAfter` after `Forget`
    /// (replica_set.go:786-788): a pod that is ready but not yet available is
    /// re-checked by a second sync when `minReadySeconds` has passed, with no
    /// watch event to wake the key. (`WorkQueue` times on `std::time::Instant`,
    /// so a paused tokio clock cannot drive it; the delay here is real and 1s.)
    #[tokio::test]
    async fn rs_worker_requeues_the_key_after_the_next_sync_duration() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = Arc::new(ReplicaSetController::new(Arc::clone(&storage), 30));
        let mut rs = make_replicaset("avail-later", make_labels(&[("app", "al")]), 1);
        rs.spec.min_ready_seconds = Some(1);
        let pod = ranked_pod("p", &rs, "n1", Phase::Running, Some(chrono::Utc::now()));
        seed_rs(&storage, &rs, &[pod]).await;

        let queue = WorkQueue::new();
        let worker = tokio::spawn({
            let controller = Arc::clone(&controller);
            let queue = queue.clone();
            async move { controller.worker(queue).await }
        });
        queue
            .add("replicasets/default/avail-later".to_string())
            .await;

        let available = async {
            loop {
                let rs: ReplicaSet = storage
                    .get(&build_key("replicasets", Some("default"), "avail-later"))
                    .await
                    .unwrap();
                if rs.status.is_some_and(|s| s.available_replicas == 1) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), available)
            .await
            .expect("the AddAfter must drive a second sync that sees the pod available");
        queue.shutdown().await;
        worker.await.unwrap();
    }
}
