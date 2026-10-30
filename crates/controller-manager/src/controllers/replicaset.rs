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
    pub storage: Arc<S>,
    pub interval: Duration,
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
    pub expectations: Arc<ControllerExpectations>,
    /// What the shared pod informer's indexer gives upstream's event handlers:
    /// add-versus-update, and the adds/deletes a watch gap hid
    /// ([`super::pod_informer`]). The handlers, not the sync, lower
    /// `expectations` (replica_set.go:436, :568).
    pub pod_informer: Arc<SharedPodInformer<S>>,
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
    pub fn expectations_satisfied(&self, key: &str) -> bool {
        self.expectations.satisfied(key)
    }

    /// Record that the controller is about to create `adds` and delete `dels`
    /// pods; reconciliation is then gated until they are observed.
    pub fn set_expectations(&self, key: &str, adds: i64, dels: i64) {
        self.expectations.set_expectations(key, adds, dels);
    }

    /// Lower the outstanding-create count after observing a pod creation (or
    /// after a create call failed, so we don't wait on a pod that never lands).
    pub fn observe_creation(&self, key: &str) {
        self.expectations.creation_observed(key);
    }

    /// Lower the outstanding-delete count after observing `pod_key` being
    /// deleted — either its deletionTimestamp being set or its final removal.
    /// Counted once per expected pod (`UIDTrackingControllerExpectations.
    /// DeletionObserved`, `pkg/controller/controller_utils.go:380-390`).
    pub fn observe_deletion(&self, key: &str, pod_key: &str) {
        self.expectations.deletion_observed_of(key, pod_key);
    }

    /// Drop all expectations for a ReplicaSet (e.g. when it is deleted).
    pub fn clear_expectations(&self, key: &str) {
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
    pub async fn worker(&self, queue: WorkQueue) {
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
    pub async fn relist_pods(&self, queue: &WorkQueue) {
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
    pub async fn enqueue_owner_replicaset(&self, queue: &WorkQueue, event: &WatchEvent) {
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
    pub async fn reconcile_replicaset(
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
    pub async fn update_status(
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
pub struct PodCounts {
    pub replicas: i32,
    pub fully_labeled: i32,
    pub ready: i32,
    pub available: i32,
}

/// `calculateStatus`'s loop (replica_set_utils.go:100-118): a pod is fully
/// labeled when the TEMPLATE labels are a subset of its labels ("the matching
/// pods may have more labels than are in the template"), ready per
/// `IsPodReady`, available per `IsPodAvailable` at `now`.
pub fn count_pods(
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
pub fn calculate_status(
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
pub fn get_pods_to_delete<'a>(
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
pub struct RelatedSet<'a> {
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
pub fn indirectly_related_pods<'a>(
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
pub fn replica_failure_reason(active: i32, desired: i32) -> Option<&'static str> {
    match active.cmp(&desired) {
        std::cmp::Ordering::Less => Some("FailedCreate"),
        std::cmp::Ordering::Greater => Some("FailedDelete"),
        std::cmp::Ordering::Equal => None,
    }
}
