use super::controller_utils::{find_min_next_pod_availability_check, is_pod_active, pod_key};
use super::expectations::ControllerExpectations;
use super::pod_informer::{
    apply_pod_observation, ControllerRef, PodEventTarget, PodObservation, ResolvedController,
    SharedPodInformer,
};
use super::replicaset::{
    count_pods, get_pods_to_delete, indirectly_related_pods, PodCounts, RelatedSet,
};
use crate::controllers::worker_pool::spawn_workers;
use chrono::Utc;
use futures::StreamExt;
use rusternetes_common::{
    resources::{Pod, PodStatus, ReplicationController, ReplicationControllerCondition},
    types::{label_selector_as_selector, ObjectMeta, OwnerReference, Phase, Selector},
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::{sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

/// Workers draining this controller's queue: upstream `ConcurrentRCSyncs` default,
/// pkg/controller/replication/config/v1alpha1/defaults.go:33-34, launched at cmd/kube-controller-manager/app/core.go:551.
const CONCURRENT_RC_SYNCS: usize = 5;

/// ReplicationControllerController reconciles ReplicationController resources
pub struct ReplicationControllerController<S: Storage> {
    pub storage: Arc<S>,
    pub interval: Duration,
    /// In-flight pod creates/deletes this controller has issued but not yet
    /// observed on the pod watch, keyed "ns/name".
    ///
    /// Upstream gates `manageReplicas` on this
    /// (pkg/controller/replicaset/replica_set.go:728, 756) and it is what makes
    /// slow-start batching safe: without it a watch-driven reconcile re-enters
    /// while a whole batch of creates is still in flight, lists pods that do
    /// not include them yet, and issues the batch again.
    pub expectations: Arc<ControllerExpectations>,
    /// The shared pod informer's indexer, as far as the event handlers need
    /// it ([`super::pod_informer`]): the handlers, not the sync, lower
    /// `expectations` (replica_set.go:436, :568).
    pub pod_informer: Arc<SharedPodInformer<S>>,
}

impl<S: Storage + 'static> ReplicationControllerController<S> {
    pub fn new(storage: Arc<S>, interval_secs: u64) -> Self {
        let interval = Duration::from_secs(interval_secs);
        let pod_informer = Arc::new(SharedPodInformer::new(storage.clone(), interval));
        Self::with_pod_informer(storage, interval_secs, pod_informer)
    }

    /// A controller whose pod event handlers hang off an informer shared with
    /// other controllers.
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

    pub async fn run(self: Arc<Self>) -> rusternetes_common::Result<()> {
        info!("ReplicationController controller started (watch-based)");

        let queue = WorkQueue::new();

        // Upstream launches `ConcurrentRCSyncs` workers over one shared queue
        // (`for i := 0; i < workers; i++ { go wait.UntilWithContext(ctx, worker, time.Second) }`);
        // a key in flight is never handed to a second worker.
        spawn_workers(CONCURRENT_RC_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        // Upstream's controller registers its handlers on the shared pod
        // informer (`podInformer.Informer().AddEventHandler`, replica_set.go:207)
        // and the informer factory runs the one watch; the first controller to
        // get here starts it.
        self.pod_informer
            .add_event_handler(Arc::clone(&self), queue.clone())
            .await;
        self.pod_informer.start();

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch for changes to ReplicationControllers AND Pods.
            // Pod watch is required for low-latency status updates: when a pod
            // transitions Pending -> Running or its Ready condition flips, the
            // RC must reconcile immediately so status.readyReplicas reflects
            // reality. Otherwise conformance tests (e.g. "should serve a basic
            // image") time out waiting on the periodic resync.
            let prefix = build_prefix("replicationcontrollers", None);
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
    /// (replica_set.go:415-570; the RC runs on the same code), for one
    /// classified pod event ([`PodInformerCache::on_event`]): resolve the
    /// controller reference, lower the expectations and enqueue the RCs it
    /// names.
    async fn apply_observation(&self, queue: &WorkQueue, observation: &PodObservation) {
        apply_pod_observation(self, queue, observation).await;
    }

    /// One pod watch event through the informer handlers: `addPod`,
    /// `updatePod` or `deletePod`, whichever the pod cache says it is.
    pub async fn enqueue_owner_rc(
        &self,
        queue: &WorkQueue,
        event: &rusternetes_storage::WatchEvent,
    ) {
        if let Some(observation) = self.pod_informer.cache().on_event(event) {
            self.apply_observation(queue, &observation).await;
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
            let storage_key = build_key("replicationcontrollers", Some(ns), name);
            match self
                .storage
                .get::<ReplicationController>(&storage_key)
                .await
            {
                Ok(resource) => match self.reconcile_rc(&resource).await {
                    Ok(next_sync) => {
                        queue.forget(&key).await;
                        // `rsc.queue.AddAfter(key, *nextSyncDuration)`
                        // (replica_set.go:786-788); the RC rides this code.
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
                    // Resource was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<ReplicationController>("/registry/replicationcontrollers/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("replicationcontrollers/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list replicationcontrollers for enqueue: {}", e);
            }
        }
    }

    /// One synchronous pass, for callers that do not run the watch loop: let the
    /// pod informer catch up (the reflector's list, [`Self::relist_pods`]) and
    /// then sync every object once. The informer step is what lowers the
    /// expectations for pods created by an earlier pass, as the pod watch's
    /// Added events do in [`Self::run`] (replica_set.go:436).
    pub async fn reconcile_all(&self) -> rusternetes_common::Result<()> {
        debug!("Reconciling all replicationcontrollers");
        self.relist_pods(&WorkQueue::new()).await;

        // Get all replicationcontrollers
        let prefix = build_prefix("replicationcontrollers", None);
        let rcs: Vec<ReplicationController> = self.storage.list(&prefix).await?;

        for rc in rcs {
            if let Err(e) = self.reconcile_rc(&rc).await {
                error!(
                    "Error reconciling replicationcontroller {}: {}",
                    rc.metadata.name, e
                );
            }
        }

        Ok(())
    }

    pub async fn reconcile_rc(
        &self,
        rc: &ReplicationController,
    ) -> rusternetes_common::Result<Option<Duration>> {
        let namespace = rc.metadata.namespace.as_deref().unwrap_or("default");
        let rc_key = format!("{}/{}", namespace, rc.metadata.name);

        // If RC is being deleted with Orphan policy, remove ownerReferences from pods
        // and remove the orphan finalizer, then delete the RC.
        if rc.metadata.deletion_timestamp.is_some() {
            let has_orphan_finalizer = rc
                .metadata
                .finalizers
                .as_ref()
                .is_some_and(|f| f.contains(&"orphan".to_string()));
            if has_orphan_finalizer {
                info!(
                    "RC {}/{} being deleted with orphan policy, removing ownerRefs from pods",
                    namespace, rc.metadata.name
                );
                // Remove ownerReferences from all owned pods.
                // K8s orphan processing: must remove ALL ownerRefs before removing
                // the finalizer. If any PATCH fails (CAS conflict), retry on the
                // next reconcile cycle instead of deleting the RC with un-orphaned pods.
                let pods_prefix = build_prefix("pods", Some(namespace));
                let all_pods: Vec<Pod> = self.storage.list(&pods_prefix).await?;
                let mut all_orphaned = true;
                for pod in &all_pods {
                    let owned = pod
                        .metadata
                        .owner_references
                        .as_ref()
                        .is_some_and(|refs| refs.iter().any(|r| r.uid == rc.metadata.uid));
                    if owned {
                        let mut updated_pod = pod.clone();
                        updated_pod.metadata.owner_references =
                            updated_pod.metadata.owner_references.map(|refs| {
                                refs.into_iter()
                                    .filter(|r| r.uid != rc.metadata.uid)
                                    .collect()
                            });
                        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                        if let Err(e) = self.storage.update(&pod_key, &updated_pod).await {
                            debug!(
                                "Failed to orphan pod {}: {} — will retry",
                                pod.metadata.name, e
                            );
                            all_orphaned = false;
                        }
                    }
                }
                // Only remove the finalizer and delete if ALL pods were orphaned.
                // If any failed, return and retry on the next cycle.
                if !all_orphaned {
                    info!(
                        "RC {}/{} orphan incomplete, will retry next cycle",
                        namespace, rc.metadata.name
                    );
                    return Ok(None);
                }
                // Remove orphan finalizer and delete the RC
                let mut updated_rc = rc.clone();
                if let Some(ref mut finalizers) = updated_rc.metadata.finalizers {
                    finalizers.retain(|f| f != "orphan");
                }
                let rc_key =
                    build_key("replicationcontrollers", Some(namespace), &rc.metadata.name);
                if updated_rc
                    .metadata
                    .finalizers
                    .as_ref()
                    .is_none_or(|f| f.is_empty())
                {
                    let _ = self.storage.delete(&rc_key).await;
                } else {
                    let _ = self.storage.update(&rc_key, &updated_rc).await;
                }
                return Ok(None);
            }
            // If being deleted without orphan, skip reconciliation (let it terminate)
            self.expectations.delete_expectations(&rc_key);
            return Ok(None);
        }

        debug!(
            "Reconciling replicationcontroller: {}/{}",
            namespace, rc.metadata.name
        );

        // Read expectations BEFORE listing pods. Upstream does the same
        // (replica_set.go:728 precedes the pod list at :736) and its
        // `TestRSSyncExpectations` explains why: if the list is taken first and
        // expectations checked second, a pod arriving in between makes the
        // record look fulfilled while the list still lacks that pod — and the
        // controller creates a duplicate.
        let needs_sync = self.expectations.satisfied(&rc_key);

        // `metav1.LabelSelectorAsSelector(rs.Spec.Selector)` over the RC's
        // converted selector: an invalid one is logged and the sync returns
        // nil (replica_set.go:729-733).
        let selector = match rc_selector_of(rc) {
            Ok(selector) => selector,
            Err(e) => {
                error!(
                    "error converting pod selector to selector for rc {}: {}",
                    rc_key, e
                );
                return Ok(None);
            }
        };

        // Get all pods in namespace
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods: Vec<Pod> = self.storage.list(&pods_prefix).await?;
        debug!(
            "Found {} total pods in namespace {}",
            all_pods.len(),
            namespace
        );

        // Adopt orphan pods and release non-matching owned pods
        // (`claimPods`, replica_set.go:792-806).
        let all_pods = self
            .adopt_and_release(rc, &selector, all_pods, namespace)
            .await?;

        // `activePods`: owned by this RC (after adopt/release) and
        // `FilterActivePods` (controller_utils.go:1001): not Failed/Succeeded,
        // not terminating. They are what `manageReplicas` and `calculateStatus`
        // both read, and `manageReplicas` does NOT modify them
        // (replica_set.go:597), so pods created or deleted this sync show up
        // in the status of the sync their watch events trigger (:743).
        let active_rc_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| self.is_owned_by(p, rc))
            .filter(|p| is_pod_active(p))
            .collect();

        let current_replicas = active_rc_pods.len() as i32;
        let desired_replicas = rc.spec.replicas.unwrap_or(1);

        debug!(
            "ReplicationController {}/{}: current={}, desired={}",
            namespace, rc.metadata.name, current_replicas, desired_replicas
        );

        // Upstream's `manageReplicasErr`: recorded as the ReplicaFailure
        // condition by `calculateStatus` and returned from the sync AFTER the
        // status update, so the workqueue retries with backoff
        // (replica_set.go:755-774).
        let mut manage_error: Option<String> = None;
        // Upstream: `if rsNeedsSync && rs.DeletionTimestamp == nil { manageReplicas }`
        // (replica_set.go:757). The deletionTimestamp half is handled by the
        // early return above. Status is still recalculated below either way —
        // upstream "always updates status as pods come up or die".
        if needs_sync {
            manage_error = self.manage_replicas(&active_rc_pods, &all_pods, rc).await;
        } else {
            debug!(
                "RC {}: waiting on in-flight pod creates/deletes, skipping manage",
                rc_key
            );
        }

        // "Use the same time for calculating status and nextSyncDuration"
        // (replica_set.go:745-746).
        let now = Utc::now();
        let min_ready_seconds = rc.spec.min_ready_seconds.unwrap_or(0);
        let counts = count_pods(
            rc.spec
                .template
                .metadata
                .as_ref()
                .and_then(|m| m.labels.as_ref()),
            min_ready_seconds,
            &active_rc_pods,
            now,
        );

        // K8s only sets ReplicaFailure from actual pod creation errors
        // (manageReplicas return value), NOT from existing Failed pods.
        // See: pkg/controller/replication/replication_controller.go — syncReplicationController
        self.update_status(
            rc,
            counts,
            manage_error.as_deref(),
            super::replicaset::replica_failure_reason(current_replicas, desired_replicas),
        )
        .await?;

        // `if manageReplicasErr != nil { return manageReplicasErr }` after the
        // status update (replica_set.go:772-774): the error requeues the key
        // with rate-limited backoff (see `worker`).
        if let Some(e) = manage_error {
            return Err(rusternetes_common::Error::Internal(e));
        }

        // Plan the next availability check (replica_set.go:775-788).
        if min_ready_seconds > 0 && counts.ready != counts.available {
            return Ok(Some(
                find_min_next_pod_availability_check(
                    &active_rc_pods,
                    min_ready_seconds,
                    now,
                    Utc::now(),
                )
                .unwrap_or(Duration::from_secs(min_ready_seconds as u64)),
            ));
        }

        Ok(None)
    }

    /// `manageReplicas` (replica_set.go:601-702): create or delete pods to
    /// close the gap between `activePods` and `.spec.replicas`. Returns the
    /// error upstream returns, if any. `all_pods` is what the pod lister would
    /// hold, for `getIndirectlyRelatedPods`.
    async fn manage_replicas(
        &self,
        active_rc_pods: &[&Pod],
        all_pods: &[Pod],
        rc: &ReplicationController,
    ) -> Option<String> {
        let namespace = rc.metadata.namespace.as_deref().unwrap_or("default");
        let rc_key = format!("{}/{}", namespace, rc.metadata.name);
        let current_replicas = active_rc_pods.len() as i32;
        let desired_replicas = rc.spec.replicas.unwrap_or(1);
        let mut manage_error: Option<String> = None;

        if current_replicas < desired_replicas {
            // Create the missing pods in upstream's slow-start batches, every
            // pod within a batch concurrently — `slowStartBatch` spawns a
            // goroutine per pod and waits for the batch
            // (pkg/controller/replicaset/replica_set.go:826-835).
            //
            // Serially, this loop could not keep up: `create_pod` is a
            // ServiceAccount GET, a ResourceQuota LIST and the pod CREATE, and
            // on the kine leg those measured ~4s in total, so 100 replicas
            // needed ~400s against the conformance specs' 120s
            // `replicaSyncTimeout`. Three `[sig-api-machinery] Garbage
            // collector` specs failed in setup on `rc.Status.Replicas (0)`
            // never reaching `Spec.Replicas (100)` (#1847).
            let to_create = ((desired_replicas - current_replicas) as usize).min(BURST_REPLICAS);
            // Record the creates before issuing them, so a reconcile triggered
            // by our own pod-create watch events does not re-issue the batch
            // (upstream `ExpectCreations`, replica_set.go:619).
            self.expectations
                .expect_creations(&rc_key, to_create as i64);
            // Pods this reconcile successfully asked the backend to create.
            // Only these are worth waiting to observe — a create that failed
            // will never produce a pod.
            let mut created = 0usize;
            for batch in slow_start_batches(to_create, SLOW_START_INITIAL_BATCH_SIZE) {
                let results = futures::future::join_all(
                    (0..batch).map(|i| self.create_pod(rc, (created + i) as i32)),
                )
                .await;

                let mut batch_failure = None;
                for result in results {
                    // `HasStatusCause(err, v1.NamespaceTerminatingCause)`
                    // -> `return nil` inside the `slowStartBatch`
                    // callback (replica_set.go:630-639): the rejected
                    // create is a success to `slowStartBatch`, so it is
                    // neither a ReplicaFailure nor a reason to stop, and
                    // it counts toward `successfulCreations`.
                    let result = match result {
                        Err(e) if is_namespace_terminating_rejection(&e) => Ok(()),
                        other => other,
                    };
                    match result {
                        Ok(()) => created += 1,
                        Err(e) => {
                            error!("Failed to create pod for RC {}: {}", rc.metadata.name, e);
                            batch_failure.get_or_insert_with(|| e.to_string());
                        }
                    }
                }

                // Upstream stops at the first failing batch and reports what
                // succeeded (`replica_set.go:838-840`), rather than pushing
                // the remaining rounds at an api-server that is already
                // rejecting — the next reconcile retries from the new count.
                if let Some(e) = batch_failure {
                    manage_error = Some(e);
                    break;
                }
            }
            // Creates we never attempted must not be waited on: upstream
            // decrements the expectation for each skipped pod, because no
            // watch event will ever arrive for them (replica_set.go:644-650).
            // The creates that did land are observed by the pod watch, as
            // upstream's informer observes them (`addPod`, :436).
            let skipped = to_create.saturating_sub(created);
            for _ in 0..skipped {
                self.expectations.creation_observed(&rc_key);
            }
        } else if current_replicas > desired_replicas {
            // Need to delete excess pods; capped at burstReplicas (:653-655).
            let to_delete = ((current_replicas - desired_replicas) as usize).min(BURST_REPLICAS);
            // `getIndirectlyRelatedPods` then `getPodsToDelete` (:658-662):
            // "Choose which Pods to delete, preferring those in earlier phases
            // of startup".
            let related = self.get_indirectly_related_pods(rc, all_pods).await;
            let doomed = get_pods_to_delete(active_rc_pods, &related, to_delete);
            // Snapshot the doomed pods' keys so each is observed exactly
            // once, as a deletionTimestamp update or as the delete
            // (`ExpectDeletions`, replica_set.go:670).
            let doomed_keys: Vec<String> = doomed.iter().map(|p| pod_key(p)).collect();
            self.expectations.expect_deletions_of(&rc_key, &doomed_keys);
            // The deletes run concurrently and every one is attempted; only
            // the first error is reported, after the status update
            // (replica_set.go:672-698).
            let results = futures::future::join_all(
                doomed
                    .iter()
                    .map(|pod| self.delete_pod(&pod.metadata.name, namespace)),
            )
            .await;
            for (result, pod_key) in results.into_iter().zip(&doomed_keys) {
                if let Err(e) = result {
                    // The informer will not observe this deletion
                    // (replica_set.go:679-686).
                    self.expectations.deletion_observed_of(&rc_key, pod_key);
                    if !matches!(e, rusternetes_common::Error::NotFound(_)) {
                        manage_error.get_or_insert_with(|| e.to_string());
                    }
                }
            }
        }
        manage_error
    }

    /// `getIndirectlyRelatedPods` (replica_set.go:848-872) for the RC, which
    /// upstream runs as a converted ReplicaSet: the pods selected by any RC
    /// sharing this one's controller (none when it has no controller, the
    /// usual case).
    async fn get_indirectly_related_pods<'a>(
        &self,
        rc: &ReplicationController,
        pods: &'a [Pod],
    ) -> Vec<&'a Pod> {
        let namespace = rc.metadata.namespace.as_deref().unwrap_or("default");
        let controllers: Vec<ReplicationController> = match self
            .storage
            .list(&build_prefix("replicationcontrollers", Some(namespace)))
            .await
        {
            Ok(items) => items,
            Err(e) => {
                error!("listing ReplicationControllers for related pods: {}", e);
                Vec::new()
            }
        };
        let sets: Vec<RelatedSet<'_>> = controllers
            .iter()
            .map(|c| RelatedSet {
                metadata: &c.metadata,
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: c.spec.selector.clone(),
                    match_expressions: None,
                },
            })
            .collect();
        indirectly_related_pods(&rc.metadata, &sets, pods)
    }

    /// Check if a pod is owned by this RC (has a controller ownerReference pointing to it)
    fn is_owned_by(&self, pod: &Pod, rc: &ReplicationController) -> bool {
        // K8s uses UID matching for ownership, not name matching.
        // Name matching can cause false positives across namespaces or after recreation.
        // See: pkg/controller/replication/replication_controller.go
        pod.metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter().any(|r| {
                    r.kind == "ReplicationController"
                        && r.uid == rc.metadata.uid
                        && r.controller == Some(true)
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
        rc: &ReplicationController,
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
            let owned = self.is_owned_by(pod, rc);

            if labels_match && !owned && !self.has_controller_owner(pod) {
                // Adopt orphan pod: labels match, no controller owner
                let mut adopted_pod = pod.clone();
                let owner_ref = OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "ReplicationController".to_string(),
                    name: rc.metadata.name.clone(),
                    uid: rc.metadata.uid.clone(),
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
                            "Adopted orphan pod {} into RC {}/{}",
                            pod.metadata.name, namespace, rc.metadata.name
                        );
                        all_pods[i] = adopted_pod;
                    }
                    Err(e) => {
                        debug!("Failed to adopt pod {}: {}", pod.metadata.name, e);
                    }
                }
            } else if !labels_match && owned {
                // Release owned pod: labels no longer match
                let mut released_pod = pod.clone();
                if let Some(refs) = &mut released_pod.metadata.owner_references {
                    refs.retain(|r| {
                        !(r.kind == "ReplicationController"
                            && r.name == rc.metadata.name
                            && r.controller == Some(true))
                    });
                    if refs.is_empty() {
                        released_pod.metadata.owner_references = None;
                    }
                }

                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                match self.storage.update(&pod_key, &released_pod).await {
                    Ok(_) => {
                        info!(
                            "Released pod {} from RC {}/{}",
                            pod.metadata.name, namespace, rc.metadata.name
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

    async fn create_pod(
        &self,
        rc: &ReplicationController,
        _index: i32,
    ) -> rusternetes_common::Result<()> {
        let namespace = rc.metadata.namespace.as_deref().unwrap_or("default");

        // Name is generated per attempt below, so a collision can be retried
        // under a new one (upstream `createWithGenerateNameRetry`).
        let pod_name = generate_pod_name(&rc.metadata.name);

        let mut metadata = ObjectMeta::new(&pod_name);
        metadata.namespace = Some(namespace.to_string());
        // Use template labels, falling back to the RC's selector labels.
        // In K8s, template labels must be a superset of selector labels.
        // If the template has no labels at all, use the selector to ensure
        // created pods can be matched by the controller.
        metadata.labels = rc
            .spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .or_else(|| rc.spec.selector.clone());
        metadata.owner_references = Some(vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "ReplicationController".to_string(),
            name: rc.metadata.name.clone(),
            uid: rc.metadata.uid.clone(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);

        // DefaultTolerationSeconds admission (#442): controllers bypass the
        // api-server admission path that adds these NoExecute tolerations.
        let mut pod_spec = rc.spec.template.spec.clone();
        rusternetes_common::tolerations::add_default_tolerations(&mut pod_spec);

        let mut pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            spec: Some(pod_spec),
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

        // Inject the ServiceAccount token volume and propagate the SA's
        // imagePullSecrets exactly as the api-server's admission does for HTTP
        // pod creates (#1118). Controllers write pods straight to storage and
        // bypass that admission path; mirrors the ReplicaSet wiring — upstream
        // Go's RC controller IS the ReplicaSet controller via adapters.
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
        }

        // Retry a name collision under a freshly generated name, as upstream's
        // api-server does for `generateName` objects
        // (registry/generic/registry/store.go:464-475). Our controllers write
        // pods with a client-side name rather than through the generateName
        // path, so the retry has to live here.
        //
        // Any other error is returned to the caller on the first attempt: only
        // AlreadyExists is worth a new name.
        let mut attempt_name = pod_name;
        for attempt in 0..MAX_NAME_GENERATION_CREATE_ATTEMPTS {
            pod.metadata.name = attempt_name.clone();
            let key = build_key("pods", Some(namespace), &attempt_name);
            match self.storage.create(&key, &pod).await {
                Ok(_) => {
                    info!(
                        "Created pod {}/{} for replicationcontroller {}",
                        namespace, attempt_name, rc.metadata.name
                    );
                    return Ok(());
                }
                Err(rusternetes_common::Error::AlreadyExists(_))
                    if attempt + 1 < MAX_NAME_GENERATION_CREATE_ATTEMPTS =>
                {
                    debug!(
                        "Pod name {}/{} already taken, regenerating (attempt {})",
                        namespace,
                        attempt_name,
                        attempt + 1
                    );
                    attempt_name = generate_pod_name(&rc.metadata.name);
                }
                Err(e) => return Err(e),
            }
        }

        Err(rusternetes_common::Error::AlreadyExists(format!(
            "could not generate a free pod name for {}/{} after {} attempts",
            namespace, rc.metadata.name, MAX_NAME_GENERATION_CREATE_ATTEMPTS
        )))
    }

    async fn delete_pod(&self, name: &str, namespace: &str) -> rusternetes_common::Result<()> {
        let key = build_key("pods", Some(namespace), name);
        // Upstream `RealPodControl.DeletePod` is a graceful delete,
        // `Pods(ns).Delete(ctx, podID, metav1.DeleteOptions{})`
        // (pkg/controller/controller_utils.go:618).
        self.storage.delete_gracefully(&key).await?;

        info!("Deleted pod {}/{}", namespace, name);

        Ok(())
    }

    pub async fn update_status(
        &self,
        rc: &ReplicationController,
        counts: PodCounts,
        failure_message: Option<&str>,
        failure_reason: Option<&str>,
    ) -> rusternetes_common::Result<()> {
        let namespace = rc.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key("replicationcontrollers", Some(namespace), &rc.metadata.name);

        // Base the write on the object this sync read (upstream
        // `updateReplicationControllerStatus`,
        // pkg/controller/replication/conversion.go, hands the rc to
        // `UpdateStatus` and its resourceVersion is the precondition).
        let mut updated_rc: ReplicationController = rc.clone();

        // Build conditions: preserve existing conditions of unknown types,
        // only manage "ReplicaFailure" condition type
        let existing_conditions = updated_rc
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .cloned()
            .unwrap_or_default();

        // `calculateStatus` (replica_set_utils.go:124-136; the RC rides the RS
        // controller upstream): ReplicaFailure is only SET when none exists and
        // only REMOVED when there is no error, so a persisting failure keeps its
        // lastTransitionTime and message (`SetCondition`, :169-176).
        let mut conditions: Vec<_> = existing_conditions;
        let has_failure_condition = conditions
            .iter()
            .any(|c| c.condition_type == "ReplicaFailure");
        match failure_message {
            Some(msg) if !has_failure_condition => {
                conditions.push(ReplicationControllerCondition {
                    condition_type: "ReplicaFailure".to_string(),
                    status: "True".to_string(),
                    last_transition_time: Some(Utc::now()),
                    reason: failure_reason.map(str::to_string),
                    message: Some(msg.to_string()),
                });
            }
            None if has_failure_condition => {
                conditions.retain(|c| c.condition_type != "ReplicaFailure");
            }
            _ => {}
        }

        let final_conditions = if conditions.is_empty() {
            None
        } else {
            Some(conditions)
        };

        let new_status = rusternetes_common::resources::ReplicationControllerStatus {
            replicas: counts.replicas,
            fully_labeled_replicas: Some(counts.fully_labeled),
            ready_replicas: Some(counts.ready),
            available_replicas: Some(counts.available),
            observed_generation: updated_rc.metadata.generation,
            conditions: final_conditions,
        };

        // Only write if status actually changed to avoid unnecessary storage writes
        // that increment resourceVersion and cause conflicts for concurrent clients
        if updated_rc.status.as_ref() == Some(&new_status) {
            debug!(
                "Status unchanged for replicationcontroller {}/{}, skipping write",
                namespace, rc.metadata.name
            );
            return Ok(());
        }

        updated_rc.status = Some(new_status);

        // update_status_cas, NOT update: a full-object PUT has its `.status`
        // stripped by any api-server that exposes a status subresource (see
        // crates/storage/src/api_storage.rs; #1712 class), and it must be
        // conditional on the resourceVersion of the rc this sync read (#2153).
        // A Conflict is returned so the worker requeues rate-limited, as
        // upstream's `syncReplicationController` returns the status error to
        // the workqueue; NotFound means deleted mid-sync.
        match self.storage.update_status_cas(&key, &updated_rc).await {
            Ok(_) => {}
            Err(rusternetes_common::Error::NotFound(_)) => {}
            Err(e) => return Err(e),
        }

        debug!(
            "Updated status for replicationcontroller {}/{}",
            namespace, rc.metadata.name
        );

        Ok(())
    }
}

/// `Convert_v1_ReplicationControllerSpec_To_apps_ReplicaSetSpec`
/// (pkg/apis/core/v1/conversion.go:117-130) then `LabelSelectorAsSelector`:
/// an absent selector converts to a nil `LabelSelector` (`labels.Nothing()`),
/// a present one — even `{}` — to `MatchLabels` (`labels.Everything()` when
/// empty).
fn rc_selector_of(rc: &ReplicationController) -> Result<Selector, String> {
    let converted =
        rc.spec
            .selector
            .as_ref()
            .map(|match_labels| rusternetes_common::types::LabelSelector {
                match_labels: Some(match_labels.clone()),
                match_expressions: None,
            });
    label_selector_as_selector(converted.as_ref())
}

/// The ReplicationController half of the pod event handlers: the RC rides the
/// ReplicaSet controller (replication_controller.go:57-79) with its lister
/// adapters, so `resolveControllerRef` is the RS one over the RC lister.
impl<S: Storage + 'static> PodEventTarget for ReplicationControllerController<S> {
    fn queue_prefix(&self) -> &'static str {
        "replicationcontrollers"
    }

    /// `resolveControllerRef` (replica_set.go:302-321).
    async fn resolve_controller_ref(
        &self,
        namespace: &str,
        controller_ref: &ControllerRef,
    ) -> Option<ResolvedController> {
        if controller_ref.kind != "ReplicationController" {
            return None;
        }
        let rc: ReplicationController = self
            .storage
            .get(&build_key(
                "replicationcontrollers",
                Some(namespace),
                &controller_ref.name,
            ))
            .await
            .ok()?;
        (rc.metadata.uid == controller_ref.uid).then(|| ResolvedController {
            name: rc.metadata.name.clone(),
            min_ready_seconds: rc.spec.min_ready_seconds.unwrap_or(0),
        })
    }

    /// `ReplicationControllerLister.GetPodControllers` (client-go
    /// `listers/core/v1/replicationcontroller_expansion.go:36-66`): a pod
    /// without labels matches none, and "an rc with a nil or empty selector
    /// creeps in, it should match nothing, not everything".
    async fn pod_controllers(
        &self,
        namespace: &str,
        pod_labels: &HashMap<String, String>,
    ) -> Vec<String> {
        if pod_labels.is_empty() {
            return Vec::new();
        }
        let Ok(controllers) = self
            .storage
            .list::<ReplicationController>(&build_prefix("replicationcontrollers", Some(namespace)))
            .await
        else {
            return Vec::new();
        };
        controllers
            .into_iter()
            .filter(|rc| {
                rc.spec.selector.as_ref().is_some_and(|selector| {
                    !selector.is_empty()
                        && selector.iter().all(|(k, v)| pod_labels.get(k) == Some(v))
                })
            })
            .map(|rc| rc.metadata.name)
            .collect()
    }

    fn creation_observed(&self, key: &str) {
        self.expectations.creation_observed(key);
    }

    fn deletion_observed(&self, key: &str, pod_key: &str) {
        self.expectations.deletion_observed_of(key, pod_key);
    }
}

/// How many pods to create per round, following upstream's `slowStartBatch`
/// (pkg/controller/replicaset/replica_set.go:820-844): start at
/// `initial_batch_size`, double each round, and cap every batch at what
/// remains.
///
/// Upstream states the round count as
/// `1+floor(log_2(ceil(N/SlowStartInitialBatchSize)))`
/// (pkg/controller/controller_utils.go:84-86) — 7 rounds for 100 pods rather
/// than 100. The doubling is a damage bound as much as a speed-up: when
/// creates are being rejected (quota, admission) the first failing round costs
/// one create, not N.
pub fn slow_start_batches(count: usize, initial_batch_size: usize) -> Vec<usize> {
    slow_start_batches_capped(count, initial_batch_size, usize::MAX)
}

/// `slow_start_batches` with each batch additionally capped at `max_batch_size`,
/// as the StatefulSet controller's `slowStartBatch` does
/// (pkg/controller/statefulset/stateful_set_control.go:334,
/// `MaxBatchSize = 500` at :42).
pub fn slow_start_batches_capped(
    count: usize,
    initial_batch_size: usize,
    max_batch_size: usize,
) -> Vec<usize> {
    let mut batches = Vec::new();
    let mut remaining = count;
    let mut batch = initial_batch_size.min(remaining);
    while batch > 0 {
        batches.push(batch);
        remaining -= batch;
        batch = (2 * batch).min(remaining).min(max_batch_size);
    }
    batches
}

/// Whether a pod-create failure is the api-server refusing to add content to a
/// namespace that is being terminated.
///
/// Upstream pairs the admission rejection with a controller that expects it:
///
/// ```text
/// if apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
///     // if the namespace is being terminated, we don't have to do
///     // anything because any creation will fail
///     return nil
/// }
/// ```
///
/// (pkg/controller/replicaset/replica_set.go:632-637). It is deliberately NOT
/// a `ReplicaFailure`: the namespace is going away, so there is nothing wrong
/// with this ReplicaSet and nothing for an operator to act on.
///
/// Upstream matches the structured `StatusCause`; our storage layer renders an
/// api-server error into a string (`map_write_err`, crates/storage/src/
/// api_storage.rs), so we match the cause reason or the message text the
/// api-server emits (crates/middleware/src/lib.rs, added in #1849). Both
/// spellings are accepted so this keeps working if the cause is ever surfaced
/// structurally.
pub fn is_namespace_terminating_rejection(err: &rusternetes_common::Error) -> bool {
    let rusternetes_common::Error::Forbidden(message) = err else {
        return false;
    };
    message.contains("NamespaceTerminating") || message.contains("because it is being terminated")
}

/// How many times a create is retried under a freshly generated name when the
/// name collides.
///
/// Upstream's `maxNameGenerationCreateAttempts`
/// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:440),
/// whose comment gives the arithmetic that makes this mandatory rather than
/// defensive:
///
/// ```text
/// Without retry, a 0.1% probability occurs at ~500
/// generated names and a 50% probability occurs at ~4500
/// generated names.
/// ```
pub const MAX_NAME_GENERATION_CREATE_ATTEMPTS: usize = 8;

/// Alphabet for generated name suffixes.
///
/// Upstream's `utilrand.String` alphabet
/// (staging/src/k8s.io/apiserver/pkg/storage/names/generate.go:53, via
/// k8s.io/apimachinery/pkg/util/rand): 27 consonants and digits, chosen to
/// avoid accidentally spelling words. It also matters for collisions — 27^5 is
/// ~14.3M names against ~1M for the 5 hex characters this previously took from
/// a UUID, i.e. a 14x larger space before the retry above is even needed.
const NAME_SUFFIX_ALPHABET: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";

/// Length of a generated name suffix. Upstream's `randomLength`
/// (`generate.go:45`).
const NAME_SUFFIX_LENGTH: usize = 5;

/// A `<base>-<suffix>` name, as upstream's `simpleNameGenerator` builds it.
pub fn generate_pod_name(base: &str) -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let suffix: String = (0..NAME_SUFFIX_LENGTH)
        .map(|_| NAME_SUFFIX_ALPHABET[rng.random_range(0..NAME_SUFFIX_ALPHABET.len())] as char)
        .collect();
    format!("{base}-{suffix}")
}

/// Most pods one sync will create or delete for a single ReplicationController.
///
/// Upstream's `BurstReplicas` (pkg/controller/replicaset/replica_set.go:72),
/// applied to the diff in both directions (`:611-612` creates, `:653-654`
/// deletes).
///
/// Slow-start batching does not bound this on its own — its batches double
/// without limit — so without the cap one large object can issue an unbounded
/// burst at an api-server it shares with every other client. The cap is a
/// throttle rather than a ceiling: whatever is left is created by the next
/// sync.
pub const BURST_REPLICAS: usize = 500;

/// Upstream's `SlowStartInitialBatchSize` (pkg/controller/controller_utils.go).
pub const SLOW_START_INITIAL_BATCH_SIZE: usize = 1;
