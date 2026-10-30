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
    storage: Arc<S>,
    interval: Duration,
    /// In-flight pod creates/deletes this controller has issued but not yet
    /// observed on the pod watch, keyed "ns/name".
    ///
    /// Upstream gates `manageReplicas` on this
    /// (pkg/controller/replicaset/replica_set.go:728, 756) and it is what makes
    /// slow-start batching safe: without it a watch-driven reconcile re-enters
    /// while a whole batch of creates is still in flight, lists pods that do
    /// not include them yet, and issues the batch again.
    expectations: Arc<ControllerExpectations>,
    /// The shared pod informer's indexer, as far as the event handlers need
    /// it ([`super::pod_informer`]): the handlers, not the sync, lower
    /// `expectations` (replica_set.go:436, :568).
    pod_informer: Arc<SharedPodInformer<S>>,
}

impl<S: Storage + 'static> ReplicationControllerController<S> {
    #[allow(dead_code)] // the binary shares one informer via `with_pod_informer`
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
    /// (replica_set.go:415-570; the RC runs on the same code), for one
    /// classified pod event ([`PodInformerCache::on_event`]): resolve the
    /// controller reference, lower the expectations and enqueue the RCs it
    /// names.
    async fn apply_observation(&self, queue: &WorkQueue, observation: &PodObservation) {
        apply_pod_observation(self, queue, observation).await;
    }

    /// One pod watch event through the informer handlers: `addPod`,
    /// `updatePod` or `deletePod`, whichever the pod cache says it is.
    #[cfg(test)]
    async fn enqueue_owner_rc(&self, queue: &WorkQueue, event: &rusternetes_storage::WatchEvent) {
        if let Some(observation) = self.pod_informer.cache().on_event(event) {
            self.apply_observation(queue, &observation).await;
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

    #[allow(dead_code)]
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

    async fn reconcile_rc(
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

    async fn update_status(
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
pub(crate) fn slow_start_batches(count: usize, initial_batch_size: usize) -> Vec<usize> {
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
pub(crate) const MAX_NAME_GENERATION_CREATE_ATTEMPTS: usize = 8;

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
pub(crate) fn generate_pod_name(base: &str) -> String {
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
pub(crate) const BURST_REPLICAS: usize = 500;

/// Upstream's `SlowStartInitialBatchSize` (pkg/controller/controller_utils.go).
pub(crate) const SLOW_START_INITIAL_BATCH_SIZE: usize = 1;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rusternetes_common::resources::workloads::{PodTemplateSpec, ReplicationControllerSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    const ZERO_COUNTS: PodCounts = PodCounts {
        replicas: 0,
        fully_labeled: 0,
        ready: 0,
        available: 0,
    };

    fn make_rc(
        name: &str,
        ns: &str,
        replicas: i32,
        selector: HashMap<String, String>,
        template_labels: Option<HashMap<String, String>>,
    ) -> ReplicationController {
        ReplicationController {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "ReplicationController".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some(ns.to_string());
                m
            },
            spec: ReplicationControllerSpec {
                replicas: Some(replicas),
                selector: Some(selector),
                template: PodTemplateSpec {
                    metadata: template_labels.map(|labels| {
                        let mut m = ObjectMeta::new("");
                        m.labels = Some(labels);
                        m
                    }),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![rusternetes_common::resources::Container {
                            name: "test".to_string(),
                            image: "busybox".to_string(),
                            command: None,
                            args: None,
                            working_dir: None,
                            ports: None,
                            env: None,
                            env_from: None,
                            resources: None,
                            volume_mounts: None,
                            volume_devices: None,
                            liveness_probe: None,
                            readiness_probe: None,
                            startup_probe: None,
                            lifecycle: None,
                            termination_message_path: None,
                            termination_message_policy: None,
                            image_pull_policy: None,
                            security_context: None,
                            stdin: None,
                            stdin_once: None,
                            tty: None,
                            resize_policy: None,
                            restart_policy: None,
                            ..Default::default()
                        }],
                        init_containers: None,
                        ephemeral_containers: None,
                        restart_policy: Some("Always".to_string()),
                        termination_grace_period_seconds: None,
                        dns_policy: None,
                        node_selector: None,
                        service_account_name: None,
                        service_account: None,
                        automount_service_account_token: None,
                        node_name: None,
                        host_network: None,
                        host_pid: None,
                        host_ipc: None,
                        security_context: None,
                        image_pull_secrets: None,
                        hostname: None,
                        subdomain: None,
                        affinity: None,
                        scheduler_name: None,
                        tolerations: None,
                        host_aliases: None,
                        priority_class_name: None,
                        priority: None,
                        preemption_policy: None,
                        overhead: None,
                        topology_spread_constraints: None,
                        volumes: None,
                        active_deadline_seconds: None,
                        dns_config: None,
                        enable_service_links: None,
                        readiness_gates: None,
                        runtime_class_name: None,
                        os: None,
                        set_hostname_as_fqdn: None,
                        share_process_namespace: None,
                        scheduling_gates: None,
                        resource_claims: None,
                        host_users: None,
                        resources: None,
                        ..Default::default()
                    },
                },
                min_ready_seconds: None,
            },
            status: None,
        }
    }

    fn make_orphan_pod(name: &str, labels: HashMap<String, String>) -> Pod {
        Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some("default".to_string());
                m.labels = Some(labels);
                m
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "test".to_string(),
                    image: "busybox".to_string(),
                    command: None,
                    args: None,
                    working_dir: None,
                    ports: None,
                    env: None,
                    env_from: None,
                    resources: None,
                    volume_mounts: None,
                    volume_devices: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    image_pull_policy: None,
                    security_context: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    resize_policy: None,
                    restart_policy: None,
                    ..Default::default()
                }],
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: Some("Always".to_string()),
                termination_grace_period_seconds: None,
                dns_policy: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                automount_service_account_token: None,
                node_name: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                security_context: None,
                image_pull_secrets: None,
                hostname: None,
                subdomain: None,
                affinity: None,
                scheduler_name: None,
                tolerations: None,
                host_aliases: None,
                priority_class_name: None,
                priority: None,
                preemption_policy: None,
                overhead: None,
                topology_spread_constraints: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_config: None,
                enable_service_links: None,
                readiness_gates: None,
                runtime_class_name: None,
                os: None,
                set_hostname_as_fqdn: None,
                share_process_namespace: None,
                scheduling_gates: None,
                resource_claims: None,
                host_users: None,
                resources: None,
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
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
        }
    }

    /// RC status must be written through the STATUS SUBRESOURCE.
    ///
    /// `Storage::update` is a full-object PUT, and an api-server that exposes
    /// `/status` for a type strips `.status` from those. Driving a vanilla
    /// control plane, every RC status write vanished — a Running single-replica
    /// RC reported `status={"replicas":0}` forever:
    ///
    /// * `[sig-apps] ReplicationController should test the lifecycle of a
    ///   ReplicationController` watches for `status.replicas == 1 &&
    ///   status.readyReplicas == 1` (test/e2e/apps/rc.go:190) and timed out.
    /// * `... should get and update a ReplicationController scale` could not
    ///   "confirm the quantity of ReplicationController replicas" (rc.go:442).
    /// * `... should surface a failure condition on a common issue like
    ///   exceeded quota` never saw the ReplicaFailure condition the controller
    ///   does build (rc.go:594) — conditions live in `.status` too.
    ///
    /// MemoryStorage cannot catch this (its `update_status` funnels through
    /// `update`), hence `StatusSubresourceStorage`.
    #[tokio::test]
    async fn rc_status_survives_an_apiserver_that_strips_status_on_put() {
        use crate::controllers::status_subresource_double::StatusSubresourceStorage;

        let storage = Arc::new(StatusSubresourceStorage::new());
        let mut selector = HashMap::new();
        selector.insert("name".to_string(), "test-rc".to_string());
        let rc = make_rc("test-rc", "default", 1, selector.clone(), None);
        let key = build_key("replicationcontrollers", Some("default"), "test-rc");
        storage.create(&key, &rc).await.unwrap();

        let controller = ReplicationControllerController::new(storage.clone(), 1);
        // One reconcile creates the pod; mark it Ready, then reconcile again so
        // the controller observes a ready replica and reports it.
        controller.reconcile_all().await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&rusternetes_storage::build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 1, "reconcile must create the replica");
        let pod_key = build_key("pods", Some("default"), &pods[0].metadata.name);
        let mut pod = pods[0].clone();
        let mut status = pod.status.clone().unwrap_or_default();
        status.phase = Some(Phase::Running);
        status.conditions = Some(vec![rusternetes_common::resources::PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        }]);
        pod.status = Some(status);
        storage.update_status(&pod_key, &pod).await.unwrap();

        controller.reconcile_all().await.unwrap();

        let stored: ReplicationController = storage.get(&key).await.unwrap();
        let st = stored
            .status
            .expect("RC status must persist through the status subresource");
        assert_eq!(
            st.replicas, 1,
            "status.replicas must report the live replica"
        );
        assert_eq!(
            st.ready_replicas,
            Some(1),
            "status.readyReplicas must report the ready replica; the lifecycle \
             spec watches for exactly this"
        );
    }

    #[tokio::test]
    async fn test_rc_creates_pods_with_selector_labels_when_template_has_none() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "test".to_string());

        // Template has NO labels (metadata is None)
        let rc = make_rc("test-rc", "default", 1, selector.clone(), None);
        storage
            .create("/registry/replicationcontrollers/default/test-rc", &rc)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        // The pod should exist and have the selector labels
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(!pods.is_empty(), "RC should have created a pod");

        let pod = &pods[0];
        let pod_labels = pod
            .metadata
            .labels
            .as_ref()
            .expect("Pod should have labels");
        assert_eq!(
            pod_labels.get("app"),
            Some(&"test".to_string()),
            "Pod should inherit selector labels when template has no labels"
        );
    }

    #[tokio::test]
    async fn test_rc_creates_pods_with_template_labels_when_present() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "test".to_string());

        let mut template_labels = HashMap::new();
        template_labels.insert("app".to_string(), "test".to_string());
        template_labels.insert("version".to_string(), "v1".to_string());

        let rc = make_rc("test-rc", "default", 1, selector, Some(template_labels));
        storage
            .create("/registry/replicationcontrollers/default/test-rc", &rc)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(!pods.is_empty(), "RC should have created a pod");

        let pod_labels = pods[0].metadata.labels.as_ref().unwrap();
        assert_eq!(pod_labels.get("app"), Some(&"test".to_string()));
        assert_eq!(
            pod_labels.get("version"),
            Some(&"v1".to_string()),
            "Pod should use template labels when present"
        );
    }

    #[tokio::test]
    async fn test_rc_created_pods_get_default_tolerations() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "tol-test".to_string());

        let rc = make_rc("tol-rc", "default", 1, selector, None);
        storage
            .create("/registry/replicationcontrollers/default/tol-rc", &rc)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(!pods.is_empty(), "RC should have created a pod");

        // DefaultTolerationSeconds admission parity (#442): RC-created pods
        // bypass the api-server, so the controller must add the defaults.
        let tolerations = pods[0]
            .spec
            .as_ref()
            .and_then(|s| s.tolerations.as_ref())
            .expect("RC-created pod must carry default tolerations");
        for key in [
            "node.kubernetes.io/not-ready",
            "node.kubernetes.io/unreachable",
        ] {
            let tol = tolerations
                .iter()
                .find(|t| t.key.as_deref() == Some(key))
                .unwrap_or_else(|| panic!("missing default toleration for {key}"));
            assert_eq!(tol.operator.as_deref(), Some("Exists"));
            assert_eq!(tol.effect.as_deref(), Some("NoExecute"));
            assert_eq!(tol.toleration_seconds, Some(300));
        }
    }

    #[tokio::test]
    async fn test_rc_pods_inherit_sa_image_pull_secrets_and_token_volume() {
        // #1118: SA imagePullSecrets and the SA token volume must reach
        // RC-created pods, which bypass the api-server admission path —
        // mirrors the ReplicaSet wiring (upstream Go's RC controller IS the
        // ReplicaSet controller via adapters).
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

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

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "pullsecrets".to_string());
        let rc = make_rc("pullsecrets-rc", "default", 1, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/pullsecrets-rc",
                &rc,
            )
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 1);
        let spec = pods[0].spec.as_ref().unwrap();
        let secrets = spec
            .image_pull_secrets
            .as_ref()
            .expect("pod must inherit the SA's imagePullSecrets");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "regcred");
        assert!(
            spec.volumes
                .as_ref()
                .is_some_and(|vols| vols.iter().any(|v| v.name.starts_with("kube-api-access"))),
            "pod must get the SA token volume injected"
        );
    }

    /// `calculateStatus` (replica_set_utils.go:124-136; the RC rides the RS
    /// controller upstream) sets ReplicaFailure only when absent, so a failure
    /// that persists keeps its lastTransitionTime and message; the reason is
    /// FailedDelete when over-replicated.
    #[tokio::test]
    async fn rc_replica_failure_is_not_rewritten_while_it_persists() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "stable".to_string());
        let rc = make_rc("stable", "default", 1, selector, None);
        let key = "/registry/replicationcontrollers/default/stable";
        storage.create(key, &rc).await.unwrap();

        controller
            .update_status(
                &rc,
                ZERO_COUNTS,
                Some("first failure"),
                Some("FailedDelete"),
            )
            .await
            .unwrap();
        let cond = |rc: ReplicationController| {
            rc.status
                .unwrap()
                .conditions
                .unwrap()
                .into_iter()
                .find(|c| c.condition_type == "ReplicaFailure")
                .unwrap()
        };
        let first = cond(storage.get(key).await.unwrap());
        assert_eq!(first.reason.as_deref(), Some("FailedDelete"));

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let rc: ReplicationController = storage.get(key).await.unwrap();
        controller
            .update_status(
                &rc,
                ZERO_COUNTS,
                Some("second failure"),
                Some("FailedCreate"),
            )
            .await
            .unwrap();
        let second = cond(storage.get(key).await.unwrap());
        assert_eq!(second.message, first.message);
        assert_eq!(second.last_transition_time, first.last_transition_time);
    }

    #[tokio::test]
    async fn test_rc_sets_replica_failure_on_quota_exceeded() {
        // The api-server's quota admission denies the create; the controller has
        // no client-side quota check (#3618).
        let storage =
            Arc::new(crate::controllers::quota_admission_double::QuotaAdmissionStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "quota-test".to_string());

        // Create RC wanting 2 replicas
        let rc = make_rc("quota-rc", "default", 2, selector.clone(), None);
        storage
            .create("/registry/replicationcontrollers/default/quota-rc", &rc)
            .await
            .unwrap();

        // Create a ResourceQuota that limits pods to 0
        let quota: serde_json::Value = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {
                "name": "test-quota",
                "namespace": "default",
                "uid": "quota-uid-1"
            },
            "spec": {
                "hard": {
                    "pods": "0"
                }
            }
        });
        storage
            .create("/registry/resourcequotas/default/test-quota", &quota)
            .await
            .unwrap();

        // Reconcile — pod creation should fail due to quota
        controller.reconcile_all().await.unwrap();

        // Verify no pods were created
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            0,
            "No pods should be created when quota is exceeded"
        );
        assert!(
            storage
                .denied_creates
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0,
            "the controller must reach the api-server and be denied there, \
             not pre-check quota client-side"
        );

        // Verify the RC has a ReplicaFailure condition
        let updated_rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/quota-rc")
            .await
            .unwrap();
        let status = updated_rc.status.expect("RC should have status");
        let conditions = status.conditions.expect("RC should have conditions");
        let failure_condition = conditions
            .iter()
            .find(|c| c.condition_type == "ReplicaFailure")
            .expect("RC should have a ReplicaFailure condition");
        assert_eq!(failure_condition.status, "True");
        assert_eq!(failure_condition.reason.as_deref(), Some("FailedCreate"));
        assert!(
            failure_condition
                .message
                .as_ref()
                .unwrap()
                .contains("exceeded quota"),
            "Message should mention exceeded quota, got: {}",
            failure_condition.message.as_ref().unwrap()
        );
    }

    #[tokio::test]
    async fn test_rc_clears_replica_failure_when_pods_created_successfully() {
        // The api-server's quota admission denies the create; the controller has
        // no client-side quota check (#3618).
        let storage =
            Arc::new(crate::controllers::quota_admission_double::QuotaAdmissionStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "clear-test".to_string());

        // Create RC wanting 1 replica
        let rc = make_rc("clear-rc", "default", 1, selector.clone(), None);
        storage
            .create("/registry/replicationcontrollers/default/clear-rc", &rc)
            .await
            .unwrap();

        // Create a ResourceQuota that limits pods to 0 — forces failure
        let quota: serde_json::Value = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {
                "name": "test-quota2",
                "namespace": "default",
                "uid": "quota-uid-2"
            },
            "spec": {
                "hard": {
                    "pods": "0"
                }
            }
        });
        storage
            .create("/registry/resourcequotas/default/test-quota2", &quota)
            .await
            .unwrap();

        // First reconcile — should fail and set ReplicaFailure
        controller.reconcile_all().await.unwrap();

        let rc_after_fail: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/clear-rc")
            .await
            .unwrap();
        assert!(
            rc_after_fail
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .and_then(|c| c.iter().find(|c| c.condition_type == "ReplicaFailure"))
                .map(|c| c.status == "True")
                .unwrap_or(false),
            "Should have ReplicaFailure=True after quota exceeded"
        );

        // Remove the quota so pods can be created
        storage
            .delete("/registry/resourcequotas/default/test-quota2")
            .await
            .unwrap();

        // Second reconcile — should succeed and clear the condition
        controller.reconcile_all().await.unwrap();

        let rc_after_success: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/clear-rc")
            .await
            .unwrap();
        let conditions = rc_after_success
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        // Conditions should be None (cleared) when no failure
        assert!(
            conditions.is_none(),
            "ReplicaFailure condition should be cleared after successful pod creation, got: {:?}",
            conditions
        );

        // Verify pod was actually created
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(
            !pods.is_empty(),
            "Pod should be created after quota removed"
        );
    }

    #[tokio::test]
    async fn test_rc_adopt_orphan_pods_and_release_non_matching() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "web".to_string());

        // Create RC with selector app=web, wanting 1 replica
        let rc = make_rc("adopt-rc", "default", 1, selector.clone(), None);
        storage
            .create("/registry/replicationcontrollers/default/adopt-rc", &rc)
            .await
            .unwrap();

        // Create an orphan pod with matching labels (no ownerReference)
        let mut orphan_labels = HashMap::new();
        orphan_labels.insert("app".to_string(), "web".to_string());
        let orphan_pod = make_orphan_pod("orphan-pod", orphan_labels);
        storage
            .create("/registry/pods/default/orphan-pod", &orphan_pod)
            .await
            .unwrap();

        // Reconcile — should adopt the orphan pod (1 desired, 1 orphan matches)
        controller.reconcile_all().await.unwrap();

        // Verify the orphan pod was adopted (now has ownerReference pointing to the RC)
        let adopted_pod: Pod = storage
            .get("/registry/pods/default/orphan-pod")
            .await
            .unwrap();
        let owner_refs = adopted_pod.metadata.owner_references.as_ref().unwrap();
        assert!(
            owner_refs.iter().any(|r| r.kind == "ReplicationController"
                && r.name == "adopt-rc"
                && r.controller == Some(true)),
            "Pod should have ownerReference to RC after adoption"
        );

        // Verify no extra pods were created (orphan counts toward desired replicas)
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            all_pods.len(),
            1,
            "Should still have only 1 pod — orphan was adopted, not a new pod created"
        );

        // Now change the pod's labels so it no longer matches the selector
        let mut changed_pod = adopted_pod.clone();
        let labels = changed_pod.metadata.labels.as_mut().unwrap();
        labels.clear();
        labels.insert("app".to_string(), "backend".to_string());
        storage
            .update("/registry/pods/default/orphan-pod", &changed_pod)
            .await
            .unwrap();

        // Reconcile — should release the pod (labels no longer match)
        controller.reconcile_all().await.unwrap();

        // Verify the pod's ownerReference was removed (released)
        let released_pod: Pod = storage
            .get("/registry/pods/default/orphan-pod")
            .await
            .unwrap();
        let has_rc_owner = released_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.kind == "ReplicationController" && r.name == "adopt-rc")
            })
            .unwrap_or(false);
        assert!(
            !has_rc_owner,
            "Pod should no longer have ownerReference to RC after label change"
        );
    }

    #[tokio::test]
    async fn test_rc_matches_created_pods_on_next_reconcile() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "test".to_string());

        let rc = make_rc("test-rc", "default", 2, selector, None);
        storage
            .create("/registry/replicationcontrollers/default/test-rc", &rc)
            .await
            .unwrap();

        // First reconcile: creates pods
        controller.reconcile_all().await.unwrap();

        let pods_after_first: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods_after_first.len(), 2, "Should create 2 pods");

        // Second reconcile: should match existing pods, not create more
        controller.reconcile_all().await.unwrap();

        let pods_after_second: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods_after_second.len(),
            2,
            "Should still have 2 pods — RC must match its own pods on second reconcile"
        );
    }

    /// Pod creates must go out in upstream's slow-start batches, not one at a
    /// time.
    ///
    /// Upstream `slowStartBatch`
    /// (pkg/controller/replicaset/replica_set.go:820-844) starts at
    /// `SlowStartInitialBatchSize` and doubles each round, capping each batch
    /// at what remains, and creates every pod WITHIN a batch concurrently.
    /// The doubling is what bounds the damage when creates are being rejected
    /// (a quota denial costs one create, not N) while still reaching a large
    /// replica count in a logarithmic number of rounds.
    ///
    /// This matters because our creates are not cheap. Measured on the kine
    /// leg (run 33750828466), `create_pod` takes ~4s — a ServiceAccount GET, a
    /// ResourceQuota LIST and the pod CREATE, at ~1.3s per api call — so a
    /// serial loop needs ~400s for 100 replicas. The conformance specs give
    /// `replicaSyncTimeout` 120s (garbage_collector.go:128), which is why
    /// three `[sig-api-machinery] Garbage collector` specs fail in setup on
    /// `rc.Status.Replicas (0)` never reaching `Spec.Replicas (100)` (#1847).
    /// Batched, the same 100 creates take 7 rounds instead of 100.
    #[test]
    fn slow_start_batches_capped_respects_max_batch_size() {
        // statefulset slowStartBatch: min(min(2*batch, remaining), MaxBatchSize)
        assert_eq!(slow_start_batches_capped(10, 1, 4), vec![1, 2, 4, 3]);
        assert_eq!(
            slow_start_batches_capped(10, 1, 4).iter().sum::<usize>(),
            10
        );
    }

    #[test]
    fn slow_start_batches_double_and_cap_at_remaining() {
        // Upstream's worked example: 100 pods from an initial batch of 1.
        assert_eq!(
            slow_start_batches(100, 1),
            vec![1, 2, 4, 8, 16, 32, 37],
            "batches double until the tail is capped by what remains"
        );
        assert_eq!(slow_start_batches(100, 1).iter().sum::<usize>(), 100);

        // Round counts follow upstream's 1+floor(log2(ceil(N/initial)))
        // (controller_utils.go:84-86): 100 pods is 7 rounds, not 100.
        assert_eq!(slow_start_batches(100, 1).len(), 7);

        assert_eq!(slow_start_batches(5, 1), vec![1, 2, 2]);
        assert_eq!(slow_start_batches(1, 1), vec![1]);
        assert!(slow_start_batches(0, 1).is_empty(), "nothing to create");

        // A larger initial batch is still capped by the count.
        assert_eq!(slow_start_batches(3, 8), vec![3]);
    }

    /// Wraps `MemoryStorage` and records how many pod creates are in flight at
    /// once, so a test can tell a batched implementation from a serial one.
    /// Every other method delegates straight through.
    pub(crate) struct ConcurrencyProbeStorage {
        inner: Arc<MemoryStorage>,
        in_flight: std::sync::atomic::AtomicUsize,
        peak_in_flight: std::sync::atomic::AtomicUsize,
    }

    impl ConcurrencyProbeStorage {
        pub(crate) fn new(inner: Arc<MemoryStorage>) -> Self {
            Self {
                inner,
                in_flight: std::sync::atomic::AtomicUsize::new(0),
                peak_in_flight: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        pub(crate) fn peak(&self) -> usize {
            self.peak_in_flight
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for ConcurrencyProbeStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if !key.starts_with("/registry/pods/") {
                return self.inner.create(key, value).await;
            }
            use std::sync::atomic::Ordering;
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_in_flight.fetch_max(now, Ordering::SeqCst);
            // Hold the slot open across an await so overlapping creates are
            // actually observable; a serial loop can never exceed 1.
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let out = self.inner.create(key, value).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            out
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
            self.inner.list(prefix).await
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

    /// One reconcile must bring a 100-replica RC all the way to 100 pods AND
    /// issue those creates concurrently.
    ///
    /// This is the behavioural guard for the slow-start port; the sizes are
    /// pinned separately by
    /// [`slow_start_batches_double_and_cap_at_remaining`]. A future change that
    /// reverts to `for i in 0..to_create { create_pod(..).await }` still
    /// converges and would pass an outcome-only test, but it drops the peak
    /// in-flight count to exactly 1 — which is what actually broke the
    /// conformance specs, because serial creates at ~1.3s per api call need
    /// ~400s for 100 pods against a 120s `replicaSyncTimeout` (#1847).
    #[tokio::test]
    async fn rc_creates_all_replicas_in_one_reconcile_and_concurrently() {
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(ConcurrencyProbeStorage::new(inner));
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "batched".to_string());
        let rc = make_rc("simpletest-rc", "default", 100, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            100,
            "one reconcile must create every missing replica, not one per pass"
        );

        // The load-bearing assertion: a serial loop peaks at exactly 1.
        assert!(
            storage.peak() > 1,
            "pod creates must overlap; peak in-flight was {} (serial implementation?)",
            storage.peak()
        );

        // Deliberately not asserting peak == the batch size. `create_pod`
        // awaits a ServiceAccount GET and a ResourceQuota LIST before the
        // create, so futures in a batch interleave at those earlier awaits and
        // the peak *create* overlap is a lower bound on the batch width.

        // `calculateStatus` counts the pods listed BEFORE `manageReplicas`
        // (replica_set.go:743), so this sync reported 0. The pod watch's Added
        // events (replica_set.go:436 `CreationObserved`) lower the
        // expectations and enqueue the RC, and that sync reports the pods.
        for _ in 0..100 {
            controller
                .expectations
                .creation_observed("default/simpletest-rc");
        }
        controller.reconcile_all().await.unwrap();

        let observed: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/simpletest-rc")
            .await
            .unwrap();
        assert_eq!(
            observed.status.as_ref().map(|st| st.replicas),
            Some(100),
            "status.replicas must reach spec.replicas — this is what \
             garbage_collector.go:1174 waits on"
        );
    }

    /// A create rejected because the namespace is terminating is not a replica
    /// failure.
    ///
    /// Upstream treats the admission rejection and the controller's reaction as
    /// one design. `manageReplicas` inspects the create error and swallows this
    /// specific cause:
    ///
    /// ```text
    /// if apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
    ///     // if the namespace is being terminated, we don't have to do
    ///     // anything because any creation will fail
    ///     return nil
    /// }
    /// ```
    ///
    /// (pkg/controller/replicaset/replica_set.go:632-637)
    ///
    /// We shipped the api-server half of that pair in #1849 — creates into a
    /// Terminating namespace now return 403 with the `NamespaceTerminating`
    /// cause — without the controller half. So every namespace teardown put the
    /// RC into a retry loop against creates that can only ever fail, and
    /// stamped a `ReplicaFailure` condition upstream deliberately does not set.
    #[test]
    fn namespace_terminating_rejection_is_recognised() {
        use rusternetes_common::Error;

        // The message our api-server produces (#1849), as it reaches the
        // controller through ApiStorage's stringified error chain.
        assert!(is_namespace_terminating_rejection(&Error::Forbidden(
            "POST https://api-server:6443/api/v1/namespaces/ns-1/pods failed: \
             (Forbidden) pods is forbidden: unable to create new content in \
             namespace ns-1 because it is being terminated"
                .to_string()
        )));

        // The structured cause reason, if it ever reaches us intact — this is
        // what upstream actually matches on.
        assert!(is_namespace_terminating_rejection(&Error::Forbidden(
            "(Forbidden) NamespaceTerminating".to_string()
        )));

        // Any other Forbidden is a real failure and must still surface: a quota
        // denial is the case that ReplicaFailure exists for.
        assert!(!is_namespace_terminating_rejection(&Error::Forbidden(
            "exceeded quota: pods, requested: 1, used: 10, limited: 10".to_string()
        )));

        // Non-Forbidden errors are never this case.
        assert!(!is_namespace_terminating_rejection(&Error::Storage(
            "connection refused".to_string()
        )));
        assert!(!is_namespace_terminating_rejection(&Error::Conflict(
            "resourceVersion mismatch".to_string()
        )));
    }

    /// Rejects every pod create the way our api-server does for a Terminating
    /// namespace (#1849), and counts the attempts. Everything else delegates.
    pub(crate) struct TerminatingNamespaceStorage {
        inner: Arc<MemoryStorage>,
        pod_create_attempts: std::sync::atomic::AtomicUsize,
    }

    impl TerminatingNamespaceStorage {
        pub(crate) fn new(inner: Arc<MemoryStorage>) -> Self {
            Self {
                inner,
                pod_create_attempts: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        pub(crate) fn attempts(&self) -> usize {
            self.pod_create_attempts
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for TerminatingNamespaceStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if key.starts_with("/registry/pods/") {
                self.pod_create_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(rusternetes_common::Error::Forbidden(format!(
                    "POST {key} failed: (Forbidden) pods is forbidden: unable to create new \
                     content in namespace ns-term because it is being terminated"
                )));
            }
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
            self.inner.list(prefix).await
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

    /// `HasStatusCause(err, v1.NamespaceTerminatingCause)` -> `return nil`
    /// inside the `slowStartBatch` callback (replica_set.go:632-637), so the
    /// rejected create counts as a SUCCESS: no ReplicaFailure, no requeue, and
    /// the batches carry on (:820-844 only stops on an error). A 20-replica RC
    /// therefore attempts all 20 creates (batches 1, 2, 4, 8, 5) rather than
    /// abandoning after the first rejection.
    ///
    /// Upstream also does NOT decrement the expectation for these creates
    /// (`successfulCreations` includes them, so the skipped-pod loop at
    /// :644-650 has nothing to undo); replicated, not fixed.
    ///
    /// There is no replication_controller_test.go upstream: the RC runs through
    /// the ReplicaSet controller (pkg/controller/replication/conversion.go), so
    /// the equivalents are `TestSlowStartBatch` (replica_set_test.go:1872) and
    /// `TestSyncReplicaSetCreateFailures` (replica_set_test.go:281).
    #[tokio::test]
    async fn rc_continues_slow_start_past_namespace_terminating_rejection() {
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(TerminatingNamespaceStorage::new(inner));
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "doomed".to_string());
        let rc = make_rc("simpletest-rc", "default", 20, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        // Must not surface as a reconcile error: upstream's callback returns nil.
        controller
            .reconcile_rc(&rc)
            .await
            .expect("a terminating namespace must not fail the reconcile");

        let observed: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/simpletest-rc")
            .await
            .unwrap();
        let has_replica_failure = observed
            .status
            .as_ref()
            .and_then(|st| st.conditions.as_ref())
            .is_some_and(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure"));
        assert!(
            !has_replica_failure,
            "a terminating namespace must not raise ReplicaFailure"
        );

        assert_eq!(
            storage.attempts(),
            20,
            "slowStartBatch keeps going: the rejection is a nil return, not a failure"
        );
        assert_eq!(
            controller
                .expectations
                .get_expectations("default/simpletest-rc"),
            Some((20, 0)),
            "upstream counts the rejected creates as successes, so none is decremented"
        );
    }

    /// `syncReplicaSet` returns `manageReplicasErr` AFTER updating status
    /// (replica_set.go:772-774) so the workqueue requeues the key with backoff:
    /// the error must reach the worker, and ReplicaFailure must already be on
    /// the object. Mirrors `TestSyncReplicaSetCreateFailures`
    /// (replica_set_test.go:281).
    #[tokio::test]
    async fn rc_returns_create_failure_after_updating_status_for_requeue() {
        let storage =
            Arc::new(crate::controllers::quota_admission_double::QuotaAdmissionStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "cf".to_string());
        let rc = make_rc("cf", "default", 3, selector, None);
        let rc_key = "/registry/replicationcontrollers/default/cf";
        storage.create(rc_key, &rc).await.unwrap();
        storage
            .create(
                "/registry/resourcequotas/default/pq",
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "ResourceQuota",
                    "metadata": {"name": "pq", "namespace": "default", "uid": "q"},
                    "spec": {"hard": {"pods": "0"}}
                }),
            )
            .await
            .unwrap();

        let result = controller.reconcile_rc(&rc).await;
        assert!(result.is_err(), "a create failure must requeue the key");

        let after: ReplicationController = storage.get(rc_key).await.unwrap();
        let failure = after
            .status
            .and_then(|s| s.conditions)
            .and_then(|cs| {
                cs.into_iter()
                    .find(|c| c.condition_type == "ReplicaFailure")
            })
            .expect("status is written before the error is returned");
        assert_eq!(failure.reason.as_deref(), Some("FailedCreate"));
    }

    /// Fails `delete_gracefully` for every pod once armed, and counts the
    /// attempts. Everything else delegates.
    pub(crate) struct DeleteFailingStorage {
        inner: Arc<MemoryStorage>,
        fail_with_not_found: bool,
        armed: std::sync::atomic::AtomicBool,
        delete_attempts: std::sync::atomic::AtomicUsize,
    }

    impl DeleteFailingStorage {
        pub(crate) fn new(inner: Arc<MemoryStorage>, fail_with_not_found: bool) -> Self {
            Self {
                inner,
                fail_with_not_found,
                armed: std::sync::atomic::AtomicBool::new(false),
                delete_attempts: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn arm(&self) {
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn attempts(&self) -> usize {
            self.delete_attempts
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for DeleteFailingStorage {
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

        async fn delete_gracefully(&self, key: &str) -> rusternetes_common::Result<()> {
            if key.starts_with("/registry/pods/")
                && self.armed.load(std::sync::atomic::Ordering::SeqCst)
            {
                self.delete_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(if self.fail_with_not_found {
                    rusternetes_common::Error::NotFound(format!("pod {key} not found"))
                } else {
                    rusternetes_common::Error::Internal(format!("delete {key} failed"))
                });
            }
            self.inner.delete_gracefully(key).await
        }

        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list(prefix).await
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

    /// Scale an RC from 3 to 1 against `storage`, with every pod delete failing
    /// once the storage is armed.
    async fn scale_down_with_failing_deletes(
        storage: &Arc<DeleteFailingStorage>,
    ) -> (
        ReplicationControllerController<DeleteFailingStorage>,
        rusternetes_common::Result<()>,
    ) {
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "df".to_string());
        let rc = make_rc("df", "default", 3, selector, None);
        let rc_key = "/registry/replicationcontrollers/default/df";
        storage.create(rc_key, &rc).await.unwrap();
        controller.reconcile_rc(&rc).await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 3);
        // The creates are observed (pod watch Added events); simulate them.
        for _ in 0..3 {
            controller.expectations.creation_observed("default/df");
        }

        let mut rc: ReplicationController = storage.get(rc_key).await.unwrap();
        rc.spec.replicas = Some(1);
        storage.update(rc_key, &rc).await.unwrap();
        // Reconcile the object as the worker would read it: the update bumped
        // its resourceVersion, which the status write is conditional on.
        let rc: ReplicationController = storage.get(rc_key).await.unwrap();
        storage.arm();
        let result = controller.reconcile_rc(&rc).await;
        (controller, result.map(|_| ()))
    }

    /// The deletes run concurrently, every one is attempted, and only the
    /// first error is reported AFTER the status update
    /// (replica_set.go:672-698, :772-774): no early return on the first failed
    /// delete, ReplicaFailure=FailedDelete recorded, error returned to requeue.
    #[tokio::test]
    async fn rc_attempts_every_delete_and_updates_status_when_a_delete_fails() {
        let storage = Arc::new(DeleteFailingStorage::new(
            Arc::new(MemoryStorage::new()),
            false,
        ));
        let (controller, result) = scale_down_with_failing_deletes(&storage).await;

        assert!(result.is_err(), "a delete failure must requeue the key");
        assert_eq!(storage.attempts(), 2, "every delete is attempted");

        let after: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/df")
            .await
            .unwrap();
        let status = after.status.expect("status is updated despite the error");
        let failure = status
            .conditions
            .and_then(|cs| {
                cs.into_iter()
                    .find(|c| c.condition_type == "ReplicaFailure")
            })
            .expect("ReplicaFailure recorded for the failed delete");
        assert_eq!(failure.reason.as_deref(), Some("FailedDelete"));

        // The informer will not observe the failed deletions, so each lowers
        // the expectation back (replica_set.go:679-686).
        assert_eq!(
            controller.expectations.get_expectations("default/df"),
            Some((0, 0))
        );
    }

    /// `!apierrors.IsNotFound(err)` guards the error report
    /// (replica_set.go:682-685): a pod already gone is no failure.
    #[tokio::test]
    async fn rc_delete_not_found_is_not_a_failure() {
        let storage = Arc::new(DeleteFailingStorage::new(
            Arc::new(MemoryStorage::new()),
            true,
        ));
        let (_controller, result) = scale_down_with_failing_deletes(&storage).await;

        result.expect("NotFound on delete must not fail the sync");
        let after: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/df")
            .await
            .unwrap();
        assert!(
            !after
                .status
                .and_then(|s| s.conditions)
                .is_some_and(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure")),
            "NotFound is not a ReplicaFailure"
        );
    }

    /// While creates are in flight and unobserved, a re-entrant reconcile must
    /// not issue them again.
    ///
    /// This is the whole purpose of expectations, and the failure it prevents is
    /// concrete: a watch-driven controller wakes on its OWN pod-create events,
    /// lists pods from a backend that has not caught up, sees too few, and
    /// creates a second batch. Upstream gates `manageReplicas` on
    /// `SatisfiedExpectations` for exactly this
    /// (pkg/controller/replicaset/replica_set.go:728, 756).
    ///
    /// Simulated by leaving an outstanding expectation and reconciling against
    /// a storage that reports no pods at all — the worst case of a stale list.
    /// Without the gate the controller creates the full replica count again.
    #[tokio::test]
    async fn rc_defers_creates_while_expectations_are_outstanding() {
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(ConcurrencyProbeStorage::new(inner));
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "gated".to_string());
        let rc = make_rc("simpletest-rc", "default", 10, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        // Stand in for a batch this controller has just issued and not yet
        // seen land.
        controller
            .expectations
            .expect_creations("default/simpletest-rc", 10);

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(
            pods.is_empty(),
            "no pods may be created while 10 creates are outstanding, got {}",
            pods.len()
        );

        // Once those creates are observed the gate opens and the controller
        // does its job — the brake must not become a wedge.
        for _ in 0..10 {
            controller
                .expectations
                .creation_observed("default/simpletest-rc");
        }
        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            10,
            "once expectations are satisfied the controller must create the replicas"
        );
    }

    /// A single sync creates at most `BURST_REPLICAS` pods, and the next sync
    /// picks up the rest.
    ///
    /// Upstream caps the diff in both directions before doing anything with it:
    ///
    /// ```text
    /// if diff > rsc.burstReplicas {
    ///     diff = rsc.burstReplicas
    /// }
    /// ```
    ///
    /// (pkg/controller/replicaset/replica_set.go:611-612 for creates, :653-654
    /// for deletes; `BurstReplicas = 500` at :72.)
    ///
    /// The cap bounds what one object can do to the api-server in one sync.
    /// Slow-start batching alone does not: its batches double without limit, so
    /// a single large RC can otherwise issue an unbounded burst — and this
    /// controller shares an api-server with every other client, including the
    /// conformance suite whose own rate limiter starved during run
    /// 33762395978.
    #[tokio::test]
    async fn rc_caps_creates_at_burst_replicas_and_finishes_next_sync() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "burst".to_string());
        let desired = BURST_REPLICAS as i32 + 100;
        let rc = make_rc("simpletest-rc", "default", desired, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            BURST_REPLICAS,
            "one sync must not exceed the burst cap"
        );

        // The cap is a throttle, not a ceiling: once the creates are observed
        // (the pod watch's Added events), the next sync finishes the job.
        for _ in 0..BURST_REPLICAS {
            controller
                .expectations
                .creation_observed("default/simpletest-rc");
        }
        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            desired as usize,
            "the following sync must create the remainder"
        );
    }

    /// Fails the first `n` pod creates with AlreadyExists, then behaves. Stands
    /// in for a generated pod name colliding with one already in the namespace.
    pub(crate) struct NameCollisionStorage {
        inner: Arc<MemoryStorage>,
        remaining_collisions: std::sync::atomic::AtomicUsize,
    }

    impl NameCollisionStorage {
        pub(crate) fn new(inner: Arc<MemoryStorage>, collisions: usize) -> Self {
            Self {
                inner,
                remaining_collisions: std::sync::atomic::AtomicUsize::new(collisions),
            }
        }
    }

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for NameCollisionStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            use std::sync::atomic::Ordering;
            if key.starts_with("/registry/pods/")
                && self
                    .remaining_collisions
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
            {
                return Err(rusternetes_common::Error::AlreadyExists(format!(
                    "pods {key} already exists"
                )));
            }
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
            self.inner.list(prefix).await
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

    /// A generated pod name that collides must be retried with a new name, not
    /// reported as a failure.
    ///
    /// Upstream retries the create up to `maxNameGenerationCreateAttempts` (8),
    /// generating a fresh name each time
    /// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:431-470).
    /// Its comment gives the arithmetic that makes this mandatory rather than
    /// defensive:
    ///
    /// ```text
    /// Without retry, a 0.1% probability occurs at ~500
    /// generated names and a 50% probability occurs at ~4500
    /// generated names.
    /// ```
    ///
    /// This is not hypothetical here. `rc_caps_creates_at_burst_replicas_and_
    /// finishes_next_sync` creates 500 pods in one namespace and failed in CI
    /// with 254 of them — a collision killed the batch — while passing locally.
    #[tokio::test]
    async fn rc_retries_pod_create_when_the_generated_name_collides() {
        let inner = Arc::new(MemoryStorage::new());
        // Three collisions in a row: within upstream's budget of 8.
        let storage = Arc::new(NameCollisionStorage::new(inner, 3));
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "collide".to_string());
        let rc = make_rc("simpletest-rc", "default", 1, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            1,
            "the create must be retried under a new name, not abandoned"
        );

        // And it must not be reported as a replica failure: nothing is wrong
        // with this RC.
        let observed: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/simpletest-rc")
            .await
            .unwrap();
        let has_replica_failure = observed
            .status
            .as_ref()
            .and_then(|st| st.conditions.as_ref())
            .is_some_and(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure"));
        assert!(
            !has_replica_failure,
            "a retried name collision is not a ReplicaFailure"
        );
    }

    /// #2458: upstream `DeletePod` is a graceful delete
    /// (`pkg/controller/controller_utils.go:618`, `DeleteOptions{}`): the pod
    /// keeps living, stamped with a deletionTimestamp, for its grace period.
    /// Terminating pods are not replicas, so the follow-up sync must not take
    /// another pod down.
    #[tokio::test]
    async fn rc_scale_down_deletes_pods_gracefully() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "g".to_string());
        let rc = make_rc("g", "default", 3, selector.clone(), None);
        let rc_key = "/registry/replicationcontrollers/default/g";
        storage.create(rc_key, &rc).await.unwrap();
        controller.reconcile_all().await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 3);
        // The creates are observed (pod watch Added events); simulate them.
        for _ in 0..3 {
            controller.expectations.creation_observed("default/g");
        }

        let mut rc: ReplicationController = storage.get(rc_key).await.unwrap();
        rc.spec.replicas = Some(1);
        storage.update(rc_key, &rc).await.unwrap();
        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 3, "graceful delete keeps the pods in storage");
        assert_eq!(
            pods.iter()
                .filter(|p| p.metadata.deletion_timestamp.is_some())
                .count(),
            2
        );

        // Terminating pods are not replicas: nothing further is deleted.
        controller.reconcile_all().await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.iter()
                .filter(|p| p.metadata.deletion_timestamp.is_some())
                .count(),
            2
        );
    }

    /// #2458: the deletion is observed on the MODIFIED event that sets the
    /// deletionTimestamp (`replica_set.go:468-480`), once per pod
    /// (`controller_utils.go:380-390`).
    #[tokio::test]
    async fn rc_observes_deletion_on_deletion_timestamp_once() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        // `resolveControllerRef` needs the RC the pod's reference names.
        let mut rc = make_rc("g", "default", 1, rc_selector("g"), None);
        rc.metadata.uid = "u".to_string();
        put_rc(&storage, &rc).await;
        controller.expectations.expect_deletions_of(
            "default/g",
            &["default/p1".to_string(), "default/p2".to_string()],
        );
        let pod = serde_json::json!({
            "metadata": {
                "name": "p1",
                "namespace": "default",
                "deletionTimestamp": "2026-10-07T00:00:00Z",
                "ownerReferences": [{"apiVersion": "v1", "kind": "ReplicationController",
                    "name": "g", "uid": "u", "controller": true}]
            }
        })
        .to_string();
        let key = "/registry/pods/default/p1".to_string();

        controller
            .enqueue_owner_rc(
                &queue,
                &rusternetes_storage::WatchEvent::Modified(key.clone(), pod.clone()),
            )
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((0, 1)),
            "the deletionTimestamp update is the observation"
        );
        controller
            .enqueue_owner_rc(&queue, &rusternetes_storage::WatchEvent::Deleted(key, pod))
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((0, 1))
        );
    }

    // ---------------------------------------------------------------------
    // syncReplicaSet / calculateStatus / getPodsToDelete parity (#3627)
    //
    // The RC rides the ReplicaSet controller upstream
    // (pkg/controller/replication/replication_controller.go:57-79,
    // `NewBaseController` with `EnableStatusTerminatingReplicas: false`), so
    // each behaviour below is the RS one (#3624): replica_set.go
    // `syncReplicaSet` :713-790, `getPodsToDelete` :874,
    // replica_set_utils.go `calculateStatus` :96-141.
    // ---------------------------------------------------------------------

    fn rc_selector(app: &str) -> HashMap<String, String> {
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), app.to_string());
        selector
    }

    async fn seed_rc(
        storage: &Arc<MemoryStorage>,
        controller: &ReplicationControllerController<MemoryStorage>,
        rc: &ReplicationController,
    ) {
        storage
            .create(
                &format!(
                    "/registry/replicationcontrollers/default/{}",
                    rc.metadata.name
                ),
                rc,
            )
            .await
            .unwrap();
        let stored: ReplicationController = storage
            .get(&format!(
                "/registry/replicationcontrollers/default/{}",
                rc.metadata.name
            ))
            .await
            .unwrap();
        controller.reconcile_rc(&stored).await.unwrap();
    }

    async fn rc_pods(storage: &Arc<MemoryStorage>) -> Vec<Pod> {
        storage.list("/registry/pods/default/").await.unwrap()
    }

    async fn write_pod(storage: &Arc<MemoryStorage>, pod: &Pod) {
        storage
            .update(
                &format!("/registry/pods/default/{}", pod.metadata.name),
                pod,
            )
            .await
            .unwrap();
    }

    fn make_ready(pod: &mut Pod, since: chrono::DateTime<chrono::Utc>) {
        let status = pod.status.get_or_insert_with(Default::default);
        status.phase = Some(Phase::Running);
        status.conditions = Some(vec![rusternetes_common::resources::pod::PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(since),
            observed_generation: None,
        }]);
    }

    async fn rc_status(storage: &Arc<MemoryStorage>, name: &str) -> ReplicationControllerStatus {
        let rc: ReplicationController = storage
            .get(&format!("/registry/replicationcontrollers/default/{name}"))
            .await
            .unwrap();
        rc.status.expect("status written")
    }

    use rusternetes_common::resources::ReplicationControllerStatus;

    /// `calculateStatus(rs, activePods, ...)` over the pods listed before
    /// `manageReplicas` (replica_set.go:743, :597 "Does NOT modify
    /// <activePods>").
    #[tokio::test]
    async fn rc_status_counts_the_pods_listed_before_manage_replicas() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let rc = make_rc("pre", "default", 3, rc_selector("pre"), None);
        seed_rc(&storage, &controller, &rc).await;

        assert_eq!(rc_pods(&storage).await.len(), 3);
        assert_eq!(
            rc_status(&storage, "pre").await.replicas,
            0,
            "status is the pre-manage count, not a re-list"
        );
    }

    /// `CreationObserved` is the informer's `addPod` (replica_set.go:436);
    /// the sync never lowers its own expectations.
    #[tokio::test]
    async fn rc_sync_does_not_lower_its_own_create_expectations() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let rc = make_rc("exp", "default", 3, rc_selector("exp"), None);
        seed_rc(&storage, &controller, &rc).await;

        assert_eq!(
            controller.expectations.get_expectations("default/exp"),
            Some((3, 0))
        );
    }

    /// `getPodsToDelete` (replica_set.go:874) ranks victims: not-ready before
    /// ready (controller_utils.go:849).
    #[tokio::test]
    async fn rc_scale_down_deletes_the_not_ready_pod_first() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let rc = make_rc("rank", "default", 2, rc_selector("rank"), None);
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        make_ready(&mut pods[0], ago);
        for pod in &mut pods {
            pod.spec.as_mut().unwrap().node_name = Some("n1".to_string());
            write_pod(&storage, pod).await;
        }
        let keep = pods[0].metadata.name.clone();
        controller.expectations.delete_expectations("default/rank");

        let mut rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/rank")
            .await
            .unwrap();
        rc.spec.replicas = Some(1);
        controller.reconcile_rc(&rc).await.unwrap();

        let dying: Vec<String> = rc_pods(&storage)
            .await
            .into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
            .map(|p| p.metadata.name)
            .collect();
        assert_eq!(dying.len(), 1);
        assert_ne!(dying[0], keep, "the Ready pod must survive");
    }

    /// `fullyLabeledReplicas` matches the TEMPLATE labels
    /// (replica_set_utils.go:103-110).
    #[tokio::test]
    async fn rc_fully_labeled_replicas_matches_template_labels() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut template = rc_selector("fl");
        template.insert("tier".to_string(), "web".to_string());
        let rc = make_rc("fl", "default", 2, rc_selector("fl"), Some(template));
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        pods[0].metadata.labels = Some(rc_selector("fl"));
        write_pod(&storage, &pods[0]).await;
        controller.expectations.delete_expectations("default/fl");

        let rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/fl")
            .await
            .unwrap();
        controller.reconcile_rc(&rc).await.unwrap();

        let status = rc_status(&storage, "fl").await;
        assert_eq!(status.replicas, 2);
        assert_eq!(status.fully_labeled_replicas, Some(1));
    }

    /// `IsPodAvailable` measures minReadySeconds from the Ready condition's
    /// lastTransitionTime (pkg/api/v1/pod/util.go:283-294); the RC's
    /// `availableReplicas` is that count, not `readyReplicas`.
    #[tokio::test]
    async fn rc_availability_honours_min_ready_seconds() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut rc = make_rc("avail", "default", 2, rc_selector("avail"), None);
        rc.spec.min_ready_seconds = Some(60);
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        let now = chrono::Utc::now();
        make_ready(&mut pods[0], now - chrono::Duration::seconds(10));
        make_ready(&mut pods[1], now - chrono::Duration::seconds(120));
        for pod in &pods {
            write_pod(&storage, pod).await;
        }
        controller.expectations.delete_expectations("default/avail");

        let rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/avail")
            .await
            .unwrap();
        controller.reconcile_rc(&rc).await.unwrap();

        let status = rc_status(&storage, "avail").await;
        assert_eq!(status.ready_replicas, Some(2));
        assert_eq!(status.available_replicas, Some(1));
    }

    /// `syncReplicaSet` plans the next availability check
    /// (replica_set.go:775-788).
    #[tokio::test]
    async fn rc_requeues_when_a_ready_pod_is_not_yet_available() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut rc = make_rc("next", "default", 1, rc_selector("next"), None);
        rc.spec.min_ready_seconds = Some(60);
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        make_ready(
            &mut pods[0],
            chrono::Utc::now() - chrono::Duration::seconds(10),
        );
        write_pod(&storage, &pods[0]).await;
        controller.expectations.delete_expectations("default/next");

        let rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/next")
            .await
            .unwrap();
        let next = controller
            .reconcile_rc(&rc)
            .await
            .unwrap()
            .expect("an AddAfter must be planned");

        assert!(
            next > Duration::from_secs(48) && next <= Duration::from_secs(50),
            "~50s left until the pod is available, got {next:?}"
        );
    }

    /// The informer's re-list on (re)connect (`DeltaFIFO.Replace`): pods
    /// created while the watch was down are observed as adds, once, so a lost
    /// Added event does not wedge the sync until the expectations TTL.
    #[tokio::test]
    async fn rc_relist_observes_creations_the_watch_missed() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        let rc = make_rc("gap", "default", 2, rc_selector("gap"), None);
        seed_rc(&storage, &controller, &rc).await;
        assert_eq!(rc_pods(&storage).await.len(), 2);
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((2, 0)),
            "the sync itself does not observe its creates"
        );

        controller.relist_pods(&queue).await;
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((0, 0))
        );

        // Nothing new on a second re-list.
        controller.expectations.expect_creations("default/gap", 1);
        controller.relist_pods(&queue).await;
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((1, 0))
        );
    }

    // ---------------------------------------------------------------------
    // #3633 / #3634: selector semantics and informer event handlers. The RC
    // rides the RS controller (replication_controller.go:57-79); its lister
    // is `GetPodControllers` (client-go listers/core/v1/
    // replicationcontroller_expansion.go:36-66) and its selector is
    // `Convert_v1_ReplicationControllerSpec_To_apps_ReplicaSetSpec`
    // (pkg/apis/core/v1/conversion.go:117-130).
    // ---------------------------------------------------------------------

    async fn queued(queue: &WorkQueue) -> Vec<String> {
        let mut keys = Vec::new();
        while queue.len().await > 0 {
            let key = queue.get().await.unwrap();
            queue.done(&key).await;
            keys.push(key);
        }
        keys.sort();
        keys
    }

    /// A pod event for `p` in `default`. `owner` is (kind, name, uid, controller).
    fn rc_pod_event(
        make: fn(String, String) -> rusternetes_storage::WatchEvent,
        name: &str,
        labels: &[(&str, &str)],
        owner: Option<(&str, &str, &str, bool)>,
        rv: &str,
    ) -> rusternetes_storage::WatchEvent {
        let labels: HashMap<&str, &str> = labels.iter().copied().collect();
        let mut v = serde_json::json!({
            "metadata": {"name": name, "namespace": "default", "labels": labels,
                "resourceVersion": rv},
            "spec": {"containers": []},
        });
        if let Some((kind, owner_name, uid, controller)) = owner {
            v["metadata"]["ownerReferences"] = serde_json::json!([{
                "apiVersion": "v1", "kind": kind, "name": owner_name, "uid": uid,
                "controller": controller
            }]);
        }
        make(format!("/registry/pods/default/{name}"), v.to_string())
    }

    async fn put_rc(storage: &Arc<MemoryStorage>, rc: &ReplicationController) {
        storage
            .create(
                &format!(
                    "/registry/replicationcontrollers/default/{}",
                    rc.metadata.name
                ),
                rc,
            )
            .await
            .unwrap();
    }

    /// `selector: {}` converts to `LabelSelector{MatchLabels: {}}` and
    /// `LabelSelectorAsSelector` turns that into `labels.Everything()`, which
    /// claims even an unlabelled orphan.
    #[tokio::test]
    async fn rc_empty_selector_selects_everything_as_upstream() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        // `selector: {}` converts to `LabelSelector{MatchLabels: {}}`, which
        // `LabelSelectorAsSelector` turns into `labels.Everything()`.
        let rc = make_rc("all", "default", 1, HashMap::new(), Some(HashMap::new()));
        put_rc(&storage, &rc).await;
        let orphan = pod_from_event_json("o");
        storage
            .create("/registry/pods/default/o", &orphan)
            .await
            .unwrap();

        controller.reconcile_rc(&rc).await.unwrap();

        let pods = rc_pods(&storage).await;
        assert_eq!(
            pods.len(),
            1,
            "the unlabelled orphan is claimed, none created"
        );
        assert!(pods[0].metadata.owner_references.is_some());
    }

    fn pod_from_event_json(name: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "default"},
            "spec": {"containers": []},
            "status": {"phase": "Running"},
        }))
        .unwrap()
    }

    /// `addPod`: only a `controller: true` reference counts
    /// (`GetControllerOf`, replica_set.go:426-440).
    #[tokio::test]
    async fn rc_pod_event_ignores_a_non_controller_owner_reference() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        let rc = make_rc("g", "default", 1, rc_selector("g"), None);
        put_rc(&storage, &rc).await;
        controller.expectations.set_expectations("default/g", 1, 0);

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(
                    rusternetes_storage::WatchEvent::Added,
                    "p",
                    &[("other", "x")],
                    Some(("ReplicationController", "g", &rc.metadata.uid, false)),
                    "1",
                ),
            )
            .await;

        assert!(queued(&queue).await.is_empty());
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((1, 0))
        );
    }

    /// `resolveControllerRef` (replica_set.go:302-321): wrong kind, missing
    /// controller and UID mismatch resolve to nil.
    #[tokio::test]
    async fn rc_pod_event_unresolvable_controller_ref_does_nothing() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        let rc = make_rc("g", "default", 1, rc_selector("g"), None);
        put_rc(&storage, &rc).await;
        controller.expectations.set_expectations("default/g", 1, 0);

        for (i, (kind, name, uid)) in [
            ("ReplicaSet", "g", rc.metadata.uid.as_str()),
            ("ReplicationController", "missing", rc.metadata.uid.as_str()),
            ("ReplicationController", "g", "recreated-uid"),
        ]
        .into_iter()
        .enumerate()
        {
            controller
                .enqueue_owner_rc(
                    &queue,
                    &rc_pod_event(
                        rusternetes_storage::WatchEvent::Added,
                        &format!("p{i}"),
                        &[("app", "g")],
                        Some((kind, name, uid, true)),
                        "1",
                    ),
                )
                .await;
        }

        assert!(queued(&queue).await.is_empty());
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((1, 0))
        );
    }

    /// `addPod` of an orphan syncs the RCs `GetPodControllers` returns: those
    /// whose non-empty selector the pod's labels satisfy. A pod without
    /// labels matches none, and no creation is observed.
    #[tokio::test]
    async fn rc_orphan_pod_added_enqueues_only_matching_controllers() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        put_rc(
            &storage,
            &make_rc("hit", "default", 1, rc_selector("a"), None),
        )
        .await;
        put_rc(
            &storage,
            &make_rc("miss", "default", 1, rc_selector("z"), None),
        )
        .await;
        put_rc(
            &storage,
            &make_rc("empty", "default", 1, HashMap::new(), None),
        )
        .await;
        controller
            .expectations
            .set_expectations("default/hit", 1, 0);

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(
                    rusternetes_storage::WatchEvent::Added,
                    "orphan",
                    &[("app", "a")],
                    None,
                    "1",
                ),
            )
            .await;
        assert_eq!(
            queued(&queue).await,
            vec!["replicationcontrollers/default/hit"]
        );
        assert_eq!(
            controller.expectations.get_expectations("default/hit"),
            Some((1, 0))
        );

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(
                    rusternetes_storage::WatchEvent::Added,
                    "unlabeled",
                    &[],
                    None,
                    "1",
                ),
            )
            .await;
        assert!(queued(&queue).await.is_empty());
    }

    /// `updatePod`: dropping the ControllerRef wakes the former owner and the
    /// label-matching controllers; an unchanged orphan wakes nobody.
    #[tokio::test]
    async fn rc_update_pod_follows_the_controller_ref() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        let rc1 = make_rc("foo", "default", 1, rc_selector("foo"), None);
        let rc2 = make_rc("barfoo", "default", 1, rc_selector("bar"), None);
        put_rc(&storage, &rc1).await;
        put_rc(&storage, &rc2).await;
        let added = rusternetes_storage::WatchEvent::Added;
        let modified = rusternetes_storage::WatchEvent::Modified;

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(
                    added,
                    "p",
                    &[("app", "foo")],
                    Some(("ReplicationController", "foo", &rc1.metadata.uid, true)),
                    "1",
                ),
            )
            .await;
        queued(&queue).await;

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(modified, "p", &[("app", "bar")], None, "2"),
            )
            .await;
        assert_eq!(
            queued(&queue).await,
            vec![
                "replicationcontrollers/default/barfoo",
                "replicationcontrollers/default/foo"
            ]
        );

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(modified, "p", &[("app", "bar")], None, "3"),
            )
            .await;
        assert!(queued(&queue).await.is_empty());
    }

    /// The worker wires `nextSyncDuration` to `queue.AddAfter` after `Forget`
    /// (replica_set.go:786-788; the RC rides this code): a ready but not yet
    /// available pod is re-checked by a second sync after `minReadySeconds`
    /// with no watch event to wake the key. (`WorkQueue` times on
    /// `std::time::Instant`, so a paused tokio clock cannot drive it; the delay
    /// here is real and 1s.)
    #[tokio::test]
    async fn rc_worker_requeues_the_key_after_the_next_sync_duration() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = Arc::new(ReplicationControllerController::new(storage.clone(), 5));
        let mut rc = make_rc("later", "default", 1, rc_selector("later"), None);
        rc.spec.min_ready_seconds = Some(1);
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        make_ready(&mut pods[0], chrono::Utc::now());
        write_pod(&storage, &pods[0]).await;
        controller.expectations.delete_expectations("default/later");

        let queue = WorkQueue::new();
        let worker = tokio::spawn({
            let controller = Arc::clone(&controller);
            let queue = queue.clone();
            async move { controller.worker(queue).await }
        });
        queue
            .add("replicationcontrollers/default/later".to_string())
            .await;

        let available = async {
            loop {
                let rc: ReplicationController = storage
                    .get("/registry/replicationcontrollers/default/later")
                    .await
                    .unwrap();
                if rc.status.is_some_and(|s| s.available_replicas == Some(1)) {
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
