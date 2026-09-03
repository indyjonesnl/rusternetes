use super::expectations::ControllerExpectations;
use chrono::Utc;
use futures::StreamExt;
use rusternetes_common::{
    resources::{Pod, PodStatus, ReplicationController, ReplicationControllerCondition},
    types::{ObjectMeta, OwnerReference, Phase},
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::{sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

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
}

impl<S: Storage + 'static> ReplicationControllerController<S> {
    pub fn new(storage: Arc<S>, interval_secs: u64) -> Self {
        Self {
            storage,
            interval: Duration::from_secs(interval_secs),
            expectations: Arc::new(ControllerExpectations::new()),
        }
    }

    pub async fn run(self: Arc<Self>) -> rusternetes_common::Result<()> {
        info!("ReplicationController controller started (watch-based)");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

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
                                self.enqueue_owner_rc(&queue, &ev).await;
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

    /// When a pod changes, check its ownerReferences for a ReplicationController
    /// owner and enqueue that RC for reconciliation.
    async fn enqueue_owner_rc(&self, queue: &WorkQueue, event: &rusternetes_storage::WatchEvent) {
        let pod_key = extract_key(event);
        let parts: Vec<&str> = pod_key.splitn(3, '/').collect();
        let ns = match parts.get(1) {
            Some(ns) => *ns,
            None => return,
        };

        // Upstream's addPod/deletePod lower the owning controller's
        // expectations as the pod is observed
        // (replica_set.go:436 `CreationObserved`, :568 `DeletionObserved`).
        // Without this the counts never come down and every sync waits out the
        // 5-minute TTL instead of proceeding as soon as its creates land.
        match event {
            rusternetes_storage::WatchEvent::Added(_, value) => {
                for owner in rc_owner_names(value) {
                    self.expectations
                        .creation_observed(&format!("{ns}/{owner}"));
                }
            }
            // The delete event carries the previous value, so the owner is
            // still identifiable here even though the object is gone.
            rusternetes_storage::WatchEvent::Deleted(_, previous) => {
                for owner in rc_owner_names(previous) {
                    self.expectations
                        .deletion_observed(&format!("{ns}/{owner}"));
                }
            }
            rusternetes_storage::WatchEvent::Modified(..) => {}
        }

        let storage_key = format!("/registry/{}", pod_key);
        match self.storage.get::<Pod>(&storage_key).await {
            Ok(pod) => {
                if let Some(refs) = &pod.metadata.owner_references {
                    for owner_ref in refs {
                        if owner_ref.kind == "ReplicationController" {
                            queue
                                .add(format!("replicationcontrollers/{}/{}", ns, owner_ref.name))
                                .await;
                        }
                    }
                }
            }
            Err(_) => {
                // Pod deleted — enqueue all RCs in this namespace so they
                // notice the missing replica and create a replacement.
                if let Ok(items) = self
                    .storage
                    .list::<ReplicationController>(&build_prefix(
                        "replicationcontrollers",
                        Some(ns),
                    ))
                    .await
                {
                    for rc in &items {
                        queue
                            .add(format!(
                                "replicationcontrollers/{}/{}",
                                ns, rc.metadata.name
                            ))
                            .await;
                    }
                }
            }
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
                    Ok(()) => queue.forget(&key).await,
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
    pub async fn reconcile_all(&self) -> rusternetes_common::Result<()> {
        debug!("Reconciling all replicationcontrollers");

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

    async fn reconcile_rc(&self, rc: &ReplicationController) -> rusternetes_common::Result<()> {
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
                    return Ok(());
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
                return Ok(());
            }
            // If being deleted without orphan, skip reconciliation (let it terminate)
            self.expectations.delete_expectations(&rc_key);
            return Ok(());
        }

        debug!(
            "Reconciling replicationcontroller: {}/{}",
            namespace, rc.metadata.name
        );

        // Read expectations BEFORE listing pods. Upstream does the same
        // (replica_set.go:728 precedes the pod list) and its
        // `TestRSSyncExpectations` explains why: if the list is taken first and
        // expectations checked second, a pod arriving in between makes the
        // record look fulfilled while the list still lacks that pod — and the
        // controller creates a duplicate.
        let needs_sync = self.expectations.satisfied(&rc_key);

        // Get all pods in namespace
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods: Vec<Pod> = self.storage.list(&pods_prefix).await?;
        debug!(
            "Found {} total pods in namespace {}",
            all_pods.len(),
            namespace
        );

        // Adopt orphan pods and release non-matching owned pods
        let all_pods = self.adopt_and_release(rc, all_pods, namespace).await?;

        // Filter to only pods owned by this RC (after adopt/release).
        // K8s FilterActivePods: exclude Failed/Succeeded and terminating pods.
        // See: pkg/controller/controller_utils.go — FilterActivePods
        let rc_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|p| self.is_owned_by(p, rc))
            .collect();

        let active_rc_pods: Vec<&Pod> = rc_pods
            .iter()
            .filter(|p| {
                // Exclude terminating pods
                if p.metadata.deletion_timestamp.is_some() {
                    return false;
                }
                // Exclude terminal pods (Failed/Succeeded)
                !matches!(
                    p.status.as_ref().and_then(|s| s.phase.as_ref()),
                    Some(Phase::Failed) | Some(Phase::Succeeded)
                )
            })
            .collect();

        let current_replicas = active_rc_pods.len() as i32;
        let desired_replicas = rc.spec.replicas.unwrap_or(1);

        debug!(
            "ReplicationController {}/{}: current={}, desired={} (matched {} pods)",
            namespace,
            rc.metadata.name,
            current_replicas,
            desired_replicas,
            rc_pods.len()
        );

        let mut create_failure: Option<String> = None;
        // Upstream: `if rsNeedsSync && rs.DeletionTimestamp == nil { manageReplicas }`
        // (replica_set.go:756). The deletionTimestamp half is handled by the
        // early return above. Status is still recalculated below either way —
        // upstream "always updates status as pods come up or die".
        if !needs_sync {
            debug!(
                "RC {}: waiting on in-flight pod creates/deletes, skipping manage",
                rc_key
            );
        }
        // Set when creates are rejected because the namespace is terminating.
        // Deliberately NOT folded into `create_failure`: that drives the
        // ReplicaFailure condition, and upstream does not raise one for this.
        let mut namespace_terminating = false;
        // Pods this reconcile successfully asked the backend to create. Only
        // these are worth waiting to observe — a create that failed will never
        // produce a pod.
        let mut created = 0usize;
        if needs_sync {
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
                let to_create =
                    ((desired_replicas - current_replicas) as usize).min(BURST_REPLICAS);
                // Record the creates before issuing them, so a reconcile triggered
                // by our own pod-create watch events does not re-issue the batch
                // (upstream `ExpectCreations`, replica_set.go:619).
                self.expectations
                    .expect_creations(&rc_key, to_create as i64);
                for batch in slow_start_batches(to_create, SLOW_START_INITIAL_BATCH_SIZE) {
                    let results = futures::future::join_all(
                        (0..batch).map(|i| self.create_pod(rc, (created + i) as i32)),
                    )
                    .await;

                    let mut batch_failure = None;
                    for result in results {
                        match result {
                            Ok(()) => created += 1,
                            // The namespace is going away, so every create in this
                            // reconcile will fail and none of them matter. Upstream
                            // returns nil here so the create is not counted as a
                            // replica failure (replica_set.go:643-651); stopping
                            // now additionally spares the api-server a batch of
                            // requests it would only reject.
                            Err(e) if is_namespace_terminating_rejection(&e) => {
                                debug!(
                                    "RC {}/{}: namespace is terminating, abandoning pod creation",
                                    namespace, rc.metadata.name
                                );
                                namespace_terminating = true;
                            }
                            Err(e) => {
                                error!("Failed to create pod for RC {}: {}", rc.metadata.name, e);
                                batch_failure.get_or_insert_with(|| e.to_string());
                            }
                        }
                    }

                    if namespace_terminating {
                        break;
                    }

                    // Upstream stops at the first failing batch and reports what
                    // succeeded (`replica_set.go:838-840`), rather than pushing
                    // the remaining rounds at an api-server that is already
                    // rejecting — the next reconcile retries from the new count.
                    if let Some(e) = batch_failure {
                        create_failure = Some(e);
                        break;
                    }
                }
                // Creates we never attempted must not be waited on: upstream
                // decrements the expectation for each skipped pod, because no watch
                // event will ever arrive for them (replica_set.go:645-651).
                let skipped = to_create.saturating_sub(created);
                for _ in 0..skipped {
                    self.expectations.creation_observed(&rc_key);
                }
            } else if current_replicas > desired_replicas {
                // Need to delete excess pods
                let to_delete =
                    ((current_replicas - desired_replicas) as usize).min(BURST_REPLICAS);
                self.expectations
                    .expect_deletions(&rc_key, to_delete as i64);
                for pod in rc_pods.iter().take(to_delete) {
                    self.delete_pod(&pod.metadata.name, namespace).await?;
                }
            }
        }

        // Re-fetch and recount pods after create/delete operations to get accurate status
        let pods_prefix = build_prefix("pods", Some(namespace));
        let all_pods_after: Vec<Pod> = self.storage.list(&pods_prefix).await?;

        let rc_pods_after: Vec<Pod> = all_pods_after
            .into_iter()
            .filter(|p| self.is_owned_by(p, rc))
            .collect();

        // Count only active (non-Failed, non-Succeeded) pods as replicas
        let active_pods: Vec<&Pod> = rc_pods_after
            .iter()
            .filter(|pod| {
                !matches!(
                    pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                    Some(Phase::Failed) | Some(Phase::Succeeded)
                )
            })
            .collect();

        // Lower expectations for the creates this re-list already shows.
        //
        // Upstream lowers them from informer events (replica_set.go:436
        // `CreationObserved`) because its sync reads a cache rather than the
        // store. Ours reads storage directly, so this re-list is an
        // observation in its own right — and a fresher one than the watch
        // event that will follow. The pod watch still lowers the count as
        // well, which covers the window before this runs.
        //
        // Only the creates that SUCCEEDED can ever be observed; the failed
        // ones were already settled above. Counting "pods still missing"
        // instead would re-arm the expectation after a create that failed
        // (a quota denial, say) and wedge the next sync until the 5-minute TTL
        // — the opposite of what expectations are for.
        //
        // The guard survives where it matters: against an api-server serving a
        // stale list the created pods are not visible yet, so they stay
        // outstanding and the next reconcile is still gated.
        if needs_sync && created > 0 {
            let newly_visible =
                (active_pods.len() as i64 - current_replicas as i64).clamp(0, created as i64);
            for _ in 0..newly_visible {
                self.expectations.creation_observed(&rc_key);
            }
        }

        let final_current_replicas = active_pods.len() as i32;
        let final_ready_replicas = active_pods
            .iter()
            .filter(|pod| {
                pod.status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conditions| {
                        conditions
                            .iter()
                            .any(|c| c.condition_type == "Ready" && c.status == "True")
                    })
                    .unwrap_or(false)
            })
            .count() as i32;

        // Check for failed pods as a failure signal
        let _failed_pods = rc_pods_after
            .iter()
            .filter(|pod| {
                matches!(
                    pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                    Some(Phase::Failed)
                )
            })
            .count();

        // K8s only sets ReplicaFailure from actual pod creation errors
        // (manageReplicas return value), NOT from existing Failed pods.
        // Don't override create_failure here — the condition should clear
        // once pod creation succeeds again.
        // See: pkg/controller/replication/replication_controller.go — syncReplicationController

        // Update status with accurate counts
        self.update_status(
            rc,
            final_current_replicas,
            final_ready_replicas,
            create_failure.as_deref(),
        )
        .await?;

        Ok(())
    }

    /// Check if a pod's labels match the RC's selector (pure label check, ignores ownerReference)
    fn labels_match_selector(&self, pod: &Pod, rc: &ReplicationController) -> bool {
        if let Some(selector) = &rc.spec.selector {
            if let Some(pod_labels) = &pod.metadata.labels {
                for (key, value) in selector {
                    if pod_labels.get(key) != Some(value) {
                        return false;
                    }
                }
                return true;
            }
        }
        false
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

            let labels_match = self.labels_match_selector(pod, rc);
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

        // K8s uses <rc-name>-<5-char-random> to keep pod names under 63 chars
        let suffix: String = uuid::Uuid::new_v4().to_string().chars().take(5).collect();
        let pod_name = format!("{}-{}", rc.metadata.name, suffix);

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

        // Check ResourceQuota before creating pod
        super::check_resource_quota(&*self.storage, namespace)
            .await
            .map_err(|e| rusternetes_common::Error::Forbidden(e.to_string()))?;

        let key = build_key("pods", Some(namespace), &pod_name);
        self.storage.create(&key, &pod).await?;

        info!(
            "Created pod {}/{} for replicationcontroller {}",
            namespace, pod_name, rc.metadata.name
        );

        Ok(())
    }

    async fn delete_pod(&self, name: &str, namespace: &str) -> rusternetes_common::Result<()> {
        let key = build_key("pods", Some(namespace), name);
        self.storage.delete(&key).await?;

        info!("Deleted pod {}/{}", namespace, name);

        Ok(())
    }

    async fn update_status(
        &self,
        rc: &ReplicationController,
        current_replicas: i32,
        ready_replicas: i32,
        failure_message: Option<&str>,
    ) -> rusternetes_common::Result<()> {
        let namespace = rc.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key("replicationcontrollers", Some(namespace), &rc.metadata.name);

        // Re-read from storage for fresh resourceVersion to avoid CAS conflicts
        let mut updated_rc: ReplicationController = match self.storage.get(&key).await {
            Ok(rc) => rc,
            Err(_) => rc.clone(),
        };

        // Build conditions: preserve existing conditions of unknown types,
        // only manage "ReplicaFailure" condition type
        let existing_conditions = updated_rc
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .cloned()
            .unwrap_or_default();

        // Keep all conditions that are NOT managed by this controller
        let mut conditions: Vec<_> = existing_conditions
            .into_iter()
            .filter(|c| c.condition_type != "ReplicaFailure")
            .collect();

        if let Some(msg) = failure_message {
            // Add ReplicaFailure when there's an actual failure message
            // (quota exceeded, pod creation failed, etc.)
            conditions.push(ReplicationControllerCondition {
                condition_type: "ReplicaFailure".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(Utc::now()),
                reason: Some("FailedCreate".to_string()),
                message: Some(msg.to_string()),
            });
        }
        // else: ReplicaFailure already filtered out above, clearing it

        let final_conditions = if conditions.is_empty() {
            None
        } else {
            Some(conditions)
        };

        let new_status = rusternetes_common::resources::ReplicationControllerStatus {
            replicas: current_replicas,
            fully_labeled_replicas: Some(current_replicas),
            ready_replicas: Some(ready_replicas),
            available_replicas: Some(ready_replicas),
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

        let new_status_clone = new_status.clone();
        updated_rc.status = Some(new_status);

        // update_status, NOT update: a full-object PUT has its `.status`
        // stripped by any api-server that exposes a status subresource (see
        // crates/storage/src/api_storage.rs). Against a vanilla control plane
        // every RC status write vanished — a Running single-replica RC reported
        // `status={"replicas":0}` forever, so the lifecycle spec's watch for
        // `replicas == readyReplicas == 1` timed out, the scale spec could not
        // confirm its replica count, and the ReplicaFailure condition this
        // function builds never reached the object. Same defect the PDB
        // controller had in #1712.
        if let Err(e) = self.storage.update_status(&key, &updated_rc).await {
            // CAS conflict — re-read and retry once to ensure condition updates persist
            debug!("RC status update CAS conflict, retrying: {}", e);
            if let Ok(mut fresh_rc) = self.storage.get::<ReplicationController>(&key).await {
                if fresh_rc.status.as_ref() != Some(&new_status_clone) {
                    fresh_rc.status = Some(new_status_clone);
                    let _ = self.storage.update_status(&key, &fresh_rc).await;
                }
            }
        }

        debug!(
            "Updated status for replicationcontroller {}/{}",
            namespace, rc.metadata.name
        );

        Ok(())
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
fn slow_start_batches(count: usize, initial_batch_size: usize) -> Vec<usize> {
    let mut batches = Vec::new();
    let mut remaining = count;
    let mut batch = initial_batch_size.min(remaining);
    while batch > 0 {
        batches.push(batch);
        remaining -= batch;
        batch = (2 * batch).min(remaining);
    }
    batches
}

/// Names of the ReplicationControllers owning a serialised pod.
///
/// Used to attribute a pod watch event to the controller whose expectations it
/// fulfils. A payload we cannot parse yields nothing, which merely means the
/// expectation waits for its TTL rather than being wrongly lowered.
fn rc_owner_names(serialised_pod: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(serialised_pod) else {
        return Vec::new();
    };
    value
        .pointer("/metadata/ownerReferences")
        .and_then(|refs| refs.as_array())
        .map(|refs| {
            refs.iter()
                .filter(|r| r.get("kind").and_then(|k| k.as_str()) == Some("ReplicationController"))
                .filter_map(|r| r.get("name").and_then(|n| n.as_str()).map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
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
/// (pkg/controller/replicaset/replica_set.go:643-651). It is deliberately NOT
/// a `ReplicaFailure`: the namespace is going away, so there is nothing wrong
/// with this ReplicaSet and nothing for an operator to act on.
///
/// Upstream matches the structured `StatusCause`; our storage layer renders an
/// api-server error into a string (`map_write_err`, crates/storage/src/
/// api_storage.rs), so we match the cause reason or the message text the
/// api-server emits (crates/middleware/src/lib.rs, added in #1849). Both
/// spellings are accepted so this keeps working if the cause is ever surfaced
/// structurally.
fn is_namespace_terminating_rejection(err: &rusternetes_common::Error) -> bool {
    let rusternetes_common::Error::Forbidden(message) = err else {
        return false;
    };
    message.contains("NamespaceTerminating") || message.contains("because it is being terminated")
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
const BURST_REPLICAS: usize = 500;

/// Upstream's `SlowStartInitialBatchSize` (pkg/controller/controller_utils.go).
const SLOW_START_INITIAL_BATCH_SIZE: usize = 1;

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::workloads::{PodTemplateSpec, ReplicationControllerSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

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

    #[tokio::test]
    async fn test_rc_sets_replica_failure_on_quota_exceeded() {
        let storage = Arc::new(MemoryStorage::new());
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
        let storage = Arc::new(MemoryStorage::new());
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
    struct ConcurrencyProbeStorage {
        inner: Arc<MemoryStorage>,
        in_flight: std::sync::atomic::AtomicUsize,
        peak_in_flight: std::sync::atomic::AtomicUsize,
    }

    impl ConcurrencyProbeStorage {
        fn new(inner: Arc<MemoryStorage>) -> Self {
            Self {
                inner,
                in_flight: std::sync::atomic::AtomicUsize::new(0),
                peak_in_flight: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn peak(&self) -> usize {
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
    /// (pkg/controller/replicaset/replica_set.go:643-651)
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
    struct TerminatingNamespaceStorage {
        inner: Arc<MemoryStorage>,
        pod_create_attempts: std::sync::atomic::AtomicUsize,
    }

    impl TerminatingNamespaceStorage {
        fn new(inner: Arc<MemoryStorage>) -> Self {
            Self {
                inner,
                pod_create_attempts: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn attempts(&self) -> usize {
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

    /// When the namespace is terminating, the RC must give up quietly: no
    /// ReplicaFailure, and no batch of creates the api-server can only reject.
    ///
    /// Before this, #1849's admission rejection had no consumer, so every
    /// namespace teardown left the RC retrying creates that could never
    /// succeed and stamping a condition upstream deliberately never sets
    /// (pkg/controller/replicaset/replica_set.go:643-651).
    #[tokio::test]
    async fn rc_abandons_creates_when_namespace_is_terminating() {
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

        // Must not surface as a reconcile error: the namespace going away is
        // not this controller's problem to report.
        controller
            .reconcile_all()
            .await
            .expect("a terminating namespace must not fail the reconcile");

        // Upstream sets ReplicaFailure only for real create failures — a quota
        // denial, say. Not for this.
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

        // And it must stop rather than push the remaining batches at an
        // api-server that is rejecting all of them: the first slow-start batch
        // is 1, so exactly one attempt for a 20-replica RC.
        assert_eq!(
            storage.attempts(),
            1,
            "must abandon after the first rejected batch, not attempt all 20"
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

        // The cap is a throttle, not a ceiling: the next sync finishes the job.
        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            desired as usize,
            "the following sync must create the remainder"
        );
    }
}
