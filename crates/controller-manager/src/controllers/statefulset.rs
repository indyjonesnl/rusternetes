use super::pod_informer::{ControllerRef, PodEventTarget, ResolvedController, SharedPodInformer};
use super::replicationcontroller::slow_start_batches_capped;
use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::{
    PersistentVolumeClaim, Pod, PodStatus, StatefulSet, StatefulSetStatus,
};
use rusternetes_common::types::{
    label_selector_as_selector, ObjectMeta, OwnerReference, Phase, Selector, TypeMeta,
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// `slowStartBatch` initial size for the StatefulSet controller's burst paths
/// (pkg/controller/statefulset/stateful_set_control.go:544, `slowStartBatch(1, ...)`).
const SLOW_START_INITIAL_BATCH_SIZE: usize = 1;

/// `MaxBatchSize` (pkg/controller/statefulset/stateful_set_control.go:42).
const MAX_BATCH_SIZE: usize = 500;

/// Upstream `ConcurrentStatefulSetSyncs` default, workers launched by `Run`
/// (pkg/controller/statefulset/config/v1alpha1/defaults.go:34;
/// pkg/controller/statefulset/stateful_set.go:193-196).
const CONCURRENT_STATEFULSET_SYNCS: usize = 5;

pub struct StatefulSetController<S: Storage> {
    pub storage: Arc<S>,
    /// The shared pod informer whose `addPod`/`updatePod`/`deletePod`
    /// handlers this controller registers (stateful_set.go:127-141,
    /// `podInformer.Informer().AddEventHandler`).
    pub pod_informer: Arc<SharedPodInformer<S>>,
}

impl<S: Storage + 'static> StatefulSetController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        let pod_informer = Arc::new(SharedPodInformer::new(
            storage.clone(),
            Duration::from_secs(5),
        ));
        Self::with_pod_informer(storage, pod_informer)
    }

    /// A controller whose pod handlers hang off `pod_informer`, the one
    /// informer the binary shares between controllers.
    pub fn with_pod_informer(storage: Arc<S>, pod_informer: Arc<SharedPodInformer<S>>) -> Self {
        Self {
            storage,
            pod_informer,
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting StatefulSetController (watch-based)");
        let retry_interval = Duration::from_secs(5);

        let queue = WorkQueue::new();

        // Upstream starts `workers` goroutines over one queue (stateful_set.go:193-196).
        spawn_workers(CONCURRENT_STATEFULSET_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        // Pod events come from the shared pod informer, as upstream's
        // `podInformer.Informer().AddEventHandler` (stateful_set.go:127-141).
        self.pod_informer
            .add_event_handler(Arc::clone(&self), queue.clone())
            .await;
        self.pod_informer.start();

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch for changes to StatefulSets
            let prefix = "/registry/statefulsets/";
            let watch_result = self.storage.watch(prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

            // Periodic full resync as safety net (every 30s)
            let mut resync = tokio::time::interval(Duration::from_secs(30));
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
            let storage_key = build_key("statefulsets", Some(ns), name);
            match self.storage.get::<StatefulSet>(&storage_key).await {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.reconcile(&mut resource).await {
                        Ok(()) => queue.forget(&key).await,
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
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
            .list::<StatefulSet>("/registry/statefulsets/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("statefulsets/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list statefulsets for enqueue: {}", e);
            }
        }
    }

    pub async fn reconcile_all(&self) -> Result<()> {
        let statefulsets: Vec<StatefulSet> = self.storage.list("/registry/statefulsets/").await?;

        for mut statefulset in statefulsets {
            if let Err(e) = self.reconcile(&mut statefulset).await {
                error!(
                    "Failed to reconcile StatefulSet {}: {}",
                    statefulset.metadata.name, e
                );
            }
        }

        Ok(())
    }

    pub async fn reconcile(&self, statefulset: &mut StatefulSet) -> Result<()> {
        let name = &statefulset.metadata.name;
        let namespace = statefulset.metadata.namespace.as_ref().unwrap();

        // Skip reconciliation for StatefulSets being deleted — GC handles pod cleanup
        if statefulset.metadata.is_being_deleted() {
            return Ok(());
        }

        debug!("Reconciling StatefulSet {}/{}", namespace, name);

        let desired_replicas = statefulset.spec.replicas.unwrap_or(1);

        // Get current pods for this StatefulSet
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Filter pods that belong to this StatefulSet via ownerReferences (authoritative)
        // Fall back to label matching for backwards compatibility
        let statefulset_uid = &statefulset.metadata.uid;
        let statefulset_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == statefulset_uid))
                    .unwrap_or(false);
                let owned_by_label = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("app"))
                    .map(|app| app == name)
                    .unwrap_or(false)
                    && pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|labels| labels.get("statefulset.kubernetes.io/pod-name"))
                        .is_some();
                owned_by_ref || owned_by_label
            })
            .collect();

        // K8s processReplica(): delete Failed/Succeeded pods so they get recreated.
        // This matches K8s behavior where the StatefulSet controller deletes completed
        // pods and recreates them on the next sync cycle.
        let mut active_pods = Vec::new();
        for pod in statefulset_pods {
            let is_terminal = matches!(
                pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Failed) | Some(Phase::Succeeded)
            );
            if is_terminal && pod.metadata.deletion_timestamp.is_none() {
                // Delete the terminal pod so it gets recreated. Issue a DELETE
                // rather than writing deletionTimestamp: that field is
                // immutable on update, so an api-server that enforces it
                // rejects the write and the replica is never replaced.
                // Upstream deletes a Failed/Succeeded replica the same way,
                // with default DeleteOptions
                // (pkg/controller/statefulset/stateful_set_control.go:428 ->
                // DeleteStatefulPod).
                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                if let Err(e) = self.storage.delete_gracefully(&pod_key).await {
                    error!("failed to delete terminal pod {}: {}", pod_key, e);
                }
                info!(
                    "StatefulSet {}/{}: deleted terminal pod {} (phase: {:?})",
                    namespace,
                    name,
                    pod.metadata.name,
                    pod.status.as_ref().and_then(|s| s.phase.as_ref())
                );
                // Deliberately NOT kept below: this pod has just been asked to go
                // away, and upstream ends the sync here anyway (see below).
            } else {
                // Everything else stays — INCLUDING a terminal pod that is already
                // terminating. Dropping those was the second half of the
                // scale-down cascade (#1821): once the kubelet killed `ss-2` its
                // phase went Failed while its deletionTimestamp was set, so it
                // vanished from this list, `ss-1` became the head of the condemned
                // list, and the next reconcile deleted it — then `ss-0`. All three
                // pods got "Killing: Stopping container webserver" on the same
                // second and `[sig-apps] StatefulSet ... Scaling should happen in
                // predictable order` timed out on the ordered DELETED events.
                //
                // Upstream never drops them: `processReplica`
                // (pkg/controller/statefulset/stateful_set_control.go:426-434)
                // returns `shouldExit = true` for a Failed/Succeeded replica
                // whether or not it already carries a deletionTimestamp, so the
                // sync ends before the condemned loop runs at all; and the
                // condemned loop itself blocks on `isTerminating` (:510-518).
                // Keeping the pod visible is what lets that guard fire.
                active_pods.push(pod);
            }
        }
        let mut statefulset_pods = active_pods;

        // Sort pods by ordinal index
        statefulset_pods.sort_by_key(|pod| {
            pod.metadata
                .name
                .rsplit_once('-')
                .and_then(|(_, idx)| idx.parse::<i32>().ok())
                .unwrap_or(0)
        });

        // Count only non-terminating pods as current replicas.
        // Terminating pods (deletion_timestamp set) should not prevent scale-up
        // to recreate them with the new template.
        let current_replicas = statefulset_pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .count() as i32;

        debug!(
            "StatefulSet {}/{}: desired={}, current={}",
            namespace, name, desired_replicas, current_replicas
        );

        let is_ordered_ready = statefulset
            .spec
            .pod_management_policy
            .as_ref()
            .map(|p| p == "OrderedReady")
            .unwrap_or(true);

        // Extract partition for rolling update — pods below partition use current (old) template
        let partition = statefulset
            .spec
            .update_strategy
            .as_ref()
            .and_then(|s| s.rolling_update.as_ref())
            .and_then(|ru| ru.partition)
            .unwrap_or(0);

        // Scale up or down
        if current_replicas < desired_replicas {
            // Scale up: create any missing pods in ordinal order.
            // During rolling updates, gaps can appear at any ordinal (not just at the end),
            // so we check all ordinals 0..desired rather than current..desired.
            for i in 0..desired_replicas {
                let pod_name = format!("{}-{}", name, i);
                let pod_key = build_key("pods", Some(namespace), &pod_name);
                // Treat evicted/terminating pods (deletionTimestamp set) as missing
                // so the controller recreates them. The kubelet handles actual
                // deletion from storage after graceful shutdown.
                let pod_exists = match self.storage.get::<Pod>(&pod_key).await {
                    Ok(pod) => pod.metadata.deletion_timestamp.is_none(),
                    Err(_) => false,
                };
                if pod_exists {
                    continue;
                }
                // For OrderedReady policy, check that the previous pod is Ready before
                // creating the next one. If it's not ready, halt scaling.
                if is_ordered_ready && i > 0 {
                    let prev_pod_name = format!("{}-{}", name, i - 1);
                    let prev_pod_key = build_key("pods", Some(namespace), &prev_pod_name);
                    match self.storage.get::<Pod>(&prev_pod_key).await {
                        Ok(prev_pod) => {
                            let is_ready = prev_pod
                                .status
                                .as_ref()
                                .and_then(|s| s.conditions.as_ref())
                                .map(|conditions| {
                                    conditions
                                        .iter()
                                        .any(|c| c.condition_type == "Ready" && c.status == "True")
                                })
                                .unwrap_or(false);

                            if !is_ready {
                                info!(
                                    "StatefulSet {}: pod {} not ready, halting scale-up",
                                    name, prev_pod_name
                                );
                                break;
                            }
                        }
                        Err(_) => {
                            // Previous pod doesn't exist yet
                            info!(
                                "StatefulSet {}: pod {} not found, halting scale-up",
                                name, prev_pod_name
                            );
                            break;
                        }
                    }
                }

                // Ensure PVCs exist for this ordinal before creating the pod
                self.ensure_pvcs_for_ordinal(statefulset, i, namespace)
                    .await?;
                // For rolling updates with partition: pods below partition should
                // use the CURRENT revision (old template), not the update revision.
                // This matches K8s behavior where newVersionedStatefulSetPod creates
                // pods with the appropriate template based on ordinal vs partition.
                let current_rev = statefulset
                    .status
                    .as_ref()
                    .and_then(|s| s.current_revision.as_deref());
                let update_rev_str = Self::compute_revision(&statefulset.spec.template);
                if i < partition {
                    if let Some(cr_rev) = current_rev {
                        if cr_rev != update_rev_str {
                            // Pod is below partition — try to create with the old template
                            // by looking up the ControllerRevision
                            if let Some(old_template) = self
                                .get_template_from_revision(
                                    namespace,
                                    &statefulset.metadata.name,
                                    cr_rev,
                                )
                                .await
                            {
                                self.create_pod_with_template(
                                    statefulset,
                                    i,
                                    namespace,
                                    &old_template,
                                    cr_rev,
                                )
                                .await?;
                                info!(
                                    "Created pod {}-{} with current revision {}",
                                    name, i, cr_rev
                                );
                                continue;
                            }
                        }
                    }
                }
                self.create_pod(statefulset, i, namespace).await?;
                info!("Created pod {}-{}", name, i);
            }
        } else if current_replicas > desired_replicas {
            // Scale down following K8s processCondemned() logic:
            // Pods with ordinal >= desired_replicas are "condemned" (to be deleted).
            // Process them in REVERSE ordinal order (highest first).
            // For OrderedReady policy, enforce:
            //   - If any pod is terminating, BLOCK (wait for it)
            //   - Find the first unhealthy pod (lowest ordinal, any pod not Running+Ready)
            //   - A condemned pod can only be deleted if:
            //     a) It IS Running and Ready, OR
            //     b) It IS the firstUnhealthyPod (the one with lowest ordinal among unhealthy)
            //   - Delete at most ONE pod per reconcile cycle

            // Find the first unhealthy pod across ALL pods (lowest ordinal first)
            let first_unhealthy_name = if is_ordered_ready {
                statefulset_pods.iter().find_map(|p| {
                    let is_ready = p
                        .status
                        .as_ref()
                        .and_then(|s| s.conditions.as_ref())
                        .map(|conds| {
                            conds
                                .iter()
                                .any(|c| c.condition_type == "Ready" && c.status == "True")
                        })
                        .unwrap_or(false);
                    let is_running = matches!(
                        p.status.as_ref().and_then(|s| s.phase.as_ref()),
                        Some(Phase::Running)
                    );
                    if (!is_ready || !is_running) && p.metadata.deletion_timestamp.is_none() {
                        Some(p.metadata.name.clone())
                    } else {
                        None
                    }
                })
            } else {
                None
            };

            // Process condemned pods (ordinal >= desired_replicas) in reverse order
            let mut condemned: Vec<&Pod> = statefulset_pods
                .iter()
                .filter(|p| {
                    let ordinal = p
                        .metadata
                        .name
                        .rsplit_once('-')
                        .and_then(|(_, idx)| idx.parse::<i32>().ok())
                        .unwrap_or(0);
                    ordinal >= desired_replicas
                })
                .collect();
            condemned.sort_by_key(|p| {
                std::cmp::Reverse(
                    p.metadata
                        .name
                        .rsplit_once('-')
                        .and_then(|(_, idx)| idx.parse::<i32>().ok())
                        .unwrap_or(0),
                )
            });

            // Upstream `processCondemned`
            // (pkg/controller/statefulset/stateful_set_control.go:508-535) and
            // `runForAll` (:537-549):
            //
            // * monotonic (OrderedReady): `shouldExit = true` from EVERY branch, and
            //   `runForAll` stops on the first one. Exactly ONE condemned pod is
            //   processed per sync, whatever the outcome: blocked, skipped,
            //   deleted, or failed to delete. A terminating pod blocks (:510-518).
            //   (This used to fall through to the next, lower ordinal when the
            //   freshly-read pod was already terminating, the delete errored or
            //   the pod was gone; against a vanilla api-server the reconcile list
            //   is a moment stale, so a 3->0 scale-down killed all three pods in
            //   one second, out of order (#1821).)
            // * Parallel (burst, `!monotonic`): `processCondemned` returns
            //   `(false, nil)` for a terminating pod so the batch keeps going, and
            //   `runForAll` runs `slowStartBatch(1, len(condemned), fn)` over ALL
            //   condemned pods (#1822). A failing batch stops the rest and the
            //   errors are aggregated (:351-356).
            //
            // No expectations here: upstream's StatefulSet controller has none,
            // and the re-read below keeps a stale list from re-deleting a pod.
            if is_ordered_ready {
                if let Some(pod) = condemned.first() {
                    let blocked = Self::condemned_blocked(pod, true, first_unhealthy_name.as_ref());
                    if let Err(e) = self
                        .delete_condemned_pod(
                            namespace,
                            pod,
                            blocked,
                            current_replicas,
                            desired_replicas,
                        )
                        .await
                    {
                        error!("failed to delete pod {}: {}", pod.metadata.name, e);
                    }
                }
            } else {
                // `processCondemned` skips (not blocks) a terminating pod.
                let targets: Vec<&Pod> = condemned
                    .iter()
                    .copied()
                    .filter(|p| p.metadata.deletion_timestamp.is_none())
                    .collect();
                let mut next = 0usize;
                for batch in slow_start_batches_capped(
                    targets.len(),
                    SLOW_START_INITIAL_BATCH_SIZE,
                    MAX_BATCH_SIZE,
                ) {
                    let results =
                        futures::future::join_all(targets[next..next + batch].iter().map(|pod| {
                            self.delete_condemned_pod(
                                namespace,
                                pod,
                                false,
                                current_replicas,
                                desired_replicas,
                            )
                        }))
                        .await;
                    next += batch;
                    let errors: Vec<String> = results
                        .into_iter()
                        .filter_map(|r| r.err().map(|e| e.to_string()))
                        .collect();
                    if !errors.is_empty() {
                        error!(
                            "StatefulSet {}/{}: {} condemned pod delete(s) failed, stopping the batch: {}",
                            namespace,
                            name,
                            errors.len(),
                            errors.join("; ")
                        );
                        break;
                    }
                }
            }
        }

        // PVC retention policy: when `whenScaled=Delete`, garbage-collect PVCs
        // whose ordinal is beyond the desired replica count. K8s creates a PVC
        // per ordinal from `volumeClaimTemplates`; on scale-down those PVCs
        // become orphaned and must be reclaimed (default policy is `Retain`).
        // See: pkg/controller/statefulset/stateful_set_control.go (PVC GC).
        self.gc_scaled_down_pvcs(statefulset, namespace, desired_replicas)
            .await?;

        // Rolling update: if replica count matches but pods have old revision, delete one at a time.
        // The controller will recreate them with the new template on the next reconcile.
        // Skip if updateStrategy is OnDelete (user must manually delete pods to trigger update).
        let update_strategy = statefulset
            .spec
            .update_strategy
            .as_ref()
            .and_then(|s| s.strategy_type.as_deref())
            .unwrap_or("RollingUpdate");

        if current_replicas == desired_replicas
            && desired_replicas > 0
            && update_strategy == "RollingUpdate"
        {
            let update_revision = Self::compute_revision(&statefulset.spec.template);
            debug!(
                "StatefulSet {}/{}: rolling update check, update_revision={}",
                namespace, name, update_revision
            );

            // Check pods in reverse order for rolling update.
            // Only update pods with ordinal >= partition.
            // For each pod: if it has a stale revision AND is Ready (or at least Running),
            // delete it so it gets recreated with the new template.
            // If the most recently deleted pod's replacement is not yet Ready, wait before
            // deleting the next one.
            let mut deleted_one = false;
            let min_ready_seconds_for_update =
                statefulset.spec.min_ready_seconds.unwrap_or(0).max(0);
            for pod in statefulset_pods.iter().rev() {
                let ordinal = pod
                    .metadata
                    .name
                    .rsplit_once('-')
                    .and_then(|(_, idx)| idx.parse::<i32>().ok())
                    .unwrap_or(0);

                // Only update pods with ordinal >= partition
                if ordinal < partition {
                    continue;
                }

                let pod_revision = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("controller-revision-hash"))
                    .map(|s| s.as_str())
                    .unwrap_or("");
                debug!(
                    "StatefulSet {}/{}: pod {} revision={} vs update_revision={}",
                    namespace, name, pod.metadata.name, pod_revision, update_revision
                );
                if pod_revision != update_revision {
                    // Check if this pod is at least Running or Ready — don't delete pods
                    // that haven't even started yet (prevents cascading deletions during initial creation)
                    let pod_phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                    let pod_is_active =
                        matches!(pod_phase, Some(Phase::Running) | Some(Phase::Pending));
                    let pod_is_ready = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.conditions.as_ref())
                        .map(|c| {
                            c.iter()
                                .any(|cond| cond.condition_type == "Ready" && cond.status == "True")
                        })
                        .unwrap_or(false);

                    // Delete pods with stale revision using graceful termination
                    // (set deletionTimestamp instead of direct delete, so the kubelet
                    // can perform cleanup and the pod gets properly recreated).
                    // Empty revision means newly created — skip those.
                    if !pod_revision.is_empty() && (pod_is_ready || pod_is_active) {
                        let pod_key = format!("/registry/pods/{}/{}", namespace, pod.metadata.name);
                        if pod.metadata.deletion_timestamp.is_none() {
                            // Same as scale-down: DELETE, never stamp. Upstream
                            // rolls pods by deleting them
                            // (stateful_set_control.go:721 ->
                            // DeleteStatefulPod) and recreating at the new
                            // revision.
                            if let Err(e) = self.storage.delete_gracefully(&pod_key).await {
                                error!("failed to delete pod {} for update: {}", pod_key, e);
                            } else {
                                info!(
                                    "Rolling update: deleted pod {} (old revision {}, update revision {})",
                                    pod.metadata.name, pod_revision, update_revision
                                );
                            }
                        }
                        deleted_one = true;
                        break; // Delete one at a time for OrderedReady rolling updates
                    }
                }
                // Upstream, still inside the same ordinal loop
                // (pkg/controller/statefulset/stateful_set_control.go:743-749,
                // `updateStatefulSet`): "wait for unavailable Pods on update".
                // A pod already at the update revision that is terminating or
                // not yet available (`isUnavailable`,
                // stateful_set_utils.go:491) ends the sync, so the next lower
                // ordinal is never replaced while this replacement is coming up.
                // (MaxUnavailableStatefulSet is Beta default-off in 1.35, so
                // maxUnavailable is always 1.)
                if pod_revision == update_revision
                    && (pod.metadata.deletion_timestamp.is_some()
                        || !Self::is_pod_available(pod, min_ready_seconds_for_update))
                {
                    debug!(
                        "StatefulSet {}/{}: waiting for pod {} to become available",
                        namespace, name, pod.metadata.name
                    );
                    break;
                }
            }
            let _ = deleted_one;
        }

        // Re-fetch and recount pods after create/delete operations to get accurate status
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods_after: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        let statefulset_pods_after: Vec<Pod> = all_pods_after
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == statefulset_uid))
                    .unwrap_or(false);
                let owned_by_label = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("statefulset.kubernetes.io/pod-name"))
                    .is_some()
                    && pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|labels| labels.get("app"))
                        .map(|app| app == name)
                        .unwrap_or(false);
                owned_by_ref || owned_by_label
            })
            .collect();

        // K8s computeReplicaStatus() counts differently per field:
        // - replicas: isCreated(pod) — includes terminating pods
        // - readyReplicas: isRunningAndReady(pod) — Running + Ready condition
        // - availableReplicas: isRunningAndAvailable(pod, minReadySeconds)
        // - currentReplicas/updatedReplicas: isCreated && !isTerminating && revision match
        // See: pkg/controller/statefulset/stateful_set_control.go:370-399
        let is_created =
            |pod: &&Pod| -> bool { pod.status.as_ref().and_then(|s| s.phase.as_ref()).is_some() };
        let is_terminating = |pod: &&Pod| -> bool { pod.metadata.deletion_timestamp.is_some() };
        let is_ready = |pod: &&Pod| -> bool {
            // K8s isRunningAndReady excludes terminating pods so that a pod
            // marked for graceful deletion drops out of readyReplicas
            // immediately. Conformance tests for SS eviction/scale-down rely
            // on this — see pkg/controller/statefulset/stateful_set_utils.go.
            if is_terminating(pod) {
                return false;
            }
            matches!(
                pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Running)
            ) && pod
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conditions| {
                    conditions
                        .iter()
                        .any(|c| c.condition_type == "Ready" && c.status == "True")
                })
                .unwrap_or(false)
        };

        // replicas = created pods excluding those being terminated.
        // K8s historically counted all created pods, but conformance tests for
        // StatefulSet eviction/scale-down observe replicas decrement as soon as
        // deletionTimestamp is set (graceful termination), not only when the
        // pod is fully removed. Counting terminating pods here causes the
        // scale-down test to flap on `status.replicas` and the eviction tests
        // to fail on PDB recalculation timing.
        let final_current_replicas = statefulset_pods_after
            .iter()
            .filter(|p| is_created(p) && !is_terminating(p))
            .count() as i32;
        // readyReplicas = Running + Ready (non-terminating implied by Running phase)
        let final_ready_pods = statefulset_pods_after
            .iter()
            .filter(|p| is_ready(p))
            .count() as i32;
        // availableReplicas = pods that have been Ready for at least
        // `spec.minReadySeconds`. K8s computes this via IsPodAvailable
        // (pkg/api/v1/pod/util.go), which looks at the Ready condition's
        // `lastTransitionTime` and checks whether the elapsed wall-clock
        // duration meets the threshold. When `minReadySeconds` is zero (the
        // default) every ready pod is immediately available.
        let min_ready_seconds = statefulset.spec.min_ready_seconds.unwrap_or(0).max(0);
        let now = chrono::Utc::now();
        let is_available = |pod: &&Pod| -> bool {
            if !is_ready(pod) {
                return false;
            }
            if min_ready_seconds == 0 {
                return true;
            }
            let ready_since = pod
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .and_then(|conds| {
                    conds
                        .iter()
                        .find(|c| c.condition_type == "Ready" && c.status == "True")
                        .and_then(|c| c.last_transition_time)
                });
            match ready_since {
                Some(ts) => now.signed_duration_since(ts).num_seconds() >= min_ready_seconds as i64,
                // No transition timestamp means we can't prove the pod has met
                // the threshold yet — treat as not-available.
                None => false,
            }
        };
        let final_available_pods = statefulset_pods_after
            .iter()
            .filter(|p| is_available(p))
            .count() as i32;

        // Generate a revision hash from the current pod template spec
        let update_revision = Self::compute_revision(&statefulset.spec.template);

        // The current_revision is the revision that existing pods are running.
        // During a rolling update, this differs from update_revision.
        // Preserve the existing current_revision if set, otherwise derive from pods.
        let current_revision = statefulset
            .status
            .as_ref()
            .and_then(|s| s.current_revision.clone())
            .or_else(|| {
                // No current_revision in status — derive from actual pod labels
                statefulset_pods_after.iter().find_map(|pod| {
                    pod.metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("controller-revision-hash"))
                        .cloned()
                })
            })
            .unwrap_or_else(|| update_revision.clone());

        // K8s: currentReplicas/updatedReplicas only count isCreated && !isTerminating pods
        let updated_count = statefulset_pods_after
            .iter()
            .filter(|pod| {
                is_created(pod)
                    && !is_terminating(pod)
                    && pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("controller-revision-hash"))
                        .map(|h| h == &update_revision)
                        .unwrap_or(false)
            })
            .count() as i32;

        let current_rev_count = statefulset_pods_after
            .iter()
            .filter(|pod| {
                is_created(pod)
                    && !is_terminating(pod)
                    && pod
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("controller-revision-hash"))
                        .map(|h| h == &current_revision)
                        .unwrap_or(false)
            })
            .count() as i32;

        // Determine the final current_revision:
        // Only advance current_revision to update_revision when ALL pods have been
        // updated (updated_count >= desired_replicas). This ensures that during a
        // rolling update, currentRevision != updateRevision, which conformance tests verify.
        let final_current_revision = if updated_count >= desired_replicas {
            update_revision.clone()
        } else {
            current_revision.clone()
        };

        // Preserve existing conditions of unknown types — the StatefulSet controller
        // doesn't manage any condition types itself, so keep ALL existing conditions.
        // This prevents overwriting conditions set via PUT /status (e.g. "StatusUpdate"
        // condition from conformance tests).
        let existing_conditions = statefulset
            .status
            .as_ref()
            .and_then(|s| s.conditions.clone());

        // Update status with accurate counts
        // current_replicas = pods matching currentRevision (K8s semantics)
        // updated_replicas = pods matching updateRevision
        let new_status = Some(StatefulSetStatus {
            replicas: final_current_replicas,
            ready_replicas: Some(final_ready_pods),
            current_replicas: Some(if final_current_revision == update_revision {
                // All pods are on the same (current) revision
                final_current_replicas
            } else {
                // During rolling update: count pods on the old (current) revision
                current_rev_count
            }),
            updated_replicas: Some(updated_count),
            available_replicas: Some(final_available_pods),
            collision_count: None,
            observed_generation: statefulset.metadata.generation,
            current_revision: Some(final_current_revision),
            update_revision: Some(update_revision),
            conditions: existing_conditions,
        });

        // Only write status if it actually changed to avoid unnecessary storage writes
        // that trigger watch events and cause feedback loops
        if statefulset.status != new_status {
            statefulset.status = new_status;
            let key = format!("/registry/statefulsets/{}/{}", namespace, name);
            // Status subresource (a full-object PUT strips `.status` through
            // the api-server), conditional on the resourceVersion of the
            // StatefulSet this sync read: upstream `updateStatefulSetStatus`
            // (pkg/controller/statefulset/stateful_set_control.go) writes
            // through `statusUpdater.UpdateStatefulSetStatus` -> `UpdateStatus`
            // on the set it was handed. A Conflict propagates so the worker
            // requeues rate-limited (#2153).
            self.storage.update_status_cas(&key, statefulset).await?;
        }

        // Ensure a ControllerRevision exists for the current template revision
        let revision = Self::compute_revision(&statefulset.spec.template);
        let cr_name = format!(
            "{}-{}",
            name,
            &revision[..std::cmp::min(10, revision.len())]
        );
        let cr_key = format!("/registry/controllerrevisions/{}/{}", namespace, cr_name);
        if self
            .storage
            .get::<serde_json::Value>(&cr_key)
            .await
            .is_err()
        {
            // Create the ControllerRevision
            let template_data =
                serde_json::to_value(&statefulset.spec.template).unwrap_or_default();
            let cr = serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "ControllerRevision",
                "metadata": {
                    "name": cr_name,
                    "namespace": namespace,
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": chrono::Utc::now().to_rfc3339(),
                    "labels": {
                        "controller.kubernetes.io/hash": revision,
                        "app": name
                    },
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "StatefulSet",
                        "name": name,
                        "uid": statefulset.metadata.uid,
                        "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "data": template_data,
                "revision": 1
            });
            if let Err(e) = self.storage.create(&cr_key, &cr).await {
                debug!(
                    "ControllerRevision {} already exists or failed: {}",
                    cr_name, e
                );
            } else {
                info!(
                    "Created ControllerRevision {} for StatefulSet {}/{}",
                    cr_name, namespace, name
                );
            }
        }

        // GC ControllerRevisions beyond revisionHistoryLimit.
        // Mirrors upstream truncateHistory() in
        // pkg/controller/statefulset/stateful_set_control.go.
        // The limit caps the TOTAL number of retained revisions (including the
        // current one). Oldest revisions are deleted first; the current revision
        // (matching the live update_revision hash) is never deleted.
        if let Some(limit) = statefulset.spec.revision_history_limit {
            self.truncate_history(namespace, name, &statefulset.metadata.uid, &revision, limit)
                .await;
        }

        Ok(())
    }

    /// Whether `processCondemned` (stateful_set_control.go:508-535) refuses to
    /// delete this condemned pod right now. A terminating pod blocks only under
    /// `monotonic`; the Ready/first-unhealthy gates are monotonic-only too.
    /// `isRunningAndAvailable` (stateful_set_utils.go:461 ->
    /// `podutil.IsPodAvailable`): Ready, and Ready for at least
    /// `minReadySeconds`.
    fn is_pod_available(pod: &Pod, min_ready_seconds: i32) -> bool {
        let ready_since = pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|conds| {
                conds
                    .iter()
                    .find(|c| c.condition_type == "Ready" && c.status == "True")
            });
        let Some(cond) = ready_since else {
            return false;
        };
        if min_ready_seconds == 0 {
            return true;
        }
        match cond.last_transition_time {
            Some(ts) => {
                chrono::Utc::now().signed_duration_since(ts).num_seconds()
                    >= min_ready_seconds as i64
            }
            None => false,
        }
    }

    fn condemned_blocked(
        pod: &Pod,
        monotonic: bool,
        first_unhealthy_name: Option<&String>,
    ) -> bool {
        if pod.metadata.deletion_timestamp.is_some() {
            return monotonic;
        }
        if !monotonic {
            return false;
        }
        let is_ready = pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|conds| {
                conds
                    .iter()
                    .any(|c| c.condition_type == "Ready" && c.status == "True")
            })
            .unwrap_or(false);
        let is_running = matches!(
            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(Phase::Running)
        );
        // Can only delete if Ready+Running OR the firstUnhealthyPod.
        !(is_ready && is_running) && first_unhealthy_name != Some(&pod.metadata.name)
    }

    /// `DeleteStatefulPod` for one condemned pod
    /// (pkg/controller/statefulset/stateful_pod_control.go:97).
    ///
    /// The re-read is a safety net against a stale list (never re-delete a pod
    /// that is already terminating). It must NOT steer control flow.
    async fn delete_condemned_pod(
        &self,
        namespace: &str,
        pod: &Pod,
        blocked: bool,
        current_replicas: i32,
        desired_replicas: i32,
    ) -> Result<()> {
        if blocked {
            debug!(
                "StatefulSet pod {}/{}: scale-down blocked (terminating, or waiting on an unhealthy lower pod)",
                namespace, pod.metadata.name
            );
            return Ok(());
        }
        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
        match self.storage.get::<Pod>(&pod_key).await {
            Ok(pod_to_delete) => {
                if pod_to_delete.metadata.deletion_timestamp.is_none() {
                    // DELETE the pod; do NOT write deletionTimestamp (it is
                    // immutable on update; an api-server rejects us stamping it).
                    // The graceful variant keeps terminationGracePeriodSeconds.
                    self.storage.delete_gracefully(&pod_key).await?;
                    info!(
                        "Scale down: deleted pod {} ({} -> {})",
                        pod.metadata.name, current_replicas, desired_replicas
                    );
                }
            }
            Err(_) => {
                info!("Scale down: pod {} already gone", pod.metadata.name);
            }
        }
        Ok(())
    }

    /// Delete ControllerRevisions owned by this StatefulSet beyond `limit`.
    /// Oldest-first deletion order mirrors upstream `truncateHistory`. A revision
    /// is never deleted if it is the current one OR still referenced by a live
    /// pod (a pod mid-rollout below the partition runs an older revision —
    /// deleting its ControllerRevision would break partition rollouts and
    /// rollback). Ownership is matched by ownerReference uid (with name as a
    /// fallback for revisions written before a uid was assigned).
    async fn truncate_history(
        &self,
        namespace: &str,
        statefulset_name: &str,
        statefulset_uid: &str,
        current_revision_hash: &str,
        limit: i32,
    ) {
        if limit < 0 {
            return;
        }

        let owned_by_this = |refs: &[serde_json::Value]| {
            refs.iter().any(|r| {
                r.pointer("/kind").and_then(|v| v.as_str()) == Some("StatefulSet")
                    && (r.pointer("/uid").and_then(|v| v.as_str()) == Some(statefulset_uid)
                        || r.pointer("/name").and_then(|v| v.as_str()) == Some(statefulset_name))
            })
        };

        // Revision hashes still referenced by live pods owned by this
        // StatefulSet — these must never be GC'd. Mirrors upstream, which keeps
        // any revision a current pod is running.
        let pods_prefix = format!("/registry/pods/{}/", namespace);
        let live_pods: Vec<serde_json::Value> =
            self.storage.list(&pods_prefix).await.unwrap_or_default();
        let mut live_hashes: std::collections::HashSet<String> = std::collections::HashSet::new();
        live_hashes.insert(current_revision_hash.to_string());
        for pod in &live_pods {
            let owned = pod
                .pointer("/metadata/ownerReferences")
                .and_then(|v| v.as_array())
                .map(|refs| owned_by_this(refs))
                .unwrap_or(false);
            if !owned {
                continue;
            }
            if let Some(h) = pod
                .pointer("/metadata/labels/controller-revision-hash")
                .and_then(|v| v.as_str())
            {
                if !h.is_empty() {
                    live_hashes.insert(h.to_string());
                }
            }
        }

        let prefix = format!("/registry/controllerrevisions/{}/", namespace);
        let mut revisions: Vec<serde_json::Value> = match self.storage.list(&prefix).await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "ControllerRevision GC: failed to list revisions for {}/{}: {}",
                    namespace, statefulset_name, e
                );
                return;
            }
        };

        // Keep only revisions owned by this StatefulSet.
        revisions.retain(|cr| {
            cr.pointer("/metadata/ownerReferences")
                .and_then(|v| v.as_array())
                .map(|refs| owned_by_this(refs))
                .unwrap_or(false)
        });

        // Protected revisions — the current one and any still referenced by a
        // live pod (already collected in `live_hashes`) — are never deleted and
        // do NOT count against revisionHistoryLimit. The limit caps only the
        // NON-current history, matching upstream truncateHistory.
        let total = revisions.len() as i32;
        let protected = revisions
            .iter()
            .filter(|cr| {
                let h = cr
                    .pointer("/metadata/labels/controller.kubernetes.io~1hash")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                live_hashes.contains(h)
            })
            .count() as i32;
        if (total - protected) <= limit {
            return;
        }

        // Sort by creationTimestamp ascending (oldest first), tie-broken by name
        // so the deletion order is fully deterministic.
        revisions.sort_by(|a, b| {
            let ts_a = a
                .pointer("/metadata/creationTimestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let ts_b = b
                .pointer("/metadata/creationTimestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let name_a = a
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let name_b = b
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            ts_a.cmp(ts_b).then_with(|| name_a.cmp(name_b))
        });

        // Delete oldest revisions until we are at or below the limit, but never
        // delete the current revision or one still referenced by a live pod.
        let to_delete = ((total - protected) - limit) as usize;
        let mut deleted = 0;
        for cr in &revisions {
            if deleted >= to_delete {
                break;
            }
            let hash = cr
                .pointer("/metadata/labels/controller.kubernetes.io~1hash")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if live_hashes.contains(hash) {
                continue;
            }
            let cr_name = cr
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if cr_name.is_empty() {
                continue;
            }
            let key = format!("/registry/controllerrevisions/{}/{}", namespace, cr_name);
            match self.storage.delete(&key).await {
                Ok(_) => {
                    info!(
                        "ControllerRevision GC: deleted {} for StatefulSet {}/{}",
                        cr_name, namespace, statefulset_name
                    );
                    deleted += 1;
                }
                Err(rusternetes_common::Error::NotFound(_)) => {
                    deleted += 1; // already gone, count it
                }
                Err(e) => {
                    warn!(
                        "ControllerRevision GC: failed to delete {} for {}/{}: {}",
                        cr_name, namespace, statefulset_name, e
                    );
                }
            }
        }
    }

    /// Build the `persistentVolumeClaim` pod volumes for this StatefulSet's
    /// `volumeClaimTemplates` at the given ordinal. Upstream's `updateStorage`
    /// adds, for each template, a pod volume
    /// `{name, persistentVolumeClaim:{claimName: <template>-<set>-<ordinal>}}`.
    /// Without it the container's volumeMount has no backing volume in
    /// `pod.spec.volumes`, so the kubelet mounts nothing and the mountPath
    /// (e.g. /data) is absent in the container.
    fn volume_claim_template_volumes(
        statefulset: &StatefulSet,
        ordinal: i32,
    ) -> Vec<rusternetes_common::resources::Volume> {
        let Some(templates) = statefulset.spec.volume_claim_templates.as_ref() else {
            return Vec::new();
        };
        templates
            .iter()
            .filter_map(|t| {
                let claim_name = format!(
                    "{}-{}-{}",
                    t.metadata.name, statefulset.metadata.name, ordinal
                );
                serde_json::from_value(serde_json::json!({
                    "name": t.metadata.name,
                    "persistentVolumeClaim": {"claimName": claim_name},
                }))
                .ok()
            })
            .collect()
    }

    /// Return the name of the cluster's default StorageClass (the one annotated
    /// `storageclass.kubernetes.io/is-default-class=true`, or the beta variant),
    /// if any. Used to default `volumeClaimTemplate` PVCs that don't set a
    /// `storageClassName`, matching the api-server's DefaultStorageClass
    /// admission. `namespace` is accepted only for log context.
    async fn default_storage_class_name(&self, namespace: &str) -> Option<String> {
        let storage_classes: Vec<rusternetes_common::resources::StorageClass> = self
            .storage
            .list("/registry/storageclasses/")
            .await
            .unwrap_or_default();
        for sc in storage_classes {
            if let Some(annotations) = &sc.metadata.annotations {
                if annotations.get("storageclass.kubernetes.io/is-default-class")
                    == Some(&"true".to_string())
                    || annotations.get("storageclass.beta.kubernetes.io/is-default-class")
                        == Some(&"true".to_string())
                {
                    info!(
                        "Defaulting StatefulSet PVC in {} to storage class '{}'",
                        namespace, sc.metadata.name
                    );
                    return Some(sc.metadata.name.clone());
                }
            }
        }
        None
    }

    async fn ensure_pvcs_for_ordinal(
        &self,
        statefulset: &StatefulSet,
        ordinal: i32,
        namespace: &str,
    ) -> Result<()> {
        if let Some(ref templates) = statefulset.spec.volume_claim_templates {
            for template in templates {
                let pvc_name = format!(
                    "{}-{}-{}",
                    template.metadata.name, statefulset.metadata.name, ordinal
                );
                let key = build_key("persistentvolumeclaims", Some(namespace), &pvc_name);

                // Check if PVC already exists
                if self
                    .storage
                    .get::<PersistentVolumeClaim>(&key)
                    .await
                    .is_ok()
                {
                    continue; // PVC already exists
                }

                // Create PVC from template
                let mut pvc_metadata =
                    ObjectMeta::new(&pvc_name).with_namespace(namespace.to_string());

                // Copy labels and annotations from template metadata
                if let Some(ref tmpl_labels) = template.metadata.labels {
                    pvc_metadata.labels = Some(tmpl_labels.clone());
                }
                if let Some(ref tmpl_annotations) = template.metadata.annotations {
                    pvc_metadata.annotations = Some(tmpl_annotations.clone());
                }

                // Set owner reference to the StatefulSet
                pvc_metadata.owner_references = Some(vec![OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "StatefulSet".to_string(),
                    name: statefulset.metadata.name.clone(),
                    uid: statefulset.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]);

                // Apply DefaultStorageClass defaulting. The api-server runs this
                // admission on PVCs created via the REST handler, but the
                // StatefulSet controller writes volumeClaimTemplate PVCs straight
                // to storage — so without this the PVC keeps the template's unset
                // storageClassName, the dynamic provisioner never claims it, the
                // PVC never binds, and the pod starts with the volume unmounted
                // (StatefulSet pods then never become Ready). Mirrors
                // api-server `admission::set_default_storage_class`.
                let mut pvc_spec = template.spec.clone();
                if pvc_spec.storage_class_name.is_none() {
                    if let Some(default_sc) = self.default_storage_class_name(namespace).await {
                        pvc_spec.storage_class_name = Some(default_sc);
                    }
                }

                let pvc = PersistentVolumeClaim {
                    type_meta: TypeMeta {
                        kind: "PersistentVolumeClaim".to_string(),
                        api_version: "v1".to_string(),
                    },
                    metadata: pvc_metadata,
                    spec: pvc_spec,
                    status: None,
                };

                self.storage.create(&key, &pvc).await?;
                info!(
                    "Created PVC {} for StatefulSet {}/{}",
                    pvc_name, namespace, statefulset.metadata.name
                );
            }
        }
        Ok(())
    }

    /// Delete PVCs for ordinals at or above `desired_replicas` when the
    /// StatefulSet's `persistentVolumeClaimRetentionPolicy.whenScaled` is
    /// `Delete`. PVCs are matched via the `<template>-<statefulset>-<ordinal>`
    /// naming scheme used in `ensure_pvcs_for_ordinal`. Other policies
    /// (`Retain`, unset) leave the PVCs in place.
    async fn gc_scaled_down_pvcs(
        &self,
        statefulset: &StatefulSet,
        namespace: &str,
        desired_replicas: i32,
    ) -> Result<()> {
        let when_scaled = statefulset
            .spec
            .persistent_volume_claim_retention_policy
            .as_ref()
            .and_then(|p| p.when_scaled.as_deref());
        if when_scaled != Some("Delete") {
            return Ok(());
        }
        let Some(templates) = statefulset.spec.volume_claim_templates.as_ref() else {
            return Ok(());
        };
        if templates.is_empty() {
            return Ok(());
        }

        let ss_name = &statefulset.metadata.name;
        let ss_uid = &statefulset.metadata.uid;
        let pvc_prefix = format!("/registry/persistentvolumeclaims/{}/", namespace);
        let pvcs: Vec<PersistentVolumeClaim> =
            self.storage.list(&pvc_prefix).await.unwrap_or_default();

        for pvc in pvcs {
            // Match PVC names produced by ensure_pvcs_for_ordinal:
            // "<template>-<statefulset>-<ordinal>". The ordinal is the final
            // dash-separated segment; the StatefulSet name precedes it.
            let pvc_name = &pvc.metadata.name;
            let Some((prefix, ordinal_str)) = pvc_name.rsplit_once('-') else {
                continue;
            };
            let Ok(ordinal) = ordinal_str.parse::<i32>() else {
                continue;
            };
            let suffix = format!("-{}", ss_name);
            if !prefix.ends_with(&suffix) {
                continue;
            }
            let template_name = &prefix[..prefix.len() - suffix.len()];
            if !templates.iter().any(|t| t.metadata.name == template_name) {
                continue;
            }
            // Defensive: only delete PVCs we actually own. The ownerRef is set
            // in ensure_pvcs_for_ordinal — if it points at this StatefulSet's
            // UID we're clear to reclaim it. Skip otherwise to avoid stomping
            // on a PVC the user adopted manually.
            let owned = pvc
                .metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().any(|r| &r.uid == ss_uid))
                .unwrap_or(false);
            if !owned {
                continue;
            }
            if ordinal < desired_replicas {
                continue;
            }

            let key = build_key("persistentvolumeclaims", Some(namespace), pvc_name);
            match self.storage.delete(&key).await {
                Ok(_) => info!(
                    "PVC retention (whenScaled=Delete): deleted PVC {} for StatefulSet {}/{}",
                    pvc_name, namespace, ss_name
                ),
                Err(rusternetes_common::Error::NotFound(_)) => {}
                Err(e) => {
                    warn!(
                        "PVC retention: failed to delete PVC {} for {}/{}: {}",
                        pvc_name, namespace, ss_name, e
                    );
                }
            }
        }
        Ok(())
    }

    /// Compute a revision string from the pod template spec.
    /// This produces a deterministic hash that captures template changes.
    ///
    /// IMPORTANT: We must convert to serde_json::Value first before serializing
    /// to string. Direct `to_string` on the struct iterates HashMap fields in
    /// arbitrary order (HashMap has no guaranteed iteration order), producing
    /// non-deterministic output. Converting to Value first normalizes all maps
    /// into BTreeMap-backed serde_json::Map, which iterates in sorted key order.
    pub fn compute_revision(template: &rusternetes_common::resources::PodTemplateSpec) -> String {
        use sha2::{Digest, Sha256};
        // Convert to Value first to normalize HashMap ordering to sorted BTreeMap
        let value = serde_json::to_value(template).unwrap_or_default();
        let serialized = serde_json::to_string(&value).unwrap_or_default();
        let hash = Sha256::digest(serialized.as_bytes());
        let revision = format!(
            "{:010x}",
            u64::from_be_bytes(hash[..8].try_into().unwrap_or([0u8; 8]))
        );
        // Log the first container's image for debugging rolling updates
        let image = value
            .pointer("/spec/containers/0/image")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        debug!(
            "compute_revision: image={}, hash={}, json_len={}",
            image,
            revision,
            serialized.len()
        );
        revision
    }

    async fn create_pod(
        &self,
        statefulset: &StatefulSet,
        ordinal: i32,
        namespace: &str,
    ) -> Result<()> {
        let statefulset_name = &statefulset.metadata.name;
        let pod_name = format!("{}-{}", statefulset_name, ordinal);

        // Create pod from template
        let template = &statefulset.spec.template;
        let mut labels = template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("app".to_string(), statefulset_name.clone());
        labels.insert(
            "statefulset.kubernetes.io/pod-name".to_string(),
            pod_name.clone(),
        );
        // Set the controller-revision-hash label so tests can verify pod revision
        let revision = Self::compute_revision(&statefulset.spec.template);
        labels.insert("controller-revision-hash".to_string(), revision);

        let mut metadata = rusternetes_common::types::ObjectMeta::new(pod_name.clone())
            .with_namespace(namespace.to_string())
            .with_labels(labels)
            .with_owner_reference(OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "StatefulSet".to_string(),
                name: statefulset_name.clone(),
                uid: statefulset.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            });

        if let Some(template_meta) = &template.metadata {
            if let Some(ref annotations) = template_meta.annotations {
                metadata.annotations = Some(annotations.clone());
            }
        }

        // Stamp `pod.spec.subdomain` from `statefulset.spec.serviceName` and
        // `pod.spec.hostname` from the pod name so the headless governing
        // Service generates per-pod DNS A records under
        // `<pod>.<serviceName>.<ns>.svc.cluster.local`.
        // Mirrors upstream initIdentity() in
        // pkg/controller/statefulset/stateful_set_utils.go.
        let mut pod_spec = template.spec.clone();
        if !statefulset.spec.service_name.is_empty() {
            pod_spec.subdomain = Some(statefulset.spec.service_name.clone());
            pod_spec.hostname = Some(pod_name.clone());
        }

        // Inject a persistentVolumeClaim pod volume per volumeClaimTemplate so the
        // template's volumeMounts have a backing volume (mirrors updateStorage).
        let pvc_volumes = Self::volume_claim_template_volumes(statefulset, ordinal);
        if !pvc_volumes.is_empty() {
            let vols = pod_spec.volumes.get_or_insert_with(Vec::new);
            for v in pvc_volumes {
                if !vols.iter().any(|existing| existing.name == v.name) {
                    vols.push(v);
                }
            }
        }

        // Propagate the SA's imagePullSecrets (#1084) — controllers bypass the
        // api-server admission path that normally does this.
        super::propagate_sa_image_pull_secrets(&*self.storage, namespace, &mut pod_spec).await;

        // DefaultTolerationSeconds admission (#442): controllers bypass the
        // api-server admission path that adds these NoExecute tolerations.
        rusternetes_common::tolerations::add_default_tolerations(&mut pod_spec);

        let pod = Pod {
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
                pod_ip: None,
                host_ip: None,
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

        let key = format!("/registry/pods/{}/{}", namespace, pod_name);
        match self.storage.create(&key, &pod).await {
            Ok(_) => Ok(()),
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                debug!("Pod {} already exists, skipping creation", pod_name);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Create a pod with a specific template and revision (for pods below the partition
    /// that should use the old/current template, not the update template).
    async fn create_pod_with_template(
        &self,
        statefulset: &StatefulSet,
        ordinal: i32,
        namespace: &str,
        template: &rusternetes_common::resources::PodTemplateSpec,
        revision: &str,
    ) -> Result<()> {
        let statefulset_name = &statefulset.metadata.name;
        let pod_name = format!("{}-{}", statefulset_name, ordinal);

        let mut labels = template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("app".to_string(), statefulset_name.clone());
        labels.insert(
            "statefulset.kubernetes.io/pod-name".to_string(),
            pod_name.clone(),
        );
        labels.insert("controller-revision-hash".to_string(), revision.to_string());

        let mut metadata = rusternetes_common::types::ObjectMeta::new(pod_name.clone())
            .with_namespace(namespace.to_string())
            .with_labels(labels)
            .with_owner_reference(OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "StatefulSet".to_string(),
                name: statefulset_name.clone(),
                uid: statefulset.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            });

        if let Some(template_meta) = &template.metadata {
            if let Some(ref annotations) = template_meta.annotations {
                metadata.annotations = Some(annotations.clone());
            }
        }

        // Stamp `pod.spec.subdomain` from `statefulset.spec.serviceName` and
        // `pod.spec.hostname` from the pod name (mirrors initIdentity()).
        let mut pod_spec = template.spec.clone();
        if !statefulset.spec.service_name.is_empty() {
            pod_spec.subdomain = Some(statefulset.spec.service_name.clone());
            pod_spec.hostname = Some(pod_name.clone());
        }

        // Inject a persistentVolumeClaim pod volume per volumeClaimTemplate so the
        // template's volumeMounts have a backing volume (mirrors updateStorage).
        let pvc_volumes = Self::volume_claim_template_volumes(statefulset, ordinal);
        if !pvc_volumes.is_empty() {
            let vols = pod_spec.volumes.get_or_insert_with(Vec::new);
            for v in pvc_volumes {
                if !vols.iter().any(|existing| existing.name == v.name) {
                    vols.push(v);
                }
            }
        }

        // Propagate the SA's imagePullSecrets (#1084) — controllers bypass the
        // api-server admission path that normally does this.
        super::propagate_sa_image_pull_secrets(&*self.storage, namespace, &mut pod_spec).await;

        // DefaultTolerationSeconds admission (#442): controllers bypass the
        // api-server admission path that adds these NoExecute tolerations.
        rusternetes_common::tolerations::add_default_tolerations(&mut pod_spec);

        let pod = Pod {
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
                pod_ip: None,
                host_ip: None,
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

        let key = format!("/registry/pods/{}/{}", namespace, pod_name);
        match self.storage.create(&key, &pod).await {
            Ok(_) => Ok(()),
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                debug!("Pod {} already exists, skipping creation", pod_name);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Look up a ControllerRevision and extract the PodTemplateSpec from its data field.
    async fn get_template_from_revision(
        &self,
        namespace: &str,
        statefulset_name: &str,
        revision_hash: &str,
    ) -> Option<rusternetes_common::resources::PodTemplateSpec> {
        // ControllerRevision names follow the pattern: {ss-name}-{revision-hash-prefix}
        let cr_name = format!(
            "{}-{}",
            statefulset_name,
            &revision_hash[..std::cmp::min(10, revision_hash.len())]
        );
        let cr_key = format!("/registry/controllerrevisions/{}/{}", namespace, cr_name);
        if let Ok(cr) = self.storage.get::<serde_json::Value>(&cr_key).await {
            if let Some(data) = cr.get("data") {
                if let Ok(template) = serde_json::from_value::<
                    rusternetes_common::resources::PodTemplateSpec,
                >(data.clone())
                {
                    return Some(template);
                }
            }
        }
        None
    }
}

/// The StatefulSet half of the pod event handlers (stateful_set.go:202-310,
/// `resolveControllerRef` :408, `getStatefulSetsForPod` :387). StatefulSets
/// keep no expectations, so the creation/deletion hooks have nothing to lower.
impl<S: Storage + 'static> PodEventTarget for StatefulSetController<S> {
    fn queue_prefix(&self) -> &'static str {
        "statefulsets"
    }

    /// "We can't look up by UID, so look up by Name and then verify UID.
    /// Don't even try to look up by Name if it's the wrong Kind."
    async fn resolve_controller_ref(
        &self,
        namespace: &str,
        controller_ref: &ControllerRef,
    ) -> Option<ResolvedController> {
        if controller_ref.kind != "StatefulSet" {
            return None;
        }
        let set: StatefulSet = self
            .storage
            .get(&build_key(
                "statefulsets",
                Some(namespace),
                &controller_ref.name,
            ))
            .await
            .ok()?;
        (set.metadata.uid == controller_ref.uid).then(|| ResolvedController {
            name: set.metadata.name.clone(),
            min_ready_seconds: set.spec.min_ready_seconds.unwrap_or(0),
        })
    }

    /// `StatefulSetLister.GetPodStatefulSets` (client-go
    /// `listers/apps/v1/statefulset_expansion.go:39-80`): a pod without
    /// labels matches none; an invalid selector does not match; an empty
    /// selector matches nothing.
    async fn pod_controllers(
        &self,
        namespace: &str,
        pod_labels: &HashMap<String, String>,
    ) -> Vec<String> {
        if pod_labels.is_empty() {
            return Vec::new();
        }
        let Ok(sets) = self
            .storage
            .list::<StatefulSet>(&build_prefix("statefulsets", Some(namespace)))
            .await
        else {
            return Vec::new();
        };
        sets.into_iter()
            .filter(|set| {
                matches!(
                    label_selector_as_selector(Some(&set.spec.selector)),
                    Ok(selector @ Selector::Requirements(_)) if selector.matches(Some(pod_labels))
                )
            })
            .map(|set| set.metadata.name)
            .collect()
    }

    fn creation_observed(&self, _key: &str) {}

    fn deletion_observed(&self, _key: &str, _pod_key: &str) {}
}
