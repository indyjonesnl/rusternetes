//! Pod garbage collector.
//!
//! Ported from `pkg/controller/podgc/gc_controller.go` (release-1.35) and
//! launched like `newPodGarbageCollectorController`
//! (`cmd/kube-controller-manager/app/core.go:564-576`). Every `gcCheckPeriod`
//! (20s) one pass, in upstream's order (`gc`, :117-134):
//!
//!   1. `gcTerminated`: when `--terminated-pod-gc-threshold` is > 0, delete the
//!      oldest terminated (not Pending/Running/Unknown) pods beyond the
//!      threshold, evicted pods first;
//!   2. `gcTerminating`: force-delete terminating pods on a not-Ready node
//!      that carries the `node.kubernetes.io/out-of-service` taint;
//!   3. `gcOrphaned`: pods bound to a node that is gone. A node missing from the
//!      lister is first quarantined for `quarantineTime` (40s) in a delaying
//!      queue, then confirmed gone with a live GET before its pods are
//!      deleted;
//!   4. `gcUnscheduledTerminating`: terminating pods with no node.
//!
//! Every deletion goes through `markFailedAndDeletePod(WithCondition)`
//! (:342-366): move a non-terminal pod to `Failed` with a status patch (UID
//! pre-condition), then DELETE with `gracePeriodSeconds=0`.
//!
//! Rust-only differences: upstream reads pods/nodes from informer listers and
//! confirms a quarantined node with a client GET; here both come from storage,
//! so the lister snapshot (taken at the top of a pass) and the confirming GET
//! (taken later) are the lagging/live pair. The delaying queue is a map of
//! `node -> ready time` (`AddAfter` on a waiting item keeps the earlier time).
//! The status patch is a CAS-free `update_status` of the new status after a UID
//! check rather than a two-way strategic merge patch.

use crate::controllers::podgc_metrics as metrics;
use crate::controllers::taint_eviction::update_pod_condition;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusternetes_common::resources::{Node, Pod, PodCondition};
use rusternetes_common::types::Phase;
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info};

/// `gcCheckPeriod` (gc_controller.go:48): frequency of the main loop.
pub const GC_CHECK_PERIOD: Duration = Duration::from_secs(20);
/// `quarantineTime` (gc_controller.go:51): how long orphan GC waits for a node
/// to show up before issuing a GET to check it is truly gone.
pub const QUARANTINE_TIME: Duration = Duration::from_secs(40);
/// `RecommendedDefaultPodGCControllerConfiguration` default
/// (`pkg/controller/podgc/config/v1alpha1/defaults.go:34`).
pub const DEFAULT_TERMINATED_POD_GC_THRESHOLD: i32 = 12500;

/// `v1.TaintNodeOutOfService`.
pub const TAINT_NODE_OUT_OF_SERVICE: &str = "node.kubernetes.io/out-of-service";
/// `eviction.Reason`.
const EVICTION_REASON: &str = "Evicted";

/// `PodGCController` (gc_controller.go:54-67).
pub struct PodGcController<S: Storage> {
    pub storage: Arc<S>,
    pub terminated_pod_threshold: i32,
    pub gc_check_period: Duration,
    pub quarantine_time: ChronoDuration,
    /// `nodeQueue`: node name -> time it becomes ready for a live check.
    pub node_queue: Mutex<HashMap<String, DateTime<Utc>>>,
    /// Test seam standing in for the fake clientset's recorded actions.
    pub patched: Mutex<Vec<(String, rusternetes_common::resources::PodStatus)>>,
    pub deleted: Mutex<Vec<String>>,
}

impl<S: Storage + 'static> PodGcController<S> {
    /// `NewPodGC` (gc_controller.go:69-72).
    pub fn new(storage: Arc<S>, terminated_pod_threshold: i32) -> Self {
        Self::new_internal(
            storage,
            terminated_pod_threshold,
            GC_CHECK_PERIOD,
            QUARANTINE_TIME,
        )
    }

    /// `NewPodGCInternal` (gc_controller.go:75-92).
    pub fn new_internal(
        storage: Arc<S>,
        terminated_pod_threshold: i32,
        gc_check_period: Duration,
        quarantine_time: Duration,
    ) -> Self {
        Self {
            storage,
            terminated_pod_threshold,
            gc_check_period,
            quarantine_time: ChronoDuration::from_std(quarantine_time)
                .unwrap_or_else(|_| ChronoDuration::seconds(40)),
            node_queue: Mutex::new(HashMap::new()),
            patched: Mutex::new(Vec::new()),
            deleted: Mutex::new(Vec::new()),
        }
    }

    /// `Run` (gc_controller.go:94-115): `wait.UntilWithContext(ctx, gcc.gc,
    /// gcCheckPeriod)` runs a pass first, then sleeps.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting GC controller");
        loop {
            self.gc(Utc::now()).await;
            tokio::time::sleep(self.gc_check_period).await;
        }
    }

    /// `gc` (gc_controller.go:117-134). `now` stands in for the delaying
    /// queue's clock.
    pub async fn gc(&self, now: DateTime<Utc>) {
        let pods: Vec<Pod> = match self.storage.list(&build_prefix("pods", None)).await {
            Ok(p) => p,
            Err(e) => {
                error!("Error while listing all pods: {e}");
                return;
            }
        };
        let nodes: Vec<Node> = match self.storage.list(&build_prefix("nodes", None)).await {
            Ok(n) => n,
            Err(e) => {
                error!("Error while listing all nodes: {e}");
                return;
            }
        };
        self.gc_snapshot(&pods, &nodes, now).await;
    }

    /// The body of `gc` after the two lister reads.
    pub async fn gc_snapshot(&self, pods: &[Pod], nodes: &[Node], now: DateTime<Utc>) {
        if self.terminated_pod_threshold > 0 {
            self.gc_terminated(pods).await;
        }
        self.gc_terminating(pods, nodes).await;
        self.gc_orphaned(pods, nodes, now).await;
        self.gc_unscheduled_terminating(pods).await;
    }

    /// `gcTerminating` (gc_controller.go:148-191).
    pub async fn gc_terminating(&self, pods: &[Pod], nodes: &[Node]) {
        debug!("GC'ing terminating pods that are on out-of-service nodes");
        let mut terminating: Vec<&Pod> = Vec::new();
        for pod in pods {
            if is_pod_terminating(pod) {
                let node_name = pod_node_name(pod);
                let Some(node) = nodes.iter().find(|n| n.metadata.name == node_name) else {
                    error!("Failed to get node {node_name:?}: not found");
                    continue;
                };
                // 1. Node is not ready. 2. Node has the out-of-service taint.
                if !is_node_ready(node) && has_out_of_service_taint(node) {
                    debug!(
                        "Garbage collecting pod that is terminating: {}",
                        pod.metadata.name
                    );
                    terminating.push(pod);
                }
            }
        }
        if terminating.is_empty() {
            return;
        }
        debug!(
            "Garbage collecting {} pods that are terminating on node tainted with {}",
            terminating.len(),
            TAINT_NODE_OUT_OF_SERVICE
        );
        terminating.sort_by(|a, b| by_eviction_and_creation_timestamp(a, b));
        let reason = metrics::REASON_TERMINATING_OUT_OF_SERVICE;
        futures::future::join_all(terminating.into_iter().map(|pod| async move {
            let ns = pod_namespace(pod);
            metrics::inc_deleting_pods(ns, reason);
            if let Err(e) = self.mark_failed_and_delete_pod(pod).await {
                error!(
                    "Failed to delete terminating pod on out-of-service node {}: {e}",
                    pod.metadata.name
                );
                metrics::inc_deleting_pod_errors(ns, reason);
            }
        }))
        .await;
    }

    /// `gcTerminated` (gc_controller.go:193-226).
    pub async fn gc_terminated(&self, pods: &[Pod]) {
        let mut terminated: Vec<&Pod> = pods.iter().filter(|p| is_pod_terminated(p)).collect();
        let delete_count = terminated.len() as i64 - self.terminated_pod_threshold as i64;
        if delete_count <= 0 {
            return;
        }
        info!("Garbage collecting pods: numPods={delete_count}");
        terminated.sort_by(|a, b| by_eviction_and_creation_timestamp(a, b));
        let reason = metrics::REASON_TERMINATED;
        futures::future::join_all(terminated.into_iter().take(delete_count as usize).map(
            |pod| async move {
                let ns = pod_namespace(pod);
                if let Err(e) = self.mark_failed_and_delete_pod(pod).await {
                    error!("Failed to delete terminated pod {}: {e}", pod.metadata.name);
                    metrics::inc_deleting_pod_errors(ns, reason);
                }
                metrics::inc_deleting_pods(ns, reason);
            },
        ))
        .await;
    }

    /// `gcOrphaned` (gc_controller.go:228-268): delete pods bound to nodes that
    /// don't exist.
    pub async fn gc_orphaned(&self, pods: &[Pod], nodes: &[Node], now: DateTime<Utc>) {
        debug!("GC'ing orphaned");
        let existing: HashSet<&str> = nodes.iter().map(|n| n.metadata.name.as_str()).collect();
        // Add newly found unknown nodes to quarantine.
        {
            let mut q = self.node_queue.lock().unwrap();
            for pod in pods {
                let node_name = pod_node_name(pod);
                if !node_name.is_empty() && !existing.contains(node_name) {
                    // `AddAfter` on an item already waiting keeps the earlier
                    // ready time.
                    q.entry(node_name.to_string())
                        .or_insert(now + self.quarantine_time);
                }
            }
        }
        // Check if nodes are still missing after the quarantine period.
        let deleted_nodes = self.discover_deleted_nodes(&existing, now).await;
        // Delete orphaned pods.
        for pod in pods {
            if !deleted_nodes.contains(pod_node_name(pod)) {
                continue;
            }
            debug!(
                "Found orphaned Pod {} assigned to the Node {}, deleting",
                pod.metadata.name,
                pod_node_name(pod)
            );
            let condition = PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                reason: Some("DeletionByPodGC".to_string()),
                message: Some("PodGC: node no longer exists".to_string()),
                last_probe_time: None,
                last_transition_time: Some(Utc::now()),
                // `CalculatePodConditionObservedGeneration`: the pod's
                // generation (PodObservedGenerationTracking is GA, locked on).
                observed_generation: observed_generation(pod),
            };
            let ns = pod_namespace(pod);
            match self
                .mark_failed_and_delete_pod_with_condition(pod, Some(condition))
                .await
            {
                Err(e) => {
                    error!("Failed to delete orphaned pod {}: {e}", pod.metadata.name);
                    metrics::inc_deleting_pod_errors(ns, metrics::REASON_ORPHANED);
                }
                Ok(()) => info!(
                    "Forced deletion of orphaned Pod {} succeeded",
                    pod.metadata.name
                ),
            }
            metrics::inc_deleting_pods(ns, metrics::REASON_ORPHANED);
        }
    }

    /// `discoverDeletedNodes` (gc_controller.go:270-291): drain the queue items
    /// whose quarantine has elapsed (`for gcc.nodeQueue.Len() > 0` only sees
    /// ready items) and keep those a live GET confirms gone.
    async fn discover_deleted_nodes(
        &self,
        existing: &HashSet<&str>,
        now: DateTime<Utc>,
    ) -> HashSet<String> {
        let ready: Vec<String> = {
            let mut q = self.node_queue.lock().unwrap();
            let names: Vec<String> = q
                .iter()
                .filter(|(_, at)| **at <= now)
                .map(|(n, _)| n.clone())
                .collect();
            for n in &names {
                q.remove(n);
            }
            names
        };
        let mut deleted = HashSet::new();
        for node_name in ready {
            if !existing.contains(node_name.as_str()) {
                match self.check_if_node_exists(&node_name).await {
                    // Node will be added back to the queue in the subsequent
                    // loop if still needed.
                    Err(e) => error!("Error while getting node {node_name:?}: {e}"),
                    Ok(false) => {
                        deleted.insert(node_name);
                    }
                    Ok(true) => {}
                }
            }
        }
        deleted
    }

    /// `checkIfNodeExists` (gc_controller.go:293-299): a live GET; NotFound is
    /// `false`, any other error is an error.
    async fn check_if_node_exists(&self, name: &str) -> Result<bool> {
        match self
            .storage
            .get::<Node>(&build_key("nodes", None, name))
            .await
        {
            Ok(_) => Ok(true),
            Err(rusternetes_common::Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// `gcUnscheduledTerminating` (gc_controller.go:302-320).
    pub async fn gc_unscheduled_terminating(&self, pods: &[Pod]) {
        debug!("GC'ing unscheduled pods which are terminating");
        for pod in pods {
            if pod.metadata.deletion_timestamp.is_none() || !pod_node_name(pod).is_empty() {
                continue;
            }
            debug!(
                "Found unscheduled terminating Pod {} not assigned to any Node, deleting",
                pod.metadata.name
            );
            let ns = pod_namespace(pod);
            match self.mark_failed_and_delete_pod(pod).await {
                Err(e) => {
                    error!(
                        "Failed to delete unscheduled terminating pod {}: {e}",
                        pod.metadata.name
                    );
                    metrics::inc_deleting_pod_errors(ns, metrics::REASON_TERMINATING_UNSCHEDULED);
                }
                Ok(()) => info!(
                    "Forced deletion of unscheduled terminating Pod {} succeeded",
                    pod.metadata.name
                ),
            }
            metrics::inc_deleting_pods(ns, metrics::REASON_TERMINATING_UNSCHEDULED);
        }
    }

    /// `markFailedAndDeletePod` (gc_controller.go:342-344).
    pub async fn mark_failed_and_delete_pod(&self, pod: &Pod) -> Result<()> {
        self.mark_failed_and_delete_pod_with_condition(pod, None)
            .await
    }

    /// `markFailedAndDeletePodWithCondition` (gc_controller.go:346-366).
    async fn mark_failed_and_delete_pod_with_condition(
        &self,
        pod: &Pod,
        condition: Option<PodCondition>,
    ) -> Result<()> {
        info!("PodGC is force deleting Pod {}", pod.metadata.name);
        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
        let key = build_key("pods", Some(ns), &pod.metadata.name);
        // Patch the pod to make sure it is transitioned to the Failed phase
        // before deletion. Especially important when it is orphaned: no kubelet
        // is left to move the phase off Running.
        let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
        if !matches!(phase, Some(Phase::Succeeded) | Some(Phase::Failed)) {
            let mut new_status = pod.status.clone().unwrap_or_default();
            new_status.phase = Some(Phase::Failed);
            new_status.observed_generation = observed_generation(pod);
            if let Some(c) = condition {
                update_pod_condition(new_status.conditions.get_or_insert_with(Vec::new), c);
            }
            self.patch_pod_status(pod, &key, new_status).await?;
        }
        self.deleted.lock().unwrap().push(pod.metadata.name.clone());
        // `NewDeleteOptions(0)`: gracePeriodSeconds=0, i.e. remove now.
        self.storage.delete(&key).await?;
        Ok(())
    }

    /// `utilpod.PatchPodStatus` (pkg/util/pod/pod.go:34-48): the UID rides in
    /// the patch as a precondition, so a pod recreated under the same name is
    /// never touched.
    async fn patch_pod_status(
        &self,
        pod: &Pod,
        key: &str,
        new_status: rusternetes_common::resources::PodStatus,
    ) -> Result<()> {
        let mut current: Pod = self
            .storage
            .get(key)
            .await
            .map_err(|e| anyhow!("failed to patch status for pod {key:?}: {e}"))?;
        if current.metadata.uid != pod.metadata.uid {
            return Err(anyhow!(
                "failed to patch status for pod {key:?}: Precondition failed: UID in precondition: {}, UID in object meta: {}",
                pod.metadata.uid,
                current.metadata.uid
            ));
        }
        // `unchanged` short-circuit (pod.go:39-41).
        if serde_json::to_value(&pod.status)? == serde_json::to_value(Some(&new_status))? {
            return Ok(());
        }
        self.patched
            .lock()
            .unwrap()
            .push((pod.metadata.name.clone(), new_status.clone()));
        current.status = Some(new_status);
        self.storage
            .update_status(key, &current)
            .await
            .map_err(|e| anyhow!("failed to patch status for pod {key:?}: {e}"))?;
        Ok(())
    }
}

fn pod_node_name(pod: &Pod) -> &str {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("")
}

fn pod_namespace(pod: &Pod) -> &str {
    pod.metadata.namespace.as_deref().unwrap_or("")
}

/// `CalculatePodStatusObservedGeneration` with PodObservedGenerationTracking
/// GA (locked on in 1.35): the pod's generation, omitted when 0.
fn observed_generation(pod: &Pod) -> Option<i64> {
    pod.metadata.generation.filter(|g| *g != 0)
}

/// `isPodTerminated` (gc_controller.go:136-141): any phase other than
/// Pending/Running/Unknown, including an empty one.
fn is_pod_terminated(pod: &Pod) -> bool {
    !matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(Phase::Pending) | Some(Phase::Running) | Some(Phase::Unknown)
    )
}

/// `isPodTerminating` (gc_controller.go:144-146).
fn is_pod_terminating(pod: &Pod) -> bool {
    pod.metadata.deletion_timestamp.is_some()
}

/// `eviction.PodIsEvicted` (pkg/kubelet/eviction/helpers.go): phase Failed with
/// reason `Evicted`.
fn pod_is_evicted(pod: &Pod) -> bool {
    pod.status.as_ref().is_some_and(|s| {
        matches!(s.phase, Some(Phase::Failed)) && s.reason.as_deref() == Some(EVICTION_REASON)
    })
}

/// `nodeutil.IsNodeReady` (pkg/util/node): the NodeReady condition is True; no
/// such condition means not ready.
fn is_node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.condition_type == "Ready"))
        .is_some_and(|c| c.status == "True")
}

/// `taints.TaintKeyExists(node.Spec.Taints, v1.TaintNodeOutOfService)`: the
/// key only, whatever the value or effect.
fn has_out_of_service_taint(node: &Node) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .is_some_and(|t| t.iter().any(|t| t.key == TAINT_NODE_OUT_OF_SERVICE))
}

/// `byEvictionAndCreationTimestamp.Less` (gc_controller.go:330-340): evicted
/// pods first, then oldest creationTimestamp, name as the tie breaker. A zero
/// timestamp sorts before any real one.
fn by_eviction_and_creation_timestamp(a: &Pod, b: &Pod) -> std::cmp::Ordering {
    let (a_ev, b_ev) = (pod_is_evicted(a), pod_is_evicted(b));
    if a_ev != b_ev {
        // Evicted pod is smaller.
        return b_ev.cmp(&a_ev);
    }
    a.metadata
        .creation_timestamp
        .cmp(&b.metadata.creation_timestamp)
        .then_with(|| a.metadata.name.cmp(&b.metadata.name))
}
