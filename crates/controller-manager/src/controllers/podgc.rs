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
const TAINT_NODE_OUT_OF_SERVICE: &str = "node.kubernetes.io/out-of-service";
/// `eviction.Reason`.
const EVICTION_REASON: &str = "Evicted";

/// `PodGCController` (gc_controller.go:54-67).
pub struct PodGcController<S: Storage> {
    storage: Arc<S>,
    terminated_pod_threshold: i32,
    gc_check_period: Duration,
    quarantine_time: ChronoDuration,
    /// `nodeQueue`: node name -> time it becomes ready for a live check.
    node_queue: Mutex<HashMap<String, DateTime<Utc>>>,
    /// Test seam standing in for the fake clientset's recorded actions.
    #[cfg(test)]
    patched: Mutex<Vec<(String, rusternetes_common::resources::PodStatus)>>,
    #[cfg(test)]
    deleted: Mutex<Vec<String>>,
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
            #[cfg(test)]
            patched: Mutex::new(Vec::new()),
            #[cfg(test)]
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
    async fn mark_failed_and_delete_pod(&self, pod: &Pod) -> Result<()> {
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
        #[cfg(test)]
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
        #[cfg(test)]
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

#[cfg(test)]
mod tests {
    //! Cases ported from `gc_controller_test.go` (release-1.35): TestGCTerminated,
    //! TestGCOrphaned, TestGCUnscheduledTerminating, TestGCTerminating,
    //! TestGCInspectingPatchedPodBeforeDeletion. The fake clientset's recorded
    //! `patch`/`delete` actions are the controller's test seam; the metric
    //! assertions use one namespace per test because the counters are
    //! process-global and tests run in parallel (upstream resets them).
    use super::*;
    use chrono::TimeZone;
    use rusternetes_storage::MemoryStorage;
    use serde_json::json;

    fn phase_str(p: Option<&str>) -> serde_json::Value {
        match p {
            Some(p) => json!({ "phase": p }),
            None => json!({}),
        }
    }

    fn pod_json(
        ns: &str,
        name: &str,
        node: &str,
        phase: Option<&str>,
        created_hours: i64,
        deleting: bool,
    ) -> Pod {
        let created = Utc.timestamp_opt(created_hours * 3600, 0).unwrap();
        let mut v = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": name, "namespace": ns, "uid": format!("uid-{ns}-{name}"),
                "creationTimestamp": created.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            },
            "spec": { "containers": [], "nodeName": node },
            "status": phase_str(phase),
        });
        if deleting {
            v["metadata"]["deletionTimestamp"] = json!("1970-01-01T00:00:00Z");
        }
        serde_json::from_value(v).unwrap()
    }

    fn node_json(name: &str, ready: Option<&str>, taints: Vec<(&str, &str)>) -> Node {
        let mut v = json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": { "name": name, "uid": format!("uid-{name}") },
            "spec": { "taints": taints.iter().map(|(k, e)| json!({"key": k, "effect": e})).collect::<Vec<_>>() },
        });
        if let Some(r) = ready {
            v["status"] = json!({ "conditions": [{ "type": "Ready", "status": r }] });
        }
        serde_json::from_value(v).unwrap()
    }

    async fn seed(storage: &MemoryStorage, pods: &[Pod], nodes: &[Node]) {
        for p in pods {
            let key = build_key("pods", p.metadata.namespace.as_deref(), &p.metadata.name);
            storage.create(&key, p).await.unwrap();
        }
        for n in nodes {
            storage
                .create(&build_key("nodes", None, &n.metadata.name), n)
                .await
                .unwrap();
        }
    }

    fn names(v: &Mutex<Vec<String>>) -> Vec<String> {
        let mut n = v.lock().unwrap().clone();
        n.sort();
        n.dedup();
        n
    }

    fn patched_names<S: Storage>(gcc: &PodGcController<S>) -> Vec<String> {
        let mut n: Vec<String> = gcc
            .patched
            .lock()
            .unwrap()
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        n.sort();
        n
    }

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn check_metrics(ns: &str, deleted: usize, reason: &str) {
        assert_eq!(metrics::deleting_pods(ns, reason), deleted as u64, "total");
        assert_eq!(metrics::deleting_pod_errors(ns, reason), 0, "errors");
    }

    fn controller(storage: &Arc<MemoryStorage>, threshold: i32) -> PodGcController<MemoryStorage> {
        PodGcController::new(storage.clone(), threshold)
    }

    #[tokio::test]
    async fn gc_terminated() {
        // (name, phase, reason) per pod; created one hour apart in order.
        type Case = (
            &'static str,
            Vec<(&'static str, &'static str, &'static str)>,
            i32,
            Vec<&'static str>,
        );
        let cases: Vec<Case> = vec![
            (
                "delete pod a which is PodFailed and pod b which is PodSucceeded",
                vec![
                    ("a", "Failed", ""),
                    ("b", "Succeeded", ""),
                    ("c", "Failed", ""),
                ],
                1,
                vec!["a", "b"],
            ),
            (
                "threshold = 0, disables terminated pod deletion",
                vec![("a", "Failed", ""), ("b", "Succeeded", "")],
                0,
                vec![],
            ),
            (
                "threshold = 1, delete pod b which is PodSucceeded",
                vec![
                    ("a", "Running", ""),
                    ("b", "Succeeded", ""),
                    ("c", "Failed", ""),
                ],
                1,
                vec!["b"],
            ),
            (
                "threshold = 1, delete pod a which is PodFailed",
                vec![("a", "Failed", ""), ("b", "Succeeded", "")],
                1,
                vec!["a"],
            ),
            (
                "threshold = 5, don't delete pod",
                vec![("a", "Failed", ""), ("b", "Succeeded", "")],
                5,
                vec![],
            ),
            (
                "evicted pod goes first",
                vec![
                    ("a", "Failed", ""),
                    ("b", "Succeeded", ""),
                    ("c", "Failed", "Evicted"),
                ],
                1,
                vec!["a", "c"],
            ),
            (
                "evicted pod goes first, running pod kept",
                vec![
                    ("a", "Running", ""),
                    ("b", "Succeeded", ""),
                    ("c", "Failed", "Evicted"),
                ],
                1,
                vec!["c"],
            ),
        ];
        for (i, (name, pods, threshold, want_deleted)) in cases.into_iter().enumerate() {
            let ns = format!("terminated-{i}");
            let storage = Arc::new(MemoryStorage::new());
            let mut objs = Vec::new();
            for (h, (n, phase, reason)) in pods.iter().enumerate() {
                let mut p = pod_json(&ns, n, "node", Some(phase), h as i64 + 1, false);
                if !reason.is_empty() {
                    p.status.as_mut().unwrap().reason = Some(reason.to_string());
                }
                objs.push(p);
            }
            seed(&storage, &objs, &[node_json("node", Some("True"), vec![])]).await;
            let gcc = controller(&storage, threshold);
            gcc.gc(Utc::now()).await;
            assert_eq!(names(&gcc.deleted), strs(&want_deleted), "{name}");
            assert!(patched_names(&gcc).is_empty(), "{name}: patched");
            check_metrics(&ns, want_deleted.len(), metrics::REASON_TERMINATED);
            // The deleted pods are really gone from storage.
            let left: Vec<Pod> = storage
                .list(&build_prefix("pods", Some(&ns)))
                .await
                .unwrap();
            assert_eq!(left.len(), pods.len() - want_deleted.len(), "{name}");
        }
    }

    struct OrphanCase {
        name: &'static str,
        // Nodes in the (lagging) lister snapshot / live in storage at the 1st pass.
        lister_nodes: Vec<&'static str>,
        client_nodes: Vec<&'static str>,
        delay: ChronoDuration,
        added_client: Vec<&'static str>,
        deleted_client: Vec<&'static str>,
        added_lister: Vec<&'static str>,
        deleted_lister: Vec<&'static str>,
        pods: Vec<(&'static str, &'static str, &'static str)>,
        items_in_queue: usize,
        deleted: Vec<&'static str>,
        patched: Vec<&'static str>,
    }

    impl Default for OrphanCase {
        fn default() -> Self {
            Self {
                name: "",
                lister_nodes: vec![],
                client_nodes: vec![],
                delay: ChronoDuration::seconds(80),
                added_client: vec![],
                deleted_client: vec![],
                added_lister: vec![],
                deleted_lister: vec![],
                pods: vec![],
                items_in_queue: 0,
                deleted: vec![],
                patched: vec![],
            }
        }
    }

    #[tokio::test]
    async fn gc_orphaned() {
        let two_q = ChronoDuration::seconds(80);
        let cases = vec![
            OrphanCase {
                name: "nodes present in lister",
                lister_nodes: vec!["existing1", "existing2"],
                delay: two_q,
                pods: vec![
                    ("a", "existing1", "Running"),
                    ("b", "existing2", "Failed"),
                    ("c", "existing2", "Succeeded"),
                ],
                items_in_queue: 0,
                ..Default::default()
            },
            OrphanCase {
                name: "nodes present in client",
                client_nodes: vec!["existing1", "existing2"],
                delay: two_q,
                pods: vec![
                    ("a", "existing1", "Running"),
                    ("b", "existing2", "Failed"),
                    ("c", "existing2", "Succeeded"),
                ],
                items_in_queue: 2,
                ..Default::default()
            },
            OrphanCase {
                name: "no nodes",
                delay: two_q,
                pods: vec![("a", "deleted", "Failed"), ("b", "deleted", "Succeeded")],
                items_in_queue: 1,
                deleted: vec!["a", "b"],
                ..Default::default()
            },
            OrphanCase {
                name: "no nodes, one running pod",
                delay: two_q,
                pods: vec![
                    ("a", "deleted", "Failed"),
                    ("b", "deleted", "Succeeded"),
                    ("c", "deleted", "Running"),
                ],
                items_in_queue: 1,
                deleted: vec!["a", "b", "c"],
                patched: vec!["c"],
                ..Default::default()
            },
            OrphanCase {
                name: "quarantine not finished",
                delay: ChronoDuration::seconds(20),
                pods: vec![("a", "deleted", "Failed")],
                items_in_queue: 0,
                ..Default::default()
            },
            OrphanCase {
                name: "wrong nodes",
                lister_nodes: vec!["existing"],
                delay: two_q,
                pods: vec![("a", "deleted", "Running")],
                items_in_queue: 1,
                deleted: vec!["a"],
                patched: vec!["a"],
                ..Default::default()
            },
            OrphanCase {
                name: "some nodes missing",
                lister_nodes: vec!["existing"],
                delay: two_q,
                pods: vec![
                    ("a", "deleted", "Failed"),
                    ("b", "existing", "Failed"),
                    ("c", "deleted", "Succeeded"),
                    ("d", "deleted", "Running"),
                ],
                items_in_queue: 1,
                deleted: vec!["a", "c", "d"],
                patched: vec!["d"],
                ..Default::default()
            },
            OrphanCase {
                name: "node added to client after quarantine",
                delay: two_q,
                added_client: vec!["node"],
                pods: vec![("a", "node", "Running")],
                items_in_queue: 1,
                ..Default::default()
            },
            OrphanCase {
                name: "node added to informer after quarantine",
                delay: two_q,
                added_lister: vec!["node"],
                pods: vec![("a", "node", "Failed")],
                items_in_queue: 1,
                ..Default::default()
            },
            OrphanCase {
                // It shouldn't happen that the client lags behind the informer;
                // upstream keeps this as a sanity check.
                name: "node deleted from client after quarantine",
                client_nodes: vec!["node"],
                delay: two_q,
                deleted_client: vec!["node"],
                pods: vec![("a", "node", "Failed")],
                items_in_queue: 1,
                deleted: vec!["a"],
                ..Default::default()
            },
            OrphanCase {
                name: "node deleted from informer after quarantine",
                lister_nodes: vec!["node"],
                delay: two_q,
                deleted_lister: vec!["node"],
                pods: vec![("a", "node", "Succeeded")],
                items_in_queue: 0,
                ..Default::default()
            },
        ];
        for (i, c) in cases.into_iter().enumerate() {
            let ns = format!("orphaned-{i}");
            let storage = Arc::new(MemoryStorage::new());
            let pods: Vec<Pod> = c
                .pods
                .iter()
                .map(|(n, node, ph)| pod_json(&ns, n, node, Some(ph), 0, false))
                .collect();
            let client_nodes: Vec<Node> = c
                .client_nodes
                .iter()
                .map(|n| node_json(n, Some("True"), vec![]))
                .collect();
            seed(&storage, &pods, &client_nodes).await;
            let mut lister: Vec<Node> = c
                .lister_nodes
                .iter()
                .map(|n| node_json(n, Some("True"), vec![]))
                .collect();
            let gcc = controller(&storage, -1);
            let t0 = Utc.timestamp_opt(1_000_000, 0).unwrap();

            // First GC of orphaned pods.
            gcc.gc_snapshot(&pods, &lister, t0).await;
            assert!(
                gcc.deleted.lock().unwrap().is_empty(),
                "{}: no pods should be deleted at this point",
                c.name
            );

            // Move the clock forward; the quarantined items become ready.
            let t1 = t0 + c.delay;
            let ready = gcc
                .node_queue
                .lock()
                .unwrap()
                .values()
                .filter(|at| **at <= t1)
                .count();
            assert_eq!(ready, c.items_in_queue, "{}: items in node queue", c.name);

            // Execute planned node changes.
            for n in &c.added_client {
                let node = node_json(n, Some("True"), vec![]);
                storage
                    .create(&build_key("nodes", None, n), &node)
                    .await
                    .unwrap();
            }
            for n in &c.deleted_client {
                storage.delete(&build_key("nodes", None, n)).await.unwrap();
            }
            for n in &c.added_lister {
                lister.push(node_json(n, Some("True"), vec![]));
            }
            lister.retain(|n| !c.deleted_lister.contains(&n.metadata.name.as_str()));

            // Actual pod deletion.
            gcc.gc_snapshot(&pods, &lister, t1).await;
            assert_eq!(names(&gcc.deleted), strs(&c.deleted), "{}: deleted", c.name);
            assert_eq!(patched_names(&gcc), strs(&c.patched), "{}: patched", c.name);
            check_metrics(&ns, c.deleted.len(), metrics::REASON_ORPHANED);
        }
    }

    #[tokio::test]
    async fn gc_unscheduled_terminating() {
        // (name, phase, deleting, nodeName, expected)
        struct Case {
            name: &'static str,
            pods: Vec<(&'static str, &'static str, bool, &'static str)>,
            deleted: Vec<&'static str>,
            patched: Vec<&'static str>,
        }
        let cases = vec![
            Case {
                name: "Unscheduled pod in any phase must be deleted",
                pods: vec![
                    ("a", "Failed", true, ""),
                    ("b", "Succeeded", true, ""),
                    ("c", "Running", true, ""),
                ],
                deleted: vec!["a", "b", "c"],
                patched: vec!["c"],
            },
            Case {
                name: "Scheduled pod in any phase must not be deleted",
                pods: vec![
                    ("a", "Failed", false, ""),
                    ("b", "Succeeded", false, "node"),
                    ("c", "Running", true, "node"),
                ],
                deleted: vec![],
                patched: vec![],
            },
        ];
        for (i, c) in cases.into_iter().enumerate() {
            let ns = format!("unscheduled-{i}");
            let storage = Arc::new(MemoryStorage::new());
            let pods: Vec<Pod> = c
                .pods
                .iter()
                .enumerate()
                .map(|(h, (n, ph, del, node))| pod_json(&ns, n, node, Some(ph), h as i64 + 1, *del))
                .collect();
            seed(&storage, &pods, &[]).await;
            let gcc = controller(&storage, -1);
            gcc.gc_unscheduled_terminating(&pods).await;
            assert_eq!(names(&gcc.deleted), strs(&c.deleted), "{}", c.name);
            assert_eq!(patched_names(&gcc), strs(&c.patched), "{}", c.name);
            check_metrics(
                &ns,
                c.deleted.len(),
                metrics::REASON_TERMINATING_UNSCHEDULED,
            );
        }
    }

    #[tokio::test]
    async fn gc_terminating() {
        const NOEXEC: &str = "NoExecute";
        let oos = TAINT_NODE_OUT_OF_SERVICE;
        // Upstream's second case, verbatim (pod `a5` sits on worker-3).
        let nodes = vec![
            node_json("worker-0", Some("False"), vec![(oos, NOEXEC)]),
            node_json("worker-1", Some("False"), vec![]),
            node_json("worker-2", Some("True"), vec![]),
            node_json("worker-3", Some("False"), vec![(oos, "NoSchedule")]),
            node_json("worker-4", Some("False"), vec![(oos, "PreferNoSchedule")]),
            node_json("worker-5", Some("False"), vec![(oos, NOEXEC)]),
        ];
        struct Case {
            name: &'static str,
            nodes: Vec<Node>,
            // (name, phase, deleting, nodeName)
            pods: Vec<(String, Option<&'static str>, bool, String)>,
            deleted: Vec<String>,
            patched: Vec<String>,
        }
        let mut mixed_pods = Vec::new();
        for w in 0..6 {
            let node = format!("worker-{w}");
            let s = w + 1;
            mixed_pods.push((format!("a{s}"), None, false, node.clone()));
            mixed_pods.push((format!("b{s}"), None, true, node.clone()));
            mixed_pods.push((format!("c{s}"), Some("Pending"), false, node.clone()));
            mixed_pods.push((format!("d{s}"), Some("Running"), false, node.clone()));
            mixed_pods.push((format!("e{s}"), Some("Unknown"), false, node));
        }
        // `{name: "a5", nodeName: "worker-3"}`.
        mixed_pods[20].3 = "worker-3".to_string();
        let cases = vec![
            Case {
                name: "pods have deletion timestamp set and the corresponding nodes are not ready",
                nodes: vec![
                    node_json("worker-0", Some("False"), vec![]),
                    node_json("worker-1", Some("False"), vec![]),
                ],
                pods: vec![
                    ("a".into(), None, true, "worker-0".into()),
                    ("b".into(), None, true, "worker-1".into()),
                ],
                deleted: vec![],
                patched: vec![],
            },
            Case {
                name: "some pods have deletion timestamp and/or phase set and some of the corresponding nodes have an outofservice taint that are not ready",
                nodes,
                pods: mixed_pods,
                deleted: strs(&["b1", "b4", "b5", "b6"]),
                patched: strs(&["b1", "b4", "b5", "b6"]),
            },
            Case {
                name: "pods deleted from node tainted out-of-service",
                nodes: vec![node_json("worker", Some("False"), vec![(oos, NOEXEC)])],
                pods: vec![
                    ("a".into(), Some("Running"), true, "worker".into()),
                    ("b".into(), Some("Failed"), true, "worker".into()),
                    ("c".into(), Some("Succeeded"), true, "worker".into()),
                ],
                deleted: strs(&["a", "b", "c"]),
                patched: strs(&["a"]),
            },
        ];
        for (i, c) in cases.into_iter().enumerate() {
            let ns = format!("terminating-{i}");
            let storage = Arc::new(MemoryStorage::new());
            let pods: Vec<Pod> = c
                .pods
                .iter()
                .enumerate()
                .map(|(h, (n, ph, del, node))| pod_json(&ns, n, node, *ph, h as i64 + 1, *del))
                .collect();
            seed(&storage, &pods, &c.nodes).await;
            let gcc = controller(&storage, -1);
            gcc.gc(Utc::now()).await;
            assert_eq!(names(&gcc.deleted), c.deleted, "{}", c.name);
            assert_eq!(patched_names(&gcc), c.patched, "{}", c.name);
            check_metrics(
                &ns,
                c.deleted.len(),
                metrics::REASON_TERMINATING_OUT_OF_SERVICE,
            );
        }
    }

    #[tokio::test]
    async fn gc_inspecting_patched_pod_before_deletion() {
        // "orphaned pod should have DisruptionTarget condition added before deletion"
        let storage = Arc::new(MemoryStorage::new());
        let mut pod = pod_json(
            "default",
            "testPod",
            "deletedNode",
            Some("Running"),
            0,
            false,
        );
        pod.status.as_mut().unwrap().conditions = Some(vec![PodCondition {
            condition_type: "Ready".into(),
            status: "True".into(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        }]);
        seed(&storage, std::slice::from_ref(&pod), &[]).await;
        // `gcc.quarantineTime = time.Duration(-1)`.
        let gcc = PodGcController::new_internal(
            storage.clone(),
            -1,
            GC_CHECK_PERIOD,
            Duration::from_secs(0),
        );
        let now = Utc::now();
        // A zero quarantine is ready within the same pass (AddAfter(-1) upstream).
        gcc.gc_snapshot(std::slice::from_ref(&pod), &[], now).await;

        let patched = gcc.patched.lock().unwrap().clone();
        assert_eq!(patched.len(), 1, "exactly one status patch");
        let (name, status) = &patched[0];
        assert_eq!(name, "testPod");
        assert_eq!(status.phase, Some(Phase::Failed));
        let conds = status.conditions.as_ref().unwrap();
        assert_eq!(conds.len(), 2);
        assert_eq!(conds[0].condition_type, "Ready");
        assert_eq!(conds[0].status, "True");
        assert_eq!(conds[1].condition_type, "DisruptionTarget");
        assert_eq!(conds[1].status, "True");
        assert_eq!(conds[1].reason.as_deref(), Some("DeletionByPodGC"));
        assert_eq!(
            conds[1].message.as_deref(),
            Some("PodGC: node no longer exists")
        );
        assert_eq!(names(&gcc.deleted), strs(&["testPod"]));
        assert!(storage
            .get::<Pod>(&build_key("pods", Some("default"), "testPod"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn patch_precondition_uid_mismatch_is_not_deleted() {
        // utilpod.PatchPodStatus carries the UID as a precondition: a pod that was
        // recreated under the same name must survive.
        let storage = Arc::new(MemoryStorage::new());
        let old = pod_json("uid-ns", "p", "gone", Some("Running"), 1, false);
        let mut new = old.clone();
        new.metadata.uid = "recreated".into();
        seed(&storage, &[new], &[]).await;
        let gcc = controller(&storage, -1);
        let err = gcc.mark_failed_and_delete_pod(&old).await.unwrap_err();
        assert!(err.to_string().contains("Precondition failed"), "{err}");
        assert!(storage
            .get::<Pod>(&build_key("pods", Some("uid-ns"), "p"))
            .await
            .is_ok());
    }
}
