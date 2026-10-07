//! NoExecute taint manager.
//!
//! Ported from `pkg/controller/tainteviction/taint_eviction.go` (kubernetes
//! release-1.35), together with `timed_workers.go` (see
//! `taint_eviction_timed_workers.rs`).
//!
//! Mechanism, as upstream:
//!   * node and pod changes go into two dedup'ing work queues
//!     (`nodeUpdateQueue`/`podUpdateQueue`, :103-104);
//!   * two dispatchers move items from those queues into `UpdateWorkerSize = 8`
//!     per-worker channels, sharded by `hash(nodeName, UpdateWorkerSize)`
//!     (`Run`, :305-347). Pods are sharded by the node they sit on, so the
//!     worker that sets `taintedNodes` for a node is the same one that reads it
//!     for that node's pods (:334-337);
//!   * each worker prefers node updates over pod updates (`worker`, :357-386);
//!   * evictions are deferred through the `TimedWorkerQueue` and cancelled when
//!     the taint goes away, the pod is deleted or its tolerations change
//!     (`processPodOnNode`, :451-490).
//!
//! Rust-only difference: upstream reads pods/nodes from informer listers and a
//! `spec.nodeName` pod indexer; this controller reads them from storage (a
//! list-and-filter for `getPodsAssignedToNode`) and feeds its own
//! `PodUpdated`/`NodeUpdated` from a watch plus a periodic relist diff, which
//! plays the informer's part.

use crate::controllers::taint_eviction_timed_workers::{TimedWorkerQueue, WorkArgs};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::{
    EventSource, EventType, Node, Pod, PodCondition, Taint, Toleration,
};
use rusternetes_storage::{build_key, build_prefix, EventRecorder, Storage, WatchEvent, WorkQueue};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// `NodeUpdateChannelSize` (taint_eviction.go:55).
pub const NODE_UPDATE_CHANNEL_SIZE: usize = 10;
/// `UpdateWorkerSize` (taint_eviction.go:57).
pub const UPDATE_WORKER_SIZE: usize = 8;
/// `podUpdateChannelSize` (taint_eviction.go:58).
const POD_UPDATE_CHANNEL_SIZE: usize = 1;
/// `retries` (taint_eviction.go:59).
const RETRIES: usize = 5;
/// Component name upstream passes as `controllerName` (cmd/kube-controller-manager
/// names this controller `taint-eviction-controller`).
const CONTROLLER_NAME: &str = "taint-eviction-controller";
/// Informer-style resync, kept from the previous implementation.
const RESYNC_PERIOD: Duration = Duration::from_secs(30);

/// `hash` (taint_eviction.go:72): FNV-1a 32 of `val`, modulo `max`.
pub fn hash(val: &str, max: usize) -> usize {
    let mut h: u32 = 0x811c_9dc5;
    for b in val.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    (h % max as u32) as usize
}

/// `getMinTolerationTime` result (taint_eviction.go:160): Go returns a
/// negative duration for "infinite".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinTolerationTime {
    /// `-1`: every used toleration lacks `tolerationSeconds`.
    Forever,
    /// `0` or the smallest `tolerationSeconds`.
    After(Duration),
}

/// `getMinTolerationTime` (taint_eviction.go:161-182).
pub fn get_min_toleration_time(tolerations: &[Toleration]) -> MinTolerationTime {
    let mut min_toleration_time = i64::MAX;
    if tolerations.is_empty() {
        return MinTolerationTime::After(Duration::ZERO);
    }
    for t in tolerations {
        if let Some(seconds) = t.toleration_seconds {
            if seconds <= 0 {
                return MinTolerationTime::After(Duration::ZERO);
            } else if seconds < min_toleration_time {
                min_toleration_time = seconds;
            }
        }
    }
    if min_toleration_time == i64::MAX {
        return MinTolerationTime::Forever;
    }
    MinTolerationTime::After(Duration::from_secs(min_toleration_time as u64))
}

/// `getNoExecuteTaints` (taint_eviction.go:150).
pub fn get_no_execute_taints(taints: &[Taint]) -> Vec<Taint> {
    taints
        .iter()
        .filter(|t| t.effect == "NoExecute")
        .cloned()
        .collect()
}

/// `Toleration.ToleratesTaint` (staging/src/k8s.io/api/core/v1/toleration.go:49)
/// with `enableComparisonOperators = false`: `TaintTolerationComparisonOperators`
/// is Alpha, default off in 1.35 (pkg/features/kube_features.go:1859), so
/// `Lt`/`Gt` tolerations never match.
fn toleration_tolerates_taint(t: &Toleration, taint: &Taint) -> bool {
    if t.effect.as_deref().is_some_and(|e| !e.is_empty())
        && t.effect.as_deref() != Some(&taint.effect)
    {
        return false;
    }
    if t.key.as_deref().is_some_and(|k| !k.is_empty()) && t.key.as_deref() != Some(&taint.key) {
        return false;
    }
    match t.operator.as_deref() {
        None | Some("") | Some("Equal") => {
            t.value.as_deref().unwrap_or("") == taint.value.as_deref().unwrap_or("")
        }
        Some("Exists") => true,
        _ => false,
    }
}

/// `GetMatchingTolerations` (pkg/apis/core/v1/helper/helpers.go:293): whether
/// every taint is tolerated, and the tolerations that matched.
pub fn get_matching_tolerations(
    taints: &[Taint],
    tolerations: &[Toleration],
) -> (bool, Vec<Toleration>) {
    if taints.is_empty() {
        return (true, Vec::new());
    }
    if tolerations.is_empty() {
        return (false, Vec::new());
    }
    let mut result = Vec::new();
    for taint in taints {
        match tolerations
            .iter()
            .find(|t| toleration_tolerates_taint(t, taint))
        {
            Some(t) => result.push(t.clone()),
            None => return (false, Vec::new()),
        }
    }
    (true, result)
}

/// `v1helper.UpdatePodCondition` (pkg/api/v1/pod/util.go): returns whether the
/// conditions changed.
fn update_pod_condition(conditions: &mut Vec<PodCondition>, new: PodCondition) -> bool {
    match conditions
        .iter_mut()
        .find(|c| c.condition_type == new.condition_type)
    {
        None => {
            conditions.push(new);
            true
        }
        Some(old) => {
            let status_changed = old.status != new.status;
            let changed = status_changed
                || old.reason != new.reason
                || old.message != new.message
                || old.observed_generation != new.observed_generation;
            if !changed {
                return false;
            }
            if status_changed {
                old.last_transition_time = new.last_transition_time;
            }
            old.status = new.status;
            old.reason = new.reason;
            old.message = new.message;
            old.last_probe_time = new.last_probe_time;
            old.observed_generation = new.observed_generation;
            true
        }
    }
}

/// `nodeUpdateItem` / `podUpdateItem` (taint_eviction.go:62-70), encoded as the
/// string keys our `WorkQueue` dedups on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PodUpdateItem {
    pod_name: String,
    pod_namespace: String,
    node_name: String,
}

impl PodUpdateItem {
    fn encode(&self) -> String {
        // Node names, namespaces and pod names cannot contain '/'.
        format!(
            "{}/{}/{}",
            self.node_name, self.pod_namespace, self.pod_name
        )
    }
    fn decode(key: &str) -> Option<Self> {
        let mut it = key.splitn(3, '/');
        Some(Self {
            node_name: it.next()?.to_string(),
            pod_namespace: it.next()?.to_string(),
            pod_name: it.next()?.to_string(),
        })
    }
}

/// What `PodUpdated` compares between old and new (taint_eviction.go:408):
/// tolerations and node name.
#[derive(Debug, Clone, PartialEq)]
struct PodSnapshot {
    namespace: String,
    name: String,
    node_name: String,
    tolerations: Vec<Toleration>,
}

impl PodSnapshot {
    fn of(pod: &Pod) -> Self {
        Self {
            namespace: pod.metadata.namespace.clone().unwrap_or_default(),
            name: pod.metadata.name.clone(),
            node_name: pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.clone())
                .unwrap_or_default(),
            tolerations: pod
                .spec
                .as_ref()
                .and_then(|s| s.tolerations.clone())
                .unwrap_or_default(),
        }
    }
}

type TaintKey = (String, Option<String>, String, Option<DateTime<Utc>>);

/// The NoExecute taints of a node in a `DeepEqual`-able form (`Taint` itself is
/// not `PartialEq`).
fn taint_keys(taints: &[Taint]) -> Vec<TaintKey> {
    taints
        .iter()
        .map(|t| {
            (
                t.key.clone(),
                t.value.clone(),
                t.effect.clone(),
                t.time_added,
            )
        })
        .collect()
}

fn node_no_execute_taints(node: &Node) -> Vec<Taint> {
    get_no_execute_taints(
        node.spec
            .as_ref()
            .and_then(|s| s.taints.as_deref())
            .unwrap_or(&[]),
    )
}

/// `Controller` (taint_eviction.go:83).
pub struct TaintEvictionController<S: Storage> {
    storage: Arc<S>,
    recorder: EventRecorder<S>,
    /// `taintEvictionQueue`.
    taint_eviction_queue: TimedWorkerQueue,
    /// `taintedNodes`: node name -> its NoExecute taints.
    pub(crate) tainted_nodes: Mutex<HashMap<String, Vec<Taint>>>,
    /// `nodeUpdateQueue` / `podUpdateQueue`.
    node_update_queue: WorkQueue,
    pod_update_queue: WorkQueue,
    /// Last-seen state, standing in for the informer's old object when
    /// computing `UpdateFunc(prev, obj)`.
    node_cache: Mutex<HashMap<String, Vec<TaintKey>>>,
    pod_cache: Mutex<HashMap<String, PodSnapshot>>,
}

impl<S: Storage + 'static> TaintEvictionController<S> {
    /// `New` (taint_eviction.go:185).
    pub fn new(storage: Arc<S>) -> Self {
        let recorder = EventRecorder::new(Arc::clone(&storage));
        let work_storage = Arc::clone(&storage);
        let work_recorder = recorder.clone();
        let taint_eviction_queue = TimedWorkerQueue::new(Arc::new(move |fire_at, args| {
            let storage = Arc::clone(&work_storage);
            let recorder = work_recorder.clone();
            Box::pin(delete_pod_handler(storage, recorder, fire_at, args))
        }));
        Self {
            storage,
            recorder,
            taint_eviction_queue,
            tainted_nodes: Mutex::new(HashMap::new()),
            node_update_queue: WorkQueue::new(),
            pod_update_queue: WorkQueue::new(),
            node_cache: Mutex::new(HashMap::new()),
            pod_cache: Mutex::new(HashMap::new()),
        }
    }

    /// `PodUpdated` (taint_eviction.go:389).
    #[allow(dead_code)] // upstream API; the bin's watch path goes through observe_*
    pub async fn pod_updated(&self, old_pod: Option<&Pod>, new_pod: Option<&Pod>) {
        self.snapshot_updated(
            old_pod.map(PodSnapshot::of).as_ref(),
            new_pod.map(PodSnapshot::of).as_ref(),
        )
        .await;
    }

    async fn snapshot_updated(&self, old: Option<&PodSnapshot>, new: Option<&PodSnapshot>) {
        // The new object wins for the identity fields, as in Go.
        let Some(basis) = new.or(old) else { return };
        if let (Some(o), Some(n)) = (old, new) {
            if o.tolerations == n.tolerations && o.node_name == n.node_name {
                return;
            }
        }
        let item = PodUpdateItem {
            pod_name: basis.name.clone(),
            pod_namespace: basis.namespace.clone(),
            node_name: basis.node_name.clone(),
        };
        self.pod_update_queue.add(item.encode()).await;
    }

    /// `NodeUpdated` (taint_eviction.go:421).
    #[allow(dead_code)] // upstream API; the bin's watch path goes through observe_*
    pub async fn node_updated(&self, old_node: Option<&Node>, new_node: Option<&Node>) {
        let old = old_node.map(|n| taint_keys(&node_no_execute_taints(n)));
        self.taints_updated(
            old_node.or(new_node).map(|n| n.metadata.name.as_str()),
            old.as_deref(),
            new_node.map(|n| taint_keys(&node_no_execute_taints(n))),
        )
        .await;
    }

    async fn taints_updated(
        &self,
        node_name: Option<&str>,
        old: Option<&[TaintKey]>,
        new: Option<Vec<TaintKey>>,
    ) {
        let Some(node_name) = node_name else { return };
        if let (Some(o), Some(n)) = (old, new.as_ref()) {
            if o == n.as_slice() {
                return;
            }
        }
        self.node_update_queue.add(node_name.to_string()).await;
    }

    /// `cancelWorkWithEvent` (taint_eviction.go:445).
    async fn cancel_work_with_event(&self, namespace: &str, name: &str) {
        let key = WorkArgs::new(name, namespace).key();
        if self.taint_eviction_queue.cancel_work(&key) {
            self.emit_event(
                namespace,
                name,
                &format!("Cancelling deletion of Pod {key}"),
            )
            .await;
        }
    }

    async fn emit_event(&self, namespace: &str, name: &str, message: &str) {
        emit_pod_event(&self.recorder, namespace, name, message).await;
    }

    /// `processPodOnNode` (taint_eviction.go:451).
    async fn process_pod_on_node(
        &self,
        namespace: &str,
        name: &str,
        node_name: &str,
        tolerations: &[Toleration],
        taints: &[Taint],
        now: DateTime<Utc>,
    ) {
        if taints.is_empty() {
            self.cancel_work_with_event(namespace, name).await;
            // Deviation: upstream has no `return` here (:460-462) and falls
            // through into `AddWork(now, now)`. Both of its callers
            // short-circuit on empty taints first (:527, :575), so the path is
            // unreachable there; returning keeps it unreachable for us too.
            return;
        }
        let (all_tolerated, used_tolerations) = get_matching_tolerations(taints, tolerations);
        if !all_tolerated {
            debug!(
                "Not all taints are tolerated after update for pod {namespace}/{name} on node {node_name}"
            );
            // We're canceling scheduled work (if any), as we're going to
            // delete the Pod right away.
            self.cancel_work_with_event(namespace, name).await;
            self.taint_eviction_queue
                .add_work(WorkArgs::new(name, namespace), now, now);
            return;
        }
        let min_toleration_time = match get_min_toleration_time(&used_tolerations) {
            // getMinTolerationTime returns negative value to denote infinite
            // toleration.
            MinTolerationTime::Forever => {
                debug!(
                    "Current tolerations for pod {namespace}/{name} tolerate forever, cancelling any scheduled deletion"
                );
                self.cancel_work_with_event(namespace, name).await;
                return;
            }
            MinTolerationTime::After(d) => chrono::Duration::from_std(d).unwrap_or_default(),
        };

        let mut start_time = now;
        let trigger_time = start_time + min_toleration_time;
        let key = WorkArgs::new(name, namespace).key();
        if let Some(scheduled) = self.taint_eviction_queue.get_worker_unsafe(&key) {
            start_time = scheduled.created_at;
            if start_time + min_toleration_time < trigger_time {
                return;
            }
            self.cancel_work_with_event(namespace, name).await;
        }
        self.taint_eviction_queue.add_work(
            WorkArgs::new(name, namespace),
            start_time,
            trigger_time,
        );
    }

    /// `handlePodUpdate` (taint_eviction.go:492).
    async fn handle_pod_update(&self, update: &PodUpdateItem) {
        let key = build_key("pods", Some(&update.pod_namespace), &update.pod_name);
        let pod = match self.storage.get::<Pod>(&key).await {
            Ok(pod) => pod,
            Err(rusternetes_common::Error::NotFound(_)) => {
                // Delete
                debug!(
                    "Noticed pod deletion {}/{}",
                    update.pod_namespace, update.pod_name
                );
                self.cancel_work_with_event(&update.pod_namespace, &update.pod_name)
                    .await;
                return;
            }
            Err(e) => {
                error!(
                    "could not get pod {}/{}: {e}",
                    update.pod_name, update.pod_namespace
                );
                return;
            }
        };
        let snap = PodSnapshot::of(&pod);
        // We key the workqueue and shard workers by nodeName. If we don't
        // match the current state we should not be the one processing the
        // current object.
        if snap.node_name != update.node_name {
            return;
        }
        // Create or Update
        if snap.node_name.is_empty() {
            return;
        }
        let taints = self
            .tainted_nodes
            .lock()
            .unwrap()
            .get(&snap.node_name)
            .cloned();
        // It's possible that Node was deleted, or Taints were removed before,
        // which triggered eviction cancelling if it was needed.
        let Some(taints) = taints else { return };
        self.process_pod_on_node(
            &snap.namespace,
            &snap.name,
            &snap.node_name,
            &snap.tolerations,
            &taints,
            Utc::now(),
        )
        .await;
    }

    /// `handleNodeUpdate` (taint_eviction.go:533).
    async fn handle_node_update(&self, node_name: &str) {
        let node = match self
            .storage
            .get::<Node>(&build_key("nodes", None, node_name))
            .await
        {
            Ok(node) => node,
            Err(rusternetes_common::Error::NotFound(_)) => {
                // Delete
                debug!("Noticed node deletion {node_name}");
                self.tainted_nodes.lock().unwrap().remove(node_name);
                return;
            }
            Err(e) => {
                error!("cannot get node {node_name}: {e}");
                return;
            }
        };

        // Create or Update
        let taints = node_no_execute_taints(&node);
        {
            let mut tainted = self.tainted_nodes.lock().unwrap();
            if taints.is_empty() {
                tainted.remove(node_name);
            } else {
                tainted.insert(node_name.to_string(), taints.clone());
            }
        }

        // This is critical that we update tainted_nodes before we call
        // getPodsAssignedToNode: it can be delayed as long as all future
        // updates to pods will call PodUpdated which will use tainted_nodes to
        // potentially delete delayed pods (:563-565).
        let pods = match self.get_pods_assigned_to_node(node_name).await {
            Ok(pods) => pods,
            Err(e) => {
                error!("Failed to get pods assigned to node {node_name}: {e}");
                return;
            }
        };
        if pods.is_empty() {
            return;
        }
        // Short circuit, to make this controller a bit faster.
        if taints.is_empty() {
            debug!("All taints were removed from node {node_name}. Cancelling all evictions...");
            for pod in &pods {
                self.cancel_work_with_event(&pod.namespace, &pod.name).await;
            }
            return;
        }

        let now = Utc::now();
        for pod in &pods {
            self.process_pod_on_node(
                &pod.namespace,
                &pod.name,
                node_name,
                &pod.tolerations,
                &taints,
                now,
            )
            .await;
        }
    }

    /// `getPodsAssignedToNode` (taint_eviction.go:203, the `spec.nodeName`
    /// index), as a list-and-filter.
    async fn get_pods_assigned_to_node(&self, node_name: &str) -> Result<Vec<PodSnapshot>> {
        let pods = self
            .storage
            .list::<Pod>(&build_prefix("pods", None))
            .await?;
        Ok(pods
            .iter()
            .map(PodSnapshot::of)
            .filter(|p| p.node_name == node_name)
            .collect())
    }

    /// One worker of the `UpdateWorkerSize` pool (`worker`, taint_eviction.go:357).
    async fn worker(
        &self,
        mut node_rx: mpsc::Receiver<String>,
        mut pod_rx: mpsc::Receiver<String>,
    ) {
        // When processing events we want to prioritize Node updates over Pod
        // updates, as NodeUpdates that interest the controller should be
        // handled as soon as possible - we don't want user (or system) to wait
        // until PodUpdate queue is drained before it can start evicting Pods
        // from tainted Nodes.
        loop {
            tokio::select! {
                biased;
                Some(node) = node_rx.recv() => {
                    self.handle_node_update(&node).await;
                    self.node_update_queue.done(&node).await;
                }
                Some(key) = pod_rx.recv() => {
                    // If we found a Pod update we need to empty Node queue first.
                    while let Ok(node) = node_rx.try_recv() {
                        self.handle_node_update(&node).await;
                        self.node_update_queue.done(&node).await;
                    }
                    // After Node queue is emptied we process podUpdate.
                    if let Some(item) = PodUpdateItem::decode(&key) {
                        self.handle_pod_update(&item).await;
                    }
                    self.pod_update_queue.done(&key).await;
                }
                else => break,
            }
        }
    }

    /// The dispatch + worker part of `Run` (taint_eviction.go:305-353):
    /// `UpdateWorkerSize` workers, each with its own node/pod channels, fed by
    /// two dispatchers that shard by `hash(nodeName)`.
    pub fn start_workers(self: &Arc<Self>) {
        let mut node_txs = Vec::with_capacity(UPDATE_WORKER_SIZE);
        let mut pod_txs = Vec::with_capacity(UPDATE_WORKER_SIZE);
        for _ in 0..UPDATE_WORKER_SIZE {
            let (node_tx, node_rx) = mpsc::channel(NODE_UPDATE_CHANNEL_SIZE);
            let (pod_tx, pod_rx) = mpsc::channel(POD_UPDATE_CHANNEL_SIZE);
            node_txs.push(node_tx);
            pod_txs.push(pod_tx);
            let this = Arc::clone(self);
            tokio::spawn(async move { this.worker(node_rx, pod_rx).await });
        }

        let queue = self.node_update_queue.clone();
        tokio::spawn(async move {
            while let Some(node_name) = queue.get().await {
                let idx = hash(&node_name, UPDATE_WORKER_SIZE);
                // queue.done is called by the worker.
                if node_txs[idx].send(node_name.clone()).await.is_err() {
                    queue.done(&node_name).await;
                    return;
                }
            }
        });

        let queue = self.pod_update_queue.clone();
        tokio::spawn(async move {
            while let Some(key) = queue.get().await {
                // The fact that pods are processed by the same worker as nodes
                // is used to avoid races between node worker setting
                // taintedNodes and pod worker reading this (:334-337).
                let node = key.split('/').next().unwrap_or_default();
                let idx = hash(node, UPDATE_WORKER_SIZE);
                if pod_txs[idx].send(key.clone()).await.is_err() {
                    queue.done(&key).await;
                    return;
                }
            }
        });
    }

    /// The deferred shutdown of `Run` (taint_eviction.go:292-298).
    #[allow(dead_code)] // upstream API; the bin's watch path goes through observe_*
    pub async fn shutdown(&self) {
        self.node_update_queue.shutdown().await;
        self.pod_update_queue.shutdown().await;
        self.taint_eviction_queue.cancel_and_wait().await;
    }

    /// Informer stand-in for nodes: diff `node` (None = deleted) against the
    /// last-seen state and call `NodeUpdated` accordingly.
    async fn observe_node(&self, name: &str, node: Option<&Node>) {
        let new = node.map(|n| taint_keys(&node_no_execute_taints(n)));
        let old = {
            let mut cache = self.node_cache.lock().unwrap();
            match &new {
                Some(taints) => cache.insert(name.to_string(), taints.clone()),
                None => cache.remove(name),
            }
        };
        self.taints_updated(Some(name), old.as_deref(), new).await;
    }

    /// Informer stand-in for pods.
    async fn observe_pod(&self, key: &str, pod: Option<&Pod>) {
        let new = pod.map(PodSnapshot::of);
        let old = {
            let mut cache = self.pod_cache.lock().unwrap();
            match &new {
                Some(snap) => cache.insert(key.to_string(), snap.clone()),
                None => cache.remove(key),
            }
        };
        self.snapshot_updated(old.as_ref(), new.as_ref()).await;
    }

    /// Relist nodes and pods, feeding the differences to
    /// `NodeUpdated`/`PodUpdated` (an informer resync).
    async fn sync(&self) {
        match self
            .storage
            .list::<Node>(&build_prefix("nodes", None))
            .await
        {
            Ok(nodes) => {
                let mut seen = std::collections::HashSet::new();
                for node in &nodes {
                    seen.insert(node.metadata.name.clone());
                    self.observe_node(&node.metadata.name, Some(node)).await;
                }
                let gone: Vec<String> = self
                    .node_cache
                    .lock()
                    .unwrap()
                    .keys()
                    .filter(|k| !seen.contains(*k))
                    .cloned()
                    .collect();
                for name in gone {
                    self.observe_node(&name, None).await;
                }
            }
            Err(e) => error!("Failed to list nodes for taint eviction: {e}"),
        }
        match self.storage.list::<Pod>(&build_prefix("pods", None)).await {
            Ok(pods) => {
                let mut seen = std::collections::HashSet::new();
                for pod in &pods {
                    let key = format!(
                        "{}/{}",
                        pod.metadata.namespace.as_deref().unwrap_or_default(),
                        pod.metadata.name
                    );
                    seen.insert(key.clone());
                    self.observe_pod(&key, Some(pod)).await;
                }
                let gone: Vec<String> = self
                    .pod_cache
                    .lock()
                    .unwrap()
                    .keys()
                    .filter(|k| !seen.contains(*k))
                    .cloned()
                    .collect();
                for key in gone {
                    self.observe_pod(&key, None).await;
                }
            }
            Err(e) => error!("Failed to list pods for taint eviction: {e}"),
        }
    }

    async fn handle_watch_event(&self, is_node: bool, event: WatchEvent) {
        let (key, value, deleted) = match event {
            WatchEvent::Added(k, v) | WatchEvent::Modified(k, v) => (k, v, false),
            WatchEvent::Deleted(k, v) => (k, v, true),
        };
        let short = key.strip_prefix("/registry/").unwrap_or(&key);
        if is_node {
            let name = short.strip_prefix("nodes/").unwrap_or(short);
            if deleted {
                self.observe_node(name, None).await;
            } else if let Ok(node) = serde_json::from_str::<Node>(&value) {
                self.observe_node(name, Some(&node)).await;
            }
        } else {
            let pod_key = short.strip_prefix("pods/").unwrap_or(short);
            if deleted {
                self.observe_pod(pod_key, None).await;
            } else if let Ok(pod) = serde_json::from_str::<Pod>(&value) {
                self.observe_pod(pod_key, Some(&pod)).await;
            }
        }
    }

    /// `Run` (taint_eviction.go:279): start the worker pool, then keep the
    /// update queues fed from a watch on nodes and pods (with a periodic
    /// relist standing in for the informer resync).
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting {CONTROLLER_NAME}");
        self.start_workers();

        loop {
            self.sync().await;

            let nodes = self.storage.watch(&build_prefix("nodes", None)).await;
            let pods = self.storage.watch(&build_prefix("pods", None)).await;
            let (nodes, pods) = match (nodes, pods) {
                (Ok(n), Ok(p)) => (n, p),
                (n, p) => {
                    let e = n.err().or(p.err()).map(|e| e.to_string());
                    error!("Failed to establish watch: {e:?}, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut events = futures::stream::select(
                nodes.map(|e| (true, e)).boxed(),
                pods.map(|e| (false, e)).boxed(),
            );

            let mut resync = tokio::time::interval(RESYNC_PERIOD);
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = events.next() => match event {
                        Some((is_node, Ok(ev))) => self.handle_watch_event(is_node, ev).await,
                        Some((_, Err(e))) => {
                            warn!("Watch error: {e}, reconnecting");
                            watch_broken = true;
                        }
                        None => {
                            warn!("Watch stream ended, reconnecting");
                            watch_broken = true;
                        }
                    },
                    _ = resync.tick() => self.sync().await,
                }
            }
        }
    }

    /// Single synchronous pass over every node, for callers (and tests) that
    /// have no running worker pool: handles each node directly and waits for
    /// the evictions that fire immediately.
    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting taint eviction reconciliation");
        let nodes: Vec<Node> = self.storage.list(&build_prefix("nodes", None)).await?;
        for node in &nodes {
            self.handle_node_update(&node.metadata.name).await;
        }
        self.taint_eviction_queue.wait_idle().await;
        Ok(())
    }
}

/// `emitPodDeletionEvent` / `emitCancelPodDeletionEvent` (taint_eviction.go:590,
/// :603): a Normal `TaintManagerEviction` event on the Pod.
async fn emit_pod_event<S: Storage + 'static>(
    recorder: &EventRecorder<S>,
    namespace: &str,
    name: &str,
    message: &str,
) {
    let involved = ObjectReference {
        api_version: Some("v1".to_string()),
        kind: Some("Pod".to_string()),
        namespace: Some(namespace.to_string()),
        name: Some(name.to_string()),
        ..Default::default()
    };
    let source = EventSource {
        component: CONTROLLER_NAME.to_string(),
        host: None,
    };
    if let Err(e) = recorder
        .event(
            &involved,
            &source,
            EventType::Normal,
            "TaintManagerEviction",
            message,
        )
        .await
    {
        warn!("failed to record TaintManagerEviction event: {e}");
    }
}

/// `deletePodHandler` (taint_eviction.go:107).
async fn delete_pod_handler<S: Storage + 'static>(
    storage: Arc<S>,
    recorder: EventRecorder<S>,
    _fire_at: DateTime<Utc>,
    args: WorkArgs,
) -> Result<()> {
    info!(
        "Deleting pod controller={CONTROLLER_NAME} pod={}",
        args.key()
    );
    emit_pod_event(
        &recorder,
        &args.namespace,
        &args.name,
        &format!("Marking for deletion Pod {}", args.key()),
    )
    .await;
    let mut err = None;
    for _ in 0..RETRIES {
        match add_condition_and_delete_pod(storage.as_ref(), &args.name, &args.namespace).await {
            Ok(()) => return Ok(()),
            Err(e) => err = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(err.unwrap())
}

/// `addConditionAndDeletePod` (taint_eviction.go:129): publish the
/// `DisruptionTarget` condition on the STATUS subresource, then DELETE the pod.
/// The eviction must NOT stamp deletionTimestamp itself: the field is immutable
/// on update, so an api-server that enforces that rejects the write.
async fn add_condition_and_delete_pod<S: Storage>(
    storage: &S,
    name: &str,
    namespace: &str,
) -> Result<()> {
    let key = build_key("pods", Some(namespace), name);
    let mut pod = storage.get::<Pod>(&key).await?;

    // Pre-existing deviation, kept from the previous implementation (upstream
    // has no such skip): leave pods already terminating and kube-system pods
    // alone.
    if pod.metadata.deletion_timestamp.is_some() {
        return Ok(());
    }
    if namespace == "kube-system" {
        debug!("Skipping eviction of system pod {namespace}/{name}");
        return Ok(());
    }

    let status = pod.status.get_or_insert_with(Default::default);
    let conditions = status.conditions.get_or_insert_with(Vec::new);
    let updated = update_pod_condition(
        conditions,
        PodCondition {
            condition_type: "DisruptionTarget".to_string(),
            status: "True".to_string(),
            reason: Some("DeletionByTaintManager".to_string()),
            message: Some("Taint manager: deleting due to NoExecute taint".to_string()),
            last_probe_time: None,
            last_transition_time: Some(Utc::now()),
            observed_generation: None,
        },
    );
    if updated {
        storage.update_status(&key, &pod).await?;
    }
    storage.delete_gracefully(&key).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Cases ported from `taint_eviction_test.go` (kubernetes release-1.35).
    //! The fake clientset's "patch"/"delete" actions become: the pod gaining a
    //! `DisruptionTarget` condition (status patch) and being deleted.
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::json;

    fn pod(name: &str, node: &str) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "default", "uid": format!("uid-{name}")},
            "spec": {"nodeName": node, "containers": [{"name": "c", "image": "i"}]},
            "status": {"phase": "Running"}
        }))
        .unwrap()
    }

    /// `createNoExecuteTaint`.
    fn taint(i: usize) -> Taint {
        Taint {
            key: format!("testTaint{i}"),
            value: Some(format!("test{i}")),
            effect: "NoExecute".to_string(),
            time_added: Some(Utc::now()),
        }
    }

    fn toleration(i: usize, seconds: Option<i64>) -> Toleration {
        Toleration {
            key: Some(format!("testTaint{i}")),
            operator: None,
            value: Some(format!("test{i}")),
            effect: Some("NoExecute".to_string()),
            toleration_seconds: seconds,
        }
    }

    /// `addToleration`: duration < 0 means no tolerationSeconds.
    fn add_toleration(mut p: Pod, i: usize, duration: i64) -> Pod {
        p.spec.as_mut().unwrap().tolerations =
            Some(vec![toleration(i, (duration >= 0).then_some(duration))]);
        p
    }

    fn node(name: &str, taints: &[usize]) -> Node {
        let mut n: Node = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name}, "spec": {}
        }))
        .unwrap();
        n.spec.as_mut().unwrap().taints =
            Some(taints.iter().map(|i| taint(*i)).collect::<Vec<_>>()).filter(|t| !t.is_empty());
        n
    }

    struct Fixture {
        storage: Arc<MemoryStorage>,
        ctl: Arc<TaintEvictionController<MemoryStorage>>,
    }

    impl Fixture {
        async fn new(pods: Vec<Pod>) -> Self {
            let storage = Arc::new(MemoryStorage::new());
            for p in &pods {
                let key = build_key("pods", p.metadata.namespace.as_deref(), &p.metadata.name);
                storage.create(&key, p).await.unwrap();
            }
            let ctl = Arc::new(TaintEvictionController::new(Arc::clone(&storage)));
            ctl.start_workers();
            Self { storage, ctl }
        }

        async fn put_node(&self, n: &Node) {
            let key = build_key("nodes", None, &n.metadata.name);
            if self.storage.update(&key, n).await.is_err() {
                self.storage.create(&key, n).await.unwrap();
            }
        }

        /// (patched, deleted) for a pod, `verifyPodActions`' two booleans.
        async fn actions(&self, name: &str) -> (bool, bool) {
            let key = build_key("pods", Some("default"), name);
            match self.storage.get::<Pod>(&key).await {
                Err(_) => (true, true),
                Ok(p) => (
                    p.status
                        .and_then(|s| s.conditions)
                        .is_some_and(|c| c.iter().any(|c| c.condition_type == "DisruptionTarget")),
                    p.metadata.deletion_timestamp.is_some(),
                ),
            }
        }

        /// `verifyPodActions`: poll until the expected outcome (or 5s).
        async fn verify(&self, name: &str, patch: bool, delete: bool) {
            for _ in 0..500 {
                if self.actions(name).await == (patch, delete) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(self.actions(name).await, (patch, delete), "pod {name}");
        }

        /// Give the controller time to (not) act, then assert.
        async fn verify_stays(&self, name: &str, patch: bool, delete: bool) {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(self.actions(name).await, (patch, delete), "pod {name}");
        }

        fn set_tainted(&self, node: &str, taints: Vec<Taint>) {
            self.ctl
                .tainted_nodes
                .lock()
                .unwrap()
                .insert(node.to_string(), taints);
        }

        fn queued(&self, name: &str) -> bool {
            self.ctl
                .taint_eviction_queue
                .get_worker_unsafe(&format!("default/{name}"))
                .is_some()
        }
    }

    // ---- helpers ---------------------------------------------------------

    #[test]
    fn hash_is_fnv1a_32_mod_max() {
        // FNV-1a 32 of "" is the offset basis 0x811c9dc5 = 2166136261.
        assert_eq!(hash("", 8), (2166136261u32 % 8) as usize);
        // Known FNV-1a 32 vector: "a" -> 0xe40c292c.
        assert_eq!(hash("a", 1 << 16), (0xe40c292cu32 % (1 << 16)) as usize);
        for n in ["node1", "node-2", "x"] {
            assert!(hash(n, UPDATE_WORKER_SIZE) < UPDATE_WORKER_SIZE);
            assert_eq!(hash(n, 8), hash(n, 8), "stable");
        }
    }

    /// `TestFilterNoExecuteTaints`.
    #[test]
    fn filter_no_execute_taints() {
        let taints = vec![
            Taint {
                key: "one".into(),
                value: Some("one".into()),
                effect: "NoExecute".into(),
                time_added: None,
            },
            Taint {
                key: "two".into(),
                value: Some("two".into()),
                effect: "NoSchedule".into(),
                time_added: None,
            },
        ];
        let got = get_no_execute_taints(&taints);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].effect, "NoExecute");
    }

    /// `TestGetMinTolerationTime`.
    #[test]
    fn get_min_toleration_time_cases() {
        let t = |s: Option<i64>| Toleration {
            key: None,
            operator: None,
            value: None,
            effect: None,
            toleration_seconds: s,
        };
        let one = Duration::from_secs(1);
        assert_eq!(
            get_min_toleration_time(&[]),
            MinTolerationTime::After(Duration::ZERO)
        );
        assert_eq!(
            get_min_toleration_time(&[t(None)]),
            MinTolerationTime::Forever
        );
        assert_eq!(
            get_min_toleration_time(&[t(Some(1)), t(Some(2))]),
            MinTolerationTime::After(one)
        );
        assert_eq!(
            get_min_toleration_time(&[t(Some(1)), t(None)]),
            MinTolerationTime::After(one)
        );
        assert_eq!(
            get_min_toleration_time(&[t(None), t(Some(1))]),
            MinTolerationTime::After(one)
        );
        assert_eq!(
            get_min_toleration_time(&[t(Some(0))]),
            MinTolerationTime::After(Duration::ZERO)
        );
    }

    // ---- pods ------------------------------------------------------------

    /// `TestCreatePod`.
    #[tokio::test]
    async fn create_pod() {
        struct Case {
            description: &'static str,
            pod: Pod,
            tainted: Vec<(&'static str, Vec<Taint>)>,
            patch: bool,
            delete: bool,
        }
        let cases = vec![
            Case {
                description: "not scheduled - ignore",
                pod: pod("pod1", ""),
                tainted: vec![],
                patch: false,
                delete: false,
            },
            Case {
                description: "scheduled on untainted Node",
                pod: pod("pod1", "node1"),
                tainted: vec![],
                patch: false,
                delete: false,
            },
            Case {
                description: "schedule on tainted Node",
                pod: pod("pod1", "node1"),
                tainted: vec![("node1", vec![taint(1)])],
                patch: true,
                delete: true,
            },
            Case {
                description: "schedule on tainted Node with finite toleration",
                pod: add_toleration(pod("pod1", "node1"), 1, 100),
                tainted: vec![("node1", vec![taint(1)])],
                patch: false,
                delete: false,
            },
            Case {
                description: "schedule on tainted Node with infinite toleration",
                pod: add_toleration(pod("pod1", "node1"), 1, -1),
                tainted: vec![("node1", vec![taint(1)])],
                patch: false,
                delete: false,
            },
            Case {
                description: "schedule on tainted Node with infinite invalid toleration",
                pod: add_toleration(pod("pod1", "node1"), 2, -1),
                tainted: vec![("node1", vec![taint(1)])],
                patch: true,
                delete: true,
            },
        ];
        for c in cases {
            let f = Fixture::new(vec![c.pod.clone()]).await;
            for (n, t) in c.tainted {
                f.set_tainted(n, t);
            }
            f.ctl.pod_updated(None, Some(&c.pod)).await;
            if c.delete {
                f.verify("pod1", c.patch, c.delete).await;
            } else {
                f.verify_stays("pod1", c.patch, c.delete).await;
            }
            eprintln!("ok: {}", c.description);
            f.ctl.shutdown().await;
        }
    }

    /// `TestDeletePod`: a deleted pod must not panic.
    #[tokio::test]
    async fn delete_pod() {
        let f = Fixture::new(vec![]).await;
        f.set_tainted("node1", vec![taint(1)]);
        f.ctl.pod_updated(Some(&pod("pod1", "node1")), None).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        f.ctl.shutdown().await;
    }

    /// `TestUpdatePod`.
    #[tokio::test]
    async fn update_pod() {
        struct Case {
            description: &'static str,
            prev: Pod,
            await_scheduled: bool,
            new: Pod,
            patch: bool,
            delete: bool,
        }
        let cases = vec![
            Case {
                description: "scheduling onto tainted Node",
                prev: pod("pod1", ""),
                await_scheduled: false,
                new: pod("pod1", "node1"),
                patch: true,
                delete: true,
            },
            Case {
                description: "scheduling onto tainted Node with toleration",
                prev: add_toleration(pod("pod1", ""), 1, -1),
                await_scheduled: false,
                new: add_toleration(pod("pod1", "node1"), 1, -1),
                patch: false,
                delete: false,
            },
            Case {
                description: "removing toleration",
                prev: add_toleration(pod("pod1", "node1"), 1, 100),
                await_scheduled: true,
                new: pod("pod1", "node1"),
                patch: true,
                delete: true,
            },
            Case {
                description: "lengthening toleration shouldn't work",
                prev: add_toleration(pod("pod1", "node1"), 1, 1),
                await_scheduled: true,
                new: add_toleration(pod("pod1", "node1"), 1, 100),
                patch: true,
                delete: true,
            },
        ];
        for c in cases {
            let f = Fixture::new(vec![c.prev.clone()]).await;
            f.set_tainted("node1", vec![taint(1)]);
            f.ctl.pod_updated(None, Some(&c.prev)).await;
            if c.await_scheduled {
                for _ in 0..100 {
                    if f.queued("pod1") {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert!(
                    f.queued("pod1"),
                    "{}: eviction not scheduled",
                    c.description
                );
            }
            // The informer's podIndexer.Update(newPod).
            let key = build_key("pods", Some("default"), "pod1");
            f.storage.update(&key, &c.new).await.unwrap();
            f.ctl.pod_updated(Some(&c.prev), Some(&c.new)).await;
            if c.delete {
                f.verify("pod1", c.patch, c.delete).await;
            } else {
                f.verify_stays("pod1", c.patch, c.delete).await;
            }
            eprintln!("ok: {}", c.description);
            f.ctl.shutdown().await;
        }
    }

    // ---- nodes -----------------------------------------------------------

    /// `TestCreateNode`.
    #[tokio::test]
    async fn create_node() {
        let cases: Vec<(&str, Pod, Node, bool)> = vec![
            (
                "Creating Node matching already assigned Pod",
                pod("pod1", "node1"),
                node("node1", &[]),
                false,
            ),
            (
                "Creating tainted Node matching already assigned Pod",
                pod("pod1", "node1"),
                node("node1", &[1]),
                true,
            ),
            (
                "Creating tainted Node matching already assigned tolerating Pod",
                add_toleration(pod("pod1", "node1"), 1, -1),
                node("node1", &[1]),
                false,
            ),
        ];
        for (description, p, n, evicted) in cases {
            let f = Fixture::new(vec![p]).await;
            f.put_node(&n).await;
            f.ctl.node_updated(None, Some(&n)).await;
            if evicted {
                f.verify("pod1", true, true).await;
            } else {
                f.verify_stays("pod1", false, false).await;
            }
            eprintln!("ok: {description}");
            f.ctl.shutdown().await;
        }
    }

    /// `TestDeleteNode`.
    #[tokio::test]
    async fn delete_node() {
        let f = Fixture::new(vec![]).await;
        f.set_tainted("node1", vec![taint(1)]);
        f.ctl.node_updated(Some(&node("node1", &[])), None).await;
        for _ in 0..100 {
            if !f.ctl.tainted_nodes.lock().unwrap().contains_key("node1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !f.ctl.tainted_nodes.lock().unwrap().contains_key("node1"),
            "Failed to await for processing node deleted"
        );
        f.ctl.shutdown().await;
    }

    /// `TestUpdateNode`.
    #[tokio::test]
    async fn update_node() {
        let multi = {
            let mut p = pod("pod1", "node1");
            p.spec.as_mut().unwrap().tolerations =
                Some(vec![toleration(1, Some(1)), toleration(2, Some(100))]);
            p
        };
        // (description, pod, old, new, patch, delete, extra sleep ms)
        let cases: Vec<(&str, Pod, Node, Node, bool, bool, u64)> = vec![
            (
                "Added taint, expect node patched and deleted",
                pod("pod1", "node1"),
                node("node1", &[]),
                node("node1", &[1]),
                true,
                true,
                0,
            ),
            (
                "Added tolerated taint",
                add_toleration(pod("pod1", "node1"), 1, 100),
                node("node1", &[]),
                node("node1", &[1]),
                false,
                false,
                0,
            ),
            (
                "Only one added taint tolerated",
                add_toleration(pod("pod1", "node1"), 1, 100),
                node("node1", &[]),
                node("node1", &[1, 2]),
                true,
                true,
                0,
            ),
            (
                "Taint removed",
                add_toleration(pod("pod1", "node1"), 1, 1),
                node("node1", &[1]),
                node("node1", &[]),
                false,
                false,
                1500,
            ),
            (
                "Pod with multiple tolerations are evicted when first one runs out",
                multi,
                node("node1", &[]),
                node("node1", &[1, 2]),
                true,
                true,
                0,
            ),
        ];
        for (description, p, old, new, patch, delete, extra) in cases {
            let f = Fixture::new(vec![p]).await;
            f.put_node(&new).await;
            f.ctl.node_updated(Some(&old), Some(&new)).await;
            if extra > 0 {
                tokio::time::sleep(Duration::from_millis(extra)).await;
            }
            if delete {
                f.verify("pod1", patch, delete).await;
            } else {
                f.verify_stays("pod1", patch, delete).await;
            }
            eprintln!("ok: {description}");
            f.ctl.shutdown().await;
        }
    }

    /// `TestUpdateNodeWithMultipleTaints`.
    #[tokio::test]
    async fn update_node_with_multiple_taints() {
        let mut p = pod("pod1", "node1");
        let exists = |i: usize, secs: Option<i64>| Toleration {
            key: Some(format!("testTaint{i}")),
            operator: Some("Exists".into()),
            value: None,
            effect: Some("NoExecute".into()),
            toleration_seconds: secs,
        };
        p.spec.as_mut().unwrap().tolerations = Some(vec![exists(1, None), exists(2, Some(60))]);
        let f = Fixture::new(vec![p]).await;

        // no taint
        f.put_node(&node("node1", &[])).await;
        f.ctl.handle_node_update("node1").await;
        assert!(!f.queued("pod1"), "pod queued for deletion with no taints");

        // no taint -> infinitely tolerated taint
        f.put_node(&node("node1", &[1])).await;
        f.ctl.handle_node_update("node1").await;
        assert!(
            !f.queued("pod1"),
            "pod queued for deletion with permanently tolerated taint"
        );

        // infinitely tolerated taint -> temporarily tolerated taint
        f.put_node(&node("node1", &[1, 2])).await;
        f.ctl.handle_node_update("node1").await;
        assert!(
            f.queued("pod1"),
            "pod not queued for deletion after addition of temporarily tolerated taint"
        );

        // temporarily tolerated taint -> infinitely tolerated taint
        f.put_node(&node("node1", &[1])).await;
        f.ctl.handle_node_update("node1").await;
        assert!(
            !f.queued("pod1"),
            "pod queued for deletion after removal of temporarily tolerated taint"
        );

        assert_eq!(
            f.actions("pod1").await,
            (false, false),
            "Unexpected deletion"
        );
        f.ctl.shutdown().await;
    }

    /// `TestUpdateNodeWithMultiplePods`.
    #[tokio::test]
    async fn update_node_with_multiple_pods() {
        // Taint 1 only: pod1 (no toleration) goes at once, pod2 (1s) after
        // ~1s, pod3 (forever) never.
        let f = Fixture::new(vec![
            pod("pod1", "node1"),
            add_toleration(pod("pod2", "node1"), 1, 1),
            add_toleration(pod("pod3", "node1"), 1, -1),
        ])
        .await;
        let new = node("node1", &[1]);
        f.put_node(&new).await;
        f.ctl
            .node_updated(Some(&node("node1", &[])), Some(&new))
            .await;
        f.verify("pod1", true, true).await;
        assert_eq!(f.actions("pod2").await, (false, false), "pod2 too early");
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(f.actions("pod2").await, (true, true), "pod2 after 1s");
        assert_eq!(
            f.actions("pod3").await,
            (false, false),
            "pod3 tolerates forever"
        );
        f.ctl.shutdown().await;

        // Taints 1 and 2: every pod fails to tolerate both (pod3/pod2 only
        // tolerate taint 1), so all go at once.
        let f = Fixture::new(vec![
            pod("pod1", "node1"),
            add_toleration(pod("pod2", "node1"), 1, 1),
            add_toleration(pod("pod3", "node1"), 1, -1),
        ])
        .await;
        let new = node("node1", &[1, 2]);
        f.put_node(&new).await;
        f.ctl
            .node_updated(Some(&node("node1", &[])), Some(&new))
            .await;
        for p in ["pod1", "pod2", "pod3"] {
            f.verify(p, true, true).await;
        }
        f.ctl.shutdown().await;
    }

    /// `TestEventualConsistency`: a pod the node update did not know about is
    /// handled when its own update arrives.
    #[tokio::test]
    async fn eventual_consistency() {
        let cases: Vec<(&str, Option<Pod>, Pod)> = vec![
            (
                "existing pod2 scheduled onto tainted Node",
                Some(pod("pod2", "")),
                pod("pod2", "node1"),
            ),
            (
                "existing pod2 with taint toleration scheduled onto tainted Node",
                Some(add_toleration(pod("pod2", ""), 1, 100)),
                add_toleration(pod("pod2", "node1"), 1, 100),
            ),
            (
                "new pod2 created on tainted Node",
                None,
                pod("pod2", "node1"),
            ),
            (
                "new pod2 with tait toleration created on tainted Node",
                None,
                add_toleration(pod("pod2", "node1"), 1, 100),
            ),
        ];
        for (description, prev, new_pod) in cases {
            let f = Fixture::new(vec![pod("pod1", "node1")]).await;
            let new_node = node("node1", &[1]);
            f.put_node(&new_node).await;
            if let Some(prev) = &prev {
                let key = build_key("pods", Some("default"), "pod2");
                f.storage.create(&key, prev).await.unwrap();
                f.ctl.pod_updated(None, Some(prev)).await;
            }
            // First a NodeUpdate that deletes pod1; it does not know pod2 yet.
            f.ctl
                .node_updated(Some(&node("node1", &[])), Some(&new_node))
                .await;
            f.verify("pod1", true, true).await;

            // Then the delayed update of pod2 reaches the manager: delete it too.
            let key = build_key("pods", Some("default"), "pod2");
            if prev.is_some() {
                f.storage.update(&key, &new_pod).await.unwrap();
            } else {
                f.storage.create(&key, &new_pod).await.unwrap();
            }
            f.ctl.pod_updated(prev.as_ref(), Some(&new_pod)).await;
            // pod2's toleration (100s) is shorter than nothing, but taint 1
            // only: with toleration it is scheduled, without it deleted. The
            // upstream case expects a patch+delete for all four; a tolerated
            // pod is not deleted until 100s, so only assert the intolerant ones.
            if new_pod
                .spec
                .as_ref()
                .and_then(|s| s.tolerations.as_ref())
                .is_none()
            {
                f.verify("pod2", true, true).await;
            } else {
                for _ in 0..100 {
                    if f.queued("pod2") {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert!(f.queued("pod2"), "{description}: eviction scheduled");
            }
            eprintln!("ok: {description}");
            f.ctl.shutdown().await;
        }
    }

    /// `TestPodDeletionEvent`: the two TaintManagerEviction events.
    #[tokio::test]
    async fn pod_deletion_events() {
        let storage = Arc::new(MemoryStorage::new());
        let ctl = TaintEvictionController::new(Arc::clone(&storage));
        emit_pod_event(
            &ctl.recorder,
            "test",
            "test",
            "Marking for deletion Pod test/test",
        )
        .await;
        ctl.emit_event("test", "other", "Cancelling deletion of Pod test/other")
            .await;
        let events: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/test/").await.unwrap();
        let msgs: Vec<String> = events.iter().map(|e| e.message.clone()).collect();
        assert!(
            msgs.contains(&"Marking for deletion Pod test/test".to_string()),
            "{msgs:?}"
        );
        assert!(
            msgs.contains(&"Cancelling deletion of Pod test/other".to_string()),
            "{msgs:?}"
        );
        for e in &events {
            assert_eq!(e.reason, "TaintManagerEviction");
            assert_eq!(e.event_type, EventType::Normal);
            assert_eq!(e.involved_object.kind.as_deref(), Some("Pod"));
            assert_eq!(e.involved_object.api_version.as_deref(), Some("v1"));
        }
    }

    // ---- sharding --------------------------------------------------------

    /// The pod dispatcher shards by the node a pod sits on, the same hash the
    /// node dispatcher uses (taint_eviction.go:334-338), so a node's pods and
    /// the node itself are always handled by one worker.
    #[test]
    fn pod_item_round_trips_and_shards_by_node() {
        let item = PodUpdateItem {
            pod_name: "p".into(),
            pod_namespace: "ns".into(),
            node_name: "node-7".into(),
        };
        let key = item.encode();
        assert_eq!(PodUpdateItem::decode(&key), Some(item));
        assert_eq!(
            hash(key.split('/').next().unwrap(), UPDATE_WORKER_SIZE),
            hash("node-7", UPDATE_WORKER_SIZE)
        );
    }

    /// Many nodes spread over more than one worker shard.
    #[test]
    fn nodes_spread_across_shards() {
        let shards: std::collections::HashSet<usize> = (0..64)
            .map(|i| hash(&format!("node-{i}"), UPDATE_WORKER_SIZE))
            .collect();
        assert!(shards.len() > 1);
    }

    /// Updates for distinct nodes are processed concurrently across workers: a
    /// storage that stalls node reads must not block a node on another shard.
    #[tokio::test]
    async fn node_updates_run_on_more_than_one_worker() {
        use crate::controllers::worker_pool::test_support::StallStorage;
        // Pick two node names that land on different shards.
        let a = "node-a".to_string();
        let b = (0..64)
            .map(|i| format!("node-{i}"))
            .find(|n| hash(n, UPDATE_WORKER_SIZE) != hash(&a, UPDATE_WORKER_SIZE))
            .unwrap();
        let inner = Arc::new(MemoryStorage::new());
        let storage = StallStorage::new(Arc::clone(&inner), "/registry/nodes/");
        for n in [&a, &b] {
            inner
                .create(&build_key("nodes", None, n), &node(n, &[1]))
                .await
                .unwrap();
        }
        let ctl = Arc::new(TaintEvictionController::new(Arc::clone(&storage)));
        ctl.start_workers();
        ctl.node_update_queue.add(a.clone()).await;
        ctl.node_update_queue.add(b.clone()).await;
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(storage.peak() > 1, "node updates ran on one worker only");
        ctl.shutdown().await;
    }
}
