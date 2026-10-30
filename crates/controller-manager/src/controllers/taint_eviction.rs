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
pub fn update_pod_condition(conditions: &mut Vec<PodCondition>, new: PodCondition) -> bool {
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
pub struct PodUpdateItem {
    pub pod_name: String,
    pub pod_namespace: String,
    pub node_name: String,
}

impl PodUpdateItem {
    pub fn encode(&self) -> String {
        // Node names, namespaces and pod names cannot contain '/'.
        format!(
            "{}/{}/{}",
            self.node_name, self.pod_namespace, self.pod_name
        )
    }
    pub fn decode(key: &str) -> Option<Self> {
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
pub struct PodSnapshot {
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
    pub storage: Arc<S>,
    pub recorder: EventRecorder<S>,
    /// `taintEvictionQueue`.
    pub taint_eviction_queue: TimedWorkerQueue,
    /// `taintedNodes`: node name -> its NoExecute taints.
    pub tainted_nodes: Mutex<HashMap<String, Vec<Taint>>>,
    /// `nodeUpdateQueue` / `podUpdateQueue`.
    pub node_update_queue: WorkQueue,
    pub pod_update_queue: WorkQueue,
    /// Last-seen state, standing in for the informer's old object when
    /// computing `UpdateFunc(prev, obj)`.
    pub node_cache: Mutex<HashMap<String, Vec<TaintKey>>>,
    pub pod_cache: Mutex<HashMap<String, PodSnapshot>>,
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

    pub async fn emit_event(&self, namespace: &str, name: &str, message: &str) {
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
    pub async fn handle_node_update(&self, node_name: &str) {
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
pub async fn emit_pod_event<S: Storage + 'static>(
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
/// Like upstream (taint_eviction.go:129-153) there is no skip for pods that are
/// already terminating or live in kube-system. The eviction must NOT stamp deletionTimestamp itself: the field is immutable
/// on update, so an api-server that enforces that rejects the write.
pub async fn add_condition_and_delete_pod<S: Storage>(
    storage: &S,
    name: &str,
    namespace: &str,
) -> Result<()> {
    let key = build_key("pods", Some(namespace), name);
    let mut pod = storage.get::<Pod>(&key).await?;

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
