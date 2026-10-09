use crate::controllers::node_lifecycle_queue::{
    RateLimitedTimedQueue, RateLimiter, TimedValue, EVICTION_RATE_LIMITER_BURST,
    NODE_EVICTION_PERIOD,
};
use crate::controllers::node_lifecycle_zone::{get_zone_key, EvictionConfig, ZoneState};
use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::node::Taint;
use rusternetes_common::resources::{Lease, Node, NodeCondition, NodeStatus, Pod, PodStatus};
use rusternetes_common::types::Phase;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// NodeController monitors node health and manages node lifecycle.
///
/// Responsibilities:
/// 1. Monitor node heartbeats (via status updates)
/// 2. Mark nodes as NotReady when heartbeats are missed
/// 3. Evict pods from failed nodes
/// 4. Manage node taints based on conditions
/// 5. Update node status
const NODE_MONITOR_GRACE_PERIOD_SECONDS: i64 = 40;
const POD_EVICTION_TIMEOUT_SECONDS: i64 = 300; // 5 minutes
const NODE_STARTUP_GRACE_PERIOD_SECS: u64 = 60;

/// Workers draining the per-node queue: `nodeUpdateWorkerSize = 8`
/// (pkg/controller/nodelifecycle/node_lifecycle_controller.go:133), launched in
/// `Run` (:483-491). Upstream's pool runs the per-node taint/label pass
/// (`doNodeProcessingPassWorker`, :516) off `nodeUpdateQueue`; here
/// `process_node` is that pass (shutdown taint, Lease, allocatable). A node key
/// is never handed to two workers at once (queue processing set), matching the
/// :484-488 comment.
const NODE_UPDATE_WORKER_SIZE: usize = 8;

/// `podUpdateWorkerSize = 4` (node_lifecycle_controller.go:131), launched in
/// `Run` (:493-497): workers draining the pod-assignment queue
/// (`doPodProcessingWorker`, :1097).
const POD_UPDATE_WORKER_SIZE: usize = 4;

/// `--node-monitor-period` default of 5s
/// (pkg/controller/apis/config/v1alpha1/defaults.go `NodeMonitorPeriod`);
/// `Run` ticks `monitorNodeHealth` on it (:506-512).
const NODE_MONITOR_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

/// Per-node, per-condition snapshot of the last status/transition-time the
/// controller observed. Used to detect when a condition flips status without the
/// reporter having refreshed its `lastTransitionTime`.
type ObservedConditions = HashMap<String, HashMap<String, (String, Option<DateTime<Utc>>)>>;

/// `labelNodeDisruptionExclusion` (node_lifecycle_controller.go:817-819): nodes
/// carrying it are excluded from disruption checks.
const LABEL_NODE_DISRUPTION_EXCLUSION: &str = "node.kubernetes.io/exclude-disruption";

const TAINT_NODE_NOT_READY: &str = "node.kubernetes.io/not-ready";
const TAINT_NODE_UNREACHABLE: &str = "node.kubernetes.io/unreachable";

/// `NotReadyTaintTemplate` / `UnreachableTaintTemplate`
/// (node_lifecycle_controller.go:70-83): both `NoExecute`.
fn no_execute_taint(key: &str) -> Taint {
    Taint {
        key: key.to_string(),
        value: Some(String::new()),
        effect: "NoExecute".to_string(),
        time_added: None,
    }
}

/// `taintutils.TaintExists(node.Spec.Taints, template)`: key + NoExecute effect.
fn node_has_taint(node: &Node, key: &str) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .is_some_and(|ts| ts.iter().any(|t| t.key == key && t.effect == "NoExecute"))
}

/// The state `evictorLock` guards upstream, plus the `zoneStates` and
/// `knownNodeSet` maps `monitorNodeHealth` owns.
#[derive(Default)]
struct Evictor {
    zone_states: HashMap<String, ZoneState>,
    /// `zoneNoExecuteTainter`: one rate-limited queue per zone.
    zone_no_execute_tainter: HashMap<String, Arc<RateLimitedTimedQueue>>,
    /// `knownNodeSet`: node name -> uid.
    known_node_set: HashMap<String, String>,
}

/// What `monitor_node` observed for one node, feeding `handleDisruption`.
struct NodeObservation {
    zone: String,
    ready: bool,
    excluded_from_disruption: bool,
}

/// `nodeHealthData` (node_lifecycle_controller.go:168-185): what the controller
/// last saw of one node. Only the Ready condition of the saved `status` is ever
/// consulted (`GetNodeCondition(nodeHealth.status, NodeReady)`), so that is all
/// that is kept. `lease_renew_time` is `lease.Spec.RenewTime`.
#[derive(Clone)]
struct NodeHealthData {
    probe_timestamp: DateTime<Utc>,
    ready_transition_timestamp: DateTime<Utc>,
    ready_condition: Option<NodeCondition>,
    lease_renew_time: Option<DateTime<Utc>>,
}

/// `controllerutil.GetNodeCondition(&node.Status, t)`: a copy of the condition.
fn get_node_condition(node: &Node, condition_type: &str) -> Option<NodeCondition> {
    node.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.condition_type == condition_type)
        .cloned()
}

/// `scheduler.NodeHealthUpdateRetry` (pkg/scheduler/apis/config/types.go) = 5
/// and `retrySleepTime` (node_lifecycle_controller.go:128) = 20ms.
const NODE_HEALTH_UPDATE_RETRY: u32 = 5;
const RETRY_SLEEP_TIME: std::time::Duration = std::time::Duration::from_millis(20);

pub struct NodeController<S: Storage> {
    storage: Arc<S>,
    /// `nodeHealthMap` (:235).
    health_map: Arc<std::sync::Mutex<HashMap<String, NodeHealthData>>>,
    /// Offset added to the wall clock by `now()`; see `advance_clock_for_test`.
    clock_offset: Arc<std::sync::Mutex<Duration>>,
    first_seen: Arc<std::sync::Mutex<HashMap<String, std::time::Instant>>>,
    observed_conditions: Arc<std::sync::Mutex<ObservedConditions>>,
    eviction: EvictionConfig,
    evictor: Arc<tokio::sync::Mutex<Evictor>>,
}

impl<S: Storage + 'static> NodeController<S> {
    #[allow(dead_code)]
    pub fn new(storage: Arc<S>) -> Self {
        Self::with_eviction_config(storage, EvictionConfig::default())
    }

    pub fn with_eviction_config(storage: Arc<S>, eviction: EvictionConfig) -> Self {
        Self {
            storage,
            health_map: Arc::new(std::sync::Mutex::new(HashMap::new())),
            clock_offset: Arc::new(std::sync::Mutex::new(Duration::zero())),
            first_seen: Arc::new(std::sync::Mutex::new(HashMap::new())),
            observed_conditions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            eviction,
            evictor: Arc::new(tokio::sync::Mutex::new(Evictor::default())),
        }
    }

    /// Test helper: mark `node_name` as first seen long enough ago that the
    /// startup grace period is over. Reconcile_node skips condition updates
    /// within the K8s-standard 60s startup grace; this lets tests observe the
    /// Ready-flip behavior deterministically without sleeping.
    ///
    /// `dead_code` allowed because the bin compilation unit never calls this
    /// (cfg(test) blocks aren't compiled for the bin), but integration tests
    /// under `crates/controller-manager/tests/` do.
    #[allow(dead_code)]
    #[doc(hidden)]
    pub fn seed_first_seen_for_test(&self, node_name: &str) {
        let past = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(
                NODE_STARTUP_GRACE_PERIOD_SECS * 2,
            ))
            .unwrap_or_else(std::time::Instant::now);
        self.first_seen
            .lock()
            .unwrap()
            .insert(node_name.to_string(), past);
    }

    /// Watch-based run loop. Performs an initial full reconciliation, then watches
    /// for node changes. Falls back to periodic resync every 30s.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();
        let pod_queue = WorkQueue::new();

        // Node taint/label pass pool (:483-491).
        spawn_workers(NODE_UPDATE_WORKER_SIZE, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        // Pod pool (:493-497), fed by pod assignment events (`podUpdated`, :1087).
        spawn_workers(POD_UPDATE_WORKER_SIZE, &pod_queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.pod_worker(worker_queue).await;
            }
        });
        tokio::spawn({
            let feeder = Arc::clone(&self);
            let pod_queue = pod_queue.clone();
            async move { feeder.feed_pod_queue(pod_queue).await }
        });

        // NoExecute tainting loop (:500-503
        // `wait.UntilWithContext(ctx, nc.doNoExecuteTaintingPass, scheduler.NodeEvictionPeriod)`).
        tokio::spawn({
            let tainter = Arc::clone(&self);
            async move {
                let mut tick = tokio::time::interval(NODE_EVICTION_PERIOD);
                loop {
                    tick.tick().await;
                    tainter.do_no_execute_tainting_pass().await;
                }
            }
        });

        // Separate single health-monitor loop (:506-512).
        tokio::spawn({
            let monitor = Arc::clone(&self);
            async move {
                let mut tick = tokio::time::interval(NODE_MONITOR_PERIOD);
                loop {
                    tick.tick().await;
                    if let Err(e) = monitor.monitor_node_health().await {
                        error!("Error monitoring node health: {}", e);
                    }
                }
            }
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("nodes", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

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
                                tracing::warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }

    /// Main reconciliation loop - monitors all nodes
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let name = key.strip_prefix("nodes/").unwrap_or(&key);
            let storage_key = build_key("nodes", None, name);
            match self.storage.get::<Node>(&storage_key).await {
                Ok(resource) => match self.process_node(&resource).await {
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

    /// `doPodProcessingWorker` (node_lifecycle_controller.go:1097).
    async fn pod_worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            match self.process_pod(&key).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    // :1156 `podUpdateQueue.AddRateLimited(podItem)`
                    warn!("Unable to mark pod {} NotReady: {}", key, e);
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// `podUpdated` (:1087-1095): queue every pod that has a node assigned.
    /// Upstream only enqueues on add / nodeName change; storage watch events
    /// carry no old object, so every assigned-pod event is queued (processPod
    /// is idempotent: it no-ops unless the Ready condition must flip).
    async fn feed_pod_queue(&self, queue: WorkQueue) {
        loop {
            if let Ok(pods) = self.storage.list::<Pod>("/registry/pods/").await {
                for pod in pods {
                    Self::enqueue_pod(&queue, &pod).await;
                }
            }
            let mut watch = match self.storage.watch(&build_prefix("pods", None)).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish pod watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            while let Some(ev) = watch.next().await {
                match ev {
                    Ok(rusternetes_storage::WatchEvent::Deleted(..)) => {}
                    Ok(
                        rusternetes_storage::WatchEvent::Added(key, _)
                        | rusternetes_storage::WatchEvent::Modified(key, _),
                    ) => {
                        if let Ok(pod) = self.storage.get::<Pod>(&key).await {
                            Self::enqueue_pod(&queue, &pod).await;
                        }
                    }
                    Err(e) => {
                        warn!("Pod watch error: {}, reconnecting", e);
                        break;
                    }
                }
            }
        }
    }

    async fn enqueue_pod(queue: &WorkQueue, pod: &Pod) {
        let assigned = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .is_some_and(|n| !n.is_empty());
        if let (true, Some(ns)) = (assigned, pod.metadata.namespace.as_deref()) {
            queue
                .add(format!("pods/{}/{}", ns, pod.metadata.name))
                .await;
        }
    }

    /// `processPod` (:1115-1159): for a pod on a node whose Ready condition is
    /// not True, mark the pod NotReady (`MarkPodsNotReady`,
    /// pkg/controller/util/node/controller_utils.go:121). A missing pod or node
    /// is skipped (:1120, :1131). Deviation: node readiness is read from the
    /// stored Node, as there is no `nodeHealthMap` here.
    pub async fn process_pod(&self, key: &str) -> Result<()> {
        let pod_key = format!("/registry/{}", key);
        let mut pod: Pod = match self.storage.get(&pod_key).await {
            Ok(p) => p,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let node_name = match pod.spec.as_ref().and_then(|s| s.node_name.clone()) {
            Some(n) if !n.is_empty() => n,
            _ => return Ok(()),
        };
        let node: Node = match self
            .storage
            .get(&build_key("nodes", None, &node_name))
            .await
        {
            Ok(n) => n,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        // :1144-1150 no Ready condition: handled on a later node update.
        let node_ready = match node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"))
        {
            Some(c) => c.status == "True",
            None => return Ok(()),
        };
        if node_ready {
            return Ok(());
        }
        // UpdatePodCondition: only write when the status actually changes.
        let Some(cond) = pod
            .status
            .as_mut()
            .and_then(|s| s.conditions.as_mut())
            .and_then(|cs| cs.iter_mut().find(|c| c.condition_type == "Ready"))
        else {
            return Ok(());
        };
        if cond.status == "False" {
            return Ok(());
        }
        cond.status = "False".to_string();
        cond.last_transition_time = Some(Utc::now());
        match self.storage.update(&pod_key, &pod).await {
            Ok(_) | Err(rusternetes_common::Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<Node>("/registry/nodes/").await {
            Ok(items) => {
                for item in &items {
                    let key = format!("nodes/{}", item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list nodes for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting node reconciliation");

        // One-shot form of the three loops `run()` drives separately:
        // `monitorNodeHealth`, the per-node pass, and `doNoExecuteTaintingPass`.
        self.monitor_node_health().await?;

        let nodes: Vec<Node> = self.storage.list("/registry/nodes/").await?;
        for node in nodes {
            if let Err(e) = self.process_node(&node).await {
                error!("Failed to reconcile node {}: {}", &node.metadata.name, e);
            }
        }

        self.do_no_execute_tainting_pass().await;
        Ok(())
    }

    /// True while the node is inside the K8s startup grace period
    /// (`nodeStartupGracePeriod = 60s`); records first sight.
    fn in_startup_grace(&self, node_name: &str) -> bool {
        let first_seen_time = {
            let mut first_seen = self.first_seen.lock().unwrap();
            *first_seen
                .entry(node_name.to_string())
                .or_insert_with(std::time::Instant::now)
        };
        first_seen_time.elapsed() < std::time::Duration::from_secs(NODE_STARTUP_GRACE_PERIOD_SECS)
    }

    /// `monitorNodeHealth` (node_lifecycle_controller.go:670-778): one pass over
    /// all nodes, run from its own loop. Registers new nodes/zones
    /// (`classifyNodes`, :1178), fans the per-node work out with
    /// `workqueue.ParallelizeUntil(ctx, nc.nodeUpdateWorkerSize, ...)` (:774;
    /// same bound here), then runs `handleDisruption` (:776).
    pub async fn monitor_node_health(&self) -> Result<()> {
        let nodes: Vec<Node> = self.storage.list("/registry/nodes/").await?;
        self.register_nodes(&nodes).await;

        let pending: Vec<_> = nodes
            .iter()
            .map(|node| self.monitor_node_logged(node))
            .collect();
        let observations: Vec<NodeObservation> = futures::stream::iter(pending)
            .buffer_unordered(NODE_UPDATE_WORKER_SIZE)
            .filter_map(futures::future::ready)
            .collect()
            .await;

        self.handle_disruption(&observations, &nodes).await;
        Ok(())
    }

    async fn monitor_node_logged(&self, node: &Node) -> Option<NodeObservation> {
        match self.monitor_node(node).await {
            Ok(obs) => obs,
            Err(e) => {
                // :734-737 "Skipping - no pods will be evicted"
                error!("Failed to monitor node {}: {}", &node.metadata.name, e);
                None
            }
        }
    }

    /// `classifyNodes` (:1178) + the registration half of `monitorNodeHealth`
    /// (:676-694): new zones get a tainter queue, new nodes are recorded and
    /// have stale lifecycle taints cleared, deleted nodes are forgotten.
    async fn register_nodes(&self, nodes: &[Node]) {
        let mut added: Vec<&Node> = Vec::new();
        let mut new_zone_representatives: Vec<&Node> = Vec::new();
        let mut deleted: Vec<String> = Vec::new();
        {
            let ev = self.evictor.lock().await;
            for node in nodes {
                if !ev.known_node_set.contains_key(&node.metadata.name) {
                    added.push(node);
                } else if !ev.zone_states.contains_key(&get_zone_key(node)) {
                    // Currently, we only consider new zone as updated.
                    new_zone_representatives.push(node);
                }
            }
            // If there's a difference between lengths of known Nodes and
            // observed nodes we must have removed some Node.
            if ev.known_node_set.len() + added.len() != nodes.len() {
                let observed: std::collections::HashSet<&str> =
                    nodes.iter().map(|n| n.metadata.name.as_str()).collect();
                deleted = ev
                    .known_node_set
                    .keys()
                    .filter(|k| !observed.contains(k.as_str()))
                    .cloned()
                    .collect();
            }
        }

        for node in new_zone_representatives {
            self.add_pod_evictor_for_new_zone(node).await;
        }
        for node in added {
            debug!("Controller observed a new Node {}", node.metadata.name);
            self.evictor
                .lock()
                .await
                .known_node_set
                .insert(node.metadata.name.clone(), node.metadata.uid.clone());
            self.add_pod_evictor_for_new_zone(node).await;
            if let Err(e) = self.mark_node_as_reachable(node).await {
                warn!(
                    "Failed to clear taints of new node {}: {}",
                    node.metadata.name, e
                );
            }
        }
        for name in deleted {
            debug!("Controller observed a Node deletion {}", name);
            self.evictor.lock().await.known_node_set.remove(&name);
        }
    }

    /// `addPodEvictorForNewZone` (:1224): create the zone's tainter queue with
    /// the default `evictionLimiterQPS` limiter.
    async fn add_pod_evictor_for_new_zone(&self, node: &Node) {
        let zone = get_zone_key(node);
        let mut ev = self.evictor.lock().await;
        if !ev.zone_states.contains_key(&zone) {
            ev.zone_states.insert(zone.clone(), ZoneState::Initial);
            ev.zone_no_execute_tainter.insert(
                zone,
                Arc::new(RateLimitedTimedQueue::new(RateLimiter::token_bucket(
                    self.eviction.eviction_limiter_qps,
                    EVICTION_RATE_LIMITER_BURST,
                ))),
            );
        }
    }

    /// The per-node body of `monitorNodeHealth`'s `updateNodeFunc`
    /// (node_lifecycle_controller.go:703-767): `tryUpdateNodeHealth` under the
    /// `wait.PollImmediate(retrySleepTime, retrySleepTime*NodeHealthUpdateRetry)`
    /// retry (:713-729), then `processTaintBaseEviction` (:750). Returns the
    /// observation `handleDisruption` needs; an `Err` is upstream's "Skipping -
    /// no pods will be evicted" (:727).
    async fn monitor_node(&self, node: &Node) -> Result<Option<NodeObservation>> {
        let mut node = node.clone();
        let mut attempt = 0;
        let current_ready_condition = loop {
            match self.try_update_node_health(&mut node).await {
                Ok((_observed, current)) => break current,
                Err(e) => {
                    attempt += 1;
                    if attempt >= NODE_HEALTH_UPDATE_RETRY {
                        return Err(e);
                    }
                    tokio::time::sleep(RETRY_SLEEP_TIME).await;
                    // Re-get the node: it may have been deleted or updated.
                    node = self
                        .storage
                        .get(&build_key("nodes", None, &node.metadata.name))
                        .await?;
                }
            }
        };
        let node_name = node.metadata.name.clone();
        let observation = NodeObservation {
            zone: get_zone_key(&node),
            // computeZoneState counts a nil or non-True condition as unhealthy.
            ready: current_ready_condition
                .as_ref()
                .is_some_and(|c| c.status == "True"),
            excluded_from_disruption: node
                .metadata
                .labels
                .as_ref()
                .is_some_and(|l| l.contains_key(LABEL_NODE_DISRUPTION_EXCLUSION)),
        };

        let Some(current) = current_ready_condition else {
            return Ok(Some(observation));
        };

        // Refresh lastTransitionTime on any non-Ready condition that flipped
        // status without its reporter (kubelet eviction manager) bumping the
        // timestamp. Mirrors upstream pkg/controller/nodelifecycle, which keeps
        // the pressure-condition transition times observable.
        self.reconcile_condition_transitions(&node).await?;

        // Not-ready/unreachable NoExecute taints are NOT applied here: like
        // upstream they go through the zone's rate-limited queue
        // (`processTaintBaseEviction`, :781) and are drained by
        // `do_no_execute_tainting_pass`.
        self.process_taint_base_eviction(&node, &current.status)
            .await;

        // Evict pods from nodes that have been NotReady for too long. Only
        // once the throttled pass has actually tainted the node, so a
        // partition (which the queue holds back or disables) cannot bypass the
        // rate limit through this path.
        if current.status != "True"
            && self.should_evict_pods(&node)
            && self.has_no_execute_not_ready_taint(&node_name).await
        {
            info!("Evicting pods from NotReady node {}", node_name);
            self.evict_pods_from_node(&node_name).await?;
        }

        Ok(Some(observation))
    }

    /// `nc.now()` (node_lifecycle_controller.go:238 `now func() metav1.Time`):
    /// the controller's clock, injectable so tests can pass time the way
    /// upstream's tests reassign `nodeController.now`.
    fn now(&self) -> DateTime<Utc> {
        Utc::now() + *self.clock_offset.lock().unwrap()
    }

    /// Test helper standing in for upstream tests reassigning `nc.now`: move the
    /// controller's clock forward by `by`.
    #[allow(dead_code)]
    #[doc(hidden)]
    pub fn advance_clock_for_test(&self, by: Duration) {
        *self.clock_offset.lock().unwrap() += by;
    }

    /// `tryUpdateNodeHealth` (node_lifecycle_controller.go:830-994): compare the
    /// node's Ready condition and Lease to what the controller saw last time
    /// (`nodeHealthMap`), advance `probeTimestamp` when the kubelet showed life,
    /// and once nothing has been seen for the grace period post Unknown on Ready
    /// and the pressure conditions. Returns the Ready condition as first observed
    /// and the (possibly updated) current one; `node` is updated in place.
    ///
    /// The map entry is stored on every return path (the Go `defer`, :832-834).
    async fn try_update_node_health(
        &self,
        node: &mut Node,
    ) -> Result<(NodeCondition, Option<NodeCondition>)> {
        let mut health = self
            .health_map
            .lock()
            .unwrap()
            .get(&node.metadata.name)
            .cloned();
        let result = self.try_update_node_health_inner(node, &mut health).await;
        if let Some(h) = health {
            self.health_map
                .lock()
                .unwrap()
                .insert(node.metadata.name.clone(), h);
        }
        result
    }

    async fn try_update_node_health_inner(
        &self,
        node: &mut Node,
        node_health: &mut Option<NodeHealthData>,
    ) -> Result<(NodeCondition, Option<NodeCondition>)> {
        // A zero `metav1.Time` is the Go default for an unset creationTimestamp.
        let creation_zero = node
            .metadata
            .creation_timestamp
            .unwrap_or(DateTime::UNIX_EPOCH);
        let current_ready_condition = get_node_condition(node, "Ready");
        let observed_ready_condition: NodeCondition;
        let grace_period: Duration;
        match &current_ready_condition {
            None => {
                // If ready condition is nil, then kubelet (or nodecontroller)
                // never posted node status. A fake ready condition is created,
                // where LastHeartbeatTime and LastTransitionTime is set to
                // node.CreationTimestamp to avoid handle the corner case.
                observed_ready_condition = NodeCondition {
                    condition_type: "Ready".to_string(),
                    status: "Unknown".to_string(),
                    last_heartbeat_time: node.metadata.creation_timestamp,
                    last_transition_time: node.metadata.creation_timestamp,
                    reason: None,
                    message: None,
                };
                grace_period = Duration::seconds(NODE_STARTUP_GRACE_PERIOD_SECS as i64);
                match node_health {
                    Some(h) => h.ready_condition = None,
                    None => {
                        *node_health = Some(NodeHealthData {
                            ready_condition: None,
                            probe_timestamp: creation_zero,
                            ready_transition_timestamp: creation_zero,
                            lease_renew_time: None,
                        })
                    }
                }
            }
            Some(c) => {
                // If ready condition is not nil, make a copy of it, since we
                // may modify it in place later.
                observed_ready_condition = c.clone();
                grace_period = Duration::seconds(NODE_MONITOR_GRACE_PERIOD_SECONDS);
            }
        }

        // There are following cases to check (:864-877); `savedCondition` is
        // read AFTER the nil-Ready branch above has refreshed `status` (:882).
        let saved_condition = node_health.as_ref().and_then(|h| h.ready_condition.clone());
        let saved_lease = node_health.as_ref().and_then(|h| h.lease_renew_time);
        let now = self.now();
        let fresh = |ready: &Option<NodeCondition>, transition: DateTime<Utc>| NodeHealthData {
            ready_condition: ready.clone(),
            probe_timestamp: now,
            ready_transition_timestamp: transition,
            lease_renew_time: None,
        };
        if node_health.is_none() {
            debug!(
                "Missing timestamp for Node {}. Assuming now as a timestamp",
                node.metadata.name
            );
            *node_health = Some(fresh(&current_ready_condition, now));
        } else if saved_condition.is_none() && current_ready_condition.is_some() {
            debug!(
                "Creating timestamp entry for newly observed Node {}",
                node.metadata.name
            );
            *node_health = Some(fresh(&current_ready_condition, now));
        } else if saved_condition.is_some() && current_ready_condition.is_none() {
            error!(
                "ReadyCondition was removed from Status of Node {}",
                node.metadata.name
            );
            // TODO upstream: figure out what to do in this case. For now we do
            // the same thing as above.
            *node_health = Some(fresh(&current_ready_condition, now));
        } else if let (Some(saved), Some(current)) = (&saved_condition, &current_ready_condition) {
            if saved.last_heartbeat_time != current.last_heartbeat_time {
                // If ReadyCondition changed since the last time we checked, we
                // update the transition timestamp to "now", otherwise we leave
                // it as it is.
                let transition = if saved.last_transition_time != current.last_transition_time {
                    now
                } else {
                    node_health.as_ref().unwrap().ready_transition_timestamp
                };
                // :922-926 builds a fresh struct, so `lease` is reset here.
                *node_health = Some(fresh(&current_ready_condition, transition));
            }
        }
        let h = node_health.as_mut().unwrap();

        // Always update the probe time if node lease is renewed. Note: If kubelet
        // never posted the node status, but continues renewing the heartbeat
        // leases, the node controller will assume the node is healthy and take
        // no action.
        let observed_lease = self.node_lease_renew_time(&node.metadata.name).await;
        if let Some(observed) = observed_lease {
            if saved_lease.is_none_or(|saved| saved < observed) {
                h.lease_renew_time = Some(observed);
                h.probe_timestamp = now;
            }
        }

        if now > h.probe_timestamp + grace_period {
            // NodeReady condition or lease was last set longer ago than
            // gracePeriod, so update it to Unknown (regardless of its current
            // value) in the master.
            //
            // We don't change 'NodeNetworkUnavailable' condition, as it's
            // managed on a control plane level.
            let probe = h.probe_timestamp;
            let creation = node.metadata.creation_timestamp;
            let status = node.status.get_or_insert_with(NodeStatus::default);
            let conditions = status.conditions.get_or_insert_with(Vec::new);
            for condition_type in ["Ready", "MemoryPressure", "DiskPressure", "PIDPressure"] {
                match conditions
                    .iter_mut()
                    .find(|c| c.condition_type == condition_type)
                {
                    None => {
                        debug!(
                            "Condition {} of node {} was never updated by kubelet",
                            condition_type, node.metadata.name
                        );
                        conditions.push(NodeCondition {
                            condition_type: condition_type.to_string(),
                            status: "Unknown".to_string(),
                            reason: Some("NodeStatusNeverUpdated".to_string()),
                            message: Some("Kubelet never posted node status.".to_string()),
                            last_heartbeat_time: creation,
                            last_transition_time: Some(now),
                        });
                    }
                    Some(current) => {
                        debug!(
                            "Node {} hasn't been updated for {}s ({} condition)",
                            node.metadata.name,
                            (now - probe).num_seconds(),
                            condition_type
                        );
                        if current.status != "Unknown" {
                            current.status = "Unknown".to_string();
                            current.reason = Some("NodeStatusUnknown".to_string());
                            current.message =
                                Some("Kubelet stopped posting node status.".to_string());
                            current.last_transition_time = Some(now);
                        }
                    }
                }
            }
            // We need to update currentReadyCondition due to its value
            // potentially changed.
            let current_ready_condition = get_node_condition(node, "Ready");
            if current_ready_condition.as_ref() != Some(&observed_ready_condition) {
                let key = build_key("nodes", None, &node.metadata.name);
                // Status subresource write: node conditions live under
                // `.status`, which a full-object PUT strips (#1723).
                self.storage.update_status(&key, node).await?;
                *node_health = Some(NodeHealthData {
                    ready_condition: current_ready_condition.clone(),
                    probe_timestamp: probe,
                    ready_transition_timestamp: self.now(),
                    lease_renew_time: observed_lease,
                });
            }
            return Ok((observed_ready_condition, current_ready_condition));
        }

        Ok((observed_ready_condition, current_ready_condition))
    }

    /// `nc.leaseLister.Leases(v1.NamespaceNodeLease).Get(node.Name)` (:932):
    /// the node Lease's `spec.renewTime`, if the Lease exists.
    async fn node_lease_renew_time(&self, node_name: &str) -> Option<DateTime<Utc>> {
        let key = build_key("leases", Some("kube-node-lease"), node_name);
        self.storage
            .get::<Lease>(&key)
            .await
            .ok()
            .and_then(|l| l.spec)
            .and_then(|s| s.renew_time)
    }

    /// `processTaintBaseEviction` (:781-815).
    async fn process_taint_base_eviction(&self, node: &Node, ready_status: &str) {
        match ready_status {
            "False" => {
                // Update the taint straight away if the node is already tainted
                // with the unreachable taint.
                if node_has_taint(node, TAINT_NODE_UNREACHABLE) {
                    if !self
                        .swap_node_controller_taint(
                            &node.metadata.name,
                            no_execute_taint(TAINT_NODE_NOT_READY),
                            TAINT_NODE_UNREACHABLE,
                        )
                        .await
                    {
                        error!("Failed to instantly swap UnreachableTaint to NotReadyTaint. Will try again in the next cycle");
                    }
                } else if self.mark_node_for_tainting(node, "False").await {
                    debug!(
                        "Node {} is NotReady. Adding it to the Taint queue",
                        node.metadata.name
                    );
                }
            }
            "Unknown" => {
                if node_has_taint(node, TAINT_NODE_NOT_READY) {
                    if !self
                        .swap_node_controller_taint(
                            &node.metadata.name,
                            no_execute_taint(TAINT_NODE_UNREACHABLE),
                            TAINT_NODE_NOT_READY,
                        )
                        .await
                    {
                        error!("Failed to instantly swap NotReadyTaint to UnreachableTaint. Will try again in the next cycle");
                    }
                } else if self.mark_node_for_tainting(node, "Unknown").await {
                    debug!(
                        "Node {} is unresponsive. Adding it to the Taint queue",
                        node.metadata.name
                    );
                }
            }
            "True" => match self.mark_node_as_reachable(node).await {
                Ok(true) => debug!(
                    "Node {} is healthy again, removed all taints",
                    node.metadata.name
                ),
                Ok(false) => {}
                Err(_) => error!("Failed to remove taints from node. Will retry in next iteration"),
            },
            _ => {}
        }
    }

    async fn zone_queue(&self, node: &Node) -> Option<Arc<RateLimitedTimedQueue>> {
        let ev = self.evictor.lock().await;
        ev.zone_no_execute_tainter.get(&get_zone_key(node)).cloned()
    }

    /// `markNodeForTainting` (:1239): queue the node on its zone's tainter.
    async fn mark_node_for_tainting(&self, node: &Node, status: &str) -> bool {
        let Some(queue) = self.zone_queue(node).await else {
            return false;
        };
        let name = &node.metadata.name;
        if status == "False" && !node_has_taint(node, TAINT_NODE_NOT_READY) {
            queue.remove(name);
        }
        if status == "Unknown" && !node_has_taint(node, TAINT_NODE_UNREACHABLE) {
            queue.remove(name);
        }
        queue.add(name, &node.metadata.uid)
    }

    /// `markNodeAsReachable` (:1257): remove both lifecycle taints and drop the
    /// node from its zone queue.
    async fn mark_node_as_reachable(&self, node: &Node) -> Result<bool> {
        self.remove_taint_off_node(node, TAINT_NODE_UNREACHABLE)
            .await?;
        self.remove_taint_off_node(node, TAINT_NODE_NOT_READY)
            .await?;
        Ok(match self.zone_queue(node).await {
            Some(q) => q.remove(&node.metadata.name),
            None => false,
        })
    }

    /// `controller.RemoveTaintOffNode` (pkg/controller/controller_utils.go): a
    /// no-op when the passed node carries no such NoExecute taint, otherwise
    /// remove it from the stored node.
    async fn remove_taint_off_node(&self, node: &Node, key: &str) -> Result<()> {
        if !node_has_taint(node, key) {
            return Ok(());
        }
        let node_key = build_key("nodes", None, &node.metadata.name);
        let mut stored: Node = self.storage.get(&node_key).await?;
        if let Some(spec) = stored.spec.as_mut() {
            if let Some(taints) = spec.taints.as_mut() {
                let before = taints.len();
                taints.retain(|t| !(t.key == key && t.effect == "NoExecute"));
                if taints.len() != before {
                    if taints.is_empty() {
                        spec.taints = None;
                    }
                    self.storage.update(&node_key, &stored).await?;
                    debug!("Removed {} taint from node {}", key, node.metadata.name);
                }
            }
        }
        Ok(())
    }

    /// `SwapNodeControllerTaint`
    /// (pkg/controller/util/node/controller_utils.go:194-226): stamp
    /// `timeAdded`, add-or-update the new taint (`taintutils.AddOrUpdateTaint`
    /// matches on key+effect and no-ops when fully equal), remove the opposite
    /// one. Returns true on success. Both edits are one storage write here.
    async fn swap_node_controller_taint(
        &self,
        node_name: &str,
        mut taint_to_add: Taint,
        taint_to_remove: &str,
    ) -> bool {
        taint_to_add.time_added = Some(Utc::now());
        let node_key = build_key("nodes", None, node_name);
        let mut stored: Node = match self.storage.get(&node_key).await {
            Ok(n) => n,
            Err(e) => {
                error!("unable to taint unresponsive Node {:?}: {}", node_name, e);
                return false;
            }
        };
        let spec = stored
            .spec
            .get_or_insert(rusternetes_common::resources::NodeSpec {
                pod_cidr: None,
                pod_cidrs: None,
                provider_id: None,
                unschedulable: None,
                taints: None,
            });
        let taints = spec.taints.get_or_insert_with(Vec::new);
        let before = serde_json::to_value(&*taints).unwrap_or_default();
        match taints
            .iter_mut()
            .find(|t| t.key == taint_to_add.key && t.effect == taint_to_add.effect)
        {
            Some(existing) => *existing = taint_to_add,
            None => taints.push(taint_to_add),
        }
        taints.retain(|t| !(t.key == taint_to_remove && t.effect == "NoExecute"));
        let after = serde_json::to_value(&*taints).unwrap_or_default();
        if before == after {
            return true;
        }
        match self.storage.update(&node_key, &stored).await {
            Ok(_) => true,
            Err(e) => {
                error!("unable to taint unresponsive Node {:?}: {}", node_name, e);
                false
            }
        }
    }

    async fn has_no_execute_not_ready_taint(&self, node_name: &str) -> bool {
        match self
            .storage
            .get::<Node>(&build_key("nodes", None, node_name))
            .await
        {
            Ok(n) => {
                node_has_taint(&n, TAINT_NODE_NOT_READY)
                    || node_has_taint(&n, TAINT_NODE_UNREACHABLE)
            }
            Err(_) => false,
        }
    }

    /// `doNoExecuteTaintingPass` (:595-663): drain every zone's rate-limited
    /// queue, applying the not-ready or unreachable taint according to the
    /// node's current Ready condition.
    pub async fn do_no_execute_tainting_pass(&self) {
        // Snapshot the queues so the lock is not held for the whole pass
        // (:597-610).
        let queues: Vec<Arc<RateLimitedTimedQueue>> = {
            let ev = self.evictor.lock().await;
            ev.zone_no_execute_tainter.values().cloned().collect()
        };
        for queue in queues {
            // Function returns false and a time after which it should be
            // retried, or true if it shouldn't (it succeeded).
            queue
                .try_process(|value: TimedValue| async move {
                    let key = build_key("nodes", None, &value.value);
                    let node: Node = match self.storage.get(&key).await {
                        Ok(n) => n,
                        Err(rusternetes_common::Error::NotFound(_)) => {
                            debug!("Node {} no longer present", value.value);
                            return (true, std::time::Duration::ZERO);
                        }
                        Err(e) => {
                            debug!("Failed to get Node {}: {}", value.value, e);
                            // retry in 50 millisecond
                            return (false, std::time::Duration::from_millis(50));
                        }
                    };
                    let Some(condition) = node
                        .status
                        .as_ref()
                        .and_then(|s| s.conditions.as_ref())
                        .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"))
                    else {
                        debug!("Failed to get NodeCondition from node {}", value.value);
                        return (false, std::time::Duration::from_millis(50));
                    };
                    // Because we want to mimic NodeStatus.Condition["Ready"] we
                    // make "unreachable" and "not ready" taints mutually
                    // exclusive.
                    let (to_add, opposite) = match condition.status.as_str() {
                        "False" => (TAINT_NODE_NOT_READY, TAINT_NODE_UNREACHABLE),
                        "Unknown" => (TAINT_NODE_UNREACHABLE, TAINT_NODE_NOT_READY),
                        _ => {
                            // The Node is ready again, so there's no need to
                            // taint it.
                            return (true, std::time::Duration::ZERO);
                        }
                    };
                    let ok = self
                        .swap_node_controller_taint(
                            &value.value,
                            no_execute_taint(to_add),
                            opposite,
                        )
                        .await;
                    (ok, std::time::Duration::ZERO)
                })
                .await;
        }
    }

    /// `handleDisruption` (:996-1085): derive each zone's state from its nodes'
    /// Ready conditions and retune the zone's eviction limiter.
    async fn handle_disruption(&self, observations: &[NodeObservation], nodes: &[Node]) {
        let mut zone_to_conditions: HashMap<String, Vec<bool>> = HashMap::new();
        for o in observations {
            // Some nodes may be excluded from disruption checking
            if !o.excluded_from_disruption {
                zone_to_conditions
                    .entry(o.zone.clone())
                    .or_default()
                    .push(o.ready);
            }
        }

        let mut zone_states = self.evictor.lock().await.zone_states.clone();
        let mut new_zone_states: HashMap<String, ZoneState> = HashMap::new();
        let mut all_are_fully_disrupted = true;
        for (k, v) in &zone_to_conditions {
            let (_unhealthy, new_state) = self.eviction.compute_zone_state(v);
            if new_state != ZoneState::FullDisruption {
                all_are_fully_disrupted = false;
            }
            new_zone_states.insert(k.clone(), new_state);
            zone_states.entry(k.clone()).or_insert(ZoneState::Initial);
        }

        let mut all_was_fully_disrupted = true;
        let keys: Vec<String> = zone_states.keys().cloned().collect();
        for k in keys {
            if !zone_to_conditions.contains_key(&k) {
                zone_states.remove(&k);
                continue;
            }
            if zone_states[&k] != ZoneState::FullDisruption {
                all_was_fully_disrupted = false;
                break;
            }
        }

        // At least one node was responding in previous pass or in the current
        // pass. Semantics is as follows:
        // - partialDisruption: use the reduced limiter,
        // - normal: resume normal operation,
        // - fullDisruption: restore normal eviction rate, unless all zones in
        //   the cluster are in fullDisruption - then stop all evictions.
        if !all_are_fully_disrupted || !all_was_fully_disrupted {
            if all_are_fully_disrupted {
                // We're switching to full disruption mode
                info!("Controller detected that all Nodes are not-Ready. Entering master disruption mode");
                for node in nodes {
                    if let Err(e) = self.mark_node_as_reachable(node).await {
                        error!(
                            "Failed to remove taints from Node {}: {}",
                            node.metadata.name, e
                        );
                    }
                }
                // We stop all evictions.
                for k in zone_states.keys() {
                    self.swap_zone_limiter(k, 0.0).await;
                }
                for v in zone_states.values_mut() {
                    *v = ZoneState::FullDisruption;
                }
                self.evictor.lock().await.zone_states = zone_states;
                // All rate limiters are updated, so we can return early here.
                return;
            }
            if all_was_fully_disrupted {
                // We're exiting full disruption mode
                info!(
                    "Controller detected that some Nodes are Ready. Exiting master disruption mode"
                );
                // When exiting disruption mode update probe timestamps on all
                // Nodes (:1058-1065).
                {
                    let now = self.now();
                    let mut health_map = self.health_map.lock().unwrap();
                    for node in nodes {
                        // `v := getDeepCopy(name)`: Go would nil-deref on a node
                        // never probed; skip it instead.
                        if let Some(v) = health_map.get_mut(&node.metadata.name) {
                            v.probe_timestamp = now;
                            v.ready_transition_timestamp = now;
                        }
                    }
                }
                // We reset all rate limiters to settings appropriate for the
                // given state.
                let keys: Vec<String> = zone_states.keys().cloned().collect();
                for k in keys {
                    let state = new_zone_states[&k];
                    let size = zone_to_conditions.get(&k).map_or(0, Vec::len);
                    self.set_limiter_in_zone(&k, size, state).await;
                    zone_states.insert(k, state);
                }
                self.evictor.lock().await.zone_states = zone_states;
                return;
            }
            // We know that there's at least one not-fully disrupted so, we can
            // use default behavior for rate limiters
            let keys: Vec<String> = zone_states.keys().cloned().collect();
            for k in keys {
                let new_state = new_zone_states[&k];
                if zone_states[&k] == new_state {
                    continue;
                }
                info!(
                    "Controller detected that zone {:?} is now in state {:?}",
                    k, new_state
                );
                let size = zone_to_conditions.get(&k).map_or(0, Vec::len);
                self.set_limiter_in_zone(&k, size, new_state).await;
                zone_states.insert(k, new_state);
            }
        }
        self.evictor.lock().await.zone_states = zone_states;
    }

    async fn swap_zone_limiter(&self, zone: &str, qps: f32) {
        let queue = self
            .evictor
            .lock()
            .await
            .zone_no_execute_tainter
            .get(zone)
            .cloned();
        if let Some(q) = queue {
            q.swap_limiter(qps).await;
        }
    }

    /// `setLimiterInZone` (:1161).
    async fn set_limiter_in_zone(&self, zone: &str, zone_size: usize, state: ZoneState) {
        match state {
            ZoneState::Normal => {
                self.swap_zone_limiter(zone, self.eviction.eviction_limiter_qps)
                    .await
            }
            // enterPartialDisruptionFunc = ReducedQPSFunc (:357)
            ZoneState::PartialDisruption => {
                self.swap_zone_limiter(zone, self.eviction.reduced_qps(zone_size))
                    .await
            }
            // enterFullDisruptionFunc = HealthyQPSFunc (:358): a fully
            // disrupted zone while other zones still respond keeps the normal
            // rate; only all-zones-down stops evictions (handled above).
            ZoneState::FullDisruption => {
                self.swap_zone_limiter(zone, self.eviction.eviction_limiter_qps)
                    .await
            }
            ZoneState::Initial => {}
        }
    }

    /// Per-node pass run by the 8-worker pool (`doNodeProcessingPassWorker`,
    /// node_lifecycle_controller.go:516): shutdown taint, Lease, allocatable.
    async fn process_node(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        if self.in_startup_grace(node_name) {
            return Ok(());
        }

        // Apply the shutdown taint when the node reports a graceful shutdown.
        //
        // Upstream (pkg/controller/nodelifecycle): when the kubelet enters its
        // graceful-shutdown sequence it sets Ready=False with reason "NodeShutdown".
        // The node lifecycle controller then applies `node.kubernetes.io/shutdown`
        // (NoSchedule) so the scheduler stops admitting pods to the shutting-down node.
        let is_shutdown = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"))
            .and_then(|c| c.reason.as_deref())
            .map(|r| r == "NodeShutdown")
            .unwrap_or(false);
        if is_shutdown {
            self.add_shutdown_taint(node).await?;
        } else {
            // Node is no longer shutting down — clear any stale shutdown taint so
            // scheduling resumes. Without this the taint persists indefinitely
            // and blocks all pods if a node recovers without re-registering.
            // Upstream's taintManager removes the taint on the same transition.
            self.remove_shutdown_taint(node).await?;
        }

        // NOTE: the node Lease is NOT renewed here. Upstream's kubelet renews it
        // (pkg/kubelet/nodelease/controller.go) and this controller only reads
        // it as a probe (tryUpdateNodeHealth, :932-936). Renewing it from the
        // controller would keep every probe fresh forever, so a dead kubelet's
        // heartbeat could never go stale.

        // Compute status.allocatable = status.capacity − kube-reserved.
        //
        // Upstream contract (pkg/kubelet/cm/node_container_manager_linux.go
        // → setNodeStatusMachineInfo / getNodeAllocatableAbsolute):
        //   allocatable = capacity − kube-reserved − system-reserved − eviction-hard
        // rusternetes reads reservation amounts from the annotation
        // `node.alpha.kubernetes.io/kube-reserved` (comma-separated key=value list,
        // e.g. "cpu=500m,memory=1Gi") as a proxy for the kubelet flag plumbing that
        // the kubelet stub does not yet surface.
        self.compute_allocatable(node).await?;

        Ok(())
    }

    /// Check if pods should be evicted from a node
    fn should_evict_pods(&self, node: &Node) -> bool {
        let status = match &node.status {
            Some(s) => s,
            None => return false,
        };

        let ready_condition = match &status.conditions {
            Some(conditions) => conditions.iter().find(|c| c.condition_type == "Ready"),
            None => return false,
        };

        let ready_condition = match ready_condition {
            Some(c) => c,
            None => return false,
        };

        // Only evict if node has been NotReady for a while
        if ready_condition.status == "True" {
            return false;
        }

        // Check when the node became NotReady
        if let Some(transition_time) = &ready_condition.last_transition_time {
            let now = Utc::now();
            let elapsed = now.signed_duration_since(*transition_time);

            // Evict pods after timeout
            return elapsed > Duration::seconds(POD_EVICTION_TIMEOUT_SECONDS);
        }

        false
    }

    /// Track each condition's observed status across reconciles and, when a
    /// non-Ready condition flips status without its reporter refreshing
    /// `lastTransitionTime`, bump that timestamp to now.
    ///
    /// The Ready condition is excluded — its transition time is managed by
    /// `update_node_status` — but it is still recorded so a spurious bump is
    /// never applied to it here.
    ///
    /// Upstream: the kubelet eviction manager sets pressure conditions and their
    /// transition times (pkg/kubelet/eviction); the node lifecycle controller
    /// (pkg/controller/nodelifecycle) observes the flips. rusternetes folds the
    /// transition-time refresh into the controller because the kubelet stub does
    /// not yet emit it.
    async fn reconcile_condition_transitions(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        let conditions = match node.status.as_ref().and_then(|s| s.conditions.as_ref()) {
            Some(c) => c,
            None => return Ok(()),
        };

        // Condition types whose lastTransitionTime must be refreshed now.
        let mut to_bump: Vec<String> = Vec::new();
        {
            let mut observed = self.observed_conditions.lock().unwrap();
            let node_cache = observed.entry(node_name.clone()).or_default();
            for cond in conditions {
                let prev = node_cache.get(&cond.condition_type).cloned();
                if cond.condition_type != "Ready" {
                    if let Some((prev_status, prev_ltt)) = &prev {
                        if *prev_status != cond.status {
                            // Status flipped. Did the reporter already bump LTT?
                            let reporter_bumped = match (cond.last_transition_time, prev_ltt) {
                                (Some(now_ltt), Some(old_ltt)) => now_ltt > *old_ltt,
                                (Some(_), None) => true,
                                _ => false,
                            };
                            if !reporter_bumped {
                                to_bump.push(cond.condition_type.clone());
                            }
                        }
                    }
                }
                node_cache.insert(
                    cond.condition_type.clone(),
                    (cond.status.clone(), cond.last_transition_time),
                );
            }
        }

        if to_bump.is_empty() {
            return Ok(());
        }

        let key = build_key("nodes", None, node_name);
        let mut updated: Node = self.storage.get(&key).await?;
        let now = Utc::now();
        if let Some(status) = updated.status.as_mut() {
            if let Some(conds) = status.conditions.as_mut() {
                for c in conds.iter_mut() {
                    if to_bump.contains(&c.condition_type) {
                        c.last_transition_time = Some(now);
                        debug!(
                            "Node {} condition {} flipped to {}; refreshed lastTransitionTime",
                            node_name, c.condition_type, c.status
                        );
                    }
                }
            }
        }
        // Status subresource write: a full-object PUT strips `.status` (#1723).
        self.storage.update_status(&key, &updated).await?;

        // Record the refreshed timestamps so the next reconcile sees no flip.
        {
            let mut observed = self.observed_conditions.lock().unwrap();
            if let Some(node_cache) = observed.get_mut(node_name) {
                for t in &to_bump {
                    if let Some(entry) = node_cache.get_mut(t) {
                        entry.1 = Some(now);
                    }
                }
            }
        }

        Ok(())
    }

    /// Apply the `node.kubernetes.io/shutdown` taint (NoSchedule) when the kubelet
    /// has started a graceful shutdown (Ready=False, reason="NodeShutdown").
    ///
    /// Upstream: `pkg/controller/nodelifecycle/node_lifecycle_controller.go`
    /// (`TaintNodeShutdown` constant, `taintManager.TaintNode`).
    async fn add_shutdown_taint(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        let key = build_key("nodes", None, node_name);
        let mut updated_node: Node = self.storage.get(&key).await?;

        let shutdown_taint = rusternetes_common::resources::node::Taint {
            key: "node.kubernetes.io/shutdown".to_string(),
            value: Some("".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        };

        let spec = updated_node
            .spec
            .get_or_insert(rusternetes_common::resources::NodeSpec {
                pod_cidr: None,
                pod_cidrs: None,
                provider_id: None,
                unschedulable: None,
                taints: None,
            });
        let taints = spec.taints.get_or_insert_with(Vec::new);
        if !taints.iter().any(|t| t.key == shutdown_taint.key) {
            taints.push(shutdown_taint);
            self.storage.update(&key, &updated_node).await?;
            debug!("Added shutdown taint to node {}", node_name);
        }
        Ok(())
    }

    /// Remove the `node.kubernetes.io/shutdown` taint once the node is no longer
    /// reporting a graceful shutdown, so scheduling resumes. Mirrors the removal
    /// side of upstream's taintManager. No-op (no write) when the taint is absent.
    async fn remove_shutdown_taint(&self, node: &Node) -> Result<()> {
        // Cheap pre-check on the passed-in node to avoid a storage round-trip
        // on the common path (no shutdown taint present).
        let has_taint = node
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_ref())
            .map(|ts| ts.iter().any(|t| t.key == "node.kubernetes.io/shutdown"))
            .unwrap_or(false);
        if !has_taint {
            return Ok(());
        }

        let node_name = &node.metadata.name;
        let key = build_key("nodes", None, node_name);
        let mut updated_node: Node = self.storage.get(&key).await?;
        if let Some(spec) = updated_node.spec.as_mut() {
            if let Some(taints) = spec.taints.as_mut() {
                let before = taints.len();
                taints.retain(|t| t.key != "node.kubernetes.io/shutdown");
                if taints.len() != before {
                    self.storage.update(&key, &updated_node).await?;
                    debug!("Removed shutdown taint from node {}", node_name);
                }
            }
        }
        Ok(())
    }

    /// Derive `status.allocatable` from `status.capacity` minus any resources
    /// listed in the `node.alpha.kubernetes.io/kube-reserved` annotation.
    ///
    /// Annotation format mirrors the kubelet `--kube-reserved` flag:
    /// a comma-separated list of `resource=quantity` pairs, e.g.
    /// `"cpu=500m,memory=1Gi"`.  Resources absent from the reservation pass
    /// through unchanged (allocatable == capacity for those resources).
    ///
    /// Upstream: `pkg/kubelet/cm/node_container_manager_linux.go`
    /// (`getNodeAllocatableAbsolute`).
    async fn compute_allocatable(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        let capacity = match node.status.as_ref().and_then(|s| s.capacity.as_ref()) {
            Some(c) => c.clone(),
            None => return Ok(()), // nothing to compute without capacity
        };

        // Parse the kube-reserved annotation into a resource→quantity map.
        let reserved = node
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("node.alpha.kubernetes.io/kube-reserved"))
            .map(|v| parse_resource_list(v))
            .unwrap_or_default();

        // Compute allocatable = capacity − reserved for each resource.
        let mut allocatable: HashMap<String, String> = HashMap::new();
        for (resource, cap_str) in &capacity {
            let result_str = if let Some(res_str) = reserved.get(resource) {
                match (Quantity::parse(cap_str), Quantity::parse(res_str)) {
                    (Ok(cap_q), Ok(res_q)) => match cap_q.sub(&res_q) {
                        // Clamp to zero when reserved exceeds capacity — a
                        // negative allocatable is nonsensical and would break
                        // scheduler quantity comparisons (upstream
                        // getNodeAllocatableAbsolute clamps to 0).
                        Some(result_q) if result_q.is_negative() => "0".to_string(),
                        Some(result_q) => result_q.canonical_string(),
                        None => {
                            warn!(
                                "Node {} allocatable overflow for resource {}: {} - {}",
                                node_name, resource, cap_str, res_str
                            );
                            cap_str.clone()
                        }
                    },
                    _ => {
                        warn!(
                            "Node {} could not parse capacity/reserved for resource {}: cap={:?} res={:?}",
                            node_name, resource, cap_str, res_str
                        );
                        cap_str.clone()
                    }
                }
            } else {
                cap_str.clone()
            };
            allocatable.insert(resource.clone(), result_str);
        }

        // Only write if allocatable actually changed or was previously unset.
        let current = node.status.as_ref().and_then(|s| s.allocatable.as_ref());
        if current.map(|c| c == &allocatable).unwrap_or(false) {
            return Ok(()); // already up to date
        }

        let key = build_key("nodes", None, node_name);
        let mut updated_node: Node = self.storage.get(&key).await?;
        if let Some(ref mut status) = updated_node.status {
            status.allocatable = Some(allocatable);
            self.storage.update(&key, &updated_node).await?;
            debug!("Computed allocatable for node {}", node_name);
        }
        Ok(())
    }

    /// Evict all pods from a failed node
    async fn evict_pods_from_node(&self, node_name: &str) -> Result<()> {
        info!("Evicting pods from node {}", node_name);

        // List all pods across all namespaces
        let pods: Vec<Pod> = self.storage.list("/registry/pods/").await?;

        // Filter pods running on this node
        let pods_on_node: Vec<&Pod> = pods
            .iter()
            .filter(|pod| {
                pod.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_ref())
                    .map(|n| n == node_name)
                    .unwrap_or(false)
            })
            .collect();

        info!("Found {} pods on node {}", pods_on_node.len(), node_name);

        // Delete each pod
        for pod in pods_on_node {
            let namespace = pod
                .metadata
                .namespace
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Pod has no namespace"))?;
            let pod_name = &pod.metadata.name;

            let pod_key = build_key("pods", Some(namespace), pod_name);

            match self.storage.delete(&pod_key).await {
                Ok(_) => {
                    info!(
                        "Evicted pod {}/{} from node {}",
                        namespace, pod_name, node_name
                    );
                }
                Err(rusternetes_common::Error::NotFound(_)) => {
                    // Pod already deleted
                    debug!("Pod {}/{} already deleted", namespace, pod_name);
                }
                Err(e) => {
                    warn!("Failed to evict pod {}/{}: {}", namespace, pod_name, e);
                }
            }
        }

        Ok(())
    }

    /// Mark a pod as failed due to node failure
    #[allow(dead_code)]
    async fn mark_pod_failed(&self, namespace: &str, pod_name: &str, reason: &str) -> Result<()> {
        let pod_key = build_key("pods", Some(namespace), pod_name);

        let mut pod: Pod = match self.storage.get(&pod_key).await {
            Ok(p) => p,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        // Initialize status if needed
        if pod.status.is_none() {
            pod.status = Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            });
        }

        let status = pod.status.as_mut().unwrap();
        status.phase = Some(Phase::Failed);
        status.reason = Some(reason.to_string());
        status.message = Some(format!("Node {} is not ready", reason));

        // Update pod
        self.storage.update(&pod_key, &pod).await?;

        Ok(())
    }
}

/// Parse a comma-separated `resource=quantity` list (the format used by the
/// kubelet `--kube-reserved` / `--system-reserved` flags and mirrored in the
/// `node.alpha.kubernetes.io/kube-reserved` annotation).
///
/// Returns a map of resource name → quantity string.  Malformed entries are
/// silently skipped (upstream ignores unknown resources gracefully).
fn parse_resource_list(input: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for entry in input.split(',') {
        let entry = entry.trim();
        if let Some((k, v)) = entry.split_once('=') {
            let k = k.trim();
            let v = v.trim();
            if !k.is_empty() && !v.is_empty() {
                map.insert(k.to_string(), v.to_string());
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_node_controller_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let _controller = NodeController::new(storage);
    }

    fn ready_node(name: &str, status: &str) -> Node {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name},
            "spec": {},
            "status": {"conditions": [{"type": "Ready", "status": status}]}
        }))
        .unwrap()
    }

    fn pod_on(node: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"nodeName": node, "containers": [{"name": "c", "image": "i"}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}
        }))
        .unwrap()
    }

    /// processPod (node_lifecycle_controller.go:1153-1158): a pod on a node
    /// whose Ready != True gets its Ready condition set False.
    #[tokio::test]
    async fn process_pod_marks_pod_not_ready_on_unready_node() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        storage
            .create(
                &build_key("nodes", None, "n1"),
                &ready_node("n1", "Unknown"),
            )
            .await
            .unwrap();
        let pk = build_key("pods", Some("default"), "p");
        storage.create(&pk, &pod_on("n1")).await.unwrap();
        c.process_pod("pods/default/p").await.unwrap();
        let got: Pod = storage.get(&pk).await.unwrap();
        let ready = got
            .status
            .unwrap()
            .conditions
            .unwrap()
            .into_iter()
            .find(|c| c.condition_type == "Ready")
            .unwrap();
        assert_eq!(ready.status, "False");
    }

    /// Ready node: the pod is left alone (:1153 guard).
    #[tokio::test]
    async fn process_pod_leaves_pod_on_ready_node() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        storage
            .create(&build_key("nodes", None, "n1"), &ready_node("n1", "True"))
            .await
            .unwrap();
        let pk = build_key("pods", Some("default"), "p");
        storage.create(&pk, &pod_on("n1")).await.unwrap();
        c.process_pod("pods/default/p").await.unwrap();
        let got: Pod = storage.get(&pk).await.unwrap();
        assert_eq!(got.status.unwrap().conditions.unwrap()[0].status, "True");
    }

    /// The health monitor is its own pass over all nodes (:506-512), not a
    /// per-node queue step.
    #[tokio::test]
    async fn monitor_node_health_flips_stale_node_unready() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        storage
            .create(&build_key("nodes", None, "n1"), &ready_node("n1", "True"))
            .await
            .unwrap();
        c.monitor_node_health().await.unwrap();
        // The first pass only records the probe (:885-891); no heartbeat for
        // longer than nodeMonitorGracePeriod then posts Unknown, not False.
        c.advance_clock_for_test(Duration::hours(1));
        c.monitor_node_health().await.unwrap();
        let got: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        assert_eq!(cond_of(&got, "Ready").unwrap().status, "Unknown");
    }

    // ------------------------------------------------------------------
    // tryUpdateNodeHealth (#2836). Ported from
    // pkg/controller/nodelifecycle/node_lifecycle_controller_test.go:
    // TestMonitorNodeHealthUpdateStatus (:1010).
    // ------------------------------------------------------------------

    fn cond_of(n: &Node, t: &str) -> Option<NodeCondition> {
        n.status
            .as_ref()?
            .conditions
            .as_ref()?
            .iter()
            .find(|c| c.condition_type == t)
            .cloned()
    }

    /// "Node created long time ago, with status updated by kubelet exceeds grace
    /// period. Expect Unknown status posted from node controller": Ready goes
    /// Unknown/NodeStatusUnknown (never False), and the pressure conditions
    /// the kubelet never posted are created Unknown/NodeStatusNeverUpdated.
    #[tokio::test]
    async fn stale_heartbeat_posts_ready_unknown_not_false() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        let n = znode("n1", "zone1", "True", 0);
        put(&storage, &c, &n).await;
        c.monitor_node_health().await.unwrap();
        // timeToPass (:1129, :1252): the heartbeat does not move for an hour.
        c.advance_clock_for_test(Duration::hours(1));
        c.monitor_node_health().await.unwrap();
        let got: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        let ready = cond_of(&got, "Ready").unwrap();
        assert_eq!(ready.status, "Unknown");
        assert_eq!(ready.reason.as_deref(), Some("NodeStatusUnknown"));
        assert_eq!(
            ready.message.as_deref(),
            Some("Kubelet stopped posting node status.")
        );
        for t in ["MemoryPressure", "DiskPressure", "PIDPressure"] {
            let cond = cond_of(&got, t).unwrap_or_else(|| panic!("{t} not created"));
            assert_eq!(cond.status, "Unknown");
            assert_eq!(cond.reason.as_deref(), Some("NodeStatusNeverUpdated"));
            assert_eq!(
                cond.message.as_deref(),
                Some("Kubelet never posted node status.")
            );
        }
    }

    /// "Node created long time ago, without status: Expect Unknown status posted
    /// from node controller" (reason NodeStatusNeverUpdated on all four).
    #[tokio::test]
    async fn never_posted_status_posts_unknown_never_updated() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        let mut n = znode("n1", "zone1", "True", 0);
        n.status = None;
        n.metadata.creation_timestamp = Some(Utc::now() - Duration::days(365));
        put(&storage, &c, &n).await;
        c.monitor_node_health().await.unwrap();
        let got: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        for t in ["Ready", "MemoryPressure", "DiskPressure", "PIDPressure"] {
            let cond = cond_of(&got, t).unwrap_or_else(|| panic!("{t} not created"));
            assert_eq!(cond.status, "Unknown", "{t}");
            assert_eq!(
                cond.reason.as_deref(),
                Some("NodeStatusNeverUpdated"),
                "{t}"
            );
        }
    }

    /// "Node created recently, without status. Expect no action from node
    /// controller (within startup grace period)" and "status updated recently:
    /// no action (within monitor grace period)" (:1081-1098, :1195-1227).
    #[tokio::test]
    async fn within_grace_period_takes_no_action() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        let mut fresh_no_status = znode("n0", "zone1", "True", 0);
        fresh_no_status.status = None;
        fresh_no_status.metadata.creation_timestamp = Some(Utc::now());
        put(&storage, &c, &fresh_no_status).await;
        put(&storage, &c, &znode("n1", "zone1", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(30));
        c.monitor_node_health().await.unwrap();
        let n0: Node = storage.get(&build_key("nodes", None, "n0")).await.unwrap();
        assert!(n0.status.is_none(), "startup grace is 60s");
        let n1: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        assert_eq!(cond_of(&n1, "Ready").unwrap().status, "True");
        // ... and past the 60s startup grace the never-posted node goes Unknown.
        c.advance_clock_for_test(Duration::seconds(31));
        c.monitor_node_health().await.unwrap();
        let n0: Node = storage.get(&build_key("nodes", None, "n0")).await.unwrap();
        assert_eq!(cond_of(&n0, "Ready").unwrap().status, "Unknown");
    }

    /// A moving heartbeat advances `probeTimestamp` (:907-927), so a node whose
    /// total silence never exceeds the grace period stays Ready.
    #[tokio::test]
    async fn heartbeat_renewal_resets_the_probe() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        put(&storage, &c, &znode("n1", "zone1", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(30));
        let mut n = znode("n1", "zone1", "True", 0);
        n.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].last_heartbeat_time =
            Some(Utc::now() + Duration::seconds(30));
        put(&storage, &c, &n).await;
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(30));
        c.monitor_node_health().await.unwrap();
        let got: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        assert_eq!(cond_of(&got, "Ready").unwrap().status, "True");
        c.advance_clock_for_test(Duration::seconds(11));
        c.monitor_node_health().await.unwrap();
        let got: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        assert_eq!(cond_of(&got, "Ready").unwrap().status, "Unknown");
    }

    fn node_lease(name: &str, renew: DateTime<Utc>) -> Lease {
        Lease::new(name, "kube-node-lease").with_spec(rusternetes_common::resources::LeaseSpec {
            holder_identity: Some(name.to_string()),
            lease_duration_seconds: Some(40),
            acquire_time: Some(renew),
            renew_time: Some(renew),
            lease_transitions: Some(0),
            preferred_holder: None,
            strategy: None,
        })
    }

    /// TestMonitorNodeHealthUpdateNodeAndPodStatusWithLease: a renewed Lease
    /// advances the probe even if the Ready heartbeat never moves (:928-936),
    /// and the controller itself must not renew it.
    #[tokio::test]
    async fn lease_renewal_resets_the_probe() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        put(&storage, &c, &znode("n1", "zone1", "True", 0)).await;
        let lk = build_key("leases", Some("kube-node-lease"), "n1");
        storage
            .create(&lk, &node_lease("n1", Utc::now()))
            .await
            .unwrap();
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(30));
        storage
            .update(&lk, &node_lease("n1", Utc::now() + Duration::seconds(30)))
            .await
            .unwrap();
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::seconds(30));
        c.monitor_node_health().await.unwrap();
        let got: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        assert_eq!(
            cond_of(&got, "Ready").unwrap().status,
            "True",
            "a renewed Lease keeps the node alive despite a stale heartbeat"
        );
        let lease: Lease = storage.get(&lk).await.unwrap();
        assert!(
            lease.spec.unwrap().renew_time.unwrap() > Utc::now() + Duration::seconds(29),
            "the controller must not renew the kubelet's Lease"
        );
        // The lease stops: the node goes Unknown once the grace period passes.
        c.advance_clock_for_test(Duration::seconds(41));
        c.monitor_node_health().await.unwrap();
        let got: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        assert_eq!(cond_of(&got, "Ready").unwrap().status, "Unknown");
    }

    /// Once Unknown has been posted the node is not rewritten every pass:
    /// `currentCondition.Status != Unknown` guards the transition time (:967-972).
    #[tokio::test]
    async fn unknown_is_posted_once() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        put(&storage, &c, &znode("n1", "zone1", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::hours(1));
        c.monitor_node_health().await.unwrap();
        let first: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        c.advance_clock_for_test(Duration::hours(1));
        c.monitor_node_health().await.unwrap();
        let second: Node = storage.get(&build_key("nodes", None, "n1")).await.unwrap();
        assert_eq!(
            cond_of(&first, "Ready").unwrap().last_transition_time,
            cond_of(&second, "Ready").unwrap().last_transition_time
        );
    }

    /// Exiting master disruption resets `probeTimestamp` and
    /// `readyTransitionTimestamp` on every node (:1058-1065, #2837).
    #[tokio::test]
    async fn exiting_master_disruption_resets_probe_timestamps() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        put(&storage, &c, &znode("a", "zone1", "True", 0)).await;
        put(&storage, &c, &znode("b", "zone1", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        c.advance_clock_for_test(Duration::hours(1));
        c.monitor_node_health().await.unwrap();
        let a_probe_in_disruption = c.health_map.lock().unwrap()["a"].probe_timestamp;
        c.advance_clock_for_test(Duration::minutes(10));
        // b is back: some Nodes are Ready, so master disruption ends.
        let mut b = znode("b", "zone1", "True", 0);
        b.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].last_heartbeat_time =
            Some(Utc::now() + Duration::hours(2));
        put(&storage, &c, &b).await;
        c.monitor_node_health().await.unwrap();
        let hm = c.health_map.lock().unwrap();
        assert!(hm["a"].probe_timestamp >= a_probe_in_disruption + Duration::minutes(10));
        assert!(
            hm["a"].ready_transition_timestamp >= a_probe_in_disruption + Duration::minutes(10)
        );
    }

    // ------------------------------------------------------------------
    // Zone-aware rate-limited NoExecute tainting (#2627). Ported from
    // pkg/controller/nodelifecycle/node_lifecycle_controller_test.go:
    // TestApplyNoExecuteTaints (:2289), TestApplyNoExecuteTaintsToNodesEnqueueTwice
    // (:2712), TestSwapUnreachableNotReadyTaints (:2948). Upstream's
    // newNodeLifecycleControllerFromClient passes `testRateLimiterQPS =
    // 100000` for both limiters (:58); `fast_config` does the same.
    // ------------------------------------------------------------------

    const SECS_STALE: i64 = 3600;

    fn fast_config() -> EvictionConfig {
        EvictionConfig {
            eviction_limiter_qps: 100_000.0,
            secondary_eviction_limiter_qps: 100_000.0,
            large_cluster_threshold: 50,
            unhealthy_zone_threshold: 0.55,
        }
    }

    /// A node in `zone1` with the given Ready status and heartbeat age.
    fn znode(name: &str, zone: &str, status: &str, heartbeat_age_secs: i64) -> Node {
        let hb = (Utc::now() - Duration::seconds(heartbeat_age_secs)).to_rfc3339();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name, "uid": format!("uid-{name}"), "labels": {
                "topology.kubernetes.io/region": "region1",
                "topology.kubernetes.io/zone": zone,
                "failure-domain.beta.kubernetes.io/region": "region1",
                "failure-domain.beta.kubernetes.io/zone": zone,
            }},
            "spec": {},
            "status": {"conditions": [{
                "type": "Ready", "status": status,
                "lastHeartbeatTime": hb, "lastTransitionTime": hb,
            }]}
        }))
        .unwrap()
    }

    async fn put(storage: &Arc<MemoryStorage>, c: &NodeController<MemoryStorage>, n: &Node) {
        let key = build_key("nodes", None, &n.metadata.name);
        if storage.get::<Node>(&key).await.is_ok() {
            storage.update(&key, n).await.unwrap();
        } else {
            storage.create(&key, n).await.unwrap();
        }
        c.seed_first_seen_for_test(&n.metadata.name);
    }

    async fn taint_keys(storage: &Arc<MemoryStorage>, name: &str) -> Vec<(String, String)> {
        let n: Node = storage.get(&build_key("nodes", None, name)).await.unwrap();
        n.spec
            .and_then(|s| s.taints)
            .unwrap_or_default()
            .into_iter()
            .map(|t| (t.key, t.effect))
            .collect()
    }

    fn noexec(key: &str) -> (String, String) {
        (key.to_string(), "NoExecute".to_string())
    }

    /// TestApplyNoExecuteTaints: Unknown -> unreachable, False -> not-ready,
    /// Ready -> no taint, all NoExecute with `timeAdded` stamped.
    #[tokio::test]
    async fn apply_no_execute_taints() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(
            &storage,
            &c,
            &znode("node0", "zone1", "Unknown", SECS_STALE),
        )
        .await;
        // "we need second healthy node in tests" (all-NotReady stops evictions).
        put(&storage, &c, &znode("node1", "zone1", "True", 0)).await;
        put(&storage, &c, &znode("node2", "zone1", "False", SECS_STALE)).await;

        c.monitor_node_health().await.unwrap();
        c.do_no_execute_tainting_pass().await;

        assert_eq!(
            taint_keys(&storage, "node0").await,
            [noexec("node.kubernetes.io/unreachable")]
        );
        assert!(taint_keys(&storage, "node1").await.is_empty());
        assert_eq!(
            taint_keys(&storage, "node2").await,
            [noexec("node.kubernetes.io/not-ready")]
        );
        let n0: Node = storage
            .get(&build_key("nodes", None, "node0"))
            .await
            .unwrap();
        assert!(n0.spec.unwrap().taints.unwrap()[0].time_added.is_some());
    }

    /// TestApplyNoExecuteTaintsToNodesEnqueueTwice: a node queued twice, then
    /// healthy again, must not wedge the queue ("the taint job never stuck").
    #[tokio::test]
    async fn apply_no_execute_taints_to_nodes_enqueue_twice() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(
            &storage,
            &c,
            &znode("node0", "zone1", "Unknown", SECS_STALE),
        )
        .await;
        put(&storage, &c, &znode("node1", "zone1", "True", 0)).await;

        // 1. monitor node health twice, add untainted node once
        c.monitor_node_health().await.unwrap();
        c.monitor_node_health().await.unwrap();

        // 2. mark node0 healthy
        put(&storage, &c, &znode("node0", "zone1", "True", 0)).await;
        // add other notReady nodes
        put(
            &storage,
            &c,
            &znode("node3", "zone1", "Unknown", SECS_STALE),
        )
        .await;
        put(&storage, &c, &znode("node4", "zone1", "True", 0)).await;
        put(&storage, &c, &znode("node5", "zone1", "False", SECS_STALE)).await;

        // 3. monitor again
        c.monitor_node_health().await.unwrap();
        // 4. do NoExecute taint pass
        c.do_no_execute_tainting_pass().await;

        assert!(taint_keys(&storage, "node0").await.is_empty());
        assert_eq!(
            taint_keys(&storage, "node3").await,
            [noexec("node.kubernetes.io/unreachable")]
        );
        assert_eq!(
            taint_keys(&storage, "node5").await,
            [noexec("node.kubernetes.io/not-ready")]
        );
    }

    /// TestSwapUnreachableNotReadyTaints: Unknown -> unreachable; when the
    /// condition turns False the unreachable taint is swapped for not-ready
    /// "straight away" (processTaintBaseEviction :783-786).
    #[tokio::test]
    async fn swap_unreachable_not_ready_taints() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        put(
            &storage,
            &c,
            &znode("node0", "zone1", "Unknown", SECS_STALE),
        )
        .await;
        put(&storage, &c, &znode("node1", "zone1", "True", 0)).await;

        c.monitor_node_health().await.unwrap();
        c.do_no_execute_tainting_pass().await;
        assert_eq!(
            taint_keys(&storage, "node0").await,
            [noexec("node.kubernetes.io/unreachable")]
        );

        // node0 now reports NotReady (False); keep its taint, swap the status.
        let mut n0: Node = storage
            .get(&build_key("nodes", None, "node0"))
            .await
            .unwrap();
        n0.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].status = "False".into();
        storage
            .update(&build_key("nodes", None, "node0"), &n0)
            .await
            .unwrap();

        c.monitor_node_health().await.unwrap();
        c.do_no_execute_tainting_pass().await;
        assert_eq!(
            taint_keys(&storage, "node0").await,
            [noexec("node.kubernetes.io/not-ready")]
        );
    }

    async fn zone_qps(c: &NodeController<MemoryStorage>, zone: &str) -> f32 {
        let q = c
            .evictor
            .lock()
            .await
            .zone_no_execute_tainter
            .get(&format!("region1:\0:{zone}"))
            .cloned()
            .expect("zone queue");
        q.limiter_qps().await
    }

    /// handleDisruption (:996-1085): PartialDisruption in a large cluster uses
    /// the secondary rate; recovery returns to the primary rate.
    #[tokio::test]
    async fn partial_disruption_in_large_cluster_uses_secondary_qps_then_recovers() {
        let storage = Arc::new(MemoryStorage::new());
        let cfg = EvictionConfig {
            large_cluster_threshold: 3,
            ..EvictionConfig::default()
        };
        let c = NodeController::with_eviction_config(storage.clone(), cfg);
        put(&storage, &c, &znode("ok", "zone1", "True", 0)).await;
        for n in ["b1", "b2", "b3", "b4"] {
            put(&storage, &c, &znode(n, "zone1", "False", SECS_STALE)).await;
        }
        c.monitor_node_health().await.unwrap();
        // 5 nodes > threshold 3, 4/5 unhealthy: ReducedQPSFunc -> secondary.
        assert_eq!(zone_qps(&c, "zone1").await, 0.01);

        for n in ["b1", "b2", "b3", "b4"] {
            put(&storage, &c, &znode(n, "zone1", "True", 0)).await;
        }
        c.monitor_node_health().await.unwrap();
        assert_eq!(zone_qps(&c, "zone1").await, 0.1);
    }

    /// Entering master disruption removes taints and stops evictions
    /// (:1039-1056); exiting it restores per-zone limiters and restarts the
    /// nodes' grace clocks (:1058-1074).
    #[tokio::test]
    async fn master_disruption_clears_taints_and_stops_then_resumes() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(
            storage.clone(),
            EvictionConfig {
                eviction_limiter_qps: 100_000.0,
                ..EvictionConfig::default()
            },
        );
        put(&storage, &c, &znode("a", "zone1", "Unknown", SECS_STALE)).await;
        put(&storage, &c, &znode("b", "zone1", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        c.do_no_execute_tainting_pass().await;
        assert_eq!(
            taint_keys(&storage, "a").await,
            [noexec("node.kubernetes.io/unreachable")]
        );

        // b goes stale too: every node NotReady -> master disruption.
        c.advance_clock_for_test(Duration::hours(1));
        c.monitor_node_health().await.unwrap();
        assert!(
            taint_keys(&storage, "a").await.is_empty(),
            "entering master disruption removes the taints"
        );
        assert_eq!(
            c.evictor
                .lock()
                .await
                .zone_states
                .values()
                .copied()
                .collect::<Vec<_>>(),
            [ZoneState::FullDisruption]
        );
        c.do_no_execute_tainting_pass().await;
        assert!(taint_keys(&storage, "a").await.is_empty());
        assert_ne!(zone_qps(&c, "zone1").await, 100_000.0);

        // b recovers: exiting full disruption resets the limiter for the zone.
        put(&storage, &c, &znode("b", "zone1", "True", -5)).await;
        c.monitor_node_health().await.unwrap();
        assert_eq!(
            c.evictor
                .lock()
                .await
                .zone_states
                .values()
                .copied()
                .collect::<Vec<_>>(),
            [ZoneState::Normal]
        );
        assert_eq!(zone_qps(&c, "zone1").await, 100_000.0);
    }

    /// A zone in FullDisruption while another zone still responds keeps the
    /// normal rate (`enterFullDisruptionFunc = HealthyQPSFunc`, :358); the
    /// healthy zone is unaffected.
    #[tokio::test]
    async fn single_zone_full_disruption_keeps_normal_rate() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::new(storage.clone());
        put(&storage, &c, &znode("z1a", "z1", "False", SECS_STALE)).await;
        put(&storage, &c, &znode("z1b", "z1", "False", SECS_STALE)).await;
        put(&storage, &c, &znode("z2a", "z2", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        assert_eq!(zone_qps(&c, "z1").await, 0.1);
        assert_eq!(zone_qps(&c, "z2").await, 0.1);
        let states = c.evictor.lock().await.zone_states.clone();
        assert_eq!(states["region1:\0:z1"], ZoneState::FullDisruption);
        assert_eq!(states["region1:\0:z2"], ZoneState::Normal);
    }

    /// Nodes labelled `node.kubernetes.io/exclude-disruption` are excluded
    /// from disruption checks (isNodeExcludedFromDisruptionChecks, :817-825).
    #[tokio::test]
    async fn excluded_nodes_do_not_count_towards_disruption() {
        let storage = Arc::new(MemoryStorage::new());
        let c = NodeController::with_eviction_config(storage.clone(), fast_config());
        let mut ex = znode("ex", "zone1", "False", SECS_STALE);
        ex.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(LABEL_NODE_DISRUPTION_EXCLUSION.to_string(), String::new());
        put(&storage, &c, &ex).await;
        put(&storage, &c, &znode("ok", "zone1", "True", 0)).await;
        c.monitor_node_health().await.unwrap();
        assert_eq!(
            c.evictor
                .lock()
                .await
                .zone_states
                .values()
                .copied()
                .collect::<Vec<_>>(),
            [ZoneState::Normal]
        );
    }
}
