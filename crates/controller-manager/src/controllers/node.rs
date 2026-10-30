use crate::controllers::node_lifecycle_metrics;
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
use rusternetes_common::resources::{Lease, Node, NodeCondition, NodeStatus, Pod};
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
pub const NODE_MONITOR_GRACE_PERIOD_SECONDS: i64 = 40;
const POD_EVICTION_TIMEOUT_SECONDS: i64 = 300; // 5 minutes
pub const NODE_STARTUP_GRACE_PERIOD_SECS: u64 = 60;

/// `retrySleepTime = 20 * time.Millisecond` (node_lifecycle_controller.go:128)
/// and `NodeHealthUpdateRetry = 5`
/// (pkg/controller/nodelifecycle/scheduler/rate_limited_queue.go:32): the poll
/// bound of `updateNodeFunc` (:703-717).
const RETRY_SLEEP_TIME: std::time::Duration = std::time::Duration::from_millis(20);
const NODE_HEALTH_UPDATE_RETRY: u32 = 5;

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
pub const LABEL_NODE_DISRUPTION_EXCLUSION: &str = "node.kubernetes.io/exclude-disruption";

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
pub struct Evictor {
    pub zone_states: HashMap<String, ZoneState>,
    /// `zoneNoExecuteTainter`: one rate-limited queue per zone.
    pub zone_no_execute_tainter: HashMap<String, Arc<RateLimitedTimedQueue>>,
    /// `knownNodeSet`: node name -> uid.
    pub known_node_set: HashMap<String, String>,
}

/// What `monitor_node` observed for one node, feeding `handleDisruption`.
struct NodeObservation {
    zone: String,
    ready: bool,
    excluded_from_disruption: bool,
}

pub struct NodeController<S: Storage> {
    pub storage: Arc<S>,
    pub observed_conditions: Arc<std::sync::Mutex<ObservedConditions>>,
    pub eviction: EvictionConfig,
    pub evictor: Arc<tokio::sync::Mutex<Evictor>>,
    /// Test-only shift of the controller's clock (`nc.now`, upstream's
    /// injectable `now func() metav1.Time`).
    pub clock_offset: Arc<std::sync::Mutex<Duration>>,
    /// `nodeHealthMap` (node_lifecycle_controller.go:181-216): what the
    /// controller last observed of each node, on ITS clock.
    pub node_health: Arc<std::sync::Mutex<HashMap<String, NodeHealthData>>>,
    /// Nodes `seed_first_seen_for_test` declared long-observed.
    pub backdated: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// `nodesToRetry` (:242): nodes whose Ready -> NotReady transition could
    /// not finish MarkPodsNotReady and must be retried on the next pass.
    pub nodes_to_retry: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

/// `nodeHealthData` (node_lifecycle_controller.go:168-173). `status` is kept
/// as the Ready condition saved from it, the only part `tryUpdateNodeHealth`
/// reads back; `lease` as the saved Lease's `spec.renewTime`.
#[derive(Clone)]
pub struct NodeHealthData {
    pub probe_timestamp: DateTime<Utc>,
    pub ready_transition_timestamp: DateTime<Utc>,
    pub status_ready: Option<NodeCondition>,
    pub lease: Option<LeaseObservation>,
}

/// A saved coordination Lease (`nodeHealthData.lease`).
#[derive(Clone, Copy)]
pub struct LeaseObservation {
    renew_time: Option<DateTime<Utc>>,
}

impl LeaseObservation {
    /// `savedLease.Spec.RenewTime.Before(observedLease.Spec.RenewTime)`; an
    /// unset renewTime is the zero time.
    fn renewed_before(&self, observed: &LeaseObservation) -> bool {
        match (self.renew_time, observed.renew_time) {
            (Some(a), Some(b)) => a < b,
            (None, Some(_)) => true,
            _ => false,
        }
    }
}

/// The `switch` of updateNodeFunc (node_lifecycle_controller.go:752-763):
/// MarkPodsNotReady runs on the Ready -> not-Ready transition (the first case
/// `fallthrough`s into the second's body) or when a previous attempt failed
/// (`nodesToRetry`) and the observed Ready is not True.
pub fn should_mark_pods_not_ready(current: &str, observed: &str, needs_retry: bool) -> bool {
    (current != "True" && observed == "True") || (needs_retry && observed != "True")
}

/// `apiequality.Semantic.DeepEqual` over two conditions.
fn condition_eq(a: &NodeCondition, b: &NodeCondition) -> bool {
    a.condition_type == b.condition_type
        && a.status == b.status
        && a.reason == b.reason
        && a.message == b.message
        && a.last_heartbeat_time == b.last_heartbeat_time
        && a.last_transition_time == b.last_transition_time
}

pub fn ready_condition_of(node: &Node) -> Option<&NodeCondition> {
    node.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.condition_type == "Ready")
}

/// The stale branch of `tryUpdateNodeHealth` (:937-975): Ready and the three
/// pressure conditions go Unknown; a condition the kubelet never posted is
/// appended as `NodeStatusNeverUpdated`. `NetworkUnavailable` is left to the
/// control plane (:944).
fn apply_status_unknown(
    status: &mut NodeStatus,
    creation: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) {
    let conditions = status.conditions.get_or_insert_with(Vec::new);
    for t in ["Ready", "MemoryPressure", "DiskPressure", "PIDPressure"] {
        match conditions.iter_mut().find(|c| c.condition_type == t) {
            None => conditions.push(NodeCondition {
                condition_type: t.to_string(),
                status: "Unknown".to_string(),
                reason: Some("NodeStatusNeverUpdated".to_string()),
                message: Some("Kubelet never posted node status.".to_string()),
                last_heartbeat_time: creation,
                last_transition_time: Some(now),
            }),
            Some(c) if c.status != "Unknown" => {
                c.status = "Unknown".to_string();
                c.reason = Some("NodeStatusUnknown".to_string());
                c.message = Some("Kubelet stopped posting node status.".to_string());
                c.last_transition_time = Some(now);
            }
            Some(_) => {}
        }
    }
}

impl<S: Storage + 'static> NodeController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self::with_eviction_config(storage, EvictionConfig::default())
    }

    pub fn with_eviction_config(storage: Arc<S>, eviction: EvictionConfig) -> Self {
        Self {
            storage,
            observed_conditions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            eviction,
            evictor: Arc::new(tokio::sync::Mutex::new(Evictor::default())),
            clock_offset: Arc::new(std::sync::Mutex::new(Duration::zero())),
            node_health: Arc::new(std::sync::Mutex::new(HashMap::new())),
            backdated: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            nodes_to_retry: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// `nc.now()`: the controller's clock.
    fn now(&self) -> DateTime<Utc> {
        Utc::now() + *self.clock_offset.lock().unwrap()
    }

    /// Test helper: move the controller's clock forward, the way upstream's
    /// tests advance `fakeNow`.
    #[doc(hidden)]
    pub fn advance_clock_for_test(&self, by: Duration) {
        *self.clock_offset.lock().unwrap() += by;
    }

    /// Test helper: mark `node_name` as observed long enough ago that the
    /// nodeHealthMap startup grace period is over, so tests observe the
    /// Ready-flip behavior deterministically without sleeping.
    #[doc(hidden)]
    pub fn seed_first_seen_for_test(&self, node_name: &str) {
        // The controller has been watching this node all along: its first
        // `tryUpdateNodeHealth` entry dates from the heartbeat, not from now.
        self.backdated.lock().unwrap().insert(node_name.to_string());
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
        self.mark_pod_not_ready(&pod_key, &mut pod).await
    }

    /// One pod's share of `MarkPodsNotReady` (controller_utils.go:121-155):
    /// `UpdatePodCondition` only writes when the Ready status actually
    /// changes; NotFound means the pod is already gone.
    async fn mark_pod_not_ready(&self, pod_key: &str, pod: &mut Pod) -> Result<()> {
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
        match self.storage.update(pod_key, &*pod).await {
            Ok(_) | Err(rusternetes_common::Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// `controllerutil.MarkPodsNotReady` (controller_utils.go:121-155): pods
    /// are tried one by one and failures are aggregated, not short-circuited.
    async fn mark_pods_not_ready(&self, pods: Vec<Pod>, node_name: &str) -> Result<()> {
        let mut errs: Vec<String> = Vec::new();
        for mut pod in pods {
            // Defensive check, also needed for tests.
            if pod.spec.as_ref().and_then(|s| s.node_name.as_deref()) != Some(node_name) {
                continue;
            }
            let Some(ns) = pod.metadata.namespace.clone() else {
                continue;
            };
            let key = build_key("pods", Some(&ns), &pod.metadata.name);
            if let Err(e) = self.mark_pod_not_ready(&key, &mut pod).await {
                warn!(
                    "Failed to update status for pod {}/{}: {}",
                    ns, pod.metadata.name, e
                );
                errs.push(e.to_string());
            }
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(rusternetes_common::Error::Internal(errs.join("; ")).into())
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

    /// `monitorNodeHealth` (node_lifecycle_controller.go:670-778): one pass over
    /// all nodes, run from its own loop. Registers new nodes/zones
    /// (`classifyNodes`, :1178), fans the per-node work out with
    /// `workqueue.ParallelizeUntil(ctx, nc.nodeUpdateWorkerSize, ...)` (:774;
    /// same bound here), then runs `handleDisruption` (:776).
    pub async fn monitor_node_health(&self) -> Result<()> {
        let started = std::time::Instant::now();
        let result = self.monitor_node_health_pass().await;
        // `defer updateAllNodesHealthDuration.Observe(..)` (:672-674).
        node_lifecycle_metrics::observe_update_all_nodes_health(started.elapsed().as_secs_f64());
        result
    }

    async fn monitor_node_health_pass(&self) -> Result<()> {
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
        let started = std::time::Instant::now();
        let result = self.monitor_node(node).await;
        // `defer updateNodeHealthDuration.Observe(..)` (:699-701).
        node_lifecycle_metrics::observe_update_node_health(started.elapsed().as_secs_f64());
        match result {
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
            // :430 `nc.nodesToRetry.Delete(node.Name)`
            self.nodes_to_retry.lock().unwrap().remove(&name);
        }
    }

    /// `addPodEvictorForNewZone` (:1224): create the zone's tainter queue with
    /// the default `evictionLimiterQPS` limiter.
    async fn add_pod_evictor_for_new_zone(&self, node: &Node) {
        let zone = get_zone_key(node);
        let mut ev = self.evictor.lock().await;
        if !ev.zone_states.contains_key(&zone) {
            ev.zone_states.insert(zone.clone(), ZoneState::Initial);
            // Init the metric for the new zone (:1233-1235).
            node_lifecycle_metrics::init_evictions(&zone);
            ev.zone_no_execute_tainter.insert(
                zone,
                Arc::new(RateLimitedTimedQueue::new(RateLimiter::token_bucket(
                    self.eviction.eviction_limiter_qps,
                    EVICTION_RATE_LIMITER_BURST,
                ))),
            );
        }
    }

    /// Health half of the old fused reconcile: readiness, Ready condition,
    /// condition transitions, then `processTaintBaseEviction`. Returns the
    /// observation `handleDisruption` needs, or `None` while the node is
    /// inside the startup grace period.
    async fn monitor_node(&self, node: &Node) -> Result<Option<NodeObservation>> {
        let node_name = &node.metadata.name;

        // tryUpdateNodeHealth (node_lifecycle_controller.go:830-935), retried
        // like updateNodeFunc (:703-717): wait.PollImmediate(retrySleepTime,
        // retrySleepTime*scheduler.NodeHealthUpdateRetry), re-reading the node
        // after a failure.
        let mut node = node.clone();
        let mut tries = 0;
        let (observed_ready, current_ready) = loop {
            match self.try_update_node_health(&node).await {
                Ok((_grace, observed, current)) => break (observed, current),
                Err(e) => {
                    tries += 1;
                    if tries >= NODE_HEALTH_UPDATE_RETRY {
                        return Err(e);
                    }
                    tokio::time::sleep(RETRY_SLEEP_TIME).await;
                    node = self
                        .storage
                        .get(&build_key("nodes", None, node_name))
                        .await?;
                }
            }
        };
        let node = &node;

        // The Ready status after this pass.
        let ready_status = current_ready
            .as_ref()
            .map_or("Unknown", |c| c.status.as_str())
            .to_string();
        let ready_status = ready_status.as_str();
        let is_ready = ready_status == "True";

        // Refresh lastTransitionTime on any non-Ready condition that flipped
        // status without its reporter (kubelet eviction manager) bumping the
        // timestamp. Mirrors upstream pkg/controller/nodelifecycle, which keeps
        // the pressure-condition transition times observable.
        self.reconcile_condition_transitions(node).await?;

        // updateNodeFunc :733-766. Not-ready/unreachable NoExecute taints are
        // NOT applied here: like upstream they go through the zone's
        // rate-limited queue (`processTaintBaseEviction`, :781) and are drained
        // by `do_no_execute_tainting_pass`.
        if current_ready.is_some() {
            let transitioned = ready_status != "True" && observed_ready.status == "True";
            let pods = match self.pods_on_node(node_name).await {
                Ok(p) => p,
                Err(e) => {
                    error!("Unable to list pods of node {}: {}", node_name, e);
                    if transitioned {
                        // If error happened during node status transition
                        // (Ready -> NotReady) we need to mark node for retry to
                        // force MarkPodsNotReady execution in the next
                        // iteration (:743-747).
                        self.nodes_to_retry
                            .lock()
                            .unwrap()
                            .insert(node_name.clone());
                    }
                    return Ok(Some(self.observation(node, ready_status)));
                }
            };
            self.process_taint_base_eviction(node, ready_status).await;

            let needs_retry = self.nodes_to_retry.lock().unwrap().contains(node_name);
            if should_mark_pods_not_ready(ready_status, &observed_ready.status, needs_retry) {
                if transitioned {
                    // Report node event only once when status changed.
                    info!("Node {} event: NodeNotReady", node_name);
                }
                if let Err(e) = self.mark_pods_not_ready(pods, node_name).await {
                    error!(
                        "Unable to mark all pods NotReady on node {}; queuing for retry: {}",
                        node_name, e
                    );
                    self.nodes_to_retry
                        .lock()
                        .unwrap()
                        .insert(node_name.clone());
                    return Ok(Some(self.observation(node, ready_status)));
                }
            }
        }
        self.nodes_to_retry.lock().unwrap().remove(node_name);

        // Evict pods from nodes that have been NotReady for too long. Only
        // once the throttled pass has actually tainted the node, so a
        // partition (which the queue holds back or disables) cannot bypass the
        // rate limit through this path.
        if !is_ready
            && self.should_evict_pods(node)
            && self.has_no_execute_not_ready_taint(node_name).await
        {
            info!("Evicting pods from NotReady node {}", node_name);
            self.evict_pods_from_node(node_name).await?;
        }

        Ok(Some(self.observation(node, ready_status)))
    }

    fn observation(&self, node: &Node, ready_status: &str) -> NodeObservation {
        NodeObservation {
            zone: get_zone_key(node),
            ready: ready_status == "True",
            excluded_from_disruption: node
                .metadata
                .labels
                .as_ref()
                .is_some_and(|l| l.contains_key(LABEL_NODE_DISRUPTION_EXCLUSION)),
        }
    }

    /// `nc.getPodsAssignedToNode` (the pod indexer by `spec.nodeName`).
    async fn pods_on_node(&self, node_name: &str) -> Result<Vec<Pod>> {
        let pods: Vec<Pod> = self.storage.list("/registry/pods/").await?;
        Ok(pods
            .into_iter()
            .filter(|p| p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(node_name))
            .collect())
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

    /// `doNoScheduleTaintingPass` (node_lifecycle_controller.go:540-589):
    /// derive the NoSchedule taints the node's conditions call for
    /// (`nodeConditionToTaintKeyStatusMap`, :87-104) plus `unschedulable`,
    /// diff them (`taintutils.TaintSetDiff`) against the NoSchedule taints
    /// the controller owns (`taintKeyToNodeConditionMap`, :106-113; a user's
    /// own NoSchedule taints are left alone) and apply the difference via
    /// `SwapNodeControllerTaint`. This is what clears the
    /// `not-ready:NoSchedule` taint the api-server's NodeTaint admission
    /// stamps at registration once the node is Ready (#3003).
    async fn do_no_schedule_tainting_pass(&self, node: &Node) -> Result<()> {
        // nodeConditionToTaintKeyStatusMap
        fn condition_taint(cond_type: &str, status: &str) -> Option<&'static str> {
            match (cond_type, status) {
                ("Ready", "False") => Some("node.kubernetes.io/not-ready"),
                ("Ready", "Unknown") => Some("node.kubernetes.io/unreachable"),
                ("MemoryPressure", "True") => Some("node.kubernetes.io/memory-pressure"),
                ("DiskPressure", "True") => Some("node.kubernetes.io/disk-pressure"),
                ("NetworkUnavailable", "True") => Some("node.kubernetes.io/network-unavailable"),
                ("PIDPressure", "True") => Some("node.kubernetes.io/pid-pressure"),
                _ => None,
            }
        }
        // taintKeyToNodeConditionMap + the unschedulable key.
        const OWNED: [&str; 7] = [
            "node.kubernetes.io/not-ready",
            "node.kubernetes.io/unreachable",
            "node.kubernetes.io/network-unavailable",
            "node.kubernetes.io/memory-pressure",
            "node.kubernetes.io/disk-pressure",
            "node.kubernetes.io/pid-pressure",
            "node.kubernetes.io/unschedulable",
        ];

        let mut want: Vec<&'static str> = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|cs| {
                cs.iter()
                    .filter_map(|c| condition_taint(&c.condition_type, &c.status))
                    .collect()
            })
            .unwrap_or_default();
        if node.spec.as_ref().and_then(|s| s.unschedulable) == Some(true) {
            want.push("node.kubernetes.io/unschedulable");
        }

        let have: Vec<&str> = node
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_ref())
            .map(|ts| {
                ts.iter()
                    .filter(|t| t.effect == "NoSchedule" && OWNED.contains(&t.key.as_str()))
                    .map(|t| t.key.as_str())
                    .collect()
            })
            .unwrap_or_default();
        let to_add: Vec<&str> = want.iter().copied().filter(|k| !have.contains(k)).collect();
        let to_del: Vec<&str> = have.iter().copied().filter(|k| !want.contains(k)).collect();
        if to_add.is_empty() && to_del.is_empty() {
            return Ok(());
        }

        // SwapNodeControllerTaint: stamp timeAdded, add, then delete.
        let node_key = build_key("nodes", None, &node.metadata.name);
        let mut stored: Node = self.storage.get(&node_key).await?;
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
        for key in &to_add {
            if !taints
                .iter()
                .any(|t| t.key == *key && t.effect == "NoSchedule")
            {
                taints.push(Taint {
                    key: (*key).to_string(),
                    value: None,
                    effect: "NoSchedule".to_string(),
                    time_added: Some(Utc::now()),
                });
            }
        }
        taints.retain(|t| !(t.effect == "NoSchedule" && to_del.contains(&t.key.as_str())));
        if taints.is_empty() {
            spec.taints = None;
        }
        self.storage.update(&node_key, &stored).await?;
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
                    if ok {
                        // Count the number of evictions (:654-658).
                        node_lifecycle_metrics::inc_evictions(&get_zone_key(&node));
                    }
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
            let (unhealthy, new_state) = self.eviction.compute_zone_state(v);
            node_lifecycle_metrics::set_zone_stats(k, v.len(), unhealthy);
            if new_state != ZoneState::FullDisruption {
                all_are_fully_disrupted = false;
            }
            new_zone_states.insert(k.clone(), new_state);
            zone_states.entry(k.clone()).or_insert(ZoneState::Initial);
        }

        // DELIBERATE DEVIATION from node_lifecycle_controller.go:1015-1028.
        // Upstream drops emptied zones and checks `v != stateFullDisruption`
        // in ONE loop over a Go map, `break`ing at the first non-full zone.
        // Go's map order is random, so an emptied zone visited after the
        // break is neither cleared nor deleted; later `newZoneStates[k]`
        // reads then yield Go's zero value silently, but an indexed Rust
        // `HashMap` panics (flaky `emptied_zone_metrics_reset`). Drop the
        // emptied zones in a separate pass first so the outcome is order
        // independent.
        let emptied: Vec<String> = zone_states
            .keys()
            .filter(|k| !zone_to_conditions.contains_key(*k))
            .cloned()
            .collect();
        for k in emptied {
            node_lifecycle_metrics::clear_zone(&k);
            zone_states.remove(&k);
        }

        let mut all_was_fully_disrupted = true;
        for v in zone_states.values() {
            if *v != ZoneState::FullDisruption {
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
                // Nodes (:1059-1065).
                {
                    let now = self.now();
                    let mut health = self.node_health.lock().unwrap();
                    for node in nodes {
                        if let Some(v) = health.get_mut(&node.metadata.name) {
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
    pub async fn process_node(&self, node: &Node) -> Result<()> {
        let node_name = &node.metadata.name;
        // `doNoScheduleTaintingPass` runs ahead of everything else in the
        // worker and is not subject to the startup grace period.
        if let Err(e) = self.do_no_schedule_tainting_pass(node).await {
            error!("Failed to taint NoSchedule on node {}: {}", node_name, e);
        }
        // No further startup-grace gate: after `doNoScheduleTaintingPass`
        // upstream's `doNodeProcessingPassWorker` proceeds directly
        // (node_lifecycle_controller.go:516-540). The only startup grace is
        // the nodeHealthMap one in `tryUpdateNodeHealth` (:849,
        // `gracePeriod = nc.nodeStartupGracePeriod`).

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

        // The node Lease is renewed by the kubelet (pkg/kubelet/nodelease,
        // nodeLeaseController), never by this controller: a controller-side
        // renewal would keep a dead kubelet's Lease fresh forever (#2974).

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
    /// The Ready condition is excluded — its transition time belongs to the
    /// kubelet and `try_update_node_health` — but it is still recorded so a spurious bump is
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

    /// `tryUpdateNodeHealth` (pkg/controller/nodelifecycle/
    /// node_lifecycle_controller.go:830-935): "checks a given node's conditions
    /// and tries to update it. Returns grace period to which given node is
    /// entitled, state of current and last observed Ready Condition, and an
    /// error if it occurred."
    ///
    /// Staleness is judged on THIS controller's clock: `probeTimestamp` is when
    /// the controller last saw the Ready heartbeat or the Lease change, so the
    /// kubelet's own timestamps (and any skew in them) never decide it.
    pub async fn try_update_node_health(
        &self,
        node: &Node,
    ) -> Result<(Duration, NodeCondition, Option<NodeCondition>)> {
        let name = node.metadata.name.clone();
        let mut health = self.node_health.lock().unwrap().get(&name).cloned();
        let result = self.try_update_node_health_inner(node, &mut health).await;
        // `defer nc.nodeHealthMap.set(node.Name, nodeHealth)` (:832-834).
        if let Some(h) = health {
            self.node_health.lock().unwrap().insert(name, h);
        }
        result
    }

    /// The time a node's first health entry is dated: `nc.now()` ("Assuming now
    /// as a timestamp", :896), unless the node was declared long-observed.
    fn first_probe_time(&self, node: &Node, now: DateTime<Utc>) -> DateTime<Utc> {
        if self.backdated.lock().unwrap().contains(&node.metadata.name) {
            let seen = ready_condition_of(node)
                .and_then(|c| c.last_heartbeat_time)
                .or(node.metadata.creation_timestamp);
            if let Some(t) = seen {
                return t.min(now);
            }
        }
        now
    }

    async fn try_update_node_health_inner(
        &self,
        node: &Node,
        node_health: &mut Option<NodeHealthData>,
    ) -> Result<(Duration, NodeCondition, Option<NodeCondition>)> {
        let node_name = &node.metadata.name;
        let creation = node.metadata.creation_timestamp;
        let current_ready: Option<NodeCondition> = ready_condition_of(node).cloned();
        let grace;
        let observed_ready: NodeCondition;
        match &current_ready {
            None => {
                // If ready condition is nil, then kubelet (or nodecontroller)
                // never posted node status. A fake ready condition is created,
                // where LastHeartbeatTime and LastTransitionTime is set to
                // node.CreationTimestamp (:841-860).
                let created = creation.unwrap_or_else(|| self.now());
                observed_ready = NodeCondition {
                    condition_type: "Ready".to_string(),
                    status: "Unknown".to_string(),
                    last_heartbeat_time: Some(created),
                    last_transition_time: Some(created),
                    reason: None,
                    message: None,
                };
                grace = Duration::seconds(NODE_STARTUP_GRACE_PERIOD_SECS as i64);
                match node_health {
                    Some(h) => h.status_ready = None,
                    None => {
                        *node_health = Some(NodeHealthData {
                            probe_timestamp: created,
                            ready_transition_timestamp: created,
                            status_ready: None,
                            lease: None,
                        })
                    }
                }
            }
            Some(c) => {
                observed_ready = c.clone();
                grace = Duration::seconds(NODE_MONITOR_GRACE_PERIOD_SECONDS);
            }
        }

        // The six cases of :862-878.
        let saved_condition = node_health.as_ref().and_then(|h| h.status_ready.clone());
        let saved_lease = node_health.as_ref().and_then(|h| h.lease);
        let now = self.now();
        match (node_health.as_ref(), &saved_condition, &current_ready) {
            (None, _, _) => {
                debug!(
                    "Missing timestamp for Node {}. Assuming now as a timestamp",
                    node_name
                );
                let t = self.first_probe_time(node, now);
                *node_health = Some(NodeHealthData {
                    probe_timestamp: t,
                    ready_transition_timestamp: t,
                    status_ready: current_ready.clone(),
                    lease: None,
                });
            }
            (Some(_), None, Some(_)) => {
                debug!(
                    "Creating timestamp entry for newly observed Node {}",
                    node_name
                );
                let t = self.first_probe_time(node, now);
                *node_health = Some(NodeHealthData {
                    probe_timestamp: t,
                    ready_transition_timestamp: t,
                    status_ready: current_ready.clone(),
                    lease: saved_lease,
                });
            }
            (Some(_), Some(_), None) => {
                error!(
                    "ReadyCondition was removed from Status of Node {}",
                    node_name
                );
                *node_health = Some(NodeHealthData {
                    probe_timestamp: now,
                    ready_transition_timestamp: now,
                    status_ready: None,
                    lease: saved_lease,
                });
            }
            (Some(h), Some(saved), Some(current))
                if saved.last_heartbeat_time != current.last_heartbeat_time =>
            {
                // If ReadyCondition changed since the last time we checked, we
                // update the transition timestamp to "now", otherwise we leave
                // it as it is.
                let transition = if saved.last_transition_time != current.last_transition_time {
                    now
                } else {
                    h.ready_transition_timestamp
                };
                *node_health = Some(NodeHealthData {
                    probe_timestamp: now,
                    ready_transition_timestamp: transition,
                    status_ready: current_ready.clone(),
                    lease: saved_lease,
                });
            }
            _ => {}
        }
        let health = node_health.as_mut().expect("set above");

        // Always update the probe time if node lease is renewed. Note: if
        // kubelet never posted the node status, but continues renewing the
        // heartbeat leases, the node controller will assume the node is healthy
        // and take no action (:921-929).
        let observed_lease: Option<LeaseObservation> = self
            .storage
            .get::<Lease>(&build_key("leases", Some("kube-node-lease"), node_name))
            .await
            .ok()
            .map(|l| LeaseObservation {
                renew_time: l.spec.and_then(|s| s.renew_time),
            });
        if let Some(observed) = observed_lease {
            if saved_lease.is_none_or(|saved| saved.renewed_before(&observed)) {
                health.lease = Some(observed);
                health.probe_timestamp = now;
            }
        }
        let probe_timestamp = health.probe_timestamp;

        if now > probe_timestamp + grace {
            // NodeReady condition or lease was last set longer ago than
            // gracePeriod, so update it to Unknown (regardless of its current
            // value) in the master.
            debug!(
                "Node {} hasn't been updated for {}s",
                node_name,
                (now - probe_timestamp).num_seconds()
            );
            let mut status = node.status.clone().unwrap_or_default();
            apply_status_unknown(&mut status, creation, now);
            let current = status
                .conditions
                .as_ref()
                .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"))
                .cloned();
            if !current
                .as_ref()
                .is_some_and(|c| condition_eq(c, &observed_ready))
            {
                // UpdateStatus (:966); the fresh read stands in for the
                // resourceVersion conflict retry of updateNodeFunc.
                let key = build_key("nodes", None, node_name);
                let mut stored: Node = self.storage.get(&key).await?;
                let st = stored.status.get_or_insert_with(NodeStatus::default);
                apply_status_unknown(st, creation, now);
                let written = st
                    .conditions
                    .as_ref()
                    .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"))
                    .cloned();
                // Status subresource write (#1723).
                self.storage.update_status(&key, &stored).await?;
                info!("Node {} stopped posting status; marked Unknown", node_name);
                *node_health = Some(NodeHealthData {
                    probe_timestamp,
                    ready_transition_timestamp: now,
                    status_ready: written.clone(),
                    lease: observed_lease,
                });
                return Ok((grace, observed_ready, written));
            }
            return Ok((grace, observed_ready, current));
        }
        Ok((grace, observed_ready, current_ready))
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
