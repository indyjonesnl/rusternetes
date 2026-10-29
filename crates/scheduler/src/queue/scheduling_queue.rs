//! The scheduling queue: activeQ + backoffQ + unschedulablePods, with the
//! QueueingHint requeue mechanism and in-flight event bookkeeping.
//!
//! Ported from upstream `pkg/scheduler/backend/queue/`:
//! - `scheduling_queue.go`: `PriorityQueue` (`:154-209`), options (`:226-323`),
//!   `queueingStrategy` (`:415-425`), `isEventOfInterest` (`:428-446`),
//!   `isPodWorthRequeuing` (`:456-543`), `runPreEnqueuePlugins` (`:566-626`),
//!   `moveToActiveQ` (`:638-679`), `moveToBackoffQ` (`:684-703`), `Add`
//!   (`:707-715`), `Activate`/`activate` (`:722-774`),
//!   `determineSchedulingHintForInFlightPod` (`:783-820`),
//!   `AddUnschedulableIfNotPresent` (`:864-922`), `flushBackoffQCompleted`
//!   (`:925-938`), `flushUnschedulablePodsLeftover` (`:942-958`), `Pop`/`Done`
//!   (`:965-978`), `Update` (`:1014-1111`), `Delete` (`:1115-1129`),
//!   `MoveAllToActiveOrBackoffQueue` (`:1163-1187`),
//!   `requeuePodWithQueueingStrategy` (`:1193-1214`),
//!   `movePodsToActiveOrBackoffQueue` (`:1217-1262`), `newQueuedPodInfo`
//!   (`:1420-1431`).
//! - `active_queue.go`: `activeQueue` (`:163-226`), `unlockedPop` (`:279-336`),
//!   `clusterEventsForPod` (`:384-407`), `addEventsIfPodInFlight`
//!   (`:126-139`), `addEventIfAnyInFlight` (`:420-434`), `unlockedDone`
//!   (`:453-494`).
//! - `backoff_queue.go`: `backoffQueue` (`:82-126`), `alignToWindow`
//!   (`:140-145`), `lessBackoffCompleted[WithPriority]` (`:191-206`),
//!   `isPodBackingoff` (`:211-215`), `getBackoffTime` (`:222-246`),
//!   `calculateBackoffDuration` (`:250-260`), `popAllBackoffCompleted`
//!   (`:262-290`), `add` (`:295-322`), `update` (`:326-343`), `delete`
//!   (`:347-355`), `popBackoff` (`:360-365`).
//! - `unschedulable_pods.go` (`:26-94`).
//! - `pkg/scheduler/framework/types.go`: `QueuedPodInfo` (`:500-548`).
//!
//! Feature gates fixed at their 1.35 defaults: `SchedulerQueueingHints` (GA,
//! locked on) and `SchedulerPopFromBackoffQ` (beta, on;
//! `pkg/features/kube_features.go:1758-1766`). The disabled-gate branches
//! (`addUnschedulableWithoutQueueingHint`, `moveRequestCycle`, the preCheck
//! fed `AssignedPodAdded`/`AssignedPodUpdated`) are therefore not ported.
//!
//! Deliberate deviations (Rust idiom, not mechanism):
//! - One `Mutex` guards activeQ, backoffQ, unschedulablePods and the in-flight
//!   bookkeeping instead of upstream's four nested locks; `pop` never waits
//!   while holding it, which is the only property upstream's split buys.
//! - The `nominator` (see [`super::nominator`]) is a field of the guarded
//!   state rather than an embedded struct with its own `nLock`.
//! - Metrics are not recorded.

use super::events::*;
use super::heap::Heap;
use super::nominator::{NominatingInfo, Nominator, PodLister, PodRef};
use rusternetes_common::resources::Pod;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use tracing::{debug, error, info, trace};

/// `DefaultPodMaxInUnschedulablePodsDuration` (`scheduling_queue.go:66`).
pub const DEFAULT_POD_MAX_IN_UNSCHEDULABLE_PODS_DURATION: Duration = Duration::from_secs(5 * 60);
/// `DefaultPodInitialBackoffDuration` (`:79`).
pub const DEFAULT_POD_INITIAL_BACKOFF_DURATION: Duration = Duration::from_secs(1);
/// `DefaultPodMaxBackoffDuration` (`:83`).
pub const DEFAULT_POD_MAX_BACKOFF_DURATION: Duration = Duration::from_secs(10);
/// `backoffQOrderingWindowDuration` (`backoff_queue.go:38`).
const BACKOFF_Q_ORDERING_WINDOW_DURATION: Duration = Duration::from_secs(1);
/// Interval of `flushUnschedulablePodsLeftover` (`scheduling_queue.go:410-412`).
const UNSCHEDULABLE_FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// Scheduling queue names (`scheduling_queue.go:67-70`).
pub const ACTIVE_Q: &str = "Active";
pub const BACKOFF_Q: &str = "Backoff";
pub const UNSCHEDULABLE_Q: &str = "Unschedulable";

/// `PreEnqueueCheck` (`:89`).
pub type PreEnqueueCheck<'a> = &'a dyn Fn(&Pod) -> bool;

/// `fwk.PreEnqueuePlugin` (`staging/.../framework/interface.go:376-383`):
/// an unsuccessful status marks the pod as gated and moves it to
/// unschedulableQ.
pub trait PreEnqueuePlugin: Send + Sync {
    fn name(&self) -> &str;
    /// `Err(message)` is an Unschedulable status.
    fn pre_enqueue(&self, pod: &Pod) -> Result<(), String>;
}

/// Clock abstraction (`k8s.io/utils/clock`) so tests can step time.
pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

pub struct RealClock;
impl Clock for RealClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// `testingclock.FakeClock`.
pub struct FakeClock(Mutex<SystemTime>);
impl FakeClock {
    pub fn new(t: SystemTime) -> Self {
        Self(Mutex::new(t))
    }
    pub fn step(&self, d: Duration) {
        let mut t = self.0.lock().unwrap();
        *t += d;
    }
}
impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        *self.0.lock().unwrap()
    }
}

/// `QueuedPodInfo` (`pkg/scheduler/framework/types.go:500-548`): a Pod
/// wrapper with its status in the scheduling queue.
#[derive(Clone, Debug)]
pub struct QueuedPodInfo {
    pub pod: Pod,
    /// The time pod added to the scheduling queue.
    pub timestamp: SystemTime,
    /// Number of all schedule attempts before successfully scheduled.
    pub attempts: usize,
    /// When the Pod will complete its backoff, aligned to the ordering window.
    pub backoff_expiration: Option<SystemTime>,
    /// Number of times the Pod was Unschedulable (no `Error` in between).
    pub unschedulable_count: usize,
    /// Number of consecutive `Error` outcomes; reset by an Unschedulable one.
    pub consecutive_errors_count: usize,
    /// First time the pod was added to activeQ; never updated afterwards.
    pub initial_attempt_timestamp: Option<SystemTime>,
    /// Plugins that failed the Pod with Unschedulable / UnschedulableAndUnresolvable
    /// at PreFilter, Filter, Reserve or Permit.
    pub unschedulable_plugins: HashSet<String>,
    /// Plugins that failed the Pod with Pending status.
    pub pending_plugins: HashSet<String>,
    /// The plugin that gated the Pod at PreEnqueue ("" = not gated).
    pub gating_plugin: String,
    /// Events registered by the gating plugin (cached).
    pub gating_plugin_events: Vec<ClusterEvent>,
}

impl QueuedPodInfo {
    /// `newQueuedPodInfoForLookup` (`:329-336`): a lookup/test fixture with
    /// only the Pod and `UnschedulablePlugins` set.
    pub fn for_lookup(pod: Pod, plugins: &[&str]) -> Self {
        Self {
            pod,
            timestamp: UNIX_EPOCH,
            attempts: 0,
            backoff_expiration: None,
            unschedulable_count: 0,
            consecutive_errors_count: 0,
            initial_attempt_timestamp: None,
            unschedulable_plugins: plugins.iter().map(|s| s.to_string()).collect(),
            pending_plugins: HashSet::new(),
            gating_plugin: String::new(),
            gating_plugin_events: Vec::new(),
        }
    }

    /// `Gated()` (`types.go:595-597`).
    pub fn gated(&self) -> bool {
        !self.gating_plugin.is_empty()
    }

    /// `PodInfo.Update` — replace the wrapped pod.
    pub fn update(&mut self, pod: Pod) {
        self.pod = pod;
    }
}

/// `podInfoKeyFunc` (`:1433-1435`) / `util.GetPodFullName`: both unique per
/// namespace+name, so one key form serves every sub-queue.
pub fn pod_key(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// `pInfo.Pod.Spec.SchedulerName`, with the API server's default applied
/// the same way `responsibleForPod` applies it (objects read straight from
/// storage may carry no name).
use crate::profile::pod_scheduler_name;

/// `fwk.LessFunc`.
pub type LessFn = Arc<dyn Fn(&QueuedPodInfo, &QueuedPodInfo) -> bool + Send + Sync>;

/// `queuesort.PrioritySort.Less` (`plugins/queuesort/priority_sort.go:44-48`).
pub fn priority_sort_less() -> LessFn {
    Arc::new(|a: &QueuedPodInfo, b: &QueuedPodInfo| {
        let p = |i: &QueuedPodInfo| i.pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);
        let (p1, p2) = (p(a), p(b));
        p1 > p2 || (p1 == p2 && a.timestamp < b.timestamp)
    })
}

/// `QueueingHintFunction` (`:212-215`): a `QueueingHintFn` with its plugin.
#[derive(Clone)]
pub struct QueueingHintFunction {
    pub plugin_name: String,
    pub queueing_hint_fn: QueueingHintFn,
}

/// `QueueingHintMap` (`:281`): keyed by event, valued with the hint functions
/// registered for it.
pub type QueueingHintMap = HashMap<ClusterEvent, Vec<QueueingHintFunction>>;
/// `QueueingHintMapPerProfile` (`:278`): keyed by profile (scheduler) name.
pub type QueueingHintMapPerProfile = HashMap<String, QueueingHintMap>;
/// `preEnqueuePluginMap` (`:189`): keyed by profile then plugin name.
pub type PreEnqueuePluginMap = HashMap<String, Vec<Arc<dyn PreEnqueuePlugin>>>;

/// `queueingStrategy` (`:415-425`). Upstream's variant names are kept.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum QueueingStrategy {
    /// Skip requeuing the Pod to activeQ/backoffQ.
    QueueSkip,
    /// Requeue the Pod after backoff is completed.
    QueueAfterBackoff,
    /// Skip backoff and requeue the Pod immediately to activeQ.
    QueueImmediately,
}

/// `clusterEvent` (`:218-224`): the event and involved objects.
#[derive(Clone)]
pub struct InFlightEvent {
    pub event: ClusterEvent,
    pub old_obj: Option<Arc<EventObject>>,
    pub new_obj: Option<Arc<EventObject>>,
}

/// An entry of `inFlightEvents` (`active_queue.go:195-208`): a popped pod or a
/// cluster event received while that pod was in flight.
#[derive(Clone)]
pub enum InFlightEntry {
    Pod { seq: u64, pod: Box<Pod> },
    Event { seq: u64, event: InFlightEvent },
}

impl InFlightEntry {
    fn seq(&self) -> u64 {
        match self {
            Self::Pod { seq, .. } | Self::Event { seq, .. } => *seq,
        }
    }
}

/// `priorityQueueOptions` (`:226-237`).
pub struct PriorityQueueOptions {
    pub clock: Arc<dyn Clock>,
    pub pod_initial_backoff_duration: Duration,
    pub pod_max_backoff_duration: Duration,
    pub pod_max_in_unschedulable_pods_duration: Duration,
    pub pre_enqueue_plugin_map: PreEnqueuePluginMap,
    pub queueing_hint_map: QueueingHintMapPerProfile,
    /// `WithPodLister` (`:248-252`): lets the nominator drop deleted or
    /// already-bound pods. `None` skips that check (nil lister upstream).
    pub pod_lister: Option<PodLister>,
}

impl Default for PriorityQueueOptions {
    /// `defaultPriorityQueueOptions` (`:318-323`).
    fn default() -> Self {
        Self {
            clock: Arc::new(RealClock),
            pod_initial_backoff_duration: DEFAULT_POD_INITIAL_BACKOFF_DURATION,
            pod_max_backoff_duration: DEFAULT_POD_MAX_BACKOFF_DURATION,
            pod_max_in_unschedulable_pods_duration: DEFAULT_POD_MAX_IN_UNSCHEDULABLE_PODS_DURATION,
            pre_enqueue_plugin_map: HashMap::new(),
            queueing_hint_map: HashMap::new(),
            pod_lister: None,
        }
    }
}

/// `backoffQueue` (`backoff_queue.go:82-108`): two heaps ordered by backoff
/// expiry — `podBackoffQ` for unschedulable pods, `podErrorBackoffQ` for pods
/// that came back because of an error.
struct BackoffQueue {
    pod_backoff_q: Heap<QueuedPodInfo>,
    pod_error_backoff_q: Heap<QueuedPodInfo>,
}

/// `unschedulablePods` (`unschedulable_pods.go:27-34`).
#[derive(Default)]
struct UnschedulablePods {
    pod_info_map: HashMap<String, QueuedPodInfo>,
}

impl UnschedulablePods {
    /// `addOrUpdate` (`:48-59`).
    fn add_or_update(&mut self, p_info: QueuedPodInfo, _event: &str) {
        self.pod_info_map.insert(pod_key(&p_info.pod), p_info);
    }
    /// `delete` (`:63-73`).
    fn delete(&mut self, pod: &Pod, _gated: bool) -> Option<QueuedPodInfo> {
        self.pod_info_map.remove(&pod_key(pod))
    }
    /// `get` (`:77-83`).
    fn get(&self, pod: &Pod) -> Option<&QueuedPodInfo> {
        self.pod_info_map.get(&pod_key(pod))
    }
}

/// Everything upstream guards with `PriorityQueue.lock`, `activeQueue.lock`
/// and `backoffQueue.lock`.
struct Inner {
    active_q: Heap<QueuedPodInfo>,
    backoff_q: BackoffQueue,
    unschedulable_pods: UnschedulablePods,
    /// `inFlightPods` (`active_queue.go:186-193`): UID → seq of the pod's
    /// entry in `in_flight_events`.
    in_flight_pods: HashMap<String, u64>,
    /// `inFlightEvents` (`active_queue.go:195-208`).
    in_flight_events: VecDeque<InFlightEntry>,
    next_seq: u64,
    /// `schedCycle` (`active_queue.go:210-212`).
    sched_cycle: i64,
    closed: bool,
    /// The embedded `*nominator` (`scheduling_queue.go:164`).
    nominator: Nominator,
}

impl Inner {
    /// `nominatedPodToInfo` (`:1371-1389`).
    fn nominated_pod_to_info(&self, np: &PodRef) -> Pod {
        let key = format!("{}/{}", np.namespace, np.name);
        if let Some(p) = self.active_q.get_by_key(&key) {
            return p.pod.clone();
        }
        if let Some(p) = self.unschedulable_pods.pod_info_map.get(&key) {
            return p.pod.clone();
        }
        if let Some(p) = self.backoff_q.get_by_key(&key) {
            return p.pod.clone();
        }
        let mut pod = Pod::new(np.name.clone(), Default::default());
        pod.spec = None;
        pod.metadata.namespace = Some(np.namespace.clone());
        pod.metadata.uid = np.uid.clone();
        pod
    }
}

/// `PriorityQueue` (`scheduling_queue.go:163-209`).
pub struct PriorityQueue {
    inner: Mutex<Inner>,
    /// `activeQueue.cond`: notified when a pod is added to activeQ (or, with
    /// `SchedulerPopFromBackoffQ`, to backoffQ).
    cond: Notify,
    clock: Arc<dyn Clock>,
    pod_initial_backoff: Duration,
    pod_max_backoff: Duration,
    pod_max_in_unschedulable_pods_duration: Duration,
    pre_enqueue_plugin_map: PreEnqueuePluginMap,
    queueing_hint_map: QueueingHintMapPerProfile,
    /// `pluginToEventsMap` (`:192-193`).
    plugin_to_events_map: HashMap<String, Vec<ClusterEvent>>,
}

/// `buildEventMap` (`:391-403`).
fn build_event_map(q_hint_map: &QueueingHintMapPerProfile) -> HashMap<String, Vec<ClusterEvent>> {
    let mut event_map: HashMap<String, Vec<ClusterEvent>> = HashMap::new();
    for hint_map in q_hint_map.values() {
        for (event, q_hints) in hint_map {
            for q_hint in q_hints {
                event_map
                    .entry(q_hint.plugin_name.clone())
                    .or_default()
                    .push(*event);
            }
        }
    }
    event_map
}

impl PriorityQueue {
    /// `NewPriorityQueue` (`:339-382`).
    pub fn new(less_fn: LessFn, options: PriorityQueueOptions) -> Arc<Self> {
        let less_backoff_completed = {
            let clock = Arc::clone(&options.clock);
            let (init, max) = (
                options.pod_initial_backoff_duration,
                options.pod_max_backoff_duration,
            );
            move |a: &QueuedPodInfo, b: &QueuedPodInfo| {
                backoff_time(&*clock, init, max, a) < backoff_time(&*clock, init, max, b)
            }
        };
        // `lessBackoffCompletedWithPriority` (`backoff_queue.go:191-199`):
        // PopFromBackoffQ is on, so equal backoff windows sort like activeQ.
        let less_backoff_with_priority = {
            let clock = Arc::clone(&options.clock);
            let (init, max) = (
                options.pod_initial_backoff_duration,
                options.pod_max_backoff_duration,
            );
            let less = Arc::clone(&less_fn);
            move |a: &QueuedPodInfo, b: &QueuedPodInfo| {
                let (bo1, bo2) = (
                    backoff_time(&*clock, init, max, a),
                    backoff_time(&*clock, init, max, b),
                );
                if bo1 != bo2 {
                    return bo1 < bo2;
                }
                less(a, b)
            }
        };
        let key_fn = || Box::new(|p: &QueuedPodInfo| pod_key(&p.pod));
        let active_less = Arc::clone(&less_fn);
        Arc::new(Self {
            inner: Mutex::new(Inner {
                active_q: Heap::new(key_fn(), Box::new(move |a, b| active_less(a, b))),
                backoff_q: BackoffQueue {
                    pod_backoff_q: Heap::new(key_fn(), Box::new(less_backoff_with_priority)),
                    pod_error_backoff_q: Heap::new(key_fn(), Box::new(less_backoff_completed)),
                },
                unschedulable_pods: UnschedulablePods::default(),
                in_flight_pods: HashMap::new(),
                in_flight_events: VecDeque::new(),
                next_seq: 0,
                sched_cycle: 0,
                closed: false,
                nominator: Nominator::new(options.pod_lister),
            }),
            cond: Notify::new(),
            clock: options.clock,
            pod_initial_backoff: options.pod_initial_backoff_duration,
            pod_max_backoff: options.pod_max_backoff_duration,
            pod_max_in_unschedulable_pods_duration: options.pod_max_in_unschedulable_pods_duration,
            plugin_to_events_map: build_event_map(&options.queueing_hint_map),
            pre_enqueue_plugin_map: options.pre_enqueue_plugin_map,
            queueing_hint_map: options.queueing_hint_map,
        })
    }

    /// `Run` (`:406-413`): pump backoffQ → activeQ every ordering window
    /// (aligned, `waitUntilAlignedWithOrderingWindow`) and flush
    /// unschedulable leftovers every 30s.
    pub fn run(self: &Arc<Self>) {
        let q = Arc::clone(self);
        tokio::spawn(async move {
            // waitUntilAlignedWithOrderingWindow (`backoff_queue.go:151-187`)
            let now = q.clock.now();
            let next = align_to_window(now + BACKOFF_Q_ORDERING_WINDOW_DURATION);
            tokio::time::sleep(next.duration_since(now).unwrap_or_default()).await;
            let mut ticker = tokio::time::interval(BACKOFF_Q_ORDERING_WINDOW_DURATION);
            loop {
                if q.inner.lock().unwrap().closed {
                    return;
                }
                q.flush_backoff_q_completed();
                ticker.tick().await;
            }
        });
        let q = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(UNSCHEDULABLE_FLUSH_INTERVAL);
            loop {
                ticker.tick().await;
                if q.inner.lock().unwrap().closed {
                    return;
                }
                q.flush_unschedulable_pods_leftover();
            }
        });
    }

    /// `Close` (`:1394-1402`).
    pub fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        // `activeQueue.close` (`active_queue.go:497-507`): done() every
        // in-flight pod.
        let uids: Vec<String> = inner.in_flight_pods.keys().cloned().collect();
        for uid in uids {
            inner.unlocked_done(&uid);
        }
        inner.closed = true;
        drop(inner);
        self.cond.notify_waiters();
    }

    // -----------------------------------------------------------------------
    // Hints
    // -----------------------------------------------------------------------

    /// `isEventOfInterest` (`:428-446`): true if the event is of interest by
    /// some plugins.
    fn is_event_of_interest(&self, event: &ClusterEvent) -> bool {
        if cluster_event_is_wild_card(event) {
            // Wildcard event moves Pods that failed with any plugins.
            return true;
        }
        for hint_map in self.queueing_hint_map.values() {
            for event_to_match in hint_map.keys() {
                if match_cluster_events(event_to_match, event) {
                    // This event is interested by some plugins.
                    return true;
                }
            }
        }
        trace!(event = %event.label(), "receive an event that isn't interested by any enabled plugins");
        false
    }

    /// `isPodWorthRequeuing` (`:456-543`): calls the `QueueingHintFn` of only
    /// the plugins in `UnschedulablePlugins`/`PendingPlugins`. `Queue` from a
    /// Pending plugin → `QueueImmediately`; from an unschedulable plugin →
    /// `QueueAfterBackoff`; all Skip → `QueueSkip`.
    pub fn is_pod_worth_requeuing(
        &self,
        p_info: &QueuedPodInfo,
        event: &ClusterEvent,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> QueueingStrategy {
        let rejector_plugins: HashSet<&String> = p_info
            .unschedulable_plugins
            .union(&p_info.pending_plugins)
            .collect();
        if rejector_plugins.is_empty() {
            trace!(pod = %pod_key(&p_info.pod), "Worth requeuing because no failed plugins");
            return QueueingStrategy::QueueAfterBackoff;
        }

        if cluster_event_is_wild_card(event) {
            // If the wildcard event has a Pod in newObj, the event wants to
            // be effective for that Pod only (EventForceActivate).
            if let Some(obj) = new_obj {
                match obj.downcast_ref::<Pod>() {
                    Some(pod) if pod.metadata.uid == p_info.pod.metadata.uid => {}
                    Some(pod) => {
                        // This wildcard event is not for this Pod.
                        trace!(pod = %pod_key(&p_info.pod), event = %event.label(), new_obj = %pod_key(pod), "Not worth requeuing because the event is wildcard, but for another pod");
                        return QueueingStrategy::QueueSkip;
                    }
                    None => return QueueingStrategy::QueueSkip,
                }
            }
            // Someone wants to force all Pods to move to activeQ/backoffQ.
            trace!(pod = %pod_key(&p_info.pod), event = %event.label(), "Worth requeuing because the event is wildcard");
            return QueueingStrategy::QueueAfterBackoff;
        }

        let scheduler_name = pod_scheduler_name(&p_info.pod);
        let Some(hint_map) = self.queueing_hint_map.get(scheduler_name) else {
            // shouldn't reach here unless bug.
            error!(profile = scheduler_name, pod = %pod_key(&p_info.pod), "No QueueingHintMap is registered for this profile");
            return QueueingStrategy::QueueAfterBackoff;
        };

        let pod = &p_info.pod;
        let mut queue_strategy = QueueingStrategy::QueueSkip;
        for (event_to_match, hintfns) in hint_map {
            if !match_cluster_events(event_to_match, event) {
                continue;
            }
            for hintfn in hintfns {
                if !rejector_plugins.contains(&hintfn.plugin_name) {
                    // skip if it's not hintfn from rejectorPlugins.
                    continue;
                }
                let hint = match (hintfn.queueing_hint_fn)(pod, old_obj, new_obj) {
                    Ok(h) => h,
                    Err(err) => {
                        // Treat the event as Queue so the Pod cannot get
                        // stuck in the unschedulable pod pool.
                        error!(error = %err, event = %event.label(), plugin = %hintfn.plugin_name, pod = %pod_key(pod), "QueueingHintFn returns error");
                        QueueingHint::Queue
                    }
                };
                if hint == QueueingHint::QueueSkip {
                    continue;
                }
                if p_info.pending_plugins.contains(&hintfn.plugin_name) {
                    // Queue from the Pending plugin is queueImmediately,
                    // the highest priority: return at once.
                    return QueueingStrategy::QueueImmediately;
                }
                // Queue from the unschedulable plugin is queueAfterBackoff.
                if p_info.pending_plugins.is_empty() {
                    // No Pending plugins (the only source of
                    // queueImmediately) on this Pod, and queueAfterBackoff
                    // is the second highest priority.
                    return QueueingStrategy::QueueAfterBackoff;
                }
                // Some Pending plugins are registered in this Pod; they may
                // still return Queue and make it queueImmediately.
                queue_strategy = QueueingStrategy::QueueAfterBackoff;
            }
        }
        queue_strategy
    }

    // -----------------------------------------------------------------------
    // PreEnqueue
    // -----------------------------------------------------------------------

    /// `runPreEnqueuePlugins` (`:566-600`): runs the gating plugin first,
    /// then the rest; updates `GatingPlugin` and `UnschedulablePlugins`.
    fn run_pre_enqueue_plugins(&self, p_info: &mut QueuedPodInfo) {
        let profile = pod_scheduler_name(&p_info.pod).to_string();
        let gating_plugin = p_info.gating_plugin.clone();
        let Some(plugins) = self.pre_enqueue_plugin_map.get(&profile) else {
            p_info.gating_plugin.clear();
            return;
        };
        if !gating_plugin.is_empty() {
            // Run the gating plugin first
            if let Some(pl) = plugins.iter().find(|p| p.name() == gating_plugin) {
                if self.run_pre_enqueue_plugin(pl.as_ref(), p_info).is_err() {
                    // No need to iterate other plugins
                    return;
                }
            }
        }
        for pl in plugins {
            if !gating_plugin.is_empty() && pl.name() == gating_plugin {
                // should be run already above.
                continue;
            }
            if self.run_pre_enqueue_plugin(pl.as_ref(), p_info).is_err() {
                // No need to iterate other plugins
                return;
            }
        }
        // all plugins passed
        p_info.gating_plugin.clear();
    }

    /// `runPreEnqueuePlugin` (`:602-626`).
    fn run_pre_enqueue_plugin(
        &self,
        pl: &dyn PreEnqueuePlugin,
        p_info: &mut QueuedPodInfo,
    ) -> Result<(), String> {
        let s = pl.pre_enqueue(&p_info.pod);
        if let Err(msg) = &s {
            p_info.unschedulable_plugins.insert(pl.name().to_string());
            debug!(pod = %pod_key(&p_info.pod), plugin = pl.name(), status = %msg, "Status after running PreEnqueue plugin");
            p_info.gating_plugin = pl.name().to_string();
            p_info.gating_plugin_events = self
                .plugin_to_events_map
                .get(pl.name())
                .cloned()
                .unwrap_or_default();
        }
        s
    }

    // -----------------------------------------------------------------------
    // Moves between sub-queues (all assume `inner` is locked by the caller)
    // -----------------------------------------------------------------------

    /// `moveToActiveQ` (`:638-679`): returns whether the pod was added.
    /// `moves_from_backoff_q` skips PreEnqueue (already run on backoffQ entry).
    fn move_to_active_q(
        &self,
        inner: &mut Inner,
        mut p_info: QueuedPodInfo,
        event: &str,
        moves_from_backoff_q: bool,
    ) -> bool {
        let gated_before = p_info.gated();
        if !moves_from_backoff_q {
            self.run_pre_enqueue_plugins(&mut p_info);
        }
        if p_info.gated() {
            // Add the Pod to unschedulablePods if it's not passing PreEnqueuePlugins.
            if inner.active_q.has(&p_info)
                || inner.backoff_q.has(&p_info)
                || inner.unschedulable_pods.get(&p_info.pod).is_some()
            {
                return false;
            }
            debug!(pod = %pod_key(&p_info.pod), event, queue = UNSCHEDULABLE_Q, "Pod moved to an internal scheduling queue, because the pod is gated");
            inner.unschedulable_pods.add_or_update(p_info, event);
            return false;
        }
        if p_info.initial_attempt_timestamp.is_none() {
            p_info.initial_attempt_timestamp = Some(self.clock.now());
        }
        inner.unschedulable_pods.delete(&p_info.pod, gated_before);
        inner.backoff_q.delete(&p_info);
        debug!(pod = %pod_key(&p_info.pod), event, queue = ACTIVE_Q, "Pod moved to an internal scheduling queue");
        if event == EVENT_UNSCHEDULED_POD_ADD.label()
            || event == EVENT_UNSCHEDULED_POD_UPDATE.label()
        {
            // `:675`
            inner.nominator.add_nominated_pod(&p_info.pod, None);
        }
        inner.active_q.add_or_update(p_info);
        true
    }

    /// `moveToBackoffQ` (`:684-703`): PreEnqueue runs on insertion into the
    /// backoffQ (PopFromBackoffQ on); a gated pod goes to unschedulablePods.
    fn move_to_backoff_q(&self, inner: &mut Inner, mut p_info: QueuedPodInfo, event: &str) -> bool {
        let gated_before = p_info.gated();
        self.run_pre_enqueue_plugins(&mut p_info);
        if p_info.gated() {
            if inner.unschedulable_pods.get(&p_info.pod).is_none() {
                debug!(pod = %pod_key(&p_info.pod), event, queue = UNSCHEDULABLE_Q, "Pod moved to an internal scheduling queue");
                inner.unschedulable_pods.add_or_update(p_info, event);
            }
            return false;
        }
        inner.unschedulable_pods.delete(&p_info.pod, gated_before);
        inner.backoff_q.add(p_info, event);
        true
    }

    /// `newQueuedPodInfo` (`:1420-1431`).
    fn new_queued_pod_info(&self, pod: Pod) -> QueuedPodInfo {
        QueuedPodInfo {
            pod,
            timestamp: self.clock.now(),
            attempts: 0,
            backoff_expiration: None,
            unschedulable_count: 0,
            consecutive_errors_count: 0,
            initial_attempt_timestamp: None,
            unschedulable_plugins: HashSet::new(),
            pending_plugins: HashSet::new(),
            gating_plugin: String::new(),
            gating_plugin_events: Vec::new(),
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// `Add` (`:707-715`): adds a new pod to the active queue.
    pub fn add(&self, pod: Pod) {
        let p_info = self.new_queued_pod_info(pod);
        let added = {
            let mut inner = self.inner.lock().unwrap();
            self.move_to_active_q(
                &mut inner,
                p_info,
                &EVENT_UNSCHEDULED_POD_ADD.label(),
                false,
            )
        };
        if added {
            self.cond.notify_waiters();
        }
    }

    /// `Activate` (`:722-744`): moves the given pods to activeQ. A pod that
    /// is in flight gets `EventForceActivate` registered so it is requeued
    /// when it comes back; a completely unknown pod is ignored.
    pub fn activate(&self, pods: &[Pod]) {
        let mut activated = false;
        {
            let mut inner = self.inner.lock().unwrap();
            for pod in pods {
                if self.activate_one(&mut inner, pod) {
                    activated = true;
                    continue;
                }
                inner.add_events_if_pod_in_flight(None, pod, &[EVENT_FORCE_ACTIVATE]);
            }
        }
        if activated {
            self.cond.notify_waiters();
        }
    }

    /// `activate` (`:746-774`).
    fn activate_one(&self, inner: &mut Inner, pod: &Pod) -> bool {
        let mut moves_from_backoff_q = false;
        let p_info = match inner.unschedulable_pods.delete(pod, false) {
            Some(p) => {
                // put it back; move_to_active_q deletes it again by key
                inner
                    .unschedulable_pods
                    .add_or_update(p.clone(), FORCE_ACTIVATE);
                p
            }
            None => {
                // Delete pod from the backoffQ now to make sure it won't be
                // popped from the backoffQ just before moving it to the activeQ
                let Some(p) = inner
                    .backoff_q
                    .take(&QueuedPodInfo::for_lookup(pod.clone(), &[]))
                else {
                    return false;
                };
                moves_from_backoff_q = true;
                p
            }
        };
        self.move_to_active_q(inner, p_info, FORCE_ACTIVATE, moves_from_backoff_q)
    }

    /// `SchedulingCycle` (`:777-779`).
    pub fn scheduling_cycle(&self) -> i64 {
        self.inner.lock().unwrap().sched_cycle
    }

    /// `determineSchedulingHintForInFlightPod` (`:783-820`): checks the
    /// events that happened while the Pod was in flight against its
    /// unschedulable plugins.
    fn determine_scheduling_hint_for_in_flight_pod(
        &self,
        inner: &Inner,
        p_info: &QueuedPodInfo,
    ) -> QueueingStrategy {
        if p_info.unschedulable_plugins.is_empty() && p_info.pending_plugins.is_empty() {
            // No failed plugins are associated with this Pod: something
            // unusual (a temporal failure on kube-apiserver, etc) happened.
            return QueueingStrategy::QueueAfterBackoff;
        }
        let events = match inner.cluster_events_for_pod(p_info) {
            Ok(e) => e,
            Err(e) => {
                error!(pod = %pod_key(&p_info.pod), error = %e, "Error getting cluster events for pod");
                return QueueingStrategy::QueueAfterBackoff;
            }
        };
        let mut queueing_strategy = QueueingStrategy::QueueSkip;
        for e in events {
            debug!(pod = %pod_key(&p_info.pod), event = %e.event.label(), "Checking event for in-flight pod");
            match self.is_pod_worth_requeuing(
                p_info,
                &e.event,
                e.old_obj.as_deref(),
                e.new_obj.as_deref(),
            ) {
                QueueingStrategy::QueueSkip => continue,
                // queueImmediately is the highest priority.
                QueueingStrategy::QueueImmediately => return QueueingStrategy::QueueImmediately,
                QueueingStrategy::QueueAfterBackoff => {
                    queueing_strategy = QueueingStrategy::QueueAfterBackoff;
                    if p_info.pending_plugins.is_empty() {
                        // No Pending plugins can produce queueImmediately.
                        return QueueingStrategy::QueueAfterBackoff;
                    }
                }
            }
        }
        queueing_strategy
    }

    /// `AddUnschedulableIfNotPresent` (`:864-922`): inserts a pod that cannot
    /// be scheduled back into the queue — unschedulablePods normally, or
    /// activeQ/backoffQ when an event that happened during its scheduling
    /// cycle may have made it schedulable. Always calls `done`.
    pub fn add_unschedulable_if_not_present(
        &self,
        mut p_info: QueuedPodInfo,
        pod_scheduling_cycle: i64,
    ) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        let uid = p_info.pod.metadata.uid.clone();
        let result = (|| {
            let key = pod_key(&p_info.pod);
            if inner.unschedulable_pods.get(&p_info.pod).is_some() {
                return Err(format!(
                    "Pod {key} is already present in unschedulable queue"
                ));
            }
            if inner.active_q.has(&p_info) {
                return Err(format!("Pod {key} is already present in the active queue"));
            }
            if inner.backoff_q.has(&p_info) {
                return Err(format!("Pod {key} is already present in the backoff queue"));
            }

            if p_info.unschedulable_plugins.is_empty() && p_info.pending_plugins.is_empty() {
                // This Pod came back because of some unexpected errors (e.g., a network issue).
                p_info.consecutive_errors_count += 1;
            } else {
                // Rejected by some plugins, not an unexpected error.
                p_info.unschedulable_count += 1;
                // We should reset the error count because the error is gone.
                p_info.consecutive_errors_count = 0;
            }
            // Refresh the timestamp since the pod is re-added.
            p_info.timestamp = self.clock.now();
            // Counts/timestamp changed, so the cached backoff time is stale.
            p_info.backoff_expiration = None;

            let rejector_plugins: Vec<String> = p_info
                .unschedulable_plugins
                .union(&p_info.pending_plugins)
                .cloned()
                .collect();

            // Check whether any event that happened during scheduling may
            // change this Pod's scheduling result.
            let scheduling_hint = self.determine_scheduling_hint_for_in_flight_pod(&inner, &p_info);
            let queue = self.requeue_pod_with_queueing_strategy(
                &mut inner,
                p_info,
                scheduling_hint,
                SCHEDULE_ATTEMPT_FAILURE,
            );
            info!(pod = %key, event = SCHEDULE_ATTEMPT_FAILURE, queue, scheduling_cycle = pod_scheduling_cycle, hint = ?scheduling_hint, unschedulable_plugins = ?rejector_plugins, "Pod moved to an internal scheduling queue");
            Ok(queue == ACTIVE_Q || queue == BACKOFF_Q)
        })();
        // In any case, this Pod will be moved back to the queue: `defer p.Done`.
        inner.unlocked_done(&uid);
        drop(inner);
        match result {
            Ok(true) => {
                self.cond.notify_waiters();
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// `flushBackoffQCompleted` (`:925-938`): moves all pods from backoffQ
    /// that completed backoff into activeQ.
    pub fn flush_backoff_q_completed(&self) {
        let mut activated = false;
        {
            let mut inner = self.inner.lock().unwrap();
            let completed = inner.backoff_q.pop_all_backoff_completed(
                &*self.clock,
                self.pod_initial_backoff,
                self.pod_max_backoff,
            );
            for p_info in completed {
                if self.move_to_active_q(&mut inner, p_info, BACKOFF_COMPLETE, true) {
                    activated = true;
                }
            }
        }
        if activated {
            self.cond.notify_waiters();
        }
    }

    /// `flushUnschedulablePodsLeftover` (`:942-958`): moves pods that stayed
    /// in unschedulablePods longer than `podMaxInUnschedulablePodsDuration`.
    pub fn flush_unschedulable_pods_leftover(&self) {
        let mut inner = self.inner.lock().unwrap();
        let current_time = self.clock.now();
        let pods_to_move: Vec<QueuedPodInfo> = inner
            .unschedulable_pods
            .pod_info_map
            .values()
            .filter(|p| {
                current_time
                    .duration_since(p.timestamp)
                    .map(|d| d > self.pod_max_in_unschedulable_pods_duration)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if !pods_to_move.is_empty() {
            let activated = self.move_pods_to_active_or_backoff_queue(
                &mut inner,
                pods_to_move,
                &EVENT_UNSCHEDULABLE_TIMEOUT,
                None,
                None,
            );
            drop(inner);
            if activated {
                self.cond.notify_waiters();
            }
        }
    }

    /// `Pop` (`:965-967`) / `unlockedPop` (`active_queue.go:279-336`): removes
    /// the head of activeQ — or, when activeQ is empty, of the (unschedulable)
    /// backoffQ — and returns it, blocking while both are empty. Increments
    /// the scheduling cycle and records the pod as in flight. Returns `None`
    /// once the queue is closed.
    pub async fn pop(&self) -> Option<QueuedPodInfo> {
        loop {
            // Register interest before checking, so a `notify_waiters` that
            // lands between the check and the await is not lost.
            let notified = self.cond.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut inner = self.inner.lock().unwrap();
                if !inner.active_q.is_empty() || inner.backoff_q.len_backoff() != 0 {
                    if let Some(p) = inner.unlocked_pop() {
                        return Some(p);
                    }
                    continue;
                }
                if inner.closed {
                    info!("Scheduling queue is closed");
                    return None;
                }
            }
            notified.await;
        }
    }

    /// `Done` (`:971-978`): must be called for every pod returned by `pop`.
    pub fn done(&self, uid: &str) {
        self.inner.lock().unwrap().unlocked_done(uid);
    }

    /// `InFlightPods` (`:980-987`).
    pub fn in_flight_pods(&self) -> Vec<Pod> {
        let inner = self.inner.lock().unwrap();
        inner
            .in_flight_events
            .iter()
            .filter_map(|e| match e {
                InFlightEntry::Pod { pod, .. } => Some((**pod).clone()),
                _ => None,
            })
            .collect()
    }

    /// `listInFlightEvents` (`active_queue.go:362-370`).
    pub fn in_flight_events(&self) -> Vec<InFlightEntry> {
        self.inner
            .lock()
            .unwrap()
            .in_flight_events
            .iter()
            .cloned()
            .collect()
    }

    /// `Update` (`:1014-1111`): updates a pod in activeQ/backoffQ if present;
    /// for a pod in unschedulablePods, asks the Pod/Update queueing hints
    /// whether the update may make it schedulable; an unknown pod is added
    /// to activeQ. An in-flight pod only gets the update events recorded.
    pub fn update(&self, old_pod: Option<&Pod>, new_pod: &Pod) {
        let mut inner = self.inner.lock().unwrap();
        let events = match old_pod {
            Some(old) => pod_scheduling_properties_change(new_pod, old),
            None => Vec::new(),
        };

        // The in-flight pod will be requeued using the latest version from
        // the informer cache; record this update because it may make the Pod
        // schedulable in case it gets rejected and comes back to the queue.
        if inner.add_events_if_pod_in_flight(old_pod, new_pod, &events) {
            trace!(pod = %pod_key(new_pod), "The pod doesn't need to be queued for now because it's being scheduled and will be queued back if necessary");
            return;
        }
        if let Some(old_pod) = old_pod {
            let key = pod_key(new_pod);
            // If the pod is already in the active queue, just update it there.
            if let Some(p) = inner.active_q.get_mut_by_key(&key) {
                p.update(new_pod.clone());
                let p = p.clone();
                inner.active_q.add_or_update(p);
                inner.nominator.update_nominated_pod(old_pod, new_pod);
                return;
            }
            // If the pod is in the backoff queue, update it there.
            if inner.backoff_q.update(new_pod) {
                inner.nominator.update_nominated_pod(old_pod, new_pod);
                return;
            }
        }

        // If the pod is in the unschedulable queue, updating it may make it schedulable.
        if let Some(mut p_info) = inner.unschedulable_pods.delete(new_pod, false) {
            p_info.update(new_pod.clone());
            inner
                .nominator
                .update_nominated_pod(old_pod.unwrap_or(new_pod), new_pod);
            inner
                .unschedulable_pods
                .add_or_update(p_info.clone(), &EVENT_UNSCHEDULED_POD_UPDATE.label());
            // Plugins have to implement a QueueingHint for Pod/Update if their
            // rejection could be resolved by updating the unscheduled Pod itself.
            let old_any: Option<Arc<EventObject>> =
                old_pod.map(|p| Arc::new(p.clone()) as Arc<EventObject>);
            let new_any: Arc<EventObject> = Arc::new(new_pod.clone());
            for evt in &events {
                let p_info = match inner.unschedulable_pods.get(new_pod) {
                    Some(p) => p.clone(),
                    None => break, // already moved by a previous event
                };
                let hint =
                    self.is_pod_worth_requeuing(&p_info, evt, old_any.as_deref(), Some(&*new_any));
                if hint != QueueingStrategy::QueueSkip {
                    inner.unschedulable_pods.delete(new_pod, p_info.gated());
                }
                let queue =
                    self.requeue_pod_with_queueing_strategy(&mut inner, p_info, hint, &evt.label());
                if queue != UNSCHEDULABLE_Q {
                    debug!(pod = %pod_key(new_pod), event = %evt.label(), queue, "Pod moved to an internal scheduling queue because the Pod is updated");
                }
                if queue == ACTIVE_Q || queue == BACKOFF_Q {
                    drop(inner);
                    self.cond.notify_waiters();
                    return;
                }
            }
            return;
        }
        // If pod is not in any of the queues, we put it in the active queue.
        let p_info = self.new_queued_pod_info(new_pod.clone());
        if self.move_to_active_q(
            &mut inner,
            p_info,
            &EVENT_UNSCHEDULED_POD_UPDATE.label(),
            false,
        ) {
            drop(inner);
            self.cond.notify_waiters();
        }
    }

    /// `AddNominatedPod` (`:628-632`): adds the pod to the nominator (or
    /// updates it), under the queue lock.
    pub fn add_nominated_pod(&self, pod: &Pod, nominating_info: Option<&NominatingInfo>) {
        self.inner
            .lock()
            .unwrap()
            .nominator
            .add_nominated_pod(pod, nominating_info);
    }

    /// `DeleteNominatedPodIfExists` (nominator.go:138-142).
    pub fn delete_nominated_pod_if_exists(&self, pod: &Pod) {
        self.inner.lock().unwrap().nominator.delete(pod);
    }

    /// Snapshot of `nominatedPodToNode` (pod UID -> nominated node name).
    pub fn nominated_nodes(&self) -> HashMap<String, String> {
        self.inner
            .lock()
            .unwrap()
            .nominator
            .nominated_pod_to_node
            .clone()
    }

    /// `NominatedPodsForNode` (`:1403-1417`): copies of the pods nominated to
    /// run on `node_name`, as held by the queue (`nominatedPodToInfo`,
    /// `:1371-1389`: activeQ, then unschedulablePods, then backoffQ, else a
    /// stub with only name/namespace/UID).
    pub fn nominated_pods_for_node(&self, node_name: &str) -> Vec<Pod> {
        let inner = self.inner.lock().unwrap();
        inner
            .nominator
            .nominated_pods_for_node(node_name)
            .iter()
            .map(|np| inner.nominated_pod_to_info(np))
            .collect()
    }

    /// `Delete` (`:1115-1129`): deletes the pod from whichever sub-queue
    /// holds it.
    pub fn delete(&self, pod: &Pod) {
        let mut inner = self.inner.lock().unwrap();
        inner.nominator.delete(pod);
        let lookup = QueuedPodInfo::for_lookup(pod.clone(), &[]);
        if inner.active_q.delete(&lookup).is_ok() {
            return;
        }
        if inner.backoff_q.delete(&lookup) {
            return;
        }
        inner.unschedulable_pods.delete(pod, false);
    }

    /// `MoveAllToActiveOrBackoffQueue` (`:1163-1187`): moves all pods from
    /// unschedulablePods to activeQ or backoffQ (per the queueing hints), and
    /// records the event for in-flight pods.
    pub fn move_all_to_active_or_backoff_queue(
        &self,
        event: &ClusterEvent,
        old_obj: Option<Arc<EventObject>>,
        new_obj: Option<Arc<EventObject>>,
        pre_check: Option<PreEnqueueCheck<'_>>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        if !self.is_event_of_interest(event) {
            // No plugin is interested in this event.
            return;
        }
        let unschedulable_pods: Vec<QueuedPodInfo> = inner
            .unschedulable_pods
            .pod_info_map
            .values()
            .filter(|p| pre_check.is_none_or(|check| check(&p.pod)))
            .cloned()
            .collect();
        let activated = self.move_pods_to_active_or_backoff_queue(
            &mut inner,
            unschedulable_pods,
            event,
            old_obj,
            new_obj,
        );
        drop(inner);
        if activated {
            self.cond.notify_waiters();
        }
    }

    /// `requeuePodWithQueueingStrategy` (`:1193-1214`): requeues the Pod to
    /// activeQ, backoffQ or the unschedulable pool per `strategy`; returns the
    /// queue name it went to.
    fn requeue_pod_with_queueing_strategy(
        &self,
        inner: &mut Inner,
        p_info: QueuedPodInfo,
        strategy: QueueingStrategy,
        event: &str,
    ) -> &'static str {
        if strategy == QueueingStrategy::QueueSkip {
            inner.unschedulable_pods.add_or_update(p_info, event);
            return UNSCHEDULABLE_Q;
        }
        // The Pod might have completed its backoff while in unschedulablePods,
        // so check isPodBackingoff before moving it to backoffQ.
        if strategy == QueueingStrategy::QueueAfterBackoff && self.is_pod_backing_off(&p_info) {
            if self.move_to_backoff_q(inner, p_info, event) {
                return BACKOFF_Q;
            }
            return UNSCHEDULABLE_Q;
        }
        // QueueImmediately, or Queue but the pod is not backing off.
        if self.move_to_active_q(inner, p_info, event, false) {
            return ACTIVE_Q;
        }
        // Pod is gated; moveToActiveQ already put it back to unschedulablePods.
        UNSCHEDULABLE_Q
    }

    /// `movePodsToActiveOrBackoffQueue` (`:1217-1262`). Returns whether
    /// `activeQ.broadcast()` is due.
    fn move_pods_to_active_or_backoff_queue(
        &self,
        inner: &mut Inner,
        pod_info_list: Vec<QueuedPodInfo>,
        event: &ClusterEvent,
        old_obj: Option<Arc<EventObject>>,
        new_obj: Option<Arc<EventObject>>,
    ) -> bool {
        if !self.is_event_of_interest(event) {
            // No plugin is interested in this event.
            return false;
        }
        let mut activated = false;
        for p_info in pod_info_list {
            // Gated pods are only re-evaluated for events their gating plugin
            // registered — except wildcard events (periodic flushes), which
            // always re-evaluate so pods don't get stuck on incomplete hints.
            if p_info.gated()
                && !cluster_event_is_wild_card(event)
                && !match_any_cluster_event(event, &p_info.gating_plugin_events)
            {
                continue;
            }
            let scheduling_hint =
                self.is_pod_worth_requeuing(&p_info, event, old_obj.as_deref(), new_obj.as_deref());
            if scheduling_hint == QueueingStrategy::QueueSkip {
                debug!(pod = %pod_key(&p_info.pod), event = %event.label(), "Event is not making pod schedulable");
                continue;
            }
            inner.unschedulable_pods.delete(&p_info.pod, p_info.gated());
            let queue = self.requeue_pod_with_queueing_strategy(
                inner,
                p_info,
                scheduling_hint,
                &event.label(),
            );
            if queue == ACTIVE_Q || queue == BACKOFF_Q {
                activated = true;
            }
        }
        // AddUnschedulableIfNotPresent might get called for in-flight Pods
        // later and needs to know whether events were observed meanwhile.
        if inner.add_event_if_any_in_flight(old_obj, new_obj, *event) {
            debug!(event = %event.label(), "Event received while pods are in flight");
        }
        activated
    }

    /// `isPodBackingoff` (`backoff_queue.go:211-215`).
    fn is_pod_backing_off(&self, p_info: &QueuedPodInfo) -> bool {
        let bo_time = backoff_time(
            &*self.clock,
            self.pod_initial_backoff,
            self.pod_max_backoff,
            p_info,
        );
        // Not `>`: in case of window equality the pod is still backing off.
        bo_time >= align_to_window(self.clock.now())
    }

    // -----------------------------------------------------------------------
    // Introspection (testing/debugging: `:1291-1356`)
    // -----------------------------------------------------------------------

    pub fn pods_in_active_q(&self) -> Vec<Pod> {
        self.inner
            .lock()
            .unwrap()
            .active_q
            .list()
            .into_iter()
            .map(|p| p.pod.clone())
            .collect()
    }

    pub fn pods_in_backoff_q(&self) -> Vec<Pod> {
        self.inner.lock().unwrap().backoff_q.list()
    }

    pub fn unschedulable_pods(&self) -> Vec<Pod> {
        self.inner
            .lock()
            .unwrap()
            .unschedulable_pods
            .pod_info_map
            .values()
            .map(|p| p.pod.clone())
            .collect()
    }

    /// `GetPod` (`:1312-1340`).
    pub fn get_pod(&self, name: &str, namespace: &str) -> Option<QueuedPodInfo> {
        let inner = self.inner.lock().unwrap();
        let key = format!("{namespace}/{name}");
        inner
            .active_q
            .get_by_key(&key)
            .or_else(|| inner.backoff_q.get_by_key(&key))
            .or_else(|| inner.unschedulable_pods.pod_info_map.get(&key))
            .cloned()
    }

    /// `PendingPods` (`:1342-1356`): all pending pods and a summary.
    pub fn pending_pods(&self) -> (Vec<Pod>, String) {
        let inner = self.inner.lock().unwrap();
        let mut result: Vec<Pod> = inner
            .active_q
            .list()
            .into_iter()
            .map(|p| p.pod.clone())
            .collect();
        let active = result.len();
        result.extend(inner.backoff_q.list());
        let backoff = result.len() - active;
        result.extend(
            inner
                .unschedulable_pods
                .pod_info_map
                .values()
                .map(|p| p.pod.clone()),
        );
        let unsched = result.len() - active - backoff;
        (
            result,
            format!("activeQ:{active}; backoffQ:{backoff}; unschedulablePods:{unsched}"),
        )
    }

    #[cfg(test)]
    fn backoff_q_has(&self, pod: &Pod) -> bool {
        self.inner
            .lock()
            .unwrap()
            .backoff_q
            .has(&QueuedPodInfo::for_lookup(pod.clone(), &[]))
    }

    #[cfg(test)]
    fn active_q_len(&self) -> usize {
        self.inner.lock().unwrap().active_q.len()
    }

    #[cfg(test)]
    fn backoff_q_len(&self) -> usize {
        self.inner.lock().unwrap().backoff_q.len()
    }

    #[cfg(test)]
    fn get_unschedulable_pod(&self, pod: &Pod) -> Option<Pod> {
        self.inner
            .lock()
            .unwrap()
            .unschedulable_pods
            .get(pod)
            .map(|p| p.pod.clone())
    }
}

impl Inner {
    /// `unlockedPop` (`active_queue.go:279-336`), the non-blocking part.
    fn unlocked_pop(&mut self) -> Option<QueuedPodInfo> {
        let mut p_info = match self.active_q.pop() {
            Ok(p) => p,
            Err(_) => {
                // Try to pop from backoffQ when activeQ is empty (`PopFromBackoffQ`).
                let p = self.backoff_q.pop_backoff().ok()?;
                debug!(pod = %pod_key(&p.pod), event = POP_FROM_BACKOFF_Q, "Pod popped from the backoffQ");
                p
            }
        };
        p_info.attempts += 1;
        // In flight, no concurrent events yet.
        let uid = p_info.pod.metadata.uid.clone();
        if self.in_flight_pods.contains_key(&uid) {
            // The same pod is tracked in multiple places: discard this
            // duplicate and pop the next one.
            error!(pod = %pod_key(&p_info.pod), "The same pod is tracked in multiple places in the scheduler, and just discard it");
            return self.unlocked_pop();
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.in_flight_events.push_back(InFlightEntry::Pod {
            seq,
            pod: Box::new(p_info.pod.clone()),
        });
        self.in_flight_pods.insert(uid, seq);
        self.sched_cycle += 1;

        // Reset the set of unschedulable plugins for the next attempt.
        p_info.unschedulable_plugins.clear();
        p_info.pending_plugins.clear();
        p_info.gating_plugin.clear();
        p_info.gating_plugin_events.clear();
        Some(p_info)
    }

    /// `clusterEventsForPod` (`active_queue.go:384-407`): all cluster events
    /// that happened while the pod was being scheduled.
    fn cluster_events_for_pod(&self, p_info: &QueuedPodInfo) -> Result<Vec<InFlightEvent>, String> {
        let Some(&seq) = self.in_flight_pods.get(&p_info.pod.metadata.uid) else {
            return Err("in flight Pod isn't found in the scheduling queue. If you see this error log, it's likely a bug in the scheduler".to_string());
        };
        Ok(self
            .in_flight_events
            .iter()
            .filter(|e| e.seq() > seq)
            .filter_map(|e| match e {
                InFlightEntry::Event { event, .. } => Some(event.clone()),
                // Another in-flight Pod. Can be ignored.
                InFlightEntry::Pod { .. } => None,
            })
            .collect())
    }

    /// `addEventsIfPodInFlight` (`active_queue.go:126-139`): records
    /// `events` if `new_pod` is in flight; returns whether it was.
    fn add_events_if_pod_in_flight(
        &mut self,
        old_pod: Option<&Pod>,
        new_pod: &Pod,
        events: &[ClusterEvent],
    ) -> bool {
        if !self.in_flight_pods.contains_key(&new_pod.metadata.uid) {
            return false;
        }
        for ev in events {
            let seq = self.next_seq;
            self.next_seq += 1;
            self.in_flight_events.push_back(InFlightEntry::Event {
                seq,
                event: InFlightEvent {
                    event: *ev,
                    old_obj: old_pod.map(|p| Arc::new(p.clone()) as Arc<EventObject>),
                    new_obj: Some(Arc::new(new_pod.clone()) as Arc<EventObject>),
                },
            });
        }
        true
    }

    /// `addEventIfAnyInFlight` (`active_queue.go:420-434`).
    fn add_event_if_any_in_flight(
        &mut self,
        old_obj: Option<Arc<EventObject>>,
        new_obj: Option<Arc<EventObject>>,
        event: ClusterEvent,
    ) -> bool {
        if self.in_flight_pods.is_empty() {
            return false;
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.in_flight_events.push_back(InFlightEntry::Event {
            seq,
            event: InFlightEvent {
                event,
                old_obj,
                new_obj,
            },
        });
        true
    }

    /// `unlockedDone` (`active_queue.go:453-494`): removes the pod from the
    /// in-flight list and prunes leading events no in-flight pod refers to.
    fn unlocked_done(&mut self, uid: &str) {
        let Some(seq) = self.in_flight_pods.remove(uid) else {
            // This Pod is already done()ed.
            return;
        };
        if let Some(pos) = self.in_flight_events.iter().position(|e| e.seq() == seq) {
            self.in_flight_events.remove(pos);
        }
        // Remove events only referred to by this Pod: if it was at the head
        // of the list, everything up to the next pod is no longer needed.
        while let Some(front) = self.in_flight_events.front() {
            if matches!(front, InFlightEntry::Pod { .. }) {
                break;
            }
            self.in_flight_events.pop_front();
        }
    }
}

/// `alignToWindow` (`backoff_queue.go:140-145`): truncate to the ordering
/// window (PopFromBackoffQ on).
fn align_to_window(t: SystemTime) -> SystemTime {
    let since = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs() / BACKOFF_Q_ORDERING_WINDOW_DURATION.as_secs()
        * BACKOFF_Q_ORDERING_WINDOW_DURATION.as_secs();
    UNIX_EPOCH + Duration::from_secs(secs)
}

/// `calculateBackoffDuration` (`backoff_queue.go:250-260`).
fn calculate_backoff_duration(initial: Duration, max: Duration, count: usize) -> Duration {
    if count == 0 {
        return Duration::ZERO;
    }
    let shift = (count - 1) as u32;
    let max_shifted = max.as_nanos().checked_shr(shift).unwrap_or(0);
    if initial.as_nanos() > max_shifted {
        return max;
    }
    match initial.as_nanos().checked_shl(shift) {
        Some(n) if n <= u64::MAX as u128 => Duration::from_nanos(n as u64),
        _ => max,
    }
}

/// `getBackoffTime` (`backoff_queue.go:222-246`): the time the pod completes
/// backoff. Upstream caches it in `BackoffExpiration`; the cache is cleared in
/// `AddUnschedulableIfNotPresent`, so the inputs it depends on are stable and
/// recomputing here (through a shared `&`) is equivalent.
fn backoff_time(
    _clock: &dyn Clock,
    initial: Duration,
    max: Duration,
    p_info: &QueuedPodInfo,
) -> SystemTime {
    if max.is_zero() {
        // podMaxBackoff 0 disables backoff completely.
        return UNIX_EPOCH;
    }
    let mut count = p_info.unschedulable_count;
    if p_info.consecutive_errors_count > 0 {
        // The last cycle was an error status; its count drives the backoff.
        count = p_info.consecutive_errors_count;
    }
    if count == 0 {
        // No scheduling attempts yet: no backoff.
        return UNIX_EPOCH;
    }
    if let Some(cached) = p_info.backoff_expiration {
        return cached;
    }
    align_to_window(p_info.timestamp + calculate_backoff_duration(initial, max, count))
}

impl BackoffQueue {
    fn is_error_pod(p: &QueuedPodInfo) -> bool {
        p.unschedulable_plugins.is_empty() && p.pending_plugins.is_empty()
    }

    /// `add` (`backoff_queue.go:295-322`): a pod with no unschedulable or
    /// pending plugins failed because of an error → `podErrorBackoffQ`.
    fn add(&mut self, p_info: QueuedPodInfo, event: &str) {
        let key = pod_key(&p_info.pod);
        if Self::is_error_pod(&p_info) {
            self.pod_error_backoff_q.add_or_update(p_info);
            if self.pod_backoff_q.delete_by_key(&key).is_ok() {
                error!(pod = %key, "BackoffQueue add() was called with a pod that was already in the podBackoffQ");
                return;
            }
        } else {
            self.pod_backoff_q.add_or_update(p_info);
            if self.pod_error_backoff_q.delete_by_key(&key).is_ok() {
                error!(pod = %key, "BackoffQueue add() was called with a pod that was already in the podErrorBackoffQ");
                return;
            }
        }
        debug!(pod = %key, event, queue = BACKOFF_Q, "Pod moved to an internal scheduling queue");
    }

    /// `update` (`backoff_queue.go:326-343`): returns whether the pod was in
    /// either backoff queue.
    fn update(&mut self, new_pod: &Pod) -> bool {
        let key = pod_key(new_pod);
        for q in [&mut self.pod_backoff_q, &mut self.pod_error_backoff_q] {
            if let Some(p) = q.get_mut_by_key(&key) {
                p.update(new_pod.clone());
                let p = p.clone();
                q.add_or_update(p);
                return true;
            }
        }
        false
    }

    /// `delete` (`backoff_queue.go:347-355`).
    fn delete(&mut self, p_info: &QueuedPodInfo) -> bool {
        self.pod_backoff_q.delete(p_info).is_ok() || self.pod_error_backoff_q.delete(p_info).is_ok()
    }

    /// `get` + `delete` as used by `activate` (`:754-764`).
    fn take(&mut self, lookup: &QueuedPodInfo) -> Option<QueuedPodInfo> {
        self.pod_backoff_q
            .delete(lookup)
            .or_else(|_| self.pod_error_backoff_q.delete(lookup))
            .ok()
    }

    /// `popBackoff` (`backoff_queue.go:360-365`): only `podBackoffQ`.
    fn pop_backoff(&mut self) -> Result<QueuedPodInfo, String> {
        self.pod_backoff_q.pop()
    }

    fn has(&self, p_info: &QueuedPodInfo) -> bool {
        self.pod_backoff_q.has(p_info) || self.pod_error_backoff_q.has(p_info)
    }

    fn get_by_key(&self, key: &str) -> Option<&QueuedPodInfo> {
        self.pod_backoff_q
            .get_by_key(key)
            .or_else(|| self.pod_error_backoff_q.get_by_key(key))
    }

    fn list(&self) -> Vec<Pod> {
        self.pod_backoff_q
            .list()
            .into_iter()
            .chain(self.pod_error_backoff_q.list())
            .map(|p| p.pod.clone())
            .collect()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.pod_backoff_q.len() + self.pod_error_backoff_q.len()
    }

    /// `lenBackoff` (`backoff_queue.go:411-416`).
    fn len_backoff(&self) -> usize {
        self.pod_backoff_q.len()
    }

    /// `popAllBackoffCompleted` (`backoff_queue.go:262-290`).
    fn pop_all_backoff_completed(
        &mut self,
        clock: &dyn Clock,
        initial: Duration,
        max: Duration,
    ) -> Vec<QueuedPodInfo> {
        let window = align_to_window(clock.now());
        let mut popped = Vec::new();
        for q in [&mut self.pod_backoff_q, &mut self.pod_error_backoff_q] {
            while let Some(p) = q.peek() {
                if backoff_time(clock, initial, max, p) >= window {
                    break;
                }
                match q.pop() {
                    Ok(p) => popped.push(p),
                    Err(e) => {
                        error!(error = %e, "Unable to pop pod from backoff queue despite backoff completion");
                        break;
                    }
                }
            }
        }
        popped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn mk_pod(name: &str, ns: &str, uid: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": name, "namespace": ns, "uid": uid},
            "spec": {}
        }))
        .unwrap()
    }

    fn mk_pod_prio(name: &str, ns: &str, uid: &str, prio: i32) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": name, "namespace": ns, "uid": uid},
            "spec": {"priority": prio}
        }))
        .unwrap()
    }

    fn mk_node() -> rusternetes_common::resources::Node {
        serde_json::from_value(serde_json::json!({"metadata": {"name": "node"}})).unwrap()
    }

    const NODE_ADD: ClusterEvent = ClusterEvent::new(EventResource::Node, ActionType::ADD);
    const PV_ADD: ClusterEvent =
        ClusterEvent::new(EventResource::PersistentVolume, ActionType::ADD);
    const PV_UPDATE: ClusterEvent =
        ClusterEvent::new(EventResource::PersistentVolume, ActionType::UPDATE);
    const PVC_ADD: ClusterEvent =
        ClusterEvent::new(EventResource::PersistentVolumeClaim, ActionType::ADD);
    const CSI_NODE_UPDATE: ClusterEvent =
        ClusterEvent::new(EventResource::CsiNode, ActionType::UPDATE);

    fn hint_queue(count: &Arc<AtomicUsize>) -> QueueingHintFn {
        let c = Arc::clone(count);
        Arc::new(move |_, _, _| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(QueueingHint::Queue)
        })
    }
    fn hint_skip(count: &Arc<AtomicUsize>) -> QueueingHintFn {
        let c = Arc::clone(count);
        Arc::new(move |_, _, _| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(QueueingHint::QueueSkip)
        })
    }
    fn hint_err(count: &Arc<AtomicUsize>) -> QueueingHintFn {
        let c = Arc::clone(count);
        Arc::new(move |_, _, _| {
            c.fetch_add(1, Ordering::SeqCst);
            Err("unexpected error".to_string())
        })
    }

    fn hints(
        entries: Vec<(ClusterEvent, Vec<(&str, QueueingHintFn)>)>,
    ) -> QueueingHintMapPerProfile {
        let mut m: QueueingHintMap = HashMap::new();
        for (ev, fns) in entries {
            m.entry(ev)
                .or_default()
                .extend(fns.into_iter().map(|(n, f)| QueueingHintFunction {
                    plugin_name: n.to_string(),
                    queueing_hint_fn: f,
                }));
        }
        HashMap::from([(crate::profile::DEFAULT_SCHEDULER_NAME.to_string(), m)])
    }

    /// Upstream tests hand the popped `podInfo` back to
    /// `AddUnschedulableIfNotPresent` with `UnschedulablePlugins` set, so
    /// `Attempts` carries over.
    fn rejected(mut p_info: QueuedPodInfo, plugins: &[&str]) -> QueuedPodInfo {
        p_info.unschedulable_plugins = plugins.iter().map(|s| s.to_string()).collect();
        p_info
    }

    fn new_test_queue(
        hint_map: QueueingHintMapPerProfile,
        clock: Arc<dyn Clock>,
    ) -> Arc<PriorityQueue> {
        PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                clock,
                queueing_hint_map: hint_map,
                ..Default::default()
            },
        )
    }

    /// `Test_isPodWorthRequeuing` (`scheduling_queue_test.go:4039-4347`).
    #[test]
    fn is_pod_worth_requeuing_table() {
        let count = Arc::new(AtomicUsize::new(0));
        let pod1 = mk_pod("pod1", "ns1", "1");
        let node = mk_node();
        struct Case {
            name: &'static str,
            unschedulable: &'static [&'static str],
            pending: &'static [&'static str],
            event: ClusterEvent,
            new_obj: Option<Arc<EventObject>>,
            expected: QueueingStrategy,
            expected_execution_count: usize,
            map: QueueingHintMapPerProfile,
        }
        let node_obj = || Some(Arc::new(mk_node()) as Arc<EventObject>);
        let cases = vec![
            Case {
                name: "return Queue when no queueing hint function is registered for the event",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueSkip,
                expected_execution_count: 0,
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
            },
            Case {
                name: "Treat the event as Queue when QueueHintFn returns error",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 1,
                map: hints(vec![(NODE_ADD, vec![("fooPlugin1", hint_err(&count))])]),
            },
            Case {
                name: "return Queue when the event is wildcard",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: EVENT_UNSCHEDULABLE_TIMEOUT,
                new_obj: None,
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 0,
                map: HashMap::new(),
            },
            Case {
                name: "return Queue when the event is wildcard and the wildcard targets the pod to be requeued right now",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: EVENT_FORCE_ACTIVATE,
                new_obj: Some(Arc::new(mk_pod("pod1", "ns1", "1"))),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 0,
                map: HashMap::new(),
            },
            Case {
                name: "return Skip when the event is wildcard, but the wildcard targets a different pod",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: EVENT_FORCE_ACTIVATE,
                new_obj: Some(Arc::new(mk_pod("pod-different", "ns2", "2"))),
                expected: QueueingStrategy::QueueSkip,
                expected_execution_count: 0,
                map: HashMap::new(),
            },
            Case {
                name: "interprets Queue from the Pending plugin as queueImmediately",
                unschedulable: &["fooPlugin1", "fooPlugin3"],
                pending: &["fooPlugin2"],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueImmediately,
                expected_execution_count: 2,
                map: hints(vec![(
                    NODE_ADD,
                    vec![
                        // Queue → queueAfterBackoff, but continues because the
                        // Pod has PendingPlugins which can yield queueImmediately.
                        ("fooPlugin1", hint_queue(&count)),
                        // queueImmediately: the highest priority, returns at once.
                        ("fooPlugin2", hint_queue(&count)),
                        ("fooPlugin3", hint_queue(&count)),
                        ("fooPlugin4", hint_err(&count)),
                    ],
                )]),
            },
            Case {
                name: "interprets Queue from the Unschedulable plugin as queueAfterBackoff",
                unschedulable: &["fooPlugin1", "fooPlugin2"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 2,
                map: hints(vec![(
                    NODE_ADD,
                    vec![("fooPlugin1", hint_skip(&count)), ("fooPlugin2", hint_queue(&count))],
                )]),
            },
            Case {
                name: "Queueing hint function that isn't from the plugin in UnschedulablePlugins/PendingPlugins is ignored",
                unschedulable: &["fooPlugin1", "fooPlugin2"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueSkip,
                expected_execution_count: 2,
                map: hints(vec![(
                    NODE_ADD,
                    vec![
                        ("fooPlugin1", hint_skip(&count)),
                        ("fooPlugin2", hint_skip(&count)),
                        ("fooPlugin3", hint_queue(&count)), // ignored
                    ],
                )]),
            },
            Case {
                name: "If event is specific Node update event, queueing hint function for NodeUpdate/UpdateNodeLabel is also executed",
                unschedulable: &["fooPlugin1", "fooPlugin2"],
                pending: &[],
                event: ClusterEvent::new(EventResource::Node, ActionType::UPDATE_NODE_LABEL),
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 1,
                map: hints(vec![
                    (
                        ClusterEvent::new(EventResource::Node, ActionType::UPDATE_NODE_LABEL),
                        vec![("fooPlugin1", hint_queue(&count)), ("fooPlugin2", hint_queue(&count))],
                    ),
                    (
                        ClusterEvent::new(EventResource::Node, ActionType::UPDATE),
                        vec![("fooPlugin1", hint_queue(&count))],
                    ),
                    // not executed because NodeAdd is unrelated.
                    (NODE_ADD, vec![("fooPlugin1", hint_queue(&count))]),
                ]),
            },
            Case {
                name: "If event with '*' Resource, queueing hint function for specified Resource is also executed",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: NODE_ADD,
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 1,
                map: hints(vec![(
                    ClusterEvent::new(EventResource::WildCard, ActionType::ADD),
                    vec![("fooPlugin1", hint_queue(&count))],
                )]),
            },
            Case {
                name: "If event is a wildcard one, queueing hint function for all kinds of events is executed",
                unschedulable: &["fooPlugin1"],
                pending: &[],
                event: ClusterEvent::new(
                    EventResource::Node,
                    ActionType::UPDATE_NODE_LABEL | ActionType::UPDATE_NODE_TAINT,
                ),
                new_obj: node_obj(),
                expected: QueueingStrategy::QueueAfterBackoff,
                expected_execution_count: 1,
                map: hints(vec![(
                    ClusterEvent::new(EventResource::WildCard, ActionType::ALL),
                    vec![("fooPlugin1", hint_queue(&count))],
                )]),
            },
        ];
        let _ = node;
        for c in cases {
            count.store(0, Ordering::SeqCst);
            let q = new_test_queue(c.map, Arc::new(RealClock));
            let mut p_info = QueuedPodInfo::for_lookup(pod1.clone(), c.unschedulable);
            p_info.pending_plugins = c.pending.iter().map(|s| s.to_string()).collect();
            let actual = q.is_pod_worth_requeuing(&p_info, &c.event, None, c.new_obj.as_deref());
            assert_eq!(actual, c.expected, "{}: isPodWorthRequeuing()", c.name);
            assert_eq!(
                count.load(Ordering::SeqCst),
                c.expected_execution_count,
                "{}: executed queueing hint functions",
                c.name
            );
        }
    }

    /// The API server defaults an unset `spec.schedulerName` to
    /// `default-scheduler` (`SetDefaults_PodSpec`); objects read straight from
    /// storage may still carry none. The hint map is keyed by profile name,
    /// so the lookup must use the same defaulting `responsibleForPod` does —
    /// otherwise every event degrades to "no map registered" → backoff.
    #[test]
    fn is_pod_worth_requeuing_defaults_unset_scheduler_name() {
        let count = Arc::new(AtomicUsize::new(0));
        let q = new_test_queue(
            hints(vec![(NODE_ADD, vec![("fooPlugin1", hint_skip(&count))])]),
            Arc::new(RealClock),
        );
        let pod = mk_pod("pod1", "ns1", "1");
        assert!(pod.spec.as_ref().unwrap().scheduler_name.is_none());
        let p_info = QueuedPodInfo::for_lookup(pod, &["fooPlugin1"]);
        let node = mk_node();
        assert_eq!(
            q.is_pod_worth_requeuing(&p_info, &NODE_ADD, None, Some(&node)),
            QueueingStrategy::QueueSkip,
            "the default profile's hint must decide, not the missing-map fallback"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    /// `TestPriorityQueue_AddUnschedulableIfNotPresent` (`:945-973`), minus
    /// the nominator assertion.
    #[tokio::test]
    async fn add_unschedulable_if_not_present() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let unschedulable = mk_pod_prio("up", "ns1", "upns1", 0);
        let high_pri = mk_pod_prio("hpp", "ns1", "hppns1", 100000);
        // insert unschedulablePod and pop right after that because the
        // scheduling queue records it as in-flight.
        q.add(unschedulable.clone());
        let p = q.pop().await.unwrap();
        assert_eq!(p.pod.metadata.name, "up");

        q.add(high_pri.clone());
        q.add_unschedulable_if_not_present(
            QueuedPodInfo::for_lookup(unschedulable.clone(), &["plugin"]),
            q.scheduling_cycle(),
        )
        .unwrap();
        let p = q.pop().await.unwrap();
        assert_eq!(p.pod.metadata.name, "hpp");
        // unschedulablePod is inserted to the unschedulable pool because no
        // events happened during scheduling.
        assert_eq!(
            q.get_unschedulable_pod(&unschedulable)
                .map(|p| p.metadata.name),
            Some("up".to_string())
        );
    }

    /// `TestPriorityQueue_AddUnschedulableIfNotPresent_Backoff` (`:979-1037`).
    #[tokio::test]
    async fn add_unschedulable_if_not_present_backoff() {
        let clock = Arc::new(FakeClock::new(SystemTime::now()));
        let q = new_test_queue(HashMap::new(), clock.clone());
        let total = 10;
        let mut expected = Vec::new();
        for i in 0..total {
            let p = mk_pod_prio(
                &format!("pod{i}"),
                &format!("ns{i}"),
                &format!("upns{i}"),
                i as i32,
            );
            expected.push(p.clone());
            q.add(p);
        }
        // Pop all pods except for the first one
        for i in (1..total).rev() {
            let p = q.pop().await.unwrap();
            assert_eq!(p.pod.metadata.name, expected[i].metadata.name);
        }
        // move all pods to active queue when we were trying to schedule them
        q.move_all_to_active_or_backoff_queue(&EVENT_UNSCHEDULABLE_TIMEOUT, None, None, None);
        let old_cycle = q.scheduling_cycle();
        let first = q.pop().await.unwrap();
        assert_eq!(first.pod.metadata.name, expected[0].metadata.name);
        // mark pods[1] ~ pods[totalNum-1] as unschedulable and add them back
        for p in expected.iter().skip(1) {
            q.add_unschedulable_if_not_present(
                QueuedPodInfo::for_lookup(p.clone(), &["plugin"]),
                old_cycle,
            )
            .unwrap();
        }
        // Since there was a move request at the same cycle as "oldCycle",
        // these pods should be in the backoff queue.
        for p in expected.iter().skip(1) {
            assert!(
                q.backoff_q_has(p),
                "Expected {} to be added to backoffQ.",
                p.metadata.name
            );
        }
    }

    /// `TestPriorityQueue_MoveAllToActiveOrBackoffQueueWithQueueingHint`
    /// (`:1910-2062`), the non-gated rows (PreEnqueue plugins are covered by
    /// `gated_pods_follow_their_gating_plugin_events` below).
    #[tokio::test]
    async fn move_all_to_active_or_backoff_queue_with_queueing_hint() {
        let now = UNIX_EPOCH + Duration::from_secs(1_735_689_600); // 2025-01-01
        let count = Arc::new(AtomicUsize::new(0));
        let p = mk_pod("pod1", "ns1", "1");
        struct Case {
            name: &'static str,
            unschedulable: &'static [&'static str],
            pending: &'static [&'static str],
            hint: QueueingHintFn,
            duration: Duration,
            trigger_event: Option<ClusterEvent>,
            expected_q: &'static str,
        }
        let cases = vec![
            Case {
                // This pod has PendingPlugins and hence will be pushed directly to activeQ
                name: "Queue queues pod to activeQ",
                unschedulable: &[],
                pending: &["foo"],
                hint: hint_queue(&count),
                duration: Duration::ZERO,
                trigger_event: None,
                expected_q: ACTIVE_Q,
            },
            Case {
                // needs UnschedulableCount to make it backing off.
                name: "Queue queues pod to backoffQ if Pod is backing off",
                unschedulable: &["foo"],
                pending: &[],
                hint: hint_queue(&count),
                duration: Duration::ZERO,
                trigger_event: None,
                expected_q: BACKOFF_Q,
            },
            Case {
                // The pod failed once, which gets DefaultPodInitialBackoffDuration
                // as the penalty; +1s because the pod is still backing off if
                // `{backoff expiration time} == truncate({current time})`.
                name: "Queue queues pod to activeQ if Pod is not backing off",
                unschedulable: &["foo"],
                pending: &[],
                hint: hint_queue(&count),
                duration: DEFAULT_POD_INITIAL_BACKOFF_DURATION + Duration::from_secs(1),
                trigger_event: None,
                expected_q: ACTIVE_Q,
            },
            Case {
                name: "Skip queues pod to unschedulablePods",
                unschedulable: &["foo"],
                pending: &[],
                hint: hint_skip(&count),
                duration: Duration::ZERO,
                trigger_event: None,
                expected_q: UNSCHEDULABLE_Q,
            },
            Case {
                name: "Queue queues pod to backoffQ if Pod is not gated and the event is wildcard",
                unschedulable: &["foo"],
                pending: &[],
                hint: Arc::new(|_, _, _| {
                    Err(
                        "QueueingHintFn should not be called as trigger event is wildcard"
                            .to_string(),
                    )
                }),
                duration: Duration::ZERO,
                trigger_event: Some(EVENT_UNSCHEDULABLE_TIMEOUT),
                expected_q: BACKOFF_Q,
            },
        ];
        for c in cases {
            let clock = Arc::new(FakeClock::new(now));
            let q = new_test_queue(
                hints(vec![(NODE_ADD, vec![("foo", c.hint)])]),
                clock.clone(),
            );
            q.add(p.clone());
            let popped = q.pop().await.unwrap();
            assert_eq!(popped.pod.metadata.name, "pod1", "{}", c.name);
            let mut p_info = QueuedPodInfo::for_lookup(p.clone(), c.unschedulable);
            p_info.pending_plugins = c.pending.iter().map(|s| s.to_string()).collect();
            // add to unsched pod pool
            q.add_unschedulable_if_not_present(p_info, q.scheduling_cycle())
                .unwrap();
            clock.step(c.duration);
            let event = c.trigger_event.unwrap_or(NODE_ADD);
            q.move_all_to_active_or_backoff_queue(&event, None, None, None);
            match c.expected_q {
                BACKOFF_Q => assert_ne!(
                    q.backoff_q_len(),
                    0,
                    "{}: expected pod to be queued to backoffQ",
                    c.name
                ),
                ACTIVE_Q => assert_ne!(
                    q.active_q_len(),
                    0,
                    "{}: expected pod to be queued to activeQ",
                    c.name
                ),
                _ => assert!(
                    q.get_unschedulable_pod(&p).is_some(),
                    "{}: expected pod in unschedulablePods",
                    c.name
                ),
            }
        }
    }

    /// The gated rows of
    /// `TestPriorityQueue_MoveAllToActiveOrBackoffQueueWithQueueingHint`
    /// (`:1964-2000`) with a `preEnqueuePlugin{allowlists}`-style gate.
    struct AllowlistGate {
        name: &'static str,
        allow: &'static [&'static str],
    }
    impl PreEnqueuePlugin for AllowlistGate {
        fn name(&self) -> &str {
            self.name
        }
        fn pre_enqueue(&self, pod: &Pod) -> Result<(), String> {
            let labels = pod.metadata.labels.clone().unwrap_or_default();
            if self.allow.iter().any(|a| labels.contains_key(*a)) {
                Ok(())
            } else {
                Err(format!("pod is gated by {}", self.name))
            }
        }
    }

    #[tokio::test]
    async fn gated_pods_follow_their_gating_plugin_events() {
        let now = UNIX_EPOCH + Duration::from_secs(1_735_689_600);
        let gated_pod = mk_pod("pod1", "ns1", "1");
        let count = Arc::new(AtomicUsize::new(0));
        // (gating plugin events, trigger event, hint, expected queue)
        let cases: Vec<(&str, &'static str, ClusterEvent, QueueingHintFn, &str)> = vec![
            // The hint must not be called: the gating plugin isn't interested
            // in NodeAdd, so the queue keeps the Pod in unschedQ.
            (
                "QueueHintFunction is not called when Pod is gated by the plugin that isn't interested in the event",
                "gate-pod-update",
                NODE_ADD,
                Arc::new(|_, _, _| Err("QueueingHintFn should not be called as pod is gated".to_string())),
                UNSCHEDULABLE_Q,
            ),
            // The gating plugin ("foo") is interested in NodeAdd, so the hint
            // runs; the Pod stays gated and therefore stays in unschedQ.
            (
                "QueueHintFunction is called when Pod is gated by the plugin that is interested in the event",
                "foo",
                NODE_ADD,
                hint_queue(&count),
                UNSCHEDULABLE_Q,
            ),
            // Wildcard events always re-evaluate; the Pod is still gated.
            (
                "Queue queues pod to unschedulableQ when Pod is gated and the event is wildcard",
                "foo",
                EVENT_UNSCHEDULABLE_TIMEOUT,
                Arc::new(|_, _, _| Err("should not be called".to_string())),
                UNSCHEDULABLE_Q,
            ),
        ];
        for (name, gate, event, hint, expected_q) in cases {
            count.store(0, Ordering::SeqCst);
            let hint_map = hints(vec![
                (NODE_ADD, vec![("foo", hint)]),
                (
                    EVENT_UNSCHEDULED_POD_UPDATE,
                    vec![("gate-pod-update", hint_queue(&count))],
                ),
            ]);
            let pre_enqueue: PreEnqueuePluginMap = HashMap::from([(
                crate::profile::DEFAULT_SCHEDULER_NAME.to_string(),
                vec![Arc::new(AllowlistGate {
                    name: gate,
                    allow: &["allow"],
                }) as Arc<dyn PreEnqueuePlugin>],
            )]);
            let q = PriorityQueue::new(
                priority_sort_less(),
                PriorityQueueOptions {
                    clock: Arc::new(FakeClock::new(now)),
                    queueing_hint_map: hint_map,
                    pre_enqueue_plugin_map: pre_enqueue,
                    ..Default::default()
                },
            );
            q.add(gated_pod.clone());
            // gated at Add → straight to unschedulablePods, never popped
            assert_eq!(q.active_q_len(), 0, "{name}");
            let info = q.get_pod("pod1", "ns1").unwrap();
            assert_eq!(info.gating_plugin, gate, "{name}");
            assert!(info.unschedulable_plugins.contains(gate), "{name}");
            q.move_all_to_active_or_backoff_queue(&event, None, None, None);
            match expected_q {
                UNSCHEDULABLE_Q => assert!(q.get_unschedulable_pod(&gated_pod).is_some(), "{name}"),
                other => panic!("{name}: unexpected queue {other}"),
            }
            if gate == "foo" && event == NODE_ADD {
                assert_eq!(
                    count.load(Ordering::SeqCst),
                    1,
                    "{name}: hint should run once"
                );
            }
        }
        // Ungating: the gate's Pod/Update event with a now-allowed pod moves it on.
        let hint_map = hints(vec![(
            EVENT_UNSCHEDULED_POD_UPDATE,
            vec![("gate", super::super::hints::default_queueing_hint_fn())],
        )]);
        let pre_enqueue: PreEnqueuePluginMap = HashMap::from([(
            crate::profile::DEFAULT_SCHEDULER_NAME.to_string(),
            vec![Arc::new(AllowlistGate {
                name: "gate",
                allow: &["allow"],
            }) as Arc<dyn PreEnqueuePlugin>],
        )]);
        let q = PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                clock: Arc::new(FakeClock::new(now)),
                queueing_hint_map: hint_map,
                pre_enqueue_plugin_map: pre_enqueue,
                ..Default::default()
            },
        );
        q.add(gated_pod.clone());
        assert!(q.get_unschedulable_pod(&gated_pod).is_some());
        let mut ungated = gated_pod.clone();
        ungated.metadata.labels = Some(HashMap::from([("allow".to_string(), "".to_string())]));
        q.update(Some(&gated_pod), &ungated);
        assert_eq!(
            q.active_q_len(),
            1,
            "ungated pod moves to activeQ (not backing off: never attempted)"
        );
        assert!(!q.get_pod("pod1", "ns1").unwrap().gated());
    }

    /// `Test_InFlightPods` (`:190-885`), the QHint-enabled rows.
    #[tokio::test]
    async fn in_flight_pods() {
        let pod1 = mk_pod("targetpod", "", "pod1");
        let pod2 = mk_pod("targetpod2", "", "pod2");
        let pod3 = mk_pod("targetpod3", "", "pod3");
        let count = Arc::new(AtomicUsize::new(0));

        #[allow(clippy::large_enum_variant)]
        enum Action {
            EventHappens(ClusterEvent),
            PodPopped(Pod),
            PodEnqueued(QueuedPodInfo),
        }
        use Action::*;
        #[allow(clippy::large_enum_variant)]
        enum Want {
            Pod(Pod),
            Event(ClusterEvent),
        }
        struct Case {
            name: &'static str,
            map: QueueingHintMapPerProfile,
            initial_pods: Vec<Pod>,
            actions: Vec<Action>,
            want_in_flight_pods: Vec<Pod>,
            want_in_flight_events: Vec<Want>,
            want_backoff_q: Option<Vec<&'static str>>,
            want_unsched: Vec<&'static str>,
        }
        let q_all = |count: &Arc<AtomicUsize>| {
            hints(vec![
                (PV_ADD, vec![("fooPlugin1", hint_queue(count))]),
                (NODE_ADD, vec![("fooPlugin1", hint_queue(count))]),
                (PVC_ADD, vec![("fooPlugin1", hint_queue(count))]),
                (CSI_NODE_UPDATE, vec![("fooPlugin1", hint_queue(count))]),
                (
                    EVENT_ASSIGNED_POD_ADD,
                    vec![("fooPlugin1", hint_queue(count))],
                ),
            ])
        };
        let cases = vec![
            Case {
                name: "Pod and interested events are registered in inFlightPods/inFlightEvents",
                map: hints(vec![(PV_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    // not added to inFlightEvents: no inFlightPods yet.
                    EventHappens(PVC_ADD),
                    PodPopped(pod1.clone()),
                    // gets added for the pod.
                    EventHappens(PV_ADD),
                    // not added: no plugin is interested in PvUpdate.
                    EventHappens(PV_UPDATE),
                ],
                want_in_flight_pods: vec![pod1.clone()],
                want_in_flight_events: vec![Want::Pod(pod1.clone()), Want::Event(PV_ADD)],
                want_backoff_q: None,
                want_unsched: vec![],
            },
            Case {
                name: "Pod, registered in inFlightPods, is enqueued back to backoffQ",
                map: q_all(&count),
                initial_pods: vec![pod1.clone(), pod2.clone()],
                actions: vec![
                    EventHappens(PVC_ADD),
                    PodPopped(pod1.clone()),
                    EventHappens(PV_ADD),
                    PodPopped(pod2.clone()),
                    EventHappens(NODE_ADD),
                    // requeued to backoffQ immediately: no unschedulable plugin
                    // registered → an unexpected error (e.g. network).
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &[])),
                ],
                want_in_flight_pods: vec![pod2.clone()],
                want_in_flight_events: vec![Want::Pod(pod2.clone()), Want::Event(NODE_ADD)],
                want_backoff_q: Some(vec!["targetpod"]),
                want_unsched: vec![],
            },
            Case {
                name: "All Pods registered in inFlightPods are enqueued back to activeQ",
                map: q_all(&count),
                initial_pods: vec![pod1.clone(), pod2.clone()],
                actions: vec![
                    EventHappens(PVC_ADD),
                    PodPopped(pod1.clone()),
                    EventHappens(PV_ADD),
                    PodPopped(pod2.clone()),
                    EventHappens(NODE_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &[])),
                    EventHappens(CSI_NODE_UPDATE),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod2.clone(), &[])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: Some(vec!["targetpod", "targetpod2"]),
                want_unsched: vec![],
            },
            Case {
                name: "One intermediate Pod registered in inFlightPods is enqueued back to activeQ",
                map: q_all(&count),
                initial_pods: vec![pod1.clone(), pod2.clone(), pod3.clone()],
                actions: vec![
                    EventHappens(PVC_ADD),
                    PodPopped(pod1.clone()),
                    EventHappens(PV_ADD),
                    PodPopped(pod2.clone()),
                    EventHappens(NODE_ADD),
                    // This Pod won't be requeued again.
                    PodPopped(pod3.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod2.clone(), &[])),
                ],
                want_in_flight_pods: vec![pod1.clone(), pod3.clone()],
                want_in_flight_events: vec![
                    Want::Pod(pod1.clone()),
                    Want::Event(PV_ADD),
                    Want::Event(NODE_ADD),
                    Want::Pod(pod3.clone()),
                    Want::Event(EVENT_ASSIGNED_POD_ADD),
                ],
                want_backoff_q: Some(vec!["targetpod2"]),
                want_unsched: vec![],
            },
            Case {
                name: "events before popping Pod are ignored when Pod is enqueued back to queue",
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_skip(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    EventHappens(EVENT_UNSCHEDULABLE_TIMEOUT),
                    PodPopped(pod1.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    // not requeued to activeQ/backoffQ because fooPlugin1 returns QueueSkip.
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &["fooPlugin1"])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: None,
                want_unsched: vec!["targetpod"],
            },
            Case {
                name: "pod is enqueued to backoff if no failed plugin",
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    PodPopped(pod1.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &[])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: Some(vec!["targetpod"]),
                want_unsched: vec![],
            },
            Case {
                name: "pod is enqueued to unschedulable pod pool if no events that can make the pod schedulable",
                // fooPlugin1 has no queueing hint function for NodeAdd.
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    PodPopped(pod1.clone()),
                    EventHappens(NODE_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &["fooPlugin1"])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: None,
                want_unsched: vec!["targetpod"],
            },
            Case {
                name: "pod is enqueued to unschedulable pod pool because the failed plugin has a hint fn but it returns Skip",
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_skip(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    PodPopped(pod1.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &["fooPlugin1"])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: None,
                want_unsched: vec!["targetpod"],
            },
            Case {
                name: "pod is enqueued to activeQ because the failed plugin has a hint fn and it returns Queue",
                map: hints(vec![(EVENT_ASSIGNED_POD_ADD, vec![("fooPlugin1", hint_queue(&count))])]),
                initial_pods: vec![pod1.clone()],
                actions: vec![
                    PodPopped(pod1.clone()),
                    EventHappens(EVENT_ASSIGNED_POD_ADD),
                    PodEnqueued(QueuedPodInfo::for_lookup(pod1.clone(), &["fooPlugin1"])),
                ],
                want_in_flight_pods: vec![],
                want_in_flight_events: vec![],
                want_backoff_q: Some(vec!["targetpod"]),
                want_unsched: vec![],
            },
        ];
        for c in cases {
            let clock = Arc::new(FakeClock::new(SystemTime::now()));
            let q = new_test_queue(c.map, clock.clone());
            for p in &c.initial_pods {
                q.add(p.clone());
                clock.step(Duration::from_secs(1));
            }
            for a in c.actions {
                match a {
                    PodPopped(p) => {
                        let popped = q.pop().await.unwrap();
                        assert_eq!(
                            popped.pod.metadata.uid, p.metadata.uid,
                            "{}: unexpected popped pod",
                            c.name
                        );
                    }
                    EventHappens(ev) => {
                        q.move_all_to_active_or_backoff_queue(&ev, None, None, None)
                    }
                    PodEnqueued(p) => q
                        .add_unschedulable_if_not_present(p, q.scheduling_cycle())
                        .unwrap(),
                }
            }
            let mut got: Vec<String> = q
                .in_flight_pods()
                .iter()
                .map(|p| p.metadata.uid.clone())
                .collect();
            got.sort();
            let mut want: Vec<String> = c
                .want_in_flight_pods
                .iter()
                .map(|p| p.metadata.uid.clone())
                .collect();
            want.sort();
            assert_eq!(got, want, "{}: inFlightPods", c.name);

            let got_events: Vec<String> = q
                .in_flight_events()
                .iter()
                .map(|e| match e {
                    InFlightEntry::Pod { pod, .. } => format!("pod:{}", pod.metadata.uid),
                    InFlightEntry::Event { event, .. } => format!("event:{}", event.event.label()),
                })
                .collect();
            let want_events: Vec<String> = c
                .want_in_flight_events
                .iter()
                .map(|w| match w {
                    Want::Pod(p) => format!("pod:{}", p.metadata.uid),
                    Want::Event(e) => format!("event:{}", e.label()),
                })
                .collect();
            assert_eq!(got_events, want_events, "{}: inFlightEvents", c.name);

            if let Some(want) = c.want_backoff_q {
                let mut names: Vec<String> = q
                    .pods_in_backoff_q()
                    .iter()
                    .map(|p| p.metadata.name.clone())
                    .collect();
                names.sort();
                let mut want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
                want.sort();
                assert_eq!(names, want, "{}: backoffQ pod names", c.name);
            }
            for name in c.want_unsched {
                assert!(
                    q.get_unschedulable_pod(&mk_pod(name, "", "")).is_some(),
                    "{}: Pod {name} was not found in the unschedulablePods.",
                    c.name
                );
            }
        }
    }

    /// `TestPriorityQueue_Update` (`:1123-1316`), condensed to the rows that
    /// don't need the nominator: activeQ update in place, unschedulable pod
    /// update with hints, unknown pod → activeQ.
    #[tokio::test]
    async fn update_moves_between_queues() {
        let count = Arc::new(AtomicUsize::new(0));
        let clock = Arc::new(FakeClock::new(SystemTime::now()));
        let map = hints(vec![(
            ClusterEvent::new(EventResource::Pod, ActionType::UPDATE_POD_LABEL),
            vec![("fooPlugin1", hint_queue(&count))],
        )]);
        let q = new_test_queue(map, clock.clone());
        let pod = mk_pod("p", "ns", "u1");
        // unknown pod: added to activeQ
        q.update(None, &pod);
        assert_eq!(q.active_q_len(), 1);
        // in activeQ: updated in place, still one entry
        let mut labelled = pod.clone();
        labelled.metadata.labels = Some(HashMap::from([("a".to_string(), "b".to_string())]));
        q.update(Some(&pod), &labelled);
        assert_eq!(q.active_q_len(), 1);
        assert_eq!(
            q.get_pod("p", "ns").unwrap().pod.metadata.labels,
            labelled.metadata.labels
        );
        // pop, fail with fooPlugin1 → unschedulable pool
        let popped = q.pop().await.unwrap();
        q.add_unschedulable_if_not_present(
            rejected(popped.clone(), &["fooPlugin1"]),
            q.scheduling_cycle(),
        )
        .unwrap();
        assert!(q.get_unschedulable_pod(&pod).is_some());
        // an unrelated update (general Pod/Update) → stays unschedulable
        let mut annotated = labelled.clone();
        annotated.metadata.annotations = Some(HashMap::from([("x".to_string(), "y".to_string())]));
        q.update(Some(&labelled), &annotated);
        assert!(q.get_unschedulable_pod(&pod).is_some());
        assert_eq!(count.load(Ordering::SeqCst), 0);
        // a label update → fooPlugin1's hint says Queue → backing off → backoffQ
        let mut relabelled = annotated.clone();
        relabelled.metadata.labels = Some(HashMap::from([("a".to_string(), "c".to_string())]));
        q.update(Some(&annotated), &relabelled);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(q.backoff_q_has(&pod));
        assert!(q.get_unschedulable_pod(&pod).is_none());
        // in-flight pod: update only records the event
        clock.step(Duration::from_secs(2));
        q.flush_backoff_q_completed();
        let popped = q.pop().await.unwrap();
        assert_eq!(popped.attempts, 2);
        let mut again = relabelled.clone();
        again.metadata.labels = Some(HashMap::from([("a".to_string(), "d".to_string())]));
        q.update(Some(&relabelled), &again);
        assert_eq!(q.active_q_len(), 0);
        assert_eq!(q.in_flight_events().len(), 2);
        q.add_unschedulable_if_not_present(
            rejected(popped.clone(), &["fooPlugin1"]),
            q.scheduling_cycle(),
        )
        .unwrap();
        // the recorded in-flight label event requeues it (after backoff)
        assert!(q.backoff_q_has(&pod));
    }

    /// `TestPriorityQueue_Delete` (`:1368-1390`).
    #[tokio::test]
    async fn delete_from_any_queue() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let a = mk_pod("a", "ns", "a");
        let b = mk_pod("b", "ns", "b");
        q.add(a.clone());
        q.add(b.clone());
        q.delete(&a);
        assert_eq!(q.active_q_len(), 1);
        let popped = q.pop().await.unwrap();
        assert_eq!(popped.pod.metadata.name, "b");
        q.add_unschedulable_if_not_present(
            QueuedPodInfo::for_lookup(b.clone(), &["x"]),
            q.scheduling_cycle(),
        )
        .unwrap();
        assert!(q.get_unschedulable_pod(&b).is_some());
        q.delete(&b);
        assert!(q.get_unschedulable_pod(&b).is_none());
        assert_eq!(
            q.pending_pods().1,
            "activeQ:0; backoffQ:0; unschedulablePods:0"
        );
    }

    /// `TestBackOffFlow` (`:3838-3917`): exponential backoff per attempt,
    /// capped at the max.
    #[tokio::test]
    async fn backoff_flow() {
        // Fixed start with a half-second fraction. The backoff expiry and
        // `now` are both aligned to the 1s ordering window, and each step
        // below advances the clock by `wanted - 1ms + 1s`, so a start time
        // taken from the wall clock lands the "window equality still
        // backing off" boundary on a whole second about 1 run in 1000 and
        // fails "pod should have left backoffQ".
        let clock = Arc::new(FakeClock::new(
            UNIX_EPOCH + Duration::from_millis(1_700_000_000_500),
        ));
        let q = new_test_queue(HashMap::new(), clock.clone());
        let pod = mk_pod("test-pod", "test-ns", "test-uid");
        q.add(pod.clone());
        let steps: [(&str, Duration); 5] = [
            ("1s", Duration::from_secs(1)),
            ("2s", Duration::from_secs(2)),
            ("4s", Duration::from_secs(4)),
            ("8s", Duration::from_secs(8)),
            ("10s", Duration::from_secs(10)),
        ];
        for (i, (name, wanted)) in steps.iter().enumerate() {
            let p = q.pop().await.unwrap();
            assert_eq!(p.attempts, i + 1, "{name}");
            q.add_unschedulable_if_not_present(
                rejected(p.clone(), &["plugin"]),
                q.scheduling_cycle(),
            )
            .unwrap();
            // An event moves it to backoffQ.
            q.move_all_to_active_or_backoff_queue(&EVENT_UNSCHEDULABLE_TIMEOUT, None, None, None);
            assert!(q.backoff_q_has(&pod), "{name}: pod should be in backoffQ");
            // Before backoff completes nothing moves (the window boundary
            // counts as still backing off).
            clock.step(*wanted - Duration::from_millis(1));
            q.flush_backoff_q_completed();
            assert!(
                q.backoff_q_has(&pod),
                "{name}: pod should still be backing off"
            );
            // After the window passes, it is flushed to activeQ.
            clock.step(Duration::from_secs(1));
            q.flush_backoff_q_completed();
            assert!(
                !q.backoff_q_has(&pod),
                "{name}: pod should have left backoffQ"
            );
            assert_eq!(q.active_q_len(), 1, "{name}");
        }
    }

    /// `TestPriorityQueue_Activate` (`:1392-1545`), the in-flight row: an
    /// Activate for a pod being scheduled registers `EventForceActivate`.
    #[tokio::test]
    async fn activate_in_flight_pod_registers_force_activate() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let pod = mk_pod("p", "ns", "u");
        q.add(pod.clone());
        let p = q.pop().await.unwrap();
        q.activate(std::slice::from_ref(&pod));
        assert_eq!(q.in_flight_events().len(), 2);
        q.add_unschedulable_if_not_present(rejected(p.clone(), &["plugin"]), q.scheduling_cycle())
            .unwrap();
        // ForceActivate targets this pod → queueAfterBackoff → backoffQ
        assert!(q.backoff_q_has(&pod));
        // Activate from backoffQ moves straight to activeQ.
        q.activate(std::slice::from_ref(&pod));
        assert_eq!(q.active_q_len(), 1);
        assert!(!q.backoff_q_has(&pod));
        // Unknown pod is ignored.
        q.activate(&[mk_pod("unknown", "ns", "x")]);
        assert_eq!(q.active_q_len(), 1);
    }

    /// `TestHighPriorityFlushUnschedulablePodsLeftover` (`:3006-3064`).
    #[tokio::test]
    async fn flush_unschedulable_pods_leftover_moves_old_pods() {
        let clock = Arc::new(FakeClock::new(SystemTime::now()));
        let q = new_test_queue(HashMap::new(), clock.clone());
        let high = mk_pod_prio("hp", "ns", "hp", 100);
        let med = mk_pod_prio("mp", "ns", "mp", 50);
        for p in [&high, &med] {
            q.add(p.clone());
            let popped = q.pop().await.unwrap();
            q.add_unschedulable_if_not_present(
                QueuedPodInfo::for_lookup(popped.pod, &["plugin"]),
                q.scheduling_cycle(),
            )
            .unwrap();
        }
        assert_eq!(q.unschedulable_pods().len(), 2);
        clock.step(DEFAULT_POD_MAX_IN_UNSCHEDULABLE_PODS_DURATION + Duration::from_secs(1));
        q.flush_unschedulable_pods_leftover();
        assert_eq!(q.unschedulable_pods().len(), 0);
        // Backoff has long expired → straight to activeQ, highest priority first.
        let p = q.pop().await.unwrap();
        assert_eq!(p.pod.metadata.name, "hp");
        let p = q.pop().await.unwrap();
        assert_eq!(p.pod.metadata.name, "mp");
    }

    #[tokio::test]
    async fn pop_blocks_until_add_and_returns_none_on_close() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let q2 = Arc::clone(&q);
        let h = tokio::spawn(async move { q2.pop().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        q.add(mk_pod("p", "ns", "u"));
        let popped = h.await.unwrap().unwrap();
        assert_eq!(popped.pod.metadata.name, "p");
        let q2 = Arc::clone(&q);
        let h = tokio::spawn(async move { q2.pop().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        q.close();
        assert!(h.await.unwrap().is_none());
    }

    #[test]
    fn calculate_backoff_duration_table() {
        let init = Duration::from_secs(1);
        let max = Duration::from_secs(10);
        assert_eq!(calculate_backoff_duration(init, max, 0), Duration::ZERO);
        assert_eq!(
            calculate_backoff_duration(init, max, 1),
            Duration::from_secs(1)
        );
        assert_eq!(
            calculate_backoff_duration(init, max, 2),
            Duration::from_secs(2)
        );
        assert_eq!(
            calculate_backoff_duration(init, max, 4),
            Duration::from_secs(8)
        );
        assert_eq!(
            calculate_backoff_duration(init, max, 5),
            Duration::from_secs(10)
        );
        assert_eq!(
            calculate_backoff_duration(init, max, 100),
            Duration::from_secs(10)
        );
    }

    // -- nominator tests (scheduling_queue_test.go:2565-2780) ---------------

    fn nominated_pod(name: &str, ns: &str, uid: &str, prio: i32, node: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": name, "namespace": ns, "uid": uid},
            "spec": {"priority": prio},
            "status": {"nominatedNodeName": node}
        }))
        .unwrap()
    }

    fn lister_of(pods: Vec<Pod>) -> PodLister {
        Arc::new(move |ns, name| {
            pods.iter()
                .find(|p| {
                    p.metadata.namespace.as_deref().unwrap_or("") == ns && p.metadata.name == name
                })
                .cloned()
        })
    }

    type NominatorState = (HashMap<String, Vec<PodRef>>, HashMap<String, String>);

    fn nominator_state(q: &PriorityQueue) -> NominatorState {
        let inner = q.inner.lock().unwrap();
        (
            inner.nominator.nominated_pods.clone(),
            inner.nominator.nominated_pod_to_node.clone(),
        )
    }

    fn expected_state(pairs: &[(&str, &Pod)]) -> NominatorState {
        let mut pods: HashMap<String, Vec<PodRef>> = HashMap::new();
        let mut to_node = HashMap::new();
        for (node, pod) in pairs {
            pods.entry(node.to_string())
                .or_default()
                .push(PodRef::from_pod(pod));
            to_node.insert(pod.metadata.uid.clone(), node.to_string());
        }
        (pods, to_node)
    }

    fn names(pods: &[Pod]) -> Vec<String> {
        pods.iter().map(|p| p.metadata.name.clone()).collect()
    }

    /// `TestPriorityQueue_NominatedPodsForNode` (`:2565-2589`).
    #[tokio::test]
    async fn nominated_pods_for_node() {
        let med = nominated_pod("mpp", "ns2", "mppns2", 50000, "node1");
        let unsched = nominated_pod("up", "ns1", "upns1", 0, "node1");
        let high = mk_pod_prio("hpp", "ns1", "hppns1", 100000);
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        q.add(med);
        q.add(unsched);
        q.add(high);
        assert_eq!(q.pop().await.unwrap().pod.metadata.name, "hpp");
        let mut got = q.nominated_pods_for_node("node1");
        assert_eq!(names(&got), vec!["mpp", "up"]);
        // The returned pods are copies.
        got[0].metadata.name = "not mpp".to_string();
        assert_eq!(
            names(&q.nominated_pods_for_node("node1")),
            vec!["mpp", "up"]
        );
        assert!(q.nominated_pods_for_node("node2").is_empty());
    }

    /// `TestPriorityQueue_NominatedPodDeleted` (`:2591-2650`).
    #[tokio::test]
    async fn nominated_pod_deleted() {
        let med = nominated_pod("mpp", "ns2", "mppns2", 50000, "node1");
        let high_nominated = nominated_pod("hpp", "ns1", "hppns1", 100000, "node1");
        let high = mk_pod_prio("hpp", "ns1", "hppns1", 100000);
        for (name, pod, deleted, want_len) in [
            ("alive pod gets added into PodNominator", med, false, 1),
            (
                "deleted pod shouldn't be added into PodNominator",
                high_nominated,
                true,
                0,
            ),
            (
                "pod without .status.nominatedPodName shouldn't be added into PodNominator",
                high,
                false,
                0,
            ),
        ] {
            let lister = lister_of(if deleted { vec![] } else { vec![pod.clone()] });
            let q = PriorityQueue::new(
                priority_sort_less(),
                PriorityQueueOptions {
                    pod_lister: Some(lister),
                    ..Default::default()
                },
            );
            q.add_nominated_pod(&pod, None);
            let node = pod
                .status
                .as_ref()
                .and_then(|s| s.nominated_node_name.clone())
                .unwrap_or_default();
            assert_eq!(q.nominated_pods_for_node(&node).len(), want_len, "{name}");
        }
    }

    /// `TestPriorityQueue_UpdateNominatedPodForNode` (`:2695-2780`).
    #[tokio::test]
    async fn update_nominated_pod_for_node() {
        let med = nominated_pod("mpp", "ns2", "mppns2", 50000, "node1");
        let unsched = nominated_pod("up", "ns1", "upns1", 0, "node1");
        let high = mk_pod_prio("hpp", "ns1", "hppns1", 100000);
        let mut scheduled = mk_pod_prio("sp", "ns1", "spns1", 0);
        scheduled.spec.as_mut().unwrap().node_name = Some("node1".to_string());
        let non_existent = nominated_pod("nonexistent", "ns1", "nens1", 0, "node1");
        let q = PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                pod_lister: Some(lister_of(vec![
                    med.clone(),
                    unsched.clone(),
                    high.clone(),
                    scheduled.clone(),
                ])),
                ..Default::default()
            },
        );
        q.add(med.clone());
        // Update `unsched` on a different node than specified in the pod.
        q.add_nominated_pod(&unsched, Some(&NominatingInfo::override_node("node5")));
        // Nominate a pod on a node that is not specified in the pod object.
        q.add_nominated_pod(&high, Some(&NominatingInfo::override_node("node2")));
        let expected = expected_state(&[("node1", &med), ("node2", &high), ("node5", &unsched)]);
        assert_eq!(nominator_state(&q), expected, "after adding pods");
        assert_eq!(q.pop().await.unwrap().pod.metadata.name, "mpp");
        // List of nominated pods shouldn't change after popping them from the queue.
        assert_eq!(nominator_state(&q), expected, "after popping pods");
        // Update one of the nominated pods that doesn't have nominatedNodeName
        // in the pod object.
        q.add_nominated_pod(&high, Some(&NominatingInfo::override_node("node4")));
        let expected = expected_state(&[("node1", &med), ("node4", &high), ("node5", &unsched)]);
        assert_eq!(nominator_state(&q), expected, "after updating pods");
        // Nominate a pod that was deleted from the informer cache: nothing changes.
        q.add_nominated_pod(&non_existent, Some(&NominatingInfo::override_node("node1")));
        assert_eq!(
            nominator_state(&q),
            expected,
            "after nominating a deleted pod"
        );
        // Nominate a pod that was already scheduled in the informer cache:
        // nothing changes.
        let mut scheduled_unbound = scheduled.clone();
        scheduled_unbound.spec.as_mut().unwrap().node_name = None;
        q.add_nominated_pod(
            &scheduled_unbound,
            Some(&NominatingInfo::override_node("node1")),
        );
        assert_eq!(
            nominator_state(&q),
            expected,
            "after nominating a scheduled pod"
        );
        // Delete a nominated pod that doesn't have nominatedNodeName in the pod object.
        q.delete_nominated_pod_if_exists(&high);
        let expected = expected_state(&[("node1", &med), ("node5", &unsched)]);
        assert_eq!(nominator_state(&q), expected, "after deleting pods");
    }

    /// `Delete` clears the nominator (scheduling_queue.go:1118) and an `Update`
    /// of a queued pod without `nominatedNodeName` keeps reserving the node
    /// recorded in memory (`UpdateNominatedPod`, nominator.go:110-136).
    #[tokio::test]
    async fn queue_delete_and_update_maintain_nominator() {
        let q = new_test_queue(HashMap::new(), Arc::new(RealClock));
        let pod = mk_pod_prio("p", "ns1", "p-uid", 0);
        q.add(pod.clone());
        q.add_nominated_pod(&pod, Some(&NominatingInfo::override_node("node1")));
        let mut updated = pod.clone();
        updated.metadata.resource_version = Some("2".to_string());
        q.update(Some(&pod), &updated);
        assert_eq!(q.nominated_pods_for_node("node1").len(), 1);
        q.delete(&updated);
        assert!(q.nominated_pods_for_node("node1").is_empty());
        assert!(q.nominated_nodes().is_empty());
    }
}
