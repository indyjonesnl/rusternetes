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
pub struct BackoffQueue {
    pod_backoff_q: Heap<QueuedPodInfo>,
    pod_error_backoff_q: Heap<QueuedPodInfo>,
}

/// `unschedulablePods` (`unschedulable_pods.go:27-34`).
#[derive(Default)]
pub struct UnschedulablePods {
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
pub struct Inner {
    pub active_q: Heap<QueuedPodInfo>,
    pub backoff_q: BackoffQueue,
    pub unschedulable_pods: UnschedulablePods,
    /// `inFlightPods` (`active_queue.go:186-193`): UID → seq of the pod's
    /// entry in `in_flight_events`.
    pub in_flight_pods: HashMap<String, u64>,
    /// `inFlightEvents` (`active_queue.go:195-208`).
    pub in_flight_events: VecDeque<InFlightEntry>,
    pub next_seq: u64,
    /// `schedCycle` (`active_queue.go:210-212`).
    pub sched_cycle: i64,
    pub closed: bool,
    /// The embedded `*nominator` (`scheduling_queue.go:164`).
    pub nominator: Nominator,
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
    pub inner: Mutex<Inner>,
    /// `activeQueue.cond`: notified when a pod is added to activeQ (or, with
    /// `SchedulerPopFromBackoffQ`, to backoffQ).
    pub cond: Notify,
    pub clock: Arc<dyn Clock>,
    pub pod_initial_backoff: Duration,
    pub pod_max_backoff: Duration,
    pub pod_max_in_unschedulable_pods_duration: Duration,
    pub pre_enqueue_plugin_map: PreEnqueuePluginMap,
    pub queueing_hint_map: QueueingHintMapPerProfile,
    /// `pluginToEventsMap` (`:192-193`).
    pub plugin_to_events_map: HashMap<String, Vec<ClusterEvent>>,
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

    pub fn backoff_q_has(&self, pod: &Pod) -> bool {
        self.inner
            .lock()
            .unwrap()
            .backoff_q
            .has(&QueuedPodInfo::for_lookup(pod.clone(), &[]))
    }

    pub fn active_q_len(&self) -> usize {
        self.inner.lock().unwrap().active_q.len()
    }

    pub fn backoff_q_len(&self) -> usize {
        self.inner.lock().unwrap().backoff_q.len()
    }

    pub fn get_unschedulable_pod(&self, pod: &Pod) -> Option<Pod> {
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
pub fn calculate_backoff_duration(initial: Duration, max: Duration, count: usize) -> Duration {
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

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
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
