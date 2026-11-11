//! Graceful node shutdown. Port of `pkg/kubelet/nodeshutdown`
//! (`nodeshutdown_manager.go`, `nodeshutdown_manager_linux.go`,
//! `storage.go`, `systemd/inhibit_linux.go`), release-1.35.
//!
//! Not ported (see the individual items): the real `systemd.DBusCon`
//! (needs a D-Bus client crate), the Windows manager, and wiring into the
//! kubelet (`kubelet.go:1092-1106`, `:1771`, `kubelet_node_status.go:694`).

pub mod linux;
pub mod storage;
pub mod systemd;

use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use k3rs_common::resources::{ObjectReference, Pod, PodCondition, PodStatus};
use k3rs_common::types::Phase;

use crate::config::ShutdownGracePeriodByPodPriority;

pub use linux::new_manager;

/// `scheduling.DefaultPriorityWhenNoDefaultClassExists`
/// (pkg/apis/scheduling/types.go:28).
pub const DEFAULT_PRIORITY_WHEN_NO_DEFAULT_CLASS_EXISTS: i32 = 0;
/// `scheduling.HighestUserDefinablePriority` (types.go:30).
pub const HIGHEST_USER_DEFINABLE_PRIORITY: i32 = 1_000_000_000;
/// `scheduling.SystemCriticalPriority` (types.go:32).
pub const SYSTEM_CRITICAL_PRIORITY: i32 = 2 * HIGHEST_USER_DEFINABLE_PRIORITY;

/// `NodeShutdownNotAdmittedReason` (nodeshutdown_manager.go:76).
pub const NODE_SHUTDOWN_NOT_ADMITTED_REASON: &str = "NodeShutdown";
const NODE_SHUTDOWN_NOT_ADMITTED_MESSAGE: &str = "Pod was rejected as the node is shutting down.";
/// `localStorageStateFile` (nodeshutdown_manager.go:79).
pub const LOCAL_STORAGE_STATE_FILE: &str = "graceful_node_shutdown_state";
const NODE_SHUTDOWN_REASON: &str = "Terminated";
const NODE_SHUTDOWN_MESSAGE: &str = "Pod was terminated in response to imminent node shutdown.";
/// `events.NodeShutdown` (pkg/kubelet/events/event.go:75).
pub const EVENT_NODE_SHUTDOWN: &str = "Shutdown";
/// `v1.PodReasonTerminationByKubelet` (core/v1/types.go:3553).
const POD_REASON_TERMINATION_BY_KUBELET: &str = "TerminationByKubelet";

/// `lifecycle.PodAdmitAttributes` (pkg/kubelet/lifecycle/interfaces.go).
#[derive(Debug, Clone)]
pub struct PodAdmitAttributes {
    pub pod: Arc<Pod>,
    pub other_pods: Vec<Arc<Pod>>,
}

/// `lifecycle.PodAdmitResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodAdmitResult {
    pub admit: bool,
    pub reason: String,
    pub message: String,
}

impl PodAdmitResult {
    pub(crate) fn admitted() -> Self {
        PodAdmitResult {
            admit: true,
            reason: String::new(),
            message: String::new(),
        }
    }
}

/// `Manager` (nodeshutdown_manager.go:41-47), including the embedded
/// `lifecycle.PodAdmitHandler`.
pub trait Manager: Send + Sync {
    fn admit(&self, attrs: Option<&PodAdmitAttributes>) -> PodAdmitResult;
    fn start(&self) -> Result<(), String>;
    fn shutdown_status(&self) -> Result<(), String>;
    /// Stands in for Go's `manager != managerStub{}` interface comparison.
    fn is_stub(&self) -> bool;
}

/// `eviction.ActivePodsFunc`.
pub type ActivePodsFunc = Arc<dyn Fn() -> Vec<Arc<Pod>> + Send + Sync>;
/// The `func(*v1.PodStatus)` status mutator passed to a [`KillPodFunc`].
pub type PodStatusFn<'a> = &'a mut dyn FnMut(&mut PodStatus);
/// `eviction.KillPodFunc` (pkg/kubelet/eviction/types.go).
pub type KillPodFunc =
    Arc<dyn Fn(&Pod, bool, Option<i64>, PodStatusFn<'_>) -> Result<(), String> + Send + Sync>;

/// `record.EventRecorder`, reduced to `Event`.
pub trait EventRecorder: Send + Sync {
    fn event(&self, object: &ObjectReference, event_type: &str, reason: &str, message: &str);
}

/// The `klog.Logger` calls the manager makes. `v` is the klog verbosity of an
/// `Info` call.
pub trait Logger: Send + Sync {
    fn info(&self, v: i32, msg: &str, kv: &[(&str, String)]);
    fn error(&self, err: Option<&str>, msg: &str, kv: &[(&str, String)]);
}

/// A [`Logger`] writing to `tracing` (klog V(>=1) maps to debug).
pub struct TracingLogger;

fn fmt_kv(kv: &[(&str, String)]) -> String {
    kv.iter()
        .map(|(k, v)| format!("{k}={v:?}"))
        .collect::<Vec<_>>()
        .join(" ")
}

impl Logger for TracingLogger {
    fn info(&self, v: i32, msg: &str, kv: &[(&str, String)]) {
        if v == 0 {
            tracing::info!("{msg} {}", fmt_kv(kv));
        } else {
            tracing::debug!("{msg} {}", fmt_kv(kv));
        }
    }
    fn error(&self, err: Option<&str>, msg: &str, kv: &[(&str, String)]) {
        tracing::error!("{msg} err={err:?} {}", fmt_kv(kv));
    }
}

/// `context.Context` as far as `WaitForAllPodsUnmount` uses it: cancellation
/// plus an optional deadline (real time, as `context.WithTimeout`).
pub struct Context {
    state: Mutex<Option<ContextError>>,
    cv: Condvar,
    deadline: Option<Instant>,
}

/// `ctx.Err()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextError {
    Canceled,
    DeadlineExceeded,
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContextError::Canceled => f.write_str("context canceled"),
            ContextError::DeadlineExceeded => f.write_str("context deadline exceeded"),
        }
    }
}

impl Context {
    /// `context.WithTimeout(context.Background(), d)`.
    pub fn with_timeout(d: Duration) -> Arc<Context> {
        Arc::new(Context {
            state: Mutex::new(None),
            cv: Condvar::new(),
            deadline: Some(Instant::now() + d),
        })
    }

    /// The cancel func.
    pub fn cancel(&self) {
        let mut s = self.state.lock().unwrap();
        if s.is_none() {
            *s = Some(ContextError::Canceled);
        }
        self.cv.notify_all();
    }

    /// `ctx.Err()`.
    pub fn err(&self) -> Option<ContextError> {
        let s = self.state.lock().unwrap();
        if s.is_some() {
            return *s;
        }
        match self.deadline {
            Some(d) if Instant::now() >= d => Some(ContextError::DeadlineExceeded),
            _ => None,
        }
    }

    /// Block for `d` or until the context is done, whichever is first.
    /// Returns `ctx.Err()` when the context ended first.
    pub fn wait_timeout(&self, d: Duration) -> Option<ContextError> {
        let until = Instant::now() + d;
        let mut s = self.state.lock().unwrap();
        loop {
            if s.is_some() {
                return *s;
            }
            let now = Instant::now();
            if let Some(dl) = self.deadline {
                if now >= dl {
                    return Some(ContextError::DeadlineExceeded);
                }
            }
            if now >= until {
                return None;
            }
            let mut wait = until - now;
            if let Some(dl) = self.deadline {
                wait = wait.min(dl - now);
            }
            s = self.cv.wait_timeout(s, wait).unwrap().0;
        }
    }
}

/// `volumemanager.VolumeManager`, reduced to `WaitForAllPodsUnmount`.
pub trait VolumeManager: Send + Sync {
    fn wait_for_all_pods_unmount(&self, ctx: &Context, pods: &[Arc<Pod>]) -> Result<(), String>;
}

/// A `clock.Timer` handle.
pub trait Timer: Send {
    /// `Stop`.
    fn stop(&self) -> bool;
}

/// `clock.Clock`, reduced to `Now` and `AfterFunc`. Go selects on
/// `timer.C()`; here the timer callback posts to the same channel the
/// completion goroutine posts to, which is the same first-wins `select`.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
    fn after_func(&self, d: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn Timer>;
}

/// `clock.RealClock{}`.
pub struct RealClock;

struct RealTimer {
    stopped: Arc<(Mutex<bool>, Condvar)>,
}

impl Timer for RealTimer {
    fn stop(&self) -> bool {
        let (m, cv) = &*self.stopped;
        let mut s = m.lock().unwrap();
        let was = !*s;
        *s = true;
        cv.notify_all();
        was
    }
}

impl Clock for RealClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn after_func(&self, d: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn Timer> {
        let stopped = Arc::new((Mutex::new(false), Condvar::new()));
        let s2 = stopped.clone();
        std::thread::spawn(move || {
            let (m, cv) = &*s2;
            let deadline = Instant::now() + d;
            let mut s = m.lock().unwrap();
            loop {
                if *s {
                    return;
                }
                let now = Instant::now();
                if now >= deadline {
                    *s = true;
                    drop(s);
                    f();
                    return;
                }
                s = cv.wait_timeout(s, deadline - now).unwrap().0;
            }
        });
        Box::new(RealTimer { stopped })
    }
}

/// `Config` (nodeshutdown_manager.go:50-63).
#[derive(Clone)]
pub struct Config {
    pub logger: Arc<dyn Logger>,
    pub volume_manager: Arc<dyn VolumeManager>,
    pub recorder: Arc<dyn EventRecorder>,
    pub node_ref: ObjectReference,
    pub get_pods_func: ActivePodsFunc,
    pub kill_pod_func: KillPodFunc,
    pub sync_node_status_func: Arc<dyn Fn() + Send + Sync>,
    pub shutdown_grace_period_requested: Duration,
    pub shutdown_grace_period_critical_pods: Duration,
    pub shutdown_grace_period_by_pod_priority: Vec<ShutdownGracePeriodByPodPriority>,
    pub state_directory: std::path::PathBuf,
    /// `nil` selects the real clock (`newPodManager`, :107-109).
    pub clock: Option<Arc<dyn Clock>>,
}

/// `managerStub` (nodeshutdown_manager.go:66-80).
pub struct ManagerStub;

impl Manager for ManagerStub {
    fn admit(&self, _attrs: Option<&PodAdmitAttributes>) -> PodAdmitResult {
        PodAdmitResult::admitted()
    }
    fn start(&self) -> Result<(), String> {
        Ok(())
    }
    fn shutdown_status(&self) -> Result<(), String> {
        Ok(())
    }
    fn is_stub(&self) -> bool {
        true
    }
}

/// `podManager` (nodeshutdown_manager.go:88-94): kills active pods by
/// priority.
pub struct PodManager {
    pub logger: Arc<dyn Logger>,
    pub shutdown_grace_period_by_pod_priority: Vec<ShutdownGracePeriodByPodPriority>,
    pub clock: Arc<dyn Clock>,
    pub kill_pod_func: KillPodFunc,
    pub volume_manager: Arc<dyn VolumeManager>,
}

/// `newPodManager` (nodeshutdown_manager.go:96-122).
pub fn new_pod_manager(conf: &Config) -> PodManager {
    let mut by_priority = conf.shutdown_grace_period_by_pod_priority.clone();

    // Migration from the original configuration.
    if !k3rs_common::feature_gates::enabled(
        k3rs_common::feature_gates::Feature::GracefulNodeShutdownBasedOnPodPriority,
    ) || by_priority.is_empty()
    {
        by_priority = migrate_config(
            conf.shutdown_grace_period_requested,
            conf.shutdown_grace_period_critical_pods,
        );
    }

    // Sort by priority from low to high.
    by_priority.sort_by_key(|p| p.priority);

    PodManager {
        logger: conf.logger.clone(),
        shutdown_grace_period_by_pod_priority: by_priority,
        clock: conf.clock.clone().unwrap_or_else(|| Arc::new(RealClock)),
        kill_pod_func: conf.kill_pod_func.clone(),
        volume_manager: conf.volume_manager.clone(),
    }
}

/// What the group's goroutines and timer post to the `select`
/// (nodeshutdown_manager.go:176-186).
enum GroupEvent {
    Done,
    TimedOut,
}

impl PodManager {
    /// `podManager.killPods` (nodeshutdown_manager.go:125-211).
    pub fn kill_pods(&self, active_pods: &[Arc<Pod>]) -> Result<(), String> {
        let groups = group_by_priority(&self.shutdown_grace_period_by_pod_priority, active_pods);
        for group in groups {
            // If there are no pods in a particular range, then do not wait
            // for pods in that priority range.
            if group.pods.is_empty() {
                continue;
            }

            let mut wg = Vec::with_capacity(group.pods.len());
            for pod in &group.pods {
                let pod = pod.clone();
                let logger = self.logger.clone();
                let kill = self.kill_pod_func.clone();
                let group_seconds = group.period.shutdown_grace_period_seconds;
                wg.push(std::thread::spawn(move || {
                    let mut grace_period_override = group_seconds;

                    // If the pod's spec specifies a termination gracePeriod
                    // which is less than the gracePeriodOverride calculated,
                    // use the pod spec termination gracePeriod.
                    if let Some(tgp) = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.termination_grace_period_seconds)
                    {
                        if tgp <= grace_period_override {
                            grace_period_override = tgp;
                        }
                    }

                    logger.info(
                        1,
                        "Shutdown manager killing pod with gracePeriod",
                        &[
                            ("pod", pod_key(&pod)),
                            ("gracePeriod", grace_period_override.to_string()),
                        ],
                    );

                    let generation = pod.metadata.generation.unwrap_or(0);
                    let mut mutate = |status: &mut PodStatus| {
                        // set the pod status to failed (unless it was already
                        // in a successful terminal phase)
                        if status.phase != Some(Phase::Succeeded) {
                            status.phase = Some(Phase::Failed);
                        }
                        status.message = Some(NODE_SHUTDOWN_MESSAGE.to_string());
                        status.reason = Some(NODE_SHUTDOWN_REASON.to_string());
                        let observed = calculate_pod_condition_observed_generation(
                            status,
                            generation,
                            "DisruptionTarget",
                        );
                        update_pod_condition(
                            status,
                            PodCondition {
                                condition_type: "DisruptionTarget".to_string(),
                                observed_generation: (observed != 0).then_some(observed),
                                status: "True".to_string(),
                                reason: Some(POD_REASON_TERMINATION_BY_KUBELET.to_string()),
                                message: Some(NODE_SHUTDOWN_MESSAGE.to_string()),
                                last_probe_time: None,
                                last_transition_time: None,
                            },
                        );
                    };
                    match kill(&pod, false, Some(grace_period_override), &mut mutate) {
                        Err(err) => logger.info(
                            1,
                            "Shutdown manager failed killing pod",
                            &[("pod", pod_key(&pod)), ("err", err)],
                        ),
                        Ok(()) => logger.info(
                            1,
                            "Shutdown manager finished killing pod",
                            &[("pod", pod_key(&pod))],
                        ),
                    }
                }));
            }

            // This duration determines how long the shutdown manager will
            // wait for the pods in this group to terminate before proceeding
            // to the next group.
            let secs = group.period.shutdown_grace_period_seconds.max(0) as u64;
            let group_termination_wait_duration = Duration::from_secs(secs);
            let (tx, rx) = mpsc::channel::<GroupEvent>();
            let tx_timer = tx.clone();
            let timer = self.clock.after_func(
                group_termination_wait_duration,
                Box::new(move || {
                    let _ = tx_timer.send(GroupEvent::TimedOut);
                }),
            );
            let ctx = Context::with_timeout(group_termination_wait_duration);
            {
                let ctx = ctx.clone();
                let volume_manager = self.volume_manager.clone();
                let logger = self.logger.clone();
                let pods = group.pods.clone();
                std::thread::spawn(move || {
                    for h in wg {
                        let _ = h.join();
                    }
                    // The signal to kill a Pod was sent successfully to all
                    // the pods, let's wait until all the volumes are unmounted
                    // from all the pods before continuing to the next group.
                    // This is done so that the CSI Driver (assuming that it's
                    // part of the highest group) has a chance to perform
                    // unmounts.
                    if let Err(err) = volume_manager.wait_for_all_pods_unmount(&ctx, &pods) {
                        let pod_identifiers: Vec<String> =
                            pods.iter().map(|p| pod_key(p)).collect();

                        // Waiting for volume teardown is done on a best basis
                        // effort, report an error and continue.
                        logger.error(
                            Some(&err),
                            "Failed while waiting for all the volumes belonging to Pods in this group to unmount",
                            &[("pods", format!("{pod_identifiers:?}"))],
                        );
                    }
                    ctx.cancel();
                    let _ = tx.send(GroupEvent::Done);
                });
            }

            let priority = group.period.priority.to_string();
            let grace = group.period.shutdown_grace_period_seconds.to_string();
            match rx.recv() {
                Ok(GroupEvent::Done) => {
                    timer.stop();
                    self.logger.info(
                        1,
                        "Done waiting for all pods in group to terminate",
                        &[("gracePeriod", grace), ("priority", priority)],
                    );
                }
                Ok(GroupEvent::TimedOut) | Err(_) => {
                    ctx.cancel();
                    self.logger.info(
                        1,
                        "Shutdown manager pod killing time out",
                        &[("gracePeriod", grace), ("priority", priority)],
                    );
                }
            }
        }

        Ok(())
    }

    /// `podManager.periodRequested` (nodeshutdown_manager.go:213-219).
    pub fn period_requested(&self) -> Duration {
        let sum: i64 = self
            .shutdown_grace_period_by_pod_priority
            .iter()
            .map(|p| p.shutdown_grace_period_seconds)
            .sum();
        Duration::from_secs(sum.max(0) as u64)
    }
}

fn pod_key(pod: &Pod) -> String {
    match pod.metadata.namespace.as_deref() {
        Some(ns) if !ns.is_empty() => format!("{ns}/{}", pod.metadata.name),
        _ => pod.metadata.name.clone(),
    }
}

/// `podutil.UpdatePodCondition` (pkg/api/v1/pod/util.go): shared with the
/// rest of the kubelet.
pub(crate) use k3rs_common::podutil::update_pod_condition;

/// `podutil.CalculatePodConditionObservedGeneration`
/// (pkg/api/v1/pod/util.go:533-546).
fn calculate_pod_condition_observed_generation(
    status: &PodStatus,
    generation: i64,
    condition_type: &str,
) -> i64 {
    if k3rs_common::feature_gates::gate_enabled("PodObservedGenerationTracking").unwrap_or(false) {
        return generation;
    }
    if let Some(conds) = &status.conditions {
        for c in conds {
            if c.condition_type == condition_type && c.observed_generation.unwrap_or(0) != 0 {
                return generation;
            }
        }
    }
    0
}

/// `migrateConfig` (nodeshutdown_manager.go:221-244).
pub fn migrate_config(
    shutdown_grace_period_requested: Duration,
    shutdown_grace_period_critical_pods: Duration,
) -> Vec<ShutdownGracePeriodByPodPriority> {
    if shutdown_grace_period_requested.is_zero() {
        return Vec::new();
    }
    let Some(default_priority) =
        shutdown_grace_period_requested.checked_sub(shutdown_grace_period_critical_pods)
    else {
        return Vec::new();
    };
    let Some(critical_priority) = shutdown_grace_period_requested.checked_sub(default_priority)
    else {
        return Vec::new();
    };
    vec![
        ShutdownGracePeriodByPodPriority {
            priority: DEFAULT_PRIORITY_WHEN_NO_DEFAULT_CLASS_EXISTS,
            shutdown_grace_period_seconds: default_priority.as_secs() as i64,
        },
        ShutdownGracePeriodByPodPriority {
            priority: SYSTEM_CRITICAL_PRIORITY,
            shutdown_grace_period_seconds: critical_priority.as_secs() as i64,
        },
    ]
}

/// `podShutdownGroup` (nodeshutdown_manager.go:246-249).
#[derive(Debug, Clone)]
pub struct PodShutdownGroup {
    pub period: ShutdownGracePeriodByPodPriority,
    pub pods: Vec<Arc<Pod>>,
}

/// `groupByPriority` (nodeshutdown_manager.go:251-294).
pub fn group_by_priority(
    shutdown_grace_period_by_pod_priority: &[ShutdownGracePeriodByPodPriority],
    pods: &[Arc<Pod>],
) -> Vec<PodShutdownGroup> {
    let mut groups: Vec<PodShutdownGroup> = shutdown_grace_period_by_pod_priority
        .iter()
        .map(|period| PodShutdownGroup {
            period: *period,
            pods: Vec::new(),
        })
        .collect();
    if groups.is_empty() {
        // Upstream would index groups[-1] here; NewManager never builds a
        // manager with no groups.
        return groups;
    }

    for pod in pods {
        let priority: i32 = pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);

        // Find the group index according to the priority (`sort.Search`).
        let mut index = groups.partition_point(|g| g.period.priority < priority);

        // 1. Those higher than the highest priority default to the highest priority
        // 2. Those lower than the lowest priority default to the lowest priority
        // 3. Those boundary priority default to the lower priority
        // if priority of pod is:
        //   groups[index-1].Priority <= pod priority < groups[index].Priority
        // in which case we want to pick lower one (i.e index-1)
        if index == groups.len() {
            index = groups.len() - 1;
        } else if index > 0 && groups[index].period.priority > priority {
            index -= 1;
        }

        groups[index].pods.push(pod.clone());
    }
    groups
}
