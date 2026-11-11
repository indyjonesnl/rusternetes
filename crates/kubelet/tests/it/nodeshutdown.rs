//! Ports of `pkg/kubelet/nodeshutdown/{nodeshutdown_manager_test.go,
//! nodeshutdown_manager_linux_test.go, storage_test.go}` (release-1.35).
//!
//! Not ported: `systemd/inhibit_linux_test.go` (`TestCurrentInhibitDelay`,
//! `TestInhibitShutdown`, `TestReloadLogindConf`, `TestMonitorShutdown`) --
//! they exercise the godbus-backed `DBusCon`, which has no Rust counterpart.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use k3rs_common::feature_gates::{with_feature, Feature};
use k3rs_common::resources::{ObjectReference, Pod, PodSpec, PodStatus};
use k3rs_common::types::Phase;
use k3rs_kubelet::config::ShutdownGracePeriodByPodPriority as Period;
use k3rs_kubelet::nodeshutdown::linux::{
    new_manager, set_system_dbus, DbusInhibiter, ManagerImpl, DBUS_RECONNECT_PERIOD,
};
use k3rs_kubelet::nodeshutdown::storage::{timestamp, zero_time, LocalStorage, State, Storage};
use k3rs_kubelet::nodeshutdown::systemd::InhibitLock;
use k3rs_kubelet::nodeshutdown::{
    group_by_priority, migrate_config, ActivePodsFunc, Clock, Config, Context, EventRecorder,
    KillPodFunc, Logger, PodManager, PodStatusFn, Timer, VolumeManager,
    DEFAULT_PRIORITY_WHEN_NO_DEFAULT_CLASS_EXISTS, HIGHEST_USER_DEFINABLE_PRIORITY,
    SYSTEM_CRITICAL_PRIORITY,
};
use serial_test::serial;

fn make_pod(name: &str, priority: i32, tgp: Option<i64>) -> Arc<Pod> {
    let mut pod = Pod::new(
        name,
        PodSpec {
            priority: Some(priority),
            termination_grace_period_seconds: tgp,
            ..Default::default()
        },
    );
    pod.metadata.uid = name.to_string();
    Arc::new(pod)
}

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

// ---- fakes -----------------------------------------------------------------

/// `testingclock.FakeClock`.
struct FakeClock {
    inner: Arc<Mutex<FakeClockInner>>,
}
type Waiter = (DateTime<Utc>, u64, Box<dyn FnOnce() + Send>);
struct FakeClockInner {
    now: DateTime<Utc>,
    next_id: u64,
    waiters: Vec<Waiter>,
}
struct FakeTimer {
    inner: Arc<Mutex<FakeClockInner>>,
    id: u64,
}
impl Timer for FakeTimer {
    fn stop(&self) -> bool {
        let mut i = self.inner.lock().unwrap();
        let before = i.waiters.len();
        let id = self.id;
        i.waiters.retain(|w| w.1 != id);
        i.waiters.len() != before
    }
}
impl FakeClock {
    fn new() -> Arc<FakeClock> {
        Arc::new(FakeClock {
            inner: Arc::new(Mutex::new(FakeClockInner {
                now: Utc::now(),
                next_id: 0,
                waiters: Vec::new(),
            })),
        })
    }
    fn step(&self, d: Duration) {
        let due = {
            let mut i = self.inner.lock().unwrap();
            i.now += chrono::Duration::from_std(d).unwrap();
            let now = i.now;
            let mut due = Vec::new();
            let mut rest = Vec::new();
            for w in i.waiters.drain(..) {
                if w.0 <= now {
                    due.push(w);
                } else {
                    rest.push(w);
                }
            }
            i.waiters = rest;
            due
        };
        for (_, _, f) in due {
            f();
        }
    }
}
impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        self.inner.lock().unwrap().now
    }
    fn after_func(&self, d: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn Timer> {
        let mut i = self.inner.lock().unwrap();
        let id = i.next_id;
        i.next_id += 1;
        let at = i.now + chrono::Duration::from_std(d).unwrap();
        i.waiters.push((at, id, f));
        Box::new(FakeTimer {
            inner: self.inner.clone(),
            id,
        })
    }
}

/// `volumemanager.FakeVolumeManager`'s `WaitForAllPodsUnmount`.
struct FakeVolumeManager {
    unmount_delay: Duration,
    unmount_error: Option<String>,
}
impl VolumeManager for FakeVolumeManager {
    fn wait_for_all_pods_unmount(&self, ctx: &Context, _pods: &[Arc<Pod>]) -> Result<(), String> {
        match ctx.wait_timeout(self.unmount_delay) {
            Some(e) => Err(e.to_string()),
            None => match &self.unmount_error {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            },
        }
    }
}

/// `record.FakeRecorder{}` (nil channel: events are dropped).
struct FakeRecorder;
impl EventRecorder for FakeRecorder {
    fn event(&self, _: &ObjectReference, _: &str, _: &str, _: &str) {}
}

/// `ktesting.NewLogger(... BufferLogs(true))`.
#[derive(Default)]
struct BufLogger {
    buf: Mutex<String>,
}
impl Logger for BufLogger {
    fn info(&self, _v: i32, msg: &str, kv: &[(&str, String)]) {
        self.buf
            .lock()
            .unwrap()
            .push_str(&format!("{msg} {kv:?}\n"));
    }
    fn error(&self, err: Option<&str>, msg: &str, kv: &[(&str, String)]) {
        self.buf
            .lock()
            .unwrap()
            .push_str(&format!("{msg} {err:?} {kv:?}\n"));
    }
}

#[derive(Default)]
struct FakeDbusState {
    did_inhibit_shutdown: bool,
    did_override_inhibit_delay: bool,
}
struct FakeDbus {
    current_inhibit_delay: Duration,
    override_system_inhibit_delay: Duration,
    shutdown_chan: Mutex<Option<mpsc::Receiver<bool>>>,
    state: Mutex<FakeDbusState>,
}
impl FakeDbus {
    fn new(current: Duration, override_to: Duration) -> Arc<FakeDbus> {
        Arc::new(FakeDbus {
            current_inhibit_delay: current,
            override_system_inhibit_delay: override_to,
            shutdown_chan: Mutex::new(None),
            state: Mutex::new(FakeDbusState::default()),
        })
    }
}
impl DbusInhibiter for FakeDbus {
    fn current_inhibit_delay(&self) -> Result<Duration, String> {
        if self.state.lock().unwrap().did_override_inhibit_delay {
            return Ok(self.override_system_inhibit_delay);
        }
        Ok(self.current_inhibit_delay)
    }
    fn inhibit_shutdown(&self) -> Result<InhibitLock, String> {
        self.state.lock().unwrap().did_inhibit_shutdown = true;
        Ok(InhibitLock(0))
    }
    fn release_inhibit_lock(&self, _: InhibitLock) -> Result<(), String> {
        Ok(())
    }
    fn reload_logind_conf(&self) -> Result<(), String> {
        Ok(())
    }
    fn monitor_shutdown(&self, _: &dyn Logger) -> Result<mpsc::Receiver<bool>, String> {
        Ok(self
            .shutdown_chan
            .lock()
            .unwrap()
            .take()
            .expect("shutdown channel"))
    }
    fn override_inhibit_delay(&self, _: Duration) -> Result<(), String> {
        self.state.lock().unwrap().did_override_inhibit_delay = true;
        Ok(())
    }
}

fn node_ref() -> ObjectReference {
    ObjectReference {
        kind: Some("Node".into()),
        name: Some("test".into()),
        uid: Some("test".into()),
        namespace: Some(String::new()),
        ..Default::default()
    }
}

fn base_config(
    get_pods: ActivePodsFunc,
    kill: KillPodFunc,
    requested: Duration,
    critical: Duration,
    clock: Option<Arc<dyn Clock>>,
) -> Config {
    Config {
        logger: Arc::new(BufLogger::default()),
        volume_manager: Arc::new(FakeVolumeManager {
            unmount_delay: Duration::ZERO,
            unmount_error: None,
        }),
        recorder: Arc::new(FakeRecorder),
        node_ref: node_ref(),
        get_pods_func: get_pods,
        kill_pod_func: kill,
        sync_node_status_func: Arc::new(|| {}),
        shutdown_grace_period_requested: requested,
        shutdown_grace_period_critical_pods: critical,
        shutdown_grace_period_by_pod_priority: Vec::new(),
        state_directory: std::env::temp_dir(),
        clock,
    }
}

// ---- nodeshutdown_manager_test.go -----------------------------------------

#[test]
fn test_migrate_config() {
    let p = |priority, s| Period {
        priority,
        shutdown_grace_period_seconds: s,
    };
    let d = DEFAULT_PRIORITY_WHEN_NO_DEFAULT_CLASS_EXISTS;
    let c = SYSTEM_CRITICAL_PRIORITY;
    // both shutdownGracePeriodRequested and shutdownGracePeriodCriticalPods
    assert_eq!(
        migrate_config(secs(300), secs(120)),
        vec![p(d, 180), p(c, 120)]
    );
    // only shutdownGracePeriodRequested
    assert_eq!(migrate_config(secs(100), secs(0)), vec![p(d, 100), p(c, 0)]);
    // empty configuration
    assert_eq!(migrate_config(secs(0), secs(0)), vec![]);
    // wrong configuration
    assert_eq!(migrate_config(secs(1), secs(100)), vec![]);
}

fn names(g: &k3rs_kubelet::nodeshutdown::PodShutdownGroup) -> Vec<String> {
    g.pods.iter().map(|p| p.metadata.name.clone()).collect()
}

#[test]
fn test_group_by_priority_migrate_config() {
    let pods = vec![
        make_pod(
            "normal-pod",
            DEFAULT_PRIORITY_WHEN_NO_DEFAULT_CLASS_EXISTS,
            None,
        ),
        make_pod(
            "highest-user-definable-pod",
            HIGHEST_USER_DEFINABLE_PRIORITY,
            None,
        ),
        make_pod("critical-pod", SYSTEM_CRITICAL_PRIORITY, None),
    ];
    let got = group_by_priority(&migrate_config(secs(300), secs(120)), &pods);
    assert_eq!(got.len(), 2);
    assert_eq!(
        got[0].period.priority,
        DEFAULT_PRIORITY_WHEN_NO_DEFAULT_CLASS_EXISTS
    );
    assert_eq!(got[0].period.shutdown_grace_period_seconds, 180);
    assert_eq!(
        names(&got[0]),
        vec!["normal-pod", "highest-user-definable-pod"]
    );
    assert_eq!(got[1].period.priority, SYSTEM_CRITICAL_PRIORITY);
    assert_eq!(got[1].period.shutdown_grace_period_seconds, 120);
    assert_eq!(names(&got[1]), vec!["critical-pod"]);
}

#[test]
fn test_group_by_priority_pod_priority() {
    let periods: Vec<Period> = [(1, 10), (2, 20), (3, 30), (4, 40)]
        .iter()
        .map(|&(priority, s)| Period {
            priority,
            shutdown_grace_period_seconds: s,
        })
        .collect();
    let pods: Vec<_> = (0..=5)
        .map(|i| make_pod(&format!("pod-{i}"), i, None))
        .collect();
    let got = group_by_priority(&periods, &pods);
    let want: Vec<(i32, i64, Vec<&str>)> = vec![
        (1, 10, vec!["pod-0", "pod-1"]),
        (2, 20, vec!["pod-2"]),
        (3, 30, vec!["pod-3"]),
        (4, 40, vec!["pod-4", "pod-5"]),
    ];
    assert_eq!(got.len(), want.len());
    for (g, (prio, s, n)) in got.iter().zip(want) {
        assert_eq!(g.period.priority, prio);
        assert_eq!(g.period.shutdown_grace_period_seconds, s);
        assert_eq!(names(g), n);
    }
}

// ---- storage_test.go --------------------------------------------------------

#[test]
fn test_local_storage() {
    let dir = std::env::temp_dir().join(format!("nodeshutdown-storage-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("graceful_node_shutdown_state");
    let l = LocalStorage { path: path.clone() };
    let now = Utc::now();
    let want = State {
        start_time: now,
        end_time: now,
    };
    l.store(&want).unwrap();
    let got = l.load().unwrap();
    assert_eq!(want.start_time, got.start_time);
    assert_eq!(want.end_time, got.end_time);

    let raw = std::fs::read_to_string(&path).unwrap();
    // time.RFC3339Nano: fractional seconds with trailing zeros trimmed.
    let mut now_str = now.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = format!("{:09}", now.timestamp_subsec_nanos());
    let frac = nanos.trim_end_matches('0');
    if !frac.is_empty() {
        now_str.push('.');
        now_str.push_str(frac);
    }
    now_str.push('Z');
    assert_eq!(
        raw,
        format!("{{\"startTime\":\"{now_str}\",\"endTime\":\"{now_str}\"}}")
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_local_storage_missing_file_loads_zero_state() {
    let l = LocalStorage {
        path: std::env::temp_dir().join("nodeshutdown-does-not-exist"),
    };
    assert_eq!(l.load().unwrap(), State::default());
    assert_eq!(timestamp(zero_time()), 0.0);
}

// ---- nodeshutdown_manager_linux_test.go -------------------------------------

struct Case {
    desc: &'static str,
    active_pods: Vec<Arc<Pod>>,
    requested: u64,
    critical: u64,
    system_inhibit_delay: u64,
    override_system_inhibit_delay: u64,
    expected_did_override: bool,
    expected_grace: Vec<(&'static str, i64)>,
    expected_error: Option<&'static str>,
    check_statuses: bool,
}

fn status_pod(name: &str, phase: Phase) -> Arc<Pod> {
    let mut pod = Pod::new(name, PodSpec::default());
    pod.spec.as_mut().unwrap().priority = None;
    pod.status = Some(PodStatus {
        phase: Some(phase),
        ..Default::default()
    });
    Arc::new(pod)
}

#[test]
#[serial]
fn test_manager() {
    let normal_nil = make_pod("normal-pod-nil-grace-period", 0, None);
    let critical_nil = make_pod(
        "critical-pod-nil-grace-period",
        SYSTEM_CRITICAL_PRIORITY,
        None,
    );
    let normal_short = make_pod("normal-pod-grace-period", 0, Some(2));
    let critical_short = make_pod(
        "critical-pod-grace-period",
        SYSTEM_CRITICAL_PRIORITY,
        Some(2),
    );
    let normal_long = make_pod("normal-pod-long-grace-period", 0, Some(1000));

    let cases = vec![
        Case {
            desc: "verify pod status",
            active_pods: vec![
                status_pod("running-pod", Phase::Running),
                status_pod("failed-pod", Phase::Failed),
                status_pod("succeeded-pod", Phase::Succeeded),
            ],
            requested: 30,
            critical: 10,
            system_inhibit_delay: 40,
            override_system_inhibit_delay: 40,
            expected_did_override: false,
            expected_grace: vec![("running-pod", 20), ("failed-pod", 20), ("succeeded-pod", 20)],
            expected_error: None,
            check_statuses: true,
        },
        Case {
            desc: "no override (total=30s, critical=10s)",
            active_pods: vec![normal_nil.clone(), critical_nil.clone()],
            requested: 30,
            critical: 10,
            system_inhibit_delay: 40,
            override_system_inhibit_delay: 40,
            expected_did_override: false,
            expected_grace: vec![
                ("normal-pod-nil-grace-period", 20),
                ("critical-pod-nil-grace-period", 10),
            ],
            expected_error: None,
            check_statuses: true,
        },
        Case {
            desc: "no override (total=30s, critical=10s) pods with terminationGracePeriod and without",
            active_pods: vec![
                normal_nil.clone(),
                critical_nil.clone(),
                normal_short.clone(),
                critical_short.clone(),
            ],
            requested: 30,
            critical: 10,
            system_inhibit_delay: 40,
            override_system_inhibit_delay: 40,
            expected_did_override: false,
            expected_grace: vec![
                ("normal-pod-nil-grace-period", 20),
                ("critical-pod-nil-grace-period", 10),
                ("normal-pod-grace-period", 2),
                ("critical-pod-grace-period", 2),
            ],
            expected_error: None,
            check_statuses: false,
        },
        Case {
            desc: "no override (total=30s, critical=10s) pod with long terminationGracePeriod is overridden",
            active_pods: vec![
                normal_nil.clone(),
                critical_nil.clone(),
                normal_short.clone(),
                critical_short.clone(),
                normal_long.clone(),
            ],
            requested: 30,
            critical: 10,
            system_inhibit_delay: 40,
            override_system_inhibit_delay: 40,
            expected_did_override: false,
            expected_grace: vec![
                ("normal-pod-nil-grace-period", 20),
                ("critical-pod-nil-grace-period", 10),
                ("normal-pod-grace-period", 2),
                ("critical-pod-grace-period", 2),
                ("normal-pod-long-grace-period", 20),
            ],
            expected_error: None,
            check_statuses: false,
        },
        Case {
            desc: "no override (total=30, critical=0)",
            active_pods: vec![normal_nil.clone(), critical_nil.clone()],
            requested: 30,
            critical: 0,
            system_inhibit_delay: 40,
            override_system_inhibit_delay: 40,
            expected_did_override: false,
            expected_grace: vec![
                ("normal-pod-nil-grace-period", 30),
                ("critical-pod-nil-grace-period", 0),
            ],
            expected_error: None,
            check_statuses: false,
        },
        Case {
            desc: "override successful (total=30, critical=10)",
            active_pods: vec![normal_nil.clone(), critical_nil.clone()],
            requested: 30,
            critical: 10,
            system_inhibit_delay: 5,
            override_system_inhibit_delay: 30,
            expected_did_override: true,
            expected_grace: vec![
                ("normal-pod-nil-grace-period", 20),
                ("critical-pod-nil-grace-period", 10),
            ],
            expected_error: None,
            check_statuses: false,
        },
        Case {
            desc: "override unsuccessful",
            active_pods: vec![normal_nil.clone(), critical_nil.clone()],
            requested: 30,
            critical: 10,
            system_inhibit_delay: 5,
            override_system_inhibit_delay: 5,
            expected_did_override: true,
            expected_grace: vec![
                ("normal-pod-nil-grace-period", 5),
                ("critical-pod-nil-grace-period", 0),
            ],
            expected_error: Some("node shutdown manager was timed out after 5 attempts waiting for logind InhibitDelayMaxSec to update to 30s (ShutdownGracePeriod), current value is 5s"),
            check_statuses: false,
        },
        Case {
            desc: "override unsuccessful, zero time",
            active_pods: vec![normal_nil.clone(), critical_nil.clone()],
            requested: 5,
            critical: 5,
            system_inhibit_delay: 0,
            override_system_inhibit_delay: 0,
            expected_did_override: false,
            expected_grace: vec![],
            expected_error: Some("node shutdown manager was timed out after 5 attempts waiting for logind InhibitDelayMaxSec to update to 5s (ShutdownGracePeriod), current value is 0s"),
            check_statuses: false,
        },
        Case {
            desc: "no override, all time to critical pods",
            active_pods: vec![normal_nil.clone(), critical_nil.clone()],
            requested: 5,
            critical: 5,
            system_inhibit_delay: 5,
            override_system_inhibit_delay: 5,
            expected_did_override: false,
            expected_grace: vec![
                ("normal-pod-nil-grace-period", 0),
                ("critical-pod-nil-grace-period", 5),
            ],
            expected_error: None,
            check_statuses: false,
        },
    ];

    let _gate = with_feature(Feature::GracefulNodeShutdown, true);
    let prev = set_system_dbus(Arc::new(|| Err("unset".into())));
    for tc in cases {
        let active = tc.active_pods.clone();
        let get_pods: ActivePodsFunc = Arc::new(move || active.clone());

        let (kill_tx, kill_rx) = mpsc::sync_channel::<(String, i64)>(1);
        let statuses: Arc<Mutex<HashMap<String, PodStatus>>> = Arc::new(Mutex::new(HashMap::new()));
        let st = statuses.clone();
        let kill: KillPodFunc = Arc::new(move |pod: &Pod, _evict, grace, fn_: PodStatusFn<'_>| {
            let mut status = pod.status.clone().unwrap_or_default();
            fn_(&mut status);
            st.lock().unwrap().insert(pod.metadata.name.clone(), status);
            kill_tx
                .send((pod.metadata.name.clone(), grace.unwrap_or(0)))
                .unwrap();
            Ok(())
        });

        let (shutdown_tx, shutdown_rx) = mpsc::sync_channel::<bool>(0);
        let fake = FakeDbus::new(
            secs(tc.system_inhibit_delay),
            secs(tc.override_system_inhibit_delay),
        );
        *fake.shutdown_chan.lock().unwrap() = Some(shutdown_rx);
        let f2 = fake.clone();
        set_system_dbus(Arc::new(move || Ok(f2.clone() as Arc<dyn DbusInhibiter>)));

        let clock: Arc<dyn Clock> = FakeClock::new();
        let manager = new_manager(&base_config(
            get_pods,
            kill,
            secs(tc.requested),
            secs(tc.critical),
            Some(clock),
        ));

        let err = manager.start();
        match tc.expected_error {
            Some(want) => {
                let got = err.expect_err(tc.desc);
                assert!(got.contains(want), "{}: got {got} want {want}", tc.desc);
            }
            None => {
                err.unwrap_or_else(|e| panic!("{}: unexpected error {e}", tc.desc));
                assert!(
                    fake.state.lock().unwrap().did_inhibit_shutdown,
                    "{}",
                    tc.desc
                );
                assert!(manager.shutdown_status().is_ok(), "{}", tc.desc);
                assert!(manager.admit(None).admit, "{}", tc.desc);

                // Send fake shutdown event
                shutdown_tx
                    .send_timeout_compat(true)
                    .unwrap_or_else(|_| panic!("{}: shutdown event not received", tc.desc));

                // Wait for all the pods to be killed
                let mut killed = HashMap::new();
                for _ in 0..tc.active_pods.len() {
                    let (n, g) = kill_rx
                        .recv_timeout(secs(1))
                        .unwrap_or_else(|_| panic!("{}: pod not killed", tc.desc));
                    killed.insert(n, g);
                }

                assert!(manager.shutdown_status().is_err(), "{}", tc.desc);
                assert!(!manager.admit(None).admit, "{}", tc.desc);
                let want: HashMap<String, i64> = tc
                    .expected_grace
                    .iter()
                    .map(|(n, g)| (n.to_string(), *g))
                    .collect();
                assert_eq!(want, killed, "{}", tc.desc);
                assert_eq!(
                    tc.expected_did_override,
                    fake.state.lock().unwrap().did_override_inhibit_delay,
                    "{}: override system inhibit delay differs",
                    tc.desc
                );
                if tc.check_statuses {
                    let statuses = statuses.lock().unwrap();
                    for pod in &tc.active_pods {
                        let got = &statuses[&pod.metadata.name];
                        let phase = if pod.metadata.name == "succeeded-pod" {
                            Phase::Succeeded
                        } else {
                            Phase::Failed
                        };
                        assert_eq!(got.phase, Some(phase), "{}", pod.metadata.name);
                        assert_eq!(
                            got.message.as_deref(),
                            Some("Pod was terminated in response to imminent node shutdown.")
                        );
                        assert_eq!(got.reason.as_deref(), Some("Terminated"));
                        let conds = got.conditions.as_ref().unwrap();
                        assert_eq!(conds.len(), 1);
                        assert_eq!(conds[0].condition_type, "DisruptionTarget");
                        assert_eq!(conds[0].status, "True");
                        assert_eq!(conds[0].reason.as_deref(), Some("TerminationByKubelet"));
                        assert_eq!(
                            conds[0].message.as_deref(),
                            Some("Pod was terminated in response to imminent node shutdown.")
                        );
                    }
                }
            }
        }
    }
    set_system_dbus(prev);
}

/// `select { case ch <- v: case <-time.After(1s): t.Fatal() }`.
trait SendTimeout {
    fn send_timeout_compat(&self, v: bool) -> Result<(), ()>;
}
impl SendTimeout for mpsc::SyncSender<bool> {
    fn send_timeout_compat(&self, v: bool) -> Result<(), ()> {
        let deadline = std::time::Instant::now() + secs(1);
        loop {
            match self.try_send(v) {
                Ok(()) => return Ok(()),
                Err(mpsc::TrySendError::Full(_)) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(());
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(_) => return Err(()),
            }
        }
    }
}

#[test]
#[serial]
fn test_feature_enabled() {
    let cases = [
        (
            "shutdownGracePeriodRequested 0; disables feature",
            0,
            true,
            false,
        ),
        ("feature gate disabled; disables feature", 100, false, false),
        (
            "feature gate enabled; shutdownGracePeriodRequested > 0; enables feature",
            100,
            true,
            true,
        ),
    ];
    for (desc, requested, gate, expect_enabled) in cases {
        let _g = with_feature(Feature::GracefulNodeShutdown, gate);
        let manager = new_manager(&base_config(
            Arc::new(Vec::new),
            Arc::new(|_, _, _, _| Ok(())),
            secs(requested),
            Duration::ZERO,
            None,
        ));
        assert_eq!(expect_enabled, !manager.is_stub(), "{desc}");
    }
}

#[test]
#[serial]
fn test_restart() {
    let _g = with_feature(Feature::GracefulNodeShutdown, true);
    let prev = set_system_dbus(Arc::new(|| Err("unset".into())));

    let senders: Arc<Mutex<Vec<mpsc::SyncSender<bool>>>> = Arc::new(Mutex::new(Vec::new()));
    let (conn_tx, conn_rx) = mpsc::sync_channel::<()>(1);
    let s2 = senders.clone();
    set_system_dbus(Arc::new(move || {
        let (tx, rx) = mpsc::sync_channel::<bool>(0);
        s2.lock().unwrap().push(tx);
        let d = FakeDbus::new(secs(40), secs(40));
        *d.shutdown_chan.lock().unwrap() = Some(rx);
        let _ = conn_tx.send(());
        Ok(d as Arc<dyn DbusInhibiter>)
    }));

    let manager = new_manager(&base_config(
        Arc::new(Vec::new),
        Arc::new(|_, _, _, _| Ok(())),
        secs(30),
        secs(10),
        None,
    ));
    manager.start().unwrap();

    for _ in 0..3 {
        conn_rx
            .recv_timeout(DBUS_RECONNECT_PERIOD * 5)
            .expect("wait dbus connect timeout");
        // close(shutdownChan): drop the most recent sender.
        senders.lock().unwrap().pop();
    }
    set_system_dbus(prev);
}

fn impl_for_test(
    logger: Arc<BufLogger>,
    vm: Arc<dyn VolumeManager>,
    periods: Vec<Period>,
    get_pods: ActivePodsFunc,
    kill: KillPodFunc,
    clock: Arc<dyn Clock>,
) -> ManagerImpl {
    ManagerImpl {
        logger: logger.clone(),
        recorder: Arc::new(FakeRecorder),
        node_ref: node_ref(),
        get_pods,
        sync_node_status: Arc::new(|| {}),
        dbus_con: Mutex::new(Some(FakeDbus::new(Duration::ZERO, Duration::ZERO))),
        inhibit_lock: Mutex::new(InhibitLock(0)),
        node_shutting_down_now: Mutex::new(false),
        pod_manager: PodManager {
            logger,
            shutdown_grace_period_by_pod_priority: periods,
            clock,
            kill_pod_func: kill,
            volume_manager: vm,
        },
        enable_metrics: false,
        storage: None,
    }
}

#[test]
fn test_manager_impl_process_shutdown_event_kill_pod_func_take_too_long() {
    let fakeclock = FakeClock::new();
    let logger = Arc::new(BufLogger::default());
    let fc = fakeclock.clone();
    let m = impl_for_test(
        logger.clone(),
        Arc::new(FakeVolumeManager {
            unmount_delay: Duration::ZERO,
            unmount_error: None,
        }),
        vec![
            Period {
                priority: 1,
                shutdown_grace_period_seconds: 10,
            },
            Period {
                priority: 2,
                shutdown_grace_period_seconds: 20,
            },
        ],
        Arc::new(|| {
            vec![
                make_pod("normal-pod", 1, None),
                make_pod("critical-pod", 2, None),
            ]
        }),
        Arc::new(move |_, _, _, _| {
            fc.step(secs(60));
            Ok(())
        }),
        fakeclock,
    );
    assert!(m.process_shutdown_event().is_ok());
    let log = logger.buf.lock().unwrap().clone();
    assert!(
        log.contains("Shutdown manager pod killing time out"),
        "log was: {log}"
    );
}

#[test]
fn test_process_shutdown_event_volume_unmount_timeout() {
    let fakeclock = FakeClock::new();
    let logger = Arc::new(BufLogger::default());
    let shutdown_grace_period_seconds = 2;
    let m = impl_for_test(
        logger.clone(),
        Arc::new(FakeVolumeManager {
            // Intentionally longer than the shutdown grace period to test the
            // behavior for unmount operations that exceed it.
            unmount_delay: secs(3),
            unmount_error: Some("unmount timeout".into()),
        }),
        vec![Period {
            priority: 1,
            shutdown_grace_period_seconds,
        }],
        Arc::new(|| vec![make_pod("test-pod", 1, None)]),
        Arc::new(|_, _, _, _| Ok(())),
        fakeclock.clone(),
    );

    let start = fakeclock.now();
    m.process_shutdown_event()
        .expect("should not return an error");
    let end = fakeclock.now();
    assert!((end - start).num_seconds() <= shutdown_grace_period_seconds);

    let log = logger.buf.lock().unwrap().clone();
    assert!(
        log.contains(
            "Failed while waiting for all the volumes belonging to Pods in this group to unmount"
        ),
        "log was: {log}"
    );
}
