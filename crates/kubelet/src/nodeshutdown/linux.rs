//! Port of `pkg/kubelet/nodeshutdown/nodeshutdown_manager_linux.go`.
//!
//! Watches for node level shutdown events (logind `PrepareForShutdown`) and
//! triggers graceful termination of pods running on the node prior to a
//! system shutdown, following the logind Inhibit Delay pattern
//! (https://www.freedesktop.org/wiki/Software/systemd/inhibit/).

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use rusternetes_common::resources::ObjectReference;

use super::storage::{timestamp, LocalStorage, State, Storage};
use super::systemd::InhibitLock;
use super::{
    new_pod_manager, ActivePodsFunc, Config, EventRecorder, Logger, Manager, ManagerStub,
    PodAdmitAttributes, PodAdmitResult, PodManager, EVENT_NODE_SHUTDOWN, LOCAL_STORAGE_STATE_FILE,
    NODE_SHUTDOWN_NOT_ADMITTED_MESSAGE, NODE_SHUTDOWN_NOT_ADMITTED_REASON,
};
use crate::server_metrics::{GRACEFUL_SHUTDOWN_END_TIME, GRACEFUL_SHUTDOWN_START_TIME};

/// `dbusReconnectPeriod` (:44).
pub const DBUS_RECONNECT_PERIOD: Duration = Duration::from_secs(1);

/// `dbusInhibiter` (:50-57).
pub trait DbusInhibiter: Send + Sync {
    fn current_inhibit_delay(&self) -> Result<Duration, String>;
    fn inhibit_shutdown(&self) -> Result<InhibitLock, String>;
    fn release_inhibit_lock(&self, lock: InhibitLock) -> Result<(), String>;
    fn reload_logind_conf(&self) -> Result<(), String>;
    /// `MonitorShutdown`: a channel of `PrepareForShutdown` booleans; closed
    /// (sender dropped) when the bus connection ends.
    fn monitor_shutdown(&self, logger: &dyn Logger) -> Result<Receiver<bool>, String>;
    fn override_inhibit_delay(&self, inhibit_delay_max: Duration) -> Result<(), String>;
}

/// `systemDbus` (:46-48).
pub type SystemDbusFn = Arc<dyn Fn() -> Result<Arc<dyn DbusInhibiter>, String> + Send + Sync>;

fn system_dbus_cell() -> &'static Mutex<SystemDbusFn> {
    static CELL: OnceLock<Mutex<SystemDbusFn>> = OnceLock::new();
    CELL.get_or_init(|| {
        // Upstream: `systemd.NewDBusCon()`. Not ported, see `super::systemd`.
        Mutex::new(Arc::new(|| {
            Err("system D-Bus connection is not available: no D-Bus client is linked into the kubelet"
                .to_string())
        }))
    })
}

/// Replace the `systemDbus` package variable (upstream tests assign to it);
/// returns the previous value so it can be restored.
pub fn set_system_dbus(f: SystemDbusFn) -> SystemDbusFn {
    std::mem::replace(&mut *system_dbus_cell().lock().unwrap(), f)
}

fn system_dbus() -> Result<Arc<dyn DbusInhibiter>, String> {
    let f = system_dbus_cell().lock().unwrap().clone();
    f()
}

/// `managerImpl` (:59-76). Fields are public because upstream's tests
/// (same package) construct it literally.
pub struct ManagerImpl {
    pub logger: Arc<dyn Logger>,
    pub recorder: Arc<dyn EventRecorder>,
    pub node_ref: ObjectReference,

    pub get_pods: ActivePodsFunc,
    pub sync_node_status: Arc<dyn Fn() + Send + Sync>,

    pub dbus_con: Mutex<Option<Arc<dyn DbusInhibiter>>>,
    pub inhibit_lock: Mutex<InhibitLock>,

    /// `nodeShuttingDownMutex` + `nodeShuttingDownNow`.
    pub node_shutting_down_now: Mutex<bool>,
    pub pod_manager: PodManager,

    pub enable_metrics: bool,
    pub storage: Option<Box<dyn Storage>>,
}

/// `NewManager` (:79-111).
pub fn new_manager(conf: &Config) -> Arc<dyn Manager> {
    if !rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::GracefulNodeShutdown,
    ) {
        return Arc::new(ManagerStub);
    }

    let pod_manager = new_pod_manager(conf);

    // Disable if the configuration is empty.
    if pod_manager.shutdown_grace_period_by_pod_priority.is_empty() {
        return Arc::new(ManagerStub);
    }

    let manager = Arc::new(ManagerImpl {
        logger: conf.logger.clone(),
        recorder: conf.recorder.clone(),
        node_ref: conf.node_ref.clone(),
        get_pods: conf.get_pods_func.clone(),
        sync_node_status: conf.sync_node_status_func.clone(),
        dbus_con: Mutex::new(None),
        inhibit_lock: Mutex::new(InhibitLock(0)),
        node_shutting_down_now: Mutex::new(false),
        enable_metrics: rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::GracefulNodeShutdownBasedOnPodPriority,
        ),
        storage: Some(Box::new(LocalStorage {
            path: PathBuf::from(&conf.state_directory).join(LOCAL_STORAGE_STATE_FILE),
        })),
        pod_manager,
    });
    manager.logger.info(
        0,
        "Creating node shutdown manager",
        &[
            (
                "shutdownGracePeriodRequested",
                format!("{:?}", conf.shutdown_grace_period_requested),
            ),
            (
                "shutdownGracePeriodCriticalPods",
                format!("{:?}", conf.shutdown_grace_period_critical_pods),
            ),
            (
                "shutdownGracePeriodByPodPriority",
                format!(
                    "{:?}",
                    manager.pod_manager.shutdown_grace_period_by_pod_priority
                ),
            ),
        ],
    );
    Arc::new(ManagerHandle(manager))
}

/// `Manager` over a shared [`ManagerImpl`] (the watch loop needs an owning
/// handle, which Go gets from the pointer receiver).
pub struct ManagerHandle(pub Arc<ManagerImpl>);

impl Manager for ManagerHandle {
    fn admit(&self, attrs: Option<&PodAdmitAttributes>) -> PodAdmitResult {
        self.0.admit(attrs)
    }
    fn start(&self) -> Result<(), String> {
        self.0.start_manager()
    }
    fn shutdown_status(&self) -> Result<(), String> {
        self.0.shutdown_status()
    }
    fn is_stub(&self) -> bool {
        false
    }
}

impl ManagerImpl {
    /// `Admit` (:114-125): rejects all pods if the node is shutting down.
    pub fn admit(&self, _attrs: Option<&PodAdmitAttributes>) -> PodAdmitResult {
        let node_shutting_down = self.shutdown_status().is_err();

        if node_shutting_down {
            return PodAdmitResult {
                admit: false,
                reason: NODE_SHUTDOWN_NOT_ADMITTED_REASON.to_string(),
                message: NODE_SHUTDOWN_NOT_ADMITTED_MESSAGE.to_string(),
            };
        }
        PodAdmitResult::admitted()
    }

    /// `setMetrics` (:128-143).
    pub fn set_metrics(&self) {
        if self.enable_metrics {
            if let Some(storage) = &self.storage {
                match storage.load() {
                    Err(err) => self.logger.error(
                        Some(&err.to_string()),
                        "Failed to load graceful shutdown state",
                        &[],
                    ),
                    Ok(sta) => {
                        if !super::storage::is_zero(sta.start_time) {
                            GRACEFUL_SHUTDOWN_START_TIME.set(timestamp(sta.start_time));
                        }
                        if !super::storage::is_zero(sta.end_time) {
                            GRACEFUL_SHUTDOWN_END_TIME.set(timestamp(sta.end_time));
                        }
                    }
                }
            }
        }
    }

    /// `Start` (:146-168): start watching the node for shutdown events and
    /// keep re-establishing the watch when it ends.
    pub fn start_manager(self: &Arc<Self>) -> Result<(), String> {
        let stop = self.start()?;
        let this = self.clone();
        std::thread::spawn(move || {
            let mut stop = Some(stop);
            loop {
                if let Some(s) = stop.take() {
                    // `<-stop`: returns when the sender is dropped (closed).
                    let _ = s.recv();
                }

                std::thread::sleep(DBUS_RECONNECT_PERIOD);
                this.logger
                    .info(1, "Restarting watch for node shutdown events", &[]);
                match this.start() {
                    Ok(s) => stop = Some(s),
                    Err(err) => this.logger.error(
                        Some(&err),
                        "Unable to watch the node for shutdown events",
                        &[],
                    ),
                }
            }
        });

        self.set_metrics();
        Ok(())
    }

    fn dbus(&self) -> Arc<dyn DbusInhibiter> {
        self.dbus_con
            .lock()
            .unwrap()
            .clone()
            .expect("dbus connection set by start()")
    }

    /// `start` (:170-296). The returned receiver is Go's `stop` channel: it
    /// yields `Err` once the watch loop exits (the sender is dropped).
    fn start(self: &Arc<Self>) -> Result<Receiver<()>, String> {
        let system_bus = system_dbus()?;
        *self.dbus_con.lock().unwrap() = Some(system_bus.clone());
        let dbus = system_bus;

        let current_inhibit_delay = dbus.current_inhibit_delay()?;

        // If the logind's InhibitDelayMaxUSec as configured in (logind.conf) is
        // less than periodRequested, attempt to update the value to
        // periodRequested.
        let period_requested = self.pod_manager.period_requested();
        if period_requested > current_inhibit_delay {
            dbus.override_inhibit_delay(period_requested).map_err(|e| {
                format!("unable to override inhibit delay by shutdown manager: {e}")
            })?;

            dbus.reload_logind_conf()?;

            // The ReloadLogindConf call is asynchronous. Poll with
            // exponential backoff until the configuration is updated
            // (`wait.Backoff{Duration: 100ms, Factor: 2.0, Steps: 5}`).
            let mut duration = Duration::from_millis(100);
            let mut steps = 5;
            let total_steps = steps;
            let mut updated_inhibit_delay = Duration::ZERO;
            let mut attempt = 0;
            let mut timed_out = true;
            while steps > 0 {
                attempt += 1;
                // Read the current inhibitDelay again, if the override was
                // successful, currentInhibitDelay will be equal to
                // shutdownGracePeriodRequested.
                updated_inhibit_delay = dbus.current_inhibit_delay()?;
                if period_requested <= updated_inhibit_delay {
                    timed_out = false;
                    break;
                }
                if attempt < total_steps {
                    self.logger.info(
                        3,
                        "InhibitDelayMaxSec still less than requested, retrying",
                        &[
                            ("attempt", attempt.to_string()),
                            ("current", format!("{updated_inhibit_delay:?}")),
                            ("requested", format!("{period_requested:?}")),
                        ],
                    );
                }
                if steps == 1 {
                    break;
                }
                std::thread::sleep(duration);
                duration *= 2;
                steps -= 1;
            }
            if timed_out && period_requested > updated_inhibit_delay {
                return Err(format!(
                    "node shutdown manager was timed out after {attempt} attempts waiting for logind InhibitDelayMaxSec to update to {} (ShutdownGracePeriod), current value is {}",
                    go_duration(period_requested),
                    go_duration(updated_inhibit_delay)
                ));
            }
        }

        self.acquire_inhibit_lock()?;

        let events = match dbus.monitor_shutdown(self.logger.as_ref()) {
            Ok(e) => e,
            Err(err) => {
                let lock = *self.inhibit_lock.lock().unwrap();
                if let Err(release_err) = dbus.release_inhibit_lock(lock) {
                    return Err(format!(
                        "failed releasing inhibitLock: {release_err} and failed monitoring shutdown: {err}"
                    ));
                }
                return Err(format!("failed to monitor shutdown: {err}"));
            }
        };

        let (stop_tx, stop_rx) = mpsc::channel::<()>();
        let this = self.clone();
        std::thread::spawn(move || {
            // Monitor for shutdown events. This follows the logind Inhibit
            // Delay pattern described on
            // https://www.freedesktop.org/wiki/Software/systemd/inhibit/
            // 1. When shutdown manager starts, an inhibit lock is taken.
            // 2. When shutdown(true) event is received, process the shutdown
            //    and release the inhibit lock.
            // 3. When shutdown(false) event is received, this indicates a
            //    previous shutdown was cancelled. In this case, acquire the
            //    inhibit lock again.
            loop {
                let Ok(is_shutting_down) = events.recv() else {
                    this.logger
                        .error(None, "Ended to watching the node for shutdown events", &[]);
                    drop(stop_tx);
                    return;
                };
                this.logger.info(
                    1,
                    "Shutdown manager detected new shutdown event, isNodeShuttingDownNow",
                    &[("event", is_shutting_down.to_string())],
                );

                let shutdown_type = if is_shutting_down {
                    "shutdown"
                } else {
                    "cancelled"
                };
                this.logger.info(
                    1,
                    "Shutdown manager detected new shutdown event",
                    &[("event", shutdown_type.to_string())],
                );
                if is_shutting_down {
                    this.recorder.event(
                        &this.node_ref,
                        "Normal",
                        EVENT_NODE_SHUTDOWN,
                        "Shutdown manager detected shutdown event",
                    );
                } else {
                    this.recorder.event(
                        &this.node_ref,
                        "Normal",
                        EVENT_NODE_SHUTDOWN,
                        "Shutdown manager detected shutdown cancellation",
                    );
                }

                *this.node_shutting_down_now.lock().unwrap() = is_shutting_down;

                if is_shutting_down {
                    // Update node status and ready condition.
                    let sync = this.sync_node_status.clone();
                    std::thread::spawn(move || sync());

                    let _ = this.process_shutdown_event();
                } else {
                    let _ = this.acquire_inhibit_lock();
                }
            }
        });
        Ok(stop_rx)
    }

    /// `acquireInhibitLock` (:298-308).
    fn acquire_inhibit_lock(&self) -> Result<(), String> {
        let dbus = self.dbus();
        let lock = dbus.inhibit_shutdown()?;
        let mut cur = self.inhibit_lock.lock().unwrap();
        if cur.0 != 0 {
            let _ = dbus.release_inhibit_lock(*cur);
        }
        *cur = lock;
        Ok(())
    }

    /// `ShutdownStatus` (:310-319): an error if the node is currently shutting
    /// down.
    pub fn shutdown_status(&self) -> Result<(), String> {
        if *self.node_shutting_down_now.lock().unwrap() {
            return Err("node is shutting down".to_string());
        }
        Ok(())
    }

    /// `processShutdownEvent` (:321-347).
    pub fn process_shutdown_event(&self) -> Result<(), String> {
        self.logger
            .info(1, "Shutdown manager processing shutdown event", &[]);
        let active_pods = (self.get_pods)();

        let mut end_state: Option<State> = None;
        if self.enable_metrics {
            if let Some(storage) = &self.storage {
                let start_time = chrono::Utc::now();
                let st = State {
                    start_time,
                    end_time: super::storage::zero_time(),
                };
                if let Err(err) = storage.store(&st) {
                    self.logger.error(
                        Some(&err.to_string()),
                        "Failed to store graceful shutdown state",
                        &[],
                    );
                }
                GRACEFUL_SHUTDOWN_START_TIME.set(timestamp(start_time));
                GRACEFUL_SHUTDOWN_END_TIME.set(0.0);
                end_state = Some(st);
            }
        }

        let result = self.pod_manager.kill_pods(&active_pods);

        // Deferred (LIFO): the end-time store, then the lock release.
        if let (Some(start), Some(storage)) = (end_state, &self.storage) {
            let end_time = chrono::Utc::now();
            if let Err(err) = storage.store(&State {
                start_time: start.start_time,
                end_time,
            }) {
                self.logger.error(
                    Some(&err.to_string()),
                    "Failed to store graceful shutdown state",
                    &[],
                );
            }
            GRACEFUL_SHUTDOWN_END_TIME.set(timestamp(end_time));
        }
        let lock = *self.inhibit_lock.lock().unwrap();
        let _ = self.dbus().release_inhibit_lock(lock);
        self.logger.info(
            1,
            "Shutdown manager completed processing shutdown event, node will shutdown shortly",
            &[],
        );

        result
    }
}

/// `time.Duration.String()` for whole-second-or-finer durations.
fn go_duration(d: Duration) -> String {
    rusternetes_common::go_duration::format_go_duration(d.as_nanos() as i64)
}
