use crate::cri_runtime::CriContainerRuntime;
use crate::eviction::{get_node_stats, get_pod_stats, EvictionManager, EvictionSignal};
use crate::lifecycle::{phase_is_terminal, should_skip_phase_write};
use anyhow::Result;
use rusternetes_common::{
    resources::{
        ContainerState, ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting,
        ContainerStatus, Node, NodeAddress, NodeSpec, NodeStatus, Pod, PodCondition, PodIP,
        PodStatus,
    },
    types::Phase,
};
use rusternetes_storage::{build_key, build_prefix, Storage, StorageBackend, WatchEvent};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Pod worker state machine matching K8s pkg/kubelet/pod_workers.go.
///
/// K8s transitions:
/// - SyncPod: normal operation — create/update containers, retry on failure
/// - TerminatingPod: pod is being stopped (deletionTimestamp set OR evicted)
///   → stop containers, run preStop hooks, set Phase=Failed
/// - TerminatedPod: all containers stopped → delete pod from storage
///
/// IMPORTANT: Container creation errors do NOT trigger TerminatingPod.
/// The kubelet retries in SyncPod state. Only deletion and eviction trigger it.
/// K8s ref: pkg/kubelet/pod_workers.go:110-117, 260
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
pub enum PodWorkerState {
    /// Pod is expected to be started and running. Failures are retried.
    SyncPod,
    /// Pod is being torn down — deletion or eviction requested.
    TerminatingPod,
    /// All containers stopped, pod can be removed from storage.
    TerminatedPod,
}

/// Initial CrashLoopBackOff delay before the *second* restart of a container.
/// The first restart after a crash is immediate. Matches upstream
/// `pkg/kubelet/kubelet.go` `backOffPeriod = 10s`.
const CRASHLOOP_BACKOFF_INITIAL: Duration = Duration::from_secs(10);
/// Default maximum CrashLoopBackOff delay: upstream `MaxCrashLoopBackOff` (5m),
/// overridable by `crashLoopBackOff.maxContainerRestartPeriod`.
const CRASHLOOP_BACKOFF_MAX: Duration = crate::config::MAX_CONTAINER_BACKOFF;
/// `reducedMaxCrashLoopBackOff` (pkg/kubelet/kubelet.go:167): max backoff when
/// the `ReduceDefaultCrashLoopBackOffDecay` gate is on.
const REDUCED_CRASHLOOP_BACKOFF_MAX: Duration = Duration::from_secs(60);
/// `reducedInitialCrashLoopBackOff` (pkg/kubelet/kubelet.go:174): initial
/// backoff when the `ReduceDefaultCrashLoopBackOffDecay` gate is on.
const REDUCED_CRASHLOOP_BACKOFF_INITIAL: Duration = Duration::from_secs(1);

/// Port of `newCrashLoopBackOff` (pkg/kubelet/kubelet.go:345-361), returning
/// `(boMax, boInitial)`. `configured` is
/// `kubeCfg.CrashLoopBackOff.MaxContainerRestartPeriod` after defaulting
/// (see `KubeletConfiguration::effective_max_container_restart_period_gated`);
/// it is only consulted when `KubeletCrashLoopBackOffMax` is enabled, exactly
/// as upstream.
pub(crate) fn new_crash_loop_backoff(configured: Option<Duration>) -> (Duration, Duration) {
    use rusternetes_common::feature_gates::{enabled, Feature};
    let mut bo_max = CRASHLOOP_BACKOFF_MAX;
    let mut bo_initial = CRASHLOOP_BACKOFF_INITIAL;
    if enabled(Feature::ReduceDefaultCrashLoopBackOffDecay) {
        bo_max = REDUCED_CRASHLOOP_BACKOFF_MAX;
        bo_initial = REDUCED_CRASHLOOP_BACKOFF_INITIAL;
    }
    if enabled(Feature::KubeletCrashLoopBackOffMax) {
        // operator-invoked configuration always has precedence if valid
        if let Some(d) = configured {
            bo_max = d;
        }
        if bo_max < bo_initial {
            bo_initial = bo_max;
        }
    }
    (bo_max, bo_initial)
}

/// Defensive backoff for a terminal pod whose `finalize_terminated_pod_storage`
/// reported removal but left the object in storage (#1157). The per-pod worker
/// would otherwise re-enter `TerminatingPod` every reconcile and re-stop
/// containers forever. We back off the terminate retry (initial, doubling,
/// capped) and escalate the log so a storage-delete regression is loud.
const TERMINAL_FINALIZE_BACKOFF_INITIAL: Duration = Duration::from_secs(5);
/// Cap for the terminal-finalize retry backoff.
const TERMINAL_FINALIZE_BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Attempt count at which the still-present warning escalates to `error!`.
const TERMINAL_FINALIZE_ERROR_THRESHOLD: u32 = 3;

/// Per-attempt ceiling on `runtime.start_pod` (sandbox creation → image pull →
/// container start). On expiry the future is dropped, which cancels the
/// in-flight CRI call — and a cancelled `RunPodSandbox` leaves an orphaned
/// NOT_READY sandbox that keeps its name reserved in containerd, so every
/// subsequent retry fails permanently with "failed to reserve sandbox name …
/// is reserved for <id>" and the pod never starts (#1050). The old 30s value
/// was shorter than a *cold* `pause`/app image pull from a slow registry, so an
/// early pod could churn the full 300s e2e deadline (later pods succeeded once
/// the image cached), surfacing as flaky "Networking Granular Checks" failures.
/// Upstream imposes no such blanket cap — RunPodSandbox is bounded by the 2-min
/// `runtimeRequestTimeout` and image pulls run unbounded-with-progress. 4 min
/// comfortably covers a cold pull while still bounding a genuinely wedged call.
const POD_START_TIMEOUT: Duration = Duration::from_secs(240);
const ACTIVE_DEADLINE_REASON: &str = "DeadlineExceeded";
const ACTIVE_DEADLINE_MESSAGE: &str =
    "Pod was active on the node longer than the specified deadline";

/// Backoff delay before re-attempting termination of a terminal pod whose
/// storage delete isn't taking effect, given the failed-attempt `count` (1 =
/// first failure). Pure so it can be unit-tested without wall-clock state.
fn terminal_finalize_backoff(count: u32) -> Duration {
    let shifts = count.saturating_sub(1).min(6);
    TERMINAL_FINALIZE_BACKOFF_INITIAL
        .saturating_mul(2u32.pow(shifts))
        .min(TERMINAL_FINALIZE_BACKOFF_MAX)
}

/// Per-container CrashLoopBackOff state. This is the kubelet-owned source of
/// truth for `restartCount` and restart pacing, decoupled from sync frequency.
///
/// Without this, the old code incremented `restartCount` once per *sync that
/// observed a terminated container* and restarted with no wall-clock gate — so
/// the watch-driven sync hot loop (~30 Hz) drove `restartCount` to tens of
/// thousands instead of the handful the conformance suite expects. K8s ref:
/// The pod's `status.startTime`: set once (when the pod first becomes Running)
/// and preserved across every later status rebuild. Resetting it each sync
/// would zero out the activeDeadlineSeconds elapsed clock so the deadline is
/// never reached. Upstream stamps startTime once and never moves it.
fn preserved_start_time(
    prior: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    prior.unwrap_or(now)
}

fn active_deadline_elapsed(
    status: Option<&PodStatus>,
    deadline_seconds: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<i64> {
    let start_time = status.and_then(|status| status.start_time)?;
    let elapsed = now.signed_duration_since(start_time).num_seconds();
    if elapsed >= deadline_seconds {
        Some(elapsed)
    } else {
        None
    }
}

fn init_container_completed_successfully(
    init_statuses: Option<&[ContainerStatus]>,
    container_name: &str,
) -> bool {
    init_statuses
        .and_then(|statuses| statuses.iter().find(|s| s.name == container_name))
        .map(|status| {
            matches!(
                &status.state,
                Some(ContainerState {
                    terminated: Some(ContainerStateTerminated { exit_code: 0, .. }),
                    ..
                })
            )
        })
        .unwrap_or(false)
}

/// The `waiting.reason` an app container should report when it has not started
/// because `start_pod` errored before reaching the app containers.
///
/// Upstream `convertToAPIContainerStatuses` (kubelet_pods.go:2431-2433) uses
/// `PodInitializing` as the default waiting reason for EVERY container in a pod
/// that has init containers, until initialization is complete. So an app
/// container still blocked behind a running or failing init container must
/// report `PodInitializing`, not `ContainerCreating` — this is what the
/// NodeConformance spec "should not start app containers if init containers
/// fail on a RestartAlways pod" asserts for the app container `run1`.
///
/// Once every gating init container is satisfied (a plain init container has
/// terminated with exit 0; a restartable/sidecar init container has started),
/// a genuine app-container start error surfaces as `ContainerCreating`.
fn app_container_waiting_reason(
    pod: &Pod,
    init_statuses: Option<&[ContainerStatus]>,
) -> &'static str {
    let Some(spec) = pod.spec.as_ref() else {
        return "ContainerCreating";
    };
    let init = match spec.init_containers.as_ref() {
        Some(ic) if !ic.is_empty() => ic,
        _ => return "ContainerCreating",
    };
    // Restartable (sidecar) init containers never terminate; they gate app
    // start on being *started*, which by the time app containers are attempted
    // is already true — so they don't hold the reason. Plain init containers
    // gate until they terminate successfully (exit 0). A still-running or
    // failed plain init container therefore keeps app containers PodInitializing.
    let all_init_done = init.iter().all(|c| {
        c.restart_policy.as_deref() == Some("Always")
            || init_container_completed_successfully(init_statuses, &c.name)
    });
    if all_init_done {
        "ContainerCreating"
    } else {
        "PodInitializing"
    }
}

fn init_container_failed_terminally(pod: &Pod, init_statuses: Option<&[ContainerStatus]>) -> bool {
    let restart_policy = pod
        .spec
        .as_ref()
        .and_then(|s| s.restart_policy.as_deref())
        .unwrap_or("Always");
    if restart_policy != "Never" {
        return false;
    }

    pod.spec
        .as_ref()
        .and_then(|s| s.init_containers.as_ref())
        .map(|init_containers| {
            init_containers.iter().any(|container| {
                container.restart_policy.as_deref() != Some("Always")
                    && init_statuses
                        .and_then(|statuses| statuses.iter().find(|s| s.name == container.name))
                        .is_some_and(|status| {
                            matches!(
                                &status.state,
                                Some(ContainerState { terminated: Some(ContainerStateTerminated { exit_code, .. }), .. }) if *exit_code != 0
                            )
                        })
            })
        })
        .unwrap_or(false)
}

/// Whether a pod still in the API no longer wants its volumes mounted: its
/// phase is terminal and no container is still running. Upstream's populator
/// drops a pod's volumes from the desired state once
/// `ShouldPodRuntimeBeRemoved` holds (`pod_workers.go:698`,
/// `desired_state_of_world_populator.go:221-233`), i.e. the pod worker has seen
/// every container exit.
fn pod_volumes_released(pod: &Pod) -> bool {
    let Some(status) = pod.status.as_ref() else {
        return false;
    };
    matches!(status.phase, Some(Phase::Succeeded | Phase::Failed))
        && !status
            .container_statuses
            .iter()
            .flatten()
            .chain(status.init_container_statuses.iter().flatten())
            .any(|c| {
                matches!(
                    c.state,
                    Some(ContainerState {
                        running: Some(ContainerStateRunning { .. }),
                        ..
                    })
                )
            })
}

fn deadline_exceeded_terminal(status: Option<&PodStatus>) -> bool {
    status.is_some_and(|status| {
        status.phase == Some(Phase::Failed)
            && status.reason.as_deref() == Some(ACTIVE_DEADLINE_REASON)
    })
}

/// Extract the longest `BackoffHint` from an anyhow::Error's source chain.
/// If no hint is found, returns the default backOffPeriod (10s) to match
/// K8s `podWorkers.completeWork` fallback.
pub(crate) fn backoff_from_error(err: &anyhow::Error) -> std::time::Duration {
    crate::lifecycle::BackoffHint::from_anyhow(err)
}

fn terminal_phase_requires_termination(pod: &Pod) -> bool {
    let is_terminal_phase = pod
        .status
        .as_ref()
        .and_then(|s| s.phase.as_ref())
        .map(|p| matches!(p, Phase::Succeeded | Phase::Failed))
        .unwrap_or(false);
    if !is_terminal_phase {
        return false;
    }

    let restart_policy = pod
        .spec
        .as_ref()
        .and_then(|s| s.restart_policy.as_deref())
        .unwrap_or("Always");

    restart_policy != "Always" || deadline_exceeded_terminal(pod.status.as_ref())
}

/// Set `status.allocatable` from `status.capacity`, the `Set Allocatable`
/// step of `MachineInfo` (pkg/kubelet/nodestatus/setters.go:292-316): drop an
/// extended resource capacity no longer lists, then copy every capacity entry.
/// This kubelet reserves nothing (no `--system-reserved`/`--kube-reserved`),
/// so `nodeAllocatableReservationFunc` is empty and each value copies as is.
/// Returns whether allocatable changed.
pub(crate) fn sync_allocatable_with_capacity(
    status: &mut rusternetes_common::resources::NodeStatus,
) -> bool {
    let capacity = status.capacity.clone().unwrap_or_default();
    let allocatable = status.allocatable.get_or_insert_with(HashMap::new);
    let before = allocatable.clone();
    allocatable.retain(|k, _| {
        capacity.contains_key(k) || !rusternetes_common::quota::is_extended_resource_name(k)
    });
    allocatable.extend(capacity);
    *allocatable != before
}

/// `localStorageCapacityIsolation` input of `nodestatus.ReadyCondition`
/// (kubelet_node_status.go `defaultNodeStatusFuncs`): the
/// `LocalStorageCapacityIsolation` feature gate is GA and on by default.
const LOCAL_STORAGE_CAPACITY_ISOLATION: bool = true;

/// The node's advertised capacity/allocatable. Single source of truth so the
/// NodeStatus the kubelet posts and the values used to default resourceFieldRef
/// LIMITS never drift — both the env-var path (via
/// `CriContainerRuntime::with_node_allocatable`) and the downwardAPI/projected
/// *volume* path (via [`crate::volumes::VolumeManager::node_allocatable`]) read
/// this one map.
pub(crate) fn node_allocatable_map() -> HashMap<String, String> {
    HashMap::from([
        ("cpu".to_string(), "4".to_string()),
        ("memory".to_string(), "8Gi".to_string()),
        ("pods".to_string(), "110".to_string()),
        ("ephemeral-storage".to_string(), "100Gi".to_string()),
    ])
}

/// `pkg/kubelet/kuberuntime/kuberuntime_manager.go` `doBackOff` +
/// `client-go/util/flowcontrol.Backoff`.
#[derive(Clone)]
/// `lastStatusReportTime` and `delayAfterNodeStatusChange`
/// (pkg/kubelet/kubelet.go:1370-1375). `last_report == None` is the zero time
/// of a freshly started kubelet.
#[derive(Default)]
struct StatusReportState {
    last_report: Option<std::time::Instant>,
    delay_secs: f64,
}

struct RestartBackoff {
    /// Number of times the kubelet has actually (re)started the container.
    /// Reported verbatim as `restartCount`.
    restart_count: u32,
    /// Wall-clock instant of the most recent restart.
    last_restart: Instant,
    /// Current backoff delay; the container may not be restarted again until
    /// `last_restart + backoff`. Doubles each restart, capped at the max.
    backoff: Duration,
}

/// Pod `(status.hostIP, status.hostIPs)`; `(None, None)` when the host IPs
/// are unknown.
type HostIpFields = (
    Option<String>,
    Option<Vec<rusternetes_common::resources::pod::HostIP>>,
);

pub struct Kubelet {
    node_name: String,
    /// `Kubelet.hostname` (kubelet.go:1137): `nodeutil.GetHostname(
    /// --hostname-override)`, distinct from `nodeName`.
    hostname: String,
    storage: Arc<StorageBackend>,
    /// `kubelet.podCertificateManager` (`kubelet.go:1242-1245`): fed by
    /// `pod_cache.reconcile` as pods are added to / dropped from the node, and
    /// handed to the volume host. A `NoOpManager` unless the
    /// `PodCertificateRequest` gate is on (`kubelet.go:955-972`).
    pod_certificate_manager: Arc<dyn crate::podcertificate::Manager>,
    /// The `IssuingManager` behind `pod_certificate_manager`, whose `run` is
    /// spawned from [`Kubelet::run`] (`kubelet.go:967`).
    pod_certificate_issuing: Option<Arc<crate::podcertificate::IssuingManager<StorageBackend>>>,
    /// `kubelet.podManager` as the pod certificate manager reads it.
    pod_cache: Arc<crate::podcertificate::wiring::PodCache>,
    runtime: Arc<CriContainerRuntime>,
    /// `containerRuntimeVersion` reported in NodeStatus, resolved once at startup
    /// from the CRI Version RPC (`<runtime_name>://<runtime_version>`), not a
    /// hardcoded literal. Falls back to `"unknown"` if the handshake fails.
    container_runtime_version: String,
    /// Runtime/network health fed by the CRI `Status` RPC; gates NodeReady.
    /// Upstream: `Kubelet.runtimeState` (pkg/kubelet/runtime.go).
    runtime_state: crate::runtime_state::RuntimeState,
    /// `Kubelet.recorder` for `recordNodeStatusEvent` (kubelet_node_status.go:632).
    node_event_recorder: rusternetes_storage::EventRecorder<StorageBackend>,
    sync_interval: Duration,
    /// Filesystem path used for `statvfs` when computing nodefs eviction stats.
    /// Defaults to `/var/lib/kubelet` when not provided.
    eviction_root_dir: PathBuf,
    eviction_manager: Mutex<EvictionManager>,
    /// The shared `realImageGCManager` (`kubelet.imageManager`), set once the
    /// kubelet starts; the eviction manager's `imageGC`.
    image_gc_manager: std::sync::OnceLock<Arc<crate::image_gc::ImageGcManager>>,
    /// Per-pod worker state. K8s uses a goroutine per pod; we track state
    /// per-UID and dispatch in the sync loop.
    /// K8s ref: pkg/kubelet/pod_workers.go
    pod_states: Mutex<HashMap<String, PodWorkerState>>,
    /// Per-pod sync lock, keyed by `"{namespace}/{name}"` (NOT UID).
    /// Prevents concurrent sync_pod calls for the same pod, and — because two
    /// incarnations of a recreated pod (e.g. a StatefulSet replacement) share
    /// the name but not the UID — also serializes the old incarnation's
    /// teardown against the new incarnation's stale-incarnation sweep and
    /// container creation (issue #1112).
    ///
    /// Upstream note: the Go kubelet has no single same-name barrier for
    /// regular pods — it gets the equivalent from per-UID pod workers
    /// (pkg/kubelet/pod_workers.go, UpdatePod/podWorkerLoop) whose
    /// termination state machine lets the new UID's worker create containers
    /// only after the old UID's worker has finished terminating; an explicit
    /// same-fullname barrier exists only for static pods (allowStaticPodStart,
    /// startedStaticPodsByFullname). Our name-keyed skip-and-retry is a
    /// rusternetes-specific serialization achieving the same effect.
    pod_sync_locks: crate::sync_locks::SyncLocks,
    /// Per-container CrashLoopBackOff state, keyed by
    /// `"{namespace}/{pod}/{container}"`. Source of truth for restartCount +
    /// restart pacing. See [`RestartBackoff`].
    restart_backoff: Mutex<HashMap<String, RestartBackoff>>,
    /// Terminal pods whose `finalize_terminated_pod_storage` reported removal
    /// but left the object in storage, keyed by the `/registry/...` pod key.
    /// Value is `(failed_attempts, last_attempt)` driving the retry backoff in
    /// [`terminal_finalize_backoff`] (#1157). Cleared once the object is gone.
    terminal_finalize_failures: Mutex<HashMap<String, (u32, Instant)>>,
    /// Track recently-deleted pod names (from watch events) so orphan cleanup
    /// can skip the grace period for pods that were explicitly deleted from storage.
    recently_deleted: Arc<Mutex<HashMap<String, Option<Pod>>>>,
    /// Per-pod worker signal channels. K8s uses one goroutine per pod.
    /// When a watch event arrives for a pod, we signal its channel to
    /// trigger an immediate reconciliation without a full sync_loop.
    pod_workers: Arc<Mutex<HashMap<String, mpsc::Sender<()>>>>,
    /// Unix-seconds timestamp of the most recent successful `sync_loop`
    /// completion. Exposed via [`Kubelet::healthy`] so the HTTP
    /// `/healthz` endpoint can answer 200 only when the reconciler has
    /// ticked recently. Mirrors upstream `pkg/kubelet/kubelet.go`'s
    /// `syncLoopMonitor`. 0 = no successful sync yet.
    last_sync: AtomicU64,
    /// Effective `crashLoopBackOff.maxContainerRestartPeriod` (default 5m).
    crash_loop_backoff_max: Duration,
    /// `boInitial` from `newCrashLoopBackOff` (10s, or 1s with
    /// `ReduceDefaultCrashLoopBackOffDecay`, clamped to the max).
    crash_loop_backoff_initial: Duration,
    /// Port the kubelet API server listens on (the `--metrics-port` flag).
    /// Advertised in the node's `status.daemonEndpoints.kubeletEndpoint.Port`
    /// so the api-server proxies log/exec/metrics requests to the right port.
    /// Every node uses the standard 10250 (each kubelet has its own network
    /// namespace, so there is no clash); the value stays configurable rather
    /// than hardcoded so a future shared-host topology can override it without
    /// the advertised port drifting from the bind port (the conformance
    /// framework hardcodes `<node>:10250`, so deviating breaks node-proxy).
    metrics_port: u16,
    /// Static pod manifest dir (upstream staticPodPath). None = disabled.
    pod_manifest_path: Option<PathBuf>,
    /// `KubeletConfiguration.nodeStatusUpdateFrequency`: how often the
    /// heartbeat loop computes and posts NodeStatus. Upstream
    /// `Kubelet.nodeStatusUpdateFrequency` (pkg/kubelet/kubelet.go:624),
    /// consumed by `wait.JitterUntil(kl.syncNodeStatus, ...)` (:1852).
    node_status_update_frequency: Duration,
    /// `KubeletConfiguration.fileCheckFrequency`: how often the static pod
    /// manifest dir is re-read. Upstream passes
    /// `kubeCfg.FileCheckFrequency.Duration` as the `period` of
    /// `config.NewSourceFile` (pkg/kubelet/kubelet.go:384), whose
    /// `sourceFile.run` re-lists on `time.NewTicker(s.period)`
    /// (pkg/kubelet/config/file.go:93-104).
    file_check_frequency: Duration,
    /// `KubeletConfiguration.httpCheckFrequency`: period of the manifest-URL
    /// source (`config.NewSourceURL(..., kubeCfg.HTTPCheckFrequency.Duration,
    /// ...)`, pkg/kubelet/kubelet.go:390; `wait.Until` in http.go:60).
    http_check_frequency: Duration,
    /// `staticPodURL` source; `None` = not configured (kubelet.go:388).
    static_pod_url_source: Option<Arc<crate::static_pod_url::UrlSource>>,
    /// Last SET update of each static pod source (PodConfig keeps one pod set
    /// per source and merges them, pkg/kubelet/config/config.go).
    file_static_pods: Mutex<Vec<Pod>>,
    http_static_pods: Mutex<Vec<Pod>>,
    /// `KubeletConfiguration.nodeStatusReportFrequency` (default 5m): how
    /// often an UNCHANGED node status is still written. Upstream
    /// `Kubelet.nodeStatusReportFrequency` (pkg/kubelet/kubelet.go:624).
    node_status_report_frequency: Duration,
    /// `ImageGCPolicy` built from `imageMinimumGCAge`, `imageMaximumGCAge` and
    /// `imageGC{High,Low}ThresholdPercent` (kubelet.go:509-515). `None` keeps
    /// image GC off (tests / callers that do not configure it).
    image_gc_policy: Option<crate::image_gc::ImageGcPolicy>,
    /// `lastStatusReportTime` + `delayAfterNodeStatusChange`
    /// (pkg/kubelet/kubelet.go:1374, :1370).
    status_report_state: Mutex<StatusReportState>,
    /// Current file-sourced static pods, keyed by (suffixed) pod name.
    /// Workers consult this before storage so static pods survive
    /// mirror-pod deletion.
    static_pods: Arc<Mutex<HashMap<String, Pod>>>,
    /// Sysctl admission allowlist (safe set + `--allowed-unsafe-sysctls`).
    /// A pod declaring a forbidden sysctl is rejected with reason
    /// `SysctlForbidden`. Upstream `pkg/kubelet/sysctl`.
    sysctl_allowlist: crate::sysctl::Allowlist,
    /// `Kubelet.nodeIPs` (kubelet.go:1450): the parsed `--node-ip`, threaded
    /// from `RunKubelet` -> `createAndInitKubelet` -> `NewMainKubelet`
    /// (server.go:1216-1240, kubelet.go:428, 637).
    node_ips: Vec<std::net::IpAddr>,
    /// `Kubelet.externalCloudProvider` (kubelet.go:1327, 619):
    /// `cloudprovider.IsExternal(--cloud-provider)`.
    external_cloud_provider: bool,
    /// `Kubelet.nodeIPValidator` (kubelet_node_status.go:706-740); a field so
    /// tests can override it as kubelet_node_status_test.go does.
    node_ip_validator: Arc<dyn Fn(std::net::IpAddr) -> Result<(), String> + Send + Sync>,
    /// `status.addresses` of this node as last set by the `NodeAddress`
    /// setter: the stand-in for `kl.nodeLister.Get(kl.nodeName)`
    /// (kubelet_getters.go:289-294). `None` until the first status sync, when
    /// `getNodeAnyWay` falls back to `initialNode`.
    observed_addresses: Mutex<Option<Vec<NodeAddress>>>,
    /// `getHostIPsAnyWay()` published for the CRI runtime's downward-API env
    /// resolution; empty = host IP unknown.
    host_ips: Arc<Mutex<Vec<String>>>,
}

// Kubelet needs Send+Sync for Arc<Kubelet> in spawned tasks
// All fields are Send+Sync: Arc<StorageBackend>, Arc<CriContainerRuntime>, Mutex<EvictionManager>

/// Return true iff `a.status` and `b.status` are semantically equal.
///
/// Compares via canonical JSON to ignore field ordering and `None` vs
/// `Some(default)` differences. Used to gate terminal-pod status writes
/// in `sync_pod` so a Succeeded pod whose status hasn't actually changed
/// doesn't get republished every reconcile cycle. Without this gate, the
/// terminal-pod paths re-derive status from the runtime on every sync,
/// write it back blindly, and emit a MODIFIED watch event — every cycle.
/// Whether a pod's readiness should be gated on a readiness probe — i.e. the
/// pod must start NOT-ready and only flip Ready once a probe succeeds. This
/// covers regular containers AND restartable init containers (sidecars): a
/// sidecar's readiness probe gates pod readiness just like a regular
/// container's (upstream pkg/kubelet/status/generate.go). Missing this for
/// sidecars marks the pod Ready immediately, before the probe's
/// initialDelaySeconds (NodeConformance "readiness before initial delay", #1069).
fn spec_has_readiness_probe(spec: &rusternetes_common::resources::pod::PodSpec) -> bool {
    if spec.containers.iter().any(|c| c.readiness_probe.is_some()) {
        return true;
    }
    spec.init_containers
        .as_ref()
        .map(|ics| {
            ics.iter().any(|c| {
                c.restart_policy.as_deref() == Some("Always") && c.readiness_probe.is_some()
            })
        })
        .unwrap_or(false)
}

/// Re-read the pod from storage for the per-cycle readiness block so that
/// restart_count/last_state and gate conditions patched via /status mid-cycle
/// are seen (the cycle-start `pod` is stale). Falls back to `fallback` on a
/// storage error. Extracted from the sync loop for #2387.
async fn load_readiness_pod<S: Storage>(
    storage: &S,
    namespace: &str,
    pod_name: &str,
    fallback: &Pod,
) -> Pod {
    let key = build_key("pods", Some(namespace), pod_name);
    storage
        .get::<Pod>(&key)
        .await
        .unwrap_or_else(|_| fallback.clone())
}

/// Returns `(containers_ready, ready)` for the per-cycle readiness block.
/// Restartable init containers (sidecars) count toward ContainersReady
/// (upstream pkg/kubelet/status/generate.go); plain init containers do not.
/// Readiness gates are ANDed into Ready only, evaluated on `readiness_pod`
/// (`GeneratePodReadyCondition`). Extracted from the sync loop for #2387.
fn evaluate_pod_readiness(
    readiness_pod: &Pod,
    container_statuses: &[ContainerStatus],
    init_container_statuses: Option<&[ContainerStatus]>,
) -> (bool, bool) {
    let init_sidecars_ready = match init_container_statuses {
        Some(init) => {
            let ics = readiness_pod
                .spec
                .as_ref()
                .and_then(|sp| sp.init_containers.as_ref());
            init.iter().all(|s| {
                let is_sidecar = ics
                    .and_then(|list| list.iter().find(|ic| ic.name == s.name))
                    .and_then(|ic| ic.restart_policy.as_deref())
                    == Some("Always");
                !is_sidecar || s.ready
            })
        }
        None => true,
    };
    let containers_ready = container_statuses.iter().all(|s| s.ready) && init_sidecars_ready;
    let ready = containers_ready && readiness_gates_satisfied(readiness_pod);
    (containers_ready, ready)
}

/// True when every condition named in `spec.readinessGates` is present in
/// `status.conditions` with status `"True"`. A pod with no readinessGates is
/// trivially satisfied. Mirrors upstream `GeneratePodReadyCondition`: the Ready
/// condition ANDs the gates on top of container readiness, so an unsatisfied
/// gate must hold `Ready=False` even when all containers are ready.
fn readiness_gates_satisfied(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.readiness_gates.as_ref())
        .map(|gates| {
            gates.iter().all(|gate| {
                pod.status
                    .as_ref()
                    .and_then(|st| st.conditions.as_ref())
                    .map(|conds| {
                        conds
                            .iter()
                            .any(|c| c.condition_type == gate.condition_type && c.status == "True")
                    })
                    .unwrap_or(false)
            })
        })
        .unwrap_or(true)
}

pub fn pod_status_equal(a: &Pod, b: &Pod) -> bool {
    serde_json::to_value(&a.status).ok() == serde_json::to_value(&b.status).ok()
}

/// Whether a terminating pod's grace period has run out. Since the api-server
/// follows `rest.BeforeDelete`, `deletionTimestamp` is the *deadline*
/// (`now + gracePeriod`), so the grace has elapsed once `now >= deletionTimestamp`.
/// Upstream: staging/src/k8s.io/apiserver/pkg/registry/rest/delete.go:163
/// (`requestedDeletionTimestamp := metav1.NewTime(metav1Now().Add(time.Second *
/// time.Duration(*options.GracePeriodSeconds)))`). The kill grace itself is
/// `pod.DeletionGracePeriodSeconds` (kuberuntime_container.go:1411).
pub(crate) fn terminating_grace_elapsed(
    deletion_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    deletion_timestamp.map(|dt| now >= dt).unwrap_or(true)
}

/// Decide whether a pod that has reached `TerminatedPod` should be removed
/// from storage, and remove it if so. Returns `Ok(true)` when the object was
/// deleted, `Ok(false)` when it was intentionally retained.
///
/// An explicitly-deleted pod (`deletionTimestamp` set) with no finalizers
/// MUST be removed: rusternetes has no finalizer-driven tombstone GC, so the
/// kubelet deletes directly. Leaving it makes the next reconcile default back
/// to `SyncPod`, which re-detects `deletionTimestamp => needs_terminating` and
/// re-enters `TerminatingPod` indefinitely (a `pelagos stop` storm).
///
/// A naturally-terminated pod (no `deletionTimestamp`) is retained so
/// `kubectl get pod` keeps showing its terminal status. A pod with an
/// unresolved finalizer is retained so the finalizer owner can still observe
/// it. Extracted from `sync_pod` so the decision can be unit-tested against a
/// real `Storage` backend rather than a predicate recomputed in the test.
pub async fn finalize_terminated_pod_storage<S: Storage + ?Sized>(
    storage: &S,
    key: &str,
    has_deletion_timestamp: bool,
    has_finalizers: bool,
) -> Result<bool> {
    if has_deletion_timestamp && !has_finalizers {
        storage.delete(key).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Graft the kubelet-owned fields of `desired` onto an existing Node object.
///
/// Only the labels the kubelet owns are refreshed; `spec` is left alone unless
/// the existing object has none. The kubelet does not own `spec`:
///
/// * `spec.podCIDR` / `podCIDRs` belong to kube-controller-manager's node-ipam.
///   Overwriting them with the kubelet's empty spec makes a strict api-server
///   reject the whole update (`node updates may not change podCIDR except from
///   "" to valid`), so the kubelet can never re-register after a restart.
/// * `spec.taints` and `spec.unschedulable` belong to the taint controllers and
///   to `kubectl taint` / `kubectl cordon`.
///
/// Upstream ref: pkg/kubelet/kubelet_node_status.go:110-130
/// (`tryRegisterWithAPIServer`) re-gets the node and reconciles only the CMAD
/// annotation, the default labels and extended resources — never the spec.
pub fn reconcile_existing_node(existing: &mut Node, desired: &Node) {
    existing.metadata.labels = desired.metadata.labels.clone();
    if existing.spec.is_none() {
        existing.spec = desired.spec.clone();
    }
}

impl Kubelet {
    /// Construct a Kubelet with upstream-default eviction config and
    /// `root_dir = /var/lib/kubelet`. Kept for library back-compat; the
    /// binary uses [`Kubelet::new_with_eviction`] to honor CLI flags.
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub async fn new(
        node_name: String,
        storage: Arc<StorageBackend>,
        sync_interval_secs: u64,
        volume_dir: String,
        cluster_dns: String,
        cluster_domain: String,
        network: String,
        kubernetes_service_host: String,
    ) -> Result<Self> {
        Self::new_with_eviction(
            node_name,
            storage,
            sync_interval_secs,
            volume_dir,
            cluster_dns,
            cluster_domain,
            network,
            kubernetes_service_host,
            PathBuf::from("/var/lib/kubelet"),
            EvictionManager::new(),
            10250,
            Vec::new(),
            Vec::new(),
            false,
        )
        .await
    }

    /// Construct a Kubelet with an explicit eviction-root path (for statvfs)
    /// and a pre-configured `EvictionManager`. Used by the kubelet binary's
    /// main to honor `--root-dir`, `--eviction-hard`, etc.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_eviction(
        node_name: String,
        storage: Arc<StorageBackend>,
        sync_interval_secs: u64,
        volume_dir: String,
        cluster_dns: String,
        cluster_domain: String,
        network: String,
        kubernetes_service_host: String,
        eviction_root_dir: PathBuf,
        eviction_manager: EvictionManager,
        metrics_port: u16,
        allowed_unsafe_sysctls: Vec<String>,
        node_ips: Vec<std::net::IpAddr>,
        external_cloud_provider: bool,
    ) -> Result<Self> {
        let node_ip_validator: Arc<dyn Fn(std::net::IpAddr) -> Result<(), String> + Send + Sync> =
            Arc::new(crate::node_ip::validate_node_ip_on_host);
        let host_ips: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        // CRI runtime backend (containerd + crun). The endpoint and runtime
        // handler come from the standard kubelet env vars; pod networking is
        // owned entirely by containerd's CNI plugin (CNI is the only
        // pod-networking path). `allowed_unsafe_sysctls` IS wired: it builds
        // the sysctl admission allowlist below. Cluster DNS is applied via the
        // runtime.
        let _ = &network;
        let socket = std::env::var("CONTAINER_RUNTIME_ENDPOINT")
            .unwrap_or_else(|_| "unix:///run/containerd/containerd.sock".to_string());
        let log_root = format!("{volume_dir}/pod-logs");

        // VolumeManager shares the kubelet's storage + the api-server JWT secret
        // (for projected service-account tokens), mirroring the bollard runtime.
        let jwt_secret = std::env::var("JWT_SECRET")
            .unwrap_or_else(|_| "rusternetes-secret-change-in-production".to_string());
        let token_manager = rusternetes_common::auth::TokenManager::new_auto(jwt_secret.as_bytes());
        // Pod certificate manager (`kubelet.go:955-972`), built before the
        // volume host so the projected `podCertificate` source can reach it.
        let pod_cache = crate::podcertificate::wiring::PodCache::new();
        let (pod_certificate_manager, pod_certificate_issuing) =
            crate::podcertificate::wiring::new_pod_certificate_manager(
                storage.clone(),
                pod_cache.clone(),
                &node_name,
            );
        let volumes = crate::volumes::VolumeManager::new_with_pod_certificate_manager(
            volume_dir.clone(),
            Some(storage.clone()),
            token_manager,
            Some(pod_certificate_manager.clone()),
        );

        // The kubernetes Service host:port injected into pods as
        // KUBERNETES_SERVICE_* so in-cluster clients reach the api-server.
        let service_host = if kubernetes_service_host.is_empty() {
            "10.96.0.1".to_string()
        } else {
            kubernetes_service_host
        };
        let runtime = CriContainerRuntime::connect(&socket, log_root)
            .await
            .map_err(|e| anyhow::anyhow!("connecting to CRI runtime at {socket}: {e}"))?
            .with_volumes(volumes)
            .with_event_recorder(storage.clone())
            .with_service_host(service_host, "443")
            .with_cluster_dns(&cluster_dns, &cluster_domain)
            .with_node_name(&node_name)
            // Configurer.nodeIPs (dns.go): the kubelet's node InternalIP.
            // `dns.NewConfigurer(..., nodeIPs, ...)` (kubelet.go:1017): the
            // `--node-ip` flag value, not the computed addresses.
            .with_node_ips(node_ips.iter().map(|ip| ip.to_string()).collect())
            .with_host_ips(host_ips.clone())
            .with_node_allocatable(node_allocatable_map());

        // Resolve the runtime identity once via the CRI Version RPC so
        // NodeStatus reports the runtime actually behind CONTAINER_RUNTIME_ENDPOINT
        // (e.g. containerd-rs://0.1.2) instead of a hardcoded literal.
        let container_runtime_version = match runtime.runtime_version_string().await {
            Ok(s) => {
                info!("CRI runtime: {s} (endpoint {socket})");
                s
            }
            Err(e) => {
                warn!(
                    "CRI Version RPC failed at {socket} ({e}); reporting containerRuntimeVersion as 'unknown'"
                );
                "unknown".to_string()
            }
        };

        // Log the resolved statvfs path once so operators can see which
        // mount we're measuring for eviction. Upstream cadvisor logs this
        // in `fs.go::GetFsInfoForPath`.
        crate::eviction::log_statvfs_path(&eviction_root_dir);

        let node_event_recorder = rusternetes_storage::EventRecorder::new(storage.clone());
        let kubelet = Self {
            hostname: node_name.clone(),
            node_name,
            storage,
            pod_certificate_manager,
            pod_certificate_issuing,
            pod_cache,
            runtime: Arc::new(runtime),
            container_runtime_version,
            runtime_state: crate::runtime_state::RuntimeState::new(
                crate::runtime_state::MAX_WAIT_FOR_CONTAINER_RUNTIME,
            ),
            node_event_recorder,
            sync_interval: Duration::from_secs(sync_interval_secs),
            eviction_root_dir,
            eviction_manager: Mutex::new(eviction_manager),
            image_gc_manager: std::sync::OnceLock::new(),
            pod_states: Mutex::new(HashMap::new()),
            terminal_finalize_failures: Mutex::new(HashMap::new()),
            pod_sync_locks: crate::sync_locks::SyncLocks::new(),
            restart_backoff: Mutex::new(HashMap::new()),
            recently_deleted: Arc::new(Mutex::new(HashMap::new())),
            pod_workers: Arc::new(Mutex::new(HashMap::new())),
            last_sync: AtomicU64::new(0),
            crash_loop_backoff_max: new_crash_loop_backoff(
                crate::config::KubeletConfiguration::default_max_container_restart_period(None),
            )
            .0,
            crash_loop_backoff_initial: new_crash_loop_backoff(
                crate::config::KubeletConfiguration::default_max_container_restart_period(None),
            )
            .1,
            metrics_port,
            pod_manifest_path: None,
            node_status_update_frequency: Duration::from_secs(10),
            file_check_frequency: Duration::from_secs(20),
            http_check_frequency: Duration::from_secs(20),
            static_pod_url_source: None,
            file_static_pods: Mutex::new(Vec::new()),
            http_static_pods: Mutex::new(Vec::new()),
            node_status_report_frequency: Duration::from_secs(300),
            image_gc_policy: None,
            status_report_state: Mutex::new(StatusReportState::default()),
            static_pods: Arc::new(Mutex::new(HashMap::new())),
            sysctl_allowlist: crate::sysctl::Allowlist::new(&allowed_unsafe_sysctls),
            node_ips,
            external_cloud_provider,
            node_ip_validator,
            observed_addresses: Mutex::new(None),
            host_ips,
        };
        kubelet.publish_host_ips();
        Ok(kubelet)
    }

    /// `metrics.RegisterCollectors(collectors.PodCertificateCollectorFor(m))`
    /// (`kubelet.go:969`): expose `kubelet_podcertificate_states` on
    /// `registry`. Upstream registers only alongside the `IssuingManager`, so a
    /// `NoOpManager` kubelet registers nothing.
    pub fn register_pod_certificate_metrics(&self, registry: &prometheus::Registry) {
        if self.pod_certificate_issuing.is_none() {
            return;
        }
        let collector = crate::podcertificate::wiring::PodCertificateCollector::new(
            self.pod_certificate_manager.clone(),
        );
        if let Err(e) = registry.register(Box::new(collector)) {
            warn!("registering kubelet_podcertificate_states: {e}");
        }
    }

    /// Enable static pods from a manifest directory (kubeadm staticPodPath).
    pub fn with_pod_manifest_path(mut self, path: Option<PathBuf>) -> Self {
        self.pod_manifest_path = path;
        self
    }

    /// Apply `KubeletConfiguration.nodeStatusUpdateFrequency` (with the
    /// upstream default of 10s when unset/zero, `defaults.go`).
    pub fn with_node_status_update_frequency(mut self, d: Option<Duration>) -> Self {
        self.node_status_update_frequency = d
            .filter(|d| !d.is_zero())
            .unwrap_or(Duration::from_secs(10));
        self
    }

    /// Apply `KubeletConfiguration.fileCheckFrequency` (20s when unset/zero,
    /// v1beta1/defaults.go).
    pub fn with_file_check_frequency(mut self, d: Option<Duration>) -> Self {
        self.file_check_frequency = d
            .filter(|d| !d.is_zero())
            .unwrap_or(Duration::from_secs(20));
        self
    }

    /// Apply `KubeletConfiguration.httpCheckFrequency` (20s when unset/zero,
    /// v1beta1/defaults.go).
    pub fn with_http_check_frequency(mut self, d: Option<Duration>) -> Self {
        self.http_check_frequency = d
            .filter(|d| !d.is_zero())
            .unwrap_or(Duration::from_secs(20));
        self
    }

    /// Add the manifest-URL source (`staticPodURL` + `staticPodURLHeader`,
    /// pkg/kubelet/kubelet.go:366-391). Empty URL = none, as upstream's
    /// `kubeCfg.StaticPodURL != ""`.
    pub fn with_static_pod_url(
        mut self,
        url: Option<&str>,
        header: &std::collections::BTreeMap<String, Vec<String>>,
    ) -> Result<Self> {
        if let Some(url) = url.filter(|u| !u.is_empty()) {
            info!(url, "Adding pod URL with HTTP header");
            self.static_pod_url_source = Some(Arc::new(crate::static_pod_url::UrlSource::new(
                url,
                header,
                &self.node_name,
            )?));
        }
        Ok(self)
    }

    /// Period of the manifest-URL source poll in `run`.
    pub(crate) fn static_pod_url_poll_interval(&self) -> Duration {
        self.http_check_frequency
    }

    /// Period of the static-pod file source poll in `run`.
    pub(crate) fn static_pod_poll_interval(&self) -> Duration {
        self.file_check_frequency
    }

    /// One pass of the file source (`sourceFile.listConfig`,
    /// pkg/kubelet/config/file.go:121): re-read the manifest dir, project
    /// mirror pods into storage, and publish the result to the static-pod
    /// cache that `sync_loop` consumes.
    pub(crate) async fn poll_static_pods(&self) {
        let Some(dir) = &self.pod_manifest_path else {
            return;
        };
        let pods = crate::static_pods::load_static_pods(dir, &self.node_name);
        *self.file_static_pods.lock().unwrap() = pods;
        self.publish_static_pods().await;
    }

    /// One pass of the URL source (`sourceURL.run`, pkg/kubelet/config/http.go:63):
    /// a SET update replaces this source's pods; a failed fetch keeps the
    /// previous set. Returns whether a source is configured.
    pub(crate) async fn poll_static_pod_url(&self) -> bool {
        let Some(src) = &self.static_pod_url_source else {
            return false;
        };
        if let Some(pods) = src.run().await {
            *self.http_static_pods.lock().unwrap() = pods;
            self.publish_static_pods().await;
        }
        true
    }

    /// Merge the per-source pod sets, project mirror pods and publish the
    /// result to the cache `sync_loop` consumes.
    async fn publish_static_pods(&self) {
        let mut pods: Vec<Pod> = self.file_static_pods.lock().unwrap().clone();
        pods.extend(self.http_static_pods.lock().unwrap().iter().cloned());
        if let Err(e) =
            crate::static_pods::reconcile_mirror_pods(self.storage.as_ref(), &self.node_name, &pods)
                .await
        {
            warn!("static pods: mirror reconcile failed: {}", e);
        }
        *self.static_pods.lock().unwrap() = pods
            .iter()
            .map(|p| (p.metadata.name.clone(), p.clone()))
            .collect();
    }

    /// Apply `crashLoopBackOff.maxContainerRestartPeriod` (already validated to
    /// [1s, 300s]; `None` keeps the 300s default). Ported from
    /// `newCrashLoopBackOff` (pkg/kubelet/kubelet.go:345-358).
    pub fn with_crash_loop_backoff_max(mut self, d: Option<Duration>) -> Self {
        if d.is_some() {
            let (m, i) = new_crash_loop_backoff(d);
            self.crash_loop_backoff_max = m;
            self.crash_loop_backoff_initial = i;
        }
        self
    }

    /// Enable image garbage collection with the policy `NewMainKubelet` builds
    /// from the KubeletConfiguration (kubelet.go:509-515).
    pub fn with_image_gc_policy(mut self, policy: crate::image_gc::ImageGcPolicy) -> Self {
        self.image_gc_policy = Some(policy);
        self
    }

    /// Initial backoff from `newCrashLoopBackOff` (kubelet.go:345-361).
    pub(crate) fn crash_loop_backoff_initial(&self) -> Duration {
        self.crash_loop_backoff_initial
    }

    /// Apply `KubeletConfiguration.nodeStatusReportFrequency` (5m when
    /// unset/zero; callers pass `effective_node_status_report_frequency()`).
    pub fn with_node_status_report_frequency(mut self, d: Option<Duration>) -> Self {
        self.node_status_report_frequency = d
            .filter(|d| !d.is_zero())
            .unwrap_or(Duration::from_secs(300));
        self
    }

    /// `Kubelet.nodeIPValidator` override (defaults to `validateNodeIP`,
    /// kubelet_node_status.go:706-740).
    /// `Kubelet.hostname` = `nodeutil.GetHostname(--hostname-override)`
    /// (server.go:1208, kubelet.go:601). Callers resolve it with
    /// [`crate::node_ip::get_hostname`].
    pub fn with_hostname(mut self, hostname: &str) -> Self {
        self.hostname = hostname.to_string();
        self.publish_host_ips();
        self
    }

    pub fn with_node_ip_validator(
        mut self,
        validator: impl Fn(std::net::IpAddr) -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        self.node_ip_validator = Arc::new(validator);
        self.publish_host_ips();
        self
    }

    /// Apply `KubeletConfiguration.runtimeRequestTimeout` (2m when unset/zero,
    /// v1beta1/defaults.go:188-189) to the CRI client. Must be called right
    /// after construction, before the runtime handle is shared.
    pub fn with_runtime_request_timeout(mut self, d: Option<Duration>) -> Self {
        let d = d
            .filter(|d| !d.is_zero())
            .unwrap_or(rusternetes_cri::DEFAULT_RUNTIME_REQUEST_TIMEOUT);
        let rt = Arc::get_mut(&mut self.runtime)
            .expect("runtime handle is unshared at construction time");
        let updated = rt.clone().with_runtime_request_timeout(d);
        *rt = updated;
        self
    }

    /// Set the host resolver file (`--resolv-conf`; empty disables the host
    /// base and falls back to localhost, `dns.go:431`).
    pub fn with_resolv_conf(mut self, path: &str) -> Self {
        let rt = Arc::get_mut(&mut self.runtime)
            .expect("runtime handle is unshared at construction time");
        let updated = rt.clone().with_resolv_conf(path);
        *rt = updated;
        self
    }

    /// The CRI per-request timeout in effect (test/diagnostic accessor).
    #[cfg(test)]
    pub(crate) fn runtime_request_timeout(&self) -> Duration {
        self.runtime.runtime_request_timeout()
    }

    /// Interval of the dedicated NodeStatus heartbeat in `run`.
    pub(crate) fn node_status_heartbeat_interval(&self) -> Duration {
        self.node_status_update_frequency
    }

    /// Test hook: pretend the last report went out `ago` ago with no delay.
    #[cfg(test)]
    fn set_last_status_report_for_test(&self, ago: Duration) {
        let mut st = self.status_report_state.lock().unwrap();
        st.last_report = std::time::Instant::now().checked_sub(ago);
        st.delay_secs = 0.0;
    }

    /// One period of the heartbeat loop with the upstream 4% jitter:
    /// `wait.JitterUntil(kl.syncNodeStatus, kl.nodeStatusUpdateFrequency,
    /// 0.04, true, ...)` (pkg/kubelet/kubelet.go:1852).
    fn jittered_node_status_interval(&self) -> Duration {
        crate::node_status::jitter(
            self.node_status_heartbeat_interval(),
            crate::node_status::NODE_STATUS_LOOP_JITTER_FACTOR,
            crate::node_status::rand_unit(),
        )
    }

    /// Liveness probe — true iff `sync_loop` completed inside the
    /// stale-sync window (`max(6, 2 × sync_interval)` seconds, mirroring
    /// upstream's `syncLoopHealthCheck` constants in
    /// `pkg/kubelet/kubelet.go`). Backs the HTTP `/healthz` endpoint.
    /// Returns `false` until the first successful sync_loop tick.
    pub fn healthy(&self) -> bool {
        let last = self.last_sync.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let stale_after = self.sync_interval.as_secs().saturating_mul(2).max(6);
        now.saturating_sub(last) <= stale_after
    }

    /// Ensure the node-heartbeat Lease carries an `OwnerReference` back to its
    /// owning Node.
    ///
    /// The conformance test `[sig-node] NodeLease should have OwnerReferences set`
    /// (k8s.io/kubernetes/test/e2e/common/node/node_lease.go) asserts that every
    /// kubelet-managed Lease in `kube-node-lease` references its Node via a
    /// single owner reference with apiVersion `v1`, kind `Node`, the node's
    /// name, and the node's UID.
    ///
    /// Upstream kubelet's `pkg/kubelet/nodelease/controller.go` `setOwnerFunc`
    /// has the same self-heal semantics that this helper implements: if the
    /// stored Lease already carries a matching owner reference it is left
    /// alone, otherwise the field is overwritten with the single canonical
    /// reference. Returns `true` if the lease metadata was mutated.
    fn apply_node_lease_owner_ref(
        lease: &mut rusternetes_common::resources::Lease,
        node_name: &str,
        node_uid: &str,
    ) -> bool {
        if node_uid.is_empty() {
            return false;
        }
        let matches_existing = lease
            .metadata
            .owner_references
            .as_ref()
            .and_then(|refs| refs.first())
            .map(|owner| {
                owner.api_version == "v1"
                    && owner.kind == "Node"
                    && owner.name == node_name
                    && owner.uid == node_uid
            })
            .unwrap_or(false);
        if matches_existing {
            return false;
        }
        let owner = rusternetes_common::types::OwnerReference::new(
            "v1",
            "Node",
            node_name.to_string(),
            node_uid.to_string(),
        );
        lease.metadata.owner_references = Some(vec![owner]);
        true
    }

    pub async fn run(self: &Arc<Self>) -> Result<()> {
        info!("Kubelet started for node: {}", self.node_name);

        // `kl.dnsConfigurer.CheckLimitsForResolvConf` at syncLoop start
        // (`kubelet.go:2524-2526`).
        self.runtime.check_limits_for_resolv_conf().await;

        // `go podCertificateManager.Run(ctx)` (`kubelet.go:967`). The kubelet
        // runs for the life of the process, so the token is never cancelled.
        if let Some(issuing) = self.pod_certificate_issuing.clone() {
            tokio::spawn(issuing.run(crate::podcertificate::tokio_util_cancel::Token::new()));
        }

        // Check runtime state once before registering so the first Ready
        // report is honest (kubelet.go:1849-1851), then keep polling every
        // 5s (kubelet.go:1867).
        self.update_runtime_up().await;
        {
            let rt_self = Arc::clone(self);
            tokio::spawn(async move {
                let mut t = tokio::time::interval(Duration::from_secs(5));
                loop {
                    t.tick().await;
                    rt_self.update_runtime_up().await;
                }
            });
        }

        // Register the node
        self.register_node().await?;

        // Startup cleanup: immediately remove any containers from previous runs
        // that don't correspond to pods in etcd. K8s kubelet does a full
        // reconciliation at startup to ensure no stale containers remain.
        self.startup_cleanup().await;

        // Container GC — runs every 60 seconds to remove dead containers.
        // K8s ref: pkg/kubelet/kubelet.go — ContainerGCPeriod = 1 minute
        {
            let gc_self = Arc::clone(self);
            tokio::spawn(async move {
                // Wait before first GC run to let startup_cleanup finish
                // and pods populate in etcd
                tokio::time::sleep(Duration::from_secs(60)).await;
                let mut gc_timer = tokio::time::interval(Duration::from_secs(60));
                loop {
                    gc_timer.tick().await;
                    gc_self.container_gc().await;
                }
            });
        }

        // Image GC: `NewImageGCManager` (kubelet.go:897), `imageManager.Start`
        // (kubelet.go:1709) and the image half of `StartGarbageCollection`.
        if let Some(policy) = self.image_gc_policy.clone() {
            let cri = self.runtime.cri_client();
            match crate::image_gc::ImageGcManager::new(
                Arc::new(crate::image_gc_cri::CriImageGcRuntime::new(cri.clone())),
                Arc::new(crate::image_gc_cri::CriStatsProvider::new(cri)),
                Vec::new(),
                Arc::new(crate::image_gc_cri::NodeEvents::new(
                    rusternetes_storage::EventRecorder::new(self.storage.clone()),
                    &self.node_name,
                )),
                policy,
            ) {
                Ok(manager) => {
                    let manager = Arc::new(manager);
                    let _ = self.image_gc_manager.set(manager.clone());
                    tokio::spawn(manager.clone().run_detect_loop());
                    tokio::spawn(manager.run_gc_loop());
                }
                Err(err) => anyhow::bail!("failed to initialize image manager: {err}"),
            }
        }

        // Channel for watch events to trigger immediate pod syncs
        let (watch_tx, mut watch_rx) = mpsc::channel::<String>(256);

        // Start a background watch on pod changes to react immediately
        // instead of waiting for the next poll cycle
        let storage_clone = self.storage.clone();
        let node_name = self.node_name.clone();
        let watch_tx_clone = watch_tx.clone();
        let recently_deleted_clone = self.recently_deleted.clone();
        tokio::spawn(async move {
            let prefix = build_prefix("pods", None);
            loop {
                match storage_clone.watch(&prefix).await {
                    Ok(mut stream) => {
                        use futures::StreamExt;
                        while let Some(event) = stream.next().await {
                            match event {
                                Ok(
                                    WatchEvent::Added(key, value)
                                    | WatchEvent::Modified(key, value),
                                ) => {
                                    // Parse to check nodeName reliably (avoid string matching on JSON)
                                    if let Ok(pod) =
                                        serde_json::from_str::<serde_json::Value>(&value)
                                    {
                                        let assigned_node =
                                            pod.pointer("/spec/nodeName").and_then(|v| v.as_str());
                                        if assigned_node == Some(&node_name) {
                                            let _ = watch_tx_clone.try_send(key);
                                        }
                                    }
                                }
                                Ok(WatchEvent::Deleted(key, prev_value)) => {
                                    // Only trigger for pods that were on our node
                                    if let Ok(pod) =
                                        serde_json::from_str::<serde_json::Value>(&prev_value)
                                    {
                                        let assigned_node =
                                            pod.pointer("/spec/nodeName").and_then(|v| v.as_str());
                                        if assigned_node == Some(&node_name) {
                                            // Cache the pod spec so orphan cleanup can run preStop hooks
                                            if let Some(pod_name) = pod
                                                .pointer("/metadata/name")
                                                .and_then(|v| v.as_str())
                                            {
                                                // Keyed "<namespace>/<name>": bare
                                                // names repeat across namespaces.
                                                let pod_ns = pod
                                                    .pointer("/metadata/namespace")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("default");
                                                let cached_pod =
                                                    serde_json::from_value::<Pod>(pod.clone()).ok();
                                                recently_deleted_clone.lock().unwrap().insert(
                                                    format!("{pod_ns}/{pod_name}"),
                                                    cached_pod,
                                                );
                                            }
                                            let _ = watch_tx_clone.try_send(key);
                                        }
                                    }
                                }
                                Err(e) => {
                                    debug!("Pod watch error: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to start pod watch: {}", e);
                    }
                }
                // Reconnect after a brief pause
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        // Full sync interval is the safety net — runs less frequently
        // The watch-triggered syncs handle the fast path
        let full_sync_interval = Duration::from_secs(self.sync_interval.as_secs().max(1));
        let mut full_sync_timer = tokio::time::interval(full_sync_interval);
        // File source poll: first tick fires immediately ("Read path
        // immediately to speed up startup", file.go:96), then every
        // fileCheckFrequency.
        let mut static_pod_timer = tokio::time::interval(self.static_pod_poll_interval());
        // URL source poll (`wait.Until(run, httpCheckFrequency)`, http.go:60):
        // first pass immediately, then every httpCheckFrequency.
        let mut static_pod_url_timer = tokio::time::interval(self.static_pod_url_poll_interval());
        // File source watch (`startWatch`, file.go:116): inotify events on the
        // manifest dir trigger an immediate pass; the timer above stays as the
        // resync / fallback.
        let (static_pod_watch_tx, mut static_pod_watch_rx) =
            tokio::sync::mpsc::channel(crate::static_pod_watch::EVENT_BUFFER_LEN);
        if let Some(dir) = &self.pod_manifest_path {
            // NewSourceFile: "requires a path without trailing /"
            let dir = std::path::PathBuf::from(dir.to_string_lossy().trim_end_matches('/'));
            tokio::spawn(crate::static_pod_watch::start_watch(
                dir,
                static_pod_watch_tx.clone(),
            ));
        }
        drop(static_pod_watch_tx);

        // Lease-based heartbeat in a SEPARATE task.
        // K8s kubelet uses Lease objects (coordination.k8s.io/v1) for heartbeats
        // since v1.14. The Lease is in the kube-node-lease namespace and is a
        // separate object from the Node, so updates NEVER conflict with node
        // status updates (no CAS conflicts).
        //
        // The node controller checks the Lease renewTime to determine if
        // the node is healthy. This is lightweight (just one field update)
        // and reliable (no competing writers).
        //
        // K8s ref: pkg/kubelet/util/nodelease.go, pkg/kubelet/kubelet.go:235
        {
            let lease_storage = self.storage.clone();
            let lease_node_name = self.node_name.clone();
            tokio::spawn(async move {
                let lease_key = format!("/registry/leases/kube-node-lease/{}", lease_node_name);
                let node_key = build_key("nodes", None, &lease_node_name);
                let mut lease_timer = tokio::time::interval(Duration::from_secs(10));

                // Ensure kube-node-lease namespace exists
                let ns_key = "/registry/namespaces/kube-node-lease";
                if lease_storage
                    .get::<serde_json::Value>(ns_key)
                    .await
                    .is_err()
                {
                    let ns = serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Namespace",
                        "metadata": {"name": "kube-node-lease"}
                    });
                    let _ = lease_storage.create(ns_key, &ns).await;
                }

                // Look up and cache the Node UID for OwnerReferences. Conformance
                // test [sig-node] NodeLease "should have OwnerReferences set"
                // requires every kubelet-managed Lease to carry an ownerRef
                // back to its Node. K8s ref:
                // pkg/kubelet/nodelease/controller.go — setOwnerFunc fetches the
                // node once and stamps the OwnerReference onto every Lease the
                // kubelet creates or repairs.
                //
                // The node UID is immutable after registration, so caching it
                // avoids a storage lookup on every 10s heartbeat. Retry a few
                // times in case the node CREATE hasn't been observed yet (e.g.
                // an etcd compaction or eventual-consistency window).
                let mut cached_node_uid: Option<String> = None;
                for attempt in 0..5 {
                    match lease_storage
                        .get::<rusternetes_common::resources::Node>(&node_key)
                        .await
                    {
                        Ok(node) if !node.metadata.uid.is_empty() => {
                            cached_node_uid = Some(node.metadata.uid);
                            break;
                        }
                        Ok(_) => {
                            tracing::warn!(
                                "Lease heartbeat: node {} has empty UID on attempt {}",
                                lease_node_name,
                                attempt
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Lease heartbeat: failed to read node {} for ownerRef (attempt {}): {}",
                                lease_node_name,
                                attempt,
                                e
                            );
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                if cached_node_uid.is_none() {
                    tracing::warn!(
                        "Lease heartbeat: starting without cached node UID for {}; \
                         ownerReferences will be populated lazily once the node is readable",
                        lease_node_name
                    );
                }

                loop {
                    lease_timer.tick().await;
                    let now = chrono::Utc::now();

                    // Lazy-load the node UID if it wasn't available at startup.
                    if cached_node_uid.is_none() {
                        if let Ok(node) = lease_storage
                            .get::<rusternetes_common::resources::Node>(&node_key)
                            .await
                        {
                            if !node.metadata.uid.is_empty() {
                                cached_node_uid = Some(node.metadata.uid);
                            }
                        }
                    }

                    // Try to update existing lease
                    match lease_storage
                        .get::<rusternetes_common::resources::Lease>(&lease_key)
                        .await
                    {
                        Ok(mut lease) => {
                            if let Some(ref mut spec) = lease.spec {
                                spec.renew_time = Some(now);
                            }
                            // Backfill ownerReferences if the existing lease was
                            // written by an earlier kubelet that didn't set them.
                            // Upstream kubelet's setOwnerFunc has the same
                            // self-heal behaviour so a controller restart fixes
                            // pre-existing leases without manual intervention.
                            if let Some(uid) = cached_node_uid.as_deref() {
                                Self::apply_node_lease_owner_ref(&mut lease, &lease_node_name, uid);
                            }
                            match lease_storage.update(&lease_key, &lease).await {
                                Ok(_) => {
                                    tracing::debug!(
                                        "Lease heartbeat: renewed for node {}",
                                        lease_node_name
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Lease heartbeat: update failed for {}: {}",
                                        lease_node_name,
                                        e
                                    );
                                }
                            }
                        }
                        Err(_) => {
                            // Lease doesn't exist — create it
                            let mut lease = rusternetes_common::resources::Lease::new(
                                lease_node_name.clone(),
                                "kube-node-lease",
                            )
                            .with_spec(
                                rusternetes_common::resources::LeaseSpec {
                                    holder_identity: Some(lease_node_name.clone()),
                                    lease_duration_seconds: Some(40),
                                    acquire_time: Some(now),
                                    renew_time: Some(now),
                                    lease_transitions: Some(0),
                                    preferred_holder: None,
                                    strategy: None,
                                },
                            );
                            if let Some(uid) = cached_node_uid.as_deref() {
                                Self::apply_node_lease_owner_ref(&mut lease, &lease_node_name, uid);
                            }
                            match lease_storage.create(&lease_key, &lease).await {
                                Ok(_) => {
                                    tracing::debug!(
                                        "Lease heartbeat: created lease for node {}",
                                        lease_node_name
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Lease heartbeat: create failed for {}: {}",
                                        lease_node_name,
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            });
        }

        loop {
            tokio::select! {
                // Watch-triggered: a specific pod changed, signal its per-pod worker
                Some(key) = watch_rx.recv() => {
                    // Extract pod name and namespace from key
                    // Key format: /registry/pods/{namespace}/{name}
                    let parts: Vec<&str> = key.split('/').collect();
                    let pod_name = parts.last().unwrap_or(&"").to_string();
                    if pod_name.is_empty() { continue; }

                    // Signal the per-pod worker if one exists
                    let has_worker = {
                        let workers = self.pod_workers.lock().unwrap();
                        if let Some(tx) = workers.get(&pod_name) {
                            let _ = tx.try_send(());
                            true
                        } else {
                            false
                        }
                    };

                    // If no worker exists, start one
                    if !has_worker {
                        self.ensure_pod_worker(&pod_name).await;
                    }
                }
                // Static pod file source (NewSourceFile, kubelet.go:384)
                _ = static_pod_timer.tick() => {
                    self.poll_static_pods().await;
                    if self.pod_manifest_path.is_some() {
                        if let Err(e) = self.sync_loop().await {
                            error!("Error in static pod sync: {}", e);
                        }
                    }
                }
                // Static pod URL source (NewSourceURL, kubelet.go:390)
                _ = static_pod_url_timer.tick(), if self.static_pod_url_source.is_some() => {
                    if self.poll_static_pod_url().await {
                        if let Err(e) = self.sync_loop().await {
                            error!("Error in static pod sync: {}", e);
                        }
                    }
                }
                Some(ev) = static_pod_watch_rx.recv() => {
                    // consumeWatchEvent (file.go): coalesce a burst, then
                    // re-list (see static_pod_watch module docs).
                    tracing::debug!(file = %ev.file_name.display(), kind = ?ev.event_type, "static pod manifest event");
                    while static_pod_watch_rx.try_recv().is_ok() {}
                    self.poll_static_pods().await;
                    if let Err(e) = self.sync_loop().await {
                        error!("Error in static pod sync: {}", e);
                    }
                }
                // Periodic full sync as safety net
                _ = full_sync_timer.tick() => {
                    // Timeout sync_loop to prevent blocking heartbeat.
                    // sync_loop is fire-and-forget so it should return in
                    // <100ms, but storage.list can be slow under load.
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        self.sync_loop(),
                    ).await {
                        Ok(Ok(())) => {},
                        Ok(Err(e)) => error!("Error in periodic sync: {}", e),
                        Err(_) => warn!("sync_loop timed out after 5s"),
                    }
                    // Always send heartbeat after sync
                    if let Err(e) = self.update_node_status().await {
                        error!("Error updating node status: {}", e);
                    }
                }
                // Dedicated heartbeat — runs every nodeStatusUpdateFrequency
                // (default 10s) independently of sync
                _ = tokio::time::sleep(self.jittered_node_status_interval()) => {
                    if let Err(e) = self.update_node_status().await {
                        error!("Error updating node status: {}", e);
                    }
                }
            }
        }
    }

    /// Port of `updateRuntimeUp` (pkg/kubelet/kubelet.go:3113): poll the CRI
    /// `Status` RPC and record RuntimeReady/NetworkReady in `runtime_state`.
    async fn update_runtime_up(&self) {
        let status = self
            .runtime
            .runtime_status()
            .await
            .map_err(|e| e.to_string());
        self.runtime_state.update_from_status(status);
    }

    /// `kubeletVolumeHost.SetKubeletError` (pkg/kubelet/volume_host.go:122):
    /// the CSI plugin's way to keep the node NotReady until it is initialized.
    pub fn set_kubelet_error(&self, err: Option<String>) {
        self.runtime_state.set_storage_state(err);
    }

    /// Errors gating NodeReady (setters.go:491 runtime + network + storage).
    fn ready_errors(&self) -> Vec<String> {
        [
            self.runtime_state.runtime_errors(),
            self.runtime_state.network_errors(),
            self.runtime_state.storage_errors(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    async fn register_node(&self) -> Result<()> {
        info!("Registering node: {}", self.node_name);

        let mut node = Node::new(&self.node_name);

        // Set default node labels matching K8s kubelet initialNode()
        // See: pkg/kubelet/kubelet_node_status.go:initialNode()
        node.metadata.labels = Some(default_node_labels(&self.hostname));

        // Set node spec to mark it as schedulable
        node.spec = Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: Some(false),
            // initialNode (kubelet_node_status.go:325-333): an external cloud
            // provider's CCM removes this taint once it initialises the node.
            taints: self.external_cloud_provider.then(|| {
                vec![rusternetes_common::resources::Taint {
                    key: crate::node_ip::TAINT_EXTERNAL_CLOUD_PROVIDER.to_string(),
                    value: Some("true".to_string()),
                    effect: "NoSchedule".to_string(),
                    time_added: None,
                }]
            }),
        });

        // Set node status
        let capacity = node_allocatable_map();
        node.status = Some(NodeStatus {
            capacity: Some(capacity.clone()),
            allocatable: Some(node_allocatable_map()),
            conditions: Some({
                let mut conditions = Vec::new();
                crate::runtime_state::set_ready_condition(
                    &mut conditions,
                    Some(&capacity),
                    self.ready_errors(),
                    chrono::Utc::now(),
                    LOCAL_STORAGE_CAPACITY_ISOLATION,
                );
                conditions
            }),
            // Filled by the NodeAddress setter below (setNodeStatus).
            addresses: None,
            node_info: Some(rusternetes_common::resources::NodeSystemInfo {
                machine_id: format!("rusternetes-{}", self.node_name),
                system_uuid: format!("rusternetes-{}", self.node_name),
                boot_id: format!("rusternetes-{}", self.node_name),
                kernel_version: "6.1.0-rusternetes".to_string(),
                os_image: "Rusternetes OS".to_string(),
                container_runtime_version: self.container_runtime_version.clone(),
                kubelet_version: "v1.35.0-rusternetes".to_string(),
                kube_proxy_version: "v1.35.0-rusternetes".to_string(),
                operating_system: "linux".to_string(),
                architecture: "amd64".to_string(),
                swap: None,
            }),
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            // Advertise the port the kubelet API server actually listens on
            // (the --metrics-port flag), so the api-server proxies log/exec/
            // metrics to the right port. Every node uses the standard 10250
            // (separate network namespaces, no clash); advertising the actual
            // bind port — rather than hardcoding 10250 — keeps the two in sync
            // if a future topology overrides it. The conformance framework
            // hardcodes <node>:10250, so the bind port must stay 10250.
            // See: pkg/kubelet/kubelet.go:505 — DaemonEndpoints{KubeletEndpoint{Port: kubeCfg.Port}}
            daemon_endpoints: Some(rusternetes_common::resources::NodeDaemonEndpoints {
                kubelet_endpoint: Some(rusternetes_common::resources::DaemonEndpoint {
                    port: self.metrics_port as i32,
                }),
            }),
            config: None,
            features: None,
            runtime_handlers: None,
            // KEP-5328: leave empty until the kubelet's feature-discovery
            // surface is wired up. The api-server treats `None` the same
            // as "no features declared".
            declared_features: None,
        });

        // initialNode ends with `kl.setNodeStatus(ctx, node)` (:361): run the
        // NodeAddress setter so the Node is created with its addresses and
        // (external provider) the provided-node-ip annotation.
        self.run_node_address_setter(&mut node);

        let key = build_key("nodes", None, &self.node_name);

        // Debug: log what we're trying to store
        let node_json = serde_json::to_string_pretty(&node)
            .unwrap_or_else(|_| "failed to serialize".to_string());
        debug!("Registering node with spec: {}", node_json);

        // Try to create, if it exists, update it. `create`/`update` carry the
        // node spec + labels; the node STATUS (addresses, capacity, Ready
        // condition) is written separately via update_status because a full
        // create/PUT strips `.status` through the api-server (storage mode is
        // unaffected — its update_status grafts the same status).
        match self.storage.create(&key, &node).await {
            Ok(_) => info!("Node registered successfully"),
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                // Kubelet restart: the Node object already exists. Re-GET it to
                // carry its current resourceVersion + UID, then graft our
                // labels/spec onto the fresh object before updating. PUTting the
                // freshly-built `node` (no resourceVersion/UID) is rejected by a
                // strict vanilla api-server with a 409 precondition failure
                // (#1638). Mirrors upstream `tryRegisterWithApiServer`'s
                // "node already exists → reconcile onto the existing object" path
                // (pkg/kubelet/kubelet_node_status.go).
                let mut existing: Node = self.storage.get(&key).await?;
                reconcile_existing_node(&mut existing, &node);
                self.storage.update(&key, &existing).await?;
                info!("Node updated successfully");
            }
            Err(e) => return Err(e.into()),
        }
        self.storage.update_status(&key, &node).await?;

        Ok(())
    }

    /// `net.LookupIP(node.Name)` (setters.go:170); the error is discarded
    /// (`addrs, _ = net.LookupIP(node.Name)`).
    fn lookup_node_ips(name: &str) -> Vec<std::net::IpAddr> {
        std::net::ToSocketAddrs::to_socket_addrs(&(name, 0u16))
            .map(|a| a.map(|s| s.ip()).collect())
            .unwrap_or_default()
    }

    /// `resolveAddressFunc` = `netutils.ResolveBindAddress`
    /// (apimachinery util/net/interface.go:468-483).
    fn resolve_node_ip(node_ip: Option<std::net::IpAddr>) -> Result<std::net::IpAddr, String> {
        crate::node_ip::net_interface::resolve_bind_address(node_ip).map_err(|e| e.to_string())
    }

    /// `kl.GetNode()` / `kl.getNodeAnyWay()` (kubelet_getters.go:289-306), as
    /// far as the node's addresses go: the last-observed node, else
    /// `initialNode` (kubelet_node_status.go:301-364), which runs
    /// `setNodeStatus` and with it the `NodeAddress` setter.
    fn get_node_addresses_any_way(&self) -> Vec<NodeAddress> {
        if let Some(observed) = self.observed_addresses.lock().unwrap().clone() {
            return observed;
        }
        let mut node = Node::new(&self.node_name);
        if let Err(e) = self.set_node_address(&mut node) {
            error!(
                "Failed to set some node status fields: {e} (node {})",
                self.node_name
            );
        }
        node.status.and_then(|s| s.addresses).unwrap_or_default()
    }

    /// `kl.getHostIPsAnyWay()` (kubelet_getters.go:318-326):
    /// `utilnode.GetNodeHostIPs(kl.getNodeAnyWay())`.
    fn get_host_ips_any_way(&self) -> Result<Vec<std::net::IpAddr>, String> {
        crate::node_ip::get_node_host_ips(&self.get_node_addresses_any_way())
    }

    /// Refresh the host-IP handle read by the CRI runtime.
    fn publish_host_ips(&self) {
        let ips = self
            .get_host_ips_any_way()
            .map(|ips| ips.iter().map(|ip| ip.to_string()).collect())
            .unwrap_or_default();
        *self.host_ips.lock().unwrap() = ips;
    }

    /// Pod `status.hostIP` / `status.hostIPs` from `getHostIPsAnyWay`
    /// (kubelet_pods.go:2005-2024): on error "Cannot get host IPs" is logged
    /// (V(4)) and both stay unset.
    fn host_ip_fields(&self) -> HostIpFields {
        match self.get_host_ips_any_way() {
            Ok(host_ips) => (
                Some(host_ips[0].to_string()),
                Some(
                    host_ips
                        .iter()
                        .map(|ip| rusternetes_common::resources::pod::HostIP { ip: ip.to_string() })
                        .collect(),
                ),
            ),
            Err(e) => {
                debug!("Cannot get host IPs: {e}");
                (None, None)
            }
        }
    }

    /// The host-IP block at the end of `generateAPIPodStatus`
    /// (kubelet_pods.go:2005-2040): sets HostIP/HostIPs, lets hostNetwork pods
    /// inherit the node IPs as PodIPs, and records `HostIPsIPFamilyMismatch`.
    async fn apply_host_ips_to_pod(&self, pod: &mut Pod) {
        let host_network = pod.spec.as_ref().and_then(|s| s.host_network) == Some(true);
        let Some(status) = pod.status.as_mut() else {
            return;
        };
        match self.get_host_ips_any_way() {
            Ok(host_ips) => {
                if let Some(msg) = crate::node_ip::apply_host_ips(host_network, status, &host_ips) {
                    self.runtime
                        .emit_event(
                            pod,
                            None,
                            "HostIPsIPFamilyMismatch",
                            rusternetes_common::resources::EventType::Warning,
                            &msg,
                        )
                        .await;
                }
            }
            Err(e) => debug!("Cannot get host IPs: {e}"),
        }
    }

    /// `nodestatus.NodeAddress(kl.nodeIPs, kl.nodeIPValidator, kl.hostname,
    /// kl.externalCloudProvider, utilnet.ResolveBindAddress)` applied to
    /// `node` (kubelet_node_status.go:678).
    fn set_node_address(&self, node: &mut Node) -> Result<(), String> {
        crate::node_ip::set_node_address(
            node,
            &self.node_ips,
            &*self.node_ip_validator,
            &self.hostname,
            self.external_cloud_provider,
            &Self::lookup_node_ips,
            &Self::resolve_node_ip,
        )
    }

    /// One `setNodeStatus` pass of the NodeAddress setter
    /// (kubelet_node_status.go:661-670): an error is logged and the node is
    /// left as the setter left it. Afterwards the observed-node cache that
    /// backs `getHostIPsAnyWay` follows the node's addresses.
    fn run_node_address_setter(&self, node: &mut Node) {
        if let Err(e) = self.set_node_address(node) {
            error!(
                "Failed to set some node status fields: {e} (node {})",
                node.metadata.name
            );
        }
        *self.observed_addresses.lock().unwrap() = Some(
            node.status
                .as_ref()
                .and_then(|s| s.addresses.clone())
                .unwrap_or_default(),
        );
        self.publish_host_ips();
    }

    async fn update_node_status(&self) -> Result<()> {
        debug!("Updating node status");

        let key = build_key("nodes", None, &self.node_name);
        let mut node: Node = self.storage.get(&key).await?;
        // `originalNode` of tryUpdateNodeStatus: change detection compares
        // against it (kubelet_node_status.go:483-531).
        let original_node = node.clone();

        // Get current node resource statistics via upstream-parity statvfs.
        let node_stats = self.observe_node_stats().await;

        // Check if eviction is needed — scoped block ensures the MutexGuard
        // is dropped before any subsequent .await points. When the eviction
        // subsystem is disabled (empty thresholds), this is a no-op beyond
        // clearing any stale pressure conditions.
        let active_signals = {
            let mut eviction_manager = self.eviction_manager.lock().unwrap();
            if eviction_manager.is_disabled() {
                // Disabled: ensure conditions are False/cleared without churning logs.
                eviction_manager.update_node_conditions(&mut node, &[])?;
                Vec::new()
            } else {
                let active_signals = eviction_manager.check_eviction_needed(&node_stats);
                eviction_manager.update_node_conditions(&mut node, &active_signals)?;
                active_signals
            }
        };

        // Ensure default node labels are always set (K8s kubelet updateDefaultLabels)
        update_default_labels(
            &default_node_labels(&self.hostname),
            &mut node.metadata.labels,
        );

        // Ensure capacity, allocatable, and nodeInfo are always set
        if let Some(ref mut status) = node.status {
            if status.capacity.as_ref().is_none_or(|c| c.is_empty()) {
                status.capacity = Some(HashMap::from([
                    ("cpu".to_string(), "4".to_string()),
                    ("memory".to_string(), "8Gi".to_string()),
                    ("pods".to_string(), "110".to_string()),
                    ("ephemeral-storage".to_string(), "100Gi".to_string()),
                ]));
            }
            if status.allocatable.as_ref().is_none_or(|a| a.is_empty()) {
                status.allocatable = Some(HashMap::from([
                    ("cpu".to_string(), "4".to_string()),
                    ("memory".to_string(), "8Gi".to_string()),
                    ("pods".to_string(), "110".to_string()),
                    ("ephemeral-storage".to_string(), "100Gi".to_string()),
                ]));
            }
            // Ensure nodeInfo is populated (may have been lost during updates)
            if status
                .node_info
                .as_ref()
                .is_none_or(|ni| ni.machine_id.is_empty())
            {
                status.node_info = Some(rusternetes_common::resources::NodeSystemInfo {
                    machine_id: format!("rusternetes-{}", self.node_name),
                    system_uuid: format!("rusternetes-{}", self.node_name),
                    boot_id: format!("rusternetes-{}", self.node_name),
                    kernel_version: "6.1.0-rusternetes".to_string(),
                    os_image: "Rusternetes OS".to_string(),
                    container_runtime_version: self.container_runtime_version.clone(),
                    kubelet_version: "v1.35.0-rusternetes".to_string(),
                    kube_proxy_version: "v1.35.0-rusternetes".to_string(),
                    operating_system: "linux".to_string(),
                    architecture: "amd64".to_string(),
                    swap: None,
                });
            }
        }

        // setNodeStatus runs `nodestatus.NodeAddress` on EVERY sync
        // (kubelet_node_status.go:661-670, 678): it re-resolves the addresses,
        // maintains the provided-node-ip annotation and, with an external
        // cloud provider, leaves provider-set addresses alone.
        self.run_node_address_setter(&mut node);

        // Allocatable follows capacity on every sync, extended resources
        // included (nodestatus/setters.go MachineInfo, :292-316). A client
        // that adds an extended resource to `status.capacity` relies on this
        // to see it become schedulable.
        if let Some(status) = node.status.as_mut() {
            sync_allocatable_with_capacity(status);
        }

        // Re-ensure the kubelet endpoint port on EVERY heartbeat, not only at
        // registration. `status.daemonEndpoints.kubeletEndpoint.Port` is read by
        // the e2e metrics grabber and the api-server log/exec/metrics proxy; it
        // was previously set only in register_node, so once a status
        // round-trip dropped it nothing re-added it (unlike capacity /
        // conditions / addresses, which are re-ensured above) — leaving it
        // null and producing "Invalid Kubelet port 0". Upstream's setNodeStatus
        // sets DaemonEndpoints on every sync; mirror that.
        if let Some(ref mut status) = node.status {
            let current_port = status
                .daemon_endpoints
                .as_ref()
                .and_then(|d| d.kubelet_endpoint.as_ref())
                .map(|e| e.port)
                .unwrap_or(0);
            if current_port != self.metrics_port as i32 {
                status.daemon_endpoints =
                    Some(rusternetes_common::resources::NodeDaemonEndpoints {
                        kubelet_endpoint: Some(rusternetes_common::resources::DaemonEndpoint {
                            port: self.metrics_port as i32,
                        }),
                    });
            }
        }

        // NodeReady is composed from runtime health, never asserted
        // (nodestatus.ReadyCondition, setters.go:469).
        let ready_errs = self.ready_errors();
        let mut ready_event = None;
        if let Some(ref mut status) = node.status {
            let capacity = status.capacity.clone();
            let conditions = status.conditions.get_or_insert_with(Vec::new);
            let was_ready = conditions
                .iter()
                .find(|c| c.condition_type == "Ready")
                .map(|c| c.status.clone());
            let update = crate::runtime_state::set_ready_condition(
                conditions,
                capacity.as_ref(),
                ready_errs,
                chrono::Utc::now(),
                LOCAL_STORAGE_CAPACITY_ISOLATION,
            );
            if update.changed && was_ready.as_deref() != Some("True") {
                info!("Node Ready condition changed (was {was_ready:?})");
            }
            ready_event = update.event;
        }
        if let Some(event) = ready_event {
            // recordEventFunc (setters.go:540-548): NodeReady / NodeNotReady.
            crate::runtime_state::record_node_status_event(
                &self.node_event_recorder,
                &self.node_name,
                event,
            )
            .await;
        }

        // tryUpdateNodeStatus (kubelet_node_status.go:483-531): write only if
        // the status changed (heartbeat timestamps ignored) or
        // nodeStatusReportFrequency (+ delayAfterNodeStatusChange) elapsed.
        // The node-heartbeat Lease keeps the node Ready in between.
        let changed = crate::node_status::node_status_has_changed(
            original_node.status.as_ref(),
            node.status.as_ref(),
        ) || original_node.metadata.labels != node.metadata.labels;
        let should_write = {
            let mut st = self.status_report_state.lock().unwrap();
            let expired = crate::node_status::is_update_status_period_expired(
                st.last_report,
                std::time::Instant::now(),
                self.node_status_report_frequency,
                st.delay_secs,
            );
            let should_write = changed || expired;
            if should_write {
                // Fresh random delay on change or first report; fixed
                // interval after a purely periodic one (:517-527).
                st.delay_secs = if changed || st.last_report.is_none() {
                    crate::node_status::calculate_delay_secs(
                        self.node_status_report_frequency,
                        crate::node_status::rand_unit(),
                    )
                } else {
                    0.0
                };
            }
            should_write
        };

        if should_write {
            // patchNodeStatus (kubelet_node_status.go:564-568) ->
            // nodeutil.PatchNodeStatus (component-helpers/node/util/status.go
            // :33-44): a strategic-merge PATCH of the /status subresource
            // carrying only the delta against `original_node`, not a full
            // status PUT. Direct (non-API) backends apply the delta onto the
            // current object (see Storage::patch_status_strategic_merge).
            let patch = crate::node_status::prepare_patch_for_node_status(&original_node, &node)
                .map_err(|e| anyhow::anyhow!("failed to patch status for node: {e}"))?;
            // strategicPatchObject builds the patch schema from the REST
            // scope's kind; a typed object read back from storage may carry
            // no TypeMeta, so name it here (no schema-less merge-key guess).
            node.type_meta = rusternetes_common::types::TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            };
            let _: Node = self
                .storage
                .patch_status_strategic_merge(&key, &patch, &node)
                .await?;
            self.status_report_state.lock().unwrap().last_report = Some(std::time::Instant::now());
        }

        // Collect and publish node + per-pod metrics to storage
        self.publish_node_metrics().await;
        self.publish_pod_metrics().await;

        // If eviction is needed, trigger pod eviction
        if !active_signals.is_empty() {
            if let Err(e) = self.handle_eviction(&active_signals).await {
                error!("Error handling eviction: {}", e);
            }
        }

        Ok(())
    }

    /// Collect container metrics from the runtime and write NodeMetrics to storage.
    /// The api-server reads these to serve the metrics.k8s.io API.
    async fn publish_node_metrics(&self) {
        use rusternetes_common::resources::NodeMetrics;
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        use std::collections::BTreeMap;

        // Get pods assigned to this node
        let all_pods: Vec<Pod> = self
            .storage
            .list(&build_prefix("pods", None))
            .await
            .unwrap_or_default();

        let node_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_deref())
                    .map(|n| n == self.node_name)
                    .unwrap_or(false)
            })
            .collect();

        let (cpu_millicores, memory_bytes) = self.runtime.collect_node_metrics(&node_pods).await;
        let memory_mi = memory_bytes / (1024 * 1024);

        let mut usage = BTreeMap::new();
        usage.insert("cpu".to_string(), format!("{}m", cpu_millicores));
        usage.insert("memory".to_string(), format!("{}Mi", memory_mi));

        let metrics = NodeMetrics {
            type_meta: TypeMeta {
                api_version: "metrics.k8s.io/v1beta1".to_string(),
                kind: "NodeMetrics".to_string(),
            },
            metadata: ObjectMeta {
                creation_timestamp: Some(chrono::Utc::now()),
                ..ObjectMeta::new(self.node_name.clone())
            },
            timestamp: chrono::Utc::now(),
            window: "30s".to_string(),
            usage,
        };

        let metrics_key = format!("/registry/metrics.k8s.io/nodes/{}", self.node_name);
        upsert_metrics(self.storage.as_ref(), &metrics_key, metrics, |m| {
            &mut m.metadata
        })
        .await;
    }

    /// Collect per-pod container usage from the runtime and write `PodMetrics`
    /// to storage. The api-server reads these to serve real
    /// `metrics.k8s.io` pod metrics (replacing synthetic usage=requests), which
    /// in turn lets the HPA controller compute true resource utilization.
    async fn publish_pod_metrics(&self) {
        use rusternetes_common::resources::{ContainerMetrics, PodMetrics};
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        use std::collections::BTreeMap;

        let all_pods: Vec<Pod> = self
            .storage
            .list(&build_prefix("pods", None))
            .await
            .unwrap_or_default();

        // Pods assigned to this node, keyed by name -> namespace.
        let node_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_deref())
                    .map(|n| n == self.node_name)
                    .unwrap_or(false)
            })
            .collect();
        if node_pods.is_empty() {
            return;
        }

        let per_pod = self.runtime.collect_pod_metrics(&node_pods).await;

        for pod in node_pods {
            let name = &pod.metadata.name;
            let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
            let readings = match per_pod.get(name) {
                Some(r) if !r.is_empty() => r,
                // No live stats (e.g. pod not yet running) — skip; the handler
                // falls back to request-based estimates for absent metrics.
                _ => continue,
            };

            let containers: Vec<ContainerMetrics> = readings
                .iter()
                .map(|(cname, cpu_milli, mem_bytes)| {
                    let mut usage = BTreeMap::new();
                    usage.insert("cpu".to_string(), format!("{cpu_milli}m"));
                    usage.insert("memory".to_string(), format!("{mem_bytes}"));
                    ContainerMetrics {
                        name: cname.clone(),
                        usage,
                    }
                })
                .collect();

            let metrics = PodMetrics {
                type_meta: TypeMeta {
                    api_version: "metrics.k8s.io/v1beta1".to_string(),
                    kind: "PodMetrics".to_string(),
                },
                metadata: ObjectMeta {
                    creation_timestamp: Some(chrono::Utc::now()),
                    ..ObjectMeta::new(name.clone()).with_namespace(namespace)
                },
                timestamp: chrono::Utc::now(),
                window: "30s".to_string(),
                containers,
            };

            let key = format!("/registry/metrics.k8s.io/pods/{namespace}/{name}");
            upsert_metrics(self.storage.as_ref(), &key, metrics, |m| &mut m.metadata).await;
        }
    }

    async fn sync_loop(self: &Arc<Self>) -> Result<()> {
        debug!("Running sync loop for node: {}", self.node_name);

        // Get all pods — used for both node-pod filtering and orphan cleanup
        let all_pods_prefix = build_prefix("pods", None);
        let all_pods: Vec<Pod> = self.storage.list(&all_pods_prefix).await?;

        // Static pods: the file source is polled on its own ticker
        // (`fileCheckFrequency`, see `poll_static_pods` / `run`); the sync
        // loop consumes the last published set.
        let static_pods: Vec<Pod> = self.static_pods.lock().unwrap().values().cloned().collect();

        let node_pods: Vec<Pod> =
            crate::static_pods::merge_node_pods(all_pods.clone(), static_pods, &self.node_name);

        debug!("Found {} pods assigned to this node", node_pods.len());

        // HandlePodAdditions / HandlePodRemoves: TrackPod / ForgetPod for pods
        // that arrived on / left the node (kubelet.go:2729, :2948).
        self.pod_cache
            .reconcile(&node_pods, self.pod_certificate_manager.as_ref());

        // Ensure per-pod workers exist for all assigned pods and signal them.
        // K8s ref: pkg/kubelet/pod_workers.go — podWorkerLoop (long-lived)
        for pod in &node_pods {
            let pod_name = &pod.metadata.name;

            // Signal existing worker or create a new one
            let has_worker = {
                let workers = self.pod_workers.lock().unwrap();
                if let Some(tx) = workers.get(pod_name.as_str()) {
                    let _ = tx.try_send(());
                    true
                } else {
                    false
                }
            };

            if !has_worker {
                self.ensure_pod_worker(pod_name).await;
            }
        }

        // Clean up workers for pods that are no longer assigned to this node
        {
            let worker_names: Vec<String> =
                self.pod_workers.lock().unwrap().keys().cloned().collect();
            let pod_names: HashSet<&str> =
                node_pods.iter().map(|p| p.metadata.name.as_str()).collect();
            for name in worker_names {
                if !pod_names.contains(name.as_str()) {
                    // Pod no longer exists — remove worker (it will shut down on next recv)
                    self.pod_workers.lock().unwrap().remove(&name);
                }
            }
        }

        // Legacy one-shot sync for backward compatibility during transition.
        // TODO: Remove once per-pod workers are proven stable.
        // For now, run one-shot syncs for any pod that doesn't have a worker yet.
        for pod in &node_pods {
            let pod = pod.clone();
            let kubelet = Arc::clone(self);
            let timeout_secs = 120u64;
            tokio::spawn(async move {
                let body = serde_json::to_vec(&pod).unwrap_or_default().into();
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_secs),
                    rusternetes_common::dump::with_payload(body, kubelet.sync_pod(&pod)),
                )
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        let err_str = e.to_string();
                        if err_str.contains("Failed to create container")
                            || err_str.contains("Failed to pull image")
                            || err_str.contains("FailedToStart")
                        {
                            tracing::error!(
                                "Fatal error syncing pod {}/{}: {}",
                                pod.metadata.namespace.as_deref().unwrap_or(""),
                                pod.metadata.name,
                                err_str
                            );
                            let _ = kubelet.update_pod_status_error(&pod, &err_str).await;
                        } else {
                            tracing::warn!(
                                "Transient error syncing pod {}/{} (will retry): {}",
                                pod.metadata.namespace.as_deref().unwrap_or(""),
                                pod.metadata.name,
                                err_str
                            );
                        }
                    }
                    Err(_) => {
                        tracing::warn!(
                            "Pod sync timed out for {}/{} ({}s)",
                            pod.metadata.namespace.as_deref().unwrap_or(""),
                            pod.metadata.name,
                            timeout_secs
                        );
                    }
                }
            });
        }

        // Pod sync tasks now run independently (fire-and-forget).
        // Error handling is in each spawned task above.
        // This matches K8s podWorkerLoop which runs independently per pod.

        // Clean up orphaned containers using the pod list we already fetched
        if let Err(e) = self
            .cleanup_orphaned_containers(&node_pods, &all_pods)
            .await
        {
            error!("Error cleaning up orphaned containers: {}", e);
        }

        // ...and the pod directories those containers left behind. Upstream
        // pairs the two in HandlePodCleanups, which calls
        // cleanupOrphanedPodDirs right after its container cleanup
        // (pkg/kubelet/kubelet_pods.go:1304). Running it on the sync loop rather
        // than at shutdown is what makes it survive a crash or a killed
        // teardown, which is when directories are most likely to be stranded.
        //
        // The live set is deliberately over-inclusive: a UID missing from it
        // would make a live pod look orphaned, so every pod we know about goes
        // in. A pod listed here that has no directory costs nothing.
        let live_pod_uids: std::collections::HashSet<String> = all_pods
            .iter()
            .chain(node_pods.iter())
            .map(|p| p.metadata.uid.clone())
            .filter(|uid| !uid.is_empty())
            .collect();
        // Unmount volumes first: upstream's reconciler unmounts before
        // cleanupOrphanedPodDirs, which refuses to remove a mounted volume.
        // Pods that are deleted (not in the live set) and pods that are
        // terminated but still in the API both no longer want their volumes.
        let terminated_pod_uids: std::collections::HashSet<String> = all_pods
            .iter()
            .chain(node_pods.iter())
            .filter(|p| pod_volumes_released(p))
            .map(|p| p.metadata.uid.clone())
            .filter(|uid| !uid.is_empty())
            .collect();
        self.runtime
            .unmount_orphaned_volumes(&live_pod_uids, &terminated_pod_uids)
            .await;
        // ...then release the staged devices nothing holds or wants any more
        // (reconciler unmountDetachDevices, reconciler_common.go:273): after
        // unpublish, so a shared device outlives every pod using it.
        let desired_pods: Vec<Pod> = node_pods
            .iter()
            .filter(|p| !pod_volumes_released(p))
            .cloned()
            .collect();
        self.runtime.unmount_unused_csi_devices(&desired_pods).await;
        self.runtime.cleanup_orphaned_pod_dirs(&live_pod_uids);

        // Garbage-collect terminal pods (Succeeded/Failed) from storage.
        // K8s has a terminated-pod-gc-threshold (default 12500) and the kubelet
        // periodically cleans up terminal pods. This prevents accumulation of
        // stale pod records that block namespace deletion.
        self.cleanup_terminal_pods(&node_pods).await;

        // Record completion for the /healthz liveness probe.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_sync.store(now, Ordering::Relaxed);

        Ok(())
    }

    /// Garbage-collect terminal pods (Succeeded/Failed) from storage.
    /// K8s has a terminated-pod-gc-threshold (default 12500) and the kubelet's
    /// In real K8s, the kubelet does NOT delete pods from the API server.
    /// Pod lifecycle is managed by the API server — pods are cleaned up by
    /// namespace deletion, GC (owner reference), or Job TTL controller.
    /// The kubelet only reports status changes.
    /// Previously this deleted Failed/Succeeded pods, which broke tests that
    /// need to observe terminal pod status (e.g., terminated reason check).
    async fn cleanup_terminal_pods(&self, _node_pods: &[Pod]) {
        // No-op: let the API server manage pod lifecycle.
    }

    /// Startup cleanup: remove all containers that don't correspond to pods
    /// in etcd. Runs once at kubelet startup before the main sync loop.
    /// K8s kubelet does this in syncLoopIteration → HandlePodCleanups.
    async fn startup_cleanup(&self) {
        info!("Running startup cleanup — removing stale containers from previous runs");

        // Get all pods from etcd
        let all_pods: Vec<Pod> = match self.storage.list("/registry/pods/").await {
            Ok(pods) => pods,
            Err(e) => {
                warn!("Failed to list pods for startup cleanup: {}", e);
                return;
            }
        };

        // (namespace, name) — a bare name would match a same-named pod in
        // another namespace and spare (or worse, remove) the wrong sandbox.
        let existing_pods: std::collections::HashSet<(String, String)> = all_pods
            .iter()
            .map(|p| {
                (
                    p.metadata.namespace.clone().unwrap_or_default(),
                    p.metadata.name.clone(),
                )
            })
            .collect();

        // Get all containers from Docker (including exited) so orphan cleanup
        // can remove stopped containers from deleted pods
        let running_pods = match self.runtime.list_all_pods().await {
            Ok(pods) => pods,
            Err(e) => {
                warn!("Failed to list pods for startup cleanup: {}", e);
                return;
            }
        };

        // Find orphans — present in the runtime but not in etcd
        let orphans: Vec<(String, String)> = running_pods
            .into_iter()
            .filter(|ns_name| !existing_pods.contains(ns_name))
            .collect();

        if orphans.is_empty() {
            info!("Startup cleanup: no stale containers found");
            return;
        }

        info!(
            "Startup cleanup: found {} stale containers, removing",
            orphans.len()
        );

        // Clean up in parallel (up to 10 concurrent) for fast startup
        let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
        let mut handles = Vec::new();

        for (orphan_ns, orphan_name) in orphans {
            let runtime = self.runtime.clone();
            let sem = semaphore.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                if let Err(e) = runtime.stop_and_remove_pod(&orphan_ns, &orphan_name).await {
                    warn!(
                        "Startup cleanup: failed to remove {}/{}: {}",
                        orphan_ns, orphan_name, e
                    );
                } else {
                    info!(
                        "Startup cleanup: removed stale container {}/{}",
                        orphan_ns, orphan_name
                    );
                }
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        info!("Startup cleanup complete");
    }

    async fn cleanup_orphaned_containers(
        &self,
        _current_pods: &[Pod],
        all_existing_pods: &[Pod],
    ) -> Result<()> {
        debug!("Checking for orphaned containers");

        // Reuse the pod list already fetched by sync_loop to avoid a redundant etcd round-trip.
        // NOTE: Terminal+deleted pod container cleanup is handled by the container GC
        // (container_gc method), which runs independently every 60 seconds and removes
        // exited containers. This orphan cleanup only handles containers whose pods
        // have been fully removed from etcd.
        // Keyed on (namespace, name): a bare name matches same-named pods in
        // other namespaces, which either spares a real orphan or (worse, in the
        // cleanup below) kills a live pod.
        let existing_pods: std::collections::HashSet<(String, String)> = all_existing_pods
            .iter()
            .map(|p| {
                (
                    p.metadata.namespace.clone().unwrap_or_default(),
                    p.metadata.name.clone(),
                )
            })
            .collect();

        debug!("Found {} pods in etcd", existing_pods.len());

        // Get every pod the runtime knows (including exited) so orphan cleanup
        // removes stopped containers from deleted pods.
        let running_pods = self.runtime.list_all_pods().await?;
        debug!(
            "Found {} running pods in container runtime",
            running_pods.len()
        );

        // Check for orphaned pods — present in the container runtime but not
        // found in etcd.
        // K8s ref: pkg/kubelet/kubelet_pods.go:1270 — kills orphaned runtime
        // pods not in workingPods with a 1-second grace period.
        //
        // IMPORTANT: In a shared runtime, ALL kubelets see ALL containers.
        // We must not kill containers belonging to other nodes' pods.
        //
        // There is no separate "a pod worker still tracks this" check: pod_states
        // is keyed by pod UID, and an orphan is by definition absent from etcd, so
        // its UID is unknown here. (The previous check compared UID keys against
        // pod names and could never match.)
        for (running_ns, running_pod_name) in &running_pods {
            if existing_pods.contains(&(running_ns.clone(), running_pod_name.clone())) {
                continue; // Pod exists in etcd — not an orphan
            }
            let deleted_key = format!("{running_ns}/{running_pod_name}");
            // Fast path: if this pod was explicitly deleted (via watch event),
            // skip the grace period and clean up immediately.
            let cached_pod = self
                .recently_deleted
                .lock()
                .unwrap()
                .get(&deleted_key)
                .cloned()
                .flatten();
            let is_recently_deleted = cached_pod.is_some()
                || self
                    .recently_deleted
                    .lock()
                    .unwrap()
                    .contains_key(&deleted_key);
            if !is_recently_deleted {
                // Check container age — don't kill containers younger than 30s
                let container_age = self
                    .runtime
                    .get_container_age(running_ns, running_pod_name)
                    .await
                    .unwrap_or(std::time::Duration::from_secs(0));
                if container_age < std::time::Duration::from_secs(30) {
                    debug!(
                        "Skipping recently started orphan {} (age {:?})",
                        deleted_key, container_age
                    );
                    continue;
                }
            } else {
                // Remove from tracker — we're about to clean it up
                self.recently_deleted
                    .lock()
                    .unwrap()
                    .remove(deleted_key.as_str());
                info!(
                    "Fast-path cleanup for explicitly deleted pod {} — skipping grace period",
                    deleted_key
                );
            }
            // Re-check etcd before cleanup — a new pod with the same name may have
            // been created since we fetched the pod list at the start of sync_loop.
            // Without this check, we'd delete volumes that the new pod needs.
            let still_orphaned = {
                let fresh_pods: Vec<Pod> = self
                    .storage
                    .list("/registry/pods/")
                    .await
                    .unwrap_or_default();
                !fresh_pods.iter().any(|p| {
                    p.metadata.name == *running_pod_name
                        && p.metadata.namespace.as_deref().unwrap_or_default() == *running_ns
                })
            };
            if !still_orphaned {
                debug!(
                    "Pod {} was recreated in etcd — skipping cleanup",
                    deleted_key
                );
                continue;
            }

            info!(
                "Found orphaned pod {} - not in etcd, stopping and removing containers",
                deleted_key
            );
            // Stop orphaned containers. K8s HandlePodCleanups kills orphaned
            // runtime pods with a 1-second grace period. Container removal is
            // left to the container GC.
            // K8s ref: pkg/kubelet/kubelet_pods.go:1280
            if let Some(ref pod) = cached_pod {
                // If we have a cached pod spec, run preStop hooks
                let grace = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.termination_grace_period_seconds)
                    .unwrap_or(1); // K8s uses 1s for orphans
                if let Err(e) = self.runtime.stop_pod_for(pod, grace).await {
                    warn!("Failed to stop orphaned pod {}: {}", deleted_key, e);
                }
            } else {
                // No cached spec — stop with 1s grace, no preStop hooks
                if let Err(e) = self
                    .runtime
                    .stop_pod_with_grace_period(running_ns, running_pod_name, 1)
                    .await
                {
                    warn!("Failed to stop orphaned pod {}: {}", deleted_key, e);
                }
            }
        }

        // Stale "created" containers and exited orphan containers are handled
        // by the container GC (garbage_collect_containers), which runs independently
        // every 60 seconds. K8s ref: pkg/kubelet/kuberuntime/kuberuntime_gc.go

        Ok(())
    }

    /// Container garbage collector — runs independently every 60 seconds.
    /// Matches K8s kuberuntime_gc.go behavior:
    /// 1. For deleted pods (not in etcd): remove ALL dead containers
    /// 2. For existing pods: keep at most 1 dead container per pod (for log access)
    /// 3. Remove orphaned pause containers (sandboxes) with no running app containers
    /// 4. Remove stale "created" containers that were never started
    ///
    /// K8s ref: pkg/kubelet/kuberuntime/kuberuntime_gc.go — GarbageCollect
    async fn container_gc(&self) {
        // Get pod names from etcd to distinguish deleted vs existing pods
        // K8s ref: evictContainers checks allSourcesReady
        let existing_pods: HashSet<String> = self
            .storage
            .list::<Pod>("/registry/pods/")
            .await
            .unwrap_or_default()
            .iter()
            .map(|p| p.metadata.name.clone())
            .collect();

        match self
            .runtime
            .garbage_collect_containers(&existing_pods)
            .await
        {
            Ok(removed) => {
                if removed > 0 {
                    info!("Container GC: removed {} dead/stale containers", removed);
                }
            }
            Err(e) => {
                error!("Container GC failed: {}", e);
            }
        }
    }

    /// Ensure a per-pod worker task exists for the given pod.
    /// K8s ref: pkg/kubelet/pod_workers.go — podWorkerLoop
    ///
    /// Each pod gets a persistent tokio task that:
    /// 1. Waits for a signal on its channel
    /// 2. Reads the latest pod state from storage
    /// 3. Calls sync_pod
    /// 4. Goes back to waiting
    ///
    /// The worker stays alive until the pod is deleted and cleaned up.
    /// This avoids the full sync_loop on every watch event.
    async fn ensure_pod_worker(self: &Arc<Self>, pod_name: &str) {
        let (tx, mut rx) = mpsc::channel::<()>(4);

        // Signal immediately to sync now
        let _ = tx.try_send(());

        {
            let mut workers = self.pod_workers.lock().unwrap();
            if workers.contains_key(pod_name) {
                // Worker already exists — just signal it
                if let Some(existing_tx) = workers.get(pod_name) {
                    let _ = existing_tx.try_send(());
                }
                return;
            }
            workers.insert(pod_name.to_string(), tx);
        }

        let kubelet = Arc::clone(self);
        let name = pod_name.to_string();
        let pod_workers = Arc::clone(&self.pod_workers);
        let node_name = self.node_name.clone();

        tokio::spawn(async move {
            // Per-pod worker loop — stays alive for the lifetime of the pod.
            // K8s ref: pkg/kubelet/pod_workers.go:1508-1534 completeWork —
            // on sync error the pod is requeued with backOffPeriod (10s),
            // not the regular resync interval.  Track the "backoff-until"
            // timestamp per worker so a failing sync does not spam the
            // storage or CRI on every 5s tick.
            let mut backoff_until: Option<tokio::time::Instant> = None;

            loop {
                // Backoff guard (K8s pkg/kubelet/pod_workers.go:1512-1533):
                // if the last sync failed, wait at least backOffPeriod (10s)
                // before retrying.  A new explicit signal can still break
                // out of the backoff early.
                if let Some(until) = backoff_until {
                    let now = tokio::time::Instant::now();
                    if now < until {
                        let remaining = until - now;
                        // Wait out the backoff, but break early if a fresh
                        // signal arrives (controller update, eviction, etc.)
                        match tokio::time::timeout(remaining, rx.recv()).await {
                            Ok(Some(())) => {
                                // Real signal during backoff — drain extras and
                                // proceed to sync immediately
                                while rx.try_recv().is_ok() {}
                                backoff_until = None;
                                continue;
                            }
                            Ok(None) => {
                                debug!(
                                    "Pod worker for {} shutting down (channel closed during backoff)",
                                    name
                                );
                                break;
                            }
                            Err(_) => {
                                // Backoff elapsed — clear and fall through to
                                // the normal wait/sync path below
                                backoff_until = None;
                            }
                        }
                    }
                }

                // Wait for signal (or timeout for periodic re-check)
                let signaled = tokio::time::timeout(
                    Duration::from_secs(5), // periodic fallback re-sync
                    rx.recv(),
                )
                .await;

                // If channel closed, pod worker is being removed
                if matches!(signaled, Ok(None)) {
                    debug!("Pod worker for {} shutting down (channel closed)", name);
                    break;
                }

                // Drain any additional queued signals
                while rx.try_recv().is_ok() {}

                // Read the latest pod state from storage by scanning for this pod.
                // We need to find the namespace since it's not stored in the worker key.
                // Search all namespaces for this pod name assigned to our node.
                let pod = {
                    // File-sourced static pods are authoritative: consult the
                    // cache first so a static pod keeps running even if its
                    // mirror was deleted from storage.
                    let cached = kubelet.static_pods.lock().unwrap().get(&name).cloned();
                    if let Some(p) = cached {
                        Some(p)
                    } else {
                        let prefix = build_prefix("pods", None);
                        match kubelet.storage.list::<Pod>(&prefix).await {
                            Ok(pods) => pods.into_iter().find(|p| {
                                p.metadata.name == name
                                    && p.spec
                                        .as_ref()
                                        .and_then(|s| s.node_name.as_ref())
                                        .map(|n| n == &node_name)
                                        .unwrap_or(false)
                            }),
                            Err(e) => {
                                debug!("Pod worker {}: storage error: {}", name, e);
                                continue;
                            }
                        }
                    }
                };

                match pod {
                    Some(pod) => {
                        // Use a longer timeout for pods being deleted — preStop hooks
                        // plus container stop grace period can exceed 30s.
                        // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go:860
                        //   grace_period can be up to terminationGracePeriodSeconds (default 30s)
                        //   plus preStop hook execution time.
                        // K8s pod workers don't have a per-sync timeout — they
                        // K8s pod workers don't have a per-sync timeout.
                        // 120s is generous enough for container startup + probes.
                        let timeout_secs = 120u64;
                        let body = serde_json::to_vec(&pod).unwrap_or_default().into();
                        match tokio::time::timeout(
                            Duration::from_secs(timeout_secs),
                            rusternetes_common::dump::with_payload(body, kubelet.sync_pod(&pod)),
                        )
                        .await
                        {
                            Ok(Ok(())) => {
                                // Success — clear any prior backoff
                                backoff_until = None;
                            }
                            Ok(Err(e)) => {
                                let err_str = e.to_string();
                                if err_str.contains("Failed to create container")
                                    || err_str.contains("Failed to pull image")
                                {
                                    let _ = kubelet.update_pod_status_error(&pod, &err_str).await;
                                }
                                // K8s ref: completeWork default case (lines 1512-1533):
                                //   backOffPeriod = 10s with jitter, capped at resyncInterval.
                                // Set `backoff_until` so the next loop iteration waits
                                // before retrying.  Use a backoffError-like mechanism: if
                                // the error carries a BackoffHint, honour it.
                                let backoff_dur = backoff_from_error(&e);
                                let backoff_secs = backoff_dur.as_secs();
                                backoff_until = Some(
                                    tokio::time::Instant::now()
                                        + Duration::from_secs(backoff_secs.max(10)),
                                );
                                debug!(
                                    "Pod worker {}: sync error — backing off for {:?}s: {}",
                                    name, backoff_secs, err_str
                                );
                            }
                            Err(_) => {
                                warn!("Pod worker {}: sync timed out", name);
                                // Timeout behaves like an error — backoff too
                                backoff_until =
                                    Some(tokio::time::Instant::now() + Duration::from_secs(10));
                            }
                        }
                    }
                    None => {
                        // Pod no longer exists for this node — stop the worker
                        debug!("Pod worker {}: pod not found, shutting down", name);
                        break;
                    }
                }
            }

            // Clean up worker entry
            pod_workers.lock().unwrap().remove(&name);
            debug!("Pod worker for {} removed", name);
        });
    }

    async fn sync_pod(&self, pod: &Pod) -> Result<()> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let pod_uid = &pod.metadata.uid;

        // Per-pod sync lock: prevent concurrent sync_pod calls for the same pod.
        // K8s uses one goroutine per pod; without this, concurrent syncs create
        // Docker 409 "container name already in use" errors (1014 per run).
        //
        // Keyed by namespace/name, NOT UID: a recreated pod (same name, new
        // UID — the StatefulSet replacement pattern) must not sweep/create
        // containers while a queued sync of the OLD incarnation is still
        // running, or the swept container can be resurrected mid-start
        // (issue #1112). Same-name pods cannot legitimately coexist in
        // storage, so name-keyed skip-and-retry loses no real concurrency.
        let sync_lock_key = format!("{}/{}", namespace, pod_name);
        // Skip-and-retry: if another sync already holds this pod's key, bail (a
        // later reconcile retries). The guard releases the key on every return
        // path. See `crate::sync_locks` for the rationale + tests.
        let _sync_guard = match self.pod_sync_locks.try_acquire(sync_lock_key) {
            Some(guard) => guard,
            None => {
                debug!(
                    "Skipping sync for pod {}/{} (uid {}) — already syncing",
                    namespace, pod_name, pod_uid
                );
                return Ok(());
            }
        };

        debug!("Syncing pod: {}/{}", namespace, pod_name);

        // Pod worker state machine dispatch.
        // K8s ref: pkg/kubelet/pod_workers.go — podWorkerLoop
        //
        // State transitions:
        // - SyncPod (default): normal operation — create/update containers
        // - TerminatingPod: pod is being stopped — stop containers, run preStop hooks
        //   Triggered by: deletionTimestamp set, phase Succeeded/Failed, eviction
        // - TerminatedPod: all containers stopped — update status, clean volumes
        //   Container REMOVAL is NOT done here — it's handled by the container GC.
        //
        // K8s ref: pkg/kubelet/pod_workers.go:110-117
        let current_state = {
            self.pod_states
                .lock()
                .unwrap()
                .get(pod_uid)
                .cloned()
                .unwrap_or(PodWorkerState::SyncPod)
        };

        // Determine if we need to transition to TerminatingPod.
        // Triggers:
        // 1. deletionTimestamp set (API delete, controller delete)
        // 2. Pod phase is terminal (Succeeded/Failed) AND restartPolicy prevents
        //    restart. With restartPolicy=Always, the kubelet restarts containers
        //    instead of terminating — the pod stays in SyncPod state.
        //    K8s ref: pkg/kubelet/pod_workers.go — completeSync checks isTerminal
        //    which is only true when the pod is finished (all containers exited
        //    and restartPolicy doesn't allow restart).
        let terminal_and_done = terminal_phase_requires_termination(pod);
        let needs_terminating = (pod.metadata.deletion_timestamp.is_some() || terminal_and_done)
            && matches!(current_state, PodWorkerState::SyncPod);
        // INVARIANT: a pod whose phase is terminal either transitions to
        // TerminatingPod here or dispatches into the Succeeded|Failed match
        // arm below — it never re-enters the start_pod/Pending paths, so the
        // Pending status writes in those paths don't need the
        // should_skip_phase_write guard; only write sites that re-read a
        // possibly-terminal fresh pod from storage carry it.

        if needs_terminating {
            // Backoff guard (#1157): if a prior terminate cycle reported a
            // finalize-removal but storage still has the object, don't re-stop
            // containers every reconcile. Skip until the backoff window elapses
            // — the failed-finalize path keeps extending it and escalating the
            // log, so a storage-delete regression is loud, not a silent loop.
            let key = build_key("pods", Some(namespace), pod_name);
            if let Some(remaining) = self.terminal_finalize_backoff_remaining(&key) {
                debug!(
                    "Pod {}/{}: delaying re-terminate for {:?} (previous finalize did not remove the object)",
                    namespace, pod_name, remaining
                );
                return Ok(());
            }
            self.pod_states
                .lock()
                .unwrap()
                .insert(pod_uid.clone(), PodWorkerState::TerminatingPod);
            // Fall through to TerminatingPod handling below
        }

        // Re-read state after potential transition
        let current_state = {
            self.pod_states
                .lock()
                .unwrap()
                .get(pod_uid)
                .cloned()
                .unwrap_or(PodWorkerState::SyncPod)
        };

        // === TerminatedPod: update status, clean up resources ===
        // K8s ref: pkg/kubelet/kubelet.go:2467 — SyncTerminatedPod
        // Container removal is NOT done here — left to the container GC.
        if matches!(current_state, PodWorkerState::TerminatedPod) {
            let key = build_key("pods", Some(namespace), pod_name);

            // Keep a gracefully-terminating pod visible (Terminating, NotReady)
            // until its containers actually stop or the grace period elapses.
            // stop_pod_for can return — or a concurrent sync can reach finalize —
            // while a sidecar is still draining; finalizing then removes the pod
            // from the API within ~2s of deletion, racing the readiness flip so
            // watchers never observe Ready=False ("mark readiness on pods to
            // false while pod is in progress of terminating"). Gate on the
            // container state (fast pods, whose containers stop in ~1s, are not
            // delayed) with a grace-period backstop for stuck containers.
            if pod.metadata.deletion_timestamp.is_some() {
                let grace = pod
                    .metadata
                    .deletion_grace_period_seconds
                    .or_else(|| {
                        pod.spec
                            .as_ref()
                            .and_then(|s| s.termination_grace_period_seconds)
                    })
                    .unwrap_or(30);
                let grace_elapsed =
                    terminating_grace_elapsed(pod.metadata.deletion_timestamp, chrono::Utc::now());
                let containers_running = self.runtime.is_pod_running(pod).await.unwrap_or(false);
                if containers_running && !grace_elapsed {
                    debug!(
                        "Pod {}/{} terminating — keeping visible until containers stop ({}s grace)",
                        namespace, pod_name, grace
                    );
                    return Ok(());
                }
            }

            let has_finalizers = pod
                .metadata
                .finalizers
                .as_ref()
                .map(|f| !f.is_empty())
                .unwrap_or(false);
            if !has_finalizers {
                // Update status to terminal phase before removing from storage.
                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                    let original = p.clone();
                    if let Some(ref mut status) = p.status {
                        if status.phase != Some(Phase::Failed)
                            && status.phase != Some(Phase::Succeeded)
                        {
                            status.phase = Some(Phase::Succeeded);
                        }
                        // Refresh init container statuses — set ready=true for
                        // all completed init containers. When the pod reaches
                        // Succeeded, all init containers must have completed.
                        if let Some(ref mut ics) = status.init_container_statuses {
                            for ic in ics.iter_mut() {
                                if let Some(ContainerState {
                                    terminated: Some(ContainerStateTerminated { exit_code, .. }),
                                    ..
                                }) = &ic.state
                                {
                                    if *exit_code == 0 {
                                        ic.ready = true;
                                        ic.started = Some(true);
                                    }
                                } else {
                                    // Docker removed the container — mark as completed
                                    ic.state = Some(ContainerState {
                                        terminated: Some(ContainerStateTerminated {
                                            exit_code: 0,
                                            reason: Some("Completed".to_string()),
                                            message: None,
                                            started_at: None,
                                            finished_at: None,
                                            container_id: None,
                                            signal: None,
                                        }),
                                        ..Default::default()
                                    });
                                    ic.ready = true;
                                    ic.started = Some(true);
                                }
                            }
                        }
                    }
                    // Refresh container statuses
                    let fresh_statuses = self.get_container_statuses(&p).await.ok();
                    if let Some(ref mut status) = p.status {
                        if let Some(cs) = fresh_statuses {
                            status.container_statuses = Some(cs);
                        }
                    }
                    if !pod_status_equal(&original, &p) {
                        let _ = self.storage.update_status(&key, &p).await;
                    }
                }
                // Now that containers are stopped and the terminal status is
                // persisted, decide whether the object stays or is removed. The
                // rule lives in `finalize_terminated_pod_storage` so it can be
                // unit-tested against real storage; see that helper for why an
                // explicitly-deleted, finalizer-free pod must be removed.
                match finalize_terminated_pod_storage(
                    self.storage.as_ref(),
                    &key,
                    pod.metadata.deletion_timestamp.is_some(),
                    has_finalizers,
                )
                .await
                {
                    Ok(true) => {
                        // Defense in depth (#1157): a finalize that reports
                        // removal but leaves the object behind would make this
                        // worker re-enter TerminatingPod every reconcile and
                        // re-stop containers forever. Verify the delete took;
                        // if not, record a failure (drives the retry backoff at
                        // the needs_terminating gate) and escalate the log so a
                        // storage-delete regression is loud, not a silent loop.
                        if self.storage.get::<Pod>(&key).await.is_ok() {
                            let attempts = self.record_terminal_finalize_failure(&key);
                            if attempts >= TERMINAL_FINALIZE_ERROR_THRESHOLD {
                                error!(
                                    "Pod {}/{}: finalize reported removal but the object is still in storage after {} attempts — storage delete is not taking effect (regression of #1156?)",
                                    namespace, pod_name, attempts
                                );
                            } else {
                                warn!(
                                    "Pod {}/{}: finalize reported removal but the object is still in storage (attempt {}); backing off before retry",
                                    namespace, pod_name, attempts
                                );
                            }
                        } else {
                            self.clear_terminal_finalize_failure(&key);
                            debug!(
                                "Pod {}/{} removed from storage (deletionTimestamp set, no finalizers)",
                                namespace, pod_name
                            );
                        }
                    }
                    Ok(false) => {
                        debug!("Pod {}/{} marked terminal in storage", namespace, pod_name)
                    }
                    Err(e) => warn!(
                        "Pod {}/{}: failed to finalize storage state: {}",
                        namespace, pod_name, e
                    ),
                }
                // Pod is terminal — drop its CrashLoopBackOff state.
                self.forget_restart_backoff(namespace, pod_name);
            } else {
                // Pod has finalizers — update status to Failed but don't delete
                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                    let original = p.clone();
                    if let Some(ref mut status) = p.status {
                        if status.phase != Some(Phase::Failed)
                            && status.phase != Some(Phase::Succeeded)
                        {
                            status.phase = Some(Phase::Succeeded);
                        }
                    }
                    if !pod_status_equal(&original, &p) {
                        let _ = self.storage.update_status(&key, &p).await;
                    }
                }
            }
            // Volumes are cleaned by stop_pod_for during TerminatingPod.
            // Remove pod worker state — K8s HandlePodCleanups removes finished workers
            self.pod_states.lock().unwrap().remove(pod_uid);
            return Ok(());
        }

        // === TerminatingPod: stop containers, transition to TerminatedPod ===
        // K8s ref: pkg/kubelet/kubelet.go:2398 — SyncTerminatingPod
        // Stops all containers (with preStop hooks), does NOT remove them.
        // Container removal is handled by the container GC.
        if matches!(current_state, PodWorkerState::TerminatingPod) {
            info!(
                "Pod {}/{} terminating — stopping containers",
                namespace, pod_name
            );

            // A pod in the process of terminating is no longer Ready, and its
            // liveness probes are disabled (see check_liveness, which short-
            // circuits on a deletionTimestamp). Persist Ready=False /
            // ContainersReady=False *before* the (blocking) container stop so
            // watchers observe the readiness flip while the pod drains during
            // its grace period — upstream marks a terminating pod NotReady
            // (status_manager + the prober disabling probes on termination).
            // Conformance: "should mark readiness on pods to false … while …
            // terminating" waits for this via an informer.
            {
                let key = build_key("pods", Some(namespace), pod_name);
                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                    let original = p.clone();
                    if let Some(ref mut status) = p.status {
                        status.conditions = Some(Self::merge_pod_conditions(
                            status.conditions.as_deref().unwrap_or(&[]),
                            Self::not_ready_pod_conditions(),
                        ));
                        if let Some(ref mut cs) = status.container_statuses {
                            for c in cs.iter_mut() {
                                c.ready = false;
                            }
                        }
                        if let Some(ref mut ics) = status.init_container_statuses {
                            for ic in ics.iter_mut() {
                                ic.ready = false;
                            }
                        }
                    }
                    if !pod_status_equal(&original, &p) {
                        let _ = self.storage.update_status(&key, &p).await;
                    }
                }
            }

            let grace_period = pod
                .metadata
                .deletion_grace_period_seconds
                .or_else(|| {
                    pod.spec
                        .as_ref()
                        .and_then(|s| s.termination_grace_period_seconds)
                })
                .unwrap_or(30);

            // Stop the pod containers, executing preStop lifecycle hooks.
            // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go:849
            //
            // ONLY when the pod is actually being deleted (deletionTimestamp
            // set). A pod that reached a terminal phase ON ITS OWN
            // (Succeeded/Failed with restartPolicy != Always) has already had
            // every container exit — there is nothing left to stop. Crucially,
            // stop_pod_for also REMOVES the pod sandbox, which deletes the
            // exited containers and, with them, the CRI log files the api-server
            // serves `kubectl logs` / `GET pods/<p>/log` from
            // (resolve_container_id then returns "no container found"). Tearing
            // them down the instant the pod completes breaks log retrieval for
            // every run-to-completion pod — i.e. most output-checking
            // conformance tests. Leave the exited containers in place; the
            // container GC keeps one dead container per still-existing pod for
            // log access and reaps the rest, exactly like upstream
            // SyncTerminatingPod (which kills containers but never removes them).
            if pod.metadata.deletion_timestamp.is_some() {
                if let Err(e) = self.runtime.stop_pod_for(pod, grace_period).await {
                    warn!("Error stopping pod {}/{}: {}", namespace, pod_name, e);
                }
            }

            // Transition to TerminatedPod — containers are stopped.
            self.pod_states
                .lock()
                .unwrap()
                .insert(pod_uid.clone(), PodWorkerState::TerminatedPod);

            // Update pod status to terminal phase.
            // K8s kubelet NEVER deletes pods from the API server.
            let key = build_key("pods", Some(namespace), pod_name);
            if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                let original = p.clone();
                if let Some(ref mut status) = p.status {
                    if status.phase != Some(Phase::Failed) {
                        status.phase = Some(Phase::Succeeded);
                    }
                    Self::fixup_init_container_ready(status);
                }
                let fresh_statuses = self.get_container_statuses(&p).await.ok();
                if let Some(ref mut status) = p.status {
                    if let Some(cs) = fresh_statuses {
                        status.container_statuses = Some(cs);
                    }
                }
                if !pod_status_equal(&original, &p) {
                    let _ = self.storage.update_status(&key, &p).await;
                }
            }
            debug!(
                "Pod {}/{} terminated (containers stopped, left for GC)",
                namespace, pod_name
            );
            return Ok(());
        }

        // Check activeDeadlineSeconds — terminate pod if it has been running too long
        if let Some(ref spec) = pod.spec {
            if let Some(deadline) = spec.active_deadline_seconds {
                if let Some(ref status) = pod.status {
                    if let Some(elapsed) =
                        active_deadline_elapsed(Some(status), deadline, chrono::Utc::now())
                    {
                        info!(
                            "Pod {}/{} exceeded activeDeadlineSeconds ({}s >= {}s)",
                            namespace, pod_name, elapsed, deadline
                        );
                        let key = build_key("pods", Some(namespace), pod_name);
                        let mut failed_pod = pod.clone();
                        if let Some(ref mut s) = failed_pod.status {
                            s.phase = Some(Phase::Failed);
                            s.reason = Some(ACTIVE_DEADLINE_REASON.to_string());
                            s.message = Some(ACTIVE_DEADLINE_MESSAGE.to_string());
                            s.conditions = Some(Self::failed_pod_conditions());
                            if let Some(ref mut cs) = s.container_statuses {
                                for c in cs.iter_mut() {
                                    c.ready = false;
                                }
                            }
                            if let Some(ref mut ics) = s.init_container_statuses {
                                for ic in ics.iter_mut() {
                                    ic.ready = false;
                                }
                            }
                        }
                        let _ = self.storage.update_status(&key, &failed_pod).await;
                        // Stop the pod
                        if self.runtime.is_pod_running(pod).await.unwrap_or(false) {
                            let _ = self
                                .runtime
                                .stop_pod_with_grace_period(namespace, pod_name, 0)
                                .await;
                        }
                        return Ok(());
                    }
                }
            }
        }

        // Check current runtime status with timeout to prevent sync loop blocking.
        // If the pod was already Running and the Docker check times out, assume it's
        // still running — defaulting to "not running" causes the readiness path to be
        // skipped entirely, leaving pods stuck in not-Ready state.
        let was_running = matches!(
            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
            Some(Phase::Running)
        );
        let is_running = match tokio::time::timeout(
            std::time::Duration::from_secs(15),
            self.runtime.is_pod_running(pod),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                if was_running {
                    debug!(
                        "Timeout checking pod {}/{} runtime status, assuming still running",
                        namespace, pod_name
                    );
                    true
                } else {
                    warn!(
                        "Timeout checking pod {}/{} runtime status, assuming not running",
                        namespace, pod_name
                    );
                    false
                }
            }
        };

        // Get current phase from pod status
        let current_phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_ref())
            .unwrap_or(&Phase::Pending);

        // #1560: while a pod's plain (non-sidecar) init containers are unfinished,
        // drive it through the init state machine and hold it Pending — regardless
        // of the phase already recorded or whether an init container is momentarily
        // running. Upstream `getPhase` reports Pending for such a pod and
        // `computePodActions` recomputes init progress every sync, independent of
        // phase. Without this, a pod that briefly shows a running init container
        // flips to Running (the `Pending if is_running` arm) and its failing init
        // container is never retried again. Cheap short-circuit for the common
        // (no-init) case so we don't add a CRI round-trip per sync.
        let has_plain_init = pod
            .spec
            .as_ref()
            .and_then(|s| s.init_containers.as_ref())
            .is_some_and(|ic| {
                ic.iter()
                    .any(|c| c.restart_policy.as_deref() != Some("Always"))
            });
        let init_incomplete = has_plain_init
            && !matches!(current_phase, Phase::Succeeded | Phase::Failed)
            && self.runtime.has_sandbox(pod).await
            && !self.runtime.compute_init_container_actions(pod).await.0;
        let current_phase = if init_incomplete {
            &Phase::Pending
        } else {
            current_phase
        };
        let is_running = if init_incomplete { false } else { is_running };

        // K8s kubelet admission: reject a pod whose declared OS does not match
        // this node's OS. Our nodes are Linux, so a pod with spec.os.name set to
        // anything but "linux" is rejected with Phase=Failed and reason
        // PodOSNotSupported. K8s ref: pkg/kubelet/kubelet_pods.go — the PodOS
        // admit handler (GetPodOSValidationError / "PodOSNotSupported").
        if matches!(current_phase, Phase::Pending) && !is_running {
            if let Some(os) = pod.spec.as_ref().and_then(|s| s.os.as_ref()) {
                if os.name != "linux" {
                    info!(
                        "Pod {}/{} rejected: pod OS {:?} not supported on this (linux) node",
                        namespace, pod_name, os.name
                    );
                    let key = build_key("pods", Some(namespace), pod_name);
                    if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                        if let Some(ref mut status) = p.status {
                            status.phase = Some(Phase::Failed);
                            status.reason = Some("PodOSNotSupported".to_string());
                            status.message = Some(
                                "Pod was rejected as the node does not support the requested pod OS"
                                    .to_string(),
                            );
                        }
                        let _ = self.storage.update_status(&key, &p).await;
                    }
                    return Ok(());
                }
            }
        }

        // K8s kubelet admission: reject a pod requesting a forbidden sysctl. A
        // sysctl is allowed only if it is safe (namespaced + isolated) or
        // explicitly permitted via --allowed-unsafe-sysctls; otherwise the pod
        // is rejected with Phase=Failed, reason=SysctlForbidden (without ever
        // creating containers). K8s ref: pkg/kubelet/sysctl/allowlist.go::Admit.
        if matches!(current_phase, Phase::Pending) && !is_running {
            if let Err(message) = self.sysctl_allowlist.admit(pod) {
                info!("Pod {}/{} rejected: {}", namespace, pod_name, message);
                let key = build_key("pods", Some(namespace), pod_name);
                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                    if let Some(ref mut status) = p.status {
                        status.phase = Some(Phase::Failed);
                        status.reason = Some(crate::sysctl::FORBIDDEN_REASON.to_string());
                        status.message = Some(message);
                    }
                    let _ = self.storage.update_status(&key, &p).await;
                }
                return Ok(());
            }
        }

        // K8s kubelet admission: check hostPort conflicts before starting the pod.
        // K8s ref: pkg/kubelet/kubelet.go:2752 — allocationManager.AddPod, with
        // the conflict rule in pkg/scheduler/framework/types.go
        // (HostPortInfo.CheckConflict). If a pod's hostPorts conflict with an
        // already-active pod on this node, reject it with Phase=Failed; the
        // owning controller (StatefulSet, etc.) then deletes and recreates it.
        // The pure rule + its tests live in `crate::host_port`.
        if matches!(current_phase, Phase::Pending)
            && !is_running
            && !crate::host_port::host_ports(pod).is_empty()
        {
            let all_pods_prefix = build_prefix("pods", None);
            let all_pods: Vec<Pod> = self
                .storage
                .list(&all_pods_prefix)
                .await
                .unwrap_or_default();
            if let Some(conflict) =
                crate::host_port::find_host_port_conflict(pod, &all_pods, &self.node_name)
            {
                info!(
                    "Pod {}/{} rejected: hostPort {}/{} conflicts with pod {}",
                    namespace, pod_name, conflict.port, conflict.protocol, conflict.conflicting_pod
                );
                let key = build_key("pods", Some(namespace), pod_name);
                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                    if let Some(ref mut status) = p.status {
                        status.phase = Some(Phase::Failed);
                        status.reason = Some("HostPortConflict".to_string());
                        status.message = Some(conflict.message());
                    }
                    let _ = self.storage.update_status(&key, &p).await;
                }
                return Ok(());
            }
        }
        // Stale-read guard: re-read the pod from storage to detect a concurrent
        // phase update (e.g., admission controller sets Phase::Failed between
        // the worker's read and now).  If the phase is terminal, refuse any
        // container-create or container-stop operations and let the next sync
        // cycle pick up the terminal state via `needs_terminating`.
        // K8s equivalent: generateAPIPodStatus forces terminal phases sticky
        // (pkg/kubelet/kubelet_pods.go:1934-1942).
        {
            let key = build_key("pods", Some(namespace), pod_name);
            if let Ok(fresh) = self.storage.get::<Pod>(&key).await {
                let fresh_phase = fresh.status.as_ref().and_then(|s| s.phase.as_ref());
                if phase_is_terminal(fresh_phase) && !phase_is_terminal(Some(current_phase)) {
                    warn!(
                        "Pod {}/{} became terminal while syncing (was {:?}, storage now {:?}) — \
                         skipping container operations",
                        namespace, pod_name, current_phase, fresh_phase
                    );
                    return Ok(());
                }
            }
        }

        match current_phase {
            // If pod is Pending and has been scheduled to this node, start it
            Phase::Pending if !is_running => {
                // Don't overwrite error status — the test needs to observe it.
                // For ErrImagePull, skip retry for this sync cycle to prevent
                // blocking the sync loop with repeated pull failures.
                // K8s uses exponential backoff for image pulls.
                let already_has_error = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.reason.as_deref())
                    .is_some_and(|r| {
                        r == "CreateContainerError"
                            || r == "CreateContainerConfigError"
                            || r == "ErrImagePull"
                            || r == "ErrImageNeverPull"
                            || r == "ImagePullBackOff"
                    });

                if !already_has_error {
                    let has_init_containers = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.init_containers.as_ref())
                        .is_some_and(|ic| {
                            ic.iter()
                                .any(|c| c.restart_policy.as_deref() != Some("Always"))
                        });

                    // For pods with init containers, use the state machine approach.
                    // K8s ref: pkg/kubelet/kuberuntime/kuberuntime_container.go — computeInitContainerActions
                    // Check if the pod sandbox has been created.
                    let sandbox_exists = self.runtime.has_sandbox(pod).await;

                    if has_init_containers && sandbox_exists {
                        // Pod sandbox exists — check init container progress
                        let (all_done, next_idx, should_retry) =
                            self.runtime.compute_init_container_actions(pod).await;

                        if all_done {
                            // All init containers done — start_pod will skip init and start app containers
                            info!(
                                "All init containers completed for pod {}/{}, starting app containers",
                                namespace, pod_name
                            );
                        } else if let Some(idx) = next_idx {
                            let init_containers =
                                pod.spec.as_ref().unwrap().init_containers.as_ref().unwrap();
                            let ic = &init_containers[idx];

                            if should_retry {
                                // Init container failed — update status and return.
                                // The next sync cycle will retry (with implicit backoff from sync interval).
                                debug!(
                                    "Init container {} failed for pod {}/{}, will retry next sync",
                                    ic.name, namespace, pod_name
                                );
                                // CrashLoopBackOff gate + monotonic restartCount for
                                // the failing init container, mirroring
                                // reconcile_container_restarts for app containers. The
                                // CRI attempt is reset by the remove/recreate, so the
                                // backoff map is the source of truth for the reported
                                // count — the "should not start app containers if init
                                // containers fail on a RestartAlways pod" spec asserts
                                // the init container's restartCount climbs to >= 3. The
                                // gate also spaces retries out (was a hot ~sync-interval
                                // loop with no backoff).
                                let bkey = format!("{}/{}/{}", namespace, pod_name, ic.name);
                                let now = Instant::now();
                                let (do_restart, init_restart_count) = {
                                    let mut map = self.restart_backoff.lock().unwrap();
                                    match map.get_mut(&bkey) {
                                        None => {
                                            map.insert(
                                                bkey.clone(),
                                                RestartBackoff {
                                                    restart_count: 1,
                                                    last_restart: now,
                                                    backoff: self.crash_loop_backoff_initial(),
                                                },
                                            );
                                            (true, 1)
                                        }
                                        Some(entry) => {
                                            if now.duration_since(entry.last_restart)
                                                >= entry.backoff
                                            {
                                                entry.restart_count += 1;
                                                entry.last_restart = now;
                                                entry.backoff = (entry.backoff * 2)
                                                    .min(self.crash_loop_backoff_max);
                                                (true, entry.restart_count)
                                            } else {
                                                (false, entry.restart_count)
                                            }
                                        }
                                    }
                                };
                                // Remove the exited container (→ recreate next sync) only
                                // once the backoff window elapses.
                                if do_restart {
                                    let _ = self
                                        .runtime
                                        .remove_terminated_container(&pod.metadata.uid, &ic.name)
                                        .await;
                                }
                                // Update status with CrashLoopBackOff AND make sure
                                // the PodInitialized=False condition + app-container
                                // Waiting/PodInitializing statuses are present.
                                //
                                // K8s conformance ("should not start app containers if
                                // init containers fail on a RestartAlways pod") asserts:
                                //   - pod.status.phase == Pending
                                //   - pod.conditions[Initialized].reason ==
                                //         "ContainersNotInitialized"
                                //   - pod.conditions[Initialized].message ==
                                //         "containers with incomplete status: [<names>]"
                                //   - every container_status.state.waiting.reason ==
                                //         "PodInitializing"
                                //
                                // start_pod's failure handler sets these on the first
                                // sync, but every subsequent retry that re-enters via
                                // should_retry must also assert them so a partial
                                // status overwrite from any concurrent writer can't
                                // strip the Initialized condition or leak an app
                                // container into a non-Waiting state.
                                // K8s ref: pkg/kubelet/status/generate.go —
                                //          GeneratePodInitializedCondition
                                let key = build_key("pods", Some(namespace), pod_name);
                                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                                    let mut init_statuses =
                                        self.get_init_container_statuses(&p).await;
                                    // Overlay the monotonic count from the backoff map
                                    // onto the failing init container — the recreate
                                    // resets the CRI attempt to 0, so this is the only
                                    // record that survives across retries.
                                    if let Some(list) = init_statuses.as_mut() {
                                        if let Some(st) =
                                            list.iter_mut().find(|s| s.name == ic.name)
                                        {
                                            st.restart_count =
                                                st.restart_count.max(init_restart_count);
                                            // Report the crash as CrashLoopBackOff with the
                                            // prior termination moved into lastState — how
                                            // upstream surfaces a backing-off container. The
                                            // spec's watch waits for the init container's
                                            // LastTerminationState to be set before it checks
                                            // restartCount, so a plain Terminated status (no
                                            // lastState) would hang the test.
                                            if matches!(
                                                st.state,
                                                Some(ContainerState {
                                                    terminated: Some(
                                                        ContainerStateTerminated { .. }
                                                    ),
                                                    ..
                                                })
                                            ) {
                                                st.last_state = st.state.take();
                                                st.state = Some(ContainerState {
                                                    waiting: Some(ContainerStateWaiting {
                                                        reason: Some(
                                                            "CrashLoopBackOff".to_string(),
                                                        ),
                                                        message: Some(
                                                            "back-off restarting failed container"
                                                                .to_string(),
                                                        ),
                                                    }),
                                                    ..Default::default()
                                                });
                                                st.ready = false;
                                                st.started = Some(false);
                                            }
                                        }
                                    }
                                    let qos = Self::compute_qos_class(&p);
                                    p.status = Some(
                                        self.init_failure_status(
                                            &p,
                                            init_statuses,
                                            Phase::Pending,
                                            "PodInitializing",
                                            Some(qos),
                                        )
                                        .await,
                                    );
                                    let _ = self.storage.update_status(&key, &p).await;
                                }
                                return Ok(());
                            } else {
                                // Need to start this init container
                                info!(
                                    "Starting init container {} (index {}) for pod {}/{}",
                                    ic.name, idx, namespace, pod_name
                                );
                                // Ensure image is available before starting
                                if let Err(e) = self
                                    .runtime
                                    .ensure_image(
                                        &ic.image,
                                        ic.image_pull_policy.as_deref(),
                                        Some((pod, ic.name.as_str())),
                                    )
                                    .await
                                {
                                    warn!(
                                        "Failed to pull image for init container {}: {}",
                                        ic.name, e
                                    );
                                    return Ok(());
                                }
                                // Reuse what the volume manager provisions rather
                                // than re-deriving a path per plugin: a hostPath
                                // volume's path is the host path itself, not a
                                // dir under the pod dir (#1983). Upstream records
                                // the mounted path per volume and the runtime
                                // reads that record
                                // (`GetMountedVolumesForPod`,
                                // `pkg/kubelet/volumemanager/volume_manager.go:320`,
                                // consumed at `pkg/kubelet/kubelet_pods.go:643`).
                                // `create_pod_volumes` is idempotent, as in
                                // `reconcile_container_restarts`.
                                let volume_paths = self
                                    .runtime
                                    .create_pod_volumes(pod)
                                    .await
                                    .unwrap_or_default();
                                let pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.as_deref());
                                if let Err(e) = self
                                    .runtime
                                    .start_container(pod, ic, &volume_paths, None, None, pod_ip)
                                    .await
                                {
                                    warn!(
                                        "Failed to start init container {} for {}/{}: {}",
                                        ic.name, namespace, pod_name, e
                                    );
                                }
                                // Update status
                                let init_statuses = self.get_init_container_statuses(pod).await;
                                let key = build_key("pods", Some(namespace), pod_name);
                                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                                    if let Some(ref mut s) = p.status {
                                        s.init_container_statuses = init_statuses;
                                        s.reason = Some("PodInitializing".to_string());
                                    }
                                    let _ = self.storage.update_status(&key, &p).await;
                                }
                                return Ok(());
                            }
                        } else {
                            // No next init container and not all done. When a
                            // RestartNever init container failed, publish the
                            // terminal Failed status instead of silently
                            // returning with no conditions. Otherwise this is
                            // just the "wait for a running init" case.
                            let key = build_key("pods", Some(namespace), pod_name);
                            if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                                let init_statuses = self.get_init_container_statuses(&p).await;
                                if init_container_failed_terminally(&p, init_statuses.as_deref()) {
                                    let qos = Self::compute_qos_class(&p);
                                    p.status = Some(
                                        self.init_failure_status(
                                            &p,
                                            init_statuses,
                                            Phase::Failed,
                                            "FailedToStart",
                                            Some(qos),
                                        )
                                        .await,
                                    );
                                    let _ = self.storage.update_status(&key, &p).await;
                                }
                            }
                            return Ok(());
                        }
                    }

                    info!("Starting pod: {}/{}", namespace, pod_name);
                    let reason = if has_init_containers && !sandbox_exists {
                        "PodInitializing"
                    } else {
                        "ContainerCreating"
                    };
                    self.update_pod_status(pod, Phase::Pending, Some(reason), None)
                        .await?;
                } else {
                    debug!(
                        "Pod {}/{} already has CreateContainer(Config)Error, retrying without status reset",
                        namespace, pod_name
                    );
                }

                // Start the pod with a timeout generous enough for a cold image
                // pull — see POD_START_TIMEOUT (#1050). Too short a cap cancels
                // RunPodSandbox mid-pull and orphans the reserved sandbox name.
                match tokio::time::timeout(POD_START_TIMEOUT, self.runtime.start_pod(pod)).await {
                    Err(_timeout) => {
                        warn!(
                            "Timeout starting pod {}/{}, will retry",
                            namespace, pod_name
                        );
                        return Ok(());
                    }
                    Ok(result) => match result {
                        Ok(_) => {
                            info!("Pod {}/{} started successfully", namespace, pod_name);

                            // Re-fetch the pod from etcd to get the latest resourceVersion.
                            // Between start_pod being called and now, the admission controller or
                            // another writer may have incremented the resourceVersion (e.g. injecting
                            // service account tokens). Using a stale resourceVersion causes an
                            // optimistic-concurrency conflict that silently leaves the pod in Pending,
                            // which causes sonobuoy-worker and similar clients to mis-detect that
                            // all containers have already finished.
                            let key = build_key("pods", Some(namespace), pod_name);
                            let fresh_pod: Pod = match self.storage.get(&key).await {
                                Ok(p) => p,
                                _ => pod.clone(),
                            };

                            // Terminal phases are sticky: if the pod already
                            // reached Succeeded/Failed in storage, don't write
                            // the Running status below. This subsumes the older
                            // SysctlForbidden-specific guard (start_pod may have
                            // rejected the pod during admission, e.g. an unsafe
                            // sysctl -> phase=Failed, reason=SysctlForbidden,
                            // without creating any containers) and also blocks a
                            // genuine Succeeded->Running flap that would let the
                            // job controller delete a completed pod (#1048).
                            // Upstream parity: kubelet_pods.go:1934-1942
                            // generateAPIPodStatus ("pods are not allowed to
                            // transition out of terminal phases").
                            if should_skip_phase_write(
                                fresh_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                                &Phase::Running,
                            ) {
                                debug!(
                                    "Pod {}/{} already terminal in storage ({:?}); not overwriting with Running",
                                    namespace,
                                    pod_name,
                                    fresh_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                                );
                                return Ok(());
                            }

                            // Get container statuses and pod IP
                            // Statuses are read AFTER the bounded IP wait (#2771):
                            // upstream generates the whole status at write time.
                            let (container_statuses, pod_ip) = crate::poll::observe_running_status(
                                || async { self.get_container_statuses(&fresh_pod).await.ok() },
                                || async {
                                    self.runtime.get_pod_ip(&fresh_pod).await.ok().flatten()
                                },
                                std::time::Duration::from_secs(10),
                                std::time::Duration::from_millis(150),
                            )
                            .await;
                            let pod_i_ps = pod_ip.as_ref().map(|ip| vec![PodIP { ip: ip.clone() }]);

                            // Write Running status using the fresh resourceVersion
                            let mut new_pod = fresh_pod;
                            let qos = Self::compute_qos_class(&new_pod);
                            let init_container_statuses =
                                self.get_init_container_statuses(&new_pod).await;

                            // If any container has a readiness probe, start as not-ready
                            // and let the probe check in the sync loop update Ready to True.
                            let has_readiness_probe = new_pod
                                .spec
                                .as_ref()
                                .map(spec_has_readiness_probe)
                                .unwrap_or(false);
                            let conditions = if has_readiness_probe {
                                Self::not_ready_pod_conditions()
                            } else if !readiness_gates_satisfied(&new_pod) {
                                // Containers are ready, but an unsatisfied
                                // readinessGate must hold Ready=False (upstream
                                // GeneratePodReadyCondition). ContainersReady stays
                                // True; the gate condition arrives later via the
                                // status subresource and the reconcile loop flips
                                // Ready to True.
                                Self::pod_readiness_conditions(true, false)
                            } else {
                                Self::running_pod_conditions()
                            };

                            let ephemeral_container_statuses = self
                                .runtime
                                .get_ephemeral_container_statuses(&new_pod)
                                .await;

                            // generateAPIPodStatus (kubelet_pods.go:1889), see pod_status.rs.
                            let start_time = preserved_start_time(
                                new_pod.status.as_ref().and_then(|s| s.start_time),
                                chrono::Utc::now(),
                            );
                            let (host_ip, host_ips) = self.host_ip_fields();
                            new_pod.status = Some(crate::pod_status::generate_api_pod_status(
                                &new_pod,
                                crate::pod_status::ApiPodStatusInput {
                                    phase: Phase::Running,
                                    message: Some("All containers started".to_string()),
                                    reason: None,
                                    host_ip,
                                    host_ips,
                                    pod_ip,
                                    pod_ips: pod_i_ps,
                                    conditions,
                                    container_statuses,
                                    init_container_statuses,
                                    ephemeral_container_statuses,
                                    qos_class: Some(qos),
                                    start_time,
                                },
                            ));
                            self.apply_host_ips_to_pod(&mut new_pod).await;

                            if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                                // Retry with fresh read on conflict (K8s pattern)
                                if crate::lifecycle::is_status_write_conflict(&e) {
                                    if let Ok(fresh_pod) = self.storage.get::<Pod>(&key).await {
                                        // Terminal phases are sticky — don't
                                        // regress a Succeeded/Failed pod to
                                        // Running on the conflict retry.
                                        if should_skip_phase_write(
                                            fresh_pod
                                                .status
                                                .as_ref()
                                                .and_then(|s| s.phase.as_ref()),
                                            &Phase::Running,
                                        ) {
                                            return Ok(());
                                        }
                                        let mut retry_pod = fresh_pod;
                                        // observedGeneration tracks the generation of
                                        // the re-read object, not the value computed
                                        // from the pre-conflict read — a concurrent
                                        // spec update is what bumped generation and
                                        // caused this conflict (#1170). Capture before
                                        // the mut borrow below.
                                        let retry_gen = retry_pod.metadata.generation;
                                        // Re-fetch ALL statuses for the retry — stale
                                        // init_container_statuses from an intermediate
                                        // write may have ready=false for completed inits.
                                        // K8s prober_manager.UpdatePodStatus always sets
                                        // ready=true for terminated init containers.
                                        let fresh_init_statuses =
                                            self.get_init_container_statuses(pod).await;
                                        let fresh_container_statuses =
                                            self.get_container_statuses(pod).await.ok();
                                        if let Some(ref mut status) = retry_pod.status {
                                            status.phase = Some(Phase::Running);
                                            status.message =
                                                Some("All containers ready".to_string());
                                            status.init_container_statuses = fresh_init_statuses;
                                            status.observed_generation = retry_gen;
                                            if let Some(cs) = fresh_container_statuses {
                                                status.container_statuses = Some(cs);
                                            }
                                        }
                                        self.apply_host_ips_to_pod(&mut retry_pod).await;
                                        if let Err(e2) =
                                            self.storage.update_status(&key, &retry_pod).await
                                        {
                                            warn!(
                                                "Failed to update pod {}/{} status to Running after retry: {}",
                                                namespace, pod_name, e2
                                            );
                                        }
                                    }
                                } else {
                                    warn!(
                                        "Failed to update pod {}/{} status to Running: {}",
                                        namespace, pod_name, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            let err_msg = e.to_string();

                            // K8s retries volume mounting when a volume source
                            // isn't ready yet (Secret/ConfigMap not created, or a
                            // PersistentVolumeClaim that doesn't exist / isn't
                            // bound — #1096). The pod stays Pending with containers
                            // in Waiting{ContainerCreating}; syncPod returns early
                            // without creating any containers and the pod worker
                            // retries on the next sync cycle.
                            // K8s ref: pkg/kubelet/kubelet.go:2204 — WaitForAttachAndMount
                            //          pkg/kubelet/kubelet_pods.go:2496 — defaultWaitingState
                            if crate::lifecycle::is_volume_wait_error(&e) {
                                warn!(
                                    "Pod {}/{} waiting for volume (will retry): {}",
                                    namespace, pod_name, err_msg
                                );
                                let key = build_key("pods", Some(namespace), pod_name);
                                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                                    // Set container statuses to Waiting{ContainerCreating}
                                    let container_statuses: Vec<ContainerStatus> = p
                                        .spec
                                        .as_ref()
                                        .map(|s| {
                                            s.containers
                                                .iter()
                                                .map(|c| ContainerStatus {
                                                    name: c.name.clone(),
                                                    ready: false,
                                                    restart_count: 0,
                                                    state: Some(ContainerState {
                                                        waiting: Some(ContainerStateWaiting {
                                                            reason: Some(
                                                                "ContainerCreating".to_string(),
                                                            ),
                                                            message: None,
                                                        }),
                                                        ..Default::default()
                                                    }),
                                                    last_state: None,
                                                    image: Some(c.image.clone()),
                                                    image_id: None,
                                                    container_id: None,
                                                    started: Some(false),
                                                    allocated_resources: None,
                                                    allocated_resources_status: None,
                                                    resources: None,
                                                    user: None,
                                                    volume_mounts: None,
                                                    stop_signal: None,
                                                })
                                                .collect()
                                        })
                                        .unwrap_or_default();
                                    if let Some(ref mut status) = p.status {
                                        status.phase = Some(Phase::Pending);
                                        status.container_statuses = Some(container_statuses);
                                        status.conditions = Some(Self::not_ready_pod_conditions());
                                    }
                                    let _ = self.storage.update_status(&key, &p).await;
                                }
                                return Ok(());
                            }

                            error!(
                                "Failed to start pod {}/{}: {}",
                                namespace, pod_name, err_msg
                            );

                            // Determine the error reason matching K8s container status reasons.
                            // Prefers typed downcast over substring sniffing — see upstream
                            // `pkg/kubelet/images/types.go` (sentinel errors) and
                            // `pkg/kubelet/kuberuntime/kuberuntime_container.go`.
                            let create_error_reason =
                                crate::lifecycle::reason_from_anyhow(&e).map(str::to_string);

                            if let Some(reason) = create_error_reason {
                                // Container creation/config error — pod stays Pending with
                                // container in Waiting state with appropriate reason.
                                let key = build_key("pods", Some(namespace), pod_name);
                                let fresh_pod: Pod = match self.storage.get(&key).await {
                                    Ok(p) => p,
                                    _ => pod.clone(),
                                };
                                let mut new_pod = fresh_pod;

                                // Build container statuses with the failed container
                                let container_statuses: Option<Vec<ContainerStatus>> =
                                    new_pod.spec.as_ref().map(|spec| {
                                        spec.containers
                                            .iter()
                                            .map(|c| ContainerStatus {
                                                name: c.name.clone(),
                                                ready: false,
                                                restart_count: 0,
                                                state: Some(ContainerState {
                                                    waiting: Some(ContainerStateWaiting {
                                                        reason: Some(reason.clone()),
                                                        message: Some(err_msg.clone()),
                                                    }),
                                                    ..Default::default()
                                                }),
                                                last_state: None,
                                                image: Some(c.image.clone()),
                                                image_id: None,
                                                container_id: None,
                                                started: Some(false),
                                                allocated_resources: c
                                                    .resources
                                                    .as_ref()
                                                    .and_then(|r| r.requests.clone()),
                                                allocated_resources_status: None,
                                                resources: c.resources.clone(),
                                                user: None,
                                                volume_mounts: None,
                                                stop_signal: None,
                                            })
                                            .collect()
                                    });

                                // Get init container statuses — they may have run before the error
                                let init_container_statuses =
                                    self.get_init_container_statuses(&new_pod).await;

                                let qos = Self::compute_qos_class(&new_pod);
                                new_pod.status = Some(self.create_error_pod_status(
                                    &new_pod,
                                    reason,
                                    err_msg,
                                    container_statuses,
                                    init_container_statuses,
                                    qos,
                                ));
                                self.apply_host_ips_to_pod(&mut new_pod).await;

                                if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                                    warn!(
                                        "Failed to update pod {}/{} status to container error: {}, retrying",
                                        namespace, pod_name, e
                                    );
                                    // CAS retry — re-read and apply status
                                    if let Ok(mut retry_pod) = self.storage.get::<Pod>(&key).await {
                                        retry_pod.status = new_pod.status.clone();
                                        let _ = self.storage.update_status(&key, &retry_pod).await;
                                    }
                                }
                            } else {
                                // Get init container statuses from Docker to capture
                                // actual exit codes for failed init containers
                                let key = build_key("pods", Some(namespace), pod_name);
                                let fresh_pod: Pod = match self.storage.get(&key).await {
                                    Ok(p) => p,
                                    _ => pod.clone(),
                                };
                                let init_container_statuses =
                                    self.get_init_container_statuses(&fresh_pod).await;
                                let qos = Self::compute_qos_class(&fresh_pod);
                                let mut new_pod = fresh_pod;

                                // We only reach start_pod (and thus this error handler)
                                // once every init container has completed — the init
                                // state machine above returns before start_pod is ever
                                // called while inits are pending/failing. So an
                                // unclassified error here is almost always an
                                // app-container / sandbox *creation* hiccup, NOT an init
                                // failure, and MUST NOT be reported as "Init container
                                // failed".
                                //
                                // Classify the error to decide the phase:
                                // - A genuinely terminal init failure (Never-policy init
                                //   container that exited non-zero) → Failed. Normally
                                //   surfaced by the state machine, kept here defensively.
                                // - A permanent host-port conflict → Failed: retrying
                                //   can never bind the port, so the pod is stuck.
                                //   K8s ref: pkg/kubelet/status/status_manager.go:629
                                // - Everything else (transient create/sandbox errors,
                                //   e.g. CRI contention) → stay Pending with containers
                                //   Waiting{ContainerCreating} and retry on the next sync
                                //   cycle. Upstream kubelet retries create errors in
                                //   syncPod regardless of restartPolicy; restartPolicy
                                //   Never only prevents restarting a container that has
                                //   already RUN and exited, not a creation hiccup.
                                //   K8s ref: pkg/kubelet/kubelet.go — syncPod retry;
                                //            pkg/kubelet/pod_workers.go — podWorkerLoop
                                let init_failed = init_container_failed_terminally(
                                    &new_pod,
                                    init_container_statuses.as_deref(),
                                );
                                let is_port_conflict = err_msg
                                    .contains("port is already allocated")
                                    || err_msg.contains("bind: address already in use");

                                if init_failed {
                                    new_pod.status = Some(
                                        self.init_failure_status(
                                            &new_pod,
                                            init_container_statuses,
                                            Phase::Failed,
                                            "FailedToStart",
                                            Some(qos),
                                        )
                                        .await,
                                    );
                                } else {
                                    self.apply_app_start_error_status(
                                        &mut new_pod,
                                        init_container_statuses,
                                        &err_msg,
                                        is_port_conflict,
                                        qos,
                                    )
                                    .await;
                                }

                                if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                                    warn!(
                                        "Failed to update pod {}/{} status after start error: {}",
                                        namespace, pod_name, e
                                    );
                                }
                            }
                        }
                    }, // end inner match result
                } // end outer match timeout
            }
            // If pod is Pending but containers are already running, update to Running.
            // If a container has CreateContainerError/CreateContainerConfigError, retry starting it first.
            Phase::Pending if is_running => {
                // Check if any container is in CreateContainerError or CreateContainerConfigError
                let has_create_error = pod.status.as_ref()
                    .and_then(|s| s.container_statuses.as_ref())
                    .is_some_and(|statuses| {
                        statuses.iter().any(|cs| {
                            matches!(&cs.state, Some(ContainerState { waiting: Some(ContainerStateWaiting { reason: Some(r), .. }), .. }) if r == "CreateContainerError" || r == "CreateContainerConfigError")
                        })
                    });

                let should_update_running = if has_create_error {
                    // Retry starting — spec/annotations may have changed
                    debug!(
                        "Pod {}/{} has container creation error, retrying start",
                        namespace, pod_name
                    );
                    match self.runtime.start_pod(pod).await {
                        Ok(_) => {
                            info!("Pod {}/{} retry succeeded", namespace, pod_name);
                            true // Now update to Running
                        }
                        Err(e) => {
                            debug!("Pod {}/{} retry still failing: {}", namespace, pod_name, e);
                            false // Stay in error state
                        }
                    }
                } else {
                    true // No error, proceed to Running
                };

                if should_update_running {
                    debug!(
                        "Pod {}/{} containers are running, updating status to Running",
                        namespace, pod_name
                    );

                    let key = build_key("pods", Some(namespace), pod_name);
                    let fresh_pod: Pod = match self.storage.get(&key).await {
                        Ok(p) => p,
                        _ => pod.clone(),
                    };

                    // Terminal phases are sticky — if a concurrent reconcile
                    // already set Succeeded/Failed in storage, don't regress to
                    // Running. Upstream parity: kubelet_pods.go:1934-1942
                    // generateAPIPodStatus ("pods are not allowed to
                    // transition out of terminal phases").
                    if should_skip_phase_write(
                        fresh_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                        &Phase::Running,
                    ) {
                        debug!(
                            "Pod {}/{} already terminal in storage ({:?}); not overwriting with Running",
                            namespace,
                            pod_name,
                            fresh_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                        );
                        return Ok(());
                    }

                    // Statuses are read AFTER the bounded IP wait (#2771).
                    let (container_statuses, pod_ip) = crate::poll::observe_running_status(
                        || async { self.get_container_statuses(&fresh_pod).await.ok() },
                        || async { self.runtime.get_pod_ip(&fresh_pod).await.ok().flatten() },
                        std::time::Duration::from_secs(10),
                        std::time::Duration::from_millis(150),
                    )
                    .await;
                    let pod_i_ps = pod_ip.as_ref().map(|ip| vec![PodIP { ip: ip.clone() }]);

                    // Update status to Running
                    let mut new_pod = fresh_pod;
                    let qos = Self::compute_qos_class(&new_pod);
                    let init_container_statuses = self.get_init_container_statuses(&new_pod).await;

                    let has_readiness_probe = new_pod
                        .spec
                        .as_ref()
                        .map(spec_has_readiness_probe)
                        .unwrap_or(false);
                    let conditions = if has_readiness_probe {
                        Self::not_ready_pod_conditions()
                    } else if !readiness_gates_satisfied(&new_pod) {
                        // Containers are ready, but an unsatisfied readinessGate
                        // must hold Ready=False (upstream GeneratePodReadyCondition).
                        // ContainersReady stays True; the gate condition arrives
                        // later via the status subresource and the reconcile loop
                        // flips Ready to True.
                        Self::pod_readiness_conditions(true, false)
                    } else {
                        Self::running_pod_conditions()
                    };

                    let ephemeral_container_statuses = self
                        .runtime
                        .get_ephemeral_container_statuses(&new_pod)
                        .await;

                    // generateAPIPodStatus (kubelet_pods.go:1889), see pod_status.rs.
                    let start_time = preserved_start_time(
                        new_pod.status.as_ref().and_then(|s| s.start_time),
                        chrono::Utc::now(),
                    );
                    let (host_ip, host_ips) = self.host_ip_fields();
                    new_pod.status = Some(crate::pod_status::generate_api_pod_status(
                        &new_pod,
                        crate::pod_status::ApiPodStatusInput {
                            phase: Phase::Running,
                            message: Some("All containers started".to_string()),
                            reason: None,
                            host_ip,
                            host_ips,
                            pod_ip,
                            pod_ips: pod_i_ps,
                            conditions,
                            container_statuses,
                            init_container_statuses,
                            ephemeral_container_statuses,
                            qos_class: Some(qos),
                            start_time,
                        },
                    ));
                    self.apply_host_ips_to_pod(&mut new_pod).await;

                    // Use non-fatal update: if the write fails (e.g., concurrency conflict),
                    // the next sync will retry via the Pending+is_running path.
                    // Do NOT propagate the error — that causes update_pod_status_error to
                    // set the pod to Failed, which is unrecoverable.
                    if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                        warn!(
                            "Failed to update pod {}/{} to Running (will retry): {}",
                            namespace, pod_name, e
                        );
                    }
                } // end if should_update_running
            }
            Phase::Running if is_running => {
                debug!("Pod {}/{} is running, checking health", namespace, pod_name);

                // Re-read from storage when resize is pending to get the latest spec
                // (the API server may have updated resources since our list() call).
                let key = build_key("pods", Some(namespace), pod_name);
                let resize_status = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.resize.as_deref())
                    .unwrap_or("");
                let fresh_pod = if resize_status == "Proposed" || resize_status == "InProgress" {
                    // Re-read to get fresh spec with updated resources
                    self.storage
                        .get::<Pod>(&key)
                        .await
                        .unwrap_or_else(|_| pod.clone())
                } else {
                    pod.clone()
                };

                // Re-check resize status from fresh pod (may differ from list-fetched pod)
                let resize_status = fresh_pod
                    .status
                    .as_ref()
                    .and_then(|s| s.resize.as_deref())
                    .unwrap_or("");

                // Handle in-place pod resize (KEP-1287):
                // Flow: API sets resize="Proposed" -> kubelet sets "InProgress" ->
                // applies resources -> sets resize="" with updated allocatedResources
                if resize_status == "Proposed" || resize_status == "InProgress" {
                    // Set resize to InProgress if it was Proposed
                    if resize_status == "Proposed" {
                        let rkey = build_key("pods", Some(namespace), pod_name);
                        if let Ok(mut rpod) = self.storage.get::<Pod>(&rkey).await {
                            if let Some(ref mut status) = rpod.status {
                                status.resize = Some("InProgress".to_string());
                            }
                            let _ = self.storage.update_status(&rkey, &rpod).await;
                        }
                    }

                    // Apply resource changes to containers
                    let mut all_resized = true;
                    if let Some(spec) = &fresh_pod.spec {
                        for container in &spec.containers {
                            if container.resources.is_some() {
                                let container_name = format!("{}_{}", pod_name, container.name);
                                {
                                    // The cgroup values are derived by
                                    // `translate::linux_resources`, the same
                                    // helper the create path uses. A hand-rolled
                                    // copy here skipped upstream's
                                    // `MinQuotaPeriod` floor and crun rejected
                                    // sub-10m CPU limits with
                                    // "write to `cpu.max`: Invalid argument".
                                    match self
                                        .runtime
                                        .update_container_resources(
                                            &fresh_pod.metadata.uid,
                                            container,
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            info!(
                                                "Updated container {} resources (resize)",
                                                container_name
                                            );
                                        }
                                        Err(e) => {
                                            // A resize that never reached the
                                            // runtime must be loud: the pod
                                            // stays wedged in InProgress and
                                            // status keeps reporting the old
                                            // cgroup values.
                                            warn!(
                                                "Failed to update container {} resources: {}",
                                                container_name, e
                                            );
                                            all_resized = false;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Mark resize as complete and update allocatedResources
                    if all_resized {
                        let rkey = build_key("pods", Some(namespace), pod_name);
                        if let Ok(mut rpod) = self.storage.get::<Pod>(&rkey).await {
                            if let Some(ref mut status) = rpod.status {
                                status.resize = Some(String::new()); // Empty = resize complete
                                                                     // Update allocatedResources in container statuses
                                if let Some(ref spec) = rpod.spec.clone() {
                                    if let Some(ref mut cs_list) = status.container_statuses {
                                        for cs in cs_list.iter_mut() {
                                            if let Some(c) =
                                                spec.containers.iter().find(|c| c.name == cs.name)
                                            {
                                                if let Some(ref res) = c.resources {
                                                    cs.allocated_resources = res
                                                        .requests
                                                        .clone()
                                                        .or_else(|| res.limits.clone());
                                                    cs.resources = Some(res.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            let _ = self.storage.update_status(&rkey, &rpod).await;
                        }
                    }
                }

                // Use fresh_pod for all subsequent checks (spec may have been updated by resize PATCH)
                let pod = &fresh_pod;

                // Refresh Secret/ConfigMap volumes so updates are reflected in running pods
                if let Err(e) = self.runtime.refresh_volumes(pod).await {
                    debug!(
                        "Failed to refresh volumes for pod {}/{}: {}",
                        namespace, pod_name, e
                    );
                }

                // K8s computePodActions: check if any spec containers are MISSING from the
                // runtime and need to be (re)created. This happens when a container was never
                // started (e.g., after a StatefulSet PATCH recreates the pod) or was removed.
                if let Some(ref spec) = pod.spec {
                    for container in &spec.containers {
                        if !self
                            .runtime
                            .container_exists(&pod.metadata.uid, &container.name)
                            .await
                        {
                            info!(
                                "Container {} missing for running pod {}/{}, creating",
                                container.name, namespace, pod_name
                            );
                            let empty_binds = std::collections::HashMap::new();
                            if let Err(e) = self
                                .runtime
                                .start_container(
                                    pod,
                                    container,
                                    &empty_binds,
                                    None, // netns — pod already has networking via pause
                                    None, // hosts file
                                    pod.status.as_ref().and_then(|s| s.pod_ip.as_deref()),
                                )
                                .await
                            {
                                warn!(
                                    "Failed to create missing container {} for pod {}/{}: {}",
                                    container.name, namespace, pod_name, e
                                );
                            }
                        }
                    }
                }

                // Check if all spec containers have terminated (pause container may still be running).
                // This must happen before liveness probes, which may error on exited containers.
                {
                    let restart_policy = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.restart_policy.as_deref())
                        .unwrap_or("Always");

                    if restart_policy == "Never" || restart_policy == "OnFailure" {
                        if let Ok(container_statuses) = self.get_container_statuses(pod).await {
                            let all_terminated = !container_statuses.is_empty()
                                && container_statuses.iter().all(|cs| {
                                    matches!(
                                        cs.state,
                                        Some(ContainerState {
                                            terminated: Some(ContainerStateTerminated { .. }),
                                            ..
                                        })
                                    )
                                });

                            if all_terminated && restart_policy == "Never" {
                                let any_failed = container_statuses.iter().any(|cs| {
                                    matches!(cs.state, Some(ContainerState { terminated: Some(ContainerStateTerminated { exit_code, .. }), .. }) if exit_code != 0)
                                });
                                let terminal_phase = if any_failed {
                                    Phase::Failed
                                } else {
                                    Phase::Succeeded
                                };
                                let message = if any_failed {
                                    "Pod failed".to_string()
                                } else {
                                    "Pod completed successfully".to_string()
                                };

                                let key = build_key("pods", Some(namespace), pod_name);
                                let mut new_pod: Pod = match self.storage.get(&key).await {
                                    Ok(p) => p,
                                    _ => pod.clone(),
                                };
                                let original = new_pod.clone();
                                // Refresh init container statuses so completed init containers
                                // have ready=true in the final pod status.
                                // K8s ref: pkg/kubelet/prober/prober_manager.go — UpdatePodStatus
                                let init_container_statuses =
                                    self.get_init_container_statuses(&new_pod).await;
                                if let Some(ref mut status) = new_pod.status {
                                    status.phase = Some(terminal_phase);
                                    status.message = Some(message);
                                    status.container_statuses = Some(container_statuses);
                                    if init_container_statuses.is_some() {
                                        status.init_container_statuses = init_container_statuses;
                                    }
                                    // Update conditions — terminated pod is not Ready
                                    if let Some(ref mut conditions) = status.conditions {
                                        for c in conditions.iter_mut() {
                                            if c.condition_type == "Ready"
                                                || c.condition_type == "ContainersReady"
                                            {
                                                c.status = "False".to_string();
                                                c.reason = Some("PodCompleted".to_string());
                                            }
                                        }
                                    }
                                }
                                if !pod_status_equal(&original, &new_pod) {
                                    let _ = self.storage.update_status(&key, &new_pod).await;
                                }
                                return Ok(());
                            }

                            if all_terminated && restart_policy == "OnFailure" {
                                let any_failed = container_statuses.iter().any(|cs| {
                                    matches!(cs.state, Some(ContainerState { terminated: Some(ContainerStateTerminated { exit_code, .. }), .. }) if exit_code != 0)
                                });

                                if !any_failed {
                                    // All containers exited successfully — re-read for fresh RV
                                    let key = build_key("pods", Some(namespace), pod_name);
                                    let mut new_pod: Pod = match self.storage.get(&key).await {
                                        Ok(p) => p,
                                        _ => pod.clone(),
                                    };
                                    let original = new_pod.clone();
                                    let init_container_statuses =
                                        self.get_init_container_statuses(&new_pod).await;
                                    if let Some(ref mut status) = new_pod.status {
                                        status.phase = Some(Phase::Succeeded);
                                        status.message =
                                            Some("Pod completed successfully".to_string());
                                        status.container_statuses = Some(container_statuses);
                                        if init_container_statuses.is_some() {
                                            status.init_container_statuses =
                                                init_container_statuses;
                                        }
                                        status.conditions = Some(Self::succeeded_pod_conditions());
                                    }
                                    if !pod_status_equal(&original, &new_pod) {
                                        let _ = self.storage.update_status(&key, &new_pod).await;
                                    }
                                    return Ok(());
                                }
                            }
                        }
                    }
                }

                // For restartPolicy=Always, detect exited containers and restart them.
                // Track restart counts and set CrashLoopBackOff when appropriate.
                // IMPORTANT: Use has_terminated_containers() instead of get_container_statuses()
                // to avoid running readiness probes here. Running probes twice per sync cycle
                // (once here and once in the readiness update below) causes the probe state
                // machine to advance twice, which can make intermittent probe results flip
                // the ready state from true to false within a single sync cycle.
                // Detect exited containers and restart them per restartPolicy,
                // paced by CrashLoopBackOff. restartCount is owned by the
                // per-container backoff map (incremented only on an actual
                // restart), NOT recomputed per sync observation.
                self.reconcile_container_restarts(namespace, pod_name, pod)
                    .await;

                // Start any ephemeral containers that aren't running yet.
                // Re-read the pod from storage to pick up ephemeral containers added
                // via PATCH since the last list() call.
                {
                    let key = build_key("pods", Some(namespace), pod_name);
                    let ec_pod: Pod = match self.storage.get(&key).await {
                        Ok(p) => p,
                        _ => pod.clone(),
                    };
                    if let Some(spec) = &ec_pod.spec {
                        if let Some(ecs) = &spec.ephemeral_containers {
                            let mut started_any = false;
                            for ec in ecs {
                                // Ephemeral containers are one-shot — never restart them.
                                // Skip if the container already exists in any state (running,
                                // exited, created). Only start truly new ephemeral containers.
                                if self
                                    .runtime
                                    .container_exists(&ec_pod.metadata.uid, &ec.name)
                                    .await
                                {
                                    continue;
                                }
                                info!(
                                    "Starting ephemeral container {} for pod {}/{}",
                                    ec.name, namespace, pod_name
                                );
                                // Convert EphemeralContainer to Container for start_container
                                let container = rusternetes_common::resources::Container {
                                    name: ec.name.clone(),
                                    image: ec.image.clone(),
                                    command: ec.command.clone(),
                                    args: ec.args.clone(),
                                    env: ec.env.clone(),
                                    volume_mounts: ec.volume_mounts.clone(),
                                    resources: ec.resources.clone(),
                                    image_pull_policy: ec.image_pull_policy.clone(),
                                    security_context: ec.security_context.clone(),
                                    stdin: ec.stdin,
                                    tty: ec.tty,
                                    working_dir: ec.working_dir.clone(),
                                    ports: None,
                                    env_from: None,
                                    liveness_probe: None,
                                    readiness_probe: None,
                                    startup_probe: None,
                                    lifecycle: None,
                                    termination_message_path: ec.termination_message_path.clone(),
                                    termination_message_policy: ec
                                        .termination_message_policy
                                        .clone(),
                                    stdin_once: ec.stdin_once,
                                    restart_policy: None,
                                    resize_policy: None,
                                    volume_devices: None,
                                    ..Default::default()
                                };
                                let volume_paths = self
                                    .runtime
                                    .create_pod_volumes(&ec_pod)
                                    .await
                                    .unwrap_or_default();
                                if let Err(e) = self
                                    .runtime
                                    .start_container(
                                        &ec_pod,
                                        &container,
                                        &volume_paths,
                                        None,
                                        None,
                                        None,
                                    )
                                    .await
                                {
                                    warn!("Failed to start ephemeral container {}: {}", ec.name, e);
                                } else {
                                    started_any = true;
                                }
                            }
                            // Update ephemeral container statuses after starting new ones
                            if started_any {
                                if let Ok(mut p) = self.storage.get::<Pod>(&key).await {
                                    let ec_statuses =
                                        self.runtime.get_ephemeral_container_statuses(&p).await;
                                    if let Some(ref mut status) = p.status {
                                        status.ephemeral_container_statuses = ec_statuses;
                                    }
                                    let _ = self.storage.update_status(&key, &p).await;
                                }
                            }
                        }
                    }
                }

                // Check liveness probes
                // check_liveness may error on transient probe failures — treat errors as "no restart needed"
                // to ensure the status update branch always runs
                let restart_grace = self.runtime.check_liveness(pod).await.unwrap_or(None);
                {
                    if let Some(probe_grace) = restart_grace {
                        let restart_policy = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.restart_policy.as_deref())
                            .unwrap_or("Always");

                        match restart_policy {
                            "Always" | "OnFailure" => {
                                warn!(
                                    "Restarting pod {}/{} due to failed liveness probe",
                                    namespace, pod_name
                                );

                                // Upstream emits Unhealthy when a probe fails and
                                // Killing as it tears the container down to restart.
                                self.runtime
                                    .emit_event(
                                        pod,
                                        None,
                                        crate::events::CONTAINER_UNHEALTHY,
                                        rusternetes_common::resources::EventType::Warning,
                                        "Liveness probe failed, container will be restarted",
                                    )
                                    .await;
                                self.runtime
                                    .emit_event(
                                        pod,
                                        None,
                                        crate::events::KILLING_CONTAINER,
                                        rusternetes_common::resources::EventType::Normal,
                                        "Stopping container after failed liveness probe",
                                    )
                                    .await;

                                // Stop and restart the pod using the failed
                                // probe's terminationGracePeriodSeconds (falls
                                // back to the pod's, then 30) — upstream uses the
                                // probe's grace to kill a container that failed
                                // its probe, not the (possibly much longer) pod
                                // grace. Conformance "should override
                                // timeoutGracePeriodSeconds when Liveness/
                                // StartupProbe field is set".
                                let grace = probe_grace;
                                if let Err(e) = self.runtime.stop_pod_for(pod, grace).await {
                                    error!("Failed to stop pod for restart: {}", e);
                                } else {
                                    // Restart is in back-off — upstream emits BackOff
                                    // before the container is recreated.
                                    self.runtime
                                        .emit_event(
                                            pod,
                                            None,
                                            crate::events::BACK_OFF_START_CONTAINER,
                                            rusternetes_common::resources::EventType::Warning,
                                            "Back-off restarting failed container",
                                        )
                                        .await;
                                    // Re-read for a fresh RV. The restartCount is
                                    // NOT written here: it is derived at container
                                    // (re)create time from the prior (preserved)
                                    // container status — see
                                    // `cri_runtime::runtime::next_restart_attempt`.
                                    // Overwriting container_statuses with a
                                    // synthetic Waiting status (no container id)
                                    // erased the "ran before" signal and reset the
                                    // count, causing the non-monotonic 1 → 0
                                    // NodeConformance failure.
                                    let key = build_key("pods", Some(namespace), pod_name);
                                    let mut new_pod: Pod = match self.storage.get(&key).await {
                                        Ok(p) => p,
                                        _ => pod.clone(),
                                    };
                                    // Terminal phases are sticky. A restartPolicy=
                                    // Always pod is never terminal (the kubelet keeps
                                    // it Running/CrashLoopBackOff), so this guard does
                                    // NOT affect legitimate liveness-probe restarts —
                                    // it only blocks a racing terminal->Running flap.
                                    if should_skip_phase_write(
                                        new_pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                                        &Phase::Running,
                                    ) {
                                        return Ok(());
                                    }
                                    self.apply_liveness_restart_status(&mut new_pod).await;

                                    let _ = self.storage.update_status(&key, &new_pod).await;

                                    // Start again
                                    if let Err(e) = self.runtime.start_pod(&new_pod).await {
                                        error!("Failed to restart pod: {}", e);
                                        self.update_pod_status(
                                            pod,
                                            Phase::Failed,
                                            Some("FailedToRestart"),
                                            Some(&e.to_string()),
                                        )
                                        .await?;
                                    }
                                }
                            }
                            "Never" => {
                                warn!(
                                    "Liveness probe failed but restart policy is Never for pod {}/{}",
                                    namespace, pod_name
                                );
                                self.update_pod_status(
                                    pod,
                                    Phase::Failed,
                                    Some("LivenessProbeFailedterm"),
                                    Some("Restart policy is Never"),
                                )
                                .await?;
                            }
                            _ => {}
                        }
                    } else {
                        // Resync projected/secret/configmap volumes (data may have changed)
                        if let Err(e) = self.runtime.resync_volumes(pod, &*self.storage).await {
                            debug!(
                                "Volume resync error for pod {}/{}: {}",
                                namespace, pod_name, e
                            );
                        }

                        // Update container statuses with readiness info.
                        // IMPORTANT: Read the fresh pod from storage so that
                        // restart_count/last_state set during the restart path
                        // above are preserved (the original `pod` variable is stale).
                        let readiness_pod =
                            load_readiness_pod(&*self.storage, namespace, pod_name, pod).await;
                        if let Ok(mut container_statuses) =
                            self.get_container_statuses(&readiness_pod).await
                        {
                            // Gate each container's `ready` on its readiness probe
                            // (threshold + initialDelay tracked). Without this a
                            // running container is always ready, so a failing
                            // readiness probe never holds the pod not-ready. This
                            // is the single per-cycle readiness eval point (the
                            // restart path deliberately avoids probing — see the
                            // has_terminated_containers note above).
                            self.runtime
                                .apply_readiness(&readiness_pod, &mut container_statuses)
                                .await;
                            let mut init_container_statuses =
                                self.get_init_container_statuses(&readiness_pod).await;
                            if let Some(init_statuses) = init_container_statuses.as_mut() {
                                self.runtime
                                    .apply_readiness(&readiness_pod, init_statuses)
                                    .await;
                            }
                            // Gates/sidecars evaluated on `readiness_pod` (re-read
                            // above), NOT the cycle-start `pod`; see
                            // evaluate_pod_readiness (#2384, #2387).
                            let (containers_ready, all_ready) = evaluate_pod_readiness(
                                &readiness_pod,
                                &container_statuses,
                                init_container_statuses.as_deref(),
                            );

                            // Check if all containers have terminated (for Never/OnFailure restart policies)
                            let restart_policy = pod
                                .spec
                                .as_ref()
                                .and_then(|s| s.restart_policy.as_deref())
                                .unwrap_or("Always");

                            let all_terminated = !container_statuses.is_empty()
                                && container_statuses.iter().all(|cs| {
                                    matches!(
                                        cs.state,
                                        Some(ContainerState {
                                            terminated: Some(ContainerStateTerminated { .. }),
                                            ..
                                        })
                                    )
                                });

                            if all_terminated && restart_policy == "Never" {
                                let any_failed = container_statuses.iter().any(|cs| {
                                    matches!(cs.state, Some(ContainerState { terminated: Some(ContainerStateTerminated { exit_code, .. }), .. }) if exit_code != 0)
                                });
                                let terminal_phase = if any_failed {
                                    Phase::Failed
                                } else {
                                    Phase::Succeeded
                                };
                                let message = if any_failed {
                                    "Pod failed".to_string()
                                } else {
                                    "Pod completed successfully".to_string()
                                };

                                let key = build_key("pods", Some(namespace), pod_name);
                                let mut new_pod: Pod = match self.storage.get(&key).await {
                                    Ok(p) => p,
                                    _ => pod.clone(),
                                };
                                let original = new_pod.clone();
                                // Refresh init container statuses for terminal pod
                                let init_container_statuses =
                                    self.get_init_container_statuses(&new_pod).await;
                                if let Some(ref mut status) = new_pod.status {
                                    status.phase = Some(terminal_phase);
                                    status.message = Some(message);
                                    status.container_statuses = Some(container_statuses);
                                    if init_container_statuses.is_some() {
                                        status.init_container_statuses = init_container_statuses;
                                    }
                                    // Update conditions — terminated pod is not Ready
                                    if let Some(ref mut conditions) = status.conditions {
                                        for c in conditions.iter_mut() {
                                            if c.condition_type == "Ready"
                                                || c.condition_type == "ContainersReady"
                                            {
                                                c.status = "False".to_string();
                                                c.reason = Some("PodCompleted".to_string());
                                            }
                                        }
                                    }
                                }
                                if !pod_status_equal(&original, &new_pod) {
                                    let _ = self.storage.update_status(&key, &new_pod).await;
                                }
                                return Ok(());
                            }

                            if all_terminated && restart_policy == "OnFailure" {
                                let any_failed = container_statuses.iter().any(|cs| {
                                    matches!(cs.state, Some(ContainerState { terminated: Some(ContainerStateTerminated { exit_code, .. }), .. }) if exit_code != 0)
                                });

                                if any_failed {
                                    // Restart only the failed containers
                                    warn!(
                                        "Restarting failed containers for pod {}/{} (OnFailure)",
                                        namespace, pod_name
                                    );
                                    let grace = pod
                                        .spec
                                        .as_ref()
                                        .and_then(|s| s.termination_grace_period_seconds)
                                        .unwrap_or(30);
                                    if let Err(e) = self.runtime.stop_pod_for(pod, grace).await {
                                        error!("Failed to stop pod for restart: {}", e);
                                    } else if let Err(e) = self.runtime.start_pod(pod).await {
                                        error!("Failed to restart pod: {}", e);
                                        self.update_pod_status(
                                            pod,
                                            Phase::Failed,
                                            Some("FailedToRestart"),
                                            Some(&e.to_string()),
                                        )
                                        .await?;
                                    }
                                    return Ok(());
                                } else {
                                    // All containers exited 0 — transition to Succeeded
                                    let key = build_key("pods", Some(namespace), pod_name);
                                    let mut new_pod: Pod = match self.storage.get(&key).await {
                                        Ok(p) => p,
                                        _ => pod.clone(),
                                    };
                                    let init_container_statuses =
                                        self.get_init_container_statuses(&new_pod).await;
                                    if let Some(ref mut status) = new_pod.status {
                                        status.phase = Some(Phase::Succeeded);
                                        status.message =
                                            Some("Pod completed successfully".to_string());
                                        status.container_statuses = Some(container_statuses);
                                        if init_container_statuses.is_some() {
                                            status.init_container_statuses =
                                                init_container_statuses;
                                        }
                                        status.conditions = Some(Self::succeeded_pod_conditions());
                                    }
                                    let _ = self.storage.update_status(&key, &new_pod).await;
                                    return Ok(());
                                }
                            }

                            // Get pod IP (important for pods started by docker-compose)
                            let pod_ip = self.runtime.get_pod_ip(pod).await.ok().flatten();

                            // Re-read pod from storage to get latest resourceVersion
                            // to avoid CAS conflicts when other controllers have
                            // updated the pod since we last read it.
                            let key = build_key("pods", Some(namespace), pod_name);
                            let mut new_pod: Pod = match self.storage.get::<Pod>(&key).await {
                                Ok(p) => p,
                                Err(_) => pod.clone(),
                            };
                            // Update ephemeral container statuses from Docker
                            let ephemeral_container_statuses = self
                                .runtime
                                .get_ephemeral_container_statuses(&new_pod)
                                .await;

                            if let Some(ref mut status) = new_pod.status {
                                status.container_statuses = Some(container_statuses);
                                status.init_container_statuses = init_container_statuses;
                                status.ephemeral_container_statuses = ephemeral_container_statuses;
                                status.observed_generation = new_pod.metadata.generation;
                                // Update pod IP if we got one and it's different
                                if pod_ip.is_some() && status.pod_ip != pod_ip {
                                    status.pod_i_ps =
                                        pod_ip.as_ref().map(|ip| vec![PodIP { ip: ip.clone() }]);
                                    status.pod_ip = pod_ip;
                                }
                                status.message = Some(
                                    if all_ready {
                                        "All containers ready"
                                    } else if containers_ready {
                                        "Waiting on pod readiness gates"
                                    } else {
                                        "Some containers not ready"
                                    }
                                    .to_string(),
                                );
                                // ContainersReady reflects only the containers;
                                // Ready additionally requires the readinessGates.
                                status.conditions = Some(Self::merge_pod_conditions(
                                    status.conditions.as_deref().unwrap_or(&[]),
                                    Self::pod_readiness_conditions(containers_ready, all_ready),
                                ));
                            }

                            // Skip update if status hasn't changed — avoids unnecessary
                            // resourceVersion bumps that cause CAS conflicts for kubectl replace.
                            // K8s kubelet uses status manager to diff before writing.
                            let old_status_json = serde_json::to_value(pod.status.as_ref()).ok();
                            let new_status_json =
                                serde_json::to_value(new_pod.status.as_ref()).ok();
                            if old_status_json == new_status_json {
                                debug!(
                                    "Pod {}/{} status unchanged, skipping update",
                                    namespace, pod_name
                                );
                                return Ok(());
                            }

                            if let Err(e) = self.storage.update_status(&key, &new_pod).await {
                                // CAS conflict — re-read and retry once
                                debug!("Pod status update CAS conflict, retrying: {}", e);
                                if let Ok(mut fresh_pod) = self.storage.get::<Pod>(&key).await {
                                    // observedGeneration MUST track the generation
                                    // of the object we just re-read, not the stale
                                    // value carried in new_pod / persisted earlier.
                                    // A spec update that bumped generation is what
                                    // caused this conflict; stamping the fresh
                                    // generation here is how the kubelet converges
                                    // observedGeneration under rapid parallel
                                    // updates (#1170). Capture before the mut borrow.
                                    let fresh_gen = fresh_pod.metadata.generation;
                                    if let Some(ref mut status) = fresh_pod.status {
                                        status.container_statuses = new_pod
                                            .status
                                            .as_ref()
                                            .and_then(|s| s.container_statuses.clone());
                                        status.conditions = new_pod
                                            .status
                                            .as_ref()
                                            .and_then(|s| s.conditions.clone());
                                        status.message =
                                            new_pod.status.as_ref().and_then(|s| s.message.clone());
                                        status.observed_generation = fresh_gen;
                                        if let Some(ref new_status) = new_pod.status {
                                            if new_status.pod_ip.is_some() {
                                                status.pod_ip = new_status.pod_ip.clone();
                                                status.pod_i_ps = new_status.pod_i_ps.clone();
                                            }
                                        }
                                    }
                                    if let Err(e2) =
                                        self.storage.update_status(&key, &fresh_pod).await
                                    {
                                        warn!("Failed to update pod status after retry: {}", e2);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Phase::Running if !is_running => {
                // Containers have stopped — decide based on restart policy
                let restart_policy = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.restart_policy.as_deref())
                    .unwrap_or("Always");

                let container_statuses = self.get_container_statuses(pod).await.ok();
                let any_failed = container_statuses
                    .as_ref()
                    .map(|statuses| {
                        statuses.iter().any(|cs| {
                            matches!(cs.state, Some(ContainerState { terminated: Some(ContainerStateTerminated { exit_code, .. }), .. }) if exit_code != 0)
                        })
                    })
                    .unwrap_or(false);

                match restart_policy {
                    "Always" => {
                        // Restart stopped containers individually, paced by
                        // per-container CrashLoopBackOff. restartCount is owned
                        // by the backoff map and advances ONLY on a real restart
                        // — never per sync observation (which previously inflated
                        // it to tens of thousands under the sync hot loop).
                        self.reconcile_container_restarts(namespace, pod_name, pod)
                            .await;
                    }
                    "OnFailure" => {
                        if any_failed {
                            // Restart failed containers individually, paced by
                            // per-container CrashLoopBackOff (restartCount owned
                            // by the backoff map, advanced only on real restart).
                            self.reconcile_container_restarts(namespace, pod_name, pod)
                                .await;
                        } else {
                            info!(
                                "Pod {}/{} completed successfully (restartPolicy=OnFailure)",
                                namespace, pod_name
                            );
                            let key = build_key("pods", Some(namespace), pod_name);
                            let mut new_pod: Pod = match self.storage.get(&key).await {
                                Ok(p) => p,
                                _ => pod.clone(),
                            };
                            let init_container_statuses =
                                self.get_init_container_statuses(&new_pod).await;
                            if let Some(ref mut status) = new_pod.status {
                                status.phase = Some(Phase::Succeeded);
                                status.message = Some("Pod completed successfully".to_string());
                                if let Some(ref cs) = container_statuses {
                                    status.container_statuses = Some(cs.clone());
                                }
                                if init_container_statuses.is_some() {
                                    status.init_container_statuses = init_container_statuses;
                                }
                                Self::fixup_init_container_ready(status);
                                status.conditions = Some(Self::succeeded_pod_conditions());
                            }
                            let key = build_key("pods", Some(namespace), pod_name);
                            let _ = self.storage.update_status(&key, &new_pod).await;
                        }
                    }
                    "Never" => {
                        let terminal_phase = if any_failed {
                            Phase::Failed
                        } else {
                            Phase::Succeeded
                        };
                        let message = if any_failed {
                            "Pod failed".to_string()
                        } else {
                            "Pod completed successfully".to_string()
                        };
                        info!(
                            "Pod {}/{} terminated (restartPolicy=Never, phase={:?})",
                            namespace, pod_name, terminal_phase
                        );
                        let key = build_key("pods", Some(namespace), pod_name);
                        let mut new_pod: Pod = match self.storage.get(&key).await {
                            Ok(p) => p,
                            _ => pod.clone(),
                        };
                        let init_container_statuses =
                            self.get_init_container_statuses(&new_pod).await;
                        if let Some(ref mut status) = new_pod.status {
                            status.phase = Some(terminal_phase.clone());
                            status.message = Some(message);
                            if let Some(ref cs) = container_statuses {
                                status.container_statuses = Some(cs.clone());
                            }
                            if init_container_statuses.is_some() {
                                status.init_container_statuses = init_container_statuses;
                            }
                            Self::fixup_init_container_ready(status);
                            // A terminal pod is never Ready. `IsPodReady` (which
                            // the conformance suite checks) reads the Ready
                            // CONDITION, so set it False for BOTH terminal phases
                            // — not just Succeeded (the Failed case previously
                            // leaked the Running conditions with Ready=True).
                            status.conditions = Some(if terminal_phase == Phase::Succeeded {
                                Self::succeeded_pod_conditions()
                            } else {
                                Self::failed_pod_conditions()
                            });
                        }
                        let key = build_key("pods", Some(namespace), pod_name);
                        let _ = self.storage.update_status(&key, &new_pod).await;
                    }
                    _ => {}
                }
            }
            Phase::Succeeded | Phase::Failed => {
                // Terminal phase is handled by the state machine above
                // (SyncPod → TerminatingPod → TerminatedPod).
                // This branch should not be reached for pods in SyncPod state
                // because needs_terminating transitions them first.
                debug!(
                    "Pod {}/{} reached terminal phase handler in SyncPod (should not happen)",
                    namespace, pod_name
                );
            }
            _ => {
                debug!(
                    "Pod {}/{} is in sync (phase: {:?}, running: {})",
                    namespace, pod_name, current_phase, is_running
                );
            }
        }

        Ok(())
    }

    /// Detect terminated containers and restart them per the pod's
    /// `restartPolicy`, paced by per-container CrashLoopBackOff.
    ///
    /// `restartCount` is owned by [`RestartBackoff`] and advances **only on an
    /// actual restart** — not once per sync that happens to observe a
    /// terminated container, which is what previously drove the count to tens
    /// of thousands under the watch-driven sync hot loop. The first restart
    /// after a crash is immediate; each subsequent restart waits `backoff`
    /// (10s, doubling, capped at 5m) measured from the previous restart, so the
    /// observable `restartCount` advances slowly enough for clients to see each
    /// value. Volume binds are rebuilt via `create_pod_volumes` so a restarted
    /// container re-binds the SAME on-disk volumes (emptyDir data persists).
    ///
    /// K8s ref: `pkg/kubelet/kuberuntime/kuberuntime_manager.go`
    /// (computePodActions + doBackOff) and `client-go/util/flowcontrol.Backoff`.
    async fn reconcile_container_restarts(&self, namespace: &str, pod_name: &str, pod: &Pod) {
        // Never restart containers of a terminating pod: its containers are being
        // shut down on purpose, and restarting a sidecar mid-drain would keep the
        // pod alive past its grace period and prevent it from being finalized.
        if pod.metadata.deletion_timestamp.is_some() {
            return;
        }
        let restart_policy = pod
            .spec
            .as_ref()
            .and_then(|s| s.restart_policy.as_deref())
            .unwrap_or("Always");
        if restart_policy != "Always" && restart_policy != "OnFailure" {
            return;
        }
        if !self.runtime.has_terminated_containers(pod).await {
            return;
        }
        let Some(spec) = pod.spec.as_ref() else {
            return;
        };

        // NOTE: volume (re)projection is deferred to an actual restart below —
        // it must NOT run on every sync of a running pod. Re-projecting churns
        // the mounted files (write/chmod/chown), and a watcher like kube-proxy
        // exits on ANY change to its config file ("content ... was updated") →
        // CrashLoopBackOff. Upstream never re-projects a running pod's volumes
        // (they stay mounted; re-SetUp is a no-op via AtomicWriter); we rebuild
        // only when we're about to (re)start a container. `volume_paths` is
        // computed lazily on the first restart in this pass and reused.
        let mut volume_paths: Option<std::collections::HashMap<String, String>> = None;

        let now = Instant::now();

        // App containers must not be (re)started until every plain init
        // container has completed — a never-started app container reads as
        // "not running" here and would otherwise be started straight into a
        // pod whose init sequence is still failing. Upstream `computePodActions`
        // only adds app containers to the start set once init is complete
        // ("should not start app containers if init containers fail on a
        // RestartAlways pod"). Restartable init containers (sidecars) are exempt
        // — they run alongside init/app and are always eligible for restart.
        let (all_init_done, _, _) = self.runtime.compute_init_container_actions(pod).await;

        // Restart regular containers and restartable init containers (sidecars:
        // restartPolicy=Always) the same way. start_container handles both; the
        // backoff map keys on the (unique) container name. Plain init containers
        // run to completion and are not restarted here.
        let restartable_inits = spec
            .init_containers
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|ic| ic.restart_policy.as_deref() == Some("Always"));
        for c in spec
            .containers
            .iter()
            .filter(|_| all_init_done)
            .chain(restartable_inits)
        {
            if self
                .runtime
                .is_container_running(&pod.metadata.uid, &c.name)
                .await
                .unwrap_or(true)
            {
                continue; // still running — nothing to do
            }

            // OnFailure: a clean exit (code 0) is terminal — do not restart.
            if restart_policy == "OnFailure" {
                let exit_code = self
                    .runtime
                    .get_container_exit_code(&pod.metadata.uid, &c.name)
                    .await
                    .unwrap_or(1);
                if exit_code == 0 {
                    continue;
                }
            }

            // CrashLoopBackOff gate. The check-and-advance is atomic under the
            // lock, so even if two syncs race only one wins the restart.
            let bkey = format!("{}/{}/{}", namespace, pod_name, c.name);
            let do_restart = {
                let mut map = self.restart_backoff.lock().unwrap();
                match map.get_mut(&bkey) {
                    None => {
                        // First crash → restart immediately, seed the backoff.
                        map.insert(
                            bkey.clone(),
                            RestartBackoff {
                                restart_count: 1,
                                last_restart: now,
                                backoff: self.crash_loop_backoff_initial(),
                            },
                        );
                        true
                    }
                    Some(entry) => {
                        if now.duration_since(entry.last_restart) >= entry.backoff {
                            entry.restart_count += 1;
                            entry.last_restart = now;
                            entry.backoff = (entry.backoff * 2).min(self.crash_loop_backoff_max);
                            true
                        } else {
                            false // still within the backoff window
                        }
                    }
                }
            };

            if do_restart {
                let _ = self
                    .runtime
                    .remove_terminated_container(&pod.metadata.uid, &c.name)
                    .await;
                // Rebuild volume bind paths now — only because we are actually
                // restarting a container (idempotent re-create; Memory-emptyDir
                // tmpfs and on-disk data persist as the same dir). Computed once
                // per reconcile pass and reused for any further restarts.
                if volume_paths.is_none() {
                    volume_paths = Some(
                        self.runtime
                            .create_pod_volumes(pod)
                            .await
                            .unwrap_or_default(),
                    );
                }
                let vp = volume_paths.as_ref().unwrap();
                let pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.as_deref());
                if let Err(e) = self
                    .runtime
                    .start_container(pod, c, vp, None, None, pod_ip)
                    .await
                {
                    debug!(
                        "Failed to restart container {} in pod {}/{}: {}",
                        c.name, namespace, pod_name, e
                    );
                } else {
                    info!(
                        "Restarted container {} in pod {}/{}",
                        c.name, namespace, pod_name
                    );
                }
            }
        }

        // Publish status: restartCount from the backoff map; any container still
        // Terminated here was NOT restarted this pass (backing off) → surface
        // Waiting/CrashLoopBackOff. Re-read the pod so we don't clobber a
        // concurrent writer.
        let key = build_key("pods", Some(namespace), pod_name);
        if let Ok(mut fresh_pod) = self.storage.get::<Pod>(&key).await {
            if let Ok(mut statuses) = self.get_container_statuses(&fresh_pod).await {
                {
                    let map = self.restart_backoff.lock().unwrap();
                    for cs in statuses.iter_mut() {
                        if let Some(entry) =
                            map.get(&format!("{}/{}/{}", namespace, pod_name, cs.name))
                        {
                            cs.restart_count = cs.restart_count.max(entry.restart_count);
                        }
                        if matches!(
                            cs.state,
                            Some(ContainerState {
                                terminated: Some(ContainerStateTerminated { .. }),
                                ..
                            })
                        ) {
                            cs.last_state = cs.state.take();
                            cs.state = Some(ContainerState {
                                waiting: Some(ContainerStateWaiting {
                                    reason: Some("CrashLoopBackOff".to_string()),
                                    message: Some(
                                        "Back-off restarting failed container".to_string(),
                                    ),
                                }),
                                ..Default::default()
                            });
                            cs.ready = false;
                            cs.started = Some(false);
                        }
                    }
                }
                // Restartable init containers (sidecars) carry their own
                // restart counts in init_container_statuses; overlay the backoff
                // map there too so a restarted sidecar reports its count.
                let mut init_statuses = self.get_init_container_statuses(&fresh_pod).await;
                if let Some(ref mut list) = init_statuses {
                    let map = self.restart_backoff.lock().unwrap();
                    for cs in list.iter_mut() {
                        if let Some(entry) =
                            map.get(&format!("{}/{}/{}", namespace, pod_name, cs.name))
                        {
                            cs.restart_count = cs.restart_count.max(entry.restart_count);
                        }
                    }
                }
                if let Some(ref mut s) = fresh_pod.status {
                    s.container_statuses = Some(statuses);
                    if init_statuses.is_some() {
                        s.init_container_statuses = init_statuses;
                    }
                }
                let _ = self.storage.update_status(&key, &fresh_pod).await;
            }
        }
    }

    /// Drop CrashLoopBackOff state for every container of a removed pod so the
    /// map does not grow without bound.
    fn forget_restart_backoff(&self, namespace: &str, pod_name: &str) {
        let prefix = format!("{}/{}/", namespace, pod_name);
        self.restart_backoff
            .lock()
            .unwrap()
            .retain(|k, _| !k.starts_with(&prefix));
    }

    /// Monotonicity safety net for `restartCount`. The primary source is now the
    /// CRI `metadata.attempt`, stamped at container (re)create from the prior
    /// persisted status (`cri_runtime::runtime::next_restart_attempt`, matching
    /// upstream `startContainer`). This overlay never LOWERS a count — it only
    /// raises it to the crash-loop [`RestartBackoff`] tally if that is somehow
    /// ahead — so a transient CRI read (e.g. mid sandbox recreation) can never
    /// regress a published count `1 → 0` (the container_probe NodeConformance
    /// spec). Applied everywhere statuses are published (#1514).
    fn overlay_restart_backoff(&self, statuses: &mut [ContainerStatus], pod: &Pod) {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let pod_name = &pod.metadata.name;
        let map = self.restart_backoff.lock().unwrap();
        for cs in statuses.iter_mut() {
            if let Some(entry) = map.get(&format!("{}/{}/{}", namespace, pod_name, cs.name)) {
                cs.restart_count = cs.restart_count.max(entry.restart_count);
            }
        }
    }

    /// `ContainerRuntime::get_container_statuses` + the [`RestartBackoff`] overlay.
    /// All status-publish paths go through this so `restartCount` is monotonic
    /// and consistent regardless of CRI sandbox churn (#1514).
    async fn get_container_statuses(&self, pod: &Pod) -> Result<Vec<ContainerStatus>> {
        let mut statuses = self.runtime.get_container_statuses(pod).await?;
        self.overlay_restart_backoff(&mut statuses, pod);
        Ok(statuses)
    }

    /// Restartable-init (sidecar) variant — same backoff overlay (#1514).
    async fn get_init_container_statuses(&self, pod: &Pod) -> Option<Vec<ContainerStatus>> {
        let mut statuses = self.runtime.get_init_container_statuses(pod).await?;
        self.overlay_restart_backoff(&mut statuses, pod);
        Some(statuses)
    }

    /// Record one more failed terminal finalize for `key` (the object is still
    /// present after a reported removal) and return the new attempt count (#1157).
    fn record_terminal_finalize_failure(&self, key: &str) -> u32 {
        let mut m = self.terminal_finalize_failures.lock().unwrap();
        let entry = m.entry(key.to_string()).or_insert((0, Instant::now()));
        entry.0 += 1;
        entry.1 = Instant::now();
        entry.0
    }

    /// Forget any terminal-finalize failure record for `key` (the object is now
    /// gone, so the next delete starts fresh).
    fn clear_terminal_finalize_failure(&self, key: &str) {
        self.terminal_finalize_failures.lock().unwrap().remove(key);
    }

    /// `Some(remaining)` when `key` has a pending terminal-finalize backoff that
    /// has not yet elapsed — the caller should skip re-terminating this cycle.
    /// `None` when there is no record or the window has passed (retry now).
    fn terminal_finalize_backoff_remaining(&self, key: &str) -> Option<Duration> {
        let m = self.terminal_finalize_failures.lock().unwrap();
        let (count, last) = m.get(key)?;
        terminal_finalize_backoff(*count).checked_sub(last.elapsed())
    }

    /// Preserve `last_transition_time` from existing conditions whose
    /// type+status match the incoming ones. Without this every status
    /// sync re-stamps the time → pod_status_equal returns false → write
    /// → watch event → next sync repeats. Hot loop at ~30 Hz per pod.
    fn merge_pod_conditions(
        existing: &[PodCondition],
        mut incoming: Vec<PodCondition>,
    ) -> Vec<PodCondition> {
        for new in incoming.iter_mut() {
            if let Some(prev) = existing
                .iter()
                .find(|c| c.condition_type == new.condition_type && c.status == new.status)
            {
                new.last_transition_time = prev.last_transition_time;
            }
        }
        // Preserve existing conditions this writer doesn't manage — notably the
        // custom conditions a readinessGate refers to, supplied out-of-band via
        // the pod status subresource. Without this the kubelet clobbers them on
        // every status sync and the readinessGate can never be satisfied.
        for prev in existing {
            if !incoming
                .iter()
                .any(|c| c.condition_type == prev.condition_type)
            {
                incoming.push(prev.clone());
            }
        }
        incoming
    }

    fn incomplete_init_container_names(
        pod: &Pod,
        init_statuses: Option<&[ContainerStatus]>,
    ) -> Vec<String> {
        pod.spec
            .as_ref()
            .and_then(|s| s.init_containers.as_ref())
            .map(|init_containers| {
                init_containers
                    .iter()
                    .filter(|container| {
                        !init_container_completed_successfully(init_statuses, &container.name)
                    })
                    .map(|container| container.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn pod_initializing_container_statuses(pod: &Pod) -> Option<Vec<ContainerStatus>> {
        pod.spec.as_ref().map(|spec| {
            spec.containers
                .iter()
                .map(|container| ContainerStatus {
                    name: container.name.clone(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState {
                        waiting: Some(ContainerStateWaiting {
                            reason: Some("PodInitializing".to_string()),
                            message: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some(container.image.clone()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: container
                        .resources
                        .as_ref()
                        .and_then(|r| r.requests.clone()),
                    allocated_resources_status: None,
                    resources: container.resources.clone(),
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                })
                .collect()
        })
    }

    /// Pending status for a container creation/config error (typed CRI
    /// reason), generated through `generate_api_pod_status`
    /// (pkg/kubelet/kubelet_pods.go:1889 generateAPIPodStatus). The caller
    /// applies the host-IP block afterwards.
    fn create_error_pod_status(
        &self,
        pod: &Pod,
        reason: String,
        err_msg: String,
        container_statuses: Option<Vec<ContainerStatus>>,
        init_container_statuses: Option<Vec<ContainerStatus>>,
        qos: String,
    ) -> PodStatus {
        let start_time = preserved_start_time(
            pod.status.as_ref().and_then(|s| s.start_time),
            chrono::Utc::now(),
        );
        let (host_ip, host_ips) = self.host_ip_fields();
        crate::pod_status::generate_api_pod_status(
            pod,
            crate::pod_status::ApiPodStatusInput {
                phase: Phase::Pending,
                message: Some(err_msg),
                reason: Some(reason),
                host_ip,
                host_ips,
                pod_ip: None,
                pod_ips: None,
                conditions: Vec::new(),
                container_statuses,
                init_container_statuses,
                ephemeral_container_statuses: None,
                qos_class: Some(qos),
                start_time,
            },
        )
    }

    /// Status for an app-container start error (init not terminally failed),
    /// followed by the host-IP block of `generateAPIPodStatus`
    /// (kubelet_pods.go:2005-2040).
    async fn apply_app_start_error_status(
        &self,
        new_pod: &mut Pod,
        init_container_statuses: Option<Vec<ContainerStatus>>,
        err_msg: &str,
        is_port_conflict: bool,
        qos: String,
    ) {
        // App-container start error. Waiting{ContainerCreating}
        // carries the error message; phase is Failed only for a
        // permanent port conflict, otherwise Pending to retry.
        let phase = if is_port_conflict {
            Phase::Failed
        } else {
            Phase::Pending
        };
        let conditions = if is_port_conflict {
            Self::failed_pod_conditions()
        } else {
            Self::not_ready_pod_conditions()
        };
        // App containers blocked behind an incomplete
        // init container must report PodInitializing (not
        // ContainerCreating), and carry no init-error
        // message — the failure belongs on the init
        // container's status. Genuine app-container start
        // errors (init already done) keep ContainerCreating
        // + the error message. Mirrors upstream
        // convertToAPIContainerStatuses (kubelet_pods.go).
        let app_reason = app_container_waiting_reason(new_pod, init_container_statuses.as_deref());
        let app_message = if app_reason == "PodInitializing" {
            None
        } else {
            Some(err_msg.to_string())
        };
        let container_statuses: Option<Vec<ContainerStatus>> = new_pod.spec.as_ref().map(|spec| {
            spec.containers
                .iter()
                .map(|c| ContainerStatus {
                    name: c.name.clone(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState {
                        waiting: Some(ContainerStateWaiting {
                            reason: Some(app_reason.to_string()),
                            message: app_message.clone(),
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some(c.image.clone()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: c.resources.as_ref().and_then(|r| r.requests.clone()),
                    allocated_resources_status: None,
                    resources: c.resources.clone(),
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                })
                .collect()
        });
        // generateAPIPodStatus (kubelet_pods.go:1889), see pod_status.rs.
        let prior = new_pod.status.as_ref();
        let (host_ip, host_ips) = self.host_ip_fields();
        let input = crate::pod_status::ApiPodStatusInput {
            phase,
            message: Some(err_msg.to_string()),
            reason: Some("FailedToStart".to_string()),
            host_ip: prior.and_then(|s| s.host_ip.clone()).or(host_ip),
            host_ips: prior.and_then(|s| s.host_i_ps.clone()).or(host_ips),
            pod_ip: prior.and_then(|s| s.pod_ip.clone()),
            pod_ips: prior.and_then(|s| s.pod_i_ps.clone()),
            conditions,
            container_statuses,
            init_container_statuses,
            ephemeral_container_statuses: prior
                .and_then(|s| s.ephemeral_container_statuses.clone()),
            qos_class: Some(qos),
            start_time: preserved_start_time(prior.and_then(|s| s.start_time), chrono::Utc::now()),
        };
        new_pod.status = Some(crate::pod_status::generate_api_pod_status(new_pod, input));
        self.apply_host_ips_to_pod(new_pod).await;
    }

    /// Liveness-restart status write, followed by the host-IP block of
    /// `generateAPIPodStatus` (kubelet_pods.go:2005-2040).
    async fn apply_liveness_restart_status(&self, new_pod: &mut Pod) {
        if let Some(ref mut status) = new_pod.status {
            status.phase = Some(Phase::Running);
            status.message = Some("Liveness probe failed".to_string());
            status.reason = Some("Restarting".to_string());
        } else {
            // generateAPIPodStatus (kubelet_pods.go:1889), see pod_status.rs.
            let (host_ip, host_ips) = self.host_ip_fields();
            new_pod.status = Some(crate::pod_status::generate_api_pod_status(
                new_pod,
                crate::pod_status::ApiPodStatusInput {
                    phase: Phase::Running,
                    message: Some("Liveness probe failed".to_string()),
                    reason: Some("Restarting".to_string()),
                    host_ip,
                    host_ips,
                    pod_ip: None,
                    pod_ips: None,
                    conditions: Vec::new(),
                    container_statuses: None,
                    init_container_statuses: None,
                    ephemeral_container_statuses: None,
                    qos_class: None,
                    start_time: chrono::Utc::now(),
                },
            ));
        }
        self.apply_host_ips_to_pod(new_pod).await;
    }

    /// `build_init_failure_status` followed by the host-IP block of
    /// `generateAPIPodStatus` (kubelet_pods.go:2005-2040), so the init-failure
    /// write also carries hostNetwork PodIPs and records
    /// `HostIPsIPFamilyMismatch`.
    async fn init_failure_status(
        &self,
        pod: &Pod,
        init_statuses: Option<Vec<ContainerStatus>>,
        phase: Phase,
        reason: &str,
        qos: Option<String>,
    ) -> PodStatus {
        let status = Self::build_init_failure_status(
            pod,
            self.host_ip_fields(),
            init_statuses,
            phase,
            reason,
            qos,
        );
        let mut scratch = pod.clone();
        scratch.status = Some(status);
        self.apply_host_ips_to_pod(&mut scratch).await;
        scratch.status.unwrap_or_default()
    }

    fn build_init_failure_status(
        pod: &Pod,
        (host_ip, host_i_ps): HostIpFields,
        init_statuses: Option<Vec<ContainerStatus>>,
        phase: Phase,
        reason: &str,
        qos: Option<String>,
    ) -> PodStatus {
        let incomplete_inits = Self::incomplete_init_container_names(pod, init_statuses.as_deref());
        let message = if !incomplete_inits.is_empty() {
            format!(
                "containers with incomplete status: [{}]",
                incomplete_inits.join(" ")
            )
        } else {
            "Init container failed".to_string()
        };
        let prior_status = pod.status.as_ref();

        // generateAPIPodStatus (kubelet_pods.go:1889), see pod_status.rs.
        crate::pod_status::generate_api_pod_status(
            pod,
            crate::pod_status::ApiPodStatusInput {
                phase,
                message: Some(message),
                reason: Some(reason.to_string()),
                host_ip: prior_status
                    .and_then(|status| status.host_ip.clone())
                    .or(host_ip),
                host_ips: prior_status
                    .and_then(|status| status.host_i_ps.clone())
                    .or(host_i_ps),
                pod_ip: prior_status.and_then(|status| status.pod_ip.clone()),
                pod_ips: prior_status.and_then(|status| status.pod_i_ps.clone()),
                conditions: Self::init_failed_pod_conditions(&incomplete_inits),
                container_statuses: Self::pod_initializing_container_statuses(pod),
                init_container_statuses: init_statuses,
                ephemeral_container_statuses: prior_status
                    .and_then(|status| status.ephemeral_container_statuses.clone()),
                qos_class: qos.or_else(|| prior_status.and_then(|status| status.qos_class.clone())),
                start_time: preserved_start_time(
                    prior_status.and_then(|status| status.start_time),
                    chrono::Utc::now(),
                ),
            },
        )
    }

    /// Build the standard pod conditions with ContainersReady and Ready set
    /// independently. ContainersReady reflects only container/sidecar readiness;
    /// Ready additionally requires every spec.readinessGates condition to be True
    /// (upstream GeneratePodReadyCondition). Initialized and PodScheduled are
    /// always True here (the pod has been admitted and its containers created).
    fn pod_readiness_conditions(containers_ready: bool, pod_ready: bool) -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        let bool_str = |b: bool| if b { "True" } else { "False" }.to_string();
        let not_ready_reason = |b: bool| {
            if b {
                (None, None)
            } else {
                (
                    Some("ContainersNotReady".to_string()),
                    Some("containers or readiness gates not ready".to_string()),
                )
            }
        };
        let (cr_reason, cr_msg) = not_ready_reason(containers_ready);
        let (rd_reason, rd_msg) = not_ready_reason(pod_ready);
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: bool_str(containers_ready),
                reason: cr_reason,
                message: cr_msg,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: bool_str(pod_ready),
                reason: rd_reason,
                message: rd_msg,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Build the standard pod conditions for a Running pod.
    /// Real Kubernetes sets Initialized, PodScheduled, ContainersReady, and Ready=True
    /// when all containers are running. The e2e conformance suite checks these conditions.
    #[allow(dead_code)]
    fn running_pod_conditions() -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    fn not_ready_pod_conditions() -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotReady".to_string()),
                message: Some("Not all containers are ready".to_string()),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotReady".to_string()),
                message: Some("Not all containers are ready".to_string()),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Fix up init container ready status per K8s prober_manager.go:377.
    /// For non-restartable init containers, if terminated with exit_code=0,
    /// set ready=true. This must run on EVERY status write, not just deletion.
    fn fixup_init_container_ready(status: &mut PodStatus) {
        if let Some(ref mut ics) = status.init_container_statuses {
            for ic in ics.iter_mut() {
                if let Some(ContainerState {
                    terminated: Some(ContainerStateTerminated { exit_code, .. }),
                    ..
                }) = &ic.state
                {
                    if *exit_code == 0 {
                        ic.ready = true;
                        ic.started = Some(true);
                    }
                }
            }
        }
    }

    /// Build conditions for a pod that has succeeded (all containers completed successfully).
    /// K8s ref: pkg/kubelet/status/generate.go — PodCompleted reason
    fn succeeded_pod_conditions() -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: Some("PodCompleted".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "False".to_string(),
                reason: Some("PodCompleted".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                reason: Some("PodCompleted".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Conditions for a pod that has reached the terminal `Failed` phase
    /// (e.g. restartPolicy=Never with a non-zero container exit). Like the
    /// Succeeded set, `Ready` and `ContainersReady` MUST be `False` —
    /// `podutil.IsPodReady` (which the conformance suite checks) keys off the
    /// `Ready` condition, not the container `ready` field.
    fn failed_pod_conditions() -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "False".to_string(),
                reason: Some("PodFailed".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                reason: Some("PodFailed".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Build conditions for a pod whose init containers failed.
    fn init_failed_pod_conditions(incomplete_init_names: &[String]) -> Vec<PodCondition> {
        let now = Some(chrono::Utc::now());
        let msg = if !incomplete_init_names.is_empty() {
            format!(
                "containers with incomplete status: [{}]",
                incomplete_init_names.join(" ")
            )
        } else {
            "Init container failed".to_string()
        };
        vec![
            PodCondition {
                condition_type: "Initialized".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotInitialized".to_string()),
                message: Some(msg.clone()),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "PodScheduled".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "ContainersReady".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotReady".to_string()),
                message: Some(msg.clone()),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
            PodCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                reason: Some("ContainersNotReady".to_string()),
                message: Some(msg),
                last_probe_time: None,
                last_transition_time: now,
                observed_generation: None,
            },
        ]
    }

    /// Build init container statuses for a running pod (static fallback).
    /// Prefer `runtime.get_init_container_statuses()` which inspects actual Docker state.
    #[allow(dead_code)]
    fn build_init_container_statuses(pod: &Pod) -> Option<Vec<ContainerStatus>> {
        let init_containers = pod.spec.as_ref()?.init_containers.as_ref()?;
        if init_containers.is_empty() {
            return None;
        }
        Some(
            init_containers
                .iter()
                .map(|ic| ContainerStatus {
                    name: ic.name.clone(),
                    ready: true,
                    restart_count: 0,
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 0,
                            signal: None,
                            reason: Some("Completed".to_string()),
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some(ic.image.clone()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: ic.resources.as_ref().and_then(|r| r.requests.clone()),
                    allocated_resources_status: None,
                    resources: ic.resources.clone(),
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                })
                .collect(),
        )
    }

    /// The pod's `status.qosClass`, as upstream `generateAPIPodStatus` sets it
    /// (`apiPodStatus.QOSClass = v1qos.GetPodQOS(pod)`,
    /// `pkg/kubelet/kubelet_pods.go:2097`).
    ///
    /// Delegates to [`rusternetes_common::qos::compute_pod_qos`], the one port
    /// of `ComputePodQOS`, so the class the kubelet publishes, the class the
    /// eviction manager ranks pods by, and the class the api-server wrote at
    /// create time are the same computation.
    fn compute_qos_class(pod: &Pod) -> String {
        rusternetes_common::qos::compute_pod_qos(pod)
            .as_str()
            .to_string()
    }

    async fn update_pod_status(
        &self,
        pod: &Pod,
        phase: Phase,
        reason: Option<&str>,
        message: Option<&str>,
    ) -> Result<()> {
        let key = build_key(
            "pods",
            pod.metadata.namespace.as_deref(),
            &pod.metadata.name,
        );

        // Read the fresh pod from storage so we preserve container_statuses,
        // init_container_statuses, conditions, pod_ip, start_time, etc.
        // Constructing a fresh PodStatus would WIPE those fields — destructive
        // when called on a failure path of a previously-Running pod.
        let mut new_pod = match self.storage.get::<Pod>(&key).await {
            Ok(p) => p,
            Err(_) => pod.clone(),
        };

        // Terminal pod phases are STICKY. If storage already reports the pod
        // as Succeeded/Failed, refuse to regress it to a non-terminal phase
        // (and never rewrite one terminal phase into the other). Without this
        // a Succeeded pod could flap back to Running; the job controller then
        // deletes that Running pod on job completion while its completion
        // index stays counted, dropping the index from the pod listing — the
        // Indexed Job conformance flake (#1048).
        // Upstream parity: pkg/kubelet/kubelet_pods.go:1934-1942
        // generateAPIPodStatus — "pods are not allowed to transition out of
        // terminal phases" forces the computed phase back to the API
        // server's terminal value ("Pod attempted illegal phase transition").
        let current_phase = new_pod.status.as_ref().and_then(|s| s.phase.clone());
        if should_skip_phase_write(current_phase.as_ref(), &phase) {
            debug!(
                "refusing to regress terminal phase of pod {}/{}: storage={:?} requested={:?} (upstream: generateAPIPodStatus illegal-phase-transition guard)",
                pod.metadata.namespace.as_deref().unwrap_or("default"),
                pod.metadata.name,
                current_phase,
                phase,
            );
            return Ok(());
        }

        let original = new_pod.clone();

        let mut status = new_pod.status.take().unwrap_or_default();
        status.phase = Some(phase);
        status.reason = reason.map(|s| s.to_string());
        status.message = message.map(|s| s.to_string());
        new_pod.status = Some(status);

        // Gate the write so a no-op call doesn't emit a MODIFIED watch event.
        if !pod_status_equal(&original, &new_pod) {
            self.storage.update_status(&key, &new_pod).await?;
        }

        Ok(())
    }

    async fn update_pod_status_error(&self, pod: &Pod, error: &str) -> Result<()> {
        self.update_pod_status(pod, Phase::Failed, Some("Error"), Some(error))
            .await
    }

    /// `makeSignalObservations` for the node: nodefs/memory/pids from the
    /// local filesystem plus the `imagefs.*` observation from the image GC
    /// manager's stats provider (CRI `ImageFsInfo`). The imagefs is only
    /// reported separately when it is on another device than the root dir
    /// (`HasDedicatedImageFs`); otherwise `imagefs.*` reads the nodefs numbers.
    async fn observe_node_stats(&self) -> crate::eviction::NodeStats {
        let mut stats = get_node_stats(&self.eviction_root_dir);
        let Some(mgr) = self.image_gc_manager.get() else {
            return stats;
        };
        match mgr.image_fs_stats().await {
            Ok(fs) => {
                use std::os::unix::fs::MetadataExt;
                let root_dev = std::fs::metadata(&self.eviction_root_dir)
                    .ok()
                    .map(|m| m.dev());
                let dedicated = matches!((fs.device, root_dev), (Some(a), Some(b)) if a != b);
                if let (true, Some(avail), Some(total)) =
                    (dedicated, fs.available_bytes, fs.capacity_bytes)
                {
                    stats.imagefs = Some(crate::eviction::FsUsage {
                        available_bytes: avail,
                        total_bytes: total,
                        inodes_free: fs.inodes_free.unwrap_or(0),
                        inodes_total: fs.inodes.unwrap_or(0),
                    });
                }
            }
            Err(err) => debug!(%err, "Eviction manager: failed to get image filesystem stats"),
        }
        stats
    }

    /// Handle pod eviction when node resources are exhausted
    async fn handle_eviction(&self, signals: &[EvictionSignal]) -> Result<()> {
        info!("Handling eviction for signals: {:?}", signals);

        // `byEvictionPriority` + `getReclaimableThreshold`: memory first, then
        // the first remaining signal; try node-level reclaim for it before
        // evicting end-user pods (`eviction_manager.go:376-392`).
        if let Some(signal) = signals
            .iter()
            .find(|s| **s == EvictionSignal::MemoryAvailable)
            .or_else(|| signals.first())
        {
            let stats = self.observe_node_stats().await;
            let funcs =
                crate::eviction::build_signal_to_node_reclaim_funcs(stats.imagefs.is_some(), false)
                    .remove(signal)
                    .unwrap_or_default();
            let reclaimer = KubeletNodeReclaimer { kubelet: self };
            if crate::eviction::reclaim_node_level_resources(&funcs, &reclaimer, |s| {
                self.eviction_manager.lock().unwrap().no_thresholds_met(s)
            })
            .await
            {
                info!(
                    ?signal,
                    "Eviction manager: able to reduce resource pressure without evicting pods."
                );
                return Ok(());
            }
        }

        // Get all pods assigned to this node
        let all_pods_prefix = build_prefix("pods", None);
        let all_pods: Vec<Pod> = self.storage.list(&all_pods_prefix).await?;

        let node_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|p| {
                p.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_ref())
                    .map(|n| n == &self.node_name)
                    .unwrap_or(false)
            })
            .filter(|p| {
                // Only consider running pods for eviction
                p.status
                    .as_ref()
                    .map(|s| s.phase == Some(Phase::Running))
                    .unwrap_or(false)
            })
            .collect();

        // Get pod resource usage statistics
        let pod_stats = get_pod_stats(&node_pods).await;

        // For each active signal, select pods for eviction
        for signal in signals {
            let pods_to_evict = {
                let eviction_manager = self.eviction_manager.lock().unwrap();
                eviction_manager.select_pods_for_eviction(&node_pods, &pod_stats, signal)
            };

            for pod_key in pods_to_evict {
                warn!(
                    "Evicting pod {} due to resource pressure ({:?})",
                    pod_key, signal
                );

                // Parse namespace and name from key
                let parts: Vec<&str> = pod_key.split('/').collect();
                if parts.len() != 2 {
                    continue;
                }
                let namespace = parts[0];
                let name = parts[1];

                // Find the pod
                if let Some(pod) = node_pods.iter().find(|p| {
                    p.metadata.namespace.as_deref().unwrap_or("default") == namespace
                        && p.metadata.name == name
                }) {
                    // Stop the pod (use short grace period for eviction)
                    let grace = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.termination_grace_period_seconds)
                        .unwrap_or(30);
                    if let Err(e) = self.runtime.stop_pod_for(pod, grace).await {
                        error!("Failed to stop evicted pod {}: {}", pod_key, e);
                        continue;
                    }

                    // Update pod status to reflect eviction
                    if let Err(e) = self
                        .update_pod_status(
                            pod,
                            Phase::Failed,
                            Some("Evicted"),
                            Some(&format!(
                                "Pod evicted due to resource pressure: {:?}",
                                signal
                            )),
                        )
                        .await
                    {
                        error!("Failed to update evicted pod status: {}", e);
                    }

                    info!("Successfully evicted pod {}", pod_key);
                }
            }
        }

        Ok(())
    }
}

/// Build the kubelet-managed /etc/hosts content for a pod.
///
/// Returns `None` for `hostNetwork: true` pods — those use the host's
/// /etc/hosts directly and must NOT receive a managed file (upstream
/// `pkg/kubelet/kubelet_pods.go::managedHostsFileContent`).
///
/// For non-host-network pods, the content matches upstream byte-for-byte:
/// - `# Kubernetes-managed hosts file.` header
/// - Standard IPv4/IPv6 localhost + multicast entries
/// - The pod's own IP / hostname (and FQDN if `spec.subdomain` is set)
/// - Every `spec.hostAliases[]` entry, one line per IP with hostnames
///   tab-separated. Aliases with empty/missing hostnames are skipped.
///
/// Mirrors upstream e2e site
/// `test/e2e/common/node/kubelet_etc_hosts.go:143`.
pub fn build_managed_hosts_content(
    pod: &Pod,
    pod_ip: Option<&str>,
    cluster_domain: &str,
) -> Option<String> {
    let spec = pod.spec.as_ref()?;

    // hostNetwork pods use the host's /etc/hosts directly.
    if spec.host_network == Some(true) {
        return None;
    }

    let pod_name = &pod.metadata.name;
    let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

    // Linux hostnames are limited to 63 chars; trim trailing hyphens.
    let raw_hostname = spec.hostname.as_deref().unwrap_or(pod_name);
    let hostname = if raw_hostname.len() > 63 {
        raw_hostname[..63].trim_end_matches('-')
    } else {
        raw_hostname
    };

    // Header + standard entries. IPv6 addresses match upstream
    // `pkg/kubelet/kubelet_pods.go` exactly — do NOT change without
    // verifying the upstream constants.
    let mut content = String::from(
        "# Kubernetes-managed hosts file.\n\
         127.0.0.1\tlocalhost\n\
         ::1\tlocalhost ip6-localhost ip6-loopback\n\
         fe00::0\tip6-localnet\n\
         ff00::0\tip6-mcastprefix\n\
         ff02::1\tip6-allnodes\n\
         ff02::2\tip6-allrouters\n",
    );

    // Pod's own IP entry (when known). Includes the FQDN built from
    // <hostname>.<subdomain>.<namespace>.svc.<cluster-domain> if subdomain set.
    if let Some(ip) = pod_ip {
        let mut aliases = vec![hostname.to_string()];
        if let Some(subdomain) = &spec.subdomain {
            aliases.push(format!(
                "{}.{}.{}.svc.{}",
                hostname, subdomain, namespace, cluster_domain
            ));
        }
        content.push_str(&format!("{}\t{}\n", ip, aliases.join("\t")));
    }

    // spec.hostAliases — one line per IP, hostnames tab-joined.
    // Skip aliases with empty/missing hostnames (upstream behaviour).
    for alias in spec.host_aliases.iter().flatten() {
        let hostnames = match alias.hostnames.as_deref() {
            Some(h) if !h.is_empty() => h,
            _ => continue,
        };
        content.push_str(&format!("{}\t{}\n", alias.ip, hostnames.join("\t")));
    }

    Some(content)
}

/// Create-or-update a metrics.k8s.io object the kubelet publishes.
///
/// Upstream never PUTs metrics objects (metrics-server serves them from
/// memory), so there is nothing to port; the stored-object rule that does apply
/// is `defaultUpdatedObjectInfo.Preconditions`
/// (staging/src/k8s.io/apiserver/pkg/registry/rest/update.go:188-203): a
/// non-empty `metadata.uid` on a PUT becomes a UID precondition. The object is
/// rebuilt every tick with `ObjectMeta::new` (random UID), so on update it
/// adopts the stored uid/creationTimestamp/resourceVersion, as controllers do
/// by updating a `DeepCopy()` of the stored object (#2725).
async fn upsert_metrics<S, T>(
    storage: &S,
    key: &str,
    mut metrics: T,
    meta: impl Fn(&mut T) -> &mut rusternetes_common::types::ObjectMeta,
) where
    S: Storage,
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    match storage.get::<T>(key).await {
        Ok(mut existing) => {
            let stored = meta(&mut existing).clone();
            let m = meta(&mut metrics);
            m.uid = stored.uid;
            m.creation_timestamp = stored.creation_timestamp;
            m.resource_version = stored.resource_version;
            if let Err(e) = storage.update(key, &metrics).await {
                debug!("Failed to update metrics {key}: {e}");
            }
        }
        Err(_) => {
            if let Err(e) = storage.create(key, &metrics).await {
                debug!("Failed to create metrics {key}: {e}");
            }
        }
    }
}

/// The default labels `initialNode` sets (kubelet_node_status.go:304-313;
/// `v1.LabelHostname` plus os/arch and the deprecated beta os/arch).
fn default_node_labels(hostname: &str) -> HashMap<String, String> {
    HashMap::from([
        ("kubernetes.io/hostname".to_string(), hostname.to_string()),
        ("kubernetes.io/os".to_string(), "linux".to_string()),
        ("kubernetes.io/arch".to_string(), "amd64".to_string()),
        ("beta.kubernetes.io/os".to_string(), "linux".to_string()),
        ("beta.kubernetes.io/arch".to_string(), "amd64".to_string()),
    ])
}

/// `Kubelet.updateDefaultLabels` (pkg/kubelet/kubelet_node_status.go:230-267).
fn update_default_labels(
    initial: &HashMap<String, String>,
    existing: &mut Option<HashMap<String, String>>,
) -> bool {
    const DEFAULT_LABELS: [&str; 5] = [
        "kubernetes.io/hostname",
        "kubernetes.io/os",
        "kubernetes.io/arch",
        "beta.kubernetes.io/os",
        "beta.kubernetes.io/arch",
    ];
    let labels = existing.get_or_insert_with(HashMap::new);
    let mut needs_update = false;
    for label in DEFAULT_LABELS {
        let Some(want) = initial.get(label) else {
            continue;
        };
        if labels.get(label).map(String::as_str).unwrap_or("") != want.as_str() {
            labels.insert(label.to_string(), want.clone());
            needs_update = true;
        }
        if labels.get(label).is_some_and(|v| v.is_empty()) {
            labels.remove(label);
        }
    }
    needs_update
}

#[cfg(test)]
mod default_labels_tests {
    use super::*;

    // kubelet_node_status.go:230-267: existing value != initial value is
    // overwritten (needsUpdate), equal is a no-op, empty initial is deleted.
    #[test]
    fn force_overwrites_drifted_default_labels() {
        let initial = default_node_labels("host-a");
        let mut existing = Some(HashMap::from([
            ("kubernetes.io/hostname".to_string(), "stale".to_string()),
            ("kubernetes.io/os".to_string(), "linux".to_string()),
            ("custom".to_string(), "keep".to_string()),
        ]));
        assert!(update_default_labels(&initial, &mut existing));
        let l = existing.unwrap();
        assert_eq!(l["kubernetes.io/hostname"], "host-a");
        assert_eq!(l["kubernetes.io/arch"], "amd64");
        assert_eq!(l["custom"], "keep");
    }

    #[test]
    fn noop_when_already_matching() {
        let initial = default_node_labels("h");
        let mut existing = Some(initial.clone());
        assert!(!update_default_labels(&initial, &mut existing));
        assert_eq!(existing.unwrap(), initial);
    }

    #[test]
    fn empty_initial_value_is_deleted() {
        let mut initial = default_node_labels("h");
        initial.insert("kubernetes.io/os".to_string(), String::new());
        let mut existing = Some(HashMap::from([(
            "kubernetes.io/os".to_string(),
            "windows".to_string(),
        )]));
        assert!(update_default_labels(&initial, &mut existing));
        assert!(!existing.unwrap().contains_key("kubernetes.io/os"));
    }
}

/// The eviction manager's `imageGC` / `containerGC` (`eviction_manager.go`
/// `managerImpl.imageGC`, `.containerGC`) bound to this kubelet.
struct KubeletNodeReclaimer<'a> {
    kubelet: &'a Kubelet,
}

#[async_trait::async_trait]
impl crate::eviction::NodeReclaimer for KubeletNodeReclaimer<'_> {
    async fn delete_all_unused_containers(&self) -> Result<()> {
        // Deviation: upstream `DeleteAllUnusedContainers` GCs with
        // MinAge=0, MaxPerPodContainer=-1, evictNonDeletedPods=true; this runs
        // the regular container GC (tracked as a follow-up).
        self.kubelet.container_gc().await;
        Ok(())
    }

    async fn delete_unused_images(&self) -> Result<()> {
        match self.kubelet.image_gc_manager.get() {
            Some(mgr) => mgr.delete_unused_images().await,
            None => Ok(()),
        }
    }

    async fn observe(&self) -> crate::eviction::NodeStats {
        self.kubelet.observe_node_stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::Kubelet;

    /// Storage that behaves like the api-server Store on PUT: a non-empty
    /// `metadata.uid` that differs from the stored one is a 409 (rest/update.go:188-203).
    struct UidStore(rusternetes_storage::memory::MemoryStorage);

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for UidStore {
        async fn create<T>(&self, k: &str, v: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.0.create(k, v).await
        }
        async fn get<T>(&self, k: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.0.get(k).await
        }
        async fn update<T>(&self, k: &str, v: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            let sent = serde_json::to_value(v).unwrap();
            let stored: serde_json::Value = self.0.get(k).await?;
            let (a, b) = (
                sent["metadata"]["uid"].as_str().unwrap_or(""),
                stored["metadata"]["uid"].as_str().unwrap_or(""),
            );
            if !a.is_empty() && a != b {
                return Err(rusternetes_common::Error::Conflict(format!(
                    "Precondition failed: UID in precondition: {a}, UID in object meta: {b}"
                )));
            }
            self.0.update(k, v).await
        }
        async fn update_raw(
            &self,
            k: &str,
            v: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.0.update_raw(k, v).await
        }
        async fn delete(&self, k: &str) -> rusternetes_common::Result<()> {
            self.0.delete(k).await
        }
        async fn list<T>(&self, p: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.0.list(p).await
        }
        async fn watch(
            &self,
            p: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.0.watch(p).await
        }
        async fn watch_from_revision(
            &self,
            p: &str,
            r: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.0.watch_from_revision(p, r).await
        }
        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.0.current_revision().await
        }
        async fn is_revision_compacted(&self, r: i64) -> rusternetes_common::Result<bool> {
            self.0.is_revision_compacted(r).await
        }
    }

    /// #2725: a second publish carries a fresh random UID; it must adopt the
    /// stored one or the Store 409s every tick.
    #[tokio::test]
    async fn republished_node_metrics_survive_uid_precondition() {
        use rusternetes_common::resources::NodeMetrics;
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        use rusternetes_storage::Storage;
        let st = UidStore(rusternetes_storage::memory::MemoryStorage::new());
        let mk = |cpu: &str| NodeMetrics {
            type_meta: TypeMeta {
                api_version: "metrics.k8s.io/v1beta1".into(),
                kind: "NodeMetrics".into(),
            },
            metadata: ObjectMeta::new("n1"),
            timestamp: chrono::Utc::now(),
            window: "30s".into(),
            usage: [("cpu".to_string(), cpu.to_string())].into(),
        };
        let key = "/registry/metrics.k8s.io/nodes/n1";
        super::upsert_metrics(&st, key, mk("1m"), |m| &mut m.metadata).await;
        super::upsert_metrics(&st, key, mk("2m"), |m| &mut m.metadata).await;
        let got: NodeMetrics = st.get(key).await.unwrap();
        assert_eq!(got.usage["cpu"], "2m");
    }

    /// #2459: deletionTimestamp is the deadline (now+grace), so at the deadline
    /// the grace has elapsed; before it, it has not.
    #[test]
    fn terminating_grace_elapsed_treats_deletion_timestamp_as_deadline() {
        let now = chrono::Utc::now();
        let dl = now + chrono::Duration::seconds(30);
        assert!(!super::terminating_grace_elapsed(Some(dl), now));
        assert!(super::terminating_grace_elapsed(Some(dl), dl));
        assert!(super::terminating_grace_elapsed(
            Some(now - chrono::Duration::seconds(1)),
            now
        ));
        assert!(super::terminating_grace_elapsed(None, now));
    }

    /// A terminal pod with no running container no longer wants its volumes
    /// (`ShouldPodRuntimeBeRemoved`); one still running a container, or a
    /// running pod, does.
    #[test]
    fn pod_volumes_released_only_when_terminal_and_stopped() {
        let mk = |phase: &str, state: serde_json::Value| -> rusternetes_common::resources::Pod {
            serde_json::from_value(serde_json::json!({
                "metadata": {"name": "p", "uid": "u"},
                "spec": {"containers": []},
                "status": {"phase": phase, "containerStatuses": [
                    {"name": "c", "ready": false, "restartCount": 0, "image": "i", "imageID": "", "state": state}
                ]}
            }))
            .unwrap()
        };
        let done = serde_json::json!({"terminated": {"exitCode": 0}});
        let running = serde_json::json!({"running": {}});
        assert!(super::pod_volumes_released(&mk("Succeeded", done.clone())));
        assert!(super::pod_volumes_released(&mk("Failed", done.clone())));
        assert!(!super::pod_volumes_released(&mk("Running", done)));
        assert!(!super::pod_volumes_released(&mk("Failed", running)));
    }

    /// `nodeStatusUpdateFrequency` drives the heartbeat interval; default 10s
    /// when unset/zero (upstream `defaults.go`; consumer
    /// pkg/kubelet/kubelet.go:1852 `wait.JitterUntil(kl.syncNodeStatus,
    /// kl.nodeStatusUpdateFrequency, ...)`).
    #[tokio::test]
    #[serial_test::serial]
    async fn heartbeat_interval_follows_node_status_update_frequency() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let mk = || async {
            Kubelet::new(
                "node-hb".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        let d = mk().await;
        assert_eq!(d.node_status_heartbeat_interval(), Duration::from_secs(10));
        let c = mk()
            .await
            .with_node_status_update_frequency(Some(Duration::from_secs(25)));
        assert_eq!(c.node_status_heartbeat_interval(), Duration::from_secs(25));
        let z = mk()
            .await
            .with_node_status_update_frequency(Some(Duration::ZERO));
        assert_eq!(z.node_status_heartbeat_interval(), Duration::from_secs(10));
    }

    /// `fileCheckFrequency` drives the static-pod file source poll
    /// (kubelet.go:384 -> file.go:93) and a poll publishes manifests to the
    /// cache `sync_loop` reads.
    #[tokio::test]
    #[serial_test::serial]
    async fn file_check_frequency_drives_static_pod_poll() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        let mk = || async {
            Kubelet::new(
                "node-fc".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        let d = mk().await;
        assert_eq!(d.static_pod_poll_interval(), Duration::from_secs(20));
        let z = mk().await.with_file_check_frequency(Some(Duration::ZERO));
        assert_eq!(z.static_pod_poll_interval(), Duration::from_secs(20));
        let k = mk()
            .await
            .with_pod_manifest_path(Some(manifests.clone()))
            .with_file_check_frequency(Some(Duration::from_secs(5)));
        assert_eq!(k.static_pod_poll_interval(), Duration::from_secs(5));

        std::fs::write(
            manifests.join("web.yaml"),
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web\nspec:\n  containers:\n  - name: c\n    image: busybox\n",
        )
        .unwrap();
        assert!(k.static_pods.lock().unwrap().is_empty(), "no poll yet");
        k.poll_static_pods().await;
        assert_eq!(k.static_pods.lock().unwrap().len(), 1);
    }

    /// `httpCheckFrequency` drives the manifest-URL source poll (kubelet.go:390
    /// -> http.go:60) and a poll publishes the URL's pods to the cache
    /// `sync_loop` reads, alongside file-sourced pods.
    #[tokio::test]
    #[serial_test::serial]
    async fn http_check_frequency_drives_static_pod_url_poll() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        std::fs::write(
            manifests.join("web.yaml"),
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web\nspec:\n  containers:\n  - name: c\n    image: busybox\n",
        )
        .unwrap();
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/pod", l.local_addr().unwrap());
        tokio::spawn(async move {
            let body = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: remote\nspec:\n  containers:\n  - name: c\n    image: busybox\n";
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = s.read(&mut buf).await;
            let _ = s
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await;
        });
        let mk = || async {
            Kubelet::new(
                "node-hc".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        let d = mk().await;
        assert_eq!(d.static_pod_url_poll_interval(), Duration::from_secs(20));
        assert!(!d.poll_static_pod_url().await, "no staticPodURL, no source");
        let z = mk().await.with_http_check_frequency(Some(Duration::ZERO));
        assert_eq!(z.static_pod_url_poll_interval(), Duration::from_secs(20));
        let k = mk()
            .await
            .with_pod_manifest_path(Some(manifests))
            .with_http_check_frequency(Some(Duration::from_secs(7)))
            .with_static_pod_url(Some(&url), &Default::default())
            .unwrap();
        assert_eq!(k.static_pod_url_poll_interval(), Duration::from_secs(7));
        k.poll_static_pods().await;
        assert!(k.poll_static_pod_url().await);
        let names: std::collections::BTreeSet<String> =
            k.static_pods.lock().unwrap().keys().cloned().collect();
        assert_eq!(
            names,
            ["remote-node-hc".to_string(), "web-node-hc".to_string()].into()
        );
    }

    /// `runtimeRequestTimeout` reaches the CRI client (upstream passes
    /// `kubeCfg.RuntimeRequestTimeout.Duration` into
    /// `NewRemoteRuntimeService`, pkg/kubelet/kubelet.go:408); unset or zero
    /// takes the 2m default (v1beta1/defaults.go:188-189).
    #[tokio::test]
    #[serial_test::serial]
    async fn runtime_request_timeout_reaches_cri_client() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let mk = || async {
            Kubelet::new(
                "node-rrt".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        assert_eq!(
            mk().await.runtime_request_timeout(),
            Duration::from_secs(120)
        );
        let c = mk()
            .await
            .with_runtime_request_timeout(Some(Duration::from_secs(7)));
        assert_eq!(c.runtime_request_timeout(), Duration::from_secs(7));
        let z = mk()
            .await
            .with_runtime_request_timeout(Some(Duration::ZERO));
        assert_eq!(z.runtime_request_timeout(), Duration::from_secs(120));
    }

    /// `newCrashLoopBackOff` gate matrix (pkg/kubelet/kubelet.go:345-361).
    #[test]
    #[serial_test::serial]
    fn new_crash_loop_backoff_gate_matrix() {
        use super::new_crash_loop_backoff;
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let s = std::time::Duration::from_secs;
        // Defaults (Max on, Reduce off): operator value wins, initial 10s clamped.
        assert_eq!(new_crash_loop_backoff(Some(s(45))), (s(45), s(10)));
        assert_eq!(new_crash_loop_backoff(Some(s(3))), (s(3), s(3)));
        // Max off: operator value is ignored, 300s/10s.
        let _m = with_feature(Feature::KubeletCrashLoopBackOffMax, false);
        assert_eq!(new_crash_loop_backoff(Some(s(45))), (s(300), s(10)));
        // Max off + Reduce on: 60s/1s.
        let _r = with_feature(Feature::ReduceDefaultCrashLoopBackOffDecay, true);
        assert_eq!(new_crash_loop_backoff(None), (s(60), s(1)));
        // Both on: configured value overrides the reduced max; initial stays 1s.
        let _m2 = with_feature(Feature::KubeletCrashLoopBackOffMax, true);
        assert_eq!(new_crash_loop_backoff(Some(s(45))), (s(45), s(1)));
    }

    /// `crashLoopBackOff.maxContainerRestartPeriod` caps the restart backoff and
    /// clamps the initial delay (kubelet.go:345-358).
    #[tokio::test]
    #[serial_test::serial]
    async fn crash_loop_backoff_max_applies() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let mk = || async {
            Kubelet::new(
                "node-clb".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        let d = mk().await;
        assert_eq!(d.crash_loop_backoff_max, Duration::from_secs(300));
        assert_eq!(d.crash_loop_backoff_initial(), Duration::from_secs(10));
        let c = mk()
            .await
            .with_crash_loop_backoff_max(Some(Duration::from_secs(60)));
        assert_eq!(c.crash_loop_backoff_max, Duration::from_secs(60));
        assert_eq!(c.crash_loop_backoff_initial(), Duration::from_secs(10));
        let t = mk()
            .await
            .with_crash_loop_backoff_max(Some(Duration::from_secs(3)));
        assert_eq!(t.crash_loop_backoff_initial(), Duration::from_secs(3));
    }

    /// A kubelet built with explicit `nodeIPs` / `externalCloudProvider`
    /// (`NewMainKubelet`, kubelet.go:428-430, 619, 637), with the
    /// `nodeIPValidator` override kubelet_node_status_test.go uses so the test
    /// IPs need not be on a host interface.
    async fn node_ip_kubelet(
        dir: &tempfile::TempDir,
        storage: std::sync::Arc<rusternetes_storage::StorageBackend>,
        name: &str,
        ips: &[&str],
        external: bool,
    ) -> Kubelet {
        node_ip_kubelet_h(dir, storage, name, None, ips, external).await
    }

    async fn node_ip_kubelet_h(
        dir: &tempfile::TempDir,
        storage: std::sync::Arc<rusternetes_storage::StorageBackend>,
        name: &str,
        hostname: Option<&str>,
        ips: &[&str],
        external: bool,
    ) -> Kubelet {
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let k = Kubelet::new_with_eviction(
            name.into(),
            storage,
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
            dir.path().to_path_buf(),
            crate::eviction::EvictionManager::new(),
            10250,
            Vec::new(),
            ips.iter().map(|s| s.parse().unwrap()).collect(),
            external,
        )
        .await
        .unwrap()
        .with_node_ip_validator(|_| Ok(()));
        let k = match hostname {
            Some(h) => k.with_hostname(h),
            None => k,
        };
        k.update_runtime_up().await;
        k.register_node().await.unwrap();
        k
    }

    async fn get_node(
        storage: &rusternetes_storage::StorageBackend,
        name: &str,
    ) -> rusternetes_common::resources::Node {
        use rusternetes_storage::{build_key, Storage};
        storage.get(&build_key("nodes", None, name)).await.unwrap()
    }

    fn node_addrs(n: &rusternetes_common::resources::Node) -> Vec<(String, String)> {
        n.status
            .as_ref()
            .and_then(|s| s.addresses.clone())
            .unwrap_or_default()
            .into_iter()
            .map(|a| (a.address_type, a.address))
            .collect()
    }

    fn pair(t: &str, a: &str) -> (String, String) {
        (t.to_string(), a.to_string())
    }

    /// initialNode (kubelet_node_status.go:325-333) taints an
    /// externalCloudProvider node with `node.cloudprovider.kubernetes.io/
    /// uninitialized=true:NoSchedule`, and setNodeStatus runs the NodeAddress
    /// setter (setters.go:62-135): provided-node-ip annotation + addresses.
    #[tokio::test]
    #[serial_test::serial]
    async fn external_cloud_provider_node_is_tainted_annotated_and_addressed() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let _k = node_ip_kubelet(&dir, storage.clone(), "node-ext", &["10.1.1.1"], true).await;
        let n = get_node(&storage, "node-ext").await;
        let taints = n.spec.as_ref().unwrap().taints.clone().unwrap_or_default();
        assert!(
            taints.iter().any(
                |t| t.key == "node.cloudprovider.kubernetes.io/uninitialized"
                    && t.value.as_deref() == Some("true")
                    && t.effect == "NoSchedule"
            ),
            "taints: {taints:?}"
        );
        assert_eq!(
            n.metadata.annotations.as_ref().unwrap()["alpha.kubernetes.io/provided-node-ip"],
            "10.1.1.1"
        );
        assert_eq!(
            node_addrs(&n),
            vec![pair("InternalIP", "10.1.1.1"), pair("Hostname", "node-ext")]
        );
    }

    /// Without an external provider there is no taint, no annotation.
    #[tokio::test]
    #[serial_test::serial]
    async fn non_external_node_has_no_cloud_taint_or_annotation() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let _k = node_ip_kubelet(&dir, storage.clone(), "node-int", &["10.1.1.1"], false).await;
        let n = get_node(&storage, "node-int").await;
        assert!(n
            .spec
            .as_ref()
            .and_then(|s| s.taints.clone())
            .unwrap_or_default()
            .is_empty());
        assert!(!n
            .metadata
            .annotations
            .clone()
            .unwrap_or_default()
            .contains_key("alpha.kubernetes.io/provided-node-ip"));
    }

    /// setters.go:104-110: with --cloud-provider=external and addresses
    /// already set, the setter returns early; provider addresses win on every
    /// sync.
    #[tokio::test]
    #[serial_test::serial]
    async fn external_cloud_provider_addresses_survive_status_sync() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage.clone(), "node-prov", &["10.1.1.1"], true).await;
        let key = build_key("nodes", None, "node-prov");
        let mut n = get_node(&storage, "node-prov").await;
        n.status.as_mut().unwrap().addresses = Some(vec![
            rusternetes_common::resources::NodeAddress {
                address_type: "ExternalIP".into(),
                address: "203.0.113.7".into(),
            },
            rusternetes_common::resources::NodeAddress {
                address_type: "InternalIP".into(),
                address: "10.9.9.9".into(),
            },
        ]);
        storage.update_status(&key, &n).await.unwrap();
        k.update_node_status().await.unwrap();
        assert_eq!(
            node_addrs(&get_node(&storage, "node-prov").await),
            vec![
                pair("ExternalIP", "203.0.113.7"),
                pair("InternalIP", "10.9.9.9")
            ]
        );
    }

    /// kubelet_node_status.go:678 + setNodeStatus: the NodeAddress setter
    /// re-runs on EVERY status sync (not memoized for the process lifetime),
    /// overwriting drifted addresses and deleting a stale provided-node-ip
    /// annotation when no longer using an external provider (setters.go:97-101).
    #[tokio::test]
    #[serial_test::serial]
    async fn node_address_setter_reruns_every_status_sync() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage.clone(), "node-sync", &["10.1.1.1"], false).await;
        let key = build_key("nodes", None, "node-sync");
        let mut n = get_node(&storage, "node-sync").await;
        n.status.as_mut().unwrap().addresses =
            Some(vec![rusternetes_common::resources::NodeAddress {
                address_type: "InternalIP".into(),
                address: "9.9.9.9".into(),
            }]);
        n.metadata
            .annotations
            .get_or_insert_with(Default::default)
            .insert(
                "alpha.kubernetes.io/provided-node-ip".into(),
                "9.9.9.9".into(),
            );
        storage.update(&key, &n).await.unwrap();
        k.update_node_status().await.unwrap();
        let n = get_node(&storage, "node-sync").await;
        assert_eq!(
            node_addrs(&n),
            vec![
                pair("InternalIP", "10.1.1.1"),
                pair("Hostname", "node-sync")
            ]
        );
        assert!(!n
            .metadata
            .annotations
            .clone()
            .unwrap_or_default()
            .contains_key("alpha.kubernetes.io/provided-node-ip"));
    }

    /// #3585: `kl.hostname` (nodeutil.GetHostname(--hostname-override)) is
    /// distinct from `nodeName`: the Hostname address (setters.go:128-130) and
    /// the `kubernetes.io/hostname` label (kubelet_node_status.go:306) carry
    /// the hostname, the Node object keeps `nodeName`.
    #[tokio::test]
    #[serial_test::serial]
    async fn hostname_is_distinct_from_node_name() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let _k = node_ip_kubelet_h(
            &dir,
            storage.clone(),
            "node-a",
            Some("host-a"),
            &["10.1.1.1"],
            false,
        )
        .await;
        let n = get_node(&storage, "node-a").await;
        assert_eq!(n.metadata.name, "node-a");
        assert_eq!(
            node_addrs(&n),
            vec![pair("InternalIP", "10.1.1.1"), pair("Hostname", "host-a")]
        );
        assert_eq!(
            n.metadata.labels.as_ref().unwrap()["kubernetes.io/hostname"],
            "host-a"
        );
    }

    /// #3585: setters.go:166-171 looks up `node.Name` only; the pre-existing
    /// `$HOSTNAME` retry is gone.
    #[test]
    #[serial_test::serial]
    fn lookup_node_ips_has_no_hostname_env_fallback() {
        std::env::set_var("HOSTNAME", "localhost");
        let found = Kubelet::lookup_node_ips("no-such-host.invalid");
        std::env::remove_var("HOSTNAME");
        assert!(found.is_empty(), "{found:?}");
    }

    /// #3584: pod hostIPs come from the node's status addresses
    /// (getHostIPsAnyWay -> GetNodeHostIPs), no invented fallback IP.
    #[tokio::test]
    #[serial_test::serial]
    async fn host_ips_follow_node_addresses_without_invented_fallback() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let ip = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
        let k = node_ip_kubelet(&dir, storage.clone(), "node-s", &["10.1.1.1"], false).await;
        assert_eq!(k.get_host_ips_any_way().unwrap(), vec![ip("10.1.1.1")]);
        let k = node_ip_kubelet(
            &dir,
            storage.clone(),
            "node-d",
            &["10.1.1.2", "fd00::2"],
            false,
        )
        .await;
        assert_eq!(
            k.get_host_ips_any_way().unwrap(),
            vec![ip("10.1.1.2"), ip("fd00::2")]
        );
        let (host_ip, host_ips) = k.host_ip_fields();
        assert_eq!(host_ip.as_deref(), Some("10.1.1.2"));
        let host_ips: Vec<String> = host_ips.unwrap().into_iter().map(|h| h.ip).collect();
        assert_eq!(
            host_ips,
            vec!["10.1.1.2".to_string(), "fd00::2".to_string()]
        );
        // --cloud-provider=external without --node-ip: Hostname only
        // (setters.go:130-135) => host IP unknown, hostIP left unset.
        let k = node_ip_kubelet(&dir, storage.clone(), "node-x", &[], true).await;
        let err = k.get_host_ips_any_way().unwrap_err();
        assert!(
            err.starts_with("host IP unknown; known addresses: "),
            "{err}"
        );
        let (host_ip, host_ips) = k.host_ip_fields();
        assert!(host_ip.is_none() && host_ips.is_none());
    }

    /// #3542: nodeIPs live on the Kubelet (createAndInitKubelet ->
    /// NewMainKubelet `nodeIPs`), not in process-global state: two kubelets in
    /// one process advertise their own `--node-ip`.
    #[tokio::test]
    #[serial_test::serial]
    async fn node_ips_are_per_kubelet_not_process_global() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let _a = node_ip_kubelet(&dir, storage.clone(), "node-a", &["10.1.1.1"], false).await;
        let _b = node_ip_kubelet(&dir, storage.clone(), "node-b", &["10.1.1.2"], false).await;
        assert_eq!(
            node_addrs(&get_node(&storage, "node-a").await),
            vec![pair("InternalIP", "10.1.1.1"), pair("Hostname", "node-a")]
        );
        assert_eq!(
            node_addrs(&get_node(&storage, "node-b").await),
            vec![pair("InternalIP", "10.1.1.2"), pair("Hostname", "node-b")]
        );
    }

    use std::time::Duration;

    async fn status_gate_kubelet(
        dir: &tempfile::TempDir,
        storage: std::sync::Arc<rusternetes_storage::StorageBackend>,
    ) -> Kubelet {
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let k = Kubelet::new(
            "node-gate".into(),
            storage,
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
        )
        .await
        .unwrap();
        k.update_runtime_up().await;
        k.register_node().await.unwrap();
        k
    }

    async fn ready_heartbeat(
        storage: &rusternetes_storage::StorageBackend,
    ) -> chrono::DateTime<chrono::Utc> {
        use rusternetes_storage::{build_key, Storage};
        let n: rusternetes_common::resources::Node = storage
            .get(&build_key("nodes", None, "node-gate"))
            .await
            .unwrap();
        n.status
            .unwrap()
            .conditions
            .unwrap()
            .into_iter()
            .find(|c| c.condition_type == "Ready")
            .unwrap()
            .last_heartbeat_time
            .unwrap()
    }

    async fn backdate_ready_heartbeat(
        storage: &rusternetes_storage::StorageBackend,
        secs: i64,
    ) -> chrono::DateTime<chrono::Utc> {
        use rusternetes_storage::{build_key, Storage};
        let key = build_key("nodes", None, "node-gate");
        let mut n: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        let old = chrono::Utc::now() - chrono::Duration::seconds(secs);
        for c in n.status.as_mut().unwrap().conditions.as_mut().unwrap() {
            c.last_heartbeat_time = Some(old);
        }
        storage.update_status(&key, &n).await.unwrap();
        ready_heartbeat(storage).await
    }

    /// tryUpdateNodeStatus (kubelet_node_status.go:483-531): a node whose
    /// status has not changed is NOT rewritten on each heartbeat, only once
    /// `nodeStatusReportFrequency` has elapsed. (Before #2520 every heartbeat
    /// older than 10s rewrote the Node.)
    #[tokio::test]
    #[serial_test::serial]
    async fn unchanged_node_status_is_not_rewritten_within_report_frequency() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage.clone()).await;
        // First report after start always goes out (zero lastStatusReportTime).
        k.update_node_status().await.unwrap();
        let old = backdate_ready_heartbeat(&storage, 30).await;
        // Nothing changed and well inside nodeStatusReportFrequency (5m): skip.
        k.update_node_status().await.unwrap();
        assert_eq!(ready_heartbeat(&storage).await, old);
    }

    /// A quiet node still gets a status refresh once
    /// `nodeStatusReportFrequency` has elapsed (isUpdateStatusPeriodExpired,
    /// kubelet_node_status.go:535), so a skipped heartbeat never starves
    /// consumers of the Ready condition's `lastHeartbeatTime`.
    #[tokio::test]
    #[serial_test::serial]
    async fn quiet_node_is_refreshed_after_report_frequency() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage.clone())
            .await
            .with_node_status_report_frequency(Some(Duration::from_secs(300)));
        k.update_node_status().await.unwrap();
        let old = backdate_ready_heartbeat(&storage, 30).await;
        // Just under the period (no delay): still skipped.
        k.set_last_status_report_for_test(Duration::from_secs(299));
        k.update_node_status().await.unwrap();
        assert_eq!(ready_heartbeat(&storage).await, old);
        // Period elapsed: refreshed.
        k.set_last_status_report_for_test(Duration::from_secs(300));
        k.update_node_status().await.unwrap();
        assert!(ready_heartbeat(&storage).await > old + chrono::Duration::seconds(20));
    }

    /// A real status change is written immediately, not held for the report
    /// period (`changed || isUpdateStatusPeriodExpired`, :493-496).
    #[tokio::test]
    #[serial_test::serial]
    async fn changed_node_status_is_written_immediately() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage.clone()).await;
        k.update_node_status().await.unwrap();
        let old = backdate_ready_heartbeat(&storage, 30).await;
        let key = build_key("nodes", None, "node-gate");
        let mut n: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        n.status.as_mut().unwrap().daemon_endpoints = None;
        storage.update_status(&key, &n).await.unwrap();
        k.update_node_status().await.unwrap();
        assert!(ready_heartbeat(&storage).await > old + chrono::Duration::seconds(20));
    }

    /// #3261: NoExecute eviction belongs to kube-controller-manager's
    /// taint-eviction controller alone. Upstream's kubelet has no taint
    /// handling in its node-status path: `tryUpdateNodeStatus`
    /// (pkg/kubelet/kubelet_node_status.go:483) only builds and patches node
    /// status, and `grep -rni noexecute pkg/kubelet/kubelet_node_status.go`
    /// has no match. A kubelet heartbeat must therefore leave pods on a
    /// NoExecute-tainted node untouched (running or terminal alike).
    #[tokio::test]
    #[serial_test::serial]
    async fn node_status_sync_does_not_evict_for_noexecute_taints() {
        use rusternetes_common::resources::{Node, Pod};
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage.clone()).await;

        let nkey = build_key("nodes", None, "node-gate");
        let mut n: Node = storage.get(&nkey).await.unwrap();
        n.spec.as_mut().unwrap().taints = Some(vec![serde_json::from_value(serde_json::json!({
            "key": "t", "effect": "NoExecute",
            "timeAdded": chrono::Utc::now().to_rfc3339()
        }))
        .unwrap()]);
        storage.update(&nkey, &n).await.unwrap();

        for (name, phase) in [("running", "Running"), ("done", "Succeeded")] {
            let pod: Pod = serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": name, "namespace": "default", "uid": format!("u-{name}")},
                "spec": {"nodeName": "node-gate", "containers": [{"name": "c", "image": "i"}]},
                "status": {"phase": phase}
            }))
            .unwrap();
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        k.update_node_status().await.unwrap();

        for name in ["running", "done"] {
            let pod: Pod = storage
                .get(&build_key("pods", Some("default"), name))
                .await
                .unwrap();
            assert!(
                pod.metadata.deletion_timestamp.is_none(),
                "kubelet must not evict pod {name}: eviction is the taint-eviction controller's job"
            );
            assert_ne!(
                pod.status.as_ref().and_then(|s| s.reason.as_deref()),
                Some("Evicted"),
                "pod {name}"
            );
        }
    }

    /// `nodeStatusReportFrequency` is configurable and defaults to 5m
    /// (v1beta1/defaults.go).
    #[tokio::test]
    #[serial_test::serial]
    async fn report_frequency_builder_defaults_to_five_minutes() {
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(rusternetes_storage::StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage).await;
        assert_eq!(k.node_status_report_frequency, Duration::from_secs(300));
        let k = k.with_node_status_report_frequency(Some(Duration::from_secs(42)));
        assert_eq!(k.node_status_report_frequency, Duration::from_secs(42));
        let k = k.with_node_status_report_frequency(Some(Duration::ZERO));
        assert_eq!(k.node_status_report_frequency, Duration::from_secs(300));
    }

    /// #1929: a kubelet whose CRI endpoint is an absent socket must report
    /// `Ready=False`/`KubeletNotReady`, both at registration and on every
    /// heartbeat (upstream `nodestatus.ReadyCondition`,
    /// pkg/kubelet/nodestatus/setters.go:469-513; `updateRuntimeUp`,
    /// pkg/kubelet/kubelet.go:3113-3157).
    #[tokio::test]
    #[serial_test::serial]
    async fn node_is_not_ready_when_the_cri_runtime_is_absent() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = Kubelet::new(
            "node-x".into(),
            storage.clone(),
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
        )
        .await
        .unwrap();
        k.update_runtime_up().await;
        k.register_node().await.unwrap();
        let ready = |n: &rusternetes_common::resources::Node| {
            n.status
                .as_ref()
                .unwrap()
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .find(|c| c.condition_type == "Ready")
                .cloned()
                .unwrap()
        };
        let key = build_key("nodes", None, "node-x");
        let node: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        let c = ready(&node);
        assert_eq!(c.status, "False");
        assert_eq!(c.reason.as_deref(), Some("KubeletNotReady"));
        assert!(
            c.message.as_deref().unwrap().contains("container runtime"),
            "message must name the runtime: {:?}",
            c.message
        );
        // The heartbeat must not flip it back to True.
        k.update_node_status().await.unwrap();
        let node: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        assert_eq!(ready(&node).status, "False");
    }

    /// #2591: the CSI plugin's `SetKubeletError` must surface in the node's
    /// Ready condition at the kubelet level, ordered runtime, network, storage
    /// (`errs := []error{runtimeErrorsFunc(), networkErrorsFunc(),
    /// storageErrorsFunc(), ...}`, pkg/kubelet/nodestatus/setters.go:491) and
    /// joined by `utilerrors.NewAggregate` (setters.go:505-513). The CRI
    /// socket is absent so the runtime error is the first aggregate member;
    /// the CSINode error must follow it and vanish once cleared.
    /// Regression guard: the behaviour already exists (#2590).
    #[tokio::test]
    #[serial_test::serial]
    async fn csinode_storage_error_gates_ready_condition() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = Kubelet::new(
            "node-csi".into(),
            storage.clone(),
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
        )
        .await
        .unwrap();
        k.update_runtime_up().await;
        k.set_kubelet_error(Some("CSINode is not yet initialized".into()));
        let errs = k.ready_errors();
        assert_eq!(
            errs.last().map(String::as_str),
            Some("CSINode is not yet initialized"),
            "storage error is last: {errs:?}"
        );
        assert!(errs.len() >= 2, "runtime error precedes storage: {errs:?}");

        k.register_node().await.unwrap();
        let key = build_key("nodes", None, "node-csi");
        let ready = |n: &rusternetes_common::resources::Node| {
            n.status
                .as_ref()
                .unwrap()
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .find(|c| c.condition_type == "Ready")
                .cloned()
                .unwrap()
        };
        let node: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        let c = ready(&node);
        assert_eq!(c.status, "False");
        assert_eq!(c.reason.as_deref(), Some("KubeletNotReady"));
        let msg = c.message.unwrap();
        assert!(msg.contains("CSINode is not yet initialized"), "{msg}");
        assert!(
            msg.find("container runtime").unwrap() < msg.find("CSINode").unwrap(),
            "runtime before storage: {msg}"
        );

        // Cleared (CSINode initialised): the heartbeat drops the message.
        k.set_kubelet_error(None);
        k.update_node_status().await.unwrap();
        let node: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        let c = ready(&node);
        assert!(!c.message.unwrap().contains("CSINode"));
    }

    fn resources(pairs: &[(&str, &str)]) -> Option<std::collections::HashMap<String, String>> {
        Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    /// `TestMachineInfo` "extended resources not present in capacity are
    /// removed from allocatable" (nodestatus/setters_test.go:785-812), and an
    /// extended resource added to capacity becomes allocatable.
    #[test]
    fn allocatable_follows_capacity() {
        let mut status = rusternetes_common::resources::NodeStatus {
            capacity: resources(&[("cpu", "2"), ("pods", "110")]),
            allocatable: resources(&[("example.com/extended", "1"), ("cpu", "1")]),
            ..Default::default()
        };
        assert!(super::sync_allocatable_with_capacity(&mut status));
        assert_eq!(
            status.allocatable,
            resources(&[("cpu", "2"), ("pods", "110")])
        );
        assert!(!super::sync_allocatable_with_capacity(&mut status));

        status
            .capacity
            .as_mut()
            .unwrap()
            .insert("scheduling.k8s.io/foo".into(), "5".into());
        assert!(super::sync_allocatable_with_capacity(&mut status));
        assert_eq!(
            status.allocatable,
            resources(&[
                ("cpu", "2"),
                ("pods", "110"),
                ("scheduling.k8s.io/foo", "5")
            ])
        );
    }
    use rusternetes_common::resources::pod::{PodCondition, PodSpec};
    use rusternetes_common::resources::{
        Container, ContainerState, ContainerStateRunning, ContainerStateTerminated,
        ContainerStateWaiting, ContainerStatus, Pod, PodStatus,
    };
    use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};

    #[test]
    fn start_time_is_preserved_once_set() {
        use chrono::{Duration, Utc};
        let now = Utc::now();
        let earlier = now - Duration::seconds(100);
        // An already-set startTime is kept across status rebuilds, so
        // activeDeadlineSeconds elapsed keeps growing instead of resetting to ~0
        // every sync (which would mean the deadline is never reached).
        assert_eq!(super::preserved_start_time(Some(earlier), now), earlier);
        // First Running write (no prior startTime) stamps now.
        assert_eq!(super::preserved_start_time(None, now), now);
    }

    #[test]
    fn active_deadline_elapsed_matches_upstream_boundary() {
        use chrono::{Duration, Utc};

        let now = Utc::now();
        let status = PodStatus {
            start_time: Some(now - Duration::seconds(5)),
            ..Default::default()
        };

        assert_eq!(
            super::active_deadline_elapsed(Some(&status), 5, now),
            Some(5)
        );
        assert_eq!(super::active_deadline_elapsed(Some(&status), 6, now), None);
    }

    #[test]
    fn active_deadline_elapsed_requires_start_time() {
        let status = PodStatus {
            start_time: None,
            ..Default::default()
        };

        assert_eq!(
            super::active_deadline_elapsed(Some(&status), 5, chrono::Utc::now()),
            None
        );
        assert_eq!(
            super::active_deadline_elapsed(None, 5, chrono::Utc::now()),
            None
        );
    }

    #[test]
    fn deadline_exceeded_failed_pod_requires_termination_even_with_restart_always() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("deadline-pod").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Always".to_string()),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Failed),
                reason: Some(super::ACTIVE_DEADLINE_REASON.to_string()),
                ..Default::default()
            }),
        };

        assert!(super::terminal_phase_requires_termination(&pod));
    }

    #[test]
    fn ordinary_failed_pod_with_restart_always_does_not_force_termination() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("non-deadline-failed").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Always".to_string()),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Failed),
                reason: Some("CreateContainerError".to_string()),
                ..Default::default()
            }),
        };

        assert!(!super::terminal_phase_requires_termination(&pod));
    }

    #[test]
    fn restart_never_failed_init_is_terminal() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("init-fail").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Never".to_string()),
                init_containers: Some(vec![Container {
                    name: "init1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }]),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                init_container_statuses: Some(vec![ContainerStatus {
                    name: "init1".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 1,
                            reason: Some("Error".to_string()),
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                            signal: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some("busybox:latest".to_string()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                ..Default::default()
            }),
        };

        assert!(super::init_container_failed_terminally(
            &pod,
            pod.status
                .as_ref()
                .and_then(|status| status.init_container_statuses.as_deref())
        ));
    }

    // Regression: a restartPolicy=Never pod with NO init containers must not be
    // classified as a terminal init failure. A transient app-container start
    // error (e.g. CRI contention) on such a pod previously took the
    // `restart_policy == "Never"` fast-fail branch and was reported as
    // Failed/"Init container failed"; the start-error handler now keys off
    // init_container_failed_terminally, which is false here, so the pod stays
    // Pending and retries. Node-conformance "should use the image defaults if
    // command and args are blank" flaked on exactly this.
    #[test]
    fn restart_never_no_init_containers_is_not_terminal_init_failure() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("no-init").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Never".to_string()),
                containers: vec![Container {
                    name: "agnhost-container".to_string(),
                    image: "registry.k8s.io/e2e-test-images/agnhost:2.59".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            // App container still being created — no init statuses at all.
            status: Some(PodStatus {
                container_statuses: Some(vec![ContainerStatus {
                    name: "agnhost-container".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState {
                        waiting: Some(ContainerStateWaiting {
                            reason: Some("ContainerCreating".to_string()),
                            message: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some("registry.k8s.io/e2e-test-images/agnhost:2.59".to_string()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                ..Default::default()
            }),
        };

        assert!(!super::init_container_failed_terminally(
            &pod,
            pod.status
                .as_ref()
                .and_then(|status| status.init_container_statuses.as_deref())
        ));
    }

    // Regression for NodeConformance "[sig-node] InitContainer should not start
    // app containers if init containers fail on a RestartAlways pod": while a
    // plain init container has failed (and, being RestartAlways, will retry),
    // the app container must report Waiting{PodInitializing}, not
    // ContainerCreating. Previously the start-error handler stamped
    // ContainerCreating + the init-failure message onto app containers.
    #[test]
    fn app_container_reason_is_podinitializing_while_init_incomplete() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("init-fail-restart-always").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Always".to_string()),
                init_containers: Some(vec![Container {
                    name: "init1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }]),
                containers: vec![Container {
                    name: "run1".to_string(),
                    image: "registry.k8s.io/pause:3.10.1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        // init1 failed (exit 1) — not yet completed successfully.
        let init_statuses = vec![ContainerStatus {
            name: "init1".to_string(),
            ready: false,
            restart_count: 0,
            state: Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    exit_code: 1,
                    reason: Some("Error".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                    signal: None,
                }),
                ..Default::default()
            }),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(false),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }];
        assert_eq!(
            super::app_container_waiting_reason(&pod, Some(&init_statuses)),
            "PodInitializing",
            "app container blocked behind a failed init container must be PodInitializing"
        );
    }

    // Once every init container has completed successfully, a genuine
    // app-container start error keeps the ContainerCreating reason.
    #[test]
    fn app_container_reason_is_containercreating_when_init_done() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("init-done").with_namespace("default"),
            spec: Some(PodSpec {
                init_containers: Some(vec![Container {
                    name: "init1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }]),
                containers: vec![Container {
                    name: "run1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        let init_statuses = vec![ContainerStatus {
            name: "init1".to_string(),
            ready: true,
            restart_count: 0,
            state: Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    exit_code: 0,
                    reason: Some("Completed".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                    signal: None,
                }),
                ..Default::default()
            }),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }];
        assert_eq!(
            super::app_container_waiting_reason(&pod, Some(&init_statuses)),
            "ContainerCreating"
        );
    }

    // A pod with no init containers is unaffected.
    #[test]
    fn app_container_reason_no_init_is_containercreating() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("no-init").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "run1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        assert_eq!(
            super::app_container_waiting_reason(&pod, None),
            "ContainerCreating"
        );
    }

    /// #3901: the create-error Pending write is a generateAPIPodStatus
    /// output: conditions the kubelet does not own survive
    /// (kubelet_pods.go:1957-1962), startTime is not reset, and a terminal
    /// API phase is not left (kubelet_pods.go:1937-1945).
    #[tokio::test]
    #[serial_test::serial]
    async fn create_error_status_is_generated_from_prior_status() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage, "node-ce", &["10.1.1.1"], false).await;
        let t0 = chrono::Utc::now() - chrono::Duration::hours(1);
        let mut pod = Pod::new("ce", Default::default());
        pod.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            start_time: Some(t0),
            conditions: Some(vec![rusternetes_common::resources::PodCondition {
                condition_type: "DisruptionTarget".into(),
                status: "True".into(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            ..Default::default()
        });
        let st = k.create_error_pod_status(
            &pod,
            "CreateContainerError".into(),
            "boom".into(),
            None,
            None,
            "BestEffort".into(),
        );
        assert_eq!(st.start_time, Some(t0), "startTime must not reset");
        assert!(st
            .conditions
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|c| c.condition_type == "DisruptionTarget"));
        assert_eq!(st.reason.as_deref(), Some("CreateContainerError"));

        pod.status.as_mut().unwrap().phase = Some(Phase::Failed);
        let st = k.create_error_pod_status(
            &pod,
            "CreateContainerError".into(),
            "boom".into(),
            None,
            None,
            "BestEffort".into(),
        );
        assert_eq!(st.phase, Some(Phase::Failed), "terminal phase is sticky");
    }

    /// #3687: every status write goes through the host-IP block of
    /// `generateAPIPodStatus` (kubelet_pods.go:2005-2040): a hostNetwork pod's
    /// init-failure status inherits the node IPs as PodIPs too.
    #[tokio::test]
    #[serial_test::serial]
    async fn init_failure_status_applies_host_ips_to_host_network_pod() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage, "node-hn", &["10.1.1.1"], false).await;
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("hn").with_namespace("default"),
            spec: Some(PodSpec {
                host_network: Some(true),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        let st = k
            .init_failure_status(&pod, None, Phase::Failed, "FailedToStart", None)
            .await;
        assert_eq!(st.host_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_i_ps.unwrap()[0].ip, "10.1.1.1");
    }

    fn host_network_pod_for_3687(name: &str) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace("default"),
            spec: Some(PodSpec {
                host_network: Some(true),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        }
    }

    /// #3687: the app-container start-error FailedToStart status goes through
    /// the host-IP block of `generateAPIPodStatus` (kubelet_pods.go:2005-2040).
    #[tokio::test]
    #[serial_test::serial]
    async fn app_start_error_status_applies_host_ips_to_host_network_pod() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage, "node-hn", &["10.1.1.1"], false).await;
        let mut pod = host_network_pod_for_3687("hn-start-err");
        k.apply_app_start_error_status(&mut pod, None, "boom", false, "BestEffort".to_string())
            .await;
        let st = pod.status.unwrap();
        assert_eq!(st.reason.as_deref(), Some("FailedToStart"));
        assert_eq!(st.host_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_i_ps.unwrap()[0].ip, "10.1.1.1");
    }

    /// #3687: the liveness-restart fallback (no prior status) also gets the
    /// host-IP block.
    #[tokio::test]
    #[serial_test::serial]
    async fn liveness_restart_status_applies_host_ips_to_host_network_pod() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage, "node-hn", &["10.1.1.1"], false).await;
        let mut pod = host_network_pod_for_3687("hn-liveness");
        k.apply_liveness_restart_status(&mut pod).await;
        let st = pod.status.unwrap();
        assert_eq!(st.reason.as_deref(), Some("Restarting"));
        assert_eq!(st.host_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_i_ps.unwrap()[0].ip, "10.1.1.1");
    }

    #[test]
    fn build_init_failure_status_populates_conditions_without_prior_status() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("init-pod").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Always".to_string()),
                init_containers: Some(vec![
                    Container {
                        name: "init1".to_string(),
                        image: "busybox:latest".to_string(),
                        ..Default::default()
                    },
                    Container {
                        name: "init2".to_string(),
                        image: "busybox:latest".to_string(),
                        ..Default::default()
                    },
                ]),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };

        let init_statuses = Some(vec![
            ContainerStatus {
                name: "init1".to_string(),
                ready: false,
                restart_count: 0,
                state: Some(ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code: 1,
                        reason: Some("Error".to_string()),
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                        signal: None,
                    }),
                    ..Default::default()
                }),
                last_state: None,
                image: Some("busybox:latest".to_string()),
                image_id: None,
                container_id: None,
                started: Some(false),
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            },
            ContainerStatus {
                name: "init2".to_string(),
                ready: false,
                restart_count: 0,
                state: Some(ContainerState {
                    waiting: Some(ContainerStateWaiting {
                        reason: Some("PodInitializing".to_string()),
                        message: None,
                    }),
                    ..Default::default()
                }),
                last_state: None,
                image: Some("busybox:latest".to_string()),
                image_id: None,
                container_id: None,
                started: Some(false),
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            },
        ]);

        let status = Kubelet::build_init_failure_status(
            &pod,
            (
                Some("10.0.0.1".to_string()),
                Some(vec![rusternetes_common::resources::pod::HostIP {
                    ip: "10.0.0.1".to_string(),
                }]),
            ),
            init_statuses,
            Phase::Pending,
            "PodInitializing",
            Some("Burstable".to_string()),
        );

        assert_eq!(status.phase, Some(Phase::Pending));
        assert_eq!(status.reason.as_deref(), Some("PodInitializing"));
        assert_eq!(
            status.message.as_deref(),
            Some("containers with incomplete status: [init1 init2]")
        );
        let initialized = status
            .conditions
            .as_ref()
            .and_then(|conditions| {
                conditions
                    .iter()
                    .find(|c| c.condition_type == "Initialized")
            })
            .expect("Initialized condition must be present");
        assert_eq!(initialized.status, "False");
        assert_eq!(
            initialized.reason.as_deref(),
            Some("ContainersNotInitialized")
        );
        let app_status = status
            .container_statuses
            .as_ref()
            .and_then(|statuses| statuses.iter().find(|s| s.name == "app"))
            .expect("app container status must be present");
        assert!(matches!(
            app_status.state,
            Some(ContainerState { waiting: Some(ContainerStateWaiting {
                reason: Some(ref reason),
                ..
            }), .. }) if reason == "PodInitializing"
        ));
    }

    #[test]
    fn terminal_finalize_backoff_grows_then_caps() {
        use super::{
            terminal_finalize_backoff, TERMINAL_FINALIZE_BACKOFF_INITIAL,
            TERMINAL_FINALIZE_BACKOFF_MAX,
        };
        // First failure → initial; each subsequent doubles.
        assert_eq!(
            terminal_finalize_backoff(1),
            TERMINAL_FINALIZE_BACKOFF_INITIAL
        );
        assert_eq!(
            terminal_finalize_backoff(2),
            TERMINAL_FINALIZE_BACKOFF_INITIAL * 2
        );
        assert_eq!(
            terminal_finalize_backoff(3),
            TERMINAL_FINALIZE_BACKOFF_INITIAL * 4
        );
        // Monotonic non-decreasing and never above the cap.
        let mut prev = terminal_finalize_backoff(1);
        for c in 2..=50u32 {
            let d = terminal_finalize_backoff(c);
            assert!(d >= prev, "backoff must not shrink");
            assert!(
                d <= TERMINAL_FINALIZE_BACKOFF_MAX,
                "backoff must stay capped"
            );
            prev = d;
        }
        assert_eq!(terminal_finalize_backoff(50), TERMINAL_FINALIZE_BACKOFF_MAX);
        // count 0 (defensive) must not panic and stays at the initial delay.
        assert_eq!(
            terminal_finalize_backoff(0),
            TERMINAL_FINALIZE_BACKOFF_INITIAL
        );
    }

    fn cond(t: &str, status: &str, transition: chrono::DateTime<chrono::Utc>) -> PodCondition {
        PodCondition {
            condition_type: t.to_string(),
            status: status.to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(transition),
            observed_generation: None,
        }
    }

    fn make_node_lease(node_name: &str) -> rusternetes_common::resources::Lease {
        rusternetes_common::resources::Lease::new(node_name, "kube-node-lease").with_spec(
            rusternetes_common::resources::LeaseSpec {
                holder_identity: Some(node_name.to_string()),
                lease_duration_seconds: Some(40),
                acquire_time: Some(chrono::Utc::now()),
                renew_time: Some(chrono::Utc::now()),
                lease_transitions: Some(0),
                preferred_holder: None,
                strategy: None,
            },
        )
    }

    #[test]
    fn apply_node_lease_owner_ref_sets_canonical_reference() {
        let mut lease = make_node_lease("node-a");
        assert!(lease.metadata.owner_references.is_none());

        let mutated = Kubelet::apply_node_lease_owner_ref(
            &mut lease,
            "node-a",
            "11111111-2222-3333-4444-555555555555",
        );

        assert!(mutated, "first apply must mutate metadata");
        let refs = lease
            .metadata
            .owner_references
            .as_ref()
            .expect("owner_references should be set");
        assert_eq!(refs.len(), 1, "exactly one owner reference is allowed");
        let owner = &refs[0];
        assert_eq!(owner.api_version, "v1");
        assert_eq!(owner.kind, "Node");
        assert_eq!(owner.name, "node-a");
        assert_eq!(owner.uid, "11111111-2222-3333-4444-555555555555");
        // Per upstream nodelease/controller.go: neither controller nor
        // blockOwnerDeletion is set — the test only requires the four
        // identity fields.
        assert!(owner.controller.is_none());
        assert!(owner.block_owner_deletion.is_none());
    }

    #[test]
    fn apply_node_lease_owner_ref_is_idempotent_when_already_correct() {
        let mut lease = make_node_lease("node-b");
        let _ = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-b", "uid-b");

        let mutated_again = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-b", "uid-b");

        assert!(
            !mutated_again,
            "second apply with identical owner must not mutate metadata"
        );
        assert_eq!(
            lease
                .metadata
                .owner_references
                .as_ref()
                .map(Vec::len)
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn apply_node_lease_owner_ref_backfills_stale_uid() {
        let mut lease = make_node_lease("node-c");
        lease.metadata.owner_references =
            Some(vec![rusternetes_common::types::OwnerReference::new(
                "v1", "Node", "node-c", "old-uid",
            )]);

        let mutated = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-c", "new-uid");

        assert!(mutated, "stale UID must be overwritten");
        let refs = lease.metadata.owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].uid, "new-uid");
    }

    #[test]
    fn apply_node_lease_owner_ref_noop_for_empty_uid() {
        let mut lease = make_node_lease("node-d");
        let mutated = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-d", "");
        assert!(!mutated);
        assert!(
            lease.metadata.owner_references.is_none(),
            "empty UID must never produce an invalid owner reference"
        );
    }

    #[test]
    fn merge_pod_conditions_preserves_time_when_status_unchanged() {
        let stamp_old = chrono::Utc::now() - chrono::Duration::seconds(60);
        let existing = vec![
            cond("Ready", "True", stamp_old),
            cond("Initialized", "True", stamp_old),
        ];
        // Incoming carries a fresh "now" — must be discarded for matching entries.
        let stamp_new = chrono::Utc::now();
        let incoming = vec![
            cond("Ready", "True", stamp_new),
            cond("Initialized", "True", stamp_new),
        ];

        let merged = Kubelet::merge_pod_conditions(&existing, incoming);

        assert_eq!(merged[0].last_transition_time, Some(stamp_old));
        assert_eq!(merged[1].last_transition_time, Some(stamp_old));
    }

    #[test]
    fn merge_pod_conditions_keeps_fresh_time_on_status_flip() {
        let stamp_old = chrono::Utc::now() - chrono::Duration::seconds(60);
        let stamp_new = chrono::Utc::now();
        let existing = vec![cond("Ready", "False", stamp_old)];
        let incoming = vec![cond("Ready", "True", stamp_new)];

        let merged = Kubelet::merge_pod_conditions(&existing, incoming);

        assert_eq!(merged[0].last_transition_time, Some(stamp_new));
    }

    #[test]
    fn merge_pod_conditions_passes_new_conditions_through() {
        let stamp_new = chrono::Utc::now();
        let merged = Kubelet::merge_pod_conditions(&[], vec![cond("Ready", "True", stamp_new)]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].last_transition_time, Some(stamp_new));
    }

    fn make_container(name: &str) -> Container {
        Container {
            name: name.to_string(),
            image: "nginx:latest".to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            command: None,
            args: None,
            ports: None,
            env: None,
            volume_mounts: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            resources: None,
            working_dir: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            env_from: None,
            volume_devices: None,
            ..Default::default()
        }
    }

    fn make_pod(name: &str, namespace: &str, resource_version: Option<&str>) -> Pod {
        let mut meta = ObjectMeta::new(name).with_namespace(namespace);
        if let Some(rv) = resource_version {
            meta.resource_version = Some(rv.to_string());
        }
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: meta,
            spec: Some(PodSpec {
                containers: vec![make_container("app")],
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: Some("Always".to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            }),
            status: None,
        }
    }

    fn make_running_container_status(name: &str) -> ContainerStatus {
        ContainerStatus {
            name: name.to_string(),
            ready: true,
            restart_count: 0,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: Some("docker://abc123".to_string()),
            state: Some(ContainerState {
                running: Some(ContainerStateRunning {
                    started_at: Some("2024-01-01T00:00:00Z".to_string()),
                }),
                ..Default::default()
            }),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }
    }

    // A Running pod must have containerStatuses so consumers of the pod status
    // don't misinterpret an empty list as "container already finished".
    #[test]
    fn test_running_pod_must_have_container_statuses() {
        let mut pod = make_pod("my-pod", "default", Some("1"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: Some("All containers started".to_string()),
            reason: None,
            host_ip: Some("127.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let status = pod.status.as_ref().unwrap();
        assert_eq!(status.phase, Some(Phase::Running));
        let statuses = status
            .container_statuses
            .as_ref()
            .expect("must have containerStatuses");
        assert!(
            !statuses.is_empty(),
            "Running pod must have at least one containerStatus"
        );
        assert!(statuses[0].ready, "container must be ready=true");
    }

    // Documents the problematic state: phase=Pending with no containerStatuses.
    // Consumers watching pod status may interpret this as "container already finished".
    #[test]
    fn test_pending_with_no_container_statuses_is_the_bug_state() {
        let mut pod = make_pod("my-pod", "default", Some("1"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            message: Some("ContainerCreating".to_string()),
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None, // <-- the bug: sonobuoy-worker sees this and declares done
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let status = pod.status.as_ref().unwrap();
        // Document that this state (Pending + no containerStatuses) is the problematic one
        let is_bug_state = status.phase == Some(Phase::Pending)
            && status
                .container_statuses
                .as_ref()
                .is_none_or(|v| v.is_empty());
        assert!(
            is_bug_state,
            "This is the state that triggers premature result submission"
        );
    }

    // When re-fetching from etcd fails, we fall back to the original pod clone.
    // The fallback ensures we still attempt the status update even if stale.
    #[test]
    fn test_fresh_fetch_fallback_uses_pod_clone_when_get_fails() {
        let original = make_pod("my-pod", "default", Some("42"));
        // Simulate fallback: use the original pod if re-fetch fails
        let fresh_pod = original.clone();
        assert_eq!(
            fresh_pod.metadata.resource_version.as_deref(),
            Some("42"),
            "Fallback uses original resourceVersion"
        );
    }

    #[test]
    fn readiness_gates_gate_ready_until_condition_true() {
        use rusternetes_common::resources::pod::PodReadinessGate;

        let gated_spec = || PodSpec {
            containers: vec![make_container("app")],
            readiness_gates: Some(vec![PodReadinessGate {
                condition_type: "www.example.com/feature-1".to_string(),
            }]),
            ..Default::default()
        };
        let now = chrono::Utc::now();
        let with_status = |spec: PodSpec, conds: Vec<PodCondition>| {
            let mut pod = Pod::new("pod-ready", spec);
            pod.status = Some(PodStatus {
                conditions: Some(conds),
                ..Default::default()
            });
            pod
        };

        // No readinessGates at all -> trivially satisfied.
        let no_gates = Pod::new(
            "pod-ready",
            PodSpec {
                containers: vec![make_container("app")],
                ..Default::default()
            },
        );
        assert!(super::readiness_gates_satisfied(&no_gates));

        // Gate present but no matching condition -> NOT satisfied (this is the
        // "initially Ready=False" case the conformance suite checks).
        let pending = with_status(gated_spec(), vec![cond("ContainersReady", "True", now)]);
        assert!(!super::readiness_gates_satisfied(&pending));

        // Gate condition present but False -> NOT satisfied.
        let gate_false = with_status(
            gated_spec(),
            vec![cond("www.example.com/feature-1", "False", now)],
        );
        assert!(!super::readiness_gates_satisfied(&gate_false));

        // Gate condition present and True -> satisfied.
        let gate_true = with_status(
            gated_spec(),
            vec![cond("www.example.com/feature-1", "True", now)],
        );
        assert!(super::readiness_gates_satisfied(&gate_true));

        // #2384: the verdict depends on WHICH snapshot is evaluated. The
        // cycle-start pod (stale) lacks a gate condition patched mid-cycle;
        // the re-read pod has it. The readiness cycle must evaluate the
        // re-read one (`readiness_pod`), so the stale verdict must differ.
        let stale = with_status(gated_spec(), vec![cond("ContainersReady", "True", now)]);
        let fresh = with_status(
            gated_spec(),
            vec![
                cond("ContainersReady", "True", now),
                cond("www.example.com/feature-1", "True", now),
            ],
        );
        assert!(!super::readiness_gates_satisfied(&stale));
        assert!(super::readiness_gates_satisfied(&fresh));
    }

    /// #2387 (red-first for #2384): drive the per-cycle readiness block without
    /// a CRI runtime. The cycle-start pod is stale (no gate condition); storage
    /// holds the fresh pod (gate patched via /status mid-cycle). The block must
    /// evaluate gates on the re-read pod.
    #[tokio::test]
    async fn readiness_cycle_evaluates_gates_on_reread_pod() {
        use rusternetes_common::resources::pod::{PodReadinessGate, PodSpec};
        use rusternetes_storage::{build_key, Storage};
        let now = chrono::Utc::now();
        let spec = || PodSpec {
            containers: vec![make_container("app")],
            readiness_gates: Some(vec![PodReadinessGate {
                condition_type: "www.example.com/feature-1".to_string(),
            }]),
            ..Default::default()
        };
        let mk = |conds: Vec<PodCondition>| {
            let mut p = Pod::new("pod-ready", spec());
            p.metadata.namespace = Some("ns".to_string());
            p.status = Some(PodStatus {
                conditions: Some(conds),
                ..Default::default()
            });
            p
        };
        let stale = mk(vec![cond("ContainersReady", "True", now)]);
        let fresh = mk(vec![
            cond("ContainersReady", "True", now),
            cond("www.example.com/feature-1", "True", now),
        ]);
        let storage = rusternetes_storage::memory::MemoryStorage::new();
        let key = build_key("pods", Some("ns"), "pod-ready");
        storage.create(&key, &fresh).await.unwrap();

        let statuses: Vec<ContainerStatus> = serde_json::from_value(
            serde_json::json!([{"name":"app","ready":true,"restartCount":0,"image":"i","imageID":"i"}]),
        )
        .unwrap();

        // Stale snapshot alone: gate unsatisfied.
        let (cr, ready) = super::evaluate_pod_readiness(&stale, &statuses, None);
        assert!(cr);
        assert!(!ready);

        // The cycle re-reads the pod from storage and evaluates on that.
        let rp = super::load_readiness_pod(&storage, "ns", "pod-ready", &stale).await;
        let (cr, ready) = super::evaluate_pod_readiness(&rp, &statuses, None);
        assert!(cr);
        assert!(ready, "gates must be evaluated on the re-read pod");

        // Storage miss falls back to the cycle-start pod.
        let missing = super::load_readiness_pod(&storage, "ns", "absent", &stale).await;
        assert!(!super::evaluate_pod_readiness(&missing, &statuses, None).1);

        // A not-ready container holds both conditions false.
        let mut unready = statuses.clone();
        unready[0].ready = false;
        assert_eq!(
            super::evaluate_pod_readiness(&rp, &unready, None),
            (false, false)
        );
    }

    #[test]
    fn spec_has_readiness_probe_includes_sidecars() {
        use rusternetes_common::resources::pod::{PodSpec, Probe};

        // A restartable init container (sidecar) with a readiness probe gates
        // pod readiness (#1069).
        let mut sidecar = make_container("sidecar");
        sidecar.restart_policy = Some("Always".to_string());
        sidecar.readiness_probe = Some(serde_json::from_str::<Probe>("{}").unwrap());
        let spec = PodSpec {
            containers: vec![make_container("app")],
            init_containers: Some(vec![sidecar]),
            ..Default::default()
        };
        assert!(super::spec_has_readiness_probe(&spec));

        // No readiness probe anywhere -> not gated.
        let spec_none = PodSpec {
            containers: vec![make_container("app")],
            ..Default::default()
        };
        assert!(!super::spec_has_readiness_probe(&spec_none));

        // A regular container's readiness probe still gates.
        let mut app = make_container("app");
        app.readiness_probe = Some(serde_json::from_str::<Probe>("{}").unwrap());
        let spec_reg = PodSpec {
            containers: vec![app],
            ..Default::default()
        };
        assert!(super::spec_has_readiness_probe(&spec_reg));

        // A plain (non-restartable) init container's probe does NOT gate.
        let mut plain_init = make_container("init");
        plain_init.readiness_probe = Some(serde_json::from_str::<Probe>("{}").unwrap());
        let spec_plain = PodSpec {
            containers: vec![make_container("app")],
            init_containers: Some(vec![plain_init]),
            ..Default::default()
        };
        assert!(!super::spec_has_readiness_probe(&spec_plain));
    }

    // A container with state=Running signals that the container is still in
    // progress. Consumers should not treat it as finished.
    #[test]
    fn test_container_status_running_state_prevents_premature_submission() {
        let status = make_running_container_status("app");
        match &status.state {
            Some(ContainerState {
                running: Some(ContainerStateRunning { .. }),
                ..
            }) => {
                // This state correctly signals "still running" to sonobuoy-worker
            }
            other => panic!("Expected Running state, got {:?}", other),
        }
        assert!(status.ready, "Running container must be ready=true");
    }

    // A container with state=Waiting also signals "not finished" since it hasn't
    // exited yet. Only Terminated state means the container is done.
    #[test]
    fn test_container_status_waiting_also_signals_not_finished() {
        let status = ContainerStatus {
            name: "app".to_string(),
            ready: false,
            restart_count: 0,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: None,
            state: Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some("ContainerCreating".to_string()),
                    message: None,
                }),
                ..Default::default()
            }),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        };
        let is_terminated = matches!(
            status.state,
            Some(ContainerState {
                terminated: Some(ContainerStateTerminated { .. }),
                ..
            })
        );
        assert!(
            !is_terminated,
            "Waiting container is not terminated — sonobuoy-worker should wait"
        );
    }

    // Documents why re-fetching from etcd before writing Running status is necessary.
    // An admission controller or scheduler may have incremented resourceVersion between
    // when we fetched the pod and when start_pod returned, causing update to fail.
    #[test]
    fn test_stale_resource_version_causes_conflict() {
        let stale = make_pod("my-pod", "default", Some("5"));
        // Simulate etcd advancing the resourceVersion (e.g., admission controller touch)
        let fresh = make_pod("my-pod", "default", Some("6"));

        assert_ne!(
            stale.metadata.resource_version, fresh.metadata.resource_version,
            "Stale rv={:?} differs from fresh rv={:?} — using stale would cause conflict",
            stale.metadata.resource_version, fresh.metadata.resource_version
        );

        // The fix: always use fresh.metadata.resource_version when writing status
        let rv_to_use = fresh.metadata.resource_version.as_deref().unwrap_or("0");
        assert_eq!(
            rv_to_use, "6",
            "Must use fresh resourceVersion to avoid conflict"
        );
    }

    // ---- Pod Resize Tests (KEP-1287) ----

    /// When pod status.resize is "Proposed", the kubelet should detect it as a resize request.
    #[test]
    fn test_resize_proposed_detected() {
        let mut pod = make_pod("resize-pod", "default", Some("10"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("10.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("Proposed".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");

        assert_eq!(resize_status, "Proposed");
        assert!(
            resize_status == "Proposed" || resize_status == "InProgress",
            "Kubelet should process Proposed or InProgress resize"
        );
    }

    /// When pod status.resize is "InProgress", the kubelet should continue processing.
    #[test]
    fn test_resize_in_progress_continues() {
        let mut pod = make_pod("resize-pod-ip", "default", Some("11"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("10.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("InProgress".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");

        assert_eq!(resize_status, "InProgress");
        // Kubelet should still process an InProgress resize
        assert!(resize_status == "Proposed" || resize_status == "InProgress");
    }

    /// After resize completes, status.resize should be empty string.
    #[test]
    fn test_resize_completion_sets_empty_string() {
        let mut pod = make_pod("resize-done", "default", Some("12"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
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
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("InProgress".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        // Simulate the kubelet marking resize as complete
        if let Some(ref mut status) = pod.status {
            status.resize = Some(String::new()); // Empty = resize complete
        }

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("missing");

        assert_eq!(
            resize_status, "",
            "After resize completes, status.resize should be empty string"
        );
    }

    /// Verify that allocatedResources is populated from spec resources after resize.
    #[test]
    fn test_resize_populates_allocated_resources() {
        use rusternetes_common::resources::pod::PodSpec;
        use rusternetes_common::types::ResourceRequirements;
        use std::collections::HashMap;

        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), "500m".to_string());
        requests.insert("memory".to_string(), "256Mi".to_string());

        let mut limits = HashMap::new();
        limits.insert("cpu".to_string(), "1".to_string());
        limits.insert("memory".to_string(), "512Mi".to_string());

        let mut pod = make_pod("resize-alloc", "default", Some("13"));
        pod.spec = Some(PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                image: "nginx:latest".to_string(),
                resources: Some(ResourceRequirements {
                    requests: Some(requests.clone()),
                    limits: Some(limits.clone()),
                    claims: None,
                }),
                ..make_container("app")
            }],
            ..pod.spec.unwrap()
        });
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
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
            container_statuses: Some(vec![ContainerStatus {
                name: "app".to_string(),
                ready: true,
                restart_count: 0,
                state: Some(ContainerState {
                    running: Some(ContainerStateRunning {
                        started_at: Some("2024-01-01T00:00:00Z".to_string()),
                    }),
                    ..Default::default()
                }),
                last_state: None,
                image: Some("nginx:latest".to_string()),
                image_id: None,
                container_id: Some("docker://abc123".to_string()),
                started: Some(true),
                allocated_resources: None, // Not yet populated
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            }]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("InProgress".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        // Simulate the kubelet logic: after successful resize, populate allocatedResources
        // from spec containers (mirroring the actual kubelet code at line ~930-948)
        if let Some(ref mut status) = pod.status {
            status.resize = Some(String::new());
            if let Some(ref spec) = pod.spec.clone() {
                if let Some(ref mut cs_list) = status.container_statuses {
                    for cs in cs_list.iter_mut() {
                        if let Some(c) = spec.containers.iter().find(|c| c.name == cs.name) {
                            if let Some(ref res) = c.resources {
                                cs.allocated_resources =
                                    res.requests.clone().or_else(|| res.limits.clone());
                                cs.resources = Some(res.clone());
                            }
                        }
                    }
                }
            }
        }

        // Verify allocatedResources were populated
        let cs = &pod
            .status
            .as_ref()
            .unwrap()
            .container_statuses
            .as_ref()
            .unwrap()[0];
        let alloc = cs
            .allocated_resources
            .as_ref()
            .expect("allocatedResources should be populated after resize");
        assert_eq!(alloc.get("cpu"), Some(&"500m".to_string()));
        assert_eq!(alloc.get("memory"), Some(&"256Mi".to_string()));

        // Verify resources were populated
        let res = cs
            .resources
            .as_ref()
            .expect("resources should be populated after resize");
        assert_eq!(
            res.requests.as_ref().unwrap().get("cpu"),
            Some(&"500m".to_string())
        );
        assert_eq!(
            res.limits.as_ref().unwrap().get("cpu"),
            Some(&"1".to_string())
        );
    }

    /// When resize is not Proposed or InProgress, the kubelet should not process a resize.
    #[test]
    fn test_resize_not_triggered_for_empty_or_none() {
        // No resize field
        let mut pod = make_pod("no-resize", "default", Some("14"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
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
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");
        assert!(
            resize_status != "Proposed" && resize_status != "InProgress",
            "No resize should be triggered when resize is None"
        );

        // Empty string (completed)
        pod.status.as_mut().unwrap().resize = Some(String::new());
        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");
        assert!(
            resize_status != "Proposed" && resize_status != "InProgress",
            "No resize should be triggered when resize is empty (completed)"
        );
    }

    /// Verify the resize status transition: Proposed -> InProgress -> "" (complete)
    #[test]
    fn test_resize_status_transition() {
        let mut pod = make_pod("resize-transition", "default", Some("15"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
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
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("Proposed".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        // Step 1: API sets resize="Proposed"
        assert_eq!(
            pod.status.as_ref().unwrap().resize.as_deref(),
            Some("Proposed")
        );

        // Step 2: Kubelet transitions to "InProgress"
        pod.status.as_mut().unwrap().resize = Some("InProgress".to_string());
        assert_eq!(
            pod.status.as_ref().unwrap().resize.as_deref(),
            Some("InProgress")
        );

        // Step 3: Kubelet completes resize, sets to ""
        pod.status.as_mut().unwrap().resize = Some(String::new());
        assert_eq!(pod.status.as_ref().unwrap().resize.as_deref(), Some(""));
    }

    // --- Init-container failure conditions (K8s conformance) ---
    //
    // K8s conformance test "should not start app containers if init
    // containers fail on a RestartAlways pod" (init_container.go:446) asserts:
    //   - pod.conditions[Initialized].Status  == "False"
    //   - pod.conditions[Initialized].Reason  == "ContainersNotInitialized"
    //   - pod.conditions[Initialized].Message == "containers with incomplete status: [init1 init2]"
    //
    // The kubelet sync loop's should_retry branch builds these conditions
    // from `init_failed_pod_conditions(&incomplete_inits)` using the same
    // filter as the start_pod error handler: an init container is
    // "incomplete" iff its state is NOT Terminated{exit_code:0}.

    /// Mirror the incomplete-init filter that kubelet.rs uses in the
    /// init-failure status update. Kept in sync with the inline copies in
    /// the should_retry branch and the start_pod error handler.
    fn incomplete_init_names(pod: &Pod) -> Vec<String> {
        let init_statuses = pod
            .status
            .as_ref()
            .and_then(|s| s.init_container_statuses.as_ref());
        pod.spec
            .as_ref()
            .and_then(|s| s.init_containers.as_ref())
            .map(|ics| {
                ics.iter()
                    .filter(|c| {
                        let completed = init_statuses
                            .and_then(|statuses| statuses.iter().find(|s| s.name == c.name))
                            .map(|s| {
                                matches!(
                                    &s.state,
                                    Some(ContainerState {
                                        terminated: Some(ContainerStateTerminated {
                                            exit_code: 0,
                                            ..
                                        }),
                                        ..
                                    })
                                )
                            })
                            .unwrap_or(false);
                        !completed
                    })
                    .map(|c| c.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn init_status(name: &str, state: ContainerState) -> ContainerStatus {
        ContainerStatus {
            name: name.to_string(),
            ready: false,
            restart_count: 0,
            state: Some(state),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(false),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }
    }

    fn restartalways_pod_with_two_inits() -> Pod {
        // init1 failed (exit 1), init2 never ran (Waiting/PodInitializing)
        let init_statuses = vec![
            init_status(
                "init1",
                ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code: 1,
                        signal: None,
                        reason: Some("Error".to_string()),
                        message: None,
                        started_at: Some("2026-01-01T00:00:00Z".to_string()),
                        finished_at: Some("2026-01-01T00:00:01Z".to_string()),
                        container_id: Some("docker://abc123".to_string()),
                    }),
                    ..Default::default()
                },
            ),
            init_status(
                "init2",
                ContainerState {
                    waiting: Some(ContainerStateWaiting {
                        reason: Some("PodInitializing".to_string()),
                        message: None,
                    }),
                    ..Default::default()
                },
            ),
        ];
        let mut pod = make_pod("pod-init", "default", None);
        if let Some(ref mut spec) = pod.spec {
            spec.init_containers = Some(vec![make_container("init1"), make_container("init2")]);
            spec.containers = vec![make_container("run1")];
            spec.restart_policy = Some("Always".to_string());
        }
        pod.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            message: None,
            reason: Some("PodInitializing".to_string()),
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: Some(init_statuses),
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });
        pod
    }

    #[test]
    fn test_init_failed_conditions_lists_both_inits_when_first_failed_and_second_not_run() {
        // The conformance test asserts a specific message format that
        // includes BOTH init container names — init1 (failed) and init2
        // (which never had a chance to run) — separated by a single space.
        let pod = restartalways_pod_with_two_inits();
        let incomplete = incomplete_init_names(&pod);
        assert_eq!(
            incomplete,
            vec!["init1".to_string(), "init2".to_string()],
            "both init1 (Terminated with non-zero exit) and init2 (Waiting) must be marked incomplete"
        );

        let conditions = Kubelet::init_failed_pod_conditions(&incomplete);
        let initialized = conditions
            .iter()
            .find(|c| c.condition_type == "Initialized")
            .expect("Initialized condition must be present");

        assert_eq!(initialized.status, "False");
        assert_eq!(
            initialized.reason.as_deref(),
            Some("ContainersNotInitialized")
        );
        assert_eq!(
            initialized.message.as_deref(),
            Some("containers with incomplete status: [init1 init2]"),
            "K8s conformance asserts this exact message verbatim"
        );
    }

    #[test]
    fn test_init_failed_conditions_drops_successful_init_from_list() {
        // After init1 succeeds and init2 fails, only init2 must appear in
        // the conditions message — successful init containers are dropped.
        let mut pod = restartalways_pod_with_two_inits();
        if let Some(ref mut status) = pod.status {
            status.init_container_statuses = Some(vec![
                init_status(
                    "init1",
                    ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 0,
                            signal: None,
                            reason: Some("Completed".to_string()),
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                        }),
                        ..Default::default()
                    },
                ),
                init_status(
                    "init2",
                    ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 1,
                            signal: None,
                            reason: Some("Error".to_string()),
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                        }),
                        ..Default::default()
                    },
                ),
            ]);
        }
        let incomplete = incomplete_init_names(&pod);
        assert_eq!(
            incomplete,
            vec!["init2".to_string()],
            "successfully terminated (exit 0) init containers must not appear in incomplete list"
        );
        let conditions = Kubelet::init_failed_pod_conditions(&incomplete);
        let initialized = conditions
            .iter()
            .find(|c| c.condition_type == "Initialized")
            .expect("Initialized condition must be present");
        assert_eq!(
            initialized.message.as_deref(),
            Some("containers with incomplete status: [init2]"),
            "only failing init container must appear in the message"
        );
    }

    #[test]
    fn test_init_failed_conditions_pod_initialized_is_false_with_correct_reason() {
        // RestartAlways pod whose init container fails must report
        // PodInitialized=False with reason ContainersNotInitialized.
        let conditions = Kubelet::init_failed_pod_conditions(&["init1".to_string()]);
        let initialized = conditions
            .iter()
            .find(|c| c.condition_type == "Initialized")
            .expect("Initialized condition must be present");
        assert_eq!(initialized.status, "False");
        assert_eq!(
            initialized.reason.as_deref(),
            Some("ContainersNotInitialized")
        );
        // Sanity: ContainersReady and Ready also exist and are False.
        let containers_ready = conditions
            .iter()
            .find(|c| c.condition_type == "ContainersReady")
            .expect("ContainersReady condition must be present");
        assert_eq!(containers_ready.status, "False");
        let ready = conditions
            .iter()
            .find(|c| c.condition_type == "Ready")
            .expect("Ready condition must be present");
        assert_eq!(ready.status, "False");
    }
}
