//! Kubelet runtime state and the `NodeReady` condition setter.
//!
//! Port of upstream `runtimeState` (pkg/kubelet/runtime.go), the CRI-status
//! sync `updateRuntimeUp` (pkg/kubelet/kubelet.go:3113-3157) and
//! `nodestatus.ReadyCondition` (pkg/kubelet/nodestatus/setters.go:469-551).
//! The runtime health input is the CRI v1 `Status` RPC's `RuntimeReady` and
//! `NetworkReady` conditions (kuberuntime_manager.go:424-433,
//! `toKubeRuntimeStatus` in kuberuntime/helpers.go:231).
//!
//! `storageError` (runtime.go:35,99-103,148-157) is set through
//! [`RuntimeState::set_storage_state`] by the CSI plugin's `initializeCSINode`
//! (pkg/volume/csi/csi_plugin.go:374,398,404 via `kubeletVolumeHost.SetKubeletError`,
//! pkg/kubelet/volume_host.go:122).
//!
//! Deliberately not ported: health checks (`addHealthCheck`, used only by the PLEG), the
//! container-manager soft requirements and shutdown-manager errors (no such
//! managers exist in this kubelet yet).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rusternetes_common::resources::NodeCondition;
use rusternetes_cri::v1;

/// `maxWaitForContainerRuntime` (pkg/kubelet/kubelet.go:150): how long a
/// successful runtime sync stays valid before the runtime is deemed down.
pub const MAX_WAIT_FOR_CONTAINER_RUNTIME: Duration = Duration::from_secs(30);

/// `ErrNetworkUnknown` (pkg/kubelet/errors.go:28), the initial network error.
pub const ERR_NETWORK_UNKNOWN: &str = "network state unknown";

/// Condition types the runtime reports (pkg/kubelet/container/runtime.go:578-581).
pub const RUNTIME_READY: &str = "RuntimeReady";
pub const NETWORK_READY: &str = "NetworkReady";

struct Inner {
    last_base_runtime_sync: Option<Instant>,
    base_runtime_sync_threshold: Duration,
    network_error: Option<String>,
    runtime_error: Option<String>,
    storage_error: Option<String>,
    /// `containerRuntimeReadyExpected` (kubelet.go:1344): false during the
    /// node-initialization grace period so not-ready logs stay at debug.
    container_runtime_ready_expected: bool,
}

/// Mirrors upstream `runtimeState`.
pub struct RuntimeState(Mutex<Inner>);

impl RuntimeState {
    /// `newRuntimeState` (runtime.go:176-182).
    pub fn new(threshold: Duration) -> Self {
        Self(Mutex::new(Inner {
            last_base_runtime_sync: None,
            base_runtime_sync_threshold: threshold,
            network_error: Some(ERR_NETWORK_UNKNOWN.to_string()),
            runtime_error: None,
            storage_error: None,
            container_runtime_ready_expected: false,
        }))
    }

    /// Set by `fastStatusUpdateOnce` when it exits (kubelet.go:1346-1348,
    /// `fastNodeStatusUpdate` deferred block, kubelet_node_status.go:376-382).
    pub fn set_container_runtime_ready_expected(&self) {
        self.0.lock().unwrap().container_runtime_ready_expected = true;
    }

    /// Current value of `containerRuntimeReadyExpected`.
    pub fn container_runtime_ready_expected(&self) -> bool {
        self.0.lock().unwrap().container_runtime_ready_expected
    }

    /// `runtimeErrors` (runtime.go:130-150).
    pub fn runtime_errors(&self) -> Option<String> {
        let s = self.0.lock().unwrap();
        let mut errs = Vec::new();
        match s.last_base_runtime_sync {
            None => errs.push("container runtime status check may not have completed yet".into()),
            Some(t) if t.elapsed() >= s.base_runtime_sync_threshold => {
                errs.push("container runtime is down".to_string())
            }
            Some(_) => {}
        }
        if let Some(e) = &s.runtime_error {
            errs.push(e.clone());
        }
        aggregate(errs)
    }

    /// `networkErrors` (runtime.go:152-160).
    pub fn network_errors(&self) -> Option<String> {
        aggregate(
            self.0
                .lock()
                .unwrap()
                .network_error
                .iter()
                .cloned()
                .collect(),
        )
    }

    /// `setStorageState` (runtime.go:99-103).
    pub fn set_storage_state(&self, err: Option<String>) {
        self.0.lock().unwrap().storage_error = err;
    }

    /// `storageErrors` (runtime.go:148-157).
    pub fn storage_errors(&self) -> Option<String> {
        aggregate(
            self.0
                .lock()
                .unwrap()
                .storage_error
                .iter()
                .cloned()
                .collect(),
        )
    }

    /// Port of `updateRuntimeUp` (kubelet.go:3113-3157) over the CRI `Status`
    /// result. An RPC error only logs upstream (the stale `lastBaseRuntimeSync`
    /// turns into "container runtime is down" after the threshold).
    pub fn update_from_status(&self, status: Result<v1::StatusResponse, String>) {
        let resp = match status {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Container runtime sanity check failed: {e}");
                return;
            }
        };
        // kuberuntime_manager.go:429: a response without status is an error.
        let Some(rs) = resp.status else {
            tracing::error!("Container runtime status is nil");
            return;
        };
        let mut s = self.0.lock().unwrap();
        // klogErrorS is demoted to V(4) until containerRuntimeReadyExpected
        // (kubelet.go:3129-3132) to avoid error spam at node startup.
        let expected = s.container_runtime_ready_expected;
        s.network_error = match condition(&rs, NETWORK_READY) {
            Some(c) if c.status => None,
            c => {
                let msg = format!("container runtime network not ready: {}", fmt_condition(c));
                if expected {
                    tracing::error!("{msg}");
                } else {
                    tracing::debug!("{msg}");
                }
                Some(msg)
            }
        };
        match condition(&rs, RUNTIME_READY) {
            Some(c) if c.status => {
                s.runtime_error = None;
                s.last_base_runtime_sync = Some(Instant::now());
            }
            c => {
                let msg = format!("container runtime not ready: {}", fmt_condition(c));
                if expected {
                    tracing::error!("{msg}");
                } else {
                    tracing::debug!("{msg}");
                }
                s.runtime_error = Some(msg);
            }
        }
    }
}

fn condition<'a>(rs: &'a v1::RuntimeStatus, ty: &str) -> Option<&'a v1::RuntimeCondition> {
    rs.conditions.iter().find(|c| c.r#type == ty)
}

/// `RuntimeCondition.String()` (container/runtime.go:649-651); `<nil>` is what
/// Go's `%v` prints for the missing-condition case.
fn fmt_condition(c: Option<&v1::RuntimeCondition>) -> String {
    match c {
        None => "<nil>".to_string(),
        Some(c) => format!(
            "{}={} reason:{} message:{}",
            c.r#type, c.status, c.reason, c.message
        ),
    }
}

/// `utilerrors.NewAggregate(errs).Error()` (apimachinery util/errors:70-96).
fn aggregate(mut errs: Vec<String>) -> Option<String> {
    if errs.is_empty() {
        return None;
    }
    if errs.len() == 1 {
        return errs.pop();
    }
    let mut seen: Vec<String> = Vec::new();
    for e in errs {
        if !seen.contains(&e) {
            seen.push(e);
        }
    }
    Some(if seen.len() == 1 {
        seen.remove(0)
    } else {
        format!("[{}]", seen.join(", "))
    })
}

/// What [`set_ready_condition`] did, so the caller can persist and record.
#[derive(Debug, PartialEq)]
pub struct ReadyUpdate {
    /// The condition changed in a way that must be persisted (anything but a
    /// fresh heartbeat time).
    pub changed: bool,
    /// `needToRecordEvent` (setters.go:529-548): the `events.NodeReady` /
    /// `events.NodeNotReady` reason to record through `recordNodeStatusEvent`.
    /// Only set when an existing Ready condition flipped status; the initial
    /// condition records no event upstream.
    pub event: Option<&'static str>,
}

/// `events.NodeReady` / `events.NodeNotReady` (pkg/kubelet/events/event.go:57-58).
pub const EVENT_NODE_READY: &str = "NodeReady";
pub const EVENT_NODE_NOT_READY: &str = "NodeNotReady";

/// `Kubelet.recordNodeStatusEvent` (kubelet_node_status.go:632-635): a Normal
/// event on `nodeRef` (`kubelet.go:552`, no UID or namespace) with the message
/// `Node %s status is now: %s`.
pub async fn record_node_status_event(
    recorder: &rusternetes_storage::EventRecorder<rusternetes_storage::StorageBackend>,
    node_name: &str,
    event: &str,
) {
    let node_ref = rusternetes_common::resources::ObjectReference {
        api_version: Some("v1".to_string()),
        kind: Some("Node".to_string()),
        name: Some(node_name.to_string()),
        namespace: Some(String::new()),
        ..Default::default()
    };
    let source = rusternetes_common::resources::EventSource {
        component: crate::events::KUBELET_COMPONENT.to_string(),
        host: Some(node_name.to_string()),
    };
    if let Err(e) = recorder
        .event(
            &node_ref,
            &source,
            rusternetes_common::resources::EventType::Normal,
            event,
            &format!("Node {node_name} status is now: {event}"),
        )
        .await
    {
        tracing::warn!("failed to record node event {event}: {e}");
    }
}

/// Port of `nodestatus.ReadyCondition` (setters.go:469-551) minus the
/// container-manager SoftRequirements and shutdown-manager inputs (this
/// kubelet has neither). `errs` is the runtime/network/storage error list;
/// the missing-capacity error (setters.go:491-504) is appended here from
/// `capacity`, with `ephemeral-storage` required only under
/// `local_storage_capacity_isolation`. Keeps `lastTransitionTime` while the
/// status is unchanged.
pub fn set_ready_condition(
    conditions: &mut Vec<NodeCondition>,
    capacity: Option<&HashMap<String, String>>,
    mut errs: Vec<String>,
    now: DateTime<Utc>,
    local_storage_capacity_isolation: bool,
) -> ReadyUpdate {
    let mut required = vec!["cpu", "memory", "pods"];
    if local_storage_capacity_isolation {
        required.push("ephemeral-storage");
    }
    let missing: Vec<&str> = required
        .into_iter()
        .filter(|r| !capacity.is_some_and(|c| c.contains_key(*r)))
        .collect();
    if !missing.is_empty() {
        errs.push(format!(
            "missing node capacity for resources: {}",
            missing.join(", ")
        ));
    }
    let (status, reason, message) = match aggregate(errs) {
        Some(m) => ("False", "KubeletNotReady", m),
        None => (
            "True",
            "KubeletReady",
            "kubelet is posting ready status".to_string(),
        ),
    };
    let mut new = NodeCondition {
        condition_type: "Ready".to_string(),
        status: status.to_string(),
        last_heartbeat_time: Some(now),
        last_transition_time: Some(now),
        reason: Some(reason.to_string()),
        message: Some(message),
    };
    if let Some(old) = conditions.iter_mut().find(|c| c.condition_type == "Ready") {
        let changed =
            old.status != new.status || old.reason != new.reason || old.message != new.message;
        let flipped = old.status != new.status;
        if !flipped {
            new.last_transition_time = old.last_transition_time;
        }
        *old = new;
        ReadyUpdate {
            changed,
            event: flipped.then_some(if status == "True" {
                EVENT_NODE_READY
            } else {
                EVENT_NODE_NOT_READY
            }),
        }
    } else {
        conditions.push(new);
        ReadyUpdate {
            changed: true,
            event: None,
        }
    }
}
