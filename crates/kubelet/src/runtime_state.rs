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
const RUNTIME_READY: &str = "RuntimeReady";
const NETWORK_READY: &str = "NetworkReady";

struct Inner {
    last_base_runtime_sync: Option<Instant>,
    base_runtime_sync_threshold: Duration,
    network_error: Option<String>,
    runtime_error: Option<String>,
    storage_error: Option<String>,
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
        }))
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
        s.network_error = match condition(&rs, NETWORK_READY) {
            Some(c) if c.status => None,
            c => Some(format!(
                "container runtime network not ready: {}",
                fmt_condition(c)
            )),
        };
        match condition(&rs, RUNTIME_READY) {
            Some(c) if c.status => {
                s.runtime_error = None;
                s.last_base_runtime_sync = Some(Instant::now());
            }
            c => {
                s.runtime_error =
                    Some(format!("container runtime not ready: {}", fmt_condition(c)));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn status(rt: Option<bool>, net: Option<bool>) -> v1::StatusResponse {
        let mut conditions = Vec::new();
        for (ty, v) in [(RUNTIME_READY, rt), (NETWORK_READY, net)] {
            if let Some(status) = v {
                conditions.push(v1::RuntimeCondition {
                    r#type: ty.to_string(),
                    status,
                    reason: if status {
                        String::new()
                    } else {
                        "NetworkPluginNotReady".into()
                    },
                    message: if status {
                        String::new()
                    } else {
                        "no cni config".into()
                    },
                });
            }
        }
        v1::StatusResponse {
            status: Some(v1::RuntimeStatus { conditions }),
            ..Default::default()
        }
    }

    #[test]
    fn fresh_state_has_unknown_runtime_and_network() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        assert_eq!(
            s.runtime_errors().as_deref(),
            Some("container runtime status check may not have completed yet")
        );
        assert_eq!(s.network_errors().as_deref(), Some("network state unknown"));
    }

    #[test]
    fn ready_runtime_and_network_clear_errors() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(Some(true), Some(true))));
        assert_eq!(s.runtime_errors(), None);
        assert_eq!(s.network_errors(), None);
    }

    #[test]
    fn runtime_not_ready_reports_condition_and_does_not_sync() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(Some(false), Some(true))));
        assert_eq!(
            s.runtime_errors().as_deref(),
            Some("[container runtime status check may not have completed yet, container runtime not ready: RuntimeReady=false reason:NetworkPluginNotReady message:no cni config]")
        );
    }

    #[test]
    fn missing_runtime_ready_condition_is_an_error() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(None, Some(true))));
        assert!(s
            .runtime_errors()
            .unwrap()
            .contains("container runtime not ready: <nil>"));
    }

    #[test]
    fn network_not_ready_reports_condition() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(Some(true), Some(false))));
        assert_eq!(s.runtime_errors(), None);
        assert_eq!(
            s.network_errors().as_deref(),
            Some("container runtime network not ready: NetworkReady=false reason:NetworkPluginNotReady message:no cni config")
        );
    }

    /// `kubeletVolumeHost.SetKubeletError` -> `setStorageState`
    /// (volume_host.go:122): a set error is reported, `nil` clears it.
    #[test]
    fn storage_state_sets_and_clears() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        assert_eq!(s.storage_errors(), None);
        s.set_storage_state(Some("CSINode is not yet initialized".into()));
        assert_eq!(
            s.storage_errors().as_deref(),
            Some("CSINode is not yet initialized")
        );
        s.set_storage_state(None);
        assert_eq!(s.storage_errors(), None);
    }

    #[test]
    fn stale_sync_means_runtime_is_down() {
        let s = RuntimeState::new(Duration::ZERO);
        s.update_from_status(Ok(status(Some(true), Some(true))));
        assert_eq!(
            s.runtime_errors().as_deref(),
            Some("container runtime is down")
        );
    }

    #[test]
    fn status_rpc_error_leaves_state_untouched() {
        let s = RuntimeState::new(MAX_WAIT_FOR_CONTAINER_RUNTIME);
        s.update_from_status(Ok(status(Some(true), Some(true))));
        s.update_from_status(Err("boom".into()));
        assert_eq!(s.runtime_errors(), None);
    }

    fn cap(keys: &[&str]) -> HashMap<String, String> {
        keys.iter()
            .map(|k| (k.to_string(), "1".to_string()))
            .collect()
    }
    fn full() -> HashMap<String, String> {
        cap(&["cpu", "memory", "pods", "ephemeral-storage"])
    }
    fn cond(ready: bool, msg: &str, transition: DateTime<Utc>, hb: DateTime<Utc>) -> NodeCondition {
        NodeCondition {
            condition_type: "Ready".into(),
            status: if ready { "True" } else { "False" }.into(),
            last_heartbeat_time: Some(hb),
            last_transition_time: Some(transition),
            reason: Some(
                if ready {
                    "KubeletReady"
                } else {
                    "KubeletNotReady"
                }
                .into(),
            ),
            message: Some(msg.into()),
        }
    }

    type ReadyCase = (
        &'static str,
        Option<HashMap<String, String>>,
        Vec<NodeCondition>,
        Vec<String>,
        bool,
        NodeCondition,
        Option<&'static str>,
    );

    /// `TestReadyCondition` (nodestatus/setters_test.go:1188), the cases that
    /// do not need a container manager or shutdown manager.
    #[test]
    fn ready_condition_cases() {
        let now = Utc::now();
        let before = now - chrono::Duration::seconds(1);
        let ok = "kubelet is posting ready status";
        let cases: Vec<ReadyCase> = vec![
            (
                "new, ready",
                Some(full()),
                vec![],
                vec![],
                true,
                cond(true, ok, now, now),
                None,
            ),
            (
                "new, not ready: storage errors",
                Some(full()),
                vec![],
                vec!["some storage error".into()],
                true,
                cond(false, "some storage error", now, now),
                None,
            ),
            (
                "new, not ready: runtime and network errors",
                Some(full()),
                vec![],
                vec!["runtime".into(), "network".into()],
                true,
                cond(false, "[runtime, network]", now, now),
                None,
            ),
            (
                "new, not ready: missing capacities",
                None,
                vec![],
                vec![],
                true,
                cond(
                    false,
                    "missing node capacity for resources: cpu, memory, pods, ephemeral-storage",
                    now,
                    now,
                ),
                None,
            ),
            (
                "new, not ready: errors and missing capacities aggregate",
                Some(cap(&["cpu", "memory", "pods"])),
                vec![],
                vec!["runtime".into()],
                true,
                cond(
                    false,
                    "[runtime, missing node capacity for resources: ephemeral-storage]",
                    now,
                    now,
                ),
                None,
            ),
            (
                "new, ready: localStorageCapacityIsolation is not supported",
                Some(cap(&["cpu", "memory", "pods"])),
                vec![],
                vec![],
                false,
                cond(true, ok, now, now),
                None,
            ),
            (
                "transition to ready",
                Some(full()),
                vec![cond(false, "", before, before)],
                vec![],
                true,
                cond(true, ok, now, now),
                Some("NodeReady"),
            ),
            (
                "transition to not ready",
                Some(full()),
                vec![cond(true, "", before, before)],
                vec!["foo".into()],
                true,
                cond(false, "foo", now, now),
                Some("NodeNotReady"),
            ),
            (
                "ready, no transition",
                Some(full()),
                vec![cond(true, "", before, before)],
                vec![],
                true,
                cond(true, ok, before, now),
                None,
            ),
            (
                "not ready, no transition",
                Some(full()),
                vec![cond(false, "", before, before)],
                vec!["foo".into()],
                true,
                cond(false, "foo", before, now),
                None,
            ),
        ];
        for (desc, capacity, mut conds, errs, isolation, want, want_event) in cases {
            let upd = set_ready_condition(&mut conds, capacity.as_ref(), errs, now, isolation);
            assert_eq!(
                serde_json::to_value(&conds).unwrap(),
                serde_json::to_value(vec![want]).unwrap(),
                "{desc}"
            );
            assert_eq!(upd.event, want_event, "{desc}");
        }
    }

    #[test]
    fn ready_condition_flips_and_preserves_transition_time() {
        let t0 = Utc::now() - chrono::Duration::seconds(100);
        let t1 = Utc::now();
        let c = full();
        let mut conds = Vec::new();
        assert!(set_ready_condition(&mut conds, Some(&c), vec![], t0, true).changed);
        assert_eq!(conds[0].status, "True");
        // Same status: transition time preserved, heartbeat advances.
        assert!(!set_ready_condition(&mut conds, Some(&c), vec![], t1, true).changed);
        assert_eq!(conds[0].last_transition_time, Some(t0));
        assert_eq!(conds[0].last_heartbeat_time, Some(t1));
        // Failure flips to False with the aggregate message.
        assert!(
            set_ready_condition(&mut conds, Some(&c), vec!["a".into(), "b".into()], t1, true)
                .changed
        );
        assert_eq!(conds[0].status, "False");
        assert_eq!(conds[0].reason.as_deref(), Some("KubeletNotReady"));
        assert_eq!(conds[0].message.as_deref(), Some("[a, b]"));
        assert_eq!(conds[0].last_transition_time, Some(t1));
    }

    /// `recordNodeStatusEvent` (kubelet_node_status.go:632): a Normal
    /// `NodeNotReady` event on the Node with "Node %s status is now: %s".
    #[tokio::test]
    async fn node_status_event_is_recorded_on_the_node() {
        use rusternetes_storage::Storage;
        let storage = std::sync::Arc::new(rusternetes_storage::StorageBackend::new_memory());
        let recorder = rusternetes_storage::EventRecorder::new(std::sync::Arc::clone(&storage));
        record_node_status_event(&recorder, "node-1", EVENT_NODE_NOT_READY).await;
        let evs: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].reason, "NodeNotReady");
        assert_eq!(evs[0].message, "Node node-1 status is now: NodeNotReady");
        assert_eq!(evs[0].involved_object.kind.as_deref(), Some("Node"));
        assert_eq!(evs[0].involved_object.name.as_deref(), Some("node-1"));
        assert_eq!(evs[0].source.component, "kubelet");
        assert_eq!(evs[0].source.host.as_deref(), Some("node-1"));
    }
}
