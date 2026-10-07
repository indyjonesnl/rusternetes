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
//! container-manager soft requirements and shutdown-manager errors.

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

/// Port of `nodestatus.ReadyCondition` (setters.go:469-551) minus the
/// container-manager/shutdown inputs. `errs` is the runtime/network/storage
/// error list. Keeps `lastTransitionTime` while the status is unchanged.
/// Returns true when the condition changed in a way that must be persisted
/// (anything but a fresh heartbeat time).
pub fn set_ready_condition(
    conditions: &mut Vec<NodeCondition>,
    errs: Vec<String>,
    now: DateTime<Utc>,
) -> bool {
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
        if old.status == new.status {
            new.last_transition_time = old.last_transition_time;
        }
        *old = new;
        changed
    } else {
        conditions.push(new);
        true
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

    #[test]
    fn ready_condition_flips_and_preserves_transition_time() {
        let t0 = Utc::now() - chrono::Duration::seconds(100);
        let t1 = Utc::now();
        let mut conds = Vec::new();
        assert!(set_ready_condition(&mut conds, vec![], t0));
        assert_eq!(conds[0].status, "True");
        // Same status: transition time preserved, heartbeat advances.
        assert!(!set_ready_condition(&mut conds, vec![], t1));
        assert_eq!(conds[0].last_transition_time, Some(t0));
        assert_eq!(conds[0].last_heartbeat_time, Some(t1));
        // Failure flips to False with the aggregate message.
        assert!(set_ready_condition(
            &mut conds,
            vec!["a".into(), "b".into()],
            t1
        ));
        assert_eq!(conds[0].status, "False");
        assert_eq!(conds[0].reason.as_deref(), Some("KubeletNotReady"));
        assert_eq!(conds[0].message.as_deref(), Some("[a, b]"));
        assert_eq!(conds[0].last_transition_time, Some(t1));
    }
}
