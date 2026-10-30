//! `generateAPIPodStatus` — the single generator every pod-status write
//! flows through.
//!
//! Upstream: `pkg/kubelet/kubelet_pods.go` `(*Kubelet).generateAPIPodStatus`
//! (line 1889). The host-IP block at its tail lives in
//! `Kubelet::apply_host_ips_to_pod`; this module owns the remainder that is
//! pure: observedGeneration, the terminal-phase guard, and preservation of
//! conditions the kubelet does not own.
//!
//! Slice 1 of #3808: the two Running-transition sites in `kubelet.rs` are
//! migrated. Slice 2 (#3901): create-error, app-start-error and init-failure
//! writes too, plus reason/message preservation.

use rusternetes_common::{
    resources::{pod::HostIP, ContainerStatus, Pod, PodCondition, PodIP, PodStatus},
    types::Phase,
};

/// `PodConditionsByKubelet` (pkg/kubelet/types/pod_status.go:26-33).
const POD_CONDITIONS_BY_KUBELET: &[&str] = &[
    "PodScheduled",
    "Ready",
    "Initialized",
    "ContainersReady",
    "PodResizeInProgress",
    "PodResizePending",
];

/// `kubetypes.PodConditionByKubelet` (pod_status.go:36). The feature-gated
/// `PodReadyToStartContainers` / `AllContainersRestarting` arms are not
/// ported: this kubelet does not generate those conditions.
pub fn pod_condition_by_kubelet(condition_type: &str) -> bool {
    POD_CONDITIONS_BY_KUBELET.contains(&condition_type)
}

/// What the caller observed; the generator adds everything derived from the
/// pod's prior status.
pub struct ApiPodStatusInput {
    pub phase: Phase,
    pub message: Option<String>,
    pub reason: Option<String>,
    pub host_ip: Option<String>,
    pub host_ips: Option<Vec<HostIP>>,
    pub pod_ip: Option<String>,
    pub pod_ips: Option<Vec<PodIP>>,
    /// Kubelet-owned conditions (Initialized/Ready/ContainersReady/...).
    pub conditions: Vec<PodCondition>,
    pub container_statuses: Option<Vec<ContainerStatus>>,
    pub init_container_statuses: Option<Vec<ContainerStatus>>,
    pub ephemeral_container_statuses: Option<Vec<ContainerStatus>>,
    pub qos_class: Option<String>,
    pub start_time: chrono::DateTime<chrono::Utc>,
    /// Result of the `PodSyncHandlers` `ShouldEvict` loop
    /// (kubelet_pods.go:1928-1936); `None` when no handler asked to evict.
    pub should_evict: Option<ShouldEvictResponse>,
}

/// `lifecycle.ShouldEvictResponse` (pkg/kubelet/lifecycle/interfaces.go:67),
/// the `Evict == true` arm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShouldEvictResponse {
    pub reason: String,
    pub message: String,
}

/// `activeDeadlineHandler.ShouldEvict` (pkg/kubelet/active_deadline.go:70) /
/// `pastActiveDeadline` (:79). `pod.status` stands in for the status
/// manager's cached status (`podStatusProvider.GetPodStatus`): this kubelet
/// has no separate cache, so the API status is the latest status. The
/// `recorder.Eventf` upstream emits is the caller's concern.
pub fn active_deadline_should_evict(
    pod: &Pod,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<ShouldEvictResponse> {
    let deadline = pod.spec.as_ref()?.active_deadline_seconds?;
    crate::kubelet::active_deadline_elapsed(pod.status.as_ref(), deadline, now)?;
    Some(ShouldEvictResponse {
        reason: crate::kubelet::ACTIVE_DEADLINE_REASON.to_string(),
        message: crate::kubelet::ACTIVE_DEADLINE_MESSAGE.to_string(),
    })
}

/// Port of the pure part of `generateAPIPodStatus`
/// (pkg/kubelet/kubelet_pods.go:1889). The host-IP block is applied by the
/// caller via `Kubelet::apply_host_ips_to_pod`.
pub fn generate_api_pod_status(pod: &Pod, input: ApiPodStatusInput) -> PodStatus {
    let api_phase = pod.status.as_ref().and_then(|s| s.phase.clone());

    // Three-way merge (kubelet_pods.go:1906-1917): upstream merges the status
    // manager's cached status, the runtime-derived phase and the API status.
    // This kubelet has no status-manager cache, so oldPodStatus == pod.Status
    // and the two terminal arms collapse into one.
    let mut phase = Some(input.phase);
    if !crate::lifecycle::phase_is_terminal(phase.as_ref())
        && crate::lifecycle::phase_is_terminal(api_phase.as_ref())
    {
        phase = api_phase.clone();
    }

    // "preserve all conditions not owned by the kubelet"
    // (kubelet_pods.go:1957-1962), then the kubelet-owned ones.
    let mut conditions: Vec<PodCondition> = pod
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| {
            cs.iter()
                .filter(|c| !pod_condition_by_kubelet(&c.condition_type))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    conditions.extend(input.conditions);

    // "preserve the reason and message which is associated with the phase"
    // (kubelet_pods.go:1915-1926). Upstream's generated status never carries
    // a reason/message, so the old status' wins when the phase is unchanged.
    // Here a caller-supplied Some(..) plays the role of upstream's explicit
    // overrides (the ShouldEvict block, :1929-1935, and the status-manager
    // rejection paths): it is kept as-is.
    let (mut reason, mut message) = (input.reason, input.message);
    let old_phase = pod.status.as_ref().and_then(|s| s.phase.clone());
    if phase == old_phase {
        let old = pod.status.as_ref();
        if reason.is_none() {
            reason = old.and_then(|s| s.reason.clone()).filter(|r| !r.is_empty());
        }
        if message.is_none() {
            message = old
                .and_then(|s| s.message.clone())
                .filter(|m| !m.is_empty());
        }
    }

    // "check if an internal module has requested the pod is evicted and
    // override the reason and message" (kubelet_pods.go:1928-1936).
    if let Some(evict) = input.should_evict {
        phase = Some(Phase::Failed);
        reason = Some(evict.reason);
        message = Some(evict.message);
    }

    // "pods are not allowed to transition out of terminal phases"
    // (kubelet_pods.go:1938-1945): force back to the API server's phase.
    // Reason/message set above are deliberately left, as upstream does.
    if crate::lifecycle::phase_is_terminal(api_phase.as_ref()) && phase != api_phase {
        phase = api_phase;
    }

    // Fields this kubelet does not generate are carried over from the prior
    // status: upstream writes status via a patch (status_manager.go
    // patchPodStatus), so omitted fields survive; our write is a full update.
    let prior = pod.status.as_ref();

    PodStatus {
        phase,
        message,
        reason,
        resize: prior.and_then(|s| s.resize.clone()),
        resource_claim_statuses: prior.and_then(|s| s.resource_claim_statuses.clone()),
        nominated_node_name: prior.and_then(|s| s.nominated_node_name.clone()),
        host_ip: input.host_ip,
        host_i_ps: input.host_ips,
        pod_ip: input.pod_ip,
        pod_i_ps: input.pod_ips,
        conditions: Some(conditions),
        container_statuses: input.container_statuses,
        init_container_statuses: input.init_container_statuses,
        ephemeral_container_statuses: input.ephemeral_container_statuses,
        observed_generation: pod.metadata.generation,
        qos_class: input.qos_class,
        start_time: Some(input.start_time),
        ..Default::default()
    }
}
