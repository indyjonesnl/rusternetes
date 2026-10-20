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
//! migrated. Slice 2 (#3901): the create-error Pending write too. The rest
//! are tracked in the issue.

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
}

/// Port of the pure part of `generateAPIPodStatus`
/// (pkg/kubelet/kubelet_pods.go:1889). The host-IP block is applied by the
/// caller via `Kubelet::apply_host_ips_to_pod`.
pub fn generate_api_pod_status(pod: &Pod, input: ApiPodStatusInput) -> PodStatus {
    let api_phase = pod.status.as_ref().and_then(|s| s.phase.clone());

    // "pods are not allowed to transition out of terminal phases"
    // (kubelet_pods.go:1937-1945): force back to the API server's phase.
    let phase = if crate::lifecycle::phase_is_terminal(api_phase.as_ref()) {
        api_phase
    } else {
        Some(input.phase)
    };

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

    PodStatus {
        phase,
        message: input.message,
        reason: input.reason,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cond(t: &str, s: &str) -> PodCondition {
        PodCondition {
            condition_type: t.to_string(),
            status: s.to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        }
    }

    fn input(phase: Phase) -> ApiPodStatusInput {
        ApiPodStatusInput {
            phase,
            message: Some("All containers started".into()),
            reason: None,
            host_ip: Some("10.0.0.1".into()),
            host_ips: Some(vec![HostIP {
                ip: "10.0.0.1".into(),
            }]),
            pod_ip: Some("10.1.0.5".into()),
            pod_ips: Some(vec![PodIP {
                ip: "10.1.0.5".into(),
            }]),
            conditions: vec![cond("Ready", "True")],
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            qos_class: Some("BestEffort".into()),
            start_time: chrono::Utc::now(),
        }
    }

    fn pod_with(phase: Option<Phase>, conditions: Vec<PodCondition>, gen: Option<i64>) -> Pod {
        let mut pod = Pod::new("p", Default::default());
        pod.metadata.generation = gen;
        pod.status = Some(PodStatus {
            phase,
            conditions: Some(conditions),
            ..Default::default()
        });
        pod
    }

    #[test]
    fn by_kubelet_matches_upstream_list() {
        for t in [
            "PodScheduled",
            "Ready",
            "Initialized",
            "ContainersReady",
            "PodResizeInProgress",
            "PodResizePending",
        ] {
            assert!(pod_condition_by_kubelet(t), "{t}");
        }
        assert!(!pod_condition_by_kubelet("DisruptionTarget"));
        assert!(!pod_condition_by_kubelet("example.com/gate"));
    }

    #[test]
    fn carries_input_and_observed_generation() {
        let pod = pod_with(Some(Phase::Pending), vec![], Some(7));
        let s = generate_api_pod_status(&pod, input(Phase::Running));
        assert_eq!(s.phase, Some(Phase::Running));
        assert_eq!(s.observed_generation, Some(7));
        assert_eq!(s.pod_ip.as_deref(), Some("10.1.0.5"));
        assert_eq!(s.host_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(s.qos_class.as_deref(), Some("BestEffort"));
        assert_eq!(s.message.as_deref(), Some("All containers started"));
    }

    #[test]
    fn preserves_conditions_not_owned_by_kubelet() {
        // kubelet_pods.go:1957-1962 "preserve all conditions not owned by the kubelet"
        let pod = pod_with(
            Some(Phase::Pending),
            vec![
                cond("example.com/gate", "True"),
                cond("DisruptionTarget", "True"),
                cond("Ready", "False"),
            ],
            None,
        );
        let s = generate_api_pod_status(&pod, input(Phase::Running));
        let c = s.conditions.unwrap();
        let types: Vec<&str> = c.iter().map(|c| c.condition_type.as_str()).collect();
        assert_eq!(types, vec!["example.com/gate", "DisruptionTarget", "Ready"]);
        // The stale kubelet-owned Ready=False is replaced by the fresh one.
        assert_eq!(c[2].status, "True");
    }

    #[test]
    fn terminal_api_phase_is_not_left() {
        // kubelet_pods.go:1937-1945 "pods are not allowed to transition out of terminal phases"
        let pod = pod_with(Some(Phase::Succeeded), vec![], None);
        let s = generate_api_pod_status(&pod, input(Phase::Running));
        assert_eq!(s.phase, Some(Phase::Succeeded));
    }
}
