//! Unit tests moved from `src/pod_status.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::{
        resources::{pod::HostIP, ContainerStatus, Pod, PodCondition, PodIP, PodStatus},
        types::Phase,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::pod_status::*;

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
            should_evict: None,
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

    #[test]
    fn preserves_reason_and_message_when_phase_unchanged() {
        // kubelet_pods.go:1915-1926
        let mut pod = pod_with(Some(Phase::Running), vec![], None);
        {
            let st = pod.status.as_mut().unwrap();
            st.reason = Some("Restarting".into());
            st.message = Some("Liveness probe failed".into());
        }
        let mut i = input(Phase::Running);
        i.message = None;
        let s = generate_api_pod_status(&pod, i);
        assert_eq!(s.reason.as_deref(), Some("Restarting"));
        assert_eq!(s.message.as_deref(), Some("Liveness probe failed"));
    }

    #[test]
    fn drops_reason_and_message_when_phase_changes() {
        let mut pod = pod_with(Some(Phase::Pending), vec![], None);
        {
            let st = pod.status.as_mut().unwrap();
            st.reason = Some("FailedToStart".into());
            st.message = Some("boom".into());
        }
        let mut i = input(Phase::Running);
        i.message = None;
        let s = generate_api_pod_status(&pod, i);
        assert_eq!(s.reason, None);
        assert_eq!(s.message, None);
    }

    #[test]
    fn should_evict_overrides_phase_reason_and_message() {
        // kubelet_pods.go:1928-1936
        let pod = pod_with(Some(Phase::Running), vec![], None);
        let mut i = input(Phase::Running);
        i.should_evict = Some(ShouldEvictResponse {
            reason: "DeadlineExceeded".into(),
            message: "too long".into(),
        });
        let s = generate_api_pod_status(&pod, i);
        assert_eq!(s.phase, Some(Phase::Failed));
        assert_eq!(s.reason.as_deref(), Some("DeadlineExceeded"));
        assert_eq!(s.message.as_deref(), Some("too long"));
    }

    #[test]
    fn evict_cannot_leave_a_terminal_api_phase() {
        // The terminal guard (:1938-1945) runs after ShouldEvict (:1928).
        let pod = pod_with(Some(Phase::Succeeded), vec![], None);
        let mut i = input(Phase::Running);
        i.should_evict = Some(ShouldEvictResponse {
            reason: "DeadlineExceeded".into(),
            message: "m".into(),
        });
        let s = generate_api_pod_status(&pod, i);
        assert_eq!(s.phase, Some(Phase::Succeeded));
        assert_eq!(s.reason.as_deref(), Some("DeadlineExceeded"));
    }

    #[test]
    fn terminal_phase_kept_reason_preserved() {
        // Phase forced to the terminal API phase, so it equals the old phase
        // and the old reason/message are preserved (:1915-1926).
        let mut pod = pod_with(Some(Phase::Failed), vec![], None);
        pod.status.as_mut().unwrap().reason = Some("Evicted".into());
        let mut i = input(Phase::Running);
        i.message = None;
        let s = generate_api_pod_status(&pod, i);
        assert_eq!(s.phase, Some(Phase::Failed));
        assert_eq!(s.reason.as_deref(), Some("Evicted"));
    }

    #[test]
    fn active_deadline_should_evict_matches_upstream() {
        // active_deadline.go:70-80; boundary is >= (Since(start) >= allowed).
        use chrono::Duration;
        let now = chrono::Utc::now();
        let mut pod = Pod::new("p", Default::default());
        pod.spec = Some(rusternetes_common::resources::PodSpec {
            active_deadline_seconds: Some(5),
            ..Default::default()
        });
        assert_eq!(active_deadline_should_evict(&pod, now), None);
        pod.status = Some(PodStatus {
            start_time: Some(now - Duration::seconds(4)),
            ..Default::default()
        });
        assert_eq!(active_deadline_should_evict(&pod, now), None);
        pod.status.as_mut().unwrap().start_time = Some(now - Duration::seconds(5));
        let r = active_deadline_should_evict(&pod, now).unwrap();
        assert_eq!(r.reason, "DeadlineExceeded");
        pod.spec.as_mut().unwrap().active_deadline_seconds = None;
        assert_eq!(active_deadline_should_evict(&pod, now), None);
    }

    #[test]
    fn carries_over_fields_the_kubelet_does_not_generate() {
        let mut pod = pod_with(Some(Phase::Pending), vec![], None);
        pod.status.as_mut().unwrap().nominated_node_name = Some("n1".into());
        let s = generate_api_pod_status(&pod, input(Phase::Running));
        assert_eq!(s.nominated_node_name.as_deref(), Some("n1"));
    }
    fn running_pod_with_state() -> Pod {
        let mut pod = pod_with(
            Some(Phase::Running),
            vec![cond("Ready", "True"), cond("DisruptionTarget", "True")],
            Some(3),
        );
        let st = pod.status.as_mut().unwrap();
        st.pod_ip = Some("10.1.0.5".into());
        st.host_ip = Some("10.0.0.1".into());
        st.qos_class = Some("Burstable".into());
        st.start_time = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
        st.container_statuses = Some(vec![serde_json::from_value(
            serde_json::json!({"name": "c", "ready": true, "restartCount": 0, "image": "i", "imageID": ""}),
        )
        .unwrap()]);
        st.allocated_resources = Some(Default::default());
        pod
    }

    #[test]
    fn from_prior_changes_only_phase_reason_message() {
        // kubelet_pods.go:1895-1904 over an unchanged runtime snapshot.
        let pod = running_pod_with_state();
        let prior = pod.status.clone().unwrap();
        let i = ApiPodStatusInput::from_prior(
            &pod,
            Phase::Running,
            Some("Restarting".into()),
            Some("Liveness probe failed".into()),
            (None, None),
            chrono::Utc::now(),
        );
        let s = generate_api_pod_status(&pod, i);
        assert_eq!(s.reason.as_deref(), Some("Restarting"));
        assert_eq!(s.message.as_deref(), Some("Liveness probe failed"));
        assert_eq!(s.pod_ip, prior.pod_ip);
        assert_eq!(s.host_ip, prior.host_ip);
        assert_eq!(s.qos_class, prior.qos_class);
        assert_eq!(s.start_time, prior.start_time);
        assert_eq!(
            s.container_statuses.as_ref().map(|c| c[0].name.clone()),
            Some("c".to_string())
        );
        assert_eq!(s.allocated_resources, prior.allocated_resources);
        assert_eq!(s.observed_generation, Some(3));
        let types: Vec<_> = s
            .conditions
            .unwrap()
            .into_iter()
            .map(|c| c.condition_type)
            .collect();
        assert_eq!(types, vec!["DisruptionTarget", "Ready"]);
    }

    #[test]
    fn from_prior_preserves_reason_when_phase_unchanged() {
        // kubelet_pods.go:1915-1926: None reason/message keep the old ones.
        let mut pod = running_pod_with_state();
        pod.status.as_mut().unwrap().reason = Some("Restarting".into());
        let i = ApiPodStatusInput::from_prior(
            &pod,
            Phase::Running,
            None,
            None,
            (None, None),
            chrono::Utc::now(),
        );
        let s = generate_api_pod_status(&pod, i);
        assert_eq!(s.reason.as_deref(), Some("Restarting"));
    }

    #[test]
    fn from_prior_cannot_leave_terminal_phase() {
        // kubelet_pods.go:1938-1945.
        let mut pod = running_pod_with_state();
        pod.status.as_mut().unwrap().phase = Some(Phase::Succeeded);
        let i = ApiPodStatusInput::from_prior(
            &pod,
            Phase::Running,
            None,
            None,
            (None, None),
            chrono::Utc::now(),
        );
        assert_eq!(
            generate_api_pod_status(&pod, i).phase,
            Some(Phase::Succeeded)
        );
    }

    #[test]
    fn from_prior_without_status_uses_host_fallback_and_now() {
        let pod = Pod::new("p", Default::default());
        let now = chrono::Utc::now();
        let i = ApiPodStatusInput::from_prior(
            &pod,
            Phase::Running,
            None,
            None,
            (Some("10.0.0.9".into()), None),
            now,
        );
        let s = generate_api_pod_status(&pod, i);
        assert_eq!(s.host_ip.as_deref(), Some("10.0.0.9"));
        assert_eq!(s.start_time, Some(now));
    }

    /// `rejectPod` (kubelet.go:2475-2482) + `SetPodStatus`
    /// (status_manager.go:425-439): a minimal Failed status, not the output of
    /// `generateAPIPodStatus`; kubelet-owned conditions and observed fields are
    /// dropped, others kept.
    #[test]
    fn rejected_status_is_minimal_failed() {
        let mut pod = pod_with(
            Some(Phase::Pending),
            vec![
                cond("PodScheduled", "True"),
                cond("DisruptionTarget", "True"),
            ],
            Some(4),
        );
        pod.status.as_mut().unwrap().host_ip = Some("10.0.0.1".into());
        let s = rejected_pod_status(&pod, "HostPortConflict", "port taken");
        assert_eq!(s.phase, Some(Phase::Failed));
        assert_eq!(s.reason.as_deref(), Some("HostPortConflict"));
        assert_eq!(s.message.as_deref(), Some("Pod was rejected: port taken"));
        assert_eq!(s.observed_generation, Some(4));
        assert_eq!(s.qos_class.as_deref(), Some("BestEffort"));
        assert_eq!(s.host_ip, None);
        assert_eq!(s.start_time, None);
        let conds = s.conditions.unwrap();
        assert_eq!(conds.len(), 1);
        assert_eq!(conds[0].condition_type, "DisruptionTarget");
    }
}
