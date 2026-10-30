//! Unit tests moved from `src/lifecycle.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{
        ContainerState, ContainerStateTerminated, Lifecycle, Pod,
    };
    pub use rusternetes_common::types::Phase;
    pub use std::collections::HashMap;
    pub use std::time::Duration;
}

mod conflict_predicate_tests {
    use rusternetes_common::Error;
    use rusternetes_kubelet::lifecycle::is_status_write_conflict;

    #[test]
    fn typed_conflict_is_matched() {
        assert!(is_status_write_conflict(&Error::Conflict("x".into())));
    }

    /// Only the Conflict variant is a 409 (`IsConflict`); a different variant
    /// whose text mentions the old tokens must not trigger the CAS retry.
    #[test]
    fn other_variants_with_the_tokens_are_not_matched() {
        assert!(!is_status_write_conflict(&Error::BadRequest(
            "metadata.uid mismatch".into()
        )));
        assert!(!is_status_write_conflict(&Error::Storage(
            "Conflict resolution backend unavailable".into()
        )));
        assert!(!is_status_write_conflict(&Error::NotFound(
            "pod Conflict-test".into()
        )));
    }
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::lifecycle::*;

    fn vol(source: anyhow::Error) -> rusternetes_kubelet::cri_runtime::CriRuntimeError {
        rusternetes_kubelet::cri_runtime::CriRuntimeError::Volumes {
            pod: "p".into(),
            source,
        }
    }

    /// #3513: classification is by error type, never by message text.
    #[test]
    fn volume_wait_error_classification() {
        use anyhow::Context;
        // Typed markers (also behind a later `.context()`) are waits.
        assert!(is_volume_wait_cri(&vol(anyhow::Error::new(
            VolumeWaitError("PersistentVolumeClaim is not bound to a volume".into())
        )
        .context("outer"))));
        let e: anyhow::Result<()> = Err(anyhow::anyhow!("storage down"))
            .with_context(|| VolumeWaitError("Secret foo not found in namespace ns".into()));
        assert!(is_volume_wait_cri(&vol(e.unwrap_err())));
        // The upstream ErrNoPluginMatched error stays a wait.
        assert!(is_volume_wait_cri(&vol(
            rusternetes_kubelet::volumes::NoPluginForVolume("v".into()).into()
        )));
        // Real start failures whose TEXT merely mentions the tokens are not.
        for msg in [
            "CreateContainerConfigError: bad env",
            "port is already allocated",
            "Secret foo not found in namespace ns",
            "PersistentVolumeClaim is not bound to a volume",
            "err=no volume plugin matched",
            "mounter.SetUpAt failed to get CSI client",
        ] {
            assert!(!is_volume_wait_cri(&vol(anyhow::anyhow!(msg))), "{msg}");
        }
        // Non-volume runtime errors never wait.
        assert!(!is_volume_wait_cri(
            &rusternetes_kubelet::cri_runtime::CriRuntimeError::InitContainerFailed {
                name: "i".into(),
                exit_code: 1
            }
        ));
    }

    #[test]
    fn should_run_prestop_threshold() {
        assert!(!should_run_prestop(0));
        assert!(!should_run_prestop(-1));
        assert!(should_run_prestop(1));
        assert!(should_run_prestop(30));
    }

    #[test]
    fn budget_zero_for_force_delete() {
        assert_eq!(compute_prestop_budget(0), Duration::ZERO);
        assert_eq!(compute_prestop_budget(-1), Duration::ZERO);
        assert_eq!(compute_prestop_budget(30), Duration::from_secs(30));
    }

    #[test]
    fn remaining_grace_force_delete_stays_zero() {
        assert_eq!(remaining_grace_after_prestop(0, 0), 0);
        assert_eq!(remaining_grace_after_prestop(0, 5), 0);
    }

    #[test]
    fn remaining_grace_floors_at_minimum_for_graceful_termination() {
        assert_eq!(
            remaining_grace_after_prestop(5, 10),
            MINIMUM_GRACE_PERIOD_SECS
        );
        assert_eq!(remaining_grace_after_prestop(30, 10), 20);
    }

    #[test]
    fn restart_decision_matches_policy_matrix() {
        assert!(should_restart_container(Some("Always"), 0));
        assert!(should_restart_container(Some("Always"), 1));
        assert!(!should_restart_container(Some("OnFailure"), 0));
        assert!(should_restart_container(Some("OnFailure"), 1));
        assert!(!should_restart_container(Some("Never"), 0));
        assert!(!should_restart_container(Some("Never"), 1));
        assert!(should_restart_container(None, 0));
    }

    #[test]
    fn terminal_phase_matches_policy_matrix() {
        assert_eq!(terminal_pod_phase(Some("Never"), true), Some("Failed"));
        assert_eq!(terminal_pod_phase(Some("Never"), false), Some("Succeeded"));
        assert_eq!(
            terminal_pod_phase(Some("OnFailure"), false),
            Some("Succeeded")
        );
        assert_eq!(terminal_pod_phase(Some("OnFailure"), true), None);
        assert_eq!(terminal_pod_phase(Some("Always"), false), None);
        assert_eq!(terminal_pod_phase(Some("Always"), true), None);
    }

    #[test]
    fn phase_is_terminal_only_for_succeeded_and_failed() {
        assert!(phase_is_terminal(Some(&Phase::Succeeded)));
        assert!(phase_is_terminal(Some(&Phase::Failed)));
        assert!(!phase_is_terminal(Some(&Phase::Running)));
        assert!(!phase_is_terminal(Some(&Phase::Pending)));
        assert!(!phase_is_terminal(Some(&Phase::Unknown)));
        assert!(!phase_is_terminal(None));
    }

    #[test]
    fn skip_terminal_succeeded_regression_to_running() {
        // The exact Indexed-Job flake: a Succeeded pod must not flap back to
        // Running (which would let the job controller delete it and drop its
        // completion index).
        assert!(should_skip_phase_write(
            Some(&Phase::Succeeded),
            &Phase::Running
        ));
        assert!(should_skip_phase_write(
            Some(&Phase::Succeeded),
            &Phase::Pending
        ));
    }

    #[test]
    fn allow_same_terminal_phase_reason_update() {
        // Failed -> Failed (new reason/message: eviction, preemption) must
        // still be written.
        assert!(!should_skip_phase_write(
            Some(&Phase::Failed),
            &Phase::Failed
        ));
        assert!(!should_skip_phase_write(
            Some(&Phase::Succeeded),
            &Phase::Succeeded
        ));
    }

    #[test]
    fn allow_nonterminal_progression_to_terminal() {
        // Normal Running -> Succeeded / Pending -> Failed progression.
        assert!(!should_skip_phase_write(
            Some(&Phase::Running),
            &Phase::Succeeded
        ));
        assert!(!should_skip_phase_write(
            Some(&Phase::Running),
            &Phase::Failed
        ));
        assert!(!should_skip_phase_write(
            Some(&Phase::Pending),
            &Phase::Running
        ));
        // No prior phase at all — never skip.
        assert!(!should_skip_phase_write(None, &Phase::Running));
    }

    #[test]
    fn skip_cross_terminal_rewrite() {
        // The first terminal phase wins; never rewrite one terminal into the
        // other.
        assert!(should_skip_phase_write(
            Some(&Phase::Succeeded),
            &Phase::Failed
        ));
        assert!(should_skip_phase_write(
            Some(&Phase::Failed),
            &Phase::Succeeded
        ));
    }

    #[test]
    fn image_action_matches_policy_matrix() {
        assert_eq!(image_action(Some("Always"), true), ImageAction::Pull);
        assert_eq!(image_action(Some("Always"), false), ImageAction::Pull);
        assert_eq!(image_action(Some("Never"), true), ImageAction::UseLocal);
        assert_eq!(
            image_action(Some("Never"), false),
            ImageAction::ErrImageNeverPull
        );
        assert_eq!(
            image_action(Some("IfNotPresent"), true),
            ImageAction::UseLocal
        );
        assert_eq!(image_action(Some("IfNotPresent"), false), ImageAction::Pull);
    }

    #[test]
    fn default_pull_policy_follows_tag_rule() {
        assert_eq!(default_image_pull_policy("nginx"), "Always");
        assert_eq!(default_image_pull_policy("nginx:latest"), "Always");
        assert_eq!(default_image_pull_policy("nginx:1.27"), "IfNotPresent");
        assert_eq!(
            default_image_pull_policy("registry.k8s.io/nginx:1.27"),
            "IfNotPresent"
        );
        assert_eq!(
            default_image_pull_policy("registry:5000/nginx:1.27"),
            "IfNotPresent"
        );
        assert_eq!(default_image_pull_policy("registry:5000/nginx"), "Always");
        assert_eq!(
            default_image_pull_policy("nginx@sha256:deadbeef"),
            "IfNotPresent"
        );
    }

    #[test]
    fn effective_grace_period_defaults_and_clamps() {
        assert_eq!(effective_termination_grace_period(None), 30);
        assert_eq!(effective_termination_grace_period(Some(60)), 60);
        assert_eq!(effective_termination_grace_period(Some(0)), 0);
        assert_eq!(effective_termination_grace_period(Some(-5)), 0);
    }

    #[test]
    fn docker_error_overrides_reason_for_137() {
        let state = terminated_state_from_exit(137, Some("ContainerCannotRun".into()), None);
        match state {
            ContainerState {
                terminated: Some(ContainerStateTerminated { reason, .. }),
                ..
            } => {
                assert_eq!(reason.as_deref(), Some("ContainerCannotRun"));
            }
            _ => panic!("expected Terminated"),
        }
    }
}
