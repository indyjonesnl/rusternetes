//! Unit tests moved from `src/validation/pdb.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::policy::{
        IntOrString, PodDisruptionBudget, PodDisruptionBudgetCondition, PodDisruptionBudgetSpec,
        PodDisruptionBudgetStatus,
    };
    pub use rusternetes_common::types::Condition;
    pub use rusternetes_common::validation::apps::{
        is_not_more_than_100_percent, validate_positive_int_or_percent,
    };
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        validate_conditions, validate_label_selector, LabelSelectorValidationOptions,
    };
    pub use rusternetes_common::validation::objectmeta::validate_nonnegative_field;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::policy::PodDisruptionBudgetSpec;
    use rusternetes_common::types::LabelSelector;
    use rusternetes_common::validation::pdb::*;

    fn empty_status() -> PodDisruptionBudgetStatus {
        PodDisruptionBudgetStatus {
            current_healthy: 0,
            desired_healthy: 0,
            disruptions_allowed: 0,
            expected_pods: 0,
            observed_generation: None,
            conditions: None,
            disrupted_pods: None,
        }
    }

    fn pdb_with_status(status: PodDisruptionBudgetStatus) -> PodDisruptionBudget {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector: Some(LabelSelector {
                match_labels: None,
                match_expressions: None,
            }),
            unhealthy_pod_eviction_policy: None,
        };
        let mut pdb = PodDisruptionBudget::new("pdb", "default", spec);
        pdb.status = Some(status);
        pdb
    }

    #[test]
    fn status_update_accepts_valid_status() {
        let status = empty_status();
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn status_update_no_status_is_ok() {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector: Some(LabelSelector {
                match_labels: None,
                match_expressions: None,
            }),
            unhealthy_pod_eviction_policy: None,
        };
        let pdb = PodDisruptionBudget::new("pdb", "default", spec);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(errs.is_empty());
    }

    #[test]
    fn status_update_rejects_negative_disruptions_allowed() {
        let mut status = empty_status();
        status.disruptions_allowed = -1;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.disruptionsAllowed");
    }

    #[test]
    fn status_update_rejects_negative_current_healthy() {
        let mut status = empty_status();
        status.current_healthy = -5;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.currentHealthy");
    }

    #[test]
    fn status_update_rejects_negative_desired_healthy() {
        let mut status = empty_status();
        status.desired_healthy = -1;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.desiredHealthy");
    }

    #[test]
    fn status_update_rejects_negative_expected_pods() {
        let mut status = empty_status();
        status.expected_pods = -1;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.expectedPods");
    }

    #[test]
    fn status_update_accepts_valid_condition() {
        let mut status = empty_status();
        status.conditions = Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: Some("ok".to_string()),
            observed_generation: Some(1),
        }]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn status_update_rejects_bad_condition_status() {
        let mut status = empty_status();
        status.conditions = Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "Maybe".to_string(),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: None,
            observed_generation: None,
        }]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(
            errs.iter()
                .any(|e| e.field == "status.conditions[0].status"),
            "expected condition status error, got {errs:?}"
        );
    }

    #[test]
    fn status_update_rejects_condition_missing_transition_time() {
        // Empty lastTransitionTime maps to None → required-time check fires.
        let mut status = empty_status();
        status.conditions = Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some(String::new()),
            reason: Some("SufficientPods".to_string()),
            message: None,
            observed_generation: None,
        }]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(
            errs.iter()
                .any(|e| e.field == "status.conditions[0].lastTransitionTime"),
            "expected lastTransitionTime required error, got {errs:?}"
        );
    }

    #[test]
    fn status_update_rejects_duplicate_condition_types() {
        let mut status = empty_status();
        let cond = PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: None,
            observed_generation: None,
        };
        status.conditions = Some(vec![cond.clone(), cond]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(
            errs.iter().any(|e| e.field == "status.conditions[1].type"),
            "expected duplicate-type error, got {errs:?}"
        );
    }
}
