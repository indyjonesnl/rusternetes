//! Unit tests moved from `src/registry/policy/poddisruptionbudget.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::resources::{PodDisruptionBudget, PodDisruptionBudgetStatus};
    pub use rusternetes_common::types::{LabelSelector, LabelSelectorRequirement};
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        validate_label_selector, LabelSelectorValidationOptions,
    };
    pub use rusternetes_common::validation::pdb::{
        validate_pod_disruption_budget, validate_pod_disruption_budget_status_update,
        PodDisruptionBudgetValidationOptions,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::policy::poddisruptionbudget::*;
    use rusternetes_common::resources::IntOrString;

    fn pdb(spec: serde_json::Value) -> PodDisruptionBudget {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
            "metadata": {"name": "abc", "namespace": "default", "resourceVersion": "10"},
            "spec": spec
        }))
        .unwrap()
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    /// `TestPodDisruptionBudgetStrategy` (strategy_test.go:29-119).
    #[test]
    fn strategy_matches_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(!Strategy.allow_unconditional_update());

        let mut old = pdb(serde_json::json!({
            "minAvailable": 3, "selector": {"matchLabels": {"a": "b"}},
            "unhealthyPodEvictionPolicy": "AlwaysAllow"
        }));
        Strategy.prepare_for_create(&ctx(), &mut old);
        assert_eq!(old.metadata.generation, Some(1));
        assert!(Strategy.validate(&ctx(), &old).is_empty());

        let mut new = old.clone();
        new.status.as_mut().unwrap().current_healthy = 3;
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status, old.status, "an update may not set status");
        assert_eq!(new.metadata.generation, Some(1), "spec unchanged");
        assert!(Strategy.validate_update(&ctx(), &new, &old).is_empty());

        let mut swapped = old.clone();
        swapped.spec.min_available = None;
        swapped.spec.max_unavailable = Some(IntOrString::String("28%".into()));
        Strategy.prepare_for_update(&ctx(), &mut swapped, &old);
        assert_eq!(swapped.metadata.generation, Some(2));
        assert!(Strategy.validate_update(&ctx(), &swapped, &old).is_empty());

        let mut invalid = old.clone();
        invalid.spec.unhealthy_pod_eviction_policy = Some("invalid".into());
        assert!(!Strategy.validate_update(&ctx(), &invalid, &old).is_empty());
    }

    /// `TestPodDisruptionBudgetStatusStrategy` (strategy_test.go:121-171).
    #[test]
    fn status_strategy_keeps_spec() {
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(!StatusStrategy.allow_unconditional_update());
        let mut old = pdb(serde_json::json!({
            "minAvailable": 3, "selector": {"matchLabels": {"a": "b"}}
        }));
        old.status = Some(PodDisruptionBudgetStatus {
            current_healthy: 3,
            desired_healthy: 3,
            disruptions_allowed: 1,
            expected_pods: 3,
            observed_generation: None,
            conditions: None,
            disrupted_pods: None,
        });
        let mut new = old.clone();
        new.spec.min_available = Some(IntOrString::Int(2));
        new.status.as_mut().unwrap().current_healthy = 2;
        new.status.as_mut().unwrap().disruptions_allowed = 0;
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status.as_ref().unwrap().current_healthy, 2);
        assert_eq!(new.spec.min_available, Some(IntOrString::Int(3)));
        assert!(StatusStrategy
            .validate_update(&ctx(), &new, &old)
            .is_empty());
    }

    /// A selector stored with an invalid label value may be kept on update,
    /// but a new one may not introduce it (strategy.go:112-118).
    #[test]
    fn an_invalid_selector_value_survives_only_if_already_stored() {
        let bad = pdb(serde_json::json!({"selector": {"matchExpressions": [
            {"key": "a", "operator": "In", "values": ["-bad-"]}
        ]}}));
        let good = pdb(serde_json::json!({"selector": {"matchLabels": {"a": "b"}}}));
        assert!(!Strategy.validate(&ctx(), &bad).is_empty());
        assert!(Strategy.validate_update(&ctx(), &bad, &bad).is_empty());
        assert!(!Strategy.validate_update(&ctx(), &bad, &good).is_empty());
    }

    /// `Convert_v1_PodDisruptionBudget_To_policy_PodDisruptionBudget`
    /// (conversion.go:26-42) and its test cases (conversion_test.go).
    #[test]
    fn conversion_strips_the_v1beta1_label_unless_match_none() {
        let mut none = pdb(serde_json::json!({"selector": {"matchExpressions": [
            {"key": PDB_V1BETA1_LABEL, "operator": "Exists"}
        ]}}));
        let before = none.spec.selector.clone();
        convert_to_internal(&mut none);
        assert_eq!(none.spec.selector, before, "match-none is preserved");

        // "v1 to internal with match all selector": the DoesNotExist form
        // v1beta1 used for match-all is stripped to `{}`.
        let mut all = pdb(serde_json::json!({"selector": {"matchExpressions": [
            {"key": PDB_V1BETA1_LABEL, "operator": "DoesNotExist"}
        ]}}));
        convert_to_internal(&mut all);
        assert_eq!(all.spec.selector, Some(LabelSelector::default()));

        let mut nil = pdb(serde_json::json!({}));
        convert_to_internal(&mut nil);
        assert_eq!(nil.spec.selector, None);

        let mut mixed = pdb(serde_json::json!({"selector": {
            "matchLabels": {"a": "b"},
            "matchExpressions": [{"key": PDB_V1BETA1_LABEL, "operator": "Exists"}]
        }}));
        convert_to_internal(&mut mixed);
        let sel = mixed.spec.selector.unwrap();
        assert!(sel.match_expressions.is_none(), "{sel:?}");
        assert_eq!(sel.match_labels.unwrap()["a"], "b");
    }
}
