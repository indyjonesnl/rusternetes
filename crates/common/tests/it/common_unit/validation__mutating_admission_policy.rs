//! Unit tests moved from `src/validation/mutating_admission_policy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::mutating_admission_policy::{
        MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding,
        MutatingAdmissionPolicyBindingSpec, MutatingAdmissionPolicySpec, Mutation, PatchType,
        ReinvocationPolicyType,
    };
    pub use rusternetes_common::resources::validating_admission_policy::{
        FailurePolicy, MatchResources,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_dns1123_subdomain;
    pub use rusternetes_common::validation::validating_admission_policy::{
        operation_str, set_defaults_match_resources, validate_match_conditions,
        validate_match_resources, validate_param_kind, validate_param_ref, validate_variable,
    };
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::mutating_admission_policy::*;
    use serde_json::json;

    fn policy(spec: serde_json::Value) -> MutatingAdmissionPolicy {
        serde_json::from_value(json!({"metadata": {"name": "p"}, "spec": spec})).unwrap()
    }

    /// `ignoreMutatingAdmissionPolicyMatchConditions` (validation.go:630-638).
    #[test]
    fn ignore_match_conditions_only_when_param_kind_and_conditions_unchanged() {
        let conds = json!([{"name": "c", "expression": "true"}]);
        let kind = json!({"kind": "K", "apiVersion": "v1"});
        let old = policy(json!({"matchConditions": conds, "paramKind": kind}));
        assert!(ignore_mutating_admission_policy_match_conditions(
            &old, &old
        ));
        let other = policy(
            json!({"matchConditions": [{"name": "c", "expression": "false"}], "paramKind": kind}),
        );
        assert!(!ignore_mutating_admission_policy_match_conditions(
            &other, &old
        ));
        let no_kind = policy(json!({"matchConditions": conds}));
        assert!(!ignore_mutating_admission_policy_match_conditions(
            &no_kind, &old
        ));
        // Semantic.DeepEqual: nil and empty are equal.
        assert!(ignore_mutating_admission_policy_match_conditions(
            &policy(json!({"matchConditions": []})),
            &policy(json!({}))
        ));
    }

    /// `findMutatingPolicyPreexistingExpressions` (validation.go:326-340).
    #[test]
    fn preexisting_expressions_are_collected_from_the_old_policy() {
        let old = policy(json!({
            "matchConditions": [{"name": "c", "expression": "m"}],
            "mutations": [
                {"patchType": "ApplyConfiguration", "applyConfiguration": {"expression": "a"}},
                {"patchType": "JSONPatch", "jsonPatch": {"expression": "j"}}
            ]
        }));
        let got = find_mutating_policy_preexisting_expressions(&old);
        assert_eq!(
            got.match_condition_expressions,
            HashSet::from(["m".to_string()])
        );
        assert_eq!(
            got.apply_configuration_expressions,
            HashSet::from(["a".to_string()])
        );
        assert_eq!(got.json_patch_expressions, HashSet::from(["j".to_string()]));
    }
}
