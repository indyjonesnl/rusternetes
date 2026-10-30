//! Unit tests moved from `src/registry/rbac/reconciliation.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::rbac::policy_comparator::covers;
    pub use rusternetes_common::resources::rbac::{PolicyRule, Subject};
    pub use rusternetes_common::types::LabelSelector;
    pub use serde_json::{Map, Value};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::rbac::reconciliation::*;
    use serde_json::json;

    fn role(rules: Value, agg: Option<Value>) -> Value {
        let mut r = json!({"metadata": {"name": "r"}, "rules": rules});
        if let Some(a) = agg {
            r["aggregationRule"] = json!({"clusterRoleSelectors": a});
        }
        r
    }

    /// reconcile_role_test.go `TestComputeReconciledRoleAggregationRules`:
    /// missing selectors are appended, an unwanted aggregationRule is removed.
    #[test]
    fn aggregation_selectors_added_and_removed() {
        let sel = |k: &str| json!({"matchLabels": {k: "true"}});
        let existing = role(json!([]), Some(json!([sel("a")])));
        let expected = role(json!([]), Some(json!([sel("a"), sel("b")])));
        let r = compute_reconciled_role(&existing, &expected);
        assert_eq!(r.operation, ReconcileOperation::Update);
        assert_eq!(
            r.object["aggregationRule"]["clusterRoleSelectors"],
            json!([sel("a"), sel("b")])
        );

        let r = compute_reconciled_role(&existing, &role(json!([]), None));
        assert_eq!(r.operation, ReconcileOperation::Update);
        assert!(r.object.get("aggregationRule").is_none());

        let r = compute_reconciled_role(&existing, &role(json!([]), Some(json!([sel("a")]))));
        assert_eq!(r.operation, ReconcileOperation::None);
    }

    /// `TestComputeReconciledRole`: a covered rule is not added; an uncovered
    /// one is appended (atomic).
    #[test]
    fn rules_use_covers() {
        let get_pods = json!({"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]});
        let all = json!({"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]});
        let r = compute_reconciled_role(&role(json!([all]), None), &role(json!([get_pods]), None));
        assert_eq!(r.operation, ReconcileOperation::None);
        let r = compute_reconciled_role(&role(json!([]), None), &role(json!([get_pods]), None));
        assert_eq!(r.operation, ReconcileOperation::Update);
        assert_eq!(r.object["rules"].as_array().unwrap().len(), 2);
    }

    /// `TestComputeReconciledRoleBindings`: duplicate expected subjects are
    /// added once; existing extras are kept.
    #[test]
    fn binding_subjects_added_once() {
        let s =
            |n: &str| json!({"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": n});
        let rref =
            json!({"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "x"});
        let existing = json!({"metadata": {}, "roleRef": rref, "subjects": [s("extra")]});
        let expected = json!({"metadata": {}, "roleRef": rref, "subjects": [s("a"), s("a")]});
        let r = compute_reconciled_role_binding(&existing, &expected);
        assert_eq!(r.operation, ReconcileOperation::Update);
        assert_eq!(r.object["subjects"], json!([s("extra"), s("a")]));
    }
}
