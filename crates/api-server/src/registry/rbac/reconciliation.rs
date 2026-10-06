//! Port of `staging/src/k8s.io/component-helpers/auth/rbac/reconciliation/`
//! (`reconcile_role.go` `computeReconciledRole`, `aggregationRuleCovers`,
//! `merge`; `reconcile_rolebindings.go` `computeReconciledRoleBinding`,
//! `diffSubjectLists`), release-1.35. Used by the bootstrap policy
//! (`EnsureRBACPolicy`, `pkg/registry/rbac/rest/storage_rbac.go`), which runs
//! with `RemoveExtraPermissions`/`RemoveExtraSubjects` unset, so only the
//! additive path is ported; the objects are handled as JSON so fields this
//! crate does not model survive the round trip.

use rusternetes_common::resources::rbac::{PolicyRule, Subject};
use rusternetes_common::types::LabelSelector;
use serde_json::{Map, Value};

use super::policy_comparator::covers;

/// `rbacv1.AutoUpdateAnnotationKey`.
pub const AUTOUPDATE_ANNOTATION: &str = "rbac.authorization.kubernetes.io/autoupdate";

/// `ReconcileOperation` (reconcile_role.go:33-40). `Create` is the caller's
/// not-found case and has no counterpart here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileOperation {
    Update,
    Recreate,
    None,
}

/// `ReconcileClusterRoleResult` / `ReconcileClusterRoleBindingResult`.
#[derive(Debug, Clone)]
pub struct ReconcileResult {
    /// The object to write (for `Recreate`, the expected object).
    pub object: Value,
    pub operation: ReconcileOperation,
    /// `autoupdate: "false"`: the caller must not write `object`.
    pub protected: bool,
}

fn autoupdate_false(existing: &Value) -> bool {
    existing["metadata"]["annotations"][AUTOUPDATE_ANNOTATION].as_str() == Some("false")
}

/// `merge(expected, existing)` (reconcile_role.go:255-269): later maps win; the
/// result is nil only when every input is nil.
fn merge(first: Option<&Map<String, Value>>, second: Option<&Map<String, Value>>) -> Option<Value> {
    if first.is_none() && second.is_none() {
        return None;
    }
    let mut out = Map::new();
    for m in [first, second].into_iter().flatten() {
        for (k, v) in m {
            out.insert(k.clone(), v.clone());
        }
    }
    Some(Value::Object(out))
}

/// Merge `expected`'s annotations/labels under `existing`'s into `result`;
/// true when that changed anything (`reflect.DeepEqual`, nil != empty).
fn merge_metadata(result: &mut Value, existing: &Value, expected: &Value) -> bool {
    let mut changed = false;
    for field in ["annotations", "labels"] {
        let merged = merge(
            expected["metadata"][field].as_object(),
            existing["metadata"][field].as_object(),
        );
        let before = existing["metadata"].get(field).filter(|v| !v.is_null());
        if merged.as_ref() != before {
            changed = true;
        }
        match merged {
            Some(v) => result["metadata"][field] = v,
            None => {
                if let Some(m) = result["metadata"].as_object_mut() {
                    m.remove(field);
                }
            }
        }
    }
    changed
}

fn rules_of(obj: &Value) -> Vec<PolicyRule> {
    serde_json::from_value(obj["rules"].clone()).unwrap_or_default()
}

fn selectors_of(obj: &Value) -> Option<Vec<LabelSelector>> {
    if obj["aggregationRule"].is_null() {
        return None;
    }
    Some(
        serde_json::from_value(obj["aggregationRule"]["clusterRoleSelectors"].clone())
            .unwrap_or_default(),
    )
}

/// `aggregationRuleCovers` (reconcile_role.go:271-304): the servant's
/// selectors that the owner does not have, by semantic equality.
fn aggregation_rule_uncovered(
    owner: Option<&Vec<LabelSelector>>,
    servant: Option<&Vec<LabelSelector>>,
) -> Vec<LabelSelector> {
    match (owner, servant) {
        (_, None) => Vec::new(),
        (None, Some(servant)) => servant.clone(),
        (Some(owner), Some(servant)) => servant
            .iter()
            .filter(|s| !owner.contains(s))
            .cloned()
            .collect(),
    }
}

/// `computeReconciledRole` (reconcile_role.go:178-248) with
/// `removeExtraPermissions == false`, for a Role or ClusterRole.
pub fn compute_reconciled_role(existing: &Value, expected: &Value) -> ReconcileResult {
    let mut result = existing.clone();
    let mut operation = ReconcileOperation::None;
    let protected = autoupdate_false(existing);

    if merge_metadata(&mut result, existing, expected) {
        operation = ReconcileOperation::Update;
    }

    // `Covers(existing, expected)`: the rules the existing role does not cover.
    let missing = covers(&rules_of(existing), &rules_of(expected)).1;
    if !missing.is_empty() {
        let mut rules = existing["rules"].as_array().cloned().unwrap_or_default();
        rules.extend(
            missing
                .iter()
                .map(|r| serde_json::to_value(r).unwrap_or(Value::Null)),
        );
        result["rules"] = Value::Array(rules);
        operation = ReconcileOperation::Update;
    }

    let expected_sel = selectors_of(expected);
    let existing_sel = selectors_of(existing);
    let missing_sel = aggregation_rule_uncovered(existing_sel.as_ref(), expected_sel.as_ref());
    if expected_sel.is_none() && existing_sel.is_some() {
        if let Some(m) = result.as_object_mut() {
            m.remove("aggregationRule");
        }
        operation = ReconcileOperation::Update;
    } else if !missing_sel.is_empty() {
        let mut selectors = existing_sel.unwrap_or_default();
        selectors.extend(missing_sel);
        result["aggregationRule"] = serde_json::json!({
            "clusterRoleSelectors": serde_json::to_value(selectors).unwrap_or(Value::Null)
        });
        operation = ReconcileOperation::Update;
    }

    ReconcileResult {
        object: result,
        operation,
        protected,
    }
}

fn subjects_of(obj: &Value) -> Vec<Subject> {
    serde_json::from_value(obj["subjects"].clone()).unwrap_or_default()
}

/// `diffSubjectLists(expected, existing)`, first return: subjects only in
/// `expected`, de-duplicated (reconcile_rolebindings.go:182-201).
fn missing_subjects(expected: &[Subject], existing: &[Subject]) -> Vec<Subject> {
    let mut only: Vec<Subject> = Vec::new();
    for item in expected {
        if !existing.contains(item) && !only.contains(item) {
            only.push(item.clone());
        }
    }
    only
}

/// `computeReconciledRoleBinding` (reconcile_rolebindings.go:140-180) with
/// `removeExtraSubjects == false`, for a RoleBinding or ClusterRoleBinding.
pub fn compute_reconciled_role_binding(existing: &Value, expected: &Value) -> ReconcileResult {
    let protected = autoupdate_false(existing);

    let role_ref = |v: &Value| {
        ["apiGroup", "kind", "name"].map(|f| v["roleRef"][f].as_str().unwrap_or("").to_string())
    };
    // roleRef is immutable: recreate with the expected binding.
    if role_ref(expected) != role_ref(existing) {
        return ReconcileResult {
            object: expected.clone(),
            operation: ReconcileOperation::Recreate,
            protected,
        };
    }

    let mut result = existing.clone();
    let mut operation = ReconcileOperation::None;
    if merge_metadata(&mut result, existing, expected) {
        operation = ReconcileOperation::Update;
    }

    let missing = missing_subjects(&subjects_of(expected), &subjects_of(existing));
    if !missing.is_empty() {
        let mut subjects = existing["subjects"].as_array().cloned().unwrap_or_default();
        subjects.extend(
            missing
                .iter()
                .map(|s| serde_json::to_value(s).unwrap_or(Value::Null)),
        );
        result["subjects"] = Value::Array(subjects);
        operation = ReconcileOperation::Update;
    }

    ReconcileResult {
        object: result,
        operation,
        protected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
