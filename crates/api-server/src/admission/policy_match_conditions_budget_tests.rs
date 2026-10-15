//! `RuntimeCELCostBudgetMatchConditions` enforcement of the matchConditions
//! matcher (#3313): `matcher.go:82-85` passes
//! `celconfig.RuntimeCELCostBudgetMatchConditions` to `ForInput`, and
//! `condition.go:103-109` threads the remaining budget through every
//! `activation.Evaluate` (`activation.go:132-189`).

use super::*;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use serde_json::json;

fn attrs(object: Value) -> (Attributes, VersionedAttributes) {
    let gvk = GroupVersionKind {
        group: "apps".into(),
        version: "v1".into(),
        kind: "Deployment".into(),
    };
    let a = Attributes {
        kind: gvk.clone(),
        resource: GroupVersionResource {
            group: "apps".into(),
            version: "v1".into(),
            resource: "deployments".into(),
        },
        subresource: String::new(),
        namespace: "default".into(),
        name: "foo".into(),
        operation: Operation::Create,
        object: Some(object.clone()),
        old_object: None,
        user_info: None,
        dry_run: false,
        operation_options: None,
    };
    let v = VersionedAttributes {
        versioned_kind: gvk,
        versioned_object: Some(object),
        versioned_old_object: None,
        dirty: false,
    };
    (a, v)
}

fn cond(name: &str, expr: &str) -> MatchCondition {
    MatchCondition {
        name: name.into(),
        expression: expr.into(),
    }
}

/// An object whose `xs` makes `object.xs.all(i, i >= 0)` cost more than the
/// per-call limit (1,000,000), so each such condition spends ~1M of the 2.5M
/// match-conditions budget while only failing as an evaluation error.
fn heavy_object() -> Value {
    let xs: Vec<u32> = (0..250_000).collect();
    json!({"xs": xs})
}

const HEAVY: &str = "object.xs.all(i, i >= 0)";

/// matcher.go:86-95 with `Fail`: the budget error fails the request.
#[tokio::test]
async fn exhausting_the_budget_fails_the_request_with_fail_policy() {
    let (a, v) = attrs(heavy_object());
    let m = new_matcher(
        &[cond("a", HEAVY), cond("b", HEAVY), cond("c", HEAVY)],
        &[],
        None,
        "test",
    );
    let r = m
        .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
        .await;
    assert!(!r.matches, "{r:?}");
    assert_eq!(
        r.error.as_deref(),
        Some(
            "validation failed due to running out of cost budget, no further validation rules will be run"
        )
    );
}

/// matcher.go:86-95 with `Ignore`: the policy is skipped.
#[tokio::test]
async fn exhausting_the_budget_skips_the_policy_with_ignore() {
    let (a, v) = attrs(heavy_object());
    let m = new_matcher(
        &[cond("a", HEAVY), cond("b", HEAVY), cond("c", HEAVY)],
        &[],
        Some(&FailurePolicy::Ignore),
        "test",
    );
    let r = m
        .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
        .await;
    assert!(!r.matches && r.error.is_none(), "{r:?}");
}

/// Two over-limit conditions spend ~2M; they still fit the budget and are
/// ordinary per-expression evaluation errors (activation.go:176-180).
#[tokio::test]
async fn per_call_limit_is_an_evaluation_error_within_the_budget() {
    let (a, v) = attrs(heavy_object());
    let m = new_matcher(&[cond("a", HEAVY), cond("b", HEAVY)], &[], None, "test");
    let r = m
        .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
        .await;
    let err = r
        .error
        .expect("evaluation errors resort to the fail policy");
    assert!(
        err.contains("operation cancelled: actual cost limit exceeded"),
        "{err}"
    );
    assert!(!err.contains("running out of cost budget"), "{err}");
}

/// composition.go:191-198, :217-225: a composited variable is evaluated (and
/// its cost reported) once, when first read, however many conditions read it.
#[tokio::test]
async fn variable_cost_is_charged_once() {
    let (a, v) = attrs(heavy_object());
    let m = new_matcher(
        &[
            cond("a", "variables.v"),
            cond("b", "variables.v"),
            cond("c", "variables.v"),
        ],
        &[("v".into(), HEAVY.into())],
        None,
        "test",
    );
    let r = m
        .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
        .await;
    let err = r.error.expect("the variable fails to evaluate");
    assert!(!err.contains("running out of cost budget"), "{err}");
}

/// A cheap condition set is unaffected.
#[tokio::test]
async fn cheap_conditions_stay_within_the_budget() {
    let (a, v) = attrs(json!({"xs": [1, 2, 3]}));
    let m = new_matcher(
        &[cond("a", "object.xs.all(i, i > 0)"), cond("b", "true")],
        &[],
        None,
        "test",
    );
    let r = m
        .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
        .await;
    assert!(r.matches && r.error.is_none(), "{r:?}");
}
