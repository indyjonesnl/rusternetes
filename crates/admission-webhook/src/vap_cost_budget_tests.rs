//! `RuntimeCELCostBudget` threaded across a ValidatingAdmissionPolicy's
//! variables, validations, messageExpressions and auditAnnotations (#3588,
//! #3507 item 2).
//!
//! Upstream: `validating/validator.go` `Validate` (:73-218),
//! `cel/condition.go` `ForInput` (:90-111), `cel/activation.go` `Evaluate`
//! (:132-189), tests in `validating/validator_test.go` (`messageExpression
//! out of budget after successful eval of expression`).

use crate::cel_evaluators::{
    audit_annotations_budgeted, validate_budgeted, AuditAnnotationOutcome, ValidationItem,
    ValidationOutcome, VariableCosts,
};
use rusternetes_common::cel::{CELContext, CELEvaluator};
use rusternetes_common::cel_cost::CostOptions;
use serde_json::json;

/// Costs ~0.95M of the 10M budget and succeeds (under the 1M per-call limit).
const HEAVY: &str = "object.xs.all(i, i >= 0)";

fn ctx() -> CELContext {
    let mut c = CELContext::new();
    let xs: Vec<u32> = (0..190_000).collect();
    c.add_json_variable("object", &json!({"xs": xs})).unwrap();
    c
}

fn item<'a>(expression: &'a str, message_expression: Option<&'a str>) -> ValidationItem<'a> {
    ValidationItem {
        expression,
        message_expression,
        message: Some("static"),
    }
}

fn run(
    items: &[ValidationItem<'_>],
    vars: &mut VariableCosts,
) -> Result<Vec<ValidationOutcome>, String> {
    let c = ctx();
    validate_budgeted(items, &mut CELEvaluator::new(), &c, &c, vars)
}

#[test]
fn heavy_expression_costs_what_the_tests_assume() {
    let (r, cost) = CELEvaluator::new().evaluate_with_cost(HEAVY, &ctx(), CostOptions::default());
    assert!(r.is_ok(), "{r:?}");
    assert!(cost > 500_000 && cost < 1_000_000, "cost {cost}");
}

#[test]
fn validations_share_one_budget_and_out_of_budget_is_one_error() {
    let mut items = Vec::new();
    for _ in 0..30 {
        items.push(item(HEAVY, None));
    }
    let err = run(&items, &mut VariableCosts::default()).unwrap_err();
    assert_eq!(
        err,
        "validation failed due to running out of cost budget, no further validation rules will be run"
    );
}

#[test]
fn validations_within_budget_pass() {
    let items = [item(HEAVY, None), item("true", None)];
    let out = run(&items, &mut VariableCosts::default()).unwrap();
    assert!(matches!(
        out.as_slice(),
        [ValidationOutcome::Pass, ValidationOutcome::Pass]
    ));
}

// validator_test.go "messageExpression out of budget after successful eval of
// expression": the messageExpressions run on what the validations left, and
// running out turns the decision into `failed messageExpression: ...`.
#[test]
fn message_expression_out_of_budget_after_validations() {
    let mut items = Vec::new();
    // ~9.5M for the validations, then a failing validation whose
    // messageExpression needs another ~0.95M.
    for _ in 0..10 {
        items.push(item(HEAVY, None));
    }
    items.push(item("false", Some(HEAVY)));
    let out = run(&items, &mut VariableCosts::default());
    let out = match out {
        Ok(o) => o,
        Err(e) => panic!("validations alone must fit the budget: {e}"),
    };
    match out.last().unwrap() {
        ValidationOutcome::Error { message } => assert_eq!(
            message,
            "failed messageExpression: validation failed due to running out of cost budget, no further validation rules will be run"
        ),
        other => panic!("{other:?}"),
    }
    // Every other decision that did not error is an error too (:136-139).
    assert!(out
        .iter()
        .all(|o| matches!(o, ValidationOutcome::Error { .. })));
}

#[test]
fn message_expression_in_budget_resolves_message() {
    let items = [
        item("false", Some("'bad ' + string(size(object.xs))")),
        item("false", Some("'a\\nb'")),
        item("false", Some("'  '")),
        item("false", None),
    ];
    let out = run(&items, &mut VariableCosts::default()).unwrap();
    let msgs: Vec<_> = out
        .iter()
        .map(|o| match o {
            ValidationOutcome::Fail { message } => message.as_str(),
            other => panic!("{other:?}"),
        })
        .collect();
    // Multi-line and blank results fall back to `message` (:154-181).
    assert_eq!(msgs, ["bad 190000", "static", "static", "static"]);
}

#[test]
fn fallback_message_is_the_failed_expression() {
    let items = [ValidationItem {
        expression: " false ",
        message_expression: None,
        message: None,
    }];
    match &run(&items, &mut VariableCosts::default()).unwrap()[0] {
        ValidationOutcome::Fail { message } => assert_eq!(message, "failed expression: false"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn broken_message_expression_is_not_fatal() {
    let items = [item("false", Some("no_such_var"))];
    match &run(&items, &mut VariableCosts::default()).unwrap()[0] {
        ValidationOutcome::Fail { message } => assert_eq!(message, "static"),
        other => panic!("{other:?}"),
    }
}

// activation.go:132-141: a variable's cost is charged to the first expression
// that uses it, once (the value is memoized across expressions).
#[test]
fn variable_cost_is_charged_once_on_first_reference() {
    let mut vars = VariableCosts::default();
    vars.record("a", HEAVY, 900_000);
    vars.record("b", "variables.a", 10);
    assert_eq!(vars.composition_cost("true"), 0);
    assert_eq!(vars.composition_cost("variables.b == 1"), 900_010);
    assert_eq!(vars.composition_cost("variables.a == 1"), 0);
}

#[test]
fn variable_cost_counts_against_the_budget() {
    // Eleven validations referencing a 1M variable once: 1M + ~0.01M each
    // fits; but a 10M variable cannot even run one validation.
    let mut vars = VariableCosts::default();
    vars.record("big", HEAVY, 10_000_001);
    let err = run(&[item("true || variables.big", None)], &mut vars).unwrap_err();
    assert!(err.contains("running out of cost budget"), "{err}");
    let mut vars = VariableCosts::default();
    vars.record("big", HEAVY, 9_000_000);
    assert!(run(&[item("variables.big == 1", None)], &mut vars).is_ok());
}

// validator.go:184: auditAnnotations get a fresh budget, not the remainder.
#[test]
fn audit_annotations_have_their_own_budget() {
    let c = ctx();
    let mut vars = VariableCosts::default();
    let mut ev = CELEvaluator::new();
    let items: Vec<(&str, &str)> = (0..5).map(|_| ("k", "'v'")).collect();
    let out = audit_annotations_budgeted(&items, &mut ev, &c, &mut vars).unwrap();
    assert!(out
        .iter()
        .all(|o| matches!(o, AuditAnnotationOutcome::Emit { .. })));

    let heavy: Vec<(&str, &str)> = (0..30)
        .map(|_| ("k", "object.xs.all(i, i >= 0) ? 'v' : 'w'"))
        .collect();
    let err = audit_annotations_budgeted(&heavy, &mut ev, &c, &mut vars).unwrap_err();
    assert!(err.contains("running out of cost budget"), "{err}");
}

#[test]
fn audit_annotation_null_and_blank_are_skipped() {
    let c = ctx();
    let items = [("a", "null"), ("b", "'  '"), ("c", "' x '")];
    let out = audit_annotations_budgeted(
        &items,
        &mut CELEvaluator::new(),
        &c,
        &mut VariableCosts::default(),
    )
    .unwrap();
    assert!(matches!(out[0], AuditAnnotationOutcome::Skip));
    assert!(matches!(out[1], AuditAnnotationOutcome::Skip));
    assert!(matches!(&out[2], AuditAnnotationOutcome::Emit { value, .. } if value == "x"));
}
