//! `RuntimeCELCostBudget` threaded across a ValidatingAdmissionPolicy's
//! variables, validations, messageExpressions and auditAnnotations (#3588,
//! #3507 item 2).
//!
//! Upstream: `validating/validator.go` `Validate` (:73-218),
//! `cel/condition.go` `ForInput` (:90-111), `cel/activation.go` `Evaluate`
//! (:132-189), tests in `validating/validator_test.go` (`messageExpression
//! out of budget after successful eval of expression`) and
//! `cel/composition_test.go` `TestCompositedPolicies` (lazy variable
//! composition, #3588).

use crate::cel_evaluators::{
    audit_annotations_budgeted, validate_budgeted, validate_with_budget, AuditAnnotationOutcome,
    ValidationItem, ValidationOutcome,
};
use k3rs_common::cel::{CELContext, CELEvaluator};
use k3rs_common::cel_cost::{CostOptions, LazyVariables};
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
    vars: &[(String, String)],
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
    let err = run(&items, &[]).unwrap_err();
    assert_eq!(
        err,
        "validation failed due to running out of cost budget, no further validation rules will be run"
    );
}

#[test]
fn validations_within_budget_pass() {
    let items = [item(HEAVY, None), item("true", None)];
    let out = run(&items, &[]).unwrap();
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
    let out = run(&items, &[]);
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
    let out = run(&items, &[]).unwrap();
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
    match &run(&items, &[]).unwrap()[0] {
        ValidationOutcome::Fail { message } => assert_eq!(message, "failed expression: false"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn broken_message_expression_is_not_fatal() {
    let items = [item("false", Some("no_such_var"))];
    match &run(&items, &[]).unwrap()[0] {
        ValidationOutcome::Fail { message } => assert_eq!(message, "static"),
        other => panic!("{other:?}"),
    }
}

fn defs(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter()
        .map(|(n, e)| (n.to_string(), e.to_string()))
        .collect()
}

fn endpoints_ctx() -> CELContext {
    let mut c = CELContext::new();
    c.add_json_variable(
        "object",
        &json!({"metadata": {"name": "endpoints1"}, "subsets": [{"addresses": []}]}),
    )
    .unwrap();
    c
}

/// `TestCompositedPolicies`: one validation over `variables`, with the
/// `runtimeCELCostBudget` of the case.
fn composited(
    variables: &[(&str, &str)],
    expression: &str,
    budget: i64,
) -> Result<Vec<ValidationOutcome>, String> {
    let c = endpoints_ctx();
    validate_with_budget(
        &[ValidationItem {
            expression,
            message_expression: None,
            message: None,
        }],
        &mut CELEvaluator::new(),
        &c,
        &c,
        &defs(variables),
        budget,
    )
}

const BUDGET: i64 = k3rs_common::cel_cost::RUNTIME_CEL_COST_BUDGET;

// composition_test.go "simple"
#[test]
fn composition_simple() {
    let out = composited(
        &[("name", "object.metadata.name")],
        "variables.name == 'endpoints1'",
        BUDGET,
    )
    .unwrap();
    assert!(matches!(out.as_slice(), [ValidationOutcome::Pass]));
}

// composition_test.go "delayed eval error": the failure is the value of the
// variable, an error only for the expression that reads it.
#[test]
fn composition_delayed_eval_error() {
    let out = composited(
        &[("count", "object.subsets[114514].addresses.size()")],
        "variables.count == 810",
        BUDGET,
    )
    .unwrap();
    match out.as_slice() {
        [ValidationOutcome::Error { message }] => assert!(
            message.contains(r#"composited variable "count" fails to evaluate"#),
            "{message}"
        ),
        other => panic!("{other:?}"),
    }
}

// A variable nothing reads is never evaluated, so its failure is never seen
// (`MapValue.resolveField` runs the callback on first access only).
#[test]
fn composition_unread_failing_variable_is_not_evaluated() {
    let out = composited(
        &[("count", "object.subsets[114514].addresses.size()")],
        "true",
        BUDGET,
    )
    .unwrap();
    assert!(matches!(out.as_slice(), [ValidationOutcome::Pass]));
}

// composition_test.go "out of budget during lazy evaluation": 3 for the
// variable, then the expression does not fit what is left.
#[test]
fn composition_out_of_budget_during_lazy_evaluation() {
    let err = composited(
        &[("name", "object.metadata.name")],
        "variables.name == 'endpoints1'",
        4,
    )
    .unwrap_err();
    assert!(err.contains("running out of cost budget"), "{err}");
}

// composition_test.go "lazy evaluation, budget counts only once": enough for
// one lazy evaluation but not two.
#[test]
fn composition_budget_counts_only_once() {
    let out = composited(
        &[("name", "object.metadata.name")],
        "variables.name == 'endpoints1' && variables.name == 'endpoints1' ",
        10,
    )
    .unwrap();
    assert!(matches!(out.as_slice(), [ValidationOutcome::Pass]));
}

// composition_test.go "single boolean variable in expression"
#[test]
fn composition_single_boolean_variable() {
    let out = composited(
        &[("fortuneTelling", "true")],
        "variables.fortuneTelling",
        BUDGET,
    )
    .unwrap();
    assert!(matches!(out.as_slice(), [ValidationOutcome::Pass]));
}

// composition_test.go "variable of a list" / "variable of a map"
#[test]
fn composition_list_and_map_variables() {
    let out = composited(
        &[("list", "[1, 2, 3, 4]")],
        // upstream: `variables.list.sum() == 10`; the `sum()` list extension is
        // not in this runtime (independent of composition).
        "variables.list.size() == 4 && variables.list[3] == 4",
        BUDGET,
    )
    .unwrap();
    assert!(matches!(out.as_slice(), [ValidationOutcome::Pass]));
    let out = composited(
        &[("dict", r#"{"foo": "bar"}"#)],
        "variables.dict['foo'].contains('bar')",
        BUDGET,
    )
    .unwrap();
    assert!(matches!(out.as_slice(), [ValidationOutcome::Pass]));
}

// composition_test.go "exceeds cost budget"
#[test]
fn composition_exceeds_cost_budget() {
    let err = composited(
        &[("dict", "'abc 123 def 123'.split(' ')")],
        "size(variables.dict) > 0",
        5,
    )
    .unwrap_err();
    assert_eq!(
        err,
        "validation failed due to running out of cost budget, no further validation rules will be run"
    );
}

// `variables["name"]` reads the same lazy field as `variables.name`, and a
// variable reads the ones declared before it (`CompileAndStoreVariable`).
#[test]
fn composition_index_access_and_variable_chain() {
    let out = composited(
        &[
            ("name", "object.metadata.name"),
            ("upper", "variables.name + '!'"),
        ],
        r#"variables["upper"] == 'endpoints1!'"#,
        BUDGET,
    )
    .unwrap();
    assert!(matches!(out.as_slice(), [ValidationOutcome::Pass]));
}

// `&&` / `||` / `?:` short-circuit: a variable on the branch not taken is not
// evaluated and not charged (the point of the lazy `MapValue`).
#[test]
fn composition_short_circuit_does_not_evaluate_or_charge() {
    // The 9M variable would not fit once the expression is charged; it is
    // never read.
    let heavy = format!("{HEAVY} ? 1 : 0");
    let c = ctx();
    let lazy = LazyVariables::new(defs(&[("big", heavy.as_str())]), c.variables.clone());
    let mut ev = CELEvaluator::new();
    let (r, _) = ev.evaluate_with_cost_lazy(
        "true || variables.big == 1",
        &c,
        CostOptions::default(),
        &lazy,
    );
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(lazy.evaluated_count(), 0);
    assert_eq!(lazy.get_and_reset_cost(), 0);

    let (r, _) = ev.evaluate_with_cost_lazy(
        "false ? variables.big == 1 : true",
        &c,
        CostOptions::default(),
        &lazy,
    );
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(lazy.evaluated_count(), 0);

    // Reading it evaluates it once and reports its cost once.
    let (r, _) =
        ev.evaluate_with_cost_lazy("variables.big == 1", &c, CostOptions::default(), &lazy);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(lazy.evaluated_count(), 1);
    let first = lazy.get_and_reset_cost();
    assert!(first > 500_000, "{first}");
    let (_, _) =
        ev.evaluate_with_cost_lazy("variables.big == 1", &c, CostOptions::default(), &lazy);
    assert_eq!(lazy.get_and_reset_cost(), 0);
}

// activation.go:132-141: the variable's cost counts against the budget.
#[test]
fn variable_cost_counts_against_the_budget() {
    // ~0.95M variable fits the 10M budget; with 0.9M it cannot.
    let heavy = [("big", HEAVY)];
    assert!(run(&[item("variables.big", None)], &defs(&heavy)).is_ok());
    let c = ctx();
    let out = validate_with_budget(
        &[item("variables.big", None)],
        &mut CELEvaluator::new(),
        &c,
        &c,
        &defs(&heavy),
        900_000,
    );
    assert!(out.unwrap_err().contains("running out of cost budget"));
}

// validator.go:128: the messageExpressions are a separate `ForInput`, with
// their own activation: a variable they read is evaluated and charged again.
#[test]
fn message_expressions_evaluate_variables_in_their_own_activation() {
    // Validation + message each read the ~0.95M variable: 9 validations that
    // read it once (charged once) + the message (charged again) fit 10M; the
    // variable is not memoized across the two filters.
    let c = ctx();
    let heavy = [("big", HEAVY)];
    let items = [ValidationItem {
        expression: "variables.big ? false : false",
        message_expression: Some("variables.big ? 'm' : 'm'"),
        message: None,
    }];
    // Just over one evaluation, under two: the message cannot afford its own
    // evaluation of the variable.
    let out = validate_with_budget(
        &items,
        &mut CELEvaluator::new(),
        &c,
        &c,
        &defs(&heavy),
        1_400_000,
    )
    .unwrap();
    match out.as_slice() {
        [ValidationOutcome::Error { message }] => {
            assert!(message.starts_with("failed messageExpression"), "{message}")
        }
        other => panic!("{other:?}"),
    }
}

// validator.go:184: auditAnnotations get a fresh budget, not the remainder.
#[test]
fn audit_annotations_have_their_own_budget() {
    let c = ctx();
    let mut ev = CELEvaluator::new();
    let items: Vec<(&str, &str)> = (0..5).map(|_| ("k", "'v'")).collect();
    let out = audit_annotations_budgeted(&items, &mut ev, &c, &[]).unwrap();
    assert!(out
        .iter()
        .all(|o| matches!(o, AuditAnnotationOutcome::Emit { .. })));

    let heavy: Vec<(&str, &str)> = (0..30)
        .map(|_| ("k", "object.xs.all(i, i >= 0) ? 'v' : 'w'"))
        .collect();
    let err = audit_annotations_budgeted(&heavy, &mut ev, &c, &[]).unwrap_err();
    assert!(err.contains("running out of cost budget"), "{err}");
}

#[test]
fn audit_annotation_null_and_blank_are_skipped() {
    let c = ctx();
    let items = [("a", "null"), ("b", "'  '"), ("c", "' x '")];
    let out = audit_annotations_budgeted(&items, &mut CELEvaluator::new(), &c, &[]).unwrap();
    assert!(matches!(out[0], AuditAnnotationOutcome::Skip));
    assert!(matches!(out[1], AuditAnnotationOutcome::Skip));
    assert!(matches!(&out[2], AuditAnnotationOutcome::Emit { value, .. } if value == "x"));
}
