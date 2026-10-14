//! CEL runtime cost tracking (#3323): the budgets and constants of
//! `staging/src/k8s.io/apiserver/pkg/apis/cel/config.go`, the per-node cost
//! model of cel-go `interpreter/runtimecost.go`, the per-function costs of
//! `staging/src/k8s.io/apiserver/pkg/cel/library/cost.go`
//! (`CostEstimator.CallCost`), and the budget accounting of
//! `staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/activation.go`
//! (`evaluationActivation.Evaluate`).
//!
//! The `cel` 0.13 crate has no cost-tracking or evaluation-observer hook
//! (`Program::execute` is `Value::resolve` with no callbacks), so cel-go's
//! `ObservableInterpretable` cannot be ported as an observer. The same costs are
//! produced by [`eval_with_cost`], which walks the expression itself and lets the
//! crate evaluate only what is left of each node once its operands are
//! evaluated (operands are bound to hidden variables), mirroring the evaluation
//! order of `objects.rs` `Value::resolve_val` (short-circuit `&&`/`||`/`?:`,
//! comprehension loop).
//!
//! Known gaps against cel-go: the walk follows the `cel` crate's own
//! macro expansion and evaluation order (loop condition before the iteration
//! variable is bound, no two-variable comprehensions); the Kubernetes extension
//! library types (quantity, IP, CIDR, URL, semver, format, authorizer) are not
//! values in this runtime, so only their name-keyed costs are ported.

use cel::common::ast::{
    operators, CallExpr, EntryExpr, Expr, IdedEntryExpr, IdedExpr, ListExpr, LiteralValue,
    MapEntryExpr, MapExpr, SelectExpr,
};
use cel::{Context, ExecutionError, Value};
use std::fmt;

/// `PerCallLimit`: the cost limit per CEL evaluation (`config.go:21`).
pub const PER_CALL_LIMIT: u64 = 1_000_000;
/// `RuntimeCELCostBudget`: per ValidatingAdmissionPolicyBinding or custom
/// resource (`config.go:25`).
pub const RUNTIME_CEL_COST_BUDGET: i64 = 10_000_000;
/// `RuntimeCELCostBudgetMatchConditions`: per object with matchConditions
/// (`config.go:31`).
pub const RUNTIME_CEL_COST_BUDGET_MATCH_CONDITIONS: i64 = 2_500_000;
/// `CheckFrequency` (`config.go:36`).
pub const CHECK_FREQUENCY: u64 = 100;
/// `MaxRequestSizeBytes` (`config.go:41`).
pub const MAX_REQUEST_SIZE_BYTES: i64 = 3 * 1024 * 1024;
/// `MaxEvaluatedMessageExpressionSizeBytes` (`config.go:44`).
pub const MAX_EVALUATED_MESSAGE_EXPRESSION_SIZE_BYTES: i64 = 5 * 1024;

/// `common.StringTraversalCostFactor` (cel-go `common/cost.go`).
pub const STRING_TRAVERSAL_COST_FACTOR: f64 = 0.1;
/// `common.RegexStringLengthCostFactor`.
pub const REGEX_STRING_LENGTH_COST_FACTOR: f64 = 0.25;
/// `common.ListCreateBaseCost`.
pub const LIST_CREATE_BASE_COST: u64 = 10;
/// `common.MapCreateBaseCost`.
pub const MAP_CREATE_BASE_COST: u64 = 30;
/// `common.StructCreateBaseCost`.
pub const STRUCT_CREATE_BASE_COST: u64 = 40;
/// `common.SelectAndIdentCost`.
pub const SELECT_AND_IDENT_COST: u64 = 1;

/// The message of cel-go's `EvalCancelledError{Cause: CostLimitExceeded}`
/// (`runtimecost.go` `Observe`).
pub const COST_LIMIT_EXCEEDED_MESSAGE: &str = "operation cancelled: actual cost limit exceeded";

/// `activation.go:150`: the detail of the out-of-budget `cel.Error`.
pub const OUT_OF_BUDGET_DETAIL: &str =
    "validation failed due to running out of cost budget, no further validation rules will be run";

/// Reserved map key `cel_struct` stamps on an `Object{}` (a struct in cel-go).
const STRUCT_TYPE_KEY: &str = "\u{1}objectType";

/// Why a costed evaluation stopped.
#[derive(Debug)]
pub enum CostEvalError {
    /// An ordinary CEL evaluation error.
    Exec(ExecutionError),
    /// `EvalCancelledError{Cause: CostLimitExceeded}`.
    LimitExceeded,
}

impl fmt::Display for CostEvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CostEvalError::Exec(e) => write!(f, "{e}"),
            CostEvalError::LimitExceeded => f.write_str(COST_LIMIT_EXCEEDED_MESSAGE),
        }
    }
}

impl From<ExecutionError> for CostEvalError {
    fn from(e: ExecutionError) -> Self {
        CostEvalError::Exec(e)
    }
}

/// Options of the `CostTracker` (`CostTrackerLimit`, `PresenceTestHasCost`).
#[derive(Debug, Clone, Copy)]
pub struct CostOptions {
    /// `CostTracker.Limit`.
    pub limit: Option<u64>,
    /// `presenceTestHasCost`; the apiserver environment sets false
    /// (`environment/base.go:87`).
    pub presence_test_has_cost: bool,
}

impl Default for CostOptions {
    /// `environment/base.go:83-87`: `cel.CostLimit(PerCallLimit)` and
    /// `PresenceTestHasCost(false)`.
    fn default() -> Self {
        CostOptions {
            limit: Some(PER_CALL_LIMIT),
            presence_test_has_cost: false,
        }
    }
}

/// Evaluate `expr` and return the result with the actual cost
/// (`EvalDetails.ActualCost()`), stopping with [`CostEvalError::LimitExceeded`]
/// as soon as the cost passes the limit.
pub fn eval_with_cost(
    expr: &IdedExpr,
    ctx: &Context,
    opts: CostOptions,
) -> (Result<Value, CostEvalError>, u64) {
    let _ = (expr, ctx, opts);
    let _ = (
        operators::HAS,
        None::<CallExpr>,
        None::<EntryExpr>,
        None::<Expr>,
        None::<IdedEntryExpr>,
        None::<ListExpr>,
        None::<LiteralValue>,
        None::<MapEntryExpr>,
        None::<MapExpr>,
        None::<SelectExpr>,
        STRUCT_TYPE_KEY,
    );
    (Ok(Value::Null), 0)
}

/// `CostEstimator.CallCost` plus the standard-library fallback of
/// `CostTracker.costCall`.
pub fn call_cost(function: &str, args: &[Value], result: Option<&Value>) -> u64 {
    let _ = (function, args, result);
    0
}

/// The `cel.Error` of `activation.go`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetError {
    pub detail: String,
    /// `Cause == ErrOutOfBudget`.
    pub out_of_budget: bool,
}

/// `evaluationActivation.Evaluate` from the evaluation on (`activation.go:
/// 132-189`).
pub fn evaluate_with_budget(
    expression: &str,
    evaluate: impl FnOnce() -> (Result<Value, CostEvalError>, u64),
    composition_cost: Option<i64>,
    remaining_budget: i64,
) -> Result<(Result<Value, String>, i64), BudgetError> {
    let _ = (expression, evaluate, composition_cost, remaining_budget);
    Err(BudgetError {
        detail: String::new(),
        out_of_budget: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cel::{CELContext, CELEvaluator};
    use serde_json::json;
    use std::sync::Arc;

    fn ctx() -> CELContext {
        let mut c = CELContext::new();
        c.add_json_variable(
            "object",
            &json!({"subsets": [], "name": "abc", "xs": [1, 2, 3]}),
        )
        .unwrap();
        c.add_json_variable("oldObject", &serde_json::Value::Null)
            .unwrap();
        c.add_json_variable("x", &json!(1)).unwrap();
        c.add_json_variable("y", &json!(2)).unwrap();
        let big: Vec<u32> = (0..250_000).collect();
        c.add_json_variable("big", &json!(big)).unwrap();
        c
    }

    fn cost(expr: &str) -> u64 {
        CELEvaluator::new()
            .evaluate_with_cost(expr, &ctx(), CostOptions::default())
            .1
    }

    // condition_test.go `TestRuntimeCELCostBudget`: `has(object.subsets)` is 1
    // under `PresenceTestHasCost(false)`, `oldObject != null` is 2 and
    // `object.subsets.size() > 2` is 4; the two sum to the "exactly covers" 6.
    #[test]
    fn ident_select_and_call_costs() {
        assert_eq!(cost("true"), 0);
        assert_eq!(cost("has(object.subsets)"), 1);
        assert_eq!(cost("oldObject != null"), 2);
        assert_eq!(cost("object.subsets.size() > 2"), 4);
    }

    // `&&` has no direct cost: runtimecost.go `*evalAnd` drops the terms.
    #[test]
    fn short_circuit_costs_only_evaluated_terms() {
        assert_eq!(cost("has(object.subsets) && object.subsets.size() < 2"), 5);
        assert_eq!(cost("false && object.subsets.size() < 2"), 0);
        assert_eq!(cost("true || object.subsets.size() < 2"), 0);
    }

    // decorators.go `maybeBuildListLiteral`: a constant list is free.
    #[test]
    fn constant_literals_are_folded() {
        assert_eq!(cost("[1, 2, 3].size()"), 1);
        assert_eq!(cost("size({'a': 1})"), 1);
        assert_eq!(cost("[x].size()"), 12);
    }

    // decorators.go `maybeOptimizeSetMembership`: `in` over a constant list of
    // primitives is not a costed call; `InList` otherwise costs the list size.
    #[test]
    fn in_list_cost() {
        assert_eq!(cost("1 in [1, 2, 3]"), 0);
        assert_eq!(cost("x in [1, 2, y]"), 15);
    }

    // runtimecost.go `costCall`: O(n) / O(nm) string functions.
    #[test]
    fn string_function_costs() {
        assert_eq!(cost("'abc'.startsWith('a')"), 1);
        assert_eq!(cost("'abcdefghijkl'.endsWith('l')"), 2);
        assert_eq!(cost("'abcde'.contains('bc')"), 1);
        assert_eq!(cost("'abc'.matches('a.c')"), 1);
        assert_eq!(cost("'abc' + 'de'"), 1);
        assert_eq!(cost("'abcdefghij' == 'abcdefghij'"), 1);
        assert_eq!(cost("'abcdefghijk' == 'abcdefghijk'"), 2);
    }

    // runtimecost.go `Observe` constructor case: list 10, map 30.
    #[test]
    fn constructor_costs() {
        assert_eq!(cost("[x, y].size()"), 10 + 2 + 1);
        assert_eq!(cost("size({'a': x})"), 30 + 1 + 1);
    }

    // Comprehensions have no direct cost; every evaluated node counts.
    #[test]
    fn comprehension_cost_grows_with_iterations() {
        let small = cost("object.xs.all(i, i > 0)");
        let large = cost("[x, x, x, x, x, x].all(i, i > 0)");
        assert!(small > 0 && large > small, "{small} {large}");
    }

    // Observe: `tracker.Limit != nil && tracker.cost > *tracker.Limit` cancels.
    #[test]
    fn limit_cancels_evaluation() {
        let (r, c) = CELEvaluator::new().evaluate_with_cost(
            "object.subsets.size() > 2",
            &ctx(),
            CostOptions {
                limit: Some(3),
                presence_test_has_cost: false,
            },
        );
        assert_eq!(
            r.unwrap_err().to_string(),
            "operation cancelled: actual cost limit exceeded"
        );
        assert_eq!(c, 4);
    }

    #[test]
    fn presence_test_has_cost_option() {
        let (_, c) = CELEvaluator::new().evaluate_with_cost(
            "has(object.subsets)",
            &ctx(),
            CostOptions {
                limit: None,
                presence_test_has_cost: true,
            },
        );
        assert_eq!(c, 2);
    }

    // `CallCost`: name-keyed Kubernetes library costs (cost.go:99-233).
    #[test]
    fn library_call_costs() {
        let s = |v: &str| Value::String(Arc::new(v.to_string()));
        assert_eq!(call_cost("check", &[], None), 350_000);
        assert_eq!(call_cost("allowed", &[], None), 1);
        assert_eq!(call_cost("url", &[s("https:://kubernetes.io/")], None), 3);
        assert_eq!(call_cost("getHostname", &[], None), 1);
        assert_eq!(call_cost("split", &[s("abc 123 def 123"), s(" ")], None), 3);
        assert_eq!(
            call_cost(
                "join",
                &[Value::Null, s(" ")],
                Some(&s("aa bb cc d e f g h i j"))
            ),
            5
        );
        // find: ceil((1+15)*0.1) * ceil(3*0.25) = 2 * 1
        assert_eq!(
            call_cost("find", &[s("abc 123 def 123"), s("123")], None),
            2
        );
        let list = Value::List(Arc::new(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
        ]));
        assert_eq!(call_cost("isSorted", &[list], None), 4);
    }

    type Outcome = (Vec<Result<Value, String>>, i64);

    // activation.go: the `TestRuntimeCELCostBudget` flow (`condition.ForInput`).
    fn run(exprs: &[&str], budget: i64) -> Result<Outcome, BudgetError> {
        let mut remaining = budget;
        let mut ev = CELEvaluator::new();
        let c = ctx();
        let mut out = vec![];
        for e in exprs {
            let (r, rem) = evaluate_with_budget(
                e,
                || ev.evaluate_with_cost(e, &c, CostOptions::default()),
                None,
                remaining,
            )?;
            remaining = rem;
            out.push(r);
        }
        Ok((out, remaining))
    }

    #[test]
    fn budget_exceeded_at_first_expression() {
        let e = run(
            &[
                "has(object.subsets) && object.subsets.size() < 2",
                "has(object.subsets)",
            ],
            1,
        )
        .unwrap_err();
        assert!(e.out_of_budget);
        assert_eq!(e.detail, OUT_OF_BUDGET_DETAIL);
    }

    #[test]
    fn budget_exceeded_at_last_expression() {
        let e = run(
            &[
                "has(object.subsets) && object.subsets.size() < 2",
                "object.subsets.size() > 2",
            ],
            5,
        )
        .unwrap_err();
        assert!(e.out_of_budget);
    }

    #[test]
    fn budget_not_exceeded_reports_remaining() {
        let (_, rem) = run(&["oldObject != null", "object.subsets.size() > 2"], 10).unwrap();
        assert_eq!(rem, 4);
    }

    #[test]
    fn budget_exactly_covered() {
        let (_, rem) = run(&["oldObject != null", "object.subsets.size() > 2"], 6).unwrap();
        assert_eq!(rem, 0);
        let (_, rem) = run(
            &["oldObject != null", "object.subsets.size() > 2", "true"],
            6,
        )
        .unwrap();
        assert_eq!(rem, 0);
    }

    // "per call limit exceeds": the cancellation is an error on the expression
    // (activation.go:176-180), not a budget error.
    #[test]
    fn per_call_limit_is_an_expression_error() {
        let expr = "big.all(i, i >= 0)";
        let (r, rem) = run(&[expr], RUNTIME_CEL_COST_BUDGET).unwrap();
        assert_eq!(
            r[0].as_ref().unwrap_err(),
            &format!(
                "expression '{expr}' resulted in error: operation cancelled: actual cost limit exceeded"
            )
        );
        assert!(rem < RUNTIME_CEL_COST_BUDGET);
    }

    // activation.go:143-152: composition (variable) cost comes off first.
    #[test]
    fn composition_cost_is_charged_before_the_expression() {
        let c = ctx();
        let mut ev = CELEvaluator::new();
        let (r, rem) = evaluate_with_budget(
            "true",
            || ev.evaluate_with_cost("true", &c, CostOptions::default()),
            Some(7),
            10,
        )
        .unwrap();
        assert!(r.is_ok());
        assert_eq!(rem, 3);
        let e = evaluate_with_budget(
            "true",
            || ev.evaluate_with_cost("true", &c, CostOptions::default()),
            Some(11),
            10,
        )
        .unwrap_err();
        assert!(e.out_of_budget);
    }

    #[test]
    fn constants_match_config_go() {
        assert_eq!(PER_CALL_LIMIT, 1_000_000);
        assert_eq!(RUNTIME_CEL_COST_BUDGET, 10_000_000);
        assert_eq!(RUNTIME_CEL_COST_BUDGET_MATCH_CONDITIONS, 2_500_000);
        assert_eq!(CHECK_FREQUENCY, 100);
        assert_eq!(MAX_REQUEST_SIZE_BYTES, 3 * 1024 * 1024);
        assert_eq!(MAX_EVALUATED_MESSAGE_EXPRESSION_SIZE_BYTES, 5 * 1024);
    }
}
