//! Unit tests moved from `src/cel_cost.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use cel::common::ast::{
        operators, CallExpr, ComprehensionExpr, EntryExpr, Expr, IdedEntryExpr, IdedExpr, ListExpr,
        LiteralValue, MapEntryExpr, MapExpr, SelectExpr,
    };
    pub use cel::objects::Key;
    pub use cel::{Context, ExecutionError, Value};
    pub use std::fmt;
    pub use std::panic::{catch_unwind, AssertUnwindSafe};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::cel::{CELContext, CELEvaluator};
    use rusternetes_common::cel_cost::*;
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

    // `evalConditional` has no direct cost; an index is a qualifier (constant:
    // `cost++`, attribute key: the key's attribute observation).
    #[test]
    fn conditional_and_index_costs() {
        assert_eq!(cost("x == 1 ? y : 5"), 3);
        assert_eq!(cost("object.xs[0]"), 3);
        assert_eq!(cost("object.xs[x]"), 3);
    }

    // An erroring operand is observed, the call over it is still charged.
    #[test]
    fn error_paths_are_charged() {
        let (r, c) = CELEvaluator::new().evaluate_with_cost(
            "object.missing.size()",
            &ctx(),
            CostOptions::default(),
        );
        assert!(r.is_err());
        assert_eq!(c, 3);
    }

    // `Object{}` is a struct for cel-go: `StructCreateBaseCost`, and the
    // rewrite's guard call is not a CEL call.
    #[test]
    fn struct_initializer_costs_struct_create() {
        assert_eq!(cost("size(Object{a: x})"), 40 + 1 + 1);
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
