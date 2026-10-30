//! Unit tests moved from `src/handlers/cel_validation.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::cel_cost::{
        CostEvalError, CostOptions, MAX_EVALUATED_MESSAGE_EXPRESSION_SIZE_BYTES,
        RUNTIME_CEL_COST_BUDGET,
    };
    pub use rusternetes_common::cel_versions::{compile_failure, EnvSet, EnvType};
    pub use rusternetes_common::resources::{
        CustomResource, CustomResourceDefinition, CustomResourceDefinitionSpec, JSONSchemaProps,
        JSONSchemaPropsOrArray, JSONSchemaPropsOrBool,
    };
    pub use rusternetes_common::schema_validation::PathSeg;
    pub use rusternetes_common::validation::field::{Error as FieldError, Path};
    pub use rusternetes_common::{CELContext, CELEvaluator, Error, Result};
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::cel_validation::*;
    use rusternetes_common::feature_gates::Version;
    use serde_json::json;

    fn schema_with_rule(rule: &str, message: &str) -> JSONSchemaProps {
        let raw = json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {
                        "replicas": {"type": "integer"}
                    },
                    "x-kubernetes-validations": [
                        {"rule": rule, "message": message}
                    ]
                }
            }
        });
        serde_json::from_value(raw).expect("schema deserialises")
    }

    fn optional_schema(rule: &str, optional: Option<bool>) -> JSONSchemaProps {
        let mut v = json!({"rule": rule, "message": "failed rule"});
        if let Some(b) = optional {
            v["optionalOldSelf"] = json!(b);
        }
        serde_json::from_value(json!({
            "type": "object",
            "properties": {"foo": {"type": "string"}},
            "x-kubernetes-validations": [v]
        }))
        .expect("schema deserialises")
    }

    fn fails(
        schema: &JSONSchemaProps,
        new: serde_json::Value,
        old: Option<serde_json::Value>,
    ) -> bool {
        let mut budget = RUNTIME_CEL_COST_BUDGET;
        !evaluate_cr_rules_with_paths(schema, &new, old.as_ref(), &mut budget)
            .unwrap()
            .is_empty()
    }

    /// apiextensions-apiserver `pkg/apiserver/schema/cel/validation_test.go`
    /// `TestOptionalOldSelf`: with `optionalOldSelf: true` the rule runs on
    /// create with `oldSelf` an empty optional.
    #[test]
    fn optional_old_self_evaluates_on_create_with_none() {
        let s = optional_schema("self.foo == 'not bar' || !oldSelf.hasValue()", Some(true));
        assert!(!fails(&s, json!({"foo": "bar"}), None));
        // rule requires an old value: on create the empty optional fails it.
        let s = optional_schema("oldSelf.hasValue() || self.foo == 'ok'", Some(true));
        assert!(fails(&s, json!({"foo": "bad"}), None));
        assert!(!fails(&s, json!({"foo": "ok"}), None));
    }

    #[test]
    fn optional_old_self_binds_old_value_on_update() {
        let s = optional_schema(
            "self.foo == 'valid' || (oldSelf.hasValue() && oldSelf.value().foo != 'valid')",
            Some(true),
        );
        // block: old was valid
        assert!(fails(
            &s,
            json!({"foo": "invalid"}),
            Some(json!({"foo": "valid"}))
        ));
        // ratchet: old also invalid
        assert!(!fails(
            &s,
            json!({"foo": "invalid again"}),
            Some(json!({"foo": "invalid"}))
        ));
        // `oldSelf.?foo.orValue(...)` optional field select
        let s = optional_schema(
            "self.foo != 'bar' || oldSelf.?foo.orValue('baz') == 'baz'",
            Some(true),
        );
        assert!(!fails(
            &s,
            json!({"foo": "bar"}),
            Some(json!({"foo": "baz"}))
        ));
        assert!(fails(
            &s,
            json!({"foo": "bar"}),
            Some(json!({"foo": "other"}))
        ));
    }

    /// Without the flag (or `false`) a transition rule is still skipped on
    /// create (validation.go: `compiled.UsesOldSelf && oldObj == nil`).
    #[test]
    fn transition_rule_without_optional_old_self_skipped_on_create() {
        for flag in [None, Some(false)] {
            let s = optional_schema("oldSelf.foo == self.foo", flag);
            assert!(!fails(&s, json!({"foo": "x"}), None));
        }
    }

    /// The flat (`collect_rules` / `validate_cr_rules`) path honours it too.
    #[test]
    fn collect_rules_carries_optional_old_self() {
        let s = optional_schema("!oldSelf.hasValue() || self.foo == 'x'", Some(true));
        assert!(collect_rules(&s)[0].optional_old_self);
        let s = optional_schema("oldSelf.foo == self.foo", None);
        assert!(!collect_rules(&s)[0].optional_old_self);
    }

    fn cr_with_nums(n: usize) -> CustomResource {
        let nums: Vec<u32> = (0..n as u32).collect();
        serde_json::from_value(json!({
            "apiVersion": "example.com/v1",
            "kind": "Foo",
            "metadata": {"name": "a"},
            "spec": {"nums": nums}
        }))
        .expect("cr deserialises")
    }

    fn rules_at_spec(rule: &str, count: usize) -> Vec<ValidationRule> {
        (0..count)
            .map(|_| ValidationRule {
                rule: rule.to_string(),
                message: "failed rule".to_string(),
                message_expression: String::new(),
                path: vec!["spec".to_string()],
                optional_old_self: false,
            })
            .collect()
    }

    fn err_text(e: Error) -> String {
        e.to_string()
    }

    /// `validation_test.go` `TestCELValidationLimit`: once the summed actual
    /// cost of the rules passes `celconfig.RuntimeCELCostBudget` (10M) the
    /// request fails with "validation failed due to running out of cost
    /// budget, no further validation rules will be run".
    #[test]
    fn rules_whose_summed_cost_passes_the_runtime_budget_are_rejected() {
        let cr = cr_with_nums(5000);
        let few = rules_at_spec("self.nums.all(x, x >= 0)", 5);
        validate_cr_rules(&few, &cr, None).expect("a few cheap rules fit the budget");
        let many = rules_at_spec("self.nums.all(x, x >= 0)", 3000);
        let e = validate_cr_rules(&many, &cr, None).expect_err("budget must run out");
        assert!(
            err_text(e).contains(
                "validation failed due to running out of cost budget, no further validation rules will be run"
            )
        );
    }

    /// `validation.go:431`-ish `celconfig.PerCallLimit` (1M): one rule over the
    /// per-call limit stops evaluation with "no further validation rules will
    /// be run due to call cost exceeds limit for rule: <message>".
    #[test]
    fn a_rule_over_the_per_call_limit_is_rejected() {
        let cr = cr_with_nums(2000);
        let rules = rules_at_spec("self.nums.all(x, self.nums.all(y, x >= 0 && y >= 0))", 1);
        let e = validate_cr_rules(&rules, &cr, None).expect_err("per-call limit");
        assert!(err_text(e).contains(
            "no further validation rules will be run due to call cost exceeds limit for rule: failed rule"
        ));
    }

    /// `validation.go:380-383` through the ratcheting walk: the budget is one
    /// per custom resource, passed in by the caller and spent across calls; an
    /// exhausted budget is an error (never a ratcheted failure).
    #[test]
    fn the_walk_spends_one_budget_across_calls_and_stops_when_it_is_gone() {
        let schema = schema_with_rule("self.nums.all(x, x >= 0)", "m");
        let spec = json!({"nums": (0..5000).collect::<Vec<u32>>()});
        let mut remaining = RUNTIME_CEL_COST_BUDGET;
        let mut calls = 0;
        let err = loop {
            match evaluate_cr_rules_with_paths(
                schema.properties.as_ref().unwrap().get("spec").unwrap(),
                &spec,
                None,
                &mut remaining,
            ) {
                Ok(f) => assert!(f.is_empty()),
                Err(e) => break e,
            }
            calls += 1;
            assert!(calls < 10_000, "the budget never ran out");
        };
        assert!(err.to_string().contains(OUT_OF_COST_BUDGET));
        assert!(calls > 1, "one evaluation must fit in the budget");
        assert!(remaining < 0);
    }

    /// `validation_test.go` `TestMessageExpression` (:3169-3340): the rule is
    /// `false`, `self` is `{"str": "a string"}`; the field error detail, and the
    /// remaining budget (0 = not asserted, as upstream).
    #[test]
    fn message_expression_cases_of_upstream_test_message_expression() {
        let long = format!("\"{}\"", "a".repeat(5121));
        let max = RUNTIME_CEL_COST_BUDGET;
        // (name, message, messageExpression, budget, per-call limit, detail, budget left)
        let cases: Vec<(&str, &str, String, i64, u64, &str, i64)> = vec![
            (
                "no cost",
                "",
                "\"static string\"".into(),
                300,
                1_000_000,
                "static string",
                300,
            ),
            (
                "takes precedence",
                "invisible",
                "\"this is messageExpression\"".into(),
                max,
                1_000_000,
                "this is messageExpression",
                0,
            ),
            (
                "non-string falls back",
                "",
                "true".into(),
                max,
                1_000_000,
                "failed rule",
                max,
            ),
            (
                "limit exceeded",
                "",
                "\"string 1\" + \"string 2\" + \"string 3\"".into(),
                1,
                1_000_000,
                "messageExpression evaluation failed due to running out of cost budget",
                -1,
            ),
            (
                "str concat budget",
                "",
                "\"str1 \" + self.str".into(),
                50,
                1_000_000,
                "str1 a string",
                46,
            ),
            (
                "cost preserved on error",
                "message not messageExpression",
                "\"str1 \" + [\"a\", \"b\", \"c\", \"d\"][4]".into(),
                50,
                1_000_000,
                "message not messageExpression",
                47,
            ),
            (
                "cost preserved on error (no message)",
                "",
                "\"str1 \" + [\"a\", \"b\", \"c\", \"d\"][4]".into(),
                50,
                1_000_000,
                "failed rule",
                47,
            ),
            (
                "per-call limit",
                "",
                "\"string 1\" + \"string 2\" + \"string 3\"".into(),
                max,
                1,
                "call cost exceeds limit for messageExpression",
                -1,
            ),
            (
                "newline",
                "message not messageExpression",
                "\"str with \\na newline\"".into(),
                max,
                1_000_000,
                "message not messageExpression",
                0,
            ),
            (
                ">5120 bytes",
                "message not messageExpression",
                long,
                max,
                1_000_000,
                "message not messageExpression",
                0,
            ),
            (
                "empty",
                "message not messageExpression",
                "string(\"\")".into(),
                max,
                1_000_000,
                "message not messageExpression",
                0,
            ),
            (
                "only spaces",
                "message not messageExpression",
                "string(\"     \")".into(),
                max,
                1_000_000,
                "message not messageExpression",
                0,
            ),
        ];
        for (name, message, me, budget, limit, want, want_budget) in cases {
            let mut ctx = CELContext::new();
            ctx.add_json_variable("self", &json!({"str": "a string"}))
                .unwrap();
            let mut evaluator = CELEvaluator::new();
            let mut remaining = budget;
            let opts = CostOptions {
                limit: Some(limit),
                ..CostOptions::default()
            };
            let detail =
                match eval_message_expression(&mut evaluator, &me, &ctx, &mut remaining, opts) {
                    Ok(d) => d,
                    Err(MessageExpressionError::Stop(e)) => e,
                    Err(MessageExpressionError::Fallback(_)) => {
                        if message.is_empty() {
                            "failed rule".to_string()
                        } else {
                            message.to_string()
                        }
                    }
                };
            assert!(
                detail.contains(want),
                "{name}: got {detail:?}, want {want:?}"
            );
            if want_budget != 0 {
                assert_eq!(remaining, want_budget, "{name}: remaining budget");
            }
        }
    }

    /// End to end: the evaluated `messageExpression` is the error returned for
    /// a failing rule, taking precedence over `message`; a failing
    /// `messageExpression` falls back to `message`.
    #[test]
    fn failing_rule_reports_evaluated_message_expression() {
        let cr = cr_with_nums(3);
        let mk = |me: &str| {
            vec![ValidationRule {
                rule: "self.nums.size() > 10".to_string(),
                message: "static message".to_string(),
                message_expression: me.to_string(),
                path: vec!["spec".to_string()],
                optional_old_self: false,
            }]
        };
        let e = validate_cr_rules(&mk("'got ' + string(self.nums.size())"), &cr, None)
            .expect_err("rule fails");
        assert_eq!(err_text(e), "Invalid resource: got 3");
        let e = validate_cr_rules(&mk("self.nums[9] == 1 ? 'a' : 'b'"), &cr, None)
            .expect_err("rule fails");
        assert!(err_text(e).contains("static message"));
    }

    /// `preexistingExpressions.RuleEnv` (validation.go:158-163): a new rule is
    /// compiled in `NewExpressions` at `DefaultCompatibilityVersion()`, so a
    /// library introduced after the min-compatibility version is undeclared.
    #[test]
    fn new_rules_are_compiled_at_the_compatibility_version() {
        let schema = schema_with_rule("semver('1.2.3').major() > 0", "m");
        let at_1_32 = EnvSet::must_base_env_set(Version::major_minor(1, 32));
        let root = Path::new("openAPIV3Schema");
        let pre = PreexistingExpressions::default();
        let errs = validate_crd_schema_rules(&schema, &root, &at_1_32, &pre, false);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0]
                .detail
                .contains("undeclared reference to 'semver' (in container '')"),
            "{errs:?}"
        );
        assert_eq!(
            errs[0].field,
            "openAPIV3Schema.properties[spec].x-kubernetes-validations[0].rule"
        );
        let at_1_33 = EnvSet::must_base_env_set(Version::major_minor(1, 33));
        assert!(validate_crd_schema_rules(&schema, &root, &at_1_33, &pre, false).is_empty());
    }

    /// A rule already stored on the CRD is compiled in `StoredExpressions`.
    #[test]
    fn preexisting_rules_are_compiled_in_the_stored_environment() {
        let rule = "semver('1.2.3').major() > 0";
        let schema = schema_with_rule(rule, "m");
        let at_1_32 = EnvSet::must_base_env_set(Version::major_minor(1, 32));
        let pre = PreexistingExpressions {
            rules: [rule.to_string()].into(),
            ..Default::default()
        };
        let root = Path::new("openAPIV3Schema");
        assert!(validate_crd_schema_rules(&schema, &root, &at_1_32, &pre, false).is_empty());
    }

    #[test]
    fn collect_rules_finds_nested_validation() {
        let schema = schema_with_rule("self.replicas <= 100", "too many replicas");
        let rules = collect_rules(&schema);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].path, vec!["spec"]);
        assert_eq!(rules[0].message, "too many replicas");
    }

    #[test]
    fn first_self_reference_extracts_property() {
        assert_eq!(
            first_self_reference("self.foo > 0"),
            Some("foo".to_string())
        );
        assert_eq!(first_self_reference("self > 0"), None);
        assert_eq!(first_self_reference("myself.foo > 0"), None);
    }

    #[test]
    fn detect_unknown_property_flags_misspelling() {
        let schema = schema_with_rule("self.replicaz <= 100", "msg");
        let spec = &schema.properties.as_ref().unwrap()["spec"];
        let unknown = unknown_property_in_rule(spec, "self.replicaz <= 100");
        assert_eq!(unknown.as_deref(), Some("replicaz"));
        assert_eq!(unknown_property_in_rule(spec, "self.replicas <= 100"), None);
    }

    #[test]
    fn rule_references_old_self_finds_identifier() {
        assert!(rule_references_old_self("self.x >= oldSelf.x"));
        assert!(rule_references_old_self("oldSelf == self"));
        assert!(!rule_references_old_self("self.x > 0"));
        // Substring matches don't count.
        assert!(!rule_references_old_self("myoldSelfFoo > 0"));
    }

    #[test]
    fn estimate_cost_is_large_for_nested_comprehensions() {
        // Two nested all(...) calls => 1024*1024 multiplier
        let rule = "self.list.all(x, x.all(y, y > 0))";
        assert!(estimate_cost(rule) > 10_000_000);
    }
}
