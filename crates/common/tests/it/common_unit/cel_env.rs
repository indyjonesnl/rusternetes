//! Unit tests moved from `src/cel_env.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use cel::common::ast::{EntryExpr, Expr, IdedExpr, LiteralValue};
    pub use std::collections::BTreeSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::cel_env::*;

    fn env() -> MutationEnv {
        MutationEnv {
            has_params: false,
            has_authorizer: true,
            has_patch_types: true,
            variables: Some(["x".to_string()].into()),
        }
    }

    fn fail(expr: &str, env: &MutationEnv, out: ExpectedOutput) -> String {
        mutation_env_failure(expr, env, out).unwrap_or_default()
    }

    /// validation_test.go:4286.
    #[test]
    fn a_comparison_is_not_an_object() {
        let m = fail("1 < 2", &env(), ExpectedOutput::Object);
        assert_eq!(m, "must evaluate to Object but got bool");
    }

    /// validation_test.go:4442.
    #[test]
    fn variables_fields_are_the_declared_variables() {
        let m = fail("variables.x + variables.y", &env(), ExpectedOutput::Any);
        assert!(m.contains("undefined field 'y'"), "{m}");
        assert_eq!(fail("variables.x", &env(), ExpectedOutput::Any), "");
        assert_eq!(fail("has(variables.x)", &env(), ExpectedOutput::Any), "");
    }

    /// validation_test.go:5190.
    #[test]
    fn params_needs_a_param_kind() {
        let m = fail("params.foo == 'a'", &env(), ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'params'"), "{m}");
        let with = MutationEnv {
            has_params: true,
            ..env()
        };
        assert_eq!(fail("params.foo == 'a'", &with, ExpectedOutput::Any), "");
    }

    /// plugin.go:119: a `messageExpression` has no `authorizer`.
    #[test]
    fn authorizer_needs_has_authorizer() {
        let src = "authorizer.group('').resource('pods').check('get').allowed()";
        assert_eq!(fail(src, &env(), ExpectedOutput::Bool), "");
        let without = MutationEnv {
            has_authorizer: false,
            ..env()
        };
        let m = fail(src, &without, ExpectedOutput::Bool);
        assert!(m.contains("undeclared reference to 'authorizer'"), "{m}");
    }

    /// message.go:34-36 / validation_test.go:2644.
    #[test]
    fn a_message_expression_must_be_a_string() {
        let m = fail("0 == 0", &env(), ExpectedOutput::String);
        assert_eq!(m, "must evaluate to string but got bool");
        assert_eq!(fail("'a'", &env(), ExpectedOutput::String), "");
    }

    #[test]
    fn variables_is_undeclared_in_the_stateless_environment() {
        let stateless = MutationEnv {
            has_authorizer: true,
            ..MutationEnv::default()
        };
        let m = fail("variables.x", &stateless, ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'variables'"), "{m}");
    }

    #[test]
    fn patch_types_resolve_only_with_has_patch_types() {
        let no_patch = MutationEnv {
            has_patch_types: false,
            ..env()
        };
        let m = fail("Object{}", &no_patch, ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'Object'"), "{m}");
        for ok in ["Object{}", "Object.spec{ replicas: 1 }", "Object.spec.a{}"] {
            assert_eq!(fail(ok, &env(), ExpectedOutput::Object), "", "{ok}");
        }
        let m = fail("Other{}", &env(), ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'Other'"), "{m}");
    }

    #[test]
    fn json_patch_has_fixed_fields() {
        let ok = "[JSONPatch{op: 'add', from: 'a', path: '/a', value: 1}]";
        assert_eq!(fail(ok, &env(), ExpectedOutput::JsonPatchList), "");
        let m = fail("[JSONPatch{bogus: 1}]", &env(), ExpectedOutput::Any);
        assert!(m.contains("undefined field 'bogus'"), "{m}");
    }

    #[test]
    fn comprehension_variables_are_scoped() {
        let e = env();
        assert_eq!(fail("[1].map(i, i + 1)", &e, ExpectedOutput::Any), "");
        assert_eq!(fail("[1].all(i, i > 0)", &e, ExpectedOutput::Any), "");
        assert_eq!(
            fail("object.a.filter(i, i > 0)", &e, ExpectedOutput::Any),
            ""
        );
        let m = fail("[1].map(i, i) + [i]", &e, ExpectedOutput::Any);
        assert!(m.contains("undeclared reference to 'i'"), "{m}");
    }

    #[test]
    fn output_check_skips_what_needs_inference() {
        let e = env();
        for expr in ["object.spec", "variables.x", "foo(1)", "a ? b : c"] {
            let src = expr
                .replace("foo(1)", "size(object.a)")
                .replace("a ? b : c", "true ? object : oldObject");
            assert_eq!(fail(&src, &e, ExpectedOutput::Object), "", "{expr}");
        }
        let m = fail("{'a': 1}", &e, ExpectedOutput::JsonPatchList);
        assert!(
            m.starts_with("must evaluate to list(JSONPatch) but got"),
            "{m}"
        );
    }

    /// `MatchCondition.ReturnTypes` (matcher.go:49-51) + compile.go:189-203.
    #[test]
    fn bool_output_is_enforced_only_where_the_type_is_known() {
        let e = env();
        assert_eq!(
            fail("6", &e, ExpectedOutput::Bool),
            "must evaluate to bool but got int"
        );
        assert_eq!(
            fail("'a'", &e, ExpectedOutput::Bool),
            "must evaluate to bool but got string"
        );
        for ok in [
            "true",
            "1 < 2",
            "object.spec",
            "size(object.a) > 0",
            "!false",
        ] {
            assert_eq!(fail(ok, &e, ExpectedOutput::Bool), "", "{ok}");
        }
        assert_eq!(
            output_type_failure("6", ExpectedOutput::Bool).as_deref(),
            Some("must evaluate to bool but got int")
        );
        assert_eq!(output_type_failure("1 <", ExpectedOutput::Bool), None);
    }

    #[test]
    fn syntax_errors_come_first() {
        assert!(fail("///", &env(), ExpectedOutput::Object).starts_with("compilation failed"));
    }
}
