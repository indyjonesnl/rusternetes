//! The per-expression output type checks, shadow only.
//!
//! cel-go `plugin/cel/compile.go:186-203` (`CompileCELExpression`): after the
//! check, `ast.OutputType().IsExactType(returnType) || cel.AnyType.IsExactType(
//! returnType)` for one of the accessor's `ReturnTypes()`. The comparison is
//! exact, so a `dyn` output matches neither `bool` nor `string`
//! (`compile_test.go`, `TestCompileValidatingPolicyExpression`).
//! CRD rules: `apiextensions-apiserver/pkg/apiserver/schema/cel/compilation.go`
//! `compileRule` `:217-219` and `:260-262`.

use rusternetes_cel_checker::DType;
use rusternetes_common::cel_checker_shadow::{check_rule_output, SelfScope};
use rusternetes_common::cel_env::ExpectedOutput;
use rusternetes_common::cel_mutation_types::{check_mutation_expression, PolicyExpression};
use rusternetes_common::cel_policy_types::DeclType;
use rusternetes_common::cel_versions::{default_compatibility_version, EnvSet, EnvType};

fn set() -> EnvSet {
    EnvSet::must_base_env_set(default_compatibility_version())
}

fn policy(src: &str, expected: ExpectedOutput) -> Vec<String> {
    let e = PolicyExpression {
        expression: src,
        has_params: false,
        has_authorizer: true,
        has_patch_types: false,
        expected,
        variables: &[],
        no_variables: false,
    };
    check_mutation_expression(&set(), EnvType::NewExpressions, &e).expect("parses")
}

fn crd(src: &str, self_type: DType, expected: ExpectedOutput) -> Vec<String> {
    let scope = SelfScope {
        decl: DeclType::simple(self_type),
        optional_old_self: false,
    };
    check_rule_output(src, &set(), EnvType::NewExpressions, Some(&scope), expected).expect("parses")
}

#[test]
fn validation_must_be_bool() {
    assert!(policy("object.spec.replicas > 1", ExpectedOutput::Bool).is_empty());
    assert_eq!(
        policy("1 + 1", ExpectedOutput::Bool),
        vec!["must evaluate to bool but got int"]
    );
    assert_eq!(
        policy("'a'", ExpectedOutput::Bool),
        vec!["must evaluate to bool but got string"]
    );
}

/// `dyn` is not `bool`: `object` is `dyn`, so `object.spec` is `dyn`.
#[test]
fn dyn_output_does_not_match_bool_or_string() {
    assert_eq!(
        policy("object.spec", ExpectedOutput::Bool),
        vec!["must evaluate to bool but got dyn"]
    );
    assert_eq!(
        policy("object.spec", ExpectedOutput::String),
        vec!["must evaluate to string but got dyn"]
    );
}

#[test]
fn message_expression_must_be_string() {
    assert!(policy("'replicas: ' + string(1)", ExpectedOutput::String).is_empty());
    assert_eq!(
        policy("1", ExpectedOutput::String),
        vec!["must evaluate to string but got int"]
    );
}

/// `AuditAnnotationCondition.ReturnTypes()` (`validating/interface.go:60`).
#[test]
fn audit_annotation_is_string_or_null() {
    assert!(policy("'x'", ExpectedOutput::StringOrNull).is_empty());
    assert!(policy("null", ExpectedOutput::StringOrNull).is_empty());
    assert_eq!(
        policy("true", ExpectedOutput::StringOrNull),
        vec!["must evaluate to [string, null_type] but got bool"]
    );
}

/// `Variable.ReturnTypes()` is `[any, dyn]` (`validating/interface.go:74`).
#[test]
fn variable_accepts_any_type() {
    for src in ["1", "'a'", "object.spec", "[1, 2]"] {
        assert!(policy(src, ExpectedOutput::Any).is_empty(), "{src}");
    }
}

/// A type error is reported as such, not as an output type mismatch.
#[test]
fn a_failed_check_has_no_output_error() {
    let errs = policy("1 + 'a'", ExpectedOutput::Bool);
    assert!(!errs.is_empty());
    assert!(
        errs.iter().all(|e| !e.contains("must evaluate to")),
        "{errs:?}"
    );
}

#[test]
fn crd_rule_must_be_bool() {
    assert!(crd("self > 1", DType::Int, ExpectedOutput::Bool).is_empty());
    assert_eq!(
        crd("self + 1", DType::Int, ExpectedOutput::Bool),
        vec!["cel expression must evaluate to a bool"]
    );
    assert_eq!(
        crd("self", DType::Dyn, ExpectedOutput::Bool),
        vec!["cel expression must evaluate to a bool"]
    );
}

#[test]
fn crd_message_expression_must_be_string() {
    assert!(crd("'v=' + string(self)", DType::Int, ExpectedOutput::String).is_empty());
    assert_eq!(
        crd("self", DType::Int, ExpectedOutput::String),
        vec!["messageExpression must evaluate to a string"]
    );
}
