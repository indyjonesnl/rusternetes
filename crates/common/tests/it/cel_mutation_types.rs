//! The shadow type check of MutatingAdmissionPolicy expressions
//! (`cel_mutation_types`): the environment of `plugin/cel/compile.go`
//! `createEnvForOpts` with the `Object` / `JSONPatch` types
//! (`cel/mutation/typeresolver.go`) and the output types of
//! `patch/smd.go:52` / `patch/json_patch.go:57`.

use rusternetes_common::cel_env::ExpectedOutput;
use rusternetes_common::cel_mutation_types::{
    check_mutation_expression, mutation_divergence, PolicyExpression,
};
use rusternetes_common::cel_versions::{default_compatibility_version, EnvSet, EnvType};

fn set() -> EnvSet {
    EnvSet::must_base_env_set(default_compatibility_version())
}

fn expr<'a>(src: &'a str, expected: ExpectedOutput) -> PolicyExpression<'a> {
    PolicyExpression {
        expression: src,
        has_params: false,
        has_authorizer: true,
        has_patch_types: true,
        expected,
        variables: &[],
        no_variables: false,
    }
}

fn errors_in(e: &PolicyExpression<'_>) -> Vec<String> {
    check_mutation_expression(&set(), EnvType::StoredExpressions, e).expect("parses")
}

fn errors(src: &str) -> Vec<String> {
    errors_in(&expr(src, ExpectedOutput::Any))
}

/// `TestTypeResolver` (`cel/mutation/typeresolver_test.go:34`): the cases that
/// compile.
#[test]
fn type_resolver_expressions_compile() {
    for src in [
        "string(114514)",
        "Object{}",
        "Object{spec: Object.spec{replicas: 3}}",
        r#"Object{
            spec: Object.spec{
                template: Object.spec.template{
                    containers: [
                        Object.spec.template.containers.item{
                            name: "nginx", image: "nginx", args: ["-g"]
                        }
                    ]
                }
            }
        }"#,
        "Object{intList: [1, 2, 3]}",
        r#"Object{annotations: {"foo": "bar"}}"#,
        "Object{intList: [1, 2, 3]}.intList.sum()",
        "Object{spec: Object.spec{replicas: 3}} == Object{spec: Object.spec{replicas: 1 + 2}}",
        r#"true ? JSONPatch{op: "add", path: "/spec/replicas", value: 3} : JSONPatch{op: "remove", path: "/spec/replicas"}"#,
        r#"JSONPatch{op: "add", path: "/spec/replicas", value: 3}"#,
        r#"JSONPatch{op: "remove", path: "/spec/replicas"}"#,
        r#"JSONPatch{op: "replace", path: "/spec/replicas", value: 3}"#,
        r#"JSONPatch{op: "move", from: "/spec/replicas", path: "/spec/replicas"}"#,
        r#"JSONPatch{op: "copy", from: "/spec/replicas", path: "/spec/replicas"}"#,
        r#"JSONPatch{op: "test", path: "/spec/replicas", value: 3}"#,
        // the values are not checked in compilation
        r#"JSONPatch{op: "invalid", path: "/spec/replicas", value: 3}"#,
        r#"JSONPatch{op: "add", value: 3}"#,
        r#"JSONPatch{op: "add", path: "/spec/replicas"}"#,
    ] {
        assert_eq!(errors(src), Vec::<String>::new(), "{src}");
    }
}

/// The cases of `TestTypeResolver` that do not compile, with the message of
/// `expectCompileError`.
#[test]
fn type_resolver_expressions_that_do_not_compile() {
    for (src, message) in [
        ("Invalid{}", "undeclared reference to 'Invalid'"),
        (
            r#"JSONPatch{op: "add", path: "/spec/replicas", value: Invalid{}}"#,
            "undeclared reference to 'Invalid'",
        ),
        (
            r#"JSONPatch{op: "add", path: Invalid{}, value: 3}"#,
            "undeclared reference to 'Invalid'",
        ),
        (
            r#"JSONPatch{op: "move", from: Invalid{}, path: "/spec/replicas"}"#,
            "undeclared reference to 'Invalid'",
        ),
        (
            r#"JSONPatch{op: 1, path: "/spec/replicas", value: 3}"#,
            "expected type of field 'op' is 'string' but provided type is 'int'",
        ),
        (
            r#"JSONPatch{op: "add", path: 1, value: 3}"#,
            "expected type of field 'path' is 'string' but provided type is 'int'",
        ),
        (
            r#"JSONPatch{op: "move", from: 1, path: "/spec/replicas"}"#,
            "expected type of field 'from' is 'string' but provided type is 'int'",
        ),
    ] {
        let errs = errors(src);
        assert!(
            errs.iter().any(|e| e.contains(message)),
            "{src}: {errs:?} does not contain {message:?}"
        );
    }
}

/// `TestCELOptional` (`typeresolver_test.go`): the optional syntax on the
/// patch types.
#[test]
fn optional_initializers_compile() {
    for src in [
        "Object{?existing: optional.none()}",
        "Object{?spec: optional.ofNonZeroValue(Object.spec{?replicas: Object{}.?replicas})}",
        "Object{}.?existing",
        r#"{"foo": 1}[?"bar"]"#,
        r#"has({"foo": 1}.bar)"#,
    ] {
        assert_eq!(errors(src), Vec::<String>::new(), "{src}");
    }
}

/// Without `HasPatchTypes` (variables, match conditions) the patch types do
/// not resolve, and `jsonpatch.escapeKey` is only declared with them
/// (`createEnvForOpts`, `compile.go:283-287`).
#[test]
fn patch_types_need_has_patch_types() {
    let mut plain = expr("Object{}", ExpectedOutput::Any);
    plain.has_patch_types = false;
    assert!(errors_in(&plain)
        .iter()
        .any(|e| e.contains("undeclared reference to 'Object'")));
    let mut escape = expr("jsonpatch.escapeKey('a/b')", ExpectedOutput::Any);
    assert_eq!(errors_in(&escape), Vec::<String>::new());
    escape.has_patch_types = false;
    assert!(!errors_in(&escape).is_empty());
}

/// `ApplyConfigurationCondition.ReturnTypes` is `Object`,
/// `JSONPatchCondition.ReturnTypes` is `list(JSONPatch)`
/// (`compile.go:186-203`: `must evaluate to %v but got %v`).
#[test]
fn the_output_type_is_checked() {
    let apply = |src| errors_in(&expr(src, ExpectedOutput::Object));
    let patch = |src| errors_in(&expr(src, ExpectedOutput::JsonPatchList));
    assert_eq!(
        apply("Object{spec: Object.spec{replicas: 3}}"),
        [] as [String; 0]
    );
    assert!(apply("1")[0].starts_with("must evaluate to Object but got int"));
    assert!(apply("Object.spec{}")[0].starts_with("must evaluate to Object"));
    assert_eq!(
        patch(r#"[JSONPatch{op: "add", path: "/a", value: 1}]"#),
        [] as [String; 0]
    );
    assert_eq!(
        patch(
            r#"object.spec.items.map(i, JSONPatch{op: "remove", path: "/spec/items/" + string(i)})"#
        ),
        [] as [String; 0]
    );
    assert!(patch(r#"JSONPatch{op: "add"}"#)[0].starts_with("must evaluate to list(JSONPatch)"));
    assert!(patch("[1]")[0].starts_with("must evaluate to list(JSONPatch)"));
    // `object` is `dyn`, which is not an exact `bool`.
    let cond = |src| errors_in(&expr(src, ExpectedOutput::Bool));
    assert_eq!(cond("object.spec.replicas > 1"), [] as [String; 0]);
    assert!(cond("object.spec.replicas")[0].starts_with("must evaluate to bool but got dyn"));
}

/// The declarations of `createEnvForOpts`: `params` only with a `paramKind`,
/// `authorizer` / `authorizer.requestResource` only with `HasAuthorizer`,
/// the composited `variables` only where the compiler has them.
#[test]
fn declarations_follow_the_options() {
    let mut e = expr("params.x == 1", ExpectedOutput::Any);
    assert!(!errors_in(&e).is_empty());
    e.has_params = true;
    assert_eq!(errors_in(&e), Vec::<String>::new());

    let mut e = expr(
        "authorizer.requestResource.check('get').allowed()",
        ExpectedOutput::Any,
    );
    assert_eq!(errors_in(&e), Vec::<String>::new());
    e.has_authorizer = false;
    assert!(!errors_in(&e).is_empty());

    let variables = vec![(
        "replicas".to_string(),
        "object.spec.replicas + 1".to_string(),
    )];
    let mut e = expr("variables.replicas > 2", ExpectedOutput::Bool);
    e.variables = &variables;
    assert_eq!(errors_in(&e), Vec::<String>::new());
    let mut missing = expr("variables.nope > 2", ExpectedOutput::Bool);
    missing.variables = &variables;
    assert!(!errors_in(&missing).is_empty());
    // The stateless match condition compiler has no `variables`.
    e.no_variables = true;
    assert!(!errors_in(&e).is_empty());
    // `request` and `namespaceObject` are typed.
    assert_eq!(
        errors("request.userInfo.username == 'a' && namespaceObject.metadata.name == 'b'"),
        Vec::<String>::new()
    );
    assert!(!errors("request.nope == 1").is_empty());
}

/// A syntax error is not the comparison's concern; the verdict is compared
/// with the probe's.
#[test]
fn divergence_compares_with_the_probe() {
    let e = expr("Object{}", ExpectedOutput::Object);
    assert_eq!(
        mutation_divergence(&set(), EnvType::NewExpressions, &e, &None),
        None
    );
    assert!(mutation_divergence(&set(), EnvType::NewExpressions, &e, &Some("x".into())).is_some());
    let syntax = expr("Object{", ExpectedOutput::Object);
    assert_eq!(
        check_mutation_expression(&set(), EnvType::NewExpressions, &syntax),
        None
    );
    assert_eq!(
        mutation_divergence(&set(), EnvType::NewExpressions, &syntax, &Some("x".into())),
        None
    );
}
