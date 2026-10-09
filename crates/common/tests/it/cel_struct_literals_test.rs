//! MutatingAdmissionPolicy CEL initializers: `Object{}` / `Object.spec{}` /
//! `JSONPatch{}` (#2885).
//!
//! Ported from upstream `TestTypeResolver`
//! (`staging/src/k8s.io/apiserver/pkg/cel/mutation/typeresolver_test.go:34`).
//! The `cel` 0.13 crate parses these into `Expr::Struct` and then hits
//! `todo!("Support structs!")` in `objects.rs` when executing, which would take
//! down the admission path.

use rusternetes_common::cel::{CELContext, CELEvaluator};
use serde_json::{json, Value};

fn eval(expr: &str) -> anyhow::Result<Value> {
    let mut ev = CELEvaluator::new();
    let ctx = CELContext::for_admission(
        &json!({"spec": {"replicas": 2, "items": [1, 2]}}),
        None,
        None,
    )?;
    let v = ev.evaluate_to_value(expr, &ctx)?;
    v.json().map_err(|e| anyhow::anyhow!("{e}"))
}

#[test]
fn object_literals_evaluate_to_maps() {
    assert_eq!(eval("Object{}").unwrap(), json!({}));
    assert_eq!(
        eval("Object{spec: Object.spec{replicas: 3}}").unwrap(),
        json!({"spec": {"replicas": 3}})
    );
    assert_eq!(
        eval(
            r#"Object{spec: Object.spec{template: Object.spec.template{containers: [
                 Object.spec.template.containers{name: "nginx", args: ["-g"]}]}}}"#
        )
        .unwrap(),
        json!({"spec": {"template": {"containers": [{"name": "nginx", "args": ["-g"]}]}}})
    );
    assert_eq!(
        eval(r#"Object{annotations: {"foo": "bar"}}"#).unwrap(),
        json!({"annotations": {"foo": "bar"}})
    );
    assert_eq!(
        eval("size(Object{intList: [1, 2, 3]}.intList)").unwrap(),
        json!(3)
    );
    assert_eq!(
        eval(
            "Object{spec: Object.spec{replicas: 3}} == Object{spec: Object.spec{replicas: 1 + 2}}"
        )
        .unwrap(),
        json!(true)
    );
}

#[test]
fn field_values_are_evaluated_against_the_context() {
    assert_eq!(
        eval("Object{spec: Object.spec{replicas: object.spec.replicas + 1}}").unwrap(),
        json!({"spec": {"replicas": 3}})
    );
    assert_eq!(
        eval("object.spec.items.map(i, Object{v: i})").unwrap(),
        json!([{"v": 1}, {"v": 2}])
    );
}

#[test]
fn jsonpatch_literals_evaluate_to_maps() {
    assert_eq!(
        eval(r#"JSONPatch{op: "add", path: "/spec/replicas", value: 3}"#).unwrap(),
        json!({"op": "add", "path": "/spec/replicas", "value": 3})
    );
    assert_eq!(
        eval(r#"JSONPatch{op: "remove", path: "/spec/replicas"}"#).unwrap(),
        json!({"op": "remove", "path": "/spec/replicas"})
    );
    assert_eq!(
        eval(r#"JSONPatch{op: "move", from: "/a", path: "/b"}"#).unwrap(),
        json!({"op": "move", "from": "/a", "path": "/b"})
    );
    // typeresolver_test.go "logic around JSONPatch"
    assert_eq!(
        eval(
            r#"true ? JSONPatch{op: "add", path: "/spec/replicas", value: 3}
                    : JSONPatch{op: "remove", path: "/spec/replicas"}"#
        )
        .unwrap(),
        json!({"op": "add", "path": "/spec/replicas", "value": 3})
    );
    // "JSONPatch invalid op" / "missing path": not a CEL-level error.
    assert_eq!(
        eval(r#"JSONPatch{op: "invalid", value: 3}"#).unwrap(),
        json!({"op": "invalid", "value": 3})
    );
}

#[test]
fn rejected_initializers_are_errors_not_panics() {
    for expr in [
        // jsonpatch.go:51-74: unknown field, non-string op/path/from.
        r#"JSONPatch{bogus: 1}"#,
        r#"JSONPatch{op: 1}"#,
        r#"JSONPatch{path: 1}"#,
        r#"JSONPatch{from: [1]}"#,
        // typeresolver.go:32-42: only Object[.path] and JSONPatch resolve.
        "Invalid{}",
        "Objectish{}",
        // objects.go:180 typeCheck: nested type names must match the field path.
        "Object{spec: Object.status{replicas: 3}}",
        "Object{spec: Object{replicas: 3}}",
        "Object{spec: Object.spec{t: Object.spec.u{}}}",
        "Object{l: [Object.m{}]}",
        // typeresolver_test.go spells list elements `.item{}`; that resolves, but
        // the field-path check the patchers apply (lists are unnamed) rejects it.
        "Object{c: [Object.c.item{}]}",
        // dynamic/objects.go convertField: map keys must be strings.
        "Object{m: {1: 2}}",
    ] {
        let r = std::panic::catch_unwind(|| eval(expr));
        let r = r.unwrap_or_else(|_| panic!("{expr} panicked"));
        assert!(r.is_err(), "{expr} should be an error, got {r:?}");
    }
}

/// MAP validation (`cel_env`) and evaluation (`cel_struct`) must agree: what
/// the checker accepts evaluates, and its compile-time rejections are also
/// rejected by evaluation.
#[test]
fn validation_and_evaluation_compose() {
    use rusternetes_common::cel_env::{mutation_env_failure, ExpectedOutput, MutationEnv};
    let env = MutationEnv {
        has_params: false,
        has_patch_types: true,
        variables: None,
    };
    let check = |e: &str| mutation_env_failure(e, &env, ExpectedOutput::Any);
    for ok in [
        "Object{}",
        "Object{spec: Object.spec{replicas: object.spec.replicas + 1}}",
        r#"[JSONPatch{op: "add", path: "/spec/replicas", value: 3}]"#,
        r#"JSONPatch{op: "remove", path: "/a"}"#,
    ] {
        assert_eq!(check(ok), None, "{ok}");
        assert!(eval(ok).is_ok(), "{ok}");
    }
    for bad in ["Invalid{}", "JSONPatch{bogus: 1}"] {
        assert!(check(bad).is_some(), "{bad}");
        assert!(eval(bad).is_err(), "{bad}");
    }
    // The checker accepts these (upstream rejects them at evaluation time).
    for rt in ["Object{spec: Object.status{}}", "JSONPatch{op: 1}"] {
        assert_eq!(check(rt), None, "{rt}");
        assert!(eval(rt).is_err(), "{rt}");
    }
}

#[test]
fn hostile_input_never_panics() {
    for expr in [
        "Object{a: Object.a{b: Object.a.b{c: JSONPatch{op: 1}}}}",
        "Object{}.nope",
        "Object{}[0]",
        "[Object{}, JSONPatch{}][5]",
        "has(Object{a: 1}.a)",
        "Object{a: 1 / 0}",
        "Object{a: undeclared_var}",
        "Object{a: Object{b: }}",
        "Object{",
        "JSONPatch{op: \"add\", value: Object{x: [JSONPatch{}]}}",
        "{\"a\": Object{}}.a",
        "Object{a: 1}.a.b.c",
    ] {
        let r = std::panic::catch_unwind(|| eval(expr));
        assert!(r.is_ok(), "{expr} panicked");
    }
}
