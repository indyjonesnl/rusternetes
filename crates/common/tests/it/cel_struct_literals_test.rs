//! MutatingAdmissionPolicy CEL initializers: `Object{}` / `Object.spec{}` /
//! `JSONPatch{}` (#2885).
//!
//! Ported from upstream `TestTypeResolver`
//! (`staging/src/k8s.io/apiserver/pkg/cel/mutation/typeresolver_test.go:34`).
//! The `cel` 0.13 crate parses these into `Expr::Struct` and then hits
//! `todo!("Support structs!")` in `objects.rs` when executing, which would take
//! down the admission path.

use k3rs_common::cel::{CELContext, CELEvaluator};
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
    use k3rs_common::cel_env::{mutation_env_failure, ExpectedOutput, MutationEnv};
    let env = MutationEnv {
        has_params: false,
        has_authorizer: true,
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

/// `ObjectVal.Equal` (`dynamic/objects.go:147-155`): an Object never equals a
/// plain map, and two Objects are equal only with the same type name (#2944).
#[test]
fn object_is_not_equal_to_a_plain_map() {
    assert_eq!(eval("Object{a: 1} == {'a': 1}").unwrap(), json!(false));
    assert_eq!(
        eval("Object.spec{a: 1} == Object.status{a: 1}").unwrap(),
        json!(false)
    );
    assert_eq!(
        eval("Object.spec{a: 1} == Object.spec{a: 1}").unwrap(),
        json!(true)
    );
}

/// `CheckTypeNamesMatchFieldPathNames` on an evaluated JSONPatch `value`
/// (json_patch.go:162-173) and ApplyConfiguration root (smd.go:99-102).
#[test]
fn type_names_are_checked_on_the_evaluated_result() {
    use k3rs_common::cel_struct::{compile, execute_checked, new_context_for, TypeCheck};
    let run = |src: &str, mode| {
        let c = compile(src).unwrap();
        execute_checked(&c, &new_context_for(&c), mode)
    };
    let ok = run(
        r#"[JSONPatch{op: "add", path: "/spec", value: Object{a: Object.a{}}}]"#,
        TypeCheck::JsonPatchValues,
    );
    assert!(ok.is_ok(), "{ok:?}");
    let err = run(
        r#"[JSONPatch{op: "add", path: "/spec", value: Object{a: [1].map(x, Object.b{})}}]"#,
        TypeCheck::JsonPatchValues,
    )
    .unwrap_err();
    assert!(err.contains("unexpected type name \"Object.b\", expected \"Object.a\""));
    // reached through a function, not a literal field: only a runtime check sees it
    let err = run("Object{a: [1].map(x, Object.zzz{})[0]}", TypeCheck::Root).unwrap_err();
    assert!(err.contains("\"Object.zzz\""), "{err}");
}

/// #3157: `ObjectVal.Type` (`dynamic/objects.go:158-160`) is `Object.<path>`,
/// `ObjectVal` is not a Sizer and has no marker field, so `size()` and
/// iteration see only the real fields.
#[test]
fn object_has_its_own_type_and_no_marker_field() {
    assert_eq!(
        eval("type(Object.spec{a: 1}) == type(Object.spec{})").unwrap(),
        json!(true)
    );
    assert_eq!(
        eval("type(Object{}) == type(Object.spec{})").unwrap(),
        json!(false)
    );
    assert_eq!(eval("type(Object{}) == type({})").unwrap(), json!(false));
    assert_eq!(eval("size(Object{a: 1, b: 2})").unwrap(), json!(2));
    assert_eq!(eval("Object{a: 1, b: 2}.size()").unwrap(), json!(2));
    assert_eq!(eval("size(Object{})").unwrap(), json!(0));
    assert_eq!(
        eval("Object{a: 1, b: 2}.all(k, k == 'a' || k == 'b')").unwrap(),
        json!(true)
    );
    assert_eq!(eval("Object{a: 1}.map(k, k)").unwrap(), json!(["a"]));
}

/// The structs are native values registered per expression
/// (`Env::add_type` of a `StructType`): optional initializers, `in`, the
/// JSONPatch type and `type()` of the real struct type.
#[test]
fn registered_struct_types_behave_like_the_map_rewrite() {
    // `?field:` omits an absent optional (the 0.15 evaluator ignores the flag).
    assert_eq!(
        eval("Object{?a: optional.none(), ?b: optional.of(2), c: 3}").unwrap(),
        json!({"b": 2, "c": 3})
    );
    assert_eq!(eval("Object{?a: object.?missing}").unwrap(), json!({}));
    // `in` takes the fields of an Object.
    assert_eq!(eval("'a' in Object{a: 1}").unwrap(), json!(true));
    assert_eq!(eval("'b' in Object{a: 1}").unwrap(), json!(false));
    // Field select, presence and optional select on a struct.
    assert_eq!(eval("Object{a: Object.a{b: 1}}.a.b").unwrap(), json!(1));
    assert_eq!(eval("has(Object{a: 1}.a)").unwrap(), json!(true));
    assert_eq!(eval("has(Object{a: 1}.b)").unwrap(), json!(false));
    assert_eq!(eval("Object{a: 1}.?a.hasValue()").unwrap(), json!(true));
    assert_eq!(eval("Object{a: 1}.?b.hasValue()").unwrap(), json!(false));
    // A JSONPatch is its own type, never a plain map or an Object.
    assert_eq!(
        eval(r#"JSONPatch{op: "add"} == {"op": "add"}"#).unwrap(),
        json!(false)
    );
    assert_eq!(
        eval(r#"JSONPatch{op: "add"} == JSONPatch{op: "add"}"#).unwrap(),
        json!(true)
    );
    assert_eq!(
        eval("type(JSONPatch{}) == type(Object{})").unwrap(),
        json!(false)
    );
}

/// A struct value bound into a later evaluation (a VAP variable) keeps its
/// type.
#[test]
fn struct_values_keep_their_type_when_bound_into_another_evaluation() {
    let mut ev = CELEvaluator::new();
    let ctx = CELContext::for_admission(&json!({}), None, None).unwrap();
    let v = ev.evaluate_to_value_raw("Object.a{x: 1}", &ctx).unwrap();
    let mut ctx2 = CELContext::new();
    ctx2.add_variable("v".to_string(), v);
    let eval2 =
        |ev: &mut CELEvaluator, e: &str| ev.evaluate_to_value(e, &ctx2).unwrap().json().unwrap();
    assert_eq!(eval2(&mut ev, "v == Object.a{x: 1}"), json!(true));
    assert_eq!(eval2(&mut ev, "v == Object.b{x: 1}"), json!(false));
}

/// The costed evaluator walks the field values of a struct and charges
/// `StructCreateBaseCost` (`interpreter/runtimecost.go` `InterpretableConstructor`).
#[test]
fn struct_construction_is_costed() {
    use k3rs_common::cel_cost::{CostOptions, STRUCT_CREATE_BASE_COST};
    let mut ev = CELEvaluator::new();
    let ctx = CELContext::for_admission(&json!({"spec": {"replicas": 2}}), None, None).unwrap();
    let mut cost = |expr: &str| {
        let (r, cost) = ev.evaluate_with_cost(expr, &ctx, CostOptions::default());
        assert!(r.is_ok(), "{expr}: {r:?}");
        cost
    };
    let empty = cost("Object{}");
    assert_eq!(empty, STRUCT_CREATE_BASE_COST);
    let one = cost("Object{a: object.spec.replicas}");
    assert!(one > empty, "{one} vs {empty}");
    let two = cost("Object{a: object.spec.replicas, b: object.spec.replicas}");
    assert_eq!(two - empty, 2 * (one - empty));
}
