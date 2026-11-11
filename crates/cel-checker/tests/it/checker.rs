//! Table tests ported from cel-go `checker/checker_test.go` (`TestCheck`,
//! `TestCheckInvalidOptSelectMember`, ...): the expected output type and the
//! error messages, without the typed-AST printing and without source
//! locations (the `cel` 0.13 parser drops them).

use cel::parser::Parser;
use k3rs_cel_checker::{
    check, Container, DType, Env, MapProvider, NoLocations, Options, VariableDecl,
};
use std::sync::Arc;

const PB3: &str = "google.expr.proto3.test";

fn list(t: DType) -> DType {
    DType::List(Box::new(t))
}

fn map(k: DType, v: DType) -> DType {
    DType::Map(Box::new(k), Box::new(v))
}

fn msg(n: &str) -> DType {
    DType::Struct(n.into())
}

fn provider() -> MapProvider {
    MapProvider::new()
        .with_struct(
            &format!("{PB3}.TestAllTypes"),
            &[
                ("single_int32", DType::Int),
                ("single_int64", DType::Int),
                ("repeated_int64", list(DType::Int)),
                ("map_int64_nested_type", map(DType::Int, DType::Dyn)),
                (
                    "single_nested_message",
                    msg(&format!("{PB3}.TestAllTypes.NestedMessage")),
                ),
                (
                    "repeated_nested_message",
                    list(msg(&format!("{PB3}.TestAllTypes.NestedMessage"))),
                ),
            ],
        )
        .with_struct(
            &format!("{PB3}.TestAllTypes.NestedMessage"),
            &[("bb", DType::Int)],
        )
}

/// The `default` environment of checker_test.go: typed identifiers `is`,
/// `ii`, ... plus the member function `string.fi_s_s() -> string` is not
/// needed here; custom identifiers are passed per case.
fn run(
    src: &str,
    container: &str,
    idents: &[(&str, DType)],
    opts: Options,
) -> (Option<String>, Vec<String>) {
    let expr = Parser::new()
        .enable_optional_syntax(true)
        .parse(src)
        .unwrap_or_else(|e| panic!("{src}: {e}"));
    let mut env =
        Env::with_default_declarations(Container::new(container), Arc::new(provider()), opts);
    for (n, t) in idents {
        env.add_ident(VariableDecl::new(*n, t.clone())).unwrap();
    }
    let (ast, errs) = check(&expr, &mut env, &NoLocations);
    let msgs = errs.errors().iter().map(|e| e.message.clone()).collect();
    (ast.output_type().map(DType::name), msgs)
}

fn ok(src: &str, idents: &[(&str, DType)], want: DType) {
    ok_in(src, "", idents, want);
}

fn ok_in(src: &str, container: &str, idents: &[(&str, DType)], want: DType) {
    let (t, errs) = run(src, container, idents, Options::default());
    assert!(errs.is_empty(), "{src}: unexpected errors {errs:?}");
    assert_eq!(t.as_deref(), Some(want.name().as_str()), "{src}");
}

fn err(src: &str, idents: &[(&str, DType)], want: &[&str]) {
    err_in(src, "", idents, want);
}

fn err_in(src: &str, container: &str, idents: &[(&str, DType)], want: &[&str]) {
    let (_, errs) = run(src, container, idents, Options::default());
    assert_eq!(errs, want, "{src}");
}

#[test]
fn constants() {
    ok(r#""A""#, &[], DType::Str);
    ok("12", &[], DType::Int);
    ok("12u", &[], DType::Uint);
    ok("true", &[], DType::Bool);
    ok("12.23", &[], DType::Double);
    ok("null", &[], DType::Null);
    ok(r#"b"ABC""#, &[], DType::Bytes);
}

#[test]
fn identifiers() {
    let ids = [
        ("is", DType::Str),
        ("ii", DType::Int),
        ("iu", DType::Uint),
        ("iz", DType::Bool),
        ("id", DType::Double),
        ("ix", DType::Null),
        ("ib", DType::Bytes),
    ];
    for (n, t) in &ids {
        ok(n, &ids, t.clone());
    }
    err(
        "foo",
        &[],
        &["undeclared reference to 'foo' (in container '')"],
    );
    err(
        "1 + x",
        &[],
        &["undeclared reference to 'x' (in container '')"],
    );
}

#[test]
fn aggregate_literals() {
    ok("[]", &[], list(DType::Dyn));
    ok("[1]", &[], list(DType::Int));
    ok(r#"[1, "A"]"#, &[], list(DType::Dyn));
    ok("{}", &[], map(DType::Dyn, DType::Dyn));
    ok("{1:2u, 2:3u}", &[], map(DType::Int, DType::Uint));
    ok(r#"{"a":1, "b":2}.a"#, &[], DType::Int);
    ok("{1:2u, 2u:3}", &[], map(DType::Dyn, DType::Dyn));
    ok("[1, 2u] + []", &[], list(DType::Dyn));
    ok("[] + [1,2,3,] + [4]", &[], list(DType::Int));
    ok("[1] + [dyn('string')]", &[], list(DType::Dyn));
    ok("[dyn('string')] + [1]", &[], list(DType::Dyn));
    ok(
        "([[[1]], [[2]], [[3]]][0][0] + [2, 3, {'four': {'five': 'six'}}])[3]",
        &[],
        DType::Dyn,
    );
}

#[test]
fn homogeneous_aggregate_literals_option() {
    let opts = Options {
        homogeneous_aggregate_literals: true,
        ..Options::default()
    };
    let (_, errs) = run(r#"[1, "A"]"#, "", &[], opts);
    assert_eq!(errs, ["expected type 'int' but found 'string'"]);
}

#[test]
fn operators_and_calls() {
    let x = [("x", list(DType::Int))];
    ok("1 + 2", &[], DType::Int);
    ok("[1] + [2]", &[], list(DType::Int));
    ok("size(x) == x.size()", &x, DType::Bool);
    ok(r#"int(1u) + int(uint("1"))"#, &[], DType::Int);
    ok("false && !true || false ? 2 : 3", &[], DType::Int);
    ok(r#"b"abc" + b"def""#, &[], DType::Bytes);
    ok("1.0 + 2.0 * 3.0 - 1.0 / 2.20202 != 66.6", &[], DType::Bool);
    ok("null == null && null != null", &[], DType::Bool);
    ok("1 + 2 * 3 - 1 / 2 == 6 % 1", &[], DType::Bool);
    ok(r#""abc" + "def""#, &[], DType::Str);
    ok("1u + 2u * 3u - 1u / 2u == 6u % 1u", &[], DType::Bool);
    ok("1 in [1, 2, 3]", &[], DType::Bool);
    ok("1 in dyn([1, 2, 3])", &[], DType::Bool);
    ok("type(null) == null_type", &[], DType::Bool);
    ok("type(type) == type", &[], DType::Bool);
    ok("list == type([1]) && map == type({1:2u})", &[], DType::Bool);
    err(
        "fun()",
        &[],
        &["undeclared reference to 'fun' (in container '')"],
    );
    err(
        "'string'.fun()",
        &[],
        &["undeclared reference to 'fun' (in container '')"],
    );
    err(
        "[].length",
        &[],
        &["type 'list(_var0)' does not support field selection"],
    );
    err(
        "x + y",
        &[
            ("x", list(msg(&format!("{PB3}.TestAllTypes")))),
            ("y", list(DType::Int)),
        ],
        &[&format!(
            "found no matching overload for '_+_' applied to '(list({PB3}.TestAllTypes), list(int))'"
        )],
    );
    err(
        "x[1u]",
        &[("x", list(msg(&format!("{PB3}.TestAllTypes"))))],
        &[&format!(
            "found no matching overload for '_[_]' applied to '(list({PB3}.TestAllTypes), uint)'"
        )],
    );
}

#[test]
fn cross_type_numeric_comparisons() {
    // Disabled: each mixed comparison is `found no matching overload`.
    let src = "1 <= 1.0 && 1u <= 1.0 && 1.0 <= 1 && 1.0 <= 1u && 1 <= 1u && 1u <= 1";
    let (_, errs) = run(src, "", &[], Options::default());
    assert_eq!(
        errs,
        [
            "found no matching overload for '_<=_' applied to '(int, double)'",
            "found no matching overload for '_<=_' applied to '(uint, double)'",
            "found no matching overload for '_<=_' applied to '(double, int)'",
            "found no matching overload for '_<=_' applied to '(double, uint)'",
            "found no matching overload for '_<=_' applied to '(int, uint)'",
            "found no matching overload for '_<=_' applied to '(uint, int)'",
        ]
    );
    // Enabled.
    let opts = Options {
        cross_type_numeric_comparisons: true,
        ..Options::default()
    };
    for op in ["<=", "<", ">", ">="] {
        let src = format!(
            "1 {op} 1.0 && 1u {op} 1.0 && 1.0 {op} 1 && 1.0 {op} 1u && 1 {op} 1u && 1u {op} 1"
        );
        let (t, errs) = run(&src, "", &[], opts.clone());
        assert!(errs.is_empty(), "{src}: {errs:?}");
        assert_eq!(t.as_deref(), Some("bool"));
    }
}

#[test]
fn struct_creation_and_fields() {
    let n = format!("{PB3}.TestAllTypes");
    ok_in(
        "TestAllTypes{single_int32: 1, single_int64: 2}",
        PB3,
        &[],
        msg(&n),
    );
    err_in(
        "TestAllTypes{single_int32: 1u}",
        PB3,
        &[],
        &["expected type of field 'single_int32' is 'int' but provided type is 'uint'"],
    );
    err_in(
        "TestAllTypes{single_int32: 1, undefined: 2}",
        PB3,
        &[],
        &["undefined field 'undefined'"],
    );
    ok_in(
        "TestAllTypes{}.repeated_nested_message",
        PB3,
        &[],
        list(msg(&format!("{n}.NestedMessage"))),
    );
    err("int{}", &[], &["'int' is not a message type"]);
    err(
        "Msg{}",
        &[],
        &["undeclared reference to 'Msg' (in container '')"],
    );
}

#[test]
fn field_selection_on_declared_structs() {
    let x = [("x", msg(&format!("{PB3}.TestAllTypes")))];
    ok(
        "x.single_nested_message.bb == 43 && has(x.single_nested_message)",
        &x,
        DType::Bool,
    );
    ok("x.repeated_int64[x.single_int32] == 23", &x, DType::Bool);
    ok("size(x.map_int64_nested_type) == 0", &x, DType::Bool);
    ok(
        "x.repeated_int64.map(x, double(x))",
        &x,
        list(DType::Double),
    );
    ok(
        "x.repeated_int64.map(x, x > 0, double(x))",
        &x,
        list(DType::Double),
    );
    ok(
        "x.repeated_int64.all(e, e > 0) && x.repeated_int64.exists(e, e < 0) && x.repeated_int64.exists_one(e, e == 0)",
        &x,
        DType::Bool,
    );
    ok("x.single_nested_message != null", &x, DType::Bool);
    err(
        "x.single_int64 != null",
        &x,
        &["found no matching overload for '_!=_' applied to '(int, null)'"],
    );
    err(
        "x.single_nested_message.undefined == x.undefined",
        &x,
        &["undefined field 'undefined'", "undefined field 'undefined'"],
    );
}

#[test]
fn comprehensions() {
    let x = [("x", msg(&format!("{PB3}.TestAllTypes")))];
    err(
        "x.all(y, y == true)",
        &[("x", DType::Bool)],
        &["expression of type 'bool' cannot be range of a comprehension (must be list, map, or dynamic)"],
    );
    err(
        "x.all(e, 0)",
        &x,
        &[
            &format!("expression of type '{PB3}.TestAllTypes' cannot be range of a comprehension (must be list, map, or dynamic)"),
            "expected type 'bool' but found 'int'",
        ],
    );
    err(
        "x.repeated_int64.exists(y, y > 10) && y < 5",
        &x,
        &["undeclared reference to 'y' (in container '')"],
    );
    ok(
        "[1].map(x, [x, x]).map(x, [x, x])",
        &[],
        list(list(list(DType::Int))),
    );
    ok("{}.map(c,[c,type(c)])", &[], list(list(DType::Dyn)));
    ok(
        "[{}.map(c,c,c)]+[{}.map(c,c,c)]",
        &[],
        list(list(DType::Bool)),
    );
}

#[test]
fn dyn_and_untyped_selection() {
    ok("a.b + 1 == a[0]", &[("a", DType::Dyn)], DType::Bool);
    ok(
        "x.single_value + 1 / x.single_struct.y == 23",
        &[("x", map(DType::Str, DType::Dyn))],
        DType::Bool,
    );
}

#[test]
fn container_resolves_identifiers() {
    let (t, errs) = run(
        "x",
        "container",
        &[("container.x", msg(&format!("{PB3}.TestAllTypes")))],
        Options::default(),
    );
    assert!(errs.is_empty());
    assert_eq!(t.as_deref(), Some("google.expr.proto3.test.TestAllTypes"));
}

#[test]
fn optional_syntax() {
    let a = [("a", map(DType::Str, DType::Str))];
    ok("a.?b", &a, DType::optional(DType::Str));
    ok("type(a.?b) == optional_type", &a, DType::Bool);
    let ao = [("a", DType::optional(map(DType::Str, DType::Str)))];
    ok("a.b", &ao, DType::optional(DType::Str));
    ok(
        "{?'key': {'a': 'b'}.?value}",
        &[],
        map(DType::Str, DType::Str),
    );
    ok("{?'key': {'a': 'b'}.?value}.key", &[], DType::Str);
    ok("[?a.?b, ?a.?c, 'world']", &a, list(DType::Str));
    err(
        "{?'key': 'hi'}",
        &[],
        &["expected type 'optional_type(string)' but found 'string'"],
    );
    err(
        "[?'value']",
        &[],
        &["expected type 'optional_type(string)' but found 'string'"],
    );
    let n = format!("{PB3}.TestAllTypes");
    ok_in("TestAllTypes{?single_int32: {}.?i}", PB3, &[], msg(&n));
    err_in(
        "TestAllTypes{?single_int32: 1}",
        PB3,
        &[],
        &["expected type 'optional_type(int)' but found 'int'"],
    );
}

#[test]
fn error_display_string() {
    let e = k3rs_cel_checker::CheckError {
        expr_id: 1,
        location: Some(k3rs_cel_checker::Location {
            line: 1,
            column: 25,
        }),
        message: "expected type of field 'single_int32' is 'int' but provided type is 'uint'"
            .into(),
    };
    assert_eq!(
        e.to_display_string(Some("TestAllTypes{single_int32: 1u}")),
        "ERROR: <input>:1:26: expected type of field 'single_int32' is 'int' but provided type is 'uint'\n | TestAllTypes{single_int32: 1u}\n | .........................^"
    );
}

#[test]
fn declarations_reject_overlaps() {
    let mut env = Env::new(
        Container::default(),
        Arc::new(MapProvider::new()),
        Options::default(),
    );
    env.add_ident(VariableDecl::new("x", DType::Int)).unwrap();
    // An equivalent redeclaration is accepted.
    env.add_ident(VariableDecl::new("x", DType::Int)).unwrap();
    assert_eq!(
        env.add_ident(VariableDecl::new("x", DType::Str))
            .unwrap_err(),
        "overlapping identifier for name 'x'"
    );
}
