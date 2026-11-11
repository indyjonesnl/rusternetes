//! `checker/cost_test.go` (`TestCost`, release v0.26.0, the version Kubernetes
//! release-1.35 vendors) over the cost estimator of `checker/cost.go`, plus
//! the overflow-safe estimate arithmetic.
//!
//! Not ported: the two cases that install an `OverloadCostEstimate` through the
//! test-only `list_bytes_max` / `contains_string` overrides.

use cel::parser::Parser;
use k3rs_cel_checker::cost::{
    cost, AstNode, CallEstimate, CostEstimate, CostEstimator, CostOptions, SizeEstimate,
};
use k3rs_cel_checker::overload_ids::TIMESTAMP_TO_YEAR;
use k3rs_cel_checker::{
    check, Container, DType, Env, MapProvider, NoLocations, Options, VariableDecl,
};
use std::collections::HashMap;
use std::sync::Arc;

const ALL_TYPES: &str = "google.expr.proto3.test.TestAllTypes";

fn all_types() -> DType {
    DType::Struct(ALL_TYPES.into())
}

fn list(t: DType) -> DType {
    DType::List(Box::new(t))
}

fn map(k: DType, v: DType) -> DType {
    DType::Map(Box::new(k), Box::new(v))
}

/// `testCostEstimator`.
struct TestCostEstimator {
    hints: HashMap<String, u64>,
}

impl CostEstimator for TestCostEstimator {
    fn estimate_size(&self, node: &AstNode<'_>) -> Option<SizeEstimate> {
        if let Some(l) = self.hints.get(&node.path.join(".")) {
            return Some(SizeEstimate { min: 0, max: *l });
        }
        if node.ty == DType::Bytes {
            return Some(SizeEstimate { min: 0, max: 12 });
        }
        None
    }

    fn estimate_call_cost(
        &self,
        _function: &str,
        overload_id: &str,
        _target: Option<&AstNode<'_>>,
        _args: &[AstNode<'_>],
    ) -> Option<CallEstimate> {
        (overload_id == TIMESTAMP_TO_YEAR).then_some(CallEstimate {
            cost: CostEstimate { min: 7, max: 7 },
            result_size: None,
        })
    }
}

struct Case {
    name: &'static str,
    expr: &'static str,
    vars: Vec<(&'static str, DType)>,
    hints: Vec<(&'static str, u64)>,
    presence_test_has_cost: bool,
    wanted: (u64, u64),
}

fn case(
    name: &'static str,
    expr: &'static str,
    vars: Vec<(&'static str, DType)>,
    hints: Vec<(&'static str, u64)>,
    wanted: (u64, u64),
) -> Case {
    Case {
        name,
        expr,
        vars,
        hints,
        presence_test_has_cost: true,
        wanted,
    }
}

fn none<T>() -> Vec<T> {
    Vec::new()
}

fn estimate(c: &Case) -> CostEstimate {
    let provider = MapProvider::new().with_struct(
        ALL_TYPES,
        &[
            ("single_int32", DType::Int),
            ("single_float", DType::Double),
            ("single_string", DType::Str),
        ],
    );
    let expr = Parser::new()
        .parse(c.expr)
        .unwrap_or_else(|e| panic!("{}: {e}", c.name));
    let mut env = Env::with_default_declarations(
        Container::default(),
        Arc::new(provider),
        Options::default(),
    );
    for (n, t) in &c.vars {
        env.add_ident(VariableDecl::new(*n, t.clone())).unwrap();
    }
    let (checked, errs) = check(&expr, &mut env, &NoLocations);
    assert!(
        errs.errors().is_empty(),
        "{}: {:?}",
        c.name,
        errs.errors()
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
    );
    let estimator = TestCostEstimator {
        hints: c.hints.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
    };
    cost(
        &checked,
        &expr,
        &estimator,
        CostOptions {
            presence_test_free: !c.presence_test_has_cost,
            overload_estimators: None,
        },
    )
}

#[test]
fn cost_table() {
    let all_list = list(all_types());
    let int_list = list(DType::Int);
    let nested_list = list(all_list.clone());
    let all_map = map(DType::Str, all_types());
    let nested_map = map(DType::Str, all_map.clone());
    let v = |n: &'static str, t: &DType| (n, t.clone());
    let ts = DType::Timestamp;
    let input = |t: &DType| vec![v("input", t)];
    let mut cases = vec![
        case("const", r#""Hello World!""#, none(), none(), (0, 0)),
        case("identity", "input", input(&int_list), none(), (1, 1)),
        case(
            "select: map",
            "input['key']",
            input(&map(DType::Str, DType::Str)),
            none(),
            (2, 2),
        ),
        case(
            "select: field",
            "input.single_int32",
            input(&all_types()),
            none(),
            (2, 2),
        ),
        case(
            "select: field test only",
            "has(input.single_int32)",
            input(&all_types()),
            none(),
            (2, 2),
        ),
        case(
            "select: non-proto field test has() cost",
            "has(input.testAttr.nestedAttr)",
            input(&nested_map),
            none(),
            (3, 3),
        ),
        case(
            "select: non-proto field test",
            "has(input.testAttr.nestedAttr)",
            input(&nested_map),
            none(),
            (3, 3),
        ),
        case(
            "estimated function call",
            "input.getFullYear()",
            input(&ts),
            none(),
            (8, 8),
        ),
        case("create list", "[1, 2, 3]", none(), none(), (10, 10)),
        case(
            "create struct",
            "google.expr.proto3.test.TestAllTypes{single_int32: 1, single_float: 3.14, single_string: 'str'}",
            none(),
            none(),
            (40, 40),
        ),
        case(
            "create map",
            r#"{"a": 1, "b": 2, "c": 3}"#,
            none(),
            none(),
            (30, 30),
        ),
        case(
            "all comprehension",
            "input.all(x, true)",
            input(&all_list),
            vec![("input", 100)],
            (2, 302),
        ),
        case(
            "nested all comprehension",
            "input.all(x, x.all(y, true))",
            input(&nested_list),
            vec![("input", 50), ("input.@items", 10)],
            (2, 1752),
        ),
        case(
            "all comprehension on literal",
            "[1, 2, 3].all(x, true)",
            none(),
            none(),
            (20, 20),
        ),
        case(
            "variable cost function",
            "input.matches('[0-9]')",
            input(&DType::Str),
            vec![("input", 500)],
            (3, 103),
        ),
        case(
            "variable cost function with constant",
            "'123'.matches('[0-9]')",
            none(),
            none(),
            (2, 2),
        ),
        case("or", "true || false", none(), none(), (0, 0)),
        case("and", "true && false", none(), none(), (0, 0)),
        case("lt", "1 < 2", none(), none(), (1, 1)),
        case("lte", "1 <= 2", none(), none(), (1, 1)),
        case("eq", "1 == 2", none(), none(), (1, 1)),
        case("gt", "2 > 1", none(), none(), (1, 1)),
        case("gte", "2 >= 1", none(), none(), (1, 1)),
        case("in", "2 in [1, 2, 3]", none(), none(), (13, 13)),
        case("plus", "1 + 1", none(), none(), (1, 1)),
        case("minus", "1 - 1", none(), none(), (1, 1)),
        case("/", "1 / 1", none(), none(), (1, 1)),
        case("*", "1 * 1", none(), none(), (1, 1)),
        case("%", "1 % 1", none(), none(), (1, 1)),
        case("ternary", "true ? 1 : 2", none(), none(), (0, 0)),
        case("string size", r#"size("123")"#, none(), none(), (1, 1)),
        case("bytes size", r#"size(b"123")"#, none(), none(), (1, 1)),
        case(
            "bytes to string conversion",
            "string(input)",
            input(&DType::Bytes),
            vec![("input", 500)],
            (1, 51),
        ),
        case(
            "bytes to string conversion equality",
            "string(input) == string(input)",
            input(&DType::Bytes),
            vec![("input", 500)],
            (3, 152),
        ),
        case(
            "string to bytes conversion",
            "bytes(input)",
            input(&DType::Str),
            vec![("input", 500)],
            (1, 51),
        ),
        case(
            "string to bytes conversion equality",
            "bytes(input) == bytes(input)",
            input(&DType::Str),
            vec![("input", 500)],
            (3, 302),
        ),
        case("int to string conversion", "string(1)", none(), none(), (1, 1)),
        case(
            "contains",
            "input.contains(arg1)",
            vec![v("input", &DType::Str), v("arg1", &DType::Str)],
            vec![("input", 500), ("arg1", 500)],
            (2, 2502),
        ),
        case(
            "matches",
            r"input.matches('\\d+a\\d+b')",
            input(&DType::Str),
            vec![("input", 500)],
            (3, 103),
        ),
        case(
            "startsWith",
            "input.startsWith(arg1)",
            vec![v("input", &DType::Str), v("arg1", &DType::Str)],
            vec![("arg1", 500)],
            (2, 52),
        ),
        case(
            "endsWith",
            "input.endsWith(arg1)",
            vec![v("input", &DType::Str), v("arg1", &DType::Str)],
            vec![("arg1", 500)],
            (2, 52),
        ),
        case("size receiver", "input.size()", input(&DType::Str), none(), (2, 2)),
        case("size", "size(input)", input(&DType::Str), none(), (2, 2)),
        case(
            "ternary eval",
            "(x > 2 ? input1 : input2).all(y, true)",
            vec![
                v("x", &DType::Int),
                v("input1", &all_list),
                v("input2", &all_list),
            ],
            vec![("input1", 1), ("input2", 1)],
            (4, 7),
        ),
        case(
            "comprehension over map",
            "input.all(k, input[k].single_int32 > 3)",
            input(&all_map),
            vec![("input", 10)],
            (2, 82),
        ),
        case(
            "comprehension over nested map of maps",
            "input.all(k, input[k].all(x, true))",
            input(&nested_map),
            vec![("input", 5), ("input.@values", 10)],
            (2, 187),
        ),
        case(
            "string size of map keys",
            "input.all(k, k.contains(k))",
            input(&nested_map),
            vec![("input", 5), ("input.@keys", 10)],
            (2, 32),
        ),
        case(
            "comprehension variable shadowing",
            "input.all(k, input[k].all(k, true) && k.contains(k))",
            input(&nested_map),
            vec![("input", 2), ("input.@values", 2), ("input.@keys", 5)],
            (2, 34),
        ),
        case(
            "list concat",
            "(list1 + list2).all(x, true)",
            vec![v("list1", &int_list), v("list2", &int_list)],
            vec![("list1", 10), ("list2", 10)],
            (4, 64),
        ),
        case(
            "str concat",
            r#""abcdefg".contains(str1 + str2)"#,
            vec![v("str1", &DType::Str), v("str2", &DType::Str)],
            vec![("str1", 10), ("str2", 10)],
            (2, 6),
        ),
        case(
            "list size comparison",
            "list1.size() == list2.size()",
            vec![v("list1", &int_list), v("list2", &int_list)],
            none(),
            (5, 5),
        ),
        case(
            "list size from ternary",
            "x > y ? list1.size() : list2.size()",
            vec![
                v("x", &DType::Int),
                v("y", &DType::Int),
                v("list1", &int_list),
                v("list2", &int_list),
            ],
            none(),
            (5, 5),
        ),
        case(
            "list size from concat",
            "([x, y] + list1 + list2).size()",
            vec![
                v("x", &DType::Int),
                v("y", &DType::Int),
                v("list1", &int_list),
                v("list2", &int_list),
            ],
            vec![("list1", 10), ("list2", 20)],
            (17, 17),
        ),
        case(
            "list cost tracking through comprehension",
            "[list1, list2].exists(l, l.exists(v, v.startsWith('hi')))",
            vec![v("list1", &list(DType::Str)), v("list2", &list(DType::Str))],
            vec![
                ("list1", 10),
                ("list1.@items", 64),
                ("list2", 20),
                ("list2.@items", 128),
            ],
            (21, 265),
        ),
        case(
            "str endsWith equality",
            r#"str1.endsWith("abcdefghijklmnopqrstuvwxyz") == str2.endsWith("abcdefghijklmnopqrstuvwxyz")"#,
            vec![v("str1", &DType::Str), v("str2", &DType::Str)],
            none(),
            (9, 9),
        ),
        case(
            "nested subexpression operators",
            "((5 != 6) == (1 == 2)) == ((3 <= 4) == (9 != 9))",
            none(),
            none(),
            (7, 7),
        ),
        case(
            "str size estimate",
            "string(timestamp1) == string(timestamp2)",
            vec![v("timestamp1", &ts), v("timestamp2", &ts)],
            none(),
            (5, 1844674407370955268),
        ),
        case(
            "timestamp equality check",
            "timestamp1 == timestamp2",
            vec![v("timestamp1", &ts), v("timestamp2", &ts)],
            none(),
            (3, 3),
        ),
        case(
            "duration inequality check",
            "duration1 != duration2",
            vec![
                v("duration1", &DType::Duration),
                v("duration2", &DType::Duration),
            ],
            none(),
            (3, 3),
        ),
        case(
            ".filter list literal",
            "[1,2,3,4,5].filter(x, x % 2 == 0)",
            none(),
            none(),
            (41, 101),
        ),
        case(".map list literal", "[1,2,3,4,5].map(x, x)", none(), none(), (86, 86)),
        case(
            ".map.filter list literal",
            "[1,2,3,4,5].map(x, x).filter(x, x % 2 == 0)",
            none(),
            none(),
            (117, 177),
        ),
        case(
            ".map.exists list literal",
            "[1,2,3,4,5].map(x, x).exists(x, x == 5) == true",
            none(),
            none(),
            (108, 118),
        ),
        case(
            ".map.map list literal",
            "[1,2,3,4,5].map(x, x).map(x, x)",
            none(),
            none(),
            (162, 162),
        ),
        case(
            ".map list literal selection",
            "[1,2,3,4,5].map(x, x)[4]",
            none(),
            none(),
            (87, 87),
        ),
        case(
            "nested array selection",
            "[[1,2],[1,2],[1,2],[1,2],[1,2]][4]",
            none(),
            none(),
            (61, 61),
        ),
        case(
            "nested map selection",
            "{'a': [1,2], 'b': [1,2], 'c': [1,2], 'd': [1,2], 'e': [1,2]}.b",
            none(),
            none(),
            (81, 81),
        ),
        case(
            "comprehension on nested list",
            "[[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]].all(y, y.all(y, y == 1))",
            none(),
            none(),
            (76, 136),
        ),
        case(
            "comprehension on transformed nested list",
            "[1,2,3,4,5].map(x, [x, x]).all(y, y.all(y, y == 1))",
            none(),
            none(),
            (157, 217),
        ),
        case(
            "comprehension on nested literal list",
            r#"["a", "ab", "abc", "abcd", "abcde"].map(x, [x, x]).all(y, y.all(y, y.startsWith('a')))"#,
            none(),
            none(),
            (157, 217),
        ),
        case(
            "comprehension on nested variable list",
            "input.map(x, [x, x]).all(y, y.all(y, y.startsWith('a')))",
            input(&list(DType::Str)),
            vec![("input", 5), ("input.@items", 10)],
            (13, 208),
        ),
        case(
            "comprehension chaining with concat",
            "[1,2,3,4,5].map(x, x).map(x, x) + [1]",
            none(),
            none(),
            (173, 173),
        ),
        case(
            "nested comprehension",
            "[1,2,3].all(i, i in [1,2,3].map(j, j + j))",
            none(),
            none(),
            (20, 230),
        ),
        case(
            "nested dyn comprehension",
            "dyn([1,2,3]).all(i, i in dyn([1,2,3]).map(j, j + j))",
            none(),
            none(),
            (21, 234),
        ),
        case(
            "literal map access",
            "{'hello': 'hi'}['hello'] != {'hello': 'bye'}['hello']",
            none(),
            none(),
            (63, 63),
        ),
        case(
            "literal list access",
            "['hello', 'hi'][0] != ['hello', 'bye'][1]",
            none(),
            none(),
            (23, 23),
        ),
        case("type call", "type(1)", none(), none(), (1, 1)),
        case(
            "type call variable",
            "type(self.val1)",
            vec![v("self", &map(DType::Str, DType::Int))],
            none(),
            (3, 3),
        ),
        case(
            "type call variable equality",
            "type(self.val1) == int",
            vec![v("self", &map(DType::Str, DType::Int))],
            none(),
            (5, 1844674407370955268),
        ),
        case(
            "type literal equality cost",
            "type(1) == int",
            none(),
            none(),
            (3, 1844674407370955266),
        ),
        case(
            "namespace variable equality",
            "self.val1 == 1.0",
            vec![v("self.val1", &DType::Double)],
            none(),
            (2, 2),
        ),
        case(
            "simple map variable equality",
            "self.val1 == 1.0",
            vec![v("self", &map(DType::Str, DType::Double))],
            none(),
            (3, 3),
        ),
        case(
            "date-time math",
            "self.val1 == timestamp('2011-08-18T00:00:00.000+01:00') + duration('19h3m37s10ms')",
            vec![v("self", &map(DType::Str, ts.clone()))],
            none(),
            (6, 6),
        ),
        case(
            "date-time math self-conversion",
            "timestamp(self.val1) == timestamp('2011-08-18T00:00:00.000+01:00') + duration('19h3m37s10ms')",
            vec![v("self", &map(DType::Str, ts.clone()))],
            none(),
            (7, 7),
        ),
        case(
            "boolean vars equal",
            "self.val1 != self.val2",
            vec![v("self", &map(DType::Str, DType::Bool))],
            none(),
            (5, 5),
        ),
        case(
            "boolean var equals literal",
            "self.val1 != true",
            vec![v("self", &map(DType::Str, DType::Bool))],
            none(),
            (3, 3),
        ),
        case(
            "double var equals literal",
            "self.val1 == 1.0",
            vec![v("self", &map(DType::Str, DType::Double))],
            none(),
            (3, 3),
        ),
    ];
    let bools = |n: &'static str| (n, DType::Bool);
    cases.push(case(
        "or accumulated branch cost",
        "a || b || c || d",
        vec![bools("a"), bools("b"), bools("c"), bools("d")],
        none(),
        (1, 4),
    ));
    cases.push(case(
        "and accumulated branch cost",
        "a && b && c && d",
        vec![bools("a"), bools("b"), bools("c"), bools("d")],
        none(),
        (1, 4),
    ));
    // The `PresenceTestHasCost(false)` cases.
    let mut no_has_cost = case(
        "select: field test only no has() cost",
        "has(input.single_int32)",
        input(&all_types()),
        none(),
        (1, 1),
    );
    no_has_cost.presence_test_has_cost = false;
    cases.push(no_has_cost);
    let mut no_has_cost = case(
        "select: non-proto field test no has() cost",
        "has(input.testAttr.nestedAttr)",
        input(&nested_map),
        none(),
        (2, 2),
    );
    no_has_cost.presence_test_has_cost = false;
    cases.push(no_has_cost);

    for c in &cases {
        let got = estimate(c);
        assert_eq!((got.min, got.max), c.wanted, "{}: `{}`", c.name, c.expr);
    }
}

#[test]
fn estimates_do_not_overflow() {
    let big = SizeEstimate {
        min: u64::MAX,
        max: u64::MAX,
    };
    assert_eq!(big.add(SizeEstimate::fixed(1)), big);
    assert_eq!(big.multiply(SizeEstimate::fixed(2)), big);
    assert_eq!(
        SizeEstimate::fixed(3).multiply(SizeEstimate::fixed(0)),
        SizeEstimate::fixed(0)
    );
    let c = CostEstimate::fixed(u64::MAX);
    assert_eq!(c.add(CostEstimate::fixed(1)), c);
    assert_eq!(c.multiply(CostEstimate::fixed(2)), c);
}

#[test]
fn cost_factor_rounds_up_and_saturates() {
    assert_eq!(
        SizeEstimate::fixed(5).multiply_by_cost_factor(0.1),
        CostEstimate::fixed(1)
    );
    assert_eq!(
        SizeEstimate::fixed(11).multiply_by_cost_factor(0.1),
        CostEstimate::fixed(2)
    );
    assert_eq!(
        SizeEstimate::fixed(0).multiply_by_cost_factor(0.25),
        CostEstimate::fixed(0)
    );
    // `UnknownSizeEstimate().MultiplyByCostFactor(1)` is `UnknownCostEstimate`.
    assert_eq!(
        SizeEstimate::UNKNOWN.multiply_by_cost_factor(1.0),
        CostEstimate::UNKNOWN
    );
}

#[test]
fn union_covers_both() {
    let a = SizeEstimate { min: 2, max: 5 };
    let b = SizeEstimate { min: 1, max: 4 };
    assert_eq!(a.union(b), SizeEstimate { min: 1, max: 5 });
    let a = CostEstimate { min: 2, max: 5 };
    let b = CostEstimate { min: 1, max: 4 };
    assert_eq!(a.union(b), CostEstimate { min: 1, max: 5 });
}
