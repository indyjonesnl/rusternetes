//! The static cost estimate of CRD rules:
//! `apiextensions-apiserver/pkg/apiserver/schema/cel/compilation_test.go`
//! `TestCostEstimation` (release-1.35), through the shadow estimator
//! ([`rusternetes_common::cel_checker_shadow::estimate_rule_cost`]) and the
//! schema size estimator of [`rusternetes_common::cel_crd_cost`].

use rusternetes_cel_checker::cost::{AstNode, CostEstimator, SizeEstimate};
use rusternetes_cel_checker::decl::limits;
use rusternetes_common::cel_checker_shadow::{estimate_rule_cost, SelfScope};
use rusternetes_common::cel_crd_cost::{max_cardinality, SchemaSizeEstimator};
use rusternetes_common::cel_schema_types::schema_decl_type;
use rusternetes_common::cel_versions::{default_compatibility_version, EnvSet, EnvType};
use rusternetes_common::resources::crd::JSONSchemaProps;
use serde_json::{json, Value};

/// `costLimit` of compilation_test.go.
const COST_LIMIT: u64 = 100_000_000;

type Gen = Box<dyn Fn(Option<i64>) -> Value>;

fn max_cost(schema: &Value) -> u64 {
    let s: JSONSchemaProps = serde_json::from_value(schema.clone()).unwrap();
    let rule = schema["x-kubernetes-validations"][0]["rule"]
        .as_str()
        .unwrap();
    let scope = SelfScope {
        decl: schema_decl_type(&s, false).expect("exposed"),
        optional_old_self: false,
    };
    let set = EnvSet::must_base_env_set(default_compatibility_version());
    estimate_rule_cost(rule, &set, EnvType::NewExpressions, &scope)
        .unwrap_or_else(|| panic!("`{rule}` must type-check"))
        .max
}

/// `parseRuleType`: `(type, format, x-kubernetes-int-or-string)`.
fn schema_of(t: &str) -> Value {
    match t {
        "duration" | "date" | "date-time" => json!({"type": "string", "format": t}),
        "int-or-string" => json!({"x-kubernetes-int-or-string": true}),
        other => json!({"type": other}),
    }
}

fn with_rule(mut s: Value, rule: &str) -> Value {
    s["x-kubernetes-validations"] = json!([{"rule": rule}]);
    s
}

fn with_max(mut s: Value, key: &str, max: Option<i64>) -> Value {
    if let Some(m) = max {
        s[key] = json!(m);
    }
    s
}

fn array_with_rule(t: &'static str, rule: &'static str) -> Gen {
    Box::new(move |max| {
        with_rule(
            with_max(
                json!({"type": "array", "items": schema_of(t)}),
                "maxItems",
                max,
            ),
            rule,
        )
    })
}

fn array_of_arrays_with_rule(t: &'static str, rule: &'static str) -> Gen {
    Box::new(move |max| {
        with_rule(
            with_max(
                json!({"type": "array", "items": {"type": "array", "items": {"type": t}}}),
                "maxItems",
                max,
            ),
            rule,
        )
    })
}

fn object_for_map() -> Value {
    json!({
        "type": "object",
        "properties": {"required": {"type": "string"}, "optional": {"type": "string"}},
        "required": ["required"]
    })
}

fn object_array_with_rule(rule: &'static str) -> Gen {
    Box::new(move |max| {
        with_rule(
            with_max(
                json!({"type": "array", "items": object_for_map()}),
                "maxItems",
                max,
            ),
            rule,
        )
    })
}

fn map_array_with_rule(t: &'static str, rule: &'static str) -> Gen {
    Box::new(move |max| {
        with_rule(
            with_max(
                json!({"type": "array", "items": {"type": "object", "additionalProperties": {"type": t}}}),
                "maxItems",
                max,
            ),
            rule,
        )
    })
}

fn map_with_custom_item_rule(item: Value, rule: &'static str) -> Gen {
    Box::new(move |max| {
        with_rule(
            with_max(
                json!({"type": "object", "additionalProperties": item.clone()}),
                "maxProperties",
                max,
            ),
            rule,
        )
    })
}

fn map_with_rule(t: &'static str, rule: &'static str) -> Gen {
    map_with_custom_item_rule(schema_of(t), rule)
}

fn string_with_rule(rule: &'static str) -> Gen {
    Box::new(move |max| with_rule(with_max(json!({"type": "string"}), "maxLength", max), rule))
}

fn enum_with_rule(rule: &'static str, values: &'static [&'static str]) -> Gen {
    Box::new(move |max| {
        with_rule(
            with_max(json!({"type": "string", "enum": values}), "maxLength", max),
            rule,
        )
    })
}

fn bytes_with_rule(rule: &'static str) -> Gen {
    Box::new(move |max| {
        with_rule(
            with_max(
                json!({"type": "string", "format": "byte"}),
                "maxLength",
                max,
            ),
            rule,
        )
    })
}

fn nested_spec_with_rule(rule: &'static str) -> Gen {
    Box::new(move |max| {
        let item = with_max(json!({"type": "string"}), "maxLength", max);
        with_rule(
            with_max(
                json!({"type": "object", "additionalProperties": item}),
                "maxProperties",
                max,
            ),
            rule,
        )
    })
}

fn nested_object(max: Option<i64>, with_max_properties: bool) -> Value {
    let mut o = json!({
        "type": "object",
        "required": ["required"],
        "properties": {"required": {"type": "string"}, "optional": {"type": "string"}}
    });
    if with_max_properties {
        o = with_max(o, "maxProperties", max);
    }
    o
}

fn all_max_nested_spec_with_root_rule(rule: &'static str) -> Gen {
    Box::new(move |max| {
        let items = with_max(
            json!({"type": "object", "additionalProperties": nested_object(max, true)}),
            "maxProperties",
            max,
        );
        with_rule(
            with_max(json!({"type": "array", "items": items}), "maxItems", max),
            rule,
        )
    })
}

fn one_max_nested_spec_with_root_rule(rule: &'static str) -> Gen {
    Box::new(move |max| {
        let items = with_max(
            json!({"type": "object", "additionalProperties": nested_object(max, false)}),
            "maxProperties",
            max,
        );
        with_rule(json!({"type": "array", "items": items}), rule)
    })
}

struct Case {
    name: &'static str,
    gen: Gen,
    set_max_elements: i64,
    expected_calc_cost: u64,
    calc_cost_exceeds_limit: u64,
    expected_set_cost: u64,
}

fn case(
    name: &'static str,
    gen: Gen,
    expected_calc_cost: u64,
    set_max_elements: i64,
    expected_set_cost: u64,
) -> Case {
    Case {
        name,
        gen,
        set_max_elements,
        expected_calc_cost,
        calc_cost_exceeds_limit: 0,
        expected_set_cost,
    }
}

fn cases() -> Vec<Case> {
    let mut v = vec![
        case(
            "number array with all",
            array_with_rule("number", "self.all(x, true)"),
            4718591,
            10,
            32,
        ),
        case(
            "string array with all",
            array_with_rule("string", "self.all(x, true)"),
            3145727,
            20,
            62,
        ),
        case(
            "boolean array with all",
            array_with_rule("boolean", "self.all(x, true)"),
            1887437,
            5,
            17,
        ),
        case(
            "array of number arrays with all",
            array_of_arrays_with_rule("number", "self.all(x, true)"),
            3145727,
            100,
            302,
        ),
        case(
            "array of objects with all",
            object_array_with_rule("self.all(x, true)"),
            555128,
            50,
            152,
        ),
        case(
            "map of numbers with all",
            map_with_rule("number", "self.all(x, true)"),
            1348169,
            10,
            32,
        ),
        case(
            "map of numbers with has",
            map_with_rule("number", "has(self.x)"),
            0,
            100,
            0,
        ),
        case(
            "map of strings with all",
            map_with_rule("string", "self.all(x, true)"),
            1179647,
            3,
            11,
        ),
        case(
            "map of strings with has",
            map_with_rule("string", "has(self.x)"),
            0,
            550,
            0,
        ),
        case(
            "map of booleans with all",
            map_with_rule("boolean", "self.all(x, true)"),
            943718,
            100,
            302,
        ),
        case(
            "map of booleans with has",
            map_with_rule("boolean", "has(self.x)"),
            0,
            1024,
            0,
        ),
        case(
            "string with contains",
            string_with_rule("self.contains('test')"),
            314574,
            10,
            5,
        ),
        case(
            "string with startsWith",
            string_with_rule("self.startsWith('test')"),
            2,
            15,
            2,
        ),
        case(
            "string with endsWith",
            string_with_rule("self.endsWith('test')"),
            2,
            30,
            2,
        ),
        case(
            "concat string",
            string_with_rule(r#"size(self + "hello") > size("hello")"#),
            314578,
            4,
            7,
        ),
        case(
            "index of array with numbers",
            array_with_rule("number", "self[1] == 0.0"),
            2,
            5000,
            2,
        ),
        case(
            "index of array with strings",
            array_with_rule("string", "self[1] == self[1]"),
            314577,
            8,
            314577,
        ),
        case(
            "regex matches simple",
            string_with_rule(r#"self.matches("x")"#),
            314574,
            50,
            22,
        ),
        case(
            "regex matches empty string",
            string_with_rule(r#""".matches("(((((((((())))))))))[0-9]")"#),
            7,
            10,
            7,
        ),
        case(
            "regex matches empty regex",
            string_with_rule(r#"self.matches("")"#),
            1,
            100,
            1,
        ),
        case(
            "map of strings with value length",
            nested_spec_with_rule("self.all(x, x.contains(self[x]))"),
            2752507,
            10,
            72,
        ),
        case(
            "set array maxLength to zero",
            array_with_rule("number", "self[3] == 0.0"),
            2,
            0,
            2,
        ),
        case(
            "set map maxLength to zero",
            map_with_rule("number", r#"self["x"] == 0.0"#),
            2,
            0,
            2,
        ),
        case(
            "set string maxLength to zero",
            string_with_rule(r#"self == "x""#),
            2,
            0,
            1,
        ),
        case(
            "set bytes maxLength to zero",
            bytes_with_rule(r#"self == b"x""#),
            2,
            0,
            1,
        ),
        case(
            "set maxLength greater than estimated maxLength",
            array_with_rule("number", "self.all(x, x == 0.0)"),
            6291454,
            3 * 1024 * 2048,
            25165826,
        ),
        case(
            "nested types with root rule with all supporting maxLength",
            all_max_nested_spec_with_root_rule(r#"self.all(x, x["y"].required == "z")"#),
            7340027,
            10,
            72,
        ),
        case(
            "nested types with root rule with one supporting maxLength",
            one_max_nested_spec_with_root_rule(r#"self.all(x, x["y"].required == "z")"#),
            7340027,
            10,
            7340027,
        ),
        case(
            "int-or-string array with all",
            array_with_rule("int-or-string", "self.all(x, true)"),
            4718591,
            10,
            32,
        ),
        case(
            "index of array with int-or-strings",
            array_with_rule("int-or-string", "self[0] == 5"),
            3,
            10,
            3,
        ),
        case(
            "index of array with booleans",
            array_with_rule("boolean", "self[0] == false"),
            2,
            25,
            2,
        ),
        case(
            "index of array of objects",
            object_array_with_rule("self[0] == null"),
            2,
            422,
            2,
        ),
        case(
            "index of array of array of numnbers",
            array_of_arrays_with_rule("number", "self[0][0] == -1.0"),
            3,
            51,
            3,
        ),
        case(
            "array of number maps with all",
            map_array_with_rule("number", "self.all(x, x.y == 25.2)"),
            6291452,
            12,
            74,
        ),
        case(
            "index of array of number maps",
            map_array_with_rule("number", "self[0].x > 2.0"),
            4,
            3000,
            4,
        ),
        case(
            "duration array with all",
            array_with_rule("duration", "self.all(x, true)"),
            2359295,
            5,
            17,
        ),
        case(
            "index of duration array",
            array_with_rule("duration", "self[0].getHours() == 2"),
            4,
            525,
            4,
        ),
        case(
            "date array with all",
            array_with_rule("date", "self.all(x, true)"),
            725936,
            15,
            47,
        ),
        case(
            "index of date array",
            array_with_rule("date", "self[2].getDayOfMonth() == 13"),
            4,
            42,
            4,
        ),
        case(
            "date-time array with all",
            array_with_rule("date-time", "self.all(x, true)"),
            428963,
            25,
            77,
        ),
        case(
            "index of date-time array",
            array_with_rule("date-time", "self[2].getMinutes() == 45"),
            4,
            99,
            4,
        ),
        case(
            "map of int-or-strings with all",
            map_with_rule("int-or-string", "self.all(x, true)"),
            1348169,
            15,
            47,
        ),
        case(
            "map of int-or-strings with has",
            map_with_rule("int-or-string", "has(self.x)"),
            0,
            5000,
            0,
        ),
        case(
            "map of objects with all",
            map_with_custom_item_rule(object_for_map(), "self.all(x, true)"),
            428963,
            20,
            62,
        ),
        case(
            "map of objects with has",
            map_with_custom_item_rule(object_for_map(), "has(self.x)"),
            0,
            9001,
            0,
        ),
        case(
            "map of number maps with all",
            map_with_custom_item_rule(
                json!({"type": "object", "additionalProperties": {"type": "number"}}),
                "self.all(x, true)",
            ),
            1179647,
            10,
            32,
        ),
        case(
            "map of number maps with has",
            map_with_custom_item_rule(
                json!({"type": "object", "additionalProperties": {"type": "number"}}),
                "has(self.x)",
            ),
            0,
            101,
            0,
        ),
        case(
            "map of number arrays with all",
            map_with_custom_item_rule(
                json!({"type": "array", "items": {"type": "number"}}),
                "self.all(x, true)",
            ),
            1179647,
            25,
            77,
        ),
        case(
            "map of number arrays with has",
            map_with_custom_item_rule(
                json!({"type": "array", "items": {"type": "number"}}),
                "has(self.x)",
            ),
            0,
            40000,
            0,
        ),
        case(
            "map of durations with all",
            map_with_rule("duration", "self.all(x, true)"),
            1048577,
            5,
            17,
        ),
        case(
            "map of durations with has",
            map_with_rule("duration", "has(self.x)"),
            0,
            256,
            0,
        ),
        case(
            "map of dates with all",
            map_with_rule("date", "self.all(x, true)"),
            524288,
            10,
            32,
        ),
        case(
            "map of dates with has",
            map_with_rule("date", "has(self.x)"),
            0,
            65536,
            0,
        ),
        case(
            "map of date-times with all",
            map_with_rule("date-time", "self.all(x, true)"),
            349526,
            25,
            77,
        ),
        case(
            "map of date-times with has",
            map_with_rule("date-time", "has(self.x)"),
            0,
            490,
            0,
        ),
        // Library functions are integrated with the size estimates.
        case(
            "extended library regex find",
            string_with_rule("self.find('[0-9]+') == ''"),
            629147,
            10,
            11,
        ),
        case(
            "extended library join",
            Box::new(|max| {
                let s = with_max(json!({"type": "string"}), "maxLength", max);
                let a = with_max(json!({"type": "array", "items": s}), "maxItems", max);
                with_rule(a, "self.join(' ') == 'aa bb'")
            }),
            329853068905,
            10,
            43,
        ),
        case(
            "extended library isSorted",
            Box::new(|max| {
                let s = with_max(json!({"type": "string"}), "maxLength", max);
                let a = with_max(json!({"type": "array", "items": s}), "maxItems", max);
                with_rule(a, "self.isSorted() == true")
            }),
            329854432052,
            10,
            52,
        ),
        case(
            "extended library replace",
            Box::new(|max| {
                let s = with_max(json!({"type": "string"}), "maxLength", max);
                let o = json!({
                    "type": "object",
                    "properties": {
                        "str": s,
                        "before": {"type": "string", "maxLength": 2},
                        "after": {"type": "string", "maxLength": 4}
                    }
                });
                with_rule(
                    o,
                    "self.str.replace(self.before, self.after) == 'does not matter'",
                )
            }),
            // The cost is based on the result size of the replace() call.
            629154,
            4,
            12,
        ),
        case(
            "extended library split",
            Box::new(|max| {
                let s = with_max(json!({"type": "string"}), "maxLength", max);
                let o = json!({
                    "type": "object",
                    "properties": {"str": s.clone(), "separator": s}
                });
                with_rule(o, "self.str.split(self.separator) == []")
            }),
            629160,
            10,
            22,
        ),
        case(
            "extended library lowerAscii",
            Box::new(|max| {
                let s = with_max(json!({"type": "string"}), "maxLength", max);
                with_rule(s, "self.lowerAscii() == 'lower!'")
            }),
            314575,
            10,
            6,
        ),
        case(
            "check cost of size call",
            map_with_rule("integer", "oldSelf.size() == self.size()"),
            5,
            10,
            5,
        ),
        case(
            "check cost of timestamp comparison",
            map_with_rule("date-time", r#"self["a"] == self["b"]"#),
            8,
            7,
            8,
        ),
        case(
            "check cost of duration comparison",
            map_with_rule("duration", r#"self["c"] == self["d"]"#),
            8,
            42,
            8,
        ),
        case(
            "enums with maxLength equals to the longest possible value",
            enum_with_rule("self.contains('A')", &["A", "B", "C", "LongValue"]),
            2,
            1000,
            401,
        ),
        case(
            "IntOrString type with quantity rule",
            Box::new(|max| {
                let s = with_max(
                    json!({"x-kubernetes-int-or-string": true}),
                    "maxLength",
                    max,
                );
                with_rule(s, "isQuantity(self)")
            }),
            314574,
            20,
            9,
        ),
    ];
    // Cases that only expect the unbounded cost to exceed the limit.
    for (name, rule, set_cost) in [
        (
            "O(n^2) loop with numbers",
            "self.all(x, self.all(y, true))",
            352,
        ),
        (
            "O(n^3) loop with numbers",
            "self.all(x, self.all(y, self.all(z, true)))",
            3552,
        ),
    ] {
        v.push(Case {
            name,
            gen: array_with_rule("number", rule),
            set_max_elements: 10,
            expected_calc_cost: 0,
            calc_cost_exceeds_limit: COST_LIMIT,
            expected_set_cost: set_cost,
        });
    }
    v
}

fn check(name: &str, which: &str, schema: &Value, expected: u64, exceeds: u64) {
    let got = max_cost(schema);
    if expected > 0 {
        assert_eq!(got, expected, "{name} ({which} maxLength)");
    }
    if exceeds > 0 {
        assert!(
            got >= exceeds,
            "{name} ({which} maxLength): cost did not exceed the limit as expected \
             (expected more than {exceeds}, got {got})"
        );
    }
}

/// `TestCostEstimation`.
#[test]
fn cost_estimation() {
    for c in cases() {
        // Dynamic maxLength: the estimates come from the request size.
        check(
            c.name,
            "calc",
            &(c.gen)(None),
            c.expected_calc_cost,
            c.calc_cost_exceeds_limit,
        );
        // Static maxLength.
        check(
            c.name,
            "set",
            &(c.gen)(Some(c.set_max_elements)),
            c.expected_set_cost,
            0,
        );
    }
}

fn estimator_for(schema: Value) -> SchemaSizeEstimator {
    let s: JSONSchemaProps = serde_json::from_value(schema).unwrap();
    SchemaSizeEstimator::new(schema_decl_type(&s, false).unwrap())
}

fn node(path: &[&str]) -> AstNode<'static> {
    AstNode {
        path: path.iter().map(|s| s.to_string()).collect(),
        ty: rusternetes_cel_checker::DType::Dyn,
        expr: None,
        computed_size: None,
    }
}

/// `sizeEstimator.EstimateSize`: the path is walked through `@items`,
/// `@values`, `@keys` and field names, starting after the variable.
#[test]
fn size_estimator_walks_the_path() {
    let e = estimator_for(json!({
        "type": "object",
        "properties": {
            "tags": {"type": "array", "maxItems": 7, "items": {"type": "string", "maxLength": 3}},
            "labels": {"type": "object", "maxProperties": 5,
                       "additionalProperties": {"type": "string", "maxLength": 2}}
        }
    }));
    let max = |p: &[&str]| e.estimate_size(&node(p)).map(|s| s.max);
    assert_eq!(max(&[]), None);
    assert_eq!(max(&["self", "tags"]), Some(7));
    assert_eq!(max(&["self", "tags", "@items"]), Some(12));
    assert_eq!(max(&["self", "labels"]), Some(5));
    assert_eq!(max(&["self", "labels", "@values"]), Some(8));
    // The key type is `string`, with no size of its own.
    assert_eq!(max(&["self", "labels", "@keys"]), Some(0));
    assert_eq!(max(&["self", "missing"]), None);
    assert_eq!(max(&["self", "tags", "@values"]), Some(12));
    assert_eq!(max(&["self", "tags", "@items", "x"]), None);
    assert_eq!(
        e.estimate_size(&node(&["self"])),
        Some(SizeEstimate { min: 0, max: 0 })
    );
}

/// `maxCardinality`: `MaxRequestSizeBytes / (minSize + 1)`.
#[test]
fn cardinality_is_bounded_by_the_request_size() {
    assert_eq!(
        max_cardinality(2),
        (limits::MAX_REQUEST_SIZE_BYTES / 3) as u64
    );
    assert_eq!(max_cardinality(0), limits::MAX_REQUEST_SIZE_BYTES as u64);
}
