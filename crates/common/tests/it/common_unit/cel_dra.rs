//! Unit tests moved from `src/cel_dra.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use cel::common::ast::{operators, EntryExpr, Expr, IdedExpr, LiteralValue};
    pub use rusternetes_common::cel_dra_decls::{self, DType};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::cel_dra::*;

    fn invalid(x: &str) -> Option<String> {
        match compile_selector(x) {
            SelectorCompilation::Invalid(d) => Some(d),
            SelectorCompilation::Ok { .. } => None,
        }
    }

    fn cost(x: &str) -> u64 {
        match compile_selector(x) {
            SelectorCompilation::Ok { max_cost } => max_cost,
            SelectorCompilation::Invalid(d) => panic!("{x}: {d}"),
        }
    }

    /// `ext/bindings.go` `celBind`: a non-identifier variable is a macro
    /// error, not an ordinary call.
    #[test]
    fn cel_bind_non_identifier_variable_is_a_compile_error() {
        let d = invalid("cel.bind(1, device.driver, true)").unwrap();
        assert_eq!(
            d,
            "compilation failed: ERROR: <input>:1:10: cel.bind() variable names must be simple identifiers\n | cel.bind(1, device.driver, true)\n | .........^"
        );
    }

    /// `celBind` reports at `varIdent.ID()` (ext/bindings.go:178): the ID of
    /// an operator-led expression is its operator token (parser.go:469-760:
    /// `.` of a select, `(` of a call, `[` of an index, the binary operator,
    /// `?` of a conditional, the middle `||`/`&&` of a balanced chain).
    #[test]
    fn cel_bind_non_identifier_error_column_is_the_expression_id_offset() {
        let col = |arg: &str| -> usize {
            let d = invalid(&format!("cel.bind({arg}, 1, true)")).unwrap();
            let rest = d
                .split("<input>:1:")
                .nth(1)
                .unwrap_or_else(|| panic!("{d}"));
            rest.split(':').next().unwrap().parse().unwrap()
        };
        // arg starts at column 10.
        for (arg, want) in [
            ("1", 10),
            ("-1", 10),
            ("a.b", 11),
            ("f(x)", 11),
            ("x.size()", 16),
            ("a + b", 12),
            ("a * b + c", 16),
            ("a < b", 12),
            ("a in b", 12),
            ("a[0]", 11),
            ("!a", 10),
            ("-a", 10),
            ("[1]", 10),
            ("a ? b : c", 12),
            ("a || b || c", 17),
            ("a && b", 12),
            ("(a.b)", 12),
            ("{1: 2}", 10),
            ("1.5 + 2e-3", 14),
            ("!!a", 12),
            ("a.b.c", 13),
            ("a.f(1, 2)", 13),
            ("\"s,\".size()", 19),
        ] {
            assert_eq!(col(arg), want, "{arg}");
        }
    }

    #[test]
    fn index_key_type_mismatch_reports_position() {
        let d = invalid("device.attributes[true].someBoolean").unwrap();
        assert_eq!(
            d,
            "compilation failed: ERROR: <input>:1:18: found no matching overload for '_[_]' applied to '(map(string, map(string, any)), bool)'\n | device.attributes[true].someBoolean\n | .................^"
        );
    }

    #[test]
    fn position_skips_literals_strings_and_floats() {
        // The list literal's `[`, the string's `[` and the float's `.` are not
        // operators; the failing index is the second one.
        let d =
            invalid("1.5 > 0.5 && ['['].exists(x, x == '[') && device.capacity[1].a == 1").unwrap();
        assert!(d.contains("<input>:1:"), "{d}");
        assert!(
            d.contains("'(map(string, map(string, apiserver.cel.Quantity)), int)'"),
            "{d}"
        );
        assert!(d.ends_with("^"), "{d}");
    }

    /// `checker.go:116` `checkIdent` -> `errors.go:83`.
    #[test]
    fn undeclared_identifier_is_a_compile_error() {
        assert_eq!(
            invalid("foo == 1").unwrap(),
            "compilation failed: ERROR: <input>:1:1: undeclared reference to 'foo' (in container '')\n | foo == 1\n | ^"
        );
        let d = invalid("device.driver == 'a' && other.x").unwrap();
        assert!(
            d.contains("<input>:1:25: undeclared reference to 'other' (in container '')"),
            "{d}"
        );
    }

    #[test]
    fn declared_identifiers_are_accepted() {
        // device, comprehension variables, cel.bind variables, std type idents.
        assert!(invalid("device.attributes['d'].exists(k, k == 'x')").is_none());
        assert!(invalid("[1].all(x, x > 0) && device.driver == 'a'").is_none());
        assert!(invalid("cel.bind(a, device.driver, a == 'x')").is_none());
        assert!(invalid("type(1) == int && type('a') == string").is_none());
        assert!(invalid("device.driver == 'foo' && 'x'.size() == 1").is_none());
    }

    #[test]
    fn undefined_device_field() {
        let d = invalid("device.nope == 1").unwrap();
        assert!(d.contains("<input>:1:7: undefined field 'nope'"), "{d}");
    }

    #[test]
    fn output_type_must_be_bool_or_any() {
        assert_eq!(
            invalid("device.driver").unwrap(),
            "must evaluate to bool or the unknown type, not string"
        );
        assert!(invalid("device.attributes['d'].x").is_none());
        assert!(invalid("size(device.driver) > 1").is_none());
    }

    #[test]
    fn list_index_needs_int() {
        assert!(invalid("['a'][0] == 'a'").is_none());
        assert!(invalid("['a']['x'] == 'a'")
            .unwrap()
            .contains("'(list(string), string)'"));
    }

    /// `CEL-cost` (validation_resourceclaim_test.go:610).
    #[test]
    fn nested_comprehensions_exceed_the_limit() {
        let x = "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z5, int('1'.find('[0-9]*')) < 100)))))))";
        assert!(cost(x) > CEL_SELECTOR_EXPRESSION_MAX_COST);
    }

    #[test]
    fn ordinary_selectors_are_cheap() {
        for x in [
            "device.driver == 'dra.example.com'",
            "device.attributes['d'].model == 'a'",
            "device.capacity['d'].memory.compareTo(quantity('1Gi')) >= 0",
            "device.attributes['d'].x in ['a', 'b', 'c']",
            "device.attributes['d'].x.startsWith('a') && device.attributes['d'].y.contains('b')",
        ] {
            let c = cost(x);
            assert!(c > 0 && c < CEL_SELECTOR_EXPRESSION_MAX_COST, "{x}: {c}");
        }
    }

    /// `device.driver == "x"`: select(device)=1 + ident(device)=1 + const 0 +
    /// equals: min(63, 1)*0.1 rounded up = 1 -> 3.
    #[test]
    fn simple_equality_cost_matches_cel_go() {
        assert_eq!(cost("device.driver == 'x'"), 3);
    }

    #[test]
    fn comprehension_over_env_map_is_bounded_by_its_size() {
        // device.attributes has at most 32 domains.
        let c = cost("device.attributes.all(d, d == 'x')");
        assert!(c < CEL_SELECTOR_EXPRESSION_MAX_COST, "{c}");
    }

    /// `compile_test.go` `expectCost` table (`TestCEL`,
    /// `staging/src/k8s.io/dynamic-resource-allocation/cel/compile_test.go:56-331`),
    /// the compile-time estimate only.
    #[test]
    fn upstream_compile_test_expect_cost_table() {
        let a = r#"device.attributes["dra.example.com"]"#;
        let c = r#"device.capacity["dra.example.com"]"#;
        let cases: Vec<(String, u64)> = vec![
            (r#"device.attributes["no-such-domain"].noSuchAttr.isGreaterThan(quantity("0"))"#.into(), 6),
            (r#""no-such-domain" in device.attributes"#.into(), 3),
            (r#"device.driver == """#.into(), 2),
            (r#"device.driver == "dra.example.com""#.into(), 4),
            (format!("{a}.name"), 4),
            (format!(r#"{a}["name"]"#), 4),
            (r#"cel.bind(dra, device.attributes["dra.example.com"], dra.name)"#.into(), 15),
            (format!("{a}.name > 0"), 5),
            (format!(r#"{a}.name == "fish""#), 5),
            (format!(r#"{a}.name.isGreaterThan(semver("0.0.1"))"#), 6),
            (format!(r#"{a}.name.isGreaterThan(semver("v0.0.1", true)) && isSemver("v1.0.0", true)"#), 7),
            (format!(r#"{c}.name.isGreaterThan(quantity("1Ki"))"#), 6),
            (format!(r#""name" in {c} && {c}.name.isGreaterThan(quantity("1Ki"))"#), 10),
            (format!(r#"!("name" in {c}) || {c}.name.isGreaterThan(quantity("1Ki"))"#), 11),
            (format!("{a}.a && {a}.b && {a}.c"), 12),
            (r#"device.attributes.exists_one(x, x == "dra.example.com")"#.into(), 164),
            (format!(r#"{a}.exists_one(x, x == "services")"#), 133),
            (format!(r#"{a}.services.split("example").size() >= 2"#), 19),
            (format!(r#"{a}.services.matches("[^a]?sym")"#), 18),
            (r#"device.capacity.exists_one(x, x == "dra.example.com")"#.into(), 164),
            (format!(r#"{c}.exists_one(x, x == "memory")"#), 133),
            (
                "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(x, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(y, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].all(z5, int('1'.find('[0-9]*')) < 100)))))))".into(),
                85555551,
            ),
            ("device.allowMultipleAllocations == true".into(), 3),
        ];
        let mut bad = Vec::new();
        for (x, want) in &cases {
            let got = match compile_selector(x) {
                SelectorCompilation::Ok { max_cost } => max_cost,
                SelectorCompilation::Invalid(d) => panic!("{x}: {d}"),
            };
            if got != *want {
                bad.push(format!("{x}: want {want}, got {got}"));
            }
        }
        assert!(bad.is_empty(), "{}", bad.join("\n"));
    }

    /// `_==_` on `Quantity` is O(1) (`cost.go:359-361`): two `quantity('x')`
    /// at 1 each plus 1, not a string-size product.
    #[test]
    fn quantity_equality_is_constant_cost() {
        assert_eq!(cost("quantity('1') == quantity('2')"), 3);
    }

    /// `checker.go:261-348` `resolveOverload`: every declared library
    /// function of the base environment accepts its documented arguments.
    #[test]
    fn declared_library_calls_are_accepted() {
        for x in [
            r#"device.driver.startsWith("a") && device.attributes["d"].x.isGreaterThan(semver("1.0.0"))"#,
            r#"device.capacity["d"].memory.compareTo(quantity("1Gi")) >= 0"#,
            r#"sets.contains([1], [1]) && sets.intersects(["a"], ["a"])"#,
            r#"strings.quote("a") == "\"a\"""#,
            "lists.range(3).size() == 3",
            r#"format.named("dns1123Label").hasValue()"#,
            r#"format.dns1123Label().validate("a").hasValue()"#,
            r#""%d".format([1]) == "1""#,
            "[1, 2, 3].sum() == 6",
            "[1, 2, 3].map(x, x + 1).size() == 3",
            r#"ip("1.2.3.4").family() == 4 && cidr("10.0.0.0/8").containsIP("10.1.1.1")"#,
            r#"string(ip("1.2.3.4")) == "x" && isIP("1.2.3.4") && ip.isCanonical("1.2.3.4")"#,
            "size(device.driver) > 0 && device.driver.size() == 1",
            r#"duration("1h") + timestamp("2020-01-01T00:00:00Z") > timestamp("2021-01-01T00:00:00Z")"#,
            "1 < 2.0 && 1u < 2 && 2.5 >= 1u",
            r#""a" in ["a"] && 1 in {1: "a"}"#,
            r#"device.driver.matches("a") && matches(device.driver, "a")"#,
            r#"device.driver.split(",").join("-") == "a""#,
            r#"device.driver.indexOf("a") == 0 && [1, 2].indexOf(2) == 1"#,
            r#"device.driver.find("a") == "a" && device.driver.findAll("a").size() == 1"#,
            r#"device.driver.replace("a", "b", 1).substring(1, 2).trim().lowerAscii().upperAscii().charAt(0) == "a""#,
            r#"optional.of(1).hasValue() && optional.none().orValue(2) == 2"#,
            r#"[1, 2].first().hasValue() && ["a"].last().value() == "a""#,
            r#"cel.bind(a, device.attributes["d"], a.x == 1)"#,
            r#"device.driver == "a" ? true : false"#,
            r#"isSemver("v1.0.0", true) && semver("1.2.3").major() == 1"#,
            r#"[1, 2].exists(x, x > 1) && [1, 2].all(x, x > 0) && [1, 2].filter(x, x > 1).size() == 1"#,
            r#"!(device.allowMultipleAllocations) && -1 < 0 && 5 % 3 == 2 && 6 / 3 == 2 && 2 * 3 == 6 && 3 - 1 == 2"#,
            r#"[[1], [2]].flatten().size() == 2 && [2, 1].sort() == [1, 2] && [1, 1].distinct() == [1] && [1, 2].reverse() == [2, 1] && [1, 2].slice(0, 1) == [1]"#,
            r#"url("http://a").getHost() == "a" && isURL("http://a")"#,
            r#"bytes("a") + b"b" == b"ab" && double(1) > 0.5 && int("1") == 1 && uint(1) == 1u && bool("true")"#,
        ] {
            assert_eq!(invalid(x), None, "{x}");
        }
    }

    #[test]
    fn no_matching_overload_for_an_operator() {
        assert_eq!(
            invalid(r#"1 + "x" == 2"#).unwrap(),
            "compilation failed: ERROR: <input>:1:3: found no matching overload for '_+_' applied to '(int, string)'\n | 1 + \"x\" == 2\n | ..^"
        );
        assert_eq!(
            invalid(r#"device.driver == 1"#).unwrap(),
            "compilation failed: ERROR: <input>:1:15: found no matching overload for '_==_' applied to '(string, int)'\n | device.driver == 1\n | ..............^"
        );
        // A type parameter binds once: `int == double` has no overload.
        assert!(invalid("1 == 1.0")
            .unwrap()
            .contains("'_==_' applied to '(int, double)'"));
    }

    #[test]
    fn no_matching_overload_for_a_member_call() {
        assert_eq!(
            invalid("device.driver.startsWith(1)").unwrap(),
            "compilation failed: ERROR: <input>:1:25: found no matching overload for 'startsWith' applied to 'string.(int)'\n | device.driver.startsWith(1)\n | ........................^"
        );
        // checker.go:309-322: the receiver style must match the overload.
        assert!(invalid(r#"startsWith(device.driver, "a")"#)
            .unwrap()
            .contains("'startsWith' applied to '(string, string)'"));
    }

    #[test]
    fn no_matching_overload_for_a_global_call() {
        assert_eq!(
            invalid("size(1) == 1").unwrap(),
            "compilation failed: ERROR: <input>:1:5: found no matching overload for 'size' applied to '(int)'\n | size(1) == 1\n | ....^"
        );
    }

    /// checker.go:309-322.
    #[test]
    fn logical_operators_need_bool_arguments() {
        assert_eq!(
            invalid("device.driver && true").unwrap(),
            "compilation failed: ERROR: <input>:1:7: expected type 'bool' but found 'string'\n | device.driver && true\n | ......^"
        );
    }

    /// checker.go:249-290: an unknown namespace is reported as the
    /// undeclared receiver and the undeclared function, as upstream does for
    /// `math` and `base64`, which the base environment does not declare.
    #[test]
    fn undeclared_namespaced_function_reports_receiver_and_function() {
        assert_eq!(
            invalid("math.greatest(1, 2) > 0").unwrap(),
            "compilation failed: ERROR: <input>:1:1: undeclared reference to 'math' (in container '')\n | math.greatest(1, 2) > 0\n | ^\nERROR: <input>:1:14: undeclared reference to 'greatest' (in container '')\n | math.greatest(1, 2) > 0\n | .............^"
        );
        assert!(invalid(r#"base64.encode(b"a") == "a""#)
            .unwrap()
            .contains("undeclared reference to 'base64'"));
    }

    #[test]
    fn undeclared_member_function_is_reported() {
        assert_eq!(
            invalid("device.driver.nope()").unwrap(),
            "compilation failed: ERROR: <input>:1:19: undeclared reference to 'nope' (in container '')\n | device.driver.nope()\n | ..................^"
        );
    }

    #[test]
    fn conditional_branches_must_agree() {
        assert!(invalid(r#"true ? 1 : "a""#)
            .unwrap()
            .contains("'_?_:_' applied to '(bool, int, string)'"));
    }

    /// Expression offsets come from the parser's own `SourceInfo`, also for
    /// non-ASCII source.
    #[test]
    fn error_offsets_count_characters() {
        let d = invalid(r#""é" + 1 == 2"#).unwrap();
        assert!(d.contains("<input>:1:5:"), "{d}");
    }
}
