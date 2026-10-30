//! Unit tests moved from `src/label_selector.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::validation::field::{BadValue, Error as FieldError, Path};
    pub use rusternetes_common::validation::metav1::{is_qualified_name, is_valid_label_value};
    pub use serde_json::Value;
    pub use std::collections::{BTreeSet, HashMap};
    pub use std::fmt;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::label_selector::*;
    use serde_json::json;

    #[test]
    fn test_display_does_not_panic_on_missing_values() {
        // A LabelRequirement built without values (e.g. via a code path that
        // bypasses the parser) used to panic in the Display impl. Now it
        // renders as an empty value string.
        for op in [
            LabelOperator::Equals,
            LabelOperator::NotEquals,
            LabelOperator::In,
            LabelOperator::NotIn,
        ] {
            let req = LabelRequirement::new_for_test("app", op, None);
            let sel = LabelSelector {
                requirements: vec![req],
            };
            let _ = format!("{}", sel); // must not panic
        }
    }

    #[test]
    fn test_parse_equality_simple() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "app");
        assert_eq!(selector.requirements[0].operator, LabelOperator::Equals);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["nginx".to_string()])
        );
    }

    #[test]
    fn test_parse_equality_double_equals() {
        let selector = LabelSelector::parse("app==nginx").unwrap();
        assert_eq!(
            selector.requirements[0].operator,
            LabelOperator::DoubleEquals
        );
    }

    #[test]
    fn test_parse_not_equals() {
        let selector = LabelSelector::parse("tier!=backend").unwrap();
        assert_eq!(selector.requirements[0].operator, LabelOperator::NotEquals);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["backend".to_string()])
        );
    }

    #[test]
    fn test_parse_multiple_equality() {
        let selector = LabelSelector::parse("app=nginx,tier=frontend").unwrap();
        assert_eq!(selector.requirements.len(), 2);
        assert_eq!(selector.requirements[0].key, "app");
        assert_eq!(selector.requirements[1].key, "tier");
    }

    #[test]
    fn test_parse_in_operator() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "environment");
        assert_eq!(selector.requirements[0].operator, LabelOperator::In);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["production".to_string(), "qa".to_string()])
        );
    }

    #[test]
    fn test_parse_notin_operator() {
        let selector = LabelSelector::parse("tier notin (frontend,backend)").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].operator, LabelOperator::NotIn);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["backend".to_string(), "frontend".to_string()])
        );
    }

    #[test]
    fn test_parse_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "hasFeature");
        assert_eq!(selector.requirements[0].operator, LabelOperator::Exists);
        assert_eq!(selector.requirements[0].values, None);
    }

    #[test]
    fn test_parse_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "deprecated");
        assert_eq!(
            selector.requirements[0].operator,
            LabelOperator::DoesNotExist
        );
    }

    #[test]
    fn test_parse_mixed() {
        let selector =
            LabelSelector::parse("app=nginx,environment in (production, qa),tier!=backend")
                .unwrap();
        assert_eq!(selector.requirements.len(), 3);
        assert_eq!(selector.requirements[0].operator, LabelOperator::Equals);
        assert_eq!(selector.requirements[1].operator, LabelOperator::In);
        assert_eq!(selector.requirements[2].operator, LabelOperator::NotEquals);
    }

    #[test]
    fn test_matches_equality() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_equality() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "apache".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_not_equals() {
        let selector = LabelSelector::parse("tier!=backend").unwrap();
        let mut labels = HashMap::new();
        labels.insert("tier".to_string(), "frontend".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_matches_in() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        let mut labels = HashMap::new();
        labels.insert("environment".to_string(), "production".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_in() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        let mut labels = HashMap::new();
        labels.insert("environment".to_string(), "staging".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_notin() {
        let selector = LabelSelector::parse("tier notin (frontend, backend)").unwrap();
        let mut labels = HashMap::new();
        labels.insert("tier".to_string(), "middleware".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_matches_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        let mut labels = HashMap::new();
        labels.insert("hasFeature".to_string(), "true".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        let labels = HashMap::new();

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        let labels = HashMap::new();

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        let mut labels = HashMap::new();
        labels.insert("deprecated".to_string(), "true".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_multiple_all_match() {
        let selector = LabelSelector::parse("app=nginx,tier=frontend").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_matches_multiple_one_fails() {
        let selector = LabelSelector::parse("app=nginx,tier=frontend").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("tier".to_string(), "backend".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_resource() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        let resource = json!({
            "metadata": {
                "labels": {
                    "app": "nginx",
                    "tier": "frontend"
                }
            }
        });

        assert!(selector.matches_resource(&resource));
    }

    #[test]
    fn test_empty_selector_matches_all() {
        let selector = LabelSelector::parse("").unwrap();
        let labels = HashMap::new();

        assert!(selector.matches(&labels));
        assert!(selector.is_empty());
    }

    #[test]
    fn test_display_equality() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        assert_eq!(format!("{}", selector), "app=nginx");
    }

    #[test]
    fn test_display_in() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        assert_eq!(format!("{}", selector), "environment in (production,qa)");
    }

    #[test]
    fn test_display_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        assert_eq!(format!("{}", selector), "hasFeature");
    }

    #[test]
    fn test_display_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        assert_eq!(format!("{}", selector), "!deprecated");
    }

    // ---- Ported from upstream selector_test.go (release-1.35) ----

    fn parse_err(s: &str) -> String {
        LabelSelector::parse(s).unwrap_err().to_string()
    }

    /// selector_test.go TestSelectorParse (:36-70).
    #[test]
    fn upstream_test_selector_parse() {
        for good in [
            "x=a,y=b,z=c",
            "",
            "x!=a,y=b",
            "x=",
            "x= ",
            "x=,z= ",
            "x= ,z= ",
            "!x",
            "x>1",
            "x>1,z<5",
        ] {
            let sel =
                LabelSelector::parse(good).unwrap_or_else(|e| panic!("{good:?} should parse: {e}"));
            assert_eq!(good.replace(' ', ""), sel.to_string(), "restring {good:?}");
        }
        for bad in ["x=a||y=b", "x==a==b", "!x=a", "x<a"] {
            assert!(LabelSelector::parse(bad).is_err(), "{bad:?} must fail");
        }
    }

    /// The issue's own examples (#2181).
    #[test]
    fn issue_2181_malformed_selectors_rejected() {
        assert_eq!(
            parse_err("!!!"),
            "unable to parse requirement: found '!', expected: identifier"
        );
        assert_eq!(
            parse_err("app in ("),
            "unable to parse requirement: found '', expected: ',', ')' or identifier"
        );
    }

    /// selector_test.go TestLexer (:193-225).
    #[test]
    fn upstream_test_lexer() {
        let cases = [
            ("", Token::EndOfString),
            (",", Token::Comma),
            ("notin", Token::NotIn),
            ("in", Token::In),
            ("=", Token::Equals),
            ("==", Token::DoubleEquals),
            (">", Token::GreaterThan),
            ("<", Token::LessThan),
            ("!", Token::DoesNotExist),
            ("!=", Token::NotEquals),
            ("(", Token::OpenPar),
            (")", Token::ClosedPar),
            ("~", Token::Identifier),
            ("||", Token::Identifier),
        ];
        for (s, t) in cases {
            let (tok, lit) = Lexer::new(s).lex();
            assert_eq!(tok, t, "token for {s:?}");
            assert_eq!(lit, s, "literal for {s:?}");
        }
    }

    /// selector_test.go TestLexerSequence (:227-261).
    #[test]
    fn upstream_test_lexer_sequence() {
        use Token::*;
        let cases: Vec<(&str, Vec<Token>)> = vec![
            (
                "key in ( value )",
                vec![Identifier, In, OpenPar, Identifier, ClosedPar],
            ),
            (
                "key notin ( value )",
                vec![Identifier, NotIn, OpenPar, Identifier, ClosedPar],
            ),
            (
                "key in ( value1, value2 )",
                vec![
                    Identifier, In, OpenPar, Identifier, Comma, Identifier, ClosedPar,
                ],
            ),
            ("key", vec![Identifier]),
            ("!key", vec![DoesNotExist, Identifier]),
            ("()", vec![OpenPar, ClosedPar]),
            (
                "x in (),y",
                vec![Identifier, In, OpenPar, ClosedPar, Comma, Identifier],
            ),
            (
                "== != (), = notin",
                vec![
                    DoubleEquals,
                    NotEquals,
                    OpenPar,
                    ClosedPar,
                    Comma,
                    Equals,
                    NotIn,
                ],
            ),
            ("key>2", vec![Identifier, GreaterThan, Identifier]),
        ];
        for (s, expected) in cases {
            let mut l = Lexer::new(s);
            let mut got = Vec::new();
            loop {
                let (tok, _) = l.lex();
                if tok == EndOfString {
                    break;
                }
                got.push(tok);
            }
            assert_eq!(got, expected, "sequence for {s:?}");
        }
    }

    type Want = (&'static str, LabelOperator, Option<Vec<&'static str>>);

    /// selector_test.go TestSetSelectorParser (:604-690).
    #[test]
    fn upstream_test_set_selector_parser() {
        use LabelOperator::*;
        let rows: Vec<(&str, bool, Vec<Want>)> = vec![
            ("", true, vec![]),
            ("\rx", true, vec![("x", Exists, None)]),
            (
                "this-is-a-dns.domain.com/key-with-dash",
                true,
                vec![("this-is-a-dns.domain.com/key-with-dash", Exists, None)],
            ),
            (
                "this-is-another-dns.domain.com/key-with-dash in (so,what)",
                true,
                vec![(
                    "this-is-another-dns.domain.com/key-with-dash",
                    In,
                    Some(vec!["so", "what"]),
                )],
            ),
            (
                "0.1.2.domain/99 notin (10.10.100.1, tick.tack.clock)",
                true,
                vec![(
                    "0.1.2.domain/99",
                    NotIn,
                    Some(vec!["10.10.100.1", "tick.tack.clock"]),
                )],
            ),
            (
                "foo  in\t (abc)",
                true,
                vec![("foo", In, Some(vec!["abc"]))],
            ),
            (
                "x notin\n (abc)",
                true,
                vec![("x", NotIn, Some(vec!["abc"]))],
            ),
            (
                "x  notin\t\t(abc,def)",
                true,
                vec![("x", NotIn, Some(vec!["abc", "def"]))],
            ),
            (
                "x in (abc,def)",
                true,
                vec![("x", In, Some(vec!["abc", "def"]))],
            ),
            ("x in (abc,)", true, vec![("x", In, Some(vec!["", "abc"]))]),
            ("x in (abc,abc)", true, vec![("x", In, Some(vec!["abc"]))]),
            ("x in ()", true, vec![("x", In, Some(vec![""]))]),
            ("x in (a,,)", true, vec![("x", In, Some(vec!["", "a"]))]),
            ("x in (a,,,)", true, vec![("x", In, Some(vec!["", "a"]))]),
            ("x in (a,,,,,,)", true, vec![("x", In, Some(vec!["", "a"]))]),
            (
                "x in (a,,a,,a,,a,,)",
                true,
                vec![("x", In, Some(vec!["", "a"]))],
            ),
            (
                "x notin (abc,,def),bar,z in (),w",
                true,
                vec![
                    ("bar", Exists, None),
                    ("w", Exists, None),
                    ("x", NotIn, Some(vec!["", "abc", "def"])),
                    ("z", In, Some(vec![""])),
                ],
            ),
            (
                "x,y in (a)",
                true,
                vec![("x", Exists, None), ("y", In, Some(vec!["a"]))],
            ),
            ("x=a", true, vec![("x", Equals, Some(vec!["a"]))]),
            ("x>1", true, vec![("x", GreaterThan, Some(vec!["1"]))]),
            ("x<7", true, vec![("x", LessThan, Some(vec!["7"]))]),
            (
                "x=a,y!=b",
                true,
                vec![
                    ("x", Equals, Some(vec!["a"])),
                    ("y", NotEquals, Some(vec!["b"])),
                ],
            ),
            (
                "x=a,y!=b,z in (h,i,j)",
                true,
                vec![
                    ("x", Equals, Some(vec!["a"])),
                    ("y", NotEquals, Some(vec!["b"])),
                    ("z", In, Some(vec!["h", "i", "j"])),
                ],
            ),
            ("x=a||y=b", false, vec![]),
            ("x,,y", false, vec![]),
            (",x,y", false, vec![]),
            ("x nott in (y)", false, vec![]),
            ("x notin ( )", true, vec![("x", NotIn, Some(vec![""]))]),
            (
                "x notin (, a)",
                true,
                vec![("x", NotIn, Some(vec!["", "a"]))],
            ),
            ("a in (xyz),", false, vec![]),
            ("a in (xyz)b notin ()", false, vec![]),
            ("a ", true, vec![("a", Exists, None)]),
            (
                "a in (x,y,notin, z,in)",
                true,
                vec![("a", In, Some(vec!["in", "notin", "x", "y", "z"]))],
            ),
            ("a in (xyz abc)", false, vec![]),
            ("a notin(", false, vec![]),
            ("a (", false, vec![]),
            ("(", false, vec![]),
        ];
        for (input, valid, expected) in rows {
            match LabelSelector::parse(input) {
                Err(e) => assert!(!valid, "Parse({input:?}) => {e}, expected no error"),
                Ok(sel) => {
                    assert!(valid, "Parse({input:?}) => {sel}, expected error");
                    let got: Vec<_> = sel
                        .requirements()
                        .iter()
                        .map(|r| {
                            (
                                r.key().to_string(),
                                r.operator(),
                                r.values().map(|v| v.to_vec()),
                            )
                        })
                        .collect();
                    let want: Vec<_> = expected
                        .into_iter()
                        .map(|(k, o, v)| {
                            (
                                k.to_string(),
                                o,
                                v.map(|v| v.into_iter().map(String::from).collect::<Vec<_>>()),
                            )
                        })
                        .collect();
                    assert_eq!(got, want, "Parse({input:?})");
                }
            }
        }
    }

    /// selector_test.go TestRequirementConstructor (:327-507).
    #[test]
    fn upstream_test_requirement_constructor() {
        use LabelOperator::*;
        let v = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let long = "a".repeat(254);
        let cases: Vec<(String, LabelOperator, Vec<String>, Option<&str>)> = vec![
            ("x1".into(), In, v(&[]), Some("values: Invalid value: []")),
            (
                "x2".into(),
                NotIn,
                v(&[]),
                Some("values: Invalid value: []"),
            ),
            ("x3".into(), In, v(&["foo"]), None),
            ("x4".into(), NotIn, v(&["foo"]), None),
            (
                "x5".into(),
                Equals,
                v(&["bar", "foo"]),
                Some("values: Invalid value: [\"bar\",\"foo\"]"),
            ),
            ("x6".into(), Exists, v(&[]), None),
            ("x7".into(), DoesNotExist, v(&[]), None),
            (
                "x8".into(),
                Exists,
                v(&["foo"]),
                Some("values: Invalid value: [\"foo\"]"),
            ),
            ("x11".into(), GreaterThan, v(&["1"]), None),
            ("x12".into(), LessThan, v(&["6"]), None),
            (
                "x13".into(),
                GreaterThan,
                v(&[]),
                Some("values: Invalid value: []"),
            ),
            (
                "x14".into(),
                GreaterThan,
                v(&["bar"]),
                Some("values[0]: Invalid value: \"bar\""),
            ),
            (
                "x15".into(),
                LessThan,
                v(&["bar"]),
                Some("values[0]: Invalid value: \"bar\""),
            ),
            (
                long.clone(),
                Exists,
                v(&[]),
                Some("key: Invalid value: \"aaaa"),
            ),
            (
                "x16".into(),
                Equals,
                vec![long.clone()],
                Some("values[0][x16]: Invalid value: \"aaaa"),
            ),
            (
                "x17".into(),
                Equals,
                v(&["a b"]),
                Some("values[0][x17]: Invalid value: \"a b\""),
            ),
        ];
        for (key, op, vals, want) in cases {
            let got = LabelRequirement::new(&key, op, vals);
            match want {
                None => assert!(got.is_ok(), "{key}: {got:?}"),
                Some(prefix) => {
                    let e = got.expect_err(&key);
                    assert!(
                        e.starts_with(prefix),
                        "{key}: got {e:?}, want prefix {prefix:?}"
                    );
                }
            }
        }
    }

    /// Requirement.Matches for Gt/Lt (selector.go:257-298).
    #[test]
    fn gt_lt_matching() {
        let sel = LabelSelector::parse("x>5,y<3").unwrap();
        let m = |x: &str, y: &str| {
            let mut l = HashMap::new();
            l.insert("x".to_string(), x.to_string());
            l.insert("y".to_string(), y.to_string());
            sel.matches(&l)
        };
        assert!(m("6", "2"));
        assert!(!m("5", "2"));
        assert!(!m("6", "3"));
        assert!(!m("abc", "2"));
    }

    /// Operator list in the parseOperator error (selector.go:39-43,794).
    #[test]
    fn parse_operator_error_lists_binary_operators() {
        assert_eq!(
            parse_err("x ("),
            "unable to parse requirement: found '(', expected: in, notin, =, ==, !=, gt, lt"
        );
    }

    /// Requirement.String forms: `==` kept, set values sorted.
    #[test]
    fn requirement_string_forms() {
        assert_eq!(LabelSelector::parse("x==a").unwrap().to_string(), "x==a");
        assert_eq!(
            LabelSelector::parse("x notin (b,a)").unwrap().to_string(),
            "x notin (a,b)"
        );
        assert_eq!(LabelSelector::parse("!x").unwrap().to_string(), "!x");
    }
}
