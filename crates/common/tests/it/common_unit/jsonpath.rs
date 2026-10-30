//! Unit tests moved from `src/jsonpath.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use regex::Regex;
    pub use std::fmt;
    pub use std::sync::OnceLock;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::jsonpath::*;

    fn text(s: &str) -> Node {
        Node::Text(s.into())
    }
    fn list() -> Node {
        Node::List(ListNode::default())
    }
    fn field(s: &str) -> Node {
        Node::Field(s.into())
    }
    fn pe(value: i64, known: bool, derived: bool) -> ParamsEntry {
        ParamsEntry {
            value,
            known,
            derived,
        }
    }
    fn filter(op: &str) -> Node {
        Node::Filter {
            left: ListNode::default(),
            right: ListNode::default(),
            operator: op.into(),
        }
    }

    /// `collectNode` (`parser_test.go:88-104`).
    fn collect(nodes: &mut Vec<Node>, cur: &Node) {
        nodes.push(cur.clone());
        match cur {
            Node::List(l) => l.nodes.iter().for_each(|n| collect(nodes, n)),
            Node::Filter { left, right, .. } => {
                collect(nodes, &Node::List(left.clone()));
                collect(nodes, &Node::List(right.clone()));
            }
            Node::Union(u) => u
                .iter()
                .for_each(|n| collect(nodes, &Node::List(n.clone()))),
            _ => {}
        }
    }

    /// `TestParser` (`parser_test.go:30-129`), every case.
    #[test]
    fn upstream_parser_cases() {
        let none = pe(0, false, false);
        let dq = "\"";
        #[allow(clippy::type_complexity)]
        let cases: Vec<(&str, String, Vec<Node>, bool)> = vec![
            (
                "plain",
                "hello jsonpath".into(),
                vec![text("hello jsonpath")],
                false,
            ),
            (
                "variable",
                "hello {.jsonpath}".into(),
                vec![text("hello "), list(), field("jsonpath")],
                false,
            ),
            (
                "arrayfiled",
                "hello {['jsonpath']}".into(),
                vec![text("hello "), list(), field("jsonpath")],
                false,
            ),
            ("quote", r#"{"{"}"#.into(), vec![list(), text("{")], false),
            (
                "array",
                "{[1:3]}".into(),
                vec![
                    list(),
                    Node::Array([pe(1, true, false), pe(3, true, false), none]),
                ],
                false,
            ),
            (
                "allarray",
                "{.book[*].author}".into(),
                vec![
                    list(),
                    field("book"),
                    Node::Array([none, none, none]),
                    field("author"),
                ],
                false,
            ),
            (
                "wildcard",
                "{.bicycle.*}".into(),
                vec![list(), field("bicycle"), Node::Wildcard],
                false,
            ),
            (
                "filter",
                "{[?(@.price<3)]}".into(),
                vec![
                    list(),
                    filter("<"),
                    list(),
                    field("price"),
                    list(),
                    Node::Int(3),
                ],
                false,
            ),
            (
                "recursive",
                "{..}".into(),
                vec![list(), Node::Recursive],
                false,
            ),
            (
                "recurField",
                "{..price}".into(),
                vec![list(), Node::Recursive, field("price")],
                false,
            ),
            (
                "arraydict",
                "{['book.price']}".into(),
                vec![list(), field("book"), field("price")],
                false,
            ),
            (
                "union",
                "{['bicycle.price', 3, 'book.price']}".into(),
                vec![
                    list(),
                    Node::Union(vec![]),
                    list(),
                    field("bicycle"),
                    field("price"),
                    list(),
                    Node::Array([pe(3, true, false), pe(4, true, true), none]),
                    list(),
                    field("book"),
                    field("price"),
                ],
                false,
            ),
            (
                "range",
                "{range .items}{.name},{end}".into(),
                vec![
                    list(),
                    Node::Identifier("range".into()),
                    field("items"),
                    list(),
                    field("name"),
                    text(","),
                    list(),
                    Node::Identifier("end".into()),
                ],
                false,
            ),
            ("malformat input", r"{\\\}".into(), vec![], true),
            (
                "paired parentheses in quotes",
                r#"{[?(@.status.nodeInfo.osImage == "()")]}"#.into(),
                vec![
                    list(),
                    filter("=="),
                    list(),
                    field("status"),
                    field("nodeInfo"),
                    field("osImage"),
                    list(),
                    text("()"),
                ],
                false,
            ),
            (
                "paired parentheses in double quotes and with double quotes escape",
                r#"{[?(@.status.nodeInfo.osImage == "(\"\")")]}"#.into(),
                vec![
                    list(),
                    filter("=="),
                    list(),
                    field("status"),
                    field("nodeInfo"),
                    field("osImage"),
                    list(),
                    text(&format!("({dq}{dq})")),
                ],
                false,
            ),
            (
                "unregular parentheses in double quotes",
                r#"{[?(@.test == "())(")]}"#.into(),
                vec![
                    list(),
                    filter("=="),
                    list(),
                    field("test"),
                    list(),
                    text("())("),
                ],
                false,
            ),
            (
                "plain text in single quotes",
                "{[?(@.status.nodeInfo.osImage == 'Linux')]}".into(),
                vec![
                    list(),
                    filter("=="),
                    list(),
                    field("status"),
                    field("nodeInfo"),
                    field("osImage"),
                    list(),
                    text("Linux"),
                ],
                false,
            ),
            (
                "test filter suffix",
                r#"{[?(@.status.nodeInfo.osImage == "{[()]}")]}"#.into(),
                vec![
                    list(),
                    filter("=="),
                    list(),
                    field("status"),
                    field("nodeInfo"),
                    field("osImage"),
                    list(),
                    text("{[()]}"),
                ],
                false,
            ),
            (
                "double inside single",
                r#"{[?(@.status.nodeInfo.osImage == "''")]}"#.into(),
                vec![
                    list(),
                    filter("=="),
                    list(),
                    field("status"),
                    field("nodeInfo"),
                    field("osImage"),
                    list(),
                    text("''"),
                ],
                false,
            ),
            (
                "single inside double",
                r#"{[?(@.status.nodeInfo.osImage == '""')]}"#.into(),
                vec![
                    list(),
                    filter("=="),
                    list(),
                    field("status"),
                    field("nodeInfo"),
                    field("osImage"),
                    list(),
                    text("\"\""),
                ],
                false,
            ),
            (
                "single containing escaped single",
                r#"{[?(@.status.nodeInfo.osImage == '\\\'')]}"#.into(),
                vec![
                    list(),
                    filter("=="),
                    list(),
                    field("status"),
                    field("nodeInfo"),
                    field("osImage"),
                    list(),
                    text("\\'"),
                ],
                false,
            ),
            (
                "negative index slice, equals a[len-5] to a[len-1]",
                "{[-5:]}".into(),
                vec![list(), Node::Array([pe(-5, true, false), none, none])],
                false,
            ),
            (
                "negative index slice, equals a[len-1]",
                "{[-1]}".into(),
                vec![
                    list(),
                    Node::Array([pe(-1, true, false), pe(0, true, true), none]),
                ],
                false,
            ),
            (
                "negative index slice, equals a[1] to a[len-1]",
                "{[1:-1]}".into(),
                vec![
                    list(),
                    Node::Array([pe(1, true, false), pe(-1, true, false), none]),
                ],
                false,
            ),
        ];
        for (name, input, expect, should_error) in cases {
            let parsed = parse(name, &input);
            if should_error {
                assert!(parsed.is_err(), "unexpected non-error when parsing {name}");
                continue;
            }
            let parser = parsed.unwrap_or_else(|e| panic!("parse {name} error {e}"));
            let mut result = Vec::new();
            collect(&mut result, &Node::List(parser.root));
            let result = &result[1..];
            assert_eq!(result.len(), expect.len(), "in {name}: {result:?}");
            for (i, e) in expect.iter().enumerate() {
                assert_eq!(result[i].to_string(), e.to_string(), "in {name}, node {i}");
            }
        }
    }

    /// `TestFailParser` (`parser_test.go:137-158`), every case.
    #[test]
    fn upstream_fail_parser_cases() {
        let cases = [
            ("unclosed action", "{.hello", "unclosed action"),
            (
                "unrecognized character",
                "{*}",
                "unrecognized character in action: U+002A '*'",
            ),
            ("invalid number", "{+12.3.0}", "cannot parse number +12.3.0"),
            ("unterminated array", "{[1}", "unterminated array"),
            ("unterminated filter", "{[?(.price]}", "unterminated filter"),
            (
                "invalid multiple recursive descent",
                "{........}",
                "invalid multiple recursive descent",
            ),
        ];
        for (name, input, want) in cases {
            let got = parse(name, input).err().map(|e| e.to_string());
            assert_eq!(got.as_deref(), Some(want), "in {name}");
        }
    }

    /// `validation_test.go:4174-4179`: `{spec[foo]}`.
    #[test]
    fn invalid_array_index_message() {
        let e = parse("spec", "{spec[foo]}").unwrap_err();
        assert_eq!(e.to_string(), "invalid array index foo");
    }

    #[test]
    fn unquote_extend_errors() {
        let e = parse("q", r#"{"a\qb"}"#).unwrap_err();
        assert_eq!(
            e.to_string(),
            "unquote string \"a\\qb\" error invalid syntax"
        );
        assert_eq!(
            parse("q", "{\"abc").unwrap_err().to_string(),
            "unterminated quoted string"
        );
    }
}
