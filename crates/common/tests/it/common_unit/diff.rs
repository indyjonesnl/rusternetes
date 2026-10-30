//! Unit tests moved from `src/diff.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::diff::*;

    fn d(a: &str, b: &str) -> String {
        unified_diff(&split_lines(a), &split_lines(b), 3)
    }

    #[test]
    fn identical_is_empty() {
        assert_eq!(d("a\nb", "a\nb"), "");
    }

    #[test]
    fn split_lines_appends_newline_to_last() {
        assert_eq!(split_lines("a\nb"), vec!["a\n", "b\n"]);
        assert_eq!(split_lines(""), vec!["\n"]);
    }

    #[test]
    fn single_line_change_uses_short_range() {
        // difflib/python: `@@ -1 +1 @@`
        assert_eq!(d("a", "b"), "@@ -1 +1 @@\n-a\n+b\n");
    }

    #[test]
    fn context_of_three_and_hunk_split() {
        let a: Vec<String> = (0..20).map(|i| format!("l{i}")).collect();
        let mut b = a.clone();
        b[1] = "X".into();
        b[18] = "Y".into();
        let got = d(&a.join("\n"), &b.join("\n"));
        let want = "@@ -1,5 +1,5 @@\n l0\n-l1\n+X\n l2\n l3\n l4\n@@ -16,5 +16,5 @@\n l15\n l16\n l17\n-l18\n+Y\n l19\n";
        assert_eq!(got, want);
    }

    #[test]
    fn insertion_and_deletion() {
        assert_eq!(d("a\nb", "a\nx\nb"), "@@ -1,2 +1,3 @@\n a\n+x\n b\n");
        assert_eq!(d("a\nx\nb", "a\nb"), "@@ -1,3 +1,2 @@\n a\n-x\n b\n");
    }

    #[test]
    fn marshal_indent_matches_go() {
        let v = GoJson::Object(vec![
            ("B".into(), GoJson::Array(vec![])),
            ("A".into(), GoJson::Array(vec![GoJson::str("x<y")])),
            ("C".into(), GoJson::Object(vec![])),
            ("D".into(), GoJson::Null),
        ]);
        assert_eq!(
            v.marshal_indent(),
            "{\n \"B\": [],\n \"A\": [\n  \"x\\u003cy\"\n ],\n \"C\": {},\n \"D\": null\n}"
        );
    }
}
