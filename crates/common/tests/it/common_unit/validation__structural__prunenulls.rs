//! Unit tests moved from `src/validation/structural/prunenulls.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::validation::structural::Structural;
    pub use serde_json::Value;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::structural::prunenulls::*;
    use rusternetes_common::validation::structural::Generic;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn generic(default: Option<Value>, nullable: bool) -> Structural {
        Structural {
            generic: Generic {
                default,
                nullable,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// prunenulls_test.go `TestPruneNonNullableNullsWithoutDefaults` :28-67.
    #[test]
    fn upstream_cases() {
        // "empty"
        let mut x = json!(null);
        prune_non_nullable_nulls_without_defaults(&mut x, None);
        assert_eq!(x, json!(null));

        // "scalar"
        let mut x = json!(4);
        prune_non_nullable_nulls_without_defaults(
            &mut x,
            Some(&generic(Some(json!("foo")), false)),
        );
        assert_eq!(x, json!(4));

        // "scalar array"
        let mut x = json!([1, null]);
        prune_non_nullable_nulls_without_defaults(&mut x, None);
        assert_eq!(x, json!([1, null]));

        // "object array"
        let mut props = BTreeMap::new();
        props.insert("a".to_string(), generic(Some(json!("A")), false));
        props.insert("b".to_string(), generic(None, true));
        props.insert("c".to_string(), generic(Some(json!("C")), true));
        props.insert("d".to_string(), generic(None, false));
        let s = Structural {
            items: Some(Box::new(Structural {
                properties: props,
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut x = json!([{"a":null},{"b":null},{"c":null},{"d":null},{"e":null}]);
        prune_non_nullable_nulls_without_defaults(&mut x, Some(&s));
        assert_eq!(x, json!([{"a":null},{"b":null},{"c":null},{},{"e":null}]));
    }
}
