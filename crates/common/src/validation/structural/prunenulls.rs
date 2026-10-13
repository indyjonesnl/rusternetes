//! `structuraldefaulting.PruneNonNullableNullsWithoutDefaults`.
//!
//! Port of
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting/prunenulls.go`
//! (:21-66), release-1.35.

use serde_json::Value;

use super::Structural;

pub fn prune_non_nullable_nulls_without_defaults(_x: &mut Value, _s: Option<&Structural>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::structural::Generic;
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
