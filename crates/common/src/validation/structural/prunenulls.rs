//! `structuraldefaulting.PruneNonNullableNullsWithoutDefaults`.
//!
//! Port of
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting/prunenulls.go`
//! (:21-66), release-1.35.

use serde_json::Value;

use super::Structural;

/// `isNonNullableNonDefaultableNull` (:21-23).
fn is_non_nullable_non_defaultable_null(x: &Value, s: Option<&Structural>) -> bool {
    x.is_null() && s.is_some_and(|s| !s.generic.nullable && s.generic.default.is_none())
}

/// `getSchemaForField` (:25-37).
fn get_schema_for_field<'a>(field: &str, s: Option<&'a Structural>) -> Option<&'a Structural> {
    let s = s?;
    if let Some(schema) = s.properties.get(field) {
        return Some(schema);
    }
    s.additional_properties
        .as_ref()
        .and_then(|ap| ap.structural.as_ref())
}

/// `PruneNonNullableNullsWithoutDefaults` (:39-66): removes non-nullable
/// non-defaultable null values from the object. Non-nullable nulls that have
/// a default are left alone here and are defaulted later.
pub fn prune_non_nullable_nulls_without_defaults(x: &mut Value, s: Option<&Structural>) {
    match x {
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                let schema = get_schema_for_field(&k, s);
                let remove = map
                    .get(&k)
                    .is_some_and(|v| is_non_nullable_non_defaultable_null(v, schema));
                if remove {
                    map.remove(&k);
                } else if let Some(v) = map.get_mut(&k) {
                    prune_non_nullable_nulls_without_defaults(v, schema);
                }
            }
        }
        Value::Array(items) => {
            let schema = s.and_then(|s| s.items.as_deref());
            for item in items {
                prune_non_nullable_nulls_without_defaults(item, schema);
            }
        }
        // scalars, do nothing
        _ => {}
    }
}

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
