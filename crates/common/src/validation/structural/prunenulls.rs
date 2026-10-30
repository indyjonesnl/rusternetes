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
