//! Ports of upstream `TestSelectableFields` (`validation_test.go:4544`,
//! release-1.35) plus the structural-schema edge cases of
//! `ValidateCustomResourceSelectableFields` / `cel.ValidFieldPath`
//! (`validation.go:296-320`, `:847-890`; `schema/cel/validation.go:557-676`).

use super::*;
use serde_json::json;

fn crd_with_version(schema: Option<serde_json::Value>, paths: &[&str]) -> CustomResourceDefinition {
    let mut crd = CustomResourceDefinition::new("widget", "group.com", "Plural", "plural");
    let mut version = json!({
        "name": "version",
        "served": true,
        "storage": true,
        "selectableFields": paths.iter().map(|p| json!({"jsonPath": p})).collect::<Vec<_>>(),
    });
    if let Some(s) = schema {
        version["schema"] = json!({"openAPIV3Schema": s});
    }
    crd.spec.versions = vec![serde_json::from_value(version).unwrap()];
    crd.spec.preserve_unknown_fields = Some(false);
    crd.metadata.resource_version = Some("1".to_string());
    set_defaults_custom_resource_definition(&mut crd);
    crd
}

fn selectable_errs(crd: &CustomResourceDefinition) -> Vec<String> {
    validate_custom_resource_definition_spec(&crd.spec, &Path::new("spec"))
        .iter()
        .filter(|e| e.field.contains("selectableFields"))
        .map(|e| e.to_string())
        .collect()
}

/// `validation.go:311-312`.
#[test]
fn selectable_fields_without_a_schema_are_invalid() {
    let errs = selectable_errs(&crd_with_version(None, &[".foo"]));
    assert_eq!(
        errs,
        vec![
            "spec.versions[0].selectableFields: Invalid value: \"\": may only be set when `version.schema.openAPIV3Schema` is not included"
                .to_string()
        ]
    );
}

/// `validation.go:314-317`: a schema `NewStructural` rejects is reported at the
/// literal single path element `schema.openAPIV3Schema`.
#[test]
fn unconvertible_schema_reports_at_schema_dot_openapi_path() {
    let bad = json!({
        "type": "object",
        "properties": {"foo": {"type": "string"}},
        "items": [{"type": "string"}]
    });
    let errs = selectable_errs(&crd_with_version(Some(bad.clone()), &[]));
    assert!(errs.is_empty(), "no selectableFields, no check: {errs:?}");
    let errs = selectable_errs(&crd_with_version(Some(bad), &[".foo"]));
    assert!(
        errs.iter().any(|e| e.starts_with(
            "spec.versions[0].selectableFields.schema.openAPIV3Schema: Invalid value: \"\": OpenAPIV3Schema 'items' must be a schema, but is an array"
        )),
        "{errs:?}"
    );
}

/// `NewStructural` leaves `Properties` nil when `len(s.Properties) == 0`
/// (`convert.go:93`), so `ValidFieldPath` falls through to
/// `AdditionalProperties` (`schema/cel/validation.go:660-664`).
#[test]
fn empty_properties_fall_through_to_additional_properties() {
    let errs = selectable_errs(&crd_with_version(
        Some(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": {"type": "string"}
        })),
        &[".anything"],
    ));
    assert!(errs.is_empty(), "{errs:?}");
}

/// `additionalProperties: true` is a non-nil `AdditionalProperties` whose
/// `Structural` is nil, so the leaf schema is nil and
/// `allowedSelectableFieldSchema(nil)` is false (`validation.go:880-883`).
#[test]
fn bool_additional_properties_leaf_is_not_selectable() {
    let errs = selectable_errs(&crd_with_version(
        Some(json!({"type": "object", "additionalProperties": true})),
        &[".anything"],
    ));
    assert_eq!(
        errs,
        vec![
            "spec.versions[0].selectableFields[0].jsonPath: Invalid value: \".anything\": must point to a field of type string, boolean or integer. Enum string fields and strings with formats are allowed."
                .to_string()
        ]
    );
}

/// A field reachable only through `allOf` is not in the structural schema's
/// `Properties` either, so upstream rejects it too.
#[test]
fn all_of_only_field_is_rejected_like_upstream() {
    let errs = selectable_errs(&crd_with_version(
        Some(json!({
            "type": "object",
            "properties": {"a": {"type": "string"}},
            "allOf": [{"properties": {"b": {"type": "string"}}}]
        })),
        &[".b"],
    ));
    assert_eq!(
        errs,
        vec![
            "spec.versions[0].selectableFields[0].jsonPath: Invalid value: \".b\": is an invalid path: does not refer to a valid field"
                .to_string()
        ]
    );
}

/// `TestSelectableFields` "in top level schema ...": a path under an
/// existing object resolves; an absent sibling is invalid; `.spec.foo` twice
/// is a duplicate on the second only.
#[test]
fn nested_resolution_and_duplicates() {
    let errs = selectable_errs(&crd_with_version(
        Some(json!({
            "type": "object",
            "properties": {"spec": {"type": "object", "properties": {"foo": {"type": "string"}}}}
        })),
        &[".spec.foo", ".spec.xyz", ".spec.foo"],
    ));
    assert_eq!(errs.len(), 2, "{errs:?}");
    assert!(errs[0].starts_with("spec.versions[0].selectableFields[1].jsonPath: Invalid value"));
    assert!(errs[1].starts_with("spec.versions[0].selectableFields[2].jsonPath: Duplicate value"));
}
