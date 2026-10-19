//! Ports of the structural-schema cases of upstream
//! `TestValidateCustomResourceDefinition` (`validation_test.go:1430-1700`,
//! release-1.35) and the `requireStructuralSchema` ratchet
//! (`validation.go:1748-1777`).

use super::*;
use serde_json::json;

/// `validValidationSchema` (`validation_test.go:11487`), trimmed to the
/// fields that matter: structural.
fn valid_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "description": "This is a description",
        "required": ["spec", "status"],
        "properties": {
            "spec": {"type": "object"},
            "status": {"type": "object"}
        }
    })
}

/// `validUnstructuralValidationSchema` (`validation_test.go:11523`): `type`
/// missing under `properties` and `items`.
fn unstructural_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "required": ["spec", "status"],
        "items": {"description": "This is a schema nested under Items"},
        "properties": {"spec": {}, "status": {}}
    })
}

fn crd_with(schemas: &[serde_json::Value]) -> CustomResourceDefinition {
    let mut crd = CustomResourceDefinition::new("widget", "group.com", "Plural", "plural");
    crd.spec.versions = schemas
        .iter()
        .enumerate()
        .map(|(i, s)| {
            serde_json::from_value(json!({
                "name": format!("v{}", i + 1),
                "served": true,
                "storage": i == 0,
                "schema": {"openAPIV3Schema": s}
            }))
            .unwrap()
        })
        .collect();
    crd.spec.preserve_unknown_fields = Some(false);
    crd.metadata.resource_version = Some("1".to_string());
    set_defaults_custom_resource_definition(&mut crd);
    crd
}

fn schema_errs(errs: &ErrorList) -> Vec<String> {
    errs.iter()
        .filter(|e| e.field.contains("openAPIV3Schema"))
        .map(|e| e.to_string())
        .collect()
}

#[test]
fn structural_schema_is_accepted() {
    let errs = validate_custom_resource_definition(&crd_with(&[valid_schema()]));
    assert!(schema_errs(&errs).is_empty(), "{errs:?}");
}

/// "preserveUnknownFields with unstructural schema in one version"
/// (`validation_test.go:1511`): only versions[1] is reported, with the
/// `Required` errors for the missing types.
#[test]
fn unstructural_schema_in_one_version_is_required_type() {
    let errs =
        validate_custom_resource_definition(&crd_with(&[valid_schema(), unstructural_schema()]));
    let got = schema_errs(&errs);
    for want in [
        "spec.versions[1].schema.openAPIV3Schema.properties[spec].type: Required value",
        "spec.versions[1].schema.openAPIV3Schema.properties[status].type: Required value",
        "spec.versions[1].schema.openAPIV3Schema.items.type: Required value",
    ] {
        assert!(got.iter().any(|g| g.starts_with(want)), "{want}\n{got:#?}");
    }
    assert!(got.iter().all(|g| g.contains("versions[1]")), "{got:#?}");
}

/// `NewStructural` rejects an unsupported field (`convert.go`); the error is
/// `Invalid` at the schema path.
#[test]
fn unsupported_field_is_invalid_at_schema() {
    let mut s = valid_schema();
    s["properties"]["spec"]["id"] = json!("foo");
    let errs = validate_custom_resource_definition(&crd_with(&[s]));
    assert!(
        schema_errs(&errs)
            .iter()
            .any(|e| e.starts_with("spec.versions[0].schema.openAPIV3Schema: Invalid value:")),
        "{errs:?}"
    );
}

/// `specHasDefaults` (`validation.go:391-396`): defaults require
/// `preserveUnknownFields: false` — on update, where an old `true` is
/// tolerated.
#[test]
fn defaults_need_preserve_unknown_fields_false() {
    let mut s = valid_schema();
    s["properties"]["spec"]["default"] = json!({});
    let mut crd = crd_with(&[s]);
    let old = crd.clone();
    crd.spec.preserve_unknown_fields = Some(true);
    let errs = validate_custom_resource_definition_update(&crd, &old);
    assert!(
        errs.iter().any(|e| e.field == "spec.preserveUnknownFields"
            && e.to_string()
                .contains("must be false in order to use defaults in the schema")),
        "{errs:?}"
    );
}

/// `requireStructuralSchema` (`validation.go:1748`): an object whose stored
/// schema is already non-structural is not tightened on update. Only a CRD
/// with `preserveUnknownFields: true` can be in that state, because
/// `preserveUnknownFields` false forces the requirement (`validation.go:366`).
#[test]
fn update_does_not_tighten_already_unstructural_crd() {
    let mut old = crd_with(&[unstructural_schema()]);
    old.spec.preserve_unknown_fields = Some(true);
    let new = old.clone();
    let errs = validate_custom_resource_definition_update(&new, &old);
    assert!(schema_errs(&errs).is_empty(), "{errs:?}");
}

/// An update from a structural schema to a non-structural one is rejected.
#[test]
fn update_to_unstructural_is_rejected() {
    let old = crd_with(&[valid_schema()]);
    let mut new = crd_with(&[unstructural_schema()]);
    new.metadata = old.metadata.clone();
    let errs = validate_custom_resource_definition_update(&new, &old);
    assert!(!schema_errs(&errs).is_empty(), "{errs:?}");
}

/// `openAPIV3Schema: {}` is a non-nil pointer upstream, so unlike an absent
/// one (`validation.go:914-916`) it is run through `NewStructural` and
/// `ValidateStructural`, which require a root type.
#[test]
fn empty_schema_is_validated_not_skipped() {
    let errs = validate_custom_resource_definition(&crd_with(&[json!({})]));
    assert!(
        schema_errs(&errs)
            .iter()
            .any(|e| e.starts_with("spec.versions[0].schema.openAPIV3Schema.type: Required value")),
        "{errs:?}"
    );
}

/// A CRD whose versions carry the given raw `schema` values (`None` omits the
/// key, as a manifest without `schema` does).
fn crd_with_raw_schemas(schemas: &[Option<serde_json::Value>]) -> CustomResourceDefinition {
    let mut crd = crd_with(&[]);
    crd.spec.versions = schemas
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let mut v = json!({"name": format!("v{}", i + 1), "served": true, "storage": i == 0});
            if let Some(s) = s {
                v["schema"] = s.clone();
            }
            serde_json::from_value(v).unwrap()
        })
        .collect();
    crd.spec.preserve_unknown_fields = Some(false);
    crd
}

fn required_schema_errs(errs: &ErrorList) -> Vec<String> {
    errs.iter()
        .filter(|e| {
            e.to_string().starts_with("spec.versions[")
                && e.to_string()
                    .contains("schema.openAPIV3Schema: Required value")
        })
        .map(|e| e.field.clone())
        .collect()
}

/// "preserveUnknownFields with no schema in one version"
/// (`validation_test.go:1557`): `schema: {openAPIV3Schema: nil}`.
#[test]
fn create_requires_schema_in_every_version_one_missing() {
    let crd = crd_with_raw_schemas(&[
        Some(json!({"openAPIV3Schema": valid_schema()})),
        Some(json!({})),
    ]);
    let errs = validate_custom_resource_definition(&crd);
    assert_eq!(
        required_schema_errs(&errs),
        vec!["spec.versions[1].schema.openAPIV3Schema"],
        "{errs:?}"
    );
}

/// "preserveUnknownFields with no schema at all" (`validation_test.go:1599`).
#[test]
fn create_requires_schema_when_none_at_all() {
    let crd = crd_with_raw_schemas(&[None, Some(json!({}))]);
    let errs = validate_custom_resource_definition(&crd);
    assert_eq!(
        required_schema_errs(&errs),
        vec![
            "spec.versions[0].schema.openAPIV3Schema",
            "spec.versions[1].schema.openAPIV3Schema"
        ],
        "{errs:?}"
    );
}

/// `preserveUnknownFields: true` does not excuse a missing schema on create:
/// `requireOpenAPISchema` is true for every create (`validation.go:371-377`).
#[test]
fn create_requires_schema_even_with_preserve_unknown_fields() {
    let mut crd = crd_with_raw_schemas(&[None]);
    crd.spec.preserve_unknown_fields = Some(true);
    let errs = validate_custom_resource_definition(&crd);
    assert_eq!(
        required_schema_errs(&errs),
        vec!["spec.versions[0].schema.openAPIV3Schema"],
        "{errs:?}"
    );
}

/// "schema not required if old object is missing schema"
/// (`validation_test.go:6742`): `requireOpenAPISchema` ratchet.
#[test]
fn update_does_not_require_schema_if_old_is_missing_it() {
    let mut old = crd_with_raw_schemas(&[None]);
    old.spec.preserve_unknown_fields = Some(true);
    let new = old.clone();
    let errs = validate_custom_resource_definition_update(&new, &old);
    assert!(required_schema_errs(&errs).is_empty(), "{errs:?}");
}

/// "schema not required if old object is missing schema for some versions"
/// (`validation_test.go:6770`).
#[test]
fn update_does_not_require_schema_if_old_is_missing_some() {
    let mut old =
        crd_with_raw_schemas(&[Some(json!({"openAPIV3Schema": {"type": "object"}})), None]);
    old.spec.preserve_unknown_fields = Some(true);
    let mut new = crd_with_raw_schemas(&[None, None]);
    new.spec.preserve_unknown_fields = Some(true);
    new.metadata = old.metadata.clone();
    let errs = validate_custom_resource_definition_update(&new, &old);
    assert!(required_schema_errs(&errs).is_empty(), "{errs:?}");
}

/// "schema required if all versions of old object have schema"
/// (`validation_test.go:6831`/`:6867`).
#[test]
fn update_requires_schema_if_all_old_versions_have_one() {
    let mut old = crd_with_raw_schemas(&[
        Some(json!({"openAPIV3Schema": {"type": "object", "description": "1"}})),
        Some(json!({"openAPIV3Schema": {"type": "object", "description": "2"}})),
    ]);
    old.spec.preserve_unknown_fields = Some(true);
    let mut new = crd_with_raw_schemas(&[None, None]);
    new.spec.preserve_unknown_fields = Some(true);
    new.metadata = old.metadata.clone();
    let errs = validate_custom_resource_definition_update(&new, &old);
    assert_eq!(
        required_schema_errs(&errs),
        vec![
            "spec.versions[0].schema.openAPIV3Schema",
            "spec.versions[1].schema.openAPIV3Schema"
        ],
        "{errs:?}"
    );
}

/// The `else if` arm (`validation.go:378-387`): the old object lacked a schema
/// somewhere so `requireOpenAPISchema` is false, and `preserveUnknownFields:
/// false` makes a served version without a schema prune every field.
#[test]
fn update_served_version_without_schema_is_pruned_required() {
    let mut old = crd_with_raw_schemas(&[None]);
    old.spec.preserve_unknown_fields = Some(true);
    let mut new = old.clone();
    new.spec.preserve_unknown_fields = Some(false);
    let errs = validate_custom_resource_definition_update(&new, &old);
    assert!(
        errs.iter().any(|e| e.to_string().starts_with(
            "spec.versions[0].schema.openAPIV3Schema: Required value: because otherwise all fields are pruned"
        )),
        "{errs:?}"
    );
}
