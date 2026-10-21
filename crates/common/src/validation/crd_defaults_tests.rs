//! Ports of the default cases of upstream `TestValidateCustomResourceDefinition`
//! (`apis/apiextensions/validation/validation_test.go:1780-2260`,
//! `2311-2604`, `2608-3980`, release-1.35) and the `requirePrunedDefaults`
//! ratchet (`validation.go:1779`; update cases at `:6872-7000`).
//!
//! Like upstream's `validationMatch`, expected errors are (type, field path
//! below `openAPIV3Schema`) pairs matched as a multiset.

use super::*;
use crate::validation::field::ErrorType;
use serde_json::{json, Value};

fn crd_with_schema(schema: Value) -> CustomResourceDefinition {
    let mut crd = CustomResourceDefinition::new("widget", "group.com", "Plural", "plural");
    crd.spec.versions = vec![serde_json::from_value(json!({
        "name": "v1", "served": true, "storage": true,
        "schema": {"openAPIV3Schema": schema}
    }))
    .unwrap()];
    crd.spec.preserve_unknown_fields = Some(false);
    crd.metadata.resource_version = Some("1".to_string());
    set_defaults_custom_resource_definition(&mut crd);
    crd
}

const PREFIX: &str = "spec.versions[0].schema.openAPIV3Schema";

fn got(crd: &CustomResourceDefinition) -> Vec<(ErrorType, String)> {
    let mut v: Vec<_> = validate_custom_resource_definition(crd)
        .into_iter()
        .filter(|e| e.field.starts_with(PREFIX))
        .map(|e| {
            (
                e.error_type,
                e.field.strip_prefix(PREFIX).unwrap().to_string(),
            )
        })
        .collect();
    v.sort_by(|a, b| a.1.cmp(&b.1));
    v
}

fn check(schema: Value, want: &[(ErrorType, &str)]) {
    let mut want: Vec<_> = want.iter().map(|(t, p)| (*t, p.to_string())).collect();
    want.sort_by(|a, b| a.1.cmp(&b.1));
    let g = got(&crd_with_schema(schema));
    assert_eq!(g, want);
}

fn obj(props: Value) -> Value {
    json!({"type": "object", "properties": props})
}

/// "embedded metadata defaults": the allowed shapes — unknown fields inside
/// `metadata`, and under `x-kubernetes-preserve-unknown-fields`.
#[test]
fn allowed_embedded_defaults() {
    check(
        obj(json!({
            "allowed-in-object-defaults": {
                "type": "object", "x-kubernetes-embedded-resource": true,
                "properties": {
                    "apiVersion": {"type": "string", "default": "v1"},
                    "kind": {"type": "string", "default": "Pod"},
                    "metadata": {
                        "type": "object",
                        "properties": {"name": {"type": "string", "default": "foo"}},
                        "default": {"unknown": 42}
                    }
                }
            },
            "allowed-object-defaults": {
                "type": "object", "x-kubernetes-embedded-resource": true,
                "properties": {"something": {"type": "string"}},
                "default": {"apiVersion": "v1", "kind": "Pod",
                            "metadata": {"name": "foo", "unknown": 42}}
            },
            "allowed-spanning-object-defaults": {
                "type": "object",
                "properties": {"embedded": {
                    "type": "object", "x-kubernetes-embedded-resource": true,
                    "properties": {"something": {"type": "string"}}}},
                "default": {"embedded": {"apiVersion": "v1", "kind": "Pod",
                            "metadata": {"name": "foo", "unknown": 42}}}
            },
            "x-preserve-unknown-fields-unknown-field-object-defaults": {
                "type": "object", "x-kubernetes-embedded-resource": true,
                "x-kubernetes-preserve-unknown-fields": true, "properties": {},
                "default": {"apiVersion": "v1", "kind": "Pod",
                            "metadata": {"name": "foo", "unknown": 42}, "unknown": 42}
            }
        })),
        &[],
    );
}

/// "unknown-field-object-defaults" / "...-spanning-object-defaults".
#[test]
fn unknown_fields_in_defaults_are_invalid() {
    check(
        obj(json!({
            "unknown-field-object-defaults": {
                "type": "object", "x-kubernetes-embedded-resource": true,
                "properties": {"something": {"type": "string"}},
                "default": {"apiVersion": "v1", "kind": "Pod",
                            "metadata": {"name": "foo", "unknown": 42}, "unknown": 42}
            },
            "unknown-field-spanning-object-defaults": {
                "type": "object",
                "properties": {"embedded": {
                    "type": "object", "x-kubernetes-embedded-resource": true,
                    "properties": {"something": {"type": "string"}}}},
                "default": {"embedded": {"apiVersion": "v1", "kind": "Pod",
                            "metadata": {"name": "foo", "unknown": 42}, "unknown": 42},
                            "unknown": 42}
            }
        })),
        &[
            (
                ErrorType::Invalid,
                ".properties[unknown-field-object-defaults].default",
            ),
            (
                ErrorType::Invalid,
                ".properties[unknown-field-spanning-object-defaults].default",
            ),
        ],
    );
}

/// "wrongly-typed-in-object-defaults": defaults of the `apiVersion`, `kind`
/// and `metadata` properties of an embedded resource (the `insideMeta` branch).
#[test]
fn wrongly_typed_in_object_defaults() {
    check(
        obj(json!({
            "wrongly-typed-in-object-defaults": {
                "type": "object", "x-kubernetes-embedded-resource": true,
                "properties": {
                    "apiVersion": {"type": "string", "default": 42},
                    "kind": {"type": "string", "default": 42},
                    "metadata": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "default": 42},
                            "annotations": {
                                "type": "object",
                                "additionalProperties": {"type": "string"},
                                "default": 42
                            }
                        }
                    }
                }
            }
        })),
        &[
            (
                ErrorType::Invalid,
                ".properties[wrongly-typed-in-object-defaults].properties[kind].default",
            ),
            (
                ErrorType::Invalid,
                ".properties[wrongly-typed-in-object-defaults].properties[apiVersion].default",
            ),
            (
                ErrorType::Invalid,
                ".properties[wrongly-typed-in-object-defaults].properties[metadata].properties[name].default",
            ),
            (
                ErrorType::Invalid,
                ".properties[wrongly-typed-in-object-defaults].properties[metadata].properties[annotations].default",
            ),
        ],
    );
}

/// "wrongly-typed-object-defaults-*" and the `invalid-*` / `missing-typemeta`
/// object defaults.
#[test]
fn embedded_object_defaults_are_validated() {
    let embedded = |default: Value| {
        json!({"type": "object", "x-kubernetes-embedded-resource": true,
               "properties": {"something": {"type": "string"}}, "default": default})
    };
    check(
        obj(json!({
            "wrongly-typed-object-defaults-metadata": embedded(json!({"metadata": 42})),
            "wrongly-typed-object-defaults-apiVersion":
                embedded(json!({"apiVersion": 42, "kind": "Foo"})),
            "wrongly-typed-object-defaults-kind":
                embedded(json!({"apiVersion": "foo/v1", "kind": 42})),
            "wrongly-typed-object-defaults-labels":
                embedded(json!({"metadata": {"labels": 42}})),
            "invalid-object-defaults-kind":
                embedded(json!({"apiVersion": "foo/v1", "kind": "%"})),
            "invalid-object-defaults-name":
                embedded(json!({"apiVersion": "foo/v1", "kind": "Foo",
                                "metadata": {"name": "%"}})),
            "invalid-object-defaults-labels":
                embedded(json!({"apiVersion": "foo/v1", "kind": "Foo",
                                "metadata": {"labels": {"foo": "x y"}}})),
            "object-defaults-with-missing-typemeta":
                embedded(json!({"metadata": {"name": "foo"}})),
            "valid-object-defaults":
                embedded(json!({"apiVersion": "foo/v1", "kind": "Foo",
                                "metadata": {"name": "foo"}})),
        })),
        &[
            (
                ErrorType::Invalid,
                ".properties[wrongly-typed-object-defaults-metadata].default.metadata",
            ),
            (
                ErrorType::Invalid,
                ".properties[wrongly-typed-object-defaults-apiVersion].default.apiVersion",
            ),
            (
                ErrorType::Invalid,
                ".properties[wrongly-typed-object-defaults-kind].default.kind",
            ),
            (
                ErrorType::Invalid,
                ".properties[wrongly-typed-object-defaults-labels].default.metadata",
            ),
            (
                ErrorType::Invalid,
                ".properties[invalid-object-defaults-kind].default.kind",
            ),
            (
                ErrorType::Invalid,
                ".properties[invalid-object-defaults-name].default.metadata.name",
            ),
            (
                ErrorType::Invalid,
                ".properties[invalid-object-defaults-labels].default.metadata.labels",
            ),
            (
                ErrorType::Required,
                ".properties[object-defaults-with-missing-typemeta].default.apiVersion",
            ),
            (
                ErrorType::Required,
                ".properties[object-defaults-with-missing-typemeta].default.kind",
            ),
        ],
    );
}

/// "spanning" defaults: the embedded resource sits below the object holding
/// the default.
#[test]
fn spanning_object_defaults_are_validated() {
    let spanning = |default: Value| {
        json!({"type": "object",
               "properties": {"embedded": {
                   "type": "object", "x-kubernetes-embedded-resource": true,
                   "properties": {"something": {"type": "string"}}}},
               "default": default})
    };
    check(
        obj(json!({
            "invalid-spanning-object-defaults-kind":
                spanning(json!({"embedded": {"apiVersion": "foo/v1", "kind": "%"}})),
            "invalid-spanning-object-defaults-name":
                spanning(json!({"embedded": {"apiVersion": "foo/v1", "kind": "Foo",
                                             "metadata": {"name": "%"}}})),
            "spanning-defaults-with-missing-typemeta":
                spanning(json!({"embedded": {"metadata": {"name": "foo"}}})),
        })),
        &[
            (
                ErrorType::Invalid,
                ".properties[invalid-spanning-object-defaults-kind].default.embedded.kind",
            ),
            (
                ErrorType::Invalid,
                ".properties[invalid-spanning-object-defaults-name].default.embedded.metadata.name",
            ),
            (
                ErrorType::Required,
                ".properties[spanning-defaults-with-missing-typemeta].default.embedded.apiVersion",
            ),
            (
                ErrorType::Required,
                ".properties[spanning-defaults-with-missing-typemeta].default.embedded.kind",
            ),
        ],
    );
}

/// "metadata-defaults-with-invalid-constraints-*": a default in `metadata`
/// that violates the constraints of the metadata sub-schema is `Unsupported`.
#[test]
fn metadata_defaults_violating_constraints_are_unsupported() {
    check(
        obj(json!({
            "metadata-defaults-with-invalid-constraints-name": {
                "type": "object", "x-kubernetes-embedded-resource": true,
                "properties": {"metadata": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "enum": ["foo"]},
                        "labels": {"type": "object",
                                   "additionalProperties": {"type": "string", "enum": ["foo"]}}
                    },
                    "default": {"name": "bar"}
                }}
            },
            "metadata-defaults-with-invalid-constraints-labels": {
                "type": "object", "x-kubernetes-embedded-resource": true,
                "properties": {"metadata": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "enum": ["foo"]},
                        "labels": {"type": "object",
                                   "additionalProperties": {"type": "string", "enum": ["foo"]}}
                    },
                    "default": {"name": "foo", "labels": {"foo": "bar"}}
                }}
            }
        })),
        &[
            (
                ErrorType::NotSupported,
                ".properties[metadata-defaults-with-invalid-constraints-name].properties[metadata].default.name",
            ),
            (
                ErrorType::NotSupported,
                ".properties[metadata-defaults-with-invalid-constraints-labels].properties[metadata].default.labels.foo",
            ),
        ],
    );
}

/// A plain default that does not validate against its own schema, and a
/// default under `items`.
#[test]
fn plain_defaults_are_validated_against_their_schema() {
    check(
        obj(json!({
            "ok": {"type": "integer", "default": 1},
            "bad": {"type": "integer", "default": "x"},
            "list": {"type": "array", "items": {"type": "string", "default": 5}},
            "enumd": {"type": "string", "enum": ["a"], "default": "b"},
            "nested": {"type": "object",
                       "properties": {"x": {"type": "string"}},
                       "required": ["x"], "default": {}}
        })),
        &[
            (ErrorType::Invalid, ".properties[bad].default"),
            (ErrorType::Invalid, ".properties[list].items.default"),
            (ErrorType::NotSupported, ".properties[enumd].default"),
            (ErrorType::Required, ".properties[nested].default.x"),
        ],
    );
}

fn unpruned_default_schema() -> Value {
    obj(json!({"spec": {
        "type": "object",
        "properties": {"a": {"type": "string"}},
        "default": {"a": "x", "unknown": 1}
    }}))
}

/// "add default ... structural schema, without pruning": a new unpruned
/// default is rejected on create.
#[test]
fn unpruned_default_rejected_on_create() {
    check(
        unpruned_default_schema(),
        &[(ErrorType::Invalid, ".properties[spec].default")],
    );
}

/// `requirePrunedDefaults` (`validation.go:1779`): an old object that already
/// holds an unpruned default keeps `requirePrunedDefaults=false`, so the
/// update passes; once the old object is pruned, a new unpruned default fails.
#[test]
fn require_pruned_defaults_ratchet_on_update() {
    let old = crd_with_schema(unpruned_default_schema());
    assert!(!crate::validation::structural::defaulting::require_pruned_defaults(&old.spec));
    let new = old.clone();
    let errs: Vec<_> = validate_custom_resource_definition_update(&new, &old)
        .into_iter()
        .filter(|e| e.field.starts_with(PREFIX))
        .collect();
    assert!(errs.is_empty(), "{errs:?}");

    let clean = crd_with_schema(obj(json!({"spec": {
        "type": "object", "properties": {"a": {"type": "string"}},
        "default": {"a": "x"}}})));
    assert!(crate::validation::structural::defaulting::require_pruned_defaults(&clean.spec));
    let mut bad = clean.clone();
    bad.spec = old.spec.clone();
    let errs: Vec<_> = validate_custom_resource_definition_update(&bad, &clean)
        .into_iter()
        .filter(|e| e.field.starts_with(PREFIX))
        .collect();
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert_eq!(errs[0].error_type, ErrorType::Invalid);
}

/// `PruneDefaults` (`prune.go`): prunes a default against the root schema.
#[test]
fn prune_defaults_prunes_unknown_fields() {
    use crate::validation::structural::{defaulting, new_structural};
    let schema: crate::resources::crd::JSONSchemaProps =
        serde_json::from_value(unpruned_default_schema()).unwrap();
    let mut ss = new_structural(&schema).unwrap();
    defaulting::prune_defaults(&mut ss).unwrap();
    assert_eq!(
        ss.properties["spec"].generic.default,
        Some(json!({"a": "x"}))
    );
}
