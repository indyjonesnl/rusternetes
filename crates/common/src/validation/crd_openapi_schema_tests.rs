//! Ports of the schema-error cases of upstream
//! `TestValidateCustomResourceDefinitionValidation` (`validation_test.go:8321`,
//! release-1.35): the `ValidateCustomResourceDefinitionOpenAPISchema` walk, the
//! status-subresource root restrictions and the `requireAtomicSetType` /
//! `requireMapListKeysMapSetValidation` / `requireValidPropertyType` ratchets.
//! The CEL cases of that test belong to the api-server crate.

use super::*;
use serde_json::json;

fn run(
    schema: serde_json::Value,
    status_enabled: bool,
    structural: bool,
    opts: OpenApiSchemaOptions,
) -> Vec<String> {
    let schema: JSONSchemaProps = serde_json::from_value(schema).unwrap();
    validate_custom_resource_definition_validation(
        &schema,
        status_enabled,
        structural,
        true,
        &opts,
        &Path::new("spec").child("validation"),
    )
    .iter()
    .map(|e| e.to_string())
    .collect()
}

fn ok(schema: serde_json::Value) -> Vec<String> {
    run(schema, false, false, OpenApiSchemaOptions::default())
}

fn assert_errs(got: &[String], want: &[&str]) {
    assert_eq!(got.len(), want.len(), "{got:#?}");
    for w in want {
        assert!(
            got.iter().any(|g| g.starts_with(w)),
            "missing {w}\n{got:#?}"
        );
    }
}

const P: &str = "spec.validation.openAPIV3Schema";

#[test]
fn root_type_with_status_must_be_object() {
    let got = run(json!({"type": "string"}), true, false, Default::default());
    assert_errs(&got, &[&format!("{P}.type: Invalid value: \"string\"")]);
    assert!(run(json!({"type": "string"}), false, false, Default::default()).is_empty());
}

#[test]
fn non_allowed_root_field_with_status() {
    let got = run(
        json!({"type": "object", "default": {}}),
        true,
        false,
        Default::default(),
    );
    assert_eq!(got.len(), 1, "{got:#?}");
    assert!(
        got[0].starts_with(&format!("{P}: Invalid value:")),
        "{got:#?}"
    );
    assert!(
        got[0].ends_with("only [Description Type Format Title Maximum ExclusiveMaximum Minimum ExclusiveMinimum MaxLength MinLength Pattern MaxItems MinItems UniqueItems MultipleOf Required Items Properties ExternalDocs Example XPreserveUnknownFields XValidations] fields are allowed at the root of the schema if the status subresource is enabled"),
        "{got:#?}"
    );
}

#[test]
fn allowed_root_fields_with_status() {
    let got = run(
        json!({
            "description": "d", "type": "object", "format": "f", "title": "t",
            "maximum": 1.0, "exclusiveMaximum": true, "minimum": 0.0,
            "exclusiveMinimum": true, "maxLength": 3, "minLength": 1, "pattern": "p",
            "maxItems": 4, "minItems": 1, "uniqueItems": false, "multipleOf": 2.0,
            "required": ["a"], "properties": {"a": {"type": "string"}},
            "example": {}, "x-kubernetes-preserve-unknown-fields": true
        }),
        true,
        false,
        Default::default(),
    );
    assert!(got.is_empty(), "{got:#?}");
}

#[test]
fn null_type_and_nullable_root() {
    // upstream's "null type" case runs without `requireValidPropertyType`
    let opts = OpenApiSchemaOptions {
        require_valid_property_type: false,
        ..Default::default()
    };
    let got = run(
        json!({"type": "object", "properties": {"null": {"type": "null"}}}),
        false,
        false,
        opts,
    );
    assert_errs(&got, &[&format!("{P}.properties[null].type: Forbidden")]);
    let got = ok(json!({"type": "object", "nullable": true}));
    assert_errs(&got, &[&format!("{P}.nullable: Forbidden")]);
    assert!(ok(json!({"type": "object", "properties": {"a": {"nullable": true}}})).is_empty());
}

#[test]
fn unsupported_fields_are_forbidden() {
    let got = ok(json!({
        "type": "object",
        "id": "x", "$ref": "#/x", "dependencies": {}, "additionalItems": true,
        "patternProperties": {"a": {}}, "definitions": {"a": {}},
        "items": [{"type": "string"}]
    }));
    for w in [
        "id",
        "$ref",
        "dependencies",
        "additionalItems",
        "patternProperties",
        "definitions",
        "items",
    ] {
        assert!(
            got.iter()
                .any(|g| g.starts_with(&format!("{P}.{w}: Forbidden"))),
            "{w}\n{got:#?}"
        );
    }
    // definitions are walked and keyed
    let got = ok(json!({"definitions": {"a": {"id": "x"}}}));
    assert!(got
        .iter()
        .any(|g| g.starts_with(&format!("{P}.definitions[a].id: Forbidden"))));
}

#[test]
fn unique_items_and_preserve_unknown_false() {
    let got = ok(json!({"type": "array", "uniqueItems": true, "items": {"type": "string"}}));
    assert_errs(&got, &[&format!("{P}.uniqueItems: Forbidden")]);
    let got = ok(json!({"type": "object", "x-kubernetes-preserve-unknown-fields": false}));
    assert_errs(
        &got,
        &[&format!(
            "{P}.x-kubernetes-preserve-unknown-fields: Invalid value: false: must be true or undefined"
        )],
    );
}

#[test]
fn additional_properties_and_properties_are_mutually_exclusive() {
    let got = ok(json!({
        "type": "object", "properties": {"a": {"type": "string"}},
        "additionalProperties": {"type": "string"}
    }));
    assert_errs(
        &got,
        &[&format!(
            "{P}.additionalProperties: Forbidden: additionalProperties and properties are mutual exclusive"
        )],
    );
    assert!(ok(json!({"type": "object", "properties": {"a": {"type": "string"}}, "additionalProperties": true})).is_empty());
    assert!(ok(json!({"type": "object", "additionalProperties": {"type": "string"}})).is_empty());
}

#[test]
fn require_valid_property_type() {
    let schema = json!({"type": "object", "properties": {"a": {"type": "bogus"}}});
    let got = ok(schema.clone());
    assert_errs(
        &got,
        &[&format!(
            "{P}.properties[a].type: Unsupported value: \"bogus\""
        )],
    );
    let opts = OpenApiSchemaOptions {
        require_valid_property_type: false,
        ..Default::default()
    };
    assert!(run(schema, false, false, opts).is_empty());
}

#[test]
fn defaults_forbidden_inside_top_level_meta() {
    let got = ok(json!({"type": "object", "properties": {
        "metadata": {"type": "object", "default": {}, "properties": {"name": {"type": "string", "default": "x"}}},
        "kind": {"type": "string", "default": "k"},
        "spec": {"type": "object", "default": {}}
    }}));
    assert_errs(
        &got,
        &[
            &format!("{P}.properties[metadata].default: Forbidden: must not be set in top-level metadata"),
            &format!("{P}.properties[metadata].properties[name].default: Forbidden: must not be set in top-level metadata"),
            &format!("{P}.properties[kind].default: Forbidden: must not be set in top-level kind"),
        ],
    );
}

#[test]
fn embedded_resource_meta_and_additional_properties() {
    let got = ok(json!({"type": "object", "properties": {"emb": {
        "type": "object", "x-kubernetes-embedded-resource": true,
        "properties": {"metadata": {"type": "object", "properties": {
            "labels": {"type": "object", "additionalProperties": {"type": "string", "default": "x"}},
            "inner": {"type": "object", "x-kubernetes-embedded-resource": true}
        }}}
    }}}));
    assert_errs(
        &got,
        &[
            &format!("{P}.properties[emb].properties[metadata].properties[labels].additionalProperties.default: Forbidden: must not be set inside additionalProperties applying to object metadata"),
            &format!("{P}.properties[emb].properties[metadata].properties[inner].x-kubernetes-embedded-resource: Forbidden: must not be used inside of resource meta"),
        ],
    );
}

#[test]
fn list_type_extension_checks() {
    let got = ok(json!({"type": "string", "x-kubernetes-list-type": "set"}));
    assert_errs(
        &got,
        &[&format!(
            "{P}.type: Invalid value: \"string\": must be array if x-kubernetes-list-type is specified"
        )],
    );
    let got = ok(json!({"x-kubernetes-list-type": "set"}));
    assert_errs(
        &got,
        &[&format!(
            "{P}.type: Required value: must be array if x-kubernetes-list-type is specified"
        )],
    );
    let got = ok(
        json!({"type": "array", "items": {"type": "string"}, "x-kubernetes-list-type": "invalid"}),
    );
    assert_errs(
        &got,
        &[&format!(
            "{P}.x-kubernetes-list-type: Unsupported value: \"invalid\""
        )],
    );
    let got = ok(
        json!({"type": "array", "items": {"type": "string"}, "x-kubernetes-list-map-keys": ["a"], "x-kubernetes-list-type": "invalid"}),
    );
    assert_eq!(got.len(), 2, "{got:#?}");
    assert!(got.iter().any(|g| g.starts_with(&format!(
        "{P}.x-kubernetes-list-type: Invalid value: \"invalid\": must be map if x-kubernetes-list-map-keys is non-empty"
    ))));
    let got = ok(
        json!({"type": "array", "items": {"type": "string"}, "x-kubernetes-list-map-keys": ["a"]}),
    );
    assert_errs(
        &got,
        &[&format!(
            "{P}.x-kubernetes-list-type: Required value: must be map if x-kubernetes-list-map-keys is non-empty"
        )],
    );
}

#[test]
fn map_type_extension_checks() {
    let got = ok(json!({"type": "string", "x-kubernetes-map-type": "granular"}));
    assert_errs(
        &got,
        &[&format!(
            "{P}.type: Invalid value: \"string\": must be object if x-kubernetes-map-type is specified"
        )],
    );
    let got = ok(json!({"x-kubernetes-map-type": "atomic"}));
    assert_errs(
        &got,
        &[&format!(
            "{P}.type: Required value: must be object if x-kubernetes-map-type is specified"
        )],
    );
    let got = ok(json!({"type": "object", "x-kubernetes-map-type": "invalid"}));
    assert_errs(
        &got,
        &[&format!(
            "{P}.x-kubernetes-map-type: Unsupported value: \"invalid\""
        )],
    );
    assert!(ok(json!({"type": "object", "x-kubernetes-map-type": "granular"})).is_empty());
}

fn map_list(items: serde_json::Value, keys: serde_json::Value) -> serde_json::Value {
    json!({"type": "array", "x-kubernetes-list-type": "map", "x-kubernetes-list-map-keys": keys, "items": items})
}

#[test]
fn list_type_map_checks() {
    let item = |props: serde_json::Value| json!({"type": "object", "required": ["key"], "properties": props});
    // empty keys + missing items
    let got = ok(json!({"type": "array", "x-kubernetes-list-type": "map"}));
    assert_errs(
        &got,
        &[
            &format!("{P}.x-kubernetes-list-map-keys: Required value"),
            &format!("{P}.items: Required value"),
        ],
    );
    // array of schemas
    let got = ok(
        json!({"type": "array", "x-kubernetes-list-type": "map", "x-kubernetes-list-map-keys": ["a"], "items": [{"type": "object"}]}),
    );
    assert_errs(
        &got,
        &[
            &format!("{P}.items: Forbidden"),
            &format!("{P}.items: Invalid value:"),
        ],
    );
    // non object item
    let got = ok(map_list(json!({"type": "string"}), json!(["key"])));
    assert_errs(
        &got,
        &[&format!(
            "{P}.items.type: Invalid value: \"string\": must be object if parent array's x-kubernetes-list-type is map"
        )],
    );
    // key missing from properties
    let got = ok(map_list(
        item(json!({"other": {"type": "string"}})),
        json!(["key"]),
    ));
    assert_errs(
        &got,
        &[&format!(
            "{P}.x-kubernetes-list-map-keys: Invalid value: [\"key\"]: entries must all be names of item properties"
        )],
    );
    // non scalar key; upstream reports the parent items' type
    let got = ok(map_list(
        item(json!({"key": {"type": "object"}})),
        json!(["key"]),
    ));
    assert_errs(
        &got,
        &[&format!(
            "{P}.items.properties[key].type: Invalid value: \"object\": must be a scalar type if parent array's x-kubernetes-list-type is map"
        )],
    );
    // duplicate keys
    let got = ok(map_list(
        item(json!({"key": {"type": "string"}})),
        json!(["key", "key"]),
    ));
    assert_errs(
        &got,
        &[&format!(
            "{P}.x-kubernetes-list-map-keys: Invalid value: [\"key\",\"key\"]: must not contain duplicate entries"
        )],
    );
    // allowed
    assert!(ok(map_list(
        item(json!({"key": {"type": "string"}})),
        json!(["key"])
    ))
    .is_empty());
}

#[test]
fn map_list_keys_must_be_required_or_defaulted_and_not_nullable() {
    let schema = |item: serde_json::Value| map_list(item, json!(["key"]));
    let got = ok(schema(
        json!({"type": "object", "properties": {"key": {"type": "string"}}}),
    ));
    assert_errs(
        &got,
        &[&format!(
            "{P}.items.properties[key].default: Required value: this property is in x-kubernetes-list-map-keys, so it must have a default or be a required property"
        )],
    );
    assert!(ok(schema(
        json!({"type": "object", "properties": {"key": {"type": "string", "default": "a"}}})
    ))
    .is_empty());
    let got = ok(schema(
        json!({"type": "object", "required": ["key"], "properties": {"key": {"type": "string", "nullable": true}}}),
    ));
    assert_errs(
        &got,
        &[&format!(
            "{P}.items.properties[key].nullable: Forbidden: this property is in x-kubernetes-list-map-keys, so it cannot be nullable"
        )],
    );
    let got = ok(schema(
        json!({"type": "object", "nullable": true, "required": ["key"], "properties": {"key": {"type": "string"}}}),
    ));
    assert_errs(
        &got,
        &[&format!(
            "{P}.items.nullable: Forbidden: cannot be nullable when x-kubernetes-list-type is map"
        )],
    );
    let nullable_set = json!({"type": "array", "x-kubernetes-list-type": "set", "items": {"type": "string", "nullable": true}});
    let got = ok(nullable_set.clone());
    assert_errs(
        &got,
        &[&format!(
            "{P}.items.nullable: Forbidden: cannot be nullable when x-kubernetes-list-type is set"
        )],
    );
    // the ratchet turns the check off
    let opts = OpenApiSchemaOptions {
        require_map_list_keys_map_set_validation: false,
        ..Default::default()
    };
    assert!(run(nullable_set, false, false, opts).is_empty());
}

#[test]
fn list_type_set_requires_atomic_items() {
    let set = |items: serde_json::Value| json!({"type": "array", "x-kubernetes-list-type": "set", "items": items});
    assert!(ok(set(json!({"type": "string"}))).is_empty());
    assert!(ok(set(
        json!({"type": "object", "x-kubernetes-map-type": "atomic"})
    ))
    .is_empty());
    assert!(ok(set(json!({"type": "array", "items": {"type": "string"}}))).is_empty());
    assert!(ok(set(
        json!({"type": "array", "x-kubernetes-list-type": "atomic", "items": {"type": "string"}})
    ))
    .is_empty());
    for items in [
        json!({"type": "object", "x-kubernetes-map-type": "granular"}),
        json!({"type": "object"}),
    ] {
        let got = ok(set(items));
        assert_errs(
            &got,
            &[&format!(
                "{P}.items.x-kubernetes-map-type: Invalid value: null: must be atomic as item of a list with x-kubernetes-list-type=set"
            )],
        );
    }
    let got = ok(set(
        json!({"type": "array", "x-kubernetes-list-type": "set", "items": {"type": "string"}}),
    ));
    assert_errs(
        &got,
        &[&format!(
            "{P}.items.x-kubernetes-list-type: Invalid value: \"set\": must be atomic as item of a list with x-kubernetes-list-type=set"
        )],
    );
    // the ratchet turns the check off
    let opts = OpenApiSchemaOptions {
        require_atomic_set_type: false,
        ..Default::default()
    };
    assert!(run(set(json!({"type": "object"})), false, false, opts).is_empty());
}

#[test]
fn update_options_ratchet_on_persisted_violations() {
    let spec_with = |s: serde_json::Value| {
        let mut crd = CustomResourceDefinition::new("widget", "group.com", "Plural", "plural");
        crd.spec.versions = vec![serde_json::from_value(json!({
            "name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": s}
        }))
        .unwrap()];
        crd.spec
    };
    let clean = OpenApiSchemaOptions::for_update(&spec_with(json!({"type": "object"})));
    assert!(
        clean.require_valid_property_type
            && clean.require_atomic_set_type
            && clean.require_map_list_keys_map_set_validation
    );
    let o = OpenApiSchemaOptions::for_update(&spec_with(json!({"type": "bogus"})));
    assert!(!o.require_valid_property_type && o.require_atomic_set_type);
    let o = OpenApiSchemaOptions::for_update(&spec_with(
        json!({"type": "array", "x-kubernetes-list-type": "set", "items": {"type": "object"}}),
    ));
    assert!(!o.require_atomic_set_type && o.require_map_list_keys_map_set_validation);
    let o = OpenApiSchemaOptions::for_update(&spec_with(
        json!({"type": "array", "x-kubernetes-list-type": "set", "items": {"type": "string", "nullable": true}}),
    ));
    assert!(!o.require_map_list_keys_map_set_validation && o.require_atomic_set_type);
}

/// `validation.go:975-979`: `NewStructural` init errors overlap the walk and are
/// shown only when the walk (and `ValidateStructural`) reported nothing.
#[test]
fn structural_init_errors_hidden_when_walk_reports() {
    let s = json!({"type": "object", "properties": {"a": {"type": "string", "id": "x"}}});
    let got = run(s, false, true, Default::default());
    assert_errs(
        &got,
        &[&format!(
            "{P}.properties[a].id: Forbidden: id is not supported"
        )],
    );
}

#[test]
fn structural_init_errors_shown_when_walk_is_clean() {
    // `NewStructural` rejects a non-nil `$schema` (`convert.go`) which the walk
    // does not look at.
    let s = json!({"type": "object", "$schema": "http://json-schema.org/draft-04/schema#"});
    let got = run(s, false, true, Default::default());
    assert_eq!(got.len(), 1, "{got:#?}");
    assert!(
        got[0].starts_with(&format!("{P}: Invalid value: \"\": ")),
        "{got:#?}"
    );
}
