//! Ports of `schema/validation_test.go` (release-1.35), plus `NewStructural`
//! error cases (convert.go) and end-to-end `ValidateStructural` cases for the
//! invariants that upstream exercises through the CRD validation tests.
//!
//! Upstream's two fuzz-driven tests (`TestValidateStructuralMetadataInvariants`,
//! `TestValidateNestedValueValidationComplete`) use reflection over the Go
//! struct fields; here every field is enumerated explicitly, which catches the
//! same "forgot to check a field" regression.

use serde_json::json;

use super::*;

type Mutation<T> = Vec<(&'static str, fn(&mut T))>;

fn schema(v: serde_json::Value) -> JSONSchemaProps {
    serde_json::from_value(v).expect("valid JSONSchemaProps")
}

fn structural(v: serde_json::Value) -> Structural {
    new_structural(&schema(v)).expect("convertible")
}

fn msgs(errs: &[Error]) -> Vec<String> {
    errs.iter().map(|e| e.to_string()).collect()
}

fn root() -> Path {
    Path::new("spec")
        .child("versions")
        .index(0)
        .child("schema")
        .child("openAPIV3Schema")
}

fn typed(t: &str) -> Structural {
    Structural {
        generic: Generic {
            type_: t.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

// ---- TestValidateStructuralMetadataInvariants ------------------------------

#[test]
fn metadata_type_must_be_object() {
    for t in ["object", "array", "number", "integer", "boolean", "string"] {
        let errs =
            validate_structural_metadata_invariants(&typed(t), true, Level::Root, &Path::default());
        assert_eq!(errs.is_empty(), t == "object", "{t}: {errs:?}");
    }
}

#[test]
fn metadata_only_name_and_generate_name_allowed() {
    // Upstream iterates `reflect.TypeOf(metav1.ObjectMeta{})` Go field names
    // (capitalised, so its name/generateName arm never matches); the JSON
    // names are what a schema carries.
    let fields = [
        "name",
        "generateName",
        "namespace",
        "selfLink",
        "uid",
        "resourceVersion",
        "generation",
        "creationTimestamp",
        "deletionTimestamp",
        "deletionGracePeriodSeconds",
        "labels",
        "annotations",
        "ownerReferences",
        "finalizers",
        "managedFields",
    ];
    for property in fields {
        let mut s = typed("object");
        s.properties.insert(property.into(), Structural::default());
        let errs = validate_structural_metadata_invariants(&s, true, Level::Root, &Path::default());
        let allowed = property == "name" || property == "generateName";
        assert_eq!(errs.is_empty(), allowed, "{property}: {errs:?}");
    }
}

#[test]
fn metadata_anything_but_type_and_properties_is_forbidden() {
    let mutations: Mutation<Structural> = vec![
        ("items", |s| s.items = Some(Box::default())),
        ("additionalProperties", |s| {
            s.additional_properties = Some(Box::default())
        }),
        ("description", |s| s.generic.description = "d".into()),
        ("title", |s| s.generic.title = "t".into()),
        ("nullable", |s| s.generic.nullable = true),
        ("x-preserve-unknown-fields", |s| {
            s.extensions.x_preserve_unknown_fields = true
        }),
        ("x-embedded-resource", |s| {
            s.extensions.x_embedded_resource = true
        }),
        ("x-int-or-string", |s| s.extensions.x_int_or_string = true),
        ("x-list-map-keys", |s| {
            s.extensions.x_list_map_keys = vec!["k".into()]
        }),
        ("x-list-type", |s| {
            s.extensions.x_list_type = Some("set".into())
        }),
        ("x-map-type", |s| {
            s.extensions.x_map_type = Some("atomic".into())
        }),
        ("x-validations", |s| {
            s.validation_extensions.x_validations = vec![json!({"rule": "true"})]
        }),
        ("value validation", |s| {
            s.value_validation = Some(ValueValidation {
                format: "f".into(),
                ..Default::default()
            })
        }),
    ];
    // the baseline (no mutation) is valid, and `default` is ignored
    let mut base = typed("object");
    base.properties.insert("name".into(), Structural::default());
    base.properties
        .insert("generateName".into(), Structural::default());
    assert!(
        validate_structural_metadata_invariants(&base, true, Level::Root, &Path::default())
            .is_empty()
    );
    let mut with_default = base.clone();
    with_default.generic.default = Some(json!(42.0));
    assert!(validate_structural_metadata_invariants(
        &with_default,
        true,
        Level::Root,
        &Path::default()
    )
    .is_empty());

    for (name, mutate) in mutations {
        let mut s = base.clone();
        mutate(&mut s);
        let errs = validate_structural_metadata_invariants(&s, true, Level::Root, &Path::default());
        assert!(!errs.is_empty(), "expected errors for {name}");
    }
}

// ---- TestValidateStructuralCompleteness ------------------------------------

fn string_with_min_length() -> NestedValueValidation {
    NestedValueValidation {
        value_validation: ValueValidation {
            min_length: Some(2),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn all_of(n: NestedValueValidation) -> Option<ValueValidation> {
    Some(ValueValidation {
        all_of: vec![n],
        ..Default::default()
    })
}

struct Completeness {
    name: &'static str,
    schema: Structural,
    options: ValidationOptions,
    error: &'static str,
}

#[test]
fn structural_completeness() {
    let object_with_bar = || Structural {
        generic: Generic {
            type_: "object".into(),
            ..Default::default()
        },
        properties: [("bar".to_string(), typed("string"))].into(),
        ..Default::default()
    };
    let ap_structure = || {
        Some(Box::new(StructuralOrBool {
            structural: Some(object_with_bar()),
            bool_: false,
        }))
    };
    let foo_min_length = || NestedValueValidation {
        properties: [("foo".to_string(), string_with_min_length())].into(),
        ..Default::default()
    };

    let cases = vec![
        Completeness {
            name: "allowed properties valuevalidation, additional properties structure",
            schema: Structural {
                additional_properties: ap_structure(),
                value_validation: all_of(foo_min_length()),
                ..Default::default()
            },
            options: ValidationOptions {
                allow_validation_properties_with_additional_properties: true,
                ..Default::default()
            },
            error: "",
        },
        Completeness {
            name: "disallowed properties valuevalidation, additional properties structure",
            schema: Structural {
                additional_properties: ap_structure(),
                value_validation: all_of(foo_min_length()),
                ..Default::default()
            },
            options: ValidationOptions::default(),
            error: "properties[foo]: Required value: because it is defined in allOf[0].properties[foo]",
        },
        Completeness {
            name: "disallowed additionalproperties valuevalidation, properties structure",
            schema: Structural {
                properties: [("bar".to_string(), typed("string"))].into(),
                value_validation: all_of(NestedValueValidation {
                    additional_properties: Some(Box::new(string_with_min_length())),
                    ..Default::default()
                }),
                ..Default::default()
            },
            options: ValidationOptions {
                allow_nested_additional_properties: true,
                ..Default::default()
            },
            error: "additionalProperties: Required value: because it is defined in allOf[0].additionalProperties",
        },
        Completeness {
            name: "allowed property in valuevalidation, and in structure",
            schema: Structural {
                properties: [("foo".to_string(), typed("string"))].into(),
                value_validation: all_of(foo_min_length()),
                ..Default::default()
            },
            options: ValidationOptions::default(),
            error: "",
        },
        Completeness {
            name: "disallowed property in valuevalidation, and in structure",
            schema: Structural {
                properties: [("foo".to_string(), typed("string"))].into(),
                value_validation: all_of(NestedValueValidation {
                    properties: [("notfoo".to_string(), string_with_min_length())].into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            options: ValidationOptions::default(),
            error: "properties[notfoo]: Required value: because it is defined in allOf[0].properties[notfoo]",
        },
        Completeness {
            name: "allowed items in valuevalidation, and in structure",
            schema: Structural {
                generic: Generic {
                    type_: "array".into(),
                    ..Default::default()
                },
                items: Some(Box::new(typed("string"))),
                value_validation: all_of(NestedValueValidation {
                    items: Some(Box::new(string_with_min_length())),
                    ..Default::default()
                }),
                ..Default::default()
            },
            options: ValidationOptions::default(),
            error: "",
        },
        Completeness {
            name: "disallowed items in valuevalidation, and not in structure",
            schema: Structural {
                generic: Generic {
                    type_: "object".into(),
                    ..Default::default()
                },
                properties: [("foo".to_string(), typed("string"))].into(),
                value_validation: all_of(NestedValueValidation {
                    items: Some(Box::new(string_with_min_length())),
                    ..Default::default()
                }),
                ..Default::default()
            },
            options: ValidationOptions::default(),
            error: "items: Required value: because it is defined in allOf[0].items",
        },
    ];

    for tc in cases {
        let errs = validate_structural_completeness(Some(&tc.schema), &Path::default(), tc.options);
        if tc.error.is_empty() {
            assert!(errs.is_empty(), "{}: unexpected errors: {errs:?}", tc.name);
        } else {
            assert!(
                msgs(&errs).iter().any(|m| m.contains(tc.error)),
                "{}: expected {:?}, got {errs:?}",
                tc.name,
                tc.error
            );
        }
    }
}

// ---- TestValidateNestedValueValidationComplete -----------------------------

fn nested_errs(v: &NestedValueValidation, opts: ValidationOptions) -> Vec<Error> {
    validate_nested_value_validation(Some(v), false, false, Level::Field, &Path::default(), opts)
}

#[test]
fn nested_value_validation_every_forbidden_generic_is_checked() {
    let mutations: Mutation<Generic> = vec![
        ("description", |g| g.description = "d".into()),
        ("type", |g| g.type_ = "string".into()),
        ("title", |g| g.title = "t".into()),
        ("default", |g| g.default = Some(json!(42.0))),
        ("nullable", |g| g.nullable = true),
    ];
    for (name, mutate) in mutations {
        let mut vv = NestedValueValidation::default();
        mutate(&mut vv.forbidden_generics);
        assert!(
            !nested_errs(&vv, ValidationOptions::default()).is_empty(),
            "expected ForbiddenGenerics errors for {name}"
        );
    }
}

#[test]
fn nested_value_validation_every_forbidden_extension_is_checked() {
    let mutations: Mutation<Extensions> = vec![
        ("x-preserve-unknown-fields", |x| {
            x.x_preserve_unknown_fields = true
        }),
        ("x-embedded-resource", |x| x.x_embedded_resource = true),
        ("x-int-or-string", |x| x.x_int_or_string = true),
        ("x-list-map-keys", |x| x.x_list_map_keys = vec!["k".into()]),
        ("x-list-type", |x| x.x_list_type = Some("set".into())),
        ("x-map-type", |x| x.x_map_type = Some("atomic".into())),
    ];
    for (name, mutate) in mutations {
        let mut vv = NestedValueValidation::default();
        mutate(&mut vv.forbidden_extensions);
        assert!(
            !nested_errs(&vv, ValidationOptions::default()).is_empty(),
            "expected ForbiddenExtensions errors for {name}"
        );
    }
}

#[test]
fn nested_value_validation_options_gate_x_validations_and_additional_properties() {
    for allow_x in [false, true] {
        for allow_ap in [false, true] {
            let opts = ValidationOptions {
                allow_nested_x_validations: allow_x,
                allow_nested_additional_properties: allow_ap,
                ..Default::default()
            };

            let vv = NestedValueValidation {
                validation_extensions: ValidationExtensions {
                    x_validations: vec![json!({"rule": "true"})],
                },
                ..Default::default()
            };
            assert_eq!(nested_errs(&vv, opts).is_empty(), allow_x, "x-validations");

            let vv = NestedValueValidation {
                additional_properties: Some(Box::default()),
                ..Default::default()
            };
            assert_eq!(
                nested_errs(&vv, opts).is_empty(),
                allow_ap,
                "additionalProperties"
            );
        }
    }
}

// ---- ValidateStructural end to end -----------------------------------------

#[test]
fn a_plain_structural_schema_has_no_errors() {
    let s = structural(json!({
        "type": "object",
        "properties": {
            "spec": {"type": "object", "properties": {
                "replicas": {"type": "integer", "minimum": 0},
                "tags": {"type": "array", "items": {"type": "string", "pattern": "^a+$"}},
                "port": {"x-kubernetes-int-or-string": true,
                         "anyOf": [{"type": "integer"}, {"type": "string"}]},
                "port2": {"x-kubernetes-int-or-string": true,
                          "allOf": [{"anyOf": [{"type": "integer"}, {"type": "string"}]},
                                    {"minimum": 1}]},
                "free": {"x-kubernetes-preserve-unknown-fields": true},
                "m": {"type": "object", "additionalProperties": {"type": "string"}},
            }}
        }
    }));
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        Vec::<String>::new()
    );
}

#[test]
fn root_invariants() {
    let s = structural(json!({}));
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        ["spec.versions[0].schema.openAPIV3Schema.type: Required value: must not be empty at the root"]
    );

    let s = structural(json!({"type": "string"}));
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        [
            r#"spec.versions[0].schema.openAPIV3Schema.type: Invalid value: "string": must be object at the root"#
        ]
    );

    let s = structural(json!({"type": "object", "additionalProperties": {"type": "string"}}));
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        ["spec.versions[0].schema.openAPIV3Schema.additionalProperties: Forbidden: must not be used at the root"]
    );
}

#[test]
fn array_needs_items_and_nested_types_are_required() {
    let s = structural(json!({
        "type": "object",
        "properties": {
            "a": {"type": "array"},
            "b": {"type": "array", "items": {}},
            "c": {"description": "no type"},
        }
    }));
    let p = "spec.versions[0].schema.openAPIV3Schema.properties";
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        [
            format!("{p}[a].items: Required value: must be specified"),
            format!(
                "{p}[b].items.type: Required value: must not be empty for specified array items"
            ),
            format!("{p}[c].type: Required value: must not be empty for specified object fields"),
        ]
    );
}

#[test]
fn nested_contexts_may_not_carry_structure() {
    let s = structural(json!({
        "type": "object",
        "properties": {"a": {"type": "string",
            "anyOf": [{"type": "string", "description": "d"}, {"type": "integer"}],
            "not": {"nullable": true}}}
    }));
    let p = "spec.versions[0].schema.openAPIV3Schema.properties[a]";
    let got = msgs(&validate_structural(&root(), &s));
    assert!(
        got.contains(&format!(
            "{p}.anyOf[0].type: Forbidden: must be empty to be structural"
        )),
        "{got:?}"
    );
    assert!(
        got.contains(&format!(
            "{p}.anyOf[0].description: Forbidden: must be empty to be structural"
        )),
        "{got:?}"
    );
    assert!(
        got.contains(&format!(
            "{p}.anyOf[1].type: Forbidden: must be empty to be structural"
        )),
        "{got:?}"
    );
    assert!(
        got.contains(&format!(
            "{p}.not.nullable: Forbidden: must be false to be structural"
        )),
        "{got:?}"
    );
}

#[test]
fn int_or_string_exceptions_are_only_the_two_patterns() {
    // `anyOf` of only integer is not the pattern
    let s = structural(json!({
        "type": "object",
        "properties": {"a": {"x-kubernetes-int-or-string": true,
                             "anyOf": [{"type": "integer"}]}}
    }));
    assert!(!validate_structural(&root(), &s).is_empty());

    // int-or-string excludes preserve-unknown-fields and embedded-resource
    let s = structural(json!({
        "type": "object",
        "properties": {"a": {"x-kubernetes-int-or-string": true,
                             "x-kubernetes-preserve-unknown-fields": true}}
    }));
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        ["spec.versions[0].schema.openAPIV3Schema.properties[a].x-kubernetes-preserve-unknown-fields: Invalid value: true: must be false if x-kubernetes-int-or-string is true"]
    );
}

#[test]
fn embedded_resource_invariants() {
    let p = "spec.versions[0].schema.openAPIV3Schema.properties[e]";
    let s = structural(json!({
        "type": "object",
        "properties": {"e": {"x-kubernetes-embedded-resource": true}}
    }));
    let got = msgs(&validate_structural(&root(), &s));
    assert!(
        got.contains(&format!(
            "{p}.type: Required value: must be object if x-kubernetes-embedded-resource is true"
        )),
        "{got:?}"
    );
    assert!(
        got.contains(&format!("{p}.properties: Required value: must not be empty if x-kubernetes-embedded-resource is true without x-kubernetes-preserve-unknown-fields")),
        "{got:?}"
    );

    let s = structural(json!({
        "type": "object",
        "properties": {"e": {"type": "string", "x-kubernetes-embedded-resource": true,
                             "x-kubernetes-preserve-unknown-fields": true}}
    }));
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        [format!(
            r#"{p}.type: Invalid value: "string": must be object if x-kubernetes-embedded-resource is true"#
        )]
    );

    let s = structural(json!({
        "type": "object",
        "properties": {"e": {"type": "object", "x-kubernetes-embedded-resource": true,
                             "x-kubernetes-preserve-unknown-fields": true,
                             "additionalProperties": {"type": "string"}}}
    }));
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        [format!(
            "{p}.additionalProperties: Forbidden: must not be used if x-kubernetes-embedded-resource is set"
        )]
    );
}

#[test]
fn root_metadata_may_only_restrict_name_and_generate_name() {
    let p = "spec.versions[0].schema.openAPIV3Schema.properties[metadata]";
    let ok = structural(json!({
        "type": "object",
        "properties": {"metadata": {"type": "object",
            "properties": {"name": {"type": "string", "maxLength": 10}}}}
    }));
    // the name subschema itself is dropped before the comparison (:208-212)
    assert!(validate_structural(&root(), &ok).is_empty());

    let bare = structural(json!({
        "type": "object",
        "properties": {"metadata": {"type": "object",
            "properties": {"name": {"type": "string"}, "generateName": {"type": "string"}}}}
    }));
    assert!(validate_structural(&root(), &bare).is_empty());

    let bad = structural(json!({
        "type": "object",
        "properties": {"metadata": {"type": "object",
            "properties": {"labels": {"type": "object"}}}}
    }));
    assert_eq!(
        msgs(&validate_structural(&root(), &bad)),
        [format!("{p}: Forbidden: must not specify anything other than name and generateName, but metadata is implicitly specified")]
    );

    let nested = structural(json!({
        "type": "object",
        "properties": {"a": {"type": "object",
            "allOf": [{"properties": {"metadata": {}}}],
            "properties": {"metadata": {"type": "object"}}}}
    }));
    let got = msgs(&validate_structural(&root(), &nested));
    assert!(
        got.iter().any(|m| m.contains(
            "allOf[0].properties[metadata]: Forbidden: must not be specified in a nested context"
        )),
        "{got:?}"
    );
}

#[test]
fn kind_and_api_version_must_be_strings_at_the_root() {
    let s = structural(json!({
        "type": "object",
        "properties": {"kind": {"type": "integer"}, "apiVersion": {"type": "boolean"}}
    }));
    let p = "spec.versions[0].schema.openAPIV3Schema.properties";
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        [
            format!(r#"{p}[apiVersion].type: Invalid value: "boolean": must be string"#),
            format!(r#"{p}[kind].type: Invalid value: "integer": must be string"#),
        ]
    );
}

#[test]
fn pattern_must_compile() {
    let s = structural(json!({
        "type": "object",
        "properties": {"a": {"type": "string", "pattern": "("}}
    }));
    let got = msgs(&validate_structural(&root(), &s));
    assert_eq!(got.len(), 1, "{got:?}");
    assert!(got[0].starts_with(
        r#"spec.versions[0].schema.openAPIV3Schema.properties[a].pattern: Invalid value: "(": must be a valid regular expression, but isn't: "#
    ));
}

#[test]
fn completeness_through_validate_structural() {
    let s = structural(json!({
        "type": "object",
        "properties": {"a": {"type": "string"}},
        "allOf": [{"properties": {"b": {"minLength": 1}}}]
    }));
    assert_eq!(
        msgs(&validate_structural(&root(), &s)),
        ["spec.versions[0].schema.openAPIV3Schema.properties[b]: Required value: because it is defined in spec.versions[0].schema.openAPIV3Schema.allOf[0].properties[b]"]
    );
}

#[test]
fn errors_are_sorted_by_message() {
    let s = structural(json!({
        "type": "object",
        "properties": {"z": {"type": "array"}, "a": {"type": "array"}}
    }));
    let got = msgs(&validate_structural(&root(), &s));
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted);
    assert_eq!(got.len(), 2);
}

// ---- NewStructural (convert.go) --------------------------------------------

#[test]
fn new_structural_rejects_unsupported_fields() {
    let cases = [
        (json!({"id": "x"}), "OpenAPIV3Schema 'id' is not supported"),
        (
            json!({"$schema": "x"}),
            "OpenAPIV3Schema 'schema' is not supported",
        ),
        (
            json!({"$ref": "#/x"}),
            "OpenAPIV3Schema '$ref' is not supported",
        ),
        (
            json!({"patternProperties": {"a": {}}}),
            "OpenAPIV3Schema 'patternProperties' is not supported",
        ),
        (
            json!({"dependencies": {"a": ["b"]}}),
            "OpenAPIV3Schema 'dependencies' is not supported",
        ),
        (
            json!({"additionalItems": true}),
            "OpenAPIV3Schema 'additionalItems' is not supported",
        ),
        (
            json!({"definitions": {"a": {}}}),
            "OpenAPIV3Schema 'definitions' is not supported",
        ),
        (
            json!({"items": [{"type": "string"}]}),
            "OpenAPIV3Schema 'items' must be a schema, but is an array",
        ),
        (
            json!({"x-kubernetes-preserve-unknown-fields": false}),
            "internal error: 'x-kubernetes-preserve-unknown-fields' must be true or undefined",
        ),
        // the same checks apply inside the nested value validations
        (
            json!({"allOf": [{"$ref": "#/x"}]}),
            "OpenAPIV3Schema '$ref' is not supported",
        ),
        (
            json!({"properties": {"a": {"not": {"id": "x"}}}}),
            "OpenAPIV3Schema 'id' is not supported",
        ),
    ];
    for (v, want) in cases {
        assert_eq!(new_structural(&schema(v.clone())).unwrap_err(), want, "{v}");
    }
}

#[test]
fn new_structural_converts_the_structure() {
    let s = structural(json!({
        "type": "object",
        "description": "d",
        "nullable": true,
        "default": {"a": 1},
        "x-kubernetes-preserve-unknown-fields": true,
        "properties": {"a": {"type": "array", "items": {"type": "string"}}},
        "additionalProperties": true,
        "minProperties": 1,
        "required": ["a"],
        "enum": [{"a": 1}],
        "allOf": [{"anyOf": [{"type": "integer"}]}],
    }));
    assert_eq!(s.generic.type_, "object");
    assert_eq!(s.generic.description, "d");
    assert!(s.generic.nullable);
    assert_eq!(s.generic.default, Some(json!({"a": 1})));
    assert!(s.extensions.x_preserve_unknown_fields);
    assert_eq!(
        s.properties["a"].items.as_deref().unwrap().generic.type_,
        "string"
    );
    let ap = s.additional_properties.as_deref().unwrap();
    assert!(ap.bool_ && ap.structural.is_none());
    let vv = s.value_validation.as_ref().unwrap();
    assert_eq!(vv.min_properties, Some(1));
    assert_eq!(vv.required, ["a"]);
    assert_eq!(vv.enum_, [json!({"a": 1})]);
    assert_eq!(
        vv.all_of[0].value_validation.any_of[0]
            .forbidden_generics
            .type_,
        "integer"
    );

    let s = structural(json!({"type": "object", "additionalProperties": {"type": "string"}}));
    let ap = s.additional_properties.as_deref().unwrap();
    assert!(ap.bool_);
    assert_eq!(ap.structural.as_ref().unwrap().generic.type_, "string");

    let s = structural(json!({"type": "object", "additionalProperties": false}));
    let ap = s.additional_properties.as_deref().unwrap();
    assert!(!ap.bool_ && ap.structural.is_none());
}
