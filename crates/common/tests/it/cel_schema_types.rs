//! `common.SchemaDeclType` (schemas.go) over `JSONSchemaProps`, and the typed
//! CRD rule environment of the shadow checker run.

use rusternetes_cel_checker::DType;
use rusternetes_common::cel_checker_shadow::{check_expression, SelfScope};
use rusternetes_common::cel_schema_types::schema_decl_type;
use rusternetes_common::cel_versions::{EnvSet, EnvType};
use rusternetes_common::feature_gates::Version;
use rusternetes_common::resources::crd::JSONSchemaProps;
use serde_json::json;

fn schema(v: serde_json::Value) -> JSONSchemaProps {
    serde_json::from_value(v).unwrap()
}

fn errors(s: &JSONSchemaProps, root: bool, src: &str) -> Vec<String> {
    errors_at(s, root, src, 35, false)
}

fn errors_at(s: &JSONSchemaProps, root: bool, src: &str, minor: u32, opt: bool) -> Vec<String> {
    let scope = SelfScope {
        decl: schema_decl_type(s, root).expect("exposed"),
        optional_old_self: opt,
    };
    let set = EnvSet::must_base_env_set(Version::major_minor(1, minor));
    check_expression(src, &set, EnvType::NewExpressions, Some(&scope)).expect("parses")
}

fn widget() -> JSONSchemaProps {
    schema(json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "required": ["size"],
                "properties": {
                    "size": {"type": "integer"},
                    "ratio": {"type": "number"},
                    "name": {"type": "string"},
                    "blob": {"type": "string", "format": "byte"},
                    "ttl": {"type": "string", "format": "duration"},
                    "at": {"type": "string", "format": "date-time"},
                    "port": {"x-kubernetes-int-or-string": true},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "labels": {"type": "object", "additionalProperties": {"type": "string"}},
                    "opaque": {"type": "object", "x-kubernetes-preserve-unknown-fields": true},
                    "untyped": {"x-kubernetes-preserve-unknown-fields": true},
                    "bad-name": {"type": "string"},
                    "1up": {"type": "string"},
                    "in": {"type": "boolean"},
                    "embedded": {
                        "type": "object",
                        "x-kubernetes-embedded-resource": true,
                        "x-kubernetes-preserve-unknown-fields": true
                    }
                }
            }
        }
    }))
}

#[test]
fn scalar_and_format_types() {
    let w = widget();
    let spec = schema_decl_type(&w, true).unwrap();
    let fields = spec.fields().unwrap();
    let fields = fields["spec"].ty.fields().unwrap();
    let t = |n: &str| fields[n].ty.dtype();
    assert_eq!(t("size"), DType::Int);
    assert_eq!(t("ratio"), DType::Double);
    assert_eq!(t("name"), DType::Str);
    assert_eq!(t("blob"), DType::Bytes);
    assert_eq!(t("ttl"), DType::Duration);
    assert_eq!(t("at"), DType::Timestamp);
    assert_eq!(t("port"), DType::Dyn);
    assert_eq!(t("tags"), DType::List(Box::new(DType::Str)));
    assert_eq!(
        t("labels"),
        DType::Map(Box::new(DType::Str), Box::new(DType::Str))
    );
    // Not exposed: no type, and names that cannot be escaped.
    assert!(!fields.contains_key("untyped"));
    assert!(!fields.contains_key("1up"));
    // Escaped.
    assert!(fields.contains_key("bad__dash__name"));
    assert!(fields.contains_key("__in__"));
}

#[test]
fn arrays_without_items_and_maps_of_hidden_values_are_not_exposed() {
    assert!(schema_decl_type(&schema(json!({"type": "array"})), false).is_none());
    assert!(schema_decl_type(
        &schema(json!({"type": "array", "items": {"x-kubernetes-preserve-unknown-fields": true}})),
        false
    )
    .is_none());
    assert!(schema_decl_type(
        &schema(json!({"type": "object", "additionalProperties": {}})),
        false
    )
    .is_none());
    assert!(schema_decl_type(&schema(json!({})), false).is_none());
}

#[test]
fn rules_are_checked_against_the_schema() {
    let w = widget();
    let spec = &w.properties.as_ref().unwrap()["spec"];
    assert!(errors(spec, false, "self.size > 0 && self.ratio < 1.0").is_empty());
    assert!(errors(spec, false, "self.tags.all(t, t.size() > 0)").is_empty());
    assert!(errors(spec, false, "self.ttl < duration('1h')").is_empty());
    assert!(errors(spec, false, "self.at < timestamp('2030-01-01T00:00:00Z')").is_empty());
    assert!(errors(
        spec,
        false,
        "type(self.port) == int || self.port.matches('a')"
    )
    .is_empty());
    assert!(errors(spec, false, "self.bad__dash__name == 'x'").is_empty());
    assert!(errors(spec, false, "self.__in__").is_empty());
    assert_eq!(
        errors(spec, false, "self.missing > 1"),
        ["undefined field 'missing'"]
    );
    // `x-kubernetes-preserve-unknown-fields` does not expose unknown fields.
    assert_eq!(
        errors(spec, false, "self.opaque.anything"),
        ["undefined field 'anything'"]
    );
    assert_eq!(
        errors(spec, false, "self.size == 'a'"),
        ["found no matching overload for '_==_' applied to '(int, string)'"]
    );
    // Only `self` and `oldSelf` exist in a CRD rule environment.
    assert_eq!(
        errors(spec, false, "request.userInfo.username == 'x'"),
        ["undeclared reference to 'request' (in container '')"]
    );
}

#[test]
fn resource_roots_expose_the_object_meta_fields() {
    let w = widget();
    assert!(errors(&w, true, "self.apiVersion == 'v1' && self.kind == 'W'").is_empty());
    assert!(errors(&w, true, "self.metadata.name.startsWith('a')").is_empty());
    assert!(errors(&w, true, "self.metadata.generateName == ''").is_empty());
    assert_eq!(
        errors(&w, true, "self.metadata.labels"),
        ["undefined field 'labels'"]
    );
    // A non-root node does not get them.
    assert_eq!(
        errors(&w, false, "self.apiVersion == 'v1'"),
        ["undefined field 'apiVersion'"]
    );
    // An embedded resource node is a root (`IsXEmbeddedResource`).
    let spec = &w.properties.as_ref().unwrap()["spec"];
    let embedded = &spec.properties.as_ref().unwrap()["embedded"];
    assert!(errors(spec, false, "self.embedded.metadata.name == 'x'").is_empty());
    assert!(errors(embedded, true, "self.kind == 'x'").is_empty());
}

#[test]
fn old_self_follows_the_version_and_optional_old_self() {
    let w = widget();
    let spec = &w.properties.as_ref().unwrap()["spec"];
    assert!(errors(spec, false, "self.size >= oldSelf.size").is_empty());
    assert!(errors_at(spec, false, "oldSelf.hasValue()", 35, true).is_empty());
    assert!(!errors_at(spec, false, "oldSelf.hasValue()", 35, false).is_empty());
    // `oldSelf` is introduced at 1.24.
    assert!(!errors_at(spec, false, "oldSelf.size == 1", 23, false).is_empty());
}

/// `(min_serialized_size, max_elements)` of the schema's declaration.
fn sizes(v: serde_json::Value) -> (i64, i64) {
    let t = schema_decl_type(&schema(v), false).expect("exposed");
    (t.min_serialized_size, t.max_elements)
}

#[test]
fn string_sizes() {
    // MaxRequestSizeBytes (3 MiB) minus the quotes.
    assert_eq!(sizes(json!({"type": "string"})), (2, 3145726));
    // maxLength counts code points, up to 4 bytes each.
    assert_eq!(sizes(json!({"type": "string", "maxLength": 10})), (2, 40));
    assert_eq!(sizes(json!({"type": "string", "maxLength": -3})), (2, 0));
    // The longest enum value, when there is no maxLength.
    assert_eq!(
        sizes(json!({"type": "string", "enum": ["a", "bcd"]})),
        (2, 3)
    );
    assert_eq!(
        sizes(json!({"type": "string", "maxLength": 1, "enum": ["bcd"]})),
        (2, 4)
    );
}

#[test]
fn string_format_sizes() {
    assert_eq!(
        sizes(json!({"type": "string", "format": "byte"})),
        (2, 3145726)
    );
    assert_eq!(
        sizes(json!({"type": "string", "format": "byte", "maxLength": 5})),
        (2, 5)
    );
    assert_eq!(
        sizes(json!({"type": "string", "format": "duration"})),
        (3, 32)
    );
    assert_eq!(sizes(json!({"type": "string", "format": "date"})), (12, 12));
    assert_eq!(
        sizes(json!({"type": "string", "format": "date-time"})),
        (21, 32)
    );
}

#[test]
fn int_or_string_sizes() {
    assert_eq!(
        sizes(json!({"x-kubernetes-int-or-string": true})),
        (1, 3145726)
    );
    assert_eq!(
        sizes(json!({"x-kubernetes-int-or-string": true, "maxLength": 3})),
        (1, 12)
    );
}

#[test]
fn scalar_min_sizes() {
    assert_eq!(sizes(json!({"type": "boolean"})).0, 4);
    assert_eq!(sizes(json!({"type": "integer"})).0, 1);
    assert_eq!(sizes(json!({"type": "number"})).0, 1);
}

#[test]
fn list_and_map_sizes() {
    let items = json!({"type": "string"});
    // (3145728 - 2) / (min size 2 + 1)
    assert_eq!(
        sizes(json!({"type": "array", "items": items})),
        (2, 1048575)
    );
    assert_eq!(
        sizes(json!({"type": "array", "items": items, "maxItems": 7})),
        (2, 7)
    );
    assert_eq!(
        sizes(json!({"type": "array", "items": items, "maxItems": -1})),
        (2, 0)
    );
    // (3145728 - 2) / (min size 2 + 6)
    assert_eq!(
        sizes(json!({"type": "object", "additionalProperties": items})),
        (2, 393215)
    );
    assert_eq!(
        sizes(json!({"type": "object", "additionalProperties": items, "maxProperties": 4})),
        (2, 4)
    );
}

#[test]
fn object_min_size_counts_required_properties_without_defaults() {
    // 2 for {}, plus per required property: len(name) + min size + 4.
    assert_eq!(
        sizes(json!({
            "type": "object",
            "required": ["name", "n"],
            "properties": {
                "name": {"type": "string"},
                "n": {"type": "integer"},
                "optional": {"type": "string"}
            }
        }))
        .0,
        2 + (4 + 2 + 4) + (1 + 1 + 4)
    );
    // Defaults are filled in server-side, so they do not count.
    assert_eq!(
        sizes(json!({
            "type": "object",
            "required": ["name"],
            "properties": {"name": {"type": "string", "default": "x"}}
        }))
        .0,
        2
    );
}
