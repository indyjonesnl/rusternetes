//! #2946: explicit zero numeric validations in a CRD schema must survive into
//! the published OpenAPI document.
//!
//! Upstream these are pointers with omitempty, so only nil is omitted and an
//! explicit 0 is serialised:
//! staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/types_jsonschema.go:80-94
//! (`Maximum *float64`, `MaxLength *int64`, `MaxItems *int64`, ...).

use rusternetes_api_server::handlers::openapi::{
    build_swagger_spec_for_crds, strip_false_extensions,
};
use serde_json::json;

const ZERO_KEYS: [&str; 9] = [
    "maximum",
    "minimum",
    "multipleOf",
    "maxLength",
    "minLength",
    "maxItems",
    "minItems",
    "maxProperties",
    "minProperties",
];

#[test]
fn explicit_zero_validations_are_not_stripped() {
    let mut schema = json!({"type": "object"});
    for k in ZERO_KEYS {
        schema[k] = json!(0);
    }
    strip_false_extensions(&mut schema);
    for k in ZERO_KEYS {
        assert_eq!(schema.get(k), Some(&json!(0)), "{k}: explicit 0 dropped");
    }
}

#[test]
fn crd_with_explicit_zeros_publishes_them_in_v2() {
    let crd = json!({
        "metadata": {"name": "foos.example.com"},
        "spec": {
            "group": "example.com",
            "names": {"plural": "foos", "kind": "Foo"},
            "scope": "Namespaced",
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {
                    "type": "object",
                    "properties": {
                        "s": {"type": "string", "minLength": 0, "maxLength": 0},
                        "a": {"type": "array", "items": {"type": "string"}, "minItems": 0, "maxItems": 0},
                        "o": {"type": "object", "minProperties": 0, "maxProperties": 0},
                        "n": {"type": "number", "minimum": 0, "maximum": 0, "multipleOf": 0}
                    }
                }}
            }]
        }
    });
    let spec = build_swagger_spec_for_crds(&[crd]);
    let defs = spec["definitions"].as_object().unwrap();
    let def = defs
        .values()
        .find(|d| d.pointer("/properties/s").is_some())
        .unwrap();
    for (prop, keys) in [
        ("s", vec!["minLength", "maxLength"]),
        ("a", vec!["minItems", "maxItems"]),
        ("o", vec!["minProperties", "maxProperties"]),
        ("n", vec!["minimum", "maximum", "multipleOf"]),
    ] {
        for k in keys {
            assert_eq!(
                def.pointer(&format!("/properties/{prop}/{k}")),
                Some(&json!(0)),
                "{prop}.{k}"
            );
        }
    }
}
