//! #3081: `JSONSchemaProps.items` (a `JSONSchemaPropsOrArray`) must survive a
//! protobuf roundtrip. Upstream marshals it as a message `{schema, jsonSchemas}`
//! on the wire (`apiextensions/v1/generated.proto`); JSON inlines the schema.
//! client-go negotiates protobuf for CRDs, so a lost `items.type` makes the
//! follow-up `Update` fail structural validation ("must not be empty for
//! specified array items").
use rusternetes_protobuf::ProtoRegistry;
use serde_json::json;

#[test]
fn crd_array_items_type_roundtrips() {
    let r = ProtoRegistry::new();
    let crd = json!({
        "metadata": {"name": "foos.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "foos", "kind": "Foo"},
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {
                    "type": "object",
                    "properties": {"bars": {
                        "type": "array",
                        "items": {"type": "object", "properties": {"n": {"type": "string"}}}
                    }}
                }}
            }]
        }
    });
    let bytes = r
        .encode_message("CustomResourceDefinition", &crd)
        .expect("encode");
    let back = r
        .decode_message("CustomResourceDefinition", &bytes)
        .expect("decode");
    let items = back
        .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/bars/items")
        .cloned()
        .unwrap_or(json!(null));
    let ty = items
        .get("type")
        .or_else(|| items.pointer("/schema/type"))
        .cloned();
    assert_eq!(ty, Some(json!("object")), "items after roundtrip: {back}");
}

/// The sibling unions share the bug: `additionalProperties` (bool or schema)
/// and the tuple form of `items` must survive too.
#[test]
fn crd_additional_properties_and_tuple_items_roundtrip() {
    let r = ProtoRegistry::new();
    let schema = |props: serde_json::Value| {
        json!({
            "metadata": {"name": "foos.example.com"},
            "spec": {
                "group": "example.com", "scope": "Namespaced",
                "names": {"plural": "foos", "kind": "Foo"},
                "versions": [{"name": "v1", "served": true, "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object", "properties": props}}}]
            }
        })
    };
    let crd = schema(json!({
        "closed": {"type": "object", "additionalProperties": false},
        "map": {"type": "object", "additionalProperties": {"type": "string"}},
        "tuple": {"type": "array", "items": [{"type": "string"}, {"type": "integer"}]}
    }));
    let bytes = r.encode_message("CustomResourceDefinition", &crd).unwrap();
    let back = r
        .decode_message("CustomResourceDefinition", &bytes)
        .unwrap();
    let p = |k: &str| {
        back.pointer(&format!(
            "/spec/versions/0/schema/openAPIV3Schema/properties/{k}"
        ))
        .cloned()
        .unwrap()
    };
    assert_eq!(
        p("closed")["additionalProperties"],
        json!({"allows": false})
    );
    assert_eq!(
        p("map")["additionalProperties"]["schema"]["type"],
        json!("string")
    );
    assert_eq!(
        p("tuple")["items"]["jSONSchemas"][1]["type"],
        json!("integer")
    );
}
