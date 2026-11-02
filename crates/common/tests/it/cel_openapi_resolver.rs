//! `DefinitionsSchemaResolver` / `PopulateRefs`
//! (`cel/openapi/resolver/{definitions,refs}.go`).

use rusternetes_common::cel_openapi_resolver::{
    populate_refs, DefinitionsSchemaResolver, Gvk, ResolveError,
};
use rusternetes_common::resources::crd::{JSONSchemaProps, JSONSchemaPropsOrArray};
use serde_json::{json, Map, Value};

fn defs() -> Map<String, Value> {
    json!({
        "io.k8s.Widget": {
            "type": "object",
            "x-kubernetes-group-version-kind": [
                {"group": "example.io", "version": "v1", "kind": "Widget"}
            ],
            "properties": {
                "spec": {"$ref": "#/definitions/io.k8s.WidgetSpec"},
                "wrapped": {"description": "d", "allOf": [{"$ref": "#/definitions/io.k8s.Leaf"}]},
                "list": {"type": "array", "items": {"$ref": "#/definitions/io.k8s.Leaf"}},
                "map": {"type": "object",
                        "additionalProperties": {"$ref": "#/definitions/io.k8s.Leaf"}},
                "open": {"type": "object", "additionalProperties": true}
            }
        },
        "io.k8s.WidgetSpec": {
            "type": "object",
            "properties": {
                "leaf": {"$ref": "#/definitions/io.k8s.Leaf"},
                "parent": {"$ref": "#/definitions/io.k8s.Node"}
            }
        },
        "io.k8s.Node": {
            "type": "object",
            "properties": {"next": {"$ref": "#/definitions/io.k8s.Node"},
                           "n": {"type": "integer"}}
        },
        "io.k8s.Leaf": {"type": "string"},
        "io.k8s.Broken": {
            "type": "object",
            "x-kubernetes-group-version-kind": [
                {"group": "example.io", "version": "v1", "kind": "Broken"}
            ],
            "properties": {"x": {"$ref": "#/definitions/io.k8s.Missing"}}
        },
        "io.k8s.BadGvk": {
            "x-kubernetes-group-version-kind": [{"group": "g", "version": "v"}]
        }
    })
    .as_object()
    .unwrap()
    .clone()
}

fn prop<'a>(s: &'a JSONSchemaProps, name: &str) -> &'a JSONSchemaProps {
    &s.properties.as_ref().unwrap()[name]
}

#[test]
fn refs_are_populated_everywhere() {
    let d = defs();
    let r = DefinitionsSchemaResolver::new(&d);
    let s = r
        .resolve_schema(&Gvk::new("example.io", "v1", "Widget"))
        .unwrap();
    assert_eq!(
        prop(prop(&s, "spec"), "leaf").type_.as_deref(),
        Some("string")
    );
    // `allOf: [{$ref}]` is a wrapped ref (`refOf`).
    assert_eq!(prop(&s, "wrapped").type_.as_deref(), Some("string"));
    let Some(JSONSchemaPropsOrArray::Schema(items)) = prop(&s, "list").items.as_deref() else {
        panic!()
    };
    assert_eq!(items.type_.as_deref(), Some("string"));
    assert!(prop(&s, "map").additional_properties.is_some());
    assert!(prop(&s, "open").additional_properties.is_some());
    assert!(s
        .properties
        .as_ref()
        .unwrap()
        .values()
        .all(|p| p.ref_path.is_none()));
}

#[test]
fn a_circular_ref_becomes_an_empty_object() {
    let d = defs();
    let r = DefinitionsSchemaResolver::new(&d);
    let s = r
        .resolve_schema(&Gvk::new("example.io", "v1", "Widget"))
        .unwrap();
    let node = prop(prop(&s, "spec"), "parent");
    assert_eq!(node.type_.as_deref(), Some("object"));
    // Node.next refers to Node, already being resolved: a bare object.
    let next = prop(node, "next");
    assert_eq!(next.type_.as_deref(), Some("object"));
    assert!(next.properties.is_none());
    assert_eq!(prop(node, "n").type_.as_deref(), Some("integer"));
}

#[test]
fn unknown_gvks_and_refs_are_errors() {
    let d = defs();
    let r = DefinitionsSchemaResolver::new(&d);
    assert!(matches!(
        r.resolve_schema(&Gvk::new("example.io", "v1", "Nope")),
        Err(ResolveError::NotFound(_))
    ));
    // A malformed GVK extension registers nothing (`extensionsToGVKs`).
    assert!(matches!(
        r.resolve_schema(&Gvk::new("g", "v", "")),
        Err(ResolveError::NotFound(_))
    ));
    assert!(matches!(
        r.resolve_schema(&Gvk::new("example.io", "v1", "Broken")),
        Err(ResolveError::NotFound(_))
    ));
    assert!(matches!(
        populate_refs(&|_| None, "x"),
        Err(ResolveError::NotFound(_))
    ));
}
