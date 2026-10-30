//! Unit tests moved from `src/fieldmanager/schema.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::validation::structural::{Structural, StructuralOrBool};
    pub use serde_json::{Map, Value};
    pub use std::collections::BTreeMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::fieldmanager::schema::*;
    use serde_json::json;

    fn schemas() -> Map<String, Value> {
        json!({
            "Port": {"type": "object", "properties": {
                "containerPort": {"type": "integer", "default": 0},
                "protocol": {"type": "string", "default": "TCP"}}},
            "Sel": {"type": "object", "x-kubernetes-map-type": "atomic",
                    "properties": {"a": {"type": "string"}}},
            "Spec": {"type": "object", "properties": {
                "ports": {"type": "array", "items": {"allOf": [{"$ref": "#/components/schemas/Port"}]},
                          "x-kubernetes-list-type": "map",
                          "x-kubernetes-list-map-keys": ["containerPort", "protocol"]},
                "sel": {"allOf": [{"$ref": "#/components/schemas/Sel"}]},
                "plain": {"type": "array", "items": {"type": "string"}},
                "self": {"$ref": "#/components/schemas/Spec"}}}
        })
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn openapi_annotations_become_relationships() {
        let t = Type::from_openapi(&schemas(), "Spec").unwrap();
        let Type::Struct(s) = &t else { panic!() };
        match &s.fields["ports"] {
            Type::List {
                kind: ListKind::Map(keys),
                ..
            } => {
                assert_eq!(keys[0], ("containerPort".to_string(), Some(json!(0))));
                assert_eq!(keys[1], ("protocol".to_string(), Some(json!("TCP"))));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(&s.fields["sel"], Type::Struct(st) if st.atomic));
        // No list-type annotation: atomic.
        assert!(matches!(
            &s.fields["plain"],
            Type::List {
                kind: ListKind::Atomic,
                ..
            }
        ));
        // A recursive reference ends in an untyped node, not a loop.
        assert_eq!(s.fields["self"], Type::Deduced);
    }
}
