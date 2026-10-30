//! Unit tests moved from `src/handlers/ratcheting.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::crd::{
        JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool,
    };
    pub use rusternetes_common::schema_validation::PathSeg;
    pub use serde_json::Value;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::ratcheting::*;
    use serde_json::json;

    fn from_json(v: serde_json::Value) -> JSONSchemaProps {
        serde_json::from_value(v).expect("schema")
    }

    #[test]
    fn property_correlation_walks_object_paths() {
        let schema = from_json(json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string"},
                    },
                },
            },
        }));
        let old = json!({"spec": {"field": "foo"}});
        let new = json!({"spec": {"field": "foo"}});
        let path = vec![PathSeg::Key("spec".into()), PathSeg::Key("field".into())];
        assert!(is_ratcheted(&schema, &old, &new, &path));
    }

    #[test]
    fn changed_value_is_not_ratcheted() {
        let schema = from_json(json!({
            "type": "object",
            "properties": {"field": {"type": "string"}},
        }));
        let old = json!({"field": "foo"});
        let new = json!({"field": "bar"});
        let path = vec![PathSeg::Key("field".into())];
        assert!(!is_ratcheted(&schema, &old, &new, &path));
    }

    #[test]
    fn atomic_array_index_is_uncorrelatable() {
        let schema = from_json(json!({
            "type": "object",
            "properties": {
                "list": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
        }));
        let old = json!({"list": ["foo"]});
        let new = json!({"list": ["foo"]});
        // Even though item is unchanged, atomic-array indices aren't
        // correlatable, so ratcheting cannot kick in.
        let path = vec![PathSeg::Key("list".into()), PathSeg::Index(0)];
        assert!(!is_ratcheted(&schema, &old, &new, &path));
    }

    #[test]
    fn list_type_map_correlates_by_composite_key() {
        let schema = from_json(json!({
            "type": "object",
            "properties": {
                "list": {
                    "type": "array",
                    "x-kubernetes-list-type": "map",
                    "x-kubernetes-list-map-keys": ["key"],
                    "items": {
                        "type": "object",
                        "properties": {
                            "key": {"type": "string"},
                            "field": {"type": "string"},
                        },
                        "required": ["key"],
                    },
                },
            },
        }));
        // Old has the same key but at index 1; new has it at index 0.
        let old = json!({"list": [
            {"key": "bar", "field": "bv"},
            {"key": "foo", "field": "fv"},
        ]});
        let new = json!({"list": [
            {"key": "foo", "field": "fv"},
            {"key": "bar", "field": "bv"},
        ]});
        // Path is structural — new[0] is the "foo" item.
        let path = vec![
            PathSeg::Key("list".into()),
            PathSeg::Index(0),
            PathSeg::Key("field".into()),
        ];
        assert!(is_ratcheted(&schema, &old, &new, &path));
    }

    #[test]
    fn list_type_map_changed_field_is_not_ratcheted() {
        let schema = from_json(json!({
            "type": "object",
            "properties": {
                "list": {
                    "type": "array",
                    "x-kubernetes-list-type": "map",
                    "x-kubernetes-list-map-keys": ["key"],
                    "items": {
                        "type": "object",
                        "properties": {
                            "key": {"type": "string"},
                            "field": {"type": "string"},
                        },
                    },
                },
            },
        }));
        let old = json!({"list": [{"key": "foo", "field": "old"}]});
        let new = json!({"list": [{"key": "foo", "field": "new"}]});
        let path = vec![
            PathSeg::Key("list".into()),
            PathSeg::Index(0),
            PathSeg::Key("field".into()),
        ];
        assert!(!is_ratcheted(&schema, &old, &new, &path));
    }

    #[test]
    fn additional_properties_correlate_by_name() {
        let schema = from_json(json!({
            "type": "object",
            "properties": {
                "map": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {"field": {"type": "string"}},
                    },
                },
            },
        }));
        let old = json!({"map": {"foo": {"field": "v"}}});
        let new = json!({"map": {"foo": {"field": "v"}}});
        let path = vec![
            PathSeg::Key("map".into()),
            PathSeg::Key("foo".into()),
            PathSeg::Key("field".into()),
        ];
        assert!(is_ratcheted(&schema, &old, &new, &path));
    }
}
