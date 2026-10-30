//! Unit tests moved from `src/resources/crd.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::ObjectMeta;
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::crd::*;

    #[test]
    fn test_jsonschema_items_accepts_cbor_struct_form() {
        // A vnd.kubernetes CBOR client (1.35) sends `items` in the Go struct
        // form `{"schema": {...}}` (transcoded to JSON by rusternetes), not the
        // JSON-inlined form. Both must decode to the same Schema, and the inner
        // schema's fields must be preserved (regression: the old untagged derive
        // parsed it as an empty schema and strict validation rejected
        // `items.schema`).
        let inline: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "array",
            "items": { "type": "object", "properties": { "name": { "type": "string" } } }
        }))
        .unwrap();
        let struct_form: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "array",
            "items": { "schema": { "type": "object", "properties": { "name": { "type": "string" } } } }
        }))
        .unwrap();

        for (label, props) in [("inline", &inline), ("struct", &struct_form)] {
            match props.items.as_deref() {
                Some(JSONSchemaPropsOrArray::Schema(s)) => {
                    assert_eq!(
                        s.type_.as_deref(),
                        Some("object"),
                        "{label}: items elem type"
                    );
                    assert!(
                        s.properties
                            .as_ref()
                            .is_some_and(|p| p.contains_key("name")),
                        "{label}: items elem must keep its properties"
                    );
                }
                other => panic!("{label}: expected Schema variant, got {other:?}"),
            }
        }
        // Inline and struct forms must be equivalent after decode.
        assert_eq!(inline.items, struct_form.items);
    }

    #[test]
    fn test_jsonschema_or_bool_and_stringarray_cbor_struct_form() {
        // additionalProperties: CBOR struct form {allows, schema} and {schema}.
        let ap_bool: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "object",
            "additionalProperties": { "allows": false }
        }))
        .unwrap();
        assert_eq!(
            ap_bool.additional_properties.as_deref(),
            Some(&JSONSchemaPropsOrBool::Bool(false))
        );
        let ap_schema: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "object",
            "additionalProperties": { "schema": { "type": "string" } }
        }))
        .unwrap();
        match ap_schema.additional_properties.as_deref() {
            Some(JSONSchemaPropsOrBool::Schema(s)) => {
                assert_eq!(s.type_.as_deref(), Some("string"))
            }
            other => panic!("expected Schema, got {other:?}"),
        }
        // inline bool still works
        let ap_inline: JSONSchemaProps = serde_json::from_value(serde_json::json!({
            "type": "object", "additionalProperties": true
        }))
        .unwrap();
        assert_eq!(
            ap_inline.additional_properties.as_deref(),
            Some(&JSONSchemaPropsOrBool::Bool(true))
        );
    }

    #[test]
    fn test_crd_creation() {
        let crd =
            CustomResourceDefinition::new("crontab", "stable.example.com", "CronTab", "crontabs");

        assert_eq!(crd.spec.group, "stable.example.com");
        assert_eq!(crd.spec.names.kind, "CronTab");
        assert_eq!(crd.spec.names.plural, "crontabs");
        assert_eq!(crd.metadata.name, "crontabs.stable.example.com");
    }

    #[test]
    fn test_crd_with_version() {
        let mut crd =
            CustomResourceDefinition::new("crontab", "stable.example.com", "CronTab", "crontabs");

        crd.spec.versions.push(CustomResourceDefinitionVersion {
            name: "v1".to_string(),
            served: true,
            storage: true,
            deprecated: None,
            deprecation_warning: None,
            schema: None,
            subresources: None,
            additional_printer_columns: None,
            selectable_fields: None,
        });

        assert_eq!(crd.spec.versions.len(), 1);
        assert_eq!(crd.spec.versions[0].name, "v1");
        assert!(crd.spec.versions[0].storage);
    }

    #[test]
    fn test_json_schema_simple() {
        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(HashMap::from([(
                "spec".to_string(),
                JSONSchemaProps {
                    type_: Some("object".to_string()),
                    properties: Some(HashMap::from([
                        (
                            "cronSpec".to_string(),
                            JSONSchemaProps {
                                type_: Some("string".to_string()),
                                ..Default::default()
                            },
                        ),
                        (
                            "image".to_string(),
                            JSONSchemaProps {
                                type_: Some("string".to_string()),
                                ..Default::default()
                            },
                        ),
                    ])),
                    ..Default::default()
                },
            )])),
            ..Default::default()
        };

        assert_eq!(schema.type_, Some("object".to_string()));
        assert!(schema.properties.is_some());
    }

    #[test]
    fn test_resource_scope_serialization() {
        let scoped = serde_json::to_string(&ResourceScope::Namespaced).unwrap();
        assert_eq!(scoped, r#""Namespaced""#);

        let cluster = serde_json::to_string(&ResourceScope::Cluster).unwrap();
        assert_eq!(cluster, r#""Cluster""#);
    }

    /// Test that enum values survive a JSON round-trip through the typed struct.
    /// This verifies the CRD schema with nested items + enum can be stored as
    /// raw JSON and deserialized back without losing enum values.
    #[test]
    fn test_enum_survives_json_roundtrip() {
        let crd_json = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "foos.tests.example.com"},
            "spec": {
                "group": "tests.example.com",
                "names": {
                    "plural": "foos",
                    "kind": "Foo"
                },
                "scope": "Namespaced",
                "versions": [{
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "spec": {
                                    "type": "object",
                                    "properties": {
                                        "bars": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "required": ["name"],
                                                "properties": {
                                                    "name": { "type": "string" },
                                                    "feeling": {
                                                        "type": "string",
                                                        "enum": ["Great", "Down"]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }]
            }
        });

        // Deserialize to typed struct
        let crd: rusternetes_common::resources::CustomResourceDefinition =
            serde_json::from_value(crd_json).expect("Failed to deserialize CRD");

        // Check that enum values are preserved in the typed struct
        let version = &crd.spec.versions[0];
        let schema = version.schema.as_ref().expect("no schema");
        let spec_schema = schema
            .open_apiv3_schema
            .as_ref()
            .unwrap()
            .properties
            .as_ref()
            .unwrap()
            .get("spec")
            .unwrap();
        let bars_schema = spec_schema
            .properties
            .as_ref()
            .unwrap()
            .get("bars")
            .unwrap();

        match bars_schema.items.as_ref() {
            Some(items) => match items.as_ref() {
                JSONSchemaPropsOrArray::Schema(item_schema) => {
                    let feeling = item_schema
                        .properties
                        .as_ref()
                        .unwrap()
                        .get("feeling")
                        .unwrap();
                    assert_eq!(
                        feeling.enum_,
                        Some(vec![serde_json::json!("Great"), serde_json::json!("Down")]),
                        "enum values should survive deserialization"
                    );
                }
                JSONSchemaPropsOrArray::Schemas(_) => {
                    panic!("items should be Schema variant, not Schemas");
                }
            },
            None => panic!("items should not be None"),
        }

        // Serialize back and check again
        let roundtripped = serde_json::to_value(&crd).expect("Failed to serialize CRD");
        let feeling_enum = &roundtripped["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
            ["properties"]["spec"]["properties"]["bars"]["items"]["properties"]["feeling"]["enum"];
        assert_eq!(
            feeling_enum,
            &serde_json::json!(["Great", "Down"]),
            "enum values should survive full JSON round-trip"
        );
    }
}
