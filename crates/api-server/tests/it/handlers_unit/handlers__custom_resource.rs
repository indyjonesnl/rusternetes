//! Unit tests moved from `src/handlers/custom_resource.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Bytes,
        extract::{Path, State},
        response::Response,
        Extension, Json,
    };
    pub use rusternetes_api_server::endpoints::handlers::{self as endpoints, RequestScope};
    pub use rusternetes_api_server::registry::apiextensions::controllers::{
        find_crd_condition, ESTABLISHED, NAMES_ACCEPTED, TERMINATING,
    };
    pub use rusternetes_api_server::registry::apiextensions::customresource::{
        new_scale_rest, CustomResourceRest, StrictMode,
    };
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    pub use rusternetes_common::validation::crd::is_crd_condition_true;
    pub use rusternetes_common::{
        authz::{Decision, RequestAttributes},
        resources::{CustomResource, CustomResourceDefinition, Scale},
        schema_validation::SchemaValidator,
        List, Result,
    };
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::custom_resource::*;
    use rusternetes_common::resources::{
        CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, ResourceScope,
    };
    use rusternetes_common::types::ObjectMeta;
    use serde_json::json;

    fn create_test_crd() -> CustomResourceDefinition {
        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: ObjectMeta::new("crontabs.stable.example.com"),
            spec: CustomResourceDefinitionSpec {
                group: "stable.example.com".to_string(),
                names: CustomResourceDefinitionNames {
                    plural: "crontabs".to_string(),
                    singular: Some("crontab".to_string()),
                    kind: "CronTab".to_string(),
                    short_names: Some(vec!["ct".to_string()]),
                    categories: None,
                    list_kind: Some("CronTabList".to_string()),
                },
                scope: ResourceScope::Namespaced,
                versions: vec![CustomResourceDefinitionVersion {
                    name: "v1".to_string(),
                    served: true,
                    storage: true,
                    deprecated: None,
                    deprecation_warning: None,
                    schema: None,
                    subresources: None,
                    additional_printer_columns: None,
                    selectable_fields: None,
                }],
                conversion: None,
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    fn create_test_custom_resource() -> CustomResource {
        CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: {
                let mut meta = ObjectMeta::new("my-crontab");
                meta.namespace = Some("default".to_string());
                meta
            },
            spec: Some(json!({
                "cronSpec": "* * * * */5",
                "image": "my-cron-image"
            })),
            status: None,
            extra: Default::default(),
        }
    }

    fn crd_with_conditions(conds: &[(&str, &str)]) -> CustomResourceDefinition {
        let mut crd = create_test_crd();
        crd.status = Some(
            rusternetes_common::resources::CustomResourceDefinitionStatus {
                conditions: Some(
                    conds
                        .iter()
                        .map(|(t, s)| {
                            serde_json::from_value(json!({"type": t, "status": s})).unwrap()
                        })
                        .collect(),
                ),
                accepted_names: None,
                observed_generation: None,
                stored_versions: None,
            },
        );
        crd
    }

    /// customresource_handler.go:288-293: neither NamesAccepted nor
    /// Established true -> delegate (404).
    #[test]
    fn check_serving_requires_names_accepted_or_established() {
        assert!(check_serving(&create_test_crd(), "v1", None).is_err());
        let c = crd_with_conditions(&[("NamesAccepted", "False"), ("Established", "False")]);
        assert!(check_serving(&c, "v1", None).is_err());
        let c = crd_with_conditions(&[("NamesAccepted", "True")]);
        assert!(check_serving(&c, "v1", None).is_ok());
        let c = crd_with_conditions(&[("Established", "True")]);
        assert!(check_serving(&c, "v1", None).is_ok());
    }

    #[test]
    fn test_validate_custom_resource_success() {
        let crd = create_test_crd();
        let cr = create_test_custom_resource();

        assert!(validate_custom_resource(&crd, "v1", &cr).is_ok());
    }

    #[test]
    fn test_validate_custom_resource_invalid_version() {
        let crd = create_test_crd();
        let cr = create_test_custom_resource();

        let result = validate_custom_resource(&crd, "v2", &cr);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_custom_resource_not_served() {
        let mut crd = create_test_crd();
        crd.spec.versions[0].served = false;
        let cr = create_test_custom_resource();

        let result = validate_custom_resource(&crd, "v1", &cr);
        assert!(result.is_err());
    }

    /// Helper: create a CRD with a schema that has default values
    fn create_crd_with_defaults() -> CustomResourceDefinition {
        use rusternetes_common::resources::crd::JSONSchemaProps;
        use rusternetes_common::resources::CustomResourceValidation;

        let mut spec_properties = std::collections::HashMap::new();
        spec_properties.insert(
            "cronSpec".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );
        spec_properties.insert(
            "image".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                default: Some(json!("default-image:latest")),
                ..Default::default()
            },
        );
        spec_properties.insert(
            "replicas".to_string(),
            JSONSchemaProps {
                type_: Some("integer".to_string()),
                default: Some(json!(1)),
                ..Default::default()
            },
        );
        // Nested object with defaults
        let mut nested_props = std::collections::HashMap::new();
        nested_props.insert(
            "enabled".to_string(),
            JSONSchemaProps {
                type_: Some("boolean".to_string()),
                default: Some(json!(true)),
                ..Default::default()
            },
        );
        spec_properties.insert(
            "logging".to_string(),
            JSONSchemaProps {
                type_: Some("object".to_string()),
                properties: Some(nested_props),
                ..Default::default()
            },
        );

        let spec_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(spec_properties),
            ..Default::default()
        };

        let mut top_properties = std::collections::HashMap::new();
        top_properties.insert("spec".to_string(), spec_schema);

        let top_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(top_properties),
            ..Default::default()
        };

        let mut crd = create_test_crd();
        crd.spec.versions[0].schema = Some(CustomResourceValidation {
            open_apiv3_schema: Some(top_schema),
        });
        crd
    }

    #[test]
    fn test_schema_defaults_applied_to_missing_fields() {
        let crd = create_crd_with_defaults();
        // CR with only cronSpec set — image and replicas should get defaults
        let mut cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({
                "cronSpec": "* * * * */5"
            })),
            status: None,
            extra: Default::default(),
        };

        apply_schema_defaults(&crd, "v1", &mut cr);

        let spec = cr.spec.unwrap();
        assert_eq!(spec["cronSpec"], json!("* * * * */5"));
        assert_eq!(
            spec["image"],
            json!("default-image:latest"),
            "Default for 'image' should be applied"
        );
        assert_eq!(
            spec["replicas"],
            json!(1),
            "Default for 'replicas' should be applied"
        );
    }

    #[test]
    fn test_schema_defaults_do_not_overwrite_existing() {
        let crd = create_crd_with_defaults();
        let mut cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({
                "cronSpec": "0 0 * * *",
                "image": "my-custom-image:v2",
                "replicas": 3
            })),
            status: None,
            extra: Default::default(),
        };

        apply_schema_defaults(&crd, "v1", &mut cr);

        let spec = cr.spec.unwrap();
        assert_eq!(
            spec["image"],
            json!("my-custom-image:v2"),
            "Existing value should not be overwritten"
        );
        assert_eq!(
            spec["replicas"],
            json!(3),
            "Existing value should not be overwritten"
        );
    }

    #[test]
    fn test_schema_defaults_nested_objects() {
        let crd = create_crd_with_defaults();
        // CR has a logging object but without 'enabled' — default should be applied
        let mut cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({
                "cronSpec": "* * * * */5",
                "logging": {}
            })),
            status: None,
            extra: Default::default(),
        };

        apply_schema_defaults(&crd, "v1", &mut cr);

        let spec = cr.spec.unwrap();
        assert_eq!(
            spec["logging"]["enabled"],
            json!(true),
            "Nested default for 'logging.enabled' should be applied"
        );
    }

    #[test]
    fn test_strict_field_validation_rejects_unknown_cr_fields() {
        // Simulate strict field validation on a CR body with an unknown top-level field
        let cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({"cronSpec": "* * * * */5"})),
            status: None,
            extra: Default::default(),
        };

        // Body includes an unknown field "unknownTopLevel"
        let body = br#"{"apiVersion":"stable.example.com/v1","kind":"CronTab","metadata":{"name":"my-crontab"},"spec":{"cronSpec":"* * * * */5"},"unknownTopLevel":"bad"}"#;

        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = rusternetes_api_server::handlers::validation::validate_strict_fields(
            &params, body, &cr,
        );
        assert!(
            result.is_err(),
            "Strict validation should reject unknown fields"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("strict decoding error"),
            "Error should mention strict decoding: {}",
            err_msg
        );
        assert!(
            err_msg.contains("unknownTopLevel"),
            "Error should mention the unknown field name: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_field_validation_allows_valid_cr() {
        let cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({"cronSpec": "* * * * */5"})),
            status: None,
            extra: Default::default(),
        };

        let body = br#"{"apiVersion":"stable.example.com/v1","kind":"CronTab","metadata":{"name":"my-crontab"},"spec":{"cronSpec":"* * * * */5"}}"#;

        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = rusternetes_api_server::handlers::validation::validate_strict_fields(
            &params, body, &cr,
        );
        assert!(
            result.is_ok(),
            "Strict validation should pass for valid CR: {:?}",
            result
        );
    }
}
