//! Unit tests moved from `src/conversion.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{
        CustomResource, CustomResourceDefinition, WebhookClientConfig,
    };
    pub use rusternetes_common::Result;
    pub use rusternetes_storage::Storage;
    pub use serde::{Deserialize, Serialize};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{debug, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::conversion::*;
    use rusternetes_common::resources::{
        ConversionStrategyType, CustomResourceConversion, CustomResourceDefinitionNames,
        CustomResourceDefinitionSpec, CustomResourceDefinitionVersion, ResourceScope,
    };
    use rusternetes_common::types::ObjectMeta;

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
                versions: vec![
                    CustomResourceDefinitionVersion {
                        name: "v1".to_string(),
                        served: true,
                        storage: true,
                        deprecated: None,
                        deprecation_warning: None,
                        schema: None,
                        subresources: None,
                        additional_printer_columns: None,
                        selectable_fields: None,
                    },
                    CustomResourceDefinitionVersion {
                        name: "v2".to_string(),
                        served: true,
                        storage: false,
                        deprecated: None,
                        deprecation_warning: None,
                        schema: None,
                        subresources: None,
                        additional_printer_columns: None,
                        selectable_fields: None,
                    },
                ],
                conversion: Some(CustomResourceConversion {
                    strategy: Some(ConversionStrategyType::None),
                    webhook: None,
                }),
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_extract_version() {
        assert_eq!(extract_version("stable.example.com/v1"), "v1");
        assert_eq!(extract_version("v1"), "v1");
        assert_eq!(extract_version("apps/v1"), "v1");
    }

    #[test]
    fn test_conversion_result_success() {
        let result = ConversionResult::success();
        assert_eq!(result.status, "Success");
        assert!(result.message.is_none());
    }

    #[test]
    fn test_conversion_result_failure() {
        let result = ConversionResult::failure("Test error".to_string());
        assert_eq!(result.status, "Failure");
        assert_eq!(result.message, Some("Test error".to_string()));
        assert_eq!(result.code, Some(500));
    }

    #[tokio::test]
    async fn test_convert_same_version() {
        let crd = create_test_crd();
        let resource = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(serde_json::json!({
                "cronSpec": "* * * * */5",
                "image": "my-cron-image"
            })),
            status: None,
            extra: std::collections::HashMap::new(),
        };

        let storage = Arc::new(rusternetes_storage::MemoryStorage::new());
        let result = convert_custom_resource(&crd, resource.clone(), "v1", &storage).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().api_version, resource.api_version);
    }

    #[test]
    fn test_webhook_url_from_service() {
        let client = ConversionWebhookClient::new();
        let config = WebhookClientConfig {
            url: None,
            service: Some(rusternetes_common::resources::ServiceReference {
                namespace: "default".to_string(),
                name: "converter".to_string(),
                path: Some("/convert".to_string()),
                port: Some(443),
            }),
            ca_bundle: None,
        };

        let url = client.build_webhook_url(&config);
        assert!(url.is_ok());
        assert_eq!(url.unwrap(), "https://converter.default.svc:443/convert");
    }

    #[test]
    fn test_webhook_url_from_url() {
        let client = ConversionWebhookClient::new();
        let config = WebhookClientConfig {
            url: Some("https://example.com/convert".to_string()),
            service: None,
            ca_bundle: None,
        };

        let url = client.build_webhook_url(&config);
        assert!(url.is_ok());
        assert_eq!(url.unwrap(), "https://example.com/convert");
    }
}
