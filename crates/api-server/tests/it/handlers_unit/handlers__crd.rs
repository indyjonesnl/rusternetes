//! Unit tests moved from `src/handlers/crd.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Bytes,
        extract::{Path, Query, State},
        http::HeaderMap,
        response::{IntoResponse, Response},
        Extension,
    };
    pub use rusternetes_api_server::endpoints::handlers::{self as endpoints, RequestScope};
    pub use rusternetes_api_server::registry::apiextensions::customresourcedefinition;
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::{
        admission::{GroupVersionKind, GroupVersionResource},
        authz::{Decision, RequestAttributes},
        resources::CustomResourceDefinition,
        List, Result,
    };
    pub use rusternetes_storage::{build_prefix, Storage};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::crd::*;
    use rusternetes_common::resources::{
        CustomResourceDefinition, CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, ResourceScope,
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

    #[tokio::test]
    async fn test_validating_webhook_called_for_crd_creation() {
        use rusternetes_api_server::admission_webhook::AdmissionWebhookManager;
        use rusternetes_common::admission::{
            GroupVersionKind, GroupVersionResource, Operation, UserInfo,
        };
        use rusternetes_common::resources::{
            FailurePolicy, OperationType, Rule, RuleWithOperations, SideEffectClass,
            ValidatingWebhook, ValidatingWebhookConfiguration, WebhookClientConfig,
        };
        use rusternetes_storage::memory::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let manager = AdmissionWebhookManager::new(storage.clone());

        // Create a ValidatingWebhookConfiguration targeting CRDs with FailurePolicy::Fail
        let webhook_config = ValidatingWebhookConfiguration {
            api_version: "admissionregistration.k8s.io/v1".to_string(),
            kind: "ValidatingWebhookConfiguration".to_string(),
            metadata: rusternetes_common::types::ObjectMeta::new("deny-crd-creation"),
            webhooks: Some(vec![ValidatingWebhook {
                name: "deny-crd.example.com".to_string(),
                client_config: WebhookClientConfig {
                    url: Some("https://127.0.0.1:19443/deny-crd".to_string()),
                    service: None,
                    ca_bundle: None,
                },
                rules: vec![RuleWithOperations {
                    operations: vec![OperationType::Create],
                    rule: Rule {
                        api_groups: vec!["apiextensions.k8s.io".to_string()],
                        api_versions: vec!["v1".to_string()],
                        resources: vec!["customresourcedefinitions".to_string()],
                        scope: None,
                    },
                }],
                failure_policy: Some(FailurePolicy::Fail),
                match_policy: None,
                namespace_selector: None,
                object_selector: None,
                side_effects: SideEffectClass::None,
                timeout_seconds: Some(1),
                admission_review_versions: vec!["v1".to_string()],
                match_conditions: None,
            }]),
        };

        // Store the webhook configuration
        storage
            .create(
                "/registry/validatingwebhookconfigurations/deny-crd-creation",
                &webhook_config,
            )
            .await
            .unwrap();

        // Build a CRD object and webhook parameters
        let crd = create_test_crd();
        let crd_value = serde_json::to_value(&crd).unwrap();

        let gvk = GroupVersionKind {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
        };
        let gvr = GroupVersionResource {
            group: "apiextensions.k8s.io".to_string(),
            version: "v1".to_string(),
            resource: "customresourcedefinitions".to_string(),
        };
        let user_info = UserInfo {
            username: "admin".to_string(),
            uid: "admin-uid".to_string(),
            groups: vec!["system:masters".to_string()],
        };

        // Call run_validating_webhooks — the webhook URL is unreachable,
        // and FailurePolicy is Fail, so this should return an error,
        // proving the webhook infrastructure was consulted for CRDs.
        let result = manager
            .run_validating_webhooks(
                &Operation::Create,
                &gvk,
                &gvr,
                None,
                &crd.metadata.name,
                Some(crd_value),
                None,
                &user_info,
            )
            .await;

        assert!(
            result.is_err(),
            "Expected webhook call to fail (proving webhook was consulted for CRD creation), but got: {:?}",
            result
        );
    }
}
