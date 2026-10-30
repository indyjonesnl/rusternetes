//! Unit tests moved from `src/dynamic_routes.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{routing::get, Router};
    pub use rusternetes_api_server::{handlers::custom_resource, state::ApiServerState};
    pub use rusternetes_common::resources::{CustomResourceDefinition, ResourceScope};
    pub use std::sync::Arc;
    pub use tracing::{debug, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::dynamic_routes::*;
    use rusternetes_common::resources::{
        CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, CustomResourceSubresourceScale,
        CustomResourceSubresourceStatus, CustomResourceSubresources,
    };
    use rusternetes_common::types::ObjectMeta;

    // Build an ApiServerState backed by the in-memory storage backend, so these
    // tests can exercise route-building logic without any external etcd cluster.
    async fn create_test_state() -> Arc<ApiServerState> {
        use rusternetes_common::auth::TokenManager;
        use rusternetes_common::authz::AlwaysAllowAuthorizer;
        use rusternetes_common::observability::MetricsRegistry;
        use rusternetes_storage::StorageBackend;

        let storage = Arc::new(StorageBackend::new_memory());
        let token_manager = Arc::new(TokenManager::new(b"test-secret"));
        let authorizer =
            Arc::new(AlwaysAllowAuthorizer) as Arc<dyn rusternetes_common::authz::Authorizer>;
        let metrics = Arc::new(MetricsRegistry::new());

        Arc::new(ApiServerState::new(
            storage,
            token_manager,
            authorizer,
            metrics,
            true,
        ))
    }

    fn create_test_crd(with_subresources: bool) -> CustomResourceDefinition {
        let mut version = CustomResourceDefinitionVersion {
            name: "v1".to_string(),
            served: true,
            storage: true,
            deprecated: None,
            deprecation_warning: None,
            schema: None,
            subresources: None,
            additional_printer_columns: None,
            selectable_fields: None,
        };

        if with_subresources {
            version.subresources = Some(CustomResourceSubresources {
                status: Some(CustomResourceSubresourceStatus {}),
                scale: Some(CustomResourceSubresourceScale {
                    spec_replicas_path: ".spec.replicas".to_string(),
                    status_replicas_path: ".status.replicas".to_string(),
                    label_selector_path: Some(".status.labelSelector".to_string()),
                }),
            });
        }

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
                versions: vec![version],
                conversion: None,
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn test_dynamic_route_manager_creation() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        // Manager should be created successfully
        assert!(std::ptr::addr_of!(manager) as usize > 0);
    }

    #[tokio::test]
    async fn test_build_namespaced_crd_routes() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        let crd = create_test_crd(false);

        let _routes = manager.build_crd_routes(&crd);
        // If we got here without panicking, route building succeeded
    }

    #[tokio::test]
    async fn test_build_crd_routes_with_subresources() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        let crd = create_test_crd(true);

        let _routes = manager.build_crd_routes(&crd);
        // If we got here without panicking, route building with subresources succeeded
    }

    #[tokio::test]
    async fn test_build_cluster_scoped_crd_routes() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        let mut crd = create_test_crd(false);
        crd.spec.scope = ResourceScope::Cluster;

        let _routes = manager.build_crd_routes(&crd);
        // If we got here without panicking, cluster-scoped route building succeeded
    }

    #[tokio::test]
    async fn test_register_and_unregister_crd() {
        let state = create_test_state().await;
        let manager = DynamicRouteManager::new(state);
        let crd = create_test_crd(false);

        manager.register_crd(&crd);
        manager.unregister_crd(&crd);
        // If we got here without panicking, registration/unregistration succeeded
    }
}
