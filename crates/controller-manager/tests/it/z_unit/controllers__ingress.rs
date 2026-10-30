//! Unit tests moved from `src/controllers/ingress.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::{
        ingress::{HTTPIngressPath, IngressBackend, IngressRule, IngressSpec, IngressTLS},
        Ingress, IngressClass, Secret, Service,
    };
    pub use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::ingress::{
        HTTPIngressRuleValue, IngressServiceBackend, ServiceBackendPort,
    };
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::ingress::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_validate_path_type_valid() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        let path = HTTPIngressPath {
            path: Some("/".to_string()),
            path_type: Some("Prefix".to_string()),
            backend,
        };

        assert!(controller
            .validate_http_path(&path, "default")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_validate_path_type_invalid() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        let path = HTTPIngressPath {
            path: Some("/".to_string()),
            path_type: Some("Invalid".to_string()),
            backend,
        };

        assert!(controller
            .validate_http_path(&path, "default")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_validate_backend_with_port_number() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        assert!(controller
            .validate_backend(&backend, "default")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_validate_backend_with_port_name() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: Some("http".to_string()),
                    number: None,
                }),
            }),
            resource: None,
        };

        assert!(controller
            .validate_backend(&backend, "default")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_validate_backend_missing_port_spec() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: None,
                }),
            }),
            resource: None,
        };

        assert!(controller
            .validate_backend(&backend, "default")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_validate_ingress_rule() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage);

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        let path = HTTPIngressPath {
            path: Some("/api".to_string()),
            path_type: Some("Prefix".to_string()),
            backend,
        };

        let rule = IngressRule {
            host: Some("example.com".to_string()),
            http: Some(HTTPIngressRuleValue { paths: vec![path] }),
        };

        assert!(controller
            .validate_ingress_rule(&rule, "default")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_reconcile_valid_ingress() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = IngressController::new(storage.clone());

        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        let path = HTTPIngressPath {
            path: Some("/".to_string()),
            path_type: Some("Prefix".to_string()),
            backend,
        };

        let rule = IngressRule {
            host: Some("example.com".to_string()),
            http: Some(HTTPIngressRuleValue { paths: vec![path] }),
        };

        let spec = IngressSpec {
            ingress_class_name: Some("nginx".to_string()),
            default_backend: None,
            tls: None,
            rules: Some(vec![rule]),
        };

        let ingress = Ingress {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Ingress".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata: {
                let mut meta = ObjectMeta::new("test-ingress").with_namespace("default");
                meta.uid = uuid::Uuid::new_v4().to_string();
                meta
            },
            spec: Some(spec),
            status: None,
        };

        // Create the IngressClass "nginx" so validation passes
        use rusternetes_common::resources::IngressClass;
        let class_key = build_key("ingressclasses", None, "nginx");
        storage
            .create(&class_key, &IngressClass::new("nginx"))
            .await
            .unwrap();

        // Create ingress in storage first (like API server would)
        let ingress_key = build_key("ingresses", Some("default"), "test-ingress");
        storage.create(&ingress_key, &ingress).await.unwrap();

        // Reconcile should validate and update status
        assert!(controller.reconcile_ingress(&ingress).await.is_ok());

        // Verify ingress was updated with load balancer status
        let updated_ingress: Ingress = storage.get(&ingress_key).await.unwrap();
        assert!(updated_ingress.status.is_some());
        let status = updated_ingress.status.unwrap();
        assert!(status.load_balancer.is_some());
    }
}
