//! Unit tests moved from `src/resources/ingress.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::ingress::*;

    #[test]
    fn test_ingress_creation() {
        let ingress = Ingress::new("test-ingress", "default");

        assert_eq!(ingress.metadata.name, "test-ingress");
        assert_eq!(ingress.type_meta.kind, "Ingress");
        assert_eq!(ingress.type_meta.api_version, "networking.k8s.io/v1");
    }

    #[test]
    fn test_ingress_with_spec() {
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

        let http = HTTPIngressRuleValue { paths: vec![path] };

        let rule = IngressRule {
            host: Some("example.com".to_string()),
            http: Some(http),
        };

        let spec = IngressSpec {
            ingress_class_name: Some("nginx".to_string()),
            default_backend: None,
            tls: None,
            rules: Some(vec![rule]),
        };

        let ingress = Ingress::new("test-ingress", "default").with_spec(spec);

        assert!(ingress.spec.is_some());
        let spec = ingress.spec.unwrap();
        assert_eq!(spec.rules.as_ref().unwrap().len(), 1);
    }
}
