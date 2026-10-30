//! Unit tests moved from `src/resources/ingressclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::ingressclass::*;

    #[test]
    fn test_ingress_class_creation() {
        let ingress_class = IngressClass::new("nginx");

        assert_eq!(ingress_class.metadata.name, "nginx");
        assert_eq!(ingress_class.type_meta.kind, "IngressClass");
        assert_eq!(ingress_class.type_meta.api_version, "networking.k8s.io/v1");
    }

    #[test]
    fn test_ingress_class_with_controller() {
        let ingress_class = IngressClass::new("nginx").with_controller("k8s.io/ingress-nginx");

        assert!(ingress_class.spec.is_some());
        let spec = ingress_class.spec.unwrap();
        assert_eq!(spec.controller, "k8s.io/ingress-nginx");
    }

    #[test]
    fn test_ingress_class_with_spec() {
        let spec = IngressClassSpec {
            controller: "example.com/ingress-controller".to_string(),
            parameters: Some(IngressClassParametersReference {
                api_group: Some("k8s.example.com".to_string()),
                kind: "IngressParameters".to_string(),
                name: "external-lb".to_string(),
                namespace: Some("ingress-system".to_string()),
                scope: Some("Namespace".to_string()),
            }),
        };

        let ingress_class = IngressClass::new("external-lb").with_spec(spec.clone());

        assert!(ingress_class.spec.is_some());
        let ic_spec = ingress_class.spec.unwrap();
        assert_eq!(ic_spec.controller, "example.com/ingress-controller");
        assert!(ic_spec.parameters.is_some());

        let params = ic_spec.parameters.unwrap();
        assert_eq!(params.kind, "IngressParameters");
        assert_eq!(params.name, "external-lb");
        assert_eq!(params.namespace, Some("ingress-system".to_string()));
        assert_eq!(params.scope, Some("Namespace".to_string()));
    }

    #[test]
    fn test_ingress_class_serialization() {
        let spec = IngressClassSpec {
            controller: "k8s.io/ingress-nginx".to_string(),
            parameters: None,
        };

        let ingress_class = IngressClass::new("nginx").with_spec(spec);

        let json = serde_json::to_string(&ingress_class).unwrap();
        assert!(json.contains("IngressClass"));
        assert!(json.contains("networking.k8s.io/v1"));
        assert!(json.contains("k8s.io/ingress-nginx"));
    }

    #[test]
    fn test_ingress_class_with_cluster_scoped_parameters() {
        let spec = IngressClassSpec {
            controller: "acme.io/ingress-controller".to_string(),
            parameters: Some(IngressClassParametersReference {
                api_group: Some("acme.io".to_string()),
                kind: "IngressConfig".to_string(),
                name: "global-config".to_string(),
                namespace: None,
                scope: Some("Cluster".to_string()),
            }),
        };

        let ingress_class = IngressClass::new("acme").with_spec(spec);

        assert!(ingress_class.spec.is_some());
        let params = ingress_class.spec.unwrap().parameters.unwrap();
        assert_eq!(params.scope, Some("Cluster".to_string()));
        assert!(params.namespace.is_none());
    }
}
