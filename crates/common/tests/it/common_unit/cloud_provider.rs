//! Unit tests moved from `src/cloud_provider.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::Result;
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::cloud_provider::*;

    #[test]
    fn test_cloud_provider_type_from_str() {
        assert_eq!(
            CloudProviderType::from_str("aws"),
            Some(CloudProviderType::AWS)
        );
        assert_eq!(
            CloudProviderType::from_str("AWS"),
            Some(CloudProviderType::AWS)
        );
        assert_eq!(
            CloudProviderType::from_str("gcp"),
            Some(CloudProviderType::GCP)
        );
        assert_eq!(
            CloudProviderType::from_str("google"),
            Some(CloudProviderType::GCP)
        );
        assert_eq!(
            CloudProviderType::from_str("azure"),
            Some(CloudProviderType::Azure)
        );
        assert_eq!(
            CloudProviderType::from_str("AZURE"),
            Some(CloudProviderType::Azure)
        );
        assert_eq!(
            CloudProviderType::from_str("none"),
            Some(CloudProviderType::None)
        );
        assert_eq!(
            CloudProviderType::from_str(""),
            Some(CloudProviderType::None)
        );
        assert_eq!(CloudProviderType::from_str("invalid"), None);
    }

    #[test]
    fn test_cloud_provider_type_as_str() {
        assert_eq!(CloudProviderType::AWS.as_str(), "aws");
        assert_eq!(CloudProviderType::GCP.as_str(), "gcp");
        assert_eq!(CloudProviderType::Azure.as_str(), "azure");
        assert_eq!(CloudProviderType::None.as_str(), "none");
    }

    #[test]
    fn test_cloud_provider_type_roundtrip() {
        let types = vec![
            CloudProviderType::AWS,
            CloudProviderType::GCP,
            CloudProviderType::Azure,
            CloudProviderType::None,
        ];

        for provider_type in types {
            let str_repr = provider_type.as_str();
            let parsed = CloudProviderType::from_str(str_repr);
            assert_eq!(parsed, Some(provider_type));
        }
    }

    #[test]
    fn test_loadbalancer_service_creation() {
        let mut annotations = std::collections::HashMap::new();
        annotations.insert("test-key".to_string(), "test-value".to_string());

        let service = LoadBalancerService {
            namespace: "default".to_string(),
            name: "test-service".to_string(),
            cluster_name: "test-cluster".to_string(),
            ports: vec![LoadBalancerPort {
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                port: 80,
                node_port: 30080,
            }],
            node_addresses: vec!["192.168.1.10".to_string(), "192.168.1.11".to_string()],
            session_affinity: Some("ClientIP".to_string()),
            annotations,
        };

        assert_eq!(service.namespace, "default");
        assert_eq!(service.name, "test-service");
        assert_eq!(service.ports.len(), 1);
        assert_eq!(service.node_addresses.len(), 2);
        assert_eq!(
            service.annotations.get("test-key"),
            Some(&"test-value".to_string())
        );
    }

    #[test]
    fn test_loadbalancer_status_with_ip() {
        let status = LoadBalancerStatus {
            ingress: vec![LoadBalancerIngress {
                ip: Some("203.0.113.1".to_string()),
                hostname: None,
            }],
        };

        assert_eq!(status.ingress.len(), 1);
        assert_eq!(status.ingress[0].ip, Some("203.0.113.1".to_string()));
        assert_eq!(status.ingress[0].hostname, None);
    }

    #[test]
    fn test_loadbalancer_status_with_hostname() {
        let status = LoadBalancerStatus {
            ingress: vec![LoadBalancerIngress {
                ip: None,
                hostname: Some("lb-abc123.us-west-2.elb.amazonaws.com".to_string()),
            }],
        };

        assert_eq!(status.ingress.len(), 1);
        assert_eq!(status.ingress[0].ip, None);
        assert!(status.ingress[0]
            .hostname
            .as_ref()
            .unwrap()
            .contains("elb.amazonaws.com"));
    }

    #[test]
    fn test_loadbalancer_status_multiple_ingress() {
        let status = LoadBalancerStatus {
            ingress: vec![
                LoadBalancerIngress {
                    ip: Some("203.0.113.1".to_string()),
                    hostname: None,
                },
                LoadBalancerIngress {
                    ip: Some("203.0.113.2".to_string()),
                    hostname: None,
                },
            ],
        };

        assert_eq!(status.ingress.len(), 2);
    }
}
