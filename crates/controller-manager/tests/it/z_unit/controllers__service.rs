//! Unit tests moved from `src/controllers/service.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use ipnet::{IpNet, Ipv4Net, Ipv6Net};
    pub use rusternetes_common::resources::Service;
    pub use rusternetes_common::resources::ServiceType;
    pub use rusternetes_common::resources::{
        IPFamily, IPFamilyPolicy, ServiceExternalTrafficPolicy,
    };
    pub use rusternetes_common::service_ip_ranges::ServiceIpRanges;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::collections::HashSet;
    pub use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    pub use std::sync::Arc;
    pub use tokio::sync::Mutex;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{IntOrString, ServicePort, ServiceSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::service::*;

    #[tokio::test]
    async fn test_allocate_cluster_ip() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceController::new(storage);

        // Allocate first IP (should be 10.96.0.2, as .1 is reserved)
        let ip1 = controller.allocate_cluster_ip().await.unwrap();
        assert_eq!(ip1, "10.96.0.2");

        // Allocate second IP
        let ip2 = controller.allocate_cluster_ip().await.unwrap();
        assert_eq!(ip2, "10.96.0.3");

        // IPs should be different
        assert_ne!(ip1, ip2);
    }

    /// `--service-cluster-ip-range` re-homes the allocator: ClusterIPs come
    /// from the configured primary range and its first address is the
    /// `kubernetes` Service's (`GetIndexedIP(&serviceClusterIPRange, 1)`,
    /// pkg/controlplane/apiserver/options/options.go:378-382), not the
    /// hard-coded 10.96.0.1.
    #[tokio::test]
    async fn custom_range_allocates_from_that_range() {
        use rusternetes_storage::memory::MemoryStorage;
        let controller = ServiceController::new(Arc::new(MemoryStorage::new()))
            .with_service_cluster_ip_range("192.0.2.0/24")
            .unwrap();
        assert_eq!(controller.allocate_cluster_ip().await.unwrap(), "192.0.2.2");
        assert_eq!(controller.allocate_cluster_ip().await.unwrap(), "192.0.2.3");
    }

    /// The reserved address follows the range: with 10.0.0.0/24 the
    /// `kubernetes` IP is 10.0.0.1, and 10.96.0.1 is an ordinary address of no
    /// range at all.
    #[tokio::test]
    async fn reserved_api_server_ip_is_the_first_of_the_primary_range() {
        use rusternetes_storage::memory::MemoryStorage;
        let controller = ServiceController::new(Arc::new(MemoryStorage::new()))
            .with_service_cluster_ip_range("10.0.0.0/24")
            .unwrap();
        assert_eq!(controller.api_server_service_ip().to_string(), "10.0.0.1");
        let mut got = Vec::new();
        for _ in 0..253 {
            got.push(controller.allocate_cluster_ip().await.unwrap());
        }
        assert!(!got.contains(&"10.0.0.1".to_string()));
        assert!(!got.contains(&"10.0.0.0".to_string()));
        assert!(!got.contains(&"10.0.0.255".to_string()), "broadcast");
        assert!(controller.allocate_cluster_ip().await.is_err(), "exhausted");
    }

    /// An IPv6-primary dual-stack pair: the reserved address is the v6
    /// range's first, and both families allocate from their own range.
    #[tokio::test]
    async fn ipv6_primary_dual_stack_ranges() {
        use rusternetes_storage::memory::MemoryStorage;
        let controller = ServiceController::new(Arc::new(MemoryStorage::new()))
            .with_service_cluster_ip_range("2001:db8:1::/112,192.0.2.0/24")
            .unwrap();
        assert_eq!(
            controller.api_server_service_ip().to_string(),
            "2001:db8:1::1"
        );
        assert_eq!(
            controller.allocate_cluster_ip_v6().await.unwrap(),
            "2001:db8:1::2"
        );
        assert_eq!(controller.allocate_cluster_ip().await.unwrap(), "192.0.2.1");
    }

    /// A single-stack cluster has no range for the other family; allocating
    /// from it is an error rather than an address from a default range.
    #[tokio::test]
    async fn family_without_a_range_cannot_allocate() {
        use rusternetes_storage::memory::MemoryStorage;
        let controller = ServiceController::new(Arc::new(MemoryStorage::new()))
            .with_service_cluster_ip_range("192.0.2.0/24")
            .unwrap();
        assert!(controller.allocate_cluster_ip_v6().await.is_err());
    }

    #[test]
    fn invalid_range_flag_is_rejected_like_upstream() {
        use rusternetes_storage::memory::MemoryStorage;
        let c = || ServiceController::new(Arc::new(MemoryStorage::new()));
        assert!(c().with_service_cluster_ip_range("nope").is_err());
        assert!(c()
            .with_service_cluster_ip_range("10.0.0.0/24,10.1.0.0/24")
            .is_err());
    }

    #[tokio::test]
    async fn test_allocate_node_port() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceController::new(storage);

        // Allocate first NodePort
        let port1 = controller.allocate_node_port().await.unwrap();
        assert!((NODE_PORT_MIN..=NODE_PORT_MAX).contains(&port1));

        // Allocate second NodePort
        let port2 = controller.allocate_node_port().await.unwrap();
        assert!((NODE_PORT_MIN..=NODE_PORT_MAX).contains(&port2));

        // Ports should be different
        assert_ne!(port1, port2);
    }

    #[tokio::test]
    async fn test_reconcile_clusterip_service() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceController::new(storage.clone());

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-service".to_string(),
                namespace: Some("default".to_string()),
                uid: uuid::Uuid::new_v4().to_string(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: None,
                annotations: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: ServiceSpec {
                selector: Some(std::collections::HashMap::new()),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(IntOrString::Int(8080)),
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                service_type: Some(ServiceType::ClusterIP),
                cluster_ip: None,
                external_ips: None,
                session_affinity: None,
                external_name: None,
                cluster_ips: None,
                ip_families: None,
                ip_family_policy: None,
                internal_traffic_policy: None,
                external_traffic_policy: None,
                health_check_node_port: None,
                load_balancer_class: None,
                load_balancer_ip: None,
                load_balancer_source_ranges: None,
                allocate_load_balancer_node_ports: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                traffic_distribution: None,
            },
            status: None,
        };

        // Create service in storage first (like API server would)
        let service_key = build_key("services", Some("default"), "test-service");
        storage.create(&service_key, &service).await.unwrap();

        // Reconcile should allocate ClusterIP
        controller.reconcile_service(&service).await.unwrap();

        // Verify ClusterIP was allocated
        let ips = controller.allocated_ips.lock().await;
        assert!(!ips.is_empty());

        // Verify service was updated in storage with ClusterIP
        let updated_service: Service = storage.get(&service_key).await.unwrap();
        assert!(updated_service.spec.cluster_ip.is_some());
    }

    #[tokio::test]
    async fn test_reconcile_nodeport_service() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceController::new(storage.clone());

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-nodeport-service".to_string(),
                namespace: Some("default".to_string()),
                uid: uuid::Uuid::new_v4().to_string(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: None,
                annotations: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: ServiceSpec {
                selector: Some(std::collections::HashMap::new()),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(IntOrString::Int(8080)),
                    protocol: "TCP".to_string(),
                    node_port: None, // Should be allocated
                    app_protocol: None,
                }],
                service_type: Some(ServiceType::NodePort),
                cluster_ip: None, // Should be allocated
                external_ips: None,
                session_affinity: None,
                external_name: None,
                cluster_ips: None,
                ip_families: None,
                ip_family_policy: None,
                internal_traffic_policy: None,
                external_traffic_policy: None,
                health_check_node_port: None,
                load_balancer_class: None,
                load_balancer_ip: None,
                load_balancer_source_ranges: None,
                allocate_load_balancer_node_ports: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                traffic_distribution: None,
            },
            status: None,
        };

        // Create service in storage first (like API server would)
        let service_key = build_key("services", Some("default"), "test-nodeport-service");
        storage.create(&service_key, &service).await.unwrap();

        // Reconcile should allocate both ClusterIP and NodePort
        controller.reconcile_service(&service).await.unwrap();

        // Verify ClusterIP was allocated
        let ips = controller.allocated_ips.lock().await;
        assert!(!ips.is_empty());

        // Verify NodePort was allocated
        let ports = controller.allocated_node_ports.lock().await;
        assert!(!ports.is_empty());

        // Verify service was updated in storage
        let updated_service: Service = storage.get(&service_key).await.unwrap();
        assert!(updated_service.spec.cluster_ip.is_some());
        assert!(updated_service.spec.ports[0].node_port.is_some());
    }
}
