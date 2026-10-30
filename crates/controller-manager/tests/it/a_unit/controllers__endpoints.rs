//! Unit tests moved from `src/controllers/endpoints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::{
        EndpointAddress, EndpointPort, EndpointReference, EndpointSubset, Endpoints, Pod, Service,
    };
    pub use rusternetes_controller_manager::controllers::endpoints_tracker::StaleEndpointsTracker;
    pub use rusternetes_controller_manager::controllers::trigger_time_tracker::{
        format_rfc3339_nano, TriggerTimeTracker,
    };
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, error};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::endpoints::*;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_pod_matches_selector() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage);

        let mut pod_labels = HashMap::new();
        pod_labels.insert("app".to_string(), "nginx".to_string());
        pod_labels.insert("tier".to_string(), "frontend".to_string());

        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "test-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: String::new(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: Some(pod_labels),
                annotations: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                init_containers: None,
                containers: vec![],
                volumes: None,
                restart_policy: None,
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                ephemeral_containers: None,
                overhead: None,
                scheduler_name: None,
                topology_spread_constraints: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            }),
            status: None,
        };

        // Test exact match
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "nginx".to_string());
        assert!(controller.pod_matches_selector(&pod, &selector));

        // Test multiple labels
        selector.insert("tier".to_string(), "frontend".to_string());
        assert!(controller.pod_matches_selector(&pod, &selector));

        // Test mismatch
        selector.insert("app".to_string(), "apache".to_string());
        assert!(!controller.pod_matches_selector(&pod, &selector));

        // Test missing label
        selector.clear();
        selector.insert("nonexistent".to_string(), "value".to_string());
        assert!(!controller.pod_matches_selector(&pod, &selector));
    }

    #[test]
    fn test_is_pod_ready() {
        // Pod without status
        let pod_no_status = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "test-pod".to_string(),
                namespace: None,
                uid: String::new(),
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
            spec: Some(rusternetes_common::resources::PodSpec {
                init_containers: None,
                containers: vec![],
                volumes: None,
                restart_policy: None,
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                ephemeral_containers: None,
                overhead: None,
                scheduler_name: None,
                topology_spread_constraints: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            }),
            status: None,
        };
        assert!(!rusternetes_common::podutil::is_pod_ready(&pod_no_status));

        // Pod in Pending phase
        let pod_pending = Pod {
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Pending),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
            ..pod_no_status.clone()
        };
        assert!(!rusternetes_common::podutil::is_pod_ready(&pod_pending));

        // Pod with Ready condition = True
        let pod_ready = Pod {
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                container_statuses: Some(vec![rusternetes_common::resources::ContainerStatus {
                    name: "nginx".to_string(),
                    ready: true,
                    restart_count: 0,
                    state: None,
                    last_state: None,
                    image: Some("nginx:latest".to_string()),
                    image_id: None,
                    container_id: Some("container-123".to_string()),
                    started: None,
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
            ..pod_no_status.clone()
        };
        assert!(rusternetes_common::podutil::is_pod_ready(&pod_ready));

        // Pod with Ready condition = False (not ready)
        let pod_not_ready = Pod {
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                message: None,
                reason: None,
                host_ip: None,
                host_i_ps: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "False".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                container_statuses: Some(vec![rusternetes_common::resources::ContainerStatus {
                    name: "nginx".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: None,
                    last_state: None,
                    image: Some("nginx:latest".to_string()),
                    image_id: None,
                    container_id: Some("container-123".to_string()),
                    started: None,
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
            ..pod_no_status
        };
        assert!(!rusternetes_common::podutil::is_pod_ready(&pod_not_ready));
    }

    /// Test that reconcile_all skips writing endpoints when nothing has changed.
    /// This prevents unnecessary etcd writes that would cause log spam and I/O pressure.
    #[tokio::test]
    async fn test_endpoints_skip_write_when_unchanged() {
        use rusternetes_common::resources::{Service, ServicePort, ServiceSpec};

        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());

        // Create a service with a selector
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("test-svc")
                .with_namespace("default"),
            spec: ServiceSpec {
                selector: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(rusternetes_common::resources::IntOrString::Int(8080)),
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                cluster_ip: Some("10.96.0.100".to_string()),
                cluster_ips: None,
                service_type: Some(rusternetes_common::resources::ServiceType::ClusterIP),
                external_ips: None,
                session_affinity: None,
                load_balancer_ip: None,
                external_name: None,
                external_traffic_policy: None,
                internal_traffic_policy: None,
                ip_families: None,
                ip_family_policy: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                allocate_load_balancer_node_ports: None,
                load_balancer_class: None,
                load_balancer_source_ranges: None,
                health_check_node_port: None,
                traffic_distribution: None,
            },
            status: None,
        };
        let svc_key = "/registry/services/default/test-svc";
        storage.create(svc_key, &service).await.unwrap();

        // First reconcile — should create endpoints
        controller.reconcile_all().await.unwrap();

        // Get the endpoints and their resource version
        let ep_key = "/registry/endpoints/default/test-svc";
        let ep1: Endpoints = storage.get(ep_key).await.unwrap();
        let rv1 = ep1.metadata.resource_version.clone();

        // Second reconcile — nothing changed, should skip the write
        controller.reconcile_all().await.unwrap();

        let ep2: Endpoints = storage.get(ep_key).await.unwrap();
        let rv2 = ep2.metadata.resource_version.clone();

        // Resource version should be unchanged because no write occurred
        assert_eq!(
            rv1, rv2,
            "Endpoints resource version changed despite no data change — \
             this means the controller is doing unnecessary writes every reconcile loop"
        );

        // Verify the subsets are correct (empty since no pods match)
        assert!(ep2.subsets.is_empty());
    }

    /// Test that endpoints ARE updated when pod data actually changes.
    #[tokio::test]
    async fn test_endpoints_write_when_changed() {
        use rusternetes_common::resources::{PodStatus, Service, ServicePort, ServiceSpec};
        use rusternetes_common::types::Phase;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());

        // Create a service
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("test-svc")
                .with_namespace("default"),
            spec: ServiceSpec {
                selector: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(rusternetes_common::resources::IntOrString::Int(8080)),
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                cluster_ip: Some("10.96.0.100".to_string()),
                cluster_ips: None,
                service_type: Some(rusternetes_common::resources::ServiceType::ClusterIP),
                external_ips: None,
                session_affinity: None,
                load_balancer_ip: None,
                external_name: None,
                external_traffic_policy: None,
                internal_traffic_policy: None,
                ip_families: None,
                ip_family_policy: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                allocate_load_balancer_node_ports: None,
                load_balancer_class: None,
                load_balancer_source_ranges: None,
                health_check_node_port: None,
                traffic_distribution: None,
            },
            status: None,
        };
        let svc_key = "/registry/services/default/test-svc";
        storage.create(svc_key, &service).await.unwrap();

        // First reconcile — creates empty endpoints
        controller.reconcile_all().await.unwrap();
        let ep_key = "/registry/endpoints/default/test-svc";
        let ep1: Endpoints = storage.get(ep_key).await.unwrap();
        let _rv1 = ep1.metadata.resource_version.clone();
        assert!(ep1.subsets.is_empty());

        // Now add a matching pod with an IP and Running phase
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "web-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: "pod-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ..Default::default()
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.244.0.5".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_probe_time: None,
                    last_transition_time: None,
                    reason: None,
                    message: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/web-pod", &pod)
            .await
            .unwrap();

        // Second reconcile — should detect the new pod and update endpoints
        controller.reconcile_all().await.unwrap();
        let ep2: Endpoints = storage.get(ep_key).await.unwrap();

        // Subsets should have changed from empty to containing the pod
        assert_ne!(
            ep1.subsets, ep2.subsets,
            "Endpoints subsets didn't change even though a new pod was added — \
             the skip-unchanged optimization is too aggressive"
        );

        // Verify the pod's IP is in the endpoints
        assert!(
            !ep2.subsets.is_empty(),
            "Subsets should not be empty after adding a matching pod"
        );
        let addresses = ep2.subsets[0].addresses.as_ref().unwrap();
        assert_eq!(addresses[0].ip, "10.244.0.5");
    }

    /// Test that EndpointAddress.hostname is populated from pod spec when subdomain is set.
    #[tokio::test]
    async fn test_endpoint_address_hostname_from_pod_spec() {
        use rusternetes_common::resources::{PodStatus, Service, ServicePort, ServiceSpec};
        use rusternetes_common::types::Phase;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());

        // Create a headless service (clusterIP: None) with selector
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("my-svc")
                .with_namespace("default"),
            spec: ServiceSpec {
                selector: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ports: vec![ServicePort {
                    name: Some("http".to_string()),
                    port: 80,
                    target_port: Some(rusternetes_common::resources::IntOrString::Int(8080)),
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                cluster_ip: Some("None".to_string()),
                cluster_ips: None,
                service_type: Some(rusternetes_common::resources::ServiceType::ClusterIP),
                external_ips: None,
                session_affinity: None,
                load_balancer_ip: None,
                external_name: None,
                external_traffic_policy: None,
                internal_traffic_policy: None,
                ip_families: None,
                ip_family_policy: None,
                publish_not_ready_addresses: None,
                session_affinity_config: None,
                allocate_load_balancer_node_ports: None,
                load_balancer_class: None,
                load_balancer_source_ranges: None,
                health_check_node_port: None,
                traffic_distribution: None,
            },
            status: None,
        };
        storage
            .create("/registry/services/default/my-svc", &service)
            .await
            .unwrap();

        // Create a pod with hostname and subdomain set
        let pod_with_hostname = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "web-0".to_string(),
                namespace: Some("default".to_string()),
                uid: "pod-uid-hostname".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ..Default::default()
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![],
                hostname: Some("my-host".to_string()),
                subdomain: Some("my-svc".to_string()),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.244.0.10".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_probe_time: None,
                    last_transition_time: None,
                    reason: None,
                    message: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/web-0", &pod_with_hostname)
            .await
            .unwrap();

        // Create a pod WITHOUT subdomain — hostname should NOT be set in endpoint
        let pod_no_subdomain = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "web-1".to_string(),
                namespace: Some("default".to_string()),
                uid: "pod-uid-no-subdomain".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("app".to_string(), "web".to_string());
                    m
                }),
                ..Default::default()
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![],
                hostname: Some("my-other-host".to_string()),
                subdomain: None,
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.244.0.11".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_probe_time: None,
                    last_transition_time: None,
                    reason: None,
                    message: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/web-1", &pod_no_subdomain)
            .await
            .unwrap();

        // Reconcile
        controller.reconcile_all().await.unwrap();

        // Get the endpoints
        let ep: Endpoints = storage
            .get("/registry/endpoints/default/my-svc")
            .await
            .unwrap();

        assert!(!ep.subsets.is_empty(), "Endpoints should have subsets");
        let addresses = ep.subsets[0].addresses.as_ref().unwrap();
        assert_eq!(addresses.len(), 2, "Should have 2 ready addresses");

        // Find the address for the pod with hostname+subdomain
        let addr_with_hostname = addresses
            .iter()
            .find(|a| a.ip == "10.244.0.10")
            .expect("Should find address for pod with hostname");
        assert_eq!(
            addr_with_hostname.hostname,
            Some("my-host".to_string()),
            "EndpointAddress should have hostname set when pod has hostname+subdomain"
        );

        // Find the address for the pod WITHOUT subdomain
        let addr_no_subdomain = addresses
            .iter()
            .find(|a| a.ip == "10.244.0.11")
            .expect("Should find address for pod without subdomain");
        assert_eq!(
            addr_no_subdomain.hostname, None,
            "EndpointAddress should NOT have hostname when pod has no subdomain"
        );
    }

    /// Bug regression: `split_subsets_by_node` (internalTrafficPolicy=Local)
    /// must preserve each input subset's ports per node. When the named-port
    /// path produces multiple subsets with DIFFERENT resolved container ports,
    /// pods on a given node must keep the port from the subset they came from —
    /// not the first subset's port applied to every node.
    #[test]
    fn test_split_subsets_by_node_preserves_per_subset_ports() {
        let addr = |ip: &str, node: &str| EndpointAddress {
            ip: ip.to_string(),
            hostname: None,
            node_name: Some(node.to_string()),
            target_ref: None,
        };
        let port = |p: u16| {
            Some(vec![EndpointPort {
                name: Some("http".to_string()),
                port: p,
                protocol: "TCP".to_string(),
                app_protocol: None,
            }])
        };

        // Subset A resolves the named port to 8080 and holds a pod on each node;
        // subset B resolves it to 9090 and holds a different pod on each node.
        let subsets = vec![
            EndpointSubset {
                addresses: Some(vec![addr("10.0.0.1", "node-1"), addr("10.0.0.2", "node-2")]),
                not_ready_addresses: None,
                ports: port(8080),
            },
            EndpointSubset {
                addresses: Some(vec![addr("10.0.1.1", "node-1"), addr("10.0.1.2", "node-2")]),
                not_ready_addresses: None,
                ports: port(9090),
            },
        ];

        let out = split_subsets_by_node(subsets);

        // No subset may mix nodes.
        for s in &out {
            let nodes: std::collections::HashSet<&str> = s
                .addresses
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter_map(|a| a.node_name.as_deref())
                .collect();
            assert!(nodes.len() <= 1, "subset mixes nodes: {s:?}");
        }

        // Every address must carry the port from its ORIGINAL subset, not a
        // foreign one. Build the expected ip->port mapping and assert it holds.
        let expected: std::collections::HashMap<&str, u16> = [
            ("10.0.0.1", 8080),
            ("10.0.0.2", 8080),
            ("10.0.1.1", 9090),
            ("10.0.1.2", 9090),
        ]
        .into_iter()
        .collect();

        for s in &out {
            let subset_port = s.ports.as_ref().and_then(|p| p.first()).map(|p| p.port);
            for a in s.addresses.as_deref().unwrap_or(&[]) {
                assert_eq!(
                    subset_port,
                    Some(expected[a.ip.as_str()]),
                    "address {} got wrong port; subset={:?}",
                    a.ip,
                    s,
                );
            }
        }
    }

    /// An update of an existing Endpoints must carry the stored object's
    /// identity, as upstream's `currentEndpoints.DeepCopy()` does
    /// (pkg/controller/endpoint/endpoints_controller.go:470). The generic Store
    /// turns a non-empty `metadata.uid` into a UID precondition
    /// (rest/update.go:188-203), so a rebuilt object with its own UID would 409
    /// (the EndpointSlice regression, #2718); one with no UID silently drops
    /// the stored finalizers and system fields (#2719).
    #[tokio::test]
    async fn updating_existing_endpoints_keeps_stored_identity() {
        use rusternetes_common::resources::{PodCondition, PodStatus, ServicePort, ServiceSpec};
        use rusternetes_common::types::Phase;
        use rusternetes_controller_manager::controllers::uid_precondition_double::UidPreconditionStorage;

        let storage = Arc::new(UidPreconditionStorage::new());
        let controller = EndpointsController::new(storage.clone());

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("svc").with_namespace("default"),
            spec: ServiceSpec {
                selector: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                ..Default::default()
            },
            status: None,
        };
        storage
            .create("/registry/services/default/svc", &service)
            .await
            .unwrap();

        // No pod yet: the Endpoints is created empty, and the server stamps it.
        controller.reconcile_service(&service).await.unwrap();
        let ep_key = "/registry/endpoints/default/svc";
        let mut first: Endpoints = storage.get(ep_key).await.unwrap();
        assert!(!first.metadata.uid.is_empty());
        assert!(first.metadata.creation_timestamp.is_some());

        // Somebody else (a user, another controller) put a finalizer on it.
        first.metadata.finalizers = Some(vec!["example.com/keep".to_string()]);
        storage.update(ep_key, &first).await.unwrap();
        let first: Endpoints = storage.get(ep_key).await.unwrap();
        storage.puts.lock().unwrap().clear();

        // A ready pod appears: the existing Endpoints must be updated.
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "p1".to_string(),
                namespace: Some("default".to_string()),
                uid: "pod-uid-1".to_string(),
                labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                ..Default::default()
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.1".to_string()),
                conditions: Some(vec![PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    last_probe_time: None,
                    last_transition_time: None,
                    reason: None,
                    message: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/p1", &pod)
            .await
            .unwrap();
        controller.reconcile_service(&service).await.unwrap();

        let second: Endpoints = storage.get(ep_key).await.unwrap();
        assert_eq!(second.subsets.len(), 1, "the update must land");
        assert_eq!(second.metadata.uid, first.metadata.uid);
        assert_eq!(
            second.metadata.creation_timestamp,
            first.metadata.creation_timestamp
        );
        assert_eq!(
            second.metadata.finalizers, first.metadata.finalizers,
            "an update must not drop the stored finalizers"
        );
        let puts = storage.puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_eq!(
            puts[0]["metadata"]["uid"].as_str(),
            Some(first.metadata.uid.as_str()),
            "the PUT body must carry the stored UID"
        );
    }

    /// TestSyncEndpointsHeadlessServiceLabel (endpoints_controller_test.go:525):
    /// an Endpoints already carrying managed-by + headless labels and no
    /// service labels is correct, so there must be no write.
    #[tokio::test]
    async fn headless_service_label_present_means_no_write() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());
        let mut svc = svc_for_metadata_tests("None");
        svc.metadata.labels = None;
        controller.reconcile_service(&svc).await.unwrap();
        let key = "/registry/endpoints/default/svc";
        let before: Endpoints = storage.get(key).await.unwrap();
        let labels = before.metadata.labels.clone().unwrap();
        assert_eq!(labels.len(), 2, "managed-by + headless only: {labels:?}");
        controller.reconcile_service(&svc).await.unwrap();
        let after: Endpoints = storage.get(key).await.unwrap();
        assert_eq!(
            before.metadata.resource_version,
            after.metadata.resource_version
        );
    }

    /// TestSyncEndpointsItemsPreexistingLabelsChange (:1173) and
    /// TestSyncEndpointsHeadlessService (:1284): pre-existing Endpoints with
    /// different/no labels are rewritten with the Service's labels +
    /// managed-by (+ headless), even when the subsets already match.
    #[tokio::test]
    async fn preexisting_labels_change_is_rewritten() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());
        let mut svc = svc_for_metadata_tests("None");
        svc.metadata.labels = Some(HashMap::from([("baz".to_string(), "blah".to_string())]));
        controller.reconcile_service(&svc).await.unwrap();
        let key = "/registry/endpoints/default/svc";
        let mut ep: Endpoints = storage.get(key).await.unwrap();
        ep.metadata.labels = Some(HashMap::from([("foo".to_string(), "bar".to_string())]));
        storage.update(key, &ep).await.unwrap();
        controller.reconcile_service(&svc).await.unwrap();
        let ep: Endpoints = storage.get(key).await.unwrap();
        assert_eq!(
            ep.metadata.labels.unwrap(),
            HashMap::from([
                ("baz".to_string(), "blah".to_string()),
                (IS_HEADLESS_SERVICE_LABEL.to_string(), String::new()),
                (LABEL_MANAGED_BY.to_string(), CONTROLLER_NAME.to_string()),
            ])
        );
    }

    /// TestSyncServiceOverCapacity "annotation removed below capacity"
    /// (:2261 ff.): a stale over-capacity annotation is removed even though
    /// the subsets are unchanged (capacityAnnotationSetCorrectly).
    #[tokio::test]
    async fn stale_over_capacity_annotation_is_removed() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());
        let svc = svc_for_metadata_tests("10.0.0.5");
        controller.reconcile_service(&svc).await.unwrap();
        let key = "/registry/endpoints/default/svc";
        let mut ep: Endpoints = storage.get(key).await.unwrap();
        ep.metadata.annotations = Some(HashMap::from([
            (
                ENDPOINTS_OVER_CAPACITY_ANNOTATION.to_string(),
                "truncated".to_string(),
            ),
            ("user".to_string(), "keep".to_string()),
        ]));
        storage.update(key, &ep).await.unwrap();
        controller.reconcile_service(&svc).await.unwrap();
        let ep: Endpoints = storage.get(key).await.unwrap();
        assert_eq!(
            ep.metadata.annotations.unwrap(),
            HashMap::from([("user".to_string(), "keep".to_string())])
        );
    }

    #[test]
    fn capacity_annotation_set_correctly_matches_upstream() {
        let none = HashMap::new();
        let ann = HashMap::from([(
            ENDPOINTS_OVER_CAPACITY_ANNOTATION.to_string(),
            "truncated".to_string(),
        )]);
        assert!(capacity_annotation_set_correctly(&none, &[]));
        assert!(!capacity_annotation_set_correctly(&ann, &[]));
        let addr = EndpointAddress {
            ip: "1.1.1.1".into(),
            hostname: None,
            node_name: None,
            target_ref: None,
        };
        let big = EndpointSubset {
            addresses: Some(vec![addr; MAX_ENDPOINTS_CAPACITY + 1]),
            not_ready_addresses: None,
            ports: None,
        };
        assert!(!capacity_annotation_set_correctly(&none, &[big]));
    }

    fn svc_for_metadata_tests(cluster_ip: &str) -> Service {
        use rusternetes_common::resources::ServiceSpec;
        let mut md = rusternetes_common::types::ObjectMeta::new("svc").with_namespace("default");
        md.uid = "svc-uid".to_string();
        md.labels = Some(HashMap::from([("app".to_string(), "web".to_string())]));
        md.annotations = Some(HashMap::from([("svc-only".to_string(), "x".to_string())]));
        Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: md,
            spec: ServiceSpec {
                selector: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                cluster_ip: Some(cluster_ip.to_string()),
                ..Default::default()
            },
            status: None,
        }
    }

    /// Port of the metadata handling at endpoints_controller.go:469-499 and
    /// TestSyncEndpointsItemsWithLabels (:1119): labels = service labels +
    /// managed-by (+ headless), the Service's annotations are NOT copied, and
    /// no ownerReference is set.
    #[tokio::test]
    async fn created_endpoints_labels_annotations_and_no_owner() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());
        let svc = svc_for_metadata_tests("None");
        controller.reconcile_service(&svc).await.unwrap();
        let ep: Endpoints = storage
            .get("/registry/endpoints/default/svc")
            .await
            .unwrap();
        let labels = ep.metadata.labels.unwrap();
        assert_eq!(labels.get("app").map(String::as_str), Some("web"));
        assert_eq!(
            labels.get(LABEL_MANAGED_BY).map(String::as_str),
            Some("endpoint-controller")
        );
        assert!(labels.contains_key(IS_HEADLESS_SERVICE_LABEL));
        // The Service's annotations are not copied; the only annotation is the
        // trigger time, which for a new Service is its creationTimestamp
        // (trigger_time_tracker.go:117-120).
        let annotations = ep.metadata.annotations.unwrap();
        assert_eq!(annotations.len(), 1);
        assert_eq!(
            annotations.get(LAST_CHANGE_TRIGGER_TIME_ANNOTATION),
            Some(&format_rfc3339_nano(
                svc.metadata.creation_timestamp.unwrap()
            ))
        );
        assert!(ep.metadata.owner_references.is_none());
    }

    /// Stored annotations survive an update; a service with a cluster IP has
    /// no headless label; stale labels are repaired even when the subsets are
    /// unchanged (labelsCorrectForEndpoints).
    #[tokio::test]
    async fn existing_annotations_kept_and_labels_repaired() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointsController::new(storage.clone());
        let svc = svc_for_metadata_tests("10.0.0.5");
        controller.reconcile_service(&svc).await.unwrap();
        let key = "/registry/endpoints/default/svc";
        let mut ep: Endpoints = storage.get(key).await.unwrap();
        assert!(!ep
            .metadata
            .labels
            .as_ref()
            .unwrap()
            .contains_key(IS_HEADLESS_SERVICE_LABEL));
        ep.metadata.annotations = Some(HashMap::from([("user".to_string(), "keep".to_string())]));
        ep.metadata.labels = Some(HashMap::from([("stale".to_string(), "y".to_string())]));
        storage.update(key, &ep).await.unwrap();

        controller.reconcile_service(&svc).await.unwrap();
        let ep: Endpoints = storage.get(key).await.unwrap();
        assert_eq!(
            ep.metadata
                .annotations
                .unwrap()
                .get("user")
                .map(String::as_str),
            Some("keep")
        );
        let labels = ep.metadata.labels.unwrap();
        assert!(!labels.contains_key("stale"));
        assert_eq!(labels.get("app").map(String::as_str), Some("web"));
    }

    #[test]
    fn labels_correct_for_endpoints_matches_upstream() {
        let svc = HashMap::from([("a".to_string(), "1".to_string())]);
        let mut ep = svc.clone();
        assert!(!labels_correct_for_endpoints(&ep, &svc), "no managed-by");
        ep.insert(LABEL_MANAGED_BY.into(), CONTROLLER_NAME.into());
        assert!(labels_correct_for_endpoints(&ep, &svc));
        ep.insert(IS_HEADLESS_SERVICE_LABEL.into(), String::new());
        assert!(labels_correct_for_endpoints(&ep, &svc));
        ep.insert("extra".into(), "z".into());
        assert!(!labels_correct_for_endpoints(&ep, &svc));
        let mut ep2 = HashMap::from([(LABEL_MANAGED_BY.to_string(), CONTROLLER_NAME.to_string())]);
        assert!(
            !labels_correct_for_endpoints(&ep2, &svc),
            "svc label missing"
        );
        ep2.insert("a".into(), "2".into());
        assert!(!labels_correct_for_endpoints(&ep2, &svc), "value differs");
    }
}
