//! Unit tests moved from `src/state.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::admission::policy_plugin::MutatingPolicyPlugin;
    pub use rusternetes_api_server::admission_webhook::AdmissionWebhookManager;
    pub use rusternetes_api_server::prometheus_client::PrometheusClient;
    pub use rusternetes_api_server::registry::core::service::alloc::ClusterIpAllocators;
    pub use rusternetes_api_server::registry::core::service::allocator::{
        storage::Etcd, AllocationBitmap,
    };
    pub use rusternetes_api_server::registry::core::service::ipranges::ServiceIpRanges;
    pub use rusternetes_api_server::registry::core::service::portallocator::{
        PortAllocator, PortRange, DEFAULT_SERVICE_NODE_PORT_RANGE,
    };
    pub use rusternetes_api_server::watch_cache::WatchCache;
    pub use rusternetes_common::auth::{BootstrapTokenManager, TokenManager};
    pub use rusternetes_common::authz::Authorizer;
    pub use rusternetes_common::observability::MetricsRegistry;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod node_port_range_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::service::portallocator::PortRange;
    use rusternetes_api_server::state::*;
    use rusternetes_common::authz::AlwaysAllowAuthorizer;
    use rusternetes_common::observability::MetricsRegistry;

    fn state() -> ApiServerState {
        ApiServerState::new(
            Arc::new(StorageBackend::new_memory()),
            Arc::new(TokenManager::new(b"test-secret")),
            Arc::new(AlwaysAllowAuthorizer) as Arc<dyn Authorizer>,
            Arc::new(MetricsRegistry::new()),
            true,
        )
    }

    /// `--service-node-port-range` defaults to 30000-32767
    /// (kubeoptions.DefaultServiceNodePortRange, options.go:27).
    #[tokio::test]
    async fn default_node_port_range_is_30000_32767() {
        let s = state();
        assert_eq!(
            s.node_port_allocator.port_range().to_string(),
            "30000-32767"
        );
    }

    /// The configured range reaches the allocator (storage_core.go:484-495);
    /// the repair loop reads the same range from the allocator
    /// (instance.go:397 hands `NodePortRange` to the REST storage).
    #[tokio::test]
    async fn configured_node_port_range_reaches_the_allocator() {
        let pr = PortRange::parse("20000-20099").unwrap();
        let s = state().with_service_node_port_range(pr);
        assert_eq!(s.node_port_allocator.port_range(), pr);
    }
}
