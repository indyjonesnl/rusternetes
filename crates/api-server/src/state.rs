use crate::admission_webhook::AdmissionWebhookManager;
use crate::prometheus_client::PrometheusClient;
use crate::registry::core::service::allocator::{storage::Etcd, AllocationBitmap};
use crate::registry::core::service::ipallocator::cidr::MetaAllocator;
use crate::registry::core::service::portallocator::{
    PortAllocator, DEFAULT_SERVICE_NODE_PORT_RANGE,
};
use crate::watch_cache::WatchCache;
use rusternetes_common::auth::{BootstrapTokenManager, TokenManager};
use rusternetes_common::authz::Authorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::StorageBackend;
use std::sync::Arc;

/// Shared state for the API server
pub struct ApiServerState {
    pub storage: Arc<StorageBackend>,
    pub token_manager: Arc<TokenManager>,
    pub bootstrap_token_manager: Arc<BootstrapTokenManager>,
    pub authorizer: Arc<dyn Authorizer>,
    pub metrics: Arc<MetricsRegistry>,
    pub skip_auth: bool,
    /// The ClusterIP allocator of the primary (IPv4) family: IPAddress
    /// objects out of the ServiceCIDRs.
    pub cluster_ip_allocator: Arc<MetaAllocator<StorageBackend>>,
    /// The service NodePort allocator, persisted as the
    /// `/registry/ranges/servicenodeports` RangeAllocation.
    pub node_port_allocator: Arc<PortAllocator>,
    /// The same storage-backed allocator as the repair loop's
    /// `RangeRegistry`.
    pub node_port_registry: Arc<Etcd<StorageBackend>>,
    pub webhook_manager: Arc<AdmissionWebhookManager<StorageBackend>>,
    pub watch_cache: Arc<WatchCache>,
    pub ca_cert_pem: Option<String>,
    pub prometheus_client: Option<Arc<PrometheusClient>>,
}

impl ApiServerState {
    pub fn new(
        storage: Arc<StorageBackend>,
        token_manager: Arc<TokenManager>,
        authorizer: Arc<dyn Authorizer>,
        metrics: Arc<MetricsRegistry>,
        skip_auth: bool,
    ) -> Self {
        let webhook_manager = Arc::new(AdmissionWebhookManager::new(storage.clone()));
        let watch_cache = Arc::new(WatchCache::new(storage.clone()));
        // Reclaim per-prefix replay-ring memory once a prefix's watchers drop to
        // zero (#1089).
        watch_cache.spawn_idle_gc();

        // `newServiceIPAllocators` (pkg/registry/core/rest/storage_core.go:
        // 484-495): one bitmap with the static-band offset, persisted under
        // `/ranges/servicenodeports`, shared with the repair loop.
        let pr = DEFAULT_SERVICE_NODE_PORT_RANGE;
        let offset = crate::registry::core::service::portallocator::calculate_range_offset(pr);
        let node_port_registry = Arc::new(Etcd::new(
            AllocationBitmap::with_offset(pr.size, pr.to_string(), offset),
            storage.clone(),
            "/registry/ranges/servicenodeports",
            "servicenodeportallocations",
        ));
        let backing = node_port_registry.clone();
        let node_port_allocator = Arc::new(
            PortAllocator::new(pr, Box::new(move |_, _, _| Ok(Box::new(backing))))
                .expect("the NodePort allocator factory cannot fail"),
        );

        // `NewMetaAllocator` for the primary family (storage_core.go:
        // 397-403); `--service-cluster-ip-range` is IPv4 here.
        let cluster_ip_allocator = Arc::new(MetaAllocator::new(storage.clone(), false));

        Self {
            storage,
            token_manager,
            bootstrap_token_manager: Arc::new(BootstrapTokenManager::new()),
            authorizer,
            metrics,
            skip_auth,
            cluster_ip_allocator,
            node_port_allocator,
            node_port_registry,
            webhook_manager,
            watch_cache,
            ca_cert_pem: None,
            prometheus_client: None,
        }
    }

    /// Set the CA certificate PEM for distribution to service accounts
    pub fn with_ca_cert(mut self, ca_cert_pem: Option<String>) -> Self {
        self.ca_cert_pem = ca_cert_pem;
        self
    }

    /// Set the Prometheus client for custom metrics
    pub fn with_prometheus_client(
        mut self,
        prometheus_client: Option<Arc<PrometheusClient>>,
    ) -> Self {
        self.prometheus_client = prometheus_client;
        self
    }
}
