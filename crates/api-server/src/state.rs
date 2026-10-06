use crate::admission_webhook::AdmissionWebhookManager;
use crate::prometheus_client::PrometheusClient;
use crate::registry::core::service::alloc::ClusterIpAllocators;
use crate::registry::core::service::allocator::{storage::Etcd, AllocationBitmap};
use crate::registry::core::service::ipranges::ServiceIpRanges;
use crate::registry::core::service::portallocator::{
    PortAllocator, PortRange, DEFAULT_SERVICE_NODE_PORT_RANGE,
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
    pub cluster_ip_allocators: Arc<ClusterIpAllocators<StorageBackend>>,
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
    /// `--event-ttl` in seconds: how long an Event lives after its last write
    /// (`ControlPlane.EventTTL`, pkg/controlplane/apiserver/config.go:75).
    pub event_ttl: u64,
}

/// `newServiceIPAllocators`' NodePort half (storage_core.go:484-495): one
/// bitmap with the static-band offset, persisted under
/// `/ranges/servicenodeports`, shared with the repair loop.
fn new_node_port_allocator(
    storage: &Arc<StorageBackend>,
    pr: PortRange,
) -> (Arc<Etcd<StorageBackend>>, Arc<PortAllocator>) {
    let offset = crate::registry::core::service::portallocator::calculate_range_offset(pr);
    let registry = Arc::new(Etcd::new(
        AllocationBitmap::with_offset(pr.size, pr.to_string(), offset),
        storage.clone(),
        "/registry/ranges/servicenodeports",
        "servicenodeportallocations",
    ));
    let backing = registry.clone();
    let allocator = Arc::new(
        PortAllocator::new(pr, Box::new(move |_, _, _| Ok(Box::new(backing))))
            .expect("the NodePort allocator factory cannot fail"),
    );
    (registry, allocator)
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
        let (node_port_registry, node_port_allocator) =
            new_node_port_allocator(&storage, DEFAULT_SERVICE_NODE_PORT_RANGE);

        // `NewMetaAllocator` per configured family (storage_core.go:
        // 397-403, 469-481); the default `--service-cluster-ip-range` is
        // single-stack IPv4, see `with_service_cluster_ip_ranges`.
        let cluster_ip_allocators = Arc::new(ClusterIpAllocators::new(
            storage.clone(),
            &ServiceIpRanges::default().families(),
        ));

        Self {
            storage,
            token_manager,
            bootstrap_token_manager: Arc::new(BootstrapTokenManager::new()),
            authorizer,
            metrics,
            skip_auth,
            cluster_ip_allocators,
            node_port_allocator,
            node_port_registry,
            webhook_manager,
            watch_cache,
            ca_cert_pem: None,
            prometheus_client: None,
            event_ttl: crate::registry::core::event::DEFAULT_EVENT_TTL_SECONDS,
        }
    }

    /// Use `pr` as the NodePort range (`--service-node-port-range`,
    /// cmd/kube-apiserver/app/options/options.go:124), rebuilding the
    /// allocator and the registry the repair loop shares with it
    /// (pkg/controlplane/instance.go:397 -> storage_core.go:484-495). An
    /// unset range (`Size == 0`) keeps the default, as
    /// pkg/controlplane/instance.go:285-291 does.
    pub fn with_service_node_port_range(mut self, pr: PortRange) -> Self {
        if pr.size == 0 {
            return self;
        }
        let (registry, allocator) = new_node_port_allocator(&self.storage, pr);
        self.node_port_registry = registry;
        self.node_port_allocator = allocator;
        self
    }

    /// Allocate ClusterIPs from `ranges` (`--service-cluster-ip-range`): one
    /// allocator per family, primary first.
    pub fn with_service_cluster_ip_ranges(mut self, ranges: &ServiceIpRanges) -> Self {
        self.cluster_ip_allocators = Arc::new(ClusterIpAllocators::new(
            self.storage.clone(),
            &ranges.families(),
        ));
        self
    }

    /// Set `--event-ttl`, in seconds; `0` keeps events forever.
    pub fn with_event_ttl(mut self, seconds: u64) -> Self {
        self.event_ttl = seconds;
        self
    }

    /// Give the ClusterIP allocators their loopback IPAddress client
    /// (storage_core.go:358, :430). Needs the finished `Arc` because the
    /// client reads the state back; it holds only a `Weak`.
    pub fn install_ip_address_loopback(self: &Arc<Self>) {
        self.cluster_ip_allocators.set_loopback(Arc::new(
            crate::registry::networking::ipaddress::Loopback::new(self),
        ));
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

#[cfg(test)]
mod node_port_range_tests {
    use super::*;
    use crate::registry::core::service::portallocator::PortRange;
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
