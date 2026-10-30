/// Service Controller
///
/// Manages service lifecycle including:
/// - ClusterIP allocation from service CIDR pool
/// - NodePort allocation for NodePort and LoadBalancer services
/// - Service type transitions (ClusterIP <-> NodePort <-> LoadBalancer)
/// - Cleanup of allocated resources on service deletion
///
/// The controller works in conjunction with:
/// - EndpointsController: Manages endpoint discovery based on selectors
/// - LoadBalancerController: Provisions external load balancers
/// - EndpointSliceController: Maintains endpoint slices for scalability
use anyhow::Result;
use futures::StreamExt;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use rusternetes_common::resources::Service;
use rusternetes_common::resources::ServiceType;
use rusternetes_common::resources::{IPFamily, IPFamilyPolicy, ServiceExternalTrafficPolicy};
use rusternetes_common::service_ip_ranges::ServiceIpRanges;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Default service CIDR for ClusterIP allocation
const DEFAULT_SERVICE_CIDR: &str = "10.96.0.0/12";
/// Default IPv6 service CIDR for dual-stack ClusterIP allocation
const DEFAULT_SERVICE_CIDR_V6: &str = "fd00:10:96::/112";
/// Default NodePort range (Kubernetes standard)
pub const NODE_PORT_MIN: u16 = 30000;
pub const NODE_PORT_MAX: u16 = 32767;

/// ServiceController manages service IP and port allocation
pub struct ServiceController<S: Storage> {
    pub storage: Arc<S>,
    /// Tracks allocated ClusterIPs to avoid collisions
    pub allocated_ips: Arc<Mutex<HashSet<String>>>,
    /// Tracks allocated NodePorts to avoid collisions
    pub allocated_node_ports: Arc<Mutex<HashSet<u16>>>,
    /// IPv4 service range ClusterIPs are allocated from (`None`: single-stack
    /// IPv6 cluster).
    pub cidr_v4: Option<Ipv4Net>,
    /// IPv6 service range (`None`: single-stack IPv4 cluster).
    pub cidr_v6: Option<Ipv6Net>,
    /// The address reserved for `default/kubernetes`: first address of the
    /// primary range.
    pub api_server_service_ip: IpAddr,
}

impl<S: Storage + 'static> ServiceController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            allocated_ips: Arc::new(Mutex::new(HashSet::new())),
            allocated_node_ports: Arc::new(Mutex::new(HashSet::new())),
            cidr_v4: Some(DEFAULT_SERVICE_CIDR.parse().expect("default v4 range")),
            cidr_v6: Some(DEFAULT_SERVICE_CIDR_V6.parse().expect("default v6 range")),
            api_server_service_ip: ServiceIpRanges::default().api_server_service_ip(),
        }
    }

    /// Watch-based run loop. Initializes, then watches for service changes.
    /// Falls back to periodic resync every 30s.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        self.initialize().await?;

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("services", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }

    /// Initialize the controller by scanning existing services
    pub async fn initialize(&self) -> Result<()> {
        info!("Initializing Service Controller");

        // Scan all existing services to populate allocated IPs and ports
        let services: Vec<Service> = self.storage.list("/registry/services/").await?;

        let mut ips = self.allocated_ips.lock().await;
        let mut ports = self.allocated_node_ports.lock().await;

        for service in services {
            // Track ClusterIP allocations
            if let Some(cluster_ip) = &service.spec.cluster_ip {
                if cluster_ip != "None" {
                    ips.insert(cluster_ip.clone());
                }
            }

            // Track ClusterIPs list (for dual-stack)
            if let Some(cluster_ips) = &service.spec.cluster_ips {
                for ip in cluster_ips {
                    if ip != "None" {
                        ips.insert(ip.clone());
                    }
                }
            }

            // Track NodePort allocations
            for port in &service.spec.ports {
                if let Some(node_port) = port.node_port {
                    ports.insert(node_port);
                }
            }

            // Track healthCheckNodePort: it is drawn from the same NodePort
            // range, so it must be visible to the allocator after a restart or
            // a later allocation could collide with it.
            if let Some(hcnp) = service.spec.health_check_node_port {
                if let Ok(hcnp) = u16::try_from(hcnp) {
                    ports.insert(hcnp);
                }
            }
        }

        info!(
            "Service Controller initialized: {} IPs allocated, {} NodePorts allocated",
            ips.len(),
            ports.len()
        );

        Ok(())
    }

    /// Main reconciliation loop - syncs all services
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("services", Some(ns), name);
            match self.storage.get::<Service>(&storage_key).await {
                Ok(resource) => match self.reconcile_service(&resource).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<Service>("/registry/services/").await {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("services/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list services for enqueue: {}", e);
            }
        }
    }

    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting service reconciliation");

        let services: Vec<Service> = self.storage.list("/registry/services/").await?;

        for service in services {
            if let Err(e) = self.reconcile_service(&service).await {
                error!(
                    "Failed to reconcile service {}/{}: {}",
                    service
                        .metadata
                        .namespace
                        .as_ref()
                        .unwrap_or(&"default".to_string()),
                    &service.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    /// Reconcile a single service
    pub async fn reconcile_service(&self, service: &Service) -> Result<()> {
        let namespace = service
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Service has no namespace"))?;
        let service_name = &service.metadata.name;

        debug!("Reconciling service {}/{}", namespace, service_name);

        let service_type = service
            .spec
            .service_type
            .as_ref()
            .unwrap_or(&ServiceType::ClusterIP);

        let mut updated_service = service.clone();
        let mut needs_update = false;

        // Handle ClusterIP allocation
        if service_type != &ServiceType::ExternalName {
            if service.spec.cluster_ip.as_ref() == Some(&"None".to_string()) {
                // Headless service - don't allocate IP
                debug!(
                    "Service {}/{} is headless, skipping ClusterIP allocation",
                    namespace, service_name
                );
            } else if service.spec.cluster_ip.is_none()
                || service
                    .spec
                    .cluster_ip
                    .as_ref()
                    .map(|s| s.is_empty())
                    .unwrap_or(false)
            {
                // Determine the requested IP families. Mirrors upstream
                // pkg/registry/core/service/storage which derives families
                // from spec.ipFamilies (ordered) and spec.ipFamilyPolicy.
                let families = self.resolve_ip_families(service);

                let mut cluster_ips: Vec<String> = Vec::with_capacity(families.len());
                for family in &families {
                    let ip = match family {
                        IPFamily::IPv4 => self.allocate_cluster_ip().await?,
                        IPFamily::IPv6 => self.allocate_cluster_ip_v6().await?,
                        // The API server rejects an unsupported family at
                        // validation (#2469); never allocate for one.
                        IPFamily::Unknown(_) => continue,
                    };
                    info!(
                        "Allocated ClusterIP {} ({:?}) for service {}/{}",
                        ip, family, namespace, service_name
                    );
                    cluster_ips.push(ip);
                }

                // spec.clusterIP is the primary (first) family's address; the
                // full ordered list lives in spec.clusterIPs. Family ordering
                // matches spec.ipFamilies.
                updated_service.spec.cluster_ip = cluster_ips.first().cloned();
                // Reflect what was actually allocated back into ipFamilyPolicy so
                // clients see the real outcome.  Mirrors upstream
                // pkg/registry/core/service/storage which stamps the policy after
                // assignment.  Only RequireDualStack is stamped explicitly; for a
                // single family we leave the existing value intact (it may already
                // be SingleStack or PreferDualStack and neither is wrong).
                if cluster_ips.len() >= 2 {
                    updated_service.spec.ip_family_policy = Some(IPFamilyPolicy::RequireDualStack);
                }
                updated_service.spec.ip_families = Some(families);
                updated_service.spec.cluster_ips = Some(cluster_ips);
                needs_update = true;
            }
        }

        // Handle NodePort allocation for NodePort and LoadBalancer services
        if matches!(
            service_type,
            ServiceType::NodePort | ServiceType::LoadBalancer
        ) {
            for (i, port) in updated_service.spec.ports.iter_mut().enumerate() {
                if port.node_port.is_none() {
                    // Allocate a new NodePort
                    let node_port = self.allocate_node_port().await?;
                    info!(
                        "Allocated NodePort {} for service {}/{} port {}",
                        node_port, namespace, service_name, i
                    );
                    port.node_port = Some(node_port);
                    needs_update = true;
                }
            }
        }

        // Handle healthCheckNodePort allocation for LoadBalancer Services with
        // externalTrafficPolicy=Local. Mirrors upstream
        // pkg/registry/core/service/storage allocHealthCheckNodePort: the LB
        // health check needs a dedicated nodePort so external balancers can
        // detect nodes without local endpoints and stop sending them traffic.
        if service_type == &ServiceType::LoadBalancer
            && service.spec.external_traffic_policy == Some(ServiceExternalTrafficPolicy::Local)
            && service.spec.health_check_node_port.is_none()
        {
            let hcnp = self.allocate_node_port().await?;
            info!(
                "Allocated healthCheckNodePort {} for service {}/{}",
                hcnp, namespace, service_name
            );
            updated_service.spec.health_check_node_port = Some(i32::from(hcnp));
            needs_update = true;
        }

        // Handle service type downgrades (e.g., LoadBalancer -> ClusterIP)
        // If service was previously NodePort/LoadBalancer but now is ClusterIP,
        // release NodePorts and any healthCheckNodePort (only LoadBalancer +
        // externalTrafficPolicy=Local services own an HCNP).
        if service_type == &ServiceType::ClusterIP
            && (service.spec.ports.iter().any(|p| p.node_port.is_some())
                || service.spec.health_check_node_port.is_some())
        {
            warn!(
                "Service {}/{} changed from NodePort/LoadBalancer to ClusterIP, releasing NodePorts",
                namespace, service_name
            );
            let mut ports_lock = self.allocated_node_ports.lock().await;
            for port in &mut updated_service.spec.ports {
                if let Some(node_port) = port.node_port.take() {
                    ports_lock.remove(&node_port);
                    info!(
                        "Released NodePort {} for service {}/{}",
                        node_port, namespace, service_name
                    );
                    needs_update = true;
                }
            }
            // Release the healthCheckNodePort back to the allocator.
            if let Some(hcnp) = updated_service.spec.health_check_node_port.take() {
                if let Ok(hcnp) = u16::try_from(hcnp) {
                    ports_lock.remove(&hcnp);
                }
                info!(
                    "Released healthCheckNodePort {} for service {}/{}",
                    hcnp, namespace, service_name
                );
                needs_update = true;
            }
        }

        // Update service if changes were made
        if needs_update {
            let service_key = build_key("services", Some(namespace), service_name);
            self.storage.update(&service_key, &updated_service).await?;
            info!(
                "Updated service {}/{} with allocated resources",
                namespace, service_name
            );
        }

        Ok(())
    }

    /// The `kubernetes` Service address: the first address of the primary
    /// range (`GetIndexedIP(&serviceClusterIPRange, 1)`,
    /// pkg/controlplane/apiserver/options/options.go:378-382). Never handed
    /// out to another Service.
    pub fn api_server_service_ip(&self) -> IpAddr {
        self.api_server_service_ip
    }

    /// Re-home the allocator on `--service-cluster-ip-range`: one range, or
    /// two of different families, primary first. Parsed and validated by the
    /// same code as the api-server's flag
    /// ([`ServiceIpRanges`], completion.go:94-134 + validation.go:36-84). A
    /// family without a range cannot be allocated from.
    pub fn with_service_cluster_ip_range(mut self, flag: &str) -> Result<Self, String> {
        // Unset keeps this controller's historical defaults (the default v4
        // range plus a v6 range for dual-stack services).
        if flag.is_empty() {
            return Ok(self);
        }
        let ranges = ServiceIpRanges::parse(flag)?;
        self.cidr_v4 = None;
        self.cidr_v6 = None;
        for cidr in ranges.cidrs() {
            match cidr.parse::<IpNet>().map_err(|e| e.to_string())? {
                IpNet::V4(n) => self.cidr_v4 = Some(n),
                IpNet::V6(n) => self.cidr_v6 = Some(n),
            }
        }
        self.api_server_service_ip = ranges.api_server_service_ip();
        Ok(self)
    }

    /// Allocate a ClusterIP from the IPv4 service range
    pub async fn allocate_cluster_ip(&self) -> Result<String> {
        let mut ips = self.allocated_ips.lock().await;

        let cidr = self
            .cidr_v4
            .ok_or_else(|| anyhow::anyhow!("no IPv4 service cluster IP range is configured"))?;
        let base: u32 = cidr.network().into();
        let num_ips: u64 = 1u64 << (32 - cidr.prefix_len() as u32);

        // Skip the network address and, below /31, the broadcast address
        // (`ipallocator.NewInMemory` allocates only host addresses), plus the
        // address reserved for the `kubernetes` Service.
        let last = if cidr.prefix_len() < 31 {
            num_ips - 1
        } else {
            num_ips
        };
        for offset in 1..last {
            let candidate = Ipv4Addr::from(base.wrapping_add(offset as u32));
            if IpAddr::V4(candidate) == self.api_server_service_ip {
                continue;
            }
            let candidate_str = candidate.to_string();
            if !ips.contains(&candidate_str) {
                ips.insert(candidate_str.clone());
                return Ok(candidate_str);
            }
        }

        Err(anyhow::anyhow!(
            "No available ClusterIPs in service CIDR {}",
            cidr
        ))
    }

    /// Resolve the ordered list of IP families a Service should be allocated.
    ///
    /// Mirrors upstream `pkg/registry/core/service/storage` family inference:
    /// an explicit `spec.ipFamilies` list wins (order preserved); otherwise the
    /// `ipFamilyPolicy` decides single- vs dual-stack. The cluster default
    /// primary family is IPv4. For dual-stack policies without an explicit
    /// family list we default to `[IPv4, IPv6]`.
    fn resolve_ip_families(&self, service: &Service) -> Vec<IPFamily> {
        if let Some(families) = &service.spec.ip_families {
            if !families.is_empty() {
                return families.clone();
            }
        }

        match service.spec.ip_family_policy {
            Some(IPFamilyPolicy::PreferDualStack) | Some(IPFamilyPolicy::RequireDualStack) => {
                vec![IPFamily::IPv4, IPFamily::IPv6]
            }
            _ => vec![IPFamily::IPv4],
        }
    }

    /// Allocate a ClusterIP from the IPv6 service range.
    pub async fn allocate_cluster_ip_v6(&self) -> Result<String> {
        let mut ips = self.allocated_ips.lock().await;

        let cidr = self
            .cidr_v6
            .ok_or_else(|| anyhow::anyhow!("no IPv6 service cluster IP range is configured"))?;
        let host_bits = 128 - cidr.prefix_len() as u32;
        // Cap the scan window so a very large host space doesn't spin forever;
        // the service range only needs a modest number of addresses.
        let max_offset: u128 = if host_bits >= 32 {
            u32::MAX as u128
        } else {
            (1u128 << host_bits) - 1
        };
        let base: u128 = cidr.network().into();

        // Skip ::0 (network) and the address reserved for the `kubernetes`
        // Service. IPv6 has no broadcast address.
        for offset in 1..=max_offset {
            let candidate = Ipv6Addr::from(base + offset);
            if IpAddr::V6(candidate) == self.api_server_service_ip {
                continue;
            }
            let candidate_str = candidate.to_string();
            if !ips.contains(&candidate_str) {
                ips.insert(candidate_str.clone());
                return Ok(candidate_str);
            }
        }

        Err(anyhow::anyhow!(
            "No available ClusterIPs in IPv6 service CIDR {}",
            cidr
        ))
    }

    /// Allocate a NodePort from the available range
    pub async fn allocate_node_port(&self) -> Result<u16> {
        let mut ports = self.allocated_node_ports.lock().await;

        for port in NODE_PORT_MIN..=NODE_PORT_MAX {
            if !ports.contains(&port) {
                ports.insert(port);
                return Ok(port);
            }
        }

        Err(anyhow::anyhow!(
            "No available NodePorts in range {}-{}",
            NODE_PORT_MIN,
            NODE_PORT_MAX
        ))
    }

    /// Handle service deletion - release allocated resources
    pub async fn handle_service_deletion(&self, namespace: &str, service_name: &str) -> Result<()> {
        info!(
            "Handling deletion of service {}/{}",
            namespace, service_name
        );

        let service_key = build_key("services", Some(namespace), service_name);

        // Get the service to release its resources
        let service: Result<Service, _> = self.storage.get(&service_key).await;

        match service {
            Ok(service) => {
                // Release ClusterIP
                if let Some(cluster_ip) = &service.spec.cluster_ip {
                    if cluster_ip != "None" {
                        let mut ips = self.allocated_ips.lock().await;
                        ips.remove(cluster_ip);
                        info!(
                            "Released ClusterIP {} for service {}/{}",
                            cluster_ip, namespace, service_name
                        );
                    }
                }

                // Release ClusterIPs list
                if let Some(cluster_ips) = &service.spec.cluster_ips {
                    let mut ips = self.allocated_ips.lock().await;
                    for ip in cluster_ips {
                        if ip != "None" {
                            ips.remove(ip);
                        }
                    }
                }

                // Release NodePorts
                let mut ports = self.allocated_node_ports.lock().await;
                for port in &service.spec.ports {
                    if let Some(node_port) = port.node_port {
                        ports.remove(&node_port);
                        info!(
                            "Released NodePort {} for service {}/{}",
                            node_port, namespace, service_name
                        );
                    }
                }

                // Release healthCheckNodePort (drawn from the same range).
                if let Some(hcnp) = service.spec.health_check_node_port {
                    if let Ok(hcnp) = u16::try_from(hcnp) {
                        ports.remove(&hcnp);
                    }
                    info!(
                        "Released healthCheckNodePort {} for service {}/{}",
                        hcnp, namespace, service_name
                    );
                }

                Ok(())
            }
            Err(_) => {
                // Service already deleted, nothing to clean up
                debug!("Service {}/{} already deleted", namespace, service_name);
                Ok(())
            }
        }
    }
}
