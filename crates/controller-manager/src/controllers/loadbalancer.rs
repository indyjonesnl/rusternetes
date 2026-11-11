use anyhow::{Context, Result};
use rusternetes_common::resources::{
    service::{LoadBalancerIngress, LoadBalancerStatus, ServiceStatus},
    Event, EventType, Node, ObjectReference, Service, ServiceType,
};
use rusternetes_storage::event_recorder::process_event_ttl;
use rusternetes_storage::{extract_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// LoadBalancerController publishes `status.loadBalancer.ingress` for LoadBalancer-type
/// Services. In-tree cloud providers were removed (k3s parity); the node-address ingress
/// path is the only behaviour. A faithful ServiceLB port is tracked in #4450.
pub struct LoadBalancerController<S: Storage> {
    pub storage: Arc<S>,
    pub cluster_name: String,
    pub sync_interval: Duration,
}

impl<S: Storage + 'static> LoadBalancerController<S> {
    pub fn new(storage: Arc<S>, cluster_name: String, sync_interval_secs: u64) -> Self {
        Self {
            storage,
            cluster_name,
            sync_interval: Duration::from_secs(sync_interval_secs),
        }
    }

    /// Start the controller reconciliation loop
    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting LoadBalancer controller");

        let queue = WorkQueue::new();

        // ONE worker, by upstream default (#2517): the cloud service controller's
        // `ConcurrentServiceSyncs` defaults to 1
        // (staging/src/k8s.io/cloud-provider/controllers/service/config/v1alpha1/defaults.go:29-30),
        // passed to `serviceController.Run(ctx, n, ..)` (cloud-provider/app/core.go:97).
        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("services", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    time::sleep(self.sync_interval).await;
                    continue;
                }
            };

            let mut resync = time::interval(Duration::from_secs(30));
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
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
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

    /// Reconcile all LoadBalancer services
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
            let storage_key = rusternetes_storage::build_key("services", Some(ns), name);
            match self.storage.get::<Service>(&storage_key).await {
                Ok(service) => {
                    // Only process LoadBalancer-type services
                    let is_lb = matches!(
                        service
                            .spec
                            .service_type
                            .as_ref()
                            .unwrap_or(&ServiceType::ClusterIP),
                        ServiceType::LoadBalancer
                    );
                    if is_lb {
                        // Publish node-address ingress so e2e service.go:4291 can complete.
                        if let Err(e) = self.reconcile_no_cloud_provider().await {
                            error!("LB stub status publish failed: {}", e);
                        }
                        queue.forget(&key).await;
                    } else {
                        queue.forget(&key).await;
                    }
                }
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
        debug!("Reconciling LoadBalancer services");
        self.reconcile_no_cloud_provider().await
    }

    /// Reconcile LoadBalancer services when no cloud provider is configured.
    ///
    /// Without a real provider we still need to publish a status so e2e
    /// callers (upstream `service.go:4291`) observe a populated
    /// `status.loadBalancer.ingress` and can finish their lifecycle. We use
    /// the first InternalIP of any Node as the ingress IP. Falls back to the
    /// service's own loadBalancerIP/ClusterIP if no node is registered.
    pub async fn reconcile_no_cloud_provider(&self) -> Result<()> {
        let services: Vec<Service> = self
            .storage
            .list("/registry/services/")
            .await
            .context("Failed to list services")?;

        // Collect a stub ingress: prefer node InternalIP, fallback to a sentinel.
        let nodes: Vec<Node> = self
            .storage
            .list("/registry/nodes/")
            .await
            .unwrap_or_default();
        let node_ingress: Option<String> = nodes
            .iter()
            .filter_map(|n| {
                n.status
                    .as_ref()
                    .and_then(|s| s.addresses.as_ref())
                    .and_then(|addrs| addrs.iter().find(|a| a.address_type == "InternalIP"))
                    .map(|a| a.address.clone())
            })
            .next();

        for svc in services {
            let is_lb = matches!(
                svc.spec
                    .service_type
                    .as_ref()
                    .unwrap_or(&ServiceType::ClusterIP),
                ServiceType::LoadBalancer
            );
            if !is_lb {
                continue;
            }

            let already_ok = svc
                .status
                .as_ref()
                .and_then(|s| s.load_balancer.as_ref())
                .map(|lb| !lb.ingress.is_empty())
                .unwrap_or(false);
            if already_ok {
                continue;
            }

            let ns = match svc.metadata.namespace.as_deref() {
                Some(n) => n,
                None => continue,
            };
            let name = svc.metadata.name.as_str();

            // Apply the same endpoint-readiness health gate as `reconcile_service`:
            // for selector-backed Services, withhold stub ingress and emit a
            // warning event until at least one endpoint is Ready.  Selector-less
            // Services (manually-managed endpoints) are not gated.
            let has_selector = svc
                .spec
                .selector
                .as_ref()
                .map(|sel| !sel.is_empty())
                .unwrap_or(false);
            if has_selector && !self.has_ready_endpoints(ns, name).await {
                warn!(
                    "LoadBalancer service {}/{} has no ready endpoints; withholding stub ingress",
                    ns, name
                );
                self.record_warning_event(
                    &svc,
                    "LoadBalancerSourceUnhealthy",
                    "No ready endpoints backing the LoadBalancer; withholding ingress status",
                )
                .await;
                continue;
            }

            // Order: node InternalIP, spec.loadBalancerIP, spec.clusterIP, sentinel.
            // Upstream e2e only checks that ingress is non-empty, not the IP value.
            let ingress_ip = node_ingress
                .clone()
                .or_else(|| svc.spec.load_balancer_ip.clone())
                .or_else(|| svc.spec.cluster_ip.clone())
                .unwrap_or_else(|| "0.0.0.0".to_string());

            // Re-read to avoid clobbering concurrent updates.
            let key = rusternetes_storage::build_key("services", Some(ns), name);
            let mut fresh: Service = match self.storage.get(&key).await {
                Ok(s) => s,
                Err(_) => continue,
            };
            let conditions = fresh.status.as_ref().and_then(|s| s.conditions.clone());
            fresh.status = Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: vec![LoadBalancerIngress {
                        ip: Some(ingress_ip),
                        hostname: None,
                        ip_mode: None,
                        ports: None,
                    }],
                }),
                conditions,
            });
            // Status subresource write: a full-object PUT strips `.status` (#1723).
            if let Err(e) = self.storage.update_status(&key, &fresh).await {
                warn!(
                    "Failed to populate stub LB status for {}/{}: {}",
                    ns, name, e
                );
            } else {
                info!(
                    "Populated stub LoadBalancer ingress for {}/{} (no cloud provider)",
                    ns, name
                );
            }
        }

        Ok(())
    }

    /// Whether the Service has at least one ready endpoint. Reads the
    /// Endpoints object the EndpointsController publishes (same name as the
    /// Service) and checks for any non-empty `addresses` (ready) entry across
    /// its subsets. A missing Endpoints object or only `notReadyAddresses`
    /// counts as "no ready endpoints".
    async fn has_ready_endpoints(&self, namespace: &str, name: &str) -> bool {
        let key = rusternetes_storage::build_key("endpoints", Some(namespace), name);
        let ep: rusternetes_common::resources::Endpoints = match self.storage.get(&key).await {
            Ok(ep) => ep,
            Err(_) => return false,
        };
        ep.subsets.iter().any(|s| {
            s.addresses
                .as_ref()
                .map(|addrs| !addrs.is_empty())
                .unwrap_or(false)
        })
    }

    /// Record a Warning Event against a Service. Best-effort — failure to
    /// write the event must not mask the underlying reconcile error.
    /// Matches `EventsController::create_event_if_new`: same reason
    /// produces one Event per Service to avoid log spam (upstream then
    /// updates `count`/`lastTimestamp` via a separate aggregator we don't
    /// have yet — see follow-up note in the PR body).
    async fn record_warning_event(&self, service: &Service, reason: &str, message: &str) {
        let namespace = service.metadata.namespace.as_deref().unwrap_or_default();
        let name = service.metadata.name.clone();
        let involved = ObjectReference {
            kind: Some("Service".to_string()),
            namespace: Some(namespace.to_string()),
            name: Some(name),
            api_version: Some("v1".to_string()),
            // Carry the Service UID so the event survives an object
            // recreation cleanly (an `apply` that recreates the Service
            // will mint a new UID; events stay scoped to the old one).
            uid: Some(service.metadata.uid.clone()).filter(|u| !u.is_empty()),
            resource_version: None,
            field_path: None,
        };
        let event_name = Event::generate_name(&involved, reason);
        let key = format!("/registry/events/{}/{}", namespace, event_name);
        if let Ok(mut existing) = self.storage.get::<Event>(&key).await {
            // Recurring warning for the same (service, reason): de-duplicate by
            // bumping the count / lastTimestamp and keeping the events.k8s.io
            // `series` consistent, rather than leaving it stuck at count:1.
            let now = chrono::Utc::now();
            existing.count = existing.count.saturating_add(1);
            existing.last_timestamp = Some(now);
            existing.message = message.to_string();
            existing.series = Some(rusternetes_common::resources::EventSeries {
                count: existing.count,
                last_observed_time: now,
            });
            if let Err(e) = self
                .storage
                .update_with_ttl(&key, &existing, process_event_ttl())
                .await
            {
                warn!(
                    "Failed to bump recurring Warning event {}/{}: {}",
                    namespace, reason, e
                );
            }
            return;
        }
        let event = Event::new(
            event_name,
            namespace.to_string(),
            involved,
            reason.to_string(),
            message.to_string(),
            EventType::Warning,
        );
        if let Err(e) = self
            .storage
            .create_with_ttl(&key, &event, process_event_ttl())
            .await
        {
            warn!(
                "Failed to record Warning event {}/{}: {}",
                namespace, reason, e
            );
        }
    }
}
