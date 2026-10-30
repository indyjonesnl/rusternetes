//! Port of `pkg/registry/core/service/ipallocator/controller/repairip.go`:
//! keeps the one-to-one relation between Service ClusterIPs and IPAddress
//! objects. It creates a missing IPAddress (this is also how Services from
//! before the IPAddress allocator are migrated), fixes a wrong reference or
//! wrong labels, and deletes an allocator-managed IPAddress whose Service is
//! gone.
//!
//! Upstream runs `runOnce` once and then reacts to Service and IPAddress
//! informer events, with a resync every interval. Rusternetes has no
//! informers in the api-server: `run_once` runs every interval (the
//! resync), and `spawn_workers` adds the event-driven workers over the
//! storage watch, with the same queues, 5 workers each, `maxRetries` and
//! `AddAfter` requeue of a leaked IPAddress.
//!
//! Reads go to storage (upstream's listers); every IPAddress write goes
//! through `r.client` (repairip.go:373, :430, :434, :495, :512, :552), the
//! api-server's loopback client (storage_core.go:142-148), so the IPAddress
//! Store and admission see the repair's creates and deletes.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use ipnet::IpNet;
use rusternetes_common::resources::{
    EventSource, EventType, IPAddress, ObjectReference, Service, ServiceCIDR,
};
use rusternetes_common::{Error, Result};
use rusternetes_storage::workqueue::{extract_key, WorkQueue};
use rusternetes_storage::{build_key, EventRecorder, Storage};
use tracing::{error, warn};

use super::{
    family_of, ip_address_key, new_ip_address, prefix_contains_ip, IpAddressClient,
    CONTROLLER_NAME, IP_ADDRESS_PREFIX, LABEL_IP_ADDRESS_FAMILY, LABEL_MANAGED_BY,
};

/// `maxRetries` (repairip.go:57): a key is retried this many times before
/// it is dropped out of the queue.
const MAX_RETRIES: u32 = 15;

/// `workers` (repairip.go:58): workers per queue.
const WORKERS: usize = 5;

/// The recorder's component (repairip.go:118).
const COMPONENT: &str = "ipallocator-repair-controller";

/// How long an IPAddress without its Service is left alone, covering the
/// window between the allocation and the Service write (repairip.go:541).
const LEAK_GRACE_PERIOD: Duration = Duration::from_secs(60);

/// How recent an IPAddress created after its Service has to be to be left
/// alone, covering a create transaction that is being reverted
/// (repairip.go:569-575).
const REVERT_GRACE_PERIOD: Duration = Duration::from_secs(5);

/// `defaultservicecidr.DefaultServiceCIDRName`.
pub const DEFAULT_SERVICE_CIDR_NAME: &str = "kubernetes";

/// `RepairIPAddress` (repairip.go:83-103).
pub struct RepairIpAddress<S: Storage + 'static> {
    /// `client kubernetes.Interface` (repairip.go:90): the IPAddress
    /// writes.
    client: Arc<dyn IpAddressClient>,
    storage: Arc<S>,
    recorder: EventRecorder<S>,
    /// `clock.Clock` (repairip.go:102), injectable for the tests.
    clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

/// `helper.IsServiceIPSet`.
fn is_service_ip_set(svc: &Service) -> bool {
    !matches!(
        svc.spec.cluster_ip.as_deref(),
        None | Some("") | Some("None")
    )
}

/// `svc.Spec.ClusterIPs`. A Service stored before `clusterIPs` was kept
/// in step carries only `clusterIP`.
fn cluster_ips(svc: &Service) -> Vec<String> {
    match &svc.spec.cluster_ips {
        Some(ips) if !ips.is_empty() => ips.clone(),
        _ => svc.spec.cluster_ip.clone().into_iter().collect(),
    }
}

fn label<'a>(ip: &'a IPAddress, key: &str) -> Option<&'a str> {
    ip.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(key))
        .map(String::as_str)
}

/// `managedByController` (repairip.go:612-619).
fn managed_by_controller(ip: &IPAddress) -> bool {
    label(ip, LABEL_MANAGED_BY) == Some(CONTROLLER_NAME)
}

/// `verifyIPAddressLabels` (repairip.go:621-634).
fn verify_ip_address_labels(ip: &IPAddress) -> bool {
    let family = ip
        .metadata
        .name
        .parse::<IpAddr>()
        .map(|a| family_of(&a))
        .unwrap_or("IPv4");
    label(ip, LABEL_IP_ADDRESS_FAMILY) == Some(family) && managed_by_controller(ip)
}

/// Whether the parent reference points at a Service at all.
fn references_a_service(ip: &IPAddress) -> bool {
    ip.spec
        .as_ref()
        .and_then(|s| s.parent_ref.as_ref())
        .is_some_and(|r| r.group.as_deref().unwrap_or("").is_empty() && r.resource == "services")
}

fn parent(ip: &IPAddress) -> (String, String) {
    let r = ip.spec.as_ref().and_then(|s| s.parent_ref.as_ref());
    (
        r.and_then(|r| r.namespace.clone()).unwrap_or_default(),
        r.map(|r| r.name.clone()).unwrap_or_default(),
    )
}

pub fn svc_key(ns: &str, name: &str) -> String {
    build_key("services", Some(ns), name)
}

impl<S: Storage + 'static> RepairIpAddress<S> {
    /// `NewRepairIPAddress` (repairip.go:105-113).
    pub fn new(storage: Arc<S>, client: Arc<dyn IpAddressClient>) -> Self {
        Self {
            client,
            recorder: EventRecorder::new(storage.clone()),
            storage,
            clock: Arc::new(Utc::now),
        }
    }

    /// `newRepairIPAddress`'s clock (repairip.go:115-125).
    pub fn with_clock(mut self, now: DateTime<Utc>) -> Self {
        self.clock = Arc::new(move || now);
        self
    }

    async fn event(&self, involved: ObjectReference, reason: &str, message: String) {
        let source = EventSource {
            component: COMPONENT.to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(&involved, &source, EventType::Warning, reason, &message)
            .await
        {
            warn!("{COMPONENT}: could not record {reason} event: {e}");
        }
    }

    async fn svc_event(&self, svc: &Service, reason: &str, message: String) {
        let involved = ObjectReference {
            kind: Some("Service".to_string()),
            namespace: svc.metadata.namespace.clone(),
            name: Some(svc.metadata.name.clone()),
            uid: Some(svc.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            resource_version: svc.metadata.resource_version.clone(),
            field_path: None,
        };
        self.event(involved, reason, message).await;
    }

    async fn ip_event(&self, ip: &IPAddress, reason: &str, message: String) {
        let involved = ObjectReference {
            kind: Some("IPAddress".to_string()),
            namespace: None,
            name: Some(ip.metadata.name.clone()),
            uid: Some(ip.metadata.uid.clone()),
            api_version: Some("networking.k8s.io/v1".to_string()),
            resource_version: ip.metadata.resource_version.clone(),
            field_path: None,
        };
        self.event(involved, reason, message).await;
    }

    /// `RunUntil`'s wait for the default ServiceCIDR (repairip.go:196-206).
    pub async fn default_service_cidr_exists(&self) -> bool {
        self.storage
            .get::<ServiceCIDR>(&build_key("servicecidrs", None, DEFAULT_SERVICE_CIDR_NAME))
            .await
            .is_ok()
    }

    /// `runOnce` (repairip.go:223-226): `doRunOnce` under
    /// `retry.RetryOnConflict(retry.DefaultBackoff, ...)`.
    pub async fn run_once(&self) -> Result<()> {
        let mut delay = Duration::from_millis(10);
        let mut steps = 4;
        loop {
            match self.do_run_once().await {
                Err(Error::Conflict(_)) if steps > 1 => {
                    steps -= 1;
                    tokio::time::sleep(delay).await;
                    delay *= 5;
                }
                other => return other,
            }
        }
    }

    /// `doRunOnce` (repairip.go:228-270).
    async fn do_run_once(&self) -> Result<()> {
        let services: Vec<Service> = self
            .storage
            .list("/registry/services/")
            .await
            .map_err(|e| Error::Internal(format!("unable to refresh the service IP block: {e}")))?;
        let service_cidrs: Vec<ServiceCIDR> = self.storage.list("/registry/servicecidrs/").await?;

        // Check every Service's ClusterIP, and rebuild the state as it
        // should be.
        for svc in &services {
            self.sync_service(svc, &service_cidrs).await?;
        }

        // Every Service has its IPAddress; now make sure no IPAddress the
        // allocator created is left without a Service.
        let ip_addresses: Vec<IPAddress> =
            self.storage.list(IP_ADDRESS_PREFIX).await.map_err(|e| {
                Error::Internal(format!("unable to refresh the IPAddress block: {e}"))
            })?;
        for ip in ip_addresses.iter().filter(|ip| managed_by_controller(ip)) {
            self.sync_ip_address(ip).await?;
        }
        Ok(())
    }

    /// `isIPOutOfRange` (repairip.go:588-591).
    fn is_ip_out_of_range(service_cidrs: &[ServiceCIDR], ip: &IpAddr) -> bool {
        !service_cidrs.iter().any(|sc| {
            sc.spec.as_ref().is_some_and(|spec| {
                spec.cidrs.iter().any(|c| {
                    c.parse::<IpNet>()
                        .is_ok_and(|prefix| prefix_contains_ip(&prefix, ip))
                })
            })
        })
    }

    /// `syncService` (repairip.go:307-415).
    async fn sync_service(&self, svc: &Service, service_cidrs: &[ServiceCIDR]) -> Result<()> {
        if !is_service_ip_set(svc) {
            // Didn't need a ClusterIP.
            return Ok(());
        }
        let ns = svc.metadata.namespace.as_deref().unwrap_or("");
        let name = &svc.metadata.name;
        for cluster_ip in cluster_ips(svc) {
            let Ok(ip) = cluster_ip.parse::<IpAddr>() else {
                self.svc_event(
                    svc,
                    "ClusterIPNotValid",
                    format!("Cluster IP {cluster_ip} is not a valid IP; please recreate Service"),
                )
                .await;
                error!("the ClusterIP {cluster_ip} for Service {ns}/{name} is not a valid IP; please recreate Service");
                continue;
            };
            let family = family_of(&ip);
            if Self::is_ip_out_of_range(service_cidrs, &ip) {
                self.svc_event(svc, "ClusterIPOutOfRange", format!("Cluster IP [{family}]: {ip} is not within any configured Service CIDR; please recreate service")).await;
                error!("the ClusterIP [{family}]: {ip} for Service {ns}/{name} is not within any service CIDR; please recreate");
                continue;
            }

            let ip_address = match self
                .storage
                .get::<IPAddress>(&ip_address_key(&ip.to_string()))
                .await
            {
                Ok(a) => a,
                Err(Error::NotFound(_)) => {
                    // Not allocated: create it.
                    self.svc_event(
                        svc,
                        "ClusterIPNotAllocated",
                        format!("Cluster IP [{family}]: {ip} is not allocated; repairing"),
                    )
                    .await;
                    error!("the ClusterIP [{family}]: {ip} for Service {ns}/{name} is not allocated; repairing");
                    let name = ip.to_string();
                    self.client.create(new_ip_address(&name, Some(svc))).await?;
                    continue;
                }
                Err(e) => {
                    self.svc_event(
                        svc,
                        "UnknownError",
                        format!(
                            "Unable to allocate ClusterIP [{family}]: {ip} due to an unknown error"
                        ),
                    )
                    .await;
                    return Err(Error::Internal(format!("unable to allocate ClusterIP [{family}]: {ip} for Service {ns}/{name} due to an unknown error, will retry later: {e}")));
                }
            };

            // An IPAddress that belongs to a Service must reference one.
            if !references_a_service(&ip_address) {
                self.svc_event(svc, "ClusterIPNotAllocated", format!("the ClusterIP [{family}]: {ip} for Service {ns}/{name} has a wrong reference; repairing")).await;
                self.recreate_ip_address(&ip_address.metadata.name, svc)
                    .await?;
                continue;
            }

            // ...and this Service in particular.
            let (ref_ns, ref_name) = parent(&ip_address);
            if ref_ns != ns || ref_name != *name {
                // Two Services with one IP would otherwise flip the
                // reference forever.
                match self
                    .storage
                    .get::<Service>(&svc_key(&ref_ns, &ref_name))
                    .await
                {
                    Err(_) => {
                        self.svc_event(svc, "ClusterIPNotAllocated", format!("the ClusterIP [{family}]: {ip} for Service {ns}/{name} has a wrong reference; repairing")).await;
                        self.recreate_ip_address(&ip_address.metadata.name, svc)
                            .await?;
                        continue;
                    }
                    Ok(ref_service) => {
                        if cluster_ips(&ref_service).contains(&ip_address.metadata.name) {
                            self.svc_event(svc, "ClusterIPAlreadyAllocated", format!("Cluster IP [{family}]:{ip} was assigned to multiple services; please recreate service")).await;
                            error!("the cluster IP [{family}]:{ip} for service {ns}/{name} was assigned to other services {ref_ns}/{ref_name}; please recreate");
                        }
                    }
                }
            }

            // It must carry the allocator's labels.
            if !verify_ip_address_labels(&ip_address) {
                self.recreate_ip_address(&ip_address.metadata.name, svc)
                    .await?;
                continue;
            }
        }
        Ok(())
    }

    /// `recreateIPAddress` (repairip.go:417-427).
    async fn recreate_ip_address(&self, name: &str, svc: &Service) -> Result<()> {
        match self.client.delete(name).await {
            Ok(()) | Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        self.client.create(new_ip_address(name, Some(svc))).await
    }

    /// `r.client.NetworkingV1().IPAddresses().Delete(...)` with
    /// `!apierrors.IsNotFound(err)` (repairip.go:495-498, :512-515,
    /// :552-555).
    async fn delete_ip_address(&self, name: &str) -> Result<()> {
        match self.client.delete(name).await {
            Ok(()) | Err(Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// `syncIPAddress` (repairip.go:465-585).
    pub async fn sync_ip_address(&self, ip_address: &IPAddress) -> Result<Option<Duration>> {
        // Not managed by this controller.
        if !managed_by_controller(ip_address) {
            return Ok(None);
        }
        let name = &ip_address.metadata.name;
        let spec_ref = ip_address.spec.as_ref().and_then(|s| s.parent_ref.as_ref());
        if !references_a_service(ip_address) {
            error!("IPAddress {name} appears to have been modified, not referencing a Service {spec_ref:?}: cleaning up");
            self.ip_event(ip_address, "IPAddressNotAllocated", format!("IPAddress {name} appears to have been modified, not referencing a Service {spec_ref:?}: cleaning up")).await;
            self.delete_ip_address(name).await?;
            return Ok(None);
        }

        let (ref_ns, ref_name) = parent(ip_address);
        let now = (self.clock)();
        let created = ip_address.metadata.creation_timestamp.unwrap_or(now);
        let lifetime = (now - created).to_std().unwrap_or_default();
        let svc = match self
            .storage
            .get::<Service>(&svc_key(&ref_ns, &ref_name))
            .await
        {
            Ok(svc) => svc,
            Err(Error::NotFound(_)) => {
                // No Service: clean up once the create window has passed.
                if lifetime > LEAK_GRACE_PERIOD {
                    error!("IPAddress {name} appears to have leaked: cleaning up");
                    self.ip_event(ip_address, "IPAddressNotAllocated", format!("IPAddress: {name} for Service {ref_ns}/{ref_name} appears to have leaked: cleaning up")).await;
                    self.delete_ip_address(name).await?;
                    return Ok(None);
                }
                // `r.ipQueue.AddAfter(key, gracePeriod-ipLifetime)`
                // (repairip.go:553).
                return Ok(Some(LEAK_GRACE_PERIOD - lifetime));
            }
            Err(e) => {
                error!("unable to get parent Service for IPAddress {name} due to an unknown error: {e}");
                self.ip_event(
                    ip_address,
                    "UnknownError",
                    format!(
                        "Unable to get parent Service for IPAddress {name} due to an unknown error"
                    ),
                )
                .await;
                return Err(e);
            }
        };

        // The Service loop checked Service -> IPAddress; check the reverse.
        if cluster_ips(&svc).contains(name) {
            return Ok(None);
        }

        // A create that is being reverted: its IPAddress is newer than the
        // Service it names (repairip.go:560-575).
        let svc_created = svc.metadata.creation_timestamp.unwrap_or(now);
        if created > svc_created && lifetime < REVERT_GRACE_PERIOD {
            // `r.ipQueue.AddAfter(key, 5*time.Second)` (repairip.go:572).
            return Ok(Some(REVERT_GRACE_PERIOD));
        }
        error!("the IPAddress: {name} for Service {ref_name}/{ref_ns} has a wrong reference {spec_ref:?}; cleaning up");
        self.ip_event(
            ip_address,
            "IPAddressWrongReference",
            format!("IPAddress: {name} for Service {ref_ns}/{ref_name} has a wrong reference; cleaning up"),
        )
        .await;
        self.delete_ip_address(name).await?;
        Ok(None)
    }

    /// `syncService(key)` for one queue key (`namespace/name`): the
    /// lister read (a deleted Service is nothing to do), then
    /// `syncService`.
    async fn sync_service_key(&self, key: &str) -> Result<()> {
        let (ns, name) = key.split_once('/').unwrap_or(("", key));
        let svc = match self.storage.get::<Service>(&svc_key(ns, name)).await {
            Ok(svc) => svc,
            Err(Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        let service_cidrs: Vec<ServiceCIDR> = self.storage.list("/registry/servicecidrs/").await?;
        self.sync_service(&svc, &service_cidrs).await
    }

    /// `syncIPAddress(key)` for one queue key (the IPAddress name): the
    /// lister read (gone is nothing to do), then `syncIPAddress`.
    async fn sync_ip_address_key(&self, key: &str) -> Result<Option<Duration>> {
        match self.storage.get::<IPAddress>(&ip_address_key(key)).await {
            Ok(ip) => self.sync_ip_address(&ip).await,
            Err(_) => Ok(None),
        }
    }

    /// The event-driven half of `RunUntil` (repairip.go:208-216): the
    /// Service and IPAddress event handlers feeding `svcQueue` /
    /// `ipQueue` (:130-178) and `workers` workers on each. The caller has
    /// already run `run_once`. Rusternetes has no informers in the
    /// api-server, so the handlers read the storage watch instead.
    pub fn spawn_workers(self: &Arc<Self>) -> Vec<tokio::task::JoinHandle<()>> {
        let svc_queue = WorkQueue::new();
        let ip_queue = WorkQueue::new();
        let mut handles = vec![
            self.clone()
                .spawn_event_handler("/registry/services/", svc_queue.clone()),
            self.clone()
                .spawn_event_handler(IP_ADDRESS_PREFIX, ip_queue.clone()),
        ];
        for _ in 0..WORKERS {
            handles.push(tokio::spawn(self.clone().svc_worker(svc_queue.clone())));
            handles.push(tokio::spawn(self.clone().ip_worker(ip_queue.clone())));
        }
        handles
    }

    /// `AddFunc` / `UpdateFunc` / `DeleteFunc` (repairip.go:130-178): every
    /// event enqueues the object's key. A broken watch is re-opened.
    fn spawn_event_handler(
        self: Arc<Self>,
        prefix: &'static str,
        queue: WorkQueue,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match self.storage.watch(prefix).await {
                    Ok(mut stream) => {
                        while let Some(event) = stream.next().await {
                            if let Ok(event) = event {
                                let key = extract_key(&event);
                                let key = key
                                    .strip_prefix(prefix.trim_start_matches("/registry/"))
                                    .unwrap_or(&key);
                                queue.add(key.to_string()).await;
                            }
                        }
                    }
                    Err(e) => warn!("{COMPONENT}: could not watch {prefix}: {e}"),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    }

    /// `svcWorker` / `processNextWorkSvc` / `handleSvcErr`
    /// (repairip.go:327-363).
    async fn svc_worker(self: Arc<Self>, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let result = self.sync_service_key(&key).await;
            handle_err(&queue, "Service", &key, result.map(|()| None)).await;
            queue.done(&key).await;
        }
    }

    /// `ipWorker` / `processNextWorkIP` / `handleIPErr`
    /// (repairip.go:435-463), plus the `AddAfter` requeues of
    /// `syncIPAddress`.
    async fn ip_worker(self: Arc<Self>, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let result = self.sync_ip_address_key(&key).await;
            handle_err(&queue, "IPAddress", &key, result).await;
            queue.done(&key).await;
        }
    }
}

/// `handleSvcErr` / `handleIPErr`: forget on success (re-adding after
/// the requested delay), rate-limited retry up to `MAX_RETRIES`, then drop.
async fn handle_err(queue: &WorkQueue, kind: &str, key: &str, result: Result<Option<Duration>>) {
    match result {
        Ok(requeue_after) => {
            queue.forget(key).await;
            if let Some(after) = requeue_after {
                queue.add_after(key.to_string(), after).await;
            }
        }
        Err(e) if queue.num_requeues(key).await < MAX_RETRIES => {
            tracing::debug!("{COMPONENT}: error syncing {kind} {key}, retrying: {e}");
            queue.requeue_rate_limited(key.to_string()).await;
        }
        Err(e) => {
            warn!("{COMPONENT}: dropping {kind} {key:?} out of the queue: {e}");
            queue.forget(key).await;
        }
    }
}
