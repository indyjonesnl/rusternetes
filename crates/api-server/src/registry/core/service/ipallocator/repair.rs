//! Port of `pkg/registry/core/service/ipallocator/controller/repairip.go`:
//! keeps the one-to-one relation between Service ClusterIPs and IPAddress
//! objects. It creates a missing IPAddress (this is also how Services from
//! before the IPAddress allocator are migrated), fixes a wrong reference or
//! wrong labels, and deletes an allocator-managed IPAddress whose Service is
//! gone.
//!
//! Upstream runs `runOnce` once and then reacts to Service and IPAddress
//! informer events, with a resync every interval. Rusternetes has no
//! informers in the api-server, so the loop runs `run_once` every interval
//! instead. That is the resync alone, without the event-driven workers.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ipnet::IpNet;
use rusternetes_common::resources::{
    EventSource, EventType, IPAddress, ObjectReference, Service, ServiceCIDR,
};
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_key, EventRecorder, Storage};
use tracing::{error, warn};

use super::{
    family_of, ip_address_key, new_ip_address, prefix_contains_ip, CONTROLLER_NAME,
    IP_ADDRESS_PREFIX, LABEL_IP_ADDRESS_FAMILY, LABEL_MANAGED_BY,
};

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
const DEFAULT_SERVICE_CIDR_NAME: &str = "kubernetes";

/// `RepairIPAddress` (repairip.go:83-103).
pub struct RepairIpAddress<S: Storage + 'static> {
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

fn svc_key(ns: &str, name: &str) -> String {
    build_key("services", Some(ns), name)
}

impl<S: Storage + 'static> RepairIpAddress<S> {
    /// `NewRepairIPAddress` (repairip.go:105-113).
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            recorder: EventRecorder::new(storage.clone()),
            storage,
            clock: Arc::new(Utc::now),
        }
    }

    /// `newRepairIPAddress`'s clock (repairip.go:115-125).
    #[cfg(test)]
    fn with_clock(mut self, now: DateTime<Utc>) -> Self {
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
                    match self
                        .storage
                        .create(&ip_address_key(&name), &new_ip_address(&name, Some(svc)))
                        .await
                    {
                        Ok(_) | Err(Error::AlreadyExists(_)) => {}
                        Err(e) => return Err(e),
                    }
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
        match self.storage.delete(&ip_address_key(name)).await {
            Ok(()) | Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        self.storage
            .create(&ip_address_key(name), &new_ip_address(name, Some(svc)))
            .await?;
        Ok(())
    }

    async fn delete_ip_address(&self, name: &str) -> Result<()> {
        match self.storage.delete(&ip_address_key(name)).await {
            Ok(()) | Err(Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// `syncIPAddress` (repairip.go:465-585).
    async fn sync_ip_address(&self, ip_address: &IPAddress) -> Result<()> {
        // Not managed by this controller.
        if !managed_by_controller(ip_address) {
            return Ok(());
        }
        let name = &ip_address.metadata.name;
        let spec_ref = ip_address.spec.as_ref().and_then(|s| s.parent_ref.as_ref());
        if !references_a_service(ip_address) {
            error!("IPAddress {name} appears to have been modified, not referencing a Service {spec_ref:?}: cleaning up");
            self.ip_event(ip_address, "IPAddressNotAllocated", format!("IPAddress {name} appears to have been modified, not referencing a Service {spec_ref:?}: cleaning up")).await;
            return self.delete_ip_address(name).await;
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
                }
                return Ok(());
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
            return Ok(());
        }

        // A create that is being reverted: its IPAddress is newer than the
        // Service it names (repairip.go:560-575).
        let svc_created = svc.metadata.creation_timestamp.unwrap_or(now);
        if created > svc_created && lifetime < REVERT_GRACE_PERIOD {
            return Ok(());
        }
        error!("the IPAddress: {name} for Service {ref_name}/{ref_ns} has a wrong reference {spec_ref:?}; cleaning up");
        self.ip_event(
            ip_address,
            "IPAddressWrongReference",
            format!("IPAddress: {name} for Service {ref_ns}/{ref_name} has a wrong reference; cleaning up"),
        )
        .await;
        self.delete_ip_address(name).await
    }
}

#[cfg(test)]
mod tests {
    //! Ports of controller/repairip_test.go.
    use super::*;
    use chrono::TimeZone;
    use rusternetes_common::resources::Event;
    use rusternetes_storage::MemoryStorage;
    use serde_json::json;

    const V4: &str = "10.0.0.0/16";
    const V6: &str = "2001:db8::/64";

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2012, 1, 1, 0, 0, 0).unwrap()
    }

    fn secs(n: i64) -> DateTime<Utc> {
        t0() + chrono::Duration::seconds(n)
    }

    /// `newServiceWithCreationTimestamp` (repairip_test.go).
    fn svc(name: &str, ips: &[&str], created: DateTime<Utc>) -> Service {
        let mut s: Service = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": name, "namespace": "bar"},
            "spec": {"clusterIP": ips[0], "clusterIPs": ips},
        }))
        .unwrap();
        s.metadata.creation_timestamp = Some(created);
        s
    }

    /// `newIPAddressWithCreationTimestamp`.
    fn ip_address(name: &str, svc_name: &str, created: DateTime<Utc>) -> IPAddress {
        let mut ip = new_ip_address(name, Some(&svc(svc_name, &[name], t0())));
        ip.metadata.creation_timestamp = Some(created);
        ip
    }

    fn cidr(name: &str, cidrs: &[&str]) -> ServiceCIDR {
        ServiceCIDR::new(
            name,
            cidrs
                .iter()
                .filter(|c| !c.is_empty())
                .map(|c| c.to_string())
                .collect(),
        )
    }

    struct Case {
        name: &'static str,
        svcs: Vec<Service>,
        ip_addresses: Vec<IPAddress>,
        cidrs: Vec<ServiceCIDR>,
        test_time: DateTime<Utc>,
        expected_ips: &'static [&'static str],
        /// IPAddresses that must be gone afterwards.
        deleted_ips: &'static [&'static str],
        events: &'static [&'static str],
    }

    async fn run(tc: Case) {
        let storage = Arc::new(MemoryStorage::new());
        for c in &tc.cidrs {
            storage
                .create(&build_key("servicecidrs", None, &c.metadata.name), c)
                .await
                .unwrap();
        }
        for s in &tc.svcs {
            storage
                .create(&svc_key("bar", &s.metadata.name), s)
                .await
                .unwrap();
        }
        for ip in &tc.ip_addresses {
            storage
                .create(&ip_address_key(&ip.metadata.name), ip)
                .await
                .unwrap();
        }
        let r = RepairIpAddress::new(storage.clone()).with_clock(tc.test_time);
        r.run_once()
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", tc.name));

        for name in tc.expected_ips {
            assert!(
                storage
                    .get::<IPAddress>(&ip_address_key(name))
                    .await
                    .is_ok(),
                "{}: IPAddress {name} missing",
                tc.name
            );
        }
        for name in tc.deleted_ips {
            assert!(
                storage
                    .get::<IPAddress>(&ip_address_key(name))
                    .await
                    .is_err(),
                "{}: IPAddress {name} should be gone",
                tc.name
            );
        }
        // One Event per (object, reason): repeated reasons aggregate.
        let events: Vec<Event> = storage.list("/registry/events/").await.unwrap();
        let got: HashSet<String> = events
            .iter()
            .map(|e| format!("Warning {} {}", e.reason, e.message))
            .collect();
        let want_reasons: HashSet<&str> = tc
            .events
            .iter()
            .map(|e| e.split(' ').nth(1).unwrap())
            .collect();
        let got_reasons: HashSet<&str> = got.iter().map(|e| e.split(' ').nth(1).unwrap()).collect();
        assert_eq!(got_reasons, want_reasons, "{}: events {got:?}", tc.name);
        for e in &got {
            assert!(
                tc.events.contains(&e.as_str()),
                "{}: unexpected event {e}",
                tc.name
            );
        }
    }

    use std::collections::HashSet;

    #[tokio::test]
    async fn repair_service_ip() {
        let k8s = || vec![cidr("kubernetes", &[V4, V6])];
        let cases = vec![
            Case {
                name: "no changes needed single stack",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                events: &[],
            },
            Case {
                name: "no changes needed dual stack",
                svcs: vec![svc("test-svc", &["10.0.1.1", "2001:db8::10"], secs(1))],
                ip_addresses: vec![
                    ip_address("10.0.1.1", "test-svc", t0()),
                    ip_address("2001:db8::10", "test-svc", t0()),
                ],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1", "2001:db8::10"],
                deleted_ips: &[],
                events: &[],
            },
            Case {
                name: "no changes needed dual stack multiple cidrs",
                svcs: vec![svc("test-svc", &["192.168.0.1", "2001:db8:a:b::10"], secs(1))],
                ip_addresses: vec![
                    ip_address("192.168.0.1", "test-svc", t0()),
                    ip_address("2001:db8:a:b::10", "test-svc", t0()),
                ],
                cidrs: vec![
                    cidr("kubernetes", &[V4, V6]),
                    cidr("custom", &["192.168.0.0/24", "2001:db8:a:b::/64"]),
                ],
                test_time: t0(),
                expected_ips: &["192.168.0.1", "2001:db8:a:b::10"],
                deleted_ips: &[],
                events: &[],
            },
            Case {
                name: "create IPAddress single stack",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                events: &["Warning ClusterIPNotAllocated Cluster IP [IPv4]: 10.0.1.1 is not allocated; repairing"],
            },
            Case {
                name: "create IPAddresses dual stack",
                svcs: vec![svc("test-svc", &["10.0.1.1", "2001:db8::10"], secs(1))],
                ip_addresses: vec![],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1", "2001:db8::10"],
                deleted_ips: &[],
                events: &[
                    "Warning ClusterIPNotAllocated Cluster IP [IPv4]: 10.0.1.1 is not allocated; repairing",
                    "Warning ClusterIPNotAllocated Cluster IP [IPv6]: 2001:db8::10 is not allocated; repairing",
                ],
            },
            Case {
                name: "create IPAddress single stack from secondary",
                svcs: vec![svc("test-svc", &["192.168.1.1"], secs(1))],
                ip_addresses: vec![],
                cidrs: vec![cidr("kubernetes", &[V4, V6]), cidr("custom", &["192.168.1.0/24", ""])],
                test_time: t0(),
                expected_ips: &["192.168.1.1"],
                deleted_ips: &[],
                events: &["Warning ClusterIPNotAllocated Cluster IP [IPv4]: 192.168.1.1 is not allocated; repairing"],
            },
            Case {
                name: "reconcile IPAddress single stack wrong reference",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc2", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                events: &["Warning ClusterIPNotAllocated the ClusterIP [IPv4]: 10.0.1.1 for Service bar/test-svc has a wrong reference; repairing"],
            },
            Case {
                name: "one IP out of range",
                svcs: vec![svc("test-svc", &["192.168.1.1", "2001:db8::10"], secs(1))],
                ip_addresses: vec![
                    ip_address("192.168.1.1", "test-svc", t0()),
                    ip_address("2001:db8::10", "test-svc", t0()),
                ],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["2001:db8::10"],
                deleted_ips: &[],
                events: &["Warning ClusterIPOutOfRange Cluster IP [IPv4]: 192.168.1.1 is not within any configured Service CIDR; please recreate service"],
            },
            Case {
                name: "one IP orphan within grace period",
                svcs: vec![],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                events: &[],
            },
            Case {
                name: "one IP orphan after grace period",
                svcs: vec![],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc", t0())],
                cidrs: k8s(),
                test_time: secs(70),
                expected_ips: &[],
                deleted_ips: &["10.0.1.1"],
                events: &["Warning IPAddressNotAllocated IPAddress: 10.0.1.1 for Service bar/test-svc appears to have leaked: cleaning up"],
            },
            Case {
                name: "one IP out of range matching the network address",
                svcs: vec![svc("test-svc", &["10.0.0.0"], secs(1))],
                ip_addresses: vec![ip_address("10.0.0.0", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.0.0"],
                deleted_ips: &[],
                events: &["Warning ClusterIPOutOfRange Cluster IP [IPv4]: 10.0.0.0 is not within any configured Service CIDR; please recreate service"],
            },
            Case {
                name: "one IP out of range matching the broadcast address",
                svcs: vec![svc("test-svc", &["10.0.255.255"], secs(1))],
                ip_addresses: vec![ip_address("10.0.255.255", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.255.255"],
                deleted_ips: &[],
                events: &["Warning ClusterIPOutOfRange Cluster IP [IPv4]: 10.0.255.255 is not within any configured Service CIDR; please recreate service"],
            },
            Case {
                name: "one IPv6 out of range matching the subnet address",
                svcs: vec![svc("test-svc", &["2001:db8::"], secs(1))],
                ip_addresses: vec![ip_address("2001:db8::", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["2001:db8::"],
                deleted_ips: &[],
                events: &["Warning ClusterIPOutOfRange Cluster IP [IPv6]: 2001:db8:: is not within any configured Service CIDR; please recreate service"],
            },
            Case {
                name: "one IPv6 matching the broadcast address",
                svcs: vec![svc("test-svc", &["2001:db8::ffff:ffff:ffff:ffff"], secs(1))],
                ip_addresses: vec![ip_address("2001:db8::ffff:ffff:ffff:ffff", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["2001:db8::ffff:ffff:ffff:ffff"],
                deleted_ips: &[],
                events: &[],
            },
            Case {
                name: "Two IPAddresses referencing the same service",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![
                    ip_address("10.0.1.1", "test-svc", t0()),
                    ip_address("10.0.1.2", "test-svc", t0()),
                ],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &["10.0.1.2"],
                events: &["Warning IPAddressWrongReference IPAddress: 10.0.1.2 for Service bar/test-svc has a wrong reference; cleaning up"],
            },
            Case {
                name: "Two IPAddresses referencing the same service, the second in its grace period",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![
                    ip_address("10.0.1.1", "test-svc", t0()),
                    ip_address("10.0.1.2", "test-svc", secs(2)),
                ],
                cidrs: k8s(),
                test_time: secs(3),
                expected_ips: &["10.0.1.1", "10.0.1.2"],
                deleted_ips: &[],
                events: &[],
            },
            Case {
                name: "Two IPAddresses referencing the same service, the second past its grace period",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![
                    ip_address("10.0.1.1", "test-svc", t0()),
                    ip_address("10.0.1.2", "test-svc", secs(2)),
                ],
                cidrs: k8s(),
                test_time: secs(10),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &["10.0.1.2"],
                events: &["Warning IPAddressWrongReference IPAddress: 10.0.1.2 for Service bar/test-svc has a wrong reference; cleaning up"],
            },
            Case {
                name: "Two Services with same ClusterIP",
                svcs: vec![
                    svc("test-svc", &["10.0.1.1"], secs(1)),
                    svc("test-svc2", &["10.0.1.1"], secs(1)),
                ],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc2", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                events: &["Warning ClusterIPAlreadyAllocated Cluster IP [IPv4]:10.0.1.1 was assigned to multiple services; please recreate service"],
            },
        ];
        for tc in cases {
            run(tc).await;
        }
    }

    /// An IPAddress another controller manages is left alone, whatever it
    /// references (`TestRepairIPAddress_syncIPAddress`, "not managed by this
    /// controller").
    #[tokio::test]
    async fn an_ip_address_managed_elsewhere_is_ignored() {
        let storage = Arc::new(MemoryStorage::new());
        let mut ip = ip_address("2001:db8::11", "foo", t0());
        ip.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(LABEL_MANAGED_BY.to_string(), "controller-foo".to_string());
        storage
            .create(&ip_address_key("2001:db8::11"), &ip)
            .await
            .unwrap();
        let r = RepairIpAddress::new(storage.clone()).with_clock(secs(3600));
        r.sync_ip_address(&ip).await.unwrap();
        assert!(storage
            .get::<IPAddress>(&ip_address_key("2001:db8::11"))
            .await
            .is_ok());
    }
}
