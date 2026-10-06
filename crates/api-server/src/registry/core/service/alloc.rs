//! `pkg/registry/core/service/storage/alloc.go`: how a Service create or
//! update claims and frees node ports through a [`PortAllocationOperation`]
//! and ClusterIPs through the [`MetaAllocator`].
//!
//! One ClusterIP allocator per configured family, primary first
//! (`--service-cluster-ip-range`).
//!
//! Every entry point hands back the operation. The caller commits it once the
//! Service is persisted (`callbackTransaction.commit`, alloc.go:485-491) and
//! finishes it — which rolls back an uncommitted allocation — otherwise.

use std::sync::Arc;

use rusternetes_common::resources::{
    IPFamily, IPFamilyPolicy, Service, ServiceExternalTrafficPolicy, ServiceType,
};
use rusternetes_common::validation::field;
use rusternetes_common::validation::service::{
    parse_ip_sloppy, validate_service_cluster_ips_related_fields,
};
use rusternetes_common::{Error, Result};
use rusternetes_storage::Storage;
use std::net::IpAddr;

use super::ipallocator::cidr::MetaAllocator;
use super::ipallocator::IpError;
use super::portallocator::operation::PortAllocationOperation;
use super::portallocator::{PortAllocator, PortError};
use crate::registry::rest::internal_error;

fn is_node_port_type(t: Option<&ServiceType>) -> bool {
    matches!(
        t,
        Some(ServiceType::NodePort) | Some(ServiceType::LoadBalancer)
    )
}

/// `apiservice.NeedsHealthCheck` (pkg/api/service/util.go:88-93) over
/// `RequestsOnlyLocalTraffic`.
pub fn needs_health_check(service: &Service) -> bool {
    matches!(service.spec.service_type, Some(ServiceType::LoadBalancer))
        && matches!(
            service.spec.external_traffic_policy,
            Some(ServiceExternalTrafficPolicy::Local)
        )
}

/// `shouldAllocateNodePorts` (alloc.go:973-981). Upstream dereferences
/// `AllocateLoadBalancerNodePorts`, which defaulting sets to true for a
/// LoadBalancer (pkg/apis/core/v1/defaults.go); an unset value reads the same.
fn should_allocate_node_ports(service: &Service) -> bool {
    match service.spec.service_type {
        Some(ServiceType::NodePort) => true,
        Some(ServiceType::LoadBalancer) => service
            .spec
            .allocate_load_balancer_node_ports
            .unwrap_or(true),
        _ => false,
    }
}

fn node_port(p: Option<u16>) -> usize {
    p.map(usize::from).unwrap_or(0)
}

/// `findRequestedNodePort` (alloc.go:963-971).
fn find_requested_node_port(port: i32, service: &Service) -> usize {
    service
        .spec
        .ports
        .iter()
        .find(|p| p.port as i32 == port && node_port(p.node_port) != 0)
        .map(|p| node_port(p.node_port))
        .unwrap_or(0)
}

/// `collectServiceNodePorts` (alloc.go:983-992).
fn collect_service_node_ports(service: &Service) -> Vec<usize> {
    service
        .spec
        .ports
        .iter()
        .map(|p| node_port(p.node_port))
        .filter(|p| *p != 0)
        .collect()
}

/// The `field.Invalid(spec.ports[i].nodePort)` wrapped by
/// `apierrors.NewInvalid(api.Kind("Service"), ...)`.
fn invalid_node_port(i: usize, value: usize, err: &PortError) -> Error {
    let path = field::Path::new("spec")
        .child("ports")
        .index(i)
        .child("nodePort");
    Error::Invalid(vec![field::Error::invalid(
        &path,
        value as i64,
        err.to_string(),
    )])
}

fn set_node_port(service: &mut Service, i: usize, port: usize) {
    service.spec.ports[i].node_port = Some(port as u16);
}

/// `txnAllocNodePorts` (alloc.go:481-515).
pub async fn txn_alloc_node_ports(
    pa: &Arc<PortAllocator>,
    service: &mut Service,
    dry_run: bool,
) -> Result<PortAllocationOperation> {
    let mut op = PortAllocationOperation::start(pa.clone(), dry_run);

    if is_node_port_type(service.spec.service_type.as_ref()) {
        if let Err(e) = init_node_ports(service, &mut op).await {
            op.finish().await;
            return Err(e);
        }
    }

    if needs_health_check(service) {
        if let Err(e) = alloc_health_check_node_port(service, &mut op).await {
            op.finish().await;
            return Err(internal_error(e));
        }
    }

    Ok(op)
}

/// `initNodePorts` (alloc.go:517-568).
async fn init_node_ports(service: &mut Service, op: &mut PortAllocationOperation) -> Result<()> {
    let mut svc_port_to_node_port: std::collections::HashMap<i32, usize> =
        std::collections::HashMap::new();
    for i in 0..service.spec.ports.len() {
        let port = service.spec.ports[i].port as i32;
        let requested = node_port(service.spec.ports[i].node_port);
        if requested == 0 && !should_allocate_node_ports(service) {
            // Don't allocate new ports, but do respect specific requests.
            continue;
        }
        let allocated = svc_port_to_node_port.get(&port).copied().unwrap_or(0);
        if allocated == 0 {
            // Scans forward only: any earlier match is in svc_port_to_node_port.
            let np = find_requested_node_port(port, service);
            if np != 0 {
                op.allocate(np)
                    .await
                    .map_err(|e| invalid_node_port(i, np, &e))?;
                set_node_port(service, i, np);
                svc_port_to_node_port.insert(port, np);
            } else {
                let np = op
                    .allocate_next()
                    .await
                    .map_err(|e| internal_error(format!("failed to allocate a nodePort: {e}")))?;
                set_node_port(service, i, np);
                svc_port_to_node_port.insert(port, np);
            }
        } else if requested != allocated {
            if requested == 0 {
                set_node_port(service, i, allocated);
            } else {
                op.allocate(requested)
                    .await
                    .map_err(|e| invalid_node_port(i, requested, &e))?;
            }
        }
    }
    Ok(())
}

/// `allocHealthCheckNodePort` (alloc.go:570-589). The caller wraps the
/// error in an internal error.
async fn alloc_health_check_node_port(
    service: &mut Service,
    op: &mut PortAllocationOperation,
) -> std::result::Result<(), String> {
    let requested = service.spec.health_check_node_port.unwrap_or(0);
    if requested != 0 {
        op.allocate(requested as usize).await.map_err(|e| {
            format!("failed to allocate requested HealthCheck NodePort {requested}: {e}")
        })?;
    } else {
        let np = op
            .allocate_next()
            .await
            .map_err(|e| format!("failed to allocate a HealthCheck NodePort 0: {e}"))?;
        service.spec.health_check_node_port = Some(np as i32);
    }
    Ok(())
}

/// `txnUpdateNodePorts` (alloc.go:754-795).
pub async fn txn_update_node_ports(
    pa: &Arc<PortAllocator>,
    service: &mut Service,
    old_service: &Service,
    dry_run: bool,
) -> Result<PortAllocationOperation> {
    let mut op = PortAllocationOperation::start(pa.clone(), dry_run);

    // NodePort/LoadBalancer -> ExternalName/ClusterIP releases the node ports.
    if is_node_port_type(old_service.spec.service_type.as_ref())
        && matches!(
            service.spec.service_type,
            Some(ServiceType::ExternalName) | Some(ServiceType::ClusterIP)
        )
    {
        for np in collect_service_node_ports(old_service) {
            op.release_deferred(np);
        }
    }

    if is_node_port_type(service.spec.service_type.as_ref()) {
        if let Err(e) = update_node_ports(service, old_service, &mut op).await {
            op.finish().await;
            return Err(e);
        }
    }

    if let Err(e) = update_health_check_node_port(service, old_service, &mut op).await {
        op.finish().await;
        return Err(e);
    }

    Ok(op)
}

/// `updateNodePorts` (alloc.go:805-857).
async fn update_node_ports(
    service: &mut Service,
    old_service: &Service,
    op: &mut PortAllocationOperation,
) -> Result<()> {
    let old_numbers = collect_service_node_ports(old_service);
    // `ServiceNodePort{Protocol, NodePort}` (alloc.go:46-54).
    let mut new_node_ports: Vec<(String, usize)> = Vec::new();
    let mut port_allocated = std::collections::HashSet::new();

    for i in 0..service.spec.ports.len() {
        let mut np = node_port(service.spec.ports[i].node_port);
        if np == 0 && !should_allocate_node_ports(service) {
            // Don't allocate new ports, but do respect specific requests.
            continue;
        }
        if np != 0 {
            if !old_numbers.contains(&np) && !port_allocated.contains(&np) {
                op.allocate(np)
                    .await
                    .map_err(|e| invalid_node_port(i, np, &e))?;
                port_allocated.insert(np);
            }
        } else {
            np = op
                .allocate_next()
                .await
                .map_err(|e| internal_error(format!("failed to allocate a nodePort: {e}")))?;
            set_node_port(service, i, np);
        }
        let entry = (service.spec.ports[i].protocol.clone(), np);
        if new_node_ports.contains(&entry) {
            // A plain `fmt.Errorf`, which the apiserver serves as a 500.
            return Err(internal_error(format!(
                "duplicate nodePort: {{{} {}}}",
                entry.0, entry.1
            )));
        }
        new_node_ports.push(entry);
    }

    let new_numbers = collect_service_node_ports(service);
    for old in old_numbers {
        if !new_numbers.contains(&old) {
            op.release_deferred(old);
        }
    }
    Ok(())
}

/// `updateHealthCheckNodePort` (alloc.go:859-884).
async fn update_health_check_node_port(
    service: &mut Service,
    old_service: &Service,
    op: &mut PortAllocationOperation,
) -> Result<()> {
    let needed = needs_health_check(old_service);
    let needs = needs_health_check(service);
    if !needed && needs {
        alloc_health_check_node_port(service, op)
            .await
            .map_err(internal_error)?;
    } else if needed && !needs {
        op.release_deferred(old_service.spec.health_check_node_port.unwrap_or(0) as usize);
    }
    Ok(())
}

/// The NodePort half of `releaseAllocatedResources` (alloc.go:886-908), run
/// once a Service is gone. Errors are only logged: the repair loop reclaims a
/// port whose release was lost.
pub async fn release_node_ports(pa: &PortAllocator, service: &Service) {
    for np in collect_service_node_ports(service) {
        if let Err(e) = pa.release(np).await {
            tracing::error!(
                "Error releasing service {} node port {}: {}",
                service.metadata.name,
                np,
                e
            );
        }
    }
    if needs_health_check(service) {
        let np = service.spec.health_check_node_port.unwrap_or(0);
        if np > 0 {
            if let Err(e) = pa.release(np as usize).await {
                tracing::error!(
                    "Error releasing service {} health check node port {}: {}",
                    service.metadata.name,
                    np,
                    e
                );
            }
        }
    }
}

/// Settle `op` against the outcome of the storage write: commit on success,
/// roll back otherwise (`callbackTransaction`, alloc.go:485-495).
pub async fn settle<T>(mut op: PortAllocationOperation, result: &Result<T>) {
    if result.is_ok() {
        op.commit().await;
    }
    op.finish().await;
}

// ---------------------------------------------------------------------------
// IP families
// ---------------------------------------------------------------------------

fn other_family(fam: &IPFamily) -> IPFamily {
    match fam {
        IPFamily::IPv4 => IPFamily::IPv6,
        IPFamily::IPv6 => IPFamily::IPv4,
    }
}

/// `familyOf` (alloc.go:1099-1107); `None` is upstream's `"unknown"`.
fn family_of(ip: &str) -> Option<IPFamily> {
    match parse_ip_sloppy(ip)? {
        IpAddr::V4(_) => Some(IPFamily::IPv4),
        IpAddr::V6(_) => Some(IPFamily::IPv6),
    }
}

fn spec_cluster_ips(svc: &Service) -> &[String] {
    svc.spec.cluster_ips.as_deref().unwrap_or(&[])
}

fn spec_ip_families(svc: &Service) -> &[IPFamily] {
    svc.spec.ip_families.as_deref().unwrap_or(&[])
}

/// `sameClusterIPs` (alloc.go:1052-1064).
pub fn same_cluster_ips(lhs: &Service, rhs: &Service) -> bool {
    spec_cluster_ips(lhs) == spec_cluster_ips(rhs)
}

/// `sameIPFamilies` (alloc.go:1075-1087).
fn same_ip_families(lhs: &Service, rhs: &Service) -> bool {
    spec_ip_families(lhs) == spec_ip_families(rhs)
}

/// `reducedClusterIPs` (alloc.go:1066-1073).
fn reduced_cluster_ips(service: &Service, old: &Service) -> bool {
    let new = spec_cluster_ips(service);
    !new.is_empty() && new.len() < spec_cluster_ips(old).len()
}

/// `reducedIPFamilies` (alloc.go:1089-1097).
fn reduced_ip_families(service: &Service, old: &Service) -> bool {
    let new = spec_ip_families(service);
    !new.is_empty() && new.len() < spec_ip_families(old).len()
}

fn has_selector(svc: &Service) -> bool {
    svc.spec.selector.as_ref().is_some_and(|s| !s.is_empty())
}

fn is_policy(svc: &Service, policy: IPFamilyPolicy) -> bool {
    svc.spec.ip_family_policy.as_ref() == Some(&policy)
}

/// `isMatchingPreferDualStackClusterIPFields` (alloc.go:996-1042).
fn is_matching_prefer_dual_stack_cluster_ip_fields(
    service: &Service,
    old: Option<&Service>,
) -> bool {
    let Some(old) = old else {
        return false;
    };
    if service.spec.ip_family_policy.is_none() {
        return false;
    }
    if old.spec.service_type != service.spec.service_type {
        return false;
    }
    if !matches!(
        service.spec.service_type,
        Some(ServiceType::ClusterIP)
            | Some(ServiceType::NodePort)
            | Some(ServiceType::LoadBalancer)
    ) {
        return false;
    }
    if !is_policy(service, IPFamilyPolicy::PreferDualStack) {
        return false;
    }
    if old.spec.ip_family_policy.is_some() && !is_policy(old, IPFamilyPolicy::PreferDualStack) {
        return false;
    }
    same_cluster_ips(old, service) && same_ip_families(old, service)
}

fn policy_value(svc: &Service) -> serde_json::Value {
    serde_json::to_value(&svc.spec.ip_family_policy).unwrap_or_default()
}

fn invalid_service(errs: field::ErrorList) -> Error {
    Error::Invalid(errs)
}

/// `initIPFamilyFields` (alloc.go:104-303): default `ipFamilyPolicy` and
/// `ipFamilies`, and reject families and policies this cluster cannot
/// serve. `old` is `None` on create.
pub fn init_ip_family_fields(
    service: &mut Service,
    old: Option<&Service>,
    configured: &[IPFamily],
) -> Result<()> {
    let default_family = configured[0].clone();
    if matches!(service.spec.service_type, Some(ServiceType::ExternalName)) {
        return Ok(());
    }

    if is_matching_prefer_dual_stack_cluster_ip_fields(service, old) {
        return Ok(());
    }

    let headless_selectorless =
        service.spec.cluster_ip.as_deref() == Some("None") && !has_selector(service);

    if service.spec.ip_family_policy.is_none() {
        service.spec.ip_family_policy = match old.and_then(|o| o.spec.ip_family_policy.clone()) {
            Some(p) => Some(p),
            None if headless_selectorless => Some(IPFamilyPolicy::RequireDualStack),
            None => Some(IPFamilyPolicy::SingleStack),
        };
    }

    let el = validate_service_cluster_ips_related_fields(service, old);
    if !el.is_empty() {
        return Err(invalid_service(el));
    }

    let policy_path = field::Path::new("spec").child("ipFamilyPolicy");
    let mut el = Vec::new();

    if let Some(old) = old {
        if is_policy(service, IPFamilyPolicy::SingleStack) {
            if same_cluster_ips(old, service) && spec_cluster_ips(service).len() > 1 {
                if let Some(ips) = service.spec.cluster_ips.as_mut() {
                    ips.truncate(1);
                }
            }
            if same_ip_families(old, service) && spec_ip_families(service).len() > 1 {
                if let Some(fams) = service.spec.ip_families.as_mut() {
                    fams.truncate(1);
                }
            }
        } else {
            if reduced_cluster_ips(service, old) {
                el.push(field::Error::invalid(
                    &policy_path,
                    policy_value(service),
                    "must be 'SingleStack' to release the secondary cluster IP",
                ));
            }
            if reduced_ip_families(service, old) {
                el.push(field::Error::invalid(
                    &policy_path,
                    policy_value(service),
                    "must be 'SingleStack' to release the secondary IP family",
                ));
            }
        }
    }

    if is_policy(service, IPFamilyPolicy::SingleStack) {
        if spec_cluster_ips(service).len() == 2 {
            el.push(field::Error::invalid(
                &policy_path,
                policy_value(service),
                "must be 'RequireDualStack' or 'PreferDualStack' when multiple cluster IPs are specified",
            ));
        }
        if spec_ip_families(service).len() == 2 {
            el.push(field::Error::invalid(
                &policy_path,
                policy_value(service),
                "must be 'RequireDualStack' or 'PreferDualStack' when multiple IP families are specified",
            ));
        }
    }

    // Infer ipFamilies[] from clusterIPs[].
    let ips = spec_cluster_ips(service).to_vec();
    for (i, ip) in ips.iter().enumerate() {
        if ip == "None" {
            break;
        }
        if i >= spec_ip_families(service).len() {
            match family_of(ip) {
                Some(fam) if configured.contains(&fam) => {
                    service
                        .spec
                        .ip_families
                        .get_or_insert_with(Vec::new)
                        .push(fam);
                }
                fam => {
                    let name = match fam {
                        Some(IPFamily::IPv4) => "IPv4",
                        Some(IPFamily::IPv6) => "IPv6",
                        None => "unknown",
                    };
                    el.push(field::Error::invalid(
                        &field::Path::new("spec").child("clusterIPs").index(i),
                        ips.clone(),
                        format!("{name} is not configured on this cluster"),
                    ));
                }
            }
        }
    }

    if !el.is_empty() {
        return Err(invalid_service(el));
    }

    // Headless + selectorless may carry families the cluster does not have.
    if headless_selectorless {
        let fams = service.spec.ip_families.get_or_insert_with(Vec::new);
        if fams.is_empty() {
            fams.push(default_family.clone());
        }
        if fams.len() < 2
            && service.spec.ip_family_policy.as_ref() != Some(&IPFamilyPolicy::SingleStack)
        {
            let alt = other_family(&fams[0]);
            fams.push(alt);
        }
        return Ok(());
    }

    if is_policy(service, IPFamilyPolicy::RequireDualStack) && configured.len() < 2 {
        el.push(field::Error::invalid(
            &policy_path,
            policy_value(service),
            "this cluster is not configured for dual-stack services",
        ));
    }
    for (i, fam) in spec_ip_families(service).iter().enumerate() {
        if !configured.contains(fam) {
            el.push(field::Error::invalid(
                &field::Path::new("spec").child("ipFamilies").index(i),
                serde_json::to_value(fam).unwrap_or_default(),
                "not configured on this cluster",
            ));
        }
    }
    if !el.is_empty() {
        return Err(invalid_service(el));
    }

    let fams = service.spec.ip_families.get_or_insert_with(Vec::new);
    if fams.is_empty() {
        fams.push(default_family.clone());
    }
    if service.spec.ip_family_policy.as_ref() != Some(&IPFamilyPolicy::SingleStack)
        && fams.len() == 1
        && configured.len() == 2
    {
        let alt = other_family(&fams[0]);
        fams.push(alt);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ClusterIPs
// ---------------------------------------------------------------------------

/// `Allocators.serviceIPAllocatorsByFamily`
/// (pkg/registry/core/rest/storage_core.go:329-490): one [`MetaAllocator`]
/// per configured family, primary first.
pub struct ClusterIpAllocators<S: Storage> {
    by_family: Vec<(IPFamily, Arc<MetaAllocator<S>>)>,
}

impl<S: Storage> ClusterIpAllocators<S> {
    /// One allocator per family of `families` (`--service-cluster-ip-range`,
    /// primary first).
    pub fn new(storage: Arc<S>, families: &[IPFamily]) -> Self {
        Self {
            by_family: families
                .iter()
                .map(|f| {
                    (
                        f.clone(),
                        Arc::new(MetaAllocator::new(storage.clone(), *f == IPFamily::IPv6)),
                    )
                })
                .collect(),
        }
    }

    /// The primary family, `Allocators.defaultServiceIPFamily`
    /// (alloc.go:41-48), i.e. `REST.primaryIPFamily` (storage.go:115-124).
    pub fn primary_family(&self) -> IPFamily {
        self.by_family[0].0.clone()
    }

    /// Install the loopback client every family's allocators write
    /// IPAddresses through.
    pub fn set_loopback(&self, client: Arc<dyn super::ipallocator::IpAddressClient>) {
        for (_, a) in &self.by_family {
            a.set_loopback(client.clone());
        }
    }

    /// The configured families, primary first.
    pub fn families(&self) -> Vec<IPFamily> {
        self.by_family.iter().map(|(f, _)| f.clone()).collect()
    }

    fn get(&self, family: &IPFamily) -> Option<&Arc<MetaAllocator<S>>> {
        self.by_family
            .iter()
            .find(|(f, _)| f == family)
            .map(|(_, a)| a)
    }
}

/// The ClusterIP half of a `callbackTransaction` (alloc.go:305-338,
/// 628-675): what to release if the write fails, and what to release once
/// it succeeds.
pub struct ClusterIpTxn<S: Storage> {
    pa: Arc<ClusterIpAllocators<S>>,
    allocated: Vec<(IPFamily, IpAddr)>,
    release_on_commit: Vec<(IPFamily, IpAddr)>,
    dry_run: bool,
}

impl<S: Storage> ClusterIpTxn<S> {
    fn new(pa: &Arc<ClusterIpAllocators<S>>, dry_run: bool) -> Self {
        Self {
            pa: pa.clone(),
            allocated: Vec::new(),
            release_on_commit: Vec::new(),
            dry_run,
        }
    }

    /// `releaseIPs` (alloc.go:446-475): a family this cluster does not serve
    /// is skipped, and the first failed release ends the pass.
    async fn release_all(&self, ips: &[(IPFamily, IpAddr)]) {
        for (family, ip) in ips {
            let Some(allocator) = self.pa.get(family) else {
                tracing::info!(
                    "Not releasing ClusterIP {ip} because related family {family:?} is not enabled"
                );
                continue;
            };
            if let Err(e) = allocator.release(*ip, false).await {
                tracing::error!("failed to release ClusterIP {ip}: {e}");
                return;
            }
        }
    }

    /// `commit`: release what the update gave up.
    pub async fn commit(self) {
        if !self.dry_run {
            self.release_all(&self.release_on_commit).await;
        }
    }

    /// `revert`: release what this request allocated.
    pub async fn revert(self) {
        if !self.dry_run {
            self.release_all(&self.allocated).await;
        }
    }
}

fn is_headless(service: &Service) -> bool {
    service.spec.cluster_ip.as_deref() == Some("None")
        || service
            .spec
            .cluster_ips
            .as_ref()
            .and_then(|ips| ips.first())
            .is_some_and(|ip| ip == "None")
}

/// `spec.clusterIPs`, falling back to `spec.clusterIP` for an object that
/// predates keeping the two in step.
fn cluster_ips(service: &Service) -> Vec<String> {
    match &service.spec.cluster_ips {
        Some(ips) if !ips.is_empty() => ips.clone(),
        _ => service
            .spec
            .cluster_ip
            .clone()
            .into_iter()
            .filter(|ip| !ip.is_empty())
            .collect(),
    }
}

fn invalid_cluster_ips(service: &Service, detail: String) -> Error {
    let path = field::Path::new("spec").child("clusterIPs");
    Error::Invalid(vec![field::Error::invalid(
        &path,
        cluster_ips(service),
        detail,
    )])
}

/// `allocIPs` (alloc.go:395-451): claim a named address, or allocate the
/// next one, for each `(family, ip)`. Each success is recorded in
/// `txn.allocated` at once, so a failure part-way leaves the caller able to
/// roll back what was taken.
async fn alloc_ips<S: Storage>(
    txn: &mut ClusterIpTxn<S>,
    service: &Service,
    to_alloc: Vec<(IPFamily, String)>,
) -> Result<Vec<(IPFamily, IpAddr)>> {
    let pa = txn.pa.clone();
    let mut allocated = Vec::new();
    for (family, requested) in to_alloc {
        // Always there: the families are validated against the
        // configuration first.
        let Some(allocator) = pa.get(&family) else {
            return Err(internal_error(format!(
                "no ClusterIP allocator for family {family:?}"
            )));
        };
        let ip = if requested.is_empty() {
            match allocator
                .allocate_next_service(Some(service), txn.dry_run)
                .await
            {
                Ok(ip) => ip,
                Err(IpError::Full) => {
                    return Err(internal_error(format!(
                        "failed to allocate a serviceIP: {}",
                        IpError::Full
                    )))
                }
                Err(e) => {
                    return Err(invalid_cluster_ips(
                        service,
                        format!("failed to allocate IP: {e}"),
                    ))
                }
            }
        } else {
            let ip: IpAddr = parse_ip_sloppy(&requested).ok_or_else(|| {
                internal_error(format!("failed to parse service IP {requested:?}"))
            })?;
            allocator
                .allocate_service(Some(service), ip, txn.dry_run)
                .await
                .map_err(|e| {
                    invalid_cluster_ips(service, format!("failed to allocate IP {requested}: {e}"))
                })?;
            ip
        };
        txn.allocated.push((family.clone(), ip));
        allocated.push((family, ip));
    }
    Ok(allocated)
}

/// `allocClusterIPs` (alloc.go:340-393): one address per entry of
/// `spec.ipFamilies`; a named address is claimed, an empty one allocated.
async fn alloc_cluster_ips<S: Storage>(
    txn: &mut ClusterIpTxn<S>,
    service: &mut Service,
) -> Result<()> {
    // ExternalName and headless Services get no ClusterIPs.
    if matches!(service.spec.service_type, Some(ServiceType::ExternalName)) || is_headless(service)
    {
        return Ok(());
    }
    // The Service has correct ipFamilies; it may carry only some of its
    // clusterIPs (an upgrade to dual-stack), or none.
    let mut families = spec_ip_families(service).to_vec();
    if families.is_empty() {
        families.push(txn.pa.families()[0].clone());
    }
    let mut ips = cluster_ips(service);
    while ips.len() < families.len() {
        ips.push(String::new()); // the marker
    }
    let to_alloc: Vec<(IPFamily, String)> =
        families.iter().cloned().zip(ips.iter().cloned()).collect();
    let allocated = alloc_ips(txn, service, to_alloc).await?;
    for (family, ip) in allocated {
        if let Some(i) = families.iter().position(|f| *f == family) {
            ips[i] = ip.to_string();
        }
    }
    service.spec.cluster_ip = Some(ips[0].clone());
    service.spec.cluster_ips = Some(ips);
    Ok(())
}

/// `txnAllocClusterIPs` (alloc.go:305-338). Unlike upstream, an allocation
/// that fails after the first family was taken gives that one back rather
/// than leaving it to the repair loop.
pub async fn txn_alloc_cluster_ips<S: Storage>(
    pa: &Arc<ClusterIpAllocators<S>>,
    service: &mut Service,
    dry_run: bool,
) -> Result<ClusterIpTxn<S>> {
    let mut txn = ClusterIpTxn::new(pa, dry_run);
    if let Err(e) = alloc_cluster_ips(&mut txn, service).await {
        txn.revert().await;
        return Err(e);
    }
    Ok(txn)
}

/// The family of each entry of `old`'s `spec.clusterIPs`
/// (`oldService.Spec.IPFamilies[i]`).
fn old_family_ips(old: &Service) -> Vec<(IPFamily, String)> {
    let fams = spec_ip_families(old);
    cluster_ips(old)
        .into_iter()
        .enumerate()
        .filter_map(|(i, ip)| {
            fams.get(i)
                .cloned()
                .or_else(|| family_of(&ip))
                .map(|f| (f, ip))
        })
        .collect()
}

/// `txnUpdateClusterIPs` + `updateClusterIPs` (alloc.go:628-752): A (from
/// ExternalName: allocate), B (to ExternalName: release on commit), C
/// (upgrade to dual-stack: allocate the secondary) and D (downgrade:
/// release the secondary on commit).
pub async fn txn_update_cluster_ips<S: Storage>(
    pa: &Arc<ClusterIpAllocators<S>>,
    service: &mut Service,
    old_service: &Service,
    dry_run: bool,
) -> Result<ClusterIpTxn<S>> {
    let mut txn = ClusterIpTxn::new(pa, dry_run);
    if let Err(e) = update_cluster_ips(&mut txn, service, old_service).await {
        txn.revert().await;
        return Err(e);
    }
    Ok(txn)
}

async fn update_cluster_ips<S: Storage>(
    txn: &mut ClusterIpTxn<S>,
    service: &mut Service,
    old_service: &Service,
) -> Result<()> {
    // A PreferDualStack Service is not auto-upgraded or downgraded when the
    // cluster gains or loses dual-stackness (alloc.go:660-668).
    if is_matching_prefer_dual_stack_cluster_ip_fields(service, Some(old_service)) {
        return Ok(());
    }
    let was_external = matches!(
        old_service.spec.service_type,
        Some(ServiceType::ExternalName)
    );
    let is_external = matches!(service.spec.service_type, Some(ServiceType::ExternalName));
    // CASE A.
    if was_external && !is_external {
        return alloc_cluster_ips(txn, service).await;
    }
    // Headless: no ClusterIP to manage.
    if is_headless(old_service) {
        return Ok(());
    }
    // CASE B.
    if !was_external && is_external {
        txn.release_on_commit = old_family_ips(old_service)
            .into_iter()
            .filter_map(|(f, ip)| parse_ip_sloppy(&ip).map(|ip| (f, ip)))
            .collect();
        return Ok(());
    }
    let old_len = spec_ip_families(old_service).len();
    let new_len = spec_ip_families(service).len();
    // CASE C.
    if old_len == 1 && new_len == 2 {
        let mut ips = cluster_ips(service);
        // If the secondary was named, take it; if not add a marker.
        if ips.len() < 2 {
            ips.push(String::new());
        }
        let family = spec_ip_families(service)[1].clone();
        let allocated = alloc_ips(txn, service, vec![(family, ips[1].clone())]).await?;
        if let Some((_, ip)) = allocated.first() {
            ips[1] = ip.to_string();
        }
        service.spec.cluster_ips = Some(ips);
        return Ok(());
    }
    // CASE D: the clusterIP itself is left to the action.
    if old_len == 2 && new_len == 1 {
        if let Some((f, ip)) = old_family_ips(old_service).get(1) {
            if let Some(ip) = parse_ip_sloppy(ip) {
                txn.release_on_commit = vec![(f.clone(), ip)];
            }
        }
    }
    Ok(())
}

/// The ClusterIP half of `releaseAllocatedResources` (`releaseClusterIPs`,
/// alloc.go:910-930), run once a Service is gone.
pub async fn release_cluster_ips<S: Storage>(pa: &Arc<ClusterIpAllocators<S>>, service: &Service) {
    if matches!(service.spec.service_type, Some(ServiceType::ExternalName)) || is_headless(service)
    {
        return;
    }
    let mut txn = ClusterIpTxn::new(pa, false);
    for ip in cluster_ips(service) {
        let Some(addr) = parse_ip_sloppy(&ip) else {
            continue;
        };
        // `netutils.IsIPv6String`.
        let family = if addr.is_ipv6() {
            IPFamily::IPv6
        } else {
            IPFamily::IPv4
        };
        txn.allocated.push((family, addr));
    }
    txn.revert().await;
}

/// Settle both halves against the storage write (`metaTransaction`,
/// alloc.go:65-100): commit on success, revert otherwise.
pub async fn settle_all<S: Storage, T>(
    ips: ClusterIpTxn<S>,
    ports: PortAllocationOperation,
    result: &Result<T>,
) {
    if result.is_ok() {
        ips.commit().await;
    } else {
        ips.revert().await;
    }
    settle(ports, result).await;
}

#[cfg(test)]
mod tests {
    use super::super::portallocator::PortRange;
    use super::*;
    use serde_json::json;

    fn pa() -> Arc<PortAllocator> {
        Arc::new(PortAllocator::new_in_memory(
            PortRange::parse("30000-32767").unwrap(),
        ))
    }

    fn svc(spec: serde_json::Value) -> Service {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "foo", "namespace": "default"},
            "spec": spec,
        }))
        .unwrap()
    }

    fn ports(s: &Service) -> Vec<usize> {
        s.spec
            .ports
            .iter()
            .map(|p| node_port(p.node_port))
            .collect()
    }

    async fn create(pa: &Arc<PortAllocator>, s: &mut Service) -> Result<()> {
        let op = txn_alloc_node_ports(pa, s, false).await?;
        settle(op, &Ok(())).await;
        Ok(())
    }

    /// The allocation-side cases of `TestCreateInitNodePorts`
    /// (storage_test.go:6176). The remaining cases fail validation first.
    #[tokio::test]
    async fn create_init_node_ports() {
        struct Case {
            name: &'static str,
            spec: serde_json::Value,
            expect_error: bool,
            expect_node_ports: bool,
        }
        let tcp = |name: &str, port: u16| json!({"name": name, "port": port, "protocol": "TCP"});
        let udp = |name: &str, port: u16| json!({"name": name, "port": port, "protocol": "UDP"});
        let cases = [
            Case {
                name: "type:ClusterIP",
                spec: json!({"type": "ClusterIP", "ports": [tcp("p", 80)]}),
                expect_error: false,
                expect_node_ports: false,
            },
            Case {
                name: "type:NodePort_single_port_unspecified",
                spec: json!({"type": "NodePort", "ports": [tcp("p", 80)]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_single_port_specified",
                spec: json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30001}]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_multiport_unspecified",
                spec: json!({"type": "NodePort", "ports": [tcp("p", 80), tcp("q", 443)]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_multiport_multiproto_unspecified",
                spec: json!({"type": "NodePort", "ports": [tcp("p", 53), udp("q", 53)]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_multiport_multiproto_same",
                spec: json!({"type": "NodePort", "ports": [
                    {"name": "p", "port": 53, "protocol": "TCP", "nodePort": 30053},
                    {"name": "q", "port": 53, "protocol": "UDP", "nodePort": 30053}]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_multiport_multiproto_conflict",
                spec: json!({"type": "NodePort", "ports": [
                    {"name": "p", "port": 93, "protocol": "TCP", "nodePort": 30093},
                    {"name": "q", "port": 76, "protocol": "UDP", "nodePort": 30093}]}),
                expect_error: true,
                expect_node_ports: false,
            },
            Case {
                name: "type:LoadBalancer_single_port_unspecified:on_alloc:false",
                spec: json!({"type": "LoadBalancer", "allocateLoadBalancerNodePorts": false, "ports": [tcp("p", 80)]}),
                expect_error: false,
                expect_node_ports: false,
            },
            Case {
                name: "type:LoadBalancer_single_port_unspecified:on_alloc:true",
                spec: json!({"type": "LoadBalancer", "allocateLoadBalancerNodePorts": true, "ports": [tcp("p", 80)]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:LoadBalancer_single_port_specified:on_alloc:false",
                spec: json!({"type": "LoadBalancer", "allocateLoadBalancerNodePorts": false,
                    "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30002}]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:LoadBalancer_multiport_multiproto_same",
                spec: json!({"type": "LoadBalancer", "ports": [
                    {"name": "p", "port": 53, "protocol": "TCP", "nodePort": 30054},
                    {"name": "q", "port": 53, "protocol": "UDP", "nodePort": 30054}]}),
                expect_error: false,
                expect_node_ports: true,
            },
        ];

        for tc in cases {
            let pa = pa();
            let requested = svc(tc.spec.clone());
            let mut s = requested.clone();
            let res = create(&pa, &mut s).await;
            if tc.expect_error {
                assert!(res.is_err(), "{}: expected an error", tc.name);
                assert!(pa.allocated().await.is_empty(), "{}: rolled back", tc.name);
                continue;
            }
            res.unwrap_or_else(|e| panic!("{}: {e}", tc.name));
            let got = ports(&s);
            let allocated: Vec<_> = got.iter().filter(|p| **p != 0).collect();
            if !tc.expect_node_ports {
                assert!(allocated.is_empty(), "{}: got {got:?}", tc.name);
                continue;
            }
            assert_eq!(allocated.len(), got.len(), "{}: {got:?}", tc.name);
            for p in &allocated {
                assert!(pa.has(**p).await, "{}: {p} not allocated", tc.name);
            }
            for (i, want) in ports(&requested).into_iter().enumerate() {
                if want != 0 {
                    assert_eq!(got[i], want, "{}: Ports[{i}]", tc.name);
                }
            }
            // Unique, except the same Port shared across protocols.
            let mut seen = std::collections::HashMap::new();
            for (i, p) in s.spec.ports.iter().enumerate() {
                if let Some(prev) = seen.insert(got[i], p.port) {
                    assert_eq!(prev, p.port, "{}: non-unique Ports[{i}]", tc.name);
                }
            }
        }
    }

    #[tokio::test]
    async fn a_node_port_in_use_is_rejected() {
        let pa = pa();
        let spec = json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30100}]});
        create(&pa, &mut svc(spec.clone())).await.unwrap();
        let err = create(&pa, &mut svc(spec)).await.unwrap_err();
        let Error::Invalid(errs) = err else {
            panic!("want Invalid, got {err:?}")
        };
        assert_eq!(errs[0].field, "spec.ports[0].nodePort");
        assert!(
            errs[0]
                .detail
                .contains("provided port is already allocated"),
            "{errs:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_write_rolls_the_allocation_back() {
        let pa = pa();
        let mut s = svc(json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP"}]}));
        let op = txn_alloc_node_ports(&pa, &mut s, false).await.unwrap();
        assert_eq!(pa.allocated().await.len(), 1);
        settle(op, &Err::<(), _>(Error::Internal("boom".into()))).await;
        assert!(pa.allocated().await.is_empty());
    }

    #[tokio::test]
    async fn a_dry_run_claims_nothing() {
        let pa = pa();
        let mut s = svc(json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP"}]}));
        let op = txn_alloc_node_ports(&pa, &mut s, true).await.unwrap();
        settle(op, &Ok(())).await;
        assert_ne!(ports(&s)[0], 0);
        assert!(pa.allocated().await.is_empty());
    }

    #[tokio::test]
    async fn a_local_load_balancer_gets_a_health_check_node_port() {
        let pa = pa();
        let mut s = svc(
            json!({"type": "LoadBalancer", "externalTrafficPolicy": "Local",
            "ports": [{"port": 80, "protocol": "TCP"}]}),
        );
        create(&pa, &mut s).await.unwrap();
        let hc = s.spec.health_check_node_port.unwrap() as usize;
        assert!(pa.has(hc).await);
        release_node_ports(&pa, &s).await;
        assert!(pa.allocated().await.is_empty());
    }

    /// `txnUpdateNodePorts` (alloc.go:754-795).
    #[tokio::test]
    async fn update_moves_and_frees_node_ports() {
        let pa = pa();
        let mut old = svc(
            json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30200}]}),
        );
        create(&pa, &mut old).await.unwrap();

        // Changing the requested port claims the new one and frees the old
        // one — on commit only.
        let mut new = svc(
            json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30201}]}),
        );
        let op = txn_update_node_ports(&pa, &mut new, &old, false)
            .await
            .unwrap();
        assert!(pa.has(30200).await, "the release is deferred to commit");
        settle(op, &Ok(())).await;
        assert_eq!(pa.allocated().await, vec![30201]);

        // Keeping a port does not re-allocate it.
        let mut same = new.clone();
        let op = txn_update_node_ports(&pa, &mut same, &new, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        assert_eq!(pa.allocated().await, vec![30201]);

        // NodePort -> ClusterIP frees it.
        let mut cip = svc(json!({"type": "ClusterIP", "ports": [{"port": 80, "protocol": "TCP"}]}));
        let op = txn_update_node_ports(&pa, &mut cip, &same, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        assert!(pa.allocated().await.is_empty());

        // ClusterIP -> NodePort allocates.
        let mut np = svc(json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP"}]}));
        let op = txn_update_node_ports(&pa, &mut np, &cip, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        assert_eq!(pa.allocated().await, ports(&np));
    }

    #[tokio::test]
    async fn update_to_a_port_in_use_is_rejected_and_keeps_the_old_one() {
        let pa = pa();
        create(
            &pa,
            &mut svc(json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30300}]})),
        )
        .await
        .unwrap();
        let mut old = svc(
            json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30301}]}),
        );
        create(&pa, &mut old).await.unwrap();

        let mut new = svc(
            json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30300}]}),
        );
        let err = txn_update_node_ports(&pa, &mut new, &old, false)
            .await
            .err()
            .expect("the port is taken");
        assert!(matches!(err, Error::Invalid(_)), "{err:?}");
        assert_eq!(pa.allocated().await, vec![30300, 30301]);
    }

    #[tokio::test]
    async fn update_to_local_traffic_policy_allocates_and_back_frees() {
        let pa = pa();
        let mut old = svc(
            json!({"type": "LoadBalancer", "externalTrafficPolicy": "Cluster",
            "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30400}]}),
        );
        create(&pa, &mut old).await.unwrap();

        let mut local = old.clone();
        local.spec.external_traffic_policy = Some(ServiceExternalTrafficPolicy::Local);
        let op = txn_update_node_ports(&pa, &mut local, &old, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        let hc = local.spec.health_check_node_port.expect("allocated") as usize;
        assert!(pa.has(hc).await);

        let mut cluster = local.clone();
        cluster.spec.external_traffic_policy = Some(ServiceExternalTrafficPolicy::Cluster);
        let op = txn_update_node_ports(&pa, &mut cluster, &local, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        assert!(!pa.has(hc).await);
    }
}
