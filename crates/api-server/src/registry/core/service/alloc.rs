//! `pkg/registry/core/service/storage/alloc.go`: how a Service create or
//! update claims and frees node ports through a [`PortAllocationOperation`]
//! and ClusterIPs through the [`MetaAllocator`].
//!
//! Only the primary family is allocated: this api-server configures one
//! (IPv4) ClusterIP allocator, and `initIPFamilyFields`' dual-stack handling
//! moves over with the Service strategy (#2077).
//!
//! Every entry point hands back the operation. The caller commits it once the
//! Service is persisted (`callbackTransaction.commit`, alloc.go:485-491) and
//! finishes it — which rolls back an uncommitted allocation — otherwise.

use std::sync::Arc;

use rusternetes_common::resources::{Service, ServiceExternalTrafficPolicy, ServiceType};
use rusternetes_common::validation::field;
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
// ClusterIPs
// ---------------------------------------------------------------------------

/// The ClusterIP half of a `callbackTransaction` (alloc.go:305-338,
/// 628-675): what to release if the write fails, and what to release once
/// it succeeds.
pub struct ClusterIpTxn<S: Storage> {
    pa: Arc<MetaAllocator<S>>,
    allocated: Vec<IpAddr>,
    release_on_commit: Vec<IpAddr>,
    dry_run: bool,
}

impl<S: Storage> ClusterIpTxn<S> {
    fn new(pa: &Arc<MetaAllocator<S>>, dry_run: bool) -> Self {
        Self {
            pa: pa.clone(),
            allocated: Vec::new(),
            release_on_commit: Vec::new(),
            dry_run,
        }
    }

    async fn release_all(&self, ips: &[IpAddr]) {
        for ip in ips {
            if let Err(e) = self.pa.release(*ip, false).await {
                tracing::error!("failed to release ClusterIP {ip}: {e}");
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

/// `allocClusterIPs` + `allocIPs` (alloc.go:340-451) for the primary
/// family: a named address is claimed, an empty one allocated.
async fn alloc_cluster_ips<S: Storage>(
    txn: &mut ClusterIpTxn<S>,
    service: &mut Service,
) -> Result<()> {
    // ExternalName and headless Services get no ClusterIPs.
    if matches!(service.spec.service_type, Some(ServiceType::ExternalName)) || is_headless(service)
    {
        return Ok(());
    }
    let requested = cluster_ips(service).into_iter().next().unwrap_or_default();
    let pa = txn.pa.clone();
    let ip = if requested.is_empty() {
        match pa.allocate_next_service(Some(service), txn.dry_run).await {
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
        let ip: IpAddr = requested
            .parse()
            .map_err(|_| internal_error(format!("failed to parse service IP {requested:?}")))?;
        pa.allocate_service(Some(service), ip, txn.dry_run)
            .await
            .map_err(|e| {
                invalid_cluster_ips(service, format!("failed to allocate IP {requested}: {e}"))
            })?;
        ip
    };
    txn.allocated.push(ip);
    service.spec.cluster_ip = Some(ip.to_string());
    match service.spec.cluster_ips.as_mut() {
        Some(ips) if !ips.is_empty() => ips[0] = ip.to_string(),
        _ => service.spec.cluster_ips = Some(vec![ip.to_string()]),
    }
    Ok(())
}

/// `txnAllocClusterIPs` (alloc.go:305-338).
pub async fn txn_alloc_cluster_ips<S: Storage>(
    pa: &Arc<MetaAllocator<S>>,
    service: &mut Service,
    dry_run: bool,
) -> Result<ClusterIpTxn<S>> {
    let mut txn = ClusterIpTxn::new(pa, dry_run);
    alloc_cluster_ips(&mut txn, service).await?;
    Ok(txn)
}

/// `txnUpdateClusterIPs` + `updateClusterIPs` (alloc.go:628-752), cases A
/// (from ExternalName: allocate) and B (to ExternalName: release on
/// commit). Cases C and D are dual-stack upgrades and downgrades.
pub async fn txn_update_cluster_ips<S: Storage>(
    pa: &Arc<MetaAllocator<S>>,
    service: &mut Service,
    old_service: &Service,
    dry_run: bool,
) -> Result<ClusterIpTxn<S>> {
    let mut txn = ClusterIpTxn::new(pa, dry_run);
    let was_external = matches!(
        old_service.spec.service_type,
        Some(ServiceType::ExternalName)
    );
    let is_external = matches!(service.spec.service_type, Some(ServiceType::ExternalName));
    // CASE A.
    if was_external && !is_external {
        alloc_cluster_ips(&mut txn, service).await?;
        return Ok(txn);
    }
    // Headless: no ClusterIP to manage.
    if is_headless(old_service) {
        return Ok(txn);
    }
    // CASE B.
    if !was_external && is_external {
        txn.release_on_commit = cluster_ips(old_service)
            .iter()
            .filter_map(|ip| ip.parse().ok())
            .collect();
    }
    Ok(txn)
}

/// The ClusterIP half of `releaseAllocatedResources` (`releaseClusterIPs`,
/// alloc.go:910-930), run once a Service is gone.
pub async fn release_cluster_ips<S: Storage>(pa: &MetaAllocator<S>, service: &Service) {
    if matches!(service.spec.service_type, Some(ServiceType::ExternalName)) || is_headless(service)
    {
        return;
    }
    for ip in cluster_ips(service) {
        let Ok(addr) = ip.parse::<IpAddr>() else {
            continue;
        };
        if let Err(e) = pa.release(addr, false).await {
            tracing::error!(
                "Error releasing service {} ClusterIP {ip}: {e}",
                service.metadata.name
            );
        }
    }
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
