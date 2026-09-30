//! Service strategies — port of `pkg/registry/core/service/strategy.go` and
//! `GetWarningsForService` (`pkg/api/service/warnings.go`).

use rusternetes_common::resources::{LoadBalancerStatus, Service, ServiceStatus, ServiceType};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::ipaddress::get_warnings_for_cidr;
use rusternetes_common::validation::metav1::get_warnings_for_ip;
use rusternetes_common::validation::service::{
    externally_accessible, validate_service_create, validate_service_status_update,
    validate_service_update,
};

use crate::registry::rest::{
    NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `core.DeprecatedAnnotationTopologyAwareHints` /
/// `core.AnnotationTopologyMode` (pkg/apis/core/annotation_key_constants.go).
const DEPRECATED_ANNOTATION_TOPOLOGY_AWARE_HINTS: &str =
    "service.kubernetes.io/topology-aware-hints";
const ANNOTATION_TOPOLOGY_MODE: &str = "service.kubernetes.io/topology-mode";

/// The status a create starts from: `api.ServiceStatus{}`, which serializes
/// with an empty `loadBalancer`.
fn empty_status() -> Option<ServiceStatus> {
    Some(ServiceStatus {
        load_balancer: Some(LoadBalancerStatus { ingress: vec![] }),
        conditions: None,
    })
}

fn is_type(svc: &Service, t: ServiceType) -> bool {
    svc.spec.service_type.as_ref() == Some(&t)
}

/// `needsClusterIP` (strategy.go).
fn needs_cluster_ip(svc: &Service) -> bool {
    !is_type(svc, ServiceType::ExternalName)
}

/// `needsNodePort` (strategy.go): unlike the storage helper of the same
/// name, `allocateLoadBalancerNodePorts` does not matter here.
fn needs_node_port(svc: &Service) -> bool {
    is_type(svc, ServiceType::NodePort) || is_type(svc, ServiceType::LoadBalancer)
}

/// `needsHCNodePort` (strategy.go).
fn needs_hc_node_port(svc: &Service) -> bool {
    is_type(svc, ServiceType::LoadBalancer)
        && matches!(
            svc.spec.external_traffic_policy,
            Some(rusternetes_common::resources::ServiceExternalTrafficPolicy::Local)
        )
}

/// `sameClusterIPs` (strategy.go): the singular and the plural field.
fn same_cluster_ips(old: &Service, new: &Service) -> bool {
    old.spec.cluster_ip.as_deref().unwrap_or("") == new.spec.cluster_ip.as_deref().unwrap_or("")
        && old.spec.cluster_ips.as_deref().unwrap_or(&[])
            == new.spec.cluster_ips.as_deref().unwrap_or(&[])
}

/// `sameNodePorts` (strategy.go): the old node ports are a superset of the
/// new ones.
fn same_node_ports(old: &Service, new: &Service) -> bool {
    let all = |svc: &Service| -> Vec<u16> {
        svc.spec
            .ports
            .iter()
            .filter_map(|p| p.node_port.filter(|n| *n != 0))
            .collect()
    };
    let old_ports = all(old);
    all(new).iter().all(|p| old_ports.contains(p))
}

/// `dropTypeDependentFields` (strategy.go): when a type change leaves a
/// field meaningless and the client did not change it, clear it for them.
pub fn drop_type_dependent_fields(new: &mut Service, old: &Service) {
    if needs_cluster_ip(old) && !needs_cluster_ip(new) {
        if same_cluster_ips(old, new) {
            new.spec.cluster_ip = None;
            new.spec.cluster_ips = None;
        }
        if old.spec.ip_families == new.spec.ip_families {
            new.spec.ip_families = None;
        }
        if old.spec.ip_family_policy == new.spec.ip_family_policy {
            new.spec.ip_family_policy = None;
        }
    }

    if needs_node_port(old) && !needs_node_port(new) && same_node_ports(old, new) {
        for p in &mut new.spec.ports {
            p.node_port = None;
        }
    }

    if needs_hc_node_port(old)
        && !needs_hc_node_port(new)
        && old.spec.health_check_node_port.unwrap_or(0)
            == new.spec.health_check_node_port.unwrap_or(0)
    {
        new.spec.health_check_node_port = None;
    }

    if is_type(old, ServiceType::LoadBalancer) && !is_type(new, ServiceType::LoadBalancer) {
        if let (Some(o), Some(n)) = (
            old.spec.allocate_load_balancer_node_ports,
            new.spec.allocate_load_balancer_node_ports,
        ) {
            if o == n {
                new.spec.allocate_load_balancer_node_ports = None;
            }
        }
    }

    // `canSetLoadBalancerClass` is `type == LoadBalancer`.
    if is_type(old, ServiceType::LoadBalancer)
        && !is_type(new, ServiceType::LoadBalancer)
        && old.spec.load_balancer_class == new.spec.load_balancer_class
    {
        new.spec.load_balancer_class = None;
    }

    if externally_accessible(old)
        && !externally_accessible(new)
        && old.spec.external_traffic_policy == new.spec.external_traffic_policy
    {
        new.spec.external_traffic_policy = None;
    }

    if !is_type(new, ServiceType::LoadBalancer) {
        let status = new.status.get_or_insert(ServiceStatus {
            load_balancer: None,
            conditions: None,
        });
        status.load_balancer = Some(LoadBalancerStatus { ingress: vec![] });
    }
}

/// `isHeadlessService` (pkg/api/service/warnings.go).
fn is_headless_service(svc: &Service) -> bool {
    is_type(svc, ServiceType::ClusterIP) && svc.spec.cluster_ip.as_deref() == Some("None")
}

/// `GetWarningsForService` (pkg/api/service/warnings.go:29-89).
pub fn get_warnings_for_service(svc: &Service) -> Vec<String> {
    let mut warnings = Vec::new();
    if svc
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(DEPRECATED_ANNOTATION_TOPOLOGY_AWARE_HINTS))
    {
        warnings.push(format!(
            "annotation {DEPRECATED_ANNOTATION_TOPOLOGY_AWARE_HINTS} is deprecated, please use {ANNOTATION_TOPOLOGY_MODE} instead"
        ));
    }

    // `helper.IsServiceIPSet`.
    let ip_set = !matches!(
        svc.spec.cluster_ip.as_deref(),
        None | Some("") | Some("None")
    );
    if ip_set {
        for (i, ip) in svc.spec.cluster_ips.iter().flatten().enumerate() {
            warnings.extend(get_warnings_for_ip(&format!("spec.clusterIPs[{i}]"), ip));
        }
    }

    if is_headless_service(svc) {
        if svc
            .spec
            .load_balancer_ip
            .as_deref()
            .is_some_and(|ip| !ip.is_empty())
        {
            warnings.push("spec.loadBalancerIP is ignored for headless services".to_string());
        }
        if svc
            .spec
            .external_ips
            .as_ref()
            .is_some_and(|ips| !ips.is_empty())
        {
            warnings.push("spec.externalIPs is ignored for headless services".to_string());
        }
        if svc.spec.session_affinity.as_deref() != Some("None") {
            warnings.push("spec.SessionAffinity is ignored for headless services".to_string());
        }
    }

    for (i, ip) in svc.spec.external_ips.iter().flatten().enumerate() {
        warnings.extend(get_warnings_for_ip(&format!("spec.externalIPs[{i}]"), ip));
    }
    if let Some(ip) = svc
        .spec
        .load_balancer_ip
        .as_deref()
        .filter(|ip| !ip.is_empty())
    {
        warnings.extend(get_warnings_for_ip("spec.loadBalancerIP", ip));
    }
    let ranges = Path::new("spec").child("loadBalancerSourceRanges");
    for (i, cidr) in svc
        .spec
        .load_balancer_source_ranges
        .iter()
        .flatten()
        .enumerate()
    {
        warnings.extend(get_warnings_for_cidr(&ranges.index(i), cidr));
    }

    if is_type(svc, ServiceType::ExternalName)
        && svc
            .spec
            .external_ips
            .as_ref()
            .is_some_and(|ips| !ips.is_empty())
    {
        warnings.push("spec.externalIPs is ignored when spec.type is \"ExternalName\"".to_string());
    }
    if !is_type(svc, ServiceType::ExternalName)
        && svc
            .spec
            .external_name
            .as_deref()
            .is_some_and(|n| !n.is_empty())
    {
        warnings.push(
            "spec.externalName is ignored when spec.type is not \"ExternalName\"".to_string(),
        );
    }
    if svc.spec.traffic_distribution.as_deref() == Some("PreferClose") {
        warnings.push(
            "spec.trafficDistribution: \"PreferClose\" is deprecated; use \"PreferSameZone\""
                .to_string(),
        );
    }
    warnings
}

/// `svcStrategy` (strategy.go).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<Service> for Strategy {
    /// `PrepareForCreate`: status is cleared. `dropServiceDisabledFields`
    /// drops nothing: `ServiceTrafficDistribution` is GA in 1.35.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut Service) {
        obj.status = empty_status();
    }

    fn validate(&self, _ctx: &RequestContext, obj: &Service) -> ErrorList {
        validate_service_create(obj)
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &Service) -> Vec<String> {
        get_warnings_for_service(obj)
    }
}

impl RestUpdateStrategy<Service> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        true
    }

    /// `PrepareForUpdate`: status is kept, and fields the new type does not
    /// use are dropped.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Service, old: &Service) {
        obj.status = old.status.clone();
        drop_type_dependent_fields(obj, old);
    }

    fn validate_update(&self, _ctx: &RequestContext, obj: &Service, old: &Service) -> ErrorList {
        validate_service_update(obj, old)
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &Service,
        _old: &Service,
    ) -> Vec<String> {
        get_warnings_for_service(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `svcStrategy` implements no `GarbageCollectionDeleteStrategy`.
impl RestDeleteStrategy<Service> for Strategy {}

/// `serviceStatusStrategy` (strategy.go).
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<Service> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        true
    }

    /// Only status may change.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Service, old: &Service) {
        obj.spec = old.spec.clone();
    }

    fn validate_update(&self, _ctx: &RequestContext, obj: &Service, old: &Service) -> ErrorList {
        validate_service_status_update(obj, old)
    }

    /// `WarningsOnUpdate`: `GetWarningsForIP` on each ingress IP.
    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &Service,
        _old: &Service,
    ) -> Vec<String> {
        let ingress = obj
            .status
            .as_ref()
            .and_then(|s| s.load_balancer.as_ref())
            .map(|lb| lb.ingress.as_slice())
            .unwrap_or(&[]);
        ingress
            .iter()
            .enumerate()
            .filter_map(|(i, ing)| {
                let ip = ing.ip.as_deref().filter(|ip| !ip.is_empty())?;
                Some(get_warnings_for_ip(
                    &format!("status.loadBalancer.ingress[{i}]"),
                    ip,
                ))
            })
            .flatten()
            .collect()
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}
