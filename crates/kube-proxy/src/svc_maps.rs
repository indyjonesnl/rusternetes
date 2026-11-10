//! The two views of Service / EndpointSlice state that upstream feeds to the
//! `ServiceHealthServer` at the end of every `syncProxyRules`
//! (pkg/proxy/iptables/proxier.go:1527-1535):
//! `ServicePortMap.HealthCheckNodePorts` and `EndpointsMap.LocalReadyEndpoints`.
//!
//! This crate has no `ServicePortMap`/`EndpointsMap` yet, so each is computed
//! straight from the listed objects, following the same filters upstream
//! applies while building those maps.

use crate::iptables::{cluster_ip_for_family, IpFamily};
use crate::service_health::NamespacedName;
use rusternetes_common::resources::service::ServiceExternalTrafficPolicy;
use rusternetes_common::resources::{EndpointSlice, Service, ServiceType};
use std::collections::{HashMap, HashSet};

/// `proxyutil.ShouldSkipService` (pkg/proxy/util/utils.go:55-67) plus the
/// per-family ClusterIP lookup `newBaseServiceInfo` performs
/// (serviceport.go:176 `GetClusterIPByFamily`; the service-change tracker
/// drops a Service with no ClusterIP in its family).
fn proxied_for_family(service: &Service, family: IpFamily) -> bool {
    let cluster_ip = service.spec.cluster_ip.as_deref().unwrap_or("");
    // helper.IsServiceIPSet: ClusterIP is neither "None" nor empty.
    if cluster_ip.is_empty() || cluster_ip == "None" {
        return false;
    }
    if service.spec.service_type == Some(ServiceType::ExternalName) {
        return false;
    }
    cluster_ip_for_family(service, family).is_some()
}

/// `apiservice.NeedsHealthCheck` (pkg/api/service/util.go:87-93):
/// type=LoadBalancer with `RequestsOnlyLocalTraffic`.
fn needs_health_check(service: &Service) -> bool {
    // RequestsOnlyLocalTraffic also accepts NodePort, but NeedsHealthCheck
    // narrows to LoadBalancer first.
    service.spec.service_type == Some(ServiceType::LoadBalancer)
        && service.spec.external_traffic_policy == Some(ServiceExternalTrafficPolicy::Local)
}

/// `ServicePortMap.HealthCheckNodePorts` (pkg/proxy/servicechangetracker.go:119-133):
/// Service name -> `healthCheckNodePort` for every proxied Service port with a
/// non-zero one (`newBaseServiceInfo`, serviceport.go:226-233).
pub fn health_check_node_ports(
    services: &[Service],
    family: IpFamily,
) -> HashMap<NamespacedName, u16> {
    let mut ports = HashMap::new();
    for service in services {
        // One ServicePortInfo per Service port; a Service with no ports has
        // no entry.
        if service.spec.ports.is_empty()
            || !proxied_for_family(service, family)
            || !needs_health_check(service)
        {
            continue;
        }
        // (`klog.ErrorS "Service has no healthcheck nodeport"` when zero.)
        let port = service.spec.health_check_node_port.unwrap_or(0);
        if port != 0 {
            let nsn = NamespacedName::new(
                service.metadata.namespace.clone().unwrap_or_default(),
                service.metadata.name.clone(),
            );
            ports.insert(nsn, port as u16);
        }
    }
    ports
}

/// `EndpointsMap.LocalReadyEndpoints` (pkg/proxy/endpointschangetracker.go:252-285)
/// over the endpoints `EndpointSliceCache.addEndpoints`
/// (endpointslicecache.go:188-243) would record for `family` and `node_name`.
///
/// Counts distinct local ready IPs per Service: a Service pointing at
/// 10.0.0.1:80 and 10.0.0.1:443 has 1 endpoint, not 2. Terminating endpoints
/// are not counted even though they may still serve traffic.
pub fn local_ready_endpoints(
    slices: &[EndpointSlice],
    node_name: &str,
    family: IpFamily,
) -> HashMap<NamespacedName, usize> {
    // EndpointsChangeTracker.EndpointSliceUpdate ignores other address types
    // (endpointschangetracker.go:82-85).
    let address_type = match family {
        IpFamily::V4 => "IPv4",
        IpFamily::V6 => "IPv6",
    };
    let mut local_ips: HashMap<NamespacedName, HashSet<String>> = HashMap::new();
    for slice in slices {
        if slice.address_type != address_type {
            continue;
        }
        // endpointSliceCacheKeyFunc: a slice without the service-name label
        // is an error and is skipped (endpointslicecache.go:315-318).
        let Some(service_name) = slice
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
        else {
            continue;
        };
        let nsn = NamespacedName::new(
            slice.metadata.namespace.clone().unwrap_or_default(),
            service_name.clone(),
        );
        // endpointInfoByServicePort: ports with a zero/absent number are
        // ignored, and a slice with no valid port yields no endpoints. (A
        // nil port name is defaulted to "" by the API server, so it is valid.)
        if !slice.ports.iter().any(|p| p.port.is_some_and(|n| n != 0)) {
            continue;
        }
        for endpoint in &slice.endpoints {
            let Some(address) = endpoint.addresses.first() else {
                continue; // "Ignoring invalid endpoint port with empty address"
            };
            // isLocal: len(cache.nodeName) > 0 && nodeName == cache.nodeName.
            let is_local = endpoint
                .node_name
                .as_deref()
                .is_some_and(|n| !node_name.is_empty() && n == node_name);
            let ready = endpoint
                .conditions
                .as_ref()
                .and_then(|c| c.ready)
                .unwrap_or(true);
            // Only add ready endpoints for health checking. Terminating
            // endpoints may still serve traffic but the health check signal
            // should fail if there are only terminating endpoints on a node.
            if !ready || !is_local {
                continue;
            }
            // utilnet.ParseIPSloppy(...).String()
            let ip = address
                .parse::<std::net::IpAddr>()
                .map(|ip| ip.to_string())
                .unwrap_or_else(|_| address.clone());
            local_ips.entry(nsn.clone()).or_default().insert(ip);
        }
    }
    local_ips
        .into_iter()
        .map(|(nsn, ips)| (nsn, ips.len()))
        .collect()
}
