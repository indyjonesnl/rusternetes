//! Service health inputs: `ServicePortMap.HealthCheckNodePorts`
//! (pkg/proxy/servicechangetracker.go:119) and
//! `EndpointsMap.LocalReadyEndpoints` (pkg/proxy/endpointschangetracker.go:276).
//! Cases follow `TestHealthCheckNodePorts`/`TestLocalReadyEndpoints`-style
//! expectations: only LoadBalancer+Local with a port, distinct local IPs,
//! ready only.

use rusternetes_common::resources::endpointslice::EndpointPort;
use rusternetes_common::resources::service::ServiceExternalTrafficPolicy;
use rusternetes_common::resources::{
    Endpoint, EndpointConditions, EndpointSlice, Service, ServicePort, ServiceSpec, ServiceType,
};
use rusternetes_kube_proxy::iptables::IpFamily;
use rusternetes_kube_proxy::service_health::NamespacedName;
use rusternetes_kube_proxy::svc_maps::{health_check_node_ports, local_ready_endpoints};
use std::collections::HashMap;

fn svc(
    name: &str,
    ty: ServiceType,
    policy: Option<ServiceExternalTrafficPolicy>,
    hc_port: Option<i32>,
    cluster_ip: &str,
) -> Service {
    let mut s = Service::new(
        name,
        ServiceSpec {
            service_type: Some(ty),
            external_traffic_policy: policy,
            health_check_node_port: hc_port,
            cluster_ip: Some(cluster_ip.to_string()),
            ports: vec![ServicePort {
                name: None,
                port: 80,
                target_port: None,
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            ..Default::default()
        },
    );
    s.metadata.namespace = Some("ns".into());
    s
}

fn nsn(name: &str) -> NamespacedName {
    NamespacedName::new("ns", name)
}

#[test]
fn health_check_node_ports_only_for_loadbalancer_local() {
    let local = Some(ServiceExternalTrafficPolicy::Local);
    let services = vec![
        svc(
            "lb-local",
            ServiceType::LoadBalancer,
            local.clone(),
            Some(30001),
            "10.0.0.1",
        ),
        // NodePort + Local does not NeedsHealthCheck.
        svc(
            "np-local",
            ServiceType::NodePort,
            local.clone(),
            Some(30002),
            "10.0.0.2",
        ),
        svc(
            "lb-cluster",
            ServiceType::LoadBalancer,
            Some(ServiceExternalTrafficPolicy::Cluster),
            Some(30003),
            "10.0.0.3",
        ),
        // No healthCheckNodePort: logged, not mapped.
        svc(
            "lb-noport",
            ServiceType::LoadBalancer,
            local.clone(),
            None,
            "10.0.0.4",
        ),
        // ShouldSkipService: headless.
        svc(
            "lb-headless",
            ServiceType::LoadBalancer,
            local.clone(),
            Some(30005),
            "None",
        ),
        // ShouldSkipService: ExternalName.
        svc(
            "ext",
            ServiceType::ExternalName,
            local.clone(),
            Some(30006),
            "10.0.0.6",
        ),
        // Other family has no entry for the v4 proxier.
        svc(
            "lb-v6",
            ServiceType::LoadBalancer,
            local,
            Some(30007),
            "fd00::1",
        ),
    ];
    let got = health_check_node_ports(&services, IpFamily::V4);
    assert_eq!(got, HashMap::from([(nsn("lb-local"), 30001)]));
}

fn endpoint(ip: &str, node: Option<&str>, ready: Option<bool>) -> Endpoint {
    Endpoint {
        addresses: vec![ip.to_string()],
        conditions: Some(EndpointConditions {
            ready,
            serving: None,
            terminating: None,
        }),
        node_name: node.map(str::to_string),
        hostname: None,
        target_ref: None,
        zone: None,
        hints: None,
        deprecated_topology: None,
    }
}

fn slice(svc_name: &str, ports: &[i32], endpoints: Vec<Endpoint>) -> EndpointSlice {
    let mut s = EndpointSlice::new(format!("{svc_name}-abc"), "IPv4");
    s.metadata.namespace = Some("ns".into());
    s.metadata.labels = Some(HashMap::from([(
        "kubernetes.io/service-name".to_string(),
        svc_name.to_string(),
    )]));
    s.ports = ports
        .iter()
        .map(|p| EndpointPort {
            name: Some(String::new()),
            port: Some(*p),
            protocol: "TCP".into(),
            app_protocol: None,
        })
        .collect();
    s.endpoints = endpoints;
    s
}

#[test]
fn local_ready_endpoints_counts_distinct_local_ready_ips() {
    let slices = vec![
        // Two ports, one local ready IP: counted once, not twice.
        slice(
            "a",
            &[80, 443],
            vec![
                endpoint("10.0.0.1", Some("me"), Some(true)),
                endpoint("10.0.0.2", Some("other"), Some(true)),
                // not ready (terminating endpoints are never ready)
                endpoint("10.0.0.3", Some("me"), Some(false)),
                // nil node name is not local
                endpoint("10.0.0.4", None, Some(true)),
            ],
        ),
        // nil ready means ready.
        slice("b", &[80], vec![endpoint("10.0.1.1", Some("me"), None)]),
        // Only non-ready local endpoints: no entry at all.
        slice(
            "c",
            &[80],
            vec![endpoint("10.0.2.1", Some("me"), Some(false))],
        ),
        // Zero port: slice ignored.
        slice(
            "d",
            &[0],
            vec![endpoint("10.0.3.1", Some("me"), Some(true))],
        ),
    ];
    let got = local_ready_endpoints(&slices, "me", IpFamily::V4);
    assert_eq!(got, HashMap::from([(nsn("a"), 1), (nsn("b"), 1)]));
}

#[test]
fn local_ready_endpoints_dedupes_across_slices_and_filters_family_and_label() {
    let mut v6 = slice(
        "a",
        &[80],
        vec![endpoint("fd00::5", Some("me"), Some(true))],
    );
    v6.address_type = "IPv6".into();
    let mut unlabeled = slice(
        "a",
        &[80],
        vec![endpoint("10.0.0.9", Some("me"), Some(true))],
    );
    unlabeled.metadata.labels = None;
    let slices = vec![
        slice(
            "a",
            &[80],
            vec![endpoint("10.0.0.1", Some("me"), Some(true))],
        ),
        slice(
            "a",
            &[80],
            vec![endpoint("10.0.0.1", Some("me"), Some(true))],
        ),
        slice(
            "a",
            &[80],
            vec![endpoint("10.0.0.2", Some("me"), Some(true))],
        ),
        v6,
        unlabeled,
    ];
    assert_eq!(
        local_ready_endpoints(&slices, "me", IpFamily::V4),
        HashMap::from([(nsn("a"), 2)])
    );
    assert_eq!(
        local_ready_endpoints(&slices, "me", IpFamily::V6),
        HashMap::from([(nsn("a"), 1)])
    );
    // An empty node name never matches (EndpointSliceCache.isLocal).
    assert!(local_ready_endpoints(&slices, "", IpFamily::V4).is_empty());
}
