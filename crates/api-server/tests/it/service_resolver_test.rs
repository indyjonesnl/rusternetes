//! Ports of `TestResolve` and `TestResolveEndpointDistribution`
//! (staging/src/k8s.io/apiserver/pkg/util/proxy/proxy_test.go) plus the
//! loopback resolver and `buildServiceResolver`'s routing choice
//! (kube-aggregator/pkg/apiserver/resolvers.go,
//! cmd/kube-apiserver/app/server.go:283).

use rusternetes_api_server::service_resolver::{
    resolve_cluster, resolve_endpoint, Routing, ServiceResolver,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::{json, Value};
use std::collections::BTreeSet;

async fn put_service(storage: &MemoryStorage, spec: Value) {
    let svc = json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": { "namespace": "one", "name": "alfa" },
        "spec": spec,
    });
    storage
        .create::<rusternetes_common::resources::Service>(
            &build_key("services", Some("one"), "alfa"),
            &serde_json::from_value(svc).unwrap(),
        )
        .await
        .unwrap();
}

async fn put_slice(
    storage: &MemoryStorage,
    ns: &str,
    svc: &str,
    suffix: &str,
    address_type: &str,
    port: Option<(&str, i32)>,
    endpoints: Value,
) {
    let ports = port
        .map(|(n, p)| json!([{ "name": n, "port": p }]))
        .unwrap_or(json!([]));
    let slice = json!({
        "apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
        "metadata": {
            "namespace": ns, "name": format!("{svc}-{suffix}"),
            "labels": { "kubernetes.io/service-name": svc },
        },
        "addressType": address_type,
        "endpoints": endpoints,
        "ports": ports,
    });
    storage
        .create::<rusternetes_common::resources::EndpointSlice>(
            &build_key("endpointslices", Some(ns), &format!("{svc}-{suffix}")),
            &serde_json::from_value(slice).unwrap(),
        )
        .await
        .unwrap();
}

fn https_ports() -> Value {
    json!([
        { "name": "https", "port": 443, "targetPort": 1443 },
        { "name": "", "port": 1234, "targetPort": 1234 },
    ])
}

async fn matching_slice(storage: &MemoryStorage) {
    put_slice(
        storage,
        "one",
        "alfa",
        "xxx",
        "IPv4",
        Some(("https", 1443)),
        json!([{ "hostname": "dummy-host", "addresses": ["127.0.0.1"] }]),
    )
    .await;
}

type Expect = Result<(&'static str, u16), ()>;

async fn check(storage: &MemoryStorage, name: &str, cluster: Expect, endpoint: Expect) {
    let c = resolve_cluster(storage, "one", "alfa", 443).await;
    let e = resolve_endpoint(storage, "one", "alfa", 443).await;
    for (mode, want, got) in [("cluster", cluster, c), ("endpoint", endpoint, e)] {
        match (want, got) {
            (Ok((h, p)), Ok((gh, gp))) => assert_eq!((h, p), (gh.as_str(), gp), "{name} {mode}"),
            (Err(()), Err(_)) => {}
            (w, g) => panic!("{name} in {mode} mode: want {w:?}, got {g:?}"),
        }
    }
}

#[tokio::test]
async fn cluster_ip_without_443_port() {
    let s = MemoryStorage::new();
    put_service(
        &s,
        json!({ "type": "ClusterIP", "clusterIP": "hit",
                "ports": [{ "port": 1234, "targetPort": 1234 }] }),
    )
    .await;
    check(&s, "no 443", Err(()), Err(())).await;
}

#[tokio::test]
async fn cluster_ip_resolves_service_port_or_endpoint_target_port() {
    let s = MemoryStorage::new();
    put_service(
        &s,
        json!({ "type": "ClusterIP", "clusterIP": "hit", "ports": https_ports() }),
    )
    .await;
    matching_slice(&s).await;
    check(&s, "cluster ip", Ok(("hit", 443)), Ok(("127.0.0.1", 1443))).await;
}

#[tokio::test]
async fn cluster_ip_without_endpointslices() {
    let s = MemoryStorage::new();
    put_service(
        &s,
        json!({ "type": "ClusterIP", "clusterIP": "hit", "ports": https_ports() }),
    )
    .await;
    check(&s, "no slices", Ok(("hit", 443)), Err(())).await;
}

#[tokio::test]
async fn endpointslice_without_addresses() {
    let s = MemoryStorage::new();
    put_service(
        &s,
        json!({ "type": "ClusterIP", "clusterIP": "hit", "ports": https_ports() }),
    )
    .await;
    put_slice(&s, "one", "alfa", "xxx", "IPv4", None, json!([])).await;
    check(&s, "no addresses", Ok(("hit", 443)), Err(())).await;
}

#[tokio::test]
async fn none_cluster_ip() {
    let s = MemoryStorage::new();
    put_service(&s, json!({ "type": "ClusterIP", "clusterIP": "None" })).await;
    check(&s, "none", Err(()), Err(())).await;
}

#[tokio::test]
async fn load_balancer_and_node_port() {
    for (ty, ip) in [("LoadBalancer", "lb"), ("NodePort", "np")] {
        let s = MemoryStorage::new();
        put_service(
            &s,
            json!({ "type": ty, "clusterIP": ip, "ports": https_ports() }),
        )
        .await;
        matching_slice(&s).await;
        check(&s, ty, Ok((ip, 443)), Ok(("127.0.0.1", 1443))).await;
    }
}

#[tokio::test]
async fn external_name_only_resolves_in_cluster_mode() {
    let s = MemoryStorage::new();
    put_service(
        &s,
        json!({ "type": "ExternalName", "externalName": "foo.bar.com" }),
    )
    .await;
    check(&s, "external name", Ok(("foo.bar.com", 443)), Err(())).await;
}

#[tokio::test]
async fn unsupported_service_type_and_missing_service() {
    let s = MemoryStorage::new();
    put_service(&s, json!({ "type": "unsupported" })).await;
    check(&s, "unsupported", Err(()), Err(())).await;
    check(&MemoryStorage::new(), "missing", Err(()), Err(())).await;
}

// ----- TestResolveEndpointDistribution --------------------------------------

async fn distribution(families: Value, slices: Vec<(&str, &str, Value)>) -> BTreeSet<String> {
    let s = MemoryStorage::new();
    put_service(
        &s,
        json!({ "type": "ClusterIP", "ipFamilies": families,
                "ports": [{ "name": "https", "port": 443, "targetPort": 443 }] }),
    )
    .await;
    for (suffix, family, eps) in slices {
        put_slice(&s, "one", "alfa", suffix, family, Some(("https", 443)), eps).await;
    }
    let mut seen = BTreeSet::new();
    for _ in 0..300 {
        let (h, p) = resolve_endpoint(&s, "one", "alfa", 443).await.unwrap();
        seen.insert(format!("{h}:{p}"));
    }
    seen
}

fn set(v: &[&str]) -> BTreeSet<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn distribution_covers_every_slice() {
    let got = distribution(
        json!(["IPv4"]),
        vec![
            ("1", "IPv4", json!([{ "addresses": ["10.0.0.1"] }])),
            ("2", "IPv4", json!([{ "addresses": ["10.0.0.2"] }])),
        ],
    )
    .await;
    assert_eq!(got, set(&["10.0.0.1:443", "10.0.0.2:443"]));
}

#[tokio::test]
async fn distribution_skips_non_ready_endpoints() {
    let got = distribution(
        json!(["IPv4"]),
        vec![
            (
                "1",
                "IPv4",
                json!([
                    { "addresses": ["10.0.0.1"] },
                    { "addresses": ["10.0.0.2"], "conditions": { "ready": false } },
                    { "addresses": ["10.0.0.3"], "conditions": { "ready": true } },
                ]),
            ),
            (
                "2",
                "IPv4",
                json!([{ "addresses": ["10.0.0.4"], "conditions": { "ready": true } }]),
            ),
            (
                "3",
                "IPv4",
                json!([{ "addresses": ["10.0.0.5"], "conditions": { "ready": false } }]),
            ),
        ],
    )
    .await;
    assert_eq!(got, set(&["10.0.0.1:443", "10.0.0.3:443", "10.0.0.4:443"]));
}

#[tokio::test]
async fn dual_stack_prefers_primary_family_slices() {
    let ready = |a: &str| json!({ "addresses": [a], "conditions": { "ready": true } });
    let got = distribution(
        json!(["IPv4", "IPv6"]),
        vec![
            ("v6", "IPv6", json!([ready("fd00::1"), ready("fd00::2")])),
            ("v4", "IPv4", json!([ready("10.0.0.1"), ready("10.0.0.2")])),
        ],
    )
    .await;
    assert_eq!(got, set(&["10.0.0.1:443", "10.0.0.2:443"]));
}

#[tokio::test]
async fn dual_stack_falls_back_when_primary_family_not_ready() {
    let down = |a: &str| json!({ "addresses": [a], "conditions": { "ready": false } });
    let up = |a: &str| json!({ "addresses": [a], "conditions": { "ready": true } });
    let got = distribution(
        json!(["IPv4", "IPv6"]),
        vec![
            ("v4", "IPv4", json!([down("10.0.0.1"), down("10.0.0.2")])),
            ("v6", "IPv6", json!([up("fd00::1"), up("fd00::2")])),
        ],
    )
    .await;
    assert_eq!(got, set(&["fd00::1:443", "fd00::2:443"]));
}

// ----- loopback resolver and routing choice ---------------------------------

#[tokio::test]
async fn loopback_resolver_short_circuits_default_kubernetes_443() {
    let s = MemoryStorage::new();
    let r = ServiceResolver::new(false, Some(("127.0.0.1".into(), 6443)));
    assert_eq!(
        r.resolve_endpoint(&s, "default", "kubernetes", 443)
            .await
            .unwrap(),
        ("127.0.0.1".to_string(), 6443)
    );
    // Any other port / service goes to the delegate (which finds nothing).
    assert!(r
        .resolve_endpoint(&s, "default", "kubernetes", 80)
        .await
        .is_err());
    assert!(r
        .resolve_endpoint(&s, "default", "other", 443)
        .await
        .is_err());
}

#[test]
fn routing_flag_picks_the_resolver() {
    assert_eq!(
        ServiceResolver::new(false, None).routing,
        Routing::ClusterIp
    );
    assert_eq!(ServiceResolver::new(true, None).routing, Routing::Endpoint);
    assert_eq!(ServiceResolver::default().routing, Routing::ClusterIp);
}

// Both entry points (`run` and the standalone binary) install the egress
// selector and the service resolver through `install_server_options`; a bad
// egress configuration must stop startup there, as
// `EgressSelectorOptions.Validate` does (egress_selector.go:80).
#[test]
fn install_server_options_rejects_an_unreadable_egress_config() {
    let cfg = rusternetes_api_server::ApiServerConfig {
        egress_selector_config_file: "/nonexistent/egress-selector.yaml".into(),
        ..Default::default()
    };
    assert!(rusternetes_api_server::install_server_options(&cfg).is_err());
}
