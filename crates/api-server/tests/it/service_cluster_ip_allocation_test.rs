//! ClusterIPs are allocated as IPAddress objects out of the ServiceCIDRs
//! (upstream pkg/registry/core/service/ipallocator, MetaAllocator). The old
//! allocator kept its set in memory, so a restart forgot every allocation,
//! and it never recorded one in the API.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SVCS: &str = "/api/v1/namespaces/default/services";
const IPS: &str = "/apis/networking.k8s.io/v1/ipaddresses";

fn svc(name: &str, cluster_ip: Option<&str>) -> Value {
    let mut spec = json!({"selector": {"app": name}, "ports": [{"port": 80, "protocol": "TCP"}]});
    if let Some(ip) = cluster_ip {
        spec["clusterIP"] = json!(ip);
    }
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "default"},
        "spec": spec
    })
}

#[tokio::test]
async fn a_cluster_ip_is_an_ip_address_object_until_the_service_goes() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SVCS, &svc("web", None)).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let ip = created["spec"]["clusterIP"].as_str().unwrap().to_string();
    assert_eq!(created["spec"]["clusterIPs"], json!([ip]));

    let (s, addr) = api.get(&format!("{IPS}/{ip}")).await;
    assert_eq!(s, StatusCode::OK, "{addr}");
    assert_eq!(addr["spec"]["parentRef"]["resource"], "services");
    assert_eq!(addr["spec"]["parentRef"]["namespace"], "default");
    assert_eq!(addr["spec"]["parentRef"]["name"], "web");
    assert_eq!(
        addr["metadata"]["labels"]["ipaddress.kubernetes.io/managed-by"],
        "ipallocator.k8s.io"
    );

    let (s, _) = api.delete(&format!("{SVCS}/web")).await;
    assert_eq!(s, StatusCode::OK);
    let (s, _) = api.get(&format!("{IPS}/{ip}")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "the IPAddress is released");
}

#[tokio::test]
async fn a_requested_cluster_ip_in_use_is_rejected() {
    let api = TestApiServer::new();
    let (s, _) = api.post(SVCS, &svc("a", Some("10.96.0.50"))).await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = api.post(SVCS, &svc("b", Some("10.96.0.50"))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("spec.clusterIPs")
            && msg.contains("failed to allocate IP 10.96.0.50: provided IP is already allocated"),
        "{body}"
    );
}

#[tokio::test]
async fn a_cluster_ip_outside_every_service_cidr_is_rejected() {
    let api = TestApiServer::new();
    let (s, body) = api.post(SVCS, &svc("a", Some("192.168.7.7"))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(msg.contains("failed to allocate IP 192.168.7.7"), "{body}");
}

#[tokio::test]
async fn a_failed_create_releases_its_cluster_ip() {
    let api = TestApiServer::new();
    // The node port is taken, so the create fails after the ClusterIP was
    // claimed; the ClusterIP must come back.
    let np = |name: &str| {
        let mut s = svc(name, Some("10.96.0.60"));
        s["spec"]["type"] = json!("NodePort");
        s["spec"]["ports"][0]["nodePort"] = json!(30700);
        s
    };
    let (s, _) = api.post(SVCS, &np("a")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, _) = api.delete(&format!("{SVCS}/a")).await;
    assert_eq!(s, StatusCode::OK);

    let mut holder = svc("holder", None);
    holder["spec"]["type"] = json!("NodePort");
    holder["spec"]["ports"][0]["nodePort"] = json!(30700);
    let (s, _) = api.post(SVCS, &holder).await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = api.post(SVCS, &np("b")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let (s, _) = api.get(&format!("{IPS}/10.96.0.60")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "the ClusterIP was rolled back");
}

#[tokio::test]
async fn an_external_name_round_trip_allocates_and_frees() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SVCS, &svc("x", None)).await;
    assert_eq!(s, StatusCode::CREATED);
    let ip = created["spec"]["clusterIP"].as_str().unwrap().to_string();

    let mut ext = created.clone();
    ext["spec"] = json!({"type": "ExternalName", "externalName": "example.com"});
    let (s, body) = api.put(&format!("{SVCS}/x"), &ext).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, _) = api.get(&format!("{IPS}/{ip}")).await;
    assert_eq!(
        s,
        StatusCode::NOT_FOUND,
        "ClusterIP -> ExternalName frees it"
    );

    let mut back = body.clone();
    back["spec"] = json!({"type": "ClusterIP", "selector": {"app": "x"},
        "ports": [{"port": 80, "protocol": "TCP"}]});
    let (s, body) = api.put(&format!("{SVCS}/x"), &back).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let ip = body["spec"]["clusterIP"].as_str().unwrap();
    let (s, _) = api.get(&format!("{IPS}/{ip}")).await;
    assert_eq!(s, StatusCode::OK, "ExternalName -> ClusterIP allocates");
}
