//! Mirrors of the two `[sig-network] Networking` API-server cases (#3768).
//!
//! Upstream (`release-1.35`): `test/e2e/network/apiserver.go`
//! - `:48` "should provide secure master service": the `kubernetes` Service
//!   MUST exist in `default`.
//! - `:59` "should have Endpoints and EndpointSlices pointing to API Server":
//!   the Service, an Endpoints with at least one subset, and EndpointSlices
//!   (label `kubernetes.io/service-name=kubernetes`) whose IPv4 addresses and
//!   ports equal the Endpoints' addresses and ports.
//!
//! Upstream gets these objects from the api-server's own bootstrap controller
//! (`pkg/controlplane/controller/kubernetesservice`). The mirror runs the same
//! reconcilers this api-server runs at startup (`bootstrap::reconcile_*`) with
//! a fixed address instead of the discovered interface IP, then issues the
//! upstream client calls through the router. Neither case needs kube-proxy.

use rusternetes_api_server::bootstrap::{
    reconcile_kubernetes_endpoint, reconcile_kubernetes_service,
};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::Value;
use std::collections::BTreeSet;

async fn bootstrapped() -> TestApiServer {
    let api = TestApiServer::new();
    reconcile_kubernetes_service(api.storage.as_ref(), 6443, "10.96.0.1".parse().unwrap())
        .await
        .expect("reconcile kubernetes Service");
    reconcile_kubernetes_endpoint(api.storage.as_ref(), "192.168.1.10", 6443)
        .await
        .expect("reconcile kubernetes Endpoints");
    api
}

/// apiserver.go:48 "should provide secure master service".
#[tokio::test]
async fn conformance_secure_master_service() {
    let api = bootstrapped().await;

    // :50 `Services("default").Get("kubernetes")` MUST succeed.
    let (s, svc) = api
        .get("/api/v1/namespaces/default/services/kubernetes")
        .await;
    assert_eq!(
        s, 200,
        "failed to fetch the service object for the service named kubernetes: {svc}"
    );
    assert_eq!(svc["metadata"]["name"], "kubernetes", "{svc}");
}

/// apiserver.go:59 "should have Endpoints and EndpointSlices pointing to API Server".
#[tokio::test]
async fn conformance_kubernetes_endpoints_and_endpointslices() {
    let api = bootstrapped().await;

    // :64-65 verify the Service exists.
    let (s, svc) = api
        .get("/api/v1/namespaces/default/services/kubernetes")
        .await;
    assert_eq!(s, 200, "{svc}");

    // :68-72 Endpoints exist with at least one subset.
    let (s, ep) = api
        .get("/api/v1/namespaces/default/endpoints/kubernetes")
        .await;
    assert_eq!(s, 200, "{ep}");
    let subsets = ep["subsets"].as_array().cloned().unwrap_or_default();
    assert!(
        !subsets.is_empty(),
        "Expected at least 1 subset in endpoints: {ep}"
    );

    // :75-87 collect addresses (ready and not ready) and ports.
    let mut ep_addresses = BTreeSet::new();
    let mut ep_ports = BTreeSet::new();
    for subset in &subsets {
        for key in ["addresses", "notReadyAddresses"] {
            for a in subset[key].as_array().into_iter().flatten() {
                ep_addresses.insert(a["ip"].as_str().unwrap().to_string());
            }
        }
        for p in subset["ports"].as_array().into_iter().flatten() {
            ep_ports.insert(p["port"].as_i64().unwrap());
        }
    }

    // :90-96 EndpointSlices selected by `kubernetes.io/service-name=kubernetes`.
    let (s, list) = api
        .get("/apis/discovery.k8s.io/v1/namespaces/default/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3Dkubernetes")
        .await;
    assert_eq!(s, 200, "{list}");
    let slices: Vec<Value> = list["items"].as_array().cloned().unwrap_or_default();
    assert!(
        !slices.is_empty(),
        "Expected at least 1 EndpointSlice: {list}"
    );

    // :98-122 only slices of the primary (IPv4) family are compared.
    let mut slice_addresses = BTreeSet::new();
    let mut slice_ports = BTreeSet::new();
    for slice in slices.iter().filter(|s| s["addressType"] == "IPv4") {
        for e in slice["endpoints"].as_array().into_iter().flatten() {
            for a in e["addresses"].as_array().into_iter().flatten() {
                slice_addresses.insert(a.as_str().unwrap().to_string());
            }
        }
        for p in slice["ports"].as_array().into_iter().flatten() {
            if let Some(p) = p["port"].as_i64() {
                slice_ports.insert(p);
            }
        }
    }
    assert_eq!(
        slice_addresses, ep_addresses,
        "EndpointSlice addresses do not match Endpoints addresses"
    );
    assert_eq!(
        slice_ports, ep_ports,
        "EndpointSlice ports do not match Endpoints ports"
    );
}
