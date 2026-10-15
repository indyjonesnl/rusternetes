//! Mirror of `[sig-network] EndpointSliceMirroring should mirror a custom
//! Endpoints resource through create update and delete` (#3768).
//!
//! Upstream (`release-1.35`): `test/e2e/network/endpointslicemirroring.go:55`.
//! The older mirror in `controller-manager/tests/it/conformance_network_services.rs`
//! drives storage directly; this one issues the upstream client calls through
//! the api-server router and runs the real EndpointSlice controller
//! (`EndpointSliceController`, which hosts the mirroring pass, ported from
//! `pkg/controller/endpointslicemirroring`) over `ApiStorage` pointed at the
//! served router. Upstream's 12s `wait.PollImmediate` loops become bounded
//! controller passes. Needs no kubelet or kube-proxy.

use std::sync::Arc;

use rusternetes_client::http::ApiClient;
use rusternetes_controller_manager::controllers::endpointslice::EndpointSliceController;
use rusternetes_storage::api_storage::ApiStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";
const MAX_PASSES: usize = 5;

struct Served {
    api: TestApiServer,
    storage: Arc<ApiStorage>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve() -> Served {
    let api = TestApiServer::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    let server = tokio::spawn({
        let router = api.router.clone();
        async move {
            let _ = axum::serve(listener, router).await;
        }
    });
    let client =
        Arc::new(ApiClient::new(&format!("http://{address}"), true, None).expect("client"));
    Served {
        api,
        storage: Arc::new(ApiStorage::new(client)),
        server,
    }
}

async fn send(api: &TestApiServer, method: &str, uri: &str, body: Option<&Value>) -> (u16, Value) {
    let (status, value) = api.send(method, uri, Some("application/json"), body).await;
    (status.as_u16(), value)
}

/// `EndpointSlices(ns).List(LabelSelector: kubernetes.io/service-name=<svc>)`.
async fn slices_for(api: &TestApiServer, svc: &str) -> Vec<Value> {
    let uri = format!(
        "/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3D{svc}"
    );
    let (s, l) = send(api, "GET", &uri, None).await;
    assert_eq!(s, 200, "list EndpointSlices: {l}");
    l["items"].as_array().cloned().unwrap_or_default()
}

/// `wait.PollImmediate`: reconcile until `done(slices)` holds; returns the last list.
async fn poll<F: Fn(&[Value]) -> bool>(served: &Served, svc: &str, done: F) -> (bool, Vec<Value>) {
    let ctrl = EndpointSliceController::new(served.storage.clone());
    for _ in 0..MAX_PASSES {
        let items = slices_for(&served.api, svc).await;
        if done(&items) {
            return (true, items);
        }
        ctrl.reconcile_all().await.expect("endpointslice reconcile");
    }
    let items = slices_for(&served.api, svc).await;
    (done(&items), items)
}

/// :69-77 / :135-137 every slice must have 1 port 80, 1 endpoint, 1 address == `ip`.
fn all_match(items: &[Value], ip: &str) -> bool {
    !items.is_empty()
        && items.iter().all(|s| {
            s["ports"].as_array().map(|p| p.len()) == Some(1)
                && s["ports"][0]["port"] == 80
                && s["endpoints"].as_array().map(|e| e.len()) == Some(1)
                && s["endpoints"][0]["addresses"] == json!([ip])
        })
}

fn endpoints(ip: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Endpoints",
        "metadata": { "name": "example-custom-endpoints", "namespace": NS },
        "subsets": [{ "ports": [{ "port": 80 }], "addresses": [{ "ip": ip }] }],
    })
}

/// endpointslicemirroring.go:55.
#[tokio::test]
async fn conformance_endpointslice_mirrors_custom_endpoints() {
    let served = serve().await;
    let api = &served.api;
    let svc_name = "example-custom-endpoints";

    // Description: discovery.k8s.io in /apis, v1 in its group doc, and the
    // endpointslices resource in the v1 doc.
    let (s, apis) = send(api, "GET", "/apis", None).await;
    assert_eq!(s, 200);
    assert!(
        apis["groups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["name"] == "discovery.k8s.io"),
        "{apis}"
    );
    let (s, grp) = send(api, "GET", "/apis/discovery.k8s.io", None).await;
    assert_eq!(s, 200, "{grp}");
    assert!(
        grp["versions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["version"] == "v1"),
        "{grp}"
    );
    let (s, res) = send(api, "GET", "/apis/discovery.k8s.io/v1", None).await;
    assert_eq!(s, 200, "{res}");
    assert!(
        res["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["name"] == "endpointslices"),
        "{res}"
    );

    // :56-71 create the selector-less Service (port "example" 80/TCP).
    let svc = json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": { "name": svc_name, "namespace": NS },
        "spec": { "ports": [{ "name": "example", "port": 80, "protocol": "TCP" }] },
    });
    let (s, body) = send(
        api,
        "POST",
        &format!("/api/v1/namespaces/{NS}/services"),
        Some(&svc),
    )
    .await;
    assert_eq!(s, 201, "error creating Service: {body}");

    // :86-130 "mirroring a new custom Endpoint".
    let ep_uri = format!("/api/v1/namespaces/{NS}/endpoints");
    let (s, body) = send(api, "POST", &ep_uri, Some(&endpoints("10.1.2.3"))).await;
    assert_eq!(s, 201, "Unexpected error creating Endpoints: {body}");
    let (ok, items) = poll(&served, svc_name, |i| all_match(i, "10.1.2.3")).await;
    assert!(
        ok,
        "Did not find matching EndpointSlice for {NS}/{svc_name}: {items:?}"
    );

    // :132-176 "mirroring an update to a custom Endpoint": the update carries no
    // resourceVersion (upstream reuses its local object), so it is unconditional.
    let (s, body) = send(
        api,
        "PUT",
        &format!("{ep_uri}/{svc_name}"),
        Some(&endpoints("10.2.3.4")),
    )
    .await;
    assert_eq!(s, 200, "Unexpected error updating Endpoints: {body}");
    let (ok, items) = poll(&served, svc_name, |i| {
        i.len() == 1 && all_match(i, "10.2.3.4")
    })
    .await;
    assert!(
        ok,
        "Did not find matching EndpointSlice for {NS}/{svc_name}: {items:?}"
    );

    // :178-200 "mirroring deletion of a custom Endpoint".
    let (s, body) = send(api, "DELETE", &format!("{ep_uri}/{svc_name}"), None).await;
    assert_eq!(s, 200, "Unexpected error deleting Endpoints: {body}");
    let (ok, items) = poll(&served, svc_name, |i| i.is_empty()).await;
    assert!(
        ok,
        "Waiting for 0 EndpointSlices to exist, got {}: {items:?}",
        items.len()
    );
}
