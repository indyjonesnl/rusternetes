//! EndpointSlice on the generic Store (#1990): the strategy rules of
//! `pkg/registry/discovery/endpointslice/strategy.go`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const EPS: &str = "/apis/discovery.k8s.io/v1/namespaces/default/endpointslices";

fn slice(name: &str) -> Value {
    json!({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
           "metadata": {"name": name}, "addressType": "IPv4",
           "endpoints": [{"addresses": ["10.0.0.1"]}],
           "ports": [{"port": 80}]})
}

/// `PrepareForCreate` starts the generation at 1; a user-created slice gets no
/// managed-by label of ours (the old handler added one).
#[tokio::test]
async fn create_sets_generation_one_and_no_managed_by_label() {
    let api = TestApiServer::new();
    let (s, body) = api.post(EPS, &slice("e1")).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
    assert!(
        body["metadata"]["labels"]
            .get("endpointslice.kubernetes.io/managed-by")
            .is_none(),
        "{body}"
    );
    assert_eq!(body["ports"][0]["protocol"], "TCP", "{body}");
}

/// `dropTopologyOnV1`: writes to `deprecatedTopology` are dropped.
#[tokio::test]
async fn deprecated_topology_is_dropped() {
    let api = TestApiServer::new();
    let mut obj = slice("e1");
    obj["endpoints"][0]["deprecatedTopology"] = json!({"kubernetes.io/hostname": "node-1"});
    let (s, body) = api.post(EPS, &obj).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert!(
        body["endpoints"][0].get("deprecatedTopology").is_none(),
        "{body}"
    );
}

/// The generation moves when the endpoints or labels change, not otherwise.
#[tokio::test]
async fn generation_follows_content_and_labels() {
    let api = TestApiServer::new();
    let (s, created) = api.post(EPS, &slice("e1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut same = created.clone();
    same["metadata"]["annotations"] = json!({"a": "b"});
    let (s, body) = api.put(&format!("{EPS}/e1"), &same).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");

    let mut labelled = body.clone();
    labelled["metadata"]["labels"] = json!({"x": "y"});
    let (s, body) = api.put(&format!("{EPS}/e1"), &labelled).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 2, "{body}");

    let mut changed = body.clone();
    changed["endpoints"][0]["addresses"] = json!(["10.0.0.2"]);
    let (s, body) = api.put(&format!("{EPS}/e1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 3, "{body}");
}

/// `ValidateEndpointSliceUpdate`: addressType is immutable.
#[tokio::test]
async fn address_type_is_immutable() {
    let api = TestApiServer::new();
    let (s, created) = api.post(EPS, &slice("e1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["addressType"] = json!("IPv6");
    update["endpoints"][0]["addresses"] = json!(["fd00::1"]);
    let (s, body) = api.put(&format!("{EPS}/e1"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"].as_str().unwrap().contains("addressType"));
}

/// `AllowCreateOnUpdate() == false`.
#[tokio::test]
async fn a_put_to_a_missing_slice_is_not_found() {
    let api = TestApiServer::new();
    let (s, body) = api.put(&format!("{EPS}/missing"), &slice("missing")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
}

/// `ValidateObjectMeta` runs `NameIsDNSSubdomain` on the name.
#[tokio::test]
async fn an_invalid_name_is_rejected() {
    let api = TestApiServer::new();
    let (s, body) = api.post(EPS, &slice("Bad_Name")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"].as_str().unwrap().contains("metadata.name"));
}
