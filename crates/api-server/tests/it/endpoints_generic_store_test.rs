//! Endpoints served through the generic Store and endpoint handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin Endpoints' own:
//! `pkg/registry/core/endpoint/strategy.go` and `ValidateEndpointsCreate` /
//! `ValidateEndpointsUpdate` (pkg/apis/core/validation/validation.go:8229-8261).

use axum::http::StatusCode;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const EP: &str = "/api/v1/namespaces/default/endpoints";

fn endpoints(name: &str, ip: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Endpoints",
        "metadata": {"name": name},
        "subsets": [{"addresses": [{"ip": ip}], "ports": [{"port": 80, "protocol": "TCP"}]}]
    })
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

async fn post_with_warnings(api: &TestApiServer, body: &Value) -> (StatusCode, Vec<String>, Value) {
    let (status, headers, _, out) = api
        .send_full(
            "POST",
            EP,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(body).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_string))
        .collect();
    (status, warnings, out)
}

/// `ValidateEndpointsUpdate` holds a PATCH to the subset rules. The old PATCH
/// handler validated nothing.
#[tokio::test]
async fn a_patch_is_validated() {
    let api = TestApiServer::new();
    let (status, out) = api.post(EP, &endpoints("e-patch", "10.0.0.1")).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let (status, out) = api
        .patch(
            &format!("{EP}/e-patch"),
            &json!({"subsets": [{"addresses": [{"ip": "127.0.0.1"}]}]}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("subsets[0].addresses[0].ip"),
        "{out}"
    );
}

/// validation.go:8233-8243: Endpoints stored with subsets that are invalid
/// today still accept an update that leaves the subsets alone.
#[tokio::test]
async fn unchanged_invalid_subsets_do_not_block_an_update() {
    let api = TestApiServer::new();
    let mut stored = endpoints("e-legacy", "127.0.0.1");
    stored["metadata"]["namespace"] = json!("default");
    let key = build_key("endpoints", Some("default"), "e-legacy");
    api.storage.create::<Value>(&key, &stored).await.unwrap();

    let (status, out) = api
        .patch(
            &format!("{EP}/e-legacy"),
            &json!({"metadata": {"labels": {"touched": "yes"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["labels"]["touched"], "yes", "{out}");
}

/// `endpointsWarnings` (strategy.go:88-106): a non-canonical IPv6 address is
/// accepted with a warning, unless the endpoints controller manages the
/// object.
#[tokio::test]
async fn non_canonical_ips_warn() {
    let api = TestApiServer::new();
    let (status, warnings, out) =
        post_with_warnings(&api, &endpoints("e-warn", "2001:db8:0:0::2")).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("subsets[0].addresses[0].ip: IPv6 address")
                && w.contains("should be in RFC 5952 canonical format")),
        "{warnings:?}"
    );

    let mut managed = endpoints("e-managed", "2001:db8:0:0::2");
    managed["metadata"]["labels"] =
        json!({"endpoints.kubernetes.io/managed-by": "endpoint-controller"});
    let (status, warnings, out) = post_with_warnings(&api, &managed).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        !warnings.iter().any(|w| w.contains("RFC 5952")),
        "{warnings:?}"
    );
}

/// `AllowCreateOnUpdate` is true (strategy.go:70-73).
#[tokio::test]
async fn a_put_creates_endpoints() {
    let api = TestApiServer::new();
    let (status, out) = api
        .put(&format!("{EP}/e-put"), &endpoints("e-put", "10.0.0.2"))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
}
