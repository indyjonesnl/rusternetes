//! Transitioning a Service from `ExternalName` to `ClusterIP` via a full PUT
//! that leaves `spec.externalName` populated must succeed. Upstream validates
//! `externalName` only for the ExternalName type (validation.go
//! `validateService`) and keeps the stale value, warning that it is ignored
//! (`GetWarningsForService`, pkg/api/service/warnings.go:74-76).
//!
//! Reproduces `[sig-network] DNS should provide DNS for ExternalName services`
//! (dns.go:406), which flips the Service type with a GET-modify-PUT that does
//! not clear externalName, and previously hit
//! `spec.externalName: Forbidden: may not be set for non-ExternalName services`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

#[tokio::test]
async fn externalname_to_clusterip_full_put_clears_externalname() {
    let api = TestApiServer::new();
    let ns = "/api/v1/namespaces/default/services";

    // 1. Create an ExternalName service.
    let (cs, _created) = api
        .post(
            ns,
            &json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "extn", "namespace": "default"},
                "spec": {"type": "ExternalName", "externalName": "foo.example.com"}
            }),
        )
        .await;
    assert_eq!(
        cs,
        StatusCode::CREATED,
        "ExternalName create should succeed"
    );

    // 2. Flip to ClusterIP via a full PUT that STILL carries externalName
    //    (what the DNS e2e does). Must NOT be rejected.
    let (us, updated) = api
        .put(
            "/api/v1/namespaces/default/services/extn",
            &json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "extn", "namespace": "default"},
                "spec": {
                    "type": "ClusterIP",
                    "externalName": "foo.example.com",
                    "ports": [{"port": 80}]
                }
            }),
        )
        .await;

    assert_eq!(
        us,
        StatusCode::OK,
        "ExternalName->ClusterIP must be accepted (externalName dropped), got {}: {}",
        us,
        updated
    );
    assert_eq!(
        updated.pointer("/spec/type").and_then(|v| v.as_str()),
        Some("ClusterIP")
    );
    assert_eq!(
        updated
            .pointer("/spec/externalName")
            .and_then(|v| v.as_str()),
        Some("foo.example.com"),
        "upstream keeps an ignored externalName; got {updated}"
    );
    let cip = updated
        .pointer("/spec/clusterIP")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !cip.is_empty() && cip != "None",
        "a ClusterIP should have been allocated on the transition; got {cip:?}"
    );
}
