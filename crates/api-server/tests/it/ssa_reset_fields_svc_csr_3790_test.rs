//! Server-side apply honours `GetResetFields` for Service and
//! CertificateSigningRequest (#3790, Refs #2226).
//!
//! Upstream: `pkg/registry/core/service/strategy.go:60,145` (main resets
//! `status`, `/status` resets `spec`) and
//! `pkg/registry/certificates/certificates/strategy.go:56,158,269` (main
//! resets `spec` + `status`; `/status` resets `spec` + `status.conditions`;
//! `/approval` resets `spec` + `status.certificate`).

use axum::http::StatusCode;
use base64::Engine;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const APPLY: &str = "application/apply-patch+yaml";
const CSRS: &str = "/apis/certificates.k8s.io/v1/certificatesigningrequests";
const SVCS: &str = "/api/v1/namespaces/default/services";

fn owned(out: &Value, manager: &str) -> Value {
    out["metadata"]["managedFields"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["manager"] == manager))
        .map(|e| e["fieldsV1"].clone())
        .unwrap_or(Value::Null)
}

async fn apply(api: &TestApiServer, uri: &str, manager: &str, b: &Value) -> (StatusCode, Value) {
    api.send(
        "PATCH",
        &format!("{uri}?fieldManager={manager}"),
        Some(APPLY),
        Some(b),
    )
    .await
}

fn svc(name: &str, with_status: bool) -> Value {
    let mut b = json!({"apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"type": "LoadBalancer", "selector": {"a": "b"}, "ports": [{"port": 80, "protocol": "TCP"}]}});
    if with_status {
        b["status"] = json!({"loadBalancer": {"ingress": [{"ip": "1.2.3.4"}]}});
    }
    b
}

#[tokio::test]
async fn service_main_does_not_own_status() {
    let api = TestApiServer::new();
    let (code, out) = apply(&api, &format!("{SVCS}/s1"), "user", &svc("s1", true)).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let o = owned(&out, "user");
    assert!(o.get("f:spec").is_some(), "{out}");
    assert!(o.get("f:status").is_none(), "{out}");
}

#[tokio::test]
async fn service_status_does_not_own_spec() {
    let api = TestApiServer::new();
    let (code, out) = api.post(SVCS, &svc("s2", false)).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let (code, out) = apply(&api, &format!("{SVCS}/s2/status"), "ctrl", &svc("s2", true)).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let o = owned(&out, "ctrl");
    assert!(o.get("f:status").is_some(), "{out}");
    assert!(o.get("f:spec").is_none(), "{out}");
}

fn csr_body(name: &str) -> Value {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["example.com".to_string()]).unwrap();
    let req = params.serialize_request(&key).unwrap();
    let pem = base64::engine::general_purpose::STANDARD.encode(req.pem().unwrap());
    json!({"apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequest",
        "metadata": {"name": name},
        "spec": {"request": pem, "signerName": "example.com/serving",
                 "usages": ["digital signature", "server auth"]}})
}

#[tokio::test]
async fn csr_main_does_not_own_spec() {
    let api = TestApiServer::new();
    let (code, out) = apply(&api, &format!("{CSRS}/c1"), "user", &csr_body("c1")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let o = owned(&out, "user");
    assert!(o.get("f:spec").is_none(), "{out}");
}

#[tokio::test]
async fn csr_status_does_not_own_spec_or_conditions() {
    let api = TestApiServer::new();
    let (code, out) = api.post(CSRS, &csr_body("c2")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let mut b = csr_body("c2");
    b["status"] = json!({"conditions": [{"type": "Failed", "status": "True",
        "reason": "r", "message": "m"}]});
    let (code, out) = apply(&api, &format!("{CSRS}/c2/status"), "ctrl", &b).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let o = owned(&out, "ctrl");
    assert!(o.get("f:spec").is_none(), "{out}");
    assert!(o["f:status"].get("f:conditions").is_none(), "{out}");
}

#[tokio::test]
async fn csr_approval_does_not_own_spec() {
    let api = TestApiServer::new();
    let (code, out) = api.post(CSRS, &csr_body("c3")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let mut b = csr_body("c3");
    b["status"] = json!({"conditions": [{"type": "Approved", "status": "True",
        "reason": "r", "message": "m"}]});
    let (code, out) = apply(&api, &format!("{CSRS}/c3/approval"), "approver", &b).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let o = owned(&out, "approver");
    assert!(o.get("f:spec").is_none(), "{out}");
    assert!(o["f:status"].get("f:conditions").is_some(), "{out}");
}
