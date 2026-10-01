//! CertificateSigningRequest on the generic Store (#1990): the strategy rules
//! of `pkg/registry/certificates/certificates/strategy.go`.

use axum::http::StatusCode;
use base64::Engine;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CSRS: &str = "/apis/certificates.k8s.io/v1/certificatesigningrequests";

fn request_pem() -> String {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["example.com".to_string()]).unwrap();
    let csr = params.serialize_request(&key).unwrap();
    base64::engine::general_purpose::STANDARD.encode(csr.pem().unwrap())
}

fn csr(name: &str) -> Value {
    json!({"apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequest",
           "metadata": {"name": name},
           "spec": {"request": request_pem(), "signerName": "example.com/serving",
                    "usages": ["digital signature", "server auth"]}})
}

/// `PrepareForCreate` replaces whatever user info the client sent with the
/// requester's, and a CSR cannot be created pre-approved (strategy.go:75-100).
#[tokio::test]
async fn create_injects_the_requester_and_clears_status() {
    let api = TestApiServer::new();
    let mut obj = csr("c1");
    obj["spec"]["username"] = json!("evil");
    obj["spec"]["groups"] = json!(["evil-group"]);
    obj["status"] = json!({"conditions": [{"type": "Approved", "status": "True"}]});
    let (s, body) = api.post(CSRS, &obj).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_ne!(body["spec"]["username"], "evil", "{body}");
    assert!(
        !body["spec"]["groups"].to_string().contains("evil-group"),
        "{body}"
    );
    let conditions = body["status"].get("conditions");
    assert!(
        conditions.is_none() || conditions.unwrap() == &json!([]),
        "{body}"
    );
}

/// The user info reaches the strategy through the request context.
#[tokio::test]
async fn create_records_who_asked() {
    let api = TestApiServer::new();
    let (s, body) = api.post(CSRS, &csr("c1")).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert!(
        body["spec"]["username"]
            .as_str()
            .is_some_and(|u| !u.is_empty()),
        "{body}"
    );
}

/// `PrepareForUpdate`: a CSR is immutable after creation except through its
/// subresources (strategy.go:102-110).
#[tokio::test]
async fn a_put_cannot_change_the_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CSRS, &csr("c1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["spec"]["signerName"] = json!("example.com/other");
    update["metadata"]["labels"] = json!({"a": "b"});
    let (s, body) = api.put(&format!("{CSRS}/c1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["signerName"], "example.com/serving", "{body}");
    assert_eq!(body["metadata"]["labels"]["a"], "b", "{body}");
}

/// `AllowCreateOnUpdate() == false`.
#[tokio::test]
async fn a_put_to_a_missing_csr_is_not_found() {
    let api = TestApiServer::new();
    let (s, body) = api.put(&format!("{CSRS}/missing"), &csr("missing")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
}

/// The approval strategy writes the conditions only, and stamps
/// `lastUpdateTime` / `lastTransitionTime` (`populateConditionTimestamps`).
#[tokio::test]
async fn approval_adds_a_condition_with_timestamps() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CSRS, &csr("c1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["spec"]["signerName"] = json!("example.com/other");
    update["status"] = json!({"conditions": [{"type": "Approved", "status": "True",
                                              "reason": "ByTest", "message": "ok"}]});
    let (s, body) = api.put(&format!("{CSRS}/c1/approval"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["type"], "Approved", "{body}");
    assert!(cond["lastUpdateTime"].as_str().is_some(), "{body}");
    assert!(cond["lastTransitionTime"].as_str().is_some(), "{body}");
    assert_eq!(body["spec"]["signerName"], "example.com/serving", "{body}");
}

/// `/status` may not add an Approved condition: `allowSettingApprovalConditions`
/// is only set for `/approval`.
#[tokio::test]
async fn status_cannot_approve() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CSRS, &csr("c1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["status"] = json!({"conditions": [{"type": "Approved", "status": "True"}]});
    let (s, body) = api.put(&format!("{CSRS}/c1/status"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// A PATCH of `/approval` goes through the approval strategy.
#[tokio::test]
async fn approval_patch_adds_a_condition() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CSRS, &csr("c1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let patch = json!({"status": {"conditions": [{"type": "Denied", "status": "True"}]}});
    let (s, body) = api.patch(&format!("{CSRS}/c1/approval"), &patch).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["conditions"][0]["type"], "Denied", "{body}");
    assert!(
        body["status"]["conditions"][0]["lastUpdateTime"]
            .as_str()
            .is_some(),
        "{body}"
    );
}
