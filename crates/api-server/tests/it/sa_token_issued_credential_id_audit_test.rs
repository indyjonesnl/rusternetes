//! Issuing a ServiceAccount token audits the identifier of the credential
//! (`registry/core/serviceaccount/storage/token.go:254-256`,
//! `IssuedCredentialIDAuditAnnotationKey` at `serviceaccount/util.go:38`).

use rusternetes_common::audit::{AuditBackend, AuditEvent, AuditStage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;
use std::sync::Arc;

struct Capture(tokio::sync::Mutex<Vec<AuditEvent>>);

#[async_trait::async_trait]
impl AuditBackend for Capture {
    async fn log(&self, e: AuditEvent) -> Result<(), String> {
        self.0.lock().await.push(e);
        Ok(())
    }
    async fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
async fn token_request_audits_the_issued_credential_id() {
    use base64::Engine;
    let cap = Arc::new(Capture(Default::default()));
    rusternetes_api_server::audit::install_audit(rusternetes_api_server::audit::AuditConfig {
        policy: rusternetes_api_server::audit::Policy::from_yaml(
            "apiVersion: audit.k8s.io/v1\nkind: Policy\nrules:\n- level: Metadata\n",
        )
        .unwrap(),
        sink: cap.clone(),
    });
    let api = TestApiServer::new();
    let (status, created) = api
        .post(
            "/api/v1/namespaces/default/serviceaccounts",
            &json!({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "sa"}}),
        )
        .await;
    assert_eq!(status.as_u16(), 201, "{created}");

    let path = "/api/v1/namespaces/default/serviceaccounts/sa/token";
    let (status, body) = api
        .post(
            path,
            &json!({"apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest",
                    "metadata": {}, "spec": {"audiences": ["x"]}}),
        )
        .await;
    assert_eq!(status.as_u16(), 201, "{body}");
    let token = body["status"]["token"].as_str().unwrap();
    let payload: serde_json::Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token.split('.').nth(1).unwrap())
            .unwrap(),
    )
    .unwrap();
    let jti = payload["jti"].as_str().expect("jti claim");

    let events = cap.0.lock().await;
    let ev = events
        .iter()
        .find(|e| e.request_uri == path && e.stage == AuditStage::ResponseComplete)
        .expect("ResponseComplete event");
    assert_eq!(
        ev.annotations
            .as_ref()
            .and_then(|a| a.get("authentication.kubernetes.io/issued-credential-id"))
            .map(String::as_str),
        Some(format!("JTI={jti}").as_str())
    );
}
