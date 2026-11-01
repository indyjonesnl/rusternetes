//! #4285: the review endpoints (TokenReview, SelfSubjectReview,
//! SubjectAccessReview and friends) are served by hand-written create
//! handlers, so they must call `audit.LogRequestObject` themselves, as
//! `endpoints/handlers/create.go:161` does for them upstream.

use rusternetes_common::audit::{AuditBackend, AuditEvent};
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
async fn review_requests_record_their_request_object() {
    let cap = Arc::new(Capture(Default::default()));
    rusternetes_api_server::audit::install_audit(rusternetes_api_server::audit::AuditConfig {
        policy: rusternetes_api_server::audit::Policy::from_yaml(
            "apiVersion: audit.k8s.io/v1\nkind: Policy\nrules:\n- level: Request\n",
        )
        .unwrap(),
        sink: cap.clone(),
    });
    let api = TestApiServer::new();

    let cases = [
        (
            "/apis/authentication.k8s.io/v1/tokenreviews",
            json!({"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
                   "spec": {"token": "not-a-token"}}),
            "tokenreviews",
        ),
        (
            "/apis/authentication.k8s.io/v1/selfsubjectreviews",
            json!({"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview"}),
            "selfsubjectreviews",
        ),
        (
            "/apis/authorization.k8s.io/v1/subjectaccessreviews",
            json!({"apiVersion": "authorization.k8s.io/v1", "kind": "SubjectAccessReview",
                   "spec": {"user": "u", "resourceAttributes": {"verb": "get", "resource": "pods"}}}),
            "subjectaccessreviews",
        ),
        (
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            json!({"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
                   "spec": {"resourceAttributes": {"verb": "get", "resource": "pods"}}}),
            "selfsubjectaccessreviews",
        ),
        (
            "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
            json!({"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectRulesReview",
                   "spec": {"namespace": "default"}}),
            "selfsubjectrulesreviews",
        ),
        (
            "/apis/authorization.k8s.io/v1/namespaces/default/localsubjectaccessreviews",
            json!({"apiVersion": "authorization.k8s.io/v1", "kind": "LocalSubjectAccessReview",
                   "metadata": {"namespace": "default"},
                   "spec": {"user": "u", "resourceAttributes": {"namespace": "default", "verb": "get", "resource": "pods"}}}),
            "localsubjectaccessreviews",
        ),
    ];
    for (path, body, resource) in cases {
        let (status, resp) = api.post(path, &body).await;
        assert!(status.is_success(), "{path}: {status} {resp}");
        let events = cap.0.lock().await;
        let ev = events
            .iter()
            .rev()
            .find(|e| {
                e.request_uri == path
                    && e.stage == rusternetes_common::audit::AuditStage::ResponseComplete
            })
            .unwrap_or_else(|| panic!("{path}: no ResponseComplete event"));
        let ro = ev
            .request_object
            .as_ref()
            .unwrap_or_else(|| panic!("{path}: no request object"));
        assert_eq!(ro["kind"], body["kind"], "{path}");
        let r = ev.object_ref.as_ref().unwrap();
        assert_eq!(r.resource.as_deref(), Some(resource), "{path}");
    }
}
