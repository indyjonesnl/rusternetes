//! `audit.LogRequestObject` / `LogRequestPatch`
//! (`staging/src/k8s.io/apiserver/pkg/audit/request.go:93-168`) through the
//! audit filter: level gating, ObjectRef completion, managedFields omission.

use axum::{body::Body, routing::post, Router};
use rusternetes_api_server::audit::{
    log_request_object, log_request_patch, with_audit, AuditConfig, Policy,
};
use rusternetes_common::audit::{AuditBackend, AuditEvent};
use std::sync::Arc;
use tower::ServiceExt;

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

fn obj() -> serde_json::Value {
    serde_json::json!({
        "kind": "ConfigMap", "apiVersion": "v1",
        "metadata": {
            "name": "cm", "namespace": "ns", "uid": "u1", "resourceVersion": "7",
            "managedFields": [{"manager": "m"}]
        }
    })
}

async fn run(policy_rules: &str, uri: &str, patch: bool) -> AuditEvent {
    let cap = Arc::new(Capture(Default::default()));
    let cfg = Arc::new(AuditConfig {
        policy: Policy::from_yaml(&format!(
            "apiVersion: audit.k8s.io/v1\nkind: Policy\nomitStages: [RequestReceived]\nrules:\n{policy_rules}"
        ))
        .unwrap(),
        sink: cap.clone(),
    });
    let app = Router::new()
        .route(
            "/api/v1/namespaces/ns/configmaps",
            post(move || async move {
                if patch {
                    log_request_patch(br#"{"data":{"a":"b"}}"#);
                } else {
                    log_request_object(&obj(), ("", "v1", "configmaps"), None);
                }
                "ok"
            }),
        )
        .layer(axum::middleware::from_fn(move |req, next| {
            let cfg = cfg.clone();
            async move { with_audit(cfg, req, next).await }
        }));
    app.oneshot(axum::http::Request::post(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let ev = cap.0.lock().await;
    ev.last().unwrap().clone()
}

const URI: &str = "/api/v1/namespaces/ns/configmaps";

#[tokio::test]
async fn request_level_records_object_and_completes_object_ref() {
    let ev = run("- level: Request\n", URI, false).await;
    let ro = ev.request_object.expect("request object");
    assert_eq!(ro["metadata"]["name"], "cm");
    let r = ev.object_ref.unwrap();
    assert_eq!(r.name.as_deref(), Some("cm"));
    assert_eq!(r.uid.as_deref(), Some("u1"));
    assert_eq!(r.resource_version.as_deref(), Some("7"));
    assert_eq!(r.namespace.as_deref(), Some("ns"));
}

#[tokio::test]
async fn metadata_level_completes_object_ref_without_the_object() {
    let ev = run("- level: Metadata\n", URI, false).await;
    assert!(ev.request_object.is_none());
    assert_eq!(ev.object_ref.unwrap().uid.as_deref(), Some("u1"));
}

#[tokio::test]
async fn omit_managed_fields_strips_them_from_the_request_object() {
    let ev = run("- level: Request\n  omitManagedFields: true\n", URI, false).await;
    assert!(ev.request_object.unwrap()["metadata"]
        .get("managedFields")
        .is_none());
    let ev = run("- level: Request\n", URI, false).await;
    assert!(ev.request_object.unwrap()["metadata"]
        .get("managedFields")
        .is_some());
}

#[tokio::test]
async fn patch_is_recorded_at_request_level_only() {
    let ev = run("- level: Request\n", URI, true).await;
    assert_eq!(ev.request_object.unwrap()["data"]["a"], "b");
    let ev = run("- level: Metadata\n", URI, true).await;
    assert!(ev.request_object.is_none());
}
