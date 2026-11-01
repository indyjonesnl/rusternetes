//! `audit.LogImpersonatedUser` (`pkg/audit/request.go:84-91`,
//! `context.go:135-156`, called from `impersonation.go:170`): the event's
//! user is the real caller and `impersonatedUser` the target, from the
//! Metadata level up.

use axum::{body::Body, routing::get, Router};
use rusternetes_api_server::audit::{with_audit, AuditConfig, Policy};
use rusternetes_common::audit::{AuditBackend, AuditEvent, AuditStage};
use rusternetes_common::auth::UserInfo;
use rusternetes_middleware::{AuthContext, AuthImpersonator};
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

fn user(name: &str, groups: &[&str]) -> UserInfo {
    UserInfo {
        username: name.into(),
        uid: format!("uid-{name}"),
        groups: groups.iter().map(|g| g.to_string()).collect(),
        extra: Default::default(),
    }
}

async fn run(rules: &str, impersonate: bool) -> Vec<AuditEvent> {
    let cap = Arc::new(Capture(Default::default()));
    let cfg = Arc::new(AuditConfig {
        policy: Policy::from_yaml(&format!(
            "apiVersion: audit.k8s.io/v1\nkind: Policy\nrules:\n{rules}"
        ))
        .unwrap(),
        sink: cap.clone(),
    });
    let app = Router::new()
        .route(
            "/api/v1/namespaces/ns/configmaps/cm",
            get(|| async { "ok" }),
        )
        .layer(axum::middleware::from_fn(move |req, next| {
            let cfg = cfg.clone();
            async move { with_audit(cfg, req, next).await }
        }))
        // Authentication (and its impersonation step) runs before audit here.
        .layer(axum::middleware::from_fn(
            move |mut req: axum::extract::Request, next: axum::middleware::Next| async move {
                if impersonate {
                    req.extensions_mut().insert(AuthContext {
                        user: user("target", &["g1"]),
                    });
                    req.extensions_mut()
                        .insert(AuthImpersonator(user("alice", &["system:authenticated"])));
                } else {
                    req.extensions_mut().insert(AuthContext {
                        user: user("alice", &[]),
                    });
                }
                next.run(req).await
            },
        ));
    app.oneshot(
        axum::http::Request::get("/api/v1/namespaces/ns/configmaps/cm")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    let ev = cap.0.lock().await;
    ev.clone()
}

#[tokio::test]
async fn impersonated_request_records_real_caller_and_target() {
    let ev = run("- level: Metadata\n", true).await;
    assert_eq!(ev.len(), 2);
    // RequestReceived is emitted before the impersonation filter runs.
    assert_eq!(ev[0].stage, AuditStage::RequestReceived);
    assert!(ev[0].impersonated_user.is_none());
    let done = &ev[1];
    assert_eq!(done.user.username, "alice");
    let imp = done.impersonated_user.as_ref().unwrap();
    assert_eq!(imp.username, "target");
    assert_eq!(imp.uid, "uid-target");
    assert_eq!(imp.groups, vec!["g1".to_string()]);
}

#[tokio::test]
async fn policy_sees_the_real_caller() {
    // The audit filter precedes impersonation: a rule on the real user matches.
    let ev = run("- level: Metadata\n  users: [alice]\n- level: None\n", true).await;
    assert!(!ev.is_empty());
    let ev = run(
        "- level: Metadata\n  users: [target]\n- level: None\n",
        true,
    )
    .await;
    assert!(ev.is_empty());
}

#[tokio::test]
async fn plain_request_has_no_impersonated_user() {
    let ev = run("- level: Metadata\n", false).await;
    assert!(ev.iter().all(|e| e.impersonated_user.is_none()));
    assert_eq!(ev[1].user.username, "alice");
}
