//! Regression guard for the cert-manager **cainjector** watch contract against
//! the real rhino/SQLite backend: a label-selected Secret watch must deliver
//! the ADDED event for a matching Secret **created after** the watch was opened
//! from the collection resourceVersion returned by an (empty) labeled LIST.
//!
//! Mirrors the smoke failure shape: cainjector caches Secrets with
//! `app.kubernetes.io/managed-by=cert-manager`, lists (empty) → watches from
//! that RV, and the webhook then creates `cert-manager-webhook-ca` with that
//! label. If the ADDED is not delivered, cainjector never injects the caBundle
//! and every admission call fails with `UnknownIssuer`.
//!
//! NOTE: this passes on `main` — the structural single-process path is sound.
//! The nightly smoke flake is a timing/scale race that only manifests in the
//! multi-container stack (separate api-server ↔ rhino, many concurrent
//! watches). This test pins the contract so a *structural* regression in
//! label-selected watch delivery is caught cheaply; the smoke script's caBundle
//! readiness gate handles the residual timing race operationally.
//!
//! Gated behind `sqlite` — a plain `cargo test` compiles it out.
#![cfg(feature = "sqlite")]

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use futures::StreamExt;
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{RhinoStorage, Storage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tower::ServiceExt;

fn fresh_db_path() -> String {
    let mut p = std::env::temp_dir();
    p.push(format!("rusternetes-watch-ls-{}.db", uuid::Uuid::new_v4()));
    p.to_string_lossy().into_owned()
}

async fn make_sqlite_state() -> (Arc<ApiServerState>, String) {
    let db_path = fresh_db_path();
    let store = RhinoStorage::new(&db_path)
        .await
        .expect("create rhino SQLite backend");
    let backend = Arc::new(StorageBackend::Sqlite(store));
    let state = Arc::new(ApiServerState::new(
        backend,
        Arc::new(TokenManager::new(b"test-secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        true,
    ));
    (state, db_path)
}

async fn send(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let req = if let Some(b) = body {
        req = req.header("content-type", "application/json");
        req.body(Body::from(serde_json::to_vec(b).unwrap()))
            .unwrap()
    } else {
        req.body(Body::empty()).unwrap()
    };
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, v)
}

async fn collect(router: axum::Router, uri: String, max: usize, deadline: Duration) -> Vec<Value> {
    let req = Request::builder()
        .method(Method::GET)
        .uri(&uri)
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut events = Vec::new();
    let run = async {
        while events.len() < max {
            match stream.next().await {
                Some(Ok(bytes)) => {
                    buf.push_str(&String::from_utf8_lossy(&bytes));
                    while let Some(i) = buf.find('\n') {
                        let line = buf[..i].to_string();
                        buf.drain(..=i);
                        if line.trim().is_empty() {
                            continue;
                        }
                        if let Ok(v) = serde_json::from_str::<Value>(&line) {
                            events.push(v);
                            if events.len() >= max {
                                return;
                            }
                        }
                    }
                }
                _ => return,
            }
        }
    };
    let _ = timeout(deadline, run).await;
    events
}

/// cainjector's label selector, URL-encoded (`/` → %2F, `=` → %3D).
const LS: &str = "app.kubernetes.io%2Fmanaged-by%3Dcert-manager";

#[tokio::test]
async fn label_selected_secret_watch_delivers_create_after_list_rv() {
    let (state, db_path) = make_sqlite_state().await;
    let router = build_router(state.clone(), None);

    // Namespace the webhook secret lives in.
    let _ = state
        .storage
        .create(
            &rusternetes_storage::build_key("namespaces", None, "cert-manager"),
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"cert-manager"}}),
        )
        .await;

    // 1. cainjector LISTs labeled secrets cluster-wide (empty) and reads the
    //    collection resourceVersion — exactly what its reflector watches from.
    let (ls_status, list) = send(
        &router,
        Method::GET,
        &format!("/api/v1/secrets?labelSelector={LS}"),
        None,
    )
    .await;
    assert_eq!(
        ls_status,
        StatusCode::OK,
        "labeled list should succeed: {list}"
    );
    let rv = list
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .expect("list must carry a collection resourceVersion")
        .to_string();

    // 2. Open the label-selected watch from that RV (cainjector's reflector).
    let watch_uri = format!("/api/v1/secrets?watch=true&labelSelector={LS}&resourceVersion={rv}");
    let handle = tokio::spawn(collect(
        router.clone(),
        watch_uri,
        1,
        Duration::from_secs(6),
    ));

    // Let the watch establish, then the webhook creates the labeled CA secret.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (cs, created) = send(
        &router,
        Method::POST,
        "/api/v1/namespaces/cert-manager/secrets",
        Some(&json!({
            "apiVersion":"v1","kind":"Secret",
            "metadata":{
                "name":"cert-manager-webhook-ca","namespace":"cert-manager",
                "labels":{"app.kubernetes.io/managed-by":"cert-manager"}
            },
            "data":{"tls.crt":"YQ=="}
        })),
    )
    .await;
    assert!(
        cs.is_success(),
        "secret create should succeed: {cs} {created}"
    );

    // 3. The watch MUST deliver the ADDED for the newly-created labeled secret.
    let events = handle.await.unwrap();
    let saw_added = events.iter().any(|e| {
        e.get("type").and_then(|v| v.as_str()) == Some("ADDED")
            && e.pointer("/object/metadata/name").and_then(|v| v.as_str())
                == Some("cert-manager-webhook-ca")
    });

    let _ = std::fs::remove_file(&db_path);
    assert!(
        saw_added,
        "label-selected watch (from list RV {rv}) did not deliver ADDED for the \
         after-the-fact labeled secret create — this is the cainjector caBundle flake. \
         Envelopes seen: {events:?}"
    );
}
