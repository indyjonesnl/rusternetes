//! TokenRequest.spec.expirationSeconds bounds (upstream ValidateTokenRequest):
//! must be >= 10 minutes (600s) and <= 2^32 seconds.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn sa_uri() -> String {
    format!("/api/v1/namespaces/{NS}/serviceaccounts")
}
fn token_uri(sa: &str) -> String {
    format!("{}/{sa}/token", sa_uri())
}

fn token_req(expiration: i64) -> Value {
    json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenRequest",
        "spec": {"expirationSeconds": expiration}
    })
}

async fn make_sa(state: &TestApiServer, name: &str) {
    let (code, _) = state
        .post(
            &sa_uri(),
            &json!({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": name}}),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "SA create must succeed");
}

#[tokio::test]
async fn token_expiration_below_minimum_rejected() {
    let state = TestApiServer::new();
    make_sa(&state, "sa-a").await;
    let (code, body) = state.post(&token_uri("sa-a"), &token_req(60)).await;
    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "expirationSeconds < 600 must be rejected: {body}"
    );
}

#[tokio::test]
async fn token_expiration_valid_accepted() {
    let state = TestApiServer::new();
    make_sa(&state, "sa-b").await;
    let (code, body) = state.post(&token_uri("sa-b"), &token_req(3600)).await;
    assert!(
        code == StatusCode::OK || code == StatusCode::CREATED,
        "valid expirationSeconds must succeed: {code} {body}"
    );
    assert!(
        body["status"]["token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "token must be issued: {body}"
    );
}

/// `errors.NewInvalid(gvk.GroupKind(), "", errs)`
/// (`pkg/registry/core/serviceaccount/storage/token.go:143`): the message is
/// `TokenRequest.authentication.k8s.io "" is invalid: ...` (#2392).
#[tokio::test]
async fn token_expiration_invalid_is_new_invalid_shaped() {
    let state = TestApiServer::new();
    make_sa(&state, "sa-c").await;
    let (_, body) = state.post(&token_uri("sa-c"), &token_req(60)).await;
    let msg = body["message"].as_str().unwrap_or_default();
    assert!(
        msg.starts_with("TokenRequest.authentication.k8s.io \"\" is invalid: "),
        "{body}"
    );
    assert_eq!(body["details"]["kind"], json!("TokenRequest"), "{body}");
    assert_eq!(
        body["details"]["group"],
        json!("authentication.k8s.io"),
        "{body}"
    );
    let c = &body["details"]["causes"][0];
    assert_eq!(c["field"], json!("spec.expirationSeconds"), "{body}");
    assert!(
        !c["message"]
            .as_str()
            .unwrap_or_default()
            .starts_with("spec.expirationSeconds: "),
        "{body}"
    );
}

/// `--service-account-max-token-expiration` (#2714): `TokenREST.Create`
/// shortens a longer request to the max
/// (`pkg/registry/core/serviceaccount/storage/token.go:222-226`) and the
/// issued token's `exp` and the status timestamp follow the shortened value.
#[tokio::test]
async fn token_expiration_clamped_to_max_2714() {
    use rusternetes_common::auth::{ServiceAccountOptions, TokenManager};
    let tm = TokenManager::new(b"max-exp-secret")
        .with_service_account_options(&ServiceAccountOptions {
            max_expiration: Some(std::time::Duration::from_secs(7200)),
            ..Default::default()
        })
        .unwrap();
    let state = TestApiServer::builder().token_manager(tm).build();
    make_sa(&state, "sa-max").await;
    let before = chrono::Utc::now().timestamp();
    let (code, body) = state.post(&token_uri("sa-max"), &token_req(86_400)).await;
    assert!(
        code == StatusCode::OK || code == StatusCode::CREATED,
        "{code} {body}"
    );
    let ts = chrono::DateTime::parse_from_rfc3339(
        body["status"]["expirationTimestamp"].as_str().unwrap(),
    )
    .unwrap()
    .timestamp();
    assert!(
        (ts - before - 7200).abs() <= 5,
        "expiration must be clamped to 7200s, got {}s: {body}",
        ts - before
    );
}
