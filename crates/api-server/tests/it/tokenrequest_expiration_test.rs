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
        code == StatusCode::CREATED,
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
    assert!(code == StatusCode::CREATED, "{code} {body}");
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

// --- `--service-account-extend-token-expiration` (#3725) --------------------
//
// `TokenREST.Create` (pkg/registry/core/serviceaccount/storage/token.go:228-237)
// extends a pod-bound token that asks for exactly
// `WarnOnlyBoundTokenExpirationSeconds` (3607) for kube audiences, and
// `token.Claims` (pkg/serviceaccount/claims.go:125-127) records `warnafter`.

const SA_ISSUER: &str = "https://kubernetes.default.svc";

fn extend_state(extend: bool) -> TestApiServer {
    use rusternetes_common::auth::{ServiceAccountOptions, TokenManager};
    let fixture = |n: &str| {
        format!(
            "{}/../common/tests/fixtures/sa/{n}",
            env!("CARGO_MANIFEST_DIR")
        )
    };
    let tm = TokenManager::new(b"extend-secret")
        .with_service_account_options(&ServiceAccountOptions {
            key_files: vec![fixture("rsa.pub")],
            signing_key_file: Some(fixture("rsa-pkcs1.key")),
            issuers: vec![SA_ISSUER.to_string()],
            extend_expiration: extend,
            ..Default::default()
        })
        .unwrap();
    TestApiServer::builder().token_manager(tm).build()
}

async fn make_pod(state: &TestApiServer, name: &str, sa: &str) {
    let (code, pod) = state
        .post(
            &format!("/api/v1/namespaces/{NS}/pods"),
            &json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": name},
                "spec": {"serviceAccountName": sa,
                         "containers": [{"name": "c", "image": "busybox"}]}
            }),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{pod}");
}

fn payload(token: &str) -> Value {
    use base64::Engine;
    let p = token.split('.').nth(1).unwrap();
    serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(p)
            .unwrap(),
    )
    .unwrap()
}

async fn bound_token(
    state: &TestApiServer,
    sa: &str,
    pod: Option<&str>,
    exp: i64,
) -> (Value, Value) {
    let mut req = token_req(exp);
    req["spec"]["audiences"] = json!([SA_ISSUER]);
    if let Some(p) = pod {
        req["spec"]["boundObjectRef"] = json!({"apiVersion": "v1", "kind": "Pod", "name": p});
    }
    let (code, body) = state.post(&token_uri(sa), &req).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    let claims = payload(body["status"]["token"].as_str().unwrap());
    (claims, body)
}

#[tokio::test]
async fn pod_bound_warn_only_token_is_extended_3725() {
    let state = extend_state(true);
    make_sa(&state, "sa-ext").await;
    make_pod(&state, "pod-ext", "sa-ext").await;
    let now = chrono::Utc::now().timestamp();
    let (c, body) = bound_token(&state, "sa-ext", Some("pod-ext"), 3607).await;
    assert!(
        (c["exp"].as_i64().unwrap() - now - 31_536_000).abs() <= 5,
        "{c}"
    );
    let warn = c["kubernetes.io"]["warnafter"].as_i64().unwrap();
    assert!((warn - now - 3607).abs() <= 5, "{c}");
    // status.expirationTimestamp follows the extended expiry (token.go:244).
    let ts = chrono::DateTime::parse_from_rfc3339(
        body["status"]["expirationTimestamp"].as_str().unwrap(),
    )
    .unwrap()
    .timestamp();
    assert_eq!(ts, c["exp"].as_i64().unwrap());
}

#[tokio::test]
async fn extension_does_not_apply_without_its_preconditions_3725() {
    let state = extend_state(true);
    make_sa(&state, "sa-no").await;
    make_pod(&state, "pod-no", "sa-no").await;
    let now = chrono::Utc::now().timestamp();
    // not pod-bound
    let (c, _) = bound_token(&state, "sa-no", None, 3607).await;
    assert!(c["kubernetes.io"].get("warnafter").is_none(), "{c}");
    assert!((c["exp"].as_i64().unwrap() - now - 3607).abs() <= 5);
    // pod-bound, other expiration
    let (c, _) = bound_token(&state, "sa-no", Some("pod-no"), 3608).await;
    assert!(c["kubernetes.io"].get("warnafter").is_none(), "{c}");
    assert!((c["exp"].as_i64().unwrap() - now - 3608).abs() <= 5);
    // non-kube audience
    let mut req = token_req(3607);
    req["spec"]["audiences"] = json!(["https://other.example"]);
    req["spec"]["boundObjectRef"] = json!({"apiVersion": "v1", "kind": "Pod", "name": "pod-no"});
    let (_, body) = state.post(&token_uri("sa-no"), &req).await;
    let c = payload(body["status"]["token"].as_str().unwrap());
    assert!(c["kubernetes.io"].get("warnafter").is_none(), "{c}");
}

#[tokio::test]
async fn extension_disabled_by_flag_3725() {
    let state = extend_state(false);
    make_sa(&state, "sa-off").await;
    make_pod(&state, "pod-off", "sa-off").await;
    let now = chrono::Utc::now().timestamp();
    let (c, _) = bound_token(&state, "sa-off", Some("pod-off"), 3607).await;
    assert!(c["kubernetes.io"].get("warnafter").is_none(), "{c}");
    assert!((c["exp"].as_i64().unwrap() - now - 3607).abs() <= 5);
}
