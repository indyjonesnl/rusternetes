//! Legacy secret-based service-account tokens (#2644).
//!
//! Ported from `pkg/serviceaccount/legacy.go` (`legacyValidator.Validate`,
//! `patchSecretWithLastUsedDate`) and `TestTokenGenerateAndValidate` in
//! `pkg/serviceaccount/jwt_test.go` (the `valid lookup`, `invalid secret
//! lookup`, `invalid serviceaccount lookup`, `secret is marked as invalid`
//! and issuer cases).

use base64::Engine;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SECRET: &[u8] = b"legacy-sa-token-test";
/// `serviceaccount.LegacyIssuer` (legacy.go:56).
const LEGACY_ISSUER: &str = "kubernetes/serviceaccount";
const LAST_USED: &str = "kubernetes.io/legacy-token-last-used";
const INVALID_SINCE: &str = "kubernetes.io/legacy-token-invalid-since";

fn server() -> TestApiServer {
    TestApiServer::builder()
        .secret(SECRET)
        .rbac()
        .skip_auth(false)
        .build()
}

/// `serviceaccount.LegacyClaims` + `GenerateToken`: no `exp`, no `aud`.
fn mint(iss: &str, overrides: Value) -> String {
    let mut c = json!({
        "iss": iss,
        "sub": "system:serviceaccount:ns:sa",
        "kubernetes.io/serviceaccount/namespace": "ns",
        "kubernetes.io/serviceaccount/service-account.name": "sa",
        "kubernetes.io/serviceaccount/service-account.uid": "sa-uid",
        "kubernetes.io/serviceaccount/secret.name": "tok",
    });
    for (k, v) in overrides.as_object().unwrap() {
        if v.is_null() {
            c.as_object_mut().unwrap().remove(k);
        } else {
            c[k] = v.clone();
        }
    }
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &c,
        &jsonwebtoken::EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

fn legacy_token() -> String {
    mint(LEGACY_ISSUER, json!({}))
}

async fn seed_sa(s: &TestApiServer, uid: &str, secrets: &[&str]) {
    let refs: Vec<Value> = secrets.iter().map(|n| json!({"name": n})).collect();
    s.storage
        .create(
            &build_key("serviceaccounts", Some("ns"), "sa"),
            &json!({"apiVersion": "v1", "kind": "ServiceAccount",
                    "metadata": {"name": "sa", "namespace": "ns", "uid": uid},
                    "secrets": refs}),
        )
        .await
        .unwrap();
}

async fn seed_secret(s: &TestApiServer, token: &str, labels: Value, extra_meta: Value) {
    let mut meta = json!({"name": "tok", "namespace": "ns", "uid": "sec-uid", "labels": labels});
    for (k, v) in extra_meta.as_object().unwrap() {
        meta[k] = v.clone();
    }
    s.storage
        .create(
            &build_key("secrets", Some("ns"), "tok"),
            &json!({"apiVersion": "v1", "kind": "Secret",
                    "type": "kubernetes.io/service-account-token",
                    "metadata": meta,
                    "data": {"token": base64::engine::general_purpose::STANDARD.encode(token)}}),
        )
        .await
        .unwrap();
}

/// 401 = the token was rejected; anything else (403 from RBAC) = it
/// authenticated.
async fn status_with(s: &TestApiServer, token: &str) -> u16 {
    let auth = format!("Bearer {token}");
    let (st, _h, _b, _v) = s
        .send_with_headers(
            "GET",
            "/api/v1/namespaces/ns/configmaps",
            &[("authorization", &auth)],
            None,
        )
        .await;
    st.as_u16()
}

async fn secret_json(s: &TestApiServer) -> Value {
    s.storage
        .get::<Value>(&build_key("secrets", Some("ns"), "tok"))
        .await
        .unwrap()
}

fn today() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

fn tomorrow() -> String {
    (chrono::Utc::now() + chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string()
}

#[tokio::test]
async fn valid_lookup_authenticates_and_stamps_last_used() {
    let s = server();
    let token = legacy_token();
    seed_sa(&s, "sa-uid", &["tok"]).await;
    seed_secret(&s, &token, json!({}), json!({})).await;
    assert_ne!(status_with(&s, &token).await, 401);
    // legacy.go:185-198
    assert_eq!(
        secret_json(&s).await["metadata"]["labels"][LAST_USED],
        today()
    );
}

#[tokio::test]
async fn manual_secret_not_listed_on_the_sa_also_authenticates() {
    let s = server();
    let token = legacy_token();
    seed_sa(&s, "sa-uid", &[]).await;
    seed_secret(&s, &token, json!({}), json!({})).await;
    assert_ne!(status_with(&s, &token).await, 401);
    assert_eq!(
        secret_json(&s).await["metadata"]["labels"][LAST_USED],
        today()
    );
}

#[tokio::test]
async fn last_used_is_not_repatched_when_today_or_tomorrow() {
    for fresh in [today(), tomorrow()] {
        let s = server();
        let token = legacy_token();
        seed_sa(&s, "sa-uid", &["tok"]).await;
        seed_secret(&s, &token, json!({ LAST_USED: fresh }), json!({})).await;
        let before = secret_json(&s).await["metadata"]["resourceVersion"].clone();
        assert_ne!(status_with(&s, &token).await, 401);
        let after = secret_json(&s).await;
        assert_eq!(after["metadata"]["labels"][LAST_USED], fresh);
        assert_eq!(after["metadata"]["resourceVersion"], before);
    }
}

#[tokio::test]
async fn stale_last_used_is_refreshed() {
    let s = server();
    let token = legacy_token();
    seed_sa(&s, "sa-uid", &["tok"]).await;
    seed_secret(&s, &token, json!({ LAST_USED: "2020-01-01" }), json!({})).await;
    assert_ne!(status_with(&s, &token).await, 401);
    assert_eq!(
        secret_json(&s).await["metadata"]["labels"][LAST_USED],
        today()
    );
}

#[tokio::test]
async fn missing_secret_invalidates_the_token() {
    let s = server();
    seed_sa(&s, "sa-uid", &["tok"]).await;
    assert_eq!(status_with(&s, &legacy_token()).await, 401);
}

#[tokio::test]
async fn missing_service_account_invalidates_the_token() {
    let s = server();
    let token = legacy_token();
    seed_secret(&s, &token, json!({}), json!({})).await;
    assert_eq!(status_with(&s, &token).await, 401);
}

#[tokio::test]
async fn service_account_uid_mismatch_invalidates_the_token() {
    let s = server();
    let token = legacy_token();
    seed_sa(&s, "recreated-uid", &["tok"]).await;
    seed_secret(&s, &token, json!({}), json!({})).await;
    assert_eq!(status_with(&s, &token).await, 401);
}

#[tokio::test]
async fn token_not_matching_the_secrets_copy_is_rejected() {
    let s = server();
    seed_sa(&s, "sa-uid", &["tok"]).await;
    seed_secret(&s, "some-other-token", json!({}), json!({})).await;
    assert_eq!(status_with(&s, &legacy_token()).await, 401);
}

#[tokio::test]
async fn secret_awaiting_deletion_invalidates_the_token() {
    let s = server();
    let token = legacy_token();
    seed_sa(&s, "sa-uid", &["tok"]).await;
    // legacy.go:104: any DeletionTimestamp, no leeway
    seed_secret(
        &s,
        &token,
        json!({}),
        json!({"deletionTimestamp": chrono::Utc::now().to_rfc3339(), "finalizers": ["x"]}),
    )
    .await;
    assert_eq!(status_with(&s, &token).await, 401);
}

#[tokio::test]
async fn invalid_since_label_rejects_but_still_stamps_last_used() {
    let s = server();
    let token = legacy_token();
    seed_sa(&s, "sa-uid", &["tok"]).await;
    seed_secret(
        &s,
        &token,
        json!({ INVALID_SINCE: "2022-12-20" }),
        json!({}),
    )
    .await;
    // legacy.go:149-154
    assert_eq!(status_with(&s, &token).await, 401);
    assert_eq!(
        secret_json(&s).await["metadata"]["labels"][LAST_USED],
        today()
    );

    // removing the label temporarily allows the token again
    let mut sec = secret_json(&s).await;
    sec["metadata"]["labels"]
        .as_object_mut()
        .unwrap()
        .remove(INVALID_SINCE);
    s.storage
        .update(&build_key("secrets", Some("ns"), "tok"), &sec)
        .await
        .unwrap();
    assert_ne!(status_with(&s, &token).await, 401);
}

#[tokio::test]
async fn unrecognized_issuer_is_rejected() {
    let s = server();
    let token = mint("foo", json!({}));
    seed_sa(&s, "sa-uid", &["tok"]).await;
    seed_secret(&s, &token, json!({}), json!({})).await;
    assert_eq!(status_with(&s, &token).await, 401);
}

#[tokio::test]
async fn missing_claims_are_rejected() {
    // legacy.go:76-97
    for claim in [
        "sub",
        "kubernetes.io/serviceaccount/namespace",
        "kubernetes.io/serviceaccount/secret.name",
        "kubernetes.io/serviceaccount/service-account.name",
        "kubernetes.io/serviceaccount/service-account.uid",
    ] {
        let s = server();
        let token = mint(LEGACY_ISSUER, json!({ claim: null }));
        seed_sa(&s, "sa-uid", &["tok"]).await;
        seed_secret(&s, &token, json!({}), json!({})).await;
        assert_eq!(status_with(&s, &token).await, 401, "missing {claim}");
    }
}

#[tokio::test]
async fn sub_claim_must_match_namespace_and_name() {
    // legacy.go:99-102
    let s = server();
    let token = mint(
        LEGACY_ISSUER,
        json!({"sub": "system:serviceaccount:ns:other"}),
    );
    seed_sa(&s, "sa-uid", &["tok"]).await;
    seed_secret(&s, &token, json!({}), json!({})).await;
    assert_eq!(status_with(&s, &token).await, 401);
}
