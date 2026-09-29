//! Secret served through the generic Store and endpoint handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin what is Secret's own: the
//! decode-time defaulting and conversion (`SetDefaults_Secret`,
//! `Convert_v1_Secret_To_core_Secret`) and `pkg/registry/core/secret/
//! strategy.go`.

use axum::http::StatusCode;
use base64::Engine;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SECRETS: &str = "/api/v1/namespaces/default/secrets";

fn b64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

fn secret(name: &str) -> Value {
    json!({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name}, "data": {"k": b64("v")}})
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `SetDefaults_Secret` (pkg/apis/core/v1/defaults.go:265-269): a Secret
/// created without a type is stored as `Opaque`.
#[tokio::test]
async fn create_defaults_the_type_to_opaque() {
    let api = TestApiServer::new();
    let (status, out) = api.post(SECRETS, &secret("s-type")).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["type"], "Opaque", "{out}");
    let (_, got) = api.get(&format!("{SECRETS}/s-type")).await;
    assert_eq!(got["type"], "Opaque", "{got}");
}

/// `Convert_v1_Secret_To_core_Secret` (conversion.go:407-423): `stringData`
/// is written over `data` and never stored.
#[tokio::test]
async fn create_writes_string_data_over_data() {
    let api = TestApiServer::new();
    let mut body = secret("s-sd");
    body["data"] = json!({"a": b64("old"), "b": b64("kept")});
    body["stringData"] = json!({"a": "new"});
    let (status, out) = api.post(SECRETS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["data"]["a"], b64("new"), "{out}");
    assert_eq!(out["data"]["b"], b64("kept"), "{out}");
    assert!(out.get("stringData").is_none(), "{out}");
}

/// The patched object is converted too (patch.go:357-363), so a merge patch
/// can set a key through `stringData`.
#[tokio::test]
async fn merge_patch_of_string_data_lands_in_data() {
    let api = TestApiServer::new();
    api.post(SECRETS, &secret("s-patch")).await;
    let (status, out) = api
        .patch(
            &format!("{SECRETS}/s-patch"),
            &json!({"stringData": {"p": "x"}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["data"]["p"], b64("x"), "{out}");
    assert_eq!(out["data"]["k"], b64("v"), "{out}");
    assert!(out.get("stringData").is_none(), "{out}");
}

/// Server-side apply goes through the same conversion.
#[tokio::test]
async fn apply_of_string_data_lands_in_data() {
    let api = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": "s-apply", "namespace": "default"},
        "stringData": {"a": "applied"}
    });
    let (status, out) = api
        .send(
            "PATCH",
            &format!("{SECRETS}/s-apply?fieldManager=test"),
            Some("application/apply-patch+yaml"),
            Some(&body),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["data"]["a"], b64("applied"), "{out}");
    assert_eq!(out["type"], "Opaque", "{out}");
}

/// `ValidateSecretUpdate`'s `ValidateImmutableField(newSecret.Type, …)`.
/// A PUT that omits the type is defaulted to `Opaque` before the strategy
/// sees it, so on a non-Opaque secret it is a type change too.
#[tokio::test]
async fn put_cannot_change_the_type() {
    let api = TestApiServer::new();
    let mut body = secret("s-imm-type");
    body["type"] = json!("kubernetes.io/basic-auth");
    body["data"] = json!({"username": b64("u")});
    let (status, created) = api.post(SECRETS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");

    let uri = format!("{SECRETS}/s-imm-type");
    let mut changed = created.clone();
    changed["type"] = json!("Opaque");
    let (status, out) = api.put(&uri, &changed).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("type"), "{out}");

    let mut omitted = created.clone();
    omitted.as_object_mut().unwrap().remove("type");
    let (status, out) = api.put(&uri, &omitted).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let (status, out) = api.put(&uri, &created).await;
    assert_eq!(status, StatusCode::OK, "{out}");
}

/// `ValidateSecretUpdate`: once `immutable` is set, `data` and `immutable`
/// are forbidden to change.
#[tokio::test]
async fn an_immutable_secret_refuses_data_changes() {
    let api = TestApiServer::new();
    let mut body = secret("s-imm");
    body["immutable"] = json!(true);
    let (status, created) = api.post(SECRETS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");

    let uri = format!("{SECRETS}/s-imm");
    let (status, out) = api.patch(&uri, &json!({"data": {"k": b64("w")}})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("data"), "{out}");

    let (status, out) = api.patch(&uri, &json!({"immutable": false})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let (status, out) = api
        .patch(&uri, &json!({"metadata": {"labels": {"l": "1"}}}))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
}

/// `warningsForSecret` (strategy.go:131-141): a TLS secret whose key does not
/// match its certificate is created, with the `tls.X509KeyPair` error as a
/// warning.
#[tokio::test]
async fn a_tls_secret_with_a_mismatched_pair_is_created_with_a_warning() {
    let api = TestApiServer::new();
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["example.com".to_string()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let other = rcgen::KeyPair::generate().unwrap();
    let body = json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": "s-tls"},
        "type": "kubernetes.io/tls",
        "data": {"tls.crt": b64(&cert.pem()), "tls.key": b64(&other.serialize_pem())}
    });
    let (status, headers, _, out) = api
        .send_full(
            "POST",
            SECRETS,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(&body).unwrap()),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let warnings: Vec<_> = headers
        .get_all("warning")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("tls: private key does not match public key")),
        "{warnings:?}"
    );
}

/// DELETE answers with the success Status naming `secrets` (store.go:1386-1411).
#[tokio::test]
async fn delete_returns_the_success_status() {
    let api = TestApiServer::new();
    api.post(SECRETS, &secret("s-del")).await;
    let (status, out) = api.delete(&format!("{SECRETS}/s-del")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Status", "{out}");
    assert_eq!(out["details"]["kind"], "secrets", "{out}");
    let (status, _) = api.get(&format!("{SECRETS}/s-del")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// DELETECOLLECTION returns the deleted `SecretList` (store.go:1237-1384).
#[tokio::test]
async fn deletecollection_returns_the_deleted_list() {
    let api = TestApiServer::new();
    let mut labelled = secret("s-dc-1");
    labelled["metadata"]["labels"] = json!({"app": "x"});
    api.post(SECRETS, &labelled).await;
    api.post(SECRETS, &secret("s-dc-2")).await;
    let (status, out) = api
        .delete(&format!("{SECRETS}?labelSelector=app%3Dx"))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "SecretList", "{out}");
    assert_eq!(out["items"].as_array().map(Vec::len), Some(1), "{out}");
    let (status, _) = api.get(&format!("{SECRETS}/s-dc-2")).await;
    assert_eq!(status, StatusCode::OK);
}
