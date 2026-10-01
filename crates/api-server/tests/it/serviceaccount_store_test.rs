//! ServiceAccount on the generic Store (#1990): the strategy rules of
//! `pkg/registry/core/serviceaccount/strategy.go`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SAS: &str = "/api/v1/namespaces/default/serviceaccounts";

fn sa(name: &str) -> Value {
    json!({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": name}})
}

/// `cleanSecretReferences` (strategy.go:67-71): a secret reference keeps only
/// its name, on create and on update.
#[tokio::test]
async fn secret_references_keep_only_their_name() {
    let api = TestApiServer::new();
    let mut obj = sa("s1");
    obj["secrets"] = json!([{"name": "a", "namespace": "other", "kind": "Secret", "uid": "u"}]);
    let (s, created) = api.post(SAS, &obj).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["secrets"], json!([{"name": "a"}]), "{created}");

    let mut update = created.clone();
    update["secrets"] = json!([{"name": "b", "namespace": "x", "fieldPath": "f"}]);
    let (s, body) = api.put(&format!("{SAS}/s1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["secrets"], json!([{"name": "b"}]), "{body}");
}

/// `ValidateServiceAccount` runs `NameIsDNSSubdomain` on the name.
#[tokio::test]
async fn an_invalid_name_is_rejected() {
    let api = TestApiServer::new();
    let (s, body) = api.post(SAS, &sa("Bad_Name")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"].as_str().unwrap().contains("metadata.name"));
}

/// `AllowCreateOnUpdate() == false`.
#[tokio::test]
async fn a_put_to_a_missing_serviceaccount_is_not_found() {
    let api = TestApiServer::new();
    let (s, body) = api.put(&format!("{SAS}/missing"), &sa("missing")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
}

/// `AllowUnconditionalUpdate() == true`: a PUT without a resourceVersion works.
#[tokio::test]
async fn a_put_without_resource_version_is_accepted() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SAS, &sa("s1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    update["automountServiceAccountToken"] = json!(false);
    let (s, body) = api.put(&format!("{SAS}/s1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["automountServiceAccountToken"], false, "{body}");
}

/// `ReturnDeletedObject: true` (storage.go:56): a DELETE answers with the
/// deleted ServiceAccount, not a Status.
#[tokio::test]
async fn delete_returns_the_deleted_object() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SAS, &sa("s1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let (s, body) = api.delete(&format!("{SAS}/s1")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "ServiceAccount", "{body}");
    assert_eq!(body["metadata"]["name"], "s1", "{body}");
}

/// The created ServiceAccount gets its token Secret straight away (a
/// Rusternetes shortcut kept across the migration).
#[tokio::test]
async fn create_makes_the_token_secret() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SAS, &sa("s1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let (s, body) = api.get("/api/v1/namespaces/default/secrets/s1-token").await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        body["type"], "kubernetes.io/service-account-token",
        "{body}"
    );
}
