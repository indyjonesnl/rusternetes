//! Namespace, `namespaces/status` and `namespaces/finalize` served through the
//! generic Store and endpoint handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin Namespace's own:
//! `pkg/registry/core/namespace/strategy.go` and the namespace `REST`
//! (storage/storage.go), whose `Delete` runs the namespace lifecycle.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NSS: &str = "/api/v1/namespaces";

fn ns(name: &str) -> Value {
    json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(NSS, &ns(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

async fn delete_with(api: &TestApiServer, uri: &str, options: &Value) -> (StatusCode, Value) {
    let (status, _, _, out) = api
        .send_full(
            "DELETE",
            uri,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(options).unwrap()),
        )
        .await;
    (status, out)
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `PrepareForCreate` (strategy.go:62-85) sets phase `Active` and appends the
/// `kubernetes` finalizer; `Canonicalize` (:106-131) sets the name label.
#[tokio::test]
async fn create_sets_active_the_finalizer_and_the_name_label() {
    let api = TestApiServer::new();
    let mut body = ns("ns-create");
    body["spec"] = json!({"finalizers": ["example.com/a"]});
    body["status"] = json!({"phase": "Terminating"});
    let (status, out) = api.post(NSS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["status"]["phase"], "Active", "{out}");
    assert_eq!(
        out["spec"]["finalizers"],
        json!(["example.com/a", "kubernetes"]),
        "{out}"
    );
    assert_eq!(
        out["metadata"]["labels"]["kubernetes.io/metadata.name"], "ns-create",
        "{out}"
    );
}

/// `Canonicalize` runs after `generateName` resolves, so a generated name
/// still gets its label.
#[tokio::test]
async fn a_generated_name_gets_its_label() {
    let api = TestApiServer::new();
    let (status, out) = api
        .post(
            NSS,
            &json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"generateName": "gen-"}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let name = out["metadata"]["name"].as_str().unwrap();
    assert!(name.starts_with("gen-"), "{out}");
    assert_eq!(
        out["metadata"]["labels"]["kubernetes.io/metadata.name"], name,
        "{out}"
    );
}

/// Creating a namespace has no api-server side effects: the ServiceAccounts
/// controller and the root-CA publisher own `default` and `kube-root-ca.crt`.
#[tokio::test]
async fn create_has_no_side_effects() {
    let api = TestApiServer::builder()
        .ca_cert_pem("-----BEGIN CERTIFICATE-----\nMA==\n-----END CERTIFICATE-----\n")
        .build();
    create(&api, "ns-quiet").await;
    let (status, out) = api
        .get("/api/v1/namespaces/ns-quiet/serviceaccounts/default")
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
    let (status, out) = api
        .get("/api/v1/namespaces/ns-quiet/configmaps/kube-root-ca.crt")
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// `PrepareForUpdate` (strategy.go:87-93): a PUT keeps `spec.finalizers` and
/// status; `ValidateNamespaceUpdate` checks ObjectMeta.
#[tokio::test]
async fn update_keeps_finalizers_and_status() {
    let api = TestApiServer::new();
    let mut body = create(&api, "ns-update").await;
    body["spec"]["finalizers"] = json!([]);
    body["status"]["phase"] = json!("Terminating");
    body["metadata"]["labels"]["team"] = json!("a");
    let (status, out) = api.put(&format!("{NSS}/ns-update"), &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["finalizers"], json!(["kubernetes"]), "{out}");
    assert_eq!(out["status"]["phase"], "Active", "{out}");
    assert_eq!(out["metadata"]["labels"]["team"], "a", "{out}");
}

/// `ValidateNamespaceStatusUpdate` (validation.go:8202-8215): only a deleted
/// namespace may be `Terminating`.
#[tokio::test]
async fn status_validation_holds_the_phase() {
    let api = TestApiServer::new();
    create(&api, "ns-status").await;
    let (status, out) = api
        .patch(
            &format!("{NSS}/ns-status/status"),
            &json!({"status": {"phase": "Terminating"}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("status.Phase"), "{out}");
}

/// `REST.Delete` (storage/storage.go:170-250): the first DELETE sets the
/// deletion timestamp and `Terminating` and returns the namespace; the
/// namespace stays while `spec.finalizers` remain (:252-255).
#[tokio::test]
async fn delete_starts_termination_and_finalize_removes() {
    let api = TestApiServer::new();
    create(&api, "ns-del").await;
    let uri = format!("{NSS}/ns-del");

    let (status, out) = api.delete(&uri).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Namespace", "{out}");
    assert_eq!(out["status"]["phase"], "Terminating", "{out}");
    assert!(out["metadata"]["deletionTimestamp"].is_string(), "{out}");

    // A second DELETE while spec.finalizers remain returns it unchanged.
    let (status, out) = api.delete(&uri).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["finalizers"], json!(["kubernetes"]), "{out}");

    // `/finalize` drains spec.finalizers; `ShouldDeleteNamespaceDuringUpdate`
    // then removes the namespace.
    let mut body = out;
    body["spec"]["finalizers"] = json!([]);
    let (status, out) = api.put(&format!("{uri}/finalize"), &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (status, out) = api.get(&uri).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// `REST.Delete` (:146-168): a UID precondition that does not match is a
/// Conflict with upstream's message.
#[tokio::test]
async fn delete_checks_the_uid_precondition() {
    let api = TestApiServer::new();
    let obj = create(&api, "ns-uid").await;
    let (status, out) = delete_with(
        &api,
        &format!("{NSS}/ns-uid"),
        &json!({"preconditions": {"uid": "not-the-uid"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{out}");
    let uid = obj["metadata"]["uid"].as_str().unwrap();
    assert!(
        message(&out).contains(&format!(
            "Precondition failed: UID in precondition: not-the-uid, UID in object meta: {uid}"
        )),
        "{out}"
    );
}

/// `shouldHaveDeleteDependentsFinalizer` (storage/storage.go:279-288):
/// foreground propagation adds the `foregroundDeletion` finalizer.
#[tokio::test]
async fn foreground_delete_adds_the_gc_finalizer() {
    let api = TestApiServer::new();
    create(&api, "ns-fg").await;
    let (status, out) = delete_with(
        &api,
        &format!("{NSS}/ns-fg"),
        &json!({"propagationPolicy": "Foreground"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["metadata"]["finalizers"],
        json!(["foregroundDeletion"]),
        "{out}"
    );
}

/// A dry-run DELETE (storage/storage.go:234) changes nothing.
#[tokio::test]
async fn dry_run_delete_changes_nothing() {
    let api = TestApiServer::new();
    create(&api, "ns-dry").await;
    let uri = format!("{NSS}/ns-dry");
    let (status, out) = api.delete(&format!("{uri}?dryRun=All")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["phase"], "Terminating", "{out}");
    let (status, out) = api.get(&uri).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["phase"], "Active", "{out}");
    assert!(out["metadata"].get("deletionTimestamp").is_none(), "{out}");
}

/// The namespace `REST` implements no `rest.CollectionDeleter`, so there is
/// no DELETE on the collection.
#[tokio::test]
async fn there_is_no_delete_collection() {
    let api = TestApiServer::new();
    create(&api, "ns-coll").await;
    let (status, out) = api.delete(NSS).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{out}");
    let (status, out) = api.get(&format!("{NSS}/ns-coll")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
}
