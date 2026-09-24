//! Deployment and `deployments/status` served through the generic Store and
//! endpoint handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin Deployment's own:
//! `pkg/registry/apps/deployment/strategy.go`, its `StatusREST`
//! (storage/storage.go), `SetDefaults_Deployment` and the apps validators.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const DEPLOYS: &str = "/apis/apps/v1/namespaces/default/deployments";

fn deploy(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name},
        "spec": {
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(DEPLOYS, &deploy(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `PrepareForCreate` (strategy.go:72-79) drops a client-set status and
/// starts the generation at 1; `SetDefaults_Deployment` (apps/v1/defaults.go:
/// 38-73) fills the spec.
#[tokio::test]
async fn create_resets_status_and_defaults_the_spec() {
    let api = TestApiServer::new();
    let mut body = deploy("d-create");
    body["status"] = json!({"replicas": 7, "observedGeneration": 9});
    body["metadata"]["generation"] = json!(42);
    let (status, out) = api.post(DEPLOYS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["status"], json!({}), "{out}");
    let spec = &out["spec"];
    assert_eq!(spec["replicas"], 1, "{out}");
    assert_eq!(spec["strategy"]["type"], "RollingUpdate", "{out}");
    assert_eq!(
        spec["strategy"]["rollingUpdate"]["maxSurge"], "25%",
        "{out}"
    );
    assert_eq!(
        spec["strategy"]["rollingUpdate"]["maxUnavailable"], "25%",
        "{out}"
    );
    assert_eq!(spec["revisionHistoryLimit"], 10, "{out}");
    assert_eq!(spec["progressDeadlineSeconds"], 600, "{out}");
}

/// `WarningsOnCreate` (strategy.go:88-96): a name that is a DNS subdomain
/// but not a DNS label is accepted with a warning.
#[tokio::test]
async fn create_warns_about_a_name_that_is_not_a_dns_label() {
    let api = TestApiServer::new();
    let body = deploy("d.dotted");
    let (status, headers, _, out) = api
        .send_full(
            "POST",
            DEPLOYS,
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
            .any(|w| w.contains("a DNS label is recommended")),
        "{warnings:?}"
    );
}

/// `PrepareForUpdate` (strategy.go:108-123): PUT cannot write status, and
/// the generation moves on a spec or annotation change only.
#[tokio::test]
async fn put_keeps_status_and_bumps_generation_on_spec_or_annotations() {
    let api = TestApiServer::new();
    let created = create(&api, "d-put").await;
    let uri = format!("{DEPLOYS}/d-put");

    let mut labelled = created.clone();
    labelled["metadata"]["labels"] = json!({"l": "1"});
    labelled["status"] = json!({"replicas": 5});
    let (status, out) = api.put(&uri, &labelled).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["metadata"]["generation"], 1,
        "labels do not bump: {out}"
    );
    assert_eq!(out["status"], json!({}), "PUT cannot write status: {out}");

    let mut scaled = out.clone();
    scaled["spec"]["replicas"] = json!(3);
    let (status, out) = api.put(&uri, &scaled).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");

    let mut annotated = out.clone();
    annotated["metadata"]["annotations"] = json!({"a": "1"});
    let (status, out) = api.put(&uri, &annotated).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 3, "{out}");
}

/// PATCH reaches `Store.Update` like PUT, so the strategy validates it —
/// the bespoke patch path ran no Deployment validation at all.
#[tokio::test]
async fn patch_is_validated_by_the_strategy() {
    let api = TestApiServer::new();
    create(&api, "d-patch").await;
    let uri = format!("{DEPLOYS}/d-patch");

    let (status, out) = api.patch(&uri, &json!({"spec": {"replicas": -1}})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.replicas"), "{out}");

    // ValidateDeploymentUpdate: the selector is immutable.
    let (status, out) = api
        .patch(
            &uri,
            &json!({"spec": {
                "selector": {"matchLabels": {"app": "other"}},
                "template": {"metadata": {"labels": {"app": "other"}}}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.selector"), "{out}");
    assert!(message(&out).contains("field is immutable"), "{out}");

    let (status, out) = api.patch(&uri, &json!({"spec": {"replicas": 2}})).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["replicas"], 2, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
}

/// Server-side apply creates a missing Deployment through the same strategy:
/// defaulted, generation 1.
#[tokio::test]
async fn apply_creates_a_defaulted_deployment() {
    let api = TestApiServer::new();
    let (status, out) = api
        .send(
            "PATCH",
            &format!("{DEPLOYS}/d-apply?fieldManager=test"),
            Some("application/apply-patch+yaml"),
            Some(&deploy("d-apply")),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["spec"]["revisionHistoryLimit"], 10, "{out}");
    assert!(
        out["metadata"]["annotations"]
            .get("kubectl.kubernetes.io/last-applied-configuration")
            .is_none(),
        "server-side apply does not write kubectl's annotation: {out}"
    );
}

/// `deploymentStatusStrategy.PrepareForUpdate` (strategy.go:162-169):
/// `/status` writes status only — spec and labels come from the live object —
/// and does not move the generation.
#[tokio::test]
async fn status_put_writes_only_status() {
    let api = TestApiServer::new();
    let created = create(&api, "d-status").await;
    let uri = format!("{DEPLOYS}/d-status/status");

    let mut body = created.clone();
    body["spec"]["replicas"] = json!(9);
    body["metadata"]["labels"] = json!({"l": "1"});
    body["metadata"]["annotations"] = json!({"a": "1"});
    body["status"] = json!({"replicas": 1, "updatedReplicas": 1, "observedGeneration": 1});
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["replicas"], 1, "{out}");
    assert!(out["metadata"].get("labels").is_none(), "{out}");
    assert_eq!(out["metadata"]["annotations"]["a"], "1", "{out}");
    assert_eq!(out["status"]["replicas"], 1, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");

    // The client's resourceVersion is a precondition here too.
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::CONFLICT, "stale rv: {out}");
}

/// `ValidateDeploymentStatusUpdate` (apps validation.go:718-730).
#[tokio::test]
async fn status_updates_are_validated() {
    let api = TestApiServer::new();
    create(&api, "d-status-bad").await;
    let uri = format!("{DEPLOYS}/d-status-bad/status");
    let (status, out) = api
        .patch(
            &uri,
            &json!({"status": {"replicas": 1, "readyReplicas": 2}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("status.readyReplicas"), "{out}");

    let (status, out) = api
        .patch(
            &uri,
            &json!({"status": {"replicas": 2, "readyReplicas": 1}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["readyReplicas"], 1, "{out}");
}

/// `StatusREST.Update` passes `forceAllowCreate = false` (storage.go:
/// 148-153): not even apply creates through `/status`.
#[tokio::test]
async fn status_never_creates() {
    let api = TestApiServer::new();
    let uri = format!("{DEPLOYS}/d-missing/status");
    let (status, out) = api
        .send(
            "PATCH",
            &format!("{uri}?fieldManager=test"),
            Some("application/apply-patch+yaml"),
            Some(&deploy("d-missing")),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
    let (status, out) = api.put(&uri, &deploy("d-missing")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// GET `/status` is the store's Get: the whole object.
#[tokio::test]
async fn status_get_returns_the_object() {
    let api = TestApiServer::new();
    create(&api, "d-status-get").await;
    let (status, out) = api.get(&format!("{DEPLOYS}/d-status-get/status")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Deployment", "{out}");
    assert_eq!(out["spec"]["replicas"], 1, "{out}");
}
