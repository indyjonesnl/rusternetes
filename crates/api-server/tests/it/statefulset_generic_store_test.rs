//! StatefulSet, `statefulsets/status` and `statefulsets/scale` served through
//! the generic Store and endpoint handlers (#1990, #1995).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin StatefulSet's own:
//! `pkg/registry/apps/statefulset/strategy.go`, its `StatusREST` and
//! `ScaleREST` (storage/storage.go), the apps validators, and the storage
//! codec's defaulting that `ValidateStatefulSetUpdate` depends on.

use axum::http::StatusCode;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const STS: &str = "/apis/apps/v1/namespaces/default/statefulsets";

fn sts(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "StatefulSet",
        "metadata": {"name": name},
        "spec": {
            "replicas": 2,
            "serviceName": "svc",
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(STS, &sts(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `PrepareForCreate` (strategy.go:72-81) drops a client-set status and
/// starts the generation at 1.
#[tokio::test]
async fn create_resets_status_and_generation() {
    let api = TestApiServer::new();
    let mut body = sts("s-create");
    body["status"] = json!({"replicas": 7, "updateRevision": "x"});
    body["metadata"]["generation"] = json!(42);
    let (status, out) = api.post(STS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["status"], json!({"replicas": 0}), "{out}");
}

/// `ValidateStatefulSetName` is `NameIsDNSLabel` (validation.go:50-55).
#[tokio::test]
async fn create_rejects_a_name_that_is_not_a_dns_label() {
    let api = TestApiServer::new();
    let (status, out) = api.post(STS, &sts("s.dotted")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("metadata.name"), "{out}");
}

/// `WarningsOnCreate` (strategy.go:136-147): a negative
/// `revisionHistoryLimit` is accepted with a warning.
#[tokio::test]
async fn create_warns_about_a_negative_revision_history_limit() {
    let api = TestApiServer::new();
    let mut body = sts("s-warn");
    body["spec"]["revisionHistoryLimit"] = json!(-1);
    let (status, headers, _, out) = api
        .send_full(
            "POST",
            STS,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(&body).unwrap()),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let warnings: Vec<&str> = headers
        .get_all("warning")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("spec.revisionHistoryLimit: a negative value")),
        "{warnings:?}"
    );
}

/// `PrepareForUpdate` (strategy.go:96-110): status is kept, a spec change
/// bumps the generation, and the api-server does not compute revisions —
/// `status.updateRevision` is the StatefulSet controller's.
#[tokio::test]
async fn update_keeps_status_and_bumps_generation_on_spec() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "s-update").await;
    let uri = format!("{STS}/s-update");

    obj["metadata"]["annotations"] = json!({"a": "1"});
    let (status, out) = api.put(&uri, &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");

    let mut obj = out;
    obj["spec"]["template"]["spec"]["containers"][0]["image"] = json!("nginx:2");
    obj["status"] = json!({"replicas": 9});
    let (status, out) = api.put(&uri, &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
    assert_eq!(out["status"], json!({"replicas": 0}), "{out}");
}

/// `ValidateStatefulSetUpdate` (validation.go:255-269): a field outside the
/// mutable list is Forbidden.
#[tokio::test]
async fn update_forbids_a_service_name_change() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "s-svc").await;
    obj["spec"]["serviceName"] = json!("other");
    let (status, out) = api.put(&format!("{STS}/s-svc"), &obj).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("updates to statefulset spec for fields other than"),
        "{out}"
    );
}

/// The storage codec defaults what it decodes (serializer/versioning/
/// versioning.go:166-188), so an object stored without the defaults —
/// written around the API, or before a default existed — still compares
/// equal to its defaulted update. Without it `ValidateStatefulSetUpdate`
/// sees `podManagementPolicy` and friends "change" and forbids a
/// metadata-only update.
#[tokio::test]
async fn an_undefaulted_stored_object_accepts_a_metadata_update() {
    let api = TestApiServer::new();
    let mut stored = sts("s-raw");
    stored["metadata"]["namespace"] = json!("default");
    let key = build_key("statefulsets", Some("default"), "s-raw");
    let stored: Value = api.storage.create(&key, &stored).await.unwrap();

    let mut update = stored.clone();
    update["metadata"]["annotations"] = json!({"touched": "true"});
    let (status, out) = api.put(&format!("{STS}/s-raw"), &update).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["annotations"]["touched"], "true", "{out}");
    assert_eq!(out["spec"]["podManagementPolicy"], "OrderedReady", "{out}");

    let (status, got) = api.get(&format!("{STS}/s-raw")).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(
        got["spec"]["updateStrategy"]["type"], "RollingUpdate",
        "{got}"
    );
}

/// `dropStatefulSetDisabledFields` (strategy.go:120-126): with
/// `MaxUnavailableStatefulSet` at its 1.35 default (off) a new
/// `maxUnavailable` is dropped.
#[tokio::test]
#[serial_test::serial]
async fn max_unavailable_is_dropped_while_its_gate_is_off() {
    let _gate = rusternetes_common::feature_gates::with_feature(
        rusternetes_common::feature_gates::Feature::MaxUnavailableStatefulSet,
        false,
    );
    let api = TestApiServer::new();
    let mut body = sts("s-maxu");
    body["spec"]["updateStrategy"] =
        json!({"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 2}});
    let (status, out) = api.post(STS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        out["spec"]["updateStrategy"]["rollingUpdate"]
            .get("maxUnavailable")
            .is_none(),
        "{out}"
    );
}

/// `statefulSetStatusStrategy.PrepareForUpdate` (strategy.go:208-213) and
/// `ValidateStatefulSetStatusUpdate` (validation.go:310-322).
#[tokio::test]
async fn status_put_writes_status_keeps_spec_and_validates() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "s-status").await;
    obj["spec"]["replicas"] = json!(9);
    obj["status"] = json!({"replicas": 2, "readyReplicas": 1, "updateRevision": "r1"});
    let uri = format!("{STS}/s-status/status");
    let (status, out) = api.put(&uri, &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["replicas"], 2, "{out}");
    assert_eq!(out["status"]["updateRevision"], "r1", "{out}");

    let (status, out) = api
        .patch(&uri, &json!({"status": {"readyReplicas": 5}}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("cannot be greater than status.replicas"),
        "{out}"
    );
}

/// `scaleFromStatefulSet` (storage.go:260-282) and `ScaleREST.Update`.
#[tokio::test]
async fn scale_reads_and_writes_replicas() {
    let api = TestApiServer::new();
    create(&api, "s-scale").await;
    let uri = format!("{STS}/s-scale/scale");
    let (status, mut scale) = api.get(&uri).await;
    assert_eq!(status, StatusCode::OK, "{scale}");
    assert_eq!(scale["kind"], "Scale", "{scale}");
    assert_eq!(scale["spec"]["replicas"], 2, "{scale}");
    assert_eq!(scale["status"]["selector"], "app=s-scale", "{scale}");

    scale["spec"]["replicas"] = json!(4);
    let (status, out) = api.put(&uri, &scale).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (_, obj) = api.get(&format!("{STS}/s-scale")).await;
    assert_eq!(obj["spec"]["replicas"], 4, "{obj}");
    assert_eq!(obj["metadata"]["generation"], 2, "{obj}");

    let (status, out) = api.get(&format!("{STS}/nope/scale")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
    assert_eq!(out["message"], "statefulsets.apps \"nope\" not found");
}
