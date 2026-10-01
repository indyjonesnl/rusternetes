//! FlowSchema and PriorityLevelConfiguration on the generic Store (#1990): the
//! strategy rules of `pkg/registry/flowcontrol/*/strategy.go` and the v1
//! defaults of `pkg/apis/flowcontrol/v1/defaults.go`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const FS: &str = "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas";
const PLC: &str = "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations";

fn fs(name: &str) -> Value {
    json!({"apiVersion": "flowcontrol.apiserver.k8s.io/v1", "kind": "FlowSchema",
           "metadata": {"name": name},
           "spec": {"priorityLevelConfiguration": {"name": "global-default"},
                    "rules": [{"subjects": [{"kind": "Group", "group": {"name": "system:authenticated"}}],
                               "nonResourceRules": [{"verbs": ["*"], "nonResourceURLs": ["*"]}]}]}})
}

fn plc(name: &str) -> Value {
    json!({"apiVersion": "flowcontrol.apiserver.k8s.io/v1", "kind": "PriorityLevelConfiguration",
           "metadata": {"name": name},
           "spec": {"type": "Limited",
                    "limited": {"limitResponse": {"type": "Queue", "queuing": {}}}}})
}

/// `SetDefaults_FlowSchemaSpec`: matchingPrecedence defaults to 1000;
/// `PrepareForCreate` sets generation 1.
#[tokio::test]
async fn a_flowschema_is_defaulted_and_gets_generation_one() {
    let api = TestApiServer::new();
    let (s, body) = api.post(FS, &fs("f1")).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["matchingPrecedence"], 1000, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
}

/// A spec change bumps the generation, a metadata-only change does not, and a
/// PUT cannot write the status.
#[tokio::test]
async fn a_spec_change_bumps_the_flowschema_generation() {
    let api = TestApiServer::new();
    let (s, created) = api.post(FS, &fs("f1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut labelled = created.clone();
    labelled["metadata"]["labels"] = json!({"a": "b"});
    labelled["status"] = json!({"conditions": [{"type": "Dangling", "status": "True"}]});
    let (s, body) = api.put(&format!("{FS}/f1"), &labelled).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
    assert!(body["status"].get("conditions").is_none(), "{body}");

    let mut changed = body.clone();
    changed["spec"]["matchingPrecedence"] = json!(500);
    let (s, body) = api.put(&format!("{FS}/f1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 2, "{body}");
}

/// `AllowCreateOnUpdate() == false`.
#[tokio::test]
async fn a_put_to_a_missing_flowschema_is_not_found() {
    let api = TestApiServer::new();
    let (s, body) = api.put(&format!("{FS}/missing"), &fs("missing")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
}

/// `AllowUnconditionalUpdate() == true`: a PUT without a resourceVersion works.
#[tokio::test]
async fn a_put_without_resource_version_is_accepted() {
    let api = TestApiServer::new();
    let (s, created) = api.post(FS, &fs("f1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    update["spec"]["matchingPrecedence"] = json!(200);
    let (s, body) = api.put(&format!("{FS}/f1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
}

/// `flowSchemaStatusStrategy`: the status is written, the spec is kept, and an
/// empty condition `type` is rejected (`ValidateFlowSchemaStatusUpdate`).
#[tokio::test]
async fn flowschema_status_writes_status_and_keeps_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(FS, &fs("f1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["spec"]["matchingPrecedence"] = json!(5);
    update["status"] = json!({"conditions": [{"type": "Dangling", "status": "False"}]});
    let (s, body) = api.put(&format!("{FS}/f1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        body["status"]["conditions"][0]["type"], "Dangling",
        "{body}"
    );
    assert_eq!(body["spec"]["matchingPrecedence"], 1000, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");

    let mut bad = body.clone();
    bad["status"] = json!({"conditions": [{"status": "False"}]});
    let (s, body) = api.put(&format!("{FS}/f1/status"), &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// A PATCH of `/status` goes through the status strategy.
#[tokio::test]
async fn flowschema_status_patch_keeps_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(FS, &fs("f1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let patch = json!({"spec": {"matchingPrecedence": 5},
                       "status": {"conditions": [{"type": "Dangling", "status": "True"}]}});
    let (s, body) = api.patch(&format!("{FS}/f1/status"), &patch).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["conditions"][0]["status"], "True", "{body}");
    assert_eq!(body["spec"]["matchingPrecedence"], 1000, "{body}");
}

/// The v1 defaults for a limited level: shares 30, lendablePercent 0 and the
/// queuing 64/8/50.
#[tokio::test]
async fn a_limited_level_is_defaulted() {
    let api = TestApiServer::new();
    let (s, body) = api.post(PLC, &plc("p1")).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let limited = &body["spec"]["limited"];
    assert_eq!(limited["nominalConcurrencyShares"], 30, "{body}");
    assert_eq!(limited["lendablePercent"], 0, "{body}");
    let q = &limited["limitResponse"]["queuing"];
    assert_eq!(q["queues"], 64, "{body}");
    assert_eq!(q["handSize"], 8);
    assert_eq!(q["queueLengthLimit"], 50);
    assert_eq!(body["metadata"]["generation"], 1);
}

/// `ValidatePriorityLevelConfiguration` forbids the v1beta3 roundtrip
/// annotation (validation.go:350-355).
#[tokio::test]
async fn the_roundtrip_annotation_is_forbidden() {
    let api = TestApiServer::new();
    let mut obj = plc("p1");
    obj["metadata"]["annotations"] =
        json!({"flowcontrol.k8s.io/v1beta3-preserve-zero-concurrency-shares": "true"});
    let (s, body) = api.post(PLC, &obj).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("metadata.annotations"),
        "{body}"
    );
}

/// The status strategy for a priority level keeps the spec; a spec change via
/// the main resource bumps the generation.
#[tokio::test]
async fn plc_status_keeps_spec_and_spec_change_bumps_generation() {
    let api = TestApiServer::new();
    let (s, created) = api.post(PLC, &plc("p1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["spec"]["limited"]["nominalConcurrencyShares"] = json!(99);
    update["status"] = json!({"conditions": [{"type": "Ready", "status": "True"}]});
    let (s, body) = api.put(&format!("{PLC}/p1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["conditions"][0]["type"], "Ready", "{body}");
    assert_eq!(body["spec"]["limited"]["nominalConcurrencyShares"], 30);

    let mut changed = body.clone();
    changed["spec"]["limited"]["nominalConcurrencyShares"] = json!(99);
    let (s, body) = api.put(&format!("{PLC}/p1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 2, "{body}");
    assert_eq!(body["status"]["conditions"][0]["type"], "Ready", "{body}");
}
