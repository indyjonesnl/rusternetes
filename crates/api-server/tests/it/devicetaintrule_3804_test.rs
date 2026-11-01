//! `resource.k8s.io/v1alpha3` DeviceTaintRule (#3804).
//!
//! Upstream: `pkg/registry/resource/devicetaintrule/strategy.go`
//! (`PrepareForCreate` clears status + `Generation = 1`; `PrepareForUpdate`
//! keeps status and bumps the generation on a spec change; `GetResetFields`
//! main resets `status`, `/status` resets `metadata` + `spec`),
//! `pkg/apis/resource/validation/validation.go` (`ValidateDeviceTaintRule*`),
//! `pkg/apis/resource/v1alpha3/defaults.go` (`SetDefaults_DeviceTaint`).

use axum::http::StatusCode;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const DTR: &str = "/apis/resource.k8s.io/v1alpha3/devicetaintrules";
const APPLY: &str = "application/apply-patch+yaml";

fn rule(name: &str) -> Value {
    json!({"apiVersion": "resource.k8s.io/v1alpha3", "kind": "DeviceTaintRule",
           "metadata": {"name": name},
           "spec": {"deviceSelector": {"driver": "example.com"},
                    "taint": {"key": "example.com/tainted", "effect": "NoExecute"}}})
}

fn with_status(mut b: Value) -> Value {
    b["status"] = json!({"conditions": [{"type": "EvictionInProgress", "status": "False",
        "reason": "Done", "message": "m", "lastTransitionTime": "2026-01-01T00:00:00Z"}]});
    b
}

fn owned(out: &Value, manager: &str) -> Value {
    out["metadata"]["managedFields"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["manager"] == manager))
        .map(|e| e["fieldsV1"].clone())
        .unwrap_or(Value::Null)
}

async fn apply(api: &TestApiServer, uri: &str, manager: &str, b: &Value) -> (StatusCode, Value) {
    api.send(
        "PATCH",
        &format!("{uri}?fieldManager={manager}"),
        Some(APPLY),
        Some(b),
    )
    .await
}

/// Alpha group-version, off by default: the storage is not installed -> 404.
#[tokio::test]
#[serial_test::serial]
async fn not_served_while_the_gate_is_off() {
    let _g = with_feature(Feature::DRADeviceTaintRules, false);
    let api = TestApiServer::new();
    let (code, _) = api.post(DTR, &rule("a")).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    let (code, _) = api.get(DTR).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}

/// The v3 OpenAPI index lists resource.k8s.io/v1alpha3 only while the gate
/// is on, and its document is upstream's (#3811;
/// `api/openapi-spec/v3/apis__resource.k8s.io__v1alpha3_openapi.json`).
#[tokio::test]
#[serial_test::serial]
async fn openapi_v3_publishes_v1alpha3_under_the_gate() {
    let key = "apis/resource.k8s.io/v1alpha3";
    {
        let _g = with_feature(Feature::DRADeviceTaintRules, false);
        let (_, root) = TestApiServer::new().get("/openapi/v3").await;
        assert!(root["paths"].get(key).is_none());
    }
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let (_, root) = api.get("/openapi/v3").await;
    assert_eq!(
        root["paths"][key]["serverRelativeURL"],
        "/openapi/v3/apis/resource.k8s.io/v1alpha3"
    );
    let (code, doc) = api.get("/openapi/v3/apis/resource.k8s.io/v1alpha3").await;
    assert_eq!(code, StatusCode::OK);
    let schema = &doc["components"]["schemas"]["io.k8s.api.resource.v1alpha3.DeviceTaintRule"];
    assert_eq!(
        schema["x-kubernetes-group-version-kind"][0]["kind"],
        "DeviceTaintRule"
    );
}

/// PrepareForCreate: status dropped, generation forced to 1; TimeAdded is
/// defaulted (SetDefaults_DeviceTaint).
#[tokio::test]
#[serial_test::serial]
async fn create_clears_status_sets_generation_defaults_time_added() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let mut b = with_status(rule("a"));
    b["metadata"]["generation"] = json!(42);
    let (code, out) = api.post(DTR, &b).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    assert!(out["status"].get("conditions").is_none(), "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert!(out["spec"]["taint"]["timeAdded"].is_string(), "{out}");
}

/// ValidateDeviceTaintRule through the create endpoint.
#[tokio::test]
#[serial_test::serial]
async fn create_validates() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let mut b = rule("a");
    b["spec"]["taint"]["effect"] = json!("Bogus");
    let (code, out) = api.post(DTR, &b).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(
        out["details"]["causes"][0]["field"], "spec.taint.effect",
        "{out}"
    );

    let mut b = rule("a");
    b["spec"]["taint"]["key"] = json!("");
    let (code, out) = api.post(DTR, &b).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(
        out["details"]["causes"][0]["field"], "spec.taint.key",
        "{out}"
    );
}

/// PrepareForUpdate: stored status kept; a spec change bumps the generation,
/// a metadata-only change does not.
#[tokio::test]
#[serial_test::serial]
async fn update_keeps_status_and_bumps_generation_on_spec_change() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(DTR, &rule("a")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let uri = format!("{DTR}/a");

    let mut put = with_status(out.clone());
    put["metadata"]["labels"] = json!({"x": "y"});
    let (code, out2) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::OK, "{out2}");
    assert!(out2["status"].get("conditions").is_none(), "{out2}");
    assert_eq!(out2["metadata"]["generation"], 1, "{out2}");

    let mut put = out2.clone();
    put["spec"]["taint"]["effect"] = json!("None");
    let (code, out3) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::OK, "{out3}");
    assert_eq!(out3["metadata"]["generation"], 2, "{out3}");
}

/// statusStrategy.PrepareForUpdate: spec and metadata kept; conditions
/// validated and capped at 8.
#[tokio::test]
#[serial_test::serial]
async fn status_update_keeps_spec_and_validates() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(DTR, &rule("a")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let uri = format!("{DTR}/a/status");

    let mut put = with_status(out.clone());
    put["spec"]["taint"]["effect"] = json!("None");
    put["metadata"]["labels"] = json!({"x": "y"});
    let (code, out2) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::OK, "{out2}");
    assert_eq!(out2["spec"]["taint"]["effect"], "NoExecute", "{out2}");
    assert!(out2["metadata"].get("labels").is_none(), "{out2}");
    assert_eq!(
        out2["status"]["conditions"][0]["type"],
        "EvictionInProgress"
    );
    assert_eq!(out2["metadata"]["generation"], 1, "{out2}");

    let conds: Vec<Value> = (0..9)
        .map(|i| {
            json!({"type": format!("T{i}"), "status": "True", "reason": "R",
                   "message": "m", "lastTransitionTime": "2026-01-01T00:00:00Z"})
        })
        .collect();
    let mut put = out2.clone();
    put["status"]["conditions"] = json!(conds);
    let (code, out3) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out3}");
}

/// GetResetFields: the main endpoint does not let an applier own `status`.
#[tokio::test]
#[serial_test::serial]
async fn main_apply_does_not_own_status() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let (code, out) = apply(&api, &format!("{DTR}/b"), "user", &with_status(rule("b"))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let o = owned(&out, "user");
    assert!(o.get("f:spec").is_some(), "{out}");
    assert!(o.get("f:status").is_none(), "{out}");
}

/// GetResetFields: `/status` does not let an applier own `metadata` or `spec`.
#[tokio::test]
#[serial_test::serial]
async fn status_apply_does_not_own_spec_or_metadata() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(DTR, &rule("c")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let (code, out) = apply(
        &api,
        &format!("{DTR}/c/status"),
        "ctrl",
        &with_status(rule("c")),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let o = owned(&out, "ctrl");
    assert!(o.get("f:status").is_some(), "{out}");
    assert!(o.get("f:spec").is_none(), "{out}");
    assert!(o.get("f:metadata").is_none(), "{out}");
}

fn resource_group(groups: &Value) -> Value {
    groups["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["name"] == "resource.k8s.io")
        .unwrap()
        .clone()
}

/// storage_resource.go:117-130 (`v1alpha3Storage`): with the gate off no
/// storage is installed, so the version has no discovery document and the
/// group does not list it.
#[tokio::test]
#[serial_test::serial]
async fn discovery_hides_v1alpha3_while_the_gate_is_off() {
    let _g = with_feature(Feature::DRADeviceTaintRules, false);
    let api = TestApiServer::new();
    let (status, _) = api.get("/apis/resource.k8s.io/v1alpha3").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, groups) = api.get("/apis").await;
    let g = resource_group(&groups);
    assert_eq!(g["versions"].as_array().unwrap().len(), 1, "{g}");
}

/// With the gate on, v1alpha3 is advertised next to v1 (v1 stays preferred)
/// and lists devicetaintrules + devicetaintrules/status, cluster-scoped.
#[tokio::test]
#[serial_test::serial]
async fn discovery_serves_v1alpha3_when_the_gate_is_on() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let (_, groups) = api.get("/apis").await;
    let g = resource_group(&groups);
    let versions: Vec<&str> = g["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["version"].as_str().unwrap())
        .collect();
    assert_eq!(versions, vec!["v1", "v1alpha3"], "{g}");
    assert_eq!(g["preferredVersion"]["version"], "v1");

    let (status, list) = api.get("/apis/resource.k8s.io/v1alpha3").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let res = list["resources"].as_array().unwrap();
    let names: Vec<&str> = res.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["devicetaintrules", "devicetaintrules/status"]);
    assert_eq!(res[0]["kind"], "DeviceTaintRule");
    assert_eq!(res[0]["namespaced"], false);
    assert_eq!(res[1]["verbs"], json!(["get", "patch", "update"]));

    let (status, group) = api.get("/apis/resource.k8s.io/").await;
    assert_eq!(status, StatusCode::OK, "{group}");
    assert_eq!(group["versions"].as_array().unwrap().len(), 2, "{group}");
}
