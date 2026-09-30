//! DeviceClass and ResourceSlice on the generic Store (#1990): the strategy
//! rules of `pkg/registry/resource/{deviceclass,resourceslice}/strategy.go`.

use axum::http::StatusCode;
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CLASSES: &str = "/apis/resource.k8s.io/v1/deviceclasses";
const SLICES: &str = "/apis/resource.k8s.io/v1/resourceslices";

fn class(name: &str) -> Value {
    json!({"apiVersion": "resource.k8s.io/v1", "kind": "DeviceClass",
           "metadata": {"name": name}, "spec": {}})
}

fn slice(name: &str, device: Value) -> Value {
    json!({"apiVersion": "resource.k8s.io/v1", "kind": "ResourceSlice",
           "metadata": {"name": name},
           "spec": {"driver": "dra.example.com",
                    "pool": {"name": "pool-1", "generation": 1, "resourceSliceCount": 1},
                    "allNodes": true, "devices": [device]}})
}

fn taint_device() -> Value {
    json!({"name": "gpu-0", "taints": [{"key": "example.com/t", "effect": "NoSchedule"}]})
}

/// `Generation = 1` on create; a spec change increments it and a metadata
/// change does not (deviceclass/strategy.go:52, :78-81).
#[tokio::test]
async fn a_deviceclass_generation_follows_its_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CLASSES, &class("c1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["metadata"]["generation"], 1, "{created}");

    let mut labelled = created.clone();
    labelled["metadata"]["labels"] = json!({"a": "b"});
    let (s, body) = api.put(&format!("{CLASSES}/c1"), &labelled).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");

    let mut changed = body.clone();
    changed["spec"]["selectors"] = json!([{"cel": {"expression": "true"}}]);
    let (s, body) = api.put(&format!("{CLASSES}/c1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 2, "{body}");
}

/// `AllowUnconditionalUpdate() == true` and `AllowCreateOnUpdate() == false`.
#[tokio::test]
async fn a_deviceclass_put_needs_no_resource_version_but_cannot_create() {
    let api = TestApiServer::new();
    let (s, _) = api.post(CLASSES, &class("c1")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api.put(&format!("{CLASSES}/c1"), &class("c1")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, body) = api.put(&format!("{CLASSES}/nope"), &class("nope")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
}

/// `ReturnDeletedObject: true` on both stores.
#[tokio::test]
async fn deletes_return_the_object() {
    let api = TestApiServer::new();
    let (s, _) = api.post(CLASSES, &class("c1")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api.delete(&format!("{CLASSES}/c1")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "DeviceClass", "{body}");

    let (s, _) = api
        .post(SLICES, &slice("s1", json!({"name": "gpu-0"})))
        .await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api.delete(&format!("{SLICES}/s1")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "ResourceSlice", "{body}");
}

/// `dropDisabledDRADeviceTaintsFields` (resourceslice/strategy.go:196-217):
/// while `DRADeviceTaints` is off, taints are cleared on create.
#[tokio::test]
#[serial_test::serial]
async fn taints_are_dropped_while_the_gate_is_off() {
    let api = TestApiServer::new();
    let (s, body) = api.post(SLICES, &slice("s1", taint_device())).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert!(body["spec"]["devices"][0].get("taints").is_none(), "{body}");
}

#[tokio::test]
#[serial_test::serial]
async fn taints_are_kept_while_the_gate_is_on() {
    let _gate = feature_gates::with_feature(Feature::DRADeviceTaints, true);
    let api = TestApiServer::new();
    let (s, body) = api.post(SLICES, &slice("s1", taint_device())).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["spec"]["devices"][0]["taints"][0]["key"],
        "example.com/t"
    );
}

/// A field the old object already uses survives an update with the gate off
/// (`draDeviceTaintsFeatureInUse`), so a downgrade does not strip data.
#[tokio::test]
#[serial_test::serial]
async fn in_use_taints_survive_an_update_with_the_gate_off() {
    let api = TestApiServer::new();
    let created = {
        let _gate = feature_gates::with_feature(Feature::DRADeviceTaints, true);
        let (s, body) = api.post(SLICES, &slice("s1", taint_device())).await;
        assert_eq!(s, StatusCode::CREATED, "{body}");
        body
    };
    let mut update = created.clone();
    update["metadata"]["labels"] = json!({"a": "b"});
    let (s, body) = api.put(&format!("{SLICES}/s1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        body["spec"]["devices"][0]["taints"][0]["key"],
        "example.com/t"
    );
}

/// `Generation = 1` on create and the immutable driver on update.
#[tokio::test]
async fn a_resourceslice_driver_is_immutable() {
    let api = TestApiServer::new();
    let (s, created) = api
        .post(SLICES, &slice("s1", json!({"name": "gpu-0"})))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["metadata"]["generation"], 1, "{created}");

    let mut update = created.clone();
    update["spec"]["driver"] = json!("other.example.com");
    let (s, body) = api.put(&format!("{SLICES}/s1"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"].as_str().unwrap().contains("spec.driver"));
}
