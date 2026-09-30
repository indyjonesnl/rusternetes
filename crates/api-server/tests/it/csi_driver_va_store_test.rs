//! CSIDriver and VolumeAttachment on the generic Store (#1990): the strategy
//! rules of `pkg/registry/storage/{csidriver,volumeattachment}/strategy.go`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const DRIVERS: &str = "/apis/storage.k8s.io/v1/csidrivers";
const VAS: &str = "/apis/storage.k8s.io/v1/volumeattachments";

fn driver(name: &str) -> Value {
    json!({"apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
           "metadata": {"name": name}, "spec": {}})
}

fn va(name: &str) -> Value {
    json!({"apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
           "metadata": {"name": name},
           "spec": {"attacher": "csi.example.com", "nodeName": "node-1",
                    "source": {"persistentVolumeName": "pv-1"}}})
}

/// `SetDefaults_CSIDriver` (pkg/apis/storage/v1/defaults.go:43-71) runs on
/// decode.
#[tokio::test]
async fn a_csidriver_is_defaulted_on_create() {
    let api = TestApiServer::new();
    let (s, body) = api.post(DRIVERS, &driver("d1.example.com")).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let spec = &body["spec"];
    assert_eq!(spec["attachRequired"], true, "{body}");
    assert_eq!(spec["podInfoOnMount"], false);
    assert_eq!(spec["storageCapacity"], false);
    assert_eq!(spec["fsGroupPolicy"], "ReadWriteOnceWithFSType");
    assert_eq!(spec["volumeLifecycleModes"], json!(["Persistent"]));
    assert_eq!(spec["requiresRepublish"], false);
    assert_eq!(spec["seLinuxMount"], false);
}

/// `PrepareForUpdate` bumps the generation on a spec change
/// (csidriver/strategy.go:114-117), and only then.
#[tokio::test]
async fn a_spec_change_bumps_the_csidriver_generation() {
    let api = TestApiServer::new();
    let (s, created) = api.post(DRIVERS, &driver("d1.example.com")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let g0 = created["metadata"]["generation"].as_i64().unwrap_or(0);

    let mut same = created.clone();
    same["metadata"]["labels"] = json!({"a": "b"});
    let (s, body) = api.put(&format!("{DRIVERS}/d1.example.com"), &same).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"].as_i64().unwrap_or(0), g0);

    let mut changed = body.clone();
    changed["spec"]["podInfoOnMount"] = json!(true);
    let (s, body) = api
        .put(&format!("{DRIVERS}/d1.example.com"), &changed)
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"].as_i64().unwrap_or(0), g0 + 1);
}

/// `ValidateCSIDriverUpdate`: `attachRequired` is immutable and the error
/// names upstream's `spec.attachedRequired` with the submitted value.
#[tokio::test]
async fn attach_required_is_immutable_with_its_value() {
    let api = TestApiServer::new();
    let (s, created) = api.post(DRIVERS, &driver("d1.example.com")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["spec"]["attachRequired"] = json!(false);
    let (s, body) = api.put(&format!("{DRIVERS}/d1.example.com"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let msg = body["message"].as_str().unwrap();
    assert!(
        msg.contains("spec.attachedRequired: Invalid value: false: field is immutable"),
        "{msg}"
    );
}

/// `AllowUnconditionalUpdate() == false` (store.go:727-733).
#[tokio::test]
async fn a_csidriver_put_without_a_resource_version_is_rejected() {
    let api = TestApiServer::new();
    let (s, _) = api.post(DRIVERS, &driver("d1.example.com")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api
        .put(
            &format!("{DRIVERS}/d1.example.com"),
            &driver("d1.example.com"),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// `ReturnDeletedObject: true` on both stores.
#[tokio::test]
async fn deletes_return_the_object() {
    let api = TestApiServer::new();
    let (s, _) = api.post(DRIVERS, &driver("d1.example.com")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api.delete(&format!("{DRIVERS}/d1.example.com")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "CSIDriver", "{body}");

    let (s, _) = api.post(VAS, &va("va1")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api.delete(&format!("{VAS}/va1")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "VolumeAttachment", "{body}");
}

/// `PrepareForCreate` clears the status (volumeattachment/strategy.go:65-68);
/// the Go status is a value, so it always serializes.
#[tokio::test]
async fn a_client_cannot_set_the_status_on_create() {
    let api = TestApiServer::new();
    let mut obj = va("va1");
    obj["status"] = json!({"attached": true});
    let (s, body) = api.post(VAS, &obj).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["status"], json!({"attached": false}), "{body}");
}

/// The main resource keeps the stored status on update; `/status` writes
/// only the status and keeps the spec (volumeattachment/strategy.go:97-104,
/// :146-165).
#[tokio::test]
async fn status_is_written_only_through_the_status_subresource() {
    let api = TestApiServer::new();
    let (s, created) = api.post(VAS, &va("va1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["status"] = json!({"attached": true});
    let (s, body) = api.put(&format!("{VAS}/va1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["attached"], false, "{body}");

    let mut update = body.clone();
    update["status"] = json!({"attached": true, "attachmentMetadata": {"k": "v"}});
    update["spec"]["nodeName"] = json!("node-2");
    let (s, body) = api.put(&format!("{VAS}/va1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["attached"], true, "{body}");
    assert_eq!(body["spec"]["nodeName"], "node-1", "spec kept: {body}");
}

/// `ValidateVolumeAttachmentUpdate`: the spec is read-only and the bad value
/// is the submitted spec.
#[tokio::test]
async fn a_spec_change_is_rejected_with_the_spec_as_bad_value() {
    let api = TestApiServer::new();
    let (s, created) = api.post(VAS, &va("va1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["spec"]["nodeName"] = json!("node-2");
    let (s, body) = api.put(&format!("{VAS}/va1"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let msg = body["message"].as_str().unwrap();
    assert!(msg.contains("spec: Invalid value:"), "{msg}");
    assert!(msg.contains("node-2"), "value, not a placeholder: {msg}");
}

/// `ValidateVolumeAttachmentV1` runs on create only: a valid attacher passes,
/// an invalid CSI driver name is rejected.
#[tokio::test]
async fn the_attacher_must_be_a_csi_driver_name_on_create() {
    let api = TestApiServer::new();
    let mut obj = va("va1");
    obj["spec"]["attacher"] = json!("Bad Attacher!");
    let (s, body) = api.post(VAS, &obj).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"].as_str().unwrap().contains("spec.attacher"));
}
