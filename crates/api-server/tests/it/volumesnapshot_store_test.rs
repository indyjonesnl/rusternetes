//! VolumeSnapshot, VolumeSnapshotClass and VolumeSnapshotContent on the
//! generic Store (#1990).
//!
//! `snapshot.storage.k8s.io` is not in `../kubernetes`: the types are CRDs of
//! kubernetes-csi/external-snapshotter. The strategy rules are therefore the
//! ones the apiextensions-apiserver applies to any custom resource
//! (`pkg/registry/customresource/{strategy,status_strategy}.go`) for the
//! subresources the CRDs declare (`status` on VolumeSnapshot and
//! VolumeSnapshotContent, none on VolumeSnapshotClass), plus the validation
//! webhook's default-class rule (`pkg/validation-webhook/snapshot.go`,
//! `decideSnapshotClassV1`, release-6.3).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SNAPSHOTS: &str = "/apis/snapshot.storage.k8s.io/v1/namespaces/default/volumesnapshots";
const CONTENTS: &str = "/apis/snapshot.storage.k8s.io/v1/volumesnapshotcontents";
const CLASSES: &str = "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses";
const DEFAULT_CLASS: &str = "snapshot.storage.kubernetes.io/is-default-class";

fn snap(name: &str) -> Value {
    json!({"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
           "metadata": {"name": name},
           "spec": {"source": {"persistentVolumeClaimName": "pvc-1"}}})
}

fn content(name: &str) -> Value {
    json!({"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent",
           "metadata": {"name": name},
           "spec": {"driver": "csi.example.com", "deletionPolicy": "Delete",
                    "source": {"volumeHandle": "vol-1"},
                    "volumeSnapshotRef": {"name": "s", "namespace": "default"}}})
}

fn class(name: &str, driver: &str, default: bool) -> Value {
    let mut c = json!({"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass",
           "metadata": {"name": name}, "driver": driver, "deletionPolicy": "Delete"});
    if default {
        c["metadata"]["annotations"] = json!({ DEFAULT_CLASS: "true" });
    }
    c
}

fn message(v: &Value) -> &str {
    v["message"].as_str().unwrap_or_default()
}

/// `PrepareForCreate`: the status is cleared (the CRD has a status
/// subresource) and the generation starts at 1.
#[tokio::test]
async fn a_snapshot_create_clears_status_and_sets_generation_one() {
    let api = TestApiServer::new();
    let mut body = snap("s1");
    body["status"] = json!({"readyToUse": true});
    let (s, out) = api.post(SNAPSHOTS, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    assert!(out.get("status").is_none(), "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["metadata"]["namespace"], "default", "{out}");
}

#[tokio::test]
async fn a_content_create_clears_status_and_sets_generation_one() {
    let api = TestApiServer::new();
    let mut body = content("c1");
    body["status"] = json!({"readyToUse": true});
    let (s, out) = api.post(CONTENTS, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    assert!(out.get("status").is_none(), "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
}

/// `PrepareForUpdate`: status is kept from the stored object, a spec change
/// bumps the generation and a metadata-only change does not.
#[tokio::test]
async fn a_snapshot_spec_change_bumps_generation_and_put_cannot_write_status() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SNAPSHOTS, &snap("s1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut labelled = created.clone();
    labelled["metadata"]["labels"] = json!({"a": "b"});
    labelled["status"] = json!({"readyToUse": true});
    let (s, out) = api.put(&format!("{SNAPSHOTS}/s1"), &labelled).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert!(out.get("status").is_none(), "{out}");

    let mut changed = out.clone();
    changed["spec"]["volumeSnapshotClassName"] = json!("fast");
    let (s, out) = api.put(&format!("{SNAPSHOTS}/s1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
}

/// `statusStrategy.PrepareForUpdate`: the status endpoint writes only status.
#[tokio::test]
async fn snapshot_status_writes_status_and_keeps_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SNAPSHOTS, &snap("s1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["spec"]["volumeSnapshotClassName"] = json!("fast");
    update["status"] = json!({"readyToUse": true, "boundVolumeSnapshotContentName": "c1"});
    let (s, out) = api.put(&format!("{SNAPSHOTS}/s1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["readyToUse"], true, "{out}");
    assert!(out["spec"].get("volumeSnapshotClassName").is_none(), "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");

    let (s, got) = api.get(&format!("{SNAPSHOTS}/s1/status")).await;
    assert_eq!(s, StatusCode::OK, "{got}");
    assert_eq!(got["status"]["readyToUse"], true, "{got}");

    let patch = json!({"spec": {"volumeSnapshotClassName": "x"}, "status": {"readyToUse": false}});
    let (s, out) = api.patch(&format!("{SNAPSHOTS}/s1/status"), &patch).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["readyToUse"], false, "{out}");
    assert!(out["spec"].get("volumeSnapshotClassName").is_none(), "{out}");
}

#[tokio::test]
async fn content_status_writes_status_and_keeps_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CONTENTS, &content("c1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["spec"]["driver"] = json!("other");
    update["status"] = json!({"readyToUse": true, "snapshotHandle": "h"});
    let (s, out) = api.put(&format!("{CONTENTS}/c1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["snapshotHandle"], "h", "{out}");
    assert_eq!(out["spec"]["driver"], "csi.example.com", "{out}");

    let mut spec = out.clone();
    spec["spec"]["driver"] = json!("other");
    let (s, out) = api.put(&format!("{CONTENTS}/c1"), &spec).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
    assert_eq!(out["status"]["snapshotHandle"], "h", "status is kept: {out}");
}

/// `customResourceStrategy.AllowCreateOnUpdate() == false`.
#[tokio::test]
async fn a_put_to_a_missing_object_is_not_found() {
    let api = TestApiServer::new();
    let (s, out) = api.put(&format!("{SNAPSHOTS}/nope"), &snap("nope")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{out}");
    let (s, out) = api.put(&format!("{CONTENTS}/nope"), &content("nope")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{out}");
    let (s, out) = api
        .put(&format!("{CLASSES}/nope"), &class("nope", "d", false))
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{out}");
}

/// `customResourceStrategy.AllowUnconditionalUpdate() == false`: a PUT with no
/// resourceVersion is refused.
#[tokio::test]
async fn a_put_without_resource_version_is_refused() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SNAPSHOTS, &snap("s1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    let (s, out) = api.put(&format!("{SNAPSHOTS}/s1"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("must be specified for an update"),
        "{out}"
    );
}

/// `NameIsDNSSubdomain` (customresource/validator.go:53).
#[tokio::test]
async fn names_must_be_dns_subdomains() {
    let api = TestApiServer::new();
    let (s, out) = api.post(SNAPSHOTS, &snap("Bad_Name")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    let (s, out) = api.post(CONTENTS, &content("Bad_Name")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    let (s, out) = api.post(CLASSES, &class("Bad_Name", "d", false)).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// The VolumeSnapshotClass CRD declares `required: [deletionPolicy, driver]`
/// and no status subresource.
#[tokio::test]
async fn a_class_requires_driver_and_deletion_policy_and_has_no_status() {
    let api = TestApiServer::new();
    let mut no_driver = class("c1", "d", false);
    no_driver.as_object_mut().unwrap().remove("driver");
    let (s, out) = api.post(CLASSES, &no_driver).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("driver: Required value"), "{out}");

    let mut no_policy = class("c1", "d", false);
    no_policy.as_object_mut().unwrap().remove("deletionPolicy");
    let (s, out) = api.post(CLASSES, &no_policy).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("deletionPolicy: Required value"),
        "{out}"
    );

    let (s, created) = api.post(CLASSES, &class("c1", "d", false)).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let (s, out) = api.get(&format!("{CLASSES}/c1/status")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{out}");
}

/// Without a status subresource every non-metadata change bumps the
/// generation (`PrepareForUpdate`: "except for the changes to `metadata`, any
/// other changes cause the generation to increment").
#[tokio::test]
async fn a_class_change_bumps_generation_unless_metadata_only() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CLASSES, &class("c1", "d", false)).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["metadata"]["generation"], 1, "{created}");

    let mut labelled = created.clone();
    labelled["metadata"]["labels"] = json!({"a": "b"});
    let (s, out) = api.put(&format!("{CLASSES}/c1"), &labelled).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");

    let mut changed = out.clone();
    changed["parameters"] = json!({"k": "v"});
    let (s, out) = api.put(&format!("{CLASSES}/c1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
}

/// `decideSnapshotClassV1` (external-snapshotter release-6.3
/// pkg/validation-webhook/snapshot.go:165-200): at most one default class per
/// driver; denial is a 400 with the webhook's message.
#[tokio::test]
async fn at_most_one_default_class_per_driver() {
    let api = TestApiServer::new();
    let (s, out) = api.post(CLASSES, &class("a", "csi.one", true)).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");

    let (s, out) = api.post(CLASSES, &class("b", "csi.one", true)).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{out}");
    assert!(
        message(&out).contains("default snapshot class: a already exists for driver: csi.one"),
        "{out}"
    );

    // Another driver, and a non-default class of the same driver, are fine.
    let (s, out) = api.post(CLASSES, &class("c", "csi.two", true)).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    let (s, out) = api.post(CLASSES, &class("d", "csi.one", false)).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");

    // Promoting a second class of the same driver on update is denied ...
    let mut promote = out.clone();
    promote["metadata"]["annotations"] = json!({ DEFAULT_CLASS: "true" });
    let (s, out) = api.put(&format!("{CLASSES}/d"), &promote).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{out}");
    assert!(message(&out).contains("already exists for driver: csi.one"));

    // ... while updating the existing default (same driver) is allowed.
    let (s, a) = api.get(&format!("{CLASSES}/a")).await;
    assert_eq!(s, StatusCode::OK, "{a}");
    let mut relabel = a.clone();
    relabel["metadata"]["labels"] = json!({"x": "y"});
    let (s, out) = api.put(&format!("{CLASSES}/a"), &relabel).await;
    assert_eq!(s, StatusCode::OK, "{out}");
}

/// The immutability and required rules that already existed keep working
/// through the Store.
#[tokio::test]
async fn source_immutability_and_create_rules_still_apply() {
    let api = TestApiServer::new();
    let (s, out) = api.post(SNAPSHOTS, &json!({
        "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
        "metadata": {"name": "bad"}, "spec": {}})).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.source"), "{out}");

    let (s, created) = api.post(SNAPSHOTS, &snap("s1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut changed = created.clone();
    changed["spec"]["source"]["persistentVolumeClaimName"] = json!("pvc-2");
    let (s, out) = api.put(&format!("{SNAPSHOTS}/s1"), &changed).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("persistentVolumeClaimName is immutable"),
        "{out}"
    );

    let (s, created) = api.post(CONTENTS, &content("c1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut changed = created.clone();
    changed["spec"]["source"]["volumeHandle"] = json!("vol-2");
    let (s, out) = api.put(&format!("{CONTENTS}/c1"), &changed).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("volumeHandle is immutable"), "{out}");
}

/// Delete: an object with a finalizer is marked, then removed when the last
/// finalizer is drained by an update (`ShouldDeleteDuringUpdate`); the
/// cluster-wide collection serves only LIST and WATCH.
#[tokio::test]
async fn delete_honours_finalizers_and_there_is_no_cluster_wide_deletecollection() {
    let api = TestApiServer::new();
    let mut body = snap("s1");
    body["metadata"]["finalizers"] = json!(["example.com/hold"]);
    let (s, created) = api.post(SNAPSHOTS, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let (s, out) = api.delete(&format!("{SNAPSHOTS}/s1")).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert!(out["metadata"]["deletionTimestamp"].is_string(), "{out}");

    let mut drained = out.clone();
    drained["metadata"]["finalizers"] = json!([]);
    let (s, out) = api.put(&format!("{SNAPSHOTS}/s1"), &drained).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    let (s, _) = api.get(&format!("{SNAPSHOTS}/s1")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let (s, _) = api
        .delete("/apis/snapshot.storage.k8s.io/v1/volumesnapshots")
        .await;
    assert_eq!(s, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn deletecollection_removes_the_namespaced_snapshots() {
    let api = TestApiServer::new();
    for n in ["s1", "s2"] {
        let (s, out) = api.post(SNAPSHOTS, &snap(n)).await;
        assert_eq!(s, StatusCode::CREATED, "{out}");
    }
    let (s, out) = api.delete(SNAPSHOTS).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    let (s, list) = api.get(SNAPSHOTS).await;
    assert_eq!(s, StatusCode::OK, "{list}");
    assert_eq!(list["items"].as_array().map(Vec::len), Some(0), "{list}");
}
