//! #3910: direct backends must apply `patch_strategic_merge` with the shared
//! strategic-merge applier, so the job controller's
//! `removeTrackingFinalizerPatch` (`$deleteFromPrimitiveList/finalizers`,
//! pkg/controller/job/job_controller.go:1954-1965) removes one finalizer and
//! keeps the others, instead of replacing the whole list.

use rusternetes_storage::{MemoryStorage, Storage};
use serde_json::json;

#[tokio::test]
async fn delete_from_primitive_list_removes_only_listed_finalizer() {
    let s = MemoryStorage::new();
    let key = "/registry/pods/ns/p1";
    s.create(
        key,
        &json!({"apiVersion":"v1","kind":"Pod","metadata":{
            "name":"p1","namespace":"ns",
            "finalizers":["batch.kubernetes.io/job-tracking","example.com/keep"]}}),
    )
    .await
    .unwrap();

    let patch = json!({"metadata":{
        "$deleteFromPrimitiveList/finalizers":["batch.kubernetes.io/job-tracking"]}});
    let out: serde_json::Value = s.patch_strategic_merge(key, &patch).await.unwrap();

    assert_eq!(out["metadata"]["finalizers"], json!(["example.com/keep"]));
    assert!(out["metadata"]
        .get("$deleteFromPrimitiveList/finalizers")
        .is_none());
}

#[tokio::test]
async fn patch_is_a_delta_on_the_live_object() {
    let s = MemoryStorage::new();
    let key = "/registry/pods/ns/p2";
    s.create(
        key,
        &json!({"apiVersion":"v1","kind":"Pod","metadata":{
            "name":"p2","namespace":"ns","finalizers":["batch.kubernetes.io/job-tracking"]}}),
    )
    .await
    .unwrap();
    // A concurrent writer changes status after the caller's read.
    let mut cur: serde_json::Value = s.get(key).await.unwrap();
    cur["status"] = json!({"phase":"Running"});
    s.update(key, &cur).await.unwrap();

    let patch = json!({"metadata":{
        "$deleteFromPrimitiveList/finalizers":["batch.kubernetes.io/job-tracking"]}});
    let out: serde_json::Value = s.patch_strategic_merge(key, &patch).await.unwrap();
    assert_eq!(out["status"]["phase"], "Running");
}
