//! VolumeSnapshotContent `spec.sourceVolumeMode` (#2122).
//!
//! CRD `client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml`
//! (kubernetes-csi/external-snapshotter, v1): the field is kept, carries
//! `self == oldSelf` ("sourceVolumeMode is immutable"), and the spec carries
//! `!has(oldSelf.sourceVolumeMode) || has(self.sourceVolumeMode)`
//! ("sourceVolumeMode is required once set").

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CONTENTS: &str = "/apis/snapshot.storage.k8s.io/v1/volumesnapshotcontents";

fn content(name: &str, mode: Option<&str>) -> Value {
    let mut c = json!({"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent",
           "metadata": {"name": name},
           "spec": {"driver": "csi.example.com", "deletionPolicy": "Delete",
                    "source": {"volumeHandle": "vol-1"},
                    "volumeSnapshotRef": {"name": "s", "namespace": "default"}}});
    if let Some(m) = mode {
        c["spec"]["sourceVolumeMode"] = json!(m);
    }
    c
}

fn message(v: &Value) -> &str {
    v["message"].as_str().unwrap_or_default()
}

#[tokio::test]
async fn source_volume_mode_round_trips() {
    let api = TestApiServer::new();
    let (s, out) = api.post(CONTENTS, &content("c1", Some("Block"))).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    assert_eq!(out["spec"]["sourceVolumeMode"], "Block", "{out}");
}

#[tokio::test]
async fn source_volume_mode_is_immutable_and_required_once_set() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CONTENTS, &content("c1", Some("Block"))).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut changed = created.clone();
    changed["spec"]["sourceVolumeMode"] = json!("Filesystem");
    let (s, out) = api.put(&format!("{CONTENTS}/c1"), &changed).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("sourceVolumeMode is immutable"),
        "{out}"
    );

    let mut removed = created.clone();
    removed["spec"]
        .as_object_mut()
        .unwrap()
        .remove("sourceVolumeMode");
    let (s, out) = api.put(&format!("{CONTENTS}/c1"), &removed).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("sourceVolumeMode is required once set"),
        "{out}"
    );
}

/// Unset -> set is allowed (neither rule fires).
#[tokio::test]
async fn source_volume_mode_may_be_set_when_previously_unset() {
    let api = TestApiServer::new();
    let (s, created) = api.post(CONTENTS, &content("c1", None)).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut changed = created.clone();
    changed["spec"]["sourceVolumeMode"] = json!("Filesystem");
    let (s, out) = api.put(&format!("{CONTENTS}/c1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{out}");
}
