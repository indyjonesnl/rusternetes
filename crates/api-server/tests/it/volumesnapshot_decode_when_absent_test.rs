//! A VolumeSnapshot and a VolumeSnapshotContent decode when a field is absent.
//!
//! #1939: a bare non-`Option` Rust field answers serde's `400 BadRequest` —
//! no `Status`, no `reason`, no `details.causes` — where the field's schema
//! answers `422 Invalid` with a field path.
//!
//! `snapshot.storage.k8s.io` is not a Kubernetes API, so there is no Go
//! validator: the contract is the `required:` list and the
//! `x-kubernetes-validations` CEL rules in `kubernetes-csi/external-snapshotter`
//! (`client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml` and
//! `…_volumesnapshotcontents.yaml`). Rusternetes serves both types natively
//! rather than through the CRD machinery, so before this slice a snapshot with
//! no `spec.source` at all was simply written.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SNAPSHOTS: &str = "/apis/snapshot.storage.k8s.io/v1/namespaces/default/volumesnapshots";
const CONTENTS: &str = "/apis/snapshot.storage.k8s.io/v1/volumesnapshotcontents";

fn snapshot(name: &str, spec: Value) -> Value {
    json!({
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": { "name": name },
        "spec": spec,
    })
}

fn content(name: &str, spec: Value) -> Value {
    json!({
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshotContent",
        "metadata": { "name": name },
        "spec": spec,
    })
}

fn valid_content_spec() -> Value {
    json!({
        "driver": "csi.example.com",
        "deletionPolicy": "Delete",
        "source": { "volumeHandle": "vol-1" },
        "volumeSnapshotRef": { "name": "snap-1", "namespace": "default" }
    })
}

#[tokio::test]
async fn every_absent_snapshot_field_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    let cases: Vec<(&str, &str, Value, &str)> = vec![
        (
            "a VolumeSnapshot with no spec at all",
            SNAPSHOTS,
            snapshot("snap-no-spec", json!({})),
            "spec.source",
        ),
        (
            "a VolumeSnapshot whose source sets both names",
            SNAPSHOTS,
            snapshot(
                "snap-both",
                json!({ "source": {
                    "persistentVolumeClaimName": "pvc-1",
                    "volumeSnapshotContentName": "content-1"
                } }),
            ),
            "exactly one of volumeSnapshotContentName and persistentVolumeClaimName must be set",
        ),
        (
            "a VolumeSnapshot with an empty volumeSnapshotClassName",
            SNAPSHOTS,
            snapshot(
                "snap-empty-class",
                json!({
                    "source": { "persistentVolumeClaimName": "pvc-1" },
                    "volumeSnapshotClassName": ""
                }),
            ),
            "volumeSnapshotClassName must not be the empty string when set",
        ),
        (
            "a VolumeSnapshotContent with no spec at all",
            CONTENTS,
            content("content-no-spec", json!({})),
            "spec.driver: Required value",
        ),
        (
            "a VolumeSnapshotContent with no deletionPolicy",
            CONTENTS,
            content(
                "content-no-policy",
                json!({
                    "driver": "csi.example.com",
                    "source": { "volumeHandle": "vol-1" },
                    "volumeSnapshotRef": { "name": "snap-1", "namespace": "default" }
                }),
            ),
            "spec.deletionPolicy: Required value",
        ),
        (
            "a VolumeSnapshotContent whose source sets both handles",
            CONTENTS,
            content(
                "content-both",
                json!({
                    "driver": "csi.example.com",
                    "deletionPolicy": "Delete",
                    "source": { "volumeHandle": "vol-1", "snapshotHandle": "snap-h" },
                    "volumeSnapshotRef": { "name": "snap-1", "namespace": "default" }
                }),
            ),
            "exactly one of volumeHandle and snapshotHandle must be set",
        ),
        (
            "a VolumeSnapshotContent whose volumeSnapshotRef has no namespace",
            CONTENTS,
            content(
                "content-no-ns",
                json!({
                    "driver": "csi.example.com",
                    "deletionPolicy": "Delete",
                    "source": { "volumeHandle": "vol-1" },
                    "volumeSnapshotRef": { "name": "snap-1" }
                }),
            ),
            "spec.volumeSnapshotRef.namespace: Required value",
        ),
    ];

    for (label, path, body, expected) in cases {
        let (status, response) = api
            .send("POST", path, Some("application/json"), Some(&body))
            .await;
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be 422 Invalid, got {status}: {response}"
        );
        let message = response["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

/// The accept side, including the `volumeSnapshotClassName`-absent case the
/// CRD calls out ("may be left nil to indicate that the default SnapshotClass
/// should be used") — which is why the field is an `Option` rather than a
/// defaulted `String`.
#[tokio::test]
async fn a_minimal_snapshot_and_content_are_written() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            SNAPSHOTS,
            Some("application/json"),
            Some(&snapshot(
                "snap-ok",
                json!({ "source": { "persistentVolumeClaimName": "pvc-1" } }),
            )),
        )
        .await;
    assert!(
        status.is_success(),
        "a minimal snapshot must be written: {status} {body}"
    );
    assert!(
        body["spec"].get("volumeSnapshotClassName").is_none(),
        "an absent class name must stay absent, not become \"\": {body}"
    );

    let (status, body) = api
        .send(
            "POST",
            CONTENTS,
            Some("application/json"),
            Some(&content("content-ok", valid_content_spec())),
        )
        .await;
    assert!(
        status.is_success(),
        "a minimal content must be written: {status} {body}"
    );
}

/// The CRD marks each source name `self == oldSelf` and "required once set",
/// so an update that drops or changes one is rejected.
#[tokio::test]
async fn a_source_name_is_immutable_once_set() {
    let api = TestApiServer::new();

    let (status, _) = api
        .send(
            "POST",
            SNAPSHOTS,
            Some("application/json"),
            Some(&snapshot(
                "snap-immutable",
                json!({ "source": { "persistentVolumeClaimName": "pvc-1" } }),
            )),
        )
        .await;
    assert!(status.is_success());

    let (code, body) = api
        .send(
            "PUT",
            &format!("{SNAPSHOTS}/snap-immutable"),
            Some("application/json"),
            Some(&snapshot(
                "snap-immutable",
                json!({ "source": { "persistentVolumeClaimName": "pvc-2" } }),
            )),
        )
        .await;
    assert_eq!(
        code.as_u16(),
        422,
        "changing the PVC name must be 422: {body}"
    );
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("persistentVolumeClaimName is immutable"),
        "{body}"
    );

    let (code, body) = api
        .send(
            "PUT",
            &format!("{SNAPSHOTS}/snap-immutable"),
            Some("application/json"),
            Some(&snapshot(
                "snap-immutable",
                json!({ "source": { "volumeSnapshotContentName": "content-1" } }),
            )),
        )
        .await;
    assert_eq!(
        code.as_u16(),
        422,
        "dropping the PVC name must be 422: {body}"
    );
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("persistentVolumeClaimName is required once set"),
        "{body}"
    );
}
