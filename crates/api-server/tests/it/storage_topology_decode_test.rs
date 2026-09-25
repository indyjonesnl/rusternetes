//! `allowedTopologies` is validated, and the storage-API tail decodes.
//!
//! `volume.rs` / `csi.rs` slice of #1939 (see the module the PV slice left
//! behind). Two separate problems:
//!
//! 1. `StorageClass.allowedTopologies` was stored unvalidated. Upstream runs
//!    `validateAllowedTopologies`
//!    (`pkg/apis/storage/validation/validation.go:279-303`) →
//!    `ValidateTopologySelectorTerm`
//!    (`pkg/apis/core/validation/validation.go:5074-5092`) →
//!    `validateTopologySelectorLabelRequirement` (`:5094-5113`), which requires
//!    `values`, rejects duplicate values, validates `key` as a label name and
//!    rejects a duplicate key inside one term.
//! 2. `VolumeAttachment`'s spec fields and the PVC status's
//!    `modifyVolumeStatus.status` were required at decode time, so a body
//!    upstream answers with 422 got serde's 400.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn storage_class(name: &str, allowed_topologies: Value) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
        "metadata": { "name": name },
        "provisioner": "csi.example.com",
        "allowedTopologies": allowed_topologies,
    })
}

/// `(label, path, body, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, &'static str, Value, &'static str)> {
    let sc = "/apis/storage.k8s.io/v1/storageclasses";
    let va = "/apis/storage.k8s.io/v1/volumeattachments";
    vec![
        (
            "a topology requirement with no values",
            sc,
            storage_class(
                "sc-no-values",
                json!([{ "matchLabelExpressions": [{ "key": "topology.kubernetes.io/zone" }] }]),
            ),
            "allowedTopologies[0].matchLabelExpressions[0].values: Required value",
        ),
        (
            "a topology requirement with no key",
            sc,
            storage_class(
                "sc-no-key",
                json!([{ "matchLabelExpressions": [{ "values": ["z1"] }] }]),
            ),
            "allowedTopologies[0].matchLabelExpressions[0].key: Invalid value: \"\"",
        ),
        (
            "a topology requirement with a duplicate value",
            sc,
            storage_class(
                "sc-dup-value",
                json!([{ "matchLabelExpressions": [
                    { "key": "topology.kubernetes.io/zone", "values": ["z1", "z1"] }
                ] }]),
            ),
            "allowedTopologies[0].matchLabelExpressions[0].values[1]: Duplicate value",
        ),
        (
            "a topology term with a duplicate key",
            sc,
            storage_class(
                "sc-dup-key",
                json!([{ "matchLabelExpressions": [
                    { "key": "topology.kubernetes.io/zone", "values": ["z1"] },
                    { "key": "topology.kubernetes.io/zone", "values": ["z2"] }
                ] }]),
            ),
            "allowedTopologies[0].matchLabelExpressions[1].key: Duplicate value",
        ),
        (
            "a volumeAttachment with an empty spec",
            va,
            json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
                "metadata": { "name": "va-empty" }, "spec": {}
            }),
            "spec.attacher: Required value",
        ),
        (
            "a volumeAttachment with no source",
            va,
            json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
                "metadata": { "name": "va-no-source" },
                "spec": { "attacher": "csi.example.com", "nodeName": "node-1" }
            }),
            "spec.source: Required value: must specify exactly one of inlineVolumeSpec \
             and persistentVolumeName",
        ),
    ]
}

#[tokio::test]
async fn a_storage_body_upstream_rejects_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (label, path, body, expected) in cases() {
        let (status, resp) = api
            .send("POST", path, Some("application/json"), Some(&body))
            .await;

        assert_ne!(
            status.as_u16(),
            400,
            "{label} was rejected by the decoder, before any validation: {resp}"
        );
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be Invalid, not {status}: {resp}"
        );
        let message = resp["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

/// The accept side. An empty `tokenRequests[].audience` is legal upstream —
/// `validateTokenRequests` (`pkg/apis/storage/validation/validation.go`) only
/// rejects *duplicate* audiences and out-of-range expirations — and
/// `ModifyVolumeStatus` has no upstream validator at all, so both must be
/// written rather than rejected.
#[tokio::test]
async fn a_storage_body_upstream_accepts_is_written() {
    let api = TestApiServer::new();

    let accepted: Vec<(&str, &str, Value)> = vec![
        (
            "a storageClass with well-formed allowedTopologies",
            "/apis/storage.k8s.io/v1/storageclasses",
            storage_class(
                "sc-ok",
                json!([{ "matchLabelExpressions": [
                    { "key": "topology.kubernetes.io/zone", "values": ["z1", "z2"] }
                ] }]),
            ),
        ),
        (
            "a storageClass with no allowedTopologies",
            "/apis/storage.k8s.io/v1/storageclasses",
            json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                "metadata": { "name": "sc-none" }, "provisioner": "csi.example.com"
            }),
        ),
        (
            "a csiDriver whose tokenRequest has no audience",
            "/apis/storage.k8s.io/v1/csidrivers",
            json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
                "metadata": { "name": "csi.example.com" },
                "spec": { "tokenRequests": [{}] }
            }),
        ),
        (
            "a well-formed volumeAttachment",
            "/apis/storage.k8s.io/v1/volumeattachments",
            json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
                "metadata": { "name": "va-ok" },
                "spec": {
                    "attacher": "csi.example.com",
                    "nodeName": "node-1",
                    "source": { "persistentVolumeName": "pv-1" }
                }
            }),
        ),
    ];

    for (label, path, body) in accepted {
        let (status, resp) = api
            .send("POST", path, Some("application/json"), Some(&body))
            .await;
        assert!(
            status.is_success(),
            "{label} must be written: {status} {resp}"
        );
    }
}

/// `ModifyVolumeStatus.status` has no upstream validator — the type's own doc
/// says "New statuses can be added in the future. Consumers should check for
/// unknown statuses and fail appropriately" (`pkg/apis/core/types.go:665-677`)
/// — so an absent one must decode rather than fail the request.
#[tokio::test]
async fn a_modify_volume_status_without_a_status_decodes() {
    use rusternetes_common::resources::volume::ModifyVolumeStatus;

    let decoded: ModifyVolumeStatus =
        serde_json::from_value(json!({ "targetVolumeAttributesClassName": "gold" }))
            .expect("a modifyVolumeStatus with no status must decode");
    assert!(decoded.status.is_empty());
}
