//! A PersistentVolume's **source** and **nodeAffinity** are validated.
//!
//! `volume.rs` slice of #1939. `validate_persistent_volume_spec` counted the
//! sources ("exactly one") but validated none of their fields — its own module
//! doc said so: "The exhaustive per-source field validation (each
//! `validate*VolumeSource`) is left as a follow-up." So a PV with
//! `{"csi": {}}` was a serde 400 on the non-`Option` `driver`, and a PV with
//! `{"nfs": {"server": "x"}}` and no `path` was a silent `201 Created`.
//! `nodeAffinity` was never validated at all beyond "Local needs one".
//!
//! Upstream `ValidatePersistentVolumeSpec`
//! (`pkg/apis/core/validation/validation.go:1965-2135`) runs a per-source
//! validator next to each `numVolumes++`, and `validateVolumeNodeAffinity`
//! (`:8701-8714`) requires `required` and hands it to `ValidateNodeSelector`
//! (`:5034-5048`). This test is the wire-level contract for that port.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// A PV spec with the always-required scalars filled in, plus `source` merged
/// over the top.
fn pv(source: Value) -> Value {
    let mut spec = json!({
        "capacity": { "storage": "1Gi" },
        "accessModes": ["ReadWriteOnce"],
    });
    if let (Some(obj), Some(src)) = (spec.as_object_mut(), source.as_object()) {
        for (k, val) in src {
            obj.insert(k.clone(), val.clone());
        }
    }
    spec
}

/// `(label, pv spec, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "hostPath with no path",
            pv(json!({ "hostPath": {} })),
            "spec.hostPath.path: Required value",
        ),
        (
            "hostPath with a backstep",
            pv(json!({ "hostPath": { "path": "/data/../etc" } })),
            "spec.hostPath.path: Invalid value: \"/data/../etc\"",
        ),
        (
            "local with no path",
            pv(json!({
                "local": {},
                "nodeAffinity": { "required": { "nodeSelectorTerms": [
                    { "matchExpressions": [{ "key": "k", "operator": "Exists" }] }
                ] } }
            })),
            "spec.local.path: Required value",
        ),
        (
            "nfs with no server",
            pv(json!({ "nfs": { "path": "/exports" } })),
            "spec.nfs.server: Required value",
        ),
        (
            "nfs with a relative path",
            pv(json!({ "nfs": { "server": "10.0.0.1", "path": "exports" } })),
            "spec.nfs.path: Invalid value: \"exports\"",
        ),
        (
            "csi with no driver",
            pv(json!({ "csi": { "volumeHandle": "vol-1" } })),
            "spec.csi.driver: Required value",
        ),
        (
            "csi with no volumeHandle",
            pv(json!({ "csi": { "driver": "csi.example.com" } })),
            "spec.csi.volumeHandle: Required value",
        ),
        (
            "csi secret reference with no namespace",
            pv(json!({ "csi": {
                "driver": "csi.example.com",
                "volumeHandle": "vol-1",
                "controllerPublishSecretRef": { "name": "s" }
            } })),
            "spec.csi.controllerPublishSecretRef.namespace: Required value",
        ),
        (
            "csi secret reference with no name",
            pv(json!({ "csi": {
                "driver": "csi.example.com",
                "volumeHandle": "vol-1",
                "nodePublishSecretRef": { "namespace": "default" }
            } })),
            "spec.csi.nodePublishSecretRef.name: Required value",
        ),
        (
            "iscsi with no targetPortal",
            pv(json!({ "iscsi": { "iqn": "iqn.2001-04.com.example:storage", "lun": 0 } })),
            "spec.iscsi.targetPortal: Required value",
        ),
        (
            "iscsi with an unrecognised iqn",
            pv(json!({ "iscsi": { "targetPortal": "1.2.3.4:3260", "iqn": "nope", "lun": 0 } })),
            "spec.iscsi.iqn: Invalid value: \"nope\"",
        ),
        (
            "iscsi with a lun out of range",
            pv(json!({ "iscsi": {
                "targetPortal": "1.2.3.4:3260",
                "iqn": "iqn.2001-04.com.example:storage",
                "lun": 300
            } })),
            "spec.iscsi.lun: Invalid value: 300",
        ),
        (
            "iscsi with CHAP auth and no secretRef",
            pv(json!({ "iscsi": {
                "targetPortal": "1.2.3.4:3260",
                "iqn": "iqn.2001-04.com.example:storage",
                "lun": 0,
                "chapAuthSession": true
            } })),
            "spec.iscsi.secretRef: Required value",
        ),
        (
            "nodeAffinity with no required",
            pv(json!({ "hostPath": { "path": "/data" }, "nodeAffinity": {} })),
            "spec.nodeAffinity.required: Required value: must specify required node constraints",
        ),
        (
            "nodeAffinity with no terms",
            pv(json!({
                "hostPath": { "path": "/data" },
                "nodeAffinity": { "required": { "nodeSelectorTerms": [] } }
            })),
            "spec.nodeAffinity.required.nodeSelectorTerms: Required value: must have at least one node selector term",
        ),
        (
            "a node selector requirement with an unknown operator",
            pv(json!({
                "hostPath": { "path": "/data" },
                "nodeAffinity": { "required": { "nodeSelectorTerms": [
                    { "matchExpressions": [{ "key": "k", "operator": "Nope" }] }
                ] } }
            })),
            "spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].operator: \
             Invalid value: \"Nope\"",
        ),
        (
            "a node selector requirement with In and no values",
            pv(json!({
                "hostPath": { "path": "/data" },
                "nodeAffinity": { "required": { "nodeSelectorTerms": [
                    { "matchExpressions": [{ "key": "k", "operator": "In" }] }
                ] } }
            })),
            "spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values: \
             Required value",
        ),
    ]
}

#[tokio::test]
async fn every_bad_persistent_volume_source_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, spec, expected)) in cases().into_iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/api/v1/persistentvolumes",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolume",
                    "metadata": { "name": format!("pv-{i}") },
                    "spec": spec,
                })),
            )
            .await;

        assert_ne!(
            status.as_u16(),
            400,
            "{label} was rejected by the decoder, before any validation: {body}"
        );
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be Invalid, not {status}: {body}"
        );
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

/// The accept side: a well-formed PV of each source kind must still be written.
/// A validator that rejects everything would pass the table above.
#[tokio::test]
async fn well_formed_persistent_volume_sources_are_written() {
    let api = TestApiServer::new();

    let sources = [
        json!({ "hostPath": { "path": "/data", "type": "DirectoryOrCreate" } }),
        json!({ "nfs": { "server": "10.0.0.1", "path": "/exports" } }),
        json!({ "iscsi": {
            "targetPortal": "1.2.3.4:3260",
            "iqn": "iqn.2001-04.com.example:storage",
            "lun": 0
        } }),
        json!({
            "local": { "path": "/mnt/disks/ssd1" },
            "nodeAffinity": { "required": { "nodeSelectorTerms": [
                { "matchExpressions": [{ "key": "kubernetes.io/hostname", "operator": "In", "values": ["node-1"] }] }
            ] } }
        }),
        json!({ "csi": {
            "driver": "csi.example.com",
            "volumeHandle": "vol-1",
            "nodePublishSecretRef": { "name": "s", "namespace": "default" }
        } }),
    ];

    for (i, source) in sources.iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/api/v1/persistentvolumes",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolume",
                    "metadata": { "name": format!("good-pv-{i}") },
                    "spec": pv(source.clone()),
                })),
            )
            .await;
        assert!(
            status.is_success(),
            "a well-formed PV source must be written: {source} -> {status} {body}"
        );
    }
}
