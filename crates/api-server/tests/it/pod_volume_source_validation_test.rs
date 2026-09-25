//! A pod volume's **source** is validated — and an absent field decodes.
//!
//! Third and last `pod.rs` slice of #1939. `validate_volumes` checked volume
//! names and uniqueness only: *no* volume source was validated at all. A
//! `hostPath` with no `path`, a `persistentVolumeClaim` with no `claimName`, a
//! `configMap` item with no `key`, a projected `serviceAccountToken` with no
//! `path`, an `ephemeral` volume with no template — every one of those was
//! either a serde 400 (for the fields modelled non-`Option`) or, worse, a
//! silent `201 Created`.
//!
//! Upstream runs `validateVolumeSource`
//! (`pkg/apis/core/validation/validation.go:552-814`) on every volume from
//! `ValidateVolumes` (`:453-495`), which dispatches to a per-kind validator and
//! enforces that exactly one kind is set. This test is the wire-level contract
//! for that port: one row per rule, asserted against the error a client reads.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn containers() -> Value {
    json!([{ "name": "c", "image": "nginx" }])
}

fn volume(source: Value) -> Value {
    let mut v = json!({ "name": "v" });
    if let (Some(obj), Some(src)) = (v.as_object_mut(), source.as_object()) {
        for (k, val) in src {
            obj.insert(k.clone(), val.clone());
        }
    }
    json!({ "containers": containers(), "volumes": [v] })
}

/// `(label, pod spec, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "a volume with no source at all",
            volume(json!({})),
            "spec.volumes[0]: Required value: must specify a volume type",
        ),
        (
            "a volume with two sources",
            volume(json!({ "emptyDir": {}, "hostPath": { "path": "/x" } })),
            "spec.volumes[0].hostPath: Forbidden: may not specify more than 1 volume type",
        ),
        (
            "hostPath with no path",
            volume(json!({ "hostPath": {} })),
            "spec.volumes[0].hostPath.path: Required value",
        ),
        (
            "hostPath with an unsupported type",
            volume(json!({ "hostPath": { "path": "/x", "type": "Nope" } })),
            "spec.volumes[0].hostPath.type: Unsupported value: \"Nope\"",
        ),
        (
            "persistentVolumeClaim with no claimName",
            volume(json!({ "persistentVolumeClaim": {} })),
            "spec.volumes[0].persistentVolumeClaim.claimName: Required value",
        ),
        (
            "configMap with no name",
            volume(json!({ "configMap": {} })),
            "spec.volumes[0].configMap.name: Required value",
        ),
        (
            "configMap item with no key",
            volume(json!({ "configMap": { "name": "cm", "items": [{ "path": "p" }] } })),
            "spec.volumes[0].configMap.items[0].key: Required value",
        ),
        (
            "configMap item with no path",
            volume(json!({ "configMap": { "name": "cm", "items": [{ "key": "k" }] } })),
            "spec.volumes[0].configMap.items[0].path: Required value",
        ),
        (
            "configMap item path escaping the mount",
            volume(json!({ "configMap": { "name": "cm", "items": [{ "key": "k", "path": "../esc" }] } })),
            "spec.volumes[0].configMap.items[0].path: Invalid value: \"../esc\"",
        ),
        (
            "secret with no secretName",
            volume(json!({ "secret": {} })),
            "spec.volumes[0].secret.secretName: Required value",
        ),
        (
            "downwardAPI item with no path",
            volume(json!({ "downwardAPI": { "items": [{ "fieldRef": { "apiVersion": "v1", "fieldPath": "metadata.name" } }] } })),
            "spec.volumes[0].downwardAPI.path: Required value",
        ),
        (
            "downwardAPI item with neither ref",
            volume(json!({ "downwardAPI": { "items": [{ "path": "p" }] } })),
            "spec.volumes[0].downwardAPI: Required value: one of fieldRef and resourceFieldRef is required",
        ),
        (
            "downwardAPI item projecting a spec field",
            volume(json!({ "downwardAPI": { "items": [{ "path": "p", "fieldRef": { "apiVersion": "v1", "fieldPath": "spec.nodeName" } }] } })),
            "spec.volumes[0].downwardAPI.fieldRef.fieldPath: Unsupported value: \"spec.nodeName\"",
        ),
        (
            "downwardAPI resourceFieldRef with no containerName",
            volume(json!({ "downwardAPI": { "items": [{ "path": "p", "resourceFieldRef": { "resource": "limits.cpu" } }] } })),
            "spec.volumes[0].downwardAPI.resourceFieldRef.containerName: Required value",
        ),
        (
            "projected serviceAccountToken with no path",
            volume(json!({ "projected": { "sources": [{ "serviceAccountToken": {} }] } })),
            "spec.volumes[0].projected.path: Required value",
        ),
        (
            "projected serviceAccountToken with too short an expiry",
            volume(json!({ "projected": { "sources": [{ "serviceAccountToken": { "path": "t", "expirationSeconds": 60 } }] } })),
            "may not specify a duration less than 10 minutes",
        ),
        (
            "projected clusterTrustBundle with no path",
            volume(json!({ "projected": { "sources": [{ "clusterTrustBundle": { "name": "b" } }] } })),
            "spec.volumes[0].projected.sources[0].clusterTrustBundle.path: Required value",
        ),
        (
            "projected clusterTrustBundle with neither name nor signerName",
            volume(json!({ "projected": { "sources": [{ "clusterTrustBundle": { "path": "p" } }] } })),
            "either name or signerName must be specified",
        ),
        (
            "projected podCertificate with no keyType",
            volume(json!({ "projected": { "sources": [{ "podCertificate": { "signerName": "example.com/s", "keyPath": "k" } }] } })),
            "spec.volumes[0].projected.sources[0].podCertificate.keyType: Unsupported value: \"\"",
        ),
        (
            "two projected sources claiming one path",
            volume(json!({ "projected": { "sources": [
                { "configMap": { "name": "a", "items": [{ "key": "k", "path": "same" }] } },
                { "configMap": { "name": "b", "items": [{ "key": "k", "path": "same" }] } }
            ] } })),
            "conflicting duplicate paths",
        ),
        (
            "one projected source with two kinds",
            volume(json!({ "projected": { "sources": [{ "configMap": { "name": "a" }, "secret": { "name": "b" } }] } })),
            "spec.volumes[0].projected.sources[0]: Forbidden: may not specify more than 1 volume type per source",
        ),
        (
            "ephemeral with no volumeClaimTemplate",
            volume(json!({ "ephemeral": {} })),
            "spec.volumes[0].ephemeral.volumeClaimTemplate: Required value",
        ),
        (
            "ephemeral whose template spec is empty",
            volume(json!({ "ephemeral": { "volumeClaimTemplate": { "spec": {} } } })),
            "spec.volumes[0].ephemeral.volumeClaimTemplate.spec.accessModes: Required value",
        ),
        (
            "csi with no driver",
            volume(json!({ "csi": {} })),
            "spec.volumes[0].csi.driver: Required value",
        ),
        (
            "nfs with no server",
            volume(json!({ "nfs": { "path": "/p" } })),
            "spec.volumes[0].nfs.server: Required value",
        ),
        (
            "nfs with a relative path",
            volume(json!({ "nfs": { "server": "s", "path": "rel" } })),
            "spec.volumes[0].nfs.path: Invalid value: \"rel\": must be an absolute path",
        ),
        (
            "iscsi with no targetPortal",
            volume(json!({ "iscsi": { "iqn": "iqn.2001-04.com.example:storage", "lun": 0 } })),
            "spec.volumes[0].iscsi.targetPortal: Required value",
        ),
        (
            "iscsi with an unrecognised iqn",
            volume(json!({ "iscsi": { "targetPortal": "1.2.3.4:3260", "iqn": "nope", "lun": 0 } })),
            "must be valid format starting with iqn, eui, or naa",
        ),
        (
            "iscsi with a lun out of range",
            volume(json!({ "iscsi": { "targetPortal": "1.2.3.4:3260", "iqn": "iqn.2001-04.com.example:storage", "lun": 999 } })),
            "spec.volumes[0].iscsi.lun: Invalid value: 999",
        ),
    ]
}

#[tokio::test]
async fn every_bad_volume_source_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, spec, expected)) in cases().into_iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/api/v1/namespaces/default/pods",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": format!("vol-{i}"), "namespace": "default" },
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

/// The accept side: a well-formed volume of each kind this port touches must
/// still be written. A validator that rejects everything would pass the table
/// above.
#[tokio::test]
async fn well_formed_volume_sources_are_written() {
    let api = TestApiServer::new();

    let sources = [
        json!({ "emptyDir": {} }),
        json!({ "hostPath": { "path": "/var/log", "type": "Directory" } }),
        json!({ "persistentVolumeClaim": { "claimName": "pvc" } }),
        json!({ "configMap": { "name": "cm", "items": [{ "key": "k", "path": "k.conf" }] } }),
        json!({ "secret": { "secretName": "s", "defaultMode": 420 } }),
        json!({ "downwardAPI": { "items": [{ "path": "labels", "fieldRef": { "apiVersion": "v1", "fieldPath": "metadata.labels" } }] } }),
        json!({ "projected": { "sources": [{ "serviceAccountToken": { "path": "token", "expirationSeconds": 3600 } }] } }),
        json!({ "csi": { "driver": "csi.example.com" } }),
        json!({ "nfs": { "server": "10.0.0.1", "path": "/exports" } }),
        json!({ "iscsi": { "targetPortal": "1.2.3.4:3260", "iqn": "iqn.2001-04.com.example:storage", "lun": 0 } }),
    ];

    for (i, source) in sources.iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/api/v1/namespaces/default/pods",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": format!("good-vol-{i}"), "namespace": "default" },
                    "spec": volume(source.clone()),
                })),
            )
            .await;
        assert!(
            status.is_success(),
            "a well-formed volume source must be written: {source} -> {status} {body}"
        );
    }
}
