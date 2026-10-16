//! Server-side apply honours `GetResetFields` for PV, PVC, ResourceQuota, RC,
//! FlowSchema, PriorityLevelConfiguration, VolumeAttachment,
//! ValidatingAdmissionPolicy, ResourceClaim and APIService (#3728, Refs #2226), as
//! `ssa_reset_fields_batch_test` does for Job/CronJob/HPA/...
//!
//! Upstream `pkg/registry/<group>/<kind>/strategy.go`: the main strategy
//! resets `status`; the `/status` strategy resets `spec`, and for
//! FlowSchema, PLC, VolumeAttachment, VAP and ResourceClaim also `metadata`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const APPLY: &str = "application/apply-patch+yaml";

fn owned_keys(out: &Value, manager: &str) -> Vec<String> {
    out["metadata"]["managedFields"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["manager"] == manager))
        .and_then(|e| e["fieldsV1"].as_object())
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default()
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

struct Kind {
    api_version: &'static str,
    kind: &'static str,
    prefix: &'static str,
    plural: &'static str,
    namespaced: bool,
    /// `/status` also resets `metadata` upstream.
    status_resets_metadata: bool,
    spec: Value,
    status: Value,
}

fn kinds() -> Vec<Kind> {
    vec![
        Kind {
            api_version: "v1",
            kind: "PersistentVolume",
            prefix: "/api/v1",
            plural: "persistentvolumes",
            namespaced: false,
            status_resets_metadata: false,
            spec: json!({"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                "hostPath": {"path": "/tmp/rf"}}),
            status: json!({"phase": "Available"}),
        },
        Kind {
            api_version: "v1",
            kind: "PersistentVolumeClaim",
            prefix: "/api/v1",
            plural: "persistentvolumeclaims",
            namespaced: true,
            status_resets_metadata: false,
            spec: json!({"accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1Gi"}}}),
            status: json!({"phase": "Bound"}),
        },
        Kind {
            api_version: "v1",
            kind: "ResourceQuota",
            prefix: "/api/v1",
            plural: "resourcequotas",
            namespaced: true,
            status_resets_metadata: false,
            spec: json!({"hard": {"pods": "10"}}),
            status: json!({"hard": {"pods": "10"}, "used": {"pods": "1"}}),
        },
        Kind {
            api_version: "v1",
            kind: "ReplicationController",
            prefix: "/api/v1",
            plural: "replicationcontrollers",
            namespaced: true,
            status_resets_metadata: false,
            spec: json!({"replicas": 1, "selector": {"app": "rf"}, "template": {
                "metadata": {"labels": {"app": "rf"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}}}),
            status: json!({"replicas": 1}),
        },
        Kind {
            api_version: "flowcontrol.apiserver.k8s.io/v1",
            kind: "FlowSchema",
            prefix: "/apis/flowcontrol.apiserver.k8s.io/v1",
            plural: "flowschemas",
            namespaced: false,
            status_resets_metadata: true,
            spec: json!({"priorityLevelConfiguration": {"name": "global-default"},
                "matchingPrecedence": 1000,
                "rules": [{"subjects": [{"kind": "Group", "group": {"name": "system:authenticated"}}],
                    "nonResourceRules": [{"verbs": ["*"], "nonResourceURLs": ["*"]}]}]}),
            status: json!({"conditions": [{"type": "Dangling", "status": "False",
                "reason": "Found", "lastTransitionTime": "2020-01-01T00:00:00Z"}]}),
        },
        Kind {
            api_version: "flowcontrol.apiserver.k8s.io/v1",
            kind: "PriorityLevelConfiguration",
            prefix: "/apis/flowcontrol.apiserver.k8s.io/v1",
            plural: "prioritylevelconfigurations",
            namespaced: false,
            status_resets_metadata: true,
            spec: json!({"type": "Limited", "limited": {"nominalConcurrencyShares": 5,
                "limitResponse": {"type": "Reject"}}}),
            status: json!({"conditions": [{"type": "Ready", "status": "True",
                "reason": "Ok", "lastTransitionTime": "2020-01-01T00:00:00Z"}]}),
        },
        Kind {
            api_version: "storage.k8s.io/v1",
            kind: "VolumeAttachment",
            prefix: "/apis/storage.k8s.io/v1",
            plural: "volumeattachments",
            namespaced: false,
            status_resets_metadata: true,
            spec: json!({"attacher": "rf.csi.example", "nodeName": "n1",
                "source": {"persistentVolumeName": "pv-rf"}}),
            status: json!({"attached": true}),
        },
        Kind {
            api_version: "admissionregistration.k8s.io/v1",
            kind: "ValidatingAdmissionPolicy",
            prefix: "/apis/admissionregistration.k8s.io/v1",
            plural: "validatingadmissionpolicies",
            namespaced: false,
            status_resets_metadata: true,
            spec: json!({"failurePolicy": "Fail",
                "matchConstraints": {"resourceRules": [{"apiGroups": [""], "apiVersions": ["v1"],
                    "operations": ["CREATE"], "resources": ["pods"]}]},
                "validations": [{"expression": "true"}]}),
            status: json!({"observedGeneration": 1}),
        },
        Kind {
            api_version: "resource.k8s.io/v1",
            kind: "ResourceClaim",
            prefix: "/apis/resource.k8s.io/v1",
            plural: "resourceclaims",
            namespaced: true,
            status_resets_metadata: true,
            spec: json!({"devices": {"requests": [{"name": "r",
                "exactly": {"deviceClassName": "dc"}}]}}),
            status: json!({"allocation": {"devices": {"results": [{"request": "r",
                "driver": "rf.example.com", "pool": "pool", "device": "dev"}]}},
                "reservedFor": [{"resource": "pods", "name": "p", "uid": "u-1"}]}),
        },
        Kind {
            api_version: "apiregistration.k8s.io/v1",
            kind: "APIService",
            prefix: "/apis/apiregistration.k8s.io/v1",
            plural: "apiservices",
            namespaced: false,
            status_resets_metadata: true,
            spec: json!({"group": "rf.example.com", "version": "v1",
                "groupPriorityMinimum": 100, "versionPriority": 10,
                "insecureSkipTLSVerify": true,
                "service": {"name": "svc", "namespace": "default", "port": 443}}),
            status: json!({"conditions": [{"type": "Available", "status": "True",
                "reason": "Ok", "lastTransitionTime": "2020-01-01T00:00:00Z"}]}),
        },
    ]
}

/// The stored name: an APIService must be `<version>.<group>`.
fn object_name(k: &Kind, base: &str) -> String {
    match k.kind {
        "APIService" => format!("v1.{base}.example.com"),
        _ => base.to_string(),
    }
}

fn spec_for(k: &Kind, base: &str) -> Value {
    match k.kind {
        "APIService" => {
            let mut spec = k.spec.clone();
            spec["group"] = json!(format!("{base}.example.com"));
            spec
        }
        _ => k.spec.clone(),
    }
}

fn body(k: &Kind, base: &str, with_status: bool) -> Value {
    let name = object_name(k, base);
    let name = name.as_str();
    let mut meta = json!({"name": name, "labels": {"rf": "label"}});
    if k.namespaced {
        meta["namespace"] = json!("default");
    }
    let mut b = json!({
        "apiVersion": k.api_version, "kind": k.kind, "metadata": meta, "spec": spec_for(k, base)
    });
    if with_status {
        b["status"] = k.status.clone();
    }
    b
}

fn collection(k: &Kind) -> String {
    if k.namespaced {
        format!("{}/namespaces/default/{}", k.prefix, k.plural)
    } else {
        format!("{}/{}", k.prefix, k.plural)
    }
}

async fn main_resource_does_not_own_status(i: usize) {
    let k = &kinds()[i];
    let api = TestApiServer::new();
    let name = "rf-main";
    let uri = format!("{}/{}", collection(k), object_name(k, name));
    let (code, out) = apply(&api, &uri, "user", &body(k, name, true)).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let owned = owned_keys(&out, "user");
    assert!(owned.contains(&"f:spec".to_string()), "{out}");
    assert!(!owned.contains(&"f:status".to_string()), "{out}");
}

async fn status_does_not_own_spec(i: usize) {
    let k = &kinds()[i];
    let api = TestApiServer::new();
    let name = "rf-status";
    let (code, out) = api.post(&collection(k), &body(k, name, false)).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let uri = format!("{}/{}/status", collection(k), object_name(k, name));
    let (code, out) = apply(&api, &uri, "ctrl", &body(k, name, true)).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let owned = owned_keys(&out, "ctrl");
    assert!(owned.contains(&"f:status".to_string()), "{out}");
    assert!(!owned.contains(&"f:spec".to_string()), "{out}");
    if k.status_resets_metadata {
        assert!(!owned.contains(&"f:metadata".to_string()), "{out}");
    }
}

macro_rules! cases {
    ($($idx:expr => $m:ident, $s:ident;)*) => {$(
        #[tokio::test]
        async fn $m() { main_resource_does_not_own_status($idx).await; }
        #[tokio::test]
        async fn $s() { status_does_not_own_spec($idx).await; }
    )*};
}

cases! {
    0 => pv_main_does_not_own_status, pv_status_does_not_own_spec;
    1 => pvc_main_does_not_own_status, pvc_status_does_not_own_spec;
    2 => resourcequota_main_does_not_own_status, resourcequota_status_does_not_own_spec;
    3 => rc_main_does_not_own_status, rc_status_does_not_own_spec;
    4 => flowschema_main_does_not_own_status, flowschema_status_does_not_own_spec;
    5 => plc_main_does_not_own_status, plc_status_does_not_own_spec;
    6 => volumeattachment_main_does_not_own_status, volumeattachment_status_does_not_own_spec;
    7 => vap_main_does_not_own_status, vap_status_does_not_own_spec;
    8 => resourceclaim_main_does_not_own_status, resourceclaim_status_does_not_own_spec;
    9 => apiservice_main_does_not_own_status, apiservice_status_does_not_own_spec;
}
