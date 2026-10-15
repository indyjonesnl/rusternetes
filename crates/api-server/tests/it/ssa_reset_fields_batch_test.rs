//! Server-side apply honours `GetResetFields` for Job, CronJob, HPA, PDB, Pod,
//! Node and Namespace (#2226), as `ssa_reset_fields_test` does for ServiceCIDR.
//!
//! Upstream `pkg/registry/<group>/<kind>/strategy.go`: the main strategy
//! resets `status`, the `/status` strategy resets `spec` (Pod's `/status` also
//! `metadata.deletionTimestamp` and `metadata.ownerReferences`;
//! `core/pod/strategy.go:206-214`).

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
    /// Collection path without the namespace segment.
    prefix: &'static str,
    plural: &'static str,
    namespaced: bool,
    spec: Value,
    status: Value,
}

fn kinds() -> Vec<Kind> {
    let tmpl = json!({
        "metadata": {"labels": {"app": "rf"}},
        "spec": {"containers": [{"name": "c", "image": "busybox"}], "restartPolicy": "Never"}
    });
    vec![
        Kind {
            api_version: "batch/v1",
            kind: "Job",
            prefix: "/apis/batch/v1",
            plural: "jobs",
            namespaced: true,
            spec: json!({"template": tmpl}),
            status: json!({"active": 1}),
        },
        Kind {
            api_version: "batch/v1",
            kind: "CronJob",
            prefix: "/apis/batch/v1",
            plural: "cronjobs",
            namespaced: true,
            spec: json!({"schedule": "* * * * *", "jobTemplate": {"spec": {"template": tmpl}}}),
            status: json!({"lastScheduleTime": "2020-01-01T00:00:00Z"}),
        },
        Kind {
            api_version: "autoscaling/v2",
            kind: "HorizontalPodAutoscaler",
            prefix: "/apis/autoscaling/v2",
            plural: "horizontalpodautoscalers",
            namespaced: true,
            spec: json!({
                "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "d"},
                "minReplicas": 1, "maxReplicas": 3
            }),
            status: json!({"desiredReplicas": 2, "currentReplicas": 1}),
        },
        Kind {
            api_version: "policy/v1",
            kind: "PodDisruptionBudget",
            prefix: "/apis/policy/v1",
            plural: "poddisruptionbudgets",
            namespaced: true,
            spec: json!({"minAvailable": 1, "selector": {"matchLabels": {"app": "rf"}}}),
            status: json!({
                "currentHealthy": 1, "desiredHealthy": 1, "disruptionsAllowed": 0,
                "expectedPods": 1
            }),
        },
        Kind {
            api_version: "v1",
            kind: "Pod",
            prefix: "/api/v1",
            plural: "pods",
            namespaced: true,
            spec: json!({"containers": [{"name": "c", "image": "busybox"}]}),
            status: json!({"phase": "Running"}),
        },
        Kind {
            api_version: "v1",
            kind: "Node",
            prefix: "/api/v1",
            plural: "nodes",
            namespaced: false,
            spec: json!({"unschedulable": true}),
            status: json!({"phase": "Running"}),
        },
        Kind {
            api_version: "v1",
            kind: "Namespace",
            prefix: "/api/v1",
            plural: "namespaces",
            namespaced: false,
            spec: json!({"finalizers": ["kubernetes"]}),
            status: json!({"phase": "Active"}),
        },
    ]
}

fn body(k: &Kind, name: &str, with_status: bool) -> Value {
    let mut meta = json!({"name": name});
    if k.namespaced {
        meta["namespace"] = json!("default");
    }
    let mut b = json!({
        "apiVersion": k.api_version, "kind": k.kind, "metadata": meta, "spec": k.spec
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
    let uri = format!("{}/{name}", collection(k));
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
    let uri = format!("{}/{name}/status", collection(k));
    let (code, out) = apply(&api, &uri, "ctrl", &body(k, name, true)).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let owned = owned_keys(&out, "ctrl");
    assert!(owned.contains(&"f:status".to_string()), "{out}");
    assert!(!owned.contains(&"f:spec".to_string()), "{out}");
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
    0 => job_main_does_not_own_status, job_status_does_not_own_spec;
    1 => cronjob_main_does_not_own_status, cronjob_status_does_not_own_spec;
    2 => hpa_main_does_not_own_status, hpa_status_does_not_own_spec;
    3 => pdb_main_does_not_own_status, pdb_status_does_not_own_spec;
    4 => pod_main_does_not_own_status, pod_status_does_not_own_spec;
    5 => node_main_does_not_own_status, node_status_does_not_own_spec;
    6 => namespace_main_does_not_own_status, namespace_status_does_not_own_spec;
}

/// `core/pod/strategy.go:206-214`: `/status` also resets
/// `metadata.ownerReferences` (and `deletionTimestamp`).
#[tokio::test]
async fn pod_status_does_not_own_owner_references() {
    let k = &kinds()[4];
    let api = TestApiServer::new();
    let name = "rf-owner";
    let (code, out) = api.post(&collection(k), &body(k, name, false)).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let mut b = body(k, name, true);
    b["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "v1", "kind": "ConfigMap", "name": "o", "uid": "u-1"
    }]);
    let uri = format!("{}/{name}/status", collection(k));
    let (code, out) = apply(&api, &uri, "ctrl", &b).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let meta_owned = out["metadata"]["managedFields"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["manager"] == "ctrl"))
        .map(|e| e["fieldsV1"]["f:metadata"].clone())
        .unwrap_or(Value::Null);
    assert!(meta_owned.get("f:ownerReferences").is_none(), "{out}");
}
