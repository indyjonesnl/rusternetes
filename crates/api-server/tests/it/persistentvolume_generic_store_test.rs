//! PersistentVolume and `persistentvolumes/status` served through the generic
//! Store and endpoint handlers (#1990) — the first cluster-scoped resource on
//! them.
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin PersistentVolume's own:
//! `pkg/registry/core/persistentvolume/strategy.go`, its `StatusREST`
//! (storage/storage.go) and the core and volume validators.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PVS: &str = "/api/v1/persistentvolumes";

fn pv(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": name},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {"path": format!("/tmp/{name}")}
        }
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(PVS, &pv(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

async fn send(
    api: &TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Vec<String>, Value) {
    let (status, headers, _, out) = api
        .send_full(
            method,
            uri,
            body.map(|_| "application/json"),
            None,
            body.map(|b| serde_json::to_vec(b).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_string))
        .collect();
    (status, warnings, out)
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `PrepareForCreate` (strategy.go:66-74) resets status to `Pending` with a
/// transition time, and `SetDefaults_PersistentVolume` (v1/defaults.go:282-293)
/// fills the reclaim policy and volume mode.
#[tokio::test]
async fn create_resets_status_to_pending_and_defaults() {
    let api = TestApiServer::new();
    let mut body = pv("pv-create");
    body["status"] = json!({"phase": "Bound"});
    let (status, out) = api.post(PVS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["status"]["phase"], "Pending", "{out}");
    assert!(
        out["status"]["lastPhaseTransitionTime"].is_string(),
        "{out}"
    );
    assert_eq!(
        out["spec"]["persistentVolumeReclaimPolicy"], "Retain",
        "{out}"
    );
    assert_eq!(out["spec"]["volumeMode"], "Filesystem", "{out}");
}

/// `BeforeCreate` (apiserver/pkg/registry/rest/create.go:116) and
/// `EnsureObjectNamespaceMatchesRequestNamespace` (rest/meta.go:59-62): a
/// cluster-scoped object's namespace is cleared.
#[tokio::test]
async fn create_clears_a_namespace() {
    let api = TestApiServer::new();
    let mut body = pv("pv-ns");
    body["metadata"]["namespace"] = json!("default");
    let (status, out) = api.post(PVS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(out["metadata"].get("namespace").is_none(), "{out}");
}

/// `volumevalidation.ValidatePersistentVolume`
/// (pkg/volume/validation/pv_validation.go:30-59): a hostPath volume may not
/// carry the mount-options annotation.
#[tokio::test]
async fn mount_options_annotation_is_forbidden_for_host_path() {
    let api = TestApiServer::new();
    let mut body = pv("pv-mount");
    body["metadata"]["annotations"] = json!({"volume.beta.kubernetes.io/mount-options": "ro"});
    let (status, out) = api.post(PVS, &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("may not specify mount options for this volume type"),
        "{out}"
    );
}

/// `GetWarningsForPersistentVolume` (pkg/api/persistentvolume/util.go:94-96):
/// the Recycle reclaim policy is deprecated.
#[tokio::test]
async fn recycle_warns() {
    let api = TestApiServer::new();
    let mut body = pv("pv-recycle");
    body["spec"]["persistentVolumeReclaimPolicy"] = json!("Recycle");
    let (status, warnings, out) = send(&api, "POST", PVS, Some(&body)).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("The Recycle reclaim policy is deprecated")),
        "{warnings:?}"
    );
}

/// `warningsForPersistentVolumeSpecAndMeta`
/// (pkg/api/persistentvolume/util.go:84-90) through
/// `GetWarningsForNodeSelectorTerm` (pkg/api/node/util.go:95-125): a deprecated
/// node label in `spec.nodeAffinity.required.nodeSelectorTerms` warns.
#[tokio::test]
async fn deprecated_node_label_in_node_affinity_warns() {
    let api = TestApiServer::new();
    let mut body = pv("pv-nodeaffinity");
    body["spec"]["nodeAffinity"] = json!({"required": {"nodeSelectorTerms": [
        {"matchExpressions": [{"key": "ok", "operator": "Exists"}]},
        {"matchExpressions": [
            {"key": "ok", "operator": "Exists"},
            {"key": "beta.kubernetes.io/arch", "operator": "In", "values": ["amd64"]}
        ]}
    ]}});
    let (status, warnings, out) = send(&api, "POST", PVS, Some(&body)).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let want = r#"spec.nodeAffinity.required.nodeSelectorTerms[1].matchExpressions[1].key: beta.kubernetes.io/arch is deprecated since v1.14; use \"kubernetes.io/arch\" instead"#;
    assert!(warnings.iter().any(|w| w.contains(want)), "{warnings:?}");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
}

/// `ValidatePersistentVolumeUpdate` (validation.go:2271-2306) holds a PATCH:
/// the volume source is immutable.
#[tokio::test]
async fn a_patch_is_validated() {
    let api = TestApiServer::new();
    create(&api, "pv-patch").await;
    let (status, out) = api
        .patch(
            &format!("{PVS}/pv-patch"),
            &json!({"spec": {"hostPath": {"path": "/tmp/elsewhere"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("spec.persistentvolumesource is immutable after creation"),
        "{out}"
    );
}

/// `PrepareForUpdate` (strategy.go:97-102) keeps status on a spec write; the
/// status strategy (:143-159) keeps spec and stamps a phase change.
#[tokio::test]
async fn spec_and_status_writes_keep_the_other_half() {
    let api = TestApiServer::new();
    let obj = create(&api, "pv-halves").await;
    let uri = format!("{PVS}/pv-halves");

    let mut body = obj;
    body["spec"]["capacity"] = json!({"storage": "5Gi"});
    body["status"] = json!({"phase": "Available"});
    let (status, out) = api.put(&format!("{uri}/status"), &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["capacity"]["storage"], "1Gi", "{out}");
    assert_eq!(out["status"]["phase"], "Available", "{out}");
    assert!(
        out["status"]["lastPhaseTransitionTime"].is_string(),
        "{out}"
    );

    let mut body = out;
    body["spec"]["capacity"] = json!({"storage": "2Gi"});
    body["status"] = json!({"phase": "Failed"});
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["capacity"]["storage"], "2Gi", "{out}");
    assert_eq!(out["status"]["phase"], "Available", "{out}");
}

/// `ReturnDeletedObject: true` (storage/storage.go:52): DELETE returns the
/// PersistentVolume.
#[tokio::test]
async fn delete_returns_the_object() {
    let api = TestApiServer::new();
    create(&api, "pv-del").await;
    let (status, out) = api.delete(&format!("{PVS}/pv-del")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "PersistentVolume", "{out}");
    assert_eq!(out["metadata"]["name"], "pv-del", "{out}");
}
