//! PersistentVolumeClaim and `persistentvolumeclaims/status` served through
//! the generic Store and endpoint handlers (#1990), with the in-tree
//! admission plugins that handle claims — LimitRanger and DefaultStorageClass
//! — running in the generic admission chain.
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin PersistentVolumeClaim's own:
//! `pkg/registry/core/persistentvolumeclaim/strategy.go`, its `StatusREST`
//! (storage/storage.go), `pkg/api/persistentvolumeclaim/util.go` and the two
//! plugins.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PVCS: &str = "/api/v1/namespaces/default/persistentvolumeclaims";
const SCS: &str = "/apis/storage.k8s.io/v1/storageclasses";

fn pvc(name: &str, storage: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": name},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": storage}}
        }
    })
}

async fn create(api: &TestApiServer, body: &Value) -> Value {
    let (status, out) = api.post(PVCS, body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    out
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

async fn storage_class(api: &TestApiServer, name: &str, default: bool) {
    let mut body = json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
        "metadata": {"name": name},
        "provisioner": "example.com/p"
    });
    if default {
        body["metadata"]["annotations"] =
            json!({"storageclass.kubernetes.io/is-default-class": "true"});
    }
    let (status, out) = api.post(SCS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
}

/// `PrepareForCreate` (strategy.go:65-79) clears status, which defaults back
/// to `Pending` (v1/defaults.go:294-298), and the volume mode defaults to
/// `Filesystem` (:299-304).
#[tokio::test]
async fn create_clears_status_and_defaults() {
    let api = TestApiServer::new();
    let mut body = pvc("c-create", "1Gi");
    body["status"] = json!({"phase": "Bound", "capacity": {"storage": "9Gi"}});
    let out = create(&api, &body).await;
    assert_eq!(out["status"], json!({"phase": "Pending"}), "{out}");
    assert_eq!(out["spec"]["volumeMode"], "Filesystem", "{out}");
}

/// `NormalizeDataSources` (pkg/api/persistentvolumeclaim/util.go:162-190): a
/// `dataSource` is mirrored into `dataSourceRef`.
#[tokio::test]
async fn a_data_source_is_mirrored_into_data_source_ref() {
    let api = TestApiServer::new();
    let mut body = pvc("c-ds", "1Gi");
    body["spec"]["dataSource"] = json!({"kind": "PersistentVolumeClaim", "name": "src"});
    let out = create(&api, &body).await;
    assert_eq!(
        out["spec"]["dataSourceRef"],
        json!({"kind": "PersistentVolumeClaim", "name": "src"}),
        "{out}"
    );
}

/// `GetWarningsForPersistentVolumeClaimSpec` (util.go:214-236).
#[tokio::test]
async fn a_fractional_byte_request_warns() {
    let api = TestApiServer::new();
    let (status, warnings, out) = send(&api, "POST", PVCS, Some(&pvc("c-frac", "200m"))).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("spec.resources.requests[storage]: fractional byte value")),
        "{warnings:?}"
    );
}

/// `DefaultStorageClass` (setdefault/admission.go) with `GetDefaultClass`
/// (pkg/volume/util/storageclass.go:40-71): a claim with no class gets the
/// newest default; one that names a class keeps it.
#[tokio::test]
async fn the_default_storage_class_is_applied() {
    let api = TestApiServer::new();
    storage_class(&api, "plain", false).await;
    storage_class(&api, "fast", true).await;
    let out = create(&api, &pvc("c-default", "1Gi")).await;
    assert_eq!(out["spec"]["storageClassName"], "fast", "{out}");

    let mut body = pvc("c-named", "1Gi");
    body["spec"]["storageClassName"] = json!("plain");
    let out = create(&api, &body).await;
    assert_eq!(out["spec"]["storageClassName"], "plain", "{out}");
}

/// `LimitRanger.Validate` (limitranger/admission.go:116-156) with
/// `PersistentVolumeClaimValidateLimitFunc` (:451-473) rejects a claim
/// outside the namespace's bounds, on create and on update, with
/// `admission.NewForbidden`'s message.
#[tokio::test]
async fn limit_ranges_bound_claims_on_create_and_update() {
    let api = TestApiServer::new();
    create(&api, &pvc("c-before", "20Gi")).await;
    let (status, out) = api
        .post(
            "/api/v1/namespaces/default/limitranges",
            &json!({
                "apiVersion": "v1", "kind": "LimitRange",
                "metadata": {"name": "lr"},
                "spec": {"limits": [{
                    "type": "PersistentVolumeClaim",
                    "min": {"storage": "1Gi"}, "max": {"storage": "10Gi"}
                }]}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let (status, out) = api.post(PVCS, &pvc("c-big", "100Gi")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{out}");
    assert_eq!(
        message(&out),
        r#"persistentvolumeclaims "c-big" is forbidden: maximum storage usage per PersistentVolumeClaim is 10Gi, but request is 100Gi"#,
        "{out}"
    );
    create(&api, &pvc("c-fits", "5Gi")).await;

    let (status, out) = api
        .patch(
            &format!("{PVCS}/c-before"),
            &json!({"metadata": {"labels": {"l": "v"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{out}");
    assert!(message(&out).contains("but request is 20Gi"), "{out}");
}

/// The status strategy (strategy.go:156-169) keeps spec and runs
/// `ValidatePersistentVolumeClaimStatusUpdate` (validation.go:2673-2714).
#[tokio::test]
async fn status_writes_keep_spec_and_are_validated() {
    let api = TestApiServer::new();
    let obj = create(&api, &pvc("c-status", "1Gi")).await;
    let uri = format!("{PVCS}/c-status/status");

    let mut body = obj.clone();
    body["spec"]["resources"]["requests"]["storage"] = json!("9Gi");
    body["status"] = json!({"phase": "Bound", "capacity": {"storage": "1Gi"}});
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["spec"]["resources"]["requests"]["storage"], "1Gi",
        "{out}"
    );
    assert_eq!(out["status"]["phase"], "Bound", "{out}");

    let (status, out) = api
        .patch(
            &uri,
            &json!({"status": {"allocatedResourceStatuses": {"storage": "Bogus"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("status.allocatedResourceStatuses"),
        "{out}"
    );
}

/// `ReturnDeletedObject: true` (storage/storage.go:53), and the cluster-wide
/// collection serves only LIST and WATCH (apiserver
/// pkg/endpoints/installer.go:589-596).
#[tokio::test]
async fn delete_returns_the_object_and_no_cluster_wide_deletecollection() {
    let api = TestApiServer::new();
    create(&api, &pvc("c-del", "1Gi")).await;
    let (status, _) = api.delete("/api/v1/persistentvolumeclaims").await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    let (status, out) = api.delete(&format!("{PVCS}/c-del")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "PersistentVolumeClaim", "{out}");
    assert_eq!(out["metadata"]["name"], "c-del", "{out}");
}

/// `StorageObjectInUseProtection` (storageobjectinuseprotection/admission.go
/// `admitPVC`): a created claim carries `kubernetes.io/pvc-protection`, and
/// deleting it only marks it terminating until the pvc-protection controller
/// releases it.
#[tokio::test]
async fn create_adds_the_pvc_protection_finalizer() {
    let api = TestApiServer::new();
    let out = create(&api, &pvc("c-protected", "1Gi")).await;
    assert_eq!(
        out["metadata"]["finalizers"],
        json!(["kubernetes.io/pvc-protection"]),
        "{out}"
    );
    let (status, _) = api.delete(&format!("{PVCS}/c-protected")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, out) = api.get(&format!("{PVCS}/c-protected")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(out["metadata"]["deletionTimestamp"].is_string(), "{out}");
}
