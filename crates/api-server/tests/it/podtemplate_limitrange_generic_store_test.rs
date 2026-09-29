//! PodTemplate and LimitRange served through the generic Store and endpoint
//! handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin each resource's own:
//! `pkg/registry/core/{podtemplate,limitrange}/strategy.go`, their
//! `storage/storage.go`, and the v1 LimitRange defaulting.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PT: &str = "/api/v1/namespaces/default/podtemplates";
const LR: &str = "/api/v1/namespaces/default/limitranges";

fn pod_template(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PodTemplate",
        "metadata": {"name": name},
        "template": {
            "metadata": {"labels": {"app": name}},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]}
        }
    })
}

fn limit_range(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "LimitRange",
        "metadata": {"name": name},
        "spec": {"limits": [
            {"type": "Container", "max": {"cpu": "2"}, "min": {"memory": "64Mi"}}
        ]}
    })
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `PrepareForCreate` (podtemplate/strategy.go:48-52) starts the generation
/// at 1, and `PrepareForUpdate` (:77-89) bumps it only when the template
/// changes. The old handlers never set it.
#[tokio::test]
async fn a_pod_templates_generation_tracks_its_template() {
    let api = TestApiServer::new();
    let mut body = pod_template("t-gen");
    body["metadata"]["generation"] = json!(9);
    let (status, obj) = api.post(PT, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{obj}");
    assert_eq!(obj["metadata"]["generation"], 1, "{obj}");
    let uri = format!("{PT}/t-gen");

    let (status, out) = api
        .patch(&uri, &json!({"metadata": {"labels": {"l": "v"}}}))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");

    let mut reimaged = out;
    reimaged["template"]["spec"]["containers"][0]["image"] = json!("nginx:2");
    let (status, out) = api.put(&uri, &reimaged).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
}

/// `ValidatePodTemplateUpdate` (validation.go:6551-6555) holds a PATCH to the
/// template rules. The old PATCH handler validated nothing.
#[tokio::test]
async fn a_pod_template_patch_is_validated() {
    let api = TestApiServer::new();
    let (status, obj) = api.post(PT, &pod_template("t-patch")).await;
    assert_eq!(status, StatusCode::CREATED, "{obj}");
    let (status, out) = api
        .patch(
            &format!("{PT}/t-patch"),
            &json!({"template": {"spec": {"containers": [{"name": "Bad_Name", "image": "nginx"}]}}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("template.spec.containers[0].name"),
        "{out}"
    );
}

/// `ReturnDeletedObject: true` (podtemplate/storage/storage.go:47): DELETE
/// returns the PodTemplate.
#[tokio::test]
async fn a_pod_template_delete_returns_the_object() {
    let api = TestApiServer::new();
    let (status, obj) = api.post(PT, &pod_template("t-del")).await;
    assert_eq!(status, StatusCode::CREATED, "{obj}");
    let (status, out) = api.delete(&format!("{PT}/t-del")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "PodTemplate", "{out}");
    assert_eq!(out["metadata"]["name"], "t-del", "{out}");
}

/// `SetDefaults_LimitRangeItem` (pkg/apis/core/v1/defaults.go:360-390): a
/// Container item's default limit comes from `max`, its default request from
/// the default limit and then from `min`.
#[tokio::test]
async fn a_limit_range_is_defaulted() {
    let api = TestApiServer::new();
    let (status, out) = api.post(LR, &limit_range("l-defaults")).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let item = &out["spec"]["limits"][0];
    assert_eq!(item["default"], json!({"cpu": "2"}), "{out}");
    assert_eq!(
        item["defaultRequest"],
        json!({"cpu": "2", "memory": "64Mi"}),
        "{out}"
    );
}

/// `PrepareForCreate` (limitrange/strategy.go:44-49) names a LimitRange
/// created without a name with a UUID.
#[tokio::test]
async fn an_unnamed_limit_range_is_named() {
    let api = TestApiServer::new();
    let mut body = limit_range("");
    body["metadata"] = json!({});
    let (status, out) = api.post(LR, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let name = out["metadata"]["name"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(name).is_ok(), "{out}");
}

/// `AllowCreateOnUpdate` is true (limitrange/strategy.go:68-70): a PUT to a
/// LimitRange that does not exist creates it.
#[tokio::test]
async fn a_put_creates_a_limit_range() {
    let api = TestApiServer::new();
    let (status, out) = api.put(&format!("{LR}/l-put"), &limit_range("l-put")).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let (status, _) = api.get(&format!("{LR}/l-put")).await;
    assert_eq!(status, StatusCode::OK);
}

/// `ValidateUpdate` is `ValidateLimitRange` (limitrange/strategy.go:72-76),
/// for a PATCH too.
#[tokio::test]
async fn a_limit_range_patch_is_validated() {
    let api = TestApiServer::new();
    let (status, out) = api.post(LR, &limit_range("l-patch")).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let (status, out) = api
        .patch(
            &format!("{LR}/l-patch"),
            &json!({"spec": {"limits": [{"type": "Pod"}, {"type": "Pod"}]}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("spec.limits[1].type: Duplicate"),
        "{out}"
    );
}
