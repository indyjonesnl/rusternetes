//! ResourceQuota admission on the Store-backed resources (#1992).
//!
//! Upstream's admission chain ends with ResourceQuota
//! (`pkg/kubeapiserver/options/plugins.go:106-110`); its `Validate`
//! (`apiserver/pkg/admission/plugin/resourcequota/admission.go:158-165`)
//! checks each request against `status.hard` / `status.used` and records
//! the usage it adds. These pin that for resources served through the
//! generic Store.

use axum::http::StatusCode;
use rusternetes_common::resources::{ResourceQuota, ResourceQuotaStatus};
use rusternetes_storage::Storage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::collections::HashMap;

const QUOTA_KEY: &str = "/registry/resourcequotas/default/quota";

fn list(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// A quota whose status the controller has already computed.
async fn seed_quota(api: &TestApiServer, hard: &[(&str, &str)], used: &[(&str, &str)]) {
    let mut q: ResourceQuota = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "ResourceQuota",
        "metadata": {"name": "quota", "namespace": "default"},
    }))
    .unwrap();
    q.spec.hard = Some(list(hard));
    q.status = Some(ResourceQuotaStatus {
        hard: Some(list(hard)),
        used: Some(list(used)),
    });
    api.storage.create(QUOTA_KEY, &q).await.unwrap();
}

async fn used(api: &TestApiServer) -> HashMap<String, String> {
    let q: ResourceQuota = api.storage.get(QUOTA_KEY).await.unwrap();
    q.status.unwrap().used.unwrap()
}

fn cm(name: &str) -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap",
           "metadata": {"name": name, "namespace": "default"}})
}

const CMS: &str = "/api/v1/namespaces/default/configmaps";

/// A create within quota is admitted and charged to `status.used`.
#[tokio::test]
async fn a_create_within_quota_is_charged() {
    let api = TestApiServer::new();
    seed_quota(&api, &[("configmaps", "2")], &[("configmaps", "1")]).await;
    let (status, body) = api.post(CMS, &cm("a")).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(used(&api).await["configmaps"], "2");
}

/// A create past the quota is 403 with upstream's message
/// (plugin/resourcequota/controller.go:619-625), and stores nothing.
#[tokio::test]
async fn a_create_past_quota_is_forbidden() {
    let api = TestApiServer::new();
    seed_quota(
        &api,
        &[("count/configmaps", "1")],
        &[("count/configmaps", "1")],
    )
    .await;
    let (status, body) = api.post(CMS, &cm("b")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(
        body["message"],
        "configmaps \"b\" is forbidden: exceeded quota: quota, requested: count/configmaps=1, used: count/configmaps=1, limited: count/configmaps=1",
        "{body}"
    );
    let (status, _) = api.get(&format!("{CMS}/b")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(used(&api).await["count/configmaps"], "1");
}

/// A quota whose usage is not computed yet rejects (`hasUsageStats`,
/// controller.go:763-777).
#[tokio::test]
async fn a_quota_without_usage_rejects() {
    let api = TestApiServer::new();
    seed_quota(&api, &[("count/deployments.apps", "5")], &[]).await;
    let d = json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "d", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "d"}},
            "template": {"metadata": {"labels": {"app": "d"}},
                         "spec": {"containers": [{"name": "c", "image": "i"}]}}
        }
    });
    let (status, body) = api
        .post("/apis/apps/v1/namespaces/default/deployments", &d)
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(
        body["message"],
        "deployments.apps \"d\" is forbidden: status unknown for quota: quota, resources: count/deployments.apps",
        "{body}"
    );
}

/// A dry run is checked but charges nothing (`checkQuotas`,
/// controller.go:250-254).
#[tokio::test]
async fn a_dry_run_is_not_charged() {
    let api = TestApiServer::new();
    seed_quota(&api, &[("configmaps", "2")], &[("configmaps", "1")]).await;
    let (status, body) = api.post(&format!("{CMS}?dryRun=All"), &cm("c")).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(used(&api).await["configmaps"], "1");
}

/// An object count is charged on create only: an update passes a full
/// quota.
#[tokio::test]
async fn an_update_is_not_charged_an_object_count() {
    let api = TestApiServer::new();
    let (status, body) = api.post(CMS, &cm("e")).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    seed_quota(&api, &[("configmaps", "1")], &[("configmaps", "1")]).await;
    let mut updated = body.clone();
    updated["data"] = json!({"k": "v"});
    let (status, body) = api.put(&format!("{CMS}/e"), &updated).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
