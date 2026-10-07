//! #2386: how a terminating pod behaves across the DELETEs and writes that
//! follow the first graceful DELETE, now that Pod is on the generic Store.
//!
//! A leader-election client (client-go `ReleaseOnCancel`) can only release its
//! Lease if the pod keeps its grace window long enough to receive SIGTERM and
//! run the release before SIGKILL. That window is `deletionGracePeriodSeconds`,
//! and `deletionTimestamp` is its deadline. These tests pin the api-server half
//! of that contract:
//!
//! - `BeforeDelete`, staging/src/k8s.io/apiserver/pkg/registry/rest/delete.go:
//!   an already-terminating object's grace period may only be SHORTENED,
//!   never extended; a larger or equal request is "graceful deletion pending"
//!   and changes nothing.
//! - the kubelet's final DELETE (grace 0, kubelet status manager
//!   `pkg/kubelet/status/status_manager.go` `syncPod`) removes the pod only
//!   once nothing else holds it (store.go `Delete`).
//! - a status write by the kubelet must not disturb the stamped window.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PODS: &str = "/api/v1/namespaces/default/pods";

fn scheduled_pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name},
        "spec": {"nodeName": "node-1", "containers": [{"name": "c", "image": "busybox"}]}
    })
}

async fn create(api: &TestApiServer, body: &Value) {
    let (s, created) = api.post(PODS, body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
}

async fn get(api: &TestApiServer, name: &str) -> Value {
    let (s, stored) = api.get(&format!("{PODS}/{name}")).await;
    assert_eq!(s, StatusCode::OK, "{stored}");
    stored
}

fn secs_ahead(stored: &Value) -> i64 {
    let ts = chrono::DateTime::parse_from_rfc3339(
        stored["metadata"]["deletionTimestamp"]
            .as_str()
            .expect("deletionTimestamp"),
    )
    .unwrap();
    (ts.with_timezone(&chrono::Utc) - chrono::Utc::now()).num_seconds()
}

/// A second default DELETE (e.g. a controller retry) must not push the
/// deadline out.
#[tokio::test]
async fn a_repeated_delete_does_not_extend_the_grace_window() {
    let api = TestApiServer::new();
    create(&api, &scheduled_pod("p")).await;
    let (s, _) = api.delete(&format!("{PODS}/p")).await;
    assert_eq!(s, StatusCode::OK);
    let first = get(&api, "p").await;

    let (s, body) = api.delete(&format!("{PODS}/p")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let second = get(&api, "p").await;
    assert_eq!(
        first["metadata"]["deletionTimestamp"], second["metadata"]["deletionTimestamp"],
        "{second}"
    );
    assert_eq!(second["metadata"]["deletionGracePeriodSeconds"], 30);
}

/// A larger explicit grace period cannot lengthen a pending one.
#[tokio::test]
async fn a_longer_grace_period_cannot_extend_a_pending_delete() {
    let api = TestApiServer::new();
    create(&api, &scheduled_pod("p")).await;
    api.delete(&format!("{PODS}/p")).await;
    let first = get(&api, "p").await;

    let (s, body) = api
        .delete(&format!("{PODS}/p?gracePeriodSeconds=300"))
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let second = get(&api, "p").await;
    assert_eq!(
        first["metadata"]["deletionTimestamp"], second["metadata"]["deletionTimestamp"],
        "{second}"
    );
    assert_eq!(second["metadata"]["deletionGracePeriodSeconds"], 30);
}

/// A shorter grace period pulls the deadline in.
#[tokio::test]
async fn a_shorter_grace_period_shortens_a_pending_delete() {
    let api = TestApiServer::new();
    create(&api, &scheduled_pod("p")).await;
    api.delete(&format!("{PODS}/p")).await;

    let (s, body) = api.delete(&format!("{PODS}/p?gracePeriodSeconds=5")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let stored = get(&api, "p").await;
    assert_eq!(stored["metadata"]["deletionGracePeriodSeconds"], 5);
    let ahead = secs_ahead(&stored);
    assert!(
        (3..=6).contains(&ahead),
        "deadline {ahead}s ahead: {stored}"
    );
}

/// The kubelet's final DELETE (grace 0) removes a terminating pod that has
/// nothing else holding it.
#[tokio::test]
async fn the_kubelets_final_delete_removes_a_terminating_pod() {
    let api = TestApiServer::new();
    create(&api, &scheduled_pod("p")).await;
    api.delete(&format!("{PODS}/p")).await;
    assert!(get(&api, "p").await["metadata"]["deletionTimestamp"].is_string());

    let (s, body) = api.delete(&format!("{PODS}/p?gracePeriodSeconds=0")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, _) = api.get(&format!("{PODS}/p")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// ...but a pod another party still holds by finalizer stays, terminating.
#[tokio::test]
async fn the_final_delete_keeps_a_pod_held_by_a_finalizer() {
    let api = TestApiServer::new();
    let mut body = scheduled_pod("p");
    body["metadata"]["finalizers"] = json!(["example.com/hold"]);
    create(&api, &body).await;
    api.delete(&format!("{PODS}/p")).await;

    let (s, resp) = api.delete(&format!("{PODS}/p?gracePeriodSeconds=0")).await;
    assert_eq!(s, StatusCode::OK, "{resp}");
    let stored = get(&api, "p").await;
    assert!(
        stored["metadata"]["deletionTimestamp"].is_string(),
        "{stored}"
    );
}

/// Kubelet status writes on a terminating pod keep the stamped window.
#[tokio::test]
async fn a_status_write_keeps_the_grace_window() {
    let api = TestApiServer::new();
    create(&api, &scheduled_pod("p")).await;
    api.delete(&format!("{PODS}/p")).await;
    let mut stored = get(&api, "p").await;
    let ts = stored["metadata"]["deletionTimestamp"].clone();

    stored["status"]["phase"] = json!("Running");
    stored["metadata"]["deletionTimestamp"] = json!(null);
    stored["metadata"]["deletionGracePeriodSeconds"] = json!(null);
    let (s, body) = api.put(&format!("{PODS}/p/status"), &stored).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let after = get(&api, "p").await;
    assert_eq!(after["status"]["phase"], "Running", "{after}");
    assert_eq!(after["metadata"]["deletionTimestamp"], ts, "{after}");
    assert_eq!(after["metadata"]["deletionGracePeriodSeconds"], 30);
}

/// A whole-object PUT cannot un-delete a terminating pod either.
#[tokio::test]
async fn a_put_cannot_clear_the_deletion_timestamp() {
    let api = TestApiServer::new();
    create(&api, &scheduled_pod("p")).await;
    api.delete(&format!("{PODS}/p")).await;
    let mut stored = get(&api, "p").await;
    let ts = stored["metadata"]["deletionTimestamp"].clone();

    stored["metadata"]["deletionTimestamp"] = json!(null);
    stored["metadata"]["labels"] = json!({"a": "b"});
    let (_s, _body) = api.put(&format!("{PODS}/p"), &stored).await;
    let after = get(&api, "p").await;
    assert_eq!(after["metadata"]["deletionTimestamp"], ts, "{after}");
}

/// `kubectl delete --force --grace-period=0` removes a running pod at once.
#[tokio::test]
async fn a_forced_delete_removes_a_running_pod_at_once() {
    let api = TestApiServer::new();
    create(&api, &scheduled_pod("p")).await;
    let (s, body) = api.delete(&format!("{PODS}/p?gracePeriodSeconds=0")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, _) = api.get(&format!("{PODS}/p")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}
