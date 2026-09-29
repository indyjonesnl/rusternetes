//! CronJob and `cronjobs/status` served through the generic Store and
//! endpoint handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin CronJob's own:
//! `pkg/registry/batch/cronjob/strategy.go`, its `StatusREST`
//! (storage/storage.go), and `ValidateCronJobCreate` / `ValidateCronJobUpdate`
//! (pkg/apis/batch/validation/validation.go:756-873).

use axum::http::StatusCode;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CJ: &str = "/apis/batch/v1/namespaces/default/cronjobs";

fn cron_job(name: &str) -> Value {
    json!({
        "apiVersion": "batch/v1", "kind": "CronJob",
        "metadata": {"name": name},
        "spec": {
            "schedule": "*/5 * * * *",
            "jobTemplate": {"spec": {"template": {"spec": {
                "containers": [{"name": "c", "image": "busybox"}],
                "restartPolicy": "OnFailure"
            }}}}
        }
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, out) = api.post(CJ, &cron_job(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {out}");
    out
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

async fn send(
    api: &TestApiServer,
    method: &str,
    uri: &str,
    body: &Value,
) -> (StatusCode, Vec<String>, Value) {
    let (status, headers, _, out) = api
        .send_full(
            method,
            uri,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(body).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_string))
        .collect();
    (status, warnings, out)
}

/// `PrepareForCreate` (strategy.go:87-93): a client-set status is dropped and
/// the generation starts at 1.
#[tokio::test]
async fn create_resets_status_and_generation() {
    let api = TestApiServer::new();
    let mut body = cron_job("c-create");
    body["status"] = json!({"lastScheduleTime": "2026-01-01T00:00:00Z"});
    body["metadata"]["generation"] = json!(7);
    let (status, out) = api.post(CJ, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["status"], json!({}), "{out}");
}

/// `PrepareForUpdate` (strategy.go:97-109): status is kept, and only a spec
/// change bumps the generation.
#[tokio::test]
async fn update_keeps_status_and_bumps_generation_on_spec() {
    let api = TestApiServer::new();
    let obj = create(&api, "c-update").await;
    let uri = format!("{CJ}/c-update");

    let mut touched = obj.clone();
    touched["metadata"]["annotations"] = json!({"a": "1"});
    touched["status"] = json!({"lastScheduleTime": "2026-01-01T00:00:00Z"});
    let (status, out) = api.put(&uri, &touched).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert!(out["status"].get("lastScheduleTime").is_none(), "{out}");

    let mut hourly = out;
    hourly["spec"]["schedule"] = json!("0 * * * *");
    let (status, out) = api.put(&uri, &hourly).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
}

/// `ValidateCronJobUpdate` (validation.go:772-780): PUT and PATCH are both
/// validated. The old update and patch handlers validated nothing.
#[tokio::test]
async fn updates_and_patches_are_validated() {
    let api = TestApiServer::new();
    let obj = create(&api, "c-validate").await;
    let uri = format!("{CJ}/c-validate");

    let mut bad = obj;
    bad["spec"]["schedule"] = json!("not a schedule");
    let (status, out) = api.put(&uri, &bad).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.schedule"), "{out}");

    let (status, out) = api
        .patch(&uri, &json!({"spec": {"concurrencyPolicy": "Sometimes"}}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.concurrencyPolicy"), "{out}");

    let (status, out) = api
        .patch(
            &uri,
            &json!({"spec": {"jobTemplate": {"spec": {"manualSelector": true}}}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("spec.jobTemplate.spec.manualSelector"),
        "{out}"
    );
}

/// validation.go:792-793 and :833-848: a CronJob stored with an inline TZ,
/// from before the rule, stays updatable, and every such update warns
/// (strategy.go:160-162).
#[tokio::test]
async fn an_inline_tz_is_grandfathered_and_warned_about() {
    let api = TestApiServer::new();
    let mut stored = cron_job("c-tz");
    stored["metadata"]["namespace"] = json!("default");
    stored["spec"]["schedule"] = json!("CRON_TZ=UTC */5 * * * *");
    let key = build_key("cronjobs", Some("default"), "c-tz");
    let stored: Value = api.storage.create(&key, &stored).await.unwrap();

    let mut update = stored;
    update["spec"]["suspend"] = json!(true);
    let (status, warnings, out) = send(&api, "PUT", &format!("{CJ}/c-tz"), &update).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("cannot use TZ or CRON_TZ in spec.spec.schedule")),
        "{warnings:?}"
    );
}

/// The 52-character cap is create-only (validation.go:762-768, :777-778).
#[tokio::test]
async fn the_name_cap_is_create_only() {
    let api = TestApiServer::new();
    let long = "c".repeat(53);
    let (status, out) = api.post(CJ, &cron_job(&long)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("must be no more than 52 characters"),
        "{out}"
    );

    let mut stored = cron_job(&long);
    stored["metadata"]["namespace"] = json!("default");
    let key = build_key("cronjobs", Some("default"), &long);
    api.storage.create::<Value>(&key, &stored).await.unwrap();
    let (status, out) = api
        .patch(&format!("{CJ}/{long}"), &json!({"spec": {"suspend": true}}))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
}

/// `cronJobStatusStrategy` (strategy.go:185-189): `/status` writes status and
/// keeps the spec.
#[tokio::test]
async fn status_writes_keep_spec() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "c-status").await;
    obj["spec"]["schedule"] = json!("0 * * * *");
    obj["status"] = json!({"lastScheduleTime": "2026-01-01T00:00:00Z"});
    let (status, out) = api.put(&format!("{CJ}/c-status/status"), &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["schedule"], "*/5 * * * *", "{out}");
    assert_eq!(
        out["status"]["lastScheduleTime"], "2026-01-01T00:00:00Z",
        "{out}"
    );
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
}

/// batch/v1 deletes dependents by default (strategy.go:52-65), so a plain
/// DELETE removes the CronJob with no `orphan` finalizer.
#[tokio::test]
async fn delete_does_not_orphan() {
    let api = TestApiServer::new();
    create(&api, "c-delete").await;
    let (status, out) = api.delete(&format!("{CJ}/c-delete")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (status, _) = api.get(&format!("{CJ}/c-delete")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
