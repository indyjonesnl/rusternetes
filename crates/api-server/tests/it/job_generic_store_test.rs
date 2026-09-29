//! Job and `jobs/status` served through the generic Store and endpoint
//! handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin Job's own:
//! `pkg/registry/batch/job/strategy.go`, its `REST` and `StatusREST`
//! (storage/storage.go), and the batch validators.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const JOBS: &str = "/apis/batch/v1/namespaces/default/jobs";

fn job(name: &str) -> Value {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name},
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "c", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    })
}

async fn create(api: &TestApiServer, body: &Value) -> Value {
    let (status, out) = api.post(JOBS, body).await;
    assert_eq!(status, StatusCode::CREATED, "create: {out}");
    out
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
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

/// `PrepareForCreate` (strategy.go:94-129) and `generateSelector`
/// (:230-278): the selector is generated from the new UID, the template gets
/// both label generations, status is cleared and the generation starts at 1.
#[tokio::test]
async fn create_generates_the_selector_and_resets_status() {
    let api = TestApiServer::new();
    let mut body = job("j-create");
    body["status"] = json!({"succeeded": 4});
    body["metadata"]["generation"] = json!(9);
    let out = create(&api, &body).await;
    let uid = out["metadata"]["uid"].as_str().unwrap();
    assert_eq!(
        out["spec"]["selector"],
        json!({"matchLabels": {"batch.kubernetes.io/controller-uid": uid}}),
        "{out}"
    );
    let labels = &out["spec"]["template"]["metadata"]["labels"];
    for (key, value) in [
        ("controller-uid", uid),
        ("batch.kubernetes.io/controller-uid", uid),
        ("job-name", "j-create"),
        ("batch.kubernetes.io/job-name", "j-create"),
    ] {
        assert_eq!(labels[key], value, "{key}: {out}");
    }
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert!(out["status"].get("succeeded").is_none(), "{out}");
}

/// `WarningsOnCreate` (strategy.go:210-218): a name that is a subdomain but
/// not a DNS label is accepted, with a warning.
#[tokio::test]
async fn create_warns_about_a_name_that_is_not_a_dns_label() {
    let api = TestApiServer::new();
    let (status, warnings, out) = send(&api, "POST", JOBS, Some(&job("j.dotted"))).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("metadata.name: this is used in Pod names and hostnames")),
        "{warnings:?}"
    );
}

/// `PrepareForUpdate` (strategy.go:132-166): status is kept, and a spec
/// change bumps the generation. The old PUT stored the client's status and
/// never bumped it.
#[tokio::test]
async fn update_keeps_status_and_bumps_generation_on_spec() {
    let api = TestApiServer::new();
    let obj = create(&api, &job("j-update")).await;
    let uri = format!("{JOBS}/j-update");

    let mut touched = obj.clone();
    touched["metadata"]["annotations"] = json!({"a": "1"});
    touched["status"] = json!({"succeeded": 3});
    let (status, out) = api.put(&uri, &touched).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert!(out["status"].get("succeeded").is_none(), "{out}");

    let mut wider = out;
    wider["spec"]["parallelism"] = json!(3);
    let (status, out) = api.put(&uri, &wider).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
}

/// `validatePodTemplateUpdate` (validation.go:637-676): a running Job's
/// template is immutable, and PATCH is held to it like PUT. The old PATCH
/// handler ran no update validation.
#[tokio::test]
async fn patch_cannot_change_a_running_jobs_template() {
    let api = TestApiServer::new();
    create(&api, &job("j-patch")).await;
    let uri = format!("{JOBS}/j-patch");
    let (status, out) = api
        .patch(
            &uri,
            &json!({"spec": {"template": {"spec": {"containers": [{"name": "c", "image": "other"}]}}}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.template"), "{out}");
    assert!(message(&out).contains("field is immutable"), "{out}");
}

/// `validationOptionsForJob` (strategy.go:175-207): the scheduling
/// directives of a suspended Job that never started are mutable.
#[tokio::test]
async fn a_suspended_unstarted_jobs_node_selector_is_mutable() {
    let api = TestApiServer::new();
    let mut body = job("j-suspended");
    body["spec"]["suspend"] = json!(true);
    create(&api, &body).await;
    let (status, out) = api
        .patch(
            &format!("{JOBS}/j-suspended"),
            &json!({"spec": {"template": {"spec": {"nodeSelector": {"disk": "ssd"}}}}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["spec"]["template"]["spec"]["nodeSelector"]["disk"], "ssd",
        "{out}"
    );
}

/// `validateCompletions` (validation.go:902-923): a non-indexed Job's
/// completions are immutable.
#[tokio::test]
async fn non_indexed_completions_are_immutable() {
    let api = TestApiServer::new();
    create(&api, &job("j-completions")).await;
    let (status, out) = api
        .patch(
            &format!("{JOBS}/j-completions"),
            &json!({"spec": {"completions": 5}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.completions"), "{out}");
}

/// `jobStatusStrategy` (strategy.go:318-435): `/status` keeps the spec and
/// refuses a decreasing counter.
#[tokio::test]
async fn status_writes_keep_spec_and_validate() {
    let api = TestApiServer::new();
    let mut obj = create(&api, &job("j-status")).await;
    obj["spec"]["parallelism"] = json!(7);
    obj["status"] = json!({"failed": 2});
    let uri = format!("{JOBS}/j-status/status");
    let (status, out) = api.put(&uri, &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["parallelism"], 1, "{out}");
    assert_eq!(out["status"]["failed"], 2, "{out}");

    let (status, out) = api.patch(&uri, &json!({"status": {"failed": 1}})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("cannot decrease the failed counter"),
        "{out}"
    );
}

/// batch/v1 deletes orphan by default (strategy.go:62-74), and `REST.Delete`
/// warns when the client left the policy unset (storage/storage.go:100-110).
/// An explicit policy is not warned about.
#[tokio::test]
async fn delete_orphans_by_default_and_warns() {
    let api = TestApiServer::new();
    create(&api, &job("j-orphan")).await;
    let uri = format!("{JOBS}/j-orphan");
    let (status, warnings, out) = send(&api, "DELETE", &uri, None).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("child pods are preserved by default when jobs are deleted")),
        "{warnings:?}"
    );
    let (status, got) = api.get(&uri).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["metadata"]["finalizers"], json!(["orphan"]), "{got}");
    assert!(got["metadata"]["deletionTimestamp"].is_string(), "{got}");

    create(&api, &job("j-background")).await;
    let uri = format!("{JOBS}/j-background?propagationPolicy=Background");
    let (status, warnings, out) = send(&api, "DELETE", &uri, None).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(
        !warnings
            .iter()
            .any(|w| w.contains("child pods are preserved")),
        "{warnings:?}"
    );
    let (status, _) = api.get(&format!("{JOBS}/j-background")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
