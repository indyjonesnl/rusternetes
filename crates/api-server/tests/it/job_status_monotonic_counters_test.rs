//! Job status counters are monotonically non-decreasing.
//!
//! Upstream turns both rules on in the Job strategy
//! (`pkg/registry/batch/job/strategy.go:396-397`):
//!
//! ```text
//! // We allow to decrease the counter for succeeded pods for jobs which
//! // have equal parallelism and completions, as they can be scaled-down.
//! RejectDecreasingSucceededCounter: !isIndexed || !ptr.Equal(newJob.Spec.Completions, newJob.Spec.Parallelism),
//! RejectDecreasingFailedCounter:    true,
//! ```
//!
//! and enforces them in `ValidateJobStatusUpdate`
//! (`pkg/apis/batch/validation/validation.go:722-730`).
//!
//! We implemented neither, so a Job controller writing a decreasing counter was
//! accepted here while a real api-server refused it — which is exactly how
//! #1955 stayed green in-house and reddened only the vanilla-swap leg, where
//! the rejected write meant `Complete=True` never landed and the e2e spec hung
//! for its full 900s.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn jobs_uri() -> String {
    format!("/apis/batch/v1/namespaces/{NS}/jobs")
}

fn status_uri(name: &str) -> String {
    format!("/apis/batch/v1/namespaces/{NS}/jobs/{name}/status")
}

fn job(name: &str, completion_mode: Option<&str>, completions: i32, parallelism: i32) -> Value {
    let mut spec = json!({
        "completions": completions,
        "parallelism": parallelism,
        "template": {
            "metadata": {"labels": {"app": "j"}},
            "spec": {
                "restartPolicy": "Never",
                "containers": [{"name": "c", "image": "busybox"}]
            }
        }
    });
    if let Some(mode) = completion_mode {
        spec["completionMode"] = json!(mode);
    }
    json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name},
        "spec": spec
    })
}

/// Seed the stored status, then attempt the given status, returning the response.
async fn seed_then_put(
    state: &TestApiServer,
    name: &str,
    body: Value,
    seed: Value,
    attempt: Value,
) -> (StatusCode, Value) {
    let (code, resp) = state.post(&jobs_uri(), &body).await;
    assert_eq!(code, StatusCode::CREATED, "{resp}");

    let mut seeded = resp.clone();
    seeded["status"] = seed;
    let (code, resp) = state.put(&status_uri(name), &seeded).await;
    assert_eq!(
        code,
        StatusCode::OK,
        "seeding the status must succeed: {resp}"
    );

    let mut next = resp.clone();
    next["status"] = attempt;
    state.put(&status_uri(name), &next).await
}

#[tokio::test]
async fn failed_counter_cannot_decrease() {
    let state = TestApiServer::new();
    let (code, body) = seed_then_put(
        &state,
        "j-failed",
        job("j-failed", None, 3, 3),
        json!({"failed": 3, "succeeded": 0}),
        json!({"failed": 0, "succeeded": 0}),
    )
    .await;

    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["message"], "status.failed: Invalid value: 0: cannot decrease the failed counter",
        "the wording is the contract clients match on: {body}"
    );
}

#[tokio::test]
async fn failed_counter_may_increase_and_stay_equal() {
    let state = TestApiServer::new();
    let (code, body) = seed_then_put(
        &state,
        "j-grow",
        job("j-grow", None, 3, 3),
        json!({"failed": 1, "succeeded": 0}),
        json!({"failed": 4, "succeeded": 0}),
    )
    .await;

    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["failed"], json!(4), "{body}");
}

/// Upstream rejects a decreasing `succeeded` only when the Job is NOT an
/// Indexed job with `completions == parallelism` — those can be scaled down.
#[tokio::test]
async fn succeeded_counter_cannot_decrease_for_non_indexed_job() {
    let state = TestApiServer::new();
    let (code, body) = seed_then_put(
        &state,
        "j-succ",
        job("j-succ", None, 3, 3),
        json!({"failed": 0, "succeeded": 2}),
        json!({"failed": 0, "succeeded": 1}),
    )
    .await;

    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["message"],
        "status.succeeded: Invalid value: 1: cannot decrease the succeeded counter",
        "{body}"
    );
}

/// The scale-down carve-out: Indexed with completions == parallelism.
#[tokio::test]
async fn succeeded_counter_may_decrease_for_indexed_job_scaled_down() {
    let state = TestApiServer::new();
    let (code, body) = seed_then_put(
        &state,
        "j-idx",
        job("j-idx", Some("Indexed"), 3, 3),
        json!({"failed": 0, "succeeded": 2}),
        json!({"failed": 0, "succeeded": 1}),
    )
    .await;

    assert_eq!(
        code,
        StatusCode::OK,
        "an Indexed job with completions == parallelism can be scaled down: {body}"
    );
}

/// `failed` stays protected even for that Indexed carve-out — upstream sets
/// RejectDecreasingFailedCounter unconditionally. This is the #1955 shape.
#[tokio::test]
async fn failed_counter_cannot_decrease_even_for_indexed_job() {
    let state = TestApiServer::new();
    let (code, body) = seed_then_put(
        &state,
        "j-idx-failed",
        job("j-idx-failed", Some("Indexed"), 3, 3),
        json!({"failed": 3, "succeeded": 0}),
        json!({"failed": 0, "succeeded": 3}),
    )
    .await;

    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["message"], "status.failed: Invalid value: 0: cannot decrease the failed counter",
        "{body}"
    );
}
