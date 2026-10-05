//! Eviction `deleteOptions.gracePeriodSeconds`.
//!
//! Upstream's `ValidateDeleteOptions`
//! (apimachinery/pkg/apis/meta/v1/validation/validation.go:157) does not
//! check the grace period, so a negative one is not rejected: the pod's
//! `CheckGracefulDelete` treats it as 1 second
//! (pkg/registry/core/pod/strategy.go:166-197). The first version of this test
//! asserted a 422 the old hand-written handler made up (#2145).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn pods_uri() -> String {
    format!("/api/v1/namespaces/{NS}/pods")
}
fn eviction_uri(pod: &str) -> String {
    format!("{}/{pod}/eviction", pods_uri())
}

fn pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    })
}

fn eviction(pod: &str, grace: i64) -> Value {
    json!({
        "apiVersion": "policy/v1", "kind": "Eviction",
        "metadata": {"name": pod, "namespace": NS},
        "deleteOptions": {"gracePeriodSeconds": grace}
    })
}

#[tokio::test]
async fn eviction_treats_a_negative_grace_period_as_one_second() {
    let state = TestApiServer::new();
    let name = "p-evict";
    let (code, _) = state.post(&pods_uri(), &pod(name)).await;
    assert_eq!(code, StatusCode::CREATED, "pod create must succeed");

    let (code, body) = state.post(&eviction_uri(name), &eviction(name, -5)).await;
    assert_eq!(
        code,
        StatusCode::CREATED,
        "a negative gracePeriodSeconds is not rejected: {body}"
    );
}

#[tokio::test]
async fn eviction_accepts_nonnegative_grace_period() {
    let state = TestApiServer::new();
    let name = "p-evict-ok";
    let (code, _) = state.post(&pods_uri(), &pod(name)).await;
    assert_eq!(code, StatusCode::CREATED);

    // No PDB → eviction is allowed; a valid gracePeriod must not be rejected.
    let (code, body) = state.post(&eviction_uri(name), &eviction(name, 30)).await;
    assert!(
        code == StatusCode::OK || code == StatusCode::CREATED,
        "valid eviction must succeed: {code} {body}"
    );
}
