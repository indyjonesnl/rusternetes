//! Mirror of the Terminating / NotTerminating `ResourceQuota` scope
//! conformance case, which had no mirror at all (#1770).
//!
//! Upstream (`release-1.35`): `test/e2e/apimachinery/resource_quota.go:754`
//! — "should verify ResourceQuota with terminating scopes."
//!
//! The scope keys off `spec.activeDeadlineSeconds`
//! (`pkg/quota/v1/evaluator/core/pods.go:356-359`, `podMatchesScopeFunc`:
//! `Terminating` matches `IsTerminating(pod)` at :417, i.e. a non-nil
//! `activeDeadlineSeconds >= 0`), not off the pod's phase — the phase-based
//! exclusion is a different mechanism, mirrored by
//! `resource_quota_terminal_pods_not_counted`.
//!
//! Upstream's `waitForResourceQuota` (:2495) polls until `status.used`
//! matches, and `ensureResourceQuota` (:2506) asserts it *stays* matching for
//! 10s. The mirror drives one `ResourceQuotaController::reconcile_one` pass
//! and reads the quota back over HTTP for both, the same substitution the
//! object-count mirrors make. `haveUsedResources` (:2513) compares only the
//! expected keys, with `Quantity.Equal`, so the mirror does too.

use rusternetes_common::quantity::Quantity;
use rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

const QUOTA_TERMINATING: &str = "quota-terminating";
const QUOTA_NOT_TERMINATING: &str = "quota-not-terminating";

/// `newTestResourceQuotaWithScope` (resource_quota.go:2097-2111) for the
/// Terminating / NotTerminating arm.
fn new_test_resource_quota_with_scope(name: &str, scope: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": name },
        "spec": {
            "hard": {
                "pods": "5",
                "requests.cpu": "1",
                "requests.memory": "500Mi",
                "limits.cpu": "2",
                "limits.memory": "1Gi"
            },
            "scopes": [scope]
        }
    })
}

/// `newTestPodForQuota` (resource_quota.go:2188-2211).
fn new_test_pod_for_quota(name: &str, active_deadline_seconds: Option<i64>) -> Value {
    let mut pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name },
        "spec": {
            "nodeSelector": { "x-test.k8s.io/unsatisfiable": "not-schedulable" },
            "containers": [{
                "name": "pause",
                "image": "registry.k8s.io/pause:3.10.1",
                "resources": {
                    "requests": { "cpu": "500m", "memory": "200Mi" },
                    "limits": { "cpu": "1", "memory": "400Mi" }
                }
            }]
        }
    });
    if let Some(s) = active_deadline_seconds {
        pod["spec"]["activeDeadlineSeconds"] = json!(s);
    }
    pod
}

async fn send(api: &TestApiServer, method: &str, uri: &str, body: Option<&Value>) -> (u16, Value) {
    let (status, value) = api.send(method, uri, Some("application/json"), body).await;
    (status.as_u16(), value)
}

/// One reconcile of `quota`, then `haveUsedResources`
/// (resource_quota.go:2513-2535).
async fn wait_for_resource_quota(
    api: &TestApiServer,
    mem: &Arc<MemoryStorage>,
    ns: &str,
    quota: &str,
    used: &[(&str, &str)],
) {
    ResourceQuotaController::new(mem.clone())
        .reconcile_one(ns, quota)
        .await
        .expect("reconcile quota");
    let (s, body) = send(
        api,
        "GET",
        &format!("/api/v1/namespaces/{ns}/resourcequotas/{quota}"),
        None,
    )
    .await;
    assert_eq!(s, 200, "get quota {quota}: {body}");
    for (key, want) in used {
        let got = body["status"]["used"][*key]
            .as_str()
            .unwrap_or_else(|| panic!("{quota}: status.used[{key}] missing: {body}"));
        let (g, w) = (
            Quantity::parse(got).expect("used quantity"),
            Quantity::parse(want).expect("want quantity"),
        );
        assert!(
            g.value_eq(&w),
            "{quota}: status.used[{key}] = {got}, want {want}: {body}"
        );
    }
}

/// The five keys upstream asserts, all at the same "pod captured" or
/// "released" level.
fn usage(pods: &'static str, captured: bool) -> Vec<(&'static str, &'static str)> {
    let (rc, rm, lc, lm) = if captured {
        ("500m", "200Mi", "1", "400Mi")
    } else {
        ("0", "0", "0", "0")
    };
    vec![
        ("pods", pods),
        ("requests.cpu", rc),
        ("requests.memory", rm),
        ("limits.cpu", lc),
        ("limits.memory", lm),
    ]
}

/// resource_quota.go:754 — should verify ResourceQuota with terminating
/// scopes.
#[tokio::test]
async fn resource_quota_verifies_terminating_scopes() {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    let ns = "rq-terminating-scope";
    let quotas = format!("/api/v1/namespaces/{ns}/resourcequotas");
    let pods = format!("/api/v1/namespaces/{ns}/pods");
    // `metav1.NewDeleteOptions(0)`.
    let delete_now = json!({
        "apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 0
    });

    // "Creating a ResourceQuota with terminating scope" (:755-758).
    let (s, b) = send(
        &api,
        "POST",
        &quotas,
        Some(&new_test_resource_quota_with_scope(
            QUOTA_TERMINATING,
            "Terminating",
        )),
    )
    .await;
    assert_eq!(s, 201, "create terminating quota: {b}");
    // "Ensuring ResourceQuota status is calculated" (:760-764).
    wait_for_resource_quota(&api, &mem, ns, QUOTA_TERMINATING, &[("pods", "0")]).await;

    // "Creating a ResourceQuota with not terminating scope" (:766-769).
    let (s, b) = send(
        &api,
        "POST",
        &quotas,
        Some(&new_test_resource_quota_with_scope(
            QUOTA_NOT_TERMINATING,
            "NotTerminating",
        )),
    )
    .await;
    assert_eq!(s, 201, "create not-terminating quota: {b}");
    // "Ensuring ResourceQuota status is calculated" (:771-773).
    wait_for_resource_quota(&api, &mem, ns, QUOTA_NOT_TERMINATING, &[("pods", "0")]).await;

    // "Creating a long running pod" (:775-785): no activeDeadlineSeconds.
    let (s, b) = send(
        &api,
        "POST",
        &pods,
        Some(&new_test_pod_for_quota("test-pod", None)),
    )
    .await;
    assert_eq!(s, 201, "create long running pod: {b}");

    // "Ensuring resource quota with not terminating scope captures the pod
    // usage" (:787-794).
    wait_for_resource_quota(&api, &mem, ns, QUOTA_NOT_TERMINATING, &usage("1", true)).await;
    // "Ensuring resource quota with terminating scope ignored the pod usage"
    // (:796-803).
    wait_for_resource_quota(&api, &mem, ns, QUOTA_TERMINATING, &usage("0", false)).await;

    // "Deleting the pod" (:805-807).
    let (s, b) = send(
        &api,
        "DELETE",
        &format!("{pods}/test-pod"),
        Some(&delete_now),
    )
    .await;
    assert_eq!(s, 200, "delete long running pod: {b}");

    // "Ensuring resource quota status released the pod usage" (:809-816).
    wait_for_resource_quota(&api, &mem, ns, QUOTA_NOT_TERMINATING, &usage("0", false)).await;

    // "Creating a terminating pod" (:818-824): activeDeadlineSeconds = 3600.
    let (s, b) = send(
        &api,
        "POST",
        &pods,
        Some(&new_test_pod_for_quota("terminating-pod", Some(3600))),
    )
    .await;
    assert_eq!(s, 201, "create terminating pod: {b}");

    // "Ensuring resource quota with terminating scope captures the pod usage"
    // (:826-833).
    wait_for_resource_quota(&api, &mem, ns, QUOTA_TERMINATING, &usage("1", true)).await;
    // "Ensuring resource quota with not terminating scope ignored the pod
    // usage" (:835-844).
    wait_for_resource_quota(&api, &mem, ns, QUOTA_NOT_TERMINATING, &usage("0", false)).await;

    // "Deleting the pod" (:846-848).
    let (s, b) = send(
        &api,
        "DELETE",
        &format!("{pods}/terminating-pod"),
        Some(&delete_now),
    )
    .await;
    assert_eq!(s, 200, "delete terminating pod: {b}");

    // "Ensuring resource quota status released the pod usage" (:850-857).
    wait_for_resource_quota(&api, &mem, ns, QUOTA_TERMINATING, &usage("0", false)).await;
}
