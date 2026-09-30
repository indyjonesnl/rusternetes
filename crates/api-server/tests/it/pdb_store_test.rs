//! PodDisruptionBudgets on the generic Store (#1990 step 6): upstream's
//! `pkg/registry/policy/poddisruptionbudget/strategy.go` runs inside
//! `Store.Create` / `Store.Update`, for PUT, PATCH and `/status` alike.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PDBS: &str = "/apis/policy/v1/namespaces/default/poddisruptionbudgets";

fn pdb(name: &str) -> Value {
    json!({
        "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": name}}}
    })
}

/// `PrepareForCreate` (strategy.go:67-73): a create cannot set status, and
/// the generation starts at 1.
#[tokio::test]
async fn a_create_clears_status_and_starts_generation_at_one() {
    let api = TestApiServer::new();
    let mut body = pdb("a");
    body["metadata"]["generation"] = json!(7);
    body["status"] = json!({"disruptionsAllowed": 5, "currentHealthy": 5,
                            "desiredHealthy": 1, "expectedPods": 5});
    let (s, created) = api.post(PDBS, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["metadata"]["generation"], 1, "{created}");
    assert_eq!(
        created["status"],
        json!({"disruptionsAllowed": 0, "currentHealthy": 0,
               "desiredHealthy": 0, "expectedPods": 0}),
        "{created}"
    );
}

/// `AllowUnconditionalUpdate` is false (strategy.go:125-129): a PUT with no
/// resourceVersion is invalid, "must be specified for an update"
/// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:727-733).
#[tokio::test]
async fn a_put_without_a_resource_version_is_rejected() {
    let api = TestApiServer::new();
    let (s, _) = api.post(PDBS, &pdb("a")).await;
    assert_eq!(s, StatusCode::CREATED);

    let mut update = pdb("a");
    update["spec"]["minAvailable"] = json!(2);
    let (s, body) = api.put(&format!("{PDBS}/a"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["details"]["causes"][0]["field"], "metadata.resourceVersion",
        "{body}"
    );

    let (_, stored) = api.get(&format!("{PDBS}/a")).await;
    assert_eq!(stored["spec"]["minAvailable"], 1, "{stored}");
}

/// `PrepareForUpdate` (strategy.go:76-88): status is kept and a spec change
/// bumps the generation.
#[tokio::test]
async fn a_spec_change_bumps_generation_and_keeps_status() {
    let api = TestApiServer::new();
    let (s, created) = api.post(PDBS, &pdb("a")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let (s, patched) = api
        .patch(
            &format!("{PDBS}/a"),
            &json!({"spec": {"minAvailable": 2}, "status": {"expectedPods": 9}}),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{patched}");
    assert_eq!(patched["metadata"]["generation"], 2, "{patched}");
    assert_eq!(patched["status"]["expectedPods"], 0, "{patched}");

    let (s, same) = api
        .patch(
            &format!("{PDBS}/a"),
            &json!({"metadata": {"labels": {"x": "y"}}}),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{same}");
    assert_eq!(same["metadata"]["generation"], 2, "no spec change: {same}");
}

/// `podDisruptionBudgetStatusStrategy.PrepareForUpdate` (strategy.go:156-161):
/// a `/status` PATCH writes status and leaves the spec alone.
#[tokio::test]
async fn a_status_patch_keeps_the_spec() {
    let api = TestApiServer::new();
    let (s, _) = api.post(PDBS, &pdb("a")).await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = api
        .patch(
            &format!("{PDBS}/a/status"),
            &json!({"spec": {"minAvailable": 3},
                    "status": {"disruptionsAllowed": 2, "currentHealthy": 3,
                               "desiredHealthy": 1, "expectedPods": 3}}),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["minAvailable"], 1, "{body}");
    assert_eq!(body["status"]["disruptionsAllowed"], 2, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
}

/// `ValidatePodDisruptionBudgetStatusUpdate` (validation.go:80-93) runs on
/// the `/status` PATCH too.
#[tokio::test]
async fn a_status_patch_with_a_negative_counter_is_invalid() {
    let api = TestApiServer::new();
    let (s, _) = api.post(PDBS, &pdb("a")).await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = api
        .patch(
            &format!("{PDBS}/a/status"),
            &json!({"status": {"disruptionsAllowed": -1}}),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// `Convert_v1_PodDisruptionBudget_To_policy_PodDisruptionBudget`
/// (pkg/apis/policy/v1/conversion.go:26-42): the v1beta1 match-all/none label
/// never combines with user requirements.
#[tokio::test]
async fn a_create_strips_the_v1beta1_selector_label() {
    let api = TestApiServer::new();
    let mut body = pdb("a");
    body["spec"]["selector"]["matchExpressions"] = json!([{
        "key": "pdb.kubernetes.io/deprecated-v1beta1-empty-selector-match",
        "operator": "Exists"
    }]);
    let (s, created) = api.post(PDBS, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(
        created["spec"]["selector"],
        json!({"matchLabels": {"app": "a"}}),
        "{created}"
    );
}

/// `ValidateUpdate` (strategy.go:112-118): an invalid selector label value is
/// rejected on create, so an update of a valid PDB cannot introduce one.
#[tokio::test]
async fn an_update_cannot_introduce_an_invalid_selector_value() {
    let api = TestApiServer::new();
    let (s, _) = api.post(PDBS, &pdb("a")).await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = api
        .patch(
            &format!("{PDBS}/a"),
            &json!({"spec": {"selector": {"matchExpressions": [
                {"key": "app", "operator": "In", "values": ["-bad-"]}
            ]}}}),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}
