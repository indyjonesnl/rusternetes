//! PriorityClasses on the generic Store (#1990 step 6): upstream's
//! `pkg/registry/scheduling/priorityclass` strategy and `REST.Delete`, and
//! the Priority admission plugin in the Store's validating admission.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PCS: &str = "/apis/scheduling.k8s.io/v1/priorityclasses";

fn pc(name: &str, value: i32) -> Value {
    json!({
        "apiVersion": "scheduling.k8s.io/v1", "kind": "PriorityClass",
        "metadata": {"name": name}, "value": value
    })
}

/// `PrepareForCreate` (strategy.go:46-50) and `SetDefaults_PriorityClass`
/// (pkg/apis/scheduling/v1/defaults.go).
#[tokio::test]
async fn a_create_sets_generation_and_defaults_preemption_policy() {
    let api = TestApiServer::new();
    let (s, created) = api.post(PCS, &pc("high", 1000)).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["metadata"]["generation"], 1, "{created}");
    assert_eq!(
        created["preemptionPolicy"], "PreemptLowerPriority",
        "{created}"
    );
}

/// `TestDeleteSystemPriorityClass` (storage/storage_test.go:116-133): `REST.Delete`
/// refuses a system priority class (storage.go:69-78).
#[tokio::test]
async fn a_system_priority_class_cannot_be_deleted() {
    let api = TestApiServer::new();
    let (s, body) = api
        .post(PCS, &pc("system-node-critical", 2_000_001_000))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");

    let (s, body) = api.delete(&format!("{PCS}/system-node-critical")).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(
        body["message"],
        "priorityclasses.scheduling.k8s.io \"system-node-critical\" is forbidden: this is a system priority class and cannot be deleted"
    );
    let (s, _) = api.get(&format!("{PCS}/system-node-critical")).await;
    assert_eq!(s, StatusCode::OK);
}

/// `validatePriorityClass` (plugin/pkg/admission/priority/admission.go:156-176)
/// runs on PATCH too: a second class may not be marked default.
#[tokio::test]
async fn a_patch_cannot_mark_a_second_default() {
    let api = TestApiServer::new();
    let mut first = pc("first", 10);
    first["globalDefault"] = json!(true);
    let (s, body) = api.post(PCS, &first).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, _) = api.post(PCS, &pc("second", 20)).await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = api
        .patch(&format!("{PCS}/second"), &json!({"globalDefault": true}))
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(
        body["message"],
        "priorityclasses.scheduling.k8s.io \"second\" is forbidden: PriorityClass first is already marked as default. Only one default can exist"
    );

    // Re-marking the default itself is allowed.
    let (s, body) = api
        .patch(
            &format!("{PCS}/first"),
            &json!({"globalDefault": true, "description": "d"}),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");
}

/// `ValidatePriorityClassUpdate`: preemptionPolicy is immutable, and the
/// default applies before the comparison, so a PATCH naming the default is
/// no change.
#[tokio::test]
async fn preemption_policy_is_immutable() {
    let api = TestApiServer::new();
    let (s, _) = api.post(PCS, &pc("p", 5)).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api
        .patch(&format!("{PCS}/p"), &json!({"preemptionPolicy": "Never"}))
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    let mut same = pc("p", 5);
    same["description"] = json!("resubmitted without preemptionPolicy");
    let (s, body) = api.put(&format!("{PCS}/p"), &same).await;
    assert_eq!(s, StatusCode::OK, "{body}");
}
