//! `validatingadmissionpolicy-status` controller, ported from
//! `pkg/controller/validatingadmissionpolicystatus/controller_test.go`
//! (`TestTypeChecking`, release-1.35) plus the `reconcile` / queue branches of
//! `controller.go`.

use rusternetes_common::resources::{ExpressionWarning, ValidatingAdmissionPolicy};
use rusternetes_controller_manager::controllers::vap_status::VapStatusController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const KEY_PLURAL: &str = "validatingadmissionpolicies";

/// `withGVRMatch` + `withValidations` + `makePolicy`.
fn policy(name: &str, generation: i64, expressions: &[&str]) -> Value {
    let validations: Vec<Value> = expressions
        .iter()
        .map(|e| json!({ "expression": e }))
        .collect();
    json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": name, "generation": generation, "uid": format!("uid-{name}")},
        "spec": {
            "matchConstraints": {"resourceRules": [{
                "operations": ["*"],
                "apiGroups": ["apps"],
                "apiVersions": ["v1"],
                "resources": ["deployments"],
            }]},
            "validations": validations,
        },
    })
}

async fn seed(storage: &MemoryStorage, p: Value) -> String {
    let name = p["metadata"]["name"].as_str().unwrap().to_string();
    let key = build_key(KEY_PLURAL, None, &name);
    storage.create(&key, &p).await.unwrap();
    name
}

async fn stored(storage: &MemoryStorage, name: &str) -> ValidatingAdmissionPolicy {
    storage
        .get(&build_key(KEY_PLURAL, None, name))
        .await
        .unwrap()
}

async fn warnings_of(expressions: &[&str]) -> Vec<ExpressionWarning> {
    let storage = Arc::new(MemoryStorage::new());
    let name = seed(&storage, policy("p", 1, expressions)).await;
    let c = VapStatusController::new(storage.clone());
    c.sync(&format!("{KEY_PLURAL}/{name}")).await.unwrap();
    let p = stored(&storage, &name).await;
    let status = p.status.expect("status written");
    assert_eq!(status.observed_generation, Some(1));
    status
        .type_checking
        .expect("typeChecking set even when clean")
        .expression_warnings
        .unwrap_or_default()
}

#[tokio::test]
async fn deployment_with_correct_expression() {
    assert!(warnings_of(&["object.spec.replicas > 1"]).await.is_empty());
}

#[tokio::test]
async fn deployment_with_type_confusion() {
    let w = warnings_of(&["object.spec.replicas < 100", "object.spec.replicas > '1'"]).await;
    assert_eq!(w.len(), 1, "{w:?}");
    assert_eq!(w[0].field_ref, "spec.validations[1].expression");
    assert!(
        w[0].warning
            .contains("found no matching overload for '_>_' applied to '(int, string)'"),
        "{}",
        w[0].warning
    );
    assert!(
        w[0].warning.starts_with("apps/v1, Kind=Deployment: "),
        "{}",
        w[0].warning
    );
}

#[tokio::test]
async fn two_expressions_different_type_checking_errors() {
    let w = warnings_of(&[
        "object.spec.nonExistingFirst > 1",
        "object.spec.replicas > '1'",
    ])
    .await;
    let refs: Vec<_> = w.iter().map(|w| w.field_ref.as_str()).collect();
    assert_eq!(
        refs,
        [
            "spec.validations[0].expression",
            "spec.validations[1].expression"
        ]
    );
    assert!(w[0].warning.contains("undefined field 'nonExistingFirst'"));
    assert!(w[1]
        .warning
        .contains("found no matching overload for '_>_' applied to '(int, string)'"));
}

#[tokio::test]
async fn one_expression_two_warnings() {
    let w = warnings_of(&[
        "object.spec.replicas < 100",
        "object.spec.replicas > '1' && object.spec.nonExisting == 1",
    ])
    .await;
    assert_eq!(w.len(), 1, "{w:?}");
    assert_eq!(w[0].field_ref, "spec.validations[1].expression");
    assert!(w[0].warning.contains("undefined field 'nonExisting'"));
    assert!(w[0]
        .warning
        .contains("found no matching overload for '_>_' applied to '(int, string)'"));
}

#[tokio::test]
async fn message_expression_is_checked_with_its_own_field_ref() {
    let storage = Arc::new(MemoryStorage::new());
    let mut p = policy("p", 1, &["object.spec.replicas > 1"]);
    p["spec"]["validations"][0]["messageExpression"] = json!("object.spec.nope");
    let name = seed(&storage, p).await;
    VapStatusController::new(storage.clone())
        .sync(&format!("{KEY_PLURAL}/{name}"))
        .await
        .unwrap();
    let w = stored(&storage, &name)
        .await
        .status
        .unwrap()
        .type_checking
        .unwrap()
        .expression_warnings
        .unwrap();
    assert_eq!(w.len(), 1);
    assert_eq!(w[0].field_ref, "spec.validations[0].messageExpression");
}

/// `reconcile`: `Generation <= Status.ObservedGeneration` is a no-op.
#[tokio::test]
async fn an_observed_generation_is_not_reconciled_again() {
    let storage = Arc::new(MemoryStorage::new());
    let mut p = policy("p", 2, &["object.spec.replicas > '1'"]);
    p["status"] = json!({"observedGeneration": 2});
    let name = seed(&storage, p).await;
    VapStatusController::new(storage.clone())
        .sync(&format!("{KEY_PLURAL}/{name}"))
        .await
        .unwrap();
    let status = stored(&storage, &name).await.status.unwrap();
    assert_eq!(status.observed_generation, Some(2));
    assert!(status.type_checking.is_none());
}

/// A newer generation is re-checked and the other status fields survive (the
/// apply owns only `observedGeneration` and `typeChecking`).
#[tokio::test]
async fn a_new_generation_replaces_warnings_and_keeps_conditions() {
    let storage = Arc::new(MemoryStorage::new());
    let mut p = policy("p", 2, &["object.spec.replicas > 1"]);
    p["status"] = json!({
        "observedGeneration": 1,
        "typeChecking": {"expressionWarnings": [{"fieldRef": "spec.validations[0].expression", "warning": "stale"}]},
        "conditions": [{"type": "Ready", "status": "True"}],
    });
    let name = seed(&storage, p).await;
    VapStatusController::new(storage.clone())
        .sync(&format!("{KEY_PLURAL}/{name}"))
        .await
        .unwrap();
    let status = stored(&storage, &name).await.status.unwrap();
    assert_eq!(status.observed_generation, Some(2));
    assert!(status.type_checking.unwrap().expression_warnings.is_none());
    assert_eq!(status.conditions.unwrap().len(), 1);
}

/// `processNextWorkItem`: a missing policy is "being deleted, do nothing".
#[tokio::test]
async fn a_missing_policy_is_ignored() {
    let storage = Arc::new(MemoryStorage::new());
    VapStatusController::new(storage)
        .sync(&format!("{KEY_PLURAL}/gone"))
        .await
        .unwrap();
}

/// The live path: informer events feed the queue and a worker applies the
/// status (`TestTypeChecking` runs `controller.Run`).
#[tokio::test]
async fn run_reconciles_policies_added_after_start() {
    let storage = Arc::new(MemoryStorage::new());
    let c = Arc::new(VapStatusController::new(storage.clone()));
    let handle = tokio::spawn(c.run());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let name = seed(&storage, policy("late", 1, &["object.spec.replicas > '1'"])).await;
    let mut done = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let p = stored(&storage, &name).await;
        if p.status
            .as_ref()
            .and_then(|s| s.type_checking.as_ref())
            .is_some()
        {
            done = p.status;
            break;
        }
    }
    handle.abort();
    let tc = done
        .expect("status applied by the worker")
        .type_checking
        .unwrap();
    assert_eq!(tc.expression_warnings.unwrap().len(), 1);
}
