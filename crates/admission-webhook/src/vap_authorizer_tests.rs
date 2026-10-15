//! The CEL `authorizer` library in the ValidatingAdmissionPolicy plugin.
//!
//! Upstream bindings under test:
//! * `policy/validating/plugin.go:118-119` (`compilePolicy`): `HasAuthorizer`
//!   is true for variables, matchConditions, validations and
//!   auditAnnotations, false for the messageExpressions.
//! * `policy/validating/validator.go:85, 104-105, 122, 195`: `Match` and the
//!   validation filter get `authz`; the messageExpression `OptionalVariableBindings`
//!   carries only `VersionedParams`.
//! * `cel/activation.go:52-55`: `authorizer` and `authorizer.requestResource`
//!   are bound only when `inputs.Authorizer != nil`.
//! * `cel/condition_test.go` "test authorizer ..." cases for the expressions.

use crate::cel_evaluators::VapAuthorizer;
use crate::AdmissionWebhookManager;
use rusternetes_common::admission::{GroupVersionKind, Operation};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{AlwaysAllowAuthorizer, AlwaysDenyAuthorizer, Authorizer};
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_storage::Storage;
use serde_json::{json, Value};
use std::sync::Arc;

fn user() -> UserInfo {
    UserInfo {
        username: "alice".into(),
        uid: "u1".into(),
        groups: vec!["devs".into()],
        extra: Default::default(),
    }
}

fn authz(a: impl Authorizer + 'static) -> VapAuthorizer {
    VapAuthorizer {
        user: user(),
        authorizer: Arc::new(a),
        subresource: String::new(),
    }
}

async fn manager(spec: Value) -> AdmissionWebhookManager<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    let mut spec = spec;
    spec["failurePolicy"] = json!("Fail");
    storage
        .create::<Value>(
            "/registry/validatingadmissionpolicies/p",
            &json!({"metadata": {"name": "p"}, "spec": spec}),
        )
        .await
        .unwrap();
    storage
        .create::<Value>(
            "/registry/validatingadmissionpolicybindings/b",
            &json!({"metadata": {"name": "b"},
                    "spec": {"policyName": "p", "validationActions": ["Deny"]}}),
        )
        .await
        .unwrap();
    AdmissionWebhookManager::new(storage)
}

async fn run(
    m: &AdmissionWebhookManager<MemoryStorage>,
    a: Option<&VapAuthorizer>,
) -> rusternetes_common::Result<()> {
    let gvk = GroupVersionKind {
        group: String::new(),
        version: "v1".into(),
        kind: "ConfigMap".into(),
    };
    let cm = json!({"metadata": {"name": "cm1", "namespace": "ns1"}});
    m.run_validating_admission_policies_authz(
        &Operation::Create,
        &gvk,
        Some(&cm),
        None,
        Some("configmaps"),
        Some("ns1"),
        a,
    )
    .await
}

const CHECK: &str = "authorizer.group('').resource('configmaps').check('get').allowed()";

#[tokio::test]
async fn validation_binds_authorizer() {
    let m = manager(json!({"validations": [{"expression": CHECK}]})).await;
    assert!(run(&m, Some(&authz(AlwaysAllowAuthorizer))).await.is_ok());
    let e = run(&m, Some(&authz(AlwaysDenyAuthorizer)))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("denied"), "{e}");
    assert!(!e.contains("evaluation error"), "{e}");
}

#[tokio::test]
async fn validation_binds_request_resource_authorizer() {
    let m = manager(json!({"validations": [
        {"expression": "authorizer.requestResource.check('delete').allowed()"}]}))
    .await;
    assert!(run(&m, Some(&authz(AlwaysAllowAuthorizer))).await.is_ok());
    let e = run(&m, Some(&authz(AlwaysDenyAuthorizer)))
        .await
        .unwrap_err()
        .to_string();
    assert!(!e.contains("evaluation error"), "{e}");
}

#[tokio::test]
async fn match_conditions_bind_authorizer() {
    // The policy only applies when the user may get configmaps; its
    // validation always denies.
    let m = manager(json!({
        "matchConditions": [{"name": "c", "expression": CHECK}],
        "validations": [{"expression": "false", "message": "nope"}]
    }))
    .await;
    let e = run(&m, Some(&authz(AlwaysAllowAuthorizer)))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("nope"), "{e}");
    assert!(run(&m, Some(&authz(AlwaysDenyAuthorizer))).await.is_ok());
}

#[tokio::test]
async fn variables_bind_authorizer() {
    let m = manager(json!({
        "variables": [{"name": "ok", "expression": CHECK}],
        "validations": [{"expression": "variables.ok"}]
    }))
    .await;
    assert!(run(&m, Some(&authz(AlwaysAllowAuthorizer))).await.is_ok());
    let e = run(&m, Some(&authz(AlwaysDenyAuthorizer)))
        .await
        .unwrap_err()
        .to_string();
    assert!(!e.contains("evaluation error"), "{e}");
}

#[tokio::test]
async fn audit_annotations_bind_authorizer() {
    let m = manager(json!({
        "validations": [{"expression": "true"}],
        "auditAnnotations": [{"key": "k", "valueExpression":
            "authorizer.group('').resource('configmaps').check('get').allowed() ? 'yes' : 'no'"}]
    }))
    .await;
    let r = run(&m, Some(&authz(AlwaysAllowAuthorizer))).await;
    assert!(r.is_ok(), "{r:?}");
}

#[tokio::test]
async fn message_expression_does_not_bind_authorizer() {
    // plugin.go:119 / validator.go:122: HasAuthorizer false for messageExpression,
    // so it errors and the static message is used (validator.go:176-183).
    let m = manager(json!({"validations": [{
        "expression": "false",
        "message": "static message",
        "messageExpression":
            "authorizer.group('').resource('configmaps').check('get').allowed() ? 'a' : 'b'"
    }]}))
    .await;
    let e = run(&m, Some(&authz(AlwaysAllowAuthorizer)))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("static message"), "{e}");
}

#[tokio::test]
async fn authorizer_is_unbound_without_one() {
    // activation.go:52: only when `inputs.Authorizer != nil`.
    let m = manager(json!({"validations": [{"expression": CHECK}]})).await;
    assert!(run(&m, None).await.is_err());
}

#[tokio::test]
async fn request_user_is_the_authorizer_user() {
    let m = manager(json!({"validations": [
        {"expression": "request.userInfo.username == 'alice'"}]}))
    .await;
    assert!(run(&m, Some(&authz(AlwaysAllowAuthorizer))).await.is_ok());
}
