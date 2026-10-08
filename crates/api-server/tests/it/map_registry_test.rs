//! MutatingAdmissionPolicy / Binding registry (#2744), ported from
//! `pkg/registry/admissionregistration/mutatingadmissionpolicy{,binding}/
//! {strategy,authz}.go` (and their tests), served from
//! `storage_apiserver.go:187-205` under the `MutatingAdmissionPolicy` gate.
//!
//! Requests run as an impersonated user: the default identity is
//! `system:masters`, which `rbacregistry.EscalationAllowed` always lets through.

use axum::http::StatusCode;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_common::resources::{ClusterRole, ClusterRoleBinding};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const VAP: &str = "/apis/admissionregistration.k8s.io/v1beta1/mutatingadmissionpolicies";
const VAPB: &str = "/apis/admissionregistration.k8s.io/v1beta1/mutatingadmissionpolicybindings";

fn policy(name: &str, param_kind: Option<Value>) -> Value {
    let mut spec = json!({
        "matchConstraints": {"resourceRules": [{
            "apiGroups": ["apps"], "apiVersions": ["v1"],
            "operations": ["CREATE"], "resources": ["deployments"]}]},
        "failurePolicy": "Fail", "reinvocationPolicy": "Never",
        "mutations": [{"patchType": "ApplyConfiguration",
                       "applyConfiguration": {"expression": "Object{}"}}]});
    if let Some(pk) = param_kind {
        spec["paramKind"] = pk;
    }
    json!({"apiVersion": "admissionregistration.k8s.io/v1beta1", "kind": "MutatingAdmissionPolicy",
           "metadata": {"name": name}, "spec": spec})
}

fn binding(name: &str, policy: &str, param_ref: Option<Value>) -> Value {
    let mut spec = json!({"policyName": policy});
    if let Some(pr) = param_ref {
        spec["paramRef"] = pr;
    }
    json!({"apiVersion": "admissionregistration.k8s.io/v1beta1", "kind": "MutatingAdmissionPolicyBinding",
           "metadata": {"name": name}, "spec": spec})
}

fn cm_kind() -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap"})
}

async fn grant(api: &TestApiServer, user: &str, extra: Value) {
    let mut rules = vec![json!({
        "apiGroups": ["admissionregistration.k8s.io"],
        "resources": ["mutatingadmissionpolicies", "mutatingadmissionpolicybindings"],
        "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]})];
    rules.extend(extra.as_array().unwrap().iter().cloned());
    let name = format!("grant-{user}");
    let cr: ClusterRole = serde_json::from_value(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
        "metadata": {"name": name}, "rules": rules}))
    .unwrap();
    api.storage
        .create(&build_key("clusterroles", None, &name), &cr)
        .await
        .unwrap();
    let crb: ClusterRoleBinding = serde_json::from_value(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
        "metadata": {"name": name},
        "subjects": [{"kind": "User", "name": user, "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": name}}))
    .unwrap();
    api.storage
        .create(&build_key("clusterrolebindings", None, &name), &crb)
        .await
        .unwrap();
}

async fn api() -> TestApiServer {
    // The caller holds the gate guard; see `gated()`.
    let api = TestApiServer::builder().rbac().build();
    let cr: ClusterRole = serde_json::from_value(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
        "metadata": {"name": "cluster-admin"},
        "rules": [{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]},
                  {"nonResourceURLs": ["*"], "verbs": ["*"]}]}))
    .unwrap();
    api.storage
        .create(&build_key("clusterroles", None, "cluster-admin"), &cr)
        .await
        .unwrap();
    let crb: ClusterRoleBinding = serde_json::from_value(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
        "metadata": {"name": "cluster-admin"},
        "subjects": [{"kind": "Group", "name": "system:masters", "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"}}))
    .unwrap();
    api.storage
        .create(
            &build_key("clusterrolebindings", None, "cluster-admin"),
            &crb,
        )
        .await
        .unwrap();
    // alice writes policies and bindings, reads nothing else.
    grant(&api, "alice", json!([])).await;
    // bob also reads configmaps.
    grant(
        &api,
        "bob",
        json!([{"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "list"]}]),
    )
    .await;
    // carol may only get (not list) configmaps.
    grant(
        &api,
        "carol",
        json!([{"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get"]}]),
    )
    .await;
    api
}

async fn as_user(
    api: &TestApiServer,
    user: &str,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let bytes = body.map(|b| serde_json::to_vec(b).unwrap());
    let (s, _h, _b, v) = api
        .send_with_headers(
            method,
            uri,
            &[
                ("impersonate-user", user),
                ("content-type", "application/json"),
            ],
            bytes,
        )
        .await;
    (s, v)
}

fn assert_forbidden_field(s: StatusCode, body: &Value, field: &str, needle: &str) {
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let causes = body["details"]["causes"].as_array().expect("causes");
    assert!(
        causes.iter().any(|c| c["reason"] == "FieldValueForbidden"
            && c["field"] == field
            && c["message"].as_str().unwrap().contains(needle)),
        "{body}"
    );
}

/// storage_apiserver.go:187-205 + instance.go (v1beta1 off by default): with the
/// gate off every verb is a 404.
#[tokio::test]
#[serial_test::serial]
async fn not_served_while_the_gate_is_off() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, false);
    let api = api().await;
    let (s, _) = api.post(VAP, &policy("p1", None)).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = api.get(VAP).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = api.get(&format!("{VAP}/p1")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = api.post(VAPB, &binding("b1", "p1", None)).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = api.get(VAPB).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// strategy.go:57-73: generation starts at 1 and bumps on a spec change only;
/// defaults apply; an invalid object is a 422.
#[tokio::test]
#[serial_test::serial]
async fn generation_defaults_and_validation() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let api = api().await;
    let mut p = policy("p1", None);
    p["spec"].as_object_mut().unwrap().remove("failurePolicy");
    let (s, body) = api.post(VAP, &p).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
    assert_eq!(body["spec"]["failurePolicy"], "Fail", "{body}");
    assert_eq!(body["kind"], "MutatingAdmissionPolicy", "{body}");

    let mut upd = body.clone();
    upd["metadata"]["labels"] = json!({"a": "b"});
    let (s, body) = api.put(&format!("{VAP}/p1"), &upd).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");

    let mut upd = body.clone();
    upd["spec"]["failurePolicy"] = json!("Ignore");
    let (s, body) = api.put(&format!("{VAP}/p1"), &upd).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 2, "{body}");

    // PUT creating a missing object is not allowed (strategy.go:97-99).
    let (s, _) = api
        .put(&format!("{VAP}/missing"), &policy("missing", None))
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    let mut bad = policy("p2", None);
    bad["spec"]["mutations"] = json!([]);
    bad["spec"]["matchConstraints"] = json!({});
    let (s, body) = api.post(VAP, &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    let (s, body) = api.get(VAP).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["kind"], "MutatingAdmissionPolicyList", "{body}");
    assert_eq!(body["items"].as_array().unwrap().len(), 1);

    let (s, body) = api.post(VAPB, &binding("b1", "p1", None)).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");
    let (s, body) = api.post(VAPB, &binding("", "p1", None)).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// authz.go:30-96 (policy), identical to the validating one.
#[tokio::test]
#[serial_test::serial]
async fn policy_create_with_param_kind_needs_get_on_the_kind() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let api = api().await;
    let (s, body) = as_user(
        &api,
        "alice",
        "POST",
        VAP,
        Some(&policy("p1", Some(cm_kind()))),
    )
    .await;
    assert_forbidden_field(
        s,
        &body,
        "spec.paramKind",
        "must have \"get\" permission on all objects of the referenced paramKind (kind=ConfigMap, apiVersion=v1)",
    );
    let (s, body) = as_user(
        &api,
        "bob",
        "POST",
        VAP,
        Some(&policy("p1", Some(cm_kind()))),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

/// binding authz.go:80-135, including the upstream message that swaps the user
/// and the verb (:133).
#[tokio::test]
#[serial_test::serial]
async fn binding_with_param_ref_needs_get_or_list() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let api = api().await;
    let (s, body) = api.post(VAP, &policy("p1", Some(cm_kind()))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let named = json!({"name": "cfg", "namespace": "default", "parameterNotFoundAction": "Deny"});
    let (s, body) = as_user(
        &api,
        "alice",
        "POST",
        VAPB,
        Some(&binding("b1", "p1", Some(named.clone()))),
    )
    .await;
    assert_forbidden_field(s, &body, "spec.paramRef", "does not have \"&{alice");
    let (s, body) = as_user(
        &api,
        "carol",
        "POST",
        VAPB,
        Some(&binding("b1", "p1", Some(named))),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let selector =
        json!({"namespace": "default", "selector": {}, "parameterNotFoundAction": "Deny"});
    let (s, body) = as_user(
        &api,
        "carol",
        "POST",
        VAPB,
        Some(&binding("b2", "p1", Some(selector.clone()))),
    )
    .await;
    assert_forbidden_field(s, &body, "spec.paramRef", "user list does not have");
    let (s, body) = as_user(
        &api,
        "bob",
        "POST",
        VAPB,
        Some(&binding("b2", "p1", Some(selector))),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}
