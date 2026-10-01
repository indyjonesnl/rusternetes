//! RBAC (Role, RoleBinding, ClusterRole, ClusterRoleBinding) on the generic
//! Store (#1990): the strategies of `pkg/registry/rbac/<kind>/strategy.go` and
//! the privilege-escalation checks of `pkg/registry/rbac/<kind>/policybased/`.
//!
//! Escalation tests run as an impersonated user (`Impersonate-User`): the
//! default `skip_auth` identity is `system:masters`, which
//! `rbacregistry.EscalationAllowed` (escalation_check.go:33-47) always lets
//! through.

use axum::http::StatusCode;
use rusternetes_common::resources::{ClusterRole, ClusterRoleBinding};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const RBAC: &str = "/apis/rbac.authorization.k8s.io/v1";

fn roles(ns: &str) -> String {
    format!("{RBAC}/namespaces/{ns}/roles")
}
fn rolebindings(ns: &str) -> String {
    format!("{RBAC}/namespaces/{ns}/rolebindings")
}
fn clusterroles() -> String {
    format!("{RBAC}/clusterroles")
}
fn clusterrolebindings() -> String {
    format!("{RBAC}/clusterrolebindings")
}

fn pod_reader() -> Value {
    json!([{"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}])
}

fn role(name: &str, rules: Value) -> Value {
    json!({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
        "metadata": {"name": name}, "rules": rules})
}

fn clusterrole(name: &str, rules: Value) -> Value {
    json!({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
        "metadata": {"name": name}, "rules": rules})
}

fn rolebinding(name: &str, kind: &str, role_name: &str) -> Value {
    json!({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
        "metadata": {"name": name},
        "subjects": [{"kind": "User", "name": "mallory", "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": kind, "name": role_name}})
}

fn clusterrolebinding(name: &str, role_name: &str) -> Value {
    json!({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
        "metadata": {"name": name},
        "subjects": [{"kind": "User", "name": "mallory", "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role_name}})
}

// ---------------------------------------------------------------------------
// Strategy behaviour (admin identity)
// ---------------------------------------------------------------------------

/// `ValidateRBACName` is `path.IsValidPathSegmentName` (validation.go:32-34):
/// not the DNS-1123 rule.
#[tokio::test]
async fn names_are_path_segments_not_dns_names() {
    let api = TestApiServer::new();
    let (s, body) = api
        .post(&roles("default"), &role("Bad_Name:ok", json!([])))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = api
        .post(&clusterroles(), &clusterrole("Bad_Name:ok", json!([])))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");

    let (s, body) = api.post(&roles("default"), &role("a%b", json!([]))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("metadata.name"),
        "{body}"
    );
    let (s, body) = api
        .post(&clusterrolebindings(), &clusterrolebinding("a%b", "x"))
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("metadata.name"),
        "{body}"
    );
}

/// `AllowCreateOnUpdate() == true` for all four (strategy.go): a PUT to a
/// missing object creates it.
#[tokio::test]
async fn a_put_to_a_missing_object_creates_it() {
    let api = TestApiServer::new();
    let (s, body) = api
        .put(
            &format!("{}/r1", roles("default")),
            &role("r1", pod_reader()),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = api
        .put(
            &format!("{}/r1", clusterroles()),
            &clusterrole("r1", pod_reader()),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = api
        .put(
            &format!("{}/b1", rolebindings("default")),
            &rolebinding("b1", "ClusterRole", "r1"),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = api
        .put(
            &format!("{}/b1", clusterrolebindings()),
            &clusterrolebinding("b1", "r1"),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

/// `AllowUnconditionalUpdate() == true`: a PUT without a resourceVersion works.
#[tokio::test]
async fn a_put_without_resource_version_is_accepted() {
    let api = TestApiServer::new();
    let (s, created) = api.post(&roles("default"), &role("r1", pod_reader())).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    update["rules"] = json!([{"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]}]);
    let (s, body) = api.put(&format!("{}/r1", roles("default")), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["rules"][0]["resources"], json!(["secrets"]), "{body}");
}

/// `ValidateRoleUpdate` runs the full `ValidateRole` (validation.go:51-56): an
/// invalid rule is rejected on PUT as it is on POST.
#[tokio::test]
async fn role_update_validates_the_rules() {
    let api = TestApiServer::new();
    let (s, created) = api.post(&roles("default"), &role("r1", pod_reader())).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["rules"] = json!([{"apiGroups": [""], "resources": ["pods"], "verbs": []}]);
    let (s, body) = api.put(&format!("{}/r1", roles("default")), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("rules[0].verbs"),
        "{body}"
    );
}

/// `ValidateClusterRoleUpdate` (validation.go:95-100), via PUT and PATCH.
#[tokio::test]
async fn clusterrole_update_validates_the_rules() {
    let api = TestApiServer::new();
    let (s, created) = api
        .post(&clusterroles(), &clusterrole("cr1", pod_reader()))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut update = created.clone();
    update["rules"] = json!([{"verbs": ["get"], "resources": ["pods"], "apiGroups": [""],
        "nonResourceURLs": ["/healthz"]}]);
    let (s, body) = api.put(&format!("{}/cr1", clusterroles()), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("rules[0]"),
        "{body}"
    );

    let (s, body) = api
        .patch(
            &format!("{}/cr1", clusterroles()),
            &json!({"rules": [{"verbs": [], "resources": ["pods"], "apiGroups": [""]}]}),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// `ValidateRoleBindingUpdate` / `ValidateClusterRoleBindingUpdate`: roleRef is
/// immutable (validation.go:161-170, 205-214).
#[tokio::test]
async fn binding_role_ref_is_immutable() {
    let api = TestApiServer::new();
    let (s, created) = api
        .post(
            &rolebindings("default"),
            &rolebinding("b1", "ClusterRole", "r1"),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["roleRef"]["name"] = json!("r2");
    let (s, body) = api
        .put(&format!("{}/b1", rolebindings("default")), &update)
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("cannot change roleRef"),
        "{body}"
    );

    let (s, created) = api
        .post(&clusterrolebindings(), &clusterrolebinding("b1", "r1"))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["roleRef"]["name"] = json!("r2");
    let (s, body) = api
        .put(&format!("{}/b1", clusterrolebindings()), &update)
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// `SetDefaults_RoleBinding` / `SetDefaults_Subject` (pkg/apis/rbac/v1/
/// defaults.go): an omitted `roleRef.apiGroup` and User/Group `apiGroup`
/// default to the RBAC group.
#[tokio::test]
async fn omitted_api_groups_are_defaulted() {
    let api = TestApiServer::new();
    let mut rb = rolebinding("b1", "Role", "r1");
    rb["roleRef"].as_object_mut().unwrap().remove("apiGroup");
    rb["subjects"][0]
        .as_object_mut()
        .unwrap()
        .remove("apiGroup");
    let (s, body) = api.post(&rolebindings("default"), &rb).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["roleRef"]["apiGroup"], "rbac.authorization.k8s.io",
        "{body}"
    );
    assert_eq!(
        body["subjects"][0]["apiGroup"], "rbac.authorization.k8s.io",
        "{body}"
    );

    let mut crb = clusterrolebinding("b1", "r1");
    crb["roleRef"].as_object_mut().unwrap().remove("apiGroup");
    let (s, body) = api.post(&clusterrolebindings(), &crb).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["roleRef"]["apiGroup"], "rbac.authorization.k8s.io",
        "{body}"
    );
}

/// A second DELETE is a 404 naming the group-qualified resource
/// (`NewNotFound(qualifiedResource, name)`).
#[tokio::test]
async fn delete_then_not_found_names_the_qualified_resource() {
    let api = TestApiServer::new();
    let (s, _) = api.post(&roles("default"), &role("r1", pod_reader())).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api.delete(&format!("{}/r1", roles("default"))).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, body) = api.delete(&format!("{}/r1", roles("default"))).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("roles.rbac.authorization.k8s.io \"r1\" not found"),
        "{body}"
    );
}

// ---------------------------------------------------------------------------
// Privilege escalation (policybased storage)
// ---------------------------------------------------------------------------

fn writer_rules() -> Value {
    json!([{
        "apiGroups": ["rbac.authorization.k8s.io"],
        "resources": ["roles", "rolebindings", "clusterroles", "clusterrolebindings"],
        "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]
    }])
}

/// Grant `user` the RBAC-writer rules plus `extra` rules, cluster-wide.
async fn grant(api: &TestApiServer, user: &str, extra: Value) {
    let mut rules = writer_rules().as_array().unwrap().clone();
    rules.extend(extra.as_array().unwrap().iter().cloned());
    let name = format!("grant-{user}");
    let cr: ClusterRole = serde_json::from_value(clusterrole(&name, json!(rules))).unwrap();
    api.storage
        .create(&build_key("clusterroles", None, &name), &cr)
        .await
        .unwrap();
    let crb: ClusterRoleBinding = serde_json::from_value(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
        "metadata": {"name": name},
        "subjects": [{"kind": "User", "name": user, "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": name}
    }))
    .unwrap();
    api.storage
        .create(&build_key("clusterrolebindings", None, &name), &crb)
        .await
        .unwrap();
}

/// The `system:masters` admin may impersonate (cluster-admin); the other users
/// hold only what `grant` gives them.
async fn rbac_api() -> TestApiServer {
    let api = TestApiServer::builder().rbac().build();
    let cr: ClusterRole = serde_json::from_value(clusterrole(
        "cluster-admin",
        json!([{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]},
               {"nonResourceURLs": ["*"], "verbs": ["*"]}]),
    ))
    .unwrap();
    api.storage
        .create(&build_key("clusterroles", None, "cluster-admin"), &cr)
        .await
        .unwrap();
    let crb: ClusterRoleBinding = serde_json::from_value(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
        "metadata": {"name": "cluster-admin"},
        "subjects": [{"kind": "Group", "name": "system:masters", "apiGroup": "rbac.authorization.k8s.io"}],
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"}
    }))
    .unwrap();
    api.storage
        .create(
            &build_key("clusterrolebindings", None, "cluster-admin"),
            &crb,
        )
        .await
        .unwrap();
    // alice: writes RBAC objects, holds nothing else.
    grant(&api, "alice", json!([])).await;
    // bob: may escalate roles and clusterroles.
    grant(
        &api,
        "bob",
        json!([{"apiGroups": ["rbac.authorization.k8s.io"],
            "resources": ["roles", "clusterroles"], "verbs": ["escalate"]}]),
    )
    .await;
    // carol: may bind roles and clusterroles.
    grant(
        &api,
        "carol",
        json!([{"apiGroups": ["rbac.authorization.k8s.io"],
            "resources": ["roles", "clusterroles"], "verbs": ["bind"]}]),
    )
    .await;
    // dave: holds read access to pods himself.
    grant(
        &api,
        "dave",
        json!([{"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}]),
    )
    .await;
    api
}

async fn as_user_ct(
    api: &TestApiServer,
    user: &str,
    method: &str,
    uri: &str,
    content_type: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let bytes = body.map(|b| serde_json::to_vec(b).unwrap());
    let (s, _h, _b, v) = api
        .send_with_headers(
            method,
            uri,
            &[("impersonate-user", user), ("content-type", content_type)],
            bytes,
        )
        .await;
    (s, v)
}

async fn as_user(
    api: &TestApiServer,
    user: &str,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    as_user_ct(api, user, method, uri, "application/json", body).await
}

fn assert_escalation_denied(s: StatusCode, body: &Value) {
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("is attempting to grant RBAC permissions not currently held"),
        "{body}"
    );
}

/// role/policybased/storage.go:63-73: a Role carrying rules the caller does not
/// hold is Forbidden, and the message names the missing rule
/// (rule.go:63-80 `CompactString`).
#[tokio::test]
async fn role_create_without_the_rules_is_forbidden() {
    let api = rbac_api().await;
    let (s, body) = as_user(
        &api,
        "alice",
        "POST",
        &roles("default"),
        Some(&role("r1", pod_reader())),
    )
    .await;
    assert_escalation_denied(s, &body);
    let msg = body["message"].as_str().unwrap();
    assert!(
        msg.starts_with("roles.rbac.authorization.k8s.io \"r1\" is forbidden: user \"alice\""),
        "{msg}"
    );
    assert!(
        msg.contains("{APIGroups:[\"\"], Resources:[\"pods\"], Verbs:[\"get\" \"list\"]}"),
        "{msg}"
    );

    // A Role with no rules escalates nothing.
    let (s, body) = as_user(
        &api,
        "alice",
        "POST",
        &roles("default"),
        Some(&role("empty", json!([]))),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

/// `RoleEscalationAuthorized` (escalation_check.go:62-102): the `escalate`
/// verb lets a caller grant rules it does not hold.
#[tokio::test]
async fn role_create_with_the_escalate_verb_is_allowed() {
    let api = rbac_api().await;
    let (s, body) = as_user(
        &api,
        "bob",
        "POST",
        &roles("default"),
        Some(&role("r1", pod_reader())),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = as_user(
        &api,
        "bob",
        "POST",
        &clusterroles(),
        Some(&clusterrole("r1", pod_reader())),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

/// `ConfirmNoEscalation` passes when the caller's own rules cover the new ones.
#[tokio::test]
async fn role_create_the_caller_already_holds_is_allowed() {
    let api = rbac_api().await;
    let (s, body) = as_user(
        &api,
        "dave",
        "POST",
        &roles("default"),
        Some(&role("r1", pod_reader())),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    // ... but not rules beyond them.
    let more = json!([{"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "delete"]}]);
    let (s, body) = as_user(
        &api,
        "dave",
        "POST",
        &roles("default"),
        Some(&role("r2", more)),
    )
    .await;
    assert_escalation_denied(s, &body);
}

/// The wrapped `UpdatedObjectInfo` (role/policybased/storage.go:76-98) checks
/// the new rules on PUT and PATCH.
#[tokio::test]
async fn role_update_adding_rules_is_forbidden() {
    let api = rbac_api().await;
    let (s, created) = api.post(&roles("default"), &role("r1", json!([]))).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let uri = format!("{}/r1", roles("default"));
    let mut update = created.clone();
    update["rules"] = pod_reader();
    let (s, body) = as_user(&api, "alice", "PUT", &uri, Some(&update)).await;
    assert_escalation_denied(s, &body);

    let (s, body) = as_user_ct(
        &api,
        "alice",
        "PATCH",
        &uri,
        "application/merge-patch+json",
        Some(&json!({"rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}]})),
    )
    .await;
    assert_escalation_denied(s, &body);

    let (s, body) = as_user(&api, "bob", "PUT", &uri, Some(&update)).await;
    assert_eq!(s, StatusCode::OK, "{body}");
}

/// `IsOnlyMutatingGCFields` (helpers.go:29-50): an update that changes only
/// ownerReferences and finalizers is not an escalation, so the garbage
/// collector can always finish its work.
#[tokio::test]
async fn gc_only_update_skips_the_escalation_check() {
    let api = rbac_api().await;
    let (s, created) = api.post(&roles("default"), &role("r1", pod_reader())).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let uri = format!("{}/r1", roles("default"));

    let mut gc = created.clone();
    gc["metadata"]["finalizers"] = json!(["example.com/f"]);
    let (s, body) = as_user(&api, "alice", "PUT", &uri, Some(&gc)).await;
    assert_eq!(s, StatusCode::OK, "{body}");

    // Anything else alongside is checked.
    let mut mixed = body.clone();
    mixed["metadata"]["labels"] = json!({"a": "b"});
    let (s, body) = as_user(&api, "alice", "PUT", &uri, Some(&mixed)).await;
    assert_escalation_denied(s, &body);
}

/// rolebinding/policybased/storage.go:62-95: binding a role needs its rules
/// or the `bind` verb on it.
#[tokio::test]
async fn rolebinding_create_needs_the_rules_or_bind() {
    let api = rbac_api().await;
    let (s, _) = api
        .post(&roles("default"), &role("pods", pod_reader()))
        .await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = as_user(
        &api,
        "alice",
        "POST",
        &rolebindings("default"),
        Some(&rolebinding("b1", "Role", "pods")),
    )
    .await;
    assert_escalation_denied(s, &body);
    assert!(
        body["message"].as_str().unwrap().starts_with(
            "rolebindings.rbac.authorization.k8s.io \"b1\" is forbidden: user \"alice\""
        ),
        "{body}"
    );

    let (s, body) = as_user(
        &api,
        "carol",
        "POST",
        &rolebindings("default"),
        Some(&rolebinding("b1", "Role", "pods")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = as_user(
        &api,
        "dave",
        "POST",
        &rolebindings("default"),
        Some(&rolebinding("b2", "Role", "pods")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    // `escalate` is not `bind`.
    let (s, body) = as_user(
        &api,
        "bob",
        "POST",
        &rolebindings("default"),
        Some(&rolebinding("b3", "Role", "pods")),
    )
    .await;
    assert_escalation_denied(s, &body);
}

/// `GetRoleReferenceRules` errors on a role that does not exist
/// (rule.go:259-278), and the policybased storage returns that error.
#[tokio::test]
async fn binding_a_missing_role_is_not_found_for_a_caller_without_bind() {
    let api = rbac_api().await;
    let (s, body) = as_user(
        &api,
        "alice",
        "POST",
        &rolebindings("default"),
        Some(&rolebinding("b1", "Role", "missing")),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
    let (s, body) = as_user(
        &api,
        "carol",
        "POST",
        &rolebindings("default"),
        Some(&rolebinding("b1", "Role", "missing")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    // The superuser skips the whole check.
    let (s, body) = api
        .post(
            &rolebindings("default"),
            &rolebinding("b2", "Role", "missing"),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

/// A binding in one namespace is checked against the caller's rules in that
/// namespace (`NamespaceFrom(ctx)`).
#[tokio::test]
async fn rolebinding_rules_are_resolved_in_the_binding_namespace() {
    let api = rbac_api().await;
    // erin holds pods only in ns "a", through a RoleBinding to a Role.
    let (s, body) = api.post(&roles("a"), &role("pods", pod_reader())).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = api.post(&roles("b"), &role("pods", pod_reader())).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let mut rb = rolebinding("erin-pods", "Role", "pods");
    rb["subjects"] =
        json!([{"kind": "User", "name": "erin", "apiGroup": "rbac.authorization.k8s.io"}]);
    let (s, body) = api.post(&rolebindings("a"), &rb).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    grant(&api, "erin", json!([])).await;

    let (s, body) = as_user(
        &api,
        "erin",
        "POST",
        &rolebindings("a"),
        Some(&rolebinding("b1", "Role", "pods")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = as_user(
        &api,
        "erin",
        "POST",
        &rolebindings("b"),
        Some(&rolebinding("b1", "Role", "pods")),
    )
    .await;
    assert_escalation_denied(s, &body);
}

/// A rolebinding update that changes only the subjects is still checked
/// (rolebinding/policybased/storage.go:97-137), `bind` lets it through, and
/// a GC-only change does not need either.
#[tokio::test]
async fn rolebinding_update_is_checked() {
    let api = rbac_api().await;
    let (s, _) = api
        .post(&roles("default"), &role("pods", pod_reader()))
        .await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, created) = api
        .post(&rolebindings("default"), &rolebinding("b1", "Role", "pods"))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut more = created.clone();
    more["subjects"] = json!([
        {"kind": "User", "name": "mallory", "apiGroup": "rbac.authorization.k8s.io"},
        {"kind": "User", "name": "eve", "apiGroup": "rbac.authorization.k8s.io"}]);
    let uri = format!("{}/b1", rolebindings("default"));
    let (s, body) = as_user(&api, "alice", "PUT", &uri, Some(&more)).await;
    assert_escalation_denied(s, &body);
    let (s, body) = as_user(&api, "carol", "PUT", &uri, Some(&more)).await;
    assert_eq!(s, StatusCode::OK, "{body}");

    let mut gc = body.clone();
    gc["metadata"]["finalizers"] = json!(["example.com/f"]);
    let (s, body) = as_user(&api, "alice", "PUT", &uri, Some(&gc)).await;
    assert_eq!(s, StatusCode::OK, "{body}");
}

/// clusterrolebinding/policybased/storage.go: the same check, in the empty
/// namespace.
#[tokio::test]
async fn clusterrolebinding_create_needs_the_rules_or_bind() {
    let api = rbac_api().await;
    let (s, _) = api
        .post(&clusterroles(), &clusterrole("pods", pod_reader()))
        .await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, body) = as_user(
        &api,
        "alice",
        "POST",
        &clusterrolebindings(),
        Some(&clusterrolebinding("b1", "pods")),
    )
    .await;
    assert_escalation_denied(s, &body);
    let (s, body) = as_user(
        &api,
        "carol",
        "POST",
        &clusterrolebindings(),
        Some(&clusterrolebinding("b1", "pods")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = as_user(
        &api,
        "dave",
        "POST",
        &clusterrolebindings(),
        Some(&clusterrolebinding("b2", "pods")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

/// clusterrole/policybased/storage.go:68-74: setting an aggregationRule needs
/// `*` on `*.*`, whatever the rules.
#[tokio::test]
async fn an_aggregation_rule_needs_cluster_admin() {
    let api = rbac_api().await;
    let mut agg = clusterrole("agg", json!([]));
    agg["aggregationRule"] = json!({"clusterRoleSelectors": [{"matchLabels": {"a": "b"}}]});
    let (s, body) = as_user(&api, "alice", "POST", &clusterroles(), Some(&agg)).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("must have cluster-admin privileges to use the aggregationRule"),
        "{body}"
    );
    let (s, created) = api.post(&clusterroles(), &agg).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    // Changing the rule of an existing aggregated role is checked on the OLD
    // object too (storage.go:106-110).
    let mut plain = created.clone();
    plain.as_object_mut().unwrap().remove("aggregationRule");
    let (s, body) = as_user(
        &api,
        "alice",
        "PUT",
        &format!("{}/agg", clusterroles()),
        Some(&plain),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
}
