//! `paramKind` / `paramRef` read-access authorization (#2109), ported from
//! `pkg/registry/admissionregistration/validatingadmissionpolicy/authz.go` and
//! `.../validatingadmissionpolicybinding/authz.go` (and their `authz_test.go`).
//!
//! Requests run as an impersonated user: the default identity is
//! `system:masters`, which `rbacregistry.EscalationAllowed` always lets through.

use axum::http::StatusCode;
use rusternetes_common::resources::{ClusterRole, ClusterRoleBinding};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const VAP: &str = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies";
const VAPB: &str = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings";

fn policy(name: &str, param_kind: Option<Value>) -> Value {
    let mut spec = json!({
        "matchConstraints": {"resourceRules": [{
            "apiGroups": ["apps"], "apiVersions": ["v1"],
            "operations": ["CREATE"], "resources": ["deployments"]}]},
        "validations": [{"expression": "object.spec.replicas <= 5"}]});
    if let Some(pk) = param_kind {
        spec["paramKind"] = pk;
    }
    json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy",
           "metadata": {"name": name}, "spec": spec})
}

fn binding(name: &str, policy: &str, param_ref: Option<Value>) -> Value {
    let mut spec = json!({"policyName": policy, "validationActions": ["Deny"]});
    if let Some(pr) = param_ref {
        spec["paramRef"] = pr;
    }
    json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding",
           "metadata": {"name": name}, "spec": spec})
}

fn cm_kind() -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap"})
}

async fn grant(api: &TestApiServer, user: &str, extra: Value) {
    let mut rules = vec![json!({
        "apiGroups": ["admissionregistration.k8s.io"],
        "resources": ["validatingadmissionpolicies", "validatingadmissionpolicybindings"],
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
    seed(TestApiServer::builder().rbac().build()).await
}

async fn seed(api: TestApiServer) -> TestApiServer {
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

/// authz.go:30-37, 52-89: create with a paramKind needs `get` on all objects
/// of the kind.
#[tokio::test]
async fn policy_create_with_param_kind_needs_get_on_the_kind() {
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

    // No paramKind: nothing to authorize.
    let (s, body) = as_user(&api, "alice", "POST", VAP, Some(&policy("p2", None))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

/// resolver/resolver.go:36-60: any kind the server serves resolves through
/// discovery, not just a hand-picked table. `Lease` (coordination.k8s.io) is
/// resolved to `leases`, so `get` on `leases` is enough; it is NOT authorized
/// on resource `*`.
#[tokio::test]
async fn any_served_param_kind_resolves_to_its_exact_resource() {
    let api = api().await;
    grant(
        &api,
        "dave",
        json!([{"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "verbs": ["get"]}]),
    )
    .await;
    let lease = json!({"apiVersion": "coordination.k8s.io/v1", "kind": "Lease"});
    let (s, body) = as_user(&api, "dave", "POST", VAP, Some(&policy("pl", Some(lease)))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");

    // alice has no `get` on leases: still refused.
    let lease = json!({"apiVersion": "coordination.k8s.io/v1", "kind": "Lease"});
    let (s, body) = as_user(
        &api,
        "alice",
        "POST",
        VAP,
        Some(&policy("pl2", Some(lease))),
    )
    .await;
    assert_forbidden_field(s, &body, "spec.paramKind", "kind=Lease");
}

/// `EscalationAllowed`: a superuser skips the check.
#[tokio::test]
async fn policy_create_by_a_superuser_skips_the_check() {
    let api = api().await;
    let (s, body) = api.post(VAP, &policy("p1", Some(cm_kind()))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

/// authz.go:39-50: an update is checked only when the paramKind changed.
#[tokio::test]
async fn policy_update_is_checked_only_when_param_kind_changes() {
    let api = api().await;
    let (s, body) = api.post(VAP, &policy("p1", Some(cm_kind()))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let rv = body["metadata"]["resourceVersion"].clone();

    // Same paramKind, different validation: alice is not checked.
    let mut same = policy("p1", Some(cm_kind()));
    same["metadata"]["resourceVersion"] = rv.clone();
    same["spec"]["validations"] = json!([{"expression": "object.spec.replicas <= 6"}]);
    let (s, body) = as_user(&api, "alice", "PUT", &format!("{VAP}/p1"), Some(&same)).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let rv = body["metadata"]["resourceVersion"].clone();

    // Changed paramKind: checked.
    let mut changed = policy("p1", Some(json!({"apiVersion": "v1", "kind": "Secret"})));
    changed["metadata"]["resourceVersion"] = rv;
    let (s, body) = as_user(&api, "alice", "PUT", &format!("{VAP}/p1"), Some(&changed)).await;
    assert_forbidden_field(s, &body, "spec.paramKind", "kind=Secret, apiVersion=v1");
}

/// binding authz.go:60-117: a named paramRef needs `get`, a selector `list`;
/// the resource is found through the policy.
#[tokio::test]
async fn binding_with_param_ref_needs_get_or_list() {
    let api = api().await;
    let (s, body) = api.post(VAP, &policy("p1", Some(cm_kind()))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");

    let named = json!({"name": "cfg", "namespace": "default",
                       "parameterNotFoundAction": "Deny"});
    let (s, body) = as_user(
        &api,
        "alice",
        "POST",
        VAPB,
        Some(&binding("b1", "p1", Some(named.clone()))),
    )
    .await;
    assert_forbidden_field(
        s,
        &body,
        "spec.paramRef",
        "does not have \"get\" permission on the object referenced by paramRef",
    );

    // carol can get, so a named ref passes ...
    let (s, body) = as_user(
        &api,
        "carol",
        "POST",
        VAPB,
        Some(&binding("b1", "p1", Some(named))),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");

    // ... but a selector ref needs `list`.
    let selector = json!({"namespace": "default", "selector": {},
                          "parameterNotFoundAction": "Deny"});
    let (s, body) = as_user(
        &api,
        "carol",
        "POST",
        VAPB,
        Some(&binding("b2", "p1", Some(selector.clone()))),
    )
    .await;
    assert_forbidden_field(s, &body, "spec.paramRef", "\"list\" permission");
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

/// binding authz.go:94-107: a policy that cannot be read leaves the check on
/// `*`/`*`/`*`, and the message says so.
#[tokio::test]
async fn binding_to_an_unknown_policy_needs_permission_on_everything() {
    let api = api().await;
    let (s, body) = as_user(
        &api,
        "bob",
        "POST",
        VAPB,
        Some(&binding(
            "b1",
            "missing",
            Some(json!({"name": "cfg", "namespace": "default",
                        "parameterNotFoundAction": "Deny"})),
        )),
    )
    .await;
    assert_forbidden_field(
        s,
        &body,
        "spec.paramRef",
        "unable to get policy missing to determine minimum required permissions",
    );
}

// ---- the policy plugin's `authorizer` binding (#3528, of #3446) ----------

use async_trait::async_trait;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Forwards to the real authorizer and counts the calls that reach it for
/// `get secrets`, the one check the policies below make.
struct CountingAuthorizer {
    inner: Arc<dyn Authorizer>,
    secret_gets: Arc<AtomicUsize>,
}

impl CountingAuthorizer {
    fn count(&self, attrs: &RequestAttributes) {
        if attrs.resource == "secrets" && attrs.verb == "get" {
            self.secret_gets.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl Authorizer for CountingAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> rusternetes_common::Result<Decision> {
        self.count(attrs);
        self.inner.authorize(attrs).await
    }

    async fn authorize_opinion(
        &self,
        attrs: &RequestAttributes,
    ) -> rusternetes_common::Result<Opinion> {
        self.count(attrs);
        self.inner.authorize_opinion(attrs).await
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> rusternetes_common::Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        self.inner.get_user_rules(user, namespace).await
    }
}

async fn counting_api() -> (TestApiServer, Arc<AtomicUsize>) {
    let secret_gets = Arc::new(AtomicUsize::new(0));
    let counter = secret_gets.clone();
    let api = TestApiServer::builder()
        .rbac()
        .wrap_authorizer(move |inner| {
            Arc::new(CountingAuthorizer {
                inner,
                secret_gets: counter,
            })
        })
        .build();
    let api = seed(api).await;
    let create_cm = json!([{"apiGroups": [""], "resources": ["configmaps"], "verbs": ["create"]}]);
    // erin may create configmaps; frank may also `get` secrets.
    grant(&api, "erin", create_cm.clone()).await;
    let mut frank = create_cm.as_array().unwrap().clone();
    frank.push(json!({"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]}));
    grant(&api, "frank", json!(frank)).await;
    (api, secret_gets)
}

/// A policy on configmap CREATE that holds only when the requesting user may
/// `get` secrets, and a Deny binding for it. Created by the default
/// (`system:masters`) identity.
async fn install_authorizer_policy(api: &TestApiServer, name: &str) {
    let (s, body) = api
        .post(
            VAP,
            &json!({"apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicy", "metadata": {"name": name},
                "spec": {
                    "failurePolicy": "Fail",
                    "matchConstraints": {"resourceRules": [{
                        "apiGroups": [""], "apiVersions": ["v1"],
                        "operations": ["CREATE"], "resources": ["configmaps"]}]},
                    "validations": [{
                        "expression": "authorizer.group('').resource('secrets').check('get').allowed()",
                        "message": "must be able to get secrets"}]}}),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = api
        .post(VAPB, &binding(&format!("{name}-b"), name, None))
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

fn configmap(name: &str) -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name}})
}

const CONFIGMAPS: &str = "/api/v1/namespaces/default/configmaps";

/// handlers/admission.rs passes the request user and the (unfiltered)
/// authorizer to the policy plugin: `authorizer.<group>.<resource>.check(verb)`
/// is the requester's own permission, not the server's.
#[tokio::test]
async fn policy_authorizer_decides_by_the_requesting_users_permissions() {
    let (api, _) = counting_api().await;
    install_authorizer_policy(&api, "needs-secrets").await;

    let (s, body) = as_user(&api, "erin", "POST", CONFIGMAPS, Some(&configmap("denied"))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("must be able to get secrets"),
        "{body}"
    );

    let (s, body) = as_user(
        &api,
        "frank",
        "POST",
        CONFIGMAPS,
        Some(&configmap("allowed")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
}

/// dispatcher.go:117, :221: one `NewCachingAuthorizer` per dispatch, shared by
/// every policy of the request, so N policies asking the same question reach
/// the real authorizer once, while a second request asks again.
#[tokio::test]
async fn one_request_shares_one_caching_authorizer_across_policies() {
    let (api, secret_gets) = counting_api().await;
    for name in ["needs-secrets-a", "needs-secrets-b", "needs-secrets-c"] {
        install_authorizer_policy(&api, name).await;
    }
    secret_gets.store(0, Ordering::SeqCst);

    let (s, body) = as_user(&api, "frank", "POST", CONFIGMAPS, Some(&configmap("one"))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(
        secret_gets.load(Ordering::SeqCst),
        1,
        "three policies, one check"
    );

    let (s, body) = as_user(&api, "frank", "POST", CONFIGMAPS, Some(&configmap("two"))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(
        secret_gets.load(Ordering::SeqCst),
        2,
        "the cache lives for one request"
    );
}
