//! Table-driven unit tests for `RBACAuthorizer` and `NodeAuthorizer`.
//!
//! Mirrors the structure and coverage intent of the upstream Go test files:
//!   - <https://github.com/kubernetes/kubernetes/blob/master/plugin/pkg/auth/authorizer/rbac/rbac_test.go>
//!   - <https://github.com/kubernetes/kubernetes/blob/master/plugin/pkg/auth/authorizer/node/node_authorizer_test.go>
//!
//! Each authorizer is tested through the public `Authorizer::authorize` API.
//! Subjects are exercised via Role/RoleBinding and ClusterRole/ClusterRoleBinding
//! objects seeded into a purpose-built `MockAuthzStorage` that holds RBAC objects
//! in memory, mirroring what `rusternetes_storage::MemoryStorage` would do in
//! integration tests.

use async_trait::async_trait;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::node_graph::{GraphPopulator, NodeGraph};
use rusternetes_common::authz::{
    AlwaysAllowAuthorizer, AlwaysDenyAuthorizer, Authorizer, AuthzStorage, Decision,
    NodeAuthorizer, Opinion, RBACAuthorizer, RequestAttributes,
};
use rusternetes_common::error::Result;
use rusternetes_common::resources::rbac::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Minimal in-test AuthzStorage so we don't need rusternetes-storage as a dep
// ---------------------------------------------------------------------------

/// Simple in-memory storage for RBAC objects, keyed by the same path patterns
/// that `MemoryStorage`'s `AuthzStorage` impl uses:
///   `/registry/roles/<ns>/<name>`
///   `/registry/rolebindings/<ns>/<name>`
///   `/registry/clusterroles/<name>`
///   `/registry/clusterrolebindings/<name>`
#[derive(Default)]
struct MockAuthzStorage {
    /// Serialized JSON blobs keyed by storage path.
    store: HashMap<String, String>,
}

impl MockAuthzStorage {
    fn new() -> Self {
        Self::default()
    }

    fn insert_role(&mut self, role: &Role) {
        let ns = role
            .metadata
            .namespace
            .as_deref()
            .expect("Role must have a namespace");
        let key = format!("/registry/roles/{}/{}", ns, role.metadata.name);
        self.store.insert(key, serde_json::to_string(role).unwrap());
    }

    fn insert_role_binding(&mut self, rb: &RoleBinding) {
        let ns = rb
            .metadata
            .namespace
            .as_deref()
            .expect("RoleBinding must have a namespace");
        let key = format!("/registry/rolebindings/{}/{}", ns, rb.metadata.name);
        self.store.insert(key, serde_json::to_string(rb).unwrap());
    }

    fn insert_cluster_role(&mut self, cr: &ClusterRole) {
        let key = format!("/registry/clusterroles/{}", cr.metadata.name);
        self.store.insert(key, serde_json::to_string(cr).unwrap());
    }

    fn insert_cluster_role_binding(&mut self, crb: &ClusterRoleBinding) {
        let key = format!("/registry/clusterrolebindings/{}", crb.metadata.name);
        self.store.insert(key, serde_json::to_string(crb).unwrap());
    }
}

#[async_trait]
impl AuthzStorage for MockAuthzStorage {
    /// Retrieve a single object by resource-type-derived key.
    ///
    /// Mirrors the `type_name` dispatch in `memory.rs` / `etcd.rs`.
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let tn = std::any::type_name::<T>();
        let full_key = match namespace {
            Some(ns) => {
                if tn.contains("RoleBinding") && !tn.contains("Cluster") {
                    format!("/registry/rolebindings/{}/{}", ns, key)
                } else if tn.contains("Role") && !tn.contains("Cluster") {
                    format!("/registry/roles/{}/{}", ns, key)
                } else {
                    format!("/registry/unknown/{}/{}", ns, key)
                }
            }
            None => {
                if tn.contains("ClusterRoleBinding") {
                    format!("/registry/clusterrolebindings/{}", key)
                } else if tn.contains("ClusterRole") && !tn.contains("Binding") {
                    format!("/registry/clusterroles/{}", key)
                } else {
                    format!("/registry/unknown/{}", key)
                }
            }
        };

        let json = self.store.get(&full_key).ok_or_else(|| {
            rusternetes_common::error::Error::NotFound(format!("key not found: {}", full_key))
        })?;

        serde_json::from_str(json).map_err(rusternetes_common::error::Error::Serialization)
    }

    /// List all objects of a given type in a namespace (or cluster-scope).
    async fn list<T>(&self, namespace: Option<&str>) -> Result<Vec<T>>
    where
        T: serde::Serialize + DeserializeOwned + Send + Sync,
    {
        let tn = std::any::type_name::<T>();
        let prefix = match namespace {
            Some(ns) => {
                if tn.contains("RoleBinding") && !tn.contains("Cluster") {
                    format!("/registry/rolebindings/{}/", ns)
                } else if tn.contains("Role") && !tn.contains("Cluster") {
                    format!("/registry/roles/{}/", ns)
                } else {
                    format!("/registry/unknown/{}/", ns)
                }
            }
            None => {
                if tn.contains("ClusterRoleBinding") {
                    "/registry/clusterrolebindings/".to_string()
                } else if tn.contains("ClusterRole") && !tn.contains("Binding") {
                    "/registry/clusterroles/".to_string()
                } else {
                    "/registry/unknown/".to_string()
                }
            }
        };

        let mut out = Vec::new();
        for (k, v) in &self.store {
            if k.starts_with(&prefix) {
                let item: T = serde_json::from_str(v)
                    .map_err(rusternetes_common::error::Error::Serialization)?;
                out.push(item);
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn user(username: &str) -> UserInfo {
    UserInfo {
        username: username.to_string(),
        uid: "uid-1".to_string(),
        groups: vec![],
        extra: HashMap::new(),
    }
}

fn user_with_groups(username: &str, groups: &[&str]) -> UserInfo {
    UserInfo {
        username: username.to_string(),
        uid: "uid-1".to_string(),
        groups: groups.iter().map(|s| s.to_string()).collect(),
        extra: HashMap::new(),
    }
}

fn service_account_user(namespace: &str, name: &str) -> UserInfo {
    user(&format!("system:serviceaccount:{}:{}", namespace, name))
}

fn policy_rule(
    verbs: &[&str],
    api_groups: &[&str],
    resources: &[&str],
    resource_names: Option<&[&str]>,
) -> PolicyRule {
    PolicyRule {
        verbs: verbs.iter().map(|s| s.to_string()).collect(),
        api_groups: Some(api_groups.iter().map(|s| s.to_string()).collect()),
        resources: Some(resources.iter().map(|s| s.to_string()).collect()),
        resource_names: resource_names.map(|names| names.iter().map(|s| s.to_string()).collect()),
        non_resource_urls: None,
    }
}

fn non_resource_rule(verbs: &[&str], urls: &[&str]) -> PolicyRule {
    PolicyRule {
        verbs: verbs.iter().map(|s| s.to_string()).collect(),
        api_groups: None,
        resources: None,
        resource_names: None,
        non_resource_urls: Some(urls.iter().map(|s| s.to_string()).collect()),
    }
}

fn rbac_authorizer(store: MockAuthzStorage) -> RBACAuthorizer<MockAuthzStorage> {
    RBACAuthorizer::new(Arc::new(store))
}

// ---------------------------------------------------------------------------
// AlwaysAllow / AlwaysDeny sanity tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn always_allow_authorizer_allows_everything() {
    let az = AlwaysAllowAuthorizer;
    let attrs = RequestAttributes::new(user("bob"), "delete", "pods")
        .with_namespace("default")
        .with_api_group("");
    let decision = az.authorize(&attrs).await.unwrap();
    assert_eq!(decision, Decision::Allow);
}

#[tokio::test]
async fn always_deny_authorizer_denies_everything() {
    let az = AlwaysDenyAuthorizer;
    let attrs = RequestAttributes::new(user("bob"), "get", "pods")
        .with_namespace("default")
        .with_api_group("");
    let decision = az.authorize(&attrs).await.unwrap();
    assert!(matches!(decision, Decision::Deny(_)));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — no username-only system:admin bypass
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_system_admin_requires_a_binding() {
    // rbac.go:75-80,122-126: RBAC only allows a matching rule. Privileged
    // groups are handled separately (authorizerfactory/builtin.go:82-96).
    let store = MockAuthzStorage::new();
    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("system:admin"), "delete", "pods")
        .with_namespace("kube-system")
        .with_api_group("");

    assert_eq!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(String::new())
    );
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — User subject via Role + RoleBinding
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_user_allowed_by_role_binding() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("pod-reader", "default").with_rules(vec![policy_rule(
        &["get", "list", "watch"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("bob-pod-reader", "default")
        .with_subjects(vec![Subject::user("bob")])
        .with_role_ref(RoleRef::role("pod-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    // Allowed verbs
    for verb in ["get", "list", "watch"] {
        let attrs = RequestAttributes::new(user("bob"), verb, "pods")
            .with_namespace("default")
            .with_api_group("");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "expected Allow for verb={verb}"
        );
    }

    // Denied verb
    let attrs = RequestAttributes::new(user("bob"), "delete", "pods")
        .with_namespace("default")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_user_denied_in_different_namespace() {
    // RoleBinding is in "default"; request is for "other" namespace — must deny.
    let mut store = MockAuthzStorage::new();

    let role = Role::new("pod-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("bob-pod-reader", "default")
        .with_subjects(vec![Subject::user("bob")])
        .with_role_ref(RoleRef::role("pod-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("bob"), "get", "pods")
        .with_namespace("other")
        .with_api_group("");

    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_user_denied_wrong_resource() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("pod-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("bob-pod-reader", "default")
        .with_subjects(vec![Subject::user("bob")])
        .with_role_ref(RoleRef::role("pod-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("bob"), "get", "secrets")
        .with_namespace("default")
        .with_api_group("");

    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — wildcard verb / resource
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_wildcard_verb_allows_any_verb() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("all-pods", "default").with_rules(vec![policy_rule(
        &["*"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-all-pods", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("all-pods"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    for verb in [
        "get", "list", "watch", "create", "update", "patch", "delete",
    ] {
        let attrs = RequestAttributes::new(user("alice"), verb, "pods")
            .with_namespace("default")
            .with_api_group("");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "wildcard verb should allow {verb}"
        );
    }
}

#[tokio::test]
async fn rbac_wildcard_resource_allows_any_resource() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("all-resources", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["*"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-all", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("all-resources"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    for resource in ["pods", "services", "configmaps", "secrets", "deployments"] {
        let attrs = RequestAttributes::new(user("alice"), "get", resource)
            .with_namespace("default")
            .with_api_group("");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "wildcard resource should allow {resource}"
        );
    }
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — resourceNames restriction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_resource_name_restriction_allows_matching_name() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("named-secret", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["secrets"],
        Some(&["my-secret"]),
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-named-secret", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("named-secret"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    // Allowed — name matches
    let attrs = RequestAttributes::new(user("alice"), "get", "secrets")
        .with_namespace("default")
        .with_api_group("")
        .with_name("my-secret");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_resource_name_restriction_denies_other_name() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("named-secret", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["secrets"],
        Some(&["my-secret"]),
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-named-secret", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("named-secret"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    // Denied — name does not match
    let attrs = RequestAttributes::new(user("alice"), "get", "secrets")
        .with_namespace("default")
        .with_api_group("")
        .with_name("other-secret");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_resource_name_unrestricted_allows_any_name() {
    // Rule has no resourceNames → applies to all names.
    let mut store = MockAuthzStorage::new();

    let role = Role::new("any-secret", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["secrets"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("alice-any-secret", "default")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::role("any-secret"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    for name in ["secret-a", "secret-b", "some-other-secret"] {
        let attrs = RequestAttributes::new(user("alice"), "get", "secrets")
            .with_namespace("default")
            .with_api_group("")
            .with_name(name);
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "unrestricted rule should allow {name}"
        );
    }
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — ClusterRole + ClusterRoleBinding (cluster-scoped)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_cluster_role_binding_allows_cluster_wide() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("node-reader").with_rules(vec![policy_rule(
        &["get", "list"],
        &[""],
        &["nodes"],
        None,
    )]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-node-reader")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("node-reader"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // ClusterRoleBinding → no namespace restriction
    let attrs = RequestAttributes::new(user("alice"), "get", "nodes").with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);

    // Also allowed with a namespace set (binding is cluster-scoped)
    let attrs = RequestAttributes::new(user("alice"), "list", "nodes")
        .with_api_group("")
        .with_namespace("kube-system");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_role_binding_referencing_cluster_role() {
    // A RoleBinding can reference a ClusterRole, scoping it to the binding's namespace.
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("pod-reader").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["pods"],
        None,
    )]);
    store.insert_cluster_role(&cr);

    let rb = RoleBinding::new("alice-pod-reader", "staging")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("pod-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    // Allowed in the binding's namespace
    let attrs = RequestAttributes::new(user("alice"), "get", "pods")
        .with_namespace("staging")
        .with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);

    // Denied in a different namespace
    let attrs = RequestAttributes::new(user("alice"), "get", "pods")
        .with_namespace("production")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — Group subject matching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_group_subject_allows_group_member() {
    let mut store = MockAuthzStorage::new();

    let cr =
        ClusterRole::new("ops-role").with_rules(vec![policy_rule(&["*"], &["*"], &["*"], None)]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("ops-binding")
        .with_subjects(vec![Subject::group("ops-team")])
        .with_role_ref(RoleRef::cluster_role("ops-role"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // User is member of the group
    let attrs = RequestAttributes::new(
        user_with_groups("carol", &["ops-team", "dev-team"]),
        "delete",
        "pods",
    )
    .with_namespace("default")
    .with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_group_subject_denies_non_group_member() {
    let mut store = MockAuthzStorage::new();

    let cr =
        ClusterRole::new("ops-role").with_rules(vec![policy_rule(&["*"], &["*"], &["*"], None)]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("ops-binding")
        .with_subjects(vec![Subject::group("ops-team")])
        .with_role_ref(RoleRef::cluster_role("ops-role"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // User does NOT belong to ops-team
    let attrs = RequestAttributes::new(user_with_groups("dave", &["dev-team"]), "delete", "pods")
        .with_namespace("default")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — ServiceAccount subject matching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_service_account_subject_allowed() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("sa-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["configmaps"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("sa-binding", "default")
        .with_subjects(vec![Subject::service_account("my-sa", "default")])
        .with_role_ref(RoleRef::role("sa-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(
        service_account_user("default", "my-sa"),
        "get",
        "configmaps",
    )
    .with_namespace("default")
    .with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_service_account_subject_denied_wrong_namespace() {
    // SA in "other" namespace should not match a binding for SA in "default".
    let mut store = MockAuthzStorage::new();

    let role = Role::new("sa-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["configmaps"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("sa-binding", "default")
        .with_subjects(vec![Subject::service_account("my-sa", "default")])
        .with_role_ref(RoleRef::role("sa-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(service_account_user("other", "my-sa"), "get", "configmaps")
        .with_namespace("default")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_service_account_subject_denied_wrong_name() {
    let mut store = MockAuthzStorage::new();

    let role = Role::new("sa-reader", "default").with_rules(vec![policy_rule(
        &["get"],
        &[""],
        &["configmaps"],
        None,
    )]);
    store.insert_role(&role);

    let rb = RoleBinding::new("sa-binding", "default")
        .with_subjects(vec![Subject::service_account("my-sa", "default")])
        .with_role_ref(RoleRef::role("sa-reader"));
    store.insert_role_binding(&rb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(
        service_account_user("default", "other-sa"),
        "get",
        "configmaps",
    )
    .with_namespace("default")
    .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — nonResourceURLs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_non_resource_url_allowed() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("metrics-reader")
        .with_rules(vec![non_resource_rule(&["get"], &["/metrics", "/healthz"])]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-metrics")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("metrics-reader"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/metrics");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);

    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/healthz");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_non_resource_url_denied_unlisted_path() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("metrics-reader")
        .with_rules(vec![non_resource_rule(&["get"], &["/metrics"])]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-metrics")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("metrics-reader"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/livez");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_non_resource_url_wildcard_allows_any_path() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("url-wildcard").with_rules(vec![non_resource_rule(&["*"], &["*"])]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-url-wildcard")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("url-wildcard"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    for path in ["/metrics", "/healthz", "/livez", "/readyz", "/version"] {
        let attrs = RequestAttributes::new_non_resource(user("alice"), "get", path);
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "wildcard URL should allow path {path}"
        );
    }
}

// Note: authz.rs uses `path.starts_with(url)` for non-resource URL matching,
// which means a rule for "/health" also permits "/healthz".  This test
// documents the current (permissive) behaviour so that future tightening
// shows up as a deliberate change rather than a surprise breakage.
#[tokio::test]
async fn rbac_non_resource_url_prefix_match_is_permissive() {
    let mut store = MockAuthzStorage::new();

    // Rule grants only "/health" — NOT "/healthz".
    let cr =
        ClusterRole::new("health-only").with_rules(vec![non_resource_rule(&["get"], &["/health"])]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-health-only")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("health-only"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // "/health" itself is allowed.
    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/health");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);

    // "/healthz" is currently also allowed because of the starts_with check.
    // This documents a known permissive deviation from strict K8s semantics.
    let attrs = RequestAttributes::new_non_resource(user("alice"), "get", "/healthz");
    assert_eq!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Allow,
        "current impl allows /healthz when rule is /health (starts_with); \
         tighten authz.rs to require trailing '/*' for prefix semantics"
    );
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — API group matching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_api_group_required_allows_matching_group() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("deployment-writer").with_rules(vec![policy_rule(
        &["create", "update"],
        &["apps"],
        &["deployments"],
        None,
    )]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-deployments")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("deployment-writer"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("alice"), "create", "deployments")
        .with_api_group("apps")
        .with_namespace("default");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
}

#[tokio::test]
async fn rbac_api_group_required_denies_wrong_group() {
    let mut store = MockAuthzStorage::new();

    let cr = ClusterRole::new("deployment-writer").with_rules(vec![policy_rule(
        &["create"],
        &["apps"],
        &["deployments"],
        None,
    )]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-deployments")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("deployment-writer"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    // Wrong API group ("" instead of "apps")
    let attrs = RequestAttributes::new(user("alice"), "create", "deployments")
        .with_api_group("")
        .with_namespace("default");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

#[tokio::test]
async fn rbac_wildcard_api_group_allows_any_group() {
    let mut store = MockAuthzStorage::new();

    let cr =
        ClusterRole::new("any-group").with_rules(vec![policy_rule(&["get"], &["*"], &["*"], None)]);
    store.insert_cluster_role(&cr);

    let crb = ClusterRoleBinding::new("alice-any-group")
        .with_subjects(vec![Subject::user("alice")])
        .with_role_ref(RoleRef::cluster_role("any-group"));
    store.insert_cluster_role_binding(&crb);

    let az = rbac_authorizer(store);

    for group in ["", "apps", "batch", "extensions", "storage.k8s.io"] {
        let attrs = RequestAttributes::new(user("alice"), "get", "anything")
            .with_api_group(group)
            .with_namespace("default");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "wildcard api group should allow group={group}"
        );
    }
}

// ---------------------------------------------------------------------------
// RBACAuthorizer — no bindings → deny
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rbac_no_bindings_denies_everything() {
    let store = MockAuthzStorage::new(); // empty
    let az = rbac_authorizer(store);

    let attrs = RequestAttributes::new(user("nobody"), "get", "pods")
        .with_namespace("default")
        .with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}

// ---------------------------------------------------------------------------
// NodeAuthorizer — public-API checks. The table ported from upstream's
// node_authorizer_test.go (every resource family, selector and feature
// variants) lives next to the implementation in
// `crates/common/src/authz/node_authorizer_tests.rs`; these exercise the
// graph through `NodeGraph` / `GraphPopulator` the way the api-server does.
// ---------------------------------------------------------------------------

fn node_identity(node_name: &str) -> UserInfo {
    user_with_groups(&format!("system:node:{node_name}"), &["system:nodes"])
}

fn graph_with_pod(node: &str) -> Arc<NodeGraph> {
    let graph = Arc::new(NodeGraph::new());
    let pod: rusternetes_common::resources::Pod = serde_json::from_value(serde_json::json!({
        "metadata": {"name": "p", "namespace": "ns", "uid": "u"},
        "spec": {
            "nodeName": node,
            "containers": [],
            "volumes": [{"name": "v", "secret": {"secretName": "bound"}}],
        },
    }))
    .unwrap();
    GraphPopulator::new(graph.clone()).add_pod(&pod);
    graph
}

#[tokio::test]
async fn node_authorizer_secret_follows_the_pod_graph() {
    let az = NodeAuthorizer::new(graph_with_pod("worker-1"));
    let get = |node: &str, secret: &str| {
        RequestAttributes::new(node_identity(node), "get", "secrets")
            .with_api_group("")
            .with_namespace("ns")
            .with_name(secret)
    };
    assert_eq!(
        az.authorize(&get("worker-1", "bound")).await.unwrap(),
        Decision::Allow
    );
    // Another node, and a secret no pod on the node references.
    for attrs in [get("worker-2", "bound"), get("worker-1", "other")] {
        assert!(
            matches!(az.authorize(&attrs).await.unwrap(), Decision::Deny(_)),
            "{attrs:?}"
        );
    }
}

#[tokio::test]
async fn node_authorizer_reasons_are_upstreams() {
    let az = NodeAuthorizer::new(graph_with_pod("worker-1"));
    let attrs = RequestAttributes::new(node_identity("worker-1"), "get", "secrets")
        .with_api_group("")
        .with_namespace("ns")
        .with_name("other");
    assert_eq!(
        az.authorize_opinion(&attrs).await.unwrap(),
        Opinion::NoOpinion {
            reason: "no relationship found between node 'worker-1' and this object".into(),
            error: None
        }
    );
    // A graph lookup is only attempted for a named object.
    let list = RequestAttributes::new(node_identity("worker-1"), "list", "secrets")
        .with_api_group("")
        .with_namespace("ns");
    assert_eq!(
        az.authorize_opinion(&list).await.unwrap(),
        Opinion::NoOpinion {
            reason: "No Object name found".into(),
            error: None
        }
    );
}

#[tokio::test]
async fn node_authorizer_reads_own_node_and_nothing_else_of_nodes() {
    let az = NodeAuthorizer::new(Arc::new(NodeGraph::new()));
    let get = |name: &str| {
        RequestAttributes::new(node_identity("worker-1"), "get", "nodes")
            .with_api_group("")
            .with_name(name)
    };
    assert_eq!(
        az.authorize(&get("worker-1")).await.unwrap(),
        Decision::Allow
    );
    assert_eq!(
        az.authorize_opinion(&get("worker-2")).await.unwrap(),
        Opinion::NoOpinion {
            reason: "node 'worker-1' cannot read 'worker-2', only its own Node object".into(),
            error: None
        }
    );
}

#[tokio::test]
async fn node_authorizer_requires_the_system_nodes_group() {
    // pkg/auth/nodeidentifier/default.go:42-68: prefix AND group.
    let az = NodeAuthorizer::new(Arc::new(NodeGraph::new()));
    for u in [
        user("alice"),
        user("system:node:worker-1"),
        user_with_groups("not-a-node", &["system:nodes"]),
    ] {
        let attrs = RequestAttributes::new(u, "get", "nodes")
            .with_api_group("")
            .with_name("worker-1");
        assert!(matches!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Deny(_)
        ));
        assert_eq!(
            az.authorize_opinion(&attrs).await.unwrap(),
            Opinion::NoOpinion {
                reason: String::new(),
                error: None
            }
        );
    }
}

#[tokio::test]
async fn node_authorizer_static_rules_cover_unsubdivided_resources() {
    let az = NodeAuthorizer::new(Arc::new(NodeGraph::new()));
    for verb in ["get", "list", "watch"] {
        let attrs =
            RequestAttributes::new(node_identity("worker-1"), verb, "services").with_api_group("");
        assert_eq!(
            az.authorize(&attrs).await.unwrap(),
            Decision::Allow,
            "{verb}"
        );
    }
    let attrs =
        RequestAttributes::new(node_identity("worker-1"), "create", "events").with_api_group("");
    assert_eq!(az.authorize(&attrs).await.unwrap(), Decision::Allow);
    let attrs =
        RequestAttributes::new(node_identity("worker-1"), "delete", "services").with_api_group("");
    assert!(matches!(
        az.authorize(&attrs).await.unwrap(),
        Decision::Deny(_)
    ));
}
