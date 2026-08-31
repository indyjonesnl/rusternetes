//! `/status` subresource authorization must use the request's API group.
//!
//! Regression pin for #1817. A stock upstream kube-controller-manager drives
//! Deployment/ReplicaSet progress with `UpdateStatus`, which is a
//! `PUT /apis/apps/v1/namespaces/<ns>/<resource>/<name>/status`. Its identity is
//! the `kube-system` ServiceAccount for that controller
//! (`--use-service-account-credentials`, which kubeadm sets), authorized by the
//! upstream bootstrap ClusterRoles at
//! `plugin/pkg/auth/authorizer/rbac/bootstrappolicy/controller_policy.go`:
//!
//! ```text
//! rbacv1helpers.NewRule("update").Groups(extensionsGroup, appsGroup).Resources("deployments/status").RuleOrDie(),   // :119
//! rbacv1helpers.NewRule("update").Groups(appsGroup, extensionsGroup).Resources("replicasets/status").RuleOrDie(),   // replicaset-controller
//! ```
//!
//! Those rules name the `apps` / `extensions` groups only. The api-server's
//! status handlers built their `RequestAttributes` without an API group at all,
//! so every `/status` check ran as the core (`""`) group and never matched —
//! Forbidden. The effect in the vanilla-swap api-server leg was that
//! `.status` on every Deployment and ReplicaSet stayed at
//! `{ObservedGeneration:0, Replicas:0, ...}` forever, so the sample-webhook
//! Deployment never went ready and 19 of 68 [sig-api-machinery] specs failed in
//! `[BeforeEach]`.
//!
//! The core-group cases in the same file guard the other direction: `/api/v1/…`
//! must keep authorizing as `""`.

use rusternetes_common::{
    resources::{ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, ServiceAccount, Subject},
    types::{ObjectMeta, TypeMeta},
};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const TOKEN_SECRET: &[u8] = b"status-apigroup-rbac-secret";

/// Real bearer-token authentication + the real `RBACAuthorizer`, so the
/// decision under test is the production one.
fn spawn_state() -> TestApiServer {
    TestApiServer::builder()
        .secret(TOKEN_SECRET)
        .rbac()
        .skip_auth(false)
        .build()
}

/// Seed a ServiceAccount straight through storage and return its UID.
async fn seed_sa(state: &TestApiServer, namespace: &str, name: &str) -> String {
    let uid = format!("uid-{namespace}-{name}");
    let sa = ServiceAccount {
        type_meta: TypeMeta {
            kind: "ServiceAccount".into(),
            api_version: "v1".into(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uid.clone(),
            ..Default::default()
        },
        secrets: None,
        image_pull_secrets: None,
        automount_service_account_token: None,
    };
    state
        .storage
        .create(&build_key("serviceaccounts", Some(namespace), name), &sa)
        .await
        .unwrap();
    uid
}

/// Mint a bearer token for a ServiceAccount, exactly as the TokenRequest
/// handler would (claim shape: upstream `pkg/serviceaccount/claims.go`).
fn mint_sa_token(namespace: &str, sa_name: &str, uid: &str) -> String {
    let now = chrono::Utc::now();
    let claims = rusternetes_common::auth::ServiceAccountClaims {
        sub: format!("system:serviceaccount:{namespace}:{sa_name}"),
        namespace: namespace.to_string(),
        uid: uid.to_string(),
        iat: now.timestamp(),
        exp: (now + chrono::Duration::hours(1)).timestamp(),
        iss: "https://kubernetes.default.svc.cluster.local".to_string(),
        aud: vec!["https://kubernetes.default.svc".to_string()],
        kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
            namespace: namespace.to_string(),
            svcacct: rusternetes_common::auth::KubeRef {
                name: sa_name.to_string(),
                uid: uid.to_string(),
            },
            pod: None,
            node: None,
        }),
        pod_name: None,
        pod_uid: None,
        node_name: None,
        node_uid: None,
    };
    rusternetes_common::auth::TokenManager::new(TOKEN_SECRET)
        .generate_token(claims)
        .unwrap()
}

/// Seed a ClusterRole with `rules` and bind it to `kube-system/<sa_name>`,
/// mirroring `addControllerRole` (controller_policy.go).
async fn seed_controller_role(
    state: &TestApiServer,
    sa_name: &str,
    rules: Vec<PolicyRule>,
) -> String {
    let role_name = format!("system:controller:{sa_name}");
    let cr = ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: role_name.clone(),
            ..Default::default()
        },
        rules,
        aggregation_rule: None,
    };
    state
        .storage
        .create(&build_key("clusterroles", None, &role_name), &cr)
        .await
        .unwrap();

    let crb = ClusterRoleBinding {
        type_meta: TypeMeta {
            kind: "ClusterRoleBinding".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: role_name.clone(),
            ..Default::default()
        },
        subjects: vec![Subject {
            kind: "ServiceAccount".into(),
            name: sa_name.into(),
            api_group: Some(String::new()),
            namespace: Some("kube-system".into()),
        }],
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "ClusterRole".into(),
            name: role_name.clone(),
        },
    };
    state
        .storage
        .create(&build_key("clusterrolebindings", None, &role_name), &crb)
        .await
        .unwrap();
    role_name
}

fn rule(verbs: &[&str], groups: &[&str], resources: &[&str]) -> PolicyRule {
    PolicyRule {
        verbs: verbs.iter().map(|s| (*s).to_string()).collect(),
        api_groups: Some(groups.iter().map(|s| (*s).to_string()).collect()),
        resources: Some(resources.iter().map(|s| (*s).to_string()).collect()),
        resource_names: None,
        non_resource_urls: None,
    }
}

/// PUT `body` to `uri` as the bearer `token`.
async fn put_json_bearer(
    state: &TestApiServer,
    uri: &str,
    token: &str,
    body: &Value,
) -> (u16, Value) {
    let auth = format!("Bearer {token}");
    let bytes = serde_json::to_vec(body).unwrap();
    let (status, _h, _b, value) = state
        .send_with_headers(
            "PUT",
            uri,
            &[
                ("content-type", "application/json"),
                ("authorization", &auth),
            ],
            Some(bytes),
        )
        .await;
    (status.as_u16(), value)
}

// ---------------------------------------------------------------------------
// apps group
// ---------------------------------------------------------------------------

/// `system:controller:deployment-controller` holds `update deployments/status`
/// in the `apps` group and nothing in the core group. The status write must be
/// allowed and must actually land.
#[tokio::test]
async fn deployment_status_update_is_authorized_by_the_apps_group_rule() {
    let state = spawn_state();
    let uid = seed_sa(&state, "kube-system", "deployment-controller").await;
    let token = mint_sa_token("kube-system", "deployment-controller", &uid);
    seed_controller_role(
        &state,
        "deployment-controller",
        vec![
            rule(
                &["get", "list", "watch", "update"],
                &["extensions", "apps"],
                &["deployments"],
            ),
            rule(
                &["update"],
                &["extensions", "apps"],
                &["deployments/status"],
            ),
        ],
    )
    .await;

    let ns = "webhook-1817";
    state
        .storage
        .create(
            &build_key("deployments", Some(ns), "sample-webhook-deployment"),
            &json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "sample-webhook-deployment",
                    "namespace": ns,
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {}
            }),
        )
        .await
        .unwrap();

    let (status, body) = put_json_bearer(
        &state,
        &format!("/apis/apps/v1/namespaces/{ns}/deployments/sample-webhook-deployment/status"),
        &token,
        &json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "sample-webhook-deployment", "namespace": ns},
            "spec": {"replicas": 1},
            "status": {
                "observedGeneration": 1,
                "replicas": 1,
                "updatedReplicas": 1,
                "readyReplicas": 1,
                "availableReplicas": 1,
                "conditions": [{
                    "type": "Available",
                    "status": "True",
                    "reason": "MinimumReplicasAvailable"
                }]
            }
        }),
    )
    .await;

    assert_eq!(
        status, 200,
        "deployment-controller must be allowed to write deployments/status: {body}"
    );
    assert_eq!(body["status"]["observedGeneration"], 1, "{body}");
    assert_eq!(body["status"]["availableReplicas"], 1, "{body}");

    // And the write is durable — the e2e framework re-GETs the Deployment.
    let stored: Value = state
        .storage
        .get(&build_key(
            "deployments",
            Some(ns),
            "sample-webhook-deployment",
        ))
        .await
        .unwrap();
    assert_eq!(stored["status"]["readyReplicas"], 1, "{stored}");
}

/// Same defect, the other resource the vanilla leg tripped over: the GC spec
/// "should orphan RS created by deployment ..." waits on
/// `ReplicaSet.Status.Replicas == Spec.Replicas`.
#[tokio::test]
async fn replicaset_status_update_is_authorized_by_the_apps_group_rule() {
    let state = spawn_state();
    let uid = seed_sa(&state, "kube-system", "replicaset-controller").await;
    let token = mint_sa_token("kube-system", "replicaset-controller", &uid);
    seed_controller_role(
        &state,
        "replicaset-controller",
        vec![
            rule(
                &["get", "list", "watch", "update"],
                &["apps", "extensions"],
                &["replicasets"],
            ),
            rule(
                &["update"],
                &["apps", "extensions"],
                &["replicasets/status"],
            ),
        ],
    )
    .await;

    let ns = "gc-1817";
    state
        .storage
        .create(
            &build_key("replicasets", Some(ns), "simpletest-rs"),
            &json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {"name": "simpletest-rs", "namespace": ns, "generation": 1},
                "spec": {"replicas": 2},
                "status": {}
            }),
        )
        .await
        .unwrap();

    let (status, body) = put_json_bearer(
        &state,
        &format!("/apis/apps/v1/namespaces/{ns}/replicasets/simpletest-rs/status"),
        &token,
        &json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {"name": "simpletest-rs", "namespace": ns},
            "spec": {"replicas": 2},
            "status": {"replicas": 2, "readyReplicas": 2, "observedGeneration": 1}
        }),
    )
    .await;

    assert_eq!(
        status, 200,
        "replicaset-controller must be allowed to write replicasets/status: {body}"
    );
    assert_eq!(body["status"]["replicas"], 2, "{body}");
}

/// A rule that names only the core group must NOT authorize an `apps` status
/// write — the fix has to read the group from the path, not ignore it.
#[tokio::test]
async fn core_group_rule_does_not_authorize_an_apps_status_write() {
    let state = spawn_state();
    let uid = seed_sa(&state, "kube-system", "confused-controller").await;
    let token = mint_sa_token("kube-system", "confused-controller", &uid);
    seed_controller_role(
        &state,
        "confused-controller",
        vec![rule(&["update"], &[""], &["deployments/status"])],
    )
    .await;

    let ns = "wrong-group-1817";
    state
        .storage
        .create(
            &build_key("deployments", Some(ns), "d"),
            &json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "d", "namespace": ns},
                "spec": {"replicas": 1},
                "status": {}
            }),
        )
        .await
        .unwrap();

    let (status, _body) = put_json_bearer(
        &state,
        &format!("/apis/apps/v1/namespaces/{ns}/deployments/d/status"),
        &token,
        &json!({"status": {"replicas": 1}}),
    )
    .await;
    assert_eq!(status, 403, "core-group rule must not grant apps/v1 status");
}

// ---------------------------------------------------------------------------
// core group — the direction that already worked, pinned so it stays working
// ---------------------------------------------------------------------------

/// `/api/v1/...` is the groupless prefix, so a rule with `apiGroups: [""]` must
/// still authorize a pod status write (this is the kubelet's path).
#[tokio::test]
async fn pod_status_update_stays_authorized_by_the_core_group_rule() {
    let state = spawn_state();
    let uid = seed_sa(&state, "kube-system", "pod-status-writer").await;
    let token = mint_sa_token("kube-system", "pod-status-writer", &uid);
    seed_controller_role(
        &state,
        "pod-status-writer",
        vec![rule(&["update"], &[""], &["pods/status"])],
    )
    .await;

    let ns = "core-1817";
    state
        .storage
        .create(
            &build_key("pods", Some(ns), "p"),
            &json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "p", "namespace": ns},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let (status, body) = put_json_bearer(
        &state,
        &format!("/api/v1/namespaces/{ns}/pods/p/status"),
        &token,
        &json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "p", "namespace": ns},
            "status": {"phase": "Running"}
        }),
    )
    .await;

    assert_eq!(
        status, 200,
        "core-group pods/status must stay allowed: {body}"
    );
    assert_eq!(body["status"]["phase"], "Running", "{body}");
}
