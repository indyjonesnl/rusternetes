//! The `/status` subresource must be authorized against the request's real API
//! group.
//!
//! RBAC PolicyRules name a group, so a rule granting
//! `apiGroups:["apps"], resources:["deployments/status"], verbs:["update"]`
//! only matches a request whose attributes carry `apiGroup == "apps"`. The
//! generic status handler used to authorize every status write with an empty
//! (core) group, so no rule matched and every non-core status update was
//! denied — with the union authorizer's aggregated reason
//! "Not a node user; User does not have permission to perform this action".
//!
//! That is exactly the RBAC upstream ships for the controllers whose status
//! writes matter: `plugin/pkg/auth/authorizer/rbac/bootstrappolicy/controller_policy.go`
//! grants the deployment controller
//! `rbacv1helpers.NewRule("update").Groups(appsGroup, extensionsGroup).Resources("deployments/status")`.
//! In a vanilla-swap cluster the kube-controller-manager runs its controllers
//! under those per-controller ServiceAccounts (`--use-service-account-credentials`,
//! which kubeadm sets), so the bug silently stopped Deployment/ReplicaSet/
//! DaemonSet status from ever being written.

use rusternetes_common::{
    auth::{ServiceAccountClaims, TokenManager},
    resources::{ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, ServiceAccount, Subject},
    types::{ObjectMeta, TypeMeta},
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;
use std::sync::Arc;

const SECRET: &[u8] = b"status-authz-test-secret";
const SA_NAMESPACE: &str = "kube-system";
const SA_NAME: &str = "deployment-controller";
const SA_UID: &str = "9c99fb60-9878-4e34-939e-f3c60c3d6a4b";

/// Seed the ServiceAccount plus the upstream deployment-controller RBAC: the
/// ONLY grant is `apps/deployments/status: update`, so the request can succeed
/// solely through a correctly-grouped authorization decision.
async fn seed_deployment_controller_rbac(mem: &Arc<MemoryStorage>) {
    let sa = ServiceAccount {
        type_meta: TypeMeta {
            kind: "ServiceAccount".into(),
            api_version: "v1".into(),
        },
        metadata: ObjectMeta {
            name: SA_NAME.into(),
            namespace: Some(SA_NAMESPACE.into()),
            uid: SA_UID.into(),
            ..Default::default()
        },
        secrets: None,
        image_pull_secrets: None,
        automount_service_account_token: None,
    };
    mem.create(
        &build_key("serviceaccounts", Some(SA_NAMESPACE), SA_NAME),
        &sa,
    )
    .await
    .unwrap();

    let cr = ClusterRole {
        type_meta: TypeMeta {
            kind: "ClusterRole".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "system:controller:deployment-controller".into(),
            ..Default::default()
        },
        rules: vec![PolicyRule {
            verbs: vec!["update".into()],
            api_groups: Some(vec!["apps".into(), "extensions".into()]),
            resources: Some(vec!["deployments/status".into()]),
            resource_names: None,
            non_resource_urls: None,
        }],
        aggregation_rule: None,
    };
    mem.create(
        &build_key(
            "clusterroles",
            None,
            "system:controller:deployment-controller",
        ),
        &cr,
    )
    .await
    .unwrap();

    let crb = ClusterRoleBinding {
        type_meta: TypeMeta {
            kind: "ClusterRoleBinding".into(),
            api_version: "rbac.authorization.k8s.io/v1".into(),
        },
        metadata: ObjectMeta {
            name: "system:controller:deployment-controller".into(),
            ..Default::default()
        },
        subjects: vec![Subject {
            kind: "ServiceAccount".into(),
            name: SA_NAME.into(),
            api_group: None,
            namespace: Some(SA_NAMESPACE.into()),
        }],
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "ClusterRole".into(),
            name: "system:controller:deployment-controller".into(),
        },
    };
    mem.create(
        &build_key(
            "clusterrolebindings",
            None,
            "system:controller:deployment-controller",
        ),
        &crb,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn deployment_status_update_is_authorized_with_the_apps_group() {
    let api = TestApiServer::builder()
        .rbac()
        .skip_auth(false)
        .secret(SECRET)
        .build();
    let mem = api.storage.clone();
    seed_deployment_controller_rbac(&mem).await;

    // A stored Deployment for the controller to write status onto.
    mem.create(
        &build_key("deployments", Some("default"), "probe"),
        &json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "probe", "namespace": "default", "generation": 1},
            "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "probe"}},
                     "template": {"metadata": {"labels": {"app": "probe"}},
                                  "spec": {"containers": [{"name": "c", "image": "pause"}]}}},
            "status": {}
        }),
    )
    .await
    .unwrap();

    let token = TokenManager::new(SECRET)
        .generate_token(ServiceAccountClaims::new(
            SA_NAME.into(),
            SA_NAMESPACE.into(),
            SA_UID.into(),
            24,
        ))
        .expect("mint SA token");
    let bearer = format!("Bearer {token}");

    let body = serde_json::to_vec(&json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "probe", "namespace": "default"},
        "status": {"observedGeneration": 1, "replicas": 1, "readyReplicas": 1,
                   "updatedReplicas": 1, "availableReplicas": 1}
    }))
    .unwrap();

    let (status, _headers, _bytes, value) = api
        .send_with_headers(
            "PUT",
            "/apis/apps/v1/namespaces/default/deployments/probe/status",
            &[
                ("authorization", bearer.as_str()),
                ("content-type", "application/json"),
            ],
            Some(body),
        )
        .await;

    assert_eq!(
        status.as_u16(),
        200,
        "deployments/status update must be allowed by the apps-group rule, got {}: {}",
        status,
        value
    );
    assert_eq!(
        value["status"]["observedGeneration"], 1,
        "status must be persisted: {value}"
    );
}
