//! The `certificates/approval` and `certificates/signing` admission plugins
//! (#2104): holding `update` on `certificatesigningrequests/approval` (or
//! `/status`) is not enough, the requester also needs the `approve` (or
//! `sign`) verb on the synthetic `signers` resource named by
//! `spec.signerName`, or on `<domain>/*`.
//!
//! Ported from `plugin/pkg/admission/certificates/{approval,signing}/admission.go`
//! and `pkg/certauthorization/certauthorization.go`.

use axum::http::StatusCode;
use base64::Engine;
use rusternetes_common::{
    resources::{ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, ServiceAccount, Subject},
    types::{ObjectMeta, TypeMeta},
};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const TOKEN_SECRET: &[u8] = b"csr-signer-authz-secret";

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

const CSRS: &str = "/apis/certificates.k8s.io/v1/certificatesigningrequests";
const SIGNER: &str = "example.com/serving";

fn csr_body(name: &str) -> Value {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["example.com".to_string()]).unwrap();
    let pem = params.serialize_request(&key).unwrap().pem().unwrap();
    json!({"apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequest",
           "metadata": {"name": name},
           "spec": {"request": base64::engine::general_purpose::STANDARD.encode(pem),
                    "signerName": SIGNER,
                    "usages": ["digital signature", "server auth"]}})
}

/// A server with a `csr-user` who holds `update` on the approval and status
/// subresources plus `extra` rules, and one CSR created by the test.
async fn setup(extra: Vec<PolicyRule>) -> (TestApiServer, String) {
    let state = spawn_state();
    let uid = seed_sa(&state, "kube-system", "csr-user").await;
    let token = mint_sa_token("kube-system", "csr-user", &uid);
    let mut rules = vec![
        rule(
            &["get", "list", "watch", "create"],
            &["certificates.k8s.io"],
            &["certificatesigningrequests"],
        ),
        rule(
            &["update", "patch"],
            &["certificates.k8s.io"],
            &[
                "certificatesigningrequests/approval",
                "certificatesigningrequests/status",
            ],
        ),
    ];
    rules.extend(extra);
    seed_controller_role(&state, "csr-user", rules).await;
    let (s, _, _, body) = state
        .send_with_headers(
            "POST",
            CSRS,
            &[
                ("content-type", "application/json"),
                ("authorization", &format!("Bearer {token}")),
            ],
            Some(serde_json::to_vec(&csr_body("c1")).unwrap()),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    (state, token)
}

fn signers_rule(verb: &str, names: &[&str]) -> PolicyRule {
    let mut r = rule(&[verb], &["certificates.k8s.io"], &["signers"]);
    r.resource_names = Some(names.iter().map(|s| (*s).to_string()).collect());
    r
}

async fn approve(state: &TestApiServer, token: &str) -> (u16, Value) {
    let (s, _, _, mut csr) = state
        .send_with_headers(
            "GET",
            &format!("{CSRS}/c1"),
            &[("authorization", &format!("Bearer {token}"))],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{csr}");
    csr["status"] = json!({"conditions": [{"type": "Approved", "status": "True",
        "reason": "Test", "message": "approved"}]});
    put_json_bearer(state, &format!("{CSRS}/c1/approval"), token, &csr).await
}

/// approval/admission.go:84-96: no `approve` on the signer is Forbidden.
#[tokio::test]
async fn approval_without_the_approve_verb_is_forbidden() {
    let (state, token) = setup(vec![]).await;
    let (status, body) = approve(&state, &token).await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(
        body["message"],
        "certificatesigningrequests.certificates.k8s.io \"c1\" is forbidden: user not permitted to approve requests with signerName \"example.com/serving\"",
        "{body}"
    );
}

/// `approve` on the named signer allows it.
#[tokio::test]
async fn approval_with_approve_on_the_signer_is_allowed() {
    let (state, token) = setup(vec![signers_rule("approve", &[SIGNER])]).await;
    let (status, body) = approve(&state, &token).await;
    assert_eq!(status, 200, "{body}");
}

/// certauthorization.go:44-56: `approve` on `<domain>/*` covers every signer
/// under that domain.
#[tokio::test]
async fn approval_with_the_domain_wildcard_is_allowed() {
    let (state, token) = setup(vec![signers_rule("approve", &["example.com/*"])]).await;
    let (status, body) = approve(&state, &token).await;
    assert_eq!(status, 200, "{body}");
}

/// `approve` on a different signer does not help.
#[tokio::test]
async fn approval_with_approve_on_another_signer_is_forbidden() {
    let (state, token) = setup(vec![signers_rule("approve", &["example.com/other"])]).await;
    let (status, body) = approve(&state, &token).await;
    assert_eq!(status, 403, "{body}");
}

/// A PATCH of `/approval` goes through the same plugin.
#[tokio::test]
async fn a_patch_of_approval_is_checked_too() {
    let (state, token) = setup(vec![]).await;
    let auth = format!("Bearer {token}");
    let patch = json!({"status": {"conditions": [{"type": "Approved", "status": "True",
        "reason": "Test", "message": "m"}]}});
    let (s, _, _, body) = state
        .send_with_headers(
            "PATCH",
            &format!("{CSRS}/c1/approval"),
            &[
                ("content-type", "application/merge-patch+json"),
                ("authorization", &auth),
            ],
            Some(serde_json::to_vec(&patch).unwrap()),
        )
        .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{body}");
}

/// signing/admission.go:85-99: a change to `status.certificate` needs `sign`.
#[tokio::test]
async fn a_status_certificate_without_the_sign_verb_is_forbidden() {
    let (state, token) = setup(vec![signers_rule("approve", &[SIGNER])]).await;
    let (_, _, _, mut csr) = state
        .send_with_headers(
            "GET",
            &format!("{CSRS}/c1"),
            &[("authorization", &format!("Bearer {token}"))],
            None,
        )
        .await;
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["example.com".to_string()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    csr["status"] =
        json!({"certificate": base64::engine::general_purpose::STANDARD.encode(cert.pem())});
    let (status, body) = put_json_bearer(&state, &format!("{CSRS}/c1/status"), &token, &csr).await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(
        body["message"],
        "certificatesigningrequests.certificates.k8s.io \"c1\" is forbidden: user not permitted to sign requests with signerName \"example.com/serving\"",
        "{body}"
    );
}

/// A `/status` update that changes neither certificate nor conditions is not
/// a signing action (signing/admission.go:84-87).
#[tokio::test]
async fn a_status_update_that_changes_nothing_needs_no_sign() {
    let (state, token) = setup(vec![]).await;
    let (_, _, _, csr) = state
        .send_with_headers(
            "GET",
            &format!("{CSRS}/c1"),
            &[("authorization", &format!("Bearer {token}"))],
            None,
        )
        .await;
    let (status, body) = put_json_bearer(&state, &format!("{CSRS}/c1/status"), &token, &csr).await;
    assert_eq!(status, 200, "{body}");
}
