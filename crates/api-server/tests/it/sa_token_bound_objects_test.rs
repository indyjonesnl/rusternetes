//! Service-account token binding (#1575): a token bound to a Pod, Secret or
//! Node stops authenticating when that object is deleted or recreated, and
//! `TokenRequest` records / validates the binding.
//!
//! Ported from `pkg/serviceaccount/claims.go` (`validator.Validate`, `Claims`)
//! and `pkg/registry/core/serviceaccount/storage/token.go` (`TokenREST.Create`).

use base64::Engine;
use rusternetes_common::auth::{KubeRef, KubernetesClaims, ServiceAccountClaims, TokenManager};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SECRET: &[u8] = b"sa-token-bound-objects";

fn auth_server() -> TestApiServer {
    TestApiServer::builder()
        .secret(SECRET)
        .rbac()
        .skip_auth(false)
        .build()
}

async fn seed(s: &TestApiServer, resource: &str, ns: Option<&str>, name: &str, v: Value) {
    s.storage
        .create(&build_key(resource, ns, name), &v)
        .await
        .unwrap();
}

async fn seed_sa(s: &TestApiServer, ns: &str, name: &str, uid: &str) {
    seed(
        s,
        "serviceaccounts",
        Some(ns),
        name,
        json!({"apiVersion": "v1", "kind": "ServiceAccount",
               "metadata": {"name": name, "namespace": ns, "uid": uid}}),
    )
    .await;
}

async fn seed_pod(s: &TestApiServer, ns: &str, name: &str, uid: &str, sa: &str, node: &str) {
    seed(
        s,
        "pods",
        Some(ns),
        name,
        json!({"apiVersion": "v1", "kind": "Pod",
               "metadata": {"name": name, "namespace": ns, "uid": uid},
               "spec": {"serviceAccountName": sa, "nodeName": node,
                        "containers": [{"name": "c", "image": "busybox"}]}}),
    )
    .await;
}

async fn seed_secret(s: &TestApiServer, ns: &str, name: &str, uid: &str) {
    seed(
        s,
        "secrets",
        Some(ns),
        name,
        json!({"apiVersion": "v1", "kind": "Secret",
               "metadata": {"name": name, "namespace": ns, "uid": uid}}),
    )
    .await;
}

async fn seed_node(s: &TestApiServer, name: &str, uid: &str) {
    seed(
        s,
        "nodes",
        None,
        name,
        json!({"apiVersion": "v1", "kind": "Node", "metadata": {"name": name, "uid": uid}}),
    )
    .await;
}

fn token_with(bound: impl FnOnce(&mut KubernetesClaims)) -> String {
    let now = chrono::Utc::now();
    let mut k = KubernetesClaims {
        namespace: "ns".into(),
        svcacct: KubeRef {
            name: "sa".into(),
            uid: "sa-uid".into(),
        },
        pod: None,
        node: None,
        secret: None,
    };
    bound(&mut k);
    let claims = ServiceAccountClaims {
        sub: "system:serviceaccount:ns:sa".into(),
        namespace: "ns".into(),
        uid: "sa-uid".into(),
        iat: now.timestamp(),
        exp: (now + chrono::Duration::hours(1)).timestamp(),
        iss: "https://kubernetes.default.svc.cluster.local".into(),
        aud: vec!["https://kubernetes.default.svc".into()],
        kubernetes: Some(k),
        pod_name: None,
        pod_uid: None,
        node_name: None,
        node_uid: None,
    };
    TokenManager::new(SECRET).generate_token(claims).unwrap()
}

/// Authenticated status of a request: 401 = the token was rejected; anything
/// else (403 from RBAC) = it authenticated.
async fn status_with(s: &TestApiServer, token: &str) -> u16 {
    let auth = format!("Bearer {token}");
    let (st, _h, _b, _v) = s
        .send_with_headers(
            "GET",
            "/api/v1/namespaces/ns/configmaps",
            &[("authorization", &auth)],
            None,
        )
        .await;
    st.as_u16()
}

#[tokio::test]
async fn pod_bound_token_dies_with_the_pod() {
    let s = auth_server();
    seed_sa(&s, "ns", "sa", "sa-uid").await;
    let token = token_with(|k| {
        k.pod = Some(KubeRef {
            name: "p".into(),
            uid: "pod-uid".into(),
        })
    });

    // pod missing: invalidated (claims.go:215-219)
    assert_eq!(status_with(&s, &token).await, 401);

    seed_pod(&s, "ns", "p", "pod-uid", "sa", "n1").await;
    assert_ne!(status_with(&s, &token).await, 401);

    // recreated pod, new uid (claims.go:220-223)
    s.storage
        .delete(&build_key("pods", Some("ns"), "p"))
        .await
        .unwrap();
    seed_pod(&s, "ns", "p", "other-uid", "sa", "n1").await;
    assert_eq!(status_with(&s, &token).await, 401);
}

#[tokio::test]
async fn pod_bound_token_dies_when_pod_is_being_deleted_past_the_leeway() {
    let s = auth_server();
    seed_sa(&s, "ns", "sa", "sa-uid").await;
    let token = token_with(|k| {
        k.pod = Some(KubeRef {
            name: "p".into(),
            uid: "pod-uid".into(),
        })
    });
    let mut pod = json!({"apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "ns", "uid": "pod-uid",
                     "deletionTimestamp": (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339()},
        "spec": {"serviceAccountName": "sa", "containers": [{"name": "c", "image": "busybox"}]}});
    s.storage
        .create(&build_key("pods", Some("ns"), "p"), &pod)
        .await
        .unwrap();
    assert_eq!(status_with(&s, &token).await, 401);

    // deleted within the 1 minute leeway (jwt.DefaultLeeway): still valid
    pod["metadata"]["deletionTimestamp"] = json!(chrono::Utc::now().to_rfc3339());
    s.storage
        .update(&build_key("pods", Some("ns"), "p"), &pod)
        .await
        .unwrap();
    assert_ne!(status_with(&s, &token).await, 401);
}

#[tokio::test]
async fn secret_bound_token_dies_with_the_secret() {
    let s = auth_server();
    seed_sa(&s, "ns", "sa", "sa-uid").await;
    let token = token_with(|k| {
        k.secret = Some(KubeRef {
            name: "sec".into(),
            uid: "sec-uid".into(),
        })
    });
    assert_eq!(status_with(&s, &token).await, 401);
    seed_secret(&s, "ns", "sec", "sec-uid").await;
    assert_ne!(status_with(&s, &token).await, 401);
    s.storage
        .delete(&build_key("secrets", Some("ns"), "sec"))
        .await
        .unwrap();
    seed_secret(&s, "ns", "sec", "new-uid").await;
    assert_eq!(status_with(&s, &token).await, 401);
}

#[tokio::test]
async fn node_bound_token_dies_with_the_node_but_pod_bound_node_claims_are_informational() {
    let s = auth_server();
    seed_sa(&s, "ns", "sa", "sa-uid").await;

    let node_bound = token_with(|k| {
        k.node = Some(KubeRef {
            name: "n1".into(),
            uid: "node-uid".into(),
        })
    });
    assert_eq!(status_with(&s, &node_bound).await, 401);
    seed_node(&s, "n1", "node-uid").await;
    assert_ne!(status_with(&s, &node_bound).await, 401);

    // pod-bound: the node claim is not looked up (claims.go:235-240)
    seed_pod(&s, "ns", "p", "pod-uid", "sa", "gone-node").await;
    let pod_bound = token_with(|k| {
        k.pod = Some(KubeRef {
            name: "p".into(),
            uid: "pod-uid".into(),
        });
        k.node = Some(KubeRef {
            name: "gone-node".into(),
            uid: "x".into(),
        });
    });
    assert_ne!(status_with(&s, &pod_bound).await, 401);
}

#[tokio::test]
async fn service_account_recreated_invalidates_tokens() {
    let s = auth_server();
    let token = token_with(|_| {});
    assert_eq!(status_with(&s, &token).await, 401);
    seed_sa(&s, "ns", "sa", "sa-uid").await;
    assert_ne!(status_with(&s, &token).await, 401);
    s.storage
        .delete(&build_key("serviceaccounts", Some("ns"), "sa"))
        .await
        .unwrap();
    seed_sa(&s, "ns", "sa", "different-uid").await;
    assert_eq!(status_with(&s, &token).await, 401);
}

// ---- TokenRequest ---------------------------------------------------------

fn payload(token: &str) -> Value {
    let part = token.split('.').nth(1).unwrap();
    serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(part)
            .unwrap(),
    )
    .unwrap()
}

async fn token_request(s: &TestApiServer, bound: Value) -> (u16, Value) {
    let (st, v) = s
        .post(
            "/api/v1/namespaces/ns/serviceaccounts/sa/token",
            &json!({"apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest",
                    "spec": {"audiences": ["https://kubernetes.default.svc"],
                             "boundObjectRef": bound}}),
        )
        .await;
    (st.as_u16(), v)
}

async fn plain_server() -> TestApiServer {
    let s = TestApiServer::new();
    seed_sa(&s, "ns", "sa", "sa-uid").await;
    s
}

#[tokio::test]
async fn token_request_records_pod_and_node_binding() {
    let s = plain_server().await;
    seed_node(&s, "n1", "node-uid").await;
    seed_pod(&s, "ns", "p", "pod-uid", "sa", "n1").await;
    let (st, body) = token_request(
        &s,
        json!({"apiVersion": "v1", "kind": "Pod", "name": "p", "uid": "pod-uid"}),
    )
    .await;
    assert_eq!(st, 200, "{body}");
    let p = payload(body["status"]["token"].as_str().unwrap());
    assert_eq!(p["kubernetes.io"]["pod"]["name"], "p");
    assert_eq!(p["kubernetes.io"]["pod"]["uid"], "pod-uid");
    assert_eq!(p["kubernetes.io"]["node"]["name"], "n1");
    assert_eq!(p["kubernetes.io"]["node"]["uid"], "node-uid");
}

#[tokio::test]
async fn token_request_records_secret_and_node_binding() {
    let s = plain_server().await;
    seed_secret(&s, "ns", "sec", "sec-uid").await;
    seed_node(&s, "n1", "node-uid").await;
    let (st, body) = token_request(
        &s,
        json!({"apiVersion": "v1", "kind": "Secret", "name": "sec"}),
    )
    .await;
    assert_eq!(st, 200, "{body}");
    let p = payload(body["status"]["token"].as_str().unwrap());
    assert_eq!(p["kubernetes.io"]["secret"]["uid"], "sec-uid");

    let (st, body) = token_request(
        &s,
        json!({"apiVersion": "v1", "kind": "Node", "name": "n1"}),
    )
    .await;
    assert_eq!(st, 200, "{body}");
    let p = payload(body["status"]["token"].as_str().unwrap());
    assert_eq!(p["kubernetes.io"]["node"]["uid"], "node-uid");
    assert!(p["kubernetes.io"].get("pod").is_none());
}

#[tokio::test]
async fn token_request_rejects_bad_bound_objects() {
    let s = plain_server().await;
    seed_pod(&s, "ns", "p", "pod-uid", "sa", "n1").await;
    seed_pod(&s, "ns", "other-sa-pod", "u2", "other", "n1").await;

    // missing referent (token.go:176-178)
    let (st, _) = token_request(
        &s,
        json!({"apiVersion": "v1", "kind": "Pod", "name": "missing"}),
    )
    .await;
    assert_eq!(st, 404);

    // UID mismatch is a 409 (token.go:219-221)
    let (st, body) = token_request(
        &s,
        json!({"apiVersion": "v1", "kind": "Pod", "name": "p", "uid": "stale"}),
    )
    .await;
    assert_eq!(st, 409, "{body}");

    // pod running as a different SA is a 400 (token.go:180-182)
    let (st, _) = token_request(
        &s,
        json!({"apiVersion": "v1", "kind": "Pod", "name": "other-sa-pod"}),
    )
    .await;
    assert_eq!(st, 400);

    // unsupported kind is a 400 (token.go:216-217)
    let (st, _) = token_request(
        &s,
        json!({"apiVersion": "v1", "kind": "ConfigMap", "name": "x"}),
    )
    .await;
    assert_eq!(st, 400);
}
