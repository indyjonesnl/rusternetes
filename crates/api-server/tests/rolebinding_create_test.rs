//! Conformance regression: POSTing a `rbac.authorization.k8s.io/v1`
//! RoleBinding (as the upstream AdmissionWebhook e2e helper does) must
//! succeed and, on any error, return a proper `metav1.Status` body rather
//! than the plain-text axum extractor rejection that surfaces in client-go
//! as "the server rejected our request due to an error in our request".
//!
//! Upstream helper (release-1.35):
//! `test/e2e/apimachinery/webhook.go` `createRoleBinding` →
//! `rbacv1.RoleBinding{ RoleRef: {APIGroup, Kind: "ClusterRole"|"Role"},
//!  Subjects: [{Kind: "ServiceAccount", Name, Namespace}] }`.
//! client-go marshals empty TypeMeta and a `creationTimestamp: null`.

use axum::{
    body::Body,
    http::{Method, Request},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::{
    auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
};
use rusternetes_storage::{memory::MemoryStorage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const NS: &str = "webhook-8788";

fn make_state(mem: Arc<MemoryStorage>) -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(TokenManager::new(b"rolebinding-secret"));
    let authorizer = Arc::new(AlwaysAllowAuthorizer);
    let metrics = Arc::new(MetricsRegistry::new());
    Arc::new(ApiServerState::new(
        backend,
        token_manager,
        authorizer,
        metrics,
        true, // skip_auth
    ))
}

fn spawn_router() -> axum::Router {
    let mem = Arc::new(MemoryStorage::new());
    build_router(make_state(mem), None)
}

async fn send_json(router: axum::Router, method: Method, uri: &str, body: &Value) -> (u16, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

fn rolebindings_uri() -> String {
    format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{NS}/rolebindings")
}

// --- minimal protobuf encoder to mimic client-go's native-proto write path ---

fn varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

fn ld_field(field: u32, data: &[u8], out: &mut Vec<u8>) {
    varint(((field as u64) << 3) | 2, out); // wire type 2 (length-delimited)
    varint(data.len() as u64, out);
    out.extend_from_slice(data);
}

fn str_field(field: u32, s: &str, out: &mut Vec<u8>) {
    ld_field(field, s.as_bytes(), out);
}

/// Encode a RoleBinding as the K8s protobuf `Unknown` envelope, exactly as
/// client-go does for write requests by default. roleRef.apiGroup is field 1
/// inside the roleRef message — the field the brace-scan fallback dropped.
fn encode_rolebinding_protobuf(name: &str, namespace: &str) -> Vec<u8> {
    // ObjectMeta { name(5), namespace(3) } — field numbers per apimachinery proto.
    let mut meta = Vec::new();
    str_field(5, name, &mut meta); // name
    str_field(3, namespace, &mut meta); // namespace

    // Subject { kind(1), name(3), namespace(4) }
    let mut subject = Vec::new();
    str_field(1, "ServiceAccount", &mut subject);
    str_field(3, "default", &mut subject);
    str_field(4, namespace, &mut subject);

    // RoleRef { apiGroup(1), kind(2), name(3) }
    let mut role_ref = Vec::new();
    str_field(1, "rbac.authorization.k8s.io", &mut role_ref);
    str_field(2, "Role", &mut role_ref);
    str_field(3, name, &mut role_ref);

    // RoleBinding { metadata(1), subjects(2), roleRef(3) }
    let mut rb = Vec::new();
    ld_field(1, &meta, &mut rb);
    ld_field(2, &subject, &mut rb);
    ld_field(3, &role_ref, &mut rb);

    // Unknown { typeMeta(1){apiVersion(1),kind(2)}, raw(2) }
    let mut type_meta = Vec::new();
    str_field(1, "rbac.authorization.k8s.io/v1", &mut type_meta);
    str_field(2, "RoleBinding", &mut type_meta);

    let mut unknown = Vec::new();
    ld_field(1, &type_meta, &mut unknown);
    ld_field(2, &rb, &mut unknown);

    let mut out = b"k8s\0".to_vec();
    out.extend_from_slice(&unknown);
    out
}

async fn send_protobuf(router: axum::Router, uri: &str, body: Vec<u8>) -> (u16, Value) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/vnd.kubernetes.protobuf")
        .body(Body::from(body))
        .unwrap();
    let response = router.oneshot(req).await.unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, v)
}

/// The real conformance failure: client-go POSTs the RoleBinding as native
/// K8s protobuf. Before the fix, the registry had no RBAC schema, so the
/// body fell through to the brace-scan decoder which dropped
/// `roleRef.apiGroup` and produced "roleRef: missing field `apiGroup`".
#[tokio::test]
async fn create_webhook_rolebinding_via_protobuf() {
    let router = spawn_router();
    let body = encode_rolebinding_protobuf("webhook", NS);
    let (status, resp) = send_protobuf(router, &rolebindings_uri(), body).await;
    assert_eq!(
        status, 201,
        "protobuf RoleBinding create should return 201, got {status}: {resp}"
    );
}

/// Exactly the JSON shape client-go emits from the e2e webhook helper:
/// empty `subjects` apiGroup, `roleRef` (camelCase, the Go json tag),
/// `creationTimestamp: null` from the marshalled ObjectMeta.
fn webhook_rolebinding_body() -> Value {
    json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {
            "name": "webhook",
            "creationTimestamp": null
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "extension-apiserver-authentication-reader"
        },
        "subjects": [{
            "kind": "ServiceAccount",
            "name": "default",
            "namespace": NS
        }]
    })
}

#[tokio::test]
async fn create_webhook_rolebinding_succeeds() {
    let router = spawn_router();
    let body = webhook_rolebinding_body();
    let (status, resp) = send_json(router, Method::POST, &rolebindings_uri(), &body).await;
    assert_eq!(
        status, 201,
        "RoleBinding create should return 201 Created, got {status}: {resp}"
    );
    assert_eq!(resp["metadata"]["name"], "webhook");
    assert_eq!(resp["roleRef"]["kind"], "ClusterRole");
    assert_eq!(resp["subjects"][0]["kind"], "ServiceAccount");
}

/// A genuinely malformed body must still come back as a metav1.Status
/// (apiVersion/kind/status fields present), never a bare axum text reject.
#[tokio::test]
async fn create_rolebinding_bad_body_returns_status() {
    let router = spawn_router();
    // Missing the required `roleRef` entirely.
    let body = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "broken"},
        "subjects": [{"kind": "ServiceAccount", "name": "default", "namespace": NS}]
    });
    let (status, resp) = send_json(router, Method::POST, &rolebindings_uri(), &body).await;
    assert!(
        (400..500).contains(&status),
        "expected a 4xx for malformed body, got {status}: {resp}"
    );
    // The custom Json extractor must surface a metav1.Status so client-go can
    // parse it, instead of axum's bare plain-text rejection (which surfaces as
    // "the server rejected our request due to an error in our request").
    assert_eq!(
        resp["kind"], "Status",
        "error body must be a metav1.Status, got {resp}"
    );
    assert_eq!(resp["apiVersion"], "v1");
    assert_eq!(resp["status"], "Failure");
}
