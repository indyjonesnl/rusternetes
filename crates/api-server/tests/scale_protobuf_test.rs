//! Regression tests for the `/scale` subresource served as Kubernetes protobuf.
//!
//! Two conformance tests read the scale subresource over protobuf and fail:
//!   [sig-apps] ReplicaSet ... working scale subresource [Conformance]
//!       -> "Failed to get scale subresource: proto: illegal wireType 6"
//!   [sig-apps] StatefulSet ... Scale subresource ...
//!       -> "Failed to get scale subresource: proto: Scale: wiretype end group"
//!
//! The Go client requests the scale subresource with
//! `Accept: application/vnd.kubernetes.protobuf`. The api-server must answer with
//! a valid `runtime.Unknown` envelope:
//!     "k8s\0" + Unknown{ typeMeta=TypeMeta{apiVersion,kind}, raw, contentType }
//! where field numbers match the Go `runtime.Unknown` definition:
//!     field 1 = typeMeta (nested message), field 2 = raw (bytes),
//!     field 3 = contentEncoding (string), field 4 = contentType (string).
//!
//! These tests drive the full Axum router (auth skipped) via tower's `oneshot`
//! and assert the response bytes decode as a valid envelope wrapping a valid
//! `autoscaling/v1 Scale`.

use axum::{
    body::Body,
    http::{header, Request},
};
use rusternetes_api_server::{router::build_router, state::ApiServerState};
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const PROTOBUF_MAGIC: &[u8] = b"k8s\0";

fn make_state(mem: Arc<MemoryStorage>) -> Arc<ApiServerState> {
    let backend = Arc::new(StorageBackend::Memory(mem));
    let token_manager = Arc::new(TokenManager::new(b"test-secret"));
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

/// Read a protobuf varint. Returns (value, bytes_consumed).
fn read_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        value |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

/// A single decoded protobuf field. For length-delimited fields (wire type 2)
/// `bytes` holds the inner payload; other wire types carry no payload here since
/// the envelope we assert on is entirely length-delimited.
struct ProtoField {
    field: u32,
    wire_type: u8,
    bytes: Vec<u8>,
}

/// Parse a flat protobuf message into its top-level fields, asserting every wire
/// type is one the Go decoder understands (0,1,2,5). This is what the Go proto
/// runtime does; an out-of-range wire type is exactly the `illegal wireType`
/// error seen in conformance.
fn parse_proto_fields(mut buf: &[u8]) -> Vec<ProtoField> {
    let mut out = Vec::new();
    while !buf.is_empty() {
        let (tag, n) = read_varint(buf).expect("valid field tag varint");
        buf = &buf[n..];
        let field = (tag >> 3) as u32;
        let wire_type = (tag & 0x7) as u8;
        assert!(
            matches!(wire_type, 0 | 1 | 2 | 5),
            "illegal wireType {wire_type} for field {field} — Go decoder rejects this",
        );
        match wire_type {
            0 => {
                let (_v, n) = read_varint(buf).expect("varint value");
                buf = &buf[n..];
                out.push(ProtoField {
                    field,
                    wire_type,
                    bytes: Vec::new(),
                });
            }
            2 => {
                let (len, n) = read_varint(buf).expect("length prefix");
                buf = &buf[n..];
                let len = len as usize;
                assert!(buf.len() >= len, "length-delimited field overruns buffer");
                out.push(ProtoField {
                    field,
                    wire_type,
                    bytes: buf[..len].to_vec(),
                });
                buf = &buf[len..];
            }
            1 => {
                assert!(buf.len() >= 8);
                buf = &buf[8..];
                out.push(ProtoField {
                    field,
                    wire_type,
                    bytes: Vec::new(),
                });
            }
            5 => {
                assert!(buf.len() >= 4);
                buf = &buf[4..];
                out.push(ProtoField {
                    field,
                    wire_type,
                    bytes: Vec::new(),
                });
            }
            _ => unreachable!(),
        }
    }
    out
}

/// Decode a `k8s\0` runtime.Unknown envelope and return
/// (apiVersion, kind, raw_bytes, content_type).
fn decode_unknown_envelope(data: &[u8]) -> (String, String, Vec<u8>, String) {
    assert!(
        data.len() > 4 && &data[..4] == PROTOBUF_MAGIC,
        "response must start with k8s\\0 protobuf magic, got {:?}",
        &data[..data.len().min(8)]
    );
    let fields = parse_proto_fields(&data[4..]);

    let mut api_version = String::new();
    let mut kind = String::new();
    let mut raw = Vec::new();
    let mut content_type = String::new();

    for f in &fields {
        match f.field {
            // field 1: typeMeta (nested message) — Go runtime.Unknown
            1 => {
                assert_eq!(f.wire_type, 2, "typeMeta must be length-delimited (msg)");
                let tm = parse_proto_fields(&f.bytes);
                for tf in &tm {
                    match tf.field {
                        1 => api_version = String::from_utf8(tf.bytes.clone()).unwrap(),
                        2 => kind = String::from_utf8(tf.bytes.clone()).unwrap(),
                        _ => {}
                    }
                }
            }
            // field 2: raw bytes
            2 => {
                assert_eq!(f.wire_type, 2, "raw must be length-delimited (bytes)");
                raw = f.bytes.clone();
            }
            // field 4: contentType
            4 => {
                assert_eq!(f.wire_type, 2, "contentType must be length-delimited");
                content_type = String::from_utf8(f.bytes.clone()).unwrap();
            }
            _ => {}
        }
    }
    (api_version, kind, raw, content_type)
}

/// Pre-create a ReplicaSet and GET its /scale subresource over protobuf.
#[tokio::test]
async fn test_replicaset_scale_subresource_protobuf_envelope() {
    let mem = Arc::new(MemoryStorage::new());

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "test-rs",
            "namespace": "default",
            "resourceVersion": "12",
        },
        "spec": {
            "replicas": 2,
            "selector": { "matchLabels": { "app": "test-rs" } }
        },
        "status": { "replicas": 2 }
    });
    let key = build_key("replicasets", Some("default"), "test-rs");
    mem.create(&key, &rs).await.unwrap();

    let state = make_state(mem);
    let router = build_router(state, None);

    let req = Request::builder()
        .method("GET")
        .uri("/apis/apps/v1/namespaces/default/replicasets/test-rs/scale")
        .header(header::ACCEPT, "application/vnd.kubernetes.protobuf")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    let status = response.status().as_u16();
    let ct = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    assert_eq!(status, 200, "scale GET should be 200, body={:?}", body);
    assert!(
        ct.starts_with("application/vnd.kubernetes.protobuf"),
        "Content-Type must be protobuf when Accept asks for it, got {ct}"
    );

    // This is the assertion that fails today: the envelope / wire format is
    // malformed, producing the conformance `illegal wireType` error.
    let (api_version, kind, raw, content_type) = decode_unknown_envelope(&body);
    assert_eq!(api_version, "autoscaling/v1");
    assert_eq!(kind, "Scale");
    assert_eq!(content_type, "application/json");

    let scale: Value = serde_json::from_slice(&raw).expect("raw must be valid Scale JSON");
    assert_eq!(scale["apiVersion"], "autoscaling/v1");
    assert_eq!(scale["kind"], "Scale");
    assert_eq!(scale["spec"]["replicas"], 2);
    assert_eq!(scale["status"]["replicas"], 2);
}

/// Pre-create a StatefulSet and GET its /scale subresource over protobuf.
#[tokio::test]
async fn test_statefulset_scale_subresource_protobuf_envelope() {
    let mem = Arc::new(MemoryStorage::new());

    let ss = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "test-ss",
            "namespace": "default",
            "resourceVersion": "7",
        },
        "spec": {
            "replicas": 3,
            "selector": { "matchLabels": { "app": "test-ss" } }
        },
        "status": { "replicas": 3 }
    });
    let key = build_key("statefulsets", Some("default"), "test-ss");
    mem.create(&key, &ss).await.unwrap();

    let state = make_state(mem);
    let router = build_router(state, None);

    let req = Request::builder()
        .method("GET")
        .uri("/apis/apps/v1/namespaces/default/statefulsets/test-ss/scale")
        .header(header::ACCEPT, "application/vnd.kubernetes.protobuf")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    let status = response.status().as_u16();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    assert_eq!(status, 200, "scale GET should be 200, body={:?}", body);

    let (api_version, kind, raw, _content_type) = decode_unknown_envelope(&body);
    assert_eq!(api_version, "autoscaling/v1");
    assert_eq!(kind, "Scale");

    let scale: Value = serde_json::from_slice(&raw).expect("raw must be valid Scale JSON");
    assert_eq!(scale["spec"]["replicas"], 3);
    assert_eq!(scale["status"]["replicas"], 3);
}
