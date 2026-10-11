//! #2929 end to end: a native-protobuf SubjectAccessReview written the way
//! Go's generated marshaller writes it (every scalar present, even when zero)
//! must be accepted. Upstream `ValidateSubjectAccessReview`
//! (pkg/apis/authorization/validation/validation.go:63-72) compares the decoded
//! struct, where `Name`/`GenerateName`/`Namespace`/`ResourceVersion`/`Generation`
//! are value types, so absent and zero are the same thing.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;

const PROTO_CT: &str = "application/vnd.kubernetes.protobuf";

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        buf.push(b);
        if v == 0 {
            break;
        }
    }
}

fn ld(buf: &mut Vec<u8>, field: u32, payload: &[u8]) {
    put_varint(buf, ((field as u64) << 3) | 2);
    put_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

fn varint_field(buf: &mut Vec<u8>, field: u32, v: u64) {
    put_varint(buf, (field as u64) << 3);
    put_varint(buf, v);
}

fn envelope(kind: &str, raw: &[u8]) -> Vec<u8> {
    let mut tm = Vec::new();
    ld(&mut tm, 1, b"authorization.k8s.io/v1");
    ld(&mut tm, 2, kind.as_bytes());
    let mut unknown = Vec::new();
    ld(&mut unknown, 1, &tm);
    ld(&mut unknown, 2, raw);
    ld(&mut unknown, 4, PROTO_CT.as_bytes());
    let mut out = b"k8s\0".to_vec();
    out.extend_from_slice(&unknown);
    out
}

/// ObjectMeta as generated.pb.go writes an empty one: every string scalar,
/// `generation`, and the (empty) creationTimestamp message are present.
fn go_zero_object_meta() -> Vec<u8> {
    let mut m = Vec::new();
    for field in [1, 2, 3, 4, 5, 6] {
        ld(&mut m, field, b""); // name, generateName, namespace, selfLink, uid, resourceVersion
    }
    varint_field(&mut m, 7, 0); // generation
    ld(&mut m, 8, b""); // creationTimestamp
    m
}

fn sar_bytes(spec: &[u8]) -> Vec<u8> {
    let mut status = Vec::new();
    varint_field(&mut status, 1, 0); // allowed
    ld(&mut status, 2, b""); // reason
    ld(&mut status, 3, b""); // evaluationError
    varint_field(&mut status, 4, 0); // denied
    let mut sar = Vec::new();
    ld(&mut sar, 1, &go_zero_object_meta());
    ld(&mut sar, 2, spec);
    ld(&mut sar, 3, &status);
    sar
}

async fn post_proto(path: &str, body: Vec<u8>) -> (StatusCode, String) {
    let s = TestApiServer::new();
    let (status, _h, bytes, _) = s
        .send_with_headers(
            "POST",
            path,
            &[("content-type", PROTO_CT), ("accept", "application/json")],
            Some(body),
        )
        .await;
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn proto_subject_access_review_with_go_zero_scalars_is_created() {
    // SubjectAccessReviewSpec{resourceAttributes{namespace,verb,resource}, user}
    let mut ra = Vec::new();
    ld(&mut ra, 1, b"default");
    ld(&mut ra, 2, b"get");
    ld(&mut ra, 5, b"pods");
    let mut spec = Vec::new();
    ld(&mut spec, 1, &ra);
    ld(&mut spec, 3, b"alice");
    let (st, body) = post_proto(
        "/apis/authorization.k8s.io/v1/subjectaccessreviews",
        envelope("SubjectAccessReview", &sar_bytes(&spec)),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
}

#[tokio::test]
async fn proto_self_subject_access_review_with_go_zero_scalars_is_created() {
    let mut ra = Vec::new();
    ld(&mut ra, 1, b"default");
    ld(&mut ra, 2, b"get");
    ld(&mut ra, 5, b"pods");
    let mut spec = Vec::new();
    ld(&mut spec, 1, &ra);
    let (st, body) = post_proto(
        "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
        envelope("SelfSubjectAccessReview", &sar_bytes(&spec)),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
}
