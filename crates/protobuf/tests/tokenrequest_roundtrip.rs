//! #1667: the auth TokenRequest response must round-trip `status.token` and
//! `status.expirationTimestamp` through protobuf. The controller-manager reads
//! the TokenRequest response over protobuf; a dropped expiration breaks its
//! client with "nil pointer of expiration in token request".
//!
//! The bug was a schema-key collision: `TokenRequest` exists both as the CSI
//! `{audience, expirationSeconds}` pair (no status) and as
//! `authentication.k8s.io/v1.TokenRequest` (with status). Encoding under the
//! bare kind selected the CSI schema and dropped `status`. The response encoder
//! now resolves the group-qualified key first (see
//! `rusternetes_middleware::response::encode_native_or_wrapped`); this test pins
//! that the auth schema itself round-trips the status losslessly.
use rusternetes_protobuf::ProtoRegistry;
use serde_json::json;

#[test]
fn auth_tokenrequest_roundtrips_status() {
    let r = ProtoRegistry::new();
    let tr = json!({
        "apiVersion":"authentication.k8s.io/v1","kind":"TokenRequest",
        "metadata":{"name":"node-controller"},
        "spec":{"audiences":["api"],"expirationSeconds":3600},
        "status":{"token":"abc.def.ghi","expirationTimestamp":"2026-07-24T02:00:00Z"}
    });
    let bytes = r
        .encode_message("authentication.k8s.io/v1.TokenRequest", &tr)
        .expect("auth TokenRequest must encode to protobuf");
    let decoded = r
        .decode_message("authentication.k8s.io/v1.TokenRequest", &bytes)
        .expect("auth TokenRequest must decode from protobuf");
    assert_eq!(
        decoded.pointer("/status/expirationTimestamp"),
        Some(&json!("2026-07-24T02:00:00Z")),
        "expirationTimestamp must survive the protobuf round-trip"
    );
    assert_eq!(
        decoded.pointer("/status/token"),
        Some(&json!("abc.def.ghi")),
        "token must survive the protobuf round-trip"
    );
}
