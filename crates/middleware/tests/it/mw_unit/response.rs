//! Unit tests moved from `src/response.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Body,
        http::{header, HeaderMap, StatusCode},
        response::{IntoResponse, Response},
    };
    pub use serde::Serialize;
}

mod tests {
    use super::__parent::*;
    use rusternetes_middleware::response::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct TestData {
        name: String,
        value: i32,
    }

    /// #1667: the auth TokenRequest response must encode via the group-qualified
    /// schema (`authentication.k8s.io/v1.TokenRequest`), which carries
    /// `status.token` + `status.expirationTimestamp`. Encoding under the bare
    /// kind `TokenRequest` hit the CSI schema (no status) and dropped it, so the
    /// controller-manager read a nil expiration.
    #[test]
    fn auth_tokenrequest_encodes_via_qualified_schema() {
        let tr = serde_json::json!({
            "apiVersion":"authentication.k8s.io/v1","kind":"TokenRequest",
            "metadata":{"name":"node-controller"},
            "spec":{"audiences":["api"],"expirationSeconds":3600},
            "status":{"token":"abc.def.ghi","expirationTimestamp":"2026-07-24T02:00:00Z"}
        });
        let json = serde_json::to_vec(&tr).unwrap();
        let got = encode_native_or_wrapped(&json, "authentication.k8s.io/v1", "TokenRequest");

        let expected_raw = rusternetes_protobuf::PROTO_REGISTRY
            .encode_message("authentication.k8s.io/v1.TokenRequest", &tr)
            .expect("auth TokenRequest schema must encode");
        let expected = wrap_native_proto_in_envelope(
            &expected_raw,
            "authentication.k8s.io/v1",
            "TokenRequest",
        );
        assert_eq!(
            got, expected,
            "auth TokenRequest must encode via the group-qualified schema, not the bare-kind CSI one"
        );

        // The qualified schema actually preserves the status the CM reads.
        let rt = rusternetes_protobuf::PROTO_REGISTRY
            .decode_message("authentication.k8s.io/v1.TokenRequest", &expected_raw)
            .expect("decode");
        assert!(rt.pointer("/status/expirationTimestamp").is_some());
        assert!(rt.pointer("/status/token").is_some());
    }

    /// Parse a `k8s\0`-framed Unknown envelope and return its `contentType`
    /// (field 4). Native proto envelopes advertise the protobuf content type;
    /// the JSON-in-raw fallback advertises `application/json`.
    fn envelope_content_type(out: &[u8]) -> String {
        assert_eq!(&out[..4], b"k8s\0", "missing magic prefix");
        let mut i = 4usize;
        let rv = |d: &[u8], i: &mut usize| -> u64 {
            let (mut s, mut r) = (0u32, 0u64);
            loop {
                let b = d[*i];
                *i += 1;
                r |= ((b & 0x7f) as u64) << s;
                if b & 0x80 == 0 {
                    break;
                }
                s += 7;
            }
            r
        };
        let mut ct = String::new();
        while i < out.len() {
            let tag = rv(out, &mut i);
            let (fnum, wt) = (tag >> 3, tag & 7);
            if wt != 2 {
                break;
            }
            let len = rv(out, &mut i) as usize;
            let val = &out[i..i + len];
            i += len;
            if fnum == 4 {
                ct = String::from_utf8_lossy(val).to_string();
            }
        }
        ct
    }

    #[test]
    fn scale_encodes_as_native_protobuf_not_json_fallback() {
        // A realistic autoscaling/v1 Scale body as the handler serializes it.
        // Second-precision creationTimestamp (as ObjectMeta now serializes it):
        // it must round-trip through the proto Timestamp so the guard accepts it
        // WITHOUT special-casing metadata.
        let scale = br#"{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"rs","namespace":"default","uid":"abc","resourceVersion":"42","creationTimestamp":"2026-05-31T08:00:43Z"},"spec":{"replicas":3},"status":{"replicas":3,"selector":"app=rs"}}"#;
        let out = encode_native_or_wrapped(scale, "autoscaling/v1", "Scale");
        let ct = envelope_content_type(&out);
        assert_eq!(
            ct, "application/vnd.kubernetes.protobuf",
            "Scale must encode as NATIVE protobuf (the scale client rejects the JSON fallback); got contentType={ct}"
        );
        // The raw must NOT contain the JSON key "replicas" — native proto carries
        // field numbers, not JSON keys.
        assert!(
            !String::from_utf8_lossy(&out).contains("\"replicas\""),
            "native raw should not contain JSON keys"
        );
    }

    #[test]
    fn lossy_schema_falls_back_to_json() {
        // A kind with no registered schema must fall back to the JSON envelope
        // rather than emit empty/garbage native bytes.
        let body =
            br#"{"apiVersion":"example.com/v1","kind":"DefinitelyNotRegistered","spec":{"x":1}}"#;
        let out = encode_native_or_wrapped(body, "example.com/v1", "DefinitelyNotRegistered");
        assert_eq!(envelope_content_type(&out), "application/json");
    }

    #[test]
    fn test_content_type_negotiation() {
        let mut headers = HeaderMap::new();
        assert_eq!(negotiate_content_type(&headers), ContentType::Json);

        headers.insert(header::ACCEPT, "application/json".parse().unwrap());
        assert_eq!(negotiate_content_type(&headers), ContentType::Json);

        headers.insert(
            header::ACCEPT,
            "application/vnd.kubernetes.protobuf".parse().unwrap(),
        );
        assert_eq!(negotiate_content_type(&headers), ContentType::Protobuf);
    }

    #[test]
    fn test_content_type_mime_types() {
        assert_eq!(ContentType::Json.mime_type(), "application/json");
        assert_eq!(
            ContentType::Protobuf.mime_type(),
            "application/vnd.kubernetes.protobuf"
        );
    }

    #[test]
    fn test_api_response_creation() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        let response = ApiResponse::new(data.clone());
        assert_eq!(response.status, StatusCode::OK);

        let response = ApiResponse::with_status(data, StatusCode::CREATED);
        assert_eq!(response.status, StatusCode::CREATED);
    }

    /// The default [`ProtoEncoder`] must emit a `k8s\0`-framed envelope and
    /// the body must round-trip back through
    /// [`rusternetes_common::protobuf::decode_protobuf`] to the same value.
    #[test]
    fn test_default_proto_encoder_roundtrips_via_decode_protobuf() {
        use rusternetes_common::protobuf::{decode_protobuf, is_protobuf};

        let data = TestData {
            name: "rt".into(),
            value: 7,
        };
        let json = serde_json::to_vec(&data).unwrap();
        let envelope = default_proto_encoder().encode(&json, "v1", "TestData");

        assert!(envelope.starts_with(b"k8s\0"), "magic prefix missing");
        assert!(is_protobuf(&envelope));

        let (decoded, tm): (TestData, _) = decode_protobuf(&envelope).expect("decode");
        assert_eq!(decoded.name, "rt");
        assert_eq!(decoded.value, 7);
        assert_eq!(tm.api_version, "v1");
        assert_eq!(tm.kind, "TestData");
    }

    /// `NativeProtoOptIn::pod()` must label responses as `v1` / `Pod`.
    #[test]
    fn test_native_proto_opt_in_pod_constants() {
        let opt = NativeProtoOptIn::pod();
        assert_eq!(opt.api_version, "v1");
        assert_eq!(opt.kind, "Pod");

        let list_opt = NativeProtoOptIn::pod_list();
        assert_eq!(list_opt.api_version, "v1");
        assert_eq!(list_opt.kind, "PodList");
    }

    /// Empty `api_version` + empty `kind` must still produce a valid
    /// envelope. The body should round-trip via `decode_protobuf` (which
    /// only requires the `raw` field — empty TypeMeta is fine).
    #[test]
    fn test_wrap_envelope_without_type_meta() {
        use rusternetes_common::protobuf::{decode_protobuf, is_protobuf};

        let data = TestData {
            name: "x".into(),
            value: 1,
        };
        let json = serde_json::to_vec(&data).unwrap();
        let env = wrap_json_in_protobuf_envelope(&json, "", "");
        assert!(env.starts_with(b"k8s\0"));
        assert!(is_protobuf(&env));
        let (decoded, tm): (TestData, _) = decode_protobuf(&env).expect("decode");
        assert_eq!(decoded.name, "x");
        assert!(tm.api_version.is_empty());
        assert!(tm.kind.is_empty());
    }
}

mod latency_tests {
    use super::__parent::*;
    use rusternetes_common::latency_trackers::{with_latency_trackers, LatencyTrackers};
    use rusternetes_middleware::response::*;
    use std::sync::Arc;

    /// `TrackSerializeResponseObjectLatency` (responsewriters/writers.go:354):
    /// the serializer call feeds the request's serialization tracker.
    #[tokio::test]
    async fn serializing_a_response_feeds_the_serialization_tracker() {
        struct Slow;
        impl Serialize for Slow {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                std::thread::sleep(std::time::Duration::from_millis(15));
                s.serialize_str("x")
            }
        }
        let t = Arc::new(LatencyTrackers::new());
        with_latency_trackers(t.clone(), async {
            let _ = ApiResponse::new(Slow).into_response();
            let _ = create_response(Slow, StatusCode::OK, ContentType::Json);
        })
        .await;
        assert!(t.serialization.get_latency() >= std::time::Duration::from_millis(30));
        assert_eq!(t.transform.get_latency(), std::time::Duration::ZERO);
    }
}
