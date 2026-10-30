//! Unit tests moved from `src/cbor.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use ciborium::value::Value as CborValue;
    pub use serde_json::Value as JsonValue;
}

mod tests {
    use super::__parent::*;
    use rusternetes_middleware::cbor::*;
    use serde_json::json;

    /// Round-trip a representative ConfigMap shape through CBOR and JSON
    /// to prove the codec preserves the wire-visible structure. Mirrors
    /// the upstream cbor serializer's `TestRoundtrip` in
    /// `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/cbor/cbor_test.go`.
    #[test]
    fn test_roundtrip_configmap() {
        let original = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cbor-cm",
                "namespace": "default",
            },
            "data": {
                "key": "value",
                "another": "entry",
            }
        });

        let cbor = encode_json_to_cbor(&original).expect("encode");
        let back = decode_cbor_to_json(&cbor).expect("decode");
        assert_eq!(back, original);
    }

    /// `null` is a valid CBOR major-type 7 value. The codec must
    /// preserve it during a pure round-trip so the JSON `null` literal
    /// is not destroyed when a CRD chooses to use it for "field cleared"
    /// semantics.
    #[test]
    fn test_roundtrip_null() {
        let value = JsonValue::Null;
        let cbor = encode_json_to_cbor(&value).expect("encode");
        let back = decode_cbor_to_json(&cbor).expect("decode");
        assert_eq!(back, value);
    }

    /// Integer ranges that fit in i64 must round-trip without going
    /// through floats. K8s uses int64 for several spec fields
    /// (e.g. `metadata.generation`, `spec.replicas`).
    #[test]
    fn test_roundtrip_large_integer() {
        let value = json!({"big": i64::MAX, "small": i64::MIN});
        let cbor = encode_json_to_cbor(&value).expect("encode");
        let back = decode_cbor_to_json(&cbor).expect("decode");
        assert_eq!(back, value);
    }

    /// Arrays of objects (Container[], Toleration[]) are a hot path for
    /// K8s resources — make sure they round-trip element-by-element.
    #[test]
    fn test_roundtrip_array_of_objects() {
        let value = json!({
            "containers": [
                {"name": "c1", "image": "busybox"},
                {"name": "c2", "image": "nginx", "ports": [{"containerPort": 80}]},
            ]
        });
        let cbor = encode_json_to_cbor(&value).expect("encode");
        let back = decode_cbor_to_json(&cbor).expect("decode");
        assert_eq!(back, value);
    }

    /// Malformed bytes produce a `Decode` error — not a panic, not a
    /// silent empty object.
    #[test]
    fn test_decode_malformed_errors() {
        // 0xff is a "break" stop code outside any indefinite-length item.
        let err = decode_cbor_to_json(&[0xff]).expect_err("must reject malformed CBOR");
        match err {
            CborError::Decode(_) => {}
            other => panic!("expected Decode error, got {:?}", other),
        }
    }

    /// Maps with non-string keys cannot survive a JSON round-trip and
    /// must be reported as `Transcode` errors so the caller can return
    /// 400 with a useful message.
    #[test]
    fn test_decode_non_string_key_errors() {
        // Map with one entry: key = unsigned 1, value = unsigned 2.
        // CBOR: 0xa1 (map of 1) 0x01 (uint 1) 0x02 (uint 2).
        let bytes = [0xa1, 0x01, 0x02];
        let err = decode_cbor_to_json(&bytes).expect_err("must reject maps with non-string keys");
        match err {
            CborError::Transcode(_) => {}
            other => panic!("expected Transcode error, got {:?}", other),
        }
    }

    #[test]
    fn test_is_cbor_content_type() {
        assert!(is_cbor_content_type("application/cbor"));
        assert!(is_cbor_content_type("application/CBOR"));
        assert!(is_cbor_content_type("application/cbor; charset=utf-8"));
        assert!(is_cbor_content_type("application/apply-patch+cbor"));
        assert!(!is_cbor_content_type("application/json"));
        assert!(!is_cbor_content_type("application/vnd.kubernetes.protobuf"));
        assert!(!is_cbor_content_type(""));
    }

    #[test]
    fn test_accept_wants_cbor() {
        assert!(accept_wants_cbor("application/cbor"));
        assert!(accept_wants_cbor("application/json, application/cbor"));
        assert!(accept_wants_cbor(
            "application/json;q=0.9, application/cbor;q=1.0"
        ));
        assert!(accept_wants_cbor("application/apply-patch+cbor"));
        assert!(!accept_wants_cbor("application/json"));
        assert!(!accept_wants_cbor("*/*"));
        assert!(!accept_wants_cbor(""));
    }

    /// CBOR byte strings come back as base64-encoded JSON strings. This
    /// mirrors `[]byte` round-trips through Go's `encoding/json`.
    #[test]
    fn test_byte_string_becomes_base64() {
        // CBOR: 0x44 (byte string length 4) 0x01 0x02 0x03 0x04
        let bytes = [0x44, 0x01, 0x02, 0x03, 0x04];
        let value = decode_cbor_to_json(&bytes).expect("decode");
        assert_eq!(value, JsonValue::String("AQIDBA==".to_string()));
    }
}
