//! Unit tests moved from `src/protobuf.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prost::Message;
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::protobuf::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestObject {
        pub name: String,
        pub value: i32,
    }

    #[test]
    fn test_encode_decode_protobuf() {
        let obj = TestObject {
            name: "test".to_string(),
            value: 42,
        };

        // Encode
        let encoded = encode_protobuf(&obj, "v1", "TestObject").expect("Failed to encode");

        // Verify magic number
        assert_eq!(&encoded[0..4], PROTOBUF_MAGIC);

        // Decode
        let (decoded, type_meta): (TestObject, TypeMeta) =
            decode_protobuf(&encoded).expect("Failed to decode");

        assert_eq!(decoded, obj);
        assert_eq!(type_meta.api_version, "v1");
        assert_eq!(type_meta.kind, "TestObject");
    }

    #[test]
    fn test_is_protobuf() {
        let obj = TestObject {
            name: "test".to_string(),
            value: 42,
        };

        let encoded = encode_protobuf(&obj, "v1", "TestObject").expect("Failed to encode");

        assert!(is_protobuf(&encoded));
        assert!(!is_protobuf(b"not protobuf"));
        assert!(!is_protobuf(&[0x6b, 0x38])); // Too short
    }

    #[test]
    fn test_extract_type_meta() {
        let obj = TestObject {
            name: "test".to_string(),
            value: 42,
        };

        let encoded = encode_protobuf(&obj, "apps/v1", "Deployment").expect("Failed to encode");

        let type_meta = extract_type_meta(&encoded).expect("Failed to extract type meta");

        assert_eq!(type_meta.api_version, "apps/v1");
        assert_eq!(type_meta.kind, "Deployment");
    }

    #[test]
    fn test_decode_invalid_magic() {
        let bad_data = vec![0x00, 0x00, 0x00, 0x00];
        let result: Result<(TestObject, TypeMeta), String> = decode_protobuf(&bad_data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid magic number"));
    }

    #[test]
    fn test_decode_too_short() {
        let bad_data = vec![0x6b, 0x38];
        let result: Result<(TestObject, TypeMeta), String> = decode_protobuf(&bad_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_message() {
        // Test direct Unknown encoding/decoding
        let unknown = Unknown {
            type_meta: Some(UnknownTypeMeta {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
            }),
            raw: b"{\"test\": \"data\"}".to_vec(),
            content_encoding: String::new(),
            content_type: "application/json".to_string(),
        };

        let mut buf = Vec::new();
        unknown.encode(&mut buf).expect("Failed to encode");

        let decoded = Unknown::decode(&buf[..]).expect("Failed to decode");
        assert_eq!(decoded, unknown);
    }

    /// Wire-format compatibility: Unknown.typeMeta must be at field 1 as a
    /// nested message (apiVersion=1, kind=2), Unknown.raw at field 2. This
    /// is what every upstream client decoder expects — getting it wrong
    /// produces `proto: illegal wireType 6` when the client tries to parse
    /// the apiVersion string bytes as a nested TypeMeta. See module docs
    /// on `Unknown`.
    #[test]
    fn test_unknown_wire_format_matches_upstream() {
        let mut buf = Vec::new();
        let unknown = Unknown {
            type_meta: Some(UnknownTypeMeta {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
            }),
            raw: b"{}".to_vec(),
            content_encoding: String::new(),
            content_type: "application/json".to_string(),
        };
        unknown.encode(&mut buf).expect("encode");

        // First emitted bytes MUST be the field-1 tag for an embedded
        // message: tag = (1 << 3) | 2 = 0x0A. (Old broken layout emitted
        // 0x0A 0x02 'v' '1' as a *string* — same tag byte, but the inner
        // payload is then parsed by clients as a TypeMeta proto message
        // whose first byte 'v'=0x76 carries the illegal wireType 6.)
        assert_eq!(
            buf[0], 0x0A,
            "Unknown.typeMeta must be embedded message at field 1; got tag byte 0x{:02x}",
            buf[0],
        );

        // The embedded TypeMeta should contain field 1 (apiVersion="v1")
        // then field 2 (kind="Pod"). Decode it back via the nested message
        // type to be sure the wire shape round-trips.
        let inner_len = buf[1] as usize;
        let inner = &buf[2..2 + inner_len];
        let typed = UnknownTypeMeta::decode(inner).expect("typeMeta decode");
        assert_eq!(typed.api_version, "v1");
        assert_eq!(typed.kind, "Pod");
    }
}
