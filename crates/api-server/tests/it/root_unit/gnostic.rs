//! Unit tests moved from `src/gnostic.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prost::Message;
    pub use rusternetes_api_server::gnostic::openapi_v2::*;
}

mod v3_tests {
    use super::__parent::*;
    use rusternetes_api_server::gnostic::*;

    #[test]
    fn roundtrip_minimal_v3_document() {
        // Hand-craft a tiny v3 spec, encode it, decode it back, and assert the
        // top-level Document fields survive the wire format.
        let json = serde_json::json!({
            "openapi": "3.0.0",
            "info": {
                "title": "Rusternetes Test",
                "version": "v1.35.0",
                "description": "test doc"
            },
            "paths": {
                "/api/v1/pods": {
                    "summary": "pods collection",
                    "description": "list / create pods"
                }
            }
        });
        let json_bytes = serde_json::to_vec(&json).unwrap();
        let proto = openapi_v3_json_to_protobuf(&json_bytes).expect("encode v3");
        let decoded = openapi_v3::Document::decode(proto.as_slice()).expect("decode v3");

        assert_eq!(decoded.openapi, "3.0.0");
        let info = decoded.info.expect("info present");
        assert_eq!(info.title, "Rusternetes Test");
        assert_eq!(info.version, "v1.35.0");
        assert_eq!(info.description, "test doc");

        let paths = decoded.paths.expect("paths present");
        assert_eq!(paths.path.len(), 1, "expected one path entry");
        assert_eq!(paths.path[0].name, "/api/v1/pods");
        let item = paths.path[0].value.as_ref().expect("path item present");
        assert_eq!(item.summary, "pods collection");
        assert_eq!(item.description, "list / create pods");
    }

    #[test]
    fn v3_path_keys_must_start_with_slash() {
        // Discovery sub-documents include a `components` sibling that isn't a
        // path — make sure non-`/` keys are skipped, matching the v2 helper.
        let json = serde_json::json!({
            "openapi": "3.0.0",
            "info": { "title": "T", "version": "v1" },
            "paths": {
                "/ok": { "summary": "kept" },
                "components": { "summary": "dropped" }
            }
        });
        let json_bytes = serde_json::to_vec(&json).unwrap();
        let proto = openapi_v3_json_to_protobuf(&json_bytes).unwrap();
        let decoded = openapi_v3::Document::decode(proto.as_slice()).unwrap();
        let paths = decoded.paths.unwrap();
        assert_eq!(paths.path.len(), 1);
        assert_eq!(paths.path[0].name, "/ok");
    }

    #[test]
    fn rejects_non_object_input() {
        assert!(openapi_v3_json_to_protobuf(b"\"not an object\"").is_err());
    }
}
