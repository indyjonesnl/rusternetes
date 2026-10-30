//! Unit tests moved from `src/metadata.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use serde_json::Value;
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::metadata::*;

    #[test]
    fn stamps_all_fields_when_missing() {
        let mut v = serde_json::json!({ "metadata": { "name": "foo" } });
        ensure_create_metadata(&mut v);
        let md = &v["metadata"];
        assert!(md["uid"].as_str().is_some_and(|s| !s.is_empty()));
        assert!(md["creationTimestamp"].as_str().is_some());
        assert_eq!(md["generation"], serde_json::json!(1));
    }

    #[test]
    fn creates_metadata_object_when_absent() {
        let mut v = serde_json::json!({ "kind": "Namespace" });
        ensure_create_metadata(&mut v);
        assert!(v["metadata"]["creationTimestamp"].as_str().is_some());
    }

    #[test]
    fn replaces_explicit_null_creation_timestamp() {
        // The scheduler / bootstrap path leaves creationTimestamp as null.
        let mut v = serde_json::json!({
            "metadata": { "name": "evt", "creationTimestamp": null }
        });
        ensure_create_metadata(&mut v);
        assert!(v["metadata"]["creationTimestamp"].as_str().is_some());
    }

    #[test]
    fn creation_timestamp_is_seconds_precision_like_go() {
        // Go's metav1.Time marshals RFC3339 truncated to seconds, e.g.
        // "2026-05-30T10:20:45Z" — no fractional digits.
        let mut v = serde_json::json!({ "metadata": {} });
        ensure_create_metadata(&mut v);
        let ts = v["metadata"]["creationTimestamp"].as_str().unwrap();
        assert!(ts.ends_with('Z'), "expected Zulu suffix: {ts}");
        assert!(!ts.contains('.'), "expected no sub-second digits: {ts}");
    }

    #[test]
    fn preserves_caller_supplied_values() {
        let mut v = serde_json::json!({
            "metadata": {
                "uid": "11111111-1111-1111-1111-111111111111",
                "creationTimestamp": "2020-01-02T03:04:05Z",
                "generation": 7
            }
        });
        ensure_create_metadata(&mut v);
        let md = &v["metadata"];
        assert_eq!(md["uid"], "11111111-1111-1111-1111-111111111111");
        assert_eq!(md["creationTimestamp"], "2020-01-02T03:04:05Z");
        assert_eq!(md["generation"], serde_json::json!(7));
    }

    #[test]
    fn regenerates_empty_uid() {
        let mut v = serde_json::json!({ "metadata": { "uid": "" } });
        ensure_create_metadata(&mut v);
        assert!(v["metadata"]["uid"].as_str().is_some_and(|s| !s.is_empty()));
    }
}
