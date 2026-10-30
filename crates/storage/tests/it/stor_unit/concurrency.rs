//! Unit tests moved from `src/concurrency.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::Error;
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::concurrency::*;
    use serde_json::json;

    #[test]
    fn test_extract_resource_version() {
        let metadata = json!({
            "name": "test",
            "resourceVersion": "12345"
        });

        assert_eq!(
            extract_resource_version(&metadata),
            Some("12345".to_string())
        );

        let no_rv = json!({"name": "test"});
        assert_eq!(extract_resource_version(&no_rv), None);

        // Empty string must be treated as absent (same as K8s semantics).
        let empty_rv = json!({"name": "test", "resourceVersion": ""});
        assert_eq!(extract_resource_version(&empty_rv), None);
    }

    #[test]
    fn test_validate_resource_version() {
        // Matching versions should succeed
        assert!(validate_resource_version(Some("100"), Some("100")).is_ok());

        // No expected version should succeed
        assert!(validate_resource_version(None, Some("100")).is_ok());

        // Mismatched versions should fail
        assert!(validate_resource_version(Some("100"), Some("200")).is_err());

        // Expected version but actual is missing should fail
        assert!(validate_resource_version(Some("100"), None).is_err());
    }

    #[test]
    fn test_mod_revision_conversion() {
        let rv = mod_revision_to_resource_version(12345);
        assert_eq!(rv, "12345");

        let mod_rev = resource_version_to_mod_revision("12345").unwrap();
        assert_eq!(mod_rev, 12345);

        assert!(resource_version_to_mod_revision("invalid").is_err());
    }
}
