//! Unit tests moved from `src/deletion.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::Utc;
    pub use rusternetes_common::types::DeletionPropagation;
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::Value;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::deletion::*;
    use serde_json::json;

    #[test]
    fn test_deletion_with_finalizers() {
        let mut resource = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "uid": "abc-123",
                "finalizers": ["my-finalizer"]
            }
        });

        let options = DeleteOptions::default();
        let result = process_deletion(&mut resource, &options).unwrap();

        assert_eq!(result, DeletionResult::MarkedForDeletion);
        assert!(resource["metadata"]["deletionTimestamp"].is_string());
    }

    #[test]
    fn test_deletion_without_finalizers() {
        let mut resource = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "uid": "abc-123"
            }
        });

        let options = DeleteOptions::default();
        let result = process_deletion(&mut resource, &options).unwrap();

        assert_eq!(result, DeletionResult::Deleted);
        assert!(resource["metadata"]["deletionTimestamp"].is_string());
    }

    #[test]
    fn test_deletion_with_foreground_propagation() {
        let mut resource = json!({
            "apiVersion": "v1",
            "kind": "Deployment",
            "metadata": {
                "name": "test-deployment",
                "uid": "abc-123"
            }
        });

        let options = DeleteOptions {
            propagation_policy: Some(DeletionPropagation::Foreground),
            ..Default::default()
        };

        let result = process_deletion(&mut resource, &options).unwrap();

        assert_eq!(result, DeletionResult::MarkedForDeletion);
        assert!(resource["metadata"]["finalizers"]
            .as_array()
            .unwrap()
            .contains(&json!("foregroundDeletion")));
    }

    #[test]
    fn test_deletion_with_uid_precondition() {
        let mut resource = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "uid": "abc-123"
            }
        });

        let options = DeleteOptions {
            preconditions: Some(Preconditions {
                uid: Some("wrong-uid".to_string()),
                resource_version: None,
            }),
            ..Default::default()
        };

        let result = process_deletion(&mut resource, &options).unwrap();

        match result {
            DeletionResult::Deferred(msg) => {
                assert!(msg.contains("UID precondition failed"));
            }
            _ => panic!("Expected deferred deletion"),
        }
    }

    #[test]
    fn test_remove_finalizer() {
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "finalizers".to_string(),
            json!(["finalizer1", "finalizer2"]),
        );

        remove_finalizer(&mut metadata, "finalizer1").unwrap();

        let finalizers = metadata["finalizers"].as_array().unwrap();
        assert_eq!(finalizers.len(), 1);
        assert_eq!(finalizers[0], "finalizer2");
    }

    #[test]
    fn test_is_being_deleted() {
        let resource = json!({
            "metadata": {
                "deletionTimestamp": "2024-01-01T00:00:00Z"
            }
        });

        assert!(is_being_deleted(&resource));
    }

    #[test]
    fn test_can_be_deleted() {
        let resource = json!({
            "metadata": {
                "deletionTimestamp": "2024-01-01T00:00:00Z"
            }
        });

        assert!(can_be_deleted(&resource));

        let resource_with_finalizers = json!({
            "metadata": {
                "deletionTimestamp": "2024-01-01T00:00:00Z",
                "finalizers": ["my-finalizer"]
            }
        });

        assert!(!can_be_deleted(&resource_with_finalizers));
    }
}
