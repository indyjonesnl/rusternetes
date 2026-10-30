//! Unit tests moved from `src/resources/coordination.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::coordination::*;

    #[test]
    fn test_lease_creation() {
        let lease = Lease::new("test-lease", "default");
        assert_eq!(lease.metadata.name, "test-lease");
        assert_eq!(lease.type_meta.kind, "Lease");
        assert_eq!(lease.type_meta.api_version, "coordination.k8s.io/v1");
    }

    #[test]
    fn test_lease_spec_serialization() {
        let spec = LeaseSpec {
            holder_identity: Some("node-1".to_string()),
            lease_duration_seconds: Some(15),
            acquire_time: None,
            renew_time: None,
            lease_transitions: Some(3),
            preferred_holder: Some("node-2".to_string()),
            strategy: Some("OldestEmulationVersion".to_string()),
        };

        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"preferredHolder\":\"node-2\""));
        assert!(json.contains("\"strategy\":\"OldestEmulationVersion\""));
        assert!(json.contains("\"leaseTransitions\":3"));

        let deserialized: LeaseSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.preferred_holder, Some("node-2".to_string()));
        assert_eq!(
            deserialized.strategy,
            Some("OldestEmulationVersion".to_string())
        );
    }

    #[test]
    fn test_lease_micro_time_format() {
        let spec = LeaseSpec {
            holder_identity: Some("node-1".to_string()),
            lease_duration_seconds: Some(15),
            acquire_time: Some(Utc::now()),
            renew_time: Some(Utc::now()),
            lease_transitions: Some(0),
            preferred_holder: None,
            strategy: None,
        };

        let json = serde_json::to_string(&spec).unwrap();
        // Verify MicroTime format includes microseconds (6 decimal places)
        assert!(
            json.contains("."),
            "MicroTime should include fractional seconds: {}",
            json
        );

        // Verify round-trip
        let deserialized: LeaseSpec = serde_json::from_str(&json).unwrap();
        assert!(deserialized.acquire_time.is_some());
        assert!(deserialized.renew_time.is_some());
    }

    #[test]
    fn test_lease_micro_time_deserialize_various_formats() {
        // Without microseconds
        let json = r#"{"holderIdentity":"n","leaseDurationSeconds":15,"acquireTime":"2024-01-01T00:00:00Z"}"#;
        let spec: LeaseSpec = serde_json::from_str(json).unwrap();
        assert!(spec.acquire_time.is_some());

        // With microseconds
        let json = r#"{"holderIdentity":"n","leaseDurationSeconds":15,"acquireTime":"2024-01-01T00:00:00.000000Z"}"#;
        let spec: LeaseSpec = serde_json::from_str(json).unwrap();
        assert!(spec.acquire_time.is_some());
    }

    #[test]
    fn test_lease_spec_deserialization_from_kubernetes() {
        let json = r#"{
            "holderIdentity": "apiserver-abc123",
            "leaseDurationSeconds": 15,
            "leaseTransitions": 5,
            "preferredHolder": "apiserver-def456",
            "strategy": "OldestEmulationVersion"
        }"#;

        let spec: LeaseSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.holder_identity, Some("apiserver-abc123".to_string()));
        assert_eq!(spec.lease_duration_seconds, Some(15));
        assert_eq!(spec.preferred_holder, Some("apiserver-def456".to_string()));
    }
}
