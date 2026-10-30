//! Unit tests moved from `src/controllers/volume_data_source_validator.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::{EventSource, EventType, PersistentVolumeClaim};
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{
        build_key, build_prefix, extract_key, EventRecorder, Storage, WorkQueue,
    };
    pub use serde_json::Value;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::volume_data_source_validator::*;
    use serde_json::json;

    fn populator(group: &str, kind: &str) -> Value {
        json!({"sourceKind": {"group": group, "kind": kind}})
    }

    #[test]
    fn pvc_and_snapshot_are_special_cases() {
        assert_eq!(
            validate_group_kind("", "PersistentVolumeClaim", &[]),
            Validation::Pvc
        );
        assert_eq!(
            validate_group_kind("snapshot.storage.k8s.io", "VolumeSnapshot", &[]),
            Validation::VolumeSnapshot
        );
    }

    #[test]
    fn registered_populator_matches_group_and_kind() {
        let pops = [populator("hello.example.com", "Hello")];
        assert_eq!(
            validate_group_kind("hello.example.com", "Hello", &pops),
            Validation::Populator
        );
        assert!(!validate_group_kind("other.example.com", "Hello", &pops).is_valid());
        assert!(!validate_group_kind("hello.example.com", "Other", &pops).is_valid());
        // A core-group kind is not a PVC just because a populator exists.
        assert!(!validate_group_kind("", "Hello", &pops).is_valid());
    }
}
