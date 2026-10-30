//! Unit tests moved from `src/fieldmanager/apply.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{SubsecRound, Utc};
    pub use rusternetes_common::fieldmanager::fieldset::{
        field_kind, key_element, typed_key_element, FieldKind, Set,
    };
    pub use rusternetes_common::fieldmanager::schema::{ListKind, Type};
    pub use rusternetes_common::fieldmanager::versionconverter::UpdateFailure;
    pub use rusternetes_common::fieldmanager::{
        build_manager_identifier, remove_managed_fields, strip_set, FieldManager, Managed,
        VersionedSet, OPERATION_APPLY,
    };
    pub use serde_json::{Map, Value};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::fieldmanager::apply::*;
    use serde_json::json;

    /// Ported from the removed top-level-key engine's
    /// `test_metadata_merge_preserves_system_fields`: an applied configuration
    /// never overrides the live object's system-owned metadata, while its user
    /// fields (labels) survive.
    #[test]
    fn retain_live_system_metadata_keeps_live_system_fields_and_applied_labels() {
        let live = json!({"metadata": {
            "name": "p", "uid": "abc-123", "resourceVersion": "100",
            "creationTimestamp": "2024-01-01T00:00:00Z"}});
        let mut applied = json!({"metadata": {
            "name": "p", "uid": "forged", "generation": 9, "labels": {"app": "test"}}});
        retain_live_system_metadata(&mut applied, Some(&live));
        assert_eq!(applied["metadata"]["uid"], "abc-123");
        assert_eq!(applied["metadata"]["resourceVersion"], "100");
        assert_eq!(
            applied["metadata"]["creationTimestamp"],
            "2024-01-01T00:00:00Z"
        );
        assert!(applied["metadata"].get("generation").is_none());
        assert_eq!(applied["metadata"]["labels"]["app"], "test");
    }
}
