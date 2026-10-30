//! Unit tests moved from `src/fieldmanager/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, SubsecRound, Utc};
    pub use rusternetes_common::fieldmanager::fieldset::{field_path, Fields, Set};
    pub use rusternetes_common::fieldmanager::versionconverter::{Converter, UpdateFailure};
    pub use rusternetes_common::types::ManagedFieldsEntry;
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::Value;
    pub use std::collections::BTreeMap;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::fieldmanager::*;

    /// `capManagersManager` merges per API version into one bucket, oldest first
    /// (capmanagers_test.go `TestCapUpdateManagers`).
    #[test]
    fn the_cap_merges_the_oldest_entries_into_a_versioned_bucket() {
        let mut managed = Managed::default();
        for i in 0..12 {
            let id = build_manager_identifier(&format!("m{i:02}"), OPERATION_UPDATE, "v1", "");
            let mut set = Set::new();
            set.insert(&field_path(&["data", &format!("k{i}")]));
            managed.fields.insert(
                id.clone(),
                VersionedSet {
                    set,
                    api_version: "v1".into(),
                    applied: false,
                },
            );
            managed
                .times
                .insert(id, Some(Utc::now() + chrono::Duration::seconds(i)));
        }
        cap_update_managers(&mut managed, DEFAULT_MAX_UPDATE_MANAGERS);
        assert_eq!(managed.fields.len(), 10);
        let bucket = build_manager_identifier("ancient-changes", OPERATION_UPDATE, "v1", "");
        let b = &managed.fields[&bucket];
        // m00, m01 and m02 were merged: the first seen became the bucket.
        assert!(b.set.has(&field_path(&["data", "k0"])));
        assert!(b.set.has(&field_path(&["data", "k1"])));
        assert!(b.set.has(&field_path(&["data", "k2"])));
    }
}
