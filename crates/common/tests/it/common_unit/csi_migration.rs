//! Unit tests moved from `src/csi_migration.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::csi_translation::{
        CsiTranslator, AWS_EBS_IN_TREE_PLUGIN_NAME, GCE_PD_IN_TREE_PLUGIN_NAME,
    };
    pub use rusternetes_common::resources::pod::Volume;
    pub use rusternetes_common::resources::PersistentVolume;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::csi_migration::*;

    fn inline(v: serde_json::Value) -> Volume {
        serde_json::from_value(v).unwrap()
    }

    fn gce_inline() -> VolumeSpec {
        VolumeSpec {
            volume: Some(inline(serde_json::json!({
                "name": "v", "gcePersistentDisk": {"pdName": "pd", "fsType": "ext4"}}))),
            read_only: true,
            ..Default::default()
        }
    }

    /// `TestMigrationFeatureFlagStatus` (`plugin_manager_test.go:97`), the
    /// Portworx gate rows.
    #[test]
    fn migration_flag_status_matches_upstream_table() {
        for (enabled, unregister, want_enabled, want_complete) in [
            (false, false, false, false),
            (false, true, false, false),
            (true, false, true, false),
            (true, true, true, true),
        ] {
            let pm = PluginManager {
                csi_migration_portworx: enabled,
                in_tree_plugin_portworx_unregister: unregister,
                ..Default::default()
            };
            assert_eq!(pm.is_migration_enabled_for_plugin(PORTWORX), want_enabled);
            assert_eq!(pm.is_migration_complete_for_plugin(PORTWORX), want_complete);
        }
        let pm = PluginManager::new();
        assert!(pm.is_migration_complete_for_plugin(GCE_PD_IN_TREE_PLUGIN_NAME));
        assert!(pm.is_migration_enabled_for_plugin(AWS_EBS_IN_TREE_PLUGIN_NAME));
        assert!(!pm.is_migration_enabled_for_plugin("kubernetes.io/rbd"));
    }

    #[test]
    fn is_migratable_covers_inline_and_pv_specs() {
        let pm = PluginManager::new();
        assert!(pm.is_migratable(Some(&gce_inline())).unwrap());
        assert!(pm.is_migratable(None).is_err());
        let none = VolumeSpec {
            volume: Some(inline(serde_json::json!({"name": "v", "emptyDir": {}}))),
            ..Default::default()
        };
        assert!(!pm.is_migratable(Some(&none)).unwrap());
    }

    #[test]
    fn spec_translation_marks_inline_and_migrated() {
        let out = translate_in_tree_spec_to_csi(&gce_inline(), "ns", &CsiTranslator::new())
            .expect("translates");
        assert!(out.migrated && out.inline_volume_spec_for_csi_migration && out.read_only);
        assert!(out.volume.is_none());
        let csi = out.persistent_volume.unwrap().spec.csi.unwrap();
        assert_eq!(csi.driver, "pd.csi.storage.gke.io");
    }

    #[test]
    fn spec_translation_rejects_empty_and_untranslatable_specs() {
        let t = CsiTranslator::new();
        let e = translate_in_tree_spec_to_csi(&VolumeSpec::default(), "ns", &t).unwrap_err();
        assert_eq!(
            e,
            "failed to translate in-tree pv to CSI: not a valid volume spec"
        );
        let bad = VolumeSpec {
            volume: Some(inline(serde_json::json!({"name": "v", "emptyDir": {}}))),
            ..Default::default()
        };
        assert!(translate_in_tree_spec_to_csi(&bad, "ns", &t)
            .unwrap_err()
            .starts_with("failed to translate in-tree pv to CSI: could not find"));
    }
}
