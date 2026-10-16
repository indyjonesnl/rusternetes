//! Port of `pkg/volume/csimigration/plugin_manager.go`: `PluginManager`
//! (`IsMigrationEnabledForPlugin`, `IsMigrationCompleteForPlugin`,
//! `IsMigratable`) and `TranslateInTreeSpecToCSI`, the consumer of
//! `TranslateInTreeInlineVolumeToCSI` / `TranslateInTreePVToCSI`
//! (`plugin_manager.go:123-155`).
//!
//! Plugin names this crate cannot translate yet (Cinder, AzureDisk, AzureFile,
//! vSphere, Portworx) are answered exactly as upstream answers them; they only
//! become reachable through `IsMigratable` once their csi-translation plugins
//! land (`CsiTranslator::get_in_tree_plugin_name_from_spec` fails for them).

use crate::csi_translation::{
    CsiTranslator, AWS_EBS_IN_TREE_PLUGIN_NAME, GCE_PD_IN_TREE_PLUGIN_NAME,
};
use crate::resources::pod::Volume;
use crate::resources::PersistentVolume;

const AZURE_FILE: &str = "kubernetes.io/azure-file";
const AZURE_DISK: &str = "kubernetes.io/azure-disk";
const CINDER: &str = "kubernetes.io/cinder";
const VSPHERE: &str = "kubernetes.io/vsphere-volume";
const PORTWORX: &str = "kubernetes.io/portworx-volume";

/// The fields of `volume.Spec` (`pkg/volume/plugins.go:434`) that the
/// migration code reads: one of `volume` / `persistent_volume`.
#[derive(Clone, Debug, Default)]
pub struct VolumeSpec {
    pub volume: Option<Volume>,
    pub persistent_volume: Option<PersistentVolume>,
    pub read_only: bool,
    /// `Spec.Migrated`.
    pub migrated: bool,
    /// `Spec.InlineVolumeSpecForCSIMigration`.
    pub inline_volume_spec_for_csi_migration: bool,
}

/// `PluginManager` (`plugin_manager.go:41`). The two Portworx fields are the
/// `CSIMigrationPortworx` / `InTreePluginPortworxUnregister` feature gates;
/// every other plugin is GA-locked in release-1.35.
#[derive(Clone, Debug, Default)]
pub struct PluginManager {
    pub csi_migration_portworx: bool,
    pub in_tree_plugin_portworx_unregister: bool,
    translator: CsiTranslator,
}

impl PluginManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// `IsMigrationCompleteForPlugin` (`plugin_manager.go:64-93`).
    pub fn is_migration_complete_for_plugin(&self, plugin_name: &str) -> bool {
        if !self.is_migration_enabled_for_plugin(plugin_name) {
            return false;
        }
        match plugin_name {
            AWS_EBS_IN_TREE_PLUGIN_NAME
            | GCE_PD_IN_TREE_PLUGIN_NAME
            | AZURE_FILE
            | AZURE_DISK
            | CINDER
            | VSPHERE => true,
            PORTWORX => self.in_tree_plugin_portworx_unregister,
            _ => false,
        }
    }

    /// `IsMigrationEnabledForPlugin` (`plugin_manager.go:97-118`).
    pub fn is_migration_enabled_for_plugin(&self, plugin_name: &str) -> bool {
        match plugin_name {
            AWS_EBS_IN_TREE_PLUGIN_NAME
            | GCE_PD_IN_TREE_PLUGIN_NAME
            | AZURE_FILE
            | AZURE_DISK
            | CINDER
            | VSPHERE => true,
            PORTWORX => self.csi_migration_portworx,
            _ => false,
        }
    }

    /// `IsMigratable` (`plugin_manager.go:120-133`): the plugin-name lookup
    /// error is discarded, an empty name means "not migratable".
    pub fn is_migratable(&self, spec: Option<&VolumeSpec>) -> Result<bool, String> {
        let Some(spec) = spec else {
            return Err(
                "could not find if plugin is migratable because volume spec is nil".to_string(),
            );
        };
        let plugin_name = self
            .translator
            .get_in_tree_plugin_name_from_spec(
                spec.persistent_volume.as_ref(),
                spec.volume.as_ref(),
            )
            .unwrap_or_default();
        if plugin_name.is_empty() {
            return Ok(false);
        }
        Ok(self.is_migration_enabled_for_plugin(&plugin_name))
    }
}

/// `TranslateInTreeSpecToCSI` (`plugin_manager.go:144-168`).
pub fn translate_in_tree_spec_to_csi(
    spec: &VolumeSpec,
    pod_namespace: &str,
    translator: &CsiTranslator,
) -> Result<VolumeSpec, String> {
    let (result, inline_volume) = if let Some(pv) = &spec.persistent_volume {
        (translator.translate_in_tree_pv_to_csi(pv), false)
    } else if let Some(vol) = &spec.volume {
        (
            translator.translate_in_tree_inline_volume_to_csi(vol, pod_namespace),
            true,
        )
    } else {
        (Err("not a valid volume spec".to_string()), false)
    };
    let csi_pv = result.map_err(|e| format!("failed to translate in-tree pv to CSI: {e}"))?;
    Ok(VolumeSpec {
        volume: None,
        migrated: true,
        persistent_volume: Some(csi_pv),
        read_only: spec.read_only,
        inline_volume_spec_for_csi_migration: inline_volume,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
