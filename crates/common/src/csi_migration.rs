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
pub const PORTWORX: &str = "kubernetes.io/portworx-volume";

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
    pub translator: CsiTranslator,
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
