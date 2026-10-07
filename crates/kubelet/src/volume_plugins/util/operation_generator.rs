//! Port of the node-expansion eligibility check in
//! `pkg/volume/util/operationexecutor/operation_generator.go`.
//!
//! Only `checkIfSupportsNodeExpansion` is ported. The rest of the expansion
//! flow (`GenerateExpandInUseVolumeFunc`, `nodeExpandVolume`, the
//! recovery-from-expansion `nodeExpander`) needs the operation executor
//! (#1970) and PVC status patching, and is tracked separately.

use crate::volume_plugins::plugin::{NodeExpandableVolumePlugin, Spec};
use crate::volume_plugins::registry::VolumePluginMgr;

/// Port of `operationGenerator.checkIfSupportsNodeExpansion`
/// (`operation_generator.go:1996-2010`):
///
/// ```go
/// // Get expander, if possible
/// expandableVolumePlugin, _ :=
///     og.volumePluginMgr.FindNodeExpandablePluginBySpec(volumeToMount.VolumeSpec)
/// if expandableVolumePlugin != nil &&
///     expandableVolumePlugin.RequiresFSResize() &&
///     volumeToMount.VolumeSpec.PersistentVolume != nil {
///     return true, expandableVolumePlugin
/// }
/// return false, nil
/// ```
///
/// The lookup error is discarded, as upstream does with `_`. The leading
/// `InlineVolumeSpecForCSIMigration` guard (`:1997-2001`) is not ported: there
/// is no in-tree-to-CSI migration here, so it is always false.
pub fn check_if_supports_node_expansion<'a>(
    mgr: &'a VolumePluginMgr,
    spec: &Spec<'_>,
) -> Option<&'a dyn NodeExpandableVolumePlugin> {
    let plugin = mgr
        .find_node_expandable_plugin_by_spec(spec)
        .ok()
        .flatten()?;
    (plugin.requires_fs_resize() && spec.persistent_volume.is_some()).then_some(plugin)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_plugins::csi::CsiPlugin;
    use crate::volume_plugins::KubeletVolumeHost;
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn mgr() -> VolumePluginMgr {
        let host = Arc::new(KubeletVolumeHost::new(
            "/var/lib/rusternetes".to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        ));
        VolumePluginMgr::new(vec![Box::new(CsiPlugin::new(host))])
    }

    fn pv() -> PersistentVolume {
        serde_json::from_value(json!({
            "metadata": {"name": "pv1"},
            "spec": {"csi": {"driver": "d", "volumeHandle": "h"}}
        }))
        .unwrap()
    }

    /// A CSI PersistentVolume is expandable on the node.
    #[test]
    fn csi_persistent_volume_supports_node_expansion() {
        let m = mgr();
        let v: Volume = serde_json::from_value(
            json!({"name": "v", "persistentVolumeClaim": {"claimName": "c"}}),
        )
        .unwrap();
        let pv = pv();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_some());
    }

    /// An inline CSI volume has no PersistentVolume, so it is not resized
    /// (`VolumeSpec.PersistentVolume != nil`).
    #[test]
    fn inline_csi_volume_does_not_support_node_expansion() {
        let m = mgr();
        let v: Volume =
            serde_json::from_value(json!({"name": "v", "csi": {"driver": "d"}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_none());
    }

    /// No plugin matches: the lookup error is swallowed, not propagated.
    #[test]
    fn unmatched_spec_does_not_support_node_expansion() {
        let m = mgr();
        let v: Volume = serde_json::from_value(json!({"name": "v", "emptyDir": {}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_none());
    }
}
