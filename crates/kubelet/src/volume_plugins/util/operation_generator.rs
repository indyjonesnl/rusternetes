//! Port of the node-expansion half of
//! `pkg/volume/util/operationexecutor/operation_generator.go`:
//! `checkIfSupportsNodeExpansion`, `nodeExpandVolume`,
//! `expandVolumeDuringMount`, `checkForRecoveryFromExpansion` and
//! `legacyCallNodeExpandOnPlugin`, driving [`super::node_expander`].
//!
//! Not ported: `GenerateExpandInUseVolumeFunc` / `doOnlineExpansion` — the
//! closure wrapper around `nodeExpandVolume` — which needs the operation
//! executor's `GeneratedOperations` and `volume.DeviceMounter`
//! (`DeviceStagePath`), neither of which exists yet (#1970). Nothing in the
//! running kubelet calls these functions yet.

use crate::events::{FILE_SYSTEM_RESIZE_FAILED, FILE_SYSTEM_RESIZE_SUCCESS};
use crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
use crate::volume_plugins::plugin::{
    DeviceMounterArgs, NodeExpandableVolumePlugin, NodeResizeOptions, Spec,
};
use crate::volume_plugins::registry::VolumePluginMgr;
use crate::volume_plugins::util::fs_group_from;
use crate::volume_plugins::util::node_expander::{
    pvc_object_reference, record_event, NodeExpander, NodeResizeOperationOpts,
};
use crate::volume_plugins::util::operation_executor::VolumeToMount;
use crate::volume_plugins::util::resize_util::{self, RESOURCE_STORAGE};
use crate::volume_plugins::util::types::{
    is_failed_precondition_error, is_operation_not_supported_error,
};
use anyhow::{anyhow, Error};
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::volume::{PersistentVolume, PersistentVolumeClaim};
use rusternetes_common::resources::EventType;
use rusternetes_common::resources::Pod;
use rusternetes_storage::{build_key, EventRecorder, Storage};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

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

fn quantity_of(map: Option<&HashMap<String, String>>) -> Quantity {
    map.and_then(|m| m.get(RESOURCE_STORAGE))
        .and_then(|v| Quantity::parse(v.trim()).ok())
        .unwrap_or_else(|| Quantity::from_value(0, Format::DecimalSI))
}

/// `volumeToMount.VolumeSpec.ReadOnly` (`operation_generator.go:2032`).
fn spec_read_only(vmt: &VolumeToMount) -> bool {
    vmt.volume_spec.read_only
}

/// The part of `operationGenerator` that node expansion touches: the plugin
/// manager (`og.volumePluginMgr`), the API client (`og.kubeClient`) and the
/// event recorder (`og.recorder`).
pub struct OperationGenerator<'a, S: Storage + ?Sized> {
    pub volume_plugin_mgr: &'a VolumePluginMgr,
    pub kube_client: &'a S,
    pub recorder: &'a EventRecorder<S>,
}

impl<'a, S: Storage + ?Sized> OperationGenerator<'a, S> {
    /// Port of `checkForRecoveryFromExpansion`
    /// (`operation_generator.go:2065-2088`).
    ///
    /// `RecoverVolumeExpansionFailure` is GA and locked on in 1.35
    /// (`pkg/features/kube_features.go:1683`), so the `!featureGateStatus`
    /// arm (`:2070-2077`) is unreachable and omitted.
    pub fn check_for_recovery_from_expansion(
        &self,
        pvc: &PersistentVolumeClaim,
        volume_to_mount: &VolumeToMount,
    ) -> bool {
        let status = pvc.status.as_ref();
        let resize_status = status
            .and_then(|s| s.allocated_resource_statuses.as_ref())
            .and_then(|m| m.get(RESOURCE_STORAGE))
            .map(String::as_str)
            .unwrap_or_default();
        let allocated_resource = status.and_then(|s| s.allocated_resources.as_ref());

        // Even though RecoverVolumeExpansionFailure feature gate is enabled,
        // it appears that we are running with older version of resize
        // controller, which will not populate allocatedResource and
        // resizeStatus. This can happen because of version skew and hence we
        // are going to keep expanding using older logic.
        if resize_status.is_empty() && allocated_resource.is_none() {
            warn!(
                "{}",
                volume_to_mount.generate_msg_detailed(
                    "MountVolume.NodeExpandVolume running with",
                    "older external resize controller"
                )
            );
            return false;
        }
        true
    }

    /// `og.kubeClient.CoreV1().PersistentVolumeClaims(ns).Get(name)` for the
    /// PV's `claimRef` (`operation_generator.go:1952-1953`, `:2024-2025`).
    async fn get_claim(&self, pv: &PersistentVolume) -> anyhow::Result<PersistentVolumeClaim> {
        let claim_ref = pv
            .spec
            .claim_ref
            .as_ref()
            .ok_or_else(|| anyhow!("PV {:?} has no claimRef", pv.metadata.name))?;
        let key = build_key(
            "persistentvolumeclaims",
            claim_ref.namespace.as_deref(),
            claim_ref.name.as_deref().unwrap_or_default(),
        );
        Ok(self.kube_client.get(&key).await?)
    }

    /// The `FileSystemResizeFailed` read-only warning shared by both callers
    /// (`operation_generator.go:1962-1967`, `:2032-2037`).
    async fn warn_read_only(&self, vmt: &VolumeToMount, pvc: &PersistentVolumeClaim) {
        let (simple_msg, detailed_msg) = vmt.generate_msg(
            "MountVolume.NodeExpandVolume failed",
            "requested read-only file system",
        );
        warn!("{detailed_msg}");
        let node_name = vmt.pod.spec.as_ref().and_then(|s| s.node_name.as_deref());
        record_event(
            self.recorder,
            &crate::events::pod_object_reference(&vmt.pod),
            node_name,
            EventType::Warning,
            FILE_SYSTEM_RESIZE_FAILED,
            &simple_msg,
        )
        .await;
        record_event(
            self.recorder,
            &pvc_object_reference(pvc),
            node_name,
            EventType::Warning,
            FILE_SYSTEM_RESIZE_FAILED,
            &simple_msg,
        )
        .await;
    }

    /// Port of `expandVolumeDuringMount` (`operation_generator.go:1949-1994`).
    pub async fn expand_volume_during_mount<'v>(
        &self,
        volume_to_mount: &'v VolumeToMount,
        actual_state_of_world: &'v ActualStateOfWorld,
        mut rs_opts: NodeResizeOptions<'v>,
    ) -> (bool, Option<Error>)
    where
        'a: 'v,
    {
        let spec = volume_to_mount.volume_spec.as_spec();
        let Some(expandable_plugin) =
            check_if_supports_node_expansion(self.volume_plugin_mgr, &spec)
        else {
            return (true, None);
        };
        // `supportsExpansion` implies a PersistentVolume
        // (`checkIfSupportsNodeExpansion`, `:2008`).
        let pv = volume_to_mount
            .volume_spec
            .persistent_volume
            .as_ref()
            .expect("checked by check_if_supports_node_expansion");
        let pvc = match self.get_claim(pv).await {
            Ok(pvc) => pvc,
            // Return error rather than leave the file system un-resized,
            // caller will log and retry
            Err(e) => {
                return (
                    false,
                    Some(anyhow!("mountVolume.NodeExpandVolume get PVC failed : {e}")),
                )
            }
        };

        let pvc_status_cap = quantity_of(pvc.status.as_ref().and_then(|s| s.capacity.as_ref()));
        let pv_spec_cap = quantity_of(Some(&pv.spec.capacity));
        if pvc_status_cap.cmp_value(&pv_spec_cap) != std::cmp::Ordering::Less {
            return (true, None);
        }

        if spec_read_only(volume_to_mount) {
            self.warn_read_only(volume_to_mount, &pvc).await;
            return (true, None);
        }

        rs_opts.new_size = pv_spec_cap;
        rs_opts.old_size = pvc_status_cap;
        let recovery = self.check_for_recovery_from_expansion(&pvc, volume_to_mount);
        if recovery {
            // if recovery feature is enabled, we can use allocated size from
            // PVC status as new size
            rs_opts.new_size = quantity_of(
                pvc.status
                    .as_ref()
                    .and_then(|s| s.allocated_resources.as_ref()),
            );
        }
        let resize_op = NodeResizeOperationOpts {
            vmt: volume_to_mount,
            pvc,
            pv,
            plugin_resize_opts: rs_opts,
            volume_plugin: expandable_plugin,
            actual_state_of_world,
        };
        if recovery {
            let mut node_expander = NodeExpander::new(resize_op, self.kube_client, self.recorder);
            let out = node_expander.expand_on_plugin().await;
            (out.resize_finished, out.err)
        } else {
            self.legacy_call_node_expand_on_plugin(resize_op).await
        }
    }

    /// Port of `nodeExpandVolume` (`operation_generator.go:2014-2063`): expand
    /// an in-use volume from `rs_opts.old_size` to `rs_opts.new_size`. Returns
    /// `(resizeFinished, newSize, err)`.
    pub async fn node_expand_volume<'v>(
        &self,
        volume_to_mount: &'v VolumeToMount,
        actual_state_of_world: &'v ActualStateOfWorld,
        mut rs_opts: NodeResizeOptions<'v>,
    ) -> (bool, Quantity, Option<Error>)
    where
        'a: 'v,
    {
        let spec = volume_to_mount.volume_spec.as_spec();
        let Some(expandable_plugin) =
            check_if_supports_node_expansion(self.volume_plugin_mgr, &spec)
        else {
            return (true, rs_opts.old_size, None);
        };

        // lets use sizes handed over to us by caller for comparison
        if rs_opts.new_size.cmp_value(&rs_opts.old_size) != std::cmp::Ordering::Greater {
            return (true, rs_opts.old_size, None);
        }

        let pv = volume_to_mount
            .volume_spec
            .persistent_volume
            .as_ref()
            .expect("checked by check_if_supports_node_expansion");
        let pvc = match self.get_claim(pv).await {
            Ok(pvc) => pvc,
            // Return error rather than leave the file system un-resized,
            // caller will log and retry. Upstream reads `pvc.Status.Capacity`
            // before checking `err` (`:2026`); the zero size is what a missing
            // PVC would give.
            Err(e) => {
                return (
                    false,
                    quantity_of(None),
                    Some(anyhow!("mountVolume.NodeExpandVolume get PVC failed : {e}")),
                )
            }
        };
        let current_size = quantity_of(pvc.status.as_ref().and_then(|s| s.capacity.as_ref()));

        if spec_read_only(volume_to_mount) {
            self.warn_read_only(volume_to_mount, &pvc).await;
            return (true, current_size, None);
        }

        let recovery = self.check_for_recovery_from_expansion(&pvc, volume_to_mount);
        if recovery {
            // if recovery feature is enabled, we can use allocated size from
            // PVC status as new size
            rs_opts.new_size = quantity_of(
                pvc.status
                    .as_ref()
                    .and_then(|s| s.allocated_resources.as_ref()),
            );
        }
        let new_size = rs_opts.new_size;
        let resize_op = NodeResizeOperationOpts {
            vmt: volume_to_mount,
            pvc,
            pv,
            plugin_resize_opts: rs_opts,
            volume_plugin: expandable_plugin,
            actual_state_of_world,
        };
        if recovery {
            let mut node_expander = NodeExpander::new(resize_op, self.kube_client, self.recorder);
            let out = node_expander.expand_on_plugin().await;
            (out.resize_finished, out.size, out.err)
        } else {
            let (finished, err) = self.legacy_call_node_expand_on_plugin(resize_op).await;
            (finished, new_size, err)
        }
    }

    /// Port of `legacyCallNodeExpandOnPlugin`
    /// (`operation_generator.go:2090-2135`), the version of calling node
    /// expansion on a plugin that does not support recovery from volume
    /// expansion failure.
    async fn legacy_call_node_expand_on_plugin(
        &self,
        resize_op: NodeResizeOperationOpts<'_>,
    ) -> (bool, Option<Error>) {
        let pvc = &resize_op.pvc;
        let volume_to_mount = resize_op.vmt;
        let pvc_status_cap = quantity_of(pvc.status.as_ref().and_then(|s| s.capacity.as_ref()));
        let node_name = volume_to_mount
            .pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .unwrap_or_default();

        // File system resize was requested, proceed
        debug!(
            pod = %volume_to_mount.pod.metadata.name,
            "{}",
            volume_to_mount.generate_msg_detailed(
                "MountVolume.NodeExpandVolume entering",
                &format!("DevicePath {:?}", volume_to_mount.device_path)
            )
        );

        let new_size = resize_op.plugin_resize_opts.new_size;
        if let Err(resize_err) = resize_op
            .volume_plugin
            .node_expand(resize_op.plugin_resize_opts.clone())
            .await
        {
            // This is a workaround for now, until
            // RecoverFromVolumeExpansionFailure feature goes GA. If
            // RecoverFromVolumeExpansionFailure feature is enabled, we will
            // not ever hit this state, because we will wait for
            // VolumeExpansionPendingOnNode before trying to expand volume in
            // kubelet.
            if is_operation_not_supported_error(&resize_err) {
                debug!(
                    pod = %volume_to_mount.pod.metadata.name,
                    "{}",
                    volume_to_mount.generate_msg_detailed(
                        "MountVolume.NodeExpandVolume failed",
                        "NodeExpandVolume not supported"
                    )
                );
                return (true, None);
            }

            // if driver returned FailedPrecondition error that means volume
            // expansion should not be retried on this node but expansion
            // operation should not block mounting
            if is_failed_precondition_error(&resize_err) {
                resize_op
                    .actual_state_of_world
                    .mark_for_in_use_expansion_error(&volume_to_mount.volume_name);
                error!(
                    "{}",
                    volume_to_mount.generate_error_detailed(
                        "MountVolume.NodeExapndVolume failed",
                        &resize_err
                    )
                );
                return (true, None);
            }
            return (false, Some(resize_err));
        }

        let (simple_msg, detailed_msg) =
            volume_to_mount.generate_msg("MountVolume.NodeExpandVolume succeeded", &node_name);
        for involved in [
            crate::events::pod_object_reference(&volume_to_mount.pod),
            pvc_object_reference(pvc),
        ] {
            record_event(
                self.recorder,
                &involved,
                Some(&node_name),
                EventType::Normal,
                FILE_SYSTEM_RESIZE_SUCCESS,
                &simple_msg,
            )
            .await;
        }
        info!(pod = %volume_to_mount.pod.metadata.name, "{detailed_msg}");

        // if PVC already has new size, there is no need to update it.
        if pvc_status_cap.cmp_value(&new_size) != std::cmp::Ordering::Less {
            return (true, None);
        }

        // File system resize succeeded, now update the PVC's Capacity to
        // match the PV's
        if let Err(e) = resize_util::mark_fs_resize_finished(pvc, new_size, self.kube_client).await
        {
            // On retry, NodeExpandVolume will be called again but do nothing
            return (
                false,
                Some(anyhow!(
                    "mountVolume.NodeExpandVolume update PVC status failed : {e}"
                )),
            );
        }
        (true, None)
    }
}

/// Port of the mount half of `operationGenerator.GenerateMountVolumeFunc`
/// (`operation_generator.go:431-640`): find the plugin, `NewMounter`, and for a
/// device-mountable plugin `NewDeviceMounter` -> `GetDeviceMountPath` ->
/// `MountDevice` (`NodeStageVolume` for CSI) strictly BEFORE `SetUp`
/// (`NodePublishVolume`), as the CSI spec orders them. Returns the mounter's
/// path.
///
/// Not ported (each waits on the reconciler/`ActualStateOfWorld` wiring,
/// tracked under #1970): skipping `MountDevice` when
/// `GetDeviceMountState == DeviceGloballyMounted` (`:530`),
/// `MarkDeviceAsMounted` / `markDeviceErrorState` (`:545-566`), `WaitForAttach`
/// (`:505-518`), the ReadWriteOncePod-elsewhere check (`:480-489`), the
/// post-`SetUp` expansion and `MarkVolumeAsMounted` (`:600-640`).
/// `MountDevice` is idempotent per the CSI spec, so repeating it is safe.
pub async fn mount_volume(
    mgr: &VolumePluginMgr,
    spec: &Spec<'_>,
    pod: &Pod,
    device_path: &str,
) -> anyhow::Result<String> {
    let plugin = mgr.find_plugin_by_spec(spec)?;
    let mounter = plugin.new_mounter(spec, pod).await?;

    // get deviceMounter, if possible (`:495-500`)
    let device_mounter = mgr
        .find_device_mountable_plugin_by_spec(spec)
        .and_then(|p| p.as_device_mountable_plugin())
        .and_then(|dm| dm.new_device_mounter().ok());

    if let Some(device_mounter) = device_mounter {
        let device_mount_path = device_mounter.get_device_mount_path(spec)?;
        let args = DeviceMounterArgs {
            fs_group: fs_group_from(pod),
            node_name: pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.clone())
                .unwrap_or_default(),
        };
        device_mounter
            .mount_device(spec, device_path, &device_mount_path, &args)
            .await?;
        info!(
            "MountVolume.MountDevice succeeded for volume {} (device mount path {device_mount_path:?})",
            spec.name()
        );
    }

    mounter.set_up().await?;
    Ok(mounter.get_path())
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
            read_only: false,
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
            read_only: false,
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
            read_only: false,
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_none());
    }

    // ---- node expansion flow (`nodeExpandVolume`, `expandVolumeDuringMount`) ----

    use crate::volume_plugins::util::node_expander::tests::{
        q, test_asow, test_pv, test_pvc, test_vmt, FakeExpandPlugin,
    };
    use rusternetes_common::resources::volume::PersistentVolumeClaim;
    use rusternetes_storage::StorageBackend;
    use std::sync::atomic::Ordering;

    struct Fixture {
        storage: StorageBackend,
        recorder: EventRecorder<StorageBackend>,
        mgr: VolumePluginMgr,
        plugin: FakeExpandPlugin,
    }

    impl Fixture {
        async fn new(pvc: &PersistentVolumeClaim, plugin: FakeExpandPlugin) -> Self {
            let storage = StorageBackend::new_memory();
            storage
                .create(
                    &build_key("persistentvolumeclaims", Some("ns"), "test-vol0"),
                    pvc,
                )
                .await
                .unwrap();
            Self {
                storage,
                recorder: EventRecorder::new(Arc::new(StorageBackend::new_memory())),
                mgr: VolumePluginMgr::new(vec![Box::new(plugin.sharing())]),
                plugin,
            }
        }

        fn og(&self) -> OperationGenerator<'_, StorageBackend> {
            OperationGenerator {
                volume_plugin_mgr: &self.mgr,
                kube_client: &self.storage,
                recorder: &self.recorder,
            }
        }

        async fn pvc(&self) -> PersistentVolumeClaim {
            self.storage
                .get(&build_key(
                    "persistentvolumeclaims",
                    Some("ns"),
                    "test-vol0",
                ))
                .await
                .unwrap()
        }

        fn called(&self) -> bool {
            self.plugin.called.load(Ordering::SeqCst)
        }
    }

    fn opts<'a>(spec: &'a Spec<'a>, old: &str, new: &str) -> NodeResizeOptions<'a> {
        NodeResizeOptions {
            volume_spec: spec,
            device_path: String::new(),
            device_mount_path: String::new(),
            device_stage_path: String::new(),
            new_size: q(new),
            old_size: q(old),
        }
    }

    /// `nodeExpandVolume`: `NewSize <= OldSize` is a no-op that reports the
    /// old size (`operation_generator.go:2023`, `:2062`).
    #[tokio::test]
    async fn node_expand_volume_without_growth_is_a_noop() {
        let pvc = test_pvc("2G", "2G", "2G", "", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, size, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "2G", "2G"))
            .await;
        assert!(finished && err.is_none());
        assert_eq!(size.cmp_value(&q("2G")), std::cmp::Ordering::Equal);
        assert!(!f.called());
    }

    /// With a resize controller that populates `allocatedResources`, the
    /// recovery `nodeExpander` runs and the PVC capacity is raised
    /// (`:2048-2055`).
    #[tokio::test]
    async fn node_expand_volume_uses_recovery_expander_and_updates_pvc() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, size, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(finished && err.is_none(), "{err:?}");
        assert_eq!(size.cmp_value(&q("2G")), std::cmp::Ordering::Equal);
        assert!(f.called());
        let status = f.pvc().await.status.unwrap();
        assert_eq!(status.capacity.unwrap()["storage"], "2G");
        assert!(status.allocated_resource_statuses.is_none());
    }

    /// An older resize controller sets neither `allocatedResources` nor a
    /// resize status, so `checkForRecoveryFromExpansion` is false and the
    /// legacy path runs: it expands, then `MarkFSResizeFinished`
    /// (`:2056-2059`, `:2093-2135`).
    #[tokio::test]
    async fn node_expand_volume_falls_back_to_legacy_path_for_old_controller() {
        let pvc = test_pvc("2G", "1G", "", "", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        assert!(!f
            .og()
            .check_for_recovery_from_expansion(&pvc, &test_vmt(&test_pv("2G"), "2G")));
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, size, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(finished && err.is_none(), "{err:?}");
        assert_eq!(size.cmp_value(&q("2G")), std::cmp::Ordering::Equal);
        assert!(f.called());
        assert_eq!(
            f.pvc().await.status.unwrap().capacity.unwrap()["storage"],
            "2G"
        );
    }

    /// Legacy path: a `FailedPrecondition` marks the in-use error and does not
    /// block the mount (`:2112-2120`); any other error is returned.
    #[tokio::test]
    async fn legacy_failed_precondition_marks_in_use_and_finishes() {
        use crate::volume_plugins::util::types::VolumeOperationError;
        fn fp() -> anyhow::Error {
            VolumeOperationError::FailedPrecondition("in use".into()).into()
        }
        fn other() -> anyhow::Error {
            anyhow::anyhow!("boom")
        }
        let pvc = test_pvc("2G", "1G", "", "", false, false);
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();

        let f = Fixture::new(&pvc, FakeExpandPlugin::failing(fp)).await;
        let (finished, _, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(finished && err.is_none());
        // PVC untouched: the resize did not happen.
        assert_eq!(
            f.pvc().await.status.unwrap().capacity.unwrap()["storage"],
            "1G"
        );

        let f = Fixture::new(&pvc, FakeExpandPlugin::failing(other)).await;
        let (finished, _, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(!finished && err.is_some());
    }

    /// A read-only mount cannot be resized: finished, plugin not called
    /// (`:2032-2038`).
    #[tokio::test]
    async fn node_expand_volume_skips_read_only_mounts() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let mut vmt = test_vmt(&pv, "2G");
        let mut owned = vmt.volume_spec.as_spec().to_owned_spec();
        // `NewSpecFromPersistentVolume(pv, true)`
        // (`desired_state_of_world_populator.go:588`).
        owned.read_only = true;
        vmt.volume_spec = Arc::new(owned);
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, size, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(finished && err.is_none());
        assert_eq!(size.cmp_value(&q("1G")), std::cmp::Ordering::Equal);
        assert!(!f.called());
    }

    /// A PVC that cannot be fetched is an error, not a silent skip (`:2026-2030`).
    #[tokio::test]
    async fn node_expand_volume_errors_when_pvc_is_missing() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        f.storage
            .delete(&build_key(
                "persistentvolumeclaims",
                Some("ns"),
                "test-vol0",
            ))
            .await
            .unwrap();
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, _, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(!finished);
        assert!(err
            .unwrap()
            .to_string()
            .starts_with("mountVolume.NodeExpandVolume get PVC failed"));
    }

    /// `expandVolumeDuringMount`: nothing to do once `pvc.status.capacity`
    /// has reached `pv.spec.capacity` (`:1961`, `:1993`); otherwise it expands.
    #[tokio::test]
    async fn expand_volume_during_mount_expands_only_when_pvc_is_behind() {
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();

        let current = test_pvc("2G", "2G", "", "", false, false);
        let f = Fixture::new(&current, FakeExpandPlugin::ok()).await;
        let (finished, err) = f
            .og()
            .expand_volume_during_mount(&vmt, &asow, opts(&spec, "0", "0"))
            .await;
        assert!(finished && err.is_none());
        assert!(!f.called());

        let behind = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&behind, FakeExpandPlugin::ok()).await;
        let (finished, err) = f
            .og()
            .expand_volume_during_mount(&vmt, &asow, opts(&spec, "0", "0"))
            .await;
        assert!(finished && err.is_none(), "{err:?}");
        assert!(f.called());
        assert_eq!(
            f.pvc().await.status.unwrap().capacity.unwrap()["storage"],
            "2G"
        );
    }
}
