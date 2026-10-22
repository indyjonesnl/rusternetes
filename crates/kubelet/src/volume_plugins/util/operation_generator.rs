//! Port of the node-expansion half of
//! `pkg/volume/util/operationexecutor/operation_generator.go`:
//! `checkIfSupportsNodeExpansion`, `nodeExpandVolume`,
//! `expandVolumeDuringMount`, `checkForRecoveryFromExpansion` and
//! `legacyCallNodeExpandOnPlugin`, driving [`super::node_expander`].
//!
//! `GenerateExpandInUseVolumeFunc` / `doOnlineExpansion` — the closure
//! wrapper around `nodeExpandVolume` — are ported as
//! [`OperationGenerator::generate_expand_in_use_volume_func`] and
//! [`GeneratedExpandInUse`]. Nothing in the running kubelet calls these
//! functions yet: the operation executor that runs a `GeneratedOperations`
//! (`ExpandInUseVolume`, `operation_executor.go:925`) and the volume-manager
//! reconciler's expand-in-use hook do not exist (#1970).

use crate::events::{FILE_SYSTEM_RESIZE_FAILED, FILE_SYSTEM_RESIZE_SUCCESS, VOLUME_RESIZE_FAILED};
use crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
use crate::volume_manager::cache::desired_state_of_world::{
    desired_size_limit, effective_selinux_mount_label, get_selinux_label,
};
use crate::volume_plugins::plugin::{
    DeviceMountableVolumePlugin, DeviceMounter, DeviceMounterArgs, DeviceUnmounter,
    NodeExpandableVolumePlugin, NodeResizeOptions, Spec,
};
use crate::volume_plugins::registry::VolumePluginMgr;
use crate::volume_plugins::util::hostutil::HostUtils;
use crate::volume_plugins::util::node_expander::{
    pvc_object_reference, record_event, NodeExpander, NodeResizeOperationOpts,
};
use crate::volume_plugins::util::operation_executor::{AttachedVolume, VolumeToMount};
use crate::volume_plugins::util::resize_util::{self, RESOURCE_STORAGE};
use crate::volume_plugins::util::types::{
    is_failed_precondition_error, is_operation_not_supported_error, OperationContext,
};
use crate::volume_plugins::util::{check_device_mount_refs, fs_group_from};
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
/// `InlineVolumeSpecForCSIMigration` guard (`:1997-2001`, "a migrated inline
/// volume and is not resizable") returns `false, nil` first.
pub fn check_if_supports_node_expansion<'a>(
    mgr: &'a VolumePluginMgr,
    spec: &Spec<'_>,
) -> Option<&'a dyn NodeExpandableVolumePlugin> {
    if spec.inline_volume_spec_for_csi_migration {
        return None;
    }
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

/// `waitForAttachTimeout` (`pkg/kubelet/volumemanager/volume_manager.go:88`).
const WAIT_FOR_ATTACH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10 * 60);

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
/// `MarkDeviceAsMounted` / `markDeviceErrorState` (`:545-566`), the ReadWriteOncePod-elsewhere check (`:480-489`), the
/// post-`SetUp` expansion and `MarkVolumeAsMounted` (`:600-640`).
/// `MountDevice` is idempotent per the CSI spec, so repeating it is safe.
///
/// `WaitForAttach` (`:511-526`) runs for plugins that return an attacher from
/// [`VolumePlugin::as_attachable_plugin`] (iscsi); CSI's attacher is not ported.
pub async fn mount_volume(
    mgr: &VolumePluginMgr,
    spec: &Spec<'_>,
    pod: &Pod,
    device_path: &str,
) -> anyhow::Result<String> {
    Ok(
        mount_volume_with_attributes(mgr, spec, pod, device_path, None)
            .await?
            .0,
    )
}

/// [`mount_volume`], also returning `mounter.GetAttributes()` read after
/// `SetUp` (`volume.go:187`) - what the kubelet's `makeMounts` consumes.
pub async fn mount_volume_with_attributes(
    mgr: &VolumePluginMgr,
    spec: &Spec<'_>,
    pod: &Pod,
    device_path: &str,
    recorder: Option<crate::volume_plugins::plugin::MounterRecorder>,
) -> anyhow::Result<(String, crate::volume_plugins::plugin::Attributes)> {
    let plugin = mgr.find_plugin_by_spec(spec)?;
    let mounter = plugin.new_mounter(spec, pod).await?;

    // Get attacher, if possible (`:486-492`)
    let volume_attacher = mgr
        .find_attachable_plugin_by_spec(spec)
        .and_then(|p| p.as_attachable_plugin())
        .and_then(|ap| ap.new_attacher().ok());

    // get deviceMounter, if possible (`:495-500`)
    let device_mounter = mgr
        .find_device_mountable_plugin_by_spec(spec)
        .and_then(|p| p.as_device_mountable_plugin())
        .and_then(|dm| dm.new_device_mounter().ok());

    let mut device_path = device_path.to_string();
    if let Some(volume_attacher) = &volume_attacher {
        // Wait for attachable volumes to finish attaching (`:513-525`)
        info!(
            "MountVolume.WaitForAttach entering for volume {} (DevicePath {device_path:?})",
            spec.name()
        );
        device_path = volume_attacher
            .wait_for_attach(spec, &device_path, pod, WAIT_FOR_ATTACH_TIMEOUT)
            .await
            .map_err(|e| {
                anyhow!(
                    "MountVolume.WaitForAttach failed for volume {}: {e}",
                    spec.name()
                )
            })?;
        info!(
            "MountVolume.WaitForAttach succeeded for volume {} (DevicePath {device_path:?})",
            spec.name()
        );
    }
    let device_path = device_path.as_str();

    // `volumeToMount.SELinuxLabel` / `DesiredSizeLimit`: computed once and used
    // by both `MountDevice` (`DeviceMounterArgs.SELinuxLabel`, `:547`) and
    // `SetUp` (`MounterArgs.SELinuxLabel`, `:588`).
    let mounter_args = mounter_args_for(
        mgr,
        spec,
        pod,
        &crate::volume_plugins::util::selinux::Translator,
    )?;

    if let Some(device_mounter) = device_mounter {
        let device_mount_path = device_mounter.get_device_mount_path(spec)?;
        let args = DeviceMounterArgs {
            fs_group: fs_group_from(pod),
            selinux_label: mounter_args.selinux_label.clone(),
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

    // `Recorder: og.recorder` (`operation_generator.go:587`).
    let mut mounter_args = mounter_args;
    mounter_args.recorder = recorder;
    mounter.set_up_with(&mounter_args).await?;
    Ok((mounter.get_path(), mounter.get_attributes()))
}

/// `util.CheckVolumeModeFilesystem` (`pkg/volume/util/util.go:354-365`): true
/// for Filesystem, false for Block; a PV without `volumeMode` is an error
/// (`GetVolumeMode`).
fn check_volume_mode_filesystem(spec: &Spec<'_>) -> anyhow::Result<bool> {
    let mode = crate::volume_plugins::iscsi::get_volume_mode(spec)?;
    Ok(!matches!(
        mode,
        rusternetes_common::resources::volume::PersistentVolumeMode::Block
    ))
}

/// The operation `GenerateExpandInUseVolumeFunc` returns
/// (`operation_generator.go:1815-1923`): the `GeneratedOperations` with
/// `OperationName: "volume_fs_resize"`, whose `OperationFunc` is
/// [`Self::run`] and whose `EventRecorderFunc` is
/// [`Self::record_event_on_error`].
///
/// Not ported: `CompleteFunc` (`util.OperationCompleteHook`, the per-operation
/// metrics hook), for lack of an operation executor to consume it (#1970).
pub struct GeneratedExpandInUse<'v, 'a, S: Storage + ?Sized> {
    /// `OperationName`.
    pub operation_name: &'static str,
    og: &'v OperationGenerator<'a, S>,
    volume_plugin: &'v dyn crate::volume_plugins::plugin::VolumePlugin,
    volume_to_mount: &'v VolumeToMount,
    actual_state_of_world: &'v ActualStateOfWorld,
    current_size: Quantity,
}

impl<'a, S: Storage + ?Sized> OperationGenerator<'a, S> {
    /// Port of `operationGenerator.GenerateExpandInUseVolumeFunc`
    /// (`operation_generator.go:1815-1923`): find the plugin for the spec and
    /// return the operation that expands the in-use volume from
    /// `current_size` to `volumeToMount.DesiredPersistentVolumeSize`.
    pub fn generate_expand_in_use_volume_func<'v>(
        &'v self,
        volume_to_mount: &'v VolumeToMount,
        actual_state_of_world: &'v ActualStateOfWorld,
        current_size: Quantity,
    ) -> anyhow::Result<GeneratedExpandInUse<'v, 'a, S>> {
        let spec = volume_to_mount.volume_spec.as_spec();
        let volume_plugin = self
            .volume_plugin_mgr
            .find_plugin_by_spec(&spec)
            .map_err(|e| {
                volume_to_mount.generate_error_detailed(
                    "NodeExpandVolume.FindPluginBySpec failed",
                    &anyhow!(e),
                )
            })?;
        Ok(GeneratedExpandInUse {
            operation_name: "volume_fs_resize",
            og: self,
            volume_plugin,
            volume_to_mount,
            actual_state_of_world,
            current_size,
        })
    }
}

impl<'v, 'a, S: Storage + ?Sized> GeneratedExpandInUse<'v, 'a, S>
where
    'a: 'v,
{
    /// The plugin's name, for the `CompleteFunc` hook's metric label
    /// (`GetFullQualifiedPluginNameForVolume(volumePlugin.GetPluginName(), ..)`).
    pub fn plugin_name(&self) -> &str {
        self.volume_plugin.name()
    }

    /// The `eventRecorderFunc` closure (`:1909-1913`): a failed resize is
    /// recorded as a `VolumeResizeFailed` warning on the pod.
    pub async fn record_event_on_error(&self, err: Option<&Error>) {
        let Some(err) = err else { return };
        let pod = &self.volume_to_mount.pod;
        let node_name = pod.spec.as_ref().and_then(|s| s.node_name.as_deref());
        record_event(
            self.og.recorder,
            &crate::events::pod_object_reference(pod),
            node_name,
            EventType::Warning,
            VOLUME_RESIZE_FAILED,
            &err.to_string(),
        )
        .await;
    }

    /// The `fsResizeFunc` closure (`:1826-1907`).
    pub async fn run(&self) -> OperationContext {
        let vmt = self.volume_to_mount;
        let og = self.og;
        // `migrated := false`: this closure never consults the migration
        // status upstream either.
        let migrated = false;
        let fail = |prefix: &str, err: &Error| {
            let (event_err, detailed_err) = vmt.generate_error(prefix, err);
            OperationContext::new(Some(event_err), Some(detailed_err), migrated)
        };

        let desired = match vmt.desired_persistent_volume_size {
            Some(d) if !d.is_zero() && !self.current_size.is_zero() => d,
            _ => {
                return fail(
                    "NodeExpandvolume.expansion failed",
                    &anyhow!("current or new size of the volume is not set"),
                )
            }
        };

        let spec = vmt.volume_spec.as_spec();
        let mut resize_options = NodeResizeOptions {
            volume_spec: &spec,
            device_path: vmt.device_path.clone(),
            device_mount_path: String::new(),
            device_stage_path: String::new(),
            old_size: self.current_size,
            new_size: desired,
        };
        let fs_volume = match check_volume_mode_filesystem(&spec) {
            Ok(v) => v,
            Err(e) => return fail("NodeExpandvolume.CheckVolumeModeFilesystem failed", &e),
        };

        if fs_volume {
            let volume_mounter = match self.volume_plugin.new_mounter(&spec, &vmt.pod).await {
                Ok(m) => m,
                Err(e) => return fail("NodeExpandVolume.NewMounter initialization failed", &e),
            };
            resize_options.device_mount_path = volume_mounter.get_path();

            // `deviceMountableVolumePlugin, _ := FindDeviceMountablePluginBySpec`
            // and `volumeDeviceMounter, _ = NewDeviceMounter()`: both errors
            // are discarded upstream.
            let volume_device_mounter = og
                .volume_plugin_mgr
                .find_device_mountable_plugin_by_spec(&spec)
                .and_then(|p| p.as_device_mountable_plugin())
                .and_then(|p| p.new_device_mounter().ok());
            if let Some(mounter) = volume_device_mounter {
                match mounter.get_device_mount_path(&spec) {
                    Ok(path) => resize_options.device_stage_path = path,
                    Err(e) => return fail("NodeExpandVolume.GetDeviceMountPath failed", &e),
                }
            }
        } else {
            // Get block volume mapper plugin
            let block_volume_plugin = match og.volume_plugin_mgr.find_mapper_plugin_by_spec(&spec)
            {
                Ok(Some(p)) => p,
                Ok(None) => {
                    return fail(
                        "MapVolume.FindMapperPluginBySpec failed to find BlockVolumeMapper plugin. Volume plugin is nil.",
                        &anyhow!("<nil>"),
                    )
                }
                Err(e) => return fail("MapVolume.FindMapperPluginBySpec failed", &anyhow!(e)),
            };
            let block_volume_mapper = match block_volume_plugin
                .new_block_volume_mapper(&spec, &vmt.pod)
            {
                Ok(m) => m,
                Err(e) => return fail("MapVolume.NewBlockVolumeMapper initialization failed", &e),
            };
            // if plugin supports custom mappers lets add DeviceStagePath
            if let Some(custom) = block_volume_mapper.as_custom_block_volume_mapper() {
                resize_options.device_stage_path = custom.get_staging_path();
            }
        }

        // if we are doing online expansion then volume is already published
        let (resize_done, event_err, detailed_err) = self.do_online_expansion(resize_options).await;
        if event_err.is_some() || detailed_err.is_some() {
            return OperationContext::new(event_err, detailed_err, migrated);
        }
        if !resize_done {
            info!(
                "Waiting for volume to be expandable on the node (volumeName {:?})",
                vmt.volume_name.0
            );
        }
        OperationContext::new(None, None, migrated)
    }

    /// Port of `doOnlineExpansion` (`operation_generator.go:1925-1947`):
    /// `(resizeDone, eventErr, detailedErr)`.
    async fn do_online_expansion(
        &self,
        resize_options: NodeResizeOptions<'_>,
    ) -> (bool, Option<Error>, Option<Error>) {
        let vmt = self.volume_to_mount;
        let (resize_done, new_size, err) = self
            .og
            .node_expand_volume(vmt, self.actual_state_of_world, resize_options)
            .await;
        if let Some(err) = err {
            let (e1, e2) = vmt.generate_error("NodeExpandVolume.NodeExpandVolume failed", &err);
            error!("{e2}");
            return (false, Some(e1), Some(e2));
        }
        if resize_done {
            let marking_done = self
                .actual_state_of_world
                .mark_volume_as_resized(&vmt.volume_name, new_size);
            if !marking_done {
                // On failure, return error. Caller will log and retry.
                let (e1, e2) = vmt.generate_error(
                    "NodeExpandVolume.MarkVolumeAsResized failed",
                    &anyhow!("unable to mark volume as resized"),
                );
                return (false, Some(e1), Some(e2));
            }
            return (true, None, None);
        }
        (false, None, None)
    }
}

/// The operation `GenerateUnmountDeviceFunc` returns
/// (`operation_generator.go:807-905`), i.e. the `GeneratedOperations` with
/// `OperationName: "unmount_device"` and the `OperationFunc` closure
/// ([`Self::run`]).
///
/// `CompleteFunc` is [`Self::complete_func`]
/// (`util.OperationCompleteHook(GetFullQualifiedPluginNameForVolume(..),
/// "unmount_device")`, `:910`), run by [`Self::run_operation`] the way
/// `GeneratedOperations.Run` (`pkg/volume/util/types/types.go:68-85`) wraps
/// the `OperationFunc`. Not ported: `EventRecorderFunc` (nil upstream too).
pub struct GeneratedUnmountDevice<'a> {
    /// `OperationName`.
    pub operation_name: &'static str,
    /// `CompleteFunc`.
    pub complete_func: crate::volume_plugins::util::metrics::OperationCompleteHook,
    device_mountable_volume_plugin: &'a dyn DeviceMountableVolumePlugin,
    volume_device_unmounter: Box<dyn DeviceUnmounter>,
    volume_device_mounter: Box<dyn DeviceMounter>,
    device_to_detach: &'a AttachedVolume,
    actual_state_of_world: &'a ActualStateOfWorld,
    hostutil: &'a dyn HostUtils,
}

/// Port of `operationGenerator.GenerateUnmountDeviceFunc`
/// (`operation_generator.go:807-905`): find the `DeviceMountableVolumePlugin`
/// by name, take its `DeviceUnmounter` and `DeviceMounter`, and return the
/// operation that unstages the device.
///
/// A free function over the plugin manager rather than a method of
/// [`OperationGenerator`]: the generator's client and recorder are unused by
/// this operation. `getMigratedStatusBySpec` (`:2222`) is always false here —
/// there is no in-tree-to-CSI migration.
pub fn generate_unmount_device_func<'a>(
    mgr: &'a VolumePluginMgr,
    device_to_detach: &'a AttachedVolume,
    actual_state_of_world: &'a ActualStateOfWorld,
    hostutil: &'a dyn HostUtils,
) -> anyhow::Result<GeneratedUnmountDevice<'a>> {
    // Get DeviceMounter plugin
    let device_mountable_volume_plugin =
        match mgr.find_device_mountable_plugin_by_name(&device_to_detach.plugin_name) {
            Ok(Some(plugin)) => plugin,
            // `err != nil || deviceMountableVolumePlugin == nil`: a nil error
            // for a plugin that is not device-mountable.
            Ok(None) => {
                return Err(device_to_detach.generate_error_detailed(
                    "UnmountDevice.FindDeviceMountablePluginByName failed",
                    None,
                ))
            }
            Err(e) => {
                return Err(device_to_detach.generate_error_detailed(
                    "UnmountDevice.FindDeviceMountablePluginByName failed",
                    Some(&anyhow!(e)),
                ))
            }
        };

    let volume_device_unmounter = device_mountable_volume_plugin
        .new_device_unmounter()
        .map_err(|e| {
            device_to_detach
                .generate_error_detailed("UnmountDevice.NewDeviceUnmounter failed", Some(&e))
        })?;

    let volume_device_mounter = device_mountable_volume_plugin
        .new_device_mounter()
        .map_err(|e| {
            device_to_detach
                .generate_error_detailed("UnmountDevice.NewDeviceMounter failed", Some(&e))
        })?;

    let complete_func = crate::volume_plugins::util::metrics::operation_complete_hook(
        &crate::volume_plugins::util::metrics::get_full_qualified_plugin_name_for_volume(
            device_mountable_volume_plugin.name(),
            Some(&device_to_detach.volume_spec.as_spec()),
        ),
        "unmount_device",
    );
    Ok(GeneratedUnmountDevice {
        operation_name: "unmount_device",
        complete_func,
        device_mountable_volume_plugin,
        volume_device_unmounter,
        volume_device_mounter,
        device_to_detach,
        actual_state_of_world,
        hostutil,
    })
}

impl GeneratedUnmountDevice<'_> {
    /// `GeneratedOperations.Run` (`types.go:68-85`): the `OperationFunc`, then
    /// `CompleteFunc` with its detailed error and migrated status.
    pub async fn run_operation(&self) -> OperationContext {
        let ctx = self.run().await;
        self.complete_func
            .complete(ctx.detailed_err.as_ref(), Some(ctx.migrated));
        ctx
    }

    /// The `unmountDeviceFunc` closure (`operation_generator.go:830-896`).
    pub async fn run(&self) -> OperationContext {
        let device_to_detach = self.device_to_detach;
        let asw = self.actual_state_of_world;
        // `getMigratedStatusBySpec`: see `generate_unmount_device_func`.
        let migrated = false;
        let spec = device_to_detach.volume_spec.as_spec();

        //deviceMountPath := deviceToDetach.DeviceMountPath
        let device_mount_path = match self.volume_device_mounter.get_device_mount_path(&spec) {
            Ok(path) => path,
            Err(err) => {
                // On failure other than "does not exist", return error. Caller will log and retry.
                if !format!("{err:#}").contains("does not exist") {
                    let (event_err, detailed_err) =
                        device_to_detach.generate_error("GetDeviceMountPath failed", &err);
                    return OperationContext::new(Some(event_err), Some(detailed_err), migrated);
                }
                // If the mount path could not be found, don't fail the unmount, but instead log a warning and proceed,
                // using the value from deviceToDetach.DeviceMountPath, so that the device can be marked as unmounted
                let device_mount_path = device_to_detach.device_mount_path.clone();
                warn!(
                    "{}",
                    device_to_detach.generate_msg_detailed(
                        &format!(
                            "GetDeviceMountPath failed, but unmount operation will proceed using deviceMountPath={device_mount_path}: {err}"
                        ),
                        ""
                    )
                );
                device_mount_path
            }
        };

        // `refs, err := GetDeviceMountRefs(..); if err != nil || HasMountRefs(..)`
        let refs = self
            .device_mountable_volume_plugin
            .get_device_mount_refs(&device_mount_path);
        if let Err(err) = check_device_mount_refs(&device_mount_path, refs) {
            let (event_err, detailed_err) =
                device_to_detach.generate_error("GetDeviceMountRefs check failed", &err);
            return OperationContext::new(Some(event_err), Some(detailed_err), migrated);
        }

        // Execute unmount
        if let Err(unmount_device_err) = self
            .volume_device_unmounter
            .unmount_device(&device_mount_path)
            .await
        {
            // Mark the device as uncertain, so MountDevice is called for new pods. UnmountDevice may be already in progress.
            self.mark_device_as_uncertain(&device_mount_path);

            // On failure, return error. Caller will log and retry.
            let (event_err, detailed_err) =
                device_to_detach.generate_error("UnmountDevice failed", &unmount_device_err);
            return OperationContext::new(Some(event_err), Some(detailed_err), migrated);
        }

        // Before logging that UnmountDevice succeeded and moving on,
        // use hostutil.PathIsDevice to check if the path is a device,
        // if so use hostutil.DeviceOpened to check if the device is in use anywhere
        // else on the system. Retry if it returns true.
        let device_opened = match is_device_opened(device_to_detach, self.hostutil) {
            Ok(opened) => opened,
            Err(device_opened_err) => {
                return OperationContext::new(None, Some(device_opened_err), migrated);
            }
        };
        // The device is still in use elsewhere. Caller will log and retry.
        if device_opened {
            // Mark the device as uncertain, so MountDevice is called for new pods.
            self.mark_device_as_uncertain(&device_mount_path);
            let (event_err, detailed_err) = device_to_detach.generate_error(
                "UnmountDevice failed",
                &anyhow!("the device is in use when it was no longer expected to be in use"),
            );
            return OperationContext::new(Some(event_err), Some(detailed_err), migrated);
        }

        info!(
            "{}",
            device_to_detach.generate_msg_detailed("UnmountDevice succeeded", "")
        );

        // Update actual state of world
        if let Err(mark_device_unmounted_err) =
            asw.mark_device_as_unmounted(&device_to_detach.volume_name)
        {
            // On failure, return error. Caller will log and retry.
            let (event_err, detailed_err) = device_to_detach.generate_error(
                "MarkDeviceAsUnmounted failed",
                &anyhow!(mark_device_unmounted_err),
            );
            return OperationContext::new(Some(event_err), Some(detailed_err), migrated);
        }

        OperationContext::new(None, None, migrated)
    }

    /// `actualStateOfWorld.MarkDeviceAsUncertain(..)` with the upstream
    /// handling of its error: "There is nothing else we can do. Hope that
    /// UnmountDevice will be re-tried shortly." (`:862-865`, `:884-887`).
    fn mark_device_as_uncertain(&self, device_mount_path: &str) {
        let d = self.device_to_detach;
        if let Err(e) = self.actual_state_of_world.mark_device_as_uncertain(
            &d.volume_name,
            &d.device_path,
            device_mount_path,
            &d.selinux_mount_context,
        ) {
            error!(
                "{}",
                d.generate_error_detailed(
                    "UnmountDevice.MarkDeviceAsUncertain failed",
                    Some(&anyhow!(e))
                )
            );
        }
    }
}

/// Port of `isDeviceOpened` (`operation_generator.go:2179-2198`): after
/// `UnmountDevice`, is the device still opened elsewhere on the system?
fn is_device_opened(
    device_to_detach: &AttachedVolume,
    host_util: &dyn HostUtils,
) -> anyhow::Result<bool> {
    match host_util.path_is_device(&device_to_detach.device_path) {
        Ok(false) => {
            // not a device path or path doesn't exist
            // TODO: refer to #36092
            debug!(
                "The path isn't device path or doesn't exist. Skip checking device path: {}",
                device_to_detach.device_path
            );
            Ok(false)
        }
        Err(e) if e.to_string().contains("does not exist") => {
            debug!(
                "The path isn't device path or doesn't exist. Skip checking device path: {}",
                device_to_detach.device_path
            );
            Ok(false)
        }
        Err(e) => Err(device_to_detach
            .generate_error_detailed("PathIsDevice failed", Some(&anyhow!(e.to_string())))),
        Ok(true) => host_util
            .device_opened(&device_to_detach.device_path)
            .map_err(|e| {
                device_to_detach
                    .generate_error_detailed("DeviceOpened failed", Some(&anyhow!(e.to_string())))
            }),
    }
}

/// The `volume.MounterArgs` the operation generator hands to `SetUp`
/// (`operation_generator.go:582-589`):
///
/// ```go
/// volumeMounter.SetUp(volume.MounterArgs{
///     FsUser:              util.FsUserFrom(volumeToMount.Pod),
///     FsGroup:             fsGroup,
///     DesiredSize:         volumeToMount.DesiredSizeLimit,
///     FSGroupChangePolicy: fsGroupChangePolicy,
///     Recorder:            og.recorder,
///     SELinuxLabel:        volumeToMount.SELinuxLabel,
/// })
/// ```
///
/// Upstream reads `DesiredSizeLimit` and `SELinuxLabel` off the
/// `VolumeToMount` that `DesiredStateOfWorld.AddPodToVolume` filled in. The
/// kubelet's `create_volume` has no `DesiredStateOfWorld` yet, so they are
/// computed here by the same functions `AddPodToVolume` uses
/// ([`desired_size_limit`], [`get_effective_selinux_mount_label`]).
pub(crate) fn mounter_args_for(
    mgr: &VolumePluginMgr,
    spec: &Spec<'_>,
    pod: &Pod,
    selinux_translator: &dyn crate::volume_plugins::util::selinux::SELinuxLabelTranslator,
) -> anyhow::Result<crate::volume_plugins::MounterArgs> {
    // `AddPodToVolume` (`desired_state_of_world.go:317-336`): the label the
    // containers that mount the volume need, cleared for a volume whose access
    // mode cannot carry `-o context=`.
    let (selinux_file_label, _plugin_supports_selinux_context_mount) = get_selinux_label(
        mgr,
        selinux_translator,
        spec,
        &crate::volume_plugins::util::selinux_container_contexts(pod, &spec.volume.name),
        pod.spec.as_ref().and_then(|s| s.security_context.as_ref()),
    )
    .map_err(|e| anyhow!("{e}"))?;
    let selinux_label = effective_selinux_mount_label(spec, &selinux_file_label);
    Ok(crate::volume_plugins::MounterArgs {
        fs_user: crate::volume_plugins::util::fs_user_from(pod),
        fs_group: fs_group_from(pod),
        fs_group_change_policy: pod
            .spec
            .as_ref()
            .and_then(|s| s.security_context.as_ref())
            .and_then(|sc| sc.fs_group_change_policy.clone()),
        desired_size: desired_size_limit(pod, spec.volume).map(|q| {
            // `resource.Quantity.Value()`: an int64.
            q.value().clamp(i64::MIN as i128, i64::MAX as i128) as i64
        }),
        selinux_label,
        // `Recorder: og.recorder`: set by the caller that owns a recorder
        // ([`mount_volume_with_attributes`]).
        recorder: None,
    })
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
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_some());
    }

    /// `checkIfSupportsNodeExpansion`'s leading guard
    /// (`operation_generator.go:1997-2001`): a migrated inline volume is not
    /// resizable even though its translated spec carries a CSI PV.
    #[test]
    fn migrated_inline_volume_does_not_support_node_expansion() {
        let m = mgr();
        let v: Volume = serde_json::from_value(
            json!({"name": "v", "persistentVolumeClaim": {"claimName": "c"}}),
        )
        .unwrap();
        let pv = pv();
        let mut spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: true,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_some());
        spec.inline_volume_spec_for_csi_migration = true;
        assert!(check_if_supports_node_expansion(&m, &spec).is_none());
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
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
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
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
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

    // ---- `GenerateExpandInUseVolumeFunc` (`operation_generator.go:1815-1947`) ----

    /// An actual state of world that knows the fixture's plugin, with the
    /// volume attached.
    fn attached_asow(f: &Fixture, vmt: &VolumeToMount) -> ActualStateOfWorld {
        let asow = ActualStateOfWorld::new(
            "node-1",
            Arc::new(VolumePluginMgr::new(vec![Box::new(f.plugin.sharing())])),
        );
        asow.mark_volume_as_attached(
            Some(&vmt.volume_name),
            Arc::clone(&vmt.volume_spec),
            "",
            "/dev/fake",
        )
        .unwrap();
        asow
    }

    /// The closure refuses an unset current or desired size
    /// (`:1832-1836`) before touching any plugin.
    #[tokio::test]
    async fn expand_in_use_requires_current_and_new_size() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let asow = test_asow();
        let mut vmt = test_vmt(&pv, "2G");
        let og = f.og();
        let op = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("0"))
            .unwrap();
        assert_eq!(op.operation_name, "volume_fs_resize");
        let ctx = op.run().await;
        let err = ctx.detailed_err.expect("zero current size is an error");
        assert!(err
            .to_string()
            .contains("current or new size of the volume is not set"));
        assert!(!f.called());

        vmt.desired_persistent_volume_size = None;
        let op = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .unwrap();
        assert!(op.run().await.event_err.is_some());
        assert!(!f.called());
    }

    /// Online expansion: `nodeExpandVolume` runs, then `MarkVolumeAsResized`
    /// records the new size in the actual state of world (`:1925-1947`).
    #[tokio::test]
    async fn expand_in_use_expands_and_marks_volume_resized() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = attached_asow(&f, &vmt);
        let og = f.og();
        let op = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .unwrap();
        let ctx = op.run().await;
        assert!(
            ctx.event_err.is_none() && ctx.detailed_err.is_none(),
            "{:?}",
            ctx.detailed_err
        );
        assert!(f.called());
        assert_eq!(
            f.pvc().await.status.unwrap().capacity.unwrap()["storage"],
            "2G"
        );
    }

    /// A volume the ASW does not track cannot be marked resized: the
    /// operation reports `MarkVolumeAsResized failed` for the caller to retry
    /// (`:1938-1944`).
    #[tokio::test]
    async fn expand_in_use_reports_unmarkable_volume() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let asow = test_asow();
        let vmt = test_vmt(&pv, "2G");
        let og = f.og();
        let ctx = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .unwrap()
            .run()
            .await;
        assert!(ctx
            .detailed_err
            .unwrap()
            .to_string()
            .contains("NodeExpandVolume.MarkVolumeAsResized failed"));
    }

    /// A plugin failure surfaces as `NodeExpandVolume.NodeExpandVolume failed`
    /// and the event recorder func emits a `VolumeResizeFailed` warning on
    /// the pod (`:1909-1913`).
    #[tokio::test]
    async fn expand_in_use_failure_is_reported_and_recorded() {
        let pvc = test_pvc("2G", "1G", "", "", false, false);
        let plugin = FakeExpandPlugin::failing(|| anyhow!("boom"));
        let f = Fixture::new(&pvc, plugin).await;
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = attached_asow(&f, &vmt);
        let og = f.og();
        let op = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .unwrap();
        let ctx = op.run().await;
        let event_err = ctx.event_err.expect("plugin failure is an error");
        assert!(event_err
            .to_string()
            .contains("NodeExpandVolume.NodeExpandVolume failed"));
        op.record_event_on_error(Some(&event_err)).await;
        op.record_event_on_error(None).await;
    }

    /// `FindPluginBySpec` failing aborts generation (`:1818-1821`).
    #[test]
    fn expand_in_use_generation_fails_without_a_plugin() {
        let storage = StorageBackend::new_memory();
        let recorder = EventRecorder::new(Arc::new(StorageBackend::new_memory()));
        let empty = VolumePluginMgr::new(vec![]);
        let og = OperationGenerator {
            volume_plugin_mgr: &empty,
            kube_client: &storage,
            recorder: &recorder,
        };
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let err = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .err()
            .expect("no plugin");
        assert!(err
            .to_string()
            .contains("NodeExpandVolume.FindPluginBySpec failed"));
    }

    // ---- `MounterArgs` population (`operation_generator.go:582-589`, #2900) ----

    use crate::volume_plugins::plugin::{Mounter, Unmounter, VolumePlugin};
    use crate::volume_plugins::util::selinux::FakeSELinuxLabelTranslator;
    use async_trait::async_trait;
    use rusternetes_common::feature_gates::{with_feature, Feature};

    fn pod_json(v: serde_json::Value) -> Pod {
        serde_json::from_value(v).unwrap()
    }

    fn empty_dir_mgr() -> VolumePluginMgr {
        let host = Arc::new(KubeletVolumeHost::new(
            "/var/lib/rusternetes".to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        ));
        VolumePluginMgr::new(vec![Box::new(
            crate::volume_plugins::empty_dir::EmptyDirPlugin::new(host),
        )])
    }

    fn args_of(pod: &Pod, vol: serde_json::Value) -> crate::volume_plugins::MounterArgs {
        let v: Volume = serde_json::from_value(vol).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        mounter_args_for(&empty_dir_mgr(), &spec, pod, &FakeSELinuxLabelTranslator).unwrap()
    }

    fn pod_with_limit(limit: Option<&str>) -> Pod {
        let mut c = json!({"name": "c", "image": "i"});
        if let Some(l) = limit {
            c["resources"] = json!({"limits": {"ephemeral-storage": l}});
        }
        pod_json(json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "u"},
            "spec": {"containers": [c]}
        }))
    }

    /// `AddPodToVolume` (`desired_state_of_world.go:297-322`): the emptyDir
    /// `sizeLimit` is the desired size limit.
    #[test]
    fn empty_dir_size_limit_is_the_desired_size() {
        let a = args_of(
            &pod_with_limit(None),
            json!({"name": "v", "emptyDir": {"sizeLimit": "1Gi"}}),
        );
        assert_eq!(a.desired_size, Some(1 << 30));
    }

    /// The smaller of the pod's ephemeral-storage limit and the sizeLimit wins.
    #[test]
    fn pod_ephemeral_storage_limit_wins_when_smaller() {
        let a = args_of(
            &pod_with_limit(Some("500Mi")),
            json!({"name": "v", "emptyDir": {"sizeLimit": "1Gi"}}),
        );
        assert_eq!(a.desired_size, Some(500 << 20));
        let a = args_of(
            &pod_with_limit(Some("2Gi")),
            json!({"name": "v", "emptyDir": {"sizeLimit": "1Gi"}}),
        );
        assert_eq!(a.desired_size, Some(1 << 30));
    }

    /// Not a local ephemeral volume: no desired size limit (`:315-316`).
    #[test]
    fn non_local_ephemeral_volume_has_no_desired_size() {
        let a = args_of(
            &pod_with_limit(Some("500Mi")),
            json!({"name": "v", "emptyDir": {"medium": "Memory"}}),
        );
        assert_eq!(a.desired_size, None);
    }

    struct SelinuxPlugin(bool);

    #[async_trait]
    impl VolumePlugin for SelinuxPlugin {
        fn name(&self) -> &'static str {
            "fake-selinux"
        }
        fn get_volume_name(&self, spec: &Spec<'_>) -> anyhow::Result<String> {
            Ok(spec.name().to_string())
        }
        fn can_support(&self, _spec: &Spec<'_>) -> bool {
            true
        }
        fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
            false
        }
        fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> anyhow::Result<bool> {
            Ok(self.0)
        }
        async fn new_mounter(&self, _: &Spec<'_>, _: &Pod) -> anyhow::Result<Box<dyn Mounter>> {
            unimplemented!()
        }
        fn new_unmounter(&self, _: &str, _: &str) -> anyhow::Result<Box<dyn Unmounter>> {
            unimplemented!()
        }
        fn construct_volume_spec(
            &self,
            _: &str,
            _: &str,
        ) -> anyhow::Result<crate::volume_plugins::ReconstructedVolume> {
            unimplemented!()
        }
    }

    fn selinux_args(
        plugin_supports: bool,
        access_mode: &str,
    ) -> crate::volume_plugins::MounterArgs {
        let _g = with_feature(Feature::SELinuxMountReadWriteOncePod, true);
        let mgr = VolumePluginMgr::new(vec![Box::new(SelinuxPlugin(plugin_supports))]);
        let pod = pod_json(json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "u"},
            "spec": {
                "containers": [{
                    "name": "c", "image": "i",
                    "volumeMounts": [{"name": "v", "mountPath": "/v"}],
                    "securityContext": {"seLinuxOptions": {
                        "user": "system_u", "role": "object_r",
                        "type": "container_t", "level": "s0:c1,c2"}}
                }],
                "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": "c"}}]
            }
        }));
        let v = pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv"}, "spec": {"accessModes": [access_mode]}
        }))
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        mounter_args_for(&mgr, &spec, &pod, &FakeSELinuxLabelTranslator).unwrap()
    }

    /// `AddPodToVolume` -> `getSELinuxLabel` -> `GetMountSELinuxLabel`: an RWOP
    /// volume on a plugin that supports the context mount carries the
    /// containers' file label into `MounterArgs.SELinuxLabel`.
    #[test]
    #[serial_test::serial]
    fn rwop_volume_gets_the_container_selinux_label() {
        let a = selinux_args(true, "ReadWriteOncePod");
        assert_eq!(
            a.selinux_label,
            "system_u:object_r:container_file_t:s0:c1,c2"
        );
    }

    /// A plugin without SELinux context-mount support gets no label.
    #[test]
    #[serial_test::serial]
    fn plugin_without_selinux_mount_gets_no_label() {
        assert_eq!(selinux_args(false, "ReadWriteOncePod").selinux_label, "");
    }
}
