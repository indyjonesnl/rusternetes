//! Port of the block-volume operations of
//! `pkg/volume/util/operationexecutor/operation_generator.go`
//! (`GenerateMapVolumeFunc`, `GenerateUnmapVolumeFunc`,
//! `GenerateUnmapDeviceFunc`) and of `util.MapBlockVolume` /
//! `util.UnmapBlockVolume` (`pkg/volume/util/util.go:426-492`).
//!
//! A block volume is not mounted: the host device is linked under the global
//! map path and the pod device map path, and the kubelet later hands it to the
//! runtime as a CRI `Device`. Each operation returns the
//! [`OperationContext`] its upstream `OperationFunc` returns; an `Err` is the
//! error upstream's generator returns before the function exists (plugin not
//! found, `NewBlockVolumeMapper` failed).
//!
//! Not ported, as for the filesystem `MountVolume`: the node-affinity check
//! (`checkNodeAffinity`), the success/failure events and the post-map
//! `expandVolumeDuringMount`.

use crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
use crate::volume_plugins::plugin::BlockVolumeMapper;
use crate::volume_plugins::registry::VolumePluginMgr;
use crate::volume_plugins::util::hostutil::HostUtils;
use crate::volume_plugins::util::operation_executor::{
    AttachedVolume, DeviceMountState, MarkVolumeOpts, MountedVolume, VolumeMountState,
    VolumeToMount,
};
use crate::volume_plugins::util::operation_generator::{
    is_device_opened, mark_device_error_state, mark_volume_error_state, MountState,
};
use crate::volume_plugins::util::types::{OperationContext, VolumeOperationError};
use crate::volume_plugins::util::volumepathhandler::VolumePathHandler;
use anyhow::{anyhow, Error, Result};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Port of `volumepathhandler.BlockVolumePathHandler`
/// (`volume_path_handler.go:36-62`), the interface the operations take so a
/// test can stand in for the host.
pub trait BlockVolumePathHandler: Send + Sync {
    fn map_device(
        &self,
        device_path: &str,
        map_path: &str,
        link_name: &str,
        bind_mount: bool,
    ) -> Result<()>;
    fn unmap_device(&self, map_path: &str, link_name: &str, bind_mount: bool) -> Result<()>;
    fn remove_map_path(&self, map_path: &str) -> Result<()>;
    fn attach_file_device(&self, path: &str) -> Result<String>;
    fn detach_file_device(&self, path: &str) -> Result<()>;
    fn get_device_bind_mount_refs(&self, dev_path: &str, map_path: &str) -> Result<Vec<String>>;
}

impl BlockVolumePathHandler for VolumePathHandler {
    fn map_device(
        &self,
        device_path: &str,
        map_path: &str,
        link_name: &str,
        bind_mount: bool,
    ) -> Result<()> {
        VolumePathHandler::map_device(self, device_path, map_path, link_name, bind_mount)
    }
    fn unmap_device(&self, map_path: &str, link_name: &str, bind_mount: bool) -> Result<()> {
        VolumePathHandler::unmap_device(self, map_path, link_name, bind_mount)
    }
    fn remove_map_path(&self, map_path: &str) -> Result<()> {
        VolumePathHandler::remove_map_path(self, map_path)
    }
    fn attach_file_device(&self, path: &str) -> Result<String> {
        VolumePathHandler::attach_file_device(self, path)
    }
    fn detach_file_device(&self, path: &str) -> Result<()> {
        VolumePathHandler::detach_file_device(self, path)
    }
    fn get_device_bind_mount_refs(&self, dev_path: &str, map_path: &str) -> Result<Vec<String>> {
        VolumePathHandler::get_device_bind_mount_refs(self, dev_path, map_path)
    }
}

/// `waitForAttachTimeout` (`volume_manager.go:88`), as the filesystem mount uses it.
const WAIT_FOR_ATTACH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// Port of `util.MapBlockVolume` (`util.go:426-459`).
pub fn map_block_volume(
    blk_util: &dyn BlockVolumePathHandler,
    device_path: &str,
    global_map_path: &str,
    pod_volume_map_path: &str,
    volume_map_name: &str,
    pod_uid: &str,
) -> Result<()> {
    // map devicePath to global node path as bind mount
    blk_util
        .map_device(device_path, global_map_path, pod_uid, true)
        .map_err(|e| {
            anyhow!(
                "blkUtil.MapDevice failed. devicePath: {device_path}, globalMapPath:{global_map_path}, podUID: {pod_uid}, bindMount: true: {e:#}"
            )
        })?;

    // map devicePath to pod volume path
    blk_util
        .map_device(device_path, pod_volume_map_path, volume_map_name, false)
        .map_err(|e| {
            anyhow!(
                "blkUtil.MapDevice failed. devicePath: {device_path}, podVolumeMapPath:{pod_volume_map_path}, volumeMapName: {volume_map_name}, bindMount: false: {e:#}"
            )
        })?;

    // Take file descriptor lock to keep a block device opened. Otherwise, there
    // is a case that the block device is silently removed and attached another
    // device with the same name. Container runtime can't handle this problem.
    // To avoid unexpected condition fd lock for the block device is required.
    let global_pod_path = Path::new(global_map_path).join(pod_uid);
    blk_util
        .attach_file_device(&global_pod_path.to_string_lossy())
        .map_err(|e| {
            anyhow!(
                "blkUtil.AttachFileDevice failed. globalMapPath:{global_map_path}, podUID: {pod_uid}: {e:#}"
            )
        })?;
    Ok(())
}

/// Port of `util.UnmapBlockVolume` (`util.go:464-492`).
pub fn unmap_block_volume(
    blk_util: &dyn BlockVolumePathHandler,
    global_unmap_path: &str,
    pod_device_unmap_path: &str,
    volume_map_name: &str,
    pod_uid: &str,
) -> Result<()> {
    // Release file descriptor lock.
    let global_pod_path = Path::new(global_unmap_path).join(pod_uid);
    blk_util
        .detach_file_device(&global_pod_path.to_string_lossy())
        .map_err(|e| {
            anyhow!(
                "blkUtil.DetachFileDevice failed. globalUnmapPath:{global_unmap_path}, podUID: {pod_uid}: {e:#}"
            )
        })?;

    // unmap devicePath from pod volume path
    blk_util
        .unmap_device(pod_device_unmap_path, volume_map_name, false)
        .map_err(|e| {
            anyhow!(
                "blkUtil.DetachFileDevice failed. podDeviceUnmapPath:{pod_device_unmap_path}, volumeMapName: {volume_map_name}, bindMount: false: {e:#}"
            )
        })?;

    // unmap devicePath from global node path
    blk_util
        .unmap_device(global_unmap_path, pod_uid, true)
        .map_err(|e| {
            anyhow!(
                "blkUtil.DetachFileDevice failed. globalUnmapPath:{global_unmap_path}, podUID: {pod_uid}, bindMount: true: {e:#}"
            )
        })?;
    Ok(())
}

/// The error of an operation: the simple and detailed errors are one here.
fn fail(v: &VolumeToMount, prefix: &str, e: &Error) -> OperationContext {
    let (event_err, detailed_err) = v.generate_error(prefix, e);
    OperationContext::new(Some(event_err), Some(detailed_err), false)
}

/// Port of `GenerateMapVolumeFunc` (`operation_generator.go:924-1160`) and the
/// `mapVolumeFunc` it returns.
pub async fn map_volume(
    mgr: &VolumePluginMgr,
    volume_to_mount: &VolumeToMount,
    actual_state_of_world: &ActualStateOfWorld,
) -> Result<OperationContext> {
    map_volume_with(
        mgr,
        volume_to_mount,
        actual_state_of_world,
        &VolumePathHandler,
    )
    .await
}

/// [`map_volume`] over the given `blkUtil`.
pub async fn map_volume_with(
    mgr: &VolumePluginMgr,
    volume_to_mount: &VolumeToMount,
    actual_state_of_world: &ActualStateOfWorld,
    blk_util: &dyn BlockVolumePathHandler,
) -> Result<OperationContext> {
    let v = volume_to_mount;
    let asw = actual_state_of_world;
    let spec = v.volume_spec.as_spec();

    // Get block volume mapper plugin
    let block_volume_plugin = mgr
        .find_mapper_plugin_by_spec(&spec)
        .map_err(|e| {
            v.generate_error_detailed("MapVolume.FindMapperPluginBySpec failed", &anyhow!(e))
        })?
        .ok_or_else(|| {
            anyhow!(v.generate_msg_detailed(
                "MapVolume.FindMapperPluginBySpec failed to find BlockVolumeMapper plugin. Volume plugin is nil.",
                ""
            ))
        })?;
    let block_volume_mapper: Arc<dyn BlockVolumeMapper> = Arc::from(
        block_volume_plugin
            .new_block_volume_mapper(&spec, &v.pod)
            .map_err(|e| {
                v.generate_error("MapVolume.NewBlockVolumeMapper initialization failed", &e)
                    .1
            })?,
    );

    // Get attacher, if possible
    let volume_attacher = mgr
        .find_attachable_plugin_by_spec(&spec)
        .and_then(|p| p.as_attachable_plugin())
        .and_then(|ap| ap.new_attacher().ok());

    let mut device_path = String::new();
    let mut staging_path = String::new();

    // Enforce ReadWriteOncePod access mode. This is also enforced during scheduling.
    if asw.is_volume_mounted_elsewhere(&v.volume_name, &v.pod_name) {
        if let Some(pv) = v.volume_spec.persistent_volume.as_ref() {
            // Because we do not know what access mode the pod intends to use if there are multiple.
            let modes = &pv.spec.access_modes;
            if modes.len() == 1
                && modes[0]
                    == rusternetes_common::resources::PersistentVolumeAccessMode::ReadWriteOncePod
            {
                return Ok(fail(
                    v,
                    "MapVolume.SetUpDevice failed",
                    &anyhow!("volume uses the ReadWriteOncePod access mode and is already in use by another pod"),
                ));
            }
        }
    }

    // Set up global map path under the given plugin directory using symbolic link
    let global_map_path = match block_volume_mapper.get_global_map_path(&spec) {
        Ok(p) => p,
        // On failure, return error. Caller will log and retry.
        Err(e) => return Ok(fail(v, "MapVolume.GetGlobalMapPath failed", &e)),
    };
    if let Some(volume_attacher) = &volume_attacher {
        // Wait for attachable volumes to finish attaching
        info!(
            "MapVolume.WaitForAttach entering for volume {} (DevicePath {:?})",
            v.volume_name, v.device_path
        );
        device_path = match volume_attacher
            .wait_for_attach(&spec, &v.device_path, &v.pod, WAIT_FOR_ATTACH_TIMEOUT)
            .await
        {
            Ok(p) => p,
            Err(e) => return Ok(fail(v, "MapVolume.WaitForAttach failed", &e)),
        };
        info!(
            "MapVolume.WaitForAttach succeeded for volume {} (DevicePath {device_path:?})",
            v.volume_name
        );
    }

    let state = MountState {
        volume_to_mount: v,
        actual_state_of_world: asw,
    };

    // Call SetUpDevice if blockVolumeMapper implements CustomBlockVolumeMapper
    let custom = block_volume_mapper.as_custom_block_volume_mapper();
    if let Some(custom) = custom {
        if asw.get_device_mount_state(&v.volume_name) != DeviceMountState::DeviceGloballyMounted {
            match custom.set_up_device().await {
                Ok(p) => staging_path = p,
                Err(map_err) => {
                    mark_device_error_state(&state, &device_path, &global_map_path, &map_err);
                    // On failure, return error. Caller will log and retry.
                    return Ok(fail(v, "MapVolume.SetUpDevice failed", &map_err));
                }
            }
        }
    }
    let _ = &staging_path;

    // Update actual state of world to reflect volume is globally mounted
    let marked_device_path = device_path.clone();
    if let Err(e) =
        asw.mark_device_as_mounted(&v.volume_name, &marked_device_path, &global_map_path, "")
    {
        return Ok(fail(v, "MapVolume.MarkDeviceAsMounted failed", &anyhow!(e)));
    }

    let mark_volume_opts = MarkVolumeOpts {
        pod_name: v.pod_name.clone(),
        pod_uid: v.pod.metadata.uid.clone(),
        volume_name: v.volume_name.clone(),
        mounter: None,
        block_volume_mapper: Some(block_volume_mapper.clone()),
        volume_gid_volume: v.volume_gid_value.clone(),
        volume_spec: v.volume_spec.clone(),
        volume_mount_state: VolumeMountState::VolumeMounted,
        selinux_mount_context: String::new(),
    };

    // Call MapPodDevice if blockVolumeMapper implements CustomBlockVolumeMapper
    let mut context = None;
    if let Some(custom) = custom {
        // Execute driver specific map
        let plugin_device_path = match custom.map_pod_device().await {
            Ok(p) => p,
            Err(map_err) => {
                // On failure, return error. Caller will log and retry.
                mark_volume_error_state(&state, &mark_volume_opts, &map_err);
                return Ok(fail(v, "MapVolume.MapPodDevice failed", &map_err));
            }
        };
        // if pluginDevicePath is provided, assume attacher may not provide
        // device or attachment flow uses SetupDevice to get device path
        if !plugin_device_path.is_empty() {
            device_path = plugin_device_path;
        }
        if device_path.is_empty() {
            context = Some(fail(
                v,
                "MapVolume failed",
                &anyhow!("device path of the volume is empty"),
            ));
        }
    }

    // From now on, the volume is mapped. Mark it as uncertain on error, so it
    // is unmapped when corresponding pod is deleted (the `defer` at `:1048`).
    let mapped = custom.is_some();
    let result = match context {
        Some(ctx) => ctx,
        None => {
            map_volume_common(
                v,
                asw,
                &block_volume_mapper,
                &mark_volume_opts,
                &global_map_path,
                &marked_device_path,
                device_path,
                blk_util,
            )
            .await
        }
    };
    if mapped {
        if let Some(event_err) = result.event_err.as_ref() {
            mark_volume_error_state(
                &state,
                &mark_volume_opts,
                &Error::new(VolumeOperationError::UncertainProgress(
                    event_err.to_string(),
                )),
            );
        }
    }
    Ok(result)
}

/// The part of `mapVolumeFunc` after the plugin-specific steps (`:1066-1160`).
#[allow(clippy::too_many_arguments)] // the closure upstream captures all of these
async fn map_volume_common(
    v: &VolumeToMount,
    asw: &ActualStateOfWorld,
    block_volume_mapper: &Arc<dyn BlockVolumeMapper>,
    mark_volume_opts: &MarkVolumeOpts,
    global_map_path: &str,
    marked_device_path: &str,
    device_path: String,
    blk_util: &dyn BlockVolumePathHandler,
) -> OperationContext {
    // When kubelet is containerized, devicePath may be a symlink at a place
    // unavailable to kubelet, so evaluate it on the host and expect that it
    // links to a device in /dev, which will be available to containerized
    // kubelet.
    let device_path = match std::fs::canonicalize(&device_path) {
        Ok(p) => p.to_string_lossy().into_owned(),
        Err(e) => return fail(v, "MapVolume.EvalHostSymlinks failed", &anyhow!(e)),
    };

    // Update actual state of world with the devicePath again, if devicePath has
    // changed from markedDevicePath
    if marked_device_path != device_path {
        if let Err(e) =
            asw.mark_device_as_mounted(&v.volume_name, &device_path, global_map_path, "")
        {
            return fail(v, "MapVolume.MarkDeviceAsMounted failed", &anyhow!(e));
        }
    }

    // Execute common map
    let (volume_map_path, vol_name) = block_volume_mapper.get_pod_device_map_path();
    if let Err(e) = map_block_volume(
        blk_util,
        &device_path,
        global_map_path,
        &volume_map_path,
        &vol_name,
        &v.pod.metadata.uid,
    ) {
        // On failure, return error. Caller will log and retry.
        return fail(v, "MapVolume.MapBlockVolume failed", &e);
    }

    // Device mapping for global map path succeeded
    debug!(
        "{}",
        v.generate_msg_detailed(
            "MapVolume.MapPodDevice succeeded",
            &format!("globalMapPath {global_map_path:?}")
        )
    );
    info!(
        "{}",
        v.generate_msg_detailed(
            "MapVolume.MapPodDevice succeeded",
            &format!("volumeMapPath {volume_map_path:?}")
        )
    );

    if let Err(e) = asw.mark_volume_as_mounted(mark_volume_opts.clone()) {
        // On failure, just log and exit. The mount has already completed.
        error!(
            "{}",
            v.generate_error_detailed("MapVolume.MarkVolumeAsMounted failed", &anyhow!(e))
        );
    }
    OperationContext::new(None, None, false)
}

/// Port of `GenerateUnmapVolumeFunc` (`operation_generator.go:1162-1268`).
pub async fn unmap_volume(
    mgr: &VolumePluginMgr,
    volume_to_unmount: &MountedVolume,
    actual_state_of_world: &ActualStateOfWorld,
) -> Result<OperationContext> {
    unmap_volume_with(
        mgr,
        volume_to_unmount,
        actual_state_of_world,
        &VolumePathHandler,
    )
    .await
}

/// [`unmap_volume`] over the given `blkUtil`.
pub async fn unmap_volume_with(
    mgr: &VolumePluginMgr,
    volume_to_unmount: &MountedVolume,
    actual_state_of_world: &ActualStateOfWorld,
    blk_util: &dyn BlockVolumePathHandler,
) -> Result<OperationContext> {
    let v = volume_to_unmount;
    let asw = actual_state_of_world;
    let ident = format!("volume {} pod {}", v.volume_name, v.pod_name);

    // Get block volume unmapper plugin
    let block_volume_plugin = mgr
        .find_mapper_plugin_by_name(&v.plugin_name)
        .map_err(|e| anyhow!("UnmapVolume.FindMapperPluginByName failed for {ident}: {e}"))?
        .ok_or_else(|| {
            anyhow!("UnmapVolume.FindMapperPluginByName failed to find BlockVolumeMapper plugin. Volume plugin is nil. {ident}")
        })?;
    let block_volume_unmapper = block_volume_plugin
        .new_block_volume_unmapper(&v.inner_volume_spec_name, &v.pod_uid)
        .map_err(|e| anyhow!("UnmapVolume.NewUnmapper failed for {ident}: {e:#}"))?;

    let failed = |prefix: &str, e: &Error| {
        let err = anyhow!("{prefix} for {ident}: {e:#}");
        OperationContext::new(Some(anyhow!("{err}")), Some(err), false)
    };

    // pods/{podUid}/volumeDevices/{escapeQualifiedPluginName}/{volumeName}
    let (pod_device_unmap_path, vol_name) = block_volume_unmapper.get_pod_device_map_path();
    // plugins/kubernetes.io/{PluginName}/volumeDevices/{volumePluginDependentPath}/{podUID}
    let global_unmap_path =
        match block_volume_unmapper.get_global_map_path(&v.volume_spec.as_spec()) {
            Ok(p) => p,
            // On failure, return error. Caller will log and retry.
            Err(e) => return Ok(failed("UnmapVolume.GetGlobalMapPath failed", &e)),
        };

    // Mark the device as uncertain to make sure kubelet calls UnmapDevice again
    // in all the "return err" cases below. The volume is marked as fully
    // un-mapped at the end of this function, when everything succeeds.
    let mark_volume_opts = MarkVolumeOpts {
        pod_name: v.pod_name.clone(),
        pod_uid: v.pod_uid.clone(),
        volume_name: v.volume_name.clone(),
        mounter: None,
        block_volume_mapper: None,
        volume_gid_volume: v.volume_gid_value.clone(),
        volume_spec: v.volume_spec.clone(),
        volume_mount_state: VolumeMountState::VolumeMountUncertain,
        selinux_mount_context: String::new(),
    };
    if let Err(e) = asw.mark_volume_mount_as_uncertain(mark_volume_opts) {
        return Ok(failed(
            "UnmapVolume.MarkDeviceAsUncertain failed",
            &anyhow!(e),
        ));
    }

    // Execute common unmap
    if let Err(e) = unmap_block_volume(
        blk_util,
        &global_unmap_path,
        &pod_device_unmap_path,
        &vol_name,
        &v.pod_uid,
    ) {
        return Ok(failed("UnmapVolume.UnmapBlockVolume failed", &e));
    }

    // Call UnmapPodDevice if blockVolumeUnmapper implements CustomBlockVolumeUnmapper
    if let Some(custom) = block_volume_unmapper.as_custom_block_volume_unmapper() {
        // Execute plugin specific unmap
        if let Err(e) = custom.unmap_pod_device().await {
            return Ok(failed("UnmapVolume.UnmapPodDevice failed", &e));
        }
    }

    info!(
        "UnmapVolume succeeded for volume {} pod {} (UID: {}). InnerVolumeSpecName {}. PluginName {}",
        v.volume_name, v.pod_name, v.pod_uid, v.inner_volume_spec_name, v.plugin_name
    );

    // Update actual state of world
    if let Err(e) = asw.mark_volume_as_unmounted(&v.pod_name, &v.volume_name) {
        // On failure, just log and exit
        error!("UnmapVolume.MarkVolumeAsUnmounted failed for {ident}: {e}");
    }
    Ok(OperationContext::new(None, None, false))
}

/// Port of `GenerateUnmapDeviceFunc` (`operation_generator.go:1270-1380`).
pub async fn unmap_device(
    mgr: &VolumePluginMgr,
    device_to_detach: &AttachedVolume,
    actual_state_of_world: &ActualStateOfWorld,
    host_util: &dyn HostUtils,
) -> Result<OperationContext> {
    unmap_device_with(
        mgr,
        device_to_detach,
        actual_state_of_world,
        host_util,
        &VolumePathHandler,
    )
    .await
}

/// [`unmap_device`] over the given `blkUtil`.
pub async fn unmap_device_with(
    mgr: &VolumePluginMgr,
    device_to_detach: &AttachedVolume,
    actual_state_of_world: &ActualStateOfWorld,
    host_util: &dyn HostUtils,
    blk_util: &dyn BlockVolumePathHandler,
) -> Result<OperationContext> {
    let d = device_to_detach;
    let asw = actual_state_of_world;

    let block_volume_plugin = mgr
        .find_mapper_plugin_by_name(&d.plugin_name)
        .map_err(|e| {
            d.generate_error_detailed(
                "UnmapDevice.FindMapperPluginByName failed",
                Some(&anyhow!(e)),
            )
        })?
        .ok_or_else(|| {
            d.generate_error_detailed(
                "UnmapDevice.FindMapperPluginByName failed to find BlockVolumeMapper plugin. Volume plugin is nil.",
                None,
            )
        })?;
    let block_volume_unmapper = block_volume_plugin
        .new_block_volume_unmapper(d.volume_spec.name(), "")
        .map_err(|e| d.generate_error_detailed("UnmapDevice.NewUnmapper failed", Some(&e)))?;

    let failed = |prefix: &str, e: &Error| {
        let (event_err, detailed_err) = d.generate_error(prefix, e);
        OperationContext::new(Some(event_err), Some(detailed_err), false)
    };

    // Search under globalMapPath dir if all symbolic links from pods have been
    // removed already. If symbolic links are there, pods may still refer the
    // volume.
    let global_map_path = d.device_mount_path.clone();
    let refs = match blk_util.get_device_bind_mount_refs(&d.device_path, &global_map_path) {
        Ok(refs) => refs,
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
        {
            // Looks like SetupDevice did not complete. Fall through to
            // TearDownDevice and mark the device as unmounted.
            Vec::new()
        }
        Err(e) => {
            return Ok(failed(
                "UnmapDevice.GetDeviceBindMountRefs check failed",
                &e,
            ))
        }
    };
    if !refs.is_empty() {
        return Ok(failed(
            "UnmapDevice failed",
            &anyhow!("the device {global_map_path:?} is still referenced from other Pods {refs:?}"),
        ));
    }

    // Mark the device as uncertain to make sure kubelet calls UnmapDevice again
    // in all the "return err" cases below.
    if let Err(e) =
        asw.mark_device_as_uncertain(&d.volume_name, &d.device_path, &global_map_path, "")
    {
        return Ok(failed(
            "UnmapDevice.MarkDeviceAsUncertain failed",
            &anyhow!(e),
        ));
    }

    // Call TearDownDevice if blockVolumeUnmapper implements CustomBlockVolumeUnmapper
    if let Some(custom) = block_volume_unmapper.as_custom_block_volume_unmapper() {
        if let Err(e) = custom
            .tear_down_device(&global_map_path, &d.device_path)
            .await
        {
            return Ok(failed("UnmapDevice.TearDownDevice failed", &e));
        }
    }

    // Plugin finished TearDownDevice(). Now globalMapPath dir and plugin's
    // stored data on the dir are unnecessary, clean up it.
    if let Err(e) = blk_util.remove_map_path(&global_map_path) {
        return Ok(failed("UnmapDevice.RemoveMapPath failed", &e));
    }

    // Before logging that UnmapDevice succeeded and moving on, check if the
    // path is a device, and if so whether the device is in use elsewhere.
    // Retry if it is.
    match is_device_opened(d, host_util) {
        Err(e) => return Ok(OperationContext::new(None, Some(e), false)),
        // The device is still in use elsewhere. Caller will log and retry.
        Ok(true) => {
            return Ok(failed(
                "UnmapDevice failed",
                &anyhow!("the device is in use when it was no longer expected to be in use"),
            ))
        }
        Ok(false) => {}
    }

    info!("{}", d.generate_msg_detailed("UnmapDevice succeeded", ""));

    // Update actual state of world
    if let Err(e) = asw.mark_device_as_unmounted(&d.volume_name) {
        // On failure, return error. Caller will log and retry.
        return Ok(failed("MarkDeviceAsUnmounted failed", &anyhow!(e)));
    }
    Ok(OperationContext::new(None, None, false))
}
