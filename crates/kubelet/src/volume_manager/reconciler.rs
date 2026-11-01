//! Port of `pkg/kubelet/volumemanager/reconciler` (`reconciler.go`,
//! `reconciler_common.go`) — the loop that drives the actual state of the
//! world towards the desired one.
//!
//! `reconcile` runs `unmountVolumes`, `mountOrAttachVolumes`,
//! `unmountDetachDevices` and `cleanOrphanVolumes`, the unmounts only once the
//! desired state is populated (`readyToUnmount`). Remount: a volume the desired
//! state wants, that the actual state has mounted for the pod, and whose plugin
//! asked for a remount (`RequiresRemount`: secret, configMap, projected,
//! downwardAPI), is mounted again through the plugin's `SetUp`.

use crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorldError;
use crate::volume_manager::cache::{ActualStateOfWorld, DesiredStateOfWorld};
use crate::volume_plugins::util::operation_executor::VolumeToMount;
use crate::volumes::VolumeManager as VolumeMounter;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};

/// `reconcilerLoopSleepPeriod` (`volume_manager.go`).
pub const RECONCILER_LOOP_SLEEP_PERIOD: Duration = Duration::from_millis(100);

/// Port of `reconciler` (`reconciler_common.go:44-75`).
pub struct Reconciler {
    desired_state_of_world: Arc<DesiredStateOfWorld>,
    actual_state_of_world: Arc<ActualStateOfWorld>,
    /// Runs the mount as `operationExecutor.MountVolume` does.
    mounter: Arc<VolumeMounter>,
    loop_sleep_duration: Duration,
    /// `populatorHasAddedPods`: whether the desired state is fully populated.
    populator_has_added_pods: Box<dyn Fn() -> bool + Send + Sync>,
    /// `volumesFailedReconstruction`: volumes found on disk that could not be
    /// reconstructed, force-unmounted once nothing desires them
    /// (`reconstruct.rs`).
    pub(crate) volumes_failed_reconstruction: std::sync::Mutex<Vec<crate::pod_dirs::PodVolume>>,
    /// `volumesNeedUpdateFromNodeStatus`: reconstructed volumes whose
    /// devicePath is still to be read from the node's `volumesAttached`.
    pub(crate) volumes_need_update_from_node_status:
        std::sync::Mutex<Vec<crate::volume_plugins::util::types::UniqueVolumeName>>,
}

impl Reconciler {
    /// Port of `NewReconciler` (`reconciler.go:35-68`).
    pub fn new(
        desired_state_of_world: Arc<DesiredStateOfWorld>,
        actual_state_of_world: Arc<ActualStateOfWorld>,
        mounter: Arc<VolumeMounter>,
        loop_sleep_duration: Duration,
        populator_has_added_pods: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            desired_state_of_world,
            actual_state_of_world,
            mounter,
            loop_sleep_duration,
            populator_has_added_pods: Box::new(populator_has_added_pods),
            volumes_failed_reconstruction: std::sync::Mutex::new(Vec::new()),
            volumes_need_update_from_node_status: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn actual_state_of_world(&self) -> &ActualStateOfWorld {
        &self.actual_state_of_world
    }

    pub(crate) fn desired_state_of_world(&self) -> &DesiredStateOfWorld {
        &self.desired_state_of_world
    }

    pub(crate) fn mounter(&self) -> &VolumeMounter {
        &self.mounter
    }

    pub(crate) fn plugin_mgr(&self) -> &crate::volume_plugins::VolumePluginMgr {
        &self.mounter.plugin_mgr
    }

    /// `kubeletPodsDir`.
    pub(crate) fn kubelet_pods_dir(&self) -> &str {
        &self.mounter.volumes_base_path
    }

    /// `nodeName`.
    pub(crate) fn node_name(&self) -> &str {
        &self.mounter.node_name
    }

    /// Port of `Run` (`reconciler.go:70-72`): `wait.Until(rc.reconcile, ...)`.
    /// Each operation runs in its own task, as upstream's operation executor
    /// runs each in its own goroutine, so a slow mount does not hold up the
    /// others; `isOperationPending` keeps a volume's operations apart.
    pub async fn run(&self) {
        // `rc.reconstructVolumes()` before the loop (`reconciler.go:70-72`).
        self.reconstruct_volumes().await;
        loop {
            for operation in self.reconcile_operations().await {
                tokio::spawn(operation);
            }
            tokio::time::sleep(self.loop_sleep_duration).await;
        }
    }

    /// Port of `reconcile` (`reconciler.go:33-70`), running the operations it
    /// starts to completion.
    pub async fn reconcile(&self) {
        futures::future::join_all(self.reconcile_operations().await).await;
    }

    /// The operations one `reconcile` pass starts.
    async fn reconcile_operations(&self) -> Vec<Operation> {
        let ready_to_unmount = self.ready_to_unmount();
        let mut operations: Vec<Operation> = Vec::new();
        if ready_to_unmount {
            // Unmounts are triggered before mounts so that a volume that was
            // referenced by a pod that was deleted and is now referenced by
            // another pod is unmounted from the first pod before being mounted
            // to the new pod.
            operations.extend(self.unmount_volumes());
        }

        // Next we mount required volumes. If underlying PVC was resized while
        // in-use then this also handles volume resizing.
        operations.extend(self.mount_or_attach_volumes());

        // Unmount volumes only when DSW and ASW are fully populated to prevent
        // unmounting a volume that is still needed, but it did not reach DSW
        // yet.
        if ready_to_unmount {
            // Ensure devices that should be detached/unmounted are
            // detached/unmounted.
            operations.extend(self.unmount_detach_devices());

            // Clean up any orphan volumes that failed reconstruction.
            self.clean_orphan_volumes().await;
        }

        if self.needs_update_from_node_status() {
            self.update_reconstructed_from_node_status().await;
        }
        operations
    }

    /// Port of `readyToUnmount` (`reconstruct.go:30-44`). During kubelet
    /// startup, all volumes present on disk are added as uncertain to the
    /// actual state; unmount only when the desired state is fully populated,
    /// to prevent unmounting volumes that did not reach it yet, and only once
    /// the device paths were corrected from node.status, to prevent calling
    /// unmount with a wrong devicePath.
    pub fn ready_to_unmount(&self) -> bool {
        if !(self.populator_has_added_pods)() {
            return false;
        }
        !self.needs_update_from_node_status()
    }

    /// Port of `unmountVolumes` (`reconciler_common.go:148-167`): the unmount
    /// of every volume the actual state has mounted for a pod that the desired
    /// state no longer wants it mounted for.
    pub fn unmount_volumes(&self) -> Vec<Operation> {
        let mut operations: Vec<Operation> = Vec::new();
        // Ensure volumes that should be unmounted are unmounted.
        for mounted_volume in self.actual_state_of_world.get_all_mounted_volumes() {
            if self
                .mounter
                .volume_pod_operation_pending(&mounted_volume.volume_name, &mounted_volume.pod_name)
            {
                continue;
            }
            if !self.desired_state_of_world.pod_exists_in_volume(
                &mounted_volume.pod_name,
                &mounted_volume.volume_name,
                &mounted_volume.selinux_mount_context,
            ) {
                // Volume is mounted, unmount it
                debug!(
                    "Starting operationExecutor.UnmountVolume for volume {} pod {}",
                    mounted_volume.volume_name, mounted_volume.pod_name
                );
                let mounter = self.mounter.clone();
                operations.push(Box::pin(async move {
                    mounter.unmount_volume(&mounted_volume).await;
                }));
            }
        }
        operations
    }

    /// Port of `unmountDetachDevices` (`reconciler_common.go:273-315`): the
    /// device unmount of every volume nothing is mounted to and the desired
    /// state does not want, and "detach" (the kubelet never detaches:
    /// `controllerAttachDetachEnabled || !PluginIsAttachable`) of those whose
    /// device is not mounted.
    pub fn unmount_detach_devices(&self) -> Vec<Operation> {
        let mut operations: Vec<Operation> = Vec::new();
        for attached_volume in self.actual_state_of_world.get_unmounted_volumes() {
            // Check IsOperationPending to avoid marking a volume as detached
            // if it's in the process of mounting.
            if self.desired_state_of_world.volume_exists(
                &attached_volume.volume_name,
                &attached_volume.selinux_mount_context,
            ) || self
                .mounter
                .volume_operation_pending(&attached_volume.volume_name)
            {
                continue;
            }
            // Re-read the actual state of the world, maybe the volume got
            // mounted in the meantime. This is safe, because there is no
            // pending operation (checked above) and no new operation could
            // start in the meantime.
            let Some(attached_volume) = self
                .actual_state_of_world
                .get_attached_volume(&attached_volume.volume_name)
            else {
                continue;
            };
            if attached_volume.device_may_be_mounted() {
                // Volume is globally mounted to device, unmount it
                let mounter = self.mounter.clone();
                operations.push(Box::pin(async move {
                    mounter.unmount_device(&attached_volume).await;
                }));
            } else {
                // Volume is attached to node, detach it. Kubelet not
                // responsible for detaching or this volume has a non-attachable
                // volume plugin.
                self.actual_state_of_world.mark_volume_as_detached(
                    &attached_volume.volume_name,
                    &attached_volume.node_name,
                );
                debug!("Volume detached: {}", attached_volume.volume_name);
            }
        }
        operations
    }

    /// Port of `mountOrAttachVolumes` (`reconciler_common.go:132-190`): the
    /// operations to start, one per volume that needs one.
    pub fn mount_or_attach_volumes(&self) -> Vec<Operation> {
        let mut operations: Vec<Operation> = Vec::new();
        for volume_to_mount in self.desired_state_of_world.get_volumes_to_mount() {
            let mut v = volume_to_mount.0;
            // `rc.operationExecutor.IsOperationPending(volumeName, "", "")`
            if self.mounter.volume_operation_pending(&v.volume_name) {
                continue;
            }
            let (vol_mounted, device_path, pod_exist_error) =
                self.actual_state_of_world.pod_exists_in_volume(
                    &v.pod_name,
                    &v.volume_name,
                    v.desired_persistent_volume_size,
                    &v.selinux_label,
                );
            v.device_path = device_path;
            let mounter = self.mounter.clone();
            match pod_exist_error {
                // The volume is mounted, but with an unexpected SELinux
                // context: it is unmounted by the unmount arms, then removed
                // from the actual state (`:149-155`).
                Some(err @ ActualStateOfWorldError::SELinuxMountMismatch { .. }) => {
                    self.desired_state_of_world
                        .add_error_to_pod(&v.pod_name, &err.to_string());
                }
                // `IsVolumeNotAttachedError` -> `waitForVolumeAttach`
                Some(ActualStateOfWorldError::VolumeNotAttached { .. }) => {
                    operations.push(Box::pin(wait_for_volume_attach(mounter, v)));
                }
                Some(ActualStateOfWorldError::RemountRequired { .. }) => {
                    operations.push(Box::pin(mount_attached_volume(mounter, v, true)));
                }
                Some(ActualStateOfWorldError::FsResizeRequired { current_size, .. }) => {
                    operations.push(Box::pin(expand_volume(mounter, v, current_size)));
                }
                // `!volMounted`: not mounted for the pod, or uncertain
                _ if !vol_mounted => {
                    operations.push(Box::pin(mount_attached_volume(mounter, v, false)));
                }
                _ => {}
            }
        }
        operations
    }
}

/// An operation `mountOrAttachVolumes` starts.
pub type Operation = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Port of `waitForVolumeAttach` (`reconciler_common.go:234-...`) for a volume
/// the controller attaches or that needs no attach:
/// `VerifyControllerAttachedVolume`. The kubelet never attaches volumes itself
/// here (`controllerAttachDetachEnabled`), so upstream's `AttachVolume` arm is
/// not ported. A volume still waiting for its attachment is an expected
/// failure (`isExpectedError`): it backs off and shows in the wait message as
/// unattached.
async fn wait_for_volume_attach(mounter: Arc<VolumeMounter>, v: VolumeToMount) {
    if let Err(e) = mounter.verify_controller_attached_volume(&v).await {
        debug!(
            "operationExecutor.VerifyControllerAttachedVolume failed for volume {} pod {}: {e:#}",
            v.volume_name, v.pod_name
        );
    }
}

/// Port of `expandVolume` (`reconciler_common.go:194-206`).
async fn expand_volume(
    mounter: Arc<VolumeMounter>,
    v: VolumeToMount,
    current_size: Option<rusternetes_common::quantity::Quantity>,
) {
    debug!(
        "Starting operationExecutor.ExpandInUseVolume for volume {} pod {}",
        v.volume_name, v.pod_name
    );
    // `None` is upstream's zero `resource.Quantity`; the operation rejects it.
    let current_size = current_size.unwrap_or_else(|| {
        rusternetes_common::quantity::Quantity::from_value(
            0,
            rusternetes_common::quantity::Format::BinarySI,
        )
    });
    if let Err(e) = mounter.expand_in_use_volume(&v, current_size).await {
        error!(
            "operationExecutor.ExpandInUseVolume failed for volume {} pod {}: {e:#}",
            v.volume_name, v.pod_name
        );
    }
}

/// Port of `mountAttachedVolumes` (`reconciler_common.go:210-232`). Errors are
/// expected while the operation is pending or backing off
/// (`isExpectedError`), so a refused one is only logged at debug level.
async fn mount_attached_volume(mounter: Arc<VolumeMounter>, v: VolumeToMount, is_remount: bool) {
    debug!(
        "Starting operationExecutor.MountVolume for volume {} pod {} (remount {is_remount})",
        v.volume_name, v.pod_name
    );
    let spec = v.volume_spec.as_spec();
    let volume_name = v
        .outer_volume_spec_names
        .first()
        .cloned()
        .unwrap_or_default();
    match mounter
        .run_mount_operation(&spec, &v.pod, &v, &volume_name)
        .await
    {
        Ok(_) => {}
        Err(e)
            if e.downcast_ref::<crate::lifecycle::VolumeWaitError>()
                .is_some() =>
        {
            debug!("operationExecutor.MountVolume not started: {e:#}");
        }
        Err(e) => error!(
            "operationExecutor.MountVolume failed for volume {} pod {}: {e:#}",
            v.volume_name, v.pod_name
        ),
    }
}
