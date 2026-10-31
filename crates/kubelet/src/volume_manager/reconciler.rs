//! Port of `pkg/kubelet/volumemanager/reconciler` (`reconciler.go`,
//! `reconciler_common.go`) — the loop that drives the actual state of the
//! world towards the desired one.
//!
//! Only `mountOrAttachVolumes` is ported so far (the first mount, remount and
//! expand-in-use arms); unmount and device detach stay with the kubelet's
//! disk-driven cleanup (#1970). Remount: a volume
//! the desired state wants, that the actual state has mounted for the pod,
//! and whose plugin asked for a remount (`RequiresRemount`: secret, configMap,
//! projected, downwardAPI), is mounted again through the plugin's `SetUp`.
//! The first mount, unmount and device detach stay with the kubelet's inline
//! mounting (#1970); the reconciler never touches a volume the actual state
//! does not know, so the two cannot disagree about who owns it.

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
}

impl Reconciler {
    /// Port of `NewReconciler` (`reconciler.go:35-68`).
    pub fn new(
        desired_state_of_world: Arc<DesiredStateOfWorld>,
        actual_state_of_world: Arc<ActualStateOfWorld>,
        mounter: Arc<VolumeMounter>,
        loop_sleep_duration: Duration,
    ) -> Self {
        Self {
            desired_state_of_world,
            actual_state_of_world,
            mounter,
            loop_sleep_duration,
        }
    }

    /// Port of `Run` (`reconciler.go:70-72`): `wait.Until(rc.reconcile, ...)`.
    /// Each operation runs in its own task, as upstream's operation executor
    /// runs each in its own goroutine, so a slow mount does not hold up the
    /// others; `isOperationPending` keeps a volume's operations apart.
    pub async fn run(&self) {
        loop {
            for operation in self.mount_or_attach_volumes() {
                tokio::spawn(operation);
            }
            tokio::time::sleep(self.loop_sleep_duration).await;
        }
    }

    /// Port of `reconcile` (`reconciler.go:74-...`), `mountOrAttachVolumes`
    /// only, running the operations it starts to completion.
    pub async fn reconcile(&self) {
        futures::future::join_all(self.mount_or_attach_volumes()).await;
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
                    wait_for_volume_attach(&mounter, &v);
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

/// Port of `waitForVolumeAttach` (`reconciler_common.go:234-...`) as far as a
/// volume the kubelet need not attach goes: `VerifyControllerAttachedVolume`
/// marks it attached (`operation_generator.go:1431-1446`). The controller
/// attach-detach state of an attachable plugin (`node.status.volumesAttached`,
/// `VolumesInUse`) is not consulted: the CSI mounter waits for its own
/// `VolumeAttachment`.
fn wait_for_volume_attach(mounter: &VolumeMounter, v: &VolumeToMount) {
    if let Err(e) = mounter.mark_volume_attached(v) {
        error!(
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
