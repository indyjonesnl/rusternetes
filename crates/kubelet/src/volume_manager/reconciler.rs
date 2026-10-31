//! Port of `pkg/kubelet/volumemanager/reconciler` (`reconciler.go`,
//! `reconciler_common.go`) — the loop that drives the actual state of the
//! world towards the desired one.
//!
//! Only the remount arm of `mountOrAttachVolumes` is ported so far: a volume
//! the desired state wants, that the actual state has mounted for the pod,
//! and whose plugin asked for a remount (`RequiresRemount`: secret, configMap,
//! projected, downwardAPI), is mounted again through the plugin's `SetUp`.
//! The first mount, unmount and device detach stay with the kubelet's inline
//! mounting (#1970); the reconciler never touches a volume the actual state
//! does not know, so the two cannot disagree about who owns it.

use crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorldError;
use crate::volume_manager::cache::{ActualStateOfWorld, DesiredStateOfWorld};
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
    pub async fn run(&self) {
        loop {
            self.reconcile().await;
            tokio::time::sleep(self.loop_sleep_duration).await;
        }
    }

    /// Port of `reconcile` (`reconciler.go:74-...`), remount arm only.
    pub async fn reconcile(&self) {
        self.mount_or_attach_volumes().await;
    }

    /// Port of `mountOrAttachVolumes` (`reconciler_common.go:132-190`) for
    /// the `RemountRequired` outcome of `PodExistsInVolume`.
    pub async fn mount_or_attach_volumes(&self) {
        for volume_to_mount in self.desired_state_of_world.get_volumes_to_mount() {
            let v = &volume_to_mount.0;
            let (_, _, pod_exist_error) = self.actual_state_of_world.pod_exists_in_volume(
                &v.pod_name,
                &v.volume_name,
                v.desired_persistent_volume_size,
                &v.selinux_label,
            );
            // Only a volume already mounted for the pod that asks to be
            // mounted again; everything else is the inline mount's.
            if !matches!(
                pod_exist_error,
                Some(ActualStateOfWorldError::RemountRequired { .. })
            ) {
                continue;
            }
            self.remount_volume(v).await;
        }
    }

    /// Port of `mountAttachedVolumes` (`reconciler_common.go:210-232`) with
    /// `isRemount` true. Errors are expected while the operation is pending
    /// (`isExpectedError`), so a refused one is only logged at debug level.
    async fn remount_volume(
        &self,
        v: &crate::volume_plugins::util::operation_executor::VolumeToMount,
    ) {
        debug!(
            "Starting operationExecutor.MountVolume for volume {} pod {}: volume is already mounted to pod, but remount was requested",
            v.volume_name, v.pod_name
        );
        let spec = v.volume_spec.as_spec();
        let volume_name = v
            .outer_volume_spec_names
            .first()
            .cloned()
            .unwrap_or_default();
        match self
            .mounter
            .run_mount_operation(&spec, &v.pod, v, &volume_name)
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
}
