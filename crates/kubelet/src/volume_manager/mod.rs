//! Port of `pkg/kubelet/volumemanager` — the component that keeps the volumes
//! a node's pods need attached and mounted.
//!
//! The caches, the desired-state populator and the reconciler's remount arm
//! are here so far (#1970). The kubelet's inline mounting stays authoritative
//! for the first mount and for unmounting.

pub mod cache;
pub mod populator;
pub mod reconciler;
pub mod reconstruct;

use crate::volume_manager::cache::{ActualStateOfWorld, DesiredStateOfWorld};
use crate::volume_manager::populator::{
    DesiredStateOfWorldPopulator, PodManager, PodStateProvider,
};
use crate::volume_manager::reconciler::{Reconciler, RECONCILER_LOOP_SLEEP_PERIOD};
use crate::volume_plugins::plugin::VolumePlugin;
use crate::volume_plugins::util::selinux::Translator;
use crate::volume_plugins::util::types::UniquePodName;
use crate::volume_plugins::VolumePluginMgr;
use rusternetes_common::resources::Pod;
use rusternetes_storage::StorageBackend;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// `desiredStateOfWorldPopulatorLoopSleepPeriod`
/// (`pkg/kubelet/volumemanager/volume_manager.go`).
pub const DESIRED_STATE_OF_WORLD_POPULATOR_LOOP_SLEEP_PERIOD: Duration = Duration::from_millis(100);

/// `podAttachAndMountRetryInterval` (`volume_manager.go:80`).
pub const POD_ATTACH_AND_MOUNT_RETRY_INTERVAL: Duration = Duration::from_millis(300);

/// `podAttachAndMountTimeout` (`volume_manager.go:76`) is 2m3s. The kubelet
/// bounds each `sync_pod` at 120s (`kubelet.rs` `timeout_secs`), a limit
/// upstream has not, so the wait must give up well before it: its error is
/// what puts the pod in `ContainerCreating` and the next sync retries, which
/// is what the upstream constant's comment relies on too.
pub const POD_ATTACH_AND_MOUNT_TIMEOUT: Duration = Duration::from_secs(30);

/// Port of `volumeManager` (`volume_manager.go`), so far owning the two caches
/// and the populator that fills the desired state.
pub struct VolumeManager {
    pub desired_state_of_world: Arc<DesiredStateOfWorld>,
    pub actual_state_of_world: Arc<ActualStateOfWorld>,
    pub desired_state_of_world_populator: Arc<DesiredStateOfWorldPopulator>,
    pub reconciler: Arc<Reconciler>,
    node_name: String,
    spec_factory: Arc<crate::volumes::VolumeManager>,
    /// `nim`, the CSI node info manager `VerifyExhaustedResource` refreshes the
    /// CSINode allocatable count through. Set once it exists
    /// ([`Self::set_node_info_installer`]); without it the check reads `false`.
    node_info_installer:
        std::sync::OnceLock<Arc<dyn crate::volume_plugins::nodeinfomanager::NodeInfoInstaller>>,
}

/// `VolumeAttachmentLimitExceededReason` (`volume_manager.go:92`): the reason a
/// pod is rejected when its volumes cannot attach for lack of room on the node.
pub const VOLUME_ATTACHMENT_LIMIT_EXCEEDED_REASON: &str = "VolumeAttachmentLimitExceeded";

/// Port of `VolumeAttachLimitExceededError` (`volume_manager.go:287-299`):
/// `WaitForAttachAndMount` found a volume's attach failing with
/// `ResourceExhausted`. The kubelet rejects the pod (`kubelet.go:2126-2131`).
#[derive(Debug)]
pub struct VolumeAttachLimitExceededError {
    pub unmounted_volumes: Vec<String>,
    pub unattached_volumes: Vec<String>,
    pub volumes_not_in_dsw: Vec<String>,
    pub original_error: String,
}

impl std::fmt::Display for VolumeAttachLimitExceededError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node has reached its volume attachment limit, rejecting pod. unmounted volumes={:?}, unattached volumes={:?}, failed to process volumes={:?}: {}",
            self.unmounted_volumes,
            self.unattached_volumes,
            self.volumes_not_in_dsw,
            self.original_error
        )
    }
}

impl std::error::Error for VolumeAttachLimitExceededError {}

/// Why `WaitForAttachAndMount` failed. The pod is waiting on its volumes, not
/// failing: the kubelet shows `ContainerCreating` and retries on the next
/// sync (`kubelet.go:2204`, `kubelet_pods.go:2496`).
pub fn wait_error(message: String) -> anyhow::Error {
    crate::lifecycle::VolumeWaitError(message).into()
}

impl VolumeManager {
    /// Port of `NewVolumeManager` (`volume_manager.go:182-254`), without the
    /// reconciler and the operation executor.
    pub fn new(
        node_name: &str,
        storage: Arc<StorageBackend>,
        pod_manager: Arc<dyn PodManager>,
        pod_state_provider: Arc<dyn PodStateProvider>,
        spec_factory: Arc<crate::volumes::VolumeManager>,
        volume_plugin_mgr: Arc<VolumePluginMgr>,
    ) -> Self {
        let desired_state_of_world = Arc::new(DesiredStateOfWorld::new(
            volume_plugin_mgr.clone(),
            Arc::new(Translator),
        ));
        // One actual state of world: the one inline mounting reports into.
        let actual_state_of_world = spec_factory.asw.clone();
        let desired_state_of_world_populator = Arc::new(DesiredStateOfWorldPopulator::new(
            storage,
            DESIRED_STATE_OF_WORLD_POPULATOR_LOOP_SLEEP_PERIOD,
            pod_manager,
            pod_state_provider,
            desired_state_of_world.clone(),
            actual_state_of_world.clone(),
            spec_factory.clone(),
            volume_plugin_mgr,
        ));
        let populator_has_added_pods = desired_state_of_world_populator.clone();
        let reconciler = Arc::new(Reconciler::new(
            desired_state_of_world.clone(),
            actual_state_of_world.clone(),
            spec_factory.clone(),
            RECONCILER_LOOP_SLEEP_PERIOD,
            move || populator_has_added_pods.has_added_pods(),
        ));
        Self {
            node_name: node_name.to_string(),
            spec_factory,
            node_info_installer: std::sync::OnceLock::new(),
            desired_state_of_world,
            actual_state_of_world,
            desired_state_of_world_populator,
            reconciler,
        }
    }

    /// Port of `getExpectedVolumes` (`volume_manager.go:624-627`).
    fn expected_volumes(pod: &Pod) -> Vec<String> {
        let (mounts, devices, _) = crate::volume_plugins::util::get_pod_volume_names(pod, false);
        mounts.union(&devices).cloned().collect()
    }

    /// Port of `getUnmountedVolumes` (`volume_manager.go:603-620`).
    fn unmounted_volumes(&self, pod_name: &UniquePodName, expected: &[String]) -> Vec<String> {
        let volume_names = self
            .desired_state_of_world
            .get_volume_names_for_pod(pod_name);
        let mut unmounted: Vec<String> = expected
            .iter()
            .filter(|outer| {
                volume_names.get(*outer).is_none_or(|volume_name| {
                    self.actual_state_of_world
                        .get_mounted_volume_for_pod(pod_name, volume_name)
                        .is_none()
                })
            })
            .cloned()
            .collect();
        unmounted.sort();
        unmounted
    }

    /// Port of `getVolumesNotInDSW` (`volume_manager.go:530-543`).
    fn volumes_not_in_dsw(&self, pod_name: &UniquePodName, expected: &[String]) -> Vec<String> {
        let in_dsw = self
            .desired_state_of_world
            .get_volume_names_for_pod(pod_name);
        let mut missing: Vec<String> = expected
            .iter()
            .filter(|n| !in_dsw.contains_key(*n))
            .cloned()
            .collect();
        missing.sort();
        missing
    }

    /// Port of `getUnattachedVolumes` (`volume_manager.go:545-555`).
    fn unattached_volumes_to_mount(
        &self,
        pod_name: &UniquePodName,
    ) -> Vec<crate::volume_plugins::util::operation_executor::VolumeToMount> {
        self.desired_state_of_world
            .get_volumes_to_mount()
            .into_iter()
            .map(|v| v.0)
            .filter(|v| {
                v.pod_name == *pod_name
                    && v.plugin_is_attachable
                    && !self.actual_state_of_world.volume_exists(&v.volume_name)
            })
            .collect()
    }

    /// Hand over the node info manager (`nim`) `VerifyExhaustedResource` needs.
    pub fn set_node_info_installer(
        &self,
        installer: Arc<dyn crate::volume_plugins::nodeinfomanager::NodeInfoInstaller>,
    ) {
        let _ = self.node_info_installer.set(installer);
    }

    /// Port of `WaitForAttachAndMount` (`volume_manager.go:395-472`): waits for
    /// every volume the pod's containers use to be attached and mounted by the
    /// reconciler.
    ///
    pub async fn wait_for_attach_and_mount(&self, pod: &Pod) -> anyhow::Result<()> {
        self.wait_for_attach_and_mount_within(pod, POD_ATTACH_AND_MOUNT_TIMEOUT)
            .await
    }

    /// [`Self::wait_for_attach_and_mount`] with the `podAttachAndMountTimeout`
    /// given (upstream's tests shorten it the same way).
    pub async fn wait_for_attach_and_mount_within(
        &self,
        pod: &Pod,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let expected = Self::expected_volumes(pod);
        if expected.is_empty() {
            return Ok(());
        }
        let pod_name = crate::volume_plugins::util::get_unique_pod_name(pod);

        // Some pods expect to have Setup called over and over again to update.
        // Remount plugins for which this is true.
        self.desired_state_of_world_populator
            .reprocess_pod(&pod_name);

        // `wait.PollUntilContextTimeout(.., immediate=true, verifyVolumesMountedFunc)`
        let deadline = tokio::time::Instant::now() + timeout;
        let failure = loop {
            let errors = self.desired_state_of_world.pop_pod_errors(&pod_name);
            if !errors.is_empty() {
                break errors.join("; ");
            }
            if self.unmounted_volumes(&pod_name, &expected).is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                break "context deadline exceeded".to_string();
            }
            tokio::time::sleep(POD_ATTACH_AND_MOUNT_RETRY_INTERVAL).await;
        };

        let unmounted = self.unmounted_volumes(&pod_name, &expected);
        if unmounted.is_empty() {
            return Ok(());
        }
        // Also get unattached volumes and volumes not in dsw for the message
        let unattached_to_mount = self.unattached_volumes_to_mount(&pod_name);
        let mut unattached: Vec<String> = unattached_to_mount
            .iter()
            .flat_map(|v| v.outer_volume_spec_names.clone())
            .collect();
        unattached.sort();
        let not_in_dsw = self.volumes_not_in_dsw(&pod_name, &expected);

        if rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::MutableCSINodeAllocatableCount,
        ) {
            for volume_to_mount in &unattached_to_mount {
                let spec = volume_to_mount.volume_spec.as_spec();
                // `FindAttachablePluginBySpec`: the CSI plugin is the attachable
                // plugin that implements `VerifyExhaustedResource`.
                if !self.spec_factory.csi_plugin.can_support(&spec) {
                    continue;
                }
                let Some(nim) = self.node_info_installer.get() else {
                    continue;
                };
                if self
                    .spec_factory
                    .csi_plugin
                    .verify_exhausted_resource(
                        &spec,
                        &self.node_name,
                        crate::volume_plugins::csi_drivers_store::csi_drivers(),
                        nim.as_ref(),
                    )
                    .await
                {
                    // The kubelet rejects the pod.
                    return Err(VolumeAttachLimitExceededError {
                        unmounted_volumes: unmounted,
                        unattached_volumes: unattached,
                        volumes_not_in_dsw: not_in_dsw,
                        original_error: failure,
                    }
                    .into());
                }
            }
        }

        Err(wait_error(format!(
            "unmounted volumes={unmounted:?}, unattached volumes={unattached:?}, failed to process volumes={not_in_dsw:?}: {failure}"
        )))
    }

    /// The `volume name -> path` map of the pod's mounted volumes: what the
    /// kubelet reads from `GetMountedVolumesForPod` (`volume_manager.go:320`,
    /// `kubelet_pods.go:643`), keyed by the outer volume name.
    pub fn get_mounted_volume_paths(&self, pod: &Pod) -> HashMap<String, String> {
        let pod_name = crate::volume_plugins::util::get_unique_pod_name(pod);
        let volume_names = self
            .desired_state_of_world
            .get_volume_names_for_pod(&pod_name);
        let mut paths = HashMap::new();
        for (outer, volume_name) in volume_names {
            let Some(mounted) = self
                .actual_state_of_world
                .get_mounted_volume_for_pod(&pod_name, &volume_name)
            else {
                continue;
            };
            if let Some(mounter) = mounted.mounter.as_ref() {
                paths.insert(outer, mounter.get_path());
            } else if let Some(mapper) = mounted.block_volume_mapper.as_ref() {
                // A block volume has a `BlockVolumeMapper` instead of a
                // mounter (`volume_manager.go:320`); `makeBlockVolumes`
                // reads its pod device map symlink (`kubelet_pods.go:243`).
                let (dir, name) = mapper.get_pod_device_map_path();
                paths.insert(
                    outer,
                    std::path::Path::new(&dir)
                        .join(name)
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
        paths
    }

    /// Port of `WaitForUnmount` (`volume_manager.go:475-511`): waits for the
    /// reconciler to unmount every volume of a terminated pod
    /// (`SyncTerminatedPod`, `kubelet.go:2364`).
    pub async fn wait_for_unmount(&self, pod: &Pod) -> anyhow::Result<()> {
        self.wait_for_unmount_within(pod, POD_ATTACH_AND_MOUNT_TIMEOUT)
            .await
    }

    /// [`Self::wait_for_unmount`] with the `podAttachAndMountTimeout` given.
    pub async fn wait_for_unmount_within(
        &self,
        pod: &Pod,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let pod_name = crate::volume_plugins::util::get_unique_pod_name(pod);
        self.desired_state_of_world_populator
            .reprocess_pod(&pod_name);

        // `wait.PollUntilContextTimeout(.., immediate=true, verifyVolumesUnmountedFunc)`
        let deadline = tokio::time::Instant::now() + timeout;
        let failure = loop {
            // `verifyVolumesUnmountedFunc` (`volume_manager.go:570-577`)
            let errors = self.desired_state_of_world.pop_pod_errors(&pod_name);
            if !errors.is_empty() {
                break errors.join("; ");
            }
            if !self
                .actual_state_of_world
                .pod_has_mounted_volumes(&pod_name)
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                break "context deadline exceeded".to_string();
            }
            tokio::time::sleep(POD_ATTACH_AND_MOUNT_RETRY_INTERVAL).await;
        };

        let mut mounted: Vec<String> = self
            .actual_state_of_world
            .get_mounted_volumes_for_pod(&pod_name)
            .into_iter()
            .map(|v| v.0.volume_name.0)
            .collect();
        if mounted.is_empty() {
            return Ok(());
        }
        mounted.sort();
        Err(anyhow::anyhow!("mounted volumes={mounted:?}: {failure}"))
    }

    /// Port of `Run` (`volume_manager.go:290-310`): starts the populator
    /// (`go vm.desiredStateOfWorldPopulator.Run(sourcesReady)`) and the
    /// reconciler (`go vm.reconciler.Run(stopCh)`).
    pub fn run(&self, sources_ready: impl Fn() -> bool + Send + 'static) {
        let populator = self.desired_state_of_world_populator.clone();
        tokio::spawn(async move { populator.run(sources_ready).await });
        let reconciler = self.reconciler.clone();
        tokio::spawn(async move { reconciler.run().await });
    }
}
