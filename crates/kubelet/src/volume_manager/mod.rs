//! Port of `pkg/kubelet/volumemanager` — the component that keeps the volumes
//! a node's pods need attached and mounted.
//!
//! The caches, the desired-state populator and the reconciler's remount arm
//! are here so far (#1970). The kubelet's inline mounting stays authoritative
//! for the first mount and for unmounting.

pub mod cache;
pub mod populator;
pub mod reconciler;

use crate::volume_manager::cache::{ActualStateOfWorld, DesiredStateOfWorld};
use crate::volume_manager::populator::{
    DesiredStateOfWorldPopulator, PodManager, PodStateProvider,
};
use crate::volume_manager::reconciler::{Reconciler, RECONCILER_LOOP_SLEEP_PERIOD};
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
}

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
        let _ = node_name;
        let actual_state_of_world = spec_factory.asw.clone();
        let reconciler = Arc::new(Reconciler::new(
            desired_state_of_world.clone(),
            actual_state_of_world.clone(),
            spec_factory.clone(),
            RECONCILER_LOOP_SLEEP_PERIOD,
        ));
        let desired_state_of_world_populator = Arc::new(DesiredStateOfWorldPopulator::new(
            storage,
            DESIRED_STATE_OF_WORLD_POPULATOR_LOOP_SLEEP_PERIOD,
            pod_manager,
            pod_state_provider,
            desired_state_of_world.clone(),
            actual_state_of_world.clone(),
            spec_factory,
            volume_plugin_mgr,
        ));
        Self {
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

    /// Port of `getUnattachedVolumes` (`volume_manager.go:545-555`), as outer
    /// volume names.
    fn unattached_volumes(&self, pod_name: &UniquePodName) -> Vec<String> {
        let mut unattached: Vec<String> = self
            .desired_state_of_world
            .get_volumes_to_mount()
            .into_iter()
            .filter(|v| {
                v.pod_name == *pod_name
                    && v.plugin_is_attachable
                    && !self.actual_state_of_world.volume_exists(&v.volume_name)
            })
            .flat_map(|v| v.outer_volume_spec_names.clone())
            .collect();
        unattached.sort();
        unattached
    }

    /// Port of `WaitForAttachAndMount` (`volume_manager.go:395-472`): waits for
    /// every volume the pod's containers use to be attached and mounted by the
    /// reconciler.
    ///
    /// Not ported: the `MutableCSINodeAllocatableCount` branch returning
    /// `VolumeAttachLimitExceededError` (`:425-460`, #2877).
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
        Err(wait_error(format!(
            "unmounted volumes={unmounted:?}, unattached volumes={:?}, failed to process volumes={:?}: {failure}",
            self.unattached_volumes(&pod_name),
            self.volumes_not_in_dsw(&pod_name, &expected),
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
            }
        }
        paths
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
