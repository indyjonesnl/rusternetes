//! Port of `pkg/kubelet/volumemanager` — the component that keeps the volumes
//! a node's pods need attached and mounted.
//!
//! The caches and the desired-state populator are here so far. The reconciler
//! arrives with its own sub-project (#1970); until it does the kubelet's
//! inline mounting stays authoritative, and the populator only fills the
//! desired state of the world, which nothing reads yet.

pub mod cache;
pub mod populator;

use crate::volume_manager::cache::{ActualStateOfWorld, DesiredStateOfWorld};
use crate::volume_manager::populator::{
    DesiredStateOfWorldPopulator, PodManager, PodStateProvider,
};
use crate::volume_plugins::util::selinux::Translator;
use crate::volume_plugins::VolumePluginMgr;
use rusternetes_storage::StorageBackend;
use std::sync::Arc;
use std::time::Duration;

/// `desiredStateOfWorldPopulatorLoopSleepPeriod`
/// (`pkg/kubelet/volumemanager/volume_manager.go`).
pub const DESIRED_STATE_OF_WORLD_POPULATOR_LOOP_SLEEP_PERIOD: Duration = Duration::from_millis(100);

/// Port of `volumeManager` (`volume_manager.go`), so far owning the two caches
/// and the populator that fills the desired state.
pub struct VolumeManager {
    pub desired_state_of_world: Arc<DesiredStateOfWorld>,
    pub actual_state_of_world: Arc<ActualStateOfWorld>,
    pub desired_state_of_world_populator: Arc<DesiredStateOfWorldPopulator>,
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
        let actual_state_of_world = Arc::new(ActualStateOfWorld::new(
            node_name,
            volume_plugin_mgr.clone(),
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
        }
    }

    /// Port of `Run` (`volume_manager.go:290-310`): starts the populator
    /// (`go vm.desiredStateOfWorldPopulator.Run(sourcesReady)`). The reconciler
    /// loop is not started yet.
    pub fn run(&self, sources_ready: impl Fn() -> bool + Send + 'static) {
        let populator = self.desired_state_of_world_populator.clone();
        tokio::spawn(async move { populator.run(sources_ready).await });
    }
}
