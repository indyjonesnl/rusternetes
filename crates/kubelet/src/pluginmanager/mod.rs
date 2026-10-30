//! Port of `pkg/kubelet/pluginmanager`: runs the asynchronous loops that work
//! out which kubelet plugins (CSI drivers, device plugins) need to be
//! registered or deregistered, and makes it so.
//!
//! A plugin announces itself by creating a unix socket in
//! `<kubelet root>/plugins_registry` and serving the
//! `pluginregistration.Registration` gRPC service on it (the CSI
//! `node-driver-registrar` sidecar does exactly this). The
//! [`watcher::Watcher`] fills the desired state of world, and the
//! [`reconciler::Reconciler`] runs the `GetInfo` / validate / register /
//! `NotifyRegistrationStatus` handshake (`operation.rs`).

pub mod cache;
pub mod csi_handler;
pub mod metrics;
pub mod operation;
pub mod reconciler;
pub mod watcher;

use cache::{ActualStateOfWorld, DesiredStateOfWorld, PluginHandler};
use operation::OperationExecutor;
use reconciler::Reconciler;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use watcher::Watcher;

/// `loopSleepDuration` (`plugin_manager.go:46`): the reconciler's period.
pub const LOOP_SLEEP_DURATION: Duration = Duration::from_secs(1);

/// `defaultKubeletPluginsRegistrationDirName` (`pkg/kubelet/kubelet_getters.go`):
/// the registration directory under the kubelet root directory.
pub fn plugins_registry_dir(root_dir: &std::path::Path) -> PathBuf {
    root_dir.join("plugins_registry")
}

/// `pluginManager` (`plugin_manager.go:84`).
pub struct PluginManager {
    watcher: Arc<Watcher>,
    reconciler: Arc<Reconciler>,
    pub desired_state_of_world: Arc<DesiredStateOfWorld>,
    pub actual_state_of_world: Arc<ActualStateOfWorld>,
}

impl PluginManager {
    /// `NewPluginManager` (`:53-82`).
    pub fn new(sock_dir: impl Into<PathBuf>) -> Self {
        Self::with_loop_sleep(sock_dir, LOOP_SLEEP_DURATION)
    }

    pub fn with_loop_sleep(sock_dir: impl Into<PathBuf>, loop_sleep: Duration) -> Self {
        let asw = Arc::new(ActualStateOfWorld::new());
        let dsw = Arc::new(DesiredStateOfWorld::new());
        let reconciler = Arc::new(Reconciler::new(
            OperationExecutor::new(),
            loop_sleep,
            dsw.clone(),
            asw.clone(),
        ));
        Self {
            watcher: Arc::new(Watcher::new(sock_dir, dsw.clone())),
            reconciler,
            desired_state_of_world: dsw,
            actual_state_of_world: asw,
        }
    }

    /// `AddHandler` (`:129`): register the consumer for a plugin type
    /// (`"CSIPlugin"`, `"DevicePlugin"`).
    pub fn add_handler(&self, plugin_type: &str, handler: Arc<dyn PluginHandler>) {
        self.reconciler.add_handler(plugin_type, handler);
    }

    /// `metrics.Register(pm.actualStateOfWorld, pm.desiredStateOfWorld)`
    /// (`plugin_manager.go:124`, inside `Run`): expose
    /// `plugin_manager_total_plugins` on `registry` (the kubelet's `/metrics`).
    pub fn register_metrics(&self, registry: &prometheus::Registry) {
        metrics::register(
            registry,
            self.actual_state_of_world.clone(),
            self.desired_state_of_world.clone(),
        );
    }

    /// `Run` (`:99-127`): start the watcher (populates the desired state) and
    /// then the reconciler. Fails if the registration directory cannot be
    /// created. Both loops stop when `stop` flips.
    pub fn run(&self, stop: tokio::sync::watch::Receiver<bool>) -> Result<(), String> {
        self.watcher.start(stop.clone())?;
        tracing::info!("Starting Kubelet Plugin Manager");
        let rc = self.reconciler.clone();
        tokio::spawn(async move { rc.run(stop).await });
        Ok(())
    }
}
