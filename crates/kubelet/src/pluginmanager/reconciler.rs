//! Port of `pkg/kubelet/pluginmanager/reconciler/reconciler.go`: a periodic
//! loop that reconciles the desired state of the world (sockets seen by the
//! watcher) with the actual state (plugins that completed registration) by
//! triggering unregister and register operations.

use super::cache::{ActualStateOfWorld, DesiredStateOfWorld, PluginHandler};
use super::operation::{OperationExecutor, RunError};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub struct Reconciler {
    operation_executor: OperationExecutor,
    loop_sleep_duration: Duration,
    desired_state_of_world: Arc<DesiredStateOfWorld>,
    actual_state_of_world: Arc<ActualStateOfWorld>,
    handlers: RwLock<HashMap<String, Arc<dyn PluginHandler>>>,
}

impl Reconciler {
    /// `NewReconciler` (`reconciler.go:56-69`).
    pub fn new(
        operation_executor: OperationExecutor,
        loop_sleep_duration: Duration,
        desired_state_of_world: Arc<DesiredStateOfWorld>,
        actual_state_of_world: Arc<ActualStateOfWorld>,
    ) -> Self {
        Self {
            operation_executor,
            loop_sleep_duration,
            desired_state_of_world,
            actual_state_of_world,
            handlers: RwLock::new(HashMap::new()),
        }
    }

    /// `AddHandler` (`:97`).
    pub fn add_handler(&self, plugin_type: &str, handler: Arc<dyn PluginHandler>) {
        self.handlers
            .write()
            .unwrap()
            .insert(plugin_type.to_string(), handler);
    }

    /// `Run` (`:84`): `wait.Until(reconcile, loopSleepDuration, stopCh)`.
    pub async fn run(&self, mut stop: tokio::sync::watch::Receiver<bool>) {
        loop {
            self.reconcile();
            tokio::select! {
                _ = tokio::time::sleep(self.loop_sleep_duration) => {}
                _ = stop.changed() => return,
            }
        }
    }

    /// `reconcile` (`:115-171`). Unregistrations are triggered before
    /// registrations. `AlreadyExists` and exponential-backoff refusals are
    /// expected and not logged.
    pub fn reconcile(&self) {
        let handlers = self.handlers.read().unwrap().clone();
        let dsw_plugins = self.desired_state_of_world.get_plugins_to_register();

        for registered in self.actual_state_of_world.get_registered_plugins() {
            let unregister = if !self
                .desired_state_of_world
                .plugin_exists(&registered.socket_path)
            {
                true
            } else {
                // Same socket path but a different UUID: the socket was
                // re-created (plugin update), so unregister the old one first.
                dsw_plugins
                    .iter()
                    .any(|d| d.socket_path == registered.socket_path && d.uuid != registered.uuid)
            };
            if unregister {
                match self
                    .operation_executor
                    .unregister_plugin(registered.clone(), self.actual_state_of_world.clone())
                {
                    Ok(()) => tracing::info!(plugin = ?registered, "UnregisterPlugin started"),
                    Err(RunError::AlreadyExists | RunError::ExponentialBackoff) => {}
                }
            }
        }

        for to_register in dsw_plugins {
            if !self
                .actual_state_of_world
                .plugin_exists_with_correct_uuid(&to_register)
            {
                match self.operation_executor.register_plugin(
                    &to_register.socket_path,
                    &to_register.uuid,
                    handlers.clone(),
                    self.actual_state_of_world.clone(),
                ) {
                    Ok(()) => tracing::info!(plugin = ?to_register, "RegisterPlugin started"),
                    Err(RunError::AlreadyExists | RunError::ExponentialBackoff) => {}
                }
            }
        }
    }
}
