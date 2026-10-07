//! Port of `pkg/kubelet/pluginmanager/cache` (`types.go`,
//! `desired_state_of_world.go`, `actual_state_of_world.go`): the two
//! thread-safe maps from plugin socket path to plugin information that the
//! plugin manager reconciles.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Port of `PluginHandler` (`cache/types.go:40-52`): the interface a plugin
/// consumer (CSI, device plugins, ...) implements. The watcher drives it
/// strictly and sequentially per plugin name:
/// Validate -> Register -> DeRegister (see the state-machine diagram upstream).
#[async_trait]
pub trait PluginHandler: Send + Sync {
    /// `ValidatePlugin`: error if the information provided by the potential
    /// plugin is erroneous (unsupported version, ...).
    fn validate_plugin(
        &self,
        plugin_name: &str,
        endpoint: &str,
        versions: &[String],
    ) -> Result<(), String>;

    /// `RegisterPlugin`: errors here are still notified to the plugin.
    /// `plugin_client_timeout` is `nil` upstream for the plugin manager.
    async fn register_plugin(
        &self,
        plugin_name: &str,
        endpoint: &str,
        versions: &[String],
        plugin_client_timeout: Option<Duration>,
    ) -> Result<(), String>;

    /// `DeRegisterPlugin`: called once the watcher observes that the socket
    /// has been deleted.
    async fn deregister_plugin(&self, plugin_name: &str, endpoint: &str);
}

/// Port of `PluginInfo` (`actual_state_of_world.go:73-80`).
#[derive(Clone)]
pub struct PluginInfo {
    pub socket_path: String,
    pub timestamp: SystemTime,
    pub uuid: String,
    pub handler: Option<Arc<dyn PluginHandler>>,
    pub name: String,
    pub endpoint: String,
}

impl std::fmt::Debug for PluginInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginInfo")
            .field("socket_path", &self.socket_path)
            .field("uuid", &self.uuid)
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// Port of `DesiredStateOfWorld` (`desired_state_of_world.go:36-56`).
#[derive(Default)]
pub struct DesiredStateOfWorld {
    socket_file_to_info: Mutex<HashMap<String, PluginInfo>>,
}

impl DesiredStateOfWorld {
    pub fn new() -> Self {
        Self::default()
    }

    /// `AddOrUpdatePlugin` (`:112-133`): add, or refresh the timestamp AND the
    /// UUID of an existing entry. Only the desired state gets a fresh UUID: the
    /// reconciler compares it with the actual state's to notice a re-created
    /// socket. Empty path is an error.
    pub fn add_or_update_plugin(&self, socket_path: &str) -> Result<(), String> {
        if socket_path.is_empty() {
            return Err("socket path is empty".to_string());
        }
        self.socket_file_to_info.lock().unwrap().insert(
            socket_path.to_string(),
            PluginInfo {
                socket_path: socket_path.to_string(),
                timestamp: SystemTime::now(),
                uuid: uuid::Uuid::new_v4().to_string(),
                handler: None,
                name: String::new(),
                endpoint: String::new(),
            },
        );
        Ok(())
    }

    /// `RemovePlugin` (`:135`): no-op when absent.
    pub fn remove_plugin(&self, socket_path: &str) {
        self.socket_file_to_info.lock().unwrap().remove(socket_path);
    }

    /// `GetPluginsToRegister` (`:143`).
    pub fn get_plugins_to_register(&self) -> Vec<PluginInfo> {
        self.socket_file_to_info
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// `PluginExists` (`:155`).
    pub fn plugin_exists(&self, socket_path: &str) -> bool {
        self.socket_file_to_info
            .lock()
            .unwrap()
            .contains_key(socket_path)
    }
}

/// Port of `ActualStateOfWorld` (`actual_state_of_world.go:34-66`).
#[derive(Default)]
pub struct ActualStateOfWorld {
    socket_file_to_info: Mutex<HashMap<String, PluginInfo>>,
}

impl ActualStateOfWorld {
    pub fn new() -> Self {
        Self::default()
    }

    /// `AddPlugin` (`:85`): empty socket path is an error; an existing entry
    /// is replaced.
    pub fn add_plugin(&self, info: PluginInfo) -> Result<(), String> {
        if info.socket_path.is_empty() {
            return Err("socket path is empty".to_string());
        }
        self.socket_file_to_info
            .lock()
            .unwrap()
            .insert(info.socket_path.clone(), info);
        Ok(())
    }

    /// `RemovePlugin` (`:100`): no-op when absent.
    pub fn remove_plugin(&self, socket_path: &str) {
        self.socket_file_to_info.lock().unwrap().remove(socket_path);
    }

    /// `GetRegisteredPlugins` (`:107`).
    pub fn get_registered_plugins(&self) -> Vec<PluginInfo> {
        self.socket_file_to_info
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// `PluginExistsWithCorrectUUID` (`:129`): the socket path exists in the
    /// actual state AND carries the desired state's UUID.
    pub fn plugin_exists_with_correct_uuid(&self, info: &PluginInfo) -> bool {
        self.socket_file_to_info
            .lock()
            .unwrap()
            .get(&info.socket_path)
            .is_some_and(|actual| actual.uuid == info.uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actual(path: &str, uuid: &str) -> PluginInfo {
        PluginInfo {
            socket_path: path.into(),
            timestamp: SystemTime::now(),
            uuid: uuid.into(),
            handler: None,
            name: "n".into(),
            endpoint: path.into(),
        }
    }

    /// `Test_DSW_AddOrUpdatePlugin_Positive_NewPlugin`.
    #[test]
    fn dsw_add_new_plugin() {
        let dsw = DesiredStateOfWorld::new();
        dsw.add_or_update_plugin("/a.sock").unwrap();
        assert!(dsw.plugin_exists("/a.sock"));
        assert_eq!(dsw.get_plugins_to_register().len(), 1);
    }

    /// `Test_DSW_AddOrUpdatePlugin_Positive_ExistingPlugin`: re-adding keeps one
    /// entry but changes its UUID (the reconciler's re-registration signal).
    #[test]
    fn dsw_update_existing_changes_uuid() {
        let dsw = DesiredStateOfWorld::new();
        dsw.add_or_update_plugin("/a.sock").unwrap();
        let first = dsw.get_plugins_to_register().remove(0);
        dsw.add_or_update_plugin("/a.sock").unwrap();
        let all = dsw.get_plugins_to_register();
        assert_eq!(all.len(), 1);
        assert_ne!(all[0].uuid, first.uuid);
    }

    /// `Test_DSW_AddOrUpdatePlugin_Negative_PluginMissingInfo`.
    #[test]
    fn dsw_empty_path_errors() {
        assert!(DesiredStateOfWorld::new().add_or_update_plugin("").is_err());
    }

    /// `Test_DSW_RemovePlugin_Positive`.
    #[test]
    fn dsw_remove() {
        let dsw = DesiredStateOfWorld::new();
        dsw.add_or_update_plugin("/a.sock").unwrap();
        dsw.remove_plugin("/a.sock");
        dsw.remove_plugin("/missing.sock");
        assert!(!dsw.plugin_exists("/a.sock"));
        assert!(dsw.get_plugins_to_register().is_empty());
    }

    /// `Test_ASW_AddPlugin_Positive_NewPlugin` + `_Negative_EmptySocketPath`.
    #[test]
    fn asw_add_and_empty_path() {
        let asw = ActualStateOfWorld::new();
        asw.add_plugin(actual("/a.sock", "u1")).unwrap();
        assert_eq!(asw.get_registered_plugins().len(), 1);
        assert!(asw.add_plugin(actual("", "u1")).is_err());
    }

    /// `Test_ASW_RemovePlugin_Positive`.
    #[test]
    fn asw_remove() {
        let asw = ActualStateOfWorld::new();
        asw.add_plugin(actual("/a.sock", "u1")).unwrap();
        asw.remove_plugin("/a.sock");
        assert!(asw.get_registered_plugins().is_empty());
    }

    /// `Test_ASW_PluginExistsWithCorrectUUID_Negative_WrongUUID`.
    #[test]
    fn asw_wrong_uuid() {
        let asw = ActualStateOfWorld::new();
        asw.add_plugin(actual("/a.sock", "u1")).unwrap();
        assert!(asw.plugin_exists_with_correct_uuid(&actual("/a.sock", "u1")));
        assert!(!asw.plugin_exists_with_correct_uuid(&actual("/a.sock", "u2")));
        assert!(!asw.plugin_exists_with_correct_uuid(&actual("/b.sock", "u1")));
    }
}
