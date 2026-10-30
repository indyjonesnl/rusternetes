//! Unit tests moved from `src/pluginmanager/cache.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, Mutex};
    pub use std::time::{Duration, SystemTime};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::pluginmanager::cache::*;

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
