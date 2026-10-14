//! Port of component-base/configz/configz.go:63-126.
use std::{collections::BTreeMap, sync::RwLock};

/// Process component configurations registered with the /configz handler.
/// The map and duplicate-name guard mirror configz.go:75-99.
#[derive(Default)]
pub struct ConfigRegistry {
    configs: RwLock<BTreeMap<String, serde_json::Value>>,
}

impl ConfigRegistry {
    /// Register an initially unset configuration (configz.go:75-83).
    pub fn register(&self, name: &str) -> anyhow::Result<()> {
        let mut configs = self.configs.write().expect("configz lock poisoned");
        if configs.contains_key(name) {
            anyhow::bail!("register config {name:?} twice");
        }
        configs.insert(name.to_owned(), serde_json::Value::Null);
        Ok(())
    }

    /// Replace a registered configuration (configz.go:95-99).
    pub fn set(&self, name: &str, value: serde_json::Value) -> anyhow::Result<()> {
        let mut configs = self.configs.write().expect("configz lock poisoned");
        let Some(config) = configs.get_mut(name) else {
            anyhow::bail!("config {name:?} is not registered");
        };
        *config = value;
        Ok(())
    }

    /// Remove a registration, including an absent name (configz.go:88-92).
    pub fn delete(&self, name: &str) {
        self.configs
            .write()
            .expect("configz lock poisoned")
            .remove(name);
    }

    /// JSON has no trailing newline; headers are installed by the handler
    /// (configz.go:112-126).
    pub(super) fn json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&*self.configs.read().expect("configz lock poisoned"))
    }
}
