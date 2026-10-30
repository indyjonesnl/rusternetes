//! Port of `pkg/volume/csi/csi_drivers_store.go` — the registry of CSI drivers
//! that have registered with this kubelet.
//!
//! Upstream keeps ONE package-level store (`var csiDrivers = DriversStore{}`,
//! `csi_plugin.go:71`) that the plugin-registration handler writes
//! (`RegisterPlugin`, `csi_plugin.go:118`) and every client constructor reads
//! (`newCsiDriverClient`, `csi_client.go:153`). The same shape is kept here: a
//! process-wide store, so the CSI volume plugin needs no extra plumbing to see
//! drivers registered elsewhere in the kubelet.
//!
//! The store is populated by `pluginmanager::csi_handler::RegistrationHandler`
//! when a `node-driver-registrar` socket in `<root>/plugins_registry` completes
//! the plugin-registration handshake.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Port of `Driver` (`csi_drivers_store.go:26-29`): a driver is described by
/// its socket endpoint and the highest CSI version it supports.
///
/// `highest_supported_version` is kept as the raw string the driver reported
/// during registration; upstream parses it with `utilversion` but only ever
/// stores and logs it (`csi_plugin.go:127-133`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Driver {
    pub endpoint: String,
    pub highest_supported_version: String,
}

/// Port of `DriversStore` (`csi_drivers_store.go:32-36`): a name-keyed map
/// behind an `RWMutex`.
#[derive(Default)]
pub struct DriversStore {
    store: RwLock<HashMap<String, Driver>>,
}

impl DriversStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// `Get` (`csi_drivers_store.go:41`).
    pub fn get(&self, driver_name: &str) -> Option<Driver> {
        self.store.read().unwrap().get(driver_name).cloned()
    }

    /// `Set` (`csi_drivers_store.go:52`).
    pub fn set(&self, driver_name: &str, driver: Driver) {
        self.store
            .write()
            .unwrap()
            .insert(driver_name.to_string(), driver);
    }

    /// `Delete` (`csi_drivers_store.go:65`).
    pub fn delete(&self, driver_name: &str) {
        self.store.write().unwrap().remove(driver_name);
    }

    /// `Clear` (`csi_drivers_store.go:74`).
    pub fn clear(&self) {
        self.store.write().unwrap().clear();
    }
}

/// The package-level `csiDrivers` (`csi_plugin.go:71`).
pub fn csi_drivers() -> &'static DriversStore {
    static STORE: OnceLock<DriversStore> = OnceLock::new();
    STORE.get_or_init(DriversStore::new)
}
