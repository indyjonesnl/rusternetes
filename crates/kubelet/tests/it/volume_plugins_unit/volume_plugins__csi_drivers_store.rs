//! Unit tests moved from `src/volume_plugins/csi_drivers_store.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::HashMap;
    pub use std::sync::{OnceLock, RwLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::csi_drivers_store::*;

    fn driver(endpoint: &str) -> Driver {
        Driver {
            endpoint: endpoint.to_string(),
            highest_supported_version: "1.0.0".to_string(),
        }
    }

    /// `TestDriversStore` (`csi_drivers_store_test.go`): set/get/delete/clear.
    #[test]
    fn set_get_delete_clear() {
        let s = DriversStore::new();
        assert!(s.get("a").is_none());
        s.set("a", driver("/a.sock"));
        s.set("b", driver("/b.sock"));
        assert_eq!(s.get("a").unwrap().endpoint, "/a.sock");
        s.delete("a");
        assert!(s.get("a").is_none());
        assert!(s.get("b").is_some());
        s.clear();
        assert!(s.get("b").is_none());
    }

    /// Set replaces an existing entry for the same name.
    #[test]
    fn set_overwrites() {
        let s = DriversStore::new();
        s.set("a", driver("/old.sock"));
        s.set("a", driver("/new.sock"));
        assert_eq!(s.get("a").unwrap().endpoint, "/new.sock");
    }
}
