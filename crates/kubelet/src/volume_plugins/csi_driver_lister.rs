//! The `CSIDriverLister` the CSI plugin reads (`pkg/volume/csi/csi_plugin.go:68`,
//! set from `KubeletVolumeHost.CSIDriverLister()` at `:305`), plus the
//! `csiDriversSynced` gate `kubeletVolumeHost.WaitForCacheSync`
//! (`pkg/kubelet/volume_host.go:175-188`) waits on.
//!
//! Upstream backs the lister with a `CSIDriver` shared informer. This kubelet
//! has no informer layer (see `csi_node_updater.rs`), so the same shape is a
//! store kept current by a storage watch with an initial list. The plugin
//! methods that use it (`RequiresRemount`, `SupportsSELinuxContextMount`) are
//! synchronous, as upstream's are, so the store is read under a std lock.

use rusternetes_common::resources::CSIDriver;
use rusternetes_storage::{build_prefix, Storage, WatchEvent};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Default)]
struct Inner {
    drivers: RwLock<HashMap<String, CSIDriver>>,
    synced: AtomicBool,
}

/// Cheap-to-clone handle on the shared `CSIDriver` store.
#[derive(Clone, Default)]
pub struct CsiDriverLister {
    inner: Arc<Inner>,
}

impl CsiDriverLister {
    pub fn new() -> Self {
        Self::default()
    }

    /// `csiDriversSynced` (`volume_host.go:170-172`).
    pub fn has_synced(&self) -> bool {
        self.inner.synced.load(Ordering::SeqCst)
    }

    pub fn mark_synced(&self) {
        self.inner.synced.store(true, Ordering::SeqCst);
    }

    /// `csiDriverLister.Get`.
    pub fn get(&self, name: &str) -> Option<CSIDriver> {
        self.inner
            .drivers
            .read()
            .ok()
            .and_then(|d| d.get(name).cloned())
    }

    pub fn insert(&self, driver: CSIDriver) {
        if let Ok(mut d) = self.inner.drivers.write() {
            d.insert(driver.metadata.name.clone(), driver);
        }
    }

    fn remove(&self, name: &str) {
        if let Ok(mut d) = self.inner.drivers.write() {
            d.remove(name);
        }
    }

    fn replace_all(&self, list: Vec<CSIDriver>) {
        if let Ok(mut d) = self.inner.drivers.write() {
            *d = list
                .into_iter()
                .map(|x| (x.metadata.name.clone(), x))
                .collect();
        }
    }

    /// Keep the store current: watch, then list (an event in between is
    /// replayed, so nothing is missed), and mark synced after the first list.
    /// Never returns.
    pub async fn run<S: Storage>(&self, storage: Arc<S>) {
        let prefix = build_prefix("csidrivers", None);
        loop {
            match storage.watch(&prefix).await {
                Ok(mut stream) => {
                    if let Ok(list) = storage.list::<CSIDriver>(&prefix).await {
                        self.replace_all(list);
                        self.mark_synced();
                    }
                    use futures::StreamExt;
                    while let Some(ev) = stream.next().await {
                        match ev {
                            Ok(WatchEvent::Added(_, v) | WatchEvent::Modified(_, v)) => {
                                if let Ok(d) = serde_json::from_str::<CSIDriver>(&v) {
                                    self.insert(d);
                                }
                            }
                            Ok(WatchEvent::Deleted(_, v)) => {
                                if let Ok(d) = serde_json::from_str::<CSIDriver>(&v) {
                                    self.remove(&d.metadata.name);
                                }
                            }
                            Err(e) => {
                                tracing::debug!("CSIDriver lister watch error: {e}");
                                break;
                            }
                        }
                    }
                }
                Err(e) => tracing::debug!("Failed to watch CSIDrivers: {e}"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
