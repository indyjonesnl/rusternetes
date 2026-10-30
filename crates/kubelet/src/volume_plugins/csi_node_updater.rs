//! Port of `csiNodeUpdater` (`pkg/volume/csi/csi_node_updater.go`) and of
//! `updateCSIDriver` (`pkg/volume/csi/csi_plugin.go:172-190`): while a CSI
//! driver is registered on this node and its `CSIDriver` object sets
//! `spec.nodeAllocatableUpdatePeriodSeconds`, call the driver's `NodeGetInfo`
//! on that period and write the result to the `CSINode`
//! (`nodeinfomanager.UpdateCSIDriver`), so `allocatable.count` follows the
//! driver (gate `MutableCSINodeAllocatableCount`, `csi_plugin.go:417`).
//!
//! Upstream drives the updater from a `CSIDriver` shared informer and reads the
//! driver out of the informer's store. This kubelet has no informer layer; the
//! `CSIDriver` is read from storage (the source the informer mirrors) and the
//! add/update/delete events come from a storage watch with an initial list
//! (what an informer's `AddEventHandler` replays).

use crate::pluginmanager::csi_handler::csi_driver_client_for;
use crate::volume_plugins::csi_client::CSI_TIMEOUT;
use crate::volume_plugins::csi_drivers_store::DriversStore;
use crate::volume_plugins::nodeinfomanager::NodeInfoInstaller;
use futures::StreamExt;
use rusternetes_common::resources::CSIDriver;
use rusternetes_storage::{build_key, build_prefix, Storage, WatchEvent};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

/// `getNodeAllocatableUpdatePeriod` (`csi_node_updater.go:194-199`).
pub fn get_node_allocatable_update_period(driver: Option<&CSIDriver>) -> Duration {
    match driver.and_then(|d| d.spec.node_allocatable_update_period_seconds) {
        // `time.Duration(*p) * time.Second`; a negative value is rejected by
        // validation (`csidriver.go` min 10), treat it as unset.
        Some(p) if p > 0 => Duration::from_secs(p as u64),
        _ => Duration::ZERO,
    }
}

/// `updateCSIDriver` (`csi_plugin.go:172-190`): `NodeGetInfo` against the
/// registered driver, then `nim.UpdateCSIDriver`.
pub async fn update_csi_driver(
    drivers: &DriversStore,
    nim: &dyn NodeInfoInstaller,
    plugin_name: &str,
) -> Result<(), String> {
    let csi = csi_driver_client_for(drivers, plugin_name)
        .map_err(|e| format!("failed to create CSI client for driver {plugin_name:?}: {e}"))?;
    let info = match tokio::time::timeout(CSI_TIMEOUT, csi.node_get_info()).await {
        Ok(Ok(info)) => info,
        Ok(Err(e)) => {
            return Err(format!(
                "failed to get NodeGetInfo from driver {plugin_name:?}: {e}"
            ))
        }
        Err(_) => {
            return Err(format!(
                "failed to get NodeGetInfo from driver {plugin_name:?}: context deadline exceeded"
            ))
        }
    };
    nim.update_csi_driver(
        plugin_name,
        &info.node_id,
        info.max_volumes_per_node,
        &info.accessible_topology,
    )
    .await
    .map_err(|e| format!("failed to update driver {plugin_name:?}: {e}"))
}

/// `csiNodeUpdater` (`csi_node_updater.go:30-41`).
pub struct CsiNodeUpdater<S: Storage> {
    pub storage: Arc<S>,
    /// Upstream's package-level `csiDrivers`.
    pub drivers: &'static DriversStore,
    /// Upstream's package-level `nim`.
    pub nim: Arc<dyn NodeInfoInstaller>,
    /// `driverUpdaters`: driver name -> stop signal of its update goroutine.
    /// Dropping the sender is `close(stopCh)`.
    pub driver_updaters: Mutex<HashMap<String, oneshot::Sender<()>>>,
    /// `once sync.Once`.
    pub started: AtomicBool,
}

impl<S: Storage + 'static> CsiNodeUpdater<S> {
    /// `NewCSINodeUpdater` (`csi_node_updater.go:44-52`); the informer
    /// argument is the storage the `CSIDriver` objects are read from.
    pub fn new(
        storage: Arc<S>,
        drivers: &'static DriversStore,
        nim: Arc<dyn NodeInfoInstaller>,
    ) -> Self {
        Self {
            storage,
            drivers,
            nim,
            driver_updaters: Mutex::new(HashMap::new()),
            started: AtomicBool::new(false),
        }
    }

    /// `Run` (`csi_node_updater.go:55-68`): start delivering `CSIDriver`
    /// events; only the first call does anything. Never returns.
    pub async fn run(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        tracing::debug!("csiNodeUpdater initialized successfully");
        let prefix = build_prefix("csidrivers", None);
        // What the informer's cache knew before each event, so an update can
        // be compared to its predecessor (`UpdateFunc(oldObj, newObj)`).
        let mut known: HashMap<String, CSIDriver> = HashMap::new();
        loop {
            // Watch first, then list: an event in between is replayed, and
            // every handler is idempotent.
            match self.storage.watch(&prefix).await {
                Ok(mut stream) => {
                    self.resync(&prefix, &mut known).await;
                    while let Some(ev) = stream.next().await {
                        match ev {
                            Ok(WatchEvent::Added(_, v) | WatchEvent::Modified(_, v)) => {
                                if let Ok(d) = serde_json::from_str::<CSIDriver>(&v) {
                                    self.apply(&mut known, d).await;
                                }
                            }
                            Ok(WatchEvent::Deleted(_, v)) => {
                                if let Ok(d) = serde_json::from_str::<CSIDriver>(&v) {
                                    known.remove(&d.metadata.name);
                                    self.on_driver_delete(&d).await;
                                }
                            }
                            Err(e) => {
                                tracing::debug!("CSIDriver watch error: {e}");
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

    /// Replay the current `CSIDriver` list against what was last seen: the
    /// informer's re-list on a watch restart.
    async fn resync(&self, prefix: &str, known: &mut HashMap<String, CSIDriver>) {
        let Ok(list) = self.storage.list::<CSIDriver>(prefix).await else {
            return;
        };
        let present: std::collections::HashSet<String> =
            list.iter().map(|d| d.metadata.name.clone()).collect();
        let gone: Vec<String> = known
            .keys()
            .filter(|n| !present.contains(*n))
            .cloned()
            .collect();
        for n in gone {
            if let Some(d) = known.remove(&n) {
                self.on_driver_delete(&d).await;
            }
        }
        for d in list {
            self.apply(known, d).await;
        }
    }

    async fn apply(&self, known: &mut HashMap<String, CSIDriver>, d: CSIDriver) {
        match known.insert(d.metadata.name.clone(), d.clone()) {
            Some(old) => self.on_driver_update(&old, &d).await,
            None => self.on_driver_add(&d).await,
        }
    }

    /// `onDriverAdd` (`csi_node_updater.go:71-79`).
    pub async fn on_driver_add(&self, driver: &CSIDriver) {
        tracing::trace!("onDriverAdd event driver={}", driver.metadata.name);
        self.sync_driver_updater(&driver.metadata.name).await;
    }

    /// `onDriverUpdate` (`csi_node_updater.go:82-102`): only a change of
    /// `NodeAllocatableUpdatePeriodSeconds` reconfigures.
    pub async fn on_driver_update(&self, old: &CSIDriver, new: &CSIDriver) {
        let old_period = get_node_allocatable_update_period(Some(old));
        let new_period = get_node_allocatable_update_period(Some(new));
        if old_period != new_period {
            tracing::debug!(
                "NodeAllocatableUpdatePeriodSeconds updated driver={} oldPeriod={old_period:?} newPeriod={new_period:?}",
                new.metadata.name
            );
            self.sync_driver_updater(&new.metadata.name).await;
        }
    }

    /// `onDriverDelete` (`csi_node_updater.go:105-113`).
    pub async fn on_driver_delete(&self, driver: &CSIDriver) {
        tracing::trace!("onDriverDelete event driver={}", driver.metadata.name);
        self.sync_driver_updater(&driver.metadata.name).await;
    }

    /// `syncDriverUpdater` (`csi_node_updater.go:118-171`): re-evaluate whether
    /// the periodic updater for `driver_name` should run. Called from the
    /// events above and from plugin (de)registration
    /// (`csi_plugin.go:165-167`, `:276-278`).
    pub async fn sync_driver_updater(&self, driver_name: &str) {
        // Check if the CSI plugin is installed on this node.
        if self.drivers.get(driver_name).is_none() {
            tracing::debug!("Driver not installed; stopping csiNodeUpdater driver={driver_name}");
            self.unregister_driver(driver_name);
            return;
        }
        // Get the CSIDriver object (upstream: the informer's store).
        let driver = match self
            .storage
            .get::<CSIDriver>(&build_key("csidrivers", None, driver_name))
            .await
        {
            Ok(d) => d,
            Err(rusternetes_common::Error::NotFound(_)) => {
                tracing::info!(
                    "CSIDriver object not found; stopping csiNodeUpdater driver={driver_name}"
                );
                self.unregister_driver(driver_name);
                return;
            }
            Err(e) => {
                self.unregister_driver(driver_name);
                tracing::error!("Error retrieving CSIDriver from store driver={driver_name}: {e}");
                return;
            }
        };
        let period = get_node_allocatable_update_period(Some(&driver));
        if period.is_zero() {
            tracing::trace!(
                "NodeAllocatableUpdatePeriodSeconds is not configured; disabling updates driver={driver_name}"
            );
            self.unregister_driver(driver_name);
            return;
        }

        let (stop_tx, stop_rx) = oneshot::channel();
        // If an updater is already running, stop it so we can reconfigure
        // (dropping the old sender is `close(prevStopCh)`).
        drop(
            self.driver_updaters
                .lock()
                .unwrap()
                .insert(driver_name.to_string(), stop_tx),
        );
        // Start the periodic update goroutine.
        tokio::spawn(run_periodic_update(
            self.drivers,
            self.nim.clone(),
            driver_name.to_string(),
            period,
            stop_rx,
        ));
    }

    /// `unregisterDriver` (`csi_node_updater.go:174-181`): stop any running
    /// periodic update goroutine for the driver.
    pub fn unregister_driver(&self, driver_name: &str) {
        drop(self.driver_updaters.lock().unwrap().remove(driver_name));
    }

    /// Whether an update goroutine is registered for `driver_name`
    /// (`verifyUpdaterState`, `csi_node_updater_test.go`).
    pub fn has_updater(&self, driver_name: &str) -> bool {
        self.driver_updaters
            .lock()
            .unwrap()
            .contains_key(driver_name)
    }
}

/// `runPeriodicUpdate` (`csi_node_updater.go:184-199`).
async fn run_periodic_update(
    drivers: &'static DriversStore,
    nim: Arc<dyn NodeInfoInstaller>,
    driver_name: String,
    period: Duration,
    mut stop: oneshot::Receiver<()>,
) {
    // `time.NewTicker(period)`: the first tick is one period away.
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tracing::trace!("Starting periodic updates for driver {driver_name} period={period:?}");
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(e) = update_csi_driver(drivers, nim.as_ref(), &driver_name).await {
                    tracing::error!("Failed to update CSIDriver driver={driver_name}: {e}");
                }
            }
            // The sender is dropped (or fired) when the updater is replaced
            // or unregistered.
            _ = &mut stop => {
                tracing::debug!("Stopping periodic updates for driver {driver_name} period={period:?}");
                return;
            }
        }
    }
}
