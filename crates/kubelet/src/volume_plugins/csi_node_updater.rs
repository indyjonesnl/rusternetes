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
    storage: Arc<S>,
    /// Upstream's package-level `csiDrivers`.
    drivers: &'static DriversStore,
    /// Upstream's package-level `nim`.
    nim: Arc<dyn NodeInfoInstaller>,
    /// `driverUpdaters`: driver name -> stop signal of its update goroutine.
    /// Dropping the sender is `close(stopCh)`.
    driver_updaters: Mutex<HashMap<String, oneshot::Sender<()>>>,
    /// `once sync.Once`.
    started: AtomicBool,
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
    fn unregister_driver(&self, driver_name: &str) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_plugins::csi_client::fake as csi_fake;
    use crate::volume_plugins::csi_client::proto::NodeGetInfoResponse;
    use crate::volume_plugins::csi_drivers_store::Driver;
    use async_trait::async_trait;
    use csi_fake::FakeDriver;
    use rusternetes_common::resources::CSIDriverSpec;
    use rusternetes_storage::MemoryStorage;

    const TEST_DRIVER: &str = "test-driver";

    fn store() -> &'static DriversStore {
        Box::leak(Box::new(DriversStore::new()))
    }

    #[derive(Default)]
    struct Recorder {
        updates: Mutex<Vec<(String, String, i64)>>,
    }

    #[async_trait]
    impl NodeInfoInstaller for Recorder {
        async fn install_csi_driver(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: &HashMap<String, String>,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn update_csi_driver(
            &self,
            driver_name: &str,
            driver_node_id: &str,
            max_attach_limit: i64,
            _: &HashMap<String, String>,
        ) -> Result<(), String> {
            self.updates.lock().unwrap().push((
                driver_name.into(),
                driver_node_id.into(),
                max_attach_limit,
            ));
            Ok(())
        }
        async fn uninstall_csi_driver(&self, _: &str) -> Result<(), String> {
            Ok(())
        }
    }

    fn csi_driver(name: &str, period: Option<i64>) -> CSIDriver {
        let mut d = CSIDriver {
            type_meta: Default::default(),
            metadata: rusternetes_common::types::ObjectMeta::new(name),
            spec: CSIDriverSpec::default(),
        };
        d.spec.node_allocatable_update_period_seconds = period;
        d
    }

    async fn fixture(
        driver: Option<CSIDriver>,
    ) -> (
        Arc<MemoryStorage>,
        &'static DriversStore,
        Arc<Recorder>,
        CsiNodeUpdater<MemoryStorage>,
    ) {
        let st = Arc::new(MemoryStorage::new());
        if let Some(d) = driver {
            st.create(&build_key("csidrivers", None, &d.metadata.name), &d)
                .await
                .unwrap();
        }
        let drivers = store();
        let rec = Arc::new(Recorder::default());
        let u = CsiNodeUpdater::new(st.clone(), drivers, rec.clone());
        (st, drivers, rec, u)
    }

    fn register(drivers: &DriversStore, endpoint: &str) {
        drivers.set(
            TEST_DRIVER,
            Driver {
                endpoint: endpoint.into(),
                highest_supported_version: "1.0.0".into(),
            },
        );
    }

    /// Plant a stop channel the way `driverUpdaters.Store(name, stopCh)` does;
    /// the returned receiver reports whether it was "closed".
    fn plant<S: Storage + 'static>(u: &CsiNodeUpdater<S>) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        u.driver_updaters
            .lock()
            .unwrap()
            .insert(TEST_DRIVER.into(), tx);
        rx
    }

    fn is_closed(rx: &mut oneshot::Receiver<()>) -> bool {
        matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed))
    }

    /// `TestSyncDriverUpdater` "driver not installed, should stop updater"
    /// (`csi_node_updater_test.go:99-118`).
    #[tokio::test]
    async fn driver_not_installed_stops_the_updater() {
        let (_st, _d, _r, u) = fixture(None).await;
        let mut rx = plant(&u);
        assert!(u.has_updater(TEST_DRIVER));
        u.sync_driver_updater(TEST_DRIVER).await;
        assert!(!u.has_updater(TEST_DRIVER));
        assert!(is_closed(&mut rx), "stop channel was not closed");
    }

    /// "driver not found in informer, should stop updater" (`:120-143`).
    #[tokio::test]
    async fn csidriver_object_missing_stops_the_updater() {
        let (_st, drivers, _r, u) = fixture(None).await;
        register(drivers, "unused");
        let mut rx = plant(&u);
        u.sync_driver_updater(TEST_DRIVER).await;
        assert!(!u.has_updater(TEST_DRIVER));
        assert!(is_closed(&mut rx));
    }

    /// "driver with unset updatePeriodSeconds, should stop updater"
    /// (`:145-168`).
    #[tokio::test]
    async fn unset_period_stops_the_updater() {
        let (_st, drivers, _r, u) = fixture(Some(csi_driver(TEST_DRIVER, None))).await;
        register(drivers, "unused");
        let mut rx = plant(&u);
        u.sync_driver_updater(TEST_DRIVER).await;
        assert!(!u.has_updater(TEST_DRIVER));
        assert!(is_closed(&mut rx));
    }

    /// "replace existing updater" (`:170-208`): the previous stop channel is
    /// closed and a new one registered.
    #[tokio::test]
    async fn sync_replaces_an_existing_updater() {
        let (_st, drivers, _r, u) = fixture(Some(csi_driver(TEST_DRIVER, Some(60)))).await;
        register(drivers, "unused");
        let mut old = plant(&u);
        u.sync_driver_updater(TEST_DRIVER).await;
        assert!(is_closed(&mut old), "previous stop channel not closed");
        assert!(u.has_updater(TEST_DRIVER), "no updater after replacement");
        u.unregister_driver(TEST_DRIVER);
    }

    /// `getNodeAllocatableUpdatePeriod` (`csi_node_updater.go:194-199`).
    #[test]
    fn update_period_helper() {
        assert_eq!(get_node_allocatable_update_period(None), Duration::ZERO);
        assert_eq!(
            get_node_allocatable_update_period(Some(&csi_driver("d", None))),
            Duration::ZERO
        );
        assert_eq!(
            get_node_allocatable_update_period(Some(&csi_driver("d", Some(10)))),
            Duration::from_secs(10)
        );
    }

    /// `onDriverUpdate` (`csi_node_updater.go:82-102`): only a period change
    /// reconfigures; an unrelated update leaves the running updater alone.
    #[tokio::test]
    async fn update_event_reconfigures_only_on_period_change() {
        let (_st, drivers, _r, u) = fixture(Some(csi_driver(TEST_DRIVER, Some(60)))).await;
        register(drivers, "unused");
        let mut rx = plant(&u);
        let same = csi_driver(TEST_DRIVER, Some(60));
        u.on_driver_update(&same, &same).await;
        assert!(!is_closed(&mut rx), "unchanged period must not restart");
        u.on_driver_update(&csi_driver(TEST_DRIVER, Some(30)), &same)
            .await;
        assert!(is_closed(&mut rx), "changed period must restart");
        u.unregister_driver(TEST_DRIVER);
    }

    fn driver_socket() -> String {
        let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let sock = dir.path().join("csi.sock");
        let d = FakeDriver::default();
        *d.node_info.lock().unwrap() = Some(Ok(NodeGetInfoResponse {
            node_id: "n1".into(),
            max_volumes_per_node: 7,
            accessible_topology: None,
        }));
        std::mem::forget(csi_fake::serve(d, &sock));
        sock.to_string_lossy().into_owned()
    }

    /// The periodic loop (`runPeriodicUpdate`): every period the driver's
    /// `NodeGetInfo` reaches `UpdateCSIDriver`; it stops once unregistered.
    #[tokio::test]
    async fn periodic_update_calls_update_csi_driver_until_stopped() {
        let (_st, drivers, rec, u) = fixture(Some(csi_driver(TEST_DRIVER, Some(1)))).await;
        register(drivers, &driver_socket());
        u.sync_driver_updater(TEST_DRIVER).await;
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let n = rec.updates.lock().unwrap().len();
        assert!(n >= 1, "expected periodic updates, got {n}");
        assert_eq!(
            rec.updates.lock().unwrap()[0],
            (TEST_DRIVER.into(), "n1".into(), 7)
        );
        // Deregistered: no more updates.
        drivers.delete(TEST_DRIVER);
        u.sync_driver_updater(TEST_DRIVER).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let after = rec.updates.lock().unwrap().len();
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert_eq!(rec.updates.lock().unwrap().len(), after);
    }

    /// `updateCSIDriver` error wrapping (`csi_plugin.go:173-188`).
    #[tokio::test]
    async fn update_csi_driver_unknown_driver_errors() {
        let drivers = store();
        let rec = Recorder::default();
        let e = update_csi_driver(drivers, &rec, "nope").await.unwrap_err();
        assert!(
            e.starts_with("failed to create CSI client for driver \"nope\": "),
            "{e}"
        );
    }
}
