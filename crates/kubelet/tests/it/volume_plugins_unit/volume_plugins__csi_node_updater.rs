//! Unit tests moved from `src/volume_plugins/csi_node_updater.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::CSIDriver;
    pub use rusternetes_kubelet::pluginmanager::csi_handler::csi_driver_client_for;
    pub use rusternetes_kubelet::volume_plugins::csi_client::CSI_TIMEOUT;
    pub use rusternetes_kubelet::volume_plugins::csi_drivers_store::DriversStore;
    pub use rusternetes_kubelet::volume_plugins::nodeinfomanager::NodeInfoInstaller;
    pub use rusternetes_storage::{build_key, build_prefix, Storage, WatchEvent};
    pub use std::collections::HashMap;
    pub use std::sync::atomic::{AtomicBool, Ordering};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tokio::sync::oneshot;
}

mod tests {
    use super::__parent::*;
    use crate::csi_fake;
    use async_trait::async_trait;
    use csi_fake::FakeDriver;
    use rusternetes_common::resources::CSIDriverSpec;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::NodeGetInfoResponse;
    use rusternetes_kubelet::volume_plugins::csi_drivers_store::Driver;
    use rusternetes_kubelet::volume_plugins::csi_node_updater::*;
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
