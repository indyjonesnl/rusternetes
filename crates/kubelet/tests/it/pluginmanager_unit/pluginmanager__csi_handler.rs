//! Unit tests moved from `src/pluginmanager/csi_handler.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_kubelet::pluginmanager::cache::PluginHandler;
    pub use rusternetes_kubelet::volume_plugins::csi_client::{CsiDriverClient, CSI_TIMEOUT};
    pub use rusternetes_kubelet::volume_plugins::csi_drivers_store::{
        csi_drivers, Driver, DriversStore,
    };
    pub use rusternetes_kubelet::volume_plugins::nodeinfomanager::NodeInfoInstaller;
    pub use std::cmp::Ordering;
    pub use std::sync::Arc;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use crate::csi_fake;
    use csi_fake::FakeDriver;
    use rusternetes_kubelet::pluginmanager::csi_handler::*;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::{NodeGetInfoResponse, Topology};
    use std::collections::HashMap;
    use std::sync::Mutex;

    type Install = (String, String, i64, HashMap<String, String>);

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn store() -> &'static DriversStore {
        Box::leak(Box::new(DriversStore::new()))
    }

    /// Records what the handler asks the NodeInfoManager to do.
    #[derive(Default)]
    struct RecordingNim {
        installs: Mutex<Vec<Install>>,
        uninstalls: Mutex<Vec<String>>,
        fail_install: bool,
        fail_uninstall: bool,
    }

    #[async_trait]
    impl NodeInfoInstaller for RecordingNim {
        async fn install_csi_driver(
            &self,
            driver_name: &str,
            driver_node_id: &str,
            max_attach_limit: i64,
            topology: &HashMap<String, String>,
        ) -> Result<(), String> {
            self.installs.lock().unwrap().push((
                driver_name.into(),
                driver_node_id.into(),
                max_attach_limit,
                topology.clone(),
            ));
            if self.fail_install {
                return Err("install failed".into());
            }
            Ok(())
        }
        async fn update_csi_driver(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: &HashMap<String, String>,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn uninstall_csi_driver(&self, driver_name: &str) -> Result<(), String> {
            self.uninstalls.lock().unwrap().push(driver_name.into());
            if self.fail_uninstall {
                return Err("uninstall failed".into());
            }
            Ok(())
        }
    }

    /// A fake CSI driver answering `NodeGetInfo` on a unix socket; returns the
    /// socket path (the temp dir is leaked for the test's lifetime).
    fn driver_socket(info: Option<Result<NodeGetInfoResponse, tonic::Code>>) -> String {
        let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let sock = dir.path().join("csi.sock");
        let d = FakeDriver::default();
        *d.node_info.lock().unwrap() = info;
        std::mem::forget(csi_fake::serve(d, &sock));
        sock.to_string_lossy().into_owned()
    }

    fn node_info(id: &str) -> NodeGetInfoResponse {
        NodeGetInfoResponse {
            node_id: id.into(),
            max_volumes_per_node: 7,
            accessible_topology: Some(Topology {
                segments: HashMap::from([("topology.example.com/zone".into(), "z1".into())]),
            }),
        }
    }

    /// `TestHighestSupportedVersion` (`version_test.go:350-440`), every row.
    #[test]
    fn highest_supported_version_table() {
        let cases: &[(&[&str], Option<&str>)] = &[
            (&["v1.0.0"], Some("1.0.0")),
            (&["0.3.0"], None),
            (&["0.2.0"], None),
            (&["1.0.0"], Some("1.0.0")),
            (&["v0.3.0"], None),
            (&["v0.2.0"], None),
            (&["0.2.0", "v0.3.0"], None),
            (&["0.2.0", "v1.0.0"], Some("1.0.0")),
            (&["0.2.0", "v1.2.3"], Some("1.2.3")),
            (&["v1.2.3", "v0.3.0"], Some("1.2.3")),
            (&["v1.2.3", "v0.3.0", "2.0.1"], Some("1.2.3")),
            (&["v1.2.3", "4.9.12", "v0.3.0", "2.0.1"], Some("1.2.3")),
            (&["4.9.12", "2.0.1"], None),
            (&["v1.2.3", "boo", "v0.3.0", "2.0.1"], Some("1.2.3")),
            (&[], None),
            (&["var", "boo", "foo"], None),
        ];
        for (versions, want) in cases {
            let got = highest_supported_version(&s(versions));
            match want {
                Some(w) => assert_eq!(got.unwrap().to_string(), *w, "{versions:?}"),
                None => assert!(got.is_err(), "{versions:?} should fail"),
            }
        }
    }

    /// `TestValidatePlugin` (`csi_plugin_test.go:1268`): same table through the
    /// handler (fresh store, so no existing driver).
    #[test]
    fn validate_plugin_table() {
        let h = RegistrationHandler::with_store(store(), Arc::new(RecordingNim::default()));
        let ok = |v: &[&str]| h.validate_plugin("test.plugin", "/csi.sock", &s(v)).is_ok();
        assert!(ok(&["v1.0.0"]));
        assert!(!ok(&["0.3.0"]));
        assert!(!ok(&["0.2.0", "v0.3.0"]));
        assert!(ok(&["0.2.0", "v1.0.0"]));
        assert!(ok(&["v1.2.3", "boo", "v0.3.0", "2.0.1"]));
        assert!(!ok(&["4.9.12", "2.0.1"]));
        assert!(!ok(&[]));
        assert!(!ok(&["var", "boo", "foo"]));
    }

    /// `TestValidatePluginExistingDriver` (`csi_plugin_test.go:1369`).
    #[test]
    fn validate_plugin_existing_driver() {
        let seed = |name: &str, versions: &[&str]| {
            let st = store();
            st.set(
                name,
                Driver {
                    endpoint: "/old.sock".into(),
                    highest_supported_version: highest_supported_version(&s(versions))
                        .unwrap()
                        .to_string(),
                },
            );
            RegistrationHandler::with_store(st, Arc::new(RecordingNim::default()))
        };
        // Different name: fine.
        let h = seed("test.plugin", &["v1.0.0"]);
        assert!(h
            .validate_plugin("test.plugin2", "/c.sock", &s(&["v1.0.0"]))
            .is_ok());
        // Same name, same version: refused.
        let h = seed("test.plugin", &["v1.0.0"]);
        assert!(h
            .validate_plugin("test.plugin", "/c.sock", &s(&["v1.0.0"]))
            .is_err());
        // Same name, higher version: allowed.
        let h = seed("test.plugin", &["v0.3.0", "v0.2.0", "v1.0.0"]);
        assert!(h
            .validate_plugin("test.plugin", "/c.sock", &s(&["v1.0.1"]))
            .is_ok());
    }

    /// `RegisterPlugin` (`csi_plugin.go:134-177`): after `csiDrivers.Set` it
    /// calls `NodeGetInfo` and hands the node id, max volumes and topology
    /// segments to `InstallCSIDriver`; DeRegister clears the store and calls
    /// `UninstallCSIDriver` (`unregisterDriver`, `csi_plugin.go:962-970`).
    #[tokio::test]
    async fn register_installs_node_info_and_deregister_uninstalls() {
        let st = store();
        let nim = Arc::new(RecordingNim::default());
        let h = RegistrationHandler::with_store(st, nim.clone());
        let ep = driver_socket(Some(Ok(node_info("csi-node-1"))));
        h.register_plugin("csi.example.com", &ep, &s(&["v1.2.3"]), None)
            .await
            .unwrap();
        let d = st.get("csi.example.com").expect("driver registered");
        assert_eq!(d.endpoint, ep);
        assert_eq!(d.highest_supported_version, "1.2.3");
        {
            let installs = nim.installs.lock().unwrap();
            assert_eq!(installs.len(), 1);
            assert_eq!(installs[0].0, "csi.example.com");
            assert_eq!(installs[0].1, "csi-node-1");
            assert_eq!(installs[0].2, 7);
            assert_eq!(installs[0].3["topology.example.com/zone"], "z1");
        }

        h.deregister_plugin("csi.example.com", &ep).await;
        assert!(st.get("csi.example.com").is_none());
        assert_eq!(*nim.uninstalls.lock().unwrap(), vec!["csi.example.com"]);
    }

    /// `NodeGetInfo` failing unregisters the driver again and surfaces the
    /// error (`csi_plugin.go:150-156`); nothing is installed.
    #[tokio::test]
    async fn node_get_info_failure_unregisters_the_driver() {
        let st = store();
        let nim = Arc::new(RecordingNim::default());
        let h = RegistrationHandler::with_store(st, nim.clone());
        let ep = driver_socket(Some(Err(tonic::Code::Unavailable)));
        let err = h
            .register_plugin(
                "csi.example.com",
                &ep,
                &s(&["v1.0.0"]),
                Some(Duration::from_secs(5)),
            )
            .await
            .unwrap_err();
        assert!(!err.is_empty());
        assert!(
            st.get("csi.example.com").is_none(),
            "driver must be unregistered"
        );
        assert!(nim.installs.lock().unwrap().is_empty());
        // unregisterDriver also uninstalls.
        assert_eq!(*nim.uninstalls.lock().unwrap(), vec!["csi.example.com"]);
    }

    /// `InstallCSIDriver` failing unregisters the driver again
    /// (`csi_plugin.go:158-164`).
    #[tokio::test]
    async fn install_failure_unregisters_the_driver() {
        let st = store();
        let nim = Arc::new(RecordingNim {
            fail_install: true,
            ..Default::default()
        });
        let h = RegistrationHandler::with_store(st, nim.clone());
        let ep = driver_socket(Some(Ok(node_info("csi-node-1"))));
        let err = h
            .register_plugin("csi.example.com", &ep, &s(&["v1.0.0"]), None)
            .await
            .unwrap_err();
        assert!(err.contains("install failed"), "{err}");
        assert!(st.get("csi.example.com").is_none());
    }

    /// `unregisterDriver` deletes from the store BEFORE uninstalling, so a
    /// failing uninstall still leaves the store clean (`csi_plugin.go:963-967`).
    #[tokio::test]
    async fn deregister_clears_the_store_even_if_uninstall_fails() {
        let st = store();
        let nim = Arc::new(RecordingNim {
            fail_uninstall: true,
            ..Default::default()
        });
        let h = RegistrationHandler::with_store(st, nim);
        let ep = driver_socket(Some(Ok(node_info("n"))));
        h.register_plugin("csi.example.com", &ep, &s(&["v1.0.0"]), None)
            .await
            .unwrap();
        h.deregister_plugin("csi.example.com", &ep).await;
        assert!(st.get("csi.example.com").is_none());
    }

    #[derive(Default)]
    struct RecordingSync {
        calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl DriverUpdaterSync for RecordingSync {
        async fn sync_driver_updater(&self, driver_name: &str) {
            self.calls.lock().unwrap().push(driver_name.into());
        }
    }

    /// `RegisterPlugin` ends with `csiNodeUpdaterVar.syncDriverUpdater`
    /// (`csi_plugin.go:165-167`) and `DeRegisterPlugin` does the same after
    /// `unregisterDriver` (`csi_plugin.go:276-278`).
    #[tokio::test]
    async fn register_and_deregister_sync_the_node_updater() {
        let st = store();
        let nim = Arc::new(RecordingNim::default());
        let sync = Arc::new(RecordingSync::default());
        let h = RegistrationHandler::with_store(st, nim).with_node_updater(sync.clone());
        let ep = driver_socket(Some(Ok(node_info("csi-node-1"))));
        h.register_plugin("csi.example.com", &ep, &s(&["v1.0.0"]), None)
            .await
            .unwrap();
        assert_eq!(*sync.calls.lock().unwrap(), vec!["csi.example.com"]);
        h.deregister_plugin("csi.example.com", &ep).await;
        assert_eq!(
            *sync.calls.lock().unwrap(),
            vec!["csi.example.com", "csi.example.com"]
        );
    }

    /// The sync sits after the install (`csi_plugin.go:157-167`): a failed
    /// registration returns before reaching it.
    #[tokio::test]
    async fn failed_registration_does_not_sync_the_node_updater() {
        let st = store();
        let nim = Arc::new(RecordingNim {
            fail_install: true,
            ..Default::default()
        });
        let sync = Arc::new(RecordingSync::default());
        let h = RegistrationHandler::with_store(st, nim).with_node_updater(sync.clone());
        let ep = driver_socket(Some(Ok(node_info("csi-node-1"))));
        assert!(h
            .register_plugin("csi.example.com", &ep, &s(&["v1.0.0"]), None)
            .await
            .is_err());
        assert!(sync.calls.lock().unwrap().is_empty());
    }
}
