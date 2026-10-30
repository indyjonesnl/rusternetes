//! Unit tests moved from `src/pluginmanager/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_kubelet::pluginmanager::cache::{
        ActualStateOfWorld, DesiredStateOfWorld, PluginHandler,
    };
    pub use rusternetes_kubelet::pluginmanager::operation::OperationExecutor;
    pub use rusternetes_kubelet::pluginmanager::reconciler::Reconciler;
    pub use rusternetes_kubelet::pluginmanager::watcher::Watcher;
    pub use std::path::PathBuf;
    pub use std::sync::Arc;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use crate::csi_fake::FakeDriver;
    use rusternetes_common::resources::Node;
    use rusternetes_kubelet::pluginmanager::csi_handler::{RegistrationHandler, CSI_PLUGIN};
    use rusternetes_kubelet::pluginmanager::operation::registerapi::registration_server::{
        Registration, RegistrationServer,
    };
    use rusternetes_kubelet::pluginmanager::operation::registerapi::{
        InfoRequest, PluginInfo, RegistrationStatus, RegistrationStatusResponse,
    };
    use rusternetes_kubelet::pluginmanager::*;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::node_server::NodeServer;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::NodeGetInfoResponse;
    use rusternetes_kubelet::volume_plugins::csi_drivers_store::DriversStore;
    use rusternetes_kubelet::volume_plugins::nodeinfomanager::{
        NodeInfoManager, ANNOTATION_KEY_NODE_ID,
    };
    use rusternetes_storage::{build_key, MemoryStorage, Storage};
    use std::path::Path;
    use std::sync::Mutex;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::UnixListenerStream;
    use tonic::{Request, Response, Status};

    /// The plugin side of the handshake: what a `node-driver-registrar`
    /// sidecar serves (`pluginwatcher/example_plugin.go`).
    struct FakeRegistrar {
        info: PluginInfo,
        statuses: Arc<Mutex<Vec<RegistrationStatus>>>,
    }

    #[tonic::async_trait]
    impl Registration for FakeRegistrar {
        async fn get_info(&self, _: Request<InfoRequest>) -> Result<Response<PluginInfo>, Status> {
            Ok(Response::new(self.info.clone()))
        }
        async fn notify_registration_status(
            &self,
            req: Request<RegistrationStatus>,
        ) -> Result<Response<RegistrationStatusResponse>, Status> {
            self.statuses.lock().unwrap().push(req.into_inner());
            Ok(Response::new(RegistrationStatusResponse {}))
        }
    }

    struct Registrar {
        statuses: Arc<Mutex<Vec<RegistrationStatus>>>,
        shutdown: Option<oneshot::Sender<()>>,
    }

    impl Registrar {
        /// Serve `Registration` on `socket` (creating it) like the sidecar.
        fn serve(socket: &Path, plugin_type: &str, name: &str, versions: &[&str]) -> Registrar {
            let statuses = Arc::new(Mutex::new(Vec::new()));
            // The same socket also answers the CSI Node service, as a driver
            // sidecar pair would (`NodeGetInfo` is called on the endpoint).
            let driver = FakeDriver::default();
            *driver.node_info.lock().unwrap() = Some(Ok(NodeGetInfoResponse {
                node_id: format!("{name}-node"),
                max_volumes_per_node: 0,
                accessible_topology: None,
            }));
            let node_svc = NodeServer::new(driver);
            let svc = RegistrationServer::new(FakeRegistrar {
                info: PluginInfo {
                    r#type: plugin_type.to_string(),
                    name: name.to_string(),
                    endpoint: String::new(),
                    supported_versions: versions.iter().map(|v| v.to_string()).collect(),
                },
                statuses: statuses.clone(),
            });
            let listener = tokio::net::UnixListener::bind(socket).unwrap();
            let (tx, rx) = oneshot::channel();
            tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(svc)
                    .add_service(node_svc)
                    .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                        let _ = rx.await;
                    })
                    .await;
            });
            Registrar {
                statuses,
                shutdown: Some(tx),
            }
        }

        fn stop(&mut self, socket: &Path) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
            let _ = std::fs::remove_file(socket);
        }
    }

    async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    async fn manager(
        dir: &Path,
    ) -> (
        PluginManager,
        &'static DriversStore,
        tokio::sync::watch::Sender<bool>,
    ) {
        let (pm, store, stop, _storage) = manager_with_node(dir).await;
        (pm, store, stop)
    }

    /// [`manager`], plus the storage holding the Node the handler records the
    /// CSI node info on.
    async fn manager_with_node(
        dir: &Path,
    ) -> (
        PluginManager,
        &'static DriversStore,
        tokio::sync::watch::Sender<bool>,
        Arc<MemoryStorage>,
    ) {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(&build_key("nodes", None, "node1"), &Node::new("node1"))
            .await
            .unwrap();
        let (pm, store, stop) = manager_with_storage(dir, storage.clone());
        (pm, store, stop, storage)
    }

    /// A manager whose handler records node info on the Node in `storage`
    /// (the NodeInfoManager works on `node1`; a missing Node just makes the
    /// install fail, which the tests that don't care never look at).
    fn manager_with_storage(
        dir: &Path,
        storage: Arc<MemoryStorage>,
    ) -> (
        PluginManager,
        &'static DriversStore,
        tokio::sync::watch::Sender<bool>,
    ) {
        let nim = Arc::new(NodeInfoManager::new("node1", storage));
        let store: &'static DriversStore = Box::leak(Box::new(DriversStore::new()));
        let pm = PluginManager::with_loop_sleep(dir, Duration::from_millis(50));
        pm.add_handler(
            CSI_PLUGIN,
            Arc::new(RegistrationHandler::with_store(store, nim)),
        );
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        pm.run(stop_rx).unwrap();
        (pm, store, stop_tx)
    }

    async fn nodeid_annotation(storage: &MemoryStorage) -> Option<String> {
        let n: Node = storage
            .get(&build_key("nodes", None, "node1"))
            .await
            .unwrap();
        n.metadata
            .annotations
            .and_then(|a| a.get(ANNOTATION_KEY_NODE_ID).cloned())
    }

    /// The issue's acceptance: an unmodified registrar's socket in
    /// `plugins_registry` ends with the driver in the CSI driver store and a
    /// `plugin_registered = true` notification; deleting the socket removes it.
    #[tokio::test]
    async fn csi_registrar_handshake_populates_driver_store() {
        let dir = tempfile::tempdir().unwrap();
        let (pm, store, _stop, node_storage) = manager_with_node(dir.path()).await;
        let sock = dir.path().join("csi.example.com-reg.sock");
        let mut plugin = Registrar::serve(&sock, CSI_PLUGIN, "csi.example.com", &["1.0.0"]);

        eventually("driver registered", || {
            store.get("csi.example.com").is_some()
        })
        .await;
        let d = store.get("csi.example.com").unwrap();
        // GetInfo reported no endpoint, so the socket itself is the endpoint
        // (`operation_generator.go:107-109`).
        assert_eq!(d.endpoint, sock.to_string_lossy());
        eventually("registered notification", || {
            plugin
                .statuses
                .lock()
                .unwrap()
                .iter()
                .any(|s| s.plugin_registered)
        })
        .await;
        assert_eq!(pm.actual_state_of_world.get_registered_plugins().len(), 1);
        // `NodeGetInfo` ran and `InstallCSIDriver` recorded it on the Node.
        assert_eq!(
            nodeid_annotation(&node_storage).await.as_deref(),
            Some(r#"{"csi.example.com":"csi.example.com-node"}"#)
        );

        plugin.stop(&sock);
        eventually("driver deregistered", || {
            store.get("csi.example.com").is_none()
        })
        .await;
        // `DeRegisterPlugin` -> `UninstallCSIDriver` removed it again.
        for _ in 0..100 {
            if nodeid_annotation(&node_storage).await.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(nodeid_annotation(&node_storage).await, None);
        eventually("asw emptied", || {
            pm.actual_state_of_world.get_registered_plugins().is_empty()
        })
        .await;
    }

    /// `ValidatePlugin` failure: the plugin is told `plugin_registered=false`
    /// with the validation error, and nothing is registered
    /// (`operation_generator.go:116-122`).
    #[tokio::test]
    async fn unsupported_version_is_notified_and_not_registered() {
        let dir = tempfile::tempdir().unwrap();
        let (pm, store, _stop) = manager(dir.path()).await;
        let sock = dir.path().join("old.sock");
        let plugin = Registrar::serve(&sock, CSI_PLUGIN, "csi.old.com", &["0.3.0"]);

        eventually("failure notified", || {
            !plugin.statuses.lock().unwrap().is_empty()
        })
        .await;
        let st = plugin.statuses.lock().unwrap()[0].clone();
        assert!(!st.plugin_registered);
        assert!(
            st.error.contains("plugin validation failed"),
            "{}",
            st.error
        );
        assert!(store.get("csi.old.com").is_none());
        assert!(pm.actual_state_of_world.get_registered_plugins().is_empty());
    }

    /// A plugin type with no handler is notified of the failure
    /// (`operation_generator.go:96-102`).
    #[tokio::test]
    async fn unknown_plugin_type_is_notified() {
        let dir = tempfile::tempdir().unwrap();
        let (_pm, store, _stop) = manager(dir.path()).await;
        let sock = dir.path().join("dev.sock");
        let plugin = Registrar::serve(&sock, "DevicePlugin", "vendor.com/gpu", &["v1beta1"]);

        eventually("failure notified", || {
            !plugin.statuses.lock().unwrap().is_empty()
        })
        .await;
        let st = plugin.statuses.lock().unwrap()[0].clone();
        assert!(!st.plugin_registered);
        assert!(
            st.error
                .contains("no handler registered for plugin type: DevicePlugin"),
            "{}",
            st.error
        );
        assert!(store.get("vendor.com/gpu").is_none());
    }

    /// `TestPluginReRegistration` through the reconciler: a plugin update
    /// (socket re-created, new version) deregisters the old driver and
    /// registers the new one.
    #[tokio::test]
    async fn recreated_socket_reregisters_with_new_version() {
        let dir = tempfile::tempdir().unwrap();
        let (_pm, store, _stop) = manager(dir.path()).await;
        let sock = dir.path().join("csi.sock");
        let mut v1 = Registrar::serve(&sock, CSI_PLUGIN, "csi.example.com", &["1.0.0"]);
        eventually("v1 registered", || {
            store
                .get("csi.example.com")
                .is_some_and(|d| d.highest_supported_version == "1.0.0")
        })
        .await;

        v1.stop(&sock);
        let _v2 = Registrar::serve(&sock, CSI_PLUGIN, "csi.example.com", &["1.1.0"]);
        eventually("v2 registered", || {
            store
                .get("csi.example.com")
                .is_some_and(|d| d.highest_supported_version == "1.1.0")
        })
        .await;
    }
}
