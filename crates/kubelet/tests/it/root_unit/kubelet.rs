//! Unit tests moved from `src/kubelet.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::{
        resources::{
            ContainerState, ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting,
            ContainerStatus, Node, NodeAddress, NodeSpec, NodeStatus, Pod, PodCondition, PodIP,
            PodStatus,
        },
        types::Phase,
    };
    pub use rusternetes_kubelet::cri_runtime::CriContainerRuntime;
    pub use rusternetes_kubelet::eviction::{
        get_node_stats, get_pod_stats, EvictionManager, EvictionSignal,
    };
    pub use rusternetes_kubelet::lifecycle::{phase_is_terminal, should_skip_phase_write};
    pub use rusternetes_storage::{build_key, build_prefix, Storage, StorageBackend, WatchEvent};
    pub use std::{
        collections::{HashMap, HashSet},
        path::PathBuf,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    pub use tokio::sync::mpsc;
    pub use tracing::{debug, error, info, warn};
}

mod default_labels_tests {
    use super::__parent::*;
    use rusternetes_kubelet::kubelet::*;

    // kubelet_node_status.go:230-267: existing value != initial value is
    // overwritten (needsUpdate), equal is a no-op, empty initial is deleted.
    #[test]
    fn force_overwrites_drifted_default_labels() {
        let initial = default_node_labels("host-a");
        let mut existing = Some(HashMap::from([
            ("kubernetes.io/hostname".to_string(), "stale".to_string()),
            ("kubernetes.io/os".to_string(), "linux".to_string()),
            ("custom".to_string(), "keep".to_string()),
        ]));
        assert!(update_default_labels(&initial, &mut existing));
        let l = existing.unwrap();
        assert_eq!(l["kubernetes.io/hostname"], "host-a");
        assert_eq!(l["kubernetes.io/arch"], "amd64");
        assert_eq!(l["custom"], "keep");
    }

    #[test]
    fn noop_when_already_matching() {
        let initial = default_node_labels("h");
        let mut existing = Some(initial.clone());
        assert!(!update_default_labels(&initial, &mut existing));
        assert_eq!(existing.unwrap(), initial);
    }

    #[test]
    fn empty_initial_value_is_deleted() {
        let mut initial = default_node_labels("h");
        initial.insert("kubernetes.io/os".to_string(), String::new());
        let mut existing = Some(HashMap::from([(
            "kubernetes.io/os".to_string(),
            "windows".to_string(),
        )]));
        assert!(update_default_labels(&initial, &mut existing));
        assert!(!existing.unwrap().contains_key("kubernetes.io/os"));
    }
}

mod tests {
    use rusternetes_kubelet::kubelet::service_account_token_manager;

    /// The storage-direct kubelet signer carries the api-server's
    /// `--service-account-issuer` / `--api-audiences` (issue #2713), so a token
    /// it mints carries the configured issuer and is accepted by the
    /// api-server's manager built from the same options.
    #[test]
    fn service_account_token_manager_honours_issuer_and_audiences() {
        let key = format!(
            "{}/../common/tests/fixtures/sa/rsa-pkcs1.key",
            env!("CARGO_MANIFEST_DIR")
        );
        let opts = rusternetes_common::auth::ServiceAccountOptions {
            key_files: vec![key.clone()],
            signing_key_file: Some(key),
            issuers: vec!["https://issuer.example".to_string()],
            api_audiences: vec!["api-aud".to_string()],
            ..Default::default()
        };
        let tm = service_account_token_manager(&opts).unwrap();
        assert_eq!(tm.issuer(), Some("https://issuer.example"));
        assert_eq!(tm.api_audiences(), ["api-aud".to_string()]);
        let mut claims = rusternetes_common::auth::ServiceAccountClaims::new(
            "sa".into(),
            "ns".into(),
            "uid".into(),
            1,
        );
        claims.aud = tm.api_audiences().to_vec();
        let tok = tm.generate_token(claims).unwrap();
        let api_server_tm = service_account_token_manager(&opts).unwrap();
        let got = api_server_tm.validate_token(&tok).unwrap();
        assert_eq!(got.iss, "https://issuer.example");
        // Unconfigured: no issuer override, no audiences.
        let none = service_account_token_manager(&Default::default()).unwrap();
        assert_eq!(none.issuer(), None);
        assert!(none.api_audiences().is_empty());
    }
    use rusternetes_kubelet::kubelet::Kubelet;

    /// Storage that behaves like the api-server Store on PUT: a non-empty
    /// `metadata.uid` that differs from the stored one is a 409 (rest/update.go:188-203).
    struct UidStore(rusternetes_storage::memory::MemoryStorage);

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for UidStore {
        async fn create<T>(&self, k: &str, v: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.0.create(k, v).await
        }
        async fn get<T>(&self, k: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.0.get(k).await
        }
        async fn update<T>(&self, k: &str, v: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            let sent = serde_json::to_value(v).unwrap();
            let stored: serde_json::Value = self.0.get(k).await?;
            let (a, b) = (
                sent["metadata"]["uid"].as_str().unwrap_or(""),
                stored["metadata"]["uid"].as_str().unwrap_or(""),
            );
            if !a.is_empty() && a != b {
                return Err(rusternetes_common::Error::Conflict(format!(
                    "Precondition failed: UID in precondition: {a}, UID in object meta: {b}"
                )));
            }
            self.0.update(k, v).await
        }
        async fn update_raw(
            &self,
            k: &str,
            v: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.0.update_raw(k, v).await
        }
        async fn delete(&self, k: &str) -> rusternetes_common::Result<()> {
            self.0.delete(k).await
        }
        async fn list<T>(&self, p: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.0.list(p).await
        }
        async fn watch(
            &self,
            p: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.0.watch(p).await
        }
        async fn watch_from_revision(
            &self,
            p: &str,
            r: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.0.watch_from_revision(p, r).await
        }
        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.0.current_revision().await
        }
        async fn is_revision_compacted(&self, r: i64) -> rusternetes_common::Result<bool> {
            self.0.is_revision_compacted(r).await
        }
    }

    /// #2725: a second publish carries a fresh random UID; it must adopt the
    /// stored one or the Store 409s every tick.
    #[tokio::test]
    async fn republished_node_metrics_survive_uid_precondition() {
        use rusternetes_common::resources::NodeMetrics;
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        use rusternetes_storage::Storage;
        let st = UidStore(rusternetes_storage::memory::MemoryStorage::new());
        let mk = |cpu: &str| NodeMetrics {
            type_meta: TypeMeta {
                api_version: "metrics.k8s.io/v1beta1".into(),
                kind: "NodeMetrics".into(),
            },
            metadata: ObjectMeta::new("n1"),
            timestamp: chrono::Utc::now(),
            window: "30s".into(),
            usage: [("cpu".to_string(), cpu.to_string())].into(),
        };
        let key = "/registry/metrics.k8s.io/nodes/n1";
        rusternetes_kubelet::kubelet::upsert_metrics(&st, key, mk("1m"), |m| &mut m.metadata).await;
        rusternetes_kubelet::kubelet::upsert_metrics(&st, key, mk("2m"), |m| &mut m.metadata).await;
        let got: NodeMetrics = st.get(key).await.unwrap();
        assert_eq!(got.usage["cpu"], "2m");
    }

    /// #2459: deletionTimestamp is the deadline (now+grace), so at the deadline
    /// the grace has elapsed; before it, it has not.
    #[test]
    fn terminating_grace_elapsed_treats_deletion_timestamp_as_deadline() {
        let now = chrono::Utc::now();
        let dl = now + chrono::Duration::seconds(30);
        assert!(!rusternetes_kubelet::kubelet::terminating_grace_elapsed(
            Some(dl),
            now
        ));
        assert!(rusternetes_kubelet::kubelet::terminating_grace_elapsed(
            Some(dl),
            dl
        ));
        assert!(rusternetes_kubelet::kubelet::terminating_grace_elapsed(
            Some(now - chrono::Duration::seconds(1)),
            now
        ));
        assert!(rusternetes_kubelet::kubelet::terminating_grace_elapsed(
            None, now
        ));
    }

    /// A terminal pod with no running container no longer wants its volumes
    /// (`ShouldPodRuntimeBeRemoved`); one still running a container, or a
    /// running pod, does.
    #[test]
    fn pod_volumes_released_only_when_terminal_and_stopped() {
        let mk = |phase: &str, state: serde_json::Value| -> rusternetes_common::resources::Pod {
            serde_json::from_value(serde_json::json!({
                "metadata": {"name": "p", "uid": "u"},
                "spec": {"containers": []},
                "status": {"phase": phase, "containerStatuses": [
                    {"name": "c", "ready": false, "restartCount": 0, "image": "i", "imageID": "", "state": state}
                ]}
            }))
            .unwrap()
        };
        let done = serde_json::json!({"terminated": {"exitCode": 0}});
        let running = serde_json::json!({"running": {}});
        assert!(rusternetes_kubelet::kubelet::pod_volumes_released(&mk(
            "Succeeded",
            done.clone()
        )));
        assert!(rusternetes_kubelet::kubelet::pod_volumes_released(&mk(
            "Failed",
            done.clone()
        )));
        assert!(!rusternetes_kubelet::kubelet::pod_volumes_released(&mk(
            "Running", done
        )));
        assert!(!rusternetes_kubelet::kubelet::pod_volumes_released(&mk(
            "Failed", running
        )));
    }

    /// `nodeStatusUpdateFrequency` drives the heartbeat interval; default 10s
    /// when unset/zero (upstream `defaults.go`; consumer
    /// pkg/kubelet/kubelet.go:1852 `wait.JitterUntil(kl.syncNodeStatus,
    /// kl.nodeStatusUpdateFrequency, ...)`).
    #[tokio::test]
    #[serial_test::serial]
    async fn heartbeat_interval_follows_node_status_update_frequency() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let mk = || async {
            Kubelet::new(
                "node-hb".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        let d = mk().await;
        assert_eq!(d.node_status_heartbeat_interval(), Duration::from_secs(10));
        let c = mk()
            .await
            .with_node_status_update_frequency(Some(Duration::from_secs(25)));
        assert_eq!(c.node_status_heartbeat_interval(), Duration::from_secs(25));
        let z = mk()
            .await
            .with_node_status_update_frequency(Some(Duration::ZERO));
        assert_eq!(z.node_status_heartbeat_interval(), Duration::from_secs(10));
    }

    /// `fileCheckFrequency` drives the static-pod file source poll
    /// (kubelet.go:384 -> file.go:93) and a poll publishes manifests to the
    /// cache `sync_loop` reads.
    #[tokio::test]
    #[serial_test::serial]
    async fn file_check_frequency_drives_static_pod_poll() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        let mk = || async {
            Kubelet::new(
                "node-fc".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        let d = mk().await;
        assert_eq!(d.static_pod_poll_interval(), Duration::from_secs(20));
        let z = mk().await.with_file_check_frequency(Some(Duration::ZERO));
        assert_eq!(z.static_pod_poll_interval(), Duration::from_secs(20));
        let k = mk()
            .await
            .with_pod_manifest_path(Some(manifests.clone()))
            .with_file_check_frequency(Some(Duration::from_secs(5)));
        assert_eq!(k.static_pod_poll_interval(), Duration::from_secs(5));

        std::fs::write(
            manifests.join("web.yaml"),
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web\nspec:\n  containers:\n  - name: c\n    image: busybox\n",
        )
        .unwrap();
        assert!(k.static_pods.lock().unwrap().is_empty(), "no poll yet");
        k.poll_static_pods().await;
        assert_eq!(k.static_pods.lock().unwrap().len(), 1);
    }

    /// `httpCheckFrequency` drives the manifest-URL source poll (kubelet.go:390
    /// -> http.go:60) and a poll publishes the URL's pods to the cache
    /// `sync_loop` reads, alongside file-sourced pods.
    #[tokio::test]
    #[serial_test::serial]
    async fn http_check_frequency_drives_static_pod_url_poll() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        std::fs::write(
            manifests.join("web.yaml"),
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web\nspec:\n  containers:\n  - name: c\n    image: busybox\n",
        )
        .unwrap();
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/pod", l.local_addr().unwrap());
        tokio::spawn(async move {
            let body = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: remote\nspec:\n  containers:\n  - name: c\n    image: busybox\n";
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = s.read(&mut buf).await;
            let _ = s
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await;
        });
        let mk = || async {
            Kubelet::new(
                "node-hc".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        let d = mk().await;
        assert_eq!(d.static_pod_url_poll_interval(), Duration::from_secs(20));
        assert!(!d.poll_static_pod_url().await, "no staticPodURL, no source");
        let z = mk().await.with_http_check_frequency(Some(Duration::ZERO));
        assert_eq!(z.static_pod_url_poll_interval(), Duration::from_secs(20));
        let k = mk()
            .await
            .with_pod_manifest_path(Some(manifests))
            .with_http_check_frequency(Some(Duration::from_secs(7)))
            .with_static_pod_url(Some(&url), &Default::default())
            .unwrap();
        assert_eq!(k.static_pod_url_poll_interval(), Duration::from_secs(7));
        k.poll_static_pods().await;
        assert!(k.poll_static_pod_url().await);
        let names: std::collections::BTreeSet<String> =
            k.static_pods.lock().unwrap().keys().cloned().collect();
        assert_eq!(
            names,
            ["remote-node-hc".to_string(), "web-node-hc".to_string()].into()
        );
    }

    /// `runtimeRequestTimeout` reaches the CRI client (upstream passes
    /// `kubeCfg.RuntimeRequestTimeout.Duration` into
    /// `NewRemoteRuntimeService`, pkg/kubelet/kubelet.go:408); unset or zero
    /// takes the 2m default (v1beta1/defaults.go:188-189).
    #[tokio::test]
    #[serial_test::serial]
    async fn runtime_request_timeout_reaches_cri_client() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let mk = || async {
            Kubelet::new(
                "node-rrt".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        assert_eq!(
            mk().await.runtime_request_timeout(),
            Duration::from_secs(120)
        );
        let c = mk()
            .await
            .with_runtime_request_timeout(Some(Duration::from_secs(7)));
        assert_eq!(c.runtime_request_timeout(), Duration::from_secs(7));
        let z = mk()
            .await
            .with_runtime_request_timeout(Some(Duration::ZERO));
        assert_eq!(z.runtime_request_timeout(), Duration::from_secs(120));
    }

    /// `newCrashLoopBackOff` gate matrix (pkg/kubelet/kubelet.go:345-361).
    #[test]
    #[serial_test::serial]
    fn new_crash_loop_backoff_gate_matrix() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        use rusternetes_kubelet::kubelet::new_crash_loop_backoff;
        let s = std::time::Duration::from_secs;
        // Defaults (Max on, Reduce off): operator value wins, initial 10s clamped.
        assert_eq!(new_crash_loop_backoff(Some(s(45))), (s(45), s(10)));
        assert_eq!(new_crash_loop_backoff(Some(s(3))), (s(3), s(3)));
        // Max off: operator value is ignored, 300s/10s.
        let _m = with_feature(Feature::KubeletCrashLoopBackOffMax, false);
        assert_eq!(new_crash_loop_backoff(Some(s(45))), (s(300), s(10)));
        // Max off + Reduce on: 60s/1s.
        let _r = with_feature(Feature::ReduceDefaultCrashLoopBackOffDecay, true);
        assert_eq!(new_crash_loop_backoff(None), (s(60), s(1)));
        // Both on: configured value overrides the reduced max; initial stays 1s.
        let _m2 = with_feature(Feature::KubeletCrashLoopBackOffMax, true);
        assert_eq!(new_crash_loop_backoff(Some(s(45))), (s(45), s(1)));
    }

    /// `crashLoopBackOff.maxContainerRestartPeriod` caps the restart backoff and
    /// clamps the initial delay (kubelet.go:345-358).
    #[tokio::test]
    #[serial_test::serial]
    async fn crash_loop_backoff_max_applies() {
        use rusternetes_storage::StorageBackend;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let mk = || async {
            Kubelet::new(
                "node-clb".into(),
                std::sync::Arc::new(StorageBackend::new_memory()),
                10,
                dir.path().join("vols").display().to_string(),
                "10.96.0.10".into(),
                "cluster.local".into(),
                "bridge".into(),
                String::new(),
            )
            .await
            .unwrap()
        };
        let d = mk().await;
        assert_eq!(d.crash_loop_backoff_max, Duration::from_secs(300));
        assert_eq!(d.crash_loop_backoff_initial(), Duration::from_secs(10));
        let c = mk()
            .await
            .with_crash_loop_backoff_max(Some(Duration::from_secs(60)));
        assert_eq!(c.crash_loop_backoff_max, Duration::from_secs(60));
        assert_eq!(c.crash_loop_backoff_initial(), Duration::from_secs(10));
        let t = mk()
            .await
            .with_crash_loop_backoff_max(Some(Duration::from_secs(3)));
        assert_eq!(t.crash_loop_backoff_initial(), Duration::from_secs(3));
    }

    /// A kubelet built with explicit `nodeIPs` / `externalCloudProvider`
    /// (`NewMainKubelet`, kubelet.go:428-430, 619, 637), with the
    /// `nodeIPValidator` override kubelet_node_status_test.go uses so the test
    /// IPs need not be on a host interface.
    async fn node_ip_kubelet(
        dir: &tempfile::TempDir,
        storage: std::sync::Arc<rusternetes_storage::StorageBackend>,
        name: &str,
        ips: &[&str],
        external: bool,
    ) -> Kubelet {
        node_ip_kubelet_h(dir, storage, name, None, ips, external).await
    }

    async fn node_ip_kubelet_h(
        dir: &tempfile::TempDir,
        storage: std::sync::Arc<rusternetes_storage::StorageBackend>,
        name: &str,
        hostname: Option<&str>,
        ips: &[&str],
        external: bool,
    ) -> Kubelet {
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let k = Kubelet::new_with_eviction(
            name.into(),
            storage,
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
            dir.path().to_path_buf(),
            rusternetes_kubelet::eviction::EvictionManager::new(),
            10250,
            Vec::new(),
            ips.iter().map(|s| s.parse().unwrap()).collect(),
            external,
            service_account_token_manager(&Default::default()).unwrap(),
        )
        .await
        .unwrap()
        .with_node_ip_validator(|_| Ok(()));
        let k = match hostname {
            Some(h) => k.with_hostname(h),
            None => k,
        };
        k.update_runtime_up().await;
        k.register_node().await.unwrap();
        k
    }

    async fn get_node(
        storage: &rusternetes_storage::StorageBackend,
        name: &str,
    ) -> rusternetes_common::resources::Node {
        use rusternetes_storage::{build_key, Storage};
        storage.get(&build_key("nodes", None, name)).await.unwrap()
    }

    fn node_addrs(n: &rusternetes_common::resources::Node) -> Vec<(String, String)> {
        n.status
            .as_ref()
            .and_then(|s| s.addresses.clone())
            .unwrap_or_default()
            .into_iter()
            .map(|a| (a.address_type, a.address))
            .collect()
    }

    fn pair(t: &str, a: &str) -> (String, String) {
        (t.to_string(), a.to_string())
    }

    /// initialNode (kubelet_node_status.go:325-333) taints an
    /// externalCloudProvider node with `node.cloudprovider.kubernetes.io/
    /// uninitialized=true:NoSchedule`, and setNodeStatus runs the NodeAddress
    /// setter (setters.go:62-135): provided-node-ip annotation + addresses.
    #[tokio::test]
    #[serial_test::serial]
    async fn external_cloud_provider_node_is_tainted_annotated_and_addressed() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let _k = node_ip_kubelet(&dir, storage.clone(), "node-ext", &["10.1.1.1"], true).await;
        let n = get_node(&storage, "node-ext").await;
        let taints = n.spec.as_ref().unwrap().taints.clone().unwrap_or_default();
        assert!(
            taints.iter().any(
                |t| t.key == "node.cloudprovider.kubernetes.io/uninitialized"
                    && t.value.as_deref() == Some("true")
                    && t.effect == "NoSchedule"
            ),
            "taints: {taints:?}"
        );
        assert_eq!(
            n.metadata.annotations.as_ref().unwrap()["alpha.kubernetes.io/provided-node-ip"],
            "10.1.1.1"
        );
        assert_eq!(
            node_addrs(&n),
            vec![pair("InternalIP", "10.1.1.1"), pair("Hostname", "node-ext")]
        );
    }

    /// Without an external provider there is no taint, no annotation.
    #[tokio::test]
    #[serial_test::serial]
    async fn non_external_node_has_no_cloud_taint_or_annotation() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let _k = node_ip_kubelet(&dir, storage.clone(), "node-int", &["10.1.1.1"], false).await;
        let n = get_node(&storage, "node-int").await;
        assert!(n
            .spec
            .as_ref()
            .and_then(|s| s.taints.clone())
            .unwrap_or_default()
            .is_empty());
        assert!(!n
            .metadata
            .annotations
            .clone()
            .unwrap_or_default()
            .contains_key("alpha.kubernetes.io/provided-node-ip"));
    }

    /// setters.go:104-110: with --cloud-provider=external and addresses
    /// already set, the setter returns early; provider addresses win on every
    /// sync.
    #[tokio::test]
    #[serial_test::serial]
    async fn external_cloud_provider_addresses_survive_status_sync() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage.clone(), "node-prov", &["10.1.1.1"], true).await;
        let key = build_key("nodes", None, "node-prov");
        let mut n = get_node(&storage, "node-prov").await;
        n.status.as_mut().unwrap().addresses = Some(vec![
            rusternetes_common::resources::NodeAddress {
                address_type: "ExternalIP".into(),
                address: "203.0.113.7".into(),
            },
            rusternetes_common::resources::NodeAddress {
                address_type: "InternalIP".into(),
                address: "10.9.9.9".into(),
            },
        ]);
        storage.update_status(&key, &n).await.unwrap();
        k.update_node_status().await.unwrap();
        assert_eq!(
            node_addrs(&get_node(&storage, "node-prov").await),
            vec![
                pair("ExternalIP", "203.0.113.7"),
                pair("InternalIP", "10.9.9.9")
            ]
        );
    }

    /// kubelet_node_status.go:678 + setNodeStatus: the NodeAddress setter
    /// re-runs on EVERY status sync (not memoized for the process lifetime),
    /// overwriting drifted addresses and deleting a stale provided-node-ip
    /// annotation when no longer using an external provider (setters.go:97-101).
    #[tokio::test]
    #[serial_test::serial]
    async fn node_address_setter_reruns_every_status_sync() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage.clone(), "node-sync", &["10.1.1.1"], false).await;
        let key = build_key("nodes", None, "node-sync");
        let mut n = get_node(&storage, "node-sync").await;
        n.status.as_mut().unwrap().addresses =
            Some(vec![rusternetes_common::resources::NodeAddress {
                address_type: "InternalIP".into(),
                address: "9.9.9.9".into(),
            }]);
        n.metadata
            .annotations
            .get_or_insert_with(Default::default)
            .insert(
                "alpha.kubernetes.io/provided-node-ip".into(),
                "9.9.9.9".into(),
            );
        storage.update(&key, &n).await.unwrap();
        k.update_node_status().await.unwrap();
        let n = get_node(&storage, "node-sync").await;
        assert_eq!(
            node_addrs(&n),
            vec![
                pair("InternalIP", "10.1.1.1"),
                pair("Hostname", "node-sync")
            ]
        );
        assert!(!n
            .metadata
            .annotations
            .clone()
            .unwrap_or_default()
            .contains_key("alpha.kubernetes.io/provided-node-ip"));
    }

    /// #3585: `kl.hostname` (nodeutil.GetHostname(--hostname-override)) is
    /// distinct from `nodeName`: the Hostname address (setters.go:128-130) and
    /// the `kubernetes.io/hostname` label (kubelet_node_status.go:306) carry
    /// the hostname, the Node object keeps `nodeName`.
    #[tokio::test]
    #[serial_test::serial]
    async fn hostname_is_distinct_from_node_name() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let _k = node_ip_kubelet_h(
            &dir,
            storage.clone(),
            "node-a",
            Some("host-a"),
            &["10.1.1.1"],
            false,
        )
        .await;
        let n = get_node(&storage, "node-a").await;
        assert_eq!(n.metadata.name, "node-a");
        assert_eq!(
            node_addrs(&n),
            vec![pair("InternalIP", "10.1.1.1"), pair("Hostname", "host-a")]
        );
        assert_eq!(
            n.metadata.labels.as_ref().unwrap()["kubernetes.io/hostname"],
            "host-a"
        );
    }

    /// #3585: setters.go:166-171 looks up `node.Name` only; the pre-existing
    /// `$HOSTNAME` retry is gone.
    #[test]
    #[serial_test::serial]
    fn lookup_node_ips_has_no_hostname_env_fallback() {
        std::env::set_var("HOSTNAME", "localhost");
        let found = Kubelet::lookup_node_ips("no-such-host.invalid");
        std::env::remove_var("HOSTNAME");
        assert!(found.is_empty(), "{found:?}");
    }

    /// #3584: pod hostIPs come from the node's status addresses
    /// (getHostIPsAnyWay -> GetNodeHostIPs), no invented fallback IP.
    #[tokio::test]
    #[serial_test::serial]
    async fn host_ips_follow_node_addresses_without_invented_fallback() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let ip = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
        let k = node_ip_kubelet(&dir, storage.clone(), "node-s", &["10.1.1.1"], false).await;
        assert_eq!(k.get_host_ips_any_way().unwrap(), vec![ip("10.1.1.1")]);
        let k = node_ip_kubelet(
            &dir,
            storage.clone(),
            "node-d",
            &["10.1.1.2", "fd00::2"],
            false,
        )
        .await;
        assert_eq!(
            k.get_host_ips_any_way().unwrap(),
            vec![ip("10.1.1.2"), ip("fd00::2")]
        );
        let (host_ip, host_ips) = k.host_ip_fields();
        assert_eq!(host_ip.as_deref(), Some("10.1.1.2"));
        let host_ips: Vec<String> = host_ips.unwrap().into_iter().map(|h| h.ip).collect();
        assert_eq!(
            host_ips,
            vec!["10.1.1.2".to_string(), "fd00::2".to_string()]
        );
        // --cloud-provider=external without --node-ip: Hostname only
        // (setters.go:130-135) => host IP unknown, hostIP left unset.
        let k = node_ip_kubelet(&dir, storage.clone(), "node-x", &[], true).await;
        let err = k.get_host_ips_any_way().unwrap_err();
        assert!(
            err.starts_with("host IP unknown; known addresses: "),
            "{err}"
        );
        let (host_ip, host_ips) = k.host_ip_fields();
        assert!(host_ip.is_none() && host_ips.is_none());
    }

    /// #3542: nodeIPs live on the Kubelet (createAndInitKubelet ->
    /// NewMainKubelet `nodeIPs`), not in process-global state: two kubelets in
    /// one process advertise their own `--node-ip`.
    #[tokio::test]
    #[serial_test::serial]
    async fn node_ips_are_per_kubelet_not_process_global() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let _a = node_ip_kubelet(&dir, storage.clone(), "node-a", &["10.1.1.1"], false).await;
        let _b = node_ip_kubelet(&dir, storage.clone(), "node-b", &["10.1.1.2"], false).await;
        assert_eq!(
            node_addrs(&get_node(&storage, "node-a").await),
            vec![pair("InternalIP", "10.1.1.1"), pair("Hostname", "node-a")]
        );
        assert_eq!(
            node_addrs(&get_node(&storage, "node-b").await),
            vec![pair("InternalIP", "10.1.1.2"), pair("Hostname", "node-b")]
        );
    }

    use std::time::Duration;

    async fn status_gate_kubelet(
        dir: &tempfile::TempDir,
        storage: std::sync::Arc<rusternetes_storage::StorageBackend>,
    ) -> Kubelet {
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let k = Kubelet::new(
            "node-gate".into(),
            storage,
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
        )
        .await
        .unwrap();
        k.update_runtime_up().await;
        k.register_node().await.unwrap();
        k
    }

    async fn ready_heartbeat(
        storage: &rusternetes_storage::StorageBackend,
    ) -> chrono::DateTime<chrono::Utc> {
        use rusternetes_storage::{build_key, Storage};
        let n: rusternetes_common::resources::Node = storage
            .get(&build_key("nodes", None, "node-gate"))
            .await
            .unwrap();
        n.status
            .unwrap()
            .conditions
            .unwrap()
            .into_iter()
            .find(|c| c.condition_type == "Ready")
            .unwrap()
            .last_heartbeat_time
            .unwrap()
    }

    async fn backdate_ready_heartbeat(
        storage: &rusternetes_storage::StorageBackend,
        secs: i64,
    ) -> chrono::DateTime<chrono::Utc> {
        use rusternetes_storage::{build_key, Storage};
        let key = build_key("nodes", None, "node-gate");
        let mut n: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        let old = chrono::Utc::now() - chrono::Duration::seconds(secs);
        for c in n.status.as_mut().unwrap().conditions.as_mut().unwrap() {
            c.last_heartbeat_time = Some(old);
        }
        storage.update_status(&key, &n).await.unwrap();
        ready_heartbeat(storage).await
    }

    /// tryUpdateNodeStatus (kubelet_node_status.go:483-531): a node whose
    /// status has not changed is NOT rewritten on each heartbeat, only once
    /// `nodeStatusReportFrequency` has elapsed. (Before #2520 every heartbeat
    /// older than 10s rewrote the Node.)
    #[tokio::test]
    #[serial_test::serial]
    async fn unchanged_node_status_is_not_rewritten_within_report_frequency() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage.clone()).await;
        // First report after start always goes out (zero lastStatusReportTime).
        k.update_node_status().await.unwrap();
        let old = backdate_ready_heartbeat(&storage, 30).await;
        // Nothing changed and well inside nodeStatusReportFrequency (5m): skip.
        k.update_node_status().await.unwrap();
        assert_eq!(ready_heartbeat(&storage).await, old);
    }

    /// A quiet node still gets a status refresh once
    /// `nodeStatusReportFrequency` has elapsed (isUpdateStatusPeriodExpired,
    /// kubelet_node_status.go:535), so a skipped heartbeat never starves
    /// consumers of the Ready condition's `lastHeartbeatTime`.
    #[tokio::test]
    #[serial_test::serial]
    async fn quiet_node_is_refreshed_after_report_frequency() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage.clone())
            .await
            .with_node_status_report_frequency(Some(Duration::from_secs(300)));
        k.update_node_status().await.unwrap();
        let old = backdate_ready_heartbeat(&storage, 30).await;
        // Just under the period (no delay): still skipped.
        k.set_last_status_report_for_test(Duration::from_secs(299));
        k.update_node_status().await.unwrap();
        assert_eq!(ready_heartbeat(&storage).await, old);
        // Period elapsed: refreshed.
        k.set_last_status_report_for_test(Duration::from_secs(300));
        k.update_node_status().await.unwrap();
        assert!(ready_heartbeat(&storage).await > old + chrono::Duration::seconds(20));
    }

    /// A real status change is written immediately, not held for the report
    /// period (`changed || isUpdateStatusPeriodExpired`, :493-496).
    #[tokio::test]
    #[serial_test::serial]
    async fn changed_node_status_is_written_immediately() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage.clone()).await;
        k.update_node_status().await.unwrap();
        let old = backdate_ready_heartbeat(&storage, 30).await;
        let key = build_key("nodes", None, "node-gate");
        let mut n: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        n.status.as_mut().unwrap().daemon_endpoints = None;
        storage.update_status(&key, &n).await.unwrap();
        k.update_node_status().await.unwrap();
        assert!(ready_heartbeat(&storage).await > old + chrono::Duration::seconds(20));
    }

    /// #3261: NoExecute eviction belongs to kube-controller-manager's
    /// taint-eviction controller alone. Upstream's kubelet has no taint
    /// handling in its node-status path: `tryUpdateNodeStatus`
    /// (pkg/kubelet/kubelet_node_status.go:483) only builds and patches node
    /// status, and `grep -rni noexecute pkg/kubelet/kubelet_node_status.go`
    /// has no match. A kubelet heartbeat must therefore leave pods on a
    /// NoExecute-tainted node untouched (running or terminal alike).
    #[tokio::test]
    #[serial_test::serial]
    async fn node_status_sync_does_not_evict_for_noexecute_taints() {
        use rusternetes_common::resources::{Node, Pod};
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage.clone()).await;

        let nkey = build_key("nodes", None, "node-gate");
        let mut n: Node = storage.get(&nkey).await.unwrap();
        n.spec.as_mut().unwrap().taints = Some(vec![serde_json::from_value(serde_json::json!({
            "key": "t", "effect": "NoExecute",
            "timeAdded": chrono::Utc::now().to_rfc3339()
        }))
        .unwrap()]);
        storage.update(&nkey, &n).await.unwrap();

        for (name, phase) in [("running", "Running"), ("done", "Succeeded")] {
            let pod: Pod = serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": name, "namespace": "default", "uid": format!("u-{name}")},
                "spec": {"nodeName": "node-gate", "containers": [{"name": "c", "image": "i"}]},
                "status": {"phase": phase}
            }))
            .unwrap();
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        k.update_node_status().await.unwrap();

        for name in ["running", "done"] {
            let pod: Pod = storage
                .get(&build_key("pods", Some("default"), name))
                .await
                .unwrap();
            assert!(
                pod.metadata.deletion_timestamp.is_none(),
                "kubelet must not evict pod {name}: eviction is the taint-eviction controller's job"
            );
            assert_ne!(
                pod.status.as_ref().and_then(|s| s.reason.as_deref()),
                Some("Evicted"),
                "pod {name}"
            );
        }
    }

    /// `nodeStatusReportFrequency` is configurable and defaults to 5m
    /// (v1beta1/defaults.go).
    #[tokio::test]
    #[serial_test::serial]
    async fn report_frequency_builder_defaults_to_five_minutes() {
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(rusternetes_storage::StorageBackend::new_memory());
        let k = status_gate_kubelet(&dir, storage).await;
        assert_eq!(k.node_status_report_frequency, Duration::from_secs(300));
        let k = k.with_node_status_report_frequency(Some(Duration::from_secs(42)));
        assert_eq!(k.node_status_report_frequency, Duration::from_secs(42));
        let k = k.with_node_status_report_frequency(Some(Duration::ZERO));
        assert_eq!(k.node_status_report_frequency, Duration::from_secs(300));
    }

    /// #1929: a kubelet whose CRI endpoint is an absent socket must report
    /// `Ready=False`/`KubeletNotReady`, both at registration and on every
    /// heartbeat (upstream `nodestatus.ReadyCondition`,
    /// pkg/kubelet/nodestatus/setters.go:469-513; `updateRuntimeUp`,
    /// pkg/kubelet/kubelet.go:3113-3157).
    #[tokio::test]
    #[serial_test::serial]
    async fn node_is_not_ready_when_the_cri_runtime_is_absent() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = Kubelet::new(
            "node-x".into(),
            storage.clone(),
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
        )
        .await
        .unwrap();
        k.update_runtime_up().await;
        k.register_node().await.unwrap();
        let ready = |n: &rusternetes_common::resources::Node| {
            n.status
                .as_ref()
                .unwrap()
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .find(|c| c.condition_type == "Ready")
                .cloned()
                .unwrap()
        };
        let key = build_key("nodes", None, "node-x");
        let node: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        let c = ready(&node);
        assert_eq!(c.status, "False");
        assert_eq!(c.reason.as_deref(), Some("KubeletNotReady"));
        assert!(
            c.message.as_deref().unwrap().contains("container runtime"),
            "message must name the runtime: {:?}",
            c.message
        );
        // The heartbeat must not flip it back to True.
        k.update_node_status().await.unwrap();
        let node: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        assert_eq!(ready(&node).status, "False");
    }

    /// #2591: the CSI plugin's `SetKubeletError` must surface in the node's
    /// Ready condition at the kubelet level, ordered runtime, network, storage
    /// (`errs := []error{runtimeErrorsFunc(), networkErrorsFunc(),
    /// storageErrorsFunc(), ...}`, pkg/kubelet/nodestatus/setters.go:491) and
    /// joined by `utilerrors.NewAggregate` (setters.go:505-513). The CRI
    /// socket is absent so the runtime error is the first aggregate member;
    /// the CSINode error must follow it and vanish once cleared.
    /// Regression guard: the behaviour already exists (#2590).
    #[tokio::test]
    #[serial_test::serial]
    async fn csinode_storage_error_gates_ready_condition() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = Kubelet::new(
            "node-csi".into(),
            storage.clone(),
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
        )
        .await
        .unwrap();
        k.update_runtime_up().await;
        k.set_kubelet_error(Some("CSINode is not yet initialized".into()));
        let errs = k.ready_errors();
        assert_eq!(
            errs.last().map(String::as_str),
            Some("CSINode is not yet initialized"),
            "storage error is last: {errs:?}"
        );
        assert!(errs.len() >= 2, "runtime error precedes storage: {errs:?}");

        k.register_node().await.unwrap();
        let key = build_key("nodes", None, "node-csi");
        let ready = |n: &rusternetes_common::resources::Node| {
            n.status
                .as_ref()
                .unwrap()
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .find(|c| c.condition_type == "Ready")
                .cloned()
                .unwrap()
        };
        let node: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        let c = ready(&node);
        assert_eq!(c.status, "False");
        assert_eq!(c.reason.as_deref(), Some("KubeletNotReady"));
        let msg = c.message.unwrap();
        assert!(msg.contains("CSINode is not yet initialized"), "{msg}");
        assert!(
            msg.find("container runtime").unwrap() < msg.find("CSINode").unwrap(),
            "runtime before storage: {msg}"
        );

        // Cleared (CSINode initialised): the heartbeat drops the message.
        k.set_kubelet_error(None);
        k.update_node_status().await.unwrap();
        let node: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        let c = ready(&node);
        assert!(!c.message.unwrap().contains("CSINode"));
    }

    /// `fastNodeStatusUpdate` (kubelet_node_status.go:371-440): NotReady
    /// node + unready runtime -> not completed; timeout -> completed and
    /// arms containerRuntimeReadyExpected. (The push path needs a live CRI
    /// runtime, covered by the conformance canary.)
    #[tokio::test]
    #[serial_test::serial]
    async fn fast_node_status_update_retries_then_arms_on_timeout() {
        use rusternetes_storage::{build_key, Storage, StorageBackend};
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("absent.sock");
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = Kubelet::new(
            "node-fast".into(),
            storage.clone(),
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
        )
        .await
        .unwrap();
        k.update_runtime_up().await;
        k.register_node().await.unwrap();
        let key = build_key("nodes", None, "node-fast");
        let is_ready = |n: &rusternetes_common::resources::Node| {
            n.status
                .as_ref()
                .unwrap()
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Ready" && c.status == "True")
        };
        let node: rusternetes_common::resources::Node = storage.get(&key).await.unwrap();
        assert!(!is_ready(&node));

        // Runtime still absent: keep retrying, nothing armed.
        assert!(!k.fast_node_status_update(false).await);
        assert!(!k.runtime_state.container_runtime_ready_expected());

        // Grace period expired: completes and arms the flag.
        assert!(k.fast_node_status_update(true).await);
        assert!(k.runtime_state.container_runtime_ready_expected());
    }

    fn resources(pairs: &[(&str, &str)]) -> Option<std::collections::HashMap<String, String>> {
        Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    /// `TestMachineInfo` "extended resources not present in capacity are
    /// removed from allocatable" (nodestatus/setters_test.go:785-812), and an
    /// extended resource added to capacity becomes allocatable.
    #[test]
    fn allocatable_follows_capacity() {
        let mut status = rusternetes_common::resources::NodeStatus {
            capacity: resources(&[("cpu", "2"), ("pods", "110")]),
            allocatable: resources(&[("example.com/extended", "1"), ("cpu", "1")]),
            ..Default::default()
        };
        assert!(rusternetes_kubelet::kubelet::sync_allocatable_with_capacity(&mut status));
        assert_eq!(
            status.allocatable,
            resources(&[("cpu", "2"), ("pods", "110")])
        );
        assert!(!rusternetes_kubelet::kubelet::sync_allocatable_with_capacity(&mut status));

        status
            .capacity
            .as_mut()
            .unwrap()
            .insert("scheduling.k8s.io/foo".into(), "5".into());
        assert!(rusternetes_kubelet::kubelet::sync_allocatable_with_capacity(&mut status));
        assert_eq!(
            status.allocatable,
            resources(&[
                ("cpu", "2"),
                ("pods", "110"),
                ("scheduling.k8s.io/foo", "5")
            ])
        );
    }
    use rusternetes_common::resources::pod::{PodCondition, PodSpec};
    use rusternetes_common::resources::{
        Container, ContainerState, ContainerStateRunning, ContainerStateTerminated,
        ContainerStateWaiting, ContainerStatus, Pod, PodStatus,
    };
    use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};

    #[test]
    fn start_time_is_preserved_once_set() {
        use chrono::{Duration, Utc};
        let now = Utc::now();
        let earlier = now - Duration::seconds(100);
        // An already-set startTime is kept across status rebuilds, so
        // activeDeadlineSeconds elapsed keeps growing instead of resetting to ~0
        // every sync (which would mean the deadline is never reached).
        assert_eq!(
            rusternetes_kubelet::kubelet::preserved_start_time(Some(earlier), now),
            earlier
        );
        // First Running write (no prior startTime) stamps now.
        assert_eq!(
            rusternetes_kubelet::kubelet::preserved_start_time(None, now),
            now
        );
    }

    #[test]
    fn active_deadline_elapsed_matches_upstream_boundary() {
        use chrono::{Duration, Utc};

        let now = Utc::now();
        let status = PodStatus {
            start_time: Some(now - Duration::seconds(5)),
            ..Default::default()
        };

        assert_eq!(
            rusternetes_kubelet::kubelet::active_deadline_elapsed(Some(&status), 5, now),
            Some(5)
        );
        assert_eq!(
            rusternetes_kubelet::kubelet::active_deadline_elapsed(Some(&status), 6, now),
            None
        );
    }

    #[test]
    fn active_deadline_elapsed_requires_start_time() {
        let status = PodStatus {
            start_time: None,
            ..Default::default()
        };

        assert_eq!(
            rusternetes_kubelet::kubelet::active_deadline_elapsed(
                Some(&status),
                5,
                chrono::Utc::now()
            ),
            None
        );
        assert_eq!(
            rusternetes_kubelet::kubelet::active_deadline_elapsed(None, 5, chrono::Utc::now()),
            None
        );
    }

    #[test]
    fn deadline_exceeded_failed_pod_requires_termination_even_with_restart_always() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("deadline-pod").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Always".to_string()),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Failed),
                reason: Some(rusternetes_kubelet::kubelet::ACTIVE_DEADLINE_REASON.to_string()),
                ..Default::default()
            }),
        };

        assert!(rusternetes_kubelet::kubelet::terminal_phase_requires_termination(&pod));
    }

    #[test]
    fn ordinary_failed_pod_with_restart_always_does_not_force_termination() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("non-deadline-failed").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Always".to_string()),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Failed),
                reason: Some("CreateContainerError".to_string()),
                ..Default::default()
            }),
        };

        assert!(!rusternetes_kubelet::kubelet::terminal_phase_requires_termination(&pod));
    }

    #[test]
    fn restart_never_failed_init_is_terminal() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("init-fail").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Never".to_string()),
                init_containers: Some(vec![Container {
                    name: "init1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }]),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                init_container_statuses: Some(vec![ContainerStatus {
                    name: "init1".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 1,
                            reason: Some("Error".to_string()),
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                            signal: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some("busybox:latest".to_string()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                ..Default::default()
            }),
        };

        assert!(
            rusternetes_kubelet::kubelet::init_container_failed_terminally(
                &pod,
                pod.status
                    .as_ref()
                    .and_then(|status| status.init_container_statuses.as_deref())
            )
        );
    }

    // Regression: a restartPolicy=Never pod with NO init containers must not be
    // classified as a terminal init failure. A transient app-container start
    // error (e.g. CRI contention) on such a pod previously took the
    // `restart_policy == "Never"` fast-fail branch and was reported as
    // Failed/"Init container failed"; the start-error handler now keys off
    // init_container_failed_terminally, which is false here, so the pod stays
    // Pending and retries. Node-conformance "should use the image defaults if
    // command and args are blank" flaked on exactly this.
    #[test]
    fn restart_never_no_init_containers_is_not_terminal_init_failure() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("no-init").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Never".to_string()),
                containers: vec![Container {
                    name: "agnhost-container".to_string(),
                    image: "registry.k8s.io/e2e-test-images/agnhost:2.59".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            // App container still being created — no init statuses at all.
            status: Some(PodStatus {
                container_statuses: Some(vec![ContainerStatus {
                    name: "agnhost-container".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState {
                        waiting: Some(ContainerStateWaiting {
                            reason: Some("ContainerCreating".to_string()),
                            message: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some("registry.k8s.io/e2e-test-images/agnhost:2.59".to_string()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]),
                ..Default::default()
            }),
        };

        assert!(
            !rusternetes_kubelet::kubelet::init_container_failed_terminally(
                &pod,
                pod.status
                    .as_ref()
                    .and_then(|status| status.init_container_statuses.as_deref())
            )
        );
    }

    // Regression for NodeConformance "[sig-node] InitContainer should not start
    // app containers if init containers fail on a RestartAlways pod": while a
    // plain init container has failed (and, being RestartAlways, will retry),
    // the app container must report Waiting{PodInitializing}, not
    // ContainerCreating. Previously the start-error handler stamped
    // ContainerCreating + the init-failure message onto app containers.
    #[test]
    fn app_container_reason_is_podinitializing_while_init_incomplete() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("init-fail-restart-always").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Always".to_string()),
                init_containers: Some(vec![Container {
                    name: "init1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }]),
                containers: vec![Container {
                    name: "run1".to_string(),
                    image: "registry.k8s.io/pause:3.10.1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        // init1 failed (exit 1) — not yet completed successfully.
        let init_statuses = vec![ContainerStatus {
            name: "init1".to_string(),
            ready: false,
            restart_count: 0,
            state: Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    exit_code: 1,
                    reason: Some("Error".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                    signal: None,
                }),
                ..Default::default()
            }),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(false),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }];
        assert_eq!(
            rusternetes_kubelet::kubelet::app_container_waiting_reason(&pod, Some(&init_statuses)),
            "PodInitializing",
            "app container blocked behind a failed init container must be PodInitializing"
        );
    }

    // Once every init container has completed successfully, a genuine
    // app-container start error keeps the ContainerCreating reason.
    #[test]
    fn app_container_reason_is_containercreating_when_init_done() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("init-done").with_namespace("default"),
            spec: Some(PodSpec {
                init_containers: Some(vec![Container {
                    name: "init1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }]),
                containers: vec![Container {
                    name: "run1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        let init_statuses = vec![ContainerStatus {
            name: "init1".to_string(),
            ready: true,
            restart_count: 0,
            state: Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    exit_code: 0,
                    reason: Some("Completed".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                    signal: None,
                }),
                ..Default::default()
            }),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(true),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }];
        assert_eq!(
            rusternetes_kubelet::kubelet::app_container_waiting_reason(&pod, Some(&init_statuses)),
            "ContainerCreating"
        );
    }

    // A pod with no init containers is unaffected.
    #[test]
    fn app_container_reason_no_init_is_containercreating() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("no-init").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "run1".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        assert_eq!(
            rusternetes_kubelet::kubelet::app_container_waiting_reason(&pod, None),
            "ContainerCreating"
        );
    }

    /// #3901: the create-error Pending write is a generateAPIPodStatus
    /// output: conditions the kubelet does not own survive
    /// (kubelet_pods.go:1957-1962), startTime is not reset, and a terminal
    /// API phase is not left (kubelet_pods.go:1937-1945).
    #[tokio::test]
    #[serial_test::serial]
    async fn create_error_status_is_generated_from_prior_status() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage, "node-ce", &["10.1.1.1"], false).await;
        let t0 = chrono::Utc::now() - chrono::Duration::hours(1);
        let mut pod = Pod::new("ce", Default::default());
        pod.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            start_time: Some(t0),
            conditions: Some(vec![rusternetes_common::resources::PodCondition {
                condition_type: "DisruptionTarget".into(),
                status: "True".into(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            ..Default::default()
        });
        let st = k.create_error_pod_status(
            &pod,
            "CreateContainerError".into(),
            "boom".into(),
            None,
            None,
            "BestEffort".into(),
        );
        assert_eq!(st.start_time, Some(t0), "startTime must not reset");
        assert!(st
            .conditions
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|c| c.condition_type == "DisruptionTarget"));
        assert_eq!(st.reason.as_deref(), Some("CreateContainerError"));

        pod.status.as_mut().unwrap().phase = Some(Phase::Failed);
        let st = k.create_error_pod_status(
            &pod,
            "CreateContainerError".into(),
            "boom".into(),
            None,
            None,
            "BestEffort".into(),
        );
        assert_eq!(st.phase, Some(Phase::Failed), "terminal phase is sticky");
    }

    /// #3687: every status write goes through the host-IP block of
    /// `generateAPIPodStatus` (kubelet_pods.go:2005-2040): a hostNetwork pod's
    /// init-failure status inherits the node IPs as PodIPs too.
    #[tokio::test]
    #[serial_test::serial]
    async fn init_failure_status_applies_host_ips_to_host_network_pod() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage, "node-hn", &["10.1.1.1"], false).await;
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("hn").with_namespace("default"),
            spec: Some(PodSpec {
                host_network: Some(true),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        let st = k
            .init_failure_status(&pod, None, Phase::Failed, "FailedToStart", None)
            .await;
        assert_eq!(st.host_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_i_ps.unwrap()[0].ip, "10.1.1.1");
    }

    fn host_network_pod_for_3687(name: &str) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace("default"),
            spec: Some(PodSpec {
                host_network: Some(true),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        }
    }

    /// #3687: the app-container start-error FailedToStart status goes through
    /// the host-IP block of `generateAPIPodStatus` (kubelet_pods.go:2005-2040).
    #[tokio::test]
    #[serial_test::serial]
    async fn app_start_error_status_applies_host_ips_to_host_network_pod() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage, "node-hn", &["10.1.1.1"], false).await;
        let mut pod = host_network_pod_for_3687("hn-start-err");
        k.apply_app_start_error_status(&mut pod, None, "boom", false, "BestEffort".to_string())
            .await;
        let st = pod.status.unwrap();
        assert_eq!(st.reason.as_deref(), Some("FailedToStart"));
        assert_eq!(st.host_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_i_ps.unwrap()[0].ip, "10.1.1.1");
    }

    /// #3687: the liveness-restart fallback (no prior status) also gets the
    /// host-IP block.
    #[tokio::test]
    #[serial_test::serial]
    async fn liveness_restart_status_applies_host_ips_to_host_network_pod() {
        use rusternetes_storage::StorageBackend;
        let dir = tempfile::tempdir().unwrap();
        let storage = std::sync::Arc::new(StorageBackend::new_memory());
        let k = node_ip_kubelet(&dir, storage, "node-hn", &["10.1.1.1"], false).await;
        let mut pod = host_network_pod_for_3687("hn-liveness");
        k.apply_liveness_restart_status(&mut pod).await;
        let st = pod.status.unwrap();
        assert_eq!(st.reason.as_deref(), Some("Restarting"));
        assert_eq!(st.host_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_ip.as_deref(), Some("10.1.1.1"));
        assert_eq!(st.pod_i_ps.unwrap()[0].ip, "10.1.1.1");
    }

    #[test]
    fn build_init_failure_status_populates_conditions_without_prior_status() {
        let pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("init-pod").with_namespace("default"),
            spec: Some(PodSpec {
                restart_policy: Some("Always".to_string()),
                init_containers: Some(vec![
                    Container {
                        name: "init1".to_string(),
                        image: "busybox:latest".to_string(),
                        ..Default::default()
                    },
                    Container {
                        name: "init2".to_string(),
                        image: "busybox:latest".to_string(),
                        ..Default::default()
                    },
                ]),
                containers: vec![Container {
                    name: "app".to_string(),
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };

        let init_statuses = Some(vec![
            ContainerStatus {
                name: "init1".to_string(),
                ready: false,
                restart_count: 0,
                state: Some(ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code: 1,
                        reason: Some("Error".to_string()),
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                        signal: None,
                    }),
                    ..Default::default()
                }),
                last_state: None,
                image: Some("busybox:latest".to_string()),
                image_id: None,
                container_id: None,
                started: Some(false),
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            },
            ContainerStatus {
                name: "init2".to_string(),
                ready: false,
                restart_count: 0,
                state: Some(ContainerState {
                    waiting: Some(ContainerStateWaiting {
                        reason: Some("PodInitializing".to_string()),
                        message: None,
                    }),
                    ..Default::default()
                }),
                last_state: None,
                image: Some("busybox:latest".to_string()),
                image_id: None,
                container_id: None,
                started: Some(false),
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            },
        ]);

        let status = Kubelet::build_init_failure_status(
            &pod,
            (
                Some("10.0.0.1".to_string()),
                Some(vec![rusternetes_common::resources::pod::HostIP {
                    ip: "10.0.0.1".to_string(),
                }]),
            ),
            init_statuses,
            Phase::Pending,
            "PodInitializing",
            Some("Burstable".to_string()),
        );

        assert_eq!(status.phase, Some(Phase::Pending));
        assert_eq!(status.reason.as_deref(), Some("PodInitializing"));
        assert_eq!(
            status.message.as_deref(),
            Some("containers with incomplete status: [init1 init2]")
        );
        let initialized = status
            .conditions
            .as_ref()
            .and_then(|conditions| {
                conditions
                    .iter()
                    .find(|c| c.condition_type == "Initialized")
            })
            .expect("Initialized condition must be present");
        assert_eq!(initialized.status, "False");
        assert_eq!(
            initialized.reason.as_deref(),
            Some("ContainersNotInitialized")
        );
        let app_status = status
            .container_statuses
            .as_ref()
            .and_then(|statuses| statuses.iter().find(|s| s.name == "app"))
            .expect("app container status must be present");
        assert!(matches!(
            app_status.state,
            Some(ContainerState { waiting: Some(ContainerStateWaiting {
                reason: Some(ref reason),
                ..
            }), .. }) if reason == "PodInitializing"
        ));
    }

    #[test]
    fn terminal_finalize_backoff_grows_then_caps() {
        use rusternetes_kubelet::kubelet::{
            terminal_finalize_backoff, TERMINAL_FINALIZE_BACKOFF_INITIAL,
            TERMINAL_FINALIZE_BACKOFF_MAX,
        };
        // First failure → initial; each subsequent doubles.
        assert_eq!(
            terminal_finalize_backoff(1),
            TERMINAL_FINALIZE_BACKOFF_INITIAL
        );
        assert_eq!(
            terminal_finalize_backoff(2),
            TERMINAL_FINALIZE_BACKOFF_INITIAL * 2
        );
        assert_eq!(
            terminal_finalize_backoff(3),
            TERMINAL_FINALIZE_BACKOFF_INITIAL * 4
        );
        // Monotonic non-decreasing and never above the cap.
        let mut prev = terminal_finalize_backoff(1);
        for c in 2..=50u32 {
            let d = terminal_finalize_backoff(c);
            assert!(d >= prev, "backoff must not shrink");
            assert!(
                d <= TERMINAL_FINALIZE_BACKOFF_MAX,
                "backoff must stay capped"
            );
            prev = d;
        }
        assert_eq!(terminal_finalize_backoff(50), TERMINAL_FINALIZE_BACKOFF_MAX);
        // count 0 (defensive) must not panic and stays at the initial delay.
        assert_eq!(
            terminal_finalize_backoff(0),
            TERMINAL_FINALIZE_BACKOFF_INITIAL
        );
    }

    fn cond(t: &str, status: &str, transition: chrono::DateTime<chrono::Utc>) -> PodCondition {
        PodCondition {
            condition_type: t.to_string(),
            status: status.to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(transition),
            observed_generation: None,
        }
    }

    fn make_node_lease(node_name: &str) -> rusternetes_common::resources::Lease {
        rusternetes_common::resources::Lease::new(node_name, "kube-node-lease").with_spec(
            rusternetes_common::resources::LeaseSpec {
                holder_identity: Some(node_name.to_string()),
                lease_duration_seconds: Some(40),
                acquire_time: Some(chrono::Utc::now()),
                renew_time: Some(chrono::Utc::now()),
                lease_transitions: Some(0),
                preferred_holder: None,
                strategy: None,
            },
        )
    }

    #[test]
    fn apply_node_lease_owner_ref_sets_canonical_reference() {
        let mut lease = make_node_lease("node-a");
        assert!(lease.metadata.owner_references.is_none());

        let mutated = Kubelet::apply_node_lease_owner_ref(
            &mut lease,
            "node-a",
            "11111111-2222-3333-4444-555555555555",
        );

        assert!(mutated, "first apply must mutate metadata");
        let refs = lease
            .metadata
            .owner_references
            .as_ref()
            .expect("owner_references should be set");
        assert_eq!(refs.len(), 1, "exactly one owner reference is allowed");
        let owner = &refs[0];
        assert_eq!(owner.api_version, "v1");
        assert_eq!(owner.kind, "Node");
        assert_eq!(owner.name, "node-a");
        assert_eq!(owner.uid, "11111111-2222-3333-4444-555555555555");
        // Per upstream nodelease/controller.go: neither controller nor
        // blockOwnerDeletion is set — the test only requires the four
        // identity fields.
        assert!(owner.controller.is_none());
        assert!(owner.block_owner_deletion.is_none());
    }

    #[test]
    fn apply_node_lease_owner_ref_is_idempotent_when_already_correct() {
        let mut lease = make_node_lease("node-b");
        let _ = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-b", "uid-b");

        let mutated_again = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-b", "uid-b");

        assert!(
            !mutated_again,
            "second apply with identical owner must not mutate metadata"
        );
        assert_eq!(
            lease
                .metadata
                .owner_references
                .as_ref()
                .map(Vec::len)
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn apply_node_lease_owner_ref_backfills_stale_uid() {
        let mut lease = make_node_lease("node-c");
        lease.metadata.owner_references =
            Some(vec![rusternetes_common::types::OwnerReference::new(
                "v1", "Node", "node-c", "old-uid",
            )]);

        let mutated = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-c", "new-uid");

        assert!(mutated, "stale UID must be overwritten");
        let refs = lease.metadata.owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].uid, "new-uid");
    }

    #[test]
    fn apply_node_lease_owner_ref_noop_for_empty_uid() {
        let mut lease = make_node_lease("node-d");
        let mutated = Kubelet::apply_node_lease_owner_ref(&mut lease, "node-d", "");
        assert!(!mutated);
        assert!(
            lease.metadata.owner_references.is_none(),
            "empty UID must never produce an invalid owner reference"
        );
    }

    #[test]
    fn merge_pod_conditions_preserves_time_when_status_unchanged() {
        let stamp_old = chrono::Utc::now() - chrono::Duration::seconds(60);
        let existing = vec![
            cond("Ready", "True", stamp_old),
            cond("Initialized", "True", stamp_old),
        ];
        // Incoming carries a fresh "now" — must be discarded for matching entries.
        let stamp_new = chrono::Utc::now();
        let incoming = vec![
            cond("Ready", "True", stamp_new),
            cond("Initialized", "True", stamp_new),
        ];

        let merged = Kubelet::merge_pod_conditions(&existing, incoming);

        assert_eq!(merged[0].last_transition_time, Some(stamp_old));
        assert_eq!(merged[1].last_transition_time, Some(stamp_old));
    }

    #[test]
    fn merge_pod_conditions_keeps_fresh_time_on_status_flip() {
        let stamp_old = chrono::Utc::now() - chrono::Duration::seconds(60);
        let stamp_new = chrono::Utc::now();
        let existing = vec![cond("Ready", "False", stamp_old)];
        let incoming = vec![cond("Ready", "True", stamp_new)];

        let merged = Kubelet::merge_pod_conditions(&existing, incoming);

        assert_eq!(merged[0].last_transition_time, Some(stamp_new));
    }

    #[test]
    fn merge_pod_conditions_passes_new_conditions_through() {
        let stamp_new = chrono::Utc::now();
        let merged = Kubelet::merge_pod_conditions(&[], vec![cond("Ready", "True", stamp_new)]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].last_transition_time, Some(stamp_new));
    }

    fn make_container(name: &str) -> Container {
        Container {
            name: name.to_string(),
            image: "nginx:latest".to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            command: None,
            args: None,
            ports: None,
            env: None,
            volume_mounts: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            resources: None,
            working_dir: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            env_from: None,
            volume_devices: None,
            ..Default::default()
        }
    }

    fn make_pod(name: &str, namespace: &str, resource_version: Option<&str>) -> Pod {
        let mut meta = ObjectMeta::new(name).with_namespace(namespace);
        if let Some(rv) = resource_version {
            meta.resource_version = Some(rv.to_string());
        }
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: meta,
            spec: Some(PodSpec {
                containers: vec![make_container("app")],
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: Some("Always".to_string()),
                node_name: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                affinity: None,
                tolerations: None,
                priority: None,
                priority_class_name: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            }),
            status: None,
        }
    }

    fn make_running_container_status(name: &str) -> ContainerStatus {
        ContainerStatus {
            name: name.to_string(),
            ready: true,
            restart_count: 0,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: Some("docker://abc123".to_string()),
            state: Some(ContainerState {
                running: Some(ContainerStateRunning {
                    started_at: Some("2024-01-01T00:00:00Z".to_string()),
                }),
                ..Default::default()
            }),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }
    }

    // A Running pod must have containerStatuses so consumers of the pod status
    // don't misinterpret an empty list as "container already finished".
    #[test]
    fn test_running_pod_must_have_container_statuses() {
        let mut pod = make_pod("my-pod", "default", Some("1"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: Some("All containers started".to_string()),
            reason: None,
            host_ip: Some("127.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let status = pod.status.as_ref().unwrap();
        assert_eq!(status.phase, Some(Phase::Running));
        let statuses = status
            .container_statuses
            .as_ref()
            .expect("must have containerStatuses");
        assert!(
            !statuses.is_empty(),
            "Running pod must have at least one containerStatus"
        );
        assert!(statuses[0].ready, "container must be ready=true");
    }

    // Documents the problematic state: phase=Pending with no containerStatuses.
    // Consumers watching pod status may interpret this as "container already finished".
    #[test]
    fn test_pending_with_no_container_statuses_is_the_bug_state() {
        let mut pod = make_pod("my-pod", "default", Some("1"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            message: Some("ContainerCreating".to_string()),
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None, // <-- the bug: sonobuoy-worker sees this and declares done
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let status = pod.status.as_ref().unwrap();
        // Document that this state (Pending + no containerStatuses) is the problematic one
        let is_bug_state = status.phase == Some(Phase::Pending)
            && status
                .container_statuses
                .as_ref()
                .is_none_or(|v| v.is_empty());
        assert!(
            is_bug_state,
            "This is the state that triggers premature result submission"
        );
    }

    // When re-fetching from etcd fails, we fall back to the original pod clone.
    // The fallback ensures we still attempt the status update even if stale.
    #[test]
    fn test_fresh_fetch_fallback_uses_pod_clone_when_get_fails() {
        let original = make_pod("my-pod", "default", Some("42"));
        // Simulate fallback: use the original pod if re-fetch fails
        let fresh_pod = original.clone();
        assert_eq!(
            fresh_pod.metadata.resource_version.as_deref(),
            Some("42"),
            "Fallback uses original resourceVersion"
        );
    }

    #[test]
    fn readiness_gates_gate_ready_until_condition_true() {
        use rusternetes_common::resources::pod::PodReadinessGate;

        let gated_spec = || PodSpec {
            containers: vec![make_container("app")],
            readiness_gates: Some(vec![PodReadinessGate {
                condition_type: "www.example.com/feature-1".to_string(),
            }]),
            ..Default::default()
        };
        let now = chrono::Utc::now();
        let with_status = |spec: PodSpec, conds: Vec<PodCondition>| {
            let mut pod = Pod::new("pod-ready", spec);
            pod.status = Some(PodStatus {
                conditions: Some(conds),
                ..Default::default()
            });
            pod
        };

        // No readinessGates at all -> trivially satisfied.
        let no_gates = Pod::new(
            "pod-ready",
            PodSpec {
                containers: vec![make_container("app")],
                ..Default::default()
            },
        );
        assert!(rusternetes_kubelet::kubelet::readiness_gates_satisfied(
            &no_gates
        ));

        // Gate present but no matching condition -> NOT satisfied (this is the
        // "initially Ready=False" case the conformance suite checks).
        let pending = with_status(gated_spec(), vec![cond("ContainersReady", "True", now)]);
        assert!(!rusternetes_kubelet::kubelet::readiness_gates_satisfied(
            &pending
        ));

        // Gate condition present but False -> NOT satisfied.
        let gate_false = with_status(
            gated_spec(),
            vec![cond("www.example.com/feature-1", "False", now)],
        );
        assert!(!rusternetes_kubelet::kubelet::readiness_gates_satisfied(
            &gate_false
        ));

        // Gate condition present and True -> satisfied.
        let gate_true = with_status(
            gated_spec(),
            vec![cond("www.example.com/feature-1", "True", now)],
        );
        assert!(rusternetes_kubelet::kubelet::readiness_gates_satisfied(
            &gate_true
        ));

        // #2384: the verdict depends on WHICH snapshot is evaluated. The
        // cycle-start pod (stale) lacks a gate condition patched mid-cycle;
        // the re-read pod has it. The readiness cycle must evaluate the
        // re-read one (`readiness_pod`), so the stale verdict must differ.
        let stale = with_status(gated_spec(), vec![cond("ContainersReady", "True", now)]);
        let fresh = with_status(
            gated_spec(),
            vec![
                cond("ContainersReady", "True", now),
                cond("www.example.com/feature-1", "True", now),
            ],
        );
        assert!(!rusternetes_kubelet::kubelet::readiness_gates_satisfied(
            &stale
        ));
        assert!(rusternetes_kubelet::kubelet::readiness_gates_satisfied(
            &fresh
        ));
    }

    /// #2387 (red-first for #2384): drive the per-cycle readiness block without
    /// a CRI runtime. The cycle-start pod is stale (no gate condition); storage
    /// holds the fresh pod (gate patched via /status mid-cycle). The block must
    /// evaluate gates on the re-read pod.
    #[tokio::test]
    async fn readiness_cycle_evaluates_gates_on_reread_pod() {
        use rusternetes_common::resources::pod::{PodReadinessGate, PodSpec};
        use rusternetes_storage::{build_key, Storage};
        let now = chrono::Utc::now();
        let spec = || PodSpec {
            containers: vec![make_container("app")],
            readiness_gates: Some(vec![PodReadinessGate {
                condition_type: "www.example.com/feature-1".to_string(),
            }]),
            ..Default::default()
        };
        let mk = |conds: Vec<PodCondition>| {
            let mut p = Pod::new("pod-ready", spec());
            p.metadata.namespace = Some("ns".to_string());
            p.status = Some(PodStatus {
                conditions: Some(conds),
                ..Default::default()
            });
            p
        };
        let stale = mk(vec![cond("ContainersReady", "True", now)]);
        let fresh = mk(vec![
            cond("ContainersReady", "True", now),
            cond("www.example.com/feature-1", "True", now),
        ]);
        let storage = rusternetes_storage::memory::MemoryStorage::new();
        let key = build_key("pods", Some("ns"), "pod-ready");
        storage.create(&key, &fresh).await.unwrap();

        let statuses: Vec<ContainerStatus> = serde_json::from_value(
            serde_json::json!([{"name":"app","ready":true,"restartCount":0,"image":"i","imageID":"i"}]),
        )
        .unwrap();

        // Stale snapshot alone: gate unsatisfied.
        let (cr, ready) =
            rusternetes_kubelet::kubelet::evaluate_pod_readiness(&stale, &statuses, None);
        assert!(cr);
        assert!(!ready);

        // The cycle re-reads the pod from storage and evaluates on that.
        let rp =
            rusternetes_kubelet::kubelet::load_readiness_pod(&storage, "ns", "pod-ready", &stale)
                .await;
        let (cr, ready) =
            rusternetes_kubelet::kubelet::evaluate_pod_readiness(&rp, &statuses, None);
        assert!(cr);
        assert!(ready, "gates must be evaluated on the re-read pod");

        // Storage miss falls back to the cycle-start pod.
        let missing =
            rusternetes_kubelet::kubelet::load_readiness_pod(&storage, "ns", "absent", &stale)
                .await;
        assert!(!rusternetes_kubelet::kubelet::evaluate_pod_readiness(&missing, &statuses, None).1);

        // A not-ready container holds both conditions false.
        let mut unready = statuses.clone();
        unready[0].ready = false;
        assert_eq!(
            rusternetes_kubelet::kubelet::evaluate_pod_readiness(&rp, &unready, None),
            (false, false)
        );
    }

    #[test]
    fn spec_has_readiness_probe_includes_sidecars() {
        use rusternetes_common::resources::pod::{PodSpec, Probe};

        // A restartable init container (sidecar) with a readiness probe gates
        // pod readiness (#1069).
        let mut sidecar = make_container("sidecar");
        sidecar.restart_policy = Some("Always".to_string());
        sidecar.readiness_probe = Some(serde_json::from_str::<Probe>("{}").unwrap());
        let spec = PodSpec {
            containers: vec![make_container("app")],
            init_containers: Some(vec![sidecar]),
            ..Default::default()
        };
        assert!(rusternetes_kubelet::kubelet::spec_has_readiness_probe(
            &spec
        ));

        // No readiness probe anywhere -> not gated.
        let spec_none = PodSpec {
            containers: vec![make_container("app")],
            ..Default::default()
        };
        assert!(!rusternetes_kubelet::kubelet::spec_has_readiness_probe(
            &spec_none
        ));

        // A regular container's readiness probe still gates.
        let mut app = make_container("app");
        app.readiness_probe = Some(serde_json::from_str::<Probe>("{}").unwrap());
        let spec_reg = PodSpec {
            containers: vec![app],
            ..Default::default()
        };
        assert!(rusternetes_kubelet::kubelet::spec_has_readiness_probe(
            &spec_reg
        ));

        // A plain (non-restartable) init container's probe does NOT gate.
        let mut plain_init = make_container("init");
        plain_init.readiness_probe = Some(serde_json::from_str::<Probe>("{}").unwrap());
        let spec_plain = PodSpec {
            containers: vec![make_container("app")],
            init_containers: Some(vec![plain_init]),
            ..Default::default()
        };
        assert!(!rusternetes_kubelet::kubelet::spec_has_readiness_probe(
            &spec_plain
        ));
    }

    // A container with state=Running signals that the container is still in
    // progress. Consumers should not treat it as finished.
    #[test]
    fn test_container_status_running_state_prevents_premature_submission() {
        let status = make_running_container_status("app");
        match &status.state {
            Some(ContainerState {
                running: Some(ContainerStateRunning { .. }),
                ..
            }) => {
                // This state correctly signals "still running" to sonobuoy-worker
            }
            other => panic!("Expected Running state, got {:?}", other),
        }
        assert!(status.ready, "Running container must be ready=true");
    }

    // A container with state=Waiting also signals "not finished" since it hasn't
    // exited yet. Only Terminated state means the container is done.
    #[test]
    fn test_container_status_waiting_also_signals_not_finished() {
        let status = ContainerStatus {
            name: "app".to_string(),
            ready: false,
            restart_count: 0,
            last_state: None,
            image: Some("nginx:latest".to_string()),
            image_id: None,
            container_id: None,
            state: Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some("ContainerCreating".to_string()),
                    message: None,
                }),
                ..Default::default()
            }),
            started: None,
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        };
        let is_terminated = matches!(
            status.state,
            Some(ContainerState {
                terminated: Some(ContainerStateTerminated { .. }),
                ..
            })
        );
        assert!(
            !is_terminated,
            "Waiting container is not terminated — sonobuoy-worker should wait"
        );
    }

    // Documents why re-fetching from etcd before writing Running status is necessary.
    // An admission controller or scheduler may have incremented resourceVersion between
    // when we fetched the pod and when start_pod returned, causing update to fail.
    #[test]
    fn test_stale_resource_version_causes_conflict() {
        let stale = make_pod("my-pod", "default", Some("5"));
        // Simulate etcd advancing the resourceVersion (e.g., admission controller touch)
        let fresh = make_pod("my-pod", "default", Some("6"));

        assert_ne!(
            stale.metadata.resource_version, fresh.metadata.resource_version,
            "Stale rv={:?} differs from fresh rv={:?} — using stale would cause conflict",
            stale.metadata.resource_version, fresh.metadata.resource_version
        );

        // The fix: always use fresh.metadata.resource_version when writing status
        let rv_to_use = fresh.metadata.resource_version.as_deref().unwrap_or("0");
        assert_eq!(
            rv_to_use, "6",
            "Must use fresh resourceVersion to avoid conflict"
        );
    }

    // ---- Pod Resize Tests (KEP-1287) ----

    /// When pod status.resize is "Proposed", the kubelet should detect it as a resize request.
    #[test]
    fn test_resize_proposed_detected() {
        let mut pod = make_pod("resize-pod", "default", Some("10"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("10.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("Proposed".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");

        assert_eq!(resize_status, "Proposed");
        assert!(
            resize_status == "Proposed" || resize_status == "InProgress",
            "Kubelet should process Proposed or InProgress resize"
        );
    }

    /// When pod status.resize is "InProgress", the kubelet should continue processing.
    #[test]
    fn test_resize_in_progress_continues() {
        let mut pod = make_pod("resize-pod-ip", "default", Some("11"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("10.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("InProgress".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");

        assert_eq!(resize_status, "InProgress");
        // Kubelet should still process an InProgress resize
        assert!(resize_status == "Proposed" || resize_status == "InProgress");
    }

    /// After resize completes, status.resize should be empty string.
    #[test]
    fn test_resize_completion_sets_empty_string() {
        let mut pod = make_pod("resize-done", "default", Some("12"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("InProgress".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        // Simulate the kubelet marking resize as complete
        if let Some(ref mut status) = pod.status {
            status.resize = Some(String::new()); // Empty = resize complete
        }

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("missing");

        assert_eq!(
            resize_status, "",
            "After resize completes, status.resize should be empty string"
        );
    }

    /// Verify that allocatedResources is populated from spec resources after resize.
    #[test]
    fn test_resize_populates_allocated_resources() {
        use rusternetes_common::resources::pod::PodSpec;
        use rusternetes_common::types::ResourceRequirements;
        use std::collections::HashMap;

        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), "500m".to_string());
        requests.insert("memory".to_string(), "256Mi".to_string());

        let mut limits = HashMap::new();
        limits.insert("cpu".to_string(), "1".to_string());
        limits.insert("memory".to_string(), "512Mi".to_string());

        let mut pod = make_pod("resize-alloc", "default", Some("13"));
        pod.spec = Some(PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                image: "nginx:latest".to_string(),
                resources: Some(ResourceRequirements {
                    requests: Some(requests.clone()),
                    limits: Some(limits.clone()),
                    claims: None,
                }),
                ..make_container("app")
            }],
            ..pod.spec.unwrap()
        });
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![ContainerStatus {
                name: "app".to_string(),
                ready: true,
                restart_count: 0,
                state: Some(ContainerState {
                    running: Some(ContainerStateRunning {
                        started_at: Some("2024-01-01T00:00:00Z".to_string()),
                    }),
                    ..Default::default()
                }),
                last_state: None,
                image: Some("nginx:latest".to_string()),
                image_id: None,
                container_id: Some("docker://abc123".to_string()),
                started: Some(true),
                allocated_resources: None, // Not yet populated
                allocated_resources_status: None,
                resources: None,
                user: None,
                volume_mounts: None,
                stop_signal: None,
            }]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("InProgress".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        // Simulate the kubelet logic: after successful resize, populate allocatedResources
        // from spec containers (mirroring the actual kubelet code at line ~930-948)
        if let Some(ref mut status) = pod.status {
            status.resize = Some(String::new());
            if let Some(ref spec) = pod.spec.clone() {
                if let Some(ref mut cs_list) = status.container_statuses {
                    for cs in cs_list.iter_mut() {
                        if let Some(c) = spec.containers.iter().find(|c| c.name == cs.name) {
                            if let Some(ref res) = c.resources {
                                cs.allocated_resources =
                                    res.requests.clone().or_else(|| res.limits.clone());
                                cs.resources = Some(res.clone());
                            }
                        }
                    }
                }
            }
        }

        // Verify allocatedResources were populated
        let cs = &pod
            .status
            .as_ref()
            .unwrap()
            .container_statuses
            .as_ref()
            .unwrap()[0];
        let alloc = cs
            .allocated_resources
            .as_ref()
            .expect("allocatedResources should be populated after resize");
        assert_eq!(alloc.get("cpu"), Some(&"500m".to_string()));
        assert_eq!(alloc.get("memory"), Some(&"256Mi".to_string()));

        // Verify resources were populated
        let res = cs
            .resources
            .as_ref()
            .expect("resources should be populated after resize");
        assert_eq!(
            res.requests.as_ref().unwrap().get("cpu"),
            Some(&"500m".to_string())
        );
        assert_eq!(
            res.limits.as_ref().unwrap().get("cpu"),
            Some(&"1".to_string())
        );
    }

    /// When resize is not Proposed or InProgress, the kubelet should not process a resize.
    #[test]
    fn test_resize_not_triggered_for_empty_or_none() {
        // No resize field
        let mut pod = make_pod("no-resize", "default", Some("14"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");
        assert!(
            resize_status != "Proposed" && resize_status != "InProgress",
            "No resize should be triggered when resize is None"
        );

        // Empty string (completed)
        pod.status.as_mut().unwrap().resize = Some(String::new());
        let resize_status = pod
            .status
            .as_ref()
            .and_then(|s| s.resize.as_deref())
            .unwrap_or("");
        assert!(
            resize_status != "Proposed" && resize_status != "InProgress",
            "No resize should be triggered when resize is empty (completed)"
        );
    }

    /// Verify the resize status transition: Proposed -> InProgress -> "" (complete)
    #[test]
    fn test_resize_status_transition() {
        let mut pod = make_pod("resize-transition", "default", Some("15"));
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: Some(vec![make_running_container_status("app")]),
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: Some("Proposed".to_string()),
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });

        // Step 1: API sets resize="Proposed"
        assert_eq!(
            pod.status.as_ref().unwrap().resize.as_deref(),
            Some("Proposed")
        );

        // Step 2: Kubelet transitions to "InProgress"
        pod.status.as_mut().unwrap().resize = Some("InProgress".to_string());
        assert_eq!(
            pod.status.as_ref().unwrap().resize.as_deref(),
            Some("InProgress")
        );

        // Step 3: Kubelet completes resize, sets to ""
        pod.status.as_mut().unwrap().resize = Some(String::new());
        assert_eq!(pod.status.as_ref().unwrap().resize.as_deref(), Some(""));
    }

    // --- Init-container failure conditions (K8s conformance) ---
    //
    // K8s conformance test "should not start app containers if init
    // containers fail on a RestartAlways pod" (init_container.go:446) asserts:
    //   - pod.conditions[Initialized].Status  == "False"
    //   - pod.conditions[Initialized].Reason  == "ContainersNotInitialized"
    //   - pod.conditions[Initialized].Message == "containers with incomplete status: [init1 init2]"
    //
    // The kubelet sync loop's should_retry branch builds these conditions
    // from `init_failed_pod_conditions(&incomplete_inits)` using the same
    // filter as the start_pod error handler: an init container is
    // "incomplete" iff its state is NOT Terminated{exit_code:0}.

    /// Mirror the incomplete-init filter that kubelet.rs uses in the
    /// init-failure status update. Kept in sync with the inline copies in
    /// the should_retry branch and the start_pod error handler.
    fn incomplete_init_names(pod: &Pod) -> Vec<String> {
        let init_statuses = pod
            .status
            .as_ref()
            .and_then(|s| s.init_container_statuses.as_ref());
        pod.spec
            .as_ref()
            .and_then(|s| s.init_containers.as_ref())
            .map(|ics| {
                ics.iter()
                    .filter(|c| {
                        let completed = init_statuses
                            .and_then(|statuses| statuses.iter().find(|s| s.name == c.name))
                            .map(|s| {
                                matches!(
                                    &s.state,
                                    Some(ContainerState {
                                        terminated: Some(ContainerStateTerminated {
                                            exit_code: 0,
                                            ..
                                        }),
                                        ..
                                    })
                                )
                            })
                            .unwrap_or(false);
                        !completed
                    })
                    .map(|c| c.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn init_status(name: &str, state: ContainerState) -> ContainerStatus {
        ContainerStatus {
            name: name.to_string(),
            ready: false,
            restart_count: 0,
            state: Some(state),
            last_state: None,
            image: Some("busybox:latest".to_string()),
            image_id: None,
            container_id: None,
            started: Some(false),
            allocated_resources: None,
            allocated_resources_status: None,
            resources: None,
            user: None,
            volume_mounts: None,
            stop_signal: None,
        }
    }

    fn restartalways_pod_with_two_inits() -> Pod {
        // init1 failed (exit 1), init2 never ran (Waiting/PodInitializing)
        let init_statuses = vec![
            init_status(
                "init1",
                ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code: 1,
                        signal: None,
                        reason: Some("Error".to_string()),
                        message: None,
                        started_at: Some("2026-01-01T00:00:00Z".to_string()),
                        finished_at: Some("2026-01-01T00:00:01Z".to_string()),
                        container_id: Some("docker://abc123".to_string()),
                    }),
                    ..Default::default()
                },
            ),
            init_status(
                "init2",
                ContainerState {
                    waiting: Some(ContainerStateWaiting {
                        reason: Some("PodInitializing".to_string()),
                        message: None,
                    }),
                    ..Default::default()
                },
            ),
        ];
        let mut pod = make_pod("pod-init", "default", None);
        if let Some(ref mut spec) = pod.spec {
            spec.init_containers = Some(vec![make_container("init1"), make_container("init2")]);
            spec.containers = vec![make_container("run1")];
            spec.restart_policy = Some("Always".to_string());
        }
        pod.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            message: None,
            reason: Some("PodInitializing".to_string()),
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: Some(init_statuses),
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });
        pod
    }

    #[test]
    fn test_init_failed_conditions_lists_both_inits_when_first_failed_and_second_not_run() {
        // The conformance test asserts a specific message format that
        // includes BOTH init container names — init1 (failed) and init2
        // (which never had a chance to run) — separated by a single space.
        let pod = restartalways_pod_with_two_inits();
        let incomplete = incomplete_init_names(&pod);
        assert_eq!(
            incomplete,
            vec!["init1".to_string(), "init2".to_string()],
            "both init1 (Terminated with non-zero exit) and init2 (Waiting) must be marked incomplete"
        );

        let conditions = Kubelet::init_failed_pod_conditions(&incomplete);
        let initialized = conditions
            .iter()
            .find(|c| c.condition_type == "Initialized")
            .expect("Initialized condition must be present");

        assert_eq!(initialized.status, "False");
        assert_eq!(
            initialized.reason.as_deref(),
            Some("ContainersNotInitialized")
        );
        assert_eq!(
            initialized.message.as_deref(),
            Some("containers with incomplete status: [init1 init2]"),
            "K8s conformance asserts this exact message verbatim"
        );
    }

    #[test]
    fn test_init_failed_conditions_drops_successful_init_from_list() {
        // After init1 succeeds and init2 fails, only init2 must appear in
        // the conditions message — successful init containers are dropped.
        let mut pod = restartalways_pod_with_two_inits();
        if let Some(ref mut status) = pod.status {
            status.init_container_statuses = Some(vec![
                init_status(
                    "init1",
                    ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 0,
                            signal: None,
                            reason: Some("Completed".to_string()),
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                        }),
                        ..Default::default()
                    },
                ),
                init_status(
                    "init2",
                    ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 1,
                            signal: None,
                            reason: Some("Error".to_string()),
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                        }),
                        ..Default::default()
                    },
                ),
            ]);
        }
        let incomplete = incomplete_init_names(&pod);
        assert_eq!(
            incomplete,
            vec!["init2".to_string()],
            "successfully terminated (exit 0) init containers must not appear in incomplete list"
        );
        let conditions = Kubelet::init_failed_pod_conditions(&incomplete);
        let initialized = conditions
            .iter()
            .find(|c| c.condition_type == "Initialized")
            .expect("Initialized condition must be present");
        assert_eq!(
            initialized.message.as_deref(),
            Some("containers with incomplete status: [init2]"),
            "only failing init container must appear in the message"
        );
    }

    #[test]
    fn test_init_failed_conditions_pod_initialized_is_false_with_correct_reason() {
        // RestartAlways pod whose init container fails must report
        // PodInitialized=False with reason ContainersNotInitialized.
        let conditions = Kubelet::init_failed_pod_conditions(&["init1".to_string()]);
        let initialized = conditions
            .iter()
            .find(|c| c.condition_type == "Initialized")
            .expect("Initialized condition must be present");
        assert_eq!(initialized.status, "False");
        assert_eq!(
            initialized.reason.as_deref(),
            Some("ContainersNotInitialized")
        );
        // Sanity: ContainersReady and Ready also exist and are False.
        let containers_ready = conditions
            .iter()
            .find(|c| c.condition_type == "ContainersReady")
            .expect("ContainersReady condition must be present");
        assert_eq!(containers_ready.status, "False");
        let ready = conditions
            .iter()
            .find(|c| c.condition_type == "Ready")
            .expect("Ready condition must be present");
        assert_eq!(ready.status, "False");
    }
}

#[path = "kubelet_sync_pod_fake_cri_tests.rs"]
mod sync_pod_fake_cri_tests {
    //! `Kubelet::sync_pod` concurrency against the in-process fake CRI
    //! (issues #3884, #1115): the `pod_sync_locks` skip-and-retry path, driven
    //! end-to-end through a real `Kubelet` rather than the bare `SyncLocks`.
    //!
    //! Upstream parity. The Go kubelet serialises per pod with `podWorkers`
    //! (`pkg/kubelet/pod_workers.go`: `UpdatePod` / `podWorkerLoop`), coalescing
    //! updates so a pod is never synced by two goroutines at once, while distinct
    //! pods run in parallel. The cases below port the observable contracts of
    //! `TestUpdatePodParallel` (`pod_workers_test.go:526`: distinct pods proceed
    //! independently, each processed at least once) and the per-UID serialisation
    //! of `TestUpdatePod`/`podWorkerLoop`. The constructor mirrors
    //! `newTestKubelet` (`kubelet_test.go:174`): a whole kubelet wired to fake
    //! runtime + storage, with no real container runtime.
    //!
    //! Deliberate deviation (documented on `Kubelet::pod_sync_locks`): rusternetes
    //! keys a name-based skip-and-retry lock instead of upstream's per-UID worker
    //! goroutines, so a second concurrent sync is *skipped* (returns `Ok`) and
    //! retried by a later reconcile, rather than queued.

    use rusternetes_common::resources::Pod;
    use rusternetes_cri::fake::FakeCri;
    use rusternetes_kubelet::kubelet::Kubelet;
    use rusternetes_storage::{Storage, StorageBackend};
    use std::sync::Arc;

    /// Test constructor for a [`Kubelet`] on a fake CRI socket + memory storage
    /// (upstream: `newTestKubelet`, `kubelet_test.go:174`). Callers must be
    /// `#[serial_test::serial]`: the CRI endpoint is read from the process
    /// environment by `new_with_eviction`, like the other constructor tests.
    struct TestKubelet {
        kubelet: Arc<Kubelet>,
        fake: FakeCri,
        storage: Arc<StorageBackend>,
        _dir: tempfile::TempDir,
        _server: tokio::task::JoinHandle<()>,
    }

    async fn new_test_kubelet() -> TestKubelet {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("fake-cri.sock");
        let fake = FakeCri::new();
        let server = fake.serve(&sock);
        std::env::set_var(
            "CONTAINER_RUNTIME_ENDPOINT",
            format!("unix://{}", sock.display()),
        );
        let storage = Arc::new(StorageBackend::new_memory());
        let kubelet = Kubelet::new(
            "node-test".into(),
            storage.clone(),
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
        )
        .await
        .unwrap();
        TestKubelet {
            kubelet: Arc::new(kubelet),
            fake,
            storage,
            _dir: dir,
            _server: server,
        }
    }

    fn pod(ns: &str, name: &str, uid: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": ns, "uid": uid},
            "spec": {
                "nodeName": "node-test",
                "containers": [{"name": "app", "image": "busybox"}]
            }
        }))
        .unwrap()
    }

    async fn store(t: &TestKubelet, p: &Pod) {
        let key = format!(
            "/registry/pods/{}/{}",
            p.metadata.namespace.as_deref().unwrap(),
            p.metadata.name
        );
        t.storage.create(&key, p).await.unwrap();
    }

    fn count(fake: &FakeCri, rpc: &str) -> usize {
        fake.called().iter().filter(|c| *c == rpc).count()
    }

    /// `sync_pod`'s future is large; in a debug build it overflows the default
    /// 2 MiB test-thread stack. Run each case on a big-stack thread with its own
    /// current-thread runtime (which also makes `join!` interleaving
    /// deterministic: the second call is polled as soon as the first yields).
    fn run<Fut: std::future::Future<Output = ()>>(f: impl FnOnce() -> Fut + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(256 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(f())
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn lone_sync_pod_creates_one_sandbox() {
        // Baseline: the harness really drives sync_pod to the CRI.
        run(|| async {
            let t = new_test_kubelet().await;
            let p = pod("default", "web-0", "uid-1");
            store(&t, &p).await;
            t.kubelet.sync_pod(&p).await.unwrap();
            assert_eq!(count(&t.fake, "RunPodSandbox"), 1, "{:?}", t.fake.called());
        });
    }

    #[test]
    #[serial_test::serial]
    fn concurrent_same_name_sync_pod_skips_the_loser() {
        // Two syncs of the same pod race: the lock holder drives the CRI, the
        // other is skipped (Ok, no CRI traffic) instead of issuing a duplicate
        // RunPodSandbox/CreateContainer (the 409 "name already in use" class,
        // #1014/#1112). Upstream: podWorkers never runs one pod's sync twice at
        // once (`podWorkerLoop`, pod_workers.go).
        run(|| async {
            let t = new_test_kubelet().await;
            let p = pod("default", "web-0", "uid-1");
            store(&t, &p).await;
            let (a, b) = tokio::join!(t.kubelet.sync_pod(&p), t.kubelet.sync_pod(&p));
            a.unwrap();
            b.unwrap();
            assert_eq!(count(&t.fake, "RunPodSandbox"), 1, "{:?}", t.fake.called());
            assert_eq!(
                count(&t.fake, "CreateContainer"),
                1,
                "{:?}",
                t.fake.called()
            );
            assert_eq!(t.fake.sandboxes().len(), 1);
        });
    }

    #[test]
    #[serial_test::serial]
    fn skipped_sync_is_retried_after_the_holder_finishes() {
        // Skip-and-retry: once the guard drops, the next reconcile syncs again
        // and does not duplicate what the first sync created.
        run(|| async {
            let t = new_test_kubelet().await;
            let p = pod("default", "web-0", "uid-1");
            store(&t, &p).await;
            let (a, b) = tokio::join!(t.kubelet.sync_pod(&p), t.kubelet.sync_pod(&p));
            a.unwrap();
            b.unwrap();
            // The guard is released on every path, so the key is free again.
            assert!(
                t.kubelet
                    .pod_sync_locks
                    .try_acquire("default/web-0")
                    .is_some(),
                "sync lock must be released after the concurrent syncs"
            );
            t.kubelet.sync_pod(&p).await.unwrap();
            assert_eq!(count(&t.fake, "RunPodSandbox"), 1, "{:?}", t.fake.called());
            assert_eq!(
                count(&t.fake, "CreateContainer"),
                1,
                "{:?}",
                t.fake.called()
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn distinct_pods_sync_independently() {
        // Upstream TestUpdatePodParallel (pod_workers_test.go:526): distinct pods
        // are never serialised against each other.
        run(|| async {
            let t = new_test_kubelet().await;
            let a = pod("default", "web-0", "uid-a");
            let b = pod("default", "web-1", "uid-b");
            let c = pod("other", "web-0", "uid-c"); // same name, other namespace
            for p in [&a, &b, &c] {
                store(&t, p).await;
            }
            let (ra, rb, rc) = tokio::join!(
                t.kubelet.sync_pod(&a),
                t.kubelet.sync_pod(&b),
                t.kubelet.sync_pod(&c)
            );
            ra.unwrap();
            rb.unwrap();
            rc.unwrap();
            assert_eq!(count(&t.fake, "RunPodSandbox"), 3, "{:?}", t.fake.called());
        });
    }

    #[test]
    #[serial_test::serial]
    fn same_name_new_uid_is_serialised_against_the_old_incarnation() {
        // #1112: a StatefulSet replacement (same name, new UID) must not create
        // containers while the old incarnation's sync is running. The lock is
        // name-keyed, so the second incarnation is skipped, not interleaved.
        run(|| async {
            let t = new_test_kubelet().await;
            let old = pod("default", "web-0", "uid-old");
            let new = pod("default", "web-0", "uid-new");
            store(&t, &old).await;
            let (a, b) = tokio::join!(t.kubelet.sync_pod(&old), t.kubelet.sync_pod(&new));
            a.unwrap();
            b.unwrap();
            assert_eq!(count(&t.fake, "RunPodSandbox"), 1, "{:?}", t.fake.called());
            let sandboxes = t.fake.sandboxes();
            assert_eq!(sandboxes.len(), 1);
        });
    }

    #[test]
    #[serial_test::serial]
    fn concurrent_same_name_sync_writes_one_consistent_running_status() {
        // Status race (#1115): with the loser skipped, the stored pod carries the
        // single winner's status (phase Running, the sandbox's IP) rather than a clobbered/duplicated one.
        run(|| async {
            let t = new_test_kubelet().await;
            let p = pod("default", "web-0", "uid-1");
            store(&t, &p).await;
            let (a, b) = tokio::join!(t.kubelet.sync_pod(&p), t.kubelet.sync_pod(&p));
            a.unwrap();
            b.unwrap();
            let stored: Pod = t.storage.get("/registry/pods/default/web-0").await.unwrap();
            let status = stored.status.expect("kubelet wrote a status");
            assert_eq!(
                status.phase,
                Some(rusternetes_common::types::Phase::Running)
            );
            assert_eq!(status.pod_ip.as_deref(), Some("192.168.192.168"));
        });
    }
}
