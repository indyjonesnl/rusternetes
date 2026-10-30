//! Unit tests moved from `src/controllers/endpointslice.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::endpointslice::{
        Endpoint, EndpointConditions, EndpointPort, EndpointReference,
    };
    pub use rusternetes_common::resources::{EndpointSlice, Endpoints, Pod, Service};
    pub use rusternetes_common::types::Phase;
    pub use rusternetes_controller_manager::controllers::endpointslice_tracker::EndpointSliceTracker;
    pub use rusternetes_controller_manager::controllers::trigger_time_tracker::{
        format_rfc3339_nano, TriggerTimeTracker,
    };
    pub use rusternetes_storage::{
        build_key, build_prefix, extract_key, Storage, WatchEvent, WorkQueue,
    };
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{ContainerPort, ServicePort, ServiceSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::endpointslice::*;
    use rusternetes_storage::MemoryStorage;

    /// Port of `TestSyncServiceMissing`
    /// (endpointslice_controller_test.go:318-350): syncing a Service that no
    /// longer exists makes no writes and drops only that Service's
    /// trigger-time state (endpointslice_controller.go:388-390).
    #[tokio::test]
    async fn test_sync_service_missing() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        let existing = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("stillthere").with_namespace("default"),
            spec: ServiceSpec {
                selector: Some(HashMap::from([("foo".to_string(), "bar".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        storage
            .create(
                &build_key("services", Some("default"), "stillthere"),
                &existing,
            )
            .await
            .unwrap();
        controller
            .trigger_time_tracker
            .insert_empty_service("default", "stillthere");
        controller
            .trigger_time_tracker
            .insert_empty_service("default", "notthere");

        let queue = WorkQueue::new();
        // Work-queue key format, as in `get_services_to_update`.
        queue.add("services/default/notthere".to_string()).await;
        // The worker loops on `queue.get()` forever; one item is all we feed.
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            controller.worker(queue),
        )
        .await;

        assert!(!controller
            .trigger_time_tracker
            .has_service("default", "notthere"));
        assert!(controller
            .trigger_time_tracker
            .has_service("default", "stillthere"));
        // No client actions: no EndpointSlice was created.
        let slices = storage
            .list::<EndpointSlice>(&build_prefix("endpointslices", Some("default")))
            .await
            .unwrap();
        assert!(slices.is_empty());
    }

    #[tokio::test]
    async fn test_endpointslice_controller_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let _controller = EndpointSliceController::new(storage);
    }

    #[tokio::test]
    async fn test_pod_port_filtering_named_ports() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        // Create a service with two named target ports
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![
                    ServicePort {
                        name: Some("portname1".to_string()),
                        port: 80,
                        target_port: Some(rusternetes_common::resources::IntOrString::String(
                            "svc1".to_string(),
                        )),
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    },
                    ServicePort {
                        name: Some("portname2".to_string()),
                        port: 81,
                        target_port: Some(rusternetes_common::resources::IntOrString::String(
                            "svc2".to_string(),
                        )),
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    },
                ],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };

        // Pod1 only has containerPort named "svc1" (serves portname1 only)
        let pod1 = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("pod1")
                .with_namespace("default")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c1".to_string(),
                    ports: Some(vec![ContainerPort {
                        container_port: 100,
                        name: Some("svc1".to_string()),
                        protocol: "TCP".to_string(),
                        host_port: None,
                        host_ip: None,
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };

        // Pod1 should only get portname1 (svc1→100), NOT portname2 (svc2 not found)
        let ports = controller.get_endpoint_ports(&service, &pod1);
        assert_eq!(ports.len(), 1, "Pod1 should only match portname1");
        assert_eq!(ports[0].name, Some("portname1".to_string()));
        assert_eq!(ports[0].port, Some(100));
    }

    #[tokio::test]
    async fn test_orphan_detection_skips_externally_managed_slices() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        // Create an EndpointSlice NOT managed by the controller
        let mut external_slice = EndpointSlice::new("external-slice", "IPv4");
        external_slice.metadata.namespace = Some("default".to_string());
        let key = build_key("endpointslices", Some("default"), "external-slice");
        storage.create(&key, &external_slice).await.unwrap();

        // Create an EndpointSlice managed by the controller
        let mut managed_slice = EndpointSlice::new("managed-slice", "IPv4");
        managed_slice.metadata.namespace = Some("default".to_string());
        let mut labels = HashMap::new();
        labels.insert(
            "endpointslice.kubernetes.io/managed-by".to_string(),
            "endpointslice-controller.k8s.io".to_string(),
        );
        labels.insert(
            "kubernetes.io/service-name".to_string(),
            "nonexistent-service".to_string(),
        );
        managed_slice.metadata.labels = Some(labels);
        let key2 = build_key("endpointslices", Some("default"), "managed-slice");
        storage.create(&key2, &managed_slice).await.unwrap();

        controller.reconcile_all().await.unwrap();

        // External slice should survive
        assert!(
            storage.get::<EndpointSlice>(&key).await.is_ok(),
            "externally-managed EndpointSlice should NOT be deleted"
        );

        // Managed slice should be deleted (orphaned)
        assert!(
            storage.get::<EndpointSlice>(&key2).await.is_err(),
            "controller-managed orphan EndpointSlice should be deleted"
        );
    }

    /// Regression test: when a service is created before its pods, the
    /// EndpointSlice is initially empty. When pods become ready, the
    /// EndpointSlice must be UPDATED with the pod's IP and ready=true.
    /// Bug: the controller created empty slices but never updated them,
    /// causing kube-proxy to never generate DNAT rules for the service.
    #[tokio::test]
    async fn test_endpointslice_updates_when_pod_becomes_ready() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        // 1. Create a service with one port (port 80, no targetPort)
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("endpoint-test2").with_namespace("test-ns"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        let svc_key = build_key("services", Some("test-ns"), "endpoint-test2");
        storage.create(&svc_key, &service).await.unwrap();

        // 2. Reconcile with no pods — should create empty EndpointSlice
        controller.reconcile_service(&service).await.unwrap();

        let es_key = build_key("endpointslices", Some("test-ns"), "endpoint-test2");
        let es = storage.get::<EndpointSlice>(&es_key).await.unwrap();
        assert!(
            es.endpoints.is_empty(),
            "EndpointSlice should be empty when no pods exist"
        );
        assert!(
            !es.ports.is_empty(),
            "EndpointSlice should have ports from the service"
        );

        // 3. Create a matching pod with IP and Ready condition
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("pod1")
                .with_namespace("test-ns")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "agnhost".to_string(),
                    ports: Some(vec![ContainerPort {
                        container_port: 80,
                        name: None,
                        protocol: "TCP".to_string(),
                        host_port: None,
                        host_ip: None,
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                pod_ip: Some("10.89.0.22".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        let pod_key = build_key("pods", Some("test-ns"), "pod1");
        storage.create(&pod_key, &pod).await.unwrap();

        // 4. Reconcile again — EndpointSlice MUST be updated with the pod
        controller.reconcile_service(&service).await.unwrap();

        let es_updated = storage.get::<EndpointSlice>(&es_key).await.unwrap();
        assert_eq!(
            es_updated.endpoints.len(),
            1,
            "EndpointSlice must have 1 endpoint after pod becomes ready"
        );
        assert_eq!(es_updated.endpoints[0].addresses, vec!["10.89.0.22"]);
        assert_eq!(
            es_updated.endpoints[0]
                .conditions
                .as_ref()
                .and_then(|c| c.ready),
            Some(true),
            "Endpoint must be marked ready"
        );
    }

    /// Terminating pods (with deletionTimestamp) must remain in EndpointSlices
    /// with terminating=true, ready=false. K8s kube-proxy uses these to drain
    /// connections during rolling updates. Dropping them entirely causes
    /// in-flight requests to fail mid-flight.
    #[tokio::test]
    async fn test_endpointslice_keeps_terminating_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        storage
            .create(&build_key("services", Some("default"), "svc"), &service)
            .await
            .unwrap();

        // Pod with deletionTimestamp set (terminating) but still Ready
        let mut pod_meta = ObjectMeta::new("p1")
            .with_namespace("default")
            .with_labels(HashMap::from([("app".to_string(), "test".to_string())]));
        pod_meta.deletion_timestamp = Some(chrono::Utc::now());
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.1".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "p1"), &pod)
            .await
            .unwrap();

        controller.reconcile_service(&service).await.unwrap();

        let es: EndpointSlice = storage
            .get(&build_key("endpointslices", Some("default"), "svc"))
            .await
            .unwrap();

        assert_eq!(
            es.endpoints.len(),
            1,
            "terminating pod must still appear in the EndpointSlice"
        );
        let conds = es.endpoints[0].conditions.as_ref().unwrap();
        assert_eq!(conds.terminating, Some(true), "terminating must be true");
        assert_eq!(
            conds.ready,
            Some(false),
            "ready must be false for terminating pods (unless publishNotReadyAddresses)"
        );
        assert_eq!(
            conds.serving,
            Some(true),
            "serving must mirror Ready condition independent of terminating state"
        );
    }

    /// publishNotReadyAddresses=true forces ready=true and serving=true on every
    /// endpoint, regardless of pod readiness. This is required for headless
    /// services that fronts gossip-style protocols (e.g. peer-discovery) where
    /// clients need to see all pods, including those still starting up.
    #[tokio::test]
    async fn test_endpointslice_publish_not_ready_addresses() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                publish_not_ready_addresses: Some(true),
                ..Default::default()
            },
            status: None,
        };
        storage
            .create(&build_key("services", Some("default"), "svc"), &service)
            .await
            .unwrap();

        // A pod that is Running but NOT Ready
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("p1")
                .with_namespace("default")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.2".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "False".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "p1"), &pod)
            .await
            .unwrap();

        controller.reconcile_service(&service).await.unwrap();

        let es: EndpointSlice = storage
            .get(&build_key("endpointslices", Some("default"), "svc"))
            .await
            .unwrap();
        assert_eq!(es.endpoints.len(), 1);
        let conds = es.endpoints[0].conditions.as_ref().unwrap();
        assert_eq!(
            conds.ready,
            Some(true),
            "publishNotReadyAddresses=true must force ready=true even for unready pods"
        );
        assert_eq!(
            conds.serving,
            Some(true),
            "publishNotReadyAddresses=true must force serving=true"
        );
    }

    /// One pod LIST per namespace per pass, not one per service.
    ///
    /// `reconcile_service` used to LIST every pod in the namespace for each
    /// service it reconciled, so a namespace holding N services cost N
    /// identical namespace-wide LISTs every 5 seconds.
    ///
    /// Measured on a live kine cluster after a conformance run: 154
    /// `latency-svc-*` services in one namespace (from the sig-network
    /// endpoint-latency spec) were being walked at ~1 service per 1.2s. The
    /// sweep could not finish inside its own 5s resync, it kept kine at
    /// ~1013% CPU serving the repeated LISTs, and it starved the rest of the
    /// controller-manager so completely that 335 namespaces sat in
    /// Terminating with zero drain progress over 60 seconds.
    ///
    /// Upstream never does this: pods come from a shared informer cache, and
    /// services are reconciled from a rate-limited workqueue by
    /// `ConcurrentServiceEndpointSyncs` = 5 workers
    /// (pkg/controller/endpointslice/endpointslice_controller.go:106-118,
    /// endpointslice/config/v1alpha1/defaults.go:35).
    #[tokio::test]
    async fn one_pod_list_per_namespace_not_per_service() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingStorage {
            inner: Arc<MemoryStorage>,
            pod_lists: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl Storage for CountingStorage {
            async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.create(key, value).await
            }
            async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
            where
                T: serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.get(key).await
            }
            async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.update(key, value).await
            }
            async fn update_raw(
                &self,
                key: &str,
                value: &serde_json::Value,
            ) -> rusternetes_common::Result<()> {
                self.inner.update_raw(key, value).await
            }
            async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
                self.inner.delete(key).await
            }
            async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                if prefix.starts_with("/registry/pods/") {
                    self.pod_lists.fetch_add(1, Ordering::SeqCst);
                }
                self.inner.list(prefix).await
            }
            async fn watch(
                &self,
                prefix: &str,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch(prefix).await
            }
            async fn watch_from_revision(
                &self,
                prefix: &str,
                revision: i64,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch_from_revision(prefix, revision).await
            }
            async fn current_revision(&self) -> rusternetes_common::Result<i64> {
                self.inner.current_revision().await
            }
            async fn is_revision_compacted(
                &self,
                revision: i64,
            ) -> rusternetes_common::Result<bool> {
                self.inner.is_revision_compacted(revision).await
            }
        }

        const SERVICES: usize = 12;
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(CountingStorage {
            inner: Arc::clone(&inner),
            pod_lists: AtomicUsize::new(0),
        });

        // One backing pod, and SERVICES services all selecting it.
        let mut pod_labels = HashMap::new();
        pod_labels.insert("app".to_string(), "latency".to_string());
        let mut pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("backing-pod").with_namespace("svc-latency"),
            spec: None,
            status: None,
        };
        pod.metadata.labels = Some(pod_labels.clone());
        storage
            .create("/registry/pods/svc-latency/backing-pod", &pod)
            .await
            .unwrap();

        for i in 0..SERVICES {
            let name = format!("latency-svc-{i}");
            let service = Service {
                type_meta: rusternetes_common::types::TypeMeta {
                    kind: "Service".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: ObjectMeta::new(name.clone()).with_namespace("svc-latency"),
                spec: ServiceSpec {
                    selector: Some(pod_labels.clone()),
                    ports: vec![ServicePort {
                        name: None,
                        port: 80,
                        target_port: None,
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    }],
                    ..Default::default()
                },
                status: None,
            };
            storage
                .create(&build_key("services", Some("svc-latency"), &name), &service)
                .await
                .unwrap();
        }

        let controller = EndpointSliceController::new(Arc::clone(&storage));
        controller.reconcile_all().await.unwrap();

        let lists = storage.pod_lists.load(Ordering::SeqCst);
        assert!(
            lists < SERVICES,
            "reconcile_all issued {lists} pod LISTs for {SERVICES} services in ONE namespace; \
             the pod set is the same for all of them and must be fetched once per namespace"
        );
    }

    /// The live `run()` path must drain its service queue with a worker POOL.
    ///
    /// It used to spawn exactly one worker, so after a conformance run left
    /// 154 services in one namespace the 5s resync re-enqueued all of them
    /// and a single worker walked them serially (~1 service / 1.2s), starving
    /// the controller-manager (#1869). Upstream runs
    /// `ConcurrentServiceEndpointSyncs` = 5 workers over the service queue
    /// (pkg/controller/endpointslice/endpointslice_controller.go:106-118,
    /// endpointslice/config/v1alpha1/defaults.go:35).
    #[tokio::test]
    async fn run_drains_service_queue_with_concurrent_workers() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct SlowStorage {
            inner: Arc<MemoryStorage>,
            in_flight: AtomicUsize,
            max_in_flight: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl Storage for SlowStorage {
            async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.create(key, value).await
            }
            async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
            where
                T: serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.get(key).await
            }
            async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.update(key, value).await
            }
            async fn update_raw(
                &self,
                key: &str,
                value: &serde_json::Value,
            ) -> rusternetes_common::Result<()> {
                self.inner.update_raw(key, value).await
            }
            async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
                self.inner.delete(key).await
            }
            async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                if prefix.starts_with("/registry/endpointslices/") {
                    let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    self.max_in_flight.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    self.in_flight.fetch_sub(1, Ordering::SeqCst);
                }
                self.inner.list(prefix).await
            }
            async fn watch(
                &self,
                prefix: &str,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch(prefix).await
            }
            async fn watch_from_revision(
                &self,
                prefix: &str,
                revision: i64,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch_from_revision(prefix, revision).await
            }
            async fn current_revision(&self) -> rusternetes_common::Result<i64> {
                self.inner.current_revision().await
            }
            async fn is_revision_compacted(
                &self,
                revision: i64,
            ) -> rusternetes_common::Result<bool> {
                self.inner.is_revision_compacted(revision).await
            }
        }

        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(SlowStorage {
            inner: Arc::clone(&inner),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        });

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "latency".to_string());
        for i in 0..20 {
            let name = format!("svc-{i}");
            let service = Service {
                type_meta: rusternetes_common::types::TypeMeta {
                    kind: "Service".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: ObjectMeta::new(name.clone()).with_namespace("ns"),
                spec: ServiceSpec {
                    selector: Some(labels.clone()),
                    ports: vec![ServicePort {
                        name: None,
                        port: 80,
                        target_port: None,
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    }],
                    ..Default::default()
                },
                status: None,
            };
            storage
                .create(&build_key("services", Some("ns"), &name), &service)
                .await
                .unwrap();
        }

        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        handle.abort();

        let max = storage.max_in_flight.load(Ordering::SeqCst);
        assert!(
            max > 1,
            "only {max} service sync(s) ever ran at once; the service queue must be drained \
             by a pool of CONCURRENT_SERVICE_ENDPOINT_SYNCS workers"
        );
    }

    /// Storage that counts pod LISTs, for the shared pod snapshot tests.
    struct PodListCounter {
        inner: Arc<MemoryStorage>,
        pod_lists: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Storage for PodListCounter {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }
        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.get(key).await
        }
        async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.update(key, value).await
        }
        async fn update_raw(
            &self,
            key: &str,
            value: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.inner.update_raw(key, value).await
        }
        async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
            self.inner.delete(key).await
        }
        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if prefix.starts_with("/registry/pods/") {
                self.pod_lists
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.list(prefix).await
        }
        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }
        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }
        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }
        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    fn snapshot_service(name: &str) -> Service {
        Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace("ns"),
            spec: ServiceSpec {
                selector: Some(HashMap::from([("app".to_string(), "snap".to_string())])),
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                ..Default::default()
            },
            status: None,
        }
    }

    fn snapshot_pod(name: &str, ip: &str) -> Pod {
        Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name)
                .with_namespace("ns")
                .with_labels(HashMap::from([("app".to_string(), "snap".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                pod_ip: Some(ip.to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        }
    }

    /// The live path must read pods from a snapshot shared by every worker,
    /// not LIST per service sync. Upstream's `syncService` reads
    /// `c.podLister.Pods(ns).List(selector)` from the shared pod informer's
    /// cache (endpointslice_controller.go:410); the informer LISTs once.
    /// Stand-in for informer cache (#2208).
    #[tokio::test]
    async fn run_shares_one_pod_snapshot_across_service_syncs() {
        use std::sync::atomic::Ordering;
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(PodListCounter {
            inner: Arc::clone(&inner),
            pod_lists: std::sync::atomic::AtomicUsize::new(0),
        });
        for i in 0..20 {
            let name = format!("svc-{i}");
            storage
                .create(
                    &build_key("services", Some("ns"), &name),
                    &snapshot_service(&name),
                )
                .await
                .unwrap();
        }
        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        handle.abort();

        let lists = storage.pod_lists.load(Ordering::SeqCst);
        assert!(
            lists <= 1,
            "{lists} pod LISTs for 20 services; workers must share one pod snapshot \
             (upstream podLister reads the informer cache)"
        );
    }

    /// The snapshot is kept current from the pod watch, so a pod created
    /// after the initial sync reaches the slice without any 5s full resync
    /// and without a fresh LIST (informer event handler -> podQueue ->
    /// service enqueue, endpointslice_controller.go:132-135, 532).
    #[tokio::test]
    async fn pod_snapshot_follows_pod_watch_events() {
        use std::sync::atomic::Ordering;
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(PodListCounter {
            inner: Arc::clone(&inner),
            pod_lists: std::sync::atomic::AtomicUsize::new(0),
        });
        storage
            .create(
                &build_key("services", Some("ns"), "svc"),
                &snapshot_service("svc"),
            )
            .await
            .unwrap();
        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
        let before = storage.pod_lists.load(Ordering::SeqCst);

        storage
            .create(
                &build_key("pods", Some("ns"), "p1"),
                &snapshot_pod("p1", "10.0.0.7"),
            )
            .await
            .unwrap();
        let es_key = build_key("endpointslices", Some("ns"), "svc");
        let mut seen = false;
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Ok(es) = storage.get::<EndpointSlice>(&es_key).await {
                if es.endpoints.iter().any(|e| e.addresses == vec!["10.0.0.7"]) {
                    seen = true;
                    break;
                }
            }
        }
        handle.abort();
        assert!(seen, "pod created after sync never reached the slice");
        assert_eq!(
            storage.pod_lists.load(Ordering::SeqCst),
            before,
            "a pod event must update the snapshot, not trigger a pod LIST"
        );
    }

    /// Conformance "should create Endpoints and EndpointSlices for Pods
    /// matching a Service" deletes the slices of a named-port Service and
    /// expects them back (test/e2e/network/endpointslice.go:261-263).
    /// Upstream gets that from `onEndpointSliceDelete`
    /// (pkg/controller/endpointslice/endpointslice_controller.go:583-592),
    /// which queues the owning Service.
    #[tokio::test]
    async fn deleted_slice_is_recreated_without_waiting_for_resync() {
        let storage = Arc::new(MemoryStorage::new());
        let mut svc = snapshot_service("svc");
        svc.spec.selector = Some(HashMap::from([("app".to_string(), "x".to_string())]));
        storage
            .create(&build_key("services", Some("ns"), "svc"), &svc)
            .await
            .unwrap();
        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        let es_key = build_key("endpointslices", Some("ns"), "svc");
        let mut created = false;
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if storage.get::<EndpointSlice>(&es_key).await.is_ok() {
                created = true;
                break;
            }
        }
        assert!(created, "slice never created");
        storage.delete(&es_key).await.unwrap();
        let mut back = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if storage.get::<EndpointSlice>(&es_key).await.is_ok() {
                back = true;
                break;
            }
        }
        handle.abort();
        assert!(back, "deleted slice was not recreated");
    }

    fn ev_slice(svc: Option<&str>, managed: &str, uid: &str, generation: i64) -> EndpointSlice {
        let mut l = HashMap::from([(MANAGED_BY_LABEL.to_string(), managed.to_string())]);
        if let Some(s) = svc {
            l.insert(SERVICE_NAME_LABEL.to_string(), s.to_string());
        }
        let mut es = EndpointSlice::new("x-abc", "IPv4");
        es.metadata.namespace = Some("ns".to_string());
        es.metadata.uid = uid.to_string();
        es.metadata.generation = Some(generation);
        es.metadata.labels = Some(l);
        es
    }

    const EV_KEY: &str = "/registry/endpointslices/ns/x-abc";

    fn added(es: &EndpointSlice) -> WatchEvent {
        WatchEvent::Added(EV_KEY.to_string(), serde_json::to_string(es).unwrap())
    }
    fn modified(es: &EndpointSlice) -> WatchEvent {
        WatchEvent::Modified(EV_KEY.to_string(), serde_json::to_string(es).unwrap())
    }
    fn deleted(es: &EndpointSlice) -> WatchEvent {
        WatchEvent::Deleted(EV_KEY.to_string(), serde_json::to_string(es).unwrap())
    }

    /// `onEndpointSliceDelete` (endpointslice_controller.go:583-592): only a
    /// delete the tracker did not expect queues the Service; keyed by
    /// `ServiceControllerKey` (utils.go:201-210).
    #[test]
    fn slice_delete_queues_service_only_when_unexpected() {
        let me = CONTROLLER_NAME;
        let t = EndpointSliceTracker::new();
        let mut known = HashMap::new();
        let s = ev_slice(Some("svc"), me, "u1", 1);
        // Untracked slice (`Has` false): nothing to repair.
        assert!(services_to_queue_for_slice_event(&t, &mut known, &deleted(&s)).is_empty());
        // Tracked, deleted by someone else: queue.
        t.update(&s);
        assert_eq!(
            services_to_queue_for_slice_event(&t, &mut known, &deleted(&s)),
            vec!["services/ns/svc".to_string()]
        );
        // Our own delete (ExpectDeletion): no extra sync.
        t.update(&s);
        t.expect_deletion(&s);
        assert!(services_to_queue_for_slice_event(&t, &mut known, &deleted(&s)).is_empty());
        // ManagedByController false (reconciler.go:666-669).
        let other = ev_slice(Some("svc"), "someone-else", "u2", 1);
        t.update(&other);
        assert!(services_to_queue_for_slice_event(&t, &mut known, &deleted(&other)).is_empty());
        // No service-name label: ServiceControllerKey errors.
        let nolabel = ev_slice(None, me, "u3", 1);
        t.update(&nolabel);
        assert!(services_to_queue_for_slice_event(&t, &mut known, &deleted(&nolabel)).is_empty());
    }

    /// `onEndpointSliceAdd`/`Update` (:542-578): our own write (generation
    /// == tracked) does not queue; an external edit (newer generation) does.
    #[test]
    fn slice_update_queues_service_only_for_external_changes() {
        let me = CONTROLLER_NAME;
        let t = EndpointSliceTracker::new();
        let mut known = HashMap::new();
        let own = ev_slice(Some("svc"), me, "u1", 1);
        t.update(&own);
        assert!(services_to_queue_for_slice_event(&t, &mut known, &added(&own)).is_empty());
        assert!(services_to_queue_for_slice_event(&t, &mut known, &modified(&own)).is_empty());
        let edited = ev_slice(Some("svc"), me, "u1", 2);
        assert_eq!(
            services_to_queue_for_slice_event(&t, &mut known, &modified(&edited)),
            vec!["services/ns/svc".to_string()]
        );
        // An add the tracker has never seen (e.g. after a restart) syncs.
        let fresh = ev_slice(Some("svc"), me, "u9", 1);
        assert_eq!(
            services_to_queue_for_slice_event(&t, &mut known, &added(&fresh)),
            vec!["services/ns/svc".to_string()]
        );
    }

    /// Label edits do not bump the generation, so `onEndpointSliceUpdate`
    /// handles them explicitly (:564-577).
    #[test]
    fn slice_label_changes_queue_old_and_new_service() {
        let me = CONTROLLER_NAME;
        let t = EndpointSliceTracker::new();
        let mut known = HashMap::new();
        let a = ev_slice(Some("a"), me, "u1", 1);
        t.update(&a);
        services_to_queue_for_slice_event(&t, &mut known, &added(&a));
        let b = ev_slice(Some("b"), me, "u1", 1);
        assert_eq!(
            services_to_queue_for_slice_event(&t, &mut known, &modified(&b)),
            vec!["services/ns/b".to_string(), "services/ns/a".to_string()]
        );
        // managed-by flipped away from this controller: ManagedByChanged.
        let t2 = EndpointSliceTracker::new();
        let mut known2 = HashMap::new();
        let m = ev_slice(Some("a"), me, "u1", 1);
        t2.update(&m);
        services_to_queue_for_slice_event(&t2, &mut known2, &added(&m));
        let unmanaged = ev_slice(Some("a"), "someone-else", "u1", 1);
        assert_eq!(
            services_to_queue_for_slice_event(&t2, &mut known2, &modified(&unmanaged)),
            vec!["services/ns/a".to_string()]
        );
    }

    /// An external edit to a managed slice is repaired without waiting for
    /// the resync (the add/update half of the tracker port).
    #[tokio::test]
    async fn externally_edited_slice_is_repaired_without_waiting_for_resync() {
        let storage = Arc::new(MemoryStorage::new());
        let mut svc = snapshot_service("svc");
        svc.spec.selector = Some(HashMap::from([("app".to_string(), "x".to_string())]));
        storage
            .create(&build_key("services", Some("ns"), "svc"), &svc)
            .await
            .unwrap();
        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        let es_key = build_key("endpointslices", Some("ns"), "svc");
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if storage.get::<EndpointSlice>(&es_key).await.is_ok() {
                break;
            }
        }
        let mut es: EndpointSlice = storage.get(&es_key).await.unwrap();
        let ports_before = es.ports.clone();
        es.ports = vec![];
        es.metadata.generation = Some(es.metadata.generation.unwrap_or(0) + 1);
        storage.update(&es_key, &es).await.unwrap();
        let mut repaired = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let now: EndpointSlice = storage.get(&es_key).await.unwrap();
            if now.ports == ports_before {
                repaired = true;
                break;
            }
        }
        handle.abort();
        assert!(repaired, "external edit was not repaired");
    }

    /// Upstream has no periodic full resync of its own: only the informers
    /// resync, every MinResyncPeriod (12h default,
    /// staging/src/k8s.io/controller-manager/config/v1alpha1/defaults.go:32).
    #[test]
    fn informer_resync_period_is_not_the_old_5s_sweep() {
        assert!(INFORMER_RESYNC_PERIOD >= std::time::Duration::from_secs(12 * 3600));
    }

    /// `ResyncPeriod` (controllermanager.go:176-181): `Min * (rand + 1)`,
    /// i.e. in [12h, 24h), and not the same every call.
    #[test]
    fn resync_period_is_jittered_between_1x_and_2x() {
        let samples: Vec<_> = (0..50).map(|_| resync_period()).collect();
        for d in &samples {
            assert!(*d >= INFORMER_RESYNC_PERIOD && *d < INFORMER_RESYNC_PERIOD * 2);
        }
        assert!(samples.iter().any(|d| *d != samples[0]), "no jitter");
    }

    fn labelled(name: &str, labels: &[(&str, &str)]) -> Pod {
        let mut pod = snapshot_pod(name, "10.0.0.1");
        pod.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        pod.metadata.resource_version = Some("1".into());
        pod
    }

    fn svc_with_selector(name: &str, sel: Option<&[(&str, &str)]>) -> Service {
        let mut s = snapshot_service(name);
        s.spec.selector = sel.map(|l| {
            l.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        });
        s
    }

    fn set(keys: &[&str]) -> std::collections::HashSet<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    /// TestDetermineNeededServiceUpdates
    /// (staging/src/k8s.io/endpointslice/util/controller_utils_test.go:35).
    #[test]
    fn determine_needed_service_updates_cases() {
        let (a, b, c) = (set(&["a", "b"]), set(&["b", "c"]), set(&[]));
        assert_eq!(
            determine_needed_service_updates(&a, &b, true),
            set(&["a", "b", "c"])
        );
        assert_eq!(
            determine_needed_service_updates(&a, &b, false),
            set(&["a", "c"])
        );
        assert_eq!(determine_needed_service_updates(&a, &a, false), c);
    }

    /// Add / delete / resync / unrelated-update projection
    /// (controller_utils.go:58-103).
    #[test]
    fn projection_key_add_delete_and_ignored_updates() {
        let p = labelled("p", &[("app", "x")]);
        let add = get_pod_update_projection_key(None, Some(&p)).unwrap();
        assert_eq!(add.namespace, "ns");
        assert_eq!(add.old_labels, None);
        assert!(!add.pod_changed);
        assert_eq!(get_pod_update_projection_key(Some(&p), None), Some(add));
        assert_eq!(get_pod_update_projection_key(None, None), None);
        // same resourceVersion == informer resync: ignored
        assert_eq!(
            get_pod_update_projection_key(Some(&p), Some(&p.clone())),
            None
        );
        // new RV but nothing endpoint-relevant changed: ignored
        let mut q = p.clone();
        q.metadata.resource_version = Some("2".into());
        assert_eq!(get_pod_update_projection_key(Some(&p), Some(&q)), None);
    }

    /// `newPod.ResourceVersion == oldPod.ResourceVersion` (controller_utils.go:79)
    /// compares Go strings, so two UNSET resourceVersions are equal and the
    /// update is ignored as a resync (#2365).
    #[test]
    fn projection_key_two_unset_resource_versions_are_equal() {
        let mut p = labelled("p", &[("app", "x")]);
        p.metadata.resource_version = None;
        let mut relabelled = p.clone();
        relabelled.metadata.labels = Some([("app".to_string(), "y".to_string())].into());
        assert_eq!(
            get_pod_update_projection_key(Some(&p), Some(&relabelled)),
            None
        );
    }

    /// Test_podChanged (controller_utils_test.go:448): readiness / IP change
    /// is a pod change with unchanged labels; a label change carries both
    /// label sets.
    #[test]
    fn projection_key_pod_and_label_changes() {
        let p = labelled("p", &[("app", "x")]);
        let mut ready = p.clone();
        ready.metadata.resource_version = Some("2".into());
        ready.status.as_mut().unwrap().conditions = None;
        let k = get_pod_update_projection_key(Some(&p), Some(&ready)).unwrap();
        assert_eq!(k.old_labels, None);

        let mut relabelled = p.clone();
        relabelled.metadata.resource_version = Some("2".into());
        relabelled.metadata.labels = Some(HashMap::from([("app".into(), "y".into())]));
        let k = get_pod_update_projection_key(Some(&p), Some(&relabelled)).unwrap();
        assert_eq!(k.labels["app"], "y");
        assert_eq!(k.old_labels.as_ref().unwrap()["app"], "x");
        assert!(!k.pod_changed);

        relabelled.status.as_mut().unwrap().conditions = None;
        let k = get_pod_update_projection_key(Some(&p), Some(&relabelled)).unwrap();
        assert!(k.pod_changed);
    }

    /// TestGetPodServicesToUpdate (controller_utils_test.go:333): a delete
    /// enqueues only matching services; nil selectors match nothing; a pure
    /// relabel enqueues only the symmetric difference.
    #[test]
    fn services_to_update_projection() {
        let services = vec![
            svc_with_selector("x", Some(&[("app", "x")])),
            svc_with_selector("y", Some(&[("app", "y")])),
            svc_with_selector("none", None),
        ];
        let p = labelled("p", &[("app", "x")]);
        let key = get_pod_update_projection_key(Some(&p), None).unwrap();
        assert_eq!(
            get_services_to_update(&services, &key),
            set(&["services/ns/x"])
        );

        let mut relabelled = p.clone();
        relabelled.metadata.resource_version = Some("2".into());
        relabelled.metadata.labels = Some(HashMap::from([("app".into(), "y".into())]));
        let key = get_pod_update_projection_key(Some(&p), Some(&relabelled)).unwrap();
        assert_eq!(
            get_services_to_update(&services, &key),
            set(&["services/ns/x", "services/ns/y"])
        );
    }

    /// `WaitForNamedCacheSyncWithContext(ctx, c.podsSynced, ...)`
    /// (endpointslice_controller.go:294): blocks until the first pod LIST has
    /// landed, then stays open (`HasSynced` never regresses).
    #[tokio::test]
    async fn wait_for_pods_synced_blocks_until_first_pod_list() {
        let controller = EndpointSliceController::new(Arc::new(MemoryStorage::new()));
        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            controller.wait_for_pods_synced(),
        )
        .await;
        assert!(
            pending.is_err(),
            "wait returned before the pod cache synced"
        );
        assert!(controller.sync_pod_cache().await);
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            controller.wait_for_pods_synced(),
        )
        .await
        .expect("wait must return once synced");
        // a later relist start clears the snapshot but not the sync gate
        *controller.pod_cache.write().unwrap() = None;
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            controller.wait_for_pods_synced(),
        )
        .await
        .expect("gate must not regress");
    }

    /// podQueue (endpointslice_controller.go:247, onPodUpdate :532, syncPod
    /// :491): a pod event only lands a projection key on the pod queue; a
    /// separate sync resolves it to the matching services.
    #[tokio::test]
    async fn pod_event_goes_through_pod_queue_to_matching_services() {
        let storage = Arc::new(MemoryStorage::new());
        let mut hit = snapshot_service("hit");
        hit.spec.selector = Some([("app".to_string(), "x".to_string())].into());
        let mut miss = snapshot_service("miss");
        miss.spec.selector = Some([("app".to_string(), "other".to_string())].into());
        for svc in [&hit, &miss] {
            let name = svc.metadata.name.clone();
            storage
                .create(&build_key("services", Some("ns"), &name), svc)
                .await
                .unwrap();
        }
        let controller = EndpointSliceController::new(storage);
        let service_queue = WorkQueue::new();

        let pod = labelled("p", &[("app", "x")]);
        controller.on_pod_update(None, Some(&pod));
        // nothing is resolved until the pod worker syncs the key
        assert!(service_queue.is_empty().await);

        let item = controller.pod_queue_rx.lock().await.recv().await.unwrap();
        controller
            .sync_pod(&item.key, &service_queue)
            .await
            .unwrap();
        assert_eq!(service_queue.len().await, 1);
        assert_eq!(service_queue.get().await.unwrap(), "services/ns/hit");

        // an ignored update (resync) never reaches the queue
        controller.on_pod_update(Some(&pod), Some(&pod.clone()));
        assert!(controller.pod_queue_rx.lock().await.try_recv().is_err());
    }

    /// The API server's generic Store turns a non-empty `metadata.uid` on a PUT
    /// into a UID precondition (rest/update.go:188-203). A slice the controller
    /// rebuilt from scratch carries a new random UID, so every update of an
    /// existing slice was a 409 and the slice stayed at its first (empty)
    /// content: the sig-network regression after #2108.
    #[tokio::test]
    async fn updating_an_existing_slice_keeps_its_uid() {
        use rusternetes_controller_manager::controllers::uid_precondition_double::UidPreconditionStorage;

        let storage = Arc::new(UidPreconditionStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        storage
            .create(&build_key("services", Some("default"), "svc"), &service)
            .await
            .unwrap();

        // No pod yet: the slice is created empty.
        controller.reconcile_service(&service).await.unwrap();
        let slice_key = build_key("endpointslices", Some("default"), "svc");
        let first: EndpointSlice = storage.get(&slice_key).await.unwrap();
        assert!(first.endpoints.is_empty());

        // A ready pod appears: the existing slice must be updated.
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("p1")
                .with_namespace("default")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.1".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "p1"), &pod)
            .await
            .unwrap();

        controller.reconcile_service(&service).await.unwrap();
        let second: EndpointSlice = storage.get(&slice_key).await.unwrap();
        assert_eq!(second.endpoints.len(), 1);
        assert_eq!(second.metadata.uid, first.metadata.uid);
    }
}
