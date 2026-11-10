//! Root CA cert publisher.
//!
//! Ported from `pkg/controller/certificates/rootcacertpublisher/publisher.go`
//! and `metrics.go` (release-1.35) and launched like
//! `newRootCACertificatePublisherController`
//! (`cmd/kube-controller-manager/app/certificates.go:263-287`, one worker): it
//! keeps a `kube-root-ca.crt` ConfigMap holding the cluster CA in every
//! namespace.
//!
//! Ported 1:1: the rate-limited `root_ca_cert_publisher` queue of namespace
//! names; the event handlers (`configMapDeleted` / `configMapUpdated` enqueue
//! the namespace for the `kube-root-ca.crt` ConfigMap only, `namespaceAdded`
//! always, `namespaceUpdated` only while the namespace is Active);
//! `processNextWorkItem` (error -> `AddRateLimited`, success -> `Forget`);
//! `syncNamespace` (create when absent with the description annotation and
//! `ca.crt`; otherwise leave it alone when `data` already equals
//! `{"ca.crt": <rootCA>}` and the description annotation is non-empty, else
//! overwrite `data` and set the annotation); and the
//! `root_ca_cert_publisher_sync_total` / `_sync_duration_seconds` metrics
//! keyed by status code (`recordMetrics`).
//!
//! The two informers are `rusternetes_client::reflector::Reflector`s (list once,
//! resume the watch from the last resourceVersion, serve reads from an
//! in-memory store) over the [`Storage`] trait via
//! `storage_list_watch::StorageListWatch`; `syncNamespace` reads the
//! ConfigMap from the ConfigMap store and the Namespace from the Namespace
//! store, as upstream reads its listers, so a sync costs no storage reads.
//!
//! Deviations:
//! - The ConfigMap informer is filtered to `metadata.name=kube-root-ca.crt`
//!   (a field-selector equivalent): the publisher reads no other ConfigMap, so
//!   the store does not hold every ConfigMap in the cluster.
//! - Upstream's `Create` is rejected by the apiserver when the namespace is
//!   missing or terminating (`NotFound` / `NamespaceTerminatingCause`) and the
//!   publisher swallows that error. Storage writes are not admitted, so the
//!   same outcome is produced from the Namespace store: no create when the
//!   namespace is absent or terminating.
//! - A namespace stored without `status.phase` is treated as Active (the
//!   registry defaults it to Active upstream).
//! - A lagged event subscription re-enqueues every namespace (an informer
//!   resync) instead of losing events.

use crate::controllers::storage_list_watch::StorageListWatch;
use anyhow::Result;
use prometheus::{HistogramVec, IntCounterVec};
use rusternetes_client::reflector::{Reflector, StoreEvent};
use rusternetes_common::resources::Namespace;
use rusternetes_common::Error;
use rusternetes_storage::{build_key, build_prefix, Storage, WorkQueue};
use serde_json::{json, Value};
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, error, info, warn};

/// `RootCACertConfigMapName`.
pub const ROOT_CA_CERT_CONFIG_MAP_NAME: &str = "kube-root-ca.crt";
/// `DescriptionAnnotation`.
pub const DESCRIPTION_ANNOTATION: &str = "kubernetes.io/description";
/// `Description`.
pub const DESCRIPTION: &str = "Contains a CA bundle that can be used to verify the kube-apiserver when using internal endpoints such as the internal service IP or kubernetes.default.svc. No other usage is guaranteed across distributions of Kubernetes clusters.";
/// Queue name (`NewPublisher`, `Name: "root_ca_cert_publisher"`).
pub const QUEUE_NAME: &str = "root_ca_cert_publisher";

/// `RootCACertPublisher` metrics subsystem (metrics.go:29).
static SYNC_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        prometheus::Opts::new(
            "root_ca_cert_publisher_sync_total",
            "Number of namespace syncs happened in root ca cert publisher.",
        ),
        &["code"],
    )
    .expect("valid opts");
    let _ = prometheus::default_registry().register(Box::new(c.clone()));
    c
});

/// `ExponentialBuckets(0.001, 2, 15)` (metrics.go:49).
static SYNC_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    let h = HistogramVec::new(
        prometheus::HistogramOpts::new(
            "root_ca_cert_publisher_sync_duration_seconds",
            "Number of namespace syncs happened in root ca cert publisher.",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 15).expect("valid buckets")),
        &["code"],
    )
    .expect("valid opts");
    let _ = prometheus::default_registry().register(Box::new(h.clone()));
    h
});

/// `recordMetrics` (metrics.go:57-67): `200` on success, else the error's
/// status code ([`Error::status_code`]), `500` when it carries none.
pub fn record_metrics(start: Instant, result: &Result<(), Error>) {
    let code = match result {
        Ok(()) => "200".to_string(),
        Err(e) => e.status_code().to_string(),
    };
    SYNC_LATENCY
        .with_label_values(&[&code])
        .observe(start.elapsed().as_secs_f64());
    SYNC_COUNTER.with_label_values(&[&code]).inc();
}

/// What a store event asks of the queue.
#[derive(Debug, PartialEq, Eq)]
pub enum Enqueue {
    /// Add this namespace name to the queue.
    Namespace(String),
    /// Nothing to do.
    Ignore,
}

type CmReflector = Reflector<Value, fn(&Value) -> String>;
type NsReflector = Reflector<Namespace, fn(&Namespace) -> String>;

/// `cache.MetaNamespaceKeyFunc` for a ConfigMap.
fn config_map_key(cm: &Value) -> String {
    let ns = cm.pointer("/metadata/namespace").and_then(Value::as_str);
    let name = cm.pointer("/metadata/name").and_then(Value::as_str);
    match (ns, name) {
        (Some(ns), Some(name)) if !ns.is_empty() => format!("{ns}/{name}"),
        (_, Some(name)) => name.to_string(),
        _ => String::new(),
    }
}

fn is_root_ca_config_map(cm: &Value) -> bool {
    cm.pointer("/metadata/name").and_then(Value::as_str) == Some(ROOT_CA_CERT_CONFIG_MAP_NAME)
}

fn namespace_key(ns: &Namespace) -> String {
    ns.metadata.name.clone()
}

fn is_active(ns: &Namespace) -> bool {
    use rusternetes_common::types::Phase;
    !matches!(
        ns.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(p) if *p != Phase::Active
    )
}

/// `Publisher`.
pub struct RootCACertPublisher<S: Storage> {
    storage: Arc<S>,
    root_ca: String,
    queue: WorkQueue,
    /// `cmInformer` (filtered to `kube-root-ca.crt`).
    config_maps: Arc<CmReflector>,
    /// `nsInformer`.
    namespaces: Arc<NsReflector>,
}

impl<S: Storage + 'static> RootCACertPublisher<S> {
    /// `NewPublisher`.
    pub fn new(storage: Arc<S>, root_ca: impl Into<String>) -> Self {
        let cm_lw =
            StorageListWatch::<S, Value>::new(storage.clone(), build_prefix("configmaps", None))
                .with_filter(is_root_ca_config_map);
        let ns_lw = StorageListWatch::<S, Namespace>::new(
            storage.clone(),
            build_prefix("namespaces", None),
        );
        Self {
            storage,
            root_ca: root_ca.into(),
            queue: WorkQueue::named(QUEUE_NAME),
            config_maps: Arc::new(Reflector::new(
                Arc::new(cm_lw),
                config_map_key as fn(&Value) -> String,
            )),
            namespaces: Arc::new(Reflector::new(
                Arc::new(ns_lw),
                namespace_key as fn(&Namespace) -> String,
            )),
        }
    }

    /// The controller's work queue (shared with tests).
    pub fn queue(&self) -> &WorkQueue {
        &self.queue
    }

    /// `configMapDeleted` / `configMapUpdated`: only the `kube-root-ca.crt`
    /// ConfigMap enqueues, keyed by its namespace. The ConfigMap informer has
    /// no `AddFunc`.
    pub fn on_config_map_event(event: &StoreEvent<Value>) -> Enqueue {
        let cm = match event {
            StoreEvent::Modified(cm) | StoreEvent::Deleted(cm) => cm,
            StoreEvent::Added(_) => return Enqueue::Ignore,
        };
        if !is_root_ca_config_map(cm) {
            return Enqueue::Ignore;
        }
        match cm.pointer("/metadata/namespace").and_then(Value::as_str) {
            Some(ns) if !ns.is_empty() => Enqueue::Namespace(ns.to_string()),
            _ => Enqueue::Ignore,
        }
    }

    /// `namespaceAdded` (always) / `namespaceUpdated` (only while Active).
    pub fn on_namespace_event(event: &StoreEvent<Namespace>) -> Enqueue {
        match event {
            StoreEvent::Added(ns) => Enqueue::Namespace(ns.metadata.name.clone()),
            StoreEvent::Modified(ns) if is_active(ns) => {
                Enqueue::Namespace(ns.metadata.name.clone())
            }
            StoreEvent::Modified(_) | StoreEvent::Deleted(_) => Enqueue::Ignore,
        }
    }

    async fn enqueue(&self, e: Enqueue) {
        if let Enqueue::Namespace(ns) = e {
            self.queue.add(ns).await;
        }
    }

    async fn enqueue_all_namespaces(&self) {
        for ns in self.namespaces.store().items() {
            self.queue.add(ns.metadata.name).await;
        }
    }

    /// Start both informers and their event handlers. Subscriptions are taken
    /// before the reflectors run so no event is missed.
    pub fn start_informers(self: &Arc<Self>) {
        let mut cm_rx = self.config_maps.subscribe();
        let mut ns_rx = self.namespaces.subscribe();

        let r = self.config_maps.clone();
        tokio::spawn(async move { r.run().await });
        let r = self.namespaces.clone();
        tokio::spawn(async move { r.run().await });

        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match cm_rx.recv().await {
                    Ok(ev) => this.enqueue(Self::on_config_map_event(&ev)).await,
                    Err(RecvError::Lagged(n)) => {
                        warn!("root CA publisher ConfigMap events lagged {n}; resyncing");
                        this.enqueue_all_namespaces().await;
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match ns_rx.recv().await {
                    Ok(ev) => this.enqueue(Self::on_namespace_event(&ev)).await,
                    Err(RecvError::Lagged(n)) => {
                        warn!("root CA publisher namespace events lagged {n}; resyncing");
                        this.enqueue_all_namespaces().await;
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }

    /// `Run` with one worker (certificates.go:285).
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting root CA cert publisher controller");
        self.start_informers();

        // `WaitForNamedCacheSync(cmListerSynced)`; the Namespace store is read
        // by `sync_namespace` too, so wait for it as well.
        self.config_maps.wait_for_cache_sync().await;
        self.namespaces.wait_for_cache_sync().await;
        // The informer's initial list is delivered as Add events.
        self.enqueue_all_namespaces().await;

        while self.process_next_work_item().await {}
        Ok(())
    }

    /// `processNextWorkItem`: `false` when the queue is shut down.
    pub async fn process_next_work_item(&self) -> bool {
        let Some(key) = self.queue.get().await else {
            return false;
        };
        match self.sync_namespace(&key).await {
            Ok(()) => self.queue.forget(&key).await,
            Err(e) => {
                error!("syncing {key:?} failed: {e}");
                self.queue.requeue_rate_limited(key.clone()).await;
            }
        }
        self.queue.done(&key).await;
        true
    }

    /// `syncNamespace` (publisher.go:187-236).
    pub async fn sync_namespace(&self, ns: &str) -> Result<(), Error> {
        let start = Instant::now();
        let result = self.sync_namespace_inner(ns).await;
        record_metrics(start, &result);
        debug!("Finished syncing namespace {ns} in {:?}", start.elapsed());
        result
    }

    async fn sync_namespace_inner(&self, ns: &str) -> Result<(), Error> {
        let cm_key = build_key("configmaps", Some(ns), ROOT_CA_CERT_CONFIG_MAP_NAME);
        // `c.cmLister.ConfigMaps(ns).Get(RootCACertConfigMapName)`.
        let existing = self
            .config_maps
            .store()
            .get(&format!("{ns}/{ROOT_CA_CERT_CONFIG_MAP_NAME}"));
        let Some(existing) = existing else {
            // Upstream: "don't retry a create if the namespace doesn't exist or
            // is terminating" (NotFound / NamespaceTerminatingCause).
            match self.namespaces.store().get(ns) {
                Some(n) if n.metadata.deletion_timestamp.is_none() && is_active(&n) => {}
                _ => return Ok(()),
            }
            let cm = json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": ROOT_CA_CERT_CONFIG_MAP_NAME,
                    "namespace": ns,
                    "annotations": { DESCRIPTION_ANNOTATION: DESCRIPTION },
                },
                "data": { "ca.crt": self.root_ca },
            });
            return match self.storage.create(&cm_key, &cm).await {
                Ok(_) => {
                    info!("Created kube-root-ca.crt ConfigMap in namespace {ns}");
                    Ok(())
                }
                // Lost a race with the namespace going away.
                Err(Error::NotFound(_)) => Ok(()),
                Err(e) => Err(e),
            };
        };

        let data = json!({ "ca.crt": self.root_ca });
        let annotated = existing
            .pointer("/metadata/annotations")
            .and_then(|a| a.get(DESCRIPTION_ANNOTATION))
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty());
        // Ensure the data and the one annotation describing usage match.
        if existing.get("data") == Some(&data) && annotated {
            return Ok(());
        }

        // Copy so the store's instance is not modified.
        let mut cm = existing;
        cm["data"] = data;
        if !cm["metadata"]["annotations"].is_object() {
            cm["metadata"]["annotations"] = json!({});
        }
        cm["metadata"]["annotations"][DESCRIPTION_ANNOTATION] = json!(DESCRIPTION);
        self.storage.update(&cm_key, &cm).await?;
        Ok(())
    }
}
