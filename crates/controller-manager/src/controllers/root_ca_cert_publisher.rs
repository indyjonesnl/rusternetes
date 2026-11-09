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
//! Deviations (storage seam instead of informers):
//! - Upstream reads the ConfigMap from an informer lister; the `Storage`
//!   trait has no lister, so [`RootCACertPublisher::sync_namespace`] reads it
//!   from storage.
//! - Upstream's `Create` is rejected by the apiserver when the namespace is
//!   missing or terminating (`NotFound` / `NamespaceTerminatingCause`) and the
//!   publisher swallows that error. Storage writes are not admitted, so the
//!   same outcome is produced by reading the namespace first and skipping the
//!   create when it is absent or terminating.
//! - Upstream's informers deliver an initial list as Add events and a 10h
//!   resync; here each (re)established watch first enqueues every namespace
//!   (the informer's initial list) and `Modified`/`Deleted` events drive the
//!   rest.
//! - A namespace stored without `status.phase` is treated as Active (the
//!   registry defaults it to Active upstream).

use anyhow::Result;
use futures::StreamExt;
use prometheus::{HistogramVec, IntCounterVec};
use rusternetes_common::resources::Namespace;
use rusternetes_common::Error;
use rusternetes_storage::{build_key, build_prefix, Storage, WatchEvent, WorkQueue};
use serde_json::{json, Value};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
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

/// What a watch event asks of the queue.
#[derive(Debug, PartialEq, Eq)]
pub enum Enqueue {
    /// Add this namespace name to the queue.
    Namespace(String),
    /// Nothing to do.
    Ignore,
}

/// `Publisher`.
pub struct RootCACertPublisher<S: Storage> {
    storage: Arc<S>,
    root_ca: String,
    queue: WorkQueue,
}

impl<S: Storage + 'static> RootCACertPublisher<S> {
    /// `NewPublisher`.
    pub fn new(storage: Arc<S>, root_ca: impl Into<String>) -> Self {
        Self {
            storage,
            root_ca: root_ca.into(),
            queue: WorkQueue::named(QUEUE_NAME),
        }
    }

    /// The controller's work queue (shared with tests).
    pub fn queue(&self) -> &WorkQueue {
        &self.queue
    }

    /// `configMapDeleted` / `configMapUpdated`: only the `kube-root-ca.crt`
    /// ConfigMap enqueues, keyed by its namespace. `Added` events are not
    /// handled upstream (`ResourceEventHandlerFuncs` has no `AddFunc` for the
    /// ConfigMap informer).
    pub fn on_config_map_event(event: &WatchEvent) -> Enqueue {
        let key = match event {
            WatchEvent::Modified(key, _) | WatchEvent::Deleted(key, _) => key,
            WatchEvent::Added(..) => return Enqueue::Ignore,
        };
        // /registry/configmaps/<ns>/<name>
        let mut parts = key
            .strip_prefix("/registry/configmaps/")
            .unwrap_or(key)
            .splitn(2, '/');
        match (parts.next(), parts.next()) {
            (Some(ns), Some(name)) if name == ROOT_CA_CERT_CONFIG_MAP_NAME && !ns.is_empty() => {
                Enqueue::Namespace(ns.to_string())
            }
            _ => Enqueue::Ignore,
        }
    }

    /// `namespaceAdded` (always) / `namespaceUpdated` (only while Active).
    pub fn on_namespace_event(event: &WatchEvent) -> Enqueue {
        let (key, value, is_add) = match event {
            WatchEvent::Added(key, value) => (key, value, true),
            WatchEvent::Modified(key, value) => (key, value, false),
            WatchEvent::Deleted(..) => return Enqueue::Ignore,
        };
        let name = key.strip_prefix("/registry/namespaces/").unwrap_or(key);
        if name.is_empty() {
            return Enqueue::Ignore;
        }
        if !is_add && !namespace_value_is_active(value) {
            return Enqueue::Ignore;
        }
        Enqueue::Namespace(name.to_string())
    }

    async fn enqueue(&self, e: Enqueue) {
        if let Enqueue::Namespace(ns) = e {
            self.queue.add(ns).await;
        }
    }

    /// `Run` with one worker (certificates.go:285): a worker draining the
    /// queue plus the two watch loops standing in for the informers.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting root CA cert publisher controller");

        let worker = Arc::clone(&self);
        tokio::spawn(async move { while worker.process_next_work_item().await {} });

        let cms = Arc::clone(&self);
        tokio::spawn(async move { cms.watch_config_maps().await });

        self.watch_namespaces().await;
        Ok(())
    }

    async fn watch_config_maps(&self) {
        loop {
            match self.storage.watch(&build_prefix("configmaps", None)).await {
                Ok(mut watch) => {
                    while let Some(ev) = watch.next().await {
                        match ev {
                            Ok(ev) => self.enqueue(Self::on_config_map_event(&ev)).await,
                            Err(e) => {
                                warn!("root CA publisher ConfigMap watch error: {e}, reconnecting");
                                break;
                            }
                        }
                    }
                }
                Err(e) => error!("root CA publisher failed to watch ConfigMaps: {e}, retrying"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn watch_namespaces(&self) {
        loop {
            // The informer's initial list: every existing namespace is "added".
            match self
                .storage
                .list::<Namespace>(&build_prefix("namespaces", None))
                .await
            {
                Ok(all) => {
                    for ns in all {
                        self.queue.add(ns.metadata.name).await;
                    }
                }
                Err(e) => error!("root CA publisher failed to list namespaces: {e}"),
            }
            match self.storage.watch(&build_prefix("namespaces", None)).await {
                Ok(mut watch) => {
                    while let Some(ev) = watch.next().await {
                        match ev {
                            Ok(ev) => self.enqueue(Self::on_namespace_event(&ev)).await,
                            Err(e) => {
                                warn!("root CA publisher namespace watch error: {e}, reconnecting");
                                break;
                            }
                        }
                    }
                }
                Err(e) => error!("root CA publisher failed to watch namespaces: {e}, retrying"),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
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
        let existing = match self.storage.get::<Value>(&cm_key).await {
            Ok(cm) => cm,
            Err(Error::NotFound(_)) => {
                // Upstream: "don't retry a create if the namespace doesn't
                // exist or is terminating" (NotFound / NamespaceTerminatingCause).
                match self
                    .storage
                    .get::<Namespace>(&build_key("namespaces", None, ns))
                    .await
                {
                    Ok(n) if n.metadata.deletion_timestamp.is_none() && is_active(&n) => {}
                    Ok(_) | Err(Error::NotFound(_)) => return Ok(()),
                    Err(e) => return Err(e),
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
            }
            Err(e) => return Err(e),
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

fn is_active(ns: &Namespace) -> bool {
    use rusternetes_common::types::Phase;
    !matches!(
        ns.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(p) if *p != Phase::Active
    )
}

fn namespace_value_is_active(value: &str) -> bool {
    serde_json::from_str::<Namespace>(value)
        .map(|n| is_active(&n))
        .unwrap_or(false)
}
