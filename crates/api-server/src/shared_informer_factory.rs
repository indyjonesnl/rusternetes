//! The generic server's shared informer factory — port of
//! `staging/src/k8s.io/client-go/informers/factory.go` (`sharedInformerFactory`:
//! `InformerFor`, `Start`, `WaitForCacheSync`) and of the `informer-sync`
//! readyz check of `staging/src/k8s.io/apiserver/pkg/server/healthz/healthz.go`
//! (`informerSync.Check`).
//!
//! `GenericAPIServer.New` (`server/config.go:899-917`) registers the
//! `generic-apiserver-start-informers` post-start hook, which calls
//! `SharedInformerFactory.Start`, and the `informer-sync` readyz check, which
//! fails while any *started* informer has not synced.
//!
//! The informer itself is the workspace's one reflector,
//! [`k3rs_client::reflector::Reflector`] (list + watch from the list's
//! resourceVersion, keyed store, `HasSynced`, re-list on any watch error),
//! also used by the scheduler; [`StorageListWatch`] is its `ListWatch` over
//! the storage backend, where the api-server's `ApiListWatch` is over HTTP.
//! Informers hold unstructured objects (`serde_json::Value`), like client-go's
//! `dynamicinformer`, keyed `namespace/name` or `name` (`MetaNamespaceKeyFunc`).
//!
//! Deviations (Rust-shaped): keyed by resource name rather than `reflect.Type`;
//! informers run for the life of the process (upstream stops them on `stopCh`),
//! so `Shutdown`/`WaitForStop` are not ported.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context, Result};
use futures::stream::BoxStream;
use futures::StreamExt;
use k3rs_client::reflector::{ListWatch, Reflector, WatchItem};
use k3rs_client::watch::WatchEvent as ClientWatchEvent;
use k3rs_storage::{build_prefix, Storage, StorageBackend, WatchEvent};
use serde_json::Value;

/// `cache.MetaNamespaceKeyFunc`.
pub fn meta_namespace_key(obj: &Value) -> String {
    let name = obj["metadata"]["name"].as_str().unwrap_or("");
    match obj["metadata"]["namespace"].as_str() {
        Some(ns) if !ns.is_empty() => format!("{ns}/{name}"),
        _ => name.to_string(),
    }
}

/// The shared informer: the reflector over a [`StorageListWatch`].
pub type SharedInformer = Reflector<Value, fn(&Value) -> String>;

/// `ListWatch` over a storage prefix. The list is taken at the revision read
/// just before it, so a watch resumed from there can only replay events the
/// list already contains (applied idempotently), never miss one. Storage
/// resource versions are backend revisions.
pub struct StorageListWatch {
    storage: Arc<StorageBackend>,
    prefix: String,
}

impl StorageListWatch {
    pub fn new(storage: Arc<StorageBackend>, resource: &str) -> Self {
        Self {
            storage,
            prefix: build_prefix(resource, None),
        }
    }
}

fn decode(raw: &str) -> Result<(Value, Option<String>)> {
    let obj: Value = serde_json::from_str(raw).context("decoding watch object")?;
    let rv = obj["metadata"]["resourceVersion"]
        .as_str()
        .map(str::to_string);
    Ok((obj, rv))
}

#[async_trait::async_trait]
impl ListWatch<Value> for StorageListWatch {
    async fn list(&self) -> Result<(Vec<Value>, String)> {
        let rv = self.storage.current_revision().await?;
        let items = self.storage.list::<Value>(&self.prefix).await?;
        Ok((items, rv.to_string()))
    }

    async fn watch<'a>(&'a self, rv: Option<String>) -> Result<BoxStream<'a, WatchItem<Value>>> {
        let stream = match rv.as_deref().and_then(|r| r.parse::<i64>().ok()) {
            Some(rev) => self.storage.watch_from_revision(&self.prefix, rev).await?,
            None => self.storage.watch(&self.prefix).await?,
        };
        Ok(stream
            .map(|ev| -> WatchItem<Value> {
                match ev? {
                    WatchEvent::Added(_, v) => {
                        let (o, rv) = decode(&v)?;
                        Ok((ClientWatchEvent::Added(o), rv))
                    }
                    WatchEvent::Modified(_, v) => {
                        let (o, rv) = decode(&v)?;
                        Ok((ClientWatchEvent::Modified(o), rv))
                    }
                    WatchEvent::Deleted(_, v) => {
                        let (o, rv) = decode(&v)?;
                        Ok((ClientWatchEvent::Deleted(o), rv))
                    }
                }
            })
            .boxed())
    }
}

struct Entry {
    informer: Arc<SharedInformer>,
    started: bool,
}

/// `sharedInformerFactory`.
#[derive(Default)]
pub struct SharedInformerFactory {
    informers: Mutex<BTreeMap<String, Entry>>,
}

impl SharedInformerFactory {
    pub fn new() -> Self {
        Self::default()
    }

    /// `InformerFor`: the informer for `resource`, created on first use and
    /// shared afterwards. It does not run until [`Self::start`]; the storage
    /// is bound at `start`, so creation needs none.
    pub fn informer_for(
        &self,
        resource: &str,
        storage: &Arc<StorageBackend>,
    ) -> Arc<SharedInformer> {
        let mut map = self.informers.lock().unwrap();
        if let Some(e) = map.get(resource) {
            return e.informer.clone();
        }
        let lw = Arc::new(StorageListWatch::new(storage.clone(), resource));
        let informer = Arc::new(Reflector::new(
            lw,
            meta_namespace_key as fn(&Value) -> String,
        ));
        map.insert(
            resource.to_string(),
            Entry {
                informer: informer.clone(),
                started: false,
            },
        );
        informer
    }

    /// The informer for `resource` if one was requested, without creating it.
    pub fn existing(&self, resource: &str) -> Option<Arc<SharedInformer>> {
        self.informers
            .lock()
            .unwrap()
            .get(resource)
            .map(|e| e.informer.clone())
    }

    /// `Start`: run every informer that has not been started yet.
    pub fn start(&self) {
        let mut map = self.informers.lock().unwrap();
        for entry in map.values_mut() {
            if !entry.started {
                let informer = entry.informer.clone();
                tokio::spawn(async move { informer.run().await });
                entry.started = true;
            }
        }
    }

    /// `WaitForCacheSync` with an already-closed stop channel: for each
    /// *started* informer, whether it has synced right now.
    pub fn sync_state(&self) -> BTreeMap<String, bool> {
        self.informers
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, e)| e.started)
            .map(|(name, e)| (name.clone(), e.informer.has_synced()))
            .collect()
    }

    /// `informerSync.Check` (healthz.go:138-152).
    pub fn informer_sync_check(&self) -> Result<(), String> {
        let not_synced: Vec<String> = self
            .sync_state()
            .into_iter()
            .filter(|(_, synced)| !synced)
            .map(|(name, _)| name)
            .collect();
        if not_synced.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "{} informers not started yet: [{}]",
                not_synced.len(),
                not_synced.join(" ")
            ))
        }
    }
}

/// The process-wide factory (`c.SharedInformerFactory`).
pub fn global() -> &'static Arc<SharedInformerFactory> {
    static FACTORY: OnceLock<Arc<SharedInformerFactory>> = OnceLock::new();
    FACTORY.get_or_init(|| Arc::new(SharedInformerFactory::new()))
}
