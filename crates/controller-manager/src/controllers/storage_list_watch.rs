//! `ListWatch` over the [`Storage`] trait, for informer-style controllers.
//!
//! The controller-manager's controllers are generic over [`Storage`] (so the
//! same code runs against the API-backed storage, the all-in-one embedded
//! backend and `MemoryStorage` in tests), so the client crate's
//! `ApiListWatch` (HTTP only) does not fit. This is the same adapter the
//! scheduler's data plane uses (`crates/scheduler/src/data_plane.rs`
//! `StorageListWatch`), plus an optional server-side-style filter.
//!
//! Ordering follows `resource_events::run_storage_feed`: watch first, then
//! list, so events between the two are replayed from the stream (applying an
//! event twice is idempotent in the store). The watch opened by `list` is
//! handed to the next `watch` call; a watch that ends fails the next `watch`
//! so the reflector re-lists (client-go's `ListAndWatch` restart).
//!
//! The filter stands in for a field selector (`metadata.name=<name>`): it
//! keeps a cluster-wide informer's store to the few objects a controller
//! actually reads instead of every object of the kind.

use futures::stream::BoxStream;
use futures::StreamExt;
use k3rs_client::reflector::{ListWatch, WatchItem};
use k3rs_client::watch::WatchEvent as ClientWatchEvent;
use k3rs_storage::{Storage, WatchEvent, WatchStream};
use std::sync::{Arc, Mutex};

/// Keep-predicate applied to listed items and watched objects.
pub type KeepFn<T> = fn(&T) -> bool;

pub struct StorageListWatch<S: Storage + Send + Sync + 'static, T> {
    storage: Arc<S>,
    prefix: String,
    keep: Option<KeepFn<T>>,
    pending: Mutex<Option<WatchStream>>,
}

impl<S: Storage + Send + Sync + 'static, T> StorageListWatch<S, T> {
    pub fn new(storage: Arc<S>, prefix: impl Into<String>) -> Self {
        Self {
            storage,
            prefix: prefix.into(),
            keep: None,
            pending: Mutex::new(None),
        }
    }

    /// Only objects for which `keep` is true reach the reflector's store.
    pub fn with_filter(mut self, keep: KeepFn<T>) -> Self {
        self.keep = Some(keep);
        self
    }
}

#[async_trait::async_trait]
impl<S, T> ListWatch<T> for StorageListWatch<S, T>
where
    S: Storage + Send + Sync + 'static,
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    async fn list(&self) -> anyhow::Result<(Vec<T>, String)> {
        let watch = self
            .storage
            .watch(&self.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("watch {} failed: {e}", self.prefix))?;
        let mut items = self
            .storage
            .list::<T>(&self.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("list {} failed: {e}", self.prefix))?;
        if let Some(keep) = self.keep {
            items.retain(keep);
        }
        *self.pending.lock().unwrap_or_else(|e| e.into_inner()) = Some(watch);
        Ok((items, "0".to_string()))
    }

    async fn watch<'a>(
        &'a self,
        _rv: Option<String>,
    ) -> anyhow::Result<BoxStream<'a, WatchItem<T>>> {
        let stream = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| anyhow::anyhow!("watch {} closed; re-listing", self.prefix))?;
        let keep = self.keep;
        Ok(stream
            .filter_map(move |item| async move {
                let ev = match item {
                    Ok(ev) => ev,
                    Err(e) => return Some(Err(anyhow::anyhow!("{e}"))),
                };
                let decoded = (|| -> anyhow::Result<(ClientWatchEvent<T>, Option<String>)> {
                    let (raw, wrap): (String, fn(T) -> ClientWatchEvent<T>) = match ev {
                        WatchEvent::Added(_, v) => (v, ClientWatchEvent::Added),
                        WatchEvent::Modified(_, v) => (v, ClientWatchEvent::Modified),
                        // The previous value, which is what the store removes.
                        WatchEvent::Deleted(_, v) => (v, ClientWatchEvent::Deleted),
                    };
                    let value: serde_json::Value = serde_json::from_str(&raw)?;
                    let rv = value
                        .pointer("/metadata/resourceVersion")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    Ok((wrap(serde_json::from_value(value)?), rv))
                })();
                match decoded {
                    Ok((ev, rv)) => {
                        let obj = match &ev {
                            ClientWatchEvent::Added(o)
                            | ClientWatchEvent::Modified(o)
                            | ClientWatchEvent::Deleted(o)
                            | ClientWatchEvent::Bookmark(o) => o,
                        };
                        match keep {
                            Some(k) if !k(obj) => None,
                            _ => Some(Ok((ev, rv))),
                        }
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .boxed())
    }
}
