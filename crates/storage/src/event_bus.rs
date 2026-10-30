//! In-process broadcast of `WatchEvent`s — the fast path that replaces a
//! per-consumer backend watch round-trip in the all-in-one binary (#1039).
//! Only correct when one process is the sole writer (all-in-one); multi-process
//! deployments must keep using the native backend watch.
//!
//! Tradeoff: the bus has no resourceVersion-resume equivalent to
//! `watch_from_revision`. On lag (or any stream error) a consumer must relist
//! and re-subscribe, leaving a small gap window that level-driven controllers
//! close on their next resync tick. This matches the pre-existing
//! `MemoryStorage` broadcast semantics; external HTTP watch clients, which need
//! RV-resumable replay, are served from the native feed via
//! `StorageBackend::watch_backend` instead.

use crate::{WatchEvent, WatchStream};
use rusternetes_common::Error;
use tokio::sync::broadcast;

/// Default broadcast capacity, matching the historical `MemoryStorage` value.
pub const DEFAULT_CAPACITY: usize = 1000;

/// In-process fan-out of `WatchEvent`s. Cloneable; all clones share one channel.
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<WatchEvent>,
}

impl EventBus {
    /// Create a bus with the given ring capacity.
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Publish an event. Ignores the "no current subscribers" send error.
    pub fn publish(&self, event: WatchEvent) {
        let _ = self.tx.send(event);
    }

    /// Subscribe to events whose key starts with `prefix`. On broadcast lag the
    /// stream yields `Err` so the consumer relists (consumers already reconnect
    /// + resync on a watch error) rather than silently missing events.
    pub fn subscribe(&self, prefix: &str) -> WatchStream {
        let mut rx = self.tx.subscribe();
        let prefix = prefix.to_string();
        let stream = async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let key = match &event {
                            WatchEvent::Added(k, _)
                            | WatchEvent::Modified(k, _)
                            | WatchEvent::Deleted(k, _) => k,
                        };
                        if key.starts_with(&prefix) {
                            yield Ok(event);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        yield Err(Error::Storage(format!(
                            "watch event bus lagged by {n} events; relist required"
                        )));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        Box::pin(stream)
    }
}
