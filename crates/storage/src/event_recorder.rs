//! A unified event recorder shared by every component that emits Kubernetes
//! events (controller-manager, scheduler, kubelet).
//!
//! Before this existed, each component wrote `Event` objects straight to storage
//! with its own ad-hoc de-duplication. That meant the spam-filter and
//! aggregation stages of [`EventCorrelator`] (ported in
//! `crates/common/src/event_correlator.rs`) were never exercised at emit time —
//! only the count/de-dup branch was, and only inside the controller-manager's
//! `EventsController`.
//!
//! [`EventRecorder`] routes every emission through the correlator before it
//! touches storage, mirroring upstream's `recordToSink`:
//!
//! 1. **Correlate** — run the event through spam-filter → aggregate → count.
//!    If the spam-filter trips, the event is dropped (`skip`).
//! 2. **Persist** — for a fresh `(involvedObject, reason)` create a new `Event`;
//!    for a recurrence bump `count` / `lastTimestamp` / `series` on the stored
//!    object.
//!
//! Storage remains the authority on `count` (it is read back and bumped), so a
//! process restart — which empties the correlator's in-memory logger cache —
//! never regresses a stored count. The correlator contributes the two behaviours
//! storage alone cannot: dropping flooding events (`skip`) and rewriting an
//! over-threshold message to the aggregate "(combined from similar events)"
//! form.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rusternetes_common::event_correlator::{
    CorrelatorEvent, CorrelatorEventSource, CorrelatorEventType, CorrelatorObjectReference,
    EventCorrelator, RealClock,
};
use rusternetes_common::resources::{Event, EventSeries, EventSource, EventType, ObjectReference};
use rusternetes_common::Result;

use crate::Storage;

/// `--event-ttl`'s default, 1h (`pkg/controlplane/apiserver/options/options.go:129`,
/// `EventTTL: 1 * time.Hour`), in seconds. Single source for the API server's
/// event store and for in-process recorders.
pub const DEFAULT_EVENT_TTL_SECONDS: u64 = 3600;

/// The `--event-ttl` in force for this process, in seconds.
///
/// Upstream threads one server-side value into the event store once
/// (`pkg/controlplane/apiserver/config.go:299` `EventTTL:                opts.EventTTL,`
/// -> `pkg/controlplane/apiserver/apis.go:51` `EventTTL:                    c.Extra.EventTTL,`
/// -> `pkg/controlplane/instance.go:434` `eventsrest.RESTStorageProvider{TTL: c.ControlPlane.EventTTL},`
/// -> `pkg/registry/core/event/storage/storage.go:42-44`
/// `TTLFunc: func(runtime.Object, uint64, bool) (uint64, error) { return ttl, nil }`),
/// so every event writer is governed by it. In-process recorders (controller-manager,
/// kubelet, scheduler, api-server bootstrap/repair) write storage directly and are
/// constructed in ~20 places across crates; the all-in-one binary is one process with
/// one `--event-ttl`, so it is held here once and [`EventRecorder::new`] reads it.
/// Standalone components keep the upstream default (they have no server-side source).
/// Deliberate deviation from upstream's plumbing; the semantics (one TTL for all
/// event writes) are the same.
static PROCESS_EVENT_TTL: AtomicU64 = AtomicU64::new(DEFAULT_EVENT_TTL_SECONDS);

/// Set the process-wide event TTL in seconds (`0` = no expiry). Call before
/// constructing recorders; the all-in-one binary calls it with `--event-ttl`.
pub fn set_process_event_ttl(seconds: u64) {
    PROCESS_EVENT_TTL.store(seconds, Ordering::Relaxed);
}

/// The process-wide event TTL in seconds.
pub fn process_event_ttl() -> u64 {
    PROCESS_EVENT_TTL.load(Ordering::Relaxed)
}

/// Records Kubernetes events on behalf of a component, routing each emission
/// through a shared [`EventCorrelator`] before writing to `storage`.
///
/// Cheap to `clone` for sharing across tasks — the correlator state lives behind
/// an `Arc<Mutex<..>>` so all clones share one spam-filter / aggregation
/// timeline (matching upstream where one broadcaster fronts one correlator).
pub struct EventRecorder<S: Storage + ?Sized> {
    storage: Arc<S>,
    /// The correlator is locked only for the synchronous `event_correlate`
    /// call; the guard is always dropped before any `await`, so no storage I/O
    /// ever happens while holding it.
    correlator: Arc<Mutex<EventCorrelator<RealClock>>>,
    /// Seconds an event lives after each write; `0` = no expiry.
    event_ttl: u64,
}

impl<S: Storage + ?Sized> Clone for EventRecorder<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            correlator: Arc::clone(&self.correlator),
            event_ttl: self.event_ttl,
        }
    }
}

impl<S: Storage + ?Sized> EventRecorder<S> {
    /// Build a recorder backed by `storage` with the upstream-default
    /// correlator (burst 25, qps 1/300, aggregate after 10 in 600s).
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            correlator: Arc::new(Mutex::new(EventCorrelator::new(RealClock))),
            event_ttl: process_event_ttl(),
        }
    }

    /// Upstream recorders hand events to the API server (client-go
    /// `tools/record/event.go` `recordToSink` -> `sink.Create` / `sink.Patch`),
    /// whose event store applies `--event-ttl` to every write
    /// (`pkg/registry/core/event/storage/storage.go:42-44`). In-process
    /// recorders write storage directly, so they apply the same TTL here.
    ///
    /// Override the event TTL in seconds (`--event-ttl`); `0` disables expiry.
    pub fn with_event_ttl(mut self, seconds: u64) -> Self {
        self.event_ttl = seconds;
        self
    }

    /// Emit an event about `involved` from `source`.
    ///
    /// Returns `Ok(())` both when the event is persisted and when the
    /// spam-filter drops it — a dropped event is normal back-pressure, not an
    /// error. Storage failures propagate.
    pub async fn event(
        &self,
        involved: &ObjectReference,
        source: &EventSource,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) -> Result<()> {
        let correlator_event =
            build_correlator_event(involved, source, &event_type, reason, message);

        // Lock only for the synchronous correlate call; drop before any await.
        let result = {
            let mut correlator = self
                .correlator
                .lock()
                .expect("event correlator mutex poisoned");
            correlator.event_correlate(&correlator_event)
        };

        if result.skip {
            // Spam-filter back-pressure: drop silently, as upstream does.
            return Ok(());
        }

        // The correlated event carries the (possibly aggregate-rewritten)
        // reason + message. `reason` is never rewritten by aggregation, but
        // `message` may collapse to the "(combined from similar events)" form
        // once the per-reason flood crosses the aggregation threshold.
        let correlated = result
            .event
            .expect("a non-skipped correlation always yields an event");

        self.persist(involved, source, event_type, &correlated)
            .await
    }

    /// Create a fresh `Event`, or bump the existing one for this
    /// `(involvedObject, reason)`.
    async fn persist(
        &self,
        involved: &ObjectReference,
        source: &EventSource,
        event_type: EventType,
        correlated: &CorrelatorEvent,
    ) -> Result<()> {
        let namespace = involved.namespace.as_deref().unwrap_or("default");
        // Stable, message-independent name (object.reason.uid) — identical to
        // the key the controller-manager already uses, so emissions from any
        // component de-duplicate against the same stored object.
        let name = Event::generate_name(involved, &correlated.reason);
        let key = format!("/registry/events/{}/{}", namespace, name);

        if let Ok(mut existing) = self.storage.get::<Event>(&key).await {
            // Recurrence: bump count. Storage is the authority, so we take
            // `max(stored + 1, correlator_count)` — this never regresses a
            // count the correlator's freshly-restarted logger has forgotten.
            let now = correlated.last_timestamp;
            let bumped = existing.count.saturating_add(1).max(correlated.count);
            existing.count = bumped;
            existing.last_timestamp = Some(now);
            existing.message = correlated.message.clone();
            existing.series = Some(EventSeries {
                count: bumped,
                last_observed_time: now,
            });
            self.storage
                .update_with_ttl(&key, &existing, self.event_ttl)
                .await?;
            return Ok(());
        }

        // First occurrence: create.
        let mut event = Event::new(
            name,
            namespace.to_string(),
            involved.clone(),
            correlated.reason.clone(),
            correlated.message.clone(),
            event_type,
        );
        event.source = source.clone();
        event.count = correlated.count.max(1);
        event.first_timestamp = Some(correlated.first_timestamp);
        event.last_timestamp = Some(correlated.last_timestamp);
        self.storage
            .create_with_ttl(&key, &event, self.event_ttl)
            .await?;
        Ok(())
    }
}

/// Translate the wire `(involved, source, type, reason, message)` tuple into the
/// correlator's minimal event model.
fn build_correlator_event(
    involved: &ObjectReference,
    source: &EventSource,
    event_type: &EventType,
    reason: &str,
    message: &str,
) -> CorrelatorEvent {
    let now = chrono::Utc::now();
    CorrelatorEvent {
        name: String::new(),
        namespace: involved.namespace.clone().unwrap_or_default(),
        resource_version: String::new(),
        reason: reason.to_string(),
        message: message.to_string(),
        involved_object: CorrelatorObjectReference {
            kind: involved.kind.clone().unwrap_or_default(),
            namespace: involved.namespace.clone().unwrap_or_default(),
            name: involved.name.clone().unwrap_or_default(),
            field_path: involved.field_path.clone().unwrap_or_default(),
            uid: involved.uid.clone().unwrap_or_default(),
            api_version: involved.api_version.clone().unwrap_or_default(),
        },
        source: CorrelatorEventSource {
            component: source.component.clone(),
            host: source.host.clone().unwrap_or_default(),
        },
        event_type: match event_type {
            EventType::Normal => CorrelatorEventType::Normal,
            EventType::Warning => CorrelatorEventType::Warning,
        },
        reporting_controller: source.component.clone(),
        reporting_instance: source.host.clone().unwrap_or_default(),
        count: 1,
        first_timestamp: now,
        last_timestamp: now,
    }
}
