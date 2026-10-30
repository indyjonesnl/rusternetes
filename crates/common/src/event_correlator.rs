//! Event correlation: aggregation, de-duplication/counting, and spam filtering.
//!
//! This is a faithful port of upstream Kubernetes'
//! `staging/src/k8s.io/client-go/tools/record/events_cache.go` (release-1.35).
//!
//! The correlator processes incoming events and performs analysis to avoid
//! overwhelming the system:
//!
//! - **Aggregation**: similar events (differing only by message) seen more than
//!   `maxEvents` (10) times within `maxIntervalInSeconds` (600s) are combined
//!   into a single synthetic "(combined from similar events): ..." event.
//! - **De-duplication / counting**: the *exact* same event (including message)
//!   seen multiple times is compacted into one event with an increasing
//!   `count` and an updated `lastTimestamp`, plus a JSON merge patch describing
//!   the delta to send to the server.
//! - **Spam filtering**: a token-bucket per source+object limits the burst of
//!   events about a single object (`burst` = 25, refill `1/300s`).
//!
//! The logic is pure: there is no storage or IO. A [`PassiveClock`] trait is
//! injected so the time-windowed behaviour (aggregation interval, token
//! refill) is deterministically testable, mirroring upstream's
//! `testclocks.SimpleIntervalClock`.

use chrono::{DateTime, Utc};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Maximum entries kept in the LRU caches. Matches upstream `maxLruCacheEntries`.
pub const MAX_LRU_CACHE_ENTRIES: usize = 4096;

/// If we see the same event (varying only by message) more than this many
/// times within [`DEFAULT_AGGREGATE_INTERVAL_IN_SECONDS`], aggregate it.
/// Matches upstream `defaultAggregateMaxEvents`.
pub const DEFAULT_AGGREGATE_MAX_EVENTS: usize = 10;

/// The rolling interval (seconds) over which aggregation is evaluated.
/// Matches upstream `defaultAggregateIntervalInSeconds`.
pub const DEFAULT_AGGREGATE_INTERVAL_IN_SECONDS: u64 = 600;

/// By default, allow a source to send this many events about an object as a
/// burst. Matches upstream `defaultSpamBurst`.
pub const DEFAULT_SPAM_BURST: usize = 25;

/// Token bucket refill rate (queries per second). 1 new event every 300s.
/// Matches upstream `defaultSpamQPS` (`1. / 300.`).
pub const DEFAULT_SPAM_QPS: f64 = 1.0 / 300.0;

/// A passive clock returning "now" — injectable so time windows are testable.
///
/// Mirrors upstream's `clock.PassiveClock`.
pub trait PassiveClock {
    fn now(&self) -> DateTime<Utc>;
}

/// A clock that advances by a fixed interval on every call to [`PassiveClock::now`].
///
/// Faithful port of `k8s.io/utils/clock/testing.SimpleIntervalClock`: the
/// returned time is incremented by `duration` after each read.
///
/// The state is shared (`Rc<Cell<..>>`) so that cloning the clock — as the
/// [`EventCorrelator`] does to give each sub-component its own handle — still
/// observes a single, monotonically advancing timeline. This mirrors upstream
/// where the test passes `&clock` (a pointer) to every component.
#[derive(Clone)]
pub struct SimpleIntervalClock {
    time: Rc<Cell<DateTime<Utc>>>,
    duration: chrono::Duration,
}

impl SimpleIntervalClock {
    pub fn new(start: DateTime<Utc>, duration: chrono::Duration) -> Self {
        Self {
            time: Rc::new(Cell::new(start)),
            duration,
        }
    }
}

impl PassiveClock for SimpleIntervalClock {
    fn now(&self) -> DateTime<Utc> {
        let current = self.time.get();
        self.time.set(current + self.duration);
        current
    }
}

/// A clock that always returns the same fixed time. Useful for assertions that
/// do not depend on the passage of time.
#[derive(Clone)]
pub struct FixedClock {
    time: DateTime<Utc>,
}

impl FixedClock {
    pub fn new(time: DateTime<Utc>) -> Self {
        Self { time }
    }
}

impl PassiveClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.time
    }
}

/// The production clock: returns the real wall-clock time. Unlike the test
/// clocks above (which use `Rc<Cell<..>>` and are therefore `!Send`), this is a
/// zero-sized `Send + Sync + Clone` type, so an [`EventCorrelator<RealClock>`]
/// can live inside an `Arc<Mutex<..>>` shared across async tasks — which is how
/// the [`crate`-level] event recorder holds it.
#[derive(Clone, Copy, Default, Debug)]
pub struct RealClock;

impl PassiveClock for RealClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Event type discriminator. Mirrors `v1.EventTypeNormal` / `v1.EventTypeWarning`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CorrelatorEventType {
    #[default]
    Normal,
    Warning,
}

impl CorrelatorEventType {
    fn as_str(&self) -> &'static str {
        match self {
            CorrelatorEventType::Normal => "Normal",
            CorrelatorEventType::Warning => "Warning",
        }
    }
}

/// Source of an event (component + host). Mirrors `v1.EventSource`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorrelatorEventSource {
    pub component: String,
    pub host: String,
}

/// Reference to the object an event is about. Mirrors the subset of
/// `v1.ObjectReference` used by the correlator key functions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorrelatorObjectReference {
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub field_path: String,
    pub uid: String,
    pub api_version: String,
}

/// A minimal event model mirroring `v1.Event`, holding exactly the fields the
/// correlator reads or writes. This keeps the ported logic and tests a
/// faithful 1:1 translation of upstream, independent of the richer
/// [`crate::resources::Event`] wire type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelatorEvent {
    pub name: String,
    pub namespace: String,
    pub resource_version: String,
    pub reason: String,
    pub message: String,
    pub involved_object: CorrelatorObjectReference,
    pub source: CorrelatorEventSource,
    pub event_type: CorrelatorEventType,
    pub reporting_controller: String,
    pub reporting_instance: String,
    pub count: i32,
    pub first_timestamp: DateTime<Utc>,
    pub last_timestamp: DateTime<Utc>,
}

impl CorrelatorEvent {
    /// Builds the full event key based on source, involvedObject, type, reason
    /// and message. Mirrors upstream `getEventKey`.
    pub fn event_key(&self) -> String {
        [
            self.source.component.as_str(),
            self.source.host.as_str(),
            self.involved_object.kind.as_str(),
            self.involved_object.namespace.as_str(),
            self.involved_object.name.as_str(),
            self.involved_object.field_path.as_str(),
            self.involved_object.uid.as_str(),
            self.involved_object.api_version.as_str(),
            self.event_type.as_str(),
            self.reason.as_str(),
            self.message.as_str(),
        ]
        .concat()
    }
}

/// Builds a unique spam key based on source + involvedObject + type. Mirrors
/// upstream `getSpamKey` (message excluded).
pub fn get_spam_key(event: &CorrelatorEvent) -> String {
    [
        event.source.component.as_str(),
        event.source.host.as_str(),
        event.involved_object.kind.as_str(),
        event.involved_object.namespace.as_str(),
        event.involved_object.name.as_str(),
        event.involved_object.uid.as_str(),
        event.involved_object.api_version.as_str(),
        event.event_type.as_str(),
    ]
    .concat()
}

/// Function that produces a unique spam key for an event.
pub type EventSpamKeyFunc = fn(&CorrelatorEvent) -> String;

/// Aggregates events by exact match on source, involvedObject, type, reason,
/// reportingController and reportingInstance — message is EXCLUDED. Returns
/// `(aggregateKey, localKey)` where `localKey` is the message. Mirrors
/// upstream `EventAggregatorByReasonFunc`.
pub fn event_aggregator_by_reason_func(event: &CorrelatorEvent) -> (String, String) {
    let aggregate_key = [
        event.source.component.as_str(),
        event.source.host.as_str(),
        event.involved_object.kind.as_str(),
        event.involved_object.namespace.as_str(),
        event.involved_object.name.as_str(),
        event.involved_object.uid.as_str(),
        event.involved_object.api_version.as_str(),
        event.event_type.as_str(),
        event.reason.as_str(),
        event.reporting_controller.as_str(),
        event.reporting_instance.as_str(),
    ]
    .concat();
    (aggregate_key, event.message.clone())
}

/// Produces the aggregate message by prefixing the incoming message. Mirrors
/// upstream `EventAggregatorByReasonMessageFunc`.
pub fn event_aggregator_by_reason_message_func(event: &CorrelatorEvent) -> String {
    format!("(combined from similar events): {}", event.message)
}

/// Function that groups events for aggregation.
pub type EventAggregatorKeyFunc = fn(&CorrelatorEvent) -> (String, String);

/// Function that produces an aggregation message.
pub type EventAggregatorMessageFunc = fn(&CorrelatorEvent) -> String;

/// Tracks aggregation state for an aggregate key.
#[derive(Clone, Default)]
struct AggregateRecord {
    local_keys: HashSet<String>,
    last_timestamp: Option<DateTime<Utc>>,
}

/// Identifies similar events and aggregates them into a single event.
///
/// Faithful port of upstream `EventAggregator`.
pub struct EventAggregator<C: PassiveClock> {
    cache: HashMap<String, AggregateRecord>,
    key_func: EventAggregatorKeyFunc,
    message_func: EventAggregatorMessageFunc,
    max_events: usize,
    max_interval_in_seconds: u64,
    clock: C,
}

impl<C: PassiveClock> EventAggregator<C> {
    pub fn new(
        key_func: EventAggregatorKeyFunc,
        message_func: EventAggregatorMessageFunc,
        max_events: usize,
        max_interval_in_seconds: u64,
        clock: C,
    ) -> Self {
        Self {
            cache: HashMap::new(),
            key_func,
            message_func,
            max_events,
            max_interval_in_seconds,
            clock,
        }
    }

    /// Checks if a similar event has been seen according to the aggregation
    /// configuration and returns the (possibly synthetic) event to create plus
    /// the cache key for correlation. Mirrors upstream `EventAggregate`.
    pub fn event_aggregate(&mut self, new_event: &CorrelatorEvent) -> (CorrelatorEvent, String) {
        let now = self.clock.now();
        // eventKey is the full cache key for this event.
        let event_key = new_event.event_key();
        // aggregateKey is for the aggregate event, if one is needed.
        let (aggregate_key, local_key) = (self.key_func)(new_event);

        let mut record = self.cache.get(&aggregate_key).cloned().unwrap_or_default();

        // Is the previous record too old? If so, make a fresh one. If we didn't
        // find a record, its last_timestamp is None and we treat it as new.
        let max_interval = chrono::Duration::seconds(self.max_interval_in_seconds as i64);
        let too_old = match record.last_timestamp {
            Some(ts) => (now - ts) > max_interval,
            None => true,
        };
        if too_old {
            record = AggregateRecord::default();
        }

        // Write the new event into the aggregation record and cache it.
        record.local_keys.insert(local_key);
        record.last_timestamp = Some(now);
        self.cache.insert(aggregate_key.clone(), record.clone());

        // Not yet over the threshold for unique events — don't correlate.
        if record.local_keys.len() < self.max_events {
            return (new_event.clone(), event_key);
        }

        // Do not grow the local key set larger than max: drop one entry, then
        // persist the trimmed record so the cache size stays bounded.
        if let Some(any) = record.local_keys.iter().next().cloned() {
            record.local_keys.remove(&any);
        }
        self.cache.insert(aggregate_key.clone(), record);

        // Create a synthetic aggregate event, returning the aggregateKey as the
        // cache key (so it can be overwritten on subsequent observations).
        let event_copy = CorrelatorEvent {
            name: format!(
                "{}.{:x}",
                new_event.involved_object.name,
                now.timestamp_nanos_opt().unwrap_or(0)
            ),
            namespace: new_event.namespace.clone(),
            resource_version: String::new(),
            count: 1,
            first_timestamp: now,
            last_timestamp: now,
            involved_object: new_event.involved_object.clone(),
            message: (self.message_func)(new_event),
            event_type: new_event.event_type.clone(),
            reason: new_event.reason.clone(),
            source: new_event.source.clone(),
            reporting_controller: new_event.reporting_controller.clone(),
            reporting_instance: new_event.reporting_instance.clone(),
        };
        (event_copy, aggregate_key)
    }
}

/// Records data about when an event was observed. Mirrors upstream `eventLog`.
#[derive(Clone, Default)]
struct EventLog {
    count: u32,
    first_timestamp: Option<DateTime<Utc>>,
    name: String,
    resource_version: String,
}

/// Logs occurrences of an event and produces count-bump patches on repeats.
///
/// Faithful port of upstream `eventLogger`.
pub struct EventLogger<C: PassiveClock> {
    cache: HashMap<String, EventLog>,
    #[allow(dead_code)]
    clock: C,
}

impl<C: PassiveClock> EventLogger<C> {
    pub fn new(clock: C) -> Self {
        Self {
            cache: HashMap::new(),
            clock,
        }
    }

    /// Records an event, or updates an existing one if `key` is a cache hit.
    /// Returns the (possibly count-incremented) event and an optional JSON
    /// merge patch describing the delta. Mirrors upstream `eventObserve`.
    pub fn event_observe(
        &mut self,
        new_event: &CorrelatorEvent,
        key: &str,
    ) -> (CorrelatorEvent, Option<Vec<u8>>) {
        let mut event = new_event.clone();
        let mut patch: Option<Vec<u8>> = None;

        let last_observation = self.cache.get(key).cloned().unwrap_or_default();

        // If we found a prior observation, prepare a patch.
        if last_observation.count > 0 {
            event.name = last_observation.name.clone();
            event.resource_version = last_observation.resource_version.clone();
            if let Some(ts) = last_observation.first_timestamp {
                event.first_timestamp = ts;
            }
            event.count = last_observation.count as i32 + 1;

            // JSON merge patch describing the count / lastTimestamp / RV delta.
            // (Upstream emits a strategic-merge patch; for our single-document
            // Event the equivalent JSON merge patch carries the same fields.)
            let patch_json = serde_json::json!({
                "count": event.count,
                "lastTimestamp": event.last_timestamp.to_rfc3339(),
                "metadata": { "resourceVersion": event.resource_version },
            });
            patch = Some(patch_json.to_string().into_bytes());
        }

        // Record our new observation.
        self.cache.insert(
            key.to_string(),
            EventLog {
                count: event.count.max(0) as u32,
                first_timestamp: Some(event.first_timestamp),
                name: event.name.clone(),
                resource_version: event.resource_version.clone(),
            },
        );

        (event, patch)
    }

    /// Updates internal tracking based on the latest server state. Mirrors
    /// upstream `updateState`.
    pub fn update_state(&mut self, event: &CorrelatorEvent) {
        let key = event.event_key();
        self.cache.insert(
            key,
            EventLog {
                count: event.count.max(0) as u32,
                first_timestamp: Some(event.first_timestamp),
                name: event.name.clone(),
                resource_version: event.resource_version.clone(),
            },
        );
    }
}

/// A token-bucket rate limiter faithful to `golang.org/x/time/rate.Limiter` as
/// driven by `flowcontrol.NewTokenBucketPassiveRateLimiterWithClock`.
///
/// Tokens start full at `burst`, refill at `qps` tokens/second capped at
/// `burst`. `try_accept(now)` succeeds iff at least one token is available at
/// `now`, consuming it.
#[derive(Clone)]
pub struct TokenBucket {
    qps: f64,
    burst: f64,
    tokens: f64,
    // The instant the bucket state was last advanced. `None` means "never",
    // which upstream models with the zero time (so the first call always sees
    // the bucket as full).
    last: Option<DateTime<Utc>>,
}

impl TokenBucket {
    pub fn new(qps: f64, burst: usize) -> Self {
        Self {
            qps,
            burst: burst as f64,
            tokens: burst as f64,
            last: None,
        }
    }

    /// Returns the number of tokens available at `t` accounting for refill,
    /// capped at `burst`. Mirrors `rate.Limiter.advance`.
    fn advance(&self, t: DateTime<Utc>) -> f64 {
        let last = match self.last {
            Some(l) if l <= t => l,
            Some(_) => t, // t before last: clamp
            None => t,    // first call: elapsed measured from t == 0 refill
        };
        let elapsed = (t - last).num_microseconds().unwrap_or(0) as f64 / 1_000_000.0;
        let delta = elapsed * self.qps;
        (self.tokens + delta).min(self.burst)
    }

    /// Attempts to consume one token at time `t`. Mirrors `AllowN(t, 1)`.
    pub fn try_accept(&mut self, t: DateTime<Utc>) -> bool {
        let tokens = self.advance(t) - 1.0;
        // ok iff no wait is required (tokens non-negative) — burst >= 1 always
        // holds for our configs.
        let ok = tokens >= 0.0;
        if ok {
            self.last = Some(t);
            self.tokens = tokens;
        }
        ok
    }
}

/// Holds the rate limiter used for spam decisions about one source+object.
#[derive(Clone)]
struct SpamRecord {
    rate_limiter: TokenBucket,
}

/// Throttles the amount of events a source+object can produce.
///
/// Faithful port of upstream `EventSourceObjectSpamFilter`.
pub struct EventSourceObjectSpamFilter<C: PassiveClock> {
    cache: HashMap<String, SpamRecord>,
    burst: usize,
    qps: f64,
    clock: C,
    spam_key_func: EventSpamKeyFunc,
}

impl<C: PassiveClock> EventSourceObjectSpamFilter<C> {
    pub fn new(burst: usize, qps: f64, clock: C, spam_key_func: EventSpamKeyFunc) -> Self {
        Self {
            cache: HashMap::new(),
            burst,
            qps,
            clock,
            spam_key_func,
        }
    }

    /// Returns `true` if the event should be DROPPED (rate exceeded), `false`
    /// if it should be allowed. Mirrors upstream `Filter`.
    pub fn filter(&mut self, event: &CorrelatorEvent) -> bool {
        let event_key = (self.spam_key_func)(event);
        let mut record = self.cache.get(&event_key).cloned().unwrap_or(SpamRecord {
            rate_limiter: TokenBucket::new(self.qps, self.burst),
        });

        // True == drop (no token available).
        let filtered = !record.rate_limiter.try_accept(self.clock.now());

        self.cache.insert(event_key, record.clone());
        let _ = &mut record;
        filtered
    }
}

/// The result of an [`EventCorrelator::event_correlate`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCorrelateResult {
    /// The event after correlation (None if skipped).
    pub event: Option<CorrelatorEvent>,
    /// If present, a patch to apply when updating the server record.
    pub patch: Option<Vec<u8>>,
    /// If true, do no further processing of the event.
    pub skip: bool,
}

/// Options to configure an [`EventCorrelator`]. Mirrors upstream
/// `CorrelatorOptions`; unset (`None`/zero) fields fall back to defaults.
pub struct CorrelatorOptions<C: PassiveClock> {
    pub clock: C,
    pub burst_size: Option<usize>,
    pub qps: Option<f64>,
    pub key_func: Option<EventAggregatorKeyFunc>,
    pub message_func: Option<EventAggregatorMessageFunc>,
    pub max_events: Option<usize>,
    pub max_interval_in_seconds: Option<u64>,
    pub spam_key_func: Option<EventSpamKeyFunc>,
}

/// Processes incoming events: filter → aggregate → count/de-duplicate.
///
/// Faithful port of upstream `EventCorrelator`. The three stages run in the
/// same order as upstream `EventCorrelate`.
pub struct EventCorrelator<C: PassiveClock + Clone> {
    spam_filter: EventSourceObjectSpamFilter<C>,
    aggregator: EventAggregator<C>,
    logger: EventLogger<C>,
}

impl<C: PassiveClock + Clone> EventCorrelator<C> {
    /// Builds a correlator with the production defaults (burst 25, qps 1/300,
    /// aggregate after 10 in 600s). Mirrors upstream `NewEventCorrelator`.
    pub fn new(clock: C) -> Self {
        Self::with_options(CorrelatorOptions {
            clock,
            burst_size: None,
            qps: None,
            key_func: None,
            message_func: None,
            max_events: None,
            max_interval_in_seconds: None,
            spam_key_func: None,
        })
    }

    /// Builds a correlator from explicit options, applying upstream defaults to
    /// any unset field. Mirrors upstream `NewEventCorrelatorWithOptions`.
    pub fn with_options(options: CorrelatorOptions<C>) -> Self {
        let burst = options.burst_size.unwrap_or(DEFAULT_SPAM_BURST);
        let qps = options.qps.unwrap_or(DEFAULT_SPAM_QPS);
        let key_func = options.key_func.unwrap_or(event_aggregator_by_reason_func);
        let message_func = options
            .message_func
            .unwrap_or(event_aggregator_by_reason_message_func);
        let max_events = options.max_events.unwrap_or(DEFAULT_AGGREGATE_MAX_EVENTS);
        let max_interval = options
            .max_interval_in_seconds
            .unwrap_or(DEFAULT_AGGREGATE_INTERVAL_IN_SECONDS);
        let spam_key_func = options.spam_key_func.unwrap_or(get_spam_key);

        Self {
            spam_filter: EventSourceObjectSpamFilter::new(
                burst,
                qps,
                options.clock.clone(),
                spam_key_func,
            ),
            aggregator: EventAggregator::new(
                key_func,
                message_func,
                max_events,
                max_interval,
                options.clock.clone(),
            ),
            logger: EventLogger::new(options.clock),
        }
    }

    /// Filters, aggregates, counts and de-duplicates an incoming event.
    /// Mirrors upstream `EventCorrelate`.
    pub fn event_correlate(&mut self, new_event: &CorrelatorEvent) -> EventCorrelateResult {
        let (aggregate_event, ckey) = self.aggregator.event_aggregate(new_event);
        let (observed_event, patch) = self.logger.event_observe(&aggregate_event, &ckey);
        if self.spam_filter.filter(&observed_event) {
            return EventCorrelateResult {
                event: None,
                patch: None,
                skip: true,
            };
        }
        EventCorrelateResult {
            event: Some(observed_event),
            patch,
            skip: false,
        }
    }

    /// Updates internal logger state from the latest observed server state.
    /// Mirrors upstream `UpdateState`.
    pub fn update_state(&mut self, event: &CorrelatorEvent) {
        self.logger.update_state(event);
    }
}
