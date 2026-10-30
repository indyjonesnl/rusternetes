//! APF watch tracking and the watch initialization signal.
//!
//! Ports, from `staging/src/k8s.io/apiserver/pkg` (release-1.35):
//!
//! - `util/flowcontrol/watch_tracker.go`: `WatchTracker`, `RegisterWatch`
//!   (:131-157), `updateIndexLocked` (:159-181), `forgetWatch` (:183-193),
//!   `GetInterestedWatchCount` (:195-233), `getBuiltinIndexes` (:78-87).
//! - `util/flowcontrol/apf_context.go`: `InitializationSignal` (:66-100),
//!   `WithInitializationSignal`, `WatchInitialized` (:47-53).
//!
//! The Go signal rides the request `context.Context`; the Rust equivalent is a
//! task-local scoped around the handler call by the filter
//! ([`scope_signal`]), which the watch handlers read through
//! [`watch_initialized`]. Upstream calls `WatchInitialized` from
//! `cacher/cache_watcher.go:527` (`process`, once the initial events are queued)
//! and `etcd3/watcher.go:124`; ours calls it from the watch handlers once the
//! initial state is snapshotted and the live stream is open.
//!
//! DEVIATION: upstream's `getIndexValue` decodes `ListOptions` through
//! `ParameterCodec`; we read the `fieldSelector` parameter directly
//! ([`crate::audit::field_selector_exact_match`]). An unparsable selector is
//! `<unset>` in both.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::audit::field_selector_exact_match;
use crate::flow_control_work_estimator::RequestInfo;

/// `readOnlyVerbs`: `get`, `list`, `watch`, `proxy`.
fn is_read_only_verb(verb: &str) -> bool {
    matches!(verb, "get" | "list" | "watch" | "proxy")
}

/// `watchIdentifier`: watches are similar when they share the resource type,
/// namespace and name; selectors are ignored.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct WatchIdentifier {
    api_group: String,
    resource: String,
    namespace: String,
    name: String,
}

/// `unsetValue`.
const UNSET_VALUE: &str = "<unset>";

/// `getBuiltinIndexes`: the indexes watchcache keeps that speed up watch
/// processing. Only `spec.nodeName` for pods needs listing; the
/// `metadata.name` indexes are covered by `RequestInfo.Name`.
fn builtin_index_field(resource: &str) -> Option<&'static str> {
    match resource {
        "pods" => Some("spec.nodeName"),
        _ => None,
    }
}

/// `indexValue`: the value of the index field a watch's selector pins.
struct IndexValue {
    value: String,
}

struct TrackerInner {
    /// `watchCount`.
    watch_count: Mutex<HashMap<WatchIdentifier, i64>>,
}

impl TrackerInner {
    /// `updateIndexLocked`.
    fn update_index_locked(
        counts: &mut HashMap<WatchIdentifier, i64>,
        identifier: &WatchIdentifier,
        index: Option<&IndexValue>,
        incr: i64,
    ) {
        match index {
            None => *counts.entry(identifier.clone()).or_insert(0) += incr,
            Some(index) => {
                // For a resource with an index, a watch event is only
                // processed for watchers that (a) do not select on the index
                // field or (b) select its value in the processed object. As
                // upstream does, (b) is approximated by the value "".
                if index.value == UNSET_VALUE || index.value.is_empty() {
                    *counts.entry(identifier.clone()).or_insert(0) += incr;
                }
            }
        }
    }
}

/// `ForgetWatchFunc`: the watch is forgotten when this is dropped, exactly once.
pub struct ForgetWatch {
    inner: Arc<TrackerInner>,
    identifier: WatchIdentifier,
    index: Option<IndexValue>,
}

impl Drop for ForgetWatch {
    /// `forgetWatch`'s returned func (watch_tracker.go:183-193).
    fn drop(&mut self) {
        let mut counts = self
            .inner
            .watch_count
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        TrackerInner::update_index_locked(&mut counts, &self.identifier, self.index.as_ref(), -1);
        if counts.get(&self.identifier) == Some(&0) {
            counts.remove(&self.identifier);
        }
    }
}

/// `WatchTracker`: tracks the number of watches in the system to estimate the
/// cost of incoming mutating requests.
///
/// Upstream's TODO stands: only this API server's watches are tracked.
pub struct WatchTracker {
    inner: Arc<TrackerInner>,
}

impl Default for WatchTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchTracker {
    /// `NewWatchTracker`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TrackerInner {
                watch_count: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// `RegisterWatch` (watch_tracker.go:131-157): registers a watch request
    /// (`info` as the request-info filter decided it, `query` the raw query
    /// string) and returns what forgets it again. `None` for a request that is
    /// not a watch.
    pub fn register_watch(&self, info: &RequestInfo, query: &str) -> Option<ForgetWatch> {
        if info.verb != "watch" {
            return None;
        }
        let index = builtin_index_field(&info.resource).map(|field| IndexValue {
            value: field_selector_exact_match(query, field)
                .unwrap_or_else(|| UNSET_VALUE.to_string()),
        });
        let identifier = WatchIdentifier {
            api_group: info.api_group.clone(),
            resource: info.resource.clone(),
            namespace: info.namespace.clone(),
            name: info.name.clone(),
        };
        {
            let mut counts = self
                .inner
                .watch_count
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            TrackerInner::update_index_locked(&mut counts, &identifier, index.as_ref(), 1);
        }
        Some(ForgetWatch {
            inner: self.inner.clone(),
            identifier,
            index,
        })
    }

    /// `GetInterestedWatchCount` (watch_tracker.go:195-233): the number of
    /// watches potentially interested in a request, for estimating its cost.
    pub fn get_interested_watch_count(&self, info: Option<&RequestInfo>) -> i64 {
        let Some(info) = info else { return 0 };
        if is_read_only_verb(&info.verb) {
            return 0;
        }
        // Interested: watches of the whole resource type, of the same
        // namespace, and of this very object.
        let mut identifier = WatchIdentifier {
            api_group: info.api_group.clone(),
            resource: info.resource.clone(),
            namespace: String::new(),
            name: String::new(),
        };
        let counts = self
            .inner
            .watch_count
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let count = |id: &WatchIdentifier| counts.get(id).copied().unwrap_or(0);
        let mut result = count(&identifier);
        if !info.namespace.is_empty() {
            identifier.namespace = info.namespace.clone();
            result += count(&identifier);
        }
        if !info.name.is_empty() {
            identifier.name = info.name.clone();
            result += count(&identifier);
        }
        result
    }

    pub fn watch_count_for_test(&self, g: &str, r: &str, ns: &str, n: &str) -> i64 {
        let id = WatchIdentifier {
            api_group: g.into(),
            resource: r.into(),
            namespace: ns.into(),
            name: n.into(),
        };
        self.inner
            .watch_count
            .lock()
            .unwrap()
            .get(&id)
            .copied()
            .unwrap_or(0)
    }
}

/// `InitializationSignal` (apf_context.go:66-100): sent once a watch is
/// initialized; `Signal` is idempotent (`sync.Once`).
#[derive(Clone)]
pub struct InitializationSignal {
    tx: Arc<watch::Sender<bool>>,
}

impl InitializationSignal {
    /// `NewInitializationSignal`.
    pub fn new() -> Self {
        Self {
            tx: Arc::new(watch::channel(false).0),
        }
    }

    /// `Signal`.
    pub fn signal(&self) {
        self.tx.send_replace(true);
    }

    /// `Wait`: returns once signalled (immediately if it already was).
    pub async fn wait(&self) {
        let mut rx = self.tx.subscribe();
        let _ = rx.wait_for(|signalled| *signalled).await;
    }
}

impl Default for InitializationSignal {
    fn default() -> Self {
        Self::new()
    }
}

tokio::task_local! {
    /// The signal of the watch being served (`WithInitializationSignal`).
    static INITIALIZATION_SIGNAL: InitializationSignal;
}

/// `WithInitializationSignal`: run `fut` with `signal` in scope.
pub async fn scope_signal<F: std::future::Future>(
    signal: InitializationSignal,
    fut: F,
) -> F::Output {
    INITIALIZATION_SIGNAL.scope(signal, fut).await
}

/// `utilflowcontrol.WatchInitialized(ctx)` (apf_context.go:49-53): tell the
/// dispatcher the watch in scope is initialized; a no-op outside a watch
/// served through the filter.
pub fn watch_initialized() {
    let _ = INITIALIZATION_SIGNAL.try_with(|s| s.signal());
}
