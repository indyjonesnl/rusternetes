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

    #[cfg(test)]
    fn watch_count_for_test(&self, g: &str, r: &str, ns: &str, n: &str) -> i64 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::request_info;
    use axum::http::Method;

    /// `requestInfoFactory.NewRequestInfo` + `httpRequest` of the Go tests.
    fn info_of(method: &str, path: &str, query: &str) -> RequestInfo {
        let a = request_info(
            &Method::from_bytes(method.as_bytes()).unwrap(),
            path,
            Some(query),
        );
        RequestInfo {
            verb: a.verb,
            api_group: a.api_group,
            resource: a.resource,
            subresource: a.subresource,
            namespace: a.namespace,
            name: a.name,
        }
    }

    fn count_of(t: &WatchTracker, group: &str, res: &str, ns: &str, name: &str) -> i64 {
        t.watch_count_for_test(group, res, ns, name)
    }

    type RegisterCase = (
        &'static str,
        &'static str,
        &'static str,
        Option<(&'static str, &'static str, &'static str, &'static str)>,
    );

    /// TestRegisterWatch (watch_tracker_test.go:48-141).
    #[test]
    fn register_watch() {
        let cases: Vec<RegisterCase> = vec![
            (
                "watch all objects",
                "/api/v1/pods",
                "watch=true",
                Some(("", "pods", "", "")),
            ),
            ("list all objects", "/api/v1/pods", "", None),
            (
                "watch namespace-scoped objects",
                "/api/v1/namespaces/foo/pods",
                "watch=true",
                Some(("", "pods", "foo", "")),
            ),
            (
                "watch single object",
                "/api/v1/namespaces/foo/pods",
                "watch=true&fieldSelector=metadata.name=mypod",
                Some(("", "pods", "foo", "mypod")),
            ),
            (
                "watch single cluster-scoped object",
                "/api/v1/namespaces",
                "watch=true&fieldSelector=metadata.name=myns",
                Some(("", "namespaces", "", "myns")),
            ),
            (
                "watch all objects from api-group",
                "/apis/group/v1/pods",
                "watch=true",
                Some(("group", "pods", "", "")),
            ),
            (
                "watch namespace-scoped objects (group)",
                "/apis/group/v1/namespaces/foo/pods",
                "watch=true",
                Some(("group", "pods", "foo", "")),
            ),
            (
                "watch single object (group)",
                "/apis/group/v1/namespaces/foo/pods",
                "watch=true&fieldSelector=metadata.name=mypod",
                Some(("group", "pods", "foo", "mypod")),
            ),
            (
                "watch indexed object",
                "/apis/group/v1/namespaces/foo/pods",
                "watch=true&fieldSelector=spec.nodeName=",
                Some(("group", "pods", "foo", "")),
            ),
        ];
        for (name, path, query, expected) in cases {
            let tracker = Arc::new(WatchTracker::new());
            let info = info_of("GET", path, query);
            let forget = tracker.register_watch(&info, query);
            let Some((g, r, ns, n)) = expected else {
                assert!(forget.is_none(), "{name}: unexpected watch registered");
                continue;
            };
            let forget = forget.unwrap_or_else(|| panic!("{name}: watch should be registered"));
            assert_eq!(count_of(&tracker, g, r, ns, n), 1, "{name}");
            drop(forget);
            assert_eq!(
                count_of(&tracker, g, r, ns, n),
                0,
                "{name}: forget unregisters"
            );
        }
    }

    fn register_all(t: &Arc<WatchTracker>, watches: &[(&str, &str)]) -> Vec<ForgetWatch> {
        watches
            .iter()
            .map(|(path, query)| {
                t.register_watch(&info_of("GET", path, query), query)
                    .unwrap_or_else(|| panic!("watch wasn't registered: {path}?{query}"))
            })
            .collect()
    }

    /// TestGetInterestedWatchCount (watch_tracker_test.go:143-265).
    #[test]
    fn get_interested_watch_count() {
        let t = Arc::new(WatchTracker::new());
        let _held = register_all(
            &t,
            &[
                ("api/v1/pods", "watch=true"),
                ("api/v1/namespaces/foo/pods", "watch=true"),
                (
                    "api/v1/namespaces/foo/pods",
                    "watch=true&fieldSelector=metadata.name=mypod",
                ),
                (
                    "api/v1/namespaces/bar/pods",
                    "watch=true&fieldSelector=metadata.name=mypod",
                ),
                ("apis/group/v1/namespaces/foo/pods", "watch=true"),
                (
                    "apis/group/v1/namespaces/bar/pods",
                    "watch=true&fieldSelector=metadata.name=mypod",
                ),
            ],
        );
        let cases: Vec<(&str, &str, &str, &str, i64)> = vec![
            (
                "pod creation in foo namespace",
                "POST",
                "/api/v1/namespaces/foo/pods",
                "",
                2,
            ),
            (
                "mypod update in foo namespace",
                "PUT",
                "/api/v1/namespaces/foo/pods/mypod",
                "",
                3,
            ),
            (
                "mypod patch in foo namespace",
                "PATCH",
                "/api/v1/namespaces/foo/pods/mypod",
                "",
                3,
            ),
            (
                "mypod deletion in foo namespace",
                "DELETE",
                "/api/v1/namespaces/foo/pods/mypod",
                "",
                3,
            ),
            (
                "otherpod update in foo namespace",
                "PUT",
                "/api/v1/namespaces/foo/pods/otherpod",
                "",
                2,
            ),
            (
                "mypod get in foo namespace",
                "GET",
                "/api/v1/namespaces/foo/pods/mypod",
                "",
                0,
            ),
            (
                "pods list in foo namespace",
                "GET",
                "/api/v1/namespaces/foo/pods",
                "",
                0,
            ),
            (
                "pods watch in foo namespace",
                "GET",
                "/api/v1/namespaces/foo/pods",
                "watch=true",
                0,
            ),
            (
                "pods proxy in foo namespace",
                "GET",
                "/api/v1/proxy/namespaces/foo/pods/mypod",
                "",
                0,
            ),
            (
                "pod creation in bar namespace",
                "POST",
                "/api/v1/namespaces/bar/pods",
                "",
                1,
            ),
            (
                "mypod update in bar namespace",
                "PUT",
                "/api/v1/namespaces/bar/pods/mypod",
                "",
                2,
            ),
            (
                "mypod update in foo namespace in group group",
                "PUT",
                "/apis/group/v1/namespaces/foo/pods/mypod",
                "",
                1,
            ),
            (
                "otherpod update in foo namespace in group group",
                "PUT",
                "/apis/group/v1/namespaces/foo/pods/otherpod",
                "",
                1,
            ),
            (
                "mypod update in var namespace in group group",
                "PUT",
                "/apis/group/v1/namespaces/bar/pods/mypod",
                "",
                1,
            ),
            (
                "otherpod update in bar namespace in group group",
                "PUT",
                "/apis/group/v1/namespaces/bar/pods/otherpod",
                "",
                0,
            ),
        ];
        for (name, method, path, query, expected) in cases {
            let got = t.get_interested_watch_count(Some(&info_of(method, path, query)));
            assert_eq!(got, expected, "{name}");
        }
        assert_eq!(t.get_interested_watch_count(None), 0);
    }

    /// TestGetInterestedWatchCountWithIndex (watch_tracker_test.go:267-335).
    #[test]
    fn get_interested_watch_count_with_index() {
        let t = Arc::new(WatchTracker::new());
        let _held = register_all(
            &t,
            &[
                ("api/v1/pods", "watch=true"),
                ("api/v1/namespaces/foo/pods", "watch=true"),
                (
                    "api/v1/namespaces/foo/pods",
                    "watch=true&fieldSelector=metadata.name=mypod",
                ),
                (
                    "api/v1/namespaces/foo/pods",
                    "watch=true&fieldSelector=spec.nodeName=",
                ),
                // The watches below are ignored due to the index.
                (
                    "api/v1/namespaces/foo/pods",
                    "watch=true&fieldSelector=spec.nodeName=node1",
                ),
                (
                    "api/v1/namespaces/foo/pods",
                    "watch=true&fieldSelector=spec.nodeName=node2",
                ),
            ],
        );
        for (name, method, path, expected) in [
            (
                "pod creation in foo namespace",
                "POST",
                "/api/v1/namespaces/foo/pods",
                3,
            ),
            (
                "mypod update in foo namespace",
                "PUT",
                "/api/v1/namespaces/foo/pods/mypod",
                4,
            ),
            (
                "mypod patch in foo namespace",
                "PATCH",
                "/api/v1/namespaces/foo/pods/mypod",
                4,
            ),
            (
                "mypod deletion in foo namespace",
                "DELETE",
                "/api/v1/namespaces/foo/pods/mypod",
                4,
            ),
        ] {
            let got = t.get_interested_watch_count(Some(&info_of(method, path, "")));
            assert_eq!(got, expected, "{name}");
        }
    }

    /// Forgetting a watch that was ignored due to the index must not disturb
    /// the count of the identifier it shares with real watchers
    /// (`forgetWatch` applies the same `updateIndexLocked(-1)`).
    #[test]
    fn forgetting_an_indexed_watch_leaves_other_watchers_counted() {
        let t = Arc::new(WatchTracker::new());
        let q = "watch=true&fieldSelector=spec.nodeName=node1";
        let ignored = t
            .register_watch(&info_of("GET", "/api/v1/namespaces/foo/pods", q), q)
            .unwrap();
        let counted = t
            .register_watch(
                &info_of("GET", "/api/v1/namespaces/foo/pods", "watch=true"),
                "watch=true",
            )
            .unwrap();
        drop(ignored);
        assert_eq!(count_of(&t, "", "pods", "foo", ""), 1);
        drop(counted);
        assert_eq!(count_of(&t, "", "pods", "foo", ""), 0);
    }

    /// `WatchInitialized` signals the signal in scope and is a no-op without
    /// one (apf_context.go:47-53).
    #[tokio::test]
    async fn watch_initialized_signals_the_scoped_signal() {
        watch_initialized(); // no signal in scope: must not panic
        let s = InitializationSignal::new();
        scope_signal(s.clone(), async {
            watch_initialized();
        })
        .await;
        tokio::time::timeout(std::time::Duration::from_secs(1), s.wait())
            .await
            .expect("the scoped signal fired");
    }

    /// `initializationSignal.Signal` is idempotent (sync.Once) and `Wait`
    /// returns for a signal that already fired.
    #[tokio::test]
    async fn signal_is_idempotent_and_wait_returns_after_it_fired() {
        let s = InitializationSignal::new();
        s.signal();
        s.signal();
        tokio::time::timeout(std::time::Duration::from_secs(1), s.wait())
            .await
            .expect("wait returns once signalled");
    }
}
