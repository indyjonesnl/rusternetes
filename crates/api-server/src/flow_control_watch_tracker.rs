//! APF watch tracking and the watch initialization signal.
//!
//! Ports, from `staging/src/k8s.io/apiserver/pkg` (release-1.35):
//!
//! - `util/flowcontrol/watch_tracker.go`: `WatchTracker`, `RegisterWatch`,
//!   `forgetWatch`, `GetInterestedWatchCount`, `getBuiltinIndexes`.
//! - `util/flowcontrol/apf_context.go`: `InitializationSignal`,
//!   `WithInitializationSignal`, `WatchInitialized`.
//!
//! The Go signal rides the request `context.Context`; the Rust equivalent is a
//! task-local scoped around the handler call by the filter, which the watch
//! handlers read through [`watch_initialized`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::flow_control_work_estimator::RequestInfo;

/// `ForgetWatchFunc`: dropping it forgets the registered watch.
pub struct ForgetWatch;

/// `WatchTracker`.
#[derive(Default)]
pub struct WatchTracker {
    _watch_count: Mutex<HashMap<(String, String, String, String), i64>>,
}

impl WatchTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// `RegisterWatch`.
    pub fn register_watch(
        self: &Arc<Self>,
        _info: &RequestInfo,
        _query: &str,
    ) -> Option<ForgetWatch> {
        None
    }

    /// `GetInterestedWatchCount`.
    pub fn get_interested_watch_count(&self, _info: Option<&RequestInfo>) -> i64 {
        0
    }

    #[cfg(test)]
    fn watch_count_for_test(&self, _g: &str, _r: &str, _ns: &str, _n: &str) -> i64 {
        0
    }
}

/// `InitializationSignal`.
#[derive(Clone)]
pub struct InitializationSignal;

impl InitializationSignal {
    pub fn new() -> Self {
        Self
    }
    pub fn signal(&self) {}
    pub async fn wait(&self) {
        std::future::pending::<()>().await
    }
}

impl Default for InitializationSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// `WithInitializationSignal`: run `fut` with `signal` in scope.
pub async fn scope_signal<F: std::future::Future>(
    _signal: InitializationSignal,
    fut: F,
) -> F::Output {
    fut.await
}

/// `utilflowcontrol.WatchInitialized(ctx)`.
pub fn watch_initialized() {}

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
