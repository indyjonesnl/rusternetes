//! API Priority and Fairness as a request filter (`WithPriorityAndFairness`).
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/server/filters/priority-and-fairness.go`
//! (release-1.35): `Handle` (:73-323 = long-running exemption, classification,
//! work estimate, execute-or-429), `setResponseHeaders` (:341-352),
//! `tooManyRequests` (:354-358), `getRequestWaitContext` (:372-411); plus
//! `util/flowcontrol/dropped_requests_tracker.go` (the `Retry-After` value) and
//! `server/filters/longrunning.go` (`BasicLongRunningRequestCheck`, with
//! kube-apiserver's sets from `pkg/controlplane/apiserver/config.go:183-186`).
//!
//! PLACEMENT (`server/config.go:1015-1043`, `DefaultBuildHandlerChain`): the
//! chain, outermost first, is authentication -> audit -> impersonation ->
//! priority-and-fairness -> authorization -> handler. Here the filter is layered
//! on the protected routes directly inside `audit_middleware` and inside the
//! authentication/impersonation layer (`auth_middleware`), so it sees the
//! authenticated (and impersonated) user, exactly as upstream.
//!
//! OFF BY DEFAULT. Upstream installs the filter whenever the
//! `APIPriorityAndFairness` gate is on (GA, default on in 1.35). A mis-wired
//! filter can 429 conformance traffic, and this port has not had a
//! sig-api-machinery run yet, so it is opt-in via `--enable-priority-and-fairness`
//! (see [`install_flow_control`]); flipping the default is tracked in #2807.
//!
//! DELIBERATE DEVIATIONS (each tracked as an issue):
//!
//! - Watch initialization signal and `RegisterWatch` (#2808) are ported:
//!   `serve_watch` holds the seat until `watch_initialized()` fires (the watch
//!   handlers call it once the initial state is read and the live stream is
//!   open, `watch_snapshot`) or the handler returns, and keeps the watch
//!   registered with the `WatchTracker` until the response body is dropped.
//!   The tracker feeds the estimator's interested-watcher count. Remaining
//!   gap: a watch handler that never reaches `watch_snapshot` frees its seat
//!   only when it returns.
//! - Object counts come from `flow_control_stats_poller` (the
//!   `Store.startObservingCount` port) for a fixed table of built-in
//!   resources; custom resources are not observed, so their lists report
//!   `ObjectCountNotFound` and cost the minimum number of seats.
//! - No watermark/metrics (`apiserver_flowcontrol_*`, `RecordDroppedRequest`).
//!   (#2809)
//! - `getRequestWaitContext` has no request deadline to take 1/4 of (we have no
//!   `WithRequestDeadline`/timeout filter), so the default limit applies:
//!   `RequestTimeout/4` = 15s (config.go:445, :1027).
//! - Public routes (health probes, discovery, metrics) are not behind the
//!   filter; upstream sends the probes to the `exempt` level and discovery to a
//!   limited one. Never limiting them is the safe side of that difference.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header::RETRY_AFTER, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use rusternetes_storage::{Storage, StorageBackend};

use crate::audit::request_info;
use crate::flow_control::{Classification, FlowControlEngine, FlowControlPermit, RequestDigest};
use crate::flow_control_object_count::ObjectCountTracker;
use crate::flow_control_watch_tracker::{
    scope_signal, ForgetWatch, InitializationSignal, WatchTracker,
};
use crate::flow_control_work_estimator::{RequestInfo, WorkEstimator, WorkEstimatorConfig};

/// `ResponseHeaderMatchedPriorityLevelConfigurationUID`
/// (`k8s.io/api/flowcontrol/v1/types.go:56`).
pub const HEADER_PRIORITY_LEVEL_UID: &str = "X-Kubernetes-PF-PriorityLevel-UID";
/// `ResponseHeaderMatchedFlowSchemaUID` (`types.go:57`).
pub const HEADER_FLOW_SCHEMA_UID: &str = "X-Kubernetes-PF-FlowSchema-UID";

/// `RequestTimeout` default, 60s (`server/config.go:445`).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// The hard cap on a request's queue wait (`getRequestWaitContext`, :402-404).
const MAX_REQUEST_WAIT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------
// getRequestWaitContext
// ---------------------------------------------------------------------

/// `getRequestWaitContext` (priority-and-fairness.go:372-411): how long, from
/// `now`, the request may wait in its queue before being rejected.
///
/// `received_at` is `apirequest.ReceivedTimestampFrom(ctx)` (defaults to `now`),
/// `deadline` the context deadline. With a deadline the limit is a quarter of
/// the time from arrival to it; without, `default_request_wait_limit`; in both
/// cases capped at one minute, and measured from arrival.
pub fn request_wait_limit(
    now: Instant,
    received_at: Option<Instant>,
    deadline: Option<Instant>,
    default_request_wait_limit: Duration,
) -> Duration {
    let arrived_at = received_at.unwrap_or(now);
    let mut limit = default_request_wait_limit;
    if let Some(d) = deadline {
        limit = d.saturating_duration_since(arrived_at) / 4;
    }
    if limit > MAX_REQUEST_WAIT {
        limit = MAX_REQUEST_WAIT;
    }
    (arrived_at + limit).saturating_duration_since(now)
}

// ---------------------------------------------------------------------
// DroppedRequestsTracker
// ---------------------------------------------------------------------

/// `maxRetryAfter` (dropped_requests_tracker.go:32).
const MAX_RETRY_AFTER: i64 = 32;

struct DroppedStats {
    /// `history`: `(unixTime, requests)` per past second.
    history: Vec<(i64, i64)>,
    current_unix: i64,
    current_count: i64,
    retry_after: i64,
    retry_after_update_unix: i64,
}

impl DroppedStats {
    fn new() -> Self {
        Self {
            history: Vec::new(),
            current_unix: 0,
            current_count: 0,
            retry_after: 1,
            retry_after_update_unix: 0,
        }
    }

    /// `recordDroppedRequest` (:79-110).
    fn record(&mut self, unix_time: i64) {
        if self.current_unix == unix_time {
            self.current_count += 1;
            return;
        }
        self.update_history(self.current_unix, self.current_count);
        self.current_unix = unix_time;
        self.current_count = 1;
        // Only considered when bumping the current second (:104-109).
        self.update_retry_after_if_needed(unix_time);
    }

    /// `updateHistory` (:112-128).
    fn update_history(&mut self, unix_time: i64, count: i64) {
        self.history.push((unix_time, count));
        let max_history = (2 * self.retry_after).min(MAX_RETRY_AFTER);
        let start = self
            .history
            .iter()
            .position(|(t, _)| unix_time - t <= max_history)
            .unwrap_or(self.history.len());
        if start > 0 {
            self.history.drain(..start);
        }
    }

    /// `updateRetryAfterIfNeededLocked` (:130-180).
    fn update_retry_after_if_needed(&mut self, unix_time: i64) {
        let mut retry_after = self.retry_after;
        let mut dropped = 0;
        for (t, n) in self.history.iter().rev() {
            if unix_time - t > retry_after {
                break;
            }
            if *t < unix_time {
                dropped += n;
            }
        }
        if unix_time - self.retry_after_update_unix >= retry_after && dropped >= 3 * retry_after {
            // Mimic TCP: double.
            retry_after = (retry_after * 2).min(MAX_RETRY_AFTER);
            self.retry_after = retry_after;
            self.retry_after_update_unix = unix_time;
            return;
        }
        if dropped < retry_after && retry_after > 1 {
            // Mimic TCP: linear decrease.
            self.retry_after = retry_after - 1;
        }
    }
}

/// `DroppedRequestsTracker` (dropped_requests_tracker.go:36-46): per priority
/// level history of dropped requests, used to adapt `Retry-After`.
pub struct DroppedRequestsTracker {
    now: Box<dyn Fn() -> i64 + Send + Sync>,
    pl_stats: RwLock<HashMap<String, Mutex<DroppedStats>>>,
}

impl Default for DroppedRequestsTracker {
    fn default() -> Self {
        Self::with_clock(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
        })
    }
}

impl DroppedRequestsTracker {
    /// `newDroppedRequestsTracker`; `now` returns unix seconds.
    pub fn with_clock(now: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        Self {
            now: Box::new(now),
            pl_stats: RwLock::new(HashMap::new()),
        }
    }

    /// `RecordDroppedRequest` (:196-222).
    pub fn record_dropped_request(&self, pl_name: &str) {
        let unix = (self.now)();
        if let Some(s) = self.pl_stats.read().unwrap().get(pl_name) {
            s.lock().unwrap().record(unix);
            return;
        }
        let mut w = self.pl_stats.write().unwrap();
        w.entry(pl_name.to_string())
            .or_insert_with(|| Mutex::new(DroppedStats::new()))
            .get_mut()
            .unwrap()
            .record(unix);
    }

    /// `GetRetryAfter` (:224-234).
    pub fn get_retry_after(&self, pl_name: &str) -> i64 {
        self.pl_stats
            .read()
            .unwrap()
            .get(pl_name)
            .map(|s| s.lock().unwrap().retry_after)
            .unwrap_or(1)
    }
}

// ---------------------------------------------------------------------
// Long-running check
// ---------------------------------------------------------------------

/// `BasicLongRunningRequestCheck` (longrunning.go:28-40) with kube-apiserver's
/// `("watch","proxy")` verbs and `("attach","exec","proxy","log","portforward")`
/// subresources (`pkg/controlplane/apiserver/config.go:183-186`).
pub fn is_long_running(
    verb: &str,
    is_resource_request: bool,
    subresource: &str,
    path: &str,
) -> bool {
    if verb == "watch" || verb == "proxy" {
        return true;
    }
    if is_resource_request
        && matches!(
            subresource,
            "attach" | "exec" | "proxy" | "log" | "portforward"
        )
    {
        return true;
    }
    !is_resource_request && path.starts_with("/debug/pprof/")
}

// ---------------------------------------------------------------------
// The filter
// ---------------------------------------------------------------------

/// The state `WithPriorityAndFairness` closes over: `fcIfc`, `workEstimator`,
/// `droppedRequests` and `defaultRequestWaitLimit`.
pub struct ApfFilter<S: Storage> {
    engine: Arc<FlowControlEngine<S>>,
    estimator: WorkEstimator,
    /// `StorageObjectCountTracker` (config.go:472); fed by
    /// `flow_control_stats_poller`, read by the list estimator.
    object_counts: Arc<ObjectCountTracker>,
    dropped: DroppedRequestsTracker,
    default_wait_limit: Duration,
    watch_tracker: Arc<WatchTracker>,
}

impl<S: Storage + 'static> ApfFilter<S> {
    /// `defaultRequestWaitLimit` is `RequestTimeout/4` (config.go:1027).
    pub fn new(engine: Arc<FlowControlEngine<S>>, default_wait_limit: Duration) -> Self {
        let max_seats_engine = engine.clone();
        let object_counts = Arc::new(ObjectCountTracker::new());
        // The estimator's `watchCountGetter` is
        // `FlowControl.GetInterestedWatchCount` (server/config.go:1025); the
        // tracker is the controller's `NewWatchTracker()`
        // (apf_controller.go:289).
        let watch_tracker = Arc::new(WatchTracker::new());
        let counting_tracker = watch_tracker.clone();
        let estimator = WorkEstimator::new(
            // `c.StorageObjectCountTracker.Get` (server/config.go:1025).
            object_counts.stats_getter(),
            Box::new(move |info| counting_tracker.get_interested_watch_count(Some(info))),
            WorkEstimatorConfig::default(),
            Box::new(move |pl| max_seats_engine.max_seats(pl)),
        );
        Self {
            engine,
            estimator,
            object_counts,
            dropped: DroppedRequestsTracker::default(),
            default_wait_limit,
            watch_tracker,
        }
    }

    /// The tracker the list estimator reads; the stats poller and its pruner
    /// are run against it at startup.
    pub fn object_count_tracker(&self) -> Arc<ObjectCountTracker> {
        self.object_counts.clone()
    }

    #[cfg(test)]
    fn watch_count_for_test(&self, info: &RequestInfo) -> i64 {
        self.estimator.watch_count(info)
    }

    /// The `WatchTracker` the filter registers watches with.
    #[cfg(test)]
    pub fn watch_tracker(&self) -> &Arc<WatchTracker> {
        &self.watch_tracker
    }
}

/// `setResponseHeaders` (priority-and-fairness.go:341-352): the UIDs, not the
/// names, so cluster-admin-chosen names are not exposed.
fn set_response_headers(c: &Classification, resp: &mut Response) {
    for (name, value) in [
        (HEADER_PRIORITY_LEVEL_UID, &c.priority_level_uid),
        (HEADER_FLOW_SCHEMA_UID, &c.flow_schema_uid),
    ] {
        if let Ok(v) = HeaderValue::from_str(value) {
            resp.headers_mut().insert(
                HeaderName::from_bytes(name.to_ascii_lowercase().as_bytes())
                    .expect("static header name"),
                v,
            );
        }
    }
}

/// `tooManyRequests` (priority-and-fairness.go:354-358).
fn too_many_requests(retry_after: i64) -> Response {
    let mut r = (
        StatusCode::TOO_MANY_REQUESTS,
        "Too many requests, please try again later.\n",
    )
        .into_response();
    r.headers_mut().insert(
        RETRY_AFTER,
        HeaderValue::from_str(&retry_after.to_string()).expect("digits are a valid header"),
    );
    r
}

/// A response body that keeps a watch registered with the [`WatchTracker`]
/// until the body is dropped, i.e. until the watch is over.
fn forget_watch_with_body(resp: Response, forget: ForgetWatch) -> Response {
    let (parts, body) = resp.into_parts();
    let stream = body.into_data_stream().map(move |chunk| {
        // Moved into the closure so it lives exactly as long as the stream.
        let _registered = &forget;
        chunk
    });
    Response::from_parts(parts, Body::from_stream(stream))
}

/// The watch branch of `Handle` (priority-and-fairness.go:171-290), given the
/// seat `execute()` runs under.
///
/// Upstream runs `execute` (register the watch, then wait for the
/// initialization signal while holding the seat) on a goroutine beside the
/// handler. Here one task drives the handler future and, as soon as the
/// signal fires, frees the seat while the handler carries on. The deferred
/// `watchInitializationSignal.Signal()` (:186-190) is the handler returning
/// without ever signalling: the seat is released then. `forgetWatch` runs when
/// the watch is over, which is when the response body is dropped (or at once
/// if the handler produced no body worth tracking).
async fn serve_watch<S: Storage + 'static>(
    f: &ApfFilter<S>,
    attrs: &crate::audit::Attributes,
    req: Request,
    next: Next,
    permit: FlowControlPermit,
) -> Response {
    let info = RequestInfo {
        verb: attrs.verb.clone(),
        api_group: attrs.api_group.clone(),
        resource: attrs.resource.clone(),
        subresource: attrs.subresource.clone(),
        namespace: attrs.namespace.clone(),
        name: attrs.name.clone(),
    };
    // `forgetWatch = h.fcIfc.RegisterWatch(r)` (:211)
    let forget = f
        .watch_tracker
        .register_watch(&info, req.uri().query().unwrap_or(""));

    let signal = InitializationSignal::new();
    let handler = scope_signal(signal.clone(), next.run(req));
    tokio::pin!(handler);
    let mut permit = Some(permit);
    let resp = tokio::select! {
        resp = &mut handler => resp,
        // `watchInitializationSignal.Wait()` returned (:218): the request is
        // finished from the APF point of view.
        _ = signal.wait() => {
            permit.take();
            handler.await
        }
    };
    drop(permit);
    match forget {
        Some(forget) => forget_watch_with_body(resp, forget),
        None => resp,
    }
}

/// `priorityAndFairnessHandler.Handle` (priority-and-fairness.go:73-323).
pub async fn priority_and_fairness<S: Storage + 'static>(
    State(f): State<Arc<ApfFilter<S>>>,
    req: Request,
    next: Next,
) -> Response {
    let attrs = request_info(req.method(), req.uri().path(), req.uri().query());
    let user = req
        .extensions()
        .get::<rusternetes_middleware::AuthContext>()
        .map(|c| c.user.clone())
        .unwrap_or_else(rusternetes_common::auth::UserInfo::anonymous);

    let is_watch = attrs.verb == "watch";
    // "Skip tracking long running non-watch requests." (:90-95)
    if !is_watch
        && is_long_running(
            &attrs.verb,
            attrs.is_resource_request,
            &attrs.subresource,
            &attrs.path,
        )
    {
        return next.run(req).await;
    }

    let digest = RequestDigest {
        user_name: user.username.clone(),
        groups: user.groups.clone(),
        is_resource_request: attrs.is_resource_request,
        verb: attrs.verb.clone(),
        api_group: attrs.api_group.clone(),
        resource: attrs.resource.clone(),
        subresource: attrs.subresource.clone(),
        namespace: attrs.namespace.clone(),
        path: attrs.path.clone(),
    };
    let classification = f.engine.classify(&digest);

    // `estimateWork` runs after classification (:121-135).
    let estimate = f.estimator.estimate_work(
        Some(&RequestInfo {
            verb: attrs.verb.clone(),
            api_group: attrs.api_group.clone(),
            resource: attrs.resource.clone(),
            subresource: attrs.subresource.clone(),
            namespace: attrs.namespace.clone(),
            name: attrs.name.clone(),
        }),
        req.uri().query().unwrap_or(""),
        &classification.flow_schema,
        &classification.priority_level,
    );

    let wait = request_wait_limit(Instant::now(), None, None, f.default_wait_limit);
    match f
        .engine
        .execute_with_estimate(&classification, &estimate, wait)
        .await
    {
        Ok(permit) => {
            let mut resp = if is_watch {
                serve_watch(&f, &attrs, req, next, permit).await
            } else {
                // The seat is held while the handler runs and released on drop.
                let resp = next.run(req).await;
                drop(permit);
                resp
            };
            // `Handle`'s deferred `if idle { maybeReap(pl.Name) }`
            // (apf_filter.go:171-177); `maybe_reap` re-checks that the level
            // is quiescing and its queueset idle.
            f.engine.maybe_reap(&classification.priority_level);
            set_response_headers(&classification, &mut resp);
            resp
        }
        Err(_) => {
            // `!served` (:297-319): headers, then 429 with an adaptive Retry-After.
            f.dropped
                .record_dropped_request(&classification.priority_level);
            let mut resp =
                too_many_requests(f.dropped.get_retry_after(&classification.priority_level));
            set_response_headers(&classification, &mut resp);
            resp
        }
    }
}

// ---------------------------------------------------------------------
// Installation
// ---------------------------------------------------------------------

static FLOW_CONTROL: OnceLock<Arc<ApfFilter<StorageBackend>>> = OnceLock::new();

/// Install the process-wide filter, built once at startup when
/// `--enable-priority-and-fairness` is set. Without it the router leaves the
/// filter out, as `WithPriorityAndFairness` returns the handler untouched when
/// `fcIfc == nil` (priority-and-fairness.go:327-330).
pub fn install_flow_control(filter: Arc<ApfFilter<StorageBackend>>) {
    let _ = FLOW_CONTROL.set(filter);
}

/// The installed filter, if any.
pub fn installed() -> Option<Arc<ApfFilter<StorageBackend>>> {
    FLOW_CONTROL.get().cloned()
}

/// Keep the engine's configuration current with the stored FlowSchemas and
/// PriorityLevelConfigurations. Upstream reacts to informer events
/// (`apf_controller.go` `Run`); a short poll is the equivalent here. (#2808)
pub fn spawn_config_reloader<S: Storage + 'static>(
    engine: Arc<FlowControlEngine<S>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(e) = engine.initialize().await {
                tracing::warn!("APF configuration reload failed: {e}");
            }
            tokio::time::sleep(interval).await;
        }
    })
}

/// Adjust the priority levels' concurrency limits every
/// `borrowingAdjustmentPeriod` (apf_controller.go:386, `wait.Until(
/// cfgCtlr.updateBorrowing, borrowingAdjustmentPeriod, stopCh)`). (#2743)
pub fn spawn_borrowing_updater<S: Storage + 'static>(
    engine: Arc<FlowControlEngine<S>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(crate::flow_control::BORROWING_ADJUSTMENT_PERIOD).await;
            engine.update_borrowing();
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_control_work_estimator::Stats;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use axum::Router;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_storage::memory::MemoryStorage;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tower::ServiceExt;

    // ---- getRequestWaitContext (TestGetRequestWaitContext) ----

    #[test]
    fn wait_limit_one_fourth_of_remaining_deadline() {
        let now = Instant::now();
        let got = request_wait_limit(
            now,
            None,
            Some(now + Duration::from_secs(60)),
            Duration::ZERO,
        );
        assert_eq!(got, Duration::from_secs(15));
    }

    #[test]
    fn wait_limit_counts_from_received_timestamp() {
        let now = Instant::now();
        let received = now - Duration::from_secs(10);
        let got = request_wait_limit(
            now,
            Some(received),
            Some(now + Duration::from_secs(50)),
            Duration::ZERO,
        );
        assert_eq!(got, Duration::from_secs(5));
    }

    #[test]
    fn wait_limit_default_without_deadline() {
        let now = Instant::now();
        let d = Duration::from_secs(15);
        assert_eq!(request_wait_limit(now, None, None, d), d);
        assert_eq!(
            request_wait_limit(now, Some(now - Duration::from_secs(10)), None, d),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn wait_limit_never_exceeds_one_minute() {
        let now = Instant::now();
        assert_eq!(
            request_wait_limit(
                now,
                None,
                Some(now + Duration::from_secs(480)),
                Duration::ZERO
            ),
            Duration::from_secs(60)
        );
        assert_eq!(
            request_wait_limit(now, None, None, Duration::from_secs(120)),
            Duration::from_secs(60)
        );
    }

    // ---- TestDroppedRequestsTracker / ...PLIndependent ----

    fn fake_clock() -> (Arc<AtomicI64>, DroppedRequestsTracker) {
        let t = Arc::new(AtomicI64::new(1_000_000));
        let c = t.clone();
        (
            t,
            DroppedRequestsTracker::with_clock(move || c.load(Ordering::SeqCst)),
        )
    }

    #[test]
    fn dropped_requests_tracker_adapts_retry_after() {
        let (clock, tracker) = fake_clock();
        // (seconds elapsed, dropped requests, expected retryAfter)
        let steps = [
            (0, 5, 1),
            (1, 11, 2),
            (2, 1, 2),
            (3, 11, 4),
            (4, 1, 4),
            (7, 1, 8),
            (11, 1, 8),
            (15, 1, 7),
            (17, 1, 6),
            (21, 14, 5),
            (22, 1, 10),
        ];
        let mut prev = 0;
        for (elapsed, dropped, want) in steps {
            clock.fetch_add(elapsed - prev, Ordering::SeqCst);
            prev = elapsed;
            for _ in 0..dropped {
                tracker.record_dropped_request("pl");
            }
            assert_eq!(tracker.get_retry_after("pl"), want, "at +{elapsed}s");
        }
    }

    #[test]
    fn dropped_requests_tracker_is_per_priority_level() {
        let (clock, tracker) = fake_clock();
        for i in 0..10 {
            tracker.record_dropped_request(&format!("pl-{i}"));
        }
        clock.fetch_add(1, Ordering::SeqCst);
        for i in 0..10 {
            let pl = format!("pl-{i}");
            tracker.record_dropped_request(&pl);
            assert_eq!(tracker.get_retry_after(&pl), 1);
        }
        for _ in 0..5 {
            tracker.record_dropped_request("pl-0");
        }
        clock.fetch_add(1, Ordering::SeqCst);
        for i in 0..10 {
            let pl = format!("pl-{i}");
            tracker.record_dropped_request(&pl);
            assert_eq!(tracker.get_retry_after(&pl), if i == 0 { 2 } else { 1 });
        }
        assert_eq!(tracker.get_retry_after("other-pl"), 1);
    }

    // ---- long-running check ----

    #[test]
    fn long_running_matches_kube_apiserver_sets() {
        assert!(is_long_running("watch", true, "", ""));
        assert!(is_long_running("proxy", true, "", ""));
        assert!(is_long_running("get", true, "exec", ""));
        assert!(is_long_running("get", true, "log", ""));
        assert!(is_long_running("get", false, "", "/debug/pprof/heap"));
        assert!(!is_long_running("get", true, "status", ""));
        assert!(!is_long_running("list", true, "", ""));
        // a subresource only counts for resource requests
        assert!(!is_long_running("get", false, "exec", "/x"));
    }

    // ---- the filter (TestApf*) ----

    /// `limit` seats in total (`catch-all` shares everything), handler runs
    /// `hook` while holding its seat.
    async fn app(
        limit: i64,
        wait: Duration,
        hook: Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync>,
        user: UserInfo,
    ) -> Router {
        let engine = Arc::new(FlowControlEngine::with_limits(
            Arc::new(MemoryStorage::new()),
            limit,
            0,
        ));
        engine.initialize().await.unwrap();
        let filter = Arc::new(ApfFilter::new(engine, wait));
        let ctx = rusternetes_middleware::AuthContext { user };
        Router::new()
            .route(
                "/api/v1/namespaces/default",
                get(move || {
                    let hook = hook.clone();
                    async move {
                        if let Some(n) = hook() {
                            n.notified().await;
                        }
                        "ok"
                    }
                }),
            )
            .route(
                "/api/v1/namespaces/default/pods/p/exec",
                get(|| async { "x" }),
            )
            .route("/api/v1/watch/namespaces", get(|| async { "w" }))
            .layer(from_fn_with_state(
                filter,
                priority_and_fairness::<MemoryStorage>,
            ))
            .layer(axum::middleware::from_fn(
                move |mut req: axum::extract::Request, next: Next| {
                    let ctx = ctx.clone();
                    async move {
                        req.extensions_mut().insert(ctx);
                        next.run(req).await
                    }
                },
            ))
    }

    fn get_req(path: &str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .uri(path)
            .body(Body::empty())
            .unwrap()
    }

    fn none_hook() -> Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync> {
        Arc::new(|| None)
    }

    fn bob() -> UserInfo {
        UserInfo {
            username: "bob".into(),
            uid: String::new(),
            groups: vec!["system:authenticated".into()],
            extra: Default::default(),
        }
    }

    fn masters() -> UserInfo {
        UserInfo {
            username: "admin".into(),
            uid: String::new(),
            groups: vec!["system:masters".into()],
            extra: Default::default(),
        }
    }

    #[tokio::test]
    async fn executes_a_request_and_sets_uid_headers() {
        let app = app(10, Duration::from_millis(50), none_hook(), bob()).await;
        let resp = app
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().contains_key(HEADER_PRIORITY_LEVEL_UID));
        assert!(resp.headers().contains_key(HEADER_FLOW_SCHEMA_UID));
    }

    #[tokio::test]
    async fn rejects_with_429_and_retry_after_when_seats_are_exhausted() {
        // One seat on `catch-all` (a Reject level): a second concurrent
        // request is a 429 (TestApfRejectRequest).
        let gate = Arc::new(Notify::new());
        let first = Arc::new(AtomicUsize::new(0));
        let (g, f) = (gate.clone(), first.clone());
        let hook: Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync> =
            Arc::new(move || (f.fetch_add(1, Ordering::SeqCst) == 0).then(|| g.clone()));
        let app = app(1, Duration::from_millis(50), hook, bob()).await;
        let a = app.clone();
        let held =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/namespaces/default")).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers().get(RETRY_AFTER).unwrap(), "1");
        assert!(resp.headers().contains_key(HEADER_PRIORITY_LEVEL_UID));

        gate.notify_one();
        assert_eq!(held.await.unwrap().unwrap().status(), StatusCode::OK);
        // The seat is free again once the first request finished.
        assert_eq!(
            app.oneshot(get_req("/api/v1/namespaces/default"))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn system_masters_is_exempt_from_the_limit() {
        // TestApfExemptRequest: the `exempt` level never rejects, even with
        // every seat of the server taken.
        let gate = Arc::new(Notify::new());
        let first = Arc::new(AtomicUsize::new(0));
        let (g, f) = (gate.clone(), first.clone());
        let hook: Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync> =
            Arc::new(move || (f.fetch_add(1, Ordering::SeqCst) < 3).then(|| g.clone()));
        let app = app(1, Duration::from_millis(50), hook, masters()).await;
        let mut tasks = vec![];
        for _ in 0..3 {
            let a = app.clone();
            tasks.push(tokio::spawn(async move {
                a.oneshot(get_req("/api/v1/namespaces/default")).await
            }));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        // all three are executing concurrently: a fourth is also served
        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        gate.notify_waiters();
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap().status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn long_running_requests_skip_the_filter() {
        // TestApfSkipLongRunningRequest: an `exec` is never classified, so it is
        // served with no UID headers even though every seat is taken.
        let gate = Arc::new(Notify::new());
        let first = Arc::new(AtomicUsize::new(0));
        let (g, f) = (gate.clone(), first.clone());
        let hook: Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync> =
            Arc::new(move || (f.fetch_add(1, Ordering::SeqCst) == 0).then(|| g.clone()));
        let app = app(1, Duration::from_millis(50), hook, bob()).await;
        let a = app.clone();
        let held =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/namespaces/default")).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default/pods/p/exec"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(!resp.headers().contains_key(HEADER_PRIORITY_LEVEL_UID));
        gate.notify_one();
        let _ = held.await;
    }

    #[tokio::test]
    async fn a_watch_is_classified_not_skipped() {
        // Unlike other long-running requests a watch goes through APF
        // (priority-and-fairness.go:90, `&& !isWatchRequest`).
        let app = app(10, Duration::from_millis(50), none_hook(), bob()).await;
        let resp = app
            .oneshot(get_req("/api/v1/watch/namespaces"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().contains_key(HEADER_PRIORITY_LEVEL_UID));
    }

    #[tokio::test]
    async fn queued_request_that_waits_too_long_is_a_429() {
        // TestApfCancelWaitRequest, against a level that queues. The filter is
        // built straight on an engine with a `Queue` level.
        use rusternetes_common::resources::flowcontrol::{
            LimitResponse, LimitResponseType, LimitedPriorityLevelConfiguration,
            PriorityLevelConfiguration, QueuingConfiguration,
        };
        use rusternetes_common::types::ObjectMeta;
        use rusternetes_common::validation::flowcontrol_bootstrap::mandatory_priority_level_configuration;
        use rusternetes_storage::build_key;

        let mut pl: PriorityLevelConfiguration =
            mandatory_priority_level_configuration("catch-all").unwrap();
        pl.metadata = ObjectMeta::new("catch-all");
        pl.spec.limited = Some(LimitedPriorityLevelConfiguration {
            nominal_concurrency_shares: Some(5),
            lending_concurrency_limit: None,
            lendable_percent: None,
            borrowing_limit_percent: None,
            limit_response: Some(LimitResponse {
                type_: LimitResponseType::Queue,
                queuing: Some(QueuingConfiguration {
                    queues: 1,
                    hand_size: 1,
                    queue_length_limit: 10,
                }),
            }),
        });
        let st = Arc::new(MemoryStorage::new());
        st.create(
            &build_key("prioritylevelconfigurations", None, "catch-all"),
            &pl,
        )
        .await
        .unwrap();
        let engine = Arc::new(FlowControlEngine::with_limits(st, 1, 0));
        engine.initialize().await.unwrap();
        let filter = Arc::new(ApfFilter::new(engine, Duration::from_millis(80)));

        let gate = Arc::new(Notify::new());
        let g = gate.clone();
        let seen = Arc::new(AtomicUsize::new(0));
        let s = seen.clone();
        let router = Router::new()
            .route(
                "/api/v1/namespaces/default",
                get(move || {
                    let (g, s) = (g.clone(), s.clone());
                    async move {
                        if s.fetch_add(1, Ordering::SeqCst) == 0 {
                            g.notified().await;
                        }
                        "ok"
                    }
                }),
            )
            .layer(from_fn_with_state(
                filter,
                priority_and_fairness::<MemoryStorage>,
            ));
        let a = router.clone();
        let held =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/namespaces/default")).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let started = Instant::now();
        let resp = router
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            started.elapsed() >= Duration::from_millis(70),
            "it queued first"
        );
        gate.notify_one();
        let _ = held.await;
    }

    #[tokio::test]
    async fn finishing_the_last_request_reaps_a_quiescing_level() {
        // #2803: a removed level that is still busy quiesces; when its last
        // request finishes the filter reaps it (apf_filter.go:171-177).
        use rusternetes_common::resources::flowcontrol::{
            LimitResponse, LimitResponseType, LimitedPriorityLevelConfiguration,
            PriorityLevelConfiguration, QueuingConfiguration,
        };
        use rusternetes_common::types::ObjectMeta;
        use rusternetes_common::validation::flowcontrol_bootstrap::{
            mandatory_flow_schema, mandatory_priority_level_configuration,
        };
        use rusternetes_storage::build_key;

        let mut pl: PriorityLevelConfiguration =
            mandatory_priority_level_configuration("catch-all").unwrap();
        pl.metadata = ObjectMeta::new("a");
        pl.spec.limited = Some(LimitedPriorityLevelConfiguration {
            nominal_concurrency_shares: Some(5),
            lending_concurrency_limit: None,
            lendable_percent: None,
            borrowing_limit_percent: None,
            limit_response: Some(LimitResponse {
                type_: LimitResponseType::Queue,
                queuing: Some(QueuingConfiguration {
                    queues: 4,
                    hand_size: 2,
                    queue_length_limit: 10,
                }),
            }),
        });
        let mut fs = mandatory_flow_schema("catch-all").unwrap();
        fs.metadata = ObjectMeta::new("to-a");
        fs.spec.matching_precedence = 1;
        fs.spec.priority_level_configuration.name = "a".into();
        let st = Arc::new(MemoryStorage::new());
        let pl_key = build_key("prioritylevelconfigurations", None, "a");
        st.create(&pl_key, &pl).await.unwrap();
        st.create(&build_key("flowschemas", None, "to-a"), &fs)
            .await
            .unwrap();
        let engine = Arc::new(FlowControlEngine::with_limits(st.clone(), 100, 0));
        engine.initialize().await.unwrap();
        assert_eq!(
            engine
                .classify(&RequestDigest {
                    user_name: "bob".into(),
                    groups: vec!["system:authenticated".into()],
                    is_resource_request: true,
                    verb: "get".into(),
                    resource: "namespaces".into(),
                    ..Default::default()
                })
                .priority_level,
            "a"
        );
        let filter = Arc::new(ApfFilter::new(engine.clone(), Duration::from_millis(50)));

        let gate = Arc::new(Notify::new());
        let g = gate.clone();
        let ctx = rusternetes_middleware::AuthContext { user: bob() };
        let router = Router::new()
            .route(
                "/api/v1/namespaces/default",
                get(move || {
                    let g = g.clone();
                    async move {
                        g.notified().await;
                        "ok"
                    }
                }),
            )
            .layer(from_fn_with_state(
                filter,
                priority_and_fairness::<MemoryStorage>,
            ))
            .layer(axum::middleware::from_fn(
                move |mut req: axum::extract::Request, next: Next| {
                    let ctx = ctx.clone();
                    async move {
                        req.extensions_mut().insert(ctx);
                        next.run(req).await
                    }
                },
            ));
        let held =
            tokio::spawn(
                async move { router.oneshot(get_req("/api/v1/namespaces/default")).await },
            );
        tokio::time::sleep(Duration::from_millis(50)).await;
        st.delete(&pl_key).await.unwrap();
        engine.initialize().await.unwrap();
        assert!(engine.is_quiescing("a"));
        gate.notify_one();
        assert_eq!(held.await.unwrap().unwrap().status(), StatusCode::OK);
        assert!(engine.queueset_for("a").is_none(), "reaped on finish");
    }

    /// The estimator reads the filter's own tracker: a polled, large `pods`
    /// resource makes a list cost more than the minimum, an unpolled one does
    /// not (`ObjectCountNotFoundErr` -> minimum seats).
    #[tokio::test]
    async fn list_cost_follows_the_polled_object_counts() {
        let st = Arc::new(MemoryStorage::new());
        let engine = Arc::new(FlowControlEngine::with_limits(st.clone(), 100, 0));
        engine.initialize().await.unwrap();
        let filter = ApfFilter::new(engine.clone(), Duration::from_secs(1));
        let c = engine.classify(&RequestDigest {
            user_name: "bob".into(),
            groups: vec!["system:authenticated".into()],
            is_resource_request: true,
            verb: "list".into(),
            resource: "pods".into(),
            ..Default::default()
        });
        let info = RequestInfo {
            verb: "list".into(),
            resource: "pods".into(),
            ..Default::default()
        };
        let cost = |f: &ApfFilter<MemoryStorage>| {
            f.estimator
                .estimate_work(Some(&info), "", &c.flow_schema, &c.priority_level)
                .initial_seats
        };
        assert_eq!(cost(&filter), 1, "unpolled resource costs the minimum");
        filter.object_count_tracker().set(
            "pods",
            Stats {
                object_count: 50_000,
                estimated_average_object_size_bytes: 10_000,
            },
        );
        assert!(cost(&filter) > 1, "a large polled resource costs more");
    }

    // ---- watch initialization + RegisterWatch (priority-and-fairness.go:171-290) ----

    /// A router whose watch route optionally signals initialization, then
    /// keeps the handler alive until `gate` fires (a watch that has sent its
    /// initial events and is still being served).
    async fn watch_app(
        gate: Arc<Notify>,
        initialized: bool,
    ) -> (Router, Arc<ApfFilter<MemoryStorage>>) {
        let engine = Arc::new(FlowControlEngine::with_limits(
            Arc::new(MemoryStorage::new()),
            1,
            0,
        ));
        engine.initialize().await.unwrap();
        let filter = Arc::new(ApfFilter::new(engine, Duration::from_millis(50)));
        let ctx = rusternetes_middleware::AuthContext { user: bob() };
        let router = Router::new()
            .route(
                "/api/v1/watch/namespaces",
                get(move || {
                    let gate = gate.clone();
                    async move {
                        if initialized {
                            crate::flow_control_watch_tracker::watch_initialized();
                        }
                        gate.notified().await;
                        "w"
                    }
                }),
            )
            .route("/api/v1/namespaces/default", get(|| async { "ok" }))
            .layer(from_fn_with_state(
                filter.clone(),
                priority_and_fairness::<MemoryStorage>,
            ))
            .layer(axum::middleware::from_fn(
                move |mut req: axum::extract::Request, next: Next| {
                    let ctx = ctx.clone();
                    async move {
                        req.extensions_mut().insert(ctx);
                        next.run(req).await
                    }
                },
            ));
        (router, filter)
    }

    fn create_namespace_info() -> RequestInfo {
        RequestInfo {
            verb: "create".into(),
            resource: "namespaces".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_watch_releases_its_seat_when_initialized_not_when_it_returns() {
        // The seat is held "until the request is finished from the APF point
        // of view (which is when its initialization is done)" (:217-218).
        let gate = Arc::new(Notify::new());
        let (app, _) = watch_app(gate.clone(), true).await;
        let a = app.clone();
        let watch =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/watch/namespaces")).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        // One seat on `catch-all`, and the watch is still being served: a
        // second request is admitted because the watch finished initializing.
        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        gate.notify_one();
        let _ = watch.await;
    }

    #[tokio::test]
    async fn an_uninitialized_watch_keeps_its_seat_until_the_handler_returns() {
        // The deferred `watchInitializationSignal.Signal()` (:186-190) only
        // fires once the handler is done, so until then the seat is held.
        let gate = Arc::new(Notify::new());
        let (app, _) = watch_app(gate.clone(), false).await;
        let a = app.clone();
        let watch =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/watch/namespaces")).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        gate.notify_one();
        let _ = watch.await;
    }

    #[tokio::test]
    async fn a_watch_is_registered_until_its_response_is_dropped() {
        // `forgetWatch` runs when the watch is over (:196-198), not when the
        // response head is ready.
        let gate = Arc::new(Notify::new());
        gate.notify_one(); // let the handler return its response right away
        let (app, filter) = watch_app(gate, true).await;
        let create = create_namespace_info();
        assert_eq!(
            filter
                .watch_tracker()
                .get_interested_watch_count(Some(&create)),
            0
        );
        let resp = app
            .oneshot(get_req("/api/v1/watch/namespaces"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            filter
                .watch_tracker()
                .get_interested_watch_count(Some(&create)),
            1,
            "registered while the body is alive"
        );
        drop(resp);
        assert_eq!(
            filter
                .watch_tracker()
                .get_interested_watch_count(Some(&create)),
            0,
            "forgotten when the watch ends"
        );
    }

    #[tokio::test]
    async fn the_estimator_sees_the_tracked_watches() {
        // #2775: the work estimator's watch count getter reads the tracker.
        let gate = Arc::new(Notify::new());
        gate.notify_one();
        let (app, filter) = watch_app(gate, true).await;
        let resp = app
            .oneshot(get_req("/api/v1/watch/namespaces"))
            .await
            .unwrap();
        let info = create_namespace_info();
        assert_eq!(filter.watch_count_for_test(&info), 1);
        drop(resp);
        assert_eq!(filter.watch_count_for_test(&info), 0);
    }
}
