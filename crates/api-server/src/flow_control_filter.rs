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
//! - Metrics: the counters, queue/executing gauges and wait/execution/seat
//!   histograms are ported (`flow_control_metrics`, #2809), and so are the
//!   timing-ratio histograms (seat/request utilization, read-vs-write) and the
//!   limit/demand gauges (#2960), `demand_seats`, the queueset R/S gauges and
//!   counters, and `watch_count_samples` (#2987). Not ported:
//!   `RecordDroppedRequest`/`RecordRequestTermination` (see the module doc of
//!   `flow_control_metrics`).
//! - `getRequestWaitContext` takes 1/4 of the deadline `WithRequestDeadline`
//!   put on the request (`request_timeout::RequestDeadline`, incl. a user
//!   `?timeout=`); long-running requests carry none and get the default limit,
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
use futures::{FutureExt, StreamExt};
use rusternetes_storage::{Storage, StorageBackend};

use crate::apiserver_request_metrics::{self, RequestMeta};
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

/// `epmetrics.APIServerComponent` (endpoints/metrics/metrics.go:53).
const COMPONENT: &str = "apiserver";

/// `nonMutatingRequestVerbs` (server/filters/maxinflight.go:46).
const NON_MUTATING_REQUEST_VERBS: &[&str] = &["get", "list", "watch"];

/// `truncateLogField` (priority-and-fairness.go:61-68).
pub fn truncate_log_field(s: &str) -> &str {
    const MAX_FIELD_LOG_LENGTH: usize = 64;
    if s.len() > MAX_FIELD_LOG_LENGTH {
        // Back off to a char boundary (Go slices bytes; Rust must not split a char).
        let mut end = MAX_FIELD_LOG_LENGTH;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        return &s[..end];
    }
    s
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
    pub engine: Arc<FlowControlEngine<S>>,
    pub estimator: WorkEstimator,
    /// `StorageObjectCountTracker` (config.go:472); fed by
    /// `flow_control_stats_poller`, read by the list estimator.
    pub object_counts: Arc<ObjectCountTracker>,
    pub dropped: DroppedRequestsTracker,
    pub default_wait_limit: Duration,
    pub watch_tracker: Arc<WatchTracker>,
}

impl<S: Storage + 'static> ApfFilter<S> {
    /// `defaultRequestWaitLimit` is `RequestTimeout/4` (config.go:1027).
    pub fn new(engine: Arc<FlowControlEngine<S>>, default_wait_limit: Duration) -> Self {
        // `fcmetrics.Register()`.
        crate::flow_control_metrics::register_all();
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

    pub fn watch_count_for_test(&self, info: &RequestInfo) -> i64 {
        self.estimator.watch_count(info)
    }

    /// The `WatchTracker` the filter registers watches with.
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

    // `startedAt := time.Now()` (priority-and-fairness.go:201) in `execute`.
    let started_at = Instant::now();
    let signal = InitializationSignal::new();
    let handler = scope_signal(signal.clone(), next.run(req));
    tokio::pin!(handler);
    let mut permit = permit;
    permit.mark_watch();
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
    // `defer httplog.AddKeyValue(ctx, "apf_init_latency", time.Since(startedAt))`
    // (:203-205).
    tracing::debug!(apf_init_latency = ?started_at.elapsed(), "APF watch initialized");
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
    // `ctx.Deadline()` / `ReceivedTimestampFrom(ctx)`, set by
    // `WithRequestDeadline` (request_timeout.rs); absent for long-running
    // requests.
    let request_deadline = req
        .extensions()
        .get::<crate::request_timeout::RequestDeadline>()
        .copied();
    let method = req.method().clone();
    let query = req.uri().query().unwrap_or("").to_string();
    let apply_patch = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes() == b"application/apply-patch+yaml");
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

    // `ObserveWorkEstimatedSeats` (priority-and-fairness.go:134).
    crate::flow_control_metrics::observe_work_estimated_seats(
        &classification.priority_level,
        &classification.flow_schema,
        estimate.max_seats(),
    );

    // `httplog.AddKeyValue(ctx, "apf_pl"/"apf_fs"/"apf_iseats"/"apf_fseats"/
    // "apf_additionalLatency", ...)` (priority-and-fairness.go:119-120,
    // :135-137). There is no httplog request line to attach them to, so they
    // are one structured event carrying the same keys.
    tracing::debug!(
        apf_pl = truncate_log_field(&classification.priority_level),
        apf_fs = truncate_log_field(&classification.flow_schema),
        apf_iseats = estimate.initial_seats,
        apf_fseats = estimate.final_seats,
        apf_additional_latency = ?estimate.additional_latency,
        "APF classified request"
    );

    let meta = RequestMeta {
        method: method.as_str().to_string(),
        query,
        path: attrs.path.clone(),
        apply_patch,
        is_resource_request: attrs.is_resource_request,
        verb: attrs.verb.clone(),
        namespace: attrs.namespace.clone(),
        name: attrs.name.clone(),
        api_group: attrs.api_group.clone(),
        api_version: attrs.api_version.clone(),
        resource: attrs.resource.clone(),
        subresource: attrs.subresource.clone(),
        ..Default::default()
    };
    let is_mutating = !NON_MUTATING_REQUEST_VERBS.contains(&attrs.verb.as_str());

    let wait = request_wait_limit(
        Instant::now(),
        request_deadline.map(|d| d.received_at),
        request_deadline.map(|d| d.deadline),
        f.default_wait_limit,
    );
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
            // `MonitorRequest` runs in `apiserver_request_metrics::instrument_request`,
            // which wraps every route (InstrumentRouteFunc), not only admitted ones.
            set_response_headers(&classification, &mut resp);
            resp
        }
        Err(_) => {
            // `!served` (:297-319): headers, then 429 with an adaptive Retry-After.
            apiserver_request_metrics::record_dropped_request(&meta, COMPONENT, is_mutating);
            apiserver_request_metrics::record_request_termination(
                &meta,
                COMPONENT,
                StatusCode::TOO_MANY_REQUESTS.as_u16(),
            );
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

/// `configQueue`'s rate limiter (apf_controller.go:295-298):
/// `NewTypedItemExponentialFailureRateLimiter(200ms, 8h)`. "Start with
/// longish delay because conflicts will be between different processes".
const CONFIG_QUEUE_BASE_DELAY: Duration = Duration::from_millis(200);
const CONFIG_QUEUE_MAX_DELAY: Duration = Duration::from_secs(8 * 3600);
/// The single queue item (`configQueue.Add(0)`).
const CONFIG_QUEUE_KEY: &str = "config_consumer";

/// What the informer event handlers (apf_controller.go:308-368) compare
/// between the old and new object: a PriorityLevelConfiguration's spec
/// (:314-322); a FlowSchema's spec and status (:337-358).
fn relevant_part(is_flow_schema: bool, value: &serde_json::Value) -> serde_json::Value {
    let get = |k: &str| value.get(k).cloned().unwrap_or(serde_json::Value::Null);
    if is_flow_schema {
        serde_json::json!([get("spec"), get("status")])
    } else {
        get("spec")
    }
}

/// The informer store's role for the handlers' `UpdateFunc(old, new)`: the last
/// seen relevant part of every object, so a modification that changes neither
/// spec (nor, for a FlowSchema, status) does not trigger a reload.
#[derive(Default)]
pub struct ConfigEventFilter {
    seen: HashMap<String, serde_json::Value>,
}

impl ConfigEventFilter {
    /// Whether `event` should `configQueue.Add(0)`: AddFunc and DeleteFunc
    /// always do; UpdateFunc only when the relevant part changed (an object
    /// never seen before counts as changed).
    pub fn triggers(
        &mut self,
        is_flow_schema: bool,
        event: &rusternetes_storage::WatchEvent,
    ) -> bool {
        use rusternetes_storage::WatchEvent::{Added, Deleted, Modified};
        match event {
            Added(k, v) | Modified(k, v) => {
                let new = serde_json::from_str::<serde_json::Value>(v)
                    .map(|v| relevant_part(is_flow_schema, &v))
                    .unwrap_or(serde_json::Value::Null);
                let old = self.seen.insert(k.clone(), new.clone());
                match (event, old) {
                    (Modified(..), Some(old)) => old != new,
                    _ => true,
                }
            }
            Deleted(k, _) => {
                self.seen.remove(k);
                true
            }
        }
    }
}

/// Drive the engine's configuration from watch events rather than a poll: the
/// port of `configController.Run`'s worker (apf_controller.go:371-391) and
/// `processNextWorkItem` (:508-529) over the informers' event handlers
/// (:308-368). (#2811, was #2808's 2s poll)
///
/// Mechanism, as upstream: every relevant FlowSchema or
/// PriorityLevelConfiguration event adds the one item to `configQueue`
/// (deduplicated, so a burst of events costs one sync); the worker runs
/// `syncOne`, and on error requeues with the exponential-failure rate limiter
/// (`AddRateLimited`, 200ms..8h) or `Forget`s the failures on success. The
/// watches are opened *before* the initial sync is enqueued (the informers'
/// list-then-watch, `WaitForCacheSync`), and a watch that breaks is reopened
/// with a fresh enqueue, standing in for the reflector's relist.
///
/// DEVIATION: upstream's informers resync every 10 minutes
/// (pkg/controlplane/apiserver/config.go:159), but the handlers' `UpdateFunc`
/// drops a resync (old == new), so it never reloads anything; there is
/// therefore no periodic wake-up here and an idle cluster does not poll.
/// `specificDelay` (:521-522) requeues after that delay without recording a
/// failure.
pub fn spawn_config_reloader<S: Storage + 'static>(
    engine: Arc<FlowControlEngine<S>>,
) -> tokio::task::JoinHandle<()> {
    let storage = engine.storage();
    tokio::spawn(run_config_worker(
        move || {
            let engine = engine.clone();
            async move { engine.sync_one().await.map_err(|e| e.to_string()) }
        },
        move |prefix: String| {
            let storage = storage.clone();
            async move { storage.watch(&prefix).await.ok() }
        },
    ))
}

/// The worker loop, with the sync and the watch source injected so the queue
/// semantics are testable. `watch(prefix)` returns `None` when a watch cannot
/// be opened; it is retried with the same backoff.
pub async fn run_config_worker<Sync, SyncFut, Watch, WatchFut>(mut sync: Sync, mut watch: Watch)
where
    Sync: FnMut() -> SyncFut,
    SyncFut: std::future::Future<Output = Result<Duration, String>>,
    Watch: FnMut(String) -> WatchFut,
    WatchFut: std::future::Future<Output = Option<rusternetes_storage::WatchStream>>,
{
    use crate::registry::apiextensions::customresourcedefinition::ItemExponentialFailureRateLimiter;
    use rusternetes_storage::build_key;

    let fs_prefix = build_key("flowschemas", None, "");
    let pl_prefix = build_key("prioritylevelconfigurations", None, "");
    let mut limiter =
        ItemExponentialFailureRateLimiter::new(CONFIG_QUEUE_BASE_DELAY, CONFIG_QUEUE_MAX_DELAY);
    let mut filter = ConfigEventFilter::default();
    let mut fs_watch: Option<rusternetes_storage::WatchStream> = None;
    let mut pl_watch: Option<rusternetes_storage::WatchStream> = None;
    let mut retry_at: Option<tokio::time::Instant> = None;
    // The informers' initial list delivers an Add for every object.
    let mut queued = true;
    loop {
        // (Re)open broken watches before syncing, so no event is missed
        // between the list and the watch; a reopen is a relist.
        for (slot, prefix) in [(&mut fs_watch, &fs_prefix), (&mut pl_watch, &pl_prefix)] {
            if slot.is_none() {
                *slot = watch(prefix.clone()).await;
                if slot.is_some() {
                    queued = true;
                } else {
                    retry_at.get_or_insert(
                        tokio::time::Instant::now() + limiter.when(CONFIG_QUEUE_KEY),
                    );
                }
            }
        }
        if queued {
            queued = false;
            match sync().await {
                // `case specificDelay > 0: AddAfter(obj, specificDelay)`
                // (:521-522): requeue after the delay, failures not forgotten.
                Ok(delay) if !delay.is_zero() => {
                    retry_at = Some(tokio::time::Instant::now() + delay);
                }
                Ok(_) => {
                    limiter.forget(CONFIG_QUEUE_KEY);
                    // A watch still down must be retried even with nothing
                    // queued, or the worker would sleep forever.
                    retry_at = (fs_watch.is_none() || pl_watch.is_none())
                        .then(|| tokio::time::Instant::now() + limiter.when(CONFIG_QUEUE_KEY));
                }
                Err(e) => {
                    tracing::warn!("APF configuration reload failed: {e}");
                    retry_at = Some(tokio::time::Instant::now() + limiter.when(CONFIG_QUEUE_KEY));
                }
            }
        }
        let timer = async {
            match retry_at {
                Some(t) => tokio::time::sleep_until(t).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = timer => { retry_at = None; queued = true; }
            ev = next_event(&mut fs_watch) => match ev {
                Some(Ok(ev)) => queued |= filter.triggers(true, &ev),
                Some(Err(_)) | None => fs_watch = None,
            },
            ev = next_event(&mut pl_watch) => match ev {
                Some(Ok(ev)) => queued |= filter.triggers(false, &ev),
                Some(Err(_)) | None => pl_watch = None,
            },
        }
        // The queue holds one deduplicated item: absorb every event already
        // delivered so a burst costs a single sync.
        for (slot, is_fs) in [(&mut fs_watch, true), (&mut pl_watch, false)] {
            while let Some(ev) = next_event(slot).now_or_never() {
                match ev {
                    Some(Ok(ev)) => queued |= filter.triggers(is_fs, &ev),
                    Some(Err(_)) | None => {
                        *slot = None;
                        break;
                    }
                }
            }
        }
    }
}

async fn next_event(
    w: &mut Option<rusternetes_storage::WatchStream>,
) -> Option<rusternetes_common::Result<rusternetes_storage::WatchEvent>> {
    match w {
        Some(w) => w.next().await,
        None => std::future::pending().await,
    }
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

/// `priorityAndFairnessConfigConsumerHookName` (server/config.go:921).
pub const APF_CONFIG_CONSUMER_HOOK: &str = "priority-and-fairness-config-consumer";
/// `priorityAndFairnessFilterHookName` (server/config.go:938).
pub const APF_FILTER_HOOK: &str = "priority-and-fairness-filter";
/// `storageObjectCountTrackerHookName` (server/config.go:963).
pub const STORAGE_OBJECT_COUNT_TRACKER_HOOK: &str = "storage-object-count-tracker-hook";

/// The three hooks `GenericAPIServer.New` adds when API Priority and Fairness
/// is on (server/config.go:921-970), as `AddPostStartHook` + the shared
/// `runPostStartHook` runner: each only starts background work and returns
/// nil, so each finishes at once and can never be fatal.
///
/// - `priority-and-fairness-config-consumer` (:921-934): `go c.FlowControl.Run(stop)`,
///   i.e. the config worker (`apf_controller.go:371-391` `Run`: config worker
///   + `updateBorrowing` every `borrowingAdjustmentPeriod`);
/// - `priority-and-fairness-filter` (:937-947):
///   `StartPriorityAndFairnessWatermarkMaintenance` (priority-and-fairness.go:360);
/// - `storage-object-count-tracker-hook` (:961-970):
///   `go c.StorageObjectCountTracker.RunUntil(stop)`.
///
/// Upstream also registers `max-in-flight-filter` (:949-958) when APF is off;
/// the max-in-flight hook lives in `post_start_hooks::register_generic_server_hooks` (#3664);
/// the max-in-flight filter itself is still absent.
pub fn register_post_start_hooks<S: Storage + 'static>(
    registry: &crate::post_start_hooks::PostStartHooks,
    engine: Arc<FlowControlEngine<S>>,
    tracker: Arc<ObjectCountTracker>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    use crate::post_start_hooks::spawn_starting_hook_with;
    spawn_starting_hook_with(registry, APF_CONFIG_CONSUMER_HOOK, move || {
        spawn_config_reloader(engine.clone());
        spawn_borrowing_updater(engine);
    });
    spawn_starting_hook_with(registry, APF_FILTER_HOOK, || {
        apiserver_request_metrics::start_watermark_maintenance();
    });
    register_storage_object_count_tracker_hook(registry, tracker, stop);
}

/// The hook with APF off: a fresh tracker (`NewConfig` sets
/// `StorageObjectCountTracker = flowcontrolrequest.NewStorageObjectCountTracker()`,
/// config.go:472) whose stop channel never fires for the process lifetime.
pub fn register_object_count_tracker_hook_without_apf(
    registry: &crate::post_start_hooks::PostStartHooks,
) {
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    std::mem::forget(stop_tx);
    register_storage_object_count_tracker_hook(
        registry,
        Arc::new(ObjectCountTracker::new()),
        stop_rx,
    );
}

/// `storage-object-count-tracker-hook` (server/config.go:961-970). Upstream
/// registers it whenever `c.StorageObjectCountTracker != nil`, which
/// `NewConfig` always sets (config.go:472), independent of APF vs
/// max-in-flight -- so callers register it with APF off too
/// (server/config_test.go lists it beside `max-in-flight-filter`).
pub fn register_storage_object_count_tracker_hook(
    registry: &crate::post_start_hooks::PostStartHooks,
    tracker: Arc<ObjectCountTracker>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    crate::post_start_hooks::spawn_starting_hook_with(
        registry,
        STORAGE_OBJECT_COUNT_TRACKER_HOOK,
        move || {
            tokio::spawn(async move { tracker.run_until(stop).await });
        },
    );
}
