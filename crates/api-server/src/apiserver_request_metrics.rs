//! `apiserver_*` request metrics used by the APF filter.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/endpoints/metrics/metrics.go`
//! (release-1.35): `requestCounter` (:78-86), `currentInflightRequests`
//! (:193-201), `currentInqueueRequests` (:202-210), `requestTerminationsTotal`
//! (:212-220), `UpdateInflightRequestMetrics` (:465-476), `RecordDroppedRequest`
//! (:508-526) and `RecordRequestTermination` (:532-549); and of
//! `server/filters/maxinflight.go`'s `requestWatermark` (:56-84) and
//! `startWatermarkMaintenance` (:92-104).
//!
//! General instrumentation: [`instrument_request`] is the middleware
//! counterpart of `InstrumentRouteFunc` (metrics.go:633-650); it wraps every
//! protected route (so long-running non-watch and no-resource-info requests
//! are counted whether or not APF is installed) and calls [`monitor_request`]
//! (:590-629) with the elapsed time and response size. Also ported:
//! `requestLatencies` (:104-118), `responseSizes` (:152-162),
//! `requestAbortsTotal` (:243-252) and `RecordRequestAbort` (:496-506).
//!
//! Also ported: `apiSelfRequestCounter` (:222-230, :604-607),
//! `fieldValidationRequestLatencies` (:139-151; the child is created but, as
//! upstream, never observed :617-618) and `longRunningRequestsGauge` with
//! `RecordLongRunning` (:87-95, :551-575). The SLI/SLO histograms (:119-149)
//! and [`record_request_abort`] (called by the timeout filter,
//! `request_timeout`) are ported as well.
//!
//! DEVIATIONS: (1) the elapsed time of a watch is the time to response
//! headers, since the body streams after the handler returns; (2) response
//! size is the declared/exact body length (zero for unsized streams) rather
//! than bytes written; (3) `RecordLongRunning` is applied as a response-body
//! guard for WATCH/CONNECT verbs, so upgraded (101) exec/attach/portforward
//! connections are not held open by it; (4) `deprecated`/`removedRelease` are
//! resolved per request from `crate::deprecation` instead of being captured
//! per route at install time (`installer.go:781-797`); the endpoint-level
//! `Warning:` header is not emitted.

use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Duration;

use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts};
use rusternetes_common::latency_trackers::{with_latency_trackers, LatencyTrackers};

/// The request fields `RecordDroppedRequest`/`RecordRequestTermination` read
/// from `*http.Request` and `*request.RequestInfo`.
#[derive(Debug, Clone, Default)]
pub struct RequestMeta {
    /// HTTP method (`req.Method`).
    pub method: String,
    /// Raw query string (`req.URL.RawQuery`).
    pub query: String,
    pub path: String,
    /// `Content-Type: application/apply-patch+yaml`.
    pub apply_patch: bool,
    pub is_resource_request: bool,
    /// The Kubernetes verb (`requestInfo.Verb`).
    pub verb: String,
    pub namespace: String,
    pub name: String,
    pub api_group: String,
    pub api_version: String,
    pub resource: String,
    pub subresource: String,
    /// The authenticated user's name (`request.UserFrom(ctx)`), empty when
    /// authentication did not run.
    pub user_name: String,
}

const APISERVER: &str = "apiserver";
/// `OtherRequestMethod` (metrics.go:54).
const OTHER_REQUEST_METHOD: &str = "other";
/// `ReadOnlyKind` / `MutatingKind` (metrics.go:369-371).
const READ_ONLY_KIND: &str = "readOnly";
const MUTATING_KIND: &str = "mutating";
/// `inflightUsageMetricUpdatePeriod` (maxinflight.go:42).
const INFLIGHT_USAGE_METRIC_UPDATE_PERIOD: Duration = Duration::from_secs(1);

/// `validRequestMethods` (metrics.go:324-338).
const VALID_REQUEST_METHODS: &[&str] = &[
    "APPLY",
    "CONNECT",
    "CREATE",
    "DELETE",
    "DELETECOLLECTION",
    "GET",
    "LIST",
    "PATCH",
    "POST",
    "PROXY",
    "PUT",
    "UPDATE",
    "WATCH",
    "WATCHLIST",
];
/// `validConnectRequests` (metrics.go:341-346).
const VALID_CONNECT_REQUESTS: &[&str] = &["log", "exec", "portforward", "attach", "proxy"];

fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help).namespace(APISERVER), labels)
        .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
}

fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let g =
        IntGaugeVec::new(Opts::new(name, help).namespace(APISERVER), labels).expect("valid metric");
    let _ = register(Box::new(g.clone()));
    g
}

/// `requestCounter` (metrics.go:78-86).
static REQUEST_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "request_total",
        "Counter of apiserver requests broken out for each verb, dry run value, group, version, resource, scope, component, and HTTP response code.",
        &[
            "verb",
            "dry_run",
            "group",
            "version",
            "resource",
            "subresource",
            "scope",
            "component",
            "code",
        ],
    )
});

/// `requestTerminationsTotal` (metrics.go:212-220).
static REQUEST_TERMINATIONS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "request_terminations_total",
        "Number of requests which apiserver terminated in self-defense.",
        &[
            "verb",
            "group",
            "version",
            "resource",
            "subresource",
            "scope",
            "component",
            "code",
        ],
    )
});

/// `currentInflightRequests` (metrics.go:193-201).
static CURRENT_INFLIGHT_REQUESTS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "current_inflight_requests",
        "Maximal number of currently used inflight request limit of this apiserver per request kind in last second.",
        &["request_kind"],
    )
});

/// `currentInqueueRequests` (metrics.go:202-210).
static CURRENT_INQUEUE_REQUESTS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "current_inqueue_requests",
        "Maximal number of queued requests in this apiserver per request kind in last second.",
        &["request_kind"],
    )
});

/// `requestLatencies` (metrics.go:104-118).
static REQUEST_LATENCIES: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "request_duration_seconds",
        "Response latency distribution in seconds for each verb, dry run value, group, version, resource, subresource, scope and component.",
        vec![
            0.005, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0,
            8.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0,
        ],
        &[
            "verb",
            "dry_run",
            "group",
            "version",
            "resource",
            "subresource",
            "scope",
            "component",
        ],
    )
});

/// `requestSloLatencies` (metrics.go:110-124, deprecated since 1.27) and
/// `requestSliLatencies` (:125-139): latency not counting webhook and APF
/// queue-wait time.
fn sli_histogram(name: &str) -> HistogramVec {
    histogram_vec(
        name,
        "Response latency distribution (not counting webhook duration and priority & fairness queue wait times) in seconds for each verb, group, version, resource, subresource, scope and component.",
        vec![
            0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0,
            15.0, 20.0, 30.0, 45.0, 60.0,
        ],
        &[
            "verb",
            "group",
            "version",
            "resource",
            "subresource",
            "scope",
            "component",
        ],
    )
}

static REQUEST_SLO_LATENCIES: LazyLock<HistogramVec> =
    LazyLock::new(|| sli_histogram("request_slo_duration_seconds"));
static REQUEST_SLI_LATENCIES: LazyLock<HistogramVec> =
    LazyLock::new(|| sli_histogram("request_sli_duration_seconds"));

/// `responseSizes` (metrics.go:152-162): `ExponentialBuckets(1000, 10.0, 7)`.
static RESPONSE_SIZES: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "response_sizes",
        "Response size distribution in bytes for each group, version, verb, resource, subresource, scope and component.",
        prometheus::exponential_buckets(1000.0, 10.0, 7).expect("valid buckets"),
        &[
            "verb",
            "group",
            "version",
            "resource",
            "subresource",
            "scope",
            "component",
        ],
    )
});

/// `requestAbortsTotal` (metrics.go:243-252).
static REQUEST_ABORTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "request_aborts_total",
        "Number of requests which apiserver aborted possibly due to a timeout, for each group, version, verb, resource, subresource and scope",
        &["verb", "group", "version", "resource", "subresource", "scope"],
    )
});

/// `requestPostTimeoutTotal` (metrics.go:254-270): the activity of a request
/// handler after the apiserver timed its request out.
static REQUEST_POST_TIMEOUT_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "request_post_timeout_total",
        "Tracks the activity of the request handlers after the associated requests have been timed out by the apiserver",
        &["source", "status"],
    )
});

/// `PostTimeoutSourceTimeoutHandler` (metrics.go:392).
pub const POST_TIMEOUT_SOURCE_TIMEOUT_HANDLER: &str = "timeout-handler";
/// `PostTimeoutHandlerPanic` (metrics.go:402).
pub const POST_TIMEOUT_HANDLER_PANIC: &str = "panic";
/// `PostTimeoutHandlerOK` (metrics.go:410).
pub const POST_TIMEOUT_HANDLER_OK: &str = "ok";

/// `RecordRequestPostTimeout` (metrics.go:487-489).
pub fn record_request_post_timeout(source: &str, status: &str) {
    REQUEST_POST_TIMEOUT_TOTAL
        .with_label_values(&[source, status])
        .inc();
}

fn histogram_vec(name: &str, help: &str, buckets: Vec<f64>, labels: &[&str]) -> HistogramVec {
    let h = HistogramVec::new(
        HistogramOpts::new(name, help)
            .namespace(APISERVER)
            .buckets(buckets),
        labels,
    )
    .expect("valid metric");
    let _ = register(Box::new(h.clone()));
    h
}

/// `CleanScope` (metrics.go:684-696).
fn clean_scope(r: &RequestMeta) -> &'static str {
    if !r.name.is_empty() || r.verb == "create" {
        "resource"
    } else if !r.namespace.is_empty() {
        "namespace"
    } else if r.is_resource_request {
        "cluster"
    } else {
        ""
    }
}

/// `CanonicalVerb` (metrics.go:720-730); `verb` is upper-case.
fn canonical_verb(verb: &str, scope: &str) -> String {
    match verb {
        "GET" | "HEAD" if scope != "resource" && !scope.is_empty() => "LIST".to_string(),
        "HEAD" => "GET".to_string(),
        v => v.to_string(),
    }
}

/// `getVerbIfWatch` (metrics.go:793-803).
fn is_watch_query(method: &str, query: &str) -> bool {
    if !method.eq_ignore_ascii_case("GET") && !method.eq_ignore_ascii_case("LIST") {
        return false;
    }
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix("watch="))
        .is_some_and(|v| {
            let v = v.to_ascii_lowercase();
            v != "0" && v != "false"
        })
}

/// `cleanVerb(verb, "", req, requestInfo)` (metrics.go:777-790) over
/// `CleanVerb` (:734-750).
fn clean_verb(verb: String, r: &RequestMeta) -> String {
    let mut reported = verb.clone();
    if is_watch_query(&r.method, &r.query) || verb == "WATCHLIST" {
        reported = "WATCH".into();
    }
    if verb == "PATCH" && r.apply_patch {
        reported = "APPLY".into();
    }
    if r.is_resource_request
        && !r.subresource.is_empty()
        && VALID_CONNECT_REQUESTS.contains(&r.subresource.as_str())
    {
        reported = "CONNECT".into();
    }
    if VALID_REQUEST_METHODS.contains(&reported.as_str()) {
        reported
    } else {
        OTHER_REQUEST_METHOD.to_string()
    }
}

fn reported_verb(r: &RequestMeta, scope: &str) -> String {
    clean_verb(canonical_verb(&r.method.to_ascii_uppercase(), scope), r)
}

/// `cleanDryRun` (metrics.go:805-820); `ValidateDryRun` allows only `All`
/// (`allowedDryRunValues`, apimachinery meta/v1/validation:233).
fn clean_dry_run(query: &str) -> String {
    if !query.contains("dryRun") {
        return String::new();
    }
    let mut values: Vec<&str> = query
        .split('&')
        .filter_map(|kv| kv.strip_prefix("dryRun="))
        .collect();
    if values.iter().any(|v| !v.is_empty() && *v != "All") {
        return "invalid".into();
    }
    values.sort_unstable();
    values.dedup();
    values.join(",")
}

fn resource_gvr(req: &RequestMeta) -> (&str, &str, &str) {
    if req.is_resource_request {
        (
            req.api_group.as_str(),
            req.api_version.as_str(),
            req.resource.as_str(),
        )
    } else {
        ("", "", "")
    }
}

/// The `requestCounter` increment of `MonitorRequest` (metrics.go:599-603).
fn count_request(req: &RequestMeta, component: &str, code: u16) {
    let scope = clean_scope(req);
    let dry_run = clean_dry_run(&req.query);
    let verb = reported_verb(req, scope);
    let (group, version, resource) = resource_gvr(req);
    REQUEST_COUNTER
        .with_label_values(&[
            &verb,
            &dry_run,
            group,
            version,
            resource,
            &req.subresource,
            scope,
            component,
            &code.to_string(),
        ])
        .inc();
}

/// `MonitorRequest` (metrics.go:590-629): count the request, observe its
/// latency and, for reads, its response size.
pub fn monitor_request(
    req: &RequestMeta,
    component: &str,
    code: u16,
    elapsed: Duration,
    resp_size: u64,
    excluded: Option<Duration>,
) {
    count_request(req, component, code);
    let scope = clean_scope(req);
    let dry_run = clean_dry_run(&req.query);
    let verb = reported_verb(req, scope);
    let (group, version, resource) = resource_gvr(req);
    // "MonitorRequest happens after authentication, so we can trust the
    // username given by the request" (:604-607).
    if req.user_name == API_SERVER_USER {
        API_SELF_REQUEST_COUNTER
            .with_label_values(&[&verb, group, resource, &req.subresource])
            .inc();
    }
    // `deprecated` / `removedRelease` are fixed per route at install time
    // (installer.go:781-797); resolved here from the request's resource.
    let (deprecated, removed_release) =
        crate::deprecation::deprecation_for(group, version, resource, &req.subresource);
    if deprecated {
        DEPRECATED_REQUEST_GAUGE
            .with_label_values(&[group, version, resource, &req.subresource, &removed_release])
            .set(1);
        crate::audit::add_audit_annotation(DEPRECATED_ANNOTATION_KEY, "true");
        if !removed_release.is_empty() {
            crate::audit::add_audit_annotation(REMOVED_RELEASE_ANNOTATION_KEY, &removed_release);
        }
    }
    REQUEST_LATENCIES
        .with_label_values(&[
            &verb,
            &dry_run,
            group,
            version,
            resource,
            &req.subresource,
            scope,
            component,
        ])
        .observe(elapsed.as_secs_f64());
    // Upstream creates the child here but never calls Observe (:617-618);
    // replicated, so the series exists with a zero count.
    FIELD_VALIDATION_REQUEST_LATENCIES.with_label_values(&[&clean_field_validation(&req.query)]);
    // `if wd, ok := LatencyTrackersFrom(ctx); ok && dryRun == ""` (:620-625):
    // the latency minus webhook and APF queue-wait time.
    if let (Some(excluded), "") = (excluded, dry_run.as_str()) {
        let sli = elapsed.as_secs_f64() - excluded.as_secs_f64();
        let labels = [
            verb.as_str(),
            group,
            version,
            resource,
            req.subresource.as_str(),
            scope,
            component,
        ];
        REQUEST_SLO_LATENCIES
            .with_label_values(&labels)
            .observe(sli);
        REQUEST_SLI_LATENCIES
            .with_label_values(&labels)
            .observe(sli);
    }
    // "We are only interested in response sizes of read requests." (:624)
    if verb == "GET" || verb == "LIST" {
        RESPONSE_SIZES
            .with_label_values(&[
                &verb,
                group,
                version,
                resource,
                &req.subresource,
                scope,
                component,
            ])
            .observe(resp_size as f64);
    }
}

/// `cleanFieldValidation` (metrics.go:822-834) with `ValidateFieldValidation`
/// (apimachinery meta/v1/validation:242-252): `allowedFieldValidationValues`
/// is `{"", Ignore, Warn, Strict}`.
pub fn clean_field_validation(query: &str) -> String {
    // "avoid allocating when we don't see fieldValidation in the query"
    if !query.contains("fieldValidation") {
        return String::new();
    }
    let values: Vec<_> = url::form_urlencoded::parse(query.as_bytes())
        .filter(|(k, _)| k == "fieldValidation")
        .map(|(_, v)| v)
        .collect();
    if values.len() != 1 || !matches!(&*values[0], "" | "Ignore" | "Warn" | "Strict") {
        return "invalid".into();
    }
    values[0].to_string()
}

/// `deprecatedRequestGauge` (metrics.go:66-74).
static DEPRECATED_REQUEST_GAUGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "requested_deprecated_apis",
        "Gauge of deprecated APIs that have been requested, broken out by API group, version, resource, subresource, and removed_release.",
        &["group", "version", "resource", "subresource", "removed_release"],
    )
});

/// `deprecatedAnnotationKey` (metrics.go:380-382).
const DEPRECATED_ANNOTATION_KEY: &str = "k8s.io/deprecated";
/// `removedReleaseAnnotationKey` (metrics.go:383-387).
const REMOVED_RELEASE_ANNOTATION_KEY: &str = "k8s.io/removed-release";

/// `apiSelfRequestCounter` (metrics.go:222-230).
static API_SELF_REQUEST_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "selfrequest_total",
        "Counter of apiserver self-requests broken out for each verb, API resource and subresource.",
        &["verb", "group", "resource", "subresource"],
    )
});

/// `fieldValidationRequestLatencies` (metrics.go:139-151). Upstream sets no
/// `Subsystem`, so the name carries no `apiserver_` prefix.
static FIELD_VALIDATION_REQUEST_LATENCIES: LazyLock<HistogramVec> = LazyLock::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "field_validation_request_duration_seconds",
            "Response latency distribution in seconds for each field validation value",
        )
        .buckets(vec![
            0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0,
            15.0, 20.0, 30.0, 45.0, 60.0,
        ]),
        &["field_validation"],
    )
    .expect("valid metric");
    let _ = register(Box::new(h.clone()));
    h
});

/// `longRunningRequestsGauge` (metrics.go:87-95).
static LONG_RUNNING_REQUESTS_GAUGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "longrunning_requests",
        "Gauge of all active long-running apiserver requests broken out by verb, group, version, resource, scope and component. Not all requests are tracked this way.",
        &["verb", "group", "version", "resource", "subresource", "scope", "component"],
    )
});

/// `user.APIServerUser` (authentication/user/user.go:78).
const API_SERVER_USER: &str = "system:apiserver";

/// Holds the `longrunning_requests` gauge up for the life of the guard: the
/// `g.Inc(); defer g.Dec(); fn()` of `RecordLongRunning` (metrics.go:571-574)
/// with the closure replaced by scope.
pub struct LongRunningGuard(prometheus::IntGauge);

impl Drop for LongRunningGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// `RecordLongRunning` (metrics.go:551-575).
pub fn record_long_running(req: &RequestMeta, component: &str) -> LongRunningGuard {
    let scope = clean_scope(req);
    // As in `MonitorRequest`, the verb comes from the method, not requestInfo.
    let verb = reported_verb(req, scope);
    let g = if req.is_resource_request {
        LONG_RUNNING_REQUESTS_GAUGE.with_label_values(&[
            &verb,
            &req.api_group,
            &req.api_version,
            &req.resource,
            &req.subresource,
            scope,
            component,
        ])
    } else {
        LONG_RUNNING_REQUESTS_GAUGE
            .with_label_values(&[&verb, "", "", "", &req.path, scope, component])
    };
    g.inc();
    LongRunningGuard(g)
}

/// A response body that carries a [`LongRunningGuard`]: the gauge stays up
/// until the body (the watch / connect / log stream) is finished or dropped.
struct GuardedBody {
    inner: axum::body::Body,
    guard: Arc<StreamMonitor>,
}

/// `MonitorRequest` for a blocking handler. Upstream runs `MonitorRequest`
/// after `routeFunc` returns (`InstrumentRouteFunc`, metrics.go:645-647), and a
/// WATCH / CONNECT handler blocks until the stream or connection ends, so the
/// duration covers the connection's lifetime. Here the handler returns at the
/// response headers, so the observation moves to the drop of the last owner:
/// the response body (watch, log) or, for an upgraded connect, the
/// `ConnectionHold` moved into the splice task. Also owns the
/// `longrunning_requests` gauge (`RecordLongRunning`, metrics.go:551-575), which
/// falls after the observation, as `defer g.Dec()` runs inside the handler.
struct StreamMonitor {
    meta: RequestMeta,
    started: std::time::Instant,
    code: std::sync::atomic::AtomicU16,
    /// `ResponseWriterDelegator.written` (metrics.go:844, :863).
    written: std::sync::atomic::AtomicU64,
    trackers: Option<Arc<LatencyTrackers>>,
    _guard: LongRunningGuard,
}

impl Drop for StreamMonitor {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering::Relaxed;
        monitor_request(
            &self.meta,
            "apiserver",
            self.code.load(Relaxed),
            self.started.elapsed(),
            self.written.load(Relaxed),
            self.trackers.as_ref().map(|t| t.excluded_latency()),
        );
    }
}

impl http_body::Body for GuardedBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let poll = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        // `ResponseWriterDelegator.Write` counts the bytes written
        // (metrics.go:858-865).
        if let std::task::Poll::Ready(Some(Ok(frame))) = &poll {
            if let Some(data) = frame.data_ref() {
                self.guard
                    .written
                    .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
            }
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// `RecordRequestAbort` (metrics.go:496-506).
pub fn record_request_abort(req: &RequestMeta) {
    let scope = clean_scope(req);
    let verb = reported_verb(req, scope);
    REQUEST_ABORTS_TOTAL
        .with_label_values(&[
            &verb,
            &req.api_group,
            &req.api_version,
            &req.resource,
            &req.subresource,
            scope,
        ])
        .inc();
}

/// `NewRequestInfo`-derived [`RequestMeta`]; a request without resource info
/// reports `RequestInfo{Verb: req.Method, Path: req.URL.Path}` (:591-594).
pub fn request_meta(req: &axum::extract::Request) -> RequestMeta {
    let attrs = crate::audit::request_info(req.method(), req.uri().path(), req.uri().query());
    RequestMeta {
        method: req.method().as_str().to_string(),
        query: req.uri().query().unwrap_or("").to_string(),
        path: attrs.path,
        apply_patch: req
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .is_some_and(|v| v.as_bytes() == b"application/apply-patch+yaml"),
        is_resource_request: attrs.is_resource_request,
        verb: attrs.verb,
        namespace: attrs.namespace,
        name: attrs.name,
        api_group: attrs.api_group,
        api_version: attrs.api_version,
        resource: attrs.resource,
        subresource: attrs.subresource,
        user_name: req
            .extensions()
            .get::<rusternetes_middleware::AuthContext>()
            .map(|c| c.user.username.clone())
            .unwrap_or_default(),
    }
}

/// `WithLatencyTrackers` (endpoints/filters/webhook_duration.go:36-61): install
/// the per-request trackers for every request except watches. Placed outside
/// authentication and audit (server/config.go:1084), so both can read/write
/// them; `instrument_request` reuses the instance.
///
/// The Go filter also wraps the ResponseWriter in `writeLatencyTracker`
/// (response-write tracking); that part is not ported (axum bodies stream
/// outside the handler chain) and is tracked as a follow-up.
pub async fn with_latency_trackers_filter(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let attrs = crate::audit::request_info(req.method(), req.uri().path(), req.uri().query());
    if attrs.verb == "watch" {
        return next.run(req).await;
    }
    with_latency_trackers(Arc::new(LatencyTrackers::new()), next.run(req)).await
}

/// `InstrumentRouteFunc` (metrics.go:633-650) as a middleware: time the inner
/// handler and call [`monitor_request`].
pub async fn instrument_request(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::body::HttpBody;
    let meta = request_meta(&req);
    let started = std::time::Instant::now();
    // `WithLatencyTrackers` (filters/webhook_duration.go:36-61) installs the
    // trackers for every request except watches.
    // When the outer [`with_latency_trackers_filter`] already installed them
    // (so authentication and audit share them), reuse that instance.
    let outer_trackers = rusternetes_common::latency_trackers::latency_trackers_from_context();
    let install_here = outer_trackers.is_none() && meta.verb != "watch";
    let trackers =
        outer_trackers.or_else(|| install_here.then(|| Arc::new(LatencyTrackers::new())));
    // Upgraded CONNECTs (exec/attach/portforward) return their 101 at once and
    // splice in a detached task, so the gauge must travel with the connection:
    // `RecordLongRunning` wraps the blocking `ServeHTTP` (rest.go:218). The
    // hold is shared with the response body guard below; the gauge falls when
    // the last owner drops.
    let verb = reported_verb(&meta, clean_scope(&meta));
    let upgrade = req.headers().contains_key(axum::http::header::UPGRADE) && verb == "CONNECT";
    let monitor = matches!(verb.as_str(), "WATCH" | "CONNECT").then(|| {
        Arc::new(StreamMonitor {
            _guard: record_long_running(&meta, "apiserver"),
            meta: meta.clone(),
            started,
            code: std::sync::atomic::AtomicU16::new(0),
            written: std::sync::atomic::AtomicU64::new(0),
            trackers: trackers.clone(),
        })
    });
    if upgrade {
        if let Some(g) = &monitor {
            req.extensions_mut()
                .insert(rusternetes_streamproxy::ConnectionHold(g.clone()));
        }
    }
    let resp = match &trackers {
        Some(t) if install_here => with_latency_trackers(t.clone(), next.run(req)).await,
        _ => next.run(req).await,
    };
    // `RecordLongRunning` wraps watch serving (handlers/get.go:283), connect
    // handlers (handlers/rest.go:218) and `StreamObject` for logs
    // (responsewriters/writers.go:327); the verbs those paths report are
    // WATCH and CONNECT (`validConnectRequests` includes log). Their
    // `MonitorRequest` runs when the stream ends (see [`StreamMonitor`]).
    if let Some(monitor) = monitor {
        monitor
            .code
            .store(resp.status().as_u16(), std::sync::atomic::Ordering::Relaxed);
        return resp.map(|body| {
            axum::body::Body::new(GuardedBody {
                inner: body,
                guard: monitor,
            })
        });
    }
    let size = resp
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse().ok())
        .or_else(|| resp.body().size_hint().exact())
        .unwrap_or(0);
    monitor_request(
        &meta,
        "apiserver",
        resp.status().as_u16(),
        started.elapsed(),
        size,
        trackers.map(|t| t.excluded_latency()),
    );
    resp
}

/// `RecordDroppedRequest` (metrics.go:508-526): the request was rejected via
/// `http.TooManyRequests`; counted in `apiserver_request_total` with code 429.
pub fn record_dropped_request(req: &RequestMeta, component: &str, _is_mutating: bool) {
    count_request(req, component, 429);
}

/// `RecordRequestTermination` (metrics.go:532-549): the request was
/// terminated early as part of an apiserver self-defense mechanism; "should
/// only be called zero or one times per request".
pub fn record_request_termination(req: &RequestMeta, component: &str, code: u16) {
    let scope = clean_scope(req);
    let verb = reported_verb(req, scope);
    let code = code.to_string();
    if req.is_resource_request {
        REQUEST_TERMINATIONS_TOTAL
            .with_label_values(&[
                &verb,
                &req.api_group,
                &req.api_version,
                &req.resource,
                &req.subresource,
                scope,
                component,
                &code,
            ])
            .inc();
    } else {
        // Non-resource requests report the path as the subresource (:547).
        REQUEST_TERMINATIONS_TOTAL
            .with_label_values(&[&verb, "", "", "", &req.path, scope, component, &code])
            .inc();
    }
}

/// `requestWatermark` (maxinflight.go:56-84): the maximal number of requests
/// in a phase since the last maintenance tick.
#[derive(Default)]
pub struct Watermark {
    /// `(readOnlyWatermark, mutatingWatermark)`.
    marks: Mutex<(i64, i64)>,
}

impl Watermark {
    pub fn record_mutating(&self, v: i64) {
        let mut m = self.marks.lock().unwrap();
        m.1 = m.1.max(v);
    }

    pub fn record_read_only(&self, v: i64) {
        let mut m = self.marks.lock().unwrap();
        m.0 = m.0.max(v);
    }

    /// `(readOnly, mutating)` maxima since the last call, resetting both
    /// (maxinflight.go:94-100).
    pub fn take(&self) -> (i64, i64) {
        std::mem::take(&mut *self.marks.lock().unwrap())
    }
}

/// One phase's live counts and its watermark (`atomicReadOnlyExecuting` & co
/// plus `watermark`/`waitingMark`, priority-and-fairness.go:44-56).
#[derive(Default)]
struct Phase {
    counts: Mutex<(i64, i64)>,
    mark: Watermark,
}

impl Phase {
    /// `noteExecutingDelta`/`noteWaitingDelta` (priority-and-fairness.go:143-156).
    fn note_delta(&self, mutating: bool, delta: i64) {
        let mut c = self.counts.lock().unwrap();
        if mutating {
            c.1 += delta;
            self.mark.record_mutating(c.1);
        } else {
            c.0 += delta;
            self.mark.record_read_only(c.0);
        }
    }
}

static EXECUTING: LazyLock<Phase> = LazyLock::new(Phase::default);
static WAITING: LazyLock<Phase> = LazyLock::new(Phase::default);

pub fn note_executing_delta(mutating: bool, delta: i64) {
    EXECUTING.note_delta(mutating, delta);
}

pub fn note_waiting_delta(mutating: bool, delta: i64) {
    WAITING.note_delta(mutating, delta);
}

/// `UpdateInflightRequestMetrics` (metrics.go:465-476).
fn update_inflight_request_metrics(executing: bool, non_mutating: i64, mutating: i64) {
    let gauge = if executing {
        &CURRENT_INFLIGHT_REQUESTS
    } else {
        &CURRENT_INQUEUE_REQUESTS
    };
    gauge.with_label_values(&[READ_ONLY_KIND]).set(non_mutating);
    gauge.with_label_values(&[MUTATING_KIND]).set(mutating);
}

/// One tick of `startWatermarkMaintenance` (maxinflight.go:93-103) for the
/// executing and waiting watermarks.
///
/// Upstream's tick resets the watermark to 0, so an executing request that
/// spans ticks is not re-reported; this keeps that behaviour exactly.
pub fn flush_watermarks() {
    let (ro, m) = EXECUTING.mark.take();
    update_inflight_request_metrics(true, ro, m);
    let (ro, m) = WAITING.mark.take();
    update_inflight_request_metrics(false, ro, m);
}

/// `StartPriorityAndFairnessWatermarkMaintenance` (priority-and-fairness.go:358-363),
/// started once; the loop lives for the process.
pub fn start_watermark_maintenance() {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }
    tokio::spawn(async {
        let mut tick = tokio::time::interval(INFLIGHT_USAGE_METRIC_UPDATE_PERIOD);
        loop {
            tick.tick().await;
            flush_watermarks();
        }
    });
}
