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
//! DEVIATION: this server has no general request-instrumentation filter yet,
//! so `apiserver_request_total` is only incremented by
//! [`record_dropped_request`] (the 429s); upstream also increments it per
//! served request in `MonitorRequest`.

use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::Duration;

use prometheus::{register, IntCounterVec, IntGaugeVec, Opts};

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

/// `RecordDroppedRequest` (metrics.go:508-526): the request was rejected via
/// `http.TooManyRequests`; counted in `apiserver_request_total` with code 429.
pub fn record_dropped_request(req: &RequestMeta, component: &str, _is_mutating: bool) {
    let scope = clean_scope(req);
    let dry_run = clean_dry_run(&req.query);
    let verb = reported_verb(req, scope);
    let (group, version, resource) = if req.is_resource_request {
        (
            req.api_group.as_str(),
            req.api_version.as_str(),
            req.resource.as_str(),
        )
    } else {
        ("", "", "")
    };
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
            "429",
        ])
        .inc();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_control::metric_test_util::sample;

    fn meta(resource: &str) -> RequestMeta {
        RequestMeta {
            method: "GET".into(),
            path: format!("/api/v1/namespaces/default/{resource}"),
            is_resource_request: true,
            verb: "list".into(),
            namespace: "default".into(),
            api_version: "v1".into(),
            resource: resource.into(),
            ..Default::default()
        }
    }

    // RecordDroppedRequest (metrics.go:508-526): `apiserver_request_total`
    // with the 429 code, verb canonicalised (a collection GET is LIST).
    #[test]
    fn dropped_request_counts_apiserver_request_total_429() {
        let l = [
            ("verb", "LIST"),
            ("resource", "droppedthings"),
            ("scope", "namespace"),
            ("component", "apiserver"),
            ("code", "429"),
        ];
        let before = sample("apiserver_request_total", &l);
        record_dropped_request(&meta("droppedthings"), "apiserver", false);
        assert_eq!(sample("apiserver_request_total", &l), before + 1.0);
    }

    // RecordRequestTermination (metrics.go:532-549).
    #[test]
    fn request_termination_counts_terminations_total() {
        let l = [
            ("verb", "LIST"),
            ("resource", "terminatedthings"),
            ("scope", "namespace"),
            ("component", "apiserver"),
            ("code", "429"),
        ];
        let before = sample("apiserver_request_terminations_total", &l);
        record_request_termination(&meta("terminatedthings"), "apiserver", 429);
        assert_eq!(
            sample("apiserver_request_terminations_total", &l),
            before + 1.0
        );
    }

    // Non-resource requests report the path as subresource and empty
    // group/version/resource (metrics.go:547).
    #[test]
    fn non_resource_termination_uses_path_as_subresource() {
        let req = RequestMeta {
            method: "GET".into(),
            path: "/nonresourcepath".into(),
            verb: "get".into(),
            ..Default::default()
        };
        record_request_termination(&req, "apiserver", 429);
        assert!(crate::flow_control::metric_test_util::has_series(
            "apiserver_request_terminations_total",
            &[("subresource", "/nonresourcepath"), ("scope", "")]
        ));
    }

    // requestWatermark (maxinflight.go:56-84, 93-103): records the maximum
    // since the last tick, which `take` returns and resets.
    #[test]
    fn watermark_reports_max_then_resets() {
        let w = Watermark::default();
        w.record_mutating(1);
        w.record_mutating(3);
        w.record_mutating(2);
        w.record_read_only(5);
        assert_eq!(w.take(), (5, 3));
        assert_eq!(w.take(), (0, 0));
    }

    // The gauges exist (readOnly/mutating) after a flush
    // (UpdateInflightRequestMetrics, metrics.go:465-476), for both phases.
    #[test]
    fn flush_sets_inflight_and_inqueue_gauges() {
        note_executing_delta(true, 1);
        note_waiting_delta(false, 1);
        flush_watermarks();
        note_executing_delta(true, -1);
        note_waiting_delta(false, -1);
        for (name, kind) in [
            ("apiserver_current_inflight_requests", "readOnly"),
            ("apiserver_current_inflight_requests", "mutating"),
            ("apiserver_current_inqueue_requests", "readOnly"),
            ("apiserver_current_inqueue_requests", "mutating"),
        ] {
            assert!(
                crate::flow_control::metric_test_util::has_series(name, &[("request_kind", kind)]),
                "{name}{{request_kind={kind}}}"
            );
        }
    }
}
