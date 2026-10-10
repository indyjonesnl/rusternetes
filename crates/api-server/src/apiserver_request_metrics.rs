//! `apiserver_*` request metrics used by the APF filter.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/endpoints/metrics/metrics.go`
//! (release-1.35).

/// The request fields `RecordDroppedRequest`/`RecordRequestTermination` read
/// from `*http.Request` and `*request.RequestInfo`.
#[derive(Debug, Clone, Default)]
pub struct RequestMeta {
    pub method: String,
    pub query: String,
    pub path: String,
    pub is_resource_request: bool,
    pub verb: String,
    pub namespace: String,
    pub name: String,
    pub api_group: String,
    pub api_version: String,
    pub resource: String,
    pub subresource: String,
}

pub fn record_dropped_request(_req: &RequestMeta, _component: &str, _is_mutating: bool) {}

pub fn record_request_termination(_req: &RequestMeta, _component: &str, _code: u16) {}

#[derive(Default)]
pub struct Watermark;

impl Watermark {
    pub fn record_mutating(&self, _v: i64) {}
    pub fn record_read_only(&self, _v: i64) {}
    /// `(readOnly, mutating)` maxima since the last call.
    pub fn take(&self) -> (i64, i64) {
        (0, 0)
    }
}

pub fn note_executing_delta(_mutating: bool, _delta: i64) {}

pub fn note_waiting_delta(_mutating: bool, _delta: i64) {}

/// One tick of `startWatermarkMaintenance` (maxinflight.go:93-103) for both marks.
pub fn flush_watermarks() {}

pub fn start_watermark_maintenance() {}

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
