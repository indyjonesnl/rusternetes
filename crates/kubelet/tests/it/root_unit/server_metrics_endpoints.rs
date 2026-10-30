//! Unit tests moved from `src/server_metrics_endpoints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        http::{header, Method, StatusCode},
        response::{IntoResponse, Response},
        routing::any,
        Router,
    };
    pub use prometheus::{
        Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
    };
    pub use std::sync::LazyLock;
    pub use std::time::Instant;
}

mod tests {
    use super::__parent::*;
    use axum::body::Body;
    use axum::http::Request;
    use rusternetes_kubelet::server_metrics_endpoints::*;
    use tower::ServiceExt;

    async fn call(method: &str, path: &str) -> (StatusCode, String) {
        let resp = router()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    /// Slis: observed checks are served with upstream's names/labels; DELETE
    /// resets them (`HandlerWithReset`). Single test, since the registry is
    /// process-global.
    #[tokio::test]
    async fn slis_serve_observations_and_reset() {
        observe_healthcheck("t-ping", "healthz", HealthcheckStatus::Success);
        observe_healthcheck("t-sync", "healthz", HealthcheckStatus::Error);
        let (status, body) = call("GET", "/metrics/slis").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("kubernetes_healthcheck{name=\"t-ping\",type=\"healthz\"} 1"));
        assert!(body.contains("kubernetes_healthcheck{name=\"t-sync\",type=\"healthz\"} 0"));
        assert!(body.contains(
            "kubernetes_healthchecks_total{name=\"t-sync\",status=\"error\",type=\"healthz\"} 1"
        ));
        assert!(body.contains("process_start_time_seconds"));

        let (status, body) = call("DELETE", "/metrics/slis").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "metrics reset\n");
        let (_, body) = call("GET", "/metrics/slis").await;
        assert!(!body.contains("name=\"t-ping\""));
    }

    /// Probes: results counter plus duration (not for failures,
    /// `worker.go:358-360`).
    #[tokio::test]
    async fn probes_serve_results_and_duration() {
        let now = Instant::now();
        observe_probe(
            ProbeType::Liveness,
            ProbeResult::Successful,
            "c",
            "p",
            "ns",
            "uid-1",
            now,
        );
        observe_probe(
            ProbeType::Readiness,
            ProbeResult::Failed,
            "c",
            "p",
            "ns",
            "uid-1",
            now,
        );
        let (status, body) = call("GET", "/metrics/probes").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("prober_probe_total{container=\"c\",namespace=\"ns\",pod=\"p\",pod_uid=\"uid-1\",probe_type=\"Liveness\",result=\"successful\"} 1"));
        assert!(body.contains("result=\"failed\""));
        assert!(body.contains("prober_probe_duration_seconds_count{container=\"c\",namespace=\"ns\",pod=\"p\",probe_type=\"Liveness\"} 1"));
        assert!(!body.contains("prober_probe_duration_seconds_count{container=\"c\",namespace=\"ns\",pod=\"p\",probe_type=\"Readiness\"}"));
    }
}
