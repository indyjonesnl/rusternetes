//! Unit tests moved from `src/controllers/hpa_metrics_client.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use async_trait::async_trait;
    pub use chrono::{DateTime, Utc};
    pub use reqwest::Client;
    pub use rusternetes_common::quantity::parse_resource_value;
    pub use rusternetes_common::resources::CrossVersionObjectReference;
    pub use rusternetes_common::types::LabelSelector;
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::hpa_metrics_client::*;

    #[tokio::test]
    async fn fake_returns_seeded_pods_metric() {
        let mut fake = FakeMetricsClient::new();
        fake.pods.insert(
            "requests-per-second".into(),
            FakeMetricsClient::pods_info(&[("p1", 200, None), ("p2", 200, None)]),
        );
        let sel = LabelSelector::default();
        let (info, _) = fake
            .get_raw_metric("requests-per-second", "default", &sel, "")
            .await
            .unwrap();
        assert_eq!(info.len(), 2);
        assert_eq!(info["p1"].value, 200);
    }

    #[test]
    fn custom_metric_values_are_milli() {
        // client.go:162 `m.Value.MilliValue()`: "5" reads 5000, "500m" 500.
        assert_eq!(metric_milli_value("5"), 5000);
        assert_eq!(metric_milli_value("500m"), 500);
        assert_eq!(metric_milli_value("1k"), 1_000_000);
        assert_eq!(metric_milli_value("0.5"), 500);
        assert_eq!(metric_milli_value("bogus"), 0);
        assert_eq!(parse_milli("bogus"), None);
    }

    #[test]
    fn milli_quantity_formats_like_new_milli_quantity() {
        assert_eq!(format_milli_quantity(5000), "5");
        assert_eq!(format_milli_quantity(500), "500m");
        assert_eq!(format_milli_quantity(1_500), "1500m");
        assert_eq!(format_milli_quantity(0), "0");
    }

    #[test]
    fn parse_quantity_cpu_and_memory_units() {
        // CPU -> millicores
        assert_eq!(parse_quantity_milli("500m", "cpu"), 500);
        assert_eq!(parse_quantity_milli("2", "cpu"), 2000);
        // Memory -> bytes, binary + SI suffixes
        assert_eq!(parse_quantity_milli("128Mi", "memory"), 128 * 1024 * 1024);
        assert_eq!(parse_quantity_milli("1Gi", "memory"), 1024 * 1024 * 1024);
        assert_eq!(parse_quantity_milli("1M", "memory"), 1_000_000);
        assert_eq!(parse_quantity_milli("1048576", "memory"), 1_048_576);
        // Other metrics -> base unit
        assert_eq!(parse_quantity_milli("42", "requests-per-second"), 42);
    }

    /// CPU parsed the pre-`m` digits with `parse::<i64>()`, so a fractional
    /// millicore reading collapsed to 0 — an HPA target computed from that
    /// scales on a utilisation of zero. Non-cpu/memory metrics took only the
    /// leading digit run, so a canonicalised `"1k"` read as 1.
    #[test]
    fn parse_quantity_handles_fractional_cpu_and_suffixed_custom_metrics() {
        assert_eq!(parse_quantity_milli("10.5m", "cpu"), 11);
        assert_eq!(parse_quantity_milli("0.5m", "cpu"), 1);
        assert_eq!(parse_quantity_milli("0.7", "cpu"), 700);
        assert_eq!(parse_quantity_milli("1500u", "cpu"), 2);
        assert_eq!(parse_quantity_milli("1k", "requests-per-second"), 1_000);
        assert_eq!(parse_quantity_milli("2M", "requests-per-second"), 2_000_000);
        // Unparseable stays 0 — the caller has no error channel.
        assert_eq!(parse_quantity_milli("bogus", "cpu"), 0);
        assert_eq!(parse_quantity_milli("", "memory"), 0);
    }

    #[tokio::test]
    async fn fake_container_resource_metric_lookup() {
        let mut fake = FakeMetricsClient::new();
        fake.resource.insert(
            "cpu/app".to_string(),
            FakeMetricsClient::pods_info(&[("p1", 50, Some(75))]),
        );
        let sel = LabelSelector::default();
        let info = fake
            .get_container_resource_metric("cpu", "app", "default", &sel)
            .await
            .unwrap();
        assert_eq!(info["p1"].utilization, Some(75));
        // Missing container -> error.
        assert!(fake
            .get_container_resource_metric("cpu", "missing", "default", &sel)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn fake_errors_on_missing_metric() {
        let fake = FakeMetricsClient::new();
        let sel = LabelSelector::default();
        assert!(fake
            .get_resource_metric("cpu", "default", &sel)
            .await
            .is_err());
    }

    /// In-cluster shape (#1144): no client cert on disk, just a bearer token
    /// and `insecure_skip_tls_verify`. This is exactly what `run_api_mode`
    /// builds; it must construct a client (and trim the base URL) rather than
    /// failing on a missing `/etc/kubernetes/pki` cert read.
    #[test]
    fn http_client_builds_without_client_cert_insecure() {
        let cfg = HttpMetricsConfig {
            api_server_url: "https://api-server:6443/".to_string(),
            ca_pem: None,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            token: Some("test-bearer-token".to_string()),
            insecure_skip_tls_verify: true,
        };
        let client = HttpMetricsClient::new(cfg).expect("in-cluster (token, insecure) builds");
        // Trailing slash is trimmed off the base.
        assert_eq!(client.base, "https://api-server:6443");
    }

    /// Legacy mTLS path: a configured-but-missing client cert must surface a
    /// clear read error naming the path (not a generic builder failure).
    #[test]
    fn http_client_missing_client_cert_errors_clearly() {
        let cfg = HttpMetricsConfig {
            client_cert_path: Some("/nonexistent/api-server.crt".to_string()),
            client_key_path: Some("/nonexistent/api-server.key".to_string()),
            ..HttpMetricsConfig::default()
        };
        let err = HttpMetricsClient::new(cfg)
            .map(|_| ())
            .expect_err("missing cert must error");
        assert!(
            err.to_string().contains("/nonexistent/api-server.crt"),
            "error should name the missing cert path, got: {err}"
        );
    }

    // -- wire contract: custom/external metrics requests -------------------
    //
    // Upstream pkg/controller/podautoscaler/metrics/client_test.go drives the
    // REST metrics client through a fake clientset; here the same assertions
    // run against an in-process api-server stub so the request the controller
    // really puts on the wire is what is checked.

    use std::sync::{Arc, Mutex};

    type Requests = Arc<Mutex<Vec<(String, HashMap<String, String>)>>>;

    /// Serve `routes` (path -> JSON body) on an ephemeral port; every request
    /// is recorded as (path, decoded query). Unknown paths answer 404.
    async fn stub_api_server(routes: Vec<(&'static str, serde_json::Value)>) -> (String, Requests) {
        let seen: Requests = Arc::new(Mutex::new(Vec::new()));
        let seen_h = seen.clone();
        let routes = Arc::new(routes);
        let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
            let seen = seen_h.clone();
            let routes = routes.clone();
            async move {
                let query: HashMap<String, String> = reqwest::Url::parse(&format!(
                    "http://stub{}",
                    uri.path_and_query().map(|p| p.as_str()).unwrap_or("/")
                ))
                .unwrap()
                .query_pairs()
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
                seen.lock().unwrap().push((uri.path().to_string(), query));
                match routes.iter().find(|(p, _)| *p == uri.path()) {
                    Some((_, body)) => (axum::http::StatusCode::OK, axum::Json(body.clone())),
                    None => (
                        axum::http::StatusCode::NOT_FOUND,
                        axum::Json(serde_json::json!({"message": "not found"})),
                    ),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), seen)
    }

    fn client_for(url: String) -> HttpMetricsClient {
        HttpMetricsClient::new(HttpMetricsConfig {
            api_server_url: url,
            ca_pem: None,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            token: None,
            insecure_skip_tls_verify: true,
        })
        .unwrap()
    }

    fn pod_selector(app: &str) -> LabelSelector {
        LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), app.to_string())])),
            match_expressions: None,
        }
    }

    fn metric_item(name: &str, value: &str, ts: &str, window: Option<i64>) -> serde_json::Value {
        let mut item = serde_json::json!({
            "describedObject": {"kind": "Pod", "name": name, "apiVersion": "v1"},
            "metric": {"name": "qps"},
            "timestamp": ts,
            "value": value,
        });
        if let Some(w) = window {
            item["windowSeconds"] = serde_json::json!(w);
        }
        item
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn core_discovery() -> (&'static str, serde_json::Value) {
        (
            "/api/v1",
            serde_json::json!({
                "kind": "APIResourceList", "groupVersion": "v1",
                "resources": [
                    {"name": "pods", "kind": "Pod", "namespaced": true},
                    {"name": "pods/log", "kind": "Pod", "namespaced": true},
                ],
            }),
        )
    }

    fn apps_discovery() -> Vec<(&'static str, serde_json::Value)> {
        vec![
            (
                "/apis/apps",
                serde_json::json!({
                    "kind": "APIGroup", "name": "apps",
                    "versions": [{"groupVersion": "apps/v1", "version": "v1"}],
                    "preferredVersion": {"groupVersion": "apps/v1", "version": "v1"},
                }),
            ),
            (
                "/apis/apps/v1",
                serde_json::json!({
                    "kind": "APIResourceList", "groupVersion": "apps/v1",
                    "resources": [
                        {"name": "deployments", "kind": "Deployment", "namespaced": true},
                        {"name": "deployments/scale", "kind": "Scale", "namespaced": true},
                    ],
                }),
            ),
        ]
    }

    /// `GetRawMetric` -> `GetForObjects(Pod, selector, metricName,
    /// metricSelector)` (metrics/client.go:130-132) puts the pod selector in
    /// `labelSelector` and the metric selector in `metricLabelSelector`
    /// (custom_metrics/versioned_client.go:226-251), and reads
    /// `windowSeconds` per item, defaulting to 1m (client.go:136-143).
    #[tokio::test]
    async fn raw_metric_sends_pod_and_metric_selectors_and_reads_window() {
        let body = serde_json::json!({"kind": "MetricValueList", "items": [
            metric_item("web-0", "5", "2026-01-01T00:00:10Z", Some(30)),
            metric_item("web-1", "7", "2026-01-01T00:00:20Z", None),
        ]});
        let (url, seen) = stub_api_server(vec![
            core_discovery(),
            (
                "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/pods/*/qps",
                body,
            ),
        ])
        .await;
        let (info, timestamp) = client_for(url)
            .get_raw_metric("qps", "ns", &pod_selector("web"), "env=prod")
            .await
            .unwrap();
        let reqs = seen.lock().unwrap().clone();
        // The first request is the REST-mapper discovery of `Pod`.
        let reqs = &reqs[reqs.len() - 1..];
        assert_eq!(
            reqs[0].1.get("labelSelector").map(String::as_str),
            Some("app=web")
        );
        assert_eq!(
            reqs[0].1.get("metricLabelSelector").map(String::as_str),
            Some("env=prod")
        );
        assert_eq!(info["web-0"].window, std::time::Duration::from_secs(30));
        assert_eq!(info["web-1"].window, METRIC_SERVER_DEFAULT_METRIC_WINDOW);
        assert_eq!(info["web-0"].timestamp, ts("2026-01-01T00:00:10Z"));
        assert_eq!(info["web-1"].timestamp, ts("2026-01-01T00:00:20Z"));
        // client.go:147-149: the returned timestamp is `Items[0].Timestamp`.
        assert_eq!(timestamp, ts("2026-01-01T00:00:10Z"));
    }

    /// An empty selector is not sent at all (`VersionedParams` omits empty
    /// `omitempty` fields).
    #[tokio::test]
    async fn raw_metric_omits_empty_metric_selector() {
        let body = serde_json::json!({"kind": "MetricValueList", "items": [
            metric_item("web-0", "5", "2026-01-01T00:00:10Z", None),
        ]});
        let (url, seen) = stub_api_server(vec![
            core_discovery(),
            (
                "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/pods/*/qps",
                body,
            ),
        ])
        .await;
        client_for(url)
            .get_raw_metric("qps", "ns", &pod_selector("web"), "")
            .await
            .unwrap();
        let reqs = seen.lock().unwrap().clone();
        assert!(!reqs[0].1.contains_key("metricLabelSelector"));
    }

    /// client.go:122-128 error texts.
    #[tokio::test]
    async fn raw_metric_error_texts_match_upstream() {
        let (url, _) = stub_api_server(vec![
            core_discovery(),
            (
                "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/pods/*/qps",
                serde_json::json!({"kind": "MetricValueList", "items": []}),
            ),
        ])
        .await;
        let c = client_for(url);
        let err = c
            .get_raw_metric("qps", "ns", &pod_selector("web"), "")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "no metrics returned from custom metrics API"
        );
        let err = c
            .get_raw_metric("other", "ns", &pod_selector("web"), "")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .starts_with("unable to fetch metrics from custom metrics API: "),
            "got: {err}"
        );
    }

    /// `GetObjectMetric` resolves the plural resource through the REST mapper
    /// (`qualResourceForKind`, versioned_client.go:84-92 -> `deployments.apps`)
    /// and sends `metricLabelSelector` (versioned_client.go:196-205).
    #[tokio::test]
    async fn object_metric_resolves_resource_via_discovery() {
        let mut routes = apps_discovery();
        routes.push((
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/deployments.apps/web/qps",
            serde_json::json!({"kind": "MetricValueList", "items": [
                metric_item("web", "9", "2026-01-01T00:00:10Z", None),
            ]}),
        ));
        let (url, seen) = stub_api_server(routes).await;
        let obj = CrossVersionObjectReference {
            kind: "Deployment".into(),
            name: "web".into(),
            api_version: Some("apps/v1".into()),
        };
        let (value, stamp) = client_for(url)
            .get_object_metric("qps", "ns", &obj, "env=prod")
            .await
            .unwrap();
        assert_eq!(value, 9000);
        assert_eq!(stamp, ts("2026-01-01T00:00:10Z"));
        let reqs = seen.lock().unwrap().clone();
        let metric_req = reqs.last().unwrap();
        assert_eq!(
            metric_req.0,
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/deployments.apps/web/qps"
        );
        assert_eq!(
            metric_req.1.get("metricLabelSelector").map(String::as_str),
            Some("env=prod")
        );
    }

    /// `qualResourceForKind` failure text (versioned_client.go:86) wrapped by
    /// client.go:152; the mapper's `NoKindMatchError` for a group search.
    #[tokio::test]
    async fn object_metric_unmappable_kind_errors_like_upstream() {
        let (url, _) = stub_api_server(apps_discovery()).await;
        let obj = CrossVersionObjectReference {
            kind: "Widget".into(),
            name: "w".into(),
            api_version: Some("apps/v1".into()),
        };
        let err = client_for(url)
            .get_object_metric("qps", "ns", &obj, "")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "unable to fetch metrics from custom metrics API: unable to map kind Widget.apps to resource: no matches for kind \"Widget\" in group \"apps\""
        );
    }

    /// A Namespace object reads `namespaces/{ns}/metrics/{metric}` and still
    /// carries `metricLabelSelector` (versioned_client.go:103-133).
    #[tokio::test]
    async fn namespace_object_metric_sends_metric_selector() {
        let (url, seen) = stub_api_server(vec![(
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/metrics/qps",
            serde_json::json!({"kind": "MetricValueList", "items": [
                metric_item("ns", "3", "2026-01-01T00:00:10Z", None),
            ]}),
        )])
        .await;
        let obj = CrossVersionObjectReference {
            kind: "Namespace".into(),
            name: "ns".into(),
            api_version: Some("v1".into()),
        };
        client_for(url)
            .get_object_metric("qps", "ns", &obj, "env=prod")
            .await
            .unwrap();
        let reqs = seen.lock().unwrap().clone();
        assert_eq!(
            reqs[0].1.get("metricLabelSelector").map(String::as_str),
            Some("env=prod")
        );
    }

    /// `NamespacedMetrics(ns).List(metricName, selector)`
    /// (external_metrics/client.go:84-92) sends `labelSelector`; the
    /// timestamp is `Items[0]`'s (metrics/client.go:224-226); error texts
    /// client.go:210-216.
    #[tokio::test]
    async fn external_metric_sends_label_selector_and_uses_first_timestamp() {
        let items = serde_json::json!({"kind": "ExternalMetricValueList", "items": [
            {"metricName": "qps", "timestamp": "2026-01-01T00:00:30Z", "value": "4"},
            {"metricName": "qps", "timestamp": "2026-01-01T00:00:10Z", "value": "6"},
        ]});
        let (url, seen) = stub_api_server(vec![
            (
                "/apis/external.metrics.k8s.io/v1beta1/namespaces/ns/qps",
                items,
            ),
            (
                "/apis/external.metrics.k8s.io/v1beta1/namespaces/ns/empty",
                serde_json::json!({"kind": "ExternalMetricValueList", "items": []}),
            ),
        ])
        .await;
        let c = client_for(url);
        let (values, stamp) = c
            .get_external_metric("qps", "ns", "env=prod")
            .await
            .unwrap();
        assert_eq!(values, vec![4000, 6000]);
        assert_eq!(stamp, ts("2026-01-01T00:00:30Z"));
        let reqs = seen.lock().unwrap().clone();
        assert_eq!(
            reqs[0].1.get("labelSelector").map(String::as_str),
            Some("env=prod")
        );
        let err = c.get_external_metric("empty", "ns", "").await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "no metrics returned from external metrics API"
        );
        let err = c.get_external_metric("nope", "ns", "").await.unwrap_err();
        assert!(
            err.to_string()
                .starts_with("unable to fetch metrics from external metrics API: "),
            "got: {err}"
        );
    }
}
