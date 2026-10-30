//! Unit tests moved from `src/prometheus_client.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use prometheus_http_query::Client;
    pub use rusternetes_common::label_selector::{LabelOperator, LabelSelector};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::sync::RwLock;
    pub use tracing::{debug, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::prometheus_client::*;

    #[test]
    fn test_extract_resource_name_label() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        assert_eq!(
            client.extract_resource_name_label("pods"),
            Some("pod".to_string())
        );
        assert_eq!(
            client.extract_resource_name_label("services"),
            Some("service".to_string())
        );
        assert_eq!(
            client.extract_resource_name_label("deployments"),
            Some("deployment".to_string())
        );
        assert_eq!(
            client.extract_resource_name_label("nodes"),
            Some("node".to_string())
        );
    }

    #[test]
    fn test_build_object_query() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let query =
            client.build_object_query("http_requests_total", "default", "pods", "nginx-pod", &[]);

        assert!(query.contains("namespace=\"default\""));
        assert!(query.contains("pod=\"nginx-pod\""));
        assert!(query.contains("http_requests_total"));
    }

    #[test]
    fn test_build_object_query_with_labels() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let labels =
            promql_matchers(&LabelSelector::parse("app=nginx,env=production").unwrap()).unwrap();

        let query = client.build_object_query(
            "custom_metric",
            "kube-system",
            "services",
            "kube-dns",
            &labels,
        );

        assert!(query.contains("namespace=\"kube-system\""));
        assert!(query.contains("service=\"kube-dns\""));
        assert!(query.contains("app=\"nginx\""));
        assert!(query.contains("env=\"production\""));
    }

    #[test]
    fn external_series_carry_their_labels_without_the_metric_name() {
        let metric: HashMap<String, String> = [
            ("__name__", "queue_depth"),
            ("queue", "worker"),
            ("namespace", "default"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let s = external_series_from_sample(&metric, "42");
        assert_eq!(s.value, "42");
        assert_eq!(s.labels.len(), 2);
        assert_eq!(s.labels["queue"], "worker");
        assert!(!s.labels.contains_key("__name__"));
    }

    #[test]
    fn promql_matchers_cover_set_based_requirements() {
        let m = |s: &str| promql_matchers(&LabelSelector::parse(s).unwrap()).unwrap();
        assert_eq!(m("a=b"), ["a=\"b\""]);
        assert_eq!(m("a!=b"), ["a!=\"b\""]);
        assert_eq!(m("a in (x,y.z)"), ["a=~\"x|y\\\\.z\""]);
        assert_eq!(m("a notin (x)"), ["a!~\"x\""]);
        assert_eq!(m("a"), ["a=~\".+\""]);
        assert_eq!(m("!a"), ["a=\"\""]);
        assert!(promql_matchers(&LabelSelector::parse("a>1").unwrap()).is_err());
    }

    #[test]
    fn test_build_list_query() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let query = client.build_list_query("memory_usage_bytes", "default", "pods", &[]);

        assert!(query.contains("namespace=\"default\""));
        assert!(query.contains("memory_usage_bytes"));
    }

    #[tokio::test]
    async fn test_cache_value_and_get() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let query = "test_metric{namespace=\"default\"}";
        let value = "42.5";

        // Cache the value
        client
            .cache_value(query, value, Duration::from_secs(60))
            .await;

        // Should retrieve from cache
        let cached = client.get_cached(query).await;
        assert_eq!(cached, Some(value.to_string()));
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let client = PrometheusClient::new("http://localhost:9090").unwrap();

        let query = "test_metric{namespace=\"default\"}";
        let value = "42.5";

        // Cache with very short TTL
        client
            .cache_value(query, value, Duration::from_millis(10))
            .await;

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Should not retrieve from cache
        let cached = client.get_cached(query).await;
        assert_eq!(cached, None);
    }
}
