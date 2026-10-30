//! Unit tests moved from `src/resources/custom_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::types::{ListMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::BTreeMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::custom_metrics::*;

    /// A whole-second timestamp. `Utc::now()` carries nanoseconds, which these
    /// fields no longer round-trip: they are `metav1.Time` upstream and now
    /// serialize with `time.RFC3339` (second precision), so a nanosecond input
    /// legitimately comes back truncated. Using a second-precision value keeps
    /// the round-trip assertion about the round trip rather than about the
    /// precision loss.
    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-08T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn test_metric_value_serialization() {
        let metric_value = MetricValue {
            type_meta: TypeMeta {
                api_version: "custom.metrics.k8s.io/v1beta2".to_string(),
                kind: "MetricValue".to_string(),
            },
            described_object: ObjectReference {
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "test-pod".to_string(),
                api_version: Some("v1".to_string()),
            },
            metric: MetricIdentifier {
                name: "http_requests_per_second".to_string(),
                selector: Some(MetricSelector {
                    match_labels: Some(BTreeMap::from([("app".to_string(), "nginx".to_string())])),
                    match_expressions: None,
                }),
            },
            timestamp: fixed_time(),
            window_seconds: Some(60),
            value: "100".to_string(),
        };

        let json = serde_json::to_string(&metric_value).unwrap();
        let deserialized: MetricValue = serde_json::from_str(&json).unwrap();
        assert_eq!(metric_value, deserialized);
    }

    #[test]
    fn test_metric_value_list_serialization() {
        let metric_value_list = MetricValueList {
            type_meta: TypeMeta {
                api_version: "custom.metrics.k8s.io/v1beta2".to_string(),
                kind: "MetricValueList".to_string(),
            },
            metadata: ListMeta::default(),
            items: vec![MetricValue {
                type_meta: TypeMeta {
                    api_version: "custom.metrics.k8s.io/v1beta2".to_string(),
                    kind: "MetricValue".to_string(),
                },
                described_object: ObjectReference {
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "test-pod-1".to_string(),
                    api_version: Some("v1".to_string()),
                },
                metric: MetricIdentifier {
                    name: "http_requests_per_second".to_string(),
                    selector: None,
                },
                timestamp: fixed_time(),
                window_seconds: Some(60),
                value: "100".to_string(),
            }],
        };

        let json = serde_json::to_string(&metric_value_list).unwrap();
        let deserialized: MetricValueList = serde_json::from_str(&json).unwrap();
        assert_eq!(metric_value_list, deserialized);
    }

    #[test]
    fn test_metric_value_fields() {
        let metric_value = MetricValue {
            type_meta: TypeMeta {
                api_version: "custom.metrics.k8s.io/v1beta2".to_string(),
                kind: "MetricValue".to_string(),
            },
            described_object: ObjectReference {
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "test-pod".to_string(),
                api_version: None,
            },
            metric: MetricIdentifier {
                name: "http_requests".to_string(),
                selector: None,
            },
            timestamp: fixed_time(),
            window_seconds: None,
            value: "50".to_string(),
        };

        assert_eq!(
            metric_value.type_meta.api_version,
            "custom.metrics.k8s.io/v1beta2"
        );
        assert_eq!(metric_value.type_meta.kind, "MetricValue");
        assert_eq!(metric_value.described_object.name, "test-pod");
        assert_eq!(
            metric_value.described_object.namespace,
            Some("default".to_string())
        );
    }

    /// `MetricIdentifier.selector` is `json:"selector"` (no omitempty) and
    /// `WindowSeconds` is `omitempty` (`custom_metrics/v1beta2/types.go`).
    #[test]
    fn metric_value_wire_shape_matches_v1beta2() {
        let value = MetricValue {
            type_meta: TypeMeta::default(),
            described_object: ObjectReference {
                kind: "Namespace".to_string(),
                namespace: None,
                name: "ns".to_string(),
                api_version: None,
            },
            metric: MetricIdentifier {
                name: "qps".to_string(),
                selector: None,
            },
            timestamp: fixed_time(),
            window_seconds: None,
            value: "1".to_string(),
        };
        let json = serde_json::to_value(&value).unwrap();
        assert_eq!(json["metric"]["name"], "qps");
        assert!(json["metric"]["selector"].is_null());
        assert!(json["metric"].as_object().unwrap().contains_key("selector"));
        assert!(json.get("windowSeconds").is_none());
        assert!(json.get("metricName").is_none());
        assert!(json["describedObject"].get("namespace").is_none());
    }
}
