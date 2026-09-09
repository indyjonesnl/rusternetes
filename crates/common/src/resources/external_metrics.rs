// External Metrics API Resources (external.metrics.k8s.io/v1beta1)
//
// Mirrors k8s.io/metrics/pkg/apis/external_metrics/v1beta1. The HPA controller
// reads these for the `External` metric type — values not associated with any
// Kubernetes object (e.g. a cloud queue depth).
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// ExternalMetricValue is a single value for a global metric.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMetricValue {
    pub api_version: String,
    pub kind: String,
    /// The name of the metric.
    pub metric_name: String,
    /// The labels for the metric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_labels: Option<BTreeMap<String, String>>,
    #[serde(
        serialize_with = "crate::types::k8s_time_required::serialize",
        deserialize_with = "crate::types::k8s_time_required::deserialize"
    )]
    pub timestamp: DateTime<Utc>,
    /// Window over which the value was produced, e.g. "60s".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<String>,
    /// The value of the metric as a quantity string.
    pub value: String,
}

/// ExternalMetricValueList is a list of values for a global metric.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMetricValueList {
    pub api_version: String,
    pub kind: String,
    pub metadata: super::custom_metrics::ListMetadata,
    pub items: Vec<ExternalMetricValue>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn external_metric_value_list_round_trips() {
        let list = ExternalMetricValueList {
            api_version: "external.metrics.k8s.io/v1beta1".to_string(),
            kind: "ExternalMetricValueList".to_string(),
            metadata: super::super::custom_metrics::ListMetadata { self_link: None },
            items: vec![ExternalMetricValue {
                api_version: "external.metrics.k8s.io/v1beta1".to_string(),
                kind: "ExternalMetricValue".to_string(),
                metric_name: "queue_depth".to_string(),
                metric_labels: Some(BTreeMap::from([(
                    "queue".to_string(),
                    "worker".to_string(),
                )])),
                timestamp: fixed_time(),
                window: Some("60s".to_string()),
                value: "42".to_string(),
            }],
        };
        let json = serde_json::to_string(&list).unwrap();
        // camelCase field names per K8s API contract.
        assert!(json.contains("\"metricName\":\"queue_depth\""));
        assert!(json.contains("\"metricLabels\""));
        let back: ExternalMetricValueList = serde_json::from_str(&json).unwrap();
        assert_eq!(list, back);
    }
}
