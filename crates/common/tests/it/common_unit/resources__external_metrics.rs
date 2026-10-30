//! Unit tests moved from `src/resources/external_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::types::{ListMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::BTreeMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::external_metrics::*;

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
            type_meta: TypeMeta {
                api_version: "external.metrics.k8s.io/v1beta1".to_string(),
                kind: "ExternalMetricValueList".to_string(),
            },
            metadata: ListMeta::default(),
            items: vec![ExternalMetricValue {
                type_meta: TypeMeta {
                    api_version: "external.metrics.k8s.io/v1beta1".to_string(),
                    kind: "ExternalMetricValue".to_string(),
                },
                metric_name: "queue_depth".to_string(),
                metric_labels: Some(BTreeMap::from([(
                    "queue".to_string(),
                    "worker".to_string(),
                )])),
                timestamp: fixed_time(),
                window_seconds: Some(60),
                value: "42".to_string(),
            }],
        };
        let json = serde_json::to_string(&list).unwrap();
        // camelCase field names per K8s API contract.
        assert!(json.contains("\"metricName\":\"queue_depth\""));
        assert!(json.contains("\"metricLabels\""));
        // `window` is an integer on the wire.
        assert!(json.contains("\"window\":60"));
        let back: ExternalMetricValueList = serde_json::from_str(&json).unwrap();
        assert_eq!(list, back);
    }
}
