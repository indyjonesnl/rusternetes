//! Unit tests moved from `src/resources/metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::BTreeMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::metrics::*;

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
    fn test_node_metrics_serialization() {
        let node_metrics = NodeMetrics {
            type_meta: TypeMeta {
                api_version: "metrics.k8s.io/v1beta1".to_string(),
                kind: "NodeMetrics".to_string(),
            },
            metadata: ObjectMeta {
                creation_timestamp: Some(fixed_time()),
                ..ObjectMeta::new("node-1")
            },
            timestamp: fixed_time(),
            window: "30s".to_string(),
            usage: BTreeMap::from([
                ("cpu".to_string(), "100m".to_string()),
                ("memory".to_string(), "1Gi".to_string()),
            ]),
        };

        let json = serde_json::to_string(&node_metrics).unwrap();
        let deserialized: NodeMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(node_metrics, deserialized);
    }

    #[test]
    fn test_pod_metrics_serialization() {
        let pod_metrics = PodMetrics {
            type_meta: TypeMeta {
                api_version: "metrics.k8s.io/v1beta1".to_string(),
                kind: "PodMetrics".to_string(),
            },
            metadata: ObjectMeta {
                creation_timestamp: Some(fixed_time()),
                ..ObjectMeta::new("test-pod").with_namespace("default")
            },
            timestamp: fixed_time(),
            window: "30s".to_string(),
            containers: vec![ContainerMetrics {
                name: "nginx".to_string(),
                usage: BTreeMap::from([
                    ("cpu".to_string(), "50m".to_string()),
                    ("memory".to_string(), "512Mi".to_string()),
                ]),
            }],
        };

        let json = serde_json::to_string(&pod_metrics).unwrap();
        let deserialized: PodMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(pod_metrics, deserialized);
    }

    #[test]
    fn test_node_metrics_fields() {
        let node_metrics = NodeMetrics {
            type_meta: TypeMeta {
                api_version: "metrics.k8s.io/v1beta1".to_string(),
                kind: "NodeMetrics".to_string(),
            },
            metadata: ObjectMeta::new("node-1"),
            timestamp: fixed_time(),
            window: "30s".to_string(),
            usage: BTreeMap::new(),
        };

        assert_eq!(node_metrics.type_meta.api_version, "metrics.k8s.io/v1beta1");
        assert_eq!(node_metrics.type_meta.kind, "NodeMetrics");
        assert_eq!(node_metrics.metadata.name, "node-1");
    }

    #[test]
    fn test_pod_metrics_fields() {
        let pod_metrics = PodMetrics {
            type_meta: TypeMeta {
                api_version: "metrics.k8s.io/v1beta1".to_string(),
                kind: "PodMetrics".to_string(),
            },
            metadata: ObjectMeta::new("test-pod").with_namespace("default"),
            timestamp: fixed_time(),
            window: "30s".to_string(),
            containers: vec![],
        };

        assert_eq!(pod_metrics.type_meta.api_version, "metrics.k8s.io/v1beta1");
        assert_eq!(pod_metrics.type_meta.kind, "PodMetrics");
        assert_eq!(pod_metrics.metadata.name, "test-pod");
        assert_eq!(pod_metrics.metadata.namespace.as_deref(), Some("default"));
    }
}
