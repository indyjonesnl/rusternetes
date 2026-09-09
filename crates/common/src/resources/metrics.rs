// Metrics API Resources (metrics.k8s.io/v1beta1)
use crate::types::{ObjectMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// NodeMetrics contains resource usage metrics for a node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeMetrics {
    /// `metav1.TypeMeta` + `metav1.ObjectMeta`, as upstream
    /// (`staging/src/k8s.io/metrics/pkg/apis/metrics/types.go:31-45`). Both were
    /// bespoke here: `apiVersion`/`kind` were required at decode time, and
    /// `metadata` was a three-field struct that dropped labels, annotations,
    /// uid and resourceVersion (#1916).
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(
        serialize_with = "crate::types::k8s_time_required::serialize",
        deserialize_with = "crate::types::k8s_time_required::deserialize"
    )]
    pub timestamp: DateTime<Utc>,
    pub window: String,
    pub usage: BTreeMap<String, String>,
}

/// PodMetrics contains resource usage metrics for a pod
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodMetrics {
    /// See [`NodeMetrics`] — same upstream shape
    /// (`metrics/pkg/apis/metrics/types.go:60-77`).
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(
        serialize_with = "crate::types::k8s_time_required::serialize",
        deserialize_with = "crate::types::k8s_time_required::deserialize"
    )]
    pub timestamp: DateTime<Utc>,
    pub window: String,
    pub containers: Vec<ContainerMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContainerMetrics {
    pub name: String,
    pub usage: BTreeMap<String, String>,
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
