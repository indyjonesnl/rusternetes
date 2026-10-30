// External Metrics API Resources (external.metrics.k8s.io/v1beta1)
//
// Mirrors k8s.io/metrics/pkg/apis/external_metrics/v1beta1. The HPA controller
// reads these for the `External` metric type — values not associated with any
// Kubernetes object (e.g. a cloud queue depth).
use crate::types::{ListMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// ExternalMetricValue is a single value for a global metric.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMetricValue {
    /// `metav1.TypeMeta` (`metrics/pkg/apis/external_metrics/types.go:40-41`);
    /// no ObjectMeta upstream. Required-at-decode before #1916.
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    /// The name of the metric.
    #[serde(default)]
    pub metric_name: String,
    /// The labels for the metric. `MetricLabels map[string]string
    /// json:"metricLabels"` has no omitempty: a nil map is `null`
    /// (`external_metrics/v1beta1/types.go`).
    #[serde(default)]
    pub metric_labels: Option<BTreeMap<String, String>>,
    #[serde(
        default = "zero_time",
        serialize_with = "crate::types::k8s_time_required::serialize",
        deserialize_with = "crate::types::k8s_time_required::deserialize"
    )]
    pub timestamp: DateTime<Utc>,
    /// `WindowSeconds *int64 json:"window,omitempty"`: the window in seconds
    /// (an integer on the wire, not a duration string).
    #[serde(rename = "window", default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<i64>,
    /// The value of the metric as a quantity string.
    #[serde(default)]
    pub value: String,
}

/// ExternalMetricValueList is a list of values for a global metric.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMetricValueList {
    /// `metav1.TypeMeta` + `metav1.ListMeta`, as the custom-metrics list
    /// (`metrics/pkg/apis/external_metrics/types.go:62-69`).
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ListMeta,
    #[serde(default)]
    pub items: Vec<ExternalMetricValue>,
}

/// The Go zero `metav1.Time`, which is what an absent `timestamp` decodes to
/// upstream. These are the response types of a read-only aggregated API —
/// nothing upstream validates or creates them — so the absent case has to
/// decode rather than be rejected.
fn zero_time() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch is a valid timestamp")
}
