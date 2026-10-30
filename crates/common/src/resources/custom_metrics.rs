// Custom Metrics API Resources (custom.metrics.k8s.io/v1beta2)
use crate::types::{ListMeta, TypeMeta};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// MetricValue represents a single metric value for a given object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricValue {
    /// `metav1.TypeMeta` (`metrics/pkg/apis/custom_metrics/types.go:49-50`).
    /// These were required at decode time, so a body that omitted them -- what
    /// every client sends inside a list -- failed to decode (#1916). Upstream
    /// gives MetricValue no ObjectMeta; the object it describes is
    /// `describedObject`.
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub described_object: ObjectReference,
    /// `Metric MetricIdentifier` (`custom_metrics/v1beta2/types.go`).
    #[serde(default)]
    pub metric: MetricIdentifier,
    #[serde(
        default = "zero_time",
        serialize_with = "crate::types::k8s_time_required::serialize",
        deserialize_with = "crate::types::k8s_time_required::deserialize"
    )]
    pub timestamp: DateTime<Utc>,
    /// `WindowSeconds *int64` (`json:"windowSeconds,omitempty"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<i64>,
    #[serde(default)]
    pub value: String,
}

/// `custom_metrics/v1beta2/types.go` `MetricIdentifier`. `selector` is
/// `json:"selector"` with no `omitempty`, so an unset one serializes as
/// `null`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct MetricIdentifier {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub selector: Option<MetricSelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ObjectReference {
    #[serde(default)]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default)]
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricSelector {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<BTreeMap<String, String>>,
    /// `metav1.LabelSelector.MatchExpressions`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<crate::types::LabelSelectorRequirement>>,
}

/// MetricValueList contains a list of metric values
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricValueList {
    /// `metav1.TypeMeta` + `metav1.ListMeta`
    /// (`metrics/pkg/apis/custom_metrics/types.go:38-44`). `metadata` used to be
    /// a bespoke one-field struct carrying only the long-deprecated `selfLink`,
    /// which upstream stopped populating in 1.20 (#1916).
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ListMeta,
    #[serde(default)]
    pub items: Vec<MetricValue>,
}

/// The Go zero `metav1.Time`, which is what an absent `timestamp` decodes to
/// upstream. These are the response types of a read-only aggregated API —
/// nothing upstream validates or creates them — so the absent case has to
/// decode rather than be rejected.
fn zero_time() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch is a valid timestamp")
}
