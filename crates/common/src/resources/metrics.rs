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
    #[serde(default)]
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub window: String,
    #[serde(
        default,
        serialize_with = "crate::quantity::wire::serialize_btree_map",
        deserialize_with = "crate::quantity::wire::deserialize_btree_map"
    )]
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
    #[serde(default)]
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub window: String,
    #[serde(default)]
    pub containers: Vec<ContainerMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContainerMetrics {
    /// `metrics.k8s.io` is a read-only aggregated API with no upstream
    /// validation, so an absent name decodes rather than failing.
    #[serde(default)]
    pub name: String,
    #[serde(
        default,
        serialize_with = "crate::quantity::wire::serialize_btree_map",
        deserialize_with = "crate::quantity::wire::deserialize_btree_map"
    )]
    pub usage: BTreeMap<String, String>,
}
