//! The metrics APIs must decode a body that omits `apiVersion`/`kind`, and must
//! carry the real `ObjectMeta` upstream gives them.
//!
//! `staging/src/k8s.io/metrics/pkg/apis/metrics/types.go:31-45`:
//!
//! ```go
//! type NodeMetrics struct {
//!     metav1.TypeMeta
//!     // Standard object's metadata.
//!     // +optional
//!     metav1.ObjectMeta
//!
//!     Timestamp metav1.Time
//!     Window    metav1.Duration
//!     Usage corev1.ResourceList
//! }
//! ```
//!
//! Rusternetes declared `api_version: String` / `kind: String` with no
//! `#[serde(default)]` — so a body omitting them failed to decode, the trap
//! that made a created CRD unreadable (#1913) — and a bespoke three-field
//! `NodeMetricsMetadata` / `PodMetricsMetadata`, so labels, annotations, uid
//! and resourceVersion were silently dropped and every generic metadata rule
//! skipped these types (#1916).

use rusternetes_common::resources::{
    ExternalMetricValue, ExternalMetricValueList, MetricValue, MetricValueList, NodeMetrics,
    PodMetrics,
};
use serde_json::json;

#[test]
fn node_metrics_decodes_without_type_meta_and_keeps_full_object_meta() {
    let node: NodeMetrics = serde_json::from_value(json!({
        "metadata": {
            "name": "node-1",
            "uid": "11111111-2222-3333-4444-555555555555",
            "resourceVersion": "42",
            "labels": { "kubernetes.io/hostname": "node-1" },
            "annotations": { "example.com/note": "kept" },
            "creationTimestamp": "2026-09-08T10:00:00Z",
        },
        "timestamp": "2026-09-08T10:05:00Z",
        "window": "30s",
        "usage": { "cpu": "100m", "memory": "1Gi" },
    }))
    .expect("a body without apiVersion/kind must decode");

    assert_eq!(node.metadata.name, "node-1");
    assert_eq!(node.metadata.uid, "11111111-2222-3333-4444-555555555555");
    assert_eq!(node.metadata.resource_version.as_deref(), Some("42"));
    assert_eq!(
        node.metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/hostname"))
            .map(String::as_str),
        Some("node-1"),
        "labels must survive: upstream NodeMetrics carries a real ObjectMeta"
    );
    assert_eq!(
        node.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("example.com/note"))
            .map(String::as_str),
        Some("kept")
    );
    assert!(node.metadata.creation_timestamp.is_some());
}

#[test]
fn pod_metrics_decodes_without_type_meta_and_keeps_its_namespace() {
    let pod: PodMetrics = serde_json::from_value(json!({
        "metadata": { "name": "web-0", "namespace": "kube-system", "labels": { "app": "web" } },
        "timestamp": "2026-09-08T10:05:00Z",
        "window": "30s",
        "containers": [{ "name": "web", "usage": { "cpu": "1m" } }],
    }))
    .expect("a body without apiVersion/kind must decode");

    assert_eq!(pod.metadata.name, "web-0");
    assert_eq!(pod.metadata.namespace.as_deref(), Some("kube-system"));
    assert_eq!(
        pod.metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("app"))
            .map(String::as_str),
        Some("web")
    );
}

/// `custom_metrics.MetricValue` and `external_metrics.ExternalMetricValue`
/// carry `metav1.TypeMeta` and no `ObjectMeta`
/// (`custom_metrics/types.go:49-68`, `external_metrics/types.go:40-59`), so only
/// the decode half applies to them.
#[test]
fn the_custom_and_external_metric_values_decode_without_type_meta() {
    let value: MetricValue = serde_json::from_value(json!({
        "describedObject": { "kind": "Pod", "name": "web-0", "namespace": "default" },
        "metricName": "requests-per-second",
        "timestamp": "2026-09-08T10:05:00Z",
        "value": "10",
    }))
    .expect("MetricValue must decode without apiVersion/kind");
    assert_eq!(value.metric_name, "requests-per-second");

    let external: ExternalMetricValue = serde_json::from_value(json!({
        "metricName": "queue-depth",
        "timestamp": "2026-09-08T10:05:00Z",
        "value": "7",
    }))
    .expect("ExternalMetricValue must decode without apiVersion/kind");
    assert_eq!(external.value, "7");
}

/// Both lists carry `metav1.ListMeta` upstream
/// (`custom_metrics/types.go:38-44`), not a one-field bespoke struct — so a
/// `resourceVersion` on the list survives.
#[test]
fn the_metric_lists_carry_a_real_list_meta() {
    let list: MetricValueList = serde_json::from_value(json!({
        "metadata": { "resourceVersion": "7" },
        "items": [],
    }))
    .expect("MetricValueList must decode without apiVersion/kind");
    assert_eq!(list.metadata.resource_version.as_deref(), Some("7"));

    let list: ExternalMetricValueList = serde_json::from_value(json!({
        "metadata": { "resourceVersion": "9" },
        "items": [],
    }))
    .expect("ExternalMetricValueList must decode without apiVersion/kind");
    assert_eq!(list.metadata.resource_version.as_deref(), Some("9"));
}

/// The wire form still names the type: a response must carry apiVersion/kind,
/// which is what the flattened `TypeMeta` keeps doing.
#[test]
fn a_served_node_metrics_still_names_its_type() {
    let node: NodeMetrics = serde_json::from_value(json!({
        "apiVersion": "metrics.k8s.io/v1beta1",
        "kind": "NodeMetrics",
        "metadata": { "name": "node-1" },
        "timestamp": "2026-09-08T10:05:00Z",
        "window": "30s",
        "usage": {},
    }))
    .expect("decode");
    let wire = serde_json::to_value(&node).expect("encode");
    assert_eq!(wire["apiVersion"], json!("metrics.k8s.io/v1beta1"));
    assert_eq!(wire["kind"], json!("NodeMetrics"));
    assert_eq!(wire["metadata"]["name"], json!("node-1"));
}
