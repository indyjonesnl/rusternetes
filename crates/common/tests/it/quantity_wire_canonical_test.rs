//! Quantity fields must canonicalise on serialise and reject garbage on
//! decode, like Go `resource.Quantity` (`MarshalJSON` / `UnmarshalJSON`,
//! `staging/src/k8s.io/apimachinery/pkg/api/resource/quantity.go:694`,
//! `:733`). Issue #2956.

use rusternetes_common::resources::{CSIStorageCapacity, PersistentVolume, Pod};
use serde_json::json;

fn roundtrip<T: serde::de::DeserializeOwned + serde::Serialize>(
    v: serde_json::Value,
) -> serde_json::Value {
    serde_json::to_value(serde_json::from_value::<T>(v).expect("decode")).unwrap()
}

#[test]
fn pv_capacity_serialises_canonical() {
    let out = roundtrip::<PersistentVolume>(json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": "pv"},
        "spec": {"capacity": {"storage": "1024Mi"}, "accessModes": ["ReadWriteOnce"]}
    }));
    assert_eq!(out["spec"]["capacity"]["storage"], "1Gi");
}

#[test]
fn pv_capacity_rejects_bogus() {
    let r = serde_json::from_value::<PersistentVolume>(json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": "pv"},
        "spec": {"capacity": {"storage": "bogus"}}
    }));
    assert!(r.is_err());
}

#[test]
fn resource_requirements_serialise_canonical_and_keep_zero() {
    let out = roundtrip::<Pod>(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p"},
        "spec": {"containers": [{"name": "c", "image": "i", "resources": {
            "requests": {"cpu": "1000m", "memory": "0Mi", "x": "0.5"}
        }}]}
    }));
    let r = &out["spec"]["containers"][0]["resources"]["requests"];
    assert_eq!(r["cpu"], "1");
    assert_eq!(r["memory"], "0");
    assert_eq!(r["x"], "500m");
}

#[test]
fn empty_dir_size_limit_canonical_and_validated() {
    let out = roundtrip::<Pod>(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p"},
        "spec": {"containers": [], "volumes": [{"name": "v", "emptyDir": {"sizeLimit": "1024Mi"}}]}
    }));
    assert_eq!(out["spec"]["volumes"][0]["emptyDir"]["sizeLimit"], "1Gi");
    assert!(serde_json::from_value::<Pod>(json!({
        "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
        "spec": {"containers": [], "volumes": [{"name": "v", "emptyDir": {"sizeLimit": "bogus"}}]}
    }))
    .is_err());
}

#[test]
fn csi_storage_capacity_canonical() {
    let out = roundtrip::<CSIStorageCapacity>(json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "CSIStorageCapacity",
        "metadata": {"name": "c"}, "storageClassName": "sc",
        "capacity": "1024Mi", "maximumVolumeSize": "2048Mi"
    }));
    assert_eq!(out["capacity"], "1Gi");
    assert_eq!(out["maximumVolumeSize"], "2Gi");
}

// #3146 item 2: the remaining `resource.Quantity` fields.

#[test]
fn downward_api_divisor_canonical_and_validated() {
    use rusternetes_common::resources::pod::ResourceFieldSelector;
    let out = roundtrip::<ResourceFieldSelector>(
        json!({"resource": "limits.memory", "divisor": "1024Mi"}),
    );
    assert_eq!(out["divisor"], "1Gi");
    assert!(serde_json::from_value::<ResourceFieldSelector>(
        json!({"resource": "limits.memory", "divisor": "bogus"})
    )
    .is_err());
    // absent stays absent
    let out = roundtrip::<ResourceFieldSelector>(json!({"resource": "limits.cpu"}));
    assert!(out.get("divisor").is_none());
}

#[test]
fn runtime_class_pod_fixed_canonical_and_validated() {
    use rusternetes_common::resources::runtimeclass::RuntimeClass;
    let out = roundtrip::<RuntimeClass>(json!({
        "apiVersion": "node.k8s.io/v1", "kind": "RuntimeClass",
        "metadata": {"name": "rc"}, "handler": "h",
        "overhead": {"podFixed": {"cpu": "1000m", "memory": "1024Mi"}}
    }));
    assert_eq!(out["overhead"]["podFixed"]["cpu"], "1");
    assert_eq!(out["overhead"]["podFixed"]["memory"], "1Gi");
    assert!(serde_json::from_value::<RuntimeClass>(json!({
        "apiVersion": "node.k8s.io/v1", "kind": "RuntimeClass",
        "metadata": {"name": "rc"}, "handler": "h",
        "overhead": {"podFixed": {"cpu": "bogus"}}
    }))
    .is_err());
}

#[test]
fn metrics_usage_canonical_and_validated() {
    use rusternetes_common::resources::metrics::PodMetrics;
    let _ = std::any::type_name::<PodMetrics>();
    let out = roundtrip::<PodMetrics>(json!({
        "metadata": {"name": "p"}, "window": "15s",
        "containers": [{"name": "c", "usage": {"cpu": "1000m", "memory": "1024Mi"}}]
    }));
    assert_eq!(out["containers"][0]["usage"]["cpu"], "1");
    assert_eq!(out["containers"][0]["usage"]["memory"], "1Gi");
    assert!(serde_json::from_value::<PodMetrics>(json!({
        "metadata": {"name": "p"},
        "containers": [{"name": "c", "usage": {"cpu": "bogus"}}]
    }))
    .is_err());
}
