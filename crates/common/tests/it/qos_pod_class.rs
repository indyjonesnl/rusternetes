//! `rusternetes_common::qos` — the one port of upstream
//! `pkg/apis/core/v1/helper/qos/qos.go`.
//!
//! [`compute_pod_qos`] (upstream `ComputePodQOS`) is covered in depth from the
//! kubelet side (`crates/kubelet/tests/it/coverage_qos.rs`) and through the API
//! surface (`crates/api-server/tests/it/pod_qos_class_parity_test.rs`). What is
//! pinned here is the reader entry point [`get_pod_qos`] (upstream `GetPodQOS`,
//! qos.go:37-44), whose whole job is to prefer the *published*
//! `status.qosClass` over recomputing it — that preference is what makes the
//! ResourceQuota `BestEffort` scope and the CPU-resize gate agree with the
//! class the api-server wrote.

use rusternetes_common::qos::{compute_pod_qos, get_pod_qos, QoSClass};
use rusternetes_common::resources::Pod;
use serde_json::json;

fn pod(value: serde_json::Value) -> Pod {
    serde_json::from_value(value).expect("pod fixture must decode")
}

/// A Guaranteed-by-spec pod with no status at all: nothing to read, so
/// `GetPodQOS` falls through to `ComputePodQOS`.
#[test]
fn get_pod_qos_computes_when_status_is_absent() {
    let p = pod(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "no-status" },
        "spec": { "containers": [{
            "name": "c", "image": "busybox",
            "resources": {
                "limits":   { "cpu": "100m", "memory": "128Mi" },
                "requests": { "cpu": "100m", "memory": "128Mi" },
            },
        }] },
    }));
    assert_eq!(get_pod_qos(&p), QoSClass::Guaranteed);
    assert_eq!(compute_pod_qos(&p), QoSClass::Guaranteed);
}

/// The published class wins over the spec — upstream returns
/// `pod.Status.QOSClass` untouched whenever it is non-empty (qos.go:39-42).
/// `status.qosClass` is set once by the api-server on create and the pod's
/// resources cannot change class afterwards, so readers must not second-guess
/// it.
#[test]
fn get_pod_qos_prefers_published_status_over_the_spec() {
    let p = pod(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "published" },
        "spec": { "containers": [{
            "name": "c", "image": "busybox",
            "resources": {
                "limits":   { "cpu": "100m", "memory": "128Mi" },
                "requests": { "cpu": "100m", "memory": "128Mi" },
            },
        }] },
        "status": { "qosClass": "Burstable" },
    }));
    assert_eq!(get_pod_qos(&p), QoSClass::Burstable);
    assert_eq!(
        compute_pod_qos(&p),
        QoSClass::Guaranteed,
        "the writer-side entry point ignores status and derives from the spec"
    );
}

/// An unparseable `status.qosClass` is not a fourth class. Upstream, being a
/// typed string, hands the junk value back; a typed enum cannot, so it
/// recomputes rather than guessing.
#[test]
fn get_pod_qos_recomputes_for_an_unrecognised_status_value() {
    let p = pod(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "junk-status" },
        "spec": { "containers": [{
            "name": "c", "image": "busybox",
            "resources": { "requests": { "cpu": "100m" } },
        }] },
        "status": { "qosClass": "Sporadic" },
    }));
    assert_eq!(get_pod_qos(&p), QoSClass::Burstable);
}

/// A pod with no spec is BestEffort, not a panic (`ComputePodQOS` over empty
/// container lists: both maps stay empty, qos.go:156-158).
#[test]
fn get_pod_qos_handles_a_pod_without_a_spec() {
    let p = pod(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": "spec-less" },
    }));
    assert_eq!(get_pod_qos(&p), QoSClass::BestEffort);
}

/// `Ord` on the enum is the eviction comparator: BestEffort is evicted first,
/// Guaranteed last (`pkg/kubelet/eviction/helpers.go`'s `qosComparator`), so the
/// discriminant order is load-bearing and not merely cosmetic.
#[test]
fn qos_class_orders_besteffort_first() {
    let mut classes = [
        QoSClass::Guaranteed,
        QoSClass::BestEffort,
        QoSClass::Burstable,
    ];
    classes.sort();
    assert_eq!(
        classes,
        [
            QoSClass::BestEffort,
            QoSClass::Burstable,
            QoSClass::Guaranteed
        ]
    );
}

/// The three `v1.PodQOSClass` constant strings round-trip exactly — they are
/// serialised into `status.qosClass` and read back by every consumer.
#[test]
fn qos_class_strings_round_trip() {
    for class in [
        QoSClass::Guaranteed,
        QoSClass::Burstable,
        QoSClass::BestEffort,
    ] {
        assert_eq!(QoSClass::from_status_str(class.as_str()), Some(class));
    }
    assert_eq!(QoSClass::from_status_str("besteffort"), None);
    assert_eq!(QoSClass::from_status_str(""), None);
}
