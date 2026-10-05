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

// ---------------------------------------------------------------------------
// PodLevelResources branch - upstream `ComputePodQOS`, qos.go:97-112, an
// `if/else` against the container loop. With the gate on and `spec.resources`
// set, the pod is classified from the pod-level block ONLY; the containers are
// never consulted.
// ---------------------------------------------------------------------------

use rusternetes_common::feature_gates::{with_feature, Feature};

/// Pod-level limits for both cpu and memory, containers declaring nothing.
/// `SetDefaults_Pod` (defaults.go:196-199, `defaultPodRequests`) defaults the
/// pod-level requests to the pod-level limits, so the pod is Guaranteed.
#[test]
#[serial_test::serial]
fn pod_level_limits_only_is_guaranteed() {
    let p = pod(json!({
        "apiVersion": "v1", "kind": "Pod", "metadata": { "name": "pl-g" },
        "spec": {
            "resources": { "limits": { "cpu": "1", "memory": "1Gi" } },
            "containers": [{ "name": "c", "image": "busybox" }],
        },
    }));
    assert_eq!(compute_pod_qos(&p), QoSClass::Guaranteed);
}

/// Pod-level requests alone: no limits, so `isGuaranteed` stays true but
/// `len(requests) != len(limits)` - Burstable.
#[test]
#[serial_test::serial]
fn pod_level_requests_only_is_burstable() {
    let p = pod(json!({
        "apiVersion": "v1", "kind": "Pod", "metadata": { "name": "pl-b" },
        "spec": {
            "resources": { "requests": { "cpu": "1", "memory": "1Gi" } },
            "containers": [{ "name": "c", "image": "busybox" }],
        },
    }));
    assert_eq!(compute_pod_qos(&p), QoSClass::Burstable);
}

/// Pod-level limits missing memory: `!qosLimitResources.HasAll(memory, cpu)`
/// (qos.go:107-109) forfeits Guaranteed.
#[test]
#[serial_test::serial]
fn pod_level_limits_missing_memory_is_burstable() {
    let p = pod(json!({
        "apiVersion": "v1", "kind": "Pod", "metadata": { "name": "pl-cpu" },
        "spec": {
            "resources": { "limits": { "cpu": "1" } },
            "containers": [{ "name": "c", "image": "busybox" }],
        },
    }));
    assert_eq!(compute_pod_qos(&p), QoSClass::Burstable);
}

/// The branch is `if/else`, not additive: container resources are IGNORED when
/// `spec.resources` is set. The containers here are individually Guaranteed,
/// but the pod-level block only carries a cpu request, so the pod is Burstable.
#[test]
#[serial_test::serial]
fn pod_level_branch_ignores_container_resources() {
    let p = pod(json!({
        "apiVersion": "v1", "kind": "Pod", "metadata": { "name": "pl-ignore" },
        "spec": {
            "resources": { "requests": { "cpu": "1" } },
            "containers": [{
                "name": "c", "image": "busybox",
                "resources": {
                    "limits":   { "cpu": "1", "memory": "1Gi" },
                    "requests": { "cpu": "1", "memory": "1Gi" },
                },
            }],
        },
    }));
    assert_eq!(compute_pod_qos(&p), QoSClass::Burstable);
}

/// A present-but-empty `spec.resources` still takes the branch (`!= nil`), so
/// the pod is BestEffort even though its container is Guaranteed.
#[test]
#[serial_test::serial]
fn empty_pod_level_resources_is_best_effort_despite_containers() {
    let p = pod(json!({
        "apiVersion": "v1", "kind": "Pod", "metadata": { "name": "pl-empty" },
        "spec": {
            "resources": {},
            "containers": [{
                "name": "c", "image": "busybox",
                "resources": {
                    "limits":   { "cpu": "1", "memory": "1Gi" },
                    "requests": { "cpu": "1", "memory": "1Gi" },
                },
            }],
        },
    }));
    assert_eq!(compute_pod_qos(&p), QoSClass::BestEffort);
}

/// With the gate off the pod-level block is not consulted and the container
/// loop runs (qos.go:97 `Enabled(PodLevelResources) &&`).
#[test]
#[serial_test::serial]
fn gate_off_falls_back_to_the_container_loop() {
    let _gate = with_feature(Feature::PodLevelResources, false);
    let p = pod(json!({
        "apiVersion": "v1", "kind": "Pod", "metadata": { "name": "pl-off" },
        "spec": {
            "resources": { "requests": { "cpu": "1" } },
            "containers": [{
                "name": "c", "image": "busybox",
                "resources": {
                    "limits":   { "cpu": "1", "memory": "1Gi" },
                    "requests": { "cpu": "1", "memory": "1Gi" },
                },
            }],
        },
    }));
    assert_eq!(compute_pod_qos(&p), QoSClass::Guaranteed);
}
