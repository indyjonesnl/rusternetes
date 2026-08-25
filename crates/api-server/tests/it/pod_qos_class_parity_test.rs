//! `status.qosClass` parity with upstream `ComputePodQOS`.
//!
//! The api-server is the **authoritative writer** of `status.qosClass`:
//! upstream sets it in the registry strategy
//! (`pkg/registry/core/pod/strategy.go:92`, `QOSClass: qos.GetPodQOS(pod)`) and
//! the kubelet later recomputes it in `generateAPIPodStatus`
//! (`pkg/kubelet/kubelet_pods.go:2097`). The two must agree, or `qosClass`
//! flips after the pod is scheduled — and eviction ordering, which reads the
//! kubelet's answer, disagrees with what the API reports.
//!
//! Each test here pins one divergence the api-server's former hand-rolled
//! classifier had against `pkg/apis/core/v1/helper/qos/qos.go:92-172`:
//!
//!   1. a container declaring **only** cpu (or only memory) was published
//!      `BestEffort` — upstream: `requests` is non-empty, so
//!      `len(requests) == 0 && len(limits) == 0` is false (qos.go:156-158) →
//!      `Burstable`. A `BestEffort` pod is the first thing evicted under node
//!      pressure, so this one had a live blast radius;
//!   2. `spec.initContainers` were ignored — upstream folds them into the same
//!      container set (qos.go:113-116);
//!   3. quantities were compared as **strings**, so `cpu: "1"` vs
//!      `cpu: "1000m"` read as different — upstream compares values
//!      (`lim.Cmp(req) != 0`, qos.go:161);
//!   4. no filtering to cpu/memory (`isSupportedQoSComputeResource`,
//!      qos.go:29-35) nor to `> 0` (`quantity.Cmp(zeroQuantity) == 1`,
//!      qos.go:57/122), so a device-plugin-only pod or an explicit `cpu: "0"`
//!      was not BestEffort;
//!   5. no cross-container summing (qos.go:100-105, 126-133).
//!
//! Harness: same in-process axum router over `MemoryStorage` used by
//! `pod_lifecycle_extended_test.rs`.

use axum::http::{Method, StatusCode};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

async fn create_namespace(router: &TestApiServer, name: &str) {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    });
    let (status, _) = router
        .send(
            Method::POST.as_str(),
            "/api/v1/namespaces",
            Some("application/json"),
            Some(&body),
        )
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "namespace {name} create failed: {status}"
    );
}

/// POST the pod and return the server-computed `status.qosClass`.
async fn created_qos_class(ns: &str, pod: Value) -> String {
    let router = TestApiServer::new();
    create_namespace(&router, ns).await;

    let (status, created) = router
        .send(
            Method::POST.as_str(),
            &format!("/api/v1/namespaces/{ns}/pods"),
            Some("application/json"),
            Some(&pod),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "pod create failed: {created}");

    created
        .get("status")
        .and_then(|s| s.get("qosClass"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

fn pod(name: &str, spec: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name },
        "spec": spec,
    })
}

fn container(name: &str, resources: Value) -> Value {
    json!({ "name": name, "image": "busybox", "resources": resources })
}

// ---------------------------------------------------------------------------
// 1. cpu-only / memory-only pods are Burstable, not BestEffort
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cpu_only_requests_is_burstable_not_besteffort() {
    let qos = created_qos_class(
        "qos-cpu-only",
        pod(
            "cpu-only",
            json!({ "containers": [container("c", json!({ "requests": { "cpu": "100m" } }))] }),
        ),
    )
    .await;
    assert_eq!(
        qos, "Burstable",
        "a cpu-only request is a non-empty `requests` map, so upstream \
         qos.go:156-158 cannot return BestEffort"
    );
}

#[tokio::test]
async fn memory_only_requests_is_burstable_not_besteffort() {
    let qos = created_qos_class(
        "qos-mem-only",
        pod(
            "mem-only",
            json!({ "containers": [container("c", json!({ "requests": { "memory": "64Mi" } }))] }),
        ),
    )
    .await;
    assert_eq!(qos, "Burstable");
}

#[tokio::test]
async fn cpu_only_limits_is_burstable_not_besteffort() {
    // Limits-only, cpu only: `qosLimitsFound` lacks memory (qos.go:149-152) so
    // the pod is not Guaranteed, but `limits` is non-empty so it is not
    // BestEffort either.
    let qos = created_qos_class(
        "qos-cpu-limit",
        pod(
            "cpu-limit",
            json!({ "containers": [container("c", json!({ "limits": { "cpu": "1" } }))] }),
        ),
    )
    .await;
    assert_eq!(qos, "Burstable");
}

// ---------------------------------------------------------------------------
// 2. initContainers participate (qos.go:113-116)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn init_container_without_limits_drops_pod_to_burstable() {
    let qos = created_qos_class(
        "qos-init-burst",
        pod(
            "init-burst",
            json!({
                "containers": [container("app", json!({
                    "limits":   { "cpu": "100m", "memory": "128Mi" },
                    "requests": { "cpu": "100m", "memory": "128Mi" },
                }))],
                "initContainers": [container("init", json!({
                    "requests": { "cpu": "50m", "memory": "32Mi" },
                }))],
            }),
        ),
    )
    .await;
    assert_eq!(
        qos, "Burstable",
        "the init container declares no limits, so `qosLimitsFound` misses \
         cpu+memory for it and the whole pod forfeits Guaranteed"
    );
}

#[tokio::test]
async fn init_container_matching_limits_keeps_pod_guaranteed() {
    let qos = created_qos_class(
        "qos-init-guar",
        pod(
            "init-guar",
            json!({
                "containers": [container("app", json!({
                    "limits":   { "cpu": "100m", "memory": "128Mi" },
                    "requests": { "cpu": "100m", "memory": "128Mi" },
                }))],
                "initContainers": [container("init", json!({
                    "limits":   { "cpu": "50m", "memory": "32Mi" },
                    "requests": { "cpu": "50m", "memory": "32Mi" },
                }))],
            }),
        ),
    )
    .await;
    assert_eq!(qos, "Guaranteed");
}

// ---------------------------------------------------------------------------
// 3. quantities compare by value, not by string (qos.go:161)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn equal_quantities_in_different_units_are_guaranteed() {
    let qos = created_qos_class(
        "qos-units",
        pod(
            "units",
            json!({ "containers": [container("c", json!({
                "limits":   { "cpu": "1",     "memory": "1Gi" },
                "requests": { "cpu": "1000m", "memory": "1024Mi" },
            }))] }),
        ),
    )
    .await;
    assert_eq!(
        qos, "Guaranteed",
        "`1` == `1000m` cpu and `1Gi` == `1024Mi` memory — upstream compares \
         values with `Cmp`, never the serialised strings"
    );
}

// ---------------------------------------------------------------------------
// 4. only cpu/memory count, and only when > 0 (qos.go:29-35, 57/122)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn extended_resource_only_pod_is_besteffort() {
    let qos = created_qos_class(
        "qos-gpu",
        pod(
            "gpu",
            json!({ "containers": [container("c", json!({
                "limits":   { "nvidia.com/gpu": "1" },
                "requests": { "nvidia.com/gpu": "1" },
            }))] }),
        ),
    )
    .await;
    assert_eq!(
        qos, "BestEffort",
        "`nvidia.com/gpu` is not a supported QoS compute resource, so both \
         maps stay empty and qos.go:156-158 returns BestEffort"
    );
}

#[tokio::test]
async fn zero_quantities_are_besteffort() {
    let qos = created_qos_class(
        "qos-zero",
        pod(
            "zero",
            json!({ "containers": [container("c", json!({
                "requests": { "cpu": "0", "memory": "0" },
            }))] }),
        ),
    )
    .await;
    assert_eq!(
        qos, "BestEffort",
        "only quantities strictly greater than zero are collected"
    );
}

// ---------------------------------------------------------------------------
// 5. requests and limits are summed across containers (qos.go:100-105, 126-133)
//
// The Guaranteed/Burstable boundary the sums decide is not reachable through
// this surface: it needs a container whose request exceeds its own limit so
// that only the *totals* match, and validation rejects that per container
// ("must be less than or equal to cpu limit", same as upstream
// `validateResourceRequirements`). What the API can pin is that the summed
// totals are compared by value across containers declaring the same quantity
// in different units. The summing itself is covered at the port
// (`crates/kubelet/tests/it/coverage_qos.rs::
// requests_and_limits_are_summed_across_containers`).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cross_container_sums_compare_by_value() {
    let qos = created_qos_class(
        "qos-sum",
        pod(
            "sum",
            json!({ "containers": [
                container("a", json!({
                    "limits":   { "cpu": "1",     "memory": "1Gi" },
                    "requests": { "cpu": "1000m", "memory": "1024Mi" },
                })),
                container("b", json!({
                    "limits":   { "cpu": "500m", "memory": "512Mi" },
                    "requests": { "cpu": "0.5",  "memory": "524288Ki" },
                })),
            ] }),
        ),
    )
    .await;
    assert_eq!(
        qos, "Guaranteed",
        "summed requests (1500m / 1536Mi) equal summed limits, though no two \
         strings match"
    );
}
