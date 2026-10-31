//! Memory-backed emptyDir sizing and tmpfs mount options (#3872).
//!
//! Ports `TestCalculateEmptyDirMemorySize` and `TestTmpfsMountOptions`
//! (`pkg/volume/emptydir/empty_dir_test.go:1076-1210`), `TestIsSwapEnabled`'s
//! sibling kernel/userns helpers (`pkg/kubelet/util/swap/swap_util.go`) and the
//! memory limit of `cm.ResourceConfigForPod` (`helpers_linux_test.go`).

use rusternetes_common::resources::Pod;
use rusternetes_kubelet::cm_helpers::resource_config_for_pod_memory;
use rusternetes_kubelet::swap_util::{
    parse_generic_version, running_in_user_ns_from, version_at_least,
};
use rusternetes_kubelet::volume_plugins::empty_dir::{
    calculate_empty_dir_memory_size, generate_tmpfs_mount_options,
};

const GI: i64 = 1 << 30;

fn pod(spec: serde_json::Value) -> Pod {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "default", "uid": "u"},
        "spec": spec,
    }))
    .unwrap()
}

fn container(res: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"name": "c", "image": "i", "resources": res})
}

// TestCalculateEmptyDirMemorySize
#[test]
fn calculate_empty_dir_memory_size_cases() {
    let limits_10 = pod(serde_json::json!({"containers": [container(
        serde_json::json!({"limits": {"memory": "10Gi"}}))]}));
    let requests_10 = pod(serde_json::json!({"containers": [container(
        serde_json::json!({"requests": {"memory": "10Gi"}}))]}));
    // EmptyDirLocalLimit
    assert_eq!(
        calculate_empty_dir_memory_size(16 * GI, Some("1Gi"), &limits_10),
        GI
    );
    // PodLocalLimit
    assert_eq!(
        calculate_empty_dir_memory_size(16 * GI, Some("0"), &limits_10),
        10 * GI
    );
    // NodeAllocatableLimit
    assert_eq!(
        calculate_empty_dir_memory_size(16 * GI, Some("0"), &requests_10),
        16 * GI
    );
    // A volume limit above the pod's is ignored; no volume limit at all too.
    assert_eq!(
        calculate_empty_dir_memory_size(16 * GI, Some("12Gi"), &limits_10),
        10 * GI
    );
    assert_eq!(
        calculate_empty_dir_memory_size(16 * GI, None, &limits_10),
        10 * GI
    );
    // A pod limit above node allocatable is ignored.
    assert_eq!(
        calculate_empty_dir_memory_size(4 * GI, None, &limits_10),
        4 * GI
    );
    // Missing node allocatable (zero) stays zero: nothing is `<= 0`.
    assert_eq!(
        calculate_empty_dir_memory_size(0, Some("1Gi"), &limits_10),
        0
    );
}

// TestTmpfsMountOptions
#[test]
fn tmpfs_mount_options() {
    let size = 123 * 1024;
    assert!(generate_tmpfs_mount_options(0, false).is_empty());
    assert_eq!(generate_tmpfs_mount_options(0, true), ["noswap"]);
    assert_eq!(generate_tmpfs_mount_options(size, false), ["size=125952"]);
    assert_eq!(
        generate_tmpfs_mount_options(size, true),
        ["size=125952", "noswap"]
    );
}

#[test]
fn pod_memory_limit_by_qos() {
    // BestEffort: nil.
    let be = pod(serde_json::json!({"containers": [container(serde_json::json!({}))]}));
    assert_eq!(resource_config_for_pod_memory(&be), None);

    // Guaranteed: the summed limit.
    let g = pod(
        serde_json::json!({"containers": [container(serde_json::json!({
        "requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "1", "memory": "1Gi"}}))]}),
    );
    assert_eq!(resource_config_for_pod_memory(&g), Some(GI));

    // Burstable with every container limited.
    let b = pod(serde_json::json!({"containers": [
        container(serde_json::json!({"limits": {"memory": "1Gi"}})),
        container(serde_json::json!({"limits": {"memory": "2Gi"}})),
    ]}));
    assert_eq!(resource_config_for_pod_memory(&b), Some(3 * GI));

    // Burstable where one container has no memory limit: nil.
    let b = pod(serde_json::json!({"containers": [
        container(serde_json::json!({"limits": {"memory": "1Gi"}})),
        container(serde_json::json!({"requests": {"memory": "1Gi"}})),
    ]}));
    assert_eq!(resource_config_for_pod_memory(&b), None);

    // Overhead is added to a non-zero limit.
    let b = pod(serde_json::json!({
        "overhead": {"memory": "1Gi"},
        "containers": [container(serde_json::json!({"limits": {"memory": "1Gi"}}))]}));
    assert_eq!(resource_config_for_pod_memory(&b), Some(2 * GI));
}

// ContainerFn on init containers: a plain init container without a limit
// clears `memoryLimitsDeclared`, unless restartable init containers before it
// already contribute a limit.
#[test]
fn init_containers_gate_memory_declared() {
    let main = container(serde_json::json!({"limits": {"memory": "1Gi"}}));
    let init_no_limit = serde_json::json!({"name": "i", "image": "i",
        "resources": {"requests": {"memory": "1Gi"}}});
    let p = pod(serde_json::json!({"containers": [main.clone()],
        "initContainers": [init_no_limit.clone()]}));
    assert_eq!(resource_config_for_pod_memory(&p), None);

    let sidecar = serde_json::json!({"name": "s", "image": "i", "restartPolicy": "Always",
        "resources": {"limits": {"memory": "1Gi"}}});
    let p = pod(serde_json::json!({"containers": [main],
        "initContainers": [sidecar, init_no_limit]}));
    assert_eq!(resource_config_for_pod_memory(&p), Some(2 * GI));
}

#[test]
fn kernel_version_parsing_and_comparison() {
    let v = parse_generic_version("6.8.0-45-generic").unwrap();
    assert_eq!(v, [6, 8, 0]);
    let min = parse_generic_version("6.4").unwrap();
    assert!(version_at_least(&v, &min));
    assert!(version_at_least(&[6, 4], &min));
    assert!(version_at_least(&[6, 4, 0], &min));
    assert!(!version_at_least(&[5, 15, 0], &min));
    assert!(!version_at_least(&[6, 3, 99], &min));
    assert_eq!(parse_generic_version("v5.10"), Some(vec![5, 10]));
    assert_eq!(parse_generic_version("garbage"), None);
}

#[test]
fn user_namespace_detection() {
    assert!(!running_in_user_ns_from(Some(
        "         0          0 4294967295\n"
    )));
    assert!(running_in_user_ns_from(Some(
        "         0       1000          1\n"
    )));
    assert!(running_in_user_ns_from(Some("0 0 1\n1 100000 65536\n")));
    assert!(!running_in_user_ns_from(None));
}
