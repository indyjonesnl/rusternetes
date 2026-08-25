// Integration tests for admission controllers (LimitRange and ResourceQuota)
//
// These tests use in-memory storage and don't require a running etcd instance.
// The admission controllers are tested through the functions which:
// 1. apply_limit_range() - applies defaults and validates constraints
// 2. check_resource_quota() - ensures pod doesn't exceed quota
//
// Full E2E testing happens through the workflow tests that test the pod creation handler.

use rusternetes_api_server::admission::{apply_limit_range, check_resource_quota};
use rusternetes_common::resources::{
    Container, LimitRange, LimitRangeItem, LimitRangeSpec, Pod, PodSpec, ResourceQuota,
    ResourceQuotaSpec,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn create_minimal_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "test-container".to_string(),
                image: "nginx:latest".to_string(),
                command: None,
                args: None,
                working_dir: None,
                ports: None,
                env: None,
                resources: None,
                volume_mounts: None,
                image_pull_policy: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                restart_policy: None,
                resize_policy: None,
                security_context: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
            init_containers: None,
            volumes: None,
            restart_policy: None,
            node_name: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority_class_name: None,
            priority: None,
            automount_service_account_token: None,
            ephemeral_containers: None,
            overhead: None,
            scheduler_name: None,
            topology_spread_constraints: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
            ..Default::default()
        }),
        status: None,
    }
}

#[tokio::test]
async fn test_limit_range_allows_when_no_limit_exists() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pod = create_minimal_pod("test-pod", "test-namespace");

    let result = apply_limit_range(&storage, "test-namespace", &mut pod).await;
    assert!(result.is_ok());
    assert!(
        result.unwrap(),
        "Pod should be allowed when no LimitRange exists"
    );
}

#[tokio::test]
async fn test_quota_allows_when_no_quota_exists() {
    let storage = Arc::new(MemoryStorage::new());
    let pod = create_minimal_pod("test-pod", "test-namespace");

    let result = check_resource_quota(&storage, "test-namespace", &pod).await;
    assert!(result.is_ok());
    assert!(
        result.unwrap(),
        "Pod should be allowed when no quota exists"
    );
}

#[tokio::test]
async fn test_limit_range_applies_defaults() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a LimitRange with defaults
    let mut default_limits = HashMap::new();
    default_limits.insert("cpu".to_string(), "500m".to_string());
    default_limits.insert("memory".to_string(), "512Mi".to_string());

    let limit_range = LimitRange {
        type_meta: TypeMeta {
            kind: "LimitRange".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("test-limit-range").with_namespace("test-namespace"),
        spec: LimitRangeSpec {
            limits: vec![LimitRangeItem {
                item_type: "Container".to_string(),
                max: None,
                min: None,
                default: Some(default_limits.clone()),
                default_request: None,
                max_limit_request_ratio: None,
            }],
        },
    };

    let key = build_key("limitranges", Some("test-namespace"), "test-limit-range");
    storage.create(&key, &limit_range).await.unwrap();

    // Create a pod without resource limits
    let mut pod = create_minimal_pod("test-pod", "test-namespace");

    // Apply limit range
    let result = apply_limit_range(&storage, "test-namespace", &mut pod).await;
    assert!(result.is_ok());
    assert!(result.unwrap(), "LimitRange admission should pass");

    // Verify defaults were applied
    let container = &pod.spec.unwrap().containers[0];
    assert!(container.resources.is_some());
    let resources = container.resources.as_ref().unwrap();
    assert!(resources.limits.is_some());
    let limits = resources.limits.as_ref().unwrap();
    assert_eq!(limits.get("cpu").unwrap(), "500m");
    assert_eq!(limits.get("memory").unwrap(), "512Mi");
}

#[tokio::test]
async fn test_quota_allows_when_under_limit() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a quota
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "10".to_string());

    let quota = ResourceQuota::new(
        "test-quota",
        "test-namespace",
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        },
    );

    let key = build_key("resourcequotas", Some("test-namespace"), "test-quota");
    storage.create(&key, &quota).await.unwrap();

    // Try to create a pod
    let pod = create_minimal_pod("test-pod", "test-namespace");

    let result = check_resource_quota(&storage, "test-namespace", &pod).await;
    assert!(result.is_ok());
    assert!(result.unwrap(), "Pod should be allowed under quota");
}

#[tokio::test]
async fn test_quota_rejects_when_exceeding_pod_count() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a quota with low pod limit
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "1".to_string());

    let quota = ResourceQuota::new(
        "test-quota",
        "test-namespace",
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        },
    );

    let quota_key = build_key("resourcequotas", Some("test-namespace"), "test-quota");
    storage.create(&quota_key, &quota).await.unwrap();

    // Create an existing pod
    let existing_pod = create_minimal_pod("existing-pod", "test-namespace");
    let pod_key = build_key("pods", Some("test-namespace"), "existing-pod");
    storage.create(&pod_key, &existing_pod).await.unwrap();

    // Try to create a second pod (should exceed quota)
    let new_pod = create_minimal_pod("new-pod", "test-namespace");

    let result = check_resource_quota(&storage, "test-namespace", &new_pod).await;
    assert!(result.is_ok());
    assert!(
        !result.unwrap(),
        "Pod should be rejected for exceeding pod count quota"
    );
}

// ===== DefaultStorageClass admission tests =====

use rusternetes_api_server::admission::set_default_storage_class;
use rusternetes_common::resources::volume::ResourceRequirements;
use rusternetes_common::resources::{
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeClaimSpec, StorageClass,
};

fn create_test_pvc(name: &str, namespace: &str) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "1Gi".to_string());

    PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                requests: Some(requests),
                limits: None,
            },
            storage_class_name: None,
            volume_name: None,
            selector: None,
            volume_mode: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    }
}

#[tokio::test]
async fn test_default_storage_class_no_default() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pvc = create_test_pvc("test-pvc", "test-namespace");

    // No storage classes exist
    let result = set_default_storage_class(&storage, &mut pvc).await;
    assert!(result.is_ok());
    // storageClassName should remain None
    assert!(pvc.spec.storage_class_name.is_none());
}

#[tokio::test]
async fn test_default_storage_class_sets_default() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pvc = create_test_pvc("test-pvc", "test-namespace");

    // Create a default storage class
    let mut annotations = HashMap::new();
    annotations.insert(
        "storageclass.kubernetes.io/is-default-class".to_string(),
        "true".to_string(),
    );

    let storage_class = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("default-sc").with_annotations(annotations),
        provisioner: "kubernetes.io/no-provisioner".to_string(),
        parameters: None,
        reclaim_policy: None,
        volume_binding_mode: None,
        allow_volume_expansion: None,
        mount_options: None,
        allowed_topologies: None,
    };

    let sc_key = build_key("storageclasses", None, "default-sc");
    storage.create(&sc_key, &storage_class).await.unwrap();

    // Apply default storage class
    let result = set_default_storage_class(&storage, &mut pvc).await;
    assert!(result.is_ok());

    // storageClassName should be set to "default-sc"
    assert_eq!(pvc.spec.storage_class_name, Some("default-sc".to_string()));
}

#[tokio::test]
async fn test_default_storage_class_already_set() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pvc = create_test_pvc("test-pvc", "test-namespace");
    pvc.spec.storage_class_name = Some("my-custom-sc".to_string());

    // Create a default storage class
    let mut annotations = HashMap::new();
    annotations.insert(
        "storageclass.kubernetes.io/is-default-class".to_string(),
        "true".to_string(),
    );

    let storage_class = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("default-sc").with_annotations(annotations),
        provisioner: "kubernetes.io/no-provisioner".to_string(),
        parameters: None,
        reclaim_policy: None,
        volume_binding_mode: None,
        allow_volume_expansion: None,
        mount_options: None,
        allowed_topologies: None,
    };

    let sc_key = build_key("storageclasses", None, "default-sc");
    storage.create(&sc_key, &storage_class).await.unwrap();

    // Apply default storage class
    let result = set_default_storage_class(&storage, &mut pvc).await;
    assert!(result.is_ok());

    // storageClassName should remain "my-custom-sc"
    assert_eq!(
        pvc.spec.storage_class_name,
        Some("my-custom-sc".to_string())
    );
}

#[tokio::test]
async fn test_default_storage_class_beta_annotation() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pvc = create_test_pvc("test-pvc", "test-namespace");

    // Create a default storage class with beta annotation
    let mut annotations = HashMap::new();
    annotations.insert(
        "storageclass.beta.kubernetes.io/is-default-class".to_string(),
        "true".to_string(),
    );

    let storage_class = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("default-sc").with_annotations(annotations),
        provisioner: "kubernetes.io/no-provisioner".to_string(),
        parameters: None,
        reclaim_policy: None,
        volume_binding_mode: None,
        allow_volume_expansion: None,
        mount_options: None,
        allowed_topologies: None,
    };

    let sc_key = build_key("storageclasses", None, "default-sc");
    storage.create(&sc_key, &storage_class).await.unwrap();

    // Apply default storage class
    let result = set_default_storage_class(&storage, &mut pvc).await;
    assert!(result.is_ok());

    // storageClassName should be set even with beta annotation
    assert_eq!(pvc.spec.storage_class_name, Some("default-sc".to_string()));
}

// ===========================================================================
// LimitRange admission — e2e parity pins (relocated from the controller-manager
// stub suite; issue #1031). Upstream enforces LimitRange in the LimitRanger
// ADMISSION plugin (plugin/pkg/admission/limitranger), NOT a background
// controller. These mirror test/e2e/apimachinery/limit_range.go (release-1.35).
// ===========================================================================

use rusternetes_api_server::admission::{apply_limit_range_to_pvc, apply_limit_range_with};
use rusternetes_common::resources::volume::ResourceRequirements as PvcResourceRequirements;
use rusternetes_common::types::ResourceRequirements as PodResourceRequirements;

const LR_NS: &str = "limitrange-e2e";

fn str_map(pairs: &[(&str, &str)]) -> Option<HashMap<String, String>> {
    if pairs.is_empty() {
        None
    } else {
        Some(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        )
    }
}

fn container_item(
    default: &[(&str, &str)],
    default_request: &[(&str, &str)],
    min: &[(&str, &str)],
    max: &[(&str, &str)],
    ratio: &[(&str, &str)],
) -> LimitRangeItem {
    LimitRangeItem {
        item_type: "Container".to_string(),
        default: str_map(default),
        default_request: str_map(default_request),
        min: str_map(min),
        max: str_map(max),
        max_limit_request_ratio: str_map(ratio),
    }
}

fn limit_range(items: Vec<LimitRangeItem>) -> LimitRange {
    LimitRange::new("lr", LR_NS, LimitRangeSpec { limits: items })
}

fn pod_one_container(name: &str, requests: &[(&str, &str)], limits: &[(&str, &str)]) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(LR_NS),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                resources: Some(PodResourceRequirements {
                    requests: str_map(requests),
                    limits: str_map(limits),
                    claims: None,
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    }
}

async fn make_pvc(storage: &Arc<MemoryStorage>, name: &str, storage_req: &str) {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), storage_req.to_string());
    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(LR_NS),
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![],
            resources: PvcResourceRequirements {
                requests: Some(requests),
                limits: None,
            },
            storage_class_name: None,
            volume_name: None,
            selector: None,
            volume_mode: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    };
    let key = build_key("persistentvolumeclaims", Some(LR_NS), name);
    storage.create(&key, &pvc).await.expect("create pvc");
}

/// 1. Container defaults: a pod declaring no resources inherits `default`
/// (limits) and `defaultRequest` from a `type: Container` item.
#[tokio::test]
async fn limitrange_container_defaults_apply_to_unspecified_pods() {
    let lr = limit_range(vec![container_item(
        &[("cpu", "200m"), ("memory", "256Mi")],
        &[("cpu", "100m"), ("memory", "128Mi")],
        &[],
        &[],
        &[],
    )]);
    let mut pod = pod_one_container("noresources", &[], &[]);

    let allowed = apply_limit_range_with(&mut pod, &vec![lr]).expect("apply");
    assert!(allowed, "defaulting must not reject");

    let c = &pod.spec.unwrap().containers[0];
    let rr = c.resources.as_ref().unwrap();
    let requests = rr.requests.as_ref().unwrap();
    let limits = rr.limits.as_ref().unwrap();
    assert_eq!(requests.get("cpu"), Some(&"100m".to_string()));
    assert_eq!(requests.get("memory"), Some(&"128Mi".to_string()));
    assert_eq!(limits.get("cpu"), Some(&"200m".to_string()));
    assert_eq!(limits.get("memory"), Some(&"256Mi".to_string()));
}

/// 2. Min/max: a container outside `[min, max]` is rejected; in-range admitted.
#[tokio::test]
async fn limitrange_min_max_rejects_out_of_range_pods() {
    let lr = limit_range(vec![container_item(
        &[],
        &[],
        &[("cpu", "200m")],
        &[("cpu", "2")],
        &[],
    )]);

    let mut in_range = pod_one_container("in-range", &[("cpu", "500m")], &[("cpu", "1")]);
    assert!(
        apply_limit_range_with(&mut in_range, &vec![lr.clone()]).expect("apply"),
        "in-range pod must be admitted",
    );

    let mut too_big = pod_one_container("too-big", &[("cpu", "1")], &[("cpu", "4")]);
    assert!(
        !apply_limit_range_with(&mut too_big, &vec![lr]).expect("apply"),
        "cpu limit 4 above max 2 must be rejected",
    );
}

/// 3. Ratio: `maxLimitRequestRatio` caps `limits/requests` per resource.
#[tokio::test]
async fn limitrange_ratio_rejects_high_ratio_pods() {
    let lr = limit_range(vec![container_item(&[], &[], &[], &[], &[("cpu", "3")])]);

    let mut low = pod_one_container("low-ratio", &[("cpu", "100m")], &[("cpu", "200m")]);
    assert!(
        apply_limit_range_with(&mut low, &vec![lr.clone()]).expect("apply"),
        "ratio 2 must survive cap 3",
    );

    let mut high = pod_one_container("high-ratio", &[("cpu", "100m")], &[("cpu", "500m")]);
    assert!(
        !apply_limit_range_with(&mut high, &vec![lr]).expect("apply"),
        "ratio 5 must be rejected by cap 3",
    );
}

/// 4. PVC storage min/max: a `type: PersistentVolumeClaim` item bounds the
/// PVC's `resources.requests.storage`.
#[tokio::test]
async fn limitrange_pvc_storage_min_max_enforced() {
    let storage = Arc::new(MemoryStorage::new());
    let lr = LimitRange::new(
        "lr",
        LR_NS,
        LimitRangeSpec {
            limits: vec![LimitRangeItem {
                item_type: "PersistentVolumeClaim".to_string(),
                default: None,
                default_request: None,
                min: str_map(&[("storage", "1Gi")]),
                max: str_map(&[("storage", "10Gi")]),
                max_limit_request_ratio: None,
            }],
        },
    );
    let lr_key = build_key("limitranges", Some(LR_NS), "lr");
    storage.create(&lr_key, &lr).await.unwrap();

    make_pvc(&storage, "too-small", "500Mi").await;
    make_pvc(&storage, "ok", "5Gi").await;
    make_pvc(&storage, "too-big", "100Gi").await;

    let small_key = build_key("persistentvolumeclaims", Some(LR_NS), "too-small");
    let mut too_small: PersistentVolumeClaim = storage.get(&small_key).await.unwrap();
    assert!(
        !apply_limit_range_to_pvc(&storage, LR_NS, &mut too_small)
            .await
            .expect("apply"),
        "500Mi below min 1Gi must be rejected",
    );

    let ok_key = build_key("persistentvolumeclaims", Some(LR_NS), "ok");
    let mut ok: PersistentVolumeClaim = storage.get(&ok_key).await.unwrap();
    assert!(
        apply_limit_range_to_pvc(&storage, LR_NS, &mut ok)
            .await
            .expect("apply"),
        "5Gi within [1Gi,10Gi] must be admitted",
    );

    let big_key = build_key("persistentvolumeclaims", Some(LR_NS), "too-big");
    let mut too_big: PersistentVolumeClaim = storage.get(&big_key).await.unwrap();
    assert!(
        !apply_limit_range_to_pvc(&storage, LR_NS, &mut too_big)
            .await
            .expect("apply"),
        "100Gi above max 10Gi must be rejected",
    );
}

/// 5. Pod-level aggregation: a `type: Pod` item's max applies to the SUM
/// across all containers, not each container individually.
#[tokio::test]
async fn limitrange_pod_level_aggregates_across_containers() {
    let lr = LimitRange::new(
        "lr",
        LR_NS,
        LimitRangeSpec {
            limits: vec![LimitRangeItem {
                item_type: "Pod".to_string(),
                default: None,
                default_request: None,
                min: None,
                max: str_map(&[("cpu", "2")]),
                max_limit_request_ratio: None,
            }],
        },
    );

    let container = |n: &str| Container {
        name: n.to_string(),
        image: "pause:latest".to_string(),
        resources: Some(PodResourceRequirements {
            requests: str_map(&[("cpu", "1500m")]),
            limits: None,
            claims: None,
        }),
        ..Default::default()
    };
    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("oversized-sum").with_namespace(LR_NS),
        spec: Some(PodSpec {
            containers: vec![container("a"), container("b")],
            ..Default::default()
        }),
        status: None,
    };

    assert!(
        !apply_limit_range_with(&mut pod, &vec![lr]).expect("apply"),
        "two containers at 1500m (sum 3) must exceed type:Pod max.cpu=2",
    );
}

// ===== ResourceQuota accounting on Quantity (#1714) =====

/// `create_minimal_pod` with `requests` set on its single container.
fn pod_with_requests(name: &str, namespace: &str, requests: &[(&str, &str)]) -> Pod {
    let mut pod = create_minimal_pod(name, namespace);
    let map: HashMap<String, String> = requests
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    pod.spec.as_mut().unwrap().containers[0].resources =
        Some(rusternetes_common::types::ResourceRequirements {
            requests: Some(map),
            limits: None,
            claims: None,
        });
    pod
}

async fn put_quota(storage: &Arc<MemoryStorage>, name: &str, hard: &[(&str, &str)]) {
    let hard: HashMap<String, String> = hard
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let quota = ResourceQuota::new(
        name,
        "test-namespace",
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        },
    );
    let key = build_key("resourcequotas", Some("test-namespace"), name);
    storage.create(&key, &quota).await.unwrap();
}

/// An extended-resource limit carrying a suffix used to go through
/// `limit_str.parse().unwrap_or(i64::MAX)`, so `hard: requests.example.com/dongle: 1k`
/// was not enforced at all — the same fail-open shape as the `1Ti` memory quota
/// fixed in #1722, one layer further in.
#[tokio::test]
async fn quota_enforces_extended_resource_limit_with_suffix() {
    let storage = Arc::new(MemoryStorage::new());
    put_quota(
        &storage,
        "dongles",
        &[("requests.example.com/dongle", "1k")],
    )
    .await;

    // Under the limit.
    let ok = pod_with_requests("small", "test-namespace", &[("example.com/dongle", "999")]);
    assert!(
        check_resource_quota(&storage, "test-namespace", &ok)
            .await
            .unwrap(),
        "999 dongles is under a 1k limit"
    );

    // Over the limit: 1001 > 1000.
    let too_big = pod_with_requests("big", "test-namespace", &[("example.com/dongle", "1001")]);
    assert!(
        !check_resource_quota(&storage, "test-namespace", &too_big)
            .await
            .unwrap(),
        "1001 dongles must be rejected against a 1k limit"
    );
}

/// A fractional quota limit. `0.5Gi` read as 0 through every hand-rolled parser,
/// and `Value()`-to-bytes made `1.5Gi` and `1610612736` indistinguishable only
/// by luck; both now compare as quantities.
#[tokio::test]
async fn quota_enforces_fractional_memory_limit() {
    let storage = Arc::new(MemoryStorage::new());
    put_quota(&storage, "mem", &[("requests.memory", "1.5Gi")]).await;

    let fits = pod_with_requests("fits", "test-namespace", &[("memory", "1Gi")]);
    assert!(
        check_resource_quota(&storage, "test-namespace", &fits)
            .await
            .unwrap(),
        "1Gi fits in a 1.5Gi quota"
    );

    let existing = pod_with_requests("existing", "test-namespace", &[("memory", "1Gi")]);
    storage
        .create(
            &build_key("pods", Some("test-namespace"), "existing"),
            &existing,
        )
        .await
        .unwrap();

    let second = pod_with_requests("second", "test-namespace", &[("memory", "1Gi")]);
    assert!(
        !check_resource_quota(&storage, "test-namespace", &second)
            .await
            .unwrap(),
        "2Gi total must be rejected against a 1.5Gi quota"
    );
}

/// A pod's peak footprint includes its init containers (upstream `PodRequests`
/// takes the max). Summing only `spec.containers` let a 4Gi init container in
/// under a 2Gi quota.
#[tokio::test]
async fn quota_charges_init_container_peak() {
    let storage = Arc::new(MemoryStorage::new());
    put_quota(&storage, "mem", &[("requests.memory", "2Gi")]).await;

    let mut pod = pod_with_requests("init-heavy", "test-namespace", &[("memory", "1Gi")]);
    let mut init = pod.spec.as_ref().unwrap().containers[0].clone();
    init.name = "init".to_string();
    let mut init_reqs = HashMap::new();
    init_reqs.insert("memory".to_string(), "4Gi".to_string());
    init.resources = Some(rusternetes_common::types::ResourceRequirements {
        requests: Some(init_reqs),
        limits: None,
        claims: None,
    });
    pod.spec.as_mut().unwrap().init_containers = Some(vec![init]);

    assert!(
        !check_resource_quota(&storage, "test-namespace", &pod)
            .await
            .unwrap(),
        "a 4Gi init container must be charged against a 2Gi quota"
    );
}

/// `status.used` written by admission must be canonical quantities restricted to
/// the quota's hard keys — the same shape the quota controller writes. Admission
/// used to emit raw byte counts (`format!("{}", bytes)`) plus keys the quota did
/// not constrain, so the two components overwrote each other's spelling.
#[tokio::test]
async fn quota_status_used_is_canonical_and_masked() {
    let storage = Arc::new(MemoryStorage::new());
    put_quota(
        &storage,
        "mem",
        &[("requests.memory", "4Gi"), ("pods", "10")],
    )
    .await;

    let pod = pod_with_requests(
        "p",
        "test-namespace",
        &[("memory", "512Mi"), ("cpu", "250m")],
    );
    assert!(check_resource_quota(&storage, "test-namespace", &pod)
        .await
        .unwrap());

    let stored: ResourceQuota = storage
        .get(&build_key("resourcequotas", Some("test-namespace"), "mem"))
        .await
        .unwrap();
    let used = stored.status.unwrap().used.unwrap();
    let mut keys: Vec<&String> = used.keys().collect();
    keys.sort();
    assert_eq!(keys, vec!["pods", "requests.memory"]);
    assert_eq!(
        used.get("requests.memory").map(String::as_str),
        Some("512Mi")
    );
    assert_eq!(used.get("pods").map(String::as_str), Some("1"));
}

/// A quota dimension the pod does not ask for cannot reject it. Upstream masks
/// the comparison to the names the request actually charges
/// (`Mask(newUsage, ResourceNames(requestedUsage))`), so a namespace already at
/// its memory ceiling still admits a pod that requests no memory.
#[tokio::test]
async fn quota_ignores_dimensions_the_pod_does_not_request() {
    let storage = Arc::new(MemoryStorage::new());
    put_quota(
        &storage,
        "mem",
        &[("requests.memory", "1Gi"), ("pods", "10")],
    )
    .await;

    let existing = pod_with_requests("existing", "test-namespace", &[("memory", "1Gi")]);
    storage
        .create(
            &build_key("pods", Some("test-namespace"), "existing"),
            &existing,
        )
        .await
        .unwrap();

    let no_memory = pod_with_requests("cpu-only", "test-namespace", &[("cpu", "100m")]);
    assert!(
        check_resource_quota(&storage, "test-namespace", &no_memory)
            .await
            .unwrap(),
        "a pod requesting no memory must not fail a full memory quota"
    );
}

/// An UPDATE that changes no charged dimension has a zero delta and is admitted
/// even when the namespace is exactly at its limit — the pod's own usage is
/// already counted. Upstream reaches this via
/// `RemoveZeros(SubtractWithNonNegativeResult(new, old))`.
#[tokio::test]
async fn quota_admits_update_with_zero_delta_at_the_limit() {
    use rusternetes_api_server::admission::check_resource_quota_with_old;

    let storage = Arc::new(MemoryStorage::new());
    put_quota(
        &storage,
        "mem",
        &[("requests.memory", "1Gi"), ("pods", "1")],
    )
    .await;

    let pod = pod_with_requests("p", "test-namespace", &[("memory", "1Gi")]);
    storage
        .create(&build_key("pods", Some("test-namespace"), "p"), &pod)
        .await
        .unwrap();

    // Same resources, so nothing new is charged.
    let mut updated = pod.clone();
    updated
        .metadata
        .labels
        .get_or_insert_with(HashMap::new)
        .insert("touched".to_string(), "yes".to_string());

    assert!(
        check_resource_quota_with_old(&storage, "test-namespace", &updated, Some(&pod))
            .await
            .unwrap(),
        "an update charging nothing must be admitted at the limit"
    );

    // Raising the request past the ceiling still fails.
    let bigger = pod_with_requests("p", "test-namespace", &[("memory", "2Gi")]);
    assert!(
        !check_resource_quota_with_old(&storage, "test-namespace", &bigger, Some(&pod))
            .await
            .unwrap(),
        "an update that raises usage past the limit must be rejected"
    );
}
