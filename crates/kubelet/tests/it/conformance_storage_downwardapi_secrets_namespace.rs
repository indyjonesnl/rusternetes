//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-storage] Downward API volumes.
//!
//! Source of truth: `test/e2e/common/storage/downwardapi_volume.go` on
//! `release-1.35`.
//!
//! Every spec here drives the REAL kubelet volume code
//! (`VolumeManager::create_pod_volumes`, whose re-run is the reconciler's remount, in
//! `crates/kubelet/src/volumes.rs`, which dispatch to the downwardAPI volume
//! plugin and the AtomicWriter) against a memory-backed API and a
//! `tempfile::TempDir` kubelet root, then asserts on the files the container
//! would see. It does NOT reimplement the volume build (#1762 item 2: the
//! previous version carried local `build_downwardapi_volume` /
//! `resolve_pod_field` / `build_secret_volume` replicas, so it could never
//! fail when the kubelet regressed; two of its "node allocatable" specs only
//! asserted a struct field they had just set).
//!
//! The "secret with same name in different namespace" spec that used to live
//! here is covered against the real code by
//! `conformance_storage_configmap_secret_projected.rs`
//! (`other_ns_secret`), so it is not duplicated.
//!
//! Where an upstream spec reads a file from inside a container (agnhost
//! `mounttest --file_content / --file_mode`), the mirror reads the same file
//! from the host path the kubelet returns for that volume.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;

use rusternetes_common::resources::Pod;
use rusternetes_kubelet::volumes::VolumeManager;
use rusternetes_storage::StorageBackend;
use serde_json::{json, Value};

struct Node {
    _root: tempfile::TempDir,
    vm: VolumeManager,
}

impl Node {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let api = Arc::new(StorageBackend::new_memory());
        // The downwardAPI volume is a wrapped `Medium: Memory` emptyDir
        // (downwardapi.go:54-58), i.e. a tmpfs mount, which an unprivileged
        // test run cannot make: use the recording fake mounter, as upstream's
        // FakeVolumeHost does (mount.NewFakeMounter).
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            root.path().to_string_lossy().to_string(),
            Some(api.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );
        Self { _root: root, vm }
    }

    /// The kubelet's volume setup for the pod.
    async fn setup(&self, pod: &Pod) -> Paths {
        Paths(self.vm.create_pod_volumes(pod).await.unwrap())
    }

    /// The kubelet's periodic volume resync.
    async fn resync(&self, pod: &Pod) {
        self.vm.create_pod_volumes(pod).await.unwrap();
    }
}

struct Paths(HashMap<String, String>);

impl Paths {
    fn file(&self, rel: &str) -> PathBuf {
        PathBuf::from(&self.0["podinfo"]).join(rel)
    }
    /// agnhost `mounttest --file_content`.
    fn content(&self, rel: &str) -> String {
        String::from_utf8(std::fs::read(self.file(rel)).unwrap()).unwrap()
    }
    /// agnhost `mounttest --file_mode` (permission bits of the followed file).
    fn mode(&self, rel: &str) -> u32 {
        std::fs::metadata(self.file(rel))
            .unwrap()
            .permissions()
            .mode()
            & 0o777
    }
}

fn podname_item(mode: Option<i32>) -> Value {
    let mut item = json!({
        "path": "podname",
        "fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.name"},
    });
    if let Some(m) = mode {
        item["mode"] = json!(m);
    }
    item
}

/// `downwardAPIVolumeBasePod` with the given downwardAPI volume source and the
/// container's `resources` (null for none).
fn dapi_pod(name: &str, mut source: Value, resources: Value) -> Pod {
    // The API server defaults `defaultMode` to 0644 before the kubelet sees the
    // pod (`SetDefaults_DownwardAPIVolumeSource`); the kubelet's plugin
    // rejects a source that still lacks one, so the fixture carries it.
    if source.get("defaultMode").is_none() {
        source["defaultMode"] = json!(0o644);
    }
    let mut container = json!({"name": "client-container", "image": "agnhost"});
    if !resources.is_null() {
        container["resources"] = resources;
    }
    serde_json::from_value(json!({
        "metadata": {"name": name, "namespace": "ns", "uid": format!("uid-{name}")},
        "spec": {
            "containers": [container],
            "volumes": [{"name": "podinfo", "downwardAPI": source}],
            "restartPolicy": "Never",
        },
    }))
    .unwrap()
}

/// `downwardAPIVolumeBaseContainers` resources.
fn base_resources() -> Value {
    json!({
        "requests": {"cpu": "250m", "memory": "32Mi"},
        "limits": {"cpu": "1250m", "memory": "128Mi"},
    })
}

fn resource_item(path: &str, resource: &str) -> Value {
    json!({
        "path": path,
        "resourceFieldRef": {"containerName": "client-container", "resource": resource},
    })
}

/// [sig-storage] Downward API volume should provide podname only [NodeConformance] [Conformance]
/// Upstream: downwardapi_volume.go:54
#[tokio::test]
async fn downwardapi_volume_should_provide_podname_only() {
    let n = Node::new();
    let p = dapi_pod(
        "downwardapi-volume-x",
        json!({"items": [podname_item(None)]}),
        Value::Null,
    );
    let paths = n.setup(&p).await;
    assert_eq!(paths.content("podname"), "downwardapi-volume-x");
}

/// [sig-storage] Downward API volume should set DefaultMode on files [LinuxOnly] [NodeConformance] [Conformance]
/// Upstream: downwardapi_volume.go:69 (expects `-r--------`)
#[tokio::test]
async fn downwardapi_volume_should_set_defaultmode_on_files() {
    let n = Node::new();
    let p = dapi_pod(
        "downwardapi-volume-x",
        json!({"defaultMode": 0o400, "items": [podname_item(None)]}),
        Value::Null,
    );
    let paths = n.setup(&p).await;
    assert_eq!(paths.mode("podname"), 0o400);
}

/// [sig-storage] Downward API volume should set mode on item file [LinuxOnly] [NodeConformance] [Conformance]
/// Upstream: downwardapi_volume.go:85 (expects `-r--------`; the item mode
/// overrides the volume defaultMode)
#[tokio::test]
async fn downwardapi_volume_should_set_mode_on_item_file() {
    let n = Node::new();
    let p = dapi_pod(
        "downwardapi-volume-x",
        json!({"defaultMode": 0o644, "items": [podname_item(Some(0o400))]}),
        Value::Null,
    );
    let paths = n.setup(&p).await;
    assert_eq!(paths.mode("podname"), 0o400);
}

/// [sig-storage] Downward API volume should update labels on modification [NodeConformance] [Conformance]
/// Upstream: downwardapi_volume.go:131 (agnhost prints `key="value"\n`)
#[tokio::test]
async fn downwardapi_volume_should_update_labels_on_modification() {
    let n = Node::new();
    let mut p = dapi_pod(
        "labelsupdate",
        json!({"items": [{"path": "labels", "fieldRef": {"fieldPath": "metadata.labels", "apiVersion": "v1"}}]}),
        Value::Null,
    );
    p.metadata.labels = Some(HashMap::from([
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
    ]));
    let paths = n.setup(&p).await;
    // `fieldpath.FormatMap`: sorted `key="value"` lines, trailing "\n" trimmed.
    assert_eq!(paths.content("labels"), "key1=\"value1\"\nkey2=\"value2\"");
    p.metadata
        .labels
        .as_mut()
        .unwrap()
        .insert("key3".to_string(), "value3".to_string());
    n.resync(&p).await;
    assert_eq!(
        paths.content("labels"),
        "key1=\"value1\"\nkey2=\"value2\"\nkey3=\"value3\""
    );
}

/// [sig-storage] Downward API volume should update annotations on modification [NodeConformance] [Conformance]
/// Upstream: downwardapi_volume.go:163
#[tokio::test]
async fn downwardapi_volume_should_update_annotations_on_modification() {
    let n = Node::new();
    let mut p = dapi_pod(
        "annotationupdate",
        json!({"items": [{"path": "annotations", "fieldRef": {"fieldPath": "metadata.annotations", "apiVersion": "v1"}}]}),
        Value::Null,
    );
    p.metadata.annotations = Some(HashMap::from([("builder".to_string(), "bar".to_string())]));
    let paths = n.setup(&p).await;
    assert_eq!(paths.content("annotations"), "builder=\"bar\"");
    p.metadata
        .annotations
        .as_mut()
        .unwrap()
        .insert("builder".to_string(), "foo".to_string());
    n.resync(&p).await;
    assert_eq!(paths.content("annotations"), "builder=\"foo\"");
}

/// [sig-storage] Downward API volume should provide container's cpu/memory
/// limit/request [NodeConformance] [Conformance]
/// Upstream: downwardapi_volume.go:194, :208, :222, :236 (expects `2`,
/// `134217728`, `1`, `33554432`: divisor defaults to 1, cpu rounds up)
#[tokio::test]
async fn downwardapi_volume_should_provide_container_cpu_memory_limits_and_requests() {
    let n = Node::new();
    let p = dapi_pod(
        "downwardapi-volume-x",
        json!({"items": [
            resource_item("cpu_limit", "limits.cpu"),
            resource_item("memory_limit", "limits.memory"),
            resource_item("cpu_request", "requests.cpu"),
            resource_item("memory_request", "requests.memory"),
        ]}),
        base_resources(),
    );
    let paths = n.setup(&p).await;
    assert_eq!(paths.content("cpu_limit"), "2");
    assert_eq!(paths.content("memory_limit"), "134217728");
    assert_eq!(paths.content("cpu_request"), "1");
    assert_eq!(paths.content("memory_request"), "33554432");
}

/// [sig-storage] Downward API volume should provide node allocatable (cpu|memory)
/// as default limit if the limit is not set [NodeConformance] [Conformance]
/// Upstream: downwardapi_volume.go:250, :262 (`TestContainerOutputRegexp`
/// `[1-9]`: the container sets no limit, so the file holds the node's
/// allocatable)
#[tokio::test]
async fn downwardapi_volume_should_provide_node_allocatable_as_default_limit() {
    let n = Node::new();
    let p = dapi_pod(
        "downwardapi-volume-x",
        json!({"items": [
            resource_item("cpu_limit", "limits.cpu"),
            resource_item("memory_limit", "limits.memory"),
        ]}),
        Value::Null,
    );
    let paths = n.setup(&p).await;
    for file in ["cpu_limit", "memory_limit"] {
        let got = paths.content(file);
        assert!(
            got.chars().any(|c| ('1'..='9').contains(&c)),
            "{file} must default to the node's allocatable, got {got:?}"
        );
    }
}
