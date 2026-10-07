use super::*;
use rusternetes_common::resources::volume::{VolumeSnapshotSource, VolumeSnapshotSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::memory::MemoryStorage;

#[test]
fn test_is_driver_supported() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VolumeSnapshotController::new(storage);

    assert!(controller.is_driver_supported("rusternetes.io/hostpath-snapshotter"));
    assert!(controller.is_driver_supported("hostpath-snapshotter"));
    assert!(!controller.is_driver_supported("kubernetes.io/aws-ebs"));
}

#[test]
fn test_create_snapshot_content_structure() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VolumeSnapshotController::new(storage);

    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("test-class"),
        driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Delete,
    };

    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-snapshot");
            meta.namespace = Some("default".to_string());
            meta.uid = "test-uid-123".to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: VolumeSnapshotSource {
                persistent_volume_claim_name: Some("test-pvc".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: Some("test-class".to_string()),
        },
        status: None,
    };

    let content = controller
        .create_snapshot_content(&vsc, &vs, "test-content", "test-pv")
        .unwrap();

    assert_eq!(content.metadata.name, "test-content");
    assert_eq!(content.spec.driver, "rusternetes.io/hostpath-snapshotter");
    assert_eq!(content.spec.deletion_policy, DeletionPolicy::Delete);
    assert_eq!(
        content.spec.volume_snapshot_ref.name,
        Some("test-snapshot".to_string())
    );
    assert_eq!(
        content.spec.volume_snapshot_ref.namespace,
        Some("default".to_string())
    );
    assert_eq!(
        content.spec.source.volume_handle,
        Some("test-pv".to_string())
    );
    assert!(content.status.is_some());
    assert_eq!(content.status.as_ref().unwrap().ready_to_use, Some(true));
}

#[test]
fn test_create_snapshot_content_with_retain_policy() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VolumeSnapshotController::new(storage);

    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("test-class"),
        driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Retain,
    };

    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-snapshot");
            meta.namespace = Some("default".to_string());
            meta.uid = "test-uid-123".to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: VolumeSnapshotSource {
                persistent_volume_claim_name: Some("test-pvc".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: Some("test-class".to_string()),
        },
        status: None,
    };

    let content = controller
        .create_snapshot_content(&vsc, &vs, "test-content", "test-pv")
        .unwrap();

    assert_eq!(content.spec.deletion_policy, DeletionPolicy::Retain);
}

#[test]
fn test_snapshot_handle_uniqueness() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = VolumeSnapshotController::new(storage);

    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("test-class"),
        driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Delete,
    };

    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-snapshot");
            meta.namespace = Some("default".to_string());
            meta.uid = "test-uid-123".to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: VolumeSnapshotSource {
                persistent_volume_claim_name: Some("test-pvc".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: Some("test-class".to_string()),
        },
        status: None,
    };

    let content1 = controller
        .create_snapshot_content(&vsc, &vs, "test-content-1", "test-pv")
        .unwrap();

    let content2 = controller
        .create_snapshot_content(&vsc, &vs, "test-content-2", "test-pv")
        .unwrap();

    // Snapshot handles should be unique
    assert_ne!(
        content1.status.as_ref().unwrap().snapshot_handle,
        content2.status.as_ref().unwrap().snapshot_handle
    );
}

fn orphan_content(name: &str, snapshot: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent",
        "metadata": {"name": name, "uid": format!("uid-{name}")},
        "spec": {
            "source": {"volumeHandle": "h"},
            "volumeSnapshotRef": {"namespace": "default", "name": snapshot},
            "deletionPolicy": "Delete",
            "driver": "hostpath-snapshotter"
        }
    })
}

/// external-snapshotter `deleteSnapshot`
/// (`pkg/common-controller/snapshot_controller_base.go:585`) acts only on the
/// snapshot whose delete event it was handed, never on a sweep of every
/// content. A worker handed one key must leave other keys' contents alone.
#[tokio::test]
async fn deleted_snapshot_key_only_releases_its_own_content() {
    let storage = Arc::new(MemoryStorage::new());
    for (c, s) in [("content-a", "snap-a"), ("content-b", "snap-b")] {
        storage
            .create(
                &build_key("volumesnapshotcontents", None, c),
                &orphan_content(c, s),
            )
            .await
            .unwrap();
    }
    let controller = VolumeSnapshotController::new(Arc::clone(&storage));

    controller
        .process_key("volumesnapshots/default/snap-a")
        .await
        .unwrap();

    let a = storage
        .get::<serde_json::Value>(&build_key("volumesnapshotcontents", None, "content-a"))
        .await;
    let b = storage
        .get::<serde_json::Value>(&build_key("volumesnapshotcontents", None, "content-b"))
        .await;
    assert!(a.is_err(), "content-a should be deleted");
    assert!(b.is_ok(), "content-b belongs to another key and must stay");
}
