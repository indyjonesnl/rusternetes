//! Storage version migrator migration controller, ported from
//! `pkg/controller/storageversionmigrator/storageversionmigrator_test.go`
//! (`TestSync`, release-1.35) through the seams the upstream test fakes: the
//! garbage collector's monitor and the dynamic client's apply `Patch`.

use async_trait::async_trait;
use rusternetes_common::resources::storageversionmigration::StorageVersionMigration;
use rusternetes_controller_manager::controllers::storage_version_migrator_rv::SvmDiscovery;
use rusternetes_controller_manager::controllers::storage_version_migrator_svm::{
    GraphMonitor, MigrationPatcher, Monitor, MonitoredObject, PatchError, SvmController,
};
use rusternetes_discovery::restmapper::{
    new_discovery_rest_mapper, ApiGroupResources, ApiResource, Gvr, PriorityRestMapper,
    ServedResource,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

const PLURAL: &str = "storageversionmigrations";
const MANAGER: &str = "storage-version-migrator-controller";

struct FakeDiscovery {
    mapped: bool,
}

#[async_trait]
impl SvmDiscovery for FakeDiscovery {
    async fn rest_mapper(&self) -> anyhow::Result<PriorityRestMapper> {
        let mut versioned = BTreeMap::new();
        if self.mapped {
            versioned.insert(
                "v1".to_string(),
                vec![ApiResource {
                    name: "deployments".into(),
                    singular_name: "deployment".into(),
                    namespaced: true,
                    kind: "Deployment".into(),
                }],
            );
        }
        Ok(new_discovery_rest_mapper(&[ApiGroupResources {
            name: "apps".into(),
            versions: vec!["v1".into()],
            preferred_version: "v1".into(),
            versioned_resources: versioned,
        }]))
    }

    async fn server_resources_for_group_version(
        &self,
        _group: &str,
        _version: &str,
    ) -> anyhow::Result<Option<Vec<ServedResource>>> {
        Ok(None)
    }
}

/// `mockGraphBuilder` / `mockMonitor`.
struct FakeMonitor(Option<Monitor>);

#[async_trait]
impl GraphMonitor for FakeMonitor {
    async fn get_monitor(&self, _gvr: &Gvr) -> anyhow::Result<Option<Monitor>> {
        Ok(self.0.clone())
    }
}

/// The dynamic fake client: records every apply, fails the keys it is told to.
#[derive(Default)]
struct FakePatcher {
    calls: Mutex<Vec<(String, String, Value, String)>>,
    errors: HashMap<String, fn() -> PatchError>,
}

#[async_trait]
impl MigrationPatcher for FakePatcher {
    async fn apply(
        &self,
        _gvr: &Gvr,
        namespace: Option<&str>,
        name: &str,
        body: &Value,
        field_manager: &str,
    ) -> Result<(), PatchError> {
        let ns = namespace.unwrap_or("").to_string();
        self.calls.lock().unwrap().push((
            ns.clone(),
            name.to_string(),
            body.clone(),
            field_manager.to_string(),
        ));
        match self.errors.get(&format!("{ns}/{name}")) {
            Some(e) => Err(e()),
            None => Ok(()),
        }
    }
}

fn obj(name: &str, ns: &str, rv: &str, uid: &str) -> MonitoredObject {
    MonitoredObject {
        namespace: Some(ns.to_string()),
        name: name.to_string(),
        uid: uid.to_string(),
        resource_version: rv.to_string(),
    }
}

fn monitor(last_sync: &str, objects: Vec<MonitoredObject>) -> Option<Monitor> {
    Some(Monitor {
        last_sync_resource_version: last_sync.to_string(),
        objects,
    })
}

fn svm(name: &str, rv: &str, conditions: Value, age_secs: i64) -> Value {
    let created = chrono::Utc::now() - chrono::Duration::seconds(age_secs);
    json!({
        "apiVersion": "storagemigration.k8s.io/v1beta1",
        "kind": "StorageVersionMigration",
        "metadata": {"name": name, "uid": format!("uid-{name}"), "creationTimestamp": created.to_rfc3339()},
        "spec": {"resource": {"group": "apps", "resource": "deployments"}},
        "status": {"resourceVersion": rv, "conditions": conditions},
    })
}

struct Harness {
    storage: Arc<MemoryStorage>,
    patcher: Arc<FakePatcher>,
    controller: SvmController<MemoryStorage>,
}

fn harness(mon: Option<Monitor>, patcher: FakePatcher, mapped: bool) -> Harness {
    let storage = Arc::new(MemoryStorage::new());
    let patcher = Arc::new(patcher);
    let controller = SvmController::with_seams(
        storage.clone(),
        MANAGER,
        Arc::new(FakeDiscovery { mapped }),
        Arc::new(FakeMonitor(mon)),
        patcher.clone(),
    );
    Harness {
        storage,
        patcher,
        controller,
    }
}

async fn put(h: &Harness, v: Value) {
    let name = v["metadata"]["name"].as_str().unwrap().to_string();
    h.storage
        .create(&build_key(PLURAL, None, &name), &v)
        .await
        .unwrap();
}

async fn get(h: &Harness, name: &str) -> StorageVersionMigration {
    h.storage.get(&build_key(PLURAL, None, name)).await.unwrap()
}

fn status_of(s: &StorageVersionMigration, ty: &str) -> Option<String> {
    s.status
        .conditions
        .iter()
        .find(|c| c.condition_type == ty)
        .map(|c| c.status.clone())
}

fn calls(h: &Harness) -> Vec<(String, String, Value, String)> {
    h.patcher.calls.lock().unwrap().clone()
}

#[tokio::test]
async fn successful_migration_skips_objects_past_the_checkpoint() {
    let mon = monitor(
        "100",
        vec![
            obj("res1", "ns1", "90", "uid1"),
            obj("res2", "ns1", "100", "uid2"),
            obj("res3", "ns2", "101", "uid3"), // past the checkpoint: skipped
        ],
    );
    let h = harness(mon, FakePatcher::default(), true);
    put(&h, svm("test-svm", "100", json!([]), 0)).await;

    h.controller.sync("test-svm").await.unwrap();

    let c = calls(&h);
    assert_eq!(c.len(), 2);
    assert_eq!(
        (c[0].0.as_str(), c[0].1.as_str(), &c[0].3[..]),
        ("ns1", "res1", MANAGER)
    );
    // The apply body carries only apiVersion, kind, uid and resourceVersion.
    assert_eq!(
        c[0].2,
        json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"uid": "uid1", "resourceVersion": "90"},
        })
    );
    assert_eq!(c[1].1, "res2");

    let got = get(&h, "test-svm").await;
    assert_eq!(status_of(&got, "Running").as_deref(), Some("False"));
    assert_eq!(status_of(&got, "Succeeded").as_deref(), Some("True"));
}

#[tokio::test]
async fn svm_not_found_is_not_an_error() {
    let h = harness(None, FakePatcher::default(), true);
    h.controller.sync("non-existent-svm").await.unwrap();
}

#[tokio::test]
async fn already_succeeded_is_skipped() {
    let h = harness(
        monitor("100", vec![obj("r", "ns", "1", "u")]),
        FakePatcher::default(),
        true,
    );
    put(
        &h,
        svm(
            "done",
            "100",
            json!([{"type": "Succeeded", "status": "True"}]),
            0,
        ),
    )
    .await;
    h.controller.sync("done").await.unwrap();
    assert!(calls(&h).is_empty());
}

#[tokio::test]
async fn an_svm_without_a_resource_version_waits() {
    let h = harness(
        monitor("100", vec![obj("r", "ns", "1", "u")]),
        FakePatcher::default(),
        true,
    );
    put(&h, svm("no-rv", "", json!([]), 0)).await;
    h.controller.sync("no-rv").await.unwrap();
    assert!(calls(&h).is_empty());
    assert!(get(&h, "no-rv").await.status.conditions.is_empty());
}

#[tokio::test]
async fn stale_gc_cache_requeues() {
    let h = harness(monitor("99", vec![]), FakePatcher::default(), true);
    put(&h, svm("stale-gc-svm", "100", json!([]), 0)).await;
    let err = h.controller.sync("stale-gc-svm").await.unwrap_err();
    assert!(err.to_string().contains("GC cache is not up to date"));
    assert!(get(&h, "stale-gc-svm").await.status.conditions.is_empty());
}

#[tokio::test]
async fn resource_not_in_gc_fails_an_old_svm() {
    let h = harness(None, FakePatcher::default(), true);
    put(&h, svm("no-resource", "100", json!([]), 120)).await;
    h.controller.sync("no-resource").await.unwrap();
    let got = get(&h, "no-resource").await;
    assert_eq!(status_of(&got, "Failed").as_deref(), Some("True"));
}

#[tokio::test]
async fn resource_not_in_gc_requeues_a_young_svm() {
    let h = harness(None, FakePatcher::default(), true);
    put(&h, svm("young", "100", json!([]), 0)).await;
    assert!(h.controller.sync("young").await.is_err());
    assert!(get(&h, "young").await.status.conditions.is_empty());
}

#[tokio::test]
async fn resource_missing_from_the_mapper_fails_an_old_svm() {
    let h = harness(monitor("100", vec![]), FakePatcher::default(), false);
    put(&h, svm("unmapped", "100", json!([]), 120)).await;
    h.controller.sync("unmapped").await.unwrap();
    let got = get(&h, "unmapped").await;
    assert_eq!(status_of(&got, "Failed").as_deref(), Some("True"));
    let msg = got
        .status
        .conditions
        .iter()
        .find(|c| c.condition_type == "Failed")
        .and_then(|c| c.message.clone())
        .unwrap();
    assert!(msg.contains("unable to obtain GVR from discovery"), "{msg}");
}

#[tokio::test]
async fn fatal_patch_error_fails_the_migration() {
    let mut p = FakePatcher::default();
    p.errors.insert("ns1/res1".into(), || {
        PatchError::Fatal("fatal error".into())
    });
    let h = harness(
        monitor("100", vec![obj("res1", "ns1", "90", "uid1")]),
        p,
        true,
    );
    put(&h, svm("fatal-error-svm", "100", json!([]), 0)).await;

    h.controller.sync("fatal-error-svm").await.unwrap();

    let got = get(&h, "fatal-error-svm").await;
    assert_eq!(status_of(&got, "Running").as_deref(), Some("False"));
    assert_eq!(status_of(&got, "Failed").as_deref(), Some("True"));
    assert_eq!(status_of(&got, "Succeeded"), None);
}

#[tokio::test]
async fn conflict_on_patch_is_ignored() {
    let mut p = FakePatcher::default();
    p.errors.insert("ns1/res1".into(), || {
        PatchError::Conflict("conflict".into())
    });
    let h = harness(
        monitor(
            "100",
            vec![
                obj("res1", "ns1", "90", "uid1"),
                obj("res2", "ns1", "95", "uid2"),
            ],
        ),
        p,
        true,
    );
    put(&h, svm("conflict-svm", "100", json!([]), 0)).await;

    h.controller.sync("conflict-svm").await.unwrap();

    // The conflicting object is skipped and the next one still migrated.
    assert_eq!(calls(&h).len(), 2);
    let got = get(&h, "conflict-svm").await;
    assert_eq!(status_of(&got, "Succeeded").as_deref(), Some("True"));
}

#[tokio::test]
async fn retriable_patch_error_is_returned_and_leaves_the_svm_running() {
    let mut p = FakePatcher::default();
    p.errors.insert("ns1/res1".into(), || {
        PatchError::Retriable("too many requests".into())
    });
    let h = harness(
        monitor("100", vec![obj("res1", "ns1", "90", "uid1")]),
        p,
        true,
    );
    put(&h, svm("retriable-svm", "100", json!([]), 0)).await;

    assert!(h.controller.sync("retriable-svm").await.is_err());

    let got = get(&h, "retriable-svm").await;
    assert_eq!(status_of(&got, "Running").as_deref(), Some("True"));
    assert_eq!(status_of(&got, "Failed"), None);
    assert_eq!(status_of(&got, "Succeeded"), None);
}

#[tokio::test]
async fn incomparable_gc_resource_version_fails_the_migration() {
    let h = harness(
        monitor("not-a-number", vec![obj("r", "ns", "90", "u")]),
        FakePatcher::default(),
        true,
    );
    put(&h, svm("bad-gc-rv", "100", json!([]), 0)).await;
    h.controller.sync("bad-gc-rv").await.unwrap();
    let got = get(&h, "bad-gc-rv").await;
    assert_eq!(status_of(&got, "Failed").as_deref(), Some("True"));
    assert!(calls(&h).is_empty());
}

#[tokio::test]
async fn incomparable_object_resource_version_fails_the_migration() {
    let h = harness(
        monitor("100", vec![obj("r", "ns", "oops", "u")]),
        FakePatcher::default(),
        true,
    );
    put(&h, svm("bad-obj-rv", "100", json!([]), 0)).await;
    h.controller.sync("bad-obj-rv").await.unwrap();
    let got = get(&h, "bad-obj-rv").await;
    assert_eq!(status_of(&got, "Running").as_deref(), Some("False"));
    assert_eq!(status_of(&got, "Failed").as_deref(), Some("True"));
    assert!(calls(&h).is_empty());
}

#[tokio::test]
async fn storage_monitor_lists_every_namespace_with_uid_and_resource_version() {
    use rusternetes_controller_manager::controllers::storage_version_migrator_svm::StorageGraphMonitor;
    let storage = Arc::new(MemoryStorage::new());
    for (ns, name) in [("a", "one"), ("b", "two")] {
        storage
            .create(
                &build_key("widgets", Some(ns), name),
                &json!({
                    "apiVersion": "example.com/v1", "kind": "Widget",
                    "metadata": {"name": name, "namespace": ns, "uid": format!("uid-{name}")},
                }),
            )
            .await
            .unwrap();
    }
    let before = storage.current_revision().await.unwrap();
    let m = StorageGraphMonitor::new(storage.clone())
        .get_monitor(&Gvr::new("example.com", "v1", "widgets"))
        .await
        .unwrap()
        .expect("always synced");
    assert!(m.last_sync_resource_version.parse::<i64>().unwrap() >= before);
    assert_eq!(m.objects.len(), 2);
    for o in &m.objects {
        assert!(!o.uid.is_empty());
        assert!(
            o.resource_version.parse::<u64>().is_ok(),
            "resourceVersion must be a number: {:?}",
            o.resource_version
        );
    }
}
