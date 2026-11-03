//! Storage version migrator resource-version controller, ported from
//! `pkg/controller/storageversionmigrator/resourceversion_test.go`
//! (`TestRVSync`, `TestIsResourceMigratable`, release-1.35) through the same
//! seams the upstream test fakes (discovery and the metadata list).

use async_trait::async_trait;
use rusternetes_common::resources::storageversionmigration::StorageVersionMigration;
use rusternetes_controller_manager::controllers::storage_version_migrator_rv::{
    LatestResourceVersion, ResourceVersionController, SvmDiscovery, FAKE_SVM_NAMESPACE_NAME,
};
use rusternetes_discovery::restmapper::{
    new_discovery_rest_mapper, ApiGroupResources, ApiResource, Gvr, PriorityRestMapper,
    ServedResource,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const PLURAL: &str = "storageversionmigrations";

struct FakeDiscovery {
    /// `None`: the group-version is a 404.
    resources: Option<Vec<ServedResource>>,
}

#[async_trait]
impl SvmDiscovery for FakeDiscovery {
    async fn rest_mapper(&self) -> anyhow::Result<PriorityRestMapper> {
        let mut versioned = BTreeMap::new();
        versioned.insert(
            "v1".to_string(),
            vec![ApiResource {
                name: "deployments".into(),
                singular_name: "deployment".into(),
                namespaced: true,
                kind: "Deployment".into(),
            }],
        );
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
        Ok(self.resources.clone())
    }
}

struct FakeMetadata {
    rv: Result<String, String>,
    namespaces: Mutex<Vec<Option<String>>>,
}

#[async_trait]
impl LatestResourceVersion for FakeMetadata {
    async fn list_resource_version(
        &self,
        _gvr: &Gvr,
        namespace: Option<&str>,
    ) -> anyhow::Result<String> {
        self.namespaces
            .lock()
            .unwrap()
            .push(namespace.map(str::to_string));
        self.rv.clone().map_err(|e| anyhow::anyhow!(e))
    }
}

fn deployments(verbs: &[&str]) -> Option<Vec<ServedResource>> {
    Some(vec![ServedResource {
        name: "deployments".into(),
        namespaced: true,
        verbs: verbs.iter().map(|v| v.to_string()).collect(),
    }])
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
    metadata: Arc<FakeMetadata>,
    controller: ResourceVersionController<MemoryStorage>,
}

fn harness(resources: Option<Vec<ServedResource>>, rv: Result<&str, &str>) -> Harness {
    let storage = Arc::new(MemoryStorage::new());
    let metadata = Arc::new(FakeMetadata {
        rv: rv.map(str::to_string).map_err(str::to_string),
        namespaces: Mutex::new(vec![]),
    });
    let controller = ResourceVersionController::with_seams(
        storage.clone(),
        Arc::new(FakeDiscovery { resources }),
        metadata.clone(),
    );
    Harness {
        storage,
        metadata,
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

fn has_condition(s: &StorageVersionMigration, ty: &str) -> bool {
    s.status
        .conditions
        .iter()
        .any(|c| c.condition_type == ty && c.status == "True")
}

const OK_VERBS: [&str; 3] = ["list", "update", "patch"];

#[tokio::test]
async fn successful_rv_acquisition() {
    let h = harness(deployments(&OK_VERBS), Ok("12345"));
    put(&h, svm("test-svm", "", json!([]), 0)).await;
    h.controller
        .sync("storageversionmigrations/test-svm")
        .await
        .unwrap();
    assert_eq!(get(&h, "test-svm").await.status.resource_version, "12345");
    // A namespaced resource is listed from the fake namespace.
    assert_eq!(
        *h.metadata.namespaces.lock().unwrap(),
        vec![Some(FAKE_SVM_NAMESPACE_NAME.to_string())]
    );
}

#[tokio::test]
async fn svm_not_found() {
    let h = harness(None, Ok("1"));
    h.controller.sync("non-existent-svm").await.unwrap();
}

#[tokio::test]
async fn already_succeeded_or_failed_or_rv_set_is_skipped() {
    for (name, rv, cond) in [
        (
            "succeeded",
            "100",
            json!([{"type": "Succeeded", "status": "True"}]),
        ),
        (
            "failed",
            "100",
            json!([{"type": "Failed", "status": "True"}]),
        ),
        ("rv-set", "123", json!([])),
    ] {
        let h = harness(deployments(&OK_VERBS), Ok("999"));
        put(&h, svm(name, rv, cond, 0)).await;
        h.controller.sync(name).await.unwrap();
        assert_eq!(get(&h, name).await.status.resource_version, rv, "{name}");
        assert!(h.metadata.namespaces.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn resource_not_migratable_fails_the_migration() {
    // Each missing verb of `verbsRequiredForMigration` (update, patch, list).
    for verbs in [
        &["update", "patch"][..],
        &["list", "patch"][..],
        &["list", "update"][..],
    ] {
        let h = harness(deployments(verbs), Ok("12345"));
        put(&h, svm("nm", "", json!([]), 0)).await;
        h.controller.sync("nm").await.unwrap();
        let s = get(&h, "nm").await;
        assert!(has_condition(&s, "Failed"), "{verbs:?}");
        assert_eq!(s.status.resource_version, "");
        let c = &s.status.conditions[0];
        assert_eq!(c.reason.as_deref(), Some("StorageVersionMigrationFailed"));
        assert!(c
            .message
            .as_deref()
            .unwrap()
            .contains("does not support discovery operations"));
    }
}

#[tokio::test]
async fn group_version_not_served_is_not_migratable() {
    let h = harness(None, Ok("12345"));
    put(&h, svm("nf", "", json!([]), 0)).await;
    h.controller.sync("nf").await.unwrap();
    assert!(has_condition(&get(&h, "nf").await, "Failed"));
}

#[tokio::test]
async fn resource_missing_from_discovery_document_is_an_error() {
    let h = harness(
        Some(vec![ServedResource {
            name: "other".into(),
            namespaced: true,
            verbs: OK_VERBS.map(String::from).to_vec(),
        }]),
        Ok("12345"),
    );
    put(&h, svm("unknown", "", json!([]), 0)).await;
    let err = h.controller.sync("unknown").await.unwrap_err().to_string();
    assert!(err.contains("not found in discovery"), "{err}");
    assert!(get(&h, "unknown").await.status.conditions.is_empty());
}

#[tokio::test]
async fn metadata_list_error_is_returned_for_requeue() {
    let h = harness(deployments(&OK_VERBS), Err("boom"));
    put(&h, svm("me", "", json!([]), 0)).await;
    let err = h.controller.sync("me").await.unwrap_err().to_string();
    assert!(
        err.contains("error getting latest resourceVersion for apps/v1"),
        "{err}"
    );
}

#[tokio::test]
async fn invalid_rv_fails_the_migration() {
    for bad in ["abcde", "", "0", "012"] {
        let h = harness(deployments(&OK_VERBS), Ok(bad));
        put(&h, svm("inv", "", json!([]), 0)).await;
        h.controller.sync("inv").await.unwrap();
        let s = get(&h, "inv").await;
        assert!(has_condition(&s, "Failed"), "{bad:?}");
        assert_eq!(s.status.resource_version, "");
    }
}

#[tokio::test]
async fn unknown_resource_requeues_when_young_and_fails_when_old() {
    let h = harness(deployments(&OK_VERBS), Ok("1"));
    let mut young = svm("young", "", json!([]), 5);
    young["spec"]["resource"]["resource"] = json!("widgets");
    put(&h, young).await;
    let err = h.controller.sync("young").await.unwrap_err().to_string();
    assert!(err.contains("does not exist in our rest mapper"), "{err}");

    let mut old = svm("old", "", json!([]), 120);
    old["spec"]["resource"]["resource"] = json!("widgets");
    put(&h, old).await;
    h.controller.sync("old").await.unwrap();
    let s = get(&h, "old").await;
    assert!(has_condition(&s, "Failed"));
    assert_eq!(
        s.status.conditions[0].message.as_deref(),
        Some("resource does not exist in discovery")
    );
}

/// The production wiring: this server's own discovery, RESTMapper and store
/// revision, for a built-in resource and for an established CRD.
#[tokio::test]
async fn storage_backed_seams_stamp_the_store_revision() {
    let storage = Arc::new(MemoryStorage::new());
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Cluster",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
            "versions": [{"name": "v1", "served": true, "storage": true}],
        },
        "status": {"conditions": [{"type": "Established", "status": "True"}]},
    });
    storage
        .create(
            &build_key("customresourcedefinitions", None, "widgets.example.com"),
            &crd,
        )
        .await
        .unwrap();
    for (name, group, resource) in [
        ("dep", "apps", "deployments"),
        ("widgets", "example.com", "widgets"),
    ] {
        let mut v = svm(name, "", json!([]), 0);
        v["spec"]["resource"] = json!({"group": group, "resource": resource});
        storage
            .create(&build_key(PLURAL, None, name), &v)
            .await
            .unwrap();
    }
    let c = ResourceVersionController::new(storage.clone());
    for name in ["dep", "widgets"] {
        c.sync(name).await.unwrap();
        let s: StorageVersionMigration = storage.get(&build_key(PLURAL, None, name)).await.unwrap();
        assert!(
            s.status.resource_version.parse::<u64>().unwrap() > 0,
            "{name}: {:?}",
            s.status
        );
    }
}
