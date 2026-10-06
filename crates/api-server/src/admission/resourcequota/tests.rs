//! Ports of plugin/pkg/admission/resourcequota/admission_test.go for the
//! evaluators ported here (services and object counts), run against
//! [`MemoryStorage`] in place of the fake clientset.

use std::collections::HashMap;

use rusternetes_common::admission::Operation;
use rusternetes_common::resources::{ResourceQuota, ResourceQuotaStatus};
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use serde_json::{json, Value};

use super::evaluator::{evaluator_for, ObjectCountEvaluator, ServiceEvaluator};
use super::{check_request, evaluate, Attributes, QuotaError};
use crate::registry::rest::GroupResource;

fn list(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn quota(hard: &[(&str, &str)], used: &[(&str, &str)]) -> ResourceQuota {
    let mut q: ResourceQuota = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "ResourceQuota",
        "metadata": {"name": "quota", "namespace": "test"},
    }))
    .unwrap();
    q.spec.hard = Some(list(hard));
    q.status = Some(ResourceQuotaStatus {
        hard: Some(list(hard)),
        used: Some(list(used)),
    });
    q
}

async fn stored(storage: &MemoryStorage, q: &ResourceQuota) -> ResourceQuota {
    storage
        .create(&build_key("resourcequotas", Some("test"), "quota"), q)
        .await
        .unwrap()
}

async fn used(storage: &MemoryStorage) -> HashMap<String, String> {
    let q: ResourceQuota = storage
        .get(&build_key("resourcequotas", Some("test"), "quota"))
        .await
        .unwrap();
    q.status.unwrap().used.unwrap()
}

fn service(rv: &str, spec: Value) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": "service", "namespace": "test", "resourceVersion": rv},
        "spec": spec,
    })
}

fn configmap() -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap",
           "metadata": {"name": "cm", "namespace": "test"}})
}

fn attrs<'a>(op: Operation, obj: &'a Value, old: Option<&'a Value>) -> Attributes<'a> {
    Attributes {
        operation: op,
        namespace: "test",
        subresource: None,
        object: obj,
        old_object: old,
        dry_run: false,
    }
}

fn services_quota(used_lb: &str) -> ResourceQuota {
    quota(
        &[
            ("services", "10"),
            ("services.loadbalancers", "10"),
            ("services.nodeports", "10"),
        ],
        &[
            ("services", "1"),
            ("services.loadbalancers", used_lb),
            ("services.nodeports", "0"),
        ],
    )
}

/// `TestAdmissionIgnoresDelete`.
#[tokio::test]
async fn admission_ignores_delete() {
    let storage = MemoryStorage::new();
    stored(
        &storage,
        &quota(&[("count/configmaps", "0")], &[("count/configmaps", "0")]),
    )
    .await;
    let cm = configmap();
    let ev = evaluator_for(&GroupResource::new("", "configmaps")).unwrap();
    evaluate(&storage, &*ev, &attrs(Operation::Delete, &cm, None))
        .await
        .unwrap();
}

/// `TestAdmissionIgnoresSubresources`, for an object count.
#[tokio::test]
async fn admission_ignores_subresources() {
    let storage = MemoryStorage::new();
    stored(
        &storage,
        &quota(&[("configmaps", "1")], &[("configmaps", "1")]),
    )
    .await;
    let cm = configmap();
    let ev = evaluator_for(&GroupResource::new("", "configmaps")).unwrap();
    let err = evaluate(&storage, &*ev, &attrs(Operation::Create, &cm, None))
        .await
        .unwrap_err();
    assert_eq!(
        err,
        QuotaError::Forbidden(
            "exceeded quota: quota, requested: configmaps=1, used: configmaps=1, limited: configmaps=1"
                .to_string()
        )
    );
    let mut sub = attrs(Operation::Create, &cm, None);
    sub.subresource = Some("subresource");
    evaluate(&storage, &*ev, &sub).await.unwrap();
}

/// `TestAdmitBelowQuotaLimit`, for an object count: the usage is recorded
/// on the quota. The legacy alias and `count/` name are both charged.
#[tokio::test]
async fn admit_below_quota_limit_records_usage() {
    let storage = MemoryStorage::new();
    stored(
        &storage,
        &quota(
            &[("configmaps", "5"), ("count/configmaps", "5")],
            &[("configmaps", "3"), ("count/configmaps", "3")],
        ),
    )
    .await;
    let cm = configmap();
    let ev = evaluator_for(&GroupResource::new("", "configmaps")).unwrap();
    evaluate(&storage, &*ev, &attrs(Operation::Create, &cm, None))
        .await
        .unwrap();
    let used = used(&storage).await;
    assert_eq!(used["configmaps"], "4");
    assert_eq!(used["count/configmaps"], "4");
}

/// `TestAdmitDryRun`: a dry run is still rejected, and records nothing.
#[tokio::test]
async fn admit_dry_run() {
    let storage = MemoryStorage::new();
    stored(
        &storage,
        &quota(&[("configmaps", "4")], &[("configmaps", "3")]),
    )
    .await;
    let cm = configmap();
    let ev = evaluator_for(&GroupResource::new("", "configmaps")).unwrap();
    let mut a = attrs(Operation::Create, &cm, None);
    a.dry_run = true;
    evaluate(&storage, &*ev, &a).await.unwrap();
    assert_eq!(used(&storage).await["configmaps"], "3");

    let storage = MemoryStorage::new();
    stored(
        &storage,
        &quota(&[("configmaps", "3")], &[("configmaps", "3")]),
    )
    .await;
    assert!(evaluate(&storage, &*ev, &a).await.is_err());
}

/// `TestAdmitHandlesOldObjects`: a LoadBalancer turned NodePort is charged
/// the node port, and the load balancer it gave up is not credited.
#[tokio::test]
async fn admit_handles_old_objects() {
    let storage = MemoryStorage::new();
    stored(&storage, &services_quota("1")).await;
    let old = service("1", json!({"type": "LoadBalancer"}));
    let new = service("", json!({"type": "NodePort", "ports": [{"port": 1234}]}));
    evaluate(
        &storage,
        &ServiceEvaluator,
        &attrs(Operation::Update, &new, Some(&old)),
    )
    .await
    .unwrap();
    let used = used(&storage).await;
    assert_eq!(used["services"], "1");
    assert_eq!(used["services.loadbalancers"], "1");
    assert_eq!(used["services.nodeports"], "1");
}

/// `TestAdmitHandlesCreatingUpdates`: an old object without a
/// resourceVersion is a create-on-update, charged in full.
#[tokio::test]
async fn admit_handles_creating_updates() {
    let storage = MemoryStorage::new();
    stored(&storage, &services_quota("1")).await;
    let old = service("", json!({"type": "LoadBalancer"}));
    let new = service("", json!({"type": "NodePort", "ports": [{"port": 1234}]}));
    evaluate(
        &storage,
        &ServiceEvaluator,
        &attrs(Operation::Update, &new, Some(&old)),
    )
    .await
    .unwrap();
    let used = used(&storage).await;
    assert_eq!(used["services"], "2");
    assert_eq!(used["services.loadbalancers"], "1");
    assert_eq!(used["services.nodeports"], "1");
}

/// The e2e "capture the life of a service" shape: a LoadBalancer with a
/// node port for each of its ports is rejected past `services.nodeports`.
#[tokio::test]
async fn admit_exceed_quota_limit_for_node_ports() {
    let storage = MemoryStorage::new();
    stored(
        &storage,
        &quota(
            &[("services.nodeports", "1"), ("services.loadbalancers", "1")],
            &[("services.nodeports", "1"), ("services.loadbalancers", "0")],
        ),
    )
    .await;
    let new = service("", json!({"type": "LoadBalancer", "ports": [{"port": 80}]}));
    let err = evaluate(
        &storage,
        &ServiceEvaluator,
        &attrs(Operation::Create, &new, None),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        QuotaError::Forbidden(
            "exceeded quota: quota, requested: services.nodeports=1, used: services.nodeports=1, limited: services.nodeports=1"
                .to_string()
        )
    );
}

/// `TestAdmitWhenUnrelatedResourceExceedsQuota`: a quota already over on a
/// resource the request does not consume does not reject it.
#[tokio::test]
async fn admit_when_unrelated_resource_exceeds_quota() {
    let quotas = [quota(
        &[("services", "3"), ("configmaps", "4")],
        &[("services", "4"), ("configmaps", "1")],
    )];
    let cm = configmap();
    let ev = evaluator_for(&GroupResource::new("", "configmaps")).unwrap();
    let out = check_request(&quotas, &attrs(Operation::Create, &cm, None), &*ev).unwrap();
    assert_eq!(
        out[0].status.as_ref().unwrap().used.as_ref().unwrap()["configmaps"],
        "2"
    );
}

/// `hasUsageStats`: a quota the controller has not counted yet rejects.
#[test]
fn status_unknown_rejects() {
    let quotas = [quota(&[("count/deployments.apps", "2")], &[])];
    let d = json!({"metadata": {"name": "d", "namespace": "test"}});
    let ev = evaluator_for(&GroupResource::new("apps", "deployments")).unwrap();
    let err = check_request(&quotas, &attrs(Operation::Create, &d, None), &*ev).unwrap_err();
    assert_eq!(
        err,
        QuotaError::Forbidden(
            "status unknown for quota: quota, resources: count/deployments.apps".to_string()
        )
    );
}

/// `TestAdmitAllowDecreaseUsageWithoutCoveringQuota` without limited
/// resources: an update the quota does not cover passes untouched, and an
/// object count never charges an update.
#[test]
fn updates_that_decrease_usage_pass() {
    let quotas = [services_quota("1")];
    let old = service("1", json!({"type": "LoadBalancer"}));
    let new = service("", json!({"type": "ClusterIP"}));
    let out = check_request(
        &quotas,
        &attrs(Operation::Update, &new, Some(&old)),
        &ServiceEvaluator,
    )
    .unwrap();
    assert_eq!(out[0].status, quotas[0].status);

    let cm = configmap();
    let ev = ObjectCountEvaluator::new(&GroupResource::new("", "configmaps"), Some("configmaps"));
    let quotas = [quota(&[("configmaps", "1")], &[("configmaps", "1")])];
    check_request(&quotas, &attrs(Operation::Update, &cm, Some(&cm)), &ev).unwrap();
}

/// A scoped quota never matches the object-count and service evaluators
/// (`MatchesNoScopeFunc`).
#[test]
fn scoped_quotas_do_not_match() {
    let mut q = quota(&[("configmaps", "0")], &[("configmaps", "0")]);
    q.spec.scopes = Some(vec!["NotTerminating".to_string()]);
    let cm = configmap();
    let ev = evaluator_for(&GroupResource::new("", "configmaps")).unwrap();
    check_request(&[q], &attrs(Operation::Create, &cm, None), &*ev).unwrap();
}

/// `NewEvaluators`: pods have their own evaluator, which is not on this path.
#[test]
fn registry_skips_unported_evaluators() {
    assert!(evaluator_for(&GroupResource::new("", "pods")).is_none());
    assert!(evaluator_for(&GroupResource::new("", "persistentvolumeclaims")).is_some());
}

/// `checkQuotas` retries on a conflicting status write against the
/// latest quota.
#[tokio::test]
async fn a_conflicting_status_write_is_retried() {
    let storage = MemoryStorage::new();
    let first = stored(
        &storage,
        &quota(&[("configmaps", "5")], &[("configmaps", "1")]),
    )
    .await;
    // Another writer moves the quota on after `first` was read.
    let mut moved = first.clone();
    moved.status.as_mut().unwrap().used = Some(list(&[("configmaps", "2")]));
    storage
        .update(&build_key("resourcequotas", Some("test"), "quota"), &moved)
        .await
        .unwrap();

    let cm = configmap();
    let ev = evaluator_for(&GroupResource::new("", "configmaps")).unwrap();
    super::check_quotas(
        &storage,
        vec![first],
        &attrs(Operation::Create, &cm, None),
        &*ev,
        3,
    )
    .await
    .unwrap();
    assert_eq!(used(&storage).await["configmaps"], "3");
}

fn pvc(rv: &str, storage: &str, class: Option<&str>) -> Value {
    let mut spec = json!({"resources": {"requests": {"storage": storage}}});
    if let Some(c) = class {
        spec["storageClassName"] = json!(c);
    }
    json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": "pvc-to-update", "namespace": "test", "resourceVersion": rv},
        "spec": spec,
    })
}

fn pvc_quota() -> ResourceQuota {
    quota(
        &[
            ("persistentvolumeclaims", "3"),
            ("requests.storage", "100Gi"),
        ],
        &[
            ("persistentvolumeclaims", "1"),
            ("requests.storage", "10Gi"),
        ],
    )
}

fn pvc_evaluator() -> Box<dyn super::evaluator::Evaluator> {
    evaluator_for(&GroupResource::new("", "persistentvolumeclaims")).unwrap()
}

/// `TestAdmitHandlesPVCUpdates`: growing a claim charges the difference,
/// and the claim count stays put.
#[tokio::test]
async fn admit_handles_pvc_updates() {
    let storage = MemoryStorage::new();
    stored(&storage, &pvc_quota()).await;
    let old = pvc("1", "10Gi", None);
    let new = pvc("", "15Gi", None);
    evaluate(
        &storage,
        &*pvc_evaluator(),
        &attrs(Operation::Update, &new, Some(&old)),
    )
    .await
    .unwrap();
    let used = used(&storage).await;
    assert_eq!(used["persistentvolumeclaims"], "1");
    assert_eq!(used["requests.storage"], "15Gi");
}

/// `TestAdmitHandlesNegativePVCUpdates`: shrinking a claim writes nothing.
#[tokio::test]
async fn admit_handles_negative_pvc_updates() {
    let storage = MemoryStorage::new();
    stored(&storage, &pvc_quota()).await;
    let old = pvc("1", "10Gi", None);
    let new = pvc("", "5Gi", None);
    evaluate(
        &storage,
        &*pvc_evaluator(),
        &attrs(Operation::Update, &new, Some(&old)),
    )
    .await
    .unwrap();
    assert_eq!(used(&storage).await["requests.storage"], "10Gi");
}

/// A created claim is charged, and one over the storage limit is refused.
#[tokio::test]
async fn pvc_create_is_charged_and_limited() {
    let storage = MemoryStorage::new();
    stored(&storage, &pvc_quota()).await;
    let ok = pvc("", "20Gi", None);
    evaluate(
        &storage,
        &*pvc_evaluator(),
        &attrs(Operation::Create, &ok, None),
    )
    .await
    .unwrap();
    let used_now = used(&storage).await;
    assert_eq!(used_now["persistentvolumeclaims"], "2");
    assert_eq!(used_now["requests.storage"], "30Gi");

    let big = pvc("", "80Gi", None);
    let err = evaluate(
        &storage,
        &*pvc_evaluator(),
        &attrs(Operation::Create, &big, None),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QuotaError::Forbidden(m) if m.contains("requests.storage")));
}

/// Per-storage-class keys are charged only for a claim of that class.
#[tokio::test]
async fn pvc_storage_class_quota() {
    let storage = MemoryStorage::new();
    stored(
        &storage,
        &quota(
            &[("gold.storageclass.storage.k8s.io/requests.storage", "10Gi")],
            &[("gold.storageclass.storage.k8s.io/requests.storage", "0")],
        ),
    )
    .await;
    let silver = pvc("", "50Gi", Some("silver"));
    evaluate(
        &storage,
        &*pvc_evaluator(),
        &attrs(Operation::Create, &silver, None),
    )
    .await
    .unwrap();
    let gold = pvc("", "50Gi", Some("gold"));
    assert!(evaluate(
        &storage,
        &*pvc_evaluator(),
        &attrs(Operation::Create, &gold, None)
    )
    .await
    .is_err());
}
