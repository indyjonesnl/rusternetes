//! Ports of plugin/pkg/admission/resourcequota/admission_test.go for the
//! evaluators ported here (services and object counts), run against
//! [`MemoryStorage`] in place of the fake clientset.

use std::collections::HashMap;

use rusternetes_common::admission::Operation;
use rusternetes_common::resources::{ResourceQuota, ResourceQuotaStatus};
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use serde_json::{json, Value};

use super::evaluator::{
    evaluator_for, Evaluator, ObjectCountEvaluator, PodEvaluator, ServiceEvaluator,
};
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

/// `NewEvaluators` (registry.go:41-70): pods have their own evaluator,
/// which tracks the pod resources and not the other kinds' names.
#[test]
fn registry_has_a_pod_evaluator() {
    let pods = evaluator_for(&GroupResource::new("", "pods")).unwrap();
    assert_eq!(
        pods.matching_resources(&["requests.cpu".to_string(), "services".to_string()]),
        ["requests.cpu"]
    );
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

/// A VolumeAttributesClass-scoped quota, as in
/// `TestAdmitBelowVolumeAttributesClassQuotaLimit`
/// (plugin/pkg/admission/resourcequota/admission_test.go:989-1117).
fn vac_quota(name: &str, class: &str) -> ResourceQuota {
    let mut q = quota(
        &[
            ("persistentvolumeclaims", "3"),
            ("requests.storage", "100Gi"),
        ],
        &[
            ("persistentvolumeclaims", "1"),
            ("requests.storage", "10Gi"),
        ],
    );
    q.metadata.name = name.to_string();
    q.spec.scope_selector = Some(
        serde_json::from_value(json!({"matchExpressions": [
            {"scopeName": "VolumeAttributesClass", "operator": "In", "values": [class]}
        ]}))
        .unwrap(),
    );
    q
}

fn vac_pvc(
    rv: &str,
    spec_class: Option<&str>,
    current: Option<&str>,
    target: Option<&str>,
) -> Value {
    let mut c = pvc(rv, "1Gi", None);
    if let Some(s) = spec_class {
        c["spec"]["volumeAttributesClassName"] = json!(s);
    }
    let mut status = json!({"phase": "Bound"});
    if let Some(s) = current {
        status["currentVolumeAttributesClassName"] = json!(s);
    }
    if let Some(s) = target {
        status["modifyVolumeStatus"] =
            json!({"targetVolumeAttributesClassName": s, "status": "Pending"});
    }
    c["status"] = status;
    c
}

async fn stored_all(storage: &MemoryStorage, quotas: &[ResourceQuota]) {
    for q in quotas {
        storage
            .create(
                &build_key("resourcequotas", Some("test"), &q.metadata.name),
                q,
            )
            .await
            .unwrap();
    }
}

async fn used_of(storage: &MemoryStorage, name: &str) -> HashMap<String, String> {
    let q: ResourceQuota = storage
        .get(&build_key("resourcequotas", Some("test"), name))
        .await
        .unwrap();
    q.status.unwrap().used.unwrap()
}

/// `TestAdmitBelowVolumeAttributesClassQuotaLimit`: a claim with the gold
/// class is charged to the gold quota only.
#[tokio::test]
async fn admit_below_vac_quota_limit() {
    let storage = MemoryStorage::new();
    stored_all(
        &storage,
        &[
            vac_quota("quota-gold", "gold"),
            vac_quota("quota-silver", "silver"),
        ],
    )
    .await;
    let new = vac_pvc("", Some("gold"), None, None);
    evaluate(
        &storage,
        &*pvc_evaluator(),
        &attrs(Operation::Create, &new, None),
    )
    .await
    .unwrap();
    let gold = used_of(&storage, "quota-gold").await;
    assert_eq!(gold["persistentvolumeclaims"], "2");
    assert_eq!(gold["requests.storage"], "11Gi");
    let silver = used_of(&storage, "quota-silver").await;
    assert_eq!(silver["persistentvolumeclaims"], "1");
    assert_eq!(silver["requests.storage"], "10Gi");
}

/// `TestAdmitBelowVolumeAttributesClassQuotaLimitWhenPVCScopeUpdated`
/// (admission_test.go:1126-1460): which quota is charged as a claim's
/// referenced classes change, on the claim and on its status subresource.
#[tokio::test]
async fn admit_below_vac_quota_limit_when_pvc_scope_updated() {
    // (spec class, current class, target class)
    type C<'a> = (Option<&'a str>, Option<&'a str>, Option<&'a str>);
    type Case<'a> = (&'a str, Option<&'static str>, C<'a>, C<'a>, Option<&'a str>);
    let cases: Vec<Case> = vec![
        (
            "desired class nil to gold",
            None,
            (None, None, None),
            (Some("gold"), None, None),
            Some("quota-gold"),
        ),
        (
            "target class to gold",
            Some("status"),
            (Some("gold"), None, None),
            (Some("gold"), None, Some("gold")),
            None,
        ),
        (
            "current class nil to gold",
            Some("status"),
            (Some("gold"), None, Some("gold")),
            (Some("gold"), Some("gold"), None),
            None,
        ),
        (
            "desired class gold to silver",
            None,
            (Some("gold"), Some("gold"), None),
            (Some("silver"), Some("gold"), None),
            Some("quota-silver"),
        ),
        (
            "target class to silver",
            Some("status"),
            (Some("silver"), Some("gold"), None),
            (Some("silver"), Some("gold"), Some("silver")),
            None,
        ),
        (
            "desired class silver to copper",
            None,
            (Some("silver"), Some("gold"), Some("silver")),
            (Some("copper"), Some("gold"), Some("silver")),
            Some("quota-copper"),
        ),
        (
            "current class to silver on status gains a class",
            Some("status"),
            (Some("gold"), Some("gold"), None),
            (Some("gold"), Some("silver"), Some("gold")),
            Some("quota-silver"),
        ),
    ];
    for (desc, subresource, old, new, charged) in cases {
        let storage = MemoryStorage::new();
        stored_all(
            &storage,
            &[
                vac_quota("quota-gold", "gold"),
                vac_quota("quota-silver", "silver"),
                vac_quota("quota-copper", "copper"),
            ],
        )
        .await;
        let old = vac_pvc("1", old.0, old.1, old.2);
        let new = vac_pvc("", new.0, new.1, new.2);
        let mut a = attrs(Operation::Update, &new, Some(&old));
        a.subresource = subresource;
        evaluate(&storage, &*pvc_evaluator(), &a).await.unwrap();
        for name in ["quota-gold", "quota-silver", "quota-copper"] {
            let u = used_of(&storage, name).await;
            if Some(name) == charged {
                assert_eq!(u["persistentvolumeclaims"], "2", "{desc}: {name}");
                assert_eq!(u["requests.storage"], "11Gi", "{desc}: {name}");
            } else {
                assert_eq!(u["persistentvolumeclaims"], "1", "{desc}: {name}");
                assert_eq!(u["requests.storage"], "10Gi", "{desc}: {name}");
            }
        }
    }
}

/// "allow update pvc status when a quota is exceeded" (admission_test.go:
/// 1390-1450): a status update that adds a class is charged past the limit.
#[tokio::test]
async fn pvc_status_update_is_charged_past_the_limit() {
    let storage = MemoryStorage::new();
    let mut silver = vac_quota("quota-silver", "silver");
    silver.status = Some(ResourceQuotaStatus {
        hard: Some(list(&[
            ("persistentvolumeclaims", "1"),
            ("requests.storage", "10Gi"),
        ])),
        used: Some(list(&[
            ("persistentvolumeclaims", "1"),
            ("requests.storage", "10Gi"),
        ])),
    });
    stored_all(&storage, &[silver]).await;
    let old = vac_pvc("1", Some("gold"), Some("gold"), None);
    let new = vac_pvc("", Some("gold"), Some("silver"), Some("gold"));
    let mut a = attrs(Operation::Update, &new, Some(&old));
    a.subresource = Some("status");
    evaluate(&storage, &*pvc_evaluator(), &a).await.unwrap();
    let u = used_of(&storage, "quota-silver").await;
    assert_eq!(u["persistentvolumeclaims"], "2");
    assert_eq!(u["requests.storage"], "11Gi");
}

/// `TestPersistentVolumeClaimEvaluatorHandles`
/// (persistent_volume_claims_test.go:348-400) plus the `status` subresource
/// arm (persistent_volume_claims.go:96-111).
#[test]
fn pvc_evaluator_handles() {
    let ev = pvc_evaluator();
    let obj = pvc("1", "1Gi", None);
    let h = |op: Operation, sub: Option<&'static str>, o: &Value, old: Option<&Value>| {
        let mut a = attrs(op, o, old);
        a.subresource = sub;
        ev.handles(&a)
    };
    assert!(h(Operation::Create, None, &obj, None));
    assert!(h(Operation::Update, None, &obj, None));
    assert!(!h(Operation::Delete, None, &obj, None));
    assert!(!h(Operation::Connect, None, &obj, None));
    assert!(!h(Operation::Create, Some("subresource"), &obj, None));
    assert!(!h(Operation::Update, Some("subresource"), &obj, None));

    // status: only when RequiresQuotaReplenish.
    let unchanged = obj.clone();
    assert!(!h(
        Operation::Update,
        Some("status"),
        &unchanged,
        Some(&obj)
    ));
    let moved = vac_pvc("1", None, Some("gold"), None);
    assert!(h(Operation::Update, Some("status"), &moved, Some(&obj)));
    let grown = {
        let mut c = obj.clone();
        c["status"] = json!({"allocatedResources": {"storage": "2Gi"}});
        c
    };
    assert!(h(Operation::Update, Some("status"), &grown, Some(&obj)));
    // an undecodable object is not handled
    assert!(!h(
        Operation::Update,
        Some("status"),
        &json!("x"),
        Some(&obj)
    ));
}

/// `TestPersistentVolumeClaimEvaluatorMatchingScopes` through the trait.
#[test]
fn pvc_evaluator_matching_scopes_and_uncovered() {
    use rusternetes_common::resources::ScopedResourceSelectorRequirement as Sel;
    let ev = pvc_evaluator();
    let sel = |op: &str, v: &[&str]| Sel {
        scope_name: "VolumeAttributesClass".into(),
        operator: op.into(),
        values: if v.is_empty() {
            None
        } else {
            Some(v.iter().map(|s| s.to_string()).collect())
        },
    };
    let c = vac_pvc("1", Some("class1"), None, None);
    let got = ev
        .matching_scopes(&c, &[sel("DoesNotExist", &[]), sel("Exists", &[])])
        .unwrap();
    assert_eq!(got, vec![sel("Exists", &[])]);
    // `UncoveredQuotaScopes` (:141-161): limited scopes without a matched
    // quota scope of the same name.
    let uncovered = ev
        .uncovered_quota_scopes(&[sel("Exists", &[])], &[])
        .unwrap();
    assert_eq!(uncovered, vec![sel("Exists", &[])]);
    let covered = ev
        .uncovered_quota_scopes(&[sel("Exists", &[])], &[sel("In", &["a"])])
        .unwrap();
    assert!(covered.is_empty());
    // The evaluators with no scope function have no matching scopes.
    let cm = configmap();
    let cme = evaluator_for(&GroupResource::new("", "configmaps")).unwrap();
    assert!(cme
        .matching_scopes(&cm, &[sel("Exists", &[])])
        .unwrap()
        .is_empty());
}

// ---- the pod evaluator: pkg/quota/v1/evaluator/core/pods_test.go ----

fn pod(rv: &str, spec: Value) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "pod", "namespace": "test", "resourceVersion": rv},
        "spec": spec,
    })
}

fn container(requests: Value) -> Value {
    json!({"name": "c", "image": "i", "resources": {"requests": requests, "limits": requests}})
}

/// `TestPodEvaluatorHandles`.
#[test]
fn pod_evaluator_handles() {
    let plain = pod("1", json!({"containers": []}));
    let deadline = pod("1", json!({"containers": [], "activeDeadlineSeconds": 1}));
    let other_deadline = pod("1", json!({"containers": [], "activeDeadlineSeconds": 2}));
    let h = |op: Operation, sub: Option<&str>, obj: Option<&Value>, old: Option<&Value>| {
        let empty = Value::Null;
        let a = Attributes {
            operation: op,
            namespace: "test",
            subresource: sub,
            // An absent object is one that does not decode as a pod.
            object: obj.unwrap_or(&empty),
            old_object: old,
            dry_run: false,
        };
        PodEvaluator.handles(&a)
    };
    assert!(h(Operation::Create, None, None, None));
    // activeDeadlineSeconds to nil and from nil: the Terminating scope flips.
    assert!(h(Operation::Update, None, Some(&plain), Some(&deadline)));
    assert!(h(Operation::Update, None, Some(&deadline), Some(&plain)));
    assert!(!h(
        Operation::Update,
        None,
        Some(&deadline),
        Some(&other_deadline)
    ));
    // An update with no decodable pods, deletes and connects are not handled.
    assert!(!h(Operation::Update, None, None, None));
    assert!(!h(Operation::Delete, None, None, None));
    assert!(!h(Operation::Connect, None, None, None));
    assert!(!h(Operation::Create, Some("subresource"), None, None));
    assert!(!h(Operation::Update, Some("subresource"), None, None));
    assert!(h(Operation::Update, Some("resize"), None, None));
}

/// `MatchingResources` (pods.go:208-222): `podResources`, hugepages, and
/// `requests.<extended>`.
#[test]
fn pod_evaluator_matching_resources() {
    let names = [
        "pods",
        "count/pods",
        "requests.cpu",
        "limits.memory",
        "hugepages-2Mi",
        "requests.hugepages-2Mi",
        "requests.example.com/dongle",
        "limits.example.com/dongle",
        "requests.storage",
        "services",
    ]
    .map(String::from);
    assert_eq!(
        PodEvaluator.matching_resources(&names),
        [
            "count/pods",
            "hugepages-2Mi",
            "limits.memory",
            "pods",
            "requests.cpu",
            "requests.example.com/dongle",
            "requests.hugepages-2Mi",
        ]
    );
}

/// `TestPodEvaluatorMatchingScopes`, a representative slice.
#[test]
fn pod_evaluator_matching_scopes() {
    use rusternetes_common::resources::ScopedResourceSelectorRequirement as S;
    let sel = |name: &str, op: &str, values: Option<Vec<&str>>| S {
        scope_name: name.to_string(),
        operator: op.to_string(),
        values: values.map(|v| v.into_iter().map(String::from).collect()),
    };
    let all = vec![
        sel("Terminating", "", None),
        sel("NotTerminating", "", None),
        sel("BestEffort", "", None),
        sel("NotBestEffort", "", None),
        sel("PriorityClass", "In", Some(vec!["class1"])),
        sel("CrossNamespacePodAffinity", "", None),
    ];
    let names = |p: &Value| -> Vec<String> {
        PodEvaluator
            .matching_scopes(p, &all)
            .unwrap()
            .into_iter()
            .map(|s| s.scope_name)
            .collect()
    };
    assert_eq!(
        names(&pod("1", json!({"containers": []}))),
        ["NotTerminating", "BestEffort"]
    );
    assert_eq!(
        names(&pod(
            "1",
            json!({"containers": [], "priorityClassName": "class1"})
        )),
        ["NotTerminating", "BestEffort", "PriorityClass"]
    );
    assert_eq!(
        names(&pod(
            "1",
            json!({"containers": [], "activeDeadlineSeconds": 30,
                   "affinity": {"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution":
                       [{"labelSelector": {}, "namespaces": ["ns1"], "topologyKey": "k"}]}}})
        )),
        ["Terminating", "BestEffort", "CrossNamespacePodAffinity"]
    );
}

/// A pod create is charged `pods` and its requests, and recorded.
#[tokio::test]
async fn pod_create_is_charged_and_recorded() {
    let storage = MemoryStorage::new();
    stored(
        &storage,
        &quota(
            &[("pods", "2"), ("requests.cpu", "1")],
            &[("pods", "1"), ("requests.cpu", "250m")],
        ),
    )
    .await;
    let p = pod(
        "",
        json!({"containers": [container(json!({"cpu": "250m"}))]}),
    );
    evaluate(&storage, &PodEvaluator, &attrs(Operation::Create, &p, None))
        .await
        .unwrap();
    let u = used(&storage).await;
    assert_eq!(u["pods"], "2");
    assert_eq!(u["requests.cpu"], "500m");

    // The second pod would exceed `pods`.
    let err = evaluate(&storage, &PodEvaluator, &attrs(Operation::Create, &p, None))
        .await
        .unwrap_err();
    assert_eq!(
        err,
        QuotaError::Forbidden(
            "exceeded quota: quota, requested: pods=1, used: pods=2, limited: pods=2".to_string()
        )
    );
}

/// `Constraints` runs from `CheckRequest` (controller.go:470-472), before
/// `hasUsageStats`: a container omitting a quota'd cpu is refused even when
/// the quota has no `status.used` yet.
#[test]
fn pod_constraints_fail_before_usage_stats() {
    let mut q = quota(&[("requests.cpu", "1")], &[]);
    q.metadata.name = "cpu".to_string();
    let p = pod("", json!({"containers": [{"name": "c", "image": "i"}]}));
    let err = check_request(&[q], &attrs(Operation::Create, &p, None), &PodEvaluator).unwrap_err();
    assert_eq!(
        err,
        QuotaError::Forbidden("failed quota: cpu: must specify requests.cpu for: c".to_string())
    );
}

/// `generic.Matches`: a scoped quota only covers the pods its scopes match.
#[tokio::test]
async fn scoped_pod_quota_only_charges_matching_pods() {
    let storage = MemoryStorage::new();
    let mut q = quota(&[("pods", "1")], &[("pods", "1")]);
    q.spec.scopes = Some(vec!["Terminating".to_string()]);
    stored(&storage, &q).await;
    // A NotTerminating pod is not covered by the Terminating-scoped quota.
    let plain = pod("", json!({"containers": []}));
    evaluate(
        &storage,
        &PodEvaluator,
        &attrs(Operation::Create, &plain, None),
    )
    .await
    .unwrap();
    // A Terminating one is, and the quota is full.
    let terminating = pod("", json!({"containers": [], "activeDeadlineSeconds": 10}));
    let err = evaluate(
        &storage,
        &PodEvaluator,
        &attrs(Operation::Create, &terminating, None),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QuotaError::Forbidden(m) if m.starts_with("exceeded quota: quota")));
}

/// An update moving a pod into a quota's scope is charged the whole
/// footprint: the quota did not match the old object, so the delta is not
/// substituted (controller.go:572-586).
#[test]
fn update_into_scope_charges_full_usage() {
    let mut q = quota(&[("pods", "5")], &[("pods", "1")]);
    q.spec.scopes = Some(vec!["Terminating".to_string()]);
    let old = pod("3", json!({"containers": []}));
    let new = pod("3", json!({"containers": [], "activeDeadlineSeconds": 10}));
    let out = check_request(
        &[q],
        &attrs(Operation::Update, &new, Some(&old)),
        &PodEvaluator,
    )
    .unwrap();
    assert_eq!(
        out[0].status.as_ref().unwrap().used.as_ref().unwrap()["pods"],
        "2"
    );
}

/// A plain pod update (no scope flip) is not handled: nothing is charged.
#[tokio::test]
async fn plain_pod_update_is_ignored() {
    let storage = MemoryStorage::new();
    stored(&storage, &quota(&[("pods", "1")], &[("pods", "1")])).await;
    let old = pod("3", json!({"containers": []}));
    let new = pod("3", json!({"containers": [], "priorityClassName": "x"}));
    evaluate(
        &storage,
        &PodEvaluator,
        &attrs(Operation::Update, &new, Some(&old)),
    )
    .await
    .unwrap();
    assert_eq!(used(&storage).await["pods"], "1");
}
