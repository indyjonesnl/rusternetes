//! Port of upstream `pkg/controller/disruption/disruption_test.go`
//! (release-1.35) for the PodDisruptionBudget controller, plus the
//! pod-event / recheck behaviour that file's `Run` path depends on.
//!
//! Upstream drives `dc.sync(ctx, key)` against informer stores and captures the
//! status written by a fake updater (`pdbStates`). Here the stores are a
//! `MemoryStorage`, `reconcile_all()` is the `sync` call, and "the status the
//! updater saw" is the PDB read back from storage.
//!
//! Every case names the upstream test it ports. The shared fixtures mirror the
//! upstream helpers of the same name (`newPod`, `newMinAvailablePodDisruptionBudget`,
//! `newReplicaSet`, ...).

use rusternetes_common::resources::{Event, PodDisruptionBudget};
use rusternetes_controller_manager::controllers::pod_disruption_budget::PodDisruptionBudgetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const NS: &str = "default";
const PDB: &str = "foobar";

fn storage() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

fn fresh_uid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// `newMinAvailablePodDisruptionBudget` / `newMaxUnavailablePodDisruptionBudget`.
fn pdb_json(min_available: Option<Value>, max_unavailable: Option<Value>) -> Value {
    let mut spec = json!({ "selector": { "matchLabels": { "foo": "bar" } } });
    if let Some(v) = min_available {
        spec["minAvailable"] = v;
    }
    if let Some(v) = max_unavailable {
        spec["maxUnavailable"] = v;
    }
    json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": { "name": PDB, "namespace": NS, "uid": fresh_uid(), "generation": 1 },
        "spec": spec,
    })
}

fn min_available(v: Value) -> Value {
    pdb_json(Some(v), None)
}

fn max_unavailable(v: Value) -> Value {
    pdb_json(None, Some(v))
}

async fn put(storage: &Arc<MemoryStorage>, resource: &str, name: &str, obj: &Value) {
    let key = build_key(resource, Some(NS), name);
    storage.create(&key, obj).await.expect("create");
}

async fn put_pdb(storage: &Arc<MemoryStorage>, pdb: &Value) {
    put(storage, "poddisruptionbudgets", PDB, pdb).await;
}

/// `newPod`: labels foo=bar, `Ready=True`.
fn pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name, "namespace": NS, "uid": fresh_uid(),
            "labels": { "foo": "bar" },
            "creationTimestamp": "2024-01-01T00:00:00Z",
        },
        "spec": { "containers": [{ "name": "c", "image": "busybox" }] },
        "status": { "conditions": [{ "type": "Ready", "status": "True" }] },
    })
}

fn owned_by(mut pod: Value, owner: &Value) -> Value {
    let refs = pod["metadata"]["ownerReferences"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut refs = refs;
    refs.push(json!({
        "apiVersion": owner["apiVersion"],
        "kind": owner["kind"],
        "name": owner["metadata"]["name"],
        "uid": owner["metadata"]["uid"],
        "controller": true,
    }));
    pod["metadata"]["ownerReferences"] = Value::Array(refs);
    pod
}

fn unready(mut pod: Value) -> Value {
    pod["status"]["conditions"] = json!([]);
    pod
}

fn workload(api_version: &str, kind: &str, name: &str, replicas: i32) -> Value {
    json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": { "name": name, "namespace": NS, "uid": fresh_uid(),
                      "labels": { "foo": "bar" } },
        "spec": { "replicas": replicas, "selector": { "matchLabels": { "foo": "bar" } } },
    })
}

fn replica_set(replicas: i32) -> Value {
    workload("apps/v1", "ReplicaSet", PDB, replicas)
}

async fn sync(storage: &Arc<MemoryStorage>) {
    PodDisruptionBudgetController::new(storage.clone())
        .reconcile_all()
        .await
        .expect("reconcile_all");
}

async fn pdb_now(storage: &Arc<MemoryStorage>) -> PodDisruptionBudget {
    let key = build_key("poddisruptionbudgets", Some(NS), PDB);
    storage.get(&key).await.expect("get pdb")
}

/// `VerifyPdbStatus`: counters, `observedGeneration == generation`, the
/// disrupted-pod names, and the `DisruptionAllowed` condition's status.
async fn verify(
    storage: &Arc<MemoryStorage>,
    disruptions_allowed: i32,
    current_healthy: i32,
    desired_healthy: i32,
    expected_pods: i32,
) {
    let pdb = pdb_now(storage).await;
    let status = pdb.status.clone().expect("status written");
    assert_eq!(
        (
            status.disruptions_allowed,
            status.current_healthy,
            status.desired_healthy,
            status.expected_pods
        ),
        (
            disruptions_allowed,
            current_healthy,
            desired_healthy,
            expected_pods
        ),
        "(disruptionsAllowed, currentHealthy, desiredHealthy, expectedPods) mismatch; conditions: {:?}",
        status.conditions
    );
    assert_eq!(
        status.observed_generation, pdb.metadata.generation,
        "observedGeneration must equal metadata.generation"
    );
    let cond = status
        .conditions
        .as_ref()
        .and_then(|c| c.iter().find(|c| c.condition_type == "DisruptionAllowed"))
        .expect("DisruptionAllowed condition");
    let want = if disruptions_allowed > 0 {
        "True"
    } else {
        "False"
    };
    assert_eq!(cond.status, want, "DisruptionAllowed condition status");
}

async fn verify_disruption_allowed(storage: &Arc<MemoryStorage>, want: i32) {
    let got = pdb_now(storage)
        .await
        .status
        .expect("status")
        .disruptions_allowed;
    assert_eq!(got, want, "disruptionsAllowed");
}

async fn events(storage: &Arc<MemoryStorage>) -> Vec<Event> {
    storage
        .list::<Event>("/registry/events/")
        .await
        .unwrap_or_default()
}

/// `verifyEventEmitted`.
async fn verify_event_emitted(storage: &Arc<MemoryStorage>, reason: &str) {
    let evs = events(storage).await;
    assert!(
        evs.iter().any(|e| e.reason == reason),
        "expected event {reason}, got {:?}",
        evs.iter().map(|e| e.reason.clone()).collect::<Vec<_>>()
    );
}

async fn sync_failed_message(storage: &Arc<MemoryStorage>) -> Option<String> {
    pdb_now(storage)
        .await
        .status?
        .conditions?
        .into_iter()
        .find(|c| {
            c.condition_type == "DisruptionAllowed" && c.reason.as_deref() == Some("SyncFailed")
        })
        .and_then(|c| c.message)
}

// ---------------------------------------------------------------------------
// TestNoSelector
// ---------------------------------------------------------------------------
#[tokio::test]
async fn no_selector_matches_all_pods() {
    let s = storage();
    let mut pdb = min_available(json!(3));
    pdb["spec"]["selector"] = json!({});
    put_pdb(&s, &pdb).await;
    sync(&s).await;
    verify(&s, 0, 0, 3, 0).await;

    put(&s, "pods", "yo-yo-yo", &pod("yo-yo-yo")).await;
    sync(&s).await;
    verify(&s, 0, 1, 3, 1).await;
}

// ---------------------------------------------------------------------------
// TestUnavailable
// ---------------------------------------------------------------------------
#[tokio::test]
async fn unavailable_pod_lowers_current_healthy() {
    let s = storage();
    put_pdb(&s, &min_available(json!(3))).await;
    sync(&s).await;
    for i in 0..4 {
        verify(&s, 0, i, 3, i).await;
        put(&s, "pods", &format!("yo-{i}"), &pod(&format!("yo-{i}"))).await;
        sync(&s).await;
    }
    verify(&s, 1, 4, 3, 4).await;

    // Now set one pod as unavailable.
    let key = build_key("pods", Some(NS), "yo-0");
    s.update(&key, &unready(pod("yo-0"))).await.unwrap();
    sync(&s).await;
    verify(&s, 0, 3, 3, 4).await;
}

// ---------------------------------------------------------------------------
// TestIntegerMaxUnavailable
// ---------------------------------------------------------------------------
#[tokio::test]
async fn integer_max_unavailable_disallows_disruption_for_naked_pods() {
    let s = storage();
    put_pdb(&s, &max_unavailable(json!(1))).await;
    sync(&s).await;
    verify_disruption_allowed(&s, 0).await;

    put(&s, "pods", "naked", &pod("naked")).await;
    sync(&s).await;
    verify_disruption_allowed(&s, 0).await;
    verify_event_emitted(&s, "UnmanagedPods").await;
}

// ---------------------------------------------------------------------------
// TestIntegerMaxUnavailableWithScaling
// ---------------------------------------------------------------------------
#[tokio::test]
async fn integer_max_unavailable_recomputes_when_scale_changes() {
    let s = storage();
    put_pdb(&s, &max_unavailable(json!(2))).await;
    let mut rs = replica_set(7);
    put(&s, "replicasets", PDB, &rs).await;
    put(&s, "pods", "pod", &owned_by(pod("pod"), &rs)).await;
    sync(&s).await;
    verify(&s, 0, 1, 5, 7).await;

    rs["spec"]["replicas"] = json!(5);
    s.update(&build_key("replicasets", Some(NS), PDB), &rs)
        .await
        .unwrap();
    sync(&s).await;
    verify(&s, 0, 1, 3, 5).await;
}

// ---------------------------------------------------------------------------
// TestPercentageMaxUnavailableWithScaling
// ---------------------------------------------------------------------------
#[tokio::test]
async fn percentage_max_unavailable_recomputes_when_scale_changes() {
    let s = storage();
    put_pdb(&s, &max_unavailable(json!("30%"))).await;
    let mut rs = replica_set(7);
    put(&s, "replicasets", PDB, &rs).await;
    put(&s, "pods", "pod", &owned_by(pod("pod"), &rs)).await;
    sync(&s).await;
    verify(&s, 0, 1, 4, 7).await;

    rs["spec"]["replicas"] = json!(3);
    s.update(&build_key("replicasets", Some(NS), PDB), &rs)
        .await
        .unwrap();
    sync(&s).await;
    verify(&s, 0, 1, 2, 3).await;
}

// ---------------------------------------------------------------------------
// TestNakedPod
// ---------------------------------------------------------------------------
#[tokio::test]
async fn naked_pod_with_percentage_disallows_disruption() {
    let s = storage();
    put_pdb(&s, &min_available(json!("28%"))).await;
    sync(&s).await;
    verify_disruption_allowed(&s, 0).await;

    put(&s, "pods", "naked", &pod("naked")).await;
    sync(&s).await;
    verify_disruption_allowed(&s, 0).await;
    verify_event_emitted(&s, "UnmanagedPods").await;
}

// ---------------------------------------------------------------------------
// TestUnsupportedControllerPod
// ---------------------------------------------------------------------------
#[tokio::test]
async fn unsupported_controller_pod_fails_safe() {
    let s = storage();
    put_pdb(&s, &min_available(json!("28%"))).await;
    sync(&s).await;
    verify_disruption_allowed(&s, 0).await;

    let mut p = pod("naked");
    p["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "apps.test.io/v1",
        "kind": "TestWorkload",
        "name": "fake-controller",
        "uid": "b7329742-8daa-493a-8881-6ca07139172b",
        "controller": true,
    }]);
    put(&s, "pods", "naked", &p).await;
    sync(&s).await;

    verify_disruption_allowed(&s, 0).await;
    verify_event_emitted(&s, "CalculateExpectedPodCountFailed").await;
    // failSafe: the status says why.
    assert!(
        sync_failed_message(&s).await.is_some(),
        "failSafe must set DisruptionAllowed=False reason=SyncFailed"
    );
}

// ---------------------------------------------------------------------------
// TestStatusForUnmanagedPod / TestTotalUnmanagedPods
// ---------------------------------------------------------------------------
#[tokio::test]
async fn unmanaged_pod_is_not_a_sync_error() {
    let s = storage();
    put_pdb(&s, &min_available(json!("28%"))).await;
    sync(&s).await;
    verify_disruption_allowed(&s, 0).await;

    put(&s, "pods", "unmanaged", &pod("unmanaged")).await;
    sync(&s).await;
    // VerifyNoStatusError: no SyncFailed condition for "found no controller ref".
    assert!(
        sync_failed_message(&s).await.is_none(),
        "unmanaged pods must not trip failSafe"
    );
    verify_event_emitted(&s, "UnmanagedPods").await;
    // getExpectedScale: the unmanaged pod contributes nothing.
    verify(&s, 0, 1, 0, 0).await;
}

// ---------------------------------------------------------------------------
// TestReplicaSet
// ---------------------------------------------------------------------------
#[tokio::test]
async fn replica_set_without_deployment_counts_its_scale() {
    let s = storage();
    put_pdb(&s, &min_available(json!("20%"))).await;
    let rs = replica_set(10);
    put(&s, "replicasets", PDB, &rs).await;
    put(&s, "pods", "pod", &owned_by(pod("pod"), &rs)).await;
    sync(&s).await;
    verify(&s, 0, 1, 2, 10).await;
}

// ---------------------------------------------------------------------------
// TestScaleResource / TestScaleFinderNoResource (via a CRD scale subresource)
// ---------------------------------------------------------------------------
fn scale_crd(with_scale: bool) -> Value {
    let mut version = json!({
        "name": "v1", "served": true, "storage": true,
        "schema": { "openAPIV3Schema": { "type": "object",
                                        "x-kubernetes-preserve-unknown-fields": true } },
    });
    if with_scale {
        version["subresources"] = json!({
            "scale": { "specReplicasPath": ".spec.replicas",
                       "statusReplicasPath": ".status.replicas" }
        });
    }
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "customresources.custom.k8s.io", "uid": fresh_uid() },
        "spec": {
            "group": "custom.k8s.io",
            "scope": "Namespaced",
            "names": { "plural": "customresources", "singular": "customresource",
                       "kind": "customresource" },
            "versions": [version],
        },
    })
}

fn custom_resource(replicas: Option<i32>) -> Value {
    let mut cr = json!({
        "apiVersion": "custom.k8s.io/v1",
        "kind": "customresource",
        "metadata": { "name": "cr", "namespace": NS, "uid": fresh_uid() },
        "spec": {},
    });
    if let Some(r) = replicas {
        cr["spec"]["replicas"] = json!(r);
    }
    cr
}

#[tokio::test]
async fn scale_resource_supplies_expected_pods() {
    let (replicas, pods, max_unavail) = (10, 4, 5);
    let s = storage();
    s.create(
        &build_key(
            "customresourcedefinitions",
            None,
            "customresources.custom.k8s.io",
        ),
        &scale_crd(true),
    )
    .await
    .unwrap();
    let cr = custom_resource(Some(replicas));
    put(&s, "custom_k8s_io_customresources", "cr", &cr).await;
    put_pdb(&s, &max_unavailable(json!(max_unavail))).await;
    for i in 0..pods {
        put(
            &s,
            "pods",
            &format!("pod-{i}"),
            &owned_by(pod(&format!("pod-{i}")), &cr),
        )
        .await;
    }
    sync(&s).await;
    let allowed = if replicas - pods < max_unavail {
        max_unavail - (replicas - pods)
    } else {
        0
    };
    verify(&s, allowed, pods, replicas - max_unavail, replicas).await;
}

#[tokio::test]
async fn scale_finder_resource_implements_scale_but_object_missing() {
    // "resource implements scale": getScaleController returns (nil, nil) for a
    // NotFound object, so the sync fails with "found no controllers" — NOT with
    // "does not implement the scale subresource".
    let s = storage();
    s.create(
        &build_key(
            "customresourcedefinitions",
            None,
            "customresources.custom.k8s.io",
        ),
        &scale_crd(true),
    )
    .await
    .unwrap();
    put_pdb(&s, &max_unavailable(json!(1))).await;
    put(
        &s,
        "pods",
        "p",
        &owned_by(pod("p"), &custom_resource(Some(3))),
    )
    .await;
    sync(&s).await;
    let msg = sync_failed_message(&s).await.expect("sync fails safe");
    assert!(msg.contains("found no controllers"), "{msg}");
    assert!(!msg.contains("does not implement"), "{msg}");
}

#[tokio::test]
async fn scale_finder_resource_without_scale_subresource_is_an_error() {
    // "resource does not implement scale" / "unsupported data format".
    let s = storage();
    s.create(
        &build_key(
            "customresourcedefinitions",
            None,
            "customresources.custom.k8s.io",
        ),
        &scale_crd(false),
    )
    .await
    .unwrap();
    put_pdb(&s, &max_unavailable(json!(1))).await;
    let cr = custom_resource(Some(3));
    put(&s, "custom_k8s_io_customresources", "cr", &cr).await;
    put(&s, "pods", "p", &owned_by(pod("p"), &cr)).await;
    sync(&s).await;
    let msg = sync_failed_message(&s).await.expect("sync fails safe");
    assert!(
        msg.contains("does not implement the scale subresource"),
        "{msg}"
    );
}

#[tokio::test]
async fn scale_resource_with_unset_spec_replicas_reads_as_zero() {
    // apiextensions scaleFromCustomResource (customresource/etcd.go:255-257):
    // an absent specReplicasPath value is replicas 0, not "unresolvable".
    let s = storage();
    s.create(
        &build_key(
            "customresourcedefinitions",
            None,
            "customresources.custom.k8s.io",
        ),
        &scale_crd(true),
    )
    .await
    .unwrap();
    let cr = custom_resource(None);
    put(&s, "custom_k8s_io_customresources", "cr", &cr).await;
    put_pdb(&s, &max_unavailable(json!(1))).await;
    put(&s, "pods", "p", &owned_by(pod("p"), &cr)).await;
    sync(&s).await;
    verify(&s, 0, 1, 0, 0).await;
}

// ---------------------------------------------------------------------------
// TestMultipleControllers
// ---------------------------------------------------------------------------
#[tokio::test]
async fn multiple_controllers() {
    let s = storage();
    put_pdb(&s, &min_available(json!("1%"))).await;
    let mut pods = vec![];
    for i in 0..2 {
        pods.push(pod(&format!("pod-{i}")));
        put(&s, "pods", &format!("pod-{i}"), &pods[i]).await;
    }
    sync(&s).await;
    // No controllers yet => no disruption allowed.
    verify_disruption_allowed(&s, 0).await;

    let rc = workload("v1", "ReplicationController", "rc-1", 1);
    put(&s, "replicationcontrollers", "rc-1", &rc).await;
    for (i, p) in pods.iter().enumerate() {
        let key = build_key("pods", Some(NS), &format!("pod-{i}"));
        s.update(&key, &owned_by(p.clone(), &rc)).await.unwrap();
    }
    sync(&s).await;
    // One RC and 200%>1% healthy => disruption allowed.
    verify_disruption_allowed(&s, 1).await;
}

// ---------------------------------------------------------------------------
// TestReplicationController
// ---------------------------------------------------------------------------
#[tokio::test]
async fn replication_controller_scale_and_rogue_pod() {
    let labels = json!({ "foo": "bar", "baz": "quux" });
    let s = storage();
    // 34% should round up to 2.
    put_pdb(&s, &min_available(json!("34%"))).await;
    let mut rc = workload("v1", "ReplicationController", PDB, 3);
    rc["spec"]["selector"] = labels.clone();
    put(&s, "replicationcontrollers", PDB, &rc).await;
    sync(&s).await;
    // With no pods the PDB doesn't know about the RC (upstream: "a known bug").
    verify(&s, 0, 0, 0, 0).await;

    for i in 0..3 {
        let mut p = owned_by(pod(&format!("foobar-{i}")), &rc);
        p["metadata"]["labels"] = labels.clone();
        put(&s, "pods", &format!("foobar-{i}"), &p).await;
        sync(&s).await;
        if i < 2 {
            verify(&s, 0, i + 1, 2, 3).await;
        } else {
            verify(&s, 1, 3, 2, 3).await;
        }
    }

    // A rogue pod matching only foo=bar: unmanaged, contributes no scale.
    put(&s, "pods", "rogue", &pod("rogue")).await;
    sync(&s).await;
    verify_disruption_allowed(&s, 2).await;
}

// ---------------------------------------------------------------------------
// TestStatefulSetController
// ---------------------------------------------------------------------------
#[tokio::test]
async fn stateful_set_controller() {
    let labels = json!({ "foo": "bar", "baz": "quux" });
    let s = storage();
    put_pdb(&s, &min_available(json!("34%"))).await;
    let ss = workload("apps/v1", "StatefulSet", PDB, 3);
    put(&s, "statefulsets", PDB, &ss).await;
    sync(&s).await;
    verify(&s, 0, 0, 0, 0).await;

    for i in 0..3 {
        let mut p = owned_by(pod(&format!("foobar-{i}")), &ss);
        p["metadata"]["labels"] = labels.clone();
        put(&s, "pods", &format!("foobar-{i}"), &p).await;
        sync(&s).await;
        if i < 2 {
            verify(&s, 0, i + 1, 2, 3).await;
        } else {
            verify(&s, 1, 3, 2, 3).await;
        }
    }
}

// ---------------------------------------------------------------------------
// TestTwoControllers
// ---------------------------------------------------------------------------
#[tokio::test]
async fn two_controllers() {
    let rc_labels = json!({ "foo": "bar", "baz": "quux" });
    let d_labels = json!({ "foo": "bar", "baz": "quuux" });
    let s = storage();

    const COLLECTION: i32 = 11;
    const MINIMUM_ONE: i32 = 4;
    const MINIMUM_TWO: i32 = 7;

    put_pdb(&s, &min_available(json!("28%"))).await;
    let mut rc = workload("v1", "ReplicationController", PDB, COLLECTION);
    rc["spec"]["selector"] = rc_labels.clone();
    put(&s, "replicationcontrollers", PDB, &rc).await;
    sync(&s).await;
    verify(&s, 0, 0, 0, 0).await;

    let mut names: Vec<String> = vec![];
    let mut unavailable = COLLECTION - MINIMUM_ONE - 1;
    for i in 1..=COLLECTION {
        let name = format!("quux-{i}");
        let mut p = owned_by(pod(&name), &rc);
        p["metadata"]["labels"] = rc_labels.clone();
        if i <= unavailable {
            p = unready(p);
        }
        put(&s, "pods", &name, &p).await;
        names.push(name);
        sync(&s).await;
        if i <= unavailable {
            verify(&s, 0, 0, MINIMUM_ONE, COLLECTION).await;
        } else if i - unavailable <= MINIMUM_ONE {
            verify(&s, 0, i - unavailable, MINIMUM_ONE, COLLECTION).await;
        } else {
            verify(&s, 1, i - unavailable, MINIMUM_ONE, COLLECTION).await;
        }
    }

    // A Deployment with no ReplicaSet, and an unrelated RS: no change.
    let d = workload("apps/v1", "Deployment", "dep", COLLECTION);
    put(&s, "deployments", "dep", &d).await;
    sync(&s).await;
    verify(&s, 1, MINIMUM_ONE + 1, MINIMUM_ONE, COLLECTION).await;

    let mut rs = replica_set(COLLECTION);
    rs["metadata"]["labels"] = d_labels.clone();
    put(&s, "replicasets", PDB, &rs).await;
    sync(&s).await;
    verify(&s, 1, MINIMUM_ONE + 1, MINIMUM_ONE, COLLECTION).await;

    unavailable = 2 * COLLECTION - (MINIMUM_TWO + 2) - unavailable;
    for i in 1..=COLLECTION {
        let name = format!("quuux-{i}");
        let mut p = owned_by(pod(&name), &rs);
        p["metadata"]["labels"] = d_labels.clone();
        if i <= unavailable {
            p = unready(p);
        }
        put(&s, "pods", &name, &p).await;
        names.push(name);
        sync(&s).await;
        if i <= unavailable {
            verify(&s, 0, MINIMUM_ONE + 1, MINIMUM_TWO, 2 * COLLECTION).await;
        } else if i - unavailable <= MINIMUM_TWO - (MINIMUM_ONE + 1) {
            verify(
                &s,
                0,
                (MINIMUM_ONE + 1) + (i - unavailable),
                MINIMUM_TWO,
                2 * COLLECTION,
            )
            .await;
        } else {
            verify(
                &s,
                i - unavailable - (MINIMUM_TWO - (MINIMUM_ONE + 1)),
                (MINIMUM_ONE + 1) + (i - unavailable),
                MINIMUM_TWO,
                2 * COLLECTION,
            )
            .await;
        }
    }

    // Bring down one pod: a disruption is still permitted; two: not; heal one:
    // permitted again.
    verify(&s, 2, 2 + MINIMUM_TWO, MINIMUM_TWO, 2 * COLLECTION).await;
    let flip = |name: String, ready: bool| {
        let s = s.clone();
        async move {
            let key = build_key("pods", Some(NS), &name);
            let mut p: Value = s.get(&key).await.unwrap();
            p["status"]["conditions"] = if ready {
                json!([{ "type": "Ready", "status": "True" }])
            } else {
                json!([])
            };
            s.update(&key, &p).await.unwrap();
        }
    };
    let last = names[(COLLECTION - 1) as usize].clone();
    let second_last = names[(COLLECTION - 2) as usize].clone();
    flip(last.clone(), false).await;
    sync(&s).await;
    verify(&s, 1, 1 + MINIMUM_TWO, MINIMUM_TWO, 2 * COLLECTION).await;
    flip(second_last, false).await;
    sync(&s).await;
    verify(&s, 0, MINIMUM_TWO, MINIMUM_TWO, 2 * COLLECTION).await;
    flip(last, true).await;
    sync(&s).await;
    verify(&s, 1, 1 + MINIMUM_TWO, MINIMUM_TWO, 2 * COLLECTION).await;
}

// ---------------------------------------------------------------------------
// TestUpdateDisruptedPods
// ---------------------------------------------------------------------------
#[tokio::test]
async fn disrupted_pods_are_pruned() {
    let s = storage();
    let now = chrono::Utc::now();
    let fmt = |t: chrono::DateTime<chrono::Utc>| t.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let p3_time = now - chrono::Duration::seconds(60);
    let mut pdb = min_available(json!(1));
    pdb["status"] = json!({
        "currentHealthy": 0, "desiredHealthy": 0, "disruptionsAllowed": 0, "expectedPods": 0,
        "disruptedPods": {
            "p1": fmt(now),                                    // removed: pod deletion started
            "p2": fmt(now - chrono::Duration::minutes(3)),     // removed: expired
            "p3": fmt(p3_time),                                // kept: pod untouched
            "notthere": fmt(now),                              // removed: pod deleted
        },
    });
    put_pdb(&s, &pdb).await;

    let mut p1 = pod("p1");
    p1["metadata"]["deletionTimestamp"] = json!(fmt(now));
    put(&s, "pods", "p1", &p1).await;
    put(&s, "pods", "p2", &pod("p2")).await;
    put(&s, "pods", "p3", &pod("p3")).await;

    sync(&s).await;

    // p1 is terminating (not healthy); p3 is in disruptedPods within
    // DeletionTimeout (not healthy); only p2 counts.
    verify(&s, 0, 1, 1, 3).await;
    let status = pdb_now(&s).await.status.unwrap();
    let mut names: Vec<String> = status
        .disrupted_pods
        .unwrap_or_default()
        .keys()
        .cloned()
        .collect();
    names.sort();
    assert_eq!(names, vec!["p3".to_string()]);
}

// ---------------------------------------------------------------------------
// TestInvalidSelectors
// ---------------------------------------------------------------------------
#[tokio::test]
async fn invalid_selectors_fail_safe() {
    let cases = [
        (
            "illegal value key",
            json!({ "matchLabels": { "k8s.io/too/many/slashes": "value" } }),
        ),
        (
            "illegal operator",
            json!({ "matchExpressions": [
                { "key": "foo", "operator": "illegal", "values": ["bar"] }
            ] }),
        ),
    ];
    for (name, selector) in cases {
        let s = storage();
        let mut pdb = min_available(json!(3));
        pdb["spec"]["selector"] = selector;
        put_pdb(&s, &pdb).await;
        put(&s, "pods", "p", &pod("p")).await;
        sync(&s).await;
        // failSafe only zeroes disruptionsAllowed and sets SyncFailed; the other
        // counters keep their (zero) values. observedGeneration is NOT advanced
        // (upstream's PDB has generation 0 so `VerifyPdbStatus` cannot see it).
        let status = pdb_now(&s).await.status.expect("failSafe writes a status");
        assert_eq!(
            (
                status.disruptions_allowed,
                status.current_healthy,
                status.desired_healthy,
                status.expected_pods
            ),
            (0, 0, 0, 0),
            "{name}"
        );
        assert!(
            sync_failed_message(&s).await.is_some(),
            "{name}: an unusable selector must fail safe"
        );
    }
}

// ---------------------------------------------------------------------------
// TestKeepExistingPDBConditionDuringSync
// ---------------------------------------------------------------------------
#[tokio::test]
async fn existing_pdb_conditions_survive_a_sync() {
    let s = storage();
    let mut pdb = min_available(json!(3));
    pdb["spec"]["selector"] = json!({});
    pdb["status"] = json!({
        "currentHealthy": 0, "desiredHealthy": 0, "disruptionsAllowed": 0, "expectedPods": 0,
        "conditions": [{
            "type": "ExistingTestCondition", "status": "True",
            "message": "This is a test condition", "reason": "Test",
            "lastTransitionTime": "2024-01-01T00:00:00Z",
        }],
    });
    put_pdb(&s, &pdb).await;
    sync(&s).await;
    verify(&s, 0, 0, 3, 0).await;
    let conds = pdb_now(&s).await.status.unwrap().conditions.unwrap();
    assert_eq!(conds.len(), 2, "{conds:?}");
    assert!(conds
        .iter()
        .any(|c| c.condition_type == "ExistingTestCondition"));
}

// ---------------------------------------------------------------------------
// getExpectedPodCount: precedence and the int/percent split
// ---------------------------------------------------------------------------
#[tokio::test]
async fn max_unavailable_takes_precedence_over_min_available() {
    // disruption.go getExpectedPodCount checks MaxUnavailable first.
    let s = storage();
    put_pdb(&s, &pdb_json(Some(json!(5)), Some(json!("50%")))).await;
    let rs = replica_set(10);
    put(&s, "replicasets", PDB, &rs).await;
    put(&s, "pods", "p", &owned_by(pod("p"), &rs)).await;
    sync(&s).await;
    // desired = 10 - ceil(50% of 10) = 5, expected = RS scale.
    verify(&s, 0, 1, 5, 10).await;
}

#[tokio::test]
async fn integer_min_available_counts_pods_not_scale() {
    // Int minAvailable: expectedCount = len(pods), the controller scale is not read.
    let s = storage();
    put_pdb(&s, &min_available(json!(2))).await;
    let rs = replica_set(10);
    put(&s, "replicasets", PDB, &rs).await;
    for i in 0..3 {
        put(
            &s,
            "pods",
            &format!("p{i}"),
            &owned_by(pod(&format!("p{i}")), &rs),
        )
        .await;
    }
    sync(&s).await;
    verify(&s, 1, 3, 2, 3).await;
}

#[tokio::test]
async fn max_unavailable_larger_than_expected_clamps_desired_healthy_to_zero() {
    let s = storage();
    put_pdb(&s, &max_unavailable(json!(5))).await;
    let rs = replica_set(2);
    put(&s, "replicasets", PDB, &rs).await;
    put(&s, "pods", "p", &owned_by(pod("p"), &rs)).await;
    sync(&s).await;
    // desiredHealthy = 2 - 5 -> 0 (disruption.go: `if desiredHealthy < 0`).
    verify(&s, 1, 1, 0, 2).await;
}

#[tokio::test]
async fn percentage_without_percent_sign_fails_safe() {
    // intstr.getIntOrPercentValueSafely: "a string is not a percentage".
    let s = storage();
    put_pdb(&s, &min_available(json!("5"))).await;
    put(&s, "pods", "p", &pod("p")).await;
    sync(&s).await;
    verify_disruption_allowed(&s, 0).await;
    assert!(sync_failed_message(&s).await.is_some());
}

// ---------------------------------------------------------------------------
// countHealthyPods
// ---------------------------------------------------------------------------
#[tokio::test]
async fn health_is_the_ready_condition_not_the_running_phase() {
    let s = storage();
    put_pdb(&s, &min_available(json!(1))).await;
    let mut running_not_ready = pod("a");
    running_not_ready["status"] = json!({ "phase": "Running", "conditions": [] });
    let mut pending_ready = pod("b");
    pending_ready["status"] = json!({
        "phase": "Pending",
        "conditions": [{ "type": "Ready", "status": "True" }],
    });
    put(&s, "pods", "a", &running_not_ready).await;
    put(&s, "pods", "b", &pending_ready).await;
    sync(&s).await;
    // apipod.IsPodReady: only pod b counts.
    verify(&s, 0, 1, 1, 2).await;
}

#[tokio::test]
async fn terminating_pod_is_not_healthy() {
    let s = storage();
    put_pdb(&s, &min_available(json!(1))).await;
    put(&s, "pods", "a", &pod("a")).await;
    let mut b = pod("b");
    b["metadata"]["deletionTimestamp"] = json!("2024-01-01T00:00:00Z");
    put(&s, "pods", "b", &b).await;
    sync(&s).await;
    // countHealthyPods skips a pod with a DeletionTimestamp, so the budget does
    // not regrow while an evicted pod is still terminating.
    verify(&s, 0, 1, 1, 2).await;
}

#[tokio::test]
async fn pod_in_disrupted_pods_is_not_healthy_within_deletion_timeout() {
    let s = storage();
    let mut pdb = min_available(json!(1));
    let t = (chrono::Utc::now() - chrono::Duration::seconds(30))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    pdb["status"] = json!({
        "currentHealthy": 2, "desiredHealthy": 1, "disruptionsAllowed": 1, "expectedPods": 2,
        "disruptedPods": { "a": t },
    });
    put_pdb(&s, &pdb).await;
    put(&s, "pods", "a", &pod("a")).await;
    put(&s, "pods", "b", &pod("b")).await;
    sync(&s).await;
    verify(&s, 0, 1, 1, 2).await;
    let kept = pdb_now(&s).await.status.unwrap().disrupted_pods.unwrap();
    assert!(kept.contains_key("a"));
}

// ---------------------------------------------------------------------------
// DisruptionAllowed condition + observedGeneration (pdbhelper)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn disruption_allowed_condition_reasons() {
    let s = storage();
    put_pdb(&s, &min_available(json!(1))).await;
    put(&s, "pods", "a", &pod("a")).await;
    sync(&s).await;
    let cond = |p: PodDisruptionBudget| {
        p.status
            .unwrap()
            .conditions
            .unwrap()
            .into_iter()
            .find(|c| c.condition_type == "DisruptionAllowed")
            .unwrap()
    };
    let c = cond(pdb_now(&s).await);
    assert_eq!(
        (c.status.as_str(), c.reason.as_deref()),
        ("False", Some("InsufficientPods"))
    );
    assert_eq!(c.observed_generation, Some(1));

    put(&s, "pods", "b", &pod("b")).await;
    sync(&s).await;
    let c = cond(pdb_now(&s).await);
    assert_eq!(
        (c.status.as_str(), c.reason.as_deref()),
        ("True", Some("SufficientPods"))
    );
}

#[tokio::test]
async fn sync_is_idempotent_when_nothing_changed() {
    // updatePdbStatus returns early when the status already matches, so a second
    // sync must not bump the PDB's resourceVersion.
    let s = storage();
    put_pdb(&s, &min_available(json!(1))).await;
    put(&s, "pods", "a", &pod("a")).await;
    sync(&s).await;
    let rv1 = pdb_now(&s).await.metadata.resource_version;
    sync(&s).await;
    let rv2 = pdb_now(&s).await.metadata.resource_version;
    assert_eq!(rv1, rv2);
}

// ---------------------------------------------------------------------------
// Pod events and the recheck queue (disruption.go addPod/updatePod/deletePod,
// enqueuePdbForRecheck): the part of the controller `Run` wires up.
// ---------------------------------------------------------------------------
async fn eventually<F, Fut>(what: &str, secs: u64, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if check().await {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("timed out waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn pod_readiness_change_updates_the_budget_without_waiting_for_a_resync() {
    let s = storage();
    put_pdb(&s, &min_available(json!(1))).await;
    put(&s, "pods", "a", &pod("a")).await;
    put(&s, "pods", "b", &pod("b")).await;

    let controller = Arc::new(PodDisruptionBudgetController::new(s.clone()));
    let handle = tokio::spawn(controller.run());

    let s2 = s.clone();
    eventually("initial status", 5, || {
        let s = s2.clone();
        async move { pdb_now(&s).await.status.map(|st| st.current_healthy) == Some(2) }
    })
    .await;

    // The pod goes unready. The PDB must follow within a couple of seconds,
    // far less than the 30s resync the controller used to depend on.
    let key = build_key("pods", Some(NS), "a");
    s.update(&key, &unready(pod("a"))).await.unwrap();
    let s2 = s.clone();
    eventually("currentHealthy drops to 1 on the pod event", 5, || {
        let s = s2.clone();
        async move { pdb_now(&s).await.status.map(|st| st.current_healthy) == Some(1) }
    })
    .await;

    // And a pod deletion likewise.
    s.delete(&build_key("pods", Some(NS), "b")).await.unwrap();
    let s2 = s.clone();
    eventually("currentHealthy drops to 0 on the pod delete", 5, || {
        let s = s2.clone();
        async move {
            pdb_now(&s)
                .await
                .status
                .map(|st| (st.current_healthy, st.expected_pods))
                == Some((0, 1))
        }
    })
    .await;
    handle.abort();
}

#[tokio::test]
async fn disrupted_pods_entry_is_rechecked_when_it_expires() {
    // buildDisruptedPodMap returns the earliest expiry and trySync enqueues the
    // PDB for recheck then (enqueuePdbForRecheck): the entry is pruned without
    // any other event arriving.
    let s = storage();
    let mut pdb = min_available(json!(1));
    // Expires in ~2s (DeletionTimeout is 2 minutes).
    let t = (chrono::Utc::now() - chrono::Duration::seconds(118))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    pdb["status"] = json!({
        "currentHealthy": 1, "desiredHealthy": 1, "disruptionsAllowed": 0, "expectedPods": 2,
        "disruptedPods": { "a": t },
    });
    put_pdb(&s, &pdb).await;
    put(&s, "pods", "a", &pod("a")).await;
    put(&s, "pods", "b", &pod("b")).await;

    let controller = Arc::new(PodDisruptionBudgetController::new(s.clone()));
    let handle = tokio::spawn(controller.run());

    let s2 = s.clone();
    eventually(
        "a is pruned from disruptedPods after it expires",
        10,
        || {
            let s = s2.clone();
            async move {
                pdb_now(&s)
                    .await
                    .status
                    .map(|st| {
                        st.disrupted_pods.map(|m| m.is_empty()).unwrap_or(true)
                            && st.current_healthy == 2
                    })
                    .unwrap_or(false)
            }
        },
    )
    .await;
    handle.abort();
}

// ---------------------------------------------------------------------------
// TestUpdatePDBStatusRetries
// ---------------------------------------------------------------------------

/// A storage double that, the first time pods are listed, plays the eviction
/// handler: it decrements `disruptionsAllowed`, records `disruptedPods` on the
/// PDB and deletes the pods. That is exactly the window the upstream test opens
/// between the controller's read and its write.
struct EvictingStorage {
    inner: MemoryStorage,
    fired: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl Storage for EvictingStorage {
    async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.create(key, value).await
    }
    async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
    where
        T: serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.get(key).await
    }
    async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.update(key, value).await
    }
    async fn update_raw(&self, key: &str, value: &Value) -> rusternetes_common::Result<()> {
        self.inner.update_raw(key, value).await
    }
    async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
        self.inner.delete(key).await
    }
    async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        let out = self.inner.list::<T>(prefix).await?;
        if prefix.contains("/pods/") && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            // (B) evict two pods between the controller's read and its write.
            let key = build_key("poddisruptionbudgets", Some(NS), PDB);
            let mut pdb: Value = self.inner.get(&key).await.unwrap();
            pdb["status"]["disruptionsAllowed"] = json!(0);
            pdb["status"]["disruptedPods"] = json!({
                "larry": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "curly": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            });
            self.inner.update(&key, &pdb).await.unwrap();
        }
        Ok(out)
    }
    async fn watch(
        &self,
        prefix: &str,
    ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
        self.inner.watch(prefix).await
    }
    async fn watch_from_revision(
        &self,
        prefix: &str,
        revision: i64,
    ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
        self.inner.watch_from_revision(prefix, revision).await
    }
    async fn current_revision(&self) -> rusternetes_common::Result<i64> {
        self.inner.current_revision().await
    }
    async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
        self.inner.is_revision_compacted(revision).await
    }
}

/// The controller must not write a status computed from a read the eviction
/// handler has since overtaken (`TestUpdatePDBStatusRetries`: "failed updates
/// due to ResourceVersion conflict should not cause a stale value of
/// DisruptionsAllowed to be written"). It must return the conflict so the key is
/// requeued, leaving the eviction's `disruptionsAllowed: 0` in place.
#[tokio::test]
async fn status_write_from_a_stale_read_does_not_clobber_a_concurrent_eviction() {
    let s = Arc::new(EvictingStorage {
        inner: MemoryStorage::new(),
        fired: std::sync::atomic::AtomicBool::new(false),
    });
    let mut pdb = min_available(json!(1));
    pdb["status"] = json!({
        "currentHealthy": 3, "desiredHealthy": 1, "disruptionsAllowed": 2, "expectedPods": 3,
    });
    s.create(&build_key("poddisruptionbudgets", Some(NS), PDB), &pdb)
        .await
        .unwrap();
    for n in ["moe", "larry", "curly"] {
        s.create(&build_key("pods", Some(NS), n), &pod(n))
            .await
            .unwrap();
    }

    let controller = PodDisruptionBudgetController::new(s.clone());
    let _ = controller.reconcile_all().await;

    let got: PodDisruptionBudget = s
        .get(&build_key("poddisruptionbudgets", Some(NS), PDB))
        .await
        .unwrap();
    let status = got.status.unwrap();
    assert_eq!(
        status.disruptions_allowed, 0,
        "a stale computation (disruptionsAllowed=2) must not overwrite the eviction"
    );
    assert!(status
        .disrupted_pods
        .unwrap_or_default()
        .contains_key("larry"));
}

// ---------------------------------------------------------------------------
// TestStalePodDisruption: the parts the pod-event wiring adds
// (`nonTerminatingPodHasStaleDisruptionCondition`, disruption.go:1046-1062)
// ---------------------------------------------------------------------------
fn pod_with_disruption_target(name: &str, phase: &str, reason: Option<&str>) -> Value {
    let mut cond = json!({
        "type": "DisruptionTarget", "status": "True", "message": "evicting",
        "lastTransitionTime": "2020-01-01T00:00:00Z",
    });
    if let Some(r) = reason {
        cond["reason"] = json!(r);
    }
    let mut p = pod(name);
    p["status"] = json!({ "phase": phase, "conditions": [cond] });
    p
}

async fn disruption_target(s: &Arc<MemoryStorage>, name: &str) -> Value {
    let p: Value = s.get(&build_key("pods", Some(NS), name)).await.unwrap();
    p["status"]["conditions"][0].clone()
}

#[tokio::test]
async fn stale_disruption_target_is_reset_on_any_non_terminal_phase_and_loses_its_reason() {
    use rusternetes_controller_manager::controllers::pod_disruption_budget::StalePodDisruptionController;
    let s = storage();
    // Upstream is not limited to Running: a Pending pod is non-terminal too.
    put(
        &s,
        "pods",
        "pending",
        &pod_with_disruption_target("pending", "Pending", Some("DeletionByTaintManager")),
    )
    .await;
    // Terminal phases and kubelet-set conditions are never stale.
    put(
        &s,
        "pods",
        "succeeded",
        &pod_with_disruption_target("succeeded", "Succeeded", None),
    )
    .await;
    put(
        &s,
        "pods",
        "kubelet",
        &pod_with_disruption_target("kubelet", "Running", Some("TerminationByKubelet")),
    )
    .await;

    StalePodDisruptionController::new(s.clone())
        .reconcile_all()
        .await
        .unwrap();

    let c = disruption_target(&s, "pending").await;
    assert_eq!(c["status"], "False");
    // apipod.UpdatePodCondition replaces the whole condition.
    assert!(
        c.get("reason").is_none() && c.get("message").is_none(),
        "{c}"
    );
    assert_eq!(disruption_target(&s, "succeeded").await["status"], "True");
    assert_eq!(disruption_target(&s, "kubelet").await["status"], "True");
}

#[tokio::test]
async fn stale_disruption_target_is_reset_by_the_pod_watch_without_a_resync() {
    use rusternetes_controller_manager::controllers::pod_disruption_budget::StalePodDisruptionController;
    let s = storage();
    let controller = Arc::new(StalePodDisruptionController::with_timeout(
        s.clone(),
        Duration::from_millis(100),
    ));
    let handle = tokio::spawn(controller.run());
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut p = pod_with_disruption_target("late", "Running", None);
    p["status"]["conditions"][0]["lastTransitionTime"] =
        json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    put(&s, "pods", "late", &p).await;

    let s2 = s.clone();
    eventually("the condition is reset once it goes stale", 8, || {
        let s = s2.clone();
        async move { disruption_target(&s, "late").await["status"] == "False" }
    })
    .await;
    handle.abort();
}
