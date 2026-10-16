//! Mirrors of the four `Garbage collector` conformance cases that had no
//! mirror at all (#1770).
//!
//! Upstream (`release-1.35`): `test/e2e/apimachinery/garbage_collector.go`
//!
//! | case | line |
//! |---|---|
//! | should delete RS created by deployment when not orphaning | :488 |
//! | should orphan RS created by deployment when deleteOptions.PropagationPolicy is Orphan | :547 |
//! | should not delete dependents that have both valid owner and owner that's waiting for dependents to be deleted | :722 |
//! | should not be blocked by dependency circle | :826 |
//!
//! Every case is controller-owned: the Deployment controller makes the
//! ReplicaSet, the ReplicaSet / ReplicationController controllers make the
//! pods, and the garbage collector does the deleting. The mirror drives the
//! real controllers over `ApiStorage` against a served `TestApiServer`, so
//! every controller write goes through the HTTP surface the way it does in a
//! cluster (same shape as `conformance_apimachinery_namespace_lifecycle.rs`).
//! Upstream's `wait.PollUntilContextTimeout` loops become a bounded number of
//! controller passes (`pump`).

use std::sync::Arc;

use rusternetes_client::http::ApiClient;
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_controller_manager::controllers::garbage_collector::GarbageCollector;
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_controller_manager::controllers::replicationcontroller::ReplicationControllerController;
use rusternetes_storage::api_storage::ApiStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// Upper bound on controller passes standing in for one upstream poll budget.
const MAX_PASSES: usize = 20;

/// `imageutils.GetE2EImage(imageutils.Nginx)`.
const NGINX: &str = "registry.k8s.io/e2e-test-images/nginx:1.14-4";

/// A `TestApiServer` served on a loopback port, plus `ApiStorage` pointed at
/// it for the controllers.
struct Served {
    api: TestApiServer,
    storage: Arc<ApiStorage>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve() -> Served {
    let api = TestApiServer::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test api-server");
    let address = listener.local_addr().expect("test api-server address");
    let server = tokio::spawn({
        let router = api.router.clone();
        async move {
            let _ = axum::serve(listener, router).await;
        }
    });
    let client = Arc::new(
        ApiClient::new(&format!("http://{address}"), true, None).expect("build api-server client"),
    );
    Served {
        api,
        storage: Arc::new(ApiStorage::new(client)),
        server,
    }
}

async fn send(api: &TestApiServer, method: &str, uri: &str, body: Option<&Value>) -> (u16, Value) {
    let (status, value) = api.send(method, uri, Some("application/json"), body).await;
    (status.as_u16(), value)
}

async fn list(api: &TestApiServer, uri: &str) -> Vec<Value> {
    let (s, l) = send(api, "GET", uri, None).await;
    assert_eq!(s, 200, "list {uri}: {l}");
    l["items"].as_array().cloned().unwrap_or_default()
}

/// One pass of every controller these cases depend on: the workload
/// controllers (Deployment → ReplicaSet → Pod, ReplicationController → Pod)
/// and the garbage collector.
async fn pass(served: &Served) {
    let s = served.storage.clone();
    DeploymentController::new(s.clone(), 1)
        .reconcile_all()
        .await
        .expect("deployment reconcile");
    ReplicaSetController::new(s.clone(), 1)
        .reconcile_all()
        .await
        .expect("replicaset reconcile");
    ReplicationControllerController::new(s.clone(), 1)
        .reconcile_all()
        .await
        .expect("replicationcontroller reconcile");
    GarbageCollector::new(s)
        .scan_and_collect()
        .await
        .expect("garbage collector scan");
}

/// `wait.PollUntilContextTimeout(..., condition)`: run controller passes until
/// `done` holds; `None` if it never does.
async fn pump<F, Fut>(served: &Served, mut done: F) -> Option<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..MAX_PASSES {
        if done().await {
            return Some(());
        }
        pass(served).await;
    }
    done().await.then_some(())
}

/// `newOwnerDeployment` (:148-150) → `e2edeployment.NewDeployment`
/// (test/e2e/framework/deployment/fixtures.go:42-72): 2 replicas,
/// RollingUpdate, the unique label as both selector and pod label.
fn owner_deployment(name: &str, label: (&str, &str)) -> Value {
    let labels = json!({ label.0: label.1 });
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": name, "labels": labels },
        "spec": {
            "replicas": 2,
            "selector": { "matchLabels": labels },
            "strategy": { "type": "RollingUpdate" },
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{ "name": "nginx", "image": NGINX, "securityContext": {} }]
                }
            }
        }
    })
}

/// `newOwnerRC` (:152-169) with `getPodTemplateSpec` (:131-146).
fn owner_rc(name: &str, replicas: i64, label: (&str, &str)) -> Value {
    let labels = json!({ label.0: label.1 });
    json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": { "name": name },
        "spec": {
            "replicas": replicas,
            "selector": labels,
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{ "name": "nginx", "image": NGINX }]
                }
            }
        }
    })
}

/// `newGCPod` (:171-191).
fn gc_pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name },
        "spec": {
            "terminationGracePeriodSeconds": 0,
            "containers": [{ "name": "nginx", "image": NGINX }]
        }
    })
}

/// `getBackgroundOptions` / `getOrphanOptions` / `getForegroundOptions`
/// (:93-106) plus `metav1.NewUIDPreconditions(uid)`.
fn delete_options(policy: &str, uid: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "propagationPolicy": policy,
        "preconditions": { "uid": uid }
    })
}

fn uid(obj: &Value) -> String {
    obj["metadata"]["uid"]
        .as_str()
        .unwrap_or_else(|| panic!("object has a uid: {obj}"))
        .to_string()
}

/// `verifyRemainingObjects` (:194-259) for the kinds these cases count.
async fn remaining(api: &TestApiServer, ns: &str) -> (usize, usize, usize) {
    (
        list(api, &format!("/apis/apps/v1/namespaces/{ns}/deployments"))
            .await
            .len(),
        list(api, &format!("/apis/apps/v1/namespaces/{ns}/replicasets"))
            .await
            .len(),
        list(api, &format!("/api/v1/namespaces/{ns}/pods"))
            .await
            .len(),
    )
}

/// garbage_collector.go:488 — should delete RS created by deployment when not
/// orphaning.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_deletes_rs_created_by_deployment_when_not_orphaning() {
    let served = serve().await;
    let api = &served.api;
    let ns = "gc-delete-rs";
    let deploys = format!("/apis/apps/v1/namespaces/{ns}/deployments");
    let rss = format!("/apis/apps/v1/namespaces/{ns}/replicasets");

    // "create the deployment" (:495-500).
    let (s, created) = send(
        api,
        "POST",
        &deploys,
        Some(&owner_deployment(
            "simpletest.deployment",
            ("gctest-00001", "delete_rs-00001"),
        )),
    )
    .await;
    assert_eq!(s, 201, "Failed to create deployment: {created}");

    // "Wait for the Deployment to create new ReplicaSet" (:501-512).
    pump(&served, || async { !list(api, &rss).await.is_empty() })
        .await
        .expect("Failed to wait for the Deployment to create some ReplicaSet");

    // "delete the deployment" (:514-519) — Background, UID precondition.
    let (s, b) = send(
        api,
        "DELETE",
        &format!("{deploys}/simpletest.deployment"),
        Some(&delete_options("Background", &uid(&created))),
    )
    .await;
    assert!(
        (200..300).contains(&s),
        "failed to delete the deployment: {s} {b}"
    );

    // "wait for all rs to be garbage collected" (:520-540):
    // Deployments 0, ReplicaSets 0, Pods 0.
    if pump(&served, || async { remaining(api, ns).await == (0, 0, 0) })
        .await
        .is_none()
    {
        panic!(
            "Failed to wait for all rs to be garbage collected; remaining \
             (deployments, rs, pods) = {:?}; remaining rs are: {:?}",
            remaining(api, ns).await,
            list(api, &rss).await
        );
    }
}

/// garbage_collector.go:547 — should orphan RS created by deployment when
/// deleteOptions.PropagationPolicy is Orphan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_orphans_rs_created_by_deployment_when_propagation_is_orphan() {
    let served = serve().await;
    let api = &served.api;
    let ns = "gc-orphan-rs";
    let deploys = format!("/apis/apps/v1/namespaces/{ns}/deployments");
    let rss = format!("/apis/apps/v1/namespaces/{ns}/replicasets");

    // "create the deployment" (:554-558).
    let (s, created) = send(
        api,
        "POST",
        &deploys,
        Some(&owner_deployment(
            "simpletest.deployment",
            ("gctest-00002", "orphan_rs-00002"),
        )),
    )
    .await;
    assert_eq!(s, 201, "Failed to create deployment: {created}");

    // "Wait for the Deployment to create new ReplicaSet" (:560-576).
    pump(&served, || async { !list(api, &rss).await.is_empty() })
        .await
        .expect("Failed to wait for the Deployment to create some ReplicaSet");
    let replicaset = list(api, &rss).await.remove(0);
    let rs_name = replicaset["metadata"]["name"].as_str().unwrap().to_string();

    // .Status.ObservedGeneration >= desired && .Status.Replicas ==
    // *.Spec.Replicas (:578-587).
    let desired_generation = replicaset["metadata"]["generation"].as_i64().unwrap_or(0);
    let spec_replicas = replicaset["spec"]["replicas"].clone();
    let rs_uri = format!("{rss}/{rs_name}");
    pump(&served, || async {
        let (s, rs) = send(api, "GET", &rs_uri, None).await;
        s == 200
            && rs["status"]["observedGeneration"].as_i64().unwrap_or(0) >= desired_generation
            && rs["status"]["replicas"] == spec_replicas
    })
    .await
    .unwrap_or_else(|| {
        panic!(
            "failed to verify .Status.Replicas is equal to .Spec.Replicas for replicaset {rs_name}"
        )
    });

    // "delete the deployment" (:589-594) — Orphan, UID precondition.
    let (s, b) = send(
        api,
        "DELETE",
        &format!("{deploys}/simpletest.deployment"),
        Some(&delete_options("Orphan", &uid(&created))),
    )
    .await;
    assert!(
        (200..300).contains(&s),
        "failed to delete the deployment: {s} {b}"
    );

    // "wait for deployment deletion to see if the garbage collector
    // mistakenly deletes the rs" (:595-605).
    pump(&served, || async { list(api, &deploys).await.is_empty() })
        .await
        .expect("Failed to wait for the Deployment to be deleted");

    // Deployments 0, ReplicaSets 1, Pods 2 (:606-628).
    let left = remaining(api, ns).await;
    assert_eq!(
        left,
        (0, 1, 2),
        "Failed to verify remaining (deployments, rs, pods); remaining rs post mortem: {:?}",
        list(api, &rss).await
    );
    // "Found ReplicaSet with non nil ownerRef" (:629-637): no controller ref.
    for rs in list(api, &rss).await {
        let controller = rs["metadata"]["ownerReferences"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|r| r["controller"] == json!(true));
        assert!(
            controller.is_none(),
            "Found ReplicaSet with non nil ownerRef {rs}"
        );
    }
}

/// garbage_collector.go:722 — should not delete dependents that have both
/// valid owner and owner that's waiting for dependents to be deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_keeps_dependents_with_a_valid_owner_during_foreground_delete() {
    let served = serve().await;
    let api = &served.api;
    let ns = "gc-valid-and-pending";
    let rcs = format!("/api/v1/namespaces/{ns}/replicationcontrollers");
    let pods = format!("/api/v1/namespaces/{ns}/pods");

    // estimateMaximumPods(ctx, clientSet, 10, 100) (:60-91): with no ready
    // schedulable nodes in-process the estimate is 0, bounded up to min=10.
    let replicas = 10;
    let half_replicas = replicas / 2;

    // "create the rc1" (:731-737).
    let (s, rc1) = send(
        api,
        "POST",
        &rcs,
        Some(&owner_rc(
            "simpletest-rc-to-be-deleted",
            replicas as i64,
            ("gctest_d-00003", "valid_and_pending_owners_d-00003"),
        )),
    )
    .await;
    assert_eq!(s, 201, "Failed to create replication controller: {rc1}");
    // "create the rc2" (:738-743) — zero replicas.
    let (s, rc2) = send(
        api,
        "POST",
        &rcs,
        Some(&owner_rc(
            "simpletest-rc-to-stay",
            0,
            ("gctest_s-00004", "valid_and_pending_owners_s-00004"),
        )),
    )
    .await;
    assert_eq!(s, 201, "Failed to create replication controller: {rc2}");

    // waitForReplicas(rc1) (:745, :1156-1178): status.replicas == spec.
    let rc1_uri = format!("{rcs}/simpletest-rc-to-be-deleted");
    pump(&served, || async {
        let (s, rc) = send(api, "GET", &rc1_uri, None).await;
        s == 200 && rc["status"]["replicas"] == json!(replicas)
    })
    .await
    .expect("failed to wait for the rc.Status.Replicas to reach rc.Spec.Replicas");

    // "set half of pods created by rc ... to have rc ... as owner as well"
    // (:747-755) — a strategic merge patch, which merges ownerReferences by
    // uid rather than replacing the list.
    let all = list(api, &pods).await;
    assert_eq!(all.len(), replicas, "pods created by rc1");
    let patch = json!({
        "metadata": { "ownerReferences": [{
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "name": "simpletest-rc-to-stay",
            "uid": uid(&rc2),
        }]}
    });
    for pod in all.iter().take(half_replicas) {
        let name = pod["metadata"]["name"].as_str().unwrap();
        let (status, b) = api
            .send(
                "PATCH",
                &format!("{pods}/{name}"),
                Some("application/strategic-merge-patch+json"),
                Some(&patch),
            )
            .await;
        assert_eq!(
            status.as_u16(),
            200,
            "failed to apply to pod {name} a strategic merge patch: {b}"
        );
    }

    // "delete the rc simpletest-rc-to-be-deleted" (:757-762) — Foreground.
    let (s, b) = send(
        api,
        "DELETE",
        &rc1_uri,
        Some(&delete_options("Foreground", &uid(&rc1))),
    )
    .await;
    assert!((200..300).contains(&s), "failed to delete the rc: {s} {b}");

    // "wait for the rc to be deleted" (:763-795).
    if pump(&served, || async {
        send(api, "GET", &rc1_uri, None).await.0 == 404
    })
    .await
    .is_none()
    {
        panic!(
            "failed to delete rc simpletest-rc-to-be-deleted; remaining pods: {:?}",
            list(api, &pods)
                .await
                .iter()
                .map(|p| &p["metadata"])
                .collect::<Vec<_>>()
        );
    }

    // "half of the pods should still exist" (:797-814): no deletion
    // timestamp, exactly one ownerReference left.
    let left = list(api, &pods).await;
    assert_eq!(
        left.len(),
        half_replicas,
        "expected {half_replicas} pods, got {}: {left:?}",
        left.len()
    );
    for pod in &left {
        assert!(
            pod["metadata"]["deletionTimestamp"].is_null(),
            "expected pod DeletionTimestamp to be nil, got {}",
            pod["metadata"]
        );
        let owners = pod["metadata"]["ownerReferences"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(0);
        assert_eq!(
            owners, 1,
            "expected pod to only have 1 owner, got {}",
            pod["metadata"]["ownerReferences"]
        );
    }
}

/// garbage_collector.go:826 — should not be blocked by dependency circle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_is_not_blocked_by_dependency_circle() {
    let served = serve().await;
    let api = &served.api;
    let ns = "gc-circle";
    let pods = format!("/api/v1/namespaces/{ns}/pods");

    // Create pod1, pod2, pod3 (:829-840).
    let mut created = Vec::new();
    for name in ["pod1", "pod2", "pod3"] {
        let (s, p) = send(api, "POST", &pods, Some(&gc_pod(name))).await;
        assert_eq!(s, 201, "failed to create pod {name}: {p}");
        created.push(p);
    }

    // "create circular dependency" (:841-856): pod1 -> pod3, pod2 -> pod1,
    // pod3 -> pod2, each controller + blockOwnerDeletion, as strategic merge
    // patches.
    let add_ref_patch = |owner: &Value| {
        json!({ "metadata": { "ownerReferences": [{
            "apiVersion": "v1",
            "kind": "Pod",
            "name": owner["metadata"]["name"],
            "uid": owner["metadata"]["uid"],
            "controller": true,
            "blockOwnerDeletion": true,
        }]}})
    };
    let mut pod1 = Value::Null;
    for (dependent, owner) in [(0usize, 2usize), (1, 0), (2, 1)] {
        let name = created[dependent]["metadata"]["name"]
            .as_str()
            .unwrap()
            .to_string();
        let (status, b) = api
            .send(
                "PATCH",
                &format!("{pods}/{name}"),
                Some("application/strategic-merge-patch+json"),
                Some(&add_ref_patch(&created[owner])),
            )
            .await;
        assert_eq!(
            status.as_u16(),
            200,
            "failed to apply to pod {name} a strategic merge patch: {b}"
        );
        if dependent == 0 {
            pod1 = b;
        }
    }

    // "delete one pod, should result in the deletion of all pods" (:857-861)
    // — Foreground, UID precondition on pod1.
    let (s, b) = send(
        api,
        "DELETE",
        &format!("{pods}/pod1"),
        Some(&delete_options("Foreground", &uid(&pod1))),
    )
    .await;
    assert!(
        (200..300).contains(&s),
        "failed to delete pod pod1: {s} {b}"
    );

    // Wait for every pod to be gone (:862-882).
    if pump(&served, || async { list(api, &pods).await.is_empty() })
        .await
        .is_none()
    {
        panic!(
            "failed to wait for all pods to be deleted; pods are {:?}",
            list(api, &pods).await
        );
    }
}
