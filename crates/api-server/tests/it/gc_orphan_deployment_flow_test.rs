//! The requests the kube-controller-manager garbage collector makes to orphan
//! a Deployment's ReplicaSet, replayed against the api-server.
//!
//! Ported from `attemptToOrphanWorker` (pkg/controller/garbagecollector/
//! garbagecollector.go:739-765): `orphanDependents` (:673-709) sends a
//! strategic merge patch deleting the owner reference
//! (`GenerateDeleteOwnerRefStrategicMergeBytes`,
//! pkg/controller/controller_ref_manager.go:572-589) to each dependent, then
//! `removeFinalizer` (operations.go:104-149) sends a merge patch carrying the
//! owner's `resourceVersion` and the finalizers without `orphan`.
//!
//! Conformance: "Garbage collector should orphan RS created by deployment
//! when deleteOptions.PropagationPolicy is Orphan"
//! (test/e2e/apimachinery/garbage_collector.go:547).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const DEPLOYMENTS: &str = "/apis/apps/v1/namespaces/default/deployments";
const REPLICASETS: &str = "/apis/apps/v1/namespaces/default/replicasets";

/// The metadata client's Accept header (client-go metadata/metadata.go
/// `ConfigFor`).
const POM_ACCEPT: &str = "application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json";

fn template() -> Value {
    json!({
        "metadata": {"labels": {"app": "gc"}},
        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
    })
}

#[tokio::test]
async fn orphaning_a_replicaset_lets_the_deployment_go() {
    let api = TestApiServer::new();

    let (st, dep) = api
        .post(
            DEPLOYMENTS,
            &json!({
                "apiVersion": "apps/v1", "kind": "Deployment",
                "metadata": {"name": "simpletest.deployment"},
                "spec": {"replicas": 2,
                         "selector": {"matchLabels": {"app": "gc"}},
                         "template": template()}
            }),
        )
        .await;
    assert!(st.is_success(), "{st} {dep}");
    let dep_uid = dep["metadata"]["uid"].as_str().unwrap().to_string();

    let (st, rs) = api
        .post(
            REPLICASETS,
            &json!({
                "apiVersion": "apps/v1", "kind": "ReplicaSet",
                "metadata": {"name": "simpletest.deployment-5f55f58754",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1", "kind": "Deployment",
                        "name": "simpletest.deployment", "uid": dep_uid,
                        "controller": true, "blockOwnerDeletion": true}]},
                "spec": {"replicas": 2,
                         "selector": {"matchLabels": {"app": "gc"}},
                         "template": template()}
            }),
        )
        .await;
    assert!(st.is_success(), "{st} {rs}");
    let rs_uid = rs["metadata"]["uid"].as_str().unwrap().to_string();

    // The e2e delete: Orphan plus a UID precondition.
    let (st, _, body) = api
        .send_raw(
            "DELETE",
            &format!("{DEPLOYMENTS}/simpletest.deployment"),
            Some("application/json"),
            Some(&json!({
                "propagationPolicy": "Orphan",
                "preconditions": {"uid": dep_uid}
            })),
        )
        .await;
    assert!(st.is_success(), "delete: {st} {body:?}");

    let (st, held) = api
        .get(&format!("{DEPLOYMENTS}/simpletest.deployment"))
        .await;
    assert!(st.is_success(), "held by the orphan finalizer: {held}");
    assert_eq!(held["metadata"]["finalizers"], json!(["orphan"]));

    // orphanDependents: strategic merge patch on the RS, as the metadata client.
    let (st, _, body, _) = api
        .send_full(
            "PATCH",
            &format!("{REPLICASETS}/simpletest.deployment-5f55f58754"),
            Some("application/strategic-merge-patch+json"),
            Some(POM_ACCEPT),
            Some(
                serde_json::to_vec(&json!({"metadata": {
                    "uid": rs_uid,
                    "ownerReferences": [{"$patch": "delete", "uid": dep_uid}]}}))
                .unwrap(),
            ),
        )
        .await;
    assert!(st.is_success(), "orphan RS: {st} {body:?}");

    // removeFinalizer: GET then merge patch with the owner's resourceVersion.
    let (st, _, _, got) = api
        .send_full(
            "GET",
            &format!("{DEPLOYMENTS}/simpletest.deployment"),
            None,
            Some(POM_ACCEPT),
            None,
        )
        .await;
    assert!(st.is_success(), "{st} {got}");
    let rv = got["metadata"]["resourceVersion"].clone();
    let (st, _, out, _) = api
        .send_full(
            "PATCH",
            &format!("{DEPLOYMENTS}/simpletest.deployment"),
            Some("application/merge-patch+json"),
            Some(POM_ACCEPT),
            Some(
                serde_json::to_vec(&json!({"metadata": {"resourceVersion": rv, "finalizers": []}}))
                    .unwrap(),
            ),
        )
        .await;
    assert!(st.is_success(), "remove finalizer: {st} {out:?}");

    let (st, left) = api
        .get(&format!("{DEPLOYMENTS}/simpletest.deployment"))
        .await;
    assert_eq!(st, 404, "the deployment must be gone: {left}");
    let (st, rs) = api
        .get(&format!("{REPLICASETS}/simpletest.deployment-5f55f58754"))
        .await;
    assert!(st.is_success(), "the RS survives: {rs}");
    assert!(
        rs["metadata"]["ownerReferences"]
            .as_array()
            .map_or(true, |a| a.is_empty()),
        "the RS is orphaned: {rs}"
    );
}

/// The same flow while the Deployment and ReplicaSet controllers keep writing
/// status (GET then PUT /status with the read resourceVersion), as they do in
/// the e2e run. The GC retries on conflict (operations.go:105
/// `retry.RetryOnConflict`); the Deployment must still end up gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphaning_survives_concurrent_status_writers() {
    for round in 0..40 {
        let api = TestApiServer::new();
        let dname = format!("d{round}");
        let rname = format!("d{round}-rs");
        let (st, dep) = api
            .post(
                DEPLOYMENTS,
                &json!({"apiVersion":"apps/v1","kind":"Deployment",
                    "metadata":{"name":dname},
                    "spec":{"replicas":2,"selector":{"matchLabels":{"app":"gc"}},
                            "template":template()}}),
            )
            .await;
        assert!(st.is_success(), "{dep}");
        let dep_uid = dep["metadata"]["uid"].as_str().unwrap().to_string();
        let (st, rs) = api
            .post(
                REPLICASETS,
                &json!({"apiVersion":"apps/v1","kind":"ReplicaSet",
                    "metadata":{"name":rname,"ownerReferences":[{
                        "apiVersion":"apps/v1","kind":"Deployment","name":dname,
                        "uid":dep_uid,"controller":true,"blockOwnerDeletion":true}]},
                    "spec":{"replicas":2,"selector":{"matchLabels":{"app":"gc"}},
                            "template":template()}}),
            )
            .await;
        assert!(st.is_success(), "{rs}");
        let rs_uid = rs["metadata"]["uid"].as_str().unwrap().to_string();

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut writers = Vec::new();
        for (base, name) in [(DEPLOYMENTS, dname.clone()), (REPLICASETS, rname.clone())] {
            let (api, stop) = (api.clone(), stop.clone());
            writers.push(tokio::spawn(async move {
                let mut n = 0i64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let (st, mut obj) = api.get(&format!("{base}/{name}")).await;
                    if !st.is_success() {
                        break;
                    }
                    n += 1;
                    obj["status"] = json!({"observedGeneration": n, "replicas": 2});
                    let _ = api.put(&format!("{base}/{name}/status"), &obj).await;
                    tokio::task::yield_now().await;
                }
            }));
        }

        let (st, _, body) = api
            .send_raw(
                "DELETE",
                &format!("{DEPLOYMENTS}/{dname}"),
                Some("application/json"),
                Some(&json!({"propagationPolicy":"Orphan",
                             "preconditions":{"uid":dep_uid}})),
            )
            .await;
        assert!(st.is_success(), "{body:?}");

        // orphanDependents
        let (st, _, body) = api
            .send_raw(
                "PATCH",
                &format!("{REPLICASETS}/{rname}"),
                Some("application/strategic-merge-patch+json"),
                Some(&json!({"metadata":{"uid":rs_uid,
                    "ownerReferences":[{"$patch":"delete","uid":dep_uid}]}})),
            )
            .await;
        assert!(st.is_success(), "round {round}: {body:?}");

        // removeFinalizer with RetryOnConflict, then the GC requeues.
        let mut done = false;
        for _ in 0..200 {
            let (st, got) = api.get(&format!("{DEPLOYMENTS}/{dname}")).await;
            if !st.is_success() {
                done = true;
                break;
            }
            let rv = got["metadata"]["resourceVersion"].clone();
            let (st, out) = api
                .patch(
                    &format!("{DEPLOYMENTS}/{dname}"),
                    &json!({"metadata":{"resourceVersion":rv,"finalizers":[]}}),
                )
                .await;
            if st.is_success() {
                done = true;
                break;
            }
            assert_eq!(st, 409, "round {round}: {out}");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in writers {
            let _ = w.await;
        }
        assert!(done, "round {round}: the GC never removed the finalizer");
        let (st, left) = api.get(&format!("{DEPLOYMENTS}/{dname}")).await;
        assert_eq!(
            st, 404,
            "round {round}: the deployment must be gone: {left}"
        );
        let (_, rs) = api.get(&format!("{REPLICASETS}/{rname}")).await;
        assert!(
            rs["metadata"]["ownerReferences"]
                .as_array()
                .map_or(true, |a| a.is_empty()),
            "round {round}: RS still owned: {rs}"
        );
    }
}
