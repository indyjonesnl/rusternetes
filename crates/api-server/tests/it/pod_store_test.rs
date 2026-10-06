//! Pod on the generic Store (#1990): the strategy rules of
//! `pkg/registry/core/pod/strategy.go` and the in-tree admission chain the
//! pod handlers used to run inline.

use axum::http::StatusCode;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PODS: &str = "/api/v1/namespaces/default/pods";

fn pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    })
}

fn scheduled_pod(name: &str) -> Value {
    let mut p = pod(name);
    p["spec"]["nodeName"] = json!("node-1");
    p
}

async fn create(api: &TestApiServer, body: &Value) -> Value {
    let (s, created) = api.post(PODS, body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    created
}

fn finalizers(stored: &Value) -> Vec<String> {
    stored["metadata"]["finalizers"]
        .as_array()
        .map(|f| f.iter().map(|v| v.as_str().unwrap().to_string()).collect())
        .unwrap_or_default()
}

/// `PrepareForCreate` (strategy.go:84-100): whatever status the client sent,
/// a new pod is `Pending` with its QoS class, at generation 1.
#[tokio::test]
async fn create_resets_status_to_pending_with_the_qos_class() {
    let api = TestApiServer::new();
    let mut body = pod("p1");
    body["status"] = json!({"phase": "Running", "podIP": "10.0.0.9"});
    body["metadata"]["generation"] = json!(7);
    let created = create(&api, &body).await;
    assert_eq!(created["status"]["phase"], "Pending", "{created}");
    assert_eq!(created["status"]["qosClass"], "BestEffort", "{created}");
    assert!(created["status"].get("podIP").is_none(), "{created}");
    assert_eq!(created["metadata"]["generation"], 1, "{created}");
}

/// `ValidatePod` runs `ValidatePodName`, a DNS subdomain.
#[tokio::test]
async fn an_invalid_pod_name_is_rejected() {
    let api = TestApiServer::new();
    let (s, body) = api.post(PODS, &pod("Bad_Name")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"].as_str().unwrap().contains("metadata.name"));
}

/// `CheckGracefulDelete` (strategy.go:166-197): a pod no node has been bound
/// to is deleted immediately, whatever its grace period says.
#[tokio::test]
async fn an_unscheduled_pod_is_deleted_immediately() {
    let api = TestApiServer::new();
    let mut body = pod("p1");
    body["spec"]["terminationGracePeriodSeconds"] = json!(30);
    create(&api, &body).await;
    let (s, deleted) = api.delete(&format!("{PODS}/p1")).await;
    assert_eq!(s, StatusCode::OK, "{deleted}");
    assert_eq!(deleted["kind"], "Pod", "{deleted}");
    let (s, _) = api.get(&format!("{PODS}/p1")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// A scheduled pod is deleted gracefully: it stays, stamped with a
/// `deletionTimestamp` and the grace period, for the kubelet to act on.
#[tokio::test]
async fn a_scheduled_pod_is_deleted_gracefully() {
    let api = TestApiServer::new();
    let mut body = scheduled_pod("p1");
    body["spec"]["terminationGracePeriodSeconds"] = json!(45);
    create(&api, &body).await;
    let (s, deleted) = api.delete(&format!("{PODS}/p1")).await;
    assert_eq!(s, StatusCode::OK, "{deleted}");
    let (s, stored) = api.get(&format!("{PODS}/p1")).await;
    assert_eq!(s, StatusCode::OK, "{stored}");
    assert!(
        stored["metadata"]["deletionTimestamp"].is_string(),
        "{stored}"
    );
    assert_eq!(stored["metadata"]["deletionGracePeriodSeconds"], 45);
}

/// A pod that already terminated is deleted immediately (strategy.go:186-189).
#[tokio::test]
async fn a_terminated_pod_is_deleted_immediately() {
    let api = TestApiServer::new();
    create(&api, &scheduled_pod("p1")).await;
    let key = build_key("pods", Some("default"), "p1");
    let mut stored: Value = api.storage.get(&key).await.unwrap();
    stored["status"]["phase"] = json!("Succeeded");
    api.storage.update(&key, &stored).await.unwrap();

    let (s, body) = api.delete(&format!("{PODS}/p1")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let (s, _) = api.get(&format!("{PODS}/p1")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// A graceful pod DELETE leaves the garbage-collection finalizer the
/// propagation policy calls for, as upstream's generic registry does for every
/// resource (`deletionFinalizersForGarbageCollection`, store.go:976) -- what
/// `[sig-api-machinery] Garbage collector should not be blocked by dependency
/// circle` depends on.
#[tokio::test]
async fn delete_applies_the_propagation_policy_finalizer() {
    for (query, expected) in [
        ("?propagationPolicy=Foreground", vec!["foregroundDeletion"]),
        ("?propagationPolicy=Orphan", vec!["orphan"]),
        ("?propagationPolicy=Background", vec![]),
        ("", vec![]),
    ] {
        let api = TestApiServer::new();
        create(&api, &scheduled_pod("gc-pod")).await;
        let (s, body) = api.delete(&format!("{PODS}/gc-pod{query}")).await;
        assert_eq!(s, StatusCode::OK, "{query}: {body}");
        let (_, stored) = api.get(&format!("{PODS}/gc-pod")).await;
        assert!(
            stored["metadata"]["deletionTimestamp"].is_string(),
            "{stored}"
        );
        assert_eq!(finalizers(&stored), expected, "{query}");
    }
}

/// A zero grace period does not bypass finalizers (store.go:1174): only a pod
/// with nothing pending is removed outright. `newGCPod`
/// (test/e2e/apimachinery/garbage_collector.go:181) has a zero grace period,
/// so deleting pod1 with foreground propagation must keep it for the cascade
/// (#1804).
#[tokio::test]
async fn grace_zero_delete_still_honours_finalizers() {
    for (existing, query, survives) in [
        (vec![], "", false),
        (vec![], "?propagationPolicy=Foreground", true),
        (vec![], "?propagationPolicy=Orphan", true),
        (vec!["example.com/blocker"], "", true),
        (vec![], "?propagationPolicy=Background", false),
    ] {
        let api = TestApiServer::new();
        let mut body = scheduled_pod("grace0");
        body["spec"]["terminationGracePeriodSeconds"] = json!(0);
        if !existing.is_empty() {
            body["metadata"]["finalizers"] = json!(existing);
        }
        create(&api, &body).await;
        let (s, resp) = api.delete(&format!("{PODS}/grace0{query}")).await;
        assert_eq!(s, StatusCode::OK, "{existing:?} {query}: {resp}");
        let (s, stored) = api.get(&format!("{PODS}/grace0")).await;
        assert_eq!(
            s == StatusCode::OK,
            survives,
            "finalizers={existing:?} {query}: {stored}"
        );
        if survives {
            assert!(
                stored["metadata"]["deletionTimestamp"].is_string(),
                "{stored}"
            );
            assert!(!finalizers(&stored).is_empty(), "{stored}");
        }
    }
}

/// `PrepareForUpdate` (strategy.go:103-109): the main resource never writes
/// status.
#[tokio::test]
async fn a_put_does_not_write_status() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut update = created.clone();
    update["status"]["phase"] = json!("Running");
    update["status"]["podIP"] = json!("10.1.1.1");
    let (s, body) = api.put(&format!("{PODS}/p1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["phase"], "Pending", "{body}");
    assert!(body["status"].get("podIP").is_none(), "{body}");
}

/// `updatePodGeneration` (strategy.go:230-236): a spec change moves the
/// generation, a metadata-only change does not.
#[tokio::test]
async fn generation_moves_with_the_spec() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    assert_eq!(created["metadata"]["generation"], 1);

    let mut update = created.clone();
    update["metadata"]["labels"] = json!({"a": "b"});
    let (s, body) = api.put(&format!("{PODS}/p1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 1, "{body}");

    let mut update = body.clone();
    update["spec"]["activeDeadlineSeconds"] = json!(60);
    let (s, body) = api.put(&format!("{PODS}/p1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["generation"], 2, "{body}");
}

/// A PATCH is an update: it meets `ValidatePodUpdate`'s fence too, so it
/// cannot change container resources outside `/resize`.
#[tokio::test]
async fn a_patch_cannot_change_container_resources() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let patch = json!({"spec": {"containers": [
        {"name": "c", "resources": {"limits": {"cpu": "1"}}}
    ]}});
    let (s, body) = api
        .send(
            "PATCH",
            &format!("{PODS}/p1"),
            Some("application/strategic-merge-patch+json"),
            Some(&patch),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("pod updates may not change fields"));
}

/// `podResizeStrategy` (strategy.go): `/resize` takes the new `resources` of
/// the containers, drops every other change, and flags the resize for the
/// kubelet (`status.resize = Proposed`, KEP-1287).
#[tokio::test]
async fn a_resize_patch_changes_only_the_resources() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let patch = json!({"spec": {"containers": [
        {"name": "c", "image": "other", "resources": {"requests": {"cpu": "500m"}}}
    ]}});
    let (s, body) = api
        .send(
            "PATCH",
            &format!("{PODS}/p1/resize"),
            Some("application/strategic-merge-patch+json"),
            Some(&patch),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let c = &body["spec"]["containers"][0];
    assert_eq!(c["resources"]["requests"]["cpu"], "500m", "{body}");
    assert_eq!(
        c["image"], "busybox",
        "the image is not a resize field: {body}"
    );
    assert_eq!(body["status"]["resize"], "Proposed", "{body}");
}

/// `ValidatePodEphemeralContainersUpdate` (validation.go:6181-6212): an
/// ephemeral container can be added through its subresource, and never
/// changed afterwards.
#[tokio::test]
async fn ephemeral_containers_are_add_only_through_their_subresource() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut update = created.clone();
    update["spec"]["ephemeralContainers"] = json!([{"name": "dbg", "image": "busybox"}]);
    let (s, body) = api
        .put(&format!("{PODS}/p1/ephemeralcontainers"), &update)
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["ephemeralContainers"][0]["name"], "dbg");

    let mut change = body.clone();
    change["spec"]["ephemeralContainers"][0]["image"] = json!("other");
    let (s, body) = api
        .put(&format!("{PODS}/p1/ephemeralcontainers"), &change)
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("may not be changed"));
}

/// The conformance spec "should update the ephemeral containers in an
/// existing pod" (test/e2e/common/node/ephemeral_containers.go:104): add one
/// container with a strategic-merge PATCH, read the pod back, append a second
/// container to what was read and PUT it. The existing container comes back
/// unchanged, so `ValidatePodEphemeralContainersUpdate` must accept it.
#[tokio::test]
async fn an_ephemeral_container_added_by_patch_survives_a_read_modify_put() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let patch = json!({"spec": {"$setElementOrder/ephemeralContainers": [{"name": "debugger"}], "ephemeralContainers": [{
        "name": "debugger", "image": "busybox",
        "command": ["/bin/sh", "-c", "while true; do echo polo; sleep 2; done"],
        "stdin": true, "tty": true
    }]}});
    let (s, body) = api
        .send(
            "PATCH",
            &format!("{PODS}/p1/ephemeralcontainers"),
            Some("application/strategic-merge-patch+json"),
            Some(&patch),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");

    let (s, mut read) = api.get(&format!("{PODS}/p1")).await;
    // client-go serializes the non-pointer `resources` struct as `{}`.
    read["spec"]["ephemeralContainers"][0]["resources"] = json!({});
    assert_eq!(s, StatusCode::OK, "{read}");
    read["spec"]["ephemeralContainers"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "name": "debugger2", "image": "busybox",
            "imagePullPolicy": "IfNotPresent", "terminationMessagePolicy": "File"
        }));
    let (s, body) = api
        .put(&format!("{PODS}/p1/ephemeralcontainers"), &read)
        .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(
        body["spec"]["ephemeralContainers"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

/// The conformance spec "should support pod readiness gates"
/// (test/e2e/common/node/pods.go:779) patches `status.conditions` twice with a
/// strategic-merge PATCH naming one condition each. `PodStatus.conditions` is
/// `patchStrategy:"merge" patchMergeKey:"type"` (core/v1/types.go), so the
/// second patch must add to the first rather than replace the list.
#[tokio::test]
async fn status_condition_patches_merge_by_type() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    for ty in ["k8s.io/test-condition1", "k8s.io/test-condition2"] {
        let patch = json!({"status": {"conditions": [{"type": ty, "status": "True"}]}});
        let (s, body) = api
            .send(
                "PATCH",
                &format!("{PODS}/p1/status"),
                Some("application/strategic-merge-patch+json"),
                Some(&patch),
            )
            .await;
        assert_eq!(s, StatusCode::OK, "{body}");
    }
    let (_, got) = api.get(&format!("{PODS}/p1")).await;
    let types: Vec<&str> = got["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["type"].as_str().unwrap())
        .collect();
    assert!(types.contains(&"k8s.io/test-condition1"), "{types:?}");
    assert!(types.contains(&"k8s.io/test-condition2"), "{types:?}");
}

/// `Priority.Admit` (plugin/pkg/admission/priority/admission.go:162-201): an
/// unknown PriorityClass is refused.
#[tokio::test]
async fn an_unknown_priority_class_is_forbidden() {
    let api = TestApiServer::new();
    let mut body = pod("p1");
    body["spec"]["priorityClassName"] = json!("missing");
    let (s, resp) = api.post(PODS, &body).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{resp}");
    assert!(resp["message"]
        .as_str()
        .unwrap()
        .contains("no PriorityClass with name missing was found"));
}

/// ... and `spec.priority` must be the one the class computes.
#[tokio::test]
async fn priority_comes_from_the_priority_class() {
    let api = TestApiServer::new();
    let (s, pc) = api
        .post(
            "/apis/scheduling.k8s.io/v1/priorityclasses",
            &json!({
                "apiVersion": "scheduling.k8s.io/v1", "kind": "PriorityClass",
                "metadata": {"name": "high"}, "value": 1000
            }),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{pc}");

    let mut body = pod("p1");
    body["spec"]["priorityClassName"] = json!("high");
    let created = create(&api, &body).await;
    assert_eq!(created["spec"]["priority"], 1000, "{created}");

    let mut body = pod("p2");
    body["spec"]["priorityClassName"] = json!("high");
    body["spec"]["priority"] = json!(5);
    let (s, resp) = api.post(PODS, &body).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{resp}");
}

/// `getDefaultPriorityClass` (plugin/pkg/admission/priority/admission.go:
/// 268-285): if a race left several `globalDefault` classes, the one with the
/// lowest value wins - not the first listed.
#[tokio::test]
async fn the_lowest_valued_global_default_wins() {
    use rusternetes_common::resources::PriorityClass;

    let api = TestApiServer::new();
    // "a-high" lists first but has the higher value.
    for (name, value) in [("a-high", 500), ("z-low", 10)] {
        let mut pc = PriorityClass::new(name, value);
        pc.global_default = Some(true);
        api.storage
            .create(&format!("/registry/priorityclasses/{name}"), &pc)
            .await
            .unwrap();
    }
    let created = create(&api, &pod("p1")).await;
    assert_eq!(created["spec"]["priority"], 10, "{created}");
    assert_eq!(created["spec"]["priorityClassName"], "z-low", "{created}");
}

/// `DefaultTolerationSeconds` adds the NotReady and Unreachable NoExecute
/// tolerations to a new pod.
#[tokio::test]
async fn default_tolerations_are_added() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let keys: Vec<&str> = created["spec"]["tolerations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["key"].as_str().unwrap())
        .collect();
    assert!(keys.contains(&"node.kubernetes.io/not-ready"), "{created}");
    assert!(
        keys.contains(&"node.kubernetes.io/unreachable"),
        "{created}"
    );
}

/// Kubelet-shaped status PATCH (`PatchPodStatus`, pkg/util/pod/pod.go:34-65;
/// the diff comes from `strategicpatch.CreateTwoWayMergePatch`). A container
/// going Waiting -> Running sends `state.running` and `state.waiting: null`;
/// the merge must drop `waiting` and the pod must stay decodable (#2453).
async fn seed_waiting_pod(api: &TestApiServer) {
    create(api, &scheduled_pod("p1")).await;
    let key = build_key("pods", Some("default"), "p1");
    let mut stored: Value = api.storage.get(&key).await.unwrap();
    stored["status"]["containerStatuses"] = json!([{
        "name": "c", "image": "busybox", "imageID": "", "ready": false, "restartCount": 0,
        "state": {"waiting": {"reason": "ContainerCreating"}}
    }]);
    api.storage.update(&key, &stored).await.unwrap();
}

async fn patch_status(api: &TestApiServer, patch: &Value) -> (StatusCode, Value) {
    api.send(
        "PATCH",
        &format!("{PODS}/p1/status"),
        Some("application/strategic-merge-patch+json"),
        Some(patch),
    )
    .await
}

#[tokio::test]
async fn kubelet_status_patch_waiting_to_running_with_null() {
    let api = TestApiServer::new();
    seed_waiting_pod(&api).await;
    let patch = json!({"status": {
        "$setElementOrder/containerStatuses": [{"name": "c"}],
        "containerStatuses": [{"name": "c", "ready": true,
            "state": {"running": {"startedAt": "2026-10-07T00:00:00Z"}, "waiting": null}}]}});
    let (s, body) = patch_status(&api, &patch).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let st = &body["status"]["containerStatuses"][0]["state"];
    assert!(
        st.get("running").is_some() && st.get("waiting").is_none(),
        "{body}"
    );
}

/// Go's `ContainerState` is a struct of three optional pointers and upstream
/// validation never requires exactly one (`ValidateContainerStateTransition`,
/// validation.go:5841 only checks Terminated transitions), so a patch lacking
/// `waiting: null` is accepted (Go stores both keys; see below).
#[tokio::test]
async fn kubelet_status_patch_tolerates_running_and_waiting_both_set() {
    let api = TestApiServer::new();
    seed_waiting_pod(&api).await;
    let patch = json!({"status": {"containerStatuses": [{"name": "c",
        "state": {"running": {"startedAt": "2026-10-07T00:00:00Z"}}}]}});
    let (s, body) = patch_status(&api, &patch).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let st = &body["status"]["containerStatuses"][0]["state"];
    // Deliberate deviation: our `ContainerState` holds one state, so the
    // stored pod keeps the more final one (Running over Waiting).
    assert!(
        st.get("running").is_some() && st.get("waiting").is_none(),
        "{body}"
    );
}
