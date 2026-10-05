//! Pod `/status`, `/binding` and the `/resize` PUT on the generic Store
//! (#1990, #2118): `podStatusStrategy` (pkg/registry/core/pod/strategy.go),
//! `ValidatePodStatusUpdate` (pkg/apis/core/validation/validation.go),
//! `BindingREST` (pkg/registry/core/pod/storage/storage.go) and
//! `ResizeREST.Update`.

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

async fn create(api: &TestApiServer, body: &Value) -> Value {
    let (s, created) = api.post(PODS, body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    created
}

async fn stored(api: &TestApiServer, name: &str) -> Value {
    api.storage
        .get(&build_key("pods", Some("default"), name))
        .await
        .unwrap()
}

async fn put_stored(api: &TestApiServer, name: &str, obj: &Value) {
    api.storage
        .update(&build_key("pods", Some("default"), name), obj)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// /status
// ---------------------------------------------------------------------------

/// `podStatusStrategy.PrepareForUpdate` (strategy.go:210-228): the spec is
/// the stored one, whatever the request carries; status is the request's.
#[tokio::test]
async fn status_put_changes_status_and_keeps_the_spec() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut update = created.clone();
    update["spec"]["containers"][0]["image"] = json!("other");
    update["spec"]["activeDeadlineSeconds"] = json!(5);
    update["status"]["phase"] = json!("Running");
    update["status"]["podIP"] = json!("10.0.0.7");
    update["status"]["podIPs"] = json!([{"ip": "10.0.0.7"}]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["phase"], "Running", "{out}");
    assert_eq!(out["status"]["podIP"], "10.0.0.7", "{out}");
    assert_eq!(out["spec"]["containers"][0]["image"], "busybox", "{out}");
    assert!(out["spec"].get("activeDeadlineSeconds").is_none(), "{out}");
    // the status write does not move the generation (the spec is unchanged)
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
}

/// strategy.go:215-219: `/status` cannot rewrite ownerReferences, "since old
/// kubelets corrupt them in a way that breaks garbage collection".
#[tokio::test]
async fn status_put_keeps_the_owner_references() {
    let api = TestApiServer::new();
    let mut body = pod("p1");
    body["metadata"]["ownerReferences"] = json!([
        {"apiVersion": "v1", "kind": "ConfigMap", "name": "owner", "uid": "u-1"}
    ]);
    let created = create(&api, &body).await;
    let mut update = created.clone();
    update["metadata"]["ownerReferences"] = json!([]);
    update["status"]["phase"] = json!("Running");
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(
        out["metadata"]["ownerReferences"][0]["name"], "owner",
        "{out}"
    );
}

/// strategy.go:220-224: an old kubelet drops `qosClass` when it rejects a
/// pod; the strategy backfills it so the immutability check passes.
#[tokio::test]
async fn status_put_backfills_a_missing_qos_class() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    assert_eq!(created["status"]["qosClass"], "BestEffort");
    let mut update = created.clone();
    update["status"] = json!({"phase": "Failed", "reason": "Rejected"});
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["qosClass"], "BestEffort", "{out}");
    assert_eq!(out["status"]["phase"], "Failed", "{out}");
}

/// `ValidatePodStatusUpdate` (validation.go:6034-6035): the QoS class is
/// immutable.
#[tokio::test]
async fn status_put_refuses_a_different_qos_class() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut update = created.clone();
    update["status"]["qosClass"] = json!("Guaranteed");
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    let msg = out["message"].as_str().unwrap();
    assert!(msg.contains("status.qosClass"), "{msg}");
    assert!(msg.contains("field is immutable"), "{msg}");
}

/// `preserveOldObservedGeneration` (strategy.go:230-259): a request that
/// clears `observedGeneration`, in the status or in a condition, keeps the
/// stored value.
#[tokio::test]
async fn status_put_preserves_observed_generation() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut seeded = stored(&api, "p1").await;
    seeded["status"]["observedGeneration"] = json!(1);
    seeded["status"]["conditions"] = json!([
        {"type": "Ready", "status": "True", "observedGeneration": 1},
        {"type": "PodScheduled", "status": "True", "observedGeneration": 1}
    ]);
    put_stored(&api, "p1", &seeded).await;

    let mut update = created.clone();
    update["metadata"]["resourceVersion"] = Value::Null;
    update["status"]["phase"] = json!("Running");
    update["status"]["conditions"] = json!([
        {"type": "PodScheduled", "status": "True"},
        {"type": "Ready", "status": "True"},
        {"type": "Ready", "status": "False", "observedGeneration": 4}
    ]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["observedGeneration"], 1, "{out}");
    let conds = out["status"]["conditions"].as_array().unwrap();
    assert_eq!(conds[0]["observedGeneration"], 1, "{out}");
    assert_eq!(conds[1]["observedGeneration"], 1, "{out}");
    // a value the request set is its own
    assert_eq!(conds[2]["observedGeneration"], 4, "{out}");
}

/// `ValidatePodStatusUpdate` (validation.go:6049-6055): `podIPs` are valid
/// IPs, at most one per family.
#[tokio::test]
async fn status_put_validates_the_pod_ips() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut bad = created.clone();
    bad["status"]["podIPs"] = json!([{"ip": "not-an-ip"}]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("status.podIPs[0]"),
        "{out}"
    );

    let mut two_v4 = created.clone();
    two_v4["status"]["podIPs"] = json!([{"ip": "10.0.0.1"}, {"ip": "10.0.0.2"}]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &two_v4).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("may specify no more than one IP for each IP family"),
        "{out}"
    );

    let mut dual = created.clone();
    dual["status"]["podIPs"] = json!([{"ip": "10.0.0.1"}, {"ip": "fd00::1"}]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &dual).await;
    assert_eq!(s, StatusCode::OK, "{out}");
}

/// `hostIPs[0]` must equal `hostIP` (validation.go:4585-4588).
#[tokio::test]
async fn status_put_requires_host_ip_to_match_the_first_host_ips() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut bad = created.clone();
    bad["status"]["hostIP"] = json!("10.1.1.1");
    bad["status"]["hostIPs"] = json!([{"ip": "10.1.1.2"}]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("must be equal to `hostIP`"),
        "{out}"
    );
}

/// `ValidateContainerStateTransition` (validation.go:5841-5903): with
/// `restartPolicy: Never` a terminated container cannot become non-terminated.
#[tokio::test]
async fn status_put_refuses_to_restart_a_terminated_container() {
    let api = TestApiServer::new();
    let mut body = pod("p1");
    body["spec"]["restartPolicy"] = json!("Never");
    let created = create(&api, &body).await;
    let mut seeded = stored(&api, "p1").await;
    seeded["status"]["containerStatuses"] = json!([{
        "name": "c", "ready": false, "restartCount": 0,
        "state": {"terminated": {"exitCode": 0}}
    }]);
    put_stored(&api, "p1", &seeded).await;

    let mut update = created.clone();
    update["metadata"]["resourceVersion"] = Value::Null;
    update["status"]["containerStatuses"] = json!([{
        "name": "c", "ready": true, "restartCount": 1,
        "state": {"running": {}}
    }]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    let msg = out["message"].as_str().unwrap();
    assert!(
        msg.contains("status.containerStatuses[0].state")
            && msg.contains("may not be transitioned to non-terminated state"),
        "{msg}"
    );
}

/// With `restartPolicy: Always` the same transition is allowed.
#[tokio::test]
async fn status_put_lets_an_always_restart_container_leave_terminated() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut seeded = stored(&api, "p1").await;
    seeded["status"]["containerStatuses"] = json!([{
        "name": "c", "ready": false, "restartCount": 0,
        "state": {"terminated": {"exitCode": 1}}
    }]);
    put_stored(&api, "p1", &seeded).await;

    let mut update = created.clone();
    update["metadata"]["resourceVersion"] = Value::Null;
    update["status"]["containerStatuses"] = json!([{
        "name": "c", "ready": true, "restartCount": 1,
        "state": {"running": {}}
    }]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{out}");
}

/// `validatePodConditions` (validation.go:6071-6087): a custom condition type
/// is a qualified name; a negative `observedGeneration` is refused.
#[tokio::test]
async fn status_put_validates_the_conditions() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut bad = created.clone();
    bad["status"]["conditions"] = json!([{"type": "not a qualified name!", "status": "True"}]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("status.conditions[0].Type"),
        "{out}"
    );

    let mut negative = created.clone();
    negative["status"]["conditions"] =
        json!([{"type": "Ready", "status": "True", "observedGeneration": -1}]);
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &negative).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("status.conditions[0].observedGeneration"),
        "{out}"
    );
}

/// `ValidatePodStatusUpdate` (validation.go:6015-6019): a nominated node name
/// is a valid node name.
#[tokio::test]
async fn status_put_validates_the_nominated_node_name() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut bad = created.clone();
    bad["status"]["nominatedNodeName"] = json!("Not_A_Node");
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("status.nominatedNodeName"),
        "{out}"
    );
}

/// validation.go:6021-6028: a nominated node cannot be set on a pod already
/// bound to a node (`ClearingNominatedNodeNameAfterBinding`, beta, on).
#[tokio::test]
async fn status_put_refuses_a_nomination_on_a_bound_pod() {
    let api = TestApiServer::new();
    let mut body = pod("p1");
    body["spec"]["nodeName"] = json!("node-1");
    let created = create(&api, &body).await;
    let mut bad = created.clone();
    bad["status"]["nominatedNodeName"] = json!("node-2");
    let (s, out) = api.put(&format!("{PODS}/p1/status"), &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("may not be set on pods that are already bound to a node"),
        "{out}"
    );
}

/// Subresources never create on update: the pod has to exist.
#[tokio::test]
async fn status_put_on_a_missing_pod_is_not_found() {
    let api = TestApiServer::new();
    let update = pod("ghost");
    let (s, out) = api.put(&format!("{PODS}/ghost/status"), &update).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{out}");
}

/// `StatusREST.Get` is the store's.
#[tokio::test]
async fn status_get_returns_the_pod() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let (s, out) = api.get(&format!("{PODS}/p1/status")).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Pod", "{out}");
    assert_eq!(out["status"]["phase"], "Pending", "{out}");
}

/// A merge patch into `/status` merges into the stored status; the spec in
/// the patch is dropped by the strategy.
#[tokio::test]
async fn status_patch_merges_and_ignores_the_spec() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let patch = json!({
        "spec": {"activeDeadlineSeconds": 9},
        "status": {"phase": "Running", "message": "up"}
    });
    let (s, out) = api.patch(&format!("{PODS}/p1/status"), &patch).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["phase"], "Running", "{out}");
    assert_eq!(out["status"]["message"], "up", "{out}");
    assert_eq!(out["status"]["qosClass"], "BestEffort", "{out}");
    assert!(out["spec"].get("activeDeadlineSeconds").is_none(), "{out}");
}

// ---------------------------------------------------------------------------
// /binding
// ---------------------------------------------------------------------------

fn binding(name: &str, node: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Binding",
        "metadata": {"name": name, "namespace": "default"},
        "target": {"kind": "Node", "name": node}
    })
}

fn binding_uri(name: &str) -> String {
    format!("{PODS}/{name}/binding")
}

/// `BindingREST.Create` (storage.go): `nodeName` is set, the nomination is
/// cleared, `PodScheduled=True` is recorded, the binding's annotations are
/// merged, and the answer is a success `Status`.
#[tokio::test]
async fn binding_assigns_the_pod_and_records_it() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let mut seeded = stored(&api, "p1").await;
    seeded["status"]["nominatedNodeName"] = json!("node-9");
    put_stored(&api, "p1", &seeded).await;

    let mut b = binding("p1", "node-1");
    b["metadata"]["annotations"] = json!({"scheduler/decision": "x"});
    let (s, out) = api.post(&binding_uri("p1"), &b).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    assert_eq!(out["kind"], "Status", "{out}");
    assert_eq!(out["status"], "Success", "{out}");

    let pod = stored(&api, "p1").await;
    assert_eq!(pod["spec"]["nodeName"], "node-1", "{pod}");
    assert!(pod["status"].get("nominatedNodeName").is_none(), "{pod}");
    assert_eq!(pod["metadata"]["annotations"]["scheduler/decision"], "x");
    let conds = pod["status"]["conditions"].as_array().unwrap();
    let scheduled = conds.iter().find(|c| c["type"] == "PodScheduled").unwrap();
    assert_eq!(scheduled["status"], "True", "{pod}");
}

/// storage.go: "name in URL does not match name in Binding object".
#[tokio::test]
async fn binding_name_must_match_the_url() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let (s, out) = api.post(&binding_uri("p1"), &binding("other", "n")).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("name in URL does not match name in Binding object"),
        "{out}"
    );
}

/// `ValidatePodBinding` (validation.go:6527-6539).
#[tokio::test]
async fn binding_needs_a_target_name() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let (s, out) = api.post(&binding_uri("p1"), &binding("p1", "")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"].as_str().unwrap().contains("target.name"),
        "{out}"
    );
}

/// `setPodNodeAndMetadata`: a pod is bound once; the failure is a Conflict
/// on `pods/binding` (assignPod wraps any non-status error).
#[tokio::test]
async fn binding_an_assigned_pod_conflicts() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let (s, _) = api.post(&binding_uri("p1"), &binding("p1", "node-1")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, out) = api.post(&binding_uri("p1"), &binding("p1", "node-2")).await;
    assert_eq!(s, StatusCode::CONFLICT, "{out}");
    let msg = out["message"].as_str().unwrap();
    assert!(
        msg.contains("pods/binding") && msg.contains("pod p1 is already assigned to node"),
        "{msg}"
    );
}

/// "Reject binding to a scheduling un-ready Pod."
#[tokio::test]
async fn binding_a_gated_pod_conflicts() {
    let api = TestApiServer::new();
    let mut body = pod("p1");
    body["spec"]["schedulingGates"] = json!([{"name": "wait"}]);
    create(&api, &body).await;
    let (s, out) = api.post(&binding_uri("p1"), &binding("p1", "node-1")).await;
    assert_eq!(s, StatusCode::CONFLICT, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("has non-empty .spec.schedulingGates"),
        "{out}"
    );
}

/// "pod %s is being deleted, cannot be assigned to a host".
#[tokio::test]
async fn binding_a_deleting_pod_conflicts() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let mut seeded = stored(&api, "p1").await;
    seeded["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
    put_stored(&api, "p1", &seeded).await;
    let (s, out) = api.post(&binding_uri("p1"), &binding("p1", "node-1")).await;
    assert_eq!(s, StatusCode::CONFLICT, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap()
            .contains("is being deleted, cannot be assigned to a host"),
        "{out}"
    );
}

/// The binding's `metadata.uid` and `resourceVersion` are preconditions on
/// the pod (`PreserveRequestObjectMetaSystemFieldsOnSubresourceCreate`).
#[tokio::test]
async fn binding_uid_precondition_is_enforced() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut b = binding("p1", "node-1");
    b["metadata"]["uid"] = json!("not-the-uid");
    let (s, out) = api.post(&binding_uri("p1"), &b).await;
    assert_eq!(s, StatusCode::CONFLICT, "{out}");
    assert!(stored(&api, "p1").await["spec"].get("nodeName").is_none());

    b["metadata"]["uid"] = created["metadata"]["uid"].clone();
    let (s, out) = api.post(&binding_uri("p1"), &b).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
}

/// Binding a pod that is not there is a plain NotFound.
#[tokio::test]
async fn binding_a_missing_pod_is_not_found() {
    let api = TestApiServer::new();
    let (s, out) = api
        .post(&binding_uri("ghost"), &binding("ghost", "n"))
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{out}");
}

/// A dry-run binding answers and writes nothing.
#[tokio::test]
async fn binding_dry_run_does_not_write() {
    let api = TestApiServer::new();
    create(&api, &pod("p1")).await;
    let (s, out) = api
        .post(
            &format!("{}?dryRun=All", binding_uri("p1")),
            &binding("p1", "node-1"),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    assert!(stored(&api, "p1").await["spec"].get("nodeName").is_none());
}

/// `PodTopologyLabelsAdmission` (beta, on): the plugin puts the node's
/// topology labels on the Binding and the REST copies them onto the pod.
#[tokio::test]
async fn binding_copies_the_node_topology_labels() {
    let api = TestApiServer::new();
    let node = json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": "node-1", "labels": {
            "topology.kubernetes.io/zone": "z1",
            "topology.kubernetes.io/region": "r1",
            "unrelated": "no"
        }}
    });
    let (s, out) = api.post("/api/v1/nodes", &node).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    create(&api, &pod("p1")).await;
    let (s, out) = api.post(&binding_uri("p1"), &binding("p1", "node-1")).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    let pod = stored(&api, "p1").await;
    assert_eq!(
        pod["metadata"]["labels"]["topology.kubernetes.io/zone"],
        "z1"
    );
    assert_eq!(
        pod["metadata"]["labels"]["topology.kubernetes.io/region"],
        "r1"
    );
    assert!(pod["metadata"]["labels"].get("unrelated").is_none());
}

// ---------------------------------------------------------------------------
// /resize PUT
// ---------------------------------------------------------------------------

/// `ResizeREST.Update` is the store's: a PUT takes the new container
/// `resources`, drops every other change, flags the resize, and bumps the
/// generation (`updatePodGeneration`).
#[tokio::test]
async fn resize_put_changes_only_the_resources() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut update = created.clone();
    update["spec"]["containers"][0]["image"] = json!("other");
    update["spec"]["containers"][0]["resources"] = json!({"requests": {"cpu": "500m"}});
    let (s, out) = api.put(&format!("{PODS}/p1/resize"), &update).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    let c = &out["spec"]["containers"][0];
    assert_eq!(c["resources"]["requests"]["cpu"], "500m", "{out}");
    assert_eq!(c["image"], "busybox", "{out}");
    assert_eq!(out["status"]["resize"], "Proposed", "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
}

/// A resize that names a container that is not there is invalid
/// (`ValidatePodResize`).
#[tokio::test]
async fn resize_put_refuses_a_renamed_container() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut update = created.clone();
    update["spec"]["containers"][0]["name"] = json!("renamed");
    let (s, out) = api.put(&format!("{PODS}/p1/resize"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// The PUT absorbs a stale `resourceVersion`-less write the way the store
/// does for every unconditional update, so a kubelet status write between the
/// client's GET and PUT does not turn the resize into a 409 (KEP-1287).
#[tokio::test]
async fn resize_put_without_a_resource_version_is_unconditional() {
    let api = TestApiServer::new();
    let created = create(&api, &pod("p1")).await;
    let mut seeded = stored(&api, "p1").await;
    seeded["status"]["phase"] = json!("Running");
    put_stored(&api, "p1", &seeded).await;

    let mut update = created.clone();
    update["metadata"]["resourceVersion"] = Value::Null;
    update["spec"]["containers"][0]["resources"] = json!({"requests": {"cpu": "1"}});
    let (s, out) = api.put(&format!("{PODS}/p1/resize"), &update).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["phase"], "Running", "{out}");
}
