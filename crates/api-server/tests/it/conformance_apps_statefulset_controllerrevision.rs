//! Mirrors of three `[sig-apps]` conformance cases (#3768).
//!
//! Upstream (`release-1.35`):
//! - `test/e2e/apps/statefulset.go:987` "[sig-apps] StatefulSet Basic
//!   StatefulSet functionality [StatefulSetBasic] should have a working scale
//!   subresource".
//! - `test/e2e/apps/statefulset.go:1047` "... should list, patch and delete a
//!   collection of StatefulSets".
//! - `test/e2e/apps/controller_revision.go:128` "[sig-apps] ControllerRevision
//!   [Serial] should manage the lifecycle of a ControllerRevision".
//!
//! Each body mixes REST calls with waits on the StatefulSet / DaemonSet
//! controllers and the kubelet (`WaitForRunningAndReady`, `waitForStatus`,
//! `checkRunningOnAllNodes`). The mirror drives the api-server's router in the
//! order the upstream body issues its client calls. Where upstream waits for a
//! controller to act, the mirror performs the controller's write itself, through
//! the same endpoint the controller uses, and says so at that step; the
//! controllers' reconcile loops are pinned by `controller-manager`'s tests.
//! Random `utilrand` suffixes are fixed.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

async fn send(
    api: &TestApiServer,
    method: &str,
    uri: &str,
    ct: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let (status, value) = api.send(method, uri, Some(ct), body).await;
    (status.as_u16(), value)
}

/// `apimachineryutils.HaveValidResourceVersion`
/// (test/e2e/common/apimachinery/resourceversion_matchers.go:39-54).
fn resource_version(obj: &Value) -> u128 {
    let rv = obj["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or_else(|| panic!("object has no resourceVersion: {obj}"));
    rv.parse::<u128>()
        .unwrap_or_else(|_| panic!("resourceVersion {rv:?} is not a valid integer"))
}

fn items(list: &Value) -> usize {
    list["items"]
        .as_array()
        .unwrap_or_else(|| panic!("list has no items array: {list}"))
        .len()
}

/// `e2estatefulset.NewStatefulSet(name, ns, "test", replicas, nil, nil, labels)`
/// (test/e2e/framework/statefulset/fixtures.go) as marshalled by the client.
fn new_statefulset(name: &str, replicas: i64, labels: &Value) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": { "name": name, "namespace": NS, "labels": labels },
        "spec": {
            "replicas": replicas,
            "serviceName": "test",
            "selector": { "matchLabels": labels },
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "containers": [{
                        "name": "webserver",
                        "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4",
                    }],
                },
            },
            "podManagementPolicy": "OrderedReady",
            "updateStrategy": { "type": "RollingUpdate" },
        },
    })
}

/// statefulset.go:987 "should have a working scale subresource".
#[tokio::test]
async fn conformance_statefulset_scale_subresource() {
    let api = TestApiServer::new();
    let sts_base = format!("/apis/apps/v1/namespaces/{NS}/statefulsets");
    let one = format!("{sts_base}/ss");
    let scale_uri = format!("{one}/scale");
    let labels = json!({ "baz": "blah", "foo": "bar" });

    // :988-991 create replicas=1.
    let (s, created) = send(
        &api,
        "POST",
        &sts_base,
        "application/json",
        Some(&new_statefulset("ss", 1, &labels)),
    )
    .await;
    assert_eq!(s, 201, "create statefulset: {created}");

    // :993-994 WaitForRunningAndReady / waitForStatus wait for the
    // StatefulSet controller to publish status.replicas=1 through the status
    // subresource (stateful_set_control.go updateStatefulSetStatus); do that
    // write here.
    let mut with_status = created.clone();
    with_status["status"] = json!({ "replicas": 1, "readyReplicas": 1, "availableReplicas": 1 });
    let (s, body) = send(
        &api,
        "PUT",
        &format!("{one}/status"),
        "application/json",
        Some(&with_status),
    )
    .await;
    assert_eq!(s, 200, "controller status write: {body}");

    // :996-1002 getting scale subresource; Spec.Replicas and Status.Replicas
    // MUST both be 1.
    let (s, mut scale) = send(&api, "GET", &scale_uri, "application/json", None).await;
    assert_eq!(s, 200, "Failed to get scale subresource: {scale}");
    assert_eq!(scale["spec"]["replicas"], 1, "{scale}");
    assert_eq!(scale["status"]["replicas"], 1, "{scale}");

    // :1004-1011 updating a scale subresource: `scale.ResourceVersion = ""`
    // makes the update unconditional; Spec.Replicas = 2 MUST be returned.
    scale["metadata"]["resourceVersion"] = json!("");
    scale["spec"]["replicas"] = json!(2);
    let (s, scale_result) = send(&api, "PUT", &scale_uri, "application/json", Some(&scale)).await;
    assert_eq!(s, 200, "Failed to put scale subresource: {scale_result}");
    assert_eq!(scale_result["spec"]["replicas"], 2, "{scale_result}");

    // :1013-1018 verifying the statefulset Spec.Replicas was modified.
    let (s, ss) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "Failed to get statefulset resource: {ss}");
    assert_eq!(ss["spec"]["replicas"], 2, "{ss}");

    // :1020-1030 patch a scale subresource: the payload is a marshalled
    // autoscalingv1.Scale (zero metadata/status included) with Replicas = 4,
    // sent as a strategic merge patch against "scale".
    let (s, patched) = send(
        &api,
        "PATCH",
        &scale_uri,
        "application/strategic-merge-patch+json",
        Some(&json!({
            "metadata": { "creationTimestamp": null },
            "spec": { "replicas": 4 },
            "status": { "replicas": 0 },
        })),
    )
    .await;
    assert_eq!(s, 200, "Failed to patch stateful set: {patched}");

    // :1033-1036 verifying the statefulset Spec.Replicas was modified.
    let (s, ss) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "Failed to get statefulset resource: {ss}");
    assert_eq!(
        ss["spec"]["replicas"], 4,
        "statefulset should have 4 replicas: {ss}"
    );
}

/// statefulset.go:1047 "should list, patch and delete a collection of
/// StatefulSets".
#[tokio::test]
async fn conformance_statefulset_list_patch_delete_collection() {
    let api = TestApiServer::new();
    let sts_base = format!("/apis/apps/v1/namespaces/{NS}/statefulsets");
    let all = "/apis/apps/v1/statefulsets?labelSelector=test-ss%3Dpatched";
    let in_ns = format!("{sts_base}?labelSelector=test-ss%3Dpatched");
    let one = format!("{sts_base}/test-ss");
    let patch_image = "registry.k8s.io/pause:3.10.1";

    // :1059-1063 NewStatefulSet("test-ss", ..., 1, ..., ssPodLabels); create;
    // the new object MUST have a valid resourceVersion.
    let pod_labels = json!({ "name": "sample-pod", "pod": "agnhost" });
    let (s, ss) = send(
        &api,
        "POST",
        &sts_base,
        "application/json",
        Some(&new_statefulset("test-ss", 1, &pod_labels)),
    )
    .await;
    assert_eq!(s, 201, "create statefulset: {ss}");
    let created_rv = resource_version(&ss);

    // :1064-1065 WaitForRunningAndReady / waitForStatus: controller-driven.

    // :1067-1088 patching the StatefulSet (strategic merge).
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({
            "metadata": { "labels": { "test-ss": "patched" } },
            "spec": {
                "replicas": 2,
                "template": {
                    "spec": {
                        "terminationGracePeriodSeconds": 1,
                        "containers": [{ "name": "test-ss", "image": patch_image }],
                    },
                },
            },
        })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch Set: {patched}");
    assert!(
        created_rv < resource_version(&patched),
        "patched object should have a larger resource version: {patched}"
    );

    // :1089-1092 get; replicas MUST be 2 and Containers[0].Image MUST be the
    // patched one.
    // The upstream fixture names its container "webserver" while the patch
    // names "test-ss", so the merged list holds both; the strategic merge puts
    // the patch-only element ahead of the server-only one (no $setElementOrder),
    // making Containers[0] the patched image (apimachinery strategicpatch
    // patch.go:404-427 normalizeElementOrder / mergeSortedSlice).
    let (s, ss) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "Failed to get statefulset resource: {ss}");
    assert_eq!(
        ss["spec"]["replicas"], 2,
        "statefulset should have 2 replicas: {ss}"
    );
    assert_eq!(
        ss["spec"]["template"]["spec"]["containers"][0]["image"], patch_image,
        "statefulset not using ssPatchImage: {ss}"
    );

    // :1093-1094 WaitForRunningAndReady / waitForStatus: controller-driven.

    // :1096-1099 listing all StatefulSets across namespaces by label selector;
    // MUST find exactly one.
    let (s, list) = send(&api, "GET", all, "application/json", None).await;
    assert_eq!(s, 200, "failed to list StatefulSets: {list}");
    assert_eq!(items(&list), 1, "filtered list wasn't found: {list}");

    // :1101-1102 delete all of the StatefulSets (DeleteCollection with
    // gracePeriodSeconds=1).
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{in_ns}&gracePeriodSeconds=1"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to delete StatefulSets: {body}");

    // :1105-1108 verify that StatefulSets have been deleted.
    let (s, list) = send(&api, "GET", all, "application/json", None).await;
    assert_eq!(s, 200, "failed to list StatefulSets: {list}");
    assert_eq!(
        items(&list),
        0,
        "filtered list should have no Statefulsets: {list}"
    );
}

/// controller_revision.go:128 "should manage the lifecycle of a
/// ControllerRevision".
#[tokio::test]
async fn conformance_controllerrevision_lifecycle() {
    let api = TestApiServer::new();
    let ds_uri = format!("/apis/apps/v1/namespaces/{NS}/daemonsets");
    let cr_uri = format!("/apis/apps/v1/namespaces/{NS}/controllerrevisions");
    let cr_all = "/apis/apps/v1/controllerrevisions";
    let ds_name = "daemon-set";
    let ds_label = json!({ "daemonset-name": ds_name });
    let selector = "daemonset-name%3Ddaemon-set";

    // :134-135 create DaemonSet (newDaemonSetWithLabel).
    let (s, ds) = send(
        &api,
        "POST",
        &ds_uri,
        "application/json",
        Some(&json!({
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": { "name": ds_name, "namespace": NS, "labels": ds_label },
            "spec": {
                "selector": { "matchLabels": ds_label },
                "template": {
                    "metadata": { "labels": ds_label },
                    "spec": {
                        "containers": [{
                            "name": "app",
                            "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4",
                            "ports": [{ "containerPort": 9376 }],
                        }],
                        "terminationGracePeriodSeconds": 30,
                    },
                },
                "updateStrategy": { "type": "RollingUpdate" },
            },
        })),
    )
    .await;
    assert_eq!(s, 201, "create DaemonSet: {ds}");
    let ds_uid = ds["metadata"]["uid"].as_str().expect("ds uid").to_string();

    // :138-139 daemon pods launch on every node: controller+kubelet-driven.
    // The DaemonSet controller's one api-server-visible effect that this case
    // observes is the initial ControllerRevision (daemon_controller.go
    // constructHistory -> history.ControllerRevisionName, Revision 1, owner =
    // the DaemonSet), which the mirror creates the way the controller does.
    let initial_name = "daemon-set-6b9c8d7f5";
    let (s, body) = send(
        &api,
        "POST",
        &cr_uri,
        "application/json",
        Some(&json!({
            "apiVersion": "apps/v1",
            "kind": "ControllerRevision",
            "metadata": {
                "name": initial_name,
                "namespace": NS,
                "labels": {
                    "daemonset-name": ds_name,
                    "controller-revision-hash": "6b9c8d7f5",
                },
                "ownerReferences": [{
                    "apiVersion": "apps/v1", "kind": "DaemonSet", "name": ds_name,
                    "uid": ds_uid, "controller": true, "blockOwnerDeletion": true,
                }],
            },
            "data": { "spec": { "template": { "$patch": "replace" } } },
            "revision": 1,
        })),
    )
    .await;
    assert_eq!(s, 201, "controller creates the initial revision: {body}");

    // :144-147 confirm DaemonSet created with the label: list all namespaces.
    let (s, ds_list) = send(
        &api,
        "GET",
        &format!("/apis/apps/v1/daemonsets?labelSelector={selector}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to list Daemon Sets: {ds_list}");
    assert_eq!(items(&ds_list), 1, "filtered list wasn't found: {ds_list}");

    // :149 get the DaemonSet.
    let (s, _) = send(
        &api,
        "GET",
        &format!("{ds_uri}/{ds_name}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200);

    // :153-156 listing all ControllerRevisions with the label (all
    // namespaces, listAppsV1ControllerRevisionForAllNamespaces); MUST find one.
    let (s, revs) = send(
        &api,
        "GET",
        &format!("{cr_all}?labelSelector={selector}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "Failed to list ControllerRevision: {revs}");
    assert_eq!(
        items(&revs),
        1,
        "Failed to find any controllerRevisions: {revs}"
    );

    // :158-169 locate the current revision: owner is the DaemonSet; get it;
    // the listed object MUST carry a valid resourceVersion.
    let rev = revs["items"][0].clone();
    let oref = &rev["metadata"]["ownerReferences"][0];
    assert_eq!(oref["kind"], "DaemonSet");
    assert_eq!(oref["uid"], ds_uid.as_str());
    let initial_uri = format!("{cr_uri}/{}", rev["metadata"]["name"].as_str().unwrap());
    let (s, initial) = send(&api, "GET", &initial_uri, "application/json", None).await;
    assert_eq!(s, 200, "failed to lookup ControllerRevision: {initial}");
    let listed_rv = resource_version(&rev);
    let initial_name = initial["metadata"]["name"].as_str().unwrap().to_string();

    // :171-176 patching the ControllerRevision (strategic merge); the label
    // MUST be present and the RV MUST have grown.
    let (s, patched) = send(
        &api,
        "PATCH",
        &initial_uri,
        "application/strategic-merge-patch+json",
        Some(&json!({ "metadata": { "labels": { initial_name.as_str(): "patched" } } })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch ControllerRevision: {patched}");
    assert_eq!(
        patched["metadata"]["labels"][&initial_name], "patched",
        "Did not find 'patched' label: {patched}"
    );
    assert!(
        listed_rv < resource_version(&patched),
        "patched object should have a larger resource version: {patched}"
    );

    // :179-193 create a new ControllerRevision: name = ds.Name + "-" + hash,
    // owner = NewControllerRef(ds), Data = initial.Data, Revision = initial+1.
    let new_name = "daemon-set-7c5d6b8f4";
    let (s, new_rev) = send(
        &api,
        "POST",
        &cr_uri,
        "application/json",
        Some(&json!({
            "apiVersion": "apps/v1",
            "kind": "ControllerRevision",
            "metadata": {
                "name": new_name,
                "namespace": NS,
                "labels": {
                    "daemonset-name": ds_name,
                    "controller-revision-hash": "7c5d6b8f4",
                },
                "ownerReferences": [{
                    "apiVersion": "apps/v1", "kind": "DaemonSet", "name": ds_name,
                    "uid": ds_uid, "controller": true, "blockOwnerDeletion": true,
                }],
            },
            "data": initial["data"],
            "revision": initial["revision"].as_i64().unwrap() + 1,
        })),
    )
    .await;
    assert_eq!(s, 201, "Failed to create ControllerRevision: {new_rev}");

    // :197-198 confirm that there are two ControllerRevisions.
    let by_ds = format!("{cr_uri}?labelSelector={selector}");
    let (s, list) = send(&api, "GET", &by_ds, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(items(&list), 2, "{list}");

    // :201-202 deleting the initial ControllerRevision.
    let (s, body) = send(&api, "DELETE", &initial_uri, "application/json", None).await;
    assert_eq!(s, 200, "Failed to delete ControllerRevision: {body}");

    // :205-206 confirm that there is only one ControllerRevision.
    let (s, list) = send(&api, "GET", &by_ds, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(items(&list), 1, "{list}");

    // :209-210 list all in the namespace; the remaining one is "current".
    let (s, list) = send(&api, "GET", &cr_uri, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    let current_name = list["items"][0]["metadata"]["name"]
        .as_str()
        .unwrap()
        .to_string();
    let current_uri = format!("{cr_uri}/{current_name}");

    // :212-223 updating the current ControllerRevision (RetryOnConflict: get,
    // set label, update); the label MUST be present.
    let (s, mut current) = send(&api, "GET", &current_uri, "application/json", None).await;
    assert_eq!(s, 200, "Unable to get ControllerRevision: {current}");
    current["metadata"]["labels"][&current_name] = json!("updated");
    let (s, updated) = send(
        &api,
        "PUT",
        &current_uri,
        "application/json",
        Some(&current),
    )
    .await;
    assert_eq!(s, 200, "failed to update ControllerRevision: {updated}");
    assert_eq!(
        updated["metadata"]["labels"][&current_name], "updated",
        "Did not find 'updated' label: {updated}"
    );

    // :226-229 generate another ControllerRevision by patching the DaemonSet.
    let (s, body) = send(
        &api,
        "PATCH",
        &format!("{ds_uri}/{ds_name}"),
        "application/strategic-merge-patch+json",
        Some(&json!({
            "spec": { "template": { "spec": { "terminationGracePeriodSeconds": 1 } } },
            "updateStrategy": { "type": "RollingUpdate" },
        })),
    )
    .await;
    assert_eq!(s, 200, "error patching daemon set: {body}");
    // The DaemonSet controller reacts to the template change with a new
    // revision, Revision = max + 1 (daemon_controller.go getAllDaemonSetPods
    // -> dsc.constructHistory); do that write.
    let (s, body) = send(
        &api,
        "POST",
        &cr_uri,
        "application/json",
        Some(&json!({
            "apiVersion": "apps/v1",
            "kind": "ControllerRevision",
            "metadata": {
                "name": "daemon-set-5f7d9c6b8",
                "namespace": NS,
                "labels": {
                    "daemonset-name": ds_name,
                    "controller-revision-hash": "5f7d9c6b8",
                },
                "ownerReferences": [{
                    "apiVersion": "apps/v1", "kind": "DaemonSet", "name": ds_name,
                    "uid": ds_uid, "controller": true, "blockOwnerDeletion": true,
                }],
            },
            "data": initial["data"],
            "revision": updated["revision"].as_i64().unwrap() + 1,
        })),
    )
    .await;
    assert_eq!(s, 201, "controller creates the next revision: {body}");

    // :232-233 confirm that there are two ControllerRevisions.
    let (s, list) = send(&api, "GET", &by_ds, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(items(&list), 2, "{list}");

    // :239-240 removing a ControllerRevision via DeleteCollection with the
    // "updated" label selector.
    let updated_selector = format!("{current_name}%3Dupdated");
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{cr_uri}?labelSelector={updated_selector}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "Failed to delete ControllerRevision: {body}");

    // :243-244 confirm that there is only one ControllerRevision.
    let (s, list) = send(&api, "GET", &by_ds, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(items(&list), 1, "{list}");

    // :247-249 the remaining ControllerRevision's revision MUST be 3.
    let (s, list) = send(&api, "GET", &cr_uri, "application/json", None).await;
    assert_eq!(s, 200, "failed to list ControllerRevision: {list}");
    assert_eq!(
        list["items"][0]["revision"], 3,
        "failed to find the expected revision for the Controller: {list}"
    );
}
