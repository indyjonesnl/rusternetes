//! Mirrors of the RuntimeClass API and Node lifecycle conformance cases
//! (#3758).
//!
//! Upstream (`release-1.35`):
//!
//! | case | file:line |
//! |---|---|
//! | RuntimeClasses API operations | `test/e2e/common/node/runtimeclass.go:196` |
//! | lifecycle of a node (fake unschedulable node) | `test/e2e/node/node_lifecycle.go:53` |
//!
//! Both are pure API operations; each mirror drives the router step by step
//! in the order the upstream body issues its client calls. Both resources are
//! cluster-scoped, so no namespace is needed. `f.UniqueName` is a fixed
//! string here.

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

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

fn items(list: &Value) -> &Vec<Value> {
    list["items"]
        .as_array()
        .unwrap_or_else(|| panic!("list has no items array: {list}"))
}

/// The watch loop (runtimeclass.go:312-347). It stops at the FIRST event that
/// is an ADDED RuntimeClass, or a MODIFIED one carrying the
/// `patched`/`updated` annotation; every inspected event MUST decode as a
/// RuntimeClass and the channel MUST NOT close.
async fn expect_watch_result(watch: axum::response::Response) {
    let mut stream = watch.into_body().into_data_stream();
    let mut buf = String::new();
    // wait.ForeverTestTimeout is 30s; the in-process events are already
    // buffered, so a short bound suffices.
    let saw = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(chunk) = stream.next().await {
            let bytes = chunk.unwrap_or_else(|e| panic!("watch channel errored: {e}"));
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if line.trim().is_empty() {
                    continue;
                }
                let evt: Value = serde_json::from_str(&line)
                    .unwrap_or_else(|e| panic!("bad watch line {line:?}: {e}"));
                let obj = &evt["object"];
                match evt["type"].as_str() {
                    Some("MODIFIED") => {
                        assert_eq!(obj["kind"], "RuntimeClass", "expected RC, got {evt}");
                        let ann = &obj["metadata"]["annotations"];
                        if ann["patched"] == "true" || ann["updated"] == "true" {
                            return true;
                        }
                    }
                    Some("ADDED") => {
                        assert_eq!(obj["kind"], "RuntimeClass", "expected RC, got {evt}");
                        return true;
                    }
                    _ => {}
                }
            }
        }
        panic!("watch channel should not close");
    })
    .await;
    assert!(matches!(saw, Ok(true)), "timed out waiting for watch event");
}

/// `runtimeclasstest.NewRuntimeClass(name, handler)` with the label
/// `test=<unique>` (runtimeclass.go:205-210).
fn new_runtime_class(name: &str, handler: &str, unique: &str) -> Value {
    json!({
        "apiVersion": "node.k8s.io/v1", "kind": "RuntimeClass",
        "metadata": { "name": name, "labels": { "test": unique } },
        "handler": handler,
    })
}

/// runtimeclass.go:196 "should support RuntimeClasses API operations".
#[tokio::test]
async fn conformance_runtimeclass_api_operations() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let unique = "runtimeclass-mirror";
    let rcs = "/apis/node.k8s.io/v1/runtimeclasses";
    let sel = format!("labelSelector=test%3D{unique}");

    // :205-210 three RuntimeClasses with opaque handlers.
    let rc = new_runtime_class(
        &format!("{unique}-handler"),
        &format!("{unique}-conformance-runtime-class"),
        unique,
    );
    let rc2 = new_runtime_class(
        &format!("{unique}-handler2"),
        &format!("{unique}-conformance-runtime-class2"),
        unique,
    );
    let rc3 = new_runtime_class(
        &format!("{unique}-handler3"),
        &format!("{unique}-conformance-runtime-class3"),
        unique,
    );
    let rc_name = rc["metadata"]["name"].as_str().unwrap().to_string();
    let one = format!("{rcs}/{rc_name}");

    // :214-232 getting /apis: the node.k8s.io group with version v1.
    let (s, groups) = send(&api, "GET", "/apis", "application/json", None).await;
    assert_eq!(s, 200, "{groups}");
    let found = groups["groups"].as_array().is_some_and(|gs| {
        gs.iter().any(|g| {
            g["name"] == "node.k8s.io"
                && g["versions"]
                    .as_array()
                    .is_some_and(|vs| vs.iter().any(|v| v["version"] == "v1"))
        })
    });
    assert!(
        found,
        "expected RuntimeClass API group/version, got {groups}"
    );

    // :234-249 getting /apis/node.k8s.io: version v1.
    let (s, group) = send(&api, "GET", "/apis/node.k8s.io", "application/json", None).await;
    assert_eq!(s, 200, "{group}");
    assert!(
        group["versions"]
            .as_array()
            .is_some_and(|vs| vs.iter().any(|v| v["version"] == "v1")),
        "expected RuntimeClass API version, got {group}"
    );

    // :251-266 getting /apis/node.k8s.io/v1: the runtimeclasses resource.
    let (s, resources) = send(
        &api,
        "GET",
        "/apis/node.k8s.io/v1",
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{resources}");
    assert!(
        resources["resources"]
            .as_array()
            .is_some_and(|rs| rs.iter().any(|r| r["name"] == "runtimeclasses")),
        "expected runtimeclasses, got {resources}"
    );

    // :269-277 creating: rc succeeds, rc again MUST be 409 AlreadyExists, rc2
    // succeeds.
    let (s, created) = send(&api, "POST", rcs, "application/json", Some(&rc)).await;
    assert_eq!(s, 201, "{created}");
    let (s, body) = send(&api, "POST", rcs, "application/json", Some(&rc)).await;
    assert_eq!(s, 409, "expected 409, got {body}");
    assert_eq!(body["reason"], "AlreadyExists", "{body}");
    let (s, body) = send(&api, "POST", rcs, "application/json", Some(&rc2)).await;
    assert_eq!(s, 201, "{body}");

    // :279-282 watching by label.
    let watch = api
        .respond("GET", &format!("{rcs}?watch=true&{sel}"), None, None)
        .await;
    assert_eq!(watch.status().as_u16(), 200);

    // :284-286 rc3 is added for the watch.
    let (s, body) = send(&api, "POST", rcs, "application/json", Some(&rc3)).await;
    assert_eq!(s, 201, "{body}");

    // :288-292 getting: UID MUST match and the RV MUST be valid.
    let (s, gotten) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "{gotten}");
    assert_eq!(gotten["metadata"]["uid"], created["metadata"]["uid"]);
    resource_version(&gotten);

    // :294-297 listing by label: 3 items.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{rcs}?{sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(
        items(&list).len(),
        3,
        "filtered list should have 3 items: {list}"
    );

    // :299-303 patching (merge patch): annotation present, RV grew.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/merge-patch+json",
        Some(&json!({"metadata": {"annotations": {"patched": "true"}}})),
    )
    .await;
    assert_eq!(s, 200, "{patched}");
    assert_eq!(
        patched["metadata"]["annotations"]["patched"], "true",
        "patched object should have the applied annotation: {patched}"
    );
    assert!(
        resource_version(&gotten) < resource_version(&patched),
        "patched object should have a larger resource version"
    );

    // :305-310 updating: add the `updated` annotation to the patched object.
    let mut to_update = patched.clone();
    to_update["metadata"]["annotations"]["updated"] = json!("true");
    let (s, updated) = send(&api, "PUT", &one, "application/json", Some(&to_update)).await;
    assert_eq!(s, 200, "{updated}");
    assert_eq!(
        updated["metadata"]["annotations"]["updated"], "true",
        "updated object should have the applied annotation: {updated}"
    );

    // :312-347 the watch MUST deliver an expected event.
    expect_watch_result(watch).await;

    // :349-354 deleting rc; a get MUST then be 404.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "{body}");
    let (s, body) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 404, "expected 404, got {body}");

    // :356-358 listing by label: 2 items.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{rcs}?{sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(
        items(&list).len(),
        2,
        "filtered list should have 2 items: {list}"
    );

    // :360-362 deleting a collection by label.
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{rcs}?{sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{body}");

    // :363-365 listing by label: empty.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{rcs}?{sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{list}");
    assert!(
        items(&list).is_empty(),
        "filtered list should have 0 items: {list}"
    );
}

/// node_lifecycle.go:53 "should run through the lifecycle of a node".
#[tokio::test]
async fn conformance_node_lifecycle() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let nodes = "/api/v1/nodes";
    let name = "e2e-fake-node-abcde";
    let one = format!("{nodes}/{name}");

    // :60-79 a fake node: unschedulable, phase Running, Ready=True.
    let fake = json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": { "name": name },
        "spec": { "unschedulable": true },
        "status": {
            "phase": "Running",
            "conditions": [{
                "status": "True", "message": "Set from e2e test",
                "reason": "E2E", "type": "Ready",
            }],
        },
    });

    // :81-85 create; name matches and the RV is valid.
    let (s, created) = send(&api, "POST", nodes, "application/json", Some(&fake)).await;
    assert_eq!(s, 201, "failed to create node {name}: {created}");
    assert_eq!(created["metadata"]["name"], name);
    resource_version(&created);

    // :87-90 get.
    let (s, retrieved) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "Failed to retrieve Node {name}: {retrieved}");
    assert_eq!(retrieved["metadata"]["name"], name);

    // :92-98 strategic-merge patch a label keyed by the node name; the label
    // MUST be present and the RV MUST have grown.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({ "metadata": { "labels": { name: "patched" } } })),
    )
    .await;
    assert_eq!(s, 200, "Failed to patch {name}: {patched}");
    assert_eq!(patched["metadata"]["labels"][name], "patched", "{patched}");
    assert!(
        resource_version(&created) < resource_version(&patched),
        "patched object should have a larger resource version"
    );

    // :100-103 list by the patched selector: exactly one node.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{nodes}?labelSelector={name}%3Dpatched"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to list nodes: {list}");
    assert_eq!(items(&list).len(), 1, "{list}");

    // :105-117 RetryOnConflict(get, set label=updated, update).
    let (s, mut tmp) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "Unable to get {name}: {tmp}");
    tmp["metadata"]["labels"][name] = json!("updated");
    let (s, updated) = send(&api, "PUT", &one, "application/json", Some(&tmp)).await;
    assert_eq!(s, 200, "failed to update {name}: {updated}");
    assert_eq!(updated["metadata"]["labels"][name], "updated", "{updated}");

    // :119-121 delete.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to delete node: {body}");

    // :123-134 confirm deletion: get MUST be NotFound.
    let (s, body) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 404, "node still exists: {body}");
}
