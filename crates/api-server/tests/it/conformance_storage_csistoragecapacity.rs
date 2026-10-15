//! Mirror of the `CSIStorageCapacity` API conformance case (#3753).
//!
//! Upstream (`release-1.35`): `test/e2e/storage/csistoragecapacity.go:52`
//! "[sig-storage] CSIStorageCapacity should support CSIStorageCapacities API
//! operations".
//!
//! A pure API-operation case, so the mirror drives the api-server's router
//! step by step in the order the upstream body issues its client calls. The
//! e2e framework's per-test namespace (`f.Namespace`) is created up front with
//! `strict_namespaces()`; `f.UniqueName` is that namespace's name.

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

/// `expectWatchResult` (:184-221). Note the loop condition
/// `!sawAdded && !sawPatched && !sawUpdated`: it stops at the FIRST event that
/// is an ADDED CSC, or a MODIFIED CSC carrying the `patched`/`updated`
/// annotation. Every event it inspects MUST decode as a CSIStorageCapacity.
async fn expect_watch_result(kind: &str, watch: axum::response::Response) {
    let mut stream = watch.into_body().into_data_stream();
    let mut buf = String::new();
    // wait.ForeverTestTimeout is 30s; the in-process events are already
    // buffered, so a short bound suffices.
    let saw = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(chunk) = stream.next().await {
            let bytes = chunk.unwrap_or_else(|e| panic!("{kind}: watch channel errored: {e}"));
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if line.trim().is_empty() {
                    continue;
                }
                let evt: Value = serde_json::from_str(&line)
                    .unwrap_or_else(|e| panic!("{kind}: bad watch line {line:?}: {e}"));
                let obj = &evt["object"];
                match evt["type"].as_str() {
                    Some("MODIFIED") => {
                        assert_eq!(
                            obj["kind"], "CSIStorageCapacity",
                            "{kind}: expected CSC, got {evt}"
                        );
                        let ann = &obj["metadata"]["annotations"];
                        if ann["patched"] == "true" || ann["updated"] == "true" {
                            return true;
                        }
                    }
                    Some("ADDED") => {
                        assert_eq!(
                            obj["kind"], "CSIStorageCapacity",
                            "{kind}: expected CSC, got {evt}"
                        );
                        return true;
                    }
                    _ => {}
                }
            }
        }
        panic!("{kind}: watch channel should not close");
    })
    .await;
    assert!(
        matches!(saw, Ok(true)),
        "{kind}: timed out waiting for watch event"
    );
}

/// csistoragecapacity.go:52 "should support CSIStorageCapacities API
/// operations".
#[tokio::test]
async fn conformance_csistoragecapacity_api_operations() {
    let api = TestApiServer::builder().strict_namespaces().build();
    // framework.NewDefaultFramework("csistoragecapacity") (:40).
    let ns = "csistoragecapacity-mirror";
    let (s, body) = send(
        &api,
        "POST",
        "/api/v1/namespaces",
        "application/json",
        Some(&json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}})),
    )
    .await;
    assert_eq!(s, 201, "create namespace: {body}");
    let unique = ns;
    let collection = format!("/apis/storage.k8s.io/v1/namespaces/{ns}/csistoragecapacities");
    let all_ns = "/apis/storage.k8s.io/v1/csistoragecapacities";
    let sel = format!("labelSelector=test%3D{unique}");

    // :61-77 newCSIStorageCapacity(suffix): label test=f.UniqueName and the
    // fictional StorageClass "e2e.example.com".
    let new_csc = |suffix: &str| {
        json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "CSIStorageCapacity",
            "metadata": { "name": format!("{unique}{suffix}"), "labels": { "test": unique } },
            "storageClassName": "e2e.example.com",
        })
    };
    let csc = new_csc("-csc1");
    let csc2 = new_csc("-csc2");
    let csc3 = new_csc("-csc3");
    let csc_name = format!("{unique}-csc1");
    let one = format!("{collection}/{csc_name}");

    // :79-97 /apis MUST list storage.k8s.io with version v1.
    let (s, groups) = send(&api, "GET", "/apis", "application/json", None).await;
    assert_eq!(s, 200, "{groups}");
    let found = groups["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|g| g["name"] == "storage.k8s.io")
        .flat_map(|g| g["versions"].as_array().cloned().unwrap_or_default())
        .any(|v| v["version"] == "v1");
    assert!(
        found,
        "expected CSIStorageCapacity API group/version, got {groups}"
    );

    // :99-114 /apis/storage.k8s.io MUST list version v1.
    let (s, group) = send(
        &api,
        "GET",
        "/apis/storage.k8s.io",
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{group}");
    let found = group["versions"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|v| v["version"] == "v1");
    assert!(
        found,
        "expected CSIStorageCapacity API version, got {group}"
    );

    // :116-132 /apis/storage.k8s.io/v1 MUST list csistoragecapacities.
    let (s, resources) = send(
        &api,
        "GET",
        "/apis/storage.k8s.io/v1",
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{resources}");
    let found = resources["resources"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|r| r["name"] == "csistoragecapacities");
    assert!(found, "expected csistoragecapacities, got {resources}");

    // :134-136 create csc1.
    let (s, created) = send(&api, "POST", &collection, "application/json", Some(&csc)).await;
    assert_eq!(s, 201, "{created}");
    let created_rv = resource_version(&created);

    // :137-140 creating csc1 again MUST be AlreadyExists (409).
    let (s, body) = send(&api, "POST", &collection, "application/json", Some(&csc)).await;
    assert_eq!(s, 409, "expected 409, got {body}");
    assert_eq!(body["reason"], "AlreadyExists", "{body}");

    // :141-142 create csc2.
    let (s, body) = send(&api, "POST", &collection, "application/json", Some(&csc2)).await;
    assert_eq!(s, 201, "{body}");

    // :144-149 watch by label, in the namespace and across namespaces.
    let csc_watch = api
        .respond("GET", &format!("{collection}?watch=true&{sel}"), None, None)
        .await;
    assert_eq!(csc_watch.status().as_u16(), 200);
    let csc_watch_no_ns = api
        .respond("GET", &format!("{all_ns}?watch=true&{sel}"), None, None)
        .await;
    assert_eq!(csc_watch_no_ns.status().as_u16(), 200);

    // :151-153 create csc3 (added for a watch).
    let (s, body) = send(&api, "POST", &collection, "application/json", Some(&csc3)).await;
    assert_eq!(s, 201, "{body}");

    // :155-159 get csc1; UID MUST match and the RV MUST be valid.
    let (s, gotten) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "{gotten}");
    assert_eq!(gotten["metadata"]["uid"], created["metadata"]["uid"]);
    resource_version(&gotten);

    // :161-164 list in namespace by label: 3 items.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{collection}?{sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(
        items(&list).len(),
        3,
        "filtered list should have 3 items, got: {list}"
    );

    // :166-169 list across namespaces by label: 3 items.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{all_ns}?{sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(
        items(&list).len(),
        3,
        "filtered list should have 3 items, got: {list}"
    );

    // :171-175 merge patch an annotation; it MUST be present and the RV MUST
    // have grown.
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
        created_rv < resource_version(&patched),
        "patched object should have a larger resource version: {created_rv} vs {patched}"
    );

    // :177-182 update with an extra annotation; it MUST be present.
    let mut to_update = patched.clone();
    to_update["metadata"]["annotations"]["updated"] = json!("true");
    let (s, updated) = send(&api, "PUT", &one, "application/json", Some(&to_update)).await;
    assert_eq!(s, 200, "{updated}");
    assert_eq!(
        updated["metadata"]["annotations"]["updated"], "true",
        "updated object should have the applied annotation: {updated}"
    );

    // :222-223 both watches MUST deliver an expected event.
    expect_watch_result("in namespace", csc_watch).await;
    expect_watch_result("across namespace", csc_watch_no_ns).await;

    // :225-236 delete csc1; a get MUST then be NotFound (no finalizers are
    // set, so the DeletionTimestamp+Finalizers branch cannot apply).
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "{body}");
    let (s, body) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 404, "expected 404, got {body}");

    // :237-249 list MUST hold exactly min = max = 2 remaining.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{collection}?{sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(
        items(&list).len(),
        2,
        "expected 2 remaining CSIStorageCapacity objects: {list}"
    );

    // :251-253 DeleteCollection by label.
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{collection}?{sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{body}");

    // :254-262 any remaining object MUST be marked for deletion and held only
    // by a finalizer.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{collection}?{sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{list}");
    for csc in items(&list) {
        let held = !csc["metadata"]["deletionTimestamp"].is_null()
            && csc["metadata"]["finalizers"]
                .as_array()
                .is_some_and(|f| !f.is_empty());
        assert!(
            held,
            "CSIStorageCapacity should have been deleted or have DeletionTimestamp and Finalizers, but instead got: {csc}"
        );
    }
}
