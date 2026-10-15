//! Mirrors of the three `PodTemplates` conformance cases (#3753).
//!
//! Upstream (`release-1.35`): `test/e2e/common/node/podtemplates.go`
//!
//! | case | line |
//! |---|---|
//! | should run the lifecycle of PodTemplates | :56 |
//! | should delete a collection of pod templates | :127 |
//! | should replace a pod template | :181 |
//!
//! All three are pure API operations, so each mirror drives the api-server's
//! router step by step in the order the upstream body issues its client
//! calls. The e2e framework's per-test namespace (`f.Namespace`) is created
//! up front with `strict_namespaces()`, so nothing is auto-seeded.

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

/// `framework.NewDefaultFramework("podtemplate")` creates the test namespace.
async fn framework_namespace(api: &TestApiServer, name: &str) {
    let (s, ns) = send(
        api,
        "POST",
        "/api/v1/namespaces",
        "application/json",
        Some(&json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})),
    )
    .await;
    assert_eq!(s, 201, "create namespace {name}: {ns}");
}

/// `apimachineryutils.HaveValidResourceVersion`
/// (test/e2e/common/apimachinery/resourceversion_matchers.go:39-54): the
/// resourceVersion is a non-negative base-10 integer.
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

fn pod_template(name: &str, labels: Option<Value>, container: &str, image: &str) -> Value {
    let mut metadata = json!({ "name": name });
    if let Some(labels) = labels {
        metadata["labels"] = labels;
    }
    json!({
        "apiVersion": "v1",
        "kind": "PodTemplate",
        "metadata": metadata,
        "template": {
            "spec": { "containers": [{ "name": container, "image": image }] }
        }
    })
}

/// podtemplates.go:56 "should run the lifecycle of PodTemplates".
#[tokio::test]
async fn conformance_podtemplates_lifecycle() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "podtemplate-lifecycle";
    framework_namespace(&api, ns).await;
    let name = "nginx-pod-template-3c1c5d0e";
    let all = "/api/v1/podtemplates?labelSelector=podtemplate-static%3Dtrue";
    let one = format!("/api/v1/namespaces/{ns}/podtemplates/{name}");

    // :61-66 list in all namespaces by the static label; MUST be empty.
    let (s, list) = send(&api, "GET", all, "application/json", None).await;
    assert_eq!(s, 200, "list all PodTemplates: {list}");
    assert!(items(&list).is_empty(), "unable to find templates: {list}");

    // :68-83 create the PodTemplate.
    let (s, created) = send(
        &api,
        "POST",
        &format!("/api/v1/namespaces/{ns}/podtemplates"),
        "application/json",
        Some(&pod_template(
            name,
            Some(json!({ "podtemplate-static": "true" })),
            "nginx",
            "registry.k8s.io/e2e-test-images/nginx:1.14-4",
        )),
    )
    .await;
    assert_eq!(s, 201, "failed to create PodTemplate: {created}");

    // :85-89 get it; name matches and the resourceVersion is valid.
    let (s, read) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to get created PodTemplate: {read}");
    assert_eq!(read["metadata"]["name"], name);
    let read_rv = resource_version(&read);

    // :91-101 strategic-merge patch a label; the RV MUST grow.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({ "metadata": { "labels": { "podtemplate": "patched" } } })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch PodTemplate: {patched}");
    assert!(
        read_rv < resource_version(&patched),
        "patched object should have a larger resource version: {read_rv} vs {patched}"
    );

    // :103-106 get it; the new label is there.
    let (s, read) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to get PodTemplate: {read}");
    assert_eq!(
        read["metadata"]["labels"]["podtemplate"], "patched",
        "failed to patch template, new label not found: {read}"
    );

    // :108-110 delete it.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to delete PodTemplate: {body}");

    // :112-117 list by the static label again; MUST be empty.
    let (s, list) = send(&api, "GET", all, "application/json", None).await;
    assert_eq!(s, 200, "failed to list PodTemplate: {list}");
    assert!(
        items(&list).is_empty(),
        "PodTemplate list returned items, failed to delete PodTemplate: {list}"
    );
}

/// podtemplates.go:127 "should delete a collection of pod templates".
#[tokio::test]
async fn conformance_podtemplates_delete_collection() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "podtemplate-collection";
    framework_namespace(&api, ns).await;
    let names = [
        "test-podtemplate-1",
        "test-podtemplate-2",
        "test-podtemplate-3",
    ];
    let collection = format!("/api/v1/namespaces/{ns}/podtemplates");
    let selected = format!("{collection}?labelSelector=podtemplate-set%3Dtrue");

    // :130-148 "Create set of pod templates".
    for name in names {
        let (s, body) = send(
            &api,
            "POST",
            &collection,
            "application/json",
            Some(&pod_template(
                name,
                Some(json!({ "podtemplate-set": "true" })),
                "token-test",
                "registry.k8s.io/e2e-test-images/agnhost:2.59",
            )),
        )
        .await;
        assert_eq!(s, 201, "failed to create pod template {name}: {body}");
    }

    // :150-157 list by label in the namespace; MUST find all three.
    let (s, list) = send(&api, "GET", &selected, "application/json", None).await;
    assert_eq!(s, 200, "failed to get a list of pod templates: {list}");
    assert_eq!(
        items(&list).len(),
        names.len(),
        "looking for expected number of pod templates: {list}"
    );

    // :159-165 DeleteCollection by the same selector.
    let (s, body) = send(&api, "DELETE", &selected, "application/json", None).await;
    assert_eq!(s, 200, "failed to delete all pod templates: {body}");

    // :167-170 checkPodTemplateListQuantity(.., 0) (:218-237), polled.
    let mut remaining = usize::MAX;
    for _ in 0..50 {
        let (s, list) = send(&api, "GET", &selected, "application/json", None).await;
        assert_eq!(s, 200, "list pod templates: {list}");
        remaining = items(&list).len();
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(remaining, 0, "failed to count required pod templates");
}

/// podtemplates.go:181 "should replace a pod template".
#[tokio::test]
async fn conformance_podtemplates_replace() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "podtemplate-replace";
    framework_namespace(&api, ns).await;
    let name = "podtemplate-x7k2q";
    let one = format!("/api/v1/namespaces/{ns}/podtemplates/{name}");

    // :185-198 "Create a pod template".
    let (s, body) = send(
        &api,
        "POST",
        &format!("/api/v1/namespaces/{ns}/podtemplates"),
        "application/json",
        Some(&pod_template(
            name,
            None,
            "e2e-test",
            "registry.k8s.io/e2e-test-images/agnhost:2.59",
        )),
    )
    .await;
    assert_eq!(s, 201, "failed to create pod template: {body}");

    // :200-210 "Replace a pod template": get, set annotations, update — in
    // retry.RetryOnConflict, so a 409 is retried with a fresh read.
    let mut updated = Value::Null;
    for _ in 0..5 {
        let (s, mut pt) = send(&api, "GET", &one, "application/json", None).await;
        assert_eq!(s, 200, "Unable to get pod template {name}: {pt}");
        pt["metadata"]["annotations"] = json!({ "updated": "true" });
        let (s, body) = send(&api, "PUT", &one, "application/json", Some(&pt)).await;
        if s == 409 {
            continue;
        }
        assert_eq!(s, 200, "update pod template: {body}");
        updated = body;
        break;
    }

    // :211-212 the annotation MUST be on the updated object.
    assert_eq!(
        updated["metadata"]["annotations"]["updated"], "true",
        "updated object should have the applied annotation: {updated}"
    );
}
