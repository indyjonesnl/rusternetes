//! Mirrors of the DRA `resource.k8s.io/v1` CRUD conformance cases (#3758).
//!
//! Upstream (`release-1.35`), `test/e2e/dra/dra.go`:
//!
//! | case | line |
//! |---|---|
//! | CRUD operations for deviceclasses | `:87` |
//! | CRUD operations for resourceclaimtemplates | `:167` |
//! | CRUD operations for resoureslices | `:204` |
//!
//! Each case hands a `ResourceTestcase` to `e2econformance.TestResource`
//! (`test/e2e/framework/conformance/conformance.go:460`). [`test_resource`]
//! below follows that function step by step over HTTP for the shape these
//! three cases use: no `UpdateStatus` (so no status steps), no content
//! verification (`ContentVerificationEnabled` is unset, so only name,
//! namespace and a valid resourceVersion are compared), and a strategic merge
//! patch only. The informers it also runs are replaced by the list calls the
//! synchronous in-process store makes equivalent. `InitialSpec` is encoded
//! from a typed object without a TypeMeta, so the create body deliberately
//! carries no `apiVersion`/`kind` (conformance.go:414-416 and :685).

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

/// `framework.NewDefaultFramework("dra")` creates the test namespace.
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

/// `verify` without content verification (conformance.go:643-657): name and
/// namespace match and the resourceVersion is valid.
fn verify(what: &str, expected: &Value, actual: &Value) {
    assert_eq!(
        actual["metadata"]["name"], expected["metadata"]["name"],
        "{what}: name in returned object: {actual}"
    );
    assert_eq!(
        actual["metadata"].get("namespace"),
        expected["metadata"].get("namespace"),
        "{what}: namespace in returned object: {actual}"
    );
    resource_version(actual);
}

/// `e2econformance.TestResource` (conformance.go:460-925).
async fn test_resource(
    api: &TestApiServer,
    resource: &str,
    namespaced: bool,
    initial_spec: Value,
    update_spec: impl Fn(&mut Value),
    strategic_merge_patch_spec: Value,
) {
    // :461-481 clients: namespaced resources use the test namespace.
    let ns = format!("dra-crud-{resource}");
    let base = "/apis/resource.k8s.io/v1";
    let all = format!("{base}/{resource}");
    let collection = if namespaced {
        framework_namespace(api, &ns).await;
        format!("{base}/namespaces/{ns}/{resource}")
    } else {
        all.clone()
    };

    // :487-499 every object gets the label e2e-test.kubernetes.io=f.UniqueName
    // for List and DeleteCollection.
    let label_name = "e2e-test.kubernetes.io";
    let label_value = ns.as_str();
    let selector = format!("labelSelector={label_name}%3D{label_value}");
    let add_label = |obj: &mut Value| {
        if obj["metadata"]["labels"].is_null() {
            obj["metadata"]["labels"] = json!({});
        }
        obj["metadata"]["labels"][label_name] = json!(label_value);
    };

    // :502-505 desired initial object (GetInitialObject :250-262: name
    // "test", suffixed with the namespace when cluster-scoped).
    let name = if namespaced {
        "test".to_string()
    } else {
        format!("test-{ns}")
    };
    let mut desired_initial = initial_spec;
    desired_initial["metadata"] = json!({ "name": name });
    add_label(&mut desired_initial);
    if namespaced {
        desired_initial["metadata"]["namespace"] = json!(ns);
    }
    let one = format!("{collection}/{name}");

    // :517-535 "Get resource.k8s.io/v1": the resource list MUST include the
    // resource under test.
    let (s, api_resources) = send(api, "GET", base, "application/json", None).await;
    assert_eq!(s, 200, "get resource API: {api_resources}");
    assert!(
        api_resources["resources"]
            .as_array()
            .is_some_and(|rs| rs.iter().any(|r| r["name"] == resource)),
        "API for resource.k8s.io/v1 does not include entry for {resource}, got: {api_resources}"
    );

    // :684-685 create with FieldValidation=Strict. (:686 error text:
    // "create initial ...".)
    let (s, existing) = send(
        api,
        "POST",
        &format!("{collection}?fieldValidation=Strict"),
        "application/json",
        Some(&desired_initial),
    )
    .await;
    assert_eq!(s, 201, "create initial {resource}: {existing}");
    verify("create", &desired_initial, &existing);
    let created_rv = resource_version(&existing);

    // :703-710 get to check for existence.
    let (s, existing) = send(api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "get updated {resource}: {existing}");
    verify("get", &desired_initial, &existing);

    // :711-743 update with GetUpdateSpec applied to the existing object.
    let mut object = existing.clone();
    update_spec(&mut object);
    let (s, existing) = send(api, "PUT", &one, "application/json", Some(&object)).await;
    assert_eq!(s, 200, "update {resource}: {existing}");
    verify("update", &object, &existing);

    // :744-748 the resourceVersion MUST have increased during the update.
    let updated_rv = resource_version(&existing);
    assert!(
        created_rv < updated_rv,
        "ResourceVersion should have increased during update and didn't (before: {created_rv}, after: {updated_rv})"
    );

    // :795-830 patch loop (a single strategic merge patch): delete to start
    // anew, ensureNotFound, recreate, then patch.
    let (s, body) = send(api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "delete updated {resource}: {body}");
    let (s, body) = send(api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 404, "resource not removed: {body}");
    let (s, existing) = send(
        api,
        "POST",
        &collection,
        "application/json",
        Some(&desired_initial),
    )
    .await;
    assert_eq!(s, 201, "create {resource} again: {existing}");

    // createPatchObject (:446-455) copies namespace/name/uid/apiVersion/kind
    // from the existing object into the patch.
    let mut patch = strategic_merge_patch_spec;
    patch["metadata"]["name"] = existing["metadata"]["name"].clone();
    if namespaced {
        patch["metadata"]["namespace"] = existing["metadata"]["namespace"].clone();
    }
    patch["metadata"]["uid"] = existing["metadata"]["uid"].clone();
    patch["apiVersion"] = existing["apiVersion"].clone();
    patch["kind"] = existing["kind"].clone();
    // :815-822 patch with FieldValidation=Strict.
    let (s, patched) = send(
        api,
        "PATCH",
        &format!("{one}?fieldValidation=Strict"),
        "application/strategic-merge-patch+json",
        Some(&patch),
    )
    .await;
    assert_eq!(
        s, 200,
        "patch application/strategic-merge-patch+json {resource}: {patched}"
    );
    verify("patch", &object, &patched);

    // :896-904 list the collection by label: exactly the test resource.
    let (s, list) = send(
        api,
        "GET",
        &format!("{collection}?{selector}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "list {resource}: {list}");
    assert_eq!(
        items(&list).len(),
        1,
        "Should have listed exactly the test resource: {list}"
    );
    verify("list", &object, &items(&list)[0]);

    // :906-914 namespaced: list without a namespace by label too.
    if namespaced {
        let (s, list) = send(
            api,
            "GET",
            &format!("{all}?{selector}"),
            "application/json",
            None,
        )
        .await;
        assert_eq!(s, 200, "list {resource} in all namespaces: {list}");
        assert_eq!(
            items(&list).len(),
            1,
            "Should have listed exactly the test resource in all namespaces: {list}"
        );
        verify("list all namespaces", &object, &items(&list)[0]);
    }

    // :916-924 DeleteCollection by label, then the object MUST be gone.
    let (s, body) = send(
        api,
        "DELETE",
        &format!("{collection}?{selector}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "delete collection of {resource}: {body}");
    let (s, body) = send(api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 404, "resource not removed yet: {body}");
}

/// dra.go:87 "resource.k8s.io/v1 DeviceClass".
#[tokio::test]
async fn conformance_dra_deviceclass_crud() {
    let api = TestApiServer::builder().strict_namespaces().build();
    test_resource(
        &api,
        "deviceclasses",
        false,
        // :92-100 CEL expression "false" matches no devices.
        json!({ "spec": { "selectors": [{ "cel": { "expression": "false" } }] } }),
        // :101-104 still matches no devices.
        |obj| obj["spec"]["selectors"][0]["cel"]["expression"] = json!("1 == 0"),
        // :105.
        json!({ "spec": { "selectors": [{ "cel": { "expression": "1 == 0" } }] } }),
    )
    .await;
}

/// dra.go:167 "resource.k8s.io/v1 ResourceClaimTemplate".
#[tokio::test]
async fn conformance_dra_resourceclaimtemplate_crud() {
    let api = TestApiServer::builder().strict_namespaces().build();
    test_resource(
        &api,
        "resourceclaimtemplates",
        true,
        // :172-185 one exact device request.
        json!({ "spec": { "spec": { "devices": { "requests": [{
            "name": "req-0",
            "exactly": { "deviceClassName": "dra.example.com" },
        }] } } } }),
        // :186-193 the spec is immutable, so add a label instead.
        |obj| {
            if obj["metadata"]["labels"].is_null() {
                obj["metadata"]["labels"] = json!({});
            }
            obj["metadata"]["labels"]["test.dra.example.com"] = json!("test");
        },
        // :194.
        json!({ "metadata": { "labels": { "test.dra.example.com": "test" } } }),
    )
    .await;
}

/// dra.go:204 "resource.k8s.io/v1 ResourceSlice".
#[tokio::test]
async fn conformance_dra_resourceslice_crud() {
    let api = TestApiServer::builder().strict_namespaces().build();
    test_resource(
        &api,
        "resourceslices",
        false,
        // :209-220 an empty pool on a node that does not exist.
        json!({ "spec": {
            "driver": "dra.example.com",
            "pool": { "name": "cluster", "generation": 1, "resourceSliceCount": 1 },
            "nodeName": "no-such-node",
        } }),
        // :221-224 add a device.
        |obj| obj["spec"]["devices"] = json!([{ "name": "device-0" }]),
        // :225.
        json!({ "spec": { "devices": [{ "name": "device-0" }] } }),
    )
    .await;
}
