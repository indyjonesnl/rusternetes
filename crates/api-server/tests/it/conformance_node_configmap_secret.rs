//! Mirrors of the ConfigMap and Secret API conformance cases (#3758).
//!
//! Upstream (`release-1.35`):
//!
//! | case | file:line |
//! |---|---|
//! | ConfigMap, with empty-key | `test/e2e/common/node/configmap.go:141` |
//! | ConfigMap Update | `test/e2e/common/node/configmap.go:150` |
//! | ConfigMap lifecycle | `test/e2e/common/node/configmap.go:176` |
//! | Secrets, with empty-key | `test/e2e/common/node/secrets.go:143` |
//! | Secret patching | `test/e2e/common/node/secrets.go:157` |
//! | ConfigMap Volume, immutability | `test/e2e/common/storage/configmap_volume.go:504` |
//! | Secret Volume, immutability | `test/e2e/common/storage/secrets_volume.go:386` |
//!
//! All are pure API operations, so each mirror drives the router step by step
//! in the order the upstream body issues its client calls. The e2e framework's
//! per-test namespace (`f.Namespace`) is created up front with
//! `strict_namespaces()`.

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

/// `framework.NewDefaultFramework(...)` creates the test namespace.
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

/// `newConfigMap` (configmap.go:327-339).
fn new_config_map(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": { "name": name },
        "data": { "data-1": "value-1", "data-2": "value-2", "data-3": "value-3" },
    })
}

/// `secretForTest` (secrets_volume.go:459); `[]byte` values travel as
/// base64 in JSON.
fn secret_for_test(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": { "name": name },
        "data": {
            "data-1": "dmFsdWUtMQo=", // "value-1\n"
            "data-2": "dmFsdWUtMgo=", // "value-2\n"
            "data-3": "dmFsdWUtMwo=", // "value-3\n"
        },
    })
}

// ---------------------------------------------------------------- ConfigMap

/// configmap.go:141 "should fail to create ConfigMap with empty key".
#[tokio::test]
async fn conformance_configmap_empty_key() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "configmap-empty-key";
    framework_namespace(&api, ns).await;

    // newConfigMapWithEmptyKey (:341-355; the case body is :142-143): Data {"": "value-1"}; the create
    // MUST fail (ValidateConfigMap rejects the key with an Invalid status).
    let (s, body) = send(
        &api,
        "POST",
        &format!("/api/v1/namespaces/{ns}/configmaps"),
        "application/json",
        Some(&json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": { "name": "configmap-test-emptykey-1" },
            "data": { "": "value-1" },
        })),
    )
    .await;
    assert_eq!(s, 422, "created configMap with empty key: {body}");
    assert_eq!(body["reason"], "Invalid", "{body}");
}

/// configmap.go:150 "should update ConfigMap successfully".
#[tokio::test]
async fn conformance_configmap_update() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "configmap-update";
    framework_namespace(&api, ns).await;
    let name = "configmap-test-1";
    let collection = format!("/api/v1/namespaces/{ns}/configmaps");
    let one = format!("{collection}/{name}");

    // :153-155 create.
    let (s, body) = send(
        &api,
        "POST",
        &collection,
        "application/json",
        Some(&new_config_map(name)),
    )
    .await;
    assert_eq!(s, 201, "failed to create ConfigMap: {body}");

    // :157-162 replace Data and Update with the locally built object (no
    // resourceVersion, as upstream sends it).
    let mut cm = new_config_map(name);
    cm["data"] = json!({ "data": "value" });
    let (s, body) = send(&api, "PUT", &one, "application/json", Some(&cm)).await;
    assert_eq!(s, 200, "failed to update ConfigMap: {body}");

    // :164-167 get; Data MUST equal the updated Data.
    let (s, got) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to get ConfigMap: {got}");
    assert_eq!(got["data"], cm["data"], "{got}");
}

/// configmap.go:176 "should run through a ConfigMap lifecycle".
#[tokio::test]
async fn conformance_configmap_lifecycle() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "configmap-lifecycle";
    framework_namespace(&api, ns).await;
    let name = "test-configmap-1";
    let collection = format!("/api/v1/namespaces/{ns}/configmaps");
    let one = format!("{collection}/{name}");

    // :192-194 create with the static label and Data valueName=value.
    let (s, body) = send(
        &api,
        "POST",
        &collection,
        "application/json",
        Some(&json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": { "name": name, "labels": { "test-configmap-static": "true" } },
            "data": { "valueName": "value" },
        })),
    )
    .await;
    assert_eq!(s, 201, "failed to create ConfigMap: {body}");

    // :196-201 get; data, label and a valid resourceVersion.
    let (s, cm) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to get ConfigMap: {cm}");
    assert_eq!(cm["data"]["valueName"], "value");
    assert_eq!(cm["metadata"]["labels"]["test-configmap-static"], "true");
    resource_version(&cm);

    // :203-218 strategic-merge patch label test-configmap=patched and
    // valueName=value1; the RV MUST have grown.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({
            "metadata": { "labels": { "test-configmap": "patched" } },
            "data": { "valueName": "value1" },
        })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch ConfigMap: {patched}");
    assert!(
        resource_version(&cm) < resource_version(&patched),
        "patched object should have a larger resource version"
    );

    // :220-238 list in ALL namespaces by test-configmap=patched; the ConfigMap
    // MUST be found with every expected field.
    let (s, list) = send(
        &api,
        "GET",
        "/api/v1/configmaps?labelSelector=test-configmap%3Dpatched",
        "application/json",
        None,
    )
    .await;
    assert_eq!(
        s, 200,
        "failed to list ConfigMaps with LabelSelector: {list}"
    );
    assert!(
        items(&list).iter().any(|c| c["metadata"]["name"] == name
            && c["metadata"]["namespace"] == ns
            && c["metadata"]["labels"]["test-configmap-static"] == "true"
            && c["metadata"]["labels"]["test-configmap"] == "patched"
            && c["data"]["valueName"] == "value1"),
        "failed to find ConfigMap {ns}/{name} by label selector: {list}"
    );

    // :240-244 DeleteCollection by test-configmap-static=true.
    let static_sel = "labelSelector=test-configmap-static%3Dtrue";
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{collection}?{static_sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to delete ConfigMap collection: {body}");

    // :246-251 list in the namespace by the static label MUST be empty.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{collection}?{static_sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to list ConfigMap by LabelSelector: {list}");
    assert!(
        items(&list).is_empty(),
        "ConfigMap is still present after being deleted by collection: {list}"
    );
}

/// configmap_volume.go:504 "should be immutable if `immutable` field is set".
#[tokio::test]
async fn conformance_configmap_immutable() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "configmap-immutable";
    framework_namespace(&api, ns).await;
    let name = "immutable";
    let collection = format!("/api/v1/namespaces/{ns}/configmaps");
    let one = format!("{collection}/{name}");

    // :506-509 create.
    let (s, mut cur) = send(
        &api,
        "POST",
        &collection,
        "application/json",
        Some(&new_config_map(name)),
    )
    .await;
    assert_eq!(s, 201, "Failed to create config map: {cur}");

    // :511-513 add data-4 and update; MUST succeed.
    cur["data"]["data-4"] = json!("value-4");
    let (s, next) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 200, "Failed to update config map: {next}");
    cur = next;

    // :515-519 mark immutable and update; MUST succeed.
    cur["immutable"] = json!(true);
    let (s, next) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 200, "Failed to mark config map as immutable: {next}");
    cur = next;

    // :521-526 data can't change now; the error MUST be Invalid.
    cur["data"]["data-5"] = json!("value-5");
    let (s, body) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 422, "expected 'invalid' as error, got instead: {body}");
    assert_eq!(body["reason"], "Invalid", "{body}");

    // :528-533 get; the object MUST still be immutable.
    let (s, mut cur) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "{cur}");
    assert_eq!(cur["immutable"], true, "can be switched to mutable: {cur}");

    // :535-540 immutable true -> false MUST be Invalid.
    cur["immutable"] = json!(false);
    let (s, body) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 422, "expected 'invalid' as error, got instead: {body}");
    assert_eq!(body["reason"], "Invalid", "{body}");

    // :542-547 metadata (labels) can still change.
    let (s, mut cur) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "{cur}");
    cur["metadata"]["labels"] = json!({ "label1": "value1" });
    let (s, body) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 200, "Failed to update config map labels: {body}");

    // :549-551 an immutable config map can be deleted.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "Failed to delete config map: {body}");
}

// ------------------------------------------------------------------- Secret

/// secrets.go:143 "should fail to create secret due to empty secret key".
#[tokio::test]
async fn conformance_secret_empty_key() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "secrets-empty-key";
    framework_namespace(&api, ns).await;

    // createEmptyKeySecretForTest (:334-347; the case body is :144-145): Data {"": "value-1\n"}; MUST fail.
    let (s, body) = send(
        &api,
        "POST",
        &format!("/api/v1/namespaces/{ns}/secrets"),
        "application/json",
        Some(&json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": { "name": "secret-emptykey-test-1" },
            "data": { "": "dmFsdWUtMQo=" },
        })),
    )
    .await;
    assert_eq!(s, 422, "created secret with empty key: {body}");
    assert_eq!(body["reason"], "Invalid", "{body}");
}

/// secrets.go:157 "should patch a secret".
#[tokio::test]
async fn conformance_secret_patch() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "secrets-patch";
    framework_namespace(&api, ns).await;
    let name = "test-secret-1";
    let collection = format!("/api/v1/namespaces/{ns}/secrets");
    let one = format!("{collection}/{name}");

    // :158-176 create Opaque, label testsecret-constant=true, key=value.
    let (s, created) = send(
        &api,
        "POST",
        &collection,
        "application/json",
        Some(&json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": { "name": name, "labels": { "testsecret-constant": "true" } },
            "data": { "key": "dmFsdWU=" }, // "value"
            "type": "Opaque",
        })),
    )
    .await;
    assert_eq!(s, 201, "failed to create secret: {created}");
    resource_version(&created);

    // :178-184 list in all namespaces by the constant label; non-empty and
    // MUST contain the created secret (:186-197).
    let constant_sel = "labelSelector=testsecret-constant%3Dtrue";
    let (s, list) = send(
        &api,
        "GET",
        &format!("/api/v1/secrets?{constant_sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to list secrets: {list}");
    assert!(!items(&list).is_empty(), "no secrets found: {list}");
    assert!(
        items(&list)
            .iter()
            .any(|v| v["metadata"]["name"] == name && v["metadata"]["namespace"] == ns),
        "unable to find secret {ns}/{name} by name: {list}"
    );

    // :199-210 patch: label testsecret=true and data.key = []byte(base64
    // ("value1")), which JSON-encodes as base64(base64("value1")).
    let (s, body) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({
            "metadata": { "labels": { "testsecret": "true" } },
            "data": { "key": "ZG1Gc2RXVXg=" },
        })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch secret: {body}");

    // :212-219 get; the RV MUST have grown and the data decode to value1
    // (server returns base64("dmFsdWUx"); client then base64-decodes to
    // "value1").
    let (s, secret) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to get secret: {secret}");
    assert!(
        resource_version(&created) < resource_version(&secret),
        "patched object should have a larger resource version"
    );
    assert_eq!(
        secret["data"]["key"], "ZG1Gc2RXVXg=",
        "found secret, but the data wasn't updated from the patch: {secret}"
    );

    // :221-225 DeleteCollection in the namespace by testsecret=true.
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{collection}?labelSelector=testsecret%3Dtrue"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to delete patched secret: {body}");

    // :227-243 list in all namespaces by the constant label; the secret MUST
    // be gone.
    let (s, list) = send(
        &api,
        "GET",
        &format!("/api/v1/secrets?{constant_sel}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to list secrets: {list}");
    assert!(
        !items(&list)
            .iter()
            .any(|v| v["metadata"]["name"] == name && v["metadata"]["namespace"] == ns),
        "secret {ns}/{name} was not deleted successfully: {list}"
    );
}

/// secrets_volume.go:386 "should be immutable if `immutable` field is set".
#[tokio::test]
async fn conformance_secret_immutable() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "secrets-immutable";
    framework_namespace(&api, ns).await;
    let name = "immutable";
    let collection = format!("/api/v1/namespaces/{ns}/secrets");
    let one = format!("{collection}/{name}");

    // :388-391 create.
    let (s, mut cur) = send(
        &api,
        "POST",
        &collection,
        "application/json",
        Some(&secret_for_test(name)),
    )
    .await;
    assert_eq!(s, 201, "Failed to create secret: {cur}");

    // :393-395 add data-4 ("value-4\n") and update; MUST succeed.
    cur["data"]["data-4"] = json!("dmFsdWUtNAo=");
    let (s, next) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 200, "Failed to update secret: {next}");
    cur = next;

    // :397-401 mark immutable and update; MUST succeed.
    cur["immutable"] = json!(true);
    let (s, next) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 200, "Failed to mark secret as immutable: {next}");
    cur = next;

    // :403-408 data can't change now ("value-5\n"); the error MUST be Invalid.
    cur["data"]["data-5"] = json!("dmFsdWUtNQo=");
    let (s, body) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 422, "expected 'invalid' as error, got instead: {body}");
    assert_eq!(body["reason"], "Invalid", "{body}");

    // :410-415 get; the secret MUST still be immutable.
    let (s, mut cur) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "{cur}");
    assert_eq!(cur["immutable"], true, "can be switched to mutable: {cur}");

    // :417-422 immutable true -> false MUST be Invalid.
    cur["immutable"] = json!(false);
    let (s, body) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 422, "expected 'invalid' as error, got instead: {body}");
    assert_eq!(body["reason"], "Invalid", "{body}");

    // :424-429 metadata (labels) can still change.
    let (s, mut cur) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "{cur}");
    cur["metadata"]["labels"] = json!({ "label1": "value1" });
    let (s, body) = send(&api, "PUT", &one, "application/json", Some(&cur)).await;
    assert_eq!(s, 200, "Failed to update secret labels: {body}");

    // :431-433 an immutable secret can be deleted.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "Failed to delete secret: {body}");
}
