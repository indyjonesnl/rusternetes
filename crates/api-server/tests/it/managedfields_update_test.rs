//! The `managedFields` entry of a write that is not an apply (#2701), through
//! the generic Store and endpoint handlers, for ConfigMap.
//!
//! Each test names the upstream case it mirrors:
//! `staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/`
//! `managedfieldsupdater_test.go`, `capmanagers_test.go`,
//! `fieldmanager_test.go`, and the handler call sites
//! `apiserver/pkg/endpoints/handlers/{create.go:199,update.go:160-167,
//! patch.go:372,:466}`.

use std::time::Duration;

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CMS: &str = "/api/v1/namespaces/default/configmaps";

fn cm(name: &str, data: Value) -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name}, "data": data})
}

/// The entry of `manager`, if any.
fn entry<'a>(obj: &'a Value, manager: &str) -> Option<&'a Value> {
    obj["metadata"]["managedFields"]
        .as_array()?
        .iter()
        .find(|e| e["manager"] == manager)
}

fn entries(obj: &Value) -> Vec<Value> {
    obj["metadata"]["managedFields"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn post_as(api: &TestApiServer, agent: &str, query: &str, body: &Value) -> Value {
    let (status, _, _, out) = api
        .send_with_headers(
            "POST",
            &format!("{CMS}{query}"),
            &[("content-type", "application/json"), ("user-agent", agent)],
            Some(serde_json::to_vec(body).unwrap()),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    out
}

async fn write_as(
    api: &TestApiServer,
    method: &str,
    name: &str,
    agent: &str,
    query: &str,
    content_type: &str,
    body: &Value,
) -> Value {
    let (status, _, _, out) = api
        .send_with_headers(
            method,
            &format!("{CMS}/{name}{query}"),
            &[("content-type", content_type), ("user-agent", agent)],
            Some(serde_json::to_vec(body).unwrap()),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    out
}

async fn get(api: &TestApiServer, name: &str) -> Value {
    let (status, out) = api.get(&format!("{CMS}/{name}")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    out
}

/// create.go:199 `UpdateNoErrors(liveObj, obj, managerOrUserAgent(...))`:
/// a create records an `Update` entry for the user agent's product name, at
/// the served version, owning what the body set (DefaultTrackOnCreate = 1).
#[tokio::test]
async fn create_records_an_update_entry_named_after_the_user_agent() {
    let api = TestApiServer::new();
    let out = post_as(
        &api,
        "kubectl-create/v1.35.0 (linux/amd64)",
        "",
        &cm("a", json!({"k": "v"})),
    )
    .await;

    let e = entry(&out, "kubectl-create").unwrap_or_else(|| panic!("no entry: {out}"));
    assert_eq!(e["operation"], "Update", "{e}");
    assert_eq!(e["apiVersion"], "v1", "{e}");
    assert_eq!(e["fieldsType"], "FieldsV1", "{e}");
    assert!(e["time"].is_string(), "{e}");
    // `f:data` is a granular map: it is a member itself (".") and so is each key.
    assert_eq!(
        e["fieldsV1"],
        json!({"f:data": {".": {}, "f:k": {}}}),
        "{e}"
    );
    assert_eq!(entries(&out).len(), 1);
}

/// `managerOrUserAgent` (create.go:259-264): `?fieldManager=` wins over the
/// user agent.
#[tokio::test]
async fn the_field_manager_parameter_wins_over_the_user_agent() {
    let api = TestApiServer::new();
    let out = post_as(
        &api,
        "curl/8",
        "?fieldManager=my-tool",
        &cm("a", json!({"k": "v"})),
    )
    .await;
    assert!(entry(&out, "my-tool").is_some(), "{out}");
    assert!(entry(&out, "curl").is_none(), "{out}");
}

/// `prefixFromUserAgent` (create.go:266-282): no user agent is the empty
/// prefix, which `buildManagerInfo` names `unknown` (buildmanagerinfo.go).
#[tokio::test]
async fn an_empty_manager_is_unknown() {
    let api = TestApiServer::new();
    let (status, out) = api.post(CMS, &cm("a", json!({"k": "v"}))).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(entry(&out, "unknown").is_some(), "{out}");
}

/// `TestManagedFieldsUpdateDoesModifyTime`: an update that changes fields
/// stamps the manager's entry, merged with its earlier fields.
#[tokio::test]
async fn an_update_that_changes_fields_stamps_the_entry() {
    let api = TestApiServer::new();
    let created = post_as(&api, "tool-a/1", "", &cm("a", json!({"k": "v"}))).await;
    let first = entry(&created, "tool-a").unwrap()["time"].clone();
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let mut body = get(&api, "a").await;
    body["data"]["k2"] = json!("v2");
    let out = write_as(&api, "PUT", "a", "tool-a/1", "", "application/json", &body).await;
    let e = entry(&out, "tool-a").unwrap();
    assert_ne!(e["time"], first, "time must move: {e}");
    assert_eq!(
        e["fieldsV1"],
        json!({"f:data": {".": {}, "f:k": {}, "f:k2": {}}}),
        "the entry is merged with the manager's earlier fields: {e}"
    );
    assert_eq!(entries(&out).len(), 1);
}

/// `TestManagedFieldsUpdateWithoutChangesDoesNotModifyTime`, and the store's
/// no-op rule: an update that changes nothing leaves managedFields -- and so
/// the resourceVersion -- alone.
#[tokio::test]
async fn a_no_op_update_changes_nothing() {
    let api = TestApiServer::new();
    post_as(&api, "tool-a/1", "", &cm("a", json!({"k": "v"}))).await;
    let before = get(&api, "a").await;
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let out = write_as(
        &api,
        "PUT",
        "a",
        "tool-a/1",
        "",
        "application/json",
        &before,
    )
    .await;
    assert_eq!(
        out["metadata"]["managedFields"],
        before["metadata"]["managedFields"]
    );
    assert_eq!(
        out["metadata"]["resourceVersion"],
        before["metadata"]["resourceVersion"]
    );

    // The same holds for a PUT that omits managedFields: they come from the
    // live object (decodeLiveOrNew, fieldmanager.go:85-91).
    let mut body = before.clone();
    body["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("managedFields");
    let out = write_as(&api, "PUT", "a", "tool-a/1", "", "application/json", &body).await;
    assert_eq!(
        out["metadata"]["managedFields"],
        before["metadata"]["managedFields"]
    );
    assert_eq!(
        out["metadata"]["resourceVersion"],
        before["metadata"]["resourceVersion"]
    );
}

/// `TestTakingOverManagedFieldsDuringUpdateDoesNotModifyPreviousManagerTime`:
/// taking a field over moves it to the updater and leaves the previous
/// manager's time alone.
#[tokio::test]
async fn taking_over_a_field_does_not_modify_the_previous_managers_time() {
    let api = TestApiServer::new();
    let created = post_as(&api, "tool-a/1", "", &cm("a", json!({"key_a": "value"}))).await;
    let a_time = entry(&created, "tool-a").unwrap()["time"].clone();
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let mut body = get(&api, "a").await;
    body["data"]["key_a"] = json!("new-value");
    body["data"]["key_b"] = json!("b");
    let out = write_as(&api, "PUT", "a", "tool-b/1", "", "application/json", &body).await;

    let a = entry(&out, "tool-a").expect("tool-a keeps an entry");
    let b = entry(&out, "tool-b").expect("tool-b has an entry");
    assert_eq!(a["time"], a_time, "{a}");
    assert_eq!(
        a["fieldsV1"],
        json!({"f:data": {}}),
        "key_a moved to tool-b: {a}"
    );
    assert_eq!(
        b["fieldsV1"],
        json!({"f:data": {"f:key_a": {}, "f:key_b": {}}}),
        "{b}"
    );
}

/// patch.go:372 / :466: a patch records the patcher, for each non-apply
/// patch type, and a field the patch removes leaves its owner.
#[tokio::test]
async fn every_non_apply_patch_type_records_the_patcher() {
    let api = TestApiServer::new();
    post_as(&api, "tool-a/1", "", &cm("a", json!({"k": "v"}))).await;

    let out = write_as(
        &api,
        "PATCH",
        "a",
        "p-merge/1",
        "",
        "application/merge-patch+json",
        &json!({"data": {"m": "1"}}),
    )
    .await;
    assert_eq!(
        entry(&out, "p-merge").unwrap()["fieldsV1"],
        json!({"f:data": {"f:m": {}}})
    );

    let out = write_as(
        &api,
        "PATCH",
        "a",
        "p-smp/1",
        "",
        "application/strategic-merge-patch+json",
        &json!({"data": {"s": "2"}}),
    )
    .await;
    assert_eq!(
        entry(&out, "p-smp").unwrap()["fieldsV1"],
        json!({"f:data": {"f:s": {}}})
    );

    let out = write_as(
        &api,
        "PATCH",
        "a",
        "p-json/1",
        "",
        "application/json-patch+json",
        &json!([{"op": "replace", "path": "/data/k", "value": "3"}]),
    )
    .await;
    assert_eq!(
        entry(&out, "p-json").unwrap()["fieldsV1"],
        json!({"f:data": {"f:k": {}}})
    );
    // tool-a created `k`; the JSON patch changed it, so it left tool-a.
    assert_eq!(
        entry(&out, "tool-a").unwrap()["fieldsV1"],
        json!({"f:data": {}})
    );

    // Removing a key removes it from the manager that owned it.
    let out = write_as(
        &api,
        "PATCH",
        "a",
        "p-merge/1",
        "",
        "application/merge-patch+json",
        &json!({"data": {"s": null}}),
    )
    .await;
    assert!(
        entry(&out, "p-smp").is_none(),
        "an emptied entry is deleted: {out}"
    );
}

/// `?fieldManager=` on a patch names the manager.
#[tokio::test]
async fn a_patch_honours_the_field_manager_parameter() {
    let api = TestApiServer::new();
    post_as(&api, "tool-a/1", "", &cm("a", json!({"k": "v"}))).await;
    let out = write_as(
        &api,
        "PATCH",
        "a",
        "curl/8",
        "?fieldManager=named",
        "application/merge-patch+json",
        &json!({"data": {"m": "1"}}),
    )
    .await;
    assert!(entry(&out, "named").is_some(), "{out}");
    assert!(entry(&out, "curl").is_none(), "{out}");
}

/// `TestCapUpdateManagers` / `TestCapManagersManagerMergesEntries`: more than
/// `DefaultMaxUpdateManagers` (10) Update entries merge the oldest into
/// `ancient-changes`.
#[tokio::test]
async fn more_than_ten_update_managers_are_capped() {
    let api = TestApiServer::new();
    post_as(&api, "m0/1", "", &cm("a", json!({"k0": "v"}))).await;
    for i in 1..=11 {
        write_as(
            &api,
            "PATCH",
            "a",
            &format!("m{i}/1"),
            "",
            "application/merge-patch+json",
            &json!({"data": {format!("k{i}"): "v"}}),
        )
        .await;
    }
    let out = get(&api, "a").await;
    let all = entries(&out);
    assert!(all.len() <= 10, "{} entries: {out}", all.len());
    let ancient = entry(&out, "ancient-changes").unwrap_or_else(|| panic!("no bucket: {out}"));
    assert_eq!(ancient["operation"], "Update");
    assert_eq!(ancient["apiVersion"], "v1");
    // The newest manager is still its own entry.
    assert!(entry(&out, "m11").is_some(), "{out}");
}

/// `isResetManagedFields` (fieldmanager.go:164-176): `managedFields: []`
/// clears the entries; an update with no entries is then not tracked
/// (`skipNonAppliedManager`).
#[tokio::test]
async fn an_empty_managed_fields_list_resets_them() {
    let api = TestApiServer::new();
    post_as(&api, "tool-a/1", "", &cm("a", json!({"k": "v"}))).await;
    let mut body = get(&api, "a").await;
    body["metadata"]["managedFields"] = json!([]);
    body["data"]["k2"] = json!("v2");
    let out = write_as(&api, "PUT", "a", "tool-a/1", "", "application/json", &body).await;
    assert!(entries(&out).is_empty(), "{out}");
    assert_eq!(out["data"]["k2"], "v2");
}
