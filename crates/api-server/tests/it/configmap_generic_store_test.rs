//! ConfigMap served through the generic Store and endpoint handlers (#1990).
//!
//! Each test pins one upstream rule the bespoke handler did not have, with
//! the upstream source it comes from. Together they are the behavioural
//! contract of `endpoints::handlers` + `registry::generic::Store` for a
//! plain namespaced resource.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CMS: &str = "/api/v1/namespaces/default/configmaps";

fn cm(name: &str) -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name}, "data": {"k": "v"}})
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(CMS, &cm(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `createHandler` wipes client-set system fields before admission
/// (create.go:170-172, rest/meta.go:30-36) and the Store fills them
/// (store.go:449-451): a client cannot choose its uid.
#[tokio::test]
async fn create_replaces_a_client_supplied_uid() {
    let api = TestApiServer::new();
    let mut body = cm("cm-uid");
    body["metadata"]["uid"] = json!("chosen-by-client");
    let (status, out) = api.post(CMS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let uid = out["metadata"]["uid"].as_str().unwrap_or_default();
    assert!(!uid.is_empty() && uid != "chosen-by-client", "{out}");
}

/// A body naming another API version is rejected (create.go:143-148).
#[tokio::test]
async fn create_rejects_a_foreign_api_version() {
    let api = TestApiServer::new();
    let mut body = cm("cm-av");
    body["apiVersion"] = json!("v2");
    let (status, out) = api.post(CMS, &body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert_eq!(
        message(&out),
        "the API version in the data (v2) does not match the expected API version (v1)"
    );
}

/// `ValidateConfigMap` now includes `ValidateObjectMeta` with
/// `NameIsDNSSubdomain` (validation.go:7758-7760).
#[tokio::test]
async fn create_validates_the_name_as_a_dns_subdomain() {
    let api = TestApiServer::new();
    let (status, out) = api.post(CMS, &cm("Not_A_Subdomain")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("metadata.name"), "{out}");
}

/// `dryRun=All` validates without writing; any other value is rejected by
/// `ValidateCreateOptions` (validation.go:174-180).
#[tokio::test]
async fn create_dry_run_writes_nothing_and_rejects_unknown_values() {
    let api = TestApiServer::new();
    let (status, out) = api.post(&format!("{CMS}?dryRun=All"), &cm("cm-dry")).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let (status, _) = api.get(&format!("{CMS}/cm-dry")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, out) = api
        .post(&format!("{CMS}?dryRun=Sometimes"), &cm("cm-dry"))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// Upstream's NotFound wording names the qualified resource, not the storage
/// key (store.go `Get` → `storeerr.InterpretGetError`).
#[tokio::test]
async fn get_of_a_missing_object_is_upstream_not_found() {
    let api = TestApiServer::new();
    let (status, out) = api.get(&format!("{CMS}/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
    assert_eq!(message(&out), r#"configmaps "nope" not found"#);
}

/// `checkName` (rest.go:272-290): the body may not name another object.
#[tokio::test]
async fn put_with_a_different_name_is_a_bad_request() {
    let api = TestApiServer::new();
    create(&api, "cm-a").await;
    let (status, out) = api.put(&format!("{CMS}/cm-a"), &cm("cm-b")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert_eq!(
        message(&out),
        "the name of the object (cm-b) does not match the name on the URL (cm-a)"
    );
}

/// A PUT of a missing object is NotFound: ConfigMap's
/// `AllowCreateOnUpdate` is false (strategy.go:73-75, store.go:652-654).
#[tokio::test]
async fn put_of_a_missing_object_is_not_found() {
    let api = TestApiServer::new();
    let (status, out) = api.put(&format!("{CMS}/cm-none"), &cm("cm-none")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
    let (status, _) = api.get(&format!("{CMS}/cm-none")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// A stale resourceVersion conflicts with `OptimisticLockErrorMsg`
/// (store.go:733-738); none at all is an unconditional update because
/// `AllowUnconditionalUpdate` is true (strategy.go:90-92).
#[tokio::test]
async fn put_resource_version_semantics() {
    let api = TestApiServer::new();
    let created = create(&api, "cm-rv").await;

    let mut stale = created.clone();
    stale["metadata"]["resourceVersion"] = json!("1");
    stale["data"]["k"] = json!("stale");
    let (status, out) = api.put(&format!("{CMS}/cm-rv"), &stale).await;
    if created["metadata"]["resourceVersion"] != json!("1") {
        assert_eq!(status, StatusCode::CONFLICT, "{out}");
        assert!(
            message(&out).contains(
                "the object has been modified; please apply your changes to the latest version and try again"
            ),
            "{out}"
        );
    }

    let (status, out) = api
        .put(
            &format!("{CMS}/cm-rv"),
            &json!({"metadata": {"name": "cm-rv"}, "data": {"k": "unconditional"}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["data"]["k"], "unconditional");
    // BeforeUpdate carried the server-owned metadata over (update.go:131-146).
    assert_eq!(out["metadata"]["uid"], created["metadata"]["uid"], "{out}");
}

/// The supplied uid is a precondition (update.go:188-203): a PUT naming
/// another uid conflicts rather than overwriting.
#[tokio::test]
async fn put_with_a_foreign_uid_conflicts() {
    let api = TestApiServer::new();
    create(&api, "cm-pre").await;
    let mut body = cm("cm-pre");
    body["metadata"]["uid"] = json!("someone-else");
    let (status, out) = api.put(&format!("{CMS}/cm-pre"), &body).await;
    assert_eq!(status, StatusCode::CONFLICT, "{out}");
    assert!(
        message(&out).contains("Precondition failed: UID in precondition: someone-else"),
        "{out}"
    );
}

/// A JSON / merge / strategic patch has nothing to apply to a missing object
/// (`createNewObject`, patch.go:376-378, 470-472).
#[tokio::test]
async fn merge_patch_of_a_missing_object_is_not_found() {
    let api = TestApiServer::new();
    let (status, out) = api
        .patch(&format!("{CMS}/cm-none"), &json!({"data": {"k": "v"}}))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
    assert_eq!(message(&out), r#"configmaps "cm-none" not found"#);
}

/// Every patch type upstream serves works through the one Store.Update path.
#[tokio::test]
async fn json_merge_and_strategic_patches_apply() {
    let api = TestApiServer::new();
    create(&api, "cm-p").await;
    let uri = format!("{CMS}/cm-p");

    let (status, out) = api.patch(&uri, &json!({"data": {"m": "1"}})).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["data"]["m"], "1");

    let (status, out) = api
        .send(
            "PATCH",
            &uri,
            Some("application/strategic-merge-patch+json"),
            Some(&json!({"data": {"s": "2"}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["data"]["s"], "2");

    let (status, out) = api
        .send(
            "PATCH",
            &uri,
            Some("application/json-patch+json"),
            Some(&json!([{"op": "replace", "path": "/data/k", "value": "3"}])),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["data"]["k"], "3");
}

/// A patch may not rename the object (`checkName` in `applyPatch`,
/// patch.go:617-619).
#[tokio::test]
async fn patch_renaming_the_object_is_a_bad_request() {
    let api = TestApiServer::new();
    create(&api, "cm-r").await;
    let (status, out) = api
        .patch(
            &format!("{CMS}/cm-r"),
            &json!({"metadata": {"name": "other"}}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
}

/// `force` is only for apply (`ValidatePatchOptions`, validation.go:196-202).
#[tokio::test]
async fn force_on_a_merge_patch_is_invalid() {
    let api = TestApiServer::new();
    create(&api, "cm-f").await;
    let (status, out) = api
        .patch(
            &format!("{CMS}/cm-f?force=true"),
            &json!({"data": {"k": "x"}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// Apply creates (`forceAllowCreate`, patch.go:672-674) even though
/// ConfigMap's strategy refuses create-on-update.
#[tokio::test]
async fn apply_creates_a_missing_object() {
    let api = TestApiServer::new();
    let (status, out) = api
        .send(
            "PATCH",
            &format!("{CMS}/cm-apply?fieldManager=test"),
            Some("application/apply-patch+yaml"),
            Some(&cm("cm-apply")),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(!out["metadata"]["uid"]
        .as_str()
        .unwrap_or_default()
        .is_empty());
}

/// An apply naming a uid when nothing exists is a conflict, not a create
/// (patch.go:600-607).
#[tokio::test]
async fn apply_with_a_uid_and_no_object_conflicts() {
    let api = TestApiServer::new();
    let mut body = cm("cm-ghost");
    body["metadata"]["uid"] = json!("ghost-uid");
    let (status, out) = api
        .send(
            "PATCH",
            &format!("{CMS}/cm-ghost?fieldManager=test"),
            Some("application/apply-patch+yaml"),
            Some(&body),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{out}");
    assert!(
        message(&out).contains(
            "uid mismatch: the provided object specified uid ghost-uid, and no existing object was found"
        ),
        "{out}"
    );
}

/// DELETE answers with the success Status `finalizeDelete` builds, whose
/// `details.kind` is the resource name (store.go:1386-1411).
#[tokio::test]
async fn delete_returns_the_success_status() {
    let api = TestApiServer::new();
    let created = create(&api, "cm-del").await;
    let (status, out) = api.delete(&format!("{CMS}/cm-del")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Status", "{out}");
    assert_eq!(out["status"], "Success", "{out}");
    assert_eq!(out["details"]["name"], "cm-del");
    assert_eq!(out["details"]["kind"], "configmaps");
    assert_eq!(out["details"]["uid"], created["metadata"]["uid"]);
    let (status, _) = api.get(&format!("{CMS}/cm-del")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// DeleteOptions come from the query when the body is empty
/// (delete.go:121-126): a dry run leaves the object in place.
#[tokio::test]
async fn delete_dry_run_from_the_query_keeps_the_object() {
    let api = TestApiServer::new();
    create(&api, "cm-dd").await;
    let (status, out) = api.delete(&format!("{CMS}/cm-dd?dryRun=All")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (status, _) = api.get(&format!("{CMS}/cm-dd")).await;
    assert_eq!(status, StatusCode::OK);
}

/// DELETECOLLECTION deletes what the selector matches and returns the list
/// (store.go:1237-1384).
#[tokio::test]
async fn deletecollection_honours_the_selector_and_returns_the_list() {
    let api = TestApiServer::new();
    for (name, team) in [("cm-x", "a"), ("cm-y", "a"), ("cm-z", "b")] {
        let mut body = cm(name);
        body["metadata"]["labels"] = json!({"team": team});
        let (status, out) = api.post(CMS, &body).await;
        assert_eq!(status, StatusCode::CREATED, "{out}");
    }
    let (status, out) = api.delete(&format!("{CMS}?labelSelector=team%3Da")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "ConfigMapList", "{out}");
    let mut names: Vec<&str> = out["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["cm-x", "cm-y"]);

    assert_eq!(
        api.get(&format!("{CMS}/cm-x")).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(api.get(&format!("{CMS}/cm-z")).await.0, StatusCode::OK);
}
