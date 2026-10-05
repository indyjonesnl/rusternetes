//! APIService on the generic Store (#2140, epic #1990): the strategy rules of
//! `kube-aggregator/pkg/registry/apiservice/strategy.go`, the validation of
//! `pkg/apis/apiregistration/validation/validation.go` and the v1 defaults of
//! `pkg/apis/apiregistration/v1/defaults.go`.
//!
//! The handlers used to keep the document untyped (`serde_json::Value`), so
//! none of these rules ran: a POST with any name, any priority and a client
//! supplied status was stored as sent, and `/status` replaced the whole object.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const COLLECTION: &str = "/apis/apiregistration.k8s.io/v1/apiservices";
const NAME: &str = "v1alpha1.wardle.example.com";

fn apiservice(extra_spec: Value) -> Value {
    let mut spec = json!({
        "group": "wardle.example.com",
        "version": "v1alpha1",
        "groupPriorityMinimum": 2000,
        "versionPriority": 200,
    });
    for (k, v) in extra_spec.as_object().unwrap() {
        spec[k] = v.clone();
    }
    json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": {"name": NAME},
        "spec": spec,
    })
}

fn remote() -> Value {
    apiservice(json!({"service": {"namespace": "ns", "name": "svc"}}))
}

async fn create(api: &TestApiServer, body: &Value) -> Value {
    let (s, created) = api.post(COLLECTION, body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    created
}

/// `PrepareForCreate` (strategy.go:68-76): a client-supplied status is
/// dropped and a remote APIService starts with none.
#[tokio::test]
async fn a_remote_apiservice_starts_with_an_empty_status() {
    let api = TestApiServer::new();
    let mut body = remote();
    body["status"] = json!({"conditions": [{"type": "Available", "status": "True"}]});
    let created = create(&api, &body).await;
    assert!(
        created["status"]
            .get("conditions")
            .is_none_or(|c| c.as_array().is_some_and(|a| a.is_empty())),
        "{created}"
    );
}

/// `PrepareForCreate` (strategy.go:73-75): a local APIService is Available.
#[tokio::test]
async fn a_local_apiservice_is_available_on_create() {
    let api = TestApiServer::new();
    let created = create(&api, &apiservice(json!({}))).await;
    let c = &created["status"]["conditions"][0];
    assert_eq!(c["type"], "Available", "{created}");
    assert_eq!(c["status"], "True", "{created}");
    assert_eq!(c["reason"], "Local", "{created}");
    assert_eq!(
        c["message"], "Local APIServices are always available",
        "{created}"
    );
}

/// `SetDefaults_ServiceReference` (v1/defaults.go): the port is 443.
#[tokio::test]
async fn the_service_port_defaults_to_443() {
    let api = TestApiServer::new();
    let created = create(&api, &remote()).await;
    assert_eq!(created["spec"]["service"]["port"], 443, "{created}");
}

/// `ValidateAPIService` (validation.go:38-46): the name is `version.group`.
#[tokio::test]
async fn the_name_must_be_version_dot_group() {
    let api = TestApiServer::new();
    let mut body = remote();
    body["metadata"]["name"] = json!("wrong-name");
    let (s, resp) = api.post(COLLECTION, &body).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{resp}");
    assert!(
        resp["message"]
            .as_str()
            .unwrap()
            .contains("must be `spec.version+\".\"+spec.group`: \"v1alpha1.wardle.example.com\""),
        "{resp}"
    );
}

/// `ValidateAPIService` (validation.go:60-65).
#[tokio::test]
async fn the_priorities_are_bounded() {
    let api = TestApiServer::new();
    let (s, resp) = api
        .post(
            COLLECTION,
            &apiservice(json!({"groupPriorityMinimum": 0, "versionPriority": 1001})),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{resp}");
    let msg = resp["message"].as_str().unwrap();
    assert!(msg.contains("spec.groupPriorityMinimum"), "{resp}");
    assert!(msg.contains("spec.versionPriority"), "{resp}");
}

/// `ValidateAPIService` (validation.go:67-73): a local APIService carries no
/// TLS settings.
#[tokio::test]
async fn a_local_apiservice_may_not_skip_tls_verification() {
    let api = TestApiServer::new();
    let (s, resp) = api
        .post(
            COLLECTION,
            &apiservice(json!({"insecureSkipTLSVerify": true})),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{resp}");
    assert!(
        resp["message"]
            .as_str()
            .unwrap()
            .contains("local APIServices may not have insecureSkipTLSVerify"),
        "{resp}"
    );
}

/// `Store.Update` (store.go:646-650) with `AllowCreateOnUpdate() == false`
/// (strategy.go:94-96): a PUT to an absent APIService is a 404.
#[tokio::test]
async fn a_put_to_an_absent_apiservice_is_not_found() {
    let api = TestApiServer::new();
    let (s, resp) = api.put(&format!("{COLLECTION}/{NAME}"), &remote()).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{resp}");
    let (s, resp) = api
        .put(&format!("{COLLECTION}/{NAME}/status"), &remote())
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{resp}");
}

/// `PrepareForUpdate` (strategy.go:78-83): a PUT cannot write the status.
#[tokio::test]
async fn a_put_cannot_write_the_status() {
    let api = TestApiServer::new();
    let created = create(&api, &remote()).await;
    let mut body = created.clone();
    body["spec"]["groupPriorityMinimum"] = json!(3000);
    body["status"] = json!({"conditions": [{"type": "Available", "status": "True",
                                            "reason": "Forged"}]});
    let (s, updated) = api.put(&format!("{COLLECTION}/{NAME}"), &body).await;
    assert_eq!(s, StatusCode::OK, "{updated}");
    assert_eq!(updated["spec"]["groupPriorityMinimum"], 3000, "{updated}");
    assert!(
        updated["status"]
            .get("conditions")
            .is_none_or(|c| c.as_array().is_some_and(|a| a.is_empty())),
        "{updated}"
    );
}

/// `apiServerStatusStrategy.PrepareForUpdate` (strategy.go:143-149): a
/// `/status` PUT writes the status and nothing else.
#[tokio::test]
async fn a_status_put_writes_only_the_status() {
    let api = TestApiServer::new();
    let created = create(&api, &remote()).await;
    let mut body = created.clone();
    body["spec"]["groupPriorityMinimum"] = json!(3000);
    body["metadata"]["labels"] = json!({"smuggled": "yes"});
    body["status"] = json!({"conditions": [{"type": "Available", "status": "True",
                                            "reason": "Passed",
                                            "message": "all checks passed"}]});
    let (s, updated) = api.put(&format!("{COLLECTION}/{NAME}/status"), &body).await;
    assert_eq!(s, StatusCode::OK, "{updated}");
    assert_eq!(updated["status"]["conditions"][0]["reason"], "Passed");
    assert_eq!(updated["spec"]["groupPriorityMinimum"], 2000, "{updated}");
    assert!(updated["metadata"].get("labels").is_none(), "{updated}");
}

/// `ValidateAPIServiceStatus` (validation.go:102-116).
#[tokio::test]
async fn a_condition_status_must_be_true_false_or_unknown() {
    let api = TestApiServer::new();
    let created = create(&api, &remote()).await;
    let mut body = created.clone();
    body["status"] = json!({"conditions": [{"type": "Available", "status": "Maybe"}]});
    let (s, resp) = api.put(&format!("{COLLECTION}/{NAME}/status"), &body).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{resp}");
    assert!(
        resp["message"]
            .as_str()
            .unwrap()
            .contains("status.conditions[0].status"),
        "{resp}"
    );
}

/// PATCH `/status` goes through the status strategy: the patch lands in the
/// status and a spec change in the same patch is discarded.
#[tokio::test]
async fn a_status_patch_writes_only_the_status() {
    let api = TestApiServer::new();
    create(&api, &remote()).await;
    let (s, patched) = api
        .patch(
            &format!("{COLLECTION}/{NAME}/status"),
            &json!({
                "spec": {"versionPriority": 900},
                "status": {"conditions": [{"type": "Available", "status": "False",
                                           "reason": "MissingEndpoints"}]},
            }),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{patched}");
    assert_eq!(patched["status"]["conditions"][0]["status"], "False");
    assert_eq!(patched["spec"]["versionPriority"], 200, "{patched}");

    let (s, got) = api.get(&format!("{COLLECTION}/{NAME}/status")).await;
    assert_eq!(s, StatusCode::OK, "{got}");
    assert_eq!(got["status"]["conditions"][0]["reason"], "MissingEndpoints");
}

/// `Store.Update` (store.go:565): a PUT that drains the last finalizer off an
/// object pending deletion removes it in the same request.
#[tokio::test]
async fn a_put_draining_the_last_finalizer_deletes_the_apiservice() {
    let api = TestApiServer::new();
    let mut body = remote();
    body["metadata"]["finalizers"] = json!(["example.com/hold"]);
    create(&api, &body).await;

    let (s, held) = api.delete(&format!("{COLLECTION}/{NAME}")).await;
    assert_eq!(s, StatusCode::OK, "{held}");
    assert!(
        held["metadata"].get("deletionTimestamp").is_some(),
        "{held}"
    );

    let mut drained = held.clone();
    drained["metadata"]["finalizers"] = json!([]);
    let (s, resp) = api.put(&format!("{COLLECTION}/{NAME}"), &drained).await;
    assert_eq!(s, StatusCode::OK, "{resp}");
    let (s, _) = api.get(&format!("{COLLECTION}/{NAME}")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// The list serves `?labelSelector=`, and DELETE on the collection honours it
/// (`Store.DeleteCollection`).
#[tokio::test]
async fn list_and_deletecollection_honour_the_label_selector() {
    let api = TestApiServer::new();
    let mut a = remote();
    a["metadata"]["labels"] = json!({"e2e": "yes"});
    create(&api, &a).await;
    let mut b = remote();
    b["metadata"]["name"] = json!("v1beta1.wardle.example.com");
    b["spec"]["version"] = json!("v1beta1");
    create(&api, &b).await;

    let (s, list) = api
        .get(&format!("{COLLECTION}?labelSelector=e2e%3Dyes"))
        .await;
    assert_eq!(s, StatusCode::OK, "{list}");
    assert_eq!(list["kind"], "APIServiceList");
    assert_eq!(list["items"].as_array().unwrap().len(), 1, "{list}");

    let (s, resp) = api
        .delete(&format!("{COLLECTION}?labelSelector=e2e%3Dyes"))
        .await;
    assert_eq!(s, StatusCode::OK, "{resp}");
    let (_, list) = api.get(COLLECTION).await;
    let names: Vec<&str> = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["v1beta1.wardle.example.com"], "{list}");
}
