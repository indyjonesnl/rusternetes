//! Custom resources served through the generic Store (#1990).
//!
//! Each test pins one upstream rule the bespoke handler did not have, with the
//! source it comes from. Together they are the behavioural contract of
//! `registry::apiextensions::customresource` over `registry::generic::Store`:
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/registry/customresource/{strategy.go,status_strategy.go,validator.go}`
//! wired in by `pkg/apiserver/customresource_handler.go`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const GROUP: &str = "stable.example.com";
const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

fn crd(plural: &str, kind: &str, scope: &str, status: bool, schema: Value) -> Value {
    let mut version = json!({
        "name": "v1", "served": true, "storage": true,
        "schema": { "openAPIV3Schema": schema }
    });
    if status {
        version["subresources"] = json!({ "status": {} });
    }
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{plural}.{GROUP}") },
        "spec": {
            "group": GROUP,
            "scope": scope,
            "names": { "plural": plural, "kind": kind, "listKind": format!("{kind}List") },
            "versions": [version]
        }
    })
}

fn open_schema() -> Value {
    json!({ "type": "object", "x-kubernetes-preserve-unknown-fields": true })
}

fn typed_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "size": { "type": "integer", "minimum": 0 },
                    "color": { "type": "string", "default": "red" }
                }
            },
            "status": {
                "type": "object",
                "properties": { "ready": { "type": "boolean" } }
            }
        }
    })
}

async fn install(api: &TestApiServer, crd: &Value) {
    let (status, out) = api.post(CRDS, crd).await;
    assert_eq!(status, StatusCode::CREATED, "install CRD: {out}");
}

fn ns_path(plural: &str) -> String {
    format!("/apis/{GROUP}/v1/namespaces/default/{plural}")
}

fn widget(name: &str) -> Value {
    json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": { "name": name },
        "spec": { "size": 1 }
    })
}

async fn create(api: &TestApiServer, path: &str, body: &Value) -> Value {
    let (status, out) = api.post(path, body).await;
    assert_eq!(status, StatusCode::CREATED, "create: {out}");
    out
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// #2131: `BeforeCreate` stores `metadata.namespace` (rest/create.go:107-117),
/// so a namespaced custom resource always carries it.
#[tokio::test]
async fn create_stores_the_namespace_and_starts_the_generation_at_one() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let out = create(&api, &ns_path("widgets"), &widget("w1")).await;
    assert_eq!(out["metadata"]["namespace"], "default", "{out}");
    // strategy.go:120-135 PrepareForCreate: `accessor.SetGeneration(1)`.
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    let (_, got) = api.get(&format!("{}/w1", ns_path("widgets"))).await;
    assert_eq!(got["metadata"]["namespace"], "default", "{got}");
}

/// strategy.go:138-174 PrepareForUpdate: "except for the changes to
/// `metadata`, any other changes cause the generation to increment."
#[tokio::test]
async fn generation_moves_only_with_a_change_outside_metadata() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let path = format!("{}/w1", ns_path("widgets"));
    let live = create(&api, &ns_path("widgets"), &widget("w1")).await;

    let mut relabelled = live.clone();
    relabelled["metadata"]["labels"] = json!({ "a": "b" });
    let (status, out) = api.put(&path, &relabelled).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["metadata"]["generation"], 1,
        "metadata-only change: {out}"
    );

    let mut resized = out.clone();
    resized["spec"]["size"] = json!(2);
    let (status, out) = api.put(&path, &resized).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "spec change: {out}");
}

/// strategy.go:282-285: `AllowUnconditionalUpdate` is false, so a PUT must
/// carry the resourceVersion it read (validation.ValidateObjectMetaUpdate).
#[tokio::test]
async fn put_without_a_resource_version_is_refused() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    create(&api, &ns_path("widgets"), &widget("w1")).await;
    let (status, out) = api
        .put(&format!("{}/w1", ns_path("widgets")), &widget("w1"))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains(
            "metadata.resourceVersion: Invalid value: 0: must be specified for an update"
        ),
        "{out}"
    );
}

/// strategy.go:120-135: "create cannot set status" once `/status` is a
/// subresource; and :138-160 an update cannot either.
#[tokio::test]
async fn status_is_only_writable_through_the_status_subresource() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", true, typed_schema()),
    )
    .await;
    let mut body = widget("w1");
    body["status"] = json!({ "ready": true });
    let live = create(&api, &ns_path("widgets"), &body).await;
    assert!(
        live.get("status").is_none_or(Value::is_null),
        "create kept status: {live}"
    );

    let path = format!("{}/w1", ns_path("widgets"));
    let mut with_status = live.clone();
    with_status["status"] = json!({ "ready": true });
    let (status, out) = api.put(&path, &with_status).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(
        out.get("status").is_none_or(Value::is_null),
        "update kept status: {out}"
    );

    // status_strategy.go:62-86: the status write takes the new status, and
    // nothing else from the body.
    let mut status_write = out.clone();
    status_write["status"] = json!({ "ready": true });
    status_write["spec"]["size"] = json!(99);
    status_write["metadata"]["labels"] = json!({ "ignored": "yes" });
    let (status, out) = api.put(&format!("{path}/status"), &status_write).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["ready"], true, "{out}");
    assert_eq!(out["spec"]["size"], 1, "spec must not change: {out}");
    assert!(
        out["metadata"]["labels"].is_null(),
        "metadata must not change: {out}"
    );
    // A status write is not a spec change.
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
}

/// etcd.go:195-198: `StatusREST.Get` is the store's `Get` -- the whole
/// object, not the bare status.
#[tokio::test]
async fn get_status_returns_the_whole_object() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", true, typed_schema()),
    )
    .await;
    create(&api, &ns_path("widgets"), &widget("w1")).await;
    let (status, out) = api.get(&format!("{}/w1/status", ns_path("widgets"))).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Widget", "{out}");
    assert_eq!(out["metadata"]["name"], "w1", "{out}");
}

/// A PATCH of `/status` applies to the whole object and keeps only the
/// status (status_strategy.go:62-86).
#[tokio::test]
async fn patch_status_changes_only_the_status() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", true, typed_schema()),
    )
    .await;
    create(&api, &ns_path("widgets"), &widget("w1")).await;
    let (status, out) = api
        .patch(
            &format!("{}/w1/status", ns_path("widgets")),
            &json!({ "status": { "ready": true }, "spec": { "size": 50 } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["ready"], true, "{out}");
    assert_eq!(out["spec"]["size"], 1, "{out}");
}

/// customresource_handler.go: a CRD without the `status` subresource serves no
/// `/status` -- the route is not registered, so it is a 404.
#[tokio::test]
async fn status_subresource_not_enabled_is_not_found() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    create(&api, &ns_path("widgets"), &widget("w1")).await;
    let (status, out) = api
        .put(&format!("{}/w1/status", ns_path("widgets")), &widget("w1"))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// validator.go:120-132 `ValidateTypeMeta`: the body's kind must be the CRD's.
#[tokio::test]
async fn create_with_another_kind_is_invalid() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let mut body = widget("w1");
    body["kind"] = json!("Gadget");
    let (status, out) = api.post(&ns_path("widgets"), &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("kind: Invalid value: \"Gadget\": must be Widget"),
        "{out}"
    );
}

/// create.go:116-148 / rest.go:`checkName`: the body's apiVersion is the
/// request's.
#[tokio::test]
async fn create_with_another_api_version_is_a_bad_request() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let mut body = widget("w1");
    body["apiVersion"] = json!(format!("{GROUP}/v2"));
    let (status, out) = api.post(&ns_path("widgets"), &body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
}

/// validator.go:44-50: the name is validated with `NameIsDNSSubdomain`.
#[tokio::test]
async fn create_with_a_name_that_is_not_a_dns_subdomain_is_invalid() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let (status, out) = api.post(&ns_path("widgets"), &widget("Bad_Name")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("metadata.name"), "{out}");
}

/// A version the CRD does not serve has no route: 404, not a validation error.
#[tokio::test]
async fn a_version_the_crd_does_not_serve_is_not_found() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let (status, _) = api
        .get(&format!("/apis/{GROUP}/v9/namespaces/default/widgets/w1"))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// Decode-time defaulting and pruning (customresource_handler.go:1195-1250,
/// 1406-1470): defaults are filled in, unknown fields are dropped.
#[tokio::test]
async fn create_defaults_and_prunes_against_the_structural_schema() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, typed_schema()),
    )
    .await;
    let mut body = widget("w1");
    body["spec"]["unknown"] = json!("dropped");
    body["extra"] = json!("dropped");
    let out = create(&api, &ns_path("widgets"), &body).await;
    assert_eq!(out["spec"]["color"], "red", "{out}");
    assert!(out["spec"].get("unknown").is_none(), "{out}");
    assert!(out.get("extra").is_none(), "{out}");
}

/// Schema validation reaches the client as field errors.
#[tokio::test]
async fn schema_violations_are_reported_as_field_errors() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, typed_schema()),
    )
    .await;
    let mut body = widget("w1");
    body["spec"]["size"] = json!(-1);
    let (status, out) = api.post(&ns_path("widgets"), &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.size"), "{out}");
}

/// strategy.go:204-228 `generateWarningsFromObj`: a finalizer that is not
/// domain-qualified draws a warning.
#[tokio::test]
async fn a_bare_finalizer_name_draws_a_warning() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let mut body = widget("w1");
    body["metadata"]["finalizers"] = json!(["cleanup"]);
    let (status, headers, bytes, _) = api
        .send_full(
            "POST",
            &ns_path("widgets"),
            Some("application/json"),
            None,
            Some(serde_json::to_vec(&body).unwrap()),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let warning = headers
        .get_all("warning")
        .iter()
        .map(|v| v.to_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        warning.contains("prefer a domain-qualified finalizer name"),
        "warning header: {warning:?}"
    );
}

/// Cluster-scoped custom resources are keyed without a namespace and carry
/// none.
#[tokio::test]
async fn cluster_scoped_custom_resources_round_trip() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("gizmos", "Gizmo", "Cluster", false, open_schema()),
    )
    .await;
    let mut body = widget("g1");
    body["kind"] = json!("Gizmo");
    let base = format!("/apis/{GROUP}/v1/gizmos");
    let out = create(&api, &base, &body).await;
    assert!(
        out["metadata"].get("namespace").is_none_or(Value::is_null),
        "{out}"
    );
    let (status, got) = api.get(&format!("{base}/g1")).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    let (status, out) = api.delete(&format!("{base}/g1")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (status, _) = api.get(&format!("{base}/g1")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// store.go:1131-1220 `Store.Delete`: an object with finalizers stays, with a
/// `deletionTimestamp`; one without goes, answered by a Status.
#[tokio::test]
async fn delete_honours_finalizers_and_answers_with_a_status() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let mut held = widget("held");
    held["metadata"]["finalizers"] = json!(["example.com/keep"]);
    create(&api, &ns_path("widgets"), &held).await;
    create(&api, &ns_path("widgets"), &widget("plain")).await;

    let (status, out) = api.delete(&format!("{}/held", ns_path("widgets"))).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(out["metadata"]["deletionTimestamp"].is_string(), "{out}");

    let (status, out) = api.delete(&format!("{}/plain", ns_path("widgets"))).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Status", "{out}");
    let (status, _) = api.get(&format!("{}/plain", ns_path("widgets"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// delete.go:199-340 `DeleteCollection`: every object the selectors match is
/// deleted and listed in the answer.
#[tokio::test]
async fn deletecollection_deletes_what_the_selector_matches() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    for (name, color) in [("a", "blue"), ("b", "blue"), ("c", "green")] {
        let mut w = widget(name);
        w["metadata"]["labels"] = json!({ "color": color });
        create(&api, &ns_path("widgets"), &w).await;
    }
    let (status, out) = api
        .delete(&format!("{}?labelSelector=color=blue", ns_path("widgets")))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["items"].as_array().map(Vec::len), Some(2), "{out}");
    let (_, list) = api.get(&ns_path("widgets")).await;
    assert_eq!(list["items"].as_array().map(Vec::len), Some(1), "{list}");
}

/// patch.go: a merge patch is applied to the stored object and validated like
/// a PUT.
#[tokio::test]
async fn merge_patch_updates_and_revalidates() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, typed_schema()),
    )
    .await;
    create(&api, &ns_path("widgets"), &widget("w1")).await;
    let path = format!("{}/w1", ns_path("widgets"));
    let (status, out) = api.patch(&path, &json!({ "spec": { "size": 7 } })).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["size"], 7, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");

    let (status, out) = api.patch(&path, &json!({ "spec": { "size": -3 } })).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// Server-side apply creates a custom resource that does not exist
/// (patch.go:500-543 `createNewObject`) and then updates it.
#[tokio::test]
async fn server_side_apply_creates_then_updates() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let path = format!("{}/w1?fieldManager=test", ns_path("widgets"));
    let apply = |size: i64| {
        serde_json::to_vec(&json!({
            "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
            "metadata": { "name": "w1" },
            "spec": { "size": size }
        }))
        .unwrap()
    };
    let (status, _, bytes, body) = api
        .send_with_headers(
            "PATCH",
            &path,
            &[("content-type", "application/apply-patch+yaml")],
            Some(apply(1)),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(body["metadata"]["namespace"], "default", "{body}");
    let (status, _, bytes, body) = api
        .send_with_headers(
            "PATCH",
            &path,
            &[("content-type", "application/apply-patch+yaml")],
            Some(apply(2)),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(body["spec"]["size"], 2, "{body}");
}

/// `fieldValidation=Strict` still refuses a field the schema does not declare
/// (customresource_handler.go:1406-1470 with `returnUnknownFieldPaths`).
#[tokio::test]
async fn strict_field_validation_refuses_unknown_fields() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, typed_schema()),
    )
    .await;
    let mut body = widget("w1");
    body["extra"] = json!("nope");
    let (status, out) = api
        .post(
            &format!("{}?fieldValidation=Strict", ns_path("widgets")),
            &body,
        )
        .await;
    assert!(status.is_client_error(), "{status} {out}");
    assert!(message(&out).contains("extra"), "{out}");
}

/// Objects the bespoke handlers stored carry no `metadata.namespace` and no
/// generation. Reading and updating them must still work -- upstream repairs
/// the generation of "very old CRs" on decode (customresource_handler.go:1465).
#[tokio::test]
async fn objects_stored_by_the_old_handler_still_update() {
    use rusternetes_storage::Storage;
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let legacy = json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": { "name": "old", "uid": "u-1", "creationTimestamp": "2024-01-01T00:00:00Z" },
        "spec": { "size": 1 }
    });
    api.storage
        .create("/registry/stable_example_com_widgets/default/old", &legacy)
        .await
        .unwrap();
    let path = format!("{}/old", ns_path("widgets"));
    let (status, mut got) = api.get(&path).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["metadata"]["namespace"], "default", "{got}");
    got["spec"]["size"] = json!(2);
    let (status, out) = api.put(&path, &got).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["size"], 2, "{out}");
}

fn scalable_crd(plural: &str) -> Value {
    let mut c = crd(plural, "Widget", "Namespaced", true, open_schema());
    c["spec"]["versions"][0]["subresources"]["scale"] = json!({
        "specReplicasPath": ".spec.replicas",
        "statusReplicasPath": ".status.replicas",
        "labelSelectorPath": ".status.selector"
    });
    c
}

fn scaled_widget(name: &str) -> Value {
    json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": { "name": name, "labels": { "keep": "me" } },
        "spec": { "replicas": 2 }
    })
}

/// #2134: `ScaleREST.Get` serves the CR as an `autoscaling/v1` `Scale`
/// (etcd.go:157-177, `scaleFromCustomResource` etcd.go:262-309).
#[tokio::test]
async fn get_scale_serves_an_autoscaling_v1_scale() {
    let api = TestApiServer::new();
    install(&api, &scalable_crd("widgets")).await;
    create(&api, &ns_path("widgets"), &scaled_widget("w1")).await;
    let path = format!("{}/w1", ns_path("widgets"));
    let (status, out) = api
        .patch(
            &format!("{path}/status"),
            &json!({ "status": { "replicas": 5, "selector": "app=w" } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");

    let (status, scale) = api.get(&format!("{path}/scale")).await;
    assert_eq!(status, StatusCode::OK, "{scale}");
    assert_eq!(scale["apiVersion"], "autoscaling/v1", "{scale}");
    assert_eq!(scale["kind"], "Scale", "{scale}");
    assert_eq!(scale["metadata"]["name"], "w1", "{scale}");
    assert_eq!(scale["metadata"]["namespace"], "default", "{scale}");
    // scaleFromCustomResource copies name, namespace, uid, resourceVersion and
    // creationTimestamp only.
    assert!(scale["metadata"].get("labels").is_none(), "{scale}");
    assert_eq!(scale["spec"]["replicas"], 2, "{scale}");
    assert_eq!(scale["status"]["replicas"], 5, "{scale}");
    assert_eq!(scale["status"]["selector"], "app=w", "{scale}");
}

/// etcd.go:170-172: a CR without the spec replicas field is an internal error.
#[tokio::test]
async fn get_scale_without_the_spec_replicas_field_is_an_internal_error() {
    let api = TestApiServer::new();
    install(&api, &scalable_crd("widgets")).await;
    create(&api, &ns_path("widgets"), &widget("w1")).await;
    let (status, out) = api.get(&format!("{}/w1/scale", ns_path("widgets"))).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{out}");
    assert!(
        message(&out).contains("the spec replicas field \".spec.replicas\" does not exist"),
        "{out}"
    );
}

/// `ScaleREST.Update` runs the main update strategy through `Store.Update`
/// (etcd.go:179-216), so a scale is a spec change: the generation moves, the
/// rest of the object is untouched, and the answer is a `Scale`.
#[tokio::test]
async fn put_scale_updates_the_replicas_through_the_store() {
    let api = TestApiServer::new();
    install(&api, &scalable_crd("widgets")).await;
    create(&api, &ns_path("widgets"), &scaled_widget("w1")).await;
    let path = format!("{}/w1", ns_path("widgets"));
    let (_, mut scale) = api.get(&format!("{path}/scale")).await;
    scale["spec"]["replicas"] = json!(7);
    let (status, out) = api.put(&format!("{path}/scale"), &scale).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Scale", "{out}");
    assert_eq!(out["spec"]["replicas"], 7, "{out}");

    let (_, cr) = api.get(&path).await;
    assert_eq!(cr["spec"]["replicas"], 7, "{cr}");
    assert_eq!(cr["metadata"]["labels"]["keep"], "me", "{cr}");
    assert_eq!(cr["metadata"]["generation"], 2, "{cr}");
}

/// etcd.go:249-255: the Scale's resourceVersion becomes the precondition.
#[tokio::test]
async fn put_scale_with_a_stale_resource_version_conflicts() {
    let api = TestApiServer::new();
    install(&api, &scalable_crd("widgets")).await;
    create(&api, &ns_path("widgets"), &scaled_widget("w1")).await;
    let path = format!("{}/w1", ns_path("widgets"));
    let (_, scale) = api.get(&format!("{path}/scale")).await;
    let mut stale = scale.clone();
    stale["spec"]["replicas"] = json!(3);
    let (status, out) = api.put(&format!("{path}/scale"), &stale).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (status, out) = api.put(&format!("{path}/scale"), &stale).await;
    assert_eq!(status, StatusCode::CONFLICT, "{out}");
}

/// `AllowCreateOnUpdate` is false for a subresource (etcd.go:211), so a scale
/// of an absent object is NotFound.
#[tokio::test]
async fn put_scale_of_an_absent_object_is_not_found() {
    let api = TestApiServer::new();
    install(&api, &scalable_crd("widgets")).await;
    let scale = json!({
        "apiVersion": "autoscaling/v1", "kind": "Scale",
        "metadata": { "name": "nope" }, "spec": { "replicas": 1 }
    });
    let (status, out) = api
        .put(&format!("{}/nope/scale", ns_path("widgets")), &scale)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// A scale write is validated as the CR it produces: replicas must be
/// non-negative (validator.go:134-180 ValidateScaleSpec).
#[tokio::test]
async fn put_scale_with_negative_replicas_is_invalid() {
    let api = TestApiServer::new();
    install(&api, &scalable_crd("widgets")).await;
    create(&api, &ns_path("widgets"), &scaled_widget("w1")).await;
    let path = format!("{}/w1/scale", ns_path("widgets"));
    let (_, mut scale) = api.get(&path).await;
    scale["spec"]["replicas"] = json!(-1);
    let (status, out) = api.put(&path, &scale).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// PATCH of the Scale goes into `ScaleREST.Update` too.
#[tokio::test]
async fn patch_scale_updates_the_replicas() {
    let api = TestApiServer::new();
    install(&api, &scalable_crd("widgets")).await;
    create(&api, &ns_path("widgets"), &scaled_widget("w1")).await;
    let path = format!("{}/w1", ns_path("widgets"));
    let (status, out) = api
        .patch(
            &format!("{path}/scale"),
            &json!({ "spec": { "replicas": 4 } }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Scale", "{out}");
    assert_eq!(out["spec"]["replicas"], 4, "{out}");
    let (_, cr) = api.get(&path).await;
    assert_eq!(cr["spec"]["replicas"], 4, "{cr}");
}

/// customresource_handler.go:349: no `scale` subresource, no `/scale` route.
#[tokio::test]
async fn scale_not_enabled_is_not_found() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", true, open_schema()),
    )
    .await;
    create(&api, &ns_path("widgets"), &scaled_widget("w1")).await;
    let (status, out) = api.get(&format!("{}/w1/scale", ns_path("widgets"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// field_validation.go:620-735 "should detect duplicates in a CR when
/// preserving unknown fields": a YAML apply body with a repeated key under
/// `fieldValidation=Strict` is refused with
/// `line 9: key "foo" already set in map` (applyPatcher strict-decodes the
/// body, patch.go:517-527).
#[tokio::test]
async fn strict_apply_refuses_a_duplicate_yaml_key() {
    let api = TestApiServer::new();
    install(
        &api,
        &crd("widgets", "Widget", "Namespaced", false, open_schema()),
    )
    .await;
    let path = format!(
        "{}/mytest?fieldManager=field_validation_mgr&fieldValidation=Strict",
        ns_path("widgets")
    );
    let yaml = format!(
        "\napiVersion: {GROUP}/v1\nkind: Widget\nmetadata:\n  name: mytest\nspec:\n  unknown: uk1\n  foo: foo1\n  foo: foo2\n  cronSpec: \"* * * * */5\"\n  ports:\n  - name: x\n    containerPort: 80\n    protocol: TCP"
    );
    let (status, _, bytes, _) = api
        .send_with_headers(
            "PATCH",
            &path,
            &[("content-type", "application/apply-patch+yaml")],
            Some(yaml.into_bytes()),
        )
        .await;
    let text = String::from_utf8_lossy(&bytes).to_string();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(
        text.contains(r#"line 9: key \"foo\" already set in map"#),
        "{text}"
    );
}
