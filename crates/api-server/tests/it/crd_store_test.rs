//! CustomResourceDefinition served through the generic Store (#1990).
//!
//! Each test pins one upstream rule the bespoke handler did not have, with the
//! source it comes from. Together they are the behavioural contract of
//! `registry::apiextensions::customresourcedefinition` over
//! `registry::generic::Store`:
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/registry/customresourcedefinition/{strategy.go,etcd.go}`
//! and the validation in `pkg/apis/apiextensions/validation/validation.go`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

fn crd(plural: &str, kind: &str, group: &str) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{plural}.{group}") },
        "spec": {
            "group": group,
            "scope": "Namespaced",
            "names": { "plural": plural, "kind": kind },
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": {
                    "type": "object",
                    "x-kubernetes-preserve-unknown-fields": true
                }}
            }]
        }
    })
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

async fn create(api: &TestApiServer, body: &Value) -> Value {
    let (status, out) = api.post(CRDS, body).await;
    assert_eq!(status, StatusCode::CREATED, "create: {out}");
    out
}

fn condition<'a>(crd: &'a Value, kind: &str) -> Option<&'a Value> {
    crd["status"]["conditions"]
        .as_array()?
        .iter()
        .find(|c| c["type"] == kind)
}

/// `validatePreserveUnknownFields` (validation.go:1892-1904): a v1 CRD cannot
/// set `spec.preserveUnknownFields` to true.
#[tokio::test]
async fn create_rejects_preserve_unknown_fields_true() {
    let api = TestApiServer::new();
    let mut body = crd("widgets", "Widget", "example.com");
    body["spec"]["preserveUnknownFields"] = json!(true);
    let (status, out) = api.post(CRDS, &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains(
            "cannot set to true, set x-kubernetes-preserve-unknown-fields to true in spec.versions[*].schema instead"
        ),
        "{out}"
    );
}

/// `validateAPIApproval` (validation.go:1857-1890): a protected group
/// (`*.k8s.io`, `*.kubernetes.io`) needs the `api-approved.kubernetes.io`
/// annotation.
#[tokio::test]
async fn create_in_a_protected_group_needs_the_approval_annotation() {
    let api = TestApiServer::new();
    let body = crd("widgets", "Widget", "example.k8s.io");
    let (status, out) = api.post(CRDS, &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("metadata.annotations[api-approved.kubernetes.io]: Required value"),
        "{out}"
    );

    let mut approved = body.clone();
    approved["metadata"]["annotations"] =
        json!({"api-approved.kubernetes.io": "https://github.com/kubernetes/kubernetes/pull/1"});
    create(&api, &approved).await;
}

/// `PrepareForCreate` (strategy.go:74-89): the status a client sent is
/// discarded, the generation is 1 and `storedVersions` is the storage version.
#[tokio::test]
async fn create_discards_a_client_status_and_starts_generation_at_one() {
    let api = TestApiServer::new();
    let mut body = crd("widgets", "Widget", "example.com");
    body["metadata"]["generation"] = json!(7);
    body["status"] = json!({
        "acceptedNames": {"plural": "hacked", "kind": "Hacked"},
        "storedVersions": ["v9"],
        "conditions": [{"type": "Established", "status": "True"}]
    });
    let out = create(&api, &body).await;
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["status"]["storedVersions"], json!(["v1"]), "{out}");
    assert_eq!(out["status"]["acceptedNames"]["plural"], "widgets", "{out}");
    assert_eq!(out["status"]["acceptedNames"]["kind"], "Widget", "{out}");
}

/// The NamingConditionController (status/naming_controller.go:131-216): a
/// CRD whose kind is already claimed in the group is not accepted, and the
/// EstablishingController (establish/establishing_controller.go:127-135) then
/// never establishes it.
#[tokio::test]
async fn a_kind_conflict_is_not_accepted_and_not_established() {
    let api = TestApiServer::new();
    let first = create(&api, &crd("widgets", "Widget", "example.com")).await;
    assert_eq!(
        condition(&first, "Established").unwrap()["status"],
        "True",
        "{first}"
    );
    assert_eq!(
        condition(&first, "NamesAccepted").unwrap()["reason"],
        "NoConflicts",
        "{first}"
    );

    let mut gadgets = crd("gadgets", "Widget", "example.com");
    gadgets["spec"]["names"]["listKind"] = json!("GadgetList");
    let second = create(&api, &gadgets).await;
    let accepted = condition(&second, "NamesAccepted").expect("NamesAccepted");
    assert_eq!(accepted["status"], "False", "{second}");
    assert_eq!(accepted["reason"], "KindConflict", "{second}");
    assert_eq!(
        accepted["message"], "\"Widget\" is already in use",
        "{second}"
    );
    assert_ne!(
        condition(&second, "Established").map(|c| c["status"].clone()),
        Some(json!("True")),
        "{second}"
    );
}

/// `AllowUnconditionalUpdate` is false (strategy.go:151-153): a PUT must
/// carry a resourceVersion.
#[tokio::test]
async fn update_without_a_resource_version_is_rejected() {
    let api = TestApiServer::new();
    let created = create(&api, &crd("widgets", "Widget", "example.com")).await;
    let mut body = created.clone();
    body["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    let (status, out) = api.put(&format!("{CRDS}/widgets.example.com"), &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("metadata.resourceVersion: Invalid value"),
        "{out}"
    );
}

/// `PrepareForUpdate` (strategy.go:91-118): the status is the stored one, and
/// the generation moves only with the spec.
#[tokio::test]
async fn update_keeps_the_stored_status_and_bumps_generation_on_spec_change() {
    let api = TestApiServer::new();
    let created = create(&api, &crd("widgets", "Widget", "example.com")).await;
    let url = format!("{CRDS}/widgets.example.com");

    // A status the client invents is ignored; an unchanged spec keeps
    // the generation.
    let mut body = created.clone();
    body["status"]["acceptedNames"]["plural"] = json!("hacked");
    body["metadata"]["labels"] = json!({"a": "b"});
    let (status, out) = api.put(&url, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["acceptedNames"]["plural"], "widgets", "{out}");
    assert_eq!(
        out["metadata"]["generation"], created["metadata"]["generation"],
        "{out}"
    );

    let mut body = out.clone();
    body["spec"]["names"]["shortNames"] = json!(["wd"]);
    let (status, out2) = api.put(&url, &body).await;
    assert_eq!(status, StatusCode::OK, "{out2}");
    assert_eq!(out2["metadata"]["generation"], 2, "{out2}");
}

/// `ValidateCustomResourceDefinitionStoredVersions` (validation.go:261-285):
/// a version that was ever a storage version stays in `spec.versions` until
/// the migration controller trims `status.storedVersions`.
#[tokio::test]
async fn a_stored_version_cannot_be_removed_from_the_spec() {
    let api = TestApiServer::new();
    let created = create(&api, &crd("widgets", "Widget", "example.com")).await;
    let url = format!("{CRDS}/widgets.example.com");

    let mut body = created.clone();
    body["spec"]["versions"] = json!([{
        "name": "v2", "served": true, "storage": true,
        "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
    }]);
    let (status, out) = api.put(&url, &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("missing from spec.versions; v1 was previously a storage version"),
        "{out}"
    );
}

/// `statusStrategy.PrepareForUpdate` (strategy.go:267-274) keeps the spec;
/// `ValidateUpdateCustomResourceDefinitionStatus` (validation.go:288-292)
/// validates `status.acceptedNames`.
#[tokio::test]
async fn status_update_keeps_the_spec_and_validates_accepted_names() {
    let api = TestApiServer::new();
    let created = create(&api, &crd("widgets", "Widget", "example.com")).await;
    let url = format!("{CRDS}/widgets.example.com/status");

    let mut body = created.clone();
    body["spec"]["names"]["shortNames"] = json!(["wd"]);
    body["status"]["conditions"] = json!([
        {"type": "Established", "status": "True"},
        {"type": "Example", "status": "True"}
    ]);
    let (status, out) = api.put(&url, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(out["spec"]["names"].get("shortNames").is_none(), "{out}");
    assert!(condition(&out, "Example").is_some(), "{out}");

    let mut bad = out.clone();
    bad["status"]["acceptedNames"]["plural"] = json!("Not_Valid");
    let (status, out) = api.put(&url, &bad).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("status.acceptedNames.plural: Invalid value"),
        "{out}"
    );
}

/// `REST.Delete` (etcd.go:84-176): the first delete marks the CRD with the
/// cleanup finalizer and a Terminating condition; the CRD finalizer
/// (finalizer/crd_finalizer.go:112-179) then deletes the instances before the
/// CRD goes. A recreated CRD must not resurrect the old instances.
#[tokio::test]
async fn deleting_a_crd_deletes_its_instances() {
    let api = TestApiServer::new();
    // Instances are found namespace by namespace, so the namespace exists.
    let (status, out) = api
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
        )
        .await;
    assert!(
        status.is_success() || status == StatusCode::CONFLICT,
        "{out}"
    );
    create(&api, &crd("widgets", "Widget", "example.com")).await;
    let cr = "/apis/example.com/v1/namespaces/default/widgets";
    let (status, out) = api
        .post(
            cr,
            &json!({"apiVersion": "example.com/v1", "kind": "Widget", "metadata": {"name": "w1"}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let (status, out) = api.delete(&format!("{CRDS}/widgets.example.com")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    // The answer is the object as the first delete left it.
    assert!(out["metadata"]["deletionTimestamp"].is_string(), "{out}");
    assert!(
        out["metadata"]["finalizers"]
            .as_array()
            .is_some_and(|f| f.contains(&json!("customresourcecleanup.apiextensions.k8s.io"))),
        "{out}"
    );
    let (status, _) = api.get(&format!("{CRDS}/widgets.example.com")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    create(&api, &crd("widgets", "Widget", "example.com")).await;
    let (status, out) = api.get(cr).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["items"].as_array().map(Vec::len), Some(0), "{out}");
}

/// `REST.Delete` (etcd.go:97-110): a UID precondition that does not match
/// is a conflict.
#[tokio::test]
async fn delete_checks_the_uid_precondition() {
    let api = TestApiServer::new();
    create(&api, &crd("widgets", "Widget", "example.com")).await;
    let (status, out) = api
        .send(
            "DELETE",
            &format!("{CRDS}/widgets.example.com"),
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1", "kind": "DeleteOptions",
                "preconditions": {"uid": "not-the-uid"}
            })),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{out}");
    let (status, _) = api.get(&format!("{CRDS}/widgets.example.com")).await;
    assert_eq!(status, StatusCode::OK);
}

/// A dry-run create answers like a create and stores nothing.
#[tokio::test]
async fn dry_run_create_stores_nothing() {
    let api = TestApiServer::new();
    let (status, out) = api
        .post(
            &format!("{CRDS}?dryRun=All"),
            &crd("widgets", "Widget", "example.com"),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let (status, _) = api.get(&format!("{CRDS}/widgets.example.com")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// `ValidateObjectMeta`'s name function (validation.go:76-83): the name must
/// be `spec.names.plural+"."+spec.group`.
#[tokio::test]
async fn create_requires_the_name_to_be_plural_dot_group() {
    let api = TestApiServer::new();
    let mut body = crd("widgets", "Widget", "example.com");
    body["metadata"]["name"] = json!("other.example.com");
    let (status, out) = api.post(CRDS, &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("must be spec.names.plural+\".\"+spec.group"),
        "{out}"
    );
}

/// The apiapproval controller (apiapproval/apiapproval_controller.go:85-128):
/// a protected group's CRD carries a `KubernetesAPIApprovalPolicyConformant`
/// condition reflecting its annotation, and any other group's carries none.
#[tokio::test]
async fn a_protected_group_gets_the_api_approval_condition() {
    let api = TestApiServer::new();
    let mut approved = crd("widgets", "Widget", "example.k8s.io");
    approved["metadata"]["annotations"] =
        json!({"api-approved.kubernetes.io": "https://github.com/kubernetes/kubernetes/pull/1"});
    let out = create(&api, &approved).await;
    let cond = condition(&out, "KubernetesAPIApprovalPolicyConformant").expect("condition");
    assert_eq!(cond["status"], "True", "{out}");
    assert_eq!(cond["reason"], "ApprovedAnnotation", "{out}");
    assert_eq!(
        cond["message"], "approved in https://github.com/kubernetes/kubernetes/pull/1",
        "{out}"
    );

    let mut bypassed = crd("gadgets", "Gadget", "example.k8s.io");
    bypassed["metadata"]["annotations"] =
        json!({"api-approved.kubernetes.io": "unapproved, experimental"});
    let out = create(&api, &bypassed).await;
    let cond = condition(&out, "KubernetesAPIApprovalPolicyConformant").expect("condition");
    assert_eq!(cond["status"], "False", "{out}");
    assert_eq!(cond["reason"], "UnapprovedAnnotation", "{out}");

    let other = create(&api, &crd("things", "Thing", "example.com")).await;
    assert!(
        condition(&other, "KubernetesAPIApprovalPolicyConformant").is_none(),
        "{other}"
    );
    assert!(
        condition(&other, "NonStructuralSchema").is_none(),
        "{other}"
    );
}
