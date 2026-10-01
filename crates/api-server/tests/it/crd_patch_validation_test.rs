//! A CRD PATCH is validated, and a selectable field's `jsonPath` is resolved.
//!
//! Two gaps left by #2019, which wired `validateCustomResourceDefinitionSpec`
//! into the CRD create and update handlers only (#2020).
//!
//! 1. Upstream has a single `Store.Update` behind PUT, PATCH and apply
//!    (`registry/generic/registry/store.go`), reached through the CRD
//!    strategy's `ValidateUpdate`
//!    (`apiextensions-apiserver/pkg/registry/customresourcedefinition/strategy.go`),
//!    so all three run `ValidateCustomResourceDefinitionUpdate`
//!    (`validation.go:230-250`) — which is the create-time spec validation plus
//!    the immutability rules of `validateCustomResourceDefinitionSpecUpdate`
//!    (`:648-664`). A rule that only guards PUT is not a rule.
//! 2. `ValidateCustomResourceSelectableFields` (`:847-878`) resolves each
//!    `jsonPath` against the version's schema with `cel.ValidFieldPath` and
//!    rejects a path that does not exist, one that points into `metadata`, and
//!    one whose leaf is not a scalar.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

fn schema() -> Value {
    json!({ "openAPIV3Schema": {
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "color": { "type": "string" },
                    "nested": { "type": "object", "properties": { "n": { "type": "integer" } } }
                }
            }
        }
    }})
}

fn crd_body(plural: &str, kind: &str, patch: Value) -> Value {
    let mut spec = json!({
        "group": "example.com",
        "scope": "Namespaced",
        "names": { "plural": plural, "kind": kind },
        "versions": [{
            "name": "v1", "served": true, "storage": true, "schema": schema()
        }]
    });
    for (k, v) in patch.as_object().unwrap() {
        spec[k] = v.clone();
    }
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{plural}.example.com") },
        "spec": spec,
    })
}

async fn create(api: &TestApiServer, plural: &str, kind: &str) -> String {
    let (status, body) = api
        .send(
            "POST",
            CRDS,
            Some("application/json"),
            Some(&crd_body(plural, kind, json!({}))),
        )
        .await;
    assert!(status.is_success(), "create must succeed: {status} {body}");
    format!("{CRDS}/{plural}.example.com")
}

async fn patch(api: &TestApiServer, url: &str, body: Value) -> (u16, Value) {
    let (status, answer) = api
        .send(
            "PATCH",
            url,
            Some("application/merge-patch+json"),
            Some(&body),
        )
        .await;
    (status.as_u16(), answer)
}

/// The exact patch #2020 names: a `None` conversion strategy carrying webhook
/// settings, which `validateCustomResourceConversion`
/// (`validation.go:636-645`) forbids on create.
#[tokio::test]
async fn a_patch_cannot_install_a_spec_the_create_path_rejects() {
    let api = TestApiServer::new();
    let url = create(&api, "widgets", "Widget").await;

    let (status, body) = patch(
        &api,
        &url,
        json!({ "spec": { "conversion": {
            "strategy": "None",
            "webhook": {
                "clientConfig": { "url": "https://example.com/convert" },
                "conversionReviewVersions": ["v1"]
            }
        }}}),
    )
    .await;

    assert_eq!(status, 422, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("should not be set when strategy is not set to Webhook"),
        "{body}"
    );
}

/// `spec.group` and `spec.names.plural` are immutable in every case; they
/// decide the object's own name (`validation.go:658-663`).
#[tokio::test]
async fn a_patch_cannot_change_an_immutable_field() {
    let api = TestApiServer::new();
    let url = create(&api, "widgets", "Widget").await;

    for (label, body, expected) in [
        (
            "group",
            json!({ "spec": { "group": "other.com" } }),
            "spec.group: Invalid value",
        ),
        (
            "names.plural",
            json!({ "spec": { "names": { "plural": "gadgets" } } }),
            "spec.names.plural: Invalid value",
        ),
    ] {
        let (status, answer) = patch(&api, &url, body).await;
        assert_eq!(status, 422, "{label}: {answer}");
        let message = answer["message"].as_str().unwrap_or_default();
        assert!(message.contains(expected), "{label}: {message}");
        assert!(message.contains("field is immutable"), "{label}: {message}");
    }
}

/// `scope` and `names.kind` are immutable only once the CRD is Established —
/// `requireImmutableNames: IsCRDConditionTrue(oldObj, Established)`
/// (`validation.go:234`). Our create handler seeds that condition, so they are
/// immutable here.
#[tokio::test]
async fn an_established_crd_holds_its_scope_and_kind() {
    let api = TestApiServer::new();
    let url = create(&api, "widgets", "Widget").await;

    for (label, body) in [
        ("scope", json!({ "spec": { "scope": "Cluster" } })),
        (
            "names.kind",
            json!({ "spec": { "names": { "kind": "Gizmo" } } }),
        ),
    ] {
        let (status, answer) = patch(&api, &url, body).await;
        assert_eq!(status, 422, "{label}: {answer}");
        assert!(
            answer["message"]
                .as_str()
                .unwrap_or_default()
                .contains("field is immutable"),
            "{label}: {answer}"
        );
    }
}

/// A PUT runs the same immutability rules — they belong to the update
/// validator, not to the patch handler.
#[tokio::test]
async fn a_put_runs_the_same_immutability_rules() {
    let api = TestApiServer::new();
    let url = create(&api, "widgets", "Widget").await;

    let mut body = crd_body("widgets", "Widget", json!({}));
    body["spec"]["group"] = json!("other.com");
    // A PUT carries the resourceVersion it read (`AllowUnconditionalUpdate`
    // is false, customresourcedefinition/strategy.go:151).
    let (_, current) = api.get(&url).await;
    body["metadata"]["resourceVersion"] = current["metadata"]["resourceVersion"].clone();

    let (status, answer) = api
        .send("PUT", &url, Some("application/json"), Some(&body))
        .await;
    assert_eq!(status.as_u16(), 422, "{answer}");
    assert!(
        answer["message"]
            .as_str()
            .unwrap_or_default()
            .contains("field is immutable"),
        "{answer}"
    );
}

/// A patch that changes nothing forbidden still applies.
#[tokio::test]
async fn a_valid_patch_still_applies() {
    let api = TestApiServer::new();
    let url = create(&api, "widgets", "Widget").await;

    let (status, body) = patch(
        &api,
        &url,
        json!({ "spec": { "names": { "shortNames": ["wd"] } } }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["spec"]["names"]["shortNames"][0], json!("wd"));
}

fn selectable_cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "a path the schema does not declare",
            json!([{ "jsonPath": ".spec.nope" }]),
            "is an invalid path: does not refer to a valid field",
        ),
        (
            "a path into metadata",
            json!([{ "jsonPath": ".metadata.name" }]),
            "is an invalid path: does not refer to a valid field",
        ),
        (
            "a path whose leaf is an object",
            json!([{ "jsonPath": ".spec.nested" }]),
            "must point to a field of type string, boolean or integer",
        ),
        (
            "array notation",
            json!([{ "jsonPath": ".spec['color']" }]),
            "is an invalid path: array notation is not allowed",
        ),
        (
            "a path that does not start with a delimiter",
            json!([{ "jsonPath": "spec.color" }]),
            "is an invalid path: expected [ or . but got: spec",
        ),
        (
            "the same field twice",
            json!([{ "jsonPath": ".spec.color" }, { "jsonPath": ".spec.color" }]),
            "selectableFields[1].jsonPath: Duplicate value",
        ),
    ]
}

#[tokio::test]
async fn every_bad_selectable_field_is_rejected_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, fields, expected)) in selectable_cases().into_iter().enumerate() {
        let plural = format!("sel{i}s");
        let kind = format!("Sel{i}");
        let mut body = crd_body(&plural, &kind, json!({}));
        body["spec"]["versions"][0]["selectableFields"] = fields;

        let (status, answer) = api
            .send("POST", CRDS, Some("application/json"), Some(&body))
            .await;

        assert_eq!(status.as_u16(), 422, "{label}: {status} {answer}");
        let message = answer["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
        assert!(
            message.contains("spec.versions[0].selectableFields["),
            "{label} must carry a field path, got: {message}"
        );
    }
}

/// More than `MaxSelectableFields` (8) distinct paths is `TooMany`
/// (`validation.go:873-876`).
#[tokio::test]
async fn more_than_eight_selectable_fields_is_too_many() {
    let api = TestApiServer::new();

    let mut properties = serde_json::Map::new();
    let mut fields = Vec::new();
    for i in 0..9 {
        properties.insert(format!("f{i}"), json!({ "type": "string" }));
        fields.push(json!({ "jsonPath": format!(".spec.f{i}") }));
    }
    let mut body = crd_body("manys", "Many", json!({}));
    body["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"] =
        Value::Object(properties);
    body["spec"]["versions"][0]["selectableFields"] = Value::Array(fields);

    let (status, answer) = api
        .send("POST", CRDS, Some("application/json"), Some(&body))
        .await;
    assert_eq!(status.as_u16(), 422, "{answer}");
    assert!(
        answer["message"]
            .as_str()
            .unwrap_or_default()
            .contains("selectableFields: Too many"),
        "{answer}"
    );
}

/// A scalar field the schema declares is accepted, and so is a map value
/// reached through `additionalProperties`.
#[tokio::test]
async fn a_resolvable_scalar_selectable_field_is_accepted() {
    let api = TestApiServer::new();

    let mut body = crd_body("goods", "Good", json!({}));
    body["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
        ["labels"] = json!({ "type": "object", "additionalProperties": { "type": "string" } });
    body["spec"]["versions"][0]["selectableFields"] = json!([
        { "jsonPath": ".spec.color" },
        { "jsonPath": ".spec.labels.anything" }
    ]);

    let (status, answer) = api
        .send("POST", CRDS, Some("application/json"), Some(&body))
        .await;
    assert!(status.is_success(), "{status} {answer}");
}
