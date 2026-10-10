//! A CRD that was accepted on create is accepted again on a Get + Update
//! (#3081). `CustomResourcePublishOpenAPI` "removes definition from spec when
//! one version gets changed to not be served"
//! (`test/e2e/apimachinery/crd_publish_openapi.go:447-475`) creates a
//! two-version CRD with `schemaFoo` (`:745-796`: `bars` is an array whose
//! `items` is `{type: object, ...}`), GETs it, flips `versions[1].served` and
//! PUTs it back; `ValidateCustomResourceDefinitionUpdate`
//! (`apiextensions/validation/validation.go:230-250`) re-runs the create-time
//! structural check, so an `items.type` lost on the roundtrip is rejected with
//! "must not be empty for specified array items".

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

fn schema_foo() -> Value {
    json!({ "openAPIV3Schema": {
        "description": "Foo CRD for Testing",
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "description": "Specification of Foo",
                "properties": { "bars": {
                    "description": "List of Bars and their specs.",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["name"],
                        "properties": {
                            "name": { "type": "string" },
                            "age": { "type": "string" },
                            "feeling": { "type": "string", "enum": ["Great", "Down"] },
                            "bazs": { "type": "array", "items": { "type": "string" } }
                        }
                    }
                }}
            },
            "status": {
                "type": "object",
                "description": "Status of Foo",
                "properties": { "bars": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "available": { "type": "boolean" },
                            "quxType": { "type": "string", "pattern": "in-tree|out-of-tree" }
                        }
                    }
                }}
            }
        }
    }})
}

#[tokio::test]
async fn get_then_update_keeps_array_items_type() {
    let api = TestApiServer::new();
    let name = "foos.crd-publish-openapi-test-multi-to-single-ver.example.com";
    let body = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": name },
        "spec": {
            "group": "crd-publish-openapi-test-multi-to-single-ver.example.com",
            "scope": "Namespaced",
            "names": { "plural": "foos", "singular": "foo", "kind": "Foo", "listKind": "FooList" },
            "versions": [
                { "name": "v5", "served": true, "storage": true, "schema": schema_foo() },
                { "name": "v6alpha1", "served": true, "storage": false, "schema": schema_foo() }
            ]
        }
    });
    let (status, answer) = api
        .send("POST", CRDS, Some("application/json"), Some(&body))
        .await;
    assert!(status.is_success(), "create: {status} {answer}");

    let url = format!("{CRDS}/{name}");
    let (status, mut got) = api.send("GET", &url, None, None).await;
    assert!(status.is_success(), "get: {status} {got}");
    assert_eq!(
        got["spec"]["versions"][1]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
            ["bars"]["items"]["type"],
        "object",
        "GET lost items.type: {got}"
    );

    got["spec"]["versions"][1]["served"] = json!(false);
    let (status, answer) = api
        .send("PUT", &url, Some("application/json"), Some(&got))
        .await;
    assert_eq!(status.as_u16(), 200, "update: {answer}");
}

/// The same roundtrip over `application/cbor`, which client-go 1.35 negotiates.
#[tokio::test]
async fn cbor_get_then_update_keeps_array_items_type() {
    use rusternetes_api_server::cbor;
    let api = TestApiServer::new();
    let name = "foos.cbor.example.com";
    let body = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": name },
        "spec": {
            "group": "cbor.example.com",
            "scope": "Namespaced",
            "names": { "plural": "foos", "singular": "foo", "kind": "Foo", "listKind": "FooList" },
            "versions": [
                { "name": "v5", "served": true, "storage": true, "schema": schema_foo() },
                { "name": "v6alpha1", "served": true, "storage": false, "schema": schema_foo() }
            ]
        }
    });
    let hdrs = [
        ("content-type", "application/cbor"),
        ("accept", "application/cbor"),
    ];
    let (status, _, bytes, _) = api
        .send_with_headers(
            "POST",
            CRDS,
            &hdrs,
            Some(cbor::encode_json_to_cbor(&body).unwrap()),
        )
        .await;
    assert!(
        status.is_success(),
        "create: {status} {}",
        String::from_utf8_lossy(&bytes)
    );

    let url = format!("{CRDS}/{name}");
    let (status, _, bytes, _) = api.send_with_headers("GET", &url, &hdrs, None).await;
    assert!(status.is_success(), "get: {status}");
    let mut got = cbor::decode_cbor_to_json(&bytes).unwrap();
    got["spec"]["versions"][1]["served"] = json!(false);
    let (status, _, bytes, _) = api
        .send_with_headers(
            "PUT",
            &url,
            &hdrs,
            Some(cbor::encode_json_to_cbor(&got).unwrap()),
        )
        .await;
    assert_eq!(
        status.as_u16(),
        200,
        "update: {}",
        String::from_utf8_lossy(&bytes)
    );
}
