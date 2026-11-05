//! Tests for `src/openapiconv.rs` (`k8s.io/kube-openapi/pkg/openapiconv`).
//!
//! The vendored kube-openapi has no `_test.go` files; these pin the Go
//! `Convert*` functions' behaviour, and the one expected document upstream's
//! `TestV2APIService` (kube-aggregator openapiv3) carries.

use rusternetes_api_server::openapiconv::{convert_schema, convert_v2_to_v3};
use serde_json::json;

#[test]
fn an_empty_v2_spec_converts_like_test_v2_api_service() {
    // kube-aggregator openapiv3/aggregator/aggregator_test.go TestV2APIService
    let v3 = convert_v2_to_v3(&json!({
        "swagger": "2.0",
        "info": {"title": "Kubernetes", "version": "unversioned"}
    }));
    assert_eq!(
        v3,
        json!({"openapi": "3.0.0", "info": {"title": "Kubernetes", "version": "unversioned"}, "components": {}})
    );
}

#[test]
fn schema_refs_move_to_components_and_siblings_wrap_in_all_of() {
    let v3 = convert_schema(&json!({
        "type": "object",
        "properties": {
            "plain": {"$ref": "#/definitions/Foo"},
            "described": {"$ref": "#/definitions/Foo", "description": "d"},
            "list": {"type": "array", "items": {"$ref": "#/definitions/Bar"}},
            "map": {"type": "object", "additionalProperties": {"$ref": "#/definitions/Baz"}}
        }
    }));
    let p = &v3["properties"];
    assert_eq!(p["plain"], json!({"$ref": "#/components/schemas/Foo"}));
    // WrapRefs: "references [may not] have sibling elements"
    assert_eq!(
        p["described"],
        json!({"description": "d", "allOf": [{"$ref": "#/components/schemas/Foo"}]})
    );
    assert_eq!(
        p["list"]["items"],
        json!({"$ref": "#/components/schemas/Bar"})
    );
    assert_eq!(
        p["map"]["additionalProperties"],
        json!({"$ref": "#/components/schemas/Baz"})
    );
}

#[test]
fn definitions_become_component_schemas() {
    let v3 = convert_v2_to_v3(&json!({
        "swagger": "2.0",
        "info": {"title": "t", "version": "v"},
        "definitions": {"A": {"type": "string"}},
        "securityDefinitions": {
            "BearerToken": {"type": "apiKey", "name": "authorization", "in": "header"},
            "oauth": {
                "type": "oauth2", "flow": "implicit",
                "authorizationUrl": "https://a", "scopes": {"s": "d"}
            }
        }
    }));
    assert_eq!(v3["components"]["schemas"]["A"], json!({"type": "string"}));
    assert_eq!(
        v3["components"]["securitySchemes"]["BearerToken"],
        json!({"type": "apiKey", "name": "authorization", "in": "header"})
    );
    assert_eq!(
        v3["components"]["securitySchemes"]["oauth"],
        json!({"type": "oauth2", "flows": {"implicit": {"authorizationUrl": "https://a", "scopes": {"s": "d"}}}})
    );
}

#[test]
fn operations_get_request_bodies_parameters_and_response_content() {
    let v3 = convert_v2_to_v3(&json!({
        "swagger": "2.0",
        "info": {"title": "t", "version": "v"},
        "paths": {
            "x-paths-ext": true,
            "/apis/g/v1/things/{name}": {
                "parameters": [{"name": "name", "in": "path", "required": true, "type": "string"}],
                "patch": {
                    "operationId": "patchThing",
                    "description": "patch it",
                    "tags": ["g_v1"],
                    "consumes": ["application/json-patch+json", "application/merge-patch+json"],
                    "produces": ["application/json"],
                    "x-kubernetes-action": "patch",
                    "parameters": [
                        {"name": "body", "in": "body", "required": true, "schema": {"$ref": "#/definitions/Thing"}},
                        {"name": "dryRun", "in": "query", "type": "string", "uniqueItems": true}
                    ],
                    "responses": {
                        "200": {"description": "OK", "schema": {"$ref": "#/definitions/Thing"}},
                        "401": {"description": "Unauthorized"}
                    }
                }
            }
        }
    }));
    assert_eq!(v3["paths"]["x-paths-ext"], true);
    let item = &v3["paths"]["/apis/g/v1/things/{name}"];
    // a v2 simple parameter becomes a schema'd v3 parameter
    assert_eq!(
        item["parameters"][0],
        json!({"name": "name", "in": "path", "required": true, "schema": {"type": "string"}})
    );
    let op = &item["patch"];
    assert_eq!(op["operationId"], "patchThing");
    assert_eq!(op["x-kubernetes-action"], "patch");
    // the body parameter is the request body, one media type per `consumes`
    let schema = json!({"schema": {"$ref": "#/components/schemas/Thing"}});
    assert_eq!(
        op["requestBody"]["content"],
        json!({"application/json-patch+json": schema, "application/merge-patch+json": schema})
    );
    assert_eq!(
        op["parameters"],
        json!([{"name": "dryRun", "in": "query", "schema": {"type": "string", "uniqueItems": true}}])
    );
    // responses carry one media type per `produces`
    assert_eq!(
        op["responses"]["200"],
        json!({"description": "OK", "content": {"application/json": schema}})
    );
    assert_eq!(
        op["responses"]["401"],
        json!({"description": "Unauthorized"})
    );
}

#[test]
fn referenced_parameters_and_responses_move_to_components() {
    let v3 = convert_v2_to_v3(&json!({
        "swagger": "2.0",
        "info": {"title": "t", "version": "v"},
        "paths": {"/p": {"get": {
            "parameters": [{"$ref": "#/parameters/P"}],
            "responses": {"default": {"$ref": "#/responses/R"}}
        }}}
    }));
    let op = &v3["paths"]["/p"]["get"];
    assert_eq!(op["parameters"][0]["$ref"], "#/components/parameters/P");
    assert_eq!(
        op["responses"]["default"]["$ref"],
        "#/components/responses/R"
    );
}
