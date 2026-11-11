//! Custom-resource tables: `Accept: application/json;as=Table;g=meta.k8s.io;v=v1`
//! on LIST and GET renders the CRD version's `additionalPrinterColumns`
//! (upstream `customresource_handler.go:843-851`, `tableconvertor.go`), with
//! the default `Age` column when none are declared.

use axum::http::StatusCode;
use k3rs_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const GROUP: &str = "stable.example.com";
const TABLE: &str = "application/json;as=Table;g=meta.k8s.io;v=v1";

fn crd(plural: &str, kind: &str, columns: Value) -> Value {
    let mut version = json!({
        "name": "v1", "served": true, "storage": true,
        "schema": { "openAPIV3Schema": { "type": "object",
            "x-kubernetes-preserve-unknown-fields": true } }
    });
    if !columns.is_null() {
        version["additionalPrinterColumns"] = columns;
    }
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{plural}.{GROUP}") },
        "spec": {
            "group": GROUP, "scope": "Namespaced",
            "names": { "plural": plural, "kind": kind, "listKind": format!("{kind}List") },
            "versions": [version]
        }
    })
}

async fn install(api: &TestApiServer, plural: &str, kind: &str, columns: Value) {
    let (status, out) = api
        .post(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            &crd(plural, kind, columns),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established(&format!("{plural}.{GROUP}")).await;
}

async fn get_table(api: &TestApiServer, uri: &str) -> (StatusCode, String, Value) {
    let (status, headers, _bytes, body) = api
        .send_with_headers("GET", uri, &[("accept", TABLE)], None)
        .await;
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    (status, ct, body)
}

#[tokio::test]
async fn list_and_get_render_declared_printer_columns() {
    let api = TestApiServer::new();
    install(
        &api,
        "widgets",
        "Widget",
        json!([
            {"name": "Color", "type": "string", "jsonPath": ".spec.color",
             "description": "the color"},
            {"name": "Count", "type": "integer", "jsonPath": ".spec.count", "priority": 1},
            {"name": "Missing", "type": "string", "jsonPath": ".spec.nope"},
            {"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}
        ]),
    )
    .await;
    let path = "/apis/stable.example.com/v1/namespaces/default/widgets";
    let (status, out) = api
        .post(
            path,
            &json!({"apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
                    "metadata": {"name": "w1"}, "spec": {"color": "red", "count": 3}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    for uri in [path.to_string(), format!("{path}/w1")] {
        let (status, ct, table) = get_table(&api, &uri).await;
        assert_eq!(status, StatusCode::OK, "{table}");
        assert!(ct.starts_with("application/json"), "{ct}");
        assert_eq!(table["kind"], "Table", "{table}");
        assert_eq!(table["apiVersion"], "meta.k8s.io/v1");
        let cols: Vec<(&str, &str, i64)> = table["columnDefinitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    c["name"].as_str().unwrap(),
                    c["type"].as_str().unwrap(),
                    c["priority"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            cols,
            [
                ("Name", "string", 0),
                ("Color", "string", 0),
                ("Count", "integer", 1),
                ("Missing", "string", 0),
                ("Age", "date", 0)
            ]
        );
        assert_eq!(table["columnDefinitions"][1]["description"], "the color");
        let row = &table["rows"][0];
        let cells = row["cells"].as_array().unwrap();
        assert_eq!(
            cells[..4],
            [json!("w1"), json!("red"), json!(3), Value::Null]
        );
        assert!(cells[4].is_string(), "{cells:?}");
        assert_eq!(row["object"]["metadata"]["name"], "w1");
    }
}

#[tokio::test]
async fn default_age_column_when_none_declared_and_no_headers() {
    let api = TestApiServer::new();
    install(&api, "gadgets", "Gadget", Value::Null).await;
    let path = "/apis/stable.example.com/v1/namespaces/default/gadgets";
    let (status, out) = api
        .post(
            path,
            &json!({"apiVersion": format!("{GROUP}/v1"), "kind": "Gadget",
                    "metadata": {"name": "g1"}, "spec": {}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");

    let (status, _, table) = get_table(&api, path).await;
    assert_eq!(status, StatusCode::OK, "{table}");
    let names: Vec<&str> = table["columnDefinitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["Name", "Age"]);
    assert_eq!(table["columnDefinitions"][1]["type"], "date");
    assert_eq!(table["rows"][0]["cells"][0], "g1");
    assert!(table["rows"][0]["cells"][1].is_string());

    let (_, _, table) = get_table(&api, &format!("{path}?noHeaders=true")).await;
    assert_eq!(table["columnDefinitions"], json!([]), "{table}");
    assert_eq!(table["rows"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn plain_requests_and_errors_are_unchanged() {
    let api = TestApiServer::new();
    install(&api, "things", "Thing", Value::Null).await;
    let path = "/apis/stable.example.com/v1/namespaces/default/things";
    let (status, list) = api.get(path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["kind"], "ThingList");
    let (status, ct, body) = get_table(&api, &format!("{path}/absent")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(ct.starts_with("application/json"));
    assert_eq!(body["kind"], "Status");
}
