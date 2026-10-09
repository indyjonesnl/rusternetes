//! #2946: explicit zero numeric validations in a CRD schema must survive the
//! real CRD create -> OpenAPI publish path.
//!
//! Upstream these are pointers with omitempty, so only nil is omitted and an
//! explicit 0 is serialised:
//! staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/types_jsonschema.go:80-94
//! (`Maximum *float64`, `MaxLength *int64`, `MaxItems *int64`, ...).
//! (`multipleOf: 0` is rejected by upstream validation, so it is not used.)
//!
//! Drives the real router over `StorageBackend::Memory`, like
//! `crd_openapi_v2_test.rs`.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS_URI: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

fn crd(plural: &str, props: Value) -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{plural}.zero.example.com") },
        "spec": {
            "group": "zero.example.com",
            "names": {"plural": plural, "singular": plural, "kind": "Zed", "listKind": "ZedList"},
            "scope": "Namespaced",
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "properties": props}}
            }]
        }
    })
}

const CASES: [(&str, &str); 8] = [
    ("s", "minLength"),
    ("s", "maxLength"),
    ("a", "minItems"),
    ("a", "maxItems"),
    ("o", "minProperties"),
    ("o", "maxProperties"),
    ("n", "minimum"),
    ("n", "maximum"),
];

fn zero_props() -> Value {
    json!({
        "s": {"type": "string", "minLength": 0, "maxLength": 0},
        "a": {"type": "array", "items": {"type": "string"}, "minItems": 0, "maxItems": 0},
        "o": {"type": "object", "minProperties": 0, "maxProperties": 0},
        "n": {"type": "number", "minimum": 0, "maximum": 0}
    })
}

fn absent_props() -> Value {
    json!({
        "s": {"type": "string"},
        "a": {"type": "array", "items": {"type": "string"}},
        "o": {"type": "object"},
        "n": {"type": "number"}
    })
}

async fn publish(srv: &TestApiServer, plural: &str, props: Value) -> (Value, Value) {
    let (status, body) = srv.post(CRDS_URI, &crd(plural, props)).await;
    assert!(status.is_success(), "CRD create failed: {status} {body}");
    let (_, stored) = srv
        .get(&format!("{CRDS_URI}/{plural}.zero.example.com"))
        .await;
    eprintln!(
        "stored s schema: {}",
        stored
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/s")
            .unwrap_or(&Value::Null)
    );
    let (s2, v2) = srv.get("/openapi/v2").await;
    assert_eq!(s2.as_u16(), 200);
    let (s3, v3) = srv.get("/openapi/v3/apis/zero.example.com/v1").await;
    assert_eq!(s3.as_u16(), 200);
    (v2, v3)
}

fn def_of<'a>(doc: &'a Value, root: &str) -> &'a Value {
    doc.pointer(root)
        .and_then(|m| m.as_object())
        .and_then(|m| m.iter().find(|(k, _)| k.ends_with(".v1.Zed")))
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("no Zed definition under {root}: {doc}"))
}

fn missing(def: &Value) -> Vec<String> {
    CASES
        .iter()
        .filter(|(p, k)| def.pointer(&format!("/properties/{p}/{k}")) != Some(&json!(0)))
        .map(|(p, k)| format!("{p}.{k}"))
        .collect()
}

#[tokio::test]
async fn explicit_zero_validations_published_in_openapi_v2() {
    let srv = TestApiServer::new();
    let (v2, _) = publish(&srv, "zeds", zero_props()).await;
    let m = missing(def_of(&v2, "/definitions"));
    assert!(m.is_empty(), "v2 dropped explicit zeros: {m:?}");
}

#[tokio::test]
async fn explicit_zero_validations_published_in_openapi_v3() {
    let srv = TestApiServer::new();
    let (_, v3) = publish(&srv, "zeds", zero_props()).await;
    let m = missing(def_of(&v3, "/components/schemas"));
    assert!(m.is_empty(), "v3 dropped explicit zeros: {m:?}");
}

#[tokio::test]
async fn absent_validations_not_published() {
    let srv = TestApiServer::new();
    let (v2, v3) = publish(&srv, "zeds", absent_props()).await;
    for (doc, root) in [(&v2, "/definitions"), (&v3, "/components/schemas")] {
        let def = def_of(doc, root);
        for (p, k) in CASES {
            assert!(
                def.pointer(&format!("/properties/{p}/{k}")).is_none(),
                "{root}: absent {p}.{k} must not be published"
            );
        }
    }
}
