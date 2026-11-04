//! `[sig-api-machinery] CustomResourceDefinition Watch ... watch on custom
//! resource definition objects` (test/e2e/apimachinery/crd_watch.go:52).
//!
//! The spec's `waitForEvent` (watch.go:419-438) accepts an event only when
//! `apiequality.Semantic.DeepEqual(expectObject, actual.Object)`, where the
//! expected object is the CREATE response. A watch ADDED event must therefore
//! be semantically identical to what the create call returned.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const BASE: &str = "/apis/mygroup.example.com/v1beta1/noxus";

fn noxu_crd() -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "noxus.mygroup.example.com" },
        "spec": {
            "group": "mygroup.example.com",
            "scope": "Cluster",
            "names": { "plural": "noxus", "singular": "nonenglishnoxu",
                       "kind": "WishIHadChosenNoxu", "listKind": "NoxuItemList" },
            "versions": [{
                "name": "v1beta1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object",
                    "x-kubernetes-preserve-unknown-fields": true}}
            }]
        }
    })
}

#[tokio::test]
async fn watch_added_event_equals_create_response() {
    let api = TestApiServer::new();
    let (sc, out) = api
        .post(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            &noxu_crd(),
        )
        .await;
    assert_eq!(sc, StatusCode::CREATED, "{out}");
    api.wait_crd_established("noxus.mygroup.example.com").await;

    let watcher = api.clone();
    let watch = tokio::spawn(async move {
        let (_, _, bytes, _) = watcher
            .send_full(
                "GET",
                &format!("{BASE}?watch=true&timeoutSeconds=3&fieldSelector=metadata.name%3Dname1"),
                None,
                None,
                None,
            )
            .await;
        String::from_utf8(bytes).unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (sc, created) = api
        .post(
            BASE,
            &json!({
                "apiVersion": "mygroup.example.com/v1beta1",
                "kind": "WishIHadChosenNoxu",
                "metadata": { "name": "name1", "namespace": "" },
                "content": { "key": "value" },
                "num": { "num1": 9223372036854775807i64, "num2": 1000000 }
            }),
        )
        .await;
    assert_eq!(sc, StatusCode::CREATED, "{created}");

    let stream = watch.await.unwrap();
    let added: Value = stream
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find(|e| e["type"] == "ADDED")
        .unwrap_or_else(|| panic!("no ADDED event: {stream}"));
    assert_eq!(added["object"], created);
    // The client decodes JSON integers as int64 only when the text is an
    // integer literal, so both encodings must keep it verbatim.
    assert!(stream.contains("9223372036854775807"), "{stream}");
    assert!(created.to_string().contains("9223372036854775807"));
}
