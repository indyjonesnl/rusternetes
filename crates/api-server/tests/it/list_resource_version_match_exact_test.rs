//! #2684: `resourceVersionMatch=Exact` reads the collection AS OF that
//! revision. Upstream `ValidateListOptions` sets `withRev = parsedRV` for
//! Exact (`staging/src/k8s.io/apiserver/pkg/storage/interfaces.go:374-375`),
//! `GetList` ranges at that revision (`etcd3/store.go:781-786`) and stamps the
//! list with it (`UpdateList(..., withRev, ...)`, `:898`). Mirrors
//! `RunTestList` "resource version of second write, match=Exact"
//! (`storage/testing/store_tests.go:1580`).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CMS: &str = "/api/v1/namespaces/default/configmaps";

fn cm(name: &str) -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name}, "data": {"k": "v"}})
}

fn names(list: &Value) -> Vec<String> {
    list["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn exact_lists_the_collection_at_that_revision() {
    let api = TestApiServer::new();
    let (st, one) = api.post(CMS, &cm("one")).await;
    assert_eq!(st, StatusCode::CREATED, "{one}");
    let rv1 = one["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();
    let (st, two) = api.post(CMS, &cm("two")).await;
    assert_eq!(st, StatusCode::CREATED, "{two}");
    let rv2 = two["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();

    let (st, l1) = api
        .get(&format!(
            "{CMS}?resourceVersion={rv1}&resourceVersionMatch=Exact"
        ))
        .await;
    assert_eq!(st, StatusCode::OK, "{l1}");
    assert_eq!(names(&l1), ["one"], "list at the first write's rv: {l1}");
    assert_eq!(l1["metadata"]["resourceVersion"], rv1.as_str());

    let (st, l2) = api
        .get(&format!(
            "{CMS}?resourceVersion={rv2}&resourceVersionMatch=Exact"
        ))
        .await;
    assert_eq!(st, StatusCode::OK, "{l2}");
    assert_eq!(names(&l2), ["one", "two"]);
    assert_eq!(l2["metadata"]["resourceVersion"], rv2.as_str());

    // NotOlderThan is a floor, not a pin: it returns the live collection.
    let (st, l3) = api
        .get(&format!(
            "{CMS}?resourceVersion={rv1}&resourceVersionMatch=NotOlderThan"
        ))
        .await;
    assert_eq!(st, StatusCode::OK, "{l3}");
    assert_eq!(names(&l3), ["one", "two"]);
}
