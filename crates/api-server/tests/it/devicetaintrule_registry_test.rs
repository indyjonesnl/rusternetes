//! DeviceTaintRule registry round trips (#3812): the cases of
//! `pkg/registry/resource/devicetaintrule/storage/storage_test.go`
//! (`TestCreate`, `TestUpdate`, `TestDelete`, `TestGet`, `TestList`,
//! `TestWatch`, `TestUpdateStatus`), driven through the HTTP surface instead
//! of `genericregistrytest` (cluster scoped, `ClusterScope()`).

use axum::http::StatusCode;
use futures::StreamExt;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;

const DTR: &str = "/apis/resource.k8s.io/v1alpha3/devicetaintrules";

/// `validNewDeviceTaint`: `Key: example.com/taint`, `NoExecute`, a TimeAdded
/// truncated to the second.
fn valid(name: &str) -> Value {
    json!({"apiVersion": "resource.k8s.io/v1alpha3", "kind": "DeviceTaintRule",
           "metadata": {"name": name},
           "spec": {"taint": {"key": "example.com/taint", "effect": "NoExecute",
                              "timeAdded": "2026-01-01T00:00:00Z"}}})
}

/// `TestCreate`: a valid object with `GenerateName` gets a generated name; an
/// invalid one (`*BadName!`) is rejected.
#[tokio::test]
#[serial_test::serial]
async fn create_generates_name_and_rejects_invalid() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let mut body = valid("foo");
    body["metadata"] = json!({"generateName": "foo"});
    let (code, out) = api.post(DTR, &body).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let name = out["metadata"]["name"].as_str().unwrap();
    assert!(name.starts_with("foo") && name.len() > 3, "{out}");
    assert!(out["metadata"].get("namespace").is_none(), "{out}");

    let (code, out) = api.post(DTR, &valid("*BadName!")).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// `TestUpdate`: an update that sets a label is stored.
#[tokio::test]
#[serial_test::serial]
async fn update_sets_label() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let (_, out) = api.post(DTR, &valid("foo")).await;
    let mut put = out.clone();
    put["metadata"]["labels"] = json!({"foo": "bar"});
    let (code, out) = api.put(&format!("{DTR}/foo"), &put).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let (_, got) = api.get(&format!("{DTR}/foo")).await;
    assert_eq!(got["metadata"]["labels"]["foo"], "bar");
}

/// `TestDelete` (`ReturnDeletedObject`) and `TestGet`.
#[tokio::test]
#[serial_test::serial]
async fn get_and_delete() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let (_, created) = api.post(DTR, &valid("foo")).await;
    let (code, got) = api.get(&format!("{DTR}/foo")).await;
    assert_eq!(code, StatusCode::OK, "{got}");
    assert_eq!(got["spec"], created["spec"]);

    let (code, out) = api.delete(&format!("{DTR}/foo")).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["name"], "foo", "{out}");
    assert_eq!(out["kind"], "DeviceTaintRule", "{out}");
    let (code, _) = api.get(&format!("{DTR}/foo")).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}

/// `TestList`: the list kind, items, and label/field selectors.
#[tokio::test]
#[serial_test::serial]
async fn list_with_selectors() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let mut foo = valid("foo");
    foo["metadata"]["labels"] = json!({"foo": "bar"});
    api.post(DTR, &foo).await;
    api.post(DTR, &valid("other")).await;

    let (code, list) = api.get(DTR).await;
    assert_eq!(code, StatusCode::OK, "{list}");
    assert_eq!(list["kind"], "DeviceTaintRuleList");
    assert_eq!(list["items"].as_array().unwrap().len(), 2, "{list}");

    let (_, list) = api.get(&format!("{DTR}?labelSelector=foo%3Dbar")).await;
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{list}");
    assert_eq!(items[0]["metadata"]["name"], "foo");

    let (_, list) = api
        .get(&format!("{DTR}?fieldSelector=metadata.name%3Dother"))
        .await;
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{list}");
    assert_eq!(items[0]["metadata"]["name"], "other");
}

/// First watch event on `uri` as `(type, object)`.
async fn first_event(api: &TestApiServer, uri: &str) -> (String, Value) {
    let resp = api.respond("GET", uri, None, None).await;
    assert_eq!(resp.status().as_u16(), 200, "watch {uri} must open");
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            if let Some(i) = buf.find('\n') {
                let ev: Value = serde_json::from_str(&buf[..i]).unwrap();
                return (
                    ev["type"].as_str().unwrap().to_string(),
                    ev["object"].clone(),
                );
            }
        }
        panic!("watch {uri} ended without an event");
    })
    .await
    .expect("no watch event within 5s")
}

/// `TestWatch`: matching labels/fields are delivered, non-matching are not
/// (the first delivered event is the matching object's ADDED).
#[tokio::test]
#[serial_test::serial]
async fn watch_filters_by_label_and_field() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();

    let mut tagged = valid("tagged");
    tagged["metadata"]["labels"] = json!({"foo": "bar"});
    let mut plain = valid("plain");
    plain["metadata"]["labels"] = json!({"foo": "other"});
    let (_, listed) = api.get(DTR).await;
    let rv = listed["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or("0")
        .to_string();
    api.post(DTR, &plain).await;
    api.post(DTR, &tagged).await;

    let (ty, obj) = first_event(
        &api,
        &format!("{DTR}?watch=true&resourceVersion={rv}&labelSelector=foo%3Dbar"),
    )
    .await;
    assert_eq!(
        (ty.as_str(), obj["metadata"]["name"].as_str()),
        ("ADDED", Some("tagged"))
    );

    let (ty, obj) = first_event(
        &api,
        &format!("{DTR}?watch=true&resourceVersion={rv}&fieldSelector=metadata.name%3Dplain"),
    )
    .await;
    assert_eq!(
        (ty.as_str(), obj["metadata"]["name"].as_str()),
        ("ADDED", Some("plain"))
    );
}

/// `TestUpdateStatus`: a status update through `/status` is stored.
#[tokio::test]
#[serial_test::serial]
async fn update_status_is_stored() {
    let _g = with_feature(Feature::DRADeviceTaintRules, true);
    let api = TestApiServer::new();
    let (_, mut obj) = api.post(DTR, &valid("foo")).await;
    obj["status"] = json!({"conditions": [{"type": "EvicitionInProgress", "status": "True",
        "reason": "PodsLeft", "message": "100 pods left",
        "lastTransitionTime": "2026-01-01T00:00:00Z"}]});
    let (code, out) = api.put(&format!("{DTR}/foo/status"), &obj).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let (_, got) = api.get(&format!("{DTR}/foo")).await;
    assert_eq!(got["status"]["conditions"], obj["status"]["conditions"]);
}
