//! Discovery of `admissionregistration.k8s.io/v1beta1`
//! `mutatingadmissionpolicies` (#2799).
//!
//! Upstream: pkg/registry/admissionregistration/rest/storage_apiserver.go:187-205
//! installs the v1beta1 storage only while the version is enabled
//! (the `MutatingAdmissionPolicy` gate, off in v1.35: kube_features.go:2049),
//! and discovery lists only versions that have storage. Categories:
//! mutatingadmissionpolicy{,binding}/storage/storage.go `Categories()`.

use axum::http::StatusCode;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::Value;

const GV: &str = "/apis/admissionregistration.k8s.io/v1beta1";

fn versions(group: &Value) -> Vec<String> {
    group["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["version"].as_str().unwrap().to_string())
        .collect()
}

async fn group_of(api: &TestApiServer) -> Value {
    let (_, groups) = api.get("/apis").await;
    groups["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["name"] == "admissionregistration.k8s.io")
        .unwrap()
        .clone()
}

#[tokio::test]
#[serial_test::serial]
async fn v1beta1_is_not_advertised_while_the_gate_is_off() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, false);
    let api = TestApiServer::new();
    assert_eq!(versions(&group_of(&api).await), vec!["v1"]);
    let (status, _) = api.get(GV).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, group) = api.get("/apis/admissionregistration.k8s.io/").await;
    assert_eq!(status, StatusCode::OK, "{group}");
    assert_eq!(versions(&group), vec!["v1"]);
}

#[tokio::test]
#[serial_test::serial]
async fn v1beta1_is_advertised_under_the_gate() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let api = TestApiServer::new();
    let group = group_of(&api).await;
    assert_eq!(versions(&group), vec!["v1", "v1beta1"], "{group}");
    assert_eq!(group["preferredVersion"]["version"], "v1");

    let (status, list) = api.get(GV).await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list["groupVersion"], "admissionregistration.k8s.io/v1beta1");
    let res = list["resources"].as_array().unwrap();
    let names: Vec<&str> = res.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        vec![
            "mutatingadmissionpolicies",
            "mutatingadmissionpolicybindings"
        ],
        "{list}"
    );
    for r in res {
        assert_eq!(r["namespaced"], false);
        assert_eq!(r["categories"], serde_json::json!(["api-extensions"]));
    }
    assert_eq!(res[0]["kind"], "MutatingAdmissionPolicy");
    assert_eq!(res[1]["kind"], "MutatingAdmissionPolicyBinding");

    let (status, group) = api.get("/apis/admissionregistration.k8s.io/").await;
    assert_eq!(status, StatusCode::OK, "{group}");
    assert_eq!(versions(&group), vec!["v1", "v1beta1"]);
}

#[tokio::test]
#[serial_test::serial]
async fn aggregated_discovery_follows_the_gate() {
    let accept = [(
        "accept",
        "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
    )];
    for on in [false, true] {
        let _g = with_feature(Feature::MutatingAdmissionPolicy, on);
        let api = TestApiServer::new();
        let (_, _, _, body) = api.send_with_headers("GET", "/apis", &accept, None).await;
        let group = body["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["metadata"]["name"] == "admissionregistration.k8s.io")
            .unwrap();
        let vs: Vec<&str> = group["versions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["version"].as_str().unwrap())
            .collect();
        if on {
            assert_eq!(vs, vec!["v1", "v1beta1"], "{group}");
            let beta = &group["versions"][1];
            let rs: Vec<&str> = beta["resources"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["resource"].as_str().unwrap())
                .collect();
            assert_eq!(
                rs,
                vec![
                    "mutatingadmissionpolicies",
                    "mutatingadmissionpolicybindings"
                ]
            );
        } else {
            assert_eq!(vs, vec!["v1"], "{group}");
        }
    }
}

#[tokio::test]
#[serial_test::serial]
async fn openapi_v3_lists_v1beta1_only_under_the_gate() {
    for on in [false, true] {
        let _g = with_feature(Feature::MutatingAdmissionPolicy, on);
        let api = TestApiServer::new();
        let (status, body) = api.get("/openapi/v3").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.pointer("/paths/apis~1admissionregistration.k8s.io~1v1beta1")
                .is_some(),
            on
        );
    }
}
