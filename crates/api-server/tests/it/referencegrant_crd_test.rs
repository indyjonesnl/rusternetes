//! ReferenceGrant (`gateway.networking.k8s.io`) is NOT built in to
//! Kubernetes: Gateway API is CRDs only (kubernetes-sigs/gateway-api
//! `config/crd/standard/gateway.networking.k8s.io_referencegrants.yaml`; the
//! fixture is that CRD as shipped in the k3s traefik-crd chart, bundle v1.6.1).
//! These tests install the real CRD and drive the custom-resource API, as the
//! apiextensions-apiserver serves it, and check `/apis` lists the group ONCE
//! with every CRD version (customresource_discovery_controller.go:105-135,
//! :225-231, :284-288).

use axum::http::StatusCode;
use k3rs_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";
const GRANTS: &str = "/apis/gateway.networking.k8s.io/v1/namespaces/default/referencegrants";

fn crd() -> Value {
    serde_json::from_str(include_str!(
        "../fixtures/gateway.networking.k8s.io_referencegrants.crd.json"
    ))
    .expect("fixture is valid JSON")
}

fn grant(name: &str) -> Value {
    json!({"apiVersion": "gateway.networking.k8s.io/v1", "kind": "ReferenceGrant",
           "metadata": {"name": name},
           "spec": {
               "from": [{"group": "", "kind": "PersistentVolumeClaim", "namespace": "other"}],
               "to": [{"group": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot"}]}})
}

async fn installed() -> TestApiServer {
    let api = TestApiServer::new();
    let (s, out) = api.post(CRDS, &crd()).await;
    assert_eq!(
        s,
        StatusCode::CREATED,
        "the real CRD must be admitted: {out}"
    );
    api
}

#[tokio::test]
async fn the_real_referencegrant_crd_serves_grants_with_generation_one() {
    let api = installed().await;
    let (s, out) = api.post(GRANTS, &grant("g1")).await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["metadata"]["namespace"], "default", "{out}");

    let (s, got) = api.get(&format!("{GRANTS}/g1")).await;
    assert_eq!(s, StatusCode::OK, "{got}");
    assert_eq!(got["spec"]["from"][0]["namespace"], "other", "{got}");

    let (s, list) = api.get(GRANTS).await;
    assert_eq!(s, StatusCode::OK, "{list}");
    assert_eq!(list["kind"], "ReferenceGrantList", "{list}");
    assert_eq!(list["items"].as_array().map(Vec::len), Some(1), "{list}");

    // The v1beta1 version of the same CRD serves the same objects.
    let (s, all) = api
        .get("/apis/gateway.networking.k8s.io/v1beta1/referencegrants")
        .await;
    assert_eq!(s, StatusCode::OK, "{all}");
    assert_eq!(all["items"].as_array().map(Vec::len), Some(1), "{all}");
}

#[tokio::test]
async fn the_crd_schema_rejects_a_grant_without_from() {
    let api = installed().await;
    let mut bad = grant("bad");
    bad["spec"].as_object_mut().unwrap().remove("from");
    let (s, out) = api.post(GRANTS, &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// No native group: before the CRD is installed the group does not exist.
#[tokio::test]
async fn there_is_no_built_in_gateway_group() {
    let api = TestApiServer::new();
    let (s, groups) = api.get("/apis").await;
    assert_eq!(s, StatusCode::OK, "{groups}");
    assert!(
        !groups["groups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["name"] == "gateway.networking.k8s.io"),
        "{groups}"
    );
    // Nothing about the group is special: it answers as any other group no
    // CRD defines.
    let (s, _) = api.get("/apis/gateway.networking.k8s.io/v1beta1").await;
    let (unknown, _) = api.get("/apis/nothing.example.com/v1beta1").await;
    assert_eq!(s, unknown);
    let (s, _) = api
        .get("/apis/gateway.networking.k8s.io/v1beta1/referencegrants")
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// One `APIGroup`, both CRD versions, highest version preferred.
#[tokio::test]
async fn the_group_is_listed_once_with_every_crd_version() {
    let api = installed().await;
    let (s, groups) = api.get("/apis").await;
    assert_eq!(s, StatusCode::OK, "{groups}");
    let gw: Vec<&Value> = groups["groups"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|g| g["name"] == "gateway.networking.k8s.io")
        .collect();
    assert_eq!(gw.len(), 1, "exactly one APIGroup per group: {groups}");
    let versions: Vec<&str> = gw[0]["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["version"].as_str().unwrap())
        .collect();
    assert_eq!(versions, ["v1", "v1beta1"], "{groups}");
    assert_eq!(gw[0]["preferredVersion"]["version"], "v1", "{groups}");
}
