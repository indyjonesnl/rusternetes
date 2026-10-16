//! ReferenceGrant (`gateway.networking.k8s.io/v1beta1`) served by the
//! api-server (#3149).
//!
//! `gateway.networking.k8s.io` is not in `../kubernetes`: ReferenceGrant is a
//! CRD of kubernetes-sigs/gateway-api
//! (`config/crd/standard/gateway.networking.k8s.io_referencegrants.yaml`).
//! The rules are therefore the apiextensions-apiserver custom-resource
//! strategy plus that CRD's schema (`from`/`to` required, 1..=16 items, the
//! `group`/`kind`/`namespace`/`name` patterns and lengths).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const GRANTS: &str = "/apis/gateway.networking.k8s.io/v1beta1/namespaces/default/referencegrants";

fn grant(name: &str) -> Value {
    json!({"apiVersion": "gateway.networking.k8s.io/v1beta1", "kind": "ReferenceGrant",
           "metadata": {"name": name},
           "spec": {
               "from": [{"group": "", "kind": "PersistentVolumeClaim", "namespace": "other"}],
               "to": [{"group": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot"}]}})
}

#[tokio::test]
async fn a_grant_round_trips_with_generation_one() {
    let api = TestApiServer::new();
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

    let (s, all) = api
        .get("/apis/gateway.networking.k8s.io/v1beta1/referencegrants")
        .await;
    assert_eq!(s, StatusCode::OK, "{all}");
    assert_eq!(all["items"].as_array().map(Vec::len), Some(1), "{all}");
}

#[tokio::test]
async fn a_spec_change_bumps_generation_and_delete_removes() {
    let api = TestApiServer::new();
    let (s, created) = api.post(GRANTS, &grant("g1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");

    let mut labelled = created.clone();
    labelled["metadata"]["labels"] = json!({"a": "b"});
    let (s, out) = api.put(&format!("{GRANTS}/g1"), &labelled).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");

    let mut changed = out.clone();
    changed["spec"]["to"][0]["name"] = json!("only-this");
    let (s, out) = api.put(&format!("{GRANTS}/g1"), &changed).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");

    let (s, out) = api.delete(&format!("{GRANTS}/g1")).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    let (s, _) = api.get(&format!("{GRANTS}/g1")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// The CRD schema: `from` and `to` are required with `minItems: 1`, at most 16.
#[tokio::test]
async fn from_and_to_are_required_and_bounded() {
    let api = TestApiServer::new();

    let mut no_from = grant("g1");
    no_from["spec"]["from"] = json!([]);
    let (s, out) = api.post(GRANTS, &no_from).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"].as_str().unwrap_or("").contains("spec.from"),
        "{out}"
    );

    let mut absent_from = grant("g1");
    absent_from["spec"].as_object_mut().unwrap().remove("from");
    let (s, out) = api.post(GRANTS, &absent_from).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap_or("")
            .contains("spec.from: Required value"),
        "{out}"
    );

    let mut no_to = grant("g1");
    no_to["spec"]["to"] = json!([]);
    let (s, out) = api.post(GRANTS, &no_to).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let mut many = grant("g1");
    many["spec"]["to"] = json!((0..17)
        .map(|_| json!({"group": "", "kind": "Secret"}))
        .collect::<Vec<_>>());
    let (s, out) = api.post(GRANTS, &many).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"].as_str().unwrap_or("").contains("spec.to"),
        "{out}"
    );
}

/// The CRD's `kind`, `namespace` and `group` patterns.
#[tokio::test]
async fn kind_namespace_and_group_patterns_are_enforced() {
    let api = TestApiServer::new();

    let mut bad_kind = grant("g1");
    bad_kind["spec"]["to"][0]["kind"] = json!("not a kind");
    let (s, out) = api.post(GRANTS, &bad_kind).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let mut bad_ns = grant("g1");
    bad_ns["spec"]["from"][0]["namespace"] = json!("Not_A_Namespace");
    let (s, out) = api.post(GRANTS, &bad_ns).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let mut bad_group = grant("g1");
    bad_group["spec"]["from"][0]["group"] = json!("Bad_Group");
    let (s, out) = api.post(GRANTS, &bad_group).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

#[tokio::test]
async fn discovery_lists_referencegrants() {
    let api = TestApiServer::new();
    let (s, out) = api.get("/apis/gateway.networking.k8s.io/v1beta1").await;
    assert_eq!(s, StatusCode::OK, "{out}");
    let res = out["resources"]
        .as_array()
        .and_then(|r| r.iter().find(|r| r["name"] == "referencegrants"))
        .unwrap_or_else(|| panic!("{out}"));
    assert_eq!(res["kind"], "ReferenceGrant", "{res}");
    assert_eq!(res["namespaced"], true, "{res}");
    assert_eq!(res["shortNames"], json!(["refgrant"]), "{res}");
}
