//! #3638: how a failed patch and an unparseable watch `resourceVersion` are
//! reported.
//!
//! Upstream sources, all `release-1.35`:
//! * `apiserver/pkg/endpoints/handlers/patch.go:404-415` (`applyJSPatch`):
//!   more than `maxJSONPatchOperations` (:64, 10000) is
//!   `NewRequestEntityTooLargeError`.
//! * `patch.go:425-433` (merge patch): `jsonpatch.ErrBadJSONPatch` is
//!   `NewBadRequest`.
//! * `patch.go:567-569` (`strategicPatchObject`): an undecodable SMP document
//!   is `NewBadRequest(err.Error())`.
//! * `patch.go:797-806` (`interpretStrategicMergePatchError`): the sentinel
//!   errors are 400 / 422, every other error is returned raw (500).
//! * `storage/etcd3/store.go:967-970`, `storage/cacher/cacher.go:513-516`:
//!   `Watch` returns `ParseResourceVersion`'s `InvalidError` raw. Nothing in
//!   release-1.35 calls `storage/errors.InterpretWatchError`, so the endpoint
//!   renders a plain error: 500, no reason.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CM: &str = "/api/v1/namespaces/default/configmaps";
const MERGE: &str = "application/merge-patch+json";
const SMP: &str = "application/strategic-merge-patch+json";
const JSON_PATCH: &str = "application/json-patch+json";

async fn seed_cm(api: &TestApiServer) {
    let (s, out) = api
        .post(
            CM,
            &json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"c"},"data":{"a":"1"}}),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
}

async fn patch(api: &TestApiServer, uri: &str, ct: &str, body: &str) -> (StatusCode, Value) {
    let (status, _h, _b, value) = api
        .send_full("PATCH", uri, Some(ct), None, Some(body.as_bytes().to_vec()))
        .await;
    (status, value)
}

fn add_ops(n: usize) -> String {
    let ops: Vec<Value> = (0..n)
        .map(|_| json!({"op":"add","path":"/data/k","value":"v"}))
        .collect();
    serde_json::to_string(&ops).unwrap()
}

#[tokio::test]
async fn json_patch_over_ten_thousand_operations_is_413() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    let (status, out) = patch(&api, &format!("{CM}/c"), JSON_PATCH, &add_ops(10001)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{out}");
    assert_eq!(out["reason"], "RequestEntityTooLarge", "{out}");
    assert_eq!(out["code"], 413, "{out}");
    assert_eq!(
        out["message"],
        "Request entity too large: The allowed maximum operations in a JSON patch is 10000, got 10001"
    );
}

#[tokio::test]
async fn json_patch_of_exactly_ten_thousand_operations_applies() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    let (status, out) = patch(&api, &format!("{CM}/c"), JSON_PATCH, &add_ops(10000)).await;
    assert_eq!(status, StatusCode::OK, "{out}");
}

#[tokio::test]
async fn merge_patch_that_is_not_a_json_document_is_400_invalid_json_patch() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    // The default `fieldValidation` is Warn, which decodes the document
    // strictly first (patch.go:421-427); Ignore reaches `jsonpatch.MergePatch`.
    for body in ["{", "null", "5", r#""x""#] {
        let uri = format!("{CM}/c?fieldValidation=Ignore");
        let (status, out) = patch(&api, &uri, MERGE, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {out}");
        assert_eq!(out["reason"], "BadRequest", "{body}: {out}");
        assert_eq!(out["message"], "Invalid JSON Patch", "{body}: {out}");
    }
}

#[tokio::test]
async fn strict_merge_patch_decode_failure_keeps_the_error_decoding_prefix() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    let (status, out) = patch(&api, &format!("{CM}/c?fieldValidation=Strict"), MERGE, "{").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap_or_default()
            .starts_with("error decoding patch: "),
        "{out}"
    );
}

#[tokio::test]
async fn smp_document_that_does_not_decode_is_400_without_a_prefix() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    let (status, out) = patch(&api, &format!("{CM}/c"), SMP, "{").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert_eq!(out["reason"], "BadRequest", "{out}");
    assert!(
        !out["message"]
            .as_str()
            .unwrap_or_default()
            .starts_with("error decoding patch"),
        "{out}"
    );
}

#[tokio::test]
async fn smp_bad_retain_keys_format_is_400() {
    let api = TestApiServer::new();
    seed_cm(&api).await;
    let (status, out) = patch(
        &api,
        &format!("{CM}/c"),
        SMP,
        r#"{"data":{"$retainKeys":"notalist"}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert_eq!(out["reason"], "BadRequest", "{out}");
    assert_eq!(out["message"], "invalid patch format of retainKeys");
}

#[tokio::test]
async fn smp_error_that_is_not_a_sentinel_is_returned_raw_as_500() {
    let api = TestApiServer::new();
    let (s, out) = api
        .post(
            "/api/v1/namespaces/default/pods",
            &json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},
                "spec":{"containers":[{"name":"c1","image":"a"},{"name":"c2","image":"b"}]}}),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{out}");
    let (status, out) = patch(
        &api,
        "/api/v1/namespaces/default/pods/p",
        SMP,
        r#"{"spec":{"$setElementOrder/containers":[{"name":"c1"},{"name":"zzz"}],"containers":[{"name":"c2","image":"x"}]}}"#,
    )
    .await;
    // `fmt.Errorf("The order in patch list ...")` reaches
    // ErrorToAPIStatus's default arm: 500, no reason.
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{out}");
    assert!(out.get("reason").is_none_or(|r| r == ""), "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap_or_default()
            .contains("doesn't match $setElementOrder list"),
        "{out}"
    );
}

#[tokio::test]
async fn watch_with_an_unparseable_resource_version_is_a_plain_500() {
    let api = TestApiServer::new();
    let (status, out) = api
        .get(&format!("{CM}?watch=true&resourceVersion=abc"))
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{out}");
    assert!(out.get("reason").is_none_or(|r| r == ""), "{out}");
    assert_eq!(
        out["message"],
        r#"resourceVersion: Invalid value: "abc": strconv.ParseUint: parsing "abc": invalid syntax"#
    );
}
