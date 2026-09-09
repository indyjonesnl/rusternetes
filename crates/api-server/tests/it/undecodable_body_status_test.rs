//! A request body serde cannot decode must come back as a `Status`, not as
//! axum's plain-text rejection.
//!
//! Upstream decodes the body in the create/update handlers and runs any failure
//! through `transformDecodeError`
//! (staging/src/k8s.io/apiserver/pkg/endpoints/handlers/rest.go:245-256), which
//! always produces `errors.NewBadRequest(...)` — a `Status` with
//! `reason: BadRequest` and HTTP **400**:
//!
//! ```go
//! if gvk != nil && len(gvk.Kind) > 0 {
//!     return errors.NewBadRequest(fmt.Sprintf("%s in version %q cannot be handled as a %s: %v", gvk.Kind, gvk.Version, objGVK.Kind, baseErr))
//! }
//! summary := summarizeData(body, 30)
//! return errors.NewBadRequest(fmt.Sprintf("the object provided is unrecognized (must be of type %s): %v (%s)", objGVK.Kind, baseErr, summary))
//! ```
//!
//! We returned axum's `JsonRejection` verbatim:
//!
//! ```text
//! 422 Unprocessable Entity
//! Failed to deserialize the JSON body into the target type: ...
//! ```
//!
//! `text/plain`, no `kind`, no `reason`, no `code` — a client-go client cannot
//! classify it and `kubectl` prints the raw string. The status code is wrong
//! too: 422/Invalid is for an object that decoded and then failed validation;
//! an undecodable body is 400/BadRequest (#1915).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

async fn post_raw(uri: &str, body: &str) -> (u16, String) {
    let api = TestApiServer::new();
    let (status, _headers, bytes, _) = api
        .send_full(
            "POST",
            uri,
            Some("application/json"),
            None,
            Some(body.as_bytes().to_vec()),
        )
        .await;
    (status.as_u16(), String::from_utf8_lossy(&bytes).to_string())
}

fn as_status(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| {
        panic!(
            "error body is not JSON ({e}), so no client can classify it. \
             Upstream answers an undecodable body with NewBadRequest \
             (endpoints/handlers/rest.go:245-256). Body: {body}"
        )
    })
}

/// The repro from #1915: `data` is `map[string]string`, so a number fails to
/// decode.
#[tokio::test]
async fn a_wrong_typed_field_returns_a_badrequest_status() {
    let (code, body) = post_raw(
        "/api/v1/namespaces/default/configmaps",
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x"},"data":{"k":1}}"#,
    )
    .await;

    let parsed = as_status(&body);
    assert_eq!(parsed["kind"], "Status", "{body}");
    assert_eq!(parsed["apiVersion"], "v1", "{body}");
    assert_eq!(parsed["status"], "Failure", "{body}");
    assert_eq!(
        parsed["reason"], "BadRequest",
        "an undecodable body is BadRequest, not Invalid: {body}"
    );
    assert_eq!(
        code, 400,
        "422/Invalid is for an object that decoded and then failed validation; \
         a body that never decoded is 400/BadRequest: {body}"
    );
    assert_eq!(parsed["code"], 400, "{body}");
}

/// With a `kind` in the body, upstream's message names the kind, the version
/// and the target type. Pinned as a shape rather than byte-for-byte: the
/// trailing `%v` is the decoder's own error, which is serde's here and Go's
/// there.
#[tokio::test]
async fn the_message_follows_upstreams_cannot_be_handled_as_wording() {
    let (_code, body) = post_raw(
        "/api/v1/namespaces/default/configmaps",
        r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x"},"data":{"k":1}}"#,
    )
    .await;

    let parsed = as_status(&body);
    let msg = parsed["message"].as_str().unwrap_or_default();
    assert!(
        msg.starts_with("ConfigMap in version \"v1\" cannot be handled as a ConfigMap:"),
        "expected upstream's transformDecodeError wording \
         (`%s in version %q cannot be handled as a %s: %v`, rest.go:252), got: {msg}"
    );
}

/// `apps/v1` must contribute `v1` — upstream formats `gvk.Version`, not the
/// whole `apiVersion`.
#[tokio::test]
async fn a_grouped_api_version_reports_only_the_version_part() {
    let (_code, body) = post_raw(
        "/apis/apps/v1/namespaces/default/deployments",
        r#"{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"x"},"spec":{"replicas":"three"}}"#,
    )
    .await;

    let parsed = as_status(&body);
    let msg = parsed["message"].as_str().unwrap_or_default();
    assert!(
        msg.starts_with("Deployment in version \"v1\" cannot be handled as a Deployment:"),
        "gvk.Version is the version alone, not the group/version pair: {msg}"
    );
}

/// Without a `kind`, upstream takes the other branch and appends a summary of
/// the body (`summarizeData(body, 30)`, rest.go:355-370).
#[tokio::test]
async fn a_body_without_a_kind_reports_the_unrecognized_object_wording() {
    let (code, body) = post_raw(
        "/api/v1/namespaces/default/configmaps",
        r#"{"metadata":{"name":"x"},"data":{"k":1}}"#,
    )
    .await;

    let parsed = as_status(&body);
    assert_eq!(code, 400, "{body}");
    let msg = parsed["message"].as_str().unwrap_or_default();
    assert!(
        msg.starts_with("the object provided is unrecognized (must be of type ConfigMap):"),
        "expected the no-kind branch of transformDecodeError (rest.go:255), got: {msg}"
    );
    // summarizeData truncates at 30 bytes and appends " ...".
    assert!(
        msg.ends_with(r#"({"metadata":{"name":"x"},"data ...)"#),
        "the message must end with summarizeData's 30-byte prefix of the body \
         plus \" ...\" (rest.go:360-361): {msg}"
    );
}

/// Not JSON at all.
#[tokio::test]
async fn a_body_that_is_not_json_returns_a_badrequest_status() {
    let (code, body) = post_raw("/api/v1/namespaces/default/configmaps", "this is not json").await;

    let parsed = as_status(&body);
    assert_eq!(code, 400, "{body}");
    assert_eq!(parsed["reason"], "BadRequest", "{body}");
}

/// The guard against over-reach: a body that decodes cleanly must still be
/// created, and a decoded object that fails *validation* keeps 422/Invalid.
#[tokio::test]
async fn a_decodable_body_is_unaffected_and_validation_still_returns_invalid() {
    let api = TestApiServer::new();

    let (status, body) = api
        .post(
            "/api/v1/namespaces/default/configmaps",
            &json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"ok"},"data":{"k":"v"}}),
        )
        .await;
    assert!(
        status.is_success(),
        "a valid body must still create: {body}"
    );

    // Decodes fine (name is a string), fails name validation.
    let (status, body) = api
        .post(
            "/api/v1/namespaces/default/configmaps",
            &json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"Not A Valid Name"}}),
        )
        .await;
    assert_eq!(
        status.as_u16(),
        422,
        "a decoded object that fails validation stays 422/Invalid -- only an \
         undecodable body becomes 400/BadRequest: {body}"
    );
}

// ---------------------------------------------------------------------------
// the sweep
// ---------------------------------------------------------------------------

/// Every creatable resource must answer an undecodable body the same way.
///
/// The response shape cannot be a property of which handler you happen to hit.
/// Rusternetes has three decode mechanisms — the `DumpingJson` extractor, a
/// hand-rolled `serde_json::from_slice` on a `Bytes` body, and the generic
/// patch path — and they disagreed: the extractor returned axum's plain-text
/// 422, most hand-rolled sites returned `Error::InvalidResource` (422/Invalid),
/// and a few returned `Error::BadRequest` (400). Upstream has one answer for
/// all of them, `transformDecodeError` → `NewBadRequest`
/// (endpoints/handlers/rest.go:245-256).
///
/// The list comes from the server's own discovery, so a resource added later
/// with a fourth decode mechanism fails this test rather than slipping through.
///
/// The probe body sets `metadata` to a string. `metadata` is a struct on every
/// resource, so this fails to decode for all of them and for the same reason —
/// no per-resource fixture to keep in step.
use std::collections::{BTreeMap, BTreeSet};

type Gvr = (String, String, String);

async fn creatable_resources(s: &TestApiServer) -> BTreeMap<Gvr, bool> {
    let mut gvs: Vec<(String, String)> = vec![(String::new(), "v1".to_string())];
    let (st, apis) = s.get("/apis").await;
    assert!(st.is_success(), "GET /apis: {st} {apis}");
    for g in apis["groups"].as_array().cloned().unwrap_or_default() {
        let name = g["name"].as_str().unwrap_or_default().to_string();
        for v in g["versions"].as_array().cloned().unwrap_or_default() {
            gvs.push((
                name.clone(),
                v["version"].as_str().unwrap_or_default().to_string(),
            ));
        }
    }

    let mut out = BTreeMap::new();
    for (group, version) in gvs {
        let uri = if group.is_empty() {
            format!("/api/{version}")
        } else {
            format!("/apis/{group}/{version}")
        };
        let (st, list) = s.get(&uri).await;
        assert!(st.is_success(), "GET {uri}: {st} {list}");
        for r in list["resources"].as_array().cloned().unwrap_or_default() {
            let name = r["name"].as_str().unwrap_or_default();
            if name.contains('/') {
                continue;
            }
            let verbs: BTreeSet<&str> = r["verbs"]
                .as_array()
                .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
                .unwrap_or_default();
            if !verbs.contains("create") {
                continue;
            }
            out.insert(
                (group.clone(), version.clone(), name.to_string()),
                r["namespaced"].as_bool().unwrap_or(false),
            );
        }
    }
    out
}

fn collection_uri(group: &str, version: &str, resource: &str, namespaced: bool) -> String {
    let root = if group.is_empty() {
        format!("/api/{version}")
    } else {
        format!("/apis/{group}/{version}")
    };
    if namespaced {
        format!("{root}/namespaces/default/{resource}")
    } else {
        format!("{root}/{resource}")
    }
}

#[tokio::test]
async fn every_creatable_resource_rejects_an_undecodable_body_with_a_badrequest_status() {
    let api = TestApiServer::new();
    let resources = creatable_resources(&api).await;
    assert!(
        resources.len() > 60,
        "discovery returned only {} creatable resources -- the sweep would be \
         nearly vacuous",
        resources.len()
    );

    let mut offenders: Vec<String> = Vec::new();
    let mut swept = 0usize;

    for ((group, version, resource), namespaced) in &resources {
        let uri = collection_uri(group, version, resource, *namespaced);
        // `metadata` is an object on every resource; a string cannot decode.
        let body = r#"{"metadata":"not-an-object"}"#;
        let (status, _headers, bytes, _) = api
            .send_full(
                "POST",
                &uri,
                Some("application/json"),
                None,
                Some(body.into()),
            )
            .await;
        let text = String::from_utf8_lossy(&bytes).to_string();
        swept += 1;

        let parsed: Option<Value> = serde_json::from_str(&text).ok();
        let Some(parsed) = parsed else {
            offenders.push(format!("{uri}: body is not JSON: {text}"));
            continue;
        };
        if parsed["kind"] != "Status" {
            offenders.push(format!("{uri}: not a Status: {text}"));
            continue;
        }
        if parsed["reason"] != "BadRequest" || status.as_u16() != 400 {
            offenders.push(format!(
                "{uri}: got {} / reason={}, want 400 / BadRequest",
                status.as_u16(),
                parsed["reason"]
            ));
        }
    }

    assert!(
        offenders.is_empty(),
        "{} of {swept} creatable resources answer an undecodable body with \
         something other than a 400 BadRequest Status. Upstream runs every \
         decode failure through transformDecodeError → NewBadRequest \
         (endpoints/handlers/rest.go:245-256); 422/Invalid is for an object \
         that decoded and then failed validation.\n\n{}",
        offenders.len(),
        offenders.join("\n")
    );
}
