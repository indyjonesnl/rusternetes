//! A body upstream decodes must decode here too.
//!
//! Go structs have no required JSON fields. `decoder.Decode(body, &defaultGVK,
//! obj)` (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go`)
//! fills an absent key with the type's zero value, and **validation** — not the
//! decoder — is what rejects it, with a `Status` naming the field path:
//!
//! ```text
//! POST /apis/apps/v1/namespaces/default/deployments
//! {"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"x"}}
//!
//! 422 spec.selector: Required value
//! ```
//!
//! Rusternetes answered 400 BadRequest to that body for thirty-seven of the
//! sixty-one creatable resources, because the Rust field was a bare non-`Option`
//! with no `#[serde(default)]`:
//!
//! ```text
//! 400 Deployment in version "v1" cannot be handled as a Deployment:
//!     missing field `spec` at line 1 column 79
//! ```
//!
//! Different status, different `reason`, no `details.causes`, no field path —
//! and for the strategies that allow create-on-update, a different outcome
//! (#1931).
//!
//! The sweep enumerates resources from the server's own discovery, so a
//! resource added later is covered without editing this file.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

type Gvr = (String, String, String);

async fn creatable_resources(s: &TestApiServer) -> BTreeMap<Gvr, (bool, String)> {
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
                (
                    r["namespaced"].as_bool().unwrap_or(false),
                    r["kind"].as_str().unwrap_or_default().to_string(),
                ),
            );
        }
    }
    out
}

/// `transformDecodeError`'s wording — the decoder never ran the object past
/// validation.
fn is_decode_failure(answer: &Value) -> bool {
    let msg = answer["message"].as_str().unwrap_or_default();
    msg.contains("cannot be handled as a")
        || msg.contains("the object provided is unrecognized")
        || msg.contains("failed to decode")
        || msg.contains("Failed to deserialize")
}

#[tokio::test]
async fn a_create_body_of_only_a_name_reaches_validation() {
    let api = TestApiServer::new();
    let resources = creatable_resources(&api).await;
    assert!(
        resources.len() > 50,
        "discovery returned only {} creatable resources -- the sweep would be \
         nearly vacuous",
        resources.len()
    );

    let mut undecodable: Vec<String> = Vec::new();
    let mut reached = 0usize;

    for ((group, version, resource), (namespaced, kind)) in &resources {
        let root = if group.is_empty() {
            format!("/api/{version}")
        } else {
            format!("/apis/{group}/{version}")
        };
        let base = if *namespaced {
            format!("{root}/namespaces/default/{resource}")
        } else {
            format!("{root}/{resource}")
        };
        let api_version = if group.is_empty() {
            version.clone()
        } else {
            format!("{group}/{version}")
        };
        let body = json!({
            "apiVersion": api_version,
            "kind": kind,
            "metadata": { "name": "minimal-body-probe" },
        });

        let (status, answer) = api
            .send("POST", &base, Some("application/json"), Some(&body))
            .await;
        if is_decode_failure(&answer) {
            undecodable.push(format!(
                "{api_version} {resource} -> {} {}",
                status.as_u16(),
                answer["message"]
            ));
        } else {
            reached += 1;
        }
    }

    assert!(
        undecodable.is_empty(),
        "{} resource(s) answered a body of only `metadata.name` with a decode \
         failure. Upstream decodes an absent key to the zero value and lets \
         validation reject it, with a field path a client can act on. Add \
         `#[serde(default)]` to the field (and `Default` down its type tree) -- \
         and make sure a validator rejects the zero value, or the object is \
         silently accepted instead.\n\nOffenders:\n  {}",
        undecodable.len(),
        undecodable.join("\n  ")
    );
    assert!(
        reached >= 55,
        "only {reached} resources got past the decoder -- the sweep stopped \
         reaching the server and would pass vacuously"
    );
}

/// The repro from #1931, spelled out with the field path upstream produces.
#[tokio::test]
async fn a_deployment_with_no_spec_is_invalid_not_bad_request() {
    let api = TestApiServer::new();
    let (status, answer) = api
        .send(
            "POST",
            "/apis/apps/v1/namespaces/default/deployments",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": { "name": "no-spec" },
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 422, "{answer}");
    assert_eq!(answer["reason"], json!("Invalid"));
    assert_eq!(
        answer["details"]["causes"][0]["field"],
        json!("spec.selector")
    );

    // And it was not persisted on the way out.
    let (status, _) = api
        .get("/apis/apps/v1/namespaces/default/deployments/no-spec")
        .await;
    assert_eq!(status.as_u16(), 404, "the rejected create still stored it");
}

/// The zero value of an enum with no zero variant. `spec.scope` decodes to
/// `ResourceScope::Unspecified` -- upstream's `""` -- and
/// `validateEnumStrings(..., required=true)`
/// (`apiextensions-apiserver/pkg/apis/apiextensions/validation/validation.go:364,502-515`)
/// answers `Required`, so the object must not be stored.
#[tokio::test]
async fn a_crd_with_no_scope_is_required_not_bad_request() {
    let api = TestApiServer::new();
    let (status, answer) = api
        .send(
            "POST",
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "CustomResourceDefinition",
                "metadata": { "name": "widgets.example.com" },
                "spec": {
                    "group": "example.com",
                    "names": { "plural": "widgets", "singular": "widget", "kind": "Widget" },
                    "versions": [ { "name": "v1", "served": true, "storage": true } ],
                },
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 422, "{answer}");
    assert_eq!(answer["reason"], json!("Invalid"));
    assert_eq!(answer["details"]["causes"][0]["field"], json!("spec.scope"));
}

/// The same shape for `deletionPolicy`, whose invented default would have been
/// `Delete` -- i.e. deleting the backing snapshot.
#[tokio::test]
async fn a_volumesnapshotclass_with_no_deletion_policy_is_required() {
    let api = TestApiServer::new();
    let (status, answer) = api
        .send(
            "POST",
            "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "snapshot.storage.k8s.io/v1",
                "kind": "VolumeSnapshotClass",
                "metadata": { "name": "csi-snapclass" },
                "driver": "csi.example.com",
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 422, "{answer}");
    assert_eq!(answer["reason"], json!("Invalid"));
    assert_eq!(
        answer["details"]["causes"][0]["field"],
        json!("deletionPolicy")
    );

    // The valid form still works, so the check is not a blanket rejection.
    let (status, answer) = api
        .send(
            "POST",
            "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "snapshot.storage.k8s.io/v1",
                "kind": "VolumeSnapshotClass",
                "metadata": { "name": "csi-snapclass" },
                "driver": "csi.example.com",
                "deletionPolicy": "Delete",
            })),
        )
        .await;
    assert!(status.is_success(), "{status} {answer}");
    assert_eq!(answer["deletionPolicy"], json!("Delete"));
}
