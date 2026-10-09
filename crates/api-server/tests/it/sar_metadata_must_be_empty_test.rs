//! A review's `metadata` must be empty.
//!
//! Upstream faults any non-zero `ObjectMeta` on all three review kinds:
//! `ValidateSubjectAccessReview` (`pkg/apis/authorization/validation/validation.go:63-72`),
//! `ValidateSelfSubjectAccessReview` (`:74-82`) and
//! `ValidateLocalSubjectAccessReview` (`:84-106`), the last of which clears
//! `Namespace` first and reads `must be empty except for namespace`. All three
//! clear `ManagedFields` before comparing.
//!
//! A review is a virtual object: `subjectaccessreview.REST.Create`
//! (`pkg/registry/authorization/subjectaccessreview/rest.go:63-96`) never calls
//! `rest.BeforeCreate`, so nothing fills a uid or a creation timestamp and the
//! validator sees exactly what the client sent. #1944 recorded a blocker for
//! porting the rule — "our handlers stamp metadata before validation runs" —
//! that turned out not to exist: the four review handlers touch no metadata,
//! and `ObjectMeta.uid`'s `#[serde(default = "generate_uid")]` named a function
//! whose whole body was `String::new()`.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

const SAR: &str = "/apis/authorization.k8s.io/v1/subjectaccessreviews";
const SSAR: &str = "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews";
const LSAR: &str = "/apis/authorization.k8s.io/v1/namespaces/default/localsubjectaccessreviews";

fn resource_attributes(namespace: &str) -> serde_json::Value {
    json!({ "namespace": namespace, "verb": "get", "resource": "pods" })
}

#[tokio::test]
async fn a_subject_access_review_with_metadata_is_invalid() {
    let api = TestApiServer::new();

    for (label, metadata) in [
        ("a name", json!({ "name": "review-1" })),
        ("a namespace", json!({ "namespace": "default" })),
        ("labels", json!({ "labels": { "a": "b" } })),
        (
            "a uid",
            json!({ "uid": "11111111-1111-1111-1111-111111111111" }),
        ),
    ] {
        let (status, body) = api
            .send(
                "POST",
                SAR,
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "authorization.k8s.io/v1",
                    "kind": "SubjectAccessReview",
                    "metadata": metadata,
                    "spec": {
                        "user": "alice",
                        "resourceAttributes": resource_attributes("default"),
                    },
                })),
            )
            .await;

        assert_eq!(status.as_u16(), 422, "{label}: {status} {body}");
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("metadata: Invalid value") && message.contains("must be empty"),
            "{label} must report the metadata rule, got: {message}"
        );
    }
}

#[tokio::test]
async fn a_self_subject_access_review_with_metadata_is_invalid() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            SSAR,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectAccessReview",
                "metadata": { "name": "review-1" },
                "spec": { "resourceAttributes": resource_attributes("default") },
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 422, "{status} {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("must be empty"),
        "{body}"
    );
}

/// The local review exempts `namespace` — `EnsureObjectNamespaceMatchesRequestNamespace`
/// (`staging/src/k8s.io/apiserver/pkg/registry/rest/meta.go:47-68`) has already
/// defaulted it from the path by the time the validator runs.
#[tokio::test]
async fn a_local_review_may_carry_its_namespace_but_nothing_else() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            LSAR,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "LocalSubjectAccessReview",
                "metadata": { "namespace": "default" },
                "spec": {
                    "user": "alice",
                    "resourceAttributes": resource_attributes("default"),
                },
            })),
        )
        .await;
    assert!(status.is_success(), "namespace is exempt: {status} {body}");

    let (status, body) = api
        .send(
            "POST",
            LSAR,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "LocalSubjectAccessReview",
                "metadata": { "namespace": "default", "name": "review-1" },
                "spec": {
                    "user": "alice",
                    "resourceAttributes": resource_attributes("default"),
                },
            })),
        )
        .await;
    assert_eq!(status.as_u16(), 422, "{status} {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("must be empty except for namespace"),
        "{body}"
    );
}

/// A namespace that contradicts the path is a 400 before any of this, per
/// `EnsureObjectNamespaceMatchesRequestNamespace`'s default arm.
#[tokio::test]
async fn a_local_review_whose_namespace_contradicts_the_path_is_a_bad_request() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            LSAR,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "LocalSubjectAccessReview",
                "metadata": { "namespace": "other" },
                "spec": {
                    "user": "alice",
                    "resourceAttributes": resource_attributes("default"),
                },
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 400, "{status} {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("does not match the namespace sent on the request"),
        "{body}"
    );
}

/// A review with no metadata is answered, and the answer invents none: nothing
/// upstream fills a uid or a creation timestamp on a virtual object.
#[tokio::test]
async fn a_review_with_no_metadata_is_answered_and_stays_empty() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            SAR,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SubjectAccessReview",
                "spec": {
                    "user": "alice",
                    "resourceAttributes": resource_attributes("default"),
                },
            })),
        )
        .await;

    assert!(status.is_success(), "{status} {body}");
    assert!(body["status"]["allowed"].is_boolean(), "{body}");
    assert!(
        body["metadata"]["uid"].is_null(),
        "nothing mints a uid for a virtual object: {body}"
    );
    assert!(
        body["metadata"]["creationTimestamp"].is_null(),
        "nothing stamps a creation timestamp either: {body}"
    );
}

/// #2929 (follow-up to #2375): a client-go core client (the kubelet's webhook
/// authorizer) posts native protobuf, and Go's generated marshaller writes
/// every non-pointer scalar even when it is zero
/// (`k8s.io/apimachinery/pkg/apis/meta/v1/generated.pb.go`,
/// `ObjectMeta.MarshalToSizedBuffer`). The decoded review must still count as
/// having empty metadata, exactly as it does in Go where a decoded struct
/// cannot tell absent from zero.
#[tokio::test]
async fn a_protobuf_review_with_go_zero_scalars_is_accepted() {
    fn ld(out: &mut Vec<u8>, field: u8, payload: &[u8]) {
        out.push((field << 3) | 2);
        out.push(payload.len() as u8);
        out.extend_from_slice(payload);
    }
    // ObjectMeta: name, generateName, namespace, uid, resourceVersion = "",
    // generation = 0, creationTimestamp = {} (always written).
    let meta = [
        0x0a, 0x00, 0x12, 0x00, 0x1a, 0x00, 0x2a, 0x00, 0x32, 0x00, 0x38, 0x00, 0x42, 0x00,
    ];
    let mut attrs = Vec::new();
    ld(&mut attrs, 2, b"get"); // verb
    ld(&mut attrs, 5, b"pods"); // resource
    let mut spec = Vec::new();
    ld(&mut spec, 1, &attrs); // resourceAttributes
    ld(&mut spec, 3, b"alice"); // user
    ld(&mut spec, 6, b""); // uid
    let status_msg = [0x08, 0x00, 0x10, 0x00, 0x1a, 0x00, 0x22, 0x00];
    let mut sar = Vec::new();
    ld(&mut sar, 1, &meta);
    ld(&mut sar, 2, &spec);
    ld(&mut sar, 3, &status_msg);

    let ct = "application/vnd.kubernetes.protobuf";
    let mut type_meta = Vec::new();
    ld(&mut type_meta, 1, b"authorization.k8s.io/v1");
    ld(&mut type_meta, 2, b"SubjectAccessReview");
    let mut unknown = Vec::new();
    ld(&mut unknown, 1, &type_meta);
    ld(&mut unknown, 2, &sar);
    ld(&mut unknown, 4, ct.as_bytes());
    let mut body = b"k8s\0".to_vec();
    body.extend_from_slice(&unknown);

    let api = TestApiServer::new();
    let (status, _headers, bytes, _) = api
        .send_with_headers(
            "POST",
            SAR,
            &[("content-type", ct), ("accept", "application/json")],
            Some(body),
        )
        .await;
    let text = String::from_utf8_lossy(&bytes);
    assert_eq!(status.as_u16(), 201, "{status} {text}");
}
