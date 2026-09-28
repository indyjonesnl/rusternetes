//! A SubjectAccessReview the server cannot act on is `422 Invalid`, not `500`.
//!
//! Upstream validates the spec and returns `NewInvalid`
//! (`pkg/registry/authorization/subjectaccessreview/rest.go:75-77`):
//!
//! ```go
//! if errs := authorizationvalidation.ValidateSubjectAccessReview(subjectAccessReview); len(errs) > 0 {
//!     return nil, apierrors.NewInvalid(authorizationapi.Kind(subjectAccessReview.Kind), "", errs)
//! }
//! ```
//!
//! Rusternetes raised `Error::Internal` instead, so a body with neither
//! `resourceAttributes` nor `nonResourceAttributes` came back as a 500 with no
//! field path. A 500 reads as "the server broke" and `client-go` retries it;
//! the 422 is terminal and names the field (#1938).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn cause_fields(status: &Value) -> Vec<String> {
    status["details"]["causes"]
        .as_array()
        .map(|causes| {
            causes
                .iter()
                .filter_map(|c| c["field"].as_str())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// `spec.resourceAttributes == nil && spec.nonResourceAttributes == nil`
/// (`validation.go:36-38`), plus the user/groups rule that applies to the
/// non-self review only (`:39-41`).
#[tokio::test]
async fn a_subject_access_review_with_no_attributes_is_invalid() {
    let api = TestApiServer::new();
    let (status, body) = api
        .send(
            "POST",
            "/apis/authorization.k8s.io/v1/subjectaccessreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SubjectAccessReview",
                "metadata": {},
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 422, "{body}");
    assert_eq!(body["reason"], json!("Invalid"));
    let fields = cause_fields(&body);
    assert!(
        fields.contains(&"spec.resourceAttributes".to_string()),
        "expected a cause on spec.resourceAttributes, got {fields:?}: {body}"
    );
    assert!(
        fields.contains(&"spec.user".to_string()),
        "expected a cause on spec.user -- `at least one of user or group must be \
         specified` applies to the non-self review, got {fields:?}: {body}"
    );
}

/// Both set at once (`validation.go:33-35`).
#[tokio::test]
async fn a_subject_access_review_with_both_attributes_is_invalid() {
    let api = TestApiServer::new();
    let (status, body) = api
        .send(
            "POST",
            "/apis/authorization.k8s.io/v1/subjectaccessreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SubjectAccessReview",
                "spec": {
                    "user": "alice",
                    "resourceAttributes": { "verb": "get", "resource": "pods" },
                    "nonResourceAttributes": { "verb": "get", "path": "/healthz" },
                },
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 422, "{body}");
    let fields = cause_fields(&body);
    assert!(
        fields.contains(&"spec.nonResourceAttributes".to_string()),
        "expected a cause on spec.nonResourceAttributes, got {fields:?}: {body}"
    );
}

/// A well-formed review still answers 201 with a status, so the checks are not
/// a blanket rejection.
#[tokio::test]
async fn a_well_formed_subject_access_review_is_still_answered() {
    let api = TestApiServer::new();
    let (status, body) = api
        .send(
            "POST",
            "/apis/authorization.k8s.io/v1/subjectaccessreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SubjectAccessReview",
                "spec": {
                    "user": "alice",
                    "resourceAttributes": { "verb": "get", "resource": "pods", "namespace": "default" },
                },
            })),
        )
        .await;

    assert!(status.is_success(), "{status} {body}");
    assert!(body["status"]["allowed"].is_boolean(), "{body}");
}

/// The self review has no `user` field, so only the two attribute rules apply
/// (`ValidateSelfSubjectAccessReviewSpec`, `validation.go:49-60`).
#[tokio::test]
async fn a_self_subject_access_review_with_no_attributes_is_invalid() {
    let api = TestApiServer::new();
    let (status, body) = api
        .send(
            "POST",
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectAccessReview",
                "metadata": {},
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 422, "{body}");
    assert_eq!(body["reason"], json!("Invalid"));
    let fields = cause_fields(&body);
    assert!(
        fields.contains(&"spec.resourceAttributes".to_string()),
        "expected a cause on spec.resourceAttributes, got {fields:?}: {body}"
    );
    assert!(
        !fields.contains(&"spec.user".to_string()),
        "the self review carries no user field, so it must not be faulted for \
         one: {fields:?} {body}"
    );
}

/// `ValidateLocalSubjectAccessReview` (`validation.go:88-106`) adds two rules of
/// its own: `spec.resourceAttributes.namespace` must match the path namespace,
/// and `nonResourceAttributes` is disallowed outright.
#[tokio::test]
async fn a_local_subject_access_review_enforces_its_own_rules() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            "/apis/authorization.k8s.io/v1/namespaces/ns-a/localsubjectaccessreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "LocalSubjectAccessReview",
                "spec": {
                    "user": "alice",
                    "resourceAttributes": { "verb": "get", "resource": "pods", "namespace": "ns-b" },
                },
            })),
        )
        .await;
    assert_eq!(status.as_u16(), 422, "{body}");
    let fields = cause_fields(&body);
    assert!(
        fields.contains(&"spec.resourceAttributes.namespace".to_string()),
        "expected a cause on spec.resourceAttributes.namespace, got {fields:?}: {body}"
    );

    let (status, body) = api
        .send(
            "POST",
            "/apis/authorization.k8s.io/v1/namespaces/ns-a/localsubjectaccessreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "LocalSubjectAccessReview",
                "spec": {
                    "user": "alice",
                    "nonResourceAttributes": { "verb": "get", "path": "/healthz" },
                },
            })),
        )
        .await;
    assert_eq!(status.as_u16(), 422, "{body}");
    let fields = cause_fields(&body);
    assert!(
        fields.contains(&"spec.nonResourceAttributes".to_string()),
        "expected a cause on spec.nonResourceAttributes, got {fields:?}: {body}"
    );

    // The matching-namespace form still works.
    let (status, body) = api
        .send(
            "POST",
            "/apis/authorization.k8s.io/v1/namespaces/ns-a/localsubjectaccessreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "LocalSubjectAccessReview",
                "spec": {
                    "user": "alice",
                    "resourceAttributes": { "verb": "get", "resource": "pods", "namespace": "ns-a" },
                },
            })),
        )
        .await;
    assert!(status.is_success(), "{status} {body}");
}

/// `validateResourceAttributes` (`validation.go:108-118`) and the two
/// selector-attribute blocks below it. Validation used to stop at the
/// attribute-pairing rules, so a SAR could carry any `fieldSelector` /
/// `labelSelector` at all and nothing looked at it (#1951).
#[tokio::test]
async fn every_bad_selector_attribute_is_invalid_with_a_field_path() {
    let api = TestApiServer::new();

    fn sar(resource_attributes: Value) -> Value {
        json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "metadata": {},
            "spec": { "user": "alice", "resourceAttributes": resource_attributes },
        })
    }

    let cases: Vec<(&str, Value, &str)> = vec![
        (
            "a fieldSelector with neither rawSelector nor requirements",
            sar(json!({ "resource": "pods", "verb": "get", "fieldSelector": {} })),
            "spec.resourceAttributes.fieldSelector.requirements",
        ),
        (
            "a fieldSelector with both",
            sar(json!({
                "resource": "pods", "verb": "get",
                "fieldSelector": {
                    "rawSelector": "spec.nodeName=node-1",
                    "requirements": [{ "key": "spec.nodeName", "operator": "In", "values": ["node-1"] }]
                }
            })),
            "spec.resourceAttributes.fieldSelector.rawSelector",
        ),
        (
            "a field requirement with no key",
            sar(json!({
                "resource": "pods", "verb": "get",
                "fieldSelector": { "requirements": [{ "operator": "Exists" }] }
            })),
            "spec.resourceAttributes.fieldSelector.requirements[0].key",
        ),
        (
            "a field requirement whose In has no values",
            sar(json!({
                "resource": "pods", "verb": "get",
                "fieldSelector": { "requirements": [{ "key": "spec.nodeName", "operator": "In" }] }
            })),
            "spec.resourceAttributes.fieldSelector.requirements[0].values",
        ),
        (
            "a labelSelector with neither rawSelector nor requirements",
            sar(json!({ "resource": "pods", "verb": "get", "labelSelector": {} })),
            "spec.resourceAttributes.labelSelector.requirements",
        ),
        (
            "a label requirement with no key",
            sar(json!({
                "resource": "pods", "verb": "get",
                "labelSelector": { "requirements": [{ "operator": "Exists" }] }
            })),
            "spec.resourceAttributes.labelSelector.requirements[0].key",
        ),
        (
            "a label requirement whose Exists carries values",
            sar(json!({
                "resource": "pods", "verb": "get",
                "labelSelector": { "requirements": [{ "key": "app", "operator": "Exists", "values": ["web"] }] }
            })),
            "spec.resourceAttributes.labelSelector.requirements[0].values",
        ),
    ];

    for (label, body, expected_field) in cases {
        let (status, response) = api
            .send(
                "POST",
                "/apis/authorization.k8s.io/v1/subjectaccessreviews",
                Some("application/json"),
                Some(&body),
            )
            .await;
        assert_eq!(status.as_u16(), 422, "{label}: {response}");
        let fields = cause_fields(&response);
        assert!(
            fields.iter().any(|f| f == expected_field),
            "{label} must fault `{expected_field}`, got {fields:?}: {response}"
        );
    }
}

/// The skew allowance. Upstream passes
/// `AllowUnknownOperatorInRequirement: true` for a SAR
/// (`validation.go:133`, `:158`) precisely so a newer client whose operator
/// this server does not know can still be authorized — so an unrecognised
/// operator must NOT be a 422.
#[tokio::test]
async fn an_unknown_selector_operator_is_accepted() {
    let api = TestApiServer::new();
    let (status, body) = api
        .send(
            "POST",
            "/apis/authorization.k8s.io/v1/subjectaccessreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SubjectAccessReview",
                "metadata": {},
                "spec": { "user": "alice", "resourceAttributes": {
                    "resource": "pods", "verb": "get",
                    "fieldSelector": { "requirements": [
                        { "key": "spec.nodeName", "operator": "SomethingNewerClientsKnow" }
                    ] }
                } },
            })),
        )
        .await;
    assert!(
        status.is_success(),
        "an unknown operator must be allowed through, got {status}: {body}"
    );
}

/// `SelfSubjectRulesReviewSpec.namespace` is answered by the *registry*, not a
/// validator: upstream's `Create` rejects an empty namespace with a 400 before
/// it authorizes anything
/// (`pkg/registry/authorization/selfsubjectrulesreview/rest.go:70-73`).
#[tokio::test]
async fn a_self_subject_rules_review_with_no_namespace_is_a_bad_request() {
    let api = TestApiServer::new();
    let (status, body) = api
        .send(
            "POST",
            "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectRulesReview",
                "metadata": {},
                "spec": {},
            })),
        )
        .await;
    assert_eq!(status.as_u16(), 400, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("no namespace on request"),
        "{body}"
    );
}
