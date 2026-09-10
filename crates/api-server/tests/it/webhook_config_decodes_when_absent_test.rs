//! A webhook configuration decodes when its declaration omits a field.
//!
//! Upstream's `ValidatingWebhook` / `MutatingWebhook`
//! (`staging/src/k8s.io/api/admissionregistration/v1/types.go`) are plain Go
//! structs: `name`, `clientConfig`, `sideEffects` and
//! `admissionReviewVersions` are all *required*, but required by
//! **validation**, not by the decoder —
//! `validateValidatingWebhook` / `validateMutatingWebhook`
//! (`pkg/apis/admissionregistration/validation/validation.go:250-330`) answer
//! `field.Required` / `field.NotSupported` with a path the client can act on.
//!
//! Rusternetes declared them as bare non-`Option` fields, so serde rejected the
//! body first and the client got a 400 BadRequest with no `Status` details
//! (#1939, the `admission_webhook.rs` slice). Every check the fields need
//! already exists in `crates/common/src/validation/webhookconfiguration.rs`;
//! they just could not be reached.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn assert_not_a_decode_failure(status: axum::http::StatusCode, body: &Value, whose: &str) {
    assert_ne!(
        status.as_u16(),
        400,
        "{whose} was rejected by the decoder, before any validation: {body}"
    );
}

/// A webhook with nothing but a name: upstream answers 422 naming
/// `clientConfig`, `sideEffects` and `admissionReviewVersions`.
#[tokio::test]
async fn a_validating_webhook_with_only_a_name_is_a_422_not_a_400() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingWebhookConfiguration",
                "metadata": { "name": "bare.example.com" },
                "webhooks": [{ "name": "bare.example.com" }],
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a webhook with only a name");
    assert_eq!(status.as_u16(), 422, "{status} {answer}");
    let rendered = answer.to_string();
    for field in ["clientConfig", "sideEffects", "admissionReviewVersions"] {
        assert!(
            rendered.contains(field),
            "the rejection must name `{field}`: {answer}"
        );
    }
}

/// The mutating flavour is the same struct with a different name upstream, and
/// upstream validates it through the same helper.
#[tokio::test]
async fn a_mutating_webhook_with_only_a_name_is_a_422_not_a_400() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "MutatingWebhookConfiguration",
                "metadata": { "name": "bare-mut.example.com" },
                "webhooks": [{ "name": "bare-mut.example.com" }],
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a mutating webhook with only a name");
    assert_eq!(status.as_u16(), 422, "{status} {answer}");
}

/// `matchConditions[].expression` is required by validation upstream
/// (`validation.go:987`), so a condition with only a name is a 422 naming the
/// expression.
#[tokio::test]
async fn a_match_condition_without_an_expression_is_a_422_not_a_400() {
    let api = TestApiServer::new();

    let (status, answer) = api
        .send(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingWebhookConfiguration",
                "metadata": { "name": "mc.example.com" },
                "webhooks": [{
                    "name": "mc.example.com",
                    "clientConfig": { "url": "https://example.com/hook" },
                    "sideEffects": "None",
                    "admissionReviewVersions": ["v1"],
                    "matchConditions": [{ "name": "c" }],
                }],
            })),
        )
        .await;
    assert_not_a_decode_failure(status, &answer, "a matchCondition with no expression");
    assert!(
        status.as_u16() == 422 || status.as_u16() == 400,
        "{status} {answer}"
    );
    assert!(
        answer.to_string().contains("expression"),
        "the rejection names the field: {answer}"
    );
}

/// And a fully-specified configuration still writes — the defaults must not
/// change what a valid body means.
#[tokio::test]
async fn a_complete_webhook_configuration_still_writes() {
    let api = TestApiServer::new();

    let (status, created) = api
        .send(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingWebhookConfiguration",
                "metadata": { "name": "good.example.com" },
                "webhooks": [{
                    "name": "good.example.com",
                    "clientConfig": { "url": "https://example.com/hook" },
                    "sideEffects": "None",
                    "admissionReviewVersions": ["v1"],
                    "rules": [{
                        "operations": ["CREATE"],
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "resources": ["pods"],
                    }],
                }],
            })),
        )
        .await;
    assert!(status.is_success(), "{status} {created}");
    assert_eq!(created["webhooks"][0]["sideEffects"], json!("None"));
    assert_eq!(
        created["webhooks"][0]["clientConfig"]["url"],
        json!("https://example.com/hook")
    );
}
