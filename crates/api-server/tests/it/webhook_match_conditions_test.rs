//! A webhook's `matchConditions` are validated by the ported validator.
//!
//! `handlers/admission_webhook.rs` checked them with a hand-rolled loop that
//! only tested `name` and `expression` for emptiness, and answered an
//! `InvalidResource` whose field path lived in the message text rather than in
//! `details.causes` (#2024).
//!
//! Upstream reaches **one** function from both the webhook validators and the
//! policy validator: `validateMatchConditions`
//! (`pkg/apis/admissionregistration/validation/validation.go:963`), called from
//! `validateValidatingWebhook` (`:409`), `validateMutatingWebhook` (`:467`) and
//! `validateValidatingAdmissionPolicySpec` (`:800`). It caps the list at 64,
//! puts `name` through the qualified-name rule, rejects a duplicate `name`, and
//! treats a whitespace-only `expression` as `Required` — none of which the
//! ad-hoc loop did.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const VALIDATING: &str = "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations";
const MUTATING: &str = "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations";

/// A webhook that passes every other rule, carrying `match_conditions`.
fn webhook_with(name: &str, match_conditions: Value) -> Value {
    json!({
        "name": name,
        "clientConfig": { "url": "https://example.com/hook" },
        "sideEffects": "None",
        "admissionReviewVersions": ["v1"],
        "matchConditions": match_conditions,
    })
}

fn config(kind: &str, name: &str, match_conditions: Value) -> Value {
    json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": kind,
        "metadata": { "name": name },
        "webhooks": [webhook_with(name, match_conditions)],
    })
}

/// 65 conditions: one over `validateMatchConditions`'s cap (`:966`).
fn sixty_five_conditions() -> Value {
    Value::Array(
        (0..65)
            .map(|i| json!({ "name": format!("c-{i}"), "expression": "true" }))
            .collect(),
    )
}

fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "more than 64 matchConditions",
            sixty_five_conditions(),
            "webhooks[0].matchConditions: Too many: must have at most 64 items",
        ),
        (
            "a matchCondition with no name",
            json!([{ "expression": "true" }]),
            "webhooks[0].matchConditions[0].name: Required value",
        ),
        (
            "a matchCondition whose name is not a qualified name",
            json!([{ "name": "Not Valid!", "expression": "true" }]),
            "webhooks[0].matchConditions[0].name: Invalid value: \"Not Valid!\"",
        ),
        (
            "two matchConditions with the same name",
            json!([
                { "name": "same", "expression": "true" },
                { "name": "same", "expression": "false" }
            ]),
            "webhooks[0].matchConditions[1].name: Duplicate value",
        ),
        (
            "a matchCondition with no expression",
            json!([{ "name": "c" }]),
            "webhooks[0].matchConditions[0].expression: Required value",
        ),
        (
            "a matchCondition whose expression is only whitespace",
            json!([{ "name": "c", "expression": "   " }]),
            "webhooks[0].matchConditions[0].expression: Required value",
        ),
    ]
}

#[tokio::test]
async fn every_bad_match_condition_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, conditions, expected)) in cases().into_iter().enumerate() {
        for (kind, url) in [
            ("ValidatingWebhookConfiguration", VALIDATING),
            ("MutatingWebhookConfiguration", MUTATING),
        ] {
            let name = format!("mc-{i}.example.com");
            let (status, body) = api
                .send(
                    "POST",
                    url,
                    Some("application/json"),
                    Some(&config(kind, &name, conditions.clone())),
                )
                .await;

            assert_ne!(
                status.as_u16(),
                400,
                "{kind}: {label} was rejected by the decoder, before any validation: {body}"
            );
            assert_eq!(
                status.as_u16(),
                422,
                "{kind}: {label} must be Invalid, not {status}: {body}"
            );
            let message = body["message"].as_str().unwrap_or_default();
            assert!(
                message.contains(expected),
                "{kind}: {label} must report `{expected}`, got: {message}"
            );
        }
    }
}

/// The rejection is an `Invalid` `Status`, so every failing field is carried in
/// `details.causes` with its own path — not baked into one message string.
#[tokio::test]
async fn the_rejection_carries_details_causes() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            VALIDATING,
            Some("application/json"),
            Some(&config(
                "ValidatingWebhookConfiguration",
                "causes.example.com",
                json!([{ "name": "", "expression": "" }]),
            )),
        )
        .await;

    assert_eq!(status.as_u16(), 422, "{body}");
    let causes = body["details"]["causes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let fields: Vec<&str> = causes.iter().filter_map(|c| c["field"].as_str()).collect();
    assert!(
        fields.contains(&"webhooks[0].matchConditions[0].name"),
        "causes must name the field: {body}"
    );
    assert!(
        fields.contains(&"webhooks[0].matchConditions[0].expression"),
        "causes must name the field: {body}"
    );
}

/// An update whose `matchConditions` differ from the stored ones is validated:
/// upstream's `ignoreMatchConditions` (`:730`) only spares a list that is
/// unchanged.
#[tokio::test]
async fn an_update_that_changes_match_conditions_is_validated() {
    let api = TestApiServer::new();
    let name = "update-mc.example.com";

    let (status, body) = api
        .send(
            "POST",
            VALIDATING,
            Some("application/json"),
            Some(&config(
                "ValidatingWebhookConfiguration",
                name,
                json!([{ "name": "first", "expression": "true" }]),
            )),
        )
        .await;
    assert!(status.is_success(), "create must succeed: {status} {body}");

    let (status, body) = api
        .send(
            "PUT",
            &format!("{VALIDATING}/{name}"),
            Some("application/json"),
            Some(&config(
                "ValidatingWebhookConfiguration",
                name,
                json!([
                    { "name": "dup", "expression": "true" },
                    { "name": "dup", "expression": "false" }
                ]),
            )),
        )
        .await;

    assert_eq!(status.as_u16(), 422, "{status} {body}");
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("webhooks[0].matchConditions[1].name: Duplicate value"),
        "got: {message}"
    );
}

/// A well-formed list still writes, and comes back unchanged.
#[tokio::test]
async fn a_valid_match_condition_still_writes() {
    let api = TestApiServer::new();

    let (status, created) = api
        .send(
            "POST",
            VALIDATING,
            Some("application/json"),
            Some(&config(
                "ValidatingWebhookConfiguration",
                "ok-mc.example.com",
                json!([{
                    "name": "exclude-kube-system",
                    "expression": "object.metadata.namespace != 'kube-system'"
                }]),
            )),
        )
        .await;

    assert!(status.is_success(), "{status} {created}");
    assert_eq!(
        created["webhooks"][0]["matchConditions"][0]["name"],
        json!("exclude-kube-system")
    );
}
