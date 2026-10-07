use rusternetes_common::resources::admission_webhook::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};
use rusternetes_common::validation::webhookconfiguration::{
    validate_mutating_webhook_configuration, validate_validating_webhook_configuration,
};
use serde_json::json;

fn vwc(webhook: serde_json::Value) -> ValidatingWebhookConfiguration {
    serde_json::from_value(json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "cfg"},
        "webhooks": [webhook]
    }))
    .unwrap()
}

fn valid_webhook() -> serde_json::Value {
    json!({
        "name": "deny.example.com",
        "clientConfig": {"url": "https://example.com/hook"},
        "sideEffects": "None",
        "admissionReviewVersions": ["v1"],
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["pods"]
        }]
    })
}

#[test]
fn valid_config_has_no_errors() {
    assert!(validate_validating_webhook_configuration(&vwc(valid_webhook())).is_empty());
}

#[test]
fn side_effects_must_be_none_or_none_on_dry_run() {
    let mut w = valid_webhook();
    w["sideEffects"] = json!("Unknown");
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter().any(|e| e.to_string().contains("sideEffects")),
        "{errs:?}"
    );
}

#[test]
fn name_must_be_fully_qualified() {
    let mut w = valid_webhook();
    w["name"] = json!("foo");
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter().any(|e| e.to_string().contains("name")),
        "{errs:?}"
    );
}

#[test]
fn admission_review_versions_required_and_recognized() {
    let mut w = valid_webhook();
    w["admissionReviewVersions"] = json!([]);
    assert!(!validate_validating_webhook_configuration(&vwc(w)).is_empty());

    let mut w2 = valid_webhook();
    w2["admissionReviewVersions"] = json!(["v2"]);
    let errs = validate_validating_webhook_configuration(&vwc(w2));
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("admissionReviewVersions")),
        "{errs:?}"
    );
}

#[test]
fn client_config_exactly_one_of_url_or_service() {
    let mut w = valid_webhook();
    w["clientConfig"] = json!({
        "url": "https://example.com/hook",
        "service": {"namespace": "ns", "name": "svc"}
    });
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter().any(|e| e.to_string().contains("clientConfig")),
        "{errs:?}"
    );
}

#[test]
fn timeout_seconds_range() {
    let mut w = valid_webhook();
    w["timeoutSeconds"] = json!(60);
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("timeoutSeconds")),
        "{errs:?}"
    );
}

#[test]
fn duplicate_webhook_names() {
    let cfg: ValidatingWebhookConfiguration = serde_json::from_value(json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "cfg"},
        "webhooks": [valid_webhook(), valid_webhook()]
    }))
    .unwrap();
    let errs = validate_validating_webhook_configuration(&cfg);
    assert!(
        errs.iter().any(|e| e.to_string().contains("name")),
        "{errs:?}"
    );
}

#[test]
fn rule_wildcard_operation_exclusive() {
    let mut w = valid_webhook();
    w["rules"][0]["operations"] = json!(["*", "CREATE"]);
    let errs = validate_validating_webhook_configuration(&vwc(w));
    assert!(
        errs.iter().any(|e| e.to_string().contains("operations")),
        "{errs:?}"
    );
}

#[test]
fn mutating_valid_config_has_no_errors() {
    let cfg: MutatingWebhookConfiguration = serde_json::from_value(json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "cfg"},
        "webhooks": [valid_webhook()]
    }))
    .unwrap();
    assert!(validate_mutating_webhook_configuration(&cfg).is_empty());
}

/// Unknown values of the string-typed enum fields decode and validation
/// reports `Unsupported value` (422), as upstream's plain `string` types do
/// (#2475). Upstream cases: validation_test.go:327 (failurePolicy), :357
/// (sideEffects), :184/:199 (operations), :1304/:1334 (mutating); checks at
/// validation.go:371/:374/:385/:453/:547.
#[test]
fn unknown_enum_values_report_unsupported_value() {
    let cases: Vec<(&str, &str, serde_json::Value, &str)> = vec![
        (
            "failurePolicy",
            "failurePolicy",
            json!("other"),
            "webhooks[0].failurePolicy: Unsupported value: \"other\": supported values: \"Fail\", \"Ignore\"",
        ),
        (
            "matchPolicy",
            "matchPolicy",
            json!("other"),
            "webhooks[0].matchPolicy: Unsupported value: \"other\": supported values: \"Equivalent\", \"Exact\"",
        ),
        (
            "sideEffects",
            "sideEffects",
            json!("other"),
            "webhooks[0].sideEffects: Unsupported value: \"other\": supported values: \"None\", \"NoneOnDryRun\"",
        ),
    ];
    for (name, field, val, want) in cases {
        let mut w = valid_webhook();
        w[field] = val;
        let v: ValidatingWebhookConfiguration = serde_json::from_value(json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingWebhookConfiguration",
            "metadata": {"name": "cfg"},
            "webhooks": [w.clone()]
        }))
        .unwrap_or_else(|e| panic!("{name}: must decode: {e}"));
        let errs = validate_validating_webhook_configuration(&v);
        assert!(
            errs.iter().any(|e| e.to_string().contains(want)),
            "{name}: want {want:?} in {errs:?}"
        );
        let m: MutatingWebhookConfiguration = serde_json::from_value(json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "MutatingWebhookConfiguration",
            "metadata": {"name": "cfg"},
            "webhooks": [w]
        }))
        .unwrap_or_else(|e| panic!("{name} (mutating): must decode: {e}"));
        let errs = validate_mutating_webhook_configuration(&m);
        assert!(
            errs.iter().any(|e| e.to_string().contains(want)),
            "{name} (mutating): want {want:?} in {errs:?}"
        );
    }
}

#[test]
fn unknown_reinvocation_policy_and_operation_report_unsupported_value() {
    let mut w = valid_webhook();
    w["reinvocationPolicy"] = json!("other");
    w["rules"][0]["operations"] = json!(["PATCH"]);
    let m: MutatingWebhookConfiguration = serde_json::from_value(json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "cfg"},
        "webhooks": [w]
    }))
    .expect("unknown reinvocationPolicy/operation must decode");
    let errs: Vec<String> = validate_mutating_webhook_configuration(&m)
        .iter()
        .map(|e| e.to_string())
        .collect();
    for want in [
        "webhooks[0].reinvocationPolicy: Unsupported value: \"other\": supported values: \"IfNeeded\", \"Never\"",
        "webhooks[0].rules[0].operations[0]: Unsupported value: \"PATCH\": supported values: \"*\", \"CONNECT\", \"CREATE\", \"DELETE\", \"UPDATE\"",
    ] {
        assert!(errs.iter().any(|e| e.contains(want)), "want {want:?} in {errs:?}");
    }
}

/// The unrecognised string survives a round trip unchanged.
#[test]
fn unknown_enum_values_round_trip() {
    let mut w = valid_webhook();
    w["failurePolicy"] = json!("other");
    w["sideEffects"] = json!("weird");
    w["rules"][0]["operations"] = json!(["PATCH"]);
    let h: rusternetes_common::resources::admission_webhook::ValidatingWebhook =
        serde_json::from_value(w.clone()).unwrap();
    let back = serde_json::to_value(&h).unwrap();
    assert_eq!(back["failurePolicy"], "other");
    assert_eq!(back["sideEffects"], "weird");
    assert_eq!(back["rules"][0]["operations"][0], "PATCH");
}
