//! Tests for PodDisruptionBudget field validation.
//!
//! Mirrors upstream `ValidatePodDisruptionBudgetSpec`
//! (`pkg/apis/policy/validation/validation.go`, release-1.35).

use rusternetes_common::resources::policy::PodDisruptionBudget;
use rusternetes_common::resources::policy::{IntOrString, PodDisruptionBudgetSpec};
use rusternetes_common::types::LabelSelector;
use rusternetes_common::validation::field::Path;
use rusternetes_common::validation::field::{BadValue, Error, ErrorType};
use rusternetes_common::validation::pdb::{
    validate_pod_disruption_budget, validate_pod_disruption_budget_spec,
    PodDisruptionBudgetValidationOptions,
};
use serde_json::json;

fn pdb(spec: serde_json::Value) -> PodDisruptionBudget {
    serde_json::from_value(json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {"name": "pdb", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn min_available_int_passes() {
    let errs = validate_pod_disruption_budget(
        &pdb(json!({
            "minAvailable": 2,
            "selector": {"matchLabels": {"app": "web"}}
        })),
        Default::default(),
    );
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn max_unavailable_percent_passes() {
    let errs = validate_pod_disruption_budget(
        &pdb(json!({
            "maxUnavailable": "30%",
            "selector": {"matchLabels": {"app": "web"}}
        })),
        Default::default(),
    );
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn both_min_and_max_rejected() {
    let errs = validate_pod_disruption_budget(
        &pdb(json!({
            "minAvailable": 1,
            "maxUnavailable": 1,
            "selector": {"matchLabels": {"app": "web"}}
        })),
        Default::default(),
    );
    assert!(has(&errs, "spec", ErrorType::Invalid), "got: {errs:?}");
}

#[test]
fn percent_over_100_rejected() {
    let errs = validate_pod_disruption_budget(
        &pdb(json!({
            "minAvailable": "150%",
            "selector": {"matchLabels": {"app": "web"}}
        })),
        Default::default(),
    );
    assert!(
        has(&errs, "spec.minAvailable", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn negative_int_rejected() {
    let errs = validate_pod_disruption_budget(
        &pdb(json!({
            "maxUnavailable": -1,
            "selector": {"matchLabels": {"app": "web"}}
        })),
        Default::default(),
    );
    assert!(
        has(&errs, "spec.maxUnavailable", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn bad_unhealthy_pod_eviction_policy_rejected() {
    let errs = validate_pod_disruption_budget(
        &pdb(json!({
            "minAvailable": 1,
            "selector": {"matchLabels": {"app": "web"}},
            "unhealthyPodEvictionPolicy": "Bogus"
        })),
        Default::default(),
    );
    assert!(
        has(
            &errs,
            "spec.unhealthyPodEvictionPolicy",
            ErrorType::NotSupported
        ),
        "got: {errs:?}"
    );
}

#[test]
fn valid_unhealthy_pod_eviction_policy_passes() {
    let errs = validate_pod_disruption_budget(
        &pdb(json!({
            "minAvailable": 1,
            "selector": {"matchLabels": {"app": "web"}},
            "unhealthyPodEvictionPolicy": "AlwaysAllow"
        })),
        Default::default(),
    );
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

fn allow_invalid() -> PodDisruptionBudgetValidationOptions {
    PodDisruptionBudgetValidationOptions {
        allow_invalid_label_value_in_selector: true,
    }
}

fn min_available(v: IntOrString) -> PodDisruptionBudgetSpec {
    PodDisruptionBudgetSpec {
        min_available: Some(v),
        ..Default::default()
    }
}

/// `TestValidateMinAvailablePodDisruptionBudgetSpec` (validation_test.go:46-81).
#[test]
fn min_available_success_and_failure_cases() {
    let path = Path::new("foo");
    for ok in [
        IntOrString::String("0%".into()),
        IntOrString::String("1%".into()),
        IntOrString::String("100%".into()),
        IntOrString::Int(0),
        IntOrString::Int(1),
        IntOrString::Int(100),
    ] {
        let errs =
            validate_pod_disruption_budget_spec(&min_available(ok.clone()), allow_invalid(), &path);
        assert!(errs.is_empty(), "{ok:?}: {errs:?}");
    }
    for bad in [
        IntOrString::String("1.1%".into()),
        IntOrString::String("nope".into()),
        IntOrString::String("-1%".into()),
        IntOrString::String("101%".into()),
        IntOrString::Int(-1),
    ] {
        let errs = validate_pod_disruption_budget_spec(
            &min_available(bad.clone()),
            allow_invalid(),
            &path,
        );
        assert!(!errs.is_empty(), "{bad:?} accepted");
    }
}

/// A malformed percent reports `IsValidPercent`'s message, the value quoted
/// (`ValidatePositiveIntOrPercent`, pkg/apis/apps/validation/validation.go:550-553).
#[test]
fn a_malformed_percent_reports_the_percent_format() {
    let errs = validate_pod_disruption_budget_spec(
        &min_available(IntOrString::String("nope".into())),
        Default::default(),
        &Path::new("spec"),
    );
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert_eq!(
        errs[0].to_string(),
        "spec.minAvailable: Invalid value: \"nope\": a valid percent string must be a numeric string followed by an ending '%' (e.g. '1%',  or '93%', regex used for validation is '[0-9]+%')"
    );
}

/// `TestValidateMinAvailablePodAndMaxUnavailableDisruptionBudgetSpec`
/// (validation_test.go:83-95); the bad value is the internal spec
/// (validation.go:53), marshalled with Go field names.
#[test]
fn both_bounds_report_the_spec() {
    let spec = PodDisruptionBudgetSpec {
        min_available: Some(IntOrString::String("10%".into())),
        max_unavailable: Some(IntOrString::Int(1)),
        ..Default::default()
    };
    let errs = validate_pod_disruption_budget_spec(&spec, allow_invalid(), &Path::new("foo"));
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert_eq!(errs[0].field, "foo");
    assert_eq!(
        errs[0].detail,
        "minAvailable and maxUnavailable cannot be both set"
    );
    assert_eq!(
        errs[0].bad_value,
        BadValue::Marshaled(
            r#"{"MinAvailable":"10%","Selector":null,"MaxUnavailable":1,"UnhealthyPodEvictionPolicy":null}"#
                .to_string()
        )
    );
    // Go's field order, not sorted (#2086).
    assert_eq!(
        errs[0].error_body(),
        r#"Invalid value: {"MinAvailable":"10%","Selector":null,"MaxUnavailable":1,"UnhealthyPodEvictionPolicy":null}: minAvailable and maxUnavailable cannot be both set"#
    );
}

/// `TestValidateUnhealthyPodEvictionPolicyDisruptionBudgetSpec`
/// (validation_test.go:97-147): the empty policy is not supported either.
#[test]
fn an_empty_unhealthy_pod_eviction_policy_is_rejected() {
    let mut spec = min_available(IntOrString::String("10%".into()));
    spec.unhealthy_pod_eviction_policy = Some(String::new());
    let errs = validate_pod_disruption_budget_spec(&spec, allow_invalid(), &Path::new("foo"));
    assert_eq!(errs.len(), 1, "{errs:?}");
}

/// `AllowInvalidLabelValueInSelector` relaxes only the selector's values
/// (`ValidateLabelSelector`, apimachinery .../meta/v1/validation).
#[test]
fn an_invalid_selector_value_is_allowed_only_by_the_option() {
    let mut spec = min_available(IntOrString::Int(1));
    spec.selector = Some(LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![serde_json::from_value(json!({
            "key": "app", "operator": "In", "values": ["-bad-"]
        }))
        .unwrap()]),
    });
    let strict = validate_pod_disruption_budget_spec(&spec, Default::default(), &Path::new("spec"));
    assert!(!strict.is_empty());
    let relaxed = validate_pod_disruption_budget_spec(&spec, allow_invalid(), &Path::new("spec"));
    assert!(relaxed.is_empty(), "{relaxed:?}");
}
