//! Tests for DaemonSet field validation.
//!
//! Mirrors upstream `ValidateDaemonSetSpec`
//! (`pkg/apis/apps/validation/validation.go`, release-1.35). Runs after
//! defaulting, so the valid fixture carries `updateStrategy`.

use rusternetes_common::resources::workloads::DaemonSet;
use rusternetes_common::validation::apps::validate_daemonset;
use rusternetes_common::validation::field::{Error, ErrorType};
use serde_json::json;

fn ds(spec: serde_json::Value) -> DaemonSet {
    serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "agent", "namespace": "default"},
        "spec": spec
    }))
    .unwrap()
}

fn valid_spec() -> serde_json::Value {
    json!({
        "selector": {"matchLabels": {"app": "agent"}},
        "updateStrategy": {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 1}},
        "template": {"metadata": {"labels": {"app": "agent"}}, "spec": {"containers": []}}
    })
}

fn has(errs: &[Error], field: &str, ty: ErrorType) -> bool {
    errs.iter().any(|e| e.field == field && e.error_type == ty)
}

#[test]
fn valid_daemonset_passes() {
    let errs = validate_daemonset(&ds(valid_spec()));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn ondelete_strategy_passes() {
    let mut spec = valid_spec();
    spec["updateStrategy"] = json!({"type": "OnDelete"});
    let errs = validate_daemonset(&ds(spec));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn empty_selector_required() {
    let mut spec = valid_spec();
    spec["selector"] = json!({});
    let errs = validate_daemonset(&ds(spec));
    // Upstream emits Invalid("empty selector is invalid for daemonset"), not Required.
    assert!(
        errs.iter().any(|e| e.field == "spec.selector"
            && e.error_type == ErrorType::Invalid
            && e.detail.contains("empty selector is invalid for daemonset")),
        "got: {errs:?}"
    );
}

#[test]
fn template_labels_must_match_selector() {
    let mut spec = valid_spec();
    spec["template"]["metadata"]["labels"] = json!({"app": "other"});
    let errs = validate_daemonset(&ds(spec));
    assert!(
        errs.iter().any(|e| e.field.starts_with("spec.template")),
        "got: {errs:?}"
    );
}

#[test]
fn negative_min_ready_seconds_rejected() {
    let mut spec = valid_spec();
    spec["minReadySeconds"] = json!(-1);
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(&errs, "spec.minReadySeconds", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn negative_revision_history_limit_rejected() {
    let mut spec = valid_spec();
    spec["revisionHistoryLimit"] = json!(-3);
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(&errs, "spec.revisionHistoryLimit", ErrorType::Invalid),
        "got: {errs:?}"
    );
}

#[test]
fn unknown_update_strategy_not_supported() {
    let mut spec = valid_spec();
    spec["updateStrategy"] = json!({"type": "Bogus"});
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(&errs, "spec.updateStrategy", ErrorType::NotSupported),
        "got: {errs:?}"
    );
}

#[test]
fn rolling_update_requires_rolling_update_block() {
    let mut spec = valid_spec();
    spec["updateStrategy"] = json!({"type": "RollingUpdate"});
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(
            &errs,
            "spec.updateStrategy.rollingUpdate",
            ErrorType::Required
        ),
        "got: {errs:?}"
    );
}

#[test]
fn out_of_range_max_unavailable_rejected() {
    let mut spec = valid_spec();
    spec["updateStrategy"]["rollingUpdate"]["maxUnavailable"] = json!("150%");
    let errs = validate_daemonset(&ds(spec));
    assert!(
        has(
            &errs,
            "spec.updateStrategy.rollingUpdate.maxUnavailable",
            ErrorType::Invalid
        ),
        "got: {errs:?}"
    );
}

// ---------------------------------------------------------------------------
// ValidatePositiveIntOrPercent: a String IntOrString must be a percentage
// ---------------------------------------------------------------------------
//
// Upstream dispatches on the IntOrString `Type`
// (`pkg/apis/apps/validation/validation.go:547-560`):
//
//     case intstr.String:
//         for _, msg := range validation.IsValidPercent(intOrPercent.StrVal) {
//             allErrs = append(allErrs, field.Invalid(fldPath, intOrPercent, msg))
//     case intstr.Int:
//         allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(...)...)
//
// `IsValidPercent` requires `^[0-9]+%$`, so a bare `"1"` is **invalid** even
// though `getIntOrPercentValue` will still read its numeric value via
// `IntValue()`. Both facts matter: the format error is emitted, and the
// zero/non-zero switch still sees 1.

/// The exact message upstream produces, via
/// `RegexError(percentErrMsg, percentFmt, "1%", "93%")`. The double space
/// before `or` is upstream's — `RegexError` appends `"', "` per example and
/// then `" or "` before the next one.
const PERCENT_MSG: &str = "a valid percent string must be a numeric string followed by an ending '%' (e.g. '1%',  or '93%', regex used for validation is '[0-9]+%')";

fn with_max_unavailable(v: serde_json::Value) -> DaemonSet {
    let mut spec = valid_spec();
    spec["updateStrategy"] =
        json!({"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": v}});
    ds(spec)
}

#[test]
fn integer_max_unavailable_is_valid() {
    let errs = validate_daemonset(&with_max_unavailable(json!(1)));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn percent_max_unavailable_is_valid() {
    let errs = validate_daemonset(&with_max_unavailable(json!("25%")));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

/// The gap this closes: a bare numeric string was silently read as an integer.
#[test]
fn bare_numeric_string_max_unavailable_is_rejected() {
    let errs = validate_daemonset(&with_max_unavailable(json!("1")));
    let e = errs
        .iter()
        .find(|e| e.field == "spec.updateStrategy.rollingUpdate.maxUnavailable")
        .unwrap_or_else(|| panic!("expected a maxUnavailable error, got: {errs:?}"));
    assert_eq!(e.error_type, ErrorType::Invalid, "{errs:?}");
    assert_eq!(e.detail, PERCENT_MSG, "{errs:?}");
}

#[test]
fn non_numeric_string_max_unavailable_is_rejected() {
    let errs = validate_daemonset(&with_max_unavailable(json!("abc")));
    let e = errs
        .iter()
        .find(|e| e.field == "spec.updateStrategy.rollingUpdate.maxUnavailable")
        .unwrap_or_else(|| panic!("expected a maxUnavailable error, got: {errs:?}"));
    assert_eq!(e.detail, PERCENT_MSG, "{errs:?}");
}

/// A percent sign alone is not `[0-9]+%`.
#[test]
fn empty_percent_max_unavailable_is_rejected() {
    let errs = validate_daemonset(&with_max_unavailable(json!("%")));
    assert!(
        errs.iter().any(
            |e| e.field == "spec.updateStrategy.rollingUpdate.maxUnavailable"
                && e.detail == PERCENT_MSG
        ),
        "{errs:?}"
    );
}

/// The Int branch keeps upstream's `ValidateNonnegativeField` message.
#[test]
fn negative_integer_max_unavailable_is_rejected() {
    let errs = validate_daemonset(&with_max_unavailable(json!(-1)));
    let e = errs
        .iter()
        .find(|e| e.field == "spec.updateStrategy.rollingUpdate.maxUnavailable")
        .unwrap_or_else(|| panic!("expected a maxUnavailable error, got: {errs:?}"));
    assert_eq!(e.detail, "must be greater than or equal to 0", "{errs:?}");
}

/// IsNotMoreThan100Percent still applies on top of the format check.
#[test]
fn over_100_percent_max_unavailable_is_rejected() {
    let errs = validate_daemonset(&with_max_unavailable(json!("150%")));
    assert!(
        errs.iter().any(
            |e| e.field == "spec.updateStrategy.rollingUpdate.maxUnavailable"
                && e.detail == "must not be greater than 100%"
        ),
        "{errs:?}"
    );
}

/// A bare numeric string is invalid *and* still counts as non-zero for the
/// mutual-exclusion switch — upstream's `getIntOrPercentValue` falls back to
/// `IntValue()`, which parses the string. So this must NOT also complain that
/// maxUnavailable is 0 when maxSurge is 0.
#[test]
fn bare_numeric_string_still_counts_as_non_zero() {
    let errs = validate_daemonset(&with_max_unavailable(json!("2")));
    assert!(
        !errs.iter().any(|e| e.error_type == ErrorType::Required),
        "a non-zero (if malformed) maxUnavailable must not trip the \
         'cannot be 0 when maxSurge is 0' rule: {errs:?}"
    );
}
