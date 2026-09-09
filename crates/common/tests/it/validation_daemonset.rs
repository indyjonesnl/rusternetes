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

// ---------------------------------------------------------------------------
// The rendered `error_body()` — pinned against upstream's own output
// ---------------------------------------------------------------------------
//
// #1903 rendered an IntOrString bad value **unquoted**, on the strength of this
// switch:
//
// ```go
// switch t := e.BadValue.(type) {
// case int64, int32, float64, float32, bool:
//     s += fmt.Sprintf("%v", value)
// case fmt.Stringer:
//     s += fmt.Sprintf("%s", t.String())
// ...
// ```
//
// That is the **kustomize fork** of `field.Error`
// (`vendor/sigs.k8s.io/kustomize/kyaml/yaml/internal/k8sgen/pkg/util/validation/field/errors.go:49-73`),
// not apimachinery's. The real one
// (`staging/src/k8s.io/apimachinery/pkg/util/validation/field/errors.go:84-107`)
// has no `case fmt.Stringer:` at all — its `default:` arm marshals to JSON
// first and only falls back to `fmt.Stringer` when marshalling *errors*:
//
// ```go
// jb, err := json.Marshal(e.BadValue)
// if err == nil {
//     valstr = string(jb)
// } else if stringer, ok := e.BadValue.(fmt.Stringer); ok {
//     valstr = stringer.String()
// }
// ```
//
// `intstr.IntOrString` and `resource.Quantity` both have a `MarshalJSON`, so
// neither ever reaches the Stringer path. Running upstream's `ErrorBody()`
// directly on the pinned checkout:
//
// ```text
// intstr.FromString("abc")     => Invalid value: "abc": some detail
// intstr.FromString("50%")     => Invalid value: "50%": some detail
// intstr.FromInt32(5)          => Invalid value: 5: some detail
// resource.MustParse("2Gi")    => Invalid value: "2Gi": some detail
// resource.MustParse("-1")     => Invalid value: "-1": some detail
// ```
//
// So a String-typed IntOrString is **quoted**, an Int-typed one is a bare
// number, and a Quantity is **quoted** — which also answers #1907: Quantity
// already renders correctly through `BadValue::String`, and switching it to
// the Stringer form would have broken it.
//
// None of the #1903 tests caught the regression because they all asserted
// `e.detail` and never the rendered body. These assert the body.

/// A `Type: String` IntOrString renders quoted, because `json.Marshal` of an
/// `intstr.IntOrString` with `Type: String` emits a JSON string.
#[test]
fn string_typed_max_unavailable_renders_quoted_in_the_error_body() {
    let errs = validate_daemonset(&with_max_unavailable(json!("abc")));
    let e = errs
        .iter()
        .find(|e| e.field == "spec.updateStrategy.rollingUpdate.maxUnavailable")
        .unwrap_or_else(|| panic!("expected a maxUnavailable error, got: {errs:?}"));
    assert_eq!(
        e.error_body(),
        format!("Invalid value: \"abc\": {PERCENT_MSG}"),
        "upstream: `intstr.FromString(\"abc\")` => `Invalid value: \"abc\"`"
    );
}

/// Same for a percent-shaped string that fails the format check — the quoting
/// is a property of the type, not of the content.
#[test]
fn percent_shaped_string_max_unavailable_renders_quoted() {
    let errs = validate_daemonset(&with_max_unavailable(json!("50 %")));
    let e = errs
        .iter()
        .find(|e| e.field == "spec.updateStrategy.rollingUpdate.maxUnavailable")
        .unwrap_or_else(|| panic!("expected a maxUnavailable error, got: {errs:?}"));
    assert_eq!(
        e.error_body(),
        format!("Invalid value: \"50 %\": {PERCENT_MSG}")
    );
}

/// An `Type: Int` IntOrString renders as a bare number, never `"-1"`.
#[test]
fn int_typed_max_unavailable_renders_unquoted_in_the_error_body() {
    let errs = validate_daemonset(&with_max_unavailable(json!(-1)));
    let e = errs
        .iter()
        .find(|e| e.field == "spec.updateStrategy.rollingUpdate.maxUnavailable")
        .unwrap_or_else(|| panic!("expected a maxUnavailable error, got: {errs:?}"));
    assert_eq!(
        e.error_body(),
        "Invalid value: -1: must be greater than or equal to 0",
        "upstream: `intstr.FromInt32(-1)` => `Invalid value: -1`"
    );
}
