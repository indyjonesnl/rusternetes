//! Deployment field validation — port of upstream Kubernetes
//! `pkg/apis/apps/validation/validation.go` (release-1.35).
//!
//! Deployment entry points, as the strategies in
//! `pkg/registry/apps/deployment/strategy.go` call them:
//! * [`validate_deployment`] — `ValidateDeployment`, on create.
//! * [`validate_deployment_update`] — `ValidateDeploymentUpdate`.
//! * [`validate_deployment_status_update`] — `ValidateDeploymentStatusUpdate`,
//!   on `/status`.
//!
//! Mirrors upstream structure: validators return [`ErrorList`] and *accumulate*
//! every problem rather than short-circuiting on the first failure. Field paths
//! and error wording match upstream byte-for-byte so conformance log greps
//! stay valid.
//!
//! Upstream:
//! <https://github.com/kubernetes/kubernetes/blob/release-1.35/pkg/apis/apps/validation/validation.go>

use crate::resources::deployment::{
    Deployment, DeploymentStatus, DeploymentStrategy, RollingUpdateDeployment,
};
use crate::resources::policy::IntOrString;
use crate::resources::workloads::{
    DaemonSet, DaemonSetSpec, DaemonSetStatus, ReplicaSet, ReplicaSetSpec, ReplicaSetStatus,
    RollingUpdateStatefulSetStrategy, StatefulSet, StatefulSetPersistentVolumeClaimRetentionPolicy,
    StatefulSetSpec, StatefulSetStatus,
};
use crate::resources::PodTemplateSpec;
use crate::types::LabelSelector;
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, validate_label_selector, validate_labels, LabelSelectorValidationOptions,
};
use crate::validation::objectmeta::{
    name_is_dns_label, name_is_dns_subdomain, validate_annotations, validate_immutable_field,
    validate_nonnegative_field, validate_object_meta, validate_object_meta_update,
};
use crate::validation::podtemplate::validate_pod_template_spec_with_old;
use crate::validation::pvc::validate_persistent_volume_claim_spec;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the selector selects nothing (both fields absent or
/// empty). An empty selector is forbidden by upstream.
fn selector_is_empty(sel: &LabelSelector) -> bool {
    sel.match_labels.as_ref().is_none_or(|m| m.is_empty())
        && sel.match_expressions.as_ref().is_none_or(|m| m.is_empty())
}

use crate::validation::metav1::label_selector_matches_labels as selector_matches_labels;

/// Check that the template labels satisfy the selector — the full selector
/// (`matchLabels` + `matchExpressions`), mirroring upstream's
/// `LabelSelectorAsSelector(selector).Matches(template.Labels)` check in
/// `ValidateDeploymentSpec` / `ValidatePodTemplateSpecForRC` / the StatefulSet
/// and DaemonSet equivalents.
pub fn template_labels_match_selector(
    selector: &LabelSelector,
    template_labels: &std::collections::HashMap<String, String>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if !selector_matches_labels(selector, template_labels) {
        errs.push(Error::invalid(
            fld_path,
            template_labels
                .iter()
                .map(|(k2, v2)| format!("{k2}={v2}"))
                .collect::<Vec<_>>()
                .join(","),
            "`selector` does not match template `labels`",
        ));
    }
    errs
}

/// `metav1.LabelSelectorAsSelector(spec.Selector)` and both of its arms, as
/// StatefulSet, Deployment and ReplicaSet spell them:
///
/// ```go
/// selector, err := metav1.LabelSelectorAsSelector(spec.Selector)
/// if err != nil {
///     allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, <detail>))
/// } else {
///     // template labels must match the selector
/// }
/// ```
///
/// (`pkg/apis/apps/validation/validation.go:189-192` with detail `""`,
/// `:652-657` and `:823-828` with `"invalid label selector"`.) DaemonSet only
/// gates on `err == nil` (`:446-449`) and adds nothing, so it keeps calling
/// [`template_labels_match_selector`] directly (#2034).
fn selector_parses_and_matches_template(
    selector: &LabelSelector,
    template_labels: &std::collections::HashMap<String, String>,
    fld_path: &Path,
    invalid_detail: &str,
) -> ErrorList {
    if crate::types::label_selector_as_selector(Some(selector)).is_err() {
        return vec![Error::invalid(
            &fld_path.child("selector"),
            BadValue::marshal(selector),
            invalid_detail,
        )];
    }
    template_labels_match_selector(
        selector,
        template_labels,
        &fld_path.child("template").child("metadata").child("labels"),
    )
}

/// Validate a workload pod template's `restartPolicy` / `activeDeadlineSeconds`
/// constraints shared by StatefulSet, DaemonSet and ReplicaSet. Mirrors upstream
/// (e.g. `ValidatePodTemplateSpecForReplicaSet` lines 847-852,
/// `ValidateStatefulSetSpec` lines 217-222, `ValidateDaemonSetSpec` 456-461):
///
/// * `restartPolicy` must be `Always` → otherwise `NotSupported` on
///   `<path>/restartPolicy`. `None`/empty is treated as `Always` because the
///   api-server defaults it before validation runs (these workloads default to
///   `Always`).
/// * `activeDeadlineSeconds` set → `Forbidden` "activeDeadlineSeconds in <Kind>
///   is not Supported".
///
/// `fld_path` is the `template/spec` path; `kind` is the workload kind used in
/// the Forbidden message (`StatefulSet` / `DaemonSet` / `ReplicaSet`).
fn validate_workload_pod_template_restart_policy(
    pod_spec: &crate::resources::pod::PodSpec,
    fld_path: &Path,
    kind: &str,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // restartPolicy must be Always (treat unset/empty as the defaulted Always).
    match pod_spec.restart_policy.as_deref() {
        None | Some("") | Some("Always") => {}
        Some(other) => errs.push(Error::not_supported(
            &fld_path.child("restartPolicy"),
            other.to_string(),
            &["Always"],
        )),
    }

    // activeDeadlineSeconds is not supported for these workloads.
    if pod_spec.active_deadline_seconds.is_some() {
        errs.push(Error::forbidden(
            &fld_path.child("activeDeadlineSeconds"),
            format!("activeDeadlineSeconds in {kind} is not Supported"),
        ));
    }

    errs
}

/// Upstream `validation.IsValidPercent`
/// (`apimachinery/pkg/util/validation/validation.go:341-352`): the value must
/// match the anchored regex `^[0-9]+%$`. Note this rejects a bare `"1"` — a
/// String IntOrString is *only* ever a percentage upstream.
fn is_valid_percent(s: &str) -> bool {
    match s.strip_suffix('%') {
        Some(digits) => !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    }
}

/// The message upstream emits for a malformed percentage, via
/// `RegexError(percentErrMsg, percentFmt, "1%", "93%")`.
fn percent_format_error() -> String {
    crate::validation::metav1::regex_error(
        "a valid percent string must be a numeric string followed by an ending '%'",
        "[0-9]+%",
        &["1%", "93%"],
    )
}

/// Upstream `getPercentValue` (`pkg/apis/apps/validation/validation.go:562-571`):
/// only a String that passes `IsValidPercent` yields a percentage.
fn percent_value(v: &IntOrString) -> Option<i64> {
    let IntOrString::String(s) = v else {
        return None;
    };
    if !is_valid_percent(s) {
        return None;
    }
    s[..s.len() - 1].parse::<i64>().ok()
}

/// Upstream `getIntOrPercentValue` (lines 573-579): the percentage if there is
/// one, otherwise `IntValue()`.
///
/// `IntValue()` on a String does `strconv.Atoi(StrVal)` and swallows the error,
/// so a malformed `"2"` still reports 2 here even though
/// `ValidatePositiveIntOrPercent` rejects its format. Both behaviours are
/// upstream's, and the zero/non-zero switch depends on this leniency.
fn int_or_percent_value(v: &IntOrString) -> i64 {
    if let Some(pct) = percent_value(v) {
        return pct;
    }
    match v {
        IntOrString::Int(i) => *i as i64,
        IntOrString::String(s) => s.parse::<i64>().unwrap_or(0),
    }
}

/// Render an int-or-string as the `bad_value` of an error, the way upstream's
/// `field.Error.ErrorBody()` does.
///
/// `ErrorBody`'s `default:` arm marshals the value to JSON **first**
/// (`staging/src/k8s.io/apimachinery/pkg/util/validation/field/errors.go:92-97`)
/// and only reaches `fmt.Stringer` if that marshalling *errors*:
///
/// ```go
/// valstr := ""
/// jb, err := json.Marshal(e.BadValue)
/// if err == nil {
///     valstr = string(jb)
/// } else if stringer, ok := e.BadValue.(fmt.Stringer); ok {
///     valstr = stringer.String()
/// }
/// ```
///
/// `intstr.IntOrString` has a `MarshalJSON`, so it never takes the Stringer
/// path: a `Type: String` value renders **quoted** and a `Type: Int` value as a
/// bare number. Verified against the pinned checkout:
///
/// ```text
/// intstr.FromString("abc") => Invalid value: "abc"
/// intstr.FromInt32(5)      => Invalid value: 5
/// ```
///
/// So this is `BadValue::Json`, not `BadValue::Stringer`. The `fmt.Stringer`
/// switch that #1903 ported — with a `case fmt.Stringer:` sitting right after
/// the simple types, which *would* have made this unquoted — is the **kustomize
/// fork** of `field.Error`
/// (`vendor/sigs.k8s.io/kustomize/kyaml/yaml/internal/k8sgen/pkg/util/validation/field/errors.go:49-73`),
/// not apimachinery's. Same file path, different package, opposite behaviour.
fn int_or_percent_display(v: &IntOrString) -> BadValue {
    BadValue::Json(v.to_json())
}

/// Mirrors upstream `ValidatePositiveIntOrPercent`
/// (`pkg/apis/apps/validation/validation.go:547-560`), which dispatches on the
/// IntOrString **`Type`**, not on the text of a stringified value:
///
/// - `String` → must satisfy `IsValidPercent`;
/// - `Int` → must satisfy `ValidateNonnegativeField`;
/// - anything else → "must be an integer or percentage".
///
/// Percent magnitude is deliberately not bounded here; upstream applies
/// `IsNotMoreThan100Percent` separately, and only to the fields it bounds.
///
/// Returns `(is_zero, errors)`, where `is_zero` comes from
/// `int_or_percent_value` so it matches upstream's `getIntOrPercentValue` even
/// for a value whose format was just rejected.
pub fn validate_positive_int_or_percent(v: &IntOrString, fld_path: &Path) -> (bool, ErrorList) {
    let mut errs: ErrorList = Vec::new();
    match v {
        IntOrString::String(s) => {
            if !is_valid_percent(s) {
                errs.push(Error::invalid(
                    fld_path,
                    int_or_percent_display(v),
                    percent_format_error(),
                ));
            }
        }
        IntOrString::Int(i) => {
            if *i < 0 {
                errs.push(Error::invalid(
                    fld_path,
                    *i,
                    "must be greater than or equal to 0",
                ));
            }
        }
    }
    (int_or_percent_value(v) == 0, errs)
}

/// Mirrors upstream `IsNotMoreThan100Percent`
/// (`pkg/apis/apps/validation/validation.go:583-591`): only a *valid* percent
/// string is bounded, and only above 100. Integers and malformed strings are
/// left to `ValidatePositiveIntOrPercent`.
pub fn is_not_more_than_100_percent(v: &IntOrString, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(val) = percent_value(v) {
        if val > 100 {
            errs.push(Error::invalid(
                fld_path,
                int_or_percent_display(v),
                "must not be greater than 100%",
            ));
        }
    }
    errs
}

// ---------------------------------------------------------------------------
// Strategy validation
// ---------------------------------------------------------------------------

/// Validate `spec.strategy`. Mirrors upstream `ValidateDeploymentStrategy`.
fn validate_deployment_strategy(strategy: &DeploymentStrategy, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    match strategy.strategy_type.as_str() {
        "Recreate" => {
            // rollingUpdate is forbidden when strategy == Recreate
            if strategy.rolling_update.is_some() {
                errs.push(Error::forbidden(
                    &fld_path.child("rollingUpdate"),
                    "may not be specified when strategy `type` is `Recreate`",
                ));
            }
        }
        "RollingUpdate" => {
            errs.extend(validate_rolling_update_deployment(
                strategy.rolling_update.as_ref(),
                &fld_path.child("rollingUpdate"),
            ));
        }
        other => {
            errs.push(Error::not_supported(
                &fld_path.child("type"),
                other.to_string(),
                &["Recreate", "RollingUpdate"],
            ));
        }
    }

    errs
}

/// Validate `spec.strategy.rollingUpdate`. Mirrors upstream
/// `ValidateRollingUpdateDeployment`.
fn validate_rolling_update_deployment(
    ru: Option<&RollingUpdateDeployment>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // Default: 25% / 25% (handled upstream by defaulting, but we still
    // validate whatever is present)
    let ru = match ru {
        None => return errs, // nothing to validate if absent
        Some(r) => r,
    };

    let max_unavailable_zero;
    let max_surge_zero;

    // maxUnavailable
    match &ru.max_unavailable {
        None => {
            max_unavailable_zero = true; // treat absent as 0 for the both-zero check
        }
        Some(v) => {
            let mu_path = fld_path.child("maxUnavailable");
            let (is_zero, sub_errs) = validate_positive_int_or_percent(v, &mu_path);
            max_unavailable_zero = is_zero;
            errs.extend(sub_errs);
        }
    }

    // maxSurge — upstream `ValidatePositiveIntOrPercent` only; NO 100% bound
    // (lines 596-597), so `maxSurge: 200%` is valid for a Deployment.
    match &ru.max_surge {
        None => {
            max_surge_zero = true; // treat absent as 0 for the both-zero check
        }
        Some(v) => {
            let (is_zero, sub_errs) =
                validate_positive_int_or_percent(v, &fld_path.child("maxSurge"));
            max_surge_zero = is_zero;
            errs.extend(sub_errs);
        }
    }

    // Both cannot be zero simultaneously (upstream reports on maxUnavailable).
    if max_unavailable_zero && max_surge_zero {
        errs.push(Error::invalid(
            &fld_path.child("maxUnavailable"),
            "".to_string(),
            "may not be 0 when `maxSurge` is 0",
        ));
    }

    // Validate that maxUnavailable is not more than 100% (upstream line 603).
    // maxSurge is intentionally NOT bounded here for Deployments.
    if let Some(v) = &ru.max_unavailable {
        errs.extend(is_not_more_than_100_percent(
            v,
            &fld_path.child("maxUnavailable"),
        ));
    }

    errs
}

// ---------------------------------------------------------------------------
// Spec validation
// ---------------------------------------------------------------------------

/// Validate `spec`. Mirrors upstream `ValidateDeploymentSpec`.
// `old_template`: pod options come from the stored template on update
// (`GetValidationOptionsFromPodTemplate(new, old)`, pkg/registry/apps/
// {deployment,replicaset,daemonset,statefulset}/strategy.go), `None` on create.
fn validate_deployment_spec(
    spec: &crate::resources::deployment::DeploymentSpec,
    old_template: Option<&PodTemplateSpec>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // replicas must be non-negative
    if let Some(r) = spec.replicas {
        if r < 0 {
            errs.push(Error::invalid(
                &fld_path.child("replicas"),
                r,
                "must be greater than or equal to 0",
            ));
        }
    }

    // selector is required and must be non-empty. Upstream (lines 642-649)
    // first validates the selector structure, then emits
    // Invalid("empty selector is invalid for deployment") when both matchLabels
    // and matchExpressions are empty. (The `Required` "nil selector" arm cannot
    // happen here — `LabelSelector` is non-optional in rusternetes.)
    if selector_is_empty(&spec.selector) {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        errs.push(Error::invalid(
            &fld_path.child("selector"),
            "{}".to_string(),
            "empty selector is invalid for deployment",
        ));
    } else {
        // validate selector structure
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));

        // template labels must match selector
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();

        errs.extend(selector_parses_and_matches_template(
            &spec.selector,
            &template_labels,
            fld_path,
            "invalid label selector",
        ));
    }

    // strategy
    if let Some(ref strategy) = spec.strategy {
        errs.extend(validate_deployment_strategy(
            strategy,
            &fld_path.child("strategy"),
        ));
    }

    // minReadySeconds must be non-negative
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    // revisionHistoryLimit must be non-negative
    if let Some(rhl) = spec.revision_history_limit {
        if rhl < 0 {
            errs.push(Error::invalid(
                &fld_path.child("revisionHistoryLimit"),
                rhl,
                "must be greater than or equal to 0",
            ));
        }
    }

    // progressDeadlineSeconds must be > minReadySeconds
    if let Some(pds) = spec.progress_deadline_seconds {
        if pds <= 0 {
            errs.push(Error::invalid(
                &fld_path.child("progressDeadlineSeconds"),
                pds,
                "must be greater than 0",
            ));
        } else {
            let min_ready = spec.min_ready_seconds.unwrap_or(0);
            if pds <= min_ready {
                errs.push(Error::invalid(
                    &fld_path.child("progressDeadlineSeconds"),
                    pds,
                    format!("must be greater than minReadySeconds ({min_ready})"),
                ));
            }
        }
    }

    // The template is held to the same rules as a standalone pod: upstream's
    // `ValidateDeploymentSpec` calls `ValidatePodTemplateSpec` on it
    // (`pkg/apis/apps/validation/validation.go:656 via ValidatePodTemplateSpecForReplicaSet`), which is what makes
    // `spec.template.spec.containers: Required value` — not a decoder error —
    // the answer to a workload with no containers (#1939).
    errs.extend(validate_pod_template_spec_with_old(
        &spec.template,
        old_template,
        &fld_path.child("template"),
        false,
    ));
    errs
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validate a new `Deployment`: upstream `ValidateDeployment`
/// (validation.go:733-737), with `ValidateDeploymentName =
/// NameIsDNSSubdomain`.
///
/// Returns an empty `ErrorList` if the object is valid. Each entry in a
/// non-empty list corresponds to one invalid field.
pub fn validate_deployment(d: &Deployment) -> ErrorList {
    let mut errs = validate_object_meta(
        &d.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_deployment_spec(&d.spec, None, &Path::new("spec")));
    errs
}

/// Upstream `ValidateDeploymentStatus` (validation.go:678-706).
pub fn validate_deployment_status(status: &DeploymentStatus, fld_path: &Path) -> ErrorList {
    let replicas = status.replicas.unwrap_or(0);
    let updated = status.updated_replicas.unwrap_or(0);
    let ready = status.ready_replicas.unwrap_or(0);
    let available = status.available_replicas.unwrap_or(0);
    let unavailable = status.unavailable_replicas.unwrap_or(0);

    let mut errs = validate_nonnegative_field(
        status.observed_generation.unwrap_or(0),
        &fld_path.child("observedGeneration"),
    );
    errs.extend(validate_nonnegative_field(
        i64::from(replicas),
        &fld_path.child("replicas"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(updated),
        &fld_path.child("updatedReplicas"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(ready),
        &fld_path.child("readyReplicas"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(available),
        &fld_path.child("availableReplicas"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(unavailable),
        &fld_path.child("unavailableReplicas"),
    ));
    if let Some(terminating) = status.terminating_replicas {
        errs.extend(validate_nonnegative_field(
            i64::from(terminating),
            &fld_path.child("terminatingReplicas"),
        ));
    }
    if let Some(collisions) = status.collision_count {
        errs.extend(validate_nonnegative_field(
            i64::from(collisions),
            &fld_path.child("collisionCount"),
        ));
    }
    let msg = "cannot be greater than status.replicas";
    if updated > replicas {
        errs.push(Error::invalid(
            &fld_path.child("updatedReplicas"),
            updated,
            msg,
        ));
    }
    if ready > replicas {
        errs.push(Error::invalid(&fld_path.child("readyReplicas"), ready, msg));
    }
    if available > replicas {
        errs.push(Error::invalid(
            &fld_path.child("availableReplicas"),
            available,
            msg,
        ));
    }
    if available > ready {
        errs.push(Error::invalid(
            &fld_path.child("availableReplicas"),
            available,
            "cannot be greater than readyReplicas",
        ));
    }
    errs
}

/// Upstream `IsDecremented` (core/validation/validation.go:8717-8725).
fn is_decremented(update: Option<i32>, old: Option<i32>) -> bool {
    match (update, old) {
        (None, Some(_)) => true,
        (Some(u), Some(o)) => u < o,
        _ => false,
    }
}

/// Upstream `ValidateDeploymentStatusUpdate` (validation.go:718-730).
pub fn validate_deployment_status_update(new: &Deployment, old: &Deployment) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    let fld_path = Path::new("status");
    let empty = DeploymentStatus::default();
    let new_status = new.status.as_ref().unwrap_or(&empty);
    let old_status = old.status.as_ref().unwrap_or(&empty);
    errs.extend(validate_deployment_status(new_status, &fld_path));
    if is_decremented(new_status.collision_count, old_status.collision_count) {
        errs.push(Error::invalid(
            &fld_path.child("collisionCount"),
            new_status.collision_count.unwrap_or(0),
            "cannot be decremented",
        ));
    }
    errs
}

/// Validate a `ReplicaSetSpec`. Mirrors upstream `ValidateReplicaSetSpec`
/// (`pkg/apis/apps/validation/validation.go`): non-negative `replicas` and
/// `minReadySeconds`, a required + structurally-valid `selector`, and template
/// labels that satisfy the selector.
fn validate_replicaset_spec(
    spec: &ReplicaSetSpec,
    old_template: Option<&PodTemplateSpec>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // replicas must be non-negative
    if spec.replicas < 0 {
        errs.push(Error::invalid(
            &fld_path.child("replicas"),
            spec.replicas,
            "must be greater than or equal to 0",
        ));
    }

    // minReadySeconds must be non-negative
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    // selector is required and must be non-empty + structurally valid; the
    // template's labels must satisfy it. Upstream (lines 813-820) emits
    // Invalid("empty selector is invalid for deployment") — note ReplicaSet
    // reuses the "deployment" string verbatim.
    if selector_is_empty(&spec.selector) {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        errs.push(Error::invalid(
            &fld_path.child("selector"),
            "{}".to_string(),
            "empty selector is invalid for deployment",
        ));
    } else {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        errs.extend(selector_parses_and_matches_template(
            &spec.selector,
            &template_labels,
            fld_path,
            "invalid label selector",
        ));
    }

    // template.spec.restartPolicy must be Always; activeDeadlineSeconds
    // forbidden (upstream `ValidatePodTemplateSpecForReplicaSet`, lines 847-852).
    errs.extend(validate_workload_pod_template_restart_policy(
        &spec.template.spec,
        &fld_path.child("template").child("spec"),
        "ReplicaSet",
    ));

    // The template is held to the same rules as a standalone pod: upstream's
    // `ValidateReplicaSetSpec` calls `ValidatePodTemplateSpec` on it
    // (`pkg/apis/apps/validation/validation.go:847 via ValidatePodTemplateSpecForReplicaSet`), which is what makes
    // `spec.template.spec.containers: Required value` — not a decoder error —
    // the answer to a workload with no containers (#1939).
    errs.extend(validate_pod_template_spec_with_old(
        &spec.template,
        old_template,
        &fld_path.child("template"),
        false,
    ));
    errs
}

/// Validate a new `ReplicaSet`: upstream `ValidateReplicaSet`
/// (validation.go:756-760), with `ValidateReplicaSetName =
/// NameIsDNSSubdomain`.
pub fn validate_replicaset(rs: &ReplicaSet) -> ErrorList {
    let mut errs = validate_object_meta(
        &rs.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_replicaset_spec(&rs.spec, None, &Path::new("spec")));
    errs
}

/// Upstream `ValidateReplicaSetUpdate` (validation.go:763-769): the metadata
/// update, the new spec, and the immutable selector.
pub fn validate_replicaset_update(new: &ReplicaSet, old: &ReplicaSet) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_replicaset_spec(
        &new.spec,
        Some(&old.spec.template),
        &Path::new("spec"),
    ));
    errs.extend(validate_immutable_field(
        &new.spec.selector,
        &old.spec.selector,
        &Path::new("spec").child("selector"),
    ));
    errs
}

/// Upstream `ValidateReplicaSetStatusUpdate` (validation.go:772-777).
pub fn validate_replicaset_status_update(new: &ReplicaSet, old: &ReplicaSet) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    let empty = ReplicaSetStatus::default();
    errs.extend(validate_replicaset_status(
        new.status.as_ref().unwrap_or(&empty),
        &Path::new("status"),
    ));
    errs
}

/// Upstream `ValidateReplicaSetStatus` (validation.go:780-804).
pub fn validate_replicaset_status(status: &ReplicaSetStatus, fld_path: &Path) -> ErrorList {
    let replicas = status.replicas;
    let fully_labeled = status.fully_labeled_replicas.unwrap_or(0);
    let ready = status.ready_replicas;
    let available = status.available_replicas;

    let mut errs = validate_nonnegative_field(i64::from(replicas), &fld_path.child("replicas"));
    errs.extend(validate_nonnegative_field(
        i64::from(fully_labeled),
        &fld_path.child("fullyLabeledReplicas"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(ready),
        &fld_path.child("readyReplicas"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(available),
        &fld_path.child("availableReplicas"),
    ));
    errs.extend(validate_nonnegative_field(
        status.observed_generation.unwrap_or(0),
        &fld_path.child("observedGeneration"),
    ));
    if let Some(terminating) = status.terminating_replicas {
        errs.extend(validate_nonnegative_field(
            i64::from(terminating),
            &fld_path.child("terminatingReplicas"),
        ));
    }
    let msg = "cannot be greater than status.replicas";
    if fully_labeled > replicas {
        errs.push(Error::invalid(
            &fld_path.child("fullyLabeledReplicas"),
            fully_labeled,
            msg,
        ));
    }
    if ready > replicas {
        errs.push(Error::invalid(&fld_path.child("readyReplicas"), ready, msg));
    }
    if available > replicas {
        errs.push(Error::invalid(
            &fld_path.child("availableReplicas"),
            available,
            msg,
        ));
    }
    if available > ready {
        errs.push(Error::invalid(
            &fld_path.child("availableReplicas"),
            available,
            "cannot be greater than readyReplicas",
        ));
    }
    errs
}

/// Validate a `StatefulSetSpec`. Mirrors upstream `ValidateStatefulSetSpec`
/// (`pkg/apis/apps/validation/validation.go`). Intended to run *after*
/// defaulting (the api-server defaults `podManagementPolicy` and
/// `updateStrategy`), so the "required when empty" arms match upstream without
/// rejecting objects that merely relied on defaulting.
/// Upstream `StatefulSetValidationOptions` (validation.go:38-45).
#[derive(Debug, Clone, Copy, Default)]
pub struct StatefulSetValidationOptions {
    /// Allow an invalid DNS-1123 `serviceName`.
    pub allow_invalid_service_name: bool,
    /// Skip validating the pod template spec (StatefulSet update).
    pub skip_validate_pod_template_spec: bool,
    /// Skip validating the volume claim templates (StatefulSet update).
    pub skip_validate_volume_claim_templates: bool,
}

fn validate_statefulset_spec(
    spec: &StatefulSetSpec,
    old_template: Option<&PodTemplateSpec>,
    fld_path: &Path,
    set_opts: StatefulSetValidationOptions,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // podManagementPolicy: OrderedReady | Parallel.
    match spec.pod_management_policy.as_deref() {
        None | Some("") => errs.push(Error::required(&fld_path.child("podManagementPolicy"), "")),
        Some("OrderedReady") | Some("Parallel") => {}
        Some(other) => errs.push(Error::invalid(
            &fld_path.child("podManagementPolicy"),
            other.to_string(),
            "must be 'OrderedReady' or 'Parallel'",
        )),
    }

    // updateStrategy: RollingUpdate | OnDelete.
    let us_path = fld_path.child("updateStrategy");
    match &spec.update_strategy {
        None => errs.push(Error::required(&us_path, "")),
        Some(us) => match us.strategy_type.as_deref() {
            None | Some("") => errs.push(Error::required(&us_path, "")),
            Some("OnDelete") => {
                if us.rolling_update.is_some() {
                    errs.push(Error::invalid(
                        &us_path.child("rollingUpdate"),
                        "<set>".to_string(),
                        "only allowed for updateStrategy 'RollingUpdate'",
                    ));
                }
            }
            Some("RollingUpdate") => {
                if let Some(ru) = &us.rolling_update {
                    errs.extend(validate_rolling_update_statefulset(
                        ru,
                        &us_path.child("rollingUpdate"),
                    ));
                }
            }
            Some(other) => errs.push(Error::invalid(
                &us_path,
                other.to_string(),
                "must be 'RollingUpdate' or 'OnDelete'",
            )),
        },
    }

    // ValidatePersistentVolumeClaimRetentionPolicy (validation.go:93-100,
    // called at :154) and validateVolumeClaimTemplates (:118-125, :155-157).
    errs.extend(validate_pvc_retention_policy(
        spec.persistent_volume_claim_retention_policy.as_ref(),
        &fld_path.child("persistentVolumeClaimRetentionPolicy"),
    ));
    if !set_opts.skip_validate_volume_claim_templates {
        for (i, pvc) in spec.volume_claim_templates.iter().flatten().enumerate() {
            errs.extend(validate_persistent_volume_claim_spec(
                &pvc.spec,
                &fld_path
                    .child("volumeClaimTemplates")
                    .index(i)
                    .child("spec"),
            ));
        }
    }

    // replicas >= 0
    if let Some(r) = spec.replicas {
        if r < 0 {
            errs.push(Error::invalid(
                &fld_path.child("replicas"),
                r,
                "must be greater than or equal to 0",
            ));
        }
    }

    // minReadySeconds >= 0
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    // ordinals.start >= 0
    if let Some(ords) = &spec.ordinals {
        if let Some(start) = ords.start {
            if start < 0 {
                errs.push(Error::invalid(
                    &fld_path.child("ordinals.start"),
                    start,
                    "must be greater than or equal to 0",
                ));
            }
        }
    }

    // serviceName, when set, must be a DNS-1123 label (validation.go:175-177).
    if !set_opts.allow_invalid_service_name && !spec.service_name.is_empty() {
        for msg in is_dns1123_label(&spec.service_name) {
            errs.push(Error::invalid(
                &fld_path.child("serviceName"),
                spec.service_name.clone(),
                msg,
            ));
        }
    }

    // selector is required and must be non-empty + valid; template labels must
    // satisfy it. Upstream (lines 179-187) emits
    // Invalid("empty selector is invalid for statefulset") when present-but-empty.
    if selector_is_empty(&spec.selector) {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        errs.push(Error::invalid(
            &fld_path.child("selector"),
            "{}".to_string(),
            "empty selector is invalid for statefulset",
        ));
    } else {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        errs.extend(selector_parses_and_matches_template(
            &spec.selector,
            &template_labels,
            fld_path,
            "",
        ));
    }

    // template.spec.restartPolicy must be Always; activeDeadlineSeconds
    // forbidden (upstream lines 217-222).
    errs.extend(validate_workload_pod_template_restart_policy(
        &spec.template.spec,
        &fld_path.child("template").child("spec"),
        "StatefulSet",
    ));

    // The template is held to the same rules as a standalone pod: upstream's
    // `ValidateStatefulSetSpec` calls `ValidatePodTemplateSpec` on it
    // (`pkg/apis/apps/validation/validation.go:214 via ValidatePodTemplateSpecForStatefulSet`), which is what makes
    // `spec.template.spec.containers: Required value` — not a decoder error —
    // the answer to a workload with no containers (#1939). An update skips
    // it when the stored object's own spec is already invalid (:71-73).
    //
    // Before that, `subdomain` and `hostname` are cleared (the controller
    // overwrites them in initIdentity) and the volumeClaimTemplates' volumes
    // are merged in so a mount of a claim-template volume resolves
    // (validation.go:193-213).
    let template_to_validate = template_for_validation(spec);
    let template_path = fld_path.child("template");
    if !set_opts.skip_validate_pod_template_spec {
        errs.extend(validate_pod_template_spec_with_old(
            &template_to_validate,
            old_template,
            &template_path,
            false,
        ));
    } else if let Some(meta) = &template_to_validate.metadata {
        // ValidatePodTemplateSpecForStatefulSet still runs ValidateLabels /
        // ValidateAnnotations when the template spec itself is skipped (:75-76).
        if let Some(labels) = &meta.labels {
            errs.extend(validate_labels(labels, &template_path.child("labels")));
        }
        if let Some(annotations) = &meta.annotations {
            errs.extend(validate_annotations(
                annotations,
                &template_path.child("annotations"),
            ));
        }
    }
    errs
}

/// Upstream `ValidatePersistentVolumeClaimRetentionPolicy`
/// (validation.go:83-100): each policy must be `Retain` or `Delete`. An absent
/// field is not checked (upstream defaulting turns it into `Retain`).
fn validate_pvc_retention_policy(
    policy: Option<&StatefulSetPersistentVolumeClaimRetentionPolicy>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(policy) = policy else { return errs };
    for (name, value) in [
        ("whenDeleted", &policy.when_deleted),
        ("whenScaled", &policy.when_scaled),
    ] {
        if let Some(v) = value {
            if !matches!(v.as_str(), "Retain" | "Delete") {
                errs.push(Error::not_supported(
                    &fld_path.child(name),
                    v.clone(),
                    &["Retain", "Delete"],
                ));
            }
        }
    }
    errs
}

/// Upstream `validateRollingUpdateStatefulSet` (validation.go:499-516).
fn validate_rolling_update_statefulset(
    ru: &RollingUpdateStatefulSetStrategy,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(p) = ru.partition {
        if p < 0 {
            errs.push(Error::invalid(
                &fld_path.child("partition"),
                p,
                "must be greater than or equal to 0",
            ));
        }
    }
    if let Some(mu) = &ru.max_unavailable {
        let mu_path = fld_path.child("maxUnavailable");
        let (is_zero, sub) = validate_positive_int_or_percent(mu, &mu_path);
        errs.extend(sub);
        if is_zero {
            errs.push(Error::invalid(
                &mu_path,
                int_or_percent_display(mu),
                "cannot be 0",
            ));
        }
        errs.extend(is_not_more_than_100_percent(mu, &mu_path));
    }
    errs
}

/// The template upstream validates (validation.go:193-213): `subdomain` and
/// `hostname` cleared, and one `persistentVolumeClaim` volume per
/// volumeClaimTemplate (`volumesToAddForTemplates`, :102-116) merged in ahead
/// of the template's own volumes of a different name.
fn template_for_validation(spec: &StatefulSetSpec) -> crate::resources::PodTemplateSpec {
    let mut t = spec.template.clone();
    let vcts = spec.volume_claim_templates.as_deref().unwrap_or(&[]);
    if t.spec.subdomain.as_deref().is_none_or(str::is_empty)
        && t.spec.hostname.as_deref().is_none_or(str::is_empty)
        && vcts.is_empty()
    {
        return t;
    }
    t.spec.subdomain = None;
    t.spec.hostname = None;
    if !vcts.is_empty() {
        let mut volumes: Vec<crate::resources::pod::Volume> = Vec::new();
        for v in vcts {
            let name = v.metadata.name.clone();
            if volumes.iter().any(|x| x.name == name) {
                continue; // upstream keys a map by name
            }
            volumes.push(
                serde_json::from_value(serde_json::json!({
                    "name": name,
                    "persistentVolumeClaim": {"claimName": name},
                }))
                .expect("static volume shape"),
            );
        }
        for v in t.spec.volumes.take().unwrap_or_default() {
            if !vcts.iter().any(|c| c.metadata.name == v.name) {
                volumes.push(v);
            }
        }
        t.spec.volumes = Some(volumes);
    }
    t
}

/// Validate a new `StatefulSet`: upstream `ValidateStatefulSet`
/// (validation.go:228-235), whose `ValidateStatefulSetName` is
/// `NameIsDNSLabel` (:50-55). Run after defaulting (see
/// [`validate_statefulset_spec`]).
pub fn validate_statefulset(ss: &StatefulSet) -> ErrorList {
    let mut errs = validate_object_meta(
        &ss.metadata,
        true,
        name_is_dns_label,
        &Path::new("metadata"),
    );
    errs.extend(validate_statefulset_spec(
        &ss.spec,
        None,
        &Path::new("spec"),
        StatefulSetValidationOptions::default(),
    ));
    errs
}

/// Upstream `ValidateStatefulSetStatus` (validation.go:275-307).
pub fn validate_statefulset_status(status: &StatefulSetStatus, fld_path: &Path) -> ErrorList {
    let replicas = status.replicas;
    let ready = status.ready_replicas.unwrap_or(0);
    let current = status.current_replicas.unwrap_or(0);
    let updated = status.updated_replicas.unwrap_or(0);
    let available = status.available_replicas.unwrap_or(0);

    let mut errs = validate_nonnegative_field(i64::from(replicas), &fld_path.child("replicas"));
    for (value, name) in [
        (ready, "readyReplicas"),
        (current, "currentReplicas"),
        (updated, "updatedReplicas"),
        (available, "availableReplicas"),
    ] {
        errs.extend(validate_nonnegative_field(
            i64::from(value),
            &fld_path.child(name),
        ));
    }
    if let Some(generation) = status.observed_generation {
        errs.extend(validate_nonnegative_field(
            generation,
            &fld_path.child("observedGeneration"),
        ));
    }
    if let Some(collisions) = status.collision_count {
        errs.extend(validate_nonnegative_field(
            i64::from(collisions),
            &fld_path.child("collisionCount"),
        ));
    }
    let msg = "cannot be greater than status.replicas";
    for (value, name) in [
        (ready, "readyReplicas"),
        (current, "currentReplicas"),
        (updated, "updatedReplicas"),
        (available, "availableReplicas"),
    ] {
        if value > replicas {
            errs.push(Error::invalid(&fld_path.child(name), value, msg));
        }
    }
    if available > ready {
        errs.push(Error::invalid(
            &fld_path.child("availableReplicas"),
            available,
            "cannot be greater than status.readyReplicas",
        ));
    }
    errs
}

/// Upstream `ValidateStatefulSetStatusUpdate` (validation.go:310-322).
pub fn validate_statefulset_status_update(new: &StatefulSet, old: &StatefulSet) -> ErrorList {
    let empty = StatefulSetStatus::default();
    let new_status = new.status.as_ref().unwrap_or(&empty);
    let old_status = old.status.as_ref().unwrap_or(&empty);
    let mut errs = validate_statefulset_status(new_status, &Path::new("status"));
    errs.extend(validate_object_meta_update(
        &new.metadata,
        &old.metadata,
        &Path::new("metadata"),
    ));
    if is_decremented(new_status.collision_count, old_status.collision_count) {
        errs.push(Error::invalid(
            &Path::new("status").child("collisionCount"),
            new_status.collision_count.unwrap_or(0),
            "cannot be decremented",
        ));
    }
    errs
}

/// Validate a `DaemonSetSpec`. Mirrors upstream `ValidateDaemonSetSpec`
/// (`pkg/apis/apps/validation/validation.go`): a required + valid `selector`
/// whose template labels match, non-negative `minReadySeconds` /
/// `revisionHistoryLimit`, and an `updateStrategy` of `RollingUpdate`
/// (`rollingUpdate` required, with in-range `maxUnavailable`/`maxSurge`) or
/// `OnDelete`. Run after defaulting (the api-server defaults `updateStrategy`).
fn validate_daemonset_spec(
    spec: &DaemonSetSpec,
    old_template: Option<&PodTemplateSpec>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // selector required + valid + non-empty; template labels must match.
    // Upstream (lines 450-452) emits Invalid("empty selector is invalid for
    // daemonset") when present-but-empty.
    if selector_is_empty(&spec.selector) {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        errs.push(Error::invalid(
            &fld_path.child("selector"),
            "{}".to_string(),
            "empty selector is invalid for daemonset",
        ));
    } else {
        errs.extend(validate_label_selector(
            &spec.selector,
            LabelSelectorValidationOptions::default(),
            &fld_path.child("selector"),
        ));
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        errs.extend(template_labels_match_selector(
            &spec.selector,
            &template_labels,
            &fld_path.child("template").child("metadata").child("labels"),
        ));
    }

    // template.spec.restartPolicy must be Always; activeDeadlineSeconds
    // forbidden (upstream lines 456-461).
    errs.extend(validate_workload_pod_template_restart_policy(
        &spec.template.spec,
        &fld_path.child("template").child("spec"),
        "DaemonSet",
    ));

    // minReadySeconds >= 0
    if let Some(mrs) = spec.min_ready_seconds {
        if mrs < 0 {
            errs.push(Error::invalid(
                &fld_path.child("minReadySeconds"),
                mrs,
                "must be greater than or equal to 0",
            ));
        }
    }

    // revisionHistoryLimit >= 0
    if let Some(rhl) = spec.revision_history_limit {
        if rhl < 0 {
            errs.push(Error::invalid(
                &fld_path.child("revisionHistoryLimit"),
                rhl,
                "must be greater than or equal to 0",
            ));
        }
    }

    // updateStrategy: RollingUpdate | OnDelete.
    let us_path = fld_path.child("updateStrategy");
    match spec
        .update_strategy
        .as_ref()
        .and_then(|u| u.strategy_type.as_deref())
    {
        Some("OnDelete") => {}
        Some("RollingUpdate") => {
            // rollingUpdate must be present; validate its int-or-percent fields.
            match spec
                .update_strategy
                .as_ref()
                .and_then(|u| u.rolling_update.as_ref())
            {
                None => errs.push(Error::required(&us_path.child("rollingUpdate"), "")),
                Some(ru) => {
                    // Mirrors upstream `ValidateRollingUpdateDaemonSet`
                    // (lines 474-496): both fields are positive-int-or-percent,
                    // both are bounded to 100%, and exactly one of
                    // maxUnavailable/maxSurge must be non-zero. Absent fields
                    // count as 0 for the mutual-exclusion switch.
                    let ru_path = us_path.child("rollingUpdate");

                    let mut max_unavailable_zero = true;
                    if let Some(mu) = &ru.max_unavailable {
                        let path = ru_path.child("maxUnavailable");
                        let (is_zero, sub) = validate_positive_int_or_percent(mu, &path);
                        max_unavailable_zero = is_zero;
                        errs.extend(sub);
                        errs.extend(is_not_more_than_100_percent(mu, &path));
                    }

                    let mut max_surge_zero = true;
                    if let Some(ms) = &ru.max_surge {
                        let path = ru_path.child("maxSurge");
                        let (is_zero, sub) = validate_positive_int_or_percent(ms, &path);
                        max_surge_zero = is_zero;
                        errs.extend(sub);
                        errs.extend(is_not_more_than_100_percent(ms, &path));
                    }

                    // Exactly one of maxSurge / maxUnavailable must be non-zero.
                    if !max_unavailable_zero && !max_surge_zero {
                        errs.push(Error::invalid(
                            &ru_path.child("maxSurge"),
                            ru.max_surge
                                .as_ref()
                                .map(|ms| ms.to_string())
                                .unwrap_or_default(),
                            "may not be set when maxUnavailable is non-zero",
                        ));
                    } else if max_unavailable_zero && max_surge_zero {
                        errs.push(Error::required(
                            &ru_path.child("maxUnavailable"),
                            "cannot be 0 when maxSurge is 0",
                        ));
                    }
                }
            }
        }
        // Unset/empty/unknown — upstream's default arm is NotSupported.
        other => errs.push(Error::not_supported(
            &us_path,
            other.unwrap_or("").to_string(),
            &["RollingUpdate", "OnDelete"],
        )),
    }

    // The template is held to the same rules as a standalone pod: upstream's
    // `ValidateDaemonSetSpec` calls `ValidatePodTemplateSpec` on it
    // (`pkg/apis/apps/validation/validation.go:454`), which is what makes
    // `spec.template.spec.containers: Required value` — not a decoder error —
    // the answer to a workload with no containers (#1939).
    errs.extend(validate_pod_template_spec_with_old(
        &spec.template,
        old_template,
        &fld_path.child("template"),
        false,
    ));
    errs
}

/// Validate a new `DaemonSet`: upstream `ValidateDaemonSet`
/// (validation.go:371-375), whose `ValidateDaemonSetName` is
/// `NameIsDNSSubdomain` (:540). Run after defaulting (see
/// [`validate_daemonset_spec`]).
pub fn validate_daemonset(ds: &DaemonSet) -> ErrorList {
    let mut errs = validate_object_meta(
        &ds.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_daemonset_spec_with_generation(ds, None));
    errs
}

/// `ValidateDaemonSetSpec` including its `templateGeneration` check
/// (validation.go:463). The internal field is the v1 annotation, see
/// [`DaemonSet::template_generation`].
fn validate_daemonset_spec_with_generation(
    ds: &DaemonSet,
    old_template: Option<&PodTemplateSpec>,
) -> ErrorList {
    let spec_path = Path::new("spec");
    let mut errs = validate_daemonset_spec(&ds.spec, old_template, &spec_path);
    errs.extend(validate_nonnegative_field(
        ds.template_generation(),
        &spec_path.child("templateGeneration"),
    ));
    errs
}

/// Upstream `ValidateDaemonSetUpdate` (validation.go:378-383): the metadata
/// update, the spec update rules, and the new spec.
pub fn validate_daemonset_update(new: &DaemonSet, old: &DaemonSet) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_daemonset_spec_update(new, old, &Path::new("spec")));
    errs.extend(validate_daemonset_spec_with_generation(
        new,
        Some(&old.spec.template),
    ));
    errs
}

/// Upstream `ValidateDaemonSetSpecUpdate` (validation.go:386-406).
fn validate_daemonset_spec_update(new: &DaemonSet, old: &DaemonSet, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    let (new_gen, old_gen) = (new.template_generation(), old.template_generation());
    let path = fld_path.child("templateGeneration");

    // TemplateGeneration shouldn't be decremented
    if new_gen < old_gen {
        errs.push(Error::invalid(&path, new_gen, "must not be decremented"));
    }

    // TemplateGeneration should be increased when and only when template is changed
    let template_updated = !crate::equality::semantic_equal(&new.spec.template, &old.spec.template);
    if new_gen == old_gen && template_updated {
        errs.push(Error::invalid(
            &path,
            new_gen,
            "must be incremented upon template update",
        ));
    } else if new_gen > old_gen && !template_updated {
        errs.push(Error::invalid(
            &path,
            new_gen,
            "must not be incremented without template update",
        ));
    }

    // Spec.Selector is immutable
    errs.extend(validate_immutable_field(
        &new.spec.selector,
        &old.spec.selector,
        &Path::new("spec").child("selector"),
    ));
    errs
}

/// Upstream `validateDaemonSetStatus` (validation.go:409-424).
fn validate_daemonset_status(status: &DaemonSetStatus, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    for (value, name) in [
        (
            i64::from(status.current_number_scheduled),
            "currentNumberScheduled",
        ),
        (i64::from(status.number_misscheduled), "numberMisscheduled"),
        (
            i64::from(status.desired_number_scheduled),
            "desiredNumberScheduled",
        ),
        (i64::from(status.number_ready), "numberReady"),
        (
            status.observed_generation.unwrap_or(0),
            "observedGeneration",
        ),
        (
            i64::from(status.updated_number_scheduled.unwrap_or(0)),
            "updatedNumberScheduled",
        ),
        (
            i64::from(status.number_available.unwrap_or(0)),
            "numberAvailable",
        ),
        (
            i64::from(status.number_unavailable.unwrap_or(0)),
            "numberUnavailable",
        ),
    ] {
        errs.extend(validate_nonnegative_field(value, &fld_path.child(name)));
    }
    if let Some(collisions) = status.collision_count {
        errs.extend(validate_nonnegative_field(
            i64::from(collisions),
            &fld_path.child("collisionCount"),
        ));
    }
    errs
}

/// Upstream `ValidateDaemonSetStatusUpdate` (validation.go:426-438).
pub fn validate_daemonset_status_update(new: &DaemonSet, old: &DaemonSet) -> ErrorList {
    let empty = DaemonSetStatus::default();
    let new_status = new.status.as_ref().unwrap_or(&empty);
    let old_status = old.status.as_ref().unwrap_or(&empty);
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_daemonset_status(new_status, &Path::new("status")));
    if is_decremented(new_status.collision_count, old_status.collision_count) {
        errs.push(Error::invalid(
            &Path::new("status").child("collisionCount"),
            new_status.collision_count.unwrap_or(0),
            "cannot be decremented",
        ));
    }
    errs
}

/// Validate a `Deployment` update (`new` replaces `old`): upstream
/// `ValidateDeploymentUpdate` (validation.go:709-714) — the metadata update,
/// the new spec, and the immutable selector.
pub fn validate_deployment_update(new: &Deployment, old: &Deployment) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_deployment_spec(
        &new.spec,
        Some(&old.spec.template),
        &Path::new("spec"),
    ));
    errs.extend(validate_immutable_field(
        &new.spec.selector,
        &old.spec.selector,
        &Path::new("spec").child("selector"),
    ));
    errs
}

/// Upstream `ValidateStatefulSetUpdate` (validation.go:238-272): the
/// metadata update, the new spec (tolerating an invalid `serviceName` and, if
/// the stored spec is itself invalid, the pod template), and a `Forbidden`
/// for any change outside the mutable fields — decided by clearing those
/// fields and comparing with `Semantic.DeepEqual`.
pub fn validate_statefulset_update(new: &StatefulSet, old: &StatefulSet) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    let mut set_opts = StatefulSetValidationOptions {
        allow_invalid_service_name: true,
        skip_validate_volume_claim_templates: true,
        ..StatefulSetValidationOptions::default()
    };
    if !validate_statefulset_spec(&old.spec, None, &Path::new("spec"), set_opts).is_empty() {
        set_opts.skip_validate_pod_template_spec = true;
    }
    errs.extend(validate_statefulset_spec(
        &new.spec,
        Some(&old.spec.template),
        &Path::new("spec"),
        set_opts,
    ));

    let mut clone = new.spec.clone();
    clone.replicas = old.spec.replicas;
    clone.template = old.spec.template.clone();
    clone.update_strategy = old.spec.update_strategy.clone();
    clone.min_ready_seconds = old.spec.min_ready_seconds;
    clone.ordinals = old.spec.ordinals.clone();
    clone.revision_history_limit = old.spec.revision_history_limit;
    clone.persistent_volume_claim_retention_policy =
        old.spec.persistent_volume_claim_retention_policy.clone();
    if !crate::equality::semantic_equal(&clone, &old.spec) {
        errs.push(Error::forbidden(
            &Path::new("spec"),
            "updates to statefulset spec for fields other than 'replicas', 'ordinals', 'template', 'updateStrategy', 'revisionHistoryLimit', 'persistentVolumeClaimRetentionPolicy' and 'minReadySeconds' are forbidden",
        ));
    }
    errs
}
