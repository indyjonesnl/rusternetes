//! SubjectAccessReview validation — port of upstream Kubernetes
//! `pkg/apis/authorization/validation/validation.go` (release-1.35).
//!
//! The three review kinds share `ValidateSubjectAccessReviewSpec`; the self
//! review drops the user/groups rule (it has no `user` field to name), and the
//! local review adds two rules of its own.
//!
//! These run in the registry, and a failure is `NewInvalid`
//! (`pkg/registry/authorization/subjectaccessreview/rest.go:75-77`) — a
//! terminal 422 naming the field, not the 500 the handlers used to raise for
//! the same input (#1938).
//!
//! **Not ported: the `metadata` must-be-empty rule** (`validation.go:65-71`,
//! `:78-84`, `:92-97`). Upstream compares the whole `ObjectMeta` against the
//! zero value (with `ManagedFields` cleared) and faults any non-empty field;
//! the local review exempts `namespace`. Our handlers stamp metadata before
//! this point, so the check needs the write path reordered first, and it is a
//! wider behavioural change than the status-code fix this module is for.

use crate::resources::authorization::{
    FieldSelectorAttributes, FieldSelectorRequirement, LabelSelectorAttributes,
    LabelSelectorRequirement, ResourceAttributes,
};
use crate::resources::{SelfSubjectAccessReviewSpec, SubjectAccessReviewSpec};
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::{
    validate_label_selector_requirement, LabelSelectorValidationOptions,
};

/// `ValidateSubjectAccessReviewSpec` (`validation.go:31-45`).
///
/// ```go
/// if spec.ResourceAttributes != nil && spec.NonResourceAttributes != nil {
///     allErrs = append(allErrs, field.Invalid(fldPath.Child("nonResourceAttributes"), spec.NonResourceAttributes, `cannot be specified in combination with resourceAttributes`))
/// }
/// if spec.ResourceAttributes == nil && spec.NonResourceAttributes == nil {
///     allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceAttributes"), spec.NonResourceAttributes, `exactly one of nonResourceAttributes or resourceAttributes must be specified`))
/// }
/// if len(spec.User) == 0 && len(spec.Groups) == 0 {
///     allErrs = append(allErrs, field.Invalid(fldPath.Child("user"), spec.User, `at least one of user or group must be specified`))
/// }
/// ```
///
/// Note the second clause names the `resourceAttributes` path but passes
/// `NonResourceAttributes` as the offending value — a nil pointer, so the
/// message reads `Invalid value: null`. Kept verbatim: the rendered error is
/// the contract.
pub fn validate_subject_access_review_spec(
    spec: &SubjectAccessReviewSpec,
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if spec.resource_attributes.is_some() && spec.non_resource_attributes.is_some() {
        errs.push(Error::invalid(
            &path.child("nonResourceAttributes"),
            BadValue::Json(serde_json::to_value(&spec.non_resource_attributes).unwrap_or_default()),
            "cannot be specified in combination with resourceAttributes",
        ));
    }
    if spec.resource_attributes.is_none() && spec.non_resource_attributes.is_none() {
        errs.push(Error::invalid(
            &path.child("resourceAttributes"),
            BadValue::Json(serde_json::Value::Null),
            "exactly one of nonResourceAttributes or resourceAttributes must be specified",
        ));
    }
    if spec.user.as_deref().unwrap_or("").is_empty()
        && spec.groups.as_deref().unwrap_or(&[]).is_empty()
    {
        errs.push(Error::invalid(
            &path.child("user"),
            spec.user.clone().unwrap_or_default(),
            "at least one of user or group must be specified",
        ));
    }

    errs.extend(validate_resource_attributes(
        spec.resource_attributes.as_ref(),
        &path.child("resourceAttributes"),
    ));

    errs
}

/// `ValidateSelfSubjectAccessReviewSpec` (`validation.go:49-60`) — the same two
/// attribute rules, without the user/groups one: the self review authorizes the
/// caller, so there is no subject to name.
pub fn validate_self_subject_access_review_spec(
    spec: &SelfSubjectAccessReviewSpec,
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if spec.resource_attributes.is_some() && spec.non_resource_attributes.is_some() {
        errs.push(Error::invalid(
            &path.child("nonResourceAttributes"),
            BadValue::Json(serde_json::to_value(&spec.non_resource_attributes).unwrap_or_default()),
            "cannot be specified in combination with resourceAttributes",
        ));
    }
    if spec.resource_attributes.is_none() && spec.non_resource_attributes.is_none() {
        errs.push(Error::invalid(
            &path.child("resourceAttributes"),
            BadValue::Json(serde_json::Value::Null),
            "exactly one of nonResourceAttributes or resourceAttributes must be specified",
        ));
    }

    errs.extend(validate_resource_attributes(
        spec.resource_attributes.as_ref(),
        &path.child("resourceAttributes"),
    ));

    errs
}

/// `ValidateLocalSubjectAccessReview` (`validation.go:88-106`), minus the
/// metadata rule noted at the top of this module.
///
/// ```go
/// if sar.Spec.ResourceAttributes != nil && sar.Spec.ResourceAttributes.Namespace != sar.Namespace {
///     allErrs = append(allErrs, field.Invalid(field.NewPath("spec.resourceAttributes.namespace"), sar.Spec.ResourceAttributes.Namespace, `must match metadata.namespace`))
/// }
/// if sar.Spec.NonResourceAttributes != nil {
///     allErrs = append(allErrs, field.Invalid(field.NewPath("spec.nonResourceAttributes"), sar.Spec.NonResourceAttributes, `disallowed on this kind of request`))
/// }
/// ```
///
/// The namespace rule is what keeps a local review scoped to the namespace it
/// was posted to. Without it a caller can post to a namespace they may read and
/// have the server answer about a different one.
pub fn validate_local_subject_access_review(
    spec: &SubjectAccessReviewSpec,
    namespace: &str,
) -> ErrorList {
    let mut errs = validate_subject_access_review_spec(spec, &Path::new("spec"));

    if let Some(resource_attributes) = &spec.resource_attributes {
        let spec_namespace = resource_attributes.namespace.as_deref().unwrap_or("");
        if spec_namespace != namespace {
            errs.push(Error::invalid(
                &Path::new("spec.resourceAttributes.namespace"),
                spec_namespace,
                "must match metadata.namespace",
            ));
        }
    }
    if spec.non_resource_attributes.is_some() {
        errs.push(Error::invalid(
            &Path::new("spec.nonResourceAttributes"),
            BadValue::Json(serde_json::to_value(&spec.non_resource_attributes).unwrap_or_default()),
            "disallowed on this kind of request",
        ));
    }

    errs
}

/// `validateResourceAttributes` (`validation.go:108-118`): nil is fine, and the
/// only rules are the two selector-attribute blocks.
fn validate_resource_attributes(attrs: Option<&ResourceAttributes>, fld_path: &Path) -> ErrorList {
    let Some(attrs) = attrs else {
        return Vec::new();
    };
    let mut errs: ErrorList = Vec::new();
    errs.extend(validate_field_selector_attributes(
        attrs.field_selector.as_ref(),
        &fld_path.child("fieldSelector"),
    ));
    errs.extend(validate_label_selector_attributes(
        attrs.label_selector.as_ref(),
        &fld_path.child("labelSelector"),
    ));
    errs
}

/// `validateFieldSelectorAttributes` (`validation.go:120-139`).
fn validate_field_selector_attributes(
    selector: Option<&FieldSelectorAttributes>,
    fld_path: &Path,
) -> ErrorList {
    let Some(selector) = selector else {
        return Vec::new();
    };
    let raw = selector.raw_selector.as_deref().unwrap_or("");
    let reqs = selector.requirements.as_deref().unwrap_or(&[]);
    let mut errs = validate_raw_versus_requirements(raw, reqs.is_empty(), fld_path);

    // `AllowUnknownOperatorInRequirement: true` — upstream's skew allowance, so
    // a newer client whose operator this server does not know can still be
    // authorized. Without it every unrecognised operator would be a 422.
    for (i, req) in reqs.iter().enumerate() {
        errs.extend(validate_field_selector_requirement(
            req,
            &fld_path.child("requirements").index(i),
        ));
    }
    errs
}

/// `validateLabelSelectorAttributes` (`validation.go:141-162`) — the same shape
/// over `metav1validation.ValidateLabelSelectorRequirement`, which this repo
/// already ports; the requirement type here is the authorization API's own copy
/// of the struct, so it is mapped across rather than re-validated.
fn validate_label_selector_attributes(
    selector: Option<&LabelSelectorAttributes>,
    fld_path: &Path,
) -> ErrorList {
    let Some(selector) = selector else {
        return Vec::new();
    };
    let raw = selector.raw_selector.as_deref().unwrap_or("");
    let reqs = selector.requirements.as_deref().unwrap_or(&[]);
    let mut errs = validate_raw_versus_requirements(raw, reqs.is_empty(), fld_path);

    let opts = LabelSelectorValidationOptions {
        allow_invalid_label_value_in_selector: false,
        allow_unknown_operator_in_requirement: true,
    };
    for (i, req) in reqs.iter().enumerate() {
        errs.extend(validate_label_selector_requirement(
            &to_metav1_requirement(req),
            opts,
            &fld_path.child("requirements").index(i),
        ));
    }
    errs
}

/// The `rawSelector` / `requirements` pairing, identical in both upstream
/// functions (`:126-132` and `:147-153`).
fn validate_raw_versus_requirements(
    raw_selector: &str,
    requirements_empty: bool,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if !raw_selector.is_empty() && !requirements_empty {
        errs.push(Error::invalid(
            &fld_path.child("rawSelector"),
            raw_selector.to_string(),
            "may not specified at the same time as requirements",
        ));
    }
    if raw_selector.is_empty() && requirements_empty {
        errs.push(Error::required(
            &fld_path.child("requirements"),
            format!("when {fld_path} is specified, requirements or rawSelector is required"),
        ));
    }
    errs
}

/// `metav1validation.ValidateFieldSelectorRequirement`
/// (`apimachinery/pkg/apis/meta/v1/validation/validation.go:132-155`) with
/// `AllowUnknownOperatorInRequirement: true`, which is the only way this repo
/// reaches it — so the option is baked in rather than threaded through.
///
/// Unlike the label variant, the key is not a label name: a field selector key
/// is a JSON path, and upstream only requires it to be non-empty.
fn validate_field_selector_requirement(
    req: &FieldSelectorRequirement,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if req.key.is_empty() {
        errs.push(Error::required(&fld_path.child("key"), "must be specified"));
    }
    match req.operator.as_str() {
        "In" | "NotIn" if req.values.as_ref().is_none_or(|v| v.is_empty()) => {
            errs.push(Error::required(
                &fld_path.child("values"),
                "must be specified when `operator` is 'In' or 'NotIn'",
            ));
        }
        "Exists" | "DoesNotExist" if req.values.as_ref().is_some_and(|v| !v.is_empty()) => {
            errs.push(Error::forbidden(
                &fld_path.child("values"),
                "may not be specified when `operator` is 'Exists' or 'DoesNotExist'",
            ));
        }
        // Upstream's default arm is where `AllowUnknownOperatorInRequirement`
        // applies: it raises `not a valid selector operator` only when the
        // option is off, and for a SAR it is on. An operator this server does
        // not know is therefore silent, not a 422.
        _ => {}
    }
    errs
}

/// The authorization API declares its own `LabelSelectorRequirement` rather
/// than reusing `metav1`'s, so map it across to call the one ported predicate.
fn to_metav1_requirement(req: &LabelSelectorRequirement) -> crate::types::LabelSelectorRequirement {
    crate::types::LabelSelectorRequirement {
        key: req.key.clone(),
        operator: req.operator.clone(),
        values: req.values.clone(),
    }
}
