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

use crate::resources::{SelfSubjectAccessReviewSpec, SubjectAccessReviewSpec};
use crate::validation::field::{BadValue, Error, ErrorList, Path};

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
