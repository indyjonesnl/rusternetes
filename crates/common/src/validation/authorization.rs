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
//! The `metadata` must-be-empty rule (`validation.go:65-71`, `:78-84`,
//! `:92-97`) is ported here too. A review is a virtual object:
//! `subjectaccessreview.REST.Create`
//! (`pkg/registry/authorization/subjectaccessreview/rest.go:63-96`) never calls
//! `rest.BeforeCreate`, so nothing mints a uid or a creation timestamp and the
//! validator sees exactly what the client sent (#1944).

use crate::resources::authorization::{
    FieldSelectorAttributes, FieldSelectorRequirement, LabelSelectorAttributes,
    LabelSelectorRequirement, ResourceAttributes,
};
use crate::resources::{
    LocalSubjectAccessReview, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
    SubjectAccessReview, SubjectAccessReviewSpec,
};
use crate::types::ObjectMeta;
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
            BadValue::marshal(&spec.non_resource_attributes),
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
            BadValue::marshal(&spec.non_resource_attributes),
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
    review: &LocalSubjectAccessReview,
    namespace: &str,
) -> ErrorList {
    let spec = &review.spec;
    let mut errs = validate_subject_access_review_spec(spec, &Path::new("spec"));

    // Upstream clears `Namespace` as well as `ManagedFields` before comparing,
    // and says so in the message: by the time the registry runs,
    // `EnsureObjectNamespaceMatchesRequestNamespace`
    // (`staging/src/k8s.io/apiserver/pkg/registry/rest/meta.go:47-68`) has
    // already defaulted `metadata.namespace` from the request path.
    let mut meta = review.metadata.clone();
    meta.namespace = None;
    if let Some(err) = metadata_must_be_empty(
        &meta,
        &review.metadata,
        "must be empty except for namespace",
    ) {
        errs.push(err);
    }

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
            BadValue::marshal(&spec.non_resource_attributes),
            "disallowed on this kind of request",
        ));
    }

    errs
}

/// The shared half of the three `metadata` rules (`validation.go:65-71`,
/// `:78-84`, `:92-97`):
///
/// ```go
/// objectMetaShallowCopy := sar.ObjectMeta
/// objectMetaShallowCopy.ManagedFields = nil
/// if !apiequality.Semantic.DeepEqual(metav1.ObjectMeta{}, objectMetaShallowCopy) {
///     allErrs = append(allErrs, field.Invalid(field.NewPath("metadata"), sar.ObjectMeta, `must be empty`))
/// }
/// ```
///
/// `compared` is the shallow copy the caller has already blanked the exempt
/// fields on; `reported` is the untouched metadata, which is what upstream puts
/// in the error as the offending value.
fn metadata_must_be_empty(
    compared: &ObjectMeta,
    reported: &ObjectMeta,
    detail: &str,
) -> Option<Error> {
    let mut compared = compared.clone();
    compared.managed_fields = None;
    if compared == ObjectMeta::default() {
        return None;
    }
    Some(Error::invalid(
        &Path::new("metadata"),
        BadValue::marshal(reported),
        detail,
    ))
}

/// `ValidateSubjectAccessReview` (`validation.go:63-72`).
pub fn validate_subject_access_review(review: &SubjectAccessReview) -> ErrorList {
    let mut errs = validate_subject_access_review_spec(&review.spec, &Path::new("spec"));
    if let Some(err) = metadata_must_be_empty(&review.metadata, &review.metadata, "must be empty") {
        errs.push(err);
    }
    errs
}

/// `ValidateSelfSubjectAccessReview` (`validation.go:74-82`).
pub fn validate_self_subject_access_review(review: &SelfSubjectAccessReview) -> ErrorList {
    let mut errs = validate_self_subject_access_review_spec(&review.spec, &Path::new("spec"));
    if let Some(err) = metadata_must_be_empty(&review.metadata, &review.metadata, "must be empty") {
        errs.push(err);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// #2190: `metadata` renders in Go struct-field order
    /// (`ObjectMeta`: name, resourceVersion, generation, ..., labels), with
    /// map keys sorted, not as an alphabetically sorted object. Upstream:
    /// `field/errors.go:92-97` marshals the value with `json.Marshal`.
    #[test]
    fn metadata_must_be_empty_renders_in_declaration_order() {
        let meta = ObjectMeta {
            name: "n".into(),
            resource_version: Some("7".into()),
            generation: Some(2),
            labels: Some(
                [
                    ("b".to_string(), "1".to_string()),
                    ("a".to_string(), "2".to_string()),
                ]
                .into(),
            ),
            ..Default::default()
        };
        let e = metadata_must_be_empty(&meta, &meta, "must be empty").unwrap();
        assert_eq!(
            e.error_body(),
            r#"Invalid value: {"name":"n","resourceVersion":"7","generation":2,"labels":{"a":"2","b":"1"}}: must be empty"#
        );
    }

    /// #2375: the kubelet's webhook authorizer posts its SubjectAccessReview as
    /// protobuf (client-go's default for core clients). Go's generated marshaller
    /// writes every scalar `ObjectMeta` field, so the decoded metadata is
    /// `{"name":"","generation":0}`. Upstream's `DeepEqual(metav1.ObjectMeta{}, ..)`
    /// treats Go zero values as empty; `generation: Some(0)` must too, or every
    /// kubelet authz check answers 422 and the node never goes Ready.
    #[test]
    fn metadata_with_only_go_zero_scalars_is_empty() {
        let meta = ObjectMeta {
            generation: Some(0),
            generate_name: Some(String::new()),
            namespace: Some(String::new()),
            resource_version: Some(String::new()),
            ..Default::default()
        };
        assert!(metadata_must_be_empty(&meta, &meta, "must be empty").is_none());
        let review = SubjectAccessReview {
            metadata: meta,
            ..serde_json::from_value(serde_json::json!({
                "spec": {"user": "u", "resourceAttributes": {"verb": "get"}}
            }))
            .unwrap()
        };
        assert!(validate_subject_access_review(&review).is_empty());
    }
}
