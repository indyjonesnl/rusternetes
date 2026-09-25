//! ValidatingAdmissionPolicy / ValidatingAdmissionPolicyBinding validation and
//! defaulting — port of upstream
//! `pkg/apis/admissionregistration/validation/validation.go` and
//! `pkg/apis/admissionregistration/v1/defaults.go` (release-1.35).
//!
//! Scope: the CEL-free structural half of
//! `validateValidatingAdmissionPolicySpec` (`:772`) —
//! `validateParamKind` (`:832`), `validateMatchResources` (`:886`),
//! `validateMatchConditions` (`:963`), `validateVariable` (`:999`),
//! `validateValidation` (`:1034`), `validateAuditAnnotation` (`:1128`) — plus
//! `validateValidatingAdmissionPolicyBindingSpec` (`:1181`),
//! `validateParamRef` (`:1198`) and `validateValidationActions` (`:926`).
//!
//! Compiling each expression against the admission CEL environment stays in
//! `handlers::cel_validation`, which already does it for a policy's
//! `validations`; what upstream does *around* that compile — the emptiness,
//! whitespace, identifier, duplicate and enum rules — is here.

use crate::resources::admission_webhook::MatchCondition;
use crate::resources::validating_admission_policy::{
    AuditAnnotation, ExpressionWarning, MatchPolicyType, MatchResources, NamedRuleWithOperations,
    ParamKind, ParamRef, ValidatingAdmissionPolicy, ValidatingAdmissionPolicyBinding,
    ValidatingAdmissionPolicyBindingSpec, ValidatingAdmissionPolicySpec, Validation,
    ValidationAction, Variable,
};
use crate::resources::LabelSelector;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1035_label, is_dns1123_subdomain, is_qualified_name, validate_label_selector,
    LabelSelectorValidationOptions,
};
use crate::validation::webhookconfiguration::{to_metav1_selector, validate_rule_parts};
use std::collections::HashSet;

/// `maxAuditAnnotations` (`validation.go:47`).
const MAX_AUDIT_ANNOTATIONS: usize = 20;
/// `maxAuditAnnotationValueExpressionLength` (`validation.go:49`).
const MAX_AUDIT_ANNOTATION_VALUE_EXPRESSION_LENGTH: usize = 5 * 1024;
/// `validateMatchConditions` caps the list at 64 (`validation.go:966`).
const MAX_MATCH_CONDITIONS: usize = 64;

/// `SetDefaults_ValidatingAdmissionPolicySpec` + `SetDefaults_MatchResources`
/// (`pkg/apis/admissionregistration/v1/defaults.go:97-119`).
///
/// Must run before validation: `failurePolicy`, `matchPolicy` and both
/// selectors are `Required` by the validator *because* defaulting fills them.
pub fn set_defaults_validating_admission_policy(policy: &mut ValidatingAdmissionPolicy) {
    let Some(spec) = policy.spec.as_mut() else {
        return;
    };
    if spec.failure_policy.is_none() {
        spec.failure_policy =
            Some(crate::resources::validating_admission_policy::FailurePolicy::Fail);
    }
    if let Some(match_constraints) = spec.match_constraints.as_mut() {
        set_defaults_match_resources(match_constraints);
    }
}

/// `SetDefaults_MatchResources` (`defaults.go:105-119`), shared by a policy's
/// `matchConstraints` and a binding's `matchResources`.
pub fn set_defaults_match_resources(match_resources: &mut MatchResources) {
    if match_resources.match_policy.is_none() {
        match_resources.match_policy = Some(MatchPolicyType::Equivalent);
    }
    if match_resources.namespace_selector.is_none() {
        match_resources.namespace_selector = Some(LabelSelector::default());
    }
    if match_resources.object_selector.is_none() {
        match_resources.object_selector = Some(LabelSelector::default());
    }
}

/// `SetDefaults_MatchResources` on a binding (`defaults.go:105`).
pub fn set_defaults_validating_admission_policy_binding(
    binding: &mut ValidatingAdmissionPolicyBinding,
) {
    if let Some(match_resources) = binding
        .spec
        .as_mut()
        .and_then(|spec| spec.match_resources.as_mut())
    {
        set_defaults_match_resources(match_resources);
    }
}

/// `ValidateValidatingAdmissionPolicy` (`validation.go:745`) minus ObjectMeta,
/// which the handler validates separately.
pub fn validate_validating_admission_policy(policy: &ValidatingAdmissionPolicy) -> ErrorList {
    // An absent `spec` is the zero spec upstream, which fails every one of the
    // rules below rather than decoding to nothing.
    let spec = policy.spec.clone().unwrap_or_default();
    validate_validating_admission_policy_spec(&policy.metadata.name, &spec, &Path::new("spec"))
}

/// `ValidateValidatingAdmissionPolicyBinding` (`validation.go:1170`).
pub fn validate_validating_admission_policy_binding(
    binding: &ValidatingAdmissionPolicyBinding,
) -> ErrorList {
    let spec = binding.spec.clone().unwrap_or_default();
    validate_binding_spec(&spec, &Path::new("spec"))
}

/// `validateValidatingAdmissionPolicySpec` (`validation.go:772-830`).
fn validate_validating_admission_policy_spec(
    policy_name: &str,
    spec: &ValidatingAdmissionPolicySpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // failurePolicy is required (defaulting fills it) — an unsupported value
    // cannot reach here, because the closed Rust enum rejects it in the decoder
    // where upstream answers `NotSupported`.
    if spec.failure_policy.is_none() {
        errs.push(Error::required(&fld_path.child("failurePolicy"), ""));
    }

    if let Some(param_kind) = &spec.param_kind {
        errs.extend(validate_param_kind(
            param_kind,
            &fld_path.child("paramKind"),
        ));
    }

    match &spec.match_constraints {
        None => errs.push(Error::required(&fld_path.child("matchConstraints"), "")),
        Some(match_constraints) => {
            let path = fld_path.child("matchConstraints");
            errs.extend(validate_match_resources(match_constraints, &path));
            // At least one resourceRule must be defined to provide type
            // information (`validation.go:795-798`).
            if match_constraints
                .resource_rules
                .as_ref()
                .is_none_or(Vec::is_empty)
            {
                errs.push(Error::required(&path.child("resourceRules"), ""));
            }
        }
    }

    errs.extend(validate_match_conditions(
        spec.match_conditions.as_deref().unwrap_or_default(),
        &fld_path.child("matchConditions"),
    ));

    for (i, variable) in spec
        .variables
        .as_deref()
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        errs.extend(validate_variable(
            variable,
            &fld_path.child("variables").index(i),
        ));
    }

    let validations = spec.validations.as_deref().unwrap_or_default();
    let audit_annotations = spec.audit_annotations.as_deref().unwrap_or_default();
    if validations.is_empty() && audit_annotations.is_empty() {
        const DETAIL: &str = "validations or auditAnnotations must contain at least one item";
        errs.push(Error::required(&fld_path.child("validations"), DETAIL));
        errs.push(Error::required(&fld_path.child("auditAnnotations"), DETAIL));
        return errs;
    }

    for (i, validation) in validations.iter().enumerate() {
        errs.extend(validate_validation(
            validation,
            &fld_path.child("validations").index(i),
        ));
    }

    let annotations_path = fld_path.child("auditAnnotations");
    if audit_annotations.len() > MAX_AUDIT_ANNOTATIONS {
        errs.push(Error::invalid(
            &annotations_path,
            String::new(),
            format!("must not have more than {MAX_AUDIT_ANNOTATIONS} auditAnnotations"),
        ));
    }
    let mut keys: HashSet<&str> = HashSet::new();
    for (i, annotation) in audit_annotations.iter().enumerate() {
        let path = annotations_path.index(i);
        errs.extend(validate_audit_annotation(annotation, policy_name, &path));
        if !keys.insert(annotation.key.as_str()) {
            errs.push(Error::duplicate(&path.child("key"), annotation.key.clone()));
        }
    }

    errs
}

/// `validateParamKind` (`validation.go:832-884`).
fn validate_param_kind(param_kind: &ParamKind, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let api_version_path = fld_path.child("apiVersion");
    let api_version = param_kind.api_version.clone().unwrap_or_default();

    if api_version.is_empty() {
        errs.push(Error::required(&api_version_path, ""));
    } else {
        // `parseGroupVersion`: "v1" is group-less, "group/version" otherwise,
        // and anything with more than one `/` is an error
        // (`apimachinery/pkg/runtime/schema/group_version.go:100-115`).
        let parts: Vec<&str> = api_version.split('/').collect();
        let (group, version) = match parts.as_slice() {
            [version] => ("", *version),
            [group, version] => (*group, *version),
            _ => {
                errs.push(Error::invalid(
                    &api_version_path,
                    api_version.clone(),
                    format!("unexpected GroupVersion string: {api_version}"),
                ));
                return errs;
            }
        };
        if !group.is_empty() {
            for msg in is_dns1123_subdomain(group) {
                errs.push(Error::invalid(&api_version_path, group.to_string(), msg));
            }
        }
        if version.is_empty() {
            errs.push(Error::invalid(
                &api_version_path,
                api_version.clone(),
                "version must be specified",
            ));
        } else {
            for msg in is_dns1035_label(version) {
                errs.push(Error::invalid(&api_version_path, version.to_string(), msg));
            }
        }
    }

    let kind_path = fld_path.child("kind");
    if param_kind.kind.is_empty() {
        errs.push(Error::required(&kind_path, ""));
    } else {
        let msgs = is_dns1035_label(&param_kind.kind.to_lowercase());
        if !msgs.is_empty() {
            errs.push(Error::invalid(
                &kind_path,
                param_kind.kind.clone(),
                format!(
                    "may have mixed case, but should otherwise match: {}",
                    msgs.join(",")
                ),
            ));
        }
    }
    errs
}

/// `validateMatchResources` (`validation.go:886-918`), shared by a policy's
/// `matchConstraints` and a binding's `matchResources` exactly as upstream
/// shares it.
fn validate_match_resources(match_resources: &MatchResources, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if match_resources.match_policy.is_none() {
        errs.push(Error::required(&fld_path.child("matchPolicy"), ""));
    }
    match &match_resources.namespace_selector {
        None => errs.push(Error::required(&fld_path.child("namespaceSelector"), "")),
        Some(selector) => errs.extend(validate_selector(
            selector,
            &fld_path.child("namespaceSelector"),
        )),
    }
    match &match_resources.object_selector {
        None => errs.push(Error::required(&fld_path.child("objectSelector"), "")),
        Some(selector) => errs.extend(validate_selector(
            selector,
            &fld_path.child("objectSelector"),
        )),
    }

    for (name, rules) in [
        ("resourceRules", &match_resources.resource_rules),
        (
            "excludeResourceRules",
            &match_resources.exclude_resource_rules,
        ),
    ] {
        let path = fld_path.child(name);
        for (i, rule) in rules.as_deref().unwrap_or_default().iter().enumerate() {
            errs.extend(validate_named_rule_with_operations(rule, &path.index(i)));
        }
    }
    errs
}

/// The admissionregistration flavour of `metav1.LabelSelector` through the one
/// `ValidateLabelSelector` port, which is how upstream validates it (`:900`).
fn validate_selector(selector: &LabelSelector, fld_path: &Path) -> ErrorList {
    validate_label_selector(
        &to_metav1_selector(selector),
        LabelSelectorValidationOptions::default(),
        fld_path,
    )
}

/// `apivalidation.ValidateQualifiedName`: the qualified-name rule, reported as
/// `Invalid` at the given path.
fn validate_qualified_name(value: &str, fld_path: &Path) -> ErrorList {
    is_qualified_name(value)
        .into_iter()
        .map(|msg| Error::invalid(fld_path, value.to_string(), msg))
        .collect()
}

/// `validateNamedRuleWithOperations` (`validation.go:947-961`).
fn validate_named_rule_with_operations(
    rule: &NamedRuleWithOperations,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, name) in rule
        .resource_names
        .as_deref()
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        let path = fld_path.child("resourceNames").index(i);
        for msg in validate_path_segment_name(name) {
            errs.push(Error::invalid(&path, name.clone(), msg));
        }
        if !seen.insert(name.as_str()) {
            errs.push(Error::duplicate(&path, name.clone()));
        }
    }

    let operations: Vec<String> = rule
        .rule
        .operations
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(operation_str)
        .collect();
    errs.extend(validate_rule_parts(
        &operations,
        rule.rule.api_groups.as_deref().unwrap_or_default(),
        rule.rule.api_versions.as_deref().unwrap_or_default(),
        rule.rule.resources.as_deref().unwrap_or_default(),
        rule.rule.scope.as_deref(),
        fld_path,
    ));
    errs
}

fn operation_str(op: &crate::resources::validating_admission_policy::OperationType) -> String {
    use crate::resources::validating_admission_policy::OperationType as Op;
    match op {
        Op::Create => "CREATE",
        Op::Update => "UPDATE",
        Op::Delete => "DELETE",
        Op::Connect => "CONNECT",
        Op::All => "*",
    }
    .to_string()
}

/// `path.ValidatePathSegmentName(name, false)`
/// (`apimachinery/pkg/api/validation/path/name.go:29-48`): a name may not be
/// `.`, `..`, or contain `/` or `%`.
fn validate_path_segment_name(name: &str) -> Vec<String> {
    let mut msgs = Vec::new();
    if name == "." {
        msgs.push("may not be '.'".to_string());
    } else if name == ".." {
        msgs.push("may not be '..'".to_string());
    } else {
        if name.contains('/') {
            msgs.push("may not contain '/'".to_string());
        }
        if name.contains('%') {
            msgs.push("may not contain '%'".to_string());
        }
    }
    msgs
}

/// `validateMatchConditions` + `validateMatchCondition`
/// (`validation.go:963-997`), minus the CEL compile. Shared with the webhook
/// configurations, which upstream validates with this same pair.
pub fn validate_match_conditions(conditions: &[MatchCondition], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if conditions.len() > MAX_MATCH_CONDITIONS {
        errs.push(Error::too_many(fld_path, MAX_MATCH_CONDITIONS));
    }
    let mut names: HashSet<&str> = HashSet::new();
    for (i, condition) in conditions.iter().enumerate() {
        let path = fld_path.index(i);
        if condition.expression.trim().is_empty() {
            errs.push(Error::required(&path.child("expression"), ""));
        }
        if condition.name.is_empty() {
            errs.push(Error::required(&path.child("name"), ""));
        } else {
            errs.extend(validate_qualified_name(
                &condition.name,
                &path.child("name"),
            ));
            if !names.insert(condition.name.as_str()) {
                errs.push(Error::duplicate(
                    &path.child("name"),
                    condition.name.clone(),
                ));
            }
        }
    }
    errs
}

/// `validateVariable` (`validation.go:999-1032`), minus the CEL compile.
fn validate_variable(variable: &Variable, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if variable.name.trim().is_empty() {
        errs.push(Error::required(
            &fld_path.child("name"),
            "name is not specified",
        ));
    } else if !is_cel_identifier(&variable.name) {
        errs.push(Error::invalid(
            &fld_path.child("name"),
            variable.name.clone(),
            "must be a valid CEL identifier",
        ));
    }
    if variable.expression.trim().is_empty() {
        errs.push(Error::required(
            &fld_path.child("expression"),
            "expression is not specified",
        ));
    }
    errs
}

/// `isCELIdentifier` (`validation.go:1253-1258`): `[_a-zA-Z][_a-zA-Z0-9]*`.
fn is_cel_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

/// `validateValidation` (`validation.go:1034-1061`), minus the CEL compile.
fn validate_validation(validation: &Validation, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if validation.expression.trim().is_empty() {
        errs.push(Error::required(
            &fld_path.child("expression"),
            "expression is not specified",
        ));
    }

    if let Some(message_expression) = &validation.message_expression {
        if !message_expression.is_empty() && message_expression.trim().is_empty() {
            errs.push(Error::invalid(
                &fld_path.child("messageExpression"),
                message_expression.clone(),
                "must be non-empty if specified",
            ));
        }
    }

    if let Some(message) = &validation.message {
        let trimmed = message.trim();
        if !message.is_empty() && trimmed.is_empty() {
            errs.push(Error::invalid(
                &fld_path.child("message"),
                message.clone(),
                "must be non-empty if specified",
            ));
        } else if trimmed.contains('\n') || trimmed.contains('\r') {
            errs.push(Error::invalid(
                &fld_path.child("message"),
                message.clone(),
                "must not contain line breaks",
            ));
        }
    }

    // `reason` is a closed enum here, so an unsupported value is rejected by
    // the decoder where upstream answers `NotSupported` (`:1058`).
    errs
}

/// `validateAuditAnnotation` (`validation.go:1128-1162`), minus the CEL
/// compile. The key is validated as `<policy name>/<key>`, so a policy with no
/// name cannot have one at all.
fn validate_audit_annotation(
    annotation: &AuditAnnotation,
    policy_name: &str,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let key_path = fld_path.child("key");
    if policy_name.is_empty() {
        errs.push(Error::invalid(
            &key_path,
            annotation.key.clone(),
            "requires metadata.name be non-empty",
        ));
    } else {
        errs.extend(validate_qualified_name(
            &format!("{policy_name}/{}", annotation.key),
            &key_path,
        ));
    }

    let value_path = fld_path.child("valueExpression");
    let trimmed = annotation.value_expression.trim();
    if trimmed.is_empty() {
        errs.push(Error::required(
            &value_path,
            "valueExpression is not specified",
        ));
    } else if trimmed.len() > MAX_AUDIT_ANNOTATION_VALUE_EXPRESSION_LENGTH {
        // Upstream reports this as `Required` with a length detail, not as
        // `TooLong` (`validation.go:1142-1143`).
        errs.push(Error::required(
            &value_path,
            format!(
                "must not exceed {MAX_AUDIT_ANNOTATION_VALUE_EXPRESSION_LENGTH} bytes in length"
            ),
        ));
    }
    errs
}

/// `validateValidatingAdmissionPolicyBindingSpec` (`validation.go:1181-1196`).
fn validate_binding_spec(
    spec: &ValidatingAdmissionPolicyBindingSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let policy_name = spec.policy_name.clone().unwrap_or_default();
    let policy_path = fld_path.child("policyName");
    if policy_name.is_empty() {
        errs.push(Error::required(&policy_path, ""));
    } else {
        for msg in is_dns1123_subdomain(&policy_name) {
            errs.push(Error::invalid(&policy_path, policy_name.clone(), msg));
        }
    }

    if let Some(param_ref) = &spec.param_ref {
        errs.extend(validate_param_ref(param_ref, &fld_path.child("paramRef")));
    }
    if let Some(match_resources) = &spec.match_resources {
        errs.extend(validate_match_resources(
            match_resources,
            &fld_path.child("matchResources"),
        ));
    }
    errs.extend(validate_validation_actions(
        spec.validation_actions.as_deref().unwrap_or_default(),
        &fld_path.child("validationActions"),
    ));
    errs
}

/// `validateParamRef` (`validation.go:1198-1232`).
fn validate_param_ref(param_ref: &ParamRef, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let name = param_ref.name.clone().unwrap_or_default();

    if !name.is_empty() {
        for msg in validate_path_segment_name(&name) {
            errs.push(Error::invalid(&fld_path.child("name"), name.clone(), msg));
        }
        if param_ref.selector.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("name"),
                "name and selector are mutually exclusive",
            ));
        }
    }

    if let Some(selector) = &param_ref.selector {
        errs.extend(validate_selector(selector, &fld_path.child("selector")));
        if !name.is_empty() {
            errs.push(Error::forbidden(
                &fld_path.child("selector"),
                "name and selector are mutually exclusive",
            ));
        }
    }

    if name.is_empty() && param_ref.selector.is_none() {
        errs.push(Error::required(
            fld_path,
            "one of name or selector must be specified",
        ));
    }

    // Upstream has no defaulting for this field, so it really is required
    // whenever a paramRef is present (`validation.go:1227-1231`).
    if param_ref.parameter_not_found_action.is_none() {
        errs.push(Error::required(
            &fld_path.child("parameterNotFoundAction"),
            "",
        ));
    }
    errs
}

/// `validateValidationActions` (`validation.go:926-945`). An unsupported action
/// is rejected by the decoder here, where upstream answers `NotSupported`.
fn validate_validation_actions(actions: &[ValidationAction], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut seen: HashSet<&'static str> = HashSet::new();
    for (i, action) in actions.iter().enumerate() {
        let name = match action {
            ValidationAction::Deny => "Deny",
            ValidationAction::Warn => "Warn",
            ValidationAction::Audit => "Audit",
        };
        if !seen.insert(name) {
            errs.push(Error::duplicate(&fld_path.index(i), name.to_string()));
        }
    }
    if seen.contains("Deny") && seen.contains("Warn") {
        errs.push(Error::invalid(
            fld_path,
            String::new(),
            "must not contain both Deny and Warn (repeating the same validation failure \
             information in the API response and headers serves no purpose)",
        ));
    }
    if seen.is_empty() {
        errs.push(Error::required(
            fld_path,
            "at least one validation action is required",
        ));
    }
    errs
}

/// Upstream `ValidateValidatingAdmissionPolicyStatusUpdate` (`validation.go:1247`),
/// which is just `validateValidatingAdmissionPolicyStatus` on the new object —
/// the old one is unused.
pub fn validate_validating_admission_policy_status_update(
    policy: &ValidatingAdmissionPolicy,
) -> ErrorList {
    validate_validating_admission_policy_status(policy, &Path::new("status"))
}

/// Upstream `validateValidatingAdmissionPolicyStatus` (`validation.go:1256`).
fn validate_validating_admission_policy_status(
    policy: &ValidatingAdmissionPolicy,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(status) = &policy.status else {
        return errs;
    };

    // `validateTypeChecking` returns nil for a nil TypeChecking (`:1263-1266`).
    if let Some(type_checking) = &status.type_checking {
        errs.extend(validate_expression_warnings(
            type_checking.expression_warnings.as_deref().unwrap_or(&[]),
            &fld_path.child("typeChecking").child("expressionWarnings"),
        ));
    }

    if let Some(conditions) = &status.conditions {
        // `metav1validation.ValidateConditions` (`:1259`). Rusternetes models a
        // policy condition with its own struct rather than reusing
        // `metav1.Condition`, so map it across for the one shared predicate
        // instead of keeping a second copy of the condition rules.
        let mapped: Vec<crate::types::Condition> = conditions
            .iter()
            .map(|c| crate::types::Condition {
                condition_type: c.condition_type.clone(),
                status: c.status.clone(),
                observed_generation: None,
                last_transition_time: c
                    .last_transition_time
                    .as_deref()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                    .map(|t| t.with_timezone(&chrono::Utc)),
                reason: c.reason.clone(),
                message: c.message.clone(),
            })
            .collect();
        errs.extend(crate::validation::metav1::validate_conditions(
            &mapped,
            &fld_path.child("conditions"),
        ));
    }

    errs
}

/// Upstream `validateExpressionWarnings` + `validateExpressionWarning`
/// (`validation.go:1270-1285`).
fn validate_expression_warnings(warnings: &[ExpressionWarning], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (i, warning) in warnings.iter().enumerate() {
        let entry = fld_path.index(i);
        if warning.warning.is_empty() {
            errs.push(Error::required(&entry.child("warning"), ""));
        }
        errs.extend(validate_field_ref(
            &warning.field_ref,
            &entry.child("fieldRef"),
        ));
    }
    errs
}

/// Upstream `validateFieldRef` (`validation.go:1287-1297`).
///
/// Deviation, stated deliberately: upstream also runs the reference through
/// `jsonpath.New("spec").Parse("{" + fieldRef + "}")` and answers a parse
/// failure with `invalid JSONPath: <err>`. Rusternetes has no JSONPath parser
/// (`validation/crd.rs::validate_simple_json_path` implements upstream's
/// *simple* dotted-path rule, which is a different and stricter grammar), so
/// only the `Required` half is ported. Upstream's own comment on the rest —
/// "no further checks, for an easier upgrade/rollback" — is the reason this is
/// the safe half to keep: the parse is the lenient part.
fn validate_field_ref(field_ref: &str, fld_path: &Path) -> ErrorList {
    if field_ref.trim().is_empty() {
        return vec![Error::required(fld_path, "")];
    }
    Vec::new()
}
