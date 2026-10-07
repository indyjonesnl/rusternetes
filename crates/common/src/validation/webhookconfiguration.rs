//! Validation for admissionregistration.k8s.io webhook configurations,
//! ported from upstream `pkg/apis/admissionregistration/validation/validation.go`
//! (`ValidateValidatingWebhookConfiguration` / `ValidateMutatingWebhookConfiguration`).
//!
//! Scope: the behaviour-significant create-time checks — fully-qualified +
//! unique webhook names, `admissionReviewVersions` (required + recognized),
//! `sideEffects` (v1 requires None/NoneOnDryRun), `timeoutSeconds` range,
//! `clientConfig` url-XOR-service, and `rules` (operations/apiGroups/
//! apiVersions/resources required + wildcard exclusivity + scope enum).
//!
//! `failurePolicy`/`matchPolicy`/`sideEffects`/operations are typed enums in
//! the resource model, so an out-of-range *string* is already rejected at
//! decode time; here we enforce the value constraints decode can't (e.g. v1's
//! no-side-effects rule). Deep `clientConfig.url`/`service` validation
//! (`ValidateWebhookURL`/`ValidateWebhookService`) and
//! `namespaceSelector`/`objectSelector` label-selector validation are included.

use crate::resources::admission_webhook::{
    FailurePolicy, LabelSelector as WebhookLabelSelector, MatchCondition, MatchPolicy,
    MutatingWebhook, MutatingWebhookConfiguration, ReinvocationPolicy, RuleWithOperations,
    SideEffectClass, ValidatingWebhook, ValidatingWebhookConfiguration,
};
use crate::resources::WebhookClientConfig;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1035_label, is_dns1123_subdomain, validate_label_selector, LabelSelectorValidationOptions,
};
use crate::validation::validating_admission_policy::validate_match_conditions;
use std::collections::HashSet;

/// Versions of the AdmissionReview object this apiserver accepts. Mirrors
/// upstream `AcceptedAdmissionReviewVersions`.
const ACCEPTED_ADMISSION_REVIEW_VERSIONS: &[&str] = &["v1", "v1beta1"];

/// `supportedOperations` (validation.go:496-502), as `sets.String.List()` sorts.
const SUPPORTED_OPERATIONS: &[&str] = &["*", "CONNECT", "CREATE", "DELETE", "UPDATE"];

const VALID_SCOPES: &[&str] = &["Cluster", "Namespaced", "*"];

/// Upstream `IsFullyQualifiedName`: required, a valid DNS1123 subdomain, with at
/// least three dot-separated segments.
fn validate_fully_qualified_name(path: &Path, name: &str) -> ErrorList {
    let mut errs = ErrorList::new();
    if name.is_empty() {
        errs.push(Error::required(path, ""));
        return errs;
    }
    let sub_errs = is_dns1123_subdomain(name);
    if !sub_errs.is_empty() {
        errs.push(Error::invalid(path, name.to_string(), sub_errs.join(",")));
        return errs;
    }
    if name.split('.').count() < 3 {
        errs.push(Error::invalid(
            path,
            name.to_string(),
            "should be a domain with at least three segments separated by dots",
        ));
    }
    errs
}

/// Upstream `validateAdmissionReviewVersions` with
/// `requireRecognizedAdmissionReviewVersion = true` (the create path).
fn validate_admission_review_versions(versions: &[String], path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    if versions.is_empty() {
        errs.push(Error::required(
            path,
            format!(
                "must specify one of {}",
                ACCEPTED_ADMISSION_REVIEW_VERSIONS.join(", ")
            ),
        ));
        return errs;
    }
    let mut seen: HashSet<&str> = HashSet::new();
    let mut has_accepted = false;
    for (i, v) in versions.iter().enumerate() {
        if !seen.insert(v.as_str()) {
            errs.push(Error::invalid(
                &path.index(i),
                v.clone(),
                "duplicate version",
            ));
            continue;
        }
        // Upstream runs IsDNS1035Label on each version.
        for msg in is_dns1035_label(v) {
            errs.push(Error::invalid(&path.index(i), v.clone(), msg));
        }
        if ACCEPTED_ADMISSION_REVIEW_VERSIONS.contains(&v.as_str()) {
            has_accepted = true;
        }
    }
    if !has_accepted {
        errs.push(Error::invalid(
            path,
            versions.join(","),
            format!(
                "must include at least one of {}",
                ACCEPTED_ADMISSION_REVIEW_VERSIONS.join(", ")
            ),
        ));
    }
    errs
}

fn has_wildcard(values: &[String]) -> bool {
    values.iter().any(|v| v == "*")
}

/// Upstream `validateRuleWithOperations` + `validateRule` (allowSubResource),
/// expressed over the parts of a rule so the one port serves both shapes that
/// carry them: a webhook's `RuleWithOperations` and a policy's
/// `NamedRuleWithOperations` (upstream has a single type and a single
/// validator — `pkg/apis/admissionregistration/validation/validation.go:109`).
pub(crate) fn validate_rule_parts(
    operations: &[String],
    api_groups: &[String],
    api_versions: &[String],
    resources: &[String],
    scope: Option<&str>,
    path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();

    if operations.is_empty() {
        errs.push(Error::required(&path.child("operations"), ""));
    }
    let has_all = operations.iter().any(|o| o == "*");
    if operations.len() > 1 && has_all {
        errs.push(Error::invalid(
            &path.child("operations"),
            "*".to_string(),
            "if '*' is present, must not specify other operations",
        ));
    }
    // validation.go:546-548 (`supportedOperations`, sorted by `List()`).
    for (i, op) in operations.iter().enumerate() {
        if !SUPPORTED_OPERATIONS.contains(&op.as_str()) {
            errs.push(Error::not_supported(
                &path.child("operations").index(i),
                op.clone(),
                SUPPORTED_OPERATIONS,
            ));
        }
    }

    if api_groups.is_empty() {
        errs.push(Error::required(&path.child("apiGroups"), ""));
    }
    if api_groups.len() > 1 && has_wildcard(api_groups) {
        errs.push(Error::invalid(
            &path.child("apiGroups"),
            "*".to_string(),
            "if '*' is present, must not specify other API groups",
        ));
    }
    if api_versions.is_empty() {
        errs.push(Error::required(&path.child("apiVersions"), ""));
    }
    if api_versions.len() > 1 && has_wildcard(api_versions) {
        errs.push(Error::invalid(
            &path.child("apiVersions"),
            "*".to_string(),
            "if '*' is present, must not specify other API versions",
        ));
    }
    for (i, v) in api_versions.iter().enumerate() {
        if v.is_empty() {
            errs.push(Error::required(&path.child("apiVersions").index(i), ""));
        }
    }
    if resources.is_empty() {
        errs.push(Error::required(&path.child("resources"), ""));
    }
    if resources.len() > 1 && has_wildcard(resources) {
        errs.push(Error::invalid(
            &path.child("resources"),
            "*".to_string(),
            "if '*' is present, must not specify other resources",
        ));
    }
    if let Some(scope) = scope {
        if !VALID_SCOPES.contains(&scope) {
            errs.push(Error::not_supported(
                &path.child("scope"),
                scope.to_string(),
                VALID_SCOPES,
            ));
        }
    }
    errs
}

/// The webhook shape of the same rule.
fn validate_rule_with_operations(rule: &RuleWithOperations, path: &Path) -> ErrorList {
    let operations: Vec<String> = rule
        .operations
        .iter()
        .map(|o| match o {
            crate::resources::admission_webhook::OperationType::All => "*".to_string(),
            crate::resources::admission_webhook::OperationType::Unknown(v) => v.clone(),
            other => serde_json::to_value(other)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default(),
        })
        .collect();
    let r = &rule.rule;
    validate_rule_parts(
        &operations,
        &r.api_groups,
        &r.api_versions,
        &r.resources,
        r.scope.as_deref(),
        path,
    )
}

fn side_effect_str(s: &SideEffectClass) -> &str {
    match s {
        // Go's zero value for the field, i.e. an absent `sideEffects`; upstream
        // renders it in the error as the empty string it is.
        SideEffectClass::Unspecified => "",
        SideEffectClass::Unknown => "Unknown",
        SideEffectClass::None => "None",
        SideEffectClass::Some => "Some",
        SideEffectClass::NoneOnDryRun => "NoneOnDryRun",
        SideEffectClass::Unrecognized(v) => v,
    }
}

/// v1 webhooks require `sideEffects` to be `None` or `NoneOnDryRun`
/// (`requireNoSideEffects`).
fn validate_no_side_effects(side_effects: &SideEffectClass, path: &Path) -> Option<Error> {
    match side_effects {
        SideEffectClass::None | SideEffectClass::NoneOnDryRun => None,
        other => Some(Error::not_supported(
            path,
            side_effect_str(other).to_string(),
            &["None", "NoneOnDryRun"],
        )),
    }
}

/// Port of upstream `webhook.ValidateWebhookURL` (forceHttps=true): scheme must
/// be `https`, host present, no user-info / fragment / query.
pub(crate) fn validate_webhook_url(url: &str, path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    const FORM: &str = "; desired format: https://host[/path]";
    let parsed = match ::url::Url::parse(url) {
        Ok(u) => u,
        Err(e) => {
            errs.push(Error::required(
                path,
                format!("url must be a valid URL: {e}{FORM}"),
            ));
            return errs;
        }
    };
    if parsed.scheme() != "https" {
        errs.push(Error::invalid(
            path,
            parsed.scheme().to_string(),
            format!("'https' is the only allowed URL scheme{FORM}"),
        ));
    }
    if parsed.host_str().unwrap_or("").is_empty() {
        errs.push(Error::invalid(
            path,
            String::new(),
            format!("host must be specified{FORM}"),
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        errs.push(Error::invalid(
            path,
            parsed.username().to_string(),
            "user information is not permitted in the URL",
        ));
    }
    if parsed.fragment().is_some() {
        errs.push(Error::invalid(
            path,
            parsed.fragment().unwrap_or("").to_string(),
            "fragments are not permitted in the URL",
        ));
    }
    if parsed.query().is_some() {
        errs.push(Error::invalid(
            path,
            parsed.query().unwrap_or("").to_string(),
            "query parameters are not permitted in the URL",
        ));
    }
    errs
}

/// Port of upstream `webhook.ValidateWebhookService`: name/namespace required,
/// port in 1..=65535, and the path (when set) a valid `/`-rooted URL path whose
/// non-empty segments are DNS1123 subdomains.
pub(crate) fn validate_webhook_service(
    name: &str,
    namespace: &str,
    svc_path: Option<&str>,
    port: Option<i32>,
    path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    if name.is_empty() {
        errs.push(Error::required(&path.child("name"), ""));
    }
    if namespace.is_empty() {
        errs.push(Error::required(&path.child("namespace"), ""));
    }
    // Port defaults to 443 when unset (SetDefaults), which is valid; only check
    // an explicitly-set value.
    if let Some(p) = port {
        if !(1..=65535).contains(&p) {
            errs.push(Error::invalid(
                &path.child("port"),
                p,
                "port is not valid: must be between 1 and 65535, inclusive",
            ));
        }
    }
    let Some(url_path) = svc_path else {
        return errs;
    };
    if url_path == "/" || url_path.is_empty() {
        return errs;
    }
    if url_path == "//" {
        errs.push(Error::invalid(
            &path.child("path"),
            url_path.to_string(),
            "segment[0] may not be empty",
        ));
        return errs;
    }
    if !url_path.starts_with('/') {
        errs.push(Error::invalid(
            &path.child("path"),
            url_path.to_string(),
            "must start with a '/'",
        ));
    }
    let mut to_check = &url_path[1..];
    if let Some(stripped) = to_check.strip_suffix('/') {
        to_check = stripped;
    }
    for (i, step) in to_check.split('/').enumerate() {
        if step.is_empty() {
            errs.push(Error::invalid(
                &path.child("path"),
                url_path.to_string(),
                format!("segment[{i}] may not be empty"),
            ));
            continue;
        }
        for failure in is_dns1123_subdomain(step) {
            errs.push(Error::invalid(
                &path.child("path"),
                url_path.to_string(),
                format!("segment[{i}]: {failure}"),
            ));
        }
    }
    errs
}

fn validate_client_config(cc: &WebhookClientConfig, path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    // exactly one of url or service
    if cc.url.is_none() == cc.service.is_none() {
        errs.push(Error::required(
            &path.child("url"),
            "exactly one of url or service is required",
        ));
    }
    if let Some(url) = &cc.url {
        errs.extend(validate_webhook_url(url, &path.child("url")));
    }
    if let Some(svc) = &cc.service {
        errs.extend(validate_webhook_service(
            &svc.name,
            &svc.namespace,
            svc.path.as_deref(),
            svc.port,
            &path.child("service"),
        ));
    }
    errs
}

/// Convert the webhook resource's `LabelSelector` to the shared
/// `types::LabelSelector` so the common `validate_label_selector` can run.
pub(crate) fn to_metav1_selector(s: &WebhookLabelSelector) -> crate::types::LabelSelector {
    use crate::resources::admission_webhook::LabelSelectorOperator as Op;
    crate::types::LabelSelector {
        match_labels: s.match_labels.clone(),
        match_expressions: s.match_expressions.as_ref().map(|reqs| {
            reqs.iter()
                .map(|r| crate::types::LabelSelectorRequirement {
                    key: r.key.clone(),
                    operator: match r.operator {
                        // An absent `operator`; the shared validator rejects it
                        // as upstream does, rather than this mapping guessing.
                        Op::Unspecified => "",
                        Op::In => "In",
                        Op::NotIn => "NotIn",
                        Op::Exists => "Exists",
                        Op::DoesNotExist => "DoesNotExist",
                    }
                    .to_string(),
                    values: r.values.clone(),
                })
                .collect()
        }),
    }
}

/// Validate `namespaceSelector` / `objectSelector` via the shared
/// `ValidateLabelSelector` (upstream runs it on both when present).
fn validate_webhook_selectors(
    namespace_selector: Option<&WebhookLabelSelector>,
    object_selector: Option<&WebhookLabelSelector>,
    path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let opts = LabelSelectorValidationOptions::default();
    if let Some(ns) = namespace_selector {
        errs.extend(validate_label_selector(
            &to_metav1_selector(ns),
            opts,
            &path.child("namespaceSelector"),
        ));
    }
    if let Some(os) = object_selector {
        errs.extend(validate_label_selector(
            &to_metav1_selector(os),
            opts,
            &path.child("objectSelector"),
        ));
    }
    errs
}

fn validate_timeout_seconds(timeout: Option<i32>, path: &Path) -> Option<Error> {
    match timeout {
        Some(t) if !(1..=30).contains(&t) => Some(Error::invalid(
            path,
            t,
            "the timeout value must be between 1 and 30 seconds",
        )),
        _ => None,
    }
}

/// Upstream `ignoreValidatingWebhookMatchConditions` (`validation.go:605-616`)
/// and `ignoreMutatingWebhookMatchConditions` (`:592-603`) have identical
/// bodies: an update may keep a `matchConditions` list that no longer validates,
/// as long as it is *unchanged*. A list that differs in length or in any entry
/// is validated again.
fn match_conditions_unchanged(
    new_c: &[Option<&Vec<MatchCondition>>],
    old_c: &[Option<&Vec<MatchCondition>>],
) -> bool {
    new_c.len() == old_c.len() && new_c.iter().zip(old_c).all(|(n, o)| n == o)
}

pub fn ignore_validating_webhook_match_conditions(
    new_c: &ValidatingWebhookConfiguration,
    old_c: &ValidatingWebhookConfiguration,
) -> bool {
    let new_conditions: Vec<_> = new_c
        .webhooks
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|h| h.match_conditions.as_ref())
        .collect();
    let old_conditions: Vec<_> = old_c
        .webhooks
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|h| h.match_conditions.as_ref())
        .collect();
    match_conditions_unchanged(&new_conditions, &old_conditions)
}

pub fn ignore_mutating_webhook_match_conditions(
    new_c: &MutatingWebhookConfiguration,
    old_c: &MutatingWebhookConfiguration,
) -> bool {
    let new_conditions: Vec<_> = new_c
        .webhooks
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|h| h.match_conditions.as_ref())
        .collect();
    let old_conditions: Vec<_> = old_c
        .webhooks
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|h| h.match_conditions.as_ref())
        .collect();
    match_conditions_unchanged(&new_conditions, &old_conditions)
}

/// `SetDefaults_Rule` (`pkg/apis/admissionregistration/v1/defaults.go:77-82`).
fn set_defaults_rules(rules: &mut [RuleWithOperations]) {
    for rule in rules {
        rule.rule.scope.get_or_insert_with(|| "*".to_string());
    }
}

/// `SetDefaults_ServiceReference` (`defaults.go:85-89`).
fn set_defaults_client_config(client_config: &mut WebhookClientConfig) {
    if let Some(service) = client_config.service.as_mut() {
        service.port.get_or_insert(443);
    }
}

/// The defaults `SetDefaults_ValidatingWebhook` and `SetDefaults_MutatingWebhook`
/// share (`defaults.go:32-45`, `:50-66`).
#[allow(clippy::too_many_arguments)]
fn set_defaults_webhook_common(
    failure_policy: &mut Option<FailurePolicy>,
    match_policy: &mut Option<MatchPolicy>,
    namespace_selector: &mut Option<WebhookLabelSelector>,
    object_selector: &mut Option<WebhookLabelSelector>,
    timeout_seconds: &mut Option<i32>,
    rules: &mut [RuleWithOperations],
    client_config: &mut WebhookClientConfig,
) {
    failure_policy.get_or_insert(FailurePolicy::Fail);
    match_policy.get_or_insert(MatchPolicy::Equivalent);
    namespace_selector.get_or_insert_with(WebhookLabelSelector::default);
    object_selector.get_or_insert_with(WebhookLabelSelector::default);
    timeout_seconds.get_or_insert(10);
    set_defaults_rules(rules);
    set_defaults_client_config(client_config);
}

/// `SetObjectDefaults_ValidatingWebhookConfiguration`
/// (`pkg/apis/admissionregistration/v1/zz_generated.defaults.go`):
/// `SetDefaults_ValidatingWebhook`, then `SetDefaults_ServiceReference` and
/// `SetDefaults_Rule` over each webhook.
pub fn set_defaults_validating_webhook_configuration(cfg: &mut ValidatingWebhookConfiguration) {
    for hook in cfg.webhooks.iter_mut().flatten() {
        set_defaults_webhook_common(
            &mut hook.failure_policy,
            &mut hook.match_policy,
            &mut hook.namespace_selector,
            &mut hook.object_selector,
            &mut hook.timeout_seconds,
            &mut hook.rules,
            &mut hook.client_config,
        );
    }
}

/// `SetObjectDefaults_MutatingWebhookConfiguration`: as the validating one,
/// plus `reinvocationPolicy` (`SetDefaults_MutatingWebhook`, `defaults.go:64`).
pub fn set_defaults_mutating_webhook_configuration(cfg: &mut MutatingWebhookConfiguration) {
    for hook in cfg.webhooks.iter_mut().flatten() {
        set_defaults_webhook_common(
            &mut hook.failure_policy,
            &mut hook.match_policy,
            &mut hook.namespace_selector,
            &mut hook.object_selector,
            &mut hook.timeout_seconds,
            &mut hook.rules,
            &mut hook.client_config,
        );
        hook.reinvocation_policy
            .get_or_insert(ReinvocationPolicy::Never);
    }
}

/// Validate a `ValidatingWebhookConfiguration` (create path) — upstream
/// `ValidateValidatingWebhookConfiguration` (`validation.go:706`), which passes
/// `ignoreMatchConditions: false`.
pub fn validate_validating_webhook_configuration(
    cfg: &ValidatingWebhookConfiguration,
) -> ErrorList {
    validate_validating_webhook_configuration_opts(cfg, false)
}

/// Validate a `ValidatingWebhookConfiguration` on update — upstream
/// `ValidateValidatingWebhookConfigurationUpdate` (`validation.go:729-740`),
/// which re-runs the create validator on the *new* object with
/// `ignoreMatchConditions` set from the old one.
pub fn validate_validating_webhook_configuration_update(
    new_c: &ValidatingWebhookConfiguration,
    old_c: &ValidatingWebhookConfiguration,
) -> ErrorList {
    validate_validating_webhook_configuration_opts(
        new_c,
        ignore_validating_webhook_match_conditions(new_c, old_c),
    )
}

fn validate_validating_webhook_configuration_opts(
    cfg: &ValidatingWebhookConfiguration,
    ignore_match_conditions: bool,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let mut names: HashSet<String> = HashSet::new();
    if let Some(webhooks) = &cfg.webhooks {
        for (i, hook) in webhooks.iter().enumerate() {
            let path = Path::new("webhooks").index(i);
            errs.extend(validate_validating_webhook(
                hook,
                ignore_match_conditions,
                &path,
            ));
            errs.extend(validate_admission_review_versions(
                &hook.admission_review_versions,
                &path.child("admissionReviewVersions"),
            ));
            if !hook.name.is_empty() && !names.insert(hook.name.clone()) {
                errs.push(Error::duplicate(&path.child("name"), hook.name.clone()));
            }
        }
    }
    errs
}

/// Validate a `MutatingWebhookConfiguration` (create path) — upstream
/// `ValidateMutatingWebhookConfiguration` (`validation.go:711`), which passes
/// `ignoreMatchConditions: false`.
pub fn validate_mutating_webhook_configuration(cfg: &MutatingWebhookConfiguration) -> ErrorList {
    validate_mutating_webhook_configuration_opts(cfg, false)
}

/// Validate a `MutatingWebhookConfiguration` on update — upstream
/// `ValidateMutatingWebhookConfigurationUpdate` (`validation.go:742-753`).
pub fn validate_mutating_webhook_configuration_update(
    new_c: &MutatingWebhookConfiguration,
    old_c: &MutatingWebhookConfiguration,
) -> ErrorList {
    validate_mutating_webhook_configuration_opts(
        new_c,
        ignore_mutating_webhook_match_conditions(new_c, old_c),
    )
}

fn validate_mutating_webhook_configuration_opts(
    cfg: &MutatingWebhookConfiguration,
    ignore_match_conditions: bool,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let mut names: HashSet<String> = HashSet::new();
    if let Some(webhooks) = &cfg.webhooks {
        for (i, hook) in webhooks.iter().enumerate() {
            let path = Path::new("webhooks").index(i);
            errs.extend(validate_mutating_webhook(
                hook,
                ignore_match_conditions,
                &path,
            ));
            errs.extend(validate_admission_review_versions(
                &hook.admission_review_versions,
                &path.child("admissionReviewVersions"),
            ));
            if !hook.name.is_empty() && !names.insert(hook.name.clone()) {
                errs.push(Error::duplicate(&path.child("name"), hook.name.clone()));
            }
        }
    }
    errs
}

fn validate_validating_webhook(
    hook: &ValidatingWebhook,
    ignore_match_conditions: bool,
    path: &Path,
) -> ErrorList {
    let mut errs = validate_fully_qualified_name(&path.child("name"), &hook.name);
    for (i, rule) in hook.rules.iter().enumerate() {
        errs.extend(validate_rule_with_operations(
            rule,
            &path.child("rules").index(i),
        ));
    }
    // validation.go:371-376 / :427-432.
    if let Some(FailurePolicy::Unknown(v)) = &hook.failure_policy {
        errs.push(Error::not_supported(
            &path.child("failurePolicy"),
            v.clone(),
            &["Fail", "Ignore"],
        ));
    }
    if let Some(MatchPolicy::Unknown(v)) = &hook.match_policy {
        errs.push(Error::not_supported(
            &path.child("matchPolicy"),
            v.clone(),
            &["Equivalent", "Exact"],
        ));
    }
    if let Some(e) = validate_no_side_effects(&hook.side_effects, &path.child("sideEffects")) {
        errs.push(e);
    }
    if let Some(e) = validate_timeout_seconds(hook.timeout_seconds, &path.child("timeoutSeconds")) {
        errs.push(e);
    }
    errs.extend(validate_client_config(
        &hook.client_config,
        &path.child("clientConfig"),
    ));
    errs.extend(validate_webhook_selectors(
        hook.namespace_selector.as_ref(),
        hook.object_selector.as_ref(),
        path,
    ));
    // Upstream calls the *same* `validateMatchConditions` the policy validator
    // calls (`validation.go:409` / `:467` / `:800`), so this is that one
    // function rather than a webhook-local copy.
    if !ignore_match_conditions {
        errs.extend(validate_match_conditions(
            hook.match_conditions.as_deref().unwrap_or_default(),
            &path.child("matchConditions"),
        ));
    }
    errs
}

fn validate_mutating_webhook(
    hook: &MutatingWebhook,
    ignore_match_conditions: bool,
    path: &Path,
) -> ErrorList {
    let mut errs = validate_fully_qualified_name(&path.child("name"), &hook.name);
    for (i, rule) in hook.rules.iter().enumerate() {
        errs.extend(validate_rule_with_operations(
            rule,
            &path.child("rules").index(i),
        ));
    }
    // validation.go:371-376 / :427-432.
    if let Some(FailurePolicy::Unknown(v)) = &hook.failure_policy {
        errs.push(Error::not_supported(
            &path.child("failurePolicy"),
            v.clone(),
            &["Fail", "Ignore"],
        ));
    }
    if let Some(MatchPolicy::Unknown(v)) = &hook.match_policy {
        errs.push(Error::not_supported(
            &path.child("matchPolicy"),
            v.clone(),
            &["Equivalent", "Exact"],
        ));
    }
    if let Some(e) = validate_no_side_effects(&hook.side_effects, &path.child("sideEffects")) {
        errs.push(e);
    }
    if let Some(e) = validate_timeout_seconds(hook.timeout_seconds, &path.child("timeoutSeconds")) {
        errs.push(e);
    }
    errs.extend(validate_client_config(
        &hook.client_config,
        &path.child("clientConfig"),
    ));
    errs.extend(validate_webhook_selectors(
        hook.namespace_selector.as_ref(),
        hook.object_selector.as_ref(),
        path,
    ));
    // Upstream calls the *same* `validateMatchConditions` the policy validator
    // calls (`validation.go:409` / `:467` / `:800`), so this is that one
    // function rather than a webhook-local copy.
    if !ignore_match_conditions {
        errs.extend(validate_match_conditions(
            hook.match_conditions.as_deref().unwrap_or_default(),
            &path.child("matchConditions"),
        ));
    }
    // validation.go:453-455.
    if let Some(ReinvocationPolicy::Unknown(v)) = &hook.reinvocation_policy {
        errs.push(Error::not_supported(
            &path.child("reinvocationPolicy"),
            v.clone(),
            &["IfNeeded", "Never"],
        ));
    }
    errs
}

#[cfg(test)]
mod deep_validation_tests {
    use super::*;

    fn p() -> Path {
        Path::new("clientConfig")
    }

    #[test]
    fn valid_https_url_passes() {
        assert!(validate_webhook_url("https://example.com/hook", &p().child("url")).is_empty());
    }

    #[test]
    fn http_scheme_rejected() {
        let errs = validate_webhook_url("http://example.com", &p().child("url"));
        assert!(errs.iter().any(|e| e.detail.contains("https")), "{errs:?}");
    }

    #[test]
    fn url_with_query_and_fragment_rejected() {
        let errs = validate_webhook_url("https://example.com/h?a=1#frag", &p().child("url"));
        assert!(errs.iter().any(|e| e.detail.contains("query")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.detail.contains("fragment")),
            "{errs:?}"
        );
    }

    #[test]
    fn service_requires_name_and_namespace() {
        let errs = validate_webhook_service("", "", None, None, &p().child("service"));
        assert!(errs.iter().any(|e| e.field.ends_with("name")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.field.ends_with("namespace")),
            "{errs:?}"
        );
    }

    #[test]
    fn service_port_out_of_range_rejected() {
        let errs = validate_webhook_service("svc", "ns", None, Some(0), &p().child("service"));
        assert!(
            errs.iter().any(|e| e.detail.contains("port is not valid")),
            "{errs:?}"
        );
        assert!(
            validate_webhook_service("svc", "ns", None, Some(443), &p().child("service"))
                .is_empty()
        );
    }

    #[test]
    fn service_path_empty_segment_rejected() {
        let errs =
            validate_webhook_service("svc", "ns", Some("/a//b"), None, &p().child("service"));
        assert!(
            errs.iter().any(|e| e.detail.contains("may not be empty")),
            "{errs:?}"
        );
        assert!(validate_webhook_service(
            "svc",
            "ns",
            Some("/mutate"),
            None,
            &p().child("service")
        )
        .is_empty());
    }

    #[test]
    fn admission_review_version_must_be_dns1035() {
        // Leading digit is valid DNS1123 but not DNS1035.
        let errs = validate_admission_review_versions(
            &["1v".to_string(), "v1".to_string()],
            &Path::new("admissionReviewVersions"),
        );
        assert!(
            errs.iter().any(|e| e.detail.contains("DNS-1035")),
            "{errs:?}"
        );
    }
}

#[cfg(test)]
mod match_condition_update_tests {
    use super::*;
    use serde_json::json;

    fn config(conditions: serde_json::Value) -> ValidatingWebhookConfiguration {
        serde_json::from_value(json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingWebhookConfiguration",
            "metadata": { "name": "hook.example.com" },
            "webhooks": [{
                "name": "hook.example.com",
                "clientConfig": { "url": "https://example.com/hook" },
                "sideEffects": "None",
                "admissionReviewVersions": ["v1"],
                "matchConditions": conditions,
            }],
        }))
        .expect("fixture decodes")
    }

    /// The create path always validates: a duplicate name is an error.
    #[test]
    fn a_create_validates_match_conditions() {
        let bad = config(json!([
            { "name": "same", "expression": "true" },
            { "name": "same", "expression": "false" }
        ]));
        let errs = validate_validating_webhook_configuration(&bad);
        assert!(
            errs.iter()
                .any(|e| e.field == "webhooks[0].matchConditions[1].name"),
            "{errs:?}"
        );
    }

    /// Upstream `ignoreValidatingWebhookMatchConditions` (`validation.go:605`):
    /// an unchanged list is left alone, so an object stored before the rule
    /// existed stays updatable.
    #[test]
    fn an_update_that_keeps_the_list_is_spared() {
        let bad = config(json!([
            { "name": "same", "expression": "true" },
            { "name": "same", "expression": "false" }
        ]));
        let errs = validate_validating_webhook_configuration_update(&bad, &bad);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// Changing any entry re-validates the whole list.
    #[test]
    fn an_update_that_changes_the_list_is_validated() {
        let old = config(json!([
            { "name": "same", "expression": "true" },
            { "name": "same", "expression": "false" }
        ]));
        let new = config(json!([
            { "name": "same", "expression": "true" },
            { "name": "same", "expression": "changed" }
        ]));
        let errs = validate_validating_webhook_configuration_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.field == "webhooks[0].matchConditions[1].name"),
            "{errs:?}"
        );
    }

    /// A list that grows is a change even when every kept entry matches.
    #[test]
    fn a_longer_list_is_validated() {
        let old = config(json!([{ "name": "one", "expression": "true" }]));
        let new = config(json!([
            { "name": "one", "expression": "true" },
            { "name": "one", "expression": "false" }
        ]));
        let errs = validate_validating_webhook_configuration_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.field == "webhooks[0].matchConditions[1].name"),
            "{errs:?}"
        );
    }
}
