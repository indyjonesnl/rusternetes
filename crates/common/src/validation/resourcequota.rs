//! ResourceQuota validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateResourceQuotaSpec`
//! (release-1.35).
//!
//! Scope: `hard` resource names (qualified-name + standard-quota-resource
//! qualification) and quantities (valid, non-negative, integer-only resources
//! must be whole), `scopes` (known values, no conflicting pairs),
//! `scopeSelector.matchExpressions` (scope name + operator/values consistency +
//! conflicting pairs), and status updates (`resourceVersion` required + the
//! same name/quantity checks over `status.hard` / `status.used`).

use crate::quantity::Quantity;
use crate::resources::policy::{ResourceQuota, ResourceQuotaSpec};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_qualified_name;
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use std::collections::HashMap;

/// The standard ResourceQuota scopes (upstream `IsStandardResourceQuotaScope`).
const VALID_SCOPES: &[&str] = &[
    "Terminating",
    "NotTerminating",
    "BestEffort",
    "NotBestEffort",
    "PriorityClass",
    "CrossNamespacePodAffinity",
];

/// Standard resource names known to the quota tracking system. Mirrors
/// upstream `helper.standardQuotaResources`.
const STANDARD_QUOTA_RESOURCES: &[&str] = &[
    "cpu",
    "memory",
    "ephemeral-storage",
    "requests.cpu",
    "requests.memory",
    "requests.storage",
    "requests.ephemeral-storage",
    "limits.cpu",
    "limits.memory",
    "limits.ephemeral-storage",
    "pods",
    "resourcequotas",
    "services",
    "replicationcontrollers",
    "secrets",
    "persistentvolumeclaims",
    "configmaps",
    "services.nodeports",
    "services.loadbalancers",
];

/// Resources that are measured in whole-integer values. Mirrors upstream
/// `helper.integerResources` (extended resources are also integer, see
/// [`is_integer_resource_name`]).
const INTEGER_RESOURCES: &[&str] = &[
    "pods",
    "resourcequotas",
    "services",
    "replicationcontrollers",
    "secrets",
    "configmaps",
    "persistentvolumeclaims",
    "services.nodeports",
    "services.loadbalancers",
];

const HUGEPAGES_PREFIX: &str = "hugepages-";
const REQUESTS_HUGEPAGES_PREFIX: &str = "requests.hugepages-";

/// `helper.IsQuotaHugePageResourceName`.
fn is_quota_hugepage_resource_name(name: &str) -> bool {
    name.starts_with(HUGEPAGES_PREFIX) || name.starts_with(REQUESTS_HUGEPAGES_PREFIX)
}

/// `helper.IsStandardQuotaResourceName`.
fn is_standard_quota_resource_name(name: &str) -> bool {
    STANDARD_QUOTA_RESOURCES.contains(&name) || is_quota_hugepage_resource_name(name)
}

/// `helper.IsExtendedResourceName`; shared port in [`crate::quota`].
fn is_extended_resource_name(name: &str) -> bool {
    crate::quota::is_extended_resource_name(name)
}

/// `helper.IsIntegerResourceName`.
pub(crate) fn is_integer_resource_name(name: &str) -> bool {
    INTEGER_RESOURCES.contains(&name) || is_extended_resource_name(name)
}

/// Port of upstream `ValidateResourceQuotaResourceName`: the key is a valid
/// qualified name, and — when unprefixed (no `/`) — a standard quota resource.
fn validate_resource_quota_resource_name(name: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for msg in is_qualified_name(name) {
        errs.push(Error::invalid(fld_path, name.to_string(), msg));
    }
    if !errs.is_empty() {
        return errs;
    }
    // Unprefixed names (a single "/"-split segment) must be standard.
    if name.split('/').count() == 1 && !is_standard_quota_resource_name(name) {
        errs.push(Error::invalid(
            fld_path,
            name.to_string(),
            "must be a standard resource for quota",
        ));
    }
    errs
}

/// Port of upstream `ValidateResourceQuantityValue`: the quantity is a valid,
/// non-negative quantity, and — for integer-only resources — a whole integer
/// (`MilliValue() % 1000 == 0`).
pub fn validate_resource_quantity_value(name: &str, value: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match Quantity::parse(value) {
        Err(_) => {
            errs.push(Error::invalid(
                fld_path,
                value.to_string(),
                "must be a valid resource quantity",
            ));
            return errs;
        }
        Ok(q) => {
            if q.is_negative() {
                errs.push(Error::invalid(
                    fld_path,
                    value.to_string(),
                    "must be greater than or equal to 0",
                ));
            }
            if is_integer_resource_name(name) && q.milli_value() % 1000 != 0 {
                errs.push(Error::invalid(
                    fld_path,
                    value.to_string(),
                    "must be an integer",
                ));
            }
        }
    }
    errs
}

/// Validate a `resourceName: quantity` map (`spec.hard`, `status.hard`,
/// `status.used`) under `fld_path`, keying each entry with `.key()` to match
/// upstream `fldPath.Key(string(k))`.
fn validate_resource_quota_resources(
    resources: &HashMap<String, String>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (k, v) in resources {
        let res_path = fld_path.key(k.clone());
        errs.extend(validate_resource_quota_resource_name(k, &res_path));
        errs.extend(validate_resource_quantity_value(k, v, &res_path));
    }
    errs
}

/// Validate a `ResourceQuotaSpec`. Mirrors upstream `ValidateResourceQuotaSpec`.
pub fn validate_resource_quota_spec(spec: &ResourceQuotaSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // hard: each key must be a qualified, standard quota resource name and each
    // value a valid, non-negative (integer-where-required) quantity.
    if let Some(hard) = &spec.hard {
        errs.extend(validate_resource_quota_resources(
            hard,
            &fld_path.child("hard"),
        ));
    }

    // scopes: each must be a known value; certain pairs conflict.
    if let Some(scopes) = &spec.scopes {
        if !scopes.is_empty() {
            let scopes_path = fld_path.child("scopes");
            for s in scopes {
                if !VALID_SCOPES.contains(&s.as_str()) {
                    errs.push(Error::invalid(&scopes_path, s.clone(), "unsupported scope"));
                }
            }
            let has = |x: &str| scopes.iter().any(|s| s == x);
            if has("BestEffort") && has("NotBestEffort") {
                errs.push(Error::invalid(
                    &scopes_path,
                    "BestEffort,NotBestEffort".to_string(),
                    "conflicting scopes",
                ));
            }
            if has("Terminating") && has("NotTerminating") {
                errs.push(Error::invalid(
                    &scopes_path,
                    "Terminating,NotTerminating".to_string(),
                    "conflicting scopes",
                ));
            }
        }
    }

    // scopeSelector.matchExpressions
    errs.extend(validate_scope_selector(spec, fld_path));

    errs
}

/// Scopes that must use the `Exists` operator in a scopeSelector (only
/// `PriorityClass` supports `In`/`NotIn` with values).
const EXISTS_ONLY_SCOPES: &[&str] = &[
    "BestEffort",
    "NotBestEffort",
    "Terminating",
    "NotTerminating",
    "CrossNamespacePodAffinity",
];

/// Port of upstream `validateScopedResourceSelectorRequirement`: each
/// `scopeSelector.matchExpressions` entry has a known scopeName, an operator
/// consistent with that scope, and values consistent with the operator; and no
/// conflicting scope pair appears across the expressions.
fn validate_scope_selector(spec: &ResourceQuotaSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(sel) = &spec.scope_selector else {
        return errs;
    };
    let me_path = fld_path.child("scopeSelector").child("matchExpressions");
    let mut seen_scopes: Vec<&str> = Vec::new();

    for req in &sel.match_expressions {
        if !VALID_SCOPES.contains(&req.scope_name.as_str()) {
            errs.push(Error::invalid(
                &me_path.child("scopeName"),
                req.scope_name.clone(),
                "unsupported scope",
            ));
        }
        // Exists-only scopes reject any other operator.
        if EXISTS_ONLY_SCOPES.contains(&req.scope_name.as_str()) && req.operator != "Exists" {
            errs.push(Error::invalid(
                &me_path.child("operator"),
                req.operator.clone(),
                "must be 'Exists' when scope is any of ResourceQuotaScopeTerminating, ResourceQuotaScopeNotTerminating, ResourceQuotaScopeBestEffort, ResourceQuotaScopeNotBestEffort or ResourceQuotaScopeCrossNamespacePodAffinity",
            ));
        }
        let values_len = req.values.as_ref().map_or(0, |v| v.len());
        match req.operator.as_str() {
            "In" | "NotIn" => {
                if values_len == 0 {
                    errs.push(Error::required(
                        &me_path.child("values"),
                        "must be at least one value when `operator` is 'In' or 'NotIn' for scope selector",
                    ));
                }
            }
            "Exists" | "DoesNotExist" => {
                if values_len != 0 {
                    errs.push(Error::invalid(
                        &me_path.child("values"),
                        req.values.clone().unwrap_or_default(),
                        "must be no value when `operator` is 'Exist' or 'DoesNotExist' for scope selector",
                    ));
                }
            }
            other => {
                errs.push(Error::invalid(
                    &me_path.child("operator"),
                    other.to_string(),
                    "not a valid selector operator",
                ));
            }
        }
        seen_scopes.push(req.scope_name.as_str());
    }

    let has = |x: &str| seen_scopes.contains(&x);
    if has("BestEffort") && has("NotBestEffort") || has("Terminating") && has("NotTerminating") {
        errs.push(Error::invalid(
            &me_path,
            String::new(),
            "conflicting scopes",
        ));
    }

    errs
}

/// Upstream `ValidateResourceQuota` (validation.go:8068-8075): ObjectMeta
/// (`ValidateResourceQuotaName` is `NameIsDNSSubdomain`), the spec and the
/// status.
pub fn validate_resource_quota(rq: &ResourceQuota) -> ErrorList {
    let mut errs = validate_object_meta(
        &rq.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_resource_quota_spec(&rq.spec, &Path::new("spec")));
    errs.extend(validate_resource_quota_status(rq, &Path::new("status")));
    errs
}

/// Upstream `ValidateResourceQuotaStatus` (validation.go:8077-8094).
fn validate_resource_quota_status(rq: &ResourceQuota, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(status) = &rq.status {
        if let Some(hard) = &status.hard {
            errs.extend(validate_resource_quota_resources(
                hard,
                &fld_path.child("hard"),
            ));
        }
        if let Some(used) = &status.used {
            errs.extend(validate_resource_quota_resources(
                used,
                &fld_path.child("used"),
            ));
        }
    }
    errs
}

/// Upstream `ValidateResourceQuotaUpdate` (validation.go:8125-8144): the
/// ObjectMeta update, the spec, and `spec.scopes` is immutable (compared as a
/// set).
pub fn validate_resource_quota_update(new_rq: &ResourceQuota, old_rq: &ResourceQuota) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new_rq.metadata, &old_rq.metadata, &Path::new("metadata"));
    errs.extend(validate_resource_quota_spec(
        &new_rq.spec,
        &Path::new("spec"),
    ));

    let new_scopes: std::collections::HashSet<&String> =
        new_rq.spec.scopes.iter().flatten().collect();
    let old_scopes: std::collections::HashSet<&String> =
        old_rq.spec.scopes.iter().flatten().collect();
    if new_scopes != old_scopes {
        let shown = new_rq.spec.scopes.clone().unwrap_or_default().join(",");
        errs.push(Error::invalid(
            &Path::new("spec").child("scopes"),
            shown,
            "field is immutable",
        ));
    }

    errs
}

/// Upstream `ValidateResourceQuotaStatusUpdate` (validation.go:8147-8165):
/// the ObjectMeta update, a required `resourceVersion`, and every
/// `status.hard` / `status.used` entry passes the spec's resource-name and
/// quantity-value checks.
pub fn validate_resource_quota_status_update(
    new_rq: &ResourceQuota,
    old_rq: &ResourceQuota,
) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new_rq.metadata, &old_rq.metadata, &Path::new("metadata"));

    if new_rq
        .metadata
        .resource_version
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        errs.push(Error::required(&Path::new("resourceVersion"), ""));
    }

    errs.extend(validate_resource_quota_status(new_rq, &Path::new("status")));
    errs
}
