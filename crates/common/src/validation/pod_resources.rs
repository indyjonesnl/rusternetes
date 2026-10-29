//! Pod-level resources validation (`spec.resources`, feature gate
//! `PodLevelResources`).
//!
//! Port of release-1.35 `pkg/apis/core/validation/validation.go`:
//! `validatePodResources` (:4736), `validatePodResourceConsistency` (:4764),
//! `validatePodResourceRequirements` (:7817), `validateResourceRequirements`
//! (:7826), `validateResourceClaimNames` (:7885), `validatePodResourceName`
//! (:7457) and `validateResourceQuantityHugePageValue` (:7967).
//!
//! The caller gates this on `PodValidationOptions.PodLevelResourcesEnabled`
//! (validation.go:4645).

use std::collections::BTreeSet;

use crate::quantity::Quantity;
use crate::quota::aggregate_container_resources;
use crate::resources::pod::PodSpec;
use crate::resources::Pod;
use crate::types::{ResourceClaim, ResourceRequirements};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::limitrange::is_overcommit_allowed;
use crate::validation::metav1::is_dns1123_label;
use crate::validation::pod_options::PodValidationOptions;
use crate::validation::resourcequota::validate_resource_quantity_value;
use crate::validation::runtimeclass::{is_huge_page_resource_name, validate_resource_name};

const HUGEPAGES_PREFIX: &str = "hugepages-";

/// `validatePodResources` (validation.go:4736).
pub(crate) fn validate_pod_resources(
    spec: &PodSpec,
    pod_claim_names: &BTreeSet<String>,
    fld_path: &Path,
    opts: &PodValidationOptions,
) -> ErrorList {
    let Some(resources) = spec.resources.as_ref() else {
        return Vec::new();
    };
    let resources_fld_path = fld_path.child("resources");
    if spec.os.as_ref().is_some_and(|os| os.name == "windows") {
        // Do not include more detailed errors on the resources field value
        // if the resources field may not be set on the target OS.
        return vec![Error::forbidden(
            &resources_fld_path,
            "may not be set for a windows pod",
        )];
    }

    let mut errs = Vec::new();
    if resources.claims.is_some() {
        errs.push(Error::forbidden(
            &resources_fld_path.child("claims"),
            "claims may not be set for Resources at pod-level",
        ));
    }

    // validatePodResourceRequirements checks if resource names and quantities
    // are valid, and requests are less than limits.
    errs.extend(validate_pod_resource_requirements(
        resources,
        pod_claim_names,
        &resources_fld_path,
        opts,
    ));
    errs.extend(validate_pod_resource_consistency(spec, &resources_fld_path));
    errs
}

/// `validatePodResourceConsistency` (validation.go:4764): aggregate
/// container-level requests must be <= pod-level requests, and individual
/// container-level limits <= pod-level limits.
fn validate_pod_resource_consistency(spec: &PodSpec, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    let Some(pod_res) = spec.resources.as_ref() else {
        return errs;
    };
    // `AggregateContainerRequests`/`AggregateContainerLimits` over the spec
    // alone, with an empty `PodResourcesOptions` (no pod-level, no overhead).
    let pod = Pod {
        type_meta: Default::default(),
        metadata: Default::default(),
        spec: Some(spec.clone()),
        status: None,
    };
    let parse = |m: &Option<std::collections::HashMap<String, String>>, name: &str| {
        m.as_ref()
            .and_then(|m| m.get(name))
            .and_then(|v| Quantity::parse(v).ok())
    };

    // Pod-level requests must be >= aggregate requests of all containers.
    let req_path = fld_path.child("requests");
    for (name, ctr_reqs) in aggregate_container_resources(&pod, true) {
        // Skip if the pod-level request of the resource is not set.
        let Some(pod_req) = parse(&pod_res.requests, &name) else {
            continue;
        };
        if ctr_reqs.cmp_value(&pod_req) == std::cmp::Ordering::Greater {
            errs.push(Error::invalid(
                &req_path.key(&name),
                pod_req.canonical_string(),
                format!(
                    "must be greater than or equal to aggregate container requests of {}",
                    ctr_reqs.canonical_string()
                ),
            ));
        }
    }

    // Pod level hugepage limits must always be >= the aggregated container
    // level hugepage limits: hugepages are not overcommitable, so the pod
    // cgroup must reflect the pod-level request/limit and the container
    // cgroups must stay within it.
    for (name, ctr_lims) in aggregate_container_resources(&pod, false) {
        if !is_huge_page_resource_name(&name) {
            continue;
        }
        let Some(pod_lim) = parse(&pod_res.limits, &name) else {
            continue;
        };
        if ctr_lims.cmp_value(&pod_lim) == std::cmp::Ordering::Greater {
            errs.push(Error::invalid(
                &fld_path.child("limits").key(&name),
                pod_lim.canonical_string(),
                format!(
                    "must be greater than or equal to aggregate container limits of {}",
                    ctr_lims.canonical_string()
                ),
            ));
        }
    }

    // Individual container limits must be <= pod-level limits.
    for (i, ctr) in spec.containers.iter().enumerate() {
        let Some(limits) = ctr.resources.as_ref().and_then(|r| r.limits.as_ref()) else {
            continue;
        };
        let mut names: Vec<&String> = limits.keys().collect();
        names.sort();
        for name in names {
            // Skip if the pod-level limit of the resource is not set.
            let Some(pod_lim) = parse(&pod_res.limits, name) else {
                continue;
            };
            let Ok(ctr_limit) = Quantity::parse(&limits[name]) else {
                continue;
            };
            if ctr_limit.cmp_value(&pod_lim) == std::cmp::Ordering::Greater {
                // Upstream builds this path off `fldPath` (the `resources`
                // path), not the container's.
                let p = fld_path
                    .child("containers")
                    .index(i)
                    .key(name)
                    .child("limits");
                errs.push(Error::invalid(
                    &p,
                    ctr_limit.canonical_string(),
                    format!(
                        "must be less than or equal to pod limits of {}",
                        pod_lim.canonical_string()
                    ),
                ));
            }
        }
    }
    errs
}

/// `validatePodResourceName` (validation.go:7457): a valid resource name that
/// the PodLevelResources feature supports (`IsSupportedPodLevelResource`:
/// cpu, memory, or a `hugepages-` prefix).
fn validate_pod_resource_name(name: &str, fld_path: &Path) -> ErrorList {
    let errs = validate_resource_name(name, fld_path);
    if !errs.is_empty() {
        return errs;
    }
    if !(name == "cpu" || name == "memory" || name.starts_with(HUGEPAGES_PREFIX)) {
        // `sets.List(SupportedPodLevelResources())`: sorted, hugepages prefix
        // included as a literal member.
        return vec![Error::not_supported(
            fld_path,
            name.to_string(),
            &["cpu", "hugepages-", "memory"],
        )];
    }
    errs
}

/// `validatePodResourceRequirements` (validation.go:7817).
fn validate_pod_resource_requirements(
    requirements: &ResourceRequirements,
    pod_claim_names: &BTreeSet<String>,
    fld_path: &Path,
    opts: &PodValidationOptions,
) -> ErrorList {
    validate_resource_requirements(
        requirements,
        validate_pod_resource_name,
        pod_claim_names,
        fld_path,
        opts,
    )
}

/// `validateResourceRequirements` (validation.go:7826), shared shape for the
/// container- and pod-level callers; `resource_name_fn` is the
/// `resourceNameFn`.
fn validate_resource_requirements(
    requirements: &ResourceRequirements,
    resource_name_fn: fn(&str, &Path) -> ErrorList,
    pod_claim_names: &BTreeSet<String>,
    fld_path: &Path,
    opts: &PodValidationOptions,
) -> ErrorList {
    let mut errs = Vec::new();
    let lim_path = fld_path.child("limits");
    let req_path = fld_path.child("requests");
    let mut lim_contains_cpu_or_memory = false;
    let mut req_contains_cpu_or_memory = false;
    let mut lim_contains_huge_pages = false;
    let mut req_contains_huge_pages = false;
    let supported_qos = |n: &str| n == "cpu" || n == "memory";

    let limits = requirements.limits.clone().unwrap_or_default();
    let requests = requirements.requests.clone().unwrap_or_default();
    let sorted = |m: &std::collections::HashMap<String, String>| {
        let mut k: Vec<String> = m.keys().cloned().collect();
        k.sort();
        k
    };

    for name in sorted(&limits) {
        let raw = &limits[&name];
        let path = lim_path.key(&name);
        // Validate resource name.
        errs.extend(resource_name_fn(&name, &path));
        // Validate resource quantity.
        errs.extend(validate_resource_quantity_value(&name, raw, &path));
        let Ok(quantity) = Quantity::parse(raw) else {
            continue;
        };
        if is_huge_page_resource_name(&name) {
            lim_contains_huge_pages = true;
            if let Some(e) = validate_resource_quantity_huge_page_value(&name, &quantity, opts) {
                errs.push(Error::invalid(&path, quantity.canonical_string(), e));
            }
        }
        if supported_qos(&name) {
            lim_contains_cpu_or_memory = true;
        }
    }
    for name in sorted(&requests) {
        let raw = &requests[&name];
        let path = req_path.key(&name);
        errs.extend(resource_name_fn(&name, &path));
        errs.extend(validate_resource_quantity_value(&name, raw, &path));
        let Ok(quantity) = Quantity::parse(raw) else {
            continue;
        };

        // Check that request <= limit.
        match limits.get(&name).and_then(|l| Quantity::parse(l).ok()) {
            Some(limit_quantity) => {
                let ord = quantity.cmp_value(&limit_quantity);
                // For non overcommitable resources, not only requests can't
                // exceed limits, they also can't be lower, i.e. must be equal.
                if ord != std::cmp::Ordering::Equal && !is_overcommit_allowed(&name) {
                    errs.push(Error::invalid(
                        &req_path,
                        quantity.canonical_string(),
                        format!(
                            "must be equal to {name} limit of {}",
                            limit_quantity.canonical_string()
                        ),
                    ));
                } else if ord == std::cmp::Ordering::Greater {
                    errs.push(Error::invalid(
                        &req_path,
                        quantity.canonical_string(),
                        format!(
                            "must be less than or equal to {name} limit of {}",
                            limit_quantity.canonical_string()
                        ),
                    ));
                }
            }
            None if !limits.contains_key(&name) && !is_overcommit_allowed(&name) => {
                errs.push(Error::required(
                    &lim_path,
                    "Limit must be set for non overcommitable resources",
                ));
            }
            None => {}
        }
        if is_huge_page_resource_name(&name) {
            req_contains_huge_pages = true;
            if let Some(e) = validate_resource_quantity_huge_page_value(&name, &quantity, opts) {
                errs.push(Error::invalid(&path, quantity.canonical_string(), e));
            }
        }
        if supported_qos(&name) {
            req_contains_cpu_or_memory = true;
        }
    }
    if !lim_contains_cpu_or_memory
        && !req_contains_cpu_or_memory
        && (req_contains_huge_pages || lim_contains_huge_pages)
    {
        errs.push(Error::forbidden(
            fld_path,
            "HugePages require cpu or memory",
        ));
    }

    errs.extend(validate_resource_claim_names(
        requirements.claims.as_deref().unwrap_or(&[]),
        pod_claim_names,
        &fld_path.child("claims"),
    ));
    errs
}

/// `validateResourceClaimNames` (validation.go:7885): every
/// `ResourceRequirements.Claims` name must exist in
/// `PodSpec.ResourceClaims`.
fn validate_resource_claim_names(
    claims: &[ResourceClaim],
    pod_claim_names: &BTreeSet<String>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for (i, claim) in claims.iter().enumerate() {
        let name = &claim.name;
        if name.is_empty() {
            errs.push(Error::required(&fld_path.index(i), ""));
            continue;
        }
        let request = claim.request.as_deref().unwrap_or("");
        if names.contains(name) {
            // All requests of that claim already referenced.
            errs.push(Error::duplicate(&fld_path.index(i), name.clone()));
        } else {
            let mut key = name.clone();
            if !request.is_empty() {
                for msg in is_dns1123_label(request) {
                    errs.push(Error::invalid(
                        &fld_path.index(i).child("request"),
                        request.to_string(),
                        msg,
                    ));
                }
                key = format!("{name}/{request}");
            }
            if names.contains(&key) {
                // The exact same entry was already referenced.
                errs.push(Error::duplicate(&fld_path.index(i), key.clone()));
            } else if request.is_empty() {
                // Referencing a whole claim overlaps with any previously
                // referenced request of it; that needs an iteration.
                for k in &names {
                    if let Some(idx) = k.find('/') {
                        if &k[..idx] == name {
                            errs.push(Error::duplicate(&fld_path.index(i), name.clone()));
                        }
                    }
                }
            }
            names.push(key);
        }
        if !pod_claim_names.contains(name) {
            // field.NotFound doesn't accept an explanation; upstream adds one
            // for the user.
            let mut e = Error::not_found(&fld_path.index(i), name.clone());
            let mut detail = "must be one of the names in pod.spec.resourceClaims".to_string();
            if pod_claim_names.is_empty() {
                detail.push_str(" which is empty");
            } else {
                detail.push_str(": ");
                detail.push_str(
                    &pod_claim_names
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
            e.detail = detail;
            errs.push(e);
        }
    }
    errs
}

/// `validateResourceQuantityHugePageValue` (validation.go:7967) with
/// `IsHugePageResourceValueDivisible` (`pkg/apis/core/helper/helpers.go:43`).
fn validate_resource_quantity_huge_page_value(
    name: &str,
    quantity: &Quantity,
    opts: &PodValidationOptions,
) -> Option<String> {
    if !is_huge_page_resource_name(name) {
        return None;
    }
    if !opts.allow_indivisible_huge_pages_values && !is_huge_page_value_divisible(name, quantity) {
        return Some(format!(
            "{} is not positive integer multiple of {name}",
            quantity.canonical_string()
        ));
    }
    None
}

fn is_huge_page_value_divisible(name: &str, quantity: &Quantity) -> bool {
    let Some(size) = name
        .strip_prefix(HUGEPAGES_PREFIX)
        .and_then(|s| Quantity::parse(s).ok())
    else {
        return false;
    };
    if size.is_negative() || size.is_zero() || size.milli_value() % 1000 != 0 {
        return false;
    }
    quantity.value() % size.value() == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(v: serde_json::Value) -> PodSpec {
        serde_json::from_value(v).unwrap()
    }
    fn opts() -> PodValidationOptions {
        PodValidationOptions {
            allow_indivisible_huge_pages_values: true,
            ..Default::default()
        }
    }
    fn names() -> BTreeSet<String> {
        ["resource-claim".to_string()].into()
    }
    fn run(s: &PodSpec) -> Vec<String> {
        validate_pod_resources(s, &names(), &Path::new("spec"), &opts())
            .iter()
            .map(|e| e.to_string())
            .collect()
    }
    fn base(os: Option<&str>, resources: serde_json::Value) -> PodSpec {
        let mut v = json!({
            "containers": [{"name": "c", "image": "i"}],
            "resourceClaims": [{"name": "resource-claim"}],
            "resources": resources,
        });
        if let Some(os) = os {
            v["os"] = json!({"name": os});
        }
        spec(v)
    }
    fn good() -> serde_json::Value {
        json!({
            "requests": {"cpu": "10", "memory": "10Mi"},
            "limits": {"cpu": "20", "memory": "20Mi", "hugepages-2Mi": "100Mi"},
        })
    }

    /// `TestValidatePodResources` (validation_test.go:20484).
    #[test]
    fn pod_resources_os_and_claims() {
        assert!(run(&base(Some("linux"), good())).is_empty());
        let errs = run(&base(Some("windows"), good()));
        assert_eq!(
            errs,
            vec!["spec.resources: Forbidden: may not be set for a windows pod"]
        );
        let mut r = good();
        r["claims"] = json!([{"name": "resource-claim"}]);
        let errs = run(&base(Some("linux"), r));
        assert_eq!(
            errs,
            vec!["spec.resources.claims: Forbidden: claims may not be set for Resources at pod-level"]
        );
    }

    /// No `spec.resources` means no pod-level validation at all.
    #[test]
    fn no_pod_resources_is_a_no_op() {
        let s = spec(json!({"containers": [{"name": "c", "image": "i"}]}));
        assert!(run(&s).is_empty());
    }

    /// `validatePodResourceName` (:7457): only cpu, memory and hugepages.
    #[test]
    fn unsupported_pod_level_resource_name() {
        let errs = run(&base(
            None,
            json!({"requests": {"ephemeral-storage": "1Gi"}}),
        ));
        assert!(
            errs.iter().any(|e| e.contains("Unsupported value")),
            "{errs:?}"
        );
    }

    /// `validateResourceRequirements` rules reached at pod level (:7826).
    #[test]
    fn requirements_rules() {
        let errs = run(&base(
            None,
            json!({"requests": {"cpu": "2"}, "limits": {"cpu": "1"}}),
        ));
        assert!(
            errs.iter()
                .any(|e| e.contains("must be less than or equal to cpu limit of 1")),
            "{errs:?}"
        );
        let errs = run(&base(None, json!({"requests": {"hugepages-2Mi": "2Mi"}})));
        assert!(
            errs.iter()
                .any(|e| e.contains("Limit must be set for non overcommitable resources")),
            "{errs:?}"
        );
        let errs = run(&base(
            None,
            json!({"requests": {"hugepages-2Mi": "2Mi"}, "limits": {"hugepages-2Mi": "2Mi"}}),
        ));
        assert!(
            errs.iter()
                .any(|e| e.contains("HugePages require cpu or memory")),
            "{errs:?}"
        );
        let mut strict = opts();
        strict.allow_indivisible_huge_pages_values = false;
        let s = base(
            None,
            json!({"limits": {"cpu": "1", "hugepages-2Mi": "3Mi"}}),
        );
        let errs = validate_pod_resources(&s, &names(), &Path::new("spec"), &strict);
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("is not positive integer multiple of")),
            "{errs:?}"
        );
    }

    fn with_containers(pod: serde_json::Value, ctrs: serde_json::Value) -> PodSpec {
        spec(json!({"containers": ctrs, "resources": pod}))
    }
    fn consistency(s: &PodSpec) -> Vec<String> {
        validate_pod_resource_consistency(s, &Path::new("resources"))
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    /// `TestValidatePodResourceConsistency` (validation_test.go:20568).
    #[test]
    fn consistency_rules() {
        let c = |r: serde_json::Value| json!({"name": "c", "image": "i", "resources": r});
        // Aggregate requests above pod requests: two errors (cpu, memory).
        let s = with_containers(
            json!({"requests": {"cpu": "10", "memory": "10Mi"}}),
            json!([
                c(json!({"requests": {"cpu": "6", "memory": "5Mi"}})),
                c(json!({"requests": {"cpu": "8", "memory": "7Mi"}}))
            ]),
        );
        let errs = consistency(&s);
        assert_eq!(errs.len(), 2, "{errs:?}");
        assert!(errs
            .iter()
            .all(|e| e.contains("must be greater than or equal to aggregate container requests")));
        // Equal is fine.
        let s = with_containers(
            json!({"requests": {"cpu": "10"}}),
            json!([
                c(json!({"requests": {"cpu": "5"}})),
                c(json!({"requests": {"cpu": "5"}}))
            ]),
        );
        assert!(consistency(&s).is_empty());
        // Resource unsupported at pod level (storage) in containers: ignored.
        let s = with_containers(
            json!({"requests": {"cpu": "10"}}),
            json!([c(json!({"requests": {"cpu": "6", "storage": "10Gi"}}))]),
        );
        assert!(consistency(&s).is_empty());
        // Aggregate cpu limits above the pod limit are NOT an error (only
        // hugepages aggregate); each individual limit is checked instead.
        let s = with_containers(
            json!({"limits": {"cpu": "10"}}),
            json!([
                c(json!({"limits": {"cpu": "5"}})),
                c(json!({"limits": {"cpu": "6"}}))
            ]),
        );
        assert!(consistency(&s).is_empty());
        // Hugepage aggregate limits above pod limits.
        let s = with_containers(
            json!({"limits": {"cpu": "10", "hugepages-2Mi": "4Mi"}}),
            json!([
                c(json!({"limits": {"cpu": "6", "hugepages-2Mi": "4Mi"}})),
                c(json!({"limits": {"cpu": "6", "hugepages-2Mi": "4Mi"}}))
            ]),
        );
        let errs = consistency(&s);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("must be greater than or equal to aggregate container limits"));
        // Individual container limit above the pod limit.
        let s = with_containers(
            json!({"limits": {"cpu": "10"}}),
            json!([c(json!({"limits": {"cpu": "11"}}))]),
        );
        let errs = consistency(&s);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("must be less than or equal to pod limits of 10"));
    }

    /// "bad OS for PodLevelResources hides bad cpu limit" (:10633): the
    /// windows Forbidden replaces every other pod-level error.
    #[test]
    fn windows_hides_consistency_errors() {
        let s = spec(json!({
            "containers": [{"name": "c", "image": "i",
                "resources": {"limits": {"cpu": "200m"}}}],
            "resources": {"limits": {"cpu": "100m"}},
            "os": {"name": "windows"},
        }));
        assert_eq!(
            run(&s),
            vec!["spec.resources: Forbidden: may not be set for a windows pod"]
        );
    }

    #[test]
    fn claim_names_not_found() {
        let errs = validate_resource_claim_names(
            &[ResourceClaim {
                name: "x".into(),
                request: None,
            }],
            &BTreeSet::new(),
            &Path::new("claims"),
        );
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("must be one of the names in pod.spec.resourceClaims which is empty"));
    }
}
