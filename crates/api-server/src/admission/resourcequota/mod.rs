//! The ResourceQuota admission plugin, as the Store-backed resources see it.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/admission/plugin/resourcequota/`:
//! `QuotaAdmission.Validate` (admission.go:158-165), and `quotaEvaluator`'s
//! `checkAttributes` / `checkQuotas` / `CheckRequest` (controller.go:196-633).
//!
//! A request is checked against each ResourceQuota's `status.hard` and
//! `status.used`, and the usage it adds is written back to `status.used`
//! before the request proceeds — the quota controller corrects it later if
//! the write never happens. Upstream batches the requests of one namespace
//! on a work queue so they are checked one at a time; here the
//! per-namespace lock in [`evaluate`]
//! ([`crate::admission::lock_namespace_quota`]) serialises them, and the
//! optimistic status update is the cross-api-server guard, as upstream's is.
//!
//! `LimitedResources` (the admission configuration's `limitedResources`) is
//! read from the `ResourceQuota` entry of `--admission-control-config-file`
//! ([`config`]); empty by default, as in upstream.

pub mod config;
pub mod evaluator;

pub use config::LimitedResource;

use std::collections::{BTreeSet, HashMap};

use rusternetes_common::admission::Operation;
use rusternetes_common::quota::{self, ResourceList};
use rusternetes_common::resources::ResourceQuota;
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_key, build_prefix, Storage};
use serde_json::Value;

use rusternetes_common::resources::ScopedResourceSelectorRequirement;

use self::evaluator::{status_hard_names, Evaluator};
use crate::registry::rest::GroupResource;

/// The request as the plugin sees it: `admission.Attributes`.
pub struct Attributes<'a> {
    pub operation: Operation,
    pub namespace: &'a str,
    pub subresource: Option<&'a str>,
    pub object: &'a Value,
    pub old_object: Option<&'a Value>,
    pub dry_run: bool,
}

/// The `kind` of a Namespace, which `isNamespaceCreation` (admission.go:167-169)
/// exempts.
pub fn is_namespace_creation(operation: &Operation, group: &str, kind: &str) -> bool {
    *operation == Operation::Create && group.is_empty() && kind == "Namespace"
}

/// `admission.NewForbidden(a, err)` is built by the caller, which knows the
/// resource and name; this is the error the plugin returns.
#[derive(Debug, PartialEq)]
pub enum QuotaError {
    /// Rendered `<resource> "<name>" is forbidden: <msg>`.
    Forbidden(String),
    /// Any other failure, returned as-is.
    Other(String),
}

fn status_list(q: &ResourceQuota, used: bool) -> ResourceList {
    q.status
        .as_ref()
        .and_then(|s| {
            if used {
                s.used.as_ref()
            } else {
                s.hard.as_ref()
            }
        })
        .map(quota::parse_resource_list)
        .unwrap_or_default()
}

fn set_status_used(q: &mut ResourceQuota, used: &ResourceList) {
    let status = q.status.get_or_insert_with(Default::default);
    status.used = Some(quota::to_string_map(used));
}

/// `hasUsageStats` (controller.go:763-777).
pub(crate) fn has_usage_stats(q: &ResourceQuota, interesting: &[String]) -> bool {
    let used = q.status.as_ref().and_then(|s| s.used.as_ref());
    status_hard_names(q)
        .iter()
        .filter(|name| interesting.contains(name))
        .all(|name| used.is_some_and(|u| u.contains_key(name)))
}

pub(crate) fn pretty_print_resource_names(names: &[String]) -> String {
    let mut names = names.to_vec();
    names.sort();
    names.join(",")
}

/// `limitedByDefault` (controller.go:369-386): the consumed resource names
/// (usage above zero) containing one of a limited resource's `matchContains`.
fn limited_by_default(usage: &ResourceList, limited: &[&LimitedResource]) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    for lr in limited {
        for (name, value) in usage {
            if value.is_zero() || value.is_negative() {
                continue;
            }
            if lr.match_contains.iter().any(|m| name.contains(m.as_str())) {
                result.insert(name.clone());
            }
        }
    }
    result
}

/// Go's `%v` of `[]ScopedResourceSelectorRequirement`:
/// `[{PriorityClass In [a b]} {BestEffort Exists []}]`.
fn format_selectors(selectors: &[ScopedResourceSelectorRequirement]) -> String {
    let items: Vec<String> = selectors
        .iter()
        .map(|s| {
            format!(
                "{{{} {} [{}]}}",
                s.scope_name,
                s.operator,
                s.values.as_deref().unwrap_or_default().join(" ")
            )
        })
        .collect();
    format!("[{}]", items.join(" "))
}

/// `CheckRequest` (controller.go:470-633): whether `a` fits every quota, and
/// the quotas with the usage it would add. `limited` is the configuration's
/// `LimitedResources`.
pub fn check_request(
    quotas: &[ResourceQuota],
    a: &Attributes<'_>,
    evaluator: &dyn Evaluator,
    limited: &[LimitedResource],
) -> std::result::Result<Vec<ResourceQuota>, QuotaError> {
    if !evaluator.handles(a) {
        return Ok(quotas.to_vec());
    }

    // Scopes the object matches that the configuration limits
    // (`getMatchedLimitedScopes`, controller.go:388-406). Upstream returns
    // the quotas unchanged on an error here (:437-439).
    let mut limited_scopes: Vec<ScopedResourceSelectorRequirement> = Vec::new();
    for lr in limited {
        match evaluator.matching_scopes(a.object, &lr.match_scopes) {
            Ok(matched) => limited_scopes.extend(matched),
            Err(_) => return Ok(quotas.to_vec()),
        }
    }

    // The resource names that must exist in a covering quota
    // (controller.go:441-451).
    let gr = evaluator.group_resource();
    let limited_for_resource: Vec<&LimitedResource> = limited
        .iter()
        .filter(|lr| lr.api_group == gr.group && lr.resource == gr.resource)
        .collect();
    let limited_resource_names: BTreeSet<String> = if limited_for_resource.is_empty() {
        BTreeSet::new()
    } else {
        let delta_usage = evaluator.usage(a.object).map_err(QuotaError::Other)?;
        limited_by_default(&delta_usage, &limited_for_resource)
    };
    let mut restricted_resources_set: BTreeSet<String> = BTreeSet::new();

    // The quotas pertinent to this request (controller.go:505-536).
    let mut interesting: Vec<usize> = Vec::new();
    let mut restricted_scopes = Vec::new();
    for (i, q) in quotas.iter().enumerate() {
        let selectors = quota::scope_selectors_from_quota(&q.spec);
        let local = evaluator
            .matching_scopes(a.object, &selectors)
            .map_err(|e| {
                QuotaError::Other(format!(
                    "error matching scopes of quota {}, err: {e}",
                    q.metadata.name
                ))
            })?;
        restricted_scopes.extend(local);
        if !evaluator.matches(q, a.object).map_err(QuotaError::Other)? {
            continue;
        }
        let restricted = evaluator.matching_resources(&status_hard_names(q));
        // controller.go:470-472.
        evaluator.constraints(&restricted, a.object).map_err(|e| {
            QuotaError::Forbidden(format!("failed quota: {}: {e}", q.metadata.name))
        })?;
        if !has_usage_stats(q, &restricted) {
            return Err(QuotaError::Forbidden(format!(
                "status unknown for quota: {}, resources: {}",
                q.metadata.name,
                pretty_print_resource_names(&restricted)
            )));
        }
        interesting.push(i);
        restricted_resources_set.extend(restricted);
    }

    let input_usage = evaluator.usage(a.object).map_err(QuotaError::Other)?;
    let negative = quota::is_negative(&input_usage);
    if !negative.is_empty() {
        return Err(QuotaError::Forbidden(format!(
            "quota usage is negative for resource(s): {}",
            pretty_print_resource_names(&negative)
        )));
    }

    let mut delta_by_index: HashMap<usize, ResourceList> = interesting
        .iter()
        .map(|&i| (i, input_usage.clone()))
        .collect();
    let mut delta_when_no_interesting = ResourceList::new();
    if a.operation == Operation::Create && interesting.is_empty() {
        delta_when_no_interesting = input_usage.clone();
    }

    // An update is charged the growth over the stored object, unless it is a
    // create-on-update (no resourceVersion yet) (controller.go:555-591).
    if a.operation == Operation::Update {
        let Some(prev) = a.old_object else {
            return Err(QuotaError::Forbidden(
                "unable to get previous usage since prior version of object was not found"
                    .to_string(),
            ));
        };
        let prev_rv = prev
            .pointer("/metadata/resourceVersion")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !prev_rv.is_empty() {
            let prev_usage = evaluator.usage(prev).map_err(QuotaError::Other)?;
            let delta = quota::subtract_with_non_negative_result(&input_usage, &prev_usage);
            if interesting.is_empty() {
                delta_when_no_interesting = delta.clone();
            }
            // A quota the old object did not match is charged the full
            // input usage (controller.go:576-586).
            for &i in &interesting {
                if evaluator
                    .matches(&quotas[i], prev)
                    .map_err(QuotaError::Other)?
                {
                    delta_by_index.insert(i, delta.clone());
                }
            }
        } else if interesting.is_empty() {
            delta_when_no_interesting = input_usage.clone();
        }
    }

    // Zero usage cannot affect a quota (controller.go:593-610).
    delta_by_index.retain(|_, d| {
        *d = quota::remove_zeros(d);
        !d.is_empty()
    });
    if !interesting.is_empty() {
        if delta_by_index.is_empty() {
            return Ok(quotas.to_vec());
        }
    } else if quota::remove_zeros(&delta_when_no_interesting).is_empty() {
        return Ok(quotas.to_vec());
    }

    // Every resource limited by default needs a covering quota
    // (controller.go:612-618).
    let no_covering: Vec<&String> = limited_resource_names
        .difference(&restricted_resources_set)
        .collect();
    if !no_covering.is_empty() {
        return Err(QuotaError::Forbidden(format!(
            "insufficient quota to consume: {}",
            no_covering
                .iter()
                .map(|n| n.as_str())
                .collect::<Vec<_>>()
                .join(",")
        )));
    }

    // Every limited scope needs a covering quota scope (controller.go:
    // 620-628).
    let uncovered = evaluator
        .uncovered_quota_scopes(&limited_scopes, &restricted_scopes)
        .map_err(QuotaError::Other)?;
    if !uncovered.is_empty() {
        return Err(QuotaError::Other(format!(
            "insufficient quota to match these scopes: {}",
            format_selectors(&uncovered)
        )));
    }

    if interesting.is_empty() {
        return Ok(quotas.to_vec());
    }

    let mut out = quotas.to_vec();
    for &i in &interesting {
        let Some(delta) = delta_by_index.get(&i) else {
            continue;
        };
        let hard = status_list(&out[i], false);
        let used = status_list(&out[i], true);
        let requested = quota::mask(delta, &quota::resource_names(&hard));
        let new_usage = quota::add(&used, &requested);

        if a.subresource != Some("status") {
            let masked_new = quota::mask(&new_usage, &quota::resource_names(&requested));
            let (allowed, exceeded) = quota::less_than_or_equal(&masked_new, &hard);
            if !allowed {
                return Err(QuotaError::Forbidden(format!(
                    "exceeded quota: {}, requested: {}, used: {}, limited: {}",
                    out[i].metadata.name,
                    quota::pretty_print(&quota::mask(&requested, &exceeded)),
                    quota::pretty_print(&quota::mask(&used, &exceeded)),
                    quota::pretty_print(&quota::mask(&hard, &exceeded)),
                )));
            }
        }
        set_status_used(&mut out[i], &new_usage);
    }
    Ok(out)
}

/// `GetQuotas` (resource_access.go:121-155): the namespace's quotas.
async fn get_quotas<S: Storage>(storage: &S, namespace: &str) -> Result<Vec<ResourceQuota>> {
    storage
        .list(&build_prefix("resourcequotas", Some(namespace)))
        .await
}

/// `UpdateQuotaStatus` (resource_access.go:84-95): an optimistic write at
/// the quota's resourceVersion.
async fn update_quota_status<S: Storage>(storage: &S, q: &ResourceQuota) -> Result<()> {
    let key = build_key(
        "resourcequotas",
        q.metadata.namespace.as_deref(),
        &q.metadata.name,
    );
    storage.update(&key, q).await.map(|_| ())
}

/// `checkQuotas` (controller.go:228-363) for one request: check it, write
/// the quotas whose usage changed, and on a conflict re-read those quotas
/// and check again, `remaining_retries` times.
async fn check_quotas<S: Storage>(
    storage: &S,
    mut quotas: Vec<ResourceQuota>,
    a: &Attributes<'_>,
    evaluator: &dyn Evaluator,
    limited: &[LimitedResource],
    mut remaining_retries: u32,
) -> std::result::Result<(), QuotaError> {
    loop {
        let new_quotas = check_request(&quotas, a, evaluator, limited)?;
        if a.dry_run {
            return Ok(());
        }

        let changed: Vec<&ResourceQuota> = new_quotas
            .iter()
            .zip(&quotas)
            .filter(|(new, old)| !quota::equals(&status_list(new, true), &status_list(old, true)))
            .map(|(new, _)| new)
            .collect();
        if changed.is_empty() {
            return Ok(());
        }

        let mut failed: Vec<String> = Vec::new();
        let mut last_err = None;
        for q in changed {
            if let Err(e) = update_quota_status(storage, q).await {
                failed.push(q.metadata.name.clone());
                last_err = Some(e);
            }
        }
        let Some(last_err) = last_err else {
            return Ok(());
        };
        if remaining_retries == 0 {
            return Err(QuotaError::Other(last_err.to_string()));
        }
        remaining_retries -= 1;

        // Re-check only the quotas whose update failed, at their latest
        // version; a quota deleted meanwhile is skipped (controller.go:345-362).
        let namespace = quotas
            .first()
            .and_then(|q| q.metadata.namespace.clone())
            .unwrap_or_else(|| a.namespace.to_string());
        let fresh = get_quotas(storage, &namespace)
            .await
            .map_err(|_| QuotaError::Other(last_err.to_string()))?;
        quotas = fresh
            .into_iter()
            .filter(|q| failed.contains(&q.metadata.name))
            .collect();
    }
}

/// `QuotaAdmission.Validate` (admission.go:158-165) and
/// `quotaEvaluator.Evaluate` (controller.go:656-686), for one request.
pub async fn evaluate<S: Storage>(
    storage: &S,
    evaluator: &dyn Evaluator,
    a: &Attributes<'_>,
) -> std::result::Result<(), QuotaError> {
    evaluate_with(storage, evaluator, a, config::installed_limited_resources()).await
}

/// [`evaluate`] with an explicit `LimitedResources` configuration.
pub async fn evaluate_with<S: Storage>(
    storage: &S,
    evaluator: &dyn Evaluator,
    a: &Attributes<'_>,
    limited: &[LimitedResource],
) -> std::result::Result<(), QuotaError> {
    if a.namespace.is_empty() || !evaluator.handles(a) {
        return Ok(());
    }
    let _guard = crate::admission::lock_namespace_quota(a.namespace).await;
    // `checkAttributes` (controller.go:196-226).
    let quotas = get_quotas(storage, a.namespace)
        .await
        .map_err(|e| QuotaError::Other(e.to_string()))?;
    // `limitedResourcesDisabled` (controller.go:200-201).
    if quotas.is_empty() && limited.is_empty() {
        return Ok(());
    }
    check_quotas(storage, quotas, a, evaluator, limited, 3).await
}

/// `admission.NewForbidden` (apiserver/pkg/admission/errors.go:53-63):
/// `<group resource> "<name>" is forbidden: <msg>`, the resource qualified by
/// its group as `schema.GroupResource.String()` renders it.
pub fn to_api_error(err: QuotaError, gr: &GroupResource, name: &str) -> Error {
    match err {
        QuotaError::Forbidden(msg) => {
            let resource = if gr.group.is_empty() {
                gr.resource.clone()
            } else {
                format!("{}.{}", gr.resource, gr.group)
            };
            Error::Forbidden(format!("{resource} \"{name}\" is forbidden: {msg}"))
        }
        QuotaError::Other(msg) => Error::Internal(msg),
    }
}

#[cfg(test)]
mod tests;
