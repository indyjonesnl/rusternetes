//! Server-side `ClusterRole` aggregation.
//!
//! This is a Rusternetes-only mechanism. Upstream does not aggregate at write
//! time: the `clusterroleaggregation` controller
//! (`pkg/controller/clusterroleaggregation/clusterroleaggregation_controller.go`)
//! watches ClusterRoles and rewrites the `rules` of every ClusterRole with an
//! `aggregationRule` whenever a ClusterRole changes. Rusternetes has no such
//! controller, so the rules are recomputed whenever the aggregating ClusterRole
//! itself is written (not when a matching child changes). The controller port
//! is tracked separately; this module goes when it lands.

use rusternetes_common::resources::{ClusterRole, PolicyRule};
use rusternetes_storage::{build_prefix, Storage};

/// Materialise the `rules` of a ClusterRole carrying an `aggregationRule` by
/// unioning the rules of every ClusterRole whose labels match any of the
/// `clusterRoleSelectors`. The parent's own name is excluded to avoid
/// self-aggregation loops, and so are other aggregating ClusterRoles.
///
/// Selector matching is `LabelSelectorAsSelector` + `Matches`, as the
/// controller does it (clusterroleaggregation_controller.go:104-108). It has no
/// skip-empty-selectors behaviour: an aggregation rule with an empty (`{}`)
/// selector selects every ClusterRole (#2012).
///
/// No-op when `aggregation_rule` is `None`.
pub async fn materialise_aggregated_rules<S: Storage>(storage: &S, clusterrole: &mut ClusterRole) {
    let Some(aggregation_rule) = clusterrole.aggregation_rule.clone() else {
        return;
    };
    let Some(selectors) = aggregation_rule.cluster_role_selectors else {
        clusterrole.rules = Vec::new();
        return;
    };

    let prefix = build_prefix("clusterroles", None);
    let candidates = match storage.list::<ClusterRole>(&prefix).await {
        Ok(list) => list,
        Err(e) => {
            tracing::warn!(
                "Failed to list ClusterRoles for aggregation of {}: {}",
                clusterrole.metadata.name,
                e
            );
            return;
        }
    };

    let mut aggregated: Vec<PolicyRule> = Vec::new();
    for candidate in &candidates {
        // Skip the parent itself and any other aggregating ClusterRole — only
        // leaf (non-aggregating) ClusterRoles contribute rules.
        if candidate.metadata.name == clusterrole.metadata.name {
            continue;
        }
        if candidate.aggregation_rule.is_some() {
            continue;
        }
        let labels = candidate.metadata.labels.clone().unwrap_or_default();
        if !selectors.iter().any(|s| s.matches_labels(&labels)) {
            continue;
        }
        for rule in &candidate.rules {
            if !aggregated.contains(rule) {
                aggregated.push(rule.clone());
            }
        }
    }

    clusterrole.rules = aggregated;
}
