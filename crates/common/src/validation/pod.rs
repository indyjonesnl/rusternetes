//! Pod update validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePodUpdate` (release-1.35).
//!
//! Composes the four upstream pre-checks (container count, tolerations
//! additions-only, schedulingGates deletions-only, terminationGracePeriodSeconds
//! immutability with the negative→1 relaxation) and the munge+DeepEqual fence
//! that catches everything else. Shared by `pod::update()` and `pod::patch()`
//! so `kubectl patch pod` honours the same immutability contract as PUT.
//!
//! NOT covered (intentionally deferred):
//! - Image whitespace / empty-image checks — pod create-time validation already
//!   rejects these via `validatePodMetadataAndSpec`; the update path can rely
//!   on it because containers can't be added/removed.
//! - ActiveDeadlineSeconds precise semantics — the api-server handler enforces
//!   these directly (see `crates/api-server/src/handlers/pod.rs::update`)
//!   because the error wording is checked by tests pinned at that layer.
//!
//! Now covered (added on top of the calfonso port):
//! - Gated-pod `nodeSelector` / `nodeAffinity` mutation rules
//!   (`validation.go:5786-5828`) — when the OLD pod has non-empty
//!   `schedulingGates`, nodeSelector additions and nodeAffinity additions are
//!   allowed (no deletions/mutations of existing entries).

use std::collections::HashMap;

use crate::resources::pod::{
    Affinity, NodeAffinity, NodeSelectorTerm, PodSchedulingGate, PodSpec, Toleration,
};
use crate::validation::field::{Error, ErrorList, Path};

/// Mirrors upstream `validateOnlyAddedTolerations` (validation.go:5630).
///
/// Every old toleration must still appear in the new list (order independent
/// via `PartialEq`); additions are allowed. Returns a `Forbidden` error if
/// any old toleration is missing or modified.
pub fn validate_only_added_tolerations(
    old: &[Toleration],
    new: &[Toleration],
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for ot in old {
        if !new.iter().any(|nt| nt == ot) {
            errs.push(Error::forbidden(
                path,
                "existing tolerations may not be modified or removed",
            ));
            return errs;
        }
    }
    errs
}

/// Mirrors upstream `validateOnlyDeletedSchedulingGates` (validation.go:5651).
///
/// Every new gate (compared by `.name`) must already exist in old; additions
/// are forbidden. Deletions are allowed (the scheduler clears gates to
/// release a pod for scheduling).
pub fn validate_only_deleted_scheduling_gates(
    old: &[PodSchedulingGate],
    new: &[PodSchedulingGate],
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (idx, ng) in new.iter().enumerate() {
        if !old.iter().any(|og| og.name == ng.name) {
            errs.push(Error::forbidden(
                &path.index(idx),
                "only deletion is allowed, but found new scheduling gate",
            ));
        }
    }
    errs
}

/// Mirrors upstream's TerminationGracePeriodSeconds rule
/// (validation.go:5780-5783). Field is immutable, with one relaxation: an
/// old negative value may be replaced by `1` (kubelet defaulting legacy).
pub fn validate_termination_grace_period_immutable(
    old: Option<i64>,
    new: Option<i64>,
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if old == new {
        return errs;
    }
    // negative → 1 relaxation (matches upstream)
    if let (Some(o), Some(1)) = (old, new) {
        if o < 0 {
            return errs;
        }
    }
    errs.push(Error::invalid(
        path,
        format!("{:?}", new),
        "field is immutable",
    ));
    errs
}

/// Top-level immutability fence. Composes the four pre-checks above plus a
/// munge+DeepEqual fence that catches any other forbidden field changes.
/// Mirrors `ValidatePodUpdate` (validation.go:5695-5838).
///
/// `is_ephemeral_subresource` controls whether the ephemeral-containers slice
/// is reset in the munged copy — set to `true` when invoked from the
/// `/ephemeralcontainers` subresource path. The dedicated EC add-only check
/// runs upstream of this fence; resetting the field here lets legitimate
/// subresource additions pass the DeepEqual.
pub fn validate_pod_spec_update(
    old: &PodSpec,
    new: &PodSpec,
    is_ephemeral_subresource: bool,
) -> Result<(), String> {
    let spec = Path::new("spec");

    // 1. Container count immutability — upstream ValidateContainerUpdates
    //    (validation.go:5579-5598). Short-circuits with a clear message
    //    rather than hitting the broader fence (which would also reject
    //    due to slice-length diff, but with a noisier error).
    if old.containers.len() != new.containers.len() {
        return Err("pod updates may not add or remove containers".to_string());
    }

    // 2. Tolerations: additions only.
    let empty_tols: Vec<Toleration> = Vec::new();
    let old_tols = old.tolerations.as_ref().unwrap_or(&empty_tols);
    let new_tols = new.tolerations.as_ref().unwrap_or(&empty_tols);
    let errs = validate_only_added_tolerations(old_tols, new_tols, &spec.child("tolerations"));
    if let Some(e) = errs.first() {
        return Err(e.to_string());
    }

    // 3. SchedulingGates: deletions only.
    let empty_gates: Vec<PodSchedulingGate> = Vec::new();
    let old_gates = old.scheduling_gates.as_ref().unwrap_or(&empty_gates);
    let new_gates = new.scheduling_gates.as_ref().unwrap_or(&empty_gates);
    let errs = validate_only_deleted_scheduling_gates(
        old_gates,
        new_gates,
        &spec.child("schedulingGates"),
    );
    if let Some(e) = errs.first() {
        return Err(e.to_string());
    }

    // 4. TerminationGracePeriodSeconds: immutable except negative→1.
    let errs = validate_termination_grace_period_immutable(
        old.termination_grace_period_seconds,
        new.termination_grace_period_seconds,
        &spec.child("terminationGracePeriodSeconds"),
    );
    if let Some(e) = errs.first() {
        return Err(e.to_string());
    }

    // 5. Munge + DeepEqual fence. Reset every field K8s allows to mutate to
    //    the OLD value, then compare. Any remaining diff = forbidden change.
    let mut munged = new.clone();
    // containers[*].image (counts already verified equal above)
    for (i, c) in munged.containers.iter_mut().enumerate() {
        c.image = old.containers[i].image.clone();
    }
    // initContainers[*].image — only when both sides have init containers
    if let (Some(old_init), Some(new_init)) = (&old.init_containers, &mut munged.init_containers) {
        for (i, c) in new_init.iter_mut().enumerate() {
            if i < old_init.len() {
                c.image = old_init[i].image.clone();
            }
        }
    }
    munged.active_deadline_seconds = old.active_deadline_seconds;
    munged.termination_grace_period_seconds = old.termination_grace_period_seconds;
    munged.tolerations = old.tolerations.clone();
    munged.scheduling_gates = old.scheduling_gates.clone();

    // Gated-pod scheduling-directive relaxation. Mirrors upstream
    // validation.go:5786-5828. A pod with non-empty schedulingGates is not
    // yet scheduled; nodeSelector additions and nodeAffinity additions are
    // allowed so the scheduler can refine placement before clearing the
    // gates. Once gates are cleared (pod scheduled), the fields become
    // immutable again — captured here by gating the relaxation on the OLD
    // pod's schedulingGates.
    let old_pod_is_gated = old
        .scheduling_gates
        .as_ref()
        .map(|g| !g.is_empty())
        .unwrap_or(false);
    if old_pod_is_gated {
        let errs = validate_node_selector_only_added(
            old.node_selector.as_ref(),
            munged.node_selector.as_ref(),
            &spec.child("nodeSelector"),
        );
        if let Some(e) = errs.first() {
            return Err(e.to_string());
        }
        munged.node_selector = old.node_selector.clone();

        let old_node_affinity = old.affinity.as_ref().and_then(|a| a.node_affinity.as_ref());
        let munged_node_affinity = munged
            .affinity
            .as_ref()
            .and_then(|a| a.node_affinity.as_ref());
        let na_changed = !node_affinity_eq(old_node_affinity, munged_node_affinity);
        if na_changed {
            let errs = validate_node_affinity_only_added(
                old_node_affinity,
                munged_node_affinity,
                &spec.child("affinity").child("nodeAffinity"),
            );
            if let Some(e) = errs.first() {
                return Err(e.to_string());
            }
            // Mirror upstream's case-by-case munge:
            // - new affinity nil, old node-affinity nil: nothing to do.
            // - new affinity nil, old node-affinity set: rebuild with old NA.
            // - new affinity set, old affinity nil, new has only NA: drop affinity entirely (only NA was being added).
            // - otherwise: reset just the node_affinity in the munged Affinity.
            match (munged.affinity.as_mut(), old.affinity.as_ref()) {
                (None, None) => {}
                (None, Some(old_aff)) => {
                    munged.affinity = Some(Affinity {
                        node_affinity: old_aff.node_affinity.clone(),
                        pod_affinity: None,
                        pod_anti_affinity: None,
                    });
                }
                (Some(new_aff), None)
                    if new_aff.pod_affinity.is_none() && new_aff.pod_anti_affinity.is_none() =>
                {
                    munged.affinity = None;
                }
                (Some(new_aff), _) => {
                    new_aff.node_affinity = old_node_affinity.cloned();
                }
            }
        }
    }

    // Bug fix vs upstream calfonso 85c36973: also reset ephemeral_containers
    // when invoked from the /ephemeralcontainers subresource so legitimate
    // additions do not trip the fence. The dedicated add-only EC check (in
    // pod::update) runs before this and is the source of truth for add-only
    // semantics.
    if is_ephemeral_subresource {
        munged.ephemeral_containers = old.ephemeral_containers.clone();
    }

    let mut munged_json = serde_json::to_value(&munged).unwrap_or_default();
    let old_json = serde_json::to_value(old).unwrap_or_default();
    // Treat fields the client omitted (null/missing in `new`) as "unchanged":
    // copy old's value before comparing. Implements partial-update semantics
    // matching K8s' defaulting + admission pipeline (which re-runs on every
    // request and refills server-managed fields like serviceAccountName,
    // auto-injected volumes/volumeMounts, priority, etc.). Without this, a
    // client PUTting only the field they care about would trip the fence on
    // any server-defaulted field they didn't echo back.
    fill_nulls_from(&mut munged_json, &old_json);
    if munged_json != old_json {
        return Err("pod updates may not change fields other than \
             `spec.containers[*].image`, `spec.initContainers[*].image`, \
             `spec.activeDeadlineSeconds`, `spec.terminationGracePeriodSeconds`, \
             `spec.tolerations` (additions only), `spec.schedulingGates` (deletions only)"
            .to_string());
    }

    Ok(())
}

/// Recursively backfill `null`/missing keys in `dst` with the corresponding
/// value from `src`. Mirrors the effect of K8s' defaulting + admission
/// pipeline re-running on every UPDATE: a client that omits a field gets
/// the previously-stored value, which then matches old in DeepEqual.
///
/// Arrays are merged element-wise ONLY when both sides have the same
/// length (otherwise the user's intent is ambiguous and the fence catches
/// the mismatch). This handles `spec.containers[*]` correctly because the
/// container-count pre-check has already enforced equal lengths.
fn fill_nulls_from(dst: &mut serde_json::Value, src: &serde_json::Value) {
    use serde_json::Value;
    match (dst, src) {
        (Value::Object(dst_map), Value::Object(src_map)) => {
            for (k, src_v) in src_map {
                match dst_map.get_mut(k) {
                    None => {
                        dst_map.insert(k.clone(), src_v.clone());
                    }
                    Some(dst_v) if dst_v.is_null() => {
                        *dst_v = src_v.clone();
                    }
                    Some(dst_v) => fill_nulls_from(dst_v, src_v),
                }
            }
        }
        (Value::Array(dst_arr), Value::Array(src_arr)) if dst_arr.len() == src_arr.len() => {
            for (d, s) in dst_arr.iter_mut().zip(src_arr.iter()) {
                fill_nulls_from(d, s);
            }
        }
        _ => {}
    }
}

/// Mirrors upstream `validateNodeSelectorMutation` (validation.go:9311).
/// On a gated pod, every key in the old selector must reappear in the new
/// with the same value. Additions of new keys are allowed.
pub fn validate_node_selector_only_added(
    old: Option<&HashMap<String, String>>,
    new: Option<&HashMap<String, String>>,
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let empty: HashMap<String, String> = HashMap::new();
    let old_map = old.unwrap_or(&empty);
    let new_map = new.unwrap_or(&empty);
    for (k, v) in old_map {
        match new_map.get(k) {
            Some(nv) if nv == v => {}
            _ => {
                errs.push(Error::invalid(
                    path,
                    format!("{:?}", new_map),
                    "only additions to spec.nodeSelector are allowed (no mutations or deletions)",
                ));
                return errs;
            }
        }
    }
    errs
}

/// Equality on `Option<&NodeAffinity>`. NodeAffinity does not derive
/// PartialEq, so compare via JSON serialisation — cheap and sufficient
/// for validation paths.
fn node_affinity_eq(a: Option<&NodeAffinity>, b: Option<&NodeAffinity>) -> bool {
    serde_json::to_value(a).unwrap_or_default() == serde_json::to_value(b).unwrap_or_default()
}

/// Mirrors upstream `validateNodeAffinityMutation` (validation.go:9324).
/// Allows: nil old → anything; non-empty old terms → only additions to
/// MatchExpressions / MatchFields inside each existing term. Disallows:
/// changing the number of NodeSelectorTerms, deleting/mutating existing
/// MatchExpression / MatchField entries.
pub fn validate_node_affinity_only_added(
    old: Option<&NodeAffinity>,
    new: Option<&NodeAffinity>,
    path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // If old node-affinity (or its required block) is nil, anything is allowed.
    let old_required = old.and_then(|na| {
        na.required_during_scheduling_ignored_during_execution
            .as_ref()
    });
    let old_terms = match old_required {
        Some(ns) => &ns.node_selector_terms,
        None => return errs,
    };

    let new_required = new.and_then(|na| {
        na.required_during_scheduling_ignored_during_execution
            .as_ref()
    });
    let empty_terms: Vec<NodeSelectorTerm> = Vec::new();
    let new_terms = new_required
        .map(|ns| &ns.node_selector_terms)
        .unwrap_or(&empty_terms);

    let terms_path = path
        .child("requiredDuringSchedulingIgnoredDuringExecution")
        .child("nodeSelectorTerms");

    if !old_terms.is_empty() && old_terms.len() != new_terms.len() {
        errs.push(Error::invalid(
            &terms_path,
            format!("{:?}", new_terms),
            "no additions/deletions to non-empty NodeSelectorTerms list are allowed",
        ));
        return errs;
    }

    for (i, old_term) in old_terms.iter().enumerate() {
        if !node_selector_term_has_only_additions(&new_terms[i], old_term) {
            errs.push(Error::invalid(
                &terms_path.index(i),
                format!("{:?}", new_terms[i]),
                "only additions are allowed (no mutations or deletions)",
            ));
        }
    }
    errs
}

/// Mirrors upstream `validateNodeSelectorTermHasOnlyAdditions`
/// (validation.go:9354). New term's MatchExpressions/MatchFields must
/// be a prefix-superset of old's (no truncation, no mutation of
/// existing entries; appending new requirements is allowed).
fn node_selector_term_has_only_additions(
    new_term: &NodeSelectorTerm,
    old_term: &NodeSelectorTerm,
) -> bool {
    let old_me = old_term.match_expressions.as_deref().unwrap_or(&[]);
    let old_mf = old_term.match_fields.as_deref().unwrap_or(&[]);
    let new_me = new_term.match_expressions.as_deref().unwrap_or(&[]);
    let new_mf = new_term.match_fields.as_deref().unwrap_or(&[]);

    if old_me.is_empty() && old_mf.is_empty() && (!new_me.is_empty() || !new_mf.is_empty()) {
        return false;
    }
    if !old_me.is_empty() {
        if new_me.len() < old_me.len() {
            return false;
        }
        if new_me[..old_me.len()] != old_me[..] {
            return false;
        }
    }
    if !old_mf.is_empty() {
        if new_mf.len() < old_mf.len() {
            return false;
        }
        if new_mf[..old_mf.len()] != old_mf[..] {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(key: &str) -> Toleration {
        Toleration {
            key: Some(key.to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: None,
            toleration_seconds: None,
        }
    }

    fn g(name: &str) -> PodSchedulingGate {
        PodSchedulingGate {
            name: name.to_string(),
        }
    }

    #[test]
    fn tolerations_additions_only_allows_add() {
        let p = Path::new("spec").child("tolerations");
        let old = vec![t("a")];
        let new = vec![t("a"), t("b")];
        assert!(validate_only_added_tolerations(&old, &new, &p).is_empty());
    }

    #[test]
    fn tolerations_additions_only_rejects_remove() {
        let p = Path::new("spec").child("tolerations");
        let old = vec![t("a"), t("b")];
        let new = vec![t("a")];
        let errs = validate_only_added_tolerations(&old, &new, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0]
            .to_string()
            .contains("existing tolerations may not be modified or removed"));
    }

    #[test]
    fn gates_deletions_only_allows_remove() {
        let p = Path::new("spec").child("schedulingGates");
        let old = vec![g("a"), g("b")];
        let new = vec![g("a")];
        assert!(validate_only_deleted_scheduling_gates(&old, &new, &p).is_empty());
    }

    #[test]
    fn gates_deletions_only_rejects_add() {
        let p = Path::new("spec").child("schedulingGates");
        let old = vec![g("a")];
        let new = vec![g("a"), g("b")];
        let errs = validate_only_deleted_scheduling_gates(&old, &new, &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].to_string().contains("only deletion is allowed"));
    }

    #[test]
    fn tgps_unchanged_is_allowed() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        assert!(validate_termination_grace_period_immutable(Some(30), Some(30), &p).is_empty());
        assert!(validate_termination_grace_period_immutable(None, None, &p).is_empty());
    }

    #[test]
    fn tgps_negative_to_one_is_allowed() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        assert!(validate_termination_grace_period_immutable(Some(-5), Some(1), &p).is_empty());
    }

    #[test]
    fn tgps_arbitrary_change_is_rejected() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        let errs = validate_termination_grace_period_immutable(Some(30), Some(60), &p);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].to_string().contains("field is immutable"));
    }

    #[test]
    fn tgps_positive_to_nil_rejected() {
        let p = Path::new("spec").child("terminationGracePeriodSeconds");
        let errs = validate_termination_grace_period_immutable(Some(30), None, &p);
        assert_eq!(errs.len(), 1);
    }
}
