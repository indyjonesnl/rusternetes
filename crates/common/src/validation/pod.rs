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
//! - Gated-pod `nodeSelector` / `nodeAffinity` mutation rules
//!   (`validation.go:5786-5828`) — only relevant once rusternetes ships a real
//!   scheduling-gates feature. See `TODO(rusternetes)` below.
//! - ActiveDeadlineSeconds precise semantics — the api-server handler enforces
//!   these directly (see `crates/api-server/src/handlers/pod.rs::update`)
//!   because the error wording is checked by tests pinned at that layer.

use crate::resources::pod::{PodSchedulingGate, PodSpec, Toleration};
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
///
/// TODO(rusternetes): gated-pod nodeSelector/nodeAffinity mutation rules
/// (upstream validation.go:5786-5828) are deferred until rusternetes ships
/// a scheduling-gates feature. Without that path, the broad fence here is
/// strictly correct (no gated-pod mutations are permitted at all).
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
