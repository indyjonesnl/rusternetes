//! The two apiextensions controllers that only maintain an informational
//! condition on a CustomResourceDefinition, as pure functions over the object
//! plus the per-process "last seen" memo each keeps. Ports of
//!
//! - `pkg/controller/nonstructuralschema/nonstructuralschema_controller.go`
//!   (`calculateCondition`, `sync`, `deleteCustomResourceDefinition`): the
//!   `NonStructuralSchema` condition,
//! - `pkg/controller/apiapproval/apiapproval_controller.go`
//!   (`calculateCondition`, `sync`): the `KubernetesAPIApprovalPolicyConformant`
//!   condition,
//!
//! under `staging/src/k8s.io/apiextensions-apiserver/` (release-1.35). Like
//! the naming/establishing/finalizer controllers (see `controllers.rs`) they
//! run inline after the write that would have woken the informer; the logic,
//! and the memo that stops two api-servers fighting over a message, is
//! upstream's.
//!
//! Not modelled: `schema.ValidateStructural` (and `NewStructural`) over each
//! version's `openAPIV3Schema`, which feeds the `Violations` message of the
//! `NonStructuralSchema` condition. Rusternetes has no structural-schema
//! type yet; only the `spec.preserveUnknownFields` violation is produced.

use std::collections::HashMap;
use std::sync::Mutex;

use rusternetes_common::resources::{CustomResourceDefinition, CustomResourceDefinitionCondition};
use rusternetes_common::validation::crd::{
    api_approval_state, is_protected_community_group, ApiApprovalState,
    KUBE_API_APPROVED_ANNOTATION,
};
use rusternetes_common::validation::field::{Error, Path};

use super::controllers::{aggregate, condition, find_crd_condition, set_crd_condition};

/// `apiextensionsv1.NonStructuralSchema`.
pub const NON_STRUCTURAL_SCHEMA: &str = "NonStructuralSchema";
/// `apiextensionsv1.KubernetesAPIApprovalPolicyConformant`.
pub const KUBERNETES_API_APPROVAL_POLICY_CONFORMANT: &str = "KubernetesAPIApprovalPolicyConformant";

/// `RemoveCRDCondition` (`apiextensions/helpers.go`): drop every condition of
/// that type.
pub fn remove_crd_condition(crd: &mut CustomResourceDefinition, condition_type: &str) {
    if let Some(conditions) = crd.status.as_mut().and_then(|s| s.conditions.as_mut()) {
        conditions.retain(|c| c.type_ != condition_type);
    }
}

fn same_details(
    old: &CustomResourceDefinitionCondition,
    new: &CustomResourceDefinitionCondition,
) -> bool {
    old.status == new.status
        && old.reason.as_deref().unwrap_or("") == new.reason.as_deref().unwrap_or("")
        && old.message.as_deref().unwrap_or("") == new.message.as_deref().unwrap_or("")
}

/// `nonstructuralschema.calculateCondition`
/// (nonstructuralschema_controller.go:88-133): `None` when the CRD has no
/// violations (the condition is then removed).
pub fn calculate_non_structural_condition(
    crd: &CustomResourceDefinition,
) -> Option<CustomResourceDefinitionCondition> {
    let mut all_errs = Vec::new();

    // :96-100
    if crd.spec.preserve_unknown_fields.unwrap_or(false) {
        all_errs.push(
            Error::invalid(
                &Path::new("spec").child("preserveUnknownFields"),
                true,
                "must be false",
            )
            .to_string(),
        );
    }

    if all_errs.is_empty() {
        return None;
    }
    // :128-130
    Some(condition(
        NON_STRUCTURAL_SCHEMA,
        "True",
        "Violations",
        &aggregate(&all_errs),
    ))
}

/// `apiapproval.calculateCondition` (apiapproval_controller.go:85-128): `None`
/// outside the protected community groups.
pub fn calculate_api_approval_condition(
    crd: &CustomResourceDefinition,
) -> Option<CustomResourceDefinitionCondition> {
    if !is_protected_community_group(&crd.spec.group) {
        return None;
    }
    let (state, reason) = api_approval_state(crd.metadata.annotations.as_ref());
    let (status, why) = match state {
        ApiApprovalState::Invalid => ("False", "InvalidAnnotation"),
        ApiApprovalState::Missing => ("False", "MissingAnnotation"),
        ApiApprovalState::Approved => ("True", "ApprovedAnnotation"),
        ApiApprovalState::Bypassed => ("False", "UnapprovedAnnotation"),
    };
    Some(condition(
        KUBERNETES_API_APPROVAL_POLICY_CONFORMANT,
        status,
        why,
        &reason,
    ))
}

/// The state of the two controllers: what each last wrote per CRD name, "to
/// avoid two different version of the apiextensions-apiservers in HA to fight
/// for the right message" (nonstructuralschema_controller.go:55-58,
/// apiapproval_controller.go:51-54).
#[derive(Default)]
pub struct ConditionControllers {
    last_seen_generation: Mutex<HashMap<String, i64>>,
    last_seen_protected_annotation: Mutex<HashMap<String, String>>,
}

impl ConditionControllers {
    /// `ConditionController.sync` (nonstructuralschema_controller.go:135-188),
    /// up to the write: the CRD to write, or `None` when there is nothing to do.
    pub fn sync_non_structural(
        &self,
        crd: &CustomResourceDefinition,
    ) -> Option<CustomResourceDefinition> {
        // avoid repeated calculation for the same generation (:144-150)
        let generation = crd.metadata.generation.unwrap_or(0);
        if let Some(last) = self
            .last_seen_generation
            .lock()
            .unwrap()
            .get(&crd.metadata.name)
        {
            if generation <= *last {
                return None;
            }
        }

        // check old condition (:152-161)
        let cond = calculate_non_structural_condition(crd);
        let old = find_crd_condition(crd, NON_STRUCTURAL_SCHEMA);
        match (&cond, old) {
            (None, None) => return None,
            (Some(c), Some(o)) if same_details(o, c) => return None,
            _ => {}
        }

        // update condition (:163-170)
        let mut out = crd.clone();
        match cond {
            None => remove_crd_condition(&mut out, NON_STRUCTURAL_SCHEMA),
            Some(c) => set_crd_condition(&mut out, c),
        }
        Some(out)
    }

    /// The memo `sync` stores after a successful `UpdateStatus` (:181-185).
    pub fn record_non_structural(&self, written: &CustomResourceDefinition) {
        self.last_seen_generation.lock().unwrap().insert(
            written.metadata.name.clone(),
            written.metadata.generation.unwrap_or(0),
        );
    }

    /// `KubernetesAPIApprovalPolicyConformantConditionController.sync`
    /// (apiapproval_controller.go:130-173), up to the write.
    pub fn sync_api_approval(
        &self,
        crd: &CustomResourceDefinition,
    ) -> Option<CustomResourceDefinition> {
        // avoid repeated calculation for the same annotation (:139-146)
        let annotation = protection_annotation(crd);
        if let Some(last) = self
            .last_seen_protected_annotation
            .lock()
            .unwrap()
            .get(&crd.metadata.name)
        {
            if annotation == *last {
                return None;
            }
        }

        // because group is immutable, if we have no condition now, we have no
        // need to remove a condition (:149-153).
        let cond = calculate_api_approval_condition(crd)?;
        let old = find_crd_condition(crd, KUBERNETES_API_APPROVAL_POLICY_CONFORMANT);

        // don't attempt a write if all the condition details are the same (:156-160)
        if old.is_some_and(|o| same_details(o, &cond)) {
            return None;
        }

        let mut out = crd.clone();
        set_crd_condition(&mut out, cond);
        Some(out)
    }

    /// The memo `sync` stores after a successful `UpdateStatus` (:175-179).
    pub fn record_api_approval(&self, written: &CustomResourceDefinition) {
        self.last_seen_protected_annotation.lock().unwrap().insert(
            written.metadata.name.clone(),
            protection_annotation(written),
        );
    }

    /// `deleteCustomResourceDefinition`
    /// (nonstructuralschema_controller.go:263-281): a deleted CRD forgets its
    /// generation, so a recreation starts afresh. (The apiapproval controller
    /// keeps its annotation memo, as upstream.)
    pub fn forget(&self, name: &str) {
        self.last_seen_generation.lock().unwrap().remove(name);
    }
}

fn protection_annotation(crd: &CustomResourceDefinition) -> String {
    crd.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(KUBE_API_APPROVED_ANNOTATION))
        .cloned()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::CustomResourceDefinitionStatus;

    fn crd(group: &str, annotation: Option<&str>, preserve: bool) -> CustomResourceDefinition {
        let mut c: CustomResourceDefinition = serde_json::from_value(serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "foo", "generation": 1},
            "spec": {"group": group, "scope": "Namespaced", "versions": [],
                     "names": {"plural": "foos", "kind": "Foo"}}
        }))
        .unwrap();
        c.spec.preserve_unknown_fields = Some(preserve);
        if let Some(a) = annotation {
            c.metadata.annotations =
                Some([(KUBE_API_APPROVED_ANNOTATION.to_string(), a.to_string())].into());
        }
        c
    }

    /// `TestCalculateCondition` (apiapproval_controller_test.go:26-103).
    #[test]
    fn api_approval_calculate_condition() {
        let cases: [(&str, &str, &str, &str, &str); 5] = [
            ("other.io", "", "", "", ""),
            (
                "sigs.k8s.io",
                "",
                "False",
                "MissingAnnotation",
                r#"protected groups must have approval annotation "api-approved.kubernetes.io", see https://github.com/kubernetes/enhancements/pull/1111"#,
            ),
            (
                "sigs.k8s.io",
                "bad value",
                "False",
                "InvalidAnnotation",
                r#"protected groups must have approval annotation "api-approved.kubernetes.io" with either a URL or a reason starting with "unapproved", see https://github.com/kubernetes/enhancements/pull/1111"#,
            ),
            (
                "sigs.k8s.io",
                "https://github.com/kubernetes/kubernetes/pull/79724",
                "True",
                "ApprovedAnnotation",
                "approved in https://github.com/kubernetes/kubernetes/pull/79724",
            ),
            (
                "sigs.k8s.io",
                "unapproved for reasons",
                "False",
                "UnapprovedAnnotation",
                r#"not approved: "unapproved for reasons""#,
            ),
        ];
        for (group, annotation, status, reason, message) in cases {
            let got = calculate_api_approval_condition(&crd(group, Some(annotation), false));
            if status.is_empty() {
                assert!(got.is_none(), "{group}: {got:?}");
                continue;
            }
            let got = got.unwrap_or_else(|| panic!("missing condition for {annotation:?}"));
            assert_eq!(got.type_, KUBERNETES_API_APPROVAL_POLICY_CONFORMANT);
            assert_eq!(got.status, status, "{annotation:?}");
            assert_eq!(got.reason.as_deref(), Some(reason), "{annotation:?}");
            assert_eq!(got.message.as_deref(), Some(message), "{annotation:?}");
        }
    }

    /// `Test_calculateCondition` (nonstructuralschema_controller_test.go:28-66).
    #[test]
    fn non_structural_calculate_condition() {
        assert_eq!(
            calculate_non_structural_condition(&crd("a.io", None, false)),
            None
        );
        let got = calculate_non_structural_condition(&crd("a.io", None, true)).unwrap();
        assert_eq!(got.type_, NON_STRUCTURAL_SCHEMA);
        assert_eq!(got.status, "True");
        assert_eq!(got.reason.as_deref(), Some("Violations"));
        assert_eq!(
            got.message.as_deref(),
            Some("spec.preserveUnknownFields: Invalid value: true: must be false")
        );
    }

    #[test]
    fn non_structural_sync_sets_then_skips_same_generation_then_removes() {
        let c = ConditionControllers::default();
        let bad = crd("a.io", None, true);
        let written = c.sync_non_structural(&bad).expect("condition is written");
        assert!(find_crd_condition(&written, NON_STRUCTURAL_SCHEMA).is_some());
        // Same details already on the object: no write (:159-161).
        assert!(c.sync_non_structural(&written).is_none());

        // The memo skips the same generation even if the object differs (:148).
        c.record_non_structural(&written);
        let mut fixed = written.clone();
        fixed.spec.preserve_unknown_fields = Some(false);
        assert!(c.sync_non_structural(&fixed).is_none());

        // A newer generation recalculates, and the stale condition is removed (:165-166).
        fixed.metadata.generation = Some(2);
        let out = c.sync_non_structural(&fixed).expect("condition removed");
        assert!(find_crd_condition(&out, NON_STRUCTURAL_SCHEMA).is_none());

        // A deleted CRD forgets its generation (:278-280).
        c.forget("foo");
        let mut again = written.clone();
        again.metadata.generation = Some(1);
        again.spec.preserve_unknown_fields = Some(false);
        assert!(c.sync_non_structural(&again).is_some());
    }

    #[test]
    fn non_structural_sync_without_condition_or_violation_is_a_noop() {
        let c = ConditionControllers::default();
        let mut ok = crd("a.io", None, false);
        assert!(c.sync_non_structural(&ok).is_none());
        ok.status = Some(CustomResourceDefinitionStatus::default());
        assert!(c.sync_non_structural(&ok).is_none());
    }

    #[test]
    fn api_approval_sync_writes_once_and_never_removes() {
        let c = ConditionControllers::default();
        assert!(c.sync_api_approval(&crd("other.io", None, false)).is_none());

        let missing = crd("sigs.k8s.io", None, false);
        let written = c.sync_api_approval(&missing).expect("condition is written");
        let cond = find_crd_condition(&written, KUBERNETES_API_APPROVAL_POLICY_CONFORMANT).unwrap();
        assert_eq!(cond.reason.as_deref(), Some("MissingAnnotation"));
        assert!(c.sync_api_approval(&written).is_none());

        // The memo skips an unchanged annotation (:144-146) ...
        c.record_api_approval(&written);
        assert!(c.sync_api_approval(&missing).is_none());
        // ... and a changed one rewrites the condition.
        let mut approved = written.clone();
        approved.metadata.annotations = Some(
            [(
                KUBE_API_APPROVED_ANNOTATION.to_string(),
                "https://github.com/kubernetes/kubernetes/pull/1".to_string(),
            )]
            .into(),
        );
        let out = c.sync_api_approval(&approved).expect("condition updated");
        let cond = find_crd_condition(&out, KUBERNETES_API_APPROVAL_POLICY_CONFORMANT).unwrap();
        assert_eq!(
            (cond.status.as_str(), cond.reason.as_deref()),
            ("True", Some("ApprovedAnnotation"))
        );
    }
}
