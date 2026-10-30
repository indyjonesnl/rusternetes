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
//! `NewStructural` and `ValidateStructural` (`apiserver/schema`) over each
//! version's `openAPIV3Schema` are in
//! `rusternetes_common::validation::structural`.

use std::collections::HashMap;
use std::sync::Mutex;

use rusternetes_common::resources::{CustomResourceDefinition, CustomResourceDefinitionCondition};
use rusternetes_common::validation::crd::{
    api_approval_state, is_protected_community_group, ApiApprovalState,
    KUBE_API_APPROVED_ANNOTATION,
};
use rusternetes_common::validation::field::{Error, Path};
use rusternetes_common::validation::structural::{new_structural, validate_structural};

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

    // :102-122. A version with no `openAPIV3Schema` is skipped upstream
    // (`v.Schema == nil || v.Schema.OpenAPIV3Schema == nil`).
    for (i, v) in crd.spec.versions.iter().enumerate() {
        let Some(schema) = v.schema.as_ref().and_then(|s| s.open_apiv3_schema.as_ref()) else {
            continue;
        };
        let s = match new_structural(schema) {
            Ok(s) => s,
            Err(e) => {
                return Some(condition(
                    NON_STRUCTURAL_SCHEMA,
                    "Unknown",
                    "StructuralError",
                    &format!(
                        "failed to check validation schema for version {}: {e}",
                        v.name
                    ),
                ));
            }
        };
        let pth = Path::new("spec")
            .child("versions")
            .index(i)
            .child("schema")
            .child("openAPIV3Schema");
        all_errs.extend(validate_structural(&pth, &s).iter().map(|e| e.to_string()));
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
