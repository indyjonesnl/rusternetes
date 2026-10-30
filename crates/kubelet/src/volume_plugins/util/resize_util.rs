//! Port of the PVC status writers in `pkg/volume/util/resize_util.go` that the
//! kubelet's node expansion uses: `MarkNodeExpansionInProgress`,
//! `MarkNodeExpansionInfeasible`, `MarkNodeExpansionFailedCondition`,
//! `MarkNodeExpansionFinishedWithRecovery`, `MarkFSResizeFinished`,
//! `PatchPVCStatus`, `MergeResizeConditionOnPVC` and
//! `mergeStorageResourceStatus`.
//!
//! The controller-side writers (`MarkResizeInProgressWithResizer`,
//! `MarkForFSResize`, `UpdatePVSize`, ...) belong to the external resizer and
//! are not ported.
//!
//! **Transport deviation.** Upstream sends a strategic-merge PATCH to the
//! `status` subresource through a clientset. The kubelet here talks to
//! [`Storage`], whose status verbs are the equivalents:
//! - `PatchPVCStatus` (`addResourceVersionCheck=true`, `resize_util.go:325`)
//!   puts the old object's `resourceVersion` into the patch so it fails on a
//!   stale read -> [`Storage::update_status_cas`].
//! - the `createPVCPatch(..., false)` writers (`:263`, `:289`) carry no
//!   precondition -> [`Storage::update_status`], which grafts only `status`
//!   onto the live object.
//!
//! Both replace the whole `status` rather than send a delta, so a concurrent
//! write to a status field this code does not touch is overwritten in the
//! unchecked case (upstream's delta would preserve it).

use anyhow::{anyhow, Result};
use chrono::SecondsFormat;
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::volume::{
    PersistentVolumeClaim, PersistentVolumeClaimCondition, PersistentVolumeClaimStatus,
};
use rusternetes_storage::{build_key, Storage};

/// `v1.ResourceStorage`.
pub const RESOURCE_STORAGE: &str = "storage";

/// `v1.PersistentVolumeClaimNodeResizePending` (core/v1 `types.go`).
pub const CLAIM_NODE_RESIZE_PENDING: &str = "NodeResizePending";
/// `v1.PersistentVolumeClaimNodeResizeInProgress`.
pub const CLAIM_NODE_RESIZE_IN_PROGRESS: &str = "NodeResizeInProgress";
/// `v1.PersistentVolumeClaimNodeResizeInfeasible`.
pub const CLAIM_NODE_RESIZE_INFEASIBLE: &str = "NodeResizeInfeasible";

/// `v1.PersistentVolumeClaimFileSystemResizePending`.
pub const COND_FILE_SYSTEM_RESIZE_PENDING: &str = "FileSystemResizePending";
/// `v1.PersistentVolumeClaimResizing`.
pub const COND_RESIZING: &str = "Resizing";
/// `v1.PersistentVolumeClaimControllerResizeError`.
pub const COND_CONTROLLER_RESIZE_ERROR: &str = "ControllerResizeError";
/// `v1.PersistentVolumeClaimNodeResizeError`.
pub const COND_NODE_RESIZE_ERROR: &str = "NodeResizeError";

/// `knownResizeConditions` (`resize_util.go:41-46`).
fn is_known_resize_condition(condition_type: &str) -> bool {
    matches!(
        condition_type,
        COND_FILE_SYSTEM_RESIZE_PENDING
            | COND_RESIZING
            | COND_CONTROLLER_RESIZE_ERROR
            | COND_NODE_RESIZE_ERROR
    )
}

/// Port of `MergeResizeConditionOnPVC` (`resize_util.go:391-431`): update the
/// PVC with the requested resize conditions, leaving other conditions alone.
///
/// Upstream appends the unprocessed conditions in Go map-iteration order
/// (random); this appends them in the order given, which is one of those
/// orders.
pub fn merge_resize_condition_on_pvc(
    pvc: &mut PersistentVolumeClaim,
    resize_conditions: Vec<PersistentVolumeClaimCondition>,
    keep_old_resize_conditions: bool,
) {
    // `resizeConditionMap`: type -> (condition, processed).
    let mut pending: Vec<(PersistentVolumeClaimCondition, bool)> = Vec::new();
    for condition in resize_conditions {
        // A later condition of the same type replaces the earlier one, as a
        // map insert does.
        match pending
            .iter_mut()
            .find(|(c, _)| c.r#type == condition.r#type)
        {
            Some(slot) => *slot = (condition, false),
            None => pending.push((condition, false)),
        }
    }

    let old_conditions = pvc
        .status
        .as_mut()
        .and_then(|s| s.conditions.take())
        .unwrap_or_default();
    let mut new_conditions = Vec::new();
    for condition in old_conditions {
        // If Condition is of not resize type, we keep it.
        if !is_known_resize_condition(&condition.r#type) {
            new_conditions.push(condition);
            continue;
        }

        if let Some((new_condition, processed)) = pending
            .iter_mut()
            .find(|(c, _)| c.r#type == condition.r#type)
        {
            if new_condition.status != condition.status {
                new_conditions.push(new_condition.clone());
            } else {
                new_conditions.push(condition);
            }
            *processed = true;
        } else if keep_old_resize_conditions {
            // keep the old resize conditions present in the existing
            // pvc.Status.Conditions field.
            new_conditions.push(condition);
        }
    }

    // append all unprocessed conditions
    for (condition, processed) in pending {
        if !processed {
            new_conditions.push(condition);
        }
    }
    // `pvc.Status.Conditions = newConditions`: Go's empty non-nil slice
    // marshals as `omitempty`-absent, so an empty list is `None` here.
    status_mut(pvc).conditions = (!new_conditions.is_empty()).then_some(new_conditions);
}

/// Port of `mergeStorageResourceStatus` (`resize_util.go:433-444`).
pub fn merge_storage_resource_status(pvc: &mut PersistentVolumeClaim, status: &str) {
    status_mut(pvc)
        .allocated_resource_statuses
        .get_or_insert_with(Default::default)
        .insert(RESOURCE_STORAGE.to_string(), status.to_string());
}

fn status_mut(pvc: &mut PersistentVolumeClaim) -> &mut PersistentVolumeClaimStatus {
    pvc.status.get_or_insert_with(Default::default)
}

/// `delete(allocatedResourceStatusMap, v1.ResourceStorage)` followed by
/// `nil`-ing an emptied map (`resize_util.go:226-232`, `:247-253`).
fn clear_storage_resource_status(pvc: &mut PersistentVolumeClaim) {
    let status = status_mut(pvc);
    if let Some(map) = status.allocated_resource_statuses.as_mut() {
        map.remove(RESOURCE_STORAGE);
        if map.is_empty() {
            status.allocated_resource_statuses = None;
        }
    }
}

/// `metav1.Now()` as the RFC 3339 string a condition's `lastTransitionTime`
/// serialises to.
fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn pvc_key(pvc: &PersistentVolumeClaim) -> String {
    build_key(
        "persistentvolumeclaims",
        pvc.metadata.namespace.as_deref(),
        &pvc.metadata.name,
    )
}

/// Port of `PatchPVCStatus` (`resize_util.go:325-340`): write `new_pvc`'s
/// status, failing if `old_pvc` has since been modified.
pub async fn patch_pvc_status<S: Storage + ?Sized>(
    old_pvc: &PersistentVolumeClaim,
    new_pvc: &PersistentVolumeClaim,
    client: &S,
) -> Result<PersistentVolumeClaim> {
    // The precondition is `old_pvc`'s resourceVersion (`addResourceVersionCheck`).
    let mut checked = new_pvc.clone();
    checked.metadata.resource_version = old_pvc.metadata.resource_version.clone();
    client
        .update_status_cas(&pvc_key(old_pvc), &checked)
        .await
        .map_err(|e| {
            anyhow!(
                "patchPVCStatus failed to patch PVC {:?}: {e}",
                old_pvc.metadata.name
            )
        })
}

/// The unconditional `createPVCPatch(.., false)` + `Patch(..., "status")` tail
/// shared by `MarkNodeExpansionInfeasible` and `MarkNodeExpansionFailedCondition`
/// (`resize_util.go:278-286`, `:304-312`).
async fn patch_pvc_status_unchecked<S: Storage + ?Sized>(
    new_pvc: &PersistentVolumeClaim,
    client: &S,
) -> Result<PersistentVolumeClaim> {
    client
        .update_status(&pvc_key(new_pvc), new_pvc)
        .await
        .map_err(|e| {
            anyhow!(
                "patchPVCStatus failed to patch PVC {:?}: {e}",
                new_pvc.metadata.name
            )
        })
}

/// Port of `MarkNodeExpansionInProgress` (`resize_util.go:314-321`): mark pvc
/// expansion in progress on the node.
pub async fn mark_node_expansion_in_progress<S: Storage + ?Sized>(
    pvc: &PersistentVolumeClaim,
    client: &S,
) -> Result<PersistentVolumeClaim> {
    let mut new_pvc = pvc.clone();
    merge_storage_resource_status(&mut new_pvc, CLAIM_NODE_RESIZE_IN_PROGRESS);
    patch_pvc_status(pvc, &new_pvc, client).await
}

fn node_resize_error_condition(err: &anyhow::Error) -> PersistentVolumeClaimCondition {
    PersistentVolumeClaimCondition {
        r#type: COND_NODE_RESIZE_ERROR.to_string(),
        status: "True".to_string(),
        last_probe_time: None,
        last_transition_time: Some(now()),
        reason: None,
        message: Some(format!("failed to expand pvc with {err}")),
    }
}

/// Port of `MarkNodeExpansionInfeasible` (`resize_util.go:263-287`): mark a PVC
/// for node expansion as failed. Kubelet should not retry expansion of volumes
/// which are in failed state.
pub async fn mark_node_expansion_infeasible<S: Storage + ?Sized>(
    pvc: &PersistentVolumeClaim,
    client: &S,
    err: &anyhow::Error,
) -> Result<PersistentVolumeClaim> {
    let mut new_pvc = pvc.clone();
    merge_storage_resource_status(&mut new_pvc, CLAIM_NODE_RESIZE_INFEASIBLE);
    merge_resize_condition_on_pvc(
        &mut new_pvc,
        vec![node_resize_error_condition(err)],
        true, /* keepOldResizeConditions */
    );
    patch_pvc_status_unchecked(&new_pvc, client).await
}

/// Port of `MarkNodeExpansionFailedCondition` (`resize_util.go:289-312`).
pub async fn mark_node_expansion_failed_condition<S: Storage + ?Sized>(
    pvc: &PersistentVolumeClaim,
    client: &S,
    err: &anyhow::Error,
) -> Result<PersistentVolumeClaim> {
    let mut new_pvc = pvc.clone();
    merge_resize_condition_on_pvc(
        &mut new_pvc,
        vec![node_resize_error_condition(err)],
        true, /* keepOldResizeConditions */
    );
    patch_pvc_status_unchecked(&new_pvc, client).await
}

/// Port of `MarkNodeExpansionFinishedWithRecovery` (`resize_util.go:240-261`).
pub async fn mark_node_expansion_finished_with_recovery<S: Storage + ?Sized>(
    pvc: &PersistentVolumeClaim,
    new_size: Quantity,
    client: &S,
) -> Result<PersistentVolumeClaim> {
    let mut new_pvc = pvc.clone();
    status_mut(&mut new_pvc)
        .capacity
        .get_or_insert_with(Default::default)
        .insert(RESOURCE_STORAGE.to_string(), new_size.to_string());
    clear_storage_resource_status(&mut new_pvc);
    merge_resize_condition_on_pvc(&mut new_pvc, vec![], false /* keepOld */);
    patch_pvc_status(pvc, &new_pvc, client).await
}

/// Port of `MarkFSResizeFinished` (`resize_util.go:216-238`): mark file system
/// resizing as done.
///
/// `RecoverVolumeExpansionFailure` is GA and `LockToDefault` in 1.35
/// (`pkg/features/kube_features.go:1683`:
/// `{Version: 1.34, Default: true, PreRelease: GA, LockToDefault: true}`), so
/// the `if utilfeature...Enabled(...)` branch at `:224` is always taken and is
/// unconditional here.
pub async fn mark_fs_resize_finished<S: Storage + ?Sized>(
    pvc: &PersistentVolumeClaim,
    new_size: Quantity,
    client: &S,
) -> Result<PersistentVolumeClaim> {
    // Same body as `MarkNodeExpansionFinishedWithRecovery` once the gate is on.
    mark_node_expansion_finished_with_recovery(pvc, new_size, client).await
}
