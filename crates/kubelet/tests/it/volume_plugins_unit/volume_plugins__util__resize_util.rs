//! Unit tests moved from `src/volume_plugins/util/resize_util.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use chrono::SecondsFormat;
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::volume::{
        PersistentVolumeClaim, PersistentVolumeClaimCondition, PersistentVolumeClaimStatus,
    };
    pub use rusternetes_storage::{build_key, Storage};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::resize_util::*;
    use rusternetes_storage::StorageBackend;
    use serde_json::json;

    pub(crate) fn pvc(status: serde_json::Value) -> PersistentVolumeClaim {
        serde_json::from_value(json!({
            "metadata": {"name": "c", "namespace": "ns"},
            "spec": {"resources": {"requests": {"storage": "2Gi"}}},
            "status": status
        }))
        .unwrap()
    }

    fn cond(t: &str, status: &str) -> PersistentVolumeClaimCondition {
        PersistentVolumeClaimCondition {
            r#type: t.to_string(),
            status: status.to_string(),
            last_probe_time: None,
            last_transition_time: None,
            reason: None,
            message: None,
        }
    }

    fn types(pvc: &PersistentVolumeClaim) -> Vec<(String, String)> {
        pvc.status
            .as_ref()
            .and_then(|s| s.conditions.clone())
            .unwrap_or_default()
            .into_iter()
            .map(|c| (c.r#type, c.status))
            .collect()
    }

    /// Non-resize conditions survive; with `keepOld=false` old resize
    /// conditions are dropped (`resize_util.go:411-421`).
    #[test]
    fn merge_drops_old_resize_conditions_unless_kept() {
        let mut p = pvc(json!({"conditions": [
            {"type": "Other", "status": "True"},
            {"type": "NodeResizeError", "status": "True"},
        ]}));
        merge_resize_condition_on_pvc(&mut p, vec![], false);
        assert_eq!(types(&p), vec![("Other".into(), "True".into())]);

        let mut p = pvc(json!({"conditions": [
            {"type": "Other", "status": "True"},
            {"type": "NodeResizeError", "status": "True"},
        ]}));
        merge_resize_condition_on_pvc(&mut p, vec![], true);
        assert_eq!(types(&p).len(), 2);
    }

    /// A same-type condition with a different status is replaced, with the
    /// same status the old one is kept (`resize_util.go:404-410`), and an
    /// unmatched new one is appended (`:424-428`).
    #[test]
    fn merge_replaces_on_status_change_and_appends_new() {
        let mut p = pvc(json!({"conditions": [
            {"type": "NodeResizeError", "status": "False", "message": "old"},
        ]}));
        merge_resize_condition_on_pvc(
            &mut p,
            vec![
                cond(COND_NODE_RESIZE_ERROR, "True"),
                cond(COND_RESIZING, "True"),
            ],
            true,
        );
        assert_eq!(
            types(&p),
            vec![
                ("NodeResizeError".into(), "True".into()),
                ("Resizing".into(), "True".into())
            ]
        );

        let mut p = pvc(json!({"conditions": [
            {"type": "NodeResizeError", "status": "True", "message": "old"},
        ]}));
        merge_resize_condition_on_pvc(&mut p, vec![cond(COND_NODE_RESIZE_ERROR, "True")], true);
        assert_eq!(
            p.status.unwrap().conditions.unwrap()[0].message.as_deref(),
            Some("old")
        );
    }

    async fn stored(pvc: &PersistentVolumeClaim) -> (StorageBackend, PersistentVolumeClaim) {
        let storage = StorageBackend::new_memory();
        let created = storage.create(&pvc_key(pvc), pvc).await.unwrap();
        (storage, created)
    }

    /// `MarkNodeExpansionInProgress` sets `allocatedResourceStatuses[storage]`.
    #[tokio::test]
    async fn in_progress_sets_allocated_resource_status() {
        let (storage, p) = stored(&pvc(json!({}))).await;
        let updated = mark_node_expansion_in_progress(&p, &storage).await.unwrap();
        assert_eq!(
            updated.status.unwrap().allocated_resource_statuses.unwrap()["storage"],
            "NodeResizeInProgress"
        );
    }

    /// `PatchPVCStatus` carries the old resourceVersion, so a stale read
    /// conflicts instead of overwriting (`resize_util.go:321-323`).
    #[tokio::test]
    async fn patch_pvc_status_fails_on_stale_resource_version() {
        let (storage, p) = stored(&pvc(json!({}))).await;
        // A concurrent writer bumps the version.
        mark_node_expansion_in_progress(&p, &storage).await.unwrap();
        assert!(mark_node_expansion_in_progress(&p, &storage).await.is_err());
    }

    /// Finishing sets capacity, clears the resize status and the resize
    /// conditions (`resize_util.go:240-261`).
    #[tokio::test]
    async fn finished_with_recovery_sets_capacity_and_clears_status() {
        let (storage, p) = stored(&pvc(json!({
            "capacity": {"storage": "1Gi"},
            "allocatedResourceStatuses": {"storage": "NodeResizeInProgress"},
            "conditions": [{"type": "NodeResizeError", "status": "True"}]
        })))
        .await;
        let updated = mark_node_expansion_finished_with_recovery(
            &p,
            Quantity::parse("2Gi").unwrap(),
            &storage,
        )
        .await
        .unwrap();
        let s = updated.status.unwrap();
        assert_eq!(s.capacity.unwrap()["storage"], "2Gi");
        assert!(s.allocated_resource_statuses.is_none());
        assert!(s.conditions.is_none());
    }

    /// `MarkNodeExpansionInfeasible` sets the infeasible status and a
    /// `NodeResizeError` condition without a version precondition, so it
    /// succeeds on a stale object (`:263-287`).
    #[tokio::test]
    async fn infeasible_sets_status_and_condition_without_precondition() {
        let (storage, p) = stored(&pvc(json!({}))).await;
        mark_node_expansion_in_progress(&p, &storage).await.unwrap(); // p is now stale
        let updated = mark_node_expansion_infeasible(&p, &storage, &anyhow!("boom"))
            .await
            .unwrap();
        let s = updated.status.unwrap();
        assert_eq!(
            s.allocated_resource_statuses.unwrap()["storage"],
            "NodeResizeInfeasible"
        );
        let c = &s.conditions.unwrap()[0];
        assert_eq!(c.r#type, "NodeResizeError");
        assert_eq!(c.message.as_deref(), Some("failed to expand pvc with boom"));
    }

    /// `MarkNodeExpansionFailedCondition` adds only the condition.
    #[tokio::test]
    async fn failed_condition_leaves_resize_status_alone() {
        let (storage, p) = stored(&pvc(json!({}))).await;
        let updated = mark_node_expansion_failed_condition(&p, &storage, &anyhow!("boom"))
            .await
            .unwrap();
        let s = updated.status.unwrap();
        assert!(s.allocated_resource_statuses.is_none());
        assert_eq!(s.conditions.unwrap().len(), 1);
    }
}
