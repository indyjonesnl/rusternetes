//! Unit tests moved from `src/data_plane.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_client::api_status::{is_already_exists, is_conflict, is_not_found};
    pub use rusternetes_client::events::ClientEventRecorder;
    pub use rusternetes_client::http::{ApiClient, GetError};
    pub use rusternetes_client::reflector::Reflector;
    pub use rusternetes_common::resources::{
        CSIDriver, CSINode, EventType, Node, PersistentVolume, PersistentVolumeClaim, Pod,
        PodDisruptionBudget, PriorityClass, StorageClass, VolumeAttachment,
    };
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_scheduler::node_volume_limits::NodeVolumeLimitsSnapshot;
    pub use rusternetes_scheduler::volume_binding::VolumeSnapshot;
    pub use rusternetes_storage::{build_key, Storage};
    pub use serde_json::json;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::data_plane::*;

    #[test]
    fn binding_body_has_target_name_and_metadata_name() {
        let b = binding_body("web-1", "node-2");
        assert_eq!(b["target"]["name"], "node-2");
        assert_eq!(b["metadata"]["name"], "web-1");
        assert_eq!(b["kind"], "Binding");
    }

    #[test]
    fn api_err_conflict_maps_to_conflict() {
        let e = anyhow::Error::new(rusternetes_client::api_status::ApiStatusError {
            code: 409,
            reason: "Conflict".into(),
            message: "Error from server (Conflict): pod already assigned".into(),
            retry_after_seconds: None,
        });
        assert!(matches!(api_err_to_common(e), Error::Conflict(_)));
    }

    /// Typed `ApiStatusError`s must classify by reason/code (apimachinery
    /// `IsNotFound`/`IsConflict`/`IsAlreadyExists`, errors.go:529-557), not by
    /// the rendered message.
    #[test]
    fn api_err_classifies_by_typed_status() {
        use rusternetes_client::api_status::ApiStatusError;
        let typed = |code: u16, reason: &str, message: &str| {
            anyhow::Error::new(ApiStatusError {
                code,
                reason: reason.into(),
                message: message.into(),
                retry_after_seconds: None,
            })
        };
        assert!(matches!(
            api_err_to_common(typed(409, "", "request failed with status 409")),
            Error::Conflict(_)
        ));
        assert!(matches!(
            api_err_to_common(typed(404, "", "request failed with status 404")),
            Error::NotFound(_)
        ));
        assert!(matches!(
            api_err_to_common(typed(409, "AlreadyExists", "exists")),
            Error::Conflict(_)
        ));
        // A message that mentions "(Conflict)" on a non-conflict status.
        assert!(matches!(
            api_err_to_common(typed(500, "InternalError", "x (Conflict) y")),
            Error::Internal(_)
        ));
    }
}
