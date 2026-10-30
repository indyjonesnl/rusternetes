//! Unit tests moved from `src/controllers/csi_attacher.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use futures::StreamExt;
    pub use rusternetes_common::csi_translation::CsiTranslator;
    pub use rusternetes_common::resources::csi::{
        CSINode, VolumeAttachment, VolumeAttachmentStatus, VolumeError,
    };
    pub use rusternetes_common::resources::volume::{
        CSIVolumeSource, PersistentVolumeMode, PersistentVolumeSpec,
    };
    pub use rusternetes_common::resources::{PersistentVolume, Secret};
    pub use rusternetes_controller_manager::controllers::csi_provisioner::to_csi_access_mode;
    pub use rusternetes_controller_manager::controllers::pvc_protection::update_or_reap;
    pub use rusternetes_csi::controller_client::{
        ControllerError, CsiControllerClient, DriverCapabilities,
    };
    pub use rusternetes_csi::proto::controller_service_capability::rpc::Type as ControllerRpc;
    pub use rusternetes_csi::proto::plugin_capability::service::Type as PluginService;
    pub use rusternetes_csi::proto::volume_capability::{
        AccessMode, AccessType, BlockVolume, MountVolume,
    };
    pub use rusternetes_csi::proto::{
        ControllerPublishVolumeRequest, ControllerUnpublishVolumeRequest, VolumeCapability,
    };
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::{Duration, Instant};
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::csi_attacher::*;

    /// `TestSanitizeDriverName` (`util_test.go`).
    #[test]
    fn driver_names_sanitize_like_upstream() {
        assert_eq!(sanitize_driver_name("csi/example.com"), "csi-example-com");
        assert_eq!(sanitize_driver_name("a."), "a-X");
        assert_eq!(
            finalizer_name("test.csi.example.com"),
            "external-attacher/test-csi-example-com"
        );
    }

    /// `--reconcile-sync` (`main.go:76`) sets the ReconcileVA period.
    #[tokio::test]
    async fn reconcile_sync_flag_sets_next_reconcile() {
        let a = CsiAttacher::new(
            Arc::new(rusternetes_storage::memory::MemoryStorage::new()),
            "d.example.com",
            CsiControllerClient::with_endpoint("/nonexistent.sock"),
        )
        .with_reconcile_sync(Duration::from_secs(7))
        .with_max_entries(5);
        let mut next = Instant::now();
        let before = Instant::now();
        a.reconcile_va_if_due(&mut next).await;
        assert!(next >= before + Duration::from_secs(7));
        assert!(next < before + Duration::from_secs(30));
        assert_eq!(a.max_entries, 5);
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        // 1s, 2s, 4s ... capped at 5m (`ItemExponentialFailureRateLimiter`).
        let d = |n: u32| {
            RETRY_INTERVAL_START
                .saturating_mul(2u32.saturating_pow(n))
                .min(RETRY_INTERVAL_MAX)
        };
        assert_eq!(d(0), Duration::from_secs(1));
        assert_eq!(d(3), Duration::from_secs(8));
        assert_eq!(d(20), RETRY_INTERVAL_MAX);
    }
}
