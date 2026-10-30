//! Unit tests moved from `src/controllers/pcr_cleaner.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::{DateTime, Duration as ChronoDuration, Utc};
    pub use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::podcertificaterequest::PodCertificateRequestStatus;
    use rusternetes_common::types::{Condition, ObjectMeta};
    use rusternetes_controller_manager::controllers::pcr_cleaner::*;
    use rusternetes_storage::MemoryStorage;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn pcr(created: &str, denied: bool) -> PodCertificateRequest {
        let mut p = PodCertificateRequest {
            metadata: ObjectMeta {
                name: "bar".into(),
                namespace: Some("foo".into()),
                uid: "pcr-uid".into(),
                creation_timestamp: Some(ts(created)),
                ..Default::default()
            },
            ..Default::default()
        };
        p.spec.signer_name = "foo.com/abc".into();
        p.spec.pod_name = "pod-1".into();
        if denied {
            p.status = PodCertificateRequestStatus {
                conditions: vec![Condition {
                    condition_type: "Denied".into(),
                    status: "True".into(),
                    reason: Some("Foo".into()),
                    message: Some("abc".into()),
                    observed_generation: None,
                    last_transition_time: None,
                }],
                ..Default::default()
            };
        }
        p
    }

    /// pcrcleaner_test.go TestPCRCleaner: now = 00:30:00Z, threshold 15m.
    /// Returns whether the PCR survived.
    async fn run_case(p: PodCertificateRequest) -> bool {
        let storage = Arc::new(MemoryStorage::new());
        let key = build_key("podcertificaterequests", Some("foo"), "bar");
        storage.create(&key, &p).await.unwrap();
        let c = PcrCleanerController::with_threshold(storage.clone(), ChronoDuration::minutes(15));
        let now = ts("2025-01-01T00:30:00Z");
        let list: Vec<PodCertificateRequest> = storage
            .list(&build_prefix("podcertificaterequests", None))
            .await
            .unwrap();
        for p in &list {
            c.handle(p, now).await.unwrap();
        }
        storage.get::<PodCertificateRequest>(&key).await.is_ok()
    }

    #[tokio::test]
    async fn pending_within_threshold_left_alone() {
        assert!(run_case(pcr("2025-01-01T00:15:01Z", false)).await);
    }

    #[tokio::test]
    async fn pending_outside_threshold_deleted() {
        assert!(!run_case(pcr("2025-01-01T00:14:59Z", false)).await);
    }

    #[tokio::test]
    async fn terminal_within_threshold_left_alone() {
        assert!(run_case(pcr("2025-01-01T00:15:01Z", true)).await);
    }

    #[tokio::test]
    async fn terminal_outside_threshold_deleted() {
        assert!(!run_case(pcr("2025-01-01T00:14:59Z", true)).await);
    }

    /// `Preconditions{UID}` (pcrcleaner.go:93-97): a recreated object with a
    /// different UID must survive a delete aimed at the stale one.
    #[tokio::test]
    async fn uid_precondition_spares_recreated_object() {
        let storage = Arc::new(MemoryStorage::new());
        let key = build_key("podcertificaterequests", Some("foo"), "bar");
        let stale = pcr("2025-01-01T00:00:00Z", false);
        let mut fresh = pcr("2025-01-01T00:29:00Z", false);
        fresh.metadata.uid = "other-uid".into();
        storage.create(&key, &fresh).await.unwrap();
        let c = PcrCleanerController::with_threshold(storage.clone(), ChronoDuration::minutes(15));
        c.handle(&stale, ts("2025-01-01T00:30:00Z")).await.unwrap();
        assert!(storage.get::<PodCertificateRequest>(&key).await.is_ok());
    }

    /// `k8serrors.IsNotFound(err)` -> nil (pcrcleaner.go:100-102).
    #[tokio::test]
    async fn already_deleted_is_ok() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PcrCleanerController::with_threshold(storage, ChronoDuration::minutes(15));
        c.handle(
            &pcr("2025-01-01T00:00:00Z", false),
            ts("2025-01-01T00:30:00Z"),
        )
        .await
        .unwrap();
    }
}
