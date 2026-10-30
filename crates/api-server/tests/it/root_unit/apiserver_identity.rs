//! Unit tests moved from `src/apiserver_identity.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::resources::{Lease, LeaseSpec};
    pub use rusternetes_common::Error;
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use sha2::{Digest, Sha256};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{error, info};
}

/// Ported from `gc_controller_test.go` `Test_Controller` and
/// `lease/controller_test.go` `TestNewNodeLease`.
mod tests {
    use super::__parent::*;
    use chrono::TimeZone;
    use rusternetes_api_server::apiserver_identity::*;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 7, 12, 0, 0).unwrap()
    }

    fn lease(name: &str, identity: &str, renew: Option<DateTime<Utc>>, dur: Option<i32>) -> Lease {
        let mut l = Lease::new(name, LEASE_NAMESPACE);
        l.spec = Some(LeaseSpec {
            holder_identity: Some(name.to_string()),
            lease_duration_seconds: dur,
            renew_time: renew,
            ..Default::default()
        });
        label_heartbeat(&mut l, identity, "h");
        l
    }

    async fn gc_case(l: Lease) -> bool {
        let s = StorageBackend::new_memory();
        let key = lease_key(&l.metadata.name);
        s.create(&key, &l).await.unwrap();
        gc_expired_leases(&s, "kube-apiserver", t0()).await;
        s.get::<Lease>(&key).await.is_err()
    }

    fn ago(m: i64) -> Option<DateTime<Utc>> {
        Some(t0() - chrono::Duration::minutes(m))
    }

    #[tokio::test]
    async fn gc_keeps_a_live_lease() {
        assert!(!gc_case(lease("a", "kube-apiserver", ago(1), Some(3600))).await);
    }

    #[tokio::test]
    async fn gc_ignores_other_identities() {
        assert!(!gc_case(lease("a", "kube-controller-manager", ago(1), Some(10))).await);
    }

    #[tokio::test]
    async fn gc_deletes_expired_nil_renew_and_nil_duration() {
        assert!(gc_case(lease("a", "kube-apiserver", ago(1), Some(10))).await);
        assert!(gc_case(lease("a", "kube-apiserver", None, Some(10))).await);
        assert!(gc_case(lease("a", "kube-apiserver", ago(1), None)).await);
    }

    #[test]
    fn apiserver_id_shape_and_stability() {
        let id = apiserver_id("host-1");
        assert!(id.starts_with("apiserver-"));
        // 16 bytes -> 26 base32 chars, lowercase, unpadded
        assert_eq!(id.len(), "apiserver-".len() + 26);
        assert!(id["apiserver-".len()..]
            .chars()
            .all(|c| c.is_ascii_lowercase() || ('2'..='7').contains(&c)));
        assert_eq!(id, apiserver_id("host-1"));
        assert_ne!(id, apiserver_id("host-2"));
    }

    #[test]
    fn base32_matches_rfc4648_vectors() {
        // RFC 4648 section 10, lowercased, unpadded
        assert_eq!(base32_nopad_lower(b"f"), "my");
        assert_eq!(base32_nopad_lower(b"fo"), "mzxq");
        assert_eq!(base32_nopad_lower(b"foobar"), "mzxw6ytboi");
    }

    #[tokio::test]
    async fn sync_creates_then_renews_with_labels() {
        let s = Arc::new(StorageBackend::new_memory());
        let mut c = LeaseController::new(
            s.clone(),
            "apiserver-x".into(),
            "apiserver-x_u".into(),
            "h".into(),
        );
        c.sync(t0).await;
        let l: Lease = s.get(&lease_key("apiserver-x")).await.unwrap();
        let spec = l.spec.as_ref().unwrap();
        assert_eq!(spec.holder_identity.as_deref(), Some("apiserver-x_u"));
        assert_eq!(spec.lease_duration_seconds, Some(3600));
        let labels = l.metadata.labels.as_ref().unwrap();
        assert_eq!(labels[IDENTITY_LEASE_COMPONENT_LABEL_KEY], "kube-apiserver");
        assert_eq!(labels[LABEL_HOSTNAME], "h");
        let later = t0() + chrono::Duration::seconds(10);
        c.sync(|| later).await;
        let l: Lease = s.get(&lease_key("apiserver-x")).await.unwrap();
        assert_eq!(l.spec.unwrap().renew_time, Some(later));
    }

    #[tokio::test]
    async fn start_deletes_a_stale_lease() {
        let s = Arc::new(StorageBackend::new_memory());
        let stale = lease("apiserver-x", "kube-apiserver", ago(5), Some(3600));
        s.create(&lease_key("apiserver-x"), &stale).await.unwrap();
        let c = LeaseController::new(s.clone(), "apiserver-x".into(), "h_u".into(), "h".into());
        c.delete_old_lease().await;
        assert!(s.get::<Lease>(&lease_key("apiserver-x")).await.is_err());
    }

    #[tokio::test]
    async fn spawning_registers_both_hooks_and_they_finish() {
        let s = Arc::new(StorageBackend::new_memory());
        spawn_identity_hooks(s);
        for _ in 0..50 {
            let g = rusternetes_api_server::post_start_hooks::global();
            if g.check(IDENTITY_LEASE_CONTROLLER_HOOK) == Some(Ok(()))
                && g.check(IDENTITY_LEASE_GC_HOOK) == Some(Ok(()))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("identity hooks never finished");
    }
}
