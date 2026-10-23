//! PodCertificateRequest cleaner: garbage-collects PodCertificateRequests
//! older than 15 minutes (the upstream doc comment says 30; the constructor
//! call passes 15).
//!
//! Ported from `pkg/controller/certificates/cleaner/pcrcleaner.go`
//! (release-1.35) and launched like
//! `newPodCertificateRequestCleanerController`
//! (`cmd/kube-controller-manager/app/certificates.go:232-253`): gated on the
//! `PodCertificateRequest` feature, threshold 15m, polling interval 5m, ONE
//! worker; `wait.UntilWithContext(ctx, c.worker, pollingInterval)` lists
//! every PCR immediately, then once per interval.
//!
//! Deviations: lists come from storage per pass rather than an informer
//! lister (same seam as the sibling controllers), and the delete's
//! `Preconditions{UID}` is emulated with a live re-read + UID compare because
//! the `Storage` trait has no precondition delete (same approach as
//! `legacy_serviceaccount_token_cleaner.rs`).

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

/// `threshold` passed at certificates.go:248.
pub const THRESHOLD: ChronoDuration = ChronoDuration::minutes(15);
/// `pollingInterval` passed at certificates.go:249.
pub const POLLING_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// `PCRCleanerController` (pcrcleaner.go:40-46).
pub struct PcrCleanerController<S: Storage> {
    storage: Arc<S>,
    threshold: ChronoDuration,
}

impl<S: Storage + 'static> PcrCleanerController<S> {
    /// `NewPCRCleanerController` (pcrcleaner.go:49-63) with the production
    /// threshold.
    pub fn new(storage: Arc<S>) -> Self {
        Self::with_threshold(storage, THRESHOLD)
    }

    pub fn with_threshold(storage: Arc<S>, threshold: ChronoDuration) -> Self {
        Self { storage, threshold }
    }

    /// `Run` with `workers == 1` (certificates.go:251).
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting PodCertificateRequest cleaner controller");
        loop {
            self.worker(Utc::now()).await;
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    }

    /// `worker` (pcrcleaner.go:75-86). `now` stands in for `c.clock.Now()`.
    pub async fn worker(&self, now: DateTime<Utc>) {
        let pcrs: Vec<PodCertificateRequest> = match self
            .storage
            .list(&build_prefix("podcertificaterequests", None))
            .await
        {
            Ok(p) => p,
            Err(e) => {
                error!("Unable to list PodCertificateRequests: {e}");
                return;
            }
        };
        for pcr in &pcrs {
            if let Err(e) = self.handle(pcr, now).await {
                error!(
                    "Error while attempting to clean PodCertificateRequest {}/{}: {e}",
                    pcr.metadata.namespace.as_deref().unwrap_or(""),
                    pcr.metadata.name
                );
            }
        }
    }

    /// `handle` (pcrcleaner.go:88-108).
    pub async fn handle(&self, pcr: &PodCertificateRequest, now: DateTime<Utc>) -> Result<()> {
        // `c.clock.Now().Before(creationTimestamp.Add(threshold))` (:89). A
        // zero creationTimestamp is year 1 upstream, so it is always past.
        if let Some(created) = pcr.metadata.creation_timestamp {
            if now < created + self.threshold {
                return Ok(());
            }
        }

        let ns = pcr.metadata.namespace.as_deref();
        let key = build_key("podcertificaterequests", ns, &pcr.metadata.name);

        // `Preconditions{UID: pcr.UID}` (:93-97), emulated: a live object
        // with another UID is a recreation that must be spared; a missing
        // one is IsNotFound -> nil (:100-102).
        match self.storage.get::<PodCertificateRequest>(&key).await {
            Ok(live) if live.metadata.uid == pcr.metadata.uid => {}
            Ok(_) => return Ok(()),
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "unable to delete PodCertificateRequest \"{}/{}\": {e}",
                    ns.unwrap_or(""),
                    pcr.metadata.name
                ))
            }
        }
        match self.storage.delete(&key).await {
            Ok(()) | Err(rusternetes_common::Error::NotFound(_)) => Ok(()),
            Err(e) => Err(anyhow::anyhow!(
                "unable to delete PodCertificateRequest \"{}/{}\": {e}",
                ns.unwrap_or(""),
                pcr.metadata.name
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::podcertificaterequest::PodCertificateRequestStatus;
    use rusternetes_common::types::{Condition, ObjectMeta};
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
