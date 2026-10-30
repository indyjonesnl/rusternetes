//! CSR cleaner: garbage-collects old CertificateSigningRequests.
//!
//! Ported from `pkg/controller/certificates/cleaner/cleaner.go` (release-1.35)
//! and launched like `newCertificateSigningRequestCleanerController`
//! (`cmd/kube-controller-manager/app/certificates.go:217-230`): ONE worker,
//! `wait.UntilWithContext(ctx, ccc.worker, pollingInterval)` - list every CSR
//! immediately, then once per `pollingInterval` (1h).
//!
//! Deviation: lists come from storage per pass rather than an informer lister
//! (same seam as the sibling controllers).

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusternetes_common::resources::{
    CertificateSigningRequest, CertificateSigningRequestCondition,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

/// `pollingInterval` (cleaner.go:44): list every CSR once per hour.
pub const POLLING_INTERVAL: Duration = Duration::from_secs(3600);
/// `approvedExpiration` (cleaner.go:47).
const APPROVED_EXPIRATION: ChronoDuration = ChronoDuration::hours(1);
/// `deniedExpiration` (cleaner.go:48).
const DENIED_EXPIRATION: ChronoDuration = ChronoDuration::hours(1);
/// `pendingExpiration` (cleaner.go:49).
const PENDING_EXPIRATION: ChronoDuration = ChronoDuration::hours(24);

/// `CSRCleanerController` (cleaner.go:60-63).
pub struct CsrCleanerController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> CsrCleanerController<S> {
    /// `NewCSRCleanerController` (cleaner.go:66-74).
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// `Run` with `workers == 1` (certificates.go:228): `wait.UntilWithContext`
    /// runs the worker first, then sleeps `pollingInterval`.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting CSR cleaner controller");
        loop {
            self.worker(Utc::now()).await;
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    }

    /// `worker` (cleaner.go:95-107). `now` stands in for `time.Now()`.
    pub async fn worker(&self, now: DateTime<Utc>) {
        let csrs: Vec<CertificateSigningRequest> = match self
            .storage
            .list(&build_prefix("certificatesigningrequests", None))
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!("Unable to list CSRs: {e}");
                return;
            }
        };
        for csr in &csrs {
            if let Err(e) = self.handle(csr, now).await {
                error!(
                    "Error while attempting to clean CSR {}: {e}",
                    csr.metadata.name
                );
            }
        }
    }

    /// `handle` (cleaner.go:109-121).
    pub async fn handle(&self, csr: &CertificateSigningRequest, now: DateTime<Utc>) -> Result<()> {
        if is_issued_past_deadline(csr, now)
            || is_denied_past_deadline(csr, now)
            || is_failed_past_deadline(csr, now)
            || is_pending_past_deadline(csr, now)
            || is_issued_expired(csr, now)
            || is_approved_unissued_past_deadline(csr, now)
        {
            let key = build_key("certificatesigningrequests", None, &csr.metadata.name);
            self.storage.delete(&key).await.map_err(|e| {
                anyhow::anyhow!("unable to delete CSR {:?}: {e}", csr.metadata.name)
            })?;
        }
        Ok(())
    }
}

fn conditions(csr: &CertificateSigningRequest) -> &[CertificateSigningRequestCondition] {
    csr.status
        .as_ref()
        .and_then(|s| s.conditions.as_deref())
        .unwrap_or(&[])
}

fn parse_time(t: &Option<String>) -> Option<DateTime<Utc>> {
    t.as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))
}

/// `isOlderThan` (cleaner.go:208-210): `t` is non-zero and older than `d`.
fn is_older_than(t: Option<DateTime<Utc>>, d: ChronoDuration, now: DateTime<Utc>) -> bool {
    matches!(t, Some(t) if now - t > d)
}

/// `isIssued` (cleaner.go:216-218): a certificate is present.
fn is_issued(csr: &CertificateSigningRequest) -> bool {
    csr.status
        .as_ref()
        .and_then(|s| s.certificate.as_deref())
        .is_some_and(|c| !c.is_empty())
}

/// `isExpired` (cleaner.go:222-239): decode the PEM block, parse the
/// certificate, and compare its `NotAfter` to now. Any failure answers false.
/// `status.certificate` holds PEM text in this codebase; the base64 wire form
/// is accepted too.
fn is_expired(csr: &CertificateSigningRequest, now: DateTime<Utc>) -> bool {
    let Some(cert) = csr
        .status
        .as_ref()
        .and_then(|s| s.certificate.as_deref())
        .filter(|c| !c.is_empty())
    else {
        return false;
    };
    let pem_bytes = if cert.contains("-----BEGIN") {
        cert.as_bytes().to_vec()
    } else {
        use base64::Engine as _;
        match base64::engine::general_purpose::STANDARD.decode(cert) {
            Ok(b) => b,
            Err(_) => return false,
        }
    };
    let Ok((_, block)) = x509_parser::pem::parse_x509_pem(&pem_bytes) else {
        return false;
    };
    let Ok((_, parsed)) = x509_parser::parse_x509_certificate(&block.contents) else {
        return false;
    };
    now.timestamp() > parsed.validity().not_after.timestamp()
}

/// `isIssuedExpired` (cleaner.go:123-133).
fn is_issued_expired(csr: &CertificateSigningRequest, now: DateTime<Utc>) -> bool {
    conditions(csr)
        .iter()
        .any(|c| c.type_ == "Approved" && is_issued(csr) && is_expired(csr, now))
}

/// `isPendingPastDeadline` (cleaner.go:135-145): no conditions at all.
fn is_pending_past_deadline(csr: &CertificateSigningRequest, now: DateTime<Utc>) -> bool {
    conditions(csr).is_empty()
        && is_older_than(csr.metadata.creation_timestamp, PENDING_EXPIRATION, now)
}

fn condition_past(
    csr: &CertificateSigningRequest,
    type_: &str,
    d: ChronoDuration,
    now: DateTime<Utc>,
) -> bool {
    conditions(csr)
        .iter()
        .any(|c| c.type_ == type_ && is_older_than(parse_time(&c.last_update_time), d, now))
}

/// `isDeniedPastDeadline` (cleaner.go:147-157).
fn is_denied_past_deadline(csr: &CertificateSigningRequest, now: DateTime<Utc>) -> bool {
    condition_past(csr, "Denied", DENIED_EXPIRATION, now)
}

/// `isFailedPastDeadline` (cleaner.go:159-169): uses `deniedExpiration`.
fn is_failed_past_deadline(csr: &CertificateSigningRequest, now: DateTime<Utc>) -> bool {
    condition_past(csr, "Failed", DENIED_EXPIRATION, now)
}

/// `isIssuedPastDeadline` (cleaner.go:171-183).
fn is_issued_past_deadline(csr: &CertificateSigningRequest, now: DateTime<Utc>) -> bool {
    is_issued(csr) && condition_past(csr, "Approved", APPROVED_EXPIRATION, now)
}

/// `isApprovedUnissuedPastDeadline` (cleaner.go:185-198): uses
/// `pendingExpiration`.
fn is_approved_unissued_past_deadline(csr: &CertificateSigningRequest, now: DateTime<Utc>) -> bool {
    !is_issued(csr) && condition_past(csr, "Approved", PENDING_EXPIRATION, now)
}
