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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::CertificateSigningRequestStatus;
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;

    // Verbatim from cleaner_test.go:34-65.
    const EXPIRED_CERT: &str = "-----BEGIN CERTIFICATE-----
MIICIzCCAc2gAwIBAgIJAOApTlMFDOUnMA0GCSqGSIb3DQEBCwUAMG0xCzAJBgNV
BAYTAkdCMQ8wDQYDVQQIDAZMb25kb24xDzANBgNVBAcMBkxvbmRvbjEYMBYGA1UE
CgwPR2xvYmFsIFNlY3VyaXR5MRYwFAYDVQQLDA1JVCBEZXBhcnRtZW50MQowCAYD
VQQDDAEqMB4XDTE3MTAwNDIwNDgzOFoXDTE3MTAwMzIwNDgzOFowbTELMAkGA1UE
BhMCR0IxDzANBgNVBAgMBkxvbmRvbjEPMA0GA1UEBwwGTG9uZG9uMRgwFgYDVQQK
DA9HbG9iYWwgU2VjdXJpdHkxFjAUBgNVBAsMDUlUIERlcGFydG1lbnQxCjAIBgNV
BAMMASowXDANBgkqhkiG9w0BAQEFAANLADBIAkEA3Gt0KmuRXDxvqZUiX/xqAn1t
nZZX98guZvPPyxnQtV3YpA274W0sX3jL+U71Ya+3kaUstXQa4YrWBUHiXoqJnwID
AQABo1AwTjAdBgNVHQ4EFgQUtDsIpzHoUiLsO88f9fm+G0tYSPowHwYDVR0jBBgw
FoAUtDsIpzHoUiLsO88f9fm+G0tYSPowDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0B
AQsFAANBADfrlKof5CUkxGlX9Rifxv/mWOk8ZuTLWfMYQH2nycBHnmOxy6sR+87W
/Mb/uRz0TXVnGVcbu5E8Bz7e/Far1ZI=
-----END CERTIFICATE-----";
    const UNEXPIRED_CERT: &str = "-----BEGIN CERTIFICATE-----
MIICJTCCAc+gAwIBAgIJAIRjMToP+pPEMA0GCSqGSIb3DQEBCwUAMG0xCzAJBgNV
BAYTAkdCMQ8wDQYDVQQIDAZMb25kb24xDzANBgNVBAcMBkxvbmRvbjEYMBYGA1UE
CgwPR2xvYmFsIFNlY3VyaXR5MRYwFAYDVQQLDA1JVCBEZXBhcnRtZW50MQowCAYD
VQQDDAEqMCAXDTE3MTAwNDIwNDUyNFoYDzIxMTcwOTEwMjA0NTI0WjBtMQswCQYD
VQQGEwJHQjEPMA0GA1UECAwGTG9uZG9uMQ8wDQYDVQQHDAZMb25kb24xGDAWBgNV
BAoMD0dsb2JhbCBTZWN1cml0eTEWMBQGA1UECwwNSVQgRGVwYXJ0bWVudDEKMAgG
A1UEAwwBKjBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC7j9BAV5HqIJGi6r4G4YeI
ioHxH2loVu8IOKSK7xVs3v/EjR/eXbQzM+jZU7duyZqn6YjySZNLl0K0MfHCHBgX
AgMBAAGjUDBOMB0GA1UdDgQWBBTwxV40NFSNW7lpQ3eUWX7Mxs03yzAfBgNVHSME
GDAWgBTwxV40NFSNW7lpQ3eUWX7Mxs03yzAMBgNVHRMEBTADAQH/MA0GCSqGSIb3
DQEBCwUAA0EALDi9OidANHflx8q+w3p0rJo9gpA6cJcFpEtP2Lv4kvOtB1f6L0jY
MLd7MVm4cS/MNcx4L7l23UC3Hx4+nAxvIg==
-----END CERTIFICATE-----";

    fn cond(
        type_: &str,
        age: ChronoDuration,
        now: DateTime<Utc>,
    ) -> CertificateSigningRequestCondition {
        CertificateSigningRequestCondition {
            type_: type_.to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_update_time: Some((now - age).to_rfc3339()),
            last_transition_time: None,
        }
    }

    fn csr(
        name: &str,
        created: DateTime<Utc>,
        cert: Option<&str>,
        conds: Vec<CertificateSigningRequestCondition>,
    ) -> CertificateSigningRequest {
        let mut meta = ObjectMeta::new(name);
        meta.creation_timestamp = Some(created);
        CertificateSigningRequest {
            api_version: "certificates.k8s.io/v1".to_string(),
            kind: "CertificateSigningRequest".to_string(),
            metadata: meta,
            spec: Default::default(),
            status: Some(CertificateSigningRequestStatus {
                conditions: Some(conds),
                certificate: cert.map(str::to_string),
            }),
        }
    }

    type Case = (
        &'static str,
        ChronoDuration,
        Option<&'static str>,
        Vec<(&'static str, ChronoDuration)>,
        bool,
    );

    /// TestCleanerWithApprovedExpiredCSR (cleaner_test.go:67-268).
    #[tokio::test]
    async fn cleaner_with_approved_expired_csr() {
        let now = Utc::now();
        let m = ChronoDuration::minutes;
        let h = ChronoDuration::hours;
        let cases: Vec<Case> = vec![
            (
                "no delete approved not passed deadline",
                m(1),
                Some(UNEXPIRED_CERT),
                vec![("Approved", m(50))],
                false,
            ),
            (
                "no delete approved passed deadline not issued",
                m(1),
                None,
                vec![("Approved", m(50))],
                false,
            ),
            (
                "delete approved passed deadline",
                m(1),
                Some(UNEXPIRED_CERT),
                vec![("Approved", h(2))],
                true,
            ),
            (
                "no delete denied not passed deadline",
                m(1),
                None,
                vec![("Denied", m(50))],
                false,
            ),
            (
                "delete denied passed deadline",
                m(1),
                None,
                vec![("Denied", h(2))],
                true,
            ),
            (
                "no delete failed not passed deadline",
                m(1),
                None,
                vec![("Approved", h(2)), ("Failed", m(50))],
                false,
            ),
            (
                "delete failed passed deadline",
                m(1),
                None,
                vec![("Approved", h(2)), ("Failed", h(2))],
                true,
            ),
            (
                "no delete pending not passed deadline",
                h(5),
                None,
                vec![],
                false,
            ),
            ("delete pending passed deadline", h(25), None, vec![], true),
            (
                "delete approved unissued past deadline",
                m(1),
                None,
                vec![("Approved", h(25))],
                true,
            ),
            (
                "no delete approved unissued not past deadline",
                m(1),
                None,
                vec![("Approved", h(5))],
                false,
            ),
            (
                "no delete approved not passed deadline unexpired",
                m(1),
                Some(UNEXPIRED_CERT),
                vec![("Approved", m(50))],
                false,
            ),
            (
                "delete approved not passed deadline expired",
                m(1),
                Some(EXPIRED_CERT),
                vec![("Approved", m(50))],
                true,
            ),
            (
                "delete approved passed deadline unparseable",
                m(1),
                Some("garbage"),
                vec![("Approved", h(2))],
                true,
            ),
        ];
        for (name, created, cert, conds, want_delete) in cases {
            let storage = Arc::new(MemoryStorage::new());
            let c = csr(
                "fake-csr",
                now - created,
                cert,
                conds.iter().map(|(t, a)| cond(t, *a, now)).collect(),
            );
            let key = "/registry/certificatesigningrequests/fake-csr";
            storage.create(key, &c).await.unwrap();
            CsrCleanerController::new(storage.clone())
                .handle(&c, now)
                .await
                .unwrap();
            let still: Result<CertificateSigningRequest, _> = storage.get(key).await;
            assert_eq!(still.is_err(), want_delete, "{name}");
        }
    }

    /// `worker` lists every CSR and cleans each (cleaner.go:95-107).
    #[tokio::test]
    async fn worker_cleans_only_matching_csrs() {
        let now = Utc::now();
        let storage = Arc::new(MemoryStorage::new());
        for (name, age) in [
            ("old", ChronoDuration::hours(25)),
            ("new", ChronoDuration::hours(1)),
        ] {
            let mut c = csr(name, now - age, None, vec![]);
            c.status = None;
            storage
                .create(&format!("/registry/certificatesigningrequests/{name}"), &c)
                .await
                .unwrap();
        }
        CsrCleanerController::new(storage.clone()).worker(now).await;
        let left: Vec<CertificateSigningRequest> = storage
            .list("/registry/certificatesigningrequests/")
            .await
            .unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].metadata.name, "new");
    }
}
