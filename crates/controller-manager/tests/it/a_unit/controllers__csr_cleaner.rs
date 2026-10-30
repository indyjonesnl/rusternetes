//! Unit tests moved from `src/controllers/csr_cleaner.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::{DateTime, Duration as ChronoDuration, Utc};
    pub use rusternetes_common::resources::{
        CertificateSigningRequest, CertificateSigningRequestCondition,
    };
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::CertificateSigningRequestStatus;
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::csr_cleaner::*;
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
