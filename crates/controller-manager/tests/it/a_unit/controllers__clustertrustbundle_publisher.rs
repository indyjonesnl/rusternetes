//! Unit tests moved from `src/controllers/clustertrustbundle_publisher.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use prometheus::{HistogramVec, IntCounterVec};
    pub use rusternetes_common::resources::ClusterTrustBundle;
    pub use rusternetes_common::Error;
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use sha2::{Digest, Sha256};
    pub use std::sync::{Arc, LazyLock};
    pub use std::time::{Duration, Instant};
    pub use tracing::{error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::clustertrustbundle_publisher::*;
    use rusternetes_storage::memory::MemoryStorage;

    const TEST_SIGNER: &str = "test.test/testSigner";

    /// `testingCABundlleProvider` (publisher_test.go:346-367): a fresh
    /// self-signed CA, PEM-encoded.
    fn test_ca_pem() -> String {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-ca");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.self_signed(&key).unwrap().pem()
    }

    fn ctb(name: &str, signer: &str, data: &str) -> ClusterTrustBundle {
        ClusterTrustBundle::new(name, signer, data)
    }

    async fn seed(s: &MemoryStorage, objs: &[ClusterTrustBundle]) {
        for o in objs {
            s.create(&build_key("clustertrustbundles", None, &o.metadata.name), o)
                .await
                .unwrap();
        }
    }

    async fn all(s: &MemoryStorage) -> Vec<ClusterTrustBundle> {
        let mut v: Vec<ClusterTrustBundle> = s
            .list(&build_prefix("clustertrustbundles", None))
            .await
            .unwrap();
        v.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        v
    }

    async fn run_sync(
        existing: &[ClusterTrustBundle],
        ca: &str,
    ) -> (Arc<MemoryStorage>, Vec<ClusterTrustBundle>) {
        let s = Arc::new(MemoryStorage::new());
        seed(&s, existing).await;
        let p = ClusterTrustBundlePublisher::new(s.clone(), TEST_SIGNER, ca).unwrap();
        p.sync().await.unwrap();
        let after = all(&s).await;
        (s, after)
    }

    #[test]
    fn bundle_name_matches_construct_bundle_name() {
        // sha256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9c...
        assert_eq!(
            construct_bundle_name("a.b/c", b"abc"),
            "a.b:c:ba7816bf8f01cfea414140de"
        );
    }

    /// "no CTBs exist"
    #[tokio::test]
    async fn creates_when_none_exist() {
        let ca = test_ca_pem();
        let (_, after) = run_sync(&[], &ca).await;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].spec.signer_name, TEST_SIGNER);
        assert_eq!(after[0].spec.trust_bundle, ca);
        assert_eq!(
            after[0].metadata.name,
            construct_bundle_name(TEST_SIGNER, ca.as_bytes())
        );
    }

    /// "no CTBs for the current signer exist": others are left alone.
    #[tokio::test]
    async fn creates_and_leaves_other_signers() {
        let ca = test_ca_pem();
        let (_, after) = run_sync(
            &[
                ctb("nosigner", "", "somedatahere"),
                ctb("signer:one", "signer", "signerdata"),
            ],
            &ca,
        )
        .await;
        let names: Vec<_> = after.iter().map(|b| b.metadata.name.as_str()).collect();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"nosigner") && names.contains(&"signer:one"));
    }

    /// "CTB for the signer exists with different content"
    #[tokio::test]
    async fn updates_different_content() {
        let ca = test_ca_pem();
        let name = construct_bundle_name(TEST_SIGNER, ca.as_bytes());
        let (_, after) = run_sync(&[ctb(&name, TEST_SIGNER, "olddata")], &ca).await;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].spec.trust_bundle, ca);
    }

    /// "multiple CTBs for the signer": the extra one is deleted.
    #[tokio::test]
    async fn deletes_extra_bundle_of_signer() {
        let ca = test_ca_pem();
        let name = construct_bundle_name(TEST_SIGNER, ca.as_bytes());
        let (_, after) = run_sync(
            &[
                ctb(&name, TEST_SIGNER, &ca),
                ctb("test.test/testSigner:name2", TEST_SIGNER, &ca),
            ],
            &ca,
        )
        .await;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].metadata.name, name);
    }

    /// "multiple CTBs ... the one with the proper name needs changing":
    /// update the proper one AND delete the other.
    #[tokio::test]
    async fn updates_proper_and_deletes_extra() {
        let ca = test_ca_pem();
        let name = construct_bundle_name(TEST_SIGNER, ca.as_bytes());
        let (_, after) = run_sync(
            &[
                ctb(&name, TEST_SIGNER, "olddata"),
                ctb("test.test/testSigner:name2", TEST_SIGNER, &ca),
            ],
            &ca,
        )
        .await;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].metadata.name, name);
        assert_eq!(after[0].spec.trust_bundle, ca);
    }

    /// "another CTB with a different name exists for the signer": create the
    /// proper one and delete the preexisting one.
    #[tokio::test]
    async fn creates_proper_and_deletes_preexisting() {
        let ca = test_ca_pem();
        let (_, after) = run_sync(
            &[ctb("test.test/testSigner:preexisting", TEST_SIGNER, &ca)],
            &ca,
        )
        .await;
        assert_eq!(after.len(), 1);
        assert_eq!(
            after[0].metadata.name,
            construct_bundle_name(TEST_SIGNER, ca.as_bytes())
        );
    }

    /// "CTB at the correct state - noop": nothing is written (the stored
    /// resourceVersion is unchanged).
    #[tokio::test]
    async fn noop_when_correct() {
        let ca = test_ca_pem();
        let name = construct_bundle_name(TEST_SIGNER, ca.as_bytes());
        let existing = [
            ctb("nosigner", "", "somedatahere"),
            ctb("signer:one", "signer", "signerdata"),
            ctb(&name, TEST_SIGNER, &ca),
        ];
        let s = Arc::new(MemoryStorage::new());
        seed(&s, &existing).await;
        let before = all(&s).await;
        let p = ClusterTrustBundlePublisher::new(s.clone(), TEST_SIGNER, ca.as_str()).unwrap();
        p.sync().await.unwrap();
        assert_eq!(all(&s).await, before);
    }

    /// `newClusterTrustBundlePublisher`: "signerName cannot be empty".
    #[tokio::test]
    async fn rejects_empty_signer_name() {
        let s = Arc::new(MemoryStorage::new());
        let err = ClusterTrustBundlePublisher::new(s, "", test_ca_pem())
            .err()
            .unwrap();
        assert!(err.to_string().contains("signerName cannot be empty"));
    }

    /// `NewStaticCAContent` rejects empty / certificate-less content.
    #[tokio::test]
    async fn rejects_invalid_ca() {
        let s = Arc::new(MemoryStorage::new());
        assert!(ClusterTrustBundlePublisher::new(s.clone(), TEST_SIGNER, "").is_err());
        assert!(ClusterTrustBundlePublisher::new(s.clone(), TEST_SIGNER, "not pem").is_err());
        // certificates.go:306-308: no root CA -> controller skipped.
        assert!(
            ClusterTrustBundlePublisher::for_kube_apiserver_serving(s, None)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn publishes_kube_apiserver_serving_signer() {
        let ca = test_ca_pem();
        let s = Arc::new(MemoryStorage::new());
        let p = ClusterTrustBundlePublisher::for_kube_apiserver_serving(s.clone(), Some(&ca))
            .unwrap()
            .unwrap();
        p.sync().await.unwrap();
        let after = all(&s).await;
        assert_eq!(after.len(), 1);
        assert_eq!(
            after[0].spec.signer_name,
            KUBE_APISERVER_SERVING_SIGNER_NAME
        );
        assert!(after[0]
            .metadata
            .name
            .starts_with("kubernetes.io:kube-apiserver-serving:"));
    }
}
