//! `kube-apiserver-serving` ClusterTrustBundle publisher.
//!
//! Ported from `pkg/controller/certificates/clustertrustbundlepublisher/
//! publisher.go` and `metrics.go` (release-1.35) and launched like
//! `newKubeAPIServerSignerClusterTrustBundledPublisherController`
//! (`cmd/kube-controller-manager/app/certificates.go:289-353`): it keeps ONE
//! ClusterTrustBundle named `kubernetes.io:kube-apiserver-serving:<sha256[:12]>`
//! (`constructBundleName`) holding the cluster root CA for the signer
//! `kubernetes.io/kube-apiserver-serving`, and deletes every other bundle of
//! that signer. Gated on the `ClusterTrustBundle` feature gate
//! (`requiredFeatureGates`, certificates.go:293) and skipped when no root CA is
//! available (certificates.go:306-308).
//!
//! Ported 1:1: `syncClusterTrustBundle` (create when absent, update when the
//! payload differs, then delete the rest; a deletion error is returned at the
//! end so the loop retriggers), `constructBundleName`, `NewStaticCAContent`'s
//! "missing content" / "no valid certificates" rejection, and the
//! `clustertrustbundle_publisher_sync_total` / `_sync_duration_seconds`
//! metrics keyed by HTTP status code (`recordMetrics`).
//!
//! Deviations (storage seam instead of informers + workqueue):
//! - Upstream wakes on ClusterTrustBundle informer events plus the CA-content
//!   listener (the static CA never fires it) and retries via
//!   `AddRateLimited`. The `Storage` trait has no event hook, so `run` syncs
//!   every [`SYNC_PERIOD`]; `sync` is idempotent and writes only on change.
//!   A failed sync is simply retried on the next period.
//! - Upstream picks the first of v1beta1 / v1alpha1 by discovery
//!   (certificates.go:326-345). Rusternetes stores one internal
//!   ClusterTrustBundle shape (served as v1beta1), so no discovery step.
//! - The field-selector filtered informer (`spec.signerName=<signer>`) becomes
//!   a list of all bundles filtered by `spec.signerName` in memory.

use anyhow::{anyhow, Result};
use prometheus::{HistogramVec, IntCounterVec};
use rusternetes_common::resources::ClusterTrustBundle;
use rusternetes_common::Error;
use rusternetes_storage::{build_key, build_prefix, Storage};
use sha2::{Digest, Sha256};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tracing::{error, info};

/// Signer published by the controller (certificates.go:342).
pub const KUBE_APISERVER_SERVING_SIGNER_NAME: &str = "kubernetes.io/kube-apiserver-serving";
/// Re-evaluation period standing in for the informer event pokes.
pub const SYNC_PERIOD: Duration = Duration::from_secs(10);

/// `clustertrustbundlePublisher` subsystem (metrics.go:32).
static SYNC_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        prometheus::Opts::new(
            "clustertrustbundle_publisher_sync_total",
            "Number of syncs that occurred in cluster trust bundle publisher.",
        ),
        &["code"],
    )
    .expect("valid opts");
    let _ = prometheus::default_registry().register(Box::new(c.clone()));
    c
});

/// `ExponentialBuckets(0.001, 2, 15)` (metrics.go:50).
static SYNC_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    let h = HistogramVec::new(
        prometheus::HistogramOpts::new(
            "clustertrustbundle_publisher_sync_duration_seconds",
            "The time it took to sync a cluster trust bundle.",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 15).expect("valid buckets")),
        &["code"],
    )
    .expect("valid opts");
    let _ = prometheus::default_registry().register(Box::new(h.clone()));
    h
});

/// The `apierrors.APIStatus` code of a storage error (`recordMetrics`:
/// 500 when the error carries no status).
fn status_code(e: &Error) -> &'static str {
    match e {
        Error::NotFound(_) => "404",
        Error::AlreadyExists(_) | Error::Conflict(_) => "409",
        Error::BadRequest(_) => "400",
        Error::Forbidden(_) => "403",
        _ => "500",
    }
}

/// `recordMetrics` (metrics.go:57-69).
fn record_metrics(start: Instant, result: &Result<(), Error>) {
    let code = match result {
        Ok(()) => "200",
        Err(e) => status_code(e),
    };
    SYNC_LATENCY
        .with_label_values(&[code])
        .observe(start.elapsed().as_secs_f64());
    SYNC_COUNTER.with_label_values(&[code]).inc();
}

/// `constructBundleName` (publisher.go:372-377): the signer name with `/`
/// replaced by `:`, a `:`, then the first 12 bytes of the SHA-256 of the
/// bundle as lowercase hex.
pub fn construct_bundle_name(signer_name: &str, bundle: &[u8]) -> String {
    let hash = Sha256::digest(bundle);
    let hex: String = hash[..12].iter().map(|b| format!("{b:02x}")).collect();
    format!("{}:{hex}", signer_name.replace('/', ":"))
}

/// `dynamiccertificates.NewStaticCAContent` validation
/// (`newCABundleAndVerifier`, dynamic_cafile_content.go:269-286): the bundle
/// must be non-empty and hold at least one parseable certificate
/// (`cert.NewPoolFromBytes` rejects "no valid certificates").
fn validate_ca_bundle(name: &str, bundle: &[u8]) -> Result<()> {
    if bundle.is_empty() {
        return Err(anyhow!("missing content for CA bundle {name:?}"));
    }
    let blocks = pem::parse_many(bundle)
        .map_err(|e| anyhow!("error loading CA bundle for {name:?}: {e}"))?;
    let mut certs = 0;
    for b in blocks.iter().filter(|b| b.tag() == "CERTIFICATE") {
        x509_parser::parse_x509_certificate(b.contents())
            .map_err(|e| anyhow!("error loading CA bundle for {name:?}: {e}"))?;
        certs += 1;
    }
    if certs == 0 {
        return Err(anyhow!(
            "error loading CA bundle for {name:?}: data does not contain any valid RSA or ECDSA certificates"
        ));
    }
    Ok(())
}

/// `ClusterTrustBundlePublisher`.
pub struct ClusterTrustBundlePublisher<S: Storage> {
    storage: Arc<S>,
    signer_name: String,
    /// `p.ca.CurrentCABundleContent()` of a static CA provider.
    ca_bundle: String,
}

impl<S: Storage + 'static> ClusterTrustBundlePublisher<S> {
    /// `newClusterTrustBundlePublisher`: rejects an empty signer name
    /// (publisher.go:226-228) and, like the static CA provider the caller
    /// builds (certificates.go:310-313), an invalid CA bundle.
    pub fn new(
        storage: Arc<S>,
        signer_name: impl Into<String>,
        ca_bundle: impl Into<String>,
    ) -> Result<Self> {
        let signer_name = signer_name.into();
        if signer_name.is_empty() {
            return Err(anyhow!("signerName cannot be empty"));
        }
        let ca_bundle = ca_bundle.into();
        validate_ca_bundle("kube-apiserver-serving", ca_bundle.as_bytes())?;
        Ok(Self {
            storage,
            signer_name,
            ca_bundle,
        })
    }

    /// The `kubernetes.io/kube-apiserver-serving` publisher for `root_ca`
    /// (certificates.go:310-345). `Ok(None)` when no root CA is configured
    /// (certificates.go:306-308).
    pub fn for_kube_apiserver_serving(
        storage: Arc<S>,
        root_ca: Option<&str>,
    ) -> Result<Option<Self>> {
        match root_ca {
            Some(ca) if !ca.is_empty() => {
                Self::new(storage, KUBE_APISERVER_SERVING_SIGNER_NAME, ca).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// `Run`: sync immediately (`p.queue.Add("")`, publisher.go:295), then
    /// every [`SYNC_PERIOD`] in place of the informer/listener pokes.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting ClusterTrustBundle CA cert publisher controller");
        loop {
            if let Err(e) = self.sync().await {
                // processNextWorkItem: HandleError + AddRateLimited.
                error!("syncing ClusterTrustBundle failed: {e}");
            }
            tokio::time::sleep(SYNC_PERIOD).await;
        }
    }

    /// `syncClusterTrustBundle` (publisher.go:328-370).
    pub async fn sync(&self) -> Result<(), Error> {
        let start = Instant::now();
        let result = self.sync_inner().await;
        record_metrics(start, &result);
        result
    }

    async fn sync_inner(&self) -> Result<(), Error> {
        let bundle_name = construct_bundle_name(&self.signer_name, self.ca_bundle.as_bytes());
        let key = build_key("clustertrustbundles", None, &bundle_name);

        match self.storage.get::<ClusterTrustBundle>(&key).await {
            Err(Error::NotFound(_)) => {
                let ctb = ClusterTrustBundle::new(&bundle_name, &self.signer_name, &self.ca_bundle);
                self.storage.create(&key, &ctb).await?;
            }
            Ok(existing) if existing.spec.trust_bundle != self.ca_bundle => {
                // updateWithTrustBundle: DeepCopy + replace the payload.
                let mut updated = existing;
                updated.spec.trust_bundle = self.ca_bundle.clone();
                self.storage.update(&key, &updated).await?;
            }
            Ok(_) => {}
            Err(e) => return Err(e),
        }

        // The informer is filtered by `spec.signerName=<signer>`.
        let all: Vec<ClusterTrustBundle> = self
            .storage
            .list(&build_prefix("clustertrustbundles", None))
            .await?;

        // Keep the deletion error to return in the end, to retrigger the loop.
        let mut deletion_error = None;
        for b in all
            .iter()
            .filter(|b| b.spec.signer_name == self.signer_name)
        {
            if b.metadata.name == bundle_name {
                continue;
            }
            let del_key = build_key("clustertrustbundles", None, &b.metadata.name);
            match self.storage.delete(&del_key).await {
                Ok(()) | Err(Error::NotFound(_)) => {}
                Err(e) => {
                    error!(
                        "failed to remove a cluster trust bundle {}: {e}",
                        b.metadata.name
                    );
                    deletion_error = Some(e);
                }
            }
        }
        deletion_error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
