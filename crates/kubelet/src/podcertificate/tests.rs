//! Ports of `pkg/kubelet/podcertificate/podcertificatemanager_test.go`
//! (`TestTransitionInitialToWait` :46, `TestPCRDeletedWhileWaiting` :181,
//! `TestFullFlow` :292), plus the denied / failed / metric / key-type /
//! cleanup paths upstream's state machine defines but its test file does not
//! drive.

use super::*;
use rusternetes_common::resources::{PodSpec, Volume};
use rusternetes_common::types::{Condition, ObjectMeta, TypeMeta};
use rusternetes_storage::StorageBackend;

struct FakeClock(StdMutex<DateTime<Utc>>);

impl FakeClock {
    fn new(start: DateTime<Utc>) -> Arc<Self> {
        // Storage stamps creationTimestamp with the real clock (upstream's
        // fake client leaves it zero), so the fake clock starts at real now.
        Arc::new(Self(StdMutex::new(start)))
    }
    fn step(&self, d: Duration) {
        *self.0.lock().unwrap() += chrono_dur(d);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

/// `FakeSynchronousPodManager`.
struct FakePodManager(StdMutex<Vec<Pod>>);

impl PodManager for FakePodManager {
    fn get_pod_by_uid(&self, uid: &str) -> Option<Pod> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|p| p.metadata.uid == uid)
            .cloned()
    }
    fn get_pods(&self) -> Vec<Pod> {
        self.0.lock().unwrap().clone()
    }
}

const SIGNER: &str = "foo.com/signer";
const POD_UID: &str = "11111111-2222-3333-4444-555555555555";

fn workload_pod(key_type: &str) -> Pod {
    Pod {
        type_meta: TypeMeta::default(),
        metadata: ObjectMeta {
            name: "workload".into(),
            namespace: Some("ns1".into()),
            uid: POD_UID.into(),
            ..Default::default()
        },
        spec: Some(PodSpec {
            service_account_name: Some("workload".into()),
            node_name: Some("node1".into()),
            volumes: Some(vec![serde_json::from_value::<Volume>(serde_json::json!({
                "name": "certificate",
                "projected": {"sources": [{"podCertificate": {
                    "signerName": SIGNER,
                    "keyType": key_type,
                    "credentialBundlePath": "creds.pem",
                    // Defaulting doesn't work with a fake client.
                    "maxExpirationSeconds": 86400,
                    "userAnnotations": {"test.domain/foo": "bar"},
                }}]},
            }))
            .unwrap()]),
            ..Default::default()
        }),
        status: None,
    }
}

struct Fixture {
    storage: Arc<StorageBackend>,
    clock: Arc<FakeClock>,
    pods: Arc<FakePodManager>,
    mgr: Arc<IssuingManager<StorageBackend>>,
    pod: Pod,
}

async fn fixture(key_type: &str) -> Fixture {
    let storage = Arc::new(StorageBackend::new_memory());
    let clock = FakeClock::new(Utc::now());
    let node = Node {
        type_meta: TypeMeta::default(),
        metadata: ObjectMeta {
            name: "node1".into(),
            uid: "node1-uid".into(),
            ..Default::default()
        },
        spec: None,
        status: None,
    };
    storage
        .create(&build_key("nodes", None, "node1"), &node)
        .await
        .unwrap();
    let sa = ServiceAccount {
        type_meta: TypeMeta::default(),
        metadata: ObjectMeta {
            name: "workload".into(),
            namespace: Some("ns1".into()),
            uid: "sa-uid".into(),
            ..Default::default()
        },
        secrets: None,
        image_pull_secrets: None,
        automount_service_account_token: None,
    };
    storage
        .create(&build_key("serviceaccounts", Some("ns1"), "workload"), &sa)
        .await
        .unwrap();
    let pod = workload_pod(key_type);
    let pods = Arc::new(FakePodManager(StdMutex::new(vec![pod.clone()])));
    let mgr = IssuingManager::new(
        storage.clone(),
        pods.clone(),
        Some(EventRecorder::new(storage.clone())),
        "node1",
        clock.clone(),
    );
    Fixture {
        storage,
        clock,
        pods,
        mgr,
        pod,
    }
}

fn key() -> ProjectionKey {
    ProjectionKey {
        namespace: "ns1".into(),
        pod_name: "workload".into(),
        pod_uid: POD_UID.into(),
        volume_name: "certificate".into(),
        source_index: 0,
    }
}

async fn list_pcrs(s: &StorageBackend) -> Vec<PodCertificateRequest> {
    s.list(&build_prefix(PCR_RESOURCE, Some("ns1")))
        .await
        .unwrap()
}

fn cond(ty: &str, reason: &str, message: &str) -> Condition {
    Condition {
        condition_type: ty.into(),
        status: "True".into(),
        observed_generation: None,
        last_transition_time: None,
        reason: Some(reason.into()),
        message: Some(message.into()),
    }
}

/// A stand-in for `hermeticpodcertificatesigner`: marks the PCR issued.
async fn issue(f: &Fixture, pcr: &PodCertificateRequest, chain: &str) {
    let mut p = pcr.clone();
    let now = f.clock.now();
    p.status.conditions = vec![cond(CONDITION_TYPE_ISSUED, "Issued", "")];
    p.status.certificate_chain = chain.to_string();
    p.status.not_before = Some(now);
    p.status.begin_refresh_at = Some(now + ChronoDuration::hours(12));
    p.status.not_after = Some(now + ChronoDuration::hours(24));
    f.storage
        .update(
            &build_key(PCR_RESOURCE, Some("ns1"), &pcr.metadata.name),
            &p,
        )
        .await
        .unwrap();
}

async fn set_condition(f: &Fixture, pcr: &PodCertificateRequest, c: Condition) {
    let mut p = pcr.clone();
    p.status.conditions = vec![c];
    f.storage
        .update(
            &build_key(PCR_RESOURCE, Some("ns1"), &pcr.metadata.name),
            &p,
        )
        .await
        .unwrap();
}

const CHAIN_A: &str =
    "junk before\n-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\nmore junk\n";

async fn bundle(f: &Fixture) -> Result<(Vec<u8>, Vec<u8>)> {
    f.mgr
        .get_pod_certificate_credential_bundle("ns1", "workload", POD_UID, "certificate", 0)
        .await
}

#[tokio::test]
async fn transition_initial_to_wait() {
    // TestTransitionInitialToWait (:46).
    let f = fixture("ED25519").await;
    f.mgr.handle_projection(&key()).await.unwrap();

    let pcrs = list_pcrs(&f.storage).await;
    assert_eq!(pcrs.len(), 1, "wrong number of PodCertificateRequests");
    let got = &pcrs[0];

    let mut want = PodCertificateRequest::default();
    want.spec.signer_name = SIGNER.into();
    want.spec.pod_name = "workload".into();
    want.spec.pod_uid = POD_UID.into();
    want.spec.service_account_name = "workload".into();
    want.spec.service_account_uid = "sa-uid".into();
    want.spec.node_name = "node1".into();
    want.spec.node_uid = "node1-uid".into();
    want.spec.max_expiration_seconds = Some(86400);
    want.spec.unverified_user_annotations = Some(HashMap::from([(
        "test.domain/foo".to_string(),
        "bar".to_string(),
    )]));

    // Blank the fields the test does not care about (:170-175).
    let mut got_clone = got.clone();
    got_clone.spec.pkix_public_key.clear();
    got_clone.spec.proof_of_possession.clear();
    assert_eq!(got_clone.spec, want.spec);
    assert!(got.status.conditions.is_empty());
    assert_eq!(got.metadata.namespace.as_deref(), Some("ns1"));
    assert!(got.metadata.name.starts_with("req-"));
    // Owned by the pod (:763-770).
    let owner = &got.metadata.owner_references.as_ref().unwrap()[0];
    assert_eq!(
        (
            owner.api_version.as_str(),
            owner.kind.as_str(),
            owner.name.as_str(),
            owner.uid.as_str()
        ),
        ("core/v1", "Pod", "workload", POD_UID)
    );

    // And no bundle until issued.
    let err = bundle(&f).await.unwrap_err();
    assert_eq!(err.to_string(), "credential bundle is not issued yet");
}

#[tokio::test]
async fn pcr_deleted_while_waiting() {
    // TestPCRDeletedWhileWaiting (:181).
    let f = fixture("ED25519").await;
    f.mgr.handle_projection(&key()).await.unwrap();

    let pcr = list_pcrs(&f.storage).await.remove(0);
    f.storage
        .delete(&build_key(PCR_RESOURCE, Some("ns1"), &pcr.metadata.name))
        .await
        .unwrap();

    // Before the threshold a missing PCR is treated as informer lag: an error
    // (the lister's NotFound, `:485`) that keeps the state at credStateWait.
    assert!(f.mgr.handle_projection(&key()).await.is_err());
    f.clock
        .step(ASSUME_DELETED_THRESHOLD + Duration::from_secs(5 * 60 + 60));

    // Calling handleProjection again should return an error, *not* nil panic.
    let err = f.mgr.handle_projection(&key()).await.unwrap_err();
    assert!(
        err.to_string().contains("appears to have been deleted"),
        "{err}"
    );

    // Back in credStateInitial: the next pass creates a fresh PCR.
    f.mgr.handle_projection(&key()).await.unwrap();
    assert_eq!(list_pcrs(&f.storage).await.len(), 1);
}

#[tokio::test]
async fn full_flow_issue_then_refresh() {
    // TestFullFlow (:292): issue, serve bundle, pass beginRefreshAt (incl. the
    // up-to-5-minute jitter), refresh, serve the new chain.
    let f = fixture("ED25519").await;
    f.mgr.handle_projection(&key()).await.unwrap();
    let first = list_pcrs(&f.storage).await.remove(0);

    // Pending: still not issued, stays in wait.
    f.mgr.handle_projection(&key()).await.unwrap();
    assert!(bundle(&f).await.is_err());

    issue(&f, &first, CHAIN_A).await;
    f.mgr.handle_projection(&key()).await.unwrap();

    let (priv_key, chain) = bundle(&f).await.unwrap();
    assert!(String::from_utf8_lossy(&priv_key).starts_with("-----BEGIN PRIVATE KEY-----\n"));
    // cleanCertificateChain dropped the inter-block junk.
    assert_eq!(
        String::from_utf8(chain.clone()).unwrap(),
        "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
    );

    // Not yet time to refresh: handleProjection is a no-op (:543-546).
    f.clock.step(Duration::from_secs(60));
    f.mgr.handle_projection(&key()).await.unwrap();
    assert_eq!(list_pcrs(&f.storage).await.len(), 1);

    // Fast-forward past beginRefreshAt + 5min jitter.
    f.clock.step(Duration::from_secs(12 * 3600 + 6 * 60));
    f.mgr.handle_projection(&key()).await.unwrap();
    let pcrs = list_pcrs(&f.storage).await;
    assert_eq!(pcrs.len(), 2, "refresh must create a second PCR");
    let second = pcrs
        .iter()
        .find(|p| p.metadata.name != first.metadata.name)
        .unwrap();

    // While the refresh is pending the OLD bundle keeps being served (:254-256).
    let (_, still) = bundle(&f).await.unwrap();
    assert_eq!(still, chain);

    issue(
        &f,
        second,
        "-----BEGIN CERTIFICATE-----\nBBBB\n-----END CERTIFICATE-----\n",
    )
    .await;
    f.mgr.handle_projection(&key()).await.unwrap();
    let (new_priv, new_chain) = bundle(&f).await.unwrap();
    assert_eq!(
        String::from_utf8(new_chain).unwrap(),
        "-----BEGIN CERTIFICATE-----\nBBBB\n-----END CERTIFICATE-----\n"
    );
    // The refresh used a NEW key (:657-659 uses refreshPrivateKey).
    assert_ne!(new_priv, priv_key);
}

#[tokio::test]
async fn refresh_pcr_deleted_returns_to_fresh() {
    // `:611-629`.
    let f = fixture("ED25519").await;
    f.mgr.handle_projection(&key()).await.unwrap();
    let first = list_pcrs(&f.storage).await.remove(0);
    issue(&f, &first, CHAIN_A).await;
    f.mgr.handle_projection(&key()).await.unwrap();
    f.clock.step(Duration::from_secs(12 * 3600 + 6 * 60));
    f.mgr.handle_projection(&key()).await.unwrap();
    let second = list_pcrs(&f.storage)
        .await
        .into_iter()
        .find(|p| p.metadata.name != first.metadata.name)
        .unwrap();
    f.storage
        .delete(&build_key(PCR_RESOURCE, Some("ns1"), &second.metadata.name))
        .await
        .unwrap();
    f.clock
        .step(ASSUME_DELETED_THRESHOLD + Duration::from_secs(5 * 60 + 60));
    let err = f.mgr.handle_projection(&key()).await.unwrap_err();
    assert!(
        err.to_string().contains("appears to have been deleted"),
        "{err}"
    );
    // Back in credStateFresh (still serving), the next pass creates another PCR.
    assert!(bundle(&f).await.is_ok());
    f.mgr.handle_projection(&key()).await.unwrap();
    assert_eq!(
        list_pcrs(&f.storage)
            .await
            .iter()
            .filter(|p| p.metadata.name != first.metadata.name)
            .count(),
        1
    );
}

#[tokio::test]
async fn denied_and_failed_are_permanent_and_warn() {
    for (ty, want) in [
        (
            CONDITION_TYPE_DENIED,
            "was permanently denied: reason=\"Why\" message=\"Because\"",
        ),
        (
            CONDITION_TYPE_FAILED,
            "was permanently failed: reason=\"Why\" message=\"Because\"",
        ),
    ] {
        let f = fixture("ED25519").await;
        f.mgr.handle_projection(&key()).await.unwrap();
        let pcr = list_pcrs(&f.storage).await.remove(0);
        set_condition(&f, &pcr, cond(ty, "Why", "Because")).await;
        f.mgr.handle_projection(&key()).await.unwrap();

        let err = bundle(&f).await.unwrap_err();
        assert!(err.to_string().contains(want), "{err}");

        // Terminal: further passes neither error nor create PCRs (:529-537).
        f.mgr.handle_projection(&key()).await.unwrap();
        assert_eq!(list_pcrs(&f.storage).await.len(), 1);

        let events: Vec<rusternetes_common::resources::Event> =
            f.storage.list("/registry/events/").await.unwrap();
        assert!(
            events
                .iter()
                .any(|e| e.reason == ty && e.message.contains("Because")),
            "missing {ty} event: {events:?}"
        );
    }
}

#[tokio::test]
async fn overdue_and_expired_events_fire_once() {
    let f = fixture("ED25519").await;
    f.mgr.handle_projection(&key()).await.unwrap();
    let first = list_pcrs(&f.storage).await.remove(0);
    issue(&f, &first, CHAIN_A).await;
    f.mgr.handle_projection(&key()).await.unwrap();

    // Far past notAfter: the refresh PCR is created and both events fire.
    f.clock.step(Duration::from_secs(30 * 3600));
    f.mgr.handle_projection(&key()).await.unwrap();
    // A second pass in WaitRefresh must not repeat the events (:674, :681).
    f.mgr.handle_projection(&key()).await.unwrap();

    let events: Vec<rusternetes_common::resources::Event> =
        f.storage.list("/registry/events/").await.unwrap();
    let count = |r: &str| events.iter().filter(|e| e.reason == r).count();
    assert_eq!(count("CertificateOverdueForRefresh"), 1);
    assert_eq!(count("CertificateExpired"), 1);
    assert!(events.iter().all(|e| e.count == 1), "an event repeated");

    let report = f.mgr.metric_report();
    assert_eq!(
        report.pod_certificate_states,
        BTreeMap::from([(
            SignerAndState {
                signer_name: SIGNER.into(),
                state: "expired".into()
            },
            1
        )])
    );
}

#[tokio::test]
async fn metric_report_states() {
    let f = fixture("ED25519").await;
    assert_eq!(
        f.mgr.metric_report(),
        MetricReport::default(),
        "no record yet"
    );
    f.mgr.handle_projection(&key()).await.unwrap();
    let s = |st: &str| SignerAndState {
        signer_name: SIGNER.into(),
        state: st.into(),
    };
    assert_eq!(
        f.mgr.metric_report().pod_certificate_states,
        BTreeMap::from([(s("not_yet_issued"), 1)])
    );
    let first = list_pcrs(&f.storage).await.remove(0);
    issue(&f, &first, CHAIN_A).await;
    f.mgr.handle_projection(&key()).await.unwrap();
    assert_eq!(
        f.mgr.metric_report().pod_certificate_states,
        BTreeMap::from([(s("fresh"), 1)])
    );
    // begin_refresh_at (+12h, + <=5m jitter) + 10m overdue.
    f.clock.step(Duration::from_secs(12 * 3600 + 16 * 60));
    assert_eq!(
        f.mgr.metric_report().pod_certificate_states,
        BTreeMap::from([(s("overdue_for_refresh"), 1)])
    );
}

#[tokio::test]
async fn forget_pod_and_gone_pod_clear_state() {
    let f = fixture("ED25519").await;
    f.mgr.handle_projection(&key()).await.unwrap();
    assert!(f.mgr.cred_store.lock().unwrap().contains_key(&key()));
    f.mgr.forget_pod(&f.pod);
    assert!(f.mgr.cred_store.lock().unwrap().is_empty());

    f.mgr.handle_projection(&key()).await.unwrap();
    assert_eq!(f.mgr.cred_store.lock().unwrap().len(), 1);
    // Pod deleted: handleProjection clears and returns nil (:381-389).
    f.pods.0.lock().unwrap().clear();
    f.mgr.handle_projection(&key()).await.unwrap();
    assert!(f.mgr.cred_store.lock().unwrap().is_empty());
    let err = bundle(&f).await.unwrap_err();
    assert!(
        err.to_string().starts_with("no credentials yet for key="),
        "{err}"
    );
}

#[tokio::test]
async fn track_pod_and_refresh_pass_queue_projections() {
    let f = fixture("ED25519").await;
    f.mgr.track_pod(&f.pod);
    assert_eq!(f.mgr.queued(), 1);
    f.mgr.track_pod(&f.pod); // deduped
    assert_eq!(f.mgr.queued(), 1);
    f.mgr.run_refresh_pass();
    assert_eq!(f.mgr.queued(), 1);
    // Unknown pod uid: nothing queued (:312-315).
    f.mgr.queue_all_projections_for_pod("nope");
    assert_eq!(f.mgr.queued(), 1);
}

#[tokio::test]
async fn missing_source_is_dropped_not_retried() {
    let f = fixture("ED25519").await;
    let mut k = key();
    k.volume_name = "nope".into();
    f.mgr.handle_projection(&k).await.unwrap(); // `:401-405`
    assert!(list_pcrs(&f.storage).await.is_empty());
}

#[tokio::test]
async fn missing_service_account_errors_for_retry() {
    let f = fixture("ED25519").await;
    f.storage
        .delete(&build_key("serviceaccounts", Some("ns1"), "workload"))
        .await
        .unwrap();
    let err = f.mgr.handle_projection(&key()).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("while creating initial PodCertificateRequest"),
        "{err}"
    );
    assert!(
        err.to_string().contains("while fetching service account"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_drives_the_state_machine_from_pcr_events() {
    // The informer-driven path of TestFullFlow: track the pod, the processor
    // creates the PCR, the (fake) signer issues it, the watch redrives the
    // projection and the bundle appears.
    let f = fixture("ED25519").await;
    let cancel = tokio_util_cancel::Token::new();
    let run = tokio::spawn(Arc::clone(&f.mgr).run(cancel.clone()));
    f.mgr.track_pod(&f.pod);

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let pcr = loop {
        if let Some(p) = list_pcrs(&f.storage).await.pop() {
            break p;
        }
        assert!(std::time::Instant::now() < deadline, "PCR never created");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    issue(&f, &pcr, CHAIN_A).await;
    loop {
        if bundle(&f).await.is_ok() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "bundle never issued");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cancel.cancel();
    run.await.unwrap();
}

#[test]
fn item_backoff_matches_default_item_rate_limiter() {
    assert_eq!(item_backoff(0), Duration::from_millis(5));
    assert_eq!(item_backoff(1), Duration::from_millis(10));
    assert_eq!(item_backoff(30), Duration::from_secs(1000));
}

#[test]
fn clean_certificate_chain_drops_headers_and_junk() {
    let input = "x\n-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n\
                 trailing\n-----BEGIN CERTIFICATE-----\nBBBB\n-----END CERTIFICATE-----\n";
    let out = String::from_utf8(clean_certificate_chain(input.as_bytes())).unwrap();
    assert_eq!(
        out,
        "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n\
         -----BEGIN CERTIFICATE-----\nBBBB\n-----END CERTIFICATE-----\n"
    );
    assert!(clean_certificate_chain(b"no pem here").is_empty());
}

/// Every key type yields a PCR whose proof-of-possession verifies under the
/// api-server's own `ValidatePodCertificateRequestCreate` port — the
/// consumer half of the pair.
#[tokio::test]
async fn generated_proofs_verify_for_every_key_type() {
    use rusternetes_common::validation::podcertificaterequest::validate_pod_certificate_request_create;
    for kt in ["ED25519", "ECDSAP256", "ECDSAP384", "ECDSAP521", "RSA3072"] {
        let f = fixture(kt).await;
        f.mgr.handle_projection(&key()).await.unwrap();
        let pcr = list_pcrs(&f.storage).await.remove(0);
        let errs = format!("{:?}", validate_pod_certificate_request_create(&pcr));
        assert!(
            !errs.contains("proofOfPossession") && !errs.contains("pkixPublicKey"),
            "{kt}: {errs}"
        );
    }
}

#[test]
fn unknown_key_type_errors() {
    let err = generate_key_and_proof("DSA", b"x").unwrap_err();
    assert_eq!(err.to_string(), "unknown key type \"DSA\"");
}

#[tokio::test]
async fn noop_manager_returns_unimplemented() {
    let m = NoOpManager;
    let err = m
        .get_pod_certificate_credential_bundle("ns", "p", "u", "v", 0)
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), "unimplemented");
    assert_eq!(m.metric_report(), MetricReport::default());
}
