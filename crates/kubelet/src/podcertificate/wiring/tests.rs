//! Tests for the kubelet wiring of the pod certificate manager. Upstream has
//! no unit test for `HandlePodAdditions`' `TrackPod` call or for
//! `PodCertificateCollectorFor`; these pin the call order and exposition the
//! Go code defines (`kubelet.go:2729` track AFTER `AddPod`; `:2948` forget
//! BEFORE `RemovePod`).

use super::*;
use crate::podcertificate::tokio_util_cancel::Token;
use crate::podcertificate::{MetricReport, SignerAndState};
use async_trait::async_trait;
use prometheus::core::Collector;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
use rusternetes_common::resources::{Node, PodSpec, ServiceAccount, Volume};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, Storage, StorageBackend};
use std::sync::Mutex;

fn pod(uid: &str, name: &str) -> Pod {
    Pod {
        type_meta: TypeMeta::default(),
        metadata: ObjectMeta {
            name: name.into(),
            namespace: Some("ns1".into()),
            uid: uid.into(),
            ..Default::default()
        },
        spec: Some(PodSpec {
            service_account_name: Some("workload".into()),
            node_name: Some("node1".into()),
            volumes: Some(vec![serde_json::from_value::<Volume>(serde_json::json!({
                "name": "certificate",
                "projected": {"sources": [{"podCertificate": {
                    "signerName": "foo.com/signer",
                    "keyType": "ED25519",
                    "credentialBundlePath": "creds.pem",
                    "maxExpirationSeconds": 86400,
                }}]},
            }))
            .unwrap()]),
            ..Default::default()
        }),
        status: None,
    }
}

/// Records each call together with whether the pod was visible in the cache
/// at that moment, to pin upstream's ordering.
struct Recording {
    cache: Arc<PodCache>,
    calls: Mutex<Vec<(String, String, bool)>>,
}

impl Recording {
    fn new(cache: Arc<PodCache>) -> Self {
        Self {
            cache,
            calls: Mutex::new(vec![]),
        }
    }
}

#[async_trait]
impl Manager for Recording {
    fn track_pod(&self, pod: &Pod) {
        let seen = self.cache.get_pod_by_uid(&pod.metadata.uid).is_some();
        self.calls
            .lock()
            .unwrap()
            .push(("track".into(), pod.metadata.uid.clone(), seen));
    }
    fn forget_pod(&self, pod: &Pod) {
        let seen = self.cache.get_pod_by_uid(&pod.metadata.uid).is_some();
        self.calls
            .lock()
            .unwrap()
            .push(("forget".into(), pod.metadata.uid.clone(), seen));
    }
    async fn get_pod_certificate_credential_bundle(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: usize,
    ) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        unreachable!()
    }
    fn metric_report(&self) -> MetricReport {
        let mut r = MetricReport::default();
        r.pod_certificate_states.insert(
            SignerAndState {
                signer_name: "foo.com/signer".into(),
                state: "fresh".into(),
            },
            3,
        );
        r
    }
}

/// `HandlePodAdditions`: `podManager.AddPod` then `TrackPod`
/// (`kubelet.go:2724-2729`); `HandlePodRemoves`: `ForgetPod` then
/// `RemovePod` (`:2948-2949`).
#[test]
fn reconcile_tracks_after_add_and_forgets_before_remove() {
    let cache = PodCache::new();
    let rec = Recording::new(cache.clone());
    let (a, b) = (pod("uid-a", "a"), pod("uid-b", "b"));

    cache.reconcile(&[a.clone(), b.clone()], &rec);
    assert_eq!(cache.get_pods().len(), 2);
    // Unchanged set: no repeat TrackPod.
    cache.reconcile(&[a.clone(), b.clone()], &rec);
    cache.reconcile(std::slice::from_ref(&a), &rec);

    let calls = rec.calls.lock().unwrap().clone();
    let mut tracked: Vec<_> = calls.iter().filter(|c| c.0 == "track").cloned().collect();
    tracked.sort();
    assert_eq!(
        tracked,
        vec![
            ("track".to_string(), "uid-a".to_string(), true),
            ("track".to_string(), "uid-b".to_string(), true)
        ]
    );
    assert_eq!(
        calls.iter().filter(|c| c.0 == "forget").collect::<Vec<_>>(),
        vec![&("forget".to_string(), "uid-b".to_string(), true)]
    );
    assert!(cache.get_pod_by_uid("uid-b").is_none());
    assert!(cache.get_pod_by_uid("uid-a").is_some());
}

/// `podCertificateCollector.CollectWithStability`:
/// `kubelet_podcertificate_states{signer_name,state}`.
#[test]
fn collector_exposes_states_gauge() {
    let rec = Arc::new(Recording::new(PodCache::new()));
    let c = PodCertificateCollector::new(rec.clone());
    let fams = c.collect();
    assert_eq!(fams.len(), 1);
    assert_eq!(fams[0].name(), "kubelet_podcertificate_states");
    let m = &fams[0].metric[0];
    assert_eq!(m.gauge.value(), 3.0);
    let labels: Vec<_> = m
        .label
        .iter()
        .map(|l| (l.name().to_string(), l.value().to_string()))
        .collect();
    assert_eq!(
        labels,
        vec![
            ("signer_name".to_string(), "foo.com/signer".to_string()),
            ("state".to_string(), "fresh".to_string())
        ]
    );
    // Registers in a real registry (desc is well formed).
    prometheus::Registry::new()
        .register(Box::new(PodCertificateCollector::new(rec)))
        .unwrap();
}

/// `kubelet.go:970-972`: gate off -> `NoOpManager`, no `Run`.
#[tokio::test]
#[serial_test::serial]
async fn gate_off_builds_noop_manager() {
    let _g = with_feature(Feature::PodCertificateRequest, false);
    let storage = Arc::new(StorageBackend::new_memory());
    let (m, issuing) = new_pod_certificate_manager(storage, PodCache::new(), "node1");
    assert!(issuing.is_none());
    let err = m
        .get_pod_certificate_credential_bundle("ns1", "a", "uid-a", "certificate", 0)
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), "unimplemented");
}

/// End to end: gate on, a pod arriving through `reconcile` makes the running
/// manager create a PodCertificateRequest for it, and `ForgetPod` via
/// `reconcile` drops its metrics.
#[tokio::test]
#[serial_test::serial]
async fn gate_on_tracked_pod_gets_a_pcr_and_forget_clears_it() {
    let _g = with_feature(Feature::PodCertificateRequest, true);
    let storage = Arc::new(StorageBackend::new_memory());
    storage
        .create(
            &build_key("nodes", None, "node1"),
            &Node {
                type_meta: TypeMeta::default(),
                metadata: ObjectMeta {
                    name: "node1".into(),
                    uid: "node1-uid".into(),
                    ..Default::default()
                },
                spec: None,
                status: None,
            },
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("serviceaccounts", Some("ns1"), "workload"),
            &ServiceAccount {
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
            },
        )
        .await
        .unwrap();

    let cache = PodCache::new();
    let (manager, issuing) = new_pod_certificate_manager(storage.clone(), cache.clone(), "node1");
    let issuing = issuing.expect("gate on builds an IssuingManager");
    let token = Token::new();
    let run = tokio::spawn(issuing.run(token.clone()));

    let p = pod("uid-a", "a");
    cache.reconcile(std::slice::from_ref(&p), &*manager);

    let mut found = false;
    for _ in 0..100 {
        let pcrs: Vec<PodCertificateRequest> = storage
            .list(&build_prefix("podcertificaterequests", None))
            .await
            .unwrap();
        if pcrs.iter().any(|r| r.spec.pod_uid == "uid-a") {
            found = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        found,
        "no PodCertificateRequest created for the tracked pod"
    );
    assert!(!manager.metric_report().pod_certificate_states.is_empty());

    cache.reconcile(&[], &*manager);
    assert!(manager.metric_report().pod_certificate_states.is_empty());

    token.cancel();
    run.await.unwrap();
}
