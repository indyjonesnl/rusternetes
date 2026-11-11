//! `pkg/kubelet/server/stats/volume_stat_calculator.go` and
//! `fs_resource_analyzer.go`: the volume stats aggregation driven by
//! `volumeStatsAggPeriod`.
//!
//! Upstream's `volume_stat_calculator_test.go` pins what `calcAndStoreStats`
//! reports (PVC reference for PVC-backed and generic ephemeral volumes, the
//! ephemeral/persistent split, `Abnormal` carried into `VolumeHealthStats`);
//! those cases are ported here. `fs_resource_analyzer.go` has no upstream
//! test; the loop tests assert the `Start` / `updateCachedPodVolumeStats`
//! contract (a period <= 0 disables collection, calculators follow the pod
//! list, the cache feeds `GetPodVolumeStats`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use k3rs_common::quantity::{Format, Quantity};
use k3rs_common::resources::Pod;
use k3rs_kubelet::server_stats::{
    parse_pod_volume_stats, FsResourceAnalyzer, PodEventSink, StatsProvider, VolumeStatCalculator,
};
use k3rs_kubelet::volume_plugins::plugin::{
    Attributes, Metrics, MetricsProvider, Mounter, MounterArgs,
};
use serde_json::json;

struct FakeMounter {
    metrics: Metrics,
}

impl MetricsProvider for FakeMounter {
    fn get_metrics(&self) -> anyhow::Result<Metrics> {
        Ok(self.metrics.clone())
    }
}

#[async_trait]
impl Mounter for FakeMounter {
    fn get_path(&self) -> String {
        String::new()
    }
    async fn set_up_at(&self, _dir: &str, _args: &MounterArgs) -> anyhow::Result<()> {
        Ok(())
    }
    fn get_attributes(&self) -> Attributes {
        Attributes::default()
    }
}

fn q(v: i64) -> Option<Quantity> {
    Some(Quantity::from_value(v, Format::BinarySI))
}

fn metrics(abnormal: Option<bool>) -> Metrics {
    Metrics {
        time: Some(chrono::Utc::now()),
        used: q(100),
        capacity: q(1000),
        available: q(900),
        inodes: q(50),
        inodes_free: q(40),
        inodes_used: q(10),
        abnormal,
        message: abnormal.map(|_| "bad".to_string()),
        ..Default::default()
    }
}

fn pod(uid: &str) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "ns", "uid": uid},
        "spec": {
            "containers": [],
            "volumes": [
                {"name": "scratch", "emptyDir": {}},
                {"name": "data", "persistentVolumeClaim": {"claimName": "claim"}},
                {"name": "gen", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}},
                {"name": "mem", "emptyDir": {"medium": "Memory"}}
            ]
        }
    }))
    .unwrap()
}

#[derive(Default)]
struct Provider {
    pods: Mutex<Vec<Pod>>,
    get_pods_calls: AtomicUsize,
    list_calls: AtomicUsize,
    abnormal: Option<bool>,
}

impl StatsProvider for Provider {
    fn get_pods(&self) -> Vec<Pod> {
        self.get_pods_calls.fetch_add(1, Ordering::SeqCst);
        self.pods.lock().unwrap().clone()
    }
    fn list_volumes_for_pod(&self, _uid: &str) -> Option<HashMap<String, Arc<dyn Mounter>>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        let mut m: HashMap<String, Arc<dyn Mounter>> = HashMap::new();
        for n in ["scratch", "data", "gen", "mem"] {
            m.insert(
                n.to_string(),
                Arc::new(FakeMounter {
                    metrics: metrics(self.abnormal),
                }),
            );
        }
        Some(m)
    }
    fn list_block_volumes_for_pod(
        &self,
        _uid: &str,
    ) -> Option<HashMap<String, Arc<dyn k3rs_kubelet::volume_plugins::plugin::BlockVolumeMapper>>>
    {
        None
    }
}

#[derive(Default)]
struct Events(Mutex<Vec<(String, String)>>);

impl PodEventSink for Events {
    fn warning(&self, _pod: &Pod, reason: &str, message: &str) {
        self.0
            .lock()
            .unwrap()
            .push((reason.to_string(), message.to_string()));
    }
}

async fn eventually(mut f: impl FnMut() -> bool) -> bool {
    for _ in 0..200 {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

/// `TestPVCRef` / the ephemeral-vs-persistent split of `calcAndStoreStats`.
#[tokio::test]
async fn calc_splits_ephemeral_and_persistent_and_sets_pvc_refs() {
    let calc = VolumeStatCalculator::new(
        Arc::new(Provider::default()),
        Duration::from_secs(60),
        pod("u1"),
        Arc::new(Events::default()),
    );
    assert!(calc.get_latest().is_none());
    calc.calc_and_store_stats().await;
    let got = calc.get_latest().expect("stored");

    // emptyDir (default medium) is local ephemeral; Memory-backed emptyDir,
    // PVC and generic ephemeral volumes are persistent.
    let eph: Vec<_> = got.ephemeral_volumes.iter().map(|v| &v.name).collect();
    assert_eq!(eph, vec!["scratch"]);
    let mut pers: Vec<_> = got
        .persistent_volumes
        .iter()
        .map(|v| (v.name.clone(), v.pvc_ref.clone()))
        .collect();
    pers.sort_by(|a, b| a.0.cmp(&b.0));
    let names: Vec<_> = pers.iter().map(|p| p.0.as_str()).collect();
    assert_eq!(names, vec!["data", "gen", "mem"]);
    assert_eq!(pers[0].1.as_ref().unwrap().name, "claim");
    assert_eq!(pers[0].1.as_ref().unwrap().namespace, "ns");
    // `ephemeral.VolumeClaimName`: <pod>-<volume>.
    assert_eq!(pers[1].1.as_ref().unwrap().name, "p-gen");
    assert!(pers[2].1.is_none());

    let s = &got.ephemeral_volumes[0];
    assert_eq!(s.used_bytes, Some(100));
    assert_eq!(s.capacity_bytes, Some(1000));
    assert_eq!(s.available_bytes, Some(900));
    assert_eq!(s.inodes, Some(50));
    assert_eq!(s.inodes_free, Some(40));
    assert_eq!(s.inodes_used, Some(10));
    assert!(s.volume_health_stats.is_none());
}

/// `parsePodVolumeStats`: nil quantities report 0, `Abnormal` is carried.
#[test]
fn parse_pod_volume_stats_defaults_and_health() {
    let m = Metrics {
        abnormal: Some(true),
        ..Default::default()
    };
    let s = parse_pod_volume_stats("v", None, &m);
    assert_eq!(s.used_bytes, Some(0));
    assert_eq!(s.inodes_used, Some(0));
    assert!(s.volume_health_stats.unwrap().abnormal);
}

/// `CSIVolumeHealth`: an abnormal volume emits a `VolumeConditionAbnormal`
/// warning event on the pod.
#[tokio::test]
#[serial_test::serial]
async fn abnormal_volume_emits_event_when_gate_enabled() {
    k3rs_common::feature_gates::set_from_str("CSIVolumeHealth=true").unwrap();
    let events = Arc::new(Events::default());
    let calc = VolumeStatCalculator::new(
        Arc::new(Provider {
            abnormal: Some(true),
            ..Default::default()
        }),
        Duration::from_secs(60),
        pod("u1"),
        events.clone(),
    );
    calc.calc_and_store_stats().await;
    k3rs_common::feature_gates::reset_to_defaults();
    let got = events.0.lock().unwrap().clone();
    assert_eq!(got.len(), 4);
    assert!(got.iter().all(|(r, _)| r == "VolumeConditionAbnormal"));
    assert!(got.iter().any(|(_, m)| m == "Volume data: bad"));
}

/// Reaches-the-consumer: the configured `volumeStatsAggPeriod` is the period
/// of the aggregation loop. A short period re-lists the pods and re-runs
/// the calculator repeatedly; a long one runs once.
#[tokio::test]
async fn volume_stats_agg_period_drives_the_aggregation_loop() {
    let fast = Arc::new(Provider {
        pods: Mutex::new(vec![pod("u1")]),
        ..Default::default()
    });
    let a = FsResourceAnalyzer::new(
        fast.clone(),
        Duration::from_millis(40),
        Arc::new(Events::default()),
    );
    a.start();
    assert!(
        eventually(|| fast.get_pods_calls.load(Ordering::SeqCst) >= 3
            && fast.list_calls.load(Ordering::SeqCst) >= 2)
        .await,
        "a 40ms period must keep re-running the loop"
    );
    // The summary reads this cache.
    let stats = a.get_pod_volume_stats("u1").expect("cached stats");
    assert_eq!(stats.all().len(), 4);
    assert!(a.get_pod_volume_stats("missing").is_none());

    let slow = Arc::new(Provider {
        pods: Mutex::new(vec![pod("u1")]),
        ..Default::default()
    });
    let b = FsResourceAnalyzer::new(
        slow.clone(),
        Duration::from_secs(3600),
        Arc::new(Events::default()),
    );
    b.start();
    assert!(eventually(|| slow.get_pods_calls.load(Ordering::SeqCst) == 1).await);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(slow.get_pods_calls.load(Ordering::SeqCst), 1);
}

/// `Start`: `calcPeriod <= 0` disables collection.
#[tokio::test]
async fn zero_period_disables_collection() {
    let p = Arc::new(Provider {
        pods: Mutex::new(vec![pod("u1")]),
        ..Default::default()
    });
    let a = FsResourceAnalyzer::new(p.clone(), Duration::ZERO, Arc::new(Events::default()));
    a.start();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(p.get_pods_calls.load(Ordering::SeqCst), 0);
    assert!(a.get_pod_volume_stats("u1").is_none());
}

/// `updateCachedPodVolumeStats`: a calculator is started for a new pod and
/// stopped (no more `ListVolumesForPod`) once the pod is gone.
#[tokio::test]
async fn calculators_follow_the_pod_list() {
    let p = Arc::new(Provider {
        pods: Mutex::new(vec![pod("u1")]),
        ..Default::default()
    });
    let a = FsResourceAnalyzer::new(
        p.clone(),
        Duration::from_millis(30),
        Arc::new(Events::default()),
    );
    a.update_cached_pod_volume_stats();
    assert!(eventually(|| a.get_pod_volume_stats("u1").is_some()).await);

    p.pods.lock().unwrap().clear();
    a.update_cached_pod_volume_stats();
    assert!(a.get_pod_volume_stats("u1").is_none());
    // Let an in-flight run finish, then the calls must stop.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let n = p.list_calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(p.list_calls.load(Ordering::SeqCst), n);
}
