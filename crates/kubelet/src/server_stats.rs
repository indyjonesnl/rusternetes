//! Port of `pkg/kubelet/server/stats/volume_stat_calculator.go`
//! (`volumeStatCalculator`) and `fs_resource_analyzer.go`
//! (`fsResourceAnalyzer`): the periodic per-pod volume stats aggregation that
//! `volumeStatsAggPeriod` drives (`kubelet.go:705`,
//! `NewResourceAnalyzer(ctx, klet, kubeCfg.VolumeStatsAggPeriod.Duration, ..)`;
//! `kubelet.go:1724`, `kl.resourceAnalyzer.Start(ctx)`).
//!
//! The cache is read by the stats summary through
//! [`FsResourceAnalyzer::get_pod_volume_stats`], as `makePodStorageStats`
//! does (`pkg/kubelet/stats/helper.go:494-499`).
//!
//! Deviations (idiom, not mechanism): `atomic.Value` becomes an `RwLock`
//! guarding an immutable snapshot; `wait.JitterUntil` / `wait.Forever` become
//! tokio tasks with the same sliding-timer semantics; the blocking
//! `GetMetrics` (du/statfs) runs on the blocking pool.

use std::collections::HashMap;
use std::sync::{Arc, Once, RwLock};
use std::time::{Duration, Instant};

use k3rs_common::ephemeral::volume_claim_name;
use k3rs_common::resources::{Pod, Volume};
use rand::Rng;
use serde::Serialize;
use tokio::sync::watch;
use tracing::{debug, info};

use crate::volume_plugins::plugin::{BlockVolumeMapper, Metrics, Mounter};
use crate::volume_plugins::util::is_local_ephemeral_volume;
use crate::volume_plugins::util::metrics::is_not_supported;

/// `eventRecorder.Event(pod, v1.EventTypeWarning, reason, message)`.
pub trait PodEventSink: Send + Sync {
    fn warning(&self, pod: &Pod, reason: &str, message: &str);
}

/// The slice of `stats.Provider` the volume stat calculator uses
/// (`GetPods`, `ListVolumesForPod`, `ListBlockVolumesForPod`). `None` is
/// upstream's `found == false`.
pub trait StatsProvider: Send + Sync {
    /// `GetPods`.
    fn get_pods(&self) -> Vec<Pod>;
    /// `ListVolumesForPod` (`kubelet_volumes.go:39`): keyed by
    /// `OuterVolumeSpecName`.
    fn list_volumes_for_pod(&self, pod_uid: &str) -> Option<HashMap<String, Arc<dyn Mounter>>>;
    /// `ListBlockVolumesForPod` (`kubelet_volumes.go:59`).
    fn list_block_volumes_for_pod(
        &self,
        pod_uid: &str,
    ) -> Option<HashMap<String, Arc<dyn BlockVolumeMapper>>>;
}

/// `stats.PVCReference` (`stats/v1alpha1/types.go:363`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PvcReference {
    pub name: String,
    pub namespace: String,
}

/// `stats.VolumeHealthStats` (`types.go:356`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VolumeHealthStats {
    pub abnormal: bool,
}

/// `stats.VolumeStats` with its embedded `FsStats` flattened
/// (`types.go:340`, `:368`).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeStats {
    pub time: Option<chrono::DateTime<chrono::Utc>>,
    pub available_bytes: Option<u64>,
    pub capacity_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub inodes_free: Option<u64>,
    pub inodes: Option<u64>,
    pub inodes_used: Option<u64>,
    pub name: String,
    #[serde(rename = "pvcRef", skip_serializing_if = "Option::is_none")]
    pub pvc_ref: Option<PvcReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_health_stats: Option<VolumeHealthStats>,
}

/// `PodVolumeStats` (`volume_stat_calculator.go:44-50`).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PodVolumeStats {
    pub ephemeral_volumes: Vec<VolumeStats>,
    pub persistent_volumes: Vec<VolumeStats>,
}

impl PodVolumeStats {
    /// `append(append([]VolumeStats{}, Ephemeral...), Persistent...)`
    /// (`stats/helper.go:496`): the pod summary's `volume` list.
    pub fn all(&self) -> Vec<VolumeStats> {
        let mut v = self.ephemeral_volumes.clone();
        v.extend(self.persistent_volumes.iter().cloned());
        v
    }
}

/// One entry of upstream's `map[string]volume.MetricsProvider`.
enum MetricVolume {
    Fs(Arc<dyn Mounter>),
    Block(Arc<dyn BlockVolumeMapper>),
}

impl MetricVolume {
    fn get_metrics(&self) -> anyhow::Result<Metrics> {
        match self {
            MetricVolume::Fs(m) => m.get_metrics(),
            MetricVolume::Block(m) => m.get_metrics(),
        }
    }
}

/// `volumeStatCalculator` (`volume_stat_calculator.go:36-42`).
pub struct VolumeStatCalculator {
    stats_provider: Arc<dyn StatsProvider>,
    jitter_period: Duration,
    pod: Pod,
    stop: watch::Sender<bool>,
    start_once: Once,
    stop_once: Once,
    latest: RwLock<Option<PodVolumeStats>>,
    event_recorder: Arc<dyn PodEventSink>,
}

impl VolumeStatCalculator {
    /// `newVolumeStatCalculator` (`:53-61`).
    pub fn new(
        stats_provider: Arc<dyn StatsProvider>,
        jitter_period: Duration,
        pod: Pod,
        event_recorder: Arc<dyn PodEventSink>,
    ) -> Arc<Self> {
        Arc::new(Self {
            stats_provider,
            jitter_period,
            pod,
            stop: watch::channel(false).0,
            start_once: Once::new(),
            stop_once: Once::new(),
            latest: RwLock::new(None),
            event_recorder,
        })
    }

    /// `StartOnce` (`:64-72`): `go wait.JitterUntil(calcAndStoreStats,
    /// jitterPeriod, 1.0, true, stopChannel)`. Sliding: the timer starts
    /// after the function returns; the first run is immediate; each wait is
    /// `period + rand*1.0*period`.
    pub fn start_once(self: &Arc<Self>) -> Arc<Self> {
        let this = Arc::clone(self);
        self.start_once.call_once(move || {
            let mut stop = this.stop.subscribe();
            let calc = Arc::clone(&this);
            tokio::spawn(async move {
                loop {
                    if *stop.borrow() {
                        return;
                    }
                    calc.calc_and_store_stats().await;
                    let wait = calc.jitter_period
                        + calc.jitter_period.mul_f64(rand::rng().random::<f64>());
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = stop.changed() => return,
                    }
                }
            });
        });
        Arc::clone(self)
    }

    /// `StopOnce` (`:75-80`): `close(stopChannel)`.
    pub fn stop_once(self: &Arc<Self>) -> Arc<Self> {
        self.stop_once.call_once(|| {
            self.stop.send_replace(true);
        });
        Arc::clone(self)
    }

    /// `GetLatest` (`:83-89`): `(PodVolumeStats{}, false)` until the first
    /// calculation has stored a result.
    pub fn get_latest(&self) -> Option<PodVolumeStats> {
        self.latest.read().expect("latest lock").clone()
    }

    /// `calcAndStoreStats` (`:91-196`).
    pub async fn calc_and_store_stats(&self) {
        let uid = self.pod.metadata.uid.as_str();
        let volumes = self.stats_provider.list_volumes_for_pod(uid);
        let block_volumes = self.stats_provider.list_block_volumes_for_pod(uid);
        if volumes.is_none() && block_volumes.is_none() {
            return;
        }

        let mut metric_volumes: HashMap<String, MetricVolume> = HashMap::new();
        for (name, v) in volumes.into_iter().flatten() {
            metric_volumes.insert(name, MetricVolume::Fs(v));
        }
        for (name, v) in block_volumes.into_iter().flatten() {
            // Some drivers inherit the MetricsProvider interface from
            // Filesystem mode volumes, but do not implement it for Block
            // mode; SupportsMetrics() prevents panics in that case.
            if v.supports_metrics() {
                metric_volumes.insert(name, MetricVolume::Block(v));
            }
        }

        // Volume specs for the pod, keyed by volume name.
        let volumes_spec: HashMap<&str, &Volume> = self
            .pod
            .spec
            .as_ref()
            .map(|s| s.volumes.iter().flatten())
            .into_iter()
            .flatten()
            .map(|v| (v.name.as_str(), v))
            .collect();
        let namespace = self.pod.metadata.namespace.clone().unwrap_or_default();

        let mut ephemeral_stats = Vec::new();
        let mut persistent_stats = Vec::new();
        for (name, v) in metric_volumes {
            let started = Instant::now();
            let metric = tokio::task::spawn_blocking(move || v.get_metrics()).await;
            // `trace.LogIfLong(1 * time.Second)`.
            if started.elapsed() > Duration::from_secs(1) {
                info!(
                    "Calculate volume metrics of {name} for pod {namespace}/{} took {:?}",
                    self.pod.metadata.name,
                    started.elapsed()
                );
            }
            let metric = match metric {
                Ok(Ok(m)) => m,
                Ok(Err(err)) => {
                    // Expected for Volumes that don't support Metrics.
                    if !is_not_supported(&err) {
                        debug!(
                            pod = %self.pod.metadata.name,
                            pod_uid = uid,
                            volume_name = %name,
                            "Failed to calculate volume metrics: {err}"
                        );
                    }
                    continue;
                }
                Err(_) => continue,
            };

            // Add a 'PVCReference' for volumes that reference a PVC.
            let vol_spec = volumes_spec.get(name.as_str()).copied();
            let pvc_ref = vol_spec.and_then(|vs| {
                if let Some(src) = vs.persistent_volume_claim.as_ref() {
                    Some(PvcReference {
                        name: src.claim_name.clone(),
                        namespace: namespace.clone(),
                    })
                } else if vs.ephemeral.is_some() {
                    Some(PvcReference {
                        name: volume_claim_name(&self.pod, vs),
                        namespace: namespace.clone(),
                    })
                } else {
                    None
                }
            });

            let volume_stats = parse_pod_volume_stats(&name, pvc_ref, &metric);
            // A volume absent from the pod spec is the zero `v1.Volume{}`,
            // which is not a local ephemeral volume.
            if vol_spec.is_some_and(is_local_ephemeral_volume) {
                ephemeral_stats.push(volume_stats);
            } else {
                persistent_stats.push(volume_stats);
            }

            if k3rs_common::feature_gates::gate_enabled("CSIVolumeHealth").unwrap_or(false) {
                if let (Some(true), Some(message)) = (metric.abnormal, metric.message.as_ref()) {
                    self.event_recorder.warning(
                        &self.pod,
                        "VolumeConditionAbnormal",
                        &format!("Volume {name}: {message}"),
                    );
                }
            }
        }

        // Store the new stats.
        *self.latest.write().expect("latest lock") = Some(PodVolumeStats {
            ephemeral_volumes: ephemeral_stats,
            persistent_volumes: persistent_stats,
        });
    }
}

/// `parsePodVolumeStats` (`:198-221`): absent quantities report 0.
pub fn parse_pod_volume_stats(
    pod_name: &str,
    pvc_ref: Option<PvcReference>,
    metric: &Metrics,
) -> VolumeStats {
    let v =
        |q: &Option<k3rs_common::quantity::Quantity>| q.as_ref().map_or(0u64, |q| q.value() as u64);
    VolumeStats {
        name: pod_name.to_string(),
        pvc_ref,
        time: metric.time,
        available_bytes: Some(v(&metric.available)),
        capacity_bytes: Some(v(&metric.capacity)),
        used_bytes: Some(v(&metric.used)),
        inodes: Some(v(&metric.inodes)),
        inodes_free: Some(v(&metric.inodes_free)),
        inodes_used: Some(v(&metric.inodes_used)),
        volume_health_stats: metric
            .abnormal
            .map(|abnormal| VolumeHealthStats { abnormal }),
    }
}

/// `statCache` (`fs_resource_analyzer.go:34`).
type StatCache = HashMap<String, Arc<VolumeStatCalculator>>;

/// `fsResourceAnalyzer` (`fs_resource_analyzer.go:41-48`).
pub struct FsResourceAnalyzer {
    stats_provider: Arc<dyn StatsProvider>,
    calc_period: Duration,
    cached_volume_stats: RwLock<Arc<StatCache>>,
    start_once: Once,
    event_recorder: Arc<dyn PodEventSink>,
}

impl FsResourceAnalyzer {
    /// `newFsResourceAnalyzer` (`:52-61`).
    pub fn new(
        stats_provider: Arc<dyn StatsProvider>,
        calc_volume_period: Duration,
        event_recorder: Arc<dyn PodEventSink>,
    ) -> Arc<Self> {
        Arc::new(Self {
            stats_provider,
            calc_period: calc_volume_period,
            cached_volume_stats: RwLock::new(Arc::new(HashMap::new())),
            start_once: Once::new(),
            event_recorder,
        })
    }

    /// The period the loop ticks at (`calcPeriod`).
    pub fn calc_period(&self) -> Duration {
        self.calc_period
    }

    /// `Start` (`:64-76`): a non-positive period disables collection;
    /// otherwise `go wait.Forever(updateCachedPodVolumeStats, calcPeriod)`
    /// (first run immediate, then sleep after each run).
    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        self.start_once.call_once(move || {
            if this.calc_period.is_zero() {
                info!("Volume stats collection disabled");
                return;
            }
            info!("Starting FS ResourceAnalyzer");
            tokio::spawn(async move {
                loop {
                    this.update_cached_pod_volume_stats();
                    tokio::time::sleep(this.calc_period).await;
                }
            });
        });
    }

    /// `updateCachedPodVolumeStats` (`:78-98`): start a calculator for every
    /// new pod, stop the ones of pods that are gone, swap the cache.
    pub fn update_cached_pod_volume_stats(&self) {
        let old_cache = Arc::clone(&self.cached_volume_stats.read().expect("cache lock"));
        let mut new_cache: StatCache = HashMap::new();

        // Copy existing entries to the new map, creating/starting new entries
        // for pods missing from the cache.
        for pod in self.stats_provider.get_pods() {
            let uid = pod.metadata.uid.clone();
            if let Some(existing) = old_cache.get(&uid) {
                new_cache.insert(uid, Arc::clone(existing));
            } else {
                let calc = VolumeStatCalculator::new(
                    Arc::clone(&self.stats_provider),
                    self.calc_period,
                    pod,
                    Arc::clone(&self.event_recorder),
                )
                .start_once();
                new_cache.insert(uid, calc);
            }
        }

        // Stop entries for pods that have been deleted.
        for (uid, entry) in old_cache.iter() {
            if !new_cache.contains_key(uid) {
                entry.stop_once();
            }
        }

        *self.cached_volume_stats.write().expect("cache lock") = Arc::new(new_cache);
    }

    /// `GetPodVolumeStats` (`:104-110`).
    pub fn get_pod_volume_stats(&self, uid: &str) -> Option<PodVolumeStats> {
        let cache = Arc::clone(&self.cached_volume_stats.read().expect("cache lock"));
        cache.get(uid)?.get_latest()
    }
}

/// The kubelet's [`StatsProvider`] (`kubelet.go` implements `stats.Provider`):
/// `GetPods` is the pod manager's, the volume lists come from the volume
/// manager (`kubelet_volumes.go:39-74`).
pub struct KubeletStatsProvider {
    pub pod_manager: Arc<dyn crate::podcertificate::PodManager>,
    pub volume_manager: Arc<crate::volume_manager::VolumeManager>,
}

impl StatsProvider for KubeletStatsProvider {
    fn get_pods(&self) -> Vec<Pod> {
        self.pod_manager.get_pods()
    }

    fn list_volumes_for_pod(&self, pod_uid: &str) -> Option<HashMap<String, Arc<dyn Mounter>>> {
        let name = crate::volume_plugins::util::types::UniquePodName(pod_uid.to_string());
        // `volume.Mounter == nil` (recovered by the reconciler) is skipped.
        let out: HashMap<_, _> = self
            .volume_manager
            .get_mounted_volumes_for_pod(&name)
            .into_iter()
            .filter_map(|(n, v)| v.mounter.map(|m| (n, m)))
            .collect();
        (!out.is_empty()).then_some(out)
    }

    fn list_block_volumes_for_pod(
        &self,
        pod_uid: &str,
    ) -> Option<HashMap<String, Arc<dyn BlockVolumeMapper>>> {
        let name = crate::volume_plugins::util::types::UniquePodName(pod_uid.to_string());
        let out: HashMap<_, _> = self
            .volume_manager
            .get_mounted_volumes_for_pod(&name)
            .into_iter()
            .filter_map(|(n, v)| v.block_volume_mapper.map(|m| (n, m)))
            .collect();
        (!out.is_empty()).then_some(out)
    }
}

/// `record.EventRecorder` against the pod, as the kubelet's recorder.
pub struct StoragePodEvents {
    pub recorder: k3rs_storage::EventRecorder<k3rs_storage::StorageBackend>,
    pub node_name: String,
}

impl PodEventSink for StoragePodEvents {
    fn warning(&self, pod: &Pod, reason: &str, message: &str) {
        let recorder = self.recorder.clone();
        let involved = crate::events::pod_object_reference(pod);
        let source = k3rs_common::resources::EventSource {
            component: crate::events::KUBELET_COMPONENT.to_string(),
            host: Some(self.node_name.clone()),
        };
        let (reason, message) = (reason.to_string(), message.to_string());
        tokio::spawn(async move {
            let _ = recorder
                .event(
                    &involved,
                    &source,
                    k3rs_common::resources::EventType::Warning,
                    &reason,
                    &message,
                )
                .await;
        });
    }
}
