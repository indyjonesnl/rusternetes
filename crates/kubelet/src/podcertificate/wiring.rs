//! Kubelet-side wiring of the pod certificate manager.
//!
//! Ports the parts of `pkg/kubelet/kubelet.go` that surround
//! `podcertificate.IssuingManager`:
//!
//! * construction (`kubelet.go:955-972`): an `IssuingManager` when the
//!   `PodCertificateRequest` gate is on, else a `NoOpManager`;
//! * the `HandlePodAdditions` / `HandlePodRemoves` hooks
//!   (`kubelet.go:2729`, `:2948`) that feed `TrackPod` / `ForgetPod`;
//! * the pod manager the issuing manager reads (`podManager`);
//! * the metrics collector (`metrics/collectors/podcertificate_metrics.go`).

use super::{IssuingManager, Manager, NoOpManager, PodManager, RealClock};
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Gauge, LabelPair, Metric, MetricFamily, MetricType};
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::Pod;
use rusternetes_storage::{EventRecorder, Storage};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tracing::info;

/// `kubelet.podManager`, as far as the pod certificate manager reads it.
/// Rusternetes has no event-driven pod manager: the sync loop lists the
/// node's pods each pass, so [`PodCache::reconcile`] turns that list into the
/// `HandlePodAdditions` / `HandlePodRemoves` calls upstream receives as
/// config-source updates.
#[derive(Default)]
pub struct PodCache {
    pods: RwLock<HashMap<String, Pod>>,
}

impl PodCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Diff `pods` against the cache.
    ///
    /// * Added: `podManager.AddPod(pod)` then `podCertificateManager.TrackPod`
    ///   (`HandlePodAdditions`, `kubelet.go:2724-2729`) -- tracking after the
    ///   add is what lets `queueAllProjectionsForPod` find the pod by UID.
    /// * Removed: `ForgetPod` then `podManager.RemovePod`
    ///   (`HandlePodRemoves`, `kubelet.go:2948-2949`).
    /// * Already present: the cached pod is refreshed (`UpdatePod`), without
    ///   a second `TrackPod`.
    pub fn reconcile(&self, pods: &[Pod], manager: &dyn Manager) {
        let current: HashSet<&str> = pods.iter().map(|p| p.metadata.uid.as_str()).collect();
        let removed: Vec<Pod> = self
            .pods
            .read()
            .unwrap()
            .values()
            .filter(|p| !current.contains(p.metadata.uid.as_str()))
            .cloned()
            .collect();
        for pod in &removed {
            manager.forget_pod(pod);
            self.pods.write().unwrap().remove(&pod.metadata.uid);
        }
        for pod in pods {
            if pod.metadata.uid.is_empty() {
                continue;
            }
            let was_known = self
                .pods
                .write()
                .unwrap()
                .insert(pod.metadata.uid.clone(), pod.clone())
                .is_some();
            if !was_known {
                manager.track_pod(pod);
            }
        }
    }
}

impl PodManager for PodCache {
    fn get_pod_by_uid(&self, uid: &str) -> Option<Pod> {
        self.pods.read().unwrap().get(uid).cloned()
    }
    fn get_pods(&self) -> Vec<Pod> {
        self.pods.read().unwrap().values().cloned().collect()
    }
}

const POD_CERTIFICATE_STATES: &str = "kubelet_podcertificate_states";
const POD_CERTIFICATE_STATES_HELP: &str = "Gauge vector reporting the number of pod certificate projected volume sources, faceted by signer_name and state.";

/// `podCertificateCollector` (`podcertificate_metrics.go:30-62`):
/// `kubelet_podcertificate_states{signer_name,state}`.
pub struct PodCertificateCollector {
    manager: Arc<dyn Manager>,
    desc: Desc,
}

impl PodCertificateCollector {
    /// `PodCertificateCollectorFor`.
    pub fn new(manager: Arc<dyn Manager>) -> Self {
        let desc = Desc::new(
            POD_CERTIFICATE_STATES.to_string(),
            POD_CERTIFICATE_STATES_HELP.to_string(),
            vec!["signer_name".to_string(), "state".to_string()],
            Default::default(),
        )
        .expect("static metric description is valid");
        Self { manager, desc }
    }
}

impl Collector for PodCertificateCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    /// `CollectWithStability`: one gauge per (signer, state) in the report.
    fn collect(&self) -> Vec<MetricFamily> {
        let report = self.manager.metric_report();
        if report.pod_certificate_states.is_empty() {
            return Vec::new();
        }
        let mut family = MetricFamily::default();
        family.set_name(POD_CERTIFICATE_STATES.to_string());
        family.set_help(POD_CERTIFICATE_STATES_HELP.to_string());
        family.set_field_type(MetricType::GAUGE);
        let metrics = report
            .pod_certificate_states
            .iter()
            .map(|(k, count)| {
                let mut metric = Metric::default();
                metric.set_label(
                    [("signer_name", &k.signer_name), ("state", &k.state)]
                        .into_iter()
                        .map(|(n, v)| {
                            let mut lp = LabelPair::default();
                            lp.set_name(n.to_string());
                            lp.set_value(v.clone());
                            lp
                        })
                        .collect::<Vec<_>>(),
                );
                let mut gauge = Gauge::default();
                gauge.set_value(*count as f64);
                metric.set_gauge(gauge);
                metric
            })
            .collect();
        family.set_metric(metrics);
        vec![family]
    }
}

/// `kubelet.go:955-972`: an `IssuingManager` when a client exists and the
/// `PodCertificateRequest` gate is on (the caller then spawns `run`), else a
/// `NoOpManager`. Rusternetes' kubelet always has a storage client, so only
/// the gate decides.
#[allow(clippy::type_complexity)]
pub fn new_pod_certificate_manager<S: Storage + ?Sized + 'static>(
    storage: Arc<S>,
    pods: Arc<dyn PodManager>,
    node_name: &str,
) -> (Arc<dyn Manager>, Option<Arc<IssuingManager<S>>>) {
    if feature_gates::enabled(Feature::PodCertificateRequest) {
        let issuing = IssuingManager::new(
            storage.clone(),
            pods,
            Some(EventRecorder::new(storage)),
            node_name,
            Arc::new(RealClock),
        );
        (issuing.clone() as Arc<dyn Manager>, Some(issuing))
    } else {
        info!(
            "Not starting PodCertificateRequest manager because the PodCertificateRequest feature gate is disabled"
        );
        (Arc::new(NoOpManager), None)
    }
}

#[cfg(test)]
mod tests;
