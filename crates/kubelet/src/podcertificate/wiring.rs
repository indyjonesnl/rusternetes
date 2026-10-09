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

use super::{Manager, PodManager};
use rusternetes_common::resources::Pod;
use std::sync::Arc;

/// `kubelet.podManager`, as far as the pod certificate manager reads it.
/// Rusternetes has no event-driven pod manager: the sync loop lists the
/// node's pods each pass, so [`PodCache::reconcile`] turns that list into the
/// `HandlePodAdditions` / `HandlePodRemoves` calls upstream receives as
/// config-source updates.
pub struct PodCache;

impl PodCache {
    pub fn new() -> Arc<Self> {
        unimplemented!()
    }

    /// Diff `pods` against the cache: add (then `TrackPod`) new pods,
    /// `ForgetPod` (then remove) vanished ones.
    pub fn reconcile(&self, _pods: &[Pod], _manager: &dyn Manager) {
        unimplemented!()
    }
}

impl PodManager for PodCache {
    fn get_pod_by_uid(&self, _uid: &str) -> Option<Pod> {
        unimplemented!()
    }
    fn get_pods(&self) -> Vec<Pod> {
        unimplemented!()
    }
}

/// `PodCertificateCollectorFor`.
pub struct PodCertificateCollector;

impl PodCertificateCollector {
    pub fn new(_manager: Arc<dyn Manager>) -> Self {
        unimplemented!()
    }
}

impl prometheus::core::Collector for PodCertificateCollector {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        unimplemented!()
    }
    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        unimplemented!()
    }
}

/// `kubelet.go:955-972`.
#[allow(clippy::type_complexity)]
pub fn new_pod_certificate_manager<S: rusternetes_storage::Storage + ?Sized + 'static>(
    _storage: Arc<S>,
    _pods: Arc<dyn PodManager>,
    _node_name: &str,
) -> (Arc<dyn Manager>, Option<Arc<super::IssuingManager<S>>>) {
    unimplemented!()
}

#[cfg(test)]
mod tests;
