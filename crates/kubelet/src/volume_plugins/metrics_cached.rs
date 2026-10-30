//! Port of `pkg/volume/metrics_cached.go` (`cachedMetrics`).

use crate::volume_plugins::plugin::{Metrics, MetricsProvider};
use anyhow::Result;
use std::sync::Mutex;

/// `cachedMetrics` (`metrics_cached.go:28-34`): a [`MetricsProvider`] that
/// wraps another provider and caches the result. Upstream's `cacheOnce` is a
/// `sync.Once` that does not latch on error; the `Option` under the mutex is
/// the same thing (`None` until a call succeeds).
pub struct CachedMetrics {
    wrapped: Box<dyn MetricsProvider + Send + Sync>,
    result: Mutex<Option<Metrics>>,
}

impl CachedMetrics {
    /// `NewCachedMetrics` (`metrics_cached.go:37-39`).
    pub fn new(provider: impl MetricsProvider + Send + Sync + 'static) -> Self {
        Self {
            wrapped: Box::new(provider),
            result: Mutex::new(None),
        }
    }
}

/// `volume.NewCachedMetrics(volume.NewMetricsDu(path))`: what the configMap,
/// secret, downwardAPI and projected plugins embed (`secret.go:104,119`,
/// `configmap.go:100,115`, `downwardapi.go:102,116`, `projected.go:119,132`).
pub fn cached_du(path: &str) -> CachedMetrics {
    CachedMetrics::new(crate::volume_plugins::metrics_du::MetricsDu::new(path))
}

impl MetricsProvider for CachedMetrics {
    /// `GetMetrics` (`metrics_cached.go:44-50`): runs the wrapped provider's
    /// `GetMetrics` once and caches the result. An error is not cached, so the
    /// next call asks the wrapped provider again.
    fn get_metrics(&self) -> Result<Metrics> {
        let mut cached = self.result.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(m) = cached.as_ref() {
            return Ok(m.clone());
        }
        let m = self.wrapped.get_metrics()?;
        *cached = Some(m.clone());
        Ok(m)
    }
}
