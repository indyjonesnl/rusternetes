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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Fails `fail` times, then succeeds with a distinct `used` per call.
    struct Flaky {
        calls: Arc<AtomicUsize>,
        fail: usize,
    }

    impl MetricsProvider for Flaky {
        fn get_metrics(&self) -> Result<Metrics> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail {
                return Err(anyhow::anyhow!("not yet"));
            }
            Ok(Metrics {
                inodes_used: Some(rusternetes_common::quantity::Quantity::from_value(
                    n as i64,
                    rusternetes_common::quantity::Format::DecimalSI,
                )),
                ..Default::default()
            })
        }
    }

    #[test]
    fn caches_a_success() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = CachedMetrics::new(Flaky {
            calls: calls.clone(),
            fail: 0,
        });
        let a = c.get_metrics().unwrap();
        let b = c.get_metrics().unwrap();
        assert_eq!(a, b);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn does_not_cache_an_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = CachedMetrics::new(Flaky {
            calls: calls.clone(),
            fail: 1,
        });
        assert!(c.get_metrics().is_err());
        assert!(c.get_metrics().is_ok());
        assert!(c.get_metrics().is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
