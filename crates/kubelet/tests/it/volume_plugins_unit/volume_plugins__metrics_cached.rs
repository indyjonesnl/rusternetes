//! Unit tests moved from `src/volume_plugins/metrics_cached.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_kubelet::volume_plugins::plugin::{Metrics, MetricsProvider};
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::metrics_cached::*;
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
