//! Unit tests moved from `src/observability.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{
        Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
        Registry,
    };
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::observability::*;

    #[test]
    fn test_api_server_metrics() {
        let registry = Registry::new();
        let metrics = ApiServerMetrics::new(&registry).unwrap();

        metrics
            .requests_total
            .with_label_values(&["GET", "/api/v1/pods", "200"])
            .inc();
        assert_eq!(
            metrics
                .requests_total
                .with_label_values(&["GET", "/api/v1/pods", "200"])
                .get(),
            1.0
        );
    }

    #[test]
    fn test_scheduler_metrics() {
        let registry = Registry::new();
        let metrics = SchedulerMetrics::new(&registry).unwrap();

        metrics
            .scheduling_attempts_total
            .with_label_values(&["success"])
            .inc();
        assert_eq!(
            metrics
                .scheduling_attempts_total
                .with_label_values(&["success"])
                .get(),
            1.0
        );
    }

    #[test]
    fn test_metrics_registry() {
        let registry = MetricsRegistry::new()
            .with_api_server_metrics()
            .unwrap()
            .with_scheduler_metrics()
            .unwrap();

        assert!(registry.api_server.is_some());
        assert!(registry.scheduler.is_some());

        // Should be able to gather metrics
        let output = registry.gather();
        assert!(!output.is_empty());
    }
}
