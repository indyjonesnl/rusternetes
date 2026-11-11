//! Tests for `/metrics/cadvisor` (`cri_metrics.go` collector).
use k3rs_cri::v1;
use k3rs_kubelet::server_cri_metrics::*;

fn desc(name: &str, help: &str, keys: &[&str]) -> v1::MetricDescriptor {
    v1::MetricDescriptor {
        name: name.into(),
        help: help.into(),
        label_keys: keys.iter().map(|s| s.to_string()).collect(),
    }
}

fn metric(name: &str, ts: i64, ty: v1::MetricType, labels: &[&str], v: u64) -> v1::Metric {
    v1::Metric {
        name: name.into(),
        timestamp: ts,
        metric_type: ty as i32,
        label_values: labels.iter().map(|s| s.to_string()).collect(),
        value: Some(v1::UInt64Value { value: v }),
    }
}

#[test]
fn renders_pod_and_container_metrics() {
    let c = CriMetricsCollector::new(&[
        desc(
            "container_cpu_usage_seconds_total",
            "cpu",
            &["container", "pod"],
        ),
        desc("container_memory_usage_bytes", "mem", &["container", "pod"]),
    ]);
    let pods = vec![v1::PodSandboxMetrics {
        pod_sandbox_id: "s1".into(),
        metrics: vec![metric(
            "container_memory_usage_bytes",
            0,
            v1::MetricType::Gauge,
            &["", "p1"],
            7,
        )],
        container_metrics: vec![v1::ContainerMetrics {
            container_id: "c1".into(),
            metrics: vec![metric(
                "container_cpu_usage_seconds_total",
                5_000_000_000,
                v1::MetricType::Counter,
                &["app", "p1"],
                42,
            )],
        }],
    }];
    assert_eq!(
        c.render(&pods),
        "# HELP container_cpu_usage_seconds_total cpu\n\
         # TYPE container_cpu_usage_seconds_total counter\n\
         container_cpu_usage_seconds_total{container=\"app\",pod=\"p1\"} 42 5000\n\
         # HELP container_memory_usage_bytes mem\n\
         # TYPE container_memory_usage_bytes gauge\n\
         container_memory_usage_bytes{container=\"\",pod=\"p1\"} 7\n"
    );
}

#[test]
fn skips_unknown_descriptor_and_label_mismatch() {
    let c = CriMetricsCollector::new(&[desc("m", "h", &["a", "b"])]);
    let pods = vec![v1::PodSandboxMetrics {
        metrics: vec![
            metric("unknown", 0, v1::MetricType::Gauge, &[], 1),
            metric("m", 0, v1::MetricType::Gauge, &["only-one"], 1),
        ],
        ..Default::default()
    }];
    assert_eq!(c.render(&pods), "");
}

#[test]
fn empty_collector_renders_nothing_and_help_is_escaped() {
    assert_eq!(CriMetricsCollector::default().render(&[]), "");
    let c = CriMetricsCollector::new(&[desc("m", "a\\b\nc", &[])]);
    let pods = vec![v1::PodSandboxMetrics {
        metrics: vec![metric("m", 0, v1::MetricType::Counter, &[], 3)],
        ..Default::default()
    }];
    assert_eq!(
        c.render(&pods),
        "# HELP m a\\\\b\\nc\n# TYPE m counter\nm 3\n"
    );
}
