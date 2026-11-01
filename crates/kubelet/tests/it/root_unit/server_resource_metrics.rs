//! Tests for `/metrics/resource` (`resource_metrics.go`).
use rusternetes_cri::v1;
use rusternetes_kubelet::labels::{CONTAINER_NAME_LABEL, POD_NAMESPACE_LABEL, POD_NAME_LABEL};
use rusternetes_kubelet::server_resource_metrics::*;
use std::collections::HashMap;

fn u(v: u64) -> Option<v1::UInt64Value> {
    Some(v1::UInt64Value { value: v })
}

fn cri_stats(id: &str, pod: &str, ns: &str, name: &str, cpu: u64, mem: u64) -> v1::ContainerStats {
    v1::ContainerStats {
        attributes: Some(v1::ContainerAttributes {
            id: id.into(),
            labels: HashMap::from([
                (POD_NAME_LABEL.to_string(), pod.to_string()),
                (POD_NAMESPACE_LABEL.to_string(), ns.to_string()),
                (CONTAINER_NAME_LABEL.to_string(), name.to_string()),
            ]),
            ..Default::default()
        }),
        cpu: Some(v1::CpuUsage {
            timestamp: 5_000_000_000,
            usage_core_nano_seconds: u(cpu),
            ..Default::default()
        }),
        memory: Some(v1::MemoryUsage {
            timestamp: 6_000_000_000,
            working_set_bytes: u(mem),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn scrape_error_only_when_no_summary() {
    let out = render_metrics(None);
    assert!(out.contains("\nscrape_error 1\n") || out.contains("scrape_error 1\n"));
    assert!(out.contains("resource_scrape_error 1\n"));
    assert!(!out.contains("node_cpu_usage_seconds_total"));
}

#[test]
fn pods_sum_containers_and_render() {
    let stats = vec![
        cri_stats("c1", "p", "ns", "a", 2_000_000_000, 100),
        cri_stats("c2", "p", "ns", "b", 1_000_000_000, 50),
    ];
    let starts = HashMap::from([("c1".to_string(), 1_700_000_000_000_000_000i64)]);
    let pods = pods_from_cri(&stats, &starts);
    assert_eq!(pods.len(), 1);
    assert_eq!(
        pods[0].stats.cpu_usage_core_ns.unwrap().value,
        3_000_000_000
    );
    assert_eq!(pods[0].stats.memory_working_set.unwrap().value, 150);

    let out = render_metrics(Some(&Summary {
        node: UsageStats {
            cpu_usage_core_ns: Some(Sample {
                time_ns: 7_000_000_000,
                value: 1_500_000_000,
            }),
            memory_working_set: Some(Sample {
                time_ns: 7_000_000_000,
                value: 42,
            }),
            ..Default::default()
        },
        pods,
    }));
    assert!(out.contains("# TYPE node_cpu_usage_seconds_total counter\n"));
    assert!(out.contains("node_cpu_usage_seconds_total 1.5 7000\n"));
    assert!(out.contains("node_memory_working_set_bytes 42 7000\n"));
    assert!(out.contains(
        "container_cpu_usage_seconds_total{container=\"a\",pod=\"p\",namespace=\"ns\"} 2 5000\n"
    ));
    assert!(out.contains(
        "container_memory_working_set_bytes{container=\"b\",pod=\"p\",namespace=\"ns\"} 50 6000\n"
    ));
    assert!(out.contains("pod_cpu_usage_seconds_total{pod=\"p\",namespace=\"ns\"} 3 5000\n"));
    assert!(out.contains("pod_memory_working_set_bytes{pod=\"p\",namespace=\"ns\"} 150 6000\n"));
    // c1 has a start time, c2 (unknown id) is skipped as StartTime.Unix() <= 0.
    assert_eq!(out.matches("container_start_time_seconds{").count(), 1);
    assert!(out.contains(
        "container_start_time_seconds{container=\"a\",pod=\"p\",namespace=\"ns\"} 1700000000\n"
    ));
    assert!(out.contains("resource_scrape_error 0\n"));
    // One HELP per family.
    assert_eq!(
        out.matches("# HELP container_cpu_usage_seconds_total")
            .count(),
        1
    );
}

#[test]
fn containers_without_pod_labels_are_dropped() {
    let mut s = cri_stats("c", "p", "ns", "a", 1, 1);
    s.attributes.as_mut().unwrap().labels.clear();
    assert!(pods_from_cri(&[s], &HashMap::new()).is_empty());
}

#[test]
fn swap_limit_is_available_plus_usage() {
    let pods = vec![PodSummary {
        name: "p".into(),
        namespace: "ns".into(),
        containers: vec![ContainerSummary {
            name: "a".into(),
            start_time_ns: 0,
            stats: UsageStats {
                swap_usage: Some(Sample {
                    time_ns: 1_000_000,
                    value: 10,
                }),
                swap_available: Some(Sample {
                    time_ns: 1_000_000,
                    value: 90,
                }),
                ..Default::default()
            },
        }],
        stats: UsageStats::default(),
    }];
    let out = render_metrics(Some(&Summary {
        node: UsageStats::default(),
        pods,
    }));
    assert!(out
        .contains("container_swap_usage_bytes{container=\"a\",pod=\"p\",namespace=\"ns\"} 10 1\n"));
    assert!(out.contains(
        "container_swap_limit_bytes{container=\"a\",pod=\"p\",namespace=\"ns\"} 100 1\n"
    ));
}

#[test]
fn parses_proc_files() {
    let stat = "cpu  100 10 50 1000 20 5 5 0 0 0\ncpu0 1 1 1 1 1 1 1 0 0 0\n";
    // busy = 100+10+50+5+5+0 = 170 ticks at 100Hz = 1.7s
    assert_eq!(parse_proc_stat_cpu_ns(stat, 100), Some(1_700_000_000));
    let mem = "MemTotal: 1000 kB\nMemAvailable: 400 kB\nSwapTotal: 200 kB\nSwapFree: 150 kB\n";
    assert_eq!(parse_meminfo(mem), Some((600 * 1024, 50 * 1024)));
    assert_eq!(parse_meminfo("MemTotal: 1 kB\n"), None);
}

#[tokio::test]
async fn route_without_runtime_reports_scrape_error() {
    use tower::ServiceExt;
    let app = router(None);
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/metrics/resource")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&body).contains("resource_scrape_error 1"));
}
