//! The kubelet's `/metrics/resource` endpoint.
//!
//! Ports `pkg/kubelet/metrics/collectors/resource_metrics.go`
//! (`resourceMetricsCollector.CollectWithStability` and the `collect*`
//! helpers) and its registration in `pkg/kubelet/server/server.go`
//! `InstallAuthRequiredHandlers` (`/metrics/resource`, auth required).
//!
//! Deliberate deviation: upstream's `SummaryProvider.GetCPUAndMemoryStats`
//! reads node stats from cadvisor's root container and pod stats from the CRI
//! stats provider (`pkg/kubelet/server/stats/summary.go`,
//! `pkg/kubelet/stats/cri_stats_provider.go`). This kubelet has no cadvisor,
//! so the pod/container stats come from CRI `ListContainerStats` (grouped by
//! the pod name/namespace container labels, pod totals are the sum of their
//! containers) and node stats from `/proc/stat` and `/proc/meminfo`.
//! Consequently `/metrics/cadvisor` is not served.

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use rusternetes_cri::CriClient;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;

use crate::labels::{CONTAINER_NAME_LABEL, POD_NAMESPACE_LABEL, POD_NAME_LABEL};

/// Paths this module serves, for `/statusz` `RegisteredHandlePaths()`.
pub const PATHS: [&str; 1] = ["/metrics/resource"];

/// A sampled value with the time (unix nanoseconds) it was collected at.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Sample {
    pub time_ns: i64,
    pub value: u64,
}

/// `summary.CPUStats`/`MemoryStats`/`SwapStats` reduced to what the
/// collector reads.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UsageStats {
    /// `CPU.UsageCoreNanoSeconds`.
    pub cpu_usage_core_ns: Option<Sample>,
    /// `Memory.WorkingSetBytes`.
    pub memory_working_set: Option<Sample>,
    /// `Swap.SwapUsageBytes`.
    pub swap_usage: Option<Sample>,
    /// `Swap.SwapAvailableBytes` (containers only).
    pub swap_available: Option<Sample>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerSummary {
    pub name: String,
    /// `StartTime` in unix nanoseconds.
    pub start_time_ns: i64,
    pub stats: UsageStats,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PodSummary {
    pub name: String,
    pub namespace: String,
    pub containers: Vec<ContainerSummary>,
    pub stats: UsageStats,
}

/// `summary.Summary` as returned by `GetCPUAndMemoryStats`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Summary {
    pub node: UsageStats,
    pub pods: Vec<PodSummary>,
}

struct Metric<'a> {
    name: &'a str,
    help: &'a str,
    kind: &'a str,
}

const NODE_CPU: Metric = Metric {
    name: "node_cpu_usage_seconds_total",
    help: "Cumulative cpu time consumed by the node in core-seconds",
    kind: "counter",
};
const NODE_MEMORY: Metric = Metric {
    name: "node_memory_working_set_bytes",
    help: "Current working set of the node in bytes",
    kind: "gauge",
};
const NODE_SWAP: Metric = Metric {
    name: "node_swap_usage_bytes",
    help: "Current swap usage of the node in bytes. Reported only on non-windows systems",
    kind: "gauge",
};
const CONTAINER_START: Metric = Metric {
    name: "container_start_time_seconds",
    help: "Start time of the container since unix epoch in seconds",
    kind: "gauge",
};
const CONTAINER_CPU: Metric = Metric {
    name: "container_cpu_usage_seconds_total",
    help: "Cumulative cpu time consumed by the container in core-seconds",
    kind: "counter",
};
const CONTAINER_MEMORY: Metric = Metric {
    name: "container_memory_working_set_bytes",
    help: "Current working set of the container in bytes",
    kind: "gauge",
};
const CONTAINER_SWAP: Metric = Metric {
    name: "container_swap_usage_bytes",
    help:
        "Current amount of the container swap usage in bytes. Reported only on non-windows systems",
    kind: "gauge",
};
const CONTAINER_SWAP_LIMIT: Metric = Metric {
    name: "container_swap_limit_bytes",
    help:
        "Current amount of the container swap limit in bytes. Reported only on non-windows systems",
    kind: "gauge",
};
const POD_CPU: Metric = Metric {
    name: "pod_cpu_usage_seconds_total",
    help: "Cumulative cpu time consumed by the pod in core-seconds",
    kind: "counter",
};
const POD_MEMORY: Metric = Metric {
    name: "pod_memory_working_set_bytes",
    help: "Current working set of the pod in bytes",
    kind: "gauge",
};
const POD_SWAP: Metric = Metric {
    name: "pod_swap_usage_bytes",
    help: "Current amount of the pod swap usage in bytes. Reported only on non-windows systems",
    kind: "gauge",
};
const SCRAPE_ERROR: Metric = Metric {
    name: "scrape_error",
    help: "1 if there was an error while getting container metrics, 0 otherwise",
    kind: "gauge",
};
const RESOURCE_SCRAPE_ERROR: Metric = Metric {
    name: "resource_scrape_error",
    help: "1 if there was an error while getting container metrics, 0 otherwise",
    kind: "gauge",
};

/// Series grouped per metric so each family's HELP/TYPE is emitted once, as
/// the Prometheus text format requires.
#[derive(Default)]
struct Families<'a> {
    order: Vec<&'a str>,
    by_name: HashMap<&'a str, (&'a Metric<'a>, Vec<String>)>,
}

impl<'a> Families<'a> {
    /// `metrics.NewLazyConstMetric`, with `NewLazyMetricWithTimestamp` when
    /// `time_ns` is given (millisecond timestamp, as the Go client exposes).
    fn push(
        &mut self,
        m: &'a Metric<'a>,
        labels: &[(&str, &str)],
        value: f64,
        time_ns: Option<i64>,
    ) {
        let mut line = String::from(m.name);
        if !labels.is_empty() {
            line.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    line.push(',');
                }
                let _ = write!(line, "{k}=\"{}\"", escape_label(v));
            }
            line.push('}');
        }
        let _ = write!(line, " {value}");
        if let Some(t) = time_ns {
            let _ = write!(line, " {}", t / 1_000_000);
        }
        let entry = self.by_name.entry(m.name).or_insert_with(|| {
            self.order.push(m.name);
            (m, Vec::new())
        });
        entry.1.push(line);
    }

    fn render(&self) -> String {
        let mut out = String::new();
        for name in &self.order {
            let (m, lines) = &self.by_name[name];
            let _ = writeln!(out, "# HELP {} {}", m.name, m.help);
            let _ = writeln!(out, "# TYPE {} {}", m.name, m.kind);
            for l in lines {
                out.push_str(l);
                out.push('\n');
            }
        }
        out
    }
}

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

const NANOS_PER_SECOND: f64 = 1e9;

/// `resourceMetricsCollector.CollectWithStability`: the scrape error gauges
/// are always emitted (the deferred sends); the rest only on success.
pub fn render_metrics(summary: Option<&Summary>) -> String {
    let mut f = Families::default();
    let error_count = if summary.is_some() { 0.0 } else { 1.0 };
    if let Some(s) = summary {
        collect_node(&mut f, &s.node);
        for pod in &s.pods {
            for c in &pod.containers {
                collect_container(&mut f, pod, c);
            }
            collect_pod(&mut f, pod);
        }
    }
    f.push(&SCRAPE_ERROR, &[], error_count, None);
    f.push(&RESOURCE_SCRAPE_ERROR, &[], error_count, None);
    f.render()
}

fn collect_node<'a>(f: &mut Families<'a>, s: &UsageStats) {
    if let Some(c) = s.cpu_usage_core_ns {
        f.push(
            &NODE_CPU,
            &[],
            c.value as f64 / NANOS_PER_SECOND,
            Some(c.time_ns),
        );
    }
    if let Some(m) = s.memory_working_set {
        f.push(&NODE_MEMORY, &[], m.value as f64, Some(m.time_ns));
    }
    if let Some(w) = s.swap_usage {
        f.push(&NODE_SWAP, &[], w.value as f64, Some(w.time_ns));
    }
}

fn collect_container<'a>(f: &mut Families<'a>, pod: &PodSummary, c: &ContainerSummary) {
    let l = [
        ("container", c.name.as_str()),
        ("pod", pod.name.as_str()),
        ("namespace", pod.namespace.as_str()),
    ];
    // collectContainerStartTime: `s.StartTime.Unix() <= 0` is skipped.
    if c.start_time_ns / 1_000_000_000 > 0 {
        f.push(
            &CONTAINER_START,
            &l,
            c.start_time_ns as f64 / NANOS_PER_SECOND,
            None,
        );
    }
    if let Some(v) = c.stats.cpu_usage_core_ns {
        f.push(
            &CONTAINER_CPU,
            &l,
            v.value as f64 / NANOS_PER_SECOND,
            Some(v.time_ns),
        );
    }
    if let Some(v) = c.stats.memory_working_set {
        f.push(&CONTAINER_MEMORY, &l, v.value as f64, Some(v.time_ns));
    }
    // collectContainerSwapMetrics: limit = available + usage (usage 0 if unset).
    let usage = c.stats.swap_usage.map(|s| s.value as f64).unwrap_or(0.0);
    if let Some(v) = c.stats.swap_usage {
        f.push(&CONTAINER_SWAP, &l, v.value as f64, Some(v.time_ns));
    }
    if let Some(v) = c.stats.swap_available {
        f.push(
            &CONTAINER_SWAP_LIMIT,
            &l,
            v.value as f64 + usage,
            Some(v.time_ns),
        );
    }
}

fn collect_pod<'a>(f: &mut Families<'a>, pod: &PodSummary) {
    let l = [
        ("pod", pod.name.as_str()),
        ("namespace", pod.namespace.as_str()),
    ];
    if let Some(v) = pod.stats.cpu_usage_core_ns {
        f.push(
            &POD_CPU,
            &l,
            v.value as f64 / NANOS_PER_SECOND,
            Some(v.time_ns),
        );
    }
    if let Some(v) = pod.stats.memory_working_set {
        f.push(&POD_MEMORY, &l, v.value as f64, Some(v.time_ns));
    }
    if let Some(v) = pod.stats.swap_usage {
        f.push(&POD_SWAP, &l, v.value as f64, Some(v.time_ns));
    }
}

fn sample(time_ns: i64, v: Option<u64>) -> Option<Sample> {
    v.map(|value| Sample { time_ns, value })
}

/// Sum optional samples; `None` only when no input had a value, keeping the
/// newest timestamp (`addCRIPodCPUStats` and friends take the container's).
fn sum_samples(items: impl Iterator<Item = Option<Sample>>) -> Option<Sample> {
    items.flatten().fold(None, |acc, s| {
        Some(match acc {
            None => s,
            Some(a) => Sample {
                time_ns: a.time_ns.max(s.time_ns),
                value: a.value + s.value,
            },
        })
    })
}

/// Convert CRI container stats (plus container start times by id) into the
/// summary's pod/container layout. Containers without pod labels are not
/// kubelet-managed pods and are dropped, as upstream's `buildPodStats` only
/// reports containers belonging to a pod sandbox.
pub fn pods_from_cri(
    stats: &[rusternetes_cri::v1::ContainerStats],
    start_times_ns: &HashMap<String, i64>,
) -> Vec<PodSummary> {
    let mut pods: BTreeMap<(String, String), PodSummary> = BTreeMap::new();
    for s in stats {
        let Some(attrs) = &s.attributes else { continue };
        let (Some(pod), Some(ns)) = (
            attrs.labels.get(POD_NAME_LABEL),
            attrs.labels.get(POD_NAMESPACE_LABEL),
        ) else {
            continue;
        };
        let name = attrs
            .labels
            .get(CONTAINER_NAME_LABEL)
            .cloned()
            .or_else(|| attrs.metadata.as_ref().map(|m| m.name.clone()))
            .unwrap_or_default();
        let cpu = s.cpu.as_ref().and_then(|c| {
            sample(
                c.timestamp,
                c.usage_core_nano_seconds.as_ref().map(|v| v.value),
            )
        });
        let mem = s
            .memory
            .as_ref()
            .and_then(|m| sample(m.timestamp, m.working_set_bytes.as_ref().map(|v| v.value)));
        let (swap_usage, swap_avail) = match &s.swap {
            Some(w) => (
                sample(w.timestamp, w.swap_usage_bytes.as_ref().map(|v| v.value)),
                sample(
                    w.timestamp,
                    w.swap_available_bytes.as_ref().map(|v| v.value),
                ),
            ),
            None => (None, None),
        };
        let entry = pods
            .entry((ns.clone(), pod.clone()))
            .or_insert_with(|| PodSummary {
                name: pod.clone(),
                namespace: ns.clone(),
                ..Default::default()
            });
        entry.containers.push(ContainerSummary {
            name,
            start_time_ns: start_times_ns.get(&attrs.id).copied().unwrap_or_default(),
            stats: UsageStats {
                cpu_usage_core_ns: cpu,
                memory_working_set: mem,
                swap_usage,
                swap_available: swap_avail,
            },
        });
    }
    let mut out: Vec<PodSummary> = pods.into_values().collect();
    for p in &mut out {
        p.stats = UsageStats {
            cpu_usage_core_ns: sum_samples(p.containers.iter().map(|c| c.stats.cpu_usage_core_ns)),
            memory_working_set: sum_samples(
                p.containers.iter().map(|c| c.stats.memory_working_set),
            ),
            swap_usage: sum_samples(p.containers.iter().map(|c| c.stats.swap_usage)),
            swap_available: None,
        };
    }
    out
}

/// Node CPU time in core-nanoseconds from the aggregate `cpu` line of
/// `/proc/stat`: every state except idle and iowait (cadvisor's root
/// container usage is the cgroup cpu usage, i.e. busy time).
pub fn parse_proc_stat_cpu_ns(proc_stat: &str, clock_ticks_per_sec: u64) -> Option<u64> {
    let line = proc_stat.lines().find(|l| l.starts_with("cpu "))?;
    let f: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .map(|v| v.parse().ok())
        .collect::<Option<_>>()?;
    // user nice system idle iowait irq softirq steal
    let busy = f.first()?
        + f.get(1)?
        + f.get(2)?
        + f.get(5).copied().unwrap_or(0)
        + f.get(6).copied().unwrap_or(0)
        + f.get(7).copied().unwrap_or(0);
    Some(busy * (1_000_000_000 / clock_ticks_per_sec.max(1)))
}

/// `(working_set_bytes, swap_usage_bytes)` from `/proc/meminfo`: working set
/// is `MemTotal - MemAvailable`, swap is `SwapTotal - SwapFree`.
pub fn parse_meminfo(meminfo: &str) -> Option<(u64, u64)> {
    let kb = |key: &str| -> Option<u64> {
        meminfo
            .lines()
            .find_map(|l| l.strip_prefix(key)?.strip_prefix(':'))
            .and_then(|v| v.split_whitespace().next())
            .and_then(|v| v.parse::<u64>().ok())
            .map(|v| v * 1024)
    };
    let working = kb("MemTotal")?.saturating_sub(kb("MemAvailable")?);
    let swap = kb("SwapTotal")?.saturating_sub(kb("SwapFree")?);
    Some((working, swap))
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_default()
}

fn node_stats() -> UsageStats {
    let now = now_ns();
    let mut s = UsageStats::default();
    if let Ok(t) = std::fs::read_to_string("/proc/stat") {
        // USER_HZ is 100 on every Linux ABI we run on.
        s.cpu_usage_core_ns = sample(now, parse_proc_stat_cpu_ns(&t, 100));
    }
    if let Ok(t) = std::fs::read_to_string("/proc/meminfo") {
        if let Some((work, swap)) = parse_meminfo(&t) {
            s.memory_working_set = sample(now, Some(work));
            s.swap_usage = sample(now, Some(swap));
        }
    }
    s
}

/// `GetCPUAndMemoryStats`: CRI-backed pod stats plus procfs node stats.
pub async fn get_cpu_and_memory_stats(cri: &mut CriClient) -> rusternetes_cri::Result<Summary> {
    let stats = cri.list_container_stats(None).await?;
    let containers = cri.list_containers(None).await?;
    let start_times: HashMap<String, i64> = containers
        .into_iter()
        .map(|c| (c.id, c.created_at))
        .collect();
    Ok(Summary {
        node: node_stats(),
        pods: pods_from_cri(&stats, &start_times),
    })
}

async fn handle_resource(cri: Option<CriClient>) -> Response {
    let summary = match cri {
        Some(mut cri) => match get_cpu_and_memory_stats(&mut cri).await {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::error!(
                    "Error getting summary for resourceMetric prometheus endpoint: {e}"
                );
                None
            }
        },
        None => None,
    };
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render_metrics(summary.as_ref()),
    )
        .into_response()
}

/// Route for `/metrics/resource`. `cri` is the runtime client; `None` (no
/// runtime available) reports `scrape_error 1`.
pub fn router(cri: Option<CriClient>) -> Router {
    let cri = Arc::new(cri);
    Router::new().route(
        "/metrics/resource",
        any(move || {
            let cri = cri.clone();
            async move { handle_resource((*cri).clone()).await }
        }),
    )
}
