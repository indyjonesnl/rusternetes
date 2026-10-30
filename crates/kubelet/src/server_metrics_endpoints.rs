//! The kubelet's `/metrics/slis` and `/metrics/probes` endpoints.
//!
//! Ports, from `pkg/kubelet/server/server.go`:
//! - `InstallAuthNotRequiredHandlers` `:438` `slis.SLIMetricsWithReset{}.Install(s.restfulCont)`
//!   (`staging/src/k8s.io/component-base/metrics/prometheus/slis/routes.go`,
//!   `metrics.go`, `registry.go`; `HandlerWithReset` in
//!   `component-base/metrics/http.go:77-87`: `DELETE` resets the registry,
//!   anything else serves it).
//! - `InstallDefaultHandlers` `:501-510`: the prober registry holds
//!   `RegisterProcessStartTime`, `prober.ProberResults` and
//!   `prober.ProberDuration` (`pkg/kubelet/prober/prober_manager.go:41-68`),
//!   observed per probe by `worker.doProbe` (`prober/worker.go:355-365`).
//!
//! Series removal when a probe worker stops (`worker.go:176-184`) is not
//! ported: this kubelet has no per-container probe worker lifecycle.

use axum::{
    http::{header, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::sync::LazyLock;
use std::time::Instant;

/// Paths this module serves, for `/statusz` `RegisteredHandlePaths()`.
pub const PATHS: [&str; 2] = ["/metrics/slis", "/metrics/probes"];

/// `slis.HealthcheckStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthcheckStatus {
    Success,
    Error,
}

/// `RegisterProcessStartTime` (`component-base/metrics/processstarttime.go`):
/// boot time plus process start ticks, falling back to now.
fn process_start_time_gauge() -> GaugeVec {
    let gauge = GaugeVec::new(
        Opts::new(
            "process_start_time_seconds",
            "[ALPHA] Start time of the process since unix epoch in seconds.",
        ),
        &[],
    )
    .expect("static process start time definition");
    let started = process_start_time().unwrap_or_else(|| chrono::Utc::now().timestamp() as f64);
    gauge.with_label_values(&[] as &[&str]).set(started);
    gauge
}

fn process_start_time() -> Option<f64> {
    let process = std::fs::read_to_string("/proc/self/stat").ok()?;
    let (_, fields) = process.rsplit_once(')')?;
    let ticks: f64 = fields.split_whitespace().nth(19)?.parse().ok()?;
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let boot: f64 = stat
        .lines()
        .find_map(|line| line.strip_prefix("btime "))?
        .parse()
        .ok()?;
    Some(boot + ticks / 100.0)
}

/// `slis.Registry` with `slis.Register` applied (`metrics.go:33-58`,
/// `:60-65`). Both metrics are `STABLE`, so no help prefix beyond `[STABLE] `.
struct Slis {
    registry: Registry,
    healthcheck: GaugeVec,
    healthchecks_total: IntCounterVec,
}

static SLIS: LazyLock<Slis> = LazyLock::new(|| {
    let registry = Registry::new();
    let healthcheck = GaugeVec::new(
        Opts::new(
            "kubernetes_healthcheck",
            "[STABLE] This metric records the result of a single healthcheck.",
        ),
        &["name", "type"],
    )
    .expect("static healthcheck definition");
    let healthchecks_total = IntCounterVec::new(
        Opts::new(
            "kubernetes_healthchecks_total",
            "[STABLE] This metric records the results of all healthcheck.",
        ),
        &["name", "type", "status"],
    )
    .expect("static healthchecks_total definition");
    registry
        .register(Box::new(healthcheck.clone()))
        .expect("unique healthcheck");
    registry
        .register(Box::new(healthchecks_total.clone()))
        .expect("unique healthchecks_total");
    registry
        .register(Box::new(process_start_time_gauge()))
        .expect("unique process start time");
    Slis {
        registry,
        healthcheck,
        healthchecks_total,
    }
});

/// `slis.ObserveHealthcheck` (`metrics.go:75-85`).
pub fn observe_healthcheck(name: &str, healthcheck_type: &str, status: HealthcheckStatus) {
    let slis = &*SLIS;
    let value = if status == HealthcheckStatus::Success {
        1.0
    } else {
        0.0
    };
    slis.healthcheck
        .with_label_values(&[name, healthcheck_type])
        .set(value);
    let status = match status {
        HealthcheckStatus::Success => "success",
        HealthcheckStatus::Error => "error",
    };
    slis.healthchecks_total
        .with_label_values(&[name, healthcheck_type, status])
        .inc();
}

/// `prober.ProbeType` strings (`prober/worker.go` `probeType.String()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeType {
    Readiness,
    Liveness,
    Startup,
}

impl ProbeType {
    fn as_str(self) -> &'static str {
        match self {
            ProbeType::Readiness => "Readiness",
            ProbeType::Liveness => "Liveness",
            ProbeType::Startup => "Startup",
        }
    }
}

/// `probeResultSuccessful/Failed/Unknown` (`worker.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeResult {
    Successful,
    Failed,
    /// Not produced yet: `probe_container` is boolean, upstream's Unknown
    /// comes from `results.Unknown` probers.
    #[allow(dead_code)]
    Unknown,
}

struct Prober {
    registry: Registry,
    results: IntCounterVec,
    duration: HistogramVec,
}

static PROBER: LazyLock<Prober> = LazyLock::new(|| {
    let registry = Registry::new();
    // `ProberResults` (`prober_manager.go:41-53`, BETA).
    let results = IntCounterVec::new(
        Opts::new(
            "prober_probe_total",
            "[BETA] Cumulative number of a liveness, readiness or startup probe for a container by result.",
        ),
        &["probe_type", "result", "container", "pod", "namespace", "pod_uid"],
    )
    .expect("static prober_probe_total definition");
    // `ProberDuration` (`prober_manager.go:57-68`, ALPHA, default buckets).
    let duration = HistogramVec::new(
        HistogramOpts::new(
            "prober_probe_duration_seconds",
            "[ALPHA] Duration in seconds for a probe response.",
        ),
        &["probe_type", "container", "pod", "namespace"],
    )
    .expect("static prober_probe_duration_seconds definition");
    registry
        .register(Box::new(process_start_time_gauge()))
        .expect("unique process start time");
    registry
        .register(Box::new(results.clone()))
        .expect("unique ProberResults");
    registry
        .register(Box::new(duration.clone()))
        .expect("unique ProberDuration");
    Prober {
        registry,
        results,
        duration,
    }
});

/// The metric part of `worker.doProbe` (`worker.go:355-365`): success counts
/// and times the probe, failure only counts, unknown counts and times it.
pub fn observe_probe(
    probe_type: ProbeType,
    result: ProbeResult,
    container: &str,
    pod: &str,
    namespace: &str,
    pod_uid: &str,
    start: Instant,
) {
    let prober = &*PROBER;
    let probe_type = probe_type.as_str();
    let result_label = match result {
        ProbeResult::Successful => "successful",
        ProbeResult::Failed => "failed",
        ProbeResult::Unknown => "unknown",
    };
    prober
        .results
        .with_label_values(&[probe_type, result_label, container, pod, namespace, pod_uid])
        .inc();
    if result != ProbeResult::Failed {
        prober
            .duration
            .with_label_values(&[probe_type, container, pod, namespace])
            .observe(start.elapsed().as_secs_f64());
    }
}

/// `compbasemetrics.HandlerFor(registry, HandlerOpts{})` body.
fn gather(registry: &Registry) -> Response {
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    match encoder.encode(&registry.gather(), &mut buf) {
        Ok(()) => (
            [(header::CONTENT_TYPE, encoder.format_type().to_string())],
            buf,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `SLIMetricsWithReset.Install`'s handler (`http.go:77-87`).
async fn handle_slis(method: Method) -> Response {
    if method == Method::DELETE {
        SLIS.healthcheck.reset();
        SLIS.healthchecks_total.reset();
        return "metrics reset\n".into_response();
    }
    gather(&SLIS.registry)
}

async fn handle_probes() -> Response {
    gather(&PROBER.registry)
}

/// Routes for `/metrics/slis` and `/metrics/probes`.
pub fn router() -> Router {
    Router::new()
        .route("/metrics/slis", any(handle_slis))
        .route("/metrics/probes", any(handle_probes))
}
