//! The kubelet's `/metrics/cadvisor` endpoint, served from CRI metrics.
//!
//! Ports `pkg/kubelet/metrics/collectors/cri_metrics.go`
//! (`criMetricsCollector`: `NewCRIMetricsCollector`, `CollectWithStability`,
//! `criDescToProm`, `criMetricToProm`, `criTypeToProm`) and its registration
//! in `pkg/kubelet/server/server.go` `InstallDefaultHandlers`
//! (`PodAndContainerStatsFromCRI` branch, `cadvisorMetricsPath`), which fetches
//! the descriptors with CRI `ListMetricDescriptors` and the samples with CRI
//! `ListPodSandboxMetrics`.
//!
//! Deliberate deviations (this kubelet has no cAdvisor):
//! - Upstream's default (`PodAndContainerStatsFromCRI` is alpha, off) serves
//!   cAdvisor's `NewPrometheusCollector`; here the CRI branch is the only one.
//!   The `NewPrometheusMachineCollector` (`machine_*` metrics) needs cAdvisor
//!   `MachineInfo` and is not served.
//! - Upstream fetches descriptors once at registration and never retries on
//!   error; here they are fetched on the first scrape that succeeds, so a
//!   runtime that was not ready at startup does not empty the endpoint forever.
//! - Gather ordering follows the Prometheus registry: families sorted by name,
//!   series sorted by label values.

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use rusternetes_cri::{v1, CriClient};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::server_resource_metrics::escape_label;

/// Paths this module serves, for `/statusz` `RegisteredHandlePaths()`.
pub const PATHS: [&str; 1] = ["/metrics/cadvisor"];

/// `metrics.Desc` built by `criDescToProm`: label keys are variable labels.
#[derive(Debug, Clone, PartialEq)]
pub struct Desc {
    pub name: String,
    pub help: String,
    pub label_keys: Vec<String>,
}

/// `criMetricsCollector`.
#[derive(Debug, Clone, Default)]
pub struct CriMetricsCollector {
    descriptors: HashMap<String, Desc>,
}

/// `criDescToProm`.
fn cri_desc_to_prom(m: &v1::MetricDescriptor) -> Desc {
    Desc {
        name: m.name.clone(),
        help: m.help.clone(),
        label_keys: m.label_keys.clone(),
    }
}

fn escape_help(h: &str) -> String {
    h.replace('\\', "\\\\").replace('\n', "\\n")
}

type Series<'a> = (&'a Desc, &'static str, Vec<String>, String);

impl CriMetricsCollector {
    /// The success path of `NewCRIMetricsCollector`: one descriptor per name.
    pub fn new(descs: &[v1::MetricDescriptor]) -> Self {
        Self {
            descriptors: descs
                .iter()
                .map(|d| (d.name.clone(), cri_desc_to_prom(d)))
                .collect(),
        }
    }

    /// `criMetricToProm` plus the text exposition of the const metric;
    /// `None` where Go returns an error.
    fn metric_to_prom(&self, m: &v1::Metric) -> Option<Series<'_>> {
        // Descriptor not present in pre-populated list of descriptors.
        let desc = self.descriptors.get(&m.name)?;
        let kind = match v1::MetricType::try_from(m.metric_type).ok()? {
            v1::MetricType::Counter => "counter",
            v1::MetricType::Gauge => "gauge",
        };
        // `NewConstMetric` fails on a label key/value count mismatch.
        if desc.label_keys.len() != m.label_values.len() {
            tracing::error!(
                "Error getting CRI prometheus metric descriptor={} \
                 (expected {} label values, got {})",
                desc.name,
                desc.label_keys.len(),
                m.label_values.len()
            );
            return None;
        }
        let value = m.value.as_ref().map(|v| v.value).unwrap_or(0) as f64;
        let mut line = desc.name.clone();
        if !desc.label_keys.is_empty() {
            line.push('{');
            for (i, (k, v)) in desc.label_keys.iter().zip(&m.label_values).enumerate() {
                if i > 0 {
                    line.push(',');
                }
                let _ = write!(line, "{k}=\"{}\"", escape_label(v));
            }
            line.push('}');
        }
        let _ = write!(line, " {value}");
        // Timestamp 0 means the runtime did not cache the result
        // (`NewLazyMetricWithTimestamp` only otherwise).
        if m.timestamp != 0 {
            let _ = write!(line, " {}", m.timestamp / 1_000_000);
        }
        Some((desc, kind, m.label_values.clone(), line))
    }

    /// `CollectWithStability` followed by the registry's text exposition.
    pub fn render(&self, pod_metrics: &[v1::PodSandboxMetrics]) -> String {
        type Family<'a> = (&'a Desc, &'static str, BTreeMap<Vec<String>, String>);
        let mut fams: BTreeMap<&str, Family<'_>> = BTreeMap::new();
        let all = pod_metrics.iter().flat_map(|p| {
            p.metrics
                .iter()
                .chain(p.container_metrics.iter().flat_map(|c| c.metrics.iter()))
        });
        for m in all {
            if let Some((desc, kind, key, line)) = self.metric_to_prom(m) {
                fams.entry(desc.name.as_str())
                    .or_insert_with(|| (desc, kind, BTreeMap::new()))
                    .2
                    .insert(key, line);
            }
        }
        let mut out = String::new();
        for (name, (desc, kind, series)) in fams {
            let _ = writeln!(out, "# HELP {name} {}", escape_help(&desc.help));
            let _ = writeln!(out, "# TYPE {name} {kind}");
            for l in series.values() {
                out.push_str(l);
                out.push('\n');
            }
        }
        out
    }
}

struct State {
    cri: Option<CriClient>,
    collector: Mutex<Option<Arc<CriMetricsCollector>>>,
}

async fn handle(state: Arc<State>) -> Response {
    let body = match state.cri.clone() {
        Some(mut cri) => scrape(&state, &mut cri).await,
        None => String::new(),
    };
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

async fn scrape(state: &State, cri: &mut CriClient) -> String {
    let timeout = Some(cri.request_timeout());
    let collector = {
        let mut guard = state.collector.lock().await;
        if guard.is_none() {
            let req = rusternetes_cri::with_deadline(v1::ListMetricDescriptorsRequest {}, timeout);
            match cri.runtime().list_metric_descriptors(req).await {
                Ok(r) => {
                    *guard = Some(Arc::new(CriMetricsCollector::new(
                        &r.into_inner().descriptors,
                    )))
                }
                Err(e) => tracing::error!("Error reading MetricDescriptors: {e}"),
            }
        }
        guard.clone()
    };
    let Some(collector) = collector else {
        return String::new();
    };
    let req = rusternetes_cri::with_deadline(v1::ListPodSandboxMetricsRequest {}, timeout);
    match cri.runtime().list_pod_sandbox_metrics(req).await {
        Ok(r) => collector.render(&r.into_inner().pod_metrics),
        Err(e) => {
            tracing::error!("Failed to get pod metrics: {e}");
            String::new()
        }
    }
}

/// Route for `/metrics/cadvisor`.
pub fn router(cri: Option<CriClient>) -> Router {
    let state = Arc::new(State {
        cri,
        collector: Mutex::new(None),
    });
    Router::new().route(
        "/metrics/cadvisor",
        any(move || {
            let state = state.clone();
            async move { handle(state).await }
        }),
    )
}
