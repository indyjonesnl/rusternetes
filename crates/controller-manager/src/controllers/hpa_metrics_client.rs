//! Metrics source abstraction for the HPA controller.
//!
//! Mirrors k8s `pkg/controller/podautoscaler/metrics/interfaces.go`. The real
//! impl (`HttpMetricsClient`) queries the api-server metrics endpoints over
//! mTLS; tests inject `FakeMetricsClient`.
//!
//! Some surface (the `PodMetric::timestamp` field, the `pods_info` test helper)
//! is only exercised in tests, so the module keeps `allow(dead_code)` to stay
//! clean under `--all-targets` clippy on the binary crate.
#![allow(dead_code)]

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use rusternetes_common::quantity::parse_resource_value;
use rusternetes_common::resources::CrossVersionObjectReference;
use rusternetes_common::types::LabelSelector;
use std::collections::HashMap;

/// Per-pod metric reading. `value` is the raw metric in milli-units for cpu
/// (e.g. 500 = 500m) or whole units otherwise; `utilization` is the percentage
/// of the pod's request (resource metrics only).
#[derive(Debug, Clone)]
pub struct PodMetric {
    pub value: i64,
    /// Utilization percent vs request, resource metrics only.
    pub utilization: Option<i32>,
    pub timestamp: DateTime<Utc>,
    /// Upstream `PodMetric.Window` (metrics/interfaces.go): the sample window.
    /// Custom metrics default to `metricServerDefaultMetricWindow` (1m) when
    /// the item has no `windowSeconds` (metrics/client.go:136-139).
    pub window: std::time::Duration,
}

/// `metricServerDefaultMetricWindow` (pkg/controller/podautoscaler/metrics/client.go:42).
pub const METRIC_SERVER_DEFAULT_METRIC_WINDOW: std::time::Duration =
    std::time::Duration::from_secs(60);

/// Map of pod name -> metric reading.
pub type PodMetricsInfo = HashMap<String, PodMetric>;

#[async_trait]
pub trait MetricsClient: Send + Sync {
    /// Per-pod resource utilization (cpu/memory) for pods matching `selector`.
    async fn get_resource_metric(
        &self,
        resource: &str,
        namespace: &str,
        selector: &LabelSelector,
    ) -> Result<PodMetricsInfo>;

    /// Per-pod resource utilization for a single named container (cpu/memory),
    /// for the ContainerResource HPA metric type. Usage and the request
    /// denominator are both scoped to `container`.
    async fn get_container_resource_metric(
        &self,
        resource: &str,
        container: &str,
        namespace: &str,
        selector: &LabelSelector,
    ) -> Result<PodMetricsInfo>;

    /// Per-pod custom ("Pods" type) metric. `metric_selector` is
    /// `metav1.LabelSelectorAsSelector(spec.Pods.Metric.Selector).String()`
    /// (horizontal.go:478), sent as `metricLabelSelector`.
    async fn get_raw_metric(
        &self,
        metric: &str,
        namespace: &str,
        selector: &LabelSelector,
        metric_selector: &str,
    ) -> Result<PodMetricsInfo>;

    /// Single ("Object" type) metric value for a referenced object.
    /// `metric_selector` as for `get_raw_metric` (horizontal.go:468).
    async fn get_object_metric(
        &self,
        metric: &str,
        namespace: &str,
        object_ref: &CrossVersionObjectReference,
        metric_selector: &str,
    ) -> Result<(i64, DateTime<Utc>)>;

    /// External metric value(s). `metric_selector` is
    /// `LabelSelectorAsSelector(spec.External.Metric.Selector).String()`
    /// (replica_calculator.go:355), sent as `labelSelector`.
    async fn get_external_metric(
        &self,
        metric: &str,
        namespace: &str,
        metric_selector: &str,
    ) -> Result<(Vec<i64>, DateTime<Utc>)>;
}

/// Test double. Each map is keyed by metric name; resource map keyed by resource.
#[derive(Default, Clone)]
pub struct FakeMetricsClient {
    pub resource: HashMap<String, PodMetricsInfo>,
    pub pods: HashMap<String, PodMetricsInfo>,
    pub object: HashMap<String, i64>,
    pub external: HashMap<String, Vec<i64>>,
}

impl FakeMetricsClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// Helper: build a PodMetricsInfo where each named pod reports `value`
    /// (and optional utilization percent).
    pub fn pods_info(pods: &[(&str, i64, Option<i32>)]) -> PodMetricsInfo {
        let now = Utc::now();
        pods.iter()
            .map(|(name, value, util)| {
                (
                    name.to_string(),
                    PodMetric {
                        value: *value,
                        utilization: *util,
                        timestamp: now,
                        window: METRIC_SERVER_DEFAULT_METRIC_WINDOW,
                    },
                )
            })
            .collect()
    }
}

#[async_trait]
impl MetricsClient for FakeMetricsClient {
    async fn get_resource_metric(
        &self,
        resource: &str,
        _namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        self.resource
            .get(resource)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no resource metric for {resource}"))
    }

    async fn get_container_resource_metric(
        &self,
        resource: &str,
        container: &str,
        _namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        // Test double keys per-container readings as "{resource}/{container}",
        // falling back to the plain resource map for convenience.
        self.resource
            .get(&format!("{resource}/{container}"))
            .or_else(|| self.resource.get(resource))
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("no container resource metric for {resource}/{container}")
            })
    }

    async fn get_raw_metric(
        &self,
        metric: &str,
        _namespace: &str,
        _selector: &LabelSelector,
        _metric_selector: &str,
    ) -> Result<PodMetricsInfo> {
        self.pods
            .get(metric)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no pods metric for {metric}"))
    }

    async fn get_object_metric(
        &self,
        metric: &str,
        _namespace: &str,
        _object_ref: &CrossVersionObjectReference,
        _metric_selector: &str,
    ) -> Result<(i64, DateTime<Utc>)> {
        self.object
            .get(metric)
            .map(|v| (*v, Utc::now()))
            .ok_or_else(|| anyhow::anyhow!("no object metric for {metric}"))
    }

    async fn get_external_metric(
        &self,
        metric: &str,
        _namespace: &str,
        _metric_selector: &str,
    ) -> Result<(Vec<i64>, DateTime<Utc>)> {
        self.external
            .get(metric)
            .map(|v| (v.clone(), Utc::now()))
            .ok_or_else(|| anyhow::anyhow!("no external metric for {metric}"))
    }
}

/// Config for the real metrics client.
///
/// Two auth shapes are supported, matching the two ways the controller-manager
/// runs:
///   * **Legacy compose / direct-storage** — mTLS with the api-server client
///     cert (`client_cert_path` + `client_key_path`) and a CA file path. This
///     is what [`Default`] yields.
///   * **In-cluster static pod (API mode)** — no client cert on disk; instead
///     the kubeconfig-derived CA (`ca_pem`) and bearer `token` validate the
///     server and authenticate the request, exactly like the data-plane
///     `ApiClient`. See `run_api_mode` in `main.rs`.
#[derive(Clone, Debug)]
pub struct HttpMetricsConfig {
    pub api_server_url: String,
    /// CA PEM held in memory (in-cluster path, from the kubeconfig). Takes
    /// precedence over `ca_cert_path`. Ignored when `insecure_skip_tls_verify`.
    pub ca_pem: Option<Vec<u8>>,
    /// CA file path (legacy compose path). Ignored when `ca_pem` is set or when
    /// `insecure_skip_tls_verify`.
    pub ca_cert_path: Option<String>,
    /// Client cert/key for mTLS auth (legacy compose path). Both `None` in the
    /// in-cluster static pod, which authenticates with `token` instead.
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    /// Bearer token for api-server auth (in-cluster path). `None` when the
    /// api-server runs `--skip-auth` or when mTLS is used.
    pub token: Option<String>,
    /// Skip server-certificate chain verification. The cluster ships a
    /// self-signed api-server cert (the CA file is a copy of the leaf), which
    /// rustls rejects as `CaUsedAsEndEntity`. Matches the repo convention for
    /// the other in-cluster api-server clients (e.g. kubectl).
    pub insecure_skip_tls_verify: bool,
}

impl Default for HttpMetricsConfig {
    /// The legacy compose / direct-storage default: mTLS against
    /// `api-server:6443` with the api-server cert under `/etc/kubernetes/pki`,
    /// server verification skipped (self-signed leaf used as CA). The
    /// in-cluster path overrides this wholesale via
    /// `ControllerManagerConfig::metrics_config`.
    fn default() -> Self {
        Self {
            api_server_url: "https://api-server:6443".to_string(),
            ca_pem: None,
            ca_cert_path: Some("/etc/kubernetes/pki/ca.crt".to_string()),
            client_cert_path: Some("/etc/kubernetes/pki/api-server.crt".to_string()),
            client_key_path: Some("/etc/kubernetes/pki/api-server.key".to_string()),
            token: None,
            insecure_skip_tls_verify: true,
        }
    }
}

pub struct HttpMetricsClient {
    base: String,
    http: Client,
}

impl HttpMetricsClient {
    pub fn new(cfg: HttpMetricsConfig) -> Result<Self> {
        // rustls 0.23 requires a process-default CryptoProvider before any
        // ClientConfig is built. The controller-manager binary never constructs
        // a TlsConfig (unlike the api-server), so nothing installs one and
        // reqwest's `.build()` fails with a "builder error". Install the
        // aws-lc-rs provider idempotently here (no-op if already set).
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );

        // Force the rustls backend: another workspace crate (kubectl) pulls
        // reqwest with default features, so native-tls is unified in and would
        // otherwise be the default backend — incompatible with the rustls
        // identity built by `Identity::from_pem`.
        let mut builder = Client::builder().use_rustls_tls();

        // Client cert (mTLS) — legacy compose path. Absent in the in-cluster
        // static pod, which authenticates with a bearer token instead.
        if let (Some(cert_path), Some(key_path)) =
            (cfg.client_cert_path.as_ref(), cfg.client_key_path.as_ref())
        {
            let mut identity_pem = std::fs::read(cert_path)
                .map_err(|e| anyhow::anyhow!("read client cert {cert_path}: {e}"))?;
            let mut key = std::fs::read(key_path)
                .map_err(|e| anyhow::anyhow!("read client key {key_path}: {e}"))?;
            identity_pem.push(b'\n');
            identity_pem.append(&mut key);
            builder = builder.identity(reqwest::Identity::from_pem(&identity_pem)?);
        }

        // Server verification: insecure short-circuits; otherwise prefer the
        // in-memory CA (in-cluster, from the kubeconfig) over a CA file path.
        if cfg.insecure_skip_tls_verify {
            builder = builder.danger_accept_invalid_certs(true);
        } else if let Some(ca) = cfg.ca_pem.as_ref() {
            builder = builder.add_root_certificate(reqwest::Certificate::from_pem(ca)?);
        } else if let Some(ca_path) = cfg.ca_cert_path.as_ref() {
            let ca =
                std::fs::read(ca_path).map_err(|e| anyhow::anyhow!("read CA {ca_path}: {e}"))?;
            builder = builder.add_root_certificate(reqwest::Certificate::from_pem(&ca)?);
        }

        // Bearer token (in-cluster path) — applied to every metrics request via
        // a default header so the per-call code stays unaware of auth.
        if let Some(token) = cfg.token.as_ref() {
            use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
            let mut headers = HeaderMap::new();
            let mut val = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| anyhow::anyhow!("invalid bearer token: {e}"))?;
            val.set_sensitive(true);
            headers.insert(AUTHORIZATION, val);
            builder = builder.default_headers(headers);
        }

        let http = builder.build()?;
        Ok(Self {
            base: cfg.api_server_url.trim_end_matches('/').to_string(),
            http,
        })
    }

    /// Shared resource-utilization path for both whole-pod (`container == None`)
    /// and single-container (`container == Some`) metrics. Reads usage from
    /// `metrics.k8s.io`, divides by each pod's resource `requests` to produce a
    /// true utilization percentage.
    async fn resource_metric_impl(
        &self,
        resource: &str,
        container: Option<&str>,
        namespace: &str,
    ) -> Result<PodMetricsInfo> {
        let url = format!(
            "{}/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods",
            self.base, namespace
        );
        let body: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        // Per-pod request denominator (sum of the relevant containers' requests).
        let requests = self
            .fetch_pod_requests(resource, container, namespace)
            .await;

        let mut info = PodMetricsInfo::new();
        let now = Utc::now();
        if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
            for item in items {
                let name = item
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or_default()
                    .to_string();
                let mut usage = 0i64;
                if let Some(containers) = item.get("containers").and_then(|c| c.as_array()) {
                    for c in containers {
                        // For ContainerResource, only the named container counts.
                        if let Some(want) = container {
                            let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or_default();
                            if cname != want {
                                continue;
                            }
                        }
                        if let Some(q) = c
                            .get("usage")
                            .and_then(|u| u.get(resource))
                            .and_then(|v| v.as_str())
                        {
                            usage += parse_quantity_milli(q, resource);
                        }
                    }
                }
                // True utilization = usage * 100 / request. If the request
                // denominator is missing/zero we cannot express a percentage,
                // so leave it None (the replica calculator skips such pods).
                let utilization = requests
                    .get(&name)
                    .filter(|r| **r > 0)
                    .map(|r| ((usage * 100) / *r) as i32);
                info.insert(
                    name,
                    PodMetric {
                        value: usage,
                        utilization,
                        timestamp: now,
                        window: METRIC_SERVER_DEFAULT_METRIC_WINDOW,
                    },
                );
            }
        }
        if info.is_empty() {
            anyhow::bail!("no pod metrics for namespace {namespace}");
        }
        Ok(info)
    }

    /// Fetch each pod's resource `requests` denominator for `resource`, summed
    /// over either all containers (`container == None`) or just the named
    /// container. Returns a pod-name -> request (millicores for cpu, bytes for
    /// memory) map. Best-effort: a failed list yields an empty map (utilization
    /// then reported as unknown rather than fabricated).
    async fn fetch_pod_requests(
        &self,
        resource: &str,
        container: Option<&str>,
        namespace: &str,
    ) -> HashMap<String, i64> {
        let mut out = HashMap::new();
        let url = format!("{}/api/v1/namespaces/{}/pods", self.base, namespace);
        let body: serde_json::Value = match self
            .http
            .get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(resp) => match resp.json().await {
                Ok(b) => b,
                Err(_) => return out,
            },
            Err(_) => return out,
        };
        if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
            for item in items {
                let name = item
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or_default()
                    .to_string();
                let mut sum = 0i64;
                if let Some(containers) = item
                    .get("spec")
                    .and_then(|s| s.get("containers"))
                    .and_then(|c| c.as_array())
                {
                    for c in containers {
                        if let Some(want) = container {
                            let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or_default();
                            if cname != want {
                                continue;
                            }
                        }
                        if let Some(q) = c
                            .get("resources")
                            .and_then(|r| r.get("requests"))
                            .and_then(|req| req.get(resource))
                            .and_then(|v| v.as_str())
                        {
                            sum += parse_quantity_milli(q, resource);
                        }
                    }
                }
                if sum > 0 {
                    out.insert(name, sum);
                }
            }
        }
        out
    }
}

#[async_trait]
impl MetricsClient for HttpMetricsClient {
    async fn get_resource_metric(
        &self,
        resource: &str,
        namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        self.resource_metric_impl(resource, None, namespace).await
    }

    async fn get_container_resource_metric(
        &self,
        resource: &str,
        container: &str,
        namespace: &str,
        _selector: &LabelSelector,
    ) -> Result<PodMetricsInfo> {
        self.resource_metric_impl(resource, Some(container), namespace)
            .await
    }

    async fn get_raw_metric(
        &self,
        metric: &str,
        namespace: &str,
        _selector: &LabelSelector,
        _metric_selector: &str,
    ) -> Result<PodMetricsInfo> {
        let url = format!(
            "{}/apis/custom.metrics.k8s.io/v1beta2/namespaces/{}/pods/*/{}",
            self.base, namespace, metric
        );
        let body: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let mut info = PodMetricsInfo::new();
        let now = Utc::now();
        if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
            for item in items {
                let name = item
                    .get("describedObject")
                    .and_then(|o| o.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or_default()
                    .to_string();
                let value = item
                    .get("value")
                    .and_then(|v| v.as_str())
                    .map(|q| parse_quantity_milli(q, metric))
                    .unwrap_or(0);
                info.insert(
                    name,
                    PodMetric {
                        value,
                        utilization: None,
                        timestamp: now,
                        window: METRIC_SERVER_DEFAULT_METRIC_WINDOW,
                    },
                );
            }
        }
        if info.is_empty() {
            anyhow::bail!("no custom pod metrics for {metric}");
        }
        Ok(info)
    }

    async fn get_object_metric(
        &self,
        metric: &str,
        namespace: &str,
        object_ref: &CrossVersionObjectReference,
        _metric_selector: &str,
    ) -> Result<(i64, DateTime<Utc>)> {
        // `RootScopedMetrics().GetForObject` for a core `Namespace` reads
        // `namespaces/{ns}/metrics/{metric}` (client-go `getForNamespace`,
        // versioned_client.go:103-133); the object name is the HPA's own
        // namespace there.
        let url = if object_ref.kind == "Namespace" {
            format!(
                "{}/apis/custom.metrics.k8s.io/v1beta2/namespaces/{}/metrics/{}",
                self.base, namespace, metric
            )
        } else {
            format!(
                "{}/apis/custom.metrics.k8s.io/v1beta2/namespaces/{}/{}/{}/{}",
                self.base,
                namespace,
                object_ref.kind.to_lowercase(),
                object_ref.name,
                metric
            )
        };
        let body: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        // The API answers a `MetricValueList`; client-go requires exactly one
        // item (versioned_client.go:222-256, `GetForObject`).
        let items = body
            .get("items")
            .and_then(|v| v.as_array())
            .map(Vec::as_slice)
            .unwrap_or_default();
        if items.len() != 1 {
            anyhow::bail!(
                "the custom metrics API server returned {} results when we asked for exactly one",
                items.len()
            );
        }
        let value = items[0]
            .get("value")
            .and_then(|v| v.as_str())
            .map(|q| parse_quantity_milli(q, metric))
            .ok_or_else(|| anyhow::anyhow!("object metric {metric} missing value"))?;
        Ok((value, Utc::now()))
    }

    async fn get_external_metric(
        &self,
        metric: &str,
        namespace: &str,
        _metric_selector: &str,
    ) -> Result<(Vec<i64>, DateTime<Utc>)> {
        // external.metrics.k8s.io is not implemented server-side (deferred).
        let url = format!(
            "{}/apis/external.metrics.k8s.io/v1beta1/namespaces/{}/{}",
            self.base, namespace, metric
        );
        let body: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let values = body
            .get("items")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|i| i.get("value").and_then(|v| v.as_str()))
                    .map(|q| parse_quantity_milli(q, metric))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if values.is_empty() {
            anyhow::bail!("no external metric values for {metric}");
        }
        Ok((values, Utc::now()))
    }
}

/// Parse a k8s quantity to an integer in units consistent per resource so a
/// usage/request ratio is meaningful: CPU in millicores, every other metric in
/// its base unit (bytes for memory, whole items for a custom metric).
///
/// Unparseable input reads as 0 — a metrics sample the client cannot decode
/// must not scale the target on a garbage value.
///
/// The CPU branch this replaced parsed the pre-`m` digits with `i64`, so a
/// fractional millicore sample collapsed to 0, and the fallback branch took
/// only the leading digit run, so a canonicalised `"1k"` custom metric read
/// as 1.
fn parse_quantity_milli(q: &str, resource: &str) -> i64 {
    parse_resource_value(q, resource).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_returns_seeded_pods_metric() {
        let mut fake = FakeMetricsClient::new();
        fake.pods.insert(
            "requests-per-second".into(),
            FakeMetricsClient::pods_info(&[("p1", 200, None), ("p2", 200, None)]),
        );
        let sel = LabelSelector::default();
        let info = fake
            .get_raw_metric("requests-per-second", "default", &sel, "")
            .await
            .unwrap();
        assert_eq!(info.len(), 2);
        assert_eq!(info["p1"].value, 200);
    }

    #[test]
    fn parse_quantity_cpu_and_memory_units() {
        // CPU -> millicores
        assert_eq!(parse_quantity_milli("500m", "cpu"), 500);
        assert_eq!(parse_quantity_milli("2", "cpu"), 2000);
        // Memory -> bytes, binary + SI suffixes
        assert_eq!(parse_quantity_milli("128Mi", "memory"), 128 * 1024 * 1024);
        assert_eq!(parse_quantity_milli("1Gi", "memory"), 1024 * 1024 * 1024);
        assert_eq!(parse_quantity_milli("1M", "memory"), 1_000_000);
        assert_eq!(parse_quantity_milli("1048576", "memory"), 1_048_576);
        // Other metrics -> base unit
        assert_eq!(parse_quantity_milli("42", "requests-per-second"), 42);
    }

    /// CPU parsed the pre-`m` digits with `parse::<i64>()`, so a fractional
    /// millicore reading collapsed to 0 — an HPA target computed from that
    /// scales on a utilisation of zero. Non-cpu/memory metrics took only the
    /// leading digit run, so a canonicalised `"1k"` read as 1.
    #[test]
    fn parse_quantity_handles_fractional_cpu_and_suffixed_custom_metrics() {
        assert_eq!(parse_quantity_milli("10.5m", "cpu"), 11);
        assert_eq!(parse_quantity_milli("0.5m", "cpu"), 1);
        assert_eq!(parse_quantity_milli("0.7", "cpu"), 700);
        assert_eq!(parse_quantity_milli("1500u", "cpu"), 2);
        assert_eq!(parse_quantity_milli("1k", "requests-per-second"), 1_000);
        assert_eq!(parse_quantity_milli("2M", "requests-per-second"), 2_000_000);
        // Unparseable stays 0 — the caller has no error channel.
        assert_eq!(parse_quantity_milli("bogus", "cpu"), 0);
        assert_eq!(parse_quantity_milli("", "memory"), 0);
    }

    #[tokio::test]
    async fn fake_container_resource_metric_lookup() {
        let mut fake = FakeMetricsClient::new();
        fake.resource.insert(
            "cpu/app".to_string(),
            FakeMetricsClient::pods_info(&[("p1", 50, Some(75))]),
        );
        let sel = LabelSelector::default();
        let info = fake
            .get_container_resource_metric("cpu", "app", "default", &sel)
            .await
            .unwrap();
        assert_eq!(info["p1"].utilization, Some(75));
        // Missing container -> error.
        assert!(fake
            .get_container_resource_metric("cpu", "missing", "default", &sel)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn fake_errors_on_missing_metric() {
        let fake = FakeMetricsClient::new();
        let sel = LabelSelector::default();
        assert!(fake
            .get_resource_metric("cpu", "default", &sel)
            .await
            .is_err());
    }

    /// In-cluster shape (#1144): no client cert on disk, just a bearer token
    /// and `insecure_skip_tls_verify`. This is exactly what `run_api_mode`
    /// builds; it must construct a client (and trim the base URL) rather than
    /// failing on a missing `/etc/kubernetes/pki` cert read.
    #[test]
    fn http_client_builds_without_client_cert_insecure() {
        let cfg = HttpMetricsConfig {
            api_server_url: "https://api-server:6443/".to_string(),
            ca_pem: None,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            token: Some("test-bearer-token".to_string()),
            insecure_skip_tls_verify: true,
        };
        let client = HttpMetricsClient::new(cfg).expect("in-cluster (token, insecure) builds");
        // Trailing slash is trimmed off the base.
        assert_eq!(client.base, "https://api-server:6443");
    }

    /// Legacy mTLS path: a configured-but-missing client cert must surface a
    /// clear read error naming the path (not a generic builder failure).
    #[test]
    fn http_client_missing_client_cert_errors_clearly() {
        let cfg = HttpMetricsConfig {
            client_cert_path: Some("/nonexistent/api-server.crt".to_string()),
            client_key_path: Some("/nonexistent/api-server.key".to_string()),
            ..HttpMetricsConfig::default()
        };
        let err = HttpMetricsClient::new(cfg)
            .map(|_| ())
            .expect_err("missing cert must error");
        assert!(
            err.to_string().contains("/nonexistent/api-server.crt"),
            "error should name the missing cert path, got: {err}"
        );
    }

    // -- wire contract: custom/external metrics requests -------------------
    //
    // Upstream pkg/controller/podautoscaler/metrics/client_test.go drives the
    // REST metrics client through a fake clientset; here the same assertions
    // run against an in-process api-server stub so the request the controller
    // really puts on the wire is what is checked.

    use std::sync::{Arc, Mutex};

    type Requests = Arc<Mutex<Vec<(String, HashMap<String, String>)>>>;

    /// Serve `routes` (path -> JSON body) on an ephemeral port; every request
    /// is recorded as (path, decoded query). Unknown paths answer 404.
    async fn stub_api_server(routes: Vec<(&'static str, serde_json::Value)>) -> (String, Requests) {
        let seen: Requests = Arc::new(Mutex::new(Vec::new()));
        let seen_h = seen.clone();
        let routes = Arc::new(routes);
        let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
            let seen = seen_h.clone();
            let routes = routes.clone();
            async move {
                let query: HashMap<String, String> = reqwest::Url::parse(&format!(
                    "http://stub{}",
                    uri.path_and_query().map(|p| p.as_str()).unwrap_or("/")
                ))
                .unwrap()
                .query_pairs()
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
                seen.lock().unwrap().push((uri.path().to_string(), query));
                match routes.iter().find(|(p, _)| *p == uri.path()) {
                    Some((_, body)) => (axum::http::StatusCode::OK, axum::Json(body.clone())),
                    None => (
                        axum::http::StatusCode::NOT_FOUND,
                        axum::Json(serde_json::json!({"message": "not found"})),
                    ),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), seen)
    }

    fn client_for(url: String) -> HttpMetricsClient {
        HttpMetricsClient::new(HttpMetricsConfig {
            api_server_url: url,
            ca_pem: None,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            token: None,
            insecure_skip_tls_verify: true,
        })
        .unwrap()
    }

    fn pod_selector(app: &str) -> LabelSelector {
        LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), app.to_string())])),
            match_expressions: None,
        }
    }

    fn metric_item(name: &str, value: &str, ts: &str, window: Option<i64>) -> serde_json::Value {
        let mut item = serde_json::json!({
            "describedObject": {"kind": "Pod", "name": name, "apiVersion": "v1"},
            "metric": {"name": "qps"},
            "timestamp": ts,
            "value": value,
        });
        if let Some(w) = window {
            item["windowSeconds"] = serde_json::json!(w);
        }
        item
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn apps_discovery() -> Vec<(&'static str, serde_json::Value)> {
        vec![
            (
                "/apis/apps",
                serde_json::json!({
                    "kind": "APIGroup", "name": "apps",
                    "versions": [{"groupVersion": "apps/v1", "version": "v1"}],
                    "preferredVersion": {"groupVersion": "apps/v1", "version": "v1"},
                }),
            ),
            (
                "/apis/apps/v1",
                serde_json::json!({
                    "kind": "APIResourceList", "groupVersion": "apps/v1",
                    "resources": [
                        {"name": "deployments", "kind": "Deployment", "namespaced": true},
                        {"name": "deployments/scale", "kind": "Scale", "namespaced": true},
                    ],
                }),
            ),
        ]
    }

    /// `GetRawMetric` -> `GetForObjects(Pod, selector, metricName,
    /// metricSelector)` (metrics/client.go:130-132) puts the pod selector in
    /// `labelSelector` and the metric selector in `metricLabelSelector`
    /// (custom_metrics/versioned_client.go:226-251), and reads
    /// `windowSeconds` per item, defaulting to 1m (client.go:136-143).
    #[tokio::test]
    async fn raw_metric_sends_pod_and_metric_selectors_and_reads_window() {
        let body = serde_json::json!({"kind": "MetricValueList", "items": [
            metric_item("web-0", "5", "2026-01-01T00:00:10Z", Some(30)),
            metric_item("web-1", "7", "2026-01-01T00:00:20Z", None),
        ]});
        let (url, seen) = stub_api_server(vec![(
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/pods/*/qps",
            body,
        )])
        .await;
        let info = client_for(url)
            .get_raw_metric("qps", "ns", &pod_selector("web"), "env=prod")
            .await
            .unwrap();
        let reqs = seen.lock().unwrap().clone();
        assert_eq!(reqs.len(), 1);
        assert_eq!(
            reqs[0].1.get("labelSelector").map(String::as_str),
            Some("app=web")
        );
        assert_eq!(
            reqs[0].1.get("metricLabelSelector").map(String::as_str),
            Some("env=prod")
        );
        assert_eq!(info["web-0"].window, std::time::Duration::from_secs(30));
        assert_eq!(info["web-1"].window, METRIC_SERVER_DEFAULT_METRIC_WINDOW);
        assert_eq!(info["web-0"].timestamp, ts("2026-01-01T00:00:10Z"));
        assert_eq!(info["web-1"].timestamp, ts("2026-01-01T00:00:20Z"));
    }

    /// An empty selector is not sent at all (`VersionedParams` omits empty
    /// `omitempty` fields).
    #[tokio::test]
    async fn raw_metric_omits_empty_metric_selector() {
        let body = serde_json::json!({"kind": "MetricValueList", "items": [
            metric_item("web-0", "5", "2026-01-01T00:00:10Z", None),
        ]});
        let (url, seen) = stub_api_server(vec![(
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/pods/*/qps",
            body,
        )])
        .await;
        client_for(url)
            .get_raw_metric("qps", "ns", &pod_selector("web"), "")
            .await
            .unwrap();
        let reqs = seen.lock().unwrap().clone();
        assert!(!reqs[0].1.contains_key("metricLabelSelector"));
    }

    /// client.go:122-128 error texts.
    #[tokio::test]
    async fn raw_metric_error_texts_match_upstream() {
        let (url, _) = stub_api_server(vec![(
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/pods/*/qps",
            serde_json::json!({"kind": "MetricValueList", "items": []}),
        )])
        .await;
        let c = client_for(url);
        let err = c
            .get_raw_metric("qps", "ns", &pod_selector("web"), "")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "no metrics returned from custom metrics API"
        );
        let err = c
            .get_raw_metric("other", "ns", &pod_selector("web"), "")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .starts_with("unable to fetch metrics from custom metrics API: "),
            "got: {err}"
        );
    }

    /// `GetObjectMetric` resolves the plural resource through the REST mapper
    /// (`qualResourceForKind`, versioned_client.go:84-92 -> `deployments.apps`)
    /// and sends `metricLabelSelector` (versioned_client.go:196-205).
    #[tokio::test]
    async fn object_metric_resolves_resource_via_discovery() {
        let mut routes = apps_discovery();
        routes.push((
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/deployments.apps/web/qps",
            serde_json::json!({"kind": "MetricValueList", "items": [
                metric_item("web", "9", "2026-01-01T00:00:10Z", None),
            ]}),
        ));
        let (url, seen) = stub_api_server(routes).await;
        let obj = CrossVersionObjectReference {
            kind: "Deployment".into(),
            name: "web".into(),
            api_version: Some("apps/v1".into()),
        };
        let (value, stamp) = client_for(url)
            .get_object_metric("qps", "ns", &obj, "env=prod")
            .await
            .unwrap();
        assert_eq!(value, 9);
        assert_eq!(stamp, ts("2026-01-01T00:00:10Z"));
        let reqs = seen.lock().unwrap().clone();
        let metric_req = reqs.last().unwrap();
        assert_eq!(
            metric_req.0,
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/deployments.apps/web/qps"
        );
        assert_eq!(
            metric_req.1.get("metricLabelSelector").map(String::as_str),
            Some("env=prod")
        );
    }

    /// `qualResourceForKind` failure text (versioned_client.go:86) wrapped by
    /// client.go:152; the mapper's `NoKindMatchError` for a group search.
    #[tokio::test]
    async fn object_metric_unmappable_kind_errors_like_upstream() {
        let (url, _) = stub_api_server(apps_discovery()).await;
        let obj = CrossVersionObjectReference {
            kind: "Widget".into(),
            name: "w".into(),
            api_version: Some("apps/v1".into()),
        };
        let err = client_for(url)
            .get_object_metric("qps", "ns", &obj, "")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "unable to fetch metrics from custom metrics API: unable to map kind Widget.apps to resource: no matches for kind \"Widget\" in group \"apps\""
        );
    }

    /// A Namespace object reads `namespaces/{ns}/metrics/{metric}` and still
    /// carries `metricLabelSelector` (versioned_client.go:103-133).
    #[tokio::test]
    async fn namespace_object_metric_sends_metric_selector() {
        let (url, seen) = stub_api_server(vec![(
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/metrics/qps",
            serde_json::json!({"kind": "MetricValueList", "items": [
                metric_item("ns", "3", "2026-01-01T00:00:10Z", None),
            ]}),
        )])
        .await;
        let obj = CrossVersionObjectReference {
            kind: "Namespace".into(),
            name: "ns".into(),
            api_version: Some("v1".into()),
        };
        client_for(url)
            .get_object_metric("qps", "ns", &obj, "env=prod")
            .await
            .unwrap();
        let reqs = seen.lock().unwrap().clone();
        assert_eq!(
            reqs[0].1.get("metricLabelSelector").map(String::as_str),
            Some("env=prod")
        );
    }

    /// `NamespacedMetrics(ns).List(metricName, selector)`
    /// (external_metrics/client.go:84-92) sends `labelSelector`; the
    /// timestamp is `Items[0]`'s (metrics/client.go:224-226); error texts
    /// client.go:210-216.
    #[tokio::test]
    async fn external_metric_sends_label_selector_and_uses_first_timestamp() {
        let items = serde_json::json!({"kind": "ExternalMetricValueList", "items": [
            {"metricName": "qps", "timestamp": "2026-01-01T00:00:30Z", "value": "4"},
            {"metricName": "qps", "timestamp": "2026-01-01T00:00:10Z", "value": "6"},
        ]});
        let (url, seen) = stub_api_server(vec![
            (
                "/apis/external.metrics.k8s.io/v1beta1/namespaces/ns/qps",
                items,
            ),
            (
                "/apis/external.metrics.k8s.io/v1beta1/namespaces/ns/empty",
                serde_json::json!({"kind": "ExternalMetricValueList", "items": []}),
            ),
        ])
        .await;
        let c = client_for(url);
        let (values, stamp) = c
            .get_external_metric("qps", "ns", "env=prod")
            .await
            .unwrap();
        assert_eq!(values, vec![4, 6]);
        assert_eq!(stamp, ts("2026-01-01T00:00:30Z"));
        let reqs = seen.lock().unwrap().clone();
        assert_eq!(
            reqs[0].1.get("labelSelector").map(String::as_str),
            Some("env=prod")
        );
        let err = c.get_external_metric("empty", "ns", "").await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "no metrics returned from external metrics API"
        );
        let err = c.get_external_metric("nope", "ns", "").await.unwrap_err();
        assert!(
            err.to_string()
                .starts_with("unable to fetch metrics from external metrics API: "),
            "got: {err}"
        );
    }
}
