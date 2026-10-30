//! Metrics source abstraction for the HPA controller.
//!
//! Mirrors k8s `pkg/controller/podautoscaler/metrics/interfaces.go`. The real
//! impl (`HttpMetricsClient`) queries the api-server metrics endpoints over
//! mTLS; tests inject `FakeMetricsClient`.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use rusternetes_common::quantity::parse_resource_value;
use rusternetes_common::resources::CrossVersionObjectReference;
use rusternetes_common::types::LabelSelector;
use std::collections::HashMap;

/// Per-pod metric reading. `value` is the raw metric in milli-units for cpu
/// (e.g. 500 = 500m) and for custom (Pods) metrics (`Quantity.MilliValue()`,
/// `metrics/client.go:97`), whole units for other resources (memory bytes);
/// `utilization` is the percentage of the pod's request (resource metrics only).
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
    ) -> Result<(PodMetricsInfo, DateTime<Utc>)>;

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
    ) -> Result<(PodMetricsInfo, DateTime<Utc>)> {
        self.pods
            .get(metric)
            .cloned()
            .map(|info| {
                let ts = info
                    .values()
                    .map(|m| m.timestamp)
                    .next()
                    .unwrap_or_else(Utc::now);
                (info, ts)
            })
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
    pub base: String,
    pub http: Client,
    /// `qualResourceForKind` results (`Kind.group` -> `resource[.group]`); the
    /// upstream REST mapper is likewise discovery-backed and cached.
    pub resources: std::sync::Mutex<HashMap<String, String>>,
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
            resources: std::sync::Mutex::new(HashMap::new()),
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

    /// `customMetricsClient.GetRawMetric` (metrics/client.go:119-150):
    /// `NamespacedMetrics(ns).GetForObjects(Pod, selector, metricName,
    /// metricSelector)` (custom_metrics/versioned_client.go:226-251).
    async fn get_raw_metric(
        &self,
        metric: &str,
        namespace: &str,
        selector: &LabelSelector,
        metric_selector: &str,
    ) -> Result<(PodMetricsInfo, DateTime<Utc>)> {
        let body = self
            .raw_metric_body(metric, namespace, selector, metric_selector)
            .await
            .map_err(|e| anyhow::anyhow!("unable to fetch metrics from custom metrics API: {e}"))?;
        let items = items_of(&body);
        if items.is_empty() {
            anyhow::bail!("no metrics returned from custom metrics API");
        }

        // client.go:147-149: the returned timestamp is `metrics.Items[0].Timestamp`.
        let timestamp = item_timestamp(&items[0])?;
        let mut info = PodMetricsInfo::new();
        for m in items {
            // client.go:136-139: `windowSeconds` or `metricServerDefaultMetricWindow`.
            let window = match m.get("windowSeconds").and_then(|w| w.as_i64()) {
                Some(w) => std::time::Duration::from_secs(w.max(0) as u64),
                None => METRIC_SERVER_DEFAULT_METRIC_WINDOW,
            };
            let name = m
                .get("describedObject")
                .and_then(|o| o.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string();
            info.insert(
                name,
                PodMetric {
                    value: item_value(m),
                    utilization: None,
                    timestamp: item_timestamp(m)?,
                    window,
                },
            );
        }
        Ok((info, timestamp))
    }

    /// `customMetricsClient.GetObjectMetric` (metrics/client.go:152-170).
    async fn get_object_metric(
        &self,
        metric: &str,
        namespace: &str,
        object_ref: &CrossVersionObjectReference,
        metric_selector: &str,
    ) -> Result<(i64, DateTime<Utc>)> {
        self.object_metric_item(metric, namespace, object_ref, metric_selector)
            .await
            .map_err(|e| anyhow::anyhow!("unable to fetch metrics from custom metrics API: {e}"))
    }

    /// `externalMetricsClient.GetExternalMetric` (metrics/client.go:204-226):
    /// `NamespacedMetrics(ns).List(metricName, selector)`
    /// (external_metrics/client.go:84-100); the timestamp is `Items[0]`'s.
    async fn get_external_metric(
        &self,
        metric: &str,
        namespace: &str,
        metric_selector: &str,
    ) -> Result<(Vec<i64>, DateTime<Utc>)> {
        let url = format!(
            "{}/apis/external.metrics.k8s.io/v1beta1/namespaces/{}/{}",
            self.base, namespace, metric
        );
        let body = self
            .get_json(&url, &[("labelSelector", metric_selector)])
            .await
            .map_err(|e| {
                anyhow::anyhow!("unable to fetch metrics from external metrics API: {e}")
            })?;
        let items = items_of(&body);
        if items.is_empty() {
            anyhow::bail!("no metrics returned from external metrics API");
        }
        let values = items.iter().map(item_value).collect();
        Ok((values, item_timestamp(&items[0])?))
    }
}

/// `MetricValueList.Items` / `ExternalMetricValueList.Items`.
fn items_of(body: &serde_json::Value) -> &[serde_json::Value] {
    body.get("items")
        .and_then(|v| v.as_array())
        .map(Vec::as_slice)
        .unwrap_or_default()
}

/// `m.Value.MilliValue()` for one metric item.
fn item_value(item: &serde_json::Value) -> i64 {
    item.get("value")
        .and_then(|v| v.as_str())
        .map(metric_milli_value)
        .unwrap_or(0)
}

/// `Quantity.MilliValue()` for a custom / external / object metric sample
/// (`metrics/client.go:97,162,190,213`: `val.MilliValue()`,
/// `m.Value.MilliValue()`). Unlike the resource path (`parse_quantity_milli`,
/// CPU millicores / memory bytes, only ever used as a ratio), these values
/// surface in `status.currentMetrics` and are compared against targets read
/// with `MetricTarget.*.MilliValue()`, so every one is in milli-units.
/// Unparseable input reads as 0.
pub fn metric_milli_value(q: &str) -> i64 {
    parse_milli(q).unwrap_or(0)
}

/// Strict `Quantity.MilliValue()` (rounded up away from zero, saturating).
pub fn parse_milli(q: &str) -> Option<i64> {
    let v = rusternetes_common::quantity::Quantity::parse(q.trim())
        .ok()?
        .milli_value();
    Some(v.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
}

/// `resource.NewMilliQuantity(v, resource.DecimalSI).String()`, as
/// `horizontal.go:563,572,598,719,739` build `status.currentMetrics` values.
pub fn format_milli_quantity(v: i64) -> String {
    use rusternetes_common::quantity::{Format, Quantity};
    Quantity::from_milli_value(v, Format::DecimalSI).canonical_string()
}

/// `m.Timestamp.Time` for one metric item.
fn item_timestamp(item: &serde_json::Value) -> Result<DateTime<Utc>> {
    let raw = item
        .get("timestamp")
        .and_then(|t| t.as_str())
        .ok_or_else(|| anyhow::anyhow!("metric item has no timestamp"))?;
    Ok(DateTime::parse_from_rfc3339(raw)
        .map_err(|e| anyhow::anyhow!("invalid metric timestamp {raw:?}: {e}"))?
        .with_timezone(&Utc))
}

impl HttpMetricsClient {
    /// GET `url` with the non-empty `params` as the query string (an empty
    /// `omitempty` option is not encoded by `VersionedParams`) and decode JSON.
    async fn get_json(&self, url: &str, params: &[(&str, &str)]) -> Result<serde_json::Value> {
        let params: Vec<(&str, &str)> = params
            .iter()
            .copied()
            .filter(|(_, v)| !v.is_empty())
            .collect();
        Ok(self
            .http
            .get(url)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// `GetForObjects(Pod, selector, metricName, metricSelector)`:
    /// `resource/pods/*/metric?labelSelector=..&metricLabelSelector=..`.
    async fn raw_metric_body(
        &self,
        metric: &str,
        namespace: &str,
        selector: &LabelSelector,
        metric_selector: &str,
    ) -> Result<serde_json::Value> {
        let resource = self.qual_resource_for_kind("", "Pod").await?;
        let selector = selector
            .as_selector_string()
            .map_err(|e| anyhow::anyhow!(e))?;
        let url = format!(
            "{}/apis/custom.metrics.k8s.io/v1beta2/namespaces/{}/{}/*/{}",
            self.base, namespace, resource, metric
        );
        self.get_json(
            &url,
            &[
                ("labelSelector", selector.as_str()),
                ("metricLabelSelector", metric_selector),
            ],
        )
        .await
    }

    /// `GetForObject` for a namespaced object, or `getForNamespace` for a core
    /// `Namespace` (versioned_client.go:103-133,180-215); exactly one item.
    async fn object_metric_item(
        &self,
        metric: &str,
        namespace: &str,
        object_ref: &CrossVersionObjectReference,
        metric_selector: &str,
    ) -> Result<(i64, DateTime<Utc>)> {
        // `schema.FromAPIVersionAndKind` (metrics/client.go:153).
        let group = group_of(object_ref.api_version.as_deref().unwrap_or_default());
        let url = if object_ref.kind == "Namespace" && group.is_empty() {
            // NB: the object name is ignored, `CrossVersionObjectReference`
            // cannot escape the HPA's namespace (client.go:158-160).
            format!(
                "{}/apis/custom.metrics.k8s.io/v1beta2/namespaces/{}/metrics/{}",
                self.base, namespace, metric
            )
        } else {
            let resource = self.qual_resource_for_kind(group, &object_ref.kind).await?;
            format!(
                "{}/apis/custom.metrics.k8s.io/v1beta2/namespaces/{}/{}/{}/{}",
                self.base, namespace, resource, object_ref.name, metric
            )
        };
        let body = self
            .get_json(&url, &[("metricLabelSelector", metric_selector)])
            .await?;
        // versioned_client.go:134-139: exactly one item.
        let items = items_of(&body);
        if items.len() != 1 {
            anyhow::bail!(
                "the custom metrics API server returned {} results when we asked for exactly one",
                items.len()
            );
        }
        Ok((item_value(&items[0]), item_timestamp(&items[0])?))
    }

    /// `customMetricsClient.qualResourceForKind` (versioned_client.go:84-92):
    /// the REST mapping of `GroupKind` (preferred version first, no version
    /// constraint) as `resource[.group]`. The mapper upstream is a discovery
    /// REST mapper over the same api-server, so resolve it through discovery.
    async fn qual_resource_for_kind(&self, group: &str, kind: &str) -> Result<String> {
        let key = format!("{kind}.{group}");
        if let Some(hit) = self.resources.lock().unwrap().get(&key) {
            return Ok(hit.clone());
        }
        let gk = if group.is_empty() {
            kind.to_string()
        } else {
            format!("{kind}.{group}")
        };
        let no_match = || {
            anyhow::anyhow!(
                "unable to map kind {gk} to resource: no matches for kind \"{kind}\" in group \"{group}\""
            )
        };
        // Versions of the group, preferred first (restmapper priority order).
        let versions: Vec<String> = if group.is_empty() {
            vec!["v1".to_string()]
        } else {
            let url = format!("{}/apis/{}", self.base, group);
            let resp = self.http.get(&url).send().await?;
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(no_match());
            }
            let doc: serde_json::Value = resp.error_for_status()?.json().await?;
            let preferred = doc
                .get("preferredVersion")
                .and_then(|p| p.get("version"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let mut all: Vec<String> = doc
                .get("versions")
                .and_then(|v| v.as_array())
                .map(|vs| {
                    vs.iter()
                        .filter_map(|v| v.get("version").and_then(|v| v.as_str()))
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            if let Some(p) = preferred {
                all.retain(|v| *v != p);
                all.insert(0, p);
            }
            all
        };
        for version in versions {
            let url = if group.is_empty() {
                format!("{}/api/{}", self.base, version)
            } else {
                format!("{}/apis/{}/{}", self.base, group, version)
            };
            let list = self.get_json(&url, &[]).await?;
            let found = list
                .get("resources")
                .and_then(|r| r.as_array())
                .and_then(|rs| {
                    rs.iter().find(|r| {
                        r.get("kind").and_then(|k| k.as_str()) == Some(kind)
                            && r.get("name")
                                .and_then(|n| n.as_str())
                                .is_some_and(|n| !n.contains('/'))
                    })
                })
                .and_then(|r| r.get("name").and_then(|n| n.as_str()));
            if let Some(name) = found {
                let qualified = if group.is_empty() {
                    name.to_string()
                } else {
                    format!("{name}.{group}")
                };
                self.resources
                    .lock()
                    .unwrap()
                    .insert(key, qualified.clone());
                return Ok(qualified);
            }
        }
        Err(no_match())
    }
}

/// `schema.ParseGroupVersion(apiVersion).Group`.
fn group_of(api_version: &str) -> &str {
    api_version.split_once('/').map_or("", |(g, _)| g)
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
pub fn parse_quantity_milli(q: &str, resource: &str) -> i64 {
    parse_resource_value(q, resource).unwrap_or(0)
}
