//! The `ImagePolicyWebhook` admission plugin: pod image decisions delegated to
//! a webhook backend through an `imagepolicy.k8s.io/v1alpha1` `ImageReview`.
//!
//! Ported from `plugin/pkg/admission/imagepolicy/`:
//! - `admission.go`: `Plugin.Validate`, `admitPod`, `webhookError`,
//!   `filterAnnotations`, `statusTTL`, `NewImagePolicyWebhook`;
//! - `config.go`: `imagePolicyWebhookConfig`, `AdmissionConfig`,
//!   `normalizeWebhookConfig`, `normalizeConfigDuration`;
//! - the webhook client is `staging/src/k8s.io/apiserver/pkg/util/webhook/
//!   webhook.go` (`NewGenericWebhook`, `WithExponentialBackoff`,
//!   `DefaultShouldRetry`, `DefaultRetryBackoffWithInitialDelay`), over the
//!   kubeconfig loader the authorization webhook already ports
//!   ([`WebhookClientConfig`], `webhookutil.LoadKubeconfig`).
//!
//! The configuration is the legacy unversioned `imagePolicy` document: the
//! whole `--admission-control-config-file` when it is not an
//! `AdmissionConfiguration` (routed by `config::plugin_configuration_for`), or
//! the `configuration` of the `ImagePolicyWebhook` plugin entry.
//!
//! Deviation: upstream's `webhookError` also stamps the legacy pod annotation
//! `alpha.image-policy.k8s.io/failed-open` on the pod being validated (a
//! `TODO(wteiken): Remove ... for the 1.13 release`). The validating chain
//! here receives the object read-only, so [`Decision::failed_open`] reports
//! the fail-open and only the audit annotation is recorded by the caller;
//! [`mark_failed_open`] is the pod-side half for a caller that holds the pod
//! mutably.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use rusternetes_common::lru_expire_cache::LruExpireCache;
use rusternetes_common::resources::Pod;
use rusternetes_common::wait::Backoff;
use rusternetes_common::Error;
use serde::{Deserialize, Serialize};

use super::builtin_plugins::{attr_name, new_forbidden};
use crate::authorization_webhook::{is_connection_reset, WebhookClientConfig};

/// `PluginName`.
pub const PLUGIN_NAME: &str = "ImagePolicyWebhook";

/// `AuditKeyPrefix` (admission.go:60): `strings.ToLower(PluginName) +
/// ".image-policy.k8s.io/"`.
pub const AUDIT_KEY_PREFIX: &str = "imagepolicywebhook.image-policy.k8s.io/";

/// `ImagePolicyFailedOpenKeySuffix` (admission.go:66).
pub const FAILED_OPEN_KEY_SUFFIX: &str = "failed-open";

/// `api.ImagePolicyFailedOpenKey` (pkg/apis/core/annotation_key_constants.go).
pub const IMAGE_POLICY_FAILED_OPEN_KEY: &str = "alpha.image-policy.k8s.io/failed-open";

const EPHEMERAL_CONTAINERS: &str = "ephemeralcontainers";

// config.go:28-40, in nanoseconds (`time.Duration`).
const MS: i64 = 1_000_000;
const SEC: i64 = 1_000_000_000;
const DEFAULT_RETRY_BACKOFF: i64 = 500 * MS;
const MIN_RETRY_BACKOFF: i64 = 1;
const MAX_RETRY_BACKOFF: i64 = 5 * 60 * SEC;
const DEFAULT_ALLOW_TTL: i64 = 5 * 60 * SEC;
const DEFAULT_DENY_TTL: i64 = 30 * SEC;
const MIN_ALLOW_TTL: i64 = SEC;
const MAX_ALLOW_TTL: i64 = 30 * 60 * SEC;
const MIN_DENY_TTL: i64 = SEC;
const MAX_DENY_TTL: i64 = 30 * 60 * SEC;
/// `useDefault`: the sentinel for using the default TTL.
const USE_DEFAULT: i64 = 0;
/// `disableTTL`: the sentinel for disabling a TTL.
const DISABLE_TTL: i64 = -1;

/// `imagePolicyWebhookConfig` (config.go:43-49). The durations decode from
/// plain numbers, as a Go `time.Duration` does (nanoseconds until scaled by
/// [`normalize_webhook_config`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImagePolicyWebhookConfig {
    #[serde(default)]
    pub kube_config_file: String,
    #[serde(default, rename = "allowTTL")]
    pub allow_ttl: i64,
    #[serde(default, rename = "denyTTL")]
    pub deny_ttl: i64,
    #[serde(default)]
    pub retry_backoff: i64,
    #[serde(default)]
    pub default_allow: bool,
}

/// `AdmissionConfig` (config.go:52-54).
#[derive(Debug, Default, Deserialize)]
struct AdmissionConfig {
    #[serde(default, rename = "imagePolicy")]
    image_policy_webhook: ImagePolicyWebhookConfig,
}

/// `normalizeWebhookConfig` (config.go:56-67).
pub fn normalize_webhook_config(config: &mut ImagePolicyWebhookConfig) -> Result<(), String> {
    config.retry_backoff = normalize_config_duration(
        MS,
        config.retry_backoff,
        MIN_RETRY_BACKOFF,
        MAX_RETRY_BACKOFF,
        DEFAULT_RETRY_BACKOFF,
    )?;
    config.allow_ttl = normalize_config_duration(
        SEC,
        config.allow_ttl,
        MIN_ALLOW_TTL,
        MAX_ALLOW_TTL,
        DEFAULT_ALLOW_TTL,
    )?;
    config.deny_ttl = normalize_config_duration(
        SEC,
        config.deny_ttl,
        MIN_DENY_TTL,
        MAX_DENY_TTL,
        DEFAULT_DENY_TTL,
    )?;
    Ok(())
}

/// `normalizeConfigDuration` (config.go:69-90): `-1` disables (0), `0` takes
/// the default, anything else is scaled by `scale` and range checked.
fn normalize_config_duration(
    scale: i64,
    value: i64,
    min: i64,
    max: i64,
    default: i64,
) -> Result<i64, String> {
    if value == DISABLE_TTL {
        return Ok(0);
    }
    if value == USE_DEFAULT {
        return Ok(default);
    }
    // Go multiplies in wrapping int64; saturating keeps an overflowing value
    // out of range, which is the intent.
    let scaled = value.saturating_mul(scale);
    if scaled < min || scaled > max {
        return Err(format!(
            "valid value is between {min}ns and {max}ns, got {scaled}ns"
        ));
    }
    Ok(scaled)
}

// ---------------------------------------------------------------------------
// imagepolicy.k8s.io/v1alpha1 wire types (staging/src/k8s.io/api/imagepolicy/
// v1alpha1/types.go)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImageReviewContainerSpec {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    image: String,
}

/// `ImageReviewSpec`; also the response-cache key (`json.Marshal(review.Spec)`,
/// admission.go:164), so annotations are a sorted map as Go marshals them.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImageReviewSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    containers: Vec<ImageReviewContainerSpec>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    annotations: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    namespace: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImageReviewStatus {
    #[serde(default)]
    allowed: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    reason: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    audit_annotations: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ImageReviewRequest<'a> {
    api_version: &'static str,
    kind: &'static str,
    spec: &'a ImageReviewSpec,
    status: ImageReviewStatus,
}

#[derive(Debug, Deserialize)]
struct ImageReviewResponse {
    #[serde(default)]
    status: ImageReviewStatus,
}

/// One failed attempt of the webhook call.
struct CallError {
    message: String,
    retry: bool,
}

/// What a validation leaves for the caller beyond allow/deny: the audit
/// annotations `attributes.AddAnnotation` collects and whether the review
/// failed open.
#[derive(Debug, Default)]
pub struct Decision {
    /// `(AuditKeyPrefix + key, value)` pairs, in key order.
    pub audit_annotations: Vec<(String, String)>,
    /// The backend failed and `defaultAllow` admitted the pod
    /// (`webhookError`, admission.go:107-123).
    pub failed_open: bool,
    /// The refusal, if any (`NewForbidden`). `admitPod` adds the backend's
    /// audit annotations before it checks `Allowed` (admission.go:204-215), so
    /// they accompany a denial too.
    pub error: Option<Error>,
}

impl Decision {
    fn allowed() -> Self {
        Self::default()
    }

    /// The `Validate` return value.
    pub fn into_result(self) -> Result<(), Error> {
        self.error.map_or(Ok(()), Err)
    }
}

/// Stamp the legacy fail-open pod annotation (admission.go:112-119).
pub fn mark_failed_open(pod: &mut Pod) {
    pod.metadata
        .annotations
        .get_or_insert_with(Default::default)
        .insert(IMAGE_POLICY_FAILED_OPEN_KEY.to_string(), "true".to_string());
}

/// The plugin (`Plugin`, admission.go:78-85).
pub struct ImagePolicyWebhook {
    config: WebhookClientConfig,
    client: reqwest::Client,
    retry_backoff: Backoff,
    response_cache: LruExpireCache<ImageReviewStatus>,
    allow_ttl: Duration,
    deny_ttl: Duration,
    default_allow: bool,
}

impl ImagePolicyWebhook {
    /// `NewImagePolicyWebhook` (admission.go:225-262) from the configuration
    /// text (`imagePolicy: {...}`, YAML or JSON).
    pub fn from_config(text: &str) -> anyhow::Result<Self> {
        let config: AdmissionConfig = serde_yaml::from_str(text)?;
        let mut wh = config.image_policy_webhook;
        normalize_webhook_config(&mut wh).map_err(|e| anyhow::anyhow!(e))?;
        let client_config = WebhookClientConfig::load(Path::new(&wh.kube_config_file))?;
        let retry_backoff = Backoff {
            duration: Duration::from_nanos(wh.retry_backoff as u64),
            factor: 1.5,
            jitter: 0.2,
            steps: 5,
        };
        Ok(Self {
            client: client_config.build_client()?,
            config: client_config,
            retry_backoff,
            response_cache: LruExpireCache::new(1024),
            allow_ttl: Duration::from_nanos(wh.allow_ttl as u64),
            deny_ttl: Duration::from_nanos(wh.deny_ttl as u64),
            default_allow: wh.default_allow,
        })
    }

    /// `statusTTL` (admission.go:87-92).
    fn status_ttl(&self, status: &ImageReviewStatus) -> Duration {
        if status.allowed {
            self.allow_ttl
        } else {
            self.deny_ttl
        }
    }

    /// `Plugin.Validate` (admission.go:126-179) for a pod CREATE or UPDATE
    /// (`NewHandler(Create, Update)`). A subresource other than
    /// `ephemeralcontainers` is ignored.
    pub async fn validate(
        &self,
        pod: &Pod,
        namespace: &str,
        subresource: Option<&str>,
    ) -> Decision {
        let subresource = subresource.unwrap_or("");
        if !subresource.is_empty() && subresource != EPHEMERAL_CONTAINERS {
            return Decision::allowed();
        }
        let spec = pod.spec.as_ref();
        let images: Vec<String> = if subresource.is_empty() {
            spec.into_iter()
                .flat_map(|s| {
                    s.containers
                        .iter()
                        .chain(s.init_containers.iter().flatten())
                })
                .map(|c| c.image.clone())
                .collect()
        } else {
            spec.into_iter()
                .flat_map(|s| s.ephemeral_containers.iter().flatten())
                .map(|c| c.image.clone())
                .collect()
        };
        let review_spec = ImageReviewSpec {
            containers: images
                .into_iter()
                .map(|image| ImageReviewContainerSpec { image })
                .collect(),
            annotations: filter_annotations(pod),
            namespace: namespace.to_string(),
        };
        let name = attr_name(&pod.metadata);
        let mut decision = self.admit_pod(&review_spec, &name).await;
        // `admission.NewForbidden(attributes, err)` (admission.go:175-177).
        decision.error = decision
            .error
            .take()
            .map(|e| new_forbidden("pods", &name, forbidden_text(e)));
        decision
    }

    /// `admitPod` (admission.go:181-222); the error is the message the caller
    /// wraps in `NewForbidden`.
    async fn admit_pod(&self, spec: &ImageReviewSpec, name: &str) -> Decision {
        let mut decision = Decision::default();
        let cache_key = match serde_json::to_string(spec) {
            Ok(k) => k,
            Err(e) => {
                decision.error = Some(Error::Internal(e.to_string()));
                return decision;
            }
        };
        let status = match self.response_cache.get(&cache_key, Instant::now()) {
            Some(status) => status,
            None => match self.call_with_backoff(spec).await {
                Ok(status) => {
                    self.response_cache.add(
                        cache_key,
                        status.clone(),
                        self.status_ttl(&status),
                        Instant::now(),
                    );
                    status
                }
                Err(message) => return self.webhook_error(name, message),
            },
        };
        for (k, v) in &status.audit_annotations {
            decision
                .audit_annotations
                .push((format!("{AUDIT_KEY_PREFIX}{k}"), v.clone()));
        }
        if !status.allowed {
            decision.error = Some(Error::Forbidden(if status.reason.is_empty() {
                "one or more images rejected by webhook backend".to_string()
            } else {
                format!(
                    "image policy webhook backend denied one or more images: {}",
                    status.reason
                )
            }));
        }
        decision
    }

    /// `webhookError` (admission.go:100-124): fail open when `defaultAllow`,
    /// else a forbidden error (wrapped again by `Validate`, as upstream).
    fn webhook_error(&self, name: &str, err: String) -> Decision {
        tracing::debug!("error contacting webhook backend: {err}");
        if self.default_allow {
            return Decision {
                audit_annotations: vec![(
                    format!("{AUDIT_KEY_PREFIX}{FAILED_OPEN_KEY_SUFFIX}"),
                    "true".to_string(),
                )],
                failed_open: true,
                error: None,
            };
        }
        Decision {
            error: Some(new_forbidden("pods", name, err)),
            ..Decision::default()
        }
    }

    /// One attempt: POST the review, decode the status (`rest.Result`:
    /// `Error()`, `StatusCode`, `Into`).
    async fn call_once(&self, spec: &ImageReviewSpec) -> Result<ImageReviewStatus, CallError> {
        let body = ImageReviewRequest {
            api_version: "imagepolicy.k8s.io/v1alpha1",
            kind: "ImageReview",
            spec,
            status: ImageReviewStatus::default(),
        };
        let resp = self
            .config
            .post(&self.client)
            .json(&body)
            .send()
            .await
            .map_err(|e| CallError {
                retry: is_connection_reset(&e),
                message: e.to_string(),
            })?;
        let status = resp.status();
        if status.is_client_error() || status.is_server_error() {
            // DefaultShouldRetry (webhook.go:47-56): internal error, timeout,
            // too many requests, or a Retry-After header.
            let suggests_delay = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.trim().parse::<u64>().is_ok());
            return Err(CallError {
                message: format!(
                    "the server responded with the status code {}",
                    status.as_u16()
                ),
                retry: matches!(status.as_u16(), 500 | 504 | 408 | 429) || suggests_delay,
            });
        }
        if !status.is_success() {
            return Err(CallError {
                message: format!("Error contacting webhook: {}", status.as_u16()),
                retry: false,
            });
        }
        let parsed: ImageReviewResponse = resp.json().await.map_err(|e| CallError {
            message: e.to_string(),
            retry: false,
        })?;
        Ok(parsed.status)
    }

    /// `WithExponentialBackoff` (webhook.go:105-135) over
    /// `wait.ExponentialBackoff`: at most `steps` attempts; a non-retriable
    /// error ends it at once.
    async fn call_with_backoff(&self, spec: &ImageReviewSpec) -> Result<ImageReviewStatus, String> {
        let mut backoff = self.retry_backoff.clone();
        let mut last = String::new();
        while backoff.steps > 0 {
            match self.call_once(spec).await {
                Ok(s) => return Ok(s),
                Err(e) => {
                    last = e.message;
                    if !e.retry {
                        return Err(last);
                    }
                }
            }
            if backoff.steps == 1 {
                break;
            }
            tokio::time::sleep(backoff.step()).await;
        }
        Err(last)
    }
}

/// The message of a refusal, without the `Forbidden: ` display prefix.
fn forbidden_text(e: Error) -> String {
    match e {
        Error::Forbidden(m) => m,
        other => other.to_string(),
    }
}

/// `filterAnnotations` (admission.go:94-98): only `*.image-policy.k8s.io/*`.
fn filter_annotations(pod: &Pod) -> BTreeMap<String, String> {
    pod.metadata
        .annotations
        .iter()
        .flatten()
        .filter(|(k, _)| k.contains(".image-policy.k8s.io/"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

static INSTALLED: OnceLock<Arc<ImagePolicyWebhook>> = OnceLock::new();

/// Build the plugin from its configuration when it is enabled and install it
/// process-wide. An enabled plugin without a configuration is a startup error
/// (`NewImagePolicyWebhook`: `no config specified`, admission.go:226-228).
pub fn install_from_config(text: Option<&str>) -> anyhow::Result<()> {
    if !super::plugins::enabled(PLUGIN_NAME) {
        return Ok(());
    }
    let text = text.ok_or_else(|| anyhow::anyhow!("no config specified"))?;
    let plugin =
        ImagePolicyWebhook::from_config(text).map_err(|e| anyhow::anyhow!("{PLUGIN_NAME}: {e}"))?;
    let _ = INSTALLED.set(Arc::new(plugin));
    Ok(())
}

/// The installed plugin, if enabled and configured.
pub fn installed() -> Option<Arc<ImagePolicyWebhook>> {
    INSTALLED.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    async fn run(
        wh: &ImagePolicyWebhook,
        pod: &Pod,
        ns: &str,
        sub: Option<&str>,
    ) -> Result<Decision, Error> {
        let mut d = wh.validate(pod, ns, sub).await;
        match d.error.take() {
            Some(e) => Err(e),
            None => Ok(d),
        }
    }

    // config_test.go TestConfigNormalization.
    fn cfg(allow: i64, deny: i64, retry: i64) -> ImagePolicyWebhookConfig {
        ImagePolicyWebhookConfig {
            allow_ttl: allow,
            deny_ttl: deny,
            retry_backoff: retry,
            ..Default::default()
        }
    }

    #[test]
    fn config_within_normal_ranges() {
        let mut c = cfg(
            (MIN_ALLOW_TTL + MAX_ALLOW_TTL) / 2 / SEC,
            (MIN_DENY_TTL + MAX_DENY_TTL) / 2 / SEC,
            (MIN_RETRY_BACKOFF + MAX_RETRY_BACKOFF) / 2 / MS,
        );
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!(c.allow_ttl, (MIN_ALLOW_TTL + MAX_ALLOW_TTL) / 2 / SEC * SEC);
        assert_eq!(c.deny_ttl, (MIN_DENY_TTL + MAX_DENY_TTL) / 2 / SEC * SEC);
        assert_eq!(
            c.retry_backoff,
            (MIN_RETRY_BACKOFF + MAX_RETRY_BACKOFF) / 2 / MS * MS
        );
    }

    #[test]
    fn config_out_of_range_is_an_error() {
        // below: minAllowTTL - 1ns scaled by a second is still out of range
        // above max; a negative value other than -1 is below min.
        assert!(normalize_webhook_config(&mut cfg(MIN_ALLOW_TTL - 1, 0, 0)).is_err());
        assert!(normalize_webhook_config(&mut cfg(0, MIN_DENY_TTL - 1, 0)).is_err());
        assert!(normalize_webhook_config(&mut cfg(0, 0, MIN_RETRY_BACKOFF - 4)).is_err());
        assert!(normalize_webhook_config(&mut cfg(1 + MAX_ALLOW_TTL, 0, 0)).is_err());
        assert!(normalize_webhook_config(&mut cfg(0, 1 + MAX_DENY_TTL, 0)).is_err());
        assert!(normalize_webhook_config(&mut cfg(0, 0, 1 + MAX_RETRY_BACKOFF)).is_err());
        assert!(normalize_webhook_config(&mut cfg(-2, 0, 0)).is_err());
    }

    #[test]
    fn config_default_and_disabled_sentinels() {
        let mut c = cfg(USE_DEFAULT, USE_DEFAULT, USE_DEFAULT);
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!(
            (c.allow_ttl, c.deny_ttl, c.retry_backoff),
            (DEFAULT_ALLOW_TTL, DEFAULT_DENY_TTL, DEFAULT_RETRY_BACKOFF)
        );
        let mut c = cfg(DISABLE_TTL, DISABLE_TTL, DISABLE_TTL);
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!((c.allow_ttl, c.deny_ttl, c.retry_backoff), (0, 0, 0));
    }

    #[test]
    fn config_min_and_max_values() {
        let mut c = cfg(MIN_ALLOW_TTL / SEC, MIN_DENY_TTL / SEC, MIN_RETRY_BACKOFF);
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!(
            (c.allow_ttl, c.deny_ttl, c.retry_backoff),
            (MIN_ALLOW_TTL, MIN_DENY_TTL, MIN_RETRY_BACKOFF * MS)
        );
        let mut c = cfg(
            MAX_ALLOW_TTL / SEC,
            MAX_DENY_TTL / SEC,
            MAX_RETRY_BACKOFF / MS,
        );
        normalize_webhook_config(&mut c).unwrap();
        assert_eq!(
            (c.allow_ttl, c.deny_ttl, c.retry_backoff),
            (MAX_ALLOW_TTL, MAX_DENY_TTL, MAX_RETRY_BACKOFF)
        );
    }

    // --- a mock backend (admission_test.go `mockService` / `NewTestServer`)

    #[derive(Default)]
    struct MockState {
        allow: bool,
        status_code: u16,
        out_annotations: BTreeMap<String, String>,
        seen_annotations: Option<BTreeMap<String, String>>,
        calls: usize,
        seen_bodies: Vec<serde_json::Value>,
    }

    async fn handle(
        axum::extract::State(st): axum::extract::State<Arc<Mutex<MockState>>>,
        axum::Json(body): axum::Json<serde_json::Value>,
    ) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
        let mut st = st.lock().unwrap();
        st.calls += 1;
        st.seen_bodies.push(body.clone());
        let images: Vec<&str> = body["spec"]["containers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| c["image"].as_str())
            .collect();
        // `mockService.Review`: allowed unless an image is "bad" (and the
        // service allows).
        let allowed = st.allow && !images.contains(&"bad");
        let seen: BTreeMap<String, String> = body["spec"]["annotations"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
            .collect();
        st.seen_annotations = Some(seen);
        let code = axum::http::StatusCode::from_u16(st.status_code).unwrap();
        let mut resp = body;
        resp["status"] = serde_json::json!({
            "allowed": allowed,
            "auditAnnotations": st.out_annotations,
        });
        (code, axum::Json(resp))
    }

    struct Server {
        state: Arc<Mutex<MockState>>,
        url: String,
        dir: std::path::PathBuf,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn server(status_code: u16, allow: bool) -> Server {
        let state = Arc::new(Mutex::new(MockState {
            allow,
            status_code,
            ..Default::default()
        }));
        let app = axum::Router::new()
            .route("/", axum::routing::post(handle))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = std::env::temp_dir().join(format!(
            "imagepolicy-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Server { state, url, dir }
    }

    /// `newImagePolicyWebhook` (admission_test.go:362): a webhook against the
    /// server through a kubeconfig + config file.
    fn plugin(s: &Server, cache_secs: i64, default_allow: bool) -> ImagePolicyWebhook {
        write_plugin(&s.dir, &s.url, cache_secs, default_allow)
    }

    fn write_plugin(
        dir: &Path,
        url: &str,
        cache_secs: i64,
        default_allow: bool,
    ) -> ImagePolicyWebhook {
        let kc = dir.join("kubeconfig");
        std::fs::write(
            &kc,
            format!(
                "clusters:\n- name: c\n  cluster:\n    server: {url}\nusers:\n- name: u\n  user:\n    token: t\ncontexts:\n- name: x\n  context:\n    cluster: c\n    user: u\ncurrent-context: x\n"
            ),
        )
        .unwrap();
        let text = format!(
            "imagePolicy:\n  kubeConfigFile: {}\n  allowTTL: {cache_secs}\n  denyTTL: {cache_secs}\n  retryBackoff: 1\n  defaultAllow: {default_allow}\n",
            kc.display()
        );
        ImagePolicyWebhook::from_config(&text).unwrap()
    }

    fn good_pod(image: &str) -> Pod {
        Pod::new(
            "",
            rusternetes_common::resources::PodSpec {
                containers: vec![container(image)],
                ..Default::default()
            },
        )
    }

    fn container(image: &str) -> rusternetes_common::resources::Container {
        rusternetes_common::resources::Container {
            name: "c".into(),
            image: image.into(),
            ..Default::default()
        }
    }

    // TestNewFromConfig: kubeconfig selection and failures.
    #[test]
    fn new_from_config_kubeconfig_cases() {
        let dir = std::env::temp_dir().join(format!("imagepolicy-kc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = dir.join("ca.pem");
        std::fs::write(&ca, "x").unwrap();
        let cases = [
            // a single cluster, no context: the current context is unnamed
            // and matches no cluster name -> error
            (
                String::from("clusters:\n- name: foobar\n  cluster:\n    server: https://a.example.com\nusers:\n- name: n\n  user: {}\n"),
                true,
            ),
            // a context selects the cluster; its CA must be loadable
            (
                String::from("clusters:\n- name: foobar\n  cluster:\n    certificate-authority: a bad certificate path\n    server: https://a.example.com\n- name: barfoo\n  cluster:\n    server: https://a.example.com\nusers:\n- name: n\n  user: {}\ncontexts:\n- name: default\n  context:\n    cluster: barfoo\n    user: n\ncurrent-context: default\n"),
                false,
            ),
            (
                String::from("clusters:\n- name: foobar\n  cluster:\n    certificate-authority: a bad certificate path\n    server: https://a.example.com\n- name: barfoo\n  cluster:\n    server: https://a.example.com\nusers:\n- name: n\n  user: {}\ncontexts:\n- name: default\n  context:\n    cluster: foobar\n    user: n\ncurrent-context: default\n"),
                true,
            ),
        ];
        for (i, (kc, want_err)) in cases.iter().enumerate() {
            let p = dir.join(format!("kc{i}"));
            std::fs::write(&p, kc).unwrap();
            let text = format!(
                "imagePolicy:\n  kubeConfigFile: {}\n  allowTTL: 500\n  denyTTL: 500\n  retryBackoff: 500\n  defaultAllow: true\n",
                p.display()
            );
            let r = ImagePolicyWebhook::from_config(&text);
            assert_eq!(r.is_err(), *want_err, "case {i}: {:?}", r.err());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_config_and_missing_kubeconfig_fail() {
        let _g = crate::admission::plugins::EnabledForTest::new(&[PLUGIN_NAME]);
        assert!(install_from_config(None)
            .unwrap_err()
            .to_string()
            .contains("no config specified"));
        assert!(ImagePolicyWebhook::from_config("imagePolicy:\n  defaultAllow: true\n").is_err());
        assert!(ImagePolicyWebhook::from_config("imagePolicy:\n  allowTTL: -5\n").is_err());
    }

    #[test]
    fn disabled_plugin_needs_no_config() {
        let _g = crate::admission::plugins::EnabledForTest::new(&[]);
        install_from_config(None).unwrap();
    }

    // TestWebhookCache: error responses are not cached, successes are.
    #[tokio::test]
    async fn webhook_cache() {
        let s = server(500, true).await;
        let wh = plugin(&s, 200, false);
        let pod = good_pod("test");
        let cases = [
            (500u16, false),
            (404, false),
            (403, false),
            (401, false),
            (200, true),
            (500, true), // cached
        ];
        for (code, ok) in cases {
            s.state.lock().unwrap().status_code = code;
            let r = run(&wh, &pod, "namespace", None).await;
            assert_eq!(r.is_ok(), ok, "status {code}: {r:?}");
        }
        // A different request calls the webhook again.
        let pod2 = good_pod("test2");
        for (code, ok) in [(500u16, false), (200, true), (500, true)] {
            s.state.lock().unwrap().status_code = code;
            let r = run(&wh, &pod2, "namespace", None).await;
            assert_eq!(r.is_ok(), ok, "status {code}: {r:?}");
        }
    }

    // TestContainerCombinations.
    #[tokio::test]
    async fn container_combinations() {
        let s = server(200, true).await;
        let wh = plugin(&s, -1, false);
        let mut cases: Vec<(&str, Pod, bool)> = vec![
            ("single allowed", good_pod("good"), true),
            ("single denied", good_pod("bad"), false),
        ];
        for (name, images, init, ok) in [
            ("one good one bad", vec!["bad", "good"], vec![], false),
            ("multiple good", vec!["good", "good"], vec![], true),
            ("multiple bad", vec!["bad", "bad"], vec![], false),
            ("good, bad init", vec!["good"], vec!["bad"], false),
            ("good, good init", vec!["good"], vec!["good"], true),
        ] {
            let mut pod = good_pod("x");
            let spec = pod.spec.as_mut().unwrap();
            spec.containers = images.iter().map(|i| container(i)).collect();
            if !init.is_empty() {
                spec.init_containers = Some(init.iter().map(|i| container(i)).collect());
            }
            cases.push((name, pod, ok));
        }
        for (name, pod, ok) in cases {
            s.state.lock().unwrap().allow = true;
            let r = run(&wh, &pod, "ns", None).await;
            assert_eq!(r.is_ok(), ok, "{name}: {r:?}");
            // With a deny-all backend nothing is admitted.
            s.state.lock().unwrap().allow = false;
            assert!(
                run(&wh, &pod, "ns", None).await.is_err(),
                "{name}: deny-all"
            );
        }
    }

    // Containers and init containers are all reviewed, in order, with the
    // namespace; ephemeralcontainers reviews only the ephemeral containers;
    // other subresources are ignored (admission.go:128-153).
    #[tokio::test]
    async fn review_body_and_subresources() {
        let s = server(200, true).await;
        let wh = plugin(&s, -1, false);
        let mut pod = good_pod("a");
        let spec = pod.spec.as_mut().unwrap();
        spec.init_containers = Some(vec![container("i")]);
        spec.ephemeral_containers = Some(vec![rusternetes_common::resources::EphemeralContainer {
            name: "e".into(),
            image: "eph".into(),
            ..Default::default()
        }]);
        run(&wh, &pod, "ns1", None).await.unwrap();
        run(&wh, &pod, "ns1", Some("ephemeralcontainers"))
            .await
            .unwrap();
        let d = run(&wh, &pod, "ns1", Some("status")).await.unwrap();
        assert!(!d.failed_open && d.audit_annotations.is_empty());
        let st = s.state.lock().unwrap();
        assert_eq!(st.calls, 2, "the status subresource is not reviewed");
        let b = &st.seen_bodies[0];
        assert_eq!(b["apiVersion"], "imagepolicy.k8s.io/v1alpha1");
        assert_eq!(b["kind"], "ImageReview");
        assert_eq!(b["spec"]["namespace"], "ns1");
        assert_eq!(
            b["spec"]["containers"],
            serde_json::json!([{"image": "a"}, {"image": "i"}])
        );
        assert_eq!(
            st.seen_bodies[1]["spec"]["containers"],
            serde_json::json!([{"image": "eph"}])
        );
    }

    // TestDefaultAllow: an erroring backend fails open or closed.
    #[tokio::test]
    async fn default_allow() {
        for (default_allow, image) in [
            (true, "bad"),
            (true, "good"),
            (false, "good"),
            (false, "bad"),
        ] {
            let s = server(500, true).await;
            let wh = plugin(&s, -1, default_allow);
            let pod = good_pod(image);
            let r = run(&wh, &pod, "namespace", None).await;
            if default_allow {
                let d = r.unwrap();
                assert!(d.failed_open);
                assert_eq!(
                    d.audit_annotations,
                    vec![(
                        "imagepolicywebhook.image-policy.k8s.io/failed-open".to_string(),
                        "true".to_string()
                    )]
                );
                let mut pod = pod;
                mark_failed_open(&mut pod);
                assert_eq!(
                    pod.metadata.annotations.unwrap()[IMAGE_POLICY_FAILED_OPEN_KEY],
                    "true"
                );
            } else {
                let e = r.unwrap_err().to_string();
                assert!(e.contains("is forbidden"), "{e}");
            }
        }
    }

    // A refused connection is also a backend failure.
    #[tokio::test]
    async fn unreachable_backend() {
        let dir = std::env::temp_dir().join(format!("imagepolicy-un-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let wh = write_plugin(&dir, "http://127.0.0.1:1/", -1, false);
        assert!(run(&wh, &good_pod("good"), "ns", None).await.is_err());
        let wh = write_plugin(&dir, "http://127.0.0.1:1/", -1, true);
        assert!(
            run(&wh, &good_pod("good"), "ns", None)
                .await
                .unwrap()
                .failed_open
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // TestAnnotationFiltering.
    #[tokio::test]
    async fn annotation_filtering() {
        let all: HashMap<String, String> = [
            ("my.image-policy.k8s.io/test", "test"),
            ("other.image-policy.k8s.io/test2", "annotation"),
            ("test", "test"),
            ("another", "another"),
            ("", ""),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        for (annotations, want) in [
            (
                HashMap::from([
                    ("test".to_string(), "test".to_string()),
                    ("another".to_string(), "annotation".to_string()),
                    (String::new(), String::new()),
                ]),
                BTreeMap::new(),
            ),
            (
                all,
                BTreeMap::from([
                    (
                        "my.image-policy.k8s.io/test".to_string(),
                        "test".to_string(),
                    ),
                    (
                        "other.image-policy.k8s.io/test2".to_string(),
                        "annotation".to_string(),
                    ),
                ]),
            ),
        ] {
            let s = server(200, true).await;
            let wh = plugin(&s, -1, true);
            let mut pod = good_pod("test");
            pod.metadata.annotations = Some(annotations);
            run(&wh, &pod, "ns", None).await.unwrap();
            assert_eq!(
                s.state.lock().unwrap().seen_annotations.clone().unwrap(),
                want
            );
        }
    }

    // TestReturnedAnnotationAdd.
    #[tokio::test]
    async fn returned_annotations_are_prefixed() {
        let ann = |kv: &[(&str, &str)]| -> BTreeMap<String, String> {
            kv.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let cases = [
            (
                "good",
                ann(&[("foo-test", "true"), ("bar-test", "false")]),
                vec![
                    ("imagepolicywebhook.image-policy.k8s.io/bar-test", "false"),
                    ("imagepolicywebhook.image-policy.k8s.io/foo-test", "true"),
                ],
                false,
            ),
            ("good", ann(&[]), vec![], false),
            (
                "bad",
                ann(&[("foo-test", "false")]),
                vec![("imagepolicywebhook.image-policy.k8s.io/foo-test", "false")],
                true,
            ),
        ];
        for (image, out, want, want_err) in cases {
            let s = server(200, true).await;
            s.state.lock().unwrap().out_annotations = out;
            let wh = plugin(&s, -1, true);
            let d = wh.validate(&good_pod(image), "ns", None).await;
            assert_eq!(d.error.is_some(), want_err, "{image}");
            let want: Vec<(String, String)> = want
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            assert_eq!(d.audit_annotations, want);
        }
    }

    // The denial text (admission.go:210-215), wrapped by NewForbidden.
    #[tokio::test]
    async fn denial_messages() {
        let s = server(200, false).await;
        let wh = plugin(&s, -1, false);
        let e = run(&wh, &good_pod("good"), "ns", None).await.unwrap_err();
        assert_eq!(
            e.to_string().trim_start_matches("Forbidden: "),
            "pods \"Unknown\" is forbidden: one or more images rejected by webhook backend"
        );
    }
}
