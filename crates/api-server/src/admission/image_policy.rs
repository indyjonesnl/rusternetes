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
#[allow(dead_code)] // the pod-side half; see the module's deviation note
pub const IMAGE_POLICY_FAILED_OPEN_KEY: &str = "alpha.image-policy.k8s.io/failed-open";

const EPHEMERAL_CONTAINERS: &str = "ephemeralcontainers";

// config.go:28-40, in nanoseconds (`time.Duration`).
pub const MS: i64 = 1_000_000;
pub const SEC: i64 = 1_000_000_000;
pub const DEFAULT_RETRY_BACKOFF: i64 = 500 * MS;
pub const MIN_RETRY_BACKOFF: i64 = 1;
pub const MAX_RETRY_BACKOFF: i64 = 5 * 60 * SEC;
pub const DEFAULT_ALLOW_TTL: i64 = 5 * 60 * SEC;
pub const DEFAULT_DENY_TTL: i64 = 30 * SEC;
pub const MIN_ALLOW_TTL: i64 = SEC;
pub const MAX_ALLOW_TTL: i64 = 30 * 60 * SEC;
pub const MIN_DENY_TTL: i64 = SEC;
pub const MAX_DENY_TTL: i64 = 30 * 60 * SEC;
/// `useDefault`: the sentinel for using the default TTL.
pub const USE_DEFAULT: i64 = 0;
/// `disableTTL`: the sentinel for disabling a TTL.
pub const DISABLE_TTL: i64 = -1;

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
    #[allow(dead_code)] // read by `mark_failed_open` callers and the tests
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
#[allow(dead_code)] // see the module's deviation note
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
