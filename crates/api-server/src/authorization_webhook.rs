//! `--authorization-mode=Webhook` and its `--authorization-webhook-*` flags (#2895).
//!
//! Ported from
//! - `staging/src/k8s.io/apiserver/plugin/pkg/authorizer/webhook/webhook.go`
//!   (`WebhookAuthorizer`, `Authorize` :188-299, `shouldCache` :537,
//!   `subjectAccessReviewInterfaceFromConfig` :441-478, the v1beta1 conversions
//!   :549-605),
//! - `staging/src/k8s.io/apiserver/pkg/util/webhook/webhook.go`
//!   (`DefaultRetryBackoffWithInitialDelay` :26, `DefaultShouldRetry` :47,
//!   `WithExponentialBackoff` :105),
//! - `pkg/kubeapiserver/authorizer/reload.go` (the Webhook case :124-167),
//! - `pkg/kubeapiserver/options/authorization.go` (flags :179-192, Validate
//!   :142-160, `ToAuthorizationConfiguration` :270-290).
//!
//! Not ported (tracked separately): `matchConditions` (CEL; legacy flags never
//! set any), selector attributes (`AuthorizeWithSelectors`; `RequestAttributes`
//! carries no selectors) and the webhook metrics.

use async_trait::async_trait;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::error::{Error, Result as AuthzResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// `maxControlledAttrCacheSize` (webhook.go:56).
const MAX_CONTROLLED_ATTR_CACHE_SIZE: usize = 10000;
/// `cache.NewLRUExpireCache(8192)` (webhook.go:130).
const RESPONSE_CACHE_SIZE: usize = 8192;
/// `defaultRequestTimeout` (util/webhook/webhook.go:39), also the legacy flags'
/// implicit `Timeout` (options/authorization.go:282).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

const DEFAULT_WEBHOOK_VERSION: &str = "v1beta1";
const DEFAULT_CACHE_AUTHORIZED_TTL: Duration = Duration::from_secs(5 * 60);
const DEFAULT_CACHE_UNAUTHORIZED_TTL: Duration = Duration::from_secs(30);

/// The webhook flags of `BuiltInAuthorizationOptions` (authorization.go:55-58,
/// :179-192, defaults :79-82).
#[derive(clap::Args, Debug, Clone)]
pub struct AuthorizationWebhookArgs {
    /// File with webhook configuration in kubeconfig format, used with
    /// --authorization-mode=Webhook. The API server will query the remote
    /// service to determine access on the API server's secure port.
    #[arg(
        id = "authorization_webhook_config_file",
        long = "authorization-webhook-config-file"
    )]
    pub config_file: Option<String>,

    /// The API version of the authorization.k8s.io SubjectAccessReview to send
    /// to and expect from the webhook.
    #[arg(
        id = "authorization_webhook_version",
        long = "authorization-webhook-version",
        default_value = DEFAULT_WEBHOOK_VERSION
    )]
    pub version: String,

    /// The duration to cache 'authorized' responses from the webhook
    /// authorizer.
    #[arg(
        id = "authorization_webhook_cache_authorized_ttl",
        long = "authorization-webhook-cache-authorized-ttl",
        default_value = "5m",
        value_parser = parse_go_duration
    )]
    pub cache_authorized_ttl: Duration,

    /// The duration to cache 'unauthorized' responses from the webhook
    /// authorizer.
    #[arg(
        id = "authorization_webhook_cache_unauthorized_ttl",
        long = "authorization-webhook-cache-unauthorized-ttl",
        default_value = "30s",
        value_parser = parse_go_duration
    )]
    pub cache_unauthorized_ttl: Duration,
}

impl Default for AuthorizationWebhookArgs {
    fn default() -> Self {
        Self {
            config_file: None,
            version: DEFAULT_WEBHOOK_VERSION.to_string(),
            cache_authorized_ttl: DEFAULT_CACHE_AUTHORIZED_TTL,
            cache_unauthorized_ttl: DEFAULT_CACHE_UNAUTHORIZED_TTL,
        }
    }
}

/// The webhook-specific half of `BuiltInAuthorizationOptions.Validate`
/// (authorization.go:142-153; the retry-steps check at :159 cannot fire, the
/// backoff is not a flag).
pub fn validate_webhook_args(modes: &[String], args: &AuthorizationWebhookArgs) -> Vec<String> {
    let mut errs = Vec::new();
    let has_webhook = modes.iter().any(|m| m == "Webhook");
    let file_set = args.config_file.as_deref().is_some_and(|f| !f.is_empty());
    if has_webhook && !file_set {
        errs.push("authorization-mode Webhook's authorization config file not passed".to_string());
    }
    if file_set && !has_webhook {
        errs.push(
            "cannot specify --authorization-webhook-config-file without mode Webhook".to_string(),
        );
    }
    errs
}

/// Go `time.ParseDuration`, for the flag values (`fs.DurationVar`).
pub fn parse_go_duration(s: &str) -> std::result::Result<Duration, String> {
    let bad = || format!("invalid duration {s:?}");
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    if s.is_empty() {
        return Err(bad());
    }
    let mut rest = s;
    let mut total = 0f64;
    while !rest.is_empty() {
        let num_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if num_end == 0 {
            return Err(bad());
        }
        let n: f64 = rest[..num_end].parse().map_err(|_| bad())?;
        rest = &rest[num_end..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let unit = match &rest[..unit_end] {
            "ns" => 1e-9,
            "us" | "µs" | "μs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            "" => return Err(format!("missing unit in duration {s:?}")),
            u => return Err(format!("unknown unit {u:?} in duration {s:?}")),
        };
        rest = &rest[unit_end..];
        total += n * unit;
    }
    Ok(Duration::from_secs_f64(total))
}

/// `wait.Backoff`.
#[derive(Debug, Clone)]
pub struct Backoff {
    pub duration: Duration,
    pub factor: f64,
    pub jitter: f64,
    pub steps: u32,
}

impl Backoff {
    /// `DefaultAuthWebhookRetryBackoff`
    /// (`apiserver/pkg/server/options/authentication.go:46`) ==
    /// `DefaultRetryBackoffWithInitialDelay(500ms)`.
    pub fn default_auth_webhook() -> Self {
        Self {
            duration: Duration::from_millis(500),
            factor: 1.5,
            jitter: 0.2,
            steps: 5,
        }
    }

    /// `Backoff.Step`: the next sleep, then grow.
    fn step(&mut self) -> Duration {
        self.steps = self.steps.saturating_sub(1);
        let mut d = self.duration;
        if self.factor != 0.0 {
            self.duration = self.duration.mul_f64(self.factor);
        }
        if self.jitter > 0.0 {
            // wait.Jitter: d + rand*jitter*d
            d += d.mul_f64(rand::random::<f64>() * self.jitter);
        }
        d
    }
}

/// `authzconfig.FailurePolicy` (`reload.go:136-143`): the decision when the
/// webhook cannot be consulted. The legacy flags use `NoOpinion`
/// (authorization.go:283).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailurePolicy {
    NoOpinion,
    Deny,
}

// ---------------------------------------------------------------------------
// kubeconfig (`webhookutil.LoadKubeconfig`)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct Kubeconfig {
    #[serde(default)]
    current_context: String,
    #[serde(default)]
    contexts: Vec<NamedContext>,
    #[serde(default)]
    clusters: Vec<NamedCluster>,
    #[serde(default)]
    users: Vec<NamedUser>,
}

#[derive(Debug, Deserialize)]
struct NamedContext {
    #[serde(default)]
    name: String,
    #[serde(default)]
    context: ContextSpec,
}

#[derive(Debug, Deserialize, Default)]
struct ContextSpec {
    #[serde(default)]
    cluster: String,
    #[serde(default)]
    user: String,
}

#[derive(Debug, Deserialize)]
struct NamedCluster {
    #[serde(default)]
    name: String,
    #[serde(default)]
    cluster: ClusterSpec,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct ClusterSpec {
    #[serde(default)]
    server: String,
    certificate_authority: Option<String>,
    certificate_authority_data: Option<String>,
    #[serde(default)]
    insecure_skip_tls_verify: bool,
}

#[derive(Debug, Deserialize)]
struct NamedUser {
    #[serde(default)]
    name: String,
    #[serde(default)]
    user: UserSpec,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "kebab-case")]
struct UserSpec {
    client_certificate: Option<String>,
    client_certificate_data: Option<String>,
    client_key: Option<String>,
    client_key_data: Option<String>,
    token: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

/// What the webhook's kubeconfig resolves to: the exact URL the review is
/// POSTed to (`cluster.server` is used as-is, webhook.go:438-440), the TLS
/// material and the credentials.
#[derive(Debug, Clone)]
pub struct WebhookClientConfig {
    server: String,
    ca_pem: Option<Vec<u8>>,
    insecure_skip_tls_verify: bool,
    /// client certificate chain + key, concatenated PEM.
    identity_pem: Option<Vec<u8>>,
    bearer_token: Option<String>,
    basic_auth: Option<(String, String)>,
}

fn pem_from(
    data_b64: &Option<String>,
    file: &Option<String>,
    base: &Path,
    what: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    use base64::Engine;
    if let Some(d) = data_b64.as_deref().filter(|d| !d.is_empty()) {
        return Ok(Some(
            base64::engine::general_purpose::STANDARD
                .decode(d.trim())
                .map_err(|e| anyhow::anyhow!("{what}-data is not valid base64: {e}"))?,
        ));
    }
    if let Some(f) = file.as_deref().filter(|f| !f.is_empty()) {
        // clientcmd resolves relative paths against the kubeconfig's directory.
        let p = base.join(f);
        return Ok(Some(
            std::fs::read(&p).map_err(|e| anyhow::anyhow!("reading {what} {p:?}: {e}"))?,
        ));
    }
    Ok(None)
}

impl WebhookClientConfig {
    /// `webhookutil.LoadKubeconfig`: pick the current context's cluster and
    /// user (by name; clientcmd leaves a missing context empty, which matches
    /// unnamed entries).
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading webhook kubeconfig {path:?}: {e}"))?;
        let kc: Kubeconfig = serde_yaml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("parsing webhook kubeconfig {path:?}: {e}"))?;
        let ctx = kc.contexts.iter().find(|c| c.name == kc.current_context);
        let (cluster_name, user_name) = ctx
            .map(|c| (c.context.cluster.as_str(), c.context.user.as_str()))
            .unwrap_or(("", ""));
        let cluster = kc
            .clusters
            .iter()
            .find(|c| c.name == cluster_name)
            .map(|c| &c.cluster)
            .ok_or_else(|| anyhow::anyhow!("cluster {cluster_name:?} not found in {path:?}"))?;
        if cluster.server.is_empty() {
            anyhow::bail!(
                "invalid webhook kubeconfig {path:?}: no server found for cluster {cluster_name:?}"
            );
        }
        let user = kc
            .users
            .iter()
            .find(|u| u.name == user_name)
            .map(|u| &u.user)
            .cloned()
            .unwrap_or_default();
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        let cert = pem_from(
            &user.client_certificate_data,
            &user.client_certificate,
            base,
            "client-certificate",
        )?;
        let key = pem_from(&user.client_key_data, &user.client_key, base, "client-key")?;
        let identity_pem = match (cert, key) {
            (Some(mut c), Some(k)) => {
                c.push(b'\n');
                c.extend(k);
                Some(c)
            }
            (None, None) => None,
            _ => anyhow::bail!("client-certificate and client-key must be given together"),
        };
        Ok(Self {
            server: cluster.server.clone(),
            ca_pem: pem_from(
                &cluster.certificate_authority_data,
                &cluster.certificate_authority,
                base,
                "certificate-authority",
            )?,
            insecure_skip_tls_verify: cluster.insecure_skip_tls_verify,
            identity_pem,
            bearer_token: user.token.clone().filter(|t| !t.is_empty()),
            basic_auth: match (&user.username, &user.password) {
                (Some(u), p) if !u.is_empty() => Some((u.clone(), p.clone().unwrap_or_default())),
                _ => None,
            },
        })
    }

    fn build_client(&self) -> anyhow::Result<reqwest::Client> {
        let mut b = reqwest::Client::builder().timeout(REQUEST_TIMEOUT);
        if self.insecure_skip_tls_verify {
            b = b.danger_accept_invalid_certs(true);
        }
        if let Some(ca) = &self.ca_pem {
            for cert in reqwest::Certificate::from_pem_bundle(ca)? {
                b = b.add_root_certificate(cert);
            }
        }
        if let Some(id) = &self.identity_pem {
            b = b.identity(reqwest::Identity::from_pem(id)?);
        }
        Ok(b.build()?)
    }
}

// ---------------------------------------------------------------------------
// SubjectAccessReview wire types (authorization.k8s.io/v1, v1beta1 identical)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct SarSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_attributes: Option<SarResourceAttributes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    non_resource_attributes: Option<SarNonResourceAttributes>,
    #[serde(skip_serializing_if = "String::is_empty")]
    user: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    groups: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    extra: BTreeMap<String, Vec<String>>,
    #[serde(skip_serializing_if = "String::is_empty")]
    uid: String,
}

#[derive(Debug, Serialize, Default, Clone)]
struct SarResourceAttributes {
    #[serde(skip_serializing_if = "String::is_empty")]
    namespace: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    verb: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    group: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    version: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    resource: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    subresource: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    name: String,
}

#[derive(Debug, Serialize, Default, Clone)]
struct SarNonResourceAttributes {
    #[serde(skip_serializing_if = "String::is_empty")]
    path: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    verb: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarRequest<'a> {
    api_version: &'static str,
    kind: &'static str,
    spec: &'a SarSpec,
    status: SarStatus,
}

/// `SubjectAccessReviewStatus` (identical in v1 and v1beta1 for the fields the
/// webhook answers with; `v1beta1StatusToV1Status` webhook.go:549-556).
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct SarStatus {
    #[serde(default)]
    allowed: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    denied: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    evaluation_error: String,
}

#[derive(Debug, Deserialize)]
struct SarResponse {
    #[serde(default)]
    status: SarStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SarVersion {
    V1,
    V1beta1,
}

impl SarVersion {
    fn api_version(self) -> &'static str {
        match self {
            SarVersion::V1 => "authorization.k8s.io/v1",
            SarVersion::V1beta1 => "authorization.k8s.io/v1beta1",
        }
    }
}

// ---------------------------------------------------------------------------
// LRU expire cache (`k8s.io/apimachinery/pkg/util/cache.LRUExpireCache`)
// ---------------------------------------------------------------------------

struct ExpireCache {
    cap: usize,
    inner: Mutex<CacheInner>,
}

#[derive(Default)]
struct CacheInner {
    tick: u64,
    entries: HashMap<String, (SarStatus, Instant, u64)>,
}

impl ExpireCache {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            inner: Mutex::new(CacheInner::default()),
        }
    }

    fn get(&self, key: &str, now: Instant) -> Option<SarStatus> {
        let mut c = self.inner.lock().unwrap();
        c.tick += 1;
        let tick = c.tick;
        match c.entries.get_mut(key) {
            Some((status, expires, used)) if *expires > now => {
                *used = tick;
                Some(status.clone())
            }
            Some(_) => {
                c.entries.remove(key);
                None
            }
            None => None,
        }
    }

    fn add(&self, key: String, status: SarStatus, ttl: Duration, now: Instant) {
        let mut c = self.inner.lock().unwrap();
        c.tick += 1;
        let tick = c.tick;
        if !c.entries.contains_key(&key) && c.entries.len() >= self.cap {
            // drop expired entries first, else the least recently used
            c.entries.retain(|_, (_, exp, _)| *exp > now);
            if c.entries.len() >= self.cap {
                if let Some(lru) = c
                    .entries
                    .iter()
                    .min_by_key(|(_, (_, _, used))| *used)
                    .map(|(k, _)| k.clone())
                {
                    c.entries.remove(&lru);
                }
            }
        }
        c.entries.insert(key, (status, now + ttl, tick));
    }
}

// ---------------------------------------------------------------------------
// The authorizer
// ---------------------------------------------------------------------------

/// One failed attempt of the webhook call.
struct CallError {
    message: String,
    retry: bool,
}

pub struct WebhookAuthorizer {
    client: reqwest::Client,
    config: WebhookClientConfig,
    version: SarVersion,
    response_cache: ExpireCache,
    authorized_ttl: Duration,
    unauthorized_ttl: Duration,
    retry_backoff: Backoff,
    failure_policy: FailurePolicy,
}

impl WebhookAuthorizer {
    /// `webhook.New` / `newWithBackoff` (webhook.go:108-139) with the version
    /// switch of `subjectAccessReviewInterfaceFromConfig` (:447-477).
    pub fn new(
        config: WebhookClientConfig,
        version: &str,
        authorized_ttl: Duration,
        unauthorized_ttl: Duration,
        retry_backoff: Backoff,
        failure_policy: FailurePolicy,
    ) -> anyhow::Result<Self> {
        let version = match version {
            "v1" => SarVersion::V1,
            "v1beta1" => SarVersion::V1beta1,
            other => anyhow::bail!(
                "unsupported webhook authorizer version {other:?}, supported versions are \"v1\", \"v1beta1\""
            ),
        };
        Ok(Self {
            client: config.build_client()?,
            config,
            version,
            response_cache: ExpireCache::new(RESPONSE_CACHE_SIZE),
            authorized_ttl,
            unauthorized_ttl,
            retry_backoff,
            failure_policy,
        })
    }

    /// The legacy-flags path of `ToAuthorizationConfiguration` +
    /// `reload.go:124-167`: `failurePolicy: NoOpinion`, default retry backoff,
    /// a zero TTL disables that cache.
    pub fn from_args(args: &AuthorizationWebhookArgs) -> anyhow::Result<Self> {
        let file = args
            .config_file
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("webhook config file not passed"))?;
        Self::new(
            WebhookClientConfig::load(Path::new(file))?,
            &args.version,
            args.cache_authorized_ttl,
            args.cache_unauthorized_ttl,
            Backoff::default_auth_webhook(),
            FailurePolicy::NoOpinion,
        )
    }

    fn decision_on_error(&self, error: String) -> Opinion {
        match self.failure_policy {
            FailurePolicy::Deny => Opinion::Deny(String::new()),
            FailurePolicy::NoOpinion => Opinion::NoOpinion {
                reason: String::new(),
                error: Some(error),
            },
        }
    }

    /// One attempt: POST the review, read the status.
    async fn call(&self, spec: &SarSpec) -> std::result::Result<SarStatus, CallError> {
        let body = SarRequest {
            api_version: self.version.api_version(),
            kind: "SubjectAccessReview",
            spec,
            status: SarStatus::default(),
        };
        let mut req = self
            .client
            .post(&self.config.server)
            .header("Accept", "application/json")
            .json(&body);
        if let Some(t) = &self.config.bearer_token {
            req = req.bearer_auth(t);
        } else if let Some((u, p)) = &self.config.basic_auth {
            req = req.basic_auth(u, Some(p));
        }
        let resp = req.send().await.map_err(|e| CallError {
            retry: is_connection_reset(&e),
            message: e.to_string(),
        })?;
        let status = resp.status();
        if !status.is_success() {
            // DefaultShouldRetry: apierrors.IsInternalError / IsTimeout /
            // IsTooManyRequests, or any error that suggests a client delay
            // (a Retry-After header), util/webhook/webhook.go:47-56.
            let suggests_delay = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.trim().parse::<u64>().is_ok());
            let retry = matches!(status.as_u16(), 500 | 504 | 408 | 429) || suggests_delay;
            return Err(CallError {
                message: format!("webhook returned HTTP {status}"),
                retry,
            });
        }
        let parsed: SarResponse = resp.json().await.map_err(|e| CallError {
            message: format!("decoding webhook response: {e}"),
            retry: false,
        })?;
        Ok(parsed.status)
    }

    /// `webhook.WithExponentialBackoff` (util/webhook/webhook.go:105-135) over
    /// `wait.ExponentialBackoffWithContext`: at most `steps` attempts, sleeping
    /// between them; a non-retriable error ends it at once.
    async fn call_with_backoff(&self, spec: &SarSpec) -> std::result::Result<SarStatus, String> {
        let mut backoff = self.retry_backoff.clone();
        let mut last = String::new();
        while backoff.steps > 0 {
            match self.call(spec).await {
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

/// `utilnet.IsConnectionReset`.
fn is_connection_reset(e: &reqwest::Error) -> bool {
    let mut src: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::ConnectionReset {
                return true;
            }
        }
        src = s.source();
    }
    false
}

/// `convertToSARExtra` + the user / attributes half of `Authorize`
/// (webhook.go:189-206, `resourceAttributesFrom` :301-329 minus selectors).
fn spec_from(attr: &RequestAttributes) -> SarSpec {
    let user: &UserInfo = &attr.user;
    let mut spec = SarSpec {
        user: user.username.clone(),
        uid: user.uid.clone(),
        groups: user.groups.clone(),
        extra: user
            .extra
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        ..Default::default()
    };
    if attr.is_non_resource_request {
        spec.non_resource_attributes = Some(SarNonResourceAttributes {
            path: attr.path.clone().unwrap_or_default(),
            verb: attr.verb.clone(),
        });
    } else {
        spec.resource_attributes = Some(SarResourceAttributes {
            namespace: attr.namespace.clone().unwrap_or_default(),
            verb: attr.verb.clone(),
            group: attr.api_group.clone(),
            // `RequestAttributes` carries no API version.
            version: String::new(),
            resource: attr.resource.clone(),
            subresource: attr.subresource.clone().unwrap_or_default(),
            name: attr.name.clone().unwrap_or_default(),
        });
    }
    spec
}

/// `shouldCache` (webhook.go:537-547): requester-controlled attributes that are
/// too large may be a DoS attempt, so they skip the cache.
fn should_cache(attr: &RequestAttributes) -> bool {
    let size = attr.namespace.as_deref().map_or(0, str::len)
        + attr.verb.len()
        + attr.api_group.len()
        + attr.resource.len()
        + attr.subresource.as_deref().map_or(0, str::len)
        + attr.name.as_deref().map_or(0, str::len)
        + attr.path.as_deref().map_or(0, str::len);
    size < MAX_CONTROLLED_ATTR_CACHE_SIZE
}

#[async_trait]
impl Authorizer for WebhookAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> AuthzResult<Decision> {
        match self.authorize_opinion(attrs).await? {
            Opinion::Allow => Ok(Decision::Allow),
            Opinion::Deny(reason) => Ok(Decision::Deny(reason)),
            Opinion::NoOpinion { reason, error } => match error {
                Some(e) => Err(Error::Authorization(e)),
                None => Ok(Decision::Deny(reason)),
            },
        }
    }

    /// `WebhookAuthorizer.Authorize` (webhook.go:188-299).
    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> AuthzResult<Opinion> {
        let spec = spec_from(attrs);
        let key = match serde_json::to_string(&spec) {
            Ok(k) => k,
            Err(e) => return Ok(self.decision_on_error(e.to_string())),
        };
        let status = if let Some(s) = self.response_cache.get(&key, Instant::now()) {
            s
        } else {
            let status = match self.call_with_backoff(&spec).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to make webhook authorizer request: {e}");
                    return Ok(self.decision_on_error(e));
                }
            };
            if should_cache(attrs) {
                let ttl = if status.allowed {
                    self.authorized_ttl
                } else {
                    self.unauthorized_ttl
                };
                // A zero TTL expires at once: skip the insert.
                if !ttl.is_zero() {
                    self.response_cache
                        .add(key, status.clone(), ttl, Instant::now());
                }
            }
            status
        };
        Ok(match (status.denied, status.allowed) {
            // Upstream returns DecisionDeny with this error; our Deny carries
            // only a reason.
            (true, true) => Opinion::Deny(format!(
                "{}: webhook subject access review returned both allow and deny response",
                status.reason
            )),
            (true, false) => Opinion::Deny(status.reason),
            (false, true) => Opinion::Allow,
            (false, false) => Opinion::NoOpinion {
                reason: status.reason,
                error: None,
            },
        })
    }

    /// Upstream: "webhook authorizer does not support user rule resolution"
    /// (webhook.go:405-412), incomplete.
    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> AuthzResult<(
        Vec<rusternetes_common::resources::ResourceRule>,
        Vec<rusternetes_common::resources::NonResourceRule>,
    )> {
        Ok((vec![], vec![]))
    }
}
