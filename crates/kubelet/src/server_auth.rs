//! Authentication and authorization of requests to the kubelet API port.
//!
//! Ports `BuildAuth` / `BuildAuthn` / `BuildAuthz`
//! (`cmd/kubelet/app/auth.go:41-141`) and the filter `InstallAuthFilter`
//! (`pkg/kubelet/server/server.go:333-379`) together with the delegating
//! authenticator and authorizer they assemble:
//!
//! * `authenticatorfactory.DelegatingAuthenticatorConfig.New`
//!   (`staging/src/k8s.io/apiserver/pkg/authentication/authenticatorfactory/delegating.go:76-129`):
//!   x509 client certificate, then bearer token (`bearertoken` + `websocket`
//!   protocol) through a cached webhook `TokenReview`, then
//!   `AuthenticatedGroupAdder`, then `anonymous` behind `NewFailOnError`.
//! * `webhook.WebhookTokenAuthenticator.AuthenticateToken`
//!   (`plugin/pkg/authenticator/token/webhook/webhook.go`).
//! * `cache.New` (`pkg/authentication/token/cache/cached_token_authenticator.go`).
//! * `authorizerfactory.DelegatingAuthorizerConfig.New`
//!   (`pkg/authorization/authorizerfactory/delegating.go:48-67`) over
//!   `webhook.WebhookAuthorizer.Authorize`
//!   (`plugin/pkg/authorizer/webhook/webhook.go:188-299`).
//!
//! Upstream's `authenticator.Request`/`Authorizer` are Go interfaces over
//! `*http.Request`; here they are async traits over the pieces of the request
//! the authenticators read (headers, the peer certificate chain, the path).
//!
//! The kubelet builds `authenticator.AudiencesFrom(ctx)` with no audiences
//! (`DelegatingAuthenticatorConfig.APIAudiences` is unset in `BuildAuthn`), so
//! the webhook's audience intersection (`checkAuds`) never applies and is not
//! ported.

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use futures::future::{BoxFuture, FutureExt, Shared};
use hmac::{Hmac, Mac};
use rusternetes_client::api_status;
use rusternetes_client::http::ApiClient;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{AlwaysAllowAuthorizer, Authorizer, Decision, Opinion};
use rusternetes_common::lru_expire_cache::LruExpireCache;
use rusternetes_common::resources::{
    NonResourceAttributes, ResourceAttributes, SubjectAccessReview, SubjectAccessReviewSpec,
    TokenReview, TokenReviewSpec,
};
use rusternetes_common::wait::Backoff;
use rusternetes_middleware::{user_from_client_cert_der, PeerCertificates};
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::ClientCertVerifier;
use sha2::Sha256;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::config::KubeletConfiguration;
use crate::server::{get_request_attributes, RequestAttributes};

// ---------------------------------------------------------------------------
// The authenticator interfaces
// ---------------------------------------------------------------------------

/// An authentication error (the `error` of `AuthenticateRequest`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthnError(pub String);

impl fmt::Display for AuthnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AuthnError {}

/// `(*authenticator.Response, ok, error)` of upstream: `Ok(Some(user))` is
/// `ok == true`, `Ok(None)` is "no opinion", `Err` is an error.
pub type AuthnResult = Result<Option<UserInfo>, AuthnError>;

/// `authenticator.Token`.
#[async_trait]
pub trait TokenAuthenticator: Send + Sync {
    async fn authenticate_token(&self, token: &str) -> AuthnResult;
}

/// The part of `*http.Request` an `authenticator.Request` reads. Headers are
/// mutable: `bearertoken` deletes `Authorization` and `websocket` rewrites
/// `Sec-WebSocket-Protocol` once they authenticated.
pub struct AuthnRequest<'a> {
    pub headers: &'a mut HeaderMap,
    /// `req.TLS.PeerCertificates` (leaf first); `None` when the connection
    /// carried no `PeerCertificates` extension (plaintext / no client cert).
    pub peer_certs: Option<&'a PeerCertificates>,
}

/// `authenticator.Request`.
#[async_trait]
pub trait RequestAuthenticator: Send + Sync {
    async fn authenticate_request(&self, req: &mut AuthnRequest<'_>) -> AuthnResult;
}

// ---------------------------------------------------------------------------
// Review clients (`AuthenticationV1Interface` / `AuthorizationV1Interface`)
// ---------------------------------------------------------------------------

/// The `(result, statusCode, err)` of the webhook review clients' `Create`
/// (`tokenReviewer.Create`, plugin/pkg/authenticator/token/webhook/webhook.go;
/// `subjectAccessReviewer.Create`, plugin/pkg/authorizer/webhook/webhook.go):
/// the HTTP status code of the response, 0 when none was received, and the
/// outcome.
pub type ReviewResponse<T> = (u16, anyhow::Result<T>);

/// `authenticationv1client.TokenReviewInterface.Create`.
#[async_trait]
pub trait TokenReviewClient: Send + Sync {
    async fn create(&self, review: &TokenReview) -> ReviewResponse<TokenReview>;
}

/// `authorizationv1client.SubjectAccessReviewInterface.Create`.
#[async_trait]
pub trait SubjectAccessReviewClient: Send + Sync {
    async fn create(&self, review: &SubjectAccessReview) -> ReviewResponse<SubjectAccessReview>;
}

#[async_trait]
impl TokenReviewClient for ApiClient {
    async fn create(&self, review: &TokenReview) -> ReviewResponse<TokenReview> {
        self.post_with_status("/apis/authentication.k8s.io/v1/tokenreviews", review)
            .await
    }
}

#[async_trait]
impl SubjectAccessReviewClient for ApiClient {
    async fn create(&self, review: &SubjectAccessReview) -> ReviewResponse<SubjectAccessReview> {
        self.post_with_status("/apis/authorization.k8s.io/v1/subjectaccessreviews", review)
            .await
    }
}

/// `webhook.DefaultShouldRetry` (`pkg/util/webhook/webhook.go:59-73`):
/// connection reset, `utilnet.IsHTTP2ConnectionLost`, an internal error /
/// timeout / too-many-requests status, or `apierrors.SuggestsClientDelay`
/// (a `Retry-After` is "an explicit confirmation we should retry").
pub fn default_should_retry(err: &anyhow::Error) -> bool {
    let reset = err.chain().any(|c| {
        c.downcast_ref::<std::io::Error>()
            .is_some_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset)
    });
    reset
        || api_status::is_http2_connection_lost(err)
        || api_status::is_internal_error(err)
        || api_status::is_timeout(err)
        || api_status::is_too_many_requests(err)
        || api_status::suggests_client_delay(err).is_some()
}

/// `webhook.WithExponentialBackoff` (`pkg/util/webhook/webhook.go`) over
/// `wait.ExponentialBackoffWithContext`: at most `steps` attempts, sleeping
/// between them; a non-retriable error ends it at once, and the last webhook
/// error is what an exhausted run returns.
async fn with_exponential_backoff<T, F, Fut>(mut backoff: Backoff, mut f: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut last: Option<anyhow::Error> = None;
    while backoff.steps > 0 {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if !default_should_retry(&e) {
                    return Err(e);
                }
                last = Some(e);
            }
        }
        if backoff.steps == 1 {
            break;
        }
        tokio::time::sleep(backoff.step()).await;
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("timed out waiting for the condition")))
}

// ---------------------------------------------------------------------------
// delegated authn / authz request metrics
// ---------------------------------------------------------------------------

/// `AuthenticatorMetrics{RecordRequestTotal, RecordRequestLatency}`
/// (plugin/pkg/authenticator/token/webhook/metrics.go) and the request half of
/// `AuthorizerMetrics` (plugin/pkg/authorizer/webhook/metrics/metrics.go).
pub trait ReviewMetrics: Send + Sync {
    /// `RecordRequestTotal`.
    fn record_request_total(&self, code: &str);
    /// `RecordRequestLatency` (seconds).
    fn record_request_latency(&self, code: &str, latency_secs: f64);
}

/// `noopMetrics` (webhook.go): used when no metrics are wanted.
pub struct NoopReviewMetrics;

impl ReviewMetrics for NoopReviewMetrics {
    fn record_request_total(&self, _code: &str) {}
    fn record_request_latency(&self, _code: &str, _latency_secs: f64) {}
}

/// `Buckets` of the delegated request-duration histograms
/// (authenticatorfactory/metrics.go, authorizerfactory/metrics.go).
const DELEGATED_BUCKETS: [f64; 8] = [0.25, 0.5, 0.7, 1.0, 1.5, 3.0, 5.0, 10.0];

/// One `requestTotal` / `requestLatency` pair of a delegating factory, served
/// from `legacyregistry` (the default registry) with `StabilityLevel: ALPHA`
/// (help prefixed `[ALPHA] `, component-base/metrics/opts.go).
struct DelegatedMetrics {
    request_total: prometheus::CounterVec,
    request_latency: prometheus::HistogramVec,
}

impl DelegatedMetrics {
    fn new(prefix: &str) -> Self {
        Self {
            request_total: prometheus::CounterVec::new(
                prometheus::Opts::new(
                    format!("apiserver_delegated_{prefix}_request_total"),
                    "[ALPHA] Number of HTTP requests partitioned by status code.",
                ),
                &["code"],
            )
            .expect("valid delegated request counter definition"),
            request_latency: prometheus::HistogramVec::new(
                prometheus::HistogramOpts::new(
                    format!("apiserver_delegated_{prefix}_request_duration_seconds"),
                    "[ALPHA] Request latency in seconds. Broken down by status code.",
                )
                .buckets(DELEGATED_BUCKETS.to_vec()),
                &["code"],
            )
            .expect("valid delegated request duration definition"),
        }
    }

    fn register(&self) {
        let r = prometheus::default_registry();
        r.register(Box::new(self.request_total.clone()))
            .expect("delegated request counter registered once");
        r.register(Box::new(self.request_latency.clone()))
            .expect("delegated request duration registered once");
    }
}

impl ReviewMetrics for DelegatedMetrics {
    /// `requestTotal.WithContext(ctx).WithLabelValues(code).Inc()`.
    fn record_request_total(&self, code: &str) {
        self.request_total.with_label_values(&[code]).inc();
    }
    /// `requestLatency.WithContext(ctx).WithLabelValues(code).Observe(latency)`.
    fn record_request_latency(&self, code: &str, latency_secs: f64) {
        self.request_latency
            .with_label_values(&[code])
            .observe(latency_secs);
    }
}

/// `apiserver_delegated_authn_request_*` (authenticatorfactory/metrics.go),
/// registered by the package `init()`; here on first use.
static AUTHN_METRICS: std::sync::LazyLock<Arc<DelegatedMetrics>> = std::sync::LazyLock::new(|| {
    let m = DelegatedMetrics::new("authn");
    m.register();
    Arc::new(m)
});

/// `apiserver_delegated_authz_request_*` (authorizerfactory/metrics.go),
/// registered by `NewDelegatingAuthorizerMetrics` -> `RegisterMetrics`
/// (a `sync.Once`).
static AUTHZ_METRICS: std::sync::LazyLock<Arc<DelegatedMetrics>> = std::sync::LazyLock::new(|| {
    let m = DelegatedMetrics::new("authz");
    m.register();
    Arc::new(m)
});

/// The `AuthenticatorMetrics` `DelegatingAuthenticatorConfig.New` passes to
/// `webhooktoken.NewFromInterface` (authenticatorfactory/delegating.go:96-99).
pub fn delegating_authenticator_metrics() -> Arc<dyn ReviewMetrics> {
    AUTHN_METRICS.clone()
}

/// `NewDelegatingAuthorizerMetrics` (authorizerfactory/metrics.go).
pub fn new_delegating_authorizer_metrics() -> Arc<dyn ReviewMetrics> {
    AUTHZ_METRICS.clone()
}

/// The metrics half of the `WithExponentialBackoff` callback in
/// `AuthenticateToken` (webhook.go:139-150) and `Authorize`
/// (webhook.go:256-266): with a status code the request is recorded under it;
/// without one, only a failed request is recorded, as `<error>`.
fn record_review_request<T>(
    metrics: &dyn ReviewMetrics,
    status_code: u16,
    latency: Duration,
    result: &anyhow::Result<T>,
) {
    if status_code != 0 {
        let code = status_code.to_string();
        metrics.record_request_total(&code);
        metrics.record_request_latency(&code, latency.as_secs_f64());
    } else if result.is_err() {
        metrics.record_request_total("<error>");
        metrics.record_request_latency("<error>", latency.as_secs_f64());
    }
}

// ---------------------------------------------------------------------------
// webhook token authenticator
// ---------------------------------------------------------------------------

/// `webhook.WebhookTokenAuthenticator` (webhook.go:70-76).
pub struct WebhookTokenAuthenticator {
    token_review: Arc<dyn TokenReviewClient>,
    retry_backoff: Backoff,
    metrics: Arc<dyn ReviewMetrics>,
}

impl WebhookTokenAuthenticator {
    /// `webhook.NewFromInterface` (webhook.go:94-97).
    pub fn new(
        token_review: Arc<dyn TokenReviewClient>,
        retry_backoff: Backoff,
        metrics: Arc<dyn ReviewMetrics>,
    ) -> Self {
        Self {
            token_review,
            retry_backoff,
            metrics,
        }
    }
}

#[async_trait]
impl TokenAuthenticator for WebhookTokenAuthenticator {
    /// `AuthenticateToken` (webhook.go:130-216).
    async fn authenticate_token(&self, token: &str) -> AuthnResult {
        let review = TokenReview {
            api_version: "authentication.k8s.io/v1".to_string(),
            kind: "TokenReview".to_string(),
            metadata: Default::default(),
            spec: TokenReviewSpec {
                audiences: None,
                token: token.to_string(),
            },
            status: None,
        };
        let result = with_exponential_backoff(self.retry_backoff.clone(), || async {
            let start = Instant::now();
            let (status_code, result) = self.token_review.create(&review).await;
            record_review_request(&*self.metrics, status_code, start.elapsed(), &result);
            result
        })
        .await
        .map_err(|e| {
            tracing::error!("Failed to make webhook authenticator request: {e:#}");
            AuthnError(format!("{e:#}"))
        })?;

        let status = result.status.unwrap_or_default();
        if !status.authenticated.unwrap_or(false) {
            return match status.error.filter(|e| !e.is_empty()) {
                Some(e) => Err(AuthnError(e)),
                None => Ok(None),
            };
        }
        let user = status.user.unwrap_or_default();
        Ok(Some(UserInfo {
            username: user.username.unwrap_or_default(),
            uid: user.uid.unwrap_or_default(),
            groups: user.groups.unwrap_or_default(),
            extra: user.extra.unwrap_or_default(),
        }))
    }
}

// ---------------------------------------------------------------------------
// cached token authenticator
// ---------------------------------------------------------------------------

/// `cacheRecord`.
#[derive(Clone)]
struct CacheRecord {
    user: Option<UserInfo>,
    ok: bool,
    err: Option<AuthnError>,
}

/// `sharedLookupTimeout` (cached_token_authenticator.go:48).
const SHARED_LOOKUP_TIMEOUT: Duration = Duration::from_secs(30);

type SharedLookup = Shared<BoxFuture<'static, CacheRecord>>;

struct CachedInner {
    authenticator: Arc<dyn TokenAuthenticator>,
    cache_errs: bool,
    success_ttl: Duration,
    failure_ttl: Duration,
    /// `utilcache.Expiring` (cache_simple.go): key -> (record, expiry).
    cache: Mutex<HashMap<String, (CacheRecord, Instant)>>,
    /// `hmac.New(sha256.New, randomCacheKey)`.
    cache_key: [u8; 32],
    /// `singleflight.Group`.
    group: Mutex<HashMap<String, SharedLookup>>,
}

/// `cache.New(authenticator, cacheErrs, successTTL, failureTTL)`
/// (cached_token_authenticator.go:106-108): caches the wrapped token
/// authenticator's answers for the TTLs and collapses concurrent lookups of the
/// same token into one call.
#[derive(Clone)]
pub struct CachedTokenAuthenticator {
    inner: Arc<CachedInner>,
}

impl CachedTokenAuthenticator {
    pub fn new(
        authenticator: Arc<dyn TokenAuthenticator>,
        cache_errs: bool,
        success_ttl: Duration,
        failure_ttl: Duration,
    ) -> Self {
        let mut cache_key = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut cache_key);
        Self {
            inner: Arc::new(CachedInner {
                authenticator,
                cache_errs,
                success_ttl,
                failure_ttl,
                cache: Mutex::new(HashMap::new()),
                cache_key,
                group: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// `keyFunc` (:249-272) with no audiences: the length-prefixed token, then
    /// the audience count (0).
    fn key_func(&self, token: &str) -> String {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.inner.cache_key)
            .expect("HMAC accepts any key length");
        mac.update(&(token.len() as u32).to_be_bytes());
        mac.update(token.as_bytes());
        mac.update(&0u32.to_be_bytes());
        format!("{:x}", mac.finalize().into_bytes())
    }

    fn cache_get(&self, key: &str) -> Option<CacheRecord> {
        let mut c = self.inner.cache.lock().unwrap();
        match c.get(key) {
            Some((rec, exp)) if *exp > Instant::now() => Some(rec.clone()),
            Some(_) => {
                c.remove(key);
                None
            }
            None => None,
        }
    }

    fn cache_set(&self, key: String, rec: CacheRecord, ttl: Duration) {
        let now = Instant::now();
        let mut c = self.inner.cache.lock().unwrap();
        // `Expiring` drops expired entries as it goes; sweep when it grows.
        if c.len() >= 1024 && c.len().is_power_of_two() {
            c.retain(|_, (_, exp)| *exp > now);
        }
        c.insert(key, (rec, now + ttl));
    }

    /// `doAuthenticateToken` (:140-228).
    async fn do_authenticate_token(&self, token: &str) -> CacheRecord {
        let key = self.key_func(token);
        if let Some(rec) = self.cache_get(&key) {
            return rec;
        }
        let lookup = {
            let mut group = self.inner.group.lock().unwrap();
            match group.get(&key) {
                Some(f) => f.clone(),
                None => {
                    let this = self.clone();
                    let (k, t) = (key.clone(), token.to_string());
                    let f: SharedLookup = async move { this.fetch(k, t).await }.boxed().shared();
                    group.insert(key.clone(), f.clone());
                    f
                }
            }
        };
        let rec = lookup.await;
        self.inner.group.lock().unwrap().remove(&key);
        rec
    }

    /// The `singleflight` function body (:167-216).
    async fn fetch(&self, key: String, token: String) -> CacheRecord {
        // a second cache read, as upstream: another flight may have filled it
        if let Some(rec) = self.cache_get(&key) {
            return rec;
        }
        let inner = self.inner.clone();
        let call = inner.authenticator.authenticate_token(&token);
        let rec = match tokio::time::timeout(SHARED_LOOKUP_TIMEOUT, call).await {
            Ok(Ok(Some(user))) => CacheRecord {
                user: Some(user),
                ok: true,
                err: None,
            },
            Ok(Ok(None)) => CacheRecord {
                user: None,
                ok: false,
                err: None,
            },
            Ok(Err(e)) => CacheRecord {
                user: None,
                ok: false,
                err: Some(e),
            },
            Err(_) => CacheRecord {
                user: None,
                ok: false,
                err: Some(AuthnError("context deadline exceeded".into())),
            },
        };
        if !self.inner.cache_errs && rec.err.is_some() {
            return rec;
        }
        if rec.ok && !self.inner.success_ttl.is_zero() {
            self.cache_set(key, rec.clone(), self.inner.success_ttl);
        } else if !rec.ok && !self.inner.failure_ttl.is_zero() {
            self.cache_set(key, rec.clone(), self.inner.failure_ttl);
        }
        rec
    }
}

#[async_trait]
impl TokenAuthenticator for CachedTokenAuthenticator {
    /// `AuthenticateToken` (:131-138).
    async fn authenticate_token(&self, token: &str) -> AuthnResult {
        let rec = self.do_authenticate_token(token).await;
        if !rec.ok || rec.err.is_some() {
            return match rec.err {
                Some(e) => Err(e),
                None => Ok(None),
            };
        }
        Ok(rec.user)
    }
}

// ---------------------------------------------------------------------------
// bearer token + websocket protocol authenticators
// ---------------------------------------------------------------------------

/// `bearertoken.Authenticator` (request/bearertoken/bearertoken.go:41-77).
pub struct BearerTokenAuthenticator {
    auth: Arc<dyn TokenAuthenticator>,
}

impl BearerTokenAuthenticator {
    pub fn new(auth: Arc<dyn TokenAuthenticator>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl RequestAuthenticator for BearerTokenAuthenticator {
    async fn authenticate_request(&self, req: &mut AuthnRequest<'_>) -> AuthnResult {
        let auth = req
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .trim()
            .to_string();
        if auth.is_empty() {
            return Ok(None);
        }
        let parts: Vec<&str> = auth.splitn(3, ' ').collect();
        if parts.len() < 2 || parts[0].to_lowercase() != "bearer" {
            return Ok(None);
        }
        let token = parts[1];
        if token.is_empty() {
            // the "extra space before the token" warning has no sink here
            return Ok(None);
        }
        match self.auth.authenticate_token(token).await? {
            Some(user) => {
                req.headers.remove(header::AUTHORIZATION);
                Ok(Some(user))
            }
            None => Err(AuthnError("invalid bearer token".into())),
        }
    }
}

pub const BEARER_PROTOCOL_PREFIX: &str = "base64url.bearer.authorization.k8s.io.";

/// `wsstream.IsWebSocketRequest`.
fn is_websocket_request(headers: &HeaderMap) -> bool {
    let get = |h: &str| headers.get(h).and_then(|v| v.to_str().ok()).unwrap_or("");
    if !get("upgrade").eq_ignore_ascii_case("websocket") {
        return false;
    }
    get("connection")
        .to_lowercase()
        .split(',')
        .any(|t| t.trim() == "upgrade")
}

/// `websocket.ProtocolAuthenticator` (request/websocket/protocol.go:46-111).
pub struct WebsocketProtocolAuthenticator {
    auth: Arc<dyn TokenAuthenticator>,
}

impl WebsocketProtocolAuthenticator {
    pub fn new(auth: Arc<dyn TokenAuthenticator>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl RequestAuthenticator for WebsocketProtocolAuthenticator {
    async fn authenticate_request(&self, req: &mut AuthnRequest<'_>) -> AuthnResult {
        if !is_websocket_request(req.headers) {
            return Ok(None);
        }
        let mut token = String::new();
        let mut saw_token_protocol = false;
        let mut filtered: Vec<String> = Vec::new();
        for value in req.headers.get_all("sec-websocket-protocol") {
            let value = value.to_str().unwrap_or("");
            for protocol in value.split(',') {
                let protocol = protocol.trim();
                let Some(encoded) = protocol.strip_prefix(BEARER_PROTOCOL_PREFIX) else {
                    filtered.push(protocol.to_string());
                    continue;
                };
                if saw_token_protocol {
                    return Err(AuthnError(
                        "multiple base64.bearer.authorization tokens specified".into(),
                    ));
                }
                saw_token_protocol = true;
                let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(encoded)
                    .map_err(|_| {
                        AuthnError("invalid base64.bearer.authorization token encoding".into())
                    })?;
                token = String::from_utf8(decoded)
                    .map_err(|_| AuthnError("invalid base64.bearer.authorization token".into()))?;
            }
        }
        if !token.is_empty() && filtered.is_empty() {
            return Err(AuthnError("missing additional subprotocol".into()));
        }
        if token.is_empty() {
            return Ok(None);
        }
        match self.auth.authenticate_token(&token).await? {
            Some(user) => {
                if let Ok(v) = HeaderValue::from_str(&filtered.join(",")) {
                    req.headers.insert("sec-websocket-protocol", v);
                }
                Ok(Some(user))
            }
            None => Err(AuthnError("invalid bearer token".into())),
        }
    }
}

// ---------------------------------------------------------------------------
// x509 client certificate authenticator
// ---------------------------------------------------------------------------

/// `dynamiccertificates.Listener` (interfaces.go:23-26): told when the CA
/// content may have changed.
pub trait Listener: Send + Sync {
    /// `Enqueue`.
    fn enqueue(&self);
}

/// `FileRefreshDuration` (dynamic_cafile_content.go:39): upstream's
/// `wait.Until` period (`Run`, :168-172) for restarting a failed file watch.
const FILE_REFRESH_DURATION: Duration = Duration::from_secs(60);

/// The queue of `DynamicFileCAContent`, which "only ever has one item, but has
/// nice error handling backoff/retry semantics" (dynamic_cafile_content.go:63).
/// `Add` coalesces into one pending item; `AddRateLimited` re-adds after the
/// per-item exponential failure delay of `DefaultTypedControllerRateLimiter`
/// (5ms base, doubling, capped at 1000s: workqueue/default_rate_limiters.go;
/// its overall token bucket cannot bind a one-item queue).
struct SingleItemQueue {
    notify: tokio::sync::Notify,
    failures: std::sync::atomic::AtomicU32,
}

impl SingleItemQueue {
    const BASE_DELAY: Duration = Duration::from_millis(5);
    const MAX_DELAY: Duration = Duration::from_secs(1000);

    fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: tokio::sync::Notify::new(),
            failures: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// `queue.Add`.
    fn add(&self) {
        self.notify.notify_one();
    }

    /// `queue.Forget`.
    fn forget(&self) {
        self.failures.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// `ItemExponentialFailureRateLimiter.When`.
    fn when(&self) -> Duration {
        let exp = self
            .failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .min(31);
        Self::BASE_DELAY
            .saturating_mul(2u32.saturating_pow(exp))
            .min(Self::MAX_DELAY)
    }

    /// `queue.AddRateLimited`.
    fn add_rate_limited(self: &Arc<Self>) {
        let delay = self.when();
        let q = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            q.add();
        });
    }

    /// `queue.Get`.
    async fn get(&self) {
        self.notify.notified().await;
    }
}

/// `caBundleAndVerifier`.
struct CaBundleAndVerifier {
    ca_bundle: Vec<u8>,
    verifier: Arc<dyn ClientCertVerifier>,
}

/// `dynamiccertificates.DynamicFileCAContent` for the kubelet's
/// `authentication.x509.clientCAFile` (`NewDynamicCAContentFromFile
/// ("client-ca-bundle", ...)`, cmd/kubelet/app/auth.go:73-74): a verifier over
/// the file's bundle that reloads when the file changes. [`ClientCaContent::run`]
/// watches the file (inotify in place of fsnotify) and reloads on every event;
/// a reload that fails is retried with exponential backoff and keeps the
/// previous bundle.
pub struct ClientCaContent {
    name: String,
    /// The file to read.
    filename: String,
    /// The last read, non-zero length content of the file (`caBundle`).
    ca_bundle: RwLock<Option<Arc<CaBundleAndVerifier>>>,
    listeners: std::sync::Mutex<Vec<Arc<dyn Listener>>>,
    queue: Arc<SingleItemQueue>,
}

impl ClientCaContent {
    /// `NewDynamicCAContentFromFile`: loads the file now, failing when it
    /// cannot be loaded.
    pub fn from_file(path: &str) -> anyhow::Result<Arc<Self>> {
        if path.is_empty() {
            anyhow::bail!("missing filename for ca bundle");
        }
        let ret = Arc::new(Self {
            name: format!("client-ca-bundle::{path}"),
            filename: path.to_string(),
            ca_bundle: RwLock::new(None),
            listeners: std::sync::Mutex::new(Vec::new()),
            queue: SingleItemQueue::new(),
        });
        ret.load_ca_bundle()?;
        Ok(ret)
    }

    /// `AddListener`.
    pub fn add_listener(&self, listener: Arc<dyn Listener>) {
        self.listeners.lock().unwrap().push(listener);
    }

    /// `CurrentCABundleContent`.
    pub fn current_ca_bundle_content(&self) -> Vec<u8> {
        self.current()
            .map(|b| b.ca_bundle.clone())
            .unwrap_or_default()
    }

    /// The verifier for the bundle as last loaded (`VerifyOptions`).
    pub fn verifier(&self) -> Arc<dyn ClientCertVerifier> {
        // `NewDynamicCAContentFromFile` fails unless a bundle loaded, so one
        // is always present.
        self.current()
            .expect("CA bundle is loaded at construction")
            .verifier
            .clone()
    }

    fn current(&self) -> Option<Arc<CaBundleAndVerifier>> {
        self.ca_bundle.read().unwrap().clone()
    }

    /// `loadCABundle`: determines the next set of content for the file.
    fn load_ca_bundle(&self) -> anyhow::Result<()> {
        let ca_bundle = std::fs::read(&self.filename)
            .map_err(|e| anyhow::anyhow!("open {}: {e}", self.filename))?;
        if ca_bundle.is_empty() {
            anyhow::bail!("missing content for CA bundle {:?}", self.name);
        }

        // check to see if we have a change. If the values are the same, do nothing.
        if !self.has_ca_changed(&ca_bundle) {
            return Ok(());
        }

        let verifier =
            rusternetes_common::tls::client_cert_verifier_for_ca_bundle(&self.name, &ca_bundle)?;
        *self.ca_bundle.write().unwrap() = Some(Arc::new(CaBundleAndVerifier {
            ca_bundle,
            verifier,
        }));
        tracing::info!(name = %self.name, "Loaded a new CA Bundle and Verifier");

        // Clone out so a listener may call back into this provider.
        let listeners = self.listeners.lock().unwrap().clone();
        for listener in listeners {
            listener.enqueue();
        }
        Ok(())
    }

    /// `hasCAChanged`.
    fn has_ca_changed(&self, ca_bundle: &[u8]) -> bool {
        match self.current() {
            None => true,
            Some(existing) => existing.ca_bundle != ca_bundle,
        }
    }

    /// `RunOnce`.
    pub fn run_once(&self) -> anyhow::Result<()> {
        self.load_ca_bundle()
    }

    /// `Run` (dynamic_cafile_content.go:157-175): one worker, plus the loop
    /// that watches the CA file and restarts the watch every
    /// `FileRefreshDuration` after it fails. Returns when `stop` fires.
    pub async fn run(self: Arc<Self>, mut stop: tokio::sync::watch::Receiver<bool>) {
        tracing::info!(name = %self.name, "Starting controller");
        let worker = tokio::spawn(Arc::clone(&self).run_worker());
        let this = Arc::clone(&self);
        let watcher = tokio::spawn(async move {
            loop {
                if let Err(e) = this.watch_ca_file().await {
                    tracing::error!("Failed to watch CA file, will retry later: {e:#}");
                }
                tokio::time::sleep(FILE_REFRESH_DURATION).await;
            }
        });
        let _ = stop.changed().await;
        worker.abort();
        watcher.abort();
        tracing::info!(name = %self.name, "Shutting down controller");
    }

    /// The events fsnotify's inotify backend registers for a watched path
    /// (backend_inotify.go: `IN_MOVED_TO | IN_MOVED_FROM | IN_CREATE |
    /// IN_ATTRIB | IN_MODIFY | IN_MOVE_SELF | IN_DELETE | IN_DELETE_SELF`).
    fn watch_mask() -> inotify::WatchMask {
        use inotify::WatchMask;
        WatchMask::MOVED_TO
            | WatchMask::MOVED_FROM
            | WatchMask::CREATE
            | WatchMask::ATTRIB
            | WatchMask::MODIFY
            | WatchMask::MOVE_SELF
            | WatchMask::DELETE
            | WatchMask::DELETE_SELF
    }

    /// `watchCAFile`.
    async fn watch_ca_file(&self) -> anyhow::Result<()> {
        use futures_util::StreamExt;
        // Trigger a check here to ensure the content will be checked
        // periodically even if the following watch fails.
        self.queue.add();

        let inotify = inotify::Inotify::init()
            .map_err(|e| anyhow::anyhow!("error creating fsnotify watcher: {e}"))?;
        let mut watches = inotify.watches();
        watches
            .add(&self.filename, Self::watch_mask())
            .map_err(|e| anyhow::anyhow!("error adding watch for file {}: {e}", self.filename))?;
        // Trigger a check in case the file is updated before the watch starts.
        self.queue.add();

        let mut events = inotify.into_event_stream(vec![0u8; 4096])?;
        while let Some(event) = events.next().await {
            let event = event.map_err(|e| anyhow::anyhow!("received fsnotify error: {e}"))?;
            self.handle_watch_event(event.mask, &mut watches)?;
        }
        Ok(())
    }

    /// `handleWatchEvent`: triggers reloading the CA file, and restarts a new
    /// watch if it's a Remove or Rename event.
    fn handle_watch_event(
        &self,
        mask: inotify::EventMask,
        watches: &mut inotify::Watches,
    ) -> anyhow::Result<()> {
        use inotify::EventMask;
        // `defer c.queue.Add(workItemKey)`: runs after the watch is restarted
        // so no file event is missed.
        let result = (|| {
            // fsnotify reports Remove for IN_DELETE_SELF and Rename for IN_MOVE_SELF.
            if !mask.intersects(EventMask::DELETE_SELF | EventMask::MOVE_SELF) {
                return Ok(());
            }
            // A fresh `add` on the path covers the replacement file; adding an
            // already-watched inode only updates its mask.
            watches
                .add(&self.filename, Self::watch_mask())
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!("error adding watch for file {}: {e}", self.filename))
        })();
        self.queue.add();
        result
    }

    async fn run_worker(self: Arc<Self>) {
        loop {
            self.process_next_work_item().await;
        }
    }

    /// `processNextWorkItem`.
    async fn process_next_work_item(&self) {
        self.queue.get().await;
        match self.load_ca_bundle() {
            Ok(()) => self.queue.forget(),
            Err(e) => {
                tracing::error!("key failed with : {e:#}");
                self.queue.add_rate_limited();
            }
        }
    }
}

/// `x509.Authenticator` with `CommonNameUserConversion`
/// (request/x509/x509.go:138-222).
pub struct X509Authenticator {
    ca: Arc<ClientCaContent>,
}

impl X509Authenticator {
    pub fn new(ca: Arc<ClientCaContent>) -> Self {
        Self { ca }
    }
}

#[async_trait]
impl RequestAuthenticator for X509Authenticator {
    async fn authenticate_request(&self, req: &mut AuthnRequest<'_>) -> AuthnResult {
        let Some(chain) = req.peer_certs.map(|p| p.0.as_slice()) else {
            return Ok(None);
        };
        let Some((leaf, rest)) = chain.split_first() else {
            return Ok(None);
        };
        let leaf_der = CertificateDer::from(leaf.as_slice());
        let intermediates: Vec<CertificateDer<'_>> = rest
            .iter()
            .map(|c| CertificateDer::from(c.as_slice()))
            .collect();
        self.ca
            .verifier()
            .verify_client_cert(&leaf_der, &intermediates, UnixTime::now())
            .map_err(|e| AuthnError(format!("x509: {e}")))?;
        Ok(user_from_client_cert_der(chain))
    }
}

// ---------------------------------------------------------------------------
// anonymous, group adder, union
// ---------------------------------------------------------------------------

/// `anonymous.Authenticator` with no conditions (anonymous.go:43-56).
pub struct AnonymousAuthenticator;

#[async_trait]
impl RequestAuthenticator for AnonymousAuthenticator {
    async fn authenticate_request(&self, _req: &mut AuthnRequest<'_>) -> AuthnResult {
        Ok(Some(UserInfo {
            username: "system:anonymous".to_string(),
            uid: String::new(),
            groups: vec!["system:unauthenticated".to_string()],
            extra: HashMap::new(),
        }))
    }
}

/// `unionauth.unionAuthRequestHandler` (request/union/union.go).
pub struct UnionAuthenticator {
    handlers: Vec<Arc<dyn RequestAuthenticator>>,
    fail_on_error: bool,
}

impl UnionAuthenticator {
    /// `union.New`.
    pub fn union(handlers: Vec<Arc<dyn RequestAuthenticator>>) -> Arc<dyn RequestAuthenticator> {
        if handlers.len() == 1 {
            return handlers.into_iter().next().unwrap();
        }
        Arc::new(Self {
            handlers,
            fail_on_error: false,
        })
    }

    /// `union.NewFailOnError`.
    pub fn fail_on_error(
        handlers: Vec<Arc<dyn RequestAuthenticator>>,
    ) -> Arc<dyn RequestAuthenticator> {
        if handlers.len() == 1 {
            return handlers.into_iter().next().unwrap();
        }
        Arc::new(Self {
            handlers,
            fail_on_error: true,
        })
    }
}

#[async_trait]
impl RequestAuthenticator for UnionAuthenticator {
    async fn authenticate_request(&self, req: &mut AuthnRequest<'_>) -> AuthnResult {
        let mut errs: Vec<String> = Vec::new();
        for h in &self.handlers {
            match h.authenticate_request(req).await {
                Err(e) => {
                    if self.fail_on_error {
                        return Err(e);
                    }
                    errs.push(e.0);
                }
                Ok(Some(user)) => return Ok(Some(user)),
                Ok(None) => {}
            }
        }
        // `utilerrors.NewAggregate`: nil when empty, the message when one,
        // `[a, b]` otherwise.
        match errs.len() {
            0 => Ok(None),
            1 => Err(AuthnError(errs.remove(0))),
            _ => Err(AuthnError(format!("[{}]", errs.join(", ")))),
        }
    }
}

/// `group.AuthenticatedGroupAdder` (group/authenticated_group_adder.go:41-63).
pub struct AuthenticatedGroupAdder {
    pub authenticator: Arc<dyn RequestAuthenticator>,
}

#[async_trait]
impl RequestAuthenticator for AuthenticatedGroupAdder {
    async fn authenticate_request(&self, req: &mut AuthnRequest<'_>) -> AuthnResult {
        let Some(mut user) = self.authenticator.authenticate_request(req).await? else {
            return Ok(None);
        };
        if user.username == "system:anonymous"
            || user
                .groups
                .iter()
                .any(|g| g == "system:authenticated" || g == "system:unauthenticated")
        {
            return Ok(Some(user));
        }
        user.groups.push("system:authenticated".to_string());
        Ok(Some(user))
    }
}

// ---------------------------------------------------------------------------
// BuildAuthn / BuildAuthz / BuildAuth
// ---------------------------------------------------------------------------

/// The effective `KubeletAuthentication` (`kubeletconfig.KubeletAuthentication`).
#[derive(Debug, Clone, Default)]
pub struct AuthnConfig {
    /// `x509.clientCAFile`.
    pub client_ca_file: String,
    /// `webhook.enabled`.
    pub webhook_enabled: bool,
    /// `webhook.cacheTTL`.
    pub webhook_cache_ttl: Duration,
    /// `anonymous.enabled`.
    pub anonymous_enabled: bool,
}

/// `KubeletAuthorizationMode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzMode {
    AlwaysAllow,
    Webhook,
    /// Any other value; `BuildAuthz` rejects it (`""` and unknown modes).
    Other(String),
}

impl AuthzMode {
    pub fn parse(s: &str) -> Self {
        match s {
            "AlwaysAllow" => Self::AlwaysAllow,
            "Webhook" => Self::Webhook,
            other => Self::Other(other.to_string()),
        }
    }
}

/// The effective `KubeletAuthorization`.
#[derive(Debug, Clone)]
pub struct AuthzConfig {
    pub mode: AuthzMode,
    pub cache_authorized_ttl: Duration,
    pub cache_unauthorized_ttl: Duration,
}

/// The authenticator and the client-CA content to run (`runAuthenticatorCAReload`).
pub type BuiltAuthn = (Arc<dyn RequestAuthenticator>, Option<Arc<ClientCaContent>>);

/// `BuildAuthn` (cmd/kubelet/app/auth.go:70-114) over
/// `DelegatingAuthenticatorConfig.New`. Returns the authenticator and the
/// client-CA content to run (`runAuthenticatorCAReload`), when a CA file is set.
pub fn build_authn(
    client: Option<Arc<dyn TokenReviewClient>>,
    authn: &AuthnConfig,
) -> anyhow::Result<BuiltAuthn> {
    let ca = if authn.client_ca_file.is_empty() {
        None
    } else {
        Some(ClientCaContent::from_file(&authn.client_ca_file)?)
    };

    let mut authenticators: Vec<Arc<dyn RequestAuthenticator>> = Vec::new();
    if let Some(ca) = &ca {
        authenticators.push(Arc::new(X509Authenticator::new(ca.clone())));
    }
    if authn.webhook_enabled {
        let Some(client) = client else {
            anyhow::bail!("no client provided, cannot use webhook authentication");
        };
        let token_auth = WebhookTokenAuthenticator::new(
            client,
            Backoff::default_auth_webhook(),
            delegating_authenticator_metrics(),
        );
        let caching = Arc::new(CachedTokenAuthenticator::new(
            Arc::new(token_auth),
            false,
            authn.webhook_cache_ttl,
            authn.webhook_cache_ttl,
        ));
        authenticators.push(Arc::new(BearerTokenAuthenticator::new(caching.clone())));
        authenticators.push(Arc::new(WebsocketProtocolAuthenticator::new(caching)));
    }

    if authenticators.is_empty() {
        if authn.anonymous_enabled {
            return Ok((Arc::new(AnonymousAuthenticator), ca));
        }
        anyhow::bail!("no authentication method configured");
    }

    let mut authenticator: Arc<dyn RequestAuthenticator> = Arc::new(AuthenticatedGroupAdder {
        authenticator: UnionAuthenticator::union(authenticators),
    });
    if authn.anonymous_enabled {
        authenticator = UnionAuthenticator::fail_on_error(vec![
            authenticator,
            Arc::new(AnonymousAuthenticator),
        ]);
    }
    Ok((authenticator, ca))
}

/// `maxControlledAttrCacheSize` (webhook.go:56).
const MAX_CONTROLLED_ATTR_CACHE_SIZE: usize = 10000;

/// `cache.NewLRUExpireCache(8192)` (webhook.go:130).
const RESPONSE_CACHE_SIZE: usize = 8192;

/// `SubjectAccessReviewStatus` as cached by the webhook authorizer.
#[derive(Clone)]
struct SarStatus {
    allowed: bool,
    denied: bool,
    reason: String,
}

/// `webhook.WebhookAuthorizer` built by `DelegatingAuthorizerConfig.New`
/// (decisionOnError `NoOpinion`, no match conditions).
pub struct DelegatingAuthorizer {
    client: Arc<dyn SubjectAccessReviewClient>,
    response_cache: LruExpireCache<SarStatus>,
    authorized_ttl: Duration,
    unauthorized_ttl: Duration,
    retry_backoff: Backoff,
    metrics: Arc<dyn ReviewMetrics>,
}

impl DelegatingAuthorizer {
    pub fn new(
        client: Arc<dyn SubjectAccessReviewClient>,
        authorized_ttl: Duration,
        unauthorized_ttl: Duration,
        retry_backoff: Backoff,
        metrics: Arc<dyn ReviewMetrics>,
    ) -> Self {
        Self {
            client,
            response_cache: LruExpireCache::new(RESPONSE_CACHE_SIZE),
            authorized_ttl,
            unauthorized_ttl,
            retry_backoff,
            metrics,
        }
    }
}

/// `shouldCache` (webhook.go:537-547).
fn should_cache(attr: &rusternetes_common::authz::RequestAttributes) -> bool {
    let n = attr.namespace.as_deref().unwrap_or("").len()
        + attr.verb.len()
        + attr.api_group.len()
        + attr.api_version.len()
        + attr.resource.len()
        + attr.subresource.as_deref().unwrap_or("").len()
        + attr.name.as_deref().unwrap_or("").len()
        + attr.path.as_deref().unwrap_or("").len();
    n < MAX_CONTROLLED_ATTR_CACHE_SIZE
}

#[async_trait]
impl Authorizer for DelegatingAuthorizer {
    async fn authorize(
        &self,
        attrs: &rusternetes_common::authz::RequestAttributes,
    ) -> rusternetes_common::Result<Decision> {
        Ok(match self.authorize_opinion(attrs).await? {
            Opinion::Allow(_) => Decision::Allow,
            Opinion::Deny(r) => Decision::Deny(r),
            Opinion::NoOpinion { reason, .. } => Decision::Deny(reason),
        })
    }

    /// `WebhookAuthorizer.Authorize` (webhook.go:188-299).
    async fn authorize_opinion(
        &self,
        attr: &rusternetes_common::authz::RequestAttributes,
    ) -> rusternetes_common::Result<Opinion> {
        let user = &attr.user;
        let mut spec = SubjectAccessReviewSpec {
            user: Some(user.username.clone()),
            uid: Some(user.uid.clone()),
            groups: Some(user.groups.clone()),
            extra: Some(user.extra.clone()),
            ..Default::default()
        };
        if !attr.is_non_resource_request {
            spec.resource_attributes = Some(ResourceAttributes {
                namespace: attr.namespace.clone(),
                verb: Some(attr.verb.clone()),
                group: Some(attr.api_group.clone()),
                version: Some(attr.api_version.clone()),
                resource: Some(attr.resource.clone()),
                subresource: attr.subresource.clone(),
                name: attr.name.clone(),
                field_selector: None,
                label_selector: None,
            });
        } else {
            spec.non_resource_attributes = Some(NonResourceAttributes {
                path: attr.path.clone(),
                verb: Some(attr.verb.clone()),
            });
        }

        // `json.Marshal(r.Spec)`: the cache key. `extra` is a Go map, which
        // marshals with sorted keys.
        let mut keyed = serde_json::to_value(&spec).map_err(rusternetes_common::Error::from)?;
        if let Some(extra) = &spec.extra {
            let sorted: BTreeMap<_, _> = extra.iter().collect();
            keyed["extra"] =
                serde_json::to_value(sorted).map_err(rusternetes_common::Error::from)?;
        }
        let key = keyed.to_string();

        let status = match self.response_cache.get(&key, Instant::now()) {
            Some(s) => s,
            None => {
                let review = SubjectAccessReview {
                    api_version: "authorization.k8s.io/v1".to_string(),
                    kind: "SubjectAccessReview".to_string(),
                    metadata: Default::default(),
                    spec,
                    status: None,
                };
                let result = with_exponential_backoff(self.retry_backoff.clone(), || async {
                    let start = Instant::now();
                    let (status_code, result) = self.client.create(&review).await;
                    record_review_request(&*self.metrics, status_code, start.elapsed(), &result);
                    result
                })
                .await
                .map_err(|e| {
                    tracing::error!("Failed to make webhook authorizer request: {e:#}");
                    rusternetes_common::Error::Internal(format!("{e:#}"))
                })?;
                let s = result.status.unwrap_or_default();
                let status = SarStatus {
                    allowed: s.allowed,
                    denied: s.denied.unwrap_or(false),
                    reason: s.reason.unwrap_or_default(),
                };
                if should_cache(attr) {
                    let ttl = if status.allowed {
                        self.authorized_ttl
                    } else {
                        self.unauthorized_ttl
                    };
                    self.response_cache
                        .add(key, status.clone(), ttl, Instant::now());
                }
                status
            }
        };
        if status.denied && status.allowed {
            return Err(rusternetes_common::Error::Internal(
                "webhook subject access review returned both allow and deny response".into(),
            ));
        }
        Ok(if status.denied {
            Opinion::Deny(status.reason)
        } else if status.allowed {
            Opinion::Allow(status.reason)
        } else {
            Opinion::NoOpinion {
                reason: status.reason,
                error: None,
            }
        })
    }

    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> rusternetes_common::Result<(
        Vec<rusternetes_common::resources::ResourceRule>,
        Vec<rusternetes_common::resources::NonResourceRule>,
    )> {
        Ok((Vec::new(), Vec::new()))
    }
}

/// `BuildAuthz` (cmd/kubelet/app/auth.go:117-141).
pub fn build_authz(
    client: Option<Arc<dyn SubjectAccessReviewClient>>,
    authz: &AuthzConfig,
) -> anyhow::Result<Arc<dyn Authorizer>> {
    match &authz.mode {
        AuthzMode::AlwaysAllow => Ok(Arc::new(AlwaysAllowAuthorizer)),
        AuthzMode::Webhook => {
            let Some(client) = client else {
                anyhow::bail!("no client provided, cannot use webhook authorization");
            };
            Ok(Arc::new(DelegatingAuthorizer::new(
                client,
                authz.cache_authorized_ttl,
                authz.cache_unauthorized_ttl,
                Backoff::default_auth_webhook(),
                new_delegating_authorizer_metrics(),
            )))
        }
        AuthzMode::Other(m) if m.is_empty() => anyhow::bail!("no authorization mode specified"),
        AuthzMode::Other(m) => anyhow::bail!("unknown authorization mode {m}"),
    }
}

/// `server.AuthInterface` as built by `server.NewKubeletAuth`: the
/// authenticator, the attribute getter for this node and the authorizer.
pub struct KubeletAuth {
    pub authenticator: Arc<dyn RequestAuthenticator>,
    pub node_name: String,
    pub authorizer: Arc<dyn Authorizer>,
}

/// `BuildAuth` (cmd/kubelet/app/auth.go:43-67). `client` is the kubelet's API
/// client (`kubeDeps.KubeClient`), `None` in standalone mode.
pub fn build_auth(
    node_name: &str,
    client: Option<Arc<ApiClient>>,
    authn: &AuthnConfig,
    authz: &AuthzConfig,
) -> anyhow::Result<(Arc<KubeletAuth>, Option<Arc<ClientCaContent>>)> {
    let (authenticator, ca) = build_authn(
        client.clone().map(|c| c as Arc<dyn TokenReviewClient>),
        authn,
    )?;
    let authorizer = build_authz(
        client.map(|c| c as Arc<dyn SubjectAccessReviewClient>),
        authz,
    )?;
    Ok((
        Arc::new(KubeletAuth {
            authenticator,
            node_name: node_name.to_string(),
            authorizer,
        }),
        ca,
    ))
}

// ---------------------------------------------------------------------------
// the effective configuration
// ---------------------------------------------------------------------------

/// The `--anonymous-auth`, `--authentication-token-webhook`,
/// `--authentication-token-webhook-cache-ttl`, `--client-ca-file`,
/// `--authorization-mode` and `--authorization-webhook-cache-*-ttl` flags
/// (cmd/kubelet/app/options/options.go:380-402). `None` is "not passed".
#[derive(Debug, Clone, Default)]
pub struct AuthFlags {
    pub anonymous_auth: Option<bool>,
    pub authentication_token_webhook: Option<bool>,
    pub authentication_token_webhook_cache_ttl: Option<Duration>,
    pub client_ca_file: Option<String>,
    pub authorization_mode: Option<String>,
    pub authorization_webhook_cache_authorized_ttl: Option<Duration>,
    pub authorization_webhook_cache_unauthorized_ttl: Option<Duration>,
}

/// Resolve the effective authn/authz configuration.
///
/// Without `--config` the baseline is `applyLegacyDefaults`
/// (options.go:211-221): anonymous on, webhook authentication off,
/// `AlwaysAllow`. With `--config` the baseline is the `v1beta1` defaulting of
/// the file (defaults.go:92-109): anonymous off, webhook authentication on,
/// `Webhook` mode. A flag that was passed wins over either
/// (`kubeletConfigFlagPrecedence`). A zero TTL means unset and takes the
/// default (defaults.go).
pub fn resolve_auth_config(
    config_file: Option<&KubeletConfiguration>,
    flags: &AuthFlags,
) -> (AuthnConfig, AuthzConfig) {
    let nonzero = |d: Option<Duration>| d.filter(|d| !d.is_zero());
    let (anonymous, webhook, mode) = match config_file {
        None => (true, false, "AlwaysAllow".to_string()),
        Some(c) => (
            c.authentication
                .as_ref()
                .and_then(|a| a.anonymous.enabled)
                .unwrap_or(false),
            c.authentication
                .as_ref()
                .and_then(|a| a.webhook.enabled)
                .unwrap_or(true),
            c.authorization
                .as_ref()
                .and_then(|a| a.mode.clone())
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| "Webhook".to_string()),
        ),
    };
    let client_ca_file = flags
        .client_ca_file
        .clone()
        .or_else(|| {
            config_file
                .and_then(|c| c.authentication.as_ref())
                .and_then(|a| a.x509.client_ca_file.clone())
        })
        .unwrap_or_default();
    let authn = AuthnConfig {
        client_ca_file,
        webhook_enabled: flags.authentication_token_webhook.unwrap_or(webhook),
        webhook_cache_ttl: nonzero(flags.authentication_token_webhook_cache_ttl).unwrap_or_else(
            || {
                config_file
                    .map(|c| c.effective_authentication_webhook_cache_ttl())
                    .unwrap_or(Duration::from_secs(120))
            },
        ),
        anonymous_enabled: flags.anonymous_auth.unwrap_or(anonymous),
    };
    let authz = AuthzConfig {
        mode: AuthzMode::parse(flags.authorization_mode.as_deref().unwrap_or(&mode)),
        cache_authorized_ttl: nonzero(flags.authorization_webhook_cache_authorized_ttl)
            .unwrap_or_else(|| {
                config_file
                    .map(|c| c.effective_authorization_webhook_cache_authorized_ttl())
                    .unwrap_or(Duration::from_secs(300))
            }),
        cache_unauthorized_ttl: nonzero(flags.authorization_webhook_cache_unauthorized_ttl)
            .unwrap_or_else(|| {
                config_file
                    .map(|c| c.effective_authorization_webhook_cache_unauthorized_ttl())
                    .unwrap_or(Duration::from_secs(30))
            }),
    };
    (authn, authz)
}

// ---------------------------------------------------------------------------
// the filter
// ---------------------------------------------------------------------------

fn error_string(status: StatusCode, msg: impl Into<String>) -> Response {
    // `resp.WriteErrorString`: a plain-text body.
    (status, msg.into()).into_response()
}

/// `(*Server).InstallAuthFilter` (server.go:333-379) as a middleware over the
/// whole router (go-restful's `Container.Filter` wraps every route).
pub async fn auth_filter(
    State(auth): State<Arc<KubeletAuth>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    // Authenticate
    let peer = req.extensions().get::<PeerCertificates>().cloned();
    let user = {
        let mut areq = AuthnRequest {
            headers: req.headers_mut(),
            peer_certs: peer.as_ref(),
        };
        auth.authenticator.authenticate_request(&mut areq).await
    };
    let user = match user {
        Err(e) => {
            tracing::error!("Unable to authenticate the request due to an error: {e}");
            return error_string(StatusCode::UNAUTHORIZED, "Unauthorized");
        }
        Ok(None) => return error_string(StatusCode::UNAUTHORIZED, "Unauthorized"),
        Ok(Some(u)) => u,
    };

    // Get authorization attributes
    let attrs = get_request_attributes(
        &auth.node_name,
        &user.username,
        req.method().as_str(),
        req.uri().path(),
    );
    let mut allowed = false;
    let mut subresources: Vec<String> = Vec::new();
    for attr in &attrs {
        subresources.push(attr.subresource.clone());
        match auth
            .authorizer
            .authorize_opinion(&to_authz_attributes(attr, &user))
            .await
        {
            Err(e) => {
                tracing::error!(
                    "Authorization error user={} verb={} resource={} subresource={}: {e}",
                    attr.user,
                    attr.verb,
                    attr.resource,
                    attr.subresource
                );
                return error_string(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "Authorization error (user={}, verb={}, resource={}, subresource={})",
                        attr.user, attr.verb, attr.resource, attr.subresource
                    ),
                );
            }
            Ok(Opinion::Allow(_)) => {
                allowed = true;
                break;
            }
            Ok(_) => {}
        }
    }

    if !allowed {
        let Some(first) = attrs.first() else {
            tracing::error!("Authorization error: could not determine attributes for request");
            return error_string(
                StatusCode::FORBIDDEN,
                "Authorization error: could not determine attributes for request",
            );
        };
        // The attributes only differ by subresource so we just use the first one.
        tracing::debug!(
            "Forbidden user={} verb={} resource={} subresource(s)={:?}",
            first.user,
            first.verb,
            first.resource,
            subresources
        );
        return error_string(
            StatusCode::FORBIDDEN,
            format!(
                "Forbidden (user={}, verb={}, resource={}, subresource(s)=[{}])\n",
                first.user,
                first.verb,
                first.resource,
                subresources.join(" ")
            ),
        );
    }

    // Continue
    next.run(req).await
}

/// The `authorizer.Attributes` for a [`RequestAttributes`] of `user`.
fn to_authz_attributes(
    a: &RequestAttributes,
    user: &UserInfo,
) -> rusternetes_common::authz::RequestAttributes {
    let mut r = rusternetes_common::authz::RequestAttributes::new(
        user.clone(),
        a.verb.clone(),
        a.resource.clone(),
    );
    r.api_group = a.api_group.clone();
    r.api_version = a.api_version.clone();
    r.namespace = (!a.namespace.is_empty()).then(|| a.namespace.clone());
    r.name = (!a.name.is_empty()).then(|| a.name.clone());
    r.subresource = (!a.subresource.is_empty()).then(|| a.subresource.clone());
    r.path = Some(a.path.clone());
    r.is_non_resource_request = !a.resource_request;
    r
}

/// Wrap `router` in [`auth_filter`] when `auth` is set (server.go:312-314:
/// `if auth != nil { server.InstallAuthFilter(ctx) }`).
pub fn install_auth_filter(router: axum::Router, auth: Option<Arc<KubeletAuth>>) -> axum::Router {
    match auth {
        Some(auth) => router.layer(axum::middleware::from_fn_with_state(auth, auth_filter)),
        None => router,
    }
}
