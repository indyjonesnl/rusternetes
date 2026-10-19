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

/// `authenticationv1client.TokenReviewInterface.Create`.
#[async_trait]
pub trait TokenReviewClient: Send + Sync {
    async fn create(&self, review: &TokenReview) -> anyhow::Result<TokenReview>;
}

/// `authorizationv1client.SubjectAccessReviewInterface.Create`.
#[async_trait]
pub trait SubjectAccessReviewClient: Send + Sync {
    async fn create(&self, review: &SubjectAccessReview) -> anyhow::Result<SubjectAccessReview>;
}

#[async_trait]
impl TokenReviewClient for ApiClient {
    async fn create(&self, review: &TokenReview) -> anyhow::Result<TokenReview> {
        self.post("/apis/authentication.k8s.io/v1/tokenreviews", review)
            .await
    }
}

#[async_trait]
impl SubjectAccessReviewClient for ApiClient {
    async fn create(&self, review: &SubjectAccessReview) -> anyhow::Result<SubjectAccessReview> {
        self.post("/apis/authorization.k8s.io/v1/subjectaccessreviews", review)
            .await
    }
}

/// `webhook.DefaultShouldRetry` (`pkg/util/webhook/webhook.go:59-73`):
/// connection reset, `utilnet.IsHTTP2ConnectionLost`, an internal error /
/// timeout / too-many-requests status, or `apierrors.SuggestsClientDelay`
/// (a `Retry-After` is "an explicit confirmation we should retry").
fn default_should_retry(err: &anyhow::Error) -> bool {
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
// webhook token authenticator
// ---------------------------------------------------------------------------

/// `webhook.WebhookTokenAuthenticator` (webhook.go:70-76).
pub struct WebhookTokenAuthenticator {
    token_review: Arc<dyn TokenReviewClient>,
    retry_backoff: Backoff,
}

impl WebhookTokenAuthenticator {
    /// `webhook.NewFromInterface` (webhook.go:94-97).
    pub fn new(token_review: Arc<dyn TokenReviewClient>, retry_backoff: Backoff) -> Self {
        Self {
            token_review,
            retry_backoff,
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
        let result = with_exponential_backoff(self.retry_backoff.clone(), || {
            self.token_review.create(&review)
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

const BEARER_PROTOCOL_PREFIX: &str = "base64url.bearer.authorization.k8s.io.";

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

/// `dynamiccertificates.DynamicFileCAContent` for the kubelet's
/// `authentication.x509.clientCAFile` (`NewDynamicCAContentFromFile
/// ("client-ca-bundle", ...)`): a verifier over the file's bundle, re-read by
/// [`ClientCaContent::run`] every `FileRefreshDuration` (1 minute).
pub struct ClientCaContent {
    path: String,
    verifier: RwLock<Arc<dyn ClientCertVerifier>>,
}

/// `FileRefreshDuration` (dynamic_cafile_content.go:39).
const FILE_REFRESH_DURATION: Duration = Duration::from_secs(60);

impl ClientCaContent {
    /// `NewDynamicCAContentFromFile`: loads the file now, failing when it
    /// cannot be loaded.
    pub fn from_file(path: &str) -> anyhow::Result<Arc<Self>> {
        let verifier = rusternetes_common::tls::client_cert_verifier_for_ca_file(path)?;
        Ok(Arc::new(Self {
            path: path.to_string(),
            verifier: RwLock::new(verifier),
        }))
    }

    /// The verifier for the bundle as last loaded (`VerifyOptions`).
    pub fn verifier(&self) -> Arc<dyn ClientCertVerifier> {
        self.verifier.read().unwrap().clone()
    }

    /// The `Run` loop (dynamic_cafile_content.go:157-172): reload the file on
    /// every tick; a reload that fails keeps the previous bundle.
    pub async fn run(self: Arc<Self>, mut stop: tokio::sync::watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(FILE_REFRESH_DURATION);
        tick.tick().await;
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = stop.changed() => return,
            }
            match rusternetes_common::tls::client_cert_verifier_for_ca_file(&self.path) {
                Ok(v) => *self.verifier.write().unwrap() = v,
                Err(e) => tracing::error!("Reloading client CA {}: {e:#}", self.path),
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
    authenticator: Arc<dyn RequestAuthenticator>,
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
        let token_auth = WebhookTokenAuthenticator::new(client, Backoff::default_auth_webhook());
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
}

impl DelegatingAuthorizer {
    pub fn new(
        client: Arc<dyn SubjectAccessReviewClient>,
        authorized_ttl: Duration,
        unauthorized_ttl: Duration,
        retry_backoff: Backoff,
    ) -> Self {
        Self {
            client,
            response_cache: LruExpireCache::new(RESPONSE_CACHE_SIZE),
            authorized_ttl,
            unauthorized_ttl,
            retry_backoff,
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
                let result = with_exponential_backoff(self.retry_backoff.clone(), || {
                    self.client.create(&review)
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

#[cfg(test)]
mod tests {
    #[test]
    fn default_should_retry_covers_delay_and_http2_lost() {
        let st = |code: u16, reason: &str, secs: Option<i32>| {
            anyhow::Error::new(api_status::ApiStatusError {
                code,
                reason: reason.into(),
                message: "m".into(),
                retry_after_seconds: secs,
            })
        };
        // SuggestsClientDelay: Retry-After on an otherwise non-retriable code.
        assert!(default_should_retry(&st(
            503,
            "ServiceUnavailable",
            Some(2)
        )));
        assert!(default_should_retry(&st(418, "", Some(1))));
        assert!(default_should_retry(&st(418, "ServerTimeout", Some(0))));
        assert!(!default_should_retry(&st(418, "", Some(0))));
        assert!(!default_should_retry(&st(403, "Forbidden", None)));
        // IsHTTP2ConnectionLost.
        assert!(default_should_retry(&anyhow::anyhow!(
            "Post: http2: client connection lost"
        )));
        assert!(!default_should_retry(&anyhow::anyhow!("boom")));
    }

    use super::*;
    use axum::routing::get;
    use axum::Router;
    use rusternetes_common::resources::{SubjectAccessReviewStatus, TokenReviewStatus};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    // ---- fakes -----------------------------------------------------------

    /// A TokenReview backend that authenticates `good` as `alice` (groups
    /// `dev`) and reports an `error` for `broken`.
    #[derive(Default)]
    struct FakeTokenReview {
        calls: AtomicUsize,
        /// Fail this many leading calls with a 500 (`IsInternalError`).
        transient_failures: AtomicUsize,
    }

    #[async_trait]
    impl TokenReviewClient for FakeTokenReview {
        async fn create(&self, review: &TokenReview) -> anyhow::Result<TokenReview> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self
                .transient_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(anyhow::Error::new(api_status::ApiStatusError {
                    code: 500,
                    reason: "InternalError".into(),
                    message: "boom".into(),
                    retry_after_seconds: None,
                }));
            }
            let status = match review.spec.token.as_str() {
                "good" => TokenReviewStatus {
                    authenticated: Some(true),
                    user: Some(rusternetes_common::resources::authentication::UserInfo {
                        username: Some("alice".into()),
                        uid: Some("u1".into()),
                        groups: Some(vec!["dev".into()]),
                        extra: None,
                    }),
                    ..Default::default()
                },
                "broken" => TokenReviewStatus {
                    authenticated: Some(false),
                    error: Some("token is broken".into()),
                    ..Default::default()
                },
                _ => TokenReviewStatus {
                    authenticated: Some(false),
                    ..Default::default()
                },
            };
            let mut out = review.clone();
            out.status = Some(status);
            Ok(out)
        }
    }

    /// A SubjectAccessReview backend that records the specs it was asked and
    /// answers by `subresource`: `log` and `stats` are allowed, `metrics` is
    /// explicitly denied, `checkpoint` errors, anything else gets no opinion.
    #[derive(Default)]
    struct FakeSar {
        calls: AtomicUsize,
        specs: Mutex<Vec<SubjectAccessReviewSpec>>,
    }

    #[async_trait]
    impl SubjectAccessReviewClient for FakeSar {
        async fn create(
            &self,
            review: &SubjectAccessReview,
        ) -> anyhow::Result<SubjectAccessReview> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.specs.lock().unwrap().push(review.spec.clone());
            let sub = review
                .spec
                .resource_attributes
                .as_ref()
                .and_then(|r| r.subresource.clone())
                .unwrap_or_default();
            if sub == "checkpoint" {
                return Err(anyhow::Error::new(api_status::ApiStatusError {
                    code: 403,
                    reason: "Forbidden".into(),
                    message: "sar forbidden".into(),
                    retry_after_seconds: None,
                }));
            }
            let status = SubjectAccessReviewStatus {
                allowed: sub == "log" || sub == "stats",
                denied: (sub == "metrics").then_some(true),
                reason: Some(format!("fake:{sub}")),
                ..Default::default()
            };
            let mut out = review.clone();
            out.status = Some(status);
            Ok(out)
        }
    }

    fn authn(webhook: bool, anonymous: bool) -> AuthnConfig {
        AuthnConfig {
            client_ca_file: String::new(),
            webhook_enabled: webhook,
            webhook_cache_ttl: Duration::from_secs(120),
            anonymous_enabled: anonymous,
        }
    }

    fn authz_webhook() -> AuthzConfig {
        AuthzConfig {
            mode: AuthzMode::Webhook,
            cache_authorized_ttl: Duration::from_secs(300),
            cache_unauthorized_ttl: Duration::from_secs(30),
        }
    }

    fn app(auth: KubeletAuth) -> Router {
        let routes = Router::new()
            .route("/pods", get(|| async { "pods" }))
            .route("/stats/summary", get(|| async { "stats" }))
            .route("/metrics", get(|| async { "metrics" }))
            .route("/logs/x", get(|| async { "logs" }))
            .route("/checkpoint/a/b/c", get(|| async { "cp" }));
        install_auth_filter(routes, Some(Arc::new(auth)))
    }

    fn auth_with(
        tr: Arc<FakeTokenReview>,
        sar: Arc<FakeSar>,
        authn_cfg: &AuthnConfig,
        authz_cfg: &AuthzConfig,
    ) -> KubeletAuth {
        let (authenticator, _) = build_authn(Some(tr), authn_cfg).unwrap();
        let authorizer = build_authz(Some(sar), authz_cfg).unwrap();
        KubeletAuth {
            authenticator,
            node_name: "node-1".into(),
            authorizer,
        }
    }

    async fn call(app: &Router, path: &str, bearer: Option<&str>) -> (u16, String) {
        let mut req = axum::http::Request::builder().uri(path);
        if let Some(t) = bearer {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        let resp = app
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status().as_u16();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    // ---- the filter (server.go InstallAuthFilter) --------------------------

    // Before this change nothing wrapped the routes: an unauthenticated
    // request to the kubelet API was served.
    #[tokio::test]
    async fn unauthenticated_request_is_401() {
        let app = app(auth_with(
            Arc::default(),
            Arc::default(),
            &authn(true, false),
            &authz_webhook(),
        ));
        assert_eq!(
            call(&app, "/pods", None).await,
            (401, "Unauthorized".into())
        );
        // an unauthenticated bearer token is an error (bearertoken: "invalid bearer token")
        assert_eq!(
            call(&app, "/pods", Some("nope")).await,
            (401, "Unauthorized".into())
        );
        assert_eq!(
            call(&app, "/pods", Some("broken")).await,
            (401, "Unauthorized".into())
        );
    }

    #[tokio::test]
    async fn authenticated_and_authorized_request_is_served() {
        let sar = Arc::new(FakeSar::default());
        let app = app(auth_with(
            Arc::default(),
            sar.clone(),
            &authn(true, false),
            &authz_webhook(),
        ));
        assert_eq!(
            call(&app, "/stats/summary", Some("good")).await,
            (200, "stats".into())
        );
        let specs = sar.specs.lock().unwrap();
        let spec = &specs[0];
        assert_eq!(spec.user.as_deref(), Some("alice"));
        assert_eq!(spec.uid.as_deref(), Some("u1"));
        assert_eq!(
            spec.groups.as_deref(),
            Some(&["dev".to_string(), "system:authenticated".to_string()][..])
        );
        let ra = spec.resource_attributes.as_ref().unwrap();
        assert_eq!(ra.resource.as_deref(), Some("nodes"));
        assert_eq!(ra.subresource.as_deref(), Some("stats"));
        assert_eq!(ra.name.as_deref(), Some("node-1"));
        assert_eq!(ra.verb.as_deref(), Some("get"));
        assert_eq!(ra.version.as_deref(), Some("v1"));
    }

    // server.go:371-377: the Forbidden text.
    #[tokio::test]
    async fn authenticated_but_unauthorized_request_is_403() {
        let app = app(auth_with(
            Arc::default(),
            Arc::default(),
            &authn(true, false),
            &authz_webhook(),
        ));
        // `metrics` is an explicit deny; `proxy` (/pods) gets no opinion.
        assert_eq!(
            call(&app, "/metrics", Some("good")).await,
            (
                403,
                "Forbidden (user=alice, verb=get, resource=nodes, subresource(s)=[metrics])\n"
                    .into()
            )
        );
        assert_eq!(call(&app, "/pods", Some("good")).await.0, 403);
    }

    // server.go:359-364: an authorizer error is a 500, not a 403.
    #[tokio::test]
    async fn authorization_error_is_500() {
        let app = app(auth_with(
            Arc::default(),
            Arc::default(),
            &authn(true, false),
            &authz_webhook(),
        ));
        assert_eq!(
            call(&app, "/checkpoint/a/b/c", Some("good")).await,
            (
                500,
                "Authorization error (user=alice, verb=get, resource=nodes, subresource=checkpoint)"
                    .into()
            )
        );
    }

    // delegating.go:121-127: anonymous runs behind `NewFailOnError`.
    #[tokio::test]
    async fn anonymous_is_only_for_requests_without_credentials() {
        let sar = Arc::new(FakeSar::default());
        let app = app(auth_with(
            Arc::default(),
            sar.clone(),
            &authn(true, true),
            &authz_webhook(),
        ));
        // no credentials -> system:anonymous, allowed for `log`
        assert_eq!(call(&app, "/logs/x", None).await, (200, "logs".into()));
        {
            let specs = sar.specs.lock().unwrap();
            assert_eq!(specs[0].user.as_deref(), Some("system:anonymous"));
            assert_eq!(
                specs[0].groups.as_deref(),
                Some(&["system:unauthenticated".to_string()][..])
            );
        }
        // a rejected token must NOT fall back to anonymous
        assert_eq!(call(&app, "/logs/x", Some("nope")).await.0, 401);
    }

    // BuildAuthz AlwaysAllow + anonymous only (the legacy flag defaults).
    #[tokio::test]
    async fn legacy_defaults_serve_everything() {
        let (authn_cfg, authz_cfg) = resolve_auth_config(None, &AuthFlags::default());
        let (authenticator, _) = build_authn(None, &authn_cfg).unwrap();
        let app = app(KubeletAuth {
            authenticator,
            node_name: "node-1".into(),
            authorizer: build_authz(None, &authz_cfg).unwrap(),
        });
        assert_eq!(call(&app, "/pods", None).await, (200, "pods".into()));
    }

    // ---- caching (cached_token_authenticator.go / webhook.go) --------------

    // The consumer of `authentication.webhook.cacheTTL`.
    #[tokio::test]
    async fn token_review_results_are_cached_for_the_ttl() {
        let tr = Arc::new(FakeTokenReview::default());
        let (a, _) = build_authn(Some(tr.clone()), &authn(true, false)).unwrap();
        let mut h = HeaderMap::new();
        for _ in 0..3 {
            h.insert("authorization", "Bearer good".parse().unwrap());
            let mut r = AuthnRequest {
                headers: &mut h,
                peer_certs: None,
            };
            let u = a.authenticate_request(&mut r).await.unwrap().unwrap();
            assert_eq!(u.username, "alice");
            // bearertoken deletes the header once authenticated
            assert!(h.get("authorization").is_none());
        }
        assert_eq!(tr.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn zero_cache_ttl_does_not_cache() {
        let tr = Arc::new(FakeTokenReview::default());
        let mut cfg = authn(true, false);
        cfg.webhook_cache_ttl = Duration::ZERO;
        let (a, _) = build_authn(Some(tr.clone()), &cfg).unwrap();
        for _ in 0..2 {
            let mut h = HeaderMap::new();
            h.insert("authorization", "Bearer good".parse().unwrap());
            let mut r = AuthnRequest {
                headers: &mut h,
                peer_certs: None,
            };
            a.authenticate_request(&mut r).await.unwrap().unwrap();
        }
        assert_eq!(tr.calls.load(Ordering::SeqCst), 2);
    }

    // cached_token_authenticator_test.go TestSharedLookup: concurrent lookups
    // of one token share a single call.
    #[tokio::test]
    async fn concurrent_lookups_share_one_call() {
        struct Slow(AtomicUsize);
        #[async_trait]
        impl TokenAuthenticator for Slow {
            async fn authenticate_token(&self, _t: &str) -> AuthnResult {
                self.0.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(Some(UserInfo {
                    username: "bob".into(),
                    uid: String::new(),
                    groups: vec![],
                    extra: HashMap::new(),
                }))
            }
        }
        let slow = Arc::new(Slow(AtomicUsize::new(0)));
        let c = CachedTokenAuthenticator::new(
            slow.clone(),
            false,
            Duration::ZERO, // not cached: only the in-flight sharing applies
            Duration::ZERO,
        );
        let rs = futures::future::join_all((0..8).map(|_| c.authenticate_token("t"))).await;
        assert!(rs.iter().all(|r| r.as_ref().unwrap().is_some()));
        assert_eq!(slow.0.load(Ordering::SeqCst), 1);
    }

    // webhook.go:262-285: allow and deny answers are cached (5m / 30s).
    #[tokio::test]
    async fn sar_responses_are_cached() {
        let sar = Arc::new(FakeSar::default());
        let app = app(auth_with(
            Arc::default(),
            sar.clone(),
            &authn(true, false),
            &authz_webhook(),
        ));
        for _ in 0..3 {
            assert_eq!(call(&app, "/stats/summary", Some("good")).await.0, 200);
            assert_eq!(call(&app, "/metrics", Some("good")).await.0, 403);
        }
        // one SAR per distinct spec, not per request
        assert_eq!(sar.calls.load(Ordering::SeqCst), 2);
    }

    // webhook.go:235 `WithExponentialBackoff`/`DefaultShouldRetry`: a 500 is
    // retried, anything else is not.
    #[tokio::test(start_paused = true)]
    async fn transient_token_review_errors_are_retried() {
        let tr = Arc::new(FakeTokenReview::default());
        tr.transient_failures.store(2, Ordering::SeqCst);
        let w = WebhookTokenAuthenticator::new(tr.clone(), Backoff::default_auth_webhook());
        let u = w.authenticate_token("good").await.unwrap().unwrap();
        assert_eq!(u.username, "alice");
        assert_eq!(tr.calls.load(Ordering::SeqCst), 3);

        // exhausted: 5 attempts (`Steps: 5`), the webhook error is returned
        let tr = Arc::new(FakeTokenReview::default());
        tr.transient_failures.store(100, Ordering::SeqCst);
        let w = WebhookTokenAuthenticator::new(tr.clone(), Backoff::default_auth_webhook());
        assert!(w.authenticate_token("good").await.is_err());
        assert_eq!(tr.calls.load(Ordering::SeqCst), 5);
    }

    // webhook.go:~190: `status.error` is the authentication error.
    #[tokio::test]
    async fn token_review_status_error_is_an_error() {
        let w = WebhookTokenAuthenticator::new(
            Arc::new(FakeTokenReview::default()),
            Backoff::default_auth_webhook(),
        );
        assert_eq!(
            w.authenticate_token("broken").await,
            Err(AuthnError("token is broken".into()))
        );
        assert_eq!(w.authenticate_token("who").await, Ok(None));
    }

    // ---- BuildAuthn / BuildAuthz (cmd/kubelet/app/auth.go) -----------------

    #[test]
    fn build_authn_errors() {
        let err = |r: anyhow::Result<_>| r.err().map(|e| e.to_string());
        assert_eq!(
            err(build_authn(None, &authn(true, false))),
            Some("no client provided, cannot use webhook authentication".into())
        );
        assert_eq!(
            err(build_authn(None, &authn(false, false))),
            Some("no authentication method configured".into())
        );
        assert!(build_authn(None, &authn(false, true)).is_ok());
    }

    #[test]
    fn build_authz_modes() {
        let cfg = |mode: AuthzMode| AuthzConfig {
            mode,
            ..authz_webhook()
        };
        let err = |r: anyhow::Result<_>| r.err().map(|e| e.to_string());
        assert!(build_authz(None, &cfg(AuthzMode::AlwaysAllow)).is_ok());
        assert_eq!(
            err(build_authz(None, &cfg(AuthzMode::Webhook))),
            Some("no client provided, cannot use webhook authorization".into())
        );
        assert_eq!(
            err(build_authz(None, &cfg(AuthzMode::parse("")))),
            Some("no authorization mode specified".into())
        );
        assert_eq!(
            err(build_authz(None, &cfg(AuthzMode::parse("RBAC")))),
            Some("unknown authorization mode RBAC".into())
        );
    }

    // options.go:211-221 (no --config) vs v1beta1/defaults.go:92-109 (--config).
    #[test]
    fn resolve_auth_config_defaults_and_precedence() {
        let (n, z) = resolve_auth_config(None, &AuthFlags::default());
        assert!(n.anonymous_enabled && !n.webhook_enabled);
        assert_eq!(z.mode, AuthzMode::AlwaysAllow);
        assert_eq!(n.webhook_cache_ttl, Duration::from_secs(120));
        assert_eq!(z.cache_authorized_ttl, Duration::from_secs(300));
        assert_eq!(z.cache_unauthorized_ttl, Duration::from_secs(30));

        let file: KubeletConfiguration = serde_yaml::from_str(
            "apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\n\
             authentication:\n  x509:\n    clientCAFile: /ca.pem\n  webhook:\n    cacheTTL: 10s\n\
             authorization:\n  webhook:\n    cacheAuthorizedTTL: 1m\n    cacheUnauthorizedTTL: 5s\n",
        )
        .unwrap();
        let (n, z) = resolve_auth_config(Some(&file), &AuthFlags::default());
        assert!(!n.anonymous_enabled && n.webhook_enabled);
        assert_eq!(z.mode, AuthzMode::Webhook);
        assert_eq!(n.client_ca_file, "/ca.pem");
        assert_eq!(n.webhook_cache_ttl, Duration::from_secs(10));
        assert_eq!(z.cache_authorized_ttl, Duration::from_secs(60));
        assert_eq!(z.cache_unauthorized_ttl, Duration::from_secs(5));

        // a flag that was passed wins over the file
        let (n, z) = resolve_auth_config(
            Some(&file),
            &AuthFlags {
                anonymous_auth: Some(true),
                authentication_token_webhook: Some(false),
                authorization_mode: Some("AlwaysAllow".into()),
                client_ca_file: Some("/other.pem".into()),
                authentication_token_webhook_cache_ttl: Some(Duration::from_secs(7)),
                ..Default::default()
            },
        );
        assert!(n.anonymous_enabled && !n.webhook_enabled);
        assert_eq!(z.mode, AuthzMode::AlwaysAllow);
        assert_eq!(n.client_ca_file, "/other.pem");
        assert_eq!(n.webhook_cache_ttl, Duration::from_secs(7));
    }

    // ---- request authenticators (bearertoken / websocket / union / group) ---

    struct Tok;
    #[async_trait]
    impl TokenAuthenticator for Tok {
        async fn authenticate_token(&self, t: &str) -> AuthnResult {
            Ok((t == "tok").then(|| UserInfo {
                username: "u".into(),
                uid: String::new(),
                groups: vec![],
                extra: HashMap::new(),
            }))
        }
    }

    // bearertoken_test.go
    #[tokio::test]
    async fn bearer_token_header_parsing() {
        let a = BearerTokenAuthenticator::new(Arc::new(Tok));
        let run = |v: Option<&'static str>| {
            let a = &a;
            async move {
                let mut h = HeaderMap::new();
                if let Some(v) = v {
                    h.insert("authorization", v.parse().unwrap());
                }
                a.authenticate_request(&mut AuthnRequest {
                    headers: &mut h,
                    peer_certs: None,
                })
                .await
            }
        };
        assert_eq!(run(None).await, Ok(None));
        assert_eq!(run(Some("Basic abc")).await, Ok(None));
        assert_eq!(run(Some("Bearer")).await, Ok(None));
        assert_eq!(run(Some("Bearer  tok")).await, Ok(None)); // extra space
        assert_eq!(
            run(Some("Bearer bad")).await,
            Err(AuthnError("invalid bearer token".into()))
        );
        assert_eq!(
            run(Some("bearer tok")).await.unwrap().unwrap().username,
            "u"
        );
        assert_eq!(
            run(Some("BEARER tok x")).await.unwrap().unwrap().username,
            "u"
        );
    }

    // websocket/protocol_test.go
    #[tokio::test]
    async fn websocket_protocol_bearer_token() {
        let a = WebsocketProtocolAuthenticator::new(Arc::new(Tok));
        let enc = |t: &str| {
            format!(
                "{BEARER_PROTOCOL_PREFIX}{}",
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(t)
            )
        };
        let run = |proto: Option<String>, ws: bool| {
            let a = &a;
            async move {
                let mut h = HeaderMap::new();
                if ws {
                    h.insert("upgrade", "websocket".parse().unwrap());
                    h.insert("connection", "keep-alive, Upgrade".parse().unwrap());
                }
                if let Some(p) = proto {
                    h.insert("sec-websocket-protocol", p.parse().unwrap());
                }
                let r = a
                    .authenticate_request(&mut AuthnRequest {
                        headers: &mut h,
                        peer_certs: None,
                    })
                    .await;
                (r, h)
            }
        };
        // not a websocket request
        assert_eq!(
            run(Some(format!("{}, v4.channel.k8s.io", enc("tok"))), false)
                .await
                .0,
            Ok(None)
        );
        let (r, h) = run(Some(format!("{}, v4.channel.k8s.io", enc("tok"))), true).await;
        assert_eq!(r.unwrap().unwrap().username, "u");
        // the token protocol is removed from the header
        assert_eq!(
            h.get("sec-websocket-protocol").unwrap(),
            "v4.channel.k8s.io"
        );
        assert_eq!(
            run(Some(enc("tok")), true).await.0,
            Err(AuthnError("missing additional subprotocol".into()))
        );
        assert_eq!(
            run(Some(format!("{}, {}, x", enc("tok"), enc("tok"))), true)
                .await
                .0,
            Err(AuthnError(
                "multiple base64.bearer.authorization tokens specified".into()
            ))
        );
        assert_eq!(
            run(Some(format!("{}!!, x", BEARER_PROTOCOL_PREFIX)), true)
                .await
                .0,
            Err(AuthnError(
                "invalid base64.bearer.authorization token encoding".into()
            ))
        );
        assert_eq!(
            run(Some(format!("{}, x", enc("bad"))), true).await.0,
            Err(AuthnError("invalid bearer token".into()))
        );
        assert_eq!(run(Some("x".into()), true).await.0, Ok(None));
    }

    // authenticated_group_adder_test.go
    #[tokio::test]
    async fn authenticated_group_adder() {
        struct Fixed(&'static str, Vec<&'static str>);
        #[async_trait]
        impl RequestAuthenticator for Fixed {
            async fn authenticate_request(&self, _r: &mut AuthnRequest<'_>) -> AuthnResult {
                Ok(Some(UserInfo {
                    username: self.0.into(),
                    uid: String::new(),
                    groups: self.1.iter().map(|s| s.to_string()).collect(),
                    extra: HashMap::new(),
                }))
            }
        }
        for (name, groups, want) in [
            ("bob", vec![], vec!["system:authenticated"]),
            ("bob", vec!["g"], vec!["g", "system:authenticated"]),
            ("system:anonymous", vec![], vec![]),
            (
                "bob",
                vec!["system:unauthenticated"],
                vec!["system:unauthenticated"],
            ),
            (
                "bob",
                vec!["system:authenticated"],
                vec!["system:authenticated"],
            ),
        ] {
            let a = AuthenticatedGroupAdder {
                authenticator: Arc::new(Fixed(name, groups)),
            };
            let mut h = HeaderMap::new();
            let u = a
                .authenticate_request(&mut AuthnRequest {
                    headers: &mut h,
                    peer_certs: None,
                })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(u.groups, want, "{name}");
        }
    }

    // union_test.go
    #[tokio::test]
    async fn union_error_aggregation() {
        struct E(Option<&'static str>);
        #[async_trait]
        impl RequestAuthenticator for E {
            async fn authenticate_request(&self, _r: &mut AuthnRequest<'_>) -> AuthnResult {
                match self.0 {
                    Some(m) => Err(AuthnError(m.into())),
                    None => Ok(None),
                }
            }
        }
        let run = |u: Arc<dyn RequestAuthenticator>| async move {
            let mut h = HeaderMap::new();
            u.authenticate_request(&mut AuthnRequest {
                headers: &mut h,
                peer_certs: None,
            })
            .await
        };
        let hs = |v: Vec<Option<&'static str>>| -> Vec<Arc<dyn RequestAuthenticator>> {
            v.into_iter().map(|m| Arc::new(E(m)) as _).collect()
        };
        assert_eq!(
            run(UnionAuthenticator::union(hs(vec![None, None]))).await,
            Ok(None)
        );
        assert_eq!(
            run(UnionAuthenticator::union(hs(vec![Some("a"), None]))).await,
            Err(AuthnError("a".into()))
        );
        assert_eq!(
            run(UnionAuthenticator::union(hs(vec![Some("a"), Some("b")]))).await,
            Err(AuthnError("[a, b]".into()))
        );
        // FailOnError stops at the first error
        assert_eq!(
            run(UnionAuthenticator::fail_on_error(hs(vec![
                Some("a"),
                Some("b")
            ])))
            .await,
            Err(AuthnError("a".into()))
        );
    }

    // ---- x509 --------------------------------------------------------------

    struct Pki {
        ca_pem: String,
        ca: rcgen::Certificate,
        ca_key: rcgen::KeyPair,
    }

    fn pki() -> Pki {
        rusternetes_common::tls::TlsConfig::generate_self_signed("x", vec!["x".into()]).ok();
        let mut p = rcgen::CertificateParams::default();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        p.distinguished_name
            .push(rcgen::DnType::CommonName, "test-ca");
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca = p.self_signed(&ca_key).unwrap();
        Pki {
            ca_pem: ca.pem(),
            ca,
            ca_key,
        }
    }

    /// (leaf DER, leaf PEM, key PEM) of a client cert for `cn`/`org`.
    fn client_cert(pki: &Pki, cn: &str, org: &str) -> (Vec<u8>, String, String) {
        let mut p = rcgen::CertificateParams::default();
        p.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        p.distinguished_name.push(rcgen::DnType::CommonName, cn);
        p.distinguished_name
            .push(rcgen::DnType::OrganizationName, org);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &pki.ca, &pki.ca_key).unwrap();
        (cert.der().to_vec(), cert.pem(), key.serialize_pem())
    }

    fn x509(pki: &Pki) -> (X509Authenticator, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, &pki.ca_pem).unwrap();
        let ca = ClientCaContent::from_file(path.to_str().unwrap()).unwrap();
        (X509Authenticator::new(ca), dir)
    }

    // x509_test.go: CN -> user, O -> groups; an unknown CA is an error.
    #[tokio::test]
    async fn x509_authenticator() {
        let pki = pki();
        let (a, _dir) = x509(&pki);
        let (der, _, _) = client_cert(&pki, "system:node:n1", "system:nodes");
        let mut h = HeaderMap::new();
        let peer = PeerCertificates(Arc::new(vec![der]));
        let u = a
            .authenticate_request(&mut AuthnRequest {
                headers: &mut h,
                peer_certs: Some(&peer),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(u.username, "system:node:n1");
        assert!(u.groups.contains(&"system:nodes".to_string()));

        // no certificate: no opinion
        assert_eq!(
            a.authenticate_request(&mut AuthnRequest {
                headers: &mut h,
                peer_certs: None,
            })
            .await,
            Ok(None)
        );
        // a certificate from another CA: an error
        let other = self::pki();
        let (bad, _, _) = client_cert(&other, "mallory", "x");
        let peer = PeerCertificates(Arc::new(vec![bad]));
        assert!(a
            .authenticate_request(&mut AuthnRequest {
                headers: &mut h,
                peer_certs: Some(&peer),
            })
            .await
            .is_err());
    }

    // An end-to-end handshake: `ClientAuth = RequestClientCert`, the acceptor's
    // `PeerCertificates`, the x509 authenticator and the filter together.
    #[tokio::test]
    async fn x509_over_a_real_tls_connection() {
        let pki = pki();
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, &pki.ca_pem).unwrap();
        let cfg = AuthnConfig {
            client_ca_file: ca_path.to_str().unwrap().into(),
            ..authn(false, true)
        };
        let (authenticator, _) = build_authn(None, &cfg).unwrap();
        let app = install_auth_filter(
            Router::new().route(
                "/pods",
                get(|req: Request<Body>| async move {
                    req.headers()
                        .get("x-seen")
                        .map(|_| "x")
                        .unwrap_or("ok")
                        .to_string()
                }),
            ),
            Some(Arc::new(KubeletAuth {
                authenticator,
                node_name: "node-1".into(),
                authorizer: Arc::new(AlwaysAllowAuthorizer),
            })),
        );
        let tls = crate::serving_tls::kubelet_serving_tls_with_client_ca(
            "node-1",
            Some(ca_path.to_str().unwrap()),
        )
        .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        tokio::spawn(
            axum_server::from_tcp_rustls(listener, tls.clone())
                .acceptor(crate::serving_tls::PeerCertAcceptor::new(tls))
                .serve(app.into_make_service()),
        );

        let (_, cert_pem, key_pem) = client_cert(&pki, "alice", "devs");
        let anon = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let with_cert = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .identity(
                reqwest::Identity::from_pem(format!("{cert_pem}{key_pem}").as_bytes()).unwrap(),
            )
            .build()
            .unwrap();
        let url = format!("https://127.0.0.1:{}/pods", addr.port());
        // both are served: anonymous is enabled, and a valid cert authenticates
        assert_eq!(anon.get(&url).send().await.unwrap().status(), 200);
        assert_eq!(with_cert.get(&url).send().await.unwrap().status(), 200);

        // a certificate from an unknown CA is a 401 (not anonymous), as
        // `RequestClientCert` leaves verification to the authenticator
        let other = self::pki();
        let (_, c, k) = client_cert(&other, "mallory", "x");
        let bad = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .identity(reqwest::Identity::from_pem(format!("{c}{k}").as_bytes()).unwrap())
            .build()
            .unwrap();
        assert_eq!(bad.get(&url).send().await.unwrap().status(), 401);
    }
}
