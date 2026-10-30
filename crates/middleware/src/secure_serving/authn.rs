//! Delegating request authentication: the authenticator chain
//! `DelegatingAuthenticatorConfig.New` builds, and its parts.
//!
//! Upstream sources (release-1.35, all under `staging/src/k8s.io/apiserver/`):
//! * `pkg/authentication/authenticatorfactory/delegating.go:61-131` (`New`)
//! * `pkg/authentication/request/bearertoken/bearertoken.go`
//! * `pkg/authentication/request/websocket/protocol.go`
//! * `pkg/authentication/request/x509/x509.go:60-302` (`Authenticator`,
//!   `Verifier`, `CommonNameUserConversion`, `parseUIDFromCert`)
//! * `pkg/authentication/request/headerrequest/requestheader.go`
//! * `pkg/authentication/request/union/union.go`
//! * `pkg/authentication/request/anonymous/anonymous.go`
//! * `pkg/authentication/group/authenticated_group_adder.go`
//! * `pkg/authentication/token/cache/cached_token_authenticator.go`
//! * `plugin/pkg/authenticator/token/webhook/webhook.go:101-193`
//!
//! Deliberate reductions, none of which is reachable from the controller-manager
//! or scheduler chains: API audiences (`DelegatingAuthenticatorConfig.APIAudiences`
//! is never set by `DelegatingAuthenticationOptions.ApplyTo`, and
//! `BuildHandlerChain` passes `nil` `apiAuds`), audit annotations and the
//! authentication warning recorder (neither filter is in that chain),
//! authentication metrics, and the `ClientCertificateExpiration` histogram.
//! Chain building is a single path (the verifier returns one chain, not Go's
//! list of chains), and the token cache is one map instead of 32 stripes.

use super::webhook::{default_should_retry, with_exponential_backoff, Backoff, ReviewClient};
use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use futures::future::{BoxFuture, FutureExt, Shared};
use hmac::{Hmac, Mac};
use rusternetes_common::auth::UserInfo;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::time::Instant;

/// `user.Anonymous` (`pkg/authentication/user/user.go`).
pub const ANONYMOUS_USER: &str = "system:anonymous";
/// `user.AllUnauthenticated`.
pub const ALL_UNAUTHENTICATED: &str = "system:unauthenticated";
/// `user.AllAuthenticated`.
pub const ALL_AUTHENTICATED: &str = "system:authenticated";
/// `user.CredentialIDKey` (user.go:87).
pub const CREDENTIAL_ID_KEY: &str = "authentication.kubernetes.io/credential-id";

/// `authenticator.Response` (audiences are out of scope, see the module docs).
#[derive(Debug, Clone)]
pub struct Response {
    pub user: UserInfo,
}

/// `(resp, ok, err)` of `AuthenticateRequest` / `AuthenticateToken`:
/// `Ok(Some)` is `ok`, `Ok(None)` is `!ok` without an error, `Err` is the
/// error (which never accompanies `ok`).
pub type AuthResult = Result<Option<Response>, String>;

/// The parts of `*http.Request` the authenticators read: headers (which they
/// also edit), the URL path and the verified TLS peer chain (`req.TLS.PeerCertificates`).
pub struct AuthRequest<'a> {
    pub headers: &'a mut HeaderMap,
    pub path: &'a str,
    /// DER, leaf first. Empty when the client presented none.
    pub peer_certs: &'a [Vec<u8>],
}

/// `authenticator.Request`.
#[async_trait]
pub trait RequestAuthenticator: Send + Sync {
    async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult;
}

/// `authenticator.Token`.
#[async_trait]
pub trait TokenAuthenticator: Send + Sync {
    async fn authenticate_token(&self, token: &str) -> AuthResult;
}

/// `utilerrors.NewAggregate(errs).Error()` (errors.go): nil for none, the
/// message for one, `[a, b]` for several distinct messages.
pub(crate) fn aggregate(errs: &[String]) -> Option<String> {
    if errs.is_empty() {
        return None;
    }
    if errs.len() == 1 {
        return Some(errs[0].clone());
    }
    let mut seen: Vec<&String> = vec![];
    for e in errs {
        if !seen.contains(&e) {
            seen.push(e);
        }
    }
    if seen.len() == 1 {
        return Some(seen[0].clone());
    }
    Some(format!(
        "[{}]",
        seen.iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

fn header_str(v: &HeaderValue) -> String {
    String::from_utf8_lossy(v.as_bytes()).into_owned()
}

// ---------------------------------------------------------------- bearertoken

/// `bearertoken.Authenticator`.
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
    async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
        let auth = req
            .headers
            .get("authorization")
            .map(header_str)
            .unwrap_or_default();
        let auth = auth.trim();
        if auth.is_empty() {
            return Ok(None);
        }
        let parts: Vec<&str> = auth.splitn(3, ' ').collect();
        if parts.len() < 2 || parts[0].to_lowercase() != "bearer" {
            return Ok(None);
        }
        let token = parts[1];
        // Empty bearer tokens aren't valid
        if token.is_empty() {
            return Ok(None);
        }
        let r = self.auth.authenticate_token(token).await;
        // if we authenticated successfully, go ahead and remove the bearer token so that no one
        // is ever tempted to use it inside of the API server
        if matches!(r, Ok(Some(_))) {
            req.headers.remove("authorization");
        }
        // If the token authenticator didn't error, provide a default error
        match r {
            Ok(None) => Err("invalid bearer token".to_string()),
            other => other,
        }
    }
}

// ------------------------------------------------------------------ websocket

const BEARER_PROTOCOL_PREFIX: &str = "base64url.bearer.authorization.k8s.io.";

/// `wsstream.IsWebSocketRequest` / `httpstream.IsUpgradeRequest`.
fn is_websocket_request(h: &HeaderMap) -> bool {
    let upgrade = h
        .get("upgrade")
        .map(header_str)
        .unwrap_or_default()
        .eq_ignore_ascii_case("websocket");
    upgrade
        && h.get_all("connection")
            .iter()
            .any(|v| header_str(v).to_lowercase().contains("upgrade"))
}

/// `websocket.ProtocolAuthenticator`: a bearer token carried as a
/// `base64url.bearer.authorization.k8s.io.<token>` subprotocol.
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
    async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
        use base64::Engine;
        // Only accept websocket connections
        if !is_websocket_request(req.headers) {
            return Ok(None);
        }
        let mut token = String::new();
        let mut saw_token_protocol = false;
        let mut filtered: Vec<String> = vec![];
        for header in req.headers.get_all("sec-websocket-protocol") {
            for protocol in header_str(header).split(',') {
                let protocol = protocol.trim();
                let Some(encoded) = protocol.strip_prefix(BEARER_PROTOCOL_PREFIX) else {
                    filtered.push(protocol.to_string());
                    continue;
                };
                if saw_token_protocol {
                    return Err("multiple base64.bearer.authorization tokens specified".into());
                }
                saw_token_protocol = true;
                let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(encoded)
                    .map_err(|_| "invalid base64.bearer.authorization token encoding")?;
                token = String::from_utf8(decoded)
                    .map_err(|_| "invalid base64.bearer.authorization token".to_string())?;
            }
        }
        // Must pass at least one other subprotocol so that we can remove the one containing the bearer token,
        // and there is at least one to echo back to the client
        if !token.is_empty() && filtered.is_empty() {
            return Err("missing additional subprotocol".into());
        }
        if token.is_empty() {
            return Ok(None);
        }
        let r = self.auth.authenticate_token(&token).await;
        // on success, remove the protocol with the token
        if matches!(r, Ok(Some(_))) {
            if let Ok(v) = HeaderValue::from_str(&filtered.join(",")) {
                req.headers.insert("sec-websocket-protocol", v);
            }
        }
        match r {
            Ok(None) => Err("invalid bearer token".to_string()),
            other => other,
        }
    }
}

// ------------------------------------------------------------ anonymous/union

/// `anonymous.Authenticator`; `allowed_paths` are the `Conditions` paths.
pub struct AnonymousAuthenticator {
    pub allowed_paths: Vec<String>,
}

#[async_trait]
impl RequestAuthenticator for AnonymousAuthenticator {
    async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
        if !self.allowed_paths.is_empty() && !self.allowed_paths.iter().any(|p| p == req.path) {
            return Ok(None);
        }
        Ok(Some(Response {
            user: UserInfo {
                username: ANONYMOUS_USER.to_string(),
                uid: String::new(),
                groups: vec![ALL_UNAUTHENTICATED.to_string()],
                extra: HashMap::new(),
            },
        }))
    }
}

/// `unionAuthRequestHandler` (`union.New` / `union.NewFailOnError`).
pub struct UnionAuthenticator {
    handlers: Vec<Arc<dyn RequestAuthenticator>>,
    fail_on_error: bool,
}

impl UnionAuthenticator {
    /// `union.New`: a single handler is returned as is.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(handlers: Vec<Arc<dyn RequestAuthenticator>>) -> Arc<dyn RequestAuthenticator> {
        Self::build(handlers, false)
    }

    /// `union.NewFailOnError`.
    pub fn new_fail_on_error(
        handlers: Vec<Arc<dyn RequestAuthenticator>>,
    ) -> Arc<dyn RequestAuthenticator> {
        Self::build(handlers, true)
    }

    fn build(
        mut handlers: Vec<Arc<dyn RequestAuthenticator>>,
        fail_on_error: bool,
    ) -> Arc<dyn RequestAuthenticator> {
        if handlers.len() == 1 {
            return handlers.remove(0);
        }
        Arc::new(Self {
            handlers,
            fail_on_error,
        })
    }
}

#[async_trait]
impl RequestAuthenticator for UnionAuthenticator {
    async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
        let mut errlist: Vec<String> = vec![];
        for h in &self.handlers {
            match h.authenticate_request(req).await {
                Err(e) => {
                    if self.fail_on_error {
                        return Err(e);
                    }
                    errlist.push(e);
                }
                Ok(Some(r)) => return Ok(Some(r)),
                Ok(None) => {}
            }
        }
        match aggregate(&errlist) {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }
}

/// `group.AuthenticatedGroupAdder`.
pub struct AuthenticatedGroupAdder {
    auth: Arc<dyn RequestAuthenticator>,
}

impl AuthenticatedGroupAdder {
    pub fn new(auth: Arc<dyn RequestAuthenticator>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl RequestAuthenticator for AuthenticatedGroupAdder {
    async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
        let Some(mut r) = self.auth.authenticate_request(req).await? else {
            return Ok(None);
        };
        if r.user.username == ANONYMOUS_USER {
            return Ok(Some(r));
        }
        if r.user
            .groups
            .iter()
            .any(|g| g == ALL_AUTHENTICATED || g == ALL_UNAUTHENTICATED)
        {
            return Ok(Some(r));
        }
        r.user.groups.push(ALL_AUTHENTICATED.to_string());
        Ok(Some(r))
    }
}

// ----------------------------------------------------------- client CA content

/// The `dynamiccertificates.CAContentProvider` the x509 authenticators verify
/// against: a PEM CA bundle that can be replaced while the server runs.
pub struct CaContent {
    name: String,
    state: RwLock<Option<Arc<CaState>>>,
}

struct CaState {
    pem: Vec<u8>,
    verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
}

impl CaContent {
    /// An empty provider: `VerifyOptions` reports "no options" until loaded.
    pub fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_string(),
            state: RwLock::new(None),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// `newCABundleAndVerifier` (dynamic_cafile_content.go): parse a PEM bundle
    /// and make it the current one. Returns whether it differs from the
    /// previous bundle (`hasCAChanged`).
    pub fn set_pem(&self, pem: &[u8]) -> Result<bool, String> {
        if self
            .state
            .read()
            .unwrap()
            .as_ref()
            .is_some_and(|s| s.pem == pem)
        {
            return Ok(false);
        }
        let mut roots = rustls::RootCertStore::empty();
        let mut n = 0;
        for cert in rustls_pemfile::certs(&mut &pem[..]) {
            let cert = cert.map_err(|e| format!("unable to parse bytes as PEM block: {e}"))?;
            roots
                .add(cert)
                .map_err(|e| format!("unable to add certificate to the pool: {e}"))?;
            n += 1;
        }
        if n == 0 {
            return Err(format!("missing content for CA bundle \"{}\"", self.name));
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        )
        .build()
        .map_err(|e| format!("unable to build the client certificate verifier: {e}"))?;
        *self.state.write().unwrap() = Some(Arc::new(CaState {
            pem: pem.to_vec(),
            verifier,
        }));
        Ok(true)
    }

    /// `NewDynamicCAContentFromFile`: loads `path` now; an unreadable or
    /// empty bundle is an error.
    pub fn from_file(name: &str, path: &str) -> Result<Arc<Self>, String> {
        let c = Self::new(name);
        let pem = std::fs::read(path).map_err(|e| format!("open {path}: {e}"))?;
        c.set_pem(&pem)?;
        Ok(c)
    }

    /// `DynamicFileCAContent.Run` (dynamic_cafile_content.go): re-read `path`
    /// every `interval`; a bad file keeps the previous bundle. The task ends
    /// when the provider is dropped.
    pub fn run_file_refresh(self: &Arc<Self>, path: String, interval: Duration) {
        let weak = Arc::downgrade(self);
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(this) = weak.upgrade() else { return };
                match std::fs::read(&path) {
                    Ok(pem) => {
                        if let Err(e) = this.set_pem(&pem) {
                            tracing::error!("Failed to reload CA bundle {path}: {e}");
                        }
                    }
                    Err(e) => tracing::error!("Failed to read CA bundle {path}: {e}"),
                }
            }
        });
    }

    /// The current bundle, if any (`CurrentCABundleContent`).
    pub fn current_pem(&self) -> Option<Vec<u8>> {
        self.state.read().unwrap().as_ref().map(|s| s.pem.clone())
    }

    /// The verifier of the current bundle, for the TLS layer.
    pub(crate) fn verifier(&self) -> Option<Arc<dyn rustls::server::danger::ClientCertVerifier>> {
        self.state
            .read()
            .unwrap()
            .as_ref()
            .map(|s| s.verifier.clone())
    }

    /// `VerifyOptions` + `Certificate.Verify`: `None` when there is no bundle
    /// (the authenticator then does not authenticate), else whether the chain
    /// (leaf first) verifies for client auth.
    pub fn verify_client_chain(&self, chain: &[Vec<u8>]) -> Option<Result<(), String>> {
        let state = self.state.read().unwrap().clone()?;
        let Some(leaf) = chain.first() else {
            return Some(Err("no certificate presented".into()));
        };
        let leaf = rustls::pki_types::CertificateDer::from(leaf.clone());
        let intermediates: Vec<_> = chain[1..]
            .iter()
            .map(|c| rustls::pki_types::CertificateDer::from(c.clone()))
            .collect();
        Some(
            state
                .verifier
                .verify_client_cert(&leaf, &intermediates, rustls::pki_types::UnixTime::now())
                .map(|_| ())
                .map_err(|e| e.to_string()),
        )
    }
}

// ----------------------------------------------------------------------- x509

/// `x509.UID` OID (`apimachinery/pkg/apis/asn1/oid.go`): 1.3.6.1.4.1.57683.2.
const X509_UID_OID: &str = "1.3.6.1.4.1.57683.2";

/// `columnSeparatedHex` (x509.go:92-102).
fn column_separated_hex(d: &[u8]) -> String {
    d.iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// `certificateIdentifier` (x509.go:104-111).
fn certificate_identifier(der: &[u8]) -> String {
    use x509_parser::prelude::*;
    let Ok((_, c)) = parse_x509_certificate(der) else {
        return "SN=<unparsable>".to_string();
    };
    let mut skid = vec![];
    let mut akid = vec![];
    for e in c.extensions() {
        match e.parsed_extension() {
            ParsedExtension::SubjectKeyIdentifier(k) => skid = k.0.to_vec(),
            ParsedExtension::AuthorityKeyIdentifier(a) => {
                if let Some(k) = &a.key_identifier {
                    akid = k.0.to_vec()
                }
            }
            _ => {}
        }
    }
    format!(
        "SN={}, SKID={}, AKID={}",
        c.tbs_certificate.serial,
        column_separated_hex(&skid),
        column_separated_hex(&akid)
    )
}

/// `x509.UserConversion` = `CommonNameUserConversion` (x509.go:279-302),
/// with `parseUIDFromCert` (gate `AllowParsingUserUIDFromCertAuth`, beta and on
/// by default since 1.33).
pub fn common_name_user_conversion(chain: &[Vec<u8>]) -> AuthResult {
    use x509_parser::prelude::*;
    let Some(leaf) = chain.first() else {
        return Ok(None);
    };
    let (_, cert) = parse_x509_certificate(leaf).map_err(|e| format!("x509: {e}"))?;
    let subject = cert.subject();
    let cn = subject
        .iter_common_name()
        .next()
        .and_then(|a| a.as_str().ok())
        .unwrap_or("")
        .to_string();
    if cn.is_empty() {
        return Ok(None);
    }
    let fp: String = Sha256::digest(leaf)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let id = format!("X509SHA256={fp}");

    // parseUIDFromCert (x509.go:305-330)
    let mut uids = vec![];
    for rdn in subject.iter() {
        for atv in rdn.iter() {
            if atv.attr_type().to_id_string() == X509_UID_OID {
                match atv.as_str() {
                    Ok(s) => uids.push(s.to_string()),
                    Err(_) => return Err("unable to parse UID into a string".into()),
                }
            }
        }
    }
    let uid = match uids.len() {
        0 => String::new(),
        1 if uids[0].is_empty() => return Err("UID cannot be an empty string".into()),
        1 => uids.remove(0),
        _ => return Err(format!("expected 1 UID, but found multiple: {uids:?}")),
    };
    let groups = subject
        .iter_organization()
        .filter_map(|a| a.as_str().ok().map(String::from))
        .collect();
    Ok(Some(Response {
        user: UserInfo {
            username: cn,
            uid,
            groups,
            extra: HashMap::from([(CREDENTIAL_ID_KEY.to_string(), vec![id])]),
        },
    }))
}

/// `x509.Verifier` / `x509.NewDynamic` / `NewDynamicCAVerifier`.
pub struct X509Authenticator {
    ca: Arc<CaContent>,
    /// `allowedCommonNames`; empty means no restriction.
    allowed_common_names: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    /// `Some` wraps the request-header authenticator (the `Verifier` case).
    inner: Option<Arc<dyn RequestAuthenticator>>,
}

impl X509Authenticator {
    /// `x509.NewDynamic(verifyOptionsFn, CommonNameUserConversion)`.
    pub fn new_dynamic(ca: Arc<CaContent>) -> Self {
        Self {
            ca,
            allowed_common_names: Arc::new(Vec::new),
            inner: None,
        }
    }

    /// `x509.NewDynamicCAVerifier(verifyOptionsFn, auth, allowedCommonNames)`.
    pub fn new_verifier(
        ca: Arc<CaContent>,
        inner: Arc<dyn RequestAuthenticator>,
        allowed_common_names: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    ) -> Self {
        Self {
            ca,
            allowed_common_names,
            inner: Some(inner),
        }
    }

    /// `Verifier.verifySubject` (x509.go:257-269).
    fn verify_subject(&self, leaf: &[u8]) -> Result<(), String> {
        use x509_parser::prelude::*;
        let allowed = (self.allowed_common_names)();
        if allowed.is_empty() {
            return Ok(());
        }
        let cn = parse_x509_certificate(leaf)
            .ok()
            .and_then(|(_, c)| {
                c.subject()
                    .iter_common_name()
                    .next()
                    .and_then(|a| a.as_str().ok().map(String::from))
            })
            .unwrap_or_default();
        if allowed.contains(&cn) {
            return Ok(());
        }
        Err(format!(
            "x509: subject with cn={cn} is not in the allowed list"
        ))
    }
}

#[async_trait]
impl RequestAuthenticator for X509Authenticator {
    async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
        if req.peer_certs.is_empty() {
            return Ok(None);
        }
        // if there are intentionally no verify options, then we cannot authenticate this request
        let Some(verdict) = self.ca.verify_client_chain(req.peer_certs) else {
            return Ok(None);
        };
        match &self.inner {
            None => {
                verdict.map_err(|e| {
                    format!(
                        "verifying certificate {} failed: {e}",
                        certificate_identifier(&req.peer_certs[0])
                    )
                })?;
                common_name_user_conversion(req.peer_certs)
            }
            Some(inner) => {
                verdict?;
                self.verify_subject(&req.peer_certs[0])?;
                inner.authenticate_request(req).await
            }
        }
    }
}

// -------------------------------------------------------------- request header

/// The values of the dynamic `StringSliceProvider`s of the request-header
/// authenticator (`headerrequest.StringSliceProvider`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RequestHeaderSettings {
    pub username_headers: Vec<String>,
    pub uid_headers: Vec<String>,
    pub group_headers: Vec<String>,
    pub extra_header_prefixes: Vec<String>,
    pub allowed_client_names: Vec<String>,
}

/// `headerrequest.requestHeaderAuthRequestHandler`.
pub struct RequestHeaderAuthenticator {
    settings: Arc<RwLock<RequestHeaderSettings>>,
}

impl RequestHeaderAuthenticator {
    pub fn new(settings: Arc<RwLock<RequestHeaderSettings>>) -> Self {
        Self { settings }
    }
}

fn header_value(h: &HeaderMap, names: &[String]) -> String {
    for n in names {
        if let Some(v) = h.get(n.as_str()) {
            let v = header_str(v);
            if !v.is_empty() {
                return v;
            }
        }
    }
    String::new()
}

fn all_header_values(h: &HeaderMap, names: &[String]) -> Vec<String> {
    let mut ret = vec![];
    for n in names {
        for v in h.get_all(n.as_str()) {
            let v = header_str(v);
            if !v.is_empty() {
                ret.push(v);
            }
        }
    }
    ret
}

fn has_prefix_ignore_case(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len()
        && s.is_char_boundary(prefix.len())
        && s[..prefix.len()].eq_ignore_ascii_case(prefix)
}

/// `unescapeExtraKey` (`url.PathUnescape`; the raw key when malformed).
fn unescape_extra_key(encoded: &str) -> String {
    let b = encoded.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = encoded
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok());
            match hex {
                Some(v) => {
                    out.push(v);
                    i += 3;
                }
                None => return encoded.to_string(),
            }
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| encoded.to_string())
}

fn new_extra(h: &HeaderMap, prefixes: &[String]) -> HashMap<String, Vec<String>> {
    let mut ret: HashMap<String, Vec<String>> = HashMap::new();
    // iterate over prefixes first in order to have proper ordering inside the value slices
    for prefix in prefixes {
        for (name, value) in h.iter() {
            if !has_prefix_ignore_case(name.as_str(), prefix) {
                continue;
            }
            let key = unescape_extra_key(&name.as_str()[prefix.len()..].to_lowercase());
            ret.entry(key).or_default().push(header_str(value));
        }
    }
    ret
}

#[async_trait]
impl RequestAuthenticator for RequestHeaderAuthenticator {
    async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
        let s = self.settings.read().unwrap().clone();
        let name = header_value(req.headers, &s.username_headers);
        if name.is_empty() {
            return Ok(None);
        }
        let uid = header_value(req.headers, &s.uid_headers);
        let groups = all_header_values(req.headers, &s.group_headers);
        let extra = new_extra(req.headers, &s.extra_header_prefixes);
        // clear headers used for authentication
        clear_authentication_headers(
            req.headers,
            &s.username_headers,
            &s.uid_headers,
            &s.group_headers,
            &s.extra_header_prefixes,
        );
        Ok(Some(Response {
            user: UserInfo {
                username: name,
                uid,
                groups,
                extra,
            },
        }))
    }
}

/// `headerrequest.ClearAuthenticationHeaders` (requestheader.go:143-160).
pub fn clear_authentication_headers(
    h: &mut HeaderMap,
    name_headers: &[String],
    uid_headers: &[String],
    group_headers: &[String],
    extra_header_prefixes: &[String],
) {
    for n in name_headers.iter().chain(uid_headers).chain(group_headers) {
        if let Ok(name) = HeaderName::from_bytes(n.as_bytes()) {
            h.remove(name);
        }
    }
    for prefix in extra_header_prefixes {
        let doomed: Vec<HeaderName> = h
            .keys()
            .filter(|k| has_prefix_ignore_case(k.as_str(), prefix))
            .cloned()
            .collect();
        for k in doomed {
            h.remove(k);
        }
    }
}

// ---------------------------------------------------------------- token cache

/// `errAuthnCrash` (`apierrors.NewInternalError`).
const ERR_AUTHN_CRASH: &str = "Internal error occurred: authentication failed unexpectedly";
/// `sharedLookupTimeout`.
const SHARED_LOOKUP_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound on entries: 32 stripes of the simple cache's 4096
/// (`newStripedCache(32, ...)`).
const MAX_CACHE_ENTRIES: usize = 32 * 4096;

/// `cacheRecord`.
struct CacheRecord {
    result: AuthResult,
}

type Lookup = Shared<BoxFuture<'static, Arc<CacheRecord>>>;

struct CacheInner {
    authenticator: Arc<dyn TokenAuthenticator>,
    cache_errs: bool,
    success_ttl: Duration,
    failure_ttl: Duration,
    /// HMAC-SHA256 key: "to prevent precomputation and length extension attacks".
    key: [u8; 32],
    cache: Mutex<HashMap<String, (Instant, Arc<CacheRecord>)>>,
    /// `singleflight.Group`.
    inflight: Mutex<HashMap<String, Lookup>>,
}

impl CacheInner {
    /// `keyFunc` (cached_token_authenticator.go:~ 245-270), with no audiences.
    fn key_func(&self, token: &str) -> String {
        let mut h = <Hmac<Sha256> as Mac>::new_from_slice(&self.key).expect("hmac key");
        h.update(&(token.len() as u32).to_be_bytes());
        h.update(token.as_bytes());
        h.update(&0u32.to_be_bytes()); // len(auds)
        h.finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn get(&self, key: &str) -> Option<Arc<CacheRecord>> {
        let mut c = self.cache.lock().unwrap();
        match c.get(key) {
            Some((exp, r)) if *exp > Instant::now() => Some(r.clone()),
            Some(_) => {
                c.remove(key);
                None
            }
            None => None,
        }
    }

    fn set(&self, key: String, r: Arc<CacheRecord>, ttl: Duration) {
        let mut c = self.cache.lock().unwrap();
        if c.len() >= MAX_CACHE_ENTRIES {
            let now = Instant::now();
            c.retain(|_, (exp, _)| *exp > now);
            if c.len() >= MAX_CACHE_ENTRIES {
                if let Some(k) = c.keys().next().cloned() {
                    c.remove(&k);
                }
            }
        }
        c.insert(key, (Instant::now() + ttl, r));
    }
}

/// `cachedTokenAuthenticator` (`cache.New`).
pub struct CachedTokenAuthenticator {
    inner: Arc<CacheInner>,
}

impl CachedTokenAuthenticator {
    /// `cache.New(authenticator, cacheErrs, successTTL, failureTTL)`: a TTL of
    /// 0 bypasses the cache.
    pub fn new(
        inner: Arc<dyn TokenAuthenticator>,
        cache_errs: bool,
        success_ttl: Duration,
        failure_ttl: Duration,
    ) -> Self {
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        Self {
            inner: Arc::new(CacheInner {
                authenticator: inner,
                cache_errs,
                success_ttl,
                failure_ttl,
                key,
                cache: Mutex::new(HashMap::new()),
                inflight: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// `doAuthenticateToken` (cached_token_authenticator.go:~ 130-215).
    async fn do_authenticate_token(&self, token: &str) -> Arc<CacheRecord> {
        let key = self.inner.key_func(token);
        if let Some(r) = self.inner.get(&key) {
            return r;
        }
        let lookup = {
            let mut g = self.inner.inflight.lock().unwrap();
            match g.get(&key) {
                Some(f) => f.clone(),
                None => {
                    let inner = self.inner.clone();
                    let k = key.clone();
                    let token = token.to_string();
                    let f: Lookup = async move {
                        let rec = lookup(&inner, &k, &token).await;
                        inner.inflight.lock().unwrap().remove(&k);
                        rec
                    }
                    .boxed()
                    .shared();
                    g.insert(key.clone(), f.clone());
                    f
                }
            }
        };
        lookup.await
    }
}

/// The `DoChan` body: re-check the cache, call the wrapped authenticator
/// (crashes become `errAuthnCrash`), and store the record.
async fn lookup(inner: &Arc<CacheInner>, key: &str, token: &str) -> Arc<CacheRecord> {
    // Check again for a cached record. We may have raced with a fetch.
    if let Some(r) = inner.get(key) {
        return r;
    }
    let call = std::panic::AssertUnwindSafe(tokio::time::timeout(
        SHARED_LOOKUP_TIMEOUT,
        inner.authenticator.authenticate_token(token),
    ))
    .catch_unwind();
    let result = match call.await {
        Ok(Ok(r)) => r,
        Ok(Err(_)) => Err("context deadline exceeded".to_string()),
        Err(panic) => {
            tracing::error!("{:?}", panic_message(&panic));
            Err(ERR_AUTHN_CRASH.to_string())
        }
    };
    let record = Arc::new(CacheRecord { result });
    if !inner.cache_errs && record.result.is_err() {
        return record;
    }
    let ok = matches!(record.result, Ok(Some(_)));
    if ok && inner.success_ttl > Duration::ZERO {
        inner.set(key.to_string(), record.clone(), inner.success_ttl);
    } else if !ok && inner.failure_ttl > Duration::ZERO {
        inner.set(key.to_string(), record.clone(), inner.failure_ttl);
    }
    record
}

fn panic_message(p: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic".to_string()
    }
}

#[async_trait]
impl TokenAuthenticator for CachedTokenAuthenticator {
    async fn authenticate_token(&self, token: &str) -> AuthResult {
        let record = self.do_authenticate_token(token).await;
        match &record.result {
            Ok(Some(r)) => Ok(Some(r.clone())),
            Ok(None) => Ok(None),
            Err(e) => Err(e.clone()),
        }
    }
}

// ------------------------------------------------------------ webhook (review)

/// `webhook.WebhookTokenAuthenticator` over `TokenReview`.
pub struct WebhookTokenAuthenticator {
    client: Arc<dyn ReviewClient>,
    retry_backoff: Backoff,
    request_timeout: Duration,
}

impl WebhookTokenAuthenticator {
    /// `webhook.NewFromInterface`.
    pub fn new(
        client: Arc<dyn ReviewClient>,
        retry_backoff: Backoff,
        request_timeout: Duration,
    ) -> Self {
        Self {
            client,
            retry_backoff,
            request_timeout,
        }
    }
}

#[async_trait]
impl TokenAuthenticator for WebhookTokenAuthenticator {
    async fn authenticate_token(&self, token: &str) -> AuthResult {
        let review = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "spec": {"token": token},
        });
        let result: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
        let last_err: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let call = with_exponential_backoff(
            self.retry_backoff.clone(),
            || {
                let client = self.client.clone();
                let review = review.clone();
                let result = result.clone();
                async move {
                    let r = client
                        .create("/apis/authentication.k8s.io/v1/tokenreviews", &review)
                        .await?;
                    *result.lock().unwrap() = Some(r);
                    Ok(())
                }
            },
            default_should_retry,
        );
        // set a hard timeout if it was defined
        let outcome =
            if self.request_timeout > Duration::ZERO {
                match tokio::time::timeout(self.request_timeout, call).await {
                    Ok(r) => r,
                    Err(_) => Err(last_err.lock().unwrap().clone().unwrap_or_else(|| {
                        "webhook call failed: context deadline exceeded".into()
                    })),
                }
            } else {
                call.await
            };
        if let Err(e) = outcome {
            // An error here indicates bad configuration or an outage. Log for debugging.
            tracing::error!("Failed to make webhook authenticator request: {e}");
            return Err(e);
        }
        let result = result.lock().unwrap().take().unwrap_or_default();
        let status = &result["status"];
        if !status["authenticated"].as_bool().unwrap_or(false) {
            return match status["error"].as_str() {
                Some(e) if !e.is_empty() => Err(e.to_string()),
                _ => Ok(None),
            };
        }
        let u = &status["user"];
        let strings = |v: &serde_json::Value| -> Vec<String> {
            v.as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|s| s.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        };
        let extra = u["extra"]
            .as_object()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), strings(v))).collect())
            .unwrap_or_default();
        Ok(Some(Response {
            user: UserInfo {
                username: u["username"].as_str().unwrap_or("").to_string(),
                uid: u["uid"].as_str().unwrap_or("").to_string(),
                groups: strings(&u["groups"]),
                extra,
            },
        }))
    }
}

// ------------------------------------------------------------------ factory

/// `apiserver.AnonymousAuthConfig`.
#[derive(Debug, Clone, Default)]
pub struct AnonymousAuthConfig {
    pub enabled: bool,
    /// `Conditions[].Path`.
    pub condition_paths: Vec<String>,
}

/// `authenticatorfactory.RequestHeaderConfig`.
#[derive(Clone)]
pub struct RequestHeaderConfig {
    pub ca: Arc<CaContent>,
    pub settings: Arc<RwLock<RequestHeaderSettings>>,
}

/// `authenticatorfactory.DelegatingAuthenticatorConfig`.
pub struct DelegatingAuthenticatorConfig {
    pub anonymous: Option<AnonymousAuthConfig>,
    /// `TokenAccessReviewClient`; `None` ignores every token.
    pub token_access_review_client: Option<Arc<dyn ReviewClient>>,
    pub token_access_review_timeout: Duration,
    pub webhook_retry_backoff: Option<Backoff>,
    pub cache_ttl: Duration,
    pub client_certificate_ca: Option<Arc<CaContent>>,
    pub request_header_config: Option<RequestHeaderConfig>,
}

impl DelegatingAuthenticatorConfig {
    /// `DelegatingAuthenticatorConfig.New` (delegating.go:61-131).
    #[allow(clippy::new_ret_no_self)] // `DelegatingAuthenticatorConfig.New`
    pub fn new(&self) -> Result<Arc<dyn RequestAuthenticator>, String> {
        let mut authenticators: Vec<Arc<dyn RequestAuthenticator>> = vec![];

        // front-proxy first, then remote
        if let Some(rh) = &self.request_header_config {
            // NewDynamicVerifyOptionsSecure: a CA verifier over the header authenticator
            let settings = rh.settings.clone();
            let header = Arc::new(RequestHeaderAuthenticator::new(rh.settings.clone()));
            authenticators.push(Arc::new(X509Authenticator::new_verifier(
                rh.ca.clone(),
                header,
                Arc::new(move || settings.read().unwrap().allowed_client_names.clone()),
            )));
        }

        // x509 client cert auth
        if let Some(ca) = &self.client_certificate_ca {
            authenticators.push(Arc::new(X509Authenticator::new_dynamic(ca.clone())));
        }

        if let Some(client) = &self.token_access_review_client {
            let Some(backoff) = &self.webhook_retry_backoff else {
                return Err("retry backoff parameters for delegating authentication webhook has not been specified".into());
            };
            let token_auth = Arc::new(WebhookTokenAuthenticator::new(
                client.clone(),
                backoff.clone(),
                self.token_access_review_timeout,
            ));
            let caching: Arc<dyn TokenAuthenticator> = Arc::new(CachedTokenAuthenticator::new(
                token_auth,
                false,
                self.cache_ttl,
                self.cache_ttl,
            ));
            authenticators.push(Arc::new(BearerTokenAuthenticator::new(caching.clone())));
            authenticators.push(Arc::new(WebsocketProtocolAuthenticator::new(caching)));
        }

        let anonymous = self.anonymous.as_ref().filter(|a| a.enabled).map(
            |a| -> Arc<dyn RequestAuthenticator> {
                Arc::new(AnonymousAuthenticator {
                    allowed_paths: a.condition_paths.clone(),
                })
            },
        );

        if authenticators.is_empty() {
            return anonymous.ok_or_else(|| "no authentication method configured".to_string());
        }

        let authenticator: Arc<dyn RequestAuthenticator> = Arc::new(AuthenticatedGroupAdder::new(
            UnionAuthenticator::new(authenticators),
        ));
        match anonymous {
            Some(a) => Ok(UnionAuthenticator::new_fail_on_error(vec![
                authenticator,
                a,
            ])),
            None => Ok(authenticator),
        }
    }
}
