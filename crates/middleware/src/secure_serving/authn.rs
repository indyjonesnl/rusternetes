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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secure_serving::webhook::ReviewError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn user(name: &str) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: vec![],
            extra: HashMap::new(),
        }
    }

    /// `authenticator.TokenFunc`.
    struct TokenFn<F>(F);
    #[async_trait]
    impl<F> TokenAuthenticator for TokenFn<F>
    where
        F: Fn(&str) -> AuthResult + Send + Sync,
    {
        async fn authenticate_token(&self, token: &str) -> AuthResult {
            (self.0)(token)
        }
    }

    /// `authenticator.RequestFunc`.
    struct ReqFn<F>(F);
    #[async_trait]
    impl<F> RequestAuthenticator for ReqFn<F>
    where
        F: Fn(&mut AuthRequest<'_>) -> AuthResult + Send + Sync,
    {
        async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
            (self.0)(req)
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    async fn run(a: &dyn RequestAuthenticator, h: &mut HeaderMap) -> AuthResult {
        a.authenticate_request(&mut AuthRequest {
            headers: h,
            path: "/metrics",
            peer_certs: &[],
        })
        .await
    }

    fn ok_user(name: &'static str) -> Arc<dyn TokenAuthenticator> {
        Arc::new(TokenFn(move |_t: &str| {
            Ok(Some(Response { user: user(name) }))
        }))
    }

    // ---- bearertoken_test.go

    #[tokio::test]
    async fn bearer_token_authenticates() {
        let auth = BearerTokenAuthenticator::new(Arc::new(TokenFn(|t: &str| {
            assert_eq!(t, "token");
            Ok(Some(Response { user: user("user") }))
        })));
        let mut h = headers(&[("authorization", "Bearer token")]);
        let r = run(&auth, &mut h).await.unwrap();
        assert_eq!(r.unwrap().user.username, "user");
        // bearertoken.go: "remove the bearer token so that no one is ever tempted to use it"
        assert!(h.get("authorization").is_none());
    }

    // TestAuthenticateRequestIncludingValueAfterToken
    #[tokio::test]
    async fn bearer_token_ignores_values_after_the_token() {
        for v in ["Bearer token a", "Bearer token a b c", "Bearer token   a"] {
            let auth = BearerTokenAuthenticator::new(Arc::new(TokenFn(|t: &str| {
                assert_eq!(t, "token");
                Ok(Some(Response { user: user("user") }))
            })));
            let mut h = headers(&[("authorization", v)]);
            assert!(run(&auth, &mut h).await.unwrap().is_some(), "{v}");
        }
    }

    // TestAuthenticateRequestTokenInvalid: !ok without an error becomes `invalid bearer token`.
    #[tokio::test]
    async fn bearer_token_invalid_gets_the_default_error() {
        let auth = BearerTokenAuthenticator::new(Arc::new(TokenFn(|_t: &str| Ok(None))));
        let mut h = headers(&[("authorization", "Bearer token")]);
        assert_eq!(
            run(&auth, &mut h).await.unwrap_err(),
            "invalid bearer token"
        );
        // not removed on failure
        assert!(h.get("authorization").is_some());
    }

    // TestAuthenticateRequestTokenInvalidCustomError / TokenError
    #[tokio::test]
    async fn bearer_token_keeps_a_custom_error() {
        let auth =
            BearerTokenAuthenticator::new(Arc::new(TokenFn(|_t: &str| Err("custom".to_string()))));
        let mut h = headers(&[("authorization", "Bearer token")]);
        assert_eq!(run(&auth, &mut h).await.unwrap_err(), "custom");
    }

    // TestAuthenticateRequestBadValue
    #[tokio::test]
    async fn bearer_token_bad_values_are_not_authenticated() {
        for v in [
            None,
            Some("Bearer"),
            Some("bear token"),
            Some("Bearer: token"),
        ] {
            let auth = BearerTokenAuthenticator::new(Arc::new(TokenFn(|_t: &str| {
                panic!("authentication should not have been called")
            })));
            let mut h = match v {
                Some(v) => headers(&[("authorization", v)]),
                None => HeaderMap::new(),
            };
            assert!(run(&auth, &mut h).await.unwrap().is_none(), "{v:?}");
        }
    }

    // bearertoken.go: the scheme is matched case-insensitively.
    #[tokio::test]
    async fn bearer_scheme_is_case_insensitive() {
        let auth = BearerTokenAuthenticator::new(ok_user("u"));
        let mut h = headers(&[("authorization", "bEaReR token")]);
        assert!(run(&auth, &mut h).await.unwrap().is_some());
    }

    // ---- websocket protocol_test.go

    fn ws(protocols: &str) -> HeaderMap {
        headers(&[
            ("connection", "upgrade"),
            ("upgrade", "websocket"),
            ("sec-websocket-protocol", protocols),
        ])
    }

    #[tokio::test]
    async fn websocket_token_protocol_authenticates_and_is_removed() {
        let auth = WebsocketProtocolAuthenticator::new(Arc::new(TokenFn(|t: &str| {
            assert_eq!(t, "token");
            Ok(Some(Response { user: user("user") }))
        })));
        let mut h = ws("base64url.bearer.authorization.k8s.io.dG9rZW4,dummy");
        assert!(run(&auth, &mut h).await.unwrap().is_some());
        assert_eq!(h.get("sec-websocket-protocol").unwrap(), "dummy");
    }

    // TestAuthenticateRequestMultipleConnectionHeaders
    #[tokio::test]
    async fn websocket_accepts_upgrade_among_several_connection_headers() {
        let auth = WebsocketProtocolAuthenticator::new(ok_user("user"));
        let mut h = headers(&[
            ("connection", "not"),
            ("connection", "upgrade"),
            ("upgrade", "websocket"),
            (
                "sec-websocket-protocol",
                "base64url.bearer.authorization.k8s.io.dG9rZW4,dummy",
            ),
        ]);
        assert!(run(&auth, &mut h).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn websocket_rejects_malformed_protocols() {
        let auth = WebsocketProtocolAuthenticator::new(ok_user("user"));
        let cases = [
            (
                "base64url.bearer.authorization.k8s.io.dG9rZW4,base64url.bearer.authorization.k8s.io.dG9rZW4",
                "multiple base64.bearer.authorization tokens specified",
            ),
            (
                "base64url.bearer.authorization.k8s.io.!!!,dummy",
                "invalid base64.bearer.authorization token encoding",
            ),
            (
                "base64url.bearer.authorization.k8s.io.dG9rZW4",
                "missing additional subprotocol",
            ),
        ];
        for (protocols, want) in cases {
            let mut h = ws(protocols);
            assert_eq!(run(&auth, &mut h).await.unwrap_err(), want, "{protocols}");
        }
        // not a websocket request: ignored
        let mut h = headers(&[(
            "sec-websocket-protocol",
            "base64url.bearer.authorization.k8s.io.dG9rZW4,dummy",
        )]);
        assert!(run(&auth, &mut h).await.unwrap().is_none());
        // no token protocol: not authenticated, no error
        let mut h = ws("dummy");
        assert!(run(&auth, &mut h).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn websocket_invalid_token_gets_the_default_error() {
        let auth = WebsocketProtocolAuthenticator::new(Arc::new(TokenFn(|_t: &str| Ok(None))));
        let mut h = ws("base64url.bearer.authorization.k8s.io.dG9rZW4,dummy");
        assert_eq!(
            run(&auth, &mut h).await.unwrap_err(),
            "invalid bearer token"
        );
    }

    // ---- anonymous_test.go

    #[tokio::test]
    async fn anonymous_authenticates_everything_unrestricted() {
        let a = AnonymousAuthenticator {
            allowed_paths: vec![],
        };
        let r = run(&a, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(r.user.username, ANONYMOUS_USER);
        assert_eq!(r.user.groups, vec![ALL_UNAUTHENTICATED.to_string()]);
    }

    #[tokio::test]
    async fn anonymous_restricted_to_condition_paths() {
        let a = AnonymousAuthenticator {
            allowed_paths: vec!["/healthz".to_string()],
        };
        let mut h = HeaderMap::new();
        let mut req = AuthRequest {
            headers: &mut h,
            path: "/healthz",
            peer_certs: &[],
        };
        assert!(a.authenticate_request(&mut req).await.unwrap().is_some());
        req.path = "/metrics";
        assert!(a.authenticate_request(&mut req).await.unwrap().is_none());
    }

    // ---- union.go

    #[tokio::test]
    async fn union_returns_the_first_success() {
        let u = UnionAuthenticator::new(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Ok(None))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
                Ok(Some(Response { user: user("b") }))
            })),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| panic!("not reached"))),
        ]);
        let r = run(&*u, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(r.user.username, "b");
    }

    // union.go:55-62: errors are collected and the chain continues...
    #[tokio::test]
    async fn union_continues_past_errors_and_aggregates_them() {
        let u = UnionAuthenticator::new(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Err("e1".to_string()))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Err("e2".to_string()))),
        ]);
        let err = run(&*u, &mut HeaderMap::new()).await.unwrap_err();
        assert_eq!(err, "[e1, e2]");

        let u = UnionAuthenticator::new(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Err("e1".to_string()))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
                Ok(Some(Response { user: user("b") }))
            })),
        ]);
        assert!(run(&*u, &mut HeaderMap::new()).await.unwrap().is_some());
    }

    // ...except for NewFailOnError, whose first error short-circuits.
    #[tokio::test]
    async fn union_fail_on_error_stops_at_the_first_error() {
        let u = UnionAuthenticator::new_fail_on_error(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Err("e1".to_string()))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
                Ok(Some(Response { user: user("b") }))
            })),
        ]);
        assert_eq!(run(&*u, &mut HeaderMap::new()).await.unwrap_err(), "e1");
    }

    // utilerrors.NewAggregate(nil) is nil: nothing authenticated, no error.
    #[tokio::test]
    async fn union_with_no_opinion_and_no_errors_is_not_authenticated() {
        let u = UnionAuthenticator::new(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Ok(None))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Ok(None))),
        ]);
        assert!(run(&*u, &mut HeaderMap::new()).await.unwrap().is_none());
    }

    // ---- authenticated_group_adder.go

    #[tokio::test]
    async fn group_adder_adds_system_authenticated() {
        let mut u = user("bob");
        u.groups = vec!["g".into()];
        u.uid = "7".into();
        let inner = ReqFn(move |_r: &mut AuthRequest<'_>| Ok(Some(Response { user: u.clone() })));
        let a = AuthenticatedGroupAdder::new(Arc::new(inner));
        let r = run(&a, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(
            r.user.groups,
            vec!["g".to_string(), ALL_AUTHENTICATED.to_string()]
        );
        assert_eq!(r.user.uid, "7");
    }

    #[tokio::test]
    async fn group_adder_leaves_anonymous_and_already_grouped_users_alone() {
        for (name, groups) in [
            (ANONYMOUS_USER, vec![]),
            ("bob", vec![ALL_AUTHENTICATED.to_string()]),
            ("bob", vec![ALL_UNAUTHENTICATED.to_string()]),
        ] {
            let mut u = user(name);
            u.groups = groups.clone();
            let inner =
                ReqFn(move |_r: &mut AuthRequest<'_>| Ok(Some(Response { user: u.clone() })));
            let a = AuthenticatedGroupAdder::new(Arc::new(inner));
            let r = run(&a, &mut HeaderMap::new()).await.unwrap().unwrap();
            assert_eq!(r.user.groups, groups, "{name}");
        }
    }

    #[tokio::test]
    async fn group_adder_passes_failures_through() {
        let a = AuthenticatedGroupAdder::new(Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
            Err("boom".to_string())
        })));
        assert_eq!(run(&a, &mut HeaderMap::new()).await.unwrap_err(), "boom");
        let a = AuthenticatedGroupAdder::new(Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Ok(None))));
        assert!(run(&a, &mut HeaderMap::new()).await.unwrap().is_none());
    }

    // ---- requestheader_test.go / authentication_test.go TestAuthenticateRequestClearHeaders

    fn rh(
        name: &[&str],
        uid: &[&str],
        group: &[&str],
        extra: &[&str],
    ) -> RequestHeaderAuthenticator {
        let v = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        RequestHeaderAuthenticator::new(Arc::new(RwLock::new(RequestHeaderSettings {
            username_headers: v(name),
            uid_headers: v(uid),
            group_headers: v(group),
            extra_header_prefixes: v(extra),
            allowed_client_names: vec![],
        })))
    }

    #[tokio::test]
    async fn request_header_builds_the_user_and_clears_the_headers() {
        let a = rh(
            &["X-Remote-User"],
            &["X-Remote-Uid"],
            &["X-Remote-Group"],
            &["X-Remote-Extra-"],
        );
        let mut h = headers(&[
            ("x-remote-user", "Bob"),
            ("x-remote-uid", "bobs-uid"),
            ("x-remote-group", "one"),
            ("x-remote-group", "two"),
            ("x-remote-extra-Key1", "alfa"),
            ("x-remote-extra-key1", "bravo"),
            ("x-remote-extra-a%2Fb", "c"),
            ("x-unrelated", "keep"),
        ]);
        let r = run(&a, &mut h).await.unwrap().unwrap();
        assert_eq!(r.user.username, "Bob");
        assert_eq!(r.user.uid, "bobs-uid");
        assert_eq!(r.user.groups, vec!["one".to_string(), "two".to_string()]);
        assert_eq!(
            r.user.extra.get("key1").unwrap(),
            &vec!["alfa".to_string(), "bravo".to_string()]
        );
        // `unescapeExtraKey`
        assert_eq!(r.user.extra.get("a/b").unwrap(), &vec!["c".to_string()]);
        let remaining: Vec<_> = h.keys().map(|k| k.as_str().to_string()).collect();
        assert_eq!(remaining, vec!["x-unrelated".to_string()]);
    }

    #[tokio::test]
    async fn request_header_without_a_user_is_not_authenticated_and_clears_nothing() {
        let a = rh(&["X-Remote-User"], &[], &["X-Remote-Group"], &[]);
        let mut h = headers(&[("x-remote-group", "one")]);
        assert!(run(&a, &mut h).await.unwrap().is_none());
        assert!(h.get("x-remote-group").is_some());
    }

    // "user first match": the first non-empty header wins.
    #[tokio::test]
    async fn request_header_first_non_empty_username_header_wins() {
        let a = rh(&["X-Remote-User", "A-Second"], &[], &[], &[]);
        let mut h = headers(&[("x-remote-user", ""), ("a-second", "Second")]);
        let r = run(&a, &mut h).await.unwrap().unwrap();
        assert_eq!(r.user.username, "Second");
    }

    // "custom config but request contains standard headers": clearing only
    // touches the configured headers (the filter clears the standard ones).
    #[test]
    fn clear_authentication_headers_is_case_insensitive_on_prefixes() {
        let mut h = headers(&[
            ("x-remote-user", "Bob"),
            ("X-Remote-extra-1-key1", "a"),
            ("x-group-remote", "keep"),
        ]);
        clear_authentication_headers(
            &mut h,
            &["x-REMOTE-user".to_string()],
            &[],
            &[],
            &["X-Remote-Extra-1-".to_string()],
        );
        let remaining: Vec<_> = h.keys().map(|k| k.as_str().to_string()).collect();
        assert_eq!(remaining, vec!["x-group-remote".to_string()]);
    }

    // ---- x509_test.go

    struct Pki {
        ca_pem: String,
        ca: rcgen::Certificate,
        ca_key: rcgen::KeyPair,
    }

    fn pki(name: &str) -> Pki {
        let mut p = rcgen::CertificateParams::default();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.distinguished_name = rcgen::DistinguishedName::new();
        p.distinguished_name.push(rcgen::DnType::CommonName, name);
        p.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        let key = rcgen::KeyPair::generate().unwrap();
        let ca = p.self_signed(&key).unwrap();
        Pki {
            ca_pem: ca.pem(),
            ca,
            ca_key: key,
        }
    }

    fn client_cert(pki: &Pki, cn: &str, orgs: &[&str], eku_client: bool) -> Vec<u8> {
        let mut p = rcgen::CertificateParams::default();
        p.distinguished_name = rcgen::DistinguishedName::new();
        if !cn.is_empty() {
            p.distinguished_name.push(rcgen::DnType::CommonName, cn);
        }
        for o in orgs {
            p.distinguished_name
                .push(rcgen::DnType::OrganizationName, *o);
        }
        p.extended_key_usages = vec![if eku_client {
            rcgen::ExtendedKeyUsagePurpose::ClientAuth
        } else {
            rcgen::ExtendedKeyUsagePurpose::ServerAuth
        }];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &pki.ca, &pki.ca_key).unwrap();
        cert.der().to_vec()
    }

    fn ca_content(pki: &Pki) -> Arc<CaContent> {
        let c = CaContent::new("test");
        assert!(c.set_pem(pki.ca_pem.as_bytes()).unwrap());
        c
    }

    async fn run_certs(a: &dyn RequestAuthenticator, chain: &[Vec<u8>]) -> AuthResult {
        let mut h = HeaderMap::new();
        a.authenticate_request(&mut AuthRequest {
            headers: &mut h,
            path: "/metrics",
            peer_certs: chain,
        })
        .await
    }

    // x509.go:279-302 CommonNameUserConversion: CN -> name, O -> groups,
    // credential id is the SHA-256 of the DER.
    #[tokio::test]
    async fn x509_maps_cn_and_organizations() {
        let p = pki("ca");
        let der = client_cert(&p, "system:kube-scheduler", &["g1"], true);
        let a = X509Authenticator::new_dynamic(ca_content(&p));
        let r = run_certs(&a, std::slice::from_ref(&der))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.user.username, "system:kube-scheduler");
        assert_eq!(r.user.groups, vec!["g1".to_string()]); // rcgen keeps one value per DN type
        use sha2::{Digest, Sha256};
        let fp: String = Sha256::digest(&der)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            r.user.extra.get(CREDENTIAL_ID_KEY).unwrap(),
            &vec![format!("X509SHA256={fp}")]
        );
    }

    // x509.go:281-283: an empty CommonName does not authenticate (and is no error).
    #[tokio::test]
    async fn x509_empty_common_name_is_not_authenticated() {
        let p = pki("ca");
        let der = client_cert(&p, "", &["o"], true);
        let a = X509Authenticator::new_dynamic(ca_content(&p));
        assert!(run_certs(&a, &[der]).await.unwrap().is_none());
    }

    // x509.go:231-233, 237-240
    #[tokio::test]
    async fn x509_without_certs_or_without_a_bundle_is_not_authenticated() {
        let p = pki("ca");
        let der = client_cert(&p, "u", &[], true);
        let a = X509Authenticator::new_dynamic(ca_content(&p));
        assert!(run_certs(&a, &[]).await.unwrap().is_none());
        let empty = X509Authenticator::new_dynamic(CaContent::new("empty"));
        assert!(run_certs(&empty, &[der]).await.unwrap().is_none());
    }

    // x509.go:248-250: a chain that does not verify is an error.
    #[tokio::test]
    async fn x509_untrusted_or_wrong_usage_certs_are_errors() {
        let trusted = pki("ca");
        let other = pki("other");
        let a = X509Authenticator::new_dynamic(ca_content(&trusted));
        let der = client_cert(&other, "u", &[], true);
        assert!(run_certs(&a, &[der]).await.is_err());
        // DefaultVerifyOptions requires ExtKeyUsageClientAuth
        let der = client_cert(&trusted, "u", &[], false);
        assert!(run_certs(&a, &[der]).await.is_err());
    }

    // x509.go:257-269 verifySubject (request-header allowed names), wrapping the
    // inner authenticator.
    #[tokio::test]
    async fn x509_verifier_enforces_allowed_common_names_then_delegates() {
        let p = pki("ca");
        let inner: Arc<dyn RequestAuthenticator> = Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
            Ok(Some(Response {
                user: user("from-header"),
            }))
        }));
        let allowed = Arc::new(|| vec!["front-proxy".to_string()]);
        let a = X509Authenticator::new_verifier(ca_content(&p), inner, allowed);
        let ok = client_cert(&p, "front-proxy", &[], true);
        assert_eq!(
            run_certs(&a, &[ok]).await.unwrap().unwrap().user.username,
            "from-header"
        );
        let bad = client_cert(&p, "someone-else", &[], true);
        assert_eq!(
            run_certs(&a, &[bad]).await.unwrap_err(),
            "x509: subject with cn=someone-else is not in the allowed list"
        );
    }

    // the dynamic part: swapping the bundle changes the verdict.
    #[tokio::test]
    async fn ca_content_reload_changes_verification() {
        let a_pki = pki("a");
        let b_pki = pki("b");
        let content = ca_content(&a_pki);
        let a = X509Authenticator::new_dynamic(content.clone());
        let der = client_cert(&b_pki, "u", &[], true);
        assert!(run_certs(&a, std::slice::from_ref(&der)).await.is_err());
        assert!(content.set_pem(b_pki.ca_pem.as_bytes()).unwrap());
        assert!(run_certs(&a, &[der]).await.unwrap().is_some());
        // same bytes again: hasCAChanged is false
        assert!(!content.set_pem(b_pki.ca_pem.as_bytes()).unwrap());
    }

    #[test]
    fn ca_content_rejects_an_empty_or_garbage_bundle() {
        let c = CaContent::new("x");
        assert!(c.set_pem(b"").is_err());
        assert!(c.set_pem(b"not pem").is_err());
        assert!(c.current_pem().is_none());
    }

    #[test]
    fn ca_content_from_file_errors_when_unreadable() {
        assert!(CaContent::from_file("x", "/nonexistent/ca.crt").is_err());
        let dir = tempfile::tempdir().unwrap();
        let p = pki("ca");
        let path = dir.path().join("ca.crt");
        std::fs::write(&path, &p.ca_pem).unwrap();
        let c = CaContent::from_file("x", path.to_str().unwrap()).unwrap();
        assert_eq!(c.current_pem().unwrap(), p.ca_pem.as_bytes());
    }

    // ---- cache: cached_token_authenticator_test.go TestCachedTokenAuthenticator

    struct Counting {
        calls: Mutex<Vec<String>>,
        ok: Mutex<bool>,
        err: Mutex<Option<String>>,
    }
    impl Counting {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(vec![]),
                ok: Mutex::new(false),
                err: Mutex::new(None),
            })
        }
        fn take(&self) -> Vec<String> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }
    }
    #[async_trait]
    impl TokenAuthenticator for Counting {
        async fn authenticate_token(&self, token: &str) -> AuthResult {
            self.calls.lock().unwrap().push(token.to_string());
            if let Some(e) = self.err.lock().unwrap().clone() {
                return Err(e);
            }
            if *self.ok.lock().unwrap() {
                Ok(Some(Response {
                    user: user(&token.replace("token", "")),
                }))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cache_caches_successes_for_the_ttl_but_not_failures_with_zero_failure_ttl() {
        let inner = Counting::new();
        let a = CachedTokenAuthenticator::new(
            inner.clone(),
            true,
            Duration::from_secs(60),
            Duration::ZERO,
        );

        // failures are not cached (failureTTL 0)
        for _ in 0..2 {
            for t in ["bad1", "bad2"] {
                let _ = a.authenticate_token(t).await;
            }
        }
        assert_eq!(inner.take(), vec!["bad1", "bad2", "bad1", "bad2"]);

        *inner.ok.lock().unwrap() = true;
        for t in ["usertoken1", "usertoken2"] {
            assert!(a.authenticate_token(t).await.unwrap().is_some());
        }
        assert_eq!(inner.take(), vec!["usertoken1", "usertoken2"]);

        // backend now fails, cache still answers
        *inner.ok.lock().unwrap() = false;
        for (t, want) in [("usertoken1", "user1"), ("usertoken2", "user2")] {
            assert_eq!(
                a.authenticate_token(t)
                    .await
                    .unwrap()
                    .unwrap()
                    .user
                    .username,
                want
            );
        }
        assert!(inner.take().is_empty());

        tokio::time::advance(Duration::from_secs(120)).await;
        let _ = a.authenticate_token("usertoken1").await;
        assert_eq!(inner.take(), vec!["usertoken1"]);
    }

    // `case !record.ok && a.failureTTL > 0`
    #[tokio::test(start_paused = true)]
    async fn cache_caches_failures_for_the_failure_ttl() {
        let inner = Counting::new();
        let a = CachedTokenAuthenticator::new(
            inner.clone(),
            false,
            Duration::from_secs(60),
            Duration::from_secs(10),
        );
        assert!(a.authenticate_token("bad").await.unwrap().is_none());
        assert!(a.authenticate_token("bad").await.unwrap().is_none());
        assert_eq!(inner.take(), vec!["bad"]);
        tokio::time::advance(Duration::from_secs(11)).await;
        assert!(a.authenticate_token("bad").await.unwrap().is_none());
        assert_eq!(inner.take(), vec!["bad"]);
    }

    // `if !a.cacheErrs && record.err != nil { return }`: errors are not cached.
    #[tokio::test(start_paused = true)]
    async fn cache_does_not_cache_errors_unless_asked() {
        let inner = Counting::new();
        *inner.err.lock().unwrap() = Some("boom".into());
        let a = CachedTokenAuthenticator::new(
            inner.clone(),
            false,
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        assert_eq!(a.authenticate_token("t").await.unwrap_err(), "boom");
        assert_eq!(a.authenticate_token("t").await.unwrap_err(), "boom");
        assert_eq!(inner.take(), vec!["t", "t"]);
    }

    // `A ttl of 0 bypasses the cache.`
    #[tokio::test(start_paused = true)]
    async fn cache_with_zero_ttl_bypasses_the_cache() {
        let inner = Counting::new();
        *inner.ok.lock().unwrap() = true;
        let a = CachedTokenAuthenticator::new(inner.clone(), false, Duration::ZERO, Duration::ZERO);
        a.authenticate_token("u1").await.unwrap();
        a.authenticate_token("u1").await.unwrap();
        assert_eq!(inner.take(), vec!["u1", "u1"]);
    }

    // TestSharedLookup "actually shared": concurrent callers share one lookup.
    #[tokio::test]
    async fn cache_shares_concurrent_lookups_of_the_same_token() {
        struct Slow(AtomicUsize);
        #[async_trait]
        impl TokenAuthenticator for Slow {
            async fn authenticate_token(&self, _t: &str) -> AuthResult {
                self.0.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(Some(Response {
                    user: user("chewbacca"),
                }))
            }
        }
        let slow = Arc::new(Slow(AtomicUsize::new(0)));
        let a = Arc::new(CachedTokenAuthenticator::new(
            slow.clone(),
            true,
            Duration::from_secs(60),
            Duration::ZERO,
        ));
        let mut tasks = vec![];
        for _ in 0..10 {
            let a = a.clone();
            tasks.push(tokio::spawn(async move { a.authenticate_token("").await }));
        }
        for t in tasks {
            assert_eq!(
                t.await.unwrap().unwrap().unwrap().user.username,
                "chewbacca"
            );
        }
        assert!(
            slow.0.load(Ordering::SeqCst) <= 3,
            "{}",
            slow.0.load(Ordering::SeqCst)
        );
    }

    // TestSharedLookup "lookup panics": errAuthnCrash.
    #[tokio::test]
    async fn cache_turns_a_panicking_lookup_into_an_internal_error() {
        let a = CachedTokenAuthenticator::new(
            Arc::new(TokenFn(|_t: &str| -> AuthResult { panic!("uh oh") })),
            true,
            Duration::from_secs(60),
            Duration::ZERO,
        );
        assert_eq!(
            a.authenticate_token("").await.unwrap_err(),
            "Internal error occurred: authentication failed unexpectedly"
        );
    }

    // ---- webhook_v1_test.go TestV1WebhookTokenAuthenticator

    struct FakeReview {
        responses: Mutex<Vec<Result<serde_json::Value, ReviewError>>>,
        requests: Mutex<Vec<(String, serde_json::Value)>>,
    }
    impl FakeReview {
        fn new(responses: Vec<Result<serde_json::Value, ReviewError>>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses),
                requests: Mutex::new(vec![]),
            })
        }
    }
    #[async_trait]
    impl ReviewClient for FakeReview {
        async fn create(
            &self,
            path: &str,
            review: &serde_json::Value,
        ) -> Result<serde_json::Value, ReviewError> {
            self.requests
                .lock()
                .unwrap()
                .push((path.to_string(), review.clone()));
            self.responses.lock().unwrap().remove(0)
        }
    }

    fn fast_backoff() -> Backoff {
        Backoff {
            duration: Duration::from_millis(1),
            factor: 1.0,
            jitter: 0.0,
            steps: 3,
        }
    }

    fn status(v: serde_json::Value) -> Result<serde_json::Value, ReviewError> {
        Ok(
            serde_json::json!({"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status": v}),
        )
    }

    #[tokio::test]
    async fn webhook_posts_a_tokenreview_and_maps_the_user() {
        let c = FakeReview::new(vec![status(serde_json::json!({
            "authenticated": true,
            "user": {"username":"somebody","uid":"u1","groups":["a","b"],"extra":{"k":["v"]}}
        }))]);
        let a = WebhookTokenAuthenticator::new(c.clone(), fast_backoff(), Duration::from_secs(10));
        let r = a.authenticate_token("tok").await.unwrap().unwrap();
        assert_eq!(r.user.username, "somebody");
        assert_eq!(r.user.uid, "u1");
        assert_eq!(r.user.groups, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(r.user.extra.get("k").unwrap(), &vec!["v".to_string()]);
        let reqs = c.requests.lock().unwrap();
        assert_eq!(reqs[0].0, "/apis/authentication.k8s.io/v1/tokenreviews");
        assert_eq!(reqs[0].1["spec"]["token"], "tok");
    }

    #[tokio::test]
    async fn webhook_unauthenticated_is_not_ok_and_status_error_becomes_the_error() {
        let c = FakeReview::new(vec![status(serde_json::json!({"authenticated": false}))]);
        let a = WebhookTokenAuthenticator::new(c, fast_backoff(), Duration::from_secs(10));
        assert!(a.authenticate_token("t").await.unwrap().is_none());

        let c = FakeReview::new(vec![status(
            serde_json::json!({"authenticated": false, "error": "no good"}),
        )]);
        let a = WebhookTokenAuthenticator::new(c, fast_backoff(), Duration::from_secs(10));
        assert_eq!(a.authenticate_token("t").await.unwrap_err(), "no good");
    }

    // TestV1WebhookCacheAndRetry shape: a 500 is retried, a 403 is not.
    #[tokio::test]
    async fn webhook_retries_transient_errors_only() {
        let e500 = ReviewError {
            status_code: 500,
            reason: "InternalError".into(),
            message: "oops".into(),
            connection_reset: false,
            suggests_client_delay: false,
        };
        let c = FakeReview::new(vec![
            Err(e500.clone()),
            status(serde_json::json!({"authenticated": true, "user": {"username":"u"}})),
        ]);
        let a = WebhookTokenAuthenticator::new(c.clone(), fast_backoff(), Duration::from_secs(10));
        assert!(a.authenticate_token("t").await.unwrap().is_some());
        assert_eq!(c.requests.lock().unwrap().len(), 2);

        let mut e403 = e500;
        e403.status_code = 403;
        e403.reason = "Forbidden".into();
        e403.message = "denied".into();
        let c = FakeReview::new(vec![Err(e403)]);
        let a = WebhookTokenAuthenticator::new(c.clone(), fast_backoff(), Duration::from_secs(10));
        assert_eq!(a.authenticate_token("t").await.unwrap_err(), "denied");
        assert_eq!(c.requests.lock().unwrap().len(), 1);
    }

    // ---- delegating.go New()

    fn cfg() -> DelegatingAuthenticatorConfig {
        DelegatingAuthenticatorConfig {
            anonymous: Some(AnonymousAuthConfig {
                enabled: true,
                condition_paths: vec![],
            }),
            token_access_review_client: None,
            token_access_review_timeout: Duration::from_secs(10),
            webhook_retry_backoff: Some(fast_backoff()),
            cache_ttl: Duration::from_secs(10),
            client_certificate_ca: None,
            request_header_config: None,
        }
    }

    // delegating.go:121-124: nothing but anonymous
    #[tokio::test]
    async fn factory_with_only_anonymous_returns_the_anonymous_authenticator() {
        let a = cfg().new().unwrap();
        let r = run(&*a, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(r.user.username, ANONYMOUS_USER);
    }

    // delegating.go:125
    #[test]
    fn factory_without_any_method_fails() {
        let mut c = cfg();
        c.anonymous = None;
        assert_eq!(
            c.new().err().unwrap(),
            "no authentication method configured"
        );
        let mut c = cfg();
        c.anonymous = Some(AnonymousAuthConfig::default());
        assert!(c.new().is_err());
    }

    // delegating.go:101-102
    #[test]
    fn factory_with_a_token_client_needs_a_retry_backoff() {
        let mut c = cfg();
        c.token_access_review_client = Some(FakeReview::new(vec![]));
        c.webhook_retry_backoff = None;
        assert_eq!(
            c.new().err().unwrap(),
            "retry backoff parameters for delegating authentication webhook has not been specified"
        );
    }

    // delegating.go:127-130: authenticated users get system:authenticated, and
    // anonymous is the fallback behind a FailOnError union.
    #[tokio::test]
    async fn factory_token_chain_adds_group_and_falls_back_to_anonymous() {
        let mut c = cfg();
        c.token_access_review_client = Some(FakeReview::new(vec![
            status(serde_json::json!({"authenticated": true, "user": {"username":"sa"}})),
            status(serde_json::json!({"authenticated": false})),
        ]));
        let a = c.new().unwrap();
        let mut h = headers(&[("authorization", "Bearer good")]);
        let r = run(&*a, &mut h).await.unwrap().unwrap();
        assert_eq!(r.user.username, "sa");
        assert_eq!(r.user.groups, vec![ALL_AUTHENTICATED.to_string()]);

        // no credentials at all: anonymous
        let r = run(&*a, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(r.user.username, ANONYMOUS_USER);
        // a rejected token is an error (`invalid bearer token`), NOT anonymous
        let mut h = headers(&[("authorization", "Bearer bad")]);
        assert_eq!(run(&*a, &mut h).await.unwrap_err(), "invalid bearer token");
    }

    // delegating.go:76-93: client cert chain is authenticated through the CA provider.
    #[tokio::test]
    async fn factory_client_cert_chain() {
        let p = pki("ca");
        let mut c = cfg();
        c.client_certificate_ca = Some(ca_content(&p));
        let a = c.new().unwrap();
        let der = client_cert(&p, "admin", &["system:masters"], true);
        let r = run_certs(&*a, &[der]).await.unwrap().unwrap();
        assert_eq!(r.user.username, "admin");
        assert_eq!(
            r.user.groups,
            vec!["system:masters".to_string(), ALL_AUTHENTICATED.to_string()]
        );
    }

    // delegating.go:66-80: front-proxy first; headers are only trusted from a
    // client cert that verifies against the request-header CA.
    #[tokio::test]
    async fn factory_request_header_requires_a_verified_front_proxy_cert() {
        let p = pki("front-proxy-ca");
        let mut c = cfg();
        let settings = Arc::new(RwLock::new(RequestHeaderSettings {
            username_headers: vec!["x-remote-user".into()],
            group_headers: vec!["x-remote-group".into()],
            extra_header_prefixes: vec!["x-remote-extra-".into()],
            ..Default::default()
        }));
        c.request_header_config = Some(RequestHeaderConfig {
            ca: ca_content(&p),
            settings,
        });
        let a = c.new().unwrap();
        let der = client_cert(&p, "front-proxy-client", &[], true);
        let mut h = headers(&[("x-remote-user", "alice"), ("x-remote-group", "devs")]);
        let r = a
            .authenticate_request(&mut AuthRequest {
                headers: &mut h,
                path: "/metrics",
                peer_certs: &[der],
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.user.username, "alice");
        // without a cert the headers are NOT trusted: anonymous
        let mut h = headers(&[("x-remote-user", "alice")]);
        let r = run(&*a, &mut h).await.unwrap().unwrap();
        assert_eq!(r.user.username, ANONYMOUS_USER);
    }
}
