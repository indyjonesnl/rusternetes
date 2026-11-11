//! The aggregator's proxy of a request to an APIService backend: the tail of
//! `proxyHandler.ServeHTTP` plus the `UpgradeAwareHandler` it hands the
//! request to.
//!
//! Ported from:
//!
//! - `staging/src/k8s.io/kube-aggregator/pkg/apiserver/handler_proxy.go`
//!   `proxyHandler.ServeHTTP` (location, `NewRequestForProxy`,
//!   `NewAuthProxyRoundTripper` / `SetAuthProxyHeaders`,
//!   `NewUpgradeAwareHandler(location, rt, true, upgrade, responder)`,
//!   `RejectForwardingRedirects`) and `responder.Error`;
//! - `staging/src/k8s.io/apiserver/pkg/util/proxy/proxy.go` `NewRequestForProxy`;
//! - `staging/src/k8s.io/apimachinery/pkg/util/proxy/upgradeaware.go`
//!   `UpgradeAwareHandler.ServeHTTP`, `tryUpgrade`, `DialForUpgrade`, the
//!   `NewSingleHostReverseProxy` path with its `ModifyResponse` that rejects
//!   redirects, `defaultProxyTransport` and `removeCORSHeaders`;
//! - `staging/src/k8s.io/apimachinery/pkg/util/proxy/transport.go`
//!   `Transport.RoundTrip` and `rewriteURL`;
//! - `staging/src/k8s.io/client-go/transport/round_trippers.go`
//!   `SetAuthProxyHeaders`.
//!
//! Go-runtime differences and known gaps:
//!
//! - One connection per request: `transport.New` caches a pooled
//!   `http.Transport`; here every proxied request dials (through the egress
//!   selector's `Cluster` dialer, `proxyTransportDial`) and closes.
//! - The inbound `Authorization` header is dropped here. Upstream deletes it in
//!   `WithAuthentication` once the request is authenticated
//!   (`filters/authentication.go`), so it never reaches the proxy.
//! - Known gap, tracked on #3925: `rewriteResponse` (`text/html` link
//!   rewriting, which needs an HTML tokenizer) is not ported yet; the
//!   `Location` rewrite of `Transport.RoundTrip` is.
//! - `X-Forwarded-For` is only added when the request carries the peer address
//!   as axum `ConnectInfo<SocketAddr>`, as Go does when `RemoteAddr` parses.
//!   Every api-server listener provides it through
//!   `k3rs_api_server::serve_service`.

use crate::egress_selector::Dialer;
use crate::handlers::aggregator::AggregatorTarget;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use k3rs_common::auth::UserInfo;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, warn};

/// `AggregatorRejectForwardingRedirects` (`--aggregator-reject-forwarding-redirect`,
/// default true, pkg/controlplane/apiserver/options/options.go:130).
static REJECT_FORWARDING_REDIRECTS: AtomicBool = AtomicBool::new(true);

/// Set `--aggregator-reject-forwarding-redirect`.
pub fn set_reject_forwarding_redirects(reject: bool) {
    REJECT_FORWARDING_REDIRECTS.store(reject, Ordering::SeqCst);
}

/// `aggregatedDiscoveryTimeout` (apiserver/pkg/util/proxy/proxy.go).
const AGGREGATED_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// client-go's default dial timeout (`transport.New` -> `utilnet.SetTransportDefaults`
/// dialer, `Timeout: 30 * time.Second`).
const DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// The message of the `ModifyResponse` that replaces a redirect
/// (`upgradeaware.go` `ServeHTTP`).
const REJECTED_REDIRECT_MESSAGE: &str =
    "the backend attempted to redirect this request, which is not permitted";

/// `http.Error(w, msg, code)`.
pub fn http_error(message: &str, code: StatusCode) -> Response {
    let mut resp = Response::new(Body::from(format!("{message}\n")));
    *resp.status_mut() = code;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    resp
}

/// `responder.Error` (handler_proxy.go): `http.Error(r.w, err.Error(), http.StatusServiceUnavailable)`.
fn responder_error(message: &str) -> Response {
    http_error(message, StatusCode::SERVICE_UNAVAILABLE)
}

/// `httpstream.IsUpgradeRequest`: a `Connection` header value containing `upgrade`.
pub fn is_upgrade_request(headers: &HeaderMap) -> bool {
    headers.get_all(header::CONNECTION).iter().any(|v| {
        v.to_str()
            .is_ok_and(|s| s.to_lowercase().contains("upgrade"))
    })
}

/// `SetAuthProxyHeaders`: stomp the auth proxy header fields.
/// `X-Remote-Uid` is sent when the user has one (`RemoteRequestHeaderUID`,
/// beta and on by default).
pub fn set_auth_proxy_headers(headers: &mut HeaderMap, user: &UserInfo) {
    headers.remove("x-remote-user");
    headers.remove("x-remote-uid");
    headers.remove("x-remote-group");
    let extra_keys: Vec<HeaderName> = headers
        .keys()
        .filter(|k| k.as_str().starts_with("x-remote-extra-"))
        .cloned()
        .collect();
    for k in extra_keys {
        headers.remove(k);
    }
    if let Ok(v) = HeaderValue::from_str(&user.username) {
        headers.insert("x-remote-user", v);
    }
    if !user.uid.is_empty() {
        if let Ok(v) = HeaderValue::from_str(&user.uid) {
            headers.insert("x-remote-uid", v);
        }
    }
    for group in &user.groups {
        if let Ok(v) = HeaderValue::from_str(group) {
            headers.append("x-remote-group", v);
        }
    }
    let mut extras: Vec<_> = user.extra.iter().collect();
    extras.sort_by(|a, b| a.0.cmp(b.0));
    for (key, values) in extras {
        let name = format!(
            "x-remote-extra-{}",
            crate::handlers::aggregator::header_key_escape(key).to_lowercase()
        );
        let Ok(name) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        for value in values {
            if let Ok(v) = HeaderValue::from_str(value) {
                headers.append(name.clone(), v);
            }
        }
    }
}

/// Go's `net/http` hop-by-hop set (`reverseproxy.go` `hopHeaders`).
const HOP_HEADERS: [&str; 9] = [
    "connection",
    "proxy-connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// `ReverseProxy`: remove the headers named by `Connection`, then the standard
/// hop-by-hop headers.
fn remove_hop_headers(headers: &mut HeaderMap) {
    let nominated: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    for name in nominated {
        headers.remove(name.as_str());
    }
    for name in HOP_HEADERS {
        headers.remove(name);
    }
}

/// `utilnet.AppendForwardedForHeader`.
fn append_forwarded_for(headers: &mut HeaderMap, client_ip: Option<std::net::IpAddr>) {
    let Some(ip) = client_ip else { return };
    let mut value = ip.to_string();
    let prior: Vec<&str> = headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    if !prior.is_empty() {
        value = format!("{}, {}", prior.join(", "), value);
    }
    if let Ok(v) = HeaderValue::from_str(&value) {
        headers.insert("x-forwarded-for", v);
    }
}

/// Go's `path.Clean` for an absolute or relative slash path.
fn path_clean(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if matches!(out.last(), Some(l) if *l != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            s => out.push(s),
        }
    }
    let joined = out.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
}

/// `url.Values.Encode()` of `req.URL.Query()`: keys sorted, `+` for spaces.
fn encode_query(raw: Option<&str>) -> String {
    let Some(raw) = raw else {
        return String::new();
    };
    let mut pairs: Vec<(String, String)> = url::form_urlencoded::parse(raw.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    // `Encode` sorts by key; values keep their order (stable sort).
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}

/// `Transport.rewriteURL` of `Location`, for a transport whose `Scheme` and
/// `Host` are the backend's and whose `PathPrepend` is empty
/// (`defaultProxyTransport` with `url.Path == h.Location.Path`).
fn rewrite_location(location: &str, scheme: &str, host: &str) -> String {
    // A path-absolute URL has no host of its own: it is "the same host".
    if location.starts_with('/') && !location.starts_with("//") {
        return format!("{scheme}://{host}{location}");
    }
    if let Some((_, rest)) = location.split_once("://") {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        // `isDifferentHost`: the source URL's host, or the request's Host
        // (the same value here).
        if authority == host {
            let tail = &rest[authority.len()..];
            return format!("{scheme}://{host}{tail}");
        }
    }
    // Different host, or relative.
    location.to_string()
}

fn connect_ip(req: &Request) -> Option<std::net::IpAddr> {
    req.extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|c| c.0.ip())
}

/// The proxy of one aggregated request: `proxyHandler.ServeHTTP` after the
/// endpoint was resolved (`target` is `ResolveEndpoint`'s `rloc`).
///
/// `dialer` is `proxyTransportDial`'s dialer; `None` is the default dial.
pub async fn serve_proxy(
    target: &AggregatorTarget,
    user: &UserInfo,
    req: Request,
    dialer: Option<Dialer>,
) -> Response {
    let reject_redirects = REJECT_FORWARDING_REDIRECTS.load(Ordering::SeqCst);
    serve_proxy_with(target, user, req, dialer, reject_redirects).await
}

/// [`serve_proxy`] with `RejectForwardingRedirects` given.
pub async fn serve_proxy_with(
    target: &AggregatorTarget,
    user: &UserInfo,
    mut req: Request,
    dialer: Option<Dialer>,
    reject_forwarding_redirects: bool,
) -> Response {
    // `location`: scheme, the resolved endpoint's host and the request path,
    // `RawQuery = req.URL.Query().Encode()`.
    let host = if target.host.contains(':') && !target.host.starts_with('[') {
        format!("[{}]:{}", target.host, target.port)
    } else {
        format!("{}:{}", target.host, target.port)
    };
    let scheme = target.scheme;
    let path = req.uri().path().to_string();
    let query = encode_query(req.uri().query());
    let path_and_query = if query.is_empty() {
        path.clone()
    } else {
        format!("{path}?{query}")
    };

    // `NewRequestForProxy`: discovery requests get a short timeout; the
    // audit ID is propagated.
    let segments = path.trim_matches('/').split('/').count();
    let timeout =
        (path.starts_with("/apis/") && segments == 3).then_some(AGGREGATED_DISCOVERY_TIMEOUT);
    let mut headers = req.headers().clone();
    // `WithAuthentication` removes it once the request is authenticated.
    headers.remove(header::AUTHORIZATION);
    if let Some(id) = crate::audit::audit_id_from_context() {
        if let Ok(v) = HeaderValue::from_str(&id) {
            headers.insert("audit-id", v);
        }
    }
    if let Ok(v) = HeaderValue::from_str(&host) {
        headers.insert(header::HOST, v);
    }
    // Both the upgrade path (headers attached ahead of the direct dial) and
    // `NewAuthProxyRoundTripper` stomp the same fields.
    set_auth_proxy_headers(&mut headers, user);

    let upgrade = is_upgrade_request(req.headers());
    let client_ip = connect_ip(&req);
    let client_on_upgrade = req.extensions_mut().remove::<hyper::upgrade::OnUpgrade>();
    let method = req.method().clone();
    let body = std::mem::take(req.body_mut());

    let work = async {
        if upgrade {
            // `tryUpgrade`: X-Forwarded-For only on the upgrade path.
            append_forwarded_for(&mut headers, client_ip);
            try_upgrade(
                target,
                &host,
                &path_and_query,
                method,
                headers,
                client_on_upgrade,
                dialer,
            )
            .await
        } else {
            remove_hop_headers(&mut headers);
            append_forwarded_for(&mut headers, client_ip);
            // `Transport.RoundTrip`: the reverse proxy headers.
            let forwarded_uri = {
                let mut u = path_clean(&path);
                if path.ends_with('/') {
                    u.push('/');
                }
                u
            };
            if let Ok(v) = HeaderValue::from_str(&forwarded_uri) {
                headers.insert("x-forwarded-uri", v);
            }
            if let Ok(v) = HeaderValue::from_str(&host) {
                headers.insert("x-forwarded-host", v);
            }
            headers.insert(
                "x-forwarded-proto",
                HeaderValue::from_static(scheme_static(scheme)),
            );
            reverse_proxy(
                target,
                &host,
                &path_and_query,
                method,
                headers,
                body,
                dialer,
                reject_forwarding_redirects,
            )
            .await
        }
    };
    match timeout {
        Some(t) => tokio::time::timeout(t, work).await.unwrap_or_else(|_| {
            responder_error("error trying to reach service: context deadline exceeded")
        }),
        None => work.await,
    }
}

fn scheme_static(scheme: &str) -> &'static str {
    if scheme == "http" {
        "http"
    } else {
        "https"
    }
}

/// A connected, handshaken HTTP/1 client connection to the backend
/// (`transport.New`'s `DialContext` + TLS, then one request).
async fn connect(
    target: &AggregatorTarget,
    host: &str,
    dialer: Option<Dialer>,
) -> Result<hyper::client::conn::http1::SendRequest<Body>, String> {
    use tower::Service;
    let tls = crate::handlers::aggregator::proxy_tls_config(target)
        .map_err(|e| format!("aggregator client error: {e}"))?;
    let mut connector = crate::egress_http::EgressConnector::new(
        dialer.unwrap_or(Dialer::Direct),
        tls,
        target.server_name.clone(),
    );
    let uri: hyper::Uri = format!("{}://{}/", target.scheme, host)
        .parse()
        .map_err(|e| format!("invalid backend address {host}: {e}"))?;
    let io = tokio::time::timeout(DIAL_TIMEOUT, connector.call(uri))
        .await
        .map_err(|_| "dial timeout".to_string())?
        .map_err(|e| e.to_string())?;
    let (sender, conn) = hyper::client::conn::http1::handshake::<_, Body>(io)
        .await
        .map_err(|e| e.to_string())?;
    tokio::spawn(async move {
        if let Err(e) = conn.with_upgrades().await {
            debug!("aggregator proxy connection ended: {e}");
        }
    });
    Ok(sender)
}

/// `idleConnsPerHost` of client-go's transport cache (`transport/cache.go`).
const IDLE_CONNS_PER_HOST: usize = 25;

/// `utilnet.SetTransportDefaults`' `IdleConnTimeout` (`http.DefaultTransport`).
const IDLE_CONN_TIMEOUT: Duration = Duration::from_secs(90);

type PooledClient = hyper_util::client::legacy::Client<TimedConnector, Body>;

/// `tlsCacheKey` (client-go `transport/cache.go`): `insecure`, `caData`,
/// `certData`/`keyData` (one PEM here), `serverName` and the `dial` holder.
/// The URL scheme is not a field: the TLS config does not depend on it.
/// `certFile`/`keyFile`, `nextProtos`, `disableCompression` and `getCert` are
/// constant for the aggregator's `transport.Config` (handler_proxy.go:221-230),
/// and `Config.Proxy` is never set, so every config is cacheable
/// (`tlsConfigKey`'s `c.Proxy != nil` branch does not apply).
#[derive(Clone, PartialEq, Eq, Hash)]
struct TransportKey {
    server_name: Option<String>,
    insecure: bool,
    ca_bundle: Option<Vec<u8>>,
    identity: Option<Vec<u8>>,
    /// Address of the shared `DialerCreator`; 0 is the direct dialer.
    dialer: usize,
}

static TRANSPORTS: std::sync::LazyLock<std::sync::Mutex<HashMap<TransportKey, PooledClient>>> =
    std::sync::LazyLock::new(Default::default);

/// The egress connector bounded by [`DIAL_TIMEOUT`] (the transport's dialer
/// `Timeout`).
#[derive(Clone)]
struct TimedConnector(crate::egress_http::EgressConnector);

impl tower::Service<hyper::Uri> for TimedConnector {
    type Response = crate::egress_http::EgressIo;
    type Error = std::io::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<Self::Response>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: hyper::Uri) -> Self::Future {
        let mut inner = self.0.clone();
        Box::pin(async move {
            tokio::time::timeout(DIAL_TIMEOUT, tower::Service::call(&mut inner, uri))
                .await
                .map_err(|_| std::io::Error::other("dial timeout"))?
        })
    }
}

/// The cached transport for `target` (`tlsCache.get`): one connection pool
/// per distinct config (unbounded, as `tlsTransportCache.transports` is), so requests reuse backend connections instead of
/// dialing and handshaking each time.
fn pooled_client(
    target: &AggregatorTarget,
    dialer: Option<Dialer>,
) -> Result<PooledClient, String> {
    let identity = crate::handlers::aggregator::current_proxy_client_identity_pem();
    let key = TransportKey {
        server_name: target.server_name.clone(),
        insecure: target.insecure_skip_tls_verify,
        ca_bundle: target.ca_bundle.clone(),
        identity,
        dialer: match &dialer {
            Some(Dialer::Proxy(d)) => Arc::as_ptr(d) as usize,
            _ => 0,
        },
    };
    let mut cache = TRANSPORTS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(c) = cache.get(&key) {
        return Ok(c.clone());
    }
    let tls = crate::handlers::aggregator::proxy_tls_config(target)
        .map_err(|e| format!("aggregator client error: {e}"))?;
    let connector = TimedConnector(crate::egress_http::EgressConnector::new(
        dialer.unwrap_or(Dialer::Direct),
        tls,
        target.server_name.clone(),
    ));
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .pool_idle_timeout(IDLE_CONN_TIMEOUT)
        .pool_max_idle_per_host(IDLE_CONNS_PER_HOST)
        .pool_timer(hyper_util::rt::TokioTimer::new())
        .build(connector);
    cache.insert(key, client.clone());
    Ok(client)
}

/// Live transports in the cache (test seam).
pub fn cached_transport_count() -> usize {
    TRANSPORTS.lock().map(|c| c.len()).unwrap_or(0)
}

/// `ReverseProxy` with the `Transport` wrapper and, when requested, the
/// redirect-rejecting `ModifyResponse`.
#[allow(clippy::too_many_arguments)]
async fn reverse_proxy(
    target: &AggregatorTarget,
    host: &str,
    path_and_query: &str,
    method: axum::http::Method,
    headers: HeaderMap,
    body: Body,
    dialer: Option<Dialer>,
    reject_forwarding_redirects: bool,
) -> Response {
    // Every failure below is `Transport.RoundTrip`'s
    // `errors.NewServiceUnavailable("error trying to reach service: ...")`,
    // written by `responder.Error`.
    let unreachable = |e: String| responder_error(&format!("error trying to reach service: {e}"));
    let client = match pooled_client(target, dialer) {
        Ok(c) => c,
        Err(e) => {
            warn!("aggregator proxy: {e}");
            return unreachable(e);
        }
    };
    let mut out = match hyper::Request::builder()
        .method(method)
        .uri(format!("{}://{}{}", target.scheme, host, path_and_query))
        .body(body)
    {
        Ok(r) => r,
        Err(e) => return unreachable(e.to_string()),
    };
    *out.headers_mut() = headers;
    let resp = match client.request(out).await {
        Ok(r) => r,
        Err(e) => {
            warn!("aggregator proxy: {e}");
            return unreachable(e.to_string());
        }
    };
    let (mut parts, incoming) = resp.into_parts();

    // `Transport.RoundTrip`: `Location` rewrite.
    if let Some(loc) = parts.headers.get(header::LOCATION).cloned() {
        if let Ok(loc) = loc.to_str() {
            if !loc.is_empty() {
                if let Ok(v) = HeaderValue::from_str(&rewrite_location(loc, target.scheme, host)) {
                    parts.headers.insert(header::LOCATION, v);
                }
            }
        }
    }
    // `corsRemovingTransport`.
    for h in [
        "access-control-allow-credentials",
        "access-control-allow-headers",
        "access-control-allow-methods",
        "access-control-allow-origin",
    ] {
        parts.headers.remove(h);
    }

    // `ModifyResponse` of `RejectForwardingRedirects`.
    let code = parts.status.as_u16();
    if reject_forwarding_redirects
        && (300..=399).contains(&code)
        && parts
            .headers
            .get(header::LOCATION)
            .is_some_and(|v| !v.is_empty())
    {
        let mut resp = Response::new(Body::from(REJECTED_REDIRECT_MESSAGE));
        *resp.status_mut() = StatusCode::BAD_GATEWAY;
        return resp;
    }

    remove_hop_headers(&mut parts.headers);
    let mut resp = Response::new(Body::from_stream(
        http_body_util::BodyExt::into_data_stream(incoming),
    ));
    *resp.status_mut() = parts.status;
    *resp.headers_mut() = parts.headers;
    resp
}

/// `UpgradeAwareHandler.tryUpgrade`.
async fn try_upgrade(
    target: &AggregatorTarget,
    host: &str,
    path_and_query: &str,
    method: axum::http::Method,
    headers: HeaderMap,
    client_on_upgrade: Option<hyper::upgrade::OnUpgrade>,
    dialer: Option<Dialer>,
) -> Response {
    // `DialForUpgrade` -> `dial`: "error dialing backend: %v" /
    // "error sending request: %v".
    let mut sender = match connect(target, host, dialer).await {
        Ok(s) => s,
        Err(e) => {
            warn!("aggregator proxy upgrade: {e}");
            return responder_error(&format!("error dialing backend: {e}"));
        }
    };
    let mut out = match hyper::Request::builder()
        .method(method)
        .uri(path_and_query)
        .body(Body::empty())
    {
        Ok(r) => r,
        Err(e) => return responder_error(&format!("error sending request: {e}")),
    };
    *out.headers_mut() = headers;
    let mut backend = match sender.send_request(out).await {
        Ok(r) => r,
        Err(e) => return responder_error(&format!("error sending request: {e}")),
    };

    let status = backend.status();
    // "If the backend did not upgrade the request, return an error to the
    // client. If the response was an error, the error is forwarded directly."
    if status != StatusCode::SWITCHING_PROTOCOLS && status.as_u16() < 400 {
        let err = format!("invalid upgrade response: status code {}", status.as_u16());
        warn!("Proxy upgrade error: {err}");
        return responder_error(&err);
    }
    // "Once the connection is hijacked, the ErrorResponder will no longer
    // work": the client's upgrade is the last step.
    let Some(client_on_upgrade) = client_on_upgrade else {
        return responder_error("request connection cannot be hijacked");
    };

    if status != StatusCode::SWITCHING_PROTOCOLS {
        // Echo the backend's error response to the client.
        let (parts, incoming) = backend.into_parts();
        let mut resp = Response::new(Body::from_stream(
            http_body_util::BodyExt::into_data_stream(incoming),
        ));
        *resp.status_mut() = parts.status;
        *resp.headers_mut() = parts.headers;
        return resp;
    }

    let backend_on_upgrade = hyper::upgrade::on(&mut backend);
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    *resp.headers_mut() = backend.headers().clone();
    tokio::spawn(async move {
        match tokio::try_join!(client_on_upgrade, backend_on_upgrade) {
            Ok((client, backend)) => {
                splice(
                    hyper_util::rt::TokioIo::new(client),
                    hyper_util::rt::TokioIo::new(backend),
                )
                .await
            }
            Err(e) => warn!("aggregator proxy: upgrade handshake failed: {e}"),
        }
    });
    resp
}

/// "Proxy the connection. This is bidirectional ... Wait for one half the
/// connection to exit. Once it does the defer will clean up the other half."
pub async fn splice<C, B>(client: C, backend: B)
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    B: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut client_r, mut client_w) = tokio::io::split(client);
    let (mut backend_r, mut backend_w) = tokio::io::split(backend);
    let writer = tokio::io::copy(&mut client_r, &mut backend_w);
    let reader = tokio::io::copy(&mut backend_r, &mut client_w);
    tokio::select! {
        r = writer => if let Err(e) = r { debug!("Error proxying data from client to backend: {e}") },
        r = reader => if let Err(e) = r { debug!("Error proxying data from backend to client: {e}") },
    }
}
