//! The transport an admission or conversion webhook call goes over.
//!
//! Ported from:
//! - `staging/src/k8s.io/apiserver/pkg/util/webhook/client.go`
//!   (`ClientManager.HookClient` / `hookClientConfig`): a `clientConfig.service`
//!   is reached at `<name>.<namespace>.svc:<port>` with the TLS server name set
//!   to that host, and its `rest.Config.Dial` swaps that address for the one
//!   `ServiceResolver.ResolveEndpoint` returns; a `clientConfig.url` is dialed
//!   as given (port 443 when none).
//! - `staging/src/k8s.io/apiserver/pkg/util/webhook/authentication.go`
//!   (`NewDefaultAuthenticationInfoResolverWrapper`): with an egress selector
//!   the dial for a service is `Lookup(Cluster)` and for a URL is
//!   `Lookup(ControlPlane)`.
//!
//! The api-server installs the egress-aware transport at start-up; without one
//! the [`ReqwestTransport`] default (direct dial) is used, which is what the
//! crate's own tests run over.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// `admissionregistrationv1.ServiceReference`, with the port defaulted to 443.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRef {
    pub namespace: String,
    pub name: String,
    pub port: i32,
}

impl ServiceRef {
    /// `serverName` in `hookClientConfig`: `<name>.<namespace>.svc`.
    pub fn server_name(&self) -> String {
        format!("{}.{}.svc", self.name, self.namespace)
    }

    /// `host` in `hookClientConfig`: `net.JoinHostPort(serverName, port)`.
    pub fn host_port(&self) -> String {
        format!("{}:{}", self.server_name(), self.port)
    }
}

/// What `cfg.Dial` does with the service address.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Dial {
    /// Dial the URL's own host and port.
    #[default]
    Default,
    /// `ServiceResolver.ResolveEndpoint` returned this `host:port`.
    Resolved(String),
    /// `ServiceResolver.ResolveEndpoint` failed; `cfg.Dial` returns the error.
    Failed(String),
}

/// Where one webhook call goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookTarget {
    /// The request URL (for a service `https://<name>.<ns>.svc:<port><path>`).
    pub url: String,
    /// Set when the config is a `clientConfig.service`.
    pub service: Option<ServiceRef>,
    pub dial: Dial,
}

impl WebhookTarget {
    /// A `clientConfig.url`.
    pub fn from_url(url: String) -> Self {
        Self {
            url,
            service: None,
            dial: Dial::Default,
        }
    }
}

impl std::fmt::Display for WebhookTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.url)
    }
}

/// One POST of a JSON body.
pub struct WebhookHttpRequest<'a> {
    /// `target.url` already carries the `?timeout=<n>s` query.
    pub target: &'a WebhookTarget,
    pub body: Vec<u8>,
    pub timeout: Duration,
    /// PEM of `clientConfig.caBundle`.
    pub ca_bundle: Option<&'a [u8]>,
}

#[derive(Debug)]
pub struct WebhookHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// A failed exchange: what the dispatcher folds into `ErrCallingWebhook`.
#[derive(Debug, Default)]
pub struct WebhookHttpError {
    pub message: String,
    /// The wrapped causes, outermost first.
    pub causes: Vec<String>,
    pub timeout: bool,
    /// The failure was establishing the connection.
    pub connect: bool,
}

pub type TransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<WebhookHttpResponse, WebhookHttpError>> + Send + 'a>>;

pub trait WebhookTransport: Send + Sync {
    fn post_json<'a>(&'a self, req: WebhookHttpRequest<'a>) -> TransportFuture<'a>;
}

static TRANSPORT: OnceLock<Arc<dyn WebhookTransport>> = OnceLock::new();

/// Installs the process-wide transport; the first one wins.
pub fn set_transport(transport: Arc<dyn WebhookTransport>) {
    let _ = TRANSPORT.set(transport);
}

/// The installed transport, or the direct-dial default.
pub fn transport() -> Arc<dyn WebhookTransport> {
    TRANSPORT
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(ReqwestTransport))
}

/// The default transport: reqwest dialing directly.
pub struct ReqwestTransport;

impl WebhookTransport for ReqwestTransport {
    fn post_json<'a>(&'a self, req: WebhookHttpRequest<'a>) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut builder = reqwest::Client::builder()
                .timeout(req.timeout)
                .connect_timeout(Duration::from_secs(2));
            if let Some(ca_data) = req.ca_bundle {
                if let Ok(cert) = reqwest::Certificate::from_pem(ca_data) {
                    builder = builder.add_root_certificate(cert);
                }
                builder = builder.danger_accept_invalid_certs(true);
            }
            match &req.target.dial {
                Dial::Resolved(addr) => {
                    let host = req
                        .target
                        .service
                        .as_ref()
                        .map(ServiceRef::server_name)
                        .unwrap_or_default();
                    if let Ok(sock) = addr.parse::<std::net::SocketAddr>() {
                        builder = builder.resolve(&host, sock);
                    }
                }
                Dial::Failed(e) => {
                    return Err(WebhookHttpError {
                        message: format!("Post \"{}\": {e}", req.target.url),
                        connect: true,
                        ..Default::default()
                    });
                }
                Dial::Default => {}
            }
            let client = builder.build().map_err(|e| WebhookHttpError {
                message: format!("Failed to create HTTP client: {e}"),
                ..Default::default()
            })?;
            let resp = client
                .post(&req.target.url)
                .header("content-type", "application/json")
                .body(req.body)
                .send()
                .await
                .map_err(|e| {
                    let mut causes = Vec::new();
                    let mut source = std::error::Error::source(&e);
                    while let Some(cause) = source {
                        causes.push(cause.to_string());
                        source = cause.source();
                    }
                    WebhookHttpError {
                        message: e.to_string(),
                        causes,
                        timeout: e.is_timeout(),
                        connect: e.is_connect(),
                    }
                })?;
            let status = resp.status().as_u16();
            let body = resp.bytes().await.map_err(|e| WebhookHttpError {
                message: format!("Failed to read webhook response body: {e}"),
                ..Default::default()
            })?;
            Ok(WebhookHttpResponse {
                status,
                body: body.to_vec(),
            })
        })
    }
}
