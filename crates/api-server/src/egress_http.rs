//! An HTTP client whose connections are dialed through an egress selector
//! `Dialer` (#3466, split from #3416).
//!
//! Upstream sets the selected `utilnet.DialFunc` as the `DialContext` of the
//! client's `http.Transport` (`rest.Config.Dial` ->
//! `staging/src/k8s.io/client-go/rest/transport.go` `TransportConfig.DialHolder`
//! -> `transport/cache.go` `tlsCache.get`: `dial = config.DialHolder.Dial`, then
//! `utilnet.SetTransportDefaults` / `http.Transport{DialContext: dial,
//! TLSClientConfig: ...}`). reqwest 0.12 has no connector hook, so the same
//! shape is a hyper-util client over a connector that dials `host:port` with
//! `Dialer::dial("tcp", addr)` and layers TLS on top for `https` (the
//! transport does the TLS handshake over whatever `DialContext` returned).

use crate::egress_selector::{Conn, Dialer};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

/// The error of one request, carrying what the caller's retry/metrics logic
/// switches on (`utilnet.IsConnectionReset`, a timeout).
#[derive(Debug)]
pub struct EgressHttpError {
    pub message: String,
    pub timeout: bool,
    pub connection_reset: bool,
}

impl std::fmt::Display for EgressHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// A fully read response.
#[derive(Debug)]
pub struct EgressHttpResponse {
    pub status: u16,
    pub headers: hyper::HeaderMap,
    pub body: Bytes,
}

/// The connection handed to hyper: the dialed `Conn`, TLS-wrapped for `https`.
pub struct EgressIo(TokioIo<Conn>);

impl hyper::rt::Read for EgressIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for EgressIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl Connection for EgressIo {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

/// The `DialContext` of the transport: dial through the egress `Dialer`, then
/// TLS for `https`.
#[derive(Clone)]
pub struct EgressConnector {
    dialer: Dialer,
    tls: Arc<rustls::ClientConfig>,
}

impl tower::Service<Uri> for EgressConnector {
    type Response = EgressIo;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = std::io::Result<EgressIo>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            let io_err = |m: String| std::io::Error::other(m);
            let https = uri.scheme_str() == Some("https");
            let host = uri
                .host()
                .ok_or_else(|| io_err(format!("no host in url {uri}")))?
                .to_string();
            let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
            // `host` keeps the brackets of an IPv6 literal, as `host:port`
            // needs them.
            let addr = format!("{host}:{port}");
            let conn = this
                .dialer
                .dial("tcp", &addr)
                .await
                .map_err(|e| io_err(format!("dial tcp {addr}: {e}")))?;
            let conn: Conn = if https {
                let bare = host.trim_start_matches('[').trim_end_matches(']');
                let name = rustls::pki_types::ServerName::try_from(bare.to_string())
                    .map_err(|e| io_err(format!("invalid server name {bare:?}: {e}")))?;
                let tls = tokio_rustls::TlsConnector::from(this.tls.clone())
                    .connect(name, conn)
                    .await?;
                Box::new(tls)
            } else {
                conn
            };
            Ok(EgressIo(TokioIo::new(conn)))
        })
    }
}

type HyperClient = hyper_util::client::legacy::Client<EgressConnector, Full<Bytes>>;

/// An HTTP/1.1 client dialing through an egress `Dialer`.
#[derive(Clone)]
pub struct EgressHttpClient {
    client: HyperClient,
    timeout: Duration,
}

#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &rustls::pki_types::CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &rustls::pki_types::CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// The `rest.Config` TLS fields (`CAData`, `Insecure`, `CertData`+`KeyData`)
/// as a rustls client config. With a CA the pool is that CA alone (Go's
/// `rootCertPool(caData)`); without one, the host's roots.
pub fn tls_client_config(
    ca_pem: Option<&[u8]>,
    insecure: bool,
    identity_pem: Option<&[u8]>,
) -> anyhow::Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let builder = if insecure {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
    } else {
        let mut roots = rustls::RootCertStore::empty();
        match ca_pem {
            Some(ca) => {
                let certs = rustls_pemfile::certs(&mut &*ca).collect::<Result<Vec<_>, _>>()?;
                let (added, _) = roots.add_parsable_certificates(certs);
                if added == 0 {
                    anyhow::bail!("unable to load root certificates: no valid certificates found");
                }
            }
            None => {
                roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            }
        }
        builder.with_root_certificates(roots)
    };
    Ok(match identity_pem {
        Some(id) => {
            let certs = rustls_pemfile::certs(&mut &*id).collect::<Result<Vec<_>, _>>()?;
            let key = rustls_pemfile::private_key(&mut &*id)?
                .ok_or_else(|| anyhow::anyhow!("tls: failed to find any PEM data in key input"))?;
            builder.with_client_auth_cert(certs, key)?
        }
        None => builder.with_no_client_auth(),
    })
}

impl EgressHttpClient {
    pub fn new(dialer: Dialer, tls: rustls::ClientConfig, timeout: Duration) -> Self {
        let connector = EgressConnector {
            dialer,
            tls: Arc::new(tls),
        };
        Self {
            client: hyper_util::client::legacy::Client::builder(TokioExecutor::new())
                .build(connector),
            timeout,
        }
    }

    /// POST `body` (JSON) to `url`; `headers` are added as given. The whole
    /// exchange is bounded by the client timeout (`rest.Config.Timeout`).
    pub async fn post_json(
        &self,
        url: &str,
        headers: &[(&str, String)],
        body: Vec<u8>,
    ) -> Result<EgressHttpResponse, EgressHttpError> {
        let err = |message: String| EgressHttpError {
            message,
            timeout: false,
            connection_reset: false,
        };
        let mut req = hyper::Request::post(url)
            .header("content-type", "application/json")
            .header("accept", "application/json");
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        let req = req
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| err(format!("building request: {e}")))?;
        let exchange = async {
            let resp = self.client.request(req).await.map_err(|e| {
                // Go's `url.Error` message carries the wrapped dial error.
                let mut reset = false;
                let mut message = format!("Post \"{url}\": {e}");
                let mut src = std::error::Error::source(&e);
                while let Some(s) = src {
                    message.push_str(&format!(": {s}"));
                    if let Some(io) = s.downcast_ref::<std::io::Error>() {
                        reset |= io.kind() == std::io::ErrorKind::ConnectionReset;
                    }
                    src = s.source();
                }
                EgressHttpError {
                    message,
                    timeout: false,
                    connection_reset: reset,
                }
            })?;
            let (parts, body) = resp.into_parts();
            let body = body
                .collect()
                .await
                .map_err(|e| err(format!("reading response body: {e}")))?
                .to_bytes();
            Ok(EgressHttpResponse {
                status: parts.status.as_u16(),
                headers: parts.headers,
                body,
            })
        };
        match tokio::time::timeout(self.timeout, exchange).await {
            Ok(r) => r,
            Err(_) => Err(EgressHttpError {
                message: format!("Post \"{url}\": context deadline exceeded"),
                timeout: true,
                connection_reset: false,
            }),
        }
    }
}
