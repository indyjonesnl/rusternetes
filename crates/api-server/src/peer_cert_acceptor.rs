//! TLS acceptor that surfaces the verified client-certificate chain to handlers.
//!
//! `axum_server`'s stock `bind_rustls` serves the app via `into_make_service()`,
//! so even though the rustls layer verifies a client cert against
//! `--client-ca-file`, the verified peer cert never reaches a handler and CN/O
//! are never mapped to a user (#1129).
//!
//! [`PeerCertAcceptor`] wraps the stock [`RustlsAcceptor`]: after the TLS
//! handshake completes it reads the verified chain off the connection
//! ([`peer_certificates`]) and, via an [`axum::Extension`] layer applied to that
//! connection's service, injects it into every request's extensions as
//! [`PeerCertificates`]. The auth middleware then maps CN→user / O→groups.
//!
//! The chain is the one rustls already verified against the configured client
//! CA — this acceptor performs no verification of its own. When the client
//! presents no certificate (bearer-token clients), an empty chain is injected
//! and the middleware falls through to bearer-token / anonymous handling.
//!
//! [`peer_certificates`]: rustls::CommonState::peer_certificates

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use crate::dynamic_certificates::CaContentProvider;
use axum::Extension;
use axum_server::accept::Accept;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use rusternetes_middleware::request_header::{FrontProxyConnection, RequestHeaderConfig};
use rusternetes_middleware::PeerCertificates;
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::ClientCertVerifier;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Layer;

/// What the acceptor needs to split a handshake-verified chain between the two
/// authenticators that upstream keeps separate (`x509.Verifier` per CA,
/// `pkg/kubeapiserver/authenticator/config.go:113-131`).
#[derive(Clone)]
struct FrontProxyTrust {
    config: Arc<RequestHeaderConfig>,
    /// Providers, not verifiers: the CA files reload on change, and the
    /// verifier is read per connection (`VerifyOptions()` is called per
    /// request by `x509.Verifier`, x509.go:229-255).
    request_header_ca: Arc<dyn CaContentProvider>,
    /// `None` when `--client-ca-file` is unset: x509 authentication is then
    /// disabled (`config.ClientCAContentProvider != nil`, config.go:128).
    client_ca: Option<Arc<dyn CaContentProvider>>,
}

/// [`chain_verifies`] against a provider's current bundle; a provider with no
/// bundle loaded verifies nothing (`VerifyOptions` returning `false`).
fn provider_verifies(provider: &dyn CaContentProvider, chain: &[Vec<u8>]) -> bool {
    provider
        .verifier()
        .is_some_and(|v| chain_verifies(v.as_ref(), chain))
}

/// Whether `chain` (leaf first) verifies against `verifier`'s CA bundle
/// (`Certificate.Verify` with the presented intermediates, x509.go:235-250).
fn chain_verifies(verifier: &dyn ClientCertVerifier, chain: &[Vec<u8>]) -> bool {
    let Some((leaf, rest)) = chain.split_first() else {
        return false;
    };
    let leaf = CertificateDer::from(leaf.as_slice());
    let intermediates: Vec<CertificateDer<'_>> = rest
        .iter()
        .map(|c| CertificateDer::from(c.as_slice()))
        .collect();
    verifier
        .verify_client_cert(&leaf, &intermediates, UnixTime::now())
        .is_ok()
}

/// A TLS acceptor that injects the verified client-cert chain as a
/// [`PeerCertificates`] request extension. Drop-in replacement for the acceptor
/// `bind_rustls` installs.
#[derive(Clone)]
pub struct PeerCertAcceptor {
    inner: RustlsAcceptor,
    front_proxy: Option<FrontProxyTrust>,
}

impl PeerCertAcceptor {
    /// Build from the same [`RustlsConfig`] you would pass to `bind_rustls`.
    pub fn new(config: RustlsConfig) -> Self {
        Self {
            inner: RustlsAcceptor::new(config),
            front_proxy: None,
        }
    }

    /// Enable request-header (front-proxy) authentication. The `RustlsConfig`
    /// must trust the union of both CA providers (the serving controller keeps
    /// it current); this acceptor then decides, per connection, which
    /// authenticator the presented chain is valid for.
    pub fn with_request_header(
        mut self,
        config: Arc<RequestHeaderConfig>,
        request_header_ca: Arc<dyn CaContentProvider>,
        client_ca: Option<Arc<dyn CaContentProvider>>,
    ) -> Self {
        self.front_proxy = Some(FrontProxyTrust {
            config,
            request_header_ca,
            client_ca,
        });
        self
    }
}

impl<I, S> Accept<I, S> for PeerCertAcceptor
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Send + 'static,
{
    // Project the stream/service types through the wrapped acceptor + the
    // Extension layer so we never have to name `TlsStream` / `AddExtension`.
    type Stream = <RustlsAcceptor as Accept<I, S>>::Stream;
    type Service = <Extension<Option<FrontProxyConnection>> as Layer<
        <Extension<PeerCertificates> as Layer<S>>::Service,
    >>::Service;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let inner = self.inner.clone();
        let front_proxy = self.front_proxy.clone();
        Box::pin(async move {
            let (tls_stream, service) = inner.accept(stream, service).await?;

            // `get_ref().1` is the rustls ServerConnection post-handshake; the
            // chain it returns is already verified against the client CA. Copy
            // the DER out so the extension is `'static` and rustls-free.
            let chain: Vec<Vec<u8>> = tls_stream
                .get_ref()
                .1
                .peer_certificates()
                .map(|certs| certs.iter().map(|c| c.as_ref().to_vec()).collect())
                .unwrap_or_default();

            // With front-proxy authentication the handshake trusted the union
            // of both CA bundles, so the chain is re-verified per
            // authenticator (x509.Verifier). `PeerCertificates` then carries
            // only what the `--client-ca-file` x509 authenticator accepts.
            let (peer_chain, connection) = match &front_proxy {
                None => (chain, None),
                Some(trust) => {
                    let connection = FrontProxyConnection {
                        config: trust.config.clone(),
                        verified_by_request_header_ca: provider_verifies(
                            trust.request_header_ca.as_ref(),
                            &chain,
                        ),
                        verified_by_client_ca: trust
                            .client_ca
                            .as_ref()
                            .map(|v| provider_verifies(v.as_ref(), &chain)),
                        presented: Arc::new(chain),
                    };
                    (connection.x509_chain(), Some(connection))
                }
            };

            let service = Extension(PeerCertificates(Arc::new(peer_chain))).layer(service);
            let service = Extension(connection).layer(service);
            Ok((tls_stream, service))
        })
    }
}
