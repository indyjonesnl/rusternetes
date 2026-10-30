//! TLS serving of the secure port: `SecureServingInfo.Serve`.
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/apiserver/pkg/server/secure_serving.go:44-160`
//!   (`tlsConfig`, `SecureServingInfo.Serve`)
//! * `staging/src/k8s.io/apiserver/pkg/server/options/serving.go:268-338`
//!   (`ApplyTo`: cert/key files, cipher suites, min TLS version)
//! * `staging/src/k8s.io/component-base/cli/flag/ciphersuites_flag.go:30-147`
//!   (`TLSCipherSuites`, `TLSVersion`, `DefaultTLSVersion`)
//! * `staging/src/k8s.io/apiserver/pkg/server/dynamiccertificates/tlsconfig.go`
//!   (`DynamicServingCertificateController`: the serving certificate and the
//!   client CA hints are reloaded while the server runs)
//!
//! Like upstream (`tls.RequestClientCert`), the TLS layer asks for a client
//! certificate but does NOT verify the chain: "Populate PeerCertificates in
//! requests, but don't reject connections without certificates. This allows
//! certificates to be validated by authenticators, while still allowing other
//! auth types" (secure_serving.go:78-82). The x509 authenticators in
//! [`super::authn`] do the verification. The handshake itself still proves
//! possession of the private key (CertificateVerify signatures are checked).
//!
//! Deliberate reductions, each tracked in the issue: Go cipher suites that
//! rustls does not implement (CBC, RC4, 3DES, RSA key exchange) and
//! `VersionTLS10`/`VersionTLS11` are rejected with an error; SNI certificates,
//! `--http2-max-streams-per-connection`, `--permit-port-sharing` and
//! `--permit-address-sharing` are not offered.

use super::authn::CaContent;
use axum::Extension;
use axum::Router;
use axum_server::accept::Accept;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use rustls::crypto::aws_lc_rs::cipher_suite as cs;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme, SupportedCipherSuite};
use std::collections::BTreeSet;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Layer;

/// How often the serving certificate and the client CA hints are re-read.
const RELOAD_INTERVAL: Duration = Duration::from_secs(1);

/// `tls.VersionTLS12` / `tls.VersionTLS13` (the versions rustls implements).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

const VERSION_NAMES: [&str; 4] = [
    "VersionTLS10",
    "VersionTLS11",
    "VersionTLS12",
    "VersionTLS13",
];

/// `cliflag.TLSVersion` (ciphersuites_flag.go:131-139): empty means
/// [`TlsVersion::Tls12`] (`DefaultTLSVersion`).
pub fn tls_version(name: &str) -> Result<TlsVersion, String> {
    match name {
        "" | "VersionTLS12" => Ok(TlsVersion::Tls12),
        "VersionTLS13" => Ok(TlsVersion::Tls13),
        "VersionTLS10" | "VersionTLS11" => Err(format!(
            "tls version {name:?} is not supported by this TLS implementation"
        )),
        _ => Err(format!("unknown tls version {name:?}")),
    }
}

/// `cliflag.TLSPossibleVersions` (ciphersuites_flag.go:121-128).
pub fn tls_possible_versions() -> Vec<String> {
    VERSION_NAMES.iter().map(|s| s.to_string()).collect()
}

/// `tls.CipherSuites()` (the secure list) plus the two legacy ChaCha20 names
/// (`ciphers[...]` in ciphersuites_flag.go:34-37), with the rustls suite when
/// there is one.
fn preferred_ciphers() -> Vec<(&'static str, Option<SupportedCipherSuite>)> {
    vec![
        ("TLS_RSA_WITH_AES_128_CBC_SHA", None),
        ("TLS_RSA_WITH_AES_256_CBC_SHA", None),
        ("TLS_RSA_WITH_AES_128_GCM_SHA256", None),
        ("TLS_RSA_WITH_AES_256_GCM_SHA384", None),
        ("TLS_AES_128_GCM_SHA256", Some(cs::TLS13_AES_128_GCM_SHA256)),
        ("TLS_AES_256_GCM_SHA384", Some(cs::TLS13_AES_256_GCM_SHA384)),
        (
            "TLS_CHACHA20_POLY1305_SHA256",
            Some(cs::TLS13_CHACHA20_POLY1305_SHA256),
        ),
        ("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", None),
        ("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", None),
        ("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", None),
        ("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", None),
        (
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            Some(cs::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256),
        ),
        (
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            Some(cs::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384),
        ),
        (
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            Some(cs::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256),
        ),
        (
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            Some(cs::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384),
        ),
        (
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
            Some(cs::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256),
        ),
        (
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
            Some(cs::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256),
        ),
        // legacy names kept for backward compatibility
        (
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305",
            Some(cs::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256),
        ),
        (
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305",
            Some(cs::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256),
        ),
    ]
}

/// `tls.InsecureCipherSuites()`: none of them is implemented by rustls.
const INSECURE_CIPHERS: [&str; 8] = [
    "TLS_RSA_WITH_RC4_128_SHA",
    "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
    "TLS_RSA_WITH_AES_128_CBC_SHA256",
    "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
    "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
    "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
];

/// `cliflag.TLSCipherSuites` (ciphersuites_flag.go:98-112): the rustls suites for
/// the Go cipher names.
pub fn tls_cipher_suites(names: &[String]) -> Result<Vec<SupportedCipherSuite>, String> {
    let preferred = preferred_ciphers();
    let mut out = vec![];
    for name in names {
        if let Some((_, suite)) = preferred.iter().find(|(n, _)| n == name) {
            match suite {
                Some(s) => out.push(*s),
                None => {
                    return Err(format!(
                        "Cipher suite {name} is not supported by this TLS implementation"
                    ))
                }
            }
        } else if INSECURE_CIPHERS.contains(&name.as_str()) {
            return Err(format!(
                "Cipher suite {name} is not supported by this TLS implementation"
            ));
        } else {
            return Err(format!(
                "Cipher suite {name} not supported or doesn't exist"
            ));
        }
    }
    Ok(out)
}

/// `cliflag.PreferredTLSCipherNames` + `InsecureTLSCipherNames`, the lists the
/// `--tls-cipher-suites` help prints.
pub fn tls_cipher_names() -> (Vec<String>, Vec<String>) {
    let preferred: BTreeSet<String> = preferred_ciphers()
        .iter()
        .map(|(n, _)| n.to_string())
        .collect();
    let insecure: BTreeSet<String> = INSECURE_CIPHERS.iter().map(|s| s.to_string()).collect();
    (
        preferred.into_iter().collect(),
        insecure.into_iter().collect(),
    )
}

/// Where the serving certificate comes from (`c.Cert`): files that are
/// re-read when they change (`NewDynamicServingContentFromFiles`), or a fixed
/// in-memory pair (`NewStaticCertKeyContent`).
#[derive(Clone)]
pub enum ServingCert {
    Files { cert_file: String, key_file: String },
    Static { cert_pem: Vec<u8>, key_pem: Vec<u8> },
}

impl ServingCert {
    /// The certificate chain and private key currently on offer.
    pub(crate) fn load(
        &self,
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
        let (cert_pem, key_pem) = match self {
            ServingCert::Files {
                cert_file,
                key_file,
            } => (
                std::fs::read(cert_file).map_err(|e| format!("open {cert_file}: {e}"))?,
                std::fs::read(key_file).map_err(|e| format!("open {key_file}: {e}"))?,
            ),
            ServingCert::Static {
                cert_pem, key_pem, ..
            } => (cert_pem.clone(), key_pem.clone()),
        };
        let certs = rustls_pemfile::certs(&mut &cert_pem[..])
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("unable to parse the serving certificate: {e}"))?;
        if certs.is_empty() {
            return Err("the serving certificate holds no certificate".to_string());
        }
        let key = rustls_pemfile::private_key(&mut &key_pem[..])
            .map_err(|e| format!("unable to parse the serving key: {e}"))?
            .ok_or_else(|| "the serving key holds no private key".to_string())?;
        Ok((certs, key))
    }

    fn fingerprint(&self, h: &mut impl Hasher) {
        match self {
            ServingCert::Files {
                cert_file,
                key_file,
            } => {
                std::fs::read(cert_file).ok().hash(h);
                std::fs::read(key_file).ok().hash(h);
            }
            ServingCert::Static { .. } => {}
        }
    }
}

/// `server.SecureServingInfo`.
pub struct SecureServingInfo {
    pub listener: std::net::TcpListener,
    pub cert: Option<ServingCert>,
    pub cipher_suites: Vec<SupportedCipherSuite>,
    pub min_tls_version: TlsVersion,
    pub disable_http2: bool,
    /// The union of every client CA (`ClientCA`): the TLS layer requests a
    /// client certificate and lists these as acceptable CAs.
    pub client_cas: Vec<Arc<CaContent>>,
}

impl SecureServingInfo {
    /// The address the listener is bound to.
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// `ApplyClientCert` (config.go): add a client CA to the union.
    pub fn apply_client_cert(&mut self, ca: Arc<CaContent>) {
        self.client_cas.push(ca);
    }

    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        if let Some(c) = &self.cert {
            c.fingerprint(&mut h);
        }
        for ca in &self.client_cas {
            ca.current_pem().hash(&mut h);
        }
        h.finish()
    }
}

/// `tls.RequestClientCert` as a rustls verifier: offer client auth, never
/// require it, never verify the chain (see the module docs), and hint the
/// union of the client CAs' subjects.
#[derive(Debug)]
struct RequestClientCertVerifier {
    hints: Vec<DistinguishedName>,
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl ClientCertVerifier for RequestClientCertVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        false
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &self.hints
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// `tlsConfig` (secure_serving.go:44-100): the rustls server config.
pub fn build_server_config(info: &SecureServingInfo) -> Result<Arc<rustls::ServerConfig>, String> {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    // `tlsConfig.CipherSuites` only constrains TLS 1.2 in Go; the TLS 1.3
    // suites are not configurable.
    if !info.cipher_suites.is_empty() {
        let mut suites: Vec<SupportedCipherSuite> = provider
            .cipher_suites
            .iter()
            .copied()
            .filter(|s| matches!(s, SupportedCipherSuite::Tls13(_)))
            .collect();
        for s in &info.cipher_suites {
            if matches!(s, SupportedCipherSuite::Tls12(_)) && !suites.contains(s) {
                suites.push(*s);
            }
        }
        provider.cipher_suites = suites;
    }
    let algorithms = provider.signature_verification_algorithms;
    let versions: &[&'static rustls::SupportedProtocolVersion] = match info.min_tls_version {
        TlsVersion::Tls12 => &[&rustls::version::TLS12, &rustls::version::TLS13],
        TlsVersion::Tls13 => &[&rustls::version::TLS13],
    };
    let builder = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(versions)
        .map_err(|e| format!("unable to build the TLS configuration: {e}"))?;
    let builder = if info.client_cas.is_empty() {
        builder.with_no_client_auth()
    } else {
        let mut hints = vec![];
        for ca in &info.client_cas {
            if let Some(v) = ca.verifier() {
                for h in v.root_hint_subjects() {
                    if !hints
                        .iter()
                        .any(|x: &DistinguishedName| x.as_ref() == h.as_ref())
                    {
                        hints.push(h.clone());
                    }
                }
            }
        }
        builder.with_client_cert_verifier(Arc::new(RequestClientCertVerifier { hints, algorithms }))
    };
    let cert = info
        .cert
        .as_ref()
        .ok_or_else(|| "no serving certificate configured".to_string())?;
    let (certs, key) = cert.load()?;
    let mut config = builder
        .with_single_cert(certs, key)
        .map_err(|e| format!("invalid serving certificate: {e}"))?;
    // enable HTTP2 for go's 1.7 HTTP Server
    config.alpn_protocols = if info.disable_http2 {
        tracing::info!("Forcing use of http/1.1 only");
        vec![b"http/1.1".to_vec()]
    } else {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    };
    Ok(Arc::new(config))
}

/// A TLS acceptor that injects the verified client-cert chain as a
/// [`crate::PeerCertificates`] request extension (the equivalent of
/// `req.TLS.PeerCertificates`).
#[derive(Clone)]
struct PeerCertAcceptor {
    inner: RustlsAcceptor,
}

impl<I, S> Accept<I, S> for PeerCertAcceptor
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Send + 'static,
{
    type Stream = <RustlsAcceptor as Accept<I, S>>::Stream;
    type Service = <Extension<crate::PeerCertificates> as Layer<S>>::Service;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move {
            let (tls_stream, service) = inner.accept(stream, service).await?;
            let chain: Vec<Vec<u8>> = tls_stream
                .get_ref()
                .1
                .peer_certificates()
                .map(|certs| certs.iter().map(|c| c.as_ref().to_vec()).collect())
                .unwrap_or_default();
            let service = Extension(crate::PeerCertificates(Arc::new(chain))).layer(service);
            Ok((tls_stream, service))
        })
    }
}

/// Handle on a running secure server (`stoppedCh` / `listenerStoppedCh`).
pub struct ServerHandle {
    pub handle: axum_server::Handle,
    pub task: tokio::task::JoinHandle<std::io::Result<()>>,
}

/// `SecureServingInfo.Serve` (secure_serving.go:~ 150): serve `handler` over
/// TLS until the handle is shut down. Every request carries its client
/// certificate chain as [`crate::PeerCertificates`]; the serving certificate
/// and the client CA hints are reloaded when they change.
pub fn serve(handler: Router, info: SecureServingInfo) -> Result<ServerHandle, String> {
    let config = build_server_config(&info)?;
    let rustls_config = RustlsConfig::from_config(config);
    info.listener
        .set_nonblocking(true)
        .map_err(|e| format!("unable to set the listener non-blocking: {e}"))?;
    let handle = axum_server::Handle::new();
    let server = axum_server::from_tcp(
        info.listener
            .try_clone()
            .map_err(|e| format!("unable to clone the listener: {e}"))?,
    )
    .acceptor(PeerCertAcceptor {
        inner: RustlsAcceptor::new(rustls_config.clone()),
    })
    .handle(handle.clone());
    let reload_config = rustls_config.clone();
    let task = tokio::spawn(async move {
        let reload = async move {
            let mut last = info.fingerprint();
            loop {
                tokio::time::sleep(RELOAD_INTERVAL).await;
                let now = info.fingerprint();
                if now == last {
                    continue;
                }
                match build_server_config(&info) {
                    Ok(c) => {
                        reload_config.reload_from_config(c);
                        last = now;
                        tracing::info!("Reloaded the secure serving TLS configuration");
                    }
                    Err(e) => tracing::error!("Failed to reload the TLS configuration: {e}"),
                }
            }
        };
        tokio::select! {
            r = server.serve(handler.into_make_service()) => r,
            _ = reload => Ok(()),
        }
    });
    Ok(ServerHandle { handle, task })
}
