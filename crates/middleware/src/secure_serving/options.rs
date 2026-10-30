//! `SecureServingOptions`, `DelegatingAuthenticationOptions` and
//! `DelegatingAuthorizationOptions`: the flags, defaults, validation and
//! `ApplyTo` of the three option groups the controller-manager and scheduler
//! embed.
//!
//! Upstream sources (release-1.35, under `staging/src/k8s.io/apiserver/pkg/server/options/`):
//! * `serving.go:41-391` (`SecureServingOptions`, `AddFlags`, `ApplyTo`,
//!   `MaybeDefaultWithSelfSignedCerts`)
//! * `authentication.go:46-497` (`DelegatingAuthenticationOptions`,
//!   `RequestHeaderAuthenticationOptions`, `getClient`, `ApplyTo`)
//! * `authorization.go:38-243` (`DelegatingAuthorizationOptions`)
//! * `authentication_dynamic_request_header.go` and
//!   `authentication/request/headerrequest/requestheader_controller.go:197-317`
//!   (the in-cluster request-header lookup)
//! * `server/dynamiccertificates/{dynamic_cafile_content.go,configmap_cafile_content.go}`
//!
//! Not offered (see the module docs of [`super::serve`]): `--tls-sni-cert-key`,
//! `--http2-max-streams-per-connection`, `--permit-port-sharing`,
//! `--permit-address-sharing`.
//!
//! Deliberate reduction: the in-cluster CA / request-header configmap is
//! re-polled every [`CONFIGMAP_REFRESH_DURATION`] instead of being
//! informer-driven.

use super::authn::{
    AnonymousAuthConfig, CaContent, DelegatingAuthenticatorConfig, RequestAuthenticator,
    RequestHeaderConfig, RequestHeaderSettings,
};
use super::authz::{new_path_authorizer, DelegatingAuthorizerConfig};
use super::serve::{tls_cipher_suites, tls_version, SecureServingInfo, ServingCert};
use super::webhook::{ApiClientReviewClient, Backoff, ReviewClient, ReviewError};
use async_trait::async_trait;
use rusternetes_common::authz::{Authorizer, PrivilegedGroupAuthorizer, UnionAuthorizer};
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// `FileRefreshDuration` (dynamic_cafile_content.go): how often a CA file is re-read.
pub const FILE_REFRESH_DURATION: Duration = Duration::from_secs(60);

/// How often the in-cluster configmap is re-read (upstream watches it).
pub const CONFIGMAP_REFRESH_DURATION: Duration = Duration::from_secs(10);

/// `authenticationConfigMapNamespace` / `authenticationConfigMapName` (authentication.go:427-434).
pub const AUTHENTICATION_CONFIGMAP_NAMESPACE: &str = "kube-system";
pub const AUTHENTICATION_CONFIGMAP_NAME: &str = "extension-apiserver-authentication";

/// `RemoteRequestHeaderUID` is beta and on by default since 1.33
/// (`apiserver/pkg/features/kube_features.go:417-420`).
const REMOTE_REQUEST_HEADER_UID: bool = true;

// ------------------------------------------------------------- secure serving

/// `CertKey` (serving.go:83-88).
#[derive(Debug, Clone, Default)]
pub struct CertKey {
    pub cert_file: String,
    pub key_file: String,
}

/// `GeneratableKeyCert` (serving.go:90-110).
#[derive(Debug, Clone, Default)]
pub struct GeneratableKeyCert {
    pub cert_key: CertKey,
    pub cert_directory: String,
    pub pair_name: String,
    /// `GeneratedCert`: an in-memory `(cert PEM, key PEM)`.
    pub generated_cert: Option<(Vec<u8>, Vec<u8>)>,
    pub fixture_directory: String,
}

/// `SecureServingOptions` (serving.go:41-81).
pub struct SecureServingOptions {
    pub bind_address: IpAddr,
    /// May be 0 (disabled) or, with `listener`, ignored. `int` upstream.
    pub bind_port: i32,
    pub required: bool,
    pub listener: Option<std::net::TcpListener>,
    pub server_cert: GeneratableKeyCert,
    pub cipher_suites: Vec<String>,
    pub min_tls_version: String,
    pub disable_http2_serving: bool,
}

impl SecureServingOptions {
    /// `NewSecureServingOptions` (serving.go:112-121).
    pub fn new() -> Self {
        Self {
            bind_address: IpAddr::from([0, 0, 0, 0]),
            bind_port: 443,
            required: false,
            listener: None,
            server_cert: GeneratableKeyCert {
                pair_name: "apiserver".to_string(),
                cert_directory: "apiserver.local.config/certificates".to_string(),
                ..Default::default()
            },
            cipher_suites: vec![],
            min_tls_version: String::new(),
            disable_http2_serving: false,
        }
    }

    /// `Validate` (serving.go:130-148); the messages are upstream's.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = vec![];
        // serving.go:137 parses as `(Required && BindPort < 1) || BindPort > 65535`.
        if (self.required && self.bind_port < 1) || self.bind_port > 65535 {
            errors.push(format!(
                "--secure-port {} must be between 1 and 65535, inclusive. It cannot be turned off with 0",
                self.bind_port
            ));
        } else if self.bind_port < 0 || self.bind_port > 65535 {
            errors.push(format!(
                "--secure-port {} must be between 0 and 65535, inclusive. 0 for turning off secure port",
                self.bind_port
            ));
        }
        if (!self.server_cert.cert_key.cert_file.is_empty()
            || !self.server_cert.cert_key.key_file.is_empty())
            && self.server_cert.generated_cert.is_some()
        {
            errors.push("cert/key file and in-memory certificate cannot both be set".to_string());
        }
        errors
    }

    /// `ApplyTo` (serving.go:224-338): bind the listener and resolve the
    /// certificate, cipher suites and TLS version. `None` when serving is off.
    pub fn apply_to(&mut self) -> Result<Option<SecureServingInfo>, String> {
        if self.bind_port <= 0 && self.listener.is_none() {
            return Ok(None);
        }
        let listener = match self.listener.as_ref() {
            None => {
                let addr = std::net::SocketAddr::new(self.bind_address, self.bind_port as u16);
                let l = std::net::TcpListener::bind(addr)
                    .map_err(|e| format!("failed to create listener: {e}"))?;
                self.bind_port = l
                    .local_addr()
                    .map_err(|e| format!("failed to create listener: {e}"))?
                    .port() as i32;
                l
            }
            Some(l) => {
                let addr = l
                    .local_addr()
                    .map_err(|_| "failed to parse ip and port from listener".to_string())?;
                self.bind_port = addr.port() as i32;
                self.bind_address = addr.ip();
                l.try_clone()
                    .map_err(|e| format!("failed to clone the listener: {e}"))?
            }
        };
        // Keep one handle, as `s.Listener` stays set upstream.
        let info_listener = listener
            .try_clone()
            .map_err(|e| format!("failed to clone the listener: {e}"))?;
        self.listener = Some(listener);

        let key_cert = &self.server_cert.cert_key;
        let cert = if !key_cert.cert_file.is_empty() || !key_cert.key_file.is_empty() {
            let c = ServingCert::Files {
                cert_file: key_cert.cert_file.clone(),
                key_file: key_cert.key_file.clone(),
            };
            // NewDynamicServingContentFromFiles loads the pair up front.
            c.load()?;
            Some(c)
        } else {
            self.server_cert
                .generated_cert
                .as_ref()
                .map(|(cert_pem, key_pem)| ServingCert::Static {
                    cert_pem: cert_pem.clone(),
                    key_pem: key_pem.clone(),
                })
        };

        let cipher_suites = if self.cipher_suites.is_empty() {
            vec![]
        } else {
            tls_cipher_suites(&self.cipher_suites)?
        };
        let min_tls_version = tls_version(&self.min_tls_version)?;

        Ok(Some(SecureServingInfo {
            listener: info_listener,
            cert,
            cipher_suites,
            min_tls_version,
            disable_http2: self.disable_http2_serving,
            client_cas: vec![],
        }))
    }

    /// `MaybeDefaultWithSelfSignedCerts` (serving.go:340-391).
    pub fn maybe_default_with_self_signed_certs(
        &mut self,
        public_address: &str,
        alternate_dns: &[String],
        alternate_ips: &[IpAddr],
    ) -> Result<(), String> {
        if self.bind_port == 0 && self.listener.is_none() {
            return Ok(());
        }
        if !self.server_cert.cert_key.cert_file.is_empty()
            || !self.server_cert.cert_key.key_file.is_empty()
        {
            return Ok(());
        }

        let mut can_read_cert_and_key = false;
        if !self.server_cert.cert_directory.is_empty() {
            if self.server_cert.pair_name.is_empty() {
                return Err("PairName is required if CertDirectory is set".to_string());
            }
            let dir = std::path::Path::new(&self.server_cert.cert_directory);
            self.server_cert.cert_key.cert_file = dir
                .join(format!("{}.crt", self.server_cert.pair_name))
                .to_string_lossy()
                .into_owned();
            self.server_cert.cert_key.key_file = dir
                .join(format!("{}.key", self.server_cert.pair_name))
                .to_string_lossy()
                .into_owned();
            can_read_cert_and_key = rusternetes_common::self_signed::can_read_cert_and_key(
                &self.server_cert.cert_key.cert_file,
                &self.server_cert.cert_key.key_file,
            )?;
        }

        if !can_read_cert_and_key {
            // add either the bind address or localhost to the valid alternates
            let mut alternate_dns = alternate_dns.to_vec();
            let mut alternate_ips = alternate_ips.to_vec();
            if self.bind_address.is_unspecified() {
                alternate_dns.push("localhost".to_string());
            } else {
                alternate_ips.push(self.bind_address);
            }

            let (cert, key) =
                rusternetes_common::self_signed::generate_self_signed_cert_key_with_fixtures(
                    public_address,
                    &alternate_ips,
                    &alternate_dns,
                    &self.server_cert.fixture_directory,
                )
                .map_err(|e| format!("unable to generate self signed cert: {e}"))?;
            let kc = &self.server_cert.cert_key;
            if !kc.cert_file.is_empty() && !kc.key_file.is_empty() {
                write_cert(&kc.cert_file, &cert)?;
                write_key(&kc.key_file, &key)?;
                tracing::info!(
                    "Generated self-signed cert ({}, {})",
                    kc.cert_file,
                    kc.key_file
                );
            } else {
                self.server_cert.generated_cert = Some((cert, key));
                tracing::info!("Generated self-signed cert in-memory");
            }
        }
        Ok(())
    }
}

/// `certutil.WriteCert` (client-go `util/cert/io.go`): `MkdirAll(dir, 0755)`, mode 0644.
fn write_cert(path: &str, data: &[u8]) -> Result<(), String> {
    use std::os::unix::fs::OpenOptionsExt;
    if let Some(dir) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    use std::io::Write;
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(path)
        .and_then(|mut f| f.write_all(data))
        .map_err(|e| e.to_string())
}

/// `keyutil.WriteKey` (client-go `util/keyutil/key.go`): `MkdirAll(dir, 0755)`, mode 0600.
fn write_key(path: &str, data: &[u8]) -> Result<(), String> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    if let Some(dir) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .and_then(|mut f| f.write_all(data))
        .map_err(|e| e.to_string())
}

impl Default for SecureServingOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// pflag `StringSlice` semantics: an empty value is the empty list.
fn string_slice(v: &[String]) -> Option<Vec<String>> {
    if v.is_empty() {
        return None;
    }
    if v.len() == 1 && v[0].is_empty() {
        return Some(vec![]);
    }
    Some(v.to_vec())
}

/// The `--bind-address` ... `--tls-min-version` flags (serving.go:150-221).
/// Unset flags keep the component's defaults.
#[derive(clap::Args, Debug, Clone, Default)]
pub struct SecureServingFlags {
    /// The IP address on which to listen for the --secure-port port. The
    /// associated interface(s) must be reachable by the rest of the cluster, and
    /// by CLI/web clients. If blank or an unspecified address (0.0.0.0 or ::),
    /// all interfaces and IP address families will be used.
    #[arg(long = "bind-address")]
    pub bind_address: Option<IpAddr>,

    /// The port on which to serve HTTPS with authentication and authorization.
    /// If 0, don't serve HTTPS at all.
    #[arg(long = "secure-port", allow_negative_numbers = true)]
    pub secure_port: Option<i32>,

    /// If true, HTTP2 serving will be disabled [default=false]
    #[arg(long = "disable-http2-serving", num_args = 0..=1, default_missing_value = "true")]
    pub disable_http2_serving: Option<bool>,

    /// The directory where the TLS certs are located. If --tls-cert-file and
    /// --tls-private-key-file are provided, this flag will be ignored.
    #[arg(long = "cert-dir")]
    pub cert_dir: Option<String>,

    /// File containing the default x509 Certificate for HTTPS. (CA cert, if any,
    /// concatenated after server cert). If HTTPS serving is enabled, and
    /// --tls-cert-file and --tls-private-key-file are not provided, a
    /// self-signed certificate and key are generated for the public address and
    /// saved to the directory specified by --cert-dir.
    #[arg(long = "tls-cert-file")]
    pub tls_cert_file: Option<String>,

    /// File containing the default x509 private key matching --tls-cert-file.
    #[arg(long = "tls-private-key-file")]
    pub tls_private_key_file: Option<String>,

    /// Comma-separated list of cipher suites for the server. If omitted, the
    /// default cipher suites will be used.
    #[arg(long = "tls-cipher-suites", value_delimiter = ',')]
    pub tls_cipher_suites: Vec<String>,

    /// Minimum TLS version supported. Possible values: VersionTLS10,
    /// VersionTLS11, VersionTLS12, VersionTLS13
    #[arg(long = "tls-min-version")]
    pub tls_min_version: Option<String>,
}

impl SecureServingFlags {
    /// What `fs.Parse` does to the options the flags were registered on.
    pub fn apply_to_options(&self, o: &mut SecureServingOptions) {
        if let Some(v) = self.bind_address {
            o.bind_address = v;
        }
        if let Some(v) = self.secure_port {
            o.bind_port = v;
        }
        if let Some(v) = self.disable_http2_serving {
            o.disable_http2_serving = v;
        }
        if let Some(v) = &self.cert_dir {
            o.server_cert.cert_directory = v.clone();
        }
        if let Some(v) = &self.tls_cert_file {
            o.server_cert.cert_key.cert_file = v.clone();
        }
        if let Some(v) = &self.tls_private_key_file {
            o.server_cert.cert_key.key_file = v.clone();
        }
        if let Some(v) = string_slice(&self.tls_cipher_suites) {
            o.cipher_suites = v;
        }
        if let Some(v) = &self.tls_min_version {
            o.min_tls_version = v.clone();
        }
    }
}

// ------------------------------------------------------------ authentication

/// `RequestHeaderAuthenticationOptions` (authentication.go:55-65).
#[derive(Debug, Clone, Default)]
pub struct RequestHeaderAuthenticationOptions {
    pub client_ca_file: String,
    pub username_headers: Vec<String>,
    pub uid_headers: Vec<String>,
    pub group_headers: Vec<String>,
    pub extra_header_prefixes: Vec<String>,
    pub allowed_names: Vec<String>,
}

fn check_for_white_space_only(flag: &str, names: &[String]) -> Option<String> {
    names
        .iter()
        .any(|n| n.trim().is_empty())
        .then(|| format!("empty value in {flag:?}"))
}

fn case_insensitive_has(headers: &[String], header: &str) -> bool {
    headers.iter().any(|h| h.eq_ignore_ascii_case(header))
}

impl RequestHeaderAuthenticationOptions {
    /// `Validate` (authentication.go:67-108). `remote_request_header_uid` is
    /// the `RemoteRequestHeaderUID` feature gate.
    pub fn validate(&self, remote_request_header_uid: bool) -> Vec<String> {
        let mut all = vec![];
        for (flag, v) in [
            ("requestheader-username-headers", &self.username_headers),
            ("requestheader-group-headers", &self.group_headers),
            (
                "requestheader-extra-headers-prefix",
                &self.extra_header_prefixes,
            ),
            ("requestheader-allowed-names", &self.allowed_names),
        ] {
            all.extend(check_for_white_space_only(flag, v));
        }
        if !self.username_headers.is_empty()
            && !case_insensitive_has(&self.username_headers, "X-Remote-User")
        {
            tracing::warn!("--requestheader-username-headers is set without specifying the standard X-Remote-User header - API aggregation will not work");
        }
        if !self.group_headers.is_empty()
            && !case_insensitive_has(&self.group_headers, "X-Remote-Group")
        {
            tracing::warn!("--requestheader-group-headers is set without specifying the standard X-Remote-Group header - API aggregation will not work");
        }
        if !self.extra_header_prefixes.is_empty()
            && !case_insensitive_has(&self.extra_header_prefixes, "X-Remote-Extra-")
        {
            tracing::warn!("--requestheader-extra-headers-prefix is set without specifying the standard X-Remote-Extra- header prefix - API aggregation will not work");
        }
        if !remote_request_header_uid {
            if !self.uid_headers.is_empty() {
                all.push("--requestheader-uid-headers requires the \"RemoteRequestHeaderUID\" feature to be enabled".to_string());
            }
        } else {
            all.extend(check_for_white_space_only(
                "requestheader-uid-headers",
                &self.uid_headers,
            ));
            if !self.uid_headers.is_empty()
                && !case_insensitive_has(&self.uid_headers, "X-Remote-Uid")
            {
                // this was added later and so we are able to error out
                all.push("--requestheader-uid-headers is set without specifying the standard X-Remote-Uid header - API aggregation will not work".to_string());
            }
        }
        all
    }

    /// `ToAuthenticationRequestHeaderConfig` (authentication.go:159-177).
    pub fn to_authentication_request_header_config(
        &self,
    ) -> Result<Option<RequestHeaderConfig>, String> {
        if self.client_ca_file.is_empty() {
            return Ok(None);
        }
        let ca = CaContent::from_file("request-header", &self.client_ca_file)?;
        ca.run_file_refresh(self.client_ca_file.clone(), FILE_REFRESH_DURATION);
        Ok(Some(RequestHeaderConfig {
            ca,
            settings: Arc::new(RwLock::new(RequestHeaderSettings {
                username_headers: self.username_headers.clone(),
                uid_headers: self.uid_headers.clone(),
                group_headers: self.group_headers.clone(),
                extra_header_prefixes: self.extra_header_prefixes.clone(),
                allowed_client_names: self.allowed_names.clone(),
            })),
        }))
    }
}

/// `DelegatingAuthenticationOptions` (authentication.go:214-249).
pub struct DelegatingAuthenticationOptions {
    pub remote_kubeconfig_file: String,
    pub remote_kubeconfig_file_optional: bool,
    pub cache_ttl: Duration,
    /// `ClientCert.ClientCA`.
    pub client_ca: String,
    pub request_header: RequestHeaderAuthenticationOptions,
    pub skip_in_cluster_lookup: bool,
    pub tolerate_in_cluster_lookup_failure: bool,
    pub webhook_retry_backoff: Option<Backoff>,
    pub token_request_timeout: Duration,
    /// Set by `NewDelegatingAuthenticationOptions`; `ApplyTo` does not read it
    /// (it hardcodes `Enabled: true`, authentication.go:337).
    pub anonymous: Option<AnonymousAuthConfig>,
}

/// `server.AuthenticationInfo`.
pub struct AuthenticationInfo {
    pub authenticator: Option<Arc<dyn RequestAuthenticator>>,
    pub request_header_config: Option<RequestHeaderConfig>,
}

impl DelegatingAuthenticationOptions {
    /// `NewDelegatingAuthenticationOptions` (authentication.go:251-271).
    pub fn new() -> Self {
        Self {
            // very low for responsiveness, but high enough to handle storms
            cache_ttl: Duration::from_secs(10),
            client_ca: String::new(),
            request_header: RequestHeaderAuthenticationOptions {
                username_headers: vec!["x-remote-user".to_string()],
                // UID headers are deliberately not defaulted (authentication.go:257-262)
                uid_headers: vec![],
                group_headers: vec!["x-remote-group".to_string()],
                extra_header_prefixes: vec!["x-remote-extra-".to_string()],
                ..Default::default()
            },
            webhook_retry_backoff: Some(Backoff::default_auth_webhook()),
            token_request_timeout: Duration::from_secs(10),
            anonymous: Some(AnonymousAuthConfig {
                enabled: true,
                condition_paths: vec![],
            }),
            remote_kubeconfig_file: String::new(),
            remote_kubeconfig_file_optional: false,
            skip_in_cluster_lookup: false,
            tolerate_in_cluster_lookup_failure: false,
        }
    }

    /// `Validate` (authentication.go:288-301).
    pub fn validate(&self) -> Vec<String> {
        let mut all = self.request_header.validate(REMOTE_REQUEST_HEADER_UID);
        if let Some(b) = &self.webhook_retry_backoff {
            if b.steps <= 0 {
                all.push(format!(
                    "number of webhook retry attempts must be greater than 1, but is: {}",
                    b.steps
                ));
            }
        }
        all
    }

    /// `ApplyTo` (authentication.go:330-425). `serving` receives the client CAs
    /// (`authenticationInfo.ApplyClientCert`).
    pub async fn apply_to(
        &self,
        mut serving: Option<&mut SecureServingInfo>,
    ) -> Result<AuthenticationInfo, String> {
        let mut info = AuthenticationInfo {
            authenticator: None,
            request_header_config: None,
        };
        let mut cfg = DelegatingAuthenticatorConfig {
            anonymous: Some(AnonymousAuthConfig {
                enabled: true,
                condition_paths: vec![],
            }),
            token_access_review_client: None,
            token_access_review_timeout: self.token_request_timeout,
            webhook_retry_backoff: self.webhook_retry_backoff.clone(),
            cache_ttl: self.cache_ttl,
            client_certificate_ca: None,
            request_header_config: None,
        };

        let client = get_client(
            &self.remote_kubeconfig_file,
            self.remote_kubeconfig_file_optional,
            "authentication",
        )
        .map_err(|e| format!("failed to get delegated authentication kubeconfig: {e}"))?;

        // configure token review
        if let Some(c) = &client {
            cfg.token_access_review_client = Some(Arc::new(ApiClientReviewClient(c.clone())));
        }

        // get the clientCA information
        if !self.client_ca.is_empty() {
            let ca = CaContent::from_file("client-ca-bundle", &self.client_ca)
                .map_err(|e| format!("unable to load client CA provider: {e}"))?;
            ca.run_file_refresh(self.client_ca.clone(), FILE_REFRESH_DURATION);
            cfg.client_certificate_ca = Some(ca.clone());
            apply_client_cert(serving.as_deref_mut(), ca);
        } else if !self.skip_in_cluster_lookup {
            match &client {
                None => tracing::warn!(
                    "No authentication-kubeconfig provided in order to lookup client-ca-file in configmap/{} in {}, so client certificate authentication won't work.",
                    AUTHENTICATION_CONFIGMAP_NAME,
                    AUTHENTICATION_CONFIGMAP_NAMESPACE
                ),
                Some(c) => {
                    // NewDynamicCAFromConfigMapController: loaded by RunOnce when
                    // the server starts; a failure is ignored ("fail closed").
                    let ca = CaContent::new("client-ca");
                    spawn_configmap_refresh(c.clone(), &ca, "client-ca-file", None, true);
                    cfg.client_certificate_ca = Some(ca.clone());
                    apply_client_cert(serving.as_deref_mut(), ca);
                }
            }
        }

        let mut request_header_config: Option<RequestHeaderConfig> = None;
        if !self.request_header.client_ca_file.is_empty() {
            request_header_config = self
                .request_header
                .to_authentication_request_header_config()
                .map_err(|e| {
                    format!("unable to create request header authentication config: {e}")
                })?;
        } else if !self.skip_in_cluster_lookup {
            match &client {
                None => tracing::warn!(
                    "No authentication-kubeconfig provided in order to lookup requestheader-client-ca-file in configmap/{} in {}, so request-header client certificate authentication won't work.",
                    AUTHENTICATION_CONFIGMAP_NAME,
                    AUTHENTICATION_CONFIGMAP_NAMESPACE
                ),
                Some(c) => match create_request_header_config(c.clone()).await {
                    Ok(rh) => request_header_config = Some(rh),
                    Err(e) => {
                        if self.tolerate_in_cluster_lookup_failure {
                            tracing::warn!(
                                "Error looking up in-cluster authentication configuration: {e}"
                            );
                            tracing::warn!("Continuing without authentication configuration. This may treat all requests as anonymous.");
                            tracing::warn!("To require authentication configuration lookup to succeed, set --authentication-tolerate-lookup-failure=false");
                        } else {
                            return Err(format!(
                                "unable to load configmap based request-header-client-ca-file: {e}"
                            ));
                        }
                    }
                },
            }
        }
        if let Some(rh) = request_header_config {
            cfg.request_header_config = Some(rh.clone());
            apply_client_cert(serving, rh.ca.clone());
            info.request_header_config = Some(rh);
        }

        // create authenticator
        info.authenticator = Some(cfg.new()?);
        Ok(info)
    }
}

/// `AuthenticationInfo.ApplyClientCert` (config.go:535-549).
fn apply_client_cert(serving: Option<&mut SecureServingInfo>, ca: Arc<CaContent>) {
    if let Some(s) = serving {
        s.apply_client_cert(ca);
    }
}

impl Default for DelegatingAuthenticationOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// `deserializeStrings` (requestheader_controller.go): a JSON array, or nil.
fn deserialize_strings(v: Option<&serde_json::Value>) -> Result<Vec<String>, String> {
    match v.and_then(|v| v.as_str()) {
        None | Some("") => Ok(vec![]),
        Some(s) => serde_json::from_str(s).map_err(|e| e.to_string()),
    }
}

/// `getRequestHeaderBundleFromConfigMap` (requestheader_controller.go:283-317).
fn request_header_settings_from_configmap(
    cm: &serde_json::Value,
    previous_allowed: &RequestHeaderSettings,
) -> Result<RequestHeaderSettings, String> {
    let _ = previous_allowed;
    let data = &cm["data"];
    Ok(RequestHeaderSettings {
        username_headers: deserialize_strings(data.get("requestheader-username-headers"))?,
        uid_headers: deserialize_strings(data.get("requestheader-uid-headers"))?,
        group_headers: deserialize_strings(data.get("requestheader-group-headers"))?,
        extra_header_prefixes: deserialize_strings(data.get("requestheader-extra-headers-prefix"))?,
        allowed_client_names: deserialize_strings(data.get("requestheader-allowed-names"))?,
    })
}

async fn get_authentication_configmap(
    client: &rusternetes_client::http::ApiClient,
) -> Result<Option<serde_json::Value>, String> {
    let path = format!(
        "/api/v1/namespaces/{AUTHENTICATION_CONFIGMAP_NAMESPACE}/configmaps/{AUTHENTICATION_CONFIGMAP_NAME}"
    );
    match client.get::<serde_json::Value>(&path).await {
        Ok(v) => Ok(Some(v)),
        Err(rusternetes_client::http::GetError::NotFound) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

/// `ConfigMapCAController.loadCABundle` (configmap_cafile_content.go:145-171).
fn load_ca_bundle(ca: &CaContent, cm: Option<&serde_json::Value>, key: &str) -> Result<(), String> {
    let cm = cm.ok_or_else(|| format!("configmap {AUTHENTICATION_CONFIGMAP_NAME:?} not found"))?;
    let bundle = cm["data"][key].as_str().unwrap_or("");
    if bundle.is_empty() {
        return Err(format!("missing content for CA bundle {:?}", ca.name()));
    }
    ca.set_pem(bundle.as_bytes()).map(|_| ())
}

/// The refresh half of `ConfigMapCAController.Run` /
/// `RequestHeaderAuthRequestController.Run`: re-read the configmap on an
/// interval (upstream also reacts to watch events). The task ends with the CA.
fn spawn_configmap_refresh(
    client: Arc<rusternetes_client::http::ApiClient>,
    ca: &Arc<CaContent>,
    key: &'static str,
    settings: Option<Arc<RwLock<RequestHeaderSettings>>>,
    immediate: bool,
) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    let weak = Arc::downgrade(ca);
    tokio::spawn(async move {
        let mut first = immediate;
        loop {
            if !first {
                tokio::time::sleep(CONFIGMAP_REFRESH_DURATION).await;
            }
            first = false;
            let Some(ca) = weak.upgrade() else { return };
            match get_authentication_configmap(&client).await {
                Ok(cm) => {
                    if let Err(e) = load_ca_bundle(&ca, cm.as_ref(), key) {
                        tracing::error!("Failed to load CA bundle {:?}: {e}", ca.name());
                    }
                    if let (Some(settings), Some(cm)) = (&settings, &cm) {
                        let current = settings.read().unwrap().clone();
                        match request_header_settings_from_configmap(cm, &current) {
                            Ok(new) if new != current => {
                                *settings.write().unwrap() = new;
                                tracing::info!("Loaded a new request header values");
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!("Failed to load the request header values: {e}")
                            }
                        }
                    }
                }
                Err(e) => tracing::error!("Failed to read the authentication configmap: {e}"),
            }
        }
    });
}

/// `createRequestHeaderConfig` (authentication.go:430-452): the synchronous
/// `RunOnce` of the request-header controller decides whether the lookup
/// worked. `ConfigMapCAController.RunOnce` ignores its error; the header
/// controller's `RunOnce` ignores NotFound and fails on anything else.
async fn create_request_header_config(
    client: Arc<rusternetes_client::http::ApiClient>,
) -> Result<RequestHeaderConfig, String> {
    let ca = CaContent::new("client-ca");
    let settings = Arc::new(RwLock::new(RequestHeaderSettings::default()));
    let cm = get_authentication_configmap(&client).await.inspect_err(|e| {
        tracing::warn!(
            "Unable to get configmap/{AUTHENTICATION_CONFIGMAP_NAME} in {AUTHENTICATION_CONFIGMAP_NAMESPACE}: {e}"
        )
    })?;
    // ConfigMapCAController.RunOnce: `_ = c.loadCABundle()`
    let _ = load_ca_bundle(&ca, cm.as_ref(), "requestheader-client-ca-file");
    if let Some(cm) = &cm {
        let new = request_header_settings_from_configmap(cm, &RequestHeaderSettings::default())?;
        *settings.write().unwrap() = new;
    }
    spawn_configmap_refresh(
        client,
        &ca,
        "requestheader-client-ca-file",
        Some(settings.clone()),
        false,
    );
    Ok(RequestHeaderConfig { ca, settings })
}

/// `--authentication-kubeconfig` ... `--requestheader-allowed-names`
/// (authentication.go:129-155, 303-328).
#[derive(clap::Args, Debug, Clone, Default)]
pub struct AuthenticationFlags {
    /// kubeconfig file pointing at the 'core' kubernetes server with enough
    /// rights to create tokenreviews.authentication.k8s.io. This is optional. If
    /// empty, all token requests are considered to be anonymous and no client CA
    /// is looked up in the cluster.
    #[arg(long = "authentication-kubeconfig")]
    pub authentication_kubeconfig: Option<String>,

    /// The duration to cache responses from the webhook token authenticator.
    #[arg(long = "authentication-token-webhook-cache-ttl", value_parser = parse_go_duration_arg)]
    pub authentication_token_webhook_cache_ttl: Option<Duration>,

    /// If set, any request presenting a client certificate signed by one of the
    /// authorities in the client-ca-file is authenticated with an identity
    /// corresponding to the CommonName of the client certificate.
    #[arg(long = "client-ca-file")]
    pub client_ca_file: Option<String>,

    /// List of request headers to inspect for usernames. X-Remote-User is common.
    #[arg(long = "requestheader-username-headers", value_delimiter = ',')]
    pub requestheader_username_headers: Vec<String>,

    /// List of request headers to inspect for UIDs. X-Remote-Uid is suggested.
    /// Requires the RemoteRequestHeaderUID feature to be enabled.
    #[arg(long = "requestheader-uid-headers", value_delimiter = ',')]
    pub requestheader_uid_headers: Vec<String>,

    /// List of request headers to inspect for groups. X-Remote-Group is suggested.
    #[arg(long = "requestheader-group-headers", value_delimiter = ',')]
    pub requestheader_group_headers: Vec<String>,

    /// List of request header prefixes to inspect. X-Remote-Extra- is suggested.
    #[arg(long = "requestheader-extra-headers-prefix", value_delimiter = ',')]
    pub requestheader_extra_headers_prefix: Vec<String>,

    /// Root certificate bundle to use to verify client certificates on incoming
    /// requests before trusting usernames in headers specified by
    /// --requestheader-username-headers. WARNING: generally do not depend on
    /// authorization being already done for incoming requests.
    #[arg(long = "requestheader-client-ca-file")]
    pub requestheader_client_ca_file: Option<String>,

    /// List of client certificate common names to allow to provide usernames in
    /// headers specified by --requestheader-username-headers. If empty, any
    /// client certificate validated by the authorities in
    /// --requestheader-client-ca-file is allowed.
    #[arg(long = "requestheader-allowed-names", value_delimiter = ',')]
    pub requestheader_allowed_names: Vec<String>,

    /// If false, the authentication-kubeconfig will be used to lookup missing
    /// authentication configuration from the cluster.
    #[arg(long = "authentication-skip-lookup", num_args = 0..=1, default_missing_value = "true")]
    pub authentication_skip_lookup: Option<bool>,

    /// If true, failures to look up missing authentication configuration from
    /// the cluster are not considered fatal. Note that this can result in
    /// authentication that treats all requests as anonymous.
    #[arg(long = "authentication-tolerate-lookup-failure", num_args = 0..=1, default_missing_value = "true")]
    pub authentication_tolerate_lookup_failure: Option<bool>,
}

impl AuthenticationFlags {
    pub fn apply_to_options(&self, o: &mut DelegatingAuthenticationOptions) {
        if let Some(v) = &self.authentication_kubeconfig {
            o.remote_kubeconfig_file = v.clone();
        }
        if let Some(v) = self.authentication_token_webhook_cache_ttl {
            o.cache_ttl = v;
        }
        if let Some(v) = &self.client_ca_file {
            o.client_ca = v.clone();
        }
        if let Some(v) = string_slice(&self.requestheader_username_headers) {
            o.request_header.username_headers = v;
        }
        if let Some(v) = string_slice(&self.requestheader_uid_headers) {
            o.request_header.uid_headers = v;
        }
        if let Some(v) = string_slice(&self.requestheader_group_headers) {
            o.request_header.group_headers = v;
        }
        if let Some(v) = string_slice(&self.requestheader_extra_headers_prefix) {
            o.request_header.extra_header_prefixes = v;
        }
        if let Some(v) = &self.requestheader_client_ca_file {
            o.request_header.client_ca_file = v.clone();
        }
        if let Some(v) = string_slice(&self.requestheader_allowed_names) {
            o.request_header.allowed_names = v;
        }
        if let Some(v) = self.authentication_skip_lookup {
            o.skip_in_cluster_lookup = v;
        }
        if let Some(v) = self.authentication_tolerate_lookup_failure {
            o.tolerate_in_cluster_lookup_failure = v;
        }
    }
}

/// A Go `time.Duration` flag value (`24h`, `10s`, `1m30s`).
pub fn parse_go_duration_arg(s: &str) -> Result<Duration, String> {
    let nanos = rusternetes_common::go_duration::parse_go_duration(s)?;
    u64::try_from(nanos)
        .map(Duration::from_nanos)
        .map_err(|_| format!("negative duration {s:?}"))
}

// ------------------------------------------------------------- authorization

/// `DelegatingAuthorizationOptions` (authorization.go:43-76).
pub struct DelegatingAuthorizationOptions {
    pub remote_kubeconfig_file: String,
    pub remote_kubeconfig_file_optional: bool,
    pub allow_cache_ttl: Duration,
    pub deny_cache_ttl: Duration,
    pub always_allow_paths: Vec<String>,
    pub always_allow_groups: Vec<String>,
    pub client_timeout: Duration,
    pub webhook_retry_backoff: Option<Backoff>,
}

/// `server.AuthorizationInfo`.
pub struct AuthorizationInfo {
    pub authorizer: Arc<dyn Authorizer>,
}

/// `clientConfig.Timeout = s.ClientTimeout` (authorization.go:239): every
/// request is cut off after the timeout, as an error that is not retried.
struct TimeoutReviewClient {
    inner: Arc<dyn ReviewClient>,
    timeout: Duration,
}

#[async_trait]
impl ReviewClient for TimeoutReviewClient {
    async fn create(
        &self,
        path: &str,
        review: &serde_json::Value,
    ) -> Result<serde_json::Value, ReviewError> {
        match tokio::time::timeout(self.timeout, self.inner.create(path, review)).await {
            Ok(r) => r,
            Err(_) => Err(ReviewError {
                status_code: 0,
                reason: String::new(),
                message: format!(
                    "Post {path:?}: context deadline exceeded (Client.Timeout exceeded while awaiting headers)"
                ),
                connection_reset: false,
                suggests_client_delay: false,
            }),
        }
    }
}

impl DelegatingAuthorizationOptions {
    /// `NewDelegatingAuthorizationOptions` (authorization.go:78-94).
    pub fn new() -> Self {
        Self {
            // very low for responsiveness, but high enough to handle storms
            allow_cache_ttl: Duration::from_secs(10),
            deny_cache_ttl: Duration::from_secs(10),
            client_timeout: Duration::from_secs(10),
            webhook_retry_backoff: Some(Backoff::default_auth_webhook()),
            // This allows the kubelet to always get health and readiness without causing an authorization check.
            always_allow_paths: vec![
                "/healthz".to_string(),
                "/readyz".to_string(),
                "/livez".to_string(),
            ],
            // system:masters has full authority in a hard-coded authorizer of the delegated server.
            always_allow_groups: vec!["system:masters".to_string()],
            remote_kubeconfig_file: String::new(),
            remote_kubeconfig_file_optional: false,
        }
    }

    /// `Validate` (authorization.go:123-134).
    pub fn validate(&self) -> Vec<String> {
        let mut all = vec![];
        if let Some(b) = &self.webhook_retry_backoff {
            if b.steps <= 0 {
                all.push(format!(
                    "number of webhook retry attempts must be greater than 1, but is: {}",
                    b.steps
                ));
            }
        }
        all
    }

    /// `ApplyTo` (authorization.go:162-175).
    pub async fn apply_to(&self) -> Result<AuthorizationInfo, String> {
        let client = get_client(
            &self.remote_kubeconfig_file,
            self.remote_kubeconfig_file_optional,
            "authorization",
        )?
        .map(|c| -> Arc<dyn ReviewClient> {
            Arc::new(TimeoutReviewClient {
                inner: Arc::new(ApiClientReviewClient(c)),
                timeout: self.client_timeout,
            })
        });
        Ok(AuthorizationInfo {
            authorizer: self.to_authorizer(client)?,
        })
    }

    /// `toAuthorizer` (authorization.go:177-209).
    pub fn to_authorizer(
        &self,
        client: Option<Arc<dyn ReviewClient>>,
    ) -> Result<Arc<dyn Authorizer>, String> {
        let mut authorizers: Vec<Arc<dyn Authorizer>> = vec![];
        if !self.always_allow_groups.is_empty() {
            let groups: Vec<&str> = self
                .always_allow_groups
                .iter()
                .map(|s| s.as_str())
                .collect();
            authorizers.push(Arc::new(PrivilegedGroupAuthorizer::new(&groups)));
        }
        if !self.always_allow_paths.is_empty() {
            authorizers.push(new_path_authorizer(&self.always_allow_paths)?);
        }
        match client {
            None => tracing::warn!(
                "No authorization-kubeconfig provided, so SubjectAccessReview of authorization tokens won't work."
            ),
            Some(c) => authorizers.push(
                DelegatingAuthorizerConfig {
                    subject_access_review_client: c,
                    allow_cache_ttl: self.allow_cache_ttl,
                    deny_cache_ttl: self.deny_cache_ttl,
                    webhook_retry_backoff: self.webhook_retry_backoff.clone(),
                }
                .new()?,
            ),
        }
        Ok(Arc::new(UnionAuthorizer::new(authorizers)))
    }
}

impl Default for DelegatingAuthorizationOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// `--authorization-kubeconfig` ... `--authorization-always-allow-paths`
/// (authorization.go:136-160).
#[derive(clap::Args, Debug, Clone, Default)]
pub struct AuthorizationFlags {
    /// kubeconfig file pointing at the 'core' kubernetes server with enough
    /// rights to create subjectaccessreviews.authorization.k8s.io. This is
    /// optional. If empty, all requests not skipped by authorization are
    /// forbidden.
    #[arg(long = "authorization-kubeconfig")]
    pub authorization_kubeconfig: Option<String>,

    /// The duration to cache 'authorized' responses from the webhook authorizer.
    #[arg(long = "authorization-webhook-cache-authorized-ttl", value_parser = parse_go_duration_arg)]
    pub authorization_webhook_cache_authorized_ttl: Option<Duration>,

    /// The duration to cache 'unauthorized' responses from the webhook authorizer.
    #[arg(long = "authorization-webhook-cache-unauthorized-ttl", value_parser = parse_go_duration_arg)]
    pub authorization_webhook_cache_unauthorized_ttl: Option<Duration>,

    /// A list of HTTP paths to skip during authorization, i.e. these are
    /// authorized without contacting the 'core' kubernetes server.
    #[arg(long = "authorization-always-allow-paths", value_delimiter = ',')]
    pub authorization_always_allow_paths: Vec<String>,
}

impl AuthorizationFlags {
    pub fn apply_to_options(&self, o: &mut DelegatingAuthorizationOptions) {
        if let Some(v) = &self.authorization_kubeconfig {
            o.remote_kubeconfig_file = v.clone();
        }
        if let Some(v) = self.authorization_webhook_cache_authorized_ttl {
            o.allow_cache_ttl = v;
        }
        if let Some(v) = self.authorization_webhook_cache_unauthorized_ttl {
            o.deny_cache_ttl = v;
        }
        if let Some(v) = string_slice(&self.authorization_always_allow_paths) {
            o.always_allow_paths = v;
        }
    }
}

// ------------------------------------------------------------------ clients

/// `getClient` (authentication.go:462-497, authorization.go:211-243): the
/// client for delegated reviews, from `--*-kubeconfig` or the in-cluster
/// config, with `QPS = 200` and `Burst = 400`. `Ok(None)` when it is optional
/// and there is none.
pub fn get_client(
    kubeconfig_file: &str,
    optional: bool,
    what: &str,
) -> Result<Option<Arc<rusternetes_client::http::ApiClient>>, String> {
    use rusternetes_client::http::ApiClient;
    let wrap = |e: String| format!("failed to get delegated {what} kubeconfig: {e}");
    let client = if !kubeconfig_file.is_empty() {
        let cfg = rusternetes_client::kubeconfig::KubeConfig::load_from_file(
            &std::path::PathBuf::from(kubeconfig_file),
        )
        .map_err(|e| wrap(format!("{e:#}")))?;
        let server = cfg.get_server().map_err(|e| wrap(format!("{e:#}")))?;
        ApiClient::with_tls(
            &server,
            cfg.should_skip_tls_verify().unwrap_or(false),
            cfg.get_ca_cert_pem().ok().flatten(),
            cfg.get_client_cert_pem()
                .ok()
                .flatten()
                .map(String::into_bytes),
            cfg.get_client_key_pem()
                .ok()
                .flatten()
                .map(String::into_bytes),
            cfg.get_token().ok().flatten(),
        )
        .map_err(|e| wrap(format!("{e:#}")))?
    } else {
        // without the remote kubeconfig file, try to use the in-cluster config.
        // If it is optional, ignore errors.
        let cc = match rusternetes_client::config::ClientConfig::in_cluster() {
            Ok(cc) => cc,
            Err(e) => {
                if optional {
                    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
                        || std::env::var("KUBERNETES_SERVICE_HOST_OVERRIDE").is_ok()
                    {
                        tracing::warn!(
                            "failed to read in-cluster kubeconfig for delegated {what}: {e:#}"
                        );
                    }
                    return Ok(None);
                }
                return Err(wrap(format!("{e:#}")));
            }
        };
        ApiClient::from_config(&cc).map_err(|e| wrap(format!("{e:#}")))?
    };
    // set high qps/burst limits since this will effectively limit API server responsiveness
    Ok(Some(Arc::new(client.with_rate_limit(200.0, 400.0))))
}
