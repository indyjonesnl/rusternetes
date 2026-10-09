pub mod admission;
pub mod audit;
pub use rusternetes_admission_webhook as admission_webhook;
pub mod apiserver_identity;
pub mod bootstrap;
pub mod legacy_token_tracking;
pub use rusternetes_admission_webhook::cel_evaluators as cel;
pub use rusternetes_middleware::cbor;
pub mod conversion;
pub mod dynamic_routes;
pub mod endpoints;
#[allow(dead_code)]
pub mod flow_control;
pub mod flow_control_filter;
pub mod flow_control_queueset;
pub mod flow_control_work_estimator;
pub mod gnostic;
pub mod handlers;
pub use rusternetes_middleware as middleware;
pub mod openapi;
pub mod patch;
pub mod peer_cert_acceptor;
pub mod post_start_hooks;
pub mod prometheus_client;
pub mod registry;
pub mod storage_readiness_hook;
pub use rusternetes_protobuf as protobuf;
#[allow(dead_code)]
pub mod fieldmanager;
#[allow(dead_code)]
pub mod response;
pub mod router;
#[allow(dead_code)]
pub mod spdy;
pub mod spdy3;
pub mod ssa;
pub mod startup;
pub mod state;
#[allow(dead_code)]
pub mod streaming;
pub mod watch_cache;

use axum_server::tls_rustls::RustlsConfig;
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::RBACAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::tls::TlsConfig;
use rusternetes_storage::StorageBackend;
use state::ApiServerState;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};

/// Resolve the cluster **CA** certificate PEM that gets embedded as `ca.crt` in
/// ServiceAccount token secrets and `kube-root-ca.crt` ConfigMaps.
///
/// This MUST be the issuing CA, not the api-server's serving cert. The two used
/// to be the same self-signed CA:TRUE cert, so handing back the serving cert
/// happened to work; now the serving cert is a leaf (CA:FALSE) signed by a
/// separate CA, and embedding the leaf makes in-cluster clients reject the
/// api-server with `UnknownIssuer`. Prefer an explicit CA file, then the CA
/// sitting next to the serving cert, then the well-known deploy paths; fall back
/// to the serving cert only when no CA file exists (self-signed / legacy setups).
pub fn resolve_ca_cert_pem(cert_file: Option<&str>, serving_cert_pem: &str) -> String {
    let mut candidates: Vec<String> = Vec::new();
    if let Ok(p) = std::env::var("CA_CERT_PATH") {
        candidates.push(p);
    }
    if let Some(cf) = cert_file {
        if let Some(dir) = std::path::Path::new(cf).parent() {
            candidates.push(dir.join("ca.crt").to_string_lossy().into_owned());
        }
    }
    candidates.push("/etc/kubernetes/pki/ca.crt".to_string());
    if let Ok(home) = std::env::var("HOME") {
        candidates.push(format!("{home}/.rusternetes/certs/ca.crt"));
    }

    for path in &candidates {
        if let Ok(pem) = std::fs::read_to_string(path) {
            if pem.contains("BEGIN CERTIFICATE") {
                info!("Loaded cluster CA for SA tokens / kube-root-ca from {path}");
                return pem;
            }
        }
    }
    warn!(
        "No separate CA cert found (tried {:?}); falling back to the serving cert. \
         In-cluster clients using a strict TLS stack may fail to verify the api-server.",
        candidates
    );
    serving_cert_pem.to_string()
}

/// TLS material prepared from [`ApiServerConfig`].
///
/// When a self-signed certificate is generated, this keeps the generated key
/// pair together with the CA PEM derived from that same certificate so embedded
/// components can trust the exact cert the API server will serve.
pub struct PreparedTlsConfig {
    tls_config: TlsConfig,
    ca_cert_pem: Option<String>,
}

impl PreparedTlsConfig {
    pub fn ca_cert_pem(&self) -> Option<&str> {
        self.ca_cert_pem.as_deref()
    }

    pub fn into_tls_config(self) -> TlsConfig {
        self.tls_config
    }
}

/// Load or generate TLS material for an `ApiServerConfig`.
///
/// Returns `None` when TLS is disabled.
pub fn prepare_tls_for_config(
    config: &ApiServerConfig,
) -> anyhow::Result<Option<PreparedTlsConfig>> {
    if !config.tls {
        return Ok(None);
    }
    let tls_config = if let (Some(ref cert_file), Some(ref key_file)) =
        (&config.tls_cert_file, &config.tls_key_file)
    {
        TlsConfig::from_pem_files(cert_file, key_file)?
    } else if config.tls_self_signed {
        let sans: Vec<String> = config
            .tls_san
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        TlsConfig::generate_self_signed("rusternetes-api", sans)?
    } else {
        anyhow::bail!("TLS enabled but no certificate provided");
    };
    let ca_cert_pem = tls_config
        .cert_pem
        .as_deref()
        .map(|serving| resolve_ca_cert_pem(config.tls_cert_file.as_deref(), serving));
    Ok(Some(PreparedTlsConfig {
        tls_config,
        ca_cert_pem,
    }))
}

/// Derive the cluster CA certificate PEM from an `ApiServerConfig` without
/// starting the server.
///
/// Prefer [`prepare_tls_for_config`] when the same generated TLS material will
/// be used to start the API server.
pub fn ca_cert_pem_for_config(config: &ApiServerConfig) -> anyhow::Result<Option<String>> {
    Ok(prepare_tls_for_config(config)?.and_then(|prepared| prepared.ca_cert_pem))
}

/// Configuration for the API server component.
pub struct ApiServerConfig {
    pub bind_address: String,
    pub jwt_secret: String,
    /// `--service-account-key-file` / `--service-account-signing-key-file` /
    /// `--service-account-issuer` / `--api-audiences` (#1575).
    pub service_account: rusternetes_common::auth::ServiceAccountOptions,
    pub tls: bool,
    pub tls_cert_file: Option<String>,
    pub tls_key_file: Option<String>,
    pub tls_self_signed: bool,
    pub tls_san: String,
    pub skip_auth: bool,
    pub prometheus_url: Option<String>,
    /// Path to the console SPA build directory. When set, the API server
    /// serves the console UI at `/console/` and falls back to `index.html`
    /// for client-side routing.
    pub console_dir: Option<PathBuf>,
    /// Path to client CA certificate for x509 client-certificate authentication.
    /// When set, the API server verifies any client cert presented against this
    /// CA and maps its Subject CN→username and O→groups (#1129). The cert is
    /// OPTIONAL — bearer-token clients still connect; presenting one is just an
    /// additional way to authenticate.
    pub client_ca_file: Option<String>,
    /// Preloaded/generated TLS material. Embedded callers can set this after
    /// calling [`prepare_tls_for_config`] so the CA handed to other components
    /// matches the certificate served by the API server.
    pub prepared_tls: Option<PreparedTlsConfig>,
    /// `--service-node-port-range` (cmd/kube-apiserver/app/options/options.go:
    /// 124): the range NodePorts are allocated from. A zero-size range means
    /// unspecified and falls back to 30000-32767 (pkg/controlplane/
    /// instance.go:285-291).
    pub service_node_port_range: registry::core::service::portallocator::PortRange,
    /// `--service-cluster-ip-range`: one CIDR, or two of different IP
    /// families, comma-separated.
    pub service_cluster_ip_range: String,
    /// `--event-ttl` in seconds: how long an Event lives after its last write
    /// (`0` keeps events forever).
    pub event_ttl: u64,
}

/// The `--service-account-*` / `--api-audiences` flags, shared by the
/// `api-server` and all-in-one binaries (#1575, #2713). Help text and flag
/// names follow `ServiceAccountAuthenticationOptions.AddFlags`
/// (pkg/kubeapiserver/options/authentication.go:431-470).
#[derive(clap::Args, Debug, Clone, Default)]
pub struct ServiceAccountArgs {
    /// File containing PEM-encoded x509 RSA or ECDSA private or public keys,
    /// used to verify ServiceAccount tokens. The specified file can contain
    /// multiple keys, and the flag can be specified multiple times with
    /// different files. Must be specified when
    /// --service-account-signing-key-file is provided
    /// (pkg/kubeapiserver/options/authentication.go:432-437).
    #[arg(long = "service-account-key-file")]
    pub service_account_key_file: Vec<String>,

    /// Path to the file that contains the current private key of the service
    /// account token issuer. The issuer will sign issued ID tokens with this
    /// private key (pkg/controlplane/apiserver/options/options.go:207).
    #[arg(long = "service-account-signing-key-file")]
    pub service_account_signing_key_file: Option<String>,

    /// Identifier of the service account token issuer. The issuer will assert
    /// this identifier in "iss" claim of issued tokens. When this flag is
    /// specified multiple times, the first is used to generate tokens and all
    /// are used to determine which issuers are accepted
    /// (pkg/kubeapiserver/options/authentication.go:442-452).
    #[arg(long = "service-account-issuer")]
    pub service_account_issuer: Vec<String>,

    /// Identifiers of the API. The service account token authenticator will
    /// validate that tokens used against the API are bound to at least one of
    /// these audiences. If the --service-account-issuer flag is configured and
    /// this flag is not, this field defaults to a single element list
    /// containing the issuer URL
    /// (pkg/kubeapiserver/options/authentication.go:352).
    #[arg(long = "api-audiences", value_delimiter = ',')]
    pub api_audiences: Vec<String>,

    /// The maximum validity duration of a token created by the service account
    /// token issuer, as a Go duration (e.g. 24h). If an otherwise valid
    /// TokenRequest with a validity duration larger than this value is
    /// requested, a token will be issued with a validity duration of this
    /// value (authentication.go:459-462).
    #[arg(
        long = "service-account-max-token-expiration",
        value_parser = parse_go_duration_arg
    )]
    pub service_account_max_token_expiration: Option<std::time::Duration>,
}

fn parse_go_duration_arg(s: &str) -> std::result::Result<std::time::Duration, String> {
    let ns = rusternetes_common::go_duration::parse_go_duration(s)?;
    // A negative duration is below the 1h lower bound upstream rejects; map it
    // to zero here would silently mean "unset", so refuse it at parse time.
    u64::try_from(ns)
        .map(std::time::Duration::from_nanos)
        .map_err(|_| format!("negative duration {s:?} is not allowed"))
}

impl ServiceAccountArgs {
    pub fn to_options(&self) -> rusternetes_common::auth::ServiceAccountOptions {
        rusternetes_common::auth::ServiceAccountOptions {
            key_files: self.service_account_key_file.clone(),
            signing_key_file: self.service_account_signing_key_file.clone(),
            issuers: self.service_account_issuer.clone(),
            api_audiences: self.api_audiences.clone(),
            max_expiration: self.service_account_max_token_expiration,
        }
    }
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:6443".to_string(),
            jwt_secret: "rusternetes-secret-change-in-production".to_string(),
            service_account: Default::default(),
            tls: false,
            tls_cert_file: None,
            tls_key_file: None,
            tls_self_signed: false,
            tls_san: "localhost,127.0.0.1".to_string(),
            skip_auth: true,
            prometheus_url: None,
            console_dir: None,
            client_ca_file: None,
            prepared_tls: None,
            service_node_port_range:
                registry::core::service::portallocator::DEFAULT_SERVICE_NODE_PORT_RANGE,
            service_cluster_ip_range:
                registry::core::service::ipranges::DEFAULT_SERVICE_CLUSTER_IP_RANGE.to_string(),
            event_ttl: registry::core::event::DEFAULT_EVENT_TTL_SECONDS,
        }
    }
}

/// Run the API server component.
///
/// This is the main entry point for embedding the API server in the all-in-one binary.
/// Starts the HTTPS/HTTP server and blocks until shutdown.
pub async fn run(storage: Arc<StorageBackend>, mut config: ApiServerConfig) -> anyhow::Result<()> {
    info!("Starting Rusternetes API Server");

    // `validateClusterIPFlags`: refuse to start on a bad range.
    let service_ranges =
        registry::core::service::ipranges::ServiceIpRanges::parse(&config.service_cluster_ip_range)
            .map_err(|e| anyhow::anyhow!(e))?;

    let token_manager = Arc::new(
        TokenManager::new_auto(config.jwt_secret.as_bytes())
            .with_service_account_options(&config.service_account)?,
    );

    let authorizer: Arc<dyn rusternetes_common::authz::Authorizer> = if config.skip_auth {
        warn!("Authentication and authorization disabled - insecure mode");
        Arc::new(rusternetes_common::authz::AlwaysAllowAuthorizer)
    } else {
        info!("Initializing RBAC Authorizer");
        // `system:masters` superuser first, as `newForConfig`
        // (`pkg/kubeapiserver/authorizer/reload.go:97-99`) does (#1576).
        let rbac: Arc<dyn rusternetes_common::authz::Authorizer> =
            Arc::new(RBACAuthorizer::new(storage.clone()));
        Arc::new(rusternetes_common::authz::superuser_then(vec![rbac]))
    };

    let metrics = Arc::new(MetricsRegistry::new().with_api_server_metrics()?);

    // Generate or load the TLS config once so that the serving cert and the
    // kube-root-ca.crt written into SA volumes are always the same certificate.
    // Previously a second call to generate_self_signed at the server-bind site
    // produced a different random key pair, causing in-cluster kube clients
    // (flanneld, etc.) to fail TLS verification (M2a fix).
    let prepared_tls = if config.tls {
        info!("TLS enabled - loading/generating certificates");
        if let Some(prepared) = config.prepared_tls.take() {
            Some(prepared)
        } else {
            prepare_tls_for_config(&config)?
        }
    } else {
        None
    };
    let ca_cert_pem = prepared_tls
        .as_ref()
        .and_then(|prepared| prepared.ca_cert_pem().map(str::to_string));

    // Bootstrap kubernetes Service
    let api_port = config
        .bind_address
        .split(':')
        .next_back()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6443);

    startup::register_post_start_hooks(
        storage.clone(),
        api_port,
        service_ranges.api_server_service_ip(),
        service_ranges.cidrs(),
        ca_cert_pem.as_deref(),
    )
    .await;

    // Prometheus client
    let prom_client = if let Some(ref url) = config.prometheus_url {
        match prometheus_client::PrometheusClient::new(url.clone()) {
            Ok(c) => Some(Arc::new(c)),
            Err(e) => {
                warn!("Failed to init Prometheus client: {}", e);
                None
            }
        }
    } else {
        None
    };

    let state = Arc::new(
        ApiServerState::new(
            storage,
            token_manager,
            authorizer,
            metrics,
            config.skip_auth,
        )
        .with_service_cluster_ip_ranges(&service_ranges)
        .with_ca_cert(ca_cert_pem)
        .with_service_node_port_range(config.service_node_port_range)
        .with_event_ttl(config.event_ttl)
        .with_prometheus_client(prom_client),
    );

    // The ClusterIP and NodePort repair loops; startup waits for their
    // first passes. The ClusterIP repair also creates the IPAddress of
    // every Service stored without one.
    bootstrap::start_service_ip_repair_controllers(&state).await?;

    let app = router::build_router(state, config.console_dir.as_deref());

    if config.tls {
        // Reuse the cert generated/loaded above so the serving cert matches
        // the one written into kube-root-ca.crt / SA volumes.
        let tls_config = prepared_tls
            .ok_or_else(|| anyhow::anyhow!("TLS config unavailable"))?
            .into_tls_config();

        let client_cert_authn = config.client_ca_file.is_some();
        let server_config = if let Some(ref client_ca) = config.client_ca_file {
            info!(
                "Client certificate authentication enabled (CA: {})",
                client_ca
            );
            tls_config.into_mtls_server_config(client_ca)?
        } else {
            tls_config.into_server_config()?
        };
        let rustls_config = RustlsConfig::from_config(server_config);
        info!("HTTPS server listening on {}", config.bind_address);
        let addr = config.bind_address.parse()?;
        if client_cert_authn {
            // mTLS: serve via PeerCertAcceptor so the verified client cert reaches
            // handlers for x509 authn (CN→user / O→groups, #1129).
            let mut server = axum_server::bind(addr).acceptor(
                crate::peer_cert_acceptor::PeerCertAcceptor::new(rustls_config),
            );
            server
                .http_builder()
                .http2()
                .initial_stream_window_size(256 * 1024)
                .initial_connection_window_size(256 * 1024 * 100)
                .max_concurrent_streams(250);
            server.serve(app.into_make_service()).await?;
        } else {
            let mut server = axum_server::bind_rustls(addr, rustls_config);
            server
                .http_builder()
                .http2()
                .initial_stream_window_size(256 * 1024)
                .initial_connection_window_size(256 * 1024 * 100)
                .max_concurrent_streams(250);
            server.serve(app.into_make_service()).await?;
        }
    } else {
        info!(
            "API Server listening on {} (HTTP, no TLS)",
            config.bind_address
        );
        let listener = tokio::net::TcpListener::bind(&config.bind_address).await?;
        axum::serve(listener, app).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[serial_test::serial]
    fn prepare_tls_for_self_signed_exposes_served_cert_as_ca() {
        let old_ca_cert_path = std::env::var_os("CA_CERT_PATH");
        let old_home = std::env::var_os("HOME");
        std::env::set_var("CA_CERT_PATH", "/tmp/rusternetes-test-missing-ca-cert.pem");
        std::env::set_var("HOME", "/tmp/rusternetes-test-missing-home");

        let config = ApiServerConfig {
            tls: true,
            tls_self_signed: true,
            tls_san: "localhost,127.0.0.1".to_string(),
            ..Default::default()
        };

        let prepared = prepare_tls_for_config(&config)
            .expect("self-signed TLS should prepare")
            .expect("TLS is enabled");
        let ca_cert_pem = prepared
            .ca_cert_pem()
            .expect("self-signed TLS should expose a CA PEM")
            .to_string();
        let tls_config = prepared.into_tls_config();

        assert_eq!(tls_config.cert_pem.as_deref(), Some(ca_cert_pem.as_str()));

        match old_ca_cert_path {
            Some(value) => std::env::set_var("CA_CERT_PATH", value),
            None => std::env::remove_var("CA_CERT_PATH"),
        }
        match old_home {
            Some(value) => std::env::set_var("HOME", value),
            None => std::env::remove_var("HOME"),
        }
    }
}

#[cfg(test)]
mod service_account_args_tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct Wrap {
        #[command(flatten)]
        sa: ServiceAccountArgs,
    }

    #[test]
    fn flags_map_to_options_2713() {
        let w = Wrap::try_parse_from([
            "x",
            "--service-account-key-file=/k1",
            "--service-account-key-file=/k2",
            "--service-account-signing-key-file=/s",
            "--service-account-issuer=https://i",
            "--api-audiences=a,b",
            "--service-account-max-token-expiration=24h",
        ])
        .unwrap();
        let o = w.sa.to_options();
        assert_eq!(o.key_files, ["/k1", "/k2"]);
        assert_eq!(o.signing_key_file.as_deref(), Some("/s"));
        assert_eq!(o.issuers, ["https://i"]);
        assert_eq!(o.api_audiences, ["a", "b"]);
        assert_eq!(
            o.max_expiration,
            Some(std::time::Duration::from_secs(86400))
        );
    }

    #[test]
    fn bad_duration_rejected() {
        assert!(Wrap::try_parse_from(["x", "--service-account-max-token-expiration=-1h"]).is_err());
        assert!(Wrap::try_parse_from(["x", "--service-account-max-token-expiration=abc"]).is_err());
    }
}
