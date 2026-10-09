// mimalloc as the global allocator (off by default; `--features mimalloc`).
// Required for the musl static builds — musl's default allocator is ~10x
// slower under multi-threaded lock contention — and lowers idle RSS (#1041).
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod admission;
mod audit;
pub use rusternetes_admission_webhook as admission_webhook;
#[allow(dead_code)]
mod bootstrap;
#[allow(dead_code)]
mod legacy_token_tracking;
pub use rusternetes_admission_webhook::cel_evaluators as cel;
mod conversion;
mod dynamic_routes;
mod endpoints;
#[allow(dead_code)]
mod flow_control;
mod flow_control_filter;
#[allow(dead_code)]
mod flow_control_queueset;
#[allow(dead_code)]
mod flow_control_work_estimator;
mod gnostic;
mod handlers;
use rusternetes_middleware as middleware;
mod openapi;
mod patch;
mod peer_cert_acceptor;
#[allow(dead_code)]
mod post_start_hooks;
mod prometheus_client;
#[allow(dead_code)]
mod registry;
#[allow(dead_code)]
mod storage_readiness_hook;
pub use rusternetes_protobuf as protobuf;
#[allow(dead_code)]
mod response;
mod router;
#[allow(dead_code)]
mod spdy;
// The bin only drives handle_spdy3_exec; the rest of the codec API is exercised
// by the lib + tests, so allow dead_code in the binary build.
#[allow(dead_code)]
mod fieldmanager;
#[allow(dead_code)]
mod spdy3;
mod ssa;
mod state;
#[allow(dead_code)]
mod streaming;
mod watch_cache;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use prometheus_client::PrometheusClient;
use rusternetes_common::auth::TokenManager;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{StorageBackend, StorageConfig};
use state::ApiServerState;
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "rusternetes-api-server")]
#[command(about = "Rusternetes API Server - Kubernetes API reimplemented in Rust")]
struct Args {
    /// A port range to reserve for services with NodePort visibility. This
    /// must not overlap with the ephemeral port range on nodes. Example:
    /// '30000-32767'. Inclusive at both ends of the range
    /// (cmd/kube-apiserver/app/options/options.go:124).
    #[arg(long, default_value = "30000-32767")]
    service_node_port_range:
        rusternetes_api_server::registry::core::service::portallocator::PortRange,

    /// Address to bind to
    #[arg(long, default_value = "0.0.0.0:6443")]
    bind_address: String,

    /// Etcd endpoints (comma-separated)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// JWT secret for service account tokens
    #[arg(long, default_value = "rusternetes-secret-change-in-production")]
    jwt_secret: String,

    #[command(flatten)]
    service_account: rusternetes_api_server::ServiceAccountArgs,

    /// Enable TLS/HTTPS
    #[arg(long)]
    tls: bool,

    /// TLS certificate file (PEM format)
    #[arg(long)]
    tls_cert_file: Option<String>,

    /// TLS private key file (PEM format)
    #[arg(long)]
    tls_key_file: Option<String>,

    /// Generate self-signed certificate if TLS files not provided
    #[arg(long)]
    tls_self_signed: bool,

    /// Subject Alternative Names for self-signed cert (comma-separated)
    #[arg(long, default_value = "localhost,127.0.0.1")]
    tls_san: String,

    /// Skip authentication and authorization (INSECURE - development only)
    #[arg(long)]
    skip_auth: bool,

    /// Storage backend: "etcd" or "sqlite"
    #[arg(long, default_value = "etcd")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Prometheus server URL for custom metrics (optional)
    #[arg(long)]
    prometheus_url: Option<String>,

    /// Path to the console SPA build directory (enables web console at /console/)
    #[arg(long)]
    console_dir: Option<String>,

    /// ClusterIP range(s) for Services: one CIDR, or two of different IP
    /// families (dual-stack), comma-separated (`--service-cluster-ip-range`).
    #[arg(long, default_value = "10.96.0.0/12")]
    service_cluster_ip_range: String,

    /// Client CA certificate file for mTLS client certificate authentication
    #[arg(long)]
    client_ca_file: Option<String>,

    /// File with the admission control configuration (an
    /// `AdmissionConfiguration`); only the `PodSecurity` plugin's
    /// `exemptions` are read (kube-apiserver `--admission-control-config-file`).
    #[arg(long)]
    admission_control_config_file: Option<String>,

    /// Amount of time to retain events, as a Go duration (`--event-ttl`,
    /// pkg/controlplane/apiserver/options/options.go:162). `0` keeps them
    /// forever.
    #[arg(
        long,
        default_value = "1h",
        value_parser = registry::core::event::parse_event_ttl
    )]
    event_ttl: u64,

    /// Install the API Priority and Fairness request filter
    /// (`WithPriorityAndFairness`, server/config.go:1027). Upstream enables it
    /// by default (`APIPriorityAndFairness` is GA); it is opt-in here until a
    /// sig-api-machinery conformance run has exercised it.
    #[arg(long, default_value_t = false)]
    enable_priority_and_fairness: bool,

    /// `--max-requests-inflight` (server/config.go:443): with
    /// `--max-mutating-requests-inflight` this is the server concurrency limit
    /// APF divides between priority levels.
    #[arg(long, default_value_t = flow_control::DEFAULT_MAX_REQUESTS_IN_FLIGHT)]
    max_requests_inflight: i64,

    /// `--max-mutating-requests-inflight` (server/config.go:444).
    #[arg(long, default_value_t = flow_control::DEFAULT_MAX_MUTATING_REQUESTS_IN_FLIGHT)]
    max_mutating_requests_inflight: i64,

    /// Path to the file that defines the audit policy configuration
    /// (`--audit-policy-file`, pkg/server/options/audit.go:258).
    #[arg(long)]
    audit_policy_file: Option<String>,

    /// Path of the file audit events are written to; `-` is stdout
    /// (`--audit-log-path`, options/audit.go:436).
    #[arg(long)]
    audit_log_path: Option<String>,

    /// Format of saved audits (`--audit-log-format`, options/audit.go:444);
    /// only `json` is supported.
    #[arg(long, default_value = "json")]
    audit_log_format: String,
}

/// `--audit-policy-file` + `--audit-log-path` build the audit pipeline.
/// Like `WithAudit` (filters/audit.go:42), a missing policy or sink leaves
/// auditing off.
async fn install_audit_from_flags(args: &Args) -> Result<()> {
    if args.audit_log_format != "json" {
        anyhow::bail!(
            "invalid audit log format {:?}: only \"json\" is supported",
            args.audit_log_format
        );
    }
    let (Some(policy_file), Some(log_path)) = (&args.audit_policy_file, &args.audit_log_path)
    else {
        if args.audit_policy_file.is_some() || args.audit_log_path.is_some() {
            warn!("auditing needs both --audit-policy-file and --audit-log-path; it is off");
        }
        return Ok(());
    };
    let yaml = std::fs::read_to_string(policy_file)
        .with_context(|| format!("reading --audit-policy-file {policy_file}"))?;
    let policy = audit::Policy::from_yaml(&yaml)
        .map_err(|e| anyhow::anyhow!("{e}: from file {policy_file}"))?;
    let sink: std::sync::Arc<dyn rusternetes_common::audit::AuditBackend> = if log_path == "-" {
        std::sync::Arc::new(audit::StdoutAuditBackend)
    } else {
        std::sync::Arc::new(
            rusternetes_common::audit::FileAuditBackend::new(log_path.clone())
                .await
                .with_context(|| format!("opening --audit-log-path {log_path}"))?,
        )
    };
    audit::install_audit(audit::AuditConfig { policy, sink });
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    // `validateClusterIPFlags`: refuse to start on a bad range.
    let service_ranges =
        registry::core::service::ipranges::ServiceIpRanges::parse(&args.service_cluster_ip_range)
            .map_err(|e| anyhow::anyhow!(e))?;

    rusternetes_common::tracing::init_basic_tracing("api-server", &args.log_level)?;
    rusternetes_common::dump::install_panic_hook("api-server");

    if let Some(path) = &args.admission_control_config_file {
        let yaml = std::fs::read_to_string(path)
            .with_context(|| format!("reading --admission-control-config-file {path}"))?;
        let exemptions = admission::PodSecurityExemptions::from_admission_configuration(&yaml)
            .map_err(|e| anyhow::anyhow!("parsing {path}: {e}"))?;
        admission::install_pod_security_exemptions(exemptions);
    }

    install_audit_from_flags(&args).await?;

    info!(
        "Starting Rusternetes API Server {}",
        rusternetes_common::build_info::version_line()
    );

    // Initialize storage
    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Using SQLite storage backend at: {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir.clone(),
            }
        }
        _ => {
            let etcd_endpoints: Vec<String> = args
                .etcd_servers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Connecting to etcd: {:?}", etcd_endpoints);
            StorageConfig::Etcd {
                endpoints: etcd_endpoints,
            }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    // Initialize TokenManager — prefer RSA keys for RS256 (K8s OIDC compatible),
    // fall back to HMAC HS256 if no RSA keys found.
    info!("Initializing TokenManager");
    let service_account = args.service_account.to_options();
    let token_manager = Arc::new(
        TokenManager::new_auto(args.jwt_secret.as_bytes())
            .with_service_account_options(&service_account)?,
    );

    // Authorizer chain, shared with the all-in-one entry point (#2679).
    if args.skip_auth {
        warn!("⚠️  AUTHENTICATION AND AUTHORIZATION DISABLED - INSECURE MODE");
        warn!("⚠️  Using AlwaysAllowAuthorizer - all requests will be permitted");
        warn!("⚠️  This should ONLY be used in development/testing environments");
    }
    let authorizer =
        rusternetes_api_server::authorizer::build_authorizer(storage.clone(), args.skip_auth, &[])?;

    // Initialize Metrics Registry
    info!("Initializing Metrics Registry");
    let metrics = Arc::new(MetricsRegistry::new().with_api_server_metrics()?);

    let api_config = rusternetes_api_server::ApiServerConfig {
        bind_address: args.bind_address.clone(),
        tls: args.tls,
        tls_cert_file: args.tls_cert_file.clone(),
        tls_key_file: args.tls_key_file.clone(),
        tls_self_signed: args.tls_self_signed,
        tls_san: args.tls_san.clone(),
        skip_auth: args.skip_auth,
        client_ca_file: args.client_ca_file.clone(),
        service_node_port_range: args.service_node_port_range,
        service_account,
        ..Default::default()
    };
    let prepared_tls = rusternetes_api_server::prepare_tls_for_config(&api_config)?;
    let ca_cert_pem = prepared_tls
        .as_ref()
        .and_then(|prepared| prepared.ca_cert_pem().map(str::to_string));

    // Bootstrap kubernetes Service Endpoints with dynamic IP discovery
    let api_port = args
        .bind_address
        .split(':')
        .next_back()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6443);

    rusternetes_api_server::startup::register_post_start_hooks(
        storage.clone(),
        api_port,
        service_ranges.api_server_service_ip(),
        service_ranges.cidrs(),
        ca_cert_pem.as_deref(),
    )
    .await;

    // Initialize Prometheus client for custom metrics (if URL provided)
    let prometheus_client = if let Some(url) = args.prometheus_url {
        info!("Initializing Prometheus client: {}", url);
        match PrometheusClient::new(url.clone()) {
            Ok(client) => {
                info!("Prometheus client initialized successfully");
                Some(Arc::new(client))
            }
            Err(e) => {
                warn!("Failed to initialize Prometheus client: {}. Custom metrics will return mock data.", e);
                None
            }
        }
    } else {
        info!("Prometheus URL not provided, custom metrics will return mock data");
        None
    };

    // API Priority and Fairness: installed before the router is built, which
    // layers it on the protected routes when present (#2704).
    if args.enable_priority_and_fairness {
        let engine = Arc::new(flow_control::FlowControlEngine::with_limits(
            storage.clone(),
            args.max_requests_inflight,
            args.max_mutating_requests_inflight,
        ));
        engine
            .initialize()
            .await
            .map_err(|e| anyhow::anyhow!("initializing API Priority and Fairness: {e}"))?;
        flow_control_filter::spawn_config_reloader(
            engine.clone(),
            std::time::Duration::from_secs(2),
        );
        flow_control_filter::install_flow_control(Arc::new(flow_control_filter::ApfFilter::new(
            engine,
            flow_control_filter::DEFAULT_REQUEST_TIMEOUT / 4,
        )));
        info!("API Priority and Fairness enabled");
    }

    // Create shared state with CA certificate and Prometheus client
    let state = Arc::new(
        ApiServerState::new(storage, token_manager, authorizer, metrics, args.skip_auth)
            .with_service_cluster_ip_ranges(&service_ranges)
            .with_ca_cert(ca_cert_pem)
            .with_service_node_port_range(registry::core::service::portallocator::PortRange {
                base: args.service_node_port_range.base,
                size: args.service_node_port_range.size,
            })
            .with_event_ttl(args.event_ttl)
            .with_prometheus_client(prometheus_client),
    );

    // The ClusterIP and NodePort repair loops; startup waits for their
    // first passes. The ClusterIP repair also creates the IPAddress of
    // every Service stored without one.
    bootstrap::start_service_ip_repair_controllers(&state).await?;

    // Build router
    let console_path = args.console_dir.as_ref().map(std::path::PathBuf::from);
    let app = router::build_router(state, console_path.as_deref());

    // Start server (with or without TLS)
    if args.tls {
        info!("TLS enabled - starting HTTPS server");

        let tls_config = prepared_tls
            .ok_or_else(|| anyhow::anyhow!("TLS config unavailable"))?
            .into_tls_config();

        // `--client-ca-file` enables x509 client-cert authentication: build an
        // mTLS server config (client cert OPTIONAL — bearer-token clients still
        // connect) and serve via PeerCertAcceptor so the verified cert reaches
        // handlers for CN→user / O→groups mapping (#1129). Without it, plain
        // serving-only TLS as before.
        let client_cert_authn = args.client_ca_file.is_some();
        let server_config = if let Some(ref client_ca) = args.client_ca_file {
            info!(
                "Client certificate authentication enabled (CA: {})",
                client_ca
            );
            tls_config.into_mtls_server_config(client_ca)?
        } else {
            tls_config.into_server_config()?
        };
        let rustls_config = RustlsConfig::from_config(server_config);

        info!("HTTPS server listening on {}", args.bind_address);
        let addr = args.bind_address.parse()?;

        // Configure HTTP/2 settings to match K8s API server.
        // K8s sets these in secure_serving.go:175-199:
        //   MaxConcurrentStreams = 100
        //   MaxUploadBufferPerStream = 256KB
        //   IdleTimeout = 90s
        //
        // Hyper defaults (64KB window, 200 streams) cause watch stream
        // stalls with many concurrent watches — the flow control windows
        // fill up and events can't be delivered, causing client-go's
        // "Watch failed: context canceled" errors.
        //
        // `apply_http2_tuning!` keeps the two acceptor branches (mTLS vs
        // serving-only) in sync — the Server type differs by acceptor, so the
        // tuning can't be hoisted into a plain fn without naming both types.
        macro_rules! apply_http2_tuning {
            ($server:expr) => {{
                let builder = $server.http_builder();
                // Set timer first — required for HTTP/2 keepalive to function
                builder
                    .http2()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .initial_stream_window_size(256 * 1024) // 256KB per stream (K8s: 256KB)
                    .initial_connection_window_size(256 * 1024 * 100) // 25MB total (K8s: 256KB * 100)
                    .max_concurrent_streams(1000) // High limit — watch timeout (2min) handles stream recycling
                    // HTTP/2 PING keepalive: send PING frames to keep connections alive.
                    // Without this, network intermediaries (Podman Machine virtio-net,
                    // Docker Desktop proxy) may close idle TCP connections, killing
                    // watch streams with "context canceled".
                    // K8s Go server uses net.KeepAlive = 3 minutes on the TCP listener.
                    .keep_alive_interval(std::time::Duration::from_secs(30))
                    .keep_alive_timeout(std::time::Duration::from_secs(20));
            }};
        }

        if client_cert_authn {
            let mut server = axum_server::bind(addr)
                .acceptor(peer_cert_acceptor::PeerCertAcceptor::new(rustls_config));
            apply_http2_tuning!(server);
            server.serve(app.into_make_service()).await?;
        } else {
            let mut server = axum_server::bind_rustls(addr, rustls_config);
            apply_http2_tuning!(server);
            server.serve(app.into_make_service()).await?;
        }
    } else {
        info!("TLS disabled - starting HTTP server (not recommended for production)");
        info!("API Server listening on {}", args.bind_address);
        let listener = tokio::net::TcpListener::bind(&args.bind_address).await?;
        axum::serve(listener, app).await?;
    }

    Ok(())
}
