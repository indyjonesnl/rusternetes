// mimalloc as the global allocator (off by default; `--features mimalloc`).
// Required for the musl static builds — musl's default allocator is ~10x
// slower under multi-threaded lock contention — and lowers idle RSS (#1041).
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod admission;
mod audit;
mod authorization_filter;
mod negotiated_error;
pub use rusternetes_admission_webhook as admission_webhook;
#[allow(dead_code)]
mod bootstrap;
#[allow(dead_code)]
mod legacy_token_tracking;
pub use rusternetes_admission_webhook::cel_evaluators as cel;
mod apiserver_request_metrics;
mod conversion;
mod deprecation;
mod dynamic_routes;
mod endpoints;
#[allow(dead_code)]
mod flow_control;
#[allow(dead_code)]
mod flow_control_conc_alloc;
mod flow_control_filter;
#[allow(dead_code)]
mod flow_control_integrator;
#[allow(dead_code)]
mod flow_control_metrics;
#[allow(dead_code)]
mod flow_control_object_count;
#[allow(dead_code)]
mod flow_control_queueset;
#[allow(dead_code)]
mod flow_control_stats_poller;
mod flow_control_watch_tracker;
#[allow(dead_code)]
mod flow_control_work_estimator;
mod gnostic;
mod handlers;
use rusternetes_middleware as middleware;
mod openapi;
mod patch;
#[allow(dead_code)]
mod post_start_hooks;
mod prometheus_client;
#[allow(dead_code)]
mod registry;
// The task-local is read by the lib's webhook authorizer, so the bin must
// install it through the lib's module, not a second copy (#3159).
use rusternetes_api_server::request_scope;
// `s.Flagz` is read by the lib router, so set it through the lib module.
use rusternetes_api_server::zpages;
// The bin's own `bootstrap` copy names the lib's trust type (#3414).
use rusternetes_api_server::request_header_authentication;
// The bin's own `admission` copy shares the lib's webhook client (ImagePolicyWebhook).
use rusternetes_api_server::authorization_webhook;
#[allow(dead_code)]
mod storage_readiness_hook;
pub use rusternetes_protobuf as protobuf;
#[allow(dead_code)]
mod fieldmanager;
mod request_timeout;
#[allow(dead_code)]
mod response;
mod router;
mod ssa;
mod state;
#[allow(dead_code)]
mod streaming;
mod upstream_flags;
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

    /// Comma-separated `[component:]Gate=bool` pairs, e.g.
    /// `StorageCapacityScoring=true` or `kube:StorageCapacityScoring=true`;
    /// may be repeated (upstream `--feature-gates`,
    /// compatibility/registry.go:249-253). The component prefix defaults to
    /// `kube`; do not mix prefixed and unprefixed pairs.
    #[arg(long)]
    feature_gates: Vec<String>,

    /// The version the component emulates (APIs, features, ...), as
    /// `[component=]major.minor`, e.g. `--emulated-version=1.34`. Options are:
    /// kube=1.32..1.35(default:1.35) (upstream `--emulated-version`,
    /// compatibility/registry.go:231-236).
    #[arg(long, value_delimiter = ',')]
    emulated_version: Vec<String>,

    /// The min version of control plane components the server should be
    /// compatible with; must be <= `--emulated-version`. Options are:
    /// kube=1.32..1.35(default:1.34) (upstream `--min-compatibility-version`,
    /// compatibility/registry.go:243-247).
    #[arg(long, value_delimiter = ',')]
    min_compatibility_version: Vec<String>,

    /// Address to bind to
    #[arg(long, default_value = "0.0.0.0:6443")]
    bind_address: String,

    /// The IP address on which to advertise the apiserver to members of the
    /// cluster (the `kubernetes` Endpoints). If blank or unspecified, the
    /// host of `--bind-address` is used, resolved to the host's default
    /// address of its family (upstream `--advertise-address`).
    #[arg(long)]
    advertise_address: Option<std::net::IpAddr>,

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

    /// `--authorization-mode` (#2854).
    #[command(flatten)]
    authorization: rusternetes_api_server::authorizer::AuthorizationArgs,

    /// `--enable-admission-plugins` / `--disable-admission-plugins` /
    /// `--admission-control` (#3415).
    #[command(flatten)]
    admission: admission::plugins::AdmissionArgs,
    /// `--requestheader-*` front-proxy authentication (#3414).
    #[command(flatten)]
    request_header:
        rusternetes_api_server::request_header_authentication::RequestHeaderAuthenticationArgs,

    /// `--egress-selector-config-file` (#3416).
    #[command(flatten)]
    egress_selector: rusternetes_api_server::egress_selector::EgressSelectorArgs,

    /// Upstream kube-apiserver flags accepted for kubeadm/Talos argv
    /// compatibility (`--secure-port`, ignored-with-WARN flags, #3908).
    #[command(flatten)]
    upstream: upstream_flags::UpstreamCompatArgs,

    /// Enable TLS/HTTPS
    #[arg(long)]
    tls: bool,

    /// TLS certificate file (PEM format)
    #[arg(long)]
    tls_cert_file: Option<String>,

    /// TLS private key file (PEM format); upstream `--tls-private-key-file`
    #[arg(long, alias = "tls-private-key-file")]
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

    /// `--request-timeout` (server/config.go:445): the maximum a
    /// non-long-running request may take (`WithRequestDeadline`); a request's
    /// own `?timeout=` can only shorten it.
    #[arg(
        long,
        default_value = "1m0s",
        value_parser = |s: &str| -> Result<std::time::Duration, String> {
            let ns = rusternetes_common::go_duration::parse_go_duration(s)?;
            u64::try_from(ns)
                .map(std::time::Duration::from_nanos)
                .map_err(|_| format!("negative duration {s:?} is not allowed"))
        }
    )]
    request_timeout: std::time::Duration,

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

/// Apply `--emulated-version`, `--min-compatibility-version` and
/// `--feature-gates`: `ComponentGlobalsRegistry.Set()` then `Validate()`
/// (cmd/kube-apiserver/app/server.go:84-87; staging/src/k8s.io/component-base/compatibility/registry.go:344-451).
fn apply_feature_gates(args: &Args) -> Result<()> {
    let effective_version = rusternetes_common::compatibility::apply_flags(
        &args.emulated_version,
        &args.min_compatibility_version,
        &args.feature_gates,
    )
    .map_err(|e| anyhow::anyhow!("invalid --feature-gates/--emulated-version: {e}"))?;
    // `ComponentGlobalsRegistry.AddMetrics()` (cmd/kube-apiserver/app/server.go:116;
    // component-base/compatibility/registry.go:123-131): record `version_info`.
    rusternetes_common::compat_version_metrics::add_metrics(
        rusternetes_common::compatibility::DEFAULT_KUBE_COMPONENT,
        &effective_version,
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // `fs.Changed` for the legacy authorization flags needs the raw matches.
    let cmd = <Args as clap::CommandFactory>::command();
    let matches = cmd.clone().get_matches();
    // `s.Flagz = flagz.NamedFlagSetsReader{FlagSets: namedFlagSets}`
    // (cmd/kube-apiserver/app/server.go:131-134).
    zpages::set_flagz_reader(rusternetes_common::flagz::flags_from_clap(&cmd, &matches));
    let mut args = <Args as clap::FromArgMatches>::from_arg_matches(&matches)
        .map_err(|e| e.exit())
        .unwrap();
    args.authorization.mark_legacy_flags_set(&matches);
    args.upstream.validate()?;
    args.bind_address =
        upstream_flags::resolve_bind_address(&args.bind_address, args.upstream.secure_port)?;
    // Upstream serves TLS whenever a serving cert/key is configured.
    args.tls |= upstream_flags::implies_tls(&args.tls_cert_file, &args.tls_key_file);
    // `s.Admission.Validate()` + `ApplyTo` (#3415): a bad plugin flag stops startup.
    args.admission
        .validate_and_install()
        .map_err(|e| anyhow::anyhow!("invalid admission flags: {e}"))?;
    let request_header = args
        .request_header
        .resolve(args.client_ca_file.as_deref())?;
    request_timeout::set_request_timeout(args.request_timeout);
    apply_feature_gates(&args)?;
    // `validateClusterIPFlags`: refuse to start on a bad range.
    let service_ranges =
        registry::core::service::ipranges::ServiceIpRanges::parse(&args.service_cluster_ip_range)
            .map_err(|e| anyhow::anyhow!(e))?;

    rusternetes_common::tracing::init_basic_tracing("api-server", &args.log_level)?;
    rusternetes_common::dump::install_panic_hook("api-server");
    for msg in args.upstream.ignored_flags() {
        warn!("{msg}");
    }

    let mut image_policy_config = None;
    if let Some(path) = &args.admission_control_config_file {
        let yaml = std::fs::read_to_string(path)
            .with_context(|| format!("reading --admission-control-config-file {path}"))?;
        let exemptions = admission::PodSecurityExemptions::from_admission_configuration(
            &yaml,
            std::path::Path::new(path),
        )
        .map_err(|e| anyhow::anyhow!("parsing {path}: {e}"))?;
        admission::install_pod_security_exemptions(exemptions);
        let limited = admission::resourcequota::config::from_admission_configuration(
            &yaml,
            std::path::Path::new(path),
        )
        .map_err(|e| anyhow::anyhow!("parsing {path}: {e}"))?;
        admission::resourcequota::config::install_limited_resources(limited);
        admission::pod_plugins::install_from_admission_configuration(
            &yaml,
            std::path::Path::new(path),
        )
        .map_err(|e| anyhow::anyhow!("parsing {path}: {e}"))?;
        image_policy_config = admission::config::plugin_configuration_for(
            &yaml,
            std::path::Path::new(path),
            "ImagePolicyWebhook",
        )
        .map_err(|e| anyhow::anyhow!("parsing {path}: {e}"))?;
    }
    admission::image_policy::install_from_config(image_policy_config.as_deref())?;

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
    let authorizer = rusternetes_api_server::authorizer::build_authorizer_with_config(
        storage.clone(),
        args.skip_auth,
        &args.authorization.authorization_mode,
        &args.authorization.authorization_policy_file,
        &args.authorization.webhook,
        &args.authorization.authorization_config,
        args.authorization.legacy_flags_set,
    )?;

    // Initialize Metrics Registry
    info!("Initializing Metrics Registry");
    let metrics = Arc::new(MetricsRegistry::new().with_api_server_metrics()?);

    let api_config = rusternetes_api_server::ApiServerConfig {
        bind_address: args.bind_address.clone(),
        advertise_address: args.advertise_address,
        tls: args.tls,
        tls_cert_file: args.tls_cert_file.clone(),
        tls_key_file: args.tls_key_file.clone(),
        tls_self_signed: args.tls_self_signed,
        tls_san: args.tls_san.clone(),
        skip_auth: args.skip_auth,
        client_ca_file: args.client_ca_file.clone(),
        service_node_port_range: args.service_node_port_range,
        service_account,
        authorization_mode: args.authorization.authorization_mode.clone(),
        authorization_policy_file: args.authorization.authorization_policy_file.clone(),
        authorization_webhook: args.authorization.webhook.clone(),
        authorization_config: args.authorization.authorization_config.clone(),
        authorization_legacy_flags_set: args.authorization.legacy_flags_set,
        egress_selector_config_file: args.egress_selector.config_file.clone(),
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

    let crd_controllers = Default::default();
    rusternetes_api_server::startup::register_post_start_hooks(
        storage.clone(),
        api_port,
        service_ranges.api_server_service_ip(),
        rusternetes_api_server::bootstrap::AdvertiseAddress::new(
            args.advertise_address,
            &args.bind_address,
        ),
        service_ranges.cidrs(),
        rusternetes_api_server::startup::ClusterAuthnInputs {
            ca_cert_pem: ca_cert_pem.as_deref(),
            client_ca_file: args.client_ca_file.as_deref(),
            request_header: request_header.as_ref(),
        },
        {
            let c = Arc::clone(&crd_controllers);
            move |s| {
                registry::apiextensions::customresourcedefinition::start_controllers(s, c);
            }
        },
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
        let apf = Arc::new(flow_control_filter::ApfFilter::new(
            engine.clone(),
            flow_control_filter::DEFAULT_REQUEST_TIMEOUT / 4,
        ));
        // The `priority-and-fairness-config-consumer`, `priority-and-fairness-filter`
        // and `storage-object-count-tracker-hook` post-start hooks
        // (server/config.go:921-970). The senders live for the process; the
        // server never signals stop.
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        std::mem::forget(stop_tx);
        let tracker = apf.object_count_tracker();
        flow_control_filter::register_post_start_hooks(
            post_start_hooks::global(),
            engine,
            tracker.clone(),
            stop_rx.clone(),
        );
        // `Store.startObservingCount` (store.go:1638): poll the stores' object
        // counts into the tracker for the list estimator.
        tokio::spawn(flow_control_stats_poller::run(
            storage.clone(),
            tracker,
            flow_control_stats_poller::COUNT_METRIC_POLL_PERIOD,
            stop_rx,
        ));
        flow_control_filter::install_flow_control(apf);
        info!("API Priority and Fairness enabled");
    } else {
        // `storage-object-count-tracker-hook` is registered whenever the
        // tracker exists (server/config.go:961-970), APF or not.
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        std::mem::forget(stop_tx);
        flow_control_filter::register_storage_object_count_tracker_hook(
            post_start_hooks::global(),
            Arc::new(flow_control_object_count::ObjectCountTracker::new()),
            stop_rx,
        );
    }

    // Create shared state with CA certificate and Prometheus client
    let state = Arc::new(
        ApiServerState::new(storage, token_manager, authorizer, metrics, args.skip_auth)
            .with_service_cluster_ip_ranges(&service_ranges)
            .with_ca_cert(ca_cert_pem)
            .with_crd_controllers(crd_controllers)
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
        let client_cas = rusternetes_api_server::request_header_authentication::client_ca_files(
            args.client_ca_file.as_deref(),
            request_header.as_ref(),
        );
        let client_cert_authn = !client_cas.is_empty();
        // `--client-ca-file` / `--requestheader-client-ca-file` are dynamic CA
        // providers: edits to the files take effect without a restart (#3426).
        let (mtls, plain_config) = if client_cert_authn {
            info!(
                "Client certificate authentication enabled (CAs: {})",
                client_cas.join(", ")
            );
            (
                Some(
                    rusternetes_api_server::request_header_authentication::mtls_acceptor(
                        tls_config,
                        args.client_ca_file.as_deref(),
                        request_header.as_ref(),
                    )?,
                ),
                None,
            )
        } else {
            (
                None,
                Some(RustlsConfig::from_config(tls_config.into_server_config()?)),
            )
        };

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

        if let Some((acceptor, _ca_reload)) = mtls {
            let mut server = axum_server::bind(addr).acceptor(acceptor);
            apply_http2_tuning!(server);
            server.serve(app.into_make_service()).await?;
        } else {
            let mut server =
                axum_server::bind_rustls(addr, plain_config.expect("set when no client CA"));
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::feature_gates::{self, Feature};

    /// `--feature-gates` must parse and be applicable (upstream
    /// `featureGate.AddFlag`, component-base/featuregate/feature_gate.go:912).
    #[test]
    #[serial_test::serial]
    fn feature_gates_flag_enables_cross_namespace_volume_data_source() {
        feature_gates::reset_to_defaults();
        let args = Args::try_parse_from([
            "api-server",
            "--feature-gates=CrossNamespaceVolumeDataSource=true",
        ])
        .expect("--feature-gates must be accepted");
        let _guard = feature_gates::with_feature(Feature::CrossNamespaceVolumeDataSource, false);
        apply_feature_gates(&args).unwrap();
        assert!(feature_gates::enabled(
            Feature::CrossNamespaceVolumeDataSource
        ));
        feature_gates::reset_to_defaults();
    }

    /// `--emulated-version` / `--min-compatibility-version` reach the gates
    /// (compatibility/registry.go:344-428): a 1.35-only gate is PreAlpha at 1.34.
    #[test]
    #[serial_test::serial]
    fn emulated_version_flag_changes_gate_defaults() {
        feature_gates::reset_to_defaults();
        let args = Args::try_parse_from([
            "api-server",
            "--emulated-version=1.34",
            "--min-compatibility-version=1.33",
        ])
        .unwrap();
        apply_feature_gates(&args).unwrap();
        assert_eq!(
            feature_gates::emulation_version().to_string(),
            "1.34",
            "emulation version"
        );
        assert!(!feature_gates::enabled(
            Feature::StructuredAuthenticationConfigurationJWKSMetrics
        ));
        let args = Args::try_parse_from(["api-server", "--emulated-version=1.30"]).unwrap();
        let err = apply_feature_gates(&args).unwrap_err().to_string();
        assert!(
            err.contains("emulation version 1.30 is not between [1.32, 1.35]"),
            "{err}"
        );
        feature_gates::reset_to_defaults();
    }

    #[test]
    #[serial_test::serial]
    fn feature_gates_flag_rejects_unknown_gate() {
        feature_gates::reset_to_defaults();
        let args = Args::try_parse_from(["api-server", "--feature-gates=NoSuchGate=true"]).unwrap();
        assert!(apply_feature_gates(&args).is_err());
        feature_gates::reset_to_defaults();
    }
}
