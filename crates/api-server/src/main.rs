// mimalloc as the global allocator (off by default; `--features mimalloc`).
// Required for the musl static builds — musl's default allocator is ~10x
// slower under multi-threaded lock contention — and lowers idle RSS (#1041).
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod admission;
pub use rusternetes_admission_webhook as admission_webhook;
mod bootstrap;
mod legacy_token_tracking;
pub use rusternetes_admission_webhook::cel_evaluators as cel;
mod conversion;
mod dynamic_routes;
mod endpoints;
#[allow(dead_code)]
mod flow_control;
mod gnostic;
mod handlers;
use rusternetes_middleware as middleware;
mod openapi;
mod patch;
mod peer_cert_acceptor;
mod post_start_hooks;
mod prometheus_client;
mod registry;
pub use rusternetes_protobuf as protobuf;
#[allow(dead_code)]
mod response;
mod router;
#[allow(dead_code)]
mod spdy;
// The bin only drives handle_spdy3_exec; the rest of the codec API is exercised
// by the lib + tests, so allow dead_code in the binary build.
#[allow(dead_code)]
mod spdy3;
#[allow(dead_code)]
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
use rusternetes_common::authz::RBACAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{Storage, StorageBackend, StorageConfig};
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
    let token_manager = Arc::new(TokenManager::new_auto(args.jwt_secret.as_bytes()));

    // Initialize Authorizer (RBAC or AlwaysAllow based on skip_auth)
    let authorizer: Arc<dyn rusternetes_common::authz::Authorizer> = if args.skip_auth {
        warn!("⚠️  AUTHENTICATION AND AUTHORIZATION DISABLED - INSECURE MODE");
        warn!("⚠️  Using AlwaysAllowAuthorizer - all requests will be permitted");
        warn!("⚠️  This should ONLY be used in development/testing environments");
        Arc::new(rusternetes_common::authz::AlwaysAllowAuthorizer)
    } else {
        // Node,RBAC union (upstream --authorization-mode=Node,RBAC): a kubelet
        // (system:node:<name>) is authorized for its node's resources by the
        // Node authorizer; everything else falls through to RBAC. Without the
        // Node authorizer, vanilla kubelets are Forbidden on an RBAC-only store
        // (modern clusters do not bind system:nodes to system:node — #1664).
        info!("Initializing Node,RBAC union Authorizer");
        let node: Arc<dyn rusternetes_common::authz::Authorizer> =
            Arc::new(rusternetes_common::authz::NodeAuthorizer);
        let rbac: Arc<dyn rusternetes_common::authz::Authorizer> =
            Arc::new(RBACAuthorizer::new(storage.clone()));
        Arc::new(rusternetes_common::authz::UnionAuthorizer::new(vec![
            node, rbac,
        ]))
    };

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

    if let Err(e) = bootstrap::bootstrap_kubernetes_service(
        storage.clone(),
        api_port,
        service_ranges.api_server_service_ip(),
    )
    .await
    {
        warn!(
            "Failed to bootstrap kubernetes Service Endpoints: {}. Continuing anyway.",
            e
        );
    }
    // systemnamespaces controller (upstream pkg/controlplane/controller/
    // systemnamespaces): NamespaceLifecycle needs these to exist (#2533).
    if let Err(e) = bootstrap::bootstrap_system_namespaces(storage.as_ref()).await {
        warn!(
            "Failed to bootstrap system namespaces: {}. Continuing anyway.",
            e
        );
    }
    // Seed the cluster-admin ClusterRole + binding to system:masters so the
    // cluster admin is authorized on a freshly-bootstrapped (empty) store
    // (upstream bootstrap policy; #1659). Idempotent. The `rbac/bootstrap-roles`
    // PostStartHook (upstream storage_rbac.go:131-179): failing for 30s is fatal.
    let _ = bootstrap::spawn_rbac_bootstrap_roles_hook(storage.clone()).await;
    // scheduling/bootstrap-system-priority-classes PostStartHook (upstream
    // pkg/registry/scheduling/rest/storage_scheduling.go): seeds
    // system-node-critical and system-cluster-critical.
    bootstrap::spawn_system_priority_classes_hook(storage.clone());
    // start-system-namespaces-controller PostStartHook (upstream
    // pkg/controlplane/apiserver/server.go:145): keeps kube-system,
    // kube-public, default, kube-node-lease existing.
    bootstrap::spawn_system_namespaces_controller(storage.clone());
    // Keep the kubernetes endpoint tracking the live api-server IP across
    // container recreates / IP changes (upstream EndpointReconciler, #1188).
    bootstrap::spawn_endpoint_reconciler(
        storage.clone(),
        api_port,
        service_ranges.api_server_service_ip(),
    );

    // Aggregation layer: probe aggregated APIService backends and set their
    // Available condition (upstream kube-aggregator availability controller,
    // which lives in the apiserver — not KCM).
    bootstrap::spawn_apiservice_availability_controller(storage.clone());

    // CRD controllers' resync (upstream post-start hook, apiextensions-apiserver
    // pkg/apiserver/apiserver.go:244-252): retries a CRD left Terminating.
    registry::apiextensions::customresourcedefinition::spawn_resync(storage.clone());
    // crd-informer-synced (apiserver.go:263): not ready until the CRDs are readable.
    registry::apiextensions::customresourcedefinition::spawn_crd_informer_synced_hook(
        storage.clone(),
    );

    // start-legacy-token-tracking-controller (server.go:319-322): keeps
    // kube-system/kube-apiserver-legacy-service-account-token-tracking.
    legacy_token_tracking::spawn_legacy_token_tracking_controller(storage.clone());

    // The `kubernetes` ServiceCIDR, owned by the apiserver-side
    // default-ServiceCIDR controller (upstream
    // `pkg/controlplane/controller/defaultservicecidr`). Reconciles rather than
    // create-once: dual-stack upgrade, flag-mismatch warning, and `Ready=True`
    // only when the persisted CIDRs match this api-server's configuration.
    bootstrap::start_default_servicecidr_controller(storage.clone(), service_ranges.cidrs()).await;

    // kube-system/extension-apiserver-authentication, kept by the
    // apiserver-side ClusterAuthenticationTrust controller (upstream
    // `pkg/controlplane/controller/clusterauthenticationtrust`).
    if let Err(e) =
        bootstrap::bootstrap_extension_apiserver_authentication_rbac(storage.clone()).await
    {
        warn!(
            "Failed to bootstrap extension-apiserver-authentication RBAC: {e}. Continuing anyway."
        );
    }
    bootstrap::spawn_cluster_authentication_trust_controller(
        storage.clone(),
        bootstrap::cluster_authentication_info(ca_cert_pem.as_deref()),
    );

    // Create default StorageClass (like k3s/kind ship with a default)
    {
        let sc_key = rusternetes_storage::build_key("storageclasses", None, "standard");
        if storage.get::<serde_json::Value>(&sc_key).await.is_err() {
            let storage_class = serde_json::json!({
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {
                    "name": "standard",
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    "annotations": {
                        "storageclass.kubernetes.io/is-default-class": "true"
                    }
                },
                "provisioner": "rusternetes.io/hostpath",
                "reclaimPolicy": "Delete",
                "volumeBindingMode": "WaitForFirstConsumer"
            });
            if let Err(e) = storage.create(&sc_key, &storage_class).await {
                warn!("Failed to create default StorageClass: {}", e);
            } else {
                info!("Created default StorageClass 'standard' with rusternetes.io/hostpath provisioner");
            }
        }
    }

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
