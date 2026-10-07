pub mod admission;
pub use rusternetes_admission_webhook as admission_webhook;
pub mod bootstrap;
pub mod legacy_token_tracking;
pub use rusternetes_admission_webhook::cel_evaluators as cel;
pub use rusternetes_middleware::cbor;
pub mod conversion;
pub mod dynamic_routes;
pub mod endpoints;
#[allow(dead_code)]
pub mod flow_control;
pub mod gnostic;
pub mod handlers;
pub use rusternetes_middleware as middleware;
pub mod openapi;
pub mod patch;
pub mod peer_cert_acceptor;
pub mod post_start_hooks;
pub mod prometheus_client;
pub mod registry;
pub use rusternetes_protobuf as protobuf;
#[allow(dead_code)]
pub mod response;
pub mod router;
#[allow(dead_code)]
pub mod spdy;
pub mod spdy3;
#[allow(dead_code)]
pub mod spdy_handlers;
pub mod ssa;
pub mod state;
#[allow(dead_code)]
pub mod streaming;
pub mod watch_cache;

use axum_server::tls_rustls::RustlsConfig;
use rusternetes_common::auth::TokenManager;
use rusternetes_common::authz::RBACAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::tls::TlsConfig;
use rusternetes_storage::{Storage, StorageBackend};
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

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:6443".to_string(),
            jwt_secret: "rusternetes-secret-change-in-production".to_string(),
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

    let token_manager = Arc::new(TokenManager::new_auto(config.jwt_secret.as_bytes()));

    let authorizer: Arc<dyn rusternetes_common::authz::Authorizer> = if config.skip_auth {
        warn!("Authentication and authorization disabled - insecure mode");
        Arc::new(rusternetes_common::authz::AlwaysAllowAuthorizer)
    } else {
        info!("Initializing RBAC Authorizer");
        Arc::new(RBACAuthorizer::new(storage.clone()))
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
    // `rbac/bootstrap-roles` PostStartHook (upstream pkg/registry/rbac/rest/
    // storage_rbac.go:131-179), same as main.rs: the SAME hook, awaited so a
    // fresh store is never served with an empty RBAC policy (#2490).
    let _ = bootstrap::spawn_rbac_bootstrap_roles_hook(storage.clone()).await;
    // scheduling/bootstrap-system-priority-classes PostStartHook (upstream
    // pkg/registry/scheduling/rest/storage_scheduling.go), same as main.rs.
    bootstrap::spawn_system_priority_classes_hook(storage.clone());
    // start-system-namespaces-controller (server.go:145).
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

    // start-legacy-token-tracking-controller (server.go:319-322).
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
