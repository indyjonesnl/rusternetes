//! Rusternetes — all-in-one Kubernetes in a single binary.
//!
//! Runs the API server, scheduler, controller manager, kubelet, kube-proxy,
//! and cluster DNS as concurrent tokio tasks sharing a single storage backend.
//!
//! Usage:
//!   rusternetes                                   # SQLite at ./data/rusternetes.db
//!   rusternetes --data-dir /var/lib/rusternetes.db # custom path
//!   rusternetes --etcd-servers http://etcd:2379    # use etcd instead

use anyhow::Result;
use clap::Parser;
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::sync::Arc;
use tracing::{error, info};

// Heap-profiling allocator (off by default; `--features dhat-heap`). Attributes
// the all-in-one's idle RAM by backtrace for #1138. Dumps `dhat-heap.json` when
// the profiler guard in `main` drops on a clean (SIGINT) shutdown.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

// mimalloc as the global allocator (off by default; `--features mimalloc`).
// Required for the musl static builds — musl's default allocator is ~10x slower
// under multi-threaded lock contention — and lowers idle RSS (#1041). Gated so
// it never collides with dhat's allocator: when both features are on, dhat wins
// (heap profiling keeps working).
#[cfg(all(feature = "mimalloc", not(feature = "dhat-heap")))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "rusternetes")]
#[command(about = "Rusternetes — all-in-one Kubernetes in a single binary")]
#[command(version)]
struct Args {
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

    /// Storage backend: "sqlite", "etcd", or "redis"
    #[arg(long, default_value = "sqlite")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Enable the in-process watch event bus (all-in-one fast path, #1039).
    /// On by default; pass `--in-process-bus false` to use native backend
    /// watches (e.g. for A/B benchmarking).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    in_process_bus: bool,

    /// Etcd endpoints, comma-separated (only used when --storage-backend=etcd)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Redis URL (only used when --storage-backend=redis)
    #[arg(long, default_value = "redis://localhost:6379")]
    redis_url: String,

    /// API server bind address
    #[arg(long, default_value = "0.0.0.0:6443")]
    bind_address: String,

    /// Run a CSI external-attacher for a driver: `<driver-name>=<controller
    /// socket path>`. Repeatable. Same flag as the standalone controller-manager.
    #[arg(long = "csi-attacher", value_name = "DRIVER=SOCKET")]
    csi_attachers: Vec<String>,

    /// Run the CSI half of the AttachDetachController (#3048). Off by default;
    /// enable together with `--csi-attacher`.
    #[arg(long = "attach-detach-controller", default_value_t = false)]
    attach_detach_controller: bool,

    /// Node name for the embedded kubelet
    #[arg(long, default_value = "node-1")]
    node_name: String,

    /// Volume directory for pod volumes
    #[arg(long, default_value = "./data/volumes")]
    volume_dir: String,

    /// Cluster DNS IP
    #[arg(long, default_value = "10.96.0.10")]
    cluster_dns: String,

    /// Container network name
    #[arg(long, default_value = "rusternetes-network")]
    network: String,

    /// Enable TLS with self-signed certificates
    #[arg(long)]
    tls: bool,

    /// TLS certificate file
    #[arg(long)]
    tls_cert_file: Option<String>,

    /// TLS private key file
    #[arg(long)]
    tls_key_file: Option<String>,

    /// TLS Subject Alternative Names (comma-separated)
    #[arg(long, default_value = "localhost,127.0.0.1")]
    tls_san: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Controller sync interval in seconds
    #[arg(long, default_value = "5")]
    sync_interval: u64,

    /// Scheduler interval in seconds
    #[arg(long, default_value = "2")]
    scheduler_interval: u64,

    /// Path to a KubeSchedulerConfiguration file (upstream kube-scheduler
    /// `--config`). Only `profiles[0].pluginConfig[VolumeBinding].args` is
    /// consumed, as in the standalone scheduler binary (#3202).
    #[arg(long)]
    scheduler_config: Option<String>,

    /// Kubelet sync interval in seconds
    #[arg(long, default_value = "3")]
    kubelet_sync_interval: u64,

    /// Kube-proxy sync interval in seconds
    #[arg(long, default_value = "1")]
    proxy_sync_interval: u64,

    /// ClusterIP range(s) for Services: one CIDR, or two of different IP
    /// families (dual-stack), comma-separated
    /// (`--service-cluster-ip-range`). Handed to the embedded api-server; the
    /// `kubernetes` Service address and the kubelet's
    /// `KUBERNETES_SERVICE_HOST` derive from its first address.
    #[arg(long, default_value = rusternetes_api_server::registry::core::service::ipranges::DEFAULT_SERVICE_CLUSTER_IP_RANGE)]
    service_cluster_ip_range: String,

    /// ClusterIP CIDR — must match the apiserver's
    /// `--service-cluster-ip-range`. Used by kube-proxy to scope its
    /// POSTROUTING MASQUERADE rule.
    #[arg(long, default_value = rusternetes_kube_proxy::iptables::DEFAULT_CLUSTER_CIDR)]
    cluster_cidr: String,

    /// NodePort range in iptables `start:end` form — must match the
    /// apiserver's `--service-node-port-range`. Hyphen also accepted.
    #[arg(long, default_value = rusternetes_kube_proxy::iptables::DEFAULT_NODEPORT_RANGE)]
    node_port_range: String,

    /// Skip authentication (insecure, for development)
    #[arg(long, default_value = "true")]
    skip_auth: bool,

    /// Disable kube-proxy (useful when iptables is not available)
    #[arg(long)]
    disable_proxy: bool,

    /// Disable the in-process DNS server (fall back to the standalone
    /// rusternetes-dns container, or to the CoreDNS Pod from
    /// bootstrap-cluster.yaml when USE_RUSTERNETES_DNS=0).
    #[arg(long)]
    disable_dns: bool,

    /// Bind address for the in-process DNS server (UDP+TCP). Pods reach
    /// it via the kube-dns Service ClusterIP; kube-proxy DNATs to this
    /// address. Default binds all interfaces inside the container.
    #[arg(long, default_value = "0.0.0.0:53")]
    dns_bind: String,

    /// Path to the console SPA build directory (enables web console at /console/)
    #[arg(long)]
    console_dir: Option<String>,

    /// Kubernetes service host override for pods (e.g. "api-server" in containerized deployments).
    /// Falls back to KUBERNETES_SERVICE_HOST_OVERRIDE env var if not set.
    #[arg(long)]
    kubernetes_service_host: Option<String>,

    /// Client CA certificate file for mTLS client certificate authentication
    #[arg(long)]
    client_ca_file: Option<String>,

    /// Amount of time to retain events, as a Go duration (`--event-ttl`). `0`
    /// keeps them forever.
    #[arg(
        long,
        default_value = "1h",
        value_parser = rusternetes_api_server::registry::core::event::parse_event_ttl
    )]
    event_ttl: u64,

    /// `--service-account-*` / `--api-audiences` (#2713).
    #[command(flatten)]
    service_account: rusternetes_api_server::ServiceAccountArgs,

    /// `--authorization-mode` (#2854).
    #[command(flatten)]
    authorization: rusternetes_api_server::authorizer::AuthorizationArgs,
}

fn main() -> Result<()> {
    // Hold the dhat profiler for the whole process; it writes dhat-heap.json
    // when dropped (after block_on returns on a SIGINT shutdown).
    #[cfg(feature = "dhat-heap")]
    let _dhat = dhat::Profiler::new_heap();

    // The all-in-one packs the api-server, scheduler, controller-manager,
    // kubelet, and kube-proxy into one tokio runtime. The api-server's request
    // path (routing → admission → mutating/validating webhooks → watch fan-out)
    // produces deep async call chains, and creating pods overflows tokio's
    // default 2 MiB worker-thread stack (`fatal runtime error: stack overflow`).
    // Multi-container compose runs each component as its own process and isn't
    // affected; only the single-runtime all-in-one is. Give workers an 8 MiB
    // stack — virtual reservation only (Linux commits stack pages lazily), so
    // no idle-RAM cost. See #1135.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()?
        .block_on(async_main())
}

/// Apply `--emulated-version`, `--min-compatibility-version` and
/// `--feature-gates` process-wide: `ComponentGlobalsRegistry.Set()` then
/// `Validate()` (staging/src/k8s.io/component-base/compatibility/registry.go:344-451).
fn apply_feature_gates(args: &Args) -> Result<()> {
    rusternetes_common::compatibility::apply_flags(
        &args.emulated_version,
        &args.min_compatibility_version,
        &args.feature_gates,
    )
    .map_err(|e| anyhow::anyhow!("invalid --feature-gates/--emulated-version: {e}"))?;
    Ok(())
}

/// Build the embedded scheduler config, loading `--scheduler-config` when set.
fn scheduler_config_from_args(args: &Args) -> Result<rusternetes_scheduler::SchedulerConfig> {
    let volume_binding_args = match &args.scheduler_config {
        Some(path) => rusternetes_scheduler::scheduler_config::load_volume_binding_args(path)
            .map_err(|e| anyhow::anyhow!("--scheduler-config: {e}"))?,
        None => Default::default(),
    };
    Ok(rusternetes_scheduler::SchedulerConfig {
        interval: args.scheduler_interval,
        volume_binding_args,
    })
}

async fn async_main() -> Result<()> {
    // `fs.Changed` for the legacy authorization flags needs the raw matches.
    let matches = <Args as clap::CommandFactory>::command().get_matches();
    let mut args = <Args as clap::FromArgMatches>::from_arg_matches(&matches)
        .map_err(|e| e.exit())
        .unwrap();
    args.authorization.mark_legacy_flags_set(&matches);
    apply_feature_gates(&args)?;
    // Gates first: the StorageCapacityScoring default for VolumeBinding args reads them.
    let sched_config = scheduler_config_from_args(&args)?;

    rusternetes_common::tracing::init_basic_tracing("rusternetes", &args.log_level)?;

    rusternetes_common::dump::install_panic_hook("rusternetes");

    info!(
        "Starting Rusternetes (all-in-one) {}",
        rusternetes_common::build_info::version_line()
    );

    // Initialize storage — all components share one instance
    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Storage: SQLite at {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir,
            }
        }
        "etcd" => {
            let endpoints: Vec<String> = args
                .etcd_servers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Storage: etcd at {:?}", endpoints);
            StorageConfig::Etcd { endpoints }
        }
        #[cfg(feature = "redis")]
        "redis" => {
            info!("Storage: Redis at {}", args.redis_url);
            StorageConfig::Redis {
                url: args.redis_url,
            }
        }
        other => {
            anyhow::bail!(
                "Unknown storage backend: {}. Use 'sqlite', 'etcd', or 'redis'.",
                other
            );
        }
    };
    let mut storage = StorageBackend::new(storage_config).await?;
    if args.in_process_bus {
        storage.enable_event_bus();
        info!("In-process watch event bus: ENABLED (internal consumers use the fast path)");
    } else {
        info!("In-process watch event bus: disabled (native backend watches)");
    }
    let storage = Arc::new(storage);

    info!("Storage initialized, starting components...");

    // --- API Server ---
    // `validateClusterIPFlags`: refuse to start on a bad range.
    let service_ranges =
        rusternetes_api_server::registry::core::service::ipranges::ServiceIpRanges::parse(
            &args.service_cluster_ip_range,
        )
        .map_err(|e| anyhow::anyhow!(e))?;
    // Thread --event-ttl to every in-process EventRecorder (#3026).
    rusternetes_storage::event_recorder::set_process_event_ttl(args.event_ttl);
    let api_storage = storage.clone();
    let mut api_config = rusternetes_api_server::ApiServerConfig {
        bind_address: args.bind_address.clone(),
        tls: args.tls,
        tls_cert_file: args.tls_cert_file.clone(),
        tls_key_file: args.tls_key_file.clone(),
        tls_self_signed: args.tls,
        tls_san: args.tls_san.clone(),
        skip_auth: args.skip_auth,
        console_dir: args.console_dir.map(std::path::PathBuf::from),
        client_ca_file: args.client_ca_file.clone(),
        service_cluster_ip_range: args.service_cluster_ip_range.clone(),
        event_ttl: args.event_ttl,
        service_account: args.service_account.to_options(),
        authorization_mode: args.authorization.authorization_mode.clone(),
        authorization_policy_file: args.authorization.authorization_policy_file.clone(),
        authorization_webhook: args.authorization.webhook.clone(),
        authorization_config: args.authorization.authorization_config.clone(),
        authorization_legacy_flags_set: args.authorization.legacy_flags_set,
        ..Default::default()
    };
    let prepared_tls = rusternetes_api_server::prepare_tls_for_config(&api_config)?;
    let cm_ca_pem = prepared_tls
        .as_ref()
        .and_then(|prepared| prepared.ca_cert_pem().map(str::to_string));
    api_config.prepared_tls = prepared_tls;
    let api_handle = tokio::spawn(async move {
        if let Err(e) = rusternetes_api_server::run(api_storage, api_config).await {
            error!("API server error: {}", e);
        }
    });

    // Give API server a moment to bind before starting clients
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Embedded clients (scheduler, controller-manager, DNS) reach the
    // api-server over loopback, matching the trust boundary the compose path
    // enforces: only the api-server touches storage (#1128). The self-signed
    // loopback cert is skipped when TLS is on; skip_auth is the all-in-one
    // default, so no bearer token is needed (real authn tracked in #1129). The
    // port is derived from the api-server bind address; fall back to 6443.
    let api_port = args
        .bind_address
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6443);
    let loopback_scheme = if args.tls { "https" } else { "http" };
    let loopback_url = format!("{loopback_scheme}://127.0.0.1:{api_port}");

    // --- Scheduler ---
    let sched_client = Arc::new(rusternetes_client::http::ApiClient::new(
        &loopback_url,
        args.tls,
        None,
    )?);
    info!("Scheduler reading cluster state from embedded api-server at {loopback_url}");
    tokio::spawn(async move {
        if let Err(e) = rusternetes_scheduler::run_with_api(sched_client, sched_config).await {
            error!("Scheduler error: {}", e);
        }
    });

    // --- Controller Manager ---
    // The HPA metrics client talks to the in-process api-server over loopback
    // using client-cert mTLS (only when cert+key are configured).
    let metrics_config = args
        .tls_cert_file
        .as_ref()
        .zip(args.tls_key_file.as_ref())
        .map(|(cert, key)| {
            rusternetes_controller_manager::controllers::hpa_metrics_client::HttpMetricsConfig {
                api_server_url: format!("https://127.0.0.1:{api_port}"),
                ca_pem: None,
                ca_cert_path: Some(args.client_ca_file.clone().unwrap_or_else(|| cert.clone())),
                client_cert_path: Some(cert.clone()),
                client_key_path: Some(key.clone()),
                token: None,
                insecure_skip_tls_verify: true,
            }
        });
    // `--csi-attacher DRIVER=SOCKET`, same spec as the standalone binary (#3128).
    let csi_attachers = args
        .csi_attachers
        .iter()
        .map(|spec| {
            spec.split_once('=')
                .map(|(d, s)| (d.to_string(), s.to_string()))
                .ok_or_else(|| {
                    anyhow::anyhow!("--csi-attacher {spec:?}: expected <driver-name>=<socket path>")
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let cm_config = rusternetes_controller_manager::ControllerManagerConfig {
        sync_interval: args.sync_interval,
        metrics_config,
        ca_cert_pem: cm_ca_pem,
        // All-in-one does not expose node-IPAM flags; pod-CIDR allocation is a
        // multi-node compose concern (flannel stack runs the standalone CM).
        node_ipam: None,
        csi_attachers,
        attach_detach_controller: args.attach_detach_controller,
        legacy_sa_token_clean_up_period:
            rusternetes_controller_manager::controllers::legacy_serviceaccount_token_cleaner::DEFAULT_CLEAN_UP_PERIOD,
    };
    // The controller-manager reaches cluster state through the embedded
    // api-server over the same loopback client as scheduler/DNS (#1128),
    // rather than the StorageBackend directly.
    let cm_client = Arc::new(rusternetes_client::http::ApiClient::new(
        &loopback_url,
        args.tls,
        None,
    )?);
    info!("Controller manager reading cluster state from embedded api-server at {loopback_url}");
    tokio::spawn(async move {
        if let Err(e) = rusternetes_controller_manager::run_with_api(cm_client, cm_config).await {
            error!("Controller manager error: {}", e);
        }
    });

    // Pod networking is provided entirely by the CNI plugin (via the
    // kubelet's CRI backend); rusternetes runs no in-process network stack.

    // --- Kubelet ---
    let kubelet_storage = storage.clone();
    let kubelet_config = rusternetes_kubelet::KubeletConfig {
        node_name: args.node_name.clone(),
        volume_dir: args.volume_dir,
        cluster_dns: args.cluster_dns,
        cluster_domain: "cluster.local".to_string(),
        network: args.network,
        sync_interval: args.kubelet_sync_interval,
        metrics_port: 10250,
        kubernetes_service_host: args
            .kubernetes_service_host
            .clone()
            .or_else(|| std::env::var("KUBERNETES_SERVICE_HOST_OVERRIDE").ok())
            .unwrap_or_else(|| service_ranges.api_server_service_ip().to_string()),
    };
    tokio::spawn(async move {
        if let Err(e) = rusternetes_kubelet::run(kubelet_storage, kubelet_config).await {
            error!("Kubelet error: {}", e);
        }
    });

    // --- Kube-proxy ---
    if !args.disable_proxy {
        let proxy_storage = storage.clone();
        let proxy_config = rusternetes_kube_proxy::KubeProxyConfig {
            node_name: args.node_name,
            sync_interval: args.proxy_sync_interval,
            cluster_cidr: args.cluster_cidr,
            // Accept hyphen form as a convenience; iptables wants colon.
            nodeport_range: args.node_port_range.replace('-', ":"),
            // The all-in-one binary is a single node in a single network
            // namespace: there is no peer pod CIDR to route to and no per-node
            // conflist to derive, so the node-network agent stays off and the
            // image's cluster-wide CNI config keeps applying (#1691).
            node_net: None,
        };
        tokio::spawn(async move {
            if let Err(e) = rusternetes_kube_proxy::run(proxy_storage, proxy_config).await {
                error!("Kube-proxy error: {}", e);
            }
        });
    } else {
        info!("Kube-proxy disabled");
    }

    // --- DNS ---
    if !args.disable_dns {
        let dns_bind: std::net::SocketAddr = args
            .dns_bind
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid --dns-bind {:?}: {}", args.dns_bind, e))?;
        let dns_config = rusternetes_dns::DnsConfig {
            udp_bind: dns_bind,
            tcp_bind: dns_bind,
            ..rusternetes_dns::DnsConfig::default()
        };
        // DNS reaches cluster state through the embedded api-server over
        // loopback rather than the StorageBackend directly (see the loopback
        // client note above the scheduler block, #1128).
        let dns_client = Arc::new(rusternetes_client::http::ApiClient::new(
            &loopback_url,
            args.tls,
            None,
        )?);
        info!("DNS reading cluster state from embedded api-server at {loopback_url}");
        tokio::spawn(async move {
            if let Err(e) = rusternetes_dns::run_with_api(dns_client, dns_config).await {
                error!("DNS server error: {}", e);
            }
        });
    } else {
        info!("In-process DNS disabled");
    }

    info!("All components started");

    // The API server task blocks on its listener — wait for it or ctrl-c
    tokio::select! {
        result = api_handle => {
            if let Err(e) = result {
                error!("API server task panicked: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, stopping rusternetes");
        }
    }

    Ok(())
}

#[cfg(test)]
mod feature_gates_flag_tests {
    use super::*;
    use rusternetes_common::feature_gates::{enabled, reset_to_defaults, Feature};
    use serial_test::serial;

    fn parse(gates: &str) -> Args {
        let mut v: Vec<&str> = vec!["rusternetes"];
        v.push(gates);
        Args::try_parse_from(v).unwrap()
    }

    /// Upstream `--feature-gates` (component-base featuregate `Set`,
    /// feature_gate.go:340-359) is applied process-wide from the flag.
    #[test]
    #[serial]
    fn feature_gates_flag_applies() {
        reset_to_defaults();
        apply_feature_gates(&parse("--feature-gates=StorageCapacityScoring=true")).unwrap();
        assert!(enabled(Feature::StorageCapacityScoring));
        reset_to_defaults();
    }

    /// feature_gate.go:385-389.
    #[test]
    #[serial]
    fn feature_gates_flag_rejects_unknown_gate() {
        reset_to_defaults();
        let err = apply_feature_gates(&parse("--feature-gates=fooBarBaz=true")).unwrap_err();
        assert!(
            err.to_string()
                .contains("unrecognized feature gate: fooBarBaz"),
            "{err}"
        );
    }

    /// #3202: all-in-one accepts --scheduler-config like the standalone
    /// scheduler's --config.
    #[test]
    #[serial]
    fn scheduler_config_flag_feeds_volume_binding_args() {
        let dir = std::env::temp_dir().join(format!("aio-sched-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("c.yaml");
        std::fs::write(
            &f,
            "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      bindTimeoutSeconds: 7\n",
        )
        .unwrap();
        let args = Args::try_parse_from(["rusternetes", "--scheduler-config", f.to_str().unwrap()])
            .unwrap();
        let cfg = scheduler_config_from_args(&args).unwrap();
        assert_eq!(cfg.volume_binding_args.bind_timeout_seconds, Some(7));
        let none = scheduler_config_from_args(&parse("--scheduler-interval=3")).unwrap();
        assert_eq!(none.interval, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
