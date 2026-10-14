// mimalloc as the global allocator (off by default; `--features mimalloc`).
// Required for the musl static builds — musl's default allocator is ~10x
// slower under multi-threaded lock contention — and lowers idle RSS (#1041).
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod advanced;
mod data_plane;
mod diagnosis;
#[allow(dead_code)]
mod framework;
#[allow(dead_code)] // library API surface (Framework accessors); partly unused by the binary
mod framework_runtime;
#[allow(dead_code)]
mod plugins;
#[allow(dead_code)] // library API surface (Map accessors); partly unused by the binary
mod profile;
mod scheduler;
#[allow(dead_code)] // library API surface (single-profile loaders); partly unused by the binary
mod scheduler_config;
#[allow(dead_code)] // library API surface (informer delete/new); partly unused by the binary
mod volume_assume;
mod volume_bind;
mod volume_binding;

use anyhow::Result;
use axum::{routing::get, Router};
use clap::Parser;
use rusternetes_common::leader_election::{LeaderElectionConfig, LeaderElector};
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{StorageBackend, StorageConfig};
use scheduler::Scheduler;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "rusternetes-scheduler")]
#[command(about = "Rusternetes Scheduler - Assigns pods to nodes")]
struct Args {
    /// Etcd endpoints (comma-separated)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Storage backend: "etcd" or "sqlite"
    #[arg(long, default_value = "etcd")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

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

    /// Path to a KubeSchedulerConfiguration file (YAML/JSON). Every profile is
    /// defaulted and validated like upstream (`profile.NewMap`); a pod is
    /// scheduled by the profile named in its `spec.schedulerName`, using that
    /// profile's `VolumeBinding` args (bindTimeoutSeconds, shape).
    #[arg(long)]
    config: Option<String>,

    /// Scheduling interval in seconds
    #[arg(long, default_value = "2")]
    interval: u64,

    /// API-server URL. When set, the scheduler runs in API mode: reads come
    /// from pods/nodes informers and writes go through the binding subresource,
    /// status PUT, and event POSTs, with no direct storage handle. Used by the
    /// in-cluster static pod. When unset, the scheduler uses the storage backend
    /// (all-in-one binary path).
    #[arg(long)]
    api_server_url: Option<String>,

    /// Path to a kubeconfig for API mode (client cert/CA + server URL). When
    /// given, its CA validates the api-server's TLS cert; an absent CA means the
    /// connection trusts the system roots (or, with --insecure-skip-tls-verify,
    /// skips verification).
    #[arg(long)]
    kubeconfig: Option<String>,

    /// Skip TLS verification for the api-server connection in API mode.
    #[arg(long)]
    insecure_skip_tls_verify: bool,

    /// `SecureServing` (`--secure-port`, `--bind-address`, `--tls-cert-file`, ...)
    /// of kube-scheduler (cmd/kube-scheduler/app/options/options.go:119-128).
    #[command(flatten)]
    secure_serving: rusternetes_middleware::secure_serving::options::SecureServingFlags,

    /// `Authentication` (`--authentication-kubeconfig`, `--client-ca-file`, ...).
    #[command(flatten)]
    authentication: rusternetes_middleware::secure_serving::options::AuthenticationFlags,

    /// `Authorization` (`--authorization-kubeconfig`, ...).
    #[command(flatten)]
    authorization: rusternetes_middleware::secure_serving::options::AuthorizationFlags,

    /// Enable leader election (for HA)
    #[arg(long)]
    enable_leader_election: bool,

    /// Leader election identity (unique for each instance)
    #[arg(long)]
    leader_election_identity: Option<String>,

    /// Leader election lock key
    #[arg(long, default_value = "/rusternetes/scheduler/leader")]
    leader_election_lock_key: String,

    /// Leader election lease duration in seconds
    #[arg(long, default_value = "15")]
    leader_election_lease_duration: u64,
}

/// `statusz.Install` behind the `ComponentStatusz` gate
/// (cmd/kube-scheduler/app/server.go:394-396). Upstream builds the registry from
/// `compatibility.DefaultBuildEffectiveVersion()` (not the flag-derived
/// effective version) and `pathRecorderMux.ListedPaths()`; the only path this
/// server lists is `/metrics`.
fn install_statusz(app: Router, flags: rusternetes_common::flagz::NamedFlagSetsReader) -> Router {
    // `flagz.Install` behind the `ComponentFlagz` gate, before statusz
    // (cmd/kube-scheduler/app/server.go:388-392; reader from
    // cmd/kube-scheduler/app/options/options.go:270-274).
    let app =
        rusternetes_common::flagz::install_if_enabled(app, "kube-scheduler", Some(Box::new(flags)));
    rusternetes_common::statusz::install_if_enabled(app, "kube-scheduler", || {
        rusternetes_common::statusz::Registry::new(Some(
            rusternetes_common::compatibility::default_build_effective_version(),
        ))
        .with_listed_paths(vec!["/metrics".to_string()])
    })
}

/// The `SecureServing` / `Authentication` / `Authorization` options of
/// kube-scheduler with the command line applied
/// (cmd/kube-scheduler/app/options/options.go:119-128).
fn component_secure_serving(
    args: &Args,
) -> rusternetes_middleware::secure_serving::ComponentSecureServing {
    let mut c = rusternetes_middleware::secure_serving::ComponentSecureServing::for_scheduler();
    c.apply_flags(
        &args.secure_serving,
        &args.authentication,
        &args.authorization,
    );
    c
}

/// `utilerrors.NewAggregate(errs).Error()` of the options `Validate`.
fn aggregate_errors(errs: &[String]) -> String {
    if errs.len() == 1 {
        errs[0].clone()
    } else {
        format!("[{}]", errs.join(", "))
    }
}

/// Serve `/metrics`, `/statusz` and `/flagz` over the secure port behind the
/// authentication and authorization filters, as
/// `cmd/kube-scheduler/app/server.go:250-254` + `:340-352` (`buildHandlerChain`)
/// do. There is no insecure port upstream, and a failure to set up secure
/// serving is fatal.
async fn serve_secure(
    args: &Args,
    gather: impl Fn() -> String + Clone + Send + Sync + 'static,
    flags: rusternetes_common::flagz::NamedFlagSetsReader,
) -> Result<()> {
    let component = component_secure_serving(args);
    let errs = component.validate();
    if !errs.is_empty() {
        anyhow::bail!("{}", aggregate_errors(&errs));
    }
    let app = install_statusz(
        Router::new().route("/metrics", get(move || async move { gather() })),
        flags,
    );
    component
        .serve(app)
        .await
        .map_err(|e| anyhow::anyhow!("failed to start secure serving: {e}"))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = <Args as clap::CommandFactory>::command();
    let matches = cmd.clone().get_matches();
    let flags = rusternetes_common::flagz::flags_from_clap(&cmd, &matches);
    let args = <Args as clap::FromArgMatches>::from_arg_matches(&matches)
        .map_err(|e| e.exit())
        .unwrap();

    rusternetes_common::tracing::init_basic_tracing("scheduler", &args.log_level)?;

    // `ComponentGlobalsRegistry.Set()` (cmd/kube-scheduler/app/server.go:105) then
    // options `Validate` (compatibility/registry.go:430-451).
    let effective_version = rusternetes_common::compatibility::apply_flags(
        &args.emulated_version,
        &args.min_compatibility_version,
        &args.feature_gates,
    )
    .map_err(|e| anyhow::anyhow!("invalid --feature-gates/--emulated-version: {e}"))?;
    // `ComponentGlobalsRegistry.AddMetrics()` (cmd/kube-scheduler/app/server.go:165;
    // component-base/compatibility/registry.go:123-131): record `version_info`.
    rusternetes_common::compat_version_metrics::add_metrics(
        rusternetes_common::compatibility::DEFAULT_KUBE_COMPONENT,
        &effective_version,
    );

    let profiles = match &args.config {
        Some(path) => {
            scheduler_config::load_profiles(path).map_err(|e| anyhow::anyhow!("--config: {e}"))?
        }
        None => profile::Map::new_map(vec![profile::Profile::named(
            profile::DEFAULT_SCHEDULER_NAME,
        )])
        .map_err(|e| anyhow::anyhow!(e))?,
    };

    info!(
        "Starting Rusternetes Scheduler {}",
        rusternetes_common::build_info::version_line()
    );

    // API mode (in-cluster static pod): no storage backend, no leader election
    // (single instance by design). Reads come from informers; writes go through
    // the binding subresource + status + events. Resolve the api-server URL and
    // CA from --kubeconfig (preferred) or the explicit flags.
    if args.api_server_url.is_some() || args.kubeconfig.is_some() {
        return run_api_mode(args, &profiles, flags).await;
    }

    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Using SQLite storage backend at: {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir,
            }
        }
        _ => {
            let endpoints: Vec<String> = args
                .etcd_servers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Connecting to etcd: {:?}", endpoints);
            StorageConfig::Etcd { endpoints }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    // Initialize metrics
    let metrics = Arc::new(MetricsRegistry::new().with_scheduler_metrics()?);
    let metrics_clone = metrics.clone();
    serve_secure(&args, move || metrics_clone.gather(), flags).await?;

    // Leader election
    if args.enable_leader_election {
        let etcd_endpoints: Vec<String> = args
            .etcd_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let identity = args
            .leader_election_identity
            .unwrap_or_else(|| format!("scheduler-{}", Uuid::new_v4()));

        let config = LeaderElectionConfig {
            identity: identity.clone(),
            lock_key: args.leader_election_lock_key,
            lease_duration: args.leader_election_lease_duration,
            renew_interval: args.leader_election_lease_duration / 3,
            retry_interval: 2,
        };

        info!(identity = %identity, "Leader election enabled - starting in follower mode");

        let elector = Arc::new(LeaderElector::new(etcd_endpoints, config).await?);
        let elector_clone = elector.clone();
        tokio::spawn(async move {
            if let Err(e) = elector_clone.run().await {
                tracing::error!("Leader election error: {}", e);
            }
        });

        let scheduler = Arc::new(Scheduler::new_with_profiles(
            storage,
            args.interval,
            profiles.clone(),
        ));
        loop {
            while !elector.is_leader().await {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
            info!("Scheduler starting (leader acquired)");
            if let Err(e) = Arc::clone(&scheduler).run().await {
                tracing::error!("Scheduler error: {}", e);
            }
            if !elector.is_leader().await {
                warn!("Scheduler stopped (lost leadership)");
                continue;
            }
            break;
        }
    } else {
        warn!("Leader election disabled - running in single-instance mode");
        let scheduler = Arc::new(Scheduler::new_with_profiles(
            storage,
            args.interval,
            profiles.clone(),
        ));
        scheduler.run().await?;
    }

    Ok(())
}

/// Run the scheduler as an api-server client (in-cluster static pod). Resolves
/// the api-server URL + CA from `--kubeconfig` (preferred) or the explicit
/// `--api-server-url` flag, builds the [`ApiBackend`] (client + informers +
/// event recorder) and runs the single-instance scheduling loop.
async fn run_api_mode(
    args: Args,
    profiles: &profile::Map,
    flags: rusternetes_common::flagz::NamedFlagSetsReader,
) -> Result<()> {
    use rusternetes_client::http::ApiClient;
    use rusternetes_client::kubeconfig::KubeConfig;

    // Metrics server (same as storage mode).
    let metrics = Arc::new(MetricsRegistry::new().with_scheduler_metrics()?);
    let metrics_clone = metrics.clone();
    serve_secure(&args, move || metrics_clone.gather(), flags).await?;

    // Resolve connection params. A kubeconfig wins for both server URL and CA;
    // --api-server-url overrides the server URL when provided.
    let (base_url, ca_pem, kube_insecure, client_cert, client_key) =
        if let Some(path) = args.kubeconfig.as_deref() {
            let cfg = KubeConfig::load_from_file(&std::path::PathBuf::from(path))?;
            let server = args
                .api_server_url
                .clone()
                .or_else(|| cfg.get_server().ok())
                .ok_or_else(|| anyhow::anyhow!("kubeconfig has no server URL"))?;
            let ca = cfg.get_ca_cert_pem().ok().flatten();
            let insecure = cfg.should_skip_tls_verify().unwrap_or(false);
            // Client cert/key for mTLS auth (#1578).
            let cert = cfg.get_client_cert_pem().ok().flatten();
            let key = cfg.get_client_key_pem().ok().flatten();
            (server, ca, insecure, cert, key)
        } else {
            let server = args.api_server_url.clone().ok_or_else(|| {
                anyhow::anyhow!("--api-server-url is required without --kubeconfig")
            })?;
            (server, None, false, None, None)
        };

    let insecure = args.insecure_skip_tls_verify || kube_insecure;
    info!(
        "Scheduler API mode: api-server={}, ca={}, insecure={}",
        base_url,
        ca_pem.is_some(),
        insecure
    );

    // The CA validates the server's TLS cert; client cert/key (when the
    // kubeconfig provides them) authenticate this component via mTLS (#1578).
    let client = Arc::new(ApiClient::with_tls(
        &base_url,
        insecure,
        ca_pem,
        client_cert.map(|p| p.into_bytes()),
        client_key.map(|p| p.into_bytes()),
        None,
    )?);

    let backend = data_plane::ApiBackend::new(client);
    let scheduler = Arc::new(Scheduler::new_api_with_profiles(
        backend,
        args.interval,
        profiles.clone(),
    ));
    info!("Scheduler starting (API mode, single instance, no leader election)");
    scheduler.run().await?;
    Ok(())
}

#[cfg(test)]
mod secure_serving_flag_tests {
    use super::*;

    // cmd/kube-scheduler/app/options/options.go:119-128 (DefaultKubeSchedulerPort)
    #[test]
    fn default_secure_port_is_10259() {
        let args = Args::try_parse_from(["scheduler"]).unwrap();
        let c = component_secure_serving(&args);
        assert_eq!(c.secure_serving.bind_port, 10259);
    }

    // `--secure-port` / `--bind-address` (apiserver SecureServingOptions.AddFlags)
    #[test]
    fn secure_port_and_bind_address_flags_apply() {
        let args = Args::try_parse_from([
            "scheduler",
            "--secure-port=1234",
            "--bind-address=127.0.0.1",
        ])
        .unwrap();
        let c = component_secure_serving(&args);
        assert_eq!(c.secure_serving.bind_port, 1234);
        assert_eq!(c.secure_serving.bind_address.to_string(), "127.0.0.1");
    }

    // upstream has no plain-HTTP metrics port, so the flag is gone
    #[test]
    fn insecure_metrics_port_flag_is_gone() {
        assert!(Args::try_parse_from(["scheduler", "--metrics-port=8081"]).is_err());
    }
}
