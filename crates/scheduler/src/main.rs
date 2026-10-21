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
mod node_volume_limits;
#[allow(dead_code)]
mod plugins;
#[allow(dead_code)] // library API surface (Map accessors); partly unused by the binary
mod profile;
#[allow(dead_code)] // library API surface (queue introspection); partly unused by the binary
mod queue;
mod resource_events;
mod scheduler;
#[allow(dead_code)] // library API surface (single-profile loaders); partly unused by the binary
mod scheduler_config;
#[allow(dead_code)] // library API surface; test-only helpers unused by the binary
mod score;
#[allow(dead_code)] // library API surface (informer delete/new); partly unused by the binary
mod volume_assume;
mod volume_bind;
mod volume_binding;

use anyhow::Result;
use axum::{routing::get, Router};
use clap::Parser;
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

    /// Metrics server port
    #[arg(long, default_value = "8081")]
    metrics_port: u16,

    /// Enable leader election (for HA)
    #[arg(long)]
    enable_leader_election: bool,

    /// Leader election identity (unique for each instance)
    #[arg(long)]
    leader_election_identity: Option<String>,

    /// Leader election lock key (etcd key of the retired etcd-lease elector;
    /// accepted and ignored, the lock is now a Lease -- see
    /// `--leader-election-resource-namespace` / `--leader-election-resource-name`).
    #[arg(long, hide = true)]
    leader_election_lock_key: Option<String>,

    /// Namespace of the leader-election Lease
    /// (`--leader-elect-resource-namespace`, kube-system by default).
    #[arg(
        long,
        alias = "leader-elect-resource-namespace",
        default_value = "kube-system"
    )]
    leader_election_resource_namespace: String,

    /// Name of the leader-election Lease (`--leader-elect-resource-name`).
    #[arg(
        long,
        alias = "leader-elect-resource-name",
        default_value = "kube-scheduler"
    )]
    leader_election_resource_name: String,

    /// Leader election lease duration in seconds
    /// (`LeaseDuration`, 15s: component-base v1alpha1 defaults.go:37-39)
    #[arg(long, default_value = "15")]
    leader_election_lease_duration: u64,

    /// Seconds the acting leader retries refreshing leadership before giving up
    /// (`RenewDeadline`, 10s: defaults.go:40-42)
    #[arg(long, default_value = "10")]
    leader_election_renew_deadline: u64,

    /// Seconds between tries of leader-election actions
    /// (`RetryPeriod`, 2s: defaults.go:43-45)
    #[arg(long, default_value = "2")]
    leader_election_retry_period: u64,

    /// Upstream `--leader-elect` (leaderelectionconfig.go:26-30), a pflag
    /// bool: bare or `=true` enables; overrides `--enable-leader-election`.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    leader_elect: Option<bool>,

    /// Upstream `--leader-elect-lease-duration` (Go duration, e.g. `15s`).
    #[arg(long)]
    leader_elect_lease_duration: Option<String>,

    /// Upstream `--leader-elect-renew-deadline` (Go duration, e.g. `10s`).
    #[arg(long)]
    leader_elect_renew_deadline: Option<String>,

    /// Upstream `--leader-elect-retry-period` (Go duration, e.g. `2s`).
    #[arg(long)]
    leader_elect_retry_period: Option<String>,

    /// Upstream `--leader-elect-resource-lock`; only `leases` is supported
    /// (leaderelectionconfig.go:48-50).
    #[arg(long)]
    leader_elect_resource_lock: Option<String>,

    /// Upstream `--profiling` (DebuggingOptions). Accepted for argv
    /// compatibility; the scheduler exposes no pprof handlers, so it is ignored.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    profiling: Option<bool>,

    /// Upstream `--contention-profiling`. Accepted and ignored, as above.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    contention_profiling: Option<bool>,
}

/// Effective leader-election settings after resolving the upstream flag
/// names (`--leader-elect*`) over the native `--leader-election-*` ones.
#[derive(Debug, PartialEq)]
struct LeaderElectionSettings {
    enabled: bool,
    lease_duration: std::time::Duration,
    renew_deadline: std::time::Duration,
    retry_period: std::time::Duration,
}

impl Args {
    /// Upstream flag names win over the native ones when given.
    fn leader_election_settings(&self) -> Result<LeaderElectionSettings, String> {
        let dur =
            |flag: &str, v: &Option<String>, native: u64| -> Result<std::time::Duration, String> {
                match v {
                    None => Ok(std::time::Duration::from_secs(native)),
                    Some(s) => {
                        let ns = rusternetes_common::go_duration::parse_go_duration(s)
                            .map_err(|e| format!("invalid value {s:?} for --{flag}: {e}"))?;
                        Ok(std::time::Duration::from_nanos(ns.max(0) as u64))
                    }
                }
            };
        if let Some(lock) = &self.leader_elect_resource_lock {
            if lock != "leases" {
                return Err(format!(
                    "invalid --leader-elect-resource-lock {lock:?}: only \"leases\" is supported"
                ));
            }
        }
        Ok(LeaderElectionSettings {
            enabled: self.leader_elect.unwrap_or(self.enable_leader_election),
            lease_duration: dur(
                "leader-elect-lease-duration",
                &self.leader_elect_lease_duration,
                self.leader_election_lease_duration,
            )?,
            renew_deadline: dur(
                "leader-elect-renew-deadline",
                &self.leader_elect_renew_deadline,
                self.leader_election_renew_deadline,
            )?,
            retry_period: dur(
                "leader-elect-retry-period",
                &self.leader_elect_retry_period,
                self.leader_election_retry_period,
            )?,
        })
    }
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

    if args.profiling == Some(true) || args.contention_profiling == Some(true) {
        warn!("--profiling/--contention-profiling are accepted and ignored: no pprof handlers are served");
    }
    let le = args
        .leader_election_settings()
        .map_err(|e| anyhow::anyhow!(e))?;

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

    let metrics_addr = format!("0.0.0.0:{}", args.metrics_port);
    info!("Starting metrics server on {}", metrics_addr);

    tokio::spawn(async move {
        let app = install_statusz(
            Router::new().route("/metrics", get(|| async move { metrics_clone.gather() })),
            flags,
        );
        let listener = tokio::net::TcpListener::bind(&metrics_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // cmd/kube-scheduler/app/server.go:150-156: SetupSignalHandler (SIGTERM+SIGINT)
    // cancels the run context.
    let ctx = rusternetes_common::signal::setup_signal_context();

    // Leader election
    if le.enabled {
        let identity = args
            .leader_election_identity
            .unwrap_or_else(|| format!("scheduler-{}", Uuid::new_v4()));
        if args.leader_election_lock_key.is_some() {
            warn!("--leader-election-lock-key is ignored: the lock is a coordination.k8s.io Lease");
        }

        info!(identity = %identity, "Leader election enabled - starting in follower mode");

        // cmd/kube-scheduler/app/server.go:310-322 OnStoppedLeading: exit 0 if
        // asked to terminate, otherwise "Leaderelection lost" and exit 1.
        let stopped_ctx = ctx.clone();
        let elector = Arc::new(rusternetes_common::leaderelection::LeaderElector::new(
            rusternetes_common::leaderelection::ElectionConfig {
                lock: Arc::new(
                    rusternetes_storage::lease_lock::LeaseLock::new(
                        storage.clone(),
                        args.leader_election_resource_namespace.clone(),
                        args.leader_election_resource_name.clone(),
                        identity.clone(),
                    )
                    // LockConfig.EventRecorder (options.go makeLeaderElectionConfig).
                    .with_event_recorder(
                        rusternetes_storage::EventRecorder::new(storage.clone()),
                        "default-scheduler",
                    ),
                ),
                lease_duration: le.lease_duration,
                renew_deadline: le.renew_deadline,
                retry_period: le.retry_period,
                // ReleaseOnCancel: true (cmd/kube-scheduler/app/options/options.go:375).
                release_on_cancel: true,
                name: args.leader_election_resource_name.clone(),
                callbacks: rusternetes_common::leaderelection::LeaderCallbacks {
                    on_started_leading: Arc::new(|_| {}),
                    on_stopped_leading: Arc::new(move || {
                        if stopped_ctx.is_cancelled() {
                            info!("Requested to terminate, exiting");
                            std::process::exit(0);
                        }
                        tracing::error!("Leaderelection lost");
                        std::process::exit(1);
                    }),
                    on_new_leader: None,
                },
            },
        )?);
        // `leaderElector.Run(ctx)`; on cancel it releases the lock before
        // returning, then OnStoppedLeading exits 0.
        let election = {
            let (elector, ctx) = (elector.clone(), ctx.clone());
            tokio::spawn(async move { elector.run(ctx).await })
        };

        let scheduler = Arc::new(Scheduler::new_with_profiles(
            storage,
            args.interval,
            profiles.clone(),
        ));
        loop {
            while !elector.is_leader() {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {}
                    _ = ctx.cancelled() => {
                        info!("Requested to terminate, exiting");
                        let _ = election.await;
                        return Ok(());
                    }
                }
            }
            info!("Scheduler starting (leader acquired)");
            tokio::select! {
                r = Arc::clone(&scheduler).run() => {
                    if let Err(e) = r {
                        tracing::error!("Scheduler error: {}", e);
                    }
                }
                _ = ctx.cancelled() => {
                    // OnStoppedLeading with ctx.Done(): "Requested to terminate, exiting"
                    // (server.go:310-316). Wait for release() to finish first.
                    info!("Requested to terminate, exiting");
                    let _ = election.await;
                    return Ok(());
                }
            }
            if ctx.is_cancelled() {
                let _ = election.await;
                return Ok(());
            }
            if !elector.is_leader() {
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
        tokio::select! {
            r = scheduler.run() => r?,
            _ = ctx.cancelled() => info!("Requested to terminate, exiting"),
        }
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
    let metrics_addr = format!("0.0.0.0:{}", args.metrics_port);
    info!("Starting metrics server on {}", metrics_addr);
    tokio::spawn(async move {
        let app = install_statusz(
            Router::new().route("/metrics", get(|| async move { metrics_clone.gather() })),
            flags,
        );
        let listener = tokio::net::TcpListener::bind(&metrics_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

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
    // cmd/kube-scheduler/app/server.go:150-156: SetupSignalHandler cancels the
    // run context on SIGTERM/SIGINT.
    let ctx = rusternetes_common::signal::setup_signal_context();
    tokio::select! {
        r = scheduler.run() => r?,
        _ = ctx.cancelled() => info!("Requested to terminate, exiting"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn parse(argv: &[&str]) -> Result<Args, clap::Error> {
        let mut v = vec!["kube-scheduler"];
        v.extend_from_slice(argv);
        Args::try_parse_from(v)
    }

    // component-base/config/options/leaderelectionconfig.go:26-57 flag names.
    #[test]
    fn upstream_leader_elect_flags_are_accepted() {
        let a = parse(&[
            "--leader-elect=true",
            "--leader-elect-lease-duration=30s",
            "--leader-elect-renew-deadline=20s",
            "--leader-elect-retry-period=5s",
            "--leader-elect-resource-lock=leases",
            "--leader-elect-resource-name=my-sched",
            "--leader-elect-resource-namespace=my-ns",
        ])
        .expect("upstream flags must parse");
        let s = a.leader_election_settings().unwrap();
        assert_eq!(
            s,
            LeaderElectionSettings {
                enabled: true,
                lease_duration: Duration::from_secs(30),
                renew_deadline: Duration::from_secs(20),
                retry_period: Duration::from_secs(5),
            }
        );
        assert_eq!(a.leader_election_resource_name, "my-sched");
        assert_eq!(a.leader_election_resource_namespace, "my-ns");
    }

    // pflag BoolVar: bare `--leader-elect` means true; `=false` means false.
    #[test]
    fn leader_elect_bare_and_false() {
        let a = parse(&["--leader-elect"]).unwrap();
        assert!(a.leader_election_settings().unwrap().enabled);
        let a = parse(&["--leader-elect=false"]).unwrap();
        assert!(!a.leader_election_settings().unwrap().enabled);
    }

    #[test]
    fn native_flags_still_work_and_defaults_unchanged() {
        let a = parse(&["--enable-leader-election"]).unwrap();
        let s = a.leader_election_settings().unwrap();
        assert!(s.enabled);
        assert_eq!(s.lease_duration, Duration::from_secs(15));
        assert_eq!(s.renew_deadline, Duration::from_secs(10));
        assert_eq!(s.retry_period, Duration::from_secs(2));
    }

    // Go time.ParseDuration errors surface; only "leases" is a supported lock.
    #[test]
    fn invalid_duration_and_lock_rejected() {
        let a = parse(&["--leader-elect-lease-duration=abc"]).unwrap();
        assert!(a.leader_election_settings().is_err());
        let a = parse(&["--leader-elect-resource-lock=configmaps"]).unwrap();
        assert!(a.leader_election_settings().is_err());
    }

    // cmd/kube-scheduler/app/options: DebuggingOptions flags.
    #[test]
    fn profiling_flags_are_accepted() {
        parse(&["--profiling=false", "--contention-profiling=true"])
            .expect("--profiling/--contention-profiling must parse");
        parse(&["--profiling"]).expect("bare --profiling is a pflag bool");
    }
}
