use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// KubeletConfiguration contains the configuration for the Kubelet
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeletConfiguration {
    /// API version of the configuration
    #[serde(default = "default_api_version")]
    pub api_version: String,

    /// Kind of the configuration
    #[serde(default = "default_kind")]
    pub kind: String,

    /// Root directory for managing kubelet files
    /// (volume data, plugin state, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_dir: Option<String>,

    /// Directory path for managing volume data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_dir: Option<String>,

    /// Directory where volume plugins are installed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_plugin_dir: Option<String>,

    /// How frequently to sync pod state. A `metav1.Duration`: a Go duration
    /// string such as `"1m0s"` or `"500ms"`; zero means "unset" (upstream
    /// `SetDefaults_KubeletConfiguration`,
    /// `pkg/kubelet/apis/config/v1beta1/defaults.go`).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub sync_frequency: Option<std::time::Duration>,

    /// `fileCheckFrequency` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub file_check_frequency: Option<std::time::Duration>,

    /// `httpCheckFrequency` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub http_check_frequency: Option<std::time::Duration>,

    /// `streamingConnectionIdleTimeout` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub streaming_connection_idle_timeout: Option<std::time::Duration>,

    /// `nodeStatusUpdateFrequency` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub node_status_update_frequency: Option<std::time::Duration>,

    /// `nodeStatusReportFrequency` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub node_status_report_frequency: Option<std::time::Duration>,

    /// `imageMinimumGCAge` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    #[serde(rename = "imageMinimumGCAge")]
    pub image_minimum_gc_age: Option<std::time::Duration>,

    /// `volumeStatsAggPeriod` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub volume_stats_agg_period: Option<std::time::Duration>,

    /// `cpuManagerReconcilePeriod` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub cpu_manager_reconcile_period: Option<std::time::Duration>,

    /// `runtimeRequestTimeout` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub runtime_request_timeout: Option<std::time::Duration>,

    /// `evictionPressureTransitionPeriod` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub eviction_pressure_transition_period: Option<std::time::Duration>,

    /// `containerLogMonitorInterval` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub container_log_monitor_interval: Option<std::time::Duration>,

    /// `shutdownGracePeriod` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub shutdown_grace_period: Option<std::time::Duration>,

    /// `shutdownGracePeriodCriticalPods` (`metav1.Duration`, Go duration string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub shutdown_grace_period_critical_pods: Option<std::time::Duration>,

    /// `cpuCFSQuotaPeriod` (`*metav1.Duration`, upstream default 100ms).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    #[serde(rename = "cpuCFSQuotaPeriod")]
    pub cpu_cfs_quota_period: Option<std::time::Duration>,

    /// `authentication` (`KubeletAuthentication`): only the webhook cache TTL is
    /// modelled (`pkg/kubelet/apis/config/types.go` `KubeletWebhookAuthentication.CacheTTL`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<KubeletAuthentication>,

    /// `authorization` (`KubeletAuthorization`): only the webhook cache TTLs are modelled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<KubeletAuthorization>,

    /// `crashLoopBackOff` (`CrashLoopBackOffConfig`, types.go:815-823).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "crashLoopBackOff"
    )]
    pub crash_loop_backoff: Option<CrashLoopBackOffConfig>,

    /// Port for the metrics server
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics_bind_port: Option<u16>,

    /// Log verbosity level
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_level: Option<String>,

    /// Cluster service CIDR (e.g., "10.96.0.0/12")
    /// The first IP in this range is used for the kubernetes service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_service_cidr: Option<String>,
}

/// A zero `metav1.Duration` means "unset" and takes the upstream default.
#[allow(dead_code)] // consumers land with the fields they configure (#2284)
fn nonzero_or(v: Option<std::time::Duration>, default_secs: u64) -> std::time::Duration {
    v.filter(|d| !d.is_zero())
        .unwrap_or(std::time::Duration::from_secs(default_secs))
}

/// `KubeletAuthentication` (types.go:579); only `webhook.cacheTTL` is modelled.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeletAuthentication {
    #[serde(default)]
    pub webhook: KubeletWebhookAuthentication,
}

/// `KubeletWebhookAuthentication`: `cacheTTL` defaults to 2m (v1beta1/defaults.go:98-100).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeletWebhookAuthentication {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    #[serde(rename = "cacheTTL")]
    pub cache_ttl: Option<std::time::Duration>,
}

/// `KubeletAuthorization`; only `webhook.cache{Authorized,Unauthorized}TTL` are modelled.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeletAuthorization {
    #[serde(default)]
    pub webhook: KubeletWebhookAuthorization,
}

/// `KubeletWebhookAuthorization`: `cacheAuthorizedTTL` 5m, `cacheUnauthorizedTTL` 30s
/// (v1beta1/defaults.go:104-109).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeletWebhookAuthorization {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    #[serde(rename = "cacheAuthorizedTTL")]
    pub cache_authorized_ttl: Option<std::time::Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    #[serde(rename = "cacheUnauthorizedTTL")]
    pub cache_unauthorized_ttl: Option<std::time::Duration>,
}

/// `CrashLoopBackOffConfig` (types.go:815-823).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrashLoopBackOffConfig {
    /// Unset means the 300s default; an explicit value must be in [1s, 300s]
    /// (validation.go:223-225).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "rusternetes_common::go_duration::option_serde"
    )]
    pub max_container_restart_period: Option<std::time::Duration>,
}

/// `MaxContainerBackOff` (v1beta1/defaults.go:46).
pub const MAX_CONTAINER_BACKOFF: std::time::Duration = std::time::Duration::from_secs(300);

fn default_api_version() -> String {
    "kubelet.config.k8s.io/v1beta1".to_string()
}

fn default_kind() -> String {
    "KubeletConfiguration".to_string()
}

impl Default for KubeletConfiguration {
    fn default() -> Self {
        Self {
            api_version: default_api_version(),
            kind: default_kind(),
            root_dir: None,
            volume_dir: None,
            volume_plugin_dir: None,
            sync_frequency: None,
            file_check_frequency: None,
            http_check_frequency: None,
            streaming_connection_idle_timeout: None,
            node_status_update_frequency: None,
            node_status_report_frequency: None,
            image_minimum_gc_age: None,
            volume_stats_agg_period: None,
            cpu_manager_reconcile_period: None,
            runtime_request_timeout: None,
            eviction_pressure_transition_period: None,
            container_log_monitor_interval: None,
            shutdown_grace_period: None,
            shutdown_grace_period_critical_pods: None,
            cpu_cfs_quota_period: None,
            authentication: None,
            authorization: None,
            crash_loop_backoff: None,
            metrics_bind_port: None,
            log_level: None,
            cluster_service_cidr: None,
        }
    }
}

#[allow(dead_code)] // upstream-defaulted accessors; no runtime consumer yet (#2284)
impl KubeletConfiguration {
    /// `syncFrequency` with the upstream default applied (1m).
    ///
    /// Ported from `SetDefaults_KubeletConfiguration`
    /// (`pkg/kubelet/apis/config/v1beta1/defaults.go`). NOTE: the kubelet's
    /// *runtime* sync-loop tick (`RuntimeConfig::sync_frequency`) deliberately
    /// stays at 10s: it drives pod reconciliation latency here, and moving it
    /// to 1m would slow every pod status convergence. This accessor reports
    /// the upstream-defaulted value for config consumers.
    pub fn effective_sync_frequency(&self) -> std::time::Duration {
        nonzero_or(self.sync_frequency, 60)
    }

    /// `fileCheckFrequency` with the upstream default applied (20s).
    pub fn effective_file_check_frequency(&self) -> std::time::Duration {
        nonzero_or(self.file_check_frequency, 20)
    }

    /// `httpCheckFrequency` with the upstream default applied (20s).
    pub fn effective_http_check_frequency(&self) -> std::time::Duration {
        nonzero_or(self.http_check_frequency, 20)
    }

    /// `streamingConnectionIdleTimeout` with the upstream default applied (14400s).
    pub fn effective_streaming_connection_idle_timeout(&self) -> std::time::Duration {
        nonzero_or(self.streaming_connection_idle_timeout, 14400)
    }

    /// `nodeStatusUpdateFrequency` with the upstream default applied (10s).
    pub fn effective_node_status_update_frequency(&self) -> std::time::Duration {
        nonzero_or(self.node_status_update_frequency, 10)
    }

    /// `imageMinimumGCAge` with the upstream default applied (120s).
    pub fn effective_image_minimum_gc_age(&self) -> std::time::Duration {
        nonzero_or(self.image_minimum_gc_age, 120)
    }

    /// `volumeStatsAggPeriod` with the upstream default applied (60s).
    pub fn effective_volume_stats_agg_period(&self) -> std::time::Duration {
        nonzero_or(self.volume_stats_agg_period, 60)
    }

    /// `cpuManagerReconcilePeriod` with the upstream default applied (10s).
    pub fn effective_cpu_manager_reconcile_period(&self) -> std::time::Duration {
        nonzero_or(self.cpu_manager_reconcile_period, 10)
    }

    /// `runtimeRequestTimeout` with the upstream default applied (120s).
    pub fn effective_runtime_request_timeout(&self) -> std::time::Duration {
        nonzero_or(self.runtime_request_timeout, 120)
    }

    /// `evictionPressureTransitionPeriod` with the upstream default applied (300s).
    pub fn effective_eviction_pressure_transition_period(&self) -> std::time::Duration {
        nonzero_or(self.eviction_pressure_transition_period, 300)
    }

    /// `containerLogMonitorInterval` with the upstream default applied (10s).
    pub fn effective_container_log_monitor_interval(&self) -> std::time::Duration {
        nonzero_or(self.container_log_monitor_interval, 10)
    }

    /// `nodeStatusReportFrequency` with the upstream default applied: 5m, or
    /// the (explicit) `nodeStatusUpdateFrequency` for backward compatibility
    /// (`defaults.go`, `NodeStatusReportFrequency` block).
    pub fn effective_node_status_report_frequency(&self) -> std::time::Duration {
        match self.node_status_report_frequency.filter(|d| !d.is_zero()) {
            Some(d) => d,
            None => match self.node_status_update_frequency.filter(|d| !d.is_zero()) {
                Some(u) => u,
                None => std::time::Duration::from_secs(300),
            },
        }
    }

    /// `authentication.webhook.cacheTTL`, default 2m (defaults.go:98-100).
    pub fn effective_authentication_webhook_cache_ttl(&self) -> std::time::Duration {
        nonzero_or(
            self.authentication
                .as_ref()
                .and_then(|a| a.webhook.cache_ttl),
            120,
        )
    }

    /// `authorization.webhook.cacheAuthorizedTTL`, default 5m (defaults.go:104-106).
    pub fn effective_authorization_webhook_cache_authorized_ttl(&self) -> std::time::Duration {
        nonzero_or(
            self.authorization
                .as_ref()
                .and_then(|a| a.webhook.cache_authorized_ttl),
            300,
        )
    }

    /// `authorization.webhook.cacheUnauthorizedTTL`, default 30s (defaults.go:107-109).
    pub fn effective_authorization_webhook_cache_unauthorized_ttl(&self) -> std::time::Duration {
        nonzero_or(
            self.authorization
                .as_ref()
                .and_then(|a| a.webhook.cache_unauthorized_ttl),
            30,
        )
    }

    /// `crashLoopBackOff.maxContainerRestartPeriod`; a *nil* pointer takes
    /// `MaxContainerBackOff` (defaults.go:311-314). Unlike the other durations an
    /// explicit zero is kept (and rejected by `validate`).
    pub fn effective_max_container_restart_period(&self) -> std::time::Duration {
        self.crash_loop_backoff
            .as_ref()
            .and_then(|c| c.max_container_restart_period)
            .unwrap_or(MAX_CONTAINER_BACKOFF)
    }

    /// Gate-aware defaulting of `crashLoopBackOff.maxContainerRestartPeriod`
    /// (v1beta1/defaults.go:311-315): only when `KubeletCrashLoopBackOffMax` is
    /// enabled is a nil pointer defaulted to `MaxContainerBackOff`; with the
    /// gate off the field stays unset (and `validate` rejects a set value).
    pub fn default_max_container_restart_period(
        configured: Option<std::time::Duration>,
    ) -> Option<std::time::Duration> {
        if rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::KubeletCrashLoopBackOffMax,
        ) {
            Some(configured.unwrap_or(MAX_CONTAINER_BACKOFF))
        } else {
            configured
        }
    }

    /// The defaulted value fed to `newCrashLoopBackOff` (kubelet.go:353-356).
    pub fn effective_max_container_restart_period_gated(&self) -> Option<std::time::Duration> {
        Self::default_max_container_restart_period(
            self.crash_loop_backoff
                .as_ref()
                .and_then(|c| c.max_container_restart_period),
        )
    }

    /// `cpuCFSQuotaPeriod` with the upstream default applied (100ms).
    pub fn effective_cpu_cfs_quota_period(&self) -> std::time::Duration {
        self.cpu_cfs_quota_period
            .filter(|d| !d.is_zero())
            .unwrap_or(std::time::Duration::from_millis(100))
    }

    /// `syncFrequency` in whole seconds, rounded up; `None` when unset or zero
    /// (zero is defaulted upstream, so it must not override the CLI/default).
    pub fn sync_frequency_secs(&self) -> Option<u64> {
        self.sync_frequency
            .filter(|d| !d.is_zero())
            .map(|d| d.as_secs() + u64::from(d.subsec_nanos() > 0))
    }

    /// Load configuration from a YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        let config: KubeletConfiguration = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {:?}", path.as_ref()))?;

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Validate API version
        if self.api_version != "kubelet.config.k8s.io/v1beta1" {
            anyhow::bail!(
                "Unsupported apiVersion: {}. Expected: kubelet.config.k8s.io/v1beta1",
                self.api_version
            );
        }

        // Validate kind
        if self.kind != "KubeletConfiguration" {
            anyhow::bail!(
                "Invalid kind: {}. Expected: KubeletConfiguration",
                self.kind
            );
        }

        // Validate paths exist if specified
        if let Some(root_dir) = &self.root_dir {
            let path = PathBuf::from(root_dir);
            if !path.exists() {
                tracing::warn!(
                    "Root directory does not exist and will be created: {}",
                    root_dir
                );
            }
        }

        if let Some(volume_dir) = &self.volume_dir {
            let path = PathBuf::from(volume_dir);
            if !path.exists() {
                tracing::warn!(
                    "Volume directory does not exist and will be created: {}",
                    volume_dir
                );
            }
        }

        // Validate sync frequency
        if let Some(sync_freq) = self.sync_frequency_secs() {
            if sync_freq > 3600 {
                tracing::warn!(
                    "syncFrequency of {} seconds is unusually high (> 1 hour)",
                    sync_freq
                );
            }
        }

        // Validate metrics port
        if let Some(port) = self.metrics_bind_port {
            if port < 1024 {
                tracing::warn!("metricsBindPort {} is a privileged port (< 1024)", port);
            }
        }

        // Validate log level
        if let Some(level) = &self.log_level {
            match level.to_lowercase().as_str() {
                "trace" | "debug" | "info" | "warn" | "error" => {}
                _ => anyhow::bail!(
                    "Invalid logLevel: {}. Must be one of: trace, debug, info, warn, error",
                    level
                ),
            }
        }

        // validation.go:219-228. KubeletCrashLoopBackOffMax is Beta/default-on in
        // 1.35 (kube_features.go:1429-1432); the nil case is the defaulted 300s
        // (defaults.go:311-315) so only a set value is range-checked.
        let period = self
            .crash_loop_backoff
            .as_ref()
            .and_then(|c| c.max_container_restart_period);
        if rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::KubeletCrashLoopBackOffMax,
        ) {
            if let Some(d) = period {
                let ms = d.as_millis();
                if !(1000..=300_000).contains(&ms) {
                    anyhow::bail!(
                        "invalid configuration: CrashLoopBackOff.MaxContainerRestartPeriod (got: {} seconds) must be set between 1s and 300s",
                        d.as_secs_f64()
                    );
                }
            }
        } else if period.is_some() {
            anyhow::bail!(
                "invalid configuration: FeatureGate KubeletCrashLoopBackOffMax not enabled, CrashLoopBackOff.MaxContainerRestartPeriod must not be set"
            );
        }

        Ok(())
    }

    /// Save configuration to a YAML file
    #[allow(dead_code)]
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let contents = serde_yaml::to_string(self).context("Failed to serialize configuration")?;

        std::fs::write(path.as_ref(), contents)
            .with_context(|| format!("Failed to write config file: {:?}", path.as_ref()))?;

        Ok(())
    }
}

/// RuntimeConfig holds the resolved runtime configuration for the kubelet
/// after merging CLI flags, config file, environment variables, and defaults
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Root directory for kubelet files
    pub root_dir: PathBuf,

    /// Directory for volume data
    pub volume_dir: PathBuf,

    /// Directory for volume plugins
    pub volume_plugin_dir: PathBuf,

    /// Sync frequency in seconds
    pub sync_frequency: u64,

    /// Metrics server port
    pub metrics_bind_port: u16,

    /// Log level
    pub log_level: String,

    /// Node name
    pub node_name: String,

    /// Etcd endpoints
    pub etcd_endpoints: Vec<String>,

    /// Kubernetes service ClusterIP (first IP in service CIDR)
    pub kubernetes_service_host: String,
}

/// Extract the first usable IP address from a CIDR range
/// For example, "10.96.0.0/12" -> "10.96.0.1"
fn first_ip_from_cidr(cidr: &str) -> Result<String> {
    use std::net::IpAddr;

    let parts: Vec<&str> = cidr.split('/').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid CIDR format: {}", cidr);
    }

    let base_ip: IpAddr = parts[0]
        .parse()
        .with_context(|| format!("Invalid IP address in CIDR: {}", parts[0]))?;

    match base_ip {
        IpAddr::V4(ipv4) => {
            // Get the IP as u32, add 1, convert back
            let ip_u32 = u32::from(ipv4);
            let first_ip_u32 = ip_u32 + 1;
            let first_ip = std::net::Ipv4Addr::from(first_ip_u32);
            Ok(first_ip.to_string())
        }
        IpAddr::V6(ipv6) => {
            // For IPv6, convert to u128, add 1, convert back
            let ip_u128 = u128::from(ipv6);
            let first_ip_u128 = ip_u128 + 1;
            let first_ip = std::net::Ipv6Addr::from(first_ip_u128);
            Ok(first_ip.to_string())
        }
    }
}

impl RuntimeConfig {
    /// Build RuntimeConfig from multiple sources with proper precedence:
    /// CLI flags > Config file > Environment variables > Defaults
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        cli_root_dir: Option<String>,
        cli_volume_dir: Option<String>,
        cli_volume_plugin_dir: Option<String>,
        cli_sync_frequency: Option<u64>,
        cli_metrics_port: Option<u16>,
        cli_log_level: Option<String>,
        config_file: Option<KubeletConfiguration>,
        node_name: String,
        etcd_endpoints: Vec<String>,
    ) -> Result<Self> {
        // Determine root directory
        // Precedence: CLI > Config > Env > Default
        let root_dir = cli_root_dir
            .or_else(|| config_file.as_ref().and_then(|c| c.root_dir.clone()))
            .or_else(|| std::env::var("KUBELET_ROOT_DIR").ok())
            .unwrap_or_else(|| {
                // For development: use current dir, for production: /var/lib/kubelet
                std::env::current_dir()
                    .ok()
                    .and_then(|p| p.to_str().map(String::from))
                    .unwrap_or_else(|| "/var/lib/kubelet".to_string())
            });

        // Determine volume directory
        // Precedence: CLI > Config > Env > Root-dir-based default
        let volume_dir = cli_volume_dir
            .or_else(|| config_file.as_ref().and_then(|c| c.volume_dir.clone()))
            .or_else(|| std::env::var("KUBELET_VOLUMES_PATH").ok())
            .unwrap_or_else(|| format!("{}/volumes", root_dir));

        // Determine volume plugin directory
        let volume_plugin_dir = cli_volume_plugin_dir
            .or_else(|| {
                config_file
                    .as_ref()
                    .and_then(|c| c.volume_plugin_dir.clone())
            })
            .or_else(|| std::env::var("KUBELET_VOLUME_PLUGIN_DIR").ok())
            .unwrap_or_else(|| "/usr/libexec/kubernetes/kubelet-plugins/volume/exec".to_string());

        // Determine sync frequency
        let sync_frequency = cli_sync_frequency
            .or_else(|| config_file.as_ref().and_then(|c| c.sync_frequency_secs()))
            .unwrap_or(10);

        // Determine metrics port
        let metrics_bind_port = cli_metrics_port
            .or_else(|| config_file.as_ref().and_then(|c| c.metrics_bind_port))
            .unwrap_or(8082);

        // Determine log level
        let log_level = cli_log_level
            .or_else(|| config_file.as_ref().and_then(|c| c.log_level.clone()))
            .or_else(|| std::env::var("RUST_LOG").ok())
            .unwrap_or_else(|| "info".to_string());

        // Determine cluster service CIDR and extract kubernetes service host IP
        // Precedence: Config > Env > Default (10.96.0.0/12)
        let cluster_service_cidr = config_file
            .as_ref()
            .and_then(|c| c.cluster_service_cidr.clone())
            .or_else(|| std::env::var("CLUSTER_SERVICE_CIDR").ok())
            .unwrap_or_else(|| "10.96.0.0/12".to_string());

        // Use KUBERNETES_SERVICE_HOST_OVERRIDE if set — this allows the API server
        // address to be configured for environments where ClusterIP routing doesn't
        // work from pod containers (e.g., Podman Machine without br_netfilter).
        // Falls back to the kubernetes service ClusterIP (10.96.0.1).
        let kubernetes_service_host = std::env::var("KUBERNETES_SERVICE_HOST_OVERRIDE")
            .unwrap_or_else(|_| {
                first_ip_from_cidr(&cluster_service_cidr)
                    .unwrap_or_else(|_| "10.96.0.1".to_string())
            });

        let config = Self {
            root_dir: PathBuf::from(root_dir),
            volume_dir: PathBuf::from(volume_dir),
            volume_plugin_dir: PathBuf::from(volume_plugin_dir),
            sync_frequency,
            metrics_bind_port,
            log_level,
            node_name,
            etcd_endpoints,
            kubernetes_service_host,
        };

        config.validate()?;
        config.create_directories()?;

        Ok(config)
    }

    /// Validate the runtime configuration
    fn validate(&self) -> Result<()> {
        if self.node_name.is_empty() {
            anyhow::bail!("Node name cannot be empty");
        }

        if self.etcd_endpoints.is_empty() {
            anyhow::bail!("At least one etcd endpoint must be specified");
        }

        if self.sync_frequency == 0 {
            anyhow::bail!("Sync frequency must be greater than 0");
        }

        Ok(())
    }

    /// Create necessary directories
    fn create_directories(&self) -> Result<()> {
        // Create root directory
        std::fs::create_dir_all(&self.root_dir)
            .with_context(|| format!("Failed to create root directory: {:?}", self.root_dir))?;

        // Create volume directory
        std::fs::create_dir_all(&self.volume_dir)
            .with_context(|| format!("Failed to create volume directory: {:?}", self.volume_dir))?;

        // Create volume plugin directory
        if let Err(e) = std::fs::create_dir_all(&self.volume_plugin_dir) {
            tracing::warn!(
                "Failed to create volume plugin directory {:?}: {}. This is non-fatal for basic operation.",
                self.volume_plugin_dir,
                e
            );
        }

        Ok(())
    }

    /// Display the configuration (for logging/debugging)
    pub fn display(&self) -> String {
        format!(
            r#"Kubelet Runtime Configuration:
  Node Name: {}
  Root Directory: {}
  Volume Directory: {}
  Volume Plugin Directory: {}
  Sync Frequency: {}s
  Metrics Port: {}
  Log Level: {}
  Etcd Endpoints: {}"#,
            self.node_name,
            self.root_dir.display(),
            self.volume_dir.display(),
            self.volume_plugin_dir.display(),
            self.sync_frequency,
            self.metrics_bind_port,
            self.log_level,
            self.etcd_endpoints.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// A standard upstream KubeletConfiguration (as k0s emits it) carries
    /// metav1.Duration strings. Issue #1579.
    #[test]
    fn test_from_file_accepts_go_duration_strings() {
        let yaml = "apiVersion: kubelet.config.k8s.io/v1beta1\n\
                    kind: KubeletConfiguration\n\
                    syncFrequency: 1m30s\n\
                    fileCheckFrequency: 0s\n\
                    cpuManagerReconcilePeriod: 0s\n";
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(yaml.as_bytes()).unwrap();
        let loaded = KubeletConfiguration::from_file(file.path()).unwrap();
        assert_eq!(
            loaded.sync_frequency,
            Some(std::time::Duration::from_secs(90))
        );
    }

    #[test]
    fn test_webhook_ttls_and_crashloop_max_defaults_and_parse() {
        let s = std::time::Duration::from_secs;
        let d = KubeletConfiguration::default();
        assert_eq!(d.effective_authentication_webhook_cache_ttl(), s(120));
        assert_eq!(
            d.effective_authorization_webhook_cache_authorized_ttl(),
            s(300)
        );
        assert_eq!(
            d.effective_authorization_webhook_cache_unauthorized_ttl(),
            s(30)
        );
        assert_eq!(d.effective_max_container_restart_period(), s(300));

        let yaml = "authentication:\n  webhook:\n    cacheTTL: 10s\nauthorization:\n  webhook:\n    cacheAuthorizedTTL: 1m\n    cacheUnauthorizedTTL: 5s\ncrashLoopBackOff:\n  maxContainerRestartPeriod: 45s\n";
        let c: KubeletConfiguration = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(c.effective_authentication_webhook_cache_ttl(), s(10));
        assert_eq!(
            c.effective_authorization_webhook_cache_authorized_ttl(),
            s(60)
        );
        assert_eq!(
            c.effective_authorization_webhook_cache_unauthorized_ttl(),
            s(5)
        );
        assert_eq!(c.effective_max_container_restart_period(), s(45));
    }

    /// validation.go:226-228 + defaults.go:311-315 with the gate off.
    #[test]
    #[serial_test::serial]
    fn test_crashloop_max_gate_off() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let _g = with_feature(Feature::KubeletCrashLoopBackOffMax, false);
        let set: KubeletConfiguration =
            serde_yaml::from_str("crashLoopBackOff:\n  maxContainerRestartPeriod: 45s\n").unwrap();
        assert_eq!(
            set.validate().unwrap_err().to_string(),
            "invalid configuration: FeatureGate KubeletCrashLoopBackOffMax not enabled, CrashLoopBackOff.MaxContainerRestartPeriod must not be set"
        );
        let unset = KubeletConfiguration::default();
        unset.validate().unwrap();
        assert_eq!(unset.effective_max_container_restart_period_gated(), None);
    }

    /// validation_test.go:397-418 (too low / too high) and the 1s/300s bounds.
    #[test]
    fn test_crashloop_max_validation_range() {
        let mk = |y: &str| serde_yaml::from_str::<KubeletConfiguration>(y).unwrap();
        let low = mk("crashLoopBackOff:\n  maxContainerRestartPeriod: 0s\n");
        assert_eq!(
            low.validate().unwrap_err().to_string(),
            "invalid configuration: CrashLoopBackOff.MaxContainerRestartPeriod (got: 0 seconds) must be set between 1s and 300s"
        );
        let high = mk("crashLoopBackOff:\n  maxContainerRestartPeriod: 301s\n");
        assert_eq!(
            high.validate().unwrap_err().to_string(),
            "invalid configuration: CrashLoopBackOff.MaxContainerRestartPeriod (got: 301 seconds) must be set between 1s and 300s"
        );
        mk("crashLoopBackOff:\n  maxContainerRestartPeriod: 1s\n")
            .validate()
            .unwrap();
        mk("crashLoopBackOff:\n  maxContainerRestartPeriod: 300s\n")
            .validate()
            .unwrap();
    }

    #[test]
    fn test_upstream_duration_fields_and_defaults() {
        // Defaults per SetDefaults_KubeletConfiguration (defaults.go).
        let d = KubeletConfiguration::default();
        let s = std::time::Duration::from_secs;
        assert_eq!(d.effective_sync_frequency(), s(60));
        assert_eq!(d.effective_file_check_frequency(), s(20));
        assert_eq!(d.effective_http_check_frequency(), s(20));
        assert_eq!(d.effective_streaming_connection_idle_timeout(), s(4 * 3600));
        assert_eq!(d.effective_node_status_update_frequency(), s(10));
        assert_eq!(d.effective_node_status_report_frequency(), s(300));
        assert_eq!(d.effective_image_minimum_gc_age(), s(120));
        assert_eq!(d.effective_volume_stats_agg_period(), s(60));
        assert_eq!(d.effective_cpu_manager_reconcile_period(), s(10));
        assert_eq!(d.effective_runtime_request_timeout(), s(120));
        assert_eq!(d.effective_eviction_pressure_transition_period(), s(300));
        assert_eq!(d.effective_container_log_monitor_interval(), s(10));
        assert_eq!(
            d.effective_cpu_cfs_quota_period(),
            std::time::Duration::from_millis(100)
        );

        let yaml = "fileCheckFrequency: 5s\nhttpCheckFrequency: 1m\n\
                    nodeStatusUpdateFrequency: 4s\nstreamingConnectionIdleTimeout: 30m\n\
                    imageMinimumGCAge: 0s\ncpuCFSQuotaPeriod: 50ms\n\
                    shutdownGracePeriod: 30s\nshutdownGracePeriodCriticalPods: 10s\n";
        let c: KubeletConfiguration = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(c.effective_file_check_frequency(), s(5));
        assert_eq!(c.effective_http_check_frequency(), s(60));
        assert_eq!(c.effective_streaming_connection_idle_timeout(), s(1800));
        assert_eq!(c.effective_image_minimum_gc_age(), s(120));
        assert_eq!(c.effective_node_status_report_frequency(), s(4));
        assert_eq!(
            c.effective_cpu_cfs_quota_period(),
            std::time::Duration::from_millis(50)
        );
        assert_eq!(c.shutdown_grace_period, Some(s(30)));
        assert_eq!(c.shutdown_grace_period_critical_pods, Some(s(10)));
        assert!(serde_yaml::from_str::<KubeletConfiguration>("fileCheckFrequency: 5\n").is_err());
    }

    #[test]
    fn test_runtime_sync_frequency_default_stays_10s() {
        let rc = RuntimeConfig::build(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "n".into(),
            vec!["http://localhost:2379".to_string()],
        )
        .unwrap();
        assert_eq!(rc.sync_frequency, 10);
    }

    #[test]
    fn test_sync_frequency_zero_means_unset_and_subsecond_rounds_up() {
        // Upstream SetDefaults_KubeletConfiguration: zero => default.
        let yaml = "syncFrequency: 0s\n";
        let cfg: KubeletConfiguration = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.sync_frequency_secs(), None);
        let cfg: KubeletConfiguration = serde_yaml::from_str("syncFrequency: 500ms\n").unwrap();
        assert_eq!(cfg.sync_frequency_secs(), Some(1));
    }

    #[test]
    fn test_sync_frequency_numeric_rejected_and_serialized_as_go_string() {
        // metav1.Duration.UnmarshalJSON only accepts strings.
        assert!(serde_yaml::from_str::<KubeletConfiguration>("syncFrequency: 15\n").is_err());
        let cfg = KubeletConfiguration {
            sync_frequency: Some(std::time::Duration::from_secs(90)),
            ..Default::default()
        };
        assert!(serde_yaml::to_string(&cfg)
            .unwrap()
            .contains("syncFrequency: 1m30s"));
    }

    #[test]
    fn test_default_config() {
        let config = KubeletConfiguration::default();
        assert_eq!(config.api_version, "kubelet.config.k8s.io/v1beta1");
        assert_eq!(config.kind, "KubeletConfiguration");
    }

    #[test]
    fn test_config_validation() {
        let mut config = KubeletConfiguration::default();
        assert!(config.validate().is_ok());

        // Invalid API version
        config.api_version = "v1".to_string();
        assert!(config.validate().is_err());

        // Reset
        config.api_version = "kubelet.config.k8s.io/v1beta1".to_string();

        // Invalid kind
        config.kind = "Pod".to_string();
        assert!(config.validate().is_err());

        // Reset
        config.kind = "KubeletConfiguration".to_string();

        // Invalid sync frequency
        config.sync_frequency = Some(std::time::Duration::ZERO);
        assert!(config.validate().is_ok());

        // Reset
        config.sync_frequency = Some(std::time::Duration::from_secs(10));

        // Invalid log level
        config.log_level = Some("invalid".to_string());
        assert!(config.validate().is_err());

        // Valid log level
        config.log_level = Some("debug".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_file_roundtrip() {
        let config = KubeletConfiguration {
            api_version: "kubelet.config.k8s.io/v1beta1".to_string(),
            kind: "KubeletConfiguration".to_string(),
            root_dir: Some("/var/lib/kubelet".to_string()),
            volume_dir: Some("/var/lib/kubelet/volumes".to_string()),
            volume_plugin_dir: Some(
                "/usr/libexec/kubernetes/kubelet-plugins/volume/exec".to_string(),
            ),
            sync_frequency: Some(std::time::Duration::from_secs(15)),
            metrics_bind_port: Some(10250),
            log_level: Some("info".to_string()),
            cluster_service_cidr: Some("10.96.0.0/12".to_string()),
            ..Default::default()
        };

        // Write to temp file
        let mut file = NamedTempFile::new().unwrap();
        let yaml = serde_yaml::to_string(&config).unwrap();
        file.write_all(yaml.as_bytes()).unwrap();

        // Read back
        let loaded = KubeletConfiguration::from_file(file.path()).unwrap();

        assert_eq!(loaded.api_version, config.api_version);
        assert_eq!(loaded.kind, config.kind);
        assert_eq!(loaded.root_dir, config.root_dir);
        assert_eq!(loaded.volume_dir, config.volume_dir);
        assert_eq!(loaded.sync_frequency, config.sync_frequency);
        assert_eq!(loaded.metrics_bind_port, config.metrics_bind_port);
        assert_eq!(loaded.log_level, config.log_level);
    }

    #[test]
    fn test_runtime_config_precedence() {
        use tempfile::tempdir;

        // Create temp directories for testing
        let tmp_dir = tempdir().unwrap();
        let cli_root = tmp_dir.path().join("cli/root");
        let cli_volumes = tmp_dir.path().join("cli/volumes");
        let config_root = tmp_dir.path().join("config/root");
        let config_volumes = tmp_dir.path().join("config/volumes");

        // CLI values should take precedence
        let runtime = RuntimeConfig::build(
            Some(cli_root.to_str().unwrap().to_string()),
            Some(cli_volumes.to_str().unwrap().to_string()),
            None,
            Some(20),
            Some(9090),
            Some("debug".to_string()),
            Some(KubeletConfiguration {
                root_dir: Some(config_root.to_str().unwrap().to_string()),
                volume_dir: Some(config_volumes.to_str().unwrap().to_string()),
                sync_frequency: Some(std::time::Duration::from_secs(30)),
                ..Default::default()
            }),
            "test-node".to_string(),
            vec!["http://localhost:2379".to_string()],
        )
        .unwrap();

        assert_eq!(runtime.root_dir, cli_root);
        assert_eq!(runtime.volume_dir, cli_volumes);
        assert_eq!(runtime.sync_frequency, 20);
        assert_eq!(runtime.metrics_bind_port, 9090);
        assert_eq!(runtime.log_level, "debug");
    }

    #[test]
    fn test_runtime_config_defaults() {
        let runtime = RuntimeConfig::build(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "test-node".to_string(),
            vec!["http://localhost:2379".to_string()],
        )
        .unwrap();

        assert_eq!(runtime.sync_frequency, 10);
        assert_eq!(runtime.metrics_bind_port, 8082);
        assert_eq!(runtime.log_level, "info");
        assert!(runtime.volume_dir.ends_with("volumes"));
    }

    #[test]
    fn test_runtime_config_validation() {
        // Empty node name should fail
        let result = RuntimeConfig::build(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "".to_string(),
            vec!["http://localhost:2379".to_string()],
        );
        assert!(result.is_err());

        // Empty etcd endpoints should fail
        let result = RuntimeConfig::build(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "test-node".to_string(),
            vec![],
        );
        assert!(result.is_err());
    }
}
