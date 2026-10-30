//! Unit tests moved from `src/config.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use serde::{Deserialize, Serialize};
    pub use std::path::{Path, PathBuf};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::config::*;
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
    #[serial_test::serial]
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
    /// Reads the process-global KubeletCrashLoopBackOffMax gate, which
    /// `test_crashloop_max_gate_off` (and kubelet.rs's gate matrix) flip: must
    /// be serial or plain `cargo test` (one process) sees the gate off.
    #[test]
    #[serial_test::serial]
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

    /// `ValidateKubeletConfiguration` image GC rules (validation.go:87-103) and
    /// the v1beta1 defaults (defaults.go:156-162).
    #[test]
    fn test_image_gc_config_defaults_and_validation() {
        let d = KubeletConfiguration::default();
        assert_eq!(d.effective_image_gc_high_threshold_percent(), 85);
        assert_eq!(d.effective_image_gc_low_threshold_percent(), 80);
        assert_eq!(
            d.effective_image_maximum_gc_age(),
            std::time::Duration::ZERO
        );
        d.validate().unwrap();

        let mk = |y: &str| serde_yaml::from_str::<KubeletConfiguration>(y).unwrap();
        let c = mk("imageGCHighThresholdPercent: 90\nimageGCLowThresholdPercent: 70\nimageMaximumGCAge: 10m\n");
        assert_eq!(c.effective_image_gc_high_threshold_percent(), 90);
        assert_eq!(c.effective_image_gc_low_threshold_percent(), 70);
        assert_eq!(
            c.effective_image_maximum_gc_age(),
            std::time::Duration::from_secs(600)
        );
        c.validate().unwrap();

        assert_eq!(
            mk("imageGCHighThresholdPercent: 101\n")
                .validate()
                .unwrap_err()
                .to_string(),
            "invalid configuration: imageGCHighThresholdPercent (--image-gc-high-threshold) 101 must be between 0 and 100, inclusive"
        );
        assert_eq!(
            mk("imageGCLowThresholdPercent: -1\n")
                .validate()
                .unwrap_err()
                .to_string(),
            "invalid configuration: imageGCLowThresholdPercent (--image-gc-low-threshold) -1 must be between 0 and 100, inclusive"
        );
        assert_eq!(
            mk("imageGCHighThresholdPercent: 80\n")
                .validate()
                .unwrap_err()
                .to_string(),
            "invalid configuration: imageGCLowThresholdPercent (--image-gc-low-threshold) 80 must be less than imageGCHighThresholdPercent (--image-gc-high-threshold) 80"
        );
        assert_eq!(
            mk("imageMaximumGCAge: 1m\n")
                .validate()
                .unwrap_err()
                .to_string(),
            "invalid configuration: imageMaximumGCAge 1m0s must be greater than imageMinimumGCAge 2m0s"
        );
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
        let u: KubeletConfiguration =
            serde_yaml::from_str("staticPodURL: http://x/p\nstaticPodURLHeader:\n  X-A: [a, b]\n")
                .unwrap();
        assert_eq!(u.static_pod_url.as_deref(), Some("http://x/p"));
        assert_eq!(u.static_pod_url_header.unwrap()["X-A"], ["a", "b"]);
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
