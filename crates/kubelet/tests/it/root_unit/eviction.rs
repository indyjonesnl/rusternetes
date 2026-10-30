//! Unit tests moved from `src/eviction.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use rusternetes_common::qos::QoSClass;
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::{Node, NodeCondition, Pod};
    pub use std::collections::{HashMap, HashSet};
    pub use std::path::{Path, PathBuf};
    pub use std::time::{Duration, Instant};
    pub use tracing::{debug, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::eviction::*;

    /// Coverage inherited from the deleted `parse_memory_value`: every input
    /// the local suffix chain handled correctly still parses identically.
    #[test]
    fn test_parse_absolute_threshold_values() {
        for (value, expected) in [
            ("100", 100u64),
            ("100Ki", 102_400),
            ("100Mi", 104_857_600),
            ("1Gi", 1_073_741_824),
            ("1Ti", 1_099_511_627_776),
        ] {
            assert_eq!(
                parse_threshold_value("memory.available", value).unwrap(),
                Some(EvictionValue::Absolute(expected)),
                "value {value:?}"
            );
        }
    }

    #[test]
    fn test_parse_threshold_value_percentage() {
        assert_eq!(
            parse_threshold_value("memory.available", "10%").unwrap(),
            Some(EvictionValue::Percentage(10.0))
        );
        assert_eq!(
            parse_threshold_value("memory.available", "0.5%").unwrap(),
            Some(EvictionValue::Percentage(0.5))
        );
        // Out-of-range percentages are rejected.
        assert!(parse_threshold_value("memory.available", "101%").is_err());
        assert!(parse_threshold_value("memory.available", "-1%").is_err());
    }

    #[test]
    fn test_parse_eviction_flag_empty_disables() {
        let map = parse_eviction_flag("").unwrap();
        assert!(map.is_empty(), "empty flag must yield empty threshold set");
        let map = parse_eviction_flag("   ").unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn test_parse_eviction_flag_multi() {
        let map =
            parse_eviction_flag("memory.available<1Gi,nodefs.available<5%,nodefs.inodesFree<5%")
                .unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(
            map.get(&EvictionSignal::MemoryAvailable),
            Some(&EvictionValue::Absolute(1024 * 1024 * 1024))
        );
        assert_eq!(
            map.get(&EvictionSignal::NodeFsAvailable),
            Some(&EvictionValue::Percentage(5.0))
        );
        assert_eq!(
            map.get(&EvictionSignal::NodeFsInodesFree),
            Some(&EvictionValue::Percentage(5.0))
        );
    }

    #[test]
    fn test_parse_eviction_flag_invalid_signal() {
        assert!(parse_eviction_flag("bogus.signal<10%").is_err());
    }

    /// Upstream parses the absolute form with `resource.ParseQuantity`
    /// (`helpers.go:410`), which accepts the whole grammar. The local subset
    /// covered only `Ki`/`Mi`/`Gi`/`Ti` and parsed the digits with `u64`, so
    /// `--eviction-hard=memory.available<1G` failed the flag and kubelet
    /// refused to start.
    #[test]
    fn eviction_flag_accepts_full_quantity_grammar() {
        let cases = [
            ("1G", 1_000_000_000u64),
            ("1M", 1_000_000),
            ("1k", 1_000),
            ("1T", 1_000_000_000_000),
            ("0.5Gi", 536_870_912),
            ("1.5Gi", 1_610_612_736),
            ("2.5Mi", 2_621_440),
            ("1Ti", 1_099_511_627_776),
            ("1Pi", 1_125_899_906_842_624),
            ("129e6", 129_000_000),
        ];
        for (value, expected) in cases {
            let map = parse_eviction_flag(&format!("memory.available<{value}"))
                .unwrap_or_else(|e| panic!("{value} rejected: {e}"));
            assert_eq!(
                map.get(&EvictionSignal::MemoryAvailable),
                Some(&EvictionValue::Absolute(expected)),
                "value {value:?}"
            );
        }
    }

    /// `0%` and `100%` are dropped, not rejected — upstream returns
    /// `(nil, nil)` for both (`helpers.go:387-390`). Treating `100%` as a real
    /// threshold is the dangerous half: it is met on every sync, so the node
    /// would evict continuously.
    #[test]
    fn eviction_flag_ignores_zero_and_hundred_percent() {
        for value in ["0%", "100%"] {
            let map = parse_eviction_flag(&format!("memory.available<{value}"))
                .unwrap_or_else(|e| panic!("{value} must be ignored, not rejected: {e}"));
            assert!(map.is_empty(), "{value} must not produce a threshold");
        }
        // A dropped statement must not take its neighbours with it.
        let map = parse_eviction_flag("memory.available<100%,nodefs.available<5%").unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(
            map.get(&EvictionSignal::NodeFsAvailable),
            Some(&EvictionValue::Percentage(5.0))
        );
    }

    /// Upstream rejects a zero or negative quantity outright:
    /// `quantity.Sign() < 0 || quantity.IsZero()` (`helpers.go:414-416`). A
    /// zero threshold used to be accepted and simply never fired.
    #[test]
    fn eviction_flag_rejects_non_positive_absolute() {
        for value in ["0", "0Gi", "0.0", "-1", "-1Gi"] {
            let err = parse_eviction_flag(&format!("memory.available<{value}"))
                .expect_err(&format!("{value} must be rejected"));
            assert!(
                err.chain()
                    .any(|c| c.to_string().contains("must be positive")),
                "{value}: unexpected error {err:#}"
            );
        }
    }

    /// A non-finite percentage passes upstream's `< 0` / `> 1` bounds check
    /// because every comparison against NaN is false, leaving a threshold that
    /// can never be met. Reject it instead.
    #[test]
    fn eviction_flag_rejects_non_finite_percentage() {
        assert!(parse_eviction_flag("memory.available<NaN%").is_err());
        assert!(parse_eviction_flag("memory.available<inf%").is_err());
    }

    #[test]
    fn test_parse_eviction_flag_invalid_syntax() {
        // No '<' operator.
        assert!(parse_eviction_flag("memory.available=100Mi").is_err());
        // No value.
        assert!(parse_eviction_flag("memory.available<").is_err());
        // Unsupported '>' op (upstream only allows '<').
        assert!(parse_eviction_flag("memory.available>100Mi").is_err());
    }

    #[test]
    fn transition_period_reaches_consumer_from_config_and_flag_wins() {
        use rusternetes_kubelet::config::KubeletConfiguration;
        let cfg = KubeletConfiguration {
            eviction_pressure_transition_period: Some(Duration::from_secs(45)),
            ..Default::default()
        };
        // Config value reaches the EvictionManager's transition_period.
        let p = resolve_transition_period(None, Some(&cfg)).unwrap();
        assert_eq!(p, Duration::from_secs(45));
        let m = EvictionManager::with_config(Vec::new(), p);
        assert_eq!(m.transition_period, Duration::from_secs(45));
        // Flag takes precedence over the file (kubeletConfigFlagPrecedence).
        assert_eq!(
            resolve_transition_period(Some("2m"), Some(&cfg)).unwrap(),
            Duration::from_secs(120)
        );
        // Unset / zero / no file => upstream 5m default.
        assert_eq!(
            resolve_transition_period(None, None).unwrap(),
            Duration::from_secs(300)
        );
        let zero = KubeletConfiguration {
            eviction_pressure_transition_period: Some(Duration::ZERO),
            ..Default::default()
        };
        assert_eq!(
            resolve_transition_period(None, Some(&zero)).unwrap(),
            Duration::from_secs(300)
        );
        assert!(resolve_transition_period(Some("bogus"), None).is_err());
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(
            parse_duration("1h30m"),
            Some(Duration::from_secs(3600 + 1800))
        );
        assert_eq!(parse_duration("60"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("abc"), None);
    }

    #[test]
    fn test_statvfs_temp_dir() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let (avail, total, ifree, itotal, resolved) =
            statvfs_for_root_dir(tmp.path()).expect("statvfs on tempdir");
        // Real filesystem must report reasonable values.
        assert!(total > 0, "total bytes must be > 0");
        assert!(itotal > 0, "total inodes must be > 0");
        assert!(avail <= total, "available <= total bytes");
        assert!(ifree <= itotal, "free <= total inodes");
        // Resolved path should be the tempdir itself (or its parent if the
        // dir vanished mid-test, but here it exists).
        assert_eq!(resolved, tmp.path());
    }

    #[test]
    fn test_statvfs_nonexistent_falls_back_to_root() {
        let bogus = PathBuf::from("/this/path/does/not/exist/anywhere");
        let (avail, total, _ifree, _itotal, resolved) =
            statvfs_for_root_dir(&bogus).expect("fallback statvfs");
        assert!(total > 0);
        assert!(avail <= total);
        // Resolved must be `/`.
        assert_eq!(resolved, PathBuf::from("/"));
    }

    #[test]
    fn test_disabled_when_no_thresholds() {
        let mut mgr = EvictionManager::with_config(vec![], Duration::from_secs(5));
        assert!(mgr.is_disabled());
        let stats = NodeStats {
            memory_available_bytes: 1,
            memory_total_bytes: 1_000_000_000,
            nodefs_available_bytes: 1,
            nodefs_total_bytes: 1_000_000_000,
            nodefs_inodes_free: 1,
            nodefs_inodes_total: 1_000_000,
            pid_available: 1,
            pid_total: 32768,
            imagefs: None,
        };
        let active = mgr.check_eviction_needed(&stats);
        assert!(
            active.is_empty(),
            "disabled manager must never report pressure"
        );
    }

    fn pressure_stats() -> NodeStats {
        // Memory critically low (below 100Mi default).
        NodeStats {
            memory_available_bytes: 10 * 1024 * 1024,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 50 * 1024 * 1024 * 1024,
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
            nodefs_inodes_free: 5_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
            imagefs: None,
        }
    }

    fn healthy_stats() -> NodeStats {
        NodeStats {
            memory_available_bytes: 4 * 1024 * 1024 * 1024,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 50 * 1024 * 1024 * 1024,
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
            nodefs_inodes_free: 5_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
            imagefs: None,
        }
    }

    #[test]
    fn test_transition_period_holds_signal_after_recovery() {
        let mut mgr = EvictionManager::with_config(
            vec![EvictionThreshold {
                signal: EvictionSignal::MemoryAvailable,
                hard: Some(EvictionValue::Absolute(100 * 1024 * 1024)),
                soft: None,
                grace_period: None,
            }],
            Duration::from_secs(5),
        );

        let t0 = Instant::now();
        // Tick 1 @ t=0s: pressure observed for the first time, BUT within
        // HARD_MIN_OBSERVATION → not yet reported.
        let signals = mgr.check_eviction_needed_at(&pressure_stats(), t0);
        assert!(
            signals.is_empty(),
            "first observation should be gated by observation window"
        );

        // Tick 2 @ t=11s: pressure still observed, past observation window.
        let signals = mgr.check_eviction_needed_at(&pressure_stats(), t0 + Duration::from_secs(11));
        assert_eq!(signals, vec![EvictionSignal::MemoryAvailable]);

        // Tick 3 @ t=20s: pressure STILL observed (last_observed_at
        // advances to t=20s).
        let signals = mgr.check_eviction_needed_at(&pressure_stats(), t0 + Duration::from_secs(20));
        assert_eq!(signals, vec![EvictionSignal::MemoryAvailable]);

        // Tick 4 @ t=22s: recovery starts. Signal must STAY active because
        // only 2s has elapsed since `last_observed_at` (= t=20s).
        let signals = mgr.check_eviction_needed_at(&healthy_stats(), t0 + Duration::from_secs(22));
        assert_eq!(
            signals,
            vec![EvictionSignal::MemoryAvailable],
            "recovered signal must stay in active set during transition period"
        );

        // Tick 5 @ t=24s: 4s after last_observed_at — still under 5s.
        let signals = mgr.check_eviction_needed_at(&healthy_stats(), t0 + Duration::from_secs(24));
        assert_eq!(signals, vec![EvictionSignal::MemoryAvailable]);

        // Tick 6 @ t=26s: 6s after last_observed_at — transition period
        // elapsed, must clear.
        let signals = mgr.check_eviction_needed_at(&healthy_stats(), t0 + Duration::from_secs(26));
        assert!(
            signals.is_empty(),
            "pressure must clear after transition period elapses"
        );
    }

    #[test]
    fn test_no_pressure_stays_clear() {
        let mut mgr = EvictionManager::new();
        let t0 = Instant::now();
        for i in 0..10 {
            let active =
                mgr.check_eviction_needed_at(&healthy_stats(), t0 + Duration::from_secs(i));
            assert!(active.is_empty());
        }
    }

    #[test]
    fn test_log_rate_limit_under_sustained_pressure() {
        // We can't easily intercept tracing output without a subscriber; instead,
        // verify the internal `last_pressure_log` only advances on transitions
        // or once per PRESSURE_LOG_INTERVAL.
        let mut mgr = EvictionManager::with_config(
            vec![EvictionThreshold {
                signal: EvictionSignal::MemoryAvailable,
                hard: Some(EvictionValue::Absolute(100 * 1024 * 1024)),
                soft: None,
                grace_period: None,
            }],
            Duration::from_secs(60),
        );

        let t0 = Instant::now();
        let mut log_advances = 0usize;
        let mut last_seen: Option<Instant> = None;

        // 100 ticks across 1 second (well below PRESSURE_LOG_INTERVAL of 60s).
        for i in 0..100 {
            let now = t0 + Duration::from_millis(i * 10) + Duration::from_secs(11);
            // Past the observation window from t0.
            mgr.check_eviction_needed_at(&pressure_stats(), now);
            if mgr.last_pressure_log != last_seen {
                log_advances += 1;
                last_seen = mgr.last_pressure_log;
            }
        }

        // Exactly one advance: the initial transition into pressure.
        assert!(
            log_advances <= 2,
            "expected ≤ 2 log advances over 100 ticks; got {}",
            log_advances
        );
    }

    #[test]
    fn test_qos_class_best_effort() {
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("test-pod"),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "test".to_string(),
                    image: "nginx".to_string(),
                    resources: None,
                    image_pull_policy: None,
                    command: None,
                    args: None,
                    ports: None,
                    env: None,
                    volume_mounts: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    working_dir: None,
                    security_context: None,
                    restart_policy: None,
                    resize_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                }],
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: None,
                node_selector: None,
                node_name: None,
                volumes: None,
                affinity: None,
                tolerations: None,
                service_account_name: None,
                service_account: None,
                priority: None,
                priority_class_name: None,
                hostname: None,
                subdomain: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                automount_service_account_token: None,
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
                resource_claims: None,
                active_deadline_seconds: None,
                dns_policy: None,
                dns_config: None,
                security_context: None,
                image_pull_secrets: None,
                share_process_namespace: None,
                readiness_gates: None,
                runtime_class_name: None,
                enable_service_links: None,
                preemption_policy: None,
                host_users: None,
                set_hostname_as_fqdn: None,
                termination_grace_period_seconds: None,
                host_aliases: None,
                os: None,
                scheduling_gates: None,
                resources: None,
                ..Default::default()
            }),
            status: None,
        };

        assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
    }

    #[test]
    fn test_memory_pressure_detection_after_observation_window() {
        let mut manager = EvictionManager::new();

        let low_memory_stats = NodeStats {
            memory_available_bytes: 50 * 1024 * 1024,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 50 * 1024 * 1024 * 1024,
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
            nodefs_inodes_free: 1_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
            imagefs: None,
        };

        let t0 = Instant::now();
        // First observation gated.
        let signals = manager.check_eviction_needed_at(&low_memory_stats, t0);
        assert!(signals.is_empty());
        // After observation window: signal reported.
        let signals =
            manager.check_eviction_needed_at(&low_memory_stats, t0 + Duration::from_secs(11));
        assert!(signals.contains(&EvictionSignal::MemoryAvailable));
    }

    #[test]
    fn test_disk_pressure_detection_after_observation_window() {
        let mut manager = EvictionManager::new();

        let low_disk_stats = NodeStats {
            memory_available_bytes: 2 * 1024 * 1024 * 1024,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 5 * 1024 * 1024 * 1024, // 5% of 100GiB
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
            nodefs_inodes_free: 1_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
            imagefs: None,
        };

        let t0 = Instant::now();
        let _ = manager.check_eviction_needed_at(&low_disk_stats, t0);
        let signals =
            manager.check_eviction_needed_at(&low_disk_stats, t0 + Duration::from_secs(11));
        assert!(signals.contains(&EvictionSignal::NodeFsAvailable));
    }

    // Ports pkg/kubelet/eviction/eviction_manager_test.go
    // TestStaticCriticalPodsAreNotEvicted / TestCriticalPodsAreNotEvicted:
    // static, mirror and system-critical-priority pods are never evicted.
    fn candidate(
        name: &str,
        annotations: &[(&str, &str)],
        priority: Option<i32>,
    ) -> (Pod, PodStats) {
        let mut pod = Pod::new(
            name,
            rusternetes_common::resources::PodSpec {
                priority,
                ..Default::default()
            },
        );
        pod.metadata.namespace = Some("kube-system".to_string());
        if !annotations.is_empty() {
            pod.metadata.annotations = Some(
                annotations
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
        let stats = PodStats {
            name: name.to_string(),
            namespace: "kube-system".to_string(),
            memory_usage_bytes: 1 << 30,
            disk_usage_bytes: 1 << 30,
            qos_class: QoSClass::BestEffort,
        };
        (pod, stats)
    }

    #[test]
    fn eviction_skips_static_mirror_and_critical_pods() {
        let cases = [
            (
                "static",
                vec![("kubernetes.io/config.source", "file")],
                None,
            ),
            ("mirror", vec![("kubernetes.io/config.mirror", "abc")], None),
            ("critical", vec![], Some(SYSTEM_CRITICAL_PRIORITY)),
        ];
        let (normal, normal_stats) =
            candidate("normal", &[("kubernetes.io/config.source", "api")], Some(0));
        for (name, ann, prio) in cases {
            let (pod, stats) = candidate(name, &ann, prio);
            let mut stats_map = HashMap::new();
            stats_map.insert(format!("kube-system/{}", name), stats);
            stats_map.insert("kube-system/normal".to_string(), normal_stats.clone());
            let m = EvictionManager::new();
            let picked = m.select_pods_for_eviction(
                &[pod, normal.clone()],
                &stats_map,
                &EvictionSignal::NodeFsAvailable,
            );
            assert_eq!(picked, vec!["kube-system/normal".to_string()], "{name}");
        }
    }

    #[test]
    fn is_critical_pod_matches_upstream() {
        let (p, _) = candidate(
            "a",
            &[("kubernetes.io/config.source", "api")],
            Some(SYSTEM_CRITICAL_PRIORITY - 1),
        );
        assert!(!is_critical_pod(&p));
        let (p, _) = candidate("b", &[], None);
        assert!(!is_critical_pod(&p));
    }

    // Ports pkg/kubelet/eviction/eviction_manager_test.go TestNodeReclaimFuncs
    // (expectContainerGcCall / expectImageGcCall per fs layout) and
    // helpers.go buildSignalToNodeReclaimFuncs.
    use NodeReclaimFunc::{DeleteAllUnusedContainers, DeleteUnusedImages};

    #[test]
    fn node_reclaim_funcs_no_image_fs() {
        let m = build_signal_to_node_reclaim_funcs(false, false);
        for sig in [
            EvictionSignal::NodeFsAvailable,
            EvictionSignal::NodeFsInodesFree,
            EvictionSignal::ImageFsAvailable,
            EvictionSignal::ImageFsInodesFree,
        ] {
            assert_eq!(m[&sig], vec![DeleteAllUnusedContainers, DeleteUnusedImages]);
        }
    }

    #[test]
    fn node_reclaim_funcs_dedicated_image_fs() {
        let m = build_signal_to_node_reclaim_funcs(true, false);
        assert!(m[&EvictionSignal::NodeFsAvailable].is_empty());
        assert!(m[&EvictionSignal::NodeFsInodesFree].is_empty());
        assert_eq!(
            m[&EvictionSignal::ImageFsAvailable],
            vec![DeleteAllUnusedContainers, DeleteUnusedImages]
        );
        assert_eq!(
            m[&EvictionSignal::ImageFsInodesFree],
            vec![DeleteAllUnusedContainers, DeleteUnusedImages]
        );
    }

    #[test]
    fn node_reclaim_funcs_split_container_image_fs() {
        let m = build_signal_to_node_reclaim_funcs(true, true);
        assert_eq!(
            m[&EvictionSignal::ImageFsAvailable],
            vec![DeleteUnusedImages]
        );
        assert_eq!(
            m[&EvictionSignal::NodeFsAvailable],
            vec![DeleteAllUnusedContainers]
        );
    }

    struct MockDiskGc {
        container_calls: std::sync::atomic::AtomicUsize,
        image_calls: std::sync::atomic::AtomicUsize,
        /// Stats observed after GC (`summaryAfterGC`).
        after_gc: NodeStats,
    }

    #[async_trait::async_trait]
    impl NodeReclaimer for MockDiskGc {
        async fn delete_all_unused_containers(&self) -> Result<()> {
            self.container_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn delete_unused_images(&self) -> Result<()> {
            self.image_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn observe(&self) -> NodeStats {
            self.after_gc.clone()
        }
    }

    fn gib(n: u64) -> u64 {
        n * 1024 * 1024 * 1024
    }

    fn imagefs_stats(available_gib: u64) -> NodeStats {
        NodeStats {
            imagefs: Some(FsUsage {
                available_bytes: gib(available_gib),
                total_bytes: gib(16),
                inodes_free: 1_000_000,
                inodes_total: 1_000_000,
            }),
            ..healthy_stats()
        }
    }

    fn imagefs_manager() -> EvictionManager {
        EvictionManager::with_config(
            vec![EvictionThreshold {
                signal: EvictionSignal::ImageFsAvailable,
                hard: Some(EvictionValue::Absolute(gib(1))),
                soft: None,
                grace_period: None,
            }],
            Duration::from_secs(300),
        )
    }

    #[test]
    fn imagefs_signal_reads_dedicated_image_fs() {
        let mgr = imagefs_manager();
        // Healthy nodefs, starved imagefs: only the dedicated stats trip it.
        let mut starved = imagefs_stats(0);
        starved.imagefs.as_mut().unwrap().available_bytes = 100 * 1024 * 1024;
        assert!(!mgr.no_thresholds_met(&starved));
        assert!(mgr.no_thresholds_met(&imagefs_stats(8)));
    }

    #[tokio::test]
    async fn reclaim_gc_restores_disk_no_eviction() {
        let mgr = imagefs_manager();
        let gc = MockDiskGc {
            container_calls: Default::default(),
            image_calls: Default::default(),
            after_gc: imagefs_stats(8),
        };
        let funcs = build_signal_to_node_reclaim_funcs(true, false)
            .remove(&EvictionSignal::ImageFsAvailable)
            .unwrap();
        assert!(reclaim_node_level_resources(&funcs, &gc, |s| mgr.no_thresholds_met(s)).await);
        assert_eq!(
            gc.container_calls.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(gc.image_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reclaim_gc_insufficient_still_evicts() {
        let mgr = imagefs_manager();
        let mut still = imagefs_stats(0);
        still.imagefs.as_mut().unwrap().available_bytes = 100 * 1024 * 1024;
        let gc = MockDiskGc {
            container_calls: Default::default(),
            image_calls: Default::default(),
            after_gc: still,
        };
        let funcs = build_signal_to_node_reclaim_funcs(true, false)
            .remove(&EvictionSignal::ImageFsAvailable)
            .unwrap();
        assert!(!reclaim_node_level_resources(&funcs, &gc, |s| mgr.no_thresholds_met(s)).await);
        assert_eq!(gc.image_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reclaim_without_funcs_never_satisfies() {
        let mgr = imagefs_manager();
        let gc = MockDiskGc {
            container_calls: Default::default(),
            image_calls: Default::default(),
            after_gc: imagefs_stats(8),
        };
        assert!(!reclaim_node_level_resources(&[], &gc, |s| mgr.no_thresholds_met(s)).await);
        assert_eq!(gc.image_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
