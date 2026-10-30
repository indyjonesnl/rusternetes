//! Unit tests moved from `src/advanced.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::affinity::{
        check_node_affinity, check_pod_anti_affinity, match_selector, matches_pod_affinity_term,
    };
    pub use rusternetes_common::quantity::parse_resource_value;
    pub use rusternetes_common::resources::{
        IntOrString, Node, Pod, PodDisruptionBudget, PriorityClass, Taint, Toleration,
        TopologySpreadConstraint,
    };
    pub use std::collections::HashMap;
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::advanced::*;

    #[test]
    fn test_parse_cpu_quantity() {
        assert_eq!(parse_resource_quantity("100m", "cpu"), 100);
        assert_eq!(parse_resource_quantity("1", "cpu"), 1000);
        assert_eq!(parse_resource_quantity("2", "cpu"), 2000);
        // Decimal CPU values (common in K8s)
        assert_eq!(parse_resource_quantity("0.5", "cpu"), 500);
        assert_eq!(parse_resource_quantity("0.8", "cpu"), 800);
        assert_eq!(parse_resource_quantity("1.5", "cpu"), 1500);
        assert_eq!(parse_resource_quantity("0.1", "cpu"), 100);
        assert_eq!(parse_resource_quantity("0.25", "cpu"), 250);
    }

    #[test]
    fn test_parse_memory_quantity() {
        assert_eq!(parse_resource_quantity("1Ki", "memory"), 1024);
        assert_eq!(parse_resource_quantity("1Mi", "memory"), 1024 * 1024);
        assert_eq!(parse_resource_quantity("1Gi", "memory"), 1024 * 1024 * 1024);
        assert_eq!(
            parse_resource_quantity("8Gi", "memory"),
            8 * 1024 * 1024 * 1024
        );
        // SI units
        assert_eq!(parse_resource_quantity("128M", "memory"), 128_000_000);
        assert_eq!(parse_resource_quantity("1G", "memory"), 1_000_000_000);
        // Plain bytes
        assert_eq!(parse_resource_quantity("128974848", "memory"), 128974848);
        // Scientific notation
        assert_eq!(parse_resource_quantity("129e6", "memory"), 129_000_000);
    }

    /// The `<number>` production allows a decimal point with every suffix,
    /// so these are ordinary quantities that a pod spec may carry. They used
    /// to parse as 0, which reads to the scheduler as "requests nothing".
    ///
    /// Expected values checked against `resource.ParseQuantity(s).Value()`
    /// from `k8s.io/apimachinery`.
    #[test]
    fn test_parse_fractional_memory_quantity() {
        assert_eq!(parse_resource_quantity("0.5Gi", "memory"), 536_870_912);
        assert_eq!(parse_resource_quantity("1.5Gi", "memory"), 1_610_612_736);
        assert_eq!(parse_resource_quantity("0.5Mi", "memory"), 524_288);
        assert_eq!(parse_resource_quantity("2.5Mi", "memory"), 2_621_440);
        assert_eq!(parse_resource_quantity("0.5Ki", "memory"), 512);
        assert_eq!(parse_resource_quantity("1.5G", "memory"), 1_500_000_000);
        assert_eq!(parse_resource_quantity("0.5M", "memory"), 500_000);
        assert_eq!(parse_resource_quantity("1.5k", "memory"), 1_500);
        // A fractional quantity must never round down to "free".
        assert!(parse_resource_quantity("0.25Gi", "memory") > 0);
    }

    /// `Pi`/`Ei` complete the binarySI set; both previously parsed as 0.
    #[test]
    fn test_parse_large_binary_suffixes() {
        assert_eq!(parse_resource_quantity("1Ti", "memory"), 1_099_511_627_776);
        assert_eq!(
            parse_resource_quantity("1Pi", "memory"),
            1_125_899_906_842_624
        );
        assert_eq!(
            parse_resource_quantity("1Ei", "memory"),
            1_152_921_504_606_846_976
        );
        assert_eq!(
            parse_resource_quantity("2Pi", "memory"),
            2_251_799_813_685_248
        );
    }

    /// Decimal SI suffixes and the bare `<decimalExponent>` form.
    #[test]
    fn test_parse_decimal_si_and_exponent() {
        assert_eq!(parse_resource_quantity("1T", "memory"), 1_000_000_000_000);
        assert_eq!(
            parse_resource_quantity("1P", "memory"),
            1_000_000_000_000_000
        );
        assert_eq!(
            parse_resource_quantity("1E", "memory"),
            1_000_000_000_000_000_000
        );
        assert_eq!(parse_resource_quantity("1k", "memory"), 1_000);
        // "E"/"e" as an exponent marker rather than the exa suffix.
        assert_eq!(parse_resource_quantity("129E6", "memory"), 129_000_000);
        assert_eq!(parse_resource_quantity("1.5e3", "memory"), 1_500);
        // Sub-unit decimalSI suffixes upstream accepts for any resource.
        assert_eq!(parse_resource_quantity("2n", "memory"), 1);
        assert_eq!(parse_resource_quantity("1500u", "memory"), 1);
    }

    /// CPU accepts the same grammar; `m` is the shared 10^-3 decimalSI suffix.
    #[test]
    fn test_parse_cpu_fractional_and_milli() {
        assert_eq!(parse_resource_quantity("100m", "cpu"), 100);
        assert_eq!(parse_resource_quantity("1500m", "cpu"), 1500);
        assert_eq!(parse_resource_quantity("0.7", "cpu"), 700);
        assert_eq!(parse_resource_quantity("2.5", "cpu"), 2500);
        // Sub-millicore precision rounds up, matching `Quantity.MilliValue()`
        // (ceiling) — asking for some CPU must never account as none.
        assert_eq!(parse_resource_quantity("10.5m", "cpu"), 11);
        assert_eq!(parse_resource_quantity("10.1m", "cpu"), 11);
    }

    /// Malformed input degrades to 0; callers have no error channel.
    #[test]
    fn test_parse_quantity_rejects_malformed_input() {
        for bad in ["", "   ", "abc", "Gi", "1Xi", "--5", "inf", "NaN", "1..2"] {
            assert_eq!(
                parse_resource_quantity(bad, "memory"),
                0,
                "expected {bad:?} to parse as 0"
            );
            assert_eq!(
                parse_resource_quantity(bad, "cpu"),
                0,
                "expected {bad:?} to parse as 0"
            );
        }
    }

    /// An exponent past the i64 range is a *valid* quantity upstream —
    /// `ParseQuantity` only caps BinarySI (`quantity.go`, `maxAllowed`), so a
    /// DecimalExponent quantity saturates in `ScaledValue`. Match that rather
    /// than silently reading it as 0, which would be indistinguishable from
    /// "requests nothing".
    #[test]
    fn test_parse_quantity_saturates_beyond_i64() {
        assert_eq!(parse_resource_quantity("1e400", "memory"), i64::MAX);
        assert_eq!(parse_resource_quantity("-1e400", "memory"), i64::MIN);
    }

    #[test]
    fn test_toleration_matches_taint() {
        let taint = Taint {
            key: "key1".to_string(),
            value: Some("value1".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        };

        let toleration = Toleration {
            key: Some("key1".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("value1".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        };

        assert!(toleration_matches_taint(&toleration, &taint));
    }

    use rusternetes_common::resources::NodeStatus;

    /// Helper: create a minimal container with resource requests
    fn make_container(cpu: &str, memory: &str) -> rusternetes_common::resources::Container {
        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), cpu.to_string());
        requests.insert("memory".to_string(), memory.to_string());
        rusternetes_common::resources::Container {
            name: "main".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            }),
            volume_mounts: None,
            volume_devices: None,
            image_pull_policy: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            ..Default::default()
        }
    }

    /// Helper: create a node with allocatable CPU and memory
    fn make_node(name: &str, cpu: &str, memory: &str) -> Node {
        let mut allocatable = HashMap::new();
        allocatable.insert("cpu".to_string(), cpu.to_string());
        allocatable.insert("memory".to_string(), memory.to_string());
        let mut node = Node::new(name);
        node.status = Some(NodeStatus {
            capacity: None,
            allocatable: Some(allocatable),
            conditions: None,
            addresses: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        });
        node
    }

    /// Helper: create a pod with given name, priority, and resource requests, scheduled on a node
    fn make_scheduled_pod(
        name: &str,
        priority: i32,
        cpu: &str,
        memory: &str,
        node_name: &str,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container(cpu, memory)],
            priority: Some(priority),
            node_name: Some(node_name.to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Running),
            ..Default::default()
        });
        pod
    }

    /// Helper: create an unscheduled pod (the incoming pod wanting resources)
    fn make_incoming_pod(
        name: &str,
        priority: i32,
        cpu: &str,
        memory: &str,
        preemption_policy: Option<&str>,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container(cpu, memory)],
            priority: Some(priority),
            preemption_policy: preemption_policy.map(|s| s.to_string()),
            ..Default::default()
        };
        Pod::new(name, spec)
    }

    /// Extended-resource fit must honor canonical k8s quantity strings on the
    /// node's advertised capacity. The conformance `AddExtendedResource` helper
    /// writes `resource.MustParse("1000")`, which serializes as "1k"; a raw i64
    /// parse of "1k" yields 0 and wrongly rejects a pod that requests the
    /// resource (#542 — SchedulerPreemption PreemptionExecutionPath).
    #[test]
    fn test_extended_resource_fit_with_canonical_quantity() {
        let mut alloc = HashMap::new();
        alloc.insert("cpu".to_string(), "4".to_string());
        alloc.insert("memory".to_string(), "8Gi".to_string());
        alloc.insert("example.com/fakecpu".to_string(), "1k".to_string()); // == 1000
        let mut node = Node::new("node-2");
        node.status = Some(NodeStatus {
            capacity: None,
            allocatable: Some(alloc),
            conditions: None,
            addresses: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        });

        let fakecpu_pod = |name: &str, req: &str| {
            let mut requests = HashMap::new();
            requests.insert("example.com/fakecpu".to_string(), req.to_string());
            let mut container = make_container("0", "0");
            container.resources = Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            });
            Pod::new(
                name,
                rusternetes_common::resources::PodSpec {
                    containers: vec![container],
                    ..Default::default()
                },
            )
        };

        // 200 of 1000 ("1k") MUST fit (score > 0).
        let pod = fakecpu_pod("rs-pod1", "200");
        assert!(
            calculate_resource_score_with_pods(&node, &pod, &[]) > 0,
            "pod requesting 200 of a 1k extended resource must fit"
        );

        // 2k (2000) of 1000 MUST NOT fit (score == 0).
        let big = fakecpu_pod("too-big", "2k");
        assert_eq!(
            calculate_resource_score_with_pods(&node, &big, &[]),
            0,
            "pod requesting 2k of a 1k extended resource must not fit"
        );
    }

    /// A node's freed space must be reserved for a higher-or-equal-priority pod
    /// already NOMINATED to it (by preemption), so a lower-priority candidate
    /// does not schedule into it and get re-preempted in a loop (#542).
    #[test]
    fn test_nominated_pod_reserves_extended_resource() {
        let mut alloc = HashMap::new();
        alloc.insert("cpu".to_string(), "4".to_string());
        alloc.insert("memory".to_string(), "8Gi".to_string());
        alloc.insert("example.com/fakecpu".to_string(), "1k".to_string()); // 1000
        let mut node = Node::new("node-2");
        node.status = Some(NodeStatus {
            capacity: None,
            allocatable: Some(alloc),
            conditions: None,
            addresses: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        });

        let fakecpu_pod = |name: &str, req: &str, priority: i32| {
            let mut requests = HashMap::new();
            requests.insert("example.com/fakecpu".to_string(), req.to_string());
            let mut container = make_container("0", "0");
            container.resources = Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            });
            let mut pod = Pod::new(
                name,
                rusternetes_common::resources::PodSpec {
                    containers: vec![container],
                    priority: Some(priority),
                    ..Default::default()
                },
            );
            pod.metadata = pod.metadata.with_namespace("default");
            pod
        };

        // A high-priority preemptor (900) nominated to node-2 but not yet bound.
        let mut nominee = fakecpu_pod("preemptor", "900", 100);
        nominee.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Pending),
            nominated_node_name: Some("node-2".to_string()),
            ..Default::default()
        });

        // Low-priority candidate (priority 1) requesting 200.
        let candidate = fakecpu_pod("rs-pod1", "200", 1);

        // Without the nominee, 200 of 1000 fits.
        assert!(
            calculate_resource_score_with_pods(&node, &candidate, &[]) > 0,
            "200 of 1000 must fit when nothing is reserved"
        );

        // With the higher-priority nominee reserving 900, only 100 is free → 200 must NOT fit.
        assert_eq!(
            calculate_resource_score_with_pods(&node, &candidate, std::slice::from_ref(&nominee)),
            0,
            "candidate must not schedule into space reserved for a higher-priority nominee"
        );

        // The nominee itself is not reserved against itself — it still fits its nominated node.
        assert!(
            calculate_resource_score_with_pods(&node, &nominee, std::slice::from_ref(&nominee)) > 0,
            "the nominee must still fit its own nominated node"
        );
    }

    #[test]
    fn test_preemption_policy_never_should_not_preempt() {
        // Node with 2 CPUs
        let node = make_node("node-1", "2", "4Gi");

        // Existing low-priority pod using 1 CPU on node-1
        let existing = make_scheduled_pod("low-pri-pod", 100, "1", "1Gi", "node-1");

        // Incoming high-priority pod with preemptionPolicy=Never
        let incoming = make_incoming_pod("high-pri-pod", 1000, "2", "2Gi", Some("Never"));

        let (can_preempt, pods_to_evict) = check_preemption(&node, &incoming, &[existing]);

        assert!(
            !can_preempt,
            "Pod with preemptionPolicy=Never should not preempt"
        );
        assert!(
            pods_to_evict.is_empty(),
            "No pods should be evicted when preemptionPolicy=Never"
        );
    }

    #[test]
    fn test_preemption_blocked_for_system_critical_pod() {
        // Node with 2 CPUs
        let node = make_node("node-1", "2", "4Gi");

        // Existing system-critical pod (priority 2000000000) using 1 CPU
        let system_pod = make_scheduled_pod("system-critical", 2_000_000_000, "1", "1Gi", "node-1");

        // Incoming pod with priority 1000000000 — lower than the system-critical pod
        let incoming = make_incoming_pod("wants-resources", 1_000_000_000, "2", "2Gi", None);

        let (can_preempt, pods_to_evict) = check_preemption(&node, &incoming, &[system_pod]);

        // The incoming pod has lower priority than the system-critical pod,
        // so it should NOT be able to preempt it
        assert!(
            !can_preempt || pods_to_evict.is_empty(),
            "System-critical pod (priority >= 2000000000) should not be preempted by lower-priority pod"
        );
    }

    #[test]
    fn test_preemption_works_normally_for_lower_priority_pods() {
        // Sanity check: normal preemption still works
        let node = make_node("node-1", "2000m", "4Gi");

        // Existing low-priority pod using 1500m CPU
        let existing = make_scheduled_pod("low-pri", 100, "1500m", "1Gi", "node-1");

        // Incoming high-priority pod needs 1500m CPU (won't fit without eviction)
        let incoming = make_incoming_pod("high-pri", 1000, "1500m", "1Gi", None);

        let (can_preempt, pods_to_evict) = check_preemption(&node, &incoming, &[existing]);

        assert!(can_preempt, "Normal preemption should work");
        assert!(
            pods_to_evict.contains(&"low-pri".to_string()),
            "Low-priority pod should be evicted"
        );
    }

    // ---- HostPort conflict detection tests ----

    use rusternetes_common::resources::ContainerPort;

    /// Helper: create a container with a hostPort binding
    fn make_container_with_host_port(
        host_port: u16,
        protocol: &str,
        host_ip: &str,
    ) -> rusternetes_common::resources::Container {
        rusternetes_common::resources::Container {
            name: "main".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: Some(vec![ContainerPort {
                container_port: 80,
                name: None,
                protocol: protocol.to_string(),
                host_port: Some(host_port),
                host_ip: if host_ip.is_empty() {
                    None
                } else {
                    Some(host_ip.to_string())
                },
            }]),
            env: None,
            env_from: None,
            resources: None,
            volume_mounts: None,
            volume_devices: None,
            image_pull_policy: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            ..Default::default()
        }
    }

    /// Helper: create a pod with a hostPort, scheduled on a node
    fn make_host_port_pod(
        name: &str,
        host_port: u16,
        protocol: &str,
        host_ip: &str,
        node_name: &str,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container_with_host_port(host_port, protocol, host_ip)],
            node_name: Some(node_name.to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Running),
            ..Default::default()
        });
        pod
    }

    /// Helper: create an unscheduled pod with a hostPort (incoming pod)
    fn make_incoming_host_port_pod(
        name: &str,
        host_port: u16,
        protocol: &str,
        host_ip: &str,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container_with_host_port(host_port, protocol, host_ip)],
            ..Default::default()
        };
        Pod::new(name, spec)
    }

    #[test]
    fn test_host_port_no_conflict_when_no_host_ports() {
        let node = make_node("node-1", "2", "4Gi");
        let incoming = make_incoming_pod("pod-a", 0, "100m", "128Mi", None);
        assert!(
            check_host_port_conflicts(&node, &incoming, &[]),
            "Pod without hostPort should have no conflicts"
        );
    }

    #[test]
    fn test_host_port_conflict_same_port_same_protocol_same_ip() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "");

        assert!(
            !check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort, same protocol, same (wildcard) hostIP should conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_different_port() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 9090, "TCP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Different hostPort should not conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_different_protocol() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "UDP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort but different protocol should not conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_different_host_ip() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "10.0.0.1", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "10.0.0.2");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort and protocol but different specific hostIPs should not conflict"
        );
    }

    #[test]
    fn test_host_port_conflict_wildcard_vs_specific_ip() {
        let node = make_node("node-1", "2", "4Gi");
        // Existing pod binds to 0.0.0.0 (all interfaces)
        let existing = make_host_port_pod("existing", 8080, "TCP", "0.0.0.0", "node-1");
        // Incoming pod binds to a specific IP
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "10.0.0.1");

        assert!(
            !check_host_port_conflicts(&node, &incoming, &[existing]),
            "Wildcard hostIP (0.0.0.0) should conflict with any specific IP"
        );
    }

    #[test]
    fn test_host_port_conflict_empty_vs_specific_ip() {
        let node = make_node("node-1", "2", "4Gi");
        // Existing pod with empty hostIP (means all interfaces)
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        // Incoming pod binds to a specific IP
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "10.0.0.1");

        assert!(
            !check_host_port_conflicts(&node, &incoming, &[existing]),
            "Empty hostIP (wildcard) should conflict with any specific IP"
        );
    }

    #[test]
    fn test_host_port_no_conflict_on_different_node() {
        let node = make_node("node-1", "2", "4Gi");
        // Existing pod on node-2 (different node)
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-2");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Pods on different nodes should not conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_terminated_pod() {
        let node = make_node("node-1", "2", "4Gi");
        let mut existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        // Mark existing pod as Succeeded (terminated)
        existing.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Succeeded),
            ..Default::default()
        });
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Terminated pods should not cause conflicts"
        );
    }

    #[test]
    fn test_host_port_allows_same_port_different_ip_and_protocol() {
        // This is the exact scenario from the conformance test:
        // Two pods with same hostPort but different hostIP and protocol should coexist
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("pod-tcp", 8080, "TCP", "10.0.0.1", "node-1");
        let incoming = make_incoming_host_port_pod("pod-udp", 8080, "UDP", "10.0.0.2");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort with different hostIP AND different protocol should not conflict"
        );
    }

    #[test]
    fn test_host_ips_overlap() {
        // Wildcard cases
        assert!(host_ips_overlap("", "10.0.0.1"));
        assert!(host_ips_overlap("10.0.0.1", ""));
        assert!(host_ips_overlap("0.0.0.0", "10.0.0.1"));
        assert!(host_ips_overlap("10.0.0.1", "0.0.0.0"));
        assert!(host_ips_overlap("::", "10.0.0.1"));
        assert!(host_ips_overlap("", ""));
        assert!(host_ips_overlap("0.0.0.0", "0.0.0.0"));

        // Specific IPs
        assert!(host_ips_overlap("10.0.0.1", "10.0.0.1"));
        assert!(!host_ips_overlap("10.0.0.1", "10.0.0.2"));
        assert!(!host_ips_overlap("192.168.1.1", "10.0.0.1"));
    }
    /// Mark a pod as terminating *by preemption*, the way our eviction path does:
    /// deletionTimestamp plus DisruptionTarget=True/PreemptionByScheduler.
    fn mark_preempted(pod: &mut Pod) {
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let status = pod.status.get_or_insert_with(Default::default);
        status.conditions.get_or_insert_with(Vec::new).push(
            rusternetes_common::resources::PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                last_probe_time: None,
                last_transition_time: Some(chrono::Utc::now()),
                reason: Some("PreemptionByScheduler".to_string()),
                message: Some("Preempted by a higher-priority pod".to_string()),
                observed_generation: None,
            },
        );
    }

    /// #1130: a pod that already preempted must not preempt again while its
    /// victim is still terminating on the node it was nominated to.
    ///
    /// Without this gate the scheduler retried, found the preemptor still did
    /// not fit (the victim's resources are accounted for until it really goes
    /// away), and preempted a second victim on another node — which is how
    /// `pod1-1-sched-preemption-medium-priority` was deleted and
    /// `[sig-scheduling] SchedulerPreemption [Serial]` failed at
    /// test/e2e/scheduling/preemption.go:206.
    #[test]
    fn a_pod_with_a_terminating_victim_on_its_nominated_node_is_not_eligible() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        let mut victim = make_scheduled_pod("victim", 1, "100m", "128Mi", "node-1");
        mark_preempted(&mut victim);

        assert!(
            !pod_eligible_to_preempt_others(&preemptor, &[victim]),
            "must wait for the room it already made"
        );
    }

    /// Once the victim is gone, the pod may preempt again (it will normally just
    /// schedule instead).
    #[test]
    fn a_pod_whose_victim_has_gone_is_eligible_again() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        // Only a healthy, higher-priority neighbour remains on the node.
        let neighbour = make_scheduled_pod("neighbour", 2000, "100m", "128Mi", "node-1");

        assert!(pod_eligible_to_preempt_others(&preemptor, &[neighbour]));
    }

    /// A terminating pod on a DIFFERENT node must not block preemption — the
    /// gate is about the node this pod was nominated to.
    #[test]
    fn a_terminating_pod_on_another_node_does_not_block_preemption() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        let mut elsewhere = make_scheduled_pod("elsewhere", 1, "100m", "128Mi", "node-2");
        mark_preempted(&mut elsewhere);

        assert!(pod_eligible_to_preempt_others(&preemptor, &[elsewhere]));
    }

    /// A terminating pod of EQUAL or higher priority is not a victim of this
    /// preemptor, so it must not gate it either.
    #[test]
    fn a_terminating_higher_priority_pod_does_not_block_preemption() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        let mut other = make_scheduled_pod("other", 5000, "100m", "128Mi", "node-1");
        mark_preempted(&mut other);

        assert!(pod_eligible_to_preempt_others(&preemptor, &[other]));
    }

    /// Regression: a pod terminating for an ordinary reason must NOT count as
    /// "this preemptor already made room".
    ///
    /// The first version of this gate tested only `deletionTimestamp`, so any
    /// terminating lower-priority pod on the nominated node — a finished Job's
    /// pod, the previous spec's cleanup — blocked preemption forever and
    /// `[sig-scheduling] SchedulerPreemption` failed the other way round:
    /// `expected pod to be preempted, instead got pod pod0-0-…-low-priority`.
    /// Upstream requires DisruptionTarget=True with reason
    /// PreemptionByScheduler (podTerminatingByPreemption).
    #[test]
    fn a_pod_terminating_for_an_unrelated_reason_does_not_block_preemption() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        // Terminating, but not by preemption: no DisruptionTarget condition.
        let mut ordinary = make_scheduled_pod("ordinary", 1, "100m", "128Mi", "node-1");
        ordinary.metadata.deletion_timestamp = Some(chrono::Utc::now());

        assert!(
            pod_eligible_to_preempt_others(&preemptor, &[ordinary]),
            "an ordinary termination must not be mistaken for our own victim"
        );
    }

    /// A DisruptionTarget condition from a *different* disruptor (eviction API,
    /// node drain) also must not gate preemption.
    #[test]
    fn a_pod_disrupted_by_something_else_does_not_block_preemption() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        let mut drained = make_scheduled_pod("drained", 1, "100m", "128Mi", "node-1");
        drained.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let status = drained.status.get_or_insert_with(Default::default);
        status.conditions.get_or_insert_with(Vec::new).push(
            rusternetes_common::resources::PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                last_probe_time: None,
                last_transition_time: Some(chrono::Utc::now()),
                reason: Some("EvictionByEvictionAPI".to_string()),
                message: None,
                observed_generation: None,
            },
        );

        assert!(pod_eligible_to_preempt_others(&preemptor, &[drained]));
    }

    /// A pod that has never preempted (no nominatedNodeName) is always eligible.
    #[test]
    fn a_pod_without_a_nominated_node_is_eligible() {
        let preemptor = make_incoming_pod("fresh", 1000, "100m", "128Mi", None);
        assert!(pod_eligible_to_preempt_others(&preemptor, &[]));
    }

    // -----------------------------------------------------------------------
    // Victim-node selection (the residual half of #1130).
    //
    // try_preempt used to return the FIRST feasible node, so with a
    // low-priority victim on node-1 and a medium-priority one on node-2 the
    // outcome depended on node iteration order. Observed live, same binaries,
    // ~1 h apart:
    //
    //   Preempting 1 pod(s) on node node-1 ... Evicted pod0-0-…-low-priority
    //   Preempting 1 pod(s) on node node-2 ... Evicted pod1-1-…-medium-priority
    //
    // The second reads as a flake and fails
    // `SchedulerPreemption validates basic preemption works`, which asserts the
    // LOWEST-priority pod is the one preempted.
    // -----------------------------------------------------------------------

    fn candidate(
        node: &str,
        victims: &[&str],
        all_pods: &[Pod],
        pdb_violations: i64,
    ) -> PreemptionCandidate {
        PreemptionCandidate {
            node_name: node.to_string(),
            victims: victims
                .iter()
                .map(|v| {
                    all_pods
                        .iter()
                        .find(|p| p.metadata.name == *v)
                        .unwrap_or_else(|| panic!("fixture pod {v} missing"))
                        .clone()
                })
                .collect(),
            num_pdb_violations: pdb_violations,
        }
    }

    #[test]
    fn test_pick_node_prefers_lowest_priority_victim() {
        let all_pods = vec![
            make_scheduled_pod("pod0-0-low", 100, "100m", "64Mi", "node-1"),
            make_scheduled_pod("pod1-1-medium", 500, "100m", "64Mi", "node-2"),
        ];
        let candidates = vec![
            candidate("node-1", &["pod0-0-low"], &all_pods, 0),
            candidate("node-2", &["pod1-1-medium"], &all_pods, 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates).as_deref(),
            Some("node-1"),
            "must preempt the low-priority victim, not the medium-priority one"
        );
    }

    #[test]
    fn test_pick_node_is_independent_of_candidate_order() {
        // The live bug was order dependence, so the reversed list must agree.
        let all_pods = vec![
            make_scheduled_pod("pod0-0-low", 100, "100m", "64Mi", "node-1"),
            make_scheduled_pod("pod1-1-medium", 500, "100m", "64Mi", "node-2"),
        ];
        let reversed = vec![
            candidate("node-2", &["pod1-1-medium"], &all_pods, 0),
            candidate("node-1", &["pod0-0-low"], &all_pods, 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&reversed).as_deref(),
            Some("node-1"),
        );
    }

    #[test]
    fn test_pick_node_pdb_violations_outrank_priority() {
        // Upstream's first criterion. node-1 costs a PDB violation, so node-2
        // wins even though its victim has the higher priority.
        let all_pods = vec![
            make_scheduled_pod("low-but-guarded", 100, "100m", "64Mi", "node-1"),
            make_scheduled_pod("medium-free", 500, "100m", "64Mi", "node-2"),
        ];
        let candidates = vec![
            candidate("node-1", &["low-but-guarded"], &all_pods, 1),
            candidate("node-2", &["medium-free"], &all_pods, 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates).as_deref(),
            Some("node-2"),
        );
    }

    #[test]
    fn test_pick_node_breaks_priority_tie_on_summed_priorities() {
        // Same highest victim priority on both nodes; node-2 costs less in total.
        let all_pods = vec![
            make_scheduled_pod("a-top", 500, "100m", "64Mi", "node-1"),
            make_scheduled_pod("a-extra", 400, "100m", "64Mi", "node-1"),
            make_scheduled_pod("b-top", 500, "100m", "64Mi", "node-2"),
            make_scheduled_pod("b-extra", 100, "100m", "64Mi", "node-2"),
        ];
        let candidates = vec![
            candidate("node-1", &["a-top", "a-extra"], &all_pods, 0),
            candidate("node-2", &["b-top", "b-extra"], &all_pods, 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates).as_deref(),
            Some("node-2"),
        );
    }

    #[test]
    fn test_pick_node_breaks_remaining_tie_on_victim_count() {
        // Identical priorities and sums per victim, so fewest victims wins.
        let all_pods = vec![
            make_scheduled_pod("a1", 100, "100m", "64Mi", "node-1"),
            make_scheduled_pod("b1", 100, "100m", "64Mi", "node-2"),
            make_scheduled_pod("b2", 100, "100m", "64Mi", "node-2"),
        ];
        let candidates = vec![
            candidate("node-2", &["b1", "b2"], &all_pods, 0),
            candidate("node-1", &["a1"], &all_pods, 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates).as_deref(),
            Some("node-1"),
        );
    }

    #[test]
    fn test_pick_node_prefers_latest_started_victim_on_full_tie() {
        // Everything else equal: the victim that started most recently is the
        // cheaper one to kill (upstream latestStartTimeScoreFunc).
        let mut older = make_scheduled_pod("older", 100, "100m", "64Mi", "node-1");
        let mut newer = make_scheduled_pod("newer", 100, "100m", "64Mi", "node-2");
        let base = chrono::Utc::now();
        older.status.as_mut().unwrap().start_time = Some(base - chrono::Duration::hours(2));
        newer.status.as_mut().unwrap().start_time = Some(base);
        let all_pods = vec![older, newer];
        let candidates = vec![
            candidate("node-1", &["older"], &all_pods, 0),
            candidate("node-2", &["newer"], &all_pods, 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates).as_deref(),
            Some("node-2"),
        );
    }

    #[test]
    fn test_pick_node_returns_none_without_candidates() {
        assert_eq!(pick_one_node_for_preemption(&[]), None);
    }

    #[test]
    fn test_pick_node_single_candidate_is_chosen() {
        let all_pods = vec![make_scheduled_pod("only", 100, "100m", "64Mi", "node-1")];
        let candidates = vec![candidate("node-1", &["only"], &all_pods, 7)];
        assert_eq!(
            pick_one_node_for_preemption(&candidates).as_deref(),
            Some("node-1"),
            "a lone candidate is used even when it violates a PDB — upstream has no better option either"
        );
    }

    // -----------------------------------------------------------------------
    // PDB-aware preemption (#1797). try_preempt called the PDB-UNAWARE
    // check_preemption, so budget-protected pods were evicted as freely as
    // unprotected ones and upstream's first victim-node criterion
    // (minNumPDBViolatingScoreFunc) had nothing to score.
    // -----------------------------------------------------------------------

    fn make_pdb_min_available(
        name: &str,
        ns: &str,
        app: &str,
        min_available: i32,
    ) -> PodDisruptionBudget {
        let mut selector_labels = HashMap::new();
        selector_labels.insert("app".to_string(), app.to_string());
        PodDisruptionBudget::new(
            name,
            ns,
            rusternetes_common::resources::PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::Int(min_available)),
                max_unavailable: None,
                selector: Some(rusternetes_common::types::LabelSelector {
                    match_labels: Some(selector_labels),
                    match_expressions: None,
                }),
                unhealthy_pod_eviction_policy: None,
            },
        )
    }

    fn labelled_pod(name: &str, priority: i32, node: &str, app: &str) -> Pod {
        let mut pod = make_scheduled_pod(name, priority, "100m", "64Mi", node);
        pod.metadata.namespace = Some("default".to_string());
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), app.to_string());
        pod.metadata.labels = Some(labels);
        pod
    }

    #[test]
    fn test_count_pdb_violating_victims_zero_without_budgets() {
        let pods = vec![labelled_pod("a", 100, "node-1", "web")];
        assert_eq!(
            count_pdb_violating_victims(&pods[..1], &pods, &[]),
            0,
            "no budgets means nothing can violate one"
        );
    }

    #[test]
    fn test_count_pdb_violating_victims_counts_covered_pod() {
        // One replica, minAvailable=1: evicting it breaks the budget.
        let pods = vec![labelled_pod("only-web", 100, "node-1", "web")];
        let pdbs = vec![make_pdb_min_available("web-pdb", "default", "web", 1)];
        assert_eq!(count_pdb_violating_victims(&pods[..1], &pods, &pdbs), 1,);
    }

    #[test]
    fn test_count_pdb_violating_victims_ignores_uncovered_pod() {
        // The budget selects app=web; the victim is app=batch.
        let pods = vec![
            labelled_pod("web-1", 100, "node-1", "web"),
            labelled_pod("web-2", 100, "node-1", "web"),
            labelled_pod("batch-1", 100, "node-1", "batch"),
        ];
        let pdbs = vec![make_pdb_min_available("web-pdb", "default", "web", 1)];
        assert_eq!(count_pdb_violating_victims(&pods[2..], &pods, &pdbs), 0,);
    }

    #[test]
    fn test_count_pdb_violating_victims_sums_across_victims() {
        let pods = vec![
            labelled_pod("web-1", 100, "node-1", "web"),
            labelled_pod("api-1", 100, "node-1", "api"),
        ];
        let pdbs = vec![
            make_pdb_min_available("web-pdb", "default", "web", 1),
            make_pdb_min_available("api-pdb", "default", "api", 1),
        ];
        assert_eq!(count_pdb_violating_victims(&pods, &pods, &pdbs), 2,);
    }

    #[test]
    fn test_pdb_violation_count_steers_node_choice() {
        // The whole point of the count: node-1's victim is cheaper by priority
        // but protected, so the score chain must prefer node-2 even though its
        // victim has the higher priority.
        let pods = vec![
            labelled_pod("guarded-low", 100, "node-1", "web"),
            labelled_pod("free-medium", 500, "node-2", "batch"),
        ];
        let pdbs = vec![make_pdb_min_available("web-pdb", "default", "web", 1)];
        let candidates = vec![
            PreemptionCandidate {
                node_name: "node-1".to_string(),
                victims: vec![pods[0].clone()],
                num_pdb_violations: count_pdb_violating_victims(&pods[..1], &pods, &pdbs),
            },
            PreemptionCandidate {
                node_name: "node-2".to_string(),
                victims: vec![pods[1].clone()],
                num_pdb_violations: count_pdb_violating_victims(&pods[1..], &pods, &pdbs),
            },
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates).as_deref(),
            Some("node-2"),
            "a PDB violation must outrank a lower victim priority"
        );
    }

    // ---- #2268: resource fit must use upstream PodRequests ----------------
    //
    // Upstream: component-helpers/resource/helpers.go PodRequests (:149-185),
    // fed to the fit plugin by noderesources/fit.go computePodResourceRequest
    // (:293-302) and to node accounting by framework/types.go CalculateResource
    // (:722-731). Upstream cases mirrored: TestPodRequests-style init-container
    // max / sidecar sum / overhead / pod-level override in
    // component-helpers/resource/helpers_test.go.

    fn reqs(pairs: &[(&str, &str)]) -> rusternetes_common::types::ResourceRequirements {
        rusternetes_common::types::ResourceRequirements {
            requests: Some(
                pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            limits: None,
            claims: None,
        }
    }

    /// A pod whose single container asks `cpu`, with optional extras.
    fn pod_with(cpu: &str) -> Pod {
        let mut c = make_container(cpu, "1Mi");
        c.resources = Some(reqs(&[("cpu", cpu)]));
        Pod::new(
            "p",
            rusternetes_common::resources::PodSpec {
                containers: vec![c],
                ..Default::default()
            },
        )
    }

    fn init_container(
        name: &str,
        cpu: &str,
        restartable: bool,
    ) -> rusternetes_common::resources::Container {
        let mut c = make_container(cpu, "1Mi");
        c.name = name.to_string();
        c.resources = Some(reqs(&[("cpu", cpu)]));
        if restartable {
            c.restart_policy = Some("Always".to_string());
        }
        c
    }

    #[test]
    #[serial_test::serial]
    fn init_container_raises_the_pod_request_to_its_max() {
        // Upstream AggregateContainerRequests: init containers define the
        // minimum of any resource (helpers.go max(reqs, initContainerReqs)).
        let mut pod = pod_with("1");
        pod.spec.as_mut().unwrap().init_containers = Some(vec![init_container("i", "3", false)]);
        assert_eq!(pod_request_amounts(&pod)["cpu"], 3000);
        // Node with 2 CPU cannot fit it, node with 4 can.
        assert_eq!(
            calculate_resource_score(&make_node("small", "2", "1Gi"), &pod),
            0
        );
        assert!(calculate_resource_score(&make_node("big", "4", "1Gi"), &pod) > 0);
    }

    #[test]
    #[serial_test::serial]
    fn restartable_init_container_is_added_to_the_pod_request() {
        let mut pod = pod_with("1");
        pod.spec.as_mut().unwrap().init_containers = Some(vec![init_container("side", "1", true)]);
        assert_eq!(pod_request_amounts(&pod)["cpu"], 2000);
        assert_eq!(
            calculate_resource_score(&make_node("n", "1500m", "1Gi"), &pod),
            0
        );
    }

    #[test]
    #[serial_test::serial]
    fn overhead_is_added_to_the_pod_request() {
        let mut pod = pod_with("1");
        pod.spec.as_mut().unwrap().overhead =
            Some([("cpu".to_string(), "1500m".to_string())].into());
        assert_eq!(pod_request_amounts(&pod)["cpu"], 2500);
        assert_eq!(
            calculate_resource_score(&make_node("n", "2", "1Gi"), &pod),
            0
        );
        assert!(calculate_resource_score(&make_node("n", "3", "1Gi"), &pod) > 0);
    }

    #[test]
    #[serial_test::serial]
    fn pod_level_resources_override_the_container_sum_when_gate_on() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::PodLevelResources,
            true,
        );
        let mut pod = pod_with("1");
        pod.spec.as_mut().unwrap().resources = Some(reqs(&[("cpu", "3")]));
        assert_eq!(pod_request_amounts(&pod)["cpu"], 3000);
        assert_eq!(
            calculate_resource_score(&make_node("n", "2", "1Gi"), &pod),
            0
        );
    }

    #[test]
    #[serial_test::serial]
    fn pod_level_resources_ignored_when_gate_off() {
        // SkipPodLevelResources: !EnablePodLevelResources (fit.go:296-297).
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::PodLevelResources,
            false,
        );
        let mut pod = pod_with("1");
        pod.spec.as_mut().unwrap().resources = Some(reqs(&[("cpu", "3")]));
        assert_eq!(pod_request_amounts(&pod)["cpu"], 1000);
        assert!(calculate_resource_score(&make_node("n", "2", "1Gi"), &pod) > 0);
    }

    #[test]
    #[serial_test::serial]
    fn node_usage_counts_existing_pods_init_and_overhead() {
        // Node accounting uses the same PodRequests (types.go:722-731), so an
        // existing pod's init container + overhead consume node capacity.
        let mut existing = make_scheduled_pod("old", 0, "100m", "1Mi", "n");
        {
            let spec = existing.spec.as_mut().unwrap();
            spec.init_containers = Some(vec![init_container("i", "1", false)]);
            spec.overhead = Some([("cpu".to_string(), "500m".to_string())].into());
        }
        // existing requests max(100m, 1) + 500m = 1500m; 1 CPU more won't fit in 2.
        let pod = pod_with("1");
        assert_eq!(
            calculate_resource_score_with_pods(
                &make_node("n", "2", "1Gi"),
                &pod,
                &[existing.clone()]
            ),
            0
        );
        assert!(
            calculate_resource_score_with_pods(&make_node("n", "3", "1Gi"), &pod, &[existing]) > 0
        );
    }

    // ---- Non0CPU / Non0Mem (#2401) ---------------------------------------
    // Ported from TestPodInfoCalculateResources,
    // ../kubernetes/pkg/scheduler/framework/types_test.go:1441-1700.

    fn bare_container(name: &str) -> rusternetes_common::resources::Container {
        let mut c = make_container("1", "1Mi");
        c.name = name.to_string();
        c.resources = None;
        c
    }

    fn pod_of(
        containers: Vec<rusternetes_common::resources::Container>,
        init: Vec<rusternetes_common::resources::Container>,
        pod_level: Option<&[(&str, &str)]>,
    ) -> Pod {
        Pod::new(
            "p",
            rusternetes_common::resources::PodSpec {
                containers,
                init_containers: if init.is_empty() { None } else { Some(init) },
                resources: pod_level.map(reqs),
                ..Default::default()
            },
        )
    }

    fn requested(cpu: &str, mem: &str) -> rusternetes_common::resources::Container {
        let mut c = make_container(cpu, mem);
        c.resources = Some(reqs(&[("cpu", cpu), ("memory", mem)]));
        c
    }

    #[test]
    #[serial_test::serial]
    fn non_zero_requestless_container_gets_defaults() {
        // types_test.go:1457 "requestless container"
        let pod = pod_of(vec![bare_container("a")], vec![], None);
        assert_eq!(
            non_zero_request_amounts(&pod),
            (DEFAULT_MILLI_CPU_REQUEST, DEFAULT_MEMORY_REQUEST)
        );
        assert_eq!(DEFAULT_MEMORY_REQUEST, 209715200);
    }

    #[test]
    #[serial_test::serial]
    fn non_zero_equals_requests_when_set() {
        // types_test.go:1470 "1X container with requests"
        let pod = pod_of(vec![requested("500m", "500Mi")], vec![], None);
        assert_eq!(non_zero_request_amounts(&pod), (500, 500 * 1024 * 1024));
        // :1491 "2X container with requests"
        let pod = pod_of(
            vec![requested("500m", "500Mi"), requested("700m", "800Mi")],
            vec![],
            None,
        );
        assert_eq!(non_zero_request_amounts(&pod), (1200, 1300 * 1024 * 1024));
    }

    #[test]
    #[serial_test::serial]
    fn non_zero_defaults_only_the_containers_missing_a_request() {
        let pod = pod_of(
            vec![requested("500m", "500Mi"), bare_container("b")],
            vec![],
            None,
        );
        assert_eq!(
            non_zero_request_amounts(&pod),
            (500 + 100, 500 * 1024 * 1024 + DEFAULT_MEMORY_REQUEST)
        );
        // raw requests (used for fit) are untouched
        assert_eq!(pod_request_amounts(&pod)["cpu"], 500);
    }

    #[test]
    #[serial_test::serial]
    fn non_zero_pod_level_memory_only() {
        // types_test.go:1584 "1X container with pod-level memory requests"
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::PodLevelResources,
            true,
        );
        let pod = pod_of(
            vec![bare_container("a")],
            vec![bare_container("i")],
            Some(&[("memory", "1200Mi")]),
        );
        assert_eq!(
            non_zero_request_amounts(&pod),
            (DEFAULT_MILLI_CPU_REQUEST, 1200 * 1024 * 1024)
        );
    }

    #[test]
    #[serial_test::serial]
    fn non_zero_pod_level_cpu_only() {
        // types_test.go:1613 "1X container with pod-level cpu requests"
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::PodLevelResources,
            true,
        );
        let pod = pod_of(
            vec![bare_container("a")],
            vec![bare_container("i")],
            Some(&[("cpu", "500m")]),
        );
        assert_eq!(
            non_zero_request_amounts(&pod),
            (500, DEFAULT_MEMORY_REQUEST)
        );
    }

    #[test]
    #[serial_test::serial]
    fn non_zero_pod_level_overrides_containers() {
        // types_test.go:1532 "1X container and 1X sidecar with pod-level requests"
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::PodLevelResources,
            true,
        );
        let mut side = requested("500m", "500Mi");
        side.restart_policy = Some("Always".to_string());
        let pod = pod_of(
            vec![requested("500m", "500Mi")],
            vec![side],
            Some(&[("cpu", "1200m"), ("memory", "1200Mi")]),
        );
        assert_eq!(non_zero_request_amounts(&pod), (1200, 1200 * 1024 * 1024));
    }

    #[test]
    #[serial_test::serial]
    fn score_counts_best_effort_pods_on_the_node() {
        // NodeInfo.GetNonZeroRequested drives resource_allocation.go:188, so
        // a best-effort pod must weigh on its node's score. Node n1 holds a
        // best-effort pod, n2 is empty: n2 must score strictly higher.
        let mut running = make_scheduled_pod("be", 0, "1", "1Mi", "n1");
        running.spec.as_mut().unwrap().containers[0].resources = None;
        let incoming = pod_of(vec![requested("100m", "100Mi")], vec![], None);
        let n1 = make_node("n1", "1", "1Gi");
        let n2 = make_node("n2", "1", "1Gi");
        let all = [running];
        let s1 = calculate_resource_score_with_pods(&n1, &incoming, &all);
        let s2 = calculate_resource_score_with_pods(&n2, &incoming, &all);
        assert!(s2 > s1, "empty node {s2} must outscore loaded node {s1}");
    }
}
