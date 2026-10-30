//! Unit tests moved from `src/host_port.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::Pod;
    pub use rusternetes_common::types::Phase;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::{Container, ContainerPort, PodSpec, PodStatus};
    use rusternetes_kubelet::host_port::*;

    fn port(host_port: Option<u16>, protocol: &str, host_ip: Option<&str>) -> ContainerPort {
        ContainerPort {
            container_port: 80,
            name: None,
            protocol: protocol.to_string(),
            host_port,
            host_ip: host_ip.map(str::to_string),
        }
    }

    fn pod_with_ports(name: &str, node: Option<&str>, ports: Vec<ContainerPort>) -> Pod {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "c".into(),
                image: "img".into(),
                ports: if ports.is_empty() { None } else { Some(ports) },
                ..Default::default()
            }],
            ..Default::default()
        };
        spec.node_name = node.map(str::to_string);
        let mut pod = Pod::new(name, spec);
        pod.metadata.namespace = Some("default".into());
        pod
    }

    fn with_phase(mut pod: Pod, phase: Phase) -> Pod {
        pod.status = Some(PodStatus {
            phase: Some(phase),
            ..Default::default()
        });
        pod
    }

    #[test]
    fn conflict_on_same_port_protocol_and_wildcard_ip() {
        let existing = pod_with_ports(
            "holder",
            Some("node-1"),
            vec![port(Some(21017), "TCP", None)],
        );
        let incoming = pod_with_ports(
            "web-0",
            Some("node-1"),
            vec![port(Some(21017), "TCP", None)],
        );
        let conflict =
            find_host_port_conflict(&incoming, std::slice::from_ref(&existing), "node-1");
        assert_eq!(
            conflict,
            Some(HostPortConflict {
                port: 21017,
                protocol: "TCP".into(),
                conflicting_pod: "holder".into(),
            })
        );
    }

    #[test]
    fn no_conflict_when_both_host_ports_unset() {
        // hostPort omitted (None) and hostPort 0 both mean "no allocation".
        let existing = pod_with_ports("a", Some("node-1"), vec![port(Some(0), "TCP", None)]);
        let incoming = pod_with_ports("b", Some("node-1"), vec![port(None, "TCP", None)]);
        assert_eq!(
            find_host_port_conflict(&incoming, std::slice::from_ref(&existing), "node-1"),
            None
        );
    }

    #[test]
    fn no_conflict_on_different_node() {
        let existing = pod_with_ports(
            "holder",
            Some("node-2"),
            vec![port(Some(21017), "TCP", None)],
        );
        let incoming = pod_with_ports(
            "web-0",
            Some("node-1"),
            vec![port(Some(21017), "TCP", None)],
        );
        assert_eq!(
            find_host_port_conflict(&incoming, std::slice::from_ref(&existing), "node-1"),
            None
        );
    }

    #[test]
    fn no_conflict_with_terminal_pod() {
        // A Failed pod has released its host port.
        let existing = with_phase(
            pod_with_ports(
                "holder",
                Some("node-1"),
                vec![port(Some(21017), "TCP", None)],
            ),
            Phase::Failed,
        );
        let incoming = pod_with_ports(
            "web-0",
            Some("node-1"),
            vec![port(Some(21017), "TCP", None)],
        );
        assert_eq!(
            find_host_port_conflict(&incoming, std::slice::from_ref(&existing), "node-1"),
            None
        );
    }

    #[test]
    fn no_conflict_on_different_protocol() {
        let existing = pod_with_ports(
            "holder",
            Some("node-1"),
            vec![port(Some(21017), "UDP", None)],
        );
        let incoming = pod_with_ports(
            "web-0",
            Some("node-1"),
            vec![port(Some(21017), "TCP", None)],
        );
        assert_eq!(
            find_host_port_conflict(&incoming, std::slice::from_ref(&existing), "node-1"),
            None
        );
    }

    #[test]
    fn no_conflict_on_distinct_specific_host_ips() {
        let existing = pod_with_ports(
            "holder",
            Some("node-1"),
            vec![port(Some(21017), "TCP", Some("10.0.0.1"))],
        );
        let incoming = pod_with_ports(
            "web-0",
            Some("node-1"),
            vec![port(Some(21017), "TCP", Some("10.0.0.2"))],
        );
        assert_eq!(
            find_host_port_conflict(&incoming, std::slice::from_ref(&existing), "node-1"),
            None
        );
    }

    #[test]
    fn conflict_when_one_specific_ip_and_one_wildcard() {
        let existing = pod_with_ports(
            "holder",
            Some("node-1"),
            vec![port(Some(21017), "TCP", Some("10.0.0.1"))],
        );
        let incoming = pod_with_ports(
            "web-0",
            Some("node-1"),
            vec![port(Some(21017), "TCP", Some("0.0.0.0"))],
        );
        assert!(
            find_host_port_conflict(&incoming, std::slice::from_ref(&existing), "node-1").is_some()
        );
    }

    #[test]
    fn a_pod_does_not_conflict_with_itself() {
        // Same name+namespace already in storage must be skipped.
        let existing = pod_with_ports(
            "web-0",
            Some("node-1"),
            vec![port(Some(21017), "TCP", None)],
        );
        let incoming = pod_with_ports(
            "web-0",
            Some("node-1"),
            vec![port(Some(21017), "TCP", None)],
        );
        assert_eq!(
            find_host_port_conflict(&incoming, std::slice::from_ref(&existing), "node-1"),
            None
        );
    }
}
