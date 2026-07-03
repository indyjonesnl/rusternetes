//! Kubelet hostPort-conflict admission.
//!
//! Before a pod is started the kubelet rejects it if any of its `hostPort`
//! allocations conflict with a pod already running on the same node. Because
//! the conformance path that exercises this (`[sig-apps] StatefulSet ... Should
//! recreate evicted statefulset`) pre-sets `spec.nodeName` — bypassing the
//! scheduler's own `PodFitsHostPorts` check — the kubelet is the only component
//! left to detect the conflict and drive the pod into `Failed`, which the
//! owning StatefulSet controller then delete-and-recreates.
//!
//! The conflict rule mirrors upstream
//! `pkg/scheduler/framework/types.go` (`HostPortInfo.CheckConflict`): two
//! entries conflict only when their port **and** protocol match and their host
//! IPs overlap, where an empty / `0.0.0.0` / `::` host IP is a wildcard that
//! overlaps every address. `hostPort == 0` means "no host port" and is never an
//! allocation (upstream `HostPortInfo.Add` skips it), so two pods that both
//! leave `hostPort` unset must not be treated as conflicting.

use rusternetes_common::resources::pod::Pod;
use rusternetes_common::types::Phase;

/// A single host-port allocation extracted from a container port spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPortEntry {
    pub port: u16,
    pub protocol: String,
    pub host_ip: String,
}

/// A detected conflict: the incoming pod's `port`/`protocol` clashes with an
/// allocation held by `conflicting_pod` (its metadata name).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPortConflict {
    pub port: u16,
    pub protocol: String,
    pub conflicting_pod: String,
}

impl HostPortConflict {
    /// Human-readable rejection message, matching the wording written into
    /// `status.message` when the kubelet fails the pod.
    pub fn message(&self) -> String {
        format!(
            "Pod was rejected: host port {} is already in use",
            self.port
        )
    }
}

/// Every host-port allocation declared by a pod's regular and init containers,
/// skipping unset (`0`) host ports. Upstream `schedutil.GetHostPorts` walks
/// both `Spec.Containers` and `Spec.InitContainers`.
pub fn host_ports(pod: &Pod) -> Vec<HostPortEntry> {
    let Some(spec) = pod.spec.as_ref() else {
        return Vec::new();
    };
    spec.containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
        .flat_map(|c| c.ports.iter().flatten())
        .filter_map(|p| {
            p.host_port.filter(|&hp| hp != 0).map(|hp| HostPortEntry {
                port: hp,
                protocol: p.protocol.clone(),
                host_ip: p.host_ip.clone().unwrap_or_default(),
            })
        })
        .collect()
}

/// True for a host IP that overlaps every address (upstream treats an empty,
/// `0.0.0.0`, or `::` bind address as the "all interfaces" wildcard).
fn is_wildcard(ip: &str) -> bool {
    ip.is_empty() || ip == "0.0.0.0" || ip == "::"
}

/// Whether two host-port allocations conflict (upstream `CheckConflict`): same
/// port and protocol, with overlapping host IPs.
pub fn entries_conflict(a: &HostPortEntry, b: &HostPortEntry) -> bool {
    a.port == b.port
        && a.protocol == b.protocol
        && (is_wildcard(&a.host_ip) || is_wildcard(&b.host_ip) || a.host_ip == b.host_ip)
}

/// Whether `existing` still holds its host-port allocations for the purpose of
/// admitting `incoming` on `node_name`: it must be scheduled to the same node,
/// not be the incoming pod itself, not be terminal (`Failed`/`Succeeded` pods
/// have released their ports), and not be terminating (`deletionTimestamp`).
fn holds_ports_on_node(existing: &Pod, incoming: &Pod, node_name: &str) -> bool {
    let incoming_ns = incoming.metadata.namespace.as_deref().unwrap_or("default");
    let existing_ns = existing.metadata.namespace.as_deref().unwrap_or("default");
    let is_self = existing.metadata.name == incoming.metadata.name && existing_ns == incoming_ns;
    if is_self {
        return false;
    }
    if existing.spec.as_ref().and_then(|s| s.node_name.as_deref()) != Some(node_name) {
        return false;
    }
    if matches!(
        existing.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(Phase::Failed) | Some(Phase::Succeeded)
    ) {
        return false;
    }
    if existing.metadata.deletion_timestamp.is_some() {
        return false;
    }
    true
}

/// The first host-port conflict between `pod` and the pods already active on
/// `node_name`, or `None` if `pod` can be admitted. `pods_on_node` may contain
/// pods from any node/namespace/phase — this function applies the same
/// eligibility filter the kubelet admission handler uses.
pub fn find_host_port_conflict(
    pod: &Pod,
    pods_on_node: &[Pod],
    node_name: &str,
) -> Option<HostPortConflict> {
    let incoming = host_ports(pod);
    if incoming.is_empty() {
        return None;
    }
    for existing in pods_on_node {
        if !holds_ports_on_node(existing, pod, node_name) {
            continue;
        }
        for held in host_ports(existing) {
            if let Some(want) = incoming.iter().find(|e| entries_conflict(e, &held)) {
                return Some(HostPortConflict {
                    port: want.port,
                    protocol: want.protocol.clone(),
                    conflicting_pod: existing.metadata.name.clone(),
                });
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::pod::{Container, ContainerPort, PodSpec, PodStatus};

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
