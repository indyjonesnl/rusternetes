//! Per-node pod networking: CNI config, host-gw routes, no-masquerade rules.
//!
//! With one network namespace per node, the node's pod CIDR is no longer a
//! cluster-wide constant: kube-controller-manager's node-ipam splits
//! `--cluster-cidr` into a per-node subnet and publishes it as
//! `node.spec.podCIDR`. This module turns that assignment into the three things
//! a node needs:
//!
//! 1. a CNI conflist whose `host-local` range is *this* node's pod CIDR, so two
//!    nodes never hand out the same pod IP;
//! 2. host-gw routes to every other node's pod CIDR via that node's InternalIP;
//! 3. a masquerade chain that exempts cluster traffic, so a pod's source IP
//!    survives a cross-node hop.
//!
//! This mirrors kindnetd (kubernetes-sigs/kind, `images/kindnetd/cmd/kindnetd/`),
//! which is the reference "just enough CNI plumbing" node agent:
//!
//! * `main.go:313` `makeNodesReconciler` — write the CNI config for the node
//!   whose IP is ours, add routes for all others.
//! * `cni.go:41` `ComputeCNIConfigInputs` — conflist templated from
//!   `node.Spec.PodCIDRs`, falling back to the legacy `node.Spec.PodCIDR`.
//! * `routes.go:27` `syncRoute` — `netlink.Route{Dst: podCIDR, Gw: nodeIP}`.
//! * `masq.go:105` — `-d <cidr> -j RETURN` per no-masquerade CIDR, then a final
//!   `-j MASQUERADE` ("must be last in chain").
//!
//! Packaging note: upstream ships this as its own DaemonSet. Here it rides in
//! kube-proxy, which already runs one instance per node inside that node's
//! network namespace with an api-server client — no new image or netns plumbing.
//! The CNI artifacts it writes are plain spec-compliant conflists, so any
//! compliant plugin chain still applies.

use rusternetes_common::resources::Node;

/// A host-gw route to another node's pod CIDR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// Destination pod CIDR, e.g. `10.244.1.0/24`.
    pub dst: String,
    /// Gateway — the owning node's InternalIP.
    pub gw: String,
}

/// This node's CNI conflist, with `pod_cidr` as the host-local range.
///
/// Keeps the plugin chain of the committed `deploy/containerd/cni` config
/// (bridge → portmap → firewall); only the IPAM range is per-node. The bridge
/// stays `isGateway` (pods route through it) and `ipMasq` stays on so pod egress
/// to the outside world still works — cluster-internal traffic is exempted by
/// [`no_masq_rules`] instead.
pub fn cni_conflist(pod_cidr: &str) -> String {
    format!(
        r#"{{
  "cniVersion": "1.0.0",
  "name": "rusternetes",
  "plugins": [
    {{
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "ipMasq": true,
      "hairpinMode": true,
      "ipam": {{
        "type": "host-local",
        "ranges": [[{{"subnet": "{pod_cidr}"}}]],
        "routes": [{{"dst": "0.0.0.0/0"}}]
      }}
    }},
    {{
      "type": "portmap",
      "capabilities": {{"portMappings": true}},
      "snat": true
    }},
    {{
      "type": "firewall"
    }}
  ]
}}
"#
    )
}

/// The pod CIDR to program for `node`: the first IPv4 entry of `podCIDRs`, else
/// the legacy `podCIDR`. `None` when the allocator has not assigned one (or it
/// is IPv6-only — this stack is single-stack v4, and a v6 dst with a v4 gateway
/// would fail the route add).
///
/// kindnetd ref: `main.go:337-346` and `cni.go:45-53`.
pub fn pod_cidr_for(node: &Node) -> Option<&str> {
    let spec = node.spec.as_ref()?;
    spec.pod_cidrs
        .as_ref()
        .and_then(|cidrs| cidrs.iter().find(|c| is_ipv4_cidr(c)))
        .map(String::as_str)
        .or_else(|| spec.pod_cidr.as_deref().filter(|c| is_ipv4_cidr(c)))
}

/// `node`'s InternalIP, if it has reported one.
pub fn internal_ip_for(node: &Node) -> Option<&str> {
    node.status
        .as_ref()?
        .addresses
        .as_ref()?
        .iter()
        .find(|a| a.address_type == "InternalIP" && !a.address.is_empty())
        .map(|a| a.address.as_str())
}

/// Host-gw routes this node needs: one per *other* node that has both a pod
/// CIDR and an InternalIP. Our own node is skipped — its pods sit on the local
/// bridge — and a node the allocator has not reached yet is skipped rather than
/// guessed at, since a wrong route would black-hole that node's pods.
///
/// kindnetd ref: `main.go:322` (skip self after writing the CNI config),
/// `main.go:343` (skip nodes with no CIDR), `routes.go:37` (dst + gw).
pub fn desired_routes(nodes: &[Node], self_node_name: &str) -> Vec<Route> {
    nodes
        .iter()
        .filter(|n| n.metadata.name != self_node_name)
        .filter_map(|n| {
            Some(Route {
                dst: pod_cidr_for(n)?.to_string(),
                gw: internal_ip_for(n)?.to_string(),
            })
        })
        .collect()
}

/// Rule bodies for the node's masquerade chain: a `-j RETURN` for each CIDR that
/// must keep its source IP, then a final `-j MASQUERADE` for everything else.
///
/// The bridge plugin's own `ipMasq` rule only exempts the local /24, so without
/// this a pod on node-1 reaching a pod on node-2 arrives SNAT'd to node-1's IP.
///
/// kindnetd ref: `masq.go:105-112`.
pub fn no_masq_rules(no_masq_cidrs: &[&str]) -> Vec<String> {
    let mut rules: Vec<String> = no_masq_cidrs
        .iter()
        .map(|cidr| {
            format!(
                "-d {cidr} -j RETURN -m comment --comment \"rusternetes: cluster traffic is not masqueraded\""
            )
        })
        .collect();
    rules.push(
        "-j MASQUERADE -m comment --comment \"rusternetes: outbound traffic is masqueraded (must be last)\""
            .to_string(),
    );
    rules
}

/// True for an IPv4 CIDR string. A `:` can only appear in an IPv6 literal.
fn is_ipv4_cidr(cidr: &str) -> bool {
    !cidr.is_empty() && !cidr.contains(':')
}
