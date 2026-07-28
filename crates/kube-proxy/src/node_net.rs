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
/// stays `isGateway` (pods route through it) but `ipMasq` is **off**: its
/// masquerade rule would SNAT cross-node pod traffic before our own chain could
/// spare it (a RETURN only continues POSTROUTING traversal). Masquerading is the
/// agent's job — see [`no_masq_rules`] and [`postrouting_hook_args`].
///
/// kindnetd ref: `cni.go:90` — `"ipMasq": false`.
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
      "ipMasq": false,
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

// ---------------------------------------------------------------------------
// Applying the computed state
// ---------------------------------------------------------------------------

use std::process::Command;
use tracing::{debug, info, warn};

/// What the node-network agent needs to know about itself.
#[derive(Debug, Clone)]
pub struct NodeNetConfig {
    /// This node's name, matched against `metadata.name` when picking our Node.
    pub node_name: String,
    /// Where to write the CNI conflist (the dir containerd watches).
    pub cni_conf_path: String,
    /// CIDRs whose traffic must not be masqueraded — the cluster pod CIDR and
    /// the Service CIDR.
    pub no_masq_cidrs: Vec<String>,
}

/// Chain holding the node's masquerade policy, hooked into POSTROUTING.
pub const MASQ_CHAIN: &str = "RUSTERNETES-MASQ";

/// Write `conflist` to `path` when it differs from what is already there.
/// Returns whether a write happened.
///
/// Rewriting an identical file would make containerd's CNI fsnotify watcher
/// reload the network config on every sync tick.
/// kindnetd ref: `cni.go:123` — "CNIConfigWriter no-ops re-writing config with
/// the same inputs".
pub fn write_conflist_if_changed(path: &str, conflist: &str) -> std::io::Result<bool> {
    if let Ok(existing) = std::fs::read_to_string(path) {
        if existing == conflist {
            return Ok(false);
        }
    }
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, conflist)?;
    Ok(true)
}

/// Install `route` with `ip route replace`, which is idempotent and also
/// corrects a route to the same destination that points at a stale gateway
/// (kindnetd deletes-then-adds for the same reason — `routes.go:47-60`).
fn apply_route(route: &Route) {
    let out = Command::new("ip")
        .args(["route", "replace", &route.dst, "via", &route.gw])
        .output();
    match out {
        Ok(o) if o.status.success() => {
            debug!("ensured route {} via {}", route.dst, route.gw);
        }
        Ok(o) => warn!(
            "failed to add route {} via {}: {}",
            route.dst,
            route.gw,
            String::from_utf8_lossy(&o.stderr).trim()
        ),
        Err(e) => warn!("failed to exec ip route for {}: {}", route.dst, e),
    }
}

/// Rebuild the masquerade chain so cluster-internal traffic keeps its source IP.
/// Flushed and refilled each sync so a changed CIDR list converges.
fn sync_masq_chain(cfg: &NodeNetConfig) {
    let cidrs: Vec<&str> = cfg.no_masq_cidrs.iter().map(String::as_str).collect();
    let _ = Command::new("iptables")
        .args(["-t", "nat", "-N", MASQ_CHAIN])
        .output();
    let _ = Command::new("iptables")
        .args(["-t", "nat", "-F", MASQ_CHAIN])
        .output();

    for rule in no_masq_rules(&cidrs) {
        // Each rule body is a shell-free argv already; split on spaces except
        // inside the quoted comment.
        let args = split_rule(&rule);
        let mut argv: Vec<&str> = vec!["-t", "nat", "-A", MASQ_CHAIN];
        argv.extend(args.iter().map(String::as_str));
        if let Ok(o) = Command::new("iptables").args(&argv).output() {
            if !o.status.success() {
                warn!(
                    "failed to add masq rule {:?}: {}",
                    rule,
                    String::from_utf8_lossy(&o.stderr).trim()
                );
            }
        }
    }

    // Hook the chain into POSTROUTING for non-LOCAL destinations only, so
    // traffic to the node's own addresses is never SNAT'd.
    // kindnetd ref: `masq.go:113`.
    let hook = postrouting_hook_args();
    let hook_refs: Vec<&str> = hook.iter().map(String::as_str).collect();
    let mut check: Vec<&str> = vec!["-t", "nat", "-C", "POSTROUTING"];
    check.extend(hook_refs.iter().copied());
    let present = Command::new("iptables")
        .args(&check)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !present {
        let mut add: Vec<&str> = vec!["-t", "nat", "-A", "POSTROUTING"];
        add.extend(hook_refs.iter().copied());
        let _ = Command::new("iptables").args(&add).output();
    }
}

/// POSTROUTING hook for the masquerade chain: match everything whose
/// destination is not one of the node's own addresses.
///
/// kindnetd ref: `masq.go:113` —
/// `-m addrtype ! --dst-type LOCAL -j KIND-MASQ-AGENT`.
pub fn postrouting_hook_args() -> Vec<String> {
    [
        "-m",
        "addrtype",
        "!",
        "--dst-type",
        "LOCAL",
        "-j",
        MASQ_CHAIN,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Split a rule body into argv, keeping a `"quoted comment"` as one argument.
fn split_rule(rule: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for c in rule.chars() {
        match c {
            '"' => in_quotes = !in_quotes,
            ' ' if !in_quotes => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// One reconcile pass over `nodes`: write our CNI config, install host-gw routes
/// to the other nodes' pod CIDRs, and refresh the masquerade chain.
///
/// A node with no pod CIDR yet is a no-op, not an error: kube-controller-manager
/// assigns CIDRs asynchronously, and writing a guessed conflist would hand pods
/// addresses from a subnet no other node routes to.
///
/// kindnetd ref: `main.go:313` `makeNodesReconciler`.
pub fn reconcile_node_network(nodes: &[Node], cfg: &NodeNetConfig) {
    let Some(self_node) = nodes.iter().find(|n| n.metadata.name == cfg.node_name) else {
        debug!(
            "node {} not registered yet; skipping node-network sync",
            cfg.node_name
        );
        return;
    };

    match pod_cidr_for(self_node) {
        Some(pod_cidr) => {
            match write_conflist_if_changed(&cfg.cni_conf_path, &cni_conflist(pod_cidr)) {
                Ok(true) => info!(
                    "wrote CNI config for pod CIDR {} to {}",
                    pod_cidr, cfg.cni_conf_path
                ),
                Ok(false) => {}
                Err(e) => warn!("failed to write CNI config {}: {}", cfg.cni_conf_path, e),
            }
        }
        None => {
            info!(
                "node {} has no podCIDR yet (is --allocate-node-cidrs set on the controller-manager?); \
                 pods stay pending until it is assigned",
                cfg.node_name
            );
        }
    }

    for route in desired_routes(nodes, &cfg.node_name) {
        apply_route(&route);
    }

    sync_masq_chain(cfg);
}
