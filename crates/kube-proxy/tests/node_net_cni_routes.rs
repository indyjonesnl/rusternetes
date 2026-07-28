//! Per-node pod networking: CNI config + host-gw routes + no-masquerade rules.
//!
//! Once each node owns its own network namespace, a single hardcoded pod CIDR
//! no longer works: two nodes running `host-local` IPAM over the same
//! `10.244.0.0/16` both start allocating at `10.244.0.2`, so pods on different
//! nodes get identical IPs. Upstream splits the cluster CIDR per node
//! (kube-controller-manager node-ipam → `node.spec.podCIDR`) and has a node
//! agent turn that into (a) the node's CNI config and (b) host-gw routes to the
//! other nodes' pod CIDRs.
//!
//! Reference implementation is kindnetd (kubernetes-sigs/kind,
//! `images/kindnetd/cmd/kindnetd/`):
//!
//! * `main.go:313` `makeNodesReconciler` — for the node whose IP is ours, write
//!   the CNI config; for every other node, add routes to its pod CIDRs.
//! * `cni.go:41` `ComputeCNIConfigInputs` — templates the conflist from
//!   `node.Spec.PodCIDRs` (falling back to the legacy `node.Spec.PodCIDR`).
//! * `routes.go:27` `syncRoute` — `netlink.Route{Dst: podCIDR, Gw: nodeIP}`,
//!   deleting any route to the same dst with a different gateway.
//! * `masq.go:105` — one `-d <cidr> -j RETURN` per no-masquerade CIDR, then a
//!   final `-j MASQUERADE` ("must be last in chain").

use rusternetes_common::resources::Node;
use rusternetes_kube_proxy::node_net::{cni_conflist, desired_routes, no_masq_rules, Route};

/// Build a Node from its API JSON — the same shape kube-proxy decodes off the
/// wire, so the test exercises the real `podCIDR` / `addresses` field names.
fn node(name: &str, internal_ip: Option<&str>, pod_cidr: Option<&str>) -> Node {
    let mut spec = serde_json::Map::new();
    if let Some(cidr) = pod_cidr {
        spec.insert("podCIDR".into(), cidr.into());
    }
    let mut addresses = Vec::new();
    if let Some(ip) = internal_ip {
        addresses.push(serde_json::json!({"type": "InternalIP", "address": ip}));
    }
    addresses.push(serde_json::json!({"type": "Hostname", "address": name}));

    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": name},
        "spec": spec,
        "status": {"addresses": addresses},
    }))
    .expect("node fixture must decode")
}

/// The conflist handed to containerd must carry THIS node's pod CIDR as the
/// host-local range, not a cluster-wide default.
///
/// kindnetd ref: `cni.go:41` ComputeCNIConfigInputs + the conflist template's
/// `{{- range $cidr := .PodCIDRs}}` ranges block.
#[test]
fn cni_conflist_uses_this_nodes_pod_cidr() {
    let conf = cni_conflist("10.244.1.0/24");
    let parsed: serde_json::Value =
        serde_json::from_str(&conf).expect("conflist must be valid JSON");

    let subnet = parsed["plugins"][0]["ipam"]["ranges"][0][0]["subnet"]
        .as_str()
        .expect("bridge plugin must declare an ipam range subnet");
    assert_eq!(
        subnet, "10.244.1.0/24",
        "conflist must use the node's pod CIDR: {conf}"
    );
}

/// hostPort still goes through the portmap plugin, and the bridge must keep
/// acting as the pods' gateway — regenerating the conflist per node must not
/// silently drop either.
#[test]
fn cni_conflist_keeps_bridge_gateway_and_portmap() {
    let conf = cni_conflist("10.244.0.0/24");
    let parsed: serde_json::Value = serde_json::from_str(&conf).unwrap();

    assert_eq!(parsed["plugins"][0]["type"], "bridge");
    assert_eq!(
        parsed["plugins"][0]["isGateway"], true,
        "bridge must be the pod gateway: {conf}"
    );
    let has_portmap = parsed["plugins"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["type"] == "portmap" && p["capabilities"]["portMappings"] == true);
    assert!(
        has_portmap,
        "portmap capability required for hostPort: {conf}"
    );
}

/// One host-gw route per *other* node: dst = that node's pod CIDR, gw = that
/// node's InternalIP. Our own node is skipped (its pods are on the local
/// bridge), and a node the allocator has not reached yet contributes nothing.
///
/// kindnetd ref: `main.go:322` (`if nodeIPs.Has(hostIP) { ...write cni...; return }`),
/// `main.go:343` (`if len(podCIDRs) == 0 { ...ignoring... }`), `routes.go:37`
/// (`netlink.Route{Dst: dst, Gw: ip}`).
#[test]
fn desired_routes_skips_self_and_nodes_without_cidr_or_ip() {
    let nodes = vec![
        node("node-1", Some("172.28.0.3"), Some("10.244.0.0/24")), // self
        node("node-2", Some("172.28.0.4"), Some("10.244.1.0/24")), // peer
        node("node-3", Some("172.28.0.7"), None),                  // no CIDR yet
        node("node-4", None, Some("10.244.3.0/24")),               // no InternalIP
    ];

    let routes = desired_routes(&nodes, "node-1");

    assert_eq!(
        routes,
        vec![Route {
            dst: "10.244.1.0/24".to_string(),
            gw: "172.28.0.4".to_string(),
        }],
        "exactly one route, to node-2's pod CIDR via node-2's InternalIP"
    );
}

/// A node with no pod CIDR must not produce a route to a *guessed* CIDR — an
/// invented route would black-hole that node's pod traffic once the allocator
/// assigns a different subnet.
#[test]
fn desired_routes_is_empty_before_ipam_assigns() {
    let nodes = vec![
        node("node-1", Some("172.28.0.3"), None),
        node("node-2", Some("172.28.0.4"), None),
    ];
    assert!(desired_routes(&nodes, "node-1").is_empty());
}

/// IPv6 pod CIDRs are not routable in this single-stack v4 setup; emitting a v6
/// dst with a v4 gateway would fail the route add and abort the sync.
#[test]
fn desired_routes_ignores_ipv6_pod_cidrs() {
    let nodes = vec![
        node("node-1", Some("172.28.0.3"), Some("10.244.0.0/24")),
        node("node-2", Some("172.28.0.4"), Some("fd00:10:244:1::/64")),
    ];
    assert!(
        desired_routes(&nodes, "node-1").is_empty(),
        "v6 peer CIDR must be skipped in a v4 stack"
    );
}

/// Cross-node pod traffic must keep its real source IP: the bridge plugin's
/// `ipMasq` only spares its own /24, so without an explicit exemption a pod on
/// node-1 reaching a pod on node-2 arrives SNAT'd to the node IP.
///
/// kindnetd ref: `masq.go:105-112` — `-d <cidr> -j RETURN` per no-masquerade
/// CIDR, then `-j MASQUERADE` "(must be last in chain)".
#[test]
fn no_masq_rules_return_cluster_traffic_before_masquerading() {
    let rules = no_masq_rules(&["10.244.0.0/16", "10.96.0.0/12"]);

    let return_idx: Vec<usize> = rules
        .iter()
        .enumerate()
        .filter(|(_, r)| r.contains("-j RETURN"))
        .map(|(i, _)| i)
        .collect();
    let masq_idx = rules
        .iter()
        .position(|r| r.contains("-j MASQUERADE"))
        .expect("chain must end in MASQUERADE");

    assert_eq!(
        return_idx.len(),
        2,
        "one RETURN per no-masq CIDR: {rules:?}"
    );
    assert!(
        return_idx.iter().all(|i| *i < masq_idx),
        "every RETURN must precede the MASQUERADE: {rules:?}"
    );
    assert_eq!(
        masq_idx,
        rules.len() - 1,
        "MASQUERADE must be last in chain: {rules:?}"
    );
    assert!(
        rules.iter().any(|r| r.contains("-d 10.244.0.0/16")),
        "pod CIDR must be exempt from masquerade: {rules:?}"
    );
}

/// Rewriting the same conflist must be a no-op: containerd watches the CNI conf
/// dir with fsnotify and reloads its network config on every write, so a sync
/// loop that rewrites unconditionally would churn the runtime's CNI state every
/// tick.
///
/// kindnetd ref: `cni.go:123` — "CNIConfigWriter no-ops re-writing config with
/// the same inputs".
#[test]
fn conflist_write_is_a_noop_when_unchanged() {
    let dir = std::env::temp_dir().join(format!("rn-conflist-{}", std::process::id()));
    let path = dir.join("10-rusternetes.conflist");
    let path_str = path.to_string_lossy().to_string();
    let conf = cni_conflist("10.244.2.0/24");

    // First write creates it (including the parent dir).
    assert!(
        rusternetes_kube_proxy::node_net::write_conflist_if_changed(&path_str, &conf).unwrap(),
        "first write must happen"
    );
    // Identical content -> no write.
    assert!(
        !rusternetes_kube_proxy::node_net::write_conflist_if_changed(&path_str, &conf).unwrap(),
        "identical conflist must not be rewritten"
    );
    // A reassigned pod CIDR -> write.
    assert!(
        rusternetes_kube_proxy::node_net::write_conflist_if_changed(
            &path_str,
            &cni_conflist("10.244.3.0/24")
        )
        .unwrap(),
        "changed pod CIDR must be written"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The bridge plugin must NOT masquerade: its own `ipMasq` rule sits in
/// POSTROUTING and SNATs anything leaving the node, including a pod on node-1
/// talking to a pod on node-2 — a `RETURN` in our own chain cannot undo that,
/// because RETURN just continues POSTROUTING traversal into the bridge's rule.
///
/// Verified live: with `ipMasq: true`, `curl .../clientip` across nodes reported
/// the node IP (172.28.0.4) instead of the pod IP.
///
/// kindnetd ref: `cni.go:90` — `"ipMasq": false` in the conflist template; the
/// agent owns masquerading (`masq.go`).
#[test]
fn cni_conflist_leaves_masquerade_to_the_agent() {
    let parsed: serde_json::Value = serde_json::from_str(&cni_conflist("10.244.1.0/24")).unwrap();
    assert_eq!(
        parsed["plugins"][0]["ipMasq"], false,
        "bridge ipMasq must be off; the node-network agent masquerades instead"
    );
}

/// The masq chain must be reached only for non-LOCAL destinations, so traffic to
/// the node's own addresses is never SNAT'd.
///
/// kindnetd ref: `masq.go:113` — POSTROUTING gets
/// `-m addrtype ! --dst-type LOCAL -j KIND-MASQ-AGENT`.
#[test]
fn masq_chain_is_hooked_for_non_local_destinations_only() {
    let args = rusternetes_kube_proxy::node_net::postrouting_hook_args();
    let joined = args.join(" ");
    assert!(
        joined.contains("addrtype") && joined.contains("! --dst-type LOCAL"),
        "hook must exclude LOCAL destinations: {joined}"
    );
    assert!(
        joined.contains(rusternetes_kube_proxy::node_net::MASQ_CHAIN),
        "hook must jump to the masq chain: {joined}"
    );
}
