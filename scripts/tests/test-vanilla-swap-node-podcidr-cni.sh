#!/usr/bin/env bash
# Regression test for #1648: the vanilla-swap join-worker (kubelet) leg must
# drive the rusternetes node's CNI from the node's node-ipam spec.podCIDR, not
# the CRI image's baked cluster-wide 10.244.0.0/16 conflist.
#
# Mechanism ported from this repo's compose stack (#1691, compose.sqlite.yml):
#   * containerd runs with CNI_CONF_FROM_NODE_IPAM=1 (entrypoint drops the baked
#     conflist) and /etc/cni/net.d on a volume shared with the agent;
#   * kube-proxy `--configure-node-network` runs in containerd's netns and writes
#     the conflist from node.spec.podCIDR (kindnetd cni.go:41 analogue).
# Run with: bash scripts/tests/test-vanilla-swap-node-podcidr-cni.sh
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
VS_LIB_ONLY=1 . "$REPO_ROOT/scripts/vanilla-swap-common.sh"

fails=0
ok()  { echo "ok   - $1"; }
bad() { echo "FAIL - $1" >&2; fails=$((fails+1)); }

has_fn() { declare -F "$1" >/dev/null; }

if has_fn vs_cri_cni_docker_args; then
  out="$(vs_cri_cni_docker_args c1)"
  grep -qx 'CNI_CONF_FROM_NODE_IPAM=1' <<<"$out" && ok "containerd gets CNI_CONF_FROM_NODE_IPAM=1" || bad "containerd missing CNI_CONF_FROM_NODE_IPAM=1"
  grep -qx 'vanilla-swap-c1-cni-conf:/etc/cni/net.d' <<<"$out" && ok "containerd mounts shared cni-conf volume" || bad "containerd missing shared cni-conf volume"
else
  bad "vs_cri_cni_docker_args is not defined"
fi

if has_fn vs_node_net_agent_docker_args; then
  out="$(vs_node_net_agent_docker_args c1 node-w1 reg/kube-proxy:t)"
  grep -qx -- '--configure-node-network' <<<"$out" && ok "agent runs --configure-node-network" || bad "agent missing --configure-node-network"
  grep -qx 'container:vanilla-swap-c1-containerd' <<<"$out" && ok "agent shares containerd's netns" || bad "agent not in containerd's netns"
  grep -qx 'vanilla-swap-c1-cni-conf:/etc/cni/net.d' <<<"$out" && ok "agent writes to shared cni-conf volume" || bad "agent missing cni-conf volume"
  grep -qx 'node-w1' <<<"$out" && ok "agent bound to the swapped node" || bad "agent missing node name"
else
  bad "vs_node_net_agent_docker_args is not defined"
fi

# The agent image is infrastructure (like */containerd), not a module under test.
if grep -qF "grep -v -e '/containerd' -e '/kube-proxy'" "$REPO_ROOT/scripts/vanilla-swap-common.sh"; then
  ok "vs_guard_cluster excludes the node-net agent image"
else
  bad "vs_guard_cluster must exclude the node-net agent image"
fi

[ "$fails" -eq 0 ] && { echo PASS; exit 0; } || { echo "FAIL: $fails"; exit 1; }
