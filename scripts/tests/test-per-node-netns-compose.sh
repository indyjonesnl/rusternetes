#!/usr/bin/env bash
# Topology invariants for the multi-node compose stack (#1691).
#
# The [sig-network] HostPort conformance spec dials
# `curl --interface <node InternalIP> http://127.0.0.1:<hostPort>` from a
# hostNetwork pod. That only works if the address the node advertises exists in
# the network namespace pods run in — i.e. the kubelet, its container runtime and
# its kube-proxy must share one netns per node, as upstream co-locates them.
#
# These are the invariants that silently break the fix if someone edits the
# compose file: giving a kubelet its own `networks:` entry again, pointing both
# kubelets at one containerd, sharing a CRI socket volume between nodes, or
# dropping a node's kube-proxy / node-network agent. Each is a config-only
# regression that unit tests cannot see and that only surfaces as a conformance
# failure hours later.
#
# Run with: bash scripts/tests/test-per-node-netns-compose.sh

set -euo pipefail
IFS=$'\n\t'

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
COMPOSE_FILE="$REPO_ROOT/compose.sqlite.yml"

PASS_COUNT=0
FAIL_COUNT=0
FAILED_TESTS=()

fail() {
    echo "    FAIL: $1"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_TESTS+=("${CURRENT_TEST:-unknown}: $1")
}

assert_eq() {
    local expected="$1" actual="$2" label="${3:-assertion}"
    if [ "$expected" = "$actual" ]; then
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        fail "$label: expected '$expected', got '$actual'"
    fi
}

assert_contains() {
    local haystack="$1" needle="$2" label="${3:-assertion}"
    case "$haystack" in
        *"$needle"*) PASS_COUNT=$((PASS_COUNT + 1)) ;;
        *) fail "$label: '$needle' not found" ;;
    esac
}

# Rendered compose config, so the assertions see the same merged YAML the daemon
# does (anchors expanded, env substituted).
render() {
    KUBELET_VOLUMES_PATH=/tmp/kv CERTS_PATH=/tmp/certs \
        docker compose -f "$COMPOSE_FILE" config 2>/dev/null
}

# `yq`-free field read: the service's value for a top-level key inside its block.
service_field() {
    local service="$1" key="$2"
    render | awk -v svc="  $service:" -v key="$key" '
        $0 == svc { in_svc = 1; next }
        in_svc && /^  [a-zA-Z0-9_-]+:$/ { in_svc = 0 }
        in_svc && $1 == key":" { $1 = ""; sub(/^ /, ""); print; exit }
    '
}

service_block() {
    local service="$1"
    render | awk -v svc="  $service:" '
        $0 == svc { in_svc = 1; next }
        in_svc && /^  [a-zA-Z0-9_-]+:$/ { in_svc = 0 }
        in_svc { print }
    '
}

# ----- Tests -----

# Each kubelet must live in ITS OWN runtime's netns. A kubelet with its own
# network namespace advertises an InternalIP that exists nowhere pods can bind.
test_each_kubelet_shares_its_runtimes_netns() {
    assert_eq "service:containerd" "$(service_field kubelet network_mode)" \
        "kubelet must join containerd's netns"
    assert_eq "service:containerd2" "$(service_field kubelet2 network_mode)" \
        "kubelet2 must join containerd2's netns"
}

# A kubelet that also has `networks:` is not sharing a netns — compose rejects
# the combination, but a partial edit (removing network_mode, leaving networks)
# silently restores the old broken topology.
test_kubelets_have_no_own_network_attachment() {
    local block
    block="$(service_block kubelet)"
    case "$block" in
        *"networks:"*) fail "kubelet must not declare its own networks: block" ;;
        *) PASS_COUNT=$((PASS_COUNT + 1)) ;;
    esac
    block="$(service_block kubelet2)"
    case "$block" in
        *"networks:"*) fail "kubelet2 must not declare its own networks: block" ;;
        *) PASS_COUNT=$((PASS_COUNT + 1)) ;;
    esac
}

# One kube-proxy per node, each in that node's netns: the Service and hostPort
# iptables rules have to be written where that node's pods actually are.
test_kube_proxy_per_node() {
    assert_eq "service:containerd" "$(service_field kube-proxy network_mode)" \
        "kube-proxy must be in node-1's netns"
    assert_eq "service:containerd2" "$(service_field kube-proxy2 network_mode)" \
        "kube-proxy2 must be in node-2's netns"
    assert_contains "$(service_block kube-proxy)" "node-1" "kube-proxy serves node-1"
    assert_contains "$(service_block kube-proxy2)" "node-2" "kube-proxy2 serves node-2"
}

# Both kube-proxies must run the node-network agent, else the node never gets a
# CNI config (its podCIDR is never turned into a conflist) and its pods hang in
# ContainerCreating.
test_both_kube_proxies_configure_node_network() {
    assert_contains "$(service_block kube-proxy)" "--configure-node-network" \
        "node-1 kube-proxy runs the node-network agent"
    assert_contains "$(service_block kube-proxy2)" "--configure-node-network" \
        "node-2 kube-proxy runs the node-network agent"
}

# Separate CRI sockets and snapshotter roots. A shared socket volume would put
# both kubelets on one runtime again — the very thing this topology undoes.
test_runtimes_do_not_share_volumes() {
    # `compose config` renders volume sources as their short names.
    local n1 n2
    n1="$(service_block containerd | awk '$1 == "source:" {print $2}' | grep -c '^containerd-run$' || true)"
    n2="$(service_block containerd2 | awk '$1 == "source:" {print $2}' | grep -c '^containerd-run2$' || true)"
    assert_eq "1" "$n1" "node-1 mounts its own CRI socket volume"
    assert_eq "1" "$n2" "node-2 mounts its own CRI socket volume"

    # Exact match, since containerd-run is a prefix of containerd-run2.
    local shared
    shared="$(service_block containerd2 | awk '$1 == "source:" {print $2}' | grep -c '^containerd-run$' || true)"
    assert_eq "0" "$shared" "containerd2 must not mount node-1's CRI socket volume"

    local snap
    snap="$(service_block containerd2 | awk '$1 == "source:" {print $2}' | grep -c '^containerd-data$' || true)"
    assert_eq "0" "$snap" "containerd2 must not share node-1's snapshotter root"
}

# The image's cluster-wide fallback conflist must be dropped on both runtimes:
# with two nodes over one 10.244.0.0/16, host-local hands out the same addresses
# on both, so a pod could come up with an IP another node owns.
test_runtimes_drop_the_fallback_cni_conf() {
    assert_contains "$(service_block containerd)" "CNI_CONF_FROM_NODE_IPAM" \
        "node-1 runtime waits for the per-node conflist"
    assert_contains "$(service_block containerd2)" "CNI_CONF_FROM_NODE_IPAM" \
        "node-2 runtime waits for the per-node conflist"
}

# ----- Runner -----

if ! command -v docker >/dev/null 2>&1; then
    echo "SKIP: docker not available (needed for \`compose config\`)"
    exit 0
fi

for test_fn in $(declare -F | awk '{print $3}' | grep '^test_'); do
    CURRENT_TEST="$test_fn"
    echo "  $test_fn"
    "$test_fn"
done

echo
echo "passed: $PASS_COUNT  failed: $FAIL_COUNT"
if [ "$FAIL_COUNT" -gt 0 ]; then
    printf '%s\n' "${FAILED_TESTS[@]}"
    exit 1
fi
