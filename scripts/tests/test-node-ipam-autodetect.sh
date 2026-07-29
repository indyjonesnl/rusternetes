#!/usr/bin/env bash
# Node-IPAM auto-detection in scripts/bootstrap-cluster.sh.
#
# The multi-node compose stack (compose.sqlite.yml, the one every conformance
# target runs on) gets its CNI config from kube-proxy's node-network agent,
# which turns `node.spec.podCIDR` into a per-node conflist. No podCIDR means no
# conflist, and containerd then fails every single sandbox with
# `failed to setup network for sandbox ...: cni plugin not initialized` — the
# whole nightly conformance set went red this way on 2026-07-29, because node
# IPAM was gated behind an `ALLOCATE_NODE_CIDRS` env var that no bring-up path
# (CI action, cluster-up.sh, the documented manual recipe) ever set.
#
# Upstream does not use a separate opt-in flag: kubeadm turns the allocator on
# whenever the cluster declares a pod subnet
# (cmd/kubeadm/app/phases/controlplane/manifests.go:351 —
# `if cfg.Networking.PodSubnet != "" { SetArgValues(..., "allocate-node-cidrs",
# "true", 1); SetArgValues(..., "cluster-cidr", cfg.Networking.PodSubnet, 1) }`).
# bootstrap-cluster.sh ports that: the stack's pod subnet is the `--pod-cidr` its
# kube-proxy node-network agents run with, and node IPAM follows it.
#
# Run with: bash scripts/tests/test-node-ipam-autodetect.sh

set -euo pipefail
IFS=$'\n\t'

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
BOOTSTRAP="$REPO_ROOT/scripts/bootstrap-cluster.sh"

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

# A stub container runtime whose `ps` lists the given container names and whose
# `inspect -f '{{join .Config.Cmd " "}}' <name>` prints that container's command.
# Keeps the tests hermetic — no docker, no cluster.
make_stub_rt() {
    local dir="$1"
    mkdir -p "$dir/bin"
    cat > "$dir/bin/stub-rt" <<'STUB'
#!/usr/bin/env bash
case "$1" in
    ps)
        printf '%s\n' ${STUB_CONTAINERS:-}
        ;;
    inspect)
        # last arg is the container name
        name="${!#}"
        var="STUB_CMD_${name//[^a-zA-Z0-9]/_}"
        printf '%s\n' "${!var:-}"
        ;;
esac
exit 0
STUB
    chmod +x "$dir/bin/stub-rt"
}

# Source just the two helpers under test out of bootstrap-cluster.sh (the script
# itself runs a whole bootstrap when executed).
run_helper() {
    local helper="$1"
    bash -c "
        set -euo pipefail
        source <(sed -n '/^stack_pod_subnet()/,/^}/p' '$BOOTSTRAP')
        source <(sed -n '/^node_ipam_cluster_cidr()/,/^}/p' '$BOOTSTRAP')
        $helper
    "
}

# ----- Tests -----

# The pod subnet is read off the running kube-proxy node-network agents — the
# stack's own declaration, not a hardcoded constant.
test_pod_subnet_detected_from_kube_proxy() {
    local tmp; tmp="$(mktemp -d)"
    make_stub_rt "$tmp"
    local out
    out="$(CONTAINER_RT="$tmp/bin/stub-rt" \
        STUB_CONTAINERS='rusternetes-kube-proxy rusternetes-kube-proxy2' \
        STUB_CMD_rusternetes_kube_proxy='--node-name node-1 --configure-node-network --pod-cidr 10.244.0.0/16' \
        STUB_CMD_rusternetes_kube_proxy2='--node-name node-2 --configure-node-network --pod-cidr 10.244.0.0/16' \
        run_helper stack_pod_subnet)" || out="<helper-missing>"
    assert_eq "10.244.0.0/16" "$out" "detect --pod-cidr from kube-proxy"
    rm -rf "$tmp"
}

# `--pod-cidr=<cidr>` is the same declaration in the other valid spelling.
test_pod_subnet_detected_with_equals_form() {
    local tmp; tmp="$(mktemp -d)"
    make_stub_rt "$tmp"
    local out
    out="$(CONTAINER_RT="$tmp/bin/stub-rt" \
        STUB_CONTAINERS='rusternetes-kube-proxy' \
        STUB_CMD_rusternetes_kube_proxy='--configure-node-network --pod-cidr=10.42.0.0/16' \
        run_helper stack_pod_subnet)" || out="<helper-missing>"
    assert_eq "10.42.0.0/16" "$out" "detect --pod-cidr=<cidr>"
    rm -rf "$tmp"
}

# THE regression: a stack whose kube-proxy declares a pod subnet gets node IPAM
# with no env var set anywhere. This is what CI (and cluster-up.sh, and the
# documented manual recipe) relies on.
test_auto_enables_when_stack_declares_pod_subnet() {
    local tmp; tmp="$(mktemp -d)"
    make_stub_rt "$tmp"
    local out
    out="$(CONTAINER_RT="$tmp/bin/stub-rt" \
        STUB_CONTAINERS='rusternetes-kube-proxy' \
        STUB_CMD_rusternetes_kube_proxy='--configure-node-network --pod-cidr 10.244.0.0/16' \
        run_helper node_ipam_cluster_cidr)" || out="<helper-missing>"
    assert_eq "10.244.0.0/16" "$out" "auto-enable node IPAM from the stack's pod subnet"
    rm -rf "$tmp"
}

# Stacks that do NOT run the node-network agent (compose.yml/etcd,
# compose.redis.yml, node-conformance) keep node IPAM off, as #1187 intended.
test_auto_stays_off_without_pod_subnet() {
    local tmp; tmp="$(mktemp -d)"
    make_stub_rt "$tmp"
    local out
    out="$(CONTAINER_RT="$tmp/bin/stub-rt" \
        STUB_CONTAINERS='rusternetes-kube-proxy' \
        STUB_CMD_rusternetes_kube_proxy='--node-name node-1 --api-server-url https://api-server:6443' \
        run_helper node_ipam_cluster_cidr)" || out="<helper-missing>"
    assert_eq "" "$out" "no pod subnet declared -> node IPAM stays off"
    rm -rf "$tmp"
}

# Explicit ALLOCATE_NODE_CIDRS=1 (compose.calico.yml's documented recipe) keeps
# working, including its 10.244.0.0/16 default when nothing is detectable.
test_explicit_enable_still_honoured() {
    local tmp; tmp="$(mktemp -d)"
    make_stub_rt "$tmp"
    local out
    out="$(CONTAINER_RT="$tmp/bin/stub-rt" ALLOCATE_NODE_CIDRS=1 \
        STUB_CONTAINERS='' \
        run_helper node_ipam_cluster_cidr)" || out="<helper-missing>"
    assert_eq "10.244.0.0/16" "$out" "explicit enable defaults the cluster CIDR"
    rm -rf "$tmp"
}

# And an explicit opt-out wins over detection, so a stack can still be brought
# up with the allocator off for debugging.
test_explicit_disable_wins_over_detection() {
    local tmp; tmp="$(mktemp -d)"
    make_stub_rt "$tmp"
    local out
    out="$(CONTAINER_RT="$tmp/bin/stub-rt" ALLOCATE_NODE_CIDRS=0 \
        STUB_CONTAINERS='rusternetes-kube-proxy' \
        STUB_CMD_rusternetes_kube_proxy='--configure-node-network --pod-cidr 10.244.0.0/16' \
        run_helper node_ipam_cluster_cidr)" || out="<helper-missing>"
    assert_eq "" "$out" "ALLOCATE_NODE_CIDRS=0 disables node IPAM"
    rm -rf "$tmp"
}

# An explicit CLUSTER_CIDR is the operator's word on the subnet.
test_explicit_cluster_cidr_wins() {
    local tmp; tmp="$(mktemp -d)"
    make_stub_rt "$tmp"
    local out
    out="$(CONTAINER_RT="$tmp/bin/stub-rt" CLUSTER_CIDR=192.168.0.0/16 \
        STUB_CONTAINERS='rusternetes-kube-proxy' \
        STUB_CMD_rusternetes_kube_proxy='--configure-node-network --pod-cidr 10.244.0.0/16' \
        run_helper node_ipam_cluster_cidr)" || out="<helper-missing>"
    assert_eq "192.168.0.0/16" "$out" "explicit CLUSTER_CIDR wins over detection"
    rm -rf "$tmp"
}

# The compose stack the conformance targets run on must actually declare a pod
# subnet — that declaration is what the auto-detection reads.
test_sqlite_stack_declares_a_pod_subnet() {
    local compose="$REPO_ROOT/compose.sqlite.yml"
    local n
    n="$(grep -c -- '--pod-cidr' "$compose" || true)"
    if [ "$n" -ge 2 ]; then
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        fail "compose.sqlite.yml must declare --pod-cidr on both kube-proxies (found $n)"
    fi
}

# ----- Runner -----

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
