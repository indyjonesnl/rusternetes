#!/usr/bin/env bash
# CSI attacher opt-in for the all-in-one compose stacks (#3746, rest of #3133).
#
# The all-in-one binary runs the controller-manager in-process, and with
# `--csi-attacher DRIVER=SOCKET` it dials that driver socket from inside the
# `rusternetes` container. So the socket's directory has to be bind-mounted into
# the container at the same path, read-write. This is how kubeadm makes a host
# plugin-socket dir reachable to the kube-controller-manager: the flexvolume dir
# is a read-write DirectoryOrCreate hostPath "as it is used for third-party
# plugins to integrate with their storage backends via unix domain socket"
# (cmd/kubeadm/app/phases/controlplane/volumes.go:73-79). The multi-container
# stack does the same through bootstrap-cluster.sh (#3133).
#
# Both all-in-one compose files ship the attacher flags AND the socket-dir bind
# mount as one commented opt-in block (the command list cannot drop items on an
# unset env var). The commented lines are the YAML list items prefixed with
# `#   ` — uncommenting them is exactly what an operator does, so this test does
# that and renders the result with `docker compose config`, asserting:
#   - default render: no CSI flags, no socket-dir mount;
#   - opted-in render: the flags are present, and the socket's directory is
#     bind-mounted at the same path, read-write.
#
# Run with: bash scripts/tests/test-all-in-one-csi-socket-mount.sh

set -euo pipefail
IFS=$'\n\t'

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
COMPOSE_FILES=(compose.all-in-one.yml compose.all-in-one-redis.yml)

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

# The operator's opt-in: drop the `#   ` in front of each commented list item.
opt_in() {
    sed -E 's/^([[:space:]]*)#   - /\1- /' "$1"
}

# Render a compose file (a path) the way the daemon sees it.
render() {
    local dir; dir="$(dirname "$1")"
    KUBELET_VOLUMES_PATH=/tmp/kv docker compose \
        --project-directory "$dir" -f "$1" config --format json 2>/dev/null
}

# One fact per line about the `rusternetes` service: the CSI flags in its
# command, and every bind mount as source:target:read_only.
service_facts() {
    python3 -c '
import json, os, sys
svc = json.load(sys.stdin)["services"]["rusternetes"]
cmd = svc.get("command") or []
for i, a in enumerate(cmd):
    if a == "--csi-attacher":
        print("attacher=" + cmd[i + 1])
    elif a == "--attach-detach-controller":
        print("adc")
for v in svc.get("volumes") or []:
    if v.get("type") == "bind":
        print("bind=%s:%s:%s" % (v["source"], v["target"], bool(v.get("read_only"))))
'
}

check_default() {
    local f="$1" facts
    facts="$(render "$REPO_ROOT/$f" | service_facts)" || facts="<render-failed>"
    assert_eq "" "$(printf '%s\n' "$facts" | grep -E '^(attacher=|adc$)' || true)" \
        "$f: default command carries no CSI attach flags"
    assert_eq "" "$(printf '%s\n' "$facts" | grep '^bind=/var/lib/kubelet/plugins' || true)" \
        "$f: default has no CSI socket-dir mount"
}

check_opt_in() {
    local f="$1" tmp facts attacher sock dir
    tmp="$(mktemp -d)"
    opt_in "$REPO_ROOT/$f" > "$tmp/$f"
    facts="$(render "$tmp/$f" | service_facts)" || facts="<render-failed>"
    rm -rf "$tmp"
    attacher="$(printf '%s\n' "$facts" | grep '^attacher=' | head -n1 || true)"
    attacher="${attacher#attacher=}"
    if [ -z "$attacher" ]; then
        fail "$f: opt-in block has no --csi-attacher (got: $facts)"
        return
    fi
    PASS_COUNT=$((PASS_COUNT + 1))
    assert_eq "adc" "$(printf '%s\n' "$facts" | grep -x 'adc' || true)" \
        "$f: opt-in enables --attach-detach-controller"
    # Strip the scheme the way the client does (crates/csi/src/lib.rs
    # normalize_endpoint) and take the socket's directory.
    sock="${attacher#*=}"
    sock="${sock#unix://}"
    sock="${sock#unix:}"
    dir="$(dirname -- "$sock")"
    assert_eq "bind=$dir:$dir:False" \
        "$(printf '%s\n' "$facts" | grep "^bind=$dir:" || true)" \
        "$f: socket dir bind-mounted at the same path, read-write"
}

# ----- Tests -----

test_default_has_no_csi() {
    local f
    for f in "${COMPOSE_FILES[@]}"; do check_default "$f"; done
}

test_opt_in_mounts_socket_dir() {
    local f
    for f in "${COMPOSE_FILES[@]}"; do check_opt_in "$f"; done
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
