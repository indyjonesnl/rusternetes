#!/usr/bin/env bash
# Unit tests for the ClusterIP substrate-gate diagnostics dump.
#
# When the gate failed, the harness printed only the EndpointSlice list and the
# kube-system pod list. Three consecutive api-server-leg investigations then had
# to *guess* why a Running kube-proxy had not programmed the ClusterIP, because
# the one artefact that answers it — kube-proxy's own log — was never captured.
# The 2026-09-09 run is the clearest case: kube-proxy reached 1/1 Running on
# both nodes and the ClusterIP still refused, with nothing in the log to say
# why, and two earlier kube-proxy pods left in Failed with their logs
# unretrieved.
#
# So the dump must, at minimum:
#   - fetch logs for EVERY kube-proxy pod, not just a Running one;
#   - fetch --previous logs too, since a Failed pod's useful output is there;
#   - show the iptables state for the ClusterIP from the probe node, which is
#     the actual assertion being made ("no rules exist for 10.96.0.1");
#   - never abort the run when a command fails — this is a diagnostic path
#     reached only when something is already broken.
#
# Run with: bash scripts/tests/test-vanilla-swap-clusterip-diagnostics.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
VS_LIB_ONLY=1 . "$REPO_ROOT/scripts/vanilla-swap-common.sh"

PASS=0; FAIL=0
ok()  { PASS=$((PASS + 1)); echo "  ok   - $1"; }
bad() { FAIL=$((FAIL + 1)); echo "  FAIL - $1" >&2; }
contains() {
    if printf '%s' "$2" | grep -qF -- "$3"; then ok "$1"; else
        bad "$1"; printf '    expected to contain: %q\n' "$3" >&2
    fi
}
lacks() {
    if printf '%s' "$2" | grep -qF -- "$3"; then
        bad "$1"; printf '    expected NOT to contain: %q\n' "$3" >&2
    else ok "$1"; fi
}

# --- stubs -----------------------------------------------------------------
# Record every command the dump issues, so the assertions are about behaviour
# (what it collected) rather than about formatting.
STUB_LOG="$(mktemp)"
trap 'rm -f "$STUB_LOG"' EXIT

kubectl() {
    printf 'kubectl %s\n' "$*" >>"$STUB_LOG"
    case "$*" in
        *"get pods"*"-o name"*) printf 'pod/kube-proxy-aaa\npod/kube-proxy-bbb\n' ;;
        *logs*)                 printf 'stub log line\n' ;;
        *)                      printf 'stub output\n' ;;
    esac
}
docker() {
    printf 'docker %s\n' "$*" >>"$STUB_LOG"
    # Simulate the real finding: no rules at all for the ClusterIP.
    return 1
}
export -f kubectl docker 2>/dev/null || true

echo "vs_dump_clusterip_diagnostics"

OUT="$(vs_dump_clusterip_diagnostics /tmp/nonexistent.kubeconfig probe-node 10.96.0.1 443 2>&1 || true)"
CMDS="$(cat "$STUB_LOG")"

contains "enumerates kube-proxy pods rather than assuming one" \
    "$CMDS" "get pods"
contains "fetches logs for the first kube-proxy pod" \
    "$CMDS" "logs kube-proxy-aaa"
contains "fetches logs for the SECOND kube-proxy pod too" \
    "$CMDS" "logs kube-proxy-bbb"
contains "fetches --previous logs, where a Failed pod's output lives" \
    "$CMDS" "--previous"
contains "dumps the iptables state for the ClusterIP being probed" \
    "$CMDS" "10.96.0.1"
contains "reads the Service the ClusterIP belongs to" \
    "$CMDS" "kubernetes"
contains "reads the EndpointSlice backing it" \
    "$CMDS" "endpointslice"

# A diagnostic path must not itself end the run.
lacks "does not leak a set -e abort into the output" "$OUT" "command not found"

# The function must survive every command failing, since it only runs when
# the cluster is already unhealthy.
if ( set -e; vs_dump_clusterip_diagnostics /nope probe-node 10.96.0.1 443 >/dev/null 2>&1 ); then
    ok "returns success even when every probe fails"
else
    bad "returns success even when every probe fails"
fi

echo
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ]
