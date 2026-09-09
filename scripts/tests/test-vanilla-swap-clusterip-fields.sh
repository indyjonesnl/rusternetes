#!/usr/bin/env bash
# Regression tests for reading default/kubernetes's ClusterIP and port.
#
# The substrate gate added in #1820 has reported `substrate-not-ready` on every
# api-server-leg run since it was introduced, and the cause was in the gate, not
# the substrate:
#
#     # scripts/vanilla-swap-run.sh:27
#     IFS=$'\n\t'
#     ...
#     read -r svc_ip svc_port <<<"$(vs_kubernetes_clusterip "$RESTORE_KC")"
#
# `vs_kubernetes_clusterip` emits `"<ip> <port>"` from a single jsonpath, but the
# driver's strict-mode IFS has no **space** in it, so `read` did not split: the
# whole string landed in `svc_ip` and `svc_port` came out empty. Two things then
# broke silently and in the same direction:
#
#   * `vs_dial_cluster_ip` ran `exec 3<>/dev/tcp/"10.96.0.1 443"/443`, a
#     malformed path that can never connect — so the gate never actually tested
#     the ClusterIP, it just always timed out.
#   * the failure dump greped `iptables-save` for the literal `10.96.0.1 443`,
#     a string no rule can contain — so it always printed
#     `NO iptables rules reference ...`, which reads exactly like a real finding
#     and sent three investigations after kube-proxy.
#
# Every other space-splitting `read` in the driver prefixes `IFS=' '`, and
# `vanilla-swap-common.sh` even carries a comment warning about this exact
# trap. Rather than add a fourth `IFS=' '` for the next caller to forget, the
# split moved into `vs_clusterip_fields`, which uses parameter expansion and so
# has no IFS dependency at all.
#
# Run with: bash scripts/tests/test-vanilla-swap-clusterip-fields.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
VS_LIB_ONLY=1 . "$REPO_ROOT/scripts/vanilla-swap-common.sh"

PASS=0; FAIL=0
ok()  { PASS=$((PASS + 1)); echo "  ok   - $1"; }
bad() { FAIL=$((FAIL + 1)); echo "  FAIL - $1" >&2; }
eq() {
    if [ "$2" = "$3" ]; then ok "$1"; else
        bad "$1"; printf '    expected: %q\n    actual:   %q\n' "$3" "$2" >&2
    fi
}

# Stand in for the jsonpath query, which is what produces the space.
STUB_OUT='10.96.0.1 443'
vs_kubernetes_clusterip() { printf '%s' "$STUB_OUT"; }

# Every case runs in THIS shell, not a subshell: a subshell's PASS/FAIL
# increments are lost on exit, which made the summary read `passed=0` while
# printing thirteen `ok` lines. A test harness that miscounts its own results
# is the one thing it must not do.
ORIG_IFS="$IFS"
restore_ifs() { IFS="$ORIG_IFS"; }
trap restore_ifs EXIT

echo "vs_clusterip_fields under the driver's strict-mode IFS"
# THE regression: the driver runs with this IFS, and it must not matter.
IFS=$'\n\t'
vs_clusterip_fields /dev/null
eq "ip is the bare address, not 'ip port'" "$VS_SVC_IP" "10.96.0.1"
eq "port is split out"                     "$VS_SVC_PORT" "443"

echo "vs_clusterip_fields with a default IFS"
IFS=$' \t\n'
vs_clusterip_fields /dev/null
eq "ip unchanged"   "$VS_SVC_IP" "10.96.0.1"
eq "port unchanged" "$VS_SVC_PORT" "443"

echo "degenerate outputs"
IFS=$'\n\t'
STUB_OUT=''
vs_clusterip_fields /dev/null
eq "no service: empty ip"   "$VS_SVC_IP" ""
eq "no service: empty port" "$VS_SVC_PORT" ""

# A Service whose port jsonpath resolved to nothing must not leave the ip
# duplicated into the port — the caller applies its own `:-443` default.
STUB_OUT='10.96.0.1'
vs_clusterip_fields /dev/null
eq "ip only: ip kept"        "$VS_SVC_IP" "10.96.0.1"
eq "ip only: port is empty"  "$VS_SVC_PORT" ""

STUB_OUT='  10.96.0.1   443  '
vs_clusterip_fields /dev/null
eq "padded: ip trimmed"   "$VS_SVC_IP" "10.96.0.1"
eq "padded: port trimmed" "$VS_SVC_PORT" "443"

STUB_OUT=$'10.96.0.1\t443'
vs_clusterip_fields /dev/null
eq "tab-separated: ip"   "$VS_SVC_IP" "10.96.0.1"
eq "tab-separated: port" "$VS_SVC_PORT" "443"

# The dialer is what consumed the bad value. Prove it is now handed a bare IP:
# a `/dev/tcp/<host>/<port>` path with a space in the host can never connect,
# and that failure is indistinguishable from a genuinely unreachable ClusterIP.
echo "the dialer receives a bare ip and port"
STUB_OUT='10.96.0.1 443'
vs_clusterip_fields /dev/null
DIAL_ARGS=''
record_dial() { DIAL_ARGS="$2|$3"; }
VS_DIAL_CMD=record_dial vs_dial_cluster_ip node "$VS_SVC_IP" "${VS_SVC_PORT:-443}" || true
eq "dial target has no embedded space" "$DIAL_ARGS" "10.96.0.1|443"

echo
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ]
