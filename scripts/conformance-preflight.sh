#!/usr/bin/env bash
# conformance-preflight.sh
#
# Refuse to start a conformance run against a cluster that cannot produce a
# meaningful result. Three separate faults have each produced a complete,
# plausible-looking, entirely wrong failure list (#1777):
#
#   * CERTS_PATH unset at `compose up` — the node-1 kubelet then mounts the
#     certs somewhere other than the host-absolute path the control-plane static
#     pods hostPath-mount, so kube-scheduler and kube-controller-manager sit
#     Pending forever while bootstrap-cluster.sh still exits 0. A 406-spec run
#     against a cluster with no scheduler looks like ~400 product bugs.
#   * CONTROL_PLANE_IMAGE_REGISTRY unset on the prebuilt-image path — bootstrap
#     templates `rusternetes-<component>:latest`, which the containerd image
#     store cannot resolve, so the same pods (and rusternetes-dns) never start.
#   * A host over the kubelet's imagefs eviction threshold — hydrophone's
#     e2e-conformance-test pod is evicted ("Pod evicted due to resource
#     pressure: ImageFsAvailable") after the run has already logged
#     "Will run 406 of 7348 specs".
#
# This script asserts and REPORTS. It never repairs: it does not export
# variables, restart containers, relax kubelet thresholds, or create objects.
# A degraded cluster is the operator's decision to fix.
#
# Companion to scripts/conformance-*-run.sh (which call it) and to the run
# watchdog tracked in #1635 — that one catches a cluster that wedges *after*
# the run starts; this one refuses to start.
#
# Usage:
#   bash scripts/conformance-preflight.sh [flags]
#
# Flags:
#   --kubeconfig PATH   Override kubeconfig (default: $KUBECONFIG or
#                       ~/.kube/rusternetes-config)
#   --certs-path PATH   Host-absolute certs dir expected inside the kubelet
#                       (default: $CERTS_PATH, else <repo>/.rusternetes/certs)
#   --kubelet NAME      Kubelet container to inspect for the certs mount
#                       (default: rusternetes-kubelet)
#   --sandbox-image REF Image whose pull is exercised
#                       (default: registry.k8s.io/pause:3.10.1)
#   --skip-image-pull   Skip only the image-pull assertion (offline work)
#   --timeout SECONDS   Budget for the whole preflight (default: 60)
#   -h, --help          Show this help
#
# Exit codes:
#   0  every assertion passed — the caller may start the run
#   2  at least one assertion failed (usage/preflight class, matching the
#      runners' documented exit-code contract), or a flag was invalid
set -uo pipefail
IFS=$'\n\t'

SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"

KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
CERTS_PATH_EXPECTED="${CERTS_PATH:-$REPO_ROOT/.rusternetes/certs}"
KUBELET_CONTAINER="rusternetes-kubelet"
SANDBOX_IMAGE="registry.k8s.io/pause:3.10.1"
SKIP_IMAGE_PULL=0
BUDGET_SECONDS=60

die() { echo "[$SCRIPT_NAME] ERROR: $*" >&2; exit 2; }

usage() { sed -n '2,58p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
    case "$1" in
        --kubeconfig)      [ $# -ge 2 ] || die "--kubeconfig requires a value"; KUBECONFIG_PATH="$2"; shift 2 ;;
        --certs-path)      [ $# -ge 2 ] || die "--certs-path requires a value"; CERTS_PATH_EXPECTED="$2"; shift 2 ;;
        --kubelet)         [ $# -ge 2 ] || die "--kubelet requires a value"; KUBELET_CONTAINER="$2"; shift 2 ;;
        --sandbox-image)   [ $# -ge 2 ] || die "--sandbox-image requires a value"; SANDBOX_IMAGE="$2"; shift 2 ;;
        --skip-image-pull) SKIP_IMAGE_PULL=1; shift ;;
        --timeout)         [ $# -ge 2 ] || die "--timeout requires a value"; BUDGET_SECONDS="$2"; shift 2 ;;
        -h|--help)         usage; exit 0 ;;
        *)                 die "unknown flag: $1 (use --help)" ;;
    esac
done

[[ "$BUDGET_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "--timeout must be a positive integer"

KUBECTL="${KUBECTL:-kubectl}"
CONTAINER_RT="${CONTAINER_RUNTIME:-docker}"

command -v "$KUBECTL" >/dev/null 2>&1 || die "kubectl not found (set \$KUBECTL to override)"
[ -f "$KUBECONFIG_PATH" ] || die "kubeconfig not found: $KUBECONFIG_PATH"
export KUBECONFIG="$KUBECONFIG_PATH"

# Per-assertion timeout so one hung call cannot eat the whole budget. Six
# assertions share the budget; give each a sixth, floor 5s.
PER_CHECK_TIMEOUT=$(( BUDGET_SECONDS / 6 ))
[ "$PER_CHECK_TIMEOUT" -lt 5 ] && PER_CHECK_TIMEOUT=5

FAILURES=()
pass() { echo "[$SCRIPT_NAME]   OK   $*"; }
fail() { echo "[$SCRIPT_NAME]  FAIL  $*"; FAILURES+=("$*"); }

kc() { timeout "$PER_CHECK_TIMEOUT" "$KUBECTL" "$@" 2>/dev/null; }

echo "[$SCRIPT_NAME] preflight: kubeconfig=$KUBECONFIG_PATH budget=${BUDGET_SECONDS}s"

# ---------------------------------------------------------------------------
# 1. API server reachable at all. Without this every later assertion reports a
#    misleading "absent" rather than "unreachable".
# ---------------------------------------------------------------------------
if kc get --raw /healthz >/dev/null; then
    pass "api-server responds to /healthz"
else
    fail "api-server is not reachable via $KUBECONFIG_PATH — is the stack up?"
    # Nothing else can be evaluated meaningfully.
    echo "[$SCRIPT_NAME] preflight FAILED (1 condition): api-server unreachable" >&2
    exit 2
fi

# ---------------------------------------------------------------------------
# 2. Control-plane static pods RUNNING. `Pending` is the exact CERTS_PATH
#    failure mode, and a run without a scheduler or controller-manager is
#    worthless — so assert the phase, not mere existence.
# ---------------------------------------------------------------------------
for pod in kube-scheduler-node-1 kube-controller-manager-node-1; do
    phase="$(kc get pod -n kube-system "$pod" -o jsonpath='{.status.phase}')"
    case "$phase" in
        Running) pass "$pod is Running" ;;
        "")      fail "$pod does not exist — bootstrap did not template the static pod, or the kubelet never accepted it" ;;
        *)       fail "$pod is $phase (expected Running) — commonly CERTS_PATH unset at 'compose up', or an unresolvable control-plane image" ;;
    esac
done

# ---------------------------------------------------------------------------
# 3. Cluster DNS has a ready backend. rusternetes-dns is a kube-system
#    Deployment (not CoreDNS); without it every DNS-dependent spec times out.
# ---------------------------------------------------------------------------
dns_ready="$(kc get deploy -n kube-system rusternetes-dns -o jsonpath='{.status.readyReplicas}')"
if [ "${dns_ready:-0}" -ge 1 ] 2>/dev/null; then
    pass "rusternetes-dns has ${dns_ready} ready replica(s)"
else
    fail "rusternetes-dns readyReplicas=${dns_ready:-0} — cluster DNS has no backend"
fi

# ---------------------------------------------------------------------------
# 4. No node under pressure. A node at the kubelet's imagefs threshold evicts
#    hydrophone's own e2e pod, after the run has announced its spec count.
# ---------------------------------------------------------------------------
pressure_report="$(kc get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"="}{range .status.conditions[?(@.type=="DiskPressure")]}{.status}{end}{","}{range .status.conditions[?(@.type=="MemoryPressure")]}{.status}{end}{" "}{end}')"
if [ -z "${pressure_report// }" ]; then
    fail "could not read node conditions — cannot rule out eviction pressure"
else
    # Split on spaces explicitly: this script sets IFS=$'\n\t' at the top, so
    # word-splitting a space-separated jsonpath result would otherwise yield one
    # giant "entry" and silently miss every node but the first.
    pressured=""
    IFS=' ' read -r -a pressure_entries <<<"$pressure_report"
    for entry in "${pressure_entries[@]}"; do
        [ -n "$entry" ] || continue
        node="${entry%%=*}"
        conds="${entry#*=}"
        disk="${conds%%,*}"
        mem="${conds##*,}"
        [ "$disk" = "True" ] && pressured="$pressured $node(DiskPressure)"
        [ "$mem" = "True" ] && pressured="$pressured $node(MemoryPressure)"
    done
    if [ -n "${pressured// }" ]; then
        fail "node(s) under eviction pressure:${pressured} — the e2e pod will be evicted before any spec runs"
    else
        pass "no node reports Disk/MemoryPressure"
    fi
fi

# ---------------------------------------------------------------------------
# 5. The kubelet sees the certs dir at the SAME absolute path the static pods
#    hostPath-mount. Docker resolves a pod hostPath on the host, and the
#    kubelet also stat()s it inside its own container, so the mount must be
#    ${CERTS_PATH}:${CERTS_PATH}. This is the root cause behind #2 above, and
#    catching it directly makes the failure self-explanatory.
# ---------------------------------------------------------------------------
if command -v "$CONTAINER_RT" >/dev/null 2>&1; then
    if timeout "$PER_CHECK_TIMEOUT" "$CONTAINER_RT" inspect "$KUBELET_CONTAINER" >/dev/null 2>&1; then
        mounts="$(timeout "$PER_CHECK_TIMEOUT" "$CONTAINER_RT" inspect "$KUBELET_CONTAINER" \
            -f '{{range .Mounts}}{{.Source}}:{{.Destination}}{{"\n"}}{{end}}' 2>/dev/null)"
        if grep -qx "${CERTS_PATH_EXPECTED}:${CERTS_PATH_EXPECTED}" <<<"$mounts"; then
            pass "kubelet mounts certs at the identical path on both sides ($CERTS_PATH_EXPECTED)"
        else
            fail "kubelet does not mount the certs dir at the identical path on both sides (want ${CERTS_PATH_EXPECTED}:${CERTS_PATH_EXPECTED}) — export CERTS_PATH BEFORE 'compose up' and recreate the stack"
        fi
    else
        # Not fatal on its own: a remote or differently-named cluster is valid.
        pass "kubelet container '$KUBELET_CONTAINER' not present locally — skipping certs-mount assertion"
    fi
else
    pass "$CONTAINER_RT not available — skipping certs-mount assertion"
fi

# ---------------------------------------------------------------------------
# 6. A REAL image pull. registry.k8s.io redirects manifests and blobs to
#    europe-west4-docker.pkg.dev; on 2026-08-27 the front door answered 401
#    while every pkg.dev connection timed out for ~7 minutes, so a
#    reachability probe of registry.k8s.io proves nothing. Pull something tiny
#    and real instead.
# ---------------------------------------------------------------------------
if [ "$SKIP_IMAGE_PULL" = "1" ]; then
    pass "image-pull assertion skipped (--skip-image-pull)"
elif ! command -v "$CONTAINER_RT" >/dev/null 2>&1; then
    pass "$CONTAINER_RT not available — skipping image-pull assertion"
else
    pull_budget=$(( PER_CHECK_TIMEOUT * 2 ))
    if timeout "$pull_budget" "$CONTAINER_RT" pull -q "$SANDBOX_IMAGE" >/dev/null 2>&1; then
        pass "pulled $SANDBOX_IMAGE (upstream registry path works end to end)"
    else
        fail "cannot pull $SANDBOX_IMAGE — registry.k8s.io redirects blobs to pkg.dev; a 401 from the front door does not mean pulls work"
    fi
fi

# ---------------------------------------------------------------------------
if [ "${#FAILURES[@]}" -eq 0 ]; then
    echo "[$SCRIPT_NAME] preflight OK — cluster can produce a meaningful run"
    exit 0
fi

echo "[$SCRIPT_NAME] preflight FAILED (${#FAILURES[@]} condition(s)):" >&2
for f in "${FAILURES[@]}"; do
    echo "[$SCRIPT_NAME]   - $f" >&2
done
echo "[$SCRIPT_NAME] refusing to start: a run against this cluster would report failures that are not product defects" >&2
exit 2
