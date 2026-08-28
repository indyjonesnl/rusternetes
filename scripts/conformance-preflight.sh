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
#   * Per-node binaries drifting apart. kubelet/kubelet2 and
#     kube-proxy/kube-proxy2 are separate compose services, so
#     `compose build kubelet` leaves node-2 on old code. Whichever node the
#     scheduler happens to pick then decides the result — the same spec passes
#     and fails alternately and reads exactly like a flake (#1792). Compare the
#     BINARY, not the image ID: compose builds each service as its own tagged
#     image, so the two IDs differ even when built in one invocation from the
#     same Dockerfile and target, while the binaries are byte-identical.
#   * A saturated storage backend. After a few hours of runs the rhino store
#     reached state.db 512 MB + WAL 192 MB with rhino pegged at 1364% CPU, and
#     list+delete-heavy specs began failing with `context deadline exceeded`.
#     Nothing is unhealthy — /healthz answers in 96 ms — but the failure list
#     is fiction (#1794).
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
#                       (default: rusternetes-kubelet; 'none' skips the
#                       assertion — the containers are addressed by name on the
#                       local daemon, so a cluster that is not this compose
#                       stack must opt out rather than inspect someone else's)
#   --control-plane-pods LIST
#                       Comma-separated kube-system pods that must be Running
#                       (default: kube-scheduler-node-1,kube-controller-manager-node-1;
#                       'none' skips the assertion). vanilla-swap runs this
#                       against a kind cluster, where the mirror pods are
#                       named after the kind control-plane node.
#   --dns-deployment NAME
#                       kube-system Deployment that must have a ready replica
#                       (default: rusternetes-dns; 'none' skips the assertion,
#                       e.g. a kind cluster serving DNS from CoreDNS)
#   --sandbox-image REF Image whose pull is exercised
#                       (default: registry.k8s.io/pause:3.10.1)
#   --skip-image-pull   Skip only the image-pull assertion (offline work)
#   --storage-container NAME
#                       Container holding the storage backend, inspected for
#                       size (default: rusternetes-rhino; 'none' or absent =
#                       skipped, which is the etcd/remote case)
#   --max-storage-mb N  Refuse to run when state.db + WAL exceed N MB
#                       (default: 256; one full suite on a fresh cluster grows
#                       the store to ~85 MB, so 256 means "more than a couple
#                       of suites of never-reclaimed pages")
#   --skip-node-image-check
#                       Skip only the per-node binary-equality assertion
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
CONTROL_PLANE_PODS="kube-scheduler-node-1,kube-controller-manager-node-1"
DNS_DEPLOYMENT="rusternetes-dns"
SANDBOX_IMAGE="registry.k8s.io/pause:3.10.1"
SKIP_IMAGE_PULL=0
STORAGE_CONTAINER="rusternetes-rhino"
MAX_STORAGE_MB=256
SKIP_NODE_IMAGE_CHECK=0
# Per-node compose service pairs that MUST run the same build. Each entry is
# "node-1 container:node-2 container:binary path". containerd/containerd2 are
# absent deliberately: they pin `image: rusternetes-containerd:latest` in the
# compose file, so both nodes resolve one tag and cannot drift.
NODE_SERVICE_PAIRS=(
    "rusternetes-kubelet:rusternetes-kubelet2:/app/kubelet"
    "rusternetes-kube-proxy:rusternetes-kube-proxy2:/app/kube-proxy"
)
BUDGET_SECONDS=60

die() { echo "[$SCRIPT_NAME] ERROR: $*" >&2; exit 2; }

# Print the header comment block, whatever length it happens to be: everything
# after the shebang up to the first line that is not a comment.
usage() {
    awk 'NR == 1 { next } !/^#/ { exit } { print }' "${BASH_SOURCE[0]}" \
        | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        --kubeconfig)      [ $# -ge 2 ] || die "--kubeconfig requires a value"; KUBECONFIG_PATH="$2"; shift 2 ;;
        --certs-path)      [ $# -ge 2 ] || die "--certs-path requires a value"; CERTS_PATH_EXPECTED="$2"; shift 2 ;;
        --kubelet)         [ $# -ge 2 ] || die "--kubelet requires a value"; KUBELET_CONTAINER="$2"; shift 2 ;;
        --control-plane-pods) [ $# -ge 2 ] || die "--control-plane-pods requires a value"; CONTROL_PLANE_PODS="$2"; shift 2 ;;
        --dns-deployment)  [ $# -ge 2 ] || die "--dns-deployment requires a value"; DNS_DEPLOYMENT="$2"; shift 2 ;;
        --sandbox-image)   [ $# -ge 2 ] || die "--sandbox-image requires a value"; SANDBOX_IMAGE="$2"; shift 2 ;;
        --skip-image-pull) SKIP_IMAGE_PULL=1; shift ;;
        --storage-container) [ $# -ge 2 ] || die "--storage-container requires a value"; STORAGE_CONTAINER="$2"; shift 2 ;;
        --max-storage-mb)  [ $# -ge 2 ] || die "--max-storage-mb requires a value"; MAX_STORAGE_MB="$2"; shift 2 ;;
        --skip-node-image-check) SKIP_NODE_IMAGE_CHECK=1; shift ;;
        --timeout)         [ $# -ge 2 ] || die "--timeout requires a value"; BUDGET_SECONDS="$2"; shift 2 ;;
        -h|--help)         usage; exit 0 ;;
        *)                 die "unknown flag: $1 (use --help)" ;;
    esac
done

[[ "$BUDGET_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "--timeout must be a positive integer"
[[ "$MAX_STORAGE_MB" =~ ^[1-9][0-9]*$ ]] || die "--max-storage-mb must be a positive integer"

KUBECTL="${KUBECTL:-kubectl}"
CONTAINER_RT="${CONTAINER_RUNTIME:-docker}"

command -v "$KUBECTL" >/dev/null 2>&1 || die "kubectl not found (set \$KUBECTL to override)"
[ -f "$KUBECONFIG_PATH" ] || die "kubeconfig not found: $KUBECONFIG_PATH"
export KUBECONFIG="$KUBECONFIG_PATH"

# Per-assertion timeout so one hung call cannot eat the whole budget. Eight
# assertions share the budget; give each an eighth, floor 5s.
PER_CHECK_TIMEOUT=$(( BUDGET_SECONDS / 8 ))
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
#
#    The names are a compose-stack fact, not a universal one: vanilla-swap
#    (#1629) runs this preflight against a kind cluster whose mirror pods are
#    kube-<component>-<kind-node>, and a cluster whose control plane is none of
#    rusternetes' business passes 'none'.
if [ "$CONTROL_PLANE_PODS" = "none" ] || [ -z "$CONTROL_PLANE_PODS" ]; then
    pass "control-plane static-pod assertion skipped (--control-plane-pods none)"
else
    IFS=',' read -r -a control_plane_pods <<<"$CONTROL_PLANE_PODS"
    for pod in "${control_plane_pods[@]}"; do
        [ -n "$pod" ] || continue
        phase="$(kc get pod -n kube-system "$pod" -o jsonpath='{.status.phase}')"
        case "$phase" in
            Running) pass "$pod is Running" ;;
            "")      fail "$pod does not exist — bootstrap did not template the static pod, or the kubelet never accepted it" ;;
            *)       fail "$pod is $phase (expected Running) — commonly CERTS_PATH unset at 'compose up', or an unresolvable control-plane image" ;;
        esac
    done
fi

# ---------------------------------------------------------------------------
# 3. Cluster DNS has a ready backend. rusternetes-dns is a kube-system
#    Deployment (not CoreDNS); without it every DNS-dependent spec times out.
#    --dns-deployment names a different one (kind serves DNS from CoreDNS).
# ---------------------------------------------------------------------------
if [ "$DNS_DEPLOYMENT" = "none" ] || [ -z "$DNS_DEPLOYMENT" ]; then
    pass "cluster-DNS assertion skipped (--dns-deployment none)"
else
    dns_ready="$(kc get deploy -n kube-system "$DNS_DEPLOYMENT" -o jsonpath='{.status.readyReplicas}')"
    if [ "${dns_ready:-0}" -ge 1 ] 2>/dev/null; then
        pass "$DNS_DEPLOYMENT has ${dns_ready} ready replica(s)"
    else
        fail "$DNS_DEPLOYMENT readyReplicas=${dns_ready:-0} — cluster DNS has no backend"
    fi
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
if [ "$KUBELET_CONTAINER" = "none" ] || [ -z "$KUBELET_CONTAINER" ]; then
    pass "certs-mount assertion skipped (--kubelet none)"
elif command -v "$CONTAINER_RT" >/dev/null 2>&1; then
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
# 7. Both nodes run the SAME build of each per-node service. kubelet/kubelet2
#    and kube-proxy/kube-proxy2 are separate compose services, so
#    `compose build kubelet` updates node-1 only. On 2026-08-27 that cost two
#    wrong conclusions in a row about a kubelet fix: node-2 was running a binary
#    from hours earlier, and the spec's verdict depended on which node the
#    scheduler happened to pick (#1792).
#
#    Compare the BINARY, not the image ID. Compose builds each service as its
#    own tagged image, so `docker inspect -f {{.Image}}` differs between the two
#    even when both are built in a single invocation from the same Dockerfile and
#    target — measured: both pairs differed while `sha256sum /app/<binary>`
#    matched exactly. An image-ID comparison is a guaranteed false positive here.
# ---------------------------------------------------------------------------
if [ "$SKIP_NODE_IMAGE_CHECK" = "1" ]; then
    pass "per-node binary-equality assertion skipped (--skip-node-image-check)"
elif ! command -v "$CONTAINER_RT" >/dev/null 2>&1; then
    pass "$CONTAINER_RT not available — skipping per-node binary assertion"
else
    checked=0
    for pair in "${NODE_SERVICE_PAIRS[@]}"; do
        first="${pair%%:*}"
        rest="${pair#*:}"
        second="${rest%%:*}"
        binary="${rest#*:}"
        sum_first="$(timeout "$PER_CHECK_TIMEOUT" "$CONTAINER_RT" exec "$first" sha256sum "$binary" 2>/dev/null | awk '{print $1}')"
        sum_second="$(timeout "$PER_CHECK_TIMEOUT" "$CONTAINER_RT" exec "$second" sha256sum "$binary" 2>/dev/null | awk '{print $1}')"
        # A single-node stack (or an all-in-one one) legitimately has no peer,
        # and a container without that binary is not this assertion's business.
        [ -n "$sum_first" ] && [ -n "$sum_second" ] || continue
        checked=$(( checked + 1 ))
        if [ "$sum_first" = "$sum_second" ]; then
            pass "$first and $second run the same $binary (${sum_first:0:12})"
        else
            svc_first="${first#rusternetes-}"
            svc_second="${second#rusternetes-}"
            fail "$first and $second run DIFFERENT $binary (${sum_first:0:12} vs ${sum_second:0:12}) — rebuild both ('compose build $svc_first $svc_second') and recreate them, or the node the scheduler picks decides the result"
        fi
    done
    [ "$checked" -eq 0 ] && pass "no per-node service pairs present — single-node or all-in-one stack"
fi

# ---------------------------------------------------------------------------
# 8. The storage backend is not saturated. A cluster that has carried several
#    suites reaches state.db 512 MB + WAL 192 MB, at which point rhino pegs
#    ~13 cores and list+delete-heavy specs fail on `context deadline exceeded`
#    while /healthz still answers in under 100 ms (#1794). Those failures are
#    indistinguishable from product defects by inspection — one full suite on
#    a fresh cluster ends at ~85 MB, so the default ceiling of 256 MB means
#    "this store is carrying more than a couple of suites of pages that were
#    never reclaimed".
# ---------------------------------------------------------------------------
if [ "$STORAGE_CONTAINER" = "none" ] || [ -z "$STORAGE_CONTAINER" ]; then
    pass "storage-size assertion skipped (--storage-container none)"
elif ! command -v "$CONTAINER_RT" >/dev/null 2>&1; then
    pass "$CONTAINER_RT not available — skipping storage-size assertion"
elif ! timeout "$PER_CHECK_TIMEOUT" "$CONTAINER_RT" inspect "$STORAGE_CONTAINER" >/dev/null 2>&1; then
    # etcd, a remote backend, or the all-in-one binary: nothing to measure.
    pass "storage container '$STORAGE_CONTAINER' not present — skipping storage-size assertion"
else
    # stat every file that exists; a missing WAL is normal right after start.
    storage_bytes="$(timeout "$PER_CHECK_TIMEOUT" "$CONTAINER_RT" exec "$STORAGE_CONTAINER" \
        sh -c 'cat /dev/null; for f in /data/db/state.db /data/db/state.db-wal; do [ -f "$f" ] && stat -c %s "$f"; done' 2>/dev/null \
        | awk '{ total += $1 } END { printf "%d", total }')"
    if [ -z "$storage_bytes" ] || [ "$storage_bytes" -eq 0 ] 2>/dev/null; then
        # Not fatal: the layout may differ, and refusing to run over an
        # unreadable size would block more than it protects.
        pass "could not size the store in '$STORAGE_CONTAINER' — skipping storage-size assertion"
    else
        storage_mb=$(( storage_bytes / 1048576 ))
        if [ "$storage_mb" -gt "$MAX_STORAGE_MB" ]; then
            fail "storage backend is saturated: state.db + WAL = ${storage_mb} MB (ceiling ${MAX_STORAGE_MB} MB) — recreate the stack with 'down -v' before measuring; specs will otherwise fail on 'context deadline exceeded' and look like product defects (#1794)"
        else
            pass "storage backend holds ${storage_mb} MB (ceiling ${MAX_STORAGE_MB} MB)"
        fi
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
