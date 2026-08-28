#!/usr/bin/env bash
# Validate scripts/conformance-preflight.sh (#1777).
#
# The preflight exists because five environment faults each let a conformance
# run start and emit a complete, wrong failure list — the original three
# (#1777), plus per-node image drift (#1792) and a saturated storage backend
# (#1794). Each case below reproduces one of those faults with a stubbed
# kubectl/docker on $PATH and asserts that
#   * the exit code is 2 (the runners' usage/preflight class), and
#   * the message names the specific failing condition,
# plus the happy path where every stub reports health and the exit code is 0.
#
# Stubs are plain shell scripts writing canned jsonpath answers, so the test
# needs no cluster and no container runtime.
set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

PREFLIGHT="$PROJECT_ROOT/scripts/conformance-preflight.sh"
[ -x "$PREFLIGHT" ] || { echo "FAIL: $PREFLIGHT missing or not executable" >&2; exit 1; }

fail=0
TMPROOT="$(mktemp -d)"
trap 'rm -rf "$TMPROOT"' EXIT

# ---------------------------------------------------------------------------
# Stub factory. Scenario knobs come in as env vars so one stub serves every
# case: SCHED_PHASE, CM_PHASE, DNS_READY, DISK_1, DISK_2, MEM_1, MEM_2,
# HEALTHZ_OK, CERTS_MOUNTED, PULL_OK.
# ---------------------------------------------------------------------------
make_stubs() {
    local dir="$1"
    mkdir -p "$dir"

    cat >"$dir/kubectl" <<'STUB'
#!/usr/bin/env bash
# Stub kubectl: answers only the queries conformance-preflight.sh makes.
args="$*"
case "$args" in
    *"--raw /healthz"*)
        [ "${HEALTHZ_OK:-1}" = "1" ] || exit 1
        echo ok; exit 0 ;;
    *"pod -n kube-system kube-scheduler-node-1"*)
        printf '%s' "${SCHED_PHASE-Running}"; exit 0 ;;
    *"pod -n kube-system kube-controller-manager-node-1"*)
        printf '%s' "${CM_PHASE-Running}"; exit 0 ;;
    *"deploy -n kube-system rusternetes-dns"*)
        printf '%s' "${DNS_READY-1}"; exit 0 ;;
    *"get nodes"*)
        # Mirrors the jsonpath shape: name=<disk>,<mem> per node, space separated.
        printf 'node-1=%s,%s node-2=%s,%s ' \
            "${DISK_1:-False}" "${MEM_1:-False}" "${DISK_2:-False}" "${MEM_2:-False}"
        exit 0 ;;
esac
exit 0
STUB

    cat >"$dir/docker" <<'STUB'
#!/usr/bin/env bash
# Stub docker. Answers the four shapes conformance-preflight.sh uses:
#   inspect <name>                 existence probe
#   inspect <name> -f {{.Mounts}}  certs mount
#   inspect <name> -f {{.Image}}   image ID (per-node equality)
#   exec <name> sh -c ...          storage size
#   pull <ref>                     registry path
name=""
fmt=""
case "${1:-}" in
    inspect)
        shift
        while [ $# -gt 0 ]; do
            case "$1" in
                -f) fmt="$2"; shift 2 ;;
                *)  name="$1"; shift ;;
            esac
        done
        # Which containers exist. node-2 peers and the store are individually
        # switchable so the "no pair present" and "no store" paths get covered.
        case "$name" in
            rusternetes-kubelet)   [ "${KUBELET_PRESENT:-1}"  = "1" ] || exit 1 ;;
            rusternetes-kubelet2|rusternetes-kube-proxy2|rusternetes-containerd2)
                                   [ "${NODE2_PRESENT:-1}"    = "1" ] || exit 1 ;;
            rusternetes-rhino)     [ "${RHINO_PRESENT:-1}"    = "1" ] || exit 1 ;;
        esac
        case "$fmt" in
            "")
                # Existence probe: no format, no output.
                exit 0 ;;
            *)
                if [ "${CERTS_MOUNTED:-1}" = "1" ]; then
                    printf '%s:%s\n' "${CERTS_PATH}" "${CERTS_PATH}"
                else
                    printf '%s:/etc/rusternetes/certs\n' "${CERTS_PATH}"
                fi
                exit 0 ;;
        esac ;;
    exec)
        shift
        target="${1:-}"
        case "$*" in
            *sha256sum*)
                # Per-node binary equality. node-2 peers report BIN_NODE2 so a
                # drift case can be expressed; both default to the same hash.
                # Presence is checked FIRST: an absent peer must yield no output
                # at all, which is how the real docker exec behaves.
                case "$target" in
                    *2) [ "${NODE2_PRESENT:-1}" = "1" ] || exit 1 ;;
                esac
                case "$target" in
                    *2) printf '%s  /app/x\n' "${BIN_NODE2:-aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa7777bbbb8888}" ;;
                    *)  printf '%s  /app/x\n' "${BIN_NODE1:-aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa7777bbbb8888}" ;;
                esac
                exit 0 ;;
            *)
                # The store-size probe.
                [ "${STORAGE_READABLE:-1}" = "1" ] || exit 1
                printf '%s\n' "${STORAGE_DB_BYTES:-1048576}"
                printf '%s\n' "${STORAGE_WAL_BYTES:-1048576}"
                exit 0 ;;
        esac ;;
    pull)
        [ "${PULL_OK:-1}" = "1" ] || { echo "Error response from daemon: timeout" >&2; exit 1; }
        exit 0 ;;
esac
exit 0
STUB

    chmod +x "$dir/kubectl" "$dir/docker"
}

STUBS="$TMPROOT/bin"
make_stubs "$STUBS"

FAKE_KUBECONFIG="$TMPROOT/kubeconfig"
echo "apiVersion: v1" >"$FAKE_KUBECONFIG"
FAKE_CERTS="$TMPROOT/certs"
mkdir -p "$FAKE_CERTS"

# Run the preflight with stubs first on PATH, plus any scenario knobs.
# Scenario knobs are passed as NAME=VALUE pairs and reach the stubs via env.
run_preflight() {
    env PATH="$STUBS:$PATH" CERTS_PATH="$FAKE_CERTS" "$@" \
        bash "$PREFLIGHT" --kubeconfig "$FAKE_KUBECONFIG" --certs-path "$FAKE_CERTS" \
        --timeout 30 2>&1
}

# expect_case <name> <expected_rc> <expected substring> [NAME=VALUE ...]
expect_case() {
    local name="$1" want_rc="$2" want_msg="$3"; shift 3
    local out rc
    out="$(run_preflight "$@")"
    rc=$?
    if [ "$rc" != "$want_rc" ]; then
        echo "FAIL [$name]: exit $rc, expected $want_rc"
        echo "$out" | sed 's/^/    /'
        fail=1
        return
    fi
    if [ -n "$want_msg" ] && ! grep -qi -- "$want_msg" <<<"$out"; then
        echo "FAIL [$name]: exit code correct but message did not mention '$want_msg'"
        echo "$out" | sed 's/^/    /'
        fail=1
        return
    fi
    echo "ok   [$name] (exit $rc${want_msg:+, names \"$want_msg\"})"
}

echo "--- conformance-preflight.sh"

# Healthy cluster: every assertion satisfied.
expect_case "healthy cluster passes" 0 "preflight OK"

# Trap 1: CERTS_PATH unset at compose up -> static pods Pending.
expect_case "scheduler Pending is refused" 2 "kube-scheduler-node-1 is Pending" \
    "SCHED_PHASE=Pending"

expect_case "controller-manager Pending is refused" 2 "kube-controller-manager-node-1 is Pending" \
    "CM_PHASE=Pending"

expect_case "absent static pod is refused" 2 "does not exist" \
    "SCHED_PHASE="

# Trap 2: unresolvable control-plane images -> dns never becomes ready.
expect_case "dns without a ready backend is refused" 2 "rusternetes-dns readyReplicas=0" \
    "DNS_READY=0"

# Trap 3: host over the imagefs eviction threshold.
expect_case "DiskPressure is refused" 2 "eviction pressure" \
    "DISK_1=True"

expect_case "MemoryPressure is refused" 2 "eviction pressure" \
    "MEM_2=True"

# The certs mount itself — the root cause behind trap 1.
expect_case "mismatched certs mount is refused" 2 "identical path" \
    "CERTS_MOUNTED=0"

# The registry path: a real pull, not a reachability probe.
expect_case "failing image pull is refused" 2 "cannot pull" \
    "PULL_OK=0"

# An unreachable api-server must be reported as such, not as absent pods.
expect_case "unreachable api-server is refused" 2 "not reachable" \
    "HEALTHZ_OK=0"

# Multiple simultaneous faults are all listed, not just the first.
out_multi="$(run_preflight "DNS_READY=0" "DISK_1=True")"
if grep -q "2 condition(s)" <<<"$out_multi"; then
    echo "ok   [multiple faults are all reported]"
else
    echo "FAIL [multiple faults are all reported]: expected a 2-condition summary"
    echo "$out_multi" | sed 's/^/    /'
    fail=1
fi

# --skip-image-pull must bypass only the pull assertion.
out_skip="$(env PATH="$STUBS:$PATH" CERTS_PATH="$FAKE_CERTS" PULL_OK=0 \
    bash "$PREFLIGHT" --kubeconfig "$FAKE_KUBECONFIG" --certs-path "$FAKE_CERTS" \
    --skip-image-pull --timeout 30 2>&1)"
rc_skip=$?
if [ "$rc_skip" = "0" ] && grep -q "skipped" <<<"$out_skip"; then
    echo "ok   [--skip-image-pull bypasses only the pull assertion]"
else
    echo "FAIL [--skip-image-pull]: exit $rc_skip"
    echo "$out_skip" | sed 's/^/    /'
    fail=1
fi

# Per-node binary drift (#1792): node-1 rebuilt, node-2 left on old code. The
# spec then passes or fails depending on which node the scheduler picks.
#
# The comparison is on the BINARY, not the image ID: compose builds each service
# as its own tagged image, so kubelet and kubelet2 have different image IDs even
# when built in one invocation from the same Dockerfile and target. Measured on
# a live cluster — both pairs' IDs differed while sha256sum /app/<binary>
# matched, so an image-ID assertion fails 100% of the time on a healthy cluster.
expect_case "per-node binary drift is refused" 2 "DIFFERENT /app" \
    "BIN_NODE2=9999888877776666555544443333222211110000ffffeeeeddddccccbbbbaaaa"

expect_case "matching per-node binaries pass" 0 "run the same /app"

# A single-node or all-in-one stack has no node-2 peer, which is not a fault.
expect_case "absent node-2 peers are not a fault" 0 "no per-node service pairs present" \
    "NODE2_PRESENT=0"

# --skip-node-image-check bypasses only that assertion.
out_skip_img="$(env PATH="$STUBS:$PATH" CERTS_PATH="$FAKE_CERTS" \
    BIN_NODE2=9999888877776666555544443333222211110000ffffeeeeddddccccbbbbaaaa \
    bash "$PREFLIGHT" --kubeconfig "$FAKE_KUBECONFIG" --certs-path "$FAKE_CERTS" \
    --skip-node-image-check --timeout 30 2>&1)"
rc_skip_img=$?
if [ "$rc_skip_img" = "0" ] && grep -q "binary-equality assertion skipped" <<<"$out_skip_img"; then
    echo "ok   [--skip-node-image-check bypasses only that assertion]"
else
    echo "FAIL [--skip-node-image-check]: exit $rc_skip_img"
    echo "$out_skip_img" | sed 's/^/    /'
    fail=1
fi

# Storage saturation (#1794): 512 MB db + 192 MB WAL is the state at which
# list+delete-heavy specs start timing out while /healthz still answers fast.
expect_case "saturated storage is refused" 2 "storage backend is saturated" \
    "STORAGE_DB_BYTES=536870912" "STORAGE_WAL_BYTES=201326592"

# One fresh suite ends around 85 MB, which must NOT be refused.
expect_case "one suite worth of storage passes" 0 "storage backend holds" \
    "STORAGE_DB_BYTES=89128960" "STORAGE_WAL_BYTES=39845888"

# Exactly at the ceiling is allowed; only above it is refused.
expect_case "storage exactly at the ceiling passes" 0 "storage backend holds" \
    "STORAGE_DB_BYTES=268435456" "STORAGE_WAL_BYTES=0"

# etcd / remote backend: no rhino container to measure, which is not a fault.
expect_case "absent storage container is not a fault" 0 "skipping storage-size assertion" \
    "RHINO_PRESENT=0"

# An unreadable store must not block the run — refusing there would block more
# than it protects.
expect_case "unreadable storage size is not a fault" 0 "could not size the store" \
    "STORAGE_READABLE=0"

# --max-storage-mb moves the ceiling.
out_ceiling="$(env PATH="$STUBS:$PATH" CERTS_PATH="$FAKE_CERTS" \
    STORAGE_DB_BYTES=104857600 STORAGE_WAL_BYTES=0 \
    bash "$PREFLIGHT" --kubeconfig "$FAKE_KUBECONFIG" --certs-path "$FAKE_CERTS" \
    --max-storage-mb 64 --timeout 30 2>&1)"
rc_ceiling=$?
if [ "$rc_ceiling" = "2" ] && grep -q "ceiling 64 MB" <<<"$out_ceiling"; then
    echo "ok   [--max-storage-mb moves the ceiling]"
else
    echo "FAIL [--max-storage-mb]: exit $rc_ceiling"
    echo "$out_ceiling" | sed 's/^/    /'
    fail=1
fi

# A non-numeric ceiling is a usage error, not a silent default.
out_bad_mb="$(env PATH="$STUBS:$PATH" CERTS_PATH="$FAKE_CERTS" \
    bash "$PREFLIGHT" --kubeconfig "$FAKE_KUBECONFIG" --max-storage-mb yes 2>&1)"
rc_bad_mb=$?
if [ "$rc_bad_mb" = "2" ] && grep -q "must be a positive integer" <<<"$out_bad_mb"; then
    echo "ok   [non-numeric --max-storage-mb is a usage error]"
else
    echo "FAIL [non-numeric --max-storage-mb]: exit $rc_bad_mb"
    echo "$out_bad_mb" | sed 's/^/    /'
    fail=1
fi

# --help must print the header and stop there. It used to spill the first ten
# lines of code, because the range was hardcoded as lines 2-58 and every header
# edit shifted it.
out_help="$(bash "$PREFLIGHT" --help 2>&1)"
if grep -q "Exit codes:" <<<"$out_help" && ! grep -q 'SCRIPT_NAME=' <<<"$out_help"; then
    echo "ok   [--help prints the header without spilling code]"
else
    echo "FAIL [--help spills code or omits the exit-code contract]"
    echo "$out_help" | tail -12 | sed 's/^/    /'
    fail=1
fi

# The runners must treat a preflight failure as fatal, not swallow it.
echo "--- runner wiring"
for runner in scripts/conformance-tags-run.sh scripts/conformance-target-run.sh; do
    if grep -q 'conformance-preflight.sh' "$runner"; then
        echo "ok   [$runner calls the preflight]"
    else
        echo "FAIL [$runner does not call scripts/conformance-preflight.sh]"
        fail=1
    fi
    if grep -qE '(--skip-preflight|SKIP_PREFLIGHT)' "$runner"; then
        echo "ok   [$runner offers a documented bypass]"
    else
        echo "FAIL [$runner has no --skip-preflight escape hatch]"
        fail=1
    fi
done

# cluster-up.sh must remove the two footguns it can (FR-010).
echo "--- cluster-up.sh footgun removal"
if grep -qE '^[[:space:]]*export CERTS_PATH' scripts/cluster-up.sh; then
    echo "ok   [cluster-up.sh exports CERTS_PATH]"
else
    echo "FAIL [cluster-up.sh does not export CERTS_PATH]"
    fail=1
fi
if grep -q 'CONTROL_PLANE_IMAGE_REGISTRY' scripts/cluster-up.sh; then
    echo "ok   [cluster-up.sh sets CONTROL_PLANE_IMAGE_REGISTRY on the prebuilt path]"
else
    echo "FAIL [cluster-up.sh never sets CONTROL_PLANE_IMAGE_REGISTRY]"
    fail=1
fi

if [ "$fail" = "0" ]; then
    echo "PASS: conformance preflight behaves as specified"
else
    echo "FAIL: see above" >&2
fi
exit "$fail"
