#!/usr/bin/env bash
# Validate scripts/conformance-preflight.sh (#1777).
#
# The preflight exists because three environment faults each let a conformance
# run start and emit a complete, wrong failure list. Each case below reproduces
# one of those faults with a stubbed kubectl/docker on $PATH and asserts that
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
# Stub docker: inspect (certs mount) and pull (registry path) only.
case "${1:-}" in
    inspect)
        shift
        [ "${KUBELET_PRESENT:-1}" = "1" ] || exit 1
        # `docker inspect <name>` with no -f is the existence probe.
        for a in "$@"; do [ "$a" = "-f" ] && has_fmt=1; done
        [ "${has_fmt:-0}" = "1" ] || exit 0
        if [ "${CERTS_MOUNTED:-1}" = "1" ]; then
            printf '%s:%s\n' "${CERTS_PATH}" "${CERTS_PATH}"
        else
            printf '%s:/etc/rusternetes/certs\n' "${CERTS_PATH}"
        fi
        exit 0 ;;
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
