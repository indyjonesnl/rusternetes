#!/usr/bin/env bash
# Unit tests for the post-test diagnostics dump.
#
# The harness dumped diagnostics when readiness timed out and when the ClusterIP
# gate failed, but the path after the conformance subset ran went straight from
# the spec counts to vs_emit_result. So `outcome=test-failed` produced ginkgo's
# timeline and nothing else.
#
# Run 34361805618 is the case that motivated this: one failing spec
# (`[sig-api-machinery] Garbage collector should not be blocked by dependency
# circle`, #1919) and no api-server log, no kube-controller-manager log — the
# two artefacts that say whether the GC ever issued the delete it was waiting
# for. Root-causing it took a day of reading upstream instead of a minute of
# reading a log (#1921).
#
# So the dump must, at minimum:
#   - capture the swapped module's own log;
#   - capture the VANILLA peers too, because a failing spec is usually an
#     interaction between the swapped module and whatever drives it;
#   - capture events and pod state, which is where "it never got scheduled"
#     shows up;
#   - run BEFORE teardown and only when the outcome is not test-passed;
#   - never abort the run when a command fails.
#
# Run with: bash scripts/tests/test-vanilla-swap-test-failure-diagnostics.sh
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
STUB_LOG="$(mktemp)"
trap 'rm -f "$STUB_LOG"' EXIT

kubectl() {
    printf 'kubectl %s\n' "$*" >>"$STUB_LOG"
    printf 'stub output\n'
}
docker() {
    printf 'docker %s\n' "$*" >>"$STUB_LOG"
    case "$*" in
        *"--format {{.Names}}"*) printf 'vanilla-swap-c1-control-plane\n' ;;
        *)                       printf 'stub output\n' ;;
    esac
}
export -f kubectl docker 2>/dev/null || true

echo "vs_dump_test_failure_diagnostics"

VS_MODULE=api-server
VS_OUTCOME=test-failed
OUT="$(vs_dump_test_failure_diagnostics c1 /tmp/nonexistent.kubeconfig 2>&1 || true)"
CMDS="$(cat "$STUB_LOG")"

contains "names the outcome it is explaining" "$OUT" "outcome=test-failed"

# The swapped module, via the shared vs_dump_module_logs.
contains "enumerates the harness containers rather than guessing names" \
    "$CMDS" "docker ps -a --filter name=vanilla-swap-c1"
contains "reads the enumerated container's log" \
    "$CMDS" "docker logs --tail 200 vanilla-swap-c1-control-plane"
contains "reads the swapped module's in-cluster log by component label" \
    "$CMDS" "component=kube-api-server"

# The vanilla peers. #1919 needed the kube-controller-manager log specifically:
# the GC runs there, and the question was whether it ever issued the delete.
contains "captures the kube-controller-manager log" \
    "$CMDS" "component=kube-controller-manager"
contains "captures the kube-apiserver log" "$CMDS" "component=kube-apiserver"
contains "captures the kube-scheduler log" "$CMDS" "component=kube-scheduler"
contains "captures the kube-proxy log" "$CMDS" "kube-proxy"

contains "captures pod state across all namespaces" "$CMDS" "get pods -A"
contains "captures events" "$CMDS" "get events -A"
contains "sorts events by time so the tail is the failure window" \
    "$CMDS" "--sort-by=.lastTimestamp"

# Every log fetch must be bounded: an unbounded dump buries the verdict it is
# meant to explain.
if printf '%s' "$CMDS" | grep -E '^kubectl .*logs' | grep -qv -- '--tail'; then
    bad "every kubectl logs call is bounded by --tail"
    printf '%s\n' "$CMDS" | grep -E '^kubectl .*logs' | grep -v -- '--tail' >&2
else
    ok "every kubectl logs call is bounded by --tail"
fi

lacks "does not leak a set -e abort into the output" "$OUT" "command not found"

# The dump only runs when something is already broken, so a failing probe must
# not become the run's verdict.
kubectl() { return 1; }
docker()  { return 1; }
if ( set -e; vs_dump_test_failure_diagnostics c1 /nope >/dev/null 2>&1 ); then
    ok "returns success even when every probe fails"
else
    bad "returns success even when every probe fails"
fi

echo
echo "wiring in scripts/vanilla-swap-run.sh"

RUN_SH="$REPO_ROOT/scripts/vanilla-swap-run.sh"
contains "the run script calls the dump" \
    "$(cat "$RUN_SH")" "vs_dump_test_failure_diagnostics"

# Order matters twice over: after the verdict exists (so it can be gated on the
# outcome) and before vs_emit_result, because the EXIT trap tears the cluster
# down and a destroyed cluster has no logs left to read.
dump_line="$(grep -n 'vs_dump_test_failure_diagnostics' "$RUN_SH" | tail -1 | cut -d: -f1)"
emit_line="$(grep -n '^vs_emit_result' "$RUN_SH" | tail -1 | cut -d: -f1)"
verdict_line="$(grep -n 'read -r VS_OUTCOME VS_EXIT' "$RUN_SH" | tail -1 | cut -d: -f1)"
if [ -n "$dump_line" ] && [ -n "$emit_line" ] && [ -n "$verdict_line" ] \
   && [ "$dump_line" -gt "$verdict_line" ] && [ "$dump_line" -lt "$emit_line" ]; then
    ok "dump runs after the verdict and before the result is emitted"
else
    bad "dump runs after the verdict and before the result is emitted"
    printf '    verdict=%s dump=%s emit=%s\n' "$verdict_line" "$dump_line" "$emit_line" >&2
fi

# A green run must not pay for forensics it does not need.
if grep -B 2 'vs_dump_test_failure_diagnostics' "$RUN_SH" | grep -q 'test-passed'; then
    ok "gated on the outcome, so a passing run dumps nothing"
else
    bad "gated on the outcome, so a passing run dumps nothing"
fi

echo
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ]
