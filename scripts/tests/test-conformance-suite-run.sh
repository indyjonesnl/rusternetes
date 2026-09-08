#!/usr/bin/env bash
# Unit tests for the pure helpers in scripts/conformance-suite-run.sh: partition
# selection (explicit, default, skip, order, reverse, error cases) and summary
# aggregation. Sourced with SUITE_RUN_LIB_ONLY=1 so no cluster is needed.
#
# Run with: bash scripts/tests/test-conformance-suite-run.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
SUITE_RUN_LIB_ONLY=1 . "$REPO_ROOT/scripts/conformance-suite-run.sh"

command -v jq >/dev/null 2>&1 || { echo "FAIL: jq required" >&2; exit 1; }

PASS=0; FAIL=0
ok()   { PASS=$((PASS + 1)); echo "  ok   - $1"; }
bad()  { FAIL=$((FAIL + 1)); echo "  FAIL - $1" >&2; }
check() { # check <desc> <expected> <actual>
    if [ "$2" = "$3" ]; then ok "$1"; else
        bad "$1"; printf '    expected: %q\n    actual:   %q\n' "$2" "$3" >&2
    fi
}

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
FIX="$TMP/targets.json"
cat > "$FIX" <<'JSON'
[
  {"name": "sig-node",       "kind": "sig",     "focus": "n", "skip": "f"},
  {"name": "sig-apps",       "kind": "sig",     "focus": "a", "skip": "f"},
  {"name": "sig-cli",        "kind": "sig",     "focus": "c", "skip": "f"},
  {"name": "kine",           "kind": "feature", "focus": "k", "skip": "f"}
]
JSON

echo "== sig_targets =="
check "lists only kind:sig, in manifest order" \
    "sig-node sig-apps sig-cli" "$(sig_targets "$FIX" | tr '\n' ' ' | sed 's/ $//')"

echo "== resolve_selection =="
check "default selects every sig target, manifest order" \
    "sig-node sig-apps sig-cli" \
    "$(resolve_selection "$FIX" "" "" registry 0 | tr '\n' ' ' | sed 's/ $//')"

# A feature target overlaps the sig slices, so it must not arrive by default —
# but is still selectable explicitly (the kine leg runs exactly that way).
check "kind:feature target excluded from the default selection" \
    "" "$(resolve_selection "$FIX" "" "" registry 0 | grep -x kine || true)"
check "kind:feature target selectable explicitly" \
    "kine" "$(resolve_selection "$FIX" "kine" "" registry 0)"

check "explicit --targets preserves the given order, not manifest order" \
    "sig-cli sig-node" \
    "$(resolve_selection "$FIX" "sig-cli,sig-node" "" registry 0 | tr '\n' ' ' | sed 's/ $//')"

check "--skip-targets drops the named partition" \
    "sig-node sig-apps" \
    "$(resolve_selection "$FIX" "" "sig-cli" registry 0 | tr '\n' ' ' | sed 's/ $//')"

check "--skip-targets accepts a comma list" \
    "sig-apps" \
    "$(resolve_selection "$FIX" "" "sig-node,sig-cli" registry 0 | tr '\n' ' ' | sed 's/ $//')"

# A skip name that is not in the selection is a no-op, not an error: skipping
# sig-cli is safe to keep in a saved command line even when --targets omits it.
check "--skip-targets naming an unselected partition is a no-op" \
    "sig-apps" "$(resolve_selection "$FIX" "sig-apps" "sig-cli" registry 0)"

check "--order name sorts alphabetically" \
    "sig-apps sig-cli sig-node" \
    "$(resolve_selection "$FIX" "" "" name 0 | tr '\n' ' ' | sed 's/ $//')"

check "--reverse inverts the resolved order" \
    "sig-cli sig-apps sig-node" \
    "$(resolve_selection "$FIX" "" "" registry 1 | tr '\n' ' ' | sed 's/ $//')"

echo "== resolve_selection error cases =="
# An unknown name must fail loudly. Silently dropping it would run a SHORTER
# suite than asked for and report it as a complete one.
if resolve_selection "$FIX" "sig-nope" "" registry 0 >/dev/null 2>&1; then
    bad "unknown target should be rejected"
else ok "unknown target is rejected"; fi

if resolve_selection "$FIX" "" "sig-node,sig-apps,sig-cli" registry 0 >/dev/null 2>&1; then
    bad "skipping every partition should be rejected"
else ok "skipping every partition is rejected"; fi

if resolve_selection "$FIX" "" "" bogus 0 >/dev/null 2>&1; then
    bad "unknown --order should be rejected"
else ok "unknown --order is rejected"; fi

echo "== render_summary =="
TSV="$TMP/summary.tsv"
printf 'sig-node\tok\t100\t2\t102\t600\n' > "$TSV"
printf 'sig-apps\tok\t47\t1\t48\t300\n' >> "$TSV"
printf 'sig-cli\tINFRA\t0\t0\t0\t30\n' >> "$TSV"
OUT="$(render_summary "$TSV")"

check "totals sum passed/failed/total and duration in whole minutes" \
    "TOTAL 147 3 150 15m" \
    "$(awk '$1=="TOTAL"{print $1, $2, $3, $4, $5}' <<<"$OUT")"
check "an INFRA partition is rendered with its status" \
    "sig-cli INFRA 0 0 0 0m" "$(awk '$1=="sig-cli"{print $1, $2, $3, $4, $5, $6}' <<<"$OUT")"
check "header is present" "TARGET" "$(awk 'NR==1{print $1}' <<<"$OUT")"
check "one row per partition plus header and total" 5 "$(wc -l <<<"$OUT")"

# An empty run must still render a well-formed table — the driver calls
# render_summary after the first partition, and on a run that broke before any.
check "empty TSV renders header + zero total" \
    "TOTAL 0 0 0 0m" "$(: > "$TSV"; render_summary "$TSV" | awk '$1=="TOTAL"{print $1, $2, $3, $4, $5}')"

echo "== pass-throughs =="
# Every flag the driver forwards must reach conformance-target-run.sh verbatim.
# A flag silently swallowed here would run a DIFFERENT suite than asked for
# (e.g. --skip dropped means every partition keeps its serial/slow specs).
DRIVER="$REPO_ROOT/scripts/conformance-suite-run.sh"
for flag in --kubeconfig --conformance-image --hydrophone --parallel --skip --preflight-arg; do
    if grep -qE "^\s+.*\|?$flag\|" <<<"$(grep -A1 'Pass-throughs' "$DRIVER")" \
       || grep -qE "[|(]$flag[|)]" "$DRIVER"; then
        ok "$flag is forwarded to the target runner"
    else
        bad "$flag is not in the pass-through list"
    fi
done
check "--skip-preflight is forwarded as a valueless flag" \
    1 "$(grep -c -- '--skip-preflight) PASSTHRU+=("\$1"); shift ;;' "$DRIVER")"

echo "== status_for_exit =="
# 124 is `timeout`'s "I had to kill it" code and must not be read as a
# conformance result. Getting this wrong would report a wedged partition as a
# clean pass (exit 0) or hide it among ordinary infra failures.
check "exit 0 is ok"                    ok      "$(status_for_exit 0 5 100)"
check "exit 124 is TIMEOUT (SIGTERM was enough)"   TIMEOUT "$(status_for_exit 124 100 100)"
check "exit 1 is INFRA"                 INFRA   "$(status_for_exit 1 5 100)"
check "exit 2 is USAGE"                 USAGE   "$(status_for_exit 2 5 100)"
# 137 at/past the cap is --kill-after finishing the job; the same code well
# short of the cap is something else (an OOM kill) and must not be mislabelled.
check "exit 137 at the cap is TIMEOUT"  TIMEOUT "$(status_for_exit 137 120 100)"
check "exit 137 far below the cap is not TIMEOUT" USAGE "$(status_for_exit 137 5 100)"

echo "== --partition-timeout validation =="
DRIVER="$REPO_ROOT/scripts/conformance-suite-run.sh"
set +e; out=$(bash "$DRIVER" --partition-timeout abc --targets sig-cli 2>&1); rc=$?; set -e
if [ "$rc" -eq 2 ] && grep -q "whole seconds" <<<"$out"; then
    ok "a non-numeric --partition-timeout is rejected with exit 2"
else
    bad "a non-numeric --partition-timeout should exit 2 with a clear message (rc=$rc)"
fi
set +e; out=$(bash "$DRIVER" --partition-timeout 2>&1); rc=$?; set -e
if [ "$rc" -eq 2 ]; then ok "--partition-timeout with no value is rejected"; else bad "--partition-timeout with no value should exit 2 (rc=$rc)"; fi

echo "== the cap actually fires =="
# End-to-end: a stub target runner that hangs forever must be killed and
# recorded as TIMEOUT, and the run must continue rather than stall. Unit-testing
# status_for_exit alone would not have caught a missing `timeout` wrapper --
# which is the bug this whole change exists to fix.
STUB_DIR="$TMP/stub"; mkdir -p "$STUB_DIR/scripts/tests"
cp "$DRIVER" "$STUB_DIR/scripts/"
cat > "$STUB_DIR/scripts/conformance-target-run.sh" <<'STUB'
#!/usr/bin/env bash
# Hangs like a wedged hydrophone, and (like the real thing) ignores SIGTERM
# so --kill-after is what finally ends it.
trap '' TERM
sleep 300
STUB
chmod +x "$STUB_DIR/scripts/conformance-target-run.sh"
mkdir -p "$STUB_DIR/ci/conformance"; cp "$FIX" "$STUB_DIR/ci/conformance/targets.json"
run_out="$TMP/stub-run.log"
start=$(date +%s)
set +e
timeout 90 bash "$STUB_DIR/scripts/conformance-suite-run.sh" \
    --targets sig-node,sig-apps --partition-timeout 3 \
    --output-dir "$TMP/stub-out" --skip-preflight > "$run_out" 2>&1
stub_rc=$?
set -e
took=$(( $(date +%s) - start ))

if grep -q "exceeded 3s and was killed" "$run_out"; then ok "a hung partition is killed at the cap"; else bad "no kill message; log: $(tail -3 "$run_out")"; fi
check "the killed partition is recorded TIMEOUT" 2 "$(grep -c $'\tTIMEOUT\t' "$TMP/stub-out/summary.tsv" 2>/dev/null || echo 0)"
if [ "$stub_rc" -eq 1 ]; then ok "a timed-out run exits 1 (infra), not 0"; else bad "expected exit 1, got $stub_rc"; fi
# 2 partitions x 3s cap: must finish in well under the 300s the stub sleeps.
if [ "$took" -lt 120 ]; then ok "the run continued past the wedge (${took}s, stub sleeps 300s)"; else bad "run took ${took}s -- the cap did not release it"; fi
if grep -q "#1887" "$run_out"; then ok "the operator is pointed at the wedge diagnosis"; else bad "no #1887 pointer in the timeout message"; fi

echo
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
