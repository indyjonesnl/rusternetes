#!/usr/bin/env bash
# Unit tests for scripts/conformance-target-run.sh — no cluster required.
# Sources the script's lib helpers (TARGET_RUN_LIB_ONLY) and exercises the full
# CLI against a stubbed `hydrophone` that writes fixture junit files.
#
# Run with: bash scripts/tests/test-conformance-target-run.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
RUNNER="$REPO_ROOT/scripts/conformance-target-run.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0; failcnt=0
ok()   { echo "  ok: $*"; pass=$((pass + 1)); }
bad()  { echo "  FAIL: $*" >&2; failcnt=$((failcnt + 1)); }

[ -f "$RUNNER" ] || { echo "FAIL: runner missing: $RUNNER" >&2; exit 1; }

# ---- lib helper: target_counts ----
TARGET_RUN_LIB_ONLY=1 source "$RUNNER"

mkdir -p "$TMP/nojunit"
IFS=' ' read -r hj p f s t <<<"$(target_counts "$TMP/nojunit")"
[ "$hj" = "0" ] && [ "$t" = "0" ] && ok "target_counts: no junit => had_junit=0 total=0" \
    || bad "target_counts no-junit got hj=$hj total=$t"

mkdir -p "$TMP/counts"
cat > "$TMP/counts/junit_01.xml" <<'EOF'
<testsuite>
  <testcase name="a" status="passed"></testcase>
  <testcase name="b" status="passed"></testcase>
  <testcase name="c" status="failed"></testcase>
  <testcase name="d" status="skipped"></testcase>
</testsuite>
EOF
IFS=' ' read -r hj p f s t <<<"$(target_counts "$TMP/counts")"
[ "$hj" = "1" ] && [ "$p" = "2" ] && [ "$f" = "1" ] && [ "$s" = "1" ] && [ "$t" = "4" ] \
    && ok "target_counts: parses passed/failed/skipped/total" \
    || bad "target_counts got hj=$hj p=$p f=$f s=$s t=$t (want 1 2 1 1 4)"

# Ginkgo's junit carries suite-level nodes as testcases alongside the real
# specs. Upstream's own reporter can drop them (OmitSuiteSetupNodes, ginkgo
# reporters/junit_report.go:195 — `spec.LeafNodeType != types.NodeTypeIt`), and
# the set is types.NodeTypesForSuiteLevelNodes (types.go:885). Counting them
# inflates every target: sig-instrumentation reported 11/11 for a 4-spec SIG
# (#1643), and it defeats the 0-match guard, which keys off total==0.
mkdir -p "$TMP/synthetic"
cat > "$TMP/synthetic/junit_01.xml" <<'EOF'
<testsuite>
  <testcase name="[ReportBeforeSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[SynchronizedBeforeSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[SynchronizedBeforeSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[SynchronizedAfterSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[SynchronizedAfterSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[ReportAfterSuite] Invariant Metrics" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[ReportAfterSuite] Kubernetes e2e suite report" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[BeforeSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[AfterSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[DeferCleanup (Suite)]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[It] [sig-instrumentation] Events API should delete a collection of events [Conformance]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[It] [sig-instrumentation] Events should manage the lifecycle of an event [Conformance]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[It] [sig-instrumentation] Events broken spec [Conformance]" classname="Kubernetes e2e suite" status="failed"></testcase>
  <testcase name="[It] [sig-node] not in this focus" classname="Kubernetes e2e suite" status="skipped"></testcase>
</testsuite>
EOF
IFS=' ' read -r hj p f s t <<<"$(target_counts "$TMP/synthetic")"
[ "$hj" = "1" ] && [ "$p" = "2" ] && [ "$f" = "1" ] && [ "$s" = "1" ] && [ "$t" = "4" ] \
    && ok "target_counts: excludes ginkgo suite-level nodes" \
    || bad "target_counts synthetic got hj=$hj p=$p f=$f s=$s t=$t (want 1 2 1 1 4)"

# A run whose junit holds ONLY suite-level nodes matched no specs, whatever the
# run log says — total must be 0 so the caller's guard fires.
mkdir -p "$TMP/synthonly"
cat > "$TMP/synthonly/junit_01.xml" <<'EOF'
<testsuite>
  <testcase name="[ReportBeforeSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[SynchronizedBeforeSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[ReportAfterSuite] Kubernetes e2e suite report" classname="Kubernetes e2e suite" status="passed"></testcase>
</testsuite>
EOF
IFS=' ' read -r hj p f s t <<<"$(target_counts "$TMP/synthonly")"
[ "$hj" = "1" ] && [ "$p" = "0" ] && [ "$t" = "0" ] \
    && ok "target_counts: suite-level-only junit => total=0" \
    || bad "target_counts synth-only got hj=$hj p=$p total=$t (want 1 0 0)"

# ---- CLI fixtures ----
KC="$TMP/kubeconfig"; echo "kc" > "$KC"

# Stub hydrophone: --cleanup => noop; run => write fixture junit per FAKE_MODE.
FAKE="$TMP/hydrophone"
cat > "$FAKE" <<'EOF'
#!/usr/bin/env bash
out=""; prev=""
for a in "$@"; do
  [ "$prev" = "--output-dir" ] && out="$a"
  prev="$a"
done
case " $* " in *" --cleanup "*) exit 0 ;; esac
mode="${FAKE_MODE:-pass}"
[ -n "$out" ] && mkdir -p "$out"
case "$mode" in
  pass)  cat > "$out/junit_01.xml" <<'J'
<testsuite><testcase name="x" status="passed"></testcase><testcase name="y" status="failed"></testcase></testsuite>
J
  ;;
  empty) echo "Will run 0 of 7348 specs"
         cat > "$out/junit_01.xml" <<'J'
<testsuite>
  <testcase name="SynchronizedBeforeSuite" status="passed"></testcase>
  <testcase name="BeforeSuite" status="passed"></testcase>
  <testcase name="AfterSuite" status="passed"></testcase>
  <testcase name="skipped-spec" status="skipped"></testcase>
</testsuite>
J
  ;;
  # Ginkgo wrote its suite-level nodes but no spec ran, and the run log carries
  # no "Will run 0 of N specs" line (parallel runs word it differently). The
  # only signal left is the junit count — which must be 0, not 3.
  synthonly) cat > "$out/junit_01.xml" <<'J'
<testsuite>
  <testcase name="[ReportBeforeSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[SynchronizedBeforeSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[ReportAfterSuite] Kubernetes e2e suite report" classname="Kubernetes e2e suite" status="passed"></testcase>
</testsuite>
J
  ;;
  # Suite setup itself failed (BeforeSuite could not reach the cluster), so no
  # spec ran. Not "no tests matched" — the focus was fine, the cluster was not.
  setupfail) cat > "$out/junit_01.xml" <<'J'
<testsuite>
  <testcase name="[ReportBeforeSuite]" classname="Kubernetes e2e suite" status="passed"></testcase>
  <testcase name="[SynchronizedBeforeSuite]" classname="Kubernetes e2e suite" status="failed"></testcase>
  <testcase name="[ReportAfterSuite] Kubernetes e2e suite report" classname="Kubernetes e2e suite" status="passed"></testcase>
</testsuite>
J
  ;;
  nojunit) : ;;  # produce nothing
esac
exit 0
EOF
chmod +x "$FAKE"

GITHUB_OUTPUT="$TMP/github-output"
export GITHUB_OUTPUT
run_cli() { : > "$GITHUB_OUTPUT"; bash "$RUNNER" "$@"; }
gho() { grep -E "^$1=" "$GITHUB_OUTPUT" | tail -1 | cut -d= -f2-; }

# missing --target => exit 2
if run_cli --kubeconfig "$KC" --hydrophone "$FAKE" >/dev/null 2>&1; then bad "missing --target should exit 2"; else
  [ $? -eq 2 ] && ok "missing --target => exit 2" || bad "missing --target wrong exit"; fi

# unknown target => exit 2
if run_cli --target sig-nope --kubeconfig "$KC" --hydrophone "$FAKE" >/dev/null 2>&1; then bad "unknown target should exit 2"; else
  rc=$?; [ "$rc" -eq 2 ] && ok "unknown target => exit 2" || bad "unknown target exit=$rc"; fi

# missing kubeconfig => exit 2
if run_cli --target sig-node --kubeconfig "$TMP/nope" --hydrophone "$FAKE" >/dev/null 2>&1; then bad "missing kubeconfig should exit 2"; else
  rc=$?; [ "$rc" -eq 2 ] && ok "missing kubeconfig => exit 2" || bad "missing kubeconfig exit=$rc"; fi

# full run of a SIG target (pass mode): exit 0, passed=1 failed=1 total=2 focused=0
FAKE_MODE=pass run_cli --target sig-node --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o1" >/dev/null 2>&1 \
  && ok "full sig run exit 0" || bad "full sig run should exit 0"
[ "$(gho passed)" = "1" ] && [ "$(gho failed)" = "1" ] && [ "$(gho total)" = "2" ] && [ "$(gho focused)" = "0" ] \
  && ok "full run outputs passed=1 failed=1 total=2 focused=0" \
  || bad "full run outputs: passed=$(gho passed) failed=$(gho failed) total=$(gho total) focused=$(gho focused)"

# the FAILED-tests list names the failing spec (same exclusion as the counts)
set +e
out=$(FAKE_MODE=pass run_cli --target sig-node --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o1b" 2>&1)
set -e
echo "$out" | grep -q "FAILED tests:" && echo "$out" | grep -qE '^\s+- y$' \
  && ok "failed-spec list names the spec" \
  || bad "failed-spec list missing/wrong: $(echo "$out" | grep -A2 'FAILED tests' | tr '\n' ' ')"

# a FEATURE target resolves from the manifest too (identical path)
FAKE_MODE=pass run_cli --target sysctls --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/of" >/dev/null 2>&1 \
  && ok "feature target (sysctls) resolves + runs" || bad "feature target sysctls should exit 0"

# focus override => focused=1
FAKE_MODE=pass run_cli --target sig-node --focus 'x' --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o2" >/dev/null 2>&1
[ "$(gho focused)" = "1" ] && ok "--focus sets focused=1" || bad "--focus focused=$(gho focused)"

# no tests matched => exit 1, passed=0 total=0, "no tests matched" message
set +e
out=$(FAKE_MODE=empty run_cli --target sig-node --focus 'zzz' --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o3" 2>&1); rc=$?
set -e
[ "$rc" -eq 1 ] && echo "$out" | grep -qi "no tests matched" && [ "$(gho passed)" = "0" ] && [ "$(gho total)" = "0" ] \
  && ok "empty focus => exit 1 + 'no tests matched' + passed/total 0" \
  || bad "empty focus rc=$rc msg/counts wrong (passed=$(gho passed) total=$(gho total))"

# junit with suite-level nodes only => no false green, exit 1 with 0/0
set +e
out=$(FAKE_MODE=synthonly run_cli --target sig-node --focus 'zzz' --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o5" 2>&1); rc=$?
set -e
[ "$rc" -eq 1 ] && echo "$out" | grep -qi "no tests matched" && [ "$(gho passed)" = "0" ] && [ "$(gho total)" = "0" ] \
  && ok "suite-level-only junit => exit 1 + passed/total 0 (no false green)" \
  || bad "synth-only rc=$rc msg/counts wrong (passed=$(gho passed) total=$(gho total))"

# failed suite setup => exit 1, named as suite setup (not "no tests matched")
set +e
out=$(FAKE_MODE=setupfail run_cli --target sig-node --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o6" 2>&1); rc=$?
set -e
[ "$rc" -eq 1 ] && echo "$out" | grep -qi "suite setup failed" && echo "$out" | grep -q "SynchronizedBeforeSuite" \
  && [ "$(gho total)" = "0" ] \
  && ok "failed suite setup => exit 1 named as suite setup, with the node listed" \
  || bad "setupfail rc=$rc output/counts wrong: $(echo "$out" | tail -2 | tr '\n' ' ')"

# no junit (infra fail) => exit 1
if FAKE_MODE=nojunit run_cli --target sig-node --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o4" >/dev/null 2>&1; then
  bad "no junit should exit 1"; else rc=$?; [ "$rc" -eq 1 ] && ok "no junit => exit 1" || bad "no junit exit=$rc"; fi

echo
if [ "$failcnt" -eq 0 ]; then echo "PASS: conformance-target-run ($pass checks)"; else echo "FAILED: $failcnt of $((pass + failcnt))" >&2; exit 1; fi
