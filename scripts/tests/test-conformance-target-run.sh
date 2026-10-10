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
# Run the count assertion under EVERY awk on this machine. The exclusion was
# first written as a dynamic regex, which gawk honours and mawk (the awk on
# ubuntu-latest, and on Debian/Ubuntu generally) silently reinterprets — so it
# passed locally and excluded nothing in CI. Whichever awk a runner ships, the
# counts must come out the same.
for awk_impl in awk gawk mawk busybox-awk; do
    case "$awk_impl" in
        busybox-awk) command -v busybox >/dev/null 2>&1 || continue ;;
        *) command -v "$awk_impl" >/dev/null 2>&1 || continue ;;
    esac
    shim="$TMP/awk-shim-$awk_impl"; mkdir -p "$shim"
    if [ "$awk_impl" = "busybox-awk" ]; then
        printf '#!/bin/sh\nexec busybox awk "$@"\n' > "$shim/awk"
    else
        printf '#!/bin/sh\nexec %s "$@"\n' "$(command -v "$awk_impl")" > "$shim/awk"
    fi
    chmod +x "$shim/awk"
    IFS=' ' read -r hj p f s t <<<"$(PATH="$shim:$PATH" bash -c "
        TARGET_RUN_LIB_ONLY=1 source '$RUNNER' && target_counts '$TMP/synthetic'")"
    [ "$hj" = "1" ] && [ "$p" = "2" ] && [ "$f" = "1" ] && [ "$s" = "1" ] && [ "$t" = "4" ] \
        && ok "target_counts under $awk_impl: excludes ginkgo suite-level nodes" \
        || bad "target_counts synthetic under $awk_impl got hj=$hj p=$p f=$f s=$s t=$t (want 1 2 1 1 4)"
done

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
  # A wedged hydrophone (#1635): never returns (40s stands in for forever; the
  # assertions below require the kill to land well inside that). hangjunit has
  # already written its junit (suite finished, the log follow never hit EOF).
  hang) sleep 40 ;;
  hangjunit) cat > "$out/junit_01.xml" <<'J'
<testsuite><testcase name="x" status="passed"></testcase></testsuite>
J
  sleep 40 ;;
esac
exit 0
EOF
chmod +x "$FAKE"

GITHUB_OUTPUT="$TMP/github-output"
export GITHUB_OUTPUT

# The cases below exercise argument handling and junit counting, not cluster
# health, and this file promises "no cluster required". The preflight gate
# added for #1777 would otherwise refuse every one of them. Disable it by
# default here and test the gate itself explicitly at the end of the file —
# silencing it without covering it would leave the new wiring untested.
export SKIP_PREFLIGHT=1

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

# a second manifest target resolves through the identical path. This used to
# assert it for a kind:feature target (sysctls), but the two feature targets were
# removed in #1796 — their focus regexes matched ZERO specs on v1.35 while the
# badge logic reported 100% for 0/0. The manifest schema still accepts
# kind:feature and the generator/engine treat both kinds identically, so nothing
# else changed.
FAKE_MODE=pass run_cli --target sig-storage --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/of" >/dev/null 2>&1 \
  && ok "second manifest target (sig-storage) resolves + runs" || bad "sig-storage target should exit 0"

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

# ---- the preflight gate itself (#1777) ----
# With the gate ENABLED and no cluster reachable, the runner must refuse to run
# rather than emit a failure list that describes the environment instead of the
# code. conformance-preflight.sh exits 2 as soon as /healthz is unreachable —
# or, where kubectl is absent, at its `command -v` check — so this stays fast
# and needs no cluster in either environment.
if SKIP_PREFLIGHT=0 FAKE_MODE=pass run_cli --target sig-node --kubeconfig "$KC" \
     --hydrophone "$FAKE" --output-dir "$TMP/opf" >/dev/null 2>&1; then
  bad "preflight gate should refuse to run against an unreachable cluster"
else
  rc=$?; [ "$rc" -eq 2 ] && ok "preflight gate: unusable cluster => exit 2" \
    || bad "preflight gate exit=$rc (want 2)"
fi

# ...and --skip-preflight must override it, so an operator who knows the gate
# is wrong for their environment can still run. Gate left enabled in the env so
# it is the FLAG being tested, not the variable.
if SKIP_PREFLIGHT=0 FAKE_MODE=pass run_cli --target sig-node --skip-preflight \
     --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/opf2" >/dev/null 2>&1; then
  ok "--skip-preflight overrides the gate"
else
  bad "--skip-preflight should allow the run to proceed (exit=$?)"
fi

# ...and the gate must be tunable per cluster shape, not only on/off. The
# preflight's control-plane pod names and DNS Deployment are compose-stack
# facts; vanilla-swap drives this runner against a kind cluster and has to say
# so. Without a pass-through, every vanilla-swap leg reported
# "module-did-not-come-up" on three preflight FAILs about pods that only exist
# in the compose stack.
set +e
out_pfarg=$(SKIP_PREFLIGHT=0 FAKE_MODE=pass run_cli --target sig-node \
    --preflight-arg --dns-deployment --preflight-arg none \
    --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/opf3" 2>&1)
rc_pfarg=$?
set -e
[ "$rc_pfarg" -eq 2 ] || bad "--preflight-arg run exit=$rc_pfarg (want 2: cluster still unreachable)"
echo "$out_pfarg" | grep -q -- "--dns-deployment none" \
  && ok "--preflight-arg is forwarded to conformance-preflight.sh" \
  || bad "--preflight-arg was not forwarded (runner never named it)"

# An unknown flag is still a usage error — the pass-through must not swallow
# typos by forwarding everything.
if run_cli --target sig-node --not-a-flag --kubeconfig "$KC" --hydrophone "$FAKE" >/dev/null 2>&1; then
  bad "unknown flag should exit 2"; else
  rc=$?; [ "$rc" -eq 2 ] && ok "unknown flag => exit 2" || bad "unknown flag exit=$rc"; fi

# --- component logs are captured on a failing run (#1824) -------------------
# An intermittent api-server failure is unactionable without the api-server's
# own log, and the results artifact is the only place a nightly's reader will
# find it. The teardown step's `compose logs --tail=100` goes to the job log,
# truncated, and not into the artifact.
RUN_SRC="$(cat "$RUNNER")"
case "$RUN_SRC" in
  *"capture_component_logs()"*) ok "conformance-target-run defines capture_component_logs" ;;
  *) bad "conformance-target-run must define capture_component_logs" ;;
esac
# It has to be wired into the FAILED path, not merely defined.
FAILBLOCK="$(sed -n '/if \[ "\$FAILED" -gt 0 \]/,/^fi$/p' "$RUNNER")"
case "$FAILBLOCK" in
  *capture_component_logs*) ok "failing runs capture the component logs" ;;
  *) bad "the FAILED branch must call capture_component_logs" ;;
esac

# The helper must write into the results dir (so it rides along in the artifact)
# and be best-effort — it runs on a cluster that is by definition unhealthy.
HELPER="$(sed -n '/^capture_component_logs()/,/^}$/p' "$RUNNER")"
case "$HELPER" in
  *'command -v "$runtime"'*) ok "capture_component_logs tolerates a missing container runtime" ;;
  *) bad "capture_component_logs must check the runtime exists before using it" ;;
esac
case "$HELPER" in
  *'[ -n "$out" ] && [ -d "$out" ] || return 0'*) ok "capture_component_logs tolerates a missing output dir" ;;
  *) bad "capture_component_logs must tolerate a missing/empty output dir" ;;
esac
case "$HELPER" in
  *'return 0'*) ok "capture_component_logs always returns success (best-effort)" ;;
  *) bad "capture_component_logs must never fail the run" ;;
esac
case "$HELPER" in
  *api-server*) ok "capture_component_logs captures the api-server log" ;;
  *) bad "capture_component_logs must capture the api-server log" ;;
esac

# --- a hung hydrophone is killed and the run still reports (#1635) ----------
# hydrophone follows the conformance pod's logs to detect completion; when that
# never hits EOF it blocks forever and the job burns its whole budget (and, when
# the runner is lost, GitHub's timeout-minutes cannot save it). The script must
# bound the call itself and carry on to junit parsing / diagnostics.
if ! command -v timeout >/dev/null 2>&1; then
  ok "SKIP hang tests: no coreutils timeout on this machine"
else
  t0=$(date +%s)
  set +e
  out_hang=$(HYDROPHONE_TIMEOUT=2 HYDROPHONE_KILL_AFTER=1 FAKE_MODE=hang run_cli --target sig-node \
      --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/ohang" 2>&1)
  rc_hang=$?
  set -e
  elapsed=$(( $(date +%s) - t0 ))
  [ "$elapsed" -lt 20 ] && ok "hung hydrophone is killed (took ${elapsed}s)" \
    || bad "hung hydrophone not bounded (took ${elapsed}s)"
  [ "$rc_hang" -eq 1 ] && ok "hang with no junit => exit 1 (infra failure)" \
    || bad "hang with no junit exit=$rc_hang (want 1)"
  echo "$out_hang" | grep -q "hydrophone timed out" \
    && ok "timeout is named in the log" || bad "timeout not reported: $out_hang"
  echo "$out_hang" | grep -q "NO junit produced" \
    && ok "script proceeds past the kill to the junit verdict" \
    || bad "script did not reach the junit verdict after the kill"

  # Suite had finished: junit exists, only the log follow hung. Still a result.
  set +e
  out_hj=$(HYDROPHONE_TIMEOUT=2 HYDROPHONE_KILL_AFTER=1 FAKE_MODE=hangjunit run_cli --target sig-node \
      --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/ohangj" 2>&1)
  rc_hj=$?
  set -e
  [ "$rc_hj" -eq 0 ] && [ "$(gho passed)" = "1" ] \
    && ok "hang after junit written => results salvaged (exit 0, passed=1)" \
    || bad "hang-after-junit exit=$rc_hj passed=$(gho passed): $out_hj"

  # Default must fit inside the workflow's timeout-minutes with bring-up room.
  def="$(grep -oE 'HYDROPHONE_TIMEOUT:-[0-9]+' "$RUNNER" | head -1 | grep -oE '[0-9]+$')"
  job="$(grep -oE 'timeout-minutes: [0-9]+' "$REPO_ROOT/.github/workflows/conformance-target.yml" | head -1 | grep -oE '[0-9]+')"
  if [ -n "$def" ] && [ -n "$job" ] && [ "$def" -le $(( job * 60 - 1800 )) ]; then
    ok "default HYDROPHONE_TIMEOUT (${def}s) leaves >=30min of the ${job}min job budget"
  else
    bad "default HYDROPHONE_TIMEOUT '${def:-unset}' must be <= job budget ${job:-?}min minus 30min"
  fi
fi

echo
if [ "$failcnt" -eq 0 ]; then echo "PASS: conformance-target-run ($pass checks)"; else echo "FAILED: $failcnt of $((pass + failcnt))" >&2; exit 1; fi
