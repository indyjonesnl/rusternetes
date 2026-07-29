#!/usr/bin/env bash
# TDD test for the isolation-target registry parser/validator
# (vs_validate_registry / vs_resolve_target in vanilla-swap-common.sh).
# Pure logic — no cluster required. Run: bash scripts/vanilla-swap-common-test.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/vanilla-swap-common.sh
source "$SCRIPT_DIR/vanilla-swap-common.sh"

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
fails=0
ok()   { printf 'ok   - %s\n' "$1"; }
bad()  { printf 'FAIL - %s\n' "$1"; fails=$((fails+1)); }

# --- the shipped registry is valid ----------------------------------------
if vs_validate_registry "$SCRIPT_DIR/../ci/vanilla-swap/targets.json" 2>/dev/null; then
  ok "shipped registry validates"
else
  bad "shipped registry should validate"
fi

# helper: write a registry JSON to $TMP/reg.json and echo the path
mkreg() { printf '%s' "$1" >"$TMP/reg.json"; printf '%s\n' "$TMP/reg.json"; }

# --- render a recipe without envsubst --------------------------------------
RENDER_RECIPE="$TMP/render-recipe.yaml"
cat >"$RENDER_RECIPE" <<'YAML'
template: |
  image: ${VS_IMAGE}
  server: ${VS_APISERVER_URL}
  cidr: ${VS_CLUSTER_CIDR}
  nodePorts: ${VS_NODEPORT_RANGE}
  untouched: ${NOT_ALLOWED}
nextField: outside-template
YAML

AWK_ONLY_BIN="$TMP/awk-only-bin"
mkdir -p "$AWK_ONLY_BIN"
ln -s "$(command -v awk)" "$AWK_ONLY_BIN/awk"

VS_IMAGE="ghcr.io/indyjonesnl/rusternetes/kube-proxy:test"
VS_APISERVER_URL="https://172.18.0.3:6443"
VS_CLUSTER_CIDR="10.96.0.0/16"
VS_NODEPORT_RANGE="30000-32767"
EXPECTED_RENDER='image: ghcr.io/indyjonesnl/rusternetes/kube-proxy:test
server: https://172.18.0.3:6443
cidr: 10.96.0.0/16
nodePorts: 30000-32767
untouched: ${NOT_ALLOWED}'

if RENDERED="$(PATH="$AWK_ONLY_BIN" vs_render_recipe_template "$RENDER_RECIPE" \
  VS_IMAGE VS_APISERVER_URL VS_CLUSTER_CIDR VS_NODEPORT_RANGE)" \
  && [ "$RENDERED" = "$EXPECTED_RENDER" ]; then
  ok "recipe template renders without envsubst and preserves unlisted variables"
else
  bad "recipe template should render with only awk available"
fi

# --- render values in one pass --------------------------------------------
VS_IMAGE='ghcr.io/example/kube-proxy:${VS_APISERVER_URL}'
EXPECTED_ONE_PASS_RENDER='image: ghcr.io/example/kube-proxy:${VS_APISERVER_URL}
server: https://172.18.0.3:6443
cidr: 10.96.0.0/16
nodePorts: 30000-32767
untouched: ${NOT_ALLOWED}'

if RENDERED="$(vs_render_recipe_template "$RENDER_RECIPE" \
  VS_IMAGE VS_APISERVER_URL VS_CLUSTER_CIDR VS_NODEPORT_RANGE)" \
  && [ "$RENDERED" = "$EXPECTED_ONE_PASS_RENDER" ]; then
  ok "recipe template does not recursively render placeholder values"
else
  bad "recipe template should render placeholder values in one pass"
fi

# --- unset requested template variable rejected ---------------------------
unset VS_UNSET_RENDER_VALUE
if (set +u; vs_render_recipe_template "$RENDER_RECIPE" VS_UNSET_RENDER_VALUE) \
  >"$TMP/unset-render.out" 2>"$TMP/unset-render.err"; then
  bad "unset requested template variable should be rejected"
else
  ok "unset requested template variable rejected"
fi

# --- duplicate module rejected --------------------------------------------
DUP="$(mkreg '[
 {"module":"kubelet","swap":"join-worker","recipe":"ci/vanilla-swap/kind/kubelet-node.yaml","readiness":"node-ready"},
 {"module":"kubelet","swap":"join-worker","recipe":"ci/vanilla-swap/kind/kubelet-node.yaml","readiness":"node-ready"}
]')"
if vs_validate_registry "$DUP" 2>/dev/null; then bad "duplicate module should be rejected"; else ok "duplicate module rejected"; fi

# --- unknown module rejected ----------------------------------------------
UNK="$(mkreg '[{"module":"bogus","swap":"static-pod","recipe":"ci/vanilla-swap/kind/apiserver-patch.yaml","readiness":"readyz"}]')"
if vs_validate_registry "$UNK" 2>/dev/null; then bad "unknown module should be rejected"; else ok "unknown module rejected"; fi

# --- missing recipe file rejected -----------------------------------------
MISS="$(mkreg '[{"module":"kubelet","swap":"join-worker","recipe":"ci/vanilla-swap/kind/nope.yaml","readiness":"node-ready"}]')"
if vs_validate_registry "$MISS" 2>/dev/null; then bad "missing recipe file should be rejected"; else ok "missing recipe file rejected"; fi

# --- wrong entry count rejected (not exactly 5) ---------------------------
ONE="$(mkreg '[{"module":"kubelet","swap":"join-worker","recipe":"ci/vanilla-swap/kind/kubelet-node.yaml","readiness":"node-ready"}]')"
if vs_validate_registry "$ONE" 2>/dev/null; then bad "registry with != 5 entries should be rejected"; else ok "non-5 entry count rejected"; fi

# --- resolve a real target ------------------------------------------------
if vs_resolve_target "kubelet" "$SCRIPT_DIR/../ci/vanilla-swap/targets.json" 2>/dev/null \
   && [ "$VS_MODULE" = "kubelet" ] && [ "$VS_SWAP" = "join-worker" ] \
   && [ "$VS_TARGET" = "sig-node" ] && [ "$VS_READINESS" = "node-ready" ]; then
  ok "resolve kubelet sets expected fields"
else
  bad "resolve kubelet should set module/swap/target/readiness"
fi

# --- resolve unknown module fails -----------------------------------------
if vs_resolve_target "bogus" "$SCRIPT_DIR/../ci/vanilla-swap/targets.json" 2>/dev/null; then
  bad "resolving unknown module should fail"
else
  ok "resolving unknown module fails"
fi


# --- junit counting excludes ginkgo suite-level nodes ----------------------
# The badge is computed from these numbers, so counting ginkgo's suite-level
# nodes as specs publishes a lie: the scheduler leg ran 2 specs and published
# 100% (9/9) — 2 real + 7 [ReportBeforeSuite]/[SynchronizedBeforeSuite]/...
# entries (#1643 again, via the junit suite header instead of per-testcase).
mkjunit() {  # mkjunit <dir> <body> <tests> <failures> <skipped>
  mkdir -p "$1"
  # Header shaped like hydrophone's real file: <testsuites tests=… disabled=…>
  # first, then the <testsuite> with skipped= (this header is what the old
  # implementation trusted).
  { printf '<?xml version="1.0" encoding="UTF-8"?>\n<testsuites tests="%s" disabled="%s" errors="0" failures="%s">\n' "$3" "$5" "$4"
    printf '<testsuite name="Kubernetes e2e suite" tests="%s" disabled="0" skipped="%s" errors="0" failures="%s">\n' "$3" "$5" "$4"
    printf '%s\n' "$2"
    printf '</testsuite></testsuites>\n'
  } >"$1/junit_01.xml"
}

SYNTH='  <testcase name="[ReportBeforeSuite]" status="passed"></testcase>
  <testcase name="[SynchronizedBeforeSuite]" status="passed"></testcase>
  <testcase name="[SynchronizedBeforeSuite]" status="passed"></testcase>
  <testcase name="[SynchronizedAfterSuite]" status="passed"></testcase>
  <testcase name="[SynchronizedAfterSuite]" status="passed"></testcase>
  <testcase name="[ReportAfterSuite] Invariant Metrics" status="passed"></testcase>
  <testcase name="[ReportAfterSuite] Kubernetes e2e suite report" status="passed"></testcase>'

mkjunit "$TMP/j-pass" "$SYNTH
  <testcase name=\"[It] [sig-scheduling] a spec [Conformance]\" status=\"passed\"></testcase>
  <testcase name=\"[It] [sig-scheduling] another spec [Conformance]\" status=\"passed\"></testcase>
  <testcase name=\"[It] [sig-scheduling] skipped one\" status=\"skipped\"></testcase>" 10 0 1
got="$(vs_junit_counts "$TMP/j-pass")"
[ "$got" = "2 0" ] && ok "vs_junit_counts: 2 real specs, suite-level nodes excluded" \
  || bad "vs_junit_counts pass-case got '$got' (want '2 0')"

mkjunit "$TMP/j-fail" "$SYNTH
  <testcase name=\"[It] [sig-scheduling] a spec [Conformance]\" status=\"passed\"></testcase>
  <testcase name=\"[It] [sig-scheduling] broken spec [Conformance]\" status=\"failed\"></testcase>" 9 1 0
got="$(vs_junit_counts "$TMP/j-fail")"
[ "$got" = "2 1" ] && ok "vs_junit_counts: counts real failures, excludes suite-level nodes" \
  || bad "vs_junit_counts fail-case got '$got' (want '2 1')"

mkjunit "$TMP/j-synth" "$SYNTH" 7 0 0
got="$(vs_junit_counts "$TMP/j-synth")"
[ "$got" = "0 0" ] && ok "vs_junit_counts: suite-level-only junit => 0 0 (no false green)" \
  || bad "vs_junit_counts synth-only got '$got' (want '0 0')"

mkdir -p "$TMP/j-none"
if vs_junit_counts "$TMP/j-none" >/dev/null 2>&1; then
  bad "vs_junit_counts should fail when no junit is present"
else
  ok "vs_junit_counts: no junit => non-zero"
fi


# --- verdict: a run where NO spec executed is never test-passed -------------
# The kube-proxy leg swapped in fine, then the e2e framework's
# [SynchronizedBeforeSuite] failed after 600s ("Error waiting for all pods to be
# running and ready" — the vanilla control-plane static pods went unready behind
# the swapped proxy) and ginkgo ran 0 of 7348 specs. With suite-level nodes no
# longer counted the numbers are 0/0, and the verdict must say so instead of
# reporting a pass over an empty set.
got="$(vs_verdict 0 0 1)"
[ "$got" = "module-did-not-come-up $VS_EX_NOTUP" ] \
  && ok "vs_verdict: 0 specs + runner failure => module-did-not-come-up" \
  || bad "vs_verdict 0/0 rc=1 got '$got' (want 'module-did-not-come-up $VS_EX_NOTUP')"

got="$(vs_verdict 0 0 0)"
[ "$got" = "no-result $VS_EX_NOTUP" ] \
  && ok "vs_verdict: 0 specs, runner clean => no-result (focus selected nothing)" \
  || bad "vs_verdict 0/0 rc=0 got '$got' (want 'no-result $VS_EX_NOTUP')"

got="$(vs_verdict 11 2 1)"
[ "$got" = "test-failed $VS_EX_TESTFAIL" ] \
  && ok "vs_verdict: real failures => test-failed" \
  || bad "vs_verdict 11/2 got '$got' (want 'test-failed $VS_EX_TESTFAIL')"

got="$(vs_verdict 11 0 0)"
[ "$got" = "test-passed 0" ] \
  && ok "vs_verdict: all specs green => test-passed" \
  || bad "vs_verdict 11/0 got '$got' (want 'test-passed 0')"

# A module that came up and passed its specs is still a pass even if the runner
# was killed afterwards (hung post-test cleanup) — junit already has the verdict.
got="$(vs_verdict 11 0 124)"
[ "$got" = "test-passed 0" ] \
  && ok "vs_verdict: specs green but runner killed late => still test-passed" \
  || bad "vs_verdict 11/0 rc=124 got '$got' (want 'test-passed 0')"


# --- suite timeout must fit the job budget AND the biggest leg --------------
# The controller-manager leg died on the old 1200s default: 6m44s of framework
# startup, then killed part-way through 52 sig-apps specs, with junit still
# inside the conformance pod — no counts, no badge. Both sides of the budget
# live in different files, so guard the relationship.
if vs_test_budget_ok 4500 90 15; then ok "vs_test_budget_ok: 75-min suite fits a 90-min job"; else bad "4500s/90min should fit"; fi
if vs_test_budget_ok 5400 90 15; then bad "90-min suite must NOT fit a 90-min job"; else ok "vs_test_budget_ok: suite == job budget rejected"; fi
if vs_test_budget_ok 0 90 15; then bad "zero suite timeout should be rejected"; else ok "vs_test_budget_ok: zero suite timeout rejected"; fi

VS_RUN="$SCRIPT_DIR/vanilla-swap-run.sh"
VS_WF="$SCRIPT_DIR/../.github/workflows/vanilla-swap-module.yml"
suite_default="$(grep -oE 'VS_TEST_TIMEOUT:-[0-9]+' "$VS_RUN" | head -1 | grep -oE '[0-9]+')"
job_minutes="$(grep -oE 'timeout-minutes: [0-9]+' "$VS_WF" | head -1 | grep -oE '[0-9]+')"
if [ -n "$suite_default" ] && [ -n "$job_minutes" ] && vs_test_budget_ok "$suite_default" "$job_minutes" 15; then
  ok "shipped suite timeout (${suite_default}s) fits the job budget (${job_minutes}min)"
else
  bad "suite timeout ${suite_default:-?}s does not fit job budget ${job_minutes:-?}min (leave >=15min for bring-up/swap/teardown)"
fi
# The largest leg is the kubelet's 191 NodeConformance specs, ~20 min in
# node-conformance.yml, and ginkgo's startup alone costs ~7 min.
if [ -n "$suite_default" ] && [ "$suite_default" -ge 2400 ]; then
  ok "shipped suite timeout leaves room for the biggest leg"
else
  bad "suite timeout ${suite_default:-?}s is too small for a real subset (need >=2400s)"
fi


# --- stdout counter fallback must survive a runner that printed no counters -
# In CI, GITHUB_OUTPUT is set for the whole job, so conformance-target-run.sh
# writes passed=/failed=/total= THERE and not to stdout. The driver's fallback
# grepped stdout for those lines and, under `set -euo pipefail`, a grep that
# matched nothing killed the driver before it could write run-result.json — so
# every leg without junit (timeout, hydrophone infra failure) reported
# `no-result` and published no badge at all. Runs 30433160080 and 30436964889.
NO_COUNTERS="$TMP/no-counters.out"
printf '[conformance-target-run] target=x hydrophone_exit=1 — NO junit produced (infra failure)\n' >"$NO_COUNTERS"
if got="$(vs_stdout_counts "$NO_COUNTERS")"; then
  [ "$got" = "0 0 0" ] && ok "vs_stdout_counts: no counter lines => 0 0 0 (and does not fail)" \
    || bad "vs_stdout_counts no-counters got '$got' (want '0 0 0')"
else
  bad "vs_stdout_counts must not return non-zero when counters are absent"
fi

WITH_COUNTERS="$TMP/with-counters.out"
printf 'noise\npassed=9\nfailed=2\ntotal=11\nmore noise\n' >"$WITH_COUNTERS"
got="$(vs_stdout_counts "$WITH_COUNTERS")"
[ "$got" = "9 2 11" ] && ok "vs_stdout_counts: parses passed/failed/total" \
  || bad "vs_stdout_counts got '$got' (want '9 2 11')"

# Last occurrence wins (the runner prints its summary once, but a refocused
# re-run inside one file must not be averaged with the first).
printf 'passed=1\nfailed=0\ntotal=1\npassed=4\nfailed=1\ntotal=5\n' >"$WITH_COUNTERS"
got="$(vs_stdout_counts "$WITH_COUNTERS")"
[ "$got" = "4 1 5" ] && ok "vs_stdout_counts: last counter block wins" \
  || bad "vs_stdout_counts repeat got '$got' (want '4 1 5')"

got="$(vs_stdout_counts "$TMP/definitely-not-here.out")"
[ "$got" = "0 0 0" ] && ok "vs_stdout_counts: missing file => 0 0 0" \
  || bad "vs_stdout_counts missing-file got '$got' (want '0 0 0')"

# And the driver must ask the runner for stdout counters in the first place:
# leaving GITHUB_OUTPUT set sends them to the Actions file where the driver
# cannot see them.
if grep -qE 'env -u GITHUB_OUTPUT|GITHUB_OUTPUT= ' "$SCRIPT_DIR/vanilla-swap-run.sh"; then
  ok "driver clears GITHUB_OUTPUT for the runner so counters reach stdout"
else
  bad "driver must clear GITHUB_OUTPUT when invoking conformance-target-run.sh"
fi

echo "---"
[ "$fails" -eq 0 ] && { echo "PASS: all registry-parser tests"; exit 0; } || { echo "FAIL: $fails test(s)"; exit 1; }
