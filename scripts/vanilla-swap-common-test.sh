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


# --- a readiness timeout must leave evidence behind -------------------------
# The kubelet leg (run 30438506580) waited 180s for node-ready and reported
# `module-did-not-come-up` after three minutes of total silence — a red verdict
# nobody can act on. vs_wait_ready now dumps state before giving up.
STUB_BIN="$TMP/diag-bin"; mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/kubectl" <<'STUB'
#!/bin/sh
echo "STUB-kubectl $*"
exit 0
STUB
# `docker ps` must yield a container name so the log loop has something to read;
# any other subcommand just echoes. Guessing fixed names is what failed before.
cat >"$STUB_BIN/docker" <<'STUB'
#!/bin/sh
echo "STUB-docker $*"
case "$1" in
  ps) echo "vanilla-swap-vanilla-swap-kubelet-rusternetes-node" ;;
  logs) echo "STUB-kubelet-log-line" ;;
esac
exit 0
STUB
chmod +x "$STUB_BIN/kubectl" "$STUB_BIN/docker"

diag="$(VS_READINESS=node-ready VS_MODULE=kubelet PATH="$STUB_BIN:$PATH" \
  vs_dump_readiness_diagnostics vanilla-swap-kubelet /dev/null 2>&1)"
for want in "nodes" "describe node rusternetes-node" "harness containers" "swapped module logs" "STUB-docker ps -a --filter name=vanilla-swap-vanilla-swap-kubelet"; do
  case "$diag" in
    *"$want"*) ok "readiness diagnostics include: $want" ;;
    *) bad "readiness diagnostics missing '$want'" ;;
  esac
done

case "$diag" in
  *"[container vanilla-swap-vanilla-swap-kubelet-rusternetes-node]"*STUB-kubelet-log-line*)
    ok "readiness diagnostics dump logs for each enumerated container" ;;
  *) bad "readiness diagnostics must dump logs for containers found via docker ps" ;;
esac

# It runs against a cluster that is by definition unhealthy, so a failing probe
# must not become the reported failure.
cat >"$STUB_BIN/kubectl" <<'STUB'
#!/bin/sh
echo "boom" >&2
exit 7
STUB
chmod +x "$STUB_BIN/kubectl"
if VS_READINESS=readyz VS_MODULE=api-server PATH="$STUB_BIN:$PATH" \
     vs_dump_readiness_diagnostics vanilla-swap-api-server /dev/null >/dev/null 2>&1; then
  ok "readiness diagnostics survive failing probes"
else
  bad "readiness diagnostics must not fail when the cluster is broken"
fi

# And the wait itself must call them on timeout, not just return.
if grep -q 'vs_dump_readiness_diagnostics' <(sed -n '/^vs_wait_ready()/,/^}/p' "$SCRIPT_DIR/vanilla-swap-common.sh"); then
  ok "vs_wait_ready dumps diagnostics before returning VS_EX_NOTUP"
else
  bad "vs_wait_ready must dump diagnostics on timeout"
fi


# --- the node kubeconfig must not be bind-mounted from the host -------------
# `-v <host-path>:...` is resolved by the docker DAEMON. Under DinD (every CI job
# here) the daemon lives in another container, $VS_WORKDIR is not on its
# filesystem, and Docker creates a DIRECTORY at the mount source — so the kubelet
# died with `Failed to read kubeconfig from "/kc/admin.conf": Is a directory`
# and the node never registered (run 30439714160). It worked locally on a single
# daemon, so only CI ever saw it. docker cp goes through the daemon API instead.
START_NODE="$(sed -n '/^vs_swap_join_worker()/,/^}/p' "$SCRIPT_DIR/vanilla-swap-common.sh")"
[ -n "$START_NODE" ] || bad "could not extract vs_swap_join_worker (renamed? these assertions are scoped to it)"

case "$START_NODE" in
  *'-v "${kc}'*|*'-v "$kc'*)
    bad "node kubeconfig must not be bind-mounted from a host path (breaks under DinD)" ;;
  *) ok "node kubeconfig is not bind-mounted from the host" ;;
esac

case "$START_NODE" in
  *'docker cp "$kc"'*) ok "node kubeconfig is copied in with docker cp" ;;
  *) bad "node kubeconfig must be injected with docker cp (daemon-agnostic)" ;;
esac

# docker cp cannot create parent directories, so the destination has to live
# under a path the image already has.
case "$START_NODE" in
  *':/app/admin.conf'*) ok "kubeconfig destination is under /app (exists in the image)" ;;
  *) bad "kubeconfig destination must be a path that exists in the image" ;;
esac

# And a module that dies on startup must be reported immediately, not 180s later
# through a readiness timeout.
case "$START_NODE" in
  *'{{.State.Running}}'*) ok "start checks the container stayed up" ;;
  *) bad "vs_start_node must fail fast when the container exits immediately" ;;
esac


# --- the co-located kubelet must be told where its CRI stream server is -----
# The kubelet PROXIES exec/attach upgrades itself to
# stream_target() = ($CONTAINERD_STREAM_HOST, $CONTAINERD_STREAM_PORT), whose host
# defaults to the DNS name "containerd" (crates/cri/src/stream.rs). That name only
# resolves where the runtime is a compose service literally called `containerd`
# (compose.node-conformance.yml). In THIS harness the runtime container is
# vanilla-swap-<cluster>-containerd and the kubelet shares its netns, so the
# default does not resolve and every exec dies:
#
#   exec_proxy: ... target=http://containerd:10010/exec/wvXcjx6u
#   proxy_upgrade: backend request failed: client error (Connect)
#   $ kubectl exec exec-probe -- echo hello
#   error: error stream protocol error: unknown error
#
# compose.sqlite.yml sets localhost per node for exactly this reason (#1695).
# Reproduced locally against a pod pinned to the swapped node; #1708.
JOIN_WORKER="$(sed -n '/^vs_swap_join_worker()/,/^}/p' "$SCRIPT_DIR/vanilla-swap-common.sh")"
[ -n "$JOIN_WORKER" ] || bad "could not extract vs_swap_join_worker"

case "$JOIN_WORKER" in
  *CONTAINERD_STREAM_HOST*) ok "swapped kubelet is given a CRI stream host" ;;
  *) bad "swapped kubelet must set CONTAINERD_STREAM_HOST (else exec/attach dial the unresolvable name 'containerd')" ;;
esac

# It shares the runtime's network namespace, so loopback is the correct answer —
# and the only one that cannot depend on docker DNS.
case "$JOIN_WORKER" in
  *'CONTAINERD_STREAM_HOST=localhost'*) ok "CRI stream host is localhost (kubelet shares the runtime netns)" ;;
  *) bad "CONTAINERD_STREAM_HOST must be localhost for a netns-sharing kubelet" ;;
esac

# The stacks whose kubelet is NOT co-located keep relying on the DNS-name default,
# so it must not be changed globally.
NC_COMPOSE="$SCRIPT_DIR/../compose.node-conformance.yml"
if [ -f "$NC_COMPOSE" ]; then
  if grep -q "network_mode" "$NC_COMPOSE"; then
    bad "compose.node-conformance.yml now shares a netns — revisit the stream-host default"
  else
    ok "node-conformance kubelet still has its own netns (default 'containerd' must stay)"
  fi
fi


# --- test pods must be pinned to the swapped node (join-worker) -------------
# The join-worker leg adds the swapped node to a cluster that still has a
# schedulable vanilla worker, and nothing pushed test pods onto the module under
# test. Result: 8 of 8 focused specs passed locally while `kubectl exec` against a
# pinned pod was completely broken — they ran on the vanilla worker and the
# swapped kubelet logged zero exec_proxy lines for the whole suite (#1710).
#
# Fix: once hydrophone's conformance pod is SCHEDULED (cordon does not move
# placed pods, and that pod must stay on a node with kube-proxy to reach
# 10.96.0.1), make every other node unschedulable.
PIN_BIN="$TMP/pin-bin"; mkdir -p "$PIN_BIN"
PIN_CALLS="$TMP/pin-calls.txt"
# The real kubectl EXITS NON-ZERO while the conformance pod does not exist yet,
# and the driver runs under `set -euo pipefail` with IFS=$'\n\t' — the first
# version of this test used a stub that always succeeded and emitted
# space-separated node names, so it passed while the real function died on its
# first iteration and cordoned nothing.
cat >"$PIN_BIN/kubectl" <<'STUB'
#!/bin/sh
echo "kubectl $*" >>"$PIN_CALLS"
case "$*" in
  *"get pod e2e-conformance-test"*)
    # not scheduled on the first look, as in a real run
    if [ -f "$PIN_CALLS.seen" ]; then echo "vanilla-swap-kubelet-worker"; else : >"$PIN_CALLS.seen"; exit 1; fi ;;
  *"get nodes"*)
    printf '%s\n' vanilla-swap-kubelet-control-plane vanilla-swap-kubelet-worker rusternetes-node ;;
esac
exit 0
STUB
chmod +x "$PIN_BIN/kubectl"

: >"$PIN_CALLS"; rm -f "$PIN_CALLS.seen"
# Same shell settings as scripts/vanilla-swap-run.sh, or the test does not
# exercise what actually runs.
PIN_CALLS="$PIN_CALLS" PATH="$PIN_BIN:$PATH" VS_PIN_WAIT=5 bash -c "
  set -euo pipefail
  IFS=\$'\n\t'
  source '$SCRIPT_DIR/vanilla-swap-common.sh'
  vs_pin_tests_to_swapped_node /dev/null rusternetes-node
" >/dev/null 2>&1

if grep -q "cordon vanilla-swap-kubelet-worker" "$PIN_CALLS"; then
  ok "pins tests by cordoning the vanilla worker"
else
  bad "must cordon the vanilla worker so test pods cannot land there"
fi
if grep -q "cordon rusternetes-node" "$PIN_CALLS"; then
  bad "must NOT cordon the swapped node — it is where tests have to run"
else
  ok "leaves the swapped node schedulable"
fi
if grep -q "get pod e2e-conformance-test" "$PIN_CALLS"; then
  ok "waits for the conformance pod to be placed before cordoning"
else
  bad "must wait for the conformance pod to be scheduled (cordon would leave it Pending)"
fi

# A conformance pod that never schedules must leave the cluster alone rather than
# cordoning everything and guaranteeing a dead run.
cat >"$PIN_BIN/kubectl" <<'STUB'
#!/bin/sh
echo "kubectl $*" >>"$PIN_CALLS"
case "$*" in
  *"get nodes"*) echo "a b rusternetes-node" ;;
esac
exit 0
STUB
chmod +x "$PIN_BIN/kubectl"
: >"$PIN_CALLS"; rm -f "$PIN_CALLS.seen"
PIN_CALLS="$PIN_CALLS" PATH="$PIN_BIN:$PATH" VS_PIN_WAIT=1 bash -c "
  set -euo pipefail
  IFS=\$'\n\t'
  source '$SCRIPT_DIR/vanilla-swap-common.sh'
  vs_pin_tests_to_swapped_node /dev/null rusternetes-node
" >/dev/null 2>&1
if grep -q "cordon" "$PIN_CALLS"; then
  bad "must not cordon anything when the conformance pod never scheduled"
else
  ok "no cordon when the conformance pod never scheduled"
fi

# Only the join-worker leg needs this: a static-pod or daemonset module is
# cluster-wide, so every spec already exercises it.
DRIVER="$(cat "$SCRIPT_DIR/vanilla-swap-run.sh")"
case "$DRIVER" in
  *vs_pin_tests_to_swapped_node*) ok "driver pins tests for the swapped node" ;;
  *) bad "driver must call vs_pin_tests_to_swapped_node" ;;
esac
case "$DRIVER" in
  *'join-worker'*vs_pin_tests_to_swapped_node*|*vs_pin_tests_to_swapped_node*'join-worker'*)
    ok "pinning is scoped to the join-worker swap" ;;
  *) bad "pinning must be scoped to join-worker (static-pod/daemonset modules are cluster-wide)" ;;
esac


# --- a timed-out leg must still report what completed ----------------------
# Ginkgo registers its junit reporter as a ReportAfterSuite node, so the file only
# exists once the suite finishes: killing the runner mid-suite leaves NOTHING to
# copy out of the conformance pod (#1703's original premise). What does survive is
# ginkgo's progress stream, which the harness already tees to disk — one bullet per
# completed spec, `• [FAILED]` for a failure, S for a skip.
#
# The controller-manager leg (run 30443157562) was killed at 4490s having finished
# 23 of its 52 specs (16 passed, 7 failed) and published 0/0. Those numbers are
# recoverable.
PROG="$TMP/ginkgo-progress.out"
cat >"$PROG" <<'EOF'
Running in parallel across 2 processes
SSSSSSSSSSSSSSSSSSSS•SSSSSS•SSS
------------------------------
• [FAILED] [315.751 seconds]
[sig-apps] CronJob [It] should not schedule new jobs when ForbidConcurrent [Slow] [Conformance]
  Timeline >>
  STEP: Creating a ForbidConcurrent cronjob
SS•SS•
• [FAILED] [620.079 seconds]
[sig-apps] DisruptionController [It] should create a PodDisruptionBudget [Conformance]
SSS•
EOF
got="$(vs_progress_counts "$PROG")"
[ "$got" = "5 2" ] && ok "vs_progress_counts: reads passed/failed from ginkgo's progress stream" \
  || bad "vs_progress_counts got '$got' (want '5 2')"

got="$(vs_progress_counts "$TMP/not-a-file")"
[ "$got" = "0 0" ] && ok "vs_progress_counts: missing file => 0 0" \
  || bad "vs_progress_counts missing-file got '$got' (want '0 0')"

# "Ran N of M Specs" is printed only when the suite completes, which is how a
# mid-suite kill is told apart from a kill during post-suite cleanup.
printf 'SSS•\nRan 23 of 7348 Specs in 100.5 seconds\n' >"$TMP/completed.out"
if vs_suite_completed "$TMP/completed.out"; then ok "vs_suite_completed: sees the ginkgo summary"; else bad "should detect the ginkgo summary"; fi
if vs_suite_completed "$PROG"; then bad "must NOT report completion without the ginkgo summary"; else ok "vs_suite_completed: mid-suite kill is not completion"; fi

# Verdict: killed mid-suite with specs on the board is a timeout WITH numbers, not
# a module that never came up — and not a pass.
got="$(vs_verdict 23 7 124 partial)"
[ "$got" = "test-timeout $VS_EX_TESTFAIL" ] \
  && ok "vs_verdict: mid-suite timeout with specs => test-timeout" \
  || bad "vs_verdict partial-timeout got '$got' (want 'test-timeout $VS_EX_TESTFAIL')"

# All specs green and the runner killed afterwards (hung cleanup) stays a pass.
got="$(vs_verdict 52 0 124)"
[ "$got" = "test-passed 0" ] \
  && ok "vs_verdict: complete suite, late kill => still test-passed" \
  || bad "vs_verdict late-kill got '$got' (want 'test-passed 0')"

# Zero specs stays module-did-not-come-up regardless of how it died.
got="$(vs_verdict 0 0 124 partial)"
[ "$got" = "module-did-not-come-up $VS_EX_NOTUP" ] \
  && ok "vs_verdict: timeout before any spec => module-did-not-come-up" \
  || bad "vs_verdict empty-timeout got '$got'"

# The badge step must mark a partial run so a percentage cannot be read as a
# finished suite.
WF="$SCRIPT_DIR/../.github/workflows/vanilla-swap-module.yml"
if grep -q "test-timeout" "$WF"; then ok "workflow marks a partial run on the badge"; else bad "workflow must label a test-timeout badge as partial"; fi


# --- the conformance preflight must be told this is a kind cluster ----------
# conformance-preflight.sh (#1777) asserts the COMPOSE stack's shape:
# kube-scheduler-node-1 / kube-controller-manager-node-1 and the rusternetes-dns
# Deployment. vanilla-swap drives the same runner against a kind cluster, whose
# mirror pods are kube-<component>-<cluster>-control-plane and whose DNS is
# CoreDNS — so from 2026-08-27 every leg died in preflight with three FAILs
# about objects that cannot exist here, and reported module-did-not-come-up
# (runs 33103401201, 33119674890) for modules that had come up fine.
PF_ARGS="$(vs_preflight_args my-cluster | tr '\n' ' ')"
case "$PF_ARGS" in
  *"--control-plane-pods --preflight-arg kube-scheduler-my-cluster-control-plane,kube-controller-manager-my-cluster-control-plane"*)
    ok "preflight args name the kind mirror pods" ;;
  *) bad "preflight args must name kube-<component>-<cluster>-control-plane, got: $PF_ARGS" ;;
esac
case "$PF_ARGS" in
  *"--dns-deployment --preflight-arg coredns"*) ok "preflight args point DNS at CoreDNS" ;;
  *) bad "preflight args must point the DNS assertion at coredns, got: $PF_ARGS" ;;
esac
case "$PF_ARGS" in
  *rusternetes-dns*|*node-1*) bad "preflight args must not carry compose-stack names: $PF_ARGS" ;;
  *) ok "preflight args carry no compose-stack names" ;;
esac

# The container-scoped assertions address containers by NAME on the local
# daemon. A vanilla-swap cluster is kind, so rusternetes-kubelet /
# rusternetes-rhino / the kubelet2 peers either do not exist or — on a
# workstation running a compose stack next to this kind cluster — belong to a
# DIFFERENT cluster, whose certs mount then refuses this run (observed
# locally: "kubelet does not mount the certs dir ..." against a kind cluster).
case "$PF_ARGS" in
  *"--kubelet --preflight-arg none"*) ok "preflight args disable the certs-mount assertion" ;;
  *) bad "preflight args must pass --kubelet none, got: $PF_ARGS" ;;
esac
case "$PF_ARGS" in
  *"--storage-container --preflight-arg none"*) ok "preflight args disable the storage-size assertion" ;;
  *) bad "preflight args must pass --storage-container none, got: $PF_ARGS" ;;
esac
case "$PF_ARGS" in
  *"--skip-node-image-check"*) ok "preflight args disable the per-node binary assertion" ;;
  *) bad "preflight args must pass --skip-node-image-check, got: $PF_ARGS" ;;
esac

# The api-server leg is the exception. Swapping the api-server swaps the OBJECT
# STORE with it: the rusternetes api-server comes up on an empty database, so
# the kind control-plane mirror pods and the CoreDNS Deployment are not visible
# through it — not because the module failed, but because that is what this
# leg tests. Asserting them refuses the leg outright (run 33159492201:
# "kube-scheduler-vanilla-swap-api-server-control-plane does not exist",
# module-did-not-come-up) and we never see the leg's real pass rate.
PF_API="$(vs_preflight_args my-cluster api-server | tr '\n' ' ')"
case "$PF_API" in
  *"--control-plane-pods --preflight-arg none"*) ok "api-server leg does not assert kind control-plane pods" ;;
  *) bad "the api-server leg must pass --control-plane-pods none, got: $PF_API" ;;
esac
case "$PF_API" in
  *"--dns-deployment --preflight-arg none"*) ok "api-server leg does not assert cluster DNS" ;;
  *) bad "the api-server leg must pass --dns-deployment none, got: $PF_API" ;;
esac
# ...but the environment-level assertions it can still make must stay on.
case "$PF_API" in
  *"--kubelet --preflight-arg none"*) ok "api-server leg keeps the container-scoped opt-outs" ;;
  *) bad "the api-server leg must still pass --kubelet none, got: $PF_API" ;;
esac
# Every other module keeps the real gate.
case "$(vs_preflight_args my-cluster kubelet | tr '\n' ' ')" in
  *"kube-scheduler-my-cluster-control-plane"*) ok "other modules still assert the kind control-plane pods" ;;
  *) bad "only the api-server leg may skip the control-plane assertion" ;;
esac

# ...and the runner has to actually pass them. A helper nothing calls would
# leave the legs exactly as broken.
RUN_SH="$(cat "$SCRIPT_DIR/vanilla-swap-run.sh")"
case "$RUN_SH" in
  *"vs_preflight_args"*) ok "vanilla-swap-run.sh builds the preflight args" ;;
  *) bad "vanilla-swap-run.sh must call vs_preflight_args" ;;
esac
TARGET_CALL="$(sed -n '/conformance-target-run.sh/,/^set -e$/p' "$SCRIPT_DIR/vanilla-swap-run.sh")"
case "$TARGET_CALL" in
  *'VS_PREFLIGHT_ARGS[@]'*) ok "the conformance-target-run.sh call forwards them" ;;
  *) bad "the conformance-target-run.sh invocation must forward VS_PREFLIGHT_ARGS" ;;
esac

# --- cluster name must be overridable --------------------------------------
# kind creates/deletes by name, so debugging a leg locally would otherwise wipe an
# existing vanilla-swap-<module> cluster someone is using.
DRV="$(cat "$SCRIPT_DIR/vanilla-swap-run.sh")"
case "$DRV" in
  *'CLUSTER="${VS_CLUSTER_NAME:-vanilla-swap-${MODULE}}"'*)
    ok "cluster name honours VS_CLUSTER_NAME, defaulting to the module" ;;
  *) bad "CLUSTER must be overridable via VS_CLUSTER_NAME" ;;
esac

echo "---"
[ "$fails" -eq 0 ] && { echo "PASS: all registry-parser tests"; exit 0; } || { echo "FAIL: $fails test(s)"; exit 1; }
