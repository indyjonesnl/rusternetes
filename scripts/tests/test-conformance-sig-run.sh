#!/usr/bin/env bash
# Unit tests for scripts/conformance-sig-run.sh — no cluster required.
# Sources the script's lib helpers (SIG_RUN_LIB_ONLY) and exercises the full
# CLI against a stubbed `hydrophone` that writes fixture junit files.
#
# Run with: bash scripts/tests/test-conformance-sig-run.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
RUNNER="$REPO_ROOT/scripts/conformance-sig-run.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0; failcnt=0
ok()   { echo "  ok: $*"; pass=$((pass + 1)); }
bad()  { echo "  FAIL: $*" >&2; failcnt=$((failcnt + 1)); }

[ -f "$RUNNER" ] || { echo "FAIL: runner missing: $RUNNER" >&2; exit 1; }

# ---- lib helper: sig_counts ----
SIG_RUN_LIB_ONLY=1 source "$RUNNER"

mkdir -p "$TMP/nojunit"
IFS=' ' read -r hj p f s t <<<"$(sig_counts "$TMP/nojunit")"
[ "$hj" = "0" ] && [ "$t" = "0" ] && ok "sig_counts: no junit => had_junit=0 total=0" \
    || bad "sig_counts no-junit got hj=$hj total=$t"

mkdir -p "$TMP/counts"
cat > "$TMP/counts/junit_01.xml" <<'EOF'
<testsuite>
  <testcase name="a" status="passed"></testcase>
  <testcase name="b" status="passed"></testcase>
  <testcase name="c" status="failed"></testcase>
  <testcase name="d" status="skipped"></testcase>
</testsuite>
EOF
IFS=' ' read -r hj p f s t <<<"$(sig_counts "$TMP/counts")"
[ "$hj" = "1" ] && [ "$p" = "2" ] && [ "$f" = "1" ] && [ "$s" = "1" ] && [ "$t" = "4" ] \
    && ok "sig_counts: parses passed/failed/skipped/total" \
    || bad "sig_counts got hj=$hj p=$p f=$f s=$s t=$t (want 1 2 1 1 4)"

# ---- CLI fixtures ----
KC="$TMP/kubeconfig"; echo "kc" > "$KC"

# Stub hydrophone: --cleanup => noop; run => write fixture junit per FAKE_MODE.
FAKE="$TMP/hydrophone"
cat > "$FAKE" <<'EOF'
#!/usr/bin/env bash
for a in "$@"; do :; done
# find --output-dir value
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
  empty) cat > "$out/junit_01.xml" <<'J'
<testsuite></testsuite>
J
  ;;
  nojunit) : ;;  # produce nothing
esac
exit 0
EOF
chmod +x "$FAKE"

run_cli() { GITHUB_OUTPUT="$TMP/gho.$$" ; : > "$GITHUB_OUTPUT"; GITHUB_OUTPUT="$GITHUB_OUTPUT" bash "$RUNNER" "$@"; }
gho() { grep -E "^$1=" "$GITHUB_OUTPUT" | tail -1 | cut -d= -f2-; }

# missing --sig => exit 2
if run_cli --kubeconfig "$KC" --hydrophone "$FAKE" >/dev/null 2>&1; then bad "missing --sig should exit 2"; else
  [ $? -eq 2 ] && ok "missing --sig => exit 2" || bad "missing --sig wrong exit"; fi

# unknown SIG => exit 2
if run_cli --sig sig-nope --kubeconfig "$KC" --hydrophone "$FAKE" >/dev/null 2>&1; then bad "unknown SIG should exit 2"; else
  rc=$?; [ "$rc" -eq 2 ] && ok "unknown SIG => exit 2" || bad "unknown SIG exit=$rc"; fi

# missing kubeconfig => exit 2
if run_cli --sig sig-node --kubeconfig "$TMP/nope" --hydrophone "$FAKE" >/dev/null 2>&1; then bad "missing kubeconfig should exit 2"; else
  rc=$?; [ "$rc" -eq 2 ] && ok "missing kubeconfig => exit 2" || bad "missing kubeconfig exit=$rc"; fi

# full run (pass mode): exit 0, passed=1 failed=1 total=2 focused=0
FAKE_MODE=pass run_cli --sig sig-node --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o1" >/dev/null 2>&1 \
  && ok "full run exit 0" || bad "full run should exit 0"
[ "$(gho passed)" = "1" ] && [ "$(gho failed)" = "1" ] && [ "$(gho total)" = "2" ] && [ "$(gho focused)" = "0" ] \
  && ok "full run outputs passed=1 failed=1 total=2 focused=0" \
  || bad "full run outputs: passed=$(gho passed) failed=$(gho failed) total=$(gho total) focused=$(gho focused)"

# focus override => focused=1
FAKE_MODE=pass run_cli --sig sig-node --focus 'x' --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o2" >/dev/null 2>&1
[ "$(gho focused)" = "1" ] && ok "--focus sets focused=1" || bad "--focus focused=$(gho focused)"

# no tests matched (empty junit) => exit 0, passed=0 total=0, "no tests matched" message
out=$(FAKE_MODE=empty run_cli --sig sig-node --focus 'zzz' --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o3" 2>&1); rc=$?
[ "$rc" -eq 0 ] && echo "$out" | grep -qi "no tests matched" && [ "$(gho passed)" = "0" ] && [ "$(gho total)" = "0" ] \
  && ok "empty focus => exit 0 + 'no tests matched' + passed/total 0" \
  || bad "empty focus rc=$rc msg/counts wrong (passed=$(gho passed) total=$(gho total))"

# no junit (infra fail) => exit 1
if FAKE_MODE=nojunit run_cli --sig sig-node --kubeconfig "$KC" --hydrophone "$FAKE" --output-dir "$TMP/o4" >/dev/null 2>&1; then
  bad "no junit should exit 1"; else rc=$?; [ "$rc" -eq 1 ] && ok "no junit => exit 1" || bad "no junit exit=$rc"; fi

echo
if [ "$failcnt" -eq 0 ]; then echo "PASS: conformance-sig-run ($pass checks)"; else echo "FAILED: $failcnt of $((pass + failcnt))" >&2; exit 1; fi
