#!/usr/bin/env bash
# Run one SIG's upstream [Conformance] slice (or a narrowed focus) against an
# already-running rusternetes cluster via Hydrophone, parse the junit, and
# report passed/failed/total. NON-GATING: test failures do NOT fail the run —
# only an infra failure (no junit produced) exits non-zero. Same caller
# contract as scripts/conformance-tags-run.sh and conformance-feature-run.sh:
# this script does NOT bring up or tear down the cluster.
#
# The SIG's focus/skip default to the entry in ci/conformance/sigs.json; a
# --focus override narrows the run (single-test proof) and marks the run
# `focused=1` so the caller can skip badge publication (a one-test count is
# not the SIG's pass rate).
#
# Machine-readable results are written to $GITHUB_OUTPUT when set (else printed
# as key=value lines on stdout): passed, failed, total, focused.
#
# Exit codes:
#   0  hydrophone produced junit (regardless of pass/fail), OR the focus matched
#      no tests (reported explicitly, not as success)
#   1  no junit produced — infrastructure failure
#   2  usage / preflight error (unknown SIG, no kubeconfig, missing hydrophone)
#
# Usage:
#   bash scripts/conformance-sig-run.sh --sig sig-node [flags]
#
# Flags: --sig --focus --skip --kubeconfig --conformance-image --output-dir
#        --hydrophone -h|--help
set -euo pipefail
IFS=$'\n\t'

# Enable payload dumps in api-server/kubelet so any panic / 5xx / decode
# failure during the run logs the offending request body.
export RUSTERNETES_DUMP_PAYLOADS=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
MANIFEST="${SIGS_MANIFEST:-$REPO_ROOT/ci/conformance/sigs.json}"

# Count testcase statuses in <dir>/junit_01.xml.
# Echoes "had_junit passed failed skipped total". had_junit is 0/1; total is
# passed+failed+skipped (0 => the focus matched no specs / "no tests matched").
sig_counts() {
    local dir="$1"
    local junit="$dir/junit_01.xml"
    if [ ! -f "$junit" ]; then
        echo "0 0 0 0 0"; return
    fi
    local passed failed skipped total
    passed=$(grep -oE 'status="passed"' "$junit" | wc -l | tr -d ' ')
    failed=$(grep -oE 'status="failed"' "$junit" | wc -l | tr -d ' ')
    skipped=$(grep -oE 'status="skipped"' "$junit" | wc -l | tr -d ' ')
    total=$((passed + failed + skipped))
    echo "1 $passed $failed $skipped $total"
}

# Emit key=value results to $GITHUB_OUTPUT when running under Actions, else stdout.
emit_output() {
    local passed="$1" failed="$2" total="$3" focused="$4"
    local line
    for line in "passed=$passed" "failed=$failed" "total=$total" "focused=$focused"; do
        if [ -n "${GITHUB_OUTPUT:-}" ]; then
            echo "$line" >> "$GITHUB_OUTPUT"
        else
            echo "$line"
        fi
    done
}

# When sourced by the unit test, stop here — don't parse args or run.
if [ -n "${SIG_RUN_LIB_ONLY:-}" ]; then
    return 0 2>/dev/null || true
fi

# ---------- arg parsing ----------
SIG=""; FOCUS=""; SKIP=""; FOCUS_OVERRIDDEN=0
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
CONFORMANCE_IMAGE="registry.k8s.io/conformance:v1.35.0"
OUTPUT_DIR=""; HYDROPHONE_BIN=""

die() { echo "[conformance-sig-run] ERROR: $*" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) sed -nE '/^# /,/^$/ s/^# ?//p' "${BASH_SOURCE[0]}" | head -40; exit 0 ;;
        --sig) [[ $# -ge 2 ]] || die "--sig requires a value"; SIG="$2"; shift 2 ;;
        --focus) [[ $# -ge 2 ]] || die "--focus requires a value"; FOCUS="$2"; FOCUS_OVERRIDDEN=1; shift 2 ;;
        --skip) [[ $# -ge 2 ]] || die "--skip requires a value"; SKIP="$2"; shift 2 ;;
        --kubeconfig) [[ $# -ge 2 ]] || die "--kubeconfig requires a value"; KUBECONFIG_PATH="$2"; shift 2 ;;
        --conformance-image) [[ $# -ge 2 ]] || die "--conformance-image requires a value"; CONFORMANCE_IMAGE="$2"; shift 2 ;;
        --output-dir) [[ $# -ge 2 ]] || die "--output-dir requires a value"; OUTPUT_DIR="$2"; shift 2 ;;
        --hydrophone) [[ $# -ge 2 ]] || die "--hydrophone requires a value"; HYDROPHONE_BIN="$2"; shift 2 ;;
        *) die "unknown flag: $1 (use --help)" ;;
    esac
done

[ -n "$SIG" ] || die "--sig required"
command -v jq >/dev/null 2>&1 || die "jq required"
[ -f "$MANIFEST" ] || die "sigs manifest not found: $MANIFEST"

# Resolve focus/skip from the manifest unless overridden on the CLI.
entry=$(jq -c --arg s "$SIG" '.[] | select(.name == $s)' "$MANIFEST")
[ -n "$entry" ] || die "unknown SIG '$SIG' (not in $MANIFEST)"
[ "$FOCUS_OVERRIDDEN" -eq 1 ] || FOCUS=$(echo "$entry" | jq -r '.focus')
[ -n "$SKIP" ] || SKIP=$(echo "$entry" | jq -r '.skip // "\\[Flaky\\]"')

[ -f "$KUBECONFIG_PATH" ] || die "kubeconfig not found: $KUBECONFIG_PATH"

if [ -z "$HYDROPHONE_BIN" ]; then
    command -v hydrophone >/dev/null 2>&1 || die "hydrophone not on PATH; pass --hydrophone"
    HYDROPHONE_BIN="$(command -v hydrophone)"
fi
[ -x "$HYDROPHONE_BIN" ] || die "hydrophone not executable: $HYDROPHONE_BIN"

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="$REPO_ROOT/.rusternetes/volumes/sig-$SIG-$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$OUTPUT_DIR"

echo "[conformance-sig-run] sig=$SIG focused=$FOCUS_OVERRIDDEN"
echo "  focus : $FOCUS"
echo "  skip  : $SKIP"
echo "  image : $CONFORMANCE_IMAGE"

# Clear any leftover conformance namespace before deploying (hydrophone refuses
# to deploy into an existing one) — same self-containment as conformance-tags-run.sh.
"$HYDROPHONE_BIN" --cleanup --kubeconfig "$KUBECONFIG_PATH" >/dev/null 2>&1 || true

# Single ginkgo thread: a per-SIG run may include [Serial] specs, and each SIG
# owns its cluster for the run, so serial is the safe default.
set +e
"$HYDROPHONE_BIN" \
    --focus "$FOCUS" \
    --skip "$SKIP" \
    --parallel 1 \
    --output-dir "$OUTPUT_DIR" \
    --kubeconfig "$KUBECONFIG_PATH" \
    --conformance-image "$CONFORMANCE_IMAGE" 2>&1 | tee "$OUTPUT_DIR/run.log"
hydro_exit=${PIPESTATUS[0]}
set -e

IFS=' ' read -r HAD_JUNIT PASSED FAILED SKIPPED TOTAL <<<"$(sig_counts "$OUTPUT_DIR")"

if [ "$HAD_JUNIT" -eq 0 ]; then
    echo "[conformance-sig-run] sig=$SIG hydrophone_exit=$hydro_exit — NO junit produced (infra failure)"
    emit_output 0 0 0 "$FOCUS_OVERRIDDEN"
    exit 1
fi

if [ "$TOTAL" -eq 0 ]; then
    echo "[conformance-sig-run] sig=$SIG — no tests matched (focus selected 0 specs)"
    emit_output 0 0 0 "$FOCUS_OVERRIDDEN"
    exit 0
fi

echo "[conformance-sig-run] sig=$SIG hydrophone_exit=$hydro_exit passed=$PASSED failed=$FAILED skipped=$SKIPPED total=$((PASSED + FAILED))"
if [ "$FAILED" -gt 0 ]; then
    echo "[conformance-sig-run] FAILED tests:"
    grep -oE '<testcase name="[^"]+"[^>]*status="failed"' "$OUTPUT_DIR/junit_01.xml" \
        | sed -E 's|<testcase name="([^"]+)".*|  - \1|' \
        | sed -e 's/&#39;/'"'"'/g' -e 's/&amp;/\&/g' -e 's/&lt;/</g' \
              -e 's/&gt;/>/g' -e 's/&quot;/"/g' -e 's/&#34;/"/g' || true
fi

# Badge total (attempted) EXCLUDES skipped, per the existing update-badge counting.
emit_output "$PASSED" "$FAILED" "$((PASSED + FAILED))" "$FOCUS_OVERRIDDEN"
exit 0
