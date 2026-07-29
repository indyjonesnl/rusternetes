#!/usr/bin/env bash
# Run one conformance TARGET (a SIG's [Conformance] slice or a curated feature
# focus) — or a narrowed --focus — against an already-running rusternetes
# cluster via Hydrophone, parse the junit, and report passed/failed/total.
# NON-GATING: test failures do NOT fail the run — only an infra failure (no
# junit produced) exits non-zero. Same caller contract as
# scripts/conformance-tags-run.sh: this script does NOT bring up or tear down
# the cluster.
#
# The target's focus/skip default to its entry in ci/conformance/targets.json;
# a --focus override narrows the run (single-test proof) and marks the run
# `focused=1` so the caller can skip badge publication (a one-test count is not
# the target's pass rate).
#
# Machine-readable results are written to $GITHUB_OUTPUT when set (else printed
# as key=value lines on stdout): passed, failed, total, focused.
#
# Exit codes:
#   0  hydrophone produced junit (regardless of conformance pass/fail)
#   1  infrastructure failure, including no junit or a focus matching no specs
#   2  usage / preflight error (unknown target, no kubeconfig, missing hydrophone)
#
# Usage:
#   bash scripts/conformance-target-run.sh --target sig-node [flags]
#
# Flags: --target --focus --skip --parallel --kubeconfig --conformance-image
#        --output-dir --hydrophone -h|--help
#        --parallel N   ginkgo procs (default 2; ginkgo isolates [Serial] specs)
set -euo pipefail
IFS=$'\n\t'

# Enable payload dumps in api-server/kubelet so any panic / 5xx / decode
# failure during the run logs the offending request body.
export RUSTERNETES_DUMP_PAYLOADS=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
MANIFEST="${TARGETS_MANIFEST:-$REPO_ROOT/ci/conformance/targets.json}"

# Ginkgo's suite-level nodes, which it reports as <testcase> entries next to the
# real specs: `[ReportBeforeSuite]`, `[SynchronizedBeforeSuite]`, … Counting them
# as specs inflates every target (sig-instrumentation reported 11/11 for a
# 4-spec SIG) and defeats the 0-match guard below, which keys off total==0 —
# a focus matching nothing still produced a green 3/3 (#1643).
#
# The set is upstream's: ginkgo `types.NodeTypesForSuiteLevelNodes`
# (types/types.go:885 — BeforeSuite | SynchronizedBeforeSuite | AfterSuite |
# SynchronizedAfterSuite | ReportBeforeSuite | ReportAfterSuite |
# CleanupAfterSuite), which its own junit reporter drops when asked to
# (reporters/junit_report.go:195: `if config.OmitSuiteSetupNodes &&
# spec.LeafNodeType != types.NodeTypeIt { continue }`). The name each carries is
# "[<LeafNodeType>]" + optional text (junit_report.go:198), and
# NodeTypeCleanupAfterSuite stringifies as "DeferCleanup (Suite)"
# (types/types.go:910).
#
# Tab-separated LITERAL prefixes, matched with awk's index() rather than a
# dynamic regex: mawk (the awk on ubuntu-latest and on Debian/Ubuntu generally)
# rewrites `\[` in a regex string to the metacharacter `[` and warns
# "escape sequence `\[' treated as plain `['", which turned the anchored
# alternation into a character class that matched nothing — the exclusion
# silently did nothing under mawk while passing under gawk.
GINKGO_SUITE_LEVEL_NODES='[BeforeSuite]	[SynchronizedBeforeSuite]	[AfterSuite]	[SynchronizedAfterSuite]	[ReportBeforeSuite]	[ReportAfterSuite]	[DeferCleanup (Suite)]'

# awk snippet shared by the three parsers below: splits the prefix list and
# defines is_suite_level(name).
GINKGO_AWK_PRELUDE='
    BEGIN { n_skip = split(skip, skip_list, "\t") }
    function is_suite_level(name,   i) {
        for (i = 1; i <= n_skip; i++)
            if (index(name, skip_list[i]) == 1) return 1
        return 0
    }
    function tc_name(line) {
        return match(line, /name="[^"]*"/) ? substr(line, RSTART + 6, RLENGTH - 7) : ""
    }
    function tc_status(line) {
        return match(line, /status="[^"]*"/) ? substr(line, RSTART + 8, RLENGTH - 9) : ""
    }
'

# Count real-spec testcase statuses in <dir>/junit_01.xml, or in an explicit
# junit path given as the second argument (the vanilla-swap harness keeps its
# junit under a per-run workdir and picks the newest junit_*.xml).
# Echoes "had_junit passed failed skipped total". had_junit is 0/1; total is
# passed+failed+skipped (0 => the focus matched no specs / "no tests matched").
target_counts() {
    local dir="$1"
    local junit="${2:-$dir/junit_01.xml}"
    if [ ! -f "$junit" ]; then
        echo "0 0 0 0 0"; return
    fi
    # Per-testcase, so a name is matched against its OWN status. Attribute
    # values are XML-escaped (&quot;, &gt;), so `[^>]*` / `[^"]*` are safe.
    local counts
    counts=$(grep -oE '<testcase [^>]*>' "$junit" \
        | awk -v skip="$GINKGO_SUITE_LEVEL_NODES" "$GINKGO_AWK_PRELUDE"'
            !is_suite_level(tc_name($0)) { c[tc_status($0)]++ }
            END { printf "%d %d %d", c["passed"], c["failed"], c["skipped"] }')
    local passed failed skipped total
    IFS=' ' read -r passed failed skipped <<<"$counts"
    total=$((passed + failed + skipped))
    echo "1 $passed $failed $skipped $total"
}

# Names of the ginkgo suite-level nodes that FAILED, one per line ("  - <name>").
# These are excluded from the spec counts above, so without this a broken
# BeforeSuite looks exactly like a focus that matched nothing.
suite_level_failures() {
    local junit="$1/junit_01.xml"
    [ -f "$junit" ] || return 0
    grep -oE '<testcase [^>]*>' "$junit" \
        | awk -v skip="$GINKGO_SUITE_LEVEL_NODES" "$GINKGO_AWK_PRELUDE"'
            is_suite_level(tc_name($0)) && tc_status($0) == "failed" { print "  - " tc_name($0) }'
}

# Names of the real specs that FAILED, one per line ("  - <name>"), with junit's
# XML entities decoded. Same suite-level-node exclusion as target_counts, so the
# printed list always matches the reported `failed=` count.
spec_failures() {
    local junit="$1/junit_01.xml"
    [ -f "$junit" ] || return 0
    grep -oE '<testcase [^>]*>' "$junit" \
        | awk -v skip="$GINKGO_SUITE_LEVEL_NODES" "$GINKGO_AWK_PRELUDE"'
            !is_suite_level(tc_name($0)) && tc_status($0) == "failed" { print "  - " tc_name($0) }' \
        | sed -e 's/&#39;/'"'"'/g' -e 's/&amp;/\&/g' -e 's/&lt;/</g' \
              -e 's/&gt;/>/g' -e 's/&quot;/"/g' -e 's/&#34;/"/g'
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
if [ -n "${TARGET_RUN_LIB_ONLY:-}" ]; then
    return 0 2>/dev/null || true
fi

# ---------- arg parsing ----------
TARGET=""; FOCUS=""; SKIP=""; FOCUS_OVERRIDDEN=0
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
CONFORMANCE_IMAGE="registry.k8s.io/conformance:v1.35.0"
OUTPUT_DIR=""; HYDROPHONE_BIN=""; PARALLEL=2

die() { echo "[conformance-target-run] ERROR: $*" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) sed -nE '/^# /,/^$/ s/^# ?//p' "${BASH_SOURCE[0]}" | head -40; exit 0 ;;
        --target) [[ $# -ge 2 ]] || die "--target requires a value"; TARGET="$2"; shift 2 ;;
        --focus) [[ $# -ge 2 ]] || die "--focus requires a value"; FOCUS="$2"; FOCUS_OVERRIDDEN=1; shift 2 ;;
        --skip) [[ $# -ge 2 ]] || die "--skip requires a value"; SKIP="$2"; shift 2 ;;
        --kubeconfig) [[ $# -ge 2 ]] || die "--kubeconfig requires a value"; KUBECONFIG_PATH="$2"; shift 2 ;;
        --conformance-image) [[ $# -ge 2 ]] || die "--conformance-image requires a value"; CONFORMANCE_IMAGE="$2"; shift 2 ;;
        --output-dir) [[ $# -ge 2 ]] || die "--output-dir requires a value"; OUTPUT_DIR="$2"; shift 2 ;;
        --hydrophone) [[ $# -ge 2 ]] || die "--hydrophone requires a value"; HYDROPHONE_BIN="$2"; shift 2 ;;
        --parallel) [[ $# -ge 2 ]] || die "--parallel requires a value"; PARALLEL="$2"; shift 2 ;;
        *) die "unknown flag: $1 (use --help)" ;;
    esac
done

[ -n "$TARGET" ] || die "--target required"
command -v jq >/dev/null 2>&1 || die "jq required"
[ -f "$MANIFEST" ] || die "targets manifest not found: $MANIFEST"

# Resolve focus/skip from the manifest unless overridden on the CLI.
entry=$(jq -c --arg t "$TARGET" '.[] | select(.name == $t)' "$MANIFEST")
[ -n "$entry" ] || die "unknown target '$TARGET' (not in $MANIFEST)"
[ "$FOCUS_OVERRIDDEN" -eq 1 ] || FOCUS=$(echo "$entry" | jq -r '.focus')
[ -n "$SKIP" ] || SKIP=$(echo "$entry" | jq -r '.skip // "\\[Flaky\\]"')

[ -f "$KUBECONFIG_PATH" ] || die "kubeconfig not found: $KUBECONFIG_PATH"

if [ -z "$HYDROPHONE_BIN" ]; then
    command -v hydrophone >/dev/null 2>&1 || die "hydrophone not on PATH; pass --hydrophone"
    HYDROPHONE_BIN="$(command -v hydrophone)"
fi
[ -x "$HYDROPHONE_BIN" ] || die "hydrophone not executable: $HYDROPHONE_BIN"

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="$REPO_ROOT/.rusternetes/volumes/target-$TARGET-$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$OUTPUT_DIR"

echo "[conformance-target-run] target=$TARGET focused=$FOCUS_OVERRIDDEN"
echo "  focus : $FOCUS"
echo "  skip  : $SKIP"
echo "  image : $CONFORMANCE_IMAGE"

# Clear any leftover conformance namespace before deploying (hydrophone refuses
# to deploy into an existing one) — same self-containment as conformance-tags-run.sh.
"$HYDROPHONE_BIN" --cleanup --kubeconfig "$KUBECONFIG_PATH" >/dev/null 2>&1 || true

# Default 2 ginkgo procs (matches the old canary phase-1). Ginkgo isolates
# [Serial] specs into a dedicated single-proc phase, so a focus that includes
# serial specs stays correct at --parallel > 1. Single-threaded (--parallel 1)
# ran a full SIG slice past the 90-min job timeout (#1616).
set +e
"$HYDROPHONE_BIN" \
    --focus "$FOCUS" \
    --skip "$SKIP" \
    --parallel "$PARALLEL" \
    --output-dir "$OUTPUT_DIR" \
    --kubeconfig "$KUBECONFIG_PATH" \
    --conformance-image "$CONFORMANCE_IMAGE" 2>&1 | tee "$OUTPUT_DIR/run.log"
hydro_exit=${PIPESTATUS[0]}
set -e

# Ginkgo writes framework setup testcases to junit even when the focus selects
# zero specs, so junit counts alone can turn an empty run into a false 3/3
# success. Its suite summary is authoritative for focus matching.
if grep -Eq 'Will run 0 of [0-9]+ specs' "$OUTPUT_DIR/run.log"; then
    echo "[conformance-target-run] target=$TARGET — no tests matched (focus selected 0 specs)" >&2
    emit_output 0 0 0 "$FOCUS_OVERRIDDEN"
    exit 1
fi

IFS=' ' read -r HAD_JUNIT PASSED FAILED SKIPPED TOTAL <<<"$(target_counts "$OUTPUT_DIR")"

if [ "$HAD_JUNIT" -eq 0 ]; then
    echo "[conformance-target-run] target=$TARGET hydrophone_exit=$hydro_exit — NO junit produced (infra failure)"
    emit_output 0 0 0 "$FOCUS_OVERRIDDEN"
    exit 1
fi

if [ "$TOTAL" -eq 0 ]; then
    # Zero specs, so either the focus matched nothing or ginkgo never got as far
    # as running one. A failed suite-level node (BeforeSuite cannot reach the
    # cluster, AfterSuite dump failed) says which, and blaming the focus for a
    # broken cluster sends the next reader down the wrong path.
    setup_failures="$(suite_level_failures "$OUTPUT_DIR")"
    if [ -n "$setup_failures" ]; then
        echo "[conformance-target-run] target=$TARGET — suite setup failed, no spec ran:" >&2
        printf '%s\n' "$setup_failures" >&2
    else
        echo "[conformance-target-run] target=$TARGET — no tests matched (empty junit)" >&2
    fi
    emit_output 0 0 0 "$FOCUS_OVERRIDDEN"
    exit 1
fi

echo "[conformance-target-run] target=$TARGET hydrophone_exit=$hydro_exit passed=$PASSED failed=$FAILED skipped=$SKIPPED total=$((PASSED + FAILED))"
if [ "$FAILED" -gt 0 ]; then
    echo "[conformance-target-run] FAILED tests:"
    spec_failures "$OUTPUT_DIR" || true
fi

# Badge total (attempted) EXCLUDES skipped, per the existing update-badge counting.
emit_output "$PASSED" "$FAILED" "$((PASSED + FAILED))" "$FOCUS_OVERRIDDEN"
exit 0
