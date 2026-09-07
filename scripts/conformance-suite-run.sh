#!/usr/bin/env bash
# Run the [Conformance] suite as a SEQUENCE OF PARTITIONS (one per SIG) against
# an already-running cluster, reporting each partition's result as it lands
# instead of after the whole suite.
#
# Why partition at all: a single full-suite run is one ~95-minute block that
# yields nothing until it ends, mixes every SIG's failures into one junit, and
# re-runs slices (sig-cli) that change rarely. Chunking gives:
#   * faster returns  — a partition's pass/fail prints the moment it finishes
#   * isolation       — one junit + one component-log capture per SIG, and a
#                       preflight at every boundary, so a cluster wedged by
#                       partition N is attributed to N and not blamed on N+1
#   * selectivity     — --targets / --skip-targets to omit stable slices
#
# MECHANISM: this script owns only the SEQUENCING. Each partition is a plain
# `conformance-target-run.sh --target <name>` call, so focus/skip come from
# ci/conformance/targets.json and are identical to what the per-target nightly
# workflows run. There is no second definition of a partition to drift.
#
# Upstream shape: upstream drives ONE focus/skip regex per e2e invocation
# (hack/ginkgo-e2e.sh:146 `--skip=${CONFORMANCE_TEST_SKIP_REGEX}`); the
# per-SIG sequencing lives in kubernetes/test-infra job configs, one job per SIG
# focus. targets.json + the generated nightly workflows already mirror that
# split, so this is a local sequencer over the upstream-shaped mechanism — there
# is no upstream Go behaviour to port here.
#
# NOT a gate. Conformance failures never fail the run; only an INFRA failure
# (a partition that produced no junit) is reflected in the exit code.
#
# Coverage: the kind:sig partitions cover every [sig-*] label present in the
# [Conformance] set except [sig-architecture], which has no manifest entry
# (1 spec, tracked in #1880). Running every partition is therefore the full
# suite minus that one spec — scripts/tests/test-target-coverage.sh is the guard.
#
# Exit codes:
#   0  every selected partition produced junit (regardless of pass/fail)
#   1  at least one partition hit an infra failure (no junit / focus matched 0)
#   2  usage error (unknown target, empty selection, missing dependency)
#
# Usage:
#   bash scripts/conformance-suite-run.sh                          # all sig partitions
#   bash scripts/conformance-suite-run.sh --skip-targets sig-cli   # omit a stable slice
#   bash scripts/conformance-suite-run.sh --targets sig-apps,sig-storage
#
# Flags: --targets --skip-targets --order --reverse --output-dir
#        --stop-on-infra-failure and the pass-throughs --kubeconfig
#        --conformance-image --hydrophone --parallel --skip-preflight
#        --preflight-arg -h|--help
set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
MANIFEST="${TARGETS_MANIFEST:-$REPO_ROOT/ci/conformance/targets.json}"

die() { echo "[conformance-suite-run] ERROR: $*" >&2; exit 2; }

# ---------------------------------------------------------------- pure helpers

# All kind:sig target names, in manifest order. kind:feature targets are curated
# [Feature:*] focuses that overlap the sig slices, so including them by default
# would run those specs twice.
sig_targets() {
    jq -r '.[] | select(.kind == "sig") | .name' "$1"
}

# Resolve the partition list: an explicit --targets selection (validated against
# the manifest, order preserved) or every kind:sig target, minus --skip-targets.
# Emits one name per line; a name in neither the manifest nor the sig set is an
# error rather than a silent drop.
resolve_selection() {
    local manifest="$1" want="$2" drop="$3" order="$4" reverse="$5"
    local -a selected=() available=()
    mapfile -t available < <(sig_targets "$manifest")

    if [ -n "$want" ]; then
        local name
        while IFS= read -r name; do
            [ -n "$name" ] || continue
            jq -e --arg t "$name" 'any(.[]; .name == $t)' "$manifest" >/dev/null \
                || { echo "unknown target '$name'" >&2; return 1; }
            selected+=("$name")
        done < <(tr ',' '\n' <<<"$want")
    else
        selected=(${available[@]+"${available[@]}"})
    fi

    if [ -n "$drop" ]; then
        local -a kept=() name
        for name in ${selected[@]+"${selected[@]}"}; do
            grep -qxF "$name" < <(tr ',' '\n' <<<"$drop") || kept+=("$name")
        done
        selected=(${kept[@]+"${kept[@]}"})
    fi

    [ "${#selected[@]}" -gt 0 ] || { echo "empty partition selection" >&2; return 1; }

    local out
    case "$order" in
        registry) out=$(printf '%s\n' "${selected[@]}") ;;
        name)     out=$(printf '%s\n' "${selected[@]}" | sort) ;;
        *)        echo "unknown --order '$order' (registry|name)" >&2; return 1 ;;
    esac
    if [ "$reverse" = "1" ]; then printf '%s\n' "$out" | tac; else printf '%s\n' "$out"; fi
}

# Render the summary table from the TSV rows written per partition.
# Row: target<TAB>status<TAB>passed<TAB>failed<TAB>total<TAB>seconds
render_summary() {
    local tsv="$1"
    printf '%-24s %-6s %7s %7s %7s %9s\n' TARGET STATUS PASSED FAILED TOTAL DURATION
    local t s p f n d tp=0 tf=0 tn=0 td=0
    while IFS=$'\t' read -r t s p f n d; do
        [ -n "$t" ] || continue
        printf '%-24s %-6s %7s %7s %7s %8dm\n' "$t" "$s" "$p" "$f" "$n" "$((d / 60))"
        tp=$((tp + p)); tf=$((tf + f)); tn=$((tn + n)); td=$((td + d))
    done < "$tsv"
    printf '%-24s %-6s %7s %7s %7s %8dm\n' TOTAL '' "$tp" "$tf" "$tn" "$((td / 60))"
}

# When sourced by the unit test, stop here — don't parse args or run.
if [ -n "${SUITE_RUN_LIB_ONLY:-}" ]; then
    return 0 2>/dev/null || true
fi

# ---------------------------------------------------------------------- driver

TARGETS=""; SKIP_TARGETS=""; ORDER="registry"; REVERSE=0
OUTPUT_DIR=""; STOP_ON_INFRA=0
declare -a PASSTHRU=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --targets) [[ $# -ge 2 ]] || die "--targets requires a value"; TARGETS="$2"; shift 2 ;;
        --skip-targets) [[ $# -ge 2 ]] || die "--skip-targets requires a value"; SKIP_TARGETS="$2"; shift 2 ;;
        --order) [[ $# -ge 2 ]] || die "--order requires a value"; ORDER="$2"; shift 2 ;;
        --reverse) REVERSE=1; shift ;;
        --output-dir) [[ $# -ge 2 ]] || die "--output-dir requires a value"; OUTPUT_DIR="$2"; shift 2 ;;
        --stop-on-infra-failure) STOP_ON_INFRA=1; shift ;;
        # Pass-throughs, forwarded verbatim to every partition.
        --kubeconfig|--conformance-image|--hydrophone|--parallel|--preflight-arg)
            [[ $# -ge 2 ]] || die "$1 requires a value"; PASSTHRU+=("$1" "$2"); shift 2 ;;
        --skip-preflight) PASSTHRU+=("$1"); shift ;;
        -h|--help) sed -nE '/^# /,/^$/ s/^# ?//p' "${BASH_SOURCE[0]}" | head -52; exit 0 ;;
        *) die "unknown flag: $1" ;;
    esac
done

command -v jq >/dev/null 2>&1 || die "jq required"
[ -f "$MANIFEST" ] || die "targets manifest not found: $MANIFEST"

SELECTION=$(resolve_selection "$MANIFEST" "$TARGETS" "$SKIP_TARGETS" "$ORDER" "$REVERSE") \
    || die "could not resolve the partition selection (see above)"
mapfile -t PARTITIONS <<<"$SELECTION"

[ -n "$OUTPUT_DIR" ] || OUTPUT_DIR="$REPO_ROOT/.rusternetes/volumes/suite-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUTPUT_DIR"
SUMMARY_TSV="$OUTPUT_DIR/summary.tsv"
: > "$SUMMARY_TSV"

echo "[conformance-suite-run] ${#PARTITIONS[@]} partitions: ${PARTITIONS[*]}"
echo "[conformance-suite-run] output: $OUTPUT_DIR"

SUITE_START=$(date +%s)
INFRA_FAILURES=0

for target in "${PARTITIONS[@]}"; do
    part_dir="$OUTPUT_DIR/$target"
    mkdir -p "$part_dir"
    echo
    echo "==================== partition $target ===================="
    start=$(date +%s)

    # Parse counts through target-run's machine-readable channel rather than
    # scraping stdout, which also carries the whole hydrophone log.
    counts="$part_dir/counts.env"
    : > "$counts"
    set +e
    GITHUB_OUTPUT="$counts" bash "$SCRIPT_DIR/conformance-target-run.sh" \
        --target "$target" \
        --output-dir "$part_dir" \
        ${PASSTHRU[@]+"${PASSTHRU[@]}"}
    rc=$?
    set -e
    elapsed=$(( $(date +%s) - start ))

    passed=$(sed -n 's/^passed=//p' "$counts" | tail -1); passed=${passed:-0}
    failed=$(sed -n 's/^failed=//p' "$counts" | tail -1); failed=${failed:-0}
    total=$(sed -n 's/^total=//p' "$counts" | tail -1); total=${total:-0}

    case "$rc" in
        0) status="ok" ;;
        1) status="INFRA"; INFRA_FAILURES=$((INFRA_FAILURES + 1)) ;;
        *) status="USAGE"; INFRA_FAILURES=$((INFRA_FAILURES + 1)) ;;
    esac

    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$target" "$status" "$passed" "$failed" "$total" "$elapsed" >> "$SUMMARY_TSV"

    # Print the running aggregate now — this is the "faster returns" the split
    # exists for; a reader does not wait for the last partition to see the first.
    echo
    echo "[conformance-suite-run] partition $target done: status=$status passed=$passed failed=$failed total=$total ${elapsed}s"
    echo "[conformance-suite-run] running aggregate after $(wc -l < "$SUMMARY_TSV") partition(s):"
    render_summary "$SUMMARY_TSV" | sed 's/^/  /'

    if [ "$status" != "ok" ] && [ "$STOP_ON_INFRA" -eq 1 ]; then
        echo "[conformance-suite-run] stopping: $target hit an infra failure and --stop-on-infra-failure is set" >&2
        break
    fi
done

SUITE_ELAPSED=$(( $(date +%s) - SUITE_START ))

echo
echo "==================== suite summary ===================="
render_summary "$SUMMARY_TSV" | tee "$OUTPUT_DIR/summary.txt"
echo
echo "wall time: $((SUITE_ELAPSED / 60))m$((SUITE_ELAPSED % 60))s   infra failures: $INFRA_FAILURES"

# Every partition's failing spec names, in one place, so a suite run yields a
# single actionable list without re-reading nine junits.
FAILING="$OUTPUT_DIR/failing-specs.txt"
: > "$FAILING"
for target in "${PARTITIONS[@]}"; do
    log="$OUTPUT_DIR/$target/run.log"
    [ -f "$log" ] || continue
    sed -n 's/^\[FAIL\] //p;s/^  \[FAILED\] //p' "$log" >> "$FAILING" 2>/dev/null || true
done
if [ -s "$FAILING" ]; then
    echo "failing spec lines: $FAILING ($(wc -l < "$FAILING") entries)"
fi

[ "$INFRA_FAILURES" -eq 0 ] || exit 1
exit 0
