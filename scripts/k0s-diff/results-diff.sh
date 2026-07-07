#!/usr/bin/env bash
# scripts/k0s-diff/results-diff.sh — summarize / grid / regression report over
# results/<vN>/<sig>/ produced by run-variant.sh.
#
# Pass/fail counting and per-test status are both delegated to
# parse-results.py (e2e.log-primary / junit-fallback for counts, junit-only
# for per-test names) — the SAME parser run-variant.sh uses for its own
# per-run "PASS n / FAIL m" line, so this script's numbers can never drift
# from what run-variant already reported.
#
# Modes:
#   summarize <dir>
#     "PASS n / FAIL m" for one results dir, e.g. results/v0/sig-node.
#
#   grid
#     Variant x sig PASS/FAIL table across VARIANTS x SIGS (lib.sh order),
#     "-" where results/<v>/<s> doesn't exist. Written to results/GRID.tsv
#     AND printed to stdout.
#
#   <vN> <baseline-vN> <sig>
#     Regression mode: [Conformance] tests that PASS in the baseline
#     results/<baseline-vN>/<sig> junit but FAIL or are ABSENT (explicit
#     failure, skipped, or missing entirely) in results/<vN>/<sig>. One
#     "REGRESSED in <vN>: <test name>" line per regressed test, sorted.
#     Typical baseline is v0. Example: results-diff.sh v6 v0 sig-network
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
source "$here/lib.sh"

usage() {
  echo "usage: results-diff.sh summarize <dir> | grid | <vN> <baseline-vN> <sig>" >&2
  exit 1
}

# Shared by both the `summarize` mode and `grid`'s per-cell lookups so grid
# doesn't re-exec this script per cell.
summarize_dir() {
  local d="$1" p f
  read -r p f < <(python3 "$here/parse-results.py" summarize "$d")
  echo "PASS ${p:-0} / FAIL ${f:-0}"
}

cmd="${1:-}"
[ -n "$cmd" ] || usage

case "$cmd" in
  summarize)
    d="${2:?usage: results-diff.sh summarize <dir>}"
    summarize_dir "$d"
    ;;

  grid)
    mkdir -p "$here/results"
    out="$here/results/GRID.tsv"
    {
      printf 'variant'
      for s in "${SIGS[@]}"; do printf '\t%s' "$s"; done
      printf '\n'
      for v in "${VARIANTS[@]}"; do
        printf '%s' "$v"
        for s in "${SIGS[@]}"; do
          d="$here/results/$v/$s"
          if [ -d "$d" ]; then
            printf '\t%s' "$(summarize_dir "$d")"
          else
            printf '\t-'
          fi
        done
        printf '\n'
      done
    } | tee "$out"
    ;;

  *)
    # Regression mode: results-diff.sh <vN> <baseline-vN> <sig>
    v="$cmd"
    base="${2:?usage: results-diff.sh <vN> <baseline-vN> <sig>}"
    sig="${3:?usage: results-diff.sh <vN> <baseline-vN> <sig>}"
    basedir="$here/results/$base/$sig"
    vdir="$here/results/$v/$sig"
    python3 "$here/parse-results.py" regressions "$basedir" "$vdir" \
      | sed "s|^|REGRESSED in $v: |"
    ;;
esac
