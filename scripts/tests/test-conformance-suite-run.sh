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

echo
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
