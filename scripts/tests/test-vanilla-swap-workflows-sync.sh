#!/usr/bin/env bash
# Drift guard: the committed .github/workflows/vanilla-swap-<module>.yml files
# must be exactly what scripts/gen-vanilla-swap-workflows.sh produces from the
# current ci/vanilla-swap/targets.json. Mirrors
# scripts/tests/test-target-workflows-sync.sh.
#
# Also asserts the invariant the split exists for: every module gets its OWN
# workflow, and no workflow runs more than one module (no matrix).
#
# Stale detection is MARKER-based (not a filename glob): generated files carry
# the generator's GENERATED marker, so the hand-written engine
# (vanilla-swap-module.yml) is never mistaken for a generated caller.
#
# Run with: bash scripts/tests/test-vanilla-swap-workflows-sync.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
WF_DIR="$REPO_ROOT/.github/workflows"
GEN="$REPO_ROOT/scripts/gen-vanilla-swap-workflows.sh"
MANIFEST="$REPO_ROOT/ci/vanilla-swap/targets.json"

fail() { echo "FAIL: $*" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || fail "jq required"

MARKER="$(bash "$GEN" --print-marker)"

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
bash "$GEN" --out "$TMP" >/dev/null

# Every module in the manifest has a committed file matching the fresh generation.
while IFS= read -r module; do
    committed="$WF_DIR/vanilla-swap-$module.yml"
    generated="$TMP/vanilla-swap-$module.yml"
    [ -f "$committed" ] || fail "missing committed workflow for $module — run scripts/gen-vanilla-swap-workflows.sh"
    if ! diff -u "$committed" "$generated" >/dev/null; then
        echo "----- drift in vanilla-swap-$module.yml -----" >&2
        diff -u "$committed" "$generated" >&2 || true
        fail "vanilla-swap-$module.yml is out of sync — run scripts/gen-vanilla-swap-workflows.sh"
    fi
done < <(jq -r '.[].module' "$MANIFEST")

# No stale GENERATED workflow that the manifest no longer lists.
for f in "$WF_DIR"/vanilla-swap-*.yml; do
    [ -e "$f" ] || continue
    grep -qF "$MARKER" "$f" || continue          # skip the hand-written engine
    base=$(basename "$f" .yml)                    # vanilla-swap-kubelet
    module=${base#vanilla-swap-}                  # kubelet
    jq -e --arg m "$module" 'any(.[]; .module == $m)' "$MANIFEST" >/dev/null \
        || fail "stale generated workflow $f — $module not in targets.json"
done

# The point of the split: no vanilla-swap workflow may fan out over modules.
# A `strategy: matrix` here would put several modules back into one run, whose
# single status and single badge could no longer attribute a failure.
for f in "$WF_DIR"/vanilla-swap*.yml; do
    [ -e "$f" ] || continue
    if grep -qE '^[[:space:]]*(strategy:|matrix:)' "$f"; then
        fail "$f uses a matrix — vanilla-swap must run exactly one module per workflow"
    fi
done
if [ -e "$WF_DIR/vanilla-swap.yml" ]; then
    fail "the aggregate vanilla-swap.yml is back — it ran all modules in one run; use the per-module workflows"
fi

echo "PASS: per-module vanilla-swap workflows in sync with targets.json ($(jq length "$MANIFEST") modules)"
