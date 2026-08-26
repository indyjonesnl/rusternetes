#!/usr/bin/env bash
# Drift guard: the committed .github/workflows/conformance-<name>.yml files must
# be exactly what scripts/gen-target-workflows.sh produces from the current
# ci/conformance/targets.json. Mirrors scripts/tests/test-dockerfile-crate-enumeration.sh.
#
# Stale detection is MARKER-based (not a filename glob): generated files carry
# the generator's GENERATED marker, so hand-written conformance-*.yml files
# (conformance-target.yml, conformance-validate.yml, node-conformance.yml) are
# never mistaken for generated per-target callers.
#
# Run with: bash scripts/tests/test-target-workflows-sync.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
WF_DIR="$REPO_ROOT/.github/workflows"
GEN="$REPO_ROOT/scripts/gen-target-workflows.sh"
MANIFEST="$REPO_ROOT/ci/conformance/targets.json"

fail() { echo "FAIL: $*" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || fail "jq required"

MARKER="$(bash "$GEN" --print-marker)"

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
bash "$GEN" --out "$TMP" >/dev/null

# Every target in the manifest has a committed file matching the freshly generated one.
while IFS= read -r target; do
    committed="$WF_DIR/conformance-$target.yml"
    generated="$TMP/conformance-$target.yml"
    [ -f "$committed" ] || fail "missing committed workflow for $target — run scripts/gen-target-workflows.sh"
    if ! diff -u "$committed" "$generated" >/dev/null; then
        echo "----- drift in conformance-$target.yml -----" >&2
        diff -u "$committed" "$generated" >&2 || true
        fail "conformance-$target.yml is out of sync — run scripts/gen-target-workflows.sh"
    fi
done < <(jq -r '.[].name' "$MANIFEST")

# No stale GENERATED workflow that the manifest no longer lists. A file is
# "generated" iff it carries the marker — so this ignores hand-written workflows.
for f in "$WF_DIR"/conformance-*.yml; do
    [ -e "$f" ] || continue
    grep -qF "$MARKER" "$f" || continue          # skip non-generated files
    base=$(basename "$f" .yml)                    # conformance-sig-node
    target=${base#conformance-}                   # sig-node
    jq -e --arg t "$target" 'any(.[]; .name == $t)' "$MANIFEST" >/dev/null \
        || fail "stale generated workflow $f — $target not in targets.json"
done

# An explicitly-empty `image-tag` must reach the engine as empty ("build the
# branch"), not collapse back to `main`. `${{ inputs.image-tag || 'main' }}`
# looks right but is wrong: an empty string is falsy in GitHub expressions, so
# `-f image-tag=''` silently tested the published image instead of the change
# under review, making every pre-merge conformance verdict meaningless.
for f in "$WF_DIR"/conformance-*.yml; do
    [ -e "$f" ] || continue
    grep -qF "$MARKER" "$f" || continue          # generated files only
    if grep -qF "github.event.inputs.image-tag || 'main'" "$f"; then
        fail "$(basename "$f"): \`inputs.image-tag || 'main'\` swallows an explicit empty tag — use \`== null &&\` so a branch build is reachable"
    fi
    grep -qF "github.event.inputs.image-tag == null && 'main'" "$f" \
        || fail "$(basename "$f"): image-tag must default via \`== null &&\` so an explicit empty value survives"
done

echo "PASS: per-target workflows in sync with targets.json ($(jq length "$MANIFEST") targets)"
