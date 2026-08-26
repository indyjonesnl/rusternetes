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

# A branch build must stay reachable from a dispatch. The bring-up action
# compiles this ref when image-tag is EMPTY, but an empty dispatch value never
# arrives: GitHub substitutes the input's own `default: main`. Hence the `local`
# sentinel -- and it MUST be resolved in a shell step, not a `${{ }}` expression,
# because an expression cannot yield an empty string (`A && '' || B` takes the
# B branch, since '' is falsy). Two merged attempts got this wrong (#1758,
# #1760) and silently tested the published image instead of the branch.
ENGINE="$WF_DIR/conformance-target.yml"
[ -f "$ENGINE" ] || fail "missing reusable engine workflow: $ENGINE"

grep -qF 'if [ "$REQUESTED" = "local" ]' "$ENGINE" \
    || fail "conformance-target.yml must resolve the 'local' sentinel in a shell step (see #1760: a \${{ }} expression cannot produce an empty string)"

grep -qF 'image-tag: ${{ steps.tag.outputs.value }}' "$ENGINE" \
    || fail "conformance-target.yml must pass the RESOLVED tag to bring-up-cluster, not inputs.image-tag directly"

# The callers forward the raw value; mapping there is what failed before.
for f in "$WF_DIR"/conformance-*.yml; do
    [ -e "$f" ] || continue
    grep -qF "$MARKER" "$f" || continue          # generated files only
    if grep -qF "github.event.inputs.image-tag == 'local'" "$f"; then
        fail "$(basename "$f"): map the 'local' sentinel in the engine's shell step, not in a caller expression (#1760)"
    fi
done

echo "PASS: per-target workflows in sync with targets.json ($(jq length "$MANIFEST") targets)"
