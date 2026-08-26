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

# A branch build must stay reachable from a dispatch. The engine builds from
# source on an EMPTY image-tag, but an empty dispatch value never arrives:
# GitHub substitutes the input's own `default: main`. So the workflows map a
# literal `local` sentinel to the empty string. Without it, a run dispatched to
# prove a PR silently tests the published image and reports on unrelated code.
for f in "$WF_DIR"/conformance-*.yml; do
    [ -e "$f" ] || continue
    grep -qF "$MARKER" "$f" || continue          # generated files only
    grep -qF "github.event.inputs.image-tag == 'local'" "$f" \
        || fail "$(basename "$f"): image-tag must map the 'local' sentinel to an empty value, or a branch build is unreachable"
done

echo "PASS: per-target workflows in sync with targets.json ($(jq length "$MANIFEST") targets)"
