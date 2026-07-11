#!/usr/bin/env bash
# Drift guard: the committed .github/workflows/conformance-sig-<name>.yml files
# must be exactly what scripts/gen-sig-workflows.sh produces from the current
# ci/conformance/sigs.json. Mirrors scripts/tests/test-dockerfile-crate-enumeration.sh.
#
# Run with: bash scripts/tests/test-sig-workflows-sync.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
WF_DIR="$REPO_ROOT/.github/workflows"

fail() { echo "FAIL: $*" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || fail "jq required"

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
bash "$REPO_ROOT/scripts/gen-sig-workflows.sh" --out "$TMP" >/dev/null

# Every SIG in the manifest has a committed file matching the freshly generated one.
while IFS= read -r sig; do
    committed="$WF_DIR/conformance-$sig.yml"
    generated="$TMP/conformance-$sig.yml"
    [ -f "$committed" ] || fail "missing committed workflow for $sig — run scripts/gen-sig-workflows.sh"
    if ! diff -u "$committed" "$generated" >/dev/null; then
        echo "----- drift in conformance-$sig.yml -----" >&2
        diff -u "$committed" "$generated" >&2 || true
        fail "conformance-$sig.yml is out of sync — run scripts/gen-sig-workflows.sh"
    fi
done < <(jq -r '.[].name' "$REPO_ROOT/ci/conformance/sigs.json")

# No stale committed per-SIG workflow that the manifest no longer lists.
for f in "$WF_DIR"/conformance-sig-*.yml; do
    [ -e "$f" ] || continue
    base=$(basename "$f" .yml)          # conformance-sig-node
    sig=${base#conformance-}            # sig-node
    jq -e --arg s "$sig" 'any(.[]; .name == $s)' "$REPO_ROOT/ci/conformance/sigs.json" >/dev/null \
        || fail "stale workflow $f — $sig not in sigs.json"
done

echo "PASS: per-SIG workflows in sync with sigs.json ($(jq length "$REPO_ROOT/ci/conformance/sigs.json") SIGs)"
