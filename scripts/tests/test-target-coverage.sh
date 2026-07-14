#!/usr/bin/env bash
# Coverage guard (FR-016 / FR-002): every [Conformance] spec's [sig-xxx] label
# must be represented by a kind:sig entry in ci/conformance/targets.json, so the
# union of the per-SIG focuses covers the whole [Conformance] set and no spec is
# silently dropped. (kind:feature targets are curated [Feature:*] focuses and are
# NOT part of this completeness guarantee.)
#
# Sources of the spec list, in order of preference:
#   --from-file F   read a ginkgo dry-run dump from F
#   --image IMG     `docker run IMG e2e.test --ginkgo.dry-run` (full, authoritative)
#   (default)       a committed sample fixture — exercises the scan logic offline
#                   in the cheap PR validate job. FULL-set proof needs --image on
#                   the conformance runner; this default does NOT prove full coverage.
#
# A [Conformance] spec whose [sig-xxx] is absent from the manifest is "uncovered"
# and fails the check — add a kind:sig entry (e.g. sig-other) rather than
# dropping it.
#
# Run with: bash scripts/tests/test-target-coverage.sh [--from-file F | --image IMG]
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
MANIFEST="$REPO_ROOT/ci/conformance/targets.json"
FIXTURE="$REPO_ROOT/scripts/tests/testdata/sig-coverage-sample.txt"

fail() { echo "FAIL: $*" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || fail "jq required"
[ -f "$MANIFEST" ] || fail "manifest missing: $MANIFEST"

FROM_FILE=""; IMAGE=""; USING="fixture"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --from-file) FROM_FILE="$2"; USING="file:$2"; shift 2 ;;
        --image) IMAGE="$2"; USING="image:$2"; shift 2 ;;
        -h|--help) sed -nE '/^# /,/^$/ s/^# ?//p' "${BASH_SOURCE[0]}" | head -25; exit 0 ;;
        *) fail "unknown flag: $1" ;;
    esac
done

get_specs() {
    if [ -n "$FROM_FILE" ]; then
        [ -f "$FROM_FILE" ] || fail "--from-file not found: $FROM_FILE"
        cat "$FROM_FILE"
    elif [ -n "$IMAGE" ]; then
        command -v docker >/dev/null 2>&1 || fail "docker required for --image"
        docker run --rm "$IMAGE" /usr/local/bin/e2e.test --ginkgo.dry-run --ginkgo.no-color 2>/dev/null
    else
        cat "$FIXTURE"
    fi
}

# kind:sig target names in the manifest, one per line.
manifest_sigs=$(jq -r '.[] | select(.kind=="sig") | .name' "$MANIFEST" | sort -u)

# SIGs that appear on [Conformance] spec lines, one per line.
conformance_sigs=$(get_specs \
    | { grep -E '\[Conformance\]' || true; } \
    | grep -oE '\[sig-[a-z-]+\]' \
    | tr -d '[]' \
    | sort -u)

[ -n "$conformance_sigs" ] || fail "no [Conformance] specs with a [sig-*] tag found in the spec list ($USING)"

# Any conformance SIG not covered by a kind:sig entry => uncovered.
uncovered=$(comm -23 <(echo "$conformance_sigs") <(echo "$manifest_sigs") || true)
if [ -n "$uncovered" ]; then
    echo "uncovered [Conformance] SIGs (no kind:sig entry in targets.json):" >&2
    echo "$uncovered" | sed 's/^/  - /' >&2
    fail "every [Conformance] SIG must have a kind:sig entry in targets.json (add it, or an explicit sig-other) — FR-016"
fi

n_conf=$(echo "$conformance_sigs" | grep -c .)
echo "PASS: all $n_conf [Conformance] SIGs covered by kind:sig targets (source: $USING)"
