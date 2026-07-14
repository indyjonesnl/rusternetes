#!/usr/bin/env bash
# Seed / refresh the kind:sig entries of ci/conformance/targets.json from the
# conformance image's ginkgo spec list. MAINTENANCE TOOL — not in the CI path.
# Run it to bootstrap the SIG targets and on every k8s-version bump to catch new
# [sig-xxx] labels in the [Conformance] set, then hand-review the printed diff
# before merging into the manifest. (kind:feature targets are curated by hand
# and are NOT emitted here.)
#
# It lists every spec, keeps the [Conformance] ones, extracts each distinct
# [sig-xxx] label, and emits a JSON skeleton (one kind:sig entry per SIG, with
# the canonical two-order focus regex and default skip) to stdout. It NEVER
# writes the manifest itself — merge the sig entries deliberately after review.
#
# Usage:
#   bash scripts/conformance-targets-discover.sh \
#       [--conformance-image IMG] [--from-file SPEC_DUMP] [--diff]
#
#   --from-file F   Read the ginkgo dry-run dump from F instead of running the
#                   image (used by the unit test; also handy offline).
#   --diff          Print a name-level diff vs the committed manifest to stderr.
set -euo pipefail
IFS=$'\n\t'
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
MANIFEST="$REPO_ROOT/ci/conformance/targets.json"

IMAGE="registry.k8s.io/conformance:v1.35.0"
FROM_FILE=""; DO_DIFF=0
die() { echo "[targets-discover] ERROR: $*" >&2; exit 2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --conformance-image) [[ $# -ge 2 ]] || die "--conformance-image requires a value"; IMAGE="$2"; shift 2 ;;
        --from-file) [[ $# -ge 2 ]] || die "--from-file requires a value"; FROM_FILE="$2"; shift 2 ;;
        --diff) DO_DIFF=1; shift ;;
        -h|--help) sed -nE '/^# /,/^$/ s/^# ?//p' "${BASH_SOURCE[0]}" | head -30; exit 0 ;;
        *) die "unknown flag: $1" ;;
    esac
done

get_specs() {
    if [ -n "$FROM_FILE" ]; then
        [ -f "$FROM_FILE" ] || die "--from-file: file not found: $FROM_FILE"
        cat "$FROM_FILE"
    else
        docker run --rm "$IMAGE" \
            /usr/local/bin/e2e.test --ginkgo.dry-run --ginkgo.no-color 2>/dev/null
    fi
}

# Every distinct [sig-xxx] that co-occurs with [Conformance], one per line,
# turned into a manifest skeleton entry. `|| true` keeps an empty dump from
# aborting under pipefail.
emitted=$(
get_specs \
| { grep -E '\[Conformance\]' || true; } \
| grep -oE '\[sig-[a-z-]+\]' \
| tr -d '[]' \
| sort -u \
| jq -R -s '
    split("\n") | map(select(length>0)) | map({
        name: .,
        kind: "sig",
        focus: ("\\[" + . + "\\].*\\[Conformance\\]|\\[Conformance\\].*\\[" + . + "\\]"),
        skip: "\\[Flaky\\]",
        description: ""
      })
  '
)

echo "$emitted"

if [ "$DO_DIFF" -eq 1 ] && [ -f "$MANIFEST" ]; then
    new=$(comm -23 <(echo "$emitted" | jq -r '.[].name' | sort) <(jq -r '.[].name' "$MANIFEST" | sort))
    gone=$(comm -13 <(echo "$emitted" | jq -r '.[].name' | sort) <(jq -r '.[].name' "$MANIFEST" | sort))
    {
        echo "[targets-discover] diff vs $MANIFEST:"
        echo "  added:   ${new:-<none>}"
        echo "  removed: ${gone:-<none>}"
    } >&2
fi
