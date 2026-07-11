#!/usr/bin/env bash
# Validates ci/conformance/sigs.json: valid JSON, unique names, required
# fields present, names path-safe, focus/skip are valid EREs (Go RE2 ~= ERE
# for the bracket-escaped tag patterns we use).
#
# Companion to test-features-manifest.sh (features.json is the per-[Feature]
# axis; sigs.json is the coarser per-SIG [Conformance] axis).
#
# Run with: bash scripts/tests/test-sigs-manifest.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
MANIFEST="$REPO_ROOT/ci/conformance/sigs.json"

fail() { echo "FAIL: $*" >&2; exit 1; }

command -v jq >/dev/null 2>&1 || fail "jq required"
[ -f "$MANIFEST" ] || fail "manifest missing: $MANIFEST"

jq -e . "$MANIFEST" >/dev/null 2>&1 || fail "manifest is not valid JSON"
jq -e 'type == "array" and length > 0' "$MANIFEST" >/dev/null \
    || fail "manifest must be a non-empty array"

# Every entry has the required string fields (skip may be empty).
jq -e 'all(.[];
        (.name|type=="string" and length>0)
    and (.focus|type=="string" and length>0)
    and (.skip|type=="string"))' "$MANIFEST" >/dev/null \
    || fail "every entry needs a non-empty string name/focus and a string skip"

# Names unique.
dupes=$(jq -r '[.[].name] | group_by(.) | map(select(length>1)) | .[][0] // empty' "$MANIFEST")
[ -z "$dupes" ] || fail "duplicate SIG names: $dupes"

# Names must be sig-* and path-safe (used as workflow suffix + badge slug + label).
jq -e 'all(.[].name; test("^sig-[a-z][a-z-]*$"))' "$MANIFEST" >/dev/null \
    || fail "SIG names must match ^sig-[a-z][a-z-]*\$ (path-safe)"

# focus/skip compile as EREs (grep -E returns 0 match / 1 no-match / >=2 bad-regex).
# Pull each field with a raw jq per-index read — @tsv would backslash-double the
# regexes and misrepresent them to grep.
n=$(jq length "$MANIFEST")
for i in $(seq 0 $((n - 1))); do
    name=$(jq -r ".[$i].name" "$MANIFEST")
    focus=$(jq -r ".[$i].focus" "$MANIFEST")
    skip=$(jq -r ".[$i].skip // \"\"" "$MANIFEST")
    printf '' | grep -E "$focus" >/dev/null 2>&1 || [ $? -le 1 ] \
        || fail "SIG '$name': focus is not a valid ERE: $focus"
    if [ -n "$skip" ]; then
        printf '' | grep -E "$skip" >/dev/null 2>&1 || [ $? -le 1 ] \
            || fail "SIG '$name': skip is not a valid ERE: $skip"
    fi
done

echo "PASS: sigs manifest valid ($(jq length "$MANIFEST") SIGs)"
