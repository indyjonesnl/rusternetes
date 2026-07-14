#!/usr/bin/env bash
# Validates ci/conformance/targets.json: valid JSON, unique names, required
# fields present, names path-safe, kind in {sig,feature}, focus/skip are valid
# EREs (Go RE2 ~= ERE for the bracket-escaped tag patterns we use).
#
# A target is a SIG's [Conformance] slice (kind: sig) or a curated feature focus
# (kind: feature). Both drive the same runner/engine/generator.
#
# Run with: bash scripts/tests/test-targets-manifest.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
MANIFEST="$REPO_ROOT/ci/conformance/targets.json"

fail() { echo "FAIL: $*" >&2; exit 1; }

command -v jq >/dev/null 2>&1 || fail "jq required"
[ -f "$MANIFEST" ] || fail "manifest missing: $MANIFEST"

jq -e . "$MANIFEST" >/dev/null 2>&1 || fail "manifest is not valid JSON"
jq -e 'type == "array" and length > 0' "$MANIFEST" >/dev/null \
    || fail "manifest must be a non-empty array"

# Every entry has the required string fields (skip may be empty) and a kind.
jq -e 'all(.[];
        (.name|type=="string" and length>0)
    and (.kind|type=="string")
    and (.focus|type=="string" and length>0)
    and (.skip|type=="string"))' "$MANIFEST" >/dev/null \
    || fail "every entry needs a non-empty string name/focus, a string skip, and a kind"

# kind must be sig or feature.
jq -e 'all(.[].kind; . == "sig" or . == "feature")' "$MANIFEST" >/dev/null \
    || fail "every kind must be \"sig\" or \"feature\""

# Names unique (they become the workflow suffix + badge slug + label).
dupes=$(jq -r '[.[].name] | group_by(.) | map(select(length>1)) | .[][0] // empty' "$MANIFEST")
[ -z "$dupes" ] || fail "duplicate target names: $dupes"

# Names path-safe: lowercase alnum + hyphens (sig-node, sysctls, downward-api).
jq -e 'all(.[].name; test("^[a-z][a-z0-9-]*$"))' "$MANIFEST" >/dev/null \
    || fail "target names must match ^[a-z][a-z0-9-]*\$ (path-safe)"

# sig-kind names must carry the sig- prefix (feature-kind names must NOT).
jq -e 'all(.[]; if .kind == "sig" then (.name|test("^sig-")) else (.name|test("^sig-")|not) end)' "$MANIFEST" >/dev/null \
    || fail "kind:sig names must start with 'sig-'; kind:feature names must not"

# focus/skip compile as EREs (grep -E returns 0 match / 1 no-match / >=2 bad-regex).
# Pull each field with a raw jq per-index read — @tsv would backslash-double the
# regexes and misrepresent them to grep.
n=$(jq length "$MANIFEST")
for i in $(seq 0 $((n - 1))); do
    name=$(jq -r ".[$i].name" "$MANIFEST")
    focus=$(jq -r ".[$i].focus" "$MANIFEST")
    skip=$(jq -r ".[$i].skip // \"\"" "$MANIFEST")
    printf '' | grep -E "$focus" >/dev/null 2>&1 || [ $? -le 1 ] \
        || fail "target '$name': focus is not a valid ERE: $focus"
    if [ -n "$skip" ]; then
        printf '' | grep -E "$skip" >/dev/null 2>&1 || [ $? -le 1 ] \
            || fail "target '$name': skip is not a valid ERE: $skip"
    fi
done

sigs=$(jq '[.[]|select(.kind=="sig")]|length' "$MANIFEST")
feats=$(jq '[.[]|select(.kind=="feature")]|length' "$MANIFEST")
echo "PASS: targets manifest valid ($(jq length "$MANIFEST") targets: $sigs sig, $feats feature)"
