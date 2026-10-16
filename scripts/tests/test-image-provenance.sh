#!/usr/bin/env bash
# Tests scripts/image-provenance.sh with a stubbed `imagetools` (#1696).
# Run with: bash scripts/tests/test-image-provenance.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
SCRIPT="$REPO_ROOT/scripts/image-provenance.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Stub: digest is looked up in $STUB_DIGESTS as "<ref> <digest>"; missing = fail.
cat >"$tmp/imagetools" <<'STUB'
#!/usr/bin/env bash
ref="${!#}"
d="$(awk -v r="$ref" '$1==r{print $2}' "$STUB_DIGESTS")"
[ -n "$d" ] || exit 1
echo "$d"
STUB
chmod +x "$tmp/imagetools"
export STUB_DIGESTS="$tmp/digests"
export IMAGETOOLS="$tmp/imagetools"
P=ghcr.io/indyjonesnl/rusternetes/api-server

fail=0
run() { # name want_rc want_pattern tag sha
  local rc=0
  bash "$SCRIPT" "$4" "$5" >"$tmp/out" 2>&1 || rc=$?
  if [ "$rc" -ne "$2" ] || ! grep -q -- "$3" "$tmp/out"; then
    echo "FAIL: $1 (rc=$rc, want $2, pattern '$3')"; cat "$tmp/out"; fail=1
  else
    echo "PASS: $1"
  fi
}

printf '%s %s\n' "$P:main" sha256:aaa "$P:sha-abc" sha256:aaa >"$tmp/digests"
run "main == sha tag is current" 0 'sha256:aaa' main abc
if grep -q '::warning::' "$tmp/out"; then echo "FAIL: spurious warning"; fail=1; fi

printf '%s %s\n' "$P:main" sha256:old "$P:sha-abc" sha256:new >"$tmp/digests"
run "stale main warns, still rc 0" 0 '::warning::.*stale' main abc

printf '%s %s\n' "$P:main" sha256:aaa >"$tmp/digests"
run "sha tag unpublished warns" 0 '::warning::.*sha-abc' main abc

printf '%s %s\n' "$P:sha-def" sha256:ddd >"$tmp/digests"
run "pinned sha tag just records digest" 0 'sha256:ddd' sha-def abc

: >"$tmp/digests"
run "unresolvable tag warns, rc 0" 0 '::warning::.*sha-def' sha-def abc

run "local tag is skipped" 0 'built from source' local abc
exit "$fail"
