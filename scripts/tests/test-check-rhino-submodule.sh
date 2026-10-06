#!/usr/bin/env bash
# Tests scripts/check-rhino-submodule.sh against throwaway git repos (#1624).
# Run with: bash scripts/tests/test-check-rhino-submodule.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
SCRIPT="$REPO_ROOT/scripts/check-rhino-submodule.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=protocol.file.allow GIT_CONFIG_VALUE_0=always

fail=0
check() { # name expected_rc dir
  local rc=0
  bash "$SCRIPT" "$3" >"$tmp/out" 2>&1 || rc=$?
  if [ "$rc" -ne "$2" ]; then
    echo "FAIL: $1 (rc=$rc, want $2)"; cat "$tmp/out"; fail=1
  else
    echo "PASS: $1"
  fi
}

git init -q "$tmp/sub"
git -C "$tmp/sub" commit -q --allow-empty -m one
git -C "$tmp/sub" commit -q --allow-empty -m two
git init -q "$tmp/super"
git -C "$tmp/super" submodule add -q "$tmp/sub" rhino
git -C "$tmp/super" commit -q -m pin

check "in sync" 0 "$tmp/super"

git -C "$tmp/super/rhino" checkout -q HEAD~1
check "desynced (+)" 1 "$tmp/super"
grep -q 'git submodule update --init rhino' "$tmp/out" || { echo "FAIL: fix hint missing"; fail=1; }

git -C "$tmp/super" submodule update -q --init rhino
check "resynced" 0 "$tmp/super"

git -C "$tmp/super" submodule deinit -q -f rhino
check "uninitialised (-)" 1 "$tmp/super"

git init -q "$tmp/plain"
check "not a submodule" 2 "$tmp/plain"

exit "$fail"
