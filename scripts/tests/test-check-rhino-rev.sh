#!/usr/bin/env bash
# Tests scripts/check-rhino-rev.sh against copies of the pinned files.
# Run with: bash scripts/tests/test-check-rhino-rev.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
SCRIPT="$REPO_ROOT/scripts/check-rhino-rev.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fresh() { # dir
  rm -rf "$1"; mkdir -p "$1/crates/storage"
  cp "$REPO_ROOT/crates/storage/Cargo.toml" "$1/crates/storage/"
  cp "$REPO_ROOT/Cargo.lock" "$REPO_ROOT/rhino.Dockerfile" "$REPO_ROOT/release-binaries.Dockerfile" "$1/"
}

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

BAD=0000000000000000000000000000000000000000
fresh "$tmp/ok"; check "in sync" 0 "$tmp/ok"

fresh "$tmp/lock"
sed -i "s|\(source = \"git+https://github.com/indyjonesnl/rhino.git?rev=[0-9a-f]*#\)[0-9a-f]*|\1$BAD|" "$tmp/lock/Cargo.lock"
check "Cargo.lock drifted" 1 "$tmp/lock"

fresh "$tmp/rd"; sed -i "s/^ARG RHINO_REV=.*/ARG RHINO_REV=$BAD/" "$tmp/rd/rhino.Dockerfile"
check "rhino.Dockerfile drifted" 1 "$tmp/rd"

fresh "$tmp/rel"; sed -i "s/^ARG RHINO_REV=.*/ARG RHINO_REV=$BAD/" "$tmp/rel/release-binaries.Dockerfile"
check "release-binaries.Dockerfile drifted" 1 "$tmp/rel"

fresh "$tmp/tag"; sed -i 's/rev = "[0-9a-f]*"/tag = "v1"/' "$tmp/tag/crates/storage/Cargo.toml"
check "Cargo.toml not pinned by rev" 1 "$tmp/tag"

exit "$fail"
