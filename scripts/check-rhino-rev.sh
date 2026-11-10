#!/usr/bin/env bash
# Guard: every place that pins the rhino commit must name the same one.
#
# rhino is a git dependency of crates/storage pinned by `rev`. Cargo.lock
# records the resolved commit, and two Dockerfiles build `rhino-server` from the
# same commit through `ARG RHINO_REV`. A pair that drifts ships a different
# rhino in the images than the one the tests ran against (the failure mode the
# old submodule-pin guard, #1624, existed for).
#
# Usage: bash scripts/check-rhino-rev.sh [repo-dir]
set -euo pipefail
repo="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)}"

rev_of() { # label, extracted
  if ! [[ "$2" =~ ^[0-9a-f]{40}$ ]]; then
    echo "error: $1: expected a 40-hex commit, got '${2:-<none>}'" >&2
    exit 1
  fi
}

cargo_rev="$(sed -n 's/^rhino = {.*git = "https:\/\/github.com\/indyjonesnl\/rhino.git".*rev = "\([^"]*\)".*/\1/p' "$repo/crates/storage/Cargo.toml")"
lock_rev="$(sed -n 's|^source = "git+https://github.com/indyjonesnl/rhino.git?rev=[0-9a-f]*#\([0-9a-f]*\)"|\1|p' "$repo/Cargo.lock")"
rhino_docker="$(sed -n 's/^ARG RHINO_REV=\(.*\)$/\1/p' "$repo/rhino.Dockerfile")"
release_docker="$(sed -n 's/^ARG RHINO_REV=\(.*\)$/\1/p' "$repo/release-binaries.Dockerfile")"

rev_of "crates/storage/Cargo.toml" "$cargo_rev"
rev_of "Cargo.lock" "$lock_rev"
rev_of "rhino.Dockerfile" "$rhino_docker"
rev_of "release-binaries.Dockerfile" "$release_docker"

status=0
for pair in "Cargo.lock:$lock_rev" "rhino.Dockerfile:$rhino_docker" "release-binaries.Dockerfile:$release_docker"; do
  if [ "${pair#*:}" != "$cargo_rev" ]; then
    echo "error: ${pair%%:*} pins ${pair#*:} but crates/storage/Cargo.toml pins $cargo_rev" >&2
    status=1
  fi
done
if [ "$status" -eq 0 ]; then
  echo "ok: rhino pinned at $cargo_rev everywhere"
else
  echo "fix: bump all four together, then 'cargo update -p rhino'" >&2
fi
exit "$status"
