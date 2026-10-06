#!/usr/bin/env bash
# Guard: the rhino submodule checkout must match the superproject's pin (#1624).
#
# `git submodule status rhino` prefixes the SHA with:
#   ' ' in sync        '+' checked out at a different commit than the pin
#   '-' not initialised   'U' merge conflict
# A drifted rhino silently compiles pre-fix code (seen: checked out 7ec61cb,
# pinned a7dd039, missing the atomic-insert fix). Fail fast on anything but ' '.
#
# Usage: bash scripts/check-rhino-submodule.sh [repo-dir]
set -euo pipefail

command -v git >/dev/null 2>&1 || { echo "error: git not found" >&2; exit 2; }

repo="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)}"
status="$(git -C "$repo" submodule status rhino 2>&1)" || {
  echo "error: 'git submodule status rhino' failed in $repo: $status" >&2
  exit 2
}
if [ -z "$status" ]; then
  echo "error: rhino is not a registered submodule of $repo" >&2
  exit 2
fi

case "$status" in
  ' '*)
    echo "ok: rhino submodule matches the superproject pin (${status# })"
    ;;
  *)
    marker="$(printf '%s' "$status" | cut -c1)"
    case "$marker" in
      '+') why="checked out at a different commit than the superproject pin" ;;
      '-') why="not initialised" ;;
      'U') why="has merge conflicts" ;;
      *) why="in an unknown state" ;;
    esac
    echo "error: rhino submodule is $why:" >&2
    echo "  $status" >&2
    echo "fix: git submodule update --init rhino" >&2
    exit 1
    ;;
esac
