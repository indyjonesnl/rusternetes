#!/usr/bin/env bash
# Unit test for scripts/update-badge.sh — both publishing modes:
#   counts  : update-badge.sh <slug> <label> <passed> <total>
#   status  : update-badge.sh --status <slug> <label> <message> <color>
#
# The status mode exists because not every CI signal is a pass count. The
# cert-manager smoke is pass/fail, and a vanilla-swap run that never brings the
# module up reports 0/0 — with only the counts mode those would silently skip
# and leave a STALE green badge on the badges branch. Status mode publishes an
# explicit red badge instead.
#
# The remote is injected via BADGE_REMOTE_URL so the test can push to a local
# bare repo instead of github.com.
#
# Run with: bash scripts/tests/test-update-badge.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
SCRIPT="$REPO_ROOT/scripts/update-badge.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || fail "jq required"

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
BARE="$TMP/remote.git"
git init --quiet --bare "$BARE"

# Seed the orphan `badges` branch the script expects to find.
git init --quiet "$TMP/seed"
(
    cd "$TMP/seed"
    git config user.email t@t; git config user.name t
    git checkout --quiet --orphan badges
    echo badges > README.md
    git add README.md
    git commit --quiet -m init
    git remote add origin "$BARE"
    git push --quiet origin badges
)

export GITHUB_TOKEN=dummy
export GITHUB_REPOSITORY=owner/repo
export BADGE_REMOTE_URL="$BARE"

# Read a published badge file back out of the bare remote.
badge() { git -C "$BARE" show "badges:$1" 2>/dev/null || echo '{}'; }

# --- counts mode -----------------------------------------------------------
bash "$SCRIPT" demo-counts "demo" 9 10 >/dev/null
got="$(badge demo-counts.json)"
[ "$(jq -r .message <<<"$got")" = "90% (9/10)" ] \
    || fail "counts message wrong: $got"
[ "$(jq -r .label <<<"$got")" = "demo" ] || fail "counts label wrong: $got"
[ "$(jq -r .color <<<"$got")" = "green" ] || fail "counts color wrong: $got"
[ "$(jq -r .schemaVersion <<<"$got")" = "1" ] || fail "schemaVersion wrong: $got"

# --- counts mode with total=0 skips (no badge written) ---------------------
bash "$SCRIPT" demo-zero "demo" 0 0 >/dev/null
git -C "$BARE" cat-file -e badges:demo-zero.json 2>/dev/null \
    && fail "total=0 must not publish a badge"

# --- status mode -----------------------------------------------------------
bash "$SCRIPT" --status demo-status "cert-manager smoke" passing brightgreen >/dev/null
got="$(badge demo-status.json)"
[ "$(jq -r .message <<<"$got")" = "passing" ] || fail "status message wrong: $got"
[ "$(jq -r .label <<<"$got")" = "cert-manager smoke" ] || fail "status label wrong: $got"
[ "$(jq -r .color <<<"$got")" = "brightgreen" ] || fail "status color wrong: $got"

# --- status mode publishes failure (the stale-green case) ------------------
bash "$SCRIPT" --status demo-status "cert-manager smoke" failing red >/dev/null
got="$(badge demo-status.json)"
[ "$(jq -r .message <<<"$got")" = "failing" ] || fail "status overwrite failed: $got"
[ "$(jq -r .color <<<"$got")" = "red" ] || fail "status overwrite color: $got"

# --- never fails the caller ------------------------------------------------
( unset GITHUB_TOKEN; bash "$SCRIPT" demo-notoken "demo" 1 1 >/dev/null ) \
    || fail "missing token must exit 0"
BADGE_REMOTE_URL="$TMP/does-not-exist.git" bash "$SCRIPT" demo-nobranch "demo" 1 1 >/dev/null \
    || fail "unreachable remote must exit 0"
bash "$SCRIPT" --status demo-bad "demo" >/dev/null && fail "status mode must reject missing args"

echo "PASS: update-badge.sh counts + status modes"
