#!/usr/bin/env bash
# TDD test for the single-module guard (FR-003 / SC-002):
#   - vs_count_rusternetes_images counts rusternetes image refs
#   - vs_guard_recipe rejects a module resolving to >1 entry (exit 3)
#   - vs_guard_recipe accepts exactly one entry
# Pure logic — no cluster required. Run: bash scripts/vanilla-swap-guard-test.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/vanilla-swap-common.sh
source "$SCRIPT_DIR/vanilla-swap-common.sh"

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
fails=0
ok()  { printf 'ok   - %s\n' "$1"; }
bad() { printf 'FAIL - %s\n' "$1"; fails=$((fails+1)); }

# --- image counting -------------------------------------------------------
count() { printf '%s\n' "$1" | vs_count_rusternetes_images; }

n="$(printf '%s\n' \
  'registry.k8s.io/kube-apiserver:v1.35.0' \
  'ghcr.io/indyjonesnl/rusternetes/kubelet:main' \
  'registry.k8s.io/pause:3.10' | vs_count_rusternetes_images)"
[ "$n" = "1" ] && ok "counts exactly one rusternetes image" || bad "expected 1 rusternetes image, got '$n'"

n="$(printf '%s\n' \
  'ghcr.io/indyjonesnl/rusternetes/kubelet:main' \
  'rusternetes-scheduler:latest' | vs_count_rusternetes_images)"
[ "$n" = "2" ] && ok "counts two rusternetes images (guard would reject)" || bad "expected 2, got '$n'"

n="$(printf '%s\n' 'registry.k8s.io/kube-proxy:v1.35.0' | vs_count_rusternetes_images)"
[ "$n" = "0" ] && ok "counts zero rusternetes images" || bad "expected 0, got '$n'"

# --- vs_guard_recipe: >1 entry for a module is rejected (exit 3) -----------
DUP="$TMP/dup.json"
cat >"$DUP" <<'JSON'
[
 {"module":"kubelet","swap":"join-worker","recipe":"x","readiness":"node-ready"},
 {"module":"kubelet","swap":"join-worker","recipe":"y","readiness":"node-ready"}
]
JSON
( vs_guard_recipe "kubelet" "$DUP" ) 2>/dev/null
rc=$?
[ "$rc" -eq "$VS_EX_GUARD" ] && ok "guard rejects >1 entry with exit $VS_EX_GUARD" || bad "expected exit $VS_EX_GUARD, got $rc"

# --- vs_guard_recipe: exactly one entry accepted --------------------------
( vs_guard_recipe "kubelet" "$SCRIPT_DIR/../ci/vanilla-swap/targets.json" ) 2>/dev/null
rc=$?
[ "$rc" -eq 0 ] && ok "guard accepts exactly one entry" || bad "expected exit 0, got $rc"

echo "---"
[ "$fails" -eq 0 ] && { echo "PASS: all guard tests"; exit 0; } || { echo "FAIL: $fails test(s)"; exit 1; }
