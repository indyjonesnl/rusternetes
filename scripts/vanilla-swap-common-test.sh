#!/usr/bin/env bash
# TDD test for the isolation-target registry parser/validator
# (vs_validate_registry / vs_resolve_target in vanilla-swap-common.sh).
# Pure logic — no cluster required. Run: bash scripts/vanilla-swap-common-test.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/vanilla-swap-common.sh
source "$SCRIPT_DIR/vanilla-swap-common.sh"

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
fails=0
ok()   { printf 'ok   - %s\n' "$1"; }
bad()  { printf 'FAIL - %s\n' "$1"; fails=$((fails+1)); }

# --- the shipped registry is valid ----------------------------------------
if vs_validate_registry "$SCRIPT_DIR/../ci/vanilla-swap/targets.json" 2>/dev/null; then
  ok "shipped registry validates"
else
  bad "shipped registry should validate"
fi

# helper: write a registry JSON to $TMP/reg.json and echo the path
mkreg() { printf '%s' "$1" >"$TMP/reg.json"; printf '%s\n' "$TMP/reg.json"; }

# --- duplicate module rejected --------------------------------------------
DUP="$(mkreg '[
 {"module":"kubelet","swap":"join-worker","recipe":"ci/vanilla-swap/kind/kubelet-node.yaml","readiness":"node-ready"},
 {"module":"kubelet","swap":"join-worker","recipe":"ci/vanilla-swap/kind/kubelet-node.yaml","readiness":"node-ready"}
]')"
if vs_validate_registry "$DUP" 2>/dev/null; then bad "duplicate module should be rejected"; else ok "duplicate module rejected"; fi

# --- unknown module rejected ----------------------------------------------
UNK="$(mkreg '[{"module":"bogus","swap":"static-pod","recipe":"ci/vanilla-swap/kind/apiserver-patch.yaml","readiness":"readyz"}]')"
if vs_validate_registry "$UNK" 2>/dev/null; then bad "unknown module should be rejected"; else ok "unknown module rejected"; fi

# --- missing recipe file rejected -----------------------------------------
MISS="$(mkreg '[{"module":"kubelet","swap":"join-worker","recipe":"ci/vanilla-swap/kind/nope.yaml","readiness":"node-ready"}]')"
if vs_validate_registry "$MISS" 2>/dev/null; then bad "missing recipe file should be rejected"; else ok "missing recipe file rejected"; fi

# --- wrong entry count rejected (not exactly 5) ---------------------------
ONE="$(mkreg '[{"module":"kubelet","swap":"join-worker","recipe":"ci/vanilla-swap/kind/kubelet-node.yaml","readiness":"node-ready"}]')"
if vs_validate_registry "$ONE" 2>/dev/null; then bad "registry with != 5 entries should be rejected"; else ok "non-5 entry count rejected"; fi

# --- resolve a real target ------------------------------------------------
if vs_resolve_target "kubelet" "$SCRIPT_DIR/../ci/vanilla-swap/targets.json" 2>/dev/null \
   && [ "$VS_MODULE" = "kubelet" ] && [ "$VS_SWAP" = "join-worker" ] \
   && [ "$VS_TARGET" = "sig-node" ] && [ "$VS_READINESS" = "node-ready" ]; then
  ok "resolve kubelet sets expected fields"
else
  bad "resolve kubelet should set module/swap/target/readiness"
fi

# --- resolve unknown module fails -----------------------------------------
if vs_resolve_target "bogus" "$SCRIPT_DIR/../ci/vanilla-swap/targets.json" 2>/dev/null; then
  bad "resolving unknown module should fail"
else
  ok "resolving unknown module fails"
fi

echo "---"
[ "$fails" -eq 0 ] && { echo "PASS: all registry-parser tests"; exit 0; } || { echo "FAIL: $fails test(s)"; exit 1; }
