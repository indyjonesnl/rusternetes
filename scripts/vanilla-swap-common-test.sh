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

# --- render a recipe without envsubst --------------------------------------
RENDER_RECIPE="$TMP/render-recipe.yaml"
cat >"$RENDER_RECIPE" <<'YAML'
template: |
  image: ${VS_IMAGE}
  server: ${VS_APISERVER_URL}
  cidr: ${VS_CLUSTER_CIDR}
  nodePorts: ${VS_NODEPORT_RANGE}
  untouched: ${NOT_ALLOWED}
nextField: outside-template
YAML

AWK_ONLY_BIN="$TMP/awk-only-bin"
mkdir -p "$AWK_ONLY_BIN"
ln -s "$(command -v awk)" "$AWK_ONLY_BIN/awk"

VS_IMAGE="ghcr.io/indyjonesnl/rusternetes/kube-proxy:test"
VS_APISERVER_URL="https://172.18.0.3:6443"
VS_CLUSTER_CIDR="10.96.0.0/16"
VS_NODEPORT_RANGE="30000-32767"
EXPECTED_RENDER='image: ghcr.io/indyjonesnl/rusternetes/kube-proxy:test
server: https://172.18.0.3:6443
cidr: 10.96.0.0/16
nodePorts: 30000-32767
untouched: ${NOT_ALLOWED}'

if RENDERED="$(PATH="$AWK_ONLY_BIN" vs_render_recipe_template "$RENDER_RECIPE" \
  VS_IMAGE VS_APISERVER_URL VS_CLUSTER_CIDR VS_NODEPORT_RANGE)" \
  && [ "$RENDERED" = "$EXPECTED_RENDER" ]; then
  ok "recipe template renders without envsubst and preserves unlisted variables"
else
  bad "recipe template should render with only awk available"
fi

# --- render values in one pass --------------------------------------------
VS_IMAGE='ghcr.io/example/kube-proxy:${VS_APISERVER_URL}'
EXPECTED_ONE_PASS_RENDER='image: ghcr.io/example/kube-proxy:${VS_APISERVER_URL}
server: https://172.18.0.3:6443
cidr: 10.96.0.0/16
nodePorts: 30000-32767
untouched: ${NOT_ALLOWED}'

if RENDERED="$(vs_render_recipe_template "$RENDER_RECIPE" \
  VS_IMAGE VS_APISERVER_URL VS_CLUSTER_CIDR VS_NODEPORT_RANGE)" \
  && [ "$RENDERED" = "$EXPECTED_ONE_PASS_RENDER" ]; then
  ok "recipe template does not recursively render placeholder values"
else
  bad "recipe template should render placeholder values in one pass"
fi

# --- unset requested template variable rejected ---------------------------
unset VS_UNSET_RENDER_VALUE
if (set +u; vs_render_recipe_template "$RENDER_RECIPE" VS_UNSET_RENDER_VALUE) \
  >"$TMP/unset-render.out" 2>"$TMP/unset-render.err"; then
  bad "unset requested template variable should be rejected"
else
  ok "unset requested template variable rejected"
fi

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
