#!/usr/bin/env bash
# Unit tests for the vanilla-swap baseline node-image resolution.
#
# The harness used to pass `--image` only when given a FULL vX.Y.Z, and to let
# `kind` pick its bundled default for a bare vX.Y. kind's default tracks the
# KIND RELEASE, not the requested Kubernetes version, so the same command
# produced different clusters on different machines:
#
#   kind v0.31.0 (CI)     --k8s-version v1.35  ->  kindest/node:v1.35.0
#   kind v0.33.0 (local)  --k8s-version v1.35  ->  kindest/node:v1.37.0
#
# while both logged "k8s=v1.35" and stamped v1.35 into run-result.json. A local
# reproduction of a CI failure therefore ran two minor versions ahead of the
# cluster it was meant to reproduce, and the run result claimed a baseline it
# had not tested (#1889).
#
# Run with: bash scripts/tests/test-vanilla-swap-baseline-version.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
VS_LIB_ONLY=1 . "$REPO_ROOT/scripts/vanilla-swap-common.sh"

PASS=0; FAIL=0
ok()  { PASS=$((PASS + 1)); echo "  ok   - $1"; }
bad() { FAIL=$((FAIL + 1)); echo "  FAIL - $1" >&2; }
check() {
    if [ "$2" = "$3" ]; then ok "$1"; else
        bad "$1"; printf '    expected: %q\n    actual:   %q\n' "$2" "$3" >&2
    fi
}

echo "vs_resolve_node_image"

check "a full vX.Y.Z is used verbatim" \
    "kindest/node:v1.35.4" "$(vs_resolve_node_image v1.35.4)"

check "a bare minor resolves to a PINNED patch, not kind's default" \
    "kindest/node:v1.35.0" "$(vs_resolve_node_image v1.35)"

# The point of the whole exercise: the resolution must never be empty, because
# an empty image argument is exactly what let kind choose.
for v in v1.35 v1.35.0 v1.35.4; do
    img="$(vs_resolve_node_image "$v" 2>/dev/null || true)"
    if [ -n "$img" ]; then ok "resolves $v to a concrete image"; else
        bad "resolves $v to a concrete image"; fi
done

# An unknown minor must FAIL LOUDLY rather than silently fall back to kind's
# default -- the silent fallback is the bug.
if out="$(vs_resolve_node_image v9.99 2>&1)"; then
    bad "an unknown minor is rejected (got: $out)"
else
    ok "an unknown minor is rejected instead of falling back"
fi

echo
echo "vs_create_baseline always pins --image"

# `kind` seam: record the arguments instead of creating a cluster.
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
cat > "$TMP/kind-stub.sh" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$@" > "$KIND_ARGS_FILE"
STUB
chmod +x "$TMP/kind-stub.sh"
export VS_KIND_CMD="$TMP/kind-stub.sh"
export KIND_ARGS_FILE="$TMP/args"

# Skip the post-create assertion, which needs a real cluster.
export VS_SKIP_BASELINE_ASSERT=1

vs_create_baseline testcluster v1.35 >/dev/null 2>&1 || true
if grep -qx "kindest/node:v1.35.0" "$KIND_ARGS_FILE" 2>/dev/null; then
    ok "a bare minor still passes an explicit --image to kind"
else
    bad "a bare minor still passes an explicit --image to kind"
    echo "    args were: $(tr '\n' ' ' < "$KIND_ARGS_FILE" 2>/dev/null)" >&2
fi

vs_create_baseline testcluster v1.35.4 >/dev/null 2>&1 || true
if grep -qx "kindest/node:v1.35.4" "$KIND_ARGS_FILE" 2>/dev/null; then
    ok "a full version pins that exact patch"
else
    bad "a full version pins that exact patch"
fi

echo
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ]
