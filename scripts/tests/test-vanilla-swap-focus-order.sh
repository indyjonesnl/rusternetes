#!/usr/bin/env bash
# Guard: every vanilla-swap module whose focus is a per-SIG Conformance regex
# must accept BOTH tag orders, exactly like ci/conformance/targets.json does
# (`[sig-X].*[Conformance]|[Conformance].*[sig-X]`). Issue #1701: the
# vanilla-swap registry carried only the forward half, so its slice could drift
# narrower than the conformance target of the same name.
#
# Modules whose focus has no `[sig-` tag (kubelet: `[NodeConformance]`) are
# order-independent by construction and skipped.
#
# Run with: bash scripts/tests/test-vanilla-swap-focus-order.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
VS="$REPO_ROOT/ci/vanilla-swap/targets.json"
CONF="$REPO_ROOT/ci/conformance/targets.json"
fail() { echo "FAIL: $*" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || fail "jq required"

checked=0
# Unit-separator delimited: @tsv would double the backslashes in the regexes.
while IFS=$'\x1f' read -r module target focus; do
  case "$focus" in *'\[sig-'*) ;; *) continue ;; esac
  conf_focus="$(jq -r --arg t "$target" '.[] | select(.name==$t) | .focus' "$CONF")"
  [ -n "$conf_focus" ] || fail "$module: target '$target' not in ci/conformance/targets.json"
  fwd="[$target] Services Foo [Conformance]"
  rev="[Conformance] Foo [$target] Services bar"
  printf '%s\n' "$fwd" | grep -Eq -- "$focus" || fail "$module: focus misses forward order: $focus"
  printf '%s\n' "$rev" | grep -Eq -- "$focus" || fail "$module: focus misses reversed order [Conformance].*[$target]: $focus"
  checked=$((checked + 1))
done < <(jq -r '.[] | [.module, .target, .focus] | join("\u001f")' "$VS")

[ "$checked" -gt 0 ] || fail "no per-SIG focus checked"
echo "PASS: $checked vanilla-swap focus regexes accept both tag orders"
