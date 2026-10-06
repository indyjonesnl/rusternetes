#!/bin/bash
# Regression test for #2301 item 5: bootstrap must not pre-create a
# `default-token` Secret. Upstream never mints a token Secret for a
# ServiceAccount (pkg/controller/serviceaccount/tokens_controller.go
# syncServiceAccount only deletes tokens; syncSecret only populates
# Secrets a user created), and nothing in this repo consumes it.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/scripts"
cp "$REPO/scripts/generate-default-serviceaccounts.sh" "$TMP/scripts/"
bash "$TMP/scripts/generate-default-serviceaccounts.sh" >/dev/null

OUT="$TMP/.rusternetes/default-serviceaccounts.yaml"
fail=0
for ns in default kube-system; do
  grep -q "^kind: ServiceAccount" "$OUT" || { echo "FAIL: no ServiceAccount emitted"; fail=1; }
done
if grep -qE "^kind: Secret|service-account-token|default-token" "$OUT"; then
  echo "FAIL: bootstrap output still contains a token Secret"
  fail=1
fi
[ "$fail" -eq 0 ] && echo "PASS"
exit "$fail"
