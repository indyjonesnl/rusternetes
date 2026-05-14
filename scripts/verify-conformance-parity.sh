#!/usr/bin/env bash
# scripts/verify-conformance-parity.sh
#
# One-shot verification that Hydrophone (--focus='[Conformance]')
# and Sonobuoy (--mode=certified-conformance) execute the same testcase
# set against the running rusternetes cluster.
#
# Outputs to ./parity/:
#   sonobuoy-tests.txt
#   hydrophone-tests.txt
#   diff.txt
#
# Run locally with a cluster already up. Takes ~4-8 hours total
# (two full certified-conformance runs serially).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

KUBECONFIG="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
export KUBECONFIG
CONFORMANCE_IMAGE="${CONFORMANCE_IMAGE:-registry.k8s.io/conformance:v1.35.0}"
HYDROPHONE_BIN="${HYDROPHONE_BIN:-$REPO_ROOT/.bin/hydrophone}"
SONOBUOY_BIN="${SONOBUOY_BIN:-sonobuoy}"
OUT="$REPO_ROOT/parity"

log() { printf '\033[1;34m[parity]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m[parity ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

command -v "$SONOBUOY_BIN" >/dev/null || fail "sonobuoy binary not found"
[[ -x "$HYDROPHONE_BIN" ]] || fail "hydrophone not found at $HYDROPHONE_BIN (run run-ci-conformance.sh once to install)"
command -v python3 >/dev/null || fail "python3 not installed"

mkdir -p "$OUT" "$REPO_ROOT/tests/conformance"

extract_names() {
    local junit="$1" out="$2"
    python3 - "$junit" <<'PY' | sort -u > "$out"
import sys, xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
ts = root if root.tag == "testsuite" else root.find("testsuite")
if ts is None:
    sys.exit("no testsuite element")
for tc in ts.findall("testcase"):
    n = tc.get("name", "")
    if n:
        print(n)
PY
}

run_sonobuoy() {
    log "running sonobuoy certified-conformance (~2-4h)"
    "$SONOBUOY_BIN" delete --wait || true
    "$SONOBUOY_BIN" run \
        --mode=certified-conformance \
        --timeout 86400 \
        --plugin-env "e2e.E2E_EXTRA_ARGS=--kube-api-content-type=application/json" \
        --wait
    local tar; tar="$("$SONOBUOY_BIN" retrieve)"
    local extract; extract="$(mktemp -d)"
    tar -xzf "$tar" -C "$extract"
    extract_names "$extract/plugins/e2e/results/global/junit_01.xml" "$OUT/sonobuoy-tests.txt"
    "$SONOBUOY_BIN" delete --wait || true
}

run_hydrophone() {
    log "running hydrophone --conformance (~2-4h)"
    local hout="$OUT/hydrophone-run"
    rm -rf "$hout"; mkdir -p "$hout"
    "$HYDROPHONE_BIN" \
        --kubeconfig "$KUBECONFIG" \
        --focus '\[Conformance\]' \
        --conformance-image "$CONFORMANCE_IMAGE" \
        --output-dir "$hout" \
        --extra-args "--kube-api-content-type=application/json" || true
    extract_names "$hout/junit_01.xml" "$OUT/hydrophone-tests.txt"
}

run_sonobuoy
run_hydrophone

diff -u "$OUT/sonobuoy-tests.txt" "$OUT/hydrophone-tests.txt" > "$OUT/diff.txt" || true

if [[ -s "$OUT/diff.txt" ]]; then
    log "PARITY DIFF FOUND — see $OUT/diff.txt"
    head -40 "$OUT/diff.txt" >&2
    exit 2
else
    log "PARITY OK — $(wc -l < "$OUT/sonobuoy-tests.txt") testcases, identical set"
    cp "$OUT/sonobuoy-tests.txt" "$REPO_ROOT/tests/conformance/parity-fixture.txt"
    log "wrote fixture: tests/conformance/parity-fixture.txt"
fi
