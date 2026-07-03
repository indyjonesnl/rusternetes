#!/usr/bin/env bash
# Regression guard for #1153 in the cert-manager smoke path.
#
# scripts/run-cert-manager-smoke.sh brings the SQLite stack up itself. The
# node-1 kubelet bind-mounts the certs dir at ${CERTS_PATH}:${CERTS_PATH}
# (compose.sqlite.yml) and bootstrap-cluster.sh later templates that SAME
# host-absolute path into the control-plane static-pod hostPath volumes. If
# CERTS_PATH is not exported BEFORE the first `compose up`, the kubelet falls
# back to /etc/rusternetes/certs, the static pods' `type: Directory` hostPath
# check fails, they stay Pending, the controller-manager never runs, and every
# Deployment (rusternetes-dns, cert-manager) is stuck with zero pods — the
# rollout wait then times out. This kept the cert-manager Smoke workflow red.
#
# This is a static check: no cluster needed. It fails if the CERTS_PATH export
# is missing or is ordered AFTER the first `compose ... up` invocation.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SMOKE="${PROJECT_ROOT}/scripts/run-cert-manager-smoke.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }

[ -f "${SMOKE}" ] || fail "smoke script not found at ${SMOKE}"

# Line number of the CERTS_PATH export.
certs_line="$(grep -nE '^[[:space:]]*export[[:space:]]+CERTS_PATH=' "${SMOKE}" | head -1 | cut -d: -f1 || true)"
[ -n "${certs_line}" ] || fail "run-cert-manager-smoke.sh does not export CERTS_PATH (regression #1153)"

# Line number of the first `compose ... up` that starts the stack. The teardown
# lines only ever run `compose ... down`, so matching ` up` is unambiguous.
up_line="$(grep -nE 'compose.*[[:space:]]up([[:space:]]|$)' "${SMOKE}" | head -1 | cut -d: -f1 || true)"
[ -n "${up_line}" ] || fail "could not find a 'compose ... up' invocation in the smoke script"

if [ "${certs_line}" -ge "${up_line}" ]; then
    fail "CERTS_PATH export (line ${certs_line}) must come BEFORE the first 'compose up' (line ${up_line})"
fi

echo "PASS: CERTS_PATH is exported (line ${certs_line}) before the first 'compose up' (line ${up_line})"
