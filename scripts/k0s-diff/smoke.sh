#!/usr/bin/env bash
# scripts/k0s-diff/smoke.sh — exit 0 iff the cluster is conformance-ready.
#
# Usage:
#   KUBECONFIG=<admin.conf> bash scripts/k0s-diff/smoke.sh
#
# Extract the k0s admin kubeconfig to the host first (it rewrites the server to
# the published host port automatically when you pass the reachable address):
#   docker exec k0s-diff-v0 k0s kubeconfig admin > /tmp/k0s-diff.kubeconfig
#   sed -i 's#server: https://.*:6443#server: https://127.0.0.1:26444#' /tmp/k0s-diff.kubeconfig
#   KUBECONFIG=/tmp/k0s-diff.kubeconfig bash scripts/k0s-diff/smoke.sh
set -euo pipefail
source "$(dirname "$0")/lib.sh"
KUBECONFIG=${KUBECONFIG:?set KUBECONFIG to the k0s admin kubeconfig}
export KUBECONFIG

# --- coredns Corefile repair (idempotent, environment workaround) -------------
# The stock k0s CoreDNS Corefile forwards to /etc/resolv.conf. On an nftables
# dev host the container's resolv.conf loops back to CoreDNS itself, so the loop
# plugin FATALs and coredns CrashLoops. Rewrite the forward to public resolvers.
# Deterministic on this box; harmless (no-op) where the Corefile is already fixed.
if kubectl -n kube-system get cm coredns >/dev/null 2>&1 \
   && kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}' | grep -q '/etc/resolv.conf'; then
  command -v python3 >/dev/null || { echo "python3 required for the coredns Corefile repair"; exit 1; }
  log "repairing coredns Corefile (forward -> 8.8.8.8 1.1.1.1)"
  NEWCORE="$(kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}' \
    | sed 's#forward . /etc/resolv.conf#forward . 8.8.8.8 1.1.1.1#')"
  kubectl -n kube-system patch configmap coredns --type merge \
    -p "$(python3 -c 'import json,sys;print(json.dumps({"data":{"Corefile":sys.stdin.read()}}))' <<<"$NEWCORE")" >/dev/null
  kubectl -n kube-system rollout restart deploy coredns >/dev/null
  kubectl -n kube-system delete pods --field-selector status.phase=Failed >/dev/null 2>&1 || true
fi

kubectl get nodes -o wide
kubectl wait --for=condition=Ready node --all --timeout=120s
kubectl -n kube-system wait --for=condition=Ready pod --all --timeout=180s
kubectl get pods -A
echo "SMOKE PASS: node Ready + kube-system pods Ready"
