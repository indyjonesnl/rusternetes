#!/usr/bin/env bash
# scripts/k0s-diff/lib.sh — shared config for the k0s differential harness.
set -euo pipefail
export CONTAINER_RUNTIME=docker   # podman socket not running on this box
VARIANTS=(v0 v1 v2 v3 v4 v5 v6)
# swapped component per variant (index-aligned with VARIANTS)
SWAP=( "" api-server kubelet scheduler controller-manager kube-proxy dns )
# fixed sig order: highest-signal / cheapest-feedback first
SIGS=(sig-node sig-api-machinery sig-apps sig-storage sig-network sig-auth sig-scheduling sig-cli sig-instrumentation)
K8S_VERSION=v1.35.5
# Fixed k0s-diff-net subnet/gateway (pinned in compose.k0s.template.yml). Pinning
# it makes the bridge gateway deterministic BEFORE `up`, so the v5/v6 workload
# swaps can point CONTAINERD_RS_INSECURE_REGISTRIES at the host-published local
# registry (reached via the gateway) without a container recreate. 10.201.0.0/24
# is clear of the 172.x docker pool in use on this box and of the k0s pod/service
# CIDRs (10.244/16, 10.96/12). Keep in sync with the template's networks block.
K0S_DIFF_NET_GATEWAY=10.201.0.1
require_docker() { docker info >/dev/null 2>&1 || { echo "docker not available"; exit 1; }; }
log() { printf '[k0s-diff] %s\n' "$*" >&2; }
