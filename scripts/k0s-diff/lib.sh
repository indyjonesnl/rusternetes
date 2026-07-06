#!/usr/bin/env bash
# scripts/k0s-diff/lib.sh — shared config for the k0s differential harness.
set -euo pipefail
export CONTAINER_RUNTIME=docker   # podman socket not running on this box
VARIANTS=(v0 v1 v2 v3 v4 v5 v6)
# swapped component per variant (index-aligned with VARIANTS)
SWAP=( "" api-server kubelet scheduler controller-manager kube-proxy dns )
# fixed sig order: highest-signal / cheapest-feedback first
SIGS=(sig-node sig-api-machinery sig-apps sig-storage sig-network sig-auth sig-scheduling sig-cli sig-instrumentation)
K8S_VERSION=v1.35.0
require_docker() { docker info >/dev/null 2>&1 || { echo "docker not available"; exit 1; }; }
log() { printf '[k0s-diff] %s\n' "$*" >&2; }
