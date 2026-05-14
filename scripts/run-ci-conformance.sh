#!/usr/bin/env bash
# scripts/run-ci-conformance.sh
#
# CI-friendly conformance runner using Hydrophone (sigs.k8s.io/hydrophone).
# Drop-in replacement for scripts/run-conformance.sh for GitHub Actions.
#
# Env vars (all optional):
#   MODE                  ci|full   default: ci
#   FOCUS                 ginkgo focus regex override
#   SKIP                  ginkgo skip regex override
#   HYDROPHONE_VERSION    default: v0.7.0
#   CONFORMANCE_IMAGE     default: registry.k8s.io/conformance:v1.35.0
#   COMPOSE_FILE          default: compose.sqlite.yml
#   OUTPUT_DIR            default: $(pwd)/conformance-results
#   KEEP_CLUSTER          1 to skip teardown (default: tear down on exit)
#   SKIP_BRINGUP          1 to assume cluster is already running

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

HYDROPHONE_VERSION="${HYDROPHONE_VERSION:-v0.7.0}"
CONFORMANCE_IMAGE="${CONFORMANCE_IMAGE:-registry.k8s.io/conformance:v1.35.0}"
COMPOSE_FILE="${COMPOSE_FILE:-compose.sqlite.yml}"
OUTPUT_DIR="${OUTPUT_DIR:-$REPO_ROOT/conformance-results}"
MODE="${MODE:-ci}"
KUBECONFIG="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"
export KUBECONFIG

BIN_DIR="$REPO_ROOT/.bin"
HYDROPHONE_BIN="$BIN_DIR/hydrophone"

log() { printf '\033[1;34m[ci-conformance]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m[ci-conformance ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

install_hydrophone() {
    # hydrophone's `version` subcommand requires cluster access, so we
    # smoke-test the binary with --help (cluster-free) and gate re-install
    # on a marker file recording the installed version.
    local marker="$BIN_DIR/.hydrophone.version"
    if [[ -x "$HYDROPHONE_BIN" && -f "$marker" ]] \
        && [[ "$(cat "$marker")" == "$HYDROPHONE_VERSION" ]]; then
        log "hydrophone $HYDROPHONE_VERSION already installed at $HYDROPHONE_BIN"
        return 0
    fi
    log "downloading hydrophone $HYDROPHONE_VERSION"
    mkdir -p "$BIN_DIR"
    local tmp; tmp="$(mktemp -d)"
    local url="https://github.com/kubernetes-sigs/hydrophone/releases/download/${HYDROPHONE_VERSION}/hydrophone_Linux_x86_64.tar.gz"
    curl -fsSL "$url" -o "$tmp/h.tgz"
    tar -xzf "$tmp/h.tgz" -C "$tmp"
    install -m 0755 "$tmp/hydrophone" "$HYDROPHONE_BIN"
    rm -rf "$tmp"
    "$HYDROPHONE_BIN" --help >/dev/null || fail "hydrophone binary failed to run"
    printf '%s' "$HYDROPHONE_VERSION" > "$marker"
}

install_hydrophone
log "ok"
