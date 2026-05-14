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

require_env() {
    if [[ -z "${KUBELET_VOLUMES_PATH:-}" ]]; then
        export KUBELET_VOLUMES_PATH="$REPO_ROOT/.rusternetes/volumes"
        log "KUBELET_VOLUMES_PATH defaulted to $KUBELET_VOLUMES_PATH"
    fi
    mkdir -p "$KUBELET_VOLUMES_PATH"
}

generate_certs_if_missing() {
    if [[ ! -f "$REPO_ROOT/.rusternetes/certs/server.crt" ]]; then
        log "generating TLS certs"
        bash "$REPO_ROOT/scripts/generate-certs.sh"
    fi
}

compose_up() {
    log "bringing up cluster via $COMPOSE_FILE"
    podman compose -f "$COMPOSE_FILE" up -d --wait
}

compose_down() {
    if [[ "${KEEP_CLUSTER:-0}" == "1" ]]; then
        log "KEEP_CLUSTER=1 set, leaving cluster running"
        return 0
    fi
    log "tearing down cluster"
    podman compose -f "$COMPOSE_FILE" down -v || true
}

wait_for_api() {
    log "waiting for api-server on 6443"
    local i
    for i in $(seq 1 60); do
        if curl -sk --max-time 2 "https://localhost:6443/livez" >/dev/null 2>&1; then
            log "api-server ready after ${i}s"
            return 0
        fi
        sleep 1
    done
    fail "api-server did not become ready within 60s"
}

bootstrap_cluster() {
    log "running bootstrap-cluster.sh"
    bash "$REPO_ROOT/scripts/bootstrap-cluster.sh"
}

label_nodes() {
    log "labeling nodes"
    for node in node-1 node-2; do
        curl -sk -X PATCH \
            -H "Content-Type: application/strategic-merge-patch+json" \
            --data "{\"metadata\":{\"labels\":{\"kubernetes.io/os\":\"linux\",\"kubernetes.io/arch\":\"amd64\",\"kubernetes.io/hostname\":\"$node\"}}}" \
            "https://localhost:6443/api/v1/nodes/$node" >/dev/null \
            || fail "failed to label node $node"
    done
}

wait_for_coredns() {
    log "waiting for CoreDNS"
    local i
    for i in $(seq 1 60); do
        local phase
        phase="$(curl -sk "https://localhost:6443/api/v1/namespaces/kube-system/pods?labelSelector=k8s-app=kube-dns" \
            | grep -oE '"phase":"[A-Za-z]+"' | head -1 | cut -d'"' -f4 || true)"
        if [[ "$phase" == "Running" ]]; then
            log "CoreDNS Running"
            return 0
        fi
        sleep 2
    done
    fail "CoreDNS did not reach Running within 120s"
}

prepull_conformance_image() {
    log "pre-pulling $CONFORMANCE_IMAGE on host (kubelets share host runtime via mounted socket)"
    podman pull "$CONFORMANCE_IMAGE" || fail "could not pull $CONFORMANCE_IMAGE"
}

resolve_focus_skip() {
    case "$MODE" in
        ci)
            FOCUS="${FOCUS:-\\[sig-api-machinery\\].*\\[Conformance\\]}"
            SKIP="${SKIP:-\\[Slow\\]|\\[Serial\\]|\\[Disruptive\\]|\\[Flaky\\]}"
            ;;
        full)
            FOCUS="${FOCUS:-\\[Conformance\\]}"
            SKIP="${SKIP:-}"
            ;;
        *)
            fail "unknown MODE=$MODE (expected: ci | full)"
            ;;
    esac
    log "MODE=$MODE  FOCUS=$FOCUS  SKIP=${SKIP:-<none>}"
}

run_hydrophone() {
    mkdir -p "$OUTPUT_DIR"
    log "running hydrophone -> $OUTPUT_DIR"

    local args=(
        --kubeconfig "$KUBECONFIG"
        --focus "$FOCUS"
        --conformance-image "$CONFORMANCE_IMAGE"
        --output-dir "$OUTPUT_DIR"
        --extra-args "--kube-api-content-type=application/json"
    )
    if [[ -n "$SKIP" ]]; then
        args+=(--skip "$SKIP")
    fi

    local rc=0
    "$HYDROPHONE_BIN" "${args[@]}" || rc=$?
    return "$rc"
}

summarize_results() {
    local junit="$OUTPUT_DIR/junit_01.xml"
    if [[ ! -f "$junit" ]]; then
        fail "expected $junit not found"
    fi

    command -v xmllint >/dev/null || fail "xmllint not installed (apt: libxml2-utils)"

    local total failed skipped
    total="$(xmllint --xpath 'string(//testsuite/@tests)' "$junit" 2>/dev/null || echo 0)"
    failed="$(xmllint --xpath 'string(//testsuite/@failures)' "$junit" 2>/dev/null || echo 0)"
    skipped="$(xmllint --xpath 'string(//testsuite/@skipped)' "$junit" 2>/dev/null || echo 0)"
    local passed=$(( total - failed - skipped ))

    log "----- conformance summary -----"
    log "total:   $total"
    log "passed:  $passed"
    log "failed:  $failed"
    log "skipped: $skipped"
    log "artifacts in $OUTPUT_DIR"

    if [[ "$failed" -gt 0 ]]; then
        log "failed testcases:"
        xmllint --xpath '//testcase[failure]/@name' "$junit" 2>/dev/null \
            | tr ' ' '\n' | sed 's/name=//; s/"//g' | grep -v '^$' | head -50 || true
    fi
}

main() {
    require_env
    install_hydrophone

    if [[ "${SKIP_BRINGUP:-0}" != "1" ]]; then
        generate_certs_if_missing
        compose_up
        trap compose_down EXIT
        wait_for_api
        bootstrap_cluster
        wait_for_coredns
        label_nodes
        prepull_conformance_image
    else
        log "SKIP_BRINGUP=1, assuming cluster already running"
    fi

    resolve_focus_skip
    local rc=0
    run_hydrophone || rc=$?
    summarize_results
    return "$rc"
}

main "$@"
