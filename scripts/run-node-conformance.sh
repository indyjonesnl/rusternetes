#!/usr/bin/env bash
# Kubelet-scoped conformance runner.
# Boots compose.node-conformance.yml, fetches the upstream e2e_node.test
# binary, runs ginkgo focused on [NodeConformance], dumps results.
#
# See docs/superpowers/specs/2026-05-17-node-conformance-design.md.
set -euo pipefail

K8S_VERSION="${K8S_VERSION:-v1.35.0}"
ARCH="${ARCH:-linux-amd64}"
TEST_TARBALL_URL="https://dl.k8s.io/${K8S_VERSION}/kubernetes-test-${ARCH}.tar.gz"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${PROJECT_ROOT}/.bin"
RESULTS_DIR="/tmp/node-conformance"
FOCUS="${FOCUS:-\\[NodeConformance\\]}"
SKIP="${SKIP:-\\[Flaky\\]|\\[Serial\\]}"

export KUBELET_VOLUMES_PATH="${KUBELET_VOLUMES_PATH:-${PROJECT_ROOT}/.rusternetes/volumes}"
mkdir -p "${KUBELET_VOLUMES_PATH}" "${BIN_DIR}" "${RESULTS_DIR}"

KUBECONFIG_FILE="${HOME}/.kube/rusternetes-config"
if [ ! -f "${KUBECONFIG_FILE}" ]; then
    echo "ERROR: kubeconfig not found at ${KUBECONFIG_FILE}"
    echo "Run: bash scripts/generate-certs.sh && bash scripts/bootstrap-cluster.sh"
    exit 1
fi

CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-$(command -v podman >/dev/null 2>&1 && echo podman || echo docker)}"
# shellcheck disable=SC2086
# SC2086: intentional word-splitting — COMPOSE holds runtime + subcommand + flag triple
COMPOSE="${CONTAINER_RUNTIME} compose -f ${PROJECT_ROOT}/compose.node-conformance.yml"

echo "=== Rusternetes Node Conformance ==="
echo "K8S_VERSION=${K8S_VERSION} FOCUS=${FOCUS}"

echo "[1/6] Tearing down any previous node-conformance stack..."
# shellcheck disable=SC2086
${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true

echo "[2/6] Bringing up single-node stack..."
# shellcheck disable=SC2086
${COMPOSE} up -d --build

echo "[3/6] Waiting for kubelet to come up (max 60s)..."
for i in $(seq 1 60); do
    if curl -sfk "http://localhost:10250/healthz" >/dev/null 2>&1 \
        || curl -sfk "http://localhost:10249/metrics" >/dev/null 2>&1; then
        echo "kubelet is up"
        break
    fi
    sleep 1
    if [ "$i" -eq 60 ]; then
        echo "ERROR: kubelet did not come up within 60s"
        # shellcheck disable=SC2086
        ${COMPOSE} logs kubelet || true
        exit 1
    fi
done

echo "[4/6] Fetching e2e_node.test (${K8S_VERSION} ${ARCH})..."
if [ ! -f "${BIN_DIR}/e2e_node.test" ] || [ ! -f "${BIN_DIR}/ginkgo" ]; then
    TMP_TARBALL="$(mktemp)"
    curl -fL "${TEST_TARBALL_URL}" -o "${TMP_TARBALL}"
    tar -xzf "${TMP_TARBALL}" -C "${BIN_DIR}" \
        --strip-components=3 \
        kubernetes/test/bin/e2e_node.test \
        kubernetes/test/bin/ginkgo
    rm -f "${TMP_TARBALL}"
    chmod +x "${BIN_DIR}/e2e_node.test" "${BIN_DIR}/ginkgo"
fi

echo "[5/6] Running ginkgo focus=${FOCUS}..."
KUBECONFIG="${KUBECONFIG_FILE}" \
"${BIN_DIR}/ginkgo" \
    --focus="${FOCUS}" \
    --skip="${SKIP}" \
    --no-color \
    "${BIN_DIR}/e2e_node.test" \
    -- \
    --node-name=node-1 \
    --kubelet-host=localhost \
    --kubelet-port=10250 \
    --kubeconfig="${KUBECONFIG_FILE}" \
    2>&1 | tee "${RESULTS_DIR}/ginkgo.log" || true

echo "[6/6] Parsing results..."
PASS=$(grep -cE '^\s*\[PASSED\]' "${RESULTS_DIR}/ginkgo.log" || true)
FAIL=$(grep -cE '^\s*\[FAILED\]' "${RESULTS_DIR}/ginkgo.log" || true)
SKIP_CT=$(grep -cE '^\s*\[SKIPPED\]' "${RESULTS_DIR}/ginkgo.log" || true)

echo "PASS=${PASS} FAIL=${FAIL} SKIP=${SKIP_CT}"
echo "Full log: ${RESULTS_DIR}/ginkgo.log"
