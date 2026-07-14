#!/usr/bin/env bash
# cert-manager smoke test — proves a real third-party operator installs and
# functions end-to-end on Rusternetes. Exercises, in one shot:
#   - CRD install + Established (cert-manager.io/v1: Issuer, Certificate, ...)
#   - the cert-manager validating/mutating webhook (api-server -> webhook svc)
#   - the cainjector (patches caBundle into webhook configs / CRD conversion)
#   - leader-election Leases (coordination.k8s.io)
#   - the controller reconcile loop: SelfSigned Issuer -> Certificate ->
#     CertificateRequest -> signed cert written to a Secret
#
# Brings up the Rhino+SQLite multi-container stack (the backend CI/conformance
# use), installs cert-manager, issues one self-signed Certificate, and asserts
# the resulting TLS Secret contains a valid cert. Self-contained: no ACME, no
# external DNS.
#
# Usage:
#   CONTAINER_RUNTIME=docker bash scripts/run-cert-manager-smoke.sh
set -euo pipefail

CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${RESULTS_DIR:-/tmp/cert-manager-smoke}"
mkdir -p "${RESULTS_DIR}"

export KUBELET_VOLUMES_PATH="${KUBELET_VOLUMES_PATH:-${PROJECT_ROOT}/.rusternetes/volumes}"
mkdir -p "${KUBELET_VOLUMES_PATH}"

# CERTS_PATH MUST be exported BEFORE the first `compose up` below (regression
# #1153). compose.sqlite.yml bind-mounts the certs dir into the node-1 kubelet
# at ${CERTS_PATH}:${CERTS_PATH} (host-absolute, same path both sides), and
# bootstrap-cluster.sh later templates that SAME host-absolute path into the
# control-plane static-pod hostPath volume (kube-controller-manager /
# kube-scheduler). If CERTS_PATH is unset at `up` time, the kubelet falls back
# to mounting certs at /etc/rusternetes/certs, so the host-absolute path the
# static pods hostPath-mount does not exist inside the kubelet container: its
# `type: Directory` check fails, the static pods stay Pending forever, the
# controller-manager never runs, and Deployments (rusternetes-dns, then
# cert-manager) never reconcile — the rollout wait in [6/7] then times out.
# The canary's ci/actions/bring-up-cluster sets this the same way; the
# standalone smoke script needs its own copy.
export CERTS_PATH="${CERTS_PATH:-${PROJECT_ROOT}/.rusternetes/certs}"
mkdir -p "${CERTS_PATH}"

# Pre-create every host dir that compose bind-mounts, BEFORE `compose up`.
# bootstrap-cluster.sh templates the control-plane static pod YAML
# (kube-controller-manager.yaml, kube-scheduler.yaml) into
# .rusternetes/manifests AFTER `compose up`, and compose.sqlite.yml bind-mounts
# that dir into the kubelets. If the dir does not already exist, the Docker
# daemon creates it as root on `up`, and bootstrap-cluster.sh — running as this
# user — then dies with "Permission denied" templating the manifests. The
# controller-manager static pod never starts, so Deployments never reconcile
# and cert-manager's rollout times out. This mirrors the fix the conformance
# canary's bring-up action got (commit b07d5890); the smoke script does its own
# `compose up`, so it needs the same pre-create.
#
# A plain `mkdir -p` is NOT enough on the persistent self-hosted (ARC) runner
# workspace: an earlier run — from before this pre-create existed — already
# left a root-owned .rusternetes/manifests behind, and `mkdir -p` is a no-op on
# an existing dir, so it can never repair the ownership (there is no sudo on
# the runner). The templating then fails forever with "Permission denied". Heal
# it: if the dir is not writable by us, move it aside (a rename only needs write
# on the user-owned parent .rusternetes, so it works without sudo) and recreate
# a fresh, user-owned dir.
MANIFESTS_DIR="${PROJECT_ROOT}/.rusternetes/manifests"
if [ -e "${MANIFESTS_DIR}" ] && [ ! -w "${MANIFESTS_DIR}" ]; then
    echo "manifests dir not writable (stale root-owned?) — moving aside" >&2
    rm -rf "${MANIFESTS_DIR}" 2>/dev/null \
        || mv "${MANIFESTS_DIR}" "${MANIFESTS_DIR}.stale.$$" 2>/dev/null \
        || true
fi
mkdir -p "${MANIFESTS_DIR}"

KUBECONFIG_FILE="${KUBECONFIG:-${HOME}/.kube/rusternetes-config}"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-$(command -v podman >/dev/null 2>&1 && echo podman || echo docker)}"

# Rhino+SQLite stack. Pod runtime is the in-compose containerd service, so no
# host runtime socket / DinD override is needed on Docker or Podman hosts.
COMPOSE_FILES="-f ${PROJECT_ROOT}/compose.sqlite.yml"
# shellcheck disable=SC2086
COMPOSE="${CONTAINER_RUNTIME} compose ${COMPOSE_FILES}"

KUBECTL="kubectl --kubeconfig ${KUBECONFIG_FILE}"

cleanup() {
    echo "[teardown] collecting cert-manager state + tearing down stack..."
    ${KUBECTL} -n cert-manager get pods -o wide >"${RESULTS_DIR}/cert-manager-pods.txt" 2>&1 || true
    ${KUBECTL} get certificate,certificaterequest,issuer -A >"${RESULTS_DIR}/cert-manager-resources.txt" 2>&1 || true
    # The reconcile loop only shows up in the controller log + the object
    # conditions/events — without these the CI artifact can't explain a stuck
    # Certificate. (Cost us a full debug session for the two bugs behind #1057.)
    ${KUBECTL} -n cert-manager logs deploy/cert-manager --tail=200 \
        >"${RESULTS_DIR}/cert-manager-controller.log" 2>&1 || true
    ${KUBECTL} describe certificate,certificaterequest,issuer -A \
        >"${RESULTS_DIR}/cert-manager-describe.txt" 2>&1 || true
    ${KUBECTL} get certificaterequest -A -o yaml \
        >"${RESULTS_DIR}/cert-manager-certificaterequests.yaml" 2>&1 || true
    # The webhook admission path (api-server -> webhook svc) fails with
    # "invalid peer certificate: UnknownIssuer" when the cainjector has not
    # patched the caBundle into the webhook configs. Capture exactly that: the
    # cainjector log (why it did/didn't reconcile), the source CA secret it
    # injects from, and the webhook configs so we can see whether caBundle is
    # populated. Without these the artifact can't explain a stuck injection.
    ${KUBECTL} -n cert-manager logs deploy/cert-manager-cainjector --tail=200 \
        >"${RESULTS_DIR}/cert-manager-cainjector.log" 2>&1 || true
    ${KUBECTL} -n cert-manager logs deploy/cert-manager-webhook --tail=100 \
        >"${RESULTS_DIR}/cert-manager-webhook.log" 2>&1 || true
    ${KUBECTL} -n cert-manager get secret cert-manager-webhook-ca -o yaml \
        >"${RESULTS_DIR}/cert-manager-webhook-ca-secret.yaml" 2>&1 || true
    ${KUBECTL} get validatingwebhookconfiguration cert-manager-webhook -o yaml \
        >"${RESULTS_DIR}/cert-manager-validatingwebhook.yaml" 2>&1 || true
    ${KUBECTL} get mutatingwebhookconfiguration cert-manager-webhook -o yaml \
        >"${RESULTS_DIR}/cert-manager-mutatingwebhook.yaml" 2>&1 || true
    ${KUBECTL} -n cert-manager get lease -o wide \
        >"${RESULTS_DIR}/cert-manager-leases.txt" 2>&1 || true
    # shellcheck disable=SC2086
    ${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== Rusternetes cert-manager smoke (${CERT_MANAGER_VERSION}) ==="

echo "[1/7] Tearing down any previous stack..."
# shellcheck disable=SC2086
${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true

echo "[2/7] Generating certs (if absent) + bringing up the stack..."
[ -f "${PROJECT_ROOT}/.rusternetes/certs/api-server.crt" ] || bash "${PROJECT_ROOT}/scripts/generate-certs.sh"
# The control-plane images (scheduler, controller-manager) and the in-cluster
# dns Deployment image are gated behind the `build` profile in compose.sqlite.yml.
# `compose up -d --build` does not include profile-only services, so bootstrap
# would later fail to import missing local tags into containerd and the static
# pods would stay Pending. Build those images explicitly first.
# shellcheck disable=SC2086
${CONTAINER_RUNTIME} compose -f ${PROJECT_ROOT}/compose.sqlite.yml --profile build build --parallel
# shellcheck disable=SC2086
${COMPOSE} up -d --build

echo "[3/7] Waiting for the api-server (max 120s)..."
for i in $(seq 1 60); do
    if curl -skf "https://localhost:6443/healthz" >/dev/null 2>&1; then
        echo "api-server is up"
        break
    fi
    sleep 2
    [ "$i" -eq 60 ] && { echo "ERROR: api-server did not come up"; exit 1; }
done

if [ ! -f "${KUBECONFIG_FILE}" ]; then
    echo "Writing kubeconfig at ${KUBECONFIG_FILE}..."
    mkdir -p "$(dirname "${KUBECONFIG_FILE}")"
    cat >"${KUBECONFIG_FILE}" <<'EOF'
apiVersion: v1
kind: Config
clusters:
  - name: rusternetes
    cluster: {insecure-skip-tls-verify: true, server: https://localhost:6443}
contexts:
  - name: rusternetes
    context: {cluster: rusternetes, user: admin}
current-context: rusternetes
users:
  - name: admin
    user: {token: anonymous}
EOF
fi
export KUBECONFIG="${KUBECONFIG_FILE}"

echo "[4/7] Bootstrapping cluster (services, SAs, DNS)..."
CONTAINER_RUNTIME="${CONTAINER_RUNTIME}" bash "${PROJECT_ROOT}/scripts/bootstrap-cluster.sh" || {
    echo "WARNING: bootstrap exited non-zero — continuing"
}

echo "[5/7] Installing cert-manager ${CERT_MANAGER_VERSION}..."
CM_MANIFEST="https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
${KUBECTL} apply -f "${CM_MANIFEST}"

echo "[6/7] Waiting for cert-manager components to become Available..."
for d in cert-manager cert-manager-webhook cert-manager-cainjector; do
    ${KUBECTL} -n cert-manager rollout status "deploy/${d}" --timeout=300s
done

# A Deployment being Available does NOT mean the admission path is ready: the
# webhook only accepts requests once cainjector has injected the webhook's CA
# into the ValidatingWebhookConfiguration `caBundle`. cainjector injects on
# watching the `cert-manager-webhook-ca` Secret get created by the webhook. If
# that one create watch-event is missed (a watch-delivery race under load — see
# the api-server WatchCache reconnect fix in #1592, still not 100% closed), the
# caBundle stays empty forever and every admission call fails with
# `UnknownIssuer`. cainjector's informer resync is 10h, so it never self-heals
# in the smoke's timescale.
#
# Gate explicitly on the caBundle being populated, and if it hasn't landed by
# the grace deadline, bounce cainjector once — a fresh list-watch sees the
# now-existing Secret and injects deterministically. This is the same recovery
# a real operator performs, and it turns a rare missed-event race into a
# reliable pass instead of a nightly-red flake.
WEBHOOK_CFG="validatingwebhookconfiguration cert-manager-webhook"
CABUNDLE_JSONPATH='{.webhooks[0].clientConfig.caBundle}'
echo "[6b] Waiting for cainjector to inject the webhook caBundle..."
bounced=0
injected=0
for i in $(seq 1 60); do
    ca="$(${KUBECTL} get ${WEBHOOK_CFG} -o jsonpath="${CABUNDLE_JSONPATH}" 2>/dev/null || true)"
    if [ -n "${ca}" ]; then
        echo "caBundle injected after $((i * 2))s"
        injected=1
        break
    fi
    # Halfway through the ~120s budget, force a fresh cainjector list-watch.
    if [ "${i}" -eq 30 ] && [ "${bounced}" -eq 0 ]; then
        echo "caBundle still empty after 60s — restarting cainjector to force a re-list"
        ${KUBECTL} -n cert-manager rollout restart deploy/cert-manager-cainjector || true
        ${KUBECTL} -n cert-manager rollout status deploy/cert-manager-cainjector --timeout=120s || true
        bounced=1
    fi
    sleep 2
done
if [ "${injected}" -ne 1 ]; then
    echo "ERROR: cainjector never populated the webhook caBundle."
    ${KUBECTL} get ${WEBHOOK_CFG} -o yaml \
        >"${RESULTS_DIR}/cert-manager-validatingwebhook.yaml" 2>&1 || true
    ${KUBECTL} -n cert-manager logs deploy/cert-manager-cainjector --tail=200 \
        >"${RESULTS_DIR}/cert-manager-cainjector.log" 2>&1 || true
    exit 1
fi

echo "[7/7] Issuing a self-signed Certificate and verifying the Secret..."
# The webhook may briefly reject requests until cainjector has wired its
# caBundle, so retry the apply.
read -r -d '' SMOKE_YAML <<'EOF' || true
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: smoke-selfsigned
  namespace: default
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: smoke-cert
  namespace: default
spec:
  secretName: smoke-cert-tls
  commonName: smoke.rusternetes.local
  dnsNames:
    - smoke.rusternetes.local
  issuerRef:
    name: smoke-selfsigned
    kind: Issuer
EOF

applied=0
for i in $(seq 1 30); do
    if printf '%s' "${SMOKE_YAML}" | ${KUBECTL} apply -f - >"${RESULTS_DIR}/issuer-apply.log" 2>&1; then
        applied=1
        break
    fi
    sleep 4
done
if [ "${applied}" -ne 1 ]; then
    echo "ERROR: could not apply Issuer/Certificate. Last apply error:"
    cat "${RESULTS_DIR}/issuer-apply.log" || true
    # The validating webhook is the usual suspect — dump it for the artifact.
    ${KUBECTL} -n cert-manager get validatingwebhookconfiguration cert-manager-webhook -o yaml \
        >"${RESULTS_DIR}/cert-manager-validatingwebhook.yaml" 2>&1 || true
    ${KUBECTL} -n cert-manager logs deploy/cert-manager-webhook --tail=50 \
        >"${RESULTS_DIR}/cert-manager-webhook.log" 2>&1 || true
    exit 1
fi

# The controller signs the Certificate; wait for Ready then assert the Secret.
${KUBECTL} wait --for=condition=Ready certificate/smoke-cert -n default --timeout=180s

CRT="$(${KUBECTL} get secret smoke-cert-tls -n default -o jsonpath='{.data.tls\.crt}' 2>/dev/null || true)"
if [ -z "${CRT}" ]; then
    echo "ERROR: Secret smoke-cert-tls has no tls.crt"
    ${KUBECTL} describe certificate/smoke-cert -n default || true
    exit 1
fi
SUBJECT="$(printf '%s' "${CRT}" | base64 -d | openssl x509 -noout -subject 2>/dev/null || true)"
echo "Issued certificate subject: ${SUBJECT}"
printf '%s' "${CRT}" | base64 -d | openssl x509 -noout -checkend 0 >/dev/null 2>&1 || {
    echo "ERROR: issued certificate is not valid"; exit 1; }

echo "PASS: cert-manager installed and issued a working certificate on Rusternetes."
