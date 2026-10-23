#!/usr/bin/env bash
# Front-proxy PKI + bring-up wiring (#3423). Run with:
#   bash scripts/tests/test-front-proxy-certs.sh
# 1. generate_front_proxy_certs (scripts/generate-certs.sh) emits a front-proxy
#    CA distinct from the cluster CA and a CN=front-proxy-client clientAuth cert
#    chaining to it (kubeadm certlist.go KubeadmCertFrontProxyCA/Client), and is
#    idempotent.
# 2. Every file that starts the api-server with the generated serving cert also
#    passes the matching --requestheader-* flags.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail=0
ok() { echo "PASS: $1"; }
bad() { echo "FAIL: $1"; fail=1; }

export CERT_DIR="$tmp/certs"
eval "$(sed -n '/^generate_front_proxy_certs() {/,/^}/p' "$REPO_ROOT/scripts/generate-certs.sh")"
generate_front_proxy_certs >/dev/null
before="$(sha256sum "$CERT_DIR"/front-proxy-*.crt)"
generate_front_proxy_certs >/dev/null
[ "$before" = "$(sha256sum "$CERT_DIR"/front-proxy-*.crt)" ] && ok "idempotent" || bad "idempotent"

openssl verify -CAfile "$CERT_DIR/front-proxy-ca.crt" "$CERT_DIR/front-proxy-client.crt" >/dev/null 2>&1 \
  && ok "client chains to front-proxy-ca" || bad "client chains to front-proxy-ca"
openssl x509 -in "$CERT_DIR/front-proxy-client.crt" -noout -subject | grep -q 'CN *= *front-proxy-client' \
  && ok "CN=front-proxy-client" || bad "CN=front-proxy-client"
openssl x509 -in "$CERT_DIR/front-proxy-client.crt" -noout -ext extendedKeyUsage | grep -q 'TLS Web Client Authentication' \
  && ok "clientAuth EKU" || bad "clientAuth EKU"
head -1 "$CERT_DIR/front-proxy-client.key" | grep -q 'BEGIN PRIVATE KEY' \
  && ok "PKCS#8 key" || bad "PKCS#8 key"

while IFS= read -r f; do
  if grep -q 'requestheader-client-ca-file' "$REPO_ROOT/$f" \
     && grep -q 'requestheader-allowed-names' "$REPO_ROOT/$f"; then
    ok "$f passes --requestheader-*"
  else
    bad "$f starts the api-server without --requestheader-*"
  fi
done < <(cd "$REPO_ROOT" && grep -l 'pki/api-server\.key' compose*.yml docker-compose*.yml scripts/podman-start-cluster.sh 2>/dev/null)

exit "$fail"
