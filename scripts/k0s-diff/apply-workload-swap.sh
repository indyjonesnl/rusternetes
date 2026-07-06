#!/usr/bin/env bash
# scripts/k0s-diff/apply-workload-swap.sh <vN> <kube-proxy|dns> <registry_hostport>
#
# Apply a v5/v6 in-cluster workload swap AFTER the all-Go stack passed smoke, and
# verify it converged. Called by run-variant.sh with KUBECONFIG already exported
# (pointing at the variant's k0s admin kubeconfig).
#
#   dns        -> patch the kube-system coredns Deployment to rusternetes-dns
#                 (coredns-rusternetes-dns.yaml); verify the pod goes Ready and
#                 cluster DNS resolves via the kube-dns Service (10.96.0.10).
#   kube-proxy -> patch the kube-system kube-proxy DaemonSet to
#                 rusternetes-kube-proxy (kube-proxy-rusternetes.yaml); verify
#                 the pod runs and (attempt) service routing.
#
# The committed manifests carry an image placeholder; we sed-substitute it with
# the local-registry ref (<registry_hostport>/rusternetes-<component>:<tag>) that
# run-variant.sh pushed and that containerd-rs is configured to pull over HTTP.
#
# Exit 0 iff the swapped component converged; non-zero (with diagnostics dumped
# to results/<vN>/<component>-diag.txt) otherwise, so run-variant.sh skips
# conformance for a broken swap.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"

v="${1:?usage: apply-workload-swap.sh <vN> <component> <registry_hostport>}"
component="${2:?component}"
reg="${3:?registry_hostport}"
: "${KUBECONFIG:?KUBECONFIG must be exported}"

tag="${K8S_VERSION#v}"
img="${reg}/rusternetes-${component}:${tag}"
diag="$here/results/${v}/${component}-diag.txt"
mkdir -p "$(dirname "$diag")"

dump_diag() {
  {
    echo "=== $(date -u +%FT%TZ) workload-swap diagnostics: $v/$component ==="
    echo "--- image: $img ---"
    kubectl -n kube-system get pods -o wide 2>&1 || true
    echo "--- describe ($component pods) ---"
    kubectl -n kube-system describe pods -l "$1" 2>&1 | tail -60 || true
    echo "--- logs ($component pods, last 80) ---"
    for p in $(kubectl -n kube-system get pods -l "$1" -o name 2>/dev/null); do
      echo ">> $p"
      kubectl -n kube-system logs "$p" --tail=80 2>&1 || true
    done
  } | tee "$diag" >&2
}

case "$component" in
  dns)
    log "applying coredns -> rusternetes-dns swap (image=$img)"
    # Delete + recreate rather than `apply`/patch: the k0s coredns Deployment is
    # server-created (no last-applied annotation) and a strategic merge would
    # UNION our tcpSocket probes with CoreDNS's httpGet probes ("may not specify
    # more than 1 handler type"). A clean recreate installs exactly our spec. The
    # kube-dns Service (10.96.0.10) is a separate object and is left untouched.
    kubectl -n kube-system delete deploy coredns --ignore-not-found --wait=true
    sed "s#__RUSTERNETES_DNS_IMAGE__#${img}#g" "$here/coredns-rusternetes-dns.yaml" \
      | kubectl apply -f -
    if ! kubectl -n kube-system rollout status deploy/coredns --timeout=150s; then
      log "coredns rollout did not complete"; dump_diag "k8s-app=kube-dns"; exit 1
    fi
    # Functional check: resolve an in-cluster name via the kube-dns Service.
    log "verifying cluster DNS resolution via 10.96.0.10"
    kubectl -n default delete pod dnstest --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n default run dnstest --image=busybox:1.36 --restart=Never \
      --command -- sleep 120 >/dev/null
    if ! kubectl -n default wait --for=condition=Ready pod/dnstest --timeout=60s; then
      log "dnstest pod not Ready"; dump_diag "k8s-app=kube-dns"; exit 1
    fi
    ok=0
    for _ in $(seq 1 10); do
      if kubectl -n default exec dnstest -- nslookup kubernetes.default.svc.cluster.local 10.96.0.10 2>&1 \
           | tee -a "$diag" | grep -qE 'Address: *10\.96\.0\.1'; then
        ok=1; break
      fi
      sleep 3
    done
    kubectl -n default delete pod dnstest --ignore-not-found >/dev/null 2>&1 || true
    if [ "$ok" != 1 ]; then
      log "cluster DNS did NOT resolve kubernetes.default via rusternetes-dns"
      dump_diag "k8s-app=kube-dns"; exit 1
    fi
    log "CONVERGED: rusternetes-dns resolves kubernetes.default -> 10.96.0.1"
    ;;

  kube-proxy)
    log "applying kube-proxy -> rusternetes-kube-proxy swap (image=$img)"
    # Delete + recreate (see the dns branch): the k0s kube-proxy DaemonSet is
    # server-created and a strategic merge would union our args/command with
    # k0s's. A clean recreate installs exactly our spec.
    kubectl -n kube-system delete ds kube-proxy --ignore-not-found --wait=true
    sed "s#__RUSTERNETES_KUBE_PROXY_IMAGE__#${img}#g" "$here/kube-proxy-rusternetes.yaml" \
      | kubectl apply -f -
    if ! kubectl -n kube-system rollout status ds/kube-proxy --timeout=150s; then
      log "kube-proxy DaemonSet rollout did not complete"; dump_diag "k8s-app=kube-proxy"; exit 1
    fi
    # Functional check: a fresh ClusterIP Service must be reachable through the
    # iptables rules rusternetes-kube-proxy programs.
    log "verifying service routing via a probe ClusterIP"
    kubectl -n default delete deploy kp-probe --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n default delete svc kp-probe --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n default create deployment kp-probe --image=registry.k8s.io/e2e-test-images/agnhost:2.47 \
      -- /agnhost netexec --http-port=8080 >/dev/null
    kubectl -n default expose deployment kp-probe --port=80 --target-port=8080 >/dev/null
    kubectl -n default rollout status deploy/kp-probe --timeout=90s || true
    svc_ip="$(kubectl -n default get svc kp-probe -o jsonpath='{.spec.clusterIP}')"
    log "probe ClusterIP: $svc_ip"
    kubectl -n default delete pod kp-curl --ignore-not-found >/dev/null 2>&1 || true
    ok=0
    if kubectl -n default run kp-curl --image=registry.k8s.io/e2e-test-images/agnhost:2.47 \
         --restart=Never --command -- sleep 120 >/dev/null \
       && kubectl -n default wait --for=condition=Ready pod/kp-curl --timeout=60s; then
      for _ in $(seq 1 10); do
        if kubectl -n default exec kp-curl -- /agnhost connect --timeout=3s "${svc_ip}:80" 2>&1 \
             | tee -a "$diag"; then ok=1; break; fi
        sleep 3
      done
    fi
    kubectl -n default delete pod kp-curl deploy/kp-probe svc/kp-probe --ignore-not-found >/dev/null 2>&1 || true
    if [ "$ok" != 1 ]; then
      log "service routing via rusternetes-kube-proxy did NOT converge"
      dump_diag "k8s-app=kube-proxy"; exit 1
    fi
    log "CONVERGED: ClusterIP reachable via rusternetes-kube-proxy"
    ;;

  *) echo "unknown workload-swap component '$component'" >&2; exit 1 ;;
esac
