#!/usr/bin/env bash
# Guard the upstream binary names in the service images (#3908).
#
# kubeadm and Talos exec the control-plane binaries by their upstream names
# (`kube-apiserver`, `kube-controller-manager`, `kube-scheduler`, ...) from
# PATH; upstream's server images install them at /usr/local/bin/<name>
# (build/server-image/kube-apiserver/Dockerfile). Our binaries are named
# api-server / controller-manager / scheduler, so each runtime stage must
# symlink the upstream name to ours.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

fail=0
for df in services.Dockerfile local-binary.services.Dockerfile; do
  for pair in api-server:kube-apiserver controller-manager:kube-controller-manager \
              scheduler:kube-scheduler kube-proxy:kube-proxy kubelet:kubelet; do
    ours="${pair%%:*}"
    up="${pair##*:}"
    if ! grep -qE "^RUN ln -s /app/${ours} /usr/local/bin/${up}\$" "$df"; then
      echo "FAIL: $df lacks 'ln -s /app/${ours} /usr/local/bin/${up}'" >&2
      fail=1
    fi
  done
done

[ "$fail" -eq 0 ] && echo "OK: upstream binary-name symlinks present"
exit "$fail"
