#!/usr/bin/env bash
# scripts/k0s-diff/build-swap-images.sh <dns|kube-proxy>
#
# Build the workload-swap container image for a v5/v6 component. Unlike the
# baked v1..v4 binaries (build-swap-binaries.sh), v5 (kube-proxy) and v6 (dns)
# run as IN-CLUSTER PODS, so the Rusternetes binary must live in a *pullable
# image*, not on /var/lib/k0s/bin. This helper:
#
#   1. Stages the pre-built static-musl binary into .build/ (git-ignored). The
#      binary is expected under the shared cargo target dir; it is NOT rebuilt
#      here (Task 6 ships it pre-built). Override with RN_MUSL_DIR.
#   2. Builds a minimal alpine image (Dockerfile.<component>) tagging it with a
#      stable LOCAL name (k0s-diff-rusternetes-<component>:v1.35.5).
#
# run-variant.sh then re-tags that local image to the throwaway local-registry
# ref and `docker push`es it, because containerd-rs can only PULL (no local
# load — see containerd-rs.toml). Keeping the push in run-variant.sh means the
# registry address (k0s-diff-net gateway, only known at bring-up) is resolved
# there, not baked here.
#
# Idempotent; safe to re-run. Binaries are never committed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"                     # CONTAINER_RUNTIME=docker, log()
build_dir="$here/.build"
target="x86_64-unknown-linux-musl"

# Pre-built musl artifacts live in the shared cargo target dir by default (the
# repo's CARGO_TARGET_DIR convention). Override for a non-standard layout.
rn_musl_dir="${RN_MUSL_DIR:-${CARGO_TARGET_DIR:-$HOME/.cache/rusternetes-target}/$target/release}"

component="${1:-}"
case "$component" in
  dns)         bin="rusternetes-dns" ;;
  kube-proxy)  bin="kube-proxy" ;;      # crate [[bin]] name is "kube-proxy"
  *) echo "usage: $(basename "$0") <dns|kube-proxy>" >&2; exit 1 ;;
esac

staged_name="rusternetes-${component}"  # name inside the image / .build/
src="$rn_musl_dir/$bin"
[ -f "$src" ] || {
  echo "pre-built musl binary not found: $src" >&2
  echo "  (build it: cargo build --release --target $target -p rusternetes-${component})" >&2
  exit 1
}

mkdir -p "$build_dir"
install -m0755 "$src" "$build_dir/$staged_name"
log "staged $build_dir/$staged_name ($(stat -c %s "$build_dir/$staged_name") B) from $src"

img="k0s-diff-rusternetes-${component}:${K8S_VERSION#v}"
log "building image $img (Dockerfile.${component})"
docker build -f "$here/Dockerfile.${component}" -t "$img" "$here"

log "done: local image $img ready (run-variant.sh re-tags + pushes to the local registry)"
echo "$img"
