#!/usr/bin/env bash
# scripts/k0s-diff/build-swap-binaries.sh <component>
#
# Produce the two git-ignored build inputs that Dockerfile.k0s-node bakes into a
# swap image (v1..v4), so `run-variant.sh vN <sig>` works from a clean checkout:
#
#   .build/rusternetes-<component>   the Rusternetes replacement, built as a
#                                    STATIC-MUSL binary (the k0s image is Alpine;
#                                    a glibc binary won't run — see task-3-report).
#   .build/<k0s-binary>.real         the GENUINE upstream Go binary k0s would
#                                    stage, extracted from the pinned k0s image.
#                                    Used by the confirmatory probe shim and as
#                                    the authoritative pad-size source for the
#                                    stager (stage-kube-apiserver.sh).
#
# Idempotent: rebuilds/re-extracts in place; safe to re-run. Keeps .build/*
# git-ignored (binaries are never committed).
#
# Usage: build-swap-binaries.sh <api-server|kubelet|scheduler|controller-manager|kube-proxy>
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"                     # CONTAINER_RUNTIME=docker, log()
repo_root="$(cd "$here/../.." && pwd)"
build_dir="$here/.build"
target="x86_64-unknown-linux-musl"

# Pinned, immutable k0s base image — MUST match Dockerfile.k0s-node's FROM so the
# extracted .real is byte-identical to what k0s stages at runtime.
K0S_IMAGE="k0sproject/k0s:v1.35.5-k0s.0"

component="${1:-}"
[ -n "$component" ] || {
  echo "usage: $(basename "$0") <api-server|kubelet|scheduler|controller-manager|kube-proxy>" >&2
  exit 1
}

# component -> rusternetes bin target (== crate suffix) and upstream k0s binary.
# For every workspace crate the [[bin]] name equals the component, and the
# package is rusternetes-<component> (verified in crates/*/Cargo.toml).
case "$component" in
  api-server)         k0s_bin="kube-apiserver" ;;
  kubelet)            k0s_bin="kubelet" ;;
  scheduler)          k0s_bin="kube-scheduler" ;;
  controller-manager) k0s_bin="kube-controller-manager" ;;
  kube-proxy)         k0s_bin="kube-proxy" ;;
  *) echo "unknown component '$component'" >&2; exit 1 ;;
esac
crate="rusternetes-${component}"
bin="${component}"

mkdir -p "$build_dir"

# --- 1. static-musl Rusternetes binary --------------------------------------
# The workspace path-deps require the rhino submodule to resolve, even for the
# default (etcd) build that doesn't use it.
if [ ! -f "$repo_root/rhino/Cargo.toml" ]; then
  log "initializing rhino submodule (needed to resolve the workspace)"
  ( cd "$repo_root" && git submodule update --init rhino )
fi
if ! rustup target list --installed 2>/dev/null | grep -qx "$target"; then
  log "adding rust target $target"
  rustup target add "$target"
fi

log "building $crate (bin '$bin') as static-musl for $target"
( cd "$repo_root" && cargo build --release --target "$target" -p "$crate" )

# CARGO_TARGET_DIR may redirect the output tree (shared cache across worktrees).
tdir="${CARGO_TARGET_DIR:-$repo_root/target}"
artifact="$tdir/$target/release/$bin"
[ -f "$artifact" ] || { echo "build did not produce $artifact" >&2; exit 1; }
install -m0755 "$artifact" "$build_dir/rusternetes-${component}"
log "staged $build_dir/rusternetes-${component} ($(stat -c %s "$build_dir/rusternetes-${component}") B)"

# --- 2. genuine upstream Go binary from the pinned k0s image ----------------
# kube-proxy is NOT a k0s-staged binary — upstream ships it inside the kube-proxy
# DaemonSet pod image (/usr/local/bin/kube-proxy), and v5 is a workload swap, not
# a baked /var/lib/k0s/bin binary. So there is no .real to extract here.
if [ "$component" = "kube-proxy" ]; then
  log "kube-proxy is a workload (DaemonSet) swap — no /var/lib/k0s/bin/.real to extract; skipping"
else
  real_out="$build_dir/${k0s_bin}.real"
  # The stock image does NOT ship /var/lib/k0s/bin (k0s extracts it at runtime
  # from its embedded gzip). Start a throwaway `k0s controller` just long enough
  # for it to stage the binaries, then copy the genuine ELF out and remove the
  # container. Staging completes within a few seconds, well before the control
  # plane is actually up.
  log "extracting genuine $k0s_bin from $K0S_IMAGE (throwaway k0s controller)"
  cid="$(docker run -d --privileged --entrypoint sh "$K0S_IMAGE" \
        -c 'k0s controller --enable-worker >/tmp/stage.log 2>&1 & sleep 300')"
  # shellcheck disable=SC2064
  trap "docker rm -f '$cid' >/dev/null 2>&1 || true" EXIT
  staged=""
  for _ in $(seq 1 60); do
    if docker exec "$cid" sh -c "[ -f /var/lib/k0s/bin/${k0s_bin} ]" 2>/dev/null; then
      staged=1; break
    fi
    sleep 1
  done
  [ -n "$staged" ] || { echo "k0s never staged /var/lib/k0s/bin/${k0s_bin}" >&2; exit 1; }
  docker cp "$cid:/var/lib/k0s/bin/${k0s_bin}" "$real_out"
  chmod 0750 "$real_out"
  docker rm -f "$cid" >/dev/null 2>&1 || true
  trap - EXIT
  log "staged $real_out ($(stat -c %s "$real_out") B)"
fi

log "done: build inputs for '$component' ready under $build_dir"
