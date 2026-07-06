#!/bin/sh
# scripts/k0s-diff/shims/stage-swap.sh
#
# Generic pre-k0s-start interposer for the baked component swaps (v1..v4).
# Supersedes the api-server-specific stage-kube-apiserver.sh: it is driven by the
# component marker /opt/k0s-diff/swap-component (written by Dockerfile.k0s-node
# from the SWAP_COMPONENT build arg), so ONE stager serves every baked variant.
#
# HOW (unchanged from stage-kube-apiserver.sh — see SPIKE-FINDINGS.md, Crux 1
# addendum): k0s (pkg/assets/stage.go, v1.35.5) re-extracts /var/lib/k0s/bin/<name>
# from its embedded gzip on every controller start AND on every supervisor
# component restart, UNLESS the on-disk file is "up to date":
#
#     on-disk mtime == the k0s executable's mtime          (both whole-second)
#   AND on-disk size == the embedded original (uncompressed) size
#
# We write the shim, extend it to the exact original size with NUL padding (sh
# stops reading at the shim's `exec`, so padding is never parsed), and copy the
# k0s exe's mtime. k0s then treats the shim as up-to-date and execs it verbatim,
# surviving every supervisor restart — no bind-mount, no k0s rebuild.
#
# The authoritative pad-size is the GENUINE upstream Go binary
# (/opt/k0s-diff/<k0s-bin>.real, extracted from the SAME pinned k0s image by
# build-swap-binaries.sh): its size IS bin.originalSize, so a base-image bump
# can't silently make k0s re-extract the real binary over our shim.
#
# Runs from the container entrypoint BEFORE `k0s controller`. No-op on v0
# (marker absent).
set -eu

MARKER=/opt/k0s-diff/swap-component
[ -f "$MARKER" ] || exit 0
COMPONENT="$(cat "$MARKER" 2>/dev/null || true)"
[ -n "$COMPONENT" ] || exit 0

# component -> the k0s-staged binary name under /var/lib/k0s/bin.
case "$COMPONENT" in
  api-server)         K0S_BIN_NAME=kube-apiserver ;;
  kubelet)            K0S_BIN_NAME=kubelet ;;
  scheduler)          K0S_BIN_NAME=kube-scheduler ;;
  controller-manager) K0S_BIN_NAME=kube-controller-manager ;;
  *) echo "[stage] unknown swap component '$COMPONENT'" >&2; exit 1 ;;
esac

K0S_BIN=/usr/local/bin/k0s
TARGET="/var/lib/k0s/bin/${K0S_BIN_NAME}"
MODE="${K0S_DIFF_SHIM_MODE:-adapter}"          # adapter | probe (probe = api-server only)
SRC="/opt/k0s-diff/${K0S_BIN_NAME}.${MODE}"
# Only the api-server ships a probe shim; every component ships an adapter.
[ -f "$SRC" ] || SRC="/opt/k0s-diff/${K0S_BIN_NAME}.adapter"
REAL="/opt/k0s-diff/${K0S_BIN_NAME}.real"

[ -x "$K0S_BIN" ] || { echo "[stage] $K0S_BIN missing" >&2; exit 1; }
[ -f "$SRC" ]     || { echo "[stage] shim $SRC missing (component=$COMPONENT mode=$MODE)" >&2; exit 1; }
[ -f "$REAL" ]    || { echo "[stage] genuine $REAL missing — cannot derive pad size (run build-swap-binaries.sh $COMPONENT)" >&2; exit 1; }

REAL_SIZE="$(stat -c %s "$REAL")"
case "$REAL_SIZE" in
  ''|*[!0-9]*) echo "[stage] could not read size of $REAL" >&2; exit 1 ;;
esac

mkdir -p "$(dirname "$TARGET")"
cp "$SRC" "$TARGET"
cur=$(stat -c %s "$TARGET")
if [ "$cur" -gt "$REAL_SIZE" ]; then
  echo "[stage] shim ($cur B) exceeds target size ${REAL_SIZE} B — cannot pad down" >&2
  exit 1
fi
# truncate extends with NUL bytes (sparse, instant) to the exact original size.
truncate -s "$REAL_SIZE" "$TARGET"
chmod 0750 "$TARGET"
touch -r "$K0S_BIN" "$TARGET"                  # mtime := k0s exe mtime -> skip

echo "[stage] placed '$MODE' $K0S_BIN_NAME shim (component=$COMPONENT) at $TARGET" \
     "(size=$(stat -c %s "$TARGET") mtime=$(stat -c %Y "$TARGET")" \
     "k0s_mtime=$(stat -c %Y "$K0S_BIN"))" >&2
