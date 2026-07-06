#!/bin/sh
# scripts/k0s-diff/shims/stage-kube-apiserver.sh
#
# Interpose a Rusternetes shim for k0s's staged kube-apiserver WITHOUT
# bind-mounting over the running tree (ruled out, SPIKE-FINDINGS Crux 1) and
# WITHOUT rebuilding the k0s binary.
#
# HOW: k0s (pkg/assets/stage.go, v1.35.5) re-extracts /var/lib/k0s/bin/<name>
# from its embedded gzip on every controller start AND on every supervisor
# component restart, UNLESS the on-disk file is "up to date", defined as:
#
#     on-disk mtime == the k0s executable's mtime          (both whole-second)
#   AND on-disk size == the embedded original (uncompressed) size
#
# The SPIKE's Exp D matched size only (not mtime) and so still got re-extracted;
# it therefore concluded "content-based / no size loophole". The real condition
# is mtime+size. This stager satisfies BOTH: it writes the shim, extends it to
# the exact original size with NUL padding (sh stops reading at the shim's
# `exit`/`exec`, so the padding is never parsed), and copies the k0s exe's
# mtime. k0s then treats the shim as up-to-date and execs it verbatim.
#
# Runs from the container entrypoint BEFORE `k0s controller`, so the shim is in
# place before k0s's first staging pass.
set -eu

K0S_BIN=/usr/local/bin/k0s
TARGET=/var/lib/k0s/bin/kube-apiserver
MODE="${K0S_DIFF_SHIM_MODE:-adapter}"          # adapter | probe
SRC="/opt/k0s-diff/kube-apiserver.${MODE}"
# The genuine upstream kube-apiserver, extracted from the SAME pinned k0s image
# at build time (build-swap-binaries.sh). Its size IS bin.originalSize — exactly
# what k0s would stage — so it is the authoritative pad-target. Deriving it here
# (instead of a magic constant) means a base-image bump can't silently make k0s
# re-extract the real Go binary over our shim: if the size drifts, the padded
# shim would fail the skip check, so we assert loudly instead.
REAL=/opt/k0s-diff/kube-apiserver.real

[ -x "$K0S_BIN" ] || { echo "[stage] $K0S_BIN missing" >&2; exit 1; }
[ -f "$SRC" ]     || { echo "[stage] shim $SRC missing (mode=$MODE)" >&2; exit 1; }
[ -f "$REAL" ]    || { echo "[stage] genuine $REAL missing — cannot derive pad size (run build-swap-binaries.sh api-server)" >&2; exit 1; }

APISERVER_SIZE="$(stat -c %s "$REAL")"
case "$APISERVER_SIZE" in
  ''|*[!0-9]*) echo "[stage] could not read size of $REAL" >&2; exit 1 ;;
esac
# Cross-check against the value verified live on v0 (2026-07-06). A mismatch is
# not fatal (the .real is authoritative) but flags that the base image changed.
if [ "$APISERVER_SIZE" != "85881016" ]; then
  echo "[stage] NOTE: genuine kube-apiserver size $APISERVER_SIZE != last-verified 85881016 (base image bumped?)" >&2
fi

mkdir -p "$(dirname "$TARGET")"
cp "$SRC" "$TARGET"
cur=$(stat -c %s "$TARGET")
if [ "$cur" -gt "$APISERVER_SIZE" ]; then
  echo "[stage] shim ($cur B) exceeds target size ${APISERVER_SIZE} B — cannot pad down" >&2
  exit 1
fi
# truncate extends with NUL bytes (sparse, instant) to the exact original size.
truncate -s "$APISERVER_SIZE" "$TARGET"
chmod 0750 "$TARGET"
touch -r "$K0S_BIN" "$TARGET"                  # mtime := k0s exe mtime -> skip

echo "[stage] placed '$MODE' kube-apiserver shim at $TARGET" \
     "(size=$(stat -c %s "$TARGET") mtime=$(stat -c %Y "$TARGET")" \
     "k0s_mtime=$(stat -c %Y "$K0S_BIN"))" >&2
