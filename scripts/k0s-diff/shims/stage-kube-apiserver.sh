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

# k0s v1.35.5-k0s.0 embedded kube-apiserver uncompressed size (bin.originalSize).
# Verified against the v0 stack: `stat -c %s /var/lib/k0s/bin/kube-apiserver`.
# MUST equal what k0s would extract or k0s re-extracts over the shim.
APISERVER_SIZE=85881016

[ -x "$K0S_BIN" ] || { echo "[stage] $K0S_BIN missing" >&2; exit 1; }
[ -f "$SRC" ]     || { echo "[stage] shim $SRC missing (mode=$MODE)" >&2; exit 1; }

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
