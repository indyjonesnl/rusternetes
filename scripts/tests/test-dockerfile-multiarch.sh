#!/usr/bin/env bash
# Guard the multi-arch image build (.github/workflows/publish-images.yml).
#
# The publish workflow builds every stack image for BOTH linux/amd64 and
# linux/arm64 on native runners. That only works if no Dockerfile downloads a
# hardcoded-architecture release asset — an x86_64 / amd64 tarball fetched on an
# arm64 runner installs a binary that exec-format-errors at runtime.
#
# This test fails if any tracked Dockerfile fetches a release asset with a
# hardcoded arch instead of the BuildKit-injected ${TARGETARCH} (or the
# arch-derived ${SCCACHE_ARCH} / a `dpkg --print-architecture` / `uname -m`
# result). Downloads must be arch-parameterised so both arches stay buildable.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

fail=0

# Only inspect lines that actually fetch a release asset (curl/wget). A literal
# arch token elsewhere (a comment, a FROM, a --platform) is fine.
while IFS= read -r df; do
  # Flag curl/wget download lines that still carry a hardcoded arch token.
  hits=$(grep -nE '(curl|wget)[^|]*(x86_64|linux-amd64|aarch64-unknown|linux-arm64)' "$df" || true)
  if [ -n "$hits" ]; then
    echo "FAIL: $df downloads a hardcoded-arch asset (use \${TARGETARCH}):" >&2
    echo "$hits" >&2
    fail=1
  fi
done < <(git ls-files '*Dockerfile' '*.Dockerfile')

# Positive assertions: the two Dockerfiles with release downloads must consume
# an arch variable, so a future edit that drops it is caught even if it also
# happens to avoid the literal tokens above.
grep -q 'ARG TARGETARCH' containerd.Dockerfile \
  || { echo "FAIL: containerd.Dockerfile must declare ARG TARGETARCH" >&2; fail=1; }
grep -q 'ARG TARGETARCH' services.Dockerfile \
  || { echo "FAIL: services.Dockerfile must declare ARG TARGETARCH" >&2; fail=1; }

# ── The static-musl all-in-one image (#1041) must stay arch-neutral. ────────
# It builds on Alpine, whose host target IS the build target, so a native
# runner per arch needs no cross-compile plumbing — but only as long as nothing
# pins x86_64. A `--platform=linux/amd64` on FROM, or an explicit
# `--target x86_64-unknown-linux-musl` on cargo, silently makes the arm64 job
# produce an amd64 binary (or fail to link), which is exactly the aarch64 gap
# this image was supposed to close.
MUSL_DF="all-in-one-musl.Dockerfile"
if [ -f "$MUSL_DF" ]; then
  if grep -nE '^\s*(FROM|COPY)[^#]*--platform=' "$MUSL_DF"; then
    echo "FAIL: $MUSL_DF pins --platform; it must build natively per arch" >&2
    fail=1
  fi
  if grep -nE 'cargo[^#]*--target[= ]' "$MUSL_DF"; then
    echo "FAIL: $MUSL_DF passes an explicit cargo --target; the Alpine host target must be used" >&2
    fail=1
  fi
fi

# The publish workflow for that image must cover BOTH arches on native runners.
MUSL_WF=".github/workflows/publish-musl-image.yml"
if [ -f "$MUSL_WF" ]; then
  for arch in amd64 arm64; do
    grep -qE "arch: ${arch}\b" "$MUSL_WF" \
      || { echo "FAIL: $MUSL_WF has no '${arch}' matrix entry" >&2; fail=1; }
  done
  # arm64 must run on arm64 hardware — QEMU emulation turns a full release
  # build of the workspace into a multi-hour job.
  grep -q 'ubuntu-24.04-arm' "$MUSL_WF" \
    || { echo "FAIL: $MUSL_WF must build arm64 on a native arm64 runner" >&2; fail=1; }
else
  echo "FAIL: $MUSL_WF missing — the musl image is built for no arch at all" >&2
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo "test-dockerfile-multiarch: FAILED" >&2
  exit 1
fi
echo "test-dockerfile-multiarch: OK — Dockerfile downloads arch-parameterised, musl image arch-neutral + built for both arches"
