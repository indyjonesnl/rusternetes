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

if [ "$fail" -ne 0 ]; then
  echo "test-dockerfile-multiarch: FAILED" >&2
  exit 1
fi
echo "test-dockerfile-multiarch: OK — all Dockerfile release downloads are arch-parameterised"
