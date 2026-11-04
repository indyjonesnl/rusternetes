#!/usr/bin/env bash
# Copy the images listed in ci/mirror/images.txt from registry.k8s.io to
# ghcr.io/indyjonesnl/rusternetes/mirror/<path> as multi-arch manifests.
#
# `docker buildx imagetools create` copies the manifest list registry-to-registry
# (no local pull), so the destination digest equals the source digest. Idempotent:
# an image whose destination digest already matches is skipped. Each registry
# call is retried (registry.k8s.io's CDN times out intermittently).
#
# Requires: docker buildx, jq, and a prior `docker login ghcr.io` with push access.
# Usage: scripts/mirror-images.sh [images-file]
#   MIRROR_PREFIX  destination prefix (default ghcr.io/indyjonesnl/rusternetes/mirror)
#   SOURCE_REGISTRY source registry   (default registry.k8s.io)
#   RETRIES        attempts per call  (default 5)
set -euo pipefail

export BUILDKIT_PROGRESS=plain
LIST="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)/ci/mirror/images.txt}"
MIRROR_PREFIX="${MIRROR_PREFIX:-ghcr.io/indyjonesnl/rusternetes/mirror}"
SOURCE_REGISTRY="${SOURCE_REGISTRY:-registry.k8s.io}"
RETRIES="${RETRIES:-5}"

retry() {
    local n=1 delay=5
    until "$@"; do
        if [ "$n" -ge "$RETRIES" ]; then
            return 1
        fi
        echo "  retry $n/$RETRIES in ${delay}s: $1 ${2:-} ${3:-}" >&2
        sleep "$delay"
        n=$((n + 1))
        delay=$((delay * 2))
    done
}

# digest_of <ref>: prints the top-level manifest digest, fails if absent.
digest_of() {
    docker buildx imagetools inspect --format '{{json .Manifest}}' "$1" 2>/dev/null | jq -er '.digest'
}

failed=0 copied=0 skipped=0
while IFS= read -r line; do
    line="${line%%#*}"
    line="$(echo "$line" | tr -d '[:space:]')"
    [ -n "$line" ] || continue
    src="${SOURCE_REGISTRY}/${line}"
    dst="${MIRROR_PREFIX}/${line}"

    if ! src_digest="$(retry digest_of "$src")"; then
        echo "::error::cannot resolve $src"
        failed=$((failed + 1))
        continue
    fi
    # One attempt: a missing destination is the normal first-run case.
    dst_digest="$(digest_of "$dst" || true)"
    if [ "$src_digest" = "$dst_digest" ]; then
        echo "ok    $line $src_digest"
        skipped=$((skipped + 1))
        continue
    fi
    if ! retry docker buildx imagetools create --progress=plain -t "$dst" "$src"; then
        echo "::error::failed to copy $src -> $dst"
        failed=$((failed + 1))
        continue
    fi
    dst_digest="$(retry digest_of "$dst" || true)"
    if [ "$src_digest" != "$dst_digest" ]; then
        echo "::error::digest mismatch for $line: source $src_digest, mirror ${dst_digest:-<none>}"
        failed=$((failed + 1))
        continue
    fi
    echo "copy  $line $src_digest"
    copied=$((copied + 1))
done < "$LIST"

echo "mirror: copied=$copied up-to-date=$skipped failed=$failed"
[ "$failed" -eq 0 ]
