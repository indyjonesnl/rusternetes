#!/usr/bin/env bash
# Record which published image revision a conformance run tests (#1696).
#
# usage: image-provenance.sh <image-tag> <expected-main-sha>
#
# Prints markdown (for $GITHUB_STEP_SUMMARY) with the digest of the api-server
# image behind <image-tag>. When <image-tag> is the moving `main` tag it also
# compares that digest to the immutable `sha-<expected-main-sha>` tag that
# publish-images.yml applies to every main commit, and emits a ::warning:: when
# they differ, i.e. `:main` has not caught up and the run tests old binaries.
# Warn-only (always exits 0): provenance must never redden a conformance run.
#
# No upstream Kubernetes equivalent (Rusternetes-only CI concern).
set -uo pipefail

TAG="${1:?usage: image-provenance.sh <image-tag> <expected-main-sha>}"
EXPECTED_SHA="${2:?usage: image-provenance.sh <image-tag> <expected-main-sha>}"
IMAGE="${PROVENANCE_IMAGE:-ghcr.io/indyjonesnl/rusternetes/api-server}"
# Overridable so the test can stub the registry round-trip.
# (default assigned separately: the literal `}}` would close ${...:-} early)
if [ -z "${IMAGETOOLS:-}" ]; then
  IMAGETOOLS='docker buildx imagetools inspect --format {{.Manifest.Digest}}'
fi

digest() { # ref -> digest on stdout, non-zero when unresolvable
  # shellcheck disable=SC2086
  $IMAGETOOLS "$1" 2>/dev/null | head -n1 | grep -E '^sha256:'
}

if [ "$TAG" = "local" ]; then
  echo "image provenance: built from source, no published image under test"
  exit 0
fi

if ! tag_digest="$(digest "$IMAGE:$TAG")"; then
  echo "::warning::image provenance: could not resolve $IMAGE:$TAG"
  exit 0
fi
echo "image provenance: \`$IMAGE:$TAG\` = \`$tag_digest\`"

if [ "$TAG" = "main" ]; then
  sha_tag="sha-$EXPECTED_SHA"
  if ! sha_digest="$(digest "$IMAGE:$sha_tag")"; then
    echo "::warning::image provenance: $IMAGE:$sha_tag is not published yet, so \`main\` may be stale"
  elif [ "$sha_digest" != "$tag_digest" ]; then
    echo "::warning::image provenance: $IMAGE:main ($tag_digest) is stale; main HEAD $sha_tag is $sha_digest"
  else
    echo "image provenance: \`main\` is current (== \`$sha_tag\`)"
  fi
fi
exit 0
