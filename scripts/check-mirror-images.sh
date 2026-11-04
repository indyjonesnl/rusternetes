#!/usr/bin/env bash
# Fail when CI or its scripts reference a registry.k8s.io image that is not in
# ci/mirror/images.txt (the list scripts/mirror-images.sh copies to ghcr.io), so
# a version bump cannot silently bypass the mirror.
#
# Scope: scripts/*.sh, ci/, deploy/, compose*.yml, .github/. Excluded: scripts/k0s-diff
# (manual diffing tooling), conformance-single-test.sh (unpinned
# local hydrophone fallback), vanilla-swap guard fixtures, comment lines, and this check's own list.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

LIST=ci/mirror/images.txt
listed="$(sed 's/#.*//' "$LIST" | tr -d ' \t' | grep -v '^$' | sort -u)"

refs="$(grep -rhE 'registry\.k8s\.io/[a-z0-9._/-]+:[A-Za-z0-9._-]+' \
    --include='*.sh' --include='*.yml' --include='*.yaml' --include='*.toml' \
    scripts ci deploy compose*.yml .github 2>/dev/null \
    --exclude-dir=k0s-diff --exclude=vanilla-swap-guard-test.sh \
    --exclude=mirror-images.yml --exclude=conformance-single-test.sh \
    | grep -vE '^[[:space:]]*#' \
    | grep -oE 'registry\.k8s\.io/[a-z0-9._/-]+:[A-Za-z0-9._-]+' || true)"
# Control-plane images from the vanilla-swap tooling are not mirrored.
refs="$(echo "$refs" | sed 's#^registry\.k8s\.io/##' | grep -vE '^kube-(apiserver|controller-manager|scheduler|proxy):' | sort -u)"

# Everything side-loaded must also be mirrored.
while IFS= read -r ref; do
    [ -n "$ref" ] || continue
    grep -qxF "$ref" <<<"$listed" || { echo "ci/mirror/sideload.txt: $ref is not in $LIST" >&2; exit 1; }
done < <(sed 's/#.*//' ci/mirror/sideload.txt | tr -d ' \t' | grep -v '^$')

missing=0
while IFS= read -r ref; do
    [ -n "$ref" ] || continue
    if ! grep -qxF "$ref" <<<"$listed"; then
        echo "not in $LIST: registry.k8s.io/$ref" >&2
        missing=1
    fi
done <<<"$refs"
[ "$missing" -eq 0 ] && echo "mirror list covers every registry.k8s.io reference ($(echo "$refs" | grep -c .) refs)"
[ "$missing" -eq 0 ] || exit 1

# --list-images: also compare against what the pinned conformance image's e2e
# binary will actually pull (the authoritative set; manifest.go alone drifts).
if [ "${1:-}" = "--list-images" ]; then
    conf="$(grep -E '^conformance:' "$LIST" | head -1)"
    mirror="ghcr.io/indyjonesnl/rusternetes/mirror/$conf"
    img="registry.k8s.io/$conf"
    docker pull -q "$mirror" >/dev/null 2>&1 && img="$mirror"
    echo "listing images from $img"
    # node-perf/* images are multi-GB and node-performance-only (not conformance);
    # they are intentionally not mirrored, so they are skipped here.
    actual="$(docker run --rm --entrypoint e2e.test "$img" --list-images \
        | grep -E '^registry\.k8s\.io/' | grep -v '^registry\.k8s\.io/e2e-test-images/node-perf/' | sed 's#^registry\.k8s\.io/##' | sort -u)"
    [ -n "$actual" ] || { echo "e2e.test --list-images produced nothing" >&2; exit 1; }
    drift=0
    REPO_LIST=ci/mirror/e2e-repo-list.yaml
    while IFS= read -r ref; do
        grep -qxF "$ref" <<<"$listed" || { echo "e2e pulls registry.k8s.io/$ref, not in $LIST" >&2; drift=1; }
        # The registry map must redirect this image's registry to the mirror path
        # (manifest.go replaceRegistryInImageURLWithList matches on registry+user).
        # (an image name may itself contain a slash, e.g. e2e-test-images/volume/nfs).
        mapped=0
        while IFS= read -r v; do
            case "ghcr.io/indyjonesnl/rusternetes/mirror/$ref" in "$v"/*) mapped=1 ;; esac
        done < <(sed -nE 's/^[A-Za-z0-9]+Registry: *//p' "$REPO_LIST")
        [ "$mapped" -eq 1 ] || { echo "$REPO_LIST maps no registry covering $ref" >&2; drift=1; }
    done <<<"$actual"
    [ "$drift" -eq 0 ] && echo "mirror list covers all $(echo "$actual" | grep -c .) images from e2e.test --list-images"
    exit "$drift"
fi
