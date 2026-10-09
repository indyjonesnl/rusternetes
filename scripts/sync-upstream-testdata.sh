#!/usr/bin/env bash
# Refresh the vendored upstream Kubernetes test oracles under
# tests/upstream/k8s-1.35/ from a local kubernetes checkout.
#
# Usage: scripts/sync-upstream-testdata.sh [path-to-kubernetes-checkout]
#   default checkout: ../kubernetes (sibling of this repo)
#   FORCE=1 skips the release-branch check.
#
# Never edit the vendored files by hand; re-run this script instead.
set -euo pipefail

EXPECTED_BRANCH="release-1.35"
UPSTREAM_URL="https://github.com/kubernetes/kubernetes"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -ge 1 ]]; then
  k8s="$1"
else
  k8s="$repo_root/../kubernetes"
  if [[ ! -d "$k8s" ]]; then
    # In a git worktree the sibling checkout lives next to the MAIN checkout.
    main_root="$(cd "$(git -C "$repo_root" rev-parse --git-common-dir)/.." && pwd)"
    k8s="$main_root/../kubernetes"
  fi
fi
k8s="$(cd "$k8s" && pwd)"
dest="$repo_root/tests/upstream/k8s-1.35"

branch="$(git -C "$k8s" branch --show-current)"
if [[ "$branch" != "$EXPECTED_BRANCH" && "${FORCE:-0}" != "1" ]]; then
  echo "refusing: $k8s is on '$branch', expected '$EXPECTED_BRANCH' (FORCE=1 to override)" >&2
  exit 1
fi
sha="$(git -C "$k8s" log -1 --format=%H)"

# "<upstream path>:<destination under tests/upstream/k8s-1.35>"
paths=(
  "staging/src/k8s.io/api/testdata/HEAD:api-testdata"
  "staging/src/k8s.io/apiextensions-apiserver/pkg/apis/testdata/HEAD:apiextensions-testdata"
  "api/openapi-spec/v3:openapi-spec/v3"
)

mkdir -p "$dest/openapi-spec"
for p in "${paths[@]}"; do
  src="${p%%:*}"
  dst="${p##*:}"
  mkdir -p "$dest/$dst"
  rsync -a --delete "$k8s/$src/" "$dest/$dst/"
done
rsync -a "$k8s/api/openapi-spec/swagger.json" "$dest/openapi-spec/swagger.json"

copied=""
for p in "${paths[@]}"; do copied+="  ${p%%:*} -> tests/upstream/k8s-1.35/${p##*:}/"$'\n'; done
copied+="  api/openapi-spec/swagger.json -> tests/upstream/k8s-1.35/openapi-spec/swagger.json"$'\n'

cat > "$dest/PROVENANCE" <<EOF
Upstream:  $UPSTREAM_URL
Branch:    $EXPECTED_BRANCH
Commit:    $sha
Synced:    $(date -u +%Y-%m-%d)
License:   Apache-2.0 (see ../LICENSE)

Copied paths (upstream -> here):
$copied
Refresh:   scripts/sync-upstream-testdata.sh [path-to-kubernetes-checkout]
           (checkout must be on $EXPECTED_BRANCH; FORCE=1 overrides).
Do not edit these files by hand.
EOF

echo "synced from $sha"
du -sh "$dest"
