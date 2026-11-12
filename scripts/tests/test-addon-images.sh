#!/usr/bin/env bash
# Every image the k3s addon manifests in deploy/addons/ request through
# %{SYSTEM_DEFAULT_REGISTRY}% must be listed in ci/mirror/addon-images.txt, which
# is what the mirror workflow copies to ghcr and what scripts/bootstrap-cluster.sh
# side-loads into each node's containerd. A manifest bump (k3s tag bump) that
# forgets the list would leave the pod pulling from a registry inside the cluster.
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

list="$(sed 's/#.*//' ci/mirror/addon-images.txt | tr -d ' \t' | grep -v '^$' | sort -u)"
want="$(grep -hoE '%\{SYSTEM_DEFAULT_REGISTRY\}%[^"[:space:]]+' deploy/addons/*.yaml \
    | sed 's/^%{SYSTEM_DEFAULT_REGISTRY}%//' | sort -u)"

fail=0
for img in $want; do
    if grep -qxF "$img" <<<"$list"; then
        echo "ok   [$img is in ci/mirror/addon-images.txt]"
    else
        echo "FAIL [$img is requested by deploy/addons but missing from ci/mirror/addon-images.txt]"
        fail=1
    fi
done
[ -n "$want" ] || { echo "FAIL: no addon images found in deploy/addons"; fail=1; }
[ "$fail" -eq 0 ] && echo "PASS: addon images are mirrored"
exit "$fail"
