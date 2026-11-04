#!/usr/bin/env bash
# Decide whether a conformance run pulls its images from the ghcr.io mirror
# (ci/mirror/images.txt) or from registry.k8s.io, and say so on stdout:
#   mirror    every image in the list answers an anonymous manifest GET
#   upstream  anything else (mirror unreachable, or any image missing/not public)
# The reason goes to stderr. Never exits non-zero because of the mirror: a
# missing or unreachable mirror just means "upstream". Exit 2 only on usage errors.
#
# Why all-or-nothing: the e2e image override is a registry *map*
# (ci/mirror/e2e-repo-list.yaml -> e2e.test KUBE_TEST_REPO_LIST,
# kubernetes test/utils/image/manifest.go readRepoList), so it cannot fall back
# per image the way a containerd mirror host could. One missing image would turn
# into an ImagePullBackOff mid-suite, so any gap selects upstream for the run.
#
# Probe = the OCI distribution anonymous flow: GET ghcr.io/token for a pull-scoped
# token, then GET /v2/<repo>/manifests/<tag> (distribution-spec, "Pulling
# manifests"). curl only; no registry login.
#   MIRROR_PREFIX  default ghcr.io/indyjonesnl/rusternetes/mirror
#   PROBE_JOBS     parallel probes (default 8)
#   PROBE_TIMEOUT  seconds per request (default 10)
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 2

LIST="${1:-ci/mirror/images.txt}"
MIRROR_PREFIX="${MIRROR_PREFIX:-ghcr.io/indyjonesnl/rusternetes/mirror}"
PROBE_JOBS="${PROBE_JOBS:-8}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-10}"
[ -f "$LIST" ] || { echo "image list not found: $LIST" >&2; exit 2; }
command -v curl >/dev/null 2>&1 || { echo "curl not found; using upstream" >&2; echo upstream; exit 0; }

host="${MIRROR_PREFIX%%/*}"
repo_prefix="${MIRROR_PREFIX#*/}"

probe_one() {
    local ref="$1" path="${1%%:*}" tag="${1#*:}" repo token code
    repo="$repo_prefix/$path"
    token="$(curl -fsS --max-time "$PROBE_TIMEOUT" --retry 2 \
        "https://$host/token?scope=repository:$repo:pull" 2>/dev/null \
        | sed -n 's/.*"token" *: *"\([^"]*\)".*/\1/p')"
    [ -n "$token" ] || { echo "no anonymous token for $repo" >&2; return 1; }
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time "$PROBE_TIMEOUT" --retry 2 \
        -H "Authorization: Bearer $token" \
        -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
        "https://$host/v2/$repo/manifests/$tag")"
    [ "$code" = 200 ] || { echo "$MIRROR_PREFIX/$ref -> HTTP $code" >&2; return 1; }
}
export -f probe_one
export host repo_prefix PROBE_TIMEOUT MIRROR_PREFIX

refs="$(sed 's/#.*//' "$LIST" | tr -d ' \t' | grep -v '^$')"
if printf '%s\n' "$refs" | xargs -P "$PROBE_JOBS" -I{} bash -c 'probe_one "$1"' _ {}; then
    echo "mirror: all $(printf '%s\n' "$refs" | grep -c .) images answer anonymously at $MIRROR_PREFIX" >&2
    echo mirror
else
    echo "mirror incomplete or unreachable; using registry.k8s.io" >&2
    echo upstream
fi
