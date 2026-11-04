#!/usr/bin/env bash
# Pre-pull the conformance image and the core e2e images with the host container
# runtime and import them into every cluster node's containerd image store, so
# the in-cluster containerd (which can time out on TLS to registries even while
# the host daemon pulls fine) finds them already present. Same mechanism as
# scripts/bootstrap-cluster.sh import_image_into_containerd: `save | exec -i
# <containerd> ctr -n k8s.io images import -` into EACH runtime (every containerd
# keeps its own store). The pod specs use imagePullPolicy IfNotPresent
# (hydrophone v0.7.0 pkg/conformance/deploy.go:155; kubelet shouldPullImage), so a
# present image is never re-pulled.
#
# Best effort by design: every failure warns and the script exits 0, so a run
# degrades to pulling in-cluster, exactly as before.
#
# Usage: sideload-images.sh --image-source mirror|upstream [--conformance-image REF]
#   env: CONTAINER_RT (docker), CONTAINERD_SERVICE_CONTAINER (one runtime name;
#        default: discover rusternetes-containerd*), MIRROR_PREFIX, PULL_RETRIES (3)
#   --sandbox-image REF  sandbox image name containerd requests (default: the
#                        pinned_images.sandbox in deploy/containerd/config.toml)
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)" || exit 0
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
RT="${CONTAINER_RT:-docker}"
MIRROR_PREFIX="${MIRROR_PREFIX:-ghcr.io/indyjonesnl/rusternetes/mirror}"
PULL_RETRIES="${PULL_RETRIES:-3}"
LIST="${SIDELOAD_LIST:-$REPO_ROOT/ci/mirror/sideload.txt}"
SOURCE=upstream; CONF_IMAGE=""; SANDBOX=""
warn() { echo "[sideload] WARN: $*" >&2; }
say() { echo "[sideload] $*"; }

while [ $# -gt 0 ]; do
    case "$1" in
        --image-source) SOURCE="${2:-upstream}"; shift 2 ;;
        --conformance-image) CONF_IMAGE="${2:-}"; shift 2 ;;
        --sandbox-image) SANDBOX="${2:-}"; shift 2 ;;
        *) warn "ignoring unknown argument $1"; shift ;;
    esac
done
[ -n "$SANDBOX" ] || SANDBOX="$(sed -nE "s/^[[:space:]]*sandbox *= *['\"]([^'\"]+)['\"].*/\1/p" "$REPO_ROOT/deploy/containerd/config.toml" 2>/dev/null | head -1)"

command -v "$RT" >/dev/null 2>&1 || { warn "$RT not found; skipping"; exit 0; }
if [ -n "${CONTAINERD_SERVICE_CONTAINER:-}" ]; then
    runtimes="$CONTAINERD_SERVICE_CONTAINER"
else
    runtimes="$("$RT" ps --format '{{.Names}}' 2>/dev/null | grep -E '^rusternetes-containerd[0-9]*$' | sort || true)"
fi
[ -n "$runtimes" ] || { say "no separate containerd service (all-in-one stack?); nothing to side-load"; exit 0; }

pull_retry() {   # pull_retry <ref>
    local n=1 delay="${PULL_BACKOFF:-5}"
    until "$RT" pull --quiet "$1" >/dev/null 2>&1; do
        [ "$n" -ge "$PULL_RETRIES" ] && return 1
        sleep "$delay"; n=$((n + 1)); delay=$((delay * 2))
    done
}

# acquire <target-name> [<path-under-registry.k8s.io>]: pull from the chosen
# source (falling back to the other), tag as the exact name a pod will request.
TARGETS=()
acquire() {
    local target="$1" path="${2:-}" cand c
    if [ -z "$path" ]; then
        case "$target" in
            "$MIRROR_PREFIX"/*) path="${target#"$MIRROR_PREFIX"/}" ;;
            registry.k8s.io/*) path="${target#registry.k8s.io/}" ;;
        esac
    fi
    if [ "$SOURCE" = mirror ] && [ -n "$path" ]; then cand=("$MIRROR_PREFIX/$path" "registry.k8s.io/$path")
    elif [ -n "$path" ]; then cand=("registry.k8s.io/$path" "$MIRROR_PREFIX/$path")
    else cand=("$target"); fi
    for c in "${cand[@]}"; do
        if pull_retry "$c"; then
            [ "$c" = "$target" ] || "$RT" tag "$c" "$target" >/dev/null 2>&1 || { warn "cannot tag $c as $target"; return 1; }
            TARGETS+=("$target"); return 0
        fi
    done
    warn "could not pull $target (left to the cluster's own pull)"
    return 1
}

if [ -n "$CONF_IMAGE" ]; then acquire "$CONF_IMAGE"; fi
if [ -n "$SANDBOX" ]; then acquire "$SANDBOX"; fi
if [ -f "$LIST" ]; then
    while IFS= read -r path; do
        [ -n "$path" ] || continue
        if [ "$SOURCE" = mirror ]; then acquire "$MIRROR_PREFIX/$path" "$path"; else acquire "registry.k8s.io/$path" "$path"; fi
    done < <(sed 's/#.*//' "$LIST" | tr -d ' \t' | grep -v '^$')
else
    warn "list $LIST not found; only the conformance and sandbox images are side-loaded"
fi

[ "${#TARGETS[@]}" -gt 0 ] || { warn "nothing was pulled; skipping import"; exit 0; }
mapfile -t UNIQ < <(printf '%s\n' "${TARGETS[@]}" | sort -u)
for rt in $runtimes; do
    if "$RT" save "${UNIQ[@]}" | "$RT" exec -i "$rt" ctr -n k8s.io images import - >/dev/null 2>&1; then
        say "imported ${#UNIQ[@]} images into $rt"
    else
        warn "import into $rt failed; its pods will pull in-cluster"
    fi
done
exit 0
