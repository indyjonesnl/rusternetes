#!/bin/bash

# Bootstrap Cluster Script
# This script handles the complete cluster bootstrap process:
# 1. Generate ServiceAccount tokens
# 2. Apply ServiceAccounts and Secrets
# 3. Apply bootstrap resources (namespaces, services, priority classes)

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${GREEN}==>${NC} $1"
}

# Detect container runtime (docker or podman)
# Usage: bootstrap-cluster.sh [docker|podman]
# Or set CONTAINER_RUNTIME=docker|podman
if [ -n "$1" ] && [[ "$1" == "docker" || "$1" == "podman" ]]; then
    CONTAINER_RT="$1"
elif [ -n "$CONTAINER_RUNTIME" ]; then
    CONTAINER_RT="$CONTAINER_RUNTIME"
else
    HAS_PODMAN=false
    HAS_DOCKER=false
    # Use background + wait to timeout commands that may hang (e.g. docker ps when Docker Desktop is stopped)
    if command -v podman &>/dev/null; then
        podman ps &>/dev/null 2>&1 & PID=$!; ( sleep 3; kill $PID 2>/dev/null ) &>/dev/null & wait $PID 2>/dev/null && HAS_PODMAN=true
    fi
    if command -v docker &>/dev/null; then
        docker ps &>/dev/null 2>&1 & PID=$!; ( sleep 3; kill $PID 2>/dev/null ) &>/dev/null & wait $PID 2>/dev/null && HAS_DOCKER=true
    fi

    if $HAS_PODMAN && $HAS_DOCKER; then
        echo "ERROR: Both docker and podman are available. Please specify which to use:"
        echo "  bash $0 docker"
        echo "  bash $0 podman"
        echo "  CONTAINER_RUNTIME=docker bash $0"
        exit 1
    elif $HAS_PODMAN; then
        CONTAINER_RT=podman
    elif $HAS_DOCKER; then
        CONTAINER_RT=docker
    else
        echo "ERROR: No container runtime (docker or podman) found"
        exit 1
    fi
fi

# SCRIPT_DIR / PROJECT_ROOT are needed for the bridge-gateway discovery
# below (and the later kubectl / yaml-application steps). Define them
# before any block that depends on them.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- control-plane image resolution ----------------------------------------
# The kube-scheduler / kube-controller-manager static pods AND the
# CoreDNS Deployment run INSIDE the containerd CRI service, which keeps
# its own image store (the `containerd-data` volume). That store is separate
# from the docker/podman daemon `compose build`/`compose pull` write to, so the
# images must be put there one of two ways:
#
#   * CI prebuilt path — CONTROL_PLANE_IMAGE_REGISTRY is set (the
#     bring-up-cluster action exports ghcr.io/indyjonesnl/rusternetes when the
#     GHCR pull succeeds). We template that registry ref + tag into the static
#     pod manifests / dns Deployment. The docker daemon already holds those
#     images (`compose pull`, which retries), so they are imported into the
#     containerd store exactly like the local build path, and
#     imagePullPolicy: IfNotPresent then never makes containerd reach the
#     registry itself. That in-cluster pull can time out on TLS even while the
#     host daemon pulls fine (scripts/sideload-images.sh header); if the image is
#     not in the daemon the import is a warning no-op and containerd pulls it
#     as before.
#
#   * Local build path — registry unset. Images were built into the local
#     docker/podman daemon as k3rs-<component>:latest, which the separate
#     containerd store cannot see, so we `save | ctr -n k8s.io images import`
#     them over the CRI socket. Without this every static pod / dns pod fails
#     with `pull access denied ... docker.io/library/k3rs-*` (the kubelet
#     falls back to docker.io for the unqualified local tag).
CONTROL_PLANE_IMAGE_REGISTRY="${CONTROL_PLANE_IMAGE_REGISTRY:-}"
CONTROL_PLANE_IMAGE_TAG="${K3RS_IMAGE_TAG:-main}"
# Explicit single-runtime override (scripts/run-node-conformance.sh sets this to
# k3rs-nc-containerd). Empty means "discover the compose runtimes" — see
# containerd_service_containers.
CONTAINERD_SERVICE_CONTAINER="${CONTAINERD_SERVICE_CONTAINER:-}"

# Resolve the image ref for an in-containerd component (scheduler,
# controller-manager, dns): GHCR ref on the prebuilt path, local :latest tag
# otherwise.
resolve_cluster_image() {
    local component="$1"
    if [ -n "$CONTROL_PLANE_IMAGE_REGISTRY" ]; then
        echo "${CONTROL_PLANE_IMAGE_REGISTRY}/${component}:${CONTROL_PLANE_IMAGE_TAG}"
    else
        echo "k3rs-${component}:latest"
    fi
}

# Import a locally-built image into the containerd CRI service's image store so
# imagePullPolicy: IfNotPresent resolves it without a registry round-trip. No-op
# when there is no separate containerd service (all-in-one stack) or the source
# image was never built locally. `docker save` emits a docker-archive tarball
# that `ctr images import` auto-detects; ctr normalizes the RepoTag
# k3rs-X:latest to docker.io/library/k3rs-X:latest, the exact ref
# the CRI ImageService resolves the unqualified pod-spec name to, so the lookup
# matches.
# Every node's runtime that is currently up. Each containerd keeps its OWN image
# store, so a local image must be imported into all of them: importing into one
# leaves pods scheduled to the other node stuck on
# "failed to resolve image ... pull access denied" (#1691), which then fails the
# conformance suite's BeforeSuite. Honours an explicit
# CONTAINERD_SERVICE_CONTAINER (run-node-conformance.sh names its own runtime),
# else discovers the compose runtimes.
containerd_service_containers() {
    if [ -n "${CONTAINERD_SERVICE_CONTAINER:-}" ]; then
        printf '%s\n' "$CONTAINERD_SERVICE_CONTAINER"
        return 0
    fi
    ${CONTAINER_RT:-docker} ps --format '{{.Names}}' 2>/dev/null \
        | grep -E '^k3rs-containerd[0-9]*$' \
        | sort
}

# The pod subnet this stack declares, read off the running kube-proxy
# node-network agents (`--pod-cidr`). Empty when no agent runs — i.e. when the
# stack's CNI does not derive per-node subnets from node.spec.podCIDR.
stack_pod_subnet() {
    local names name cmd cidr
    names="$(${CONTAINER_RT:-docker} ps --format '{{.Names}}' 2>/dev/null \
        | grep -E '^k3rs-kube-proxy[0-9]*$' \
        | sort)"
    for name in $names; do
        cmd="$(${CONTAINER_RT:-docker} inspect -f '{{join .Config.Cmd " "}}' "$name" 2>/dev/null || true)"
        cidr="$(printf '%s\n' "$cmd" \
            | grep -oE -- '--pod-cidr[= ]+[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+' \
            | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+' \
            | head -1)"
        if [ -n "$cidr" ]; then
            printf '%s' "$cidr"
            return 0
        fi
    done
}

# Cluster CIDR to run node IPAM with, or empty to leave the allocator off.
#
# Ported from kubeadm, which does NOT gate the allocator behind its own flag —
# it turns it on whenever the cluster declares a pod subnet
# (../kubernetes/cmd/kubeadm/app/phases/controlplane/manifests.go:351:
#   if cfg.Networking.PodSubnet != "" {
#       ... SetArgValues(..., "allocate-node-cidrs", "true", 1)
#       ... SetArgValues(..., "cluster-cidr", cfg.Networking.PodSubnet, 1)
#   }
# ). Here the stack's pod subnet is what its kube-proxy node-network agents run
# with, so node IPAM follows that declaration automatically: the multi-node
# compose stack needs podCIDRs before its CNI conflists exist at all, and every
# bring-up path (CI action, cluster-up.sh, the manual recipe) gets them without
# having to remember an env var (#1697).
#
# ALLOCATE_NODE_CIDRS still overrides in both directions: 0/false/no/off forces
# the allocator off, any other non-empty value forces it on (compose.calico.yml's
# documented recipe) and defaults the CIDR when nothing is detectable.
# CLUSTER_CIDR, when set, is the operator's word on the subnet.
node_ipam_cluster_cidr() {
    local mode="${ALLOCATE_NODE_CIDRS:-auto}"
    case "$mode" in
        0 | false | no | off) return 0 ;;
    esac

    local cidr="${CLUSTER_CIDR:-}"
    [ -n "$cidr" ] || cidr="$(stack_pod_subnet)"
    if [ -z "$cidr" ] && [ "$mode" != auto ]; then
        cidr=10.244.0.0/16
    fi
    printf '%s' "$cidr"
}

# CSI external-attacher(s) + AttachDetachController for the controller-manager
# (#3128): CSI_ATTACHERS="drv1=/sock1,drv2=unix:///sock2" -> one --csi-attacher
# each; ATTACH_DETACH_CONTROLLER=1 -> --attach-detach-controller. Both off by
# default.
#
# The controller-manager dials each driver socket from inside its static pod,
# so the socket's directory has to be a hostPath volume of that pod (#3133).
# Ported from how kubeadm makes a host plugin-socket dir reachable to the
# kube-controller-manager: the flexvolume dir is mounted read-write as a
# DirectoryOrCreate hostPath, "as it is used for third-party plugins to
# integrate with their storage backends via unix domain socket"
# (cmd/kubeadm/app/phases/controlplane/volumes.go:73-79). The in-process CSI
# attacher itself is k3rs-only (upstream's controller-manager has no
# CSI socket; the external-attacher sidecar does); the mount follows kubeadm.
#
# csi_socket_dirs prints the unique parent directories of the CSI_ATTACHERS
# sockets, one per line. The unix:// / unix: scheme is stripped the way the
# client does (crates/csi/src/lib.rs normalize_endpoint).
csi_socket_dirs() {
    [ -n "${CSI_ATTACHERS:-}" ] || return 0
    local -a specs
    local spec sock
    IFS=',' read -r -a specs <<< "$CSI_ATTACHERS"
    for spec in "${specs[@]}"; do
        sock="${spec#*=}"
        case "$sock" in
            unix://*) sock="${sock#unix://}" ;;
            unix:*) sock="${sock#unix:}" ;;
        esac
        dirname -- "$sock"
    done | awk '!seen[$0]++'
}

# template_csi_attach <manifest> expands the controller-manager manifest's
# @CSI_ATTACH_ARGS@, @CSI_SOCKET_MOUNTS@ and @CSI_SOCKET_VOLUMES@ placeholder
# lines (one hostPath volume + mount per csi_socket_dirs entry), or deletes them
# when CSI attach is off. Manifests without the placeholders are untouched.
template_csi_attach() {
    local dst="$1" args='' mounts='' volumes='' spec dir i=0
    local -a specs
    if [ -n "${CSI_ATTACHERS:-}" ]; then
        IFS=',' read -r -a specs <<< "$CSI_ATTACHERS"
        for spec in "${specs[@]}"; do
            args+='    - "--csi-attacher"\n    - "'"${spec}"'"\n'
        done
    fi
    if [ "${ATTACH_DETACH_CONTROLLER:-}" = "1" ]; then
        args+='    - "--attach-detach-controller"\n'
    fi
    while IFS= read -r dir; do
        [ -n "$dir" ] || continue
        mounts+='    - name: csi-socket-'"${i}"'\n      mountPath: "'"${dir}"'"\n'
        volumes+='  - name: csi-socket-'"${i}"'\n    hostPath:\n      path: "'"${dir}"'"\n      type: DirectoryOrCreate\n'
        i=$((i + 1))
    done < <(csi_socket_dirs)
    local placeholder value
    for placeholder in CSI_ATTACH_ARGS CSI_SOCKET_MOUNTS CSI_SOCKET_VOLUMES; do
        case "$placeholder" in
            CSI_ATTACH_ARGS) value="$args" ;;
            CSI_SOCKET_MOUNTS) value="$mounts" ;;
            CSI_SOCKET_VOLUMES) value="$volumes" ;;
        esac
        value="${value%\\n}"
        if [ -n "$value" ]; then
            sed -i "s|^@${placeholder}@\$|${value}|" "$dst"
        else
            sed -i "/^@${placeholder}@\$/d" "$dst"
        fi
    done
}

import_image_into_containerd() {
    local image="$1"
    local runtimes
    runtimes="$(containerd_service_containers)"
    if [ -z "$runtimes" ]; then
        return 0   # all-in-one / no separate containerd service
    fi
    if ! $CONTAINER_RT image inspect "$image" >/dev/null 2>&1; then
        print_warning "Local image $image not found — skipping containerd import (build it: $CONTAINER_RT compose --profile build build)"
        return 0
    fi
    local rt
    for rt in $runtimes; do
        echo "  Importing $image into $rt (k8s.io namespace)..."
        if $CONTAINER_RT save "$image" \
            | $CONTAINER_RT exec -i "$rt" ctr -n k8s.io images import -; then
            print_success "Imported $image into $rt"
        else
            print_warning "Failed to import $image into $rt — its pod may stay ImagePullBackOff"
        fi
    done
}

# Side-load an upstream (Go) addon image into every node's containerd under the
# EXACT name the k3s manifest requests, so imagePullPolicy: IfNotPresent never
# reaches a registry from inside the cluster (same reasoning as
# scripts/sideload-images.sh). The host daemon pulls with retries, preferring
# the ghcr mirror (ci/mirror/addon-images.txt, filled by mirror-images.yml, so
# CI does not depend on Docker Hub rate limits) and falling back to the
# upstream registry; the mirror copy is re-tagged to the upstream name first.
# Best effort: on failure the pod's own pull is the fallback.
import_addon_image() {
    local image="$1" mirror_prefix="${MIRROR_PREFIX:-ghcr.io/indyjonesnl/rusternetes/mirror}"
    local path="${image#docker.io/}" src attempt
    if [ -z "$(containerd_service_containers)" ]; then
        return 0   # all-in-one / no separate containerd service
    fi
    if ! $CONTAINER_RT image inspect "$image" >/dev/null 2>&1; then
        for src in "${mirror_prefix}/${path}" "$image"; do
            for attempt in 1 2 3; do
                if $CONTAINER_RT pull --quiet "$src" >/dev/null 2>&1; then
                    [ "$src" = "$image" ] || $CONTAINER_RT tag "$src" "$image"
                    break 2
                fi
                sleep $((attempt * 5))
            done
        done
    fi
    import_image_into_containerd "$image"
}

# Every image of ci/mirror/addon-images.txt (the upstream images the k3s
# manifests in deploy/addons/ run, plus the pause image k3s's own tests use),
# side-loaded under its docker.io name.
import_addon_images() {
    local list="$PROJECT_ROOT/ci/mirror/addon-images.txt" path
    [ -f "$list" ] || return 0
    while IFS= read -r path; do
        [ -n "$path" ] || continue
        import_addon_image "docker.io/$path"
    done < <(sed 's/#.*//' "$list" | tr -d ' \t' | grep -v '^$')
}

# k3s templates its bundled manifests at startup (pkg/server/server.go
# `%{...}%` replacement map: CLUSTER_DOMAIN, CLUSTER_DNS, CLUSTER_DNS_LIST,
# CLUSTER_DNS_IPFAMILYPOLICY, SYSTEM_DEFAULT_REGISTRY, DEFAULT_LOCAL_STORAGE_PATH,
# PREFERRED_ADDRESS_TYPES = addrTypesPrioTemplate(false)).
# Same substitution here; the empty registry leaves the docker.io/rancher/...
# names the airgap list pins. Prints the templated manifest on stdout.
template_k3s_manifest() {
    sed -e 's|%{CLUSTER_DOMAIN}%|cluster.local|g' \
        -e 's|%{SYSTEM_DEFAULT_REGISTRY}%||g' \
        -e 's|%{SYSTEM_DEFAULT_REGISTRY_RAW}%||g' \
        -e 's|%{CLUSTER_DNS}%|10.96.0.10|g' \
        -e 's|%{CLUSTER_DNS_LIST}%|["10.96.0.10"]|g' \
        -e 's|%{CLUSTER_DNS_IPFAMILYPOLICY}%|SingleStack|g' \
        -e 's|%{DEFAULT_LOCAL_STORAGE_PATH}%|/var/lib/k3rs/storage|g' \
        -e 's|%{PREFERRED_ADDRESS_TYPES}%|InternalIP,ExternalIP,Hostname|g' \
        "$1"
}

# The sandbox (pause) image containerd requests before any pod can start
# (deploy/containerd/config.toml pinned_images.sandbox). Pull it with the host
# daemon, which retries, and import it like the control-plane images: the
# in-cluster pull of it can time out on TLS and then no static pod ever starts.
# Best effort — a failure leaves the pull to containerd, as before.
import_sandbox_image() {
    local image attempt=1
    image="$(sed -nE "s/^[[:space:]]*sandbox *= *['\"]([^'\"]+)['\"].*/\1/p" "$PROJECT_ROOT/deploy/containerd/config.toml" 2>/dev/null | head -1)"
    [ -n "$image" ] || return 0
    if ! $CONTAINER_RT image inspect "$image" >/dev/null 2>&1; then
        until $CONTAINER_RT pull --quiet "$image" >/dev/null 2>&1; do
            if [ "$attempt" -ge 3 ]; then
                print_warning "Could not pull sandbox image $image on the host — containerd will pull it itself"
                return 0
            fi
            sleep $((attempt * 5)); attempt=$((attempt + 1))
        done
    fi
    import_image_into_containerd "$image"
}

# Discover the Docker bridge gateway (always [subnet].1) so we can
# bootstrap cluster DNS and other resources without hardcoding an IP.
# Uses the discover-bridge-gateway helper; callers can override via
# K3RS_BRIDGE_GATEWAY env var if discovery fails.
#
# Invoke as a subprocess (not `source`): the helper prints the gateway
# on stdout and ends with `exit 0`, which — if sourced — would terminate
# this bootstrap script before the actual bootstrap work runs. Capturing
# stdout gives us the value without that side-effect.
if [ -z "${K3RS_BRIDGE_GATEWAY:-}" ]; then
    if [ -f "$SCRIPT_DIR/discover-bridge-gateway.sh" ]; then
        K3RS_BRIDGE_GATEWAY="$(bash "$SCRIPT_DIR/discover-bridge-gateway.sh" 2>/dev/null || true)"
        export K3RS_BRIDGE_GATEWAY
    fi
fi

if [ -n "${K3RS_BRIDGE_GATEWAY:-}" ]; then
    echo "Docker bridge gateway: $K3RS_BRIDGE_GATEWAY"
fi

echo "Using container runtime: $CONTAINER_RT"

# Podman needs base images pre-pulled (Docker Desktop caches them)
if [ "$CONTAINER_RT" = "podman" ]; then
    for img in busybox:latest; do
        if ! podman image exists "$img" 2>/dev/null; then
            echo "  Pulling required image: $img"
            podman pull "$img" >/dev/null 2>&1 || true
        fi
    done
fi

print_warning() {
    echo -e "${YELLOW}WARNING:${NC} $1"
}

print_error() {
    echo -e "${RED}ERROR:${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Check if kubectl is available. A pre-set $KUBECTL env var wins so callers can
# pin a specific binary (e.g. the system kubectl when the in-tree one lacks a
# feature). Otherwise prefer the freshly-built in-tree kubectl, then $PATH.
if [ -n "${KUBECTL:-}" ]; then
    :
elif [ -f "$PROJECT_ROOT/target/release/kubectl" ]; then
    KUBECTL="$PROJECT_ROOT/target/release/kubectl"
elif command -v kubectl &> /dev/null; then
    KUBECTL="kubectl"
else
    print_error "kubectl not found. Please build it first with: cargo build --release --bin kubectl"
    exit 1
fi

# Determine kubectl flags
KUBECTL_FLAGS="--insecure-skip-tls-verify"
if [ -z "$KUBECONFIG" ] || [ "$KUBECONFIG" = "/dev/null" ]; then
    KUBECTL_FLAGS="$KUBECTL_FLAGS --server https://localhost:6443"
fi

print_step "Bootstrapping k3rs cluster..."
echo "Using kubectl: $KUBECTL"
echo "Kubectl flags: $KUBECTL_FLAGS"
echo ""

# Step 0: Template control-plane static pod manifests.
#
# The kube-scheduler and kube-controller-manager static pods
# (manifests/control-plane/*.yaml) hostPath-mount the certs dir. Because the
# kubelet runs inside a compose container, that hostPath must be the
# HOST-absolute path of .k3rs/certs (Docker resolves pod hostPaths on the
# host AND the kubelet stat()s it inside its own container — so it has to exist
# at the same path on both sides; compose mounts CERTS_PATH:CERTS_PATH on the
# node-1 kubelet). We rewrite the committed @CERTS_PATH@ placeholder into the
# templated copies under .k3rs/manifests, which is what the node-1
# kubelet's --pod-manifest-path actually mounts. The loop below globs every
# manifest, so new control-plane pods are picked up automatically.
#
# RBAC for system:kube-scheduler (ClusterRole + ClusterRoleBinding) is seeded by
# bootstrap-cluster.yaml (applied below). It is inert while the api-server runs
# with --skip-auth (the authorizer is bypassed), but is honored the moment
# --skip-auth is dropped and --client-ca-file is set — the api-server then
# authenticates the scheduler's CN=system:kube-scheduler client cert as that user
# (x509 authn, #1129). The controller-manager equivalent
# (system:kube-controller-manager) is still TODO. See scripts/generate-certs.sh
# for the full authn notes.
CERTS_PATH="$PROJECT_ROOT/.k3rs/certs"
export CERTS_PATH
if [ -d "$PROJECT_ROOT/manifests/control-plane" ]; then
    import_sandbox_image
    print_step "Templating control-plane static pod manifests (CERTS_PATH=$CERTS_PATH)..."
    mkdir -p "$PROJECT_ROOT/.k3rs/manifests"
    # Node IPAM follows the stack's declared pod subnet (see
    # node_ipam_cluster_cidr above, ported from kubeadm). When there is one,
    # expand the @NODE_IPAM_ARGS@ placeholder in the controller-manager manifest
    # into the allocator flags; otherwise strip the placeholder line so stacks
    # with no pod subnet (etcd/redis/node-conformance) keep node IPAM off, as
    # #1187 intended.
    node_ipam_cidr="$(node_ipam_cluster_cidr)"
    if [ -n "$node_ipam_cidr" ]; then
        CLUSTER_CIDR="$node_ipam_cidr"
        NODE_CIDR_MASK_SIZE="${NODE_CIDR_MASK_SIZE:-24}"
        node_ipam_args='    - "--allocate-node-cidrs"\n    - "--cluster-cidr"\n    - "'"${CLUSTER_CIDR}"'"\n    - "--node-cidr-mask-size"\n    - "'"${NODE_CIDR_MASK_SIZE}"'"'
        print_step "Node IPAM enabled: cluster-cidr=${CLUSTER_CIDR}, node-mask=/${NODE_CIDR_MASK_SIZE}"
    else
        node_ipam_args=''
    fi
    for src in "$PROJECT_ROOT/manifests/control-plane/"*.yaml; do
        [ -e "$src" ] || continue
        dst="$PROJECT_ROOT/.k3rs/manifests/$(basename "$src")"
        # Build the manifest outside the kubelet's watched manifest dir and
        # move it into place only after the image is imported: the kubelet
        # starts the static pod the moment the file appears, and a pull of a
        # not-yet-imported local image fails into an image back-off longer
        # than the bring-up wait.
        staged="$PROJECT_ROOT/.k3rs/.staged-$(basename "$src")"
        # Derive the component (and thus the image) from the manifest name:
        # kube-scheduler.yaml -> scheduler, kube-controller-manager.yaml ->
        # controller-manager. resolve_cluster_image picks GHCR vs local :latest.
        component="$(basename "$src" .yaml)"
        component="${component#kube-}"
        cp_image="$(resolve_cluster_image "$component")"
        sed -e "s|@CERTS_PATH@|${CERTS_PATH}|g" \
            -e "s|@CONTROL_PLANE_IMAGE@|${cp_image}|g" "$src" > "$staged"
        if [ -n "$node_ipam_args" ]; then
            sed -i "s|@NODE_IPAM_ARGS@|${node_ipam_args}|" "$staged"
        else
            sed -i '/@NODE_IPAM_ARGS@/d' "$staged"
        fi
        # CSI attach flags + driver-socket hostPath mounts (#3128, #3133).
        template_csi_attach "$staged"
        echo "  $(basename "$src") -> $dst (image: $cp_image)"
        # The image only exists in the docker/podman daemon (locally built or
        # `compose pull`ed) — import it into the containerd CRI store so
        # IfNotPresent resolves it without a registry round-trip.
        import_image_into_containerd "$cp_image"
        mv -f "$staged" "$dst"
    done
    # Persist CERTS_PATH so a compose restart picks up the same mount path.
    if [ -f "$PROJECT_ROOT/.env" ] && grep -q '^CERTS_PATH=' "$PROJECT_ROOT/.env" 2>/dev/null; then
        sed -i "s|^CERTS_PATH=.*|CERTS_PATH=${CERTS_PATH}|" "$PROJECT_ROOT/.env"
    else
        echo "CERTS_PATH=${CERTS_PATH}" >> "$PROJECT_ROOT/.env"
    fi
    print_success "Static pod manifests templated"
fi

# Step 1: Generate ServiceAccount tokens
print_step "Generating ServiceAccount tokens..."
if [ -f "$SCRIPT_DIR/generate-default-serviceaccounts.sh" ]; then
    bash "$SCRIPT_DIR/generate-default-serviceaccounts.sh"
    print_success "ServiceAccount tokens generated"
else
    print_error "generate-default-serviceaccounts.sh not found"
    exit 1
fi

# Wait a moment for file system sync
sleep 1

# Step 2: Apply ServiceAccounts and Secrets
if [ -f "$PROJECT_ROOT/.k3rs/default-serviceaccounts.yaml" ]; then
    print_step "Applying ServiceAccounts and Secrets..."
    $KUBECTL $KUBECTL_FLAGS apply -f "$PROJECT_ROOT/.k3rs/default-serviceaccounts.yaml"
    print_success "ServiceAccounts and Secrets created"
else
    print_warning "ServiceAccount YAML not found at .k3rs/default-serviceaccounts.yaml"
    print_warning "Continuing with bootstrap, but pods may not have valid tokens"
fi

# Step 3: Drop the retired self-written cluster DNS. Earlier bootstraps ran a
# `k3rs-dns` Deployment behind the kube-dns Service; the multi-container stacks
# now run upstream CoreDNS exactly as k3s ships it (Step 5), so a leftover
# k3rs-dns pod would sit behind the same k8s-app=kube-dns selector and answer
# for it. Absent on a fresh cluster, hence the swallowed failure (the k3rs
# kubectl built in this workspace rejects --ignore-not-found).
print_step "Removing any retired k3rs-dns Deployment (if present)..."
$KUBECTL $KUBECTL_FLAGS delete deployment k3rs-dns -n kube-system 2>/dev/null \
    && echo "  Deleted k3rs-dns Deployment" || echo "  No k3rs-dns Deployment to delete"

# Step 4: Apply bootstrap cluster resources
# bootstrap-cluster.yaml carries namespaces, the kubernetes + kube-dns
# Services, and PriorityClasses. The CoreDNS backend behind the kube-dns
# Service is applied separately in Step 5 (deploy/addons/coredns.yaml).
#
# The gateway is injected so cluster components can reach the api-server, and to
# fail fast early if discovery broke (#787).
print_step "Applying bootstrap resources (namespaces, services, priority classes)..."
if [ -f "$PROJECT_ROOT/bootstrap-cluster.yaml" ]; then
    # Fail fast if discovery did not give us a gateway — the cluster won't
    # be able to reach the API server without it after #787.
    if [ -z "${K3RS_BRIDGE_GATEWAY:-}" ]; then
        print_error "Bridge gateway discovery failed. Set K3RS_BRIDGE_GATEWAY or fix discover-bridge-gateway.sh"
        exit 1
    fi
    $KUBECTL $KUBECTL_FLAGS apply -f "$PROJECT_ROOT/bootstrap-cluster.yaml"
    print_success "Bootstrap resources created (gateway: $K3RS_BRIDGE_GATEWAY)"
else
    print_error "bootstrap-cluster.yaml not found"
    exit 1
fi

# If the compose files use ${DOCKER_GATEWAY} env var (post-#787), ensure
# the running cluster container sees the discovered value. Write a .env
# file and restart so the compose interpolation takes effect.
#
# Only applies to the all-in-one stack — the multi-container stack
# (compose.yml) doesn't substitute DOCKER_GATEWAY anywhere and the
# `k3rs` container doesn't exist there, so attempting the restart
# would either no-op or, worse, race with a fresh `up -d` and bind-clash
# on the 6443 host port. Gate the restart on the all-in-one `k3rs`
# container actually being up.
if grep -q '\${DOCKER_GATEWAY}' "$PROJECT_ROOT/compose.all-in-one.yml" 2>/dev/null \
    && "$CONTAINER_RT" ps --filter "name=^k3rs$" --format '{{.Names}}' 2>/dev/null \
        | grep -qx 'k3rs'; then
    print_step "Restarting cluster with discovered gateway..."
    echo "DOCKER_GATEWAY=${K3RS_BRIDGE_GATEWAY}" > "$PROJECT_ROOT/.env"
    echo "KUBELET_VOLUMES_PATH=${KUBELET_VOLUMES_PATH}" >> "$PROJECT_ROOT/.env"
    "$CONTAINER_RT" compose -f "$PROJECT_ROOT/compose.all-in-one.yml" up -d
    print_success "Cluster restarted with gateway $K3RS_BRIDGE_GATEWAY"
    echo "  .env file written: $PROJECT_ROOT/.env"
fi

# Step 5: Wire the kube-dns Service to its backend.
#
# The backend depends on the stack:
#   - All-in-one stack (`k3rs` container on the bridge): the DNS
#     server is an in-process task inside that container, so the script
#     creates a hand-written EndpointSlice pointing `kube-dns` at the
#     container's bridge IP (kube-proxy then DNATs 10.96.0.10:53 to it).
#   - Multi-container stacks: the script applies upstream CoreDNS exactly as
#     k3s ships it (deploy/addons/coredns.yaml, a verbatim copy of k3s
#     manifests/coredns.yaml): a `coredns` kube-system Deployment running
#     rancher/mirrored-coredns-coredns. The endpoints controller populates the
#     kube-dns Service from the pod via the Service selector.
# Either way bootstrap-cluster.yaml only creates the kube-dns Service,
# whose ClusterIP 10.96.0.10 every Pod's /etc/resolv.conf references via
# kubelet's --cluster-dns flag, and which we keep stable.

# Some single-node stacks intentionally ship no in-cluster DNS backend (e.g.
# compose.node-conformance.yml — the [NodeConformance] suite has no
# cluster-DNS-resolution specs; those are full-cluster [Conformance]). For
# those, SKIP_DNS_WIRING=1 avoids a pointless 30s wait + an alarming
# "DNS will NOT be functional" warning. The kube-dns Service still exists
# (created above) with no endpoints, which node-scoped tests don't need.
if [ "${SKIP_DNS_WIRING:-0}" = "1" ]; then
    print_step "Skipping DNS backend wiring (SKIP_DNS_WIRING=1)."
    echo "  This stack has no in-cluster DNS backend; node-scoped tests don't need cluster DNS."
else
    # The kube-dns Service stays (created in Step 4) so the ClusterIP is stable.

    # All-in-one stack detection: the `k3rs` container runs the DNS
    # server as an in-process task binding 0.0.0.0:53 — there is no dns pod
    # image on that stack, so kube-dns is wired manually to the container IP.
    # The compose files pin the network name to `k3rs-network`.
    DNS_NETWORK="k3rs-network"
    # The all-in-one container joins the bundled containerd container's network
    # namespace (network_mode: service:containerd), so it has no network
    # settings of its own: its address is the containerd container's.
    AIO_DNS_IP=""
    if $CONTAINER_RT inspect k3rs >/dev/null 2>&1; then
        for aio_net_container in k3rs k3rs-containerd; do
            AIO_DNS_IP=$($CONTAINER_RT inspect "$aio_net_container" \
                --format "{{(index .NetworkSettings.Networks \"$DNS_NETWORK\").IPAddress}}" \
                2>/dev/null || true)
            if [ -n "$AIO_DNS_IP" ] && [ "$AIO_DNS_IP" != "<no value>" ]; then
                break
            fi
            AIO_DNS_IP=""
        done
    fi

    if [ -n "$AIO_DNS_IP" ] && [ "$AIO_DNS_IP" != "<no value>" ]; then
        print_step "Wiring kube-dns Service to the all-in-one k3rs container..."
        echo "  Found k3rs at $AIO_DNS_IP"

        # Wire up the EndpointSlice that backs the kube-dns Service.
        # Without this kube-proxy has nothing to DNAT 10.96.0.10:53 to.
        # The slice carries the standard `kubernetes.io/service-name`
        # label so kube-proxy + the EndpointSlice controller treat it
        # as belonging to kube-dns; the non-controller `managed-by` value
        # keeps the endpointslice controller from pruning it. `addressType:
        # IPv4` matches the bridge IPs; dual-stack support is a follow-up.
        cat <<EOF | $KUBECTL $KUBECTL_FLAGS apply -f -
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: kube-dns-k3rs
  namespace: kube-system
  labels:
    kubernetes.io/service-name: kube-dns
    endpointslice.kubernetes.io/managed-by: bootstrap-cluster.sh
addressType: IPv4
ports:
  - name: dns
    port: 53
    protocol: UDP
  - name: dns-tcp
    port: 53
    protocol: TCP
endpoints:
  - addresses:
      - "$AIO_DNS_IP"
    conditions:
      ready: true
      serving: true
      terminating: false
EOF
        print_success "k3rs wired up at $AIO_DNS_IP for kube-dns Service"
    else
        # Multi-container stack: upstream CoreDNS runs as the kube-system
        # `coredns` Deployment, reading cluster state from the api-server.
        # The endpoints controller wires the kube-dns Service to the pod via
        # the k8s-app=kube-dns selector.
        print_step "Applying CoreDNS (deploy/addons/coredns.yaml, from k3s manifests/coredns.yaml)..."
        coredns_manifest="$PROJECT_ROOT/deploy/addons/coredns.yaml"
        if [ ! -f "$coredns_manifest" ]; then
            print_error "$coredns_manifest not found"
            exit 1
        fi

        # A previous bootstrap may have wired kube-dns manually to a (now
        # stale) compose-container IP — drop that slice; the endpoints
        # controller owns the Service's endpoints from here on.
        $KUBECTL $KUBECTL_FLAGS delete endpointslice kube-dns-k3rs -n kube-system 2>/dev/null \
            && echo "  Deleted stale kube-dns-k3rs EndpointSlice" || true

        # The addon pods run inside containerd too (same store problem as the
        # static pods): side-load every upstream addon image under the exact
        # name the k3s manifests request (ci/mirror/addon-images.txt).
        import_addon_images

        template_k3s_manifest "$coredns_manifest" | $KUBECTL $KUBECTL_FLAGS apply -f -

        # The Corefile's `hosts /etc/coredns/NodeHosts` block and the
        # Deployment's configMap items both require a NodeHosts key. k3s fills
        # it from a node controller (pkg/node/controller.go updateHosts: one
        # "<InternalIP> <nodeName>" line per node, sorted by IP); a one-shot
        # equivalent here, re-run on every bootstrap.
        node_hosts="$($KUBECTL $KUBECTL_FLAGS get nodes \
            -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address} {.metadata.name}{"\n"}{end}' \
            2>/dev/null | awk 'NF == 2' | sort -t. -k1,1n -k2,2n -k3,3n -k4,4n || true)"
        node_hosts_json="$(printf '%s' "$node_hosts" | awk 'BEGIN{ORS="\\n"} {print}')"
        $KUBECTL $KUBECTL_FLAGS patch configmap coredns -n kube-system --type merge \
            -p "{\"data\":{\"NodeHosts\":\"${node_hosts_json}\"}}" \
            && echo "  Set coredns NodeHosts ($(printf '%s' "$node_hosts" | grep -c . || true) node entries)"

        print_step "Waiting for the coredns Deployment to be ready..."
        MAX_WAIT=90
        for i in $(seq 1 $MAX_WAIT); do
            DNS_READY=$($KUBECTL $KUBECTL_FLAGS get deployment coredns -n kube-system -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")

            if [ "$DNS_READY" == "1" ]; then
                print_success "coredns Deployment is ready!"
                break
            fi

            if [ $i -eq $MAX_WAIT ]; then
                print_warning "coredns not ready after ${MAX_WAIT} attempts (readyReplicas: ${DNS_READY:-none})"
                print_warning "Check: $KUBECTL $KUBECTL_FLAGS get pods -n kube-system"
            else
                echo "  Waiting for coredns... ($i/$MAX_WAIT)"
                sleep 2
            fi
        done

        # Upstream local-path-provisioner exactly as k3s ships it (k3s
        # manifests/local-storage.yaml): the `local-path` default StorageClass
        # (rancher.io/local-path, WaitForFirstConsumer, Delete) + RBAC +
        # config. Replaces the old api-server-seeded `standard` class served
        # by the in-tree k3rs.io/hostpath provisioner.
        print_step "Applying local-path-provisioner (deploy/addons/local-storage.yaml, from k3s manifests/local-storage.yaml)..."
        local_storage_manifest="$PROJECT_ROOT/deploy/addons/local-storage.yaml"
        if [ ! -f "$local_storage_manifest" ]; then
            print_error "$local_storage_manifest not found"
            exit 1
        fi
        template_k3s_manifest "$local_storage_manifest" | $KUBECTL $KUBECTL_FLAGS apply -f -

        print_step "Waiting for the local-path-provisioner Deployment to be ready..."
        MAX_WAIT=90
        for i in $(seq 1 $MAX_WAIT); do
            LP_READY=$($KUBECTL $KUBECTL_FLAGS get deployment local-path-provisioner -n kube-system -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")
            if [ "$LP_READY" == "1" ]; then
                print_success "local-path-provisioner Deployment is ready!"
                break
            fi
            if [ $i -eq $MAX_WAIT ]; then
                print_warning "local-path-provisioner not ready after ${MAX_WAIT} attempts (readyReplicas: ${LP_READY:-none})"
            else
                echo "  Waiting for local-path-provisioner... ($i/$MAX_WAIT)"
                sleep 2
            fi
        done

        # Upstream metrics-server exactly as k3s ships it (k3s
        # manifests/metrics-server/*.yaml: ServiceAccount, Deployment, Service,
        # APIService v1beta1.metrics.k8s.io and the four RBAC manifests).
        print_step "Applying metrics-server (deploy/addons/metrics-server/, from k3s manifests/metrics-server/)..."
        for ms_manifest in "$PROJECT_ROOT"/deploy/addons/metrics-server/*.yaml; do
            template_k3s_manifest "$ms_manifest" | $KUBECTL $KUBECTL_FLAGS apply -f -
        done

        print_step "Waiting for the metrics-server Deployment to be ready..."
        MAX_WAIT=90
        for i in $(seq 1 $MAX_WAIT); do
            MS_READY=$($KUBECTL $KUBECTL_FLAGS get deployment metrics-server -n kube-system -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")
            if [ "$MS_READY" == "1" ]; then
                print_success "metrics-server Deployment is ready!"
                break
            fi
            if [ $i -eq $MAX_WAIT ]; then
                print_warning "metrics-server not ready after ${MAX_WAIT} attempts (readyReplicas: ${MS_READY:-none})"
                $KUBECTL $KUBECTL_FLAGS -n kube-system logs deploy/metrics-server --tail=40 2>&1 | sed 's/^/    metrics-server: /' || true
            else
                echo "  Waiting for metrics-server... ($i/$MAX_WAIT)"
                sleep 2
            fi
        done

        # Bundled traefik, run the way k3s runs it: two HelmChart CRs
        # (deploy/addons/traefik.yaml, k3s manifests/traefik.yaml) reconciled by
        # the upstream helm-controller, whose klipper-helm job installs the
        # traefik-crd and traefik charts the api-server serves at
        # /static/charts/ (K3RS_STATIC_DIR). The helm.cattle.io CRDs k3s
        # registers in-process (pkg/server/context.go registerCRDs) are applied
        # from deploy/addons/helm-controller/.
        print_step "Applying the helm.cattle.io CRDs and helm-controller (deploy/addons/helm-controller/)..."
        for crd in "$PROJECT_ROOT"/deploy/addons/helm-controller/helm.cattle.io_*.yaml; do
            $KUBECTL $KUBECTL_FLAGS apply -f "$crd"
        done
        for crd_name in helmcharts.helm.cattle.io helmchartconfigs.helm.cattle.io; do
            $KUBECTL $KUBECTL_FLAGS wait --for=condition=Established "crd/$crd_name" --timeout=60s \
                || print_warning "CRD $crd_name not Established"
        done
        template_k3s_manifest "$PROJECT_ROOT/deploy/addons/helm-controller/helm-controller.yaml" | $KUBECTL $KUBECTL_FLAGS apply -f -
        print_step "Applying traefik HelmCharts (deploy/addons/traefik.yaml, from k3s manifests/traefik.yaml)..."
        template_k3s_manifest "$PROJECT_ROOT/deploy/addons/traefik.yaml" | $KUBECTL $KUBECTL_FLAGS apply -f -

        print_step "Waiting for the traefik Deployment (installed by the helm-controller job)..."
        MAX_WAIT=300
        for i in $(seq 1 $MAX_WAIT); do
            TR_READY=$($KUBECTL $KUBECTL_FLAGS get deployment traefik -n kube-system -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")
            if [ "$TR_READY" == "1" ]; then
                print_success "traefik Deployment is ready!"
                break
            fi
            if [ $i -eq $MAX_WAIT ]; then
                print_warning "traefik not ready after ${MAX_WAIT} attempts (readyReplicas: ${TR_READY:-none})"
                $KUBECTL $KUBECTL_FLAGS -n helm-controller logs deploy/helm-controller --tail=40 2>&1 | sed 's/^/    helm-controller: /' || true
                $KUBECTL $KUBECTL_FLAGS get helmcharts,jobs,pods -A 2>&1 | sed 's/^/    state: /' || true
                $KUBECTL $KUBECTL_FLAGS -n kube-system get job helm-install-traefik -o yaml 2>&1 | sed 's/^/    jobyaml: /' || true
                $KUBECTL $KUBECTL_FLAGS get events -A 2>&1 | tail -40 | sed 's/^/    event: /' || true
                $KUBECTL $KUBECTL_FLAGS -n kube-system logs kube-controller-manager-node-1 --tail=80 2>&1 | grep -i "job\|error" | tail -30 | sed 's/^/    kcm: /' || true
                for j in $($KUBECTL $KUBECTL_FLAGS -n kube-system get jobs -o name 2>/dev/null | grep helm-install || true); do
                    $KUBECTL $KUBECTL_FLAGS -n kube-system logs "$j" --tail=30 2>&1 | sed "s|^|    $j: |" || true
                done
            else
                echo "  Waiting for traefik... ($i/$MAX_WAIT)"
                sleep 2
            fi
        done
    fi
fi

# Step 6: Label node-1 as the control-plane node.
#
# node-1 runs the kube-scheduler static pod (its manifest is mounted into
# node-1's kubelet via --pod-manifest-path; placement does NOT depend on a
# taint). We label it control-plane for identification but DELIBERATELY do NOT
# taint it: this is a 2-node stack, and the [sig-architecture] conformance test
# "should have at least two untainted nodes" requires both nodes schedulable.
# Tainting node-1 NoSchedule left only one untainted node and regressed that
# test plus several scheduling-pressure-sensitive specs (DaemonSet rollback,
# SchedulerPreemption execution path, StatefulSet recreate). The scheduler pod
# coexists with workloads on node-1 instead. (A dedicated tainted control-plane
# node would need a 3rd node — tracked for the lightweight-distro story.)
# Best-effort: a fresh node object may not exist yet on the very first
# bootstrap, so failure is non-fatal and the next bootstrap re-applies.
print_step "Labeling node-1 as control-plane (no taint — 2-node stack)..."
$KUBECTL $KUBECTL_FLAGS label node node-1 node-role.kubernetes.io/control-plane= --overwrite 2>/dev/null \
    && echo "  Labeled node-1 control-plane" || print_warning "Could not label node-1 (not registered yet?)"

echo ""
print_success "Cluster bootstrap complete!"
echo ""
echo "Cluster resources:"
$KUBECTL $KUBECTL_FLAGS get namespaces
echo ""
$KUBECTL $KUBECTL_FLAGS get pods -A
echo ""
$KUBECTL $KUBECTL_FLAGS get services -A
echo ""

print_success "Bootstrap finished successfully!"
