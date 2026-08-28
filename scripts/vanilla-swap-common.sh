#!/usr/bin/env bash
# Shared helpers for the vanilla-cluster single-module swap harness.
#
# Sourced by scripts/vanilla-swap-run.sh (the driver) and by the two TDD test
# scripts (scripts/vanilla-swap-common-test.sh, scripts/vanilla-swap-guard-test.sh).
# This file deliberately does NOT `set -e`/`set -u` globally so it is safe to
# source into a test harness; every function returns an explicit status and the
# driver enforces strictness itself.
#
# Design: the guard / registry-parsing / image-counting logic is factored into
# PURE functions (no cluster, no network) so it is unit-testable without kind.
# Cluster-touching functions (create / swap / wait / teardown) are separate.

# ---------------------------------------------------------------------------
# Exit-code contract (see specs/003-vanilla-module-swap/contracts/harness-cli.md)
# ---------------------------------------------------------------------------
VS_EX_TESTFAIL=1   # module came up, scoped tests failed
VS_EX_USAGE=2      # usage / preflight / missing tool
VS_EX_GUARD=3      # more than one rusternetes component present
VS_EX_SKEW=4       # baseline version incompatible with the rusternetes build
VS_EX_NOTUP=5      # module never reached readiness within timeout

VS_MODULES="api-server kubelet scheduler controller-manager kube-proxy"
VS_SWAPS="static-pod daemonset join-worker"
VS_READINESS_KINDS="node-ready readyz lease-held service-programmed pod-scheduled deployment-reconciled"

vs_repo_root() {
  local d
  d="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
  printf '%s\n' "$d"
}

vs_registry_path() {
  printf '%s\n' "${VS_REGISTRY:-$(vs_repo_root)/ci/vanilla-swap/targets.json}"
}

vs_log()  { printf '[vanilla-swap] %s\n' "$*" >&2; }
vs_warn() { printf '[vanilla-swap] WARN: %s\n' "$*" >&2; }

# vs_die <message> [exit-code]
vs_die() {
  local code="${2:-$VS_EX_USAGE}"
  printf '[vanilla-swap] ERROR: %s\n' "$1" >&2
  exit "$code"
}

# ---------------------------------------------------------------------------
# Pure, unit-testable functions
# ---------------------------------------------------------------------------

# vs_count_rusternetes_images  — read image refs on stdin (one per line), print
# how many DISTINCT refs contain the substring "rusternetes". Distinct, not
# occurrences: a DaemonSet/Deployment legitimately runs one image across N pods,
# so N copies of the same rusternetes image is still ONE component. Two
# different rusternetes images means two components (guard violation).
vs_count_rusternetes_images() {
  grep -E 'rusternetes' 2>/dev/null | sort -u | grep -c . || true
}

# vs_validate_registry [registry-path]
# Structural validation of the isolation-target registry. Prints the first
# problem to stderr and returns 1 on any violation; returns 0 when valid.
vs_validate_registry() {
  local reg="${1:-$(vs_registry_path)}"
  local root; root="$(vs_repo_root)"
  [ -f "$reg" ] || { vs_warn "registry not found: $reg"; return 1; }

  command -v jq >/dev/null 2>&1 || { vs_warn "jq required to validate registry"; return 1; }
  jq -e 'type == "array"' "$reg" >/dev/null 2>&1 || { vs_warn "registry is not a JSON array"; return 1; }

  # Exactly one entry per module, all five present (SC-003).
  local n; n="$(jq 'length' "$reg")"
  [ "$n" -eq 5 ] || { vs_warn "registry must have exactly 5 entries (one per module), found $n"; return 1; }

  local dupes; dupes="$(jq -r '[.[].module] | group_by(.) | map(select(length>1)) | flatten | unique | .[]' "$reg")"
  [ -z "$dupes" ] || { vs_warn "duplicate module entries: $dupes"; return 1; }

  local m swap rd recipe
  while IFS=$'\t' read -r m swap rd recipe; do
    case " $VS_MODULES " in *" $m "*) ;; *) vs_warn "unknown module: $m"; return 1 ;; esac
    case " $VS_SWAPS " in *" $swap "*) ;; *) vs_warn "unknown swap for $m: $swap"; return 1 ;; esac
    case " $VS_READINESS_KINDS " in *" $rd "*) ;; *) vs_warn "unknown readiness for $m: $rd"; return 1 ;; esac
    [ -f "$root/$recipe" ] || { vs_warn "recipe file missing for $m: $recipe"; return 1; }
  done < <(jq -r '.[] | [.module, .swap, .readiness, .recipe] | @tsv' "$reg")

  return 0
}

# vs_resolve_target <module> [registry-path]
# On success sets VS_MODULE / VS_SWAP / VS_RECIPE / VS_IMAGE_REPO / VS_TARGET /
# VS_FOCUS / VS_SKIP / VS_READINESS and returns 0. Returns 1 on unknown module.
vs_resolve_target() {
  local module="$1" reg="${2:-$(vs_registry_path)}"
  case " $VS_MODULES " in *" $module "*) ;; *) vs_warn "unknown module: $module (want one of: $VS_MODULES)"; return 1 ;; esac

  # Emit one raw field per line, NOT @tsv: jq's @tsv escapes backslashes
  # (`\[` -> `\\[`), which double-escapes the focus/skip ginkgo regexes and
  # makes the focus match zero specs (a NodeConformance run then silently
  # selects nothing yet reports "passed"). `jq -r` on a bare string emits it
  # verbatim, so `\[NodeConformance\]` survives intact.
  local vals
  vals="$(jq -r --arg m "$module" '.[] | select(.module==$m) | (.module,.swap,.recipe,.imageRepo,.target,.focus,.skip,.readiness)' "$reg")"
  [ -n "$vals" ] || { vs_warn "no registry entry for module: $module"; return 1; }

  {
    IFS= read -r VS_MODULE
    IFS= read -r VS_SWAP
    IFS= read -r VS_RECIPE
    IFS= read -r VS_IMAGE_REPO
    IFS= read -r VS_TARGET
    IFS= read -r VS_FOCUS
    IFS= read -r VS_SKIP
    IFS= read -r VS_READINESS
  } <<<"$vals"
  return 0
}

# vs_resolved_image — full image ref for the module under test.
vs_resolved_image() {
  printf '%s:%s\n' "$VS_IMAGE_REPO" "${RUSTERNETES_IMAGE_TAG:-main}"
}

# vs_guard_recipe <module> [registry-path]
# PRE-bring-up guard: the requested module must resolve to exactly ONE registry
# entry. Rejects anything that would place more than one rusternetes component
# in the cluster. Exits VS_EX_GUARD on violation.
vs_guard_recipe() {
  local module="$1" reg="${2:-$(vs_registry_path)}"
  local count
  count="$(jq -r --arg m "$module" '[.[] | select(.module==$m)] | length' "$reg" 2>/dev/null || echo 0)"
  [ "$count" = "1" ] || vs_die "single-module guard: module '$module' resolves to $count registry entries (must be exactly 1)" "$VS_EX_GUARD"
}

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

# vs_require_tools — fail fast if a required tool is missing. NEVER installs
# anything (self-hosted-runner rule).
vs_require_tools() {
  local t missing=()
  for t in kind docker kubectl jq hydrophone; do
    command -v "$t" >/dev/null 2>&1 || missing+=("$t")
  done
  [ "${#missing[@]}" -eq 0 ] || vs_die "missing required tools: ${missing[*]} (install them; the harness does not)" "$VS_EX_USAGE"
}

# vs_version_skew_check <requested-version>
# The baseline is pinned to v1.35 (the project target). A non-default version is
# only permitted if explicitly allow-listed via VS_ALLOW_SKEW=1.
vs_version_skew_check() {
  local requested="$1" target="v1.35"
  case "$requested" in
    "$target"|"$target".*) return 0 ;;
    *)
      [ "${VS_ALLOW_SKEW:-0}" = "1" ] && { vs_warn "version skew allowed by VS_ALLOW_SKEW: $requested vs $target"; return 0; }
      vs_die "baseline version $requested != target $target (set VS_ALLOW_SKEW=1 to override)" "$VS_EX_SKEW"
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Cluster lifecycle
# ---------------------------------------------------------------------------

# vs_kubeconfig_path <cluster>
vs_kubeconfig_path() { printf '%s\n' "${VS_WORKDIR:-/tmp}/vanilla-swap-$1.kubeconfig"; }

vs_install_teardown_trap() {
  local cluster="$1"
  # shellcheck disable=SC2064
  trap "vs_teardown '$cluster'" EXIT INT TERM
}

vs_teardown() {
  local cluster="$1"
  if [ "${VS_KEEP:-0}" = "1" ]; then
    vs_log "--keep set; leaving cluster '$cluster' and side-containers up"
    return 0
  fi
  vs_log "tearing down cluster '$cluster'"
  kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  # Remove any rusternetes side-containers (the kubelet node + its CRI runtime).
  docker ps -aq --filter "label=rusternetes-swap-cluster=$cluster" 2>/dev/null \
    | xargs -r docker rm -f >/dev/null 2>&1 || true
  docker volume rm "vanilla-swap-${cluster}-cri" >/dev/null 2>&1 || true
  docker volume rm "vanilla-swap-${cluster}-cri-data" >/dev/null 2>&1 || true
  docker volume rm "vanilla-swap-${cluster}-kubelet-vols" >/dev/null 2>&1 || true
}

# vs_create_baseline <cluster> <k8s-version>
# A bare minor version (vX.Y) uses kind's bundled default node image (whose
# patch version tracks the kind release); a full vX.Y.Z pins kindest/node:vX.Y.Z.
vs_create_baseline() {
  local cluster="$1" version="$2" root; root="$(vs_repo_root)"
  local image_args=()
  if [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]]; then
    image_args=(--image "kindest/node:${version}")
    vs_log "creating vanilla baseline cluster '$cluster' (node image kindest/node:${version})"
  else
    vs_log "creating vanilla baseline cluster '$cluster' (kind default node image for $version)"
  fi
  kind create cluster --name "$cluster" \
    "${image_args[@]}" \
    --config "$root/ci/vanilla-swap/kind/base-cluster.yaml" \
    --kubeconfig "$(vs_kubeconfig_path "$cluster")" \
    --wait 120s
}

# vs_control_plane_node <cluster>
vs_control_plane_node() { printf '%s-control-plane\n' "$1"; }

# vs_preflight_args <cluster>
# --preflight-arg pairs for conformance-target-run.sh that describe THIS
# cluster to conformance-preflight.sh (#1777). The gate's defaults describe the
# compose stack — kube-scheduler-node-1 / kube-controller-manager-node-1 and the
# rusternetes-dns Deployment — none of which exist in a kind cluster, where the
# static-pod mirrors are named after the kind control-plane node and DNS is
# CoreDNS. Left unset, the gate refused every vanilla-swap leg on three FAILs
# describing the harness rather than the module under test.
#
# The container-scoped assertions (certs mount, store size, per-node binary
# equality) are switched off outright: they address containers by NAME on the
# local daemon, so against a kind cluster they either find nothing or — on a
# workstation that also has a compose stack up — measure a DIFFERENT cluster
# and refuse this run over its certs mount.
#
# Note this KEEPS the gate meaningful rather than turning it off: a swapped
# scheduler or controller-manager whose static pod never reached Running still
# stops the run, a cluster with no DNS backend still does, and so do node
# eviction pressure and a broken registry path.
vs_preflight_args() {
  local cluster="$1" node
  node="$(vs_control_plane_node "$cluster")"
  printf '%s\n' \
    --preflight-arg --control-plane-pods \
    --preflight-arg "kube-scheduler-${node},kube-controller-manager-${node}" \
    --preflight-arg --dns-deployment \
    --preflight-arg coredns \
    --preflight-arg --kubelet \
    --preflight-arg none \
    --preflight-arg --storage-container \
    --preflight-arg none \
    --preflight-arg --skip-node-image-check
}

# ---------------------------------------------------------------------------
# Swap functions
# ---------------------------------------------------------------------------

# vs_recipe_field <recipe-file> <key> — read a scalar `key: value` line.
vs_recipe_field() {
  local f="$1" key="$2"
  # Strip the "key:" prefix, then any trailing inline `# comment` (YAML inline
  # comments require whitespace before the `#`, so a `#` inside a value like a
  # URL fragment is preserved), then trailing whitespace and quotes. Without
  # the comment strip, a commented recipe field (e.g. `criImageRepoSuffix:
  # containerd  # sibling ...`) leaks the comment into the image ref and
  # `docker run` fails with "invalid reference format".
  grep -E "^${key}:" "$f" | head -1 \
    | sed -E "s/^${key}:[[:space:]]*//; s/[[:space:]]+#.*$//; s/[[:space:]]+$//" \
    | tr -d '"'
}

# vs_swap_static_pod <cluster> <recipe> <image>
# Replaces one control-plane static-pod manifest. If the recipe carries a
# `template:` block (preferred), the whole manifest is rendered from it (the
# rusternetes component runs in API mode using an on-node kubeconfig); otherwise
# falls back to an in-place image rewrite + extraArgs append. Either way the
# node kubelet restarts just that static pod.
vs_swap_static_pod() {
  local cluster="$1" recipe="$2" image="$3"
  local root; root="$(vs_repo_root)"
  local node manifest path
  node="$(vs_control_plane_node "$cluster")"
  manifest="$(vs_recipe_field "$root/$recipe" manifest)"
  [ -n "$manifest" ] || vs_die "recipe $recipe has no 'manifest:' field" "$VS_EX_USAGE"
  path="/etc/kubernetes/manifests/$manifest"

  if grep -qE '^template: \|' "$root/$recipe"; then
    vs_log "replacing static pod $manifest with rusternetes manifest ($image) on $node"
    export VS_IMAGE="$image"
    vs_render_recipe_template "$root/$recipe" VS_IMAGE \
      | docker exec -i "$node" sh -c "cat >'$path'"
    return 0
  fi

  vs_log "swapping static pod $manifest image -> $image on $node"
  docker exec "$node" sed -i -E "s#(^[[:space:]]*image:[[:space:]]*).*#\\1${image}#" "$path"
  local arg
  while IFS= read -r arg; do
    [ -n "$arg" ] || continue
    # Use \% as the address delimiter: the pattern contains `/usr/local/bin/`,
    # which would prematurely close a default `/.../ ` sed address (sed then
    # reports "Unmatched ( or \(" and appends nothing).
    docker exec "$node" sed -i -E "\%^[[:space:]]*- (kube-[a-z-]+|/usr/local/bin/.*)\$%a\\    - ${arg}" "$path" || true
  done < <(awk '/^extraArgs:/{f=1;next} f&&/^[[:space:]]*-/{gsub(/^[[:space:]]*-[[:space:]]*"?|"?[[:space:]]*$/,"");print} f&&/^[^[:space:]-]/{f=0}' "$root/$recipe")
}

# vs_swap_daemonset <cluster> <recipe> <image> <kubeconfig>
# Replaces the vanilla kube-proxy DaemonSet with a rusternetes API-mode
# DaemonSet. The rusternetes component authenticates to the vanilla api-server
# with client-cert auth from the control-plane admin kubeconfig (kind's
# kube-proxy uses a projected tokenFile, unsupported by the rusternetes
# kubeconfig loader), so we copy admin.conf into a Secret and mount it.
vs_swap_daemonset() {
  local cluster="$1" recipe="$2" image="$3" kubeconfig="$4"
  local root; root="$(vs_repo_root)"
  local ns vanilla secret node
  ns="$(vs_recipe_field "$root/$recipe" namespace)"
  vanilla="$(vs_recipe_field "$root/$recipe" vanillaDaemonSet)"
  secret="$(vs_recipe_field "$root/$recipe" kubeconfigSecret)"
  node="$(vs_control_plane_node "$cluster")"

  # api-server URL = the control-plane container IP (kube-proxy is hostNetwork).
  local api_ip
  api_ip="$(docker inspect "$node" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' | head -1)"
  export VS_IMAGE="$image"
  export VS_APISERVER_URL="https://${api_ip}:6443"
  export VS_CLUSTER_CIDR="${VS_CLUSTER_CIDR:-$(vs_service_cidr "$cluster" "$kubeconfig")}"
  export VS_NODEPORT_RANGE="${VS_NODEPORT_RANGE:-30000-32767}"

  vs_log "removing vanilla DaemonSet $ns/$vanilla"
  KUBECONFIG="$kubeconfig" kubectl -n "$ns" delete daemonset "$vanilla" --ignore-not-found

  vs_log "creating kubeconfig secret $ns/$secret from admin.conf"
  local tmpkc; tmpkc="$(mktemp)"
  docker exec "$node" cat /etc/kubernetes/admin.conf >"$tmpkc"
  KUBECONFIG="$kubeconfig" kubectl -n "$ns" create secret generic "$secret" \
    --from-file=admin.conf="$tmpkc" --dry-run=client -o yaml \
    | KUBECONFIG="$kubeconfig" kubectl apply -f -
  rm -f "$tmpkc"

  vs_log "applying rusternetes API-mode DaemonSet (api=$VS_APISERVER_URL cidr=$VS_CLUSTER_CIDR)"
  vs_render_recipe_template "$root/$recipe" \
    VS_IMAGE VS_APISERVER_URL VS_CLUSTER_CIDR VS_NODEPORT_RANGE \
    | KUBECONFIG="$kubeconfig" kubectl apply -f -
  KUBECONFIG="$kubeconfig" kubectl -n "$ns" rollout status "daemonset/rusternetes-kube-proxy" --timeout=120s
}

# vs_service_cidr <cluster> <kubeconfig> — read --service-cluster-ip-range off the
# vanilla kube-apiserver static pod.
vs_service_cidr() {
  local cluster="$1" kubeconfig="$2"
  KUBECONFIG="$kubeconfig" kubectl -n kube-system get pod -l component=kube-apiserver \
    -o jsonpath='{.items[0].spec.containers[0].command}' 2>/dev/null \
    | tr ',' '\n' | sed -nE 's/.*service-cluster-ip-range=([0-9./]+).*/\1/p' | head -1
}

# vs_recipe_template <recipe> — print the multi-line `template: |` block.
vs_recipe_template() {
  awk '/^template: \|/{f=1;next} f{if(/^[^[:space:]]/){f=0;next} sub(/^  /,"");print}' "$1"
}

# vs_render_recipe_template <recipe> <variable-name>...
# Render only explicitly named ${VARIABLE} tokens without requiring envsubst.
vs_render_recipe_template() {
  local recipe="$1"
  shift
  local rendered name placeholder marker index
  local -a markers=() values=()

  rendered="$(vs_recipe_template "$recipe")" || return 1
  for name in "$@"; do
    if ! [[ -v "$name" ]]; then
      vs_warn "template variable is unset: $name"
      return 1
    fi
    printf -v placeholder '${%s}' "$name"
    marker=$'\036vs-render-'${#markers[@]}$'\037'
    rendered="${rendered//"$placeholder"/"$marker"}"
    markers+=("$marker")
    values+=("${!name}")
  done
  for index in "${!markers[@]}"; do
    rendered="${rendered//"${markers[index]}"/"${values[index]}"}"
  done
  printf '%s\n' "$rendered"
}

# vs_swap_join_worker <cluster> <recipe> <image> <kubeconfig>
# Attaches ONE extra node running the rusternetes kubelet to the vanilla control
# plane, as CONTAINERS on the kind network (the kubelet binary needs a newer
# glibc than a kind node provides, so it runs from its image with its own CRI
# runtime). Interoperates over the Kubernetes API + CRI/CNI only.
#
# NOTE: currently blocked by rusternetes #1638 (kubelet crashes on a 409 Conflict
# during node status-update). The node registers and briefly reports Ready; the
# node-ready readiness signal then flaps until the bug is fixed.
vs_swap_join_worker() {
  local cluster="$1" recipe="$2" image="$3" kubeconfig="$4"
  local root; root="$(vs_repo_root)"
  local node_name cri clusterdns cri_repo
  node_name="$(vs_recipe_field "$root/$recipe" nodeName)"
  # Exported so the driver can pin test pods to this node once the suite starts
  # (vs_pin_tests_to_swapped_node, #1710).
  VS_NODE_NAME="$node_name"
  export VS_NODE_NAME
  cri="$(vs_recipe_field "$root/$recipe" criSocket)"
  clusterdns="$(vs_recipe_field "$root/$recipe" clusterDns)"
  # CRI runtime image = sibling of the kubelet imageRepo (.../rusternetes/containerd).
  cri_repo="${VS_IMAGE_REPO%/*}/$(vs_recipe_field "$root/$recipe" criImageRepoSuffix)"
  local cri_image="${cri_repo}:${RUSTERNETES_IMAGE_TAG:-main}"

  local cp node_ip vol="vanilla-swap-${cluster}-cri" datavol="vanilla-swap-${cluster}-cri-data"
  cp="$(vs_control_plane_node "$cluster")"
  node_ip="$(docker inspect "$cp" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' | head -1)"

  # Shared kubelet-volumes volume, mounted at the SAME path (/app/volumes, the
  # kubelet's default volume_dir) in BOTH the kubelet and containerd containers.
  # The kubelet stages every pod volume there — managed /etc/hosts,
  # configMap/secret/projected/downward-API files, emptyDir — and containerd (a
  # separate container) bind-mounts those host paths into pod containers. If the
  # dir is not shared, containerd cannot see the staged files and every such
  # mount fails ("mount .../etc-hosts to etc/hosts: Not a directory"), so nearly
  # every pod fails to start. Mirrors compose.sqlite.yml, which shares
  # ${KUBELET_VOLUMES_PATH} between both services.
  #
  # Use a host-path bind with `:rshared`, exactly like compose.sqlite.yml's
  # ${KUBELET_VOLUMES_PATH} (which gets NodeConformance 191/191). rshared lets
  # the kubelet's Memory-emptyDir tmpfs sub-mounts propagate into containerd's
  # mount namespace, and — unlike a docker *named* volume — presents the volume
  # tree to pods without the extra volume-driver bind layer, so inotify behaves
  # as on a real node (a config watcher like kube-proxy is not spuriously woken).
  # (rshared host-binds are rejected by Docker Desktop's /host_mnt; the harness
  # targets native docker / DinD, where / is a shared mount.)
  local volsdir="$VS_WORKDIR/kubelet-volumes"
  mkdir -p "$volsdir"

  vs_log "starting rusternetes CRI runtime ($cri_image)"
  docker volume create "$vol" >/dev/null
  docker volume create "$datavol" >/dev/null
  # /var/lib/containerd (the overlayfs snapshotter root) MUST live on a real
  # volume, not the container's own overlay rootfs — overlay-on-overlay is
  # rejected by the kernel with EINVAL ("failed to mount rootfs component ...
  # overlay ... invalid argument"), so every RunPodSandbox fails and no pod
  # (kindnet/kube-proxy or a test workload) can start. compose.sqlite.yml mounts
  # containerd-data:/var/lib/containerd for the same reason.
  docker run -d --privileged --network kind \
    --name "vanilla-swap-${cluster}-containerd" \
    --label "rusternetes-swap-cluster=$cluster" \
    -v "${vol}:/run/containerd" \
    -v "${datavol}:/var/lib/containerd" \
    -v "${volsdir}:/app/volumes:rshared" \
    "$cri_image" >/dev/null
  local i
  for (( i=0; i<30; i++ )); do
    docker exec "vanilla-swap-${cluster}-containerd" test -S /run/containerd/containerd.sock 2>/dev/null && break
    sleep 2
  done

  vs_log "exporting admin kubeconfig for the kubelet node"
  local kc="$VS_WORKDIR/node-admin.conf"
  docker exec "$cp" cat /etc/kubernetes/admin.conf >"$kc"

  vs_log "starting rusternetes kubelet node '$node_name' ($image) in API mode"
  # Run the kubelet in the CRI container's network namespace, not its own
  # `kind` netns. The bridge CNI creates the pod bridge (10.244.0.0/24) inside
  # the containerd container's netns; a kubelet in a SEPARATE netns has no route
  # to pod IPs, so every kubelet->pod path fails — HTTP/HTTPS lifecycle hooks
  # ("PostStartHookError: error sending request for url http://<podIP>:...") and
  # node->pod connectivity. On a real node the kubelet, the CRI runtime and the
  # CNI bridge all share the node's root netns; sharing containerd's netns
  # reproduces that. The node's InternalIP becomes containerd's kind IP (same
  # network), which the api-server still reaches for the :10250 log/exec proxy.
  #
  # CONTAINERD_STREAM_HOST=localhost follows from that netns sharing. The kubelet
  # PROXIES exec/attach upgrades itself to stream_target()
  # (crates/cri/src/stream.rs), whose host defaults to the DNS name "containerd" —
  # correct only where the runtime is a compose service of that name
  # (compose.node-conformance.yml), and unresolvable here, where the runtime
  # container is vanilla-swap-<cluster>-containerd. Without it every exec failed:
  #
  #   exec_proxy: ... target=http://containerd:10010/exec/wvXcjx6u
  #   proxy_upgrade: backend request failed: client error (Connect)
  #
  # surfacing to the client as `error stream protocol error: unknown error` and to
  # ginkgo as 6 failed specs (#1708). compose.sqlite.yml sets the same value per
  # node for the same reason (#1695); our containerd binds the stream server on
  # 0.0.0.0:10010 (deploy/containerd/config.toml), so loopback reaches it.
  # The kubeconfig is COPIED IN, not bind-mounted. `-v <host-path>:...` is
  # resolved by the DOCKER DAEMON, and under DinD (every CI job here) the daemon
  # runs in a different container from this script — so $VS_WORKDIR/node-admin.conf
  # does not exist on the daemon's filesystem and Docker helpfully creates a
  # DIRECTORY at the mount source. The kubelet then died instantly with
  #
  #   Error: Failed to read kubeconfig from "/kc/admin.conf": Is a directory (os error 21)
  #
  # and the node never registered (run 30439714160). It works on a local single
  # daemon, which is why this only ever failed in CI. `docker cp` goes through the
  # daemon API, so it works either way; the destination is under /app, which
  # exists in the image (docker cp does not create parent directories).
  local node_container="vanilla-swap-${cluster}-${node_name}"
  docker create --privileged \
    --network "container:vanilla-swap-${cluster}-containerd" \
    --name "$node_container" \
    --label "rusternetes-swap-cluster=$cluster" \
    -v "${vol}:/run/containerd" \
    -v "${volsdir}:/app/volumes:rshared" \
    -e "CONTAINER_RUNTIME_ENDPOINT=$cri" \
    -e "KUBELET_VOLUMES_PATH=/app/volumes" \
    -e "CONTAINERD_STREAM_HOST=localhost" \
    "$image" \
    --node-name "$node_name" \
    --kubeconfig /app/admin.conf \
    --api-server-url "https://${node_ip}:6443" \
    --cluster-dns "$clusterdns" \
    --insecure-skip-tls-verify \
    --metrics-port 10250 --sync-interval 3 >/dev/null
  docker cp "$kc" "${node_container}:/app/admin.conf" \
    || vs_die "failed to copy the node kubeconfig into $node_container" "$VS_EX_NOTUP"
  docker start "$node_container" >/dev/null

  # A module that exits on startup should say so now, not 180 seconds from now
  # via a readiness timeout: the container is already dead and its logs are the
  # whole answer.
  sleep 3
  if [ "$(docker inspect -f '{{.State.Running}}' "$node_container" 2>/dev/null)" != "true" ]; then
    vs_warn "kubelet container $node_container exited immediately — logs follow"
    docker logs --tail 40 "$node_container" >&2 2>&1 || true
    vs_die "swapped kubelet did not stay up" "$VS_EX_NOTUP"
  fi

  # Scope kube-proxy OFF the swapped node. This harness isolates the *kubelet*;
  # kube-proxy is a separate component and currently crash-loops on the
  # rusternetes node from a spurious config-watch wakeup specific to the CRI
  # ConfigMap mount (#1652), which would otherwise wedge the e2e "all system
  # pods ready" gate and prevent ANY NodeConformance spec from running. Exclude
  # it via nodeAffinity (before it schedules) and drop any pod already placed
  # there. NodeConformance specs are node-level and do not exercise kube-proxy.
  # Run kubectl INSIDE the control-plane container: the exported node-admin.conf
  # ($kc) points at the kind-internal address (https://<cp>:6443), which is not
  # resolvable from the host where this harness runs. `docker exec "$cp"` uses
  # the in-container admin.conf where that hostname resolves.
  docker exec "$cp" kubectl -n kube-system patch daemonset kube-proxy --type=json \
    -p "[{\"op\":\"add\",\"path\":\"/spec/template/spec/affinity\",\"value\":{\"nodeAffinity\":{\"requiredDuringSchedulingIgnoredDuringExecution\":{\"nodeSelectorTerms\":[{\"matchExpressions\":[{\"key\":\"kubernetes.io/hostname\",\"operator\":\"NotIn\",\"values\":[\"$node_name\"]}]}]}}}}]" \
    >/dev/null 2>&1 || true
  docker exec "$cp" kubectl -n kube-system delete pod -l k8s-app=kube-proxy \
    --field-selector "spec.nodeName=$node_name" --force --grace-period=0 \
    >/dev/null 2>&1 || true

  vs_log "node '$node_name' launched; kube-proxy scoped off it (#1652); readiness checked separately"
}

# vs_apply_swap — dispatch on VS_SWAP.
vs_apply_swap() {
  local cluster="$1" kubeconfig="$2" image
  image="$(vs_resolved_image)"
  case "$VS_SWAP" in
    static-pod)  vs_swap_static_pod "$cluster" "$VS_RECIPE" "$image" ;;
    daemonset)   vs_swap_daemonset "$cluster" "$VS_RECIPE" "$image" "$kubeconfig" ;;
    join-worker) vs_swap_join_worker "$cluster" "$VS_RECIPE" "$image" "$kubeconfig" ;;
    *) vs_die "unknown swap kind: $VS_SWAP" "$VS_EX_USAGE" ;;
  esac
}

# ---------------------------------------------------------------------------
# Post-swap guard: exactly one rusternetes image in the whole cluster
# ---------------------------------------------------------------------------

# vs_collect_cluster_images <cluster> <kubeconfig> — print every image ref in
# play: static-pod manifests, all pod containers, and node container images.
vs_collect_cluster_images() {
  local cluster="$1" kubeconfig="$2" node
  # Pod container images (covers DaemonSets, static pods surfaced as mirror pods).
  KUBECONFIG="$kubeconfig" kubectl get pods -A \
    -o jsonpath='{range .items[*]}{range .spec.containers[*]}{.image}{"\n"}{end}{end}' 2>/dev/null || true
  # Static-pod manifest images straight from the control-plane node (belt & braces).
  node="$(vs_control_plane_node "$cluster")"
  docker exec "$node" sh -c 'grep -h -E "^[[:space:]]*image:" /etc/kubernetes/manifests/*.yaml 2>/dev/null | sed -E "s/^[[:space:]]*image:[[:space:]]*//"' 2>/dev/null || true
  # Node container images (the rusternetes kubelet node, if any).
  docker ps -aq --filter "label=rusternetes-swap-cluster=$cluster" 2>/dev/null \
    | xargs -r docker inspect --format '{{.Config.Image}}' 2>/dev/null || true
}

# vs_guard_cluster <cluster> <kubeconfig> — assert exactly one rusternetes module
# image. The CRI runtime (*/containerd) is infrastructure the kubelet node needs,
# not a module under test, so it is excluded from the count.
vs_guard_cluster() {
  local cluster="$1" kubeconfig="$2" count
  count="$(vs_collect_cluster_images "$cluster" "$kubeconfig" | grep -v '/containerd' | vs_count_rusternetes_images)"
  count="${count:-0}"
  vs_log "post-swap guard: $count rusternetes image(s) present"
  [ "$count" -eq 1 ] || vs_die "single-module guard: expected exactly 1 rusternetes image in cluster, found $count" "$VS_EX_GUARD"
}

# ---------------------------------------------------------------------------
# Test-pod pinning (join-worker only)
# ---------------------------------------------------------------------------

# vs_pin_tests_to_swapped_node <kubeconfig> <swapped-node>
# Make every node EXCEPT the swapped one unschedulable, so the scoped subset
# actually exercises the module under test.
#
# The join-worker leg adds the swapped node to a cluster that still has a
# schedulable vanilla worker, and the only scheduling constraint in this harness
# pushes kube-proxy OFF the swapped node (#1652) — nothing pulled test pods ONTO
# it. So specs landed wherever the scheduler put them: 8 of 8 focused specs
# passed locally while `kubectl exec` against a pinned pod was broken outright,
# and the swapped kubelet logged zero exec_proxy lines for the entire suite. The
# pass rate measured upstream code as much as ours (#1710).
#
# Ordering matters. Hydrophone's own conformance pod must stay on a node with
# kube-proxy (it reaches the api-server via 10.96.0.1:443, which the swapped node
# deliberately cannot route), so we wait until that pod has a nodeName and only
# then cordon. `kubectl cordon` never moves already-placed pods, and DaemonSets
# tolerate unschedulable, so kindnet/kube-proxy stay put. Ginkgo then spends
# minutes on framework startup before creating its first test pod, leaving a wide
# margin before pinning takes effect.
#
# Best-effort by design: if the conformance pod never schedules we leave the
# cluster alone rather than cordoning everything and guaranteeing a dead run.
vs_pin_tests_to_swapped_node() {
  local kubeconfig="$1" keep="$2" placed="" i n
  # `|| true` is load-bearing: kubectl exits non-zero until hydrophone creates the
  # pod, and the driver runs this under `set -euo pipefail`, so without it the
  # first iteration killed the whole watcher silently — it cordoned nothing and
  # logged nothing (same shape as the counter-fallback death in #1704).
  for (( i=0; i<${VS_PIN_WAIT:-150}; i++ )); do
    placed="$(KUBECONFIG="$kubeconfig" kubectl -n conformance get pod e2e-conformance-test \
      -o 'jsonpath={.spec.nodeName}' 2>/dev/null || true)"
    [ -n "$placed" ] && break
    sleep 2
  done
  if [ -z "$placed" ]; then
    vs_warn "conformance pod never got a node; leaving every node schedulable (tests may not run on $keep)"
    return 0
  fi
  # Newline-separated, NOT the space-separated `{.items[*].metadata.name}`: the
  # driver sets IFS=$'\n\t', so a space-separated list would arrive as one word
  # and `kubectl cordon "<all three names>"` would simply fail.
  for n in $(KUBECONFIG="$kubeconfig" kubectl get nodes \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true); do
    [ "$n" = "$keep" ] && continue
    KUBECONFIG="$kubeconfig" kubectl cordon "$n" >/dev/null 2>&1 || true
  done
  vs_log "pinned test pods to '$keep' (conformance pod already placed on '$placed'; other nodes cordoned)"
}

# ---------------------------------------------------------------------------
# Readiness
# ---------------------------------------------------------------------------

# vs_wait_ready <cluster> <kubeconfig> — bounded wait on VS_READINESS.
# Returns 0 when ready, VS_EX_NOTUP on timeout.
vs_wait_ready() {
  local cluster="$1" kubeconfig="$2"
  local deadline=$(( ${VS_READY_TIMEOUT:-180} ))
  local i
  vs_log "waiting for readiness signal '$VS_READINESS' (timeout ${deadline}s)"
  for (( i=0; i<deadline; i+=5 )); do
    if vs_readiness_probe "$cluster" "$kubeconfig"; then
      vs_log "readiness '$VS_READINESS' satisfied after ${i}s"
      return 0
    fi
    sleep 5
  done
  vs_dump_readiness_diagnostics "$cluster" "$kubeconfig"
  return "$VS_EX_NOTUP"
}

# vs_dump_readiness_diagnostics <cluster> <kubeconfig>
# What the run looked like at the moment readiness gave up. Without this a
# readiness timeout is three minutes of silence followed by a bare
# `module-did-not-come-up` — the kubelet leg (run 30438506580) reported exactly
# that and left nothing to act on. Every command is best-effort: this runs on a
# cluster that is by definition unhealthy, and a failing dump must not replace
# the real verdict.
vs_dump_readiness_diagnostics() {
  local cluster="$1" kubeconfig="$2"
  vs_warn "readiness '$VS_READINESS' NOT satisfied within ${VS_READY_TIMEOUT:-180}s — diagnostics follow"

  echo "--- nodes ---" >&2
  KUBECONFIG="$kubeconfig" kubectl get nodes -o wide >&2 2>&1 || true
  echo "--- kube-system pods ---" >&2
  KUBECONFIG="$kubeconfig" kubectl get pods -n kube-system -o wide >&2 2>&1 || true

  # The object the readiness probe was watching, in detail.
  case "$VS_READINESS" in
    node-ready)
      echo "--- describe node rusternetes-node ---" >&2
      KUBECONFIG="$kubeconfig" kubectl describe node rusternetes-node >&2 2>&1 || true ;;
    readyz)
      echo "--- /readyz ---" >&2
      KUBECONFIG="$kubeconfig" kubectl get --raw=/readyz?verbose >&2 2>&1 || true ;;
    service-programmed)
      echo "--- rusternetes-kube-proxy daemonset ---" >&2
      KUBECONFIG="$kubeconfig" kubectl -n kube-system describe daemonset rusternetes-kube-proxy >&2 2>&1 || true ;;
    pod-scheduled)
      echo "--- describe canary pod ---" >&2
      KUBECONFIG="$kubeconfig" kubectl -n default describe pod vanilla-swap-canary >&2 2>&1 || true ;;
    deployment-reconciled)
      echo "--- deployments ---" >&2
      KUBECONFIG="$kubeconfig" kubectl get deployments -A >&2 2>&1 || true ;;
  esac

  # The swapped module's own logs. A join-worker module runs as its own docker
  # container beside the kind nodes (named vanilla-swap-<cluster>-<node>, see
  # vs_start_node); a static-pod / daemonset module runs inside the cluster, so
  # its logs come from the node's container runtime.
  #
  # The container list is ENUMERATED, not guessed: the first version of this dump
  # probed fixed names ("rusternetes-node", …), matched nothing, and printed an
  # empty section for the one failure it existed to explain (run 30439140932).
  echo "--- harness containers ---" >&2
  docker ps -a --filter "name=vanilla-swap-${cluster}" \
    --format '{{.Names}}\t{{.Status}}\t{{.Image}}' >&2 2>&1 || true

  echo "--- swapped module logs (last 60 lines) ---" >&2
  local c
  while IFS= read -r c; do
    [ -n "$c" ] || continue
    echo "[container $c]" >&2
    docker logs --tail 60 "$c" >&2 2>&1 || true
  done < <(docker ps -a --filter "name=vanilla-swap-${cluster}" --format '{{.Names}}' 2>/dev/null)
  KUBECONFIG="$kubeconfig" kubectl -n kube-system logs --tail=60 \
    "-l=component=kube-${VS_MODULE}" >&2 2>&1 || true
  KUBECONFIG="$kubeconfig" kubectl -n kube-system logs --tail=60 \
    "-l=app=rusternetes-${VS_MODULE}" >&2 2>&1 || true
}

vs_readiness_probe() {
  local cluster="$1" kubeconfig="$2"
  case "$VS_READINESS" in
    node-ready)
      # The rusternetes kubelet self-registers the node named in the recipe.
      KUBECONFIG="$kubeconfig" kubectl get node rusternetes-node \
        -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null \
        | grep -q True ;;
    readyz)
      KUBECONFIG="$kubeconfig" kubectl get --raw='/readyz' >/dev/null 2>&1 ;;
    lease-held)
      # A held leader-election lease has a non-empty holderIdentity.
      local lease
      case "$VS_MODULE" in
        scheduler) lease=kube-scheduler ;;
        controller-manager) lease=kube-controller-manager ;;
        *) lease="$VS_MODULE" ;;
      esac
      [ -n "$(KUBECONFIG="$kubeconfig" kubectl -n kube-system get lease "$lease" \
        -o 'jsonpath={.spec.holderIdentity}' 2>/dev/null)" ] ;;
    service-programmed)
      # rusternetes kube-proxy DaemonSet fully rolled out on every node.
      KUBECONFIG="$kubeconfig" kubectl -n kube-system rollout status daemonset/rusternetes-kube-proxy --timeout=5s >/dev/null 2>&1 ;;
    pod-scheduled)
      # The scheduler works iff a pending, unscheduled pod gets a nodeName.
      # API-mode rusternetes control-plane components do no leader election, so
      # there is no lease to check — prove scheduling directly with a canary.
      KUBECONFIG="$kubeconfig" kubectl -n default get pod vanilla-swap-canary >/dev/null 2>&1 || \
        KUBECONFIG="$kubeconfig" kubectl -n default run vanilla-swap-canary \
          --image=registry.k8s.io/pause:3.10 --restart=Never \
          --overrides='{"spec":{"tolerations":[{"operator":"Exists"}]}}' >/dev/null 2>&1 || true
      [ -n "$(KUBECONFIG="$kubeconfig" kubectl -n default get pod vanilla-swap-canary \
        -o 'jsonpath={.spec.nodeName}' 2>/dev/null)" ] ;;
    deployment-reconciled)
      # The controller-manager works iff its deployment->replicaset->pod chain
      # creates Pods for a canary Deployment (no lease in API mode).
      KUBECONFIG="$kubeconfig" kubectl -n default get deploy vanilla-swap-canary >/dev/null 2>&1 || \
        KUBECONFIG="$kubeconfig" kubectl -n default create deployment vanilla-swap-canary \
          --image=registry.k8s.io/pause:3.10 >/dev/null 2>&1 || true
      [ "$(KUBECONFIG="$kubeconfig" kubectl -n default get pods -l app=vanilla-swap-canary \
        -o 'jsonpath={.items[*].metadata.name}' 2>/dev/null | wc -w)" -ge 1 ] ;;
    *) return 1 ;;
  esac
}

# ---------------------------------------------------------------------------
# Result emission
# ---------------------------------------------------------------------------

# vs_junit_counts <dir> — parse the newest junit_*.xml in <dir> and echo
# "RAN FAILED" where RAN = the real specs attempted (passed + failed) and FAILED
# = the real specs that failed. Returns 1 (no output) when no junit is present.
# Junit is authoritative for the test verdict even when the runner is later
# killed (e.g. a module whose post-test namespace cleanup hangs) — the results
# are already on disk.
#
# Counted per <testcase> through the conformance runner's own parser, NOT from
# the junit suite header: that header's `tests` includes ginkgo's suite-level
# nodes ([ReportBeforeSuite], [SynchronizedBeforeSuite], …), which are not specs.
# The scheduler leg ran 2 specs and published a `100% (9/9)` badge straight off
# that header — 2 real + 7 suite-level entries (#1643, the same bug the
# per-target runner had). Reusing target_counts also stops the badge and the
# runner's own passed/total from disagreeing.
vs_junit_counts() {
  local dir="$1" f
  f="$(ls -t "$dir"/junit_*.xml 2>/dev/null | head -1)"
  [ -n "$f" ] || return 1
  local hj passed failed skipped total
  IFS=' ' read -r hj passed failed skipped total <<<"$(
    TARGET_RUN_LIB_ONLY=1 source "$(vs_repo_root)/scripts/conformance-target-run.sh" \
      && target_counts "$(dirname "$f")" "$f"
  )"
  [ "${hj:-0}" = "1" ] || return 1
  printf '%s %s\n' "$(( ${passed:-0} + ${failed:-0} ))" "${failed:-0}"
}

# vs_stdout_counts <runner-stdout-file> — echo "PASSED FAILED TOTAL" parsed from
# conformance-target-run.sh's `passed=`/`failed=`/`total=` lines, defaulting each
# to 0. Always succeeds: a missing file or absent lines is a legitimate state
# (the runner died before it could report), not an error.
#
# This used to be inline in the driver as `grep ... | tail -1 | cut -d= -f2`, and
# under the driver's `set -euo pipefail` a grep that matched nothing took the
# whole driver down BEFORE it wrote run-result.json — so the workflow saw no
# result, reported `no-result`, and published no badge. Which is exactly what
# happens in CI, where GITHUB_OUTPUT is set for the job and the runner therefore
# writes its counters to that file instead of stdout (runs 30433160080,
# 30436964889). The driver now clears GITHUB_OUTPUT for the child so the counters
# land here, and this parse cannot kill it either way.
vs_stdout_counts() {
  local f="${1:-}" passed="" failed="" total=""
  if [ -n "$f" ] && [ -f "$f" ]; then
    passed="$(grep -E '^passed=[0-9]+$' "$f" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
    failed="$(grep -E '^failed=[0-9]+$' "$f" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
    total="$(grep -E '^total=[0-9]+$' "$f" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
  fi
  printf '%s %s %s\n' "${passed:-0}" "${failed:-0}" "${total:-0}"
}

# vs_test_budget_ok <suite-timeout-seconds> <job-timeout-minutes> [reserve-minutes]
# True when the scoped-subset timeout leaves at least `reserve` minutes of the CI
# job budget for everything around it: kind bring-up, image pull, the swap, the
# readiness wait and teardown. The two numbers live in different files
# (scripts/vanilla-swap-run.sh and .github/workflows/vanilla-swap-module.yml), so
# raising one without the other silently reintroduces the failure this guards:
# a suite killed with its junit still inside the conformance pod, which yields no
# counts and therefore no badge.
vs_test_budget_ok() {
  local suite="${1:-0}" job_min="${2:-0}" reserve="${3:-15}"
  case "$suite$job_min" in *[!0-9]*) return 1 ;; esac
  [ "$suite" -gt 0 ] && [ "$job_min" -gt "$reserve" ] || return 1
  [ "$suite" -le "$(( (job_min - reserve) * 60 ))" ]
}

# vs_progress_counts <runner-stdout-file> — echo "PASSED FAILED" recovered from
# ginkgo's progress stream. Always succeeds; 0 0 when there is nothing to read.
#
# Needed because ginkgo registers its junit reporter as a ReportAfterSuite node:
# the file only exists once the suite finishes, so a runner killed mid-suite leaves
# no junit anywhere — not on disk, not inside the conformance pod. What does survive
# is the progress stream this harness already tees: exactly one bullet per completed
# spec, `• [FAILED]` for a failure, S for a skip. The controller-manager leg (run
# 30443157562) was killed having finished 23 of 52 specs and published 0/0; those
# numbers were sitting in its own log.
vs_progress_counts() {
  local f="${1:-}" bullets=0 failed=0
  if [ -n "$f" ] && [ -f "$f" ]; then
    bullets="$(grep -o '•' "$f" 2>/dev/null | grep -c . || true)"
    failed="$(grep -c '• \[FAILED\]' "$f" 2>/dev/null || true)"
  fi
  bullets="${bullets:-0}"; failed="${failed:-0}"
  local passed=$(( bullets - failed ))
  [ "$passed" -lt 0 ] && passed=0
  printf '%s %s\n' "$passed" "$failed"
}

# vs_suite_completed <runner-stdout-file> — true when ginkgo printed its summary
# ("Ran N of M Specs"), which it only does once the suite finishes. Distinguishes a
# kill DURING the suite (partial results) from a kill during post-suite cleanup
# (results are complete; that is what VS_TEST_TIMEOUT was originally for).
vs_suite_completed() {
  local f="${1:-}"
  [ -n "$f" ] && [ -f "$f" ] || return 1
  grep -qE 'Ran [0-9]+ of [0-9]+ Specs' "$f" 2>/dev/null
}

# vs_verdict <ran> <failed> <runner_rc> — echo "<outcome> <exit-code>".
#
# `ran` is the REAL specs attempted (ginkgo suite-level nodes excluded, see
# vs_junit_counts). Zero of them means nothing about the module was proven, so it
# is never test-passed: the kube-proxy leg swapped in cleanly, then the e2e
# framework's [SynchronizedBeforeSuite] failed after 600s and ginkgo ran 0 of
# 7348 specs — with honest counting that is 0/0, and reporting a pass over an
# empty set would publish a green badge for a module that broke the cluster.
#
# A non-zero runner_rc with specs on the board is NOT a failure by itself: the
# runner is killed on VS_TEST_TIMEOUT when post-test cleanup hangs, and junit
# already holds the verdict.
vs_verdict() {
  local ran="${1:-0}" failed="${2:-0}" rc="${3:-0}" completeness="${4:-complete}"
  if [ "$ran" -le 0 ]; then
    if [ "$rc" -ne 0 ]; then
      printf 'module-did-not-come-up %s\n' "$VS_EX_NOTUP"
    else
      printf 'no-result %s\n' "$VS_EX_NOTUP"
    fi
    return 0
  fi
  # Killed mid-suite with specs on the board: a real ratio of what ran, but not a
  # finished suite and never a pass. The caller marks the badge partial.
  if [ "$completeness" = "partial" ]; then
    printf 'test-timeout %s\n' "$VS_EX_TESTFAIL"
    return 0
  fi
  if [ "$failed" -gt 0 ]; then
    printf 'test-failed %s\n' "$VS_EX_TESTFAIL"
    return 0
  fi
  printf 'test-passed 0\n'
}

# vs_emit_result <outcome> <passed> <total> [k8s-version]
# Writes run-result.json to $VS_WORKDIR and prints a stdout summary.
vs_emit_result() {
  local outcome="$1" passed="${2:-0}" total="${3:-0}" version="${4:-${VS_K8S_VERSION:-v1.35}}"
  local image; image="$(vs_resolved_image 2>/dev/null || echo unknown)"
  local out="${VS_WORKDIR:-.}/run-result.json"
  cat >"$out" <<JSON
{
  "module": "${VS_MODULE:-unknown}",
  "k8sVersion": "${version}",
  "image": "${image}",
  "outcome": "${outcome}",
  "passed": ${passed},
  "total": ${total},
  "logPath": "${VS_WORKDIR:-.}"
}
JSON
  printf 'module=%s  k8sVersion=%s  image=%s\noutcome=%s  passed=%s/%s\n' \
    "${VS_MODULE:-unknown}" "$version" "$image" "$outcome" "$passed" "$total"
  vs_log "wrote $out"
}
