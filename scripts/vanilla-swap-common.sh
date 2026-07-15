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

  local row
  row="$(jq -r --arg m "$module" '.[] | select(.module==$m) | [.module,.swap,.recipe,.imageRepo,.target,.focus,.skip,.readiness] | @tsv' "$reg")"
  [ -n "$row" ] || { vs_warn "no registry entry for module: $module"; return 1; }

  IFS=$'\t' read -r VS_MODULE VS_SWAP VS_RECIPE VS_IMAGE_REPO VS_TARGET VS_FOCUS VS_SKIP VS_READINESS <<<"$row"
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
  # Remove any rusternetes side-containers (the kubelet node) for this cluster.
  docker ps -aq --filter "label=rusternetes-swap-cluster=$cluster" 2>/dev/null \
    | xargs -r docker rm -f >/dev/null 2>&1 || true
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

# ---------------------------------------------------------------------------
# Swap functions
# ---------------------------------------------------------------------------

# vs_recipe_field <recipe-file> <key> — read a scalar `key: value` line.
vs_recipe_field() {
  local f="$1" key="$2"
  grep -E "^${key}:" "$f" | head -1 | sed -E "s/^${key}:[[:space:]]*//" | tr -d '"'
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
    vs_recipe_template "$root/$recipe" \
      | envsubst '${VS_IMAGE}' \
      | docker exec -i "$node" sh -c "cat >'$path'"
    return 0
  fi

  vs_log "swapping static pod $manifest image -> $image on $node"
  docker exec "$node" sed -i -E "s#(^[[:space:]]*image:[[:space:]]*).*#\\1${image}#" "$path"
  local arg
  while IFS= read -r arg; do
    [ -n "$arg" ] || continue
    docker exec "$node" sed -i -E "/^[[:space:]]*- (kube-[a-z-]+|/usr/local/bin/.*)$/a\\    - ${arg}" "$path" || true
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
  vs_recipe_template "$root/$recipe" | envsubst '${VS_IMAGE} ${VS_APISERVER_URL} ${VS_CLUSTER_CIDR} ${VS_NODEPORT_RANGE}' \
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

# vs_swap_join_worker <cluster> <recipe> <image> <kubeconfig>
# Adds ONE extra worker running the rusternetes kubelet, joined to the vanilla
# control plane via kubeadm. Interoperates over CRI/CNI only.
vs_swap_join_worker() {
  local cluster="$1" recipe="$2" image="$3" kubeconfig="$4"
  local root; root="$(vs_repo_root)"
  local node_name cri
  node_name="$(vs_recipe_field "$root/$recipe" nodeName)"
  cri="$(vs_recipe_field "$root/$recipe" criSocket)"
  local container="vanilla-swap-${cluster}-${node_name}"

  vs_log "generating kubeadm join command on $(vs_control_plane_node "$cluster")"
  local join_cmd
  join_cmd="$(docker exec "$(vs_control_plane_node "$cluster")" kubeadm token create --print-join-command)"
  [ -n "$join_cmd" ] || vs_die "failed to create kubeadm join token" "$VS_EX_NOTUP"

  vs_log "starting rusternetes kubelet node '$node_name' ($image) and joining"
  # The rusternetes kubelet image is expected to run its own containerd + CNI and
  # accept a kubeadm join command; it is labelled so teardown can find it.
  docker run -d --privileged \
    --name "$container" \
    --label "rusternetes-swap-cluster=$cluster" \
    --network kind \
    --hostname "$node_name" \
    -e "KUBEADM_JOIN_CMD=$join_cmd" \
    -e "CONTAINER_RUNTIME_ENDPOINT=$cri" \
    "$image" >/dev/null

  vs_log "node '$node_name' launched; readiness is checked separately"
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

# vs_guard_cluster <cluster> <kubeconfig> — assert exactly one rusternetes image.
vs_guard_cluster() {
  local cluster="$1" kubeconfig="$2" count
  count="$(vs_collect_cluster_images "$cluster" "$kubeconfig" | vs_count_rusternetes_images)"
  count="${count:-0}"
  vs_log "post-swap guard: $count rusternetes image(s) present"
  [ "$count" -eq 1 ] || vs_die "single-module guard: expected exactly 1 rusternetes image in cluster, found $count" "$VS_EX_GUARD"
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
  return "$VS_EX_NOTUP"
}

vs_readiness_probe() {
  local cluster="$1" kubeconfig="$2"
  case "$VS_READINESS" in
    node-ready)
      KUBECONFIG="$kubeconfig" kubectl get nodes \
        -l rusternetes.io/module-under-test=kubelet \
        -o 'jsonpath={.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null \
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
# "RAN FAILED" where RAN = tests - skipped/disabled and FAILED = failures +
# errors. Returns 1 (no output) when no junit is present. Junit is authoritative
# for the test verdict even when the runner is later killed (e.g. a module whose
# post-test namespace cleanup hangs) — the results are already on disk.
vs_junit_counts() {
  local dir="$1" f line tests fail err skip disabled
  f="$(ls -t "$dir"/junit_*.xml 2>/dev/null | head -1)"
  [ -n "$f" ] || return 1
  line="$(grep -oE '<testsuites?[^>]*>' "$f" | head -1)"
  [ -n "$line" ] || return 1
  tests="$(printf '%s' "$line" | grep -oE 'tests="[0-9]+"' | grep -oE '[0-9]+' | head -1)"
  fail="$(printf '%s' "$line" | grep -oE 'failures="[0-9]+"' | grep -oE '[0-9]+' | head -1)"
  err="$(printf '%s' "$line" | grep -oE 'errors="[0-9]+"' | grep -oE '[0-9]+' | head -1)"
  skip="$(printf '%s' "$line" | grep -oE 'skipped="[0-9]+"' | grep -oE '[0-9]+' | head -1)"
  disabled="$(printf '%s' "$line" | grep -oE 'disabled="[0-9]+"' | grep -oE '[0-9]+' | head -1)"
  tests="${tests:-0}"; fail="${fail:-0}"; err="${err:-0}"; skip="${skip:-0}"; disabled="${disabled:-0}"
  printf '%s %s\n' "$(( tests - skip - disabled ))" "$(( fail + err ))"
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
