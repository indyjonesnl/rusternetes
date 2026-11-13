#!/usr/bin/env bash
# k3s Docker "basics" functional suite, ported to k3rs.
#
# Ports: k3s tests/docker/basics/basics_test.go (Test_DockerBasic, the
#   "Basic Tests" Ordered container, plus VerifyAirgapImages), with the helpers
#   tests/client.go (CheckDefaultDeployments, CheckDeployments, NodesReady,
#   PodReady) and tests/docker/test-helpers.go (NewTestConfig, ProvisionServers,
#   ProvisionAgents, DeployWorkload, VerifyValidVersion, DescribeNodesAndPods,
#   ListContainers, TailDockerLogs, Cleanup). Fixture: resources/volume-test.yaml,
#   a verbatim copy of tests/docker/resources/volume-test.yaml. See also
#   tests/TESTING.md "Docker Tests".
#
# Mapping of k3s "server" / "agent" onto the existing k3rs bring-up
# (ci/actions/bring-up-cluster, compose.sqlite.yml; this script does NOT bring
# the cluster up, that is the caller's job, exactly like
# scripts/conformance-target-run.sh):
#   server  = node-1: containers k3rs-kubelet + k3rs-containerd + k3rs-kube-proxy
#             (one shared netns), together with the control-plane containers
#             k3rs-api-server and k3rs-rhino (the datastore).
#   agent   = node-2: containers k3rs-kubelet2 + k3rs-containerd2 + k3rs-kube-proxy2.
#   ProvisionServers(1)/ProvisionAgents(1) => done by the bring-up action; the
#   provisioning spec here asserts those containers are running.
#   GetNodeNames() => node-1 node-2 (the kubelet --hostname-override values).
#   The k3s server container holds kubectl, ctr and crictl in one place. A k3rs
#   node is several containers, so a binary is run in the first server-node
#   container that has it (kubelet, containerd, kube-proxy); if none has it,
#   the check fails.
#
# Deliberate difference from Ginkgo: an Ordered container skips the specs after
# the first failure. Here every spec runs regardless, so one run lists every
# gap. No assertion is weakened, skipped or reordered.
#
# Output: one `[k3s-docker-basics] spec="<name>" result=PASS|FAIL` line per spec
# and `[k3s-docker-basics] passed=N failed=M`. Exit 1 if any spec failed.
#
# Env: KUBECONFIG (required), KUBECTL (default kubectl), DOCKER (default docker),
#      K3S_BASICS_TIMEOUT_SCALE (default 1; test hook dividing every
#      Eventually timeout and interval).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
KUBECTL="${KUBECTL:-kubectl}"
DOCKER="${DOCKER:-docker}"
SCALE="${K3S_BASICS_TIMEOUT_SCALE:-1}"
RESOURCE_DIR="${K3S_BASICS_RESOURCE_DIR:-$SCRIPT_DIR/resources}"
# k3s: const airgapImageList = "../../../scripts/airgap/image-list.txt"
AIRGAP_IMAGE_LIST="${K3S_BASICS_AIRGAP_LIST:-$REPO_ROOT/scripts/airgap/image-list.txt}"

SERVERS=(node-1)
AGENTS=(node-2)
# node name -> containers making up that node
declare -A NODE_CONTAINERS=(
  [node-1]="k3rs-kubelet k3rs-containerd k3rs-kube-proxy"
  [node-2]="k3rs-kubelet2 k3rs-containerd2 k3rs-kube-proxy2"
)
CONTROL_PLANE_CONTAINERS=(k3rs-api-server k3rs-rhino)

kc() { "$KUBECTL" --kubeconfig "$KUBECONFIG" "$@"; }
log() { echo "[k3s-docker-basics] $*"; }
node_names() { printf '%s\n' "${SERVERS[@]}" "${AGENTS[@]}"; }

# Gomega Eventually(fn, timeout, interval).Should(Succeed()): retry fn until it
# returns 0 or the timeout elapses; the last failure message is left in LAST_ERR.
LAST_ERR=""
eventually() {
  local timeout="$1" interval="$2"; shift 2
  local deadline=$(( SECONDS + timeout / SCALE )) step=$(( interval / SCALE ))
  [ "$step" -ge 1 ] || step=1
  while :; do
    if LAST_ERR="$("$@" 2>&1)"; then return 0; fi
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep "$step"
  done
}

# tests/client.go CheckDeployments: every named deployment must exist with
# status.readyReplicas == status.replicas (absent counts as 0, as in Go).
check_deployments() {
  local ns="$1"; shift
  local json name
  json="$(kc get deployments -n "$ns" -o json)" || return 1
  for name in "$@"; do
    if ! jq -e --arg n "$name" '.items[] | select(.metadata.name==$n)
          | select((.status.readyReplicas // 0) == (.status.replicas // 0))' \
          <<<"$json" >/dev/null; then
      echo "failed to deploy $name"; return 1
    fi
  done
}
# tests/client.go CheckDefaultDeployments
check_default_deployments() {
  check_deployments kube-system coredns local-path-provisioner metrics-server traefik
}

# tests/client.go NodesReady: any NodeReady condition != True is an error; each
# node with at least one condition is inserted into readyNodes; the want and
# found sets must be equal.
nodes_ready() {
  local json found want bad
  json="$(kc get nodes -o json)" || return 1
  bad="$(jq -r '.items[] | .metadata.name as $n | .status.conditions[]?
        | select(.type=="Ready" and .status!="True") | "node \($n) is not ready"' <<<"$json" | head -1)"
  if [ -n "$bad" ]; then echo "$bad"; return 1; fi
  found="$(jq -r '.items[] | select((.status.conditions // []) | length > 0) | .metadata.name' <<<"$json" | sort -u)"
  want="$(node_names | sort -u)"
  if [ "$found" != "$want" ]; then
    echo "expected nodes [$(echo "$want" | tr '\n' ' ')], found [$(echo "$found" | tr '\n' ' ')]"; return 1
  fi
}

# tests/client.go PodReady: the container named like the pod has status.ready=true.
pod_ready() {
  local pod="$1" ns="$2" json
  json="$(kc get pod "$pod" -n "$ns" -o json 2>&1)" || { echo "failed to get pod: $json"; return 1; }
  if jq -e --arg p "$pod" '.status.containerStatuses[]? | select(.name==$p and .ready==true)' <<<"$json" >/dev/null; then
    return 0
  fi
  echo "container $pod in pod $ns/$pod is not ready"; return 1
}

# test-helpers.go DeployWorkload: kubectl apply -f <resources>/<workload>.
deploy_workload() {
  local f="$RESOURCE_DIR/$1"
  [ -f "$f" ] || { echo "Unable to read resource manifest file for $1"; return 1; }
  kc apply -f "$f"
}

# First container of the node that has the binary (empty if none).
find_binary_container() {
  local node="$1" binary="$2" c
  for c in ${NODE_CONTAINERS[$node]}; do
    if "$DOCKER" exec "$c" sh -c "command -v $binary" >/dev/null 2>&1; then echo "$c"; return 0; fi
  done
  return 1
}

# test-helpers.go VerifyValidVersion: `<binary> version` must succeed and no
# line may match (?i).*(dev|head|unknown|fail|refuse|\+[^"]*\.).*
verify_valid_version() {
  local node="$1" binary="$2" where out line
  if ! where="$(find_binary_container "$node" "$binary")"; then
    echo "$binary not found in any container of node $node (${NODE_CONTAINERS[$node]})"; return 1
  fi
  if ! out="$("$DOCKER" exec "$where" "$binary" version 2>&1)"; then
    echo "\`$binary version\` failed in $where: $out"; return 1
  fi
  while IFS= read -r line; do
    if grep -Eiq '(dev|head|unknown|fail|refuse|\+[^"]*\.)' <<<"$line"; then
      echo "invalid version string found in $binary: $line"; return 1
    fi
  done <<<"$out"
}

# basics_test.go VerifyAirgapImages: the union of `crictl images` repoTags[0]
# across all nodes must equal scripts/airgap/image-list.txt (ConsistOf).
verify_airgap_images() {
  local node where images="" out
  for node in $(node_names); do
    if ! where="$(find_binary_container "$node" crictl)"; then
      echo "failed to execute crictl on node $node: crictl not found in ${NODE_CONTAINERS[$node]}"; return 1
    fi
    out="$("$DOCKER" exec "$where" crictl images -o json 2>&1)" \
      || { echo "failed to execute crictl on node $node: $out"; return 1; }
    images+="$(jq -r '.images[].repoTags[0] | select(. != null)' <<<"$out")"$'\n'
  done
  if [ ! -f "$AIRGAP_IMAGE_LIST" ]; then
    echo "airgap image list ${AIRGAP_IMAGE_LIST#"$REPO_ROOT"/} does not exist: k3rs has no build-time generated airgap image list yet (k3s generates scripts/airgap/image-list.txt during build packaging), so the installed images cannot be verified"
    echo "images found on the nodes (generate the list with scripts/airgap/generate-list.sh):"
    printf '%s' "$images" | grep -v '^$' | sort -u | sed 's/^/  /'
    return 1
  fi
  local want have
  want="$(grep -v '^$' "$AIRGAP_IMAGE_LIST" | sort -u)"
  have="$(printf '%s' "$images" | grep -v '^$' | sort -u)"
  if [ "$want" != "$have" ]; then
    echo "airgap image list mismatch (< expected, > found)"
    diff <(echo "$want") <(echo "$have") | sed 's/^/  /'
    return 1
  fi
}

# ---- specs ----------------------------------------------------------------
spec_provision() {
  local c
  # ProvisionServers(1) / ProvisionAgents(1): done by ci/actions/bring-up-cluster.
  for c in ${NODE_CONTAINERS[node-1]} ${NODE_CONTAINERS[node-2]} "${CONTROL_PLANE_CONTAINERS[@]}"; do
    [ "$("$DOCKER" inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" = "true" ] \
      || { echo "container $c is not running"; return 1; }
  done
  eventually 180 5 check_default_deployments || { echo "$LAST_ERR"; return 1; }
  eventually 40 5 nodes_ready || { echo "$LAST_ERR"; return 1; }
}
spec_apply_volume() {
  local out
  out="$(deploy_workload volume-test.yaml 2>&1)" || { echo "failed to apply volume test manifest: $out"; return 1; }
  echo "$out"
}
spec_validate_volume() {
  eventually 20 5 pod_ready volume-test kube-system || { echo "$LAST_ERR"; return 1; }
}
spec_binaries() {
  local s b rc=0
  for s in "${SERVERS[@]}"; do
    for b in kubectl ctr crictl; do
      verify_valid_version "$s" "$b" || rc=1
    done
  done
  return $rc
}
spec_airgap() { verify_airgap_images; }

# ---- runner ---------------------------------------------------------------
PASSED=0; FAILED=0
run_spec() {
  local name="$1" fn="$2" out rc
  out="$("$fn" 2>&1)"; rc=$?
  [ -z "$out" ] || printf '%s\n' "$out" | sed 's/^/    /'
  if [ "$rc" -eq 0 ]; then
    PASSED=$((PASSED + 1)); log "spec=\"$name\" result=PASS"
  else
    FAILED=$((FAILED + 1)); log "spec=\"$name\" result=FAIL"
  fi
}

# AfterSuite on failure: DescribeNodesAndPods, ListContainers, TailDockerLogs(1000).
dump_diagnostics() {
  local c
  echo "::group::describe"
  kc describe node,pod -A 2>&1 || true
  echo "::endgroup::"
  echo "::group::docker-containers"
  echo "** docker container list **"
  "$DOCKER" container list --all --no-trunc 2>&1 || true
  echo "::endgroup::"
  echo "::group::docker-logs"
  for c in ${NODE_CONTAINERS[node-1]} ${NODE_CONTAINERS[node-2]} "${CONTROL_PLANE_CONTAINERS[@]}"; do
    echo "** docker logs for node $c ***"
    "$DOCKER" logs "$c" --tail=1000 2>&1 || echo "** failed to read docker logs for node $c ***"
  done
  echo "::endgroup::"
}

main() {
  [ -n "${KUBECONFIG:-}" ] || { echo "KUBECONFIG must be set" >&2; exit 2; }
  run_spec "Basic Tests Setup Cluster should provision servers and agents" spec_provision
  run_spec "Basic Tests Use Local Storage Volume should apply local storage volume" spec_apply_volume
  run_spec "Basic Tests Use Local Storage Volume should validate local storage volume" spec_validate_volume
  run_spec "Basic Tests Verify Binaries and Images has valid bundled binaries" spec_binaries
  run_spec "Basic Tests Verify Binaries and Images has valid airgap images" spec_airgap
  [ "$FAILED" -eq 0 ] || dump_diagnostics
  log "passed=$PASSED failed=$FAILED"
  [ "$FAILED" -eq 0 ]
}

if [ -z "${K3S_BASICS_LIB_ONLY:-}" ]; then main "$@"; fi
