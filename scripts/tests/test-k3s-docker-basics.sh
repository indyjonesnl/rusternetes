#!/usr/bin/env bash
# Unit tests for scripts/k3s-tests/docker-basics.sh — no cluster required.
# Ports nothing new: it checks the harness of the k3s tests/docker/basics port
# (NodesReady / CheckDeployments / VerifyValidVersion semantics, spec result
# lines, exit code) against stubbed `kubectl` and `docker`.
#
# Run with: bash scripts/tests/test-k3s-docker-basics.sh
set -euo pipefail
IFS=$'\n\t'
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
SUITE="$REPO_ROOT/scripts/k3s-tests/docker-basics.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
pass=0; failcnt=0
ok()  { echo "  ok: $*"; pass=$((pass + 1)); }
bad() { echo "  FAIL: $*" >&2; failcnt=$((failcnt + 1)); }

[ -f "$SUITE" ] || { echo "FAIL: suite missing: $SUITE" >&2; exit 1; }
bash -n "$SUITE" && ok "suite parses"

# Stubs read fixtures from $STUB_DIR.
mkdir -p "$TMP/bin" "$TMP/stub"
cat > "$TMP/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
args="$*"
case "$args" in
  *"get deployments"*) cat "$STUB_DIR/deployments.json" ;;
  *"get nodes"*) cat "$STUB_DIR/nodes.json" ;;
  *"get pod "*) cat "$STUB_DIR/pod.json" ;;
  *apply*) echo applied ;;
  *) echo "kubectl $args" ;;
esac
EOF
cat > "$TMP/bin/docker" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  inspect) echo true ;;
  exec)
    shift; c="$1"; shift
    if [ "$1" = sh ]; then
      bin="${3#command -v }"
      grep -qx "$c $bin" "$STUB_DIR/binaries" 2>/dev/null
    else
      cat "$STUB_DIR/out-$1" 2>/dev/null || exit 1
    fi ;;
  *) echo "docker $*" ;;
esac
EOF
chmod +x "$TMP/bin/kubectl" "$TMP/bin/docker"

run_suite() {
  PATH="$TMP/bin:$PATH" STUB_DIR="$TMP/stub" KUBECONFIG=/dev/null \
    K3S_BASICS_TIMEOUT_SCALE=100 K3S_BASICS_AIRGAP_LIST="$TMP/stub/airgap.txt" \
    bash "$SUITE" 2>&1
}

# Scenario 1: everything healthy.
cat > "$TMP/stub/deployments.json" <<'EOF'
{"items":[
 {"metadata":{"name":"coredns"},"status":{"replicas":1,"readyReplicas":1}},
 {"metadata":{"name":"local-path-provisioner"},"status":{"replicas":1,"readyReplicas":1}},
 {"metadata":{"name":"metrics-server"},"status":{"replicas":1,"readyReplicas":1}},
 {"metadata":{"name":"traefik"},"status":{"replicas":1,"readyReplicas":1}}]}
EOF
cat > "$TMP/stub/nodes.json" <<'EOF'
{"items":[
 {"metadata":{"name":"node-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
 {"metadata":{"name":"node-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}
EOF
echo '{"status":{"containerStatuses":[{"name":"volume-test","ready":true}]}}' > "$TMP/stub/pod.json"
for c in k3rs-kubelet k3rs-containerd k3rs-kubelet2 k3rs-containerd2; do
  for b in kubectl ctr crictl; do echo "$c $b" >> "$TMP/stub/binaries"; done
done
echo "Client Version: v1.35.0" > "$TMP/stub/out-kubectl"
echo "Version: 2.0.0" > "$TMP/stub/out-ctr"
echo "Version: v1.35.0" > "$TMP/stub/out-crictl"
echo '{"images":[{"repoTags":["a:1"]},{"repoTags":null}]}' > "$TMP/stub/out-crictl-images"
printf 'a:1\n' > "$TMP/stub/airgap.txt"
# `crictl images -o json` goes through the same stub: key on first arg "crictl".
# Healthy crictl prints version for `version` and images json for `images`.
cat > "$TMP/bin/docker" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  inspect) echo true ;;
  exec)
    shift; c="$1"; shift
    if [ "$1" = sh ]; then
      bin="${3#command -v }"
      grep -qx "$c $bin" "$STUB_DIR/binaries" 2>/dev/null
    elif [ "$1" = crictl ] && [ "$2" = images ]; then
      cat "$STUB_DIR/out-crictl-images"
    else
      cat "$STUB_DIR/out-$1" 2>/dev/null || exit 1
    fi ;;
  *) echo "docker $*" ;;
esac
EOF
out="$(run_suite)" && rc=0 || rc=$?
[ "$rc" -eq 0 ] && ok "healthy: exit 0" || bad "healthy: rc=$rc"
grep -q '^\[k3s-docker-basics\] passed=5 failed=0$' <<<"$out" && ok "healthy: summary 5/0" || bad "healthy summary: $out"

# Scenario 2: missing deployment, invalid version, missing airgap list.
sed -i '/traefik/d; s/"name":"metrics-server"},"status":{"replicas":1,"readyReplicas":1}},/"name":"metrics-server"},"status":{"replicas":1,"readyReplicas":1}}]}/' "$TMP/stub/deployments.json"
echo "Version: 1.2.3-dev" > "$TMP/stub/out-ctr"
rm "$TMP/stub/airgap.txt"
out="$(run_suite)" && rc=0 || rc=$?
[ "$rc" -ne 0 ] && ok "failing: non-zero exit" || bad "failing: expected non-zero exit"
grep -q 'spec="Basic Tests Setup Cluster should provision servers and agents" result=FAIL' <<<"$out" && ok "provision FAIL" || bad "provision not FAIL"
grep -q 'failed to deploy traefik' <<<"$out" && ok "names the missing deployment" || bad "missing deployment reason: $out"
grep -q 'invalid version string found in ctr' <<<"$out" && ok "version regex catches dev" || bad "version regex: $out"
grep -q 'airgap image list .* does not exist' <<<"$out" && ok "airgap fails with reason" || bad "airgap reason: $out"
grep -q 'should validate local storage volume" result=PASS' <<<"$out" && ok "later specs still run" || bad "later specs skipped"
grep -q '^\[k3s-docker-basics\] passed=2 failed=3$' <<<"$out" && ok "failing: summary 2/3" || bad "failing summary: $(grep passed= <<<"$out")"
grep -q 'docker logs for node k3rs-kubelet2' <<<"$out" && ok "diagnostics dump node logs" || bad "no diagnostics"

# Scenario 3: NodesReady set-equality and not-ready.
echo '{"items":[{"metadata":{"name":"node-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}' > "$TMP/stub/nodes.json"
out="$(K3S_BASICS_LIB_ONLY=1 PATH="$TMP/bin:$PATH" STUB_DIR="$TMP/stub" KUBECONFIG=/dev/null bash -c "source '$SUITE'; nodes_ready" 2>&1)" && rc=0 || rc=$?
[ "$rc" -ne 0 ] && grep -q 'expected nodes' <<<"$out" && ok "nodes_ready: set mismatch fails" || bad "nodes_ready mismatch: rc=$rc $out"
echo '{"items":[{"metadata":{"name":"node-1"},"status":{"conditions":[{"type":"Ready","status":"False"}]}},{"metadata":{"name":"node-2"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}' > "$TMP/stub/nodes.json"
out="$(K3S_BASICS_LIB_ONLY=1 PATH="$TMP/bin:$PATH" STUB_DIR="$TMP/stub" KUBECONFIG=/dev/null bash -c "source '$SUITE'; nodes_ready" 2>&1)" && rc=0 || rc=$?
[ "$rc" -ne 0 ] && grep -q 'node node-1 is not ready' <<<"$out" && ok "nodes_ready: NotReady fails" || bad "nodes_ready notready: rc=$rc $out"

echo "passed=$pass failed=$failcnt"
[ "$failcnt" -eq 0 ]
