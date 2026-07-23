#!/usr/bin/env bash
# Vanilla-cluster single-module swap harness — driver.
#
# Stand up an UNMODIFIED upstream Kubernetes cluster (kind) and swap in exactly
# ONE rusternetes component, then run a test subset scoped to that component.
# Because every other participant is known-good upstream software, any failure
# is attributable to the single swapped module.
#
# Contract: specs/003-vanilla-module-swap/contracts/harness-cli.md
#
# Usage:
#   scripts/vanilla-swap-run.sh --module <name> [--env local|ci|cloud]
#                               [--keep] [--k8s-version vX.Y]
#
#   --module <name>    exactly one of: api-server kubelet scheduler
#                      controller-manager kube-proxy   (REQUIRED)
#   --env <target>     local (default) | ci | cloud
#   --keep             skip teardown (post-mortem; never in CI)
#   --k8s-version vX.Y baseline version (default v1.35; non-default => skew check)
#
# Exit codes: 0 test-passed · 1 test-failed · 2 usage · 3 guard-rejected
#             4 version-skew-unsupported · 5 module-did-not-come-up
set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/vanilla-swap-common.sh
source "$SCRIPT_DIR/vanilla-swap-common.sh"

usage() { sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

MODULE=""; ENVIRONMENT="local"; VS_KEEP=0; VS_K8S_VERSION="v1.35"
export VS_KEEP VS_K8S_VERSION

module_seen=0
while [ $# -gt 0 ]; do
  case "$1" in
    --module)
      [ $# -ge 2 ] || vs_die "--module requires a value" "$VS_EX_USAGE"
      [ "$module_seen" -eq 0 ] || vs_die "only one --module may be given (isolate ONE component per run)" "$VS_EX_USAGE"
      MODULE="$2"; module_seen=1; shift 2 ;;
    --env)
      [ $# -ge 2 ] || vs_die "--env requires a value" "$VS_EX_USAGE"
      ENVIRONMENT="$2"; shift 2 ;;
    --keep) VS_KEEP=1; export VS_KEEP; shift ;;
    --k8s-version)
      [ $# -ge 2 ] || vs_die "--k8s-version requires a value" "$VS_EX_USAGE"
      VS_K8S_VERSION="$2"; export VS_K8S_VERSION; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) vs_die "unknown argument: $1" "$VS_EX_USAGE" ;;
  esac
done

[ -n "$MODULE" ] || { usage; vs_die "exactly one --module is required" "$VS_EX_USAGE"; }
case "$ENVIRONMENT" in local|ci|cloud) ;; *) vs_die "unknown --env: $ENVIRONMENT" "$VS_EX_USAGE" ;; esac

# --- preflight -------------------------------------------------------------
vs_require_tools
vs_validate_registry || vs_die "isolation-target registry is invalid" "$VS_EX_USAGE"
vs_resolve_target "$MODULE" || vs_die "unknown module: $MODULE" "$VS_EX_USAGE"
# Optional single-test proof: narrow the scoped subset (mirrors
# conformance-target-run.sh --focus). Leaves the registry default in place for
# normal runs.
VS_FOCUS="${VS_FOCUS_OVERRIDE:-$VS_FOCUS}"

# --- pre-bring-up guard (FR-003): exactly one component --------------------
vs_guard_recipe "$MODULE"

# --- version skew (FR-007 edge) --------------------------------------------
vs_version_skew_check "$VS_K8S_VERSION"

# --- workspace + teardown trap ---------------------------------------------
CLUSTER="vanilla-swap-${MODULE}"
VS_WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/vanilla-swap-${MODULE}.XXXXXX")"
export VS_WORKDIR
vs_install_teardown_trap "$CLUSTER"

vs_log "module=$MODULE env=$ENVIRONMENT k8s=$VS_K8S_VERSION swap=$VS_SWAP target=$VS_TARGET"
vs_log "image=$(vs_resolved_image)"

if [ "$ENVIRONMENT" = "cloud" ]; then
  # US5: cloud baseline provisioning. Falls back to kind-in-VM semantics; the
  # swap/test/teardown steps below are environment-agnostic.
  : "${VS_CLOUD_PROVISION:?--env cloud requires VS_CLOUD_PROVISION to name a provisioner}"
  vs_log "cloud provisioning via $VS_CLOUD_PROVISION"
  "$VS_CLOUD_PROVISION" "$CLUSTER" "$VS_K8S_VERSION"
else
  vs_create_baseline "$CLUSTER" "$VS_K8S_VERSION"
fi

KUBECONFIG_FILE="$(vs_kubeconfig_path "$CLUSTER")"
[ -f "$KUBECONFIG_FILE" ] || kind get kubeconfig --name "$CLUSTER" >"$KUBECONFIG_FILE"

# --- api-server swap: snapshot the cluster substrate BEFORE the swap --------
# Swapping the api-server to an empty embedded store drops every object, and the
# still-running vanilla scheduler/CM/kubelets then tear down the now-unlisted
# system workloads (kindnet CNI, kube-proxy, CoreDNS) — leaving no schedulable,
# networked substrate for test pods. Snapshot RBAC + the kube-system addons now;
# restore them after the swap (below) so the still-running controllers rebuild
# the substrate. Only the api-server swap needs this (the other modules keep the
# real api-server + its state).
APISERVER_RESTORE="$VS_WORKDIR/apiserver-restore.json"
if [ "$MODULE" = "api-server" ]; then
  vs_log "snapshotting RBAC + kube-system addons before the api-server swap"
  {
    # NOTE: multiple resource TYPES must be comma-separated — `kubectl get A B`
    # reads B as a *name* of type A (NotFound), capturing 0 objects.
    KUBECONFIG="$KUBECONFIG_FILE" kubectl get -o json \
      namespaces,clusterroles.rbac.authorization.k8s.io,clusterrolebindings.rbac.authorization.k8s.io,priorityclasses.scheduling.k8s.io \
      2>/dev/null
    KUBECONFIG="$KUBECONFIG_FILE" kubectl get -o json -n kube-system \
      serviceaccounts,configmaps,daemonsets.apps,deployments.apps,services,roles.rbac.authorization.k8s.io,rolebindings.rbac.authorization.k8s.io \
      2>/dev/null
  } | python3 -c '
import sys, json
docs=[]
buf=sys.stdin.read()
dec=json.JSONDecoder()
i=0
while i < len(buf):
    while i < len(buf) and buf[i] in " \t\r\n": i+=1
    if i>=len(buf): break
    o,j=dec.raw_decode(buf,i); i=j
    docs += o.get("items",[o]) if o.get("kind","").endswith("List") else [o]
out=[]
for d in docs:
    k=d.get("kind","")
    if not k or k in ("Event",): continue
    m=d.setdefault("metadata",{})
    for f in ("resourceVersion","uid","creationTimestamp","generation","managedFields","selfLink","ownerReferences"):
        m.pop(f,None)
    d.pop("status",None)
    out.append(d)
json.dump({"apiVersion":"v1","kind":"List","items":out}, open(sys.argv[1],"w"))
print(len(out))
' "$APISERVER_RESTORE" | { read n; vs_log "snapshot captured $n objects"; } || vs_warn "snapshot failed (continuing)"
fi

# --- load a locally-built image into the kind nodes ------------------------
# A static-pod / daemonset swap references the rusternetes image by name; the
# node's containerd must have it. CI publishes :main to ghcr and lets the node
# pull, but a local run builds the image and it exists only in the host docker
# daemon. Load it into the kind nodes when present (no-op otherwise, so CI's
# pull path is unaffected).
if [ "$ENVIRONMENT" != "cloud" ]; then
  _vs_img="$(vs_resolved_image)"
  if docker image inspect "$_vs_img" >/dev/null 2>&1; then
    vs_log "loading local image $_vs_img into kind cluster $CLUSTER"
    kind load docker-image "$_vs_img" --name "$CLUSTER" >/dev/null 2>&1 \
      || vs_log "kind load failed (continuing; the node will try to pull instead)"
  fi
fi

# --- swap the one module ---------------------------------------------------
vs_apply_swap "$CLUSTER" "$KUBECONFIG_FILE"

# --- post-swap guard: exactly one rusternetes image ------------------------
vs_guard_cluster "$CLUSTER" "$KUBECONFIG_FILE"

# --- readiness: distinguish "did not come up" from a test failure ----------
if ! vs_wait_ready "$CLUSTER" "$KUBECONFIG_FILE"; then
  vs_emit_result "module-did-not-come-up" 0 0 "$VS_K8S_VERSION"
  exit "$VS_EX_NOTUP"
fi

# --- api-server swap: restore the substrate + re-register nodes ------------
# The swapped api-server is up (readyz) but its store is empty. Restore the
# snapshot (RBAC first, so the system components + admin are authorized; then
# the kube-system addons, which the still-running vanilla controllers turn back
# into pods) and restart each node kubelet so it re-registers its Node object
# against the fresh store (a kubelet only registers at startup; steady-state it
# just status-updates, which no-ops on an empty store). Wait for a Ready node so
# test pods can schedule + get a CNI IP.
if [ "$MODULE" = "api-server" ] && [ -f "$APISERVER_RESTORE" ]; then
  # This whole block is best-effort diagnostics/bring-up: a Forbidden kubectl in
  # a command substitution must never abort the run under `set -e`/pipefail.
  set +e
  # Restore as super-admin (O=system:masters), NOT admin.conf. On the fresh
  # empty store only system:masters is authorized (the api-server seeds a
  # cluster-admin binding to it at startup); kubeadm's admin.conf identity is
  # `kubeadm:cluster-admins`, whose binding does not exist until the restore
  # applies it — a chicken-and-egg that makes an admin.conf-driven apply
  # Forbidden (0 objects). super-admin.conf exists exactly for this
  # RBAC-independent bootstrap. Rewrite its in-cluster server to the
  # host-reachable one from the exported admin kubeconfig.
  RESTORE_KC="$KUBECONFIG_FILE"
  _cp_node="$(vs_control_plane_node "$CLUSTER" 2>/dev/null)"
  if [ -n "$_cp_node" ] && docker exec "$_cp_node" test -f /etc/kubernetes/super-admin.conf 2>/dev/null; then
    _sa="$VS_WORKDIR/super-admin.kubeconfig"
    _server="$(awk '/server:/{print $2; exit}' "$KUBECONFIG_FILE")"
    docker exec "$_cp_node" cat /etc/kubernetes/super-admin.conf >"$_sa" 2>/dev/null
    if [ -n "$_server" ] && [ -s "$_sa" ]; then
      # point at the host-reachable endpoint + skip CA verify (server SAN differs)
      sed -i -E "s#(server:).*#\\1 ${_server}#; /certificate-authority-data:/d" "$_sa"
      sed -i -E "s#(server: .*)#\\1\n    insecure-skip-tls-verify: true#" "$_sa"
      RESTORE_KC="$_sa"
      vs_log "restoring as super-admin (system:masters) via super-admin.conf"
    fi
  fi
  vs_log "restoring substrate snapshot into the swapped api-server"
  KUBECONFIG="$RESTORE_KC" kubectl apply -f "$APISERVER_RESTORE" >"$VS_WORKDIR/restore-apply.log" 2>&1 \
    || vs_warn "some snapshot objects failed to apply (see restore-apply.log)"
  # Surface distinct apply errors (decode/validation gaps in the api-server).
  grep -iE "error|invalid|missing field|unable|cannot" "$VS_WORKDIR/restore-apply.log" 2>/dev/null \
    | sed -E 's/[0-9]+//g' | sort -u | head -10 | sed 's/^/[restore-err] /'
  # Restart ONLY worker kubelets to re-register their Node objects. Never the
  # control-plane node: its kubelet owns the api-server static pod, and bouncing
  # it restarts the api-server (dropping its embedded store) mid-bring-up.
  vs_log "restarting worker kubelets to re-register nodes"
  for n in $(kind get nodes --name "$CLUSTER" 2>/dev/null | grep -v 'control-plane'); do
    docker exec "$n" systemctl restart kubelet >/dev/null 2>&1
  done
  vs_log "waiting for a Ready node + running system pods (≤300s)"
  ready=0
  for _ in $(seq 1 60); do
    ready="$(KUBECONFIG="$RESTORE_KC" kubectl get nodes --no-headers 2>/dev/null | awk '$2=="Ready"' | wc -l)"
    [ "${ready:-0}" -ge 1 ] && break
    sleep 5
  done
  nodes="$(KUBECONFIG="$RESTORE_KC" kubectl get nodes --no-headers 2>/dev/null | wc -l)"
  runpods="$(KUBECONFIG="$RESTORE_KC" kubectl get pods -A --no-headers 2>/dev/null | grep -c Running)"
  vs_log "post-restore substrate: ${nodes:-0} nodes (${ready:-0} Ready), ${runpods:-0} running pods"
  set -e
fi

# --- run the scoped subset via the existing conformance runner -------------
# Bounded by a wall-clock timeout: a module whose post-test cleanup hangs (e.g. a
# controller-manager that never finalizes namespace deletion) must not wedge the
# run indefinitely. The junit on disk is authoritative for the verdict even if
# the runner is killed mid-cleanup.
vs_log "running scoped subset (target=$VS_TARGET focus=$VS_FOCUS) via conformance-target-run.sh"
RESULT_OUT="$VS_WORKDIR/target-run.out"
set +e
timeout "${VS_TEST_TIMEOUT:-1200}" bash "$SCRIPT_DIR/conformance-target-run.sh" \
  --target "$VS_TARGET" \
  --focus "$VS_FOCUS" \
  --skip "$VS_SKIP" \
  --kubeconfig "$KUBECONFIG_FILE" \
  --output-dir "$VS_WORKDIR" | tee "$RESULT_OUT"
runner_rc=${PIPESTATUS[0]}
set -e
[ "$runner_rc" -eq 124 ] && vs_warn "conformance step hit VS_TEST_TIMEOUT (likely a hung post-test cleanup); using junit on disk for the verdict"

# Prefer junit (authoritative); fall back to the runner's stdout counters.
if counts="$(vs_junit_counts "$VS_WORKDIR")"; then
  RAN="${counts% *}"; FAILED="${counts#* }"; TOTAL="$RAN"; PASSED="$(( RAN - FAILED ))"
else
  PASSED="$(grep -E '^passed=' "$RESULT_OUT" | tail -1 | cut -d= -f2)"; PASSED="${PASSED:-0}"
  FAILED="$(grep -E '^failed=' "$RESULT_OUT" | tail -1 | cut -d= -f2)"; FAILED="${FAILED:-0}"
  TOTAL="$(grep -E '^total=' "$RESULT_OUT" | tail -1 | cut -d= -f2)"; TOTAL="${TOTAL:-0}"
fi

# No junit and the runner failed/timed out => the module never produced results.
if ! [ -n "${counts:-}" ] && [ "$runner_rc" -ne 0 ]; then
  vs_emit_result "module-did-not-come-up" "$PASSED" "$TOTAL" "$VS_K8S_VERSION"
  exit "$VS_EX_NOTUP"
fi

if [ "${FAILED:-0}" -gt 0 ]; then
  vs_emit_result "test-failed" "$PASSED" "$TOTAL" "$VS_K8S_VERSION"
  exit "$VS_EX_TESTFAIL"
fi

vs_emit_result "test-passed" "$PASSED" "$TOTAL" "$VS_K8S_VERSION"
exit 0
