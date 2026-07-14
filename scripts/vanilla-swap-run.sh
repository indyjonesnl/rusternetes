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

# --- swap the one module ---------------------------------------------------
vs_apply_swap "$CLUSTER" "$KUBECONFIG_FILE"

# --- post-swap guard: exactly one rusternetes image ------------------------
vs_guard_cluster "$CLUSTER" "$KUBECONFIG_FILE"

# --- readiness: distinguish "did not come up" from a test failure ----------
if ! vs_wait_ready "$CLUSTER" "$KUBECONFIG_FILE"; then
  vs_emit_result "module-did-not-come-up" 0 0 "$VS_K8S_VERSION"
  exit "$VS_EX_NOTUP"
fi

# --- run the scoped subset via the existing conformance runner -------------
vs_log "running scoped subset (target=$VS_TARGET focus=$VS_FOCUS) via conformance-target-run.sh"
RESULT_OUT="$VS_WORKDIR/target-run.out"
set +e
bash "$SCRIPT_DIR/conformance-target-run.sh" \
  --target "$VS_TARGET" \
  --focus "$VS_FOCUS" \
  --skip "$VS_SKIP" \
  --kubeconfig "$KUBECONFIG_FILE" \
  --output-dir "$VS_WORKDIR" | tee "$RESULT_OUT"
runner_rc=${PIPESTATUS[0]}
set -e

PASSED="$(grep -E '^passed=' "$RESULT_OUT" | tail -1 | cut -d= -f2)"; PASSED="${PASSED:-0}"
FAILED="$(grep -E '^failed=' "$RESULT_OUT" | tail -1 | cut -d= -f2)"; FAILED="${FAILED:-0}"
TOTAL="$(grep -E '^total=' "$RESULT_OUT" | tail -1 | cut -d= -f2)"; TOTAL="${TOTAL:-0}"

if [ "$runner_rc" -ne 0 ]; then
  vs_emit_result "module-did-not-come-up" "$PASSED" "$TOTAL" "$VS_K8S_VERSION"
  exit "$VS_EX_NOTUP"
fi

if [ "${FAILED:-0}" -gt 0 ]; then
  vs_emit_result "test-failed" "$PASSED" "$TOTAL" "$VS_K8S_VERSION"
  exit "$VS_EX_TESTFAIL"
fi

vs_emit_result "test-passed" "$PASSED" "$TOTAL" "$VS_K8S_VERSION"
exit 0
